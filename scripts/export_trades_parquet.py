#!/usr/bin/env python3
"""Export the ranker's read tables from SQLite to Parquet (#375, #545).

FULL ATOMIC REWRITE each run: `trades` + `market_resolutions` + `market_schedules`
(+ optional `market_price_history`, issue #421 PR4, `token_conditions`, issue #429 PR4,
and `clob_payout_evidence_v2`, issue #544, when present) -> zstd Parquet under
`--out-dir` (default data/parquet), via DuckDB's
`sqlite_scanner`. Each file is
written to `<name>.parquet.tmp` then `os.replace`-d into place, so a concurrent
reader never sees a half-written file.

SQLite stays the system-of-record; this is a read-only snapshot. sqlite_scanner
preserves the declared column types (INTEGER->BIGINT, TEXT->VARCHAR), so the DuckDB
ranker queries see `outcome_id`/`contracts` as BIGINT and `price_str` as VARCHAR —
matching the parity contract in `ranker_duck.py`.

A full rewrite means trades appended AFTER the export are invisible to that run;
acceptable because in the pipeline (`rank_and_push.sh`) the Step-0 backfill precedes
the export and the analysis window is 180 days.

Run: `python3 scripts/export_trades_parquet.py --db data/wallet_cache.db --out-dir data/parquet`
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
import time

# The three schema-one tables the ranker reads. Order is irrelevant (independent files).
# `market_schedules` now also carries `start_date_unix` (issue #421 PR4); it rides along free via
# `SELECT *`, so no change is needed here for that column.
TABLES = ("trades", "market_resolutions", "market_schedules")
# The quarantine projection (#608). Ranking excludes partial wallets, but the
# exclusion is only sound if it is evaluated against the SAME history the
# extraction reads: a wallet partial when this snapshot was taken and completed
# afterwards passes a live marker check while DuckDB still reads its partial
# rows. Exported so `ranker_duck` can exclude the snapshot-era set as well.
WALLET_COMPLETENESS = "wallet_completeness"
# OPTIONAL tables (issue #421 PR4 — the CLV price series; #429 PR4 — its token→outcome map).
# Absent on a pre-migration cache (the export attaches READ_ONLY and does not run schema), so each
# is skipped with a warning rather than aborting the whole export. `ranker_duck.py` registers each
# view conditionally to match. `true_clv` needs both `market_price_history` and `token_conditions`.
OPTIONAL_TABLES = ("market_price_history", "token_conditions", "clob_payout_evidence_v2")

# Schema two has one ranker input.  The marker table is intentionally tiny; the
# identifiers, exact amounts, payout vector, and scheduled end remain owned by
# their normalized tables and are recovered by the DuckDB join.
V2_TABLES = (
    "ranker_entries_v2",
    "activity_groups_v2",
    "clob_payout_evidence_v2",
    "activity_coverage_manifests_v2",
    "clob_payout_coverage_manifests_v2",
    "cache_v2_migration_state",
)
V2_EXPORT_MANIFEST = "schema_v2_export_manifest.json"
# Binds the schema-one trade snapshot to the completeness projection taken with
# it (#608). The two are separate files replaced one at a time, so an export that
# dies between them would otherwise leave new trades paired with an older
# "complete" marker and certify history the quarantine meant to withhold. Written
# LAST, so an interrupted export leaves it stale and the reader refuses the pair.
V1_EXPORT_MANIFEST = "schema_v1_export_manifest.json"
V1_BOUND_FILES = ("trades.parquet", "wallet_completeness.parquet")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] export_parquet: {msg}", flush=True)


def _q(s: str) -> str:
    return s.replace("'", "''")


def _table_exists(con, tbl: str) -> bool:
    """True iff `src.{tbl}` is queryable. Used to skip optional tables absent on an older cache."""
    try:
        con.execute(f"SELECT 1 FROM src.{tbl} LIMIT 0;")
        return True
    except Exception:  # noqa: BLE001 — only failure mode here is "table absent"
        return False


def _export_table(con, out_dir: str, tbl: str, row_group_size: int) -> int:
    """Atomically export `src.{tbl}` to `{out_dir}/{tbl}.parquet` (zstd) via tmp + os.replace."""
    final = os.path.join(out_dir, f"{tbl}.parquet")
    tmp = final + ".tmp"
    con.execute(
        f"COPY (SELECT * FROM src.{tbl}) TO '{_q(tmp)}' "
        f"(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {int(row_group_size)});"
    )
    os.replace(tmp, final)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_q(final)}')").fetchone()[0]
    source_n = con.execute(f"SELECT COUNT(*) FROM src.{tbl}").fetchone()[0]
    if n != source_n:
        raise ValueError(f"{tbl}: Parquet count {n} != SQLite count {source_n}")
    size_gb = os.path.getsize(final) / 1e9
    log(f"{tbl}: {n:,} rows -> {final} ({size_gb:.2f} GB)")
    return int(n)


def _completeness_witness(con) -> str | None:
    """Digest of every wallet's completeness state, for detecting a source change
    across the export.

    DuckDB's transaction does NOT snapshot an attached SQLite database — a re-read
    inside one sees an external commit — so the two exports cannot be made atomic
    that way. Instead we prove after the fact that nothing moved: `begin_walk`
    (cache.rs) commits `backfill_partial = 1` before it writes any row, so a walk
    that touched trades during the export must have changed this digest. The
    frontier and floor are included so a walk that both started and finished
    inside the window, returning the marker to its old value, still shows up.
    """
    if not _table_exists(con, "wallets"):
        return None
    columns = []
    for name in ("backfill_partial", "forward_frontier_unix", "backward_floor_unix"):
        try:
            con.execute(f"SELECT {name} FROM src.wallets LIMIT 0;")
        except Exception:  # noqa: BLE001 — column absent on an older cache
            continue
        columns.append(name)
    if "backfill_partial" not in columns:
        return None
    projection = ", ".join(f"COALESCE(CAST({c} AS VARCHAR), 'null')" for c in columns)
    rows = con.execute(
        f"SELECT wallet_hex, {projection} FROM src.wallets ORDER BY wallet_hex"
    ).fetchall()
    digest = hashlib.sha256()
    for row in rows:
        digest.update(("\x1f".join("" if v is None else str(v) for v in row) + "\x1e").encode())
    return digest.hexdigest()


def _export_wallet_completeness(con, out_dir: str, row_group_size: int) -> bool:
    """Export `wallet_hex, backfill_partial` for schema one. Returns False when the
    cache predates the marker, after removing any stale file so a snapshot never
    carries completeness evidence that does not belong to it.
    """
    final = os.path.join(out_dir, f"{WALLET_COMPLETENESS}.parquet")
    has_column = False
    if _table_exists(con, "wallets"):
        try:
            con.execute("SELECT backfill_partial FROM src.wallets LIMIT 0;")
            has_column = True
        except Exception:  # noqa: BLE001 — only failure mode here is "column absent"
            has_column = False
    if not has_column:
        if os.path.exists(final):
            os.remove(final)
            log(f"{WALLET_COMPLETENESS}: marker absent -> removed stale {final}")
        else:
            log(f"{WALLET_COMPLETENESS}: marker absent (pre-#608 cache) -> skipped")
        return False
    tmp = final + ".tmp"
    con.execute(
        f"COPY (SELECT wallet_hex, backfill_partial FROM src.wallets) TO '{_q(tmp)}' "
        f"(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {int(row_group_size)});"
    )
    os.replace(tmp, final)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_q(final)}')").fetchone()[0]
    log(f"{WALLET_COMPLETENESS}: {n:,} rows -> {final}")
    return True


def _file_identity(path: str) -> dict:
    """Size and nanosecond mtime — enough to prove two files came from the same
    export run. `os.replace` updates both, so a file swapped after the manifest was
    written no longer matches it. Deliberately not a digest: `trades.parquet` runs
    to tens of GB and the threat here is an interrupted export, not tampering.
    """
    stat = os.stat(path)
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _write_v1_export_manifest(out_dir: str) -> None:
    """Publish the schema-one trade + completeness pair as one generation."""
    import json

    value = {
        "version": 1,
        "files": {name: _file_identity(os.path.join(out_dir, name))
                  for name in V1_BOUND_FILES},
    }
    final = os.path.join(out_dir, V1_EXPORT_MANIFEST)
    temporary = final + ".tmp"
    with open(temporary, "w", encoding="utf-8") as destination:
        json.dump(value, destination, sort_keys=True, separators=(",", ":"))
        destination.write("\n")
        destination.flush()
        os.fsync(destination.fileno())
    os.replace(temporary, final)
    log(f"schema-one export manifest -> {final}")


def _discard_v1_export_manifest(out_dir: str) -> None:
    """Remove the binding when there is no completeness projection to bind."""
    final = os.path.join(out_dir, V1_EXPORT_MANIFEST)
    if os.path.exists(final):
        os.remove(final)
        log(f"schema-one export manifest removed (no completeness projection) -> {final}")


def _projection_rows(con, relation_prefix: str) -> list[dict]:
    """Canonical joined schema-two rows, matching Rust's projection digest."""
    prefix = f"{relation_prefix}." if relation_prefix else ""
    rows = con.execute(
        f"SELECT ranker.source_trade_id, ranker.activity_generation, "
        f"ranker.classifier_version, groups_v2.wallet_hex, "
        f"groups_v2.condition_id, groups_v2.asset, groups_v2.outcome_id, "
        f"groups_v2.side, groups_v2.share_amount_str, "
        f"groups_v2.price_weighted_share_amount_str, "
        f"groups_v2.source_usdc_amount_str, groups_v2.source_time_unix, "
        f"payout.payout_vector_json, payout.end_date_unix "
        f"FROM {prefix}ranker_entries_v2 ranker "
        f"JOIN {prefix}activity_groups_v2 groups_v2 "
        "ON groups_v2.source_trade_id = ranker.source_trade_id "
        "AND groups_v2.coverage_generation = ranker.activity_generation "
        f"JOIN {prefix}clob_payout_evidence_v2 payout "
        "ON payout.market_id = groups_v2.condition_id "
        "ORDER BY ranker.source_trade_id"
    ).fetchall()
    names = (
        "source_trade_id", "activity_generation", "classifier_version",
        "wallet_hex", "condition_id", "asset", "outcome_id", "side",
        "share_amount_str", "price_weighted_share_amount_str",
        "source_usdc_amount_str", "source_time_unix", "payout_vector_json",
        "end_date_unix",
    )
    return [dict(zip(names, row, strict=True)) for row in rows]


def _projection_digest(rows: list[dict]) -> str:
    import hashlib
    import json

    rendered = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(rendered.encode()).hexdigest()


def _file_sha256(path: str) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_v2_projection(con, out_dir: str) -> dict:
    """Verify SQLite authority and the exported logical projection agree exactly."""
    state = con.execute(
        "SELECT phase, ranker_projection_count, ranker_projection_digest, "
        "ranker_classifier_version FROM src.cache_v2_migration_state "
        "WHERE singleton = 1"
    ).fetchone()
    if state is None or state[0] != "finalized":
        raise ValueError("schema-two cache is not finalized")
    source_rows = _projection_rows(con, "src")
    expected_count = int(state[1]) if state[1] is not None else -1
    expected_digest = str(state[2]) if state[2] is not None else ""
    if len(source_rows) != expected_count or _projection_digest(source_rows) != expected_digest:
        raise ValueError("schema-two SQLite projection count/digest does not match final state")

    con.execute("CREATE SCHEMA IF NOT EXISTS exported")
    for table in V2_TABLES:
        path = _q(os.path.join(out_dir, f"{table}.parquet"))
        con.execute(
            f"CREATE OR REPLACE VIEW exported.{table} AS "
            f"SELECT * FROM read_parquet('{path}')"
        )
    exported_rows = _projection_rows(con, "exported")
    # `_projection_rows` addresses `exported.<table>`; DuckDB schemas are used here
    # so that the exact same join text verifies the Parquet side.
    if len(exported_rows) != expected_count or _projection_digest(exported_rows) != expected_digest:
        raise ValueError("schema-two Parquet projection count/digest mismatch")
    log(
        "schema-two projection verified: "
        f"count={expected_count:,} digest={expected_digest[:16]}… "
        f"classifier_version={state[3]}"
    )
    return {
        "count": expected_count,
        "digest": expected_digest,
        "classifier_version": int(state[3]),
    }


def _write_v2_export_manifest(out_dir: str, counts: dict[str, int], projection: dict) -> None:
    import json

    value = {
        "version": 1,
        "tables": {
            table: {
                "count": counts[table],
                "sha256": _file_sha256(os.path.join(out_dir, f"{table}.parquet")),
            }
            for table in sorted(V2_TABLES)
        },
        "projection": projection,
    }
    final = os.path.join(out_dir, V2_EXPORT_MANIFEST)
    temporary = final + ".tmp"
    with open(temporary, "w", encoding="utf-8") as destination:
        json.dump(value, destination, sort_keys=True, separators=(",", ":"))
        destination.write("\n")
        destination.flush()
        os.fsync(destination.fileno())
    os.replace(temporary, final)
    log(f"schema-two export manifest -> {final}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/wallet_cache.db")
    p.add_argument("--out-dir", default="data/parquet")
    p.add_argument("--row-group-size", type=int, default=1_000_000)
    a = p.parse_args()

    import duckdb

    os.makedirs(a.out_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL sqlite_scanner;")
    con.execute("LOAD sqlite_scanner;")
    db_abs = _q(os.path.abspath(a.db))
    con.execute(f"ATTACH '{db_abs}' AS src (TYPE sqlite, READ_ONLY);")

    with sqlite3.connect(f"file:{os.path.abspath(a.db)}?mode=ro", uri=True) as sqlite:
        schema = int(sqlite.execute("PRAGMA user_version").fetchone()[0])

    t0 = time.time()
    if schema >= 2:
        counts = {}
        for tbl in V2_TABLES:
            if not _table_exists(con, tbl):
                raise ValueError(f"schema-two cache is missing required table {tbl}")
            counts[tbl] = _export_table(con, a.out_dir, tbl, a.row_group_size)
        projection = _verify_v2_projection(con, a.out_dir)
        _write_v2_export_manifest(a.out_dir, counts, projection)
    else:
        # Witness the completeness state before the trade export so a wallet that
        # starts or finishes a backfill mid-export cannot leave partial trades
        # paired with a "complete" marker.
        witness_before = _completeness_witness(con)
        for tbl in TABLES:
            _export_table(con, a.out_dir, tbl, a.row_group_size)
        for tbl in OPTIONAL_TABLES:
            if _table_exists(con, tbl):
                _export_table(con, a.out_dir, tbl, a.row_group_size)
            else:
                log(f"{tbl}: table absent (pre-migration cache) -> skipped")
        exported = _export_wallet_completeness(con, a.out_dir, a.row_group_size)
        witness_after = _completeness_witness(con)
        if not exported:
            _discard_v1_export_manifest(a.out_dir)
        elif witness_before is None or witness_before != witness_after:
            # A wallet's completeness changed while the trades were being written,
            # so the two outputs describe different database states. Publish no
            # binding: ranking then falls back to SQLite instead of trusting them.
            log("completeness changed during the export -> snapshot NOT bound; re-export")
            _discard_v1_export_manifest(a.out_dir)
        else:
            _write_v1_export_manifest(a.out_dir)

    log(f"export complete in {time.time() - t0:.0f}s -> {a.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
