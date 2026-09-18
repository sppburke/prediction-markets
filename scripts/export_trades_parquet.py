#!/usr/bin/env python3
"""Export the ranker's read tables from SQLite to Parquet (#375, #545).

FULL ATOMIC REWRITE each run: `trades` + `market_resolutions` + `market_schedules`
(+ optional `market_price_history`, issue #421 PR4, `token_conditions`, issue #429 PR4,
and `clob_payout_evidence_v2`, issue #544, when present) -> zstd Parquet under
`--out-dir` (default data/parquet), via DuckDB's
`sqlite_scanner`. Each file is
written to `<name>.parquet.tmp` then `os.replace`-d into place, so a concurrent
reader never sees a half-written file.

Schema two exports only the complete certified projection's activity rows; the
other five required tables remain full exports. Full activity history, including
publisher freshness evidence, stays in SQLite. Manifest version 2 declares this
scope so older readers refuse it.

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
from collections.abc import Iterable, Iterator
import os
import sqlite3
import sys
import time

# The three schema-one tables the ranker reads. Order is irrelevant (independent files).
# `market_schedules` now also carries `start_date_unix` (issue #421 PR4); it rides along free via
# `SELECT *`, so no change is needed here for that column.
TABLES = ("trades", "market_resolutions", "market_schedules")
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
PROJECTION_BATCH_SIZE = 1024
CERTIFIED_ACTIVITY_SQL = (
    "SELECT g.* FROM ranker_entries_v2 AS r "
    "CROSS JOIN activity_groups_v2 AS g "
    "WHERE g.source_trade_id = r.source_trade_id "
    "AND g.coverage_generation = r.activity_generation"
)


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


def _typed_sqlite_query(con, query: str, typed_query: str) -> str:
    """Run SQL inside SQLite, restoring sqlite_scanner's BIGINT/VARCHAR types.

    sqlite_query returns every column as VARCHAR, including integers. DESCRIBE
    binds the equivalent attached-table query without scanning its source rows.
    Reject fractional integer text before CAST (DuckDB otherwise rounds it),
    and never use TRY_CAST: malformed integers must fail the export.
    """
    columns = []
    for name, dtype, *_ in con.execute(f"DESCRIBE {typed_query}").fetchall():
        if dtype not in ("BIGINT", "VARCHAR"):
            raise ValueError(f"unsupported SQLite export type {name}: {dtype}")
        quoted = '"' + name.replace('"', '""') + '"'
        value = quoted
        if dtype == "BIGINT":
            value = (
                f"CASE WHEN {quoted} IS NULL OR regexp_full_match({quoted}, '-?[0-9]+') "
                f"THEN {quoted} ELSE error('invalid SQLite INTEGER: {_q(name)}') END"
            )
        columns.append(f"CAST({value} AS {dtype}) AS {quoted}")
    return f"SELECT {', '.join(columns)} FROM sqlite_query('src', '{_q(query)}')"


def _export_table(con, out_dir: str, tbl: str, row_group_size: int) -> int:
    """Atomically export `src.{tbl}` to `{out_dir}/{tbl}.parquet` (zstd) via tmp + os.replace."""
    final = os.path.join(out_dir, f"{tbl}.parquet")
    tmp = final + ".tmp"
    query = f"SELECT * FROM src.{tbl}"
    if tbl == "activity_groups_v2":
        query = _typed_sqlite_query(con, CERTIFIED_ACTIVITY_SQL, query)
    con.execute(
        f"COPY ({query}) TO '{_q(tmp)}' "
        f"(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {int(row_group_size)});"
    )
    os.replace(tmp, final)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_q(final)}')").fetchone()[0]
    count_table = "ranker_entries_v2" if tbl == "activity_groups_v2" else tbl
    source_n = con.execute(f"SELECT COUNT(*) FROM src.{count_table}").fetchone()[0]
    if n != source_n:
        raise ValueError(f"{tbl}: Parquet count {n} != SQLite count {source_n}")
    size_gb = os.path.getsize(final) / 1e9
    log(f"{tbl}: {n:,} rows -> {final} ({size_gb:.2f} GB)")
    return int(n)


def _projection_rows(con, relation_prefix: str) -> Iterator[dict]:
    """Canonical joined schema-two rows, matching Rust's projection digest."""
    prefix = f"{relation_prefix}." if relation_prefix else ""
    query = (
        f"SELECT ranker.source_trade_id, ranker.activity_generation, "
        f"ranker.classifier_version, groups_v2.wallet_hex, "
        f"groups_v2.condition_id, groups_v2.asset, groups_v2.outcome_id, "
        f"groups_v2.side, groups_v2.share_amount_str, "
        f"groups_v2.price_weighted_share_amount_str, "
        f"groups_v2.source_usdc_amount_str, groups_v2.source_time_unix, "
        f"payout.payout_vector_json, payout.end_date_unix "
        f"FROM {prefix}ranker_entries_v2 ranker "
        f"CROSS JOIN {prefix}activity_groups_v2 groups_v2 "
        f"JOIN {prefix}clob_payout_evidence_v2 payout "
        "ON payout.market_id = groups_v2.condition_id "
        "WHERE groups_v2.source_trade_id = ranker.source_trade_id "
        "AND groups_v2.coverage_generation = ranker.activity_generation "
        "ORDER BY ranker.source_trade_id"
    )
    if relation_prefix == "src":
        # Keep source verification proportional to the projection too, rather
        # than letting DuckDB scan the full attached activity table for its join.
        query = _typed_sqlite_query(con, query.replace("src.", ""), query)
    cursor = con.execute(query)
    names = (
        "source_trade_id", "activity_generation", "classifier_version",
        "wallet_hex", "condition_id", "asset", "outcome_id", "side",
        "share_amount_str", "price_weighted_share_amount_str",
        "source_usdc_amount_str", "source_time_unix", "payout_vector_json",
        "end_date_unix",
    )
    while batch := cursor.fetchmany(PROJECTION_BATCH_SIZE):
        for row in batch:
            yield dict(zip(names, row, strict=True))


def _projection_digest(rows: Iterable[dict]) -> tuple[int, str]:
    import hashlib
    import json

    digest = hashlib.sha256(b"[")
    count = 0
    for row in rows:
        if count:
            digest.update(b",")
        digest.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
        count += 1
    digest.update(b"]")
    return count, digest.hexdigest()


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
    generation = con.execute(
        "SELECT MAX(generation) FROM src.activity_coverage_manifests_v2"
    ).fetchone()[0]
    if generation is None or con.execute(
        "SELECT COUNT(*) FROM src.ranker_entries_v2 "
        "WHERE activity_generation IS NULL OR activity_generation != ?",
        [generation],
    ).fetchone()[0]:
        raise ValueError("schema-two projection activity generation mismatch")
    source_count, source_digest = _projection_digest(_projection_rows(con, "src"))
    expected_count = int(state[1]) if state[1] is not None else -1
    expected_digest = str(state[2]) if state[2] is not None else ""
    if source_count != expected_count or source_digest != expected_digest:
        raise ValueError("schema-two SQLite projection count/digest does not match final state")

    con.execute("CREATE SCHEMA IF NOT EXISTS exported")
    for table in V2_TABLES:
        path = _q(os.path.join(out_dir, f"{table}.parquet"))
        con.execute(
            f"CREATE OR REPLACE VIEW exported.{table} AS "
            f"SELECT * FROM read_parquet('{path}')"
        )
    exported_count, exported_digest = _projection_digest(_projection_rows(con, "exported"))
    # `_projection_rows` addresses `exported.<table>`; DuckDB schemas are used here
    # so that the exact same join text verifies the Parquet side.
    if exported_count != expected_count or exported_digest != expected_digest:
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
        "activity_generation": int(generation),
    }


def _write_v2_export_manifest(out_dir: str, counts: dict[str, int], projection: dict) -> None:
    import json

    if not (counts["activity_groups_v2"] == counts["ranker_entries_v2"] == projection["count"]):
        raise ValueError("schema-two certified activity/projection count mismatch")
    value = {
        "version": 2,
        "activity_scope": "certified_ranker_entries",
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

    with sqlite3.connect(f"file:{os.path.abspath(a.db)}?mode=ro", uri=True) as sqlite:
        schema = int(sqlite.execute("PRAGMA user_version").fetchone()[0])
        if schema == -2:
            raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")

    import duckdb

    os.makedirs(a.out_dir, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL sqlite_scanner;")
    con.execute("LOAD sqlite_scanner;")
    db_abs = _q(os.path.abspath(a.db))
    con.execute(f"ATTACH '{db_abs}' AS src (TYPE sqlite, READ_ONLY);")

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
        for tbl in TABLES:
            _export_table(con, a.out_dir, tbl, a.row_group_size)
        for tbl in OPTIONAL_TABLES:
            if _table_exists(con, tbl):
                _export_table(con, a.out_dir, tbl, a.row_group_size)
            else:
                log(f"{tbl}: table absent (pre-migration cache) -> skipped")

    log(f"export complete in {time.time() - t0:.0f}s -> {a.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
