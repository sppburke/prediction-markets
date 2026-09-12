#!/usr/bin/env python3
"""DuckDB read-layer for the 72hr ranker passes (issue #375).

An OPTIONAL accelerator over the SQLite system-of-record. The two heavy ranker
passes scan the ~269M-row `trades` table; this module runs that scan + first-buy
dedup + the high-selectivity filters in DuckDB over a Parquet snapshot
(`export_trades_parquet.py`), returning the SAME qualifying rows / price tapes the
SQLite path would produce, so ALL scoring stays in shared Python — one
`eff`/`gross`/`net` + `weighted_stats` code path, hence bit-parity.

`get_engine()` is the single decision point. Schema one retains the optional
DuckDB/SQLite behavior. Schema two requires its count/hash-verified Parquet
projection and refuses SQLite; there is no second schema-two extraction engine.
connection ONLY when DuckDB is importable, the Parquet snapshot exists, and (unless
forced) is fresh; otherwise it returns `None` and the caller runs the unchanged
SQLite path. The fallback is logged, never silent. SQLite stays the
system-of-record: this layer is read-only over an exported snapshot.

Engine selection (env, overridable by the caller):
  PE_RANKER_ENGINE               auto | duck | sqlite   (default auto)
  PE_RANKER_PARQUET_DIR          snapshot dir            (default data/parquet)
  PE_RANKER_PARQUET_MAX_AGE_HOURS staleness cap, 0=off   (default 4)
  PE_RANKER_DUCKDB_MEMORY_LIMIT  DuckDB memory cap       (default 8GB)
  PE_RANKER_DUCKDB_THREADS       DuckDB thread cap       (default 4; ''/0 = uncapped). Caps the
                                 first-buy hash-aggregate's peak memory so it fits the memory
                                 cap at full-universe scale (#387).
"""
from __future__ import annotations

import json
import hashlib
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# Parquet snapshot file names (one per ranker read table).
TRADES_PARQUET = "trades.parquet"
RESOLUTIONS_PARQUET = "market_resolutions.parquet"
SCHEDULES_PARQUET = "market_schedules.parquet"
WALLET_COMPLETENESS_PARQUET = "wallet_completeness.parquet"
V1_EXPORT_MANIFEST = "schema_v1_export_manifest.json"
V1_BOUND_FILES = (TRADES_PARQUET, WALLET_COMPLETENESS_PARQUET)
# Deliberately NOT in REQUIRED_PARQUET: a cache that predates the #608 marker has
# no quarantine at all, so demanding the projection would strand the DuckDB path
# on every such cache. The caller requires it only when the live cache carries
# the marker — see `rank_72hr_buyandhold.main`.
REQUIRED_PARQUET = (TRADES_PARQUET, RESOLUTIONS_PARQUET, SCHEDULES_PARQUET)
# OPTIONAL snapshots (issue #421 PR4 / #429 PR4 — the CLV price series + its token→outcome map).
# Absent until the prices-history backfill + export run, so deliberately NOT in REQUIRED_PARQUET:
# the proxy-CLV and non-CLV bake-off axes must run without them. Registered as views only when
# their parquet is present. `true_clv` needs BOTH (it joins the series to the bought outcome via
# token_conditions.outcome_index).
MARKET_PRICE_HISTORY_PARQUET = "market_price_history.parquet"
TOKEN_CONDITIONS_PARQUET = "token_conditions.parquet"
# #544: stored-only v2 payout evidence. Registration never changes ranker
# queries; #545 owns switching any economic consumer to this view.
CLOB_PAYOUT_EVIDENCE_V2_PARQUET = "clob_payout_evidence_v2.parquet"
RANKER_ENTRIES_V2_PARQUET = "ranker_entries_v2.parquet"
ACTIVITY_GROUPS_V2_PARQUET = "activity_groups_v2.parquet"
ACTIVITY_COVERAGE_V2_PARQUET = "activity_coverage_manifests_v2.parquet"
CLOB_PAYOUT_COVERAGE_V2_PARQUET = "clob_payout_coverage_manifests_v2.parquet"
CACHE_V2_STATE_PARQUET = "cache_v2_migration_state.parquet"
V2_EXPORT_MANIFEST = "schema_v2_export_manifest.json"
REQUIRED_V2_PARQUET = (
    RANKER_ENTRIES_V2_PARQUET,
    ACTIVITY_GROUPS_V2_PARQUET,
    CLOB_PAYOUT_EVIDENCE_V2_PARQUET,
    ACTIVITY_COVERAGE_V2_PARQUET,
    CLOB_PAYOUT_COVERAGE_V2_PARQUET,
    CACHE_V2_STATE_PARQUET,
)
REQUIRED_V2_FILES = REQUIRED_V2_PARQUET + (V2_EXPORT_MANIFEST,)

DEFAULT_PARQUET_DIR = "data/parquet"
DEFAULT_MAX_AGE_HOURS = 4.0


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] ranker_duck: {msg}", flush=True)


def _q(s: str) -> str:
    """Single-quote-escape a string for embedding in a DuckDB SQL literal."""
    return s.replace("'", "''")


class SchemaTwoEngineError(RuntimeError):
    """Schema two cannot run without its verified DuckDB/Parquet projection."""


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_v2_export_manifest(parquet_dir: str) -> dict:
    path = os.path.join(parquet_dir, V2_EXPORT_MANIFEST)
    with open(path, encoding="utf-8") as source:
        value = json.load(source)
    if value.get("version") != 1 or set(value.get("tables", {})) != {
        name.removesuffix(".parquet") for name in REQUIRED_V2_PARQUET
    }:
        raise SchemaTwoEngineError("schema-two export manifest has an invalid shape")
    for name in REQUIRED_V2_PARQUET:
        table = name.removesuffix(".parquet")
        expected = value["tables"][table].get("sha256")
        actual = _sha256_file(os.path.join(parquet_dir, name))
        if expected != actual:
            raise SchemaTwoEngineError(
                f"schema-two Parquet hash mismatch for {name}"
            )
    return value


def snapshot_partial_wallets(parquet_dir: str | None = None) -> set[str]:
    """Wallets that were partially backfilled when this snapshot was taken.

    Ranking must exclude these as well as the currently marked set: a wallet
    partial at export time and completed since would otherwise pass the live
    check while DuckDB reads its partial rows out of the snapshot.
    """
    import duckdb

    _, parquet_dir, _ = engine_settings(None, parquet_dir, None)
    path = os.path.join(parquet_dir, WALLET_COMPLETENESS_PARQUET)
    if not os.path.exists(path):
        raise FileNotFoundError(f"snapshot completeness projection missing: {path}")
    # The trades and the completeness projection are separate files replaced one at
    # a time. Only accept them as evidence when the manifest written at the end of
    # the export still matches BOTH, which proves they came from the same run; an
    # export that died in between leaves a mismatch and the pair is refused.
    manifest_path = os.path.join(parquet_dir, V1_EXPORT_MANIFEST)
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(
            f"snapshot export manifest missing: {manifest_path} "
            "(trades and completeness are not bound to one export)"
        )
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    recorded = manifest.get("files") or {}
    for name in V1_BOUND_FILES:
        bound = recorded.get(name)
        target = os.path.join(parquet_dir, name)
        if bound is None or not os.path.exists(target):
            raise FileNotFoundError(f"snapshot export manifest does not bind {name}")
        stat = os.stat(target)
        if int(bound.get("size", -1)) != stat.st_size or \
                int(bound.get("mtime_ns", -1)) != stat.st_mtime_ns:
            raise FileNotFoundError(
                f"snapshot export is inconsistent: {name} does not match the manifest "
                "(an export was interrupted; re-export before using DuckDB)"
            )
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT wallet_hex FROM read_parquet('{_q(path)}') WHERE backfill_partial = 1"
        ).fetchall()
    finally:
        con.close()
    return {str(r[0]).lower() for r in rows if r[0] is not None}


def _snapshot_state(parquet_dir: str, max_age_hours: float,
                    required=REQUIRED_PARQUET) -> tuple[bool, str]:
    """Return (usable, reason). Usable iff every required Parquet file exists and —
    when `max_age_hours > 0` — the oldest is younger than that bound."""
    now = time.time()
    for name in required:
        path = os.path.join(parquet_dir, name)
        if not os.path.exists(path):
            return False, f"missing {name}"
        if max_age_hours > 0:
            age_h = (now - os.path.getmtime(path)) / 3600.0
            if age_h > max_age_hours:
                return False, f"{name} stale ({age_h:.1f}h > {max_age_hours}h)"
    return True, "fresh"


def engine_settings(force: str | None = None,
                    parquet_dir: str | None = None,
                    max_age_hours: float | None = None) -> tuple[str, str, float]:
    """Resolve (force, parquet_dir, max_age_hours) from explicit args then env then
    defaults. Kept separate so callers (and tests) can resolve config without a
    DuckDB import."""
    force = (force or os.environ.get("PE_RANKER_ENGINE", "auto") or "auto").lower()
    parquet_dir = parquet_dir or os.environ.get("PE_RANKER_PARQUET_DIR") or DEFAULT_PARQUET_DIR
    if max_age_hours is None:
        env_age = os.environ.get("PE_RANKER_PARQUET_MAX_AGE_HOURS")
        max_age_hours = float(env_age) if env_age not in (None, "") else DEFAULT_MAX_AGE_HOURS
    return force, parquet_dir, max_age_hours


def get_engine(force: str | None = None,
               parquet_dir: str | None = None,
               max_age_hours: float | None = None,
               *, schema_version: int = 1):
    """Return a configured DuckDB connection over the Parquet snapshot, or `None` to
    fall back to the SQLite path.

    `force` (or `PE_RANKER_ENGINE`): "sqlite" -> always None; "duck" -> require the
    snapshot (raise if absent, age ignored); "auto"/None -> use it only if duckdb is
    importable and the snapshot is fresh, else None. Never raises on the auto path.
    """
    force, parquet_dir, max_age_hours = engine_settings(force, parquet_dir, max_age_hours)

    schema_two = schema_version >= 2
    if force == "sqlite" and schema_two:
        raise SchemaTwoEngineError(
            "--engine sqlite is unsupported for schema-two caches; export the "
            "verified Parquet projection and use DuckDB"
        )
    if force == "sqlite":
        log("engine=sqlite (forced)")
        return None

    try:
        import duckdb
    except ImportError:
        if force == "duck":
            raise
        if schema_two:
            raise SchemaTwoEngineError(
                "schema-two cache requires DuckDB, but duckdb is not importable"
            )
        log("duckdb not importable -> SQLite path")
        return None

    required = REQUIRED_V2_FILES if schema_two else REQUIRED_PARQUET
    if force == "duck":
        usable, reason = _snapshot_state(
            parquet_dir, 0.0, required
        )  # forced: existence only
        if not usable:
            raise FileNotFoundError(
                f"--engine duck but Parquet snapshot unusable: {reason} (run export_trades_parquet.py)"
            )
    else:
        usable, reason = _snapshot_state(parquet_dir, max_age_hours, required)
        if not usable:
            if schema_two:
                raise SchemaTwoEngineError(
                    f"schema-two Parquet snapshot unusable: {reason} "
                    "(run export_trades_parquet.py)"
                )
            log(f"Parquet snapshot unusable ({reason}) -> SQLite path")
            return None

    con = duckdb.connect()
    mem = os.environ.get("PE_RANKER_DUCKDB_MEMORY_LIMIT", "8GB")
    con.execute(f"SET memory_limit='{_q(mem)}';")
    # This read-layer never depends on DuckDB row order (results go straight to pandas; pass-2
    # sorts tapes explicitly), so disable insertion-order preservation — it lets the big
    # first-buy GROUP BY spill to disk instead of OOM-ing (#387).
    con.execute("SET preserve_insertion_order=false;")
    # Cap threads: the full-universe first-buy hash aggregate's peak memory scales with thread
    # count, so DuckDB's default (all cores) OOMs the memory cap even when spilling is enabled.
    # Default 4 fits the 8GB cap at 496K-wallet scale; ''/0 leaves it uncapped (only safe with a
    # much larger PE_RANKER_DUCKDB_MEMORY_LIMIT) (#387). A non-integer value (operator typo) falls
    # back to the default rather than crashing the rank — this runs before any SQLite fallback.
    threads = os.environ.get("PE_RANKER_DUCKDB_THREADS", "4").strip()
    if threads in ("", "0"):
        threads_desc = "uncapped"
    else:
        try:
            n_threads = int(threads)
        except ValueError:
            log(f"PE_RANKER_DUCKDB_THREADS={threads!r} is not an integer; using 4")
            n_threads = 4
        con.execute(f"SET threads={n_threads};")
        threads_desc = str(n_threads)
    tmp = os.path.join(parquet_dir, ".duckdb_tmp")
    os.makedirs(tmp, exist_ok=True)
    con.execute(f"SET temp_directory='{_q(tmp)}';")
    # Views over the Parquet snapshot — typed by the export (BIGINT/VARCHAR), so
    # outcome_id/contracts are BIGINT and price_str is VARCHAR, matching the contract.
    base_views = (
        (
            ("ranker_entries_v2", RANKER_ENTRIES_V2_PARQUET),
            ("activity_groups_v2", ACTIVITY_GROUPS_V2_PARQUET),
            ("clob_payout_evidence_v2", CLOB_PAYOUT_EVIDENCE_V2_PARQUET),
            ("activity_coverage_manifests_v2", ACTIVITY_COVERAGE_V2_PARQUET),
            ("clob_payout_coverage_manifests_v2", CLOB_PAYOUT_COVERAGE_V2_PARQUET),
            ("cache_v2_migration_state", CACHE_V2_STATE_PARQUET),
        ) if schema_two else (
            ("trades", TRADES_PARQUET),
            ("market_resolutions", RESOLUTIONS_PARQUET),
            ("market_schedules", SCHEDULES_PARQUET),
        )
    )
    for tbl, name in base_views:
        path = _q(os.path.join(parquet_dir, name))
        con.execute(f"CREATE VIEW {tbl} AS SELECT * FROM read_parquet('{path}');")
    if schema_two:
        export_manifest = _load_v2_export_manifest(parquet_dir)
        for name in REQUIRED_V2_PARQUET:
            table = name.removesuffix(".parquet")
            actual_count = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            if actual_count != int(export_manifest["tables"][table]["count"]):
                raise SchemaTwoEngineError(
                    f"schema-two Parquet count mismatch for {table}"
                )
        state = con.execute(
            "SELECT phase, ranker_projection_count, ranker_projection_digest, "
            "ranker_classifier_version FROM cache_v2_migration_state WHERE singleton = 1"
        ).fetchone()
        projection = export_manifest.get("projection", {})
        if (state is None or state[0] != "finalized"
                or int(state[1]) != int(projection.get("count", -1))
                or str(state[2]) != str(projection.get("digest", ""))
                or int(state[3]) != int(projection.get("classifier_version", -1))):
            raise SchemaTwoEngineError(
                "schema-two export manifest does not match finalized projection state"
            )
    # Optional CLV view (issue #421 PR4) — registered only when its parquet exists, so the engine
    # stays usable for proxy-CLV / non-CLV runs before the prices-history backfill has run.
    mph_path = os.path.join(parquet_dir, MARKET_PRICE_HISTORY_PARQUET)
    if os.path.exists(mph_path):
        con.execute(
            f"CREATE VIEW market_price_history AS SELECT * FROM read_parquet('{_q(mph_path)}');"
        )
        log(f"registered optional view market_price_history ({MARKET_PRICE_HISTORY_PARQUET})")
    # Optional token→outcome map (issue #429 PR4) — the true_clv join maps the bought outcome_id to
    # a CLOB token_id via token_conditions.outcome_index. Registered only when its parquet exists.
    tc_path = os.path.join(parquet_dir, TOKEN_CONDITIONS_PARQUET)
    if os.path.exists(tc_path):
        con.execute(
            f"CREATE VIEW token_conditions AS SELECT * FROM read_parquet('{_q(tc_path)}');"
        )
        log(f"registered optional view token_conditions ({TOKEN_CONDITIONS_PARQUET})")
    payout_path = os.path.join(parquet_dir, CLOB_PAYOUT_EVIDENCE_V2_PARQUET)
    if not schema_two and os.path.exists(payout_path):
        con.execute(
            "CREATE VIEW clob_payout_evidence_v2 AS SELECT * "
            f"FROM read_parquet('{_q(payout_path)}');"
        )
        log("registered optional view clob_payout_evidence_v2 "
            f"({CLOB_PAYOUT_EVIDENCE_V2_PARQUET})")
    log(f"engine=duck over {parquet_dir} (memory_limit={mem}, threads={threads_desc})")
    return con


@dataclass(frozen=True)
class ClobPayoutEvidenceV2:
    """Canonical #544 row returned identically by SQLite or DuckDB.

    The payout vector remains its canonical JSON bytes (an array of exact
    decimal strings); converting it to Python float would break reader parity.
    """

    market_id: str
    is_50_50_outcome: bool | None
    payout_status: str
    payout_vector_json: str | None
    closed: bool | None
    tokens_json: str
    raw_page_sha256: str
    coverage_generation: int
    page_ordinal: int
    schema_version: int
    parser_version: int
    fetched_at_unix: int
    origin: str
    end_date_unix: int | None


def _canonical_payout_vector(value: str) -> str:
    try:
        raw = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid clob payout vector JSON: {exc}") from exc
    if (not isinstance(raw, list) or len(raw) != 2
            or any(not isinstance(item, str) for item in raw)):
        raise ValueError("clob payout vector must contain exactly two decimal strings")
    try:
        decimals = [Decimal(item) for item in raw]
    except InvalidOperation as exc:
        raise ValueError("clob payout vector contains a non-decimal string") from exc
    if tuple(decimals) not in {
        (Decimal(1), Decimal(0)),
        (Decimal(0), Decimal(1)),
        (Decimal("0.5"), Decimal("0.5")),
    }:
        raise ValueError("binary CLOB payout must be [1,0], [0,1], or [0.5,0.5]")

    def spelling(item: Decimal) -> str:
        normalized = format(item.normalize(), "f")
        return "0" if Decimal(normalized) == 0 else normalized

    canonical = json.dumps([spelling(item) for item in decimals], separators=(",", ":"))
    if canonical != value:
        raise ValueError("clob payout vector is not in canonical storage form")
    return canonical


def load_clob_payout_evidence_v2(con, market_id: str) -> ClobPayoutEvidenceV2 | None:
    """Read one v2 payout row from either sqlite3 or DuckDB.

    This reader is intentionally unused by ranking/evaluation in #544. It
    validates the stored canonical shape for replay and reader parity only.
    """
    row = con.execute(
        "SELECT market_id, is_50_50_outcome, payout_status, payout_vector_json, "
        "closed, tokens_json, raw_page_sha256, coverage_generation, page_ordinal, "
        "schema_version, parser_version, fetched_at_unix, origin, end_date_unix "
        "FROM clob_payout_evidence_v2 WHERE market_id = ?",
        [market_id],
    ).fetchone()
    if row is None:
        return None
    status = str(row[2])
    vector = None if row[3] is None else _canonical_payout_vector(str(row[3]))
    if (status == "resolved") != (vector is not None):
        raise ValueError("resolved payout status/vector presence mismatch")
    allowed = {"resolved", "unresolved_open", "unresolved_incomplete",
               "unresolved_conflicting", "unresolved_malformed_price"}
    if status not in allowed:
        raise ValueError(f"unknown clob payout status {status!r}")
    if int(row[9]) != 2 or int(row[10]) != 2:
        raise ValueError("unsupported clob payout schema/parser version")
    if str(row[12]) != "clob_closed_walk_v2":
        raise ValueError("clob payout row did not originate in the v2 closed-market walk")

    def optional_bool(value, field: str) -> bool | None:
        if value is None:
            return None
        if int(value) not in (0, 1):
            raise ValueError(f"invalid {field} boolean {value!r}")
        return bool(int(value))

    return ClobPayoutEvidenceV2(
        market_id=str(row[0]),
        is_50_50_outcome=optional_bool(row[1], "is_50_50_outcome"),
        payout_status=status,
        payout_vector_json=vector,
        closed=optional_bool(row[4], "closed"),
        tokens_json=str(row[5]),
        raw_page_sha256=str(row[6]),
        coverage_generation=int(row[7]),
        page_ordinal=int(row[8]),
        schema_version=int(row[9]),
        parser_version=int(row[10]),
        fetched_at_unix=int(row[11]),
        origin=str(row[12]),
        end_date_unix=None if row[13] is None else int(row[13]),
    )


def _assert_slice_key_premise(con) -> None:
    """Fail the extract loudly if any `source_trade_id` is not `0x` + 64 lowercase hex.

    The first-buy tie key encodes the id as four fixed-width UBIGINT hex slices (see fb0),
    which orders identically to the SQLite engine's TEXT comparison ONLY for ids of that
    exact shape: a shorter all-hex id would cast silently but order differently, and mixed
    case would flip the TEXT order. Malformed ids must therefore stop the rank (a format
    change in source ids is a source-contract change to surface, never to paper over) —
    fail-closed, mirroring the resolver-evidence rule. One id-column scan per extract
    (~70s on the production tape); raises RuntimeError with the violation count.
    """
    bad = con.execute(
        "SELECT count(*) FROM trades "
        "WHERE NOT regexp_full_match(source_trade_id, '0x[0-9a-f]{64}')"
    ).fetchone()[0]
    if bad:
        raise RuntimeError(
            f"duck tie-key premise violated: {bad} source_trade_id row(s) are not "
            "'0x'+64-lowercase-hex; the slice key would diverge from SQLite TEXT order "
            "(#530). Rank halted — inspect the tape/export before re-running."
        )


def duck_extract_positions(con, wallets, win_start, win_end, ttr_lo, ttr_secs,
                           scheduled_only, price_min, price_max, *, materialize_as=None):
    """Return a pandas DataFrame of QUALIFYING first-buy positions across `wallets` —
    the same SET the SQLite per-wallet scan+filter produces — with the 9 raw columns
    `wallet, market_id, outcome_id, entry_ts, ttr_secs, price, contracts, payoff,
    resolved_at`. `eff`/`gross`/`net` are NOT computed here; the shared Python tail
    (`rank_72hr_buyandhold.process_wallet_positions`) computes them for both engines.

    Filters mirror `rank_72hr_buyandhold.main()` exactly: first buy per
    (wallet, market) (side='buy'); entry in `[win_start, win_end)`; ttr in
    `[ttr_lo, ttr_secs)` where `ttr_lo = max(min_ttr_secs, 1)` and the TTR reference
    is `end_date_unix` (scheduled_only) or `COALESCE(end_date_unix, resolved_at_unix)`;
    market resolved (winning_outcome_id NOT NULL); price `TRY_CAST`-able and
    `0 < p < 1` and inside `[price_min, price_max]`. `payoff` = `1.0` iff the bought
    `outcome_id` equals `winning_outcome_id` (integer equality).

    The universe is registered as a typed relation and INNER-joined, so the DuckDB
    universe is identical to the SQLite `wallets` list by construction (case/validation
    quirks can't diverge). First-buy dedup is a hash `GROUP BY ... arg_min(struct_pack,
    <key>)` (NOT a `ROW_NUMBER` window) so it spills to disk instead of OOM-ing on
    the full-universe scan (#387); `struct_pack` keeps the chosen row's columns atomic.
    The arg_min key is `(timestamp_unix, source_trade_id)` (#530 Phase C) with the id
    encoded as four fixed-width UBIGINT hex slices — see the fb0 comment for why — which
    requires every id to be `0x` + 64 lowercase hex; `_assert_slice_key_premise` fails
    the extract loudly if the tape ever violates that.

    When `materialize_as` is set (a bare SQL identifier), the result is written to a DuckDB temp
    TABLE of that name and the function returns `None` instead of a pandas DataFrame — the Phase-A
    out-of-core path so the caller can LEFT JOIN further columns in DuckDB before pulling the frame
    to pandas once (issue #468 follow-up). Default `None` keeps the pandas-DataFrame return.
    """
    import pandas as pd

    _assert_slice_key_premise(con)
    # Explicit object dtype: pandas 3.0's default `str` dtype is not recognised by
    # duckdb's pandas scan; object -> VARCHAR is the supported path.
    con.register("universe",
                 pd.DataFrame({"wallet_hex": pd.Series(list(wallets), dtype=object)}))
    ref = "s.end_date_unix" if scheduled_only else "COALESCE(s.end_date_unix, r.resolved_at_unix)"
    try:
        sql = f"""
        WITH fb0 AS (
            -- First buy per (wallet, market) via a HASH GROUP BY, NOT a ROW_NUMBER window:
            -- at full-universe scale u_buys is ~every buy-trade and the window's full sort
            -- OOMs (#387), whereas DuckDB spills hash aggregates to disk. arg_min over a
            -- struct_pack keeps the picked outcome_id/price_str/contracts ATOMIC (all from the
            -- one min-key row) — matching the window's single-row pick. #530 Phase C: the
            -- arg_min KEY is (timestamp_unix, source_trade_id) — struct comparison is
            -- lexicographic by field order — so an exact timestamp tie resolves by the unique
            -- trade id, deterministically and identically to the SQLite engine's ORDER BY.
            -- The id rides as four fixed-width UBIGINT hex slices, NOT a VARCHAR: DuckDB's
            -- string aggregate state cannot spill, and a `tid := source_trade_id` key OOMs
            -- the production envelope on the real tape (152M rows / 39.1M groups @ 8GB,
            -- duckdb 1.4.5 — measured on forge, #530). For ids that are uniformly `0x` +
            -- 64 lowercase hex (asserted above) the slice tuple orders EXACTLY like the
            -- SQLite engine's TEXT comparison: equal length, and hex-digit ASCII order
            -- matches numeric order.
            SELECT t.wallet_hex, t.market_id,
                   min(t.timestamp_unix) AS timestamp_unix,
                   arg_min(struct_pack(outcome_id := t.outcome_id,
                                       price_str  := t.price_str,
                                       contracts  := t.contracts),
                           struct_pack(ts := t.timestamp_unix,
                                       h1 := ('0x' || substr(t.source_trade_id,  3, 16))::UBIGINT,
                                       h2 := ('0x' || substr(t.source_trade_id, 19, 16))::UBIGINT,
                                       h3 := ('0x' || substr(t.source_trade_id, 35, 16))::UBIGINT,
                                       h4 := ('0x' || substr(t.source_trade_id, 51, 16))::UBIGINT)
                           ) AS firstbuy
            FROM trades t
            JOIN universe u ON u.wallet_hex = t.wallet_hex
            WHERE t.side = 'buy'
            GROUP BY t.wallet_hex, t.market_id
        ),
        fb AS (
            SELECT wallet_hex, market_id, timestamp_unix,
                   firstbuy.outcome_id AS outcome_id,
                   firstbuy.price_str  AS price_str,
                   firstbuy.contracts  AS contracts
            FROM fb0
        )
        SELECT fb.wallet_hex                                   AS wallet,
               fb.market_id                                   AS market_id,
               fb.outcome_id                                  AS outcome_id,
               fb.timestamp_unix                              AS entry_ts,
               CAST({ref} - fb.timestamp_unix AS BIGINT)      AS ttr_secs,
               TRY_CAST(fb.price_str AS DOUBLE)               AS price,
               fb.contracts                                   AS contracts,
               CASE WHEN fb.outcome_id = r.winning_outcome_id THEN 1.0 ELSE 0.0 END AS payoff,
               r.resolved_at_unix                             AS resolved_at
        FROM fb
        JOIN market_resolutions r
          ON r.market_id = fb.market_id AND r.winning_outcome_id IS NOT NULL
        LEFT JOIN market_schedules s
          ON s.market_id = fb.market_id AND s.end_date_unix IS NOT NULL
        WHERE fb.timestamp_unix >= ? AND fb.timestamp_unix < ?
          AND {ref} IS NOT NULL
          AND ({ref} - fb.timestamp_unix) >= ? AND ({ref} - fb.timestamp_unix) < ?
          AND TRY_CAST(fb.price_str AS DOUBLE) IS NOT NULL
          AND TRY_CAST(fb.price_str AS DOUBLE) > 0 AND TRY_CAST(fb.price_str AS DOUBLE) < 1
          AND TRY_CAST(fb.price_str AS DOUBLE) >= ? AND TRY_CAST(fb.price_str AS DOUBLE) <= ?
        """
        params = [win_start, win_end, ttr_lo, ttr_secs, price_min, price_max]
        if materialize_as is not None:
            # Out-of-core path (Phase-A memory): CREATE the result as a DuckDB temp TABLE instead of
            # pulling it into pandas. DuckDB spills to disk under its `memory_limit`, so the
            # full-universe extract never builds a multi-GB pandas frame here; the caller then joins
            # the CLV close columns in DuckDB and `.df()`s the finished frame ONCE. Returns None.
            if not materialize_as.isidentifier():
                raise ValueError(f"materialize_as must be a bare identifier, got {materialize_as!r}")
            con.execute(f"CREATE OR REPLACE TEMP TABLE {materialize_as} AS {sql}", params)
            df = None
        else:
            df = con.execute(sql, params).df()
    finally:
        con.unregister("universe")
    return df


def duck_extract_positions_v2(con, wallets, win_start, win_end):
    """Return every structurally valid Rust-classified schema-two first buy.

    No leader-price, scheduled-horizon, price-band, or statistical filter is
    applied here. Those eligibility decisions belong to pass two after the
    minute-price approximation is selected.
    """
    import pandas as pd

    con.register(
        "universe",
        pd.DataFrame({"wallet_hex": pd.Series(list(wallets), dtype=object)}),
    )
    try:
        bad = con.execute(
            "SELECT COUNT(*) FROM ranker_entries_v2 r "
            "LEFT JOIN activity_groups_v2 g "
            "ON g.source_trade_id = r.source_trade_id "
            "AND g.coverage_generation = r.activity_generation "
            "LEFT JOIN clob_payout_evidence_v2 p ON p.market_id = g.condition_id "
            "WHERE NOT regexp_full_match(r.source_trade_id, 'g2:[0-9a-f]{64}') "
            "OR g.source_trade_id IS NULL OR p.market_id IS NULL OR g.condition_id IS NULL "
            "OR g.asset IS NULL OR g.outcome_id IS NULL OR g.side != 'buy' "
            "OR TRY_CAST(g.share_amount_str AS DECIMAL(38,6)) IS NULL "
            "OR TRY_CAST(g.share_amount_str AS DECIMAL(38,6)) <= 0 "
            "OR TRY_CAST(g.price_weighted_share_amount_str AS DECIMAL(38,12)) IS NULL "
            "OR p.end_date_unix IS NULL OR p.payout_status != 'resolved' "
            "OR p.payout_vector_json NOT IN ('[\"1\",\"0\"]','[\"0\",\"1\"]',"
            "'[\"0.5\",\"0.5\"]')"
        ).fetchone()[0]
        if bad:
            raise RuntimeError(
                f"schema-two projection contains {bad} structurally invalid row(s)"
            )
        return con.execute(
            "SELECT g.wallet_hex AS wallet, g.condition_id AS market_id, "
            "g.outcome_id, g.source_time_unix AS entry_ts, "
            "CAST(p.end_date_unix - g.source_time_unix AS BIGINT) AS ttr_secs, "
            "CAST(TRY_CAST(g.price_weighted_share_amount_str AS DECIMAL(38,12)) / "
            "TRY_CAST(g.share_amount_str AS DECIMAL(38,6)) AS DOUBLE) AS price, "
            "g.share_amount_str AS contracts, "
            "CAST(json_extract_string(p.payout_vector_json, "
            "'$[' || CAST(g.outcome_id AS VARCHAR) || ']') AS DOUBLE) AS payoff, "
            "p.end_date_unix AS resolved_at "
            "FROM ranker_entries_v2 r "
            "JOIN activity_groups_v2 g ON g.source_trade_id = r.source_trade_id "
            "AND g.coverage_generation = r.activity_generation "
            "JOIN clob_payout_evidence_v2 p ON p.market_id = g.condition_id "
            "JOIN universe u ON u.wallet_hex = g.wallet_hex "
            "WHERE g.source_time_unix >= ? AND g.source_time_unix < ? "
            "ORDER BY g.wallet_hex, g.source_time_unix, r.source_trade_id",
            [win_start, win_end],
        ).df()
    finally:
        con.unregister("universe")
