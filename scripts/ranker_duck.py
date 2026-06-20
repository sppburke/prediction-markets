#!/usr/bin/env python3
"""DuckDB read-layer for the 72hr ranker passes (issue #375).

An OPTIONAL accelerator over the SQLite system-of-record. The two heavy ranker
passes scan the ~269M-row `trades` table; this module runs that scan + first-buy
dedup + the high-selectivity filters in DuckDB over a Parquet snapshot
(`export_trades_parquet.py`), returning the SAME qualifying rows / price tapes the
SQLite path would produce, so ALL scoring stays in shared Python — one
`eff`/`gross`/`net` + `weighted_stats` code path, hence bit-parity.

`get_engine()` is the single decision point: it returns a configured DuckDB
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

import os
import time

# Parquet snapshot file names (one per ranker read table).
TRADES_PARQUET = "trades.parquet"
RESOLUTIONS_PARQUET = "market_resolutions.parquet"
SCHEDULES_PARQUET = "market_schedules.parquet"
REQUIRED_PARQUET = (TRADES_PARQUET, RESOLUTIONS_PARQUET, SCHEDULES_PARQUET)

DEFAULT_PARQUET_DIR = "data/parquet"
DEFAULT_MAX_AGE_HOURS = 4.0


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] ranker_duck: {msg}", flush=True)


def _q(s: str) -> str:
    """Single-quote-escape a string for embedding in a DuckDB SQL literal."""
    return s.replace("'", "''")


def _snapshot_state(parquet_dir: str, max_age_hours: float) -> tuple[bool, str]:
    """Return (usable, reason). Usable iff every required Parquet file exists and —
    when `max_age_hours > 0` — the oldest is younger than that bound."""
    now = time.time()
    for name in REQUIRED_PARQUET:
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
               max_age_hours: float | None = None):
    """Return a configured DuckDB connection over the Parquet snapshot, or `None` to
    fall back to the SQLite path.

    `force` (or `PE_RANKER_ENGINE`): "sqlite" -> always None; "duck" -> require the
    snapshot (raise if absent, age ignored); "auto"/None -> use it only if duckdb is
    importable and the snapshot is fresh, else None. Never raises on the auto path.
    """
    force, parquet_dir, max_age_hours = engine_settings(force, parquet_dir, max_age_hours)

    if force == "sqlite":
        log("engine=sqlite (forced)")
        return None

    try:
        import duckdb
    except ImportError:
        if force == "duck":
            raise
        log("duckdb not importable -> SQLite path")
        return None

    if force == "duck":
        usable, reason = _snapshot_state(parquet_dir, 0.0)  # forced: existence only
        if not usable:
            raise FileNotFoundError(
                f"--engine duck but Parquet snapshot unusable: {reason} (run export_trades_parquet.py)"
            )
    else:
        usable, reason = _snapshot_state(parquet_dir, max_age_hours)
        if not usable:
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
    # much larger PE_RANKER_DUCKDB_MEMORY_LIMIT) (#387).
    threads = os.environ.get("PE_RANKER_DUCKDB_THREADS", "4")
    if threads.strip() not in ("", "0"):
        con.execute(f"SET threads={int(threads)};")
    tmp = os.path.join(parquet_dir, ".duckdb_tmp")
    os.makedirs(tmp, exist_ok=True)
    con.execute(f"SET temp_directory='{_q(tmp)}';")
    # Views over the Parquet snapshot — typed by the export (BIGINT/VARCHAR), so
    # outcome_id/contracts are BIGINT and price_str is VARCHAR, matching the contract.
    for tbl, name in (
        ("trades", TRADES_PARQUET),
        ("market_resolutions", RESOLUTIONS_PARQUET),
        ("market_schedules", SCHEDULES_PARQUET),
    ):
        path = _q(os.path.join(parquet_dir, name))
        con.execute(f"CREATE VIEW {tbl} AS SELECT * FROM read_parquet('{path}');")
    log(f"engine=duck over {parquet_dir} (memory_limit={mem}, threads={threads or 'default'})")
    return con


def duck_extract_positions(con, wallets, win_start, win_end, ttr_lo, ttr_secs,
                           scheduled_only, price_min, price_max):
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
    timestamp_unix)` (NOT a `ROW_NUMBER` window) so it spills to disk instead of OOM-ing on
    the full-universe scan (#387); `struct_pack` keeps the chosen row's columns atomic.
    """
    import pandas as pd

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
            -- one min-timestamp row) — matching the window's single-row pick; an exact tie on
            -- timestamp resolves arbitrarily in both engines (documented sub-1e-9 residual).
            SELECT t.wallet_hex, t.market_id,
                   min(t.timestamp_unix) AS timestamp_unix,
                   arg_min(struct_pack(outcome_id := t.outcome_id,
                                       price_str  := t.price_str,
                                       contracts  := t.contracts),
                           t.timestamp_unix) AS firstbuy
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
        df = con.execute(sql, [win_start, win_end, ttr_lo, ttr_secs, price_min, price_max]).df()
    finally:
        con.unregister("universe")
    return df


def duck_load_tapes(con, mo_keys):
    """Return `{(market_id, outcome_id_str): (ts_list, price_str_list)}` for the given
    candidate `(market_id, outcome_id_str)` pairs — each tape is every trade in that
    (market, outcome) ordered by `timestamp_unix`, exactly like the SQLite per-pair
    query. Price is returned as the RAW string so the caller's `float()` + `0<p<1`
    fill logic is byte-identical.

    The keys are registered as a typed relation (`outcome_id` cast to BIGINT) and
    joined, so the VARCHAR-from-CSV key matches the BIGINT Parquet column (DuckDB does
    not implicitly coerce VARCHAR<->BIGINT the way SQLite does).
    """
    if not mo_keys:
        return {}
    import pandas as pd

    # Explicit dtypes: market_id -> VARCHAR (object; pandas 3.0's `str` dtype is
    # rejected by duckdb's pandas scan), outcome_id -> BIGINT, so the join matches the
    # Parquet column types (DuckDB won't implicitly coerce VARCHAR<->BIGINT).
    keys_df = pd.DataFrame({
        "market_id": pd.Series([str(m) for (m, o) in mo_keys], dtype=object),
        "outcome_id": pd.Series([int(o) for (m, o) in mo_keys], dtype="int64"),
    })
    con.register("cand_keys", keys_df)
    try:
        rows = con.execute(
            """
            SELECT t.market_id, t.outcome_id, t.timestamp_unix, t.price_str
            FROM trades t
            JOIN cand_keys k
              ON t.market_id = k.market_id AND t.outcome_id = k.outcome_id
            ORDER BY t.market_id, t.outcome_id, t.timestamp_unix
            """
        ).fetchall()
    finally:
        con.unregister("cand_keys")

    out: dict[tuple[str, str], tuple[list, list]] = {}
    for mid, oid, ts, px in rows:
        key = (mid, str(oid))
        bucket = out.get(key)
        if bucket is None:
            bucket = ([], [])
            out[key] = bucket
        bucket[0].append(ts)
        bucket[1].append(px)
    return out
