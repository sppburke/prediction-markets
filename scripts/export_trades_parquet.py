#!/usr/bin/env python3
"""Export the ranker's read tables from SQLite to Parquet for the DuckDB read-layer
(issue #375).

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
import os
import sys
import time

# The three tables the ranker reads. Order is irrelevant (independent files).
# `market_schedules` now also carries `start_date_unix` (issue #421 PR4); it rides along free via
# `SELECT *`, so no change is needed here for that column.
TABLES = ("trades", "market_resolutions", "market_schedules")
# OPTIONAL tables (issue #421 PR4 — the CLV price series; #429 PR4 — its token→outcome map).
# Absent on a pre-migration cache (the export attaches READ_ONLY and does not run schema), so each
# is skipped with a warning rather than aborting the whole export. `ranker_duck.py` registers each
# view conditionally to match. `true_clv` needs both `market_price_history` and `token_conditions`.
OPTIONAL_TABLES = ("market_price_history", "token_conditions", "clob_payout_evidence_v2")


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


def _export_table(con, out_dir: str, tbl: str, row_group_size: int) -> None:
    """Atomically export `src.{tbl}` to `{out_dir}/{tbl}.parquet` (zstd) via tmp + os.replace."""
    final = os.path.join(out_dir, f"{tbl}.parquet")
    tmp = final + ".tmp"
    con.execute(
        f"COPY (SELECT * FROM src.{tbl}) TO '{_q(tmp)}' "
        f"(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {int(row_group_size)});"
    )
    os.replace(tmp, final)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_q(final)}')").fetchone()[0]
    size_gb = os.path.getsize(final) / 1e9
    log(f"{tbl}: {n:,} rows -> {final} ({size_gb:.2f} GB)")


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

    t0 = time.time()
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
