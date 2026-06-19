#!/usr/bin/env python3
"""Export the ranker's read tables from SQLite to Parquet for the DuckDB read-layer
(issue #375).

FULL ATOMIC REWRITE each run: `trades` + `market_resolutions` + `market_schedules`
-> zstd Parquet under `--out-dir` (default data/parquet), via DuckDB's
`sqlite_scanner`. Each file is written to `<name>.parquet.tmp` then `os.replace`-d
into place, so a concurrent reader never sees a half-written file.

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
TABLES = ("trades", "market_resolutions", "market_schedules")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] export_parquet: {msg}", flush=True)


def _q(s: str) -> str:
    return s.replace("'", "''")


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
        final = os.path.join(a.out_dir, f"{tbl}.parquet")
        tmp = final + ".tmp"
        con.execute(
            f"COPY (SELECT * FROM src.{tbl}) TO '{_q(tmp)}' "
            f"(FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {int(a.row_group_size)});"
        )
        os.replace(tmp, final)
        n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_q(final)}')"
        ).fetchone()[0]
        size_gb = os.path.getsize(final) / 1e9
        log(f"{tbl}: {n:,} rows -> {final} ({size_gb:.2f} GB)")

    log(f"export complete in {time.time() - t0:.0f}s -> {a.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
