#!/usr/bin/env python3
"""Export/read one #544 payout row for the Rust cross-reader scenario.

The chain is SQLite -> Parquet export -> DuckDB view -> canonical Python reader.
Only the canonical vector is printed on the final line; the Rust caller compares
those bytes with its typed `BinaryPayoutVector`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_trades_parquet as export  # noqa: E402
import ranker_duck  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--market-id", required=True)
    args = parser.parse_args()

    old_argv = sys.argv
    try:
        sys.argv = ["export_trades_parquet.py", "--db", args.db, "--out-dir", args.out_dir]
        if export.main() != 0:
            return 1
    finally:
        sys.argv = old_argv

    con = ranker_duck.get_engine(force="duck", parquet_dir=args.out_dir, max_age_hours=0)
    row = ranker_duck.load_clob_payout_evidence_v2(con, args.market_id)
    if row is None or row.payout_vector_json is None:
        raise RuntimeError(f"no resolved v2 payout row for {args.market_id}")
    print(f"CANONICAL_VECTOR={row.payout_vector_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
