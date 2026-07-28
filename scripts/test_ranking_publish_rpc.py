#!/usr/bin/env python3
"""Live-Postgres contract test for the atomic ranking publication RPC.

CI loads ``scripts/supabase_schema.sql`` first and supplies ``PE_TEST_PG_URL``.
Without that environment variable the test self-skips, matching the existing
authoritative paper-state RPC harness.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import uuid


def psql(url: str, sql: str, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", url, "-v", "ON_ERROR_STOP=1", "-AtX", "-c", sql],
        check=check,
        capture_output=True,
        text=True,
    )


def sql_json(value) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).replace("'", "''")


def main() -> int:
    url = os.environ.get("PE_TEST_PG_URL")
    if not url:
        print("SKIP: PE_TEST_PG_URL unset — ranking RPC test runs against Postgres in CI")
        return 0
    if shutil.which("psql") is None:
        print("FAIL: psql is required when PE_TEST_PG_URL is set", file=sys.stderr)
        return 1

    suffix = uuid.uuid4().hex
    publish_key = suffix + suffix
    bad_key = ("f" * 32) + suffix
    batch = {
        "git_sha": "rpc-test",
        "band_lo": 0.15,
        "band_hi": 0.85,
        "ttr_floor_secs": 30,
        "ttr_max_secs": 172800,
        "latency_shift_secs": 20,
        "universe_size": 2,
        "notes": "ranking RPC contract test",
    }
    entries = [
        {
            "rank": 1,
            "wallet_hex": "0xaaa",
            "ls_edge": "0.1",
            "ls_tstat": "2.5",
            "fill_rate": "0.8",
            "n_trades": 20,
            "hit_rate": "0.6",
            "avg_price": "0.4",
            "last_trade_unix": 1700000000,
        },
        {
            "rank": 2,
            "wallet_hex": "0xbbb",
            "ls_edge": None,
            "ls_tstat": None,
            "fill_rate": None,
            "n_trades": None,
            "hit_rate": None,
            "avg_price": None,
            "last_trade_unix": None,
        },
    ]
    call = (
        "select publish_ranking_batch("
        f"'{publish_key}', '{sql_json(batch)}'::jsonb, "
        f"'{sql_json(entries)}'::jsonb);"
    )

    try:
        # Two concurrent ambiguous/repeated requests must converge to one complete epoch.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: psql(url, call).stdout.strip(), range(2)))
        if len(set(results)) != 1 or not results[0].isdigit():
            raise AssertionError(f"concurrent calls returned different batch ids: {results}")
        batch_id = int(results[0])

        counts = psql(
            url,
            "select "
            f"(select count(*) from ranking_batches where publish_key = '{publish_key}'),"
            f"(select count(*) from ranking_entries where batch_id = {batch_id}),"
            f"(select count(*) from latest_ranking where batch_id = {batch_id});",
        ).stdout.strip()
        if counts != "1|2|2":
            raise AssertionError(f"expected one complete latest batch, got {counts!r}")

        # A malformed request must leave no visible batch row.
        bad_batch = dict(batch)
        bad_batch["ttr_max_secs"] = "not-an-integer"
        failed = psql(
            url,
            "select publish_ranking_batch("
            f"'{bad_key}', '{sql_json(bad_batch)}'::jsonb, "
            f"'{sql_json(entries)}'::jsonb);",
            check=False,
        )
        if failed.returncode == 0:
            raise AssertionError("malformed publication unexpectedly succeeded")
        rolled_back = psql(
            url,
            f"select count(*) from ranking_batches where publish_key = '{bad_key}';",
        ).stdout.strip()
        if rolled_back != "0":
            raise AssertionError("failed publication left a partial batch")
    finally:
        psql(
            url,
            "delete from ranking_batches "
            f"where publish_key in ('{publish_key}', '{bad_key}');",
            check=False,
        )

    print("PASS: concurrent retries converge; publication is complete and rollback-atomic")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
