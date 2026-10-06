#!/usr/bin/env python3
"""Live-Postgres contract test for the atomic ranking publication RPC.

CI loads ``scripts/supabase_schema.sql`` first and supplies ``PE_TEST_PG_URL``.
Without that environment variable the test self-skips, matching the existing
authoritative paper-state RPC harness.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
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
            "survives": True,
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
            "survives": False,
        },
        # #518: a legacy durable request predates the verdict key entirely. `jsonb_to_recordset`
        # null-fills the absent column, so the row stores SQL NULL and — being neither true nor
        # false — can never authorize live admission through the fail-closed reader filter.
        {
            "rank": 3,
            "wallet_hex": "0xccc",
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
        if counts != "1|3|3":
            raise AssertionError(f"expected one complete latest batch, got {counts!r}")

        # #518: the ranker verdict round-trips through the RPC exactly — true, false, and an
        # absent key as SQL NULL. This is the only proof that the publication path persists the
        # column, so it must run against a real Postgres (this script SKIPs without one).
        verdicts = psql(
            url,
            "select coalesce(survives::text, 'NULL') from ranking_entries "
            f"where batch_id = {batch_id} order by rank;",
        ).stdout.split()
        if verdicts != ["true", "false", "NULL"]:
            raise AssertionError(
                f"expected survives = true/false/NULL by rank, got {verdicts!r}"
            )
        print("PASS: survives persists as true/false, absent key -> SQL NULL")

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

    test_v2(url, suffix, batch, entries)

    print("PASS: concurrent retries converge; publication is complete and rollback-atomic")
    return 0


def test_v2(url, suffix, batch, entries):
    current = {**batch, "classifier_version": 6, "ttr_floor_secs": 10, "latency_shift_secs": 2}
    entries = [{**entry, "history_through_unix": 1700000010} for entry in entries]
    drops = [{"wallet_hex": "0xaaa", "scope_kind": "event", "scope_id": "0xa",
              "dropped_at_unix": 1700000000, "cause": "conversion"},
             {"wallet_hex": "0xbbb", "scope_kind": "market", "scope_id": "0xb",
              "dropped_at_unix": 1700000001, "cause": "underflow"}]
    batch_id = -1
    prefix = "ranking-v2-test-" + suffix
    keys = [hashlib.sha256((prefix + str(i)).encode()).hexdigest() for i in range(8)]
    def call(key, scope_drops, entry_rows=entries):
        return ("select publish_ranking_batch_v2("
                f"'{key}', '{sql_json(current)}'::jsonb, '{sql_json(entry_rows)}'::jsonb, "
                f"'{sql_json(scope_drops)}'::jsonb);")
    try:
        legacy_key = keys[-1]
        old_batch = int(psql(url, "select publish_ranking_batch("
                             f"'{legacy_key}', '{sql_json(batch)}'::jsonb, '{sql_json(entries)}'::jsonb);").stdout.strip())
        if psql(url, f"select classifier_version is null from ranking_batches where batch_id={old_batch};").stdout.strip() != "t":
            raise AssertionError("old batch classifier_version changed")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: psql(url, call(keys[0], drops)).stdout.strip(), range(2)))
        if len(set(results)) != 1 or not results[0].isdigit():
            raise AssertionError(f"v2 same-key retry did not converge: {results}")
        batch_id = int(results[0])
        stored = psql(url, f"select classifier_version, ttr_floor_secs, latency_shift_secs from ranking_batches where batch_id={batch_id};").stdout.strip()
        if stored != "6|10|2":
            raise AssertionError(f"v2 batch metadata mismatch: {stored}")
        through = psql(url, f"select history_through_unix from ranking_entries where batch_id={batch_id} order by rank;").stdout.split()
        if through != ["1700000010"] * len(entries):
            raise AssertionError(f"v2 coverage ends mismatch: {through}")
        anon = psql(url, f"set role anon; select count(*) from ranking_scope_drops where batch_id={batch_id};").stdout.splitlines()[-1]
        if anon != "2":
            raise AssertionError("anon cannot read scope drops")
        privileges = psql(url, "select has_function_privilege('anon', 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)', 'EXECUTE'), "
                         "has_function_privilege('authenticated', 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)', 'EXECUTE'), "
                         "has_function_privilege('service_role', 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)', 'EXECUTE');").stdout.strip()
        if privileges != "f|f|t":
            raise AssertionError(f"v2 execute privilege mismatch: {privileges}")
        failures = (
            [{**drops[0], "cause": "bad-cause"}],
            [drops[0], drops[0]],
            [{**drops[0], "wallet_hex": "0xforeign"}],
            [{**drops[0], "scope_kind": "wallet"}],
        )
        for key, bad_drops in zip(keys[1:5], failures, strict=True):
            failed = psql(url, call(key, bad_drops), check=False)
            if failed.returncode == 0:
                raise AssertionError(f"invalid v2 drops accepted: {bad_drops}")
            count = psql(url, f"select count(*) from ranking_batches where publish_key='{key}';").stdout.strip()
            if count != "0":
                raise AssertionError("failing drop did not roll back batch and entries")
        # Retry counts remain enforced for both inherited entries and the extension's drops.
        for scope_drops, entry_rows in (([], entries), (drops, entries[:1])):
            if psql(url, call(keys[0], scope_drops, entry_rows), check=False).returncode == 0:
                raise AssertionError("v2 retry count mismatch accepted")
        no_end = [{**entry, "history_through_unix": None} for entry in entries]
        if psql(url, call(keys[5], drops, no_end), check=False).returncode == 0:
            raise AssertionError("v2 missing coverage end accepted")
        print("PASS: v2 drops atomic; retries converge; counts checked; old classifier NULL; anon reads drops")
    finally:
        rendered = ",".join("'" + key + "'" for key in keys)
        psql(url, f"delete from ranking_batches where publish_key in ({rendered});", check=False)
        if psql(url, f"select count(*) from ranking_scope_drops where batch_id={batch_id};").stdout.strip() != "0":
            raise AssertionError("batch delete did not cascade to drops")


if __name__ == "__main__":
    raise SystemExit(main())
