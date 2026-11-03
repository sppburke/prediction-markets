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
    group_a, group_b, shared = ["0x" + hashlib.sha256((suffix + name).encode()).hexdigest()
                                for name in ("a", "b", "shared")]
    current = {**batch, "classifier_version": 6, "ttr_floor_secs": 10, "latency_shift_secs": 2,
               "neg_risk_group_ids": sorted([group_a, shared])}
    entries = [{**entry, "history_through_unix": 1700000010} for entry in entries]
    drops = [{"wallet_hex": "0xaaa", "scope_kind": "event", "scope_id": "0xa",
              "dropped_at_unix": 1700000000, "cause": "conversion"},
             {"wallet_hex": "0xbbb", "scope_kind": "market", "scope_id": "0xb",
              "dropped_at_unix": 1700000001, "cause": "underflow"}]
    batch_id = -1
    prefix = "ranking-v2-test-" + suffix
    keys = [hashlib.sha256((prefix + str(i)).encode()).hexdigest() for i in range(16)]
    def call(key, scope_drops, entry_rows=entries, batch_row=current):
        return ("select publish_ranking_batch_v2("
                f"'{key}', '{sql_json(batch_row)}'::jsonb, '{sql_json(entry_rows)}'::jsonb, "
                f"'{sql_json(scope_drops)}'::jsonb);")

    def spans():
        return psql(url, "select group_id, from_batch_id, to_batch_id "
                    "from ranking_neg_risk_group_spans order by group_id, from_batch_id;").stdout

    def assert_groups(at_batch, expected):
        stored = psql(url, "select group_id from ranking_neg_risk_group_spans "
                      f"where from_batch_id <= {at_batch} "
                      f"and (to_batch_id is null or {at_batch} < to_batch_id) "
                      "order by group_id;").stdout.split()
        if stored != sorted(expected):
            raise AssertionError(f"batch {at_batch} groups {stored!r} != {sorted(expected)!r}")

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
        counts = psql(url, "select "
                      f"(select count(*) from ranking_batches where publish_key='{keys[0]}'), "
                      f"(select count(*) from ranking_entries where batch_id={batch_id}), "
                      f"(select count(*) from ranking_scope_drops where batch_id={batch_id}), "
                      "(select count(*) from ranking_neg_risk_group_spans);").stdout.strip()
        if counts != f"1|{len(entries)}|{len(drops)}|2":
            raise AssertionError(f"v2 concurrent retry left duplicate or partial rows: {counts}")
        assert_groups(old_batch, [])
        assert_groups(batch_id, current["neg_risk_group_ids"])
        stored = psql(url, f"select classifier_version, ttr_floor_secs, latency_shift_secs from ranking_batches where batch_id={batch_id};").stdout.strip()
        if stored != "6|10|2":
            raise AssertionError(f"v2 batch metadata mismatch: {stored}")
        through = psql(url, f"select history_through_unix from ranking_entries where batch_id={batch_id} order by rank;").stdout.split()
        if through != ["1700000010"] * len(entries):
            raise AssertionError(f"v2 coverage ends mismatch: {through}")
        anon = psql(url, f"set role anon; select count(*) from ranking_scope_drops where batch_id={batch_id};").stdout.splitlines()[-1]
        if anon != "2":
            raise AssertionError("anon cannot read scope drops")
        anon_groups = psql(url, "set role anon; select count(*) from ranking_neg_risk_group_spans;").stdout.splitlines()[-1]
        if anon_groups != "2":
            raise AssertionError("anon cannot read group spans")
        # #822: classifier 7 publishes the new cause without changing the RPC shape.
        anomaly_drop = {**drops[0], "cause": "unrepresentable"}
        anomaly_id = int(psql(url, call(keys[14], [anomaly_drop],
                                       batch_row={**current, "classifier_version": 7})).stdout.strip())
        stored_anomaly = psql(url, "select b.classifier_version, d.wallet_hex, d.scope_kind, d.scope_id, "
                             "d.dropped_at_unix, d.cause from ranking_batches b "
                             "join ranking_scope_drops d using (batch_id) "
                             f"where b.batch_id={anomaly_id};").stdout.strip()
        if stored_anomaly != "7|0xaaa|event|0xa|1700000000|unrepresentable":
            raise AssertionError(f"unrepresentable drop did not round-trip: {stored_anomaly}")
        privileges = psql(url, "select has_function_privilege('anon', 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)', 'EXECUTE'), "
                         "has_function_privilege('authenticated', 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)', 'EXECUTE'), "
                         "has_function_privilege('service_role', 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)', 'EXECUTE');").stdout.strip()
        if privileges != "f|f|t":
            raise AssertionError(f"v2 execute privilege mismatch: {privileges}")
        settings = psql(url, "select proconfig from pg_proc "
                        "where oid = 'publish_ranking_batch_v2(text,jsonb,jsonb,jsonb)'::regprocedure;").stdout.strip()
        if settings != "{statement_timeout=120s}":
            raise AssertionError(f"v2 statement timeout mismatch: {settings}")
        failures = (
            [{**drops[0], "cause": "bad-cause"}],
            [drops[0], drops[0]],
            [{**drops[0], "wallet_hex": "0xforeign"}],
            [{**drops[0], "scope_kind": "wallet"}],
        )
        for key, bad_drops in zip(keys[1:5], failures, strict=True):
            before = spans()
            failed = psql(url, call(key, bad_drops, batch_row={**current, "classifier_version": 7}), check=False)
            if failed.returncode == 0:
                raise AssertionError(f"invalid v2 drops accepted: {bad_drops}")
            counts = psql(url, "select "
                          f"(select count(*) from ranking_batches where publish_key='{key}'), "
                          "(select count(*) from ranking_entries e join ranking_batches b using (batch_id) "
                          f"where b.publish_key='{key}'), "
                          "(select count(*) from ranking_scope_drops d join ranking_batches b using (batch_id) "
                          f"where b.publish_key='{key}');").stdout.strip()
            if counts != "0|0|0":
                raise AssertionError("failing drop did not roll back batch and entries")
            if spans() != before:
                raise AssertionError("failing drop changed group spans")
        # Retry counts remain enforced for both inherited entries and the extension's drops.
        for scope_drops, entry_rows in (([], entries), (drops, entries[:1])):
            if psql(url, call(keys[0], scope_drops, entry_rows), check=False).returncode == 0:
                raise AssertionError("v2 retry count mismatch accepted")
        no_end = [{**entry, "history_through_unix": None} for entry in entries]
        if psql(url, call(keys[5], drops, no_end), check=False).returncode == 0:
            raise AssertionError("v2 missing coverage end accepted")

        next_batch = {**current, "neg_risk_group_ids": sorted([group_b, shared])}
        batch_b = int(psql(url, call(keys[6], drops, batch_row=next_batch)).stdout.strip())
        if batch_b <= batch_id:
            raise AssertionError("new publication did not get a newer batch id")
        assert_groups(batch_id, current["neg_risk_group_ids"])
        assert_groups(batch_b, next_batch["neg_risk_group_ids"])
        expected_spans = sorted([(group_a, batch_id, batch_b), (shared, batch_id, None),
                                 (group_b, batch_b, None)])
        rendered = "".join(f"{group}|{start}|{'' if end is None else end}\n"
                           for group, start, end in expected_spans)
        if spans() != rendered:
            raise AssertionError("A -> B did not close only the removed group and retain the shared span")
        before = spans()
        if psql(url, call(keys[0], drops)).stdout.strip() != str(batch_id) or spans() != before:
            raise AssertionError("retry of A after B changed group spans or returned a new batch")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            retries = list(executor.map(lambda _: psql(url, call(keys[0], drops)).stdout.strip(), range(2)))
        if retries != [str(batch_id)] * 2 or spans() != before:
            raise AssertionError("concurrent retries of A after B changed spans or batch id")
        assert_groups(batch_id, current["neg_risk_group_ids"])
        assert_groups(batch_b, next_batch["neg_risk_group_ids"])

        without_groups = dict(current)
        del without_groups["neg_risk_group_ids"]
        legacy_id = int(psql(url, call(keys[7], drops, batch_row=without_groups)).stdout.strip())
        if spans() != before:
            raise AssertionError("publication without group key changed spans")
        assert_groups(legacy_id, next_batch["neg_risk_group_ids"])

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            publications = [(keys[8], current), (keys[9], next_batch)]
            ids = list(executor.map(
                lambda item: int(psql(url, call(item[0], drops, batch_row=item[1])).stdout.strip()),
                publications,
            ))
        if len(set(ids)) != 2 or min(ids) <= legacy_id:
            raise AssertionError(f"distinct concurrent publications returned unordered/reused ids: {ids}")
        for published_id, (_, payload) in zip(ids, publications, strict=True):
            assert_groups(published_id, payload["neg_risk_group_ids"])
        assert_groups(batch_id, current["neg_risk_group_ids"])
        assert_groups(batch_b, next_batch["neg_risk_group_ids"])
        print("PASS: A, B, retry A preserve exact sets; concurrent same-key and distinct publications converge")

        # This failure occurs after closing spans, proving those writes roll back too.
        for key, groups in ((keys[12], [None]), (keys[13], "not-an-array")):
            before = spans()
            failed = psql(url, call(key, drops, batch_row={**current, "neg_risk_group_ids": groups}), check=False)
            if failed.returncode == 0:
                raise AssertionError(f"invalid group list accepted: {groups!r}")
            count = psql(url, f"select count(*) from ranking_batches where publish_key='{key}';").stdout.strip()
            if count != "0" or spans() != before:
                raise AssertionError("failing group publication left a batch or changed spans")

        empty_id = int(psql(url, call(keys[10], drops, batch_row={**current, "neg_risk_group_ids": []})).stdout.strip())
        assert_groups(empty_id, [])
        reopened_id = int(psql(url, call(keys[11], drops)).stdout.strip())
        assert_groups(reopened_id, current["neg_risk_group_ids"])
        assert_groups(empty_id, [])
        assert_groups(batch_id, current["neg_risk_group_ids"])
        before = spans()
        psql(url, f"delete from ranking_batches where batch_id in ({batch_id}, {batch_b});")
        if spans() != before:
            raise AssertionError("pruning batches changed their group spans")
        assert_groups(batch_id, current["neg_risk_group_ids"])
        assert_groups(batch_b, next_batch["neg_risk_group_ids"])
        print("PASS: group failures roll back atomically; empty sets, reopening, pruning and anon reads work")
        print("PASS: v2 drops atomic; retries converge; counts checked; old classifier NULL; anon reads drops")
        print("PASS: classifier 7 unrepresentable drop persists; unknown causes roll back atomically")
    finally:
        rendered = ",".join("'" + key + "'" for key in keys)
        psql(url, f"delete from ranking_batches where publish_key in ({rendered});", check=False)
        if psql(url, f"select count(*) from ranking_scope_drops where batch_id={batch_id};").stdout.strip() != "0":
            raise AssertionError("batch delete did not cascade to drops")
        group_ids = ",".join("'" + group + "'" for group in (group_a, group_b, shared))
        psql(url, f"delete from ranking_neg_risk_group_spans where group_id in ({group_ids});", check=False)


if __name__ == "__main__":
    raise SystemExit(main())
