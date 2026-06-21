#!/usr/bin/env python3
"""Behaviour + drift guard for the active-only upload filter in
`scripts/push_ranking_to_supabase.py` (issue #350 WS3).

Stdlib-only (`unittest` + `sqlite3` + `tempfile`); builds a throwaway `trades` cache and
asserts the recency filter and the stale-cache abort behave as specified. Also pins the
argparse defaults to the `docs/_GLOSSARY.md` values so a silent drift fails CI.

This test runs in CI as part of `.github/workflows/ci.yml` (Python drift-guard step).

Run: `python3 scripts/test_push_ranking_filter.py`
  or: `pytest scripts/test_push_ranking_filter.py -v`
"""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

# Import the push script as a module (its work is guarded behind `if __name__`).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import push_ranking_to_supabase as pr  # noqa: E402

HOUR = 3600
NOW = 1_700_000_000  # fixed clock — determinism (no time.time() in assertions)


def _make_cache(path: str, trades: list[tuple[str, int]]) -> None:
    """Build a minimal `trades` cache. `trades` is a list of (wallet_hex, timestamp_unix);
    mirrors the production schema's column set (only the two columns the filter reads)."""
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE trades (source_trade_id TEXT, wallet_hex TEXT, market_id TEXT, "
        "outcome_id TEXT, side TEXT, price_str TEXT, contracts TEXT, timestamp_unix INTEGER)"
    )
    con.execute("CREATE INDEX idx_trades_wallet_ts ON trades(wallet_hex, timestamp_unix)")
    con.executemany("INSERT INTO trades (wallet_hex, timestamp_unix) VALUES (?, ?)", trades)
    con.commit()
    con.close()


class ActiveFilterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "cache.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_drops_inactive_keeps_active(self) -> None:
        _make_cache(self.db, [
            ("0xaaa", NOW - 1 * HOUR),     # fresh -> keep
            ("0xbbb", NOW - 100 * HOUR),   # idle 100h > 72h -> drop
        ])
        rows = [{"wallet": "0xaaa"}, {"wallet": "0xbbb"}]
        kept, dropped, last = pr.filter_active_rows(rows, self.db, 72, 24, NOW)
        self.assertEqual([r["wallet"] for r in kept], ["0xaaa"])
        self.assertEqual(dropped, 1)
        # both wallets have a cached trade, so both appear in the map (#357) — incl. the dropped one.
        self.assertEqual(last, {"0xaaa": NOW - 1 * HOUR, "0xbbb": NOW - 100 * HOUR})

    def test_case_insensitive_match(self) -> None:
        # DB stores lowercase; the ranked CSV may carry mixed/upper case.
        _make_cache(self.db, [("0xabc", NOW - 1 * HOUR)])
        rows = [{"wallet": "0xABC"}]
        kept, dropped, last = pr.filter_active_rows(rows, self.db, 72, 24, NOW)
        self.assertEqual(len(kept), 1)
        self.assertEqual(dropped, 0)
        self.assertEqual(last, {"0xabc": NOW - 1 * HOUR})  # map keyed by lowercase wallet

    def test_wallet_absent_from_cache_is_dropped(self) -> None:
        _make_cache(self.db, [("0xaaa", NOW - 1 * HOUR)])
        rows = [{"wallet": "0xaaa"}, {"wallet": "0xnotincache"}]
        kept, dropped, _ = pr.filter_active_rows(rows, self.db, 72, 24, NOW)
        self.assertEqual([r["wallet"] for r in kept], ["0xaaa"])
        self.assertEqual(dropped, 1)

    def test_window_boundary_is_inclusive(self) -> None:
        # A trade exactly at the cutoff (NOW - window) is kept (>=). The fresh anchor
        # wallet keeps the cache itself non-stale so the boundary case is reachable.
        _make_cache(self.db, [
            ("0xfresh", NOW - 1 * HOUR),
            ("0xaaa", NOW - 72 * HOUR),
        ])
        rows = [{"wallet": "0xaaa"}]
        kept, dropped, _ = pr.filter_active_rows(rows, self.db, 72, 24, NOW)
        self.assertEqual(len(kept), 1)
        self.assertEqual(dropped, 0)

    def test_stale_cache_aborts(self) -> None:
        # Newest trade is 48h old > 24h staleness bound -> abort, do not filter.
        _make_cache(self.db, [("0xaaa", NOW - 48 * HOUR)])
        rows = [{"wallet": "0xaaa"}]
        with self.assertRaises(pr.CacheStaleError):
            pr.filter_active_rows(rows, self.db, 72, 24, NOW)

    def test_empty_cache_aborts(self) -> None:
        _make_cache(self.db, [])
        rows = [{"wallet": "0xaaa"}]
        with self.assertRaises(pr.CacheStaleError):
            pr.filter_active_rows(rows, self.db, 72, 24, NOW)


class DefaultsDriftTest(unittest.TestCase):
    """Pin argparse defaults to docs/_GLOSSARY.md. On an intentional change, co-update
    docs/_GLOSSARY.md AND these literals together."""

    def test_defaults_match_glossary(self) -> None:
        a = pr.build_parser().parse_args(["--ranked-csv", "x.csv"])
        self.assertEqual(a.active_window_hours, 72)       # upload_active_window_hours
        self.assertEqual(a.max_cache_staleness_hours, 24)  # upload_max_cache_staleness_hours
        self.assertEqual(a.keep_batches, 180)             # ranking_batches_retention (#411)
        self.assertIsNone(a.db)                            # filter off unless --db given


class BuildEntriesTest(unittest.TestCase):
    """`build_entries` stamps each pushed row with the wallet's real last trade (#357)."""

    def test_entries_carry_last_trade_unix(self) -> None:
        top = [
            {"wallet": "0xAAA", "mean_net_ls": "0.1", "tstat_net_ls": "2.0",
             "fill_rate": "0.5", "n_filled": "10", "hit_rate": "0.6", "avg_price": "0.4"},
            {"wallet": "0xbbb"},  # missing numerics + absent from the map
        ]
        entries = pr.build_entries(top, batch_id=42, last_trade_map={"0xaaa": NOW - 5 * HOUR})
        self.assertEqual(entries[0]["last_trade_unix"], NOW - 5 * HOUR)  # keyed by lowercase
        self.assertIsNone(entries[1]["last_trade_unix"])                 # absent -> NULL
        self.assertEqual(entries[0]["rank"], 1)
        self.assertEqual(entries[0]["wallet_hex"], "0xAAA")             # original case preserved
        self.assertEqual(entries[0]["batch_id"], 42)
        self.assertIsNone(entries[1]["ls_edge"])                        # blank numeric -> NULL


class PruneOldBatchesTest(unittest.TestCase):
    """`prune_old_batches` (#411): cutoff GET + `lte` DELETE shaping, the ≤N no-op, the
    disable switch, and the never-delete-latest guarantee. Mocks `_req` — no live network."""

    URL = "https://x.supabase.co"
    KEY = "secret"
    GET = (
        f"{URL}/rest/v1/ranking_batches"
        "?select=batch_id&order=batch_id.desc&offset=180&limit=1"
    )

    def test_prunes_when_over_keep(self) -> None:
        # GET yields the (keep+1)-th newest batch_id -> one DELETE lte that id, CASCADE drops entries.
        with mock.patch.object(pr, "_req", return_value=(200, [{"batch_id": 12345}])) as m:
            cutoff = pr.prune_old_batches(self.URL, self.KEY, 180)
        self.assertEqual(cutoff, 12345)
        self.assertEqual(m.call_count, 2)
        self.assertEqual(m.call_args_list[0].args[0], "GET")
        self.assertEqual(m.call_args_list[0].args[1], self.GET)
        self.assertEqual(m.call_args_list[1].args[0], "DELETE")
        self.assertEqual(
            m.call_args_list[1].args[1],
            f"{self.URL}/rest/v1/ranking_batches?batch_id=lte.12345",
        )
        self.assertEqual(m.call_args_list[1].kwargs.get("prefer"), "return=minimal")

    def test_noop_when_at_or_below_keep(self) -> None:
        # PostgREST returns [] (not null) past the end -> GET only, no DELETE.
        with mock.patch.object(pr, "_req", return_value=(200, [])) as m:
            cutoff = pr.prune_old_batches(self.URL, self.KEY, 180)
        self.assertIsNone(cutoff)
        self.assertEqual(m.call_count, 1)
        self.assertEqual(m.call_args_list[0].args[0], "GET")

    def test_disabled_when_keep_zero(self) -> None:
        with mock.patch.object(pr, "_req") as m:
            cutoff = pr.prune_old_batches(self.URL, self.KEY, 0)
        self.assertIsNone(cutoff)
        m.assert_not_called()  # no GET, no DELETE when disabled

    def test_never_deletes_latest(self) -> None:
        # The cutoff is taken from offset=keep on a desc order, so the newest `keep` ids
        # (incl. max, what latest_ranking reads) are excluded by construction; the DELETE
        # only ever targets <= that cutoff. Assert the GET skips exactly `keep`, desc.
        with mock.patch.object(pr, "_req", return_value=(200, [{"batch_id": 999}])) as m:
            pr.prune_old_batches(self.URL, self.KEY, 25)
        get_url = m.call_args_list[0].args[1]
        self.assertIn("order=batch_id.desc", get_url)
        self.assertIn("offset=25", get_url)
        self.assertEqual(
            m.call_args_list[1].args[1],
            f"{self.URL}/rest/v1/ranking_batches?batch_id=lte.999",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
