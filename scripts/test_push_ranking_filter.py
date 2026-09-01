#!/usr/bin/env python3
"""Behaviour + drift guard for the active-only upload filter in
`scripts/push_ranking_to_supabase.py` (issue #350 WS3).

Stdlib-only (`unittest` + `sqlite3` + `tempfile`); builds a throwaway cache and
asserts the trade/resolution/completed-sweep freshness gates and recency filter
behave as specified. Also pins the argparse defaults to the
`docs/_GLOSSARY.md` values so a silent drift fails CI.

This test runs in CI as part of `.github/workflows/ci.yml` (Python drift-guard step).

Run: `python3 scripts/test_push_ranking_filter.py`
  or: `pytest scripts/test_push_ranking_filter.py -v`
"""
import io
import sqlite3
import sys
import tempfile
import unittest
from contextlib import chdir, redirect_stderr
from pathlib import Path
from unittest import mock

# Import the push script as a module (its work is guarded behind `if __name__`).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import push_ranking_to_supabase as pr  # noqa: E402

HOUR = 3600
NOW = 1_700_000_000  # fixed clock — determinism (no time.time() in assertions)


def _make_cache(
    path: str,
    trades: list[tuple[str, int]],
    *,
    resolution_fetched_at: int | None = NOW - HOUR,
    cursor: tuple[str, int] | None = ("", NOW - HOUR),
) -> None:
    """Build the minimal production-shaped tables read by the publication gate."""
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE trades (source_trade_id TEXT, wallet_hex TEXT, market_id TEXT, "
        "outcome_id TEXT, side TEXT, price_str TEXT, contracts TEXT, timestamp_unix INTEGER)"
    )
    con.execute("CREATE INDEX idx_trades_wallet_ts ON trades(wallet_hex, timestamp_unix)")
    con.executemany("INSERT INTO trades (wallet_hex, timestamp_unix) VALUES (?, ?)", trades)
    con.execute("CREATE TABLE market_resolutions (fetched_at_unix INTEGER NOT NULL)")
    if resolution_fetched_at is not None:
        con.execute(
            "INSERT INTO market_resolutions (fetched_at_unix) VALUES (?)",
            (resolution_fetched_at,),
        )
    con.execute(
        "CREATE TABLE source_cursor (key TEXT PRIMARY KEY, value TEXT NOT NULL, "
        "updated_at INTEGER NOT NULL)"
    )
    if cursor is not None:
        con.execute(
            "INSERT INTO source_cursor (key, value, updated_at) VALUES ('clob_closed', ?, ?)",
            cursor,
        )
    con.commit()
    con.close()


def _make_v2_cache(path: str) -> None:
    """Build only the completed v2 surfaces read by the publication gate."""
    con = sqlite3.connect(path)
    con.executescript(
        f"""
        PRAGMA user_version = 2;
        CREATE TABLE trades_v1_sealed (wallet_hex TEXT, timestamp_unix INTEGER);
        CREATE TABLE market_resolutions_v1_sealed (fetched_at_unix INTEGER);
        CREATE TABLE source_cursor_v1_sealed (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL
        );
        CREATE TABLE activity_coverage_manifests_v2 (
            generation INTEGER PRIMARY KEY, completed_at_unix INTEGER
        );
        CREATE TABLE activity_groups_v2 (
            wallet_hex TEXT, source_time_unix INTEGER, activity_type TEXT,
            coverage_generation INTEGER
        );
        CREATE TABLE clob_payout_coverage_manifests_v2 (
            generation INTEGER PRIMARY KEY, terminal_kind TEXT, completed_at_unix INTEGER
        );
        INSERT INTO trades_v1_sealed VALUES ('0xsealed', {NOW});
        INSERT INTO market_resolutions_v1_sealed VALUES ({NOW});
        INSERT INTO source_cursor_v1_sealed VALUES ('clob_closed', '', {NOW});
        INSERT INTO activity_coverage_manifests_v2 VALUES (1, {NOW - HOUR});
        INSERT INTO activity_groups_v2 VALUES ('0xaaa', {NOW - HOUR}, 'TRADE', 1);
        INSERT INTO activity_groups_v2 VALUES ('0xbbb', {NOW}, 'REDEEM', 1);
        INSERT INTO activity_groups_v2 VALUES ('0xpartial', {NOW}, 'TRADE', 2);
        INSERT INTO clob_payout_coverage_manifests_v2
            VALUES (1, 'end_cursor', {NOW - HOUR});
        """
    )
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

    def test_missing_clob_cursor_aborts(self) -> None:
        _make_cache(self.db, [("0xaaa", NOW - HOUR)], cursor=None)
        with self.assertRaisesRegex(pr.CacheStaleError, "no completed CLOB sweep"):
            pr.filter_active_rows([{"wallet": "0xaaa"}], self.db, 72, 24, NOW)

    def test_stale_completed_clob_cursor_aborts(self) -> None:
        _make_cache(
            self.db,
            [("0xaaa", NOW - HOUR)],
            cursor=("", NOW - 48 * HOUR),
        )
        with self.assertRaisesRegex(pr.CacheStaleError, "completed CLOB sweep is 48.0h old"):
            pr.filter_active_rows([{"wallet": "0xaaa"}], self.db, 72, 24, NOW)

    def test_fresh_nonempty_clob_cursor_aborts(self) -> None:
        _make_cache(
            self.db,
            [("0xaaa", NOW - HOUR)],
            cursor=("PAGE2", NOW - HOUR),
        )
        with self.assertRaisesRegex(pr.CacheStaleError, "CLOB sweep is incomplete"):
            pr.filter_active_rows([{"wallet": "0xaaa"}], self.db, 72, 24, NOW)

    def test_fresh_completed_clob_cursor_probe(self) -> None:
        _make_cache(self.db, [("0xaaa", NOW - HOUR)])
        con = sqlite3.connect(self.db)
        try:
            self.assertEqual(pr._clob_sweep_completed_at(con), ("", NOW - HOUR))
        finally:
            con.close()

    def test_stale_resolution_max_aborts(self) -> None:
        _make_cache(
            self.db,
            [("0xaaa", NOW - HOUR)],
            resolution_fetched_at=NOW - 48 * HOUR,
        )
        with self.assertRaisesRegex(pr.CacheStaleError, "newest resolution fetch is 48.0h old"):
            pr.filter_active_rows([{"wallet": "0xaaa"}], self.db, 72, 24, NOW)

    def test_missing_resolution_max_aborts(self) -> None:
        _make_cache(
            self.db,
            [("0xaaa", NOW - HOUR)],
            resolution_fetched_at=None,
        )
        with self.assertRaisesRegex(pr.CacheStaleError, "newest resolution fetch is unknown"):
            pr.filter_active_rows([{"wallet": "0xaaa"}], self.db, 72, 24, NOW)

    def test_fresh_resolution_max_probe_and_everything_passes(self) -> None:
        _make_cache(self.db, [("0xaaa", NOW - HOUR)])
        con = sqlite3.connect(self.db)
        try:
            self.assertEqual(pr._newest_resolution_fetch(con), NOW - HOUR)
        finally:
            con.close()
        kept, dropped, _ = pr.filter_active_rows(
            [{"wallet": "0xaaa"}], self.db, 72, 24, NOW
        )
        self.assertEqual(kept, [{"wallet": "0xaaa"}])
        self.assertEqual(dropped, 0)

    def test_v2_filter_reads_only_latest_completed_trade_and_payout_generations(self) -> None:
        _make_v2_cache(self.db)
        rows = [
            {"wallet": "0xaaa"},
            {"wallet": "0xbbb"},
            {"wallet": "0xpartial"},
            {"wallet": "0xsealed"},
        ]

        kept, dropped, last = pr.filter_active_rows(rows, self.db, 72, 24, NOW)

        self.assertEqual(kept, [{"wallet": "0xaaa"}])
        self.assertEqual(dropped, 3)
        self.assertEqual(last, {"0xaaa": NOW - HOUR})
        with sqlite3.connect(self.db) as con:
            self.assertEqual(pr._newest_trade_unix(con), NOW - HOUR)
            self.assertEqual(pr._newest_resolution_fetch(con), NOW - HOUR)
            self.assertEqual(pr._clob_sweep_completed_at(con), ("", NOW - HOUR))


class DefaultsDriftTest(unittest.TestCase):
    """Pin argparse defaults to docs/_GLOSSARY.md. On an intentional change, co-update
    docs/_GLOSSARY.md AND these literals together."""

    def test_defaults_match_glossary(self) -> None:
        a = pr.build_parser().parse_args(["--ranked-csv", "x.csv"])
        self.assertEqual(a.active_window_hours, 72)       # upload_active_window_hours
        self.assertEqual(a.max_cache_staleness_hours, 24)  # upload_max_cache_staleness_hours
        self.assertEqual(a.keep_batches, 1080)            # ranking_batches_retention (#411; 1080 at the 4h cadence, run28 cutover)
        self.assertIsNone(a.db)                            # filter off unless --db given
        self.assertEqual(pr.SUPABASE_MAX_RETRIES, 5)       # ranking_publish_max_retries
        self.assertEqual(pr.SUPABASE_RETRY_BASE_SECS, 1)   # ranking_publish_retry_base_secs
        self.assertEqual(pr.SUPABASE_RETRY_MAX_SECS, 30)   # ranking_publish_retry_max_secs


class BuildEntriesTest(unittest.TestCase):
    """`build_entries` stamps each pushed row with the wallet's real last trade (#357)."""

    def test_entries_carry_last_trade_unix(self) -> None:
        top = [
            {"wallet": "0xAAA", "mean_net_ls": "0.1", "tstat_net_ls": "2.0",
             "fill_rate": "0.5", "n_filled": "10", "hit_rate": "0.6", "avg_price": "0.4"},
            {"wallet": "0xbbb"},  # missing numerics + absent from the map
        ]
        entries = pr.build_entries(top, last_trade_map={"0xaaa": NOW - 5 * HOUR})
        self.assertEqual(entries[0]["last_trade_unix"], NOW - 5 * HOUR)  # keyed by lowercase
        self.assertIsNone(entries[1]["last_trade_unix"])                 # absent -> NULL
        self.assertEqual(entries[0]["rank"], 1)
        self.assertEqual(entries[0]["wallet_hex"], "0xAAA")             # original case preserved
        self.assertNotIn("batch_id", entries[0])                         # RPC injects atomically
        self.assertIsNone(entries[1]["ls_edge"])                        # blank numeric -> NULL

    def test_entries_carry_the_ranker_verdict(self) -> None:
        # The producer writes a Python bool through csv.DictWriter, so the file literally holds
        # `True`/`False` (#518) — case normalisation is load-bearing, not cosmetic.
        top = [
            {"wallet": "0xaaa", "survives": "True"},
            {"wallet": "0xbbb", "survives": "False"},
            {"wallet": "0xccc"},  # key absent entirely -> SQL NULL, never admitted
        ]
        entries = pr.build_entries(top, last_trade_map={})
        self.assertIs(entries[0]["survives"], True)
        self.assertIs(entries[1]["survives"], False)
        self.assertIsNone(entries[2]["survives"])

    def test_unparseable_verdict_raises_instead_of_degrading_to_false(self) -> None:
        # Silently reading a malformed verdict as False would drop a genuine survivor out of the
        # live watchlist; fail closed at parse time instead.
        with self.assertRaises(ValueError):
            pr.build_entries([{"wallet": "0xaaa", "survives": "yes"}], last_trade_map={})


class PublishRequestTest(unittest.TestCase):
    def _request(self, keep_batches=0, verdicts=(True, False)):
        """Build a publish request. `verdicts=None` reproduces the LEGACY pre-#518 shape,
        where the key is absent from every entry — the durable-recovery case that must keep
        validating and replaying unchanged."""
        batch = {
            "git_sha": "abc123",
            "band_lo": 0.15,
            "band_hi": 0.85,
            "ttr_floor_secs": 30,
            "ttr_max_secs": 172800,
            "latency_shift_secs": 20,
            "universe_size": 2,
            "notes": "test",
        }
        entries = [
            {
                "rank": 1,
                "wallet_hex": "0xaaa",
                "ls_edge": 0.1,
                "ls_tstat": 2.5,
                "fill_rate": 0.8,
                "n_trades": 20,
                "hit_rate": 0.6,
                "avg_price": 0.4,
                "last_trade_unix": NOW,
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
        if verdicts is not None:
            for entry, verdict in zip(entries, verdicts):
                entry["survives"] = verdict
        return pr.build_publish_request(batch, entries, keep_batches)

    def test_request_round_trip_and_content_hash_tamper_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "request.json"
            request = self._request()
            pr.save_publish_request(str(path), request)
            self.assertEqual(pr.load_publish_request(str(path)), request)
            tampered = pr.load_publish_request(str(path))
            tampered["entries"][0]["wallet_hex"] = "0xchanged"
            with self.assertRaisesRegex(ValueError, "content hash mismatch"):
                pr.validate_publish_request(tampered)

    def test_pending_pointer_is_atomic_repository_relative(self):
        with tempfile.TemporaryDirectory() as tmp, chdir(tmp):
            request_path = Path("data/eval-results/cron-test/ranking_publish_request.json")
            request_path.parent.mkdir(parents=True)
            pr.save_publish_request(str(request_path), self._request())
            pending = Path("data/eval-results/rank_and_push.pending")
            pr.save_pending_pointer(str(pending), str(request_path))
            self.assertEqual(pending.read_text(), f"{request_path}\n")

    def test_prepare_only_seeds_exact_recovery_without_credentials_or_network(self):
        with tempfile.TemporaryDirectory() as tmp, chdir(tmp):
            csv_path = Path("data/eval-results/cron-test/latency_shift_ranked.csv")
            csv_path.parent.mkdir(parents=True)
            csv_path.write_text(
                "wallet,survives,tstat_net_ls\n0xaaa,true,2.5\n",
                encoding="utf-8",
            )
            request_path = csv_path.with_name("ranking_publish_request.json")
            pending_path = Path("data/eval-results/rank_and_push.pending")
            with (
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        "push",
                        "--ranked-csv",
                        str(csv_path),
                        "--request-file",
                        str(request_path),
                        "--pending-file",
                        str(pending_path),
                        "--prepare-only",
                    ],
                ),
                mock.patch.dict("os.environ", {}, clear=True),
                mock.patch.object(pr, "_request_once") as network,
            ):
                self.assertEqual(pr.main(), 0)
            network.assert_not_called()
            request = pr.load_publish_request(str(request_path))
            self.assertEqual(request["entries"][0]["wallet_hex"], "0xaaa")
            self.assertEqual(pending_path.read_text(), f"{request_path}\n")

    def test_prepare_refuses_a_batch_that_cannot_state_eligibility(self):
        # #518 fail-closed at the cheapest point: a ranked file with no verdict column is
        # rejected during preparation, before anything durable or networked happens. Publishing
        # it would store NULLs, which the reader filter then refuses — a silently empty
        # watchlist instead of a loud, local failure.
        with tempfile.TemporaryDirectory() as tmp, chdir(tmp):
            csv_path = Path("data/eval-results/cron-test/latency_shift_ranked.csv")
            csv_path.parent.mkdir(parents=True)
            csv_path.write_text("wallet,tstat_net_ls\n0xaaa,2.5\n", encoding="utf-8")
            request_path = csv_path.with_name("ranking_publish_request.json")
            with (
                mock.patch.object(
                    sys, "argv",
                    ["push", "--ranked-csv", str(csv_path), "--request-file", str(request_path),
                     "--prepare-only"],
                ),
                mock.patch.dict("os.environ", {}, clear=True),
                mock.patch.object(pr, "_request_once") as network,
                redirect_stderr(io.StringIO()) as stderr,
            ):
                # Preparation failures are a clean permanent exit (1), never a traceback and
                # never the retryable 75 — a malformed ranked file will not fix itself.
                self.assertEqual(pr.main(), 1)
            self.assertIn("survives", stderr.getvalue())
            network.assert_not_called()
            self.assertFalse(request_path.exists(), "no durable request written")

    def test_prepare_accepts_the_producers_literal_boolean_spelling(self):
        # End-to-end through main(): the ranker's own `True`/`False` spelling must reach the
        # entry payload as real booleans.
        with tempfile.TemporaryDirectory() as tmp, chdir(tmp):
            csv_path = Path("data/eval-results/cron-test/latency_shift_ranked.csv")
            csv_path.parent.mkdir(parents=True)
            csv_path.write_text(
                "wallet,survives,tstat_net_ls\n0xaaa,True,2.5\n0xbbb,False,1.0\n",
                encoding="utf-8",
            )
            request_path = csv_path.with_name("ranking_publish_request.json")
            with (
                mock.patch.object(
                    sys, "argv",
                    ["push", "--ranked-csv", str(csv_path), "--request-file", str(request_path),
                     "--prepare-only"],
                ),
                mock.patch.dict("os.environ", {}, clear=True),
                mock.patch.object(pr, "_request_once") as network,
            ):
                self.assertEqual(pr.main(), 0)
            network.assert_not_called()
            entries = pr.load_publish_request(str(request_path))["entries"]
            self.assertIs(entries[0]["survives"], True)
            self.assertIs(entries[1]["survives"], False)
            # Survivors sort ahead of failures, so rank 1 is the surviving wallet.
            self.assertEqual(entries[0]["wallet_hex"], "0xaaa")

    def test_rpc_publication_verifies_exact_returned_batch(self):
        request = self._request(keep_batches=0)
        latest = [
            {"batch_id": 42, "rank": 1, "survives": True},
            {"batch_id": 42, "rank": 2, "survives": False},
        ]
        with mock.patch.object(
            pr,
            "_req",
            side_effect=[(200, 42), (200, latest)],
        ) as request_mock:
            batch_id = pr.publish_request_to_supabase(
                request, "https://x.supabase.co", "secret"
            )
        self.assertEqual(batch_id, 42)
        self.assertEqual(request_mock.call_count, 2)
        rpc = request_mock.call_args_list[0]
        self.assertEqual(rpc.args[0], "POST")
        self.assertTrue(rpc.args[1].endswith("/rest/v1/rpc/publish_ranking_batch"))
        self.assertEqual(rpc.kwargs["body"]["p_publish_key"], request["publish_key"])
        self.assertNotIn("batch_id", rpc.kwargs["body"]["p_entries"][0])
        self.assertIs(rpc.kwargs["body"]["p_entries"][0]["survives"], True)
        # The post-publication read must ask for the verdict, or it cannot verify it (#518).
        self.assertIn("survives", request_mock.call_args_list[1].args[1])

    def test_rpc_publication_rejects_a_mismatched_stored_verdict(self):
        # The batch committed, but the stored verdict is not what this request sent — a real
        # defect in the publication path. Fail loudly rather than shipping a batch whose
        # eligibility column silently disagrees with the ranker.
        request = self._request(keep_batches=0)
        latest = [
            {"batch_id": 42, "rank": 1, "survives": False},   # submitted True
            {"batch_id": 42, "rank": 2, "survives": False},
        ]
        with mock.patch.object(pr, "_req", side_effect=[(200, 42), (200, latest)]):
            with self.assertRaises(ValueError) as caught:
                pr.publish_request_to_supabase(request, "https://x.supabase.co", "secret")
        self.assertIn("survives", str(caught.exception))

    def test_legacy_request_without_verdict_still_validates_and_verifies(self):
        # A durable request recorded before #518 carries no verdict key. It must still validate
        # (no entry-key whitelist), replay, and pass verification: absent submitted value vs
        # stored SQL NULL is a match, so the upgrade can never wedge publication recovery.
        request = self._request(keep_batches=0, verdicts=None)
        pr.validate_publish_request(request)
        self.assertNotIn("survives", request["entries"][0])
        latest = [
            {"batch_id": 42, "rank": 1, "survives": None},
            {"batch_id": 42, "rank": 2, "survives": None},
        ]
        with mock.patch.object(pr, "_req", side_effect=[(200, 42), (200, latest)]):
            self.assertEqual(
                pr.publish_request_to_supabase(request, "https://x.supabase.co", "secret"),
                42,
            )

    def test_retry_transient_only_with_bounded_exponential_backoff(self):
        transient = pr.SupabaseRequestError("timeout", retryable=True)
        with mock.patch.object(
            pr,
            "_request_once",
            side_effect=[transient, transient, (200, {"ok": True})],
        ) as once:
            sleeps = []
            result = pr._req(
                "POST",
                "https://x",
                "secret",
                max_retries=5,
                sleep=sleeps.append,
            )
        self.assertEqual(result, (200, {"ok": True}))
        self.assertEqual(once.call_count, 3)
        self.assertEqual(sleeps, [1, 2])

    def test_retry_after_is_bounded_and_never_tight_loops(self):
        transient = pr.SupabaseRequestError(
            "HTTP 429", retryable=True, status=429, retry_after_secs=0
        )
        with mock.patch.object(
            pr,
            "_request_once",
            side_effect=[transient, (200, {"ok": True})],
        ):
            sleeps = []
            pr._req(
                "POST",
                "https://x",
                "secret",
                max_retries=1,
                sleep=sleeps.append,
            )
        self.assertEqual(sleeps, [1])

    def test_permanent_error_never_retries(self):
        permanent = pr.SupabaseRequestError(
            "HTTP 401", retryable=False, status=401
        )
        with mock.patch.object(pr, "_request_once", side_effect=permanent) as once:
            with self.assertRaises(pr.SupabaseRequestError):
                pr._req("POST", "https://x", "secret", sleep=lambda _: None)
        self.assertEqual(once.call_count, 1)

    def test_transient_exhaustion_raises_typed_error(self):
        transient = pr.SupabaseRequestError("timeout", retryable=True)
        with mock.patch.object(pr, "_request_once", side_effect=transient):
            sleeps = []
            with self.assertRaises(pr.TransientRetriesExhausted):
                pr._req(
                    "POST",
                    "https://x",
                    "secret",
                    max_retries=2,
                    sleep=sleeps.append,
                )
        self.assertEqual(sleeps, [1, 2])

    def test_main_maps_exhausted_transient_to_exit_75(self):
        with tempfile.TemporaryDirectory() as tmp:
            request_path = Path(tmp) / "request.json"
            pr.save_publish_request(str(request_path), self._request())
            with (
                mock.patch.object(
                    sys,
                    "argv",
                    ["push", "--resume-request", str(request_path)],
                ),
                mock.patch.dict(
                    "os.environ",
                    {"SUPABASE_URL": "https://x", "SUPABASE_SECRET_KEY": "secret"},
                    clear=True,
                ),
                mock.patch.object(
                    pr,
                    "publish_request_to_supabase",
                    side_effect=pr.TransientRetriesExhausted("timeout"),
                ),
            ):
                self.assertEqual(pr.main(), 75)


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

    def test_malformed_response_raises_for_caller_to_swallow(self) -> None:
        # A row missing batch_id raises; main()'s best-effort except swallows it so a
        # succeeded push never fails on a prune hiccup (#411 — pins the contract).
        with mock.patch.object(pr, "_req", return_value=(200, [{}])):
            with self.assertRaises(KeyError):
                pr.prune_old_batches(self.URL, self.KEY, 180)

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


class ManifestBindingTest(unittest.TestCase):
    """#536: config_hash must bind the manifest to the ranking actually published."""

    def _prep(self, tamper: bool):
        import argparse
        import json as _json
        import tempfile
        from pathlib import Path

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        ranked = root / "latency_shift_ranked.csv"
        ranked.write_text(
            "wallet,n_total,n_filled,fill_rate,active_months,mean_net_ls,"
            "tstat_net_ls,n_eff,hit_rate,survives\n"
            "0x" + "a" * 40 + ",5,5,1.0,2,0.5,3.0,5.0,0.8,True\n"
        )
        import hashlib as _hashlib

        digest = _hashlib.sha256(ranked.read_bytes()).hexdigest()
        manifest = root / "oracle_manifest.json"
        manifest.write_text(_json.dumps({
            "oracle": "clob-minute-reference",
            "outputs": {"latency_shift_ranked_sha256": "0" * 64 if tamper else digest},
        }))
        return argparse.Namespace(
            ranked_csv=str(ranked), manifest_file=str(manifest), db=None,
            top_n=200, band_lo=0.15, band_hi=0.85, ttr_floor_secs=30,
            ttr_max_secs=172800, latency_shift_secs=2, universe_size=0,
            git_sha="test", notes="", keep_batches=0,
            active_window_hours=72, max_cache_staleness_hours=24,
        )

    def test_matching_manifest_produces_config_hash(self) -> None:
        req = pr.prepare_publish_request(self._prep(tamper=False), 1_700_000_000)
        self.assertIsNotNone(req["batch"]["config_hash"])
        print("PASS: matching manifest digest -> config_hash populated")

    def test_mismatched_manifest_refuses_to_publish(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            pr.prepare_publish_request(self._prep(tamper=True), 1_700_000_000)
        self.assertIn("refusing to publish", str(ctx.exception))
        print("PASS: manifest/ranking digest mismatch refuses publication")


if __name__ == "__main__":
    unittest.main(verbosity=2)
