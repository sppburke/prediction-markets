#!/usr/bin/env python3
"""Drift guard for the CLV source-comparison module (issue #429 PR2).

Covers the pure (no-network, no-DB) functions that drive the PR3 policy decision: hourly
bucketing, usability, source alignment + correlation, the true-CLV close lookup, the rank-signal
math, and the ``decide_policy`` branch logic. The DB/CLOB I/O paths are exercised by the operator
run that produces ``clv_source_comparison_results.md`` (this guard keeps the math honest in CI).

Run: ``python3 scripts/test_ranker_clv_source_comparison.py``
"""

import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker.clv_source_comparison import (  # noqa: E402
    aligned_pairs,
    clob_bucket_series,
    decide_policy,
    hourly_bucket_series,
    last_at_or_before,
    pearson,
    render_memo,
    spearman,
    topk_overlap,
    usable,
    wallet_rank_signal,
)


class BucketingTest(unittest.TestCase):
    def test_last_in_bucket_and_window(self) -> None:
        # start=0, bucket=3600s. Two trades in bucket 0 (keep the later), one in bucket 1,
        # one before the window (dropped), one after the window (dropped).
        ts = np.array([10, 100, 3700, -5, 999_999], dtype=np.int64)
        px = np.array([0.4, 0.5, 0.6, 0.1, 0.9], dtype=np.float64)
        out = hourly_bucket_series(ts, px, start=0, end=7200)
        self.assertEqual(
            out, {0: 0.5, 1: 0.6}
        )  # bucket 0 -> later trade (0.5); -5/999999 dropped

    def test_clob_bucket_series_empty(self) -> None:
        self.assertEqual(clob_bucket_series([], 0, 7200), {})

    def test_clob_bucket_matches_trades_bucketing(self) -> None:
        pts = [(10, 0.3), (3700, 0.7)]
        self.assertEqual(clob_bucket_series(pts, 0, 7200), {0: 0.3, 1: 0.7})


class UsableTest(unittest.TestCase):
    def test_threshold(self) -> None:
        self.assertFalse(usable(0))
        self.assertFalse(usable(2))
        self.assertTrue(usable(3))
        self.assertTrue(usable(50))


class AlignmentTest(unittest.TestCase):
    def test_pairs_on_shared_buckets_only(self) -> None:
        clob = {0: 0.5, 1: 0.6, 2: 0.7}
        trades = {1: 0.55, 2: 0.72, 3: 0.9}
        self.assertEqual(aligned_pairs(clob, trades), [(0.6, 0.55), (0.7, 0.72)])

    def test_no_overlap(self) -> None:
        self.assertEqual(aligned_pairs({0: 0.1}, {5: 0.2}), [])


class LastAtOrBeforeTest(unittest.TestCase):
    def test_picks_last_within_cutoff(self) -> None:
        pts = [(100, 0.4), (200, 0.6), (300, 0.9)]
        self.assertEqual(last_at_or_before(pts, cutoff=250), 0.6)
        self.assertEqual(last_at_or_before(pts, cutoff=1000), 0.9)

    def test_none_when_all_after_cutoff(self) -> None:
        self.assertIsNone(last_at_or_before([(100, 0.4)], cutoff=50))
        self.assertIsNone(last_at_or_before([], cutoff=50))


class CorrelationTest(unittest.TestCase):
    def test_spearman_perfect_and_inverse(self) -> None:
        a = np.array([1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(spearman(a, a * 2), 1.0, places=6)
        self.assertAlmostEqual(spearman(a, -a), -1.0, places=6)

    def test_spearman_degenerate_is_nan(self) -> None:
        self.assertTrue(np.isnan(spearman(np.array([1.0]), np.array([1.0]))))
        self.assertTrue(np.isnan(spearman(np.array([5.0, 5.0]), np.array([1.0, 2.0]))))

    def test_pearson(self) -> None:
        a = np.array([0.1, 0.2, 0.3, 0.4])
        self.assertAlmostEqual(pearson(a, a), 1.0, places=6)
        self.assertTrue(np.isnan(pearson(np.array([1.0, 1.0]), np.array([1.0, 1.0]))))


class TopKOverlapTest(unittest.TestCase):
    def test_overlap_fraction(self) -> None:
        a = {"w1": 0.9, "w2": 0.8, "w3": 0.1, "w4": 0.05}
        b = {"w1": 0.7, "w2": 0.6, "w3": 0.5, "w4": 0.4}
        self.assertEqual(topk_overlap(a, b, k=2), 1.0)  # {w1,w2} top-2 in both
        c = {"w3": 0.9, "w4": 0.8, "w1": 0.1, "w2": 0.05}
        self.assertEqual(topk_overlap(a, c, k=2), 0.0)  # disjoint top-2


class WalletRankSignalTest(unittest.TestCase):
    def test_empty(self) -> None:
        out = wallet_rank_signal([])
        self.assertEqual(out["wallets_compared"], 0)
        self.assertTrue(np.isnan(out["spearman"]))

    def test_identical_sources_perfect_rank(self) -> None:
        # proxy == true for every row -> per-wallet means rank identically -> Spearman 1.0.
        rows = []
        for i, w in enumerate(["a", "b", "c", "d"]):
            base = float(i)
            rows += [(w, base, base), (w, base + 0.1, base + 0.1)]
        out = wallet_rank_signal(rows)
        self.assertEqual(out["wallets_compared"], 4)
        self.assertAlmostEqual(out["spearman"], 1.0, places=6)

    def test_drops_wallets_below_min_positions(self) -> None:
        # 'a' has 2 positions (kept), 'b' has 1 (dropped) -> only 1 wallet -> NaN spearman.
        out = wallet_rank_signal([("a", 0.1, 0.2), ("a", 0.3, 0.4), ("b", 0.5, 0.6)])
        self.assertEqual(out["wallets_compared"], 1)
        self.assertTrue(np.isnan(out["spearman"]))


class DecidePolicyTest(unittest.TestCase):
    def test_proxy_clv_suffices_on_high_spearman(self) -> None:
        d = decide_policy(
            clob_coverage_pct=80.0,
            trades_coverage_pct=50.0,
            clob_plus_trades_coverage_pct=85.0,
            median_abs_diff=0.01,
            true_vs_proxy_spearman=0.97,
        )
        self.assertEqual(d.policy, "proxy_clv_suffices")

    def test_trades_only_when_trades_dominates(self) -> None:
        d = decide_policy(
            clob_coverage_pct=10.0,
            trades_coverage_pct=45.0,
            clob_plus_trades_coverage_pct=46.0,
            median_abs_diff=0.01,
            true_vs_proxy_spearman=0.4,
        )
        self.assertEqual(d.policy, "trades_only")

    def test_clob_primary_with_trades_fill(self) -> None:
        d = decide_policy(
            clob_coverage_pct=70.0,
            trades_coverage_pct=30.0,
            clob_plus_trades_coverage_pct=82.0,  # +12pp uplift from trades
            median_abs_diff=0.02,  # sound proxy
            true_vs_proxy_spearman=0.5,
        )
        self.assertEqual(d.policy, "clob_primary_with_trades_fill")

    def test_clob_only_when_fill_is_marginal(self) -> None:
        d = decide_policy(
            clob_coverage_pct=85.0,
            trades_coverage_pct=20.0,
            clob_plus_trades_coverage_pct=87.0,  # +2pp uplift (< 5pp)
            median_abs_diff=0.01,
            true_vs_proxy_spearman=0.5,
        )
        self.assertEqual(d.policy, "clob_only")

    def test_clob_only_when_trades_biased(self) -> None:
        d = decide_policy(
            clob_coverage_pct=60.0,
            trades_coverage_pct=40.0,
            clob_plus_trades_coverage_pct=75.0,  # +15pp uplift but...
            median_abs_diff=0.20,  # ...trades-last is a biased proxy
            true_vs_proxy_spearman=0.5,
        )
        self.assertEqual(d.policy, "clob_only")


class RenderMemoTest(unittest.TestCase):
    def test_renders_policy_and_table(self) -> None:
        metrics = {
            "sample_size": 1234,
            "clob_base": "https://clob.example",
            "window_secs": 259_200,
            "fidelity_minutes": 60,
            "measurement_a_clob_coverage_pct": 71.0,
            "measurement_b_trades_coverage_pct": 64.0,
            "clob_or_trades_coverage_pct": 78.0,
            "measurement_c_intersection_markets": 400,
            "measurement_c_correlation": 0.93,
            "measurement_c_median_abs_diff": 0.012,
            "measurement_c_mid_bias_flag": False,
            "measurement_d_thin_markets_pct": 33.0,
            "measurement_d_unrecoverable_pct": 21.0,
            "measurement_e_wallet_rank": {
                "wallets_compared": 50,
                "spearman": 0.42,
                "top_decile_overlap": 0.6,
                "top_decile_k": 5,
            },
            "policy": "clob_only",
            "rationale": "test rationale",
        }
        memo = render_memo(metrics)
        self.assertIn("Policy: `clob_only`", memo)
        self.assertIn("71.0%", memo)  # clob coverage
        self.assertIn("0.93", memo)  # correlation
        self.assertIn("0.42", memo)  # spearman
        self.assertIn("test rationale", memo)


if __name__ == "__main__":
    unittest.main(verbosity=2)
