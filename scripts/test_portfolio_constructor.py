#!/usr/bin/env python3
"""stdlib-only unit tests for portfolio_constructor pure modules.

Tests overlap.py, selector.py, sizing.py — the three CI-testable modules.
None of them import numpy, pandas, lightgbm, or composite_tuner.

Run: python3 scripts/test_portfolio_constructor.py
  or: pytest scripts/test_portfolio_constructor.py -v

Imports are done via sys.path.insert so this file loads the modules as
top-level (bypassing portfolio_constructor/__init__.py), exactly as
test_haircut_constants.py:37 bypasses composite_tuner/__init__.py.
"""
import sys
import unittest
from pathlib import Path

# Load the three stdlib-only modules as top-level to avoid triggering
# portfolio_constructor/__init__.py (which lazy-imports heavy dependencies).
sys.path.insert(0, str(Path(__file__).resolve().parent / 'portfolio_constructor'))
import overlap as _overlap   # noqa: E402
import selector as _sel      # noqa: E402
import sizing as _siz        # noqa: E402


# ─── overlap ──────────────────────────────────────────────────────────────────

class TestJaccard(unittest.TestCase):

    def test_identical(self):
        a = frozenset([(1, 0), (2, 0)])
        self.assertAlmostEqual(_overlap.jaccard(a, a), 1.0)

    def test_disjoint(self):
        a = frozenset([(1, 0)])
        b = frozenset([(2, 0)])
        self.assertAlmostEqual(_overlap.jaccard(a, b), 0.0)

    def test_half_overlap(self):
        a = frozenset([(1, 0), (2, 0)])
        b = frozenset([(2, 0), (3, 0)])
        # |intersection|=1, |union|=3
        self.assertAlmostEqual(_overlap.jaccard(a, b), 1.0 / 3.0)

    def test_empty_both(self):
        self.assertAlmostEqual(_overlap.jaccard(frozenset(), frozenset()), 0.0)

    def test_empty_one_side(self):
        a = frozenset([(1, 0)])
        self.assertAlmostEqual(_overlap.jaccard(a, frozenset()), 0.0)
        self.assertAlmostEqual(_overlap.jaccard(frozenset(), a), 0.0)


class TestMarginalOverlap(unittest.TestCase):

    def test_empty_selected_union_gives_zero(self):
        cand = frozenset([(1, 0), (2, 0)])
        val = _overlap.marginal_overlap(cand, frozenset(), {})
        self.assertAlmostEqual(val, 0.0)

    def test_full_overlap(self):
        markets = frozenset([(1, 0)])
        val = _overlap.marginal_overlap(markets, markets, {})
        self.assertAlmostEqual(val, 1.0)

    def test_partial_overlap(self):
        cand = frozenset([(1, 0), (2, 0)])
        union = frozenset([(2, 0), (3, 0)])
        # jaccard = 1/3
        val = _overlap.marginal_overlap(cand, union, {})
        self.assertAlmostEqual(val, 1.0 / 3.0)


# ─── selector ─────────────────────────────────────────────────────────────────

def _make_selector(lam=1.0):
    return _sel.GreedySelector(overlap_fn=_overlap.marginal_overlap, overlap_lambda=lam)


class TestGreedySelector(unittest.TestCase):

    def test_lambda_zero_is_top_n(self):
        """λ=0 means pure top-N by score."""
        sel = _make_selector(lam=0.0)
        scores = {'a': 0.9, 'b': 0.8, 'c': 0.7}
        # All empty market sets → overlap always 0 anyway, but λ=0 should also work.
        result = sel.select(scores, {}, max_n=2)
        self.assertEqual(result.wallets, ['a', 'b'])

    def test_overlap_deduplication(self):
        """Wallet sharing all markets with selected gets penalised."""
        sel = _make_selector(lam=1.0)
        shared = frozenset([(1, 0), (2, 0)])
        disjoint = frozenset([(3, 0), (4, 0)])
        scores = {'a': 1.0, 'b': 0.9, 'c': 0.85}
        market_sets = {'a': shared, 'b': shared, 'c': disjoint}
        result = sel.select(scores, market_sets, max_n=2)
        # 'a' is picked first (score 1.0). Then 'b' vs 'c':
        # 'b' obj = 0.9 * (1 - 1.0 * 1.0) = 0.0; 'c' obj = 0.85 * (1 - 0.0) = 0.85
        self.assertEqual(result.wallets[0], 'a')
        self.assertEqual(result.wallets[1], 'c')

    def test_min_edge_gate(self):
        """Wallets with score <= min_edge_score are excluded from candidates."""
        sel = _make_selector()
        scores = {'a': 0.5, 'b': 0.3, 'c': -0.1}
        result = sel.select(scores, {}, max_n=10, min_edge_score=0.0)
        # c <= 0.0 never enters candidates; b passes (0.3 > 0.0)
        self.assertIn('a', result.wallets)
        self.assertIn('b', result.wallets)
        self.assertNotIn('c', result.wallets)

    def test_max_n_respected(self):
        scores = {f'w{i}': float(10 - i) for i in range(10)}
        sel = _make_selector()
        result = sel.select(scores, {}, max_n=3)
        self.assertEqual(len(result.wallets), 3)

    def test_empty_scores(self):
        sel = _make_selector()
        result = sel.select({}, {}, max_n=5)
        self.assertEqual(result.wallets, [])


# ─── sizing ───────────────────────────────────────────────────────────────────

class TestFullKelly(unittest.TestCase):

    def test_positive_edge(self):
        # Simple: returns all +0.5 → mu=0.5, var=0 → falls back to 0.0
        # Use a mix to produce non-zero variance.
        returns = [1.0, -0.5, 1.0, -0.5]
        f = _siz.full_kelly_fraction(returns)
        self.assertGreater(f, 0.0)
        self.assertLessEqual(f, 1.0)

    def test_empty_returns(self):
        self.assertAlmostEqual(_siz.full_kelly_fraction([]), 0.0)

    def test_zero_variance(self):
        self.assertAlmostEqual(_siz.full_kelly_fraction([0.5, 0.5, 0.5]), 0.0)

    def test_non_positive_mean(self):
        self.assertAlmostEqual(_siz.full_kelly_fraction([-0.1, -0.2, -0.3]), 0.0)

    def test_clip_at_one(self):
        # Huge mu / tiny var → should clip at 1.0.
        returns = [100.0, 99.0, 100.0, 99.0]
        f = _siz.full_kelly_fraction(returns)
        self.assertAlmostEqual(f, 1.0)


class TestExanteKelly(unittest.TestCase):

    def test_fewer_than_5_returns_zero(self):
        for n in range(5):
            self.assertAlmostEqual(_siz.exante_kelly_fraction([0.1] * n), 0.0,
                                   msg=f'n={n}')

    def test_shrink_nonzero(self):
        # With n>=5 and positive edge, should return positive.
        returns = [0.5, -0.3, 0.4, -0.2, 0.6, -0.1]
        f = _siz.exante_kelly_fraction(returns)
        self.assertGreaterEqual(f, 0.0)
        self.assertLessEqual(f, 1.0)

    def test_negative_edge_returns_zero(self):
        returns = [-0.1, -0.2, -0.1, -0.2, -0.1, -0.2]
        self.assertAlmostEqual(_siz.exante_kelly_fraction(returns), 0.0)


class TestSimFractional(unittest.TestCase):

    def test_zero_fraction_no_change(self):
        b, placed, skipped = _siz.sim_fractional([0.5, -0.5], 0.0, 1000.0, 5.0)
        # stake = 0 * b < 5 → all skipped
        self.assertAlmostEqual(b, 1000.0)
        self.assertEqual(placed, 0)

    def test_positive_returns_grow_bankroll(self):
        b, placed, _ = _siz.sim_fractional([0.5, 0.5], 0.1, 1000.0, 1.0)
        self.assertGreater(b, 1000.0)
        self.assertEqual(placed, 2)

    def test_min_pos_floor_skips_small_stake(self):
        # f=0.001 → stake = 1.0 on $1000; min_pos=5 → should skip.
        b, placed, skipped = _siz.sim_fractional([1.0], 0.001, 1000.0, 5.0)
        self.assertEqual(placed, 0)
        self.assertEqual(skipped, 1)
        self.assertAlmostEqual(b, 1000.0)


class TestSimFlat(unittest.TestCase):

    def test_stake_below_min_pos_skips_all(self):
        b, placed, skipped = _siz.sim_flat([1.0, 1.0], stake=2.0, b0=1000.0, min_pos=5.0)
        self.assertAlmostEqual(b, 1000.0)
        self.assertEqual(placed, 0)
        self.assertEqual(skipped, 2)

    def test_positive_returns_grow(self):
        b, placed, _ = _siz.sim_flat([0.5, 0.5], stake=10.0, b0=1000.0, min_pos=1.0)
        self.assertAlmostEqual(b, 1010.0)
        self.assertEqual(placed, 2)

    def test_bankroll_exhaustion_skips(self):
        # Stake > bankroll after losses → skip remaining.
        b, placed, skipped = _siz.sim_flat([-1.0, 0.5], stake=800.0, b0=1000.0, min_pos=1.0)
        # After first bet: 1000 - 800 = 200. 200 < 800 → skip second.
        self.assertAlmostEqual(b, 200.0)
        self.assertEqual(placed, 1)
        self.assertEqual(skipped, 1)


if __name__ == '__main__':
    unittest.main()
