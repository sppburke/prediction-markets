#!/usr/bin/env python3
"""Drift guard for the bake-off set-transition policies (issue #421, PR5).

The policy is the first-class axis (unit of evaluation). Covers all four required members:
``policy_full_rerank`` (memoryless wholesale replace), ``policy_knockout_backfill`` (keep + evict
proven losers + backfill, with a cold-start), ``policy_hybrid_displacement`` (a much-higher-ranked
fresh wallet displaces an unproven incumbent), and ``policy_online_weighting`` (soft state-carrying
weights). A fake ``Demoter`` makes eviction deterministic. No RNG.

Run: ``python3 scripts/test_ranker_policies.py``
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import SetTransitionPolicy  # noqa: E402
from ranker.policies import (  # noqa: E402
    POLICY_REGISTRY,
    FullRerank,
    HybridDisplacement,
    KnockoutBackfill,
    OnlineWeighting,
)
from ranker.selectors import OnlineExpWeights  # noqa: E402

_PNL_COLS = ["wallet", "period_end", "realized_pnl"]


def _fresh(ranks: dict) -> pd.DataFrame:
    """WalletScores from a {wallet: rank} map (score = -rank so higher score = better rank)."""
    s = pd.Series(ranks)
    return pd.DataFrame({"score": -s.astype(float), "rank": s.astype(int)})


def _follow(wallets: list) -> pd.DataFrame:
    return pd.DataFrame({"wallet": wallets, "weight": [1.0] * len(wallets)})


class _FakeDemoter:
    def __init__(self, demote, min_periods: int = 5):
        self.demote = set(demote)
        self.min_periods = min_periods

    def should_demote(self, wallet, live_pnl, *, as_of) -> bool:
        return wallet in self.demote


class FullRerankTest(unittest.TestCase):
    def test_replaces_with_fresh_top_k(self) -> None:
        pol = FullRerank(2)
        fresh = _fresh({"a": 3, "b": 1, "c": 2})
        out = pol.step(_follow(["x", "y"]), fresh, pd.DataFrame(columns=_PNL_COLS), as_of=0)
        self.assertEqual(set(out["wallet"]), {"b", "c"})         # ignores prev; fresh top-2
        self.assertTrue((out["weight"] == 1.0).all())


class KnockoutBackfillTest(unittest.TestCase):
    def test_evicts_proven_and_backfills(self) -> None:
        pol = KnockoutBackfill(3, _FakeDemoter({"b"}))
        fresh = _fresh({"d": 1, "e": 2, "a": 3, "c": 4, "b": 5})
        out = pol.step(_follow(["a", "b", "c"]), fresh, pd.DataFrame(columns=_PNL_COLS), as_of=0)
        self.assertEqual(set(out["wallet"]), {"a", "c", "d"})    # b evicted; d backfills

    def test_cold_start_is_top_k(self) -> None:
        pol = KnockoutBackfill(2, _FakeDemoter(set()))
        fresh = _fresh({"a": 1, "b": 2, "c": 3})
        out = pol.step(_follow([]), fresh, pd.DataFrame(columns=_PNL_COLS), as_of=0)
        self.assertEqual(set(out["wallet"]), {"a", "b"})

    def test_keeps_undemoted_incumbents(self) -> None:
        pol = KnockoutBackfill(2, _FakeDemoter(set()))
        fresh = _fresh({"a": 5, "b": 6, "z": 1})                 # z ranks best but the set is full
        out = pol.step(_follow(["a", "b"]), fresh, pd.DataFrame(columns=_PNL_COLS), as_of=0)
        self.assertEqual(set(out["wallet"]), {"a", "b"})         # hysteresis: no eviction, no room


class HybridDisplacementTest(unittest.TestCase):
    def test_high_fresh_displaces_unproven_incumbent(self) -> None:
        pol = HybridDisplacement(2, _FakeDemoter(set(), min_periods=5), displacement_margin=2)
        fresh = _fresh({"a": 10, "b": 5, "x": 1})                # x far better than incumbent a
        out = pol.step(_follow(["a", "b"]), fresh, pd.DataFrame(columns=_PNL_COLS), as_of=0)
        self.assertIn("x", set(out["wallet"]))                   # x displaces unproven incumbent a
        self.assertNotIn("a", set(out["wallet"]))

    def test_proven_incumbent_protected(self) -> None:
        # a has >= min_periods of live evidence -> proven -> NOT displaceable.
        live = pd.DataFrame({"wallet": ["a"] * 6, "period_end": list(range(6)),
                             "realized_pnl": [1.0] * 6})
        pol = HybridDisplacement(2, _FakeDemoter(set(), min_periods=5), displacement_margin=2)
        fresh = _fresh({"a": 10, "b": 5, "x": 1})
        out = pol.step(_follow(["a", "b"]), fresh, live, as_of=100)
        self.assertIn("a", set(out["wallet"]))                   # proven a survives despite x


class OnlineWeightingTest(unittest.TestCase):
    def test_soft_weights_and_state_carries(self) -> None:
        pol = OnlineWeighting(2, OnlineExpWeights(eta=4.0, alpha=0.5))
        empty = pd.DataFrame(columns=_PNL_COLS)
        out1 = pol.step(_follow([]), _fresh({"a": 1, "b": 2, "c": 3}), empty, as_of=0)
        self.assertAlmostEqual(float(out1["weight"].sum()), 2.0)  # soft mass == k
        self.assertIsNotNone(pol._state)                          # state carried on the instance
        out2 = pol.step(out1, _fresh({"a": 1, "b": 2, "c": 3}), empty, as_of=1)
        self.assertAlmostEqual(float(out2["weight"].sum()), 2.0)


class ProtocolRegistryTest(unittest.TestCase):
    def test_all_four_registered_and_conform(self) -> None:
        self.assertEqual(set(POLICY_REGISTRY), {
            "policy_full_rerank", "policy_knockout_backfill",
            "policy_hybrid_displacement", "policy_online_weighting"})
        self.assertIsInstance(FullRerank(1), SetTransitionPolicy)
        self.assertIsInstance(KnockoutBackfill(1, _FakeDemoter(set())), SetTransitionPolicy)
        self.assertIsInstance(OnlineWeighting(1, OnlineExpWeights()), SetTransitionPolicy)


if __name__ == "__main__":
    unittest.main(verbosity=2)
