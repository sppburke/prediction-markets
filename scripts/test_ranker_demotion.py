#!/usr/bin/env python3
"""Drift guard for the bake-off demoter (issue #421, PR5).

The empirical-Bernstein demoter fires only on a PROVEN loser — the optimistic upper confidence
bound on per-period P&L is still negative AND cumulative P&L is negative AND there is enough
evidence (>= min_periods). It must NOT fire on a winner, a noisy break-even wallet, or a wallet
with too few periods. Deterministic (no RNG).

Run: ``python3 scripts/test_ranker_demotion.py``
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Demoter  # noqa: E402
from ranker.demotion import DEMOTER_REGISTRY, EmpiricalBernsteinDemoter  # noqa: E402


def _pnl(wallet: str, values: list, *, start: int = 100) -> pd.DataFrame:
    return pd.DataFrame({
        "wallet": [wallet] * len(values),
        "period_end": [start + i for i in range(len(values))],
        "realized_pnl": values,
    })


class EmpiricalBernsteinDemoterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.demoter = EmpiricalBernsteinDemoter(delta=0.05, min_periods=5)

    def test_proven_loser_is_demoted(self) -> None:
        # 12 consistent losses, low variance -> upper bound stays below 0.
        pnl = _pnl("loser", [-9.0, -10.0, -11.0, -10.0, -9.5, -10.5] * 2)
        self.assertTrue(self.demoter.should_demote("loser", pnl, as_of=10_000))

    def test_winner_not_demoted(self) -> None:
        pnl = _pnl("winner", [8.0, 9.0, 7.0, 10.0, 9.5, 8.5, 9.0])
        self.assertFalse(self.demoter.should_demote("winner", pnl, as_of=10_000))

    def test_noisy_breakeven_not_demoted(self) -> None:
        # negative mean but huge variance -> the optimistic bound is > 0 (not PROVEN).
        pnl = _pnl("noisy", [-50.0, 49.0, -48.0, 47.0, -46.0, 45.0, -2.0])
        self.assertFalse(self.demoter.should_demote("noisy", pnl, as_of=10_000))

    def test_too_few_periods_not_demoted(self) -> None:
        pnl = _pnl("thin", [-10.0, -10.0, -10.0])               # n=3 < min_periods=5
        self.assertFalse(self.demoter.should_demote("thin", pnl, as_of=10_000))

    def test_only_counts_periods_up_to_as_of(self) -> None:
        pnl = _pnl("loser", [-10.0] * 8, start=100)             # period_end 100..107
        self.assertFalse(self.demoter.should_demote("loser", pnl, as_of=103))  # only 4 <= as_of

    def test_demote_set_matches_should_demote_loop(self) -> None:
        # `demote_set` (one groupby over the closed periods) is BIT-IDENTICAL to calling `should_demote`
        # per wallet — the O(n) replacement for the per-incumbent object-`==` rescan the policy did in
        # its selection loop. Mixed population so the kept set is non-trivial.
        lp = pd.concat([_pnl("loser", [-10.0] * 8, start=100),
                        _pnl("winner", [10.0] * 8, start=100),
                        _pnl("noisy", [1.0, -1.0, 1.0, -1.0, 1.0, -1.0], start=100),
                        _pnl("short", [-10.0] * 3, start=100)], ignore_index=True)
        for as_of in (103, 200, 10_000):
            loop = {w for w in lp["wallet"].unique()
                    if self.demoter.should_demote(w, lp, as_of=as_of)}
            self.assertEqual(self.demoter.demote_set(lp, as_of=as_of), loop)

    def test_empty_live_pnl(self) -> None:
        empty = pd.DataFrame(columns=["wallet", "period_end", "realized_pnl"])
        self.assertFalse(self.demoter.should_demote("any", empty, as_of=10_000))

    def test_conforms_to_protocol_and_registry(self) -> None:
        self.assertIsInstance(self.demoter, Demoter)
        self.assertIs(DEMOTER_REGISTRY["empirical_bernstein"], EmpiricalBernsteinDemoter)


if __name__ == "__main__":
    unittest.main(verbosity=2)
