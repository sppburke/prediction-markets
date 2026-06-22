#!/usr/bin/env python3
"""Drift guard for the reference estimator ``eb_shrinkage_skill`` (issue #421, PR1).

Asserts the two core empirical-Bayes shrinkage properties on a deterministic synthetic
population (no RNG): (1) skilled wallets rank above null wallets, and (2) for the SAME raw
edge, more evidence -> a higher posterior score (the winner's-curse correction). Also checks
the ``WalletScores`` output contract.

Run: ``python3 scripts/test_ranker_estimators.py``
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker.estimators import REGISTRY, EBShrinkageSkill  # noqa: E402

PRICE = 0.50            # _eff = 0.51 for every synthetic position
_EFF = min(PRICE + 0.01, 0.999)
SKILLED = [f"skill{i}" for i in range(4)]
NULLS = [f"null{i}" for i in range(4)]


def _suff_stats(specs) -> pd.DataFrame:
    """specs: list of (wallet, n_trades, n_wins). First ``n_wins`` positions are payoff 1.0."""
    rows = []
    for wallet, n, wins in specs:
        rows.extend((wallet, 1.0 if k < wins else 0.0, PRICE, _EFF) for k in range(n))
    return pd.DataFrame(rows, columns=["wallet", "payoff", "price", "_eff"]).reset_index(drop=True)


class EBShrinkageTest(unittest.TestCase):
    def setUp(self) -> None:
        # 4 skilled (40 trades, 70% win) + 4 null (40 trades, 50% win) + 1 low-evidence wallet
        # with the SAME 70% win-rate as the skilled but only 10 trades.
        specs = (
            [(w, 40, 28) for w in SKILLED]
            + [(w, 40, 20) for w in NULLS]
            + [("low_n", 10, 7)]
        )
        self.ss = _suff_stats(specs)
        self.scores = EBShrinkageSkill().score(
            self.ss, as_of=0, weights=np.ones(len(self.ss)))

    def test_output_contract(self) -> None:
        self.assertEqual(list(self.scores.columns), ["score", "rank"])
        self.assertEqual(self.scores.index.name, "wallet")
        self.assertEqual(sorted(self.scores["rank"].tolist()),
                         list(range(1, len(self.scores) + 1)))  # 1..k, unique

    def test_skilled_take_the_top_ranks(self) -> None:
        top4 = set(self.scores.nsmallest(4, "rank").index)
        self.assertEqual(top4, set(SKILLED))

    def test_skilled_score_above_null(self) -> None:
        skilled = self.scores.loc[SKILLED, "score"].mean()
        null = self.scores.loc[NULLS, "score"].mean()
        self.assertGreater(skilled, null)
        self.assertGreater(skilled, 0.9)  # skilled wallets are confidently positive

    def test_more_evidence_scores_higher(self) -> None:
        # Same raw win-rate (70%), 40 trades vs 10 -> the 40-trade wallet is more certain.
        self.assertGreater(self.scores.loc["skill0", "score"],
                           self.scores.loc["low_n", "score"])


class DegenerateInputTest(unittest.TestCase):
    def test_single_candidate_returns_rank_one_not_empty(self) -> None:
        # < 2 candidates -> prior var is NaN; the floor must keep it deterministic (not empty).
        ss = _suff_stats([("solo", 3, 2)])
        out = EBShrinkageSkill().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertEqual(len(out), 1)
        self.assertEqual(out.loc["solo", "rank"], 1)


class RegistryTest(unittest.TestCase):
    def test_eb_shrinkage_registered_by_name(self) -> None:
        self.assertIs(REGISTRY[EBShrinkageSkill.name], EBShrinkageSkill)


if __name__ == "__main__":
    unittest.main(verbosity=2)
