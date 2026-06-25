#!/usr/bin/env python3
"""Drift guard for the bake-off selectors (issue #421, PR5).

``top_k`` takes the k best-ranked at weight 1.0 and ignores state; ``online_exp_weights`` carries
an EWMA across steps, concentrates weight on higher smoothed scores, and conforms to the stateful
``Selector`` Protocol. Deterministic (no RNG).

Run: ``python3 scripts/test_ranker_selectors.py``
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Selector  # noqa: E402
from ranker.selectors import SELECTOR_REGISTRY, OnlineExpWeights, TopK  # noqa: E402


def _scores(d: dict) -> pd.DataFrame:
    s = pd.Series(d, name="score")
    return pd.DataFrame({"score": s, "rank": s.rank(ascending=False, method="first").astype(int)})


class TopKTest(unittest.TestCase):
    def test_picks_best_ranked_weight_one(self) -> None:
        scores = _scores({"a": 0.9, "b": 0.5, "c": 0.7, "d": 0.1})
        fs, state = TopK().select(scores, k=2, state="carried")
        self.assertEqual(set(fs["wallet"]), {"a", "c"})          # top-2 by score
        self.assertTrue((fs["weight"] == 1.0).all())
        self.assertEqual(state, "carried")                       # stateless passthrough

    def test_k_larger_than_pool(self) -> None:
        fs, _ = TopK().select(_scores({"a": 1.0}), k=5, state=None)
        self.assertEqual(len(fs), 1)

    def test_conforms_to_protocol_and_registry(self) -> None:
        self.assertIsInstance(TopK(), Selector)
        self.assertIs(SELECTOR_REGISTRY["top_k"], TopK)


class OnlineExpWeightsTest(unittest.TestCase):
    def test_weights_sum_to_set_size_and_concentrate(self) -> None:
        sel = OnlineExpWeights(eta=4.0, alpha=1.0)               # alpha=1 -> no smoothing memory
        fs, state = sel.select(_scores({"a": 1.0, "b": 0.0, "c": 0.5}), k=3, state=None)
        self.assertAlmostEqual(float(fs["weight"].sum()), len(fs))  # mass == set size
        wa = float(fs.set_index("wallet").loc["a", "weight"])
        wb = float(fs.set_index("wallet").loc["b", "weight"])
        self.assertGreater(wa, wb)                               # higher score -> more weight
        self.assertIsInstance(state, dict)                       # state carries the smoothed scores

    def test_state_smooths_across_steps(self) -> None:
        sel = OnlineExpWeights(eta=4.0, alpha=0.5)
        _, state1 = sel.select(_scores({"a": 1.0, "b": 0.0}), k=2, state=None)
        # b surges; smoothing blends with its prior low score, so a still carries memory.
        fs2, _ = sel.select(_scores({"a": 0.0, "b": 1.0}), k=2, state=state1)
        self.assertAlmostEqual(float(fs2["weight"].sum()), 2.0)
        self.assertEqual(set(fs2["wallet"]), {"a", "b"})

    def test_k_zero_returns_empty_not_error(self) -> None:
        # F3 (#436 Phase F): k<=0 must short-circuit to an empty set; without the guard the empty
        # top-array's .max() raises ValueError on a zero-size reduction.
        sel = OnlineExpWeights()
        fs, state = sel.select(_scores({"a": 1.0, "b": 0.5}), k=0, state=None)
        self.assertEqual(len(fs), 0)
        self.assertIsInstance(state, dict)

    def test_conforms_to_protocol_and_registry(self) -> None:
        self.assertIsInstance(OnlineExpWeights(), Selector)
        self.assertIs(SELECTOR_REGISTRY["online_exp_weights"], OnlineExpWeights)


if __name__ == "__main__":
    unittest.main(verbosity=2)
