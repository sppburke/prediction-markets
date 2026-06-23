#!/usr/bin/env python3
"""Drift guard for the deflation module (issue #421, PR2).

Asserts the Deflated Sharpe Ratio behaves per Bailey-LdP 2014: the false-strategy threshold and
DSR are monotone in the number of trials (more trials -> a higher bar -> lower DSR), DSR rewards
higher SR, and the ``Deflator`` Protocol is satisfied.

Run: ``python3 scripts/test_ranker_deflation.py``
"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Deflator  # noqa: E402
from ranker.deflation import (  # noqa: E402
    DEFLATOR_REGISTRY,
    DeflatedSharpe,
    deflated_sharpe_ratio,
    expected_max_sharpe,
)


class ExpectedMaxSharpeTest(unittest.TestCase):
    def test_monotone_in_trials(self) -> None:
        self.assertLess(expected_max_sharpe(10, 0.01), expected_max_sharpe(5000, 0.01))

    def test_degenerate(self) -> None:
        self.assertEqual(expected_max_sharpe(1, 0.01), 0.0)   # no selection
        self.assertEqual(expected_max_sharpe(100, 0.0), 0.0)  # no dispersion


class DeflatedSharpeRatioTest(unittest.TestCase):
    def test_monotone_in_trials(self) -> None:
        # Same observed SR; more trials -> higher SR0 -> lower DSR.
        sr0_few = expected_max_sharpe(10, 0.01)
        sr0_many = expected_max_sharpe(5000, 0.01)
        dsr_few = deflated_sharpe_ratio(0.3, 300, 0.0, 3.0, sr0=sr0_few)
        dsr_many = deflated_sharpe_ratio(0.3, 300, 0.0, 3.0, sr0=sr0_many)
        self.assertGreater(dsr_few, dsr_many)

    def test_sanity(self) -> None:
        self.assertGreater(deflated_sharpe_ratio(1.0, 1000, 0.0, 3.0, sr0=0.0), 0.99)  # strong SR
        self.assertAlmostEqual(deflated_sharpe_ratio(0.2, 1000, 0.0, 3.0, sr0=0.2), 0.5, places=6)

    def test_too_few_obs_is_nan(self) -> None:
        import math
        self.assertTrue(math.isnan(deflated_sharpe_ratio(0.3, 1, 0.0, 3.0, sr0=0.0)))


class DeflatorProtocolTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scores = pd.DataFrame(
            {"sr": [0.4, 0.1, 0.05], "n_obs": [300, 300, 300],
             "skew": [0.0, 0.0, 0.0], "kurt": [3.0, 3.0, 3.0]},
            index=["a", "b", "c"],
        )

    def test_conforms_to_protocol(self) -> None:
        self.assertIsInstance(DeflatedSharpe(), Deflator)
        self.assertIs(DEFLATOR_REGISTRY["deflated_sharpe"], DeflatedSharpe)

    def test_adds_dsr_higher_sr_higher_dsr(self) -> None:
        out = DeflatedSharpe().deflate(self.scores, n_trials=100, as_of=0)
        self.assertIn("dsr", out.columns)
        self.assertGreater(out.loc["a", "dsr"], out.loc["c", "dsr"])
        self.assertTrue(((out["dsr"] >= 0) & (out["dsr"] <= 1)).all())

    def test_missing_columns_raises(self) -> None:
        bare = pd.DataFrame({"score": [0.9], "rank": [1]})            # not a Sharpe-moment frame
        with self.assertRaises(ValueError):
            DeflatedSharpe().deflate(bare, n_trials=10, as_of=0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
