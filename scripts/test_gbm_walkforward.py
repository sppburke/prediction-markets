"""Tests for gbm_walkforward module-local helpers (issue #260 §C).

Mirrors the TestComputePbo pattern in test_composite_tuner.py.
Tests pbo_result_to_dict (serialiser) and compute_pbo integration as consumed
by gbm_walkforward — no DB, no GBM training needed.
"""
import math
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gbm_walkforward import pbo_result_to_dict  # noqa: E402
from composite_tuner.pbo import compute_pbo  # noqa: E402


class TestPboResultToDict(unittest.TestCase):
    """pbo_result_to_dict serialises PboResult to a JSON-safe dict."""

    def test_passthrough_numeric_fields(self):
        # (5, 5) matrix — compute_pbo runs and returns a finite result; dict
        # fields should match the PboResult fields (logit_values not included).
        rng = np.random.default_rng(7)
        scores = rng.normal(size=(5, 5))
        result = compute_pbo(scores, n_perms=20, rng_seed=42)
        verdict = 'undefined' if math.isnan(result.pbo) else (
            'OVERFIT' if result.pbo > 0.5 else 'OK'
        )
        d = pbo_result_to_dict(result, verdict)
        self.assertEqual(d['pbo'], result.pbo)
        self.assertEqual(d['n_perms'], result.n_perms)
        self.assertEqual(d['n_trials'], result.n_trials)
        self.assertEqual(d['n_windows'], result.n_windows)
        self.assertEqual(d['median_oos_rank'], result.median_oos_rank)
        self.assertEqual(d['verdict'], verdict)
        self.assertNotIn('logit_values', d)

    def test_undefined_when_too_few_trials(self):
        # (1, 5) → compute_pbo returns NaN; dict carries that NaN and verdict='undefined'.
        result = compute_pbo(np.zeros((1, 5)), n_perms=10)
        d = pbo_result_to_dict(result, 'undefined')
        self.assertTrue(math.isnan(d['pbo']))
        self.assertEqual(d['verdict'], 'undefined')
        self.assertEqual(d['n_trials'], 1)
        self.assertEqual(d['n_windows'], 5)

    def test_undefined_when_too_few_windows(self):
        # (5, 1) → compute_pbo returns NaN; same outcome.
        result = compute_pbo(np.zeros((5, 1)), n_perms=10)
        d = pbo_result_to_dict(result, 'undefined')
        self.assertTrue(math.isnan(d['pbo']))
        self.assertEqual(d['verdict'], 'undefined')

    def test_deterministic_with_seed(self):
        # Identical inputs → identical pbo value.
        rng = np.random.default_rng(99)
        scores = rng.normal(size=(5, 5))
        r1 = compute_pbo(scores, n_perms=20, rng_seed=42)
        r2 = compute_pbo(scores, n_perms=20, rng_seed=42)
        self.assertEqual(r1.pbo, r2.pbo)
        d1 = pbo_result_to_dict(r1, 'OK' if r1.pbo <= 0.5 else 'OVERFIT')
        d2 = pbo_result_to_dict(r2, 'OK' if r2.pbo <= 0.5 else 'OVERFIT')
        self.assertEqual(d1['pbo'], d2['pbo'])
        self.assertEqual(d1['verdict'], d2['verdict'])

    def test_overfit_verdict_on_noise_matrix(self):
        # Pure noise matrix: best-IS trial is random → best-IS OOS rank is below
        # median about half the time → pbo ≈ 0.5; with many noisy trials the
        # convergence is slow, so we just assert pbo is finite and verdict maps
        # correctly to whichever side 0.5 it lands on.
        rng = np.random.default_rng(0)
        # Use a deliberately imbalanced matrix where best-IS is consistently
        # the worst-OOS trial (i.e., trial 0 dominates IS but flips to last OOS).
        n_trials, n_windows = 20, 10
        # Construct: trial 0 scores high on first half (IS), low on second (OOS).
        scores = np.zeros((n_trials, n_windows))
        scores[0, :n_windows // 2] = 10.0    # dominates IS windows
        scores[0, n_windows // 2:] = -10.0   # worst on OOS windows
        result = compute_pbo(scores, n_perms=100, rng_seed=42)
        self.assertFalse(math.isnan(result.pbo))
        verdict = 'OVERFIT' if result.pbo > 0.5 else 'OK'
        d = pbo_result_to_dict(result, verdict)
        self.assertEqual(d['verdict'], 'OVERFIT')
        self.assertGreater(d['pbo'], 0.5)


if __name__ == '__main__':
    unittest.main()
