#!/usr/bin/env python3
"""Math-correctness + drift guard for `scripts/ranker_decay.py` (issue #366).

Imports ONLY `ranker_decay` (numpy) — never the pandas-laden ranking passes — so it stays a
fast, deterministic CI drift guard. Covers: decay-weight shape, weighted-statistics correctness
(incl. the flat-weight path being **bitwise-identical** to the legacy `np.mean`/`np.std(ddof=1)`),
the NaN guards, `--as-of` parsing, the canonical-default pins, and the relative-window relation.

This test runs in CI as part of `.github/workflows/ci.yml` (Python drift-guard step), after the
`pip install -r scripts/requirements.txt` step (it needs numpy).

Run: `python3 scripts/test_ranker_decay.py`
  or: `pytest scripts/test_ranker_decay.py -v`
"""
import math
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ranker_decay as rd  # noqa: E402

SECS_PER_DAY = 86_400
AS_OF = 1_700_000_000  # fixed anchor — determinism (no clock in assertions)


class DecayWeightsTest(unittest.TestCase):
    def test_half_life_zero_is_flat(self) -> None:
        ets = [AS_OF - 10 * SECS_PER_DAY, AS_OF - SECS_PER_DAY, AS_OF]
        np.testing.assert_array_equal(rd.decay_weights(ets, AS_OF, 0), np.ones(3))

    def test_negative_half_life_is_flat(self) -> None:
        np.testing.assert_array_equal(rd.decay_weights([AS_OF, AS_OF - 5], AS_OF, -7), np.ones(2))

    def test_weight_is_half_at_one_half_life(self) -> None:
        hl_days = 30.0
        ets = [AS_OF - int(hl_days * SECS_PER_DAY)]
        self.assertAlmostEqual(float(rd.decay_weights(ets, AS_OF, hl_days)[0]), 0.5, places=12)

    def test_recent_outweighs_older_monotonic(self) -> None:
        ets = [AS_OF - 90 * SECS_PER_DAY, AS_OF - 30 * SECS_PER_DAY, AS_OF]
        w = rd.decay_weights(ets, AS_OF, 30.0)
        self.assertTrue(w[0] < w[1] < w[2])
        self.assertAlmostEqual(float(w[2]), 1.0, places=12)  # age 0 -> weight 1

    def test_future_ts_clipped_to_one(self) -> None:
        w = rd.decay_weights([AS_OF + 10 * SECS_PER_DAY], AS_OF, 30.0)
        self.assertEqual(float(w[0]), 1.0)  # future age clipped to 0 -> exactly 1.0


class WeightedStatsFlatPathTest(unittest.TestCase):
    """The flat-weight path MUST be bitwise-identical to the legacy np.mean/np.std(ddof=1)."""

    VALUES = [0.10, -0.05, 0.30, 0.02, -0.20, 0.15, 0.40]

    def _legacy(self, vals):
        v = np.asarray(vals, dtype=float)
        n = v.size
        m = float(v.mean())
        sd = float(v.std(ddof=1)) if n > 1 else float("nan")
        t = (m / sd * math.sqrt(n)) if (sd and sd > 0 and n > 1) else float("nan")
        return m, sd, float(n), t

    def test_ones_weights_bitwise_identical(self) -> None:
        wmean, wstd, n_eff, tstat = rd.weighted_stats(self.VALUES, np.ones(len(self.VALUES)))
        m, sd, n, t = self._legacy(self.VALUES)
        self.assertEqual(wmean, m)      # exact, not allclose
        self.assertEqual(wstd, sd)
        self.assertEqual(n_eff, n)
        self.assertEqual(tstat, t)

    def test_equal_nonunit_weights_short_circuit(self) -> None:
        # All-equal weights (not 1.0) must hit the same short-circuit -> identical to unweighted.
        wmean, wstd, n_eff, tstat = rd.weighted_stats(self.VALUES, np.full(len(self.VALUES), 0.3))
        m, sd, n, t = self._legacy(self.VALUES)
        self.assertEqual((wmean, wstd, n_eff, tstat), (m, sd, n, t))

    def test_decay_weights_at_half_life_zero_round_trip(self) -> None:
        # The production flat path: decay_weights(half_life=0) -> ones -> bitwise legacy.
        ets = list(range(AS_OF - 6, AS_OF + 1))
        w = rd.decay_weights(ets, AS_OF, 0)
        wmean, wstd, _, tstat = rd.weighted_stats(self.VALUES, w)
        m, sd, _, t = self._legacy(self.VALUES)
        self.assertEqual((wmean, wstd, tstat), (m, sd, t))


class WeightedStatsGeneralTest(unittest.TestCase):
    def test_n_eff_is_kish(self) -> None:
        vals = [0.1, 0.2, 0.3, 0.4]
        w = np.array([0.1, 0.2, 0.4, 0.8])
        big_w, v2 = float(w.sum()), float((w * w).sum())
        _, _, n_eff, _ = rd.weighted_stats(vals, w)
        self.assertAlmostEqual(n_eff, big_w * big_w / v2, places=10)

    def test_weighted_mean_tilts_to_recent(self) -> None:
        # Older loss, recent win, 30d half-life: recent outweighs -> positive weighted mean.
        ets = [AS_OF - 60 * SECS_PER_DAY, AS_OF]
        w = rd.decay_weights(ets, AS_OF, 30.0)  # ~[0.25, 1.0]
        wmean_up, _, _, _ = rd.weighted_stats([-0.5, 0.5], w)
        wmean_dn, _, _, _ = rd.weighted_stats([0.5, -0.5], w)
        self.assertGreater(wmean_up, 0.0)
        self.assertLess(wmean_dn, 0.0)
        # raw mean of [-0.5, 0.5] is 0 -> the sign is purely the recency tilt.
        self.assertAlmostEqual(wmean_up, -wmean_dn, places=12)

    def test_unbiased_weighted_variance_formula(self) -> None:
        vals = np.array([0.1, -0.2, 0.3])
        w = np.array([0.5, 1.0, 2.0])
        wmean, wstd, _, _ = rd.weighted_stats(vals, w)
        big_w, v2 = float(w.sum()), float((w * w).sum())
        exp_mean = float((w * vals).sum() / big_w)
        exp_var = (big_w / (big_w * big_w - v2)) * float((w * (vals - exp_mean) ** 2).sum())
        self.assertAlmostEqual(wmean, exp_mean, places=12)
        self.assertAlmostEqual(wstd, math.sqrt(exp_var), places=12)


class WeightedStatsNaNGuardTest(unittest.TestCase):
    def test_empty(self) -> None:
        wmean, wstd, n_eff, tstat = rd.weighted_stats([], [])
        self.assertTrue(math.isnan(wmean) and math.isnan(wstd) and math.isnan(tstat))
        self.assertEqual(n_eff, 0.0)

    def test_single_value(self) -> None:
        wmean, wstd, n_eff, tstat = rd.weighted_stats([0.42], rd.decay_weights([AS_OF], AS_OF, 30.0))
        self.assertEqual(wmean, 0.42)        # weighted mean of one obs is the obs
        self.assertTrue(math.isnan(wstd))    # n<=1 -> std/tstat NaN (legacy parity)
        self.assertTrue(math.isnan(tstat))
        self.assertEqual(n_eff, 1.0)

    def test_zero_variance_is_std_zero_tstat_nan(self) -> None:
        # Identical values: std=0.0 (NOT NaN, legacy parity), tstat NaN.
        wmean, wstd, _, tstat = rd.weighted_stats([0.3, 0.3, 0.3], np.ones(3))
        self.assertEqual(wmean, 0.3)
        self.assertEqual(wstd, 0.0)
        self.assertTrue(math.isnan(tstat))

    def test_negative_weight_is_nan(self) -> None:
        # F3 (#436 Phase F): a negative reliability weight is invalid by contract — fail safe to NaN,
        # not a silently corrupted weighted statistic.
        wmean, wstd, n_eff, tstat = rd.weighted_stats([0.3, 0.5, 0.1], [1.0, -0.5, 1.0])
        self.assertTrue(math.isnan(wmean) and math.isnan(wstd) and math.isnan(tstat))
        self.assertEqual(n_eff, 0.0)


class ParseAsOfTest(unittest.TestCase):
    def test_none_and_empty(self) -> None:
        self.assertIsNone(rd.parse_as_of(None))
        self.assertIsNone(rd.parse_as_of(""))
        self.assertIsNone(rd.parse_as_of("   "))

    def test_bare_epoch(self) -> None:
        self.assertEqual(rd.parse_as_of("1700000000"), 1700000000)

    def test_iso_date_is_utc_midnight(self) -> None:
        exp = int(datetime(2026, 6, 17, tzinfo=timezone.utc).timestamp())
        self.assertEqual(rd.parse_as_of("2026-06-17"), exp)

    def test_iso_datetime_naive_is_utc(self) -> None:
        exp = int(datetime(2026, 6, 17, 12, 0, 0, tzinfo=timezone.utc).timestamp())
        self.assertEqual(rd.parse_as_of("2026-06-17T12:00:00"), exp)


class DefaultsDriftTest(unittest.TestCase):
    """Pin the canonical defaults to docs/_GLOSSARY.md. On an intentional change, co-update
    docs/_GLOSSARY.md (ranker_half_life_days / ranker_window_days) AND these literals together."""

    def test_constants_match_glossary(self) -> None:
        self.assertEqual(rd.DEFAULT_HALF_LIFE_DAYS, 30.0)  # ranker_half_life_days
        self.assertEqual(rd.DEFAULT_WINDOW_DAYS, 180)      # ranker_window_days
        self.assertEqual(rd.SECS_PER_DAY, 86_400)


class RelativeDefaultTest(unittest.TestCase):
    """Assert the RELATION (not a calendar-pinned value) so it stays deterministic across days."""

    def test_window_start_is_window_days_before_end(self) -> None:
        win_end = int(datetime(2026, 6, 17, tzinfo=timezone.utc).timestamp())
        self.assertEqual(
            rd.window_start_unix(win_end, rd.DEFAULT_WINDOW_DAYS),
            win_end - rd.DEFAULT_WINDOW_DAYS * SECS_PER_DAY,
        )

    def test_today_midnight_is_a_utc_midnight(self) -> None:
        tm = rd.today_midnight_unix()
        self.assertEqual(tm % SECS_PER_DAY, 0)  # midnight property holds every calendar day


if __name__ == "__main__":
    unittest.main(verbosity=2)
