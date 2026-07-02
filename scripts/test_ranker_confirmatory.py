#!/usr/bin/env python3
"""#466: the confirmatory small-grid bake-off mode — pre-registered 13-config MinTRL-20 family.

T1 the grid is a 13-member subset of the full axes + the baseline key matches `_baseline_key`.
T2 a `grid_override` run scores exactly 13 with manifest `n_grid=13` / `n_grid_full=960`, and the
   honest Deflated-Sharpe bar receives `n_trials_dsr=960` (the full pre-screen multiplicity).
T3 the verdict is byte-identical with vs without the CLV computation (the inertness proof).
T4 membership / baseline-present / uniqueness validation raises `ValueError`.
T5 `build_default_axes(estimators)` ≡ `build_confirmatory_axes(estimators)` (the drift guard).
T6 `clv_diagnostic` returns the four keys with independent `0 ≤ cov ≤ 1`, finite t-stats given ≥2
   valid wallets, and `(nan, 0.0)` on an empty follow set.
T7 a never-live trajectory yields `(nan, 0.0)` without an `UnboundLocalError` (the crash guard).

Reuses the `_axes` / `_population` / `_FakeRunner` fixtures from `test_ranker_bakeoff.py`.
"""
import os
import sys
import unittest
from pathlib import Path

# Pin BLAS to one thread BEFORE numpy import (mirrors bakeoff.py / test_ranker_bakeoff.py): the
# process executor forks from this process, so the deadlock-safety mitigation must be set here too.
for _blas_var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_blas_var, "1")

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Criteria  # noqa: E402
from ranker import bakeoff as bo  # noqa: E402
from ranker import confirmatory_grid as cg  # noqa: E402
from ranker.estimators import clv_diagnostic  # noqa: E402
from test_ranker_bakeoff import _FakeRunner, _population  # noqa: E402


def _confirmatory_setup(*, good=6, bad=6, n_pos=120, steps=10, max_backtests=10 ** 9):
    """A live good/bad population dense enough that the MinTRL-20 challengers have >= 20 in-sample
    positions per wallet (so they produce signal and the verdict reaches `grid_deflate`), plus the
    full confirmatory axes (n_grid_full = 960) and its 13-config override. `max_backtests` is lifted
    so the synthetic step count never trips the 960×steps ceiling (which gates on the FULL axes)."""
    ss, skill = _population(8, good=good, bad=bad, n_pos=n_pos)
    points = [3_000_000 + i * 1_000_000 for i in range(steps)]
    value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
    runner = _FakeRunner(value, points, 1_000_000)
    estimators = tuple(bo.ESTIMATOR_REGISTRY)
    axes = cg.build_confirmatory_axes(estimators)
    override = cg.build_confirmatory_grid()
    params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                              k=5, screen_keep=2, demoter_kwargs={"min_periods": 3}, min_periods=2,
                              max_backtests=max_backtests)
    return ss, axes, params, runner, override


class T1_GridMembershipTest(unittest.TestCase):
    def test_grid_is_13_member_subset_with_matching_baseline(self) -> None:
        estimators = tuple(bo.ESTIMATOR_REGISTRY)
        axes = cg.build_confirmatory_axes(estimators)
        grid = cg.build_confirmatory_grid()
        self.assertEqual(len(grid), 13)
        self.assertEqual(len({g.key for g in grid}), 13)              # unique keys
        full = {g.key for g in axes.enumerate_grid()}
        for g in grid:
            self.assertIn(g.key, full)                               # every config is a member
        self.assertEqual(cg._BASELINE_GP.key, bo._baseline_key(axes))  # baseline reconstruction agrees
        cg.validate_confirmatory_grid(axes)                          # the full integrity check passes
        # The family: 1 MinTRL-0 baseline + 12 MinTRL-20 challengers, all deflator "none".
        self.assertEqual(sum(1 for g in grid if g.criteria.min_trl == 0), 1)
        self.assertEqual(sum(1 for g in grid if g.criteria.min_trl == 20), 12)
        self.assertTrue(all(g.deflator == bo.NO_DEFLATION for g in grid))
        challengers = [g for g in grid if g != cg._BASELINE_GP]
        self.assertTrue(all(g.policy in ("policy_online_weighting", "policy_hybrid_displacement")
                            for g in challengers))
        self.assertTrue(all(g.churn_cost == bo.RANKER_CHURN_COST_USD for g in challengers))


class T2_OverrideHonestMultiplicityTest(unittest.TestCase):
    def test_override_scores_13_records_960_and_dsr_uses_960(self) -> None:
        ss, axes, params, runner, override = _confirmatory_setup()
        captured = {}
        orig = bo.grid_deflate

        def spy(return_matrix, *, benchmark, n_grid, n_trials_dsr=None, seed=0):
            captured["n_grid"] = n_grid
            captured["n_trials_dsr"] = n_trials_dsr
            return orig(return_matrix, benchmark=benchmark, n_grid=n_grid,
                        n_trials_dsr=n_trials_dsr, seed=seed)

        try:
            bo.grid_deflate = spy
            res = bo.run_bakeoff(ss, runner, axes, params, created_at=1, grid_override=override)
        finally:
            bo.grid_deflate = orig
        self.assertEqual(res["manifest"]["n_grid"], 13)              # validators run on 13 columns
        self.assertEqual(res["manifest"]["n_grid_full"], 960)        # provenance: full pre-screen count
        self.assertTrue(res["manifest"]["confirmatory"])
        self.assertEqual(res["return_matrix"].shape[1], 13)          # exactly 13 configs executed
        self.assertEqual(res["decision"]["n_grid_full_pre_screen"], 960)
        self.assertEqual(captured.get("n_grid"), 13)                 # bootstrap validators: 13-col matrix
        self.assertEqual(captured.get("n_trials_dsr"), 960)          # honest DSR bar: full 960 tax


class T3_ClvInertnessTest(unittest.TestCase):
    def test_clv_computation_is_inert_to_the_verdict(self) -> None:
        ss, axes, params, runner, override = _confirmatory_setup()
        base = bo.run_bakeoff(ss, runner, axes, params, created_at=1, grid_override=override)
        orig = bo.clv_diagnostic
        try:
            # Replace the diagnostic with a garbage-but-finite value: if it were anything but inert,
            # the verdict would move. (run_trajectory looks up the module global at call time.)
            bo.clv_diagnostic = lambda *a, **k: {"proxy_clv_tstat": 1234.5, "proxy_clv_cov": 0.99,
                                                 "true_clv_tstat": -1234.5, "true_clv_cov": 0.01}
            alt = bo.run_bakeoff(ss, runner, axes, params, created_at=1, grid_override=override)
        finally:
            bo.clv_diagnostic = orig
        self.assertEqual(base["decision"]["status"], alt["decision"]["status"])
        self.assertEqual(base["decision"]["winner"], alt["decision"]["winner"])
        self.assertEqual(base["decision"]["reason"], alt["decision"]["reason"])
        pd.testing.assert_frame_equal(base["return_matrix"], alt["return_matrix"])
        # ...and the garbage CLV DID flow into the leaderboard (proves it actually ran, not skipped).
        lb = alt["decision"]["leaderboard"]
        self.assertIn("proxy_clv_cov", lb.columns)
        self.assertTrue((lb["proxy_clv_cov"] == 0.99).any())


class T4_OverrideValidationTest(unittest.TestCase):
    def test_member_baseline_and_uniqueness_validation_raises(self) -> None:
        ss, axes, params, runner, _ = _confirmatory_setup(good=3, bad=3, n_pos=40, steps=4)
        bogus = bo.GridPoint("not_an_estimator", bo.NO_DEFLATION, "policy_full_rerank",
                             cg._CRIT_BASELINE, 0.0)
        with self.assertRaises(ValueError):                          # config absent from the full axes
            bo.run_bakeoff(ss, runner, axes, params, created_at=1,
                           grid_override=[cg._BASELINE_GP, bogus])
        with self.assertRaises(ValueError):                          # missing the baseline config
            bo.run_bakeoff(ss, runner, axes, params, created_at=1,
                           grid_override=list(cg._CHALLENGERS))
        with self.assertRaises(ValueError):                          # duplicate config keys
            bo.run_bakeoff(ss, runner, axes, params, created_at=1,
                           grid_override=[cg._BASELINE_GP, cg._BASELINE_GP])

    def test_validate_raises_when_a_challenger_estimator_is_excluded(self) -> None:
        # A <30%-true_clv cache excludes true_clv, so the 4 true_clv challengers fall out of the axes;
        # the full integrity check then raises (the operator path filters the grid instead).
        axes = cg.build_confirmatory_axes(("eb_shrinkage_skill", "t_stat_baseline"))
        with self.assertRaises(ValueError):
            cg.validate_confirmatory_grid(axes)


class T5_AxesDriftGuardTest(unittest.TestCase):
    def test_default_axes_matches_confirmatory_axes(self) -> None:
        tup = tuple(bo.ESTIMATOR_REGISTRY)
        self.assertEqual(bo.build_default_axes(estimators=tup), cg.build_confirmatory_axes(tup))
        self.assertEqual(len(cg.build_confirmatory_axes(tup).enumerate_grid()), 960)


class T6_ClvDiagnosticContractTest(unittest.TestCase):
    def _in_sample(self) -> pd.DataFrame:
        # 3 wallets × 4 positions; close_proxy fully covered, true_clv_close covered for 2/3 wallets.
        proxy = {"a": [0.60, 0.65, 0.55, 0.62], "b": [0.30, 0.35, 0.32, 0.28],
                 "c": [0.50, 0.52, 0.48, 0.51]}
        true = {"a": [0.61, 0.66, 0.56, 0.63], "b": [0.31, 0.36, 0.33, 0.29],
                "c": [float("nan")] * 4}
        rows = [(w, 0.40, proxy[w][i], true[w][i]) for w in ("a", "b", "c") for i in range(4)]
        return pd.DataFrame(
            rows, columns=["wallet", "price", "close_proxy", "true_clv_close"]).reset_index(drop=True)

    def test_keys_independent_coverage_finite_and_empty_guard(self) -> None:
        ins = self._in_sample()
        follow = pd.DataFrame({"wallet": ["a", "b", "c"], "weight": [1.0, 1.0, 1.0]})
        out = clv_diagnostic(ins, follow, np.ones(len(ins)))
        self.assertEqual(set(out), {"proxy_clv_tstat", "proxy_clv_cov",
                                    "true_clv_tstat", "true_clv_cov"})
        self.assertTrue(0.0 <= out["proxy_clv_cov"] <= 1.0)
        self.assertTrue(0.0 <= out["true_clv_cov"] <= 1.0)          # independent of proxy_cov
        self.assertEqual(out["proxy_clv_cov"], 1.0)
        self.assertAlmostEqual(out["true_clv_cov"], 8 / 12)
        self.assertTrue(np.isfinite(out["proxy_clv_tstat"]))        # >= 2 valid wallets -> finite
        self.assertTrue(np.isfinite(out["true_clv_tstat"]))
        empty = clv_diagnostic(ins, pd.DataFrame({"wallet": [], "weight": []}), np.ones(len(ins)))
        self.assertTrue(np.isnan(empty["proxy_clv_tstat"]) and empty["proxy_clv_cov"] == 0.0)
        self.assertTrue(np.isnan(empty["true_clv_tstat"]) and empty["true_clv_cov"] == 0.0)


class T7_NeverLiveCrashGuardTest(unittest.TestCase):
    def test_never_live_trajectory_is_all_zero_without_crash(self) -> None:
        ss, _ = _population(8, good=3, bad=3, n_pos=40)
        points = [3_000_000 + i * 1_000_000 for i in range(4)]
        runner = _FakeRunner({}, points, 1_000_000)
        # A price band excluding EVERY position (price 0.50 ∉ [0.99, 1.0]) -> the criteria slice is
        # empty in every period -> candidates empty -> `weights` never bound, final_follow empty.
        never_live = bo.GridPoint("t_stat_baseline", bo.NO_DEFLATION, "policy_full_rerank",
                                  Criteria(0, 72.0, 0.99, 1.0, 0.0, 0), 0.0)
        res = bo.run_trajectory(never_live, ss, runner, as_of_points=points, train_secs=3_000_000,
                                horizon_secs=1_000_000, k=5, displacement_margin=5)
        self.assertTrue(res.final_follow.empty)
        # A2 (2026-07-01 decision record, #417; #475): a no-signal period records an economic
        # $0, so a never-live trajectory is all-ZERO returns (not all-NaN as pre-A2).
        self.assertTrue((res.returns == 0.0).all())
        self.assertTrue(np.isnan(res.proxy_clv_tstat))
        self.assertEqual(res.proxy_clv_cov, 0.0)
        self.assertTrue(np.isnan(res.true_clv_tstat))
        self.assertEqual(res.true_clv_cov, 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
