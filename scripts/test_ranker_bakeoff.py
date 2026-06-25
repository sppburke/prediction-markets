#!/usr/bin/env python3
"""Drift guard for the bake-off driver (issue #421, PR5, step 8).

Covers the staged sweep end-to-end with NO cache / NO network (a fake ``BacktestRunner`` and
synthetic frames): grid pre-registration + ``N_GRID``; the static criteria slice and per-``as_of``
eligibility gates; the per-wallet Deflated-Sharpe gate; the ``pe-backtest`` NDJSON parse and the
Supabase ``paper_fills`` row parse; estimator screening (8a); a policy trajectory (8c) incl. the
churn deduction; the §Acceptance winner / NO-GO decision; the AKM/MRSW/FCR winner uncertainty; and
the wallet-persistence CPR premise. Determinism is asserted (fixed seeds; the bootstrap Validators
seed at 0).

Run: ``python3 scripts/test_ranker_bakeoff.py``
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Criteria  # noqa: E402
from ranker import bakeoff as bo  # noqa: E402
from ranker.deflation import DeflatedSharpe  # noqa: E402
from ranker.suff_stats import derive_columns  # noqa: E402

_POLICIES = ("policy_full_rerank", "policy_knockout_backfill",
             "policy_hybrid_displacement", "policy_online_weighting")


def _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
          deflators=(bo.NO_DEFLATION,), policies=_POLICIES,
          criteria=(Criteria(0, 72.0, 0.0, 1.0, 0.0, 0),), churn=(0.0,)) -> bo.BakeoffAxes:
    return bo.BakeoffAxes(estimators=estimators, deflators=deflators, policies=policies,
                          criteria=criteria, churn_costs=churn)


def _population(seed: int, *, good=6, bad=14, n_pos=60, span=9_000_000):
    """A persistent good/bad wallet population: ``good`` high-edge + ``bad`` low-edge wallets, each
    with ``n_pos`` positions spread over ``[0, span)``. ``skill`` maps wallet -> win probability."""
    rng = np.random.default_rng(seed)
    wallets = [f"0x{i:040x}" for i in range(good + bad)]
    skill = {w: (0.75 if i < good else 0.30) for i, w in enumerate(wallets)}
    rows = []
    for w in wallets:
        for _ in range(n_pos):
            entry = int(rng.integers(0, span))
            rows.append((w, f"{w}_{entry}", 1, entry, 3600, 0.50, 10,
                         1.0 if rng.random() < skill[w] else 0.0, entry + 5000, 0.55))
    raw = pd.DataFrame(rows, columns=["wallet", "market_id", "outcome_id", "entry_ts", "ttr_secs",
                                      "price", "contracts", "payoff", "resolved_at", "close_proxy"])
    return derive_columns(raw), skill


class _FakeRunner:
    """Deterministic ``BacktestRunner``: pays each followed wallet ``value[w] * flat_usd`` at a
    period_end inside every step window (the trajectory windows it per step)."""

    def __init__(self, value: dict, points: list, horizon: int):
        self.value = value
        self.points = points
        self.horizon = horizon

    def run(self, wallets, *, flat_usd):
        rows = [{"wallet": w, "period_end": t + self.horizon // 2,
                 "realized_pnl": self.value.get(w, 0.0) * flat_usd, "unrealized_pnl": 0.0,
                 "n_fills": 2, "notional": flat_usd * 2}
                for w in wallets for t in self.points]
        return pd.DataFrame(rows, columns=bo._PNL_COLUMNS)


class GridTest(unittest.TestCase):
    def test_enumerate_and_n_grid(self) -> None:
        axes = _axes(estimators=("a", "b", "c"), deflators=("none", "deflated_sharpe"),
                     policies=("p1", "p2"), churn=(0.0, 1.0))
        grid = axes.enumerate_grid()
        self.assertEqual(len(grid), 3 * 2 * 2 * 1 * 2)             # est x defl x pol x crit x churn
        self.assertEqual(len({g.key for g in grid}), len(grid))   # keys unique

    def test_pre_register_manifest(self) -> None:
        manifest = bo.pre_register_grid(_axes(), created_at=42)
        self.assertEqual(manifest["created_at"], 42)
        self.assertEqual(manifest["n_grid"], len(manifest["grid_keys"]))
        self.assertEqual(manifest["n_grid"], 2 * 1 * 4 * 1 * 1)

    def test_baseline_key_must_exist(self) -> None:
        ok = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                   deflators=(bo.NO_DEFLATION,), policies=("policy_full_rerank",))
        self.assertIn(bo._baseline_key(ok), {g.key for g in ok.enumerate_grid()})
        bad = _axes(estimators=("eb_shrinkage_skill",), policies=("policy_full_rerank",))
        with self.assertRaises(ValueError):
            bo._baseline_key(bad)                                  # no t_stat_baseline in the grid


class CriteriaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ss = pd.DataFrame({
            "wallet": ["a", "a", "b"], "entry_ts": [0, 100, 0], "ttr_ref": [3600, 999999, 7200],
            "price": [0.5, 0.5, 0.95],
        })

    def test_slice_filters_ttr_and_band(self) -> None:
        out = bo.slice_by_criteria(self.ss, Criteria(0, 24.0, 0.1, 0.9, 0.0, 0))
        self.assertEqual(len(out), 1)                             # row1 ttr too long, row2 band out

    def test_eligibility_min_trl(self) -> None:
        ss = pd.DataFrame({"wallet": ["a", "a", "b"], "entry_ts": [10, 20, 30]})
        self.assertEqual(bo.eligible_wallets(ss, Criteria(0, 72.0, 0, 1, 0.0, 2), as_of=100), {"a"})

    def test_eligibility_active_within(self) -> None:
        ss = pd.DataFrame({"wallet": ["a", "b"], "entry_ts": [10, 95]})
        keep = bo.eligible_wallets(ss, Criteria(20, 72.0, 0, 1, 0.0, 0), as_of=100)
        self.assertEqual(keep, {"b"})                            # a's last trade is > 20s before T


class DeflationGateTest(unittest.TestCase):
    def test_none_is_passthrough(self) -> None:
        scores = pd.DataFrame({"score": [0.9], "rank": [1]}, index=["a"])
        out = bo.apply_deflation_gate(scores, pd.DataFrame(), bo.NO_DEFLATION, n_trials=10)
        self.assertTrue(out.equals(scores))

    def test_unknown_deflator_raises(self) -> None:
        scores = pd.DataFrame({"score": [0.9], "rank": [1]}, index=["a"])
        with self.assertRaises(ValueError):
            bo.apply_deflation_gate(scores, pd.DataFrame({"wallet": [], "payoff": [], "_eff": []}),
                                    "bogus", n_trials=10)

    def test_deflated_sharpe_drops_weak(self) -> None:
        # a: strong consistent edge (high Sharpe -> clears the expected-max-null bar); b/c: ~50%
        # null wallets (Sharpe ~ 0). n_trials = candidate count (the per-wallet selection bar).
        rows = ([("a", 1.0)] * 57 + [("a", 0.0)] * 3
                + [("b", 1.0)] * 25 + [("b", 0.0)] * 25
                + [("c", 1.0)] * 24 + [("c", 0.0)] * 26)
        ss = pd.DataFrame(rows, columns=["wallet", "payoff"])
        ss["_eff"] = 0.51
        scores = pd.DataFrame({"score": [0.9, 0.5, 0.4], "rank": [1, 2, 3]}, index=["a", "b", "c"])
        out = bo.apply_deflation_gate(scores, ss, DeflatedSharpe.name, n_trials=3, threshold=0.5)
        self.assertIn("a", out.index)                            # strong wallet clears the bar
        self.assertNotIn("b", out.index)                         # null wallets dropped
        self.assertNotIn("c", out.index)


class ParseTest(unittest.TestCase):
    def test_parse_pnl_ndjson(self) -> None:
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "pnl_by_period.ndjson"
            p.write_text('{"wallet":"0xa","period_end":100,"realized_pnl":1.5,'
                         '"unrealized_pnl":0.0,"n_fills":3,"notional":30.0}\n\n')
            df = bo.parse_pnl_by_period(p)
            self.assertEqual(list(df.columns), bo._PNL_COLUMNS)
            self.assertEqual(df.loc[0, "realized_pnl"], 1.5)

    def test_parse_missing_file_is_empty(self) -> None:
        df = bo.parse_pnl_by_period(Path("/nonexistent/pnl.ndjson"))
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), bo._PNL_COLUMNS)

    def test_paper_fills_rows_coerce_and_lowercase(self) -> None:
        rows = [{"wallet": "0xABC", "live_realized_pnl": "12.5"},
                {"wallet": "0xDef", "live_realized_pnl": "-3.0"}]
        out = bo._parse_paper_fills_rows(rows, wallet_col="wallet", pnl_col="live_realized_pnl")
        self.assertEqual(list(out["wallet"]), ["0xabc", "0xdef"])
        self.assertEqual(list(out["realized_pnl"]), [12.5, -3.0])


class ScreenTest(unittest.TestCase):
    def test_keeps_best_and_always_baseline(self) -> None:
        ss, _ = _population(1)
        points = [3_000_000 + i * 1_000_000 for i in range(6)]
        survivors = bo.screen_estimators(
            ss, ["t_stat_baseline", "eb_shrinkage_skill", "gu_koenker_npmle", "proxy_clv"],
            as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000, k=5, keep=2)
        self.assertIn("t_stat_baseline", survivors)              # baseline always retained
        self.assertLessEqual(len(survivors), 3)                  # keep=2 (+ baseline if displaced)


class TrajectoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ss, self.skill = _population(2)
        self.points = [3_000_000 + i * 1_000_000 for i in range(8)]
        self.horizon = 1_000_000
        self.value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in self.skill.items()}

    def _gp(self, policy, churn):
        return bo.GridPoint("eb_shrinkage_skill", bo.NO_DEFLATION, policy,
                            Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), churn)

    def test_returns_series_shape_and_determinism(self) -> None:
        runner = _FakeRunner(self.value, self.points, self.horizon)
        kwargs = dict(as_of_points=self.points, train_secs=3_000_000, horizon_secs=self.horizon,
                      k=5, displacement_margin=5)
        a = bo.run_trajectory(self._gp("policy_full_rerank", 0.0), self.ss, runner, **kwargs)
        b = bo.run_trajectory(self._gp("policy_full_rerank", 0.0), self.ss, runner, **kwargs)
        self.assertEqual(list(a.returns.index), self.points)
        self.assertTrue(a.returns.equals(b.returns))             # bit-deterministic
        # A1 (#436): the final-step followed set + scores are captured for the deliverable.
        self.assertGreater(len(a.final_follow), 0)
        self.assertEqual(list(a.final_scores.columns), ["score", "rank"])
        self.assertTrue(set(a.final_follow["wallet"]).issubset(set(a.final_scores.index)))

    def test_churn_cost_reduces_return_when_set_changes(self) -> None:
        runner = _FakeRunner(self.value, self.points, self.horizon)
        kwargs = dict(as_of_points=self.points, train_secs=3_000_000, horizon_secs=self.horizon,
                      k=5, displacement_margin=5)
        free = bo.run_trajectory(self._gp("policy_full_rerank", 0.0), self.ss, runner, **kwargs).returns
        costed = bo.run_trajectory(self._gp("policy_full_rerank", 10.0),
                                   self.ss, runner, **kwargs).returns
        admissions = (free - costed) / 10.0                      # difference == churn_cost x admits
        self.assertTrue((admissions >= -1e-9).all())
        self.assertGreater(admissions.sum(), 0)                  # full-rerank churns -> paid a cost


class DecisionTest(unittest.TestCase):
    def _matrix(self, *, with_winner: bool):
        rng = np.random.default_rng(0)
        n = 200
        base = rng.normal(0.0, 1.0, n)
        cols = {"baseline": base}
        if with_winner:
            cols["challenger"] = base + 0.8                      # consistently beats each period
        for i in range(3):
            cols[f"null{i}"] = base + rng.normal(0.0, 1.0, n)    # zero-mean differential
        return pd.DataFrame(cols)

    def test_winner_when_beats_survives_and_cpr_go(self) -> None:
        d = bo.select_winner_or_nogo(self._matrix(with_winner=True), baseline_key="baseline",
                                     n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "WINNER")
        self.assertEqual(d["winner"], "challenger")
        self.assertTrue(d["uncertainty"]["available"])

    def test_nogo_on_cpr_no_go(self) -> None:
        d = bo.select_winner_or_nogo(self._matrix(with_winner=True), baseline_key="baseline",
                                     n_grid=20, cpr={"go": False})
        self.assertEqual(d["status"], "NO-GO")
        self.assertIn("CPR no-go", d["reason"])

    def test_nogo_when_no_challenger_beats(self) -> None:
        d = bo.select_winner_or_nogo(self._matrix(with_winner=False), baseline_key="baseline",
                                     n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "NO-GO")
        self.assertIsNone(d["winner"])


class UncertaintyTest(unittest.TestCase):
    def test_akm_mrsw_fcr_shapes(self) -> None:
        moments = pd.DataFrame({
            "mean": [2.0, 1.0, 0.9, -0.5], "se": [0.3, 0.3, 0.3, 0.3],
        }, index=["c0", "c1", "c2", "c3"])
        out = bo.winner_uncertainty(moments)
        self.assertTrue(out["available"])
        self.assertEqual(out["winner_config"], "c0")
        self.assertLess(out["akm"]["median_unbiased"], out["akm"]["naive_estimate"])
        self.assertIn("c0", out["mrsw_top1_cs"])
        self.assertEqual(set(out["fcr"]["config"]), {"c0", "c1", "c2"})  # positive-mean selected


class CPRPremiseTest(unittest.TestCase):
    def test_go_on_persistent_population(self) -> None:
        ss, _ = _population(3, good=10, bad=10, n_pos=80)
        res = bo.wallet_persistence_cpr(ss, split_at=4_500_000)
        self.assertTrue(res["go"])

    def test_insufficient_wallets(self) -> None:
        ss = pd.DataFrame({"wallet": ["a", "b"], "entry_ts": [0, 9_000_000],
                           "payoff": [1.0, 0.0], "_eff": [0.51, 0.51]})
        self.assertFalse(bo.wallet_persistence_cpr(ss, split_at=4_500_000)["go"])


class EndToEndTest(unittest.TestCase):
    def test_run_bakeoff_runs_and_is_deterministic(self) -> None:
        ss, skill = _population(4, good=8, bad=12, n_pos=70)
        points = [3_000_000 + i * 1_000_000 for i in range(12)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 1_000_000)
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=2, demoter_kwargs={"min_periods": 3},
                                  min_periods=2)  # short synthetic trajectory: opt below the B4 floor
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                     deflators=(bo.NO_DEFLATION,), policies=_POLICIES)
        a = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        b = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        self.assertEqual(a["manifest"]["n_grid"], len(a["return_matrix"].columns))
        self.assertIn(a["decision"]["status"], {"WINNER", "NO-GO"})
        self.assertTrue(a["return_matrix"].equals(b["return_matrix"]))
        self.assertEqual(a["decision"]["status"], b["decision"]["status"])


class GridCollisionTest(unittest.TestCase):
    """A8 (#436): a duplicate axis level (here, the same Criteria twice) yields colliding config
    keys that would silently merge two matrix columns — pre-registration must fail loudly."""

    def test_duplicate_criteria_level_raises(self) -> None:
        c = Criteria(0, 72.0, 0.0, 1.0, 0.0, 0)
        axes = _axes(criteria=(c, c))
        with self.assertRaises(ValueError):
            bo.pre_register_grid(axes)


class WeightedGateTest(unittest.TestCase):
    """A4 (#436): the deflation gate's Sharpe is uniqueness-weighted (agrees with the ranker)."""

    def _scores(self):
        return pd.DataFrame({"score": [0.9], "rank": [1]}, index=["w"])

    def test_gate_respects_uniqueness_weights(self) -> None:
        # net edge = (payoff - _eff)/_eff = [+1, +1, -1, -1, -1] at _eff=0.5: loss-tilted ->
        # negative Sharpe under uniform weights, positive once the losers are down-weighted.
        in_sample = pd.DataFrame({"wallet": ["w"] * 5, "payoff": [1.0, 1.0, 0.0, 0.0, 0.0],
                                  "_eff": [0.5] * 5})
        dropped = bo.apply_deflation_gate(self._scores(), in_sample, DeflatedSharpe.name,
                                          n_trials=5, weights=np.ones(5), threshold=0.5)
        kept = bo.apply_deflation_gate(self._scores(), in_sample, DeflatedSharpe.name, n_trials=5,
                                       weights=np.array([2.0, 2.0, 0.5, 0.5, 0.5]), threshold=0.5)
        self.assertNotIn("w", dropped.index)    # uniform weights -> loss-tilted -> low Sharpe -> drop
        self.assertIn("w", kept.index)          # down-weighting the losers raises weighted Sharpe

    def test_zero_dispersion_positive_not_vetoed(self) -> None:
        # A10 (#436): constant positive net edge (sd=0) has an UNDEFINED (not low) Sharpe; the gate
        # must not silently veto an estimator's pick for one.
        in_sample = pd.DataFrame({"wallet": ["w"] * 3, "payoff": [1.0, 1.0, 1.0], "_eff": [0.5] * 3})
        out = bo.apply_deflation_gate(self._scores(), in_sample, DeflatedSharpe.name, n_trials=5,
                                      weights=np.ones(3), threshold=0.5)
        self.assertIn("w", out.index)


class PreScreenNTest(unittest.TestCase):
    """A2 (#436): the scalar Deflated-Sharpe bar takes the FULL pre-screen trial count; PBO/RW/SPA
    take the run-set columns. A larger pre-screen N is a strictly higher (lower-DSR) bar."""

    def _matrix(self):
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, 150)
        return pd.DataFrame({"baseline": base, "edge": base + 0.6,
                             "n0": base + rng.normal(0, 1, 150), "n1": base + rng.normal(0, 1, 150)})

    def test_dsr_bar_rises_with_pre_screen_n(self) -> None:
        m = self._matrix()
        small = bo.grid_deflate(m, benchmark="baseline", n_grid=4, n_trials_dsr=4)
        big = bo.grid_deflate(m, benchmark="baseline", n_grid=4, n_trials_dsr=400)
        self.assertLessEqual(float(big["deflated"]["dsr"].mean()),
                             float(small["deflated"]["dsr"].mean()) + 1e-9)
        # PBO/RW/SPA are unchanged by the DSR trial count (they read the matrix columns).
        self.assertEqual(float(small["pbo"]["pbo"].iloc[0]), float(big["pbo"]["pbo"].iloc[0]))

    def test_run_bakeoff_records_pre_screen_n(self) -> None:
        ss, skill = _population(8, good=6, bad=6, n_pos=60)
        points = [3_000_000 + i * 1_000_000 for i in range(10)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 1_000_000)
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=1, demoter_kwargs={"min_periods": 3},
                                  min_periods=2)  # short synthetic trajectory: opt below the B4 floor
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill", "gu_koenker_npmle"),
                     deflators=(bo.NO_DEFLATION,), policies=("policy_full_rerank",))
        res = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        # screen_keep=1 prunes >= 1 estimator, so the run grid is smaller than the pre-screen grid.
        self.assertGreaterEqual(res["decision"]["n_grid_full_pre_screen"], res["manifest"]["n_grid"])
        self.assertEqual(res["decision"]["n_grid_full_pre_screen"], len(axes.enumerate_grid()))


class CleanMatrixTest(unittest.TestCase):
    """A9 (#436): no-signal configs/periods are EXCLUDED, not scored a low-variance 0."""

    def _matrix(self):
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, 150)
        return pd.DataFrame({"baseline": base, "challenger": base + 0.8,
                             "n0": base + rng.normal(0, 1, 150)})

    def test_dead_config_dropped_not_scored_zero(self) -> None:
        m = self._matrix()
        m["dead"] = np.nan                       # a config that never produced a signal
        d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertIn("dead", d["dropped_configs"])
        self.assertNotIn("dead", d["leaderboard"].index)        # excluded, not a 0-variance arm
        self.assertEqual(d["winner"], "challenger")

    def test_baseline_no_signal_is_nogo_with_uniform_output(self) -> None:
        m = pd.DataFrame({"baseline": [np.nan] * 150, "challenger": [1.0] * 150})
        d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "NO-GO")
        self.assertIn("baseline", d["reason"])
        self.assertIn("leaderboard", d)                         # uniform shape for the output writer

    def test_nan_period_is_excluded_rectangular(self) -> None:
        m = self._matrix()
        m.loc[0, "n0"] = np.nan                   # n0 had no signal in period 0
        d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertEqual(d["dropped_periods"], 1)               # that period dropped for all configs
        self.assertEqual(d["winner"], "challenger")

    def test_run_trajectory_emits_nan_for_no_signal_period(self) -> None:
        # An as_of before any in-sample data exists -> no eligible set -> NaN (not a real $0).
        raw = pd.DataFrame(
            [("w1", "m1", 1, 5_000_000, 3600, 0.5, 10, 1.0, 5_005_000, 0.55),
             ("w1", "m2", 1, 5_100_000, 3600, 0.5, 10, 0.0, 5_105_000, 0.45),
             ("w2", "m3", 1, 5_000_000, 3600, 0.5, 10, 1.0, 5_005_000, 0.55),
             ("w2", "m4", 1, 5_100_000, 3600, 0.5, 10, 0.0, 5_105_000, 0.45)],
            columns=["wallet", "market_id", "outcome_id", "entry_ts", "ttr_secs", "price",
                     "contracts", "payoff", "resolved_at", "close_proxy"])
        ss = derive_columns(raw)
        runner = _FakeRunner({"w1": 1.0, "w2": -1.0}, [6_000_000], 1_000_000)
        gp = bo.GridPoint("t_stat_baseline", bo.NO_DEFLATION, "policy_full_rerank",
                          Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), 0.0)
        out = bo.run_trajectory(gp, ss, runner, as_of_points=[1_000_000, 6_000_000],
                                train_secs=3_000_000, horizon_secs=1_000_000, k=5,
                                displacement_margin=5)
        self.assertTrue(np.isnan(out.returns.iloc[0]))           # too early -> no signal -> NaN
        self.assertFalse(np.isnan(out.returns.iloc[1]))          # populated period -> a real number


class A1DeliverableTest(unittest.TestCase):
    """A1 (#436): the deliverable is the WINNING trajectory's frozen final-step followed set, not a
    cold re-application (which degenerates stateful policies)."""

    def test_deliverable_matches_decision_winner(self) -> None:
        ss, skill = _population(4, good=8, bad=12, n_pos=70)
        points = [3_000_000 + i * 1_000_000 for i in range(12)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 1_000_000)
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=2, demoter_kwargs={"min_periods": 3},
                                  min_periods=2)  # short synthetic trajectory: opt below the B4 floor
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"), policies=_POLICIES)
        res = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        deliverable = res["deliverable"]
        expected_key = res["decision"]["winner"] or res["decision"]["baseline_key"]
        self.assertEqual(deliverable["winner_key"], expected_key)
        self.assertIsNotNone(deliverable["grid_point"])          # threaded GridPoint, not re-parsed
        self.assertEqual(deliverable["grid_point"].key, expected_key)
        followed = set(deliverable["follow"]["wallet"])
        if followed:                                             # came from THAT config's scoring
            self.assertTrue(followed.issubset(set(deliverable["scores"].index)))

    def test_online_weighting_final_set_is_evolved_not_cold(self) -> None:
        from ranker.selectors import OnlineExpWeights
        ss, skill = _population(13, good=5, bad=5, n_pos=60)
        points = [2_000_000 + i * 900_000 for i in range(8)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 900_000)
        gp = bo.GridPoint("eb_shrinkage_skill", bo.NO_DEFLATION, "policy_online_weighting",
                          Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), 0.0)
        res = bo.run_trajectory(gp, ss, runner, as_of_points=points, train_secs=2_000_000,
                                horizon_secs=900_000, k=5, displacement_margin=5)
        # A cold re-apply of the online selector (empty state) reseeds the EWMA at the final scores;
        # the captured set is the EWMA-EVOLVED one — they must differ (proves A1 captures the run).
        cold, _ = OnlineExpWeights().select(res.final_scores, k=5, state=None)
        self.assertFalse(res.final_follow.reset_index(drop=True).equals(cold.reset_index(drop=True)))


class ChallengerIterationTest(unittest.TestCase):
    """A5 (#436): award the highest-cum-return RW-superior config, not the cum-argmax (which may be
    an uncertified high-variance config). The RW-superior set is controlled directly here (as the
    CLI tests control ``get_engine``) so the test exercises A5's ITERATION, not arch's bootstrap —
    whose shared FWER null is too sensitive to a lucky-noise config to construct deterministically."""

    def test_uncertified_cum_argmax_is_skipped_for_certified_config(self) -> None:
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, 60)
        # lucky has the LARGEST cumulative return; steady is the only RW-certified challenger.
        m = pd.DataFrame({"baseline": base, "lucky": base + 1.0, "steady": base + 0.5})
        self.assertGreater(float(m["lucky"].sum()), float(m["steady"].sum()))
        fake = {
            "moments": bo._config_moments(m),
            "deflated": pd.DataFrame({"dsr": [0.9, 0.9, 0.9]},
                                     index=["baseline", "lucky", "steady"]),
            "pbo": pd.DataFrame({"pbo": [0.1]}),
            "hansen_spa": pd.DataFrame({"spa_pvalue_consistent": [0.001]}),
            "romano_wolf": pd.DataFrame({"config": ["lucky", "steady"],
                                         "beats_benchmark": [False, True]}),
        }
        orig = bo.grid_deflate
        bo.grid_deflate = lambda *a, **k: fake
        try:
            d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        finally:
            bo.grid_deflate = orig
        self.assertEqual(d["status"], "WINNER")
        self.assertEqual(d["winner"], "steady")                 # the certified one, not the argmax
        self.assertEqual(d["uncertainty"]["winner_config"], "steady")


class BaselineEstimatorWinTest(unittest.TestCase):
    """A11 (#436): a winning config whose ESTIMATOR is the baseline can only have won on its
    policy/criteria/churn — label it so 'beats baseline' is not read as an estimator lift."""

    def test_baseline_estimator_win_is_labeled(self) -> None:
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, 200)
        baseline_key = "t_stat_baseline|none|policy_full_rerank|crit|churn0.0"
        winner_key = "t_stat_baseline|none|policy_knockout_backfill|crit|churn0.0"
        m = pd.DataFrame({baseline_key: base, winner_key: base + 0.8,
                          "n0": base + rng.normal(0, 1, 200)})
        d = bo.select_winner_or_nogo(m, baseline_key=baseline_key, n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "WINNER")
        self.assertEqual(d["winner"], winner_key)
        self.assertTrue(d["baseline_estimator_win"])
        self.assertIn("policy/criteria-only", d["reason"])


class DegeneratePBOTest(unittest.TestCase):
    """A7 (#436): a flat leaderboard (near-zero cross-config dispersion) cannot be certified — the
    winner gate fails SAFE rather than reading an undefined PBO as a pass."""

    def test_flat_leaderboard_is_nogo(self) -> None:
        flat = pd.DataFrame({"baseline": [1.0] * 200, "c1": [1.0] * 200, "c2": [1.0] * 200})
        d = bo.select_winner_or_nogo(flat, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "NO-GO")


class ThresholdsWiredTest(unittest.TestCase):
    """A6 (#436): the glossary thresholds are wired (were hardcoded); grid-DSR is advisory."""

    def test_constants_match_glossary(self) -> None:
        self.assertEqual(bo.RANKER_PBO_MAX, 0.5)
        self.assertEqual(bo.RANKER_DSR_MIN, 0.5)
        self.assertEqual(bo.RANKER_GRID_DSR_MIN, 0.95)
        self.assertEqual(bo.RANKER_MIN_PERIODS, 24)             # B4 (#436)

    def test_winner_surfaces_advisory_grid_dsr(self) -> None:
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, 200)
        m = pd.DataFrame({"baseline": base, "challenger": base + 0.8,
                          "n0": base + rng.normal(0, 1, 200)})
        d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "WINNER")
        self.assertIn("winner_grid_dsr", d)                     # surfaced, not silently ignored
        self.assertIn("grid_dsr_advisory_low", d)


class CliEngineTest(unittest.TestCase):
    """Regression guard (issue #421): ``main`` must pass the engine MODE to
    ``ranker_duck.get_engine`` (default ``duck``), never the ``--cache`` path — the original code
    fed ``--cache`` into the ``force`` argument, which fell back to SQLite and crashed
    ``materialize(None)``."""

    def test_parser_defaults(self) -> None:
        ns = bo._build_arg_parser().parse_args(
            ["--out-dir", "o", "--pe-backtest", "b", "--cache", "data/wallet_cache.db",
             "--start-unix", "0"])
        self.assertEqual(ns.engine, "duck")          # bake-off requires the Parquet engine
        self.assertIsNone(ns.parquet_dir)            # -> ranker_duck default (data/parquet)
        self.assertEqual(ns.cache, "data/wallet_cache.db")
        self.assertEqual(ns.steps, 24)               # B4 (#436): default >= ranker_min_periods

    def test_open_engine_passes_mode_not_cache_path(self) -> None:
        import ranker_duck
        ns = bo._build_arg_parser().parse_args(
            ["--out-dir", "o", "--pe-backtest", "b", "--cache", "data/wallet_cache.db",
             "--start-unix", "0"])
        captured: dict = {}
        orig = ranker_duck.get_engine
        ranker_duck.get_engine = lambda *a, **k: captured.update(args=a) or "CON"
        try:
            con = bo._open_engine(ns)
        finally:
            ranker_duck.get_engine = orig
        self.assertEqual(con, "CON")
        self.assertEqual(captured["args"][0], "duck")          # arg 0 = engine MODE, not a path
        self.assertIsNone(captured["args"][1])                 # arg 1 = parquet_dir (guards swap)
        self.assertNotIn("data/wallet_cache.db", captured["args"])

    def test_open_engine_raises_loudly_when_engine_yields_none(self) -> None:
        # --engine sqlite/auto can return None; the bake-off has no SQLite path, so fail loudly
        # instead of crashing later in materialize(None).
        import ranker_duck
        ns = bo._build_arg_parser().parse_args(
            ["--out-dir", "o", "--pe-backtest", "b", "--cache", "c",
             "--engine", "sqlite", "--start-unix", "0"])
        orig = ranker_duck.get_engine
        ranker_duck.get_engine = lambda *a, **k: None
        try:
            with self.assertRaises(SystemExit):
                bo._open_engine(ns)
        finally:
            ranker_duck.get_engine = orig


class NonOverlapWindowsTest(unittest.TestCase):
    """B3 (#436): BakeoffParams rejects overlapping forward windows (cutoffs spaced < horizon_secs),
    which would double-count trajectory P&L and over-count B2 proven periods."""

    def test_overlapping_cutoffs_raise(self) -> None:
        with self.assertRaises(ValueError):
            bo.BakeoffParams(as_of_points=[0, 50, 100], train_secs=300, horizon_secs=80)  # gap 50 < 80

    def test_adjacent_cutoffs_ok(self) -> None:
        # gap == horizon is the boundary: forward windows are back-to-back, non-overlapping.
        p = bo.BakeoffParams(as_of_points=[0, 80, 160], train_secs=300, horizon_secs=80)
        self.assertEqual(p.horizon_secs, 80)

    def test_operator_step_day_defaults_satisfy_invariant(self) -> None:
        # The operator defaults (--step-days 30 >= --horizon-days 30) construct without raising.
        day = 86_400
        pts = [i * 30 * day for i in range(bo.RANKER_MIN_PERIODS)]
        bo.BakeoffParams(as_of_points=pts, train_secs=180 * day, horizon_secs=30 * day)


class MinPeriodsFloorTest(unittest.TestCase):
    """B4 (#436): a verdict needs >= ranker_min_periods walk-forward cutoffs, else an EXPLICIT
    insufficient-periods NO-GO (not a silently-degenerate always-NO-GO from the bootstrap gates)."""

    def _matrix(self, n_periods: int):
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, n_periods)
        return pd.DataFrame({"baseline": base, "challenger": base + 0.8,
                             "n0": base + rng.normal(0.0, 1.0, n_periods)})

    def test_too_few_periods_is_explicit_nogo(self) -> None:
        # default min_periods == RANKER_MIN_PERIODS (24); 12 < 24 -> short-circuit before the gates.
        d = bo.select_winner_or_nogo(self._matrix(12), baseline_key="baseline", n_grid=20,
                                     cpr={"go": True})
        self.assertEqual(d["status"], "NO-GO")
        self.assertIn("insufficient periods", d["reason"])
        self.assertIn("ranker_min_periods", d["reason"])
        self.assertIsNone(d["winner"])
        self.assertIn("leaderboard", d)                          # uniform shape for the output writer
        self.assertEqual(d["n_grid"], 20)

    def test_enough_periods_clears_the_floor(self) -> None:
        # Exactly RANKER_MIN_PERIODS periods -> the floor does NOT trigger; the verdict comes from the
        # normal PBO/RW/SPA machinery (WINNER or a substantive NO-GO, not "insufficient periods").
        d = bo.select_winner_or_nogo(self._matrix(bo.RANKER_MIN_PERIODS), baseline_key="baseline",
                                     n_grid=20, cpr={"go": True})
        self.assertNotIn("insufficient periods", d.get("reason", ""))
        self.assertIn(d["status"], {"WINNER", "NO-GO"})

    def test_floor_is_overridable_for_short_synthetic_runs(self) -> None:
        # A low override lets a short synthetic trajectory reach the verdict (as run_bakeoff tests do).
        d = bo.select_winner_or_nogo(self._matrix(12), baseline_key="baseline", n_grid=20,
                                     cpr={"go": True}, min_periods=2)
        self.assertNotIn("insufficient periods", d.get("reason", ""))


class PhaseDConstantsTest(unittest.TestCase):
    """D1/D2/D3 (#436): the calibrated knobs match the glossary, and the swept criteria grid pins the
    operator-default level FIRST so criteria[0] stays the canonical 8a-screen / baseline level."""

    def test_constants_match_glossary(self) -> None:
        self.assertEqual(bo.RANKER_CHURN_COST_USD, 0.75)        # ranker_churn_cost_usd (D3)
        self.assertEqual(bo.RANKER_BAKEOFF_MAX_BACKTESTS, 30_000)  # ranker_bakeoff_max_backtests (D2)

    def test_default_grid_is_12_levels_canonical_first(self) -> None:
        grid = bo.build_criteria_grid()
        self.assertEqual(len(grid), 12)                         # 3 TTR × 2 bands × 2 min_trl
        self.assertEqual(len(set(grid)), 12)                    # all distinct -> no A8 key collision
        self.assertEqual(grid[0], bo.OPERATOR_DEFAULT_CRITERIA)
        self.assertEqual((grid[0].ttr_hours, grid[0].price_min, grid[0].price_max, grid[0].min_trl),
                         (72.0, 0.15, 0.85, 0))                 # 72h / wide band / no gate = canonical

    def test_custom_levels_first_is_canonical(self) -> None:
        grid = bo.build_criteria_grid(ttr_hours_levels=(24.0,), price_bands=((0.3, 0.7),),
                                      min_trl_levels=(5,))
        self.assertEqual(len(grid), 1)
        self.assertEqual((grid[0].ttr_hours, grid[0].price_min, grid[0].min_trl), (24.0, 0.3, 5))

    def test_half_life_and_recency_held_constant_not_swept(self) -> None:
        # half_life is carried-but-inert in v1, so the grid never varies it (sweeping it would only
        # duplicate columns); active_within is likewise held at the operator default.
        grid = bo.build_criteria_grid()
        self.assertEqual({c.half_life_days for c in grid}, {0.0})
        self.assertEqual({c.active_within_secs for c in grid}, {0})


class CriteriaSweepCliTest(unittest.TestCase):
    """D1 (#436): the criteria-sweep CLI flags parse into the swept grid; bands are lo:hi pairs."""

    def _ns(self, *extra):
        return bo._build_arg_parser().parse_args(
            ["--out-dir", "o", "--pe-backtest", "b", "--cache", "c", "--start-unix", "0", *extra])

    def test_default_sweep_flags(self) -> None:
        ns = self._ns()
        self.assertEqual(ns.ttr_hours, [72.0, 24.0, 48.0])
        self.assertEqual(ns.bands, [(0.15, 0.85), (0.30, 0.70)])
        self.assertEqual(ns.min_trl, [0, 20])
        self.assertEqual(ns.churn_cost, 0.75)

    def test_band_override_builds_grid(self) -> None:
        ns = self._ns("--ttr-hours", "72", "--bands", "0.2:0.8", "--min-trl", "0",
                      "--churn-cost", "0.5")
        grid = bo.build_criteria_grid(ttr_hours_levels=tuple(ns.ttr_hours),
                                      price_bands=tuple(ns.bands), min_trl_levels=tuple(ns.min_trl))
        self.assertEqual(len(grid), 1)
        self.assertEqual((grid[0].ttr_hours, grid[0].price_min, grid[0].price_max), (72.0, 0.2, 0.8))
        self.assertEqual(ns.churn_cost, 0.5)

    def test_malformed_band_rejected(self) -> None:
        with self.assertRaises(SystemExit):                    # argparse wraps ArgumentTypeError
            self._ns("--bands", "0.20")                        # missing ':hi'


class PluralCriteriaEndToEndTest(unittest.TestCase):
    """D1 (#436): the bake-off runs end-to-end over a PLURAL criteria axis (the singleton was the
    Phase-A shape), keys stay distinct (A8), and the full pre-screen N_GRID counts every criteria
    level for the DSR bar (A2)."""

    def test_plural_criteria_sweep_runs_and_counts_full_n(self) -> None:
        ss, skill = _population(8, good=6, bad=6, n_pos=60)
        points = [3_000_000 + i * 1_000_000 for i in range(10)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 1_000_000)
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=2, demoter_kwargs={"min_periods": 3}, min_periods=2)
        criteria = bo.build_criteria_grid(ttr_hours_levels=(72.0, 48.0),
                                          price_bands=((0.0, 1.0),), min_trl_levels=(0,))
        self.assertEqual(len(criteria), 2)
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                     deflators=(bo.NO_DEFLATION,), policies=("policy_full_rerank",),
                     criteria=criteria, churn=(0.0, 1.0))
        manifest = bo.pre_register_grid(axes)                  # A8 holds across the plural criteria axis
        self.assertEqual(len(manifest["grid_keys"]), len(set(manifest["grid_keys"])))
        res = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        self.assertEqual(res["decision"]["n_grid_full_pre_screen"], 2 * 1 * 1 * 2 * 2)  # counts all crit
        self.assertIn(res["decision"]["status"], {"WINNER", "NO-GO"})


class MemoRunnerTest(unittest.TestCase):
    """D2 (#436): the in-run memo dedups identical followed sets (order-independent, per flat_usd),
    so the churn axis and repeated sets cost one backtest each within a run."""

    def _inner(self):
        return _FakeRunner({"w1": 1.0, "w2": -1.0, "w3": 0.5}, [1000], 1000)

    def test_dedups_identical_wallet_sets(self) -> None:
        memo = bo.MemoizingBacktestRunner(self._inner())
        a = memo.run(["w1", "w2"], flat_usd=25.0)
        b = memo.run(["w1", "w2"], flat_usd=25.0)
        self.assertEqual((memo.lookups, memo.calls), (2, 1))    # inner ran ONCE
        self.assertIs(a, b)                                     # same cached frame object

    def test_key_is_order_independent(self) -> None:
        memo = bo.MemoizingBacktestRunner(self._inner())
        memo.run(["w1", "w2"], flat_usd=25.0)
        memo.run(["w2", "w1"], flat_usd=25.0)                  # same SET, different order
        self.assertEqual(memo.calls, 1)

    def test_distinct_sets_and_flat_usd_are_separate_keys(self) -> None:
        memo = bo.MemoizingBacktestRunner(self._inner())
        memo.run(["w1"], flat_usd=25.0)
        memo.run(["w1", "w2"], flat_usd=25.0)                  # different set
        memo.run(["w1"], flat_usd=50.0)                        # different flat_usd
        self.assertEqual(memo.calls, 3)

    def test_run_bakeoff_memo_collapses_churn_axis(self) -> None:
        # the two churn-cost levels produce IDENTICAL followed-set sequences (churn is a post-hoc
        # subtraction), so the memo runs each backtest once across both -> distinct == the count from
        # a single-churn run, and strictly fewer than the per-(config, step) request total.
        ss, skill = _population(8, good=6, bad=6, n_pos=60)
        points = [3_000_000 + i * 1_000_000 for i in range(10)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 1_000_000)
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=1, demoter_kwargs={"min_periods": 3}, min_periods=2)
        two = bo.run_bakeoff(ss, runner,
                             _axes(estimators=("t_stat_baseline",), deflators=(bo.NO_DEFLATION,),
                                   policies=("policy_full_rerank",), churn=(0.0, 1.0)),
                             params, created_at=1)["backtest_calls"]
        one = bo.run_bakeoff(ss, runner,
                             _axes(estimators=("t_stat_baseline",), deflators=(bo.NO_DEFLATION,),
                                   policies=("policy_full_rerank",), churn=(0.0,)),
                             params, created_at=1)["backtest_calls"]
        self.assertLess(two["distinct"], two["total"])         # memo had hits
        self.assertEqual(two["distinct"], one["distinct"])     # churn axis is free


class ComputeCeilingTest(unittest.TestCase):
    """D2 (#436): n_grid_full × steps may not exceed ranker_bakeoff_max_backtests — the search
    surface is a committed number, enforced before any expensive work."""

    def test_oversize_grid_raises(self) -> None:
        ss, skill = _population(8, good=4, bad=4, n_pos=40)
        points = [3_000_000 + i * 1_000_000 for i in range(6)]
        runner = _FakeRunner({w: 1.0 for w in skill}, points, 1_000_000)
        # n_grid_full = 2 est × 1 defl × 1 pol × 1 crit × 1 churn = 2; × 6 steps = 12 > max_backtests=5.
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=2, min_periods=2, max_backtests=5)
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                     deflators=(bo.NO_DEFLATION,), policies=("policy_full_rerank",))
        with self.assertRaises(ValueError) as cm:
            bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        self.assertIn("ranker_bakeoff_max_backtests", str(cm.exception))

    def test_default_operator_grid_within_ceiling(self) -> None:
        # drift guard: the operator-default full sweep stays under the committed ceiling.
        full = len(bo.ESTIMATOR_REGISTRY) * 2 * 4 * len(bo.build_criteria_grid()) * 2
        self.assertEqual(full, 5 * 2 * 4 * 12 * 2)              # 960 configs
        self.assertLessEqual(full * bo.RANKER_MIN_PERIODS, bo.RANKER_BAKEOFF_MAX_BACKTESTS)  # 23040<=30000


class SdFloorMomentsTest(unittest.TestCase):
    """D2 (#436): _config_moments treats sub-_SD_FLOOR dispersion as UNDEFINED (se=inf, sr=0), not a
    spurious huge Sharpe — folds the post-#439 float-fragility cleanup onto the deflation surface."""

    def test_near_constant_config_no_spurious_sharpe(self) -> None:
        # sd ≈ 5e-13 (a sub-1e-9 float-noise perturbation): the old `sd > 0` emitted a ~6e11 SR and a
        # tiny FINITE SE into the deflators / AKM CI; the floor drops it.
        mom = bo._config_moments(pd.DataFrame({"c": [0.3, 0.3, 0.3 + 1e-12, 0.3]}))
        self.assertEqual(mom.loc["c", "sr"], 0.0)
        self.assertFalse(np.isfinite(mom.loc["c", "se"]))

    def test_real_dispersion_still_finite(self) -> None:
        mom = bo._config_moments(pd.DataFrame({"c": [0.1, 0.3, 0.5, 0.2]}))  # sd ≈ 0.17 >> floor
        self.assertTrue(np.isfinite(mom.loc["c", "se"]))
        self.assertNotEqual(mom.loc["c", "sr"], 0.0)


class L1ChurnTest(unittest.TestCase):
    """D3 (#436): churn = positive-part L1 weight movement. It reduces to the admission count for the
    hard-set policies (weight 1.0) and additionally charges an OnlineWeighting weight ramp-up that the
    old admission count ignored."""

    @staticmethod
    def _fs(weights: dict) -> pd.DataFrame:
        return pd.DataFrame({"wallet": list(weights), "weight": list(weights.values())})

    def test_hard_set_reduces_to_admission_count(self) -> None:
        prev = self._fs({"w1": 1.0, "w2": 1.0})
        follow = self._fs({"w2": 1.0, "w3": 1.0, "w4": 1.0})   # +w3 +w4; w1 evicted; w2 kept
        self.assertEqual(bo._churn(prev, follow), 2.0)         # == # admissions (w3, w4)

    def test_eviction_alone_is_not_charged(self) -> None:
        self.assertEqual(bo._churn(self._fs({"w1": 1.0, "w2": 1.0}), self._fs({"w1": 1.0})), 0.0)

    def test_first_step_charges_full_mass(self) -> None:
        prev = pd.DataFrame({"wallet": [], "weight": []})
        self.assertEqual(bo._churn(prev, self._fs({"w1": 1.0, "w2": 1.0})), 2.0)

    def test_online_weight_rampup_is_charged(self) -> None:
        # same membership, weights shift: w1 0.3->0.8 (+0.5), w2 0.7->0.2 (decrease, clamped to 0).
        # the old admission count was 0 here; the L1 positive part charges the +0.5 ramp.
        self.assertAlmostEqual(
            bo._churn(self._fs({"w1": 0.3, "w2": 0.7}), self._fs({"w1": 0.8, "w2": 0.2})), 0.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
