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
                      k=5, n_grid=4, displacement_margin=5)
        a = bo.run_trajectory(self._gp("policy_full_rerank", 0.0), self.ss, runner, **kwargs)
        b = bo.run_trajectory(self._gp("policy_full_rerank", 0.0), self.ss, runner, **kwargs)
        self.assertEqual(list(a.index), self.points)
        self.assertTrue(a.equals(b))                             # bit-deterministic

    def test_churn_cost_reduces_return_when_set_changes(self) -> None:
        runner = _FakeRunner(self.value, self.points, self.horizon)
        kwargs = dict(as_of_points=self.points, train_secs=3_000_000, horizon_secs=self.horizon,
                      k=5, n_grid=4, displacement_margin=5)
        free = bo.run_trajectory(self._gp("policy_full_rerank", 0.0), self.ss, runner, **kwargs)
        costed = bo.run_trajectory(self._gp("policy_full_rerank", 10.0), self.ss, runner, **kwargs)
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
                                  k=5, screen_keep=2, demoter_kwargs={"min_periods": 3})
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                     deflators=(bo.NO_DEFLATION,), policies=_POLICIES)
        a = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        b = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        self.assertEqual(a["manifest"]["n_grid"], len(a["return_matrix"].columns))
        self.assertIn(a["decision"]["status"], {"WINNER", "NO-GO"})
        self.assertTrue(a["return_matrix"].equals(b["return_matrix"]))
        self.assertEqual(a["decision"]["status"], b["decision"]["status"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
