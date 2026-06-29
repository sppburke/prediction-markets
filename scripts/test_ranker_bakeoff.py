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
import os
import sys
import unittest
from pathlib import Path

# Pin BLAS to one thread BEFORE numpy import (mirrors bakeoff.py) — ProcessExecutorDeterminismTest
# forks from THIS process, and this test file imports numpy before `bakeoff`, so the pin must be set
# here too or the fork pool would run without the deadlock-safety mitigation.
for _blas_var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_blas_var, "1")

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


def _adversarial_short_track(seed: int):
    """F2 (#436): strong-edge skilled long-track wallets + null long-track + a swarm of short-track
    noisy wallets + a lucky short-track fluke (n=4, 3 wins) + a deterministic all-win streak (n=3,
    3 wins → zero dispersion). Returns (frame, skilled, short_track). The two short-track defenses
    are the C1 EB shrinkage (drops the zero-dispersion streak, rewards precision) and the D1 MinTRL
    eligibility gate (excludes every short-track wallet outright)."""
    rng = np.random.default_rng(seed)
    cols = ["wallet", "market_id", "outcome_id", "entry_ts", "ttr_secs",
            "price", "contracts", "payoff", "resolved_at", "close_proxy"]
    rows = []

    def emit(w, n, win_p):
        for _ in range(n):
            entry = int(rng.integers(0, 9_000_000))
            payoff = 1.0 if rng.random() < win_p else 0.0
            rows.append((w, f"{w}_{entry}", 1, entry, 3600, 0.50, 10, payoff, entry + 5000, 0.55))

    def emit_fixed(w, payoffs):
        for k, p in enumerate(payoffs):
            rows.append((w, f"{w}_{k}", 1, 8_000_000 + k, 3600, 0.50, 10, p, 8_005_000 + k, 0.55))

    skilled = [f"skill{i}" for i in range(4)]
    for w in skilled:
        emit(w, 80, 0.80)                                # long-track, STRONG real edge (precise)
    for w in (f"null{i}" for i in range(4)):
        emit(w, 50, 0.50)                                # long-track, zero edge (realistic prior)
    swarm = [f"noise{i}" for i in range(6)]
    for w in swarm:
        emit(w, 3, 0.50)                                 # short-track noise (n=3, random)
    emit_fixed("fluke", [1.0, 1.0, 1.0, 0.0])            # short-track LUCKY fluke: 4 positions, 3 wins
    emit_fixed("streak", [1.0, 1.0, 1.0])                # short-track ALL-WIN streak: zero dispersion
    raw = pd.DataFrame(rows, columns=cols)
    return derive_columns(raw), skilled, swarm + ["fluke", "streak"]


class _FakeRunner:
    """Deterministic ``BacktestRunner``: pays each followed wallet ``value[w] * flat_usd`` at a
    period_end inside every step window (the trajectory windows it per step)."""

    def __init__(self, value: dict, points: list, horizon: int):
        self.value = value
        self.points = points
        self.horizon = horizon

    def run(self, wallets, *, flat_usd, window=None):
        # Realized-only fake (no forward MTM): is_horizon_mtm=False so the objective is realized-only
        # and live_pnl keeps every row, exactly as pre-E. `window` is accepted and ignored.
        rows = [{"wallet": w, "period_end": t + self.horizon // 2,
                 "realized_pnl": self.value.get(w, 0.0) * flat_usd, "unrealized_pnl": 0.0,
                 "n_fills": 2, "notional": flat_usd * 2,
                 "is_horizon_mtm": False, "open_at_horizon": 0, "marked_at_horizon": 0,
                 "positions_in_window": 0, "resolution_lags_secs": []}
                for w in wallets for t in self.points]
        return pd.DataFrame(rows, columns=bo._PNL_COLUMNS)


class _MtmRunner(_FakeRunner):
    """``_FakeRunner`` plus ONE forward-MTM horizon row per wallet (issue #436 Phase E): the SAME
    realized day rows, plus an ``is_horizon_mtm=True`` row at the window horizon carrying the
    unrealized flow + coverage counts. So a plain vs MTM run feed the demoter/live_pnl IDENTICAL
    realized rows (the MTM rows are filtered out), but the MTM run's objective gains the flow."""

    def __init__(self, value, points, horizon, *, unrealized, open_n, marked_n,
                 positions_n=None, lags=None):
        super().__init__(value, points, horizon)
        self.unrealized = unrealized
        self.open_n = open_n
        self.marked_n = marked_n
        # F3a: positions_in_window denominator (defaults to open_n) + per-wallet resolution lags.
        self.positions_n = positions_n if positions_n is not None else open_n
        self.lags = lags or {}

    def run(self, wallets, *, flat_usd, window=None):
        df = super().run(wallets, flat_usd=flat_usd, window=window)
        if window is None:
            return df
        _as_of, horizon_end = window
        mtm = [{"wallet": w, "period_end": horizon_end, "realized_pnl": 0.0,
                "unrealized_pnl": self.unrealized.get(w, 0.0), "n_fills": 0, "notional": 0.0,
                "is_horizon_mtm": True, "open_at_horizon": self.open_n.get(w, 0),
                "marked_at_horizon": self.marked_n.get(w, 0),
                "positions_in_window": self.positions_n.get(w, 0),
                "resolution_lags_secs": list(self.lags.get(w, []))} for w in wallets]
        return pd.concat([df, pd.DataFrame(mtm, columns=bo._PNL_COLUMNS)], ignore_index=True)


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
            # A realized day row (is_horizon_mtm=false) + a forward-MTM horizon row
            # (is_horizon_mtm=true, the flow in unrealized_pnl + coverage counts) — issue #436 E.
            p.write_text('{"wallet":"0xa","period_end":100,"realized_pnl":1.5,'
                         '"unrealized_pnl":0.0,"n_fills":3,"notional":30.0,'
                         '"is_horizon_mtm":false,"open_at_horizon":0,"marked_at_horizon":0}\n'
                         '{"wallet":"0xa","period_end":200,"realized_pnl":0.0,'
                         '"unrealized_pnl":2.25,"n_fills":0,"notional":0.0,'
                         '"is_horizon_mtm":true,"open_at_horizon":2,"marked_at_horizon":1}\n\n')
            df = bo.parse_pnl_by_period(p)
            self.assertEqual(list(df.columns), bo._PNL_COLUMNS)
            self.assertEqual(df.loc[0, "realized_pnl"], 1.5)
            self.assertEqual(df.loc[1, "unrealized_pnl"], 2.25)
            self.assertTrue(bool(df.loc[1, "is_horizon_mtm"]))
            self.assertEqual(int(df.loc[1, "open_at_horizon"]), 2)

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

    def test_serial_and_process_screen_identical(self) -> None:
        # The process-parallel screen (executor='process', a fork pool over the as_of axis) is
        # BIT-IDENTICAL to the serial restructured screen: each as_of's per-estimator fwd P&L is a
        # deterministic pure function and the scoreboard is assembled in fixed estimator/as_of order,
        # so the survivor ranking is independent of how the as_of are partitioned across workers. (Real
        # fork ProcessPoolExecutor on fake data — the screen is pure, no pe-backtest.)
        ss, _ = _population(2)
        points = [3_000_000 + i * 1_000_000 for i in range(10)]
        ests = ["t_stat_baseline", "eb_shrinkage_skill", "gu_koenker_npmle", "proxy_clv"]
        kw = dict(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000, k=5, keep=2)
        serial = bo.screen_estimators(ss, ests, **kw)
        proc = bo.screen_estimators(ss, ests, max_workers=4, executor="process", **kw)
        self.assertEqual(serial, proc)                            # identical survivor ranking


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

    def test_score_cache_is_bit_identical(self) -> None:
        # The estimator-score memo (key = (estimator, criteria, as_of)) is reused across configs that
        # share that triple but differ in policy/deflator/churn. Two such configs run through a SHARED
        # cache must be BIT-IDENTICAL to the no-cache baseline — the `.copy()` in `_score_cached`
        # prevents the second config from seeing any in-place mutation of the first's cached frame.
        runner = _FakeRunner(self.value, self.points, self.horizon)
        kwargs = dict(as_of_points=self.points, train_secs=3_000_000, horizon_secs=self.horizon,
                      k=5, displacement_margin=5)
        gp_a = self._gp("policy_full_rerank", 0.0)
        gp_b = self._gp("policy_full_rerank", 10.0)              # same (estimator, criteria); diff churn
        base_a = bo.run_trajectory(gp_a, self.ss, runner, score_cache=None, **kwargs)
        base_b = bo.run_trajectory(gp_b, self.ss, runner, score_cache=None, **kwargs)
        cache: dict = {}
        ca = bo.run_trajectory(gp_a, self.ss, runner, score_cache=cache, **kwargs)
        cb = bo.run_trajectory(gp_b, self.ss, runner, score_cache=cache, **kwargs)  # reuses gp_a's scores
        self.assertTrue(ca.returns.equals(base_a.returns))
        self.assertTrue(cb.returns.equals(base_b.returns))      # cross-config reuse is bit-identical
        self.assertTrue(ca.final_scores.equals(base_a.final_scores))
        self.assertEqual(len(cache), len(self.points))          # one cached score per as_of, deduped


class MtmObjectiveTest(unittest.TestCase):
    """E2b/E3 (#436): the CLOB forward-MTM flow enters the objective, the demoter/live_pnl stay
    realized-only, and per-period coverage is surfaced."""

    def setUp(self) -> None:
        self.ss, self.skill = _population(2)
        self.points = [3_000_000 + i * 1_000_000 for i in range(8)]
        self.horizon = 1_000_000
        self.value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in self.skill.items()}

    def _gp(self, policy, churn):
        return bo.GridPoint("eb_shrinkage_skill", bo.NO_DEFLATION, policy,
                            Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), churn)

    def test_weighted_window_sums_realized_and_unrealized(self) -> None:
        window = pd.DataFrame([
            {"wallet": "w1", "realized_pnl": 3.0, "unrealized_pnl": 0.0},
            {"wallet": "w1", "realized_pnl": 0.0, "unrealized_pnl": 2.0},   # the horizon MTM row
            {"wallet": "w2", "realized_pnl": 1.0, "unrealized_pnl": -0.5},
        ])
        follow = pd.DataFrame({"wallet": ["w1", "w2"], "weight": [1.0, 2.0]})
        # w1: (3+2)=5 x1 ; w2: (1-0.5)=0.5 x2 = 1 -> 6
        self.assertAlmostEqual(bo._weighted_window_pnl(window, follow), 6.0)

    def test_flow_enters_objective_demoter_realized_only_coverage_surfaced(self) -> None:
        kwargs = dict(as_of_points=self.points, train_secs=3_000_000, horizon_secs=self.horizon,
                      k=5, displacement_margin=5, demoter_kwargs={"min_periods": 2})
        plain = _FakeRunner(self.value, self.points, self.horizon)
        mtm = _MtmRunner(self.value, self.points, self.horizon,
                         unrealized={w: 0.5 for w in self.value},
                         open_n={w: 2 for w in self.value}, marked_n={w: 1 for w in self.value})
        a = bo.run_trajectory(self._gp("policy_knockout_backfill", 0.0), self.ss, plain, **kwargs)
        b = bo.run_trajectory(self._gp("policy_knockout_backfill", 0.0), self.ss, mtm, **kwargs)
        # Demoter is realized-only: the MTM rows never enter live_pnl (they carry realized=0 and the
        # demoter counts each row as a period), so the followed set is identical with/without them —
        # it would diverge if the rows polluted live_pnl.
        self.assertEqual(set(a.final_follow["wallet"]), set(b.final_follow["wallet"]))
        # The unrealized flow enters the objective: each ran period's return rises by the weighted flow.
        diff = b.returns.fillna(0.0) - a.returns.fillna(0.0)
        self.assertTrue((diff >= -1e-9).all())
        self.assertGreater(diff.sum(), 0.0)
        # E3: per-period coverage surfaced for the MTM run; the plain run emits no MTM rows.
        self.assertIn("open_at_horizon", b.coverage.columns)
        self.assertGreater(int(b.coverage["open_at_horizon"].sum()), 0)
        self.assertGreater(int(b.coverage["marked_at_horizon"].sum()), 0)
        self.assertEqual(int(a.coverage["open_at_horizon"].sum()), 0)


class ResolutionLagTest(unittest.TestCase):
    """F3a (#436): the open-at-horizon resolution-lag distribution + open fraction are aggregated
    from the MTM rows and surfaced in mtm_coverage — advisory diagnostics, never verdict inputs."""

    def setUp(self) -> None:
        self.ss, self.skill = _population(2)
        self.points = [3_000_000 + i * 1_000_000 for i in range(8)]
        self.horizon = 1_000_000
        self.value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in self.skill.items()}

    def _gp(self, policy, churn):
        return bo.GridPoint("eb_shrinkage_skill", bo.NO_DEFLATION, policy,
                            Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), churn)

    def test_lag_distribution_quantiles(self) -> None:
        d = bo._lag_distribution([1 * 86_400, 2 * 86_400, 3 * 86_400], censored=1)
        self.assertEqual(d["n"], 3)
        self.assertEqual(d["censored"], 1)
        self.assertAlmostEqual(d["censored_frac"], 0.25)            # 1 of (3 resolved + 1 censored)
        self.assertAlmostEqual(d["p50_days"], 2.0)
        self.assertAlmostEqual(d["max_days"], 3.0)

    def test_lag_distribution_all_censored(self) -> None:
        d = bo._lag_distribution([], censored=2)
        self.assertEqual(d["n"], 0)
        self.assertEqual(d["censored"], 2)
        self.assertEqual(d["censored_frac"], 1.0)
        self.assertIsNone(d["p50_days"])

    def test_lag_distribution_empty_is_none(self) -> None:
        d = bo._lag_distribution([], censored=0)
        self.assertEqual(d["n"], 0)
        self.assertIsNone(d["censored_frac"])
        self.assertIsNone(d["p50_days"])

    def test_trajectory_aggregates_lags_open_fraction_and_censored(self) -> None:
        kwargs = dict(as_of_points=self.points, train_secs=3_000_000, horizon_secs=self.horizon,
                      k=5, displacement_margin=5, demoter_kwargs={"min_periods": 2})
        # Each followed wallet: 2 positions open at the horizon (one resolves 3d after as_of, one
        # censored) + 1 closed-in-window position → open=2, positions_in_window=3 (fraction 2/3).
        mtm = _MtmRunner(self.value, self.points, self.horizon,
                         unrealized={w: 0.5 for w in self.value},
                         open_n={w: 2 for w in self.value}, marked_n={w: 1 for w in self.value},
                         positions_n={w: 3 for w in self.value},
                         lags={w: [3 * 86_400, -1] for w in self.value})
        res = bo.run_trajectory(self._gp("policy_knockout_backfill", 0.0), self.ss, mtm, **kwargs)
        # The denominator strictly exceeds the open count — it includes the closed-in-window positions.
        self.assertGreater(int(res.coverage["positions_in_window"].sum()),
                           int(res.coverage["open_at_horizon"].sum()))
        # Censored (never-resolved) open positions are tallied, not dropped.
        self.assertGreater(int(res.coverage["censored"].sum()), 0)
        # The resolved lags are exactly the 3-day lag (the -1 censored entries are excluded from them).
        flat = [x for lags in res.lags_by_period.values() for x in lags]
        self.assertTrue(len(flat) > 0 and all(x == 3 * 86_400 for x in flat))

    def test_run_bakeoff_surfaces_resolution_lag(self) -> None:
        ss, skill = _population(4, good=8, bad=12, n_pos=70)
        points = [3_000_000 + i * 1_000_000 for i in range(12)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        mtm = _MtmRunner(value, points, 1_000_000,
                         unrealized={w: 0.5 for w in value},
                         open_n={w: 2 for w in value}, marked_n={w: 1 for w in value},
                         positions_n={w: 4 for w in value},
                         lags={w: [2 * 86_400, 4 * 86_400, -1] for w in value})
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=2, demoter_kwargs={"min_periods": 3}, min_periods=2)
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                     deflators=(bo.NO_DEFLATION,), policies=_POLICIES)
        cov = bo.run_bakeoff(ss, mtm, axes, params, created_at=1)["mtm_coverage"]
        # Every config carries the open-fraction + resolution-lag block.
        for entry in cov["by_config_overall"].values():
            self.assertIn("open_at_horizon_frac", entry)
            self.assertIn("resolution_lag", entry)
        # The winner's per-period detail surfaces the lag distribution (resolved + censored present).
        self.assertTrue(cov["winner_by_period"])
        wp = cov["winner_by_period"][0]
        self.assertIsNotNone(wp["resolution_lag"]["p50_days"])      # 2d / 4d resolved lags present
        self.assertGreater(wp["resolution_lag"]["censored"], 0)     # the -1 entries are tallied
        self.assertIsNotNone(wp["open_at_horizon_frac"])


class MinTrlEbNoiseTest(unittest.TestCase):
    """F2 (#436): short-track-wallet noise is neutralized by TWO complementary defenses — the C1 EB
    shrinkage keeps genuinely-skilled long-track wallets at the top despite a swarm of short-track
    noise, and the D1 MinTRL sweep excludes short-track wallets outright when min_trl > 0."""

    def setUp(self) -> None:
        self.ss, self.skilled, self.short_track = _adversarial_short_track(7)

    def test_eb_shrinks_short_track_below_skilled(self) -> None:
        scores = bo.ESTIMATOR_REGISTRY["eb_shrinkage_skill"]().score(
            self.ss, as_of=9_000_000, weights=bo.uniqueness_weights(self.ss))
        # C1: every genuinely-skilled long-track wallet outranks every short-track wallet — EB
        # shrinks the low-precision short-track scores (incl. the lucky n=4 fluke) below the precise
        # high-edge long-track ones, so the short-track noise neither collapses nor tops the ranking.
        present_short = [w for w in self.short_track if w in scores.index]
        self.assertTrue(present_short)                               # the swarm/fluke were scored
        worst_skilled = scores.loc[self.skilled, "rank"].max()
        best_short = scores.loc[present_short, "rank"].min()
        self.assertLess(worst_skilled, best_short)

    def test_eb_drops_zero_dispersion_streak(self) -> None:
        scores = bo.ESTIMATOR_REGISTRY["eb_shrinkage_skill"]().score(
            self.ss, as_of=9_000_000, weights=bo.uniqueness_weights(self.ss))
        # A10/C1: a short-track all-win streak (constant net edge → zero dispersion) is DROPPED, not
        # ranked #1 with an undefined-but-huge t — the small-sample curse this harness exists to kill.
        self.assertNotIn("streak", scores.index)

    def test_mintrl_sweep_excludes_short_track(self) -> None:
        wide = Criteria(0, 72.0, 0.0, 1.0, 0.0, 0)        # min_trl = 0 (no gate)
        gated = Criteria(0, 72.0, 0.0, 1.0, 0.0, 20)      # min_trl = 20 (the D1 sweep's other level)
        elig0 = bo.eligible_wallets(self.ss, wide, as_of=9_000_000)
        elig20 = bo.eligible_wallets(self.ss, gated, as_of=9_000_000)
        # Every short-track wallet (n < 20) is present at min_trl=0 and gone at min_trl=20, while the
        # skilled long-track wallets survive both sweep levels.
        self.assertTrue(set(self.short_track).issubset(elig0))
        self.assertTrue(set(self.short_track).isdisjoint(elig20))
        self.assertTrue(set(self.skilled).issubset(elig20))


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
        # #445 defect 4: estimator pruning is DISABLED — the run grid EQUALS the full pre-screen grid
        # even with screen_keep=1 (the 8a screen is advisory-only and no longer shrinks the run-set).
        self.assertEqual(res["decision"]["n_grid_full_pre_screen"], res["manifest"]["n_grid"])
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

    def test_window_is_part_of_the_key(self) -> None:
        # E (#436): the emitted unrealized_pnl flow depends on the MTM window (as_of, horizon), so
        # the same set at different windows must NOT share a cached frame; the churn-axis 2× win
        # (same set, same window) still holds.
        memo = bo.MemoizingBacktestRunner(self._inner())
        memo.run(["w1", "w2"], flat_usd=25.0, window=(0, 100))
        memo.run(["w1", "w2"], flat_usd=25.0, window=(0, 200))  # same SET, different window
        self.assertEqual(memo.calls, 2)
        memo.run(["w1", "w2"], flat_usd=25.0, window=(0, 100))  # repeat -> cached
        self.assertEqual((memo.lookups, memo.calls), (3, 2))

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


# ───────────────────────── issue #445: bake-off correctness remediation ─────────────────────────
class NoSignalStateResetTest(unittest.TestCase):
    """#445 defect 1: a no-signal period means the config HELD NOTHING. ``run_trajectory`` must reset
    ``prev`` to an empty set and rebuild the policy (clearing online/incumbency carry) so a later
    re-entry pays FULL churn; and if the FINAL cutoff is no-signal the deliverable
    (``final_follow``/``final_scores``) is empty — NOT a fall-back to the most recent non-empty set."""

    def _raw(self, rows):
        return derive_columns(pd.DataFrame(
            rows, columns=["wallet", "market_id", "outcome_id", "entry_ts", "ttr_secs", "price",
                           "contracts", "payoff", "resolved_at", "close_proxy"]))

    def _two_cluster_ss(self):
        # Cluster A (entry ~1.5M, resolved 1.55M) and cluster C (entry ~5.5M, resolved 5.55M) for
        # each wallet, with an EMPTY [3M,4M) window between them -> a no-signal middle cutoff.
        rows = []
        for wallet, payoffs in (("w1", [1.0, 1.0, 1.0, 0.0]), ("w2", [1.0, 0.0, 0.0, 0.0])):
            for j, p in enumerate(payoffs):
                rows.append((wallet, f"{wallet}_a{j}", 1, 1_500_000 + j, 3600, 0.5, 10, p,
                             1_550_000, 0.55))
                rows.append((wallet, f"{wallet}_c{j}", 1, 5_500_000 + j, 3600, 0.5, 10, p,
                             5_550_000, 0.55))
        return self._raw(rows)

    def _gp(self, churn):
        return bo.GridPoint("t_stat_baseline", bo.NO_DEFLATION, "policy_full_rerank",
                            Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), churn)

    def test_reentry_after_no_signal_pays_full_churn(self):
        ss = self._two_cluster_ss()
        points = [2_000_000, 4_000_000, 6_000_000]            # live, no-signal, live
        runner = _FakeRunner({"w1": 2.0, "w2": 1.0}, points, 1_000_000)
        out = bo.run_trajectory(self._gp(1.0), ss, runner, as_of_points=points,
                                train_secs=1_000_000, horizon_secs=1_000_000, k=5,
                                displacement_margin=5, flat_usd=25.0)
        self.assertTrue(np.isnan(out.returns.iloc[1]))           # no-signal middle period
        # gross = 2*25 + 1*25 = 75; full re-entry churn = 2 wallets * 1.0 = 2 -> 73 BOTH live periods.
        self.assertAlmostEqual(out.returns.iloc[0], 73.0)        # cold start: full admission
        self.assertAlmostEqual(out.returns.iloc[2], 73.0)        # re-entry pays full churn again
        self.assertAlmostEqual(out.returns.iloc[0], out.returns.iloc[2])

    def test_final_cutoff_no_signal_empties_deliverable(self):
        ss = self._two_cluster_ss()
        points = [2_000_000, 4_000_000]                          # live, then no-signal LAST cutoff
        runner = _FakeRunner({"w1": 2.0, "w2": 1.0}, points, 1_000_000)
        out = bo.run_trajectory(self._gp(0.0), ss, runner, as_of_points=points,
                                train_secs=1_000_000, horizon_secs=1_000_000, k=5,
                                displacement_margin=5, flat_usd=25.0)
        self.assertTrue(out.final_follow.empty)                  # no stale fall-back
        self.assertTrue(out.final_scores.empty)


class MinPeriodsAfterCleanTest(unittest.TestCase):
    """#445 defect 2: the ``ranker_min_periods`` floor applies to the CLEANED dense matrix, not the
    raw row count. 24 raw rows that clean to 2 must be an insufficient-periods NO-GO reporting
    raw/clean/dropped counts — not reach leaderboard evaluation on 2 rows."""

    def test_raw_passes_floor_but_clean_count_fails(self):
        rng = np.random.default_rng(0)
        base = rng.normal(0.0, 1.0, 24)
        m = pd.DataFrame({"baseline": base, "challenger": base + 0.8,
                          "n0": base + rng.normal(0.0, 1.0, 24)})
        m.iloc[2:, m.columns.get_loc("n0")] = np.nan             # 22 of 24 rows drop (NaN in n0)
        d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertEqual(d["status"], "NO-GO")
        self.assertIn("insufficient periods", d["reason"])
        self.assertIn("ranker_min_periods", d["reason"])
        self.assertEqual(d["raw_periods"], 24)
        self.assertEqual(d["clean_periods"], 2)
        self.assertEqual(d["dropped_periods"], 22)
        self.assertIsNone(d["winner"])

    def test_clean_count_clears_floor_reports_counts(self):
        # 24 dense rows -> 24 clean -> clears the floor; counts still reported.
        rng = np.random.default_rng(1)
        base = rng.normal(0.0, 1.0, 24)
        m = pd.DataFrame({"baseline": base, "challenger": base + 0.8,
                          "n0": base + rng.normal(0.0, 1.0, 24)})
        d = bo.select_winner_or_nogo(m, baseline_key="baseline", n_grid=20, cpr={"go": True})
        self.assertNotIn("insufficient periods", d.get("reason", ""))
        self.assertEqual(d["raw_periods"], 24)
        self.assertEqual(d["clean_periods"], 24)


class EstimatorPruningDisabledTest(unittest.TestCase):
    """#445 defect 4: the 8a forward-payoff screen no longer PRUNES the estimator axis (its proxy
    target mismatches the realized+MTM horizon objective). ALL registered estimators are carried
    into the pre-registered grid; ``n_grid`` equals the full pre-screen grid."""

    def test_all_estimators_carried_into_grid_despite_screen_keep_1(self):
        ss, skill = _population(8, good=6, bad=6, n_pos=60)
        points = [3_000_000 + i * 1_000_000 for i in range(6)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        runner = _FakeRunner(value, points, 1_000_000)
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=1, demoter_kwargs={"min_periods": 3}, min_periods=2)
        ests = ("t_stat_baseline", "eb_shrinkage_skill", "gu_koenker_npmle")
        axes = _axes(estimators=ests, deflators=(bo.NO_DEFLATION,), policies=("policy_full_rerank",))
        res = bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        grid_ests = {key.split("|")[0] for key in res["manifest"]["grid_keys"]}
        self.assertEqual(grid_ests, set(ests))                   # screen_keep=1 did NOT prune
        self.assertEqual(res["manifest"]["n_grid"], len(axes.enumerate_grid()))


class CrosscheckIdentityTest(unittest.TestCase):
    """#445 defect 5: the live cross-check follows ``final_follow`` (the held set), left-joining
    score metadata. A stateful incumbent absent from fresh scores stays in the check; a successful
    live fetch with zero overlap of a non-empty follow hard-fails (not a clean zero-row result)."""

    def _follow(self, wallets):
        return pd.DataFrame({"wallet": list(wallets), "weight": [1.0] * len(wallets)})

    def _scores(self, mapping):
        return pd.DataFrame({"score": list(mapping.values()),
                             "rank": list(range(1, len(mapping) + 1))}, index=list(mapping.keys()))

    def test_incumbent_absent_from_scores_kept(self):
        follow = self._follow(["wa", "wb"])                      # wb is a held incumbent
        scores = self._scores({"wa": 0.9})                       # wb missing from fresh scores
        realized = pd.DataFrame({"wallet": ["wa", "wb"], "realized_pnl": [5.0, -3.0]})
        crosscheck, summary = bo.crosscheck_deliverable(follow, scores, realized)
        self.assertIn("wb", set(crosscheck["wallet"]))           # NOT dropped by scores∩follow
        self.assertEqual(summary["overlap_count"], 2)
        self.assertAlmostEqual(summary["overlap_fraction"], 1.0)
        self.assertEqual(summary["live_loser_flags"], 1)         # wb negative

    def test_zero_overlap_after_successful_fetch_hard_fails(self):
        follow = self._follow(["wa", "wb"])
        scores = self._scores({"wa": 0.9, "wb": 0.8})
        realized = pd.DataFrame({"wallet": ["wx"], "realized_pnl": [1.0]})   # disjoint cohort
        with self.assertRaises(ValueError):
            bo.crosscheck_deliverable(follow, scores, realized)

    def test_empty_follow_zero_overlap_is_not_hard_fail(self):
        follow = self._follow([])
        scores = pd.DataFrame(columns=["score", "rank"])
        realized = pd.DataFrame({"wallet": ["wx"], "realized_pnl": [1.0]})
        crosscheck, summary = bo.crosscheck_deliverable(follow, scores, realized)
        self.assertEqual(summary["overlap_count"], 0)            # advisory, not a hard fail
        self.assertEqual(summary["n_follow"], 0)


class SubprocessRunnerHermeticTest(unittest.TestCase):
    """#445 defect 9: ``SubprocessBacktestRunner`` is exercised against a hermetic fake executable —
    env wiring, the lowercased wallet file, MTM window bounds, and stale/missing-output handling."""

    def _write_fake(self, path, *, emit_rows: bool):
        lines = [
            "#!/usr/bin/env python3",
            "import json, os",
            "out = os.environ['PE_BACKTEST_OUTPUT_DIR']",
            "wp = os.environ['PE_BACKTEST_INJECTED_WALLETS_PATH']",
            "wallets = [w for w in open(wp).read().splitlines() if w]",
            "assert all(w == w.lower() for w in wallets), wallets",
            "assert os.environ['PE_BACKTEST_MAX_TRADE_COUNT'] == '0'",
            "keys = ('PE_BACKTEST_FLAT_USD','PE_BOOTSTRAP_CACHE_PATH',"
            "'PE_BACKTEST_MTM_WINDOW_START_UNIX','PE_BACKTEST_MTM_WINDOW_END_UNIX')",
            "cap = {'wallets': wallets, 'env': {k: os.environ.get(k) for k in keys}}",
            # Write the capture OUTSIDE the per-call scratch dir (`out`), which the runner removes
            # after parsing — to a fixed path the test passes via `extra_env`.
            "json.dump(cap, open(os.environ['FAKE_CAPTURE_PATH'], 'w'))",
        ]
        if emit_rows:
            lines += [
                "with open(os.path.join(out, 'pnl_by_period.ndjson'), 'w') as f:",
                "    for w in wallets:",
                "        print(json.dumps({'wallet': w, 'period_end': 100, 'realized_pnl': 1.0,"
                " 'unrealized_pnl': 0.0, 'n_fills': 1, 'notional': 10.0, 'is_horizon_mtm': False,"
                " 'open_at_horizon': 0, 'marked_at_horizon': 0}), file=f)",
            ]
        path.write_text("\n".join(lines) + "\n")
        import stat
        path.chmod(path.stat().st_mode | stat.S_IEXEC)

    def test_hermetic_env_lowercase_and_window(self):
        import json as _json
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            binp = Path(d) / "fake_pe_backtest.py"
            self._write_fake(binp, emit_rows=True)
            out = Path(d) / "bt"
            capture = Path(d) / "capture.json"
            runner = bo.SubprocessBacktestRunner(str(binp), "cache.db", str(out),
                                                 extra_env={"FAKE_CAPTURE_PATH": str(capture)})
            df = runner.run(["0xABC", "0xDeF"], flat_usd=25.0, window=(1000, 2000))
            self.assertEqual(len(df), 2)
            self.assertEqual(list(df.columns), bo._PNL_COLUMNS)
            cap = _json.loads(capture.read_text())
            self.assertEqual(cap["wallets"], ["0xabc", "0xdef"])          # lowercased on write
            self.assertEqual(cap["env"]["PE_BACKTEST_MTM_WINDOW_START_UNIX"], "1000")
            self.assertEqual(cap["env"]["PE_BACKTEST_MTM_WINDOW_END_UNIX"], "2000")
            self.assertEqual(cap["env"]["PE_BOOTSTRAP_CACHE_PATH"], "cache.db")
            self.assertEqual(list(out.iterdir()), [])                     # per-call scratch removed

    def test_isolated_dir_ignores_leftover_output_and_missing_is_empty(self):
        # Per-call temp dirs isolate each run: a leftover `pnl_by_period.ndjson` in the SHARED
        # output_dir (a prior run, or a concurrent run's file) is never read, and a binary that emits
        # nothing yields an empty frame — subsumes the #445 defect-9 stale-unlink, now structural.
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "bt"
            out.mkdir(parents=True)
            (out / "pnl_by_period.ndjson").write_text(                    # leftover in the SHARED dir
                '{"wallet":"0xstale","period_end":1,"realized_pnl":99.0,"unrealized_pnl":0.0,'
                '"n_fills":1,"notional":1.0,"is_horizon_mtm":false,"open_at_horizon":0,'
                '"marked_at_horizon":0}\n')
            binp = Path(d) / "noop.py"
            self._write_fake(binp, emit_rows=False)                       # writes NO pnl this run
            capture = Path(d) / "capture.json"
            runner = bo.SubprocessBacktestRunner(str(binp), "c", str(out),
                                                 extra_env={"FAKE_CAPTURE_PATH": str(capture)})
            df = runner.run(["0xa"], flat_usd=25.0)
            self.assertTrue(df.empty)                                     # leftover NOT re-parsed
            self.assertTrue((out / "pnl_by_period.ndjson").exists())      # leftover untouched (isolated)


class TrueClvPreflightTest(unittest.TestCase):
    """#445 defect 10: ``true_clv`` is excluded from the estimator grid (visibly, not a silent
    all-NaN arm) when the CLOB views are absent or position-level ``true_clv_close`` coverage is
    below ``true_clv_coverage_warn_pct``; aborts if that leaves no estimator candidate."""

    def _ss(self, covered_frac):
        n = 100
        n_cov = int(covered_frac * n)
        return pd.DataFrame({"wallet": [f"w{i}" for i in range(n)],
                             "true_clv_close": [0.5] * n_cov + [np.nan] * (n - n_cov)})

    def test_views_absent_excludes_true_clv(self):
        ests, rpt = bo.true_clv_preflight(self._ss(0.9), views_present=False,
                                          estimators=("t_stat_baseline", "true_clv"))
        self.assertEqual(ests, ("t_stat_baseline",))
        self.assertTrue(rpt["true_clv_excluded"])

    def test_low_coverage_excludes_true_clv(self):
        ests, rpt = bo.true_clv_preflight(self._ss(0.10), views_present=True,    # 10% < 30%
                                          estimators=("t_stat_baseline", "true_clv"))
        self.assertEqual(ests, ("t_stat_baseline",))
        self.assertTrue(rpt["true_clv_excluded"])
        self.assertEqual(rpt["coverage_pct"], 10)

    def test_sufficient_coverage_keeps_true_clv(self):
        ests, rpt = bo.true_clv_preflight(self._ss(0.50), views_present=True,    # 50% >= 30%
                                          estimators=("t_stat_baseline", "true_clv"))
        self.assertIn("true_clv", ests)
        self.assertFalse(rpt["true_clv_excluded"])

    def test_exclusion_leaving_no_estimator_aborts(self):
        with self.assertRaises(ValueError):
            bo.true_clv_preflight(self._ss(0.0), views_present=False, estimators=("true_clv",))

    def test_true_clv_not_requested_is_noop(self):
        ests, rpt = bo.true_clv_preflight(self._ss(0.0), views_present=False,
                                          estimators=("t_stat_baseline",))
        self.assertEqual(ests, ("t_stat_baseline",))
        self.assertFalse(rpt["true_clv_excluded"])


# ───────────────────────── issue #451: operator knobs (universe / policies / uniqueness memo) ─────
class UniquenessCacheTest(unittest.TestCase):
    """#451: the per-run (criteria, as_of) uniqueness memo is BIT-IDENTICAL to recomputing, and
    collapses the redundant recomputations across configs/estimators sharing a criteria."""

    def _setup(self):
        ss, skill = _population(2)
        points = [3_000_000 + i * 1_000_000 for i in range(6)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        return ss, points, _FakeRunner(value, points, 1_000_000)

    def test_cache_is_bit_identical(self):
        ss, points, runner = self._setup()
        gp = bo.GridPoint("eb_shrinkage_skill", bo.NO_DEFLATION, "policy_knockout_backfill",
                          Criteria(0, 72.0, 0.0, 1.0, 0.0, 0), 0.0)
        kw = dict(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000, k=5,
                  displacement_margin=5)
        a = bo.run_trajectory(gp, ss, runner, **kw)                          # no cache
        b = bo.run_trajectory(gp, ss, runner, uniqueness_cache={}, **kw)     # with the memo
        pd.testing.assert_series_equal(a.returns, b.returns)
        self.assertEqual(set(a.final_follow["wallet"]), set(b.final_follow["wallet"]))

    def test_cache_collapses_recompute_across_configs(self):
        ss, points, runner = self._setup()
        calls = {"n": 0}
        orig = bo.uniqueness_weights

        def counting(ss_in):
            calls["n"] += 1
            return orig(ss_in)

        crit = Criteria(0, 72.0, 0.0, 1.0, 0.0, 0)
        axes = _axes(estimators=("t_stat_baseline", "eb_shrinkage_skill"),
                     deflators=(bo.NO_DEFLATION,), policies=("policy_full_rerank",), criteria=(crit,))
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=2, min_periods=2)
        bo.uniqueness_weights = counting
        try:
            bo.run_bakeoff(ss, runner, axes, params, created_at=1)
        finally:
            bo.uniqueness_weights = orig
        # 1 criteria × 6 as_of = at most 6 distinct in-samples. Without the memo the screen (2 est × 6)
        # plus the trajectories (2 configs × 6) would recompute up to 24 times; the memo caps it at the
        # distinct (criteria, as_of) count.
        self.assertLessEqual(calls["n"], len(points))
        self.assertGreater(calls["n"], 0)


class BoundedUniverseTest(unittest.TestCase):
    """#451: bounded_universe restricts the materialize input to a copyable position band (dropping
    hyperactive bots + the noise tail), optionally the top-N by activity, or None for the full set."""

    def _con(self):
        import duckdb
        con = duckdb.connect()
        con.execute("CREATE TABLE trades (wallet_hex VARCHAR, market_id VARCHAR, outcome_id INTEGER)")
        rows = [(w, f"m{m}", 0) for w, n in (("0xa", 3), ("0xb", 30), ("0xc", 600), ("0xd", 5))
                for m in range(n)]
        con.executemany("INSERT INTO trades VALUES (?, ?, ?)", rows)
        return con

    def test_none_when_no_bounds(self):
        self.assertIsNone(bo.bounded_universe(self._con()))

    def test_position_band_drops_bot_and_tail(self):
        u = set(bo.bounded_universe(self._con(), pos_min=10, pos_max=100))
        self.assertEqual(u, {"0xb"})        # a=3,d=5 (tail) + c=600 (bot) excluded; b=30 kept

    def test_max_wallets_caps_by_activity(self):
        u = bo.bounded_universe(self._con(), pos_min=1, pos_max=10_000, max_wallets=2)
        self.assertEqual(len(u), 2)
        self.assertEqual(set(u), {"0xc", "0xb"})   # the 2 most-active (600, 30)


class OperatorKnobsCliTest(unittest.TestCase):
    """#451: the universe-bounding + policy-sweep CLI flags parse; defaults preserve committed
    full-universe / all-4-policy behaviour."""

    def test_universe_and_policy_flags_parse(self):
        ns = bo._build_arg_parser().parse_args(
            ["--out-dir", "o", "--pe-backtest", "b", "--cache", "c", "--start-unix", "0",
             "--max-wallets", "1000", "--universe-pos-min", "50", "--universe-pos-max", "250",
             "--policies", "policy_full_rerank", "policy_knockout_backfill"])
        self.assertEqual(ns.max_wallets, 1000)
        self.assertEqual((ns.universe_pos_min, ns.universe_pos_max), (50, 250))
        self.assertEqual(ns.policies, ["policy_full_rerank", "policy_knockout_backfill"])

    def test_defaults_are_full_universe_all_policies(self):
        ns = bo._build_arg_parser().parse_args(
            ["--out-dir", "o", "--pe-backtest", "b", "--cache", "c", "--start-unix", "0"])
        self.assertIsNone(ns.max_wallets)
        self.assertIsNone(ns.universe_pos_min)
        self.assertIsNone(ns.universe_pos_max)
        self.assertEqual(len(ns.policies), 4)

    def test_bad_policy_name_fails_fast(self):
        # choices= makes a typo fail at argparse, not after the expensive materialize (like --engine).
        with self.assertRaises(SystemExit):
            bo._build_arg_parser().parse_args(
                ["--out-dir", "o", "--pe-backtest", "b", "--cache", "c", "--start-unix", "0",
                 "--policies", "policy_typo"])


class DirectInvocationTest(unittest.TestCase):
    """The operator entry must work as a bare script, not only as `python -m ranker.bakeoff`.
    Run directly, `scripts/ranker/` is `sys.path[0]` and its `selectors.py` shadows the stdlib
    `selectors` that `subprocess` imports, crashing at import; the top-of-file re-exec shim must
    make `python scripts/ranker/bakeoff.py` behave like the module form (#451 operator entry)."""

    def test_direct_script_help_runs(self) -> None:
        import subprocess

        script = Path(__file__).resolve().parent / "ranker" / "bakeoff.py"
        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("usage:", result.stdout)


class ParallelDeterminismTest(unittest.TestCase):
    """The parallel ``run_trajectories`` seam (``max_workers > 1``) is BIT-IDENTICAL to serial: each
    trajectory is a deterministic pure function of ``(grid_point, ss, params)`` and the matrix columns
    are assembled in fixed GRID order, so thread scheduling cannot move the return matrix, the memo
    dedup count, or the verdict. CI exercises it with the fake runner (the real speedup needs the
    pe-backtest subprocess, which releases the GIL)."""

    def _fixture(self):
        ss, skill = _population(8, good=6, bad=6, n_pos=60)
        points = [3_000_000 + i * 1_000_000 for i in range(10)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=1, demoter_kwargs={"min_periods": 3}, min_periods=2)
        # 1 est × 2 deflators × 2 policies (incl. the stateful knockout) × 2 criteria × 2 churn = 16
        # configs: enough trajectories that 4 workers genuinely overlap, a stateful policy is
        # exercised, and the 2nd criteria's uniqueness is computed DURING (concurrent) trajectories —
        # so both shared caches (backtest memo + uniqueness) take real contention.
        axes = _axes(estimators=("t_stat_baseline",),
                     deflators=(bo.NO_DEFLATION, DeflatedSharpe.name),
                     policies=("policy_full_rerank", "policy_knockout_backfill"),
                     criteria=(Criteria(0, 72.0, 0.0, 1.0, 0.0, 0),
                               Criteria(0, 72.0, 0.30, 0.70, 0.0, 0)),
                     churn=(0.0, 1.0))
        return ss, value, points, params, axes

    def test_serial_and_parallel_are_bit_identical(self) -> None:
        ss, value, points, params, axes = self._fixture()
        serial = bo.run_bakeoff(ss, _FakeRunner(value, points, 1_000_000), axes, params,
                                created_at=1, max_workers=1)
        parallel = bo.run_bakeoff(ss, _FakeRunner(value, points, 1_000_000), axes, params,
                                  created_at=1, max_workers=4)
        # per-period return matrix identical value-for-value AND column-order-for-column-order
        pd.testing.assert_frame_equal(serial["return_matrix"], parallel["return_matrix"])
        self.assertEqual(list(serial["return_matrix"].columns),
                         list(parallel["return_matrix"].columns))
        # single-flight memo ran each distinct backtest exactly once in BOTH paths
        self.assertEqual(serial["backtest_calls"], parallel["backtest_calls"])
        # verdict + deliverable identical
        self.assertEqual(serial["decision"]["status"], parallel["decision"]["status"])
        self.assertEqual(serial["decision"]["winner"], parallel["decision"]["winner"])
        self.assertEqual(serial["deliverable"]["winner_key"],
                         parallel["deliverable"]["winner_key"])
        pd.testing.assert_frame_equal(serial["deliverable"]["follow"].reset_index(drop=True),
                                      parallel["deliverable"]["follow"].reset_index(drop=True))


class SingleFlightCacheTest(unittest.TestCase):
    """``_SingleFlightCache`` runs the factory exactly once per key even under heavy concurrent
    contention — the thread-safety the shared backtest memo + uniqueness cache depend on."""

    def test_factory_runs_once_per_key_under_contention(self) -> None:
        import threading
        import time
        cache = bo._SingleFlightCache()
        invocations: dict = {}
        inv_lock = threading.Lock()

        def factory(k):
            with inv_lock:
                invocations[k] = invocations.get(k, 0) + 1
            time.sleep(0.02)                 # widen the compute window so same-key requests overlap
            return k * 10

        keys = [i % 5 for i in range(40)]    # 5 distinct keys, 40 concurrent requests
        results: list = [None] * len(keys)

        def worker(idx):
            k = keys[idx]
            results[idx] = cache.get_or_compute(k, lambda: factory(k))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(len(keys))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results, [k * 10 for k in keys])        # every caller got the right value
        self.assertEqual(max(invocations.values()), 1)           # each key computed EXACTLY once
        self.assertEqual(cache.calls, 5)                         # 5 distinct factory invocations
        self.assertEqual(cache.lookups, 40)                      # 40 total requests

    def test_owner_exception_propagates_real_error_to_waiters(self) -> None:
        # A failing factory (a real pe-backtest CalledProcessError under the parallel seam) is
        # re-raised to the waiter as the OWNER's actual exception — not a generic RuntimeError — so a
        # parallel run fails with the same diagnostics as a serial one, and is not counted as a `call`.
        import threading
        cache = bo._SingleFlightCache()
        started = threading.Event()

        class Boom(RuntimeError):
            pass

        def failing_factory():
            started.set()
            started.wait(0)            # no-op; readability — the sleep below holds the key in-flight
            import time
            time.sleep(0.10)           # hold "k" in-flight long enough for the waiter to register
            raise Boom("backtest blew up")

        errors: dict = {}

        def call(tag):
            try:
                cache.get_or_compute("k", failing_factory)
            except BaseException as exc:  # noqa: BLE001 - capturing for the assertion
                errors[tag] = exc

        owner = threading.Thread(target=call, args=("owner",))
        owner.start()
        started.wait()                 # ensure `owner` is the one running the factory
        waiter = threading.Thread(target=call, args=("waiter",))
        waiter.start()
        owner.join()
        waiter.join()
        self.assertIsInstance(errors.get("owner"), Boom)
        self.assertIsInstance(errors.get("waiter"), Boom)        # waiter got the REAL error, not RuntimeError
        self.assertEqual(cache.calls, 0)                         # a raising factory is not a successful call


class WorkersArgTest(unittest.TestCase):
    """`--workers` fails fast (at argparse) on a non-positive value rather than silently running
    serial after the expensive materialize — the fail-fast pattern PR #452 required for `--policies`."""

    def test_rejects_non_positive(self) -> None:
        import argparse
        for bad in ("0", "-1", "-8"):
            with self.assertRaises(argparse.ArgumentTypeError):
                bo._positive_workers(bad)

    def test_accepts_positive(self) -> None:
        self.assertEqual(bo._positive_workers("1"), 1)
        self.assertEqual(bo._positive_workers("12"), 12)


class _FakeRunnerFactory:
    """Picklable per-worker factory mirroring ``bo._SubprocessRunnerFactory`` for the process-executor
    test — fork workers build their own ``MemoizingBacktestRunner(_FakeRunner(...))``."""

    def __init__(self, value, points, horizon):
        self.value = value
        self.points = points
        self.horizon = horizon

    def __call__(self, worker_id):
        return bo.MemoizingBacktestRunner(_FakeRunner(self.value, self.points, self.horizon))


class ProcessExecutorDeterminismTest(unittest.TestCase):
    """``executor='process'`` (a fork pool partitioned by criteria, sharing ``ss`` copy-on-write) is
    BIT-IDENTICAL to serial — it parallelises the GIL-bound ``uniqueness_weights`` cold passes WITHOUT
    changing the math (each trajectory is the same pure function; only WHICH process runs it differs).
    Exercises the real fork ProcessPoolExecutor with a picklable fake runner (no cache/binary)."""

    def test_process_matches_serial_bit_identical(self) -> None:
        ss, skill = _population(8, good=6, bad=6, n_pos=60)
        points = [3_000_000 + i * 1_000_000 for i in range(8)]
        value = {w: (1.0 if s >= 0.5 else -1.0) for w, s in skill.items()}
        params = bo.BakeoffParams(as_of_points=points, train_secs=3_000_000, horizon_secs=1_000_000,
                                  k=5, screen_keep=1, demoter_kwargs={"min_periods": 3}, min_periods=2)
        # 2 criteria so the partition has >1 chunk (workers run distinct criteria in parallel).
        axes = _axes(estimators=("t_stat_baseline",), deflators=(bo.NO_DEFLATION,),
                     policies=("policy_full_rerank", "policy_knockout_backfill"),
                     criteria=(Criteria(0, 72.0, 0.0, 1.0, 0.0, 0),
                               Criteria(0, 72.0, 0.30, 0.70, 0.0, 0)),
                     churn=(0.0,))
        serial = bo.run_bakeoff(ss, _FakeRunner(value, points, 1_000_000), axes, params, created_at=1)
        proc = bo.run_bakeoff(ss, _FakeRunner(value, points, 1_000_000), axes, params, created_at=1,
                              max_workers=4, executor="process",
                              runner_factory=_FakeRunnerFactory(value, points, 1_000_000))
        pd.testing.assert_frame_equal(serial["return_matrix"], proc["return_matrix"])
        self.assertEqual(list(serial["return_matrix"].columns), list(proc["return_matrix"].columns))
        # every (config, step) cell is requested once in BOTH (total lookups equal); `distinct` MAY be
        # higher for the process path (per-worker memos can't dedup an identical set across criteria).
        self.assertEqual(serial["backtest_calls"]["total"], proc["backtest_calls"]["total"])
        self.assertGreaterEqual(proc["backtest_calls"]["distinct"], serial["backtest_calls"]["distinct"])
        self.assertEqual(serial["decision"]["status"], proc["decision"]["status"])
        self.assertEqual(serial["decision"]["winner"], proc["decision"]["winner"])
        self.assertEqual(serial["deliverable"]["winner_key"], proc["deliverable"]["winner_key"])
        pd.testing.assert_frame_equal(serial["deliverable"]["follow"].reset_index(drop=True),
                                      proc["deliverable"]["follow"].reset_index(drop=True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
