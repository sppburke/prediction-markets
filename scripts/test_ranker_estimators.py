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

from ranker.estimators import (  # noqa: E402
    REGISTRY,
    _SD_FLOOR,
    EBShrinkageSkill,
    GuKoenkerNPMLE,
    ProxyCLV,
    TrueCLV,
    TStatBaseline,
    _npmle_scores,
)
from ranker_decay import weighted_stats  # noqa: E402
from scipy.stats import norm  # noqa: E402

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


class EBTau2RobustnessTest(unittest.TestCase):
    """#436 C1: short-track (n<=4, huge se^2) wallets must not drive the EB prior variance tau^2
    negative and flatten the ranking. The old `Var(means) - mean(se^2)` MoM subtracts the UNWEIGHTED
    mean sampling variance, which several noisy n=2 wallets inflate until prior_var < 0; `max(., 1e-9)`
    then collapses shrink to ~0 and EVERY posterior saturates to ~`norm.cdf(mu0 / sqrt(1e-9))` == 1.0,
    so the ranking degenerates to insertion order. The DerSimonian-Laird precision-weighted tau^2
    weights each wallet by 1/se^2, so the noisy wallets cannot drag it down and EDGE wins. (Skilled are
    inserted LAST here, so the old insertion-order collapse would rank them at the BOTTOM.)"""

    def setUp(self) -> None:
        noisy = [(f"noisy{i}", 2, 1) for i in range(10)]    # n=2, 1 win: mean ~ overall mean, huge se^2
        specs = (noisy + [("precise_null", 400, 200)]       # high-n zero-edge: tiny se, no edge
                 + [(w, 50, 25) for w in NULLS]              # 50% baseline
                 + [(w, 50, 35) for w in SKILLED])           # 70% real edge, inserted LAST
        self.ss = _suff_stats(specs)
        self.scores = EBShrinkageSkill().score(self.ss, as_of=0, weights=np.ones(len(self.ss)))

    def test_skilled_outrank_a_high_precision_null(self) -> None:
        # The collapse ranks purely by 1/se (precision), so a high-n zero-edge null beats real edge.
        null_rank = self.scores.loc["precise_null", "rank"]
        self.assertTrue((self.scores.loc[SKILLED, "rank"] < null_rank).all())

    def test_skilled_take_the_top_ranks_despite_noisy_wallets(self) -> None:
        self.assertEqual(set(self.scores.nsmallest(4, "rank").index), set(SKILLED))

    def test_ranking_not_flattened(self) -> None:
        # Not collapsed: skilled posteriors clear the zero-edge null by a wide margin (the old collapse
        # saturated every score to ~1.0, so this gap would vanish).
        self.assertGreater(self.scores.loc[SKILLED, "score"].min(), 0.9)
        self.assertLess(self.scores.loc["precise_null", "score"], 0.8)


class TStatBaselineTest(unittest.TestCase):
    """The §Acceptance benchmark: raw net-edge t-stat ranks skilled above null; drops < 2 obs."""

    def setUp(self) -> None:
        specs = [(w, 40, 28) for w in SKILLED] + [(w, 40, 20) for w in NULLS]
        self.ss = _suff_stats(specs)
        self.scores = TStatBaseline().score(self.ss, as_of=0, weights=np.ones(len(self.ss)))

    def test_output_contract(self) -> None:
        self.assertEqual(list(self.scores.columns), ["score", "rank"])
        self.assertEqual(set(self.scores.nsmallest(4, "rank").index), set(SKILLED))

    def test_zero_dispersion_wallet_dropped(self) -> None:
        # all wins -> sd 0 -> undefined t-stat -> dropped (not ranked top with inf).
        ss = _suff_stats([("perfect", 5, 5), ("a", 10, 6), ("b", 10, 4)])
        out = TStatBaseline().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertNotIn("perfect", out.index)


class GuKoenkerNPMLETest(unittest.TestCase):
    """Headline NPMLE: skilled rank above null, and more evidence beats the same raw win-rate."""

    def setUp(self) -> None:
        specs = ([(w, 60, 42) for w in SKILLED] + [(w, 60, 30) for w in NULLS]
                 + [("low_n", 8, 6)])           # 75% but thin -> shrunk
        self.ss = _suff_stats(specs)
        self.scores = GuKoenkerNPMLE().score(self.ss, as_of=0, weights=np.ones(len(self.ss)))

    def test_output_contract(self) -> None:
        self.assertEqual(list(self.scores.columns), ["score", "rank"])
        self.assertEqual(sorted(self.scores["rank"]), list(range(1, len(self.scores) + 1)))

    def test_skilled_above_null(self) -> None:
        self.assertGreater(self.scores.loc[SKILLED, "score"].mean(),
                           self.scores.loc[NULLS, "score"].mean())

    def test_more_evidence_beats_thin(self) -> None:
        self.assertLess(self.scores.loc["skill0", "rank"], self.scores.loc["low_n", "rank"])

    def test_single_candidate_is_rank_one(self) -> None:
        out = GuKoenkerNPMLE().score(_suff_stats([("solo", 6, 5)]), as_of=0, weights=np.ones(6))
        self.assertEqual(out.loc["solo", "rank"], 1)


class GuKoenkerNumericsTest(unittest.TestCase):
    """#436 C2: log-space EM/scoring (no underflow of a far-from-grid precise wallet), a deterministic
    `grid == 0` boundary (split mass), monotone EM log-likelihood, and a verdict invariant to the
    iteration cap."""

    def test_loglik_monotone_non_decreasing(self) -> None:
        # The EM marginal log-likelihood never falls (catches a broken E/M step).
        x = np.array([-0.5, -0.2, 0.0, 0.3, 0.8, 1.2])
        se = np.array([0.3, 0.2, 0.25, 0.15, 0.2, 0.1])
        grid = np.linspace(x.min(), x.max(), 64)
        _, ll = _npmle_scores(x, se, grid, max_iter=2000, tol=1e-8)
        self.assertTrue(np.all(np.diff(np.asarray(ll)) >= -1e-9), "EM log-likelihood decreased")

    def test_far_from_grid_precise_wallet_not_underflowed(self) -> None:
        # A precise wallet (tiny se) whose mean falls BETWEEN grid nodes: the LINEAR likelihood row
        # underflows to all-zero, so the old EM scored it 0.0 regardless of skill. Log space keeps the
        # responsibilities finite, so this strongly-positive precise wallet scores ~1, not 0.
        x = np.concatenate([np.linspace(-1.0, 1.0, 20), [0.5 + 0.5 / 63 / 2]])
        se = np.concatenate([np.full(20, 0.3), [3e-4]])
        grid = np.linspace(x.min(), x.max(), 64)
        i = len(x) - 1
        linear_row = norm.pdf((x[i] - grid) / se[i]) / se[i]
        self.assertTrue(bool((linear_row == 0).all()))      # the underflow the linear EM hit
        scores, _ = _npmle_scores(x, se, grid, max_iter=2000, tol=1e-8)
        self.assertGreater(scores[i], 0.99)                 # not 0.0

    def test_grid_zero_boundary_mass_is_split(self) -> None:
        # A grid node landing exactly on 0 splits its mass 0.5/0.5, so a wallet at 0 in symmetric data
        # scores ~0.5 deterministically (not flipped by a strict `grid > 0`).
        x = np.array([0.0, -0.5, 0.5])
        se = np.full(3, 0.2)
        grid = np.linspace(-0.5, 0.5, 3)                    # [-0.5, 0.0, 0.5] -> a node exactly at 0
        self.assertIn(0.0, grid.tolist())
        scores, _ = _npmle_scores(x, se, grid, max_iter=2000, tol=1e-8)
        self.assertAlmostEqual(scores[0], 0.5, places=6)

    def test_ranking_stable_to_iteration_cap(self) -> None:
        ss = _suff_stats([(w, 60, 42) for w in SKILLED] + [(w, 60, 30) for w in NULLS]
                         + [("low_n", 8, 6)])

        def ranking(max_iter: int) -> list:
            out = GuKoenkerNPMLE(max_iter=max_iter).score(ss, as_of=0, weights=np.ones(len(ss)))
            return list(out.sort_values("rank").index)

        self.assertEqual(ranking(500), ranking(4000))       # verdict invariant to the cap


def _clv_ss(specs, col: str = "close_proxy") -> pd.DataFrame:
    """specs: list of (wallet, [(price, close), ...]). NaN close = no covering price. ``col`` is the
    close column the CLV estimator reads (``close_proxy`` for proxy_clv, ``true_clv_close`` for
    true_clv) — both estimators share the same weighted-CLV t-stat logic."""
    rows = [(w, p, cp) for w, positions in specs for p, cp in positions]
    return pd.DataFrame(rows, columns=["wallet", "price", col])


class ProxyCLVTest(unittest.TestCase):
    def test_positive_clv_outranks_negative(self) -> None:
        ss = _clv_ss([
            ("good", [(0.4, 0.60), (0.4, 0.65), (0.4, 0.55), (0.4, 0.62)]),   # close > entry
            ("bad", [(0.4, 0.30), (0.4, 0.35), (0.4, 0.32), (0.4, 0.28)]),    # close < entry
        ])
        out = ProxyCLV().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertLess(out.loc["good", "rank"], out.loc["bad", "rank"])

    def test_nan_and_thin_wallets_dropped(self) -> None:
        ss = _clv_ss([
            ("good", [(0.4, 0.6), (0.4, 0.55), (0.4, 0.62)]),
            ("nodata", [(0.4, float("nan")), (0.4, float("nan"))]),           # no pre-res trade
            ("thin", [(0.4, 0.6), (0.4, float("nan"))]),                      # only 1 valid
        ])
        out = ProxyCLV().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertEqual(set(out.index), {"good"})

    def test_all_nan_yields_empty(self) -> None:
        ss = _clv_ss([("w", [(0.4, float("nan")), (0.4, float("nan"))])])
        out = ProxyCLV().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertTrue(out.empty)
        self.assertEqual(list(out.columns), ["score", "rank"])


class TrueCLVTest(unittest.TestCase):
    def test_positive_clv_outranks_negative(self) -> None:
        ss = _clv_ss([
            ("good", [(0.4, 0.60), (0.4, 0.65), (0.4, 0.55), (0.4, 0.62)]),   # close > entry
            ("bad", [(0.4, 0.30), (0.4, 0.35), (0.4, 0.32), (0.4, 0.28)]),    # close < entry
        ], col="true_clv_close")
        out = TrueCLV().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertLess(out.loc["good", "rank"], out.loc["bad", "rank"])

    def test_nan_and_thin_wallets_dropped(self) -> None:
        ss = _clv_ss([
            ("good", [(0.4, 0.6), (0.4, 0.55), (0.4, 0.62)]),
            ("nodata", [(0.4, float("nan")), (0.4, float("nan"))]),           # no CLOB series
            ("thin", [(0.4, 0.6), (0.4, float("nan"))]),                      # only 1 valid
        ], col="true_clv_close")
        out = TrueCLV().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertEqual(set(out.index), {"good"})

    def test_all_nan_yields_empty(self) -> None:
        ss = _clv_ss([("w", [(0.4, float("nan")), (0.4, float("nan"))])], col="true_clv_close")
        out = TrueCLV().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertTrue(out.empty)
        self.assertEqual(list(out.columns), ["score", "rank"])


class FloatFragilitySDFloorTest(unittest.TestCase):
    """#436 A10 follow-up: np.std(ddof=1) of a mathematically-constant net/CLV series is exactly 0.0
    at some n but ~1e-16 at others (mean-rounding in the computational-variance formula), so a bare
    ``sd > 0`` admitted a 6-position win streak with a t-stat ~2e16 and ranked it #1. ``_SD_FLOOR``
    drops these zero-dispersion wallets across ALL FIVE estimators — extending the deliberate n=5
    zero-dispersion drop the existing tests pin to the n>=6 float-noise case. (n=6 verified to give a
    non-zero sd ~1e-16 through ``weighted_stats``, so each drop here fails on a bare ``> 0``.)"""

    def test_constant_series_sd_is_float_noise_below_floor(self) -> None:
        # The fragility, through the estimators' actual stat path: a constant 6-position net series
        # has a tiny NON-ZERO sd, so a bare `sd > 0` admits it; the floor (1e-9) catches it.
        ss = _suff_stats([("perfect6", 6, 6)])
        net = ((ss["payoff"] - ss["_eff"]) / ss["_eff"]).to_numpy()
        _, sd, _, _ = weighted_stats(net, np.ones(6))
        self.assertGreater(sd, 0.0)                   # n=6: float noise, NOT exactly 0 -> `>0` admits
        self.assertLess(sd, _SD_FLOOR)                # ...but below the dispersion floor

    def test_tstat_drops_six_position_streak(self) -> None:
        ss = _suff_stats([("perfect6", 6, 6), ("a", 12, 7), ("b", 12, 5)])
        out = TStatBaseline().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertNotIn("perfect6", out.index)       # undefined t-stat, not rank #1 with +2e16

    def test_gukoenker_drops_six_position_streak(self) -> None:
        ss = _suff_stats([("perfect6", 6, 6), ("a", 12, 7), ("b", 12, 5)])
        out = GuKoenkerNPMLE().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertNotIn("perfect6", out.index)

    def test_eb_drops_six_position_streak(self) -> None:
        # The EB posterior of a zero-dispersion wallet would otherwise be norm.cdf(huge) == 1.0 (max).
        ss = _suff_stats([("perfect6", 6, 6)] + [(w, 40, 28) for w in SKILLED]
                         + [(w, 40, 20) for w in NULLS])
        out = EBShrinkageSkill().score(ss, as_of=0, weights=np.ones(len(ss)))
        self.assertNotIn("perfect6", out.index)

    def test_clv_estimators_drop_six_position_constant(self) -> None:
        for col, est in (("close_proxy", ProxyCLV()), ("true_clv_close", TrueCLV())):
            ss = _clv_ss([("flat6", [(0.4, 0.6)] * 6),
                          ("good", [(0.4, 0.6), (0.4, 0.55), (0.4, 0.62)]),
                          ("bad", [(0.4, 0.3), (0.4, 0.35), (0.4, 0.31)])], col=col)
            out = est.score(ss, as_of=0, weights=np.ones(len(ss)))
            self.assertNotIn("flat6", out.index, msg=col)   # constant CLV -> undefined t-stat


class RegistryTest(unittest.TestCase):
    def test_all_estimators_registered_by_name(self) -> None:
        for cls in (EBShrinkageSkill, TStatBaseline, GuKoenkerNPMLE, ProxyCLV, TrueCLV):
            self.assertIs(REGISTRY[cls.name], cls)


if __name__ == "__main__":
    unittest.main(verbosity=2)
