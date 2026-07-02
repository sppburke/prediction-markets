#!/usr/bin/env python3
"""Drift guard for the honesty layer (issue #421, PR2).

Covers: AFML average-uniqueness de-biasing of co-trading overlap; the LANDMINE-2 look-ahead
guard + walk-forward split; PBO discriminating overfit from genuine; Romano-Wolf (StepM) and
Hansen-SPA flagging a real edge while controlling error on a pure null; Brown-Goetzmann CPR
persistence; and the paper_fills cross-check. Bootstraps are seeded for bit-reproducibility.

Run: ``python3 scripts/test_ranker_oos_validation.py``
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Validator  # noqa: E402
from ranker.oos_validation import (  # noqa: E402
    VALIDATOR_REGISTRY,
    HansenSPA,
    PBO,
    RomanoWolf,
    _uniqueness_weights_pyloop,
    akm_inference_on_winners,
    assert_no_lookahead,
    brown_goetzmann_cpr,
    fcr_selected_ci,
    mrsw_rank_cs,
    paper_fills_crosscheck,
    split_walkforward,
    uniqueness_weights,
)
from scipy.stats import norm  # noqa: E402

from ranker_decay import weighted_stats  # noqa: E402


class UniquenessWeightsTest(unittest.TestCase):
    def test_overlap_is_downweighted(self) -> None:
        # wallet A: 3 fully-overlapping labels; wallet B: 3 disjoint labels.
        ss = pd.DataFrame({
            "wallet": ["A", "A", "A", "B", "B", "B"],
            "entry_ts": [0, 0, 0, 0, 20, 40],
            "ttr_ref": [10, 10, 10, 10, 30, 50],
        })
        w = uniqueness_weights(ss)
        self.assertAlmostEqual(float(w.mean()), 1.0, places=9)         # normalised to mean 1
        self.assertLess(w[:3].mean(), w[3:].mean())                    # overlap down-weighted

    def test_requires_rangeindex(self) -> None:
        ss = pd.DataFrame({"wallet": ["a", "a"], "entry_ts": [0, 1], "ttr_ref": [5, 6]},
                          index=[3, 7])                                # non-contiguous index
        with self.assertRaises(ValueError):
            uniqueness_weights(ss)

    def test_uniform_overlap_does_not_reduce_n_eff(self) -> None:
        # #445 defect 6 (contract): AFML uniqueness is a RELATIVE (mean-1) weight, not an absolute
        # sample-size penalty. When every wallet's labels overlap identically the normalized weights
        # are all equal, so weighted_stats takes the uniform short-circuit and n_eff == n — uniform
        # overlap is NOT penalized (no double-counting vs the unweighted statistic). Locks the
        # retained behavior so a future absolute-overlap penalty is a deliberate change.
        ss = pd.DataFrame({
            "wallet": ["A", "A", "A", "B", "B", "B"],
            "entry_ts": [0, 0, 0, 0, 0, 0],
            "ttr_ref": [10, 10, 10, 10, 10, 10],
        })
        w = uniqueness_weights(ss)
        self.assertTrue(np.allclose(w, 1.0))                           # all equal after mean-1 norm
        net = np.array([0.2, -0.1, 0.3, 0.1, 0.0, -0.2])
        _, _, n_eff, _ = weighted_stats(net, w)
        self.assertEqual(n_eff, float(len(net)))                       # n_eff == n: overlap not penalized

    def test_differential_overlap_does_reduce_n_eff(self) -> None:
        # Contrast (the case uniqueness IS for): A's labels overlap, B's are disjoint -> unequal
        # weights -> Kish n_eff < n. Uniqueness bites only on DIFFERENTIAL overlap within the frame.
        ss = pd.DataFrame({
            "wallet": ["A", "A", "A", "B", "B", "B"],
            "entry_ts": [0, 0, 0, 0, 20, 40],
            "ttr_ref": [10, 10, 10, 10, 30, 50],
        })
        w = uniqueness_weights(ss)
        net = np.array([0.2, -0.1, 0.3, 0.1, 0.0, -0.2])
        _, _, n_eff, _ = weighted_stats(net, w)
        self.assertLess(n_eff, float(len(net)))                        # differential overlap reduces n_eff

    def test_numba_is_bit_identical_to_reference_loop(self) -> None:
        # The JIT `uniqueness_weights` must equal the pure-Python reference BIT-FOR-BIT (not merely
        # close) on a frame with realistic concurrency — varied label counts + overlapping spans — so
        # the ~10-17x speedup is provably free of any result change. The pairwise-sum replication
        # (`_uw_pairwise`) is what makes this hold; a naive numba sum would drift ~1e-16.
        rng = np.random.default_rng(12345)
        rows = []
        for wi in range(60):
            # every 10th wallet is a WHALE (200-400 labels -> >128 covered segments) so the test
            # exercises `_uw_pairwise`'s recursive branch, not just the <=128 base case.
            n = int(rng.integers(200, 400)) if wi % 10 == 0 else int(rng.integers(5, 40))
            starts = rng.integers(0, 1_000_000, n)
            durs = rng.integers(1, 200_000, n)
            for a, d in zip(starts, durs):
                rows.append((f"w{wi}", int(a), int(a + d)))
        ss = pd.DataFrame(rows, columns=["wallet", "entry_ts", "ttr_ref"]).reset_index(drop=True)
        fast = uniqueness_weights(ss)
        ref = _uniqueness_weights_pyloop(ss)
        self.assertTrue(np.array_equal(fast, ref),
                        msg=f"not bit-identical: max|delta|={np.abs(fast - ref).max():.3e}")

    def test_numba_matches_reference_on_edge_shapes(self) -> None:
        # single-instant (entry==ttr) and disjoint single-label wallets also match the reference.
        ss = pd.DataFrame({"wallet": ["a", "b", "c"], "entry_ts": [5, 7, 0],
                           "ttr_ref": [5, 9, 100]}).reset_index(drop=True)   # 'a' is instantaneous
        self.assertTrue(np.array_equal(uniqueness_weights(ss), _uniqueness_weights_pyloop(ss)))


class LookAheadGuardTest(unittest.TestCase):
    def test_assert_no_lookahead(self) -> None:
        assert_no_lookahead(pd.DataFrame({"resolved_at": [5, 8, 10]}), as_of=10)  # ok
        with self.assertRaises(ValueError):
            assert_no_lookahead(pd.DataFrame({"resolved_at": [5, 15]}), as_of=10)

    def test_split_excludes_unresolved_and_windows(self) -> None:
        ss = pd.DataFrame({
            "wallet": list("abcde"),
            "entry_ts": [50, 150, 250, 350, 450],
            "resolved_at": [120, 220, 320, 9999, 9999],
        })
        ins, fwd = split_walkforward(ss, as_of=300, train_secs=300, horizon_secs=200)
        # entry 250 resolves at 320 > 300 -> excluded (look-ahead guard / purge)
        self.assertEqual(set(ins["entry_ts"]), {50, 150})
        self.assertEqual(set(fwd["entry_ts"]), {350, 450})
        self.assertEqual(list(ins.index), [0, 1])                      # fresh RangeIndex
        assert_no_lookahead(ins, as_of=300)                            # guarantee holds

    def test_split_walkforward_arms_the_guard(self) -> None:
        # B1 (#436): split_walkforward arms assert_no_lookahead on the HOT PATH (not just in tests),
        # so a future refactor that reintroduced a post-as_of row into the in-sample track fails
        # loudly here. Verify the guard is invoked on the produced in-sample track at the cutoff.
        ss = pd.DataFrame({"wallet": list("ab"), "entry_ts": [50, 150],
                           "resolved_at": [120, 220]})
        with mock.patch("ranker.oos_validation.assert_no_lookahead") as guard:
            split_walkforward(ss, as_of=300, train_secs=300, horizon_secs=200)
        guard.assert_called_once()
        self.assertEqual(guard.call_args.kwargs["as_of"], 300)
        self.assertEqual(set(guard.call_args.args[0]["entry_ts"]), {50, 150})  # guarded the in-sample

    def test_embargo_shifts_forward_start(self) -> None:
        ss = pd.DataFrame({"wallet": list("ab"), "entry_ts": [350, 450],
                           "resolved_at": [9999, 9999]})
        _, fwd = split_walkforward(ss, as_of=300, train_secs=300, horizon_secs=200,
                                   embargo_secs=100)
        self.assertEqual(set(fwd["entry_ts"]), {450})                  # 350 < 300+100 dropped


def _matrix(cols: dict) -> pd.DataFrame:
    return pd.DataFrame(cols)


class PBOTest(unittest.TestCase):
    def test_genuine_dominant_config_is_not_overfit(self) -> None:
        T, n = 16, 5
        genuine = _matrix({f"c{j}": ([1.0] * T if j == 0 else [0.0] * T) for j in range(n)})
        pbo = PBO(s_groups=8).assess(genuine, n_configs=n)["pbo"].iloc[0]
        self.assertEqual(pbo, 0.0)

    def test_noise_overfits_more_than_genuine(self) -> None:
        T, n = 16, 5
        rng = np.random.default_rng(0)
        noise = _matrix({f"c{j}": rng.normal(0, 1, T) for j in range(n)})
        genuine = _matrix({f"c{j}": ([1.0] * T if j == 0 else [0.0] * T) for j in range(n)})
        pbo_noise = PBO(s_groups=8).assess(noise, n_configs=n)["pbo"].iloc[0]
        pbo_gen = PBO(s_groups=8).assess(genuine, n_configs=n)["pbo"].iloc[0]
        self.assertGreater(pbo_noise, pbo_gen)

    def test_flat_matrix_is_degenerate_nan(self) -> None:
        # A7 (#436): every config identical -> near-zero cross-config dispersion -> PBO undefined.
        # The old `<=`-rank tie rule silently returned 0.0 (reads as "not overfit").
        flat = _matrix({f"c{j}": [1.0] * 16 for j in range(5)})
        out = PBO(s_groups=8).assess(flat, n_configs=5)
        self.assertTrue(np.isnan(out["pbo"].iloc[0]))
        self.assertTrue(bool(out["degenerate"].iloc[0]))

    def test_tie_free_matrix_unchanged_by_midrank(self) -> None:
        # A7 (#436): on tie-free data the strict-better + fractional-tie midrank reduces exactly to
        # the legacy `worse + 1`, so the genuine-dominant case still scores PBO 0.0 (not degenerate).
        genuine = _matrix({f"c{j}": ([1.0] * 16 if j == 0 else [float(j) * 0.1] * 16)
                           for j in range(5)})
        out = PBO(s_groups=8).assess(genuine, n_configs=5)
        self.assertFalse(bool(out["degenerate"].iloc[0]))
        self.assertEqual(out["pbo"].iloc[0], 0.0)


def _leaderboard(seed: int, *, with_edge: bool) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    t = 250
    bench = rng.normal(0.0, 1.0, t)
    cols = {"baseline": bench}
    if with_edge:
        cols["edge"] = bench + 0.5 + rng.normal(0.0, 1.0, t)          # +0.5 mean-return edge
    for i in range(3):
        cols[f"null{i}"] = bench + rng.normal(0.0, 1.0, t)            # zero-mean differential
    return pd.DataFrame(cols)


class RomanoWolfTest(unittest.TestCase):
    def test_conforms_to_protocol(self) -> None:
        self.assertIsInstance(RomanoWolf("baseline"), Validator)
        self.assertIs(VALIDATOR_REGISTRY["romano_wolf"], RomanoWolf)

    def test_flags_real_edge(self) -> None:
        lb = _leaderboard(1, with_edge=True)
        res = RomanoWolf("baseline", reps=200, seed=1).assess(lb, n_configs=lb.shape[1])
        beats = dict(zip(res["config"], res["beats_benchmark"]))
        self.assertTrue(bool(beats["edge"]))

    def test_controls_fwer_on_null(self) -> None:
        lb = _leaderboard(2, with_edge=False)
        res = RomanoWolf("baseline", reps=200, seed=1).assess(lb, n_configs=lb.shape[1])
        self.assertFalse(bool(res["beats_benchmark"].any()))


class HansenSPATest(unittest.TestCase):
    def test_low_pvalue_with_edge(self) -> None:
        lb = _leaderboard(1, with_edge=True)
        spa = HansenSPA("baseline", reps=200, seed=1).assess(lb, n_configs=lb.shape[1])
        self.assertLess(spa["spa_pvalue_consistent"].iloc[0], 0.05)

    def test_high_pvalue_on_null(self) -> None:
        lb = _leaderboard(2, with_edge=False)
        spa = HansenSPA("baseline", reps=200, seed=1).assess(lb, n_configs=lb.shape[1])
        self.assertGreater(spa["spa_pvalue_consistent"].iloc[0], 0.10)


class BrownGoetzmannTest(unittest.TestCase):
    def test_persistence_go(self) -> None:
        rng = np.random.default_rng(2)
        n = 200
        skill = rng.normal(0, 1, n)
        idx = [f"w{i}" for i in range(n)]
        p1 = pd.Series(skill + rng.normal(0, 0.3, n), index=idx)
        p2 = pd.Series(skill + rng.normal(0, 0.3, n), index=idx)      # persistent
        res = brown_goetzmann_cpr(p1, p2)
        self.assertGreater(res["cpr"], 1.0)
        self.assertTrue(res["go"])

    def test_no_persistence(self) -> None:
        rng = np.random.default_rng(3)
        n = 200
        idx = [f"w{i}" for i in range(n)]
        p1 = pd.Series(rng.normal(0, 1, n), index=idx)
        p2 = pd.Series(rng.normal(0, 1, n), index=idx)               # independent
        self.assertFalse(brown_goetzmann_cpr(p1, p2)["go"])

    def test_perfect_persistence_is_go(self) -> None:
        # zero reversals (identical ordering) -> wl=lw=0 -> cpr=inf; go must be True, not False.
        idx = [f"w{i}" for i in range(10)]
        p = pd.Series(range(10), index=idx, dtype=float)
        res = brown_goetzmann_cpr(p, p.copy())
        self.assertEqual((res["wl"], res["lw"]), (0, 0))
        self.assertTrue(res["go"])

    def test_zero_mass_point_does_not_manufacture_go(self) -> None:
        # C3 (#436): a large net-edge zero mass-point (60 wallets at exactly 0 = the median) plus a
        # few non-zero winners. The old `> median` lumped every zero into the LOSER cell with the
        # winners staying winners -> wl=lw=0 -> cpr=inf -> spurious GO. Dropping the median-tied zeros
        # leaves no genuine losers (an empty class), so no persistence can be certified -> go=False.
        idx = [f"z{i}" for i in range(60)] + [f"w{i}" for i in range(10)]
        p1 = pd.Series([0.0] * 60 + [1.0] * 10, index=idx)
        p2 = pd.Series([0.0] * 60 + [1.0] * 10, index=idx)
        res = brown_goetzmann_cpr(p1, p2)
        self.assertEqual((res["ww"], res["ll"]), (10, 0))   # the 60 median-tied zeros are dropped
        self.assertFalse(res["go"])

    def test_tie_drop_is_noop_on_tie_free_data(self) -> None:
        # On continuous data (no exact-median ties) the drop removes <= 1 wallet per period, so a
        # genuinely persistent population still gets a GO — the tie-drop does not change the verdict.
        rng = np.random.default_rng(7)
        n = 200
        skill = rng.normal(0, 1, n)
        idx = [f"w{i}" for i in range(n)]
        p1 = pd.Series(skill + rng.normal(0, 0.3, n), index=idx)
        p2 = pd.Series(skill + rng.normal(0, 0.3, n), index=idx)
        res = brown_goetzmann_cpr(p1, p2)
        self.assertGreater(res["ww"] + res["wl"] + res["lw"] + res["ll"], n - 2)
        self.assertTrue(res["go"])


class PaperFillsCrosscheckTest(unittest.TestCase):
    def test_flags_live_loser(self) -> None:
        lb = pd.DataFrame({"wallet": ["a", "b", "c"], "rank": [1, 2, 3]})
        live = pd.DataFrame({"wallet": ["b", "c", "d"], "realized_pnl": [50.0, -20.0, 10.0]})
        out = paper_fills_crosscheck(lb, live)
        self.assertEqual(set(out["wallet"]), {"b", "c"})              # overlap only
        self.assertTrue(bool(out.loc[out.wallet == "c", "disagree"].iloc[0]))   # c lost live
        self.assertFalse(bool(out.loc[out.wallet == "b", "disagree"].iloc[0]))


class AKMWinnersTest(unittest.TestCase):
    def test_corrects_winners_curse(self) -> None:
        # 60 pure-null arms; the naive max is upward-biased -> the conditional estimate shrinks it.
        rng = np.random.default_rng(0)
        est = rng.normal(0.0, 1.0, 60)
        out = akm_inference_on_winners(est, np.ones(60))
        self.assertEqual(out["winner"], int(np.argmax(est)))
        self.assertLess(out["median_unbiased"], out["naive_estimate"])
        self.assertLessEqual(out["ci_lo"], out["median_unbiased"] + 1e-9)
        self.assertGreaterEqual(out["ci_hi"], out["median_unbiased"] - 1e-9)

    def test_single_arm_is_naive(self) -> None:
        out = akm_inference_on_winners([2.0], [0.5])
        self.assertEqual(out["median_unbiased"], 2.0)
        self.assertEqual(out["truncation"], float("-inf"))

    def test_empty_raises(self) -> None:
        with self.assertRaises(ValueError):
            akm_inference_on_winners([], [])

    def test_near_tie_falls_back_to_unconditional(self) -> None:
        # A5 (2026-07-01 decision record): a winner-vs-runner-up gap below
        # AKM_NEAR_TIE_SIGMA*s makes the truncated-normal law numerically ill-posed
        # (CDF underflow -> brentq returns a degenerate point CI; run24: gap 0.00049 sigma
        # produced ci_lo == ci_hi). Such near-ties must return the honest UNCONDITIONAL
        # normal CI: conditional=False, median == naive, non-degenerate width ~2*1.96*s.
        est = [1.0 + 1e-6, 1.0, -0.5]
        out = akm_inference_on_winners(est, [1.0, 1.0, 1.0])
        self.assertFalse(out["conditional"])
        self.assertEqual(out["median_unbiased"], out["naive_estimate"])
        self.assertGreater(out["ci_hi"] - out["ci_lo"], 3.0)  # ~3.92 at alpha=0.05

    def test_clear_gap_stays_conditional(self) -> None:
        # A gap comfortably above the near-tie threshold keeps the conditional law.
        out = akm_inference_on_winners([2.0, 1.0], [0.5, 0.5])
        self.assertTrue(out["conditional"])

    def test_named_winner_that_is_the_max_is_conditional(self) -> None:
        # A5 (#436): pointing at the actual argmax -> the conditional truncated-normal shrinkage.
        est = np.array([3.0, 1.0, 0.5])
        out = akm_inference_on_winners(est, np.ones(3), winner=0)
        self.assertEqual(out["winner"], 0)
        self.assertTrue(out["conditional"])
        self.assertLess(out["median_unbiased"], out["naive_estimate"])

    def test_named_winner_below_max_falls_back_unconditional(self) -> None:
        # A5 (#436): the awarded winner need not be the argmax; when it is not, the "selected==max"
        # event does not hold -> report the honest UNCONDITIONAL CI, not a spurious shrinkage.
        est = np.array([3.0, 1.0, 0.5])
        out = akm_inference_on_winners(est, np.ones(3), winner=1)
        self.assertEqual(out["winner"], 1)
        self.assertFalse(out["conditional"])
        self.assertEqual(out["median_unbiased"], 1.0)           # naive (no truncation correction)

    def test_zero_se_winner_is_point_ci(self) -> None:
        # F3 (#436 Phase F): a zero-SE winner makes the truncated-normal law a step function
        # (ill-posed for brentq) -> a degenerate point CI [y, y], reported unconditional.
        est = np.array([3.0, 1.0, 0.5])
        out = akm_inference_on_winners(est, np.array([0.0, 1.0, 1.0]), winner=0)
        self.assertEqual(out["winner"], 0)
        self.assertFalse(out["conditional"])
        self.assertEqual(out["ci_lo"], 3.0)
        self.assertEqual(out["ci_hi"], 3.0)
        self.assertEqual(out["median_unbiased"], 3.0)

    def test_exact_top_tie_falls_back_unconditional(self) -> None:
        # #445 defect 7: an EXACT top tie (the named/argmax winner == the runner-up estimate) has no
        # "selected == STRICT max" event, so the truncated-normal conditioning is ill-posed at the
        # boundary (`y == lower` pins the CDF at its truncation point; the probe returned a spurious
        # median/CI near -7.29). Fall back to the honest UNCONDITIONAL normal CI, not a degenerate
        # brentq solve. (Contract #445: exact ties are unconditional unless a tie-aware selection
        # event is implemented; it is not in this remediation.)
        est = np.array([1.0, 1.0, 0.0])              # winner 0 ties runner-up (est[1] == 1.0)
        out = akm_inference_on_winners(est, np.ones(3), winner=0)
        self.assertEqual(out["winner"], 0)
        self.assertFalse(out["conditional"])
        self.assertEqual(out["median_unbiased"], 1.0)            # naive (no truncation correction)
        z = float(norm.ppf(0.975))
        self.assertAlmostEqual(out["ci_lo"], 1.0 - z)            # unconditional N(y, s^2) CI, s = 1
        self.assertAlmostEqual(out["ci_hi"], 1.0 + z)

    def test_argmax_tie_default_winner_is_unconditional(self) -> None:
        # #445 defect 7: same tie via the DEFAULT argmax path (winner=None -> argmax picks index 0,
        # which ties index 1) -> still unconditional, not an ill-posed conditional solve.
        out = akm_inference_on_winners(np.array([2.0, 2.0]), np.ones(2))
        self.assertFalse(out["conditional"])
        self.assertEqual(out["median_unbiased"], 2.0)


class MRSWRankCSTest(unittest.TestCase):
    def test_clear_leader_in_cs_losers_excluded(self) -> None:
        est = np.array([5.0, 1.0, 1.0, -3.0, -3.0])
        cs = mrsw_rank_cs(est, np.full(5, 0.3), tau=1)
        in_cs = set(cs.index[cs["in_top_tau_cs"]])
        self.assertIn(0, in_cs)                                  # the clear leader could be rank 1
        self.assertNotIn(3, in_cs)                              # a clear loser cannot
        self.assertNotIn(4, in_cs)
        self.assertEqual(cs.loc[0, "point_rank"], 1)

    def test_indistinct_top_widens_cs(self) -> None:
        # near-tied leaders with wide SEs -> several share the top-1 CS (don't hard-cut).
        cs = mrsw_rank_cs(np.array([1.0, 0.95, 0.9]), np.full(3, 1.0), tau=1)
        self.assertGreaterEqual(int(cs["in_top_tau_cs"].sum()), 2)


class FCRSelectedCITest(unittest.TestCase):
    def test_wider_than_unadjusted_and_widest_at_r1(self) -> None:
        est = np.array([2.0, 1.5, 1.0, 0.5])
        ses = np.ones(4)
        naive = 2.0 * float(norm.ppf(0.975))                    # unadjusted 95% width
        r1 = fcr_selected_ci(est, ses, np.array([True, False, False, False]))
        r4 = fcr_selected_ci(est, ses, np.array([True, True, True, True]))
        w1 = float((r1["ci_hi"] - r1["ci_lo"]).iloc[0])
        w4 = float((r4["ci_hi"] - r4["ci_lo"]).iloc[0])
        self.assertGreater(w1, naive - 1e-9)                    # FCR >= nominal
        self.assertGreater(w1, w4)                              # R=1 (worst selection) widest
        self.assertAlmostEqual(float(r4["fcr_level"].iloc[0]), 0.95)  # R=m -> nominal 1-q

    def test_accepts_index_array_and_empty(self) -> None:
        est, ses = np.array([1.0, 2.0, 3.0]), np.ones(3)
        out = fcr_selected_ci(est, ses, np.array([2]))          # index form
        self.assertEqual(list(out["index"]), [2])
        empty = fcr_selected_ci(est, ses, np.array([], dtype=int))
        self.assertTrue(empty.empty)


if __name__ == "__main__":
    unittest.main(verbosity=2)
