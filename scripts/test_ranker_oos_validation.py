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

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import Validator  # noqa: E402
from ranker.oos_validation import (  # noqa: E402
    VALIDATOR_REGISTRY,
    HansenSPA,
    PBO,
    RomanoWolf,
    assert_no_lookahead,
    brown_goetzmann_cpr,
    paper_fills_crosscheck,
    split_walkforward,
    uniqueness_weights,
)


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


class PaperFillsCrosscheckTest(unittest.TestCase):
    def test_flags_live_loser(self) -> None:
        lb = pd.DataFrame({"wallet": ["a", "b", "c"], "rank": [1, 2, 3]})
        live = pd.DataFrame({"wallet": ["b", "c", "d"], "realized_pnl": [50.0, -20.0, 10.0]})
        out = paper_fills_crosscheck(lb, live)
        self.assertEqual(set(out["wallet"]), {"b", "c"})              # overlap only
        self.assertTrue(bool(out.loc[out.wallet == "c", "disagree"].iloc[0]))   # c lost live
        self.assertFalse(bool(out.loc[out.wallet == "b", "disagree"].iloc[0]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
