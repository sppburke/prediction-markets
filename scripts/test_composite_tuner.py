#!/usr/bin/env python3
"""Unit tests for scripts/composite_tuner/.

Coverage:
- objective.parse_composite_stdout: well-formed + malformed inputs.
- objective.edge_lcb: known-input formula check + n<2 returns None.
- cv: window timing math.
- pbo: deterministic synthetic score matrix gives PBO in [0, 1]; degenerate
  cases return NaN.
- data.distinct_cutoffs: in-memory SQLite fixture, multiple cutoffs.

Subprocess boundary (`invoke_composite`) is exercised by replacing
`subprocess.run` with a stub that emits canned composite stdout.

Run: python3 scripts/test_composite_tuner.py
     or: pytest scripts/test_composite_tuner.py -v
"""
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

# Make `composite_tuner` importable from the package dir.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np

from composite_tuner import cv, data, objective, pbo, tuner  # noqa: E402


# ─── objective.parse_composite_stdout ──────────────────────────────────────────


class TestParseCompositeStdout(unittest.TestCase):
    def test_valid_with_selected_wallets(self):
        stdout = (
            "composite: candidates=12345 selected=3 "
            "(cutoff_unix=1775001599, bhq_q_bps=1000, top_n=3, min_trading_days=20)\n"
            "0xaaa\tcomposite_bps=1234\tpvalue_bps=10\tsharpe_bps=2500\n"
            "0xbbb\tcomposite_bps=900\tpvalue_bps=10\tsharpe_bps=1800\n"
            "0xccc\tcomposite_bps=500\tpvalue_bps=20\tsharpe_bps=900\n"
        )
        n, hexes = objective.parse_composite_stdout(stdout)
        self.assertEqual(n, 3)
        self.assertEqual(hexes, frozenset({"0xaaa", "0xbbb", "0xccc"}))

    def test_zero_selected(self):
        stdout = "composite: candidates=100 selected=0 (cutoff_unix=1, ...)\n"
        n, hexes = objective.parse_composite_stdout(stdout)
        self.assertEqual(n, 0)
        self.assertEqual(hexes, frozenset())

    def test_malformed_header_raises(self):
        with self.assertRaises(ValueError):
            objective.parse_composite_stdout("not a composite header\nrandom line\n")


# ─── objective.edge_lcb ────────────────────────────────────────────────────────


class _MockPos:
    def __init__(self, vwap, outcome):
        self.vwap_entry = vwap
        self.outcome = outcome


class TestEdgeLcb(unittest.TestCase):
    def test_n_lt_2_returns_none(self):
        self.assertIsNone(objective.edge_lcb([_MockPos(0.5, 1.0)]))

    def test_known_inputs(self):
        # Two positions: o=1 c=0.5 (edge +0.5), o=0 c=0.5 (edge -0.5).
        # mean = 0; pstdev = 0.5; lcb = 0 - 1.645 * 0.5 / sqrt(2) = -0.5814
        positions = [_MockPos(0.5, 1.0), _MockPos(0.5, 0.0)]
        lcb = objective.edge_lcb(positions, z=1.645)
        self.assertAlmostEqual(lcb, 0 - 1.645 * 0.5 / (2 ** 0.5), places=6)

    def test_all_winners_positive_lcb_with_low_z(self):
        positions = [_MockPos(0.3, 1.0)] * 10  # all edge = +0.7
        lcb = objective.edge_lcb(positions, z=1.645)
        # std = 0, so lcb = mean = 0.7
        self.assertAlmostEqual(lcb, 0.7, places=6)


# ─── cv.windows_from_db ────────────────────────────────────────────────────────


class TestCvWindows(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "wallet_cache.db"
        with sqlite3.connect(self.db_path) as c:
            c.executescript(
                """
                CREATE TABLE wallet_features (
                    wallet_hex TEXT NOT NULL,
                    cutoff_unix INTEGER NOT NULL,
                    PRIMARY KEY (cutoff_unix, wallet_hex)
                );
                INSERT INTO wallet_features VALUES ('0xa', 1700000000), ('0xb', 1700000000);
                INSERT INTO wallet_features VALUES ('0xa', 1702592000);
                INSERT INTO wallet_features VALUES ('0xa', 1705184000);
                """
            )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_all_cutoffs(self):
        windows = cv.windows_from_db(str(self.db_path))
        self.assertEqual(len(windows), 3)
        self.assertEqual(windows[0].train_cutoff_unix, 1700000000)
        # Default fwd_days = 30
        self.assertEqual(windows[0].fwd_end_unix, 1700000000 + 30 * 86400)

    def test_n_cutoffs_limit(self):
        windows = cv.windows_from_db(str(self.db_path), n_cutoffs=2)
        self.assertEqual(len(windows), 2)
        # Most-recent 2: 1702592000 and 1705184000
        self.assertEqual([w.train_cutoff_unix for w in windows], [1702592000, 1705184000])


# ─── pbo.compute_pbo ───────────────────────────────────────────────────────────


class TestComputePbo(unittest.TestCase):
    def test_pbo_in_unit_interval(self):
        # Random score matrix, modest size.
        rng = np.random.default_rng(7)
        scores = rng.normal(size=(20, 8))
        result = pbo.compute_pbo(scores, n_perms=50, rng_seed=1)
        self.assertGreaterEqual(result.pbo, 0.0)
        self.assertLessEqual(result.pbo, 1.0)
        self.assertEqual(result.n_perms, 50)
        self.assertEqual(result.n_trials, 20)
        self.assertEqual(result.n_windows, 8)
        self.assertEqual(len(result.logit_values), 50)

    def test_too_few_returns_nan(self):
        result = pbo.compute_pbo(np.zeros((1, 5)), n_perms=10)
        self.assertTrue(np.isnan(result.pbo))
        self.assertEqual(result.logit_values, [])

    def test_deterministic_with_seed(self):
        rng = np.random.default_rng(7)
        scores = rng.normal(size=(20, 8))
        a = pbo.compute_pbo(scores, n_perms=50, rng_seed=1)
        b = pbo.compute_pbo(scores, n_perms=50, rng_seed=1)
        self.assertEqual(a.pbo, b.pbo)
        self.assertEqual(a.logit_values, b.logit_values)

    def test_perfectly_persistent_signal_low_pbo(self):
        # Trial 0 dominates in every window; PBO should be ~0 (best IS = best OOS).
        n_trials, n_windows = 30, 10
        scores = np.zeros((n_trials, n_windows))
        scores[0, :] = 10.0  # trial 0 is much better everywhere
        result = pbo.compute_pbo(scores, n_perms=100, rng_seed=42)
        self.assertLess(result.pbo, 0.1, f"expected PBO~0, got {result.pbo}")


# ─── data.distinct_cutoffs ─────────────────────────────────────────────────────


class TestDistinctCutoffs(unittest.TestCase):
    def test_empty_db(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            with sqlite3.connect(tmp.name) as c:
                c.execute(
                    "CREATE TABLE wallet_features (cutoff_unix INTEGER, wallet_hex TEXT, "
                    "PRIMARY KEY (cutoff_unix, wallet_hex))"
                )
            self.assertEqual(data.distinct_cutoffs(tmp.name), [])
        finally:
            os.unlink(tmp.name)

    def test_multiple_cutoffs_sorted(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            with sqlite3.connect(tmp.name) as c:
                c.execute(
                    "CREATE TABLE wallet_features (cutoff_unix INTEGER, wallet_hex TEXT, "
                    "PRIMARY KEY (cutoff_unix, wallet_hex))"
                )
                c.executemany(
                    "INSERT INTO wallet_features VALUES (?, ?)",
                    [(300, "a"), (100, "b"), (200, "c"), (100, "d")],
                )
            self.assertEqual(data.distinct_cutoffs(tmp.name), [100, 200, 300])
        finally:
            os.unlink(tmp.name)


# ─── objective.invoke_composite (mocked subprocess) ────────────────────────────


class TestInvokeCompositeMocked(unittest.TestCase):
    def test_sets_all_12_env_vars(self):
        canned = (
            "composite: candidates=10 selected=1 (cutoff_unix=1, ...)\n"
            "0xdead\tcomposite_bps=100\tpvalue_bps=10\tsharpe_bps=1000\n"
        )
        with mock.patch("composite_tuner.objective.subprocess.run") as run:
            run.return_value = mock.Mock(returncode=0, stdout=canned, stderr="")
            weights = {n: 100 for n in objective.WEIGHT_NAMES}
            objective.invoke_composite("/bin/echo", "/tmp/x.db", 1700, weights, top_n=5)
        env_arg = run.call_args.kwargs["env"]
        for name in objective.WEIGHT_NAMES:
            key = f"PE_SKILL_COMPOSITE_W_{name}"
            self.assertIn(key, env_arg)
            self.assertEqual(env_arg[key], "100")
        self.assertEqual(env_arg["PE_SKILL_CACHE_PATH"], "/tmp/x.db")
        self.assertEqual(env_arg["PE_SKILL_CUTOFF_UNIX"], "1700")
        self.assertEqual(env_arg["PE_SKILL_FORWARD_SOURCE"], "composite")
        self.assertEqual(env_arg["PE_SKILL_TOP_N"], "5")

    def test_nonzero_exit_raises(self):
        with mock.patch("composite_tuner.objective.subprocess.run") as run:
            run.return_value = mock.Mock(returncode=1, stdout="", stderr="boom")
            with self.assertRaises(RuntimeError):
                objective.invoke_composite(
                    "/bin/echo", "/tmp/x.db", 1700, {n: 0 for n in objective.WEIGHT_NAMES}, top_n=1
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
