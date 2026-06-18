#!/usr/bin/env python3
"""stdlib-only unit tests for the issue #369 PR1 CLOB reconciliation scripts.

Covers the pure logic of `clob_vs_polygon_reconciliation.py` and
`clob_winner_outcome_id_check.py` against in-memory sqlite fixtures — no network, no
359 GB cache DB, fully deterministic.  The at-scale run (real CLOB API + real cache DB)
is the production proof recorded in the PR body; this guards the logic in CI.

Imports the two scripts as top-level modules via sys.path.insert (same pattern as
test_portfolio_constructor.py / test_haircut_constants.py / test_ranker_decay.py).

Run: `python3 scripts/test_clob_reconciliation.py`
  or: `pytest scripts/test_clob_reconciliation.py -v`

This test runs in CI as part of `.github/workflows/ci.yml` (Python drift-guard step). It
is stdlib-only by design (no numpy/pandas), so it needs no pip install.
"""
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clob_vs_polygon_reconciliation as recon  # noqa: E402
import clob_winner_outcome_id_check as wcheck  # noqa: E402


def _fixture_conn(resolutions, trades):
    """Build an in-memory DB with the relevant `market_resolutions` + `trades` columns.

    `resolutions`: list of (market_id, winning_outcome_id, source).
    `trades`:      list of (market_id, outcome_id).
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE market_resolutions "
        "(market_id TEXT PRIMARY KEY, winning_outcome_id INTEGER, "
        " resolved_at_unix INTEGER, source TEXT)"
    )
    conn.execute("CREATE TABLE trades (market_id TEXT, outcome_id INTEGER)")
    conn.executemany(
        "INSERT INTO market_resolutions (market_id, winning_outcome_id, source) "
        "VALUES (?, ?, ?)",
        resolutions,
    )
    conn.executemany("INSERT INTO trades VALUES (?, ?)", trades)
    conn.commit()
    return conn


class NormaliseTest(unittest.TestCase):
    def test_backslash_x_becomes_0x(self):
        self.assertEqual(recon.normalise_market_id("\\xABCD"), "0xabcd")

    def test_lowercases(self):
        self.assertEqual(recon.normalise_market_id("0xDEADBEEF"), "0xdeadbeef")

    def test_passthrough_and_strip(self):
        self.assertEqual(recon.normalise_market_id("  0xabc  "), "0xabc")


class WinnerIndexTest(unittest.TestCase):
    def test_yes_picks_zero(self):
        self.assertEqual(recon.winner_index([{"winner": True}, {"winner": False}]), 0)

    def test_no_picks_one(self):
        self.assertEqual(recon.winner_index([{"winner": False}, {"winner": True}]), 1)

    def test_voided_is_none(self):
        self.assertIsNone(recon.winner_index([{"winner": False}, {"winner": False}]))

    def test_two_winners_is_none(self):
        self.assertIsNone(recon.winner_index([{"winner": True}, {"winner": True}]))

    def test_empty_is_none(self):
        self.assertIsNone(recon.winner_index([]))

    def test_multi_outcome_picks_winner(self):
        toks = [{"winner": False}, {"winner": False}, {"winner": True}, {"winner": False}]
        self.assertEqual(recon.winner_index(toks), 2)


class ReconcileTest(unittest.TestCase):
    def test_all_agree_passes_gate(self):
        conn = _fixture_conn(
            resolutions=[
                ("0xa", 1, "polygon"),
                ("0xb", 0, "polygon"),
                ("0xc", None, "polygon"),  # voided both sides
            ],
            trades=[("0xa", 1), ("0xb", 0), ("0xc", 0)],
        )
        r = recon.reconcile(conn, {"0xa": 1, "0xb": 0, "0xc": None})
        self.assertEqual(r["total_overlap"], 3)
        self.assertEqual(r["mismatches"], 0)
        self.assertEqual(r["winner_contradictions"], 0)
        self.assertTrue(r["voids_agree"])
        self.assertEqual(r["agreement_rate"], 1.0)

    def test_splits_contradiction_from_clob_null(self):
        conn = _fixture_conn(
            resolutions=[
                ("0xa", 1, "polygon"),  # clob says 1 → agree
                ("0xb", 0, "polygon"),  # clob says 1 → winner contradiction (both set, differ)
                ("0xd", 1, "polygon"),  # clob says None → clob_null_polygon_set (benign)
            ],
            trades=[("0xa", 1), ("0xb", 0), ("0xb", 1), ("0xd", 1)],
        )
        r = recon.reconcile(conn, {"0xa": 1, "0xb": 1, "0xd": None})
        self.assertEqual(r["total_overlap"], 3)
        self.assertEqual(r["mismatches"], 2)
        self.assertEqual(r["winner_contradictions"], 1)
        self.assertEqual(r["clob_null_polygon_set"], 1)
        self.assertEqual(r["polygon_null_clob_set"], 0)
        self.assertFalse(r["voids_agree"])
        # Only the genuine contradiction is sampled — the benign clob-null is not.
        self.assertEqual(len(r["contradiction_samples"]), 1)
        self.assertEqual(r["contradiction_samples"][0]["market_id"], "0xb")

    def test_polygon_null_clob_set_counted(self):
        conn = _fixture_conn(
            resolutions=[("0xv", None, "polygon")],  # polygon voided, clob has a winner
            trades=[("0xv", 0), ("0xv", 1)],
        )
        r = recon.reconcile(conn, {"0xv": 1})
        self.assertEqual(r["total_overlap"], 1)
        self.assertEqual(r["mismatches"], 1)
        self.assertEqual(r["winner_contradictions"], 0)
        self.assertEqual(r["polygon_null_clob_set"], 1)

    def test_excludes_untraded_and_non_polygon(self):
        conn = _fixture_conn(
            resolutions=[
                ("0xa", 1, "polygon"),  # traded → in overlap
                ("0xe", 1, "polygon"),  # NOT traded → excluded
                ("0xf", 1, "gamma"),  # non-polygon source → excluded
                ("0xg", 1, "dune"),  # non-polygon source → excluded
            ],
            trades=[("0xa", 1), ("0xf", 1), ("0xg", 1)],
        )
        # clob knows every market, but only the traded polygon one counts.
        r = recon.reconcile(conn, {"0xa": 1, "0xe": 1, "0xf": 1, "0xg": 1})
        self.assertEqual(r["total_overlap"], 1)
        self.assertEqual(r["mismatches"], 0)

    def test_clob_only_market_not_in_overlap(self):
        # A CLOB market with no polygon row contributes nothing (overlap is an inner join).
        conn = _fixture_conn(
            resolutions=[("0xa", 1, "polygon")],
            trades=[("0xa", 1), ("0xz", 0)],
        )
        r = recon.reconcile(conn, {"0xa": 1, "0xz": 0})
        self.assertEqual(r["total_overlap"], 1)


class WinnerOutcomeCheckTest(unittest.TestCase):
    def test_in_range_binary_passes_gate(self):
        conn = _fixture_conn(
            resolutions=[],
            trades=[("0xa", 0), ("0xa", 1), ("0xb", 1)],
        )
        r = wcheck.check(conn, {"0xa": [0, 2], "0xb": [1, 2]})
        self.assertEqual(r["candidates_in_trades"], 2)
        self.assertEqual(r["out_of_range_markets"], 0)
        self.assertEqual(r["winner_outcome_traded"], 2)

    def test_out_of_range_outcome_fails_gate(self):
        # Binary market (2 tokens) but a trade carries outcome_id 2 → index spaces misaligned.
        conn = _fixture_conn(resolutions=[], trades=[("0xa", 0), ("0xa", 2)])
        r = wcheck.check(conn, {"0xa": [0, 2]})
        self.assertEqual(r["out_of_range_markets"], 1)
        self.assertEqual(r["out_of_range_samples"][0]["market_id"], "0xa")

    def test_one_sided_trading_is_not_a_failure(self):
        # winner=0 but our wallets only traded the losing outcome 1 — benign one-sided
        # trading, NOT a mapping error: in range, so the gate must stay green.
        conn = _fixture_conn(resolutions=[], trades=[("0xb", 1)])
        r = wcheck.check(conn, {"0xb": [0, 2]})
        self.assertEqual(r["candidates_in_trades"], 1)
        self.assertEqual(r["out_of_range_markets"], 0)
        self.assertEqual(r["winner_outcome_traded"], 0)
        self.assertEqual(r["one_sided_no_winner_trade"], 1)

    def test_untraded_candidate_excluded(self):
        conn = _fixture_conn(resolutions=[], trades=[("0xa", 0)])
        r = wcheck.check(conn, {"0xa": [0, 2], "0xz": [1, 2]})
        self.assertEqual(r["candidates_in_trades"], 1)
        self.assertEqual(r["out_of_range_markets"], 0)

    def test_multi_outcome_in_range(self):
        conn = _fixture_conn(
            resolutions=[],
            trades=[("0xm", 0), ("0xm", 2), ("0xm", 3)],  # all < 4 tokens
        )
        r = wcheck.check(conn, {"0xm": [2, 4]})  # winner index 2, 4-outcome market
        self.assertEqual(r["out_of_range_markets"], 0)
        self.assertEqual(r["multi_outcome_traded"], 1)
        self.assertEqual(r["winner_outcome_traded"], 1)

    def test_voided_market_excluded(self):
        # winner_index None (voided) → not a single-winner candidate.
        conn = _fixture_conn(resolutions=[], trades=[("0xv", 0)])
        r = wcheck.check(conn, {"0xv": [None, 2]})
        self.assertEqual(r["candidates_in_trades"], 0)
        self.assertEqual(r["out_of_range_markets"], 0)


if __name__ == "__main__":
    unittest.main()
