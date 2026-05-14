#!/usr/bin/env python3
"""
Comprehensive deterministic test suite for scripts/holdout_baseline.py (issue #162).

Covers every pure post-processing function on synthetic fixtures -- no live
backtest runs, no network, no clock dependence. The one part not exercised here
is the `run_backtest` subprocess boundary (a thin `subprocess.run` wrapper);
`analyse_run` is exercised end-to-end against a synthetic ndjson + sqlite cache.

Run:  python3 scripts/test_holdout_baseline.py
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import holdout_baseline as hb  # noqa: E402


def _fill(side, leader, market, outcome, contracts, fill_price, simulated_at,
          operator_id=None):
    """Construct one fill dict in post-`read_fills` form (contracts int,
    fill_price float) -- `classify_round_trip` and friends operate on fills that
    have already been through `read_fills`. `fill_price` is accepted as a string
    or number so call sites can mirror raw ndjson literals.
    """
    return {
        "side": side,
        "leader_wallet": leader,
        "operator_id": operator_id,
        "market_id": market,
        "outcome_id": int(outcome),
        "contracts": int(contracts),
        "signal_price": float(fill_price),
        "fill_price": float(fill_price),
        "simulated_at": simulated_at,
    }


def _rec(exit_kind, e, c, r, x, buy_dt, close_dt, within_sim,
         operator_id="op", market="mkt", outcome=1, leader="ldr"):
    """Construct one classified round-trip record."""
    return {
        "leader": leader, "operator_id": operator_id, "market": market,
        "outcome": outcome, "buy_dt": buy_dt, "e": e, "c": c, "r": r, "x": x,
        "exit_kind": exit_kind, "close_dt": close_dt, "within_sim": within_sim,
    }


class TestParseDay(unittest.TestCase):
    def test_datetime_string(self):
        self.assertEqual(hb.parse_day("2026-04-05T00:00:00Z"), date(2026, 4, 5))

    def test_bare_date(self):
        self.assertEqual(hb.parse_day("2026-03-31"), date(2026, 3, 31))


class TestReadFills(unittest.TestCase):
    def test_reads_and_typecasts(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "trades.ndjson")
            with open(path, "w") as fh:
                fh.write(json.dumps({
                    "side": "buy", "leader_wallet": "L1", "operator_id": "O1",
                    "market_id": "M1", "outcome_id": 1, "contracts": 10,
                    "signal_price": "0.40", "fill_price": "0.40",
                    "simulated_at": "2026-04-05T00:00:00Z"}) + "\n")
                fh.write("\n")  # blank line tolerated
                fh.write(json.dumps({
                    "side": "resolution", "leader_wallet": "L1",
                    "operator_id": None, "market_id": "M1", "outcome_id": 1,
                    "contracts": 10, "signal_price": "0", "fill_price": "1",
                    "simulated_at": "2026-04-20T00:00:00Z"}) + "\n")
            fills = hb.read_fills(path)
        self.assertEqual(len(fills), 2)
        self.assertEqual(fills[0]["contracts"], 10)
        self.assertIsInstance(fills[0]["contracts"], int)
        self.assertEqual(fills[0]["fill_price"], 0.40)
        self.assertIsInstance(fills[0]["fill_price"], float)
        self.assertEqual(fills[0]["operator_id"], "O1")
        self.assertIsNone(fills[1]["operator_id"])


class TestLoadResolutions(unittest.TestCase):
    def test_reads_winner_and_null(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "cache.db")
            con = sqlite3.connect(db)
            con.execute("CREATE TABLE market_resolutions ("
                        "market_id TEXT, winning_outcome_id INTEGER, "
                        "resolved_at_unix INTEGER)")
            con.execute("INSERT INTO market_resolutions VALUES (?,?,?)",
                        ("mkt_A", 1, 1_700_000_000))
            con.execute("INSERT INTO market_resolutions VALUES (?,?,?)",
                        ("mkt_void", None, 1_700_000_001))
            con.commit()
            con.close()
            res = hb.load_resolutions(db)
        self.assertEqual(res["mkt_A"], (1, 1_700_000_000))
        self.assertEqual(res["mkt_void"], (None, 1_700_000_001))
        self.assertNotIn("mkt_absent", res)


class TestPairRoundTrips(unittest.TestCase):
    def test_buy_sell(self):
        fills = [
            _fill("buy", "L", "M", 1, 10, "0.4", "2026-04-05T00:00:00Z"),
            _fill("sell", "L", "M", 1, 10, "0.9", "2026-04-12T00:00:00Z"),
        ]
        rts = hb.pair_round_trips(fills)
        self.assertEqual(len(rts), 1)
        self.assertEqual(rts[0]["buy"]["side"], "buy")
        self.assertEqual(rts[0]["close"]["side"], "sell")

    def test_buy_resolution(self):
        fills = [
            _fill("buy", "L", "M", 1, 10, "0.4", "2026-04-05T00:00:00Z"),
            _fill("resolution", "L", "M", 1, 10, "1", "2026-04-20T00:00:00Z"),
        ]
        rts = hb.pair_round_trips(fills)
        self.assertEqual(len(rts), 1)
        self.assertEqual(rts[0]["close"]["side"], "resolution")

    def test_open_round_trip(self):
        fills = [_fill("buy", "L", "M", 1, 10, "0.4", "2026-04-25T00:00:00Z")]
        rts = hb.pair_round_trips(fills)
        self.assertEqual(len(rts), 1)
        self.assertIsNone(rts[0]["close"])

    def test_multiple_round_trips_same_key(self):
        # buy, sell, buy, sell on one key -> two discrete round-trips.
        fills = [
            _fill("buy", "L", "M", 1, 10, "0.4", "2026-04-05T00:00:00Z"),
            _fill("sell", "L", "M", 1, 10, "0.5", "2026-04-06T00:00:00Z"),
            _fill("buy", "L", "M", 1, 20, "0.6", "2026-04-10T00:00:00Z"),
            _fill("sell", "L", "M", 1, 20, "0.7", "2026-04-11T00:00:00Z"),
        ]
        rts = hb.pair_round_trips(fills)
        self.assertEqual(len(rts), 2)
        self.assertEqual(rts[0]["buy"]["contracts"], 10)
        self.assertEqual(rts[1]["buy"]["contracts"], 20)

    def test_cross_month_round_trips_separated(self):
        # THE key correctness property: a March round-trip and an April
        # round-trip on the SAME key must stay distinct, with their own buy
        # dates -- the `pnl_decomposition.py` sum-and-VWAP shortcut would
        # collapse them and mis-date the position.
        fills = [
            _fill("buy", "L", "M", 1, 10, "0.4", "2026-03-20T00:00:00Z"),
            _fill("sell", "L", "M", 1, 10, "0.5", "2026-03-25T00:00:00Z"),
            _fill("buy", "L", "M", 1, 20, "0.6", "2026-04-05T00:00:00Z"),
            _fill("sell", "L", "M", 1, 20, "0.7", "2026-04-10T00:00:00Z"),
        ]
        rts = hb.pair_round_trips(fills)
        self.assertEqual(len(rts), 2)
        self.assertEqual(hb.parse_day(rts[0]["buy"]["simulated_at"]),
                         date(2026, 3, 20))
        self.assertEqual(hb.parse_day(rts[1]["buy"]["simulated_at"]),
                         date(2026, 4, 5))

    def test_alternation_violation_two_buys(self):
        fills = [
            _fill("buy", "L", "M", 1, 10, "0.4", "2026-04-05T00:00:00Z"),
            _fill("buy", "L", "M", 1, 10, "0.4", "2026-04-06T00:00:00Z"),
        ]
        with self.assertRaises(ValueError):
            hb.pair_round_trips(fills)

    def test_alternation_violation_close_without_buy(self):
        fills = [_fill("sell", "L", "M", 1, 10, "0.9", "2026-04-12T00:00:00Z")]
        with self.assertRaises(ValueError):
            hb.pair_round_trips(fills)


class TestClassifyRoundTrip(unittest.TestCase):
    def setUp(self):
        post_horizon = int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp())
        self.resolutions = {
            "mkt_win": (1, 1_700_000_000),
            "mkt_lose": (0, 1_700_000_000),
            "mkt_open": (1, post_horizon),
            "mkt_void": (None, 1_700_000_000),
        }

    def _rt(self, market, close=None, outcome=1):
        buy = _fill("buy", "L", market, outcome, 10, "0.4",
                    "2026-04-05T00:00:00Z", operator_id="OP")
        return {"key": ("L", market, outcome), "buy": buy, "close": close}

    def test_sold(self):
        close = _fill("sell", "L", "mkt_win", 1, 10, "0.9",
                      "2026-04-12T00:00:00Z")
        rec = hb.classify_round_trip(self._rt("mkt_win", close), self.resolutions)
        self.assertEqual(rec["exit_kind"], "sold")
        self.assertEqual(rec["r"], 1.0)
        self.assertEqual(rec["x"], 0.9)
        self.assertEqual(rec["close_dt"], date(2026, 4, 12))
        self.assertTrue(rec["within_sim"])

    def test_held_via_resolution_row(self):
        close = _fill("resolution", "L", "mkt_lose", 1, 10, "0",
                      "2026-04-20T00:00:00Z")
        rec = hb.classify_round_trip(self._rt("mkt_lose", close), self.resolutions)
        self.assertEqual(rec["exit_kind"], "held")
        self.assertEqual(rec["r"], 0.0)   # outcome 1, winner 0 -> lost
        self.assertEqual(rec["x"], 0.0)   # held -> x == r
        self.assertEqual(rec["close_dt"], date(2026, 4, 20))
        self.assertTrue(rec["within_sim"])

    def test_held_open_cache_resolved_past_horizon(self):
        # No close row -- the sim left it open -- but the cache resolves it.
        rec = hb.classify_round_trip(self._rt("mkt_open", None), self.resolutions)
        self.assertEqual(rec["exit_kind"], "held_open")
        self.assertEqual(rec["r"], 1.0)
        self.assertEqual(rec["x"], 1.0)
        self.assertEqual(rec["close_dt"], date(2026, 6, 1))  # from cache
        self.assertFalse(rec["within_sim"])  # NOT counted by report.json

    def test_unresolved_market_absent(self):
        rec = hb.classify_round_trip(self._rt("mkt_absent", None), self.resolutions)
        self.assertEqual(rec["exit_kind"], "unresolved")
        self.assertIsNone(rec["r"])

    def test_unresolved_voided_null_winner(self):
        rec = hb.classify_round_trip(self._rt("mkt_void", None), self.resolutions)
        self.assertEqual(rec["exit_kind"], "unresolved")
        self.assertIsNone(rec["r"])

    def test_winner_match_is_integer_compare(self):
        # winning_outcome_id from sqlite is an int; outcome_id from ndjson is
        # cast to int in read_fills -- a string/int mismatch must not silently
        # produce r == 0.
        close = _fill("sell", "L", "mkt_win", 1, 10, "0.9",
                      "2026-04-12T00:00:00Z")
        rec = hb.classify_round_trip(self._rt("mkt_win", close, outcome=1),
                                     self.resolutions)
        self.assertEqual(rec["r"], 1.0)


class TestSelectTestWindow(unittest.TestCase):
    def test_inclusive_bounds_on_buy_date(self):
        records = [
            _rec("sold", 0.4, 10, 1.0, 0.9, date(2026, 3, 31), date(2026, 4, 2),
                 True),                                            # before
            _rec("sold", 0.4, 10, 1.0, 0.9, date(2026, 4, 1), date(2026, 5, 1),
                 True),                                            # start bound
            _rec("sold", 0.4, 10, 1.0, 0.9, date(2026, 4, 30), date(2026, 6, 1),
                 True),                                            # end bound
            _rec("sold", 0.4, 10, 1.0, 0.9, date(2026, 5, 1), date(2026, 5, 5),
                 True),                                            # after
        ]
        selected = hb.select_test_window(records, date(2026, 4, 1),
                                         date(2026, 4, 30))
        self.assertEqual(len(selected), 2)
        self.assertEqual({r["buy_dt"] for r in selected},
                         {date(2026, 4, 1), date(2026, 4, 30)})

    def test_close_date_outside_window_is_irrelevant(self):
        # Buy in April, close in June -> selected (windowing is on buy date).
        records = [_rec("held_open", 0.3, 5, 1.0, 1.0, date(2026, 4, 25),
                        date(2026, 6, 1), False)]
        selected = hb.select_test_window(records, date(2026, 4, 1),
                                         date(2026, 4, 30))
        self.assertEqual(len(selected), 1)


class TestDecompose(unittest.TestCase):
    def test_empty_input(self):
        d = hb.decompose([])
        self.assertEqual(d["n"], 0)
        self.assertEqual(d["total"], 0.0)
        self.assertEqual(d["cbar"], 0.0)

    def test_additive_identity_holds(self):
        records = [
            _rec("sold", 0.40, 10, 1.0, 0.70, date(2026, 4, 5), date(2026, 4, 9),
                 True),
            _rec("held", 0.60, 30, 0.0, 0.0, date(2026, 4, 6), date(2026, 4, 8),
                 True),
            _rec("held_open", 0.25, 17, 1.0, 1.0, date(2026, 4, 7),
                 date(2026, 6, 1), False),
        ]
        d = hb.decompose(records)
        self.assertAlmostEqual(
            d["selection"] + d["sizing"] + d["exit_timing"], d["total"],
            places=6)

    def test_known_values(self):
        # Hand-computed: rec A c=10 e=.40 r=1 x=1 ; rec B c=30 e=.60 r=0 x=0.
        # cbar=20; total = 10*.6 + 30*(-.6) = -12.
        # selection = 20*(.6-.6) = 0 ; sizing = -10*.6 + 10*(-.6) = -12 ;
        # exit_timing = 0 ; net edge = (.6-.6)/2 = 0.
        records = [
            _rec("held", 0.40, 10, 1.0, 1.0, date(2026, 4, 5), date(2026, 4, 9),
                 True),
            _rec("held", 0.60, 30, 0.0, 0.0, date(2026, 4, 6), date(2026, 4, 8),
                 True),
        ]
        d = hb.decompose(records)
        self.assertEqual(d["n"], 2)
        self.assertEqual(d["total_contracts"], 40)
        self.assertAlmostEqual(d["cbar"], 20.0)
        self.assertAlmostEqual(d["total"], -12.0, places=6)
        self.assertAlmostEqual(d["selection"], 0.0, places=6)
        self.assertAlmostEqual(d["sizing"], -12.0, places=6)
        self.assertAlmostEqual(d["exit_timing"], 0.0, places=6)
        self.assertAlmostEqual(d["net_per_contract_edge"], 0.0, places=6)

    def test_unresolved_records_are_filtered_out(self):
        records = [
            _rec("held", 0.40, 10, 1.0, 1.0, date(2026, 4, 5), date(2026, 4, 9),
                 True),
            _rec("unresolved", 0.50, 99, None, None, date(2026, 4, 6), None,
                 False),
        ]
        d = hb.decompose(records)
        self.assertEqual(d["n"], 1)  # the unresolved record dropped


class TestDecomposeByOperator(unittest.TestCase):
    def test_groups_and_unknown_bucket(self):
        records = [
            _rec("held", 0.4, 10, 1.0, 1.0, date(2026, 4, 5), date(2026, 4, 9),
                 True, operator_id="O1"),
            _rec("held", 0.6, 20, 0.0, 0.0, date(2026, 4, 6), date(2026, 4, 8),
                 True, operator_id="O1"),
            _rec("held", 0.3, 5, 1.0, 1.0, date(2026, 4, 7), date(2026, 4, 10),
                 True, operator_id=None),
        ]
        by_op = hb.decompose_by_operator(records)
        self.assertEqual(set(by_op), {"O1", "unknown"})
        self.assertEqual(by_op["O1"]["n"], 2)
        self.assertEqual(by_op["unknown"]["n"], 1)


class TestDailyPnlSeries(unittest.TestCase):
    def test_buckets_by_close_date_sorted(self):
        records = [
            _rec("sold", 0.4, 10, 1.0, 0.9, date(2026, 4, 5), date(2026, 4, 12),
                 True),                                       # +5.0 on 04-12
            _rec("held", 0.55, 20, 0.0, 0.0, date(2026, 4, 8), date(2026, 4, 20),
                 True),                                       # -11.0 on 04-20
            _rec("held_open", 0.3, 5, 1.0, 1.0, date(2026, 4, 25),
                 date(2026, 6, 1), False),                    # +3.5 on 06-01
        ]
        series = hb.daily_pnl_series(records)
        self.assertEqual(series, [
            (date(2026, 4, 12), 5.0),
            (date(2026, 4, 20), -11.0),
            (date(2026, 6, 1), 3.5),
        ])

    def test_same_day_closes_aggregate(self):
        records = [
            _rec("sold", 0.4, 10, 1.0, 0.9, date(2026, 4, 5), date(2026, 4, 12),
                 True),                                       # +5.0
            _rec("sold", 0.5, 10, 1.0, 0.8, date(2026, 4, 6), date(2026, 4, 12),
                 True),                                       # +3.0
        ]
        series = hb.daily_pnl_series(records)
        self.assertEqual(len(series), 1)
        self.assertAlmostEqual(series[0][1], 8.0, places=6)


class TestSharpe(unittest.TestCase):
    def test_known_series(self):
        series = [(date(2026, 4, 1), 10.0), (date(2026, 4, 2), 20.0),
                  (date(2026, 4, 3), 30.0)]
        # mean 20, sample stdev 10 -> (20/10) * sqrt(365).
        self.assertAlmostEqual(hb.sharpe(series), 2.0 * hb.SHARPE_ANNUALISATION,
                               places=6)

    def test_fewer_than_two_points(self):
        self.assertEqual(hb.sharpe([(date(2026, 4, 1), 10.0)]), 0.0)
        self.assertEqual(hb.sharpe([]), 0.0)

    def test_zero_variance(self):
        series = [(date(2026, 4, 1), 7.0), (date(2026, 4, 2), 7.0)]
        self.assertEqual(hb.sharpe(series), 0.0)


class TestMaxDrawdown(unittest.TestCase):
    def test_known_series(self):
        # cumulative: 100, 50, 20, 40 ; peak stays 100 ; max dd = 100-20 = 80.
        series = [(date(2026, 4, 1), 100.0), (date(2026, 4, 2), -50.0),
                  (date(2026, 4, 3), -30.0), (date(2026, 4, 4), 20.0)]
        self.assertAlmostEqual(hb.max_drawdown_usd(series), 80.0, places=6)

    def test_monotonic_up_has_no_drawdown(self):
        series = [(date(2026, 4, 1), 10.0), (date(2026, 4, 2), 5.0)]
        self.assertEqual(hb.max_drawdown_usd(series), 0.0)

    def test_fewer_than_two_points(self):
        self.assertEqual(hb.max_drawdown_usd([(date(2026, 4, 1), 10.0)]), 0.0)


class TestReconcile(unittest.TestCase):
    def _records(self):
        # within-sim: +6.0, -? ... use simple numbers.
        return [
            _rec("sold", 0.4, 10, 1.0, 1.0, date(2026, 4, 5), date(2026, 4, 9),
                 True),                                       # 10*(1.0-0.4)=6
            _rec("held", 0.5, 10, 1.0, 1.0, date(2026, 4, 6), date(2026, 4, 8),
                 True),                                       # 10*(1.0-0.5)=5
            _rec("held_open", 0.3, 10, 1.0, 1.0, date(2026, 4, 7),
                 date(2026, 6, 1), False),                    # 10*(1.0-0.3)=7
        ]

    def test_exact_match(self):
        recon, ok = hb.reconcile(self._records(), 11.0)
        self.assertTrue(ok)
        self.assertAlmostEqual(recon["total"], 18.0, places=6)
        self.assertAlmostEqual(recon["total_within_sim"], 11.0, places=6)
        self.assertAlmostEqual(recon["open_past_horizon"], 7.0, places=6)

    def test_within_tolerance(self):
        recon, ok = hb.reconcile(self._records(), 11.4)  # delta 0.4 < floor 1.0
        self.assertTrue(ok)

    def test_breaks_tolerance(self):
        recon, ok = hb.reconcile(self._records(), 20.0)  # delta 9 > tol
        self.assertFalse(ok)
        self.assertFalse(recon["reconciles"])

    def test_held_open_excluded_from_within_sim(self):
        # Only the two within_sim records count toward total_within_sim.
        recon, _ = hb.reconcile(self._records(), 11.0)
        self.assertAlmostEqual(recon["total_within_sim"], 11.0, places=6)
        self.assertNotAlmostEqual(recon["total"], recon["total_within_sim"])


class TestDeriveFlatConfig(unittest.TestCase):
    def test_insert_into_existing_strategy_table(self):
        base = 'output_dir = "/tmp/x"\n\n[strategy]\nslippage_rate = "0.02"\n'
        out = hb.derive_flat_config(base, "25.00")
        parsed = __import__("tomllib").loads(out)
        self.assertEqual(parsed["strategy"]["flat_usd_per_trade"], "25.00")
        self.assertEqual(parsed["strategy"]["slippage_rate"], "0.02")  # preserved
        self.assertEqual(parsed["output_dir"], "/tmp/x")               # preserved

    def test_replace_existing_key(self):
        base = '[strategy]\nflat_usd_per_trade = "10"\nslippage_rate = "0.01"\n'
        out = hb.derive_flat_config(base, "25.00")
        parsed = __import__("tomllib").loads(out)
        self.assertEqual(parsed["strategy"]["flat_usd_per_trade"], "25.00")
        self.assertEqual(parsed["strategy"]["slippage_rate"], "0.01")
        # exactly one assignment line, not two
        self.assertEqual(out.count("flat_usd_per_trade"), 1)

    def test_append_when_no_strategy_table(self):
        base = 'output_dir = "/tmp/x"\nbankroll_usd = "10000"\n'
        out = hb.derive_flat_config(base, "25.00")
        parsed = __import__("tomllib").loads(out)
        self.assertEqual(parsed["strategy"]["flat_usd_per_trade"], "25.00")
        self.assertEqual(parsed["bankroll_usd"], "10000")

    def test_does_not_touch_nested_strategy_subtable(self):
        # A `[strategy.something]` header must not be mistaken for `[strategy]`.
        base = '[strategy]\nslippage_rate = "0.02"\n\n[other]\nx = 1\n'
        out = hb.derive_flat_config(base, "25.00")
        parsed = __import__("tomllib").loads(out)
        self.assertEqual(parsed["strategy"]["flat_usd_per_trade"], "25.00")
        self.assertEqual(parsed["other"]["x"], 1)

    def test_raises_on_invalid_toml(self):
        with self.assertRaises(Exception):
            hb.derive_flat_config("this is [[[ not toml", "25.00")


class TestAnalyseRunIntegration(unittest.TestCase):
    """End-to-end exercise of analyse_run against a synthetic ndjson + cache."""

    def _build_fixture(self, d):
        post_horizon = int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp())
        db = os.path.join(d, "cache.db")
        con = sqlite3.connect(db)
        con.execute("CREATE TABLE market_resolutions ("
                    "market_id TEXT, winning_outcome_id INTEGER, "
                    "resolved_at_unix INTEGER)")
        con.executemany(
            "INSERT INTO market_resolutions VALUES (?,?,?)",
            [("mkt_A", 1, 1_700_000_000),       # RT1 sold, won
             ("mkt_B", 0, 1_700_000_001),       # RT2 held, lost
             ("mkt_open", 1, post_horizon),     # RT3 held_open, won post-horizon
             ("mkt_C", 1, 1_700_000_002),       # RT4 March, sold, won
             ("mkt_void", None, 1_700_000_003)],  # RT6 voided
        )
        con.commit()
        con.close()

        ndjson = os.path.join(d, "trades.ndjson")
        rows = [
            # RT1: April entry, sold, won.
            _fill("buy", "L1", "mkt_A", 1, 10, "0.40", "2026-04-05T00:00:00Z",
                  "O1"),
            _fill("sell", "L1", "mkt_A", 1, 10, "0.90", "2026-04-12T00:00:00Z",
                  "O1"),
            # RT2: April entry, held via resolution row, lost.
            _fill("buy", "L2", "mkt_B", 1, 20, "0.55", "2026-04-08T00:00:00Z",
                  "O2"),
            _fill("resolution", "L2", "mkt_B", 1, 20, "0", "2026-04-20T00:00:00Z",
                  "O2"),
            # RT3: April entry, no close row -> held_open, cache-resolved.
            _fill("buy", "L1", "mkt_open", 1, 5, "0.30", "2026-04-25T00:00:00Z",
                  "O1"),
            # RT4: MARCH entry (out of window), sold, won.
            _fill("buy", "L3", "mkt_C", 1, 8, "0.50", "2026-03-15T00:00:00Z",
                  "O3"),
            _fill("sell", "L3", "mkt_C", 1, 8, "0.80", "2026-03-20T00:00:00Z",
                  "O3"),
            # RT5: April entry, market absent from cache -> unresolved.
            _fill("buy", "L2", "mkt_X", 1, 12, "0.45", "2026-04-18T00:00:00Z",
                  "O2"),
            # RT6: April entry, voided market -> unresolved.
            _fill("buy", "L1", "mkt_void", 1, 7, "0.60", "2026-04-22T00:00:00Z",
                  "O1"),
        ]
        with open(ndjson, "w") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")

        report = os.path.join(d, "report.json")
        # within-sim total = RT1(+5.0) + RT2(-11.0) + RT4(+2.4) = -3.6.
        with open(report, "w") as fh:
            json.dump({"total_pnl_usd": "-3.6"}, fh)
        return ndjson, report, db

    def test_full_pipeline(self):
        with tempfile.TemporaryDirectory() as d:
            ndjson, report, db = self._build_fixture(d)
            result = hb.analyse_run(ndjson, report, db,
                                    date(2026, 4, 1), date(2026, 4, 30))

        # Reconciliation: within-sim total -3.6 matches report.json exactly.
        self.assertTrue(result["reconciles"])
        recon = result["reconciliation"]
        self.assertAlmostEqual(recon["total"], -0.1, places=6)
        self.assertAlmostEqual(recon["total_within_sim"], -3.6, places=6)
        self.assertAlmostEqual(recon["open_past_horizon"], 3.5, places=6)

        tw = result["test_window"]
        # 5 April-entered round-trips (RT1,2,3,5,6); RT4 is March, excluded.
        self.assertEqual(tw["n_round_trips"], 5)
        self.assertEqual(tw["n_resolved"], 3)     # RT1,2,3
        self.assertEqual(tw["n_unresolved"], 2)   # RT5,6
        self.assertEqual(tw["copy_count"], 3)

        # Decomposition over RT1,RT2,RT3 -- hand-computed in the test plan.
        dec = tw["decomposition"]
        self.assertEqual(dec["n"], 3)
        self.assertEqual(dec["total_contracts"], 35)
        self.assertAlmostEqual(dec["total"], -2.5, places=6)
        self.assertAlmostEqual(dec["selection"], 8.75, places=6)
        self.assertAlmostEqual(dec["sizing"], -10.25, places=6)
        self.assertAlmostEqual(dec["exit_timing"], -1.0, places=6)
        self.assertAlmostEqual(dec["net_per_contract_edge"], 0.25, places=6)
        # additive identity
        self.assertAlmostEqual(
            dec["selection"] + dec["sizing"] + dec["exit_timing"],
            dec["total"], places=6)

        # Per-operator: O1 = {RT1, RT3}, O2 = {RT2}.
        by_op = tw["by_operator"]
        self.assertEqual(set(by_op), {"O1", "O2"})
        self.assertEqual(by_op["O1"]["n"], 2)
        self.assertEqual(by_op["O2"]["n"], 1)
        self.assertAlmostEqual(by_op["O1"]["total"], 8.5, places=6)
        self.assertAlmostEqual(by_op["O2"]["total"], -11.0, places=6)

        # Secondary metrics: cumulative [5.0, -6.0, -2.5] -> max dd = 11.0.
        self.assertAlmostEqual(tw["max_drawdown_usd"], 11.0, places=6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
