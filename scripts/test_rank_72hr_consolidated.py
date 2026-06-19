#!/usr/bin/env python3
"""Behaviour tests for the consolidated 72hr buy-and-hold ranker (issue #370 PR1).

Drives `rank_72hr_buyandhold.main()` end-to-end against a tiny, deterministic, on-disk
SQLite cache (frozen window + as-of, fixed timestamps — no clock, no RNG, no network) and
asserts the four properties the consolidation must preserve plus the two it adds:

  * flat (half_life=0) per-wallet stats match an INDEPENDENT numpy recomputation of the
    legacy mean / std(ddof=1) / t-stat (locks the byte-identity proven before the old
    non-streaming script was deleted), and the ranked CSV is sorted by tstat_net desc;
  * the qualifying-positions CSV includes `outcome_id` (pass-2 hard-requires it) and uses
    the FIRST buy per market (window / band / side filters drop the rest);
  * `--universe-from-trades` enumerates exactly the distinct trade wallets, lowercased,
    with malformed rows dropped, honouring `--limit-wallets`;
  * `--universe` and `--universe-from-trades` are mutually exclusive — supplying both, or
    neither, errors;
  * decay (half_life=30) reshapes the score and yields n_eff < n (positive control);
  * an empty edge floor returns 0 (no crash).

Imports the ranker module (numpy + pandas) so CI runs it after `pip install -r
scripts/requirements.txt`, alongside test_ranker_decay.py.

Run: `python3 scripts/test_rank_72hr_consolidated.py`
  or: `pytest scripts/test_rank_72hr_consolidated.py -v`
"""
from __future__ import annotations

import csv
import math
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rank_72hr_buyandhold as rk  # noqa: E402

WA = "0x" + "a" * 40
WB = "0x" + "b" * 40
SLIP = 0.01  # default --slip-cents 1.0 -> 0.01

WIN_START_ISO = "2026-01-01"
WIN_END_ISO = "2026-04-01"   # exclusive
AS_OF_ISO = "2026-04-01"


def ts(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, 12, 0, 0, tzinfo=timezone.utc).timestamp())


def net_of(price: float, payoff: float) -> float:
    """Price-aware net edge, replicated independently of the ranker for the golden check."""
    eff = min(price + SLIP, 0.999)
    return (payoff - eff) / eff


def legacy_stats(nets: list[float]) -> tuple[float, float, float]:
    """Legacy flat mean / std(ddof=1) / t-stat — the pre-decay statistic, computed with
    plain numpy so the golden assertion does not go through the ranker's own helper."""
    v = np.asarray(nets, dtype=float)
    n = v.size
    m = float(v.mean())
    sd = float(v.std(ddof=1)) if n > 1 else float("nan")
    t = (m / sd * math.sqrt(n)) if (n > 1 and sd > 0) else float("nan")
    return m, sd, t


# (wallet, market_id, bought_outcome_id, price, entry_ts, winning_outcome_id) — buys only.
# A position qualifies iff entry in [WIN_START, WIN_END), 0.15<=price<=0.85, market resolved
# + scheduled, and 30s <= (end - entry) < 72h. end is entry+3600 for every market below.
_QUALIFYING = [
    # WA — 5 qualifying first-buys across Feb + Mar (2 active months); mixed win/loss.
    (WA, "MA1", 1, 0.50, ts(2026, 2, 5), 1),   # WON
    (WA, "MA2", 0, 0.40, ts(2026, 2, 10), 1),  # LOST
    (WA, "MA3", 1, 0.60, ts(2026, 2, 20), 1),  # WON
    (WA, "MA4", 1, 0.30, ts(2026, 3, 5), 1),   # WON
    (WA, "MA5", 0, 0.70, ts(2026, 3, 15), 1),  # LOST
    # WB — 4 qualifying first-buys across Feb + Mar.
    (WB, "MB1", 1, 0.50, ts(2026, 2, 8), 1),   # WON
    (WB, "MB2", 1, 0.50, ts(2026, 2, 18), 1),  # WON
    (WB, "MB3", 0, 0.50, ts(2026, 3, 3), 1),   # LOST
    (WB, "MB4", 1, 0.50, ts(2026, 3, 12), 1),  # WON
]

# Rows the filters must DROP — present in the cache but never qualifying.
_NONQUALIFYING = [
    (WA, "MA1", 1, 0.90, ts(2026, 2, 6), 1),    # later buy on MA1 -> first-buy keeps 0.50
    (WA, "MA_OOW", 1, 0.50, ts(2025, 6, 1), 1),  # entry out of window
    (WA, "MA_OOB", 1, 0.05, ts(2026, 2, 12), 1),  # entry price below band
]


def expected_qualifying() -> dict[str, list[tuple[float, float]]]:
    """wallet -> list of (price, payoff) for its qualifying first-buys, in insertion order."""
    out: dict[str, list[tuple[float, float]]] = {WA: [], WB: []}
    for w, _mid, oid, price, _t, win in _QUALIFYING:
        out[w].append((price, 1.0 if oid == win else 0.0))
    return out


def build_core_cache(path: str) -> None:
    """Synthetic cache with exactly wallets WA + WB and their markets."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE trades (wallet_hex TEXT, side TEXT, market_id TEXT, "
                 "outcome_id INTEGER, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER)")
    conn.execute("CREATE TABLE market_resolutions (market_id TEXT, winning_outcome_id INTEGER, "
                 "resolved_at_unix INTEGER)")
    conn.execute("CREATE TABLE market_schedules (market_id TEXT, end_date_unix INTEGER)")

    markets: dict[str, tuple[int, int]] = {}  # market_id -> (winning_outcome_id, end_date_unix)
    for w, mid, oid, price, t, win in _QUALIFYING + _NONQUALIFYING:
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                     (w, "buy", mid, oid, f"{price:.2f}", 100, t))
        end = t + 3600
        # First insertion of a market_id pins its schedule/resolution (first buy's clock).
        markets.setdefault(mid, (win, end))
    # A sell that must be ignored by the side='buy' filter (no phantom position).
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                 (WA, "sell", "MA1", 1, "0.55", 100, ts(2026, 2, 7)))

    for mid, (win, end) in markets.items():
        conn.execute("INSERT INTO market_resolutions VALUES (?,?,?)", (mid, win, end + 3600))
        conn.execute("INSERT INTO market_schedules VALUES (?,?)", (mid, end))
    conn.commit()
    conn.close()


def build_messy_cache(path: str) -> None:
    """Cache exercising load_universe_from_trades validation: duplicates, uppercase, and
    malformed wallet_hex (short + NULL)."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE trades (wallet_hex TEXT, side TEXT, market_id TEXT, "
                 "outcome_id INTEGER, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER)")
    rows = [WA, WA, WB, "0x" + "d" * 40, "0x" + "C" * 40, "0xabc", None]
    for wh in rows:
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                     (wh, "buy", "M", 1, "0.50", 1, ts(2026, 2, 1)))
    conn.commit()
    conn.close()


def run_ranker(db: str, out_dir: str, *extra: str) -> int:
    argv = ["rank_72hr_buyandhold.py", "--db", db, "--out-dir", out_dir,
            "--win-start", WIN_START_ISO, "--win-end", WIN_END_ISO, "--as-of", AS_OF_ISO,
            "--min-avg-per-month", "1", "--min-active-months", "2",
            "--target-n", "5", "--scheduled-only", *extra]
    with mock.patch.object(sys, "argv", argv):
        return rk.main()


def read_csv_rows(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


class FlatRankingGoldenTest(unittest.TestCase):
    """Flat (half_life=0) stats == independent legacy recomputation; ranked desc by tstat_net."""

    def test_flat_stats_and_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            out = str(Path(tmp) / "out")
            build_core_cache(db)
            rc = run_ranker(db, out, "--universe-from-trades", "--half-life-days", "0",
                            "--floor-tstat", "0.5")
            self.assertEqual(rc, 0)

            rows = read_csv_rows(str(Path(out) / "ranked_72hr_buyandhold.csv"))
            by_wallet = {r["wallet"]: r for r in rows}
            self.assertEqual(set(by_wallet), {WA, WB})  # both enumerated + qualified

            exp = expected_qualifying()
            exp_tstat: dict[str, float] = {}
            for w, positions in exp.items():
                nets = [net_of(p, pay) for p, pay in positions]
                m, sd, t = legacy_stats(nets)
                row = by_wallet[w]
                self.assertEqual(int(row["n"]), len(nets))
                self.assertAlmostEqual(float(row["mean_net"]), m, places=12)
                self.assertAlmostEqual(float(row["std_net"]), sd, places=12)
                self.assertAlmostEqual(float(row["tstat_net"]), t, places=12)
                self.assertEqual(float(row["n_eff"]), float(len(nets)))  # flat: n_eff == n
                self.assertEqual(row["eligible"], "True")
                exp_tstat[w] = t

            # ranked CSV must be sorted by tstat_net descending
            csv_order = [r["wallet"] for r in rows]
            indep_order = sorted(exp_tstat, key=lambda w: exp_tstat[w], reverse=True)
            self.assertEqual(csv_order, indep_order)


class PositionsOutcomeIdTest(unittest.TestCase):
    def test_outcome_id_present_and_first_buy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            out = str(Path(tmp) / "out")
            build_core_cache(db)
            run_ranker(db, out, "--universe-from-trades", "--half-life-days", "0")

            pos_path = str(Path(out) / "qualifying_positions_72hr.csv")
            with open(pos_path, newline="") as f:
                reader = csv.DictReader(f)
                self.assertIn("outcome_id", reader.fieldnames)
                rows = list(reader)

            # WA qualifies exactly 5 (dup MA1 / out-of-window / out-of-band / the sell dropped).
            wa_rows = [r for r in rows if r["wallet"] == WA]
            self.assertEqual(len(wa_rows), 5)
            ma1 = next(r for r in wa_rows if r["market_id"] == "MA1")
            self.assertEqual(int(ma1["outcome_id"]), 1)
            self.assertAlmostEqual(float(ma1["price"]), 0.50, places=12)  # FIRST buy, not 0.90


class UniverseFromTradesTest(unittest.TestCase):
    def test_enumerates_distinct_lowercased_dropping_malformed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "messy.db")
            build_messy_cache(db)
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            got = set(rk.load_universe_from_trades(conn, 0))
            self.assertEqual(got, {WA, WB, "0x" + "d" * 40, "0x" + "c" * 40})

    def test_limit_wallets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "messy.db")
            build_messy_cache(db)
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            limited = rk.load_universe_from_trades(conn, 2)
            self.assertEqual(len(limited), 2)
            self.assertTrue(set(limited) <= {WA, WB, "0x" + "d" * 40, "0x" + "c" * 40})


class UniverseMutualExclusionTest(unittest.TestCase):
    def _parse(self, *argv: str):
        with mock.patch.object(sys, "argv", ["rank_72hr_buyandhold.py", *argv]):
            return rk.parse_args()

    def test_both_errors(self) -> None:
        with self.assertRaises(SystemExit):
            self._parse("--universe", "u.txt", "--universe-from-trades")

    def test_neither_errors(self) -> None:
        with self.assertRaises(SystemExit):
            self._parse()

    def test_exactly_one_ok(self) -> None:
        p1 = self._parse("--universe-from-trades", "--win-start", WIN_START_ISO,
                         "--win-end", WIN_END_ISO)
        self.assertTrue(p1.universe_from_trades)
        self.assertIsNone(p1.universe)
        p2 = self._parse("--universe", "u.txt", "--win-start", WIN_START_ISO,
                         "--win-end", WIN_END_ISO)
        self.assertFalse(p2.universe_from_trades)
        self.assertEqual(p2.universe, "u.txt")


class DecayPositiveControlTest(unittest.TestCase):
    """half_life=30 reshapes the score (vs flat) and the Kish n_eff drops below n."""

    def _tstat_and_neff(self, half_life: str) -> tuple[float, float]:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            out = str(Path(tmp) / "out")
            build_core_cache(db)
            run_ranker(db, out, "--universe-from-trades", "--half-life-days", half_life)
            row = {r["wallet"]: r for r in
                   read_csv_rows(str(Path(out) / "ranked_72hr_buyandhold.csv"))}[WA]
            return float(row["tstat_net"]), float(row["n_eff"])

    def test_decay_reshapes_and_n_eff_lt_n(self) -> None:
        flat_t, flat_neff = self._tstat_and_neff("0")
        decay_t, decay_neff = self._tstat_and_neff("30")
        n = len(expected_qualifying()[WA])
        self.assertEqual(flat_neff, float(n))                 # flat: n_eff == n
        self.assertLess(decay_neff, float(n) - 1e-9)          # decay: spread weights => n_eff < n
        self.assertGreater(abs(decay_t - flat_t), 1e-6)       # the score actually moved


class EmptyFloorTest(unittest.TestCase):
    def test_empty_floor_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            out = str(Path(tmp) / "out")
            build_core_cache(db)
            # An unreachable floor: eligible wallets exist, but none clear t-stat >= 999.
            rc = run_ranker(db, out, "--universe-from-trades", "--half-life-days", "0",
                            "--floor-tstat", "999")
            self.assertEqual(rc, 0)  # no crash, graceful stop
            self.assertTrue((Path(out) / "ranked_72hr_buyandhold.csv").exists())
            self.assertFalse((Path(out) / "250_72hr_buyandhold_variance.txt").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
