#!/usr/bin/env python3
"""DuckDB-vs-SQLite bit-parity tests for the 72hr ranker read-layer (issue #375).

Builds one deterministic on-disk SQLite cache whose `trades` / `market_resolutions`
/ `market_schedules` declare the REAL column types (`outcome_id`/`contracts`
INTEGER, `price_str` TEXT, `winning_outcome_id` INTEGER) so DuckDB's real
BIGINT/VARCHAR behaviour is exercised, then:

  * runs pass-1 (`rank_72hr_buyandhold`) under the SQLite engine -> CSVs;
  * exports the cache to Parquet (`export_trades_parquet`);
  * runs pass-1 under the DuckDB engine -> CSVs;
  * asserts the qualifying-positions set is identical and the ranked stats are
    identical to rtol 1e-9 (the parity guarantee);
  * runs pass-2 (`latency_shift_rerank`) under both engines and asserts the
    latency-shifted ranking is identical;
  * asserts `get_engine` auto-detect falls back to SQLite on a missing/stale snapshot.

Fixture edge cases (each must be handled identically by both engines): first-buy
dedup (a later same-market buy is ignored), a sell (side filter), out-of-window,
out-of-band, unresolved market, a non-numeric `price_str`, and an exact
`price_str="1.0"` (the open-interval `0<price<1` boundary). Timestamps within a
(wallet,market) and within a (market,outcome) tape are DISTINCT, so the first-buy
and tape order are deterministic in both engines (the rare exact-tie is a documented
sub-1e-9 residual, deferred to the real-DB diff).

Deterministic: fixed window + as-of + timestamps; no clock, no RNG, no network.
Requires duckdb (skips with a clear message if unavailable).

Run: `python3 scripts/test_ranker_duck_parity.py`
  or: `pytest scripts/test_ranker_duck_parity.py -v`
"""
from __future__ import annotations

import csv
import math
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_trades_parquet as exp  # noqa: E402
import latency_shift_rerank as ls  # noqa: E402
import rank_72hr_buyandhold as rk  # noqa: E402
import ranker_duck  # noqa: E402

try:
    import duckdb  # noqa: F401

    HAVE_DUCKDB = True
except ImportError:
    HAVE_DUCKDB = False

WIN_START_ISO = "2026-01-01"
WIN_END_ISO = "2026-04-01"  # exclusive
AS_OF_ISO = "2026-04-01"

# Numeric columns of the pass-1 ranked CSV to compare at rtol 1e-9 (NaN == NaN).
RANKED_NUMERIC = ["n", "active_months", "avg_per_active_month", "mean_gross",
                  "std_gross", "tstat_gross", "mean_net", "std_net", "tstat_net",
                  "n_eff", "hit_rate", "avg_price", "avg_ttr_hours"]
LS_NUMERIC = ["n_total", "n_filled", "fill_rate", "active_months", "mean_net_ls",
              "tstat_net_ls", "n_eff", "hit_rate"]


def ts(y: int, m: int, d: int, hh: int = 12) -> int:
    return int(datetime(y, m, d, hh, 0, 0, tzinfo=timezone.utc).timestamp())


def W(c: str) -> str:
    return "0x" + c * 40


# (wallet, market, outcome_id, price_str, entry_ts, winning_outcome_id) — buys.
# end_date = entry+3600 for every market -> ttr 1h (inside [30s, 72h)).
_QUALIFYING = [
    (W("a"), "MA1", 1, "0.50", ts(2026, 2, 5), 1),
    (W("a"), "MA2", 0, "0.40", ts(2026, 2, 10), 1),
    (W("a"), "MA3", 1, "0.60", ts(2026, 2, 20), 1),
    (W("a"), "MA4", 1, "0.30", ts(2026, 3, 5), 1),
    (W("a"), "MA5", 0, "0.70", ts(2026, 3, 15), 1),
    (W("b"), "MB1", 1, "0.50", ts(2026, 2, 8), 1),
    (W("b"), "MB2", 1, "0.55", ts(2026, 2, 18), 1),
    (W("b"), "MB3", 0, "0.45", ts(2026, 3, 3), 1),
    (W("b"), "MB4", 1, "0.52", ts(2026, 3, 12), 1),
    (W("c"), "MC1", 1, "0.20", ts(2026, 2, 2), 1),
    (W("c"), "MC2", 1, "0.25", ts(2026, 2, 22), 1),
    (W("c"), "MC3", 1, "0.35", ts(2026, 3, 9), 1),
    (W("c"), "MC4", 0, "0.80", ts(2026, 3, 19), 1),
]

# Rows the filters MUST drop, identically in both engines.
_NONQUALIFYING = [
    (W("a"), "MA1", 1, "0.90", ts(2026, 2, 5, 14), 1),   # later same-market buy -> first-buy keeps 0.50
    (W("a"), "MA_OOW", 1, "0.50", ts(2025, 6, 1), 1),    # entry out of window
    (W("a"), "MA_OOB", 1, "0.05", ts(2026, 2, 12), 1),   # below band
    (W("c"), "MC_BAD", 1, "notanumber", ts(2026, 2, 14), 1),  # non-numeric price
    (W("c"), "MC_ONE", 1, "1.0", ts(2026, 2, 16), 1),    # exact 1.0 -> fails 0<p<1
    (W("c"), "MC_UNRES", 1, "0.50", ts(2026, 2, 28), 1),  # market never resolved
    (W("d"), "MD_OOB", 1, "0.95", ts(2026, 2, 4), 1),    # WD: only a dropped row -> no positions
]


def build_parity_cache(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE trades (wallet_hex TEXT, side TEXT, market_id TEXT, "
                 "outcome_id INTEGER, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER)")
    conn.execute("CREATE TABLE market_resolutions (market_id TEXT, winning_outcome_id INTEGER, "
                 "resolved_at_unix INTEGER)")
    conn.execute("CREATE TABLE market_schedules (market_id TEXT, end_date_unix INTEGER)")

    markets: dict[str, tuple[int, int]] = {}
    for w, mid, oid, px, t, win in _QUALIFYING + _NONQUALIFYING:
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                     (w, "buy", mid, oid, px, 100, t))
        markets.setdefault(mid, (win, t + 3600))
    # A sell that the side='buy' filter must ignore.
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                 (W("a"), "sell", "MA1", 1, "0.55", 100, ts(2026, 2, 6)))

    # Pass-2 tapes: a same-(market,outcome) fill ~25s after each qualifying entry, plus a
    # decoy earlier/later trade so bisect has to find the right index. DISTINCT timestamps.
    for w, mid, oid, px, t, win in _QUALIFYING:
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                     (w, "buy", mid, oid, f"{float(px) + 0.01:.3f}", 50, t + 25))  # the fill
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
                     (W("z"), "buy", mid, oid, f"{float(px) + 0.02:.3f}", 50, t + 200))  # later decoy

    for mid, (win, end) in markets.items():
        if mid == "MC_UNRES":
            continue  # unresolved: present in schedules but never resolved
        conn.execute("INSERT INTO market_resolutions VALUES (?,?,?)", (mid, win, end + 3600))
    for mid, (win, end) in markets.items():
        conn.execute("INSERT INTO market_schedules VALUES (?,?)", (mid, end))
    conn.commit()
    conn.close()


def run_pass1(db: str, out_dir: str, engine: str, parquet_dir: str) -> int:
    argv = ["rank_72hr_buyandhold.py", "--db", db, "--out-dir", out_dir,
            "--universe-from-trades",
            "--win-start", WIN_START_ISO, "--win-end", WIN_END_ISO, "--as-of", AS_OF_ISO,
            "--min-avg-per-month", "1", "--min-active-months", "2",
            "--target-n", "5", "--floor-tstat", "0.0", "--scheduled-only"]
    env = {"PE_RANKER_ENGINE": engine, "PE_RANKER_PARQUET_DIR": parquet_dir,
           "PE_RANKER_PARQUET_MAX_AGE_HOURS": "0"}
    with mock.patch.dict(os.environ, env), mock.patch.object(sys, "argv", argv):
        return rk.main()


def run_pass2(db: str, out_dir: str, engine: str, parquet_dir: str) -> int:
    argv = ["latency_shift_rerank.py", "--db", db,
            "--ranked-csv", str(Path(out_dir) / "ranked_72hr_buyandhold.csv"),
            "--positions-csv", str(Path(out_dir) / "qualifying_positions_72hr.csv"),
            "--out-dir", out_dir, "--as-of", AS_OF_ISO,
            "--latency-shift-secs", "20", "--fill-window-secs", "120",
            "--floor-tstat", "0.0", "--min-fill-rate", "0.0",
            "--min-avg-per-month", "1", "--min-active-months", "2"]
    env = {"PE_RANKER_ENGINE": engine, "PE_RANKER_PARQUET_DIR": parquet_dir,
           "PE_RANKER_PARQUET_MAX_AGE_HOURS": "0"}
    with mock.patch.dict(os.environ, env), mock.patch.object(sys, "argv", argv):
        return ls.main()


def export(db: str, parquet_dir: str) -> None:
    argv = ["export_trades_parquet.py", "--db", db, "--out-dir", parquet_dir]
    with mock.patch.object(sys, "argv", argv):
        assert exp.main() == 0


def read_rows(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _num_eq(a: str, b: str) -> bool:
    """Equal as numbers at rtol 1e-9, with '' and NaN both treated as 'absent' and equal."""
    if a == b:
        return True
    fa = float(a) if a not in ("", None) else math.nan
    fb = float(b) if b not in ("", None) else math.nan
    if math.isnan(fa) and math.isnan(fb):
        return True
    return math.isclose(fa, fb, rel_tol=1e-9, abs_tol=1e-12)


def positions_set(path: str) -> set[tuple]:
    """Qualifying positions as an order-independent set; price-derived floats rounded
    to 12 dp so the set comparison is exact while still catching real divergence."""
    out = set()
    for r in read_rows(path):
        out.add((
            r["wallet"], r["market_id"], int(r["outcome_id"]), int(r["entry_ts"]),
            int(r["ttr_secs"]), round(float(r["price"]), 12), int(r["contracts"]),
            round(float(r["payoff"]), 12), round(float(r["gross"]), 12),
            round(float(r["net"]), 12), int(r["resolved_at"]),
        ))
    return out


class DuckParityTest(unittest.TestCase):
    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_pass1_and_pass2_parity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            pq = str(Path(tmp) / "parquet")
            out_sql = str(Path(tmp) / "sql")
            out_duck = str(Path(tmp) / "duck")
            build_parity_cache(db)

            self.assertEqual(run_pass1(db, out_sql, "sqlite", pq), 0)
            export(db, pq)
            self.assertEqual(run_pass1(db, out_duck, "duck", pq), 0)

            # AC: identical qualifying-positions SET.
            ps_sql = positions_set(str(Path(out_sql) / "qualifying_positions_72hr.csv"))
            ps_duck = positions_set(str(Path(out_duck) / "qualifying_positions_72hr.csv"))
            pos_ok = ps_sql == ps_duck
            print(f"{'PASS' if pos_ok else 'FAIL'}: pass1_positions_identical "
                  f"(sqlite={len(ps_sql)}, duck={len(ps_duck)}, "
                  f"sym_diff={len(ps_sql ^ ps_duck)})")
            self.assertTrue(pos_ok, f"positions diverge: {ps_sql ^ ps_duck}")

            # AC: identical ranked stats (rtol 1e-9), same wallets, same tstat order.
            r_sql = {r["wallet"]: r for r in read_rows(str(Path(out_sql) / "ranked_72hr_buyandhold.csv"))}
            r_duck_rows = read_rows(str(Path(out_duck) / "ranked_72hr_buyandhold.csv"))
            r_duck = {r["wallet"]: r for r in r_duck_rows}
            self.assertEqual(set(r_sql), set(r_duck), "ranked wallet sets differ")
            ranked_ok = True
            for w in r_sql:
                for col in RANKED_NUMERIC:
                    if not _num_eq(r_sql[w][col], r_duck[w][col]):
                        ranked_ok = False
                        print(f"  RANK DIVERGE {w}.{col}: {r_sql[w][col]} != {r_duck[w][col]}")
                if r_sql[w]["eligible"] != r_duck[w]["eligible"]:
                    ranked_ok = False
            # tstat_net descending order must match (deterministic given identical stats)
            order_sql = [r["wallet"] for r in
                         read_rows(str(Path(out_sql) / "ranked_72hr_buyandhold.csv"))]
            order_duck = [r["wallet"] for r in r_duck_rows]
            order_ok = order_sql == order_duck
            print(f"{'PASS' if ranked_ok and order_ok else 'FAIL'}: pass1_ranked_identical "
                  f"(wallets={len(r_sql)}, rtol=1e-9, order_match={order_ok})")
            self.assertTrue(ranked_ok, "ranked stats diverge beyond rtol 1e-9")
            self.assertTrue(order_ok, f"ranked order differs: {order_sql} != {order_duck}")

            # AC: pass-2 latency-shift ranking identical across engines.
            self.assertEqual(run_pass2(db, out_sql, "sqlite", pq), 0)
            self.assertEqual(run_pass2(db, out_duck, "duck", pq), 0)
            l_sql = {r["wallet"]: r for r in read_rows(str(Path(out_sql) / "latency_shift_ranked.csv"))}
            l_duck = {r["wallet"]: r for r in read_rows(str(Path(out_duck) / "latency_shift_ranked.csv"))}
            self.assertEqual(set(l_sql), set(l_duck), "pass-2 wallet sets differ")
            ls_ok = True
            for w in l_sql:
                for col in LS_NUMERIC:
                    if not _num_eq(l_sql[w][col], l_duck[w][col]):
                        ls_ok = False
                        print(f"  LS DIVERGE {w}.{col}: {l_sql[w][col]} != {l_duck[w][col]}")
                if l_sql[w]["survives"] != l_duck[w]["survives"]:
                    ls_ok = False
            print(f"{'PASS' if ls_ok else 'FAIL'}: pass2_latency_shift_identical "
                  f"(wallets={len(l_sql)}, rtol=1e-9)")
            self.assertTrue(ls_ok, "pass-2 stats diverge beyond rtol 1e-9")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_duck_firstbuy_tie_is_atomic(self) -> None:
        """Two buys for the SAME (wallet, market) at the SAME timestamp but DIFFERENT
        outcome_id/price/contracts: the GROUP BY `arg_min(struct_pack(...))` dedup (#387) must
        return ONE source row's columns ATOMICALLY — never a frankenrow mixing columns across
        the tied rows (which a column-independent `arg_min` per column would risk). Either valid
        whole row passes (the pick is arbitrary on an exact tie, as in the SQLite path), so this
        is deterministic; a mixed row fails."""
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "tie.db")
            pq = str(Path(tmp) / "pq")
            t = ts(2026, 2, 10)
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE trades (wallet_hex TEXT, side TEXT, market_id TEXT, "
                         "outcome_id INTEGER, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER)")
            conn.execute("CREATE TABLE market_resolutions (market_id TEXT, winning_outcome_id INTEGER, "
                         "resolved_at_unix INTEGER)")
            conn.execute("CREATE TABLE market_schedules (market_id TEXT, end_date_unix INTEGER)")
            conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)", (W("a"), "buy", "M1", 0, "0.30", 100, t))
            conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?)", (W("a"), "buy", "M1", 1, "0.70", 200, t))
            conn.execute("INSERT INTO market_resolutions VALUES (?,?,?)", ("M1", 1, t + 7200))
            conn.execute("INSERT INTO market_schedules VALUES (?,?)", ("M1", t + 3600))
            conn.commit()
            conn.close()
            export(db, pq)
            con = ranker_duck.get_engine(force="duck", parquet_dir=pq, max_age_hours=0)
            df = ranker_duck.duck_extract_positions(
                con, [W("a")], ts(2026, 1, 1), ts(2026, 4, 1), 30, 259200, True, 0.0, 1.0)
            self.assertEqual(len(df), 1, "expected exactly one first-buy position")
            row = df.iloc[0]
            got = (int(row["outcome_id"]), round(float(row["price"]), 9), int(row["contracts"]))
            valid = {(0, 0.30, 100), (1, 0.70, 200)}
            print(f"{'PASS' if got in valid else 'FAIL'}: duck_firstbuy_tie_atomic (picked {got})")
            self.assertIn(got, valid, f"frankenrow {got} mixes columns across tied rows")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_bad_threads_env_falls_back(self) -> None:
        """A non-integer PE_RANKER_DUCKDB_THREADS (operator typo) must NOT crash get_engine —
        it falls back to the default thread cap, since this runs before any SQLite fallback
        (#387)."""
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "c.db")
            pq = str(Path(tmp) / "pq")
            build_parity_cache(db)
            export(db, pq)
            with mock.patch.dict(os.environ, {"PE_RANKER_DUCKDB_THREADS": "auto"}):
                con = ranker_duck.get_engine(force="duck", parquet_dir=pq, max_age_hours=0)
            print(f"{'PASS' if con is not None else 'FAIL'}: bad_threads_env_falls_back")
            self.assertIsNotNone(con, "get_engine crashed on non-integer PE_RANKER_DUCKDB_THREADS")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_autodetect_falls_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            empty = str(Path(tmp) / "no_parquet")
            os.makedirs(empty, exist_ok=True)
            # Missing snapshot -> auto returns None (SQLite fallback).
            miss = ranker_duck.get_engine(force="auto", parquet_dir=empty, max_age_hours=4)
            # Stale snapshot -> auto returns None even though files exist.
            db = str(Path(tmp) / "c.db")
            pq = str(Path(tmp) / "pq")
            build_parity_cache(db)
            export(db, pq)
            old = 0.0  # 1970 -> definitely older than 4h
            for n in ranker_duck.REQUIRED_PARQUET:
                os.utime(str(Path(pq) / n), (old, old))
            stale = ranker_duck.get_engine(force="auto", parquet_dir=pq, max_age_hours=4)
            # Forced sqlite -> None regardless.
            forced = ranker_duck.get_engine(force="sqlite", parquet_dir=pq, max_age_hours=0)
            ok = miss is None and stale is None and forced is None
            print(f"{'PASS' if ok else 'FAIL'}: autodetect_falls_back "
                  f"(missing={miss is None}, stale={stale is None}, forced_sqlite={forced is None})")
            self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main(verbosity=2)
