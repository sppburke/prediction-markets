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
  * (pass-2 lost its DuckDB path at the #536 cutover — it reads the SQLite ranker
    price store only, so no pass-2 parity claim exists to test)
  * asserts `get_engine` auto-detect falls back to SQLite on a missing/stale snapshot.

Fixture edge cases (each must be handled identically by both engines): first-buy
dedup (a later same-market buy is ignored), a sell (side filter), out-of-window,
out-of-band, unresolved market, a non-numeric `price_str`, and an exact
`price_str="1.0"` (the open-interval `0<price<1` boundary). Timestamps within a
(wallet,market) and within a (market,outcome) tape are DISTINCT except in the
dedicated equal-timestamp tie test: tape order is total via the (timestamp_unix,
source_trade_id) secondary key (#530), so exact ties resolve identically across
engines and repeated runs — no residual.

Deterministic: fixed window + as-of + timestamps; no clock, no RNG, no network.
Requires duckdb (skips with a clear message if unavailable).

Run: `python3 scripts/test_ranker_duck_parity.py`
  or: `pytest scripts/test_ranker_duck_parity.py -v`
"""
from __future__ import annotations

import csv
import math
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_trades_parquet as exp  # noqa: E402
import rank_72hr_buyandhold as rk  # noqa: E402
from collections import defaultdict  # noqa: E402
from types import SimpleNamespace  # noqa: E402
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


def ts(y: int, m: int, d: int, hh: int = 12) -> int:
    return int(datetime(y, m, d, hh, 0, 0, tzinfo=timezone.utc).timestamp())


def W(c: str) -> str:
    return "0x" + c * 40


_TID = iter(range(1, 10_000))


def tid() -> str:
    """Unique source_trade_id per fixture row (the real cache's PRIMARY KEY), in the
    production shape `0x` + 64 lowercase hex — the duck slice key asserts that format."""
    return f"0x{next(_TID):064x}"


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
                 "outcome_id INTEGER, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER, "
                 "source_trade_id TEXT PRIMARY KEY NOT NULL)")
    conn.execute("CREATE INDEX idx_trades_wallet_ts ON trades(wallet_hex, timestamp_unix)")
    conn.execute("CREATE TABLE market_resolutions (market_id TEXT, winning_outcome_id INTEGER, "
                 "resolved_at_unix INTEGER)")
    conn.execute("CREATE TABLE market_schedules (market_id TEXT, end_date_unix INTEGER)")

    markets: dict[str, tuple[int, int]] = {}
    for w, mid, oid, px, t, win in _QUALIFYING + _NONQUALIFYING:
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
                     (w, "buy", mid, oid, px, 100, t, tid()))
        markets.setdefault(mid, (win, t + 3600))
    # A sell that the side='buy' filter must ignore.
    conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
                 (W("a"), "sell", "MA1", 1, "0.55", 100, ts(2026, 2, 6), tid()))

    # Pass-2 tapes: a same-(market,outcome) fill ~25s after each qualifying entry, plus a
    # decoy earlier/later trade so bisect has to find the right index. DISTINCT timestamps.
    for w, mid, oid, px, t, win in _QUALIFYING:
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
                     (w, "buy", mid, oid, f"{float(px) + 0.01:.3f}", 50, t + 25, tid()))  # the fill
        conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
                     (W("z"), "buy", mid, oid, f"{float(px) + 0.02:.3f}", 50, t + 200, tid()))  # later decoy

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


class SnapshotQuarantineTest(unittest.TestCase):
    """#608: the Parquet snapshot must carry its own completeness evidence.

    A wallet partial when the snapshot was exported and completed afterwards
    passes a live marker check, but DuckDB still reads its partial rows, so
    ranking it would use history the quarantine was meant to withhold.
    """

    def _cache_with_marker(self, db: str, partial: str | None) -> None:
        build_parity_cache(db)
        with sqlite3.connect(db) as conn:
            conn.execute("CREATE TABLE wallets (wallet_hex TEXT PRIMARY KEY, "
                         "backfill_partial INTEGER NOT NULL DEFAULT 0, "
                         "is_active INTEGER DEFAULT 1, is_infra INTEGER DEFAULT 0)")
            for wallet in {W("a"), W("b")}:
                conn.execute("INSERT INTO wallets(wallet_hex, backfill_partial) VALUES (?,?)",
                             (wallet, 1 if wallet == partial else 0))

    def _ranked(self, tmp: str, db: str, pq: str, tag: str) -> list[str]:
        out = str(Path(tmp) / tag)
        self.assertEqual(run_pass1(db, out, "duck", pq), 0)
        return [r["wallet"] for r in read_rows(str(Path(out) / "ranked_72hr_buyandhold.csv"))]

    def test_recovered_wallet_is_not_ranked_from_a_stale_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "cache.db"), str(Path(tmp) / "parquet")
            self._cache_with_marker(db, partial=W("a"))
            export(db, pq)  # snapshot taken while 0xaaa.. was still partial
            self.assertTrue(os.path.exists(os.path.join(pq, "wallet_completeness.parquet")))
            # Backfill completes; the next export fails, so the snapshot is unchanged.
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE wallets SET backfill_partial = 0")
            stale = self._ranked(tmp, db, pq, "stale")
            self.assertNotIn(W("a"), stale,
                             "ranked a wallet from history that was partial when exported")
            # Re-exporting is what makes it usable again.
            export(db, pq)
            fresh = self._ranked(tmp, db, pq, "fresh")
            self.assertIn(W("a"), fresh, "a re-exported complete wallet must rank again")

    def test_interrupted_export_is_refused_not_trusted(self) -> None:
        """trades.parquet and wallet_completeness.parquet are replaced one at a
        time. An export that dies between them leaves new trades paired with an
        older marker, which would certify history the quarantine withheld.
        """
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "cache.db"), str(Path(tmp) / "parquet")
            self._cache_with_marker(db, partial=W("a"))
            export(db, pq)
            manifest = Path(pq) / "schema_v1_export_manifest.json"
            self.assertTrue(manifest.exists(), "export must bind the pair")

            # A later export replaces trades and then dies before the manifest.
            second = str(Path(tmp) / "parquet2")
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE wallets SET backfill_partial = 0")
            export(db, second)
            shutil.copy2(os.path.join(second, "trades.parquet"),
                         os.path.join(pq, "trades.parquet"))

            with self.assertRaises(FileNotFoundError):
                ranker_duck.snapshot_partial_wallets(pq)
            # Forced duck must fail closed rather than rank the mismatched pair.
            with self.assertRaises(FileNotFoundError):
                self._ranked(tmp, db, pq, "interrupted")
            # A complete re-export repairs the binding.
            export(db, pq)
            self.assertEqual(ranker_duck.snapshot_partial_wallets(pq), set())

    def test_completion_during_the_export_is_not_bound(self) -> None:
        """A wallet completing between the trade export and the completeness
        export would pair partial trades with a "complete" marker. DuckDB cannot
        snapshot an attached SQLite database, so the exporter must detect the
        change and publish no binding.
        """
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "cache.db"), str(Path(tmp) / "parquet")
            self._cache_with_marker(db, partial=W("a"))

            real = exp._export_table

            def complete_mid_export(con, out_dir, tbl, row_group_size):
                """Let the backfill finish right after the trades are written."""
                n = real(con, out_dir, tbl, row_group_size)
                if tbl == "trades":
                    with sqlite3.connect(db) as conn:
                        conn.execute("UPDATE wallets SET backfill_partial = 0")
                return n

            with mock.patch.object(exp, "_export_table", complete_mid_export):
                export(db, pq)
            self.assertFalse(
                (Path(pq) / "schema_v1_export_manifest.json").exists(),
                "bound a snapshot whose trades and markers describe different states")
            with self.assertRaises(FileNotFoundError):
                ranker_duck.snapshot_partial_wallets(pq)
            # A quiet re-export binds normally.
            export(db, pq)
            self.assertTrue((Path(pq) / "schema_v1_export_manifest.json").exists())

    def test_snapshot_exclusion_precedes_the_wallet_limit(self) -> None:
        """The approved plan requires load -> exclude -> slice. Truncating first
        would let --limit-wallets spend its slots on wallets the snapshot filter
        is about to drop, silently shrinking the ranked set.
        """
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "cache.db"), str(Path(tmp) / "parquet")
            self._cache_with_marker(db, partial=W("a"))
            export(db, pq)
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE wallets SET backfill_partial = 0")
            out = str(Path(tmp) / "limited")
            argv = ["rank_72hr_buyandhold.py", "--db", db, "--out-dir", out,
                    "--universe-from-trades", "--limit-wallets", "1",
                    "--win-start", WIN_START_ISO, "--win-end", WIN_END_ISO,
                    "--as-of", AS_OF_ISO, "--min-avg-per-month", "1",
                    "--min-active-months", "2", "--target-n", "5",
                    "--floor-tstat", "0.0", "--scheduled-only"]
            env = {"PE_RANKER_ENGINE": "duck", "PE_RANKER_PARQUET_DIR": pq,
                   "PE_RANKER_PARQUET_MAX_AGE_HOURS": "0"}
            with mock.patch.dict(os.environ, env), mock.patch.object(sys, "argv", argv):
                self.assertEqual(rk.main(), 0)
            ranked = [r["wallet"] for r in
                      read_rows(str(Path(out) / "ranked_72hr_buyandhold.csv"))]
            self.assertNotIn(W("a"), ranked)
            self.assertEqual(len(ranked), 1,
                             "the limit must be spent on a wallet that survives exclusion")

    def test_snapshot_without_evidence_falls_back_instead_of_ranking_blind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "cache.db"), str(Path(tmp) / "parquet")
            self._cache_with_marker(db, partial=None)
            export(db, pq)
            os.remove(os.path.join(pq, "wallet_completeness.parquet"))
            with self.assertRaises(FileNotFoundError):
                # Forced duck must fail closed rather than silently rank.
                self._ranked(tmp, db, pq, "forced")
            # A cache with no quarantine at all needs no evidence.
            plain = str(Path(tmp) / "plain.db")
            build_parity_cache(plain)
            pq2 = str(Path(tmp) / "parquet2")
            export(plain, pq2)
            self.assertFalse(os.path.exists(os.path.join(pq2, "wallet_completeness.parquet")))
            self.assertEqual(run_pass1(plain, str(Path(tmp) / "plainout"), "duck", pq2), 0)


class DuckParityTest(unittest.TestCase):
    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_schema_two_verified_projection_and_engine_refusal(self) -> None:
        """PASS: DuckDB 1.3.2 reads the joined exact schema-two projection;
        auto selects it, while forced SQLite is a typed refusal."""
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "v2.db")
            pq = str(Path(tmp) / "pq")
            conn = sqlite3.connect(db)
            conn.execute("PRAGMA user_version=2")
            conn.execute(
                "CREATE TABLE ranker_entries_v2 (source_trade_id TEXT PRIMARY KEY, "
                "activity_generation INTEGER, classifier_version INTEGER)"
            )
            conn.execute(
                "CREATE TABLE activity_groups_v2 (source_trade_id TEXT PRIMARY KEY, "
                "coverage_generation INTEGER, wallet_hex TEXT, condition_id TEXT, asset TEXT, "
                "outcome_id INTEGER, side TEXT, share_amount_str TEXT, "
                "price_weighted_share_amount_str TEXT, source_usdc_amount_str TEXT, "
                "source_time_unix INTEGER)"
            )
            conn.execute(
                "CREATE TABLE clob_payout_evidence_v2 (market_id TEXT PRIMARY KEY, "
                "payout_vector_json TEXT, end_date_unix INTEGER, payout_status TEXT)"
            )
            conn.execute(
                "CREATE TABLE activity_coverage_manifests_v2 (generation INTEGER PRIMARY KEY, "
                "reference_sha256 TEXT, wallet_count INTEGER, receipt_set_digest TEXT, "
                "aggregate_digest TEXT, source_row_count INTEGER)"
            )
            conn.execute(
                "CREATE TABLE clob_payout_coverage_manifests_v2 "
                "(generation INTEGER PRIMARY KEY, terminal_kind TEXT)"
            )
            conn.execute(
                "CREATE TABLE cache_v2_migration_state (singleton INTEGER PRIMARY KEY, "
                "phase TEXT, ranker_projection_count INTEGER, ranker_projection_digest TEXT, "
                "ranker_classifier_version INTEGER)"
            )
            gid = "g2:" + "a" * 64
            entry = ts(2026, 2, 5)
            conn.execute("INSERT INTO ranker_entries_v2 VALUES (?,?,?)", (gid, 7, 1))
            conn.execute(
                "INSERT INTO activity_groups_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (gid, 7, W("a"), "condition", "token", 1, "buy", "1.250000",
                 "0.500000000000", "0.490000", entry),
            )
            conn.execute(
                "INSERT INTO clob_payout_evidence_v2 VALUES (?,?,?,?)",
                ("condition", '[\"0.5\",\"0.5\"]', entry + 3600, "resolved"),
            )
            conn.execute("INSERT INTO activity_coverage_manifests_v2 VALUES (?,?,?,?,?,?)",
                         (7, "b" * 64, 1, "c" * 64, "d" * 64, 1))
            conn.execute("INSERT INTO clob_payout_coverage_manifests_v2 VALUES (?,?)",
                         (8, "end_cursor"))
            rows = [{
                "source_trade_id": gid, "activity_generation": 7,
                "classifier_version": 1, "wallet_hex": W("a"),
                "condition_id": "condition", "asset": "token", "outcome_id": 1,
                "side": "buy", "share_amount_str": "1.250000",
                "price_weighted_share_amount_str": "0.500000000000",
                "source_usdc_amount_str": "0.490000", "source_time_unix": entry,
                "payout_vector_json": '[\"0.5\",\"0.5\"]',
                "end_date_unix": entry + 3600,
            }]
            conn.execute("INSERT INTO cache_v2_migration_state VALUES (?,?,?,?,?)",
                         (1, "finalized", 1, exp._projection_digest(rows), 1))
            conn.commit()
            conn.close()

            export(db, pq)
            with self.assertRaises(ranker_duck.SchemaTwoEngineError):
                ranker_duck.get_engine(force="sqlite", parquet_dir=pq, schema_version=2)
            engine = ranker_duck.get_engine(
                force="auto", parquet_dir=pq, max_age_hours=0, schema_version=2
            )
            frame = ranker_duck.duck_extract_positions_v2(
                engine, [W("a")], entry - 1, entry + 1
            )
            self.assertEqual(len(frame), 1)
            self.assertEqual(str(frame.iloc[0]["contracts"]), "1.250000")
            self.assertAlmostEqual(float(frame.iloc[0]["price"]), 0.4)
            self.assertAlmostEqual(float(frame.iloc[0]["payoff"]), 0.5)
            print("PASS: schema-two projection verified; SQLite refused; half payout exact")

    def test_v2_repaired_payouts_do_not_change_v1_ranking_or_survivors(self) -> None:
        """#544 boundary: stored repaired payouts are replay evidence only.

        The frozen v1 fixture is ranked, then every market receives a v2 payout
        vector opposite its legacy winner (plus one fifty-fifty row). The v1
        qualifying positions, metrics, survivor verdicts, and output basket must
        remain byte-identical; #545 alone may switch the economic reader.
        """
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            base = str(Path(tmp) / "base")
            repaired = str(Path(tmp) / "repaired")
            pq = str(Path(tmp) / "unused")
            build_parity_cache(db)
            self.assertEqual(run_pass1(db, base, "sqlite", pq), 0)

            conn = sqlite3.connect(db)
            conn.execute(
                "CREATE TABLE clob_payout_evidence_v2 ("
                "market_id TEXT PRIMARY KEY NOT NULL, "
                "is_50_50_outcome INTEGER NULL, payout_status TEXT NOT NULL, "
                "payout_vector_json TEXT NULL, closed INTEGER NULL, tokens_json TEXT NOT NULL, "
                "raw_page_sha256 TEXT NOT NULL, coverage_generation INTEGER NOT NULL, "
                "page_ordinal INTEGER NOT NULL, schema_version INTEGER NOT NULL, "
                "parser_version INTEGER NOT NULL, fetched_at_unix INTEGER NOT NULL, "
                "origin TEXT NOT NULL)"
            )
            resolutions = list(conn.execute(
                "SELECT market_id, winning_outcome_id FROM market_resolutions ORDER BY market_id"
            ))
            for index, (market_id, legacy_winner) in enumerate(resolutions):
                if index == 0:
                    is_fifty, vector = 1, '["0.5","0.5"]'
                else:
                    is_fifty = 0
                    vector = '["0","1"]' if int(legacy_winner) == 0 else '["1","0"]'
                conn.execute(
                    "INSERT INTO clob_payout_evidence_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (market_id, is_fifty, "resolved", vector, 1, "[]", "a" * 64,
                     1, 0, 2, 2, 1_788_266_850, "clob_closed_walk_v2"),
                )
            conn.commit()
            conn.close()

            self.assertEqual(run_pass1(db, repaired, "sqlite", pq), 0)
            outputs = (
                "qualifying_positions_72hr.csv",
                "ranked_72hr_buyandhold.csv",
                "ranked_72hr_buyandhold.txt",
                "250_72hr_buyandhold_variance.txt",
            )
            for name in outputs:
                before = (Path(base) / name).read_bytes()
                after = (Path(repaired) / name).read_bytes()
                self.assertEqual(before, after, f"v2 payout evidence changed {name}")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_pass1_parity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cache.db")
            pq = str(Path(tmp) / "parquet")
            out_sql = str(Path(tmp) / "sql")
            out_duck = str(Path(tmp) / "duck")
            build_parity_cache(db)

            self.assertEqual(run_pass1(db, out_sql, "sqlite", pq), 0)
            export(db, pq)
            self.assertEqual(run_pass1(db, out_duck, "duck", pq), 0)

            # AC (#530): the exported snapshot carries source_trade_id with zero NULLs
            # — the tie key must exist and be total in the DuckDB path's real input.
            import duckdb as _d
            nn = _d.connect().execute(
                f"SELECT count(*) FILTER (WHERE source_trade_id IS NULL), count(*) "
                f"FROM read_parquet('{pq}/trades.parquet')").fetchone()
            self.assertEqual(nn[0], 0, "NULL source_trade_id in exported snapshot")
            self.assertGreater(nn[1], 0)

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

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_duck_firstbuy_tie_is_atomic(self) -> None:
        """Two buys for the SAME (wallet, market) at the SAME timestamp but DIFFERENT
        outcome_id/price/contracts: the GROUP BY `arg_min(struct_pack(...))` dedup (#387) must
        return ONE source row's columns ATOMICALLY — never a frankenrow mixing columns across
        the tied rows. #530 Phase C: the tie now resolves DETERMINISTICALLY to the row with
        the smaller `source_trade_id` (the (ts, id-slices) arg_min key), so exactly one
        specific row passes — the previously-accepted "either row" is a regression."""
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "tie.db")
            pq = str(Path(tmp) / "pq")
            t = ts(2026, 2, 10)
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE trades (wallet_hex TEXT, side TEXT, market_id TEXT, "
                         "outcome_id INTEGER, price_str TEXT, contracts INTEGER, timestamp_unix INTEGER, "
                         "source_trade_id TEXT PRIMARY KEY NOT NULL)")
            conn.execute("CREATE TABLE market_resolutions (market_id TEXT, winning_outcome_id INTEGER, "
                         "resolved_at_unix INTEGER)")
            conn.execute("CREATE TABLE market_schedules (market_id TEXT, end_date_unix INTEGER)")
            conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)", (W("a"), "buy", "M1", 0, "0.30", 100, t, tid()))
            conn.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)", (W("a"), "buy", "M1", 1, "0.70", 200, t, tid()))
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
            # First-inserted row has the smaller sequential source_trade_id -> it wins.
            print(f"{'PASS' if got == (0, 0.30, 100) else 'FAIL'}: duck_firstbuy_tie_atomic (picked {got})")
            self.assertEqual(got, (0, 0.30, 100),
                             f"tie must resolve to the min-source_trade_id row, got {got}")

    def test_tie_deterministic_across_engines_and_insertion_order(self) -> None:
        """#530 Phase C acceptance: equal-timestamp first-buy ties resolve to the row with
        the smaller source_trade_id in BOTH engines, regardless of physical insertion order.
        Two dbs carry the same two tied rows inserted in opposite orders; the SQLite scan
        (rank_72hr_buyandhold.scan_and_filter_sqlite) and the DuckDB extraction must all
        pick the identical row — eliminating the measured ±11-wallet churn class.

        The id pairs differ at BOUNDARY-SENSITIVE hex digits: the last digit of each of
        the duck key's four 16-digit slices plus the first digit of the last slice. A
        slice offset/length typo (e.g. 35 -> 34 in ranker_duck's fb0 key) leaves some
        digit uncovered, turning exactly one of these pairs insertion-order-dependent —
        so this test regression-pins the slice arithmetic, not just h4."""
        t = ts(2026, 2, 10)

        def pair_at(hex_idx: int) -> tuple[str, str]:
            """Two production-shape ids equal everywhere except hex digit `hex_idx`
            (0-based within the 64 digits after '0x'): lo has '1' there, hi has '2'."""
            base = ["0"] * 64
            base[hex_idx] = "1"
            lo = "0x" + "".join(base)
            base[hex_idx] = "2"
            return lo, "0x" + "".join(base)

        # h1 last digit, h2 last, h3 last, h4 first, h4 last.
        boundary_digits = (15, 31, 47, 48, 63)
        want = (1, "0.70", 200)  # the min-source_trade_id row carries the 0.70 side

        for hex_idx in boundary_digits:
            tid_lo, tid_hi = pair_at(hex_idx)
            rows = [(tid_hi, 0, "0.30", 100), (tid_lo, 1, "0.70", 200)]
            self._assert_tie_pick(t, rows, want, f"digit{hex_idx}")

    def _assert_tie_pick(self, t, rows, want, label) -> None:
        for order_name, insert_rows in ((f"{label}/forward", rows),
                                        (f"{label}/reversed", rows[::-1])):
            with tempfile.TemporaryDirectory() as tmp:
                db = str(Path(tmp) / "tie.db")
                pq = str(Path(tmp) / "pq")
                conn = sqlite3.connect(db)
                conn.execute("CREATE TABLE trades (wallet_hex TEXT, side TEXT, market_id TEXT, "
                             "outcome_id INTEGER, price_str TEXT, contracts INTEGER, "
                             "timestamp_unix INTEGER, source_trade_id TEXT PRIMARY KEY NOT NULL)")
                conn.execute("CREATE TABLE market_resolutions (market_id TEXT, "
                             "winning_outcome_id INTEGER, resolved_at_unix INTEGER)")
                conn.execute("CREATE TABLE market_schedules (market_id TEXT, end_date_unix INTEGER)")
                for stid, oid, price, qty in insert_rows:
                    conn.execute(
                        "INSERT INTO trades (wallet_hex, side, market_id, outcome_id, price_str, "
                        "contracts, timestamp_unix, source_trade_id) VALUES (?,?,?,?,?,?,?,?)",
                        (W("a"), "buy", "M1", oid, price, qty, t, stid))
                conn.execute("INSERT INTO market_resolutions VALUES (?,?,?)", ("M1", 1, t + 7200))
                conn.execute("INSERT INTO market_schedules VALUES (?,?)", ("M1", t + 3600))
                conn.commit()

                # SQLite engine pick — a namespace carrying exactly the fields
                # scan_and_filter_sqlite reads (the real Params has unrelated CLI fields).
                prm = SimpleNamespace(win_start=ts(2026, 1, 1), win_end=ts(2026, 4, 1),
                                      scheduled_only=True, min_ttr_secs=30,
                                      ttr_secs=259200, price_min=0.0, price_max=1.0)
                res = {"M1": (1, t + 7200)}
                sched = {"M1": t + 3600}
                diag = defaultdict(int)
                pos = rk.scan_and_filter_sqlite(conn, W("a"), prm, res, sched, diag)
                conn.close()
                self.assertEqual(len(pos), 1)
                sq = (int(pos[0]["outcome_id"]), f'{pos[0]["price"]:.2f}', int(pos[0]["contracts"]))

                # DuckDB engine pick.
                export(db, pq)
                con = ranker_duck.get_engine(force="duck", parquet_dir=pq, max_age_hours=0)
                df = ranker_duck.duck_extract_positions(
                    con, [W("a")], ts(2026, 1, 1), ts(2026, 4, 1), 30, 259200, True, 0.0, 1.0)
                self.assertEqual(len(df), 1)
                r = df.iloc[0]
                dk = (int(r["outcome_id"]), f'{float(r["price"]):.2f}', int(r["contracts"]))

                self.assertEqual(sq, want, f"[{order_name}] SQLite pick {sq} != {want}")
                self.assertEqual(dk, want, f"[{order_name}] DuckDB pick {dk} != {want}")
                print(f"PASS: tie deterministic [{order_name}]: both engines picked {want}")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_slice_key_premise_rejects_malformed_ids(self) -> None:
        """The duck extract must halt with RuntimeError BEFORE running the slice-key query
        when any source_trade_id is not `0x` + 64 lowercase hex — pinning the fail-closed
        guard for all three malformation classes, including short-but-hex (which would
        CAST silently but order differently from the SQLite engine's TEXT comparison)."""
        import duckdb as _d

        for label, bad_id in (("short-hex", "0xabc"),
                              ("uppercase", "0x" + "A" * 64),
                              ("non-hex", "t-lo")):
            con = _d.connect()
            con.execute("CREATE TABLE trades(wallet_hex VARCHAR, side VARCHAR, "
                        "market_id VARCHAR, outcome_id BIGINT, price_str VARCHAR, "
                        "contracts BIGINT, timestamp_unix BIGINT, "
                        "source_trade_id VARCHAR NOT NULL)")
            con.execute("INSERT INTO trades VALUES ('0xa','buy','M1',1,'0.50',10,1000,?)",
                        [bad_id])
            with self.assertRaisesRegex(RuntimeError, "premise violated",
                                        msg=f"{label} id must halt the extract"):
                ranker_duck.duck_extract_positions(
                    con, ["0xa"], 0, 2_000_000_000, 30, 259200, True, 0.0, 1.0)
            con.close()
            print(f"PASS: slice-key premise rejects {label}")

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
