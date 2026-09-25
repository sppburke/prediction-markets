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
import hashlib
import json
import math
import os
import sqlite3
import shutil
import sys
import tempfile
import time
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


def run_pass1(db: str, out_dir: str, engine: str, parquet_dir: str, max_age_hours: str = "0") -> int:
    argv = ["rank_72hr_buyandhold.py", "--db", db, "--out-dir", out_dir,
            "--universe-from-trades",
            "--win-start", WIN_START_ISO, "--win-end", WIN_END_ISO, "--as-of", AS_OF_ISO,
            "--min-avg-per-month", "1", "--min-active-months", "2",
            "--target-n", "5", "--floor-tstat", "0.0", "--scheduled-only"]
    env = {"PE_RANKER_ENGINE": engine, "PE_RANKER_PARQUET_DIR": parquet_dir,
           "PE_RANKER_PARQUET_MAX_AGE_HOURS": max_age_hours}
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


def whole_projection_digest(rows: list[dict]) -> str:
    """Original whole-list implementation, retained only as a test reference."""
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


ESCAPED_ASSET = 'token"\n\\'


def payout_tokens(asset: str, outcome: int) -> str:
    """A binary market's payout token list with `asset` at index `outcome`."""
    return json.dumps([{"token_id": asset if index == outcome else f"{asset}-other"}
                       for index in range(2)])


def build_certified_cache(db: str) -> tuple[list[dict], int, str]:
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA user_version=2")
    conn.execute(
        "CREATE TABLE ranker_entries_v2 (source_trade_id TEXT PRIMARY KEY, "
        "activity_generation INTEGER, classifier_version INTEGER)"
    )
    conn.execute(
        "CREATE TABLE activity_groups_v2 (source_trade_id TEXT NOT NULL, "
        "coverage_generation INTEGER, wallet_hex TEXT, condition_id TEXT, asset TEXT, "
        "outcome_id INTEGER, side TEXT, share_amount_str TEXT, "
        "price_weighted_share_amount_str TEXT, source_usdc_amount_str TEXT, "
        "source_time_unix INTEGER)"
    )
    conn.execute("CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id "
                 "ON activity_groups_v2(source_trade_id COLLATE BINARY)")
    conn.execute(
        "CREATE TABLE clob_payout_evidence_v2 (market_id TEXT PRIMARY KEY, "
        "payout_vector_json TEXT, end_date_unix INTEGER, payout_status TEXT, tokens_json TEXT)"
    )
    conn.execute(
        "CREATE TABLE activity_coverage_manifests_v2 (generation INTEGER PRIMARY KEY, "
        "reference_sha256 TEXT, wallet_count INTEGER, receipt_set_digest TEXT, "
        "aggregate_digest TEXT, source_row_count INTEGER, cursors_json TEXT, page_hashes_json TEXT, "
        "completed_at_unix INTEGER)"
    )
    conn.execute(
        "CREATE TABLE clob_payout_coverage_manifests_v2 "
        "(generation INTEGER PRIMARY KEY, terminal_kind TEXT, completed_at_unix INTEGER, "
        "manifest_json TEXT, terminal_page_sha256 TEXT)"
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
        "INSERT INTO clob_payout_evidence_v2 VALUES (?,?,?,?,?)",
        ("condition", '[\"0.5\",\"0.5\"]', entry + 3600, "resolved",
         json.dumps([{"token_id": ESCAPED_ASSET}, {"token_id": "token"}])),
    )
    marker = json.dumps({"receipt_storage": "activity_wallet_coverage_staging_v2", "version": 1},
                        sort_keys=True, separators=(",", ":"))
    conn.execute("INSERT INTO activity_coverage_manifests_v2 VALUES (?,?,?,?,?,?,?,?,?)",
                 (7, "b" * 64, 1, "c" * 64, "d" * 64, 5, marker, "[]", entry))
    conn.execute("INSERT INTO clob_payout_coverage_manifests_v2 VALUES (?,?,?,?,?)",
                 (8, "end_cursor", entry, "{}", "e" * 64))
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
    # Reverse insertion order, exact decimal strings and escaping span
    # three batches on both SQLite and the exported Parquet relation.
    for ordinal in (4, 3, 2, 1):
        row = {**rows[0], "source_trade_id": f"g2:{ordinal:064x}",
               "wallet_hex": W("b"), "asset": ESCAPED_ASSET, "outcome_id": 0}
        rows.append(row)
        conn.execute("INSERT INTO ranker_entries_v2 VALUES (?,?,?)",
                     (row["source_trade_id"], 7, 1))
        conn.execute("INSERT INTO activity_groups_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (row["source_trade_id"], 7, W("b"), "condition", row["asset"], 0,
                      "buy", "1.250000", "0.500000000000", "0.490000", entry))
    rows.sort(key=lambda row: row["source_trade_id"])
    conn.execute("INSERT INTO cache_v2_migration_state VALUES (?,?,?,?,?)",
                 (1, "finalized", len(rows), whole_projection_digest(rows), 1))
    # Watermark-only columns and acquisition view; no receipt table is
    # needed by either the watermark or the Parquet exporter.
    conn.execute("CREATE VIEW active_tradeable_wallets AS SELECT DISTINCT wallet_hex FROM activity_groups_v2")
    conn.execute("ALTER TABLE activity_groups_v2 ADD COLUMN activity_type TEXT DEFAULT 'TRADE'")
    conn.execute("ALTER TABLE clob_payout_evidence_v2 ADD COLUMN coverage_generation INTEGER DEFAULT 8")
    conn.execute("ALTER TABLE clob_payout_evidence_v2 ADD COLUMN fetched_at_unix INTEGER DEFAULT 1")
    # Re-stamping changes only generation; retained excluded history is
    # outside the current head and must not enter any equality join.
    conn.execute("UPDATE activity_groups_v2 SET coverage_generation = 1")
    conn.execute("UPDATE activity_groups_v2 SET coverage_generation = 7")
    conn.execute("INSERT INTO activity_groups_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                 ("g2:" + "f" * 64, 1, W("f"), "condition", "token", 1, "buy",
                  "1.0", "0.5", "0.5", entry + 999, "TRADE"))
    conn.commit()
    conn.close()
    return rows, entry, marker


# Frozen manifest reader from 982f294, before projection-only exports.
def _version_one_manifest_reader(parquet_dir: str) -> dict:
    path = os.path.join(parquet_dir, ranker_duck.V2_EXPORT_MANIFEST)
    with open(path, encoding="utf-8") as source:
        value = json.load(source)
    if value.get("version") != 1 or set(value.get("tables", {})) != {
        name.removesuffix(".parquet") for name in ranker_duck.REQUIRED_V2_PARQUET
    }:
        raise ranker_duck.SchemaTwoEngineError("schema-two export manifest has an invalid shape")
    for name in ranker_duck.REQUIRED_V2_PARQUET:
        table = name.removesuffix(".parquet")
        expected = value["tables"][table].get("sha256")
        actual = ranker_duck._sha256_file(os.path.join(parquet_dir, name))
        if expected != actual:
            raise ranker_duck.SchemaTwoEngineError(
                f"schema-two Parquet hash mismatch for {name}"
            )
    return value



def _export_certified_cache(db: str, pq: str, expected_wallets: set[str] | None = None) -> dict:
    os.makedirs(pq, exist_ok=True)
    con = duckdb.connect(config={"autoinstall_known_extensions": "false"})
    try:
        con.execute("LOAD sqlite_scanner")
        con.execute(f"ATTACH '{exp._q(os.path.abspath(db))}' AS src (TYPE sqlite, READ_ONLY)")
        counts = {table: exp._export_table(con, pq, table, 100) for table in exp.V2_TABLES}
        projection = exp._verify_v2_projection(con, pq)
        exp._write_v2_export_manifest(pq, counts, projection)
        if expected_wallets is not None:
            # Retained excluded history remains audit data in activity_groups_v2;
            # neither the source nor exported ranker projection may admit it.
            with sqlite3.connect(db) as source:
                source_wallets = {wallet for (wallet,) in source.execute(
                    "SELECT g.wallet_hex FROM ranker_entries_v2 r JOIN activity_groups_v2 g "
                    "ON g.source_trade_id = r.source_trade_id "
                    "AND g.coverage_generation = r.activity_generation "
                    "JOIN clob_payout_evidence_v2 p ON p.market_id = g.condition_id")}
            exported_wallets = {row["wallet_hex"] for row in exp._projection_rows(con)}
            for prefix, wallets in (("src", source_wallets), ("exported", exported_wallets)):
                assert wallets == expected_wallets, (prefix, wallets, expected_wallets)
        return projection
    finally:
        con.close()


def assert_certified_export_wallets(db: str, expected_wallets: set[str]) -> None:
    with tempfile.TemporaryDirectory() as pq:
        _export_certified_cache(db, pq, expected_wallets)
        engine = ranker_duck.get_engine(force="duck", parquet_dir=pq, max_age_hours=0, schema_version=2)
        try:
            assert set(rk.load_universe_from_export(engine)) == expected_wallets
        finally:
            engine.close()


def assert_certified_full_incremental_equivalence(full: str, incremental: str) -> None:
    """Called by the Rust collector scenario with its two real certified caches.

    Exercise the existing SQLite, export, DuckDB, watermark and publication
    readers against precisely the same full-versus-delta dataset as Rust.
    """
    import pandas as pd
    import push_ranking_to_supabase as publisher
    import rank_cycle_manifest as cycle

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        for index, db in enumerate((full, incremental)):
            pq = str(Path(tmp) / str(index))
            projection = _export_certified_cache(db, pq)
            engine = ranker_duck.get_engine(force="duck", parquet_dir=pq, max_age_hours=0, schema_version=2)
            wallets = rk.load_universe_from_export(engine)
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as sqlite:
                all_wallets = [row[0] for row in sqlite.execute("SELECT DISTINCT wallet_hex FROM activity_groups_v2")]
                last = publisher._wallet_last_trade(sqlite, all_wallets)
                newest = publisher._newest_trade_unix(sqlite)
            positions = pd.concat(ranker_duck.duck_extract_positions_v2(engine, wallets, 0, 2**62),
                                  ignore_index=True)
            engine.close()
            watermark = cycle.snapshot(Path(db), "2027-01-15", {}, {})["source_watermark"]["activity"]
            results.append((wallets, last, newest, positions, projection, watermark))
        a, b = results
        assert a[:3] == b[:3], "wallet universe or publisher source times changed"
        pd.testing.assert_frame_equal(a[3], b[3], check_exact=True)
        assert a[4] == b[4], "exported projection certification changed"
        for field in ("generation", "count", "newest_source_unix", "wallet_count", "aggregate_digest", "source_row_count"):
            assert a[5][field] == b[5][field], field
        assert a[5]["reference_sha256"] != b[5]["reference_sha256"]
        assert a[5]["receipt_set_digest"] != b[5]["receipt_set_digest"]
        print("PASS: real full/delta caches have identical exported positions, wallet universe, publisher times and content watermarks")


class DuckParityTest(unittest.TestCase):
    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_certified_subset_matches_full_export_through_publication(self):
        import latency_shift_rerank as latency
        import push_ranking_to_supabase as publisher
        from test_latency_shift_ref_oracle import FIXTURE_DDL

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = str(root / "v2.db")
            rows, entry, _ = build_certified_cache(db)
            now = ts(2026, 4, 1)
            with sqlite3.connect(db) as conn:
                conn.executescript(FIXTURE_DDL)
                # Distinct markets, equal timestamps, fractional amounts, half
                # payouts, and one certified entry outside the analysis window.
                for ordinal, row in enumerate(rows):
                    row["condition_id"] = f"market-{ordinal}"
                    row["asset"] = f"token-{ordinal}"
                    row["source_time_unix"] = (
                        ts(2025, 6, 1) if ordinal == 0 else entry + (ordinal // 2) * 86400
                    )
                    row["end_date_unix"] = row["source_time_unix"] + 3600
                    row["price_weighted_share_amount_str"] = "0.125000000000" if ordinal == 1 else "0.500000000000"
                    if ordinal == 3:
                        # A wallet needs two evaluable entries to be ranked at all (#588).
                        row["wallet_hex"] = W("a")
                    conn.execute(
                        "UPDATE activity_groups_v2 SET condition_id=?, asset=?, source_time_unix=?, "
                        "price_weighted_share_amount_str=?, wallet_hex=? WHERE source_trade_id=?",
                        tuple(row[k] for k in ("condition_id", "asset", "source_time_unix",
                                              "price_weighted_share_amount_str", "wallet_hex",
                                              "source_trade_id")),
                    )
                    conn.execute("INSERT INTO clob_payout_evidence_v2 VALUES (?,?,?,?,?,?,?)",
                                 (row["condition_id"], row["payout_vector_json"], row["end_date_unix"],
                                  "resolved", payout_tokens(row["asset"], row["outcome_id"]), 8, now))
                    conn.execute("INSERT INTO token_conditions VALUES (?,?,?,?)",
                                 (row["asset"], row["condition_id"], now, row["outcome_id"]))
                    conn.execute("INSERT INTO ranker_price_pages VALUES (?,?,?,1,'complete',1,"
                                 "'00','test',1,1,1,1,'url')",
                                 (row["asset"], row["source_time_unix"] - 119, row["source_time_unix"] + 3))
                    conn.execute("INSERT INTO ranker_price_points VALUES (?,?,?,?)",
                                 (row["asset"], row["source_time_unix"] + 2,
                                  "0.20" if ordinal % 2 else "0.40", now))
                # Freshness MUST come from these nonprojected rows: a recent
                # SELL, an unresolved BUY, and excluded old-generation history.
                for ordinal, wallet, side, market, generation in (
                    (10, W("a"), "sell", "condition", 7),
                    (11, W("b"), "buy", "unresolved", 7),
                    (12, W("e"), "buy", "condition", 1),
                    (13, W("0"), "buy", "condition", 1),
                ):
                    conn.execute("INSERT INTO activity_groups_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                 (f"g2:{ordinal:064x}", generation, wallet, market, None, None,
                                  side, "1.0", "0.5", "0.5", now - ordinal, "TRADE"))
                conn.execute("UPDATE cache_v2_migration_state SET ranker_projection_digest=?",
                             (whole_projection_digest(rows),))
                conn.execute("UPDATE clob_payout_coverage_manifests_v2 SET completed_at_unix=?", (now,))
                conn.execute("ALTER TABLE activity_coverage_manifests_v2 ADD COLUMN group_count INTEGER")
                conn.execute("UPDATE activity_coverage_manifests_v2 SET group_count=7")

            # Authentic version-one reference: SELECT * for every table, with
            # the original manifest shape. Never use the narrowed export helper.
            full, subset = root / "full", root / "subset"
            full.mkdir()
            con = duckdb.connect()
            con.execute("LOAD sqlite_scanner")
            con.execute(f"ATTACH '{exp._q(db)}' AS src (TYPE sqlite, READ_ONLY)")
            tables = {}
            for table in exp.V2_TABLES:
                path = full / f"{table}.parquet"
                con.execute(f"COPY (SELECT * FROM src.{table}) TO '{exp._q(str(path))}' (FORMAT PARQUET)")
                tables[table] = {"count": con.execute(f"SELECT COUNT(*) FROM src.{table}").fetchone()[0],
                                 "sha256": exp._file_sha256(str(path))}
            (full / exp.V2_EXPORT_MANIFEST).write_text(json.dumps({
                "version": 1, "tables": tables,
                "projection": {"count": len(rows), "digest": whole_projection_digest(rows),
                               "classifier_version": 1},
            }))
            self.assertEqual(_version_one_manifest_reader(str(full))["version"], 1)
            export(db, str(subset))
            with self.assertRaisesRegex(ranker_duck.SchemaTwoEngineError, "invalid shape"):
                _version_one_manifest_reader(str(subset))
            manifest = ranker_duck._load_v2_export_manifest(str(subset))
            self.assertEqual(manifest["version"], 2)
            self.assertEqual(manifest["activity_scope"], "certified_ranker_entries")
            self.assertEqual(manifest["projection"]["activity_generation"], 7)
            self.assertEqual(manifest["tables"]["activity_groups_v2"]["count"], len(rows))
            # The actual pinned sqlite_scanner delegates this plan to SQLite:
            # projection scan -> activity primary-key lookups, no activity scan.
            plan = con.execute("SELECT * FROM sqlite_query('src', ?)",
                               ["EXPLAIN QUERY PLAN " + exp.CERTIFIED_ACTIVITY_SQL]).fetchall()
            details = [r[3] for r in plan]
            self.assertTrue(any("SCAN r" in d for d in details), details)
            self.assertTrue(any("SEARCH g USING INDEX idx_activity_groups_v2_source_trade_id" in d
                                and "source_trade_id=?" in d for d in details), details)
            self.assertFalse(any("SCAN g" in d for d in details), details)
            con.execute("CREATE VIEW reduced AS SELECT * FROM read_parquet("
                        f"'{exp._q(str(subset / 'activity_groups_v2.parquet'))}')")
            expected = con.execute(exp.CERTIFIED_ACTIVITY_SQL.replace("ranker_entries_v2", "src.ranker_entries_v2")
                                   .replace("activity_groups_v2", "src.activity_groups_v2")
                                   + " ORDER BY g.source_trade_id").fetchall()
            self.assertEqual(con.execute("SELECT * FROM reduced ORDER BY source_trade_id").fetchall(), expected)
            self.assertEqual(con.execute("DESCRIBE reduced").fetchall(),
                             con.execute(f"DESCRIBE SELECT * FROM read_parquet('{exp._q(str(full / 'activity_groups_v2.parquet'))}')").fetchall())
            for table in exp.V2_TABLES:
                if table != "activity_groups_v2":
                    self.assertEqual(con.execute("SELECT * FROM read_parquet(?)", [str(full / f"{table}.parquet")]).fetchall(),
                                     con.execute("SELECT * FROM read_parquet(?)", [str(subset / f"{table}.parquet")]).fetchall())
            con.close()

            for name, value in (("before.json", []), ("cycle.json", {"cache_schema": 2}),
                                ("stage.json", {"cache_sha256": "aa"}),
                                ("versions.json", {"source": "polymarket-public-activity", "activity_schema": 2,
                                                   "activity_parser": 2, "clob_resolution_schema": 2,
                                                   "clob_resolution_parser": 2, "cache_schema": 2, "configuration": 1})):
                (root / name).write_text(json.dumps(value))
            results = []
            for pq in (full, subset):
                out = root / f"{pq.name}-rank"
                self.assertEqual(run_pass1(db, str(out), "duck", str(pq)), 0)
                argv = ["latency", "--db", db, "--ranked-csv", str(out / "ranked_72hr_buyandhold.csv"),
                        "--positions-csv", str(out / "qualifying_positions_72hr.csv"), "--out-dir", str(out),
                        "--as-of", AS_OF_ISO, "--half-life-days", "0", "--min-trl", "0",
                        "--min-active-months", "0", "--min-avg-per-month", "0", "--floor-tstat", "0",
                        "--before-ranking-json", str(root / "before.json"),
                        "--cycle-manifest-file", str(root / "cycle.json"),
                        "--cache-stage-record", str(root / "stage.json"),
                        "--pipeline-versions-file", str(root / "versions.json")]
                with mock.patch.object(sys, "argv", argv + ["--emit-targets", str(out / "targets.csv")]):
                    self.assertEqual(latency.main(), 0)
                with mock.patch.object(sys, "argv", argv):
                    self.assertEqual(latency.main(), 0)
                with mock.patch.object(sys, "argv", ["push", "--ranked-csv", str(out / "latency_shift_ranked.csv"),
                                                     "--db", db, "--manifest-file", str(out / "oracle_manifest.json")]):
                    args = publisher.build_parser().parse_args()
                with mock.patch.object(publisher, "_request_once") as network:
                    prepared = publisher.prepare_publish_request(args, now)
                network.assert_not_called()
                results.append((out, prepared["entries"]))
            self.assertEqual(results[0][1], results[1][1])
            self.assertTrue(results[0][1])
            for filename in ("qualifying_positions_72hr.csv", "ranked_72hr_buyandhold.csv", "targets.csv",
                             "oracle_outcomes.csv", "latency_shift_ranked.csv", "before_after_diff.json"):
                self.assertEqual((results[0][0] / filename).read_bytes(),
                                 (results[1][0] / filename).read_bytes(), filename)
            # Both wallets remain publishable solely because full SQLite
            # history retained their recent nonprojected trades.
            self.assertEqual({r["wallet_hex"]: r["last_trade_unix"] for r in results[0][1]},
                             {W("a"): now - 10, W("b"): now - 11})
            with sqlite3.connect(db) as conn:
                self.assertEqual(publisher._newest_trade_unix(conn), now - 10)
            # The universe is the certified projection's wallets under either
            # export shape; full history's uncertified wallets, even one that
            # sorts first, never enter it.
            for snapshot in (full, subset):
                engine = ranker_duck.get_engine(force="duck", parquet_dir=str(snapshot),
                                                max_age_hours=0, schema_version=2)
                try:
                    self.assertEqual(rk.load_universe_from_export(engine), [W("a"), W("b")])
                finally:
                    engine.close()

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_schema_two_pass_one_checks_the_snapshot_before_building_its_universe(self):
        # The snapshot's age bound is enforced when pass one starts, before any
        # universe is built, and no SQLite universe query runs at all.
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "v2.db"), str(Path(tmp) / "pq")
            build_certified_cache(db)
            export(db, pq)
            stale = time.time() - 5 * 3600
            os.utime(Path(pq) / "ranker_entries_v2.parquet", (stale, stale))
            with mock.patch.object(rk, "load_universe_from_export",
                                   side_effect=AssertionError("universe before the snapshot check")), \
                 mock.patch.object(rk, "load_universe_from_trades",
                                   side_effect=AssertionError("SQLite universe for schema two")):
                with self.assertRaisesRegex(ranker_duck.SchemaTwoEngineError, "stale"):
                    run_pass1(db, str(Path(tmp) / "out"), "auto", pq, max_age_hours="4")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_subset_export_and_reader_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db, pq = str(root / "v2.db"), str(root / "pq")
            rows, _, _ = build_certified_cache(db)
            export(db, pq)
            original = json.loads((Path(pq) / exp.V2_EXPORT_MANIFEST).read_text())
            for field in ("activity_scope", "count", "activity_generation", "sha256"):
                with self.subTest(field=field):
                    changed = json.loads(json.dumps(original))
                    if field == "activity_scope":
                        changed[field] = "full_history"
                    elif field == "sha256":
                        changed["tables"]["activity_groups_v2"][field] = "0" * 64
                    else:
                        changed["projection"][field] += 1
                    (Path(pq) / exp.V2_EXPORT_MANIFEST).write_text(json.dumps(changed))
                    with self.assertRaises(ranker_duck.SchemaTwoEngineError):
                        ranker_duck.get_engine(force="duck", parquet_dir=pq, schema_version=2)
            (Path(pq) / exp.V2_EXPORT_MANIFEST).write_text(json.dumps(original))
            # Mixed files and edited payload bytes fail their committed hashes.
            activity = Path(pq) / "activity_groups_v2.parquet"
            backup = activity.read_bytes()
            shutil.copyfile(Path(pq) / "ranker_entries_v2.parquet", activity)
            with self.assertRaisesRegex(ranker_duck.SchemaTwoEngineError, "hash mismatch"):
                ranker_duck.get_engine(force="duck", parquet_dir=pq, schema_version=2)
            activity.write_bytes(backup)
            # SQLite affinity permits fractional REAL values in an INTEGER
            # column. A plain DuckDB cast rounds this back to the certified
            # timestamp, hiding corruption from the digest. Reject it instead.
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE activity_groups_v2 SET source_time_unix=source_time_unix+0.25 "
                             "WHERE source_trade_id=?", (rows[0]["source_trade_id"],))
            with self.assertRaisesRegex(duckdb.InvalidInputException, "invalid SQLite INTEGER"):
                export(db, pq)
            # The failing range's worker fails the export; no shard or partial file remains.
            self.assertEqual(sorted(Path(pq).glob("activity_groups_v2.parquet.tmp*")), [])
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE activity_groups_v2 SET source_time_unix=? WHERE source_trade_id=?",
                             (rows[0]["source_time_unix"], rows[0]["source_trade_id"]))
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE activity_groups_v2 SET share_amount_str='2.0' WHERE source_trade_id=?",
                             (rows[0]["source_trade_id"],))
            with self.assertRaisesRegex(ValueError, "Parquet projection count/digest"):
                export(db, pq)
            for mutation in ("UPDATE activity_groups_v2 SET coverage_generation=99 WHERE source_trade_id=?",
                             "DELETE FROM activity_groups_v2 WHERE source_trade_id=?"):
                with sqlite3.connect(db) as conn:
                    conn.execute(mutation, (rows[0]["source_trade_id"],))
                with self.assertRaisesRegex(ValueError, "Parquet count"):
                    export(db, pq)

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_attached_types_come_from_the_catalog(self):
        # The catalog reports exactly what a bind would, without the bind's index read.
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "v2.db")
            build_certified_cache(db)
            con = duckdb.connect()
            try:
                con.execute("LOAD sqlite_scanner")
                con.execute(f"ATTACH '{exp._q(db)}' AS src (TYPE sqlite, READ_ONLY)")
                for table in exp.V2_TABLES:
                    described = [(name, dtype) for name, dtype, *_ in
                                 con.execute(f"DESCRIBE SELECT * FROM src.{table}").fetchall()]
                    self.assertEqual(exp._attached_columns(con, table), described)
                with self.assertRaisesRegex(ValueError, "missing required table absent"):
                    exp._attached_columns(con, "absent")
                with self.assertRaisesRegex(ValueError, "unsupported SQLite export type x: DOUBLE"):
                    exp._typed_sqlite_query("SELECT 1", [("x", "DOUBLE")])
            finally:
                con.close()

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_certified_activity_copy_holds_every_key_range_exactly_once(self):
        # Keys in every copy range, on each bound, and past both open ends copy
        # exactly as the single certified query copies them; shards are removed.
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "v2.db")
            build_certified_cache(db)
            extra = ["a", "g2:0", "g2:2", "g2:3", "g2:4", "g2:7", "g2:8", "g2:9", "g2:a",
                     "g2:b", "g2:c", "g2:d", "g2:e", "g2:f", "g3:"]
            with sqlite3.connect(db) as conn:
                (generation,) = conn.execute("SELECT activity_generation FROM ranker_entries_v2 LIMIT 1").fetchone()
                # The exact split values themselves, then longer keys in every range.
                bounds = [bound for bound in exp.ACTIVITY_RANGE_BOUNDS if bound]
                keys_added = bounds + [f"{prefix}{index:064x}" for index, prefix in enumerate(extra)]
                for key in keys_added:
                    conn.execute("INSERT INTO ranker_entries_v2 VALUES (?, ?, 2)", (key, generation))
                    conn.execute("CREATE TEMP TABLE copied AS SELECT * FROM activity_groups_v2 "
                                 "WHERE coverage_generation = ? LIMIT 1", (generation,))
                    conn.execute("UPDATE copied SET source_trade_id = ?", (key,))
                    conn.execute("INSERT INTO activity_groups_v2 SELECT * FROM copied")
                    conn.execute("DROP TABLE copied")
            con = duckdb.connect()
            try:
                con.execute("LOAD sqlite_scanner")
                con.execute(f"ATTACH '{exp._q(db)}' AS src (TYPE sqlite, READ_ONLY)")
                one, many = str(Path(tmp) / "one.parquet"), str(Path(tmp) / "many.parquet")
                single = exp._typed_sqlite_query(exp.CERTIFIED_ACTIVITY_SQL,
                                                 exp._attached_columns(con, "activity_groups_v2"))
                con.execute(f"COPY ({single}) TO '{exp._q(one)}' (FORMAT PARQUET)")
                exp._copy_certified_activity(con, many, 100)
                read = "SELECT * FROM read_parquet('{}') ORDER BY source_trade_id"
                copied = con.execute(read.format(many)).fetchall()
                self.assertEqual(copied, con.execute(read.format(one)).fetchall())
                self.assertEqual(con.execute(f"DESCRIBE {read.format(many)}").fetchall(),
                                 con.execute(f"DESCRIBE {read.format(one)}").fetchall())
                # A merge that cannot write fails the copy and removes its shards.
                blocked = Path(tmp) / "blocked.parquet"
                blocked.mkdir()
                with self.assertRaises(duckdb.Error):
                    exp._copy_certified_activity(con, str(blocked), 100)
                self.assertEqual(sorted(Path(tmp).glob("blocked.parquet.*")), [])
                keys = {row[0] for row in copied}
                self.assertTrue(set(keys_added) <= keys)
                self.assertEqual(sorted(Path(tmp).glob("many.parquet.*")), [])
            finally:
                con.close()

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_empty_certified_projection_preserves_schema_and_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            db, pq = str(Path(tmp) / "v2.db"), str(Path(tmp) / "pq")
            build_certified_cache(db)
            with sqlite3.connect(db) as conn:
                conn.execute("DELETE FROM ranker_entries_v2")
                conn.execute("UPDATE cache_v2_migration_state SET ranker_projection_count=0, "
                             "ranker_projection_digest=?", (whole_projection_digest([]),))
            export(db, pq)
            engine = ranker_duck.get_engine(force="duck", parquet_dir=pq, schema_version=2)
            try:
                self.assertEqual(engine.execute("SELECT COUNT(*) FROM activity_groups_v2").fetchone()[0], 0)
                self.assertEqual(engine.execute("SELECT MAX(generation) FROM activity_coverage_manifests_v2")
                                 .fetchone()[0], 7)
                self.assertEqual(list(ranker_duck.duck_extract_positions_v2(engine, [], 0, 2**62)), [])
            finally:
                engine.close()

    def test_streamed_projection_digest_matches_whole_list(self) -> None:
        for rows in ([], [{"z": None, "a": 'quote"\n\\é', "amount": "1.250000"}],
                     [{"id": i, "amount": "0.500000000000"} for i in range(9)]):
            self.assertEqual(exp._projection_digest(iter(rows)),
                             (len(rows), whole_projection_digest(rows)))

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_schema_two_verified_projection_and_engine_refusal(self) -> None:
        """PASS: DuckDB 1.3.2 reads the joined exact schema-two projection;
        auto selects it, while forced SQLite is a typed refusal."""
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "v2.db")
            pq = str(Path(tmp) / "pq")
            rows, entry, marker = build_certified_cache(db)

            batch_sizes = []
            original_rows = exp._projection_rows

            class BoundedCursor:
                def __init__(self, cursor):
                    self.cursor = cursor

                def fetchmany(self, size):
                    self.assert_size(size)
                    batch = self.cursor.fetchmany(size)
                    batch_sizes.append(len(batch))
                    return batch

                @staticmethod
                def assert_size(size):
                    if size != 2:
                        raise AssertionError(f"unbounded batch: {size}")

                def fetchall(self):
                    raise AssertionError("projection must never fetchall")

            class BoundedConnection:
                def __init__(self, con):
                    self.con = con

                def execute(self, query):
                    cursor = self.con.execute(query)
                    # Schema metadata is bounded by the column count; only the
                    # actual projection data must stream through fetchmany.
                    if query.startswith("DESCRIBE "):
                        return cursor
                    return BoundedCursor(cursor)

            def bounded_rows(con):
                return original_rows(BoundedConnection(con))

            import rank_cycle_manifest
            # Both persisted encodings are copied opaquely; new markers keep
            # watermark/export metadata bounded without exporting receipt rows.
            for cursors in (marker, '[{"wallet_hex":"legacy","pages":[]}]'):
                with sqlite3.connect(db) as conn:
                    conn.execute("UPDATE activity_coverage_manifests_v2 SET cursors_json = ?", (cursors,))
                with mock.patch.object(exp, "PROJECTION_BATCH_SIZE", 2), \
                     mock.patch.object(exp, "_projection_rows", side_effect=bounded_rows):
                    export(db, pq)
                check = duckdb.connect()
                exported = check.execute("SELECT cursors_json, page_hashes_json FROM read_parquet(?)",
                                         [str(Path(pq) / "activity_coverage_manifests_v2.parquet")]).fetchone()
                self.assertEqual(exported, (cursors, "[]"))
                check.close()
                watermark = rank_cycle_manifest.snapshot(Path(db), "2026-02-05", {}, {})
                self.assertEqual(watermark["source_watermark"]["activity"]["cursor"], cursors)
                self.assertLess(len(json.dumps(watermark)), 2500)
            # Only the exported copy is read: it is certified against the
            # finalized digest, and SQLite is not traversed a second time.
            self.assertEqual(batch_sizes, [2, 2, 1, 0] * 2)
            with self.assertRaises(ranker_duck.SchemaTwoEngineError):
                ranker_duck.get_engine(force="sqlite", parquet_dir=pq, schema_version=2)
            engine = ranker_duck.get_engine(
                force="auto", parquet_dir=pq, max_age_hours=0, schema_version=2
            )
            [frame] = ranker_duck.duck_extract_positions_v2(
                engine, [W("a")], entry - 1, entry + 1
            )
            self.assertEqual(len(frame), 1)
            self.assertEqual(str(frame.iloc[0]["contracts"]), "1.250000")
            self.assertAlmostEqual(float(frame.iloc[0]["price"]), 0.4)
            self.assertAlmostEqual(float(frame.iloc[0]["payoff"]), 0.5)
            print("PASS: schema-two projection verified; SQLite refused; half payout exact")

    @unittest.skipUnless(HAVE_DUCKDB, "duckdb not installed")
    def test_schema_two_scores_the_traded_token(self) -> None:
        """PASS: a position whose stored outcome index disagrees with its traded
        token takes the token's place in the payout evidence, for both outcome
        and payoff; an asset missing from its market's tokens is a structurally
        invalid projected row (#690). FAIL: the stored index is scored, or the
        missing asset is silently dropped."""
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "v2.db")
            rows, entry, _ = build_certified_cache(db)
            # Wallet a bought "token", stored as outcome 1, but the evidence lists
            # it first and outcome 0 won.
            for row in rows:
                row["payout_vector_json"] = '["1","0"]'
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE cache_v2_migration_state SET ranker_projection_digest = ?",
                             (whole_projection_digest(rows),))
                conn.execute("UPDATE clob_payout_evidence_v2 SET payout_vector_json = ?, tokens_json = ?",
                             ('["1","0"]', json.dumps([{"token_id": "token"}, {"token_id": ESCAPED_ASSET}])))
            pq = str(Path(tmp) / "scored")
            export(db, pq)
            engine = ranker_duck.get_engine(force="auto", parquet_dir=pq, max_age_hours=0, schema_version=2)
            [frame] = ranker_duck.duck_extract_positions_v2(engine, [W("a")], entry - 1, entry + 1)
            self.assertEqual(int(frame.iloc[0]["outcome_id"]), 0)
            self.assertAlmostEqual(float(frame.iloc[0]["payoff"]), 1.0)
            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE clob_payout_evidence_v2 SET tokens_json = ?",
                             (json.dumps([{"token_id": "elsewhere"}, {"token_id": ESCAPED_ASSET}]),))
            pq = str(Path(tmp) / "unmatched")
            export(db, pq)
            engine = ranker_duck.get_engine(force="auto", parquet_dir=pq, max_age_hours=0, schema_version=2)
            with self.assertRaisesRegex(RuntimeError, "structurally invalid"):
                list(ranker_duck.duck_extract_positions_v2(engine, [W("a")], entry - 1, entry + 1))
            print("PASS: pass one scores the traded token; an unmatched asset is invalid")

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


class BulkRootFence(unittest.TestCase):
    def test_remaining_cache_openers_refuse_before_queries_or_writes(self):
        import runpy
        import backfill_end_dates
        import clob_vs_polygon_reconciliation as reconciliation
        import clob_winner_outcome_id_check as winner_check
        import edge_persistence_walkforward
        import holdcheck_oos_72hr
        import holdout_baseline
        import pnl_decomposition
        import probe_gamma_ua
        from ranker.clv_source_comparison import open_cache
        from latency_shift_rerank import map_pair_tokens

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "bulk.db"
            empty = Path(tmp) / "empty.txt"
            empty.write_text("")
            with sqlite3.connect(db) as connection:
                connection.executescript("""PRAGMA user_version=-2;
                    CREATE TABLE token_conditions (condition_id TEXT, outcome_index INTEGER, token_id TEXT);
                    INSERT INTO token_conditions VALUES ('condition', 0, 'token');""")
            before = db.read_bytes()
            readers = (
                ("token mapping", lambda: map_pair_tokens(str(db), [("condition", "0")])),
                ("edge persistence", lambda: edge_persistence_walkforward.load_positions(db)),
                ("holdout", lambda: holdout_baseline.load_resolutions(db)),
                ("anchor probe", lambda: probe_gamma_ua.load_anchor_ids(str(db), 1)),
                ("reconciliation", lambda: reconciliation._open_ro(str(db))),
                ("CLV comparison", lambda: open_cache(str(db))),
            )
            for name, read in readers:
                with self.subTest(boundary=name), self.assertRaisesRegex(ValueError, "resume cache-populate-activity-v2 --bulk-root"):
                    read()
            with mock.patch.object(backfill_end_dates, "DB", str(db)), self.assertRaisesRegex(ValueError, "unfinished bulk root"):
                backfill_end_dates.main()
            with mock.patch.object(sys, "argv", ["pnl", str(empty), "unused.json", str(db)]), self.assertRaisesRegex(ValueError, "unfinished bulk root"):
                pnl_decomposition.main()
            with mock.patch.object(sys, "argv", ["holdcheck", "--db", str(db), "--universe", str(empty)]), self.assertRaisesRegex(ValueError, "unfinished bulk root"):
                holdcheck_oos_72hr.main()
            # The historical OOS script executes at import and has a fixed DB path.
            # Redirect only its connection to the real fenced fixture, not its SQL.
            connect = sqlite3.connect
            with mock.patch.object(sqlite3, "connect", side_effect=lambda *a, **k: connect(db)), self.assertRaisesRegex(ValueError, "unfinished bulk root"):
                runpy.run_path(str(Path(__file__).with_name("oos_gamma.py")))
            with mock.patch.object(reconciliation, "load_or_fetch", return_value={}), self.assertRaisesRegex(ValueError, "unfinished bulk root"):
                winner_check.main(["--db", str(db)])
            self.assertEqual(db.read_bytes(), before)

    def test_bulk_root_refused_by_python_cache_consumers(self):
        """Private -2 cannot fall through to any schema-one cache consumer."""
        from partial_backfill_wallets import partial_backfill_wallets
        from push_ranking_to_supabase import _cache_schema, CacheStaleError
        from rank_cycle_manifest import snapshot, candidate_targets
        from audit_wallet_history import load_cache
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "bulk.db"
            with sqlite3.connect(db) as connection:
                connection.execute("PRAGMA user_version=-2")
                for read in (partial_backfill_wallets, _cache_schema):
                    with self.assertRaisesRegex((ValueError, CacheStaleError), "unfinished bulk root"):
                        read(connection)
            for read in (
                lambda: snapshot(db, "2026-09-17", {}, {}),
                lambda: candidate_targets(db, db),
                lambda: load_cache(str(db), W("1"), 1),
            ):
                with self.assertRaisesRegex(ValueError, "unfinished bulk root"):
                    read()
            # Entry points must refuse before producing a legacy export/rank.
            import subprocess
            for script, args in (
                ("export_trades_parquet.py", ["--out-dir", str(Path(tmp) / "export")]),
                ("rank_72hr_buyandhold.py", ["--out-dir", str(Path(tmp) / "rank"), "--universe-from-trades"]),
                ("latency_shift_rerank.py", ["--out-dir", str(Path(tmp) / "rerank"), "--ranked-csv", "unused.csv", "--positions-csv", "unused.csv"]),
            ):
                result = subprocess.run([sys.executable, str(Path(__file__).with_name(script)), "--db", str(db), *args], capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("unfinished bulk root", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
