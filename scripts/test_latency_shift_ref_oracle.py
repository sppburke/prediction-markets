#!/usr/bin/env python3
"""Deterministic tests for pass-2's reference oracle and target emission (#536).

Covers: merged/padded target emission; the at-or-before lookup (a future-only
sample never fills — no look-ahead); the staleness bound; the exit-75 fail-closed
coverage gate; and unmapped pairs counting as not repriced. No network, no live
data — a fixture SQLite database and CSVs in a temporary directory.
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("latency_shift_rerank.py")
sys.path.insert(0, str(SCRIPT.parent))
from latency_shift_rerank import ORACLE_VERSION  # noqa: E402

SHIFT = 2
WINDOW = 120

FIXTURE_DDL = """
CREATE TABLE token_conditions (
    token_id        TEXT PRIMARY KEY NOT NULL,
    condition_id    TEXT NOT NULL,
    fetched_at_unix INTEGER NOT NULL,
    outcome_index   INTEGER
);
CREATE TABLE ranker_price_points (
    token_id        TEXT    NOT NULL,
    t               INTEGER NOT NULL,
    price           TEXT    NOT NULL,
    fetched_at_unix INTEGER NOT NULL,
    PRIMARY KEY (token_id, t)
);
CREATE TABLE ranker_price_pages (
    token_id         TEXT    NOT NULL,
    start_ts         INTEGER NOT NULL,
    end_ts           INTEGER NOT NULL,
    fidelity_minutes INTEGER NOT NULL,
    status           TEXT    NOT NULL,
    point_count      INTEGER NOT NULL,
    raw_sha256       TEXT    NOT NULL,
    source_id        TEXT    NOT NULL,
    schema_version   INTEGER NOT NULL,
    parser_version   INTEGER NOT NULL,
    observed_at_unix INTEGER NOT NULL,
    fetched_at_unix  INTEGER NOT NULL,
    request_envelope TEXT    NOT NULL,
    PRIMARY KEY (token_id, start_ts, end_ts, fidelity_minutes)
);
"""

POSITIONS_HEADER = [
    "wallet", "market_id", "outcome_id", "entry_ts", "ttr_secs", "price",
    "contracts", "payoff", "gross", "net", "resolved_at",
]

W1 = "0x" + "1" * 40
ENTRIES = [1_000_000, 1_050_000, 1_100_000]  # three first-buys, market 0xm outcome 0
RESOLVED_AT = 1_200_000


def schema_two_tokens(con, yes_tokens: dict[str, str]) -> None:
    """Schema two maps outcomes through each market's payout evidence token list;
    outcome 0 of every market here is its given token."""
    con.execute("CREATE TABLE clob_payout_evidence_v2 (market_id TEXT PRIMARY KEY, tokens_json TEXT NOT NULL)")
    for market, token in yes_tokens.items():
        con.execute("INSERT INTO clob_payout_evidence_v2 VALUES (?, ?)",
                    (market, json.dumps([{"token_id": token}, {"token_id": token + "-no"}])))


class TokenAuthority(unittest.TestCase):
    def test_oracle_six_identity(self):
        self.assertEqual(ORACLE_VERSION, 6)

    def test_schema_two_maps_through_payout_evidence(self):
        """PASS: schema two takes a pair's token from the payout evidence even when
        token_conditions holds a stale order; schema one keeps token_conditions (#690)."""
        from latency_shift_rerank import map_pair_tokens

        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "tokens.db")
            con = sqlite3.connect(db)
            con.executescript(FIXTURE_DDL)
            con.execute("INSERT INTO token_conditions VALUES ('STALE', '0xm', 1, 0)")
            schema_two_tokens(con, {"0xm": "TOK"})
            pairs = [("0xm", "0"), ("0xm", "1")]
            con.execute("PRAGMA user_version=1")
            con.commit()
            self.assertEqual(map_pair_tokens(db, pairs), {("0xm", "0"): "STALE"})
            con.execute("PRAGMA user_version=2")
            con.commit()
            con.close()
            self.assertEqual(map_pair_tokens(db, pairs), {("0xm", "0"): "TOK", ("0xm", "1"): "TOK-no"})


class RangeAlgebra(unittest.TestCase):
    def test_streamed_inclusive_difference(self):
        from latency_shift_rerank import subtract_ranges

        for needed, covered, expected in (
            ([(1, 5), (6, 10)], [(0, 20)], []),
            ([(1, 10)], [(0, 3), (3, 4), (6, 8)], [(5, 5), (9, 10)]),
            ([(1, 3), (7, 11)], [(0, 5), (10, 15)], [(7, 9)]),
            ([(-10, -2), (1, 4)], [], [(-10, -2), (1, 4)]),
            ([], [(0, 5)], []),
        ):
            with self.subTest(needed=needed, covered=covered):
                self.assertEqual(list(subtract_ranges(iter(needed), iter(covered))), expected)


# Captured from 2690d92's in-memory path with only the 53f1bf7 oracle-6 port.
EXPECTED_POPULATION = {
    'latency_shift_ranked.csv': (
        'wallet,n_total,n_filled,fill_rate,active_months,mean_net_ls,tstat_net_ls,n_eff,hit_rate,eligible,survives\r\n'
        '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,4,4,1.0,2,1.260483,3.8369,4.0,1.0,True,True\r\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,4,4,1.0,2,1.260483,3.8369,4.0,1.0,True,True\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,9,1,0.125,1,-1.0,,1.0,0.0,False,False\r\n'
        '0x0000000000000000000000000000000000000000,0,,,,,,,,False,False\r\n'
        '0xdddddddddddddddddddddddddddddddddddddddd,1,,,,,,,,False,False\r\n'
    ),
    'oracle_outcomes.csv': (
        'wallet,market_id,outcome_id,token_id,entry_ts,payoff,resolved_at,sample_t,sample_price,outcome\r\n'
        '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,0xm1,0,T1,1000400,1.0,1004000,1000402,0.31,repriced\r\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xm1,0,T1,1000100,1.0,1003700,1000072,0.31,repriced\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm10,0,T10,10000100,1.0,10003700,,,invalid_price\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm11,0,T11,11000100,1.0,11003700,,,future_only\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm12,0,T12,12000100,1.0,12003700,,,no_sample\r\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xm2,0,T2,2000100,1.0,2003700,2000102,0.41,repriced\r\n'
        '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,0xm2,0,T2,2000400,1.0,2004000,2000402,0.41,repriced\r\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xm3,0,T3,3000100,1.0,3003700,3000102,0.51,repriced\r\n'
        '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,0xm3,0,T3,3000400,1.0,3004000,3000402,0.51,repriced\r\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xm4,0,T4,4000100,1.0,4003700,4000102,0.61,repriced\r\n'
        '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,0xm4,0,T4,4000400,1.0,4004000,4000402,0.61,repriced\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm5,0,T5,5000100,0.0,5003700,5000102,0.50,repriced\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm5,0,T5,5000500,1.0,5000501,,,post_resolution\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm5,1,T5-no,5000200,1.0,5003800,,,stale\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm5,1,T5-no,5000300,1.0,5003900,,,stale\r\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,0xm6,0,T6,6000100,1.0,6000130,,,scheduled_horizon\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm7,0,,7000100,1.0,7003700,,,unmapped\r\n'
        '0xcccccccccccccccccccccccccccccccccccccccc,0xm8,0,T8,8000100,1.0,8003700,,,price_band\r\n'
    ),
    'oracle_targets.csv': (
        'token_id,start_ts,end_ts\n'
        'T1,999981,1000103\n'
        'T1,1000281,1000403\n'
        'T10,9999981,10000103\n'
        'T11,10999981,11000103\n'
        'T12,11999981,12000103\n'
        'T2,1999981,2000103\n'
        'T2,2000281,2000403\n'
        'T3,2999981,3000103\n'
        'T3,3000281,3000403\n'
        'T4,3999981,4000103\n'
        'T4,4000281,4000403\n'
        'T5,4999981,5000103\n'
        'T5,5000381,5000503\n'
        'T5-no,5000081,5000303\n'
        'T8,7999981,8000103\n'
    ),
    'oracle_manifest.json': (
        '{"as_of":13000000,"fidelity_minutes":1,"floor_tstat":2.0,"git_sha":"unknown","half_life_days":0.0,"inputs":{"before_ranking_sha256":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","cache_stage_record_sha256":"2d01818ed9d47605b5c8b85acf1e2b3c2d82ff703aff45dfe992f05e7d2f7eba","cycle_manifest_sha256":"0c06aa75f133913dd3a2b963013e92b008346070f97c28e8a8848d3eebb698df","oracle_targets_sha256":"ceed5f16ea3ab06e73532fb6e9e4eec74c6bd7d205ffe27c97b0df960f0678e9","positions_csv_sha256":"43cd6e82f06853333d155e561f2c20fd7399a314486537a49930a13785c01473","ranked_csv_sha256":"bcbbbc4d6ef3bec75fd513fac672f0e94fc0ab3b624d63b58bc70bef9950e824"},"latency_shift_secs":2.0,"lookup":"latest sample at-or-before entry+shift","min_active_months":0,"min_avg_per_month":0.0,"min_coverage":0.5,"min_trl":2,"oracle":"clob-minute-reference","oracle_version":6,"outputs":{"before_after_diff_sha256":"5edcf3dee79b541f396ee1c3a971b79d2a028121e8a08ed7e98c7336f1551edb","latency_shift_ranked_sha256":"64180d3275a0bd11f00d0b2e76cb711f756891fef61ed7c49429987c58015375","oracle_outcomes_sha256":"6f9265c8c255a686739237facc7add37ec5f2812140c11de5f924b56c5513bae"},"parser_version":1,"price_band":{"maximum_exclusive":0.85,"minimum_inclusive":0.15},"scheduled_horizon":{"maximum_secs_exclusive":259200,"minimum_secs":60},"schema_version":2,"slip_cents":1.0,"staleness_bound_secs":120.0,"versions":{"activity_parser":2,"activity_schema":2,"cache_schema":2,"clob_resolution_parser":2,"clob_resolution_schema":2,"configuration":1,"oracle_parser":1,"ranker":6,"source":"polymarket-public-activity"}}\n'
    ),
    'before_after_diff.json': (
        '[{"after":{"eligibility":false,"membership":false,"rank":4,"score":null,"survival":false},"before":{"eligibility":false,"membership":false,"rank":null,"score":null,"survival":false},"wallet":"0x0000000000000000000000000000000000000000"},{"after":{"eligibility":true,"membership":true,"rank":2,"score":3.8369,"survival":true},"before":{"eligibility":false,"membership":false,"rank":null,"score":null,"survival":false},"wallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"after":{"eligibility":true,"membership":true,"rank":1,"score":3.8369,"survival":true},"before":{"eligibility":false,"membership":false,"rank":null,"score":null,"survival":false},"wallet":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},{"after":{"eligibility":false,"membership":false,"rank":3,"score":null,"survival":false},"before":{"eligibility":false,"membership":false,"rank":null,"score":null,"survival":false},"wallet":"0xcccccccccccccccccccccccccccccccccccccccc"},{"after":{"eligibility":false,"membership":false,"rank":5,"score":null,"survival":false},"before":{"eligibility":false,"membership":false,"rank":null,"score":null,"survival":false},"wallet":"0xdddddddddddddddddddddddddddddddddddddddd"}]\n'
    ),
    'latency_shift_basket.txt': (
        '# latency_shift_basket — Δ=2.0s slip=0.01 floor_t=2.0 half_life=0.0d min_coverage=0.5 candidates=5 survivors=2\n'
        '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n'
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
    ),
}


class RefOracleScenario(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name)
        self.db = self.root / "fixture.db"
        con = sqlite3.connect(self.db)
        con.executescript(FIXTURE_DDL)
        con.execute(
            "INSERT INTO token_conditions VALUES ('TOK', '0xm', 1, 0)")
        con.commit()
        con.close()

        self.ranked = self.root / "ranked.csv"
        with open(self.ranked, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["wallet", "eligible", "tstat_net", "mean_net"])
            w.writeheader()
            w.writerow({"wallet": W1, "eligible": "True",
                        "tstat_net": "5.0", "mean_net": "0.5"})

        self.positions = self.root / "positions.csv"
        with open(self.positions, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(POSITIONS_HEADER)
            for ts in ENTRIES:
                w.writerow([W1, "0xm", 0, ts, 3600, 0.5, 10, 1.0, 1.0, 0.96, RESOLVED_AT])
        self.versions = self.root / "pipeline_versions.json"
        self.versions.write_text(json.dumps({
            "source": "polymarket-public-activity",
            "activity_schema": 2,
            "activity_parser": 2,
            "clob_resolution_schema": 2,
            "clob_resolution_parser": 2,
            "cache_schema": 2,
            "configuration": 1,
        }))

    def tearDown(self):
        self.dir.cleanup()

    def add_points(self, rows, token_id="TOK"):
        con = sqlite3.connect(self.db)
        for t, price in rows:
            con.execute(
                "INSERT INTO ranker_price_points VALUES (?, ?, ?, 1)", (token_id, t, price))
        con.commit()
        con.close()

    def add_full_coverage(self, entries=ENTRIES, token_id="TOK"):
        lo = entries[0] + SHIFT - WINDOW - 1
        hi = entries[-1] + SHIFT + 1
        con = sqlite3.connect(self.db)
        con.execute(
            "INSERT INTO ranker_price_pages VALUES (?, ?, ?, 1, 'complete', 1, "
            "'00', 'test', 1, 1, 1, 1, 'url')", (token_id, lo, hi))
        con.commit()
        con.close()

    def run_pass2(self, *extra):
        out = self.root / "out"
        r = subprocess.run(
            [sys.executable, str(SCRIPT), "--db", str(self.db),
             "--ranked-csv", str(self.ranked), "--positions-csv", str(self.positions),
             "--out-dir", str(out),
             "--latency-shift-secs", str(SHIFT), "--fill-window-secs", str(WINDOW),
             "--half-life-days", "0", "--min-trl", "0", "--min-active-months", "0",
             "--min-avg-per-month", "0", "--floor-tstat", "2.0",
             "--pipeline-versions-file", str(self.versions), *extra],
            capture_output=True, text=True, timeout=120)
        return r, out

    def read_row(self, out):
        with open(out / "latency_shift_ranked.csv", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 1)
        return rows[0]

    def test_emit_targets_merges_and_pads_windows(self):
        targets = self.root / "targets.csv"
        r, _ = self.run_pass2("--emit-targets", str(targets))
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        lines = targets.read_text().strip().splitlines()
        self.assertEqual(lines[0], "token_id,start_ts,end_ts")
        # Entries are 50k apart >> the 123s window → three disjoint padded windows.
        expected = [
            f"TOK,{ts + SHIFT - WINDOW - 1},{ts + SHIFT + 1}" for ts in ENTRIES
        ]
        self.assertEqual(lines[1:], expected)
        print("PASS: emit-targets writes merged, padded backward windows")

    def test_retry_removes_stale_cycle_spill_and_cleans_up_on_exit(self):
        out = self.root / "out"
        spill = out / "pass2-spill"
        other = out / "pass2-unrelated"
        other.mkdir(parents=True)
        (other / "keep").write_text("unrelated")
        targets = self.root / "targets.csv"
        for extra, expected in (((), 75), (("--emit-targets", str(targets)), 0)):
            with self.subTest(expected=expected):
                spill.mkdir()
                (spill / "stale").write_bytes(b"stale spill")
                result, _ = self.run_pass2(*extra)
                self.assertEqual(result.returncode, expected, result.stderr + result.stdout)
                self.assertFalse(spill.exists())
                self.assertEqual(list(out.glob("pass2-*")), [other])
                self.assertEqual((other / "keep").read_text(), "unrelated")

    def test_spill_path_refuses_symlinks_and_non_directories(self):
        out = self.root / "out"
        out.mkdir()
        spill = out / "pass2-spill"
        target = self.root / "keep"
        target.mkdir()
        (target / "keep").write_text("untouched")
        targets = self.root / "targets.csv"
        for kind in ("directory_symlink", "broken_symlink", "file"):
            with self.subTest(kind=kind):
                if kind == "file":
                    spill.write_text("untouched")
                else:
                    spill.symlink_to(target if kind == "directory_symlink" else self.root / "absent")
                result, _ = self.run_pass2("--emit-targets", str(targets))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("spill", result.stderr + result.stdout)
                self.assertTrue(os.path.lexists(spill))
                self.assertEqual((target / "keep").read_text(), "untouched")
                if kind == "file":
                    self.assertEqual(spill.read_text(), "untouched")
                else:
                    self.assertTrue(spill.is_symlink())
                spill.unlink()

    def test_ref_oracle_at_or_before_staleness_and_lookahead(self):
        # Position 1: fresh sample 30s before entry+Δ → repriced at it (0.40).
        # Position 2: nearest earlier sample 121s stale → NOT repriced; a sample
        #             1s AFTER entry+Δ exists and must never fill (no look-ahead).
        # Position 3: sample exactly AT entry+Δ → repriced (at-or-before inclusive).
        self.add_points([
            (ENTRIES[0] + SHIFT - 30, "0.40"),
            (ENTRIES[1] + SHIFT - WINDOW - 1, "0.10"),
            (ENTRIES[1] + SHIFT + 1, "0.99"),
            (ENTRIES[2] + SHIFT, "0.60"),
        ])
        self.add_full_coverage()
        r, out = self.run_pass2()
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        row = self.read_row(out)
        self.assertEqual(row["n_total"], "3")
        self.assertEqual(row["n_filled"], "2", "stale + future-only must not reprice")
        # Repriced at 0.40 and 0.60 (+1¢ slip), payoff 1.0:
        # nets = (1-0.41)/0.41, (1-0.61)/0.61 → mean (1.4390244+0.6393443)/2 = 1.0391843
        self.assertAlmostEqual(float(row["mean_net_ls"]), 1.039184, places=5)
        self.assertAlmostEqual(float(row["fill_rate"]), 2 / 3, places=4)
        print("PASS: at-or-before lookup, staleness bound, and no look-ahead")

    def test_uncovered_pair_exits_tempfail_75(self):
        self.add_points([(ENTRIES[0] + SHIFT - 30, "0.40")])
        # No page rows at all → the fail-closed gate must hold publication.
        r, _ = self.run_pass2()
        self.assertEqual(r.returncode, 75, r.stderr + r.stdout)
        self.assertIn("TEMPFAIL(75)", r.stdout)
        print("PASS: un-terminal coverage exits 75 (supervised retry), never publishes")

    def test_unmapped_pair_counts_as_not_repriced(self):
        con = sqlite3.connect(self.db)
        con.execute("DELETE FROM token_conditions")
        con.commit()
        con.close()
        # Unmapped pairs carry no coverage requirement and simply never reprice.
        r, out = self.run_pass2()
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        row = self.read_row(out)
        self.assertEqual((row["n_total"], row["n_filled"]), ("3", "0"))
        self.assertEqual(row["survives"], "False")
        print("PASS: unmapped pair → all positions not repriced, never a crash")

    def test_constant_return_cohort_does_not_survive(self):
        entries = [1_000_000 + 10_000 * i for i in range(20)]
        net = (1 - (.20 + .01)) / (.20 + .01)
        con = sqlite3.connect(self.db)
        for i in range(20):
            con.execute("INSERT INTO token_conditions VALUES (?, ?, 1, 0)",
                        (f"TOK{i:02}", f"0xm{i:02}"))
        con.commit()
        con.close()
        with open(self.positions, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(POSITIONS_HEADER)
            for i, entry in enumerate(entries):
                writer.writerow([W1, f"0xm{i:02}", 0, entry, 3600, 0.20, 10, 1.0,
                                 4.0, net, entry + 3600])
        for i, entry in enumerate(entries):
            self.add_points([(entry + SHIFT, "0.20")], token_id=f"TOK{i:02}")
            self.add_full_coverage(entries=[entry], token_id=f"TOK{i:02}")

        outcomes = []
        for half_life, n_eff in ((0, "20.0"), (30, "19.9952")):
            with self.subTest(half_life=half_life):
                r, out = self.run_pass2("--half-life-days", str(half_life), "--min-trl", "20")
                self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
                self.assertIn("reference coverage terminal for all 20 candidate pairs", r.stdout)
                row = self.read_row(out)
                self.assertEqual((row["n_total"], row["n_filled"]), ("20", "20"))
                self.assertEqual((row["fill_rate"], row["hit_rate"]), ("1.0", "1.0"))
                self.assertEqual(row["eligible"], "True")
                self.assertEqual(row["mean_net_ls"], "3.761905")
                self.assertEqual(row["n_eff"], n_eff)
                self.assertEqual(row["tstat_net_ls"], "")
                self.assertEqual(row["survives"], "False")
                with open(out / "oracle_outcomes.csv", newline="") as f:
                    recs = list(csv.DictReader(f))
                self.assertEqual(len(recs), 20)
                self.assertTrue(all(x["outcome"] == "repriced" for x in recs))
                outcomes.append((out / "oracle_outcomes.csv").read_bytes())
                manifest = json.loads((out / "oracle_manifest.json").read_text())
                self.assertEqual(manifest["as_of"], entries[-1])
                self.assertEqual(manifest["half_life_days"], half_life)
                self.assertEqual(manifest["oracle_version"], ORACLE_VERSION)
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(outcomes[0], outcomes[1])
        print("PASS: twenty covered/repriced constant returns have no score and never survive")

    def test_outcomes_artifact_regenerates_published_aggregates(self):
        self.add_points([
            (ENTRIES[0] + SHIFT - 30, "0.40"),
            (ENTRIES[2] + SHIFT, "0.60"),
        ])
        self.add_full_coverage()
        r, out = self.run_pass2()
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        row = self.read_row(out)
        # Recompute from the artifact alone.
        with open(out / "oracle_outcomes.csv", newline="") as f:
            recs = list(csv.DictReader(f))
        self.assertEqual(len(recs), 3)
        repriced = [x for x in recs if x["outcome"] == "repriced"]
        self.assertEqual(str(len(repriced)), row["n_filled"])
        self.assertAlmostEqual(len(repriced) / len(recs), float(row["fill_rate"]), places=4)
        hit = sum(float(x["payoff"]) for x in repriced) / len(repriced)
        self.assertAlmostEqual(hit, float(row["hit_rate"]), places=4)
        # Every mapped row carries the token_id provenance link into the price store.
        self.assertTrue(all(x["token_id"] == "TOK" for x in recs))
        # The manifest binds the exact outputs by digest.
        import hashlib, json
        man = json.load(open(out / "oracle_manifest.json"))
        for name, key in (("latency_shift_ranked.csv", "latency_shift_ranked_sha256"),
                          ("oracle_outcomes.csv", "oracle_outcomes_sha256")):
            digest = hashlib.sha256((out / name).read_bytes()).hexdigest()
            self.assertEqual(digest, man["outputs"][key], name)
        # No stage-2a targets file in this run: the input key must be present and null.
        self.assertIn("oracle_targets_sha256", man["inputs"])
        self.assertIsNone(man["inputs"]["oracle_targets_sha256"])
        self.assertEqual(man["versions"]["source"], "polymarket-public-activity")
        self.assertEqual(man["versions"]["activity_parser"], 2)
        self.assertEqual(man["versions"]["activity_schema"], 2)
        self.assertEqual(man["versions"]["clob_resolution_parser"], 2)
        self.assertEqual(man["versions"]["clob_resolution_schema"], 2)
        self.assertEqual(man["versions"]["ranker"], ORACLE_VERSION)
        self.assertEqual(man["oracle_version"], ORACLE_VERSION)
        self.assertEqual(man["versions"]["configuration"], 1)
        print("PASS: outcomes artifact + manifest regenerate and bind the published aggregates")

    def test_schema_two_price_horizon_edges_and_deterministic_diff(self):
        """PASS: schema two applies [0.15,0.85) and [60,max) only after
        +1-cent repricing, and binds a deterministic before/after diff."""
        con = sqlite3.connect(self.db)
        con.execute("PRAGMA user_version=2")
        schema_two_tokens(con, {"0xm": "TOK"})
        con.commit()
        con.close()
        cases = [
            (1_000_000, 62, "0.14"),       # shifted horizon=60, effective=.15: include
            (1_010_000, 3599, "0.839"),    # shifted horizon=3597, effective=.849: include
            (1_020_000, 3599, "0.84"),     # effective=.85: out of band, out of coverage
            (1_025_000, 3599, None),       # in horizon, stale sample: counts against coverage
            (1_030_000, 61, "0.50"),       # shifted horizon=59: out of scope, needs no page
            (1_040_000, 3602, "0.50"),     # shifted horizon=max: out of scope, needs no page
        ]
        with open(self.positions, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(POSITIONS_HEADER)
            for entry, ttr, _price in cases:
                writer.writerow([W1, "0xm", 0, entry, ttr, 0.99, "1.250000", 1.0,
                                 0, 0, entry + ttr])
        con = sqlite3.connect(self.db)
        for entry, _ttr, price in cases:
            if price is not None:
                con.execute("INSERT INTO ranker_price_points VALUES ('TOK', ?, ?, 1)",
                            (entry + SHIFT, price))
        # The page covers only the in-horizon positions' windows.
        con.execute(
            "INSERT INTO ranker_price_pages VALUES ('TOK', ?, ?, 1, 'complete', 5, "
            "'00', 'test', 1, 1, 1, 1, 'url')",
            (cases[0][0] + SHIFT - WINDOW - 1, cases[3][0] + SHIFT + 1),
        )
        con.commit()
        con.close()
        before = self.root / "before.json"
        before.write_text(json.dumps([{
            "wallet": "0x" + "2" * 40, "score": 3.0, "survives": True, "rank": 1,
        }]))
        cycle = self.root / "cycle.json"
        cycle.write_text('{"cache_schema":2}\n')
        stage = self.root / "cache-stage.json"
        stage.write_text('{"cache_sha256":"aa"}\n')
        args = (
            "--as-of", "1100000", "--min-ttr-secs", "60", "--ttr-max-secs", "3600",
            "--price-min", "0.15", "--price-max", "0.85",
            "--before-ranking-json", str(before),
            "--cycle-manifest-file", str(cycle),
            "--cache-stage-record", str(stage),
        )
        result, out = self.run_pass2(*args)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        with open(out / "oracle_outcomes.csv", newline="") as source:
            reasons = [row["outcome"] for row in csv.DictReader(source)]
        self.assertEqual(
            reasons,
            ["repriced", "repriced", "price_band", "stale", "scheduled_horizon",
             "scheduled_horizon"],
        )
        with open(out / "latency_shift_ranked.csv", newline="") as source:
            row = next(r for r in csv.DictReader(source) if r["wallet"] == W1)
        # In-horizon count 4; coverage 2 repriced / (4 - 1 out of band).
        self.assertEqual((row["n_total"], row["n_filled"], row["fill_rate"]),
                         ("4", "2", "0.6667"))
        first = (out / "before_after_diff.json").read_bytes()
        result, out = self.run_pass2(*args)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(first, (out / "before_after_diff.json").read_bytes())
        manifest = json.loads((out / "oracle_manifest.json").read_text())
        import hashlib
        self.assertEqual(manifest["oracle_version"], ORACLE_VERSION)
        self.assertEqual(manifest["versions"]["ranker"], ORACLE_VERSION)
        for name, key in (("latency_shift_ranked.csv", "latency_shift_ranked_sha256"),
                          ("oracle_outcomes.csv", "oracle_outcomes_sha256")):
            self.assertEqual(manifest["outputs"][key],
                             hashlib.sha256((out / name).read_bytes()).hexdigest(), name)
        self.assertEqual(
            manifest["outputs"]["before_after_diff_sha256"],
            hashlib.sha256(first).hexdigest(),
        )
        print("PASS: schema-two horizon/band edges and before/after diff are deterministic")

    def test_schema_two_evaluates_only_survivable_wallets_exactly(self):
        """PASS: skipping price work for wallets that cannot survive keeps every evaluated
        wallet's ranked row, tie order and repriced outcomes exact (#588)."""
        sys.path.insert(0, str(SCRIPT.parent))
        import latency_shift_rerank as lsr

        x, k1, k2 = ("0x" + c * 40 for c in "0ab")
        # X opens pair 2 before pair 1; sorted pairs fix the tie order, and its pair-6 sample is stale for K2.
        rows = [(x, 2, 0, 30, "0.5"), (x, 6, 0, 30, "0.5")]
        rows += [(k1, p, 100, 3600, px) for p, px in zip((2, 3, 4, 5), ("0.31", "0.41", "0.51", "0.61"))]
        rows += [(k2, p, 400, 3600, px) for p, px in zip((1, 3, 4, 5), ("0.31", "0.41", "0.51", "0.61"))]
        rows.append((k2, 6, 500, 3600, None))
        venue: dict[str, list[tuple[int, str]]] = {}
        spans: dict[str, list[int]] = {}
        with open(self.positions, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(POSITIONS_HEADER)
            for wallet, pair, offset, ttr, price in rows:
                entry = pair * 1_000_000 + offset
                writer.writerow([wallet, f"0xm{pair}", 0, entry, ttr, 0.5, "1.000000", 1.0,
                                 0, 0, entry + ttr])
                spans.setdefault(f"T{pair}", []).append(entry)
                if price is not None:
                    venue.setdefault(f"T{pair}", []).append((entry + SHIFT, price))
        con = sqlite3.connect(self.db)
        con.execute("PRAGMA user_version=2")
        schema_two_tokens(con, {"0xm" + token[1:]: token for token in spans})
        con.commit()
        con.close()

        def store(name, windows):
            db = self.root / name
            shutil.copy(self.db, db)
            con = sqlite3.connect(db)
            for token, lo, hi in windows:
                con.execute("INSERT INTO ranker_price_pages VALUES (?, ?, ?, 1, 'complete', 1, "
                            "'00', 'test', 1, 1, 1, 1, 'url')", (token, lo, hi))
                con.executemany("INSERT OR IGNORE INTO ranker_price_points VALUES (?, ?, ?, 1)",
                                [(token, t, px) for t, px in venue.get(token, []) if lo <= t <= hi])
            con.commit()
            con.close()
            return db

        before = self.root / "before.json"
        before.write_text("[]")
        cycle = self.root / "cycle.json"
        cycle.write_text('{"cache_schema":2}\n')
        stage = self.root / "cache-stage.json"
        stage.write_text('{"cache_sha256":"aa"}\n')
        survivable = lsr.load_survivable_wallets

        def everyone(con, a):
            survivable(con, a)
            con.execute("UPDATE wallets SET candidate=true")

        def run(db, name, *extra, prune=True, anchor=("--as-of", "7000000")):
            out = self.root / name
            argv = [str(SCRIPT), "--db", str(db), "--ranked-csv", str(self.ranked),
                    "--positions-csv", str(self.positions), "--out-dir", str(out),
                    "--latency-shift-secs", str(SHIFT), "--fill-window-secs", str(WINDOW),
                    "--half-life-days", "0", "--min-trl", "2", "--min-active-months", "0",
                    "--min-avg-per-month", "0", "--floor-tstat", "2.0",
                    "--pipeline-versions-file", str(self.versions),
                    "--before-ranking-json", str(before), "--cycle-manifest-file", str(cycle),
                    "--cache-stage-record", str(stage), *anchor, *extra]
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(lsr, "load_survivable_wallets", survivable if prune else everyone):
                return lsr.main(), out

        def read(path):
            with open(path, newline="") as source:
                return list(csv.DictReader(source))

        # The pruned cycle's store holds only what its own 2a targets fetched.
        targets = self.root / "targets.csv"
        self.assertEqual(run(self.db, "emit", "--emit-targets", str(targets))[0], 0)
        windows = [(r["token_id"], int(r["start_ts"]), int(r["end_ts"])) for r in read(targets)]
        self.assertEqual([w for w in windows if w[0] == "T1"],
                         [("T1", 1_000_400 + SHIFT - WINDOW - 1, 1_000_400 + SHIFT + 1)])
        pruned_db = store("pruned.db", windows)
        full_db = store("full.db", [(t, min(e) + SHIFT - WINDOW - 1, max(e) + SHIFT + 1)
                                    for t, e in spans.items()])
        (rc, pruned), (rc_full, full) = run(pruned_db, "pruned"), run(full_db, "full", prune=False)
        self.assertEqual((rc, rc_full), (0, 0))
        ranked, ranked_full = (read(o / "latency_shift_ranked.csv") for o in (pruned, full))
        self.assertEqual([r["wallet"] for r in ranked], [k2, k1, x])
        self.assertEqual(ranked[0]["tstat_net_ls"], ranked[1]["tstat_net_ls"])
        self.assertEqual(ranked, ranked_full)
        # X has no in-horizon position: it keeps a row whose in-scope count is zero.
        self.assertEqual(ranked[2], {"wallet": x, "n_total": "0", "n_filled": "", "fill_rate": "",
                                     "active_months": "", "mean_net_ls": "", "tstat_net_ls": "",
                                     "n_eff": "", "hit_rate": "", "eligible": "False",
                                     "survives": "False"})
        outcomes, outcomes_full = (read(o / "oracle_outcomes.csv") for o in (pruned, full))
        outcomes_full = [r for r in outcomes_full if r["wallet"] != x]
        self.assertEqual(len(outcomes), len(outcomes_full))
        unpriced = {"stale", "no_sample", "future_only"}
        for row, row_full in zip(outcomes, outcomes_full):
            if row["outcome"] != row_full["outcome"]:
                self.assertTrue({row["outcome"], row_full["outcome"]} <= unpriced, row)
                row, row_full = dict(row, outcome=""), dict(row_full, outcome="")
            self.assertEqual(row, row_full)
        self.assertEqual([(r["market_id"], r["outcome"]) for r in read(pruned / "oracle_outcomes.csv")
                          if r["outcome"] != "repriced"], [("0xm6", "no_sample")])

        # An evaluated wallet's missing window still fails closed.
        con = sqlite3.connect(pruned_db)
        con.execute("DELETE FROM ranker_price_pages WHERE token_id = 'T2'")
        con.commit()
        con.close()
        self.assertEqual(run(pruned_db, "uncovered")[0], 75)
        # With no survivable wallet, every position wallet still gets a publishable row.
        rc, out = run(pruned_db, "none", "--min-trl", "99")
        self.assertEqual(rc, 0)
        self.assertEqual(sorted((r["wallet"], r["survives"], r["tstat_net_ls"])
                                for r in read(out / "latency_shift_ranked.csv")),
                         [(w, "False", "") for w in (x, k1, k2)])
        import push_ranking_to_supabase as publisher
        with mock.patch.object(sys, "argv", ["push", "--ranked-csv", str(out / "latency_shift_ranked.csv"),
                                             "--manifest-file", str(out / "oracle_manifest.json")]):
            request = publisher.prepare_publish_request(publisher.build_parser().parse_args(), 0)
        self.assertEqual(request["batch"]["universe_size"], 3)
        self.assertEqual(sorted((e["wallet_hex"], e["survives"], e["ls_tstat"], e["n_trades"], e["fill_rate"])
                                for e in request["entries"]),
                         [(w, False, None, None, None) for w in (x, k1, k2)])
        # Scoring without an explicit anchor is refused.
        self.assertEqual(run(pruned_db, "unanchored", anchor=())[0], 1)
        print("PASS: schema two evaluates only survivable wallets, exactly as evaluating all")

    def test_shared_pair_order_and_non_candidate_keep_rounding_boundary_rows_identical(self):
        from ranker_decay import decay_weights, weighted_stats

        w2, quiet = ("0x" + c * 40 for c in "2f")
        entries = [1_000_000 + 10_000 * i for i in range(8)]
        prices = ["0.31", "0.41", "0.51", "0.61", "0.33", "0.43", "0.53",
                  "0.5151860152874558"]
        order = [7, 0, 6, 1, 5, 2, 4, 3]
        nets = [(1 - (float(price) + .01)) / (float(price) + .01) for price in prices]
        weights = decay_weights(entries, entries[-1], 30)
        mean = weighted_stats(nets, weights)[0]
        self.assertAlmostEqual(mean, 1.2500005, places=15)
        # The fixture crosses the rounding boundary if accumulation follows file order.
        self.assertNotEqual(round(mean, 6), round(weighted_stats(
            [nets[i] for i in order], [weights[i] for i in order])[0], 6))
        with sqlite3.connect(self.db) as con:
            con.execute("PRAGMA user_version=2")
            schema_two_tokens(con, {f"0xm{i}": f"T{i}" for i in range(8)})
        for i, entry in enumerate(entries):
            self.add_points([(entry + SHIFT, prices[i])], token_id=f"T{i}")
            self.add_full_coverage(entries=[entry, entry + 10], token_id=f"T{i}")
        before = self.root / "before.json"
        before.write_text("[]")
        cycle = self.root / "cycle.json"
        cycle.write_text('{"cache_schema":2}\n')
        stage = self.root / "cache-stage.json"
        stage.write_text('{"cache_sha256":"aa"}\n')

        common_rows = {}
        for i, entry in enumerate(entries):
            common_rows[i] = [
                [W1, f"0xm{i}", 0, entry, 3600, .5, 10, 1, 0, 0, entry + 3600],
                [w2, f"0xm{i}", 0, entry + 10, 3600, .5, 10, int(i != 7),
                 0, 0, entry + 3610],
            ]
        outputs = []
        for extra_wallet in (False, True):
            with self.subTest(extra_wallet=extra_wallet):
                with open(self.positions, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(POSITIONS_HEADER)
                    if extra_wallet:
                        writer.writerow([quiet, "0xm7", 0, entries[7], 30, .5, 10,
                                         1, 0, 0, entries[7] + 30])
                    for i in order if extra_wallet else range(8):
                        writer.writerows(common_rows[i])
                result, out = self.run_pass2("--as-of", str(entries[-1]),
                                             "--half-life-days", "30", "--min-trl", "2",
                                             "--before-ranking-json", str(before),
                                             "--cycle-manifest-file", str(cycle),
                                             "--cache-stage-record", str(stage))
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                prefix = tuple((wallet + ",").encode() for wallet in (W1, w2))
                outputs.append([line for line in (out / "latency_shift_ranked.csv").read_bytes().splitlines(keepends=True)
                                if line.startswith(prefix)])
                with open(out / "latency_shift_ranked.csv", newline="") as f:
                    ranked = list(csv.DictReader(f))
                boundary = next(row for row in ranked if row["wallet"] == W1)
                self.assertEqual(boundary["mean_net_ls"], "1.250001")
                self.assertEqual(boundary["survives"], "True")
                if extra_wallet:
                    self.assertIn("candidate positions: 16 across 8 (market,outcome) pairs", result.stdout)
                    skipped = next(row for row in ranked if row["wallet"] == quiet)
                    self.assertEqual((skipped["n_total"], skipped["survives"]), ("0", "False"))
                with open(out / "oracle_outcomes.csv", newline="") as f:
                    outcomes = list(csv.DictReader(f))
                self.assertEqual([(row["market_id"], row["wallet"]) for row in outcomes],
                                 [(f"0xm{i}", wallet) for i in range(8) for wallet in (W1, w2)])
        self.assertEqual(len(outputs[0]), 2)
        self.assertEqual(outputs[0], outputs[1])

    def population_fixture(self):
        """Unsorted shared pairs, tied scores, every skip shape and false verdicts."""
        a, b, c, d, quiet = ("0x" + digit * 40 for digit in "abcd0")
        rows = [(quiet, "0xm0", "0", 900_000, 30, 1.0, None, None)]
        for wallet, pair, offset, price, age in (
            (a, 2, 100, "0.41", 0), (b, 1, 400, "0.31", 0),
            (a, 1, 100, "0.31", 30), (b, 2, 400, "0.41", 0),
            (a, 3, 100, "0.51", 0), (b, 3, 400, "0.51", 0),
            (a, 4, 100, "0.61", 0), (b, 4, 400, "0.61", 0),
        ):
            rows.append((wallet, f"0xm{pair}", "0", pair * 1_000_000 + offset,
                         3600, 1.0, price, age))
        rows += [
            (a, "0xm6", "0", 6_000_100, 30, 1.0, None, None),
            (c, "0xm5", "0", 5_000_100, 3600, 0.0, "0.50", 0),
            (c, "0xm5", "1", 5_000_200, 3600, 1.0, "0.50", 121),
            (c, "0xm5", "1", 5_000_300, 3600, 1.0, None, None),
            (c, "0xm7", "0", 7_000_100, 3600, 1.0, None, None),
            (c, "0xm8", "0", 8_000_100, 3600, 1.0, "0.84", 0),
            (d, "0xm9", "0", 9_000_100, 3600, 1.0, None, None),
            (c, "0xm5", "0", 5_000_500, 3600, 1.0, "0.50", 0),
            (c, "0xm10", "0", 10_000_100, 3600, 1.0, "invalid", 0),
            (c, "0xm11", "0", 11_000_100, 3600, 1.0, "0.50", -1),
            (c, "0xm12", "0", 12_000_100, 3600, 1.0, None, None),
        ]
        with open(self.positions, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(POSITIONS_HEADER)
            for wallet, market, outcome, entry, ttr, payoff, _price, _age in rows:
                writer.writerow([wallet, market, outcome, entry, ttr, 0.99, "1.250000",
                                 payoff, 0, 0, entry + (1 if entry == 5_000_500 else ttr)])
        with sqlite3.connect(self.db) as con:
            con.execute("PRAGMA user_version=2")
            schema_two_tokens(con, {f"0xm{i}": f"T{i}" for i in range(13) if i != 7})
            for wallet, market, outcome, entry, ttr, payoff, price, age in rows:
                if price is not None:
                    token = "T" + market[3:] + ("-no" if outcome == "1" else "")
                    con.execute("INSERT INTO ranker_price_points VALUES (?, ?, ?, 1)",
                                (token, entry + SHIFT - age, price))
            for i in range(13):
                for suffix in ("", "-no"):
                    con.execute("INSERT INTO ranker_price_pages VALUES (?, 0, 14000000, 1, "
                                "'complete', 1, '00', 'test', 1, 1, 1, 1, 'url')",
                                (f"T{i}{suffix}",))
        before, cycle, stage = (self.root / name for name in ("before.json", "cycle.json", "stage.json"))
        before.write_text("[]")
        cycle.write_text('{"cache_schema":2}\n')
        stage.write_text('{"cache_sha256":"aa"}\n')
        return ("--as-of", "13000000", "--min-trl", "2",
                "--before-ranking-json", str(before), "--cycle-manifest-file", str(cycle),
                "--cache-stage-record", str(stage))

    def test_bounded_population_matches_in_memory_bytes(self):
        args = self.population_fixture()
        targets = self.root / "out" / "oracle_targets.csv"
        with mock.patch.dict(os.environ, {"PE_RANKER_DUCKDB_MEMORY_LIMIT": "64MB"}):
            result, _ = self.run_pass2(*args, "--emit-targets", str(targets))
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            result, out = self.run_pass2(*args)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        for name, expected in EXPECTED_POPULATION.items():
            self.assertEqual((out / name).read_bytes(), expected.encode(), name)
        self.assertIn("reference-sample staleness (s before entry+Δ): p50=0 p90=30 max=30", result.stdout)
        self.assertFalse(list(out.glob("pass2-*")), "the private spill is retired")

    def test_ten_second_floor_admits_short_scheduled_horizon(self):
        with sqlite3.connect(self.db) as con:
            con.execute("PRAGMA user_version=2")
            schema_two_tokens(con, {"0xm": "TOK"})
        entries = ENTRIES + [1_150_000, 1_175_000]
        with open(self.positions, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(POSITIONS_HEADER)
            for entry, ttr in zip(entries, (12, 32, 3600, 3600, 11)):
                writer.writerow([W1, "0xm", 0, entry, ttr, .5, 10, 1, 0, 0, entry + ttr])
        self.add_points([(entry + SHIFT, price) for entry, price in zip(entries, ("0.3", "0.4", "0.5", "0.6", "0.7"))])
        self.add_full_coverage(entries=entries)
        before, cycle, stage = (self.root / name for name in ("before.json", "cycle.json", "stage.json"))
        for path in (before, cycle, stage):
            path.write_text("[]" if path == before else "{}")
        args = ("--as-of", str(ENTRIES[-1]), "--before-ranking-json", str(before),
                "--cycle-manifest-file", str(cycle), "--cache-stage-record", str(stage))
        for floor, total, filled in ((10, "4", "4"), (60, "2", "2")):
            with self.subTest(floor=floor):
                result, out = self.run_pass2(*args, "--min-ttr-secs", str(floor))
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                row = self.read_row(out)
                self.assertEqual((row["n_total"], row["n_filled"]), (total, filled))
                manifest = json.loads((out / "oracle_manifest.json").read_text())
                self.assertEqual(manifest["scheduled_horizon"]["minimum_secs"], floor)
                with open(out / "oracle_outcomes.csv", newline="") as f:
                    outcomes = list(csv.DictReader(f))
                self.assertEqual([r["outcome"] for r in outcomes],
                                 ["repriced"] * 4 + ["scheduled_horizon"] if floor == 10 else
                                 ["scheduled_horizon"] * 2 + ["repriced"] * 2 + ["scheduled_horizon"])

    def test_nonpositive_fill_window_is_fatal(self):
        # #536 review M2: staleness bound 0 would admit every stale sample; reject
        # at argument parse, before any output is written.
        r, out = self.run_pass2("--fill-window-secs", "0")
        self.assertEqual(r.returncode, 1, r.stderr + r.stdout)
        self.assertFalse((out / "latency_shift_ranked.csv").exists())
        print("PASS: --fill-window-secs 0 rejected as fatal before any output")

    def test_empty_population_keeps_schema_exit_codes(self):
        self.ranked.write_text("wallet,eligible,tstat_net,mean_net\n")
        result, _ = self.run_pass2("--positions-csv", str(self.root / "absent.csv"))
        self.assertEqual(result.returncode, 76, result.stderr + result.stdout)
        self.positions.write_text(",".join(POSITIONS_HEADER) + "\n")
        with sqlite3.connect(self.db) as con:
            con.execute("PRAGMA user_version=2")
            schema_two_tokens(con, {})
        before, cycle, stage = (self.root / name for name in ("before.json", "cycle.json", "stage.json"))
        for path in (before, cycle, stage):
            path.write_text("[]" if path == before else "{}")
        result, _ = self.run_pass2("--as-of", "1100000", "--before-ranking-json", str(before),
                                   "--cycle-manifest-file", str(cycle), "--cache-stage-record", str(stage))
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)

    def test_schema_one_empty_overlap_writes_header_only(self):
        self.positions.write_text(",".join(POSITIONS_HEADER) + "\n")
        result, out = self.run_pass2()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        with open(out / "latency_shift_ranked.csv", newline="") as f:
            self.assertEqual(list(csv.DictReader(f)), [])
        manifest = json.loads((out / "oracle_manifest.json").read_text())
        self.assertEqual(manifest["as_of"], 0)

    def test_missing_outcome_column_is_fatal(self):
        self.positions.write_text("wallet,market_id\n")
        result, out = self.run_pass2()
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn("positions CSV lacks outcome_id", result.stdout)
        self.assertFalse((out / "latency_shift_ranked.csv").exists())

if __name__ == "__main__":
    unittest.main(verbosity=2)
