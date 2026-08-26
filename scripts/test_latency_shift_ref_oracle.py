#!/usr/bin/env python3
"""Deterministic tests for pass-2's reference oracle and target emission (#536).

Covers: merged/padded target emission; the at-or-before lookup (a future-only
sample never fills — no look-ahead); the staleness bound; the exit-75 fail-closed
coverage gate; and unmapped pairs counting as not repriced. No network, no live
data — a fixture SQLite database and CSVs in a temporary directory.
"""
from __future__ import annotations

import csv
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("latency_shift_rerank.py")

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

    def tearDown(self):
        self.dir.cleanup()

    def add_points(self, rows):
        con = sqlite3.connect(self.db)
        for t, price in rows:
            con.execute(
                "INSERT INTO ranker_price_points VALUES ('TOK', ?, ?, 1)", (t, price))
        con.commit()
        con.close()

    def add_full_coverage(self):
        lo = ENTRIES[0] + SHIFT - WINDOW - 1
        hi = ENTRIES[-1] + SHIFT + 1
        con = sqlite3.connect(self.db)
        con.execute(
            "INSERT INTO ranker_price_pages VALUES ('TOK', ?, ?, 1, 'complete', 1, "
            "'00', 'test', 1, 1, 1, 1, 'url')", (lo, hi))
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
             "--min-avg-per-month", "0", "--floor-tstat", "2.0", *extra],
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
        r, out = self.run_pass2("--fill-oracle", "ref")
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
        r, _ = self.run_pass2("--fill-oracle", "ref")
        self.assertEqual(r.returncode, 75, r.stderr + r.stdout)
        self.assertIn("TEMPFAIL(75)", r.stdout)
        print("PASS: un-terminal coverage exits 75 (supervised retry), never publishes")

    def test_unmapped_pair_counts_as_not_repriced(self):
        con = sqlite3.connect(self.db)
        con.execute("DELETE FROM token_conditions")
        con.commit()
        con.close()
        # Unmapped pairs carry no coverage requirement and simply never reprice.
        r, out = self.run_pass2("--fill-oracle", "ref")
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        row = self.read_row(out)
        self.assertEqual((row["n_total"], row["n_filled"]), ("3", "0"))
        self.assertEqual(row["survives"], "False")
        print("PASS: unmapped pair → all positions not repriced, never a crash")

    def test_print_oracle_default_untouched_by_new_flags(self):
        # Default mode must not require the new tables at all: point the run at a db
        # with a trades tape and no ranker tables.
        plain = self.root / "plain.db"
        con = sqlite3.connect(plain)
        con.execute("CREATE TABLE trades (source_trade_id TEXT PRIMARY KEY, wallet_hex TEXT, "
                    "market_id TEXT, outcome_id INTEGER, side TEXT, price_str TEXT, "
                    "contracts INTEGER, timestamp_unix INTEGER)")
        for k, ts in enumerate(ENTRIES):
            con.execute("INSERT INTO trades VALUES (?, '0xother', '0xm', 0, 'buy', '0.45', 1, ?)",
                        (f"t{k}", ts + SHIFT + 5))
        con.commit()
        con.close()
        r = subprocess.run(
            [sys.executable, str(SCRIPT), "--db", str(plain),
             "--ranked-csv", str(self.ranked), "--positions-csv", str(self.positions),
             "--out-dir", str(self.root / "out2"),
             "--latency-shift-secs", str(SHIFT), "--fill-window-secs", str(WINDOW),
             "--half-life-days", "0", "--min-trl", "0", "--min-active-months", "0",
             "--min-avg-per-month", "0", "--floor-tstat", "2.0"],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        with open(self.root / "out2" / "latency_shift_ranked.csv", newline="") as f:
            row = list(csv.DictReader(f))[0]
        self.assertEqual(row["n_filled"], "3", "legacy print path must fill from the tape")
        print("PASS: default print oracle unchanged, no new-table dependency")


if __name__ == "__main__":
    unittest.main(verbosity=2)
