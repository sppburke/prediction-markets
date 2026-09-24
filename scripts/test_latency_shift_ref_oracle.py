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
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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


def schema_two_tokens(con, yes_tokens: dict[str, str]) -> None:
    """Schema two maps outcomes through each market's payout evidence token list;
    outcome 0 of every market here is its given token."""
    con.execute("CREATE TABLE clob_payout_evidence_v2 (market_id TEXT PRIMARY KEY, tokens_json TEXT NOT NULL)")
    for market, token in yes_tokens.items():
        con.execute("INSERT INTO clob_payout_evidence_v2 VALUES (?, ?)",
                    (market, json.dumps([{"token_id": token}, {"token_id": token + "-no"}])))


class TokenAuthority(unittest.TestCase):
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
                self.assertEqual(manifest["oracle_version"], 3)
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
        self.assertEqual(man["versions"]["ranker"], 3)
        self.assertEqual(man["oracle_version"], 3)
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
        self.assertEqual(manifest["oracle_version"], 3)
        self.assertEqual(manifest["versions"]["ranker"], 3)
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
        # (wallet, pair, entry offset, ttr, venue sample price or None). X enters pairs 1
        # and 6 first with no horizon-feasible position; K1 and K2 tie, and K2 ranks first
        # only because pair 1, which X opens, comes first. K2's pair-6 sample is X's, stale.
        rows = [(x, 1, 0, 30, "0.5"), (x, 6, 0, 30, "0.5")]
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

        def everyone(path, a):
            universe, _ = survivable(path, a)
            return universe, set(universe)

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

    def test_nonpositive_fill_window_is_fatal(self):
        # #536 review M2: staleness bound 0 would admit every stale sample; reject
        # at argument parse, before any output is written.
        r, out = self.run_pass2("--fill-window-secs", "0")
        self.assertEqual(r.returncode, 1, r.stderr + r.stdout)
        self.assertFalse((out / "latency_shift_ranked.csv").exists())
        print("PASS: --fill-window-secs 0 rejected as fatal before any output")

if __name__ == "__main__":
    unittest.main(verbosity=2)
