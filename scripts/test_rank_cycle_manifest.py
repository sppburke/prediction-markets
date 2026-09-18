#!/usr/bin/env python3
"""Durable bulk routing and backwards-compatible candidate-target output."""

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from rank_cycle_manifest import candidate_targets
import test_rank_and_push as wrapper_tests


def identity(generation=1, base=None, version=2):
    value = dict(version=version, generation=generation, fixed_end_unix=100,
                 wallets=["0x" + "1" * 40])
    if version == 2:
        value.update(base_generation=base, base_manifest_sha256="a" * 64 if base else None,
                     start_exclusive=90 if base else 0, full_read_wallets=value["wallets"])
    value["digest"] = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return json.dumps(value)


class CandidateTargetsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.prior = Path(self.tmp.name) / "prior.db"
        self.side = Path(self.tmp.name) / "side.db"
        for path, version in ((self.prior, 1), (self.side, 2)):
            with sqlite3.connect(path) as c:
                c.executescript(wrapper_tests.RankAndPushScenario.V2_TABLES_SQL)
                c.execute(f"PRAGMA user_version={version}")
                c.execute("INSERT INTO cache_v2_migration_state(singleton) VALUES (1)")

    def targets(self, **kwargs):
        return candidate_targets(self.prior, self.side, include_bulk_root=True, **kwargs)

    def test_fresh_root_and_unchanged_three_value_api_and_cli(self):
        before = self.side.read_bytes()
        self.assertEqual(candidate_targets(self.prior, self.side), (1, 1, 0))
        self.assertEqual(self.targets(), (1, 1, 0, 1))
        command = [sys.executable, str(Path(__file__).with_name("rank_cycle_manifest.py")),
                   "candidate-targets", "--prior", str(self.prior), "--side", str(self.side)]
        for args, expected in (([], "1\n1\n0\n"), (["--include-bulk-root"], "1\n1\n0\n1\n")):
            result = subprocess.run(command + args, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, expected)
        self.assertEqual(self.side.read_bytes(), before)

    def test_each_fresh_admission_exclusion(self):
        mutations = (
            "INSERT INTO activity_groups_v2(source_trade_id) VALUES ('row')",
            "INSERT INTO activity_wallet_coverage_staging_v2 VALUES (1)",
            "INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)",
            "INSERT INTO cache_frozen_payload_verifications VALUES (1)",
            "INSERT INTO ranker_entries_v2 VALUES ('row')",
            "UPDATE cache_v2_migration_state SET phase='finalized'",
            "UPDATE cache_v2_migration_state SET ranker_projection_count=0",
            "UPDATE cache_v2_migration_state SET ranker_projection_digest='digest'",
            "UPDATE cache_v2_migration_state SET ranker_classifier_version=2",
            "UPDATE cache_v2_migration_state SET ranker_projection_inputs_json='{}'",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                original = self.side.read_bytes()
                with sqlite3.connect(self.side) as c:
                    c.execute(mutation)
                self.assertEqual(self.targets(), (1, 1, 0, 0))
                self.assertEqual(candidate_targets(self.prior, self.side), (1, 1, 0))
                self.side.write_bytes(original)

    def test_exact_named_index_admission(self):
        indexes = (
            "",  # missing
            "CREATE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id)",
            "CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id) WHERE source_trade_id IS NOT NULL",
            "CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id, wallet_hex)",
            "CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(lower(source_trade_id))",
            "CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id COLLATE NOCASE)",
            "CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id DESC)",
            "CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(wallet_hex)",
            "CREATE UNIQUE INDEX other_name ON activity_groups_v2(source_trade_id)",
        )
        for ddl in indexes:
            with self.subTest(ddl=ddl):
                original = self.side.read_bytes()
                with sqlite3.connect(self.side) as c:
                    c.execute("DROP INDEX idx_activity_groups_v2_source_trade_id")
                    if ddl:
                        c.execute(ddl)
                self.assertEqual(self.targets()[-1], 0)
                self.side.write_bytes(original)
        with sqlite3.connect(self.side) as c:
            c.executescript("""DROP TABLE activity_groups_v2;
                CREATE TABLE activity_groups_v2(source_trade_id TEXT PRIMARY KEY);
                CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id);""")
        self.assertEqual(self.targets()[-1], 0)

    def test_interrupted_ordinary_identity_never_converts_even_without_rows(self):
        for version in (1, 2):
            with self.subTest(version=version):
                with sqlite3.connect(self.side) as c:
                    c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(version=version),))
                self.assertEqual(self.targets(), (1, 1, 0, 0))

    def test_reserved_root_resumes_with_committed_rows_and_receipts(self):
        with sqlite3.connect(self.side) as c:
            c.executescript("""PRAGMA user_version=-2;
                DROP INDEX idx_activity_groups_v2_source_trade_id;
                INSERT INTO activity_groups_v2(source_trade_id) VALUES ('retained');
                INSERT INTO activity_wallet_coverage_staging_v2 VALUES (1);""")
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(),))
        self.assertEqual(self.targets(), (1, 1, 0, 1))
        with self.assertRaisesRegex(ValueError, "unfinished bulk root"):
            candidate_targets(self.prior, self.side)  # Old caller still refuses.
        with self.assertRaisesRegex(ValueError, "unfinished bulk root"):
            self.targets(after_collection=True)
        with sqlite3.connect(self.side) as c:
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
        with self.assertRaisesRegex(ValueError, "invalid unfinished bulk root"):
            self.targets()

    def test_completed_root_and_predecessor_select_ordinary_collection(self):
        root = identity()
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation, reference_sha256, collection_identity_json) VALUES (1, ?, ?)", (json.loads(root)["digest"], root))
            c.execute("INSERT INTO clob_payout_coverage_manifests_v2(generation) VALUES (1)")
        self.assertEqual(self.targets(), (1, 1, 1, 0))
        self.assertEqual(self.targets(after_collection=True, now=100, max_staleness_hours=24), (1, 1, 1, 0))
        self.assertEqual(self.targets(after_collection=True, now=90000, max_staleness_hours=24), (2, 1, 1, 0))
        with sqlite3.connect(self.prior) as c:
            c.execute("PRAGMA user_version=2")
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(2, 1),))
        self.assertEqual(self.targets(), (2, 1, 1, 0))
        self.assertEqual(candidate_targets(self.prior, self.side), (2, 1, 1))

    def test_two_file_baseline_routes_bulk_without_prior_and_freezes_successor_targets(self):
        """Proves new roots need no prior; inherited heads and payout targets use H0-era evidence."""
        baseline = dict(version=1, side_path=str(self.side), prior_path=str(self.prior),
                        source_sha256="a" * 64, activity_generation=0,
                        payout_generation=1, fresh_identity=None)
        self.prior.unlink()
        receipt = self.side.with_suffix(".stage.json")
        receipt.write_text(json.dumps(baseline))
        self.assertEqual(candidate_targets(None, self.side, include_bulk_root=True), (1, 1, 0, 1))
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("rank_cycle_manifest.py")),
                                 "candidate-targets", "--side", str(self.side), "--include-bulk-root"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "1\n1\n0\n1\n")
        root = identity()
        baseline.update(activity_generation=1, payout_generation=4, fresh_identity=json.loads(root))
        receipt.write_text(json.dumps(baseline))
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
            c.execute("INSERT INTO clob_payout_coverage_manifests_v2(generation) VALUES (4)")
        self.assertEqual(candidate_targets(None, self.side, include_bulk_root=True), (2, 4, 1, 0))
        # A mutated candidate cannot change the baseline or grant a second top-up.
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(4, 3),))
        with self.assertRaisesRegex(ValueError, "single top-up"):
            candidate_targets(None, self.side)
        self.assertFalse(self.prior.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
