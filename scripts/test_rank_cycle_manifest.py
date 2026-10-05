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

from rank_cycle_manifest import TOP_UP_RESERVE_HOURS, _fresh_identity, candidate_targets
import test_rank_and_push as wrapper_tests


def identity(generation=1, base=None, version=2):
    value = dict(version=version, generation=generation, fixed_end_unix=100,
                 wallets=["0x" + "1" * 40])
    if version >= 2:
        value.update(base_generation=base, base_manifest_sha256="a" * 64 if base else None,
                     start_exclusive=90 if base else 0, full_read_wallets=value["wallets"])
    if version == 3:
        value.update(deferred_wallets=value["wallets"] if base else [],
                     full_read_wallets=[] if base else value["wallets"],
                     quiet_after_secs=2_592_000, repoll_period_secs=604_800)
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

    def test_version_three_identity_decodes_with_deferred_wallets_and_constants(self):
        raw = identity(2, 1, version=3)
        self.assertEqual(_fresh_identity(raw), json.loads(raw))
        decoded = _fresh_identity(raw)
        self.assertEqual(decoded["deferred_wallets"], decoded["wallets"])
        self.assertEqual(decoded["full_read_wallets"], [])
        self.assertEqual((decoded["quiet_after_secs"], decoded["repoll_period_secs"]),
                         (2_592_000, 604_800))

    def test_version_three_identity_refuses_missing_extra_keys_and_wrong_digest(self):
        valid = json.loads(identity(2, 1, version=3))
        for key in valid:
            with self.subTest(missing=key):
                malformed = {k: v for k, v in valid.items() if k != key}
                with self.assertRaisesRegex(ValueError, "malformed candidate activity identity|unsupported candidate activity identity version"):
                    _fresh_identity(json.dumps(malformed))
        extra = dict(valid, unexpected=True)
        content = {key: value for key, value in extra.items() if key != "digest"}
        extra["digest"] = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with self.assertRaisesRegex(ValueError, "malformed candidate activity identity"):
            _fresh_identity(json.dumps(extra))
        for key, value in (("digest", "0" * 64), ("deferred_wallets", []),
                           ("quiet_after_secs", 1), ("repoll_period_secs", 1)):
            with self.subTest(changed=key):
                with self.assertRaisesRegex(ValueError, "identity digest mismatch"):
                    _fresh_identity(json.dumps(dict(valid, **{key: value})))

    def test_version_three_initial_head_and_archived_top_up_keep_base_gates(self):
        prior = identity()
        initial = identity(2, 1, version=3)
        with sqlite3.connect(self.prior) as c:
            c.execute("PRAGMA user_version=2")
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (prior,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (initial,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation, reference_sha256, collection_identity_json) VALUES (2, ?, ?)",
                      (json.loads(initial)["digest"], initial))
        self.assertEqual(self.targets(), (2, 1, 0, 0, 1))
        self.assertEqual(self.targets(after_collection=True, now=100, max_staleness_hours=24),
                         (2, 1, 0, 0, 1))
        top_up = identity(3, 2, version=3)
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (top_up,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation, reference_sha256, collection_identity_json) VALUES (3, ?, ?)",
                      (json.loads(top_up)["digest"], top_up))
        self.assertEqual(self.targets(), (3, 1, 0, 0, 1))
        self.assertEqual(self.targets(after_collection=True, now=100, max_staleness_hours=24),
                         (3, 1, 0, 0, 1))
        # The used top-up is the last head: it proceeds while it can still pass the
        # publisher, reserve or not, and is refused only once it cannot.
        self.assertEqual(self.targets(after_collection=True, now=100 + 86400, max_staleness_hours=24),
                         (3, 1, 0, 0, 1))
        with self.assertRaisesRegex(ValueError, "activity top-up is stale"):
            self.targets(after_collection=True, now=101 + 86400, max_staleness_hours=24)
        for head, message in ((identity(3, 1, version=3), "single top-up"),
                              (identity(2, None, version=3), "initial activity head")):
            with self.subTest(head=head):
                with sqlite3.connect(self.side) as c:
                    c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (head,))
                with self.assertRaisesRegex(ValueError, message):
                    self.targets()
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (top_up,))
            c.execute("UPDATE activity_coverage_manifests_v2 SET collection_identity_json=? WHERE generation=2",
                      (identity(2, None, version=3),))
        with self.assertRaisesRegex(ValueError, "initial activity head"):
            self.targets()

    def test_version_three_root_is_not_bulk_eligible(self):
        with sqlite3.connect(self.side) as c:
            c.executescript("PRAGMA user_version=-2; DROP INDEX idx_activity_groups_v2_source_trade_id;")
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(version=3),))
        with self.assertRaisesRegex(ValueError, "invalid unfinished bulk root"):
            self.targets()

    def test_fresh_root_and_unchanged_three_value_api_and_cli(self):
        before = self.side.read_bytes()
        self.assertEqual(candidate_targets(self.prior, self.side), (1, 1, 0))
        self.assertEqual(self.targets(), (1, 1, 0, 1, 0))
        command = [sys.executable, str(Path(__file__).with_name("rank_cycle_manifest.py")),
                   "candidate-targets", "--prior", str(self.prior), "--side", str(self.side)]
        for args, expected in (([], "1\n1\n0\n"), (["--include-bulk-root"], "1\n1\n0\n1\n0\n")):
            result = subprocess.run(command + args, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, expected)
        self.assertEqual(self.side.read_bytes(), before)

    def test_each_fresh_admission_exclusion(self):
        mutations = (
            "INSERT INTO activity_groups_v2(source_trade_id) VALUES ('row')",
            "INSERT INTO activity_wallet_coverage_staging_v2(generation) VALUES (1)",
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
                self.assertEqual(self.targets(), (1, 1, 0, 0, 0))
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
                self.assertEqual(self.targets()[3], 0)
                self.side.write_bytes(original)
        with sqlite3.connect(self.side) as c:
            c.executescript("""DROP TABLE activity_groups_v2;
                CREATE TABLE activity_groups_v2(source_trade_id TEXT PRIMARY KEY);
                CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id);""")
        self.assertEqual(self.targets()[3], 0)

    def test_interrupted_ordinary_identity_never_converts_even_without_rows(self):
        for version in (1, 2, 3):
            with self.subTest(version=version):
                with sqlite3.connect(self.side) as c:
                    c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(version=version),))
                self.assertEqual(self.targets(), (1, 1, 0, 0, 0))

    def test_reserved_root_resumes_with_committed_rows_and_receipts(self):
        with sqlite3.connect(self.side) as c:
            c.executescript("""PRAGMA user_version=-2;
                DROP INDEX idx_activity_groups_v2_source_trade_id;
                INSERT INTO activity_groups_v2(source_trade_id) VALUES ('retained');
                INSERT INTO activity_wallet_coverage_staging_v2(generation) VALUES (1);""")
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(),))
        self.assertEqual(self.targets(), (1, 1, 0, 1, 0))
        with self.assertRaisesRegex(ValueError, "unfinished bulk root"):
            candidate_targets(self.prior, self.side)  # Old caller still refuses.
        with self.assertRaisesRegex(ValueError, "unfinished bulk root"):
            self.targets(after_collection=True)
        with sqlite3.connect(self.side) as c:
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
        with self.assertRaisesRegex(ValueError, "invalid unfinished bulk root"):
            self.targets()

    def test_completed_root_skips_collection_and_unfinished_successor_still_collects(self):
        root = identity()
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation, reference_sha256, collection_identity_json) VALUES (1, ?, ?)", (json.loads(root)["digest"], root))
            c.execute("INSERT INTO clob_payout_coverage_manifests_v2(generation, completed_at_unix) VALUES (1, 100)")
        self.assertEqual(self.targets(), (1, 1, 1, 0, 1))
        self.assertEqual(self.targets(after_collection=True, now=100, max_staleness_hours=24), (1, 1, 1, 0, 1))
        self.assertEqual(self.targets(after_collection=True, now=90000, max_staleness_hours=24), (2, 1, 0, 0, 0))
        # The initial head tops up once it cannot also cover the rest of the cycle.
        reserve = TOP_UP_RESERVE_HOURS * 3600
        self.assertEqual(self.targets(after_collection=True, now=100 + 86400 - reserve,
                                      max_staleness_hours=24), (1, 1, 1, 0, 1))
        self.assertEqual(self.targets(after_collection=True, now=101 + 86400 - reserve,
                                      max_staleness_hours=24), (2, 1, 0, 0, 0))
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE activity_coverage_manifests_v2 SET reference_sha256=?", ("b" * 64,))
        self.assertEqual(self.targets(), (1, 1, 1, 0, 0))
        with self.assertRaisesRegex(ValueError, "matching completed manifest"):
            self.targets(after_collection=True, now=100, max_staleness_hours=24)
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE activity_coverage_manifests_v2 SET reference_sha256=?", (json.loads(root)["digest"],))
        with sqlite3.connect(self.prior) as c:
            c.execute("PRAGMA user_version=2")
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(2, 1),))
        self.assertEqual(self.targets(), (2, 1, 1, 0, 0))
        self.assertEqual(candidate_targets(self.prior, self.side), (2, 1, 1))

    def test_two_file_baseline_routes_bulk_without_prior_and_freezes_successor_targets(self):
        """Proves new roots need no prior; inherited heads and payout targets use H0-era evidence."""
        baseline = dict(version=1, side_path=str(self.side), prior_path=str(self.prior),
                        source_sha256="a" * 64, activity_generation=0,
                        payout_generation=1, fresh_identity=None)
        self.prior.unlink()
        receipt = self.side.with_suffix(".stage.json")
        receipt.write_text(json.dumps(baseline))
        self.assertEqual(candidate_targets(None, self.side, include_bulk_root=True), (1, 1, 0, 1, 0))
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("rank_cycle_manifest.py")),
                                 "candidate-targets", "--side", str(self.side), "--include-bulk-root"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "1\n1\n0\n1\n0\n")
        root = identity()
        baseline.update(activity_generation=1, payout_generation=4, fresh_identity=json.loads(root))
        receipt.write_text(json.dumps(baseline))
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation) VALUES (1)")
            c.execute("INSERT INTO clob_payout_coverage_manifests_v2(generation, completed_at_unix) VALUES (4, 100)")
        self.assertEqual(candidate_targets(None, self.side, include_bulk_root=True), (2, 4, 1, 0, 0))
        # A mutated candidate cannot change the baseline or grant a second top-up.
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (identity(4, 3),))
        with self.assertRaisesRegex(ValueError, "single top-up"):
            candidate_targets(None, self.side)
        self.assertFalse(self.prior.exists())

    def test_after_collection_reuses_only_the_newest_payout_at_or_after_the_head(self):
        root = identity()
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation, reference_sha256, collection_identity_json) VALUES (1, ?, ?)", (json.loads(root)["digest"], root))
        cases = (
            (((1, 100),), 1),
            (((1, 99),), 0),
            (((1, 99), (2, 100)), 1),
            (((1, 100), (2, 99)), 0),
        )
        for prior in (self.prior, None):
            if prior is None:
                self.prior.unlink()
                self.side.with_suffix(".stage.json").write_text(json.dumps(dict(
                    version=1, side_path=str(self.side), prior_path=str(self.prior),
                    source_sha256="a" * 64, activity_generation=0,
                    payout_generation=1, fresh_identity=None)))
            for rows, done in cases:
                with self.subTest(baseline="legacy" if prior else "two-file", rows=rows):
                    with sqlite3.connect(self.side) as c:
                        c.execute("DELETE FROM clob_payout_coverage_manifests_v2")
                        c.executemany("INSERT INTO clob_payout_coverage_manifests_v2(generation, completed_at_unix) VALUES (?, ?)", rows)
                    self.assertEqual(candidate_targets(prior, self.side), (1, 1, 1))
                    self.assertEqual(candidate_targets(prior, self.side, after_collection=True,
                                     now=100, max_staleness_hours=24), (1, 1, done))
                    self.assertEqual(candidate_targets(prior, self.side, include_bulk_root=True,
                                     after_collection=True, now=100, max_staleness_hours=24), (1, 1, done, 0, 1))
                    self.assertEqual(candidate_targets(prior, self.side, include_bulk_root=True,
                                     after_collection=True, now=90000, max_staleness_hours=24), (2, 1, 0, 0, 0))


    def test_after_collection_refreshes_a_completed_payout_below_the_target(self):
        """PASS: a fresh completed walk below the staged payout target is not reused, for
        both baselines. FAIL: reuse without the newest generation reaching the target."""
        root = identity()
        with sqlite3.connect(self.side) as c:
            c.execute("UPDATE cache_v2_migration_state SET fresh_collection_json=?", (root,))
            c.execute("INSERT INTO activity_coverage_manifests_v2(generation, reference_sha256, collection_identity_json) VALUES (1, ?, ?)", (json.loads(root)["digest"], root))
            c.execute("INSERT INTO clob_payout_coverage_manifests_v2(generation, completed_at_unix) VALUES (1, 100)")
        with sqlite3.connect(self.prior) as c:
            c.execute("INSERT INTO clob_payout_coverage_manifests_v2(generation, completed_at_unix) VALUES (1, 100)")
        self.assertEqual(candidate_targets(self.prior, self.side, after_collection=True,
                                           now=100, max_staleness_hours=24), (1, 2, 0))
        self.prior.unlink()
        self.side.with_suffix(".stage.json").write_text(json.dumps(dict(
            version=1, side_path=str(self.side), prior_path=str(self.prior),
            source_sha256="a" * 64, activity_generation=0,
            payout_generation=2, fresh_identity=None)))
        self.assertEqual(candidate_targets(None, self.side, after_collection=True,
                                           now=100, max_staleness_hours=24), (1, 2, 0))

if __name__ == "__main__":
    unittest.main(verbosity=2)
