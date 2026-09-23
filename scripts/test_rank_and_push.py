#!/usr/bin/env python3
"""Use-case + regression guard for the cron-ready `scripts/rank_and_push.sh` wrapper
(issue #370 PR2).

The wrapper orchestrates a long-running pipeline over a large cache and live APIs, so it
cannot be run for real in CI. This test instead stands up a fully *stubbed* repo root —
a fake `target/release/pe-bootstrap` plus fake pass-1/pass-2/push Python scripts — and
drives the real wrapper against it. No network, no real data, deterministic.

What it locks (the things that would silently break cron if wired wrong):
  - default universe source is `--universe-from-trades`, never `--universe ""` (the ranker
    hard-errors on neither/both);
  - Step 0 runs deferred discovery → controlled activation → deferred backfill → events →
    resolutions, in that order (issue #383 removed
    the redundant trailing `schedules` stage — fetch_resolutions_and_schedules already covers it);
  - PE_BOOTSTRAP_FETCH_RESOLUTIONS is unset for `backfill` (trades-only) and =1 for `resolutions`,
    so the CLOB→Gamma refresh runs exactly once, not twice (issue #383);
  - a pe-bootstrap stage exiting 2 (partial soft-fail) does NOT abort the run — backfill
    and resolutions return 2 routinely at full scale;
  - a stage exiting 1 (fatal) DOES abort, before ranking;
  - the persistent kernel lock blocks a real concurrent holder and never removes its inode;
  - the skip flags bypass Step 0 / ranking for a pure re-push;
  - zero-arg invocation auto-creates a timestamped out-dir;
  - a clean cron/nohup PATH still selects the repository Python environment;
  - PE_PYTHON overrides repository environments and accepts executable paths with spaces;
  - a missing interpreter or dependency fails before the lock, output dir, or data refresh;
  - the production half-life default is threaded to both passes.
  - a durable logical-cycle pointer exists before activation and a zero-argument retry
    reuses its exact run directory / activation batch instead of admitting another cohort;
  - successful publication performs no automatic purge, reclamation, index rebuild, or
    WAL checkpoint (#544), including through the pure re-push path.

Run: `python3 scripts/test_rank_and_push.py`
  or: `pytest scripts/test_rank_and_push.py -v`
"""
import os
import json
import shutil
import shlex
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

WRAPPER = Path(__file__).resolve().parent / "rank_and_push.sh"

# Production half-life default baked into the wrapper. Pinned here so an intentional change
# co-updates the wrapper literal AND this constant in the same commit (issue #370 PR2 lands
# the 0→30 flip as its own commit).
EXPECTED_DEFAULT_HALF_LIFE = "30"


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


class RankAndPushScenario(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "scripts").mkdir()
        (self.root / "target" / "release").mkdir(parents=True)
        (self.root / "data" / "eval-results").mkdir(parents=True)

        # The wrapper must own interpreter selection. The sandbox's canonical repo environment
        # logs every invocation, then delegates to the interpreter running this test (CI installs
        # scripts/requirements.txt into it). Tests can replace this shim to exercise failures.
        self.python_log = self.root / "python_invocations.log"
        self.repo_python = self.root / ".venv-analysis" / "bin" / "python3"
        self._write_python_shim(self.repo_python, self.python_log)

        # The wrapper passes this cache path to refresh and rank stubs.
        with sqlite3.connect(self.root / "data" / "wallet_cache.db") as connection:
            connection.executescript(
                """
                PRAGMA user_version = 1;
                CREATE TABLE trades (wallet_hex TEXT, timestamp_unix INTEGER);
                CREATE TABLE market_resolutions (fetched_at_unix INTEGER);
                CREATE TABLE source_cursor (key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER);
                CREATE TABLE wallets (
                    wallet_hex TEXT PRIMARY KEY, is_active INTEGER NOT NULL, is_infra INTEGER NOT NULL
                );
                CREATE VIEW active_tradeable_wallets AS
                    SELECT * FROM wallets WHERE is_active = 1 AND is_infra = 0;
                INSERT INTO trades VALUES ('0xabc', 1788192000);
                INSERT INTO market_resolutions VALUES (1788192000);
                INSERT INTO source_cursor VALUES ('clob_closed', '', 1788192000);
                INSERT INTO wallets VALUES ('0xabc', 1, 0);
                """
            )

        # Copy the wrapper-under-test into the sandbox so its `cd "$(dirname "$0")/.."`
        # lands in <tmp>, where the stubs / .env / target/release live.
        self.wrapper = self.root / "scripts" / "rank_and_push.sh"
        shutil.copy(WRAPPER, self.wrapper)
        shutil.copy(WRAPPER.parent / "rank_cycle_manifest.py", self.root / "scripts")
        shutil.copy(WRAPPER.parent / "partial_backfill_wallets.py", self.root / "scripts")

        # The fake publisher below does not contact this syntactically valid endpoint.
        (self.root / ".env").write_text(
            "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
        )

        # Fake pe-bootstrap: log argv to ./pe_bootstrap.log (cwd is the repo root the wrapper
        # cd's into) and the per-invocation PE_BOOTSTRAP_FETCH_RESOLUTIONS value to a sibling
        # pe_bootstrap_env.log (same line order as the argv log, so they zip by index — issue
        # #383 asserts the var is unset for `backfill`, =1 for `resolutions`). Exit code per
        # subcommand via STUB_EXIT_<sub_with_underscores> (default 0).
        _write_exec(
            self.root / "target" / "release" / "pe-bootstrap",
            '#!/usr/bin/env bash\n'
            'if [[ "$1" == "pipeline-versions" ]]; then\n'
            '  printf \'%s\\n\' \'{"source":"polymarket-public-activity","activity_schema":2,"activity_parser":2,"clob_resolution_schema":2,"clob_resolution_parser":2,"cache_schema":2,"configuration":1}\'\n'
            '  exit 0\n'
            'fi\n'
            'echo "$*" >> pe_bootstrap.log\n'
            'echo "${PE_BOOTSTRAP_FETCH_RESOLUTIONS:-}" >> pe_bootstrap_env.log\n'
            'echo "${PE_BOOTSTRAP_PURGE_DECISION_CSV:-}" >> pe_bootstrap_purge_csv.log\n'
            'if [[ -f data/eval-results/rank_and_push.cycle ]]; then\n'
            '  tr -d "\\n" < data/eval-results/rank_and_push.cycle >> pe_bootstrap_cycle.log\n'
            'else\n'
            '  printf missing >> pe_bootstrap_cycle.log\n'
            'fi\n'
            'printf "\\n" >> pe_bootstrap_cycle.log\n'
            'if [[ "$1" == "prices-history" ]]; then\n'
            '  t=""; n=$#; for ((j=1; j<=n; j++)); do [[ "${!j}" == "--targets-csv" ]] && { k=$((j+1)); t="${!k}"; }; done\n'
            '  if [[ -n "$t" && -f "$t" ]]; then echo present >> targets_seen.log; else echo absent >> targets_seen.log; fi\n'
            'fi\n'
            'sub="$1"\n'
            'key="STUB_EXIT_${sub//-/_}"\n'
            'code="${!key:-0}"\n'
            'exit "$code"\n',
        )

        # Fake pass-1 ranker: log argv, emit the two CSVs the wrapper expects in --out-dir.
        _write_exec(
            self.root / "scripts" / "export_trades_parquet.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            'open("export.log", "a").write(" ".join(sys.argv[1:]) + "\\n")\n'
            'raise SystemExit(int(os.environ.get("STUB_EXIT_export", "0")))\n',
        )
        _write_exec(
            self.root / "scripts" / "rank_72hr_buyandhold.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "a = sys.argv[1:]\n"
            'open("rank.log", "a").write(" ".join(a) + "\\n")\n'
            'rc = int(os.environ.get("STUB_EXIT_rank", "0"))\n'
            "if rc:\n"
            "    sys.exit(rc)\n"
            'out = a[a.index("--out-dir") + 1] if "--out-dir" in a else "."\n'
            "os.makedirs(out, exist_ok=True)\n"
            'open(os.path.join(out, "ranked_72hr_buyandhold.csv"), "w").write("wallet\\n0xabc\\n")\n'
            'open(os.path.join(out, "qualifying_positions_72hr.csv"), "w").write("wallet,outcome_id\\n0xabc,1\\n")\n',
        )
        # Fake pass-2 rerank: log argv, emit latency_shift_ranked.csv (non-empty).
        # Models BOTH #536 invocations: stage 2a (--emit-targets writes the targets
        # file, no ranking; STUB_EXIT_emit) and stage 2c (writes ranking + manifest;
        # STUB_EXIT_rerank).
        _write_exec(
            self.root / "scripts" / "latency_shift_rerank.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "ORACLE_VERSION = 1\n"
            "def main():\n"
            "    a = sys.argv[1:]\n"
            '    open("rerank.log", "a").write(" ".join(a) + "\\n")\n'
            '    out = a[a.index("--out-dir") + 1] if "--out-dir" in a else "."\n'
            "    os.makedirs(out, exist_ok=True)\n"
            '    if "--emit-targets" in a:\n'
            '        tpath = a[a.index("--emit-targets") + 1]\n'
            '        open(tpath, "w").write("token_id,start_ts,end_ts\\nTOK,1,100\\n")\n'
            '        return int(os.environ.get("STUB_EXIT_emit", "0"))\n'
            '    rc = int(os.environ.get("STUB_EXIT_rerank", "0"))\n'
            "    if rc:\n"
            "        return rc  # the real coverage gate exits before writing any output\n"
            '    open(os.path.join(out, "latency_shift_ranked.csv"), "w").write("wallet\\n0xabc\\n")\n'
            '    if not os.environ.get("STUB_NO_MANIFEST"):\n'
            '        import hashlib, json\n'
            '        ranked = open(os.path.join(out, "latency_shift_ranked.csv"), "rb").read()\n'
            '        open(os.path.join(out, "oracle_manifest.json"), "w").write(json.dumps(\n'
            '            {"oracle": "clob-minute-reference",\n'
            '             "outputs": {"latency_shift_ranked_sha256": hashlib.sha256(ranked).hexdigest()}}))\n'
            "    return 0\n"
            'if __name__ == "__main__":\n'
            "    raise SystemExit(main())\n",
        )
        # Fake push: log argv, emulate durable request/pending writes, and optionally
        # return the requested status (including EX_TEMPFAIL=75).
        _write_exec(
            self.root / "scripts" / "push_ranking_to_supabase.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "from pathlib import Path\n"
            "import hashlib, json\n"
            "a = sys.argv[1:]\n"
            'open("push.log", "a").write(" ".join(a) + "\\n")\n'
            'if "--validate-request" in a:\n'
            '    payload = json.load(open(a[a.index("--validate-request") + 1]))\n'
            '    identity = {"batch": payload["batch"], "entries": payload["entries"]}\n'
            '    activation = payload.get("cache_activation")\n'
            '    if activation is not None: identity["cache_activation"] = activation\n'
            '    expected = hashlib.sha256(json.dumps(identity, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()).hexdigest()\n'
            '    if payload.get("publish_key") != expected: raise SystemExit(1)\n'
            '    if activation is not None:\n'
            '        for key in ("side_path", "fixed_path", "prior_cache_backup_path", "expected_sha256"): print(activation[key])\n'
            '        if "stage_evidence_sha256" in activation: print(activation["stage_evidence_sha256"])\n'
            '    raise SystemExit(0)\n'
            'if "--snapshot-current" in a:\n'
            '    Path(a[a.index("--snapshot-current") + 1]).write_text("[]\\n")\n'
            '    raise SystemExit(0)\n'
            'if "--request-file" in a:\n'
            '    request = Path(a[a.index("--request-file") + 1])\n'
            '    request.parent.mkdir(parents=True, exist_ok=True)\n'
            '    payload = {"version": 1, "batch": {}, "entries": [{"rank": 1, "wallet_hex": "0xabc"}], "keep_batches": 1080}\n'
            '    if "--cache-side-db" in a:\n'
            '        stage = json.load(open(a[a.index("--cache-stage-record") + 1]))\n'
            '        payload["cache_activation"] = {\n'
            '            "side_path": a[a.index("--cache-side-db") + 1],\n'
            '            "fixed_path": a[a.index("--cache-fixed-db") + 1],\n'
            '            "prior_cache_backup_path": a[a.index("--prior-cache-backup") + 1],\n'
            '            "expected_sha256": stage["cache_sha256"],\n'
            '        }\n'
            '    identity = {"batch": payload["batch"], "entries": payload["entries"]}\n'
            '    if "cache_activation" in payload:\n'
            '        evidence = Path(payload["cache_activation"]["side_path"]).with_suffix(".stage.json")\n'
            '        if evidence.exists(): payload["cache_activation"]["stage_evidence_sha256"] = hashlib.sha256(evidence.read_bytes()).hexdigest()\n'
            '    if "cache_activation" in payload: identity["cache_activation"] = payload["cache_activation"]\n'
            '    payload["publish_key"] = hashlib.sha256(json.dumps(identity, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()).hexdigest()\n'
            '    request.write_text(json.dumps(payload) + "\\n")\n'
            'if "--pending-file" in a:\n'
            '    pending = Path(a[a.index("--pending-file") + 1])\n'
            '    pending.parent.mkdir(parents=True, exist_ok=True)\n'
            '    pending.write_text(str(request) + "\\n")\n'
            'if os.environ.get("STUB_REPLACE_CYCLE"):\n'
            '    replacement = Path("data/eval-results/cron-20990101T000000Z")\n'
            '    replacement.mkdir(parents=True, exist_ok=True)\n'
            '    Path("data/eval-results/rank_and_push.cycle").write_text(str(replacement) + "\\n")\n'
            'raise SystemExit(int(os.environ.get("STUB_PUSH_EXIT", "0")))\n',
        )

        # Keep importable publisher contract owners real while stubbing network/CLI work.
        shutil.copy(WRAPPER.parent / "push_ranking_to_supabase.py", self.root / "scripts" / "publisher_contract.py")
        publisher = self.root / "scripts" / "push_ranking_to_supabase.py"
        body = publisher.read_text()
        publisher.write_text("from publisher_contract import build_parser, save_pending_pointer, load_publish_request\n"
                             + "if __name__ == '__main__':\n"
                             + "".join("    " + line + "\n" for line in body.splitlines()))

    def tearDown(self):
        self._tmp.cleanup()

    # ── helpers ──────────────────────────────────────────────────────────────────────
    def _write_python_shim(self, path, log):
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_exec(
            path,
            "#!/usr/bin/env bash\n"
            f"printf '%s\\n' \"$*\" >> {shlex.quote(str(log))}\n"
            f"exec {shlex.quote(sys.executable)} \"$@\"\n",
        )

    def _run(self, *args, exit_env=None):
        env = dict(os.environ)
        env.pop("PE_PYTHON", None)
        env.update(exit_env or {})
        return subprocess.run(
            ["bash", str(self.wrapper), *args],
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def _log(self, name):
        p = self.root / name
        return p.read_text() if p.exists() else None

    # ── scenarios ────────────────────────────────────────────────────────────────────
    def test_zero_arg_clean_path_uses_repository_python(self):
        poison_dir = self.root / "poison-bin"
        poison_dir.mkdir()
        poison_log = self.root / "poison_python.log"
        _write_exec(
            poison_dir / "python3",
            "#!/usr/bin/env bash\n"
            f"echo ambient-python-used >> {shlex.quote(str(poison_log))}\n"
            "exit 99\n",
        )
        r = self._run(exit_env={"PATH": f"{poison_dir}:/usr/bin:/bin"})
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertTrue(self.python_log.exists(), "repository interpreter was never invoked")
        self.assertFalse(poison_log.exists(), "wrapper used ambient python3 from PATH")
        self.assertIn(str(self.repo_python), r.stdout)
        print("PASS: zero-arg clean-PATH run uses the repository Python, never ambient python3")

    def test_pe_python_override_with_spaces_wins(self):
        custom_log = self.root / "custom_python.log"
        custom_python = self.root / "custom python" / "bin" / "python3"
        self._write_python_shim(custom_python, custom_log)
        r = self._run(exit_env={"PE_PYTHON": str(custom_python)})
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertTrue(custom_log.exists(), "PE_PYTHON interpreter was never invoked")
        self.assertFalse(self.python_log.exists(), "repository interpreter won over PE_PYTHON")
        self.assertIn(str(custom_python), r.stdout)
        print("PASS: PE_PYTHON executable path with spaces overrides repository environments")

    def test_venv_fallback_when_analysis_environment_absent(self):
        shutil.rmtree(self.root / ".venv-analysis")
        fallback_log = self.root / "venv_python.log"
        fallback_python = self.root / ".venv" / "bin" / "python3"
        self._write_python_shim(fallback_python, fallback_log)
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertTrue(fallback_log.exists(), ".venv fallback was never invoked")
        self.assertIn(str(fallback_python), r.stdout)
        print("PASS: .venv/bin/python3 is the automatic fallback when .venv-analysis is absent")

    def test_missing_repository_python_fails_before_mutation(self):
        shutil.rmtree(self.root / ".venv-analysis")
        r = self._run()
        self.assertEqual(r.returncode, 2, f"missing repo Python should exit 2\nstderr={r.stderr}")
        self.assertIn("no repository Python found", r.stderr)
        self._assert_preflight_left_no_pipeline_state()
        print("PASS: absent repository Python fails before lock, output directory, or refresh")

    def test_dependency_failure_fails_before_mutation(self):
        _write_exec(
            self.repo_python,
            "#!/usr/bin/env bash\n"
            "echo 'simulated missing numpy' >&2\n"
            "exit 1\n",
        )
        r = self._run()
        self.assertEqual(r.returncode, 2, f"dependency failure should exit 2\nstderr={r.stderr}")
        self.assertIn("dependency preflight failed", r.stderr)
        self.assertIn("pip install -r scripts/requirements.txt", r.stderr)
        self._assert_preflight_left_no_pipeline_state()
        print("PASS: missing dependency fails before lock, output directory, or refresh")

    def test_invalid_pe_python_fails_closed(self):
        missing = self.root / "does not exist" / "python3"
        r = self._run(exit_env={"PE_PYTHON": str(missing)})
        self.assertEqual(r.returncode, 2, f"invalid PE_PYTHON should exit 2\nstderr={r.stderr}")
        self.assertIn("PE_PYTHON is not executable", r.stderr)
        self.assertFalse(self.python_log.exists(), "invalid override silently fell back to repo Python")
        self._assert_preflight_left_no_pipeline_state()
        print("PASS: invalid PE_PYTHON fails closed instead of silently falling back")

    def test_forced_duck_missing_dependency_fails_before_mutation(self):
        _write_exec(
            self.repo_python,
            "#!/usr/bin/env bash\n"
            "if [[ \"$1\" == '-c' && \"${2:-}\" == 'import duckdb' ]]; then exit 1; fi\n"
            f"exec {shlex.quote(sys.executable)} \"$@\"\n",
        )
        r = self._run("--engine", "duck", "--skip-export")
        self.assertEqual(r.returncode, 2, f"forced duck without duckdb should exit 2\nstderr={r.stderr}")
        self.assertIn("--engine duck requires duckdb", r.stderr)
        self._assert_preflight_left_no_pipeline_state()
        print("PASS: forced DuckDB fails before mutation even when Parquet export is skipped")

    def test_auto_engine_missing_duckdb_warns_and_falls_back(self):
        _write_exec(
            self.repo_python,
            "#!/usr/bin/env bash\n"
            "if [[ \"$1\" == '-c' && \"${2:-}\" == 'import duckdb' ]]; then exit 1; fi\n"
            f"exec {shlex.quote(sys.executable)} \"$@\"\n",
        )
        r = self._run()
        self.assertEqual(r.returncode, 0, f"auto DuckDB fallback failed\nstdout={r.stdout}\nstderr={r.stderr}")
        self.assertIn("engine=auto will fall back to SQLite", r.stderr)
        self.assertIsNotNone(self._log("rank.log"), "ranking did not run through SQLite fallback")
        print("PASS: auto engine preserves the reviewed SQLite fallback when duckdb is absent")

    def _assert_preflight_left_no_pipeline_state(self):
        self.assertIsNone(self._log("pe_bootstrap.log"), "refresh ran despite failed preflight")
        self.assertFalse(
            (self.root / "data" / "eval-results" / ".rank_and_push.lock").exists(),
            "failed preflight created the pipeline lock",
        )
        self.assertEqual(
            list((self.root / "data" / "eval-results").glob("cron-*")),
            [],
            "failed preflight created a run output directory",
        )

    def test_happy_path_default_universe_and_step0_order(self):
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")

        boot = self._log("pe_bootstrap.log")
        self.assertIsNotNone(boot, "Step 0 never invoked pe-bootstrap")
        subs = [ln.split()[0] for ln in boot.splitlines() if ln.strip()]
        self.assertEqual(
            subs,
            [
                "winner-discovery",
                "activate-next",
                "backfill",
                "events",
                "resolutions",
                # #536: the targeted reference fetch runs between the rank passes.
                "prices-history",
            ],
            "refresh/rank stages ran out of canonical purge-free order",
        )
        boot_lines = boot.splitlines()
        self.assertIn("--defer-activation", boot_lines[0])
        self.assertIn("--batch-id", boot_lines[1])
        self.assertIn("--audit-csv", boot_lines[1])
        self.assertIn("--defer-activation", boot_lines[2])

        rank = self._log("rank.log")
        self.assertIn("--universe-from-trades", rank)
        self.assertNotIn("--universe ", rank)  # never the curated-file form by default

        # Auto-timestamped out-dir was created.
        crons = list((self.root / "data" / "eval-results").glob("cron-*"))
        self.assertEqual(len(crons), 1, f"expected one auto out-dir, got {crons}")
        self.assertIn(f"RANK_AND_PUSH_RUN_DIR={crons[0].relative_to(self.root)}", r.stdout)
        cycle_observations = (self._log("pe_bootstrap_cycle.log") or "").splitlines()
        self.assertTrue(cycle_observations)
        self.assertNotIn("missing", cycle_observations)
        self.assertTrue(
            all(observation == str(crons[0].relative_to(self.root)) for observation in cycle_observations),
            f"cache mutation ran without the exact logical-cycle pointer: {cycle_observations}",
        )
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.cycle").exists(),
            "successful cycle did not clear its logical-cycle pointer",
        )
        print("PASS: happy path — default --universe-from-trades, Step-0 order, auto out-dir")

    def test_activation_batch_id_binds_complete_output_path(self):
        first_out = self.root / "first" / "shared-name"
        second_out = self.root / "second" / "shared-name"
        first = self._run("--out-dir", str(first_out))
        self.assertEqual(first.returncode, 0, first.stderr)
        first_line = next(
            line
            for line in (self._log("pe_bootstrap.log") or "").splitlines()
            if line.startswith("activate-next")
        )

        (self.root / "pe_bootstrap.log").unlink()
        second = self._run("--out-dir", str(second_out))
        self.assertEqual(second.returncode, 0, second.stderr)
        second_line = next(
            line
            for line in (self._log("pe_bootstrap.log") or "").splitlines()
            if line.startswith("activate-next")
        )

        def batch_id(line):
            args = line.split()
            return args[args.index("--batch-id") + 1]

        self.assertNotEqual(
            batch_id(first_line),
            batch_id(second_line),
            "distinct output paths with the same basename reused an activation cohort",
        )

    def test_activation_batch_id_accepts_output_path_with_spaces(self):
        out = self.root / "directory with spaces" / "shared name"
        result = self._run("--out-dir", str(out))
        self.assertEqual(result.returncode, 0, result.stderr)
        activation = next(
            line
            for line in (self._log("pe_bootstrap.log") or "").splitlines()
            if line.startswith("activate-next")
        )
        args = activation.split()
        batch_id = args[args.index("--batch-id") + 1]
        self.assertRegex(batch_id, r"^run-[0-9a-f]{16}$")

    def test_fetch_resolutions_env_scoped_to_resolutions_only(self):
        # Issue #383: PE_BOOTSTRAP_FETCH_RESOLUTIONS must be UNSET (empty) when `backfill` runs
        # (trades-only) and =1 only when `resolutions` runs, so the CLOB→Gamma refresh executes
        # exactly once, not twice. The stub logs argv (pe_bootstrap.log) and the env value
        # (pe_bootstrap_env.log) one line per invocation in the same order → zip by index.
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        subs = [ln.split()[0] for ln in (self._log("pe_bootstrap.log") or "").splitlines() if ln.strip()]
        envs = (self._log("pe_bootstrap_env.log") or "").splitlines()
        self.assertEqual(len(subs), len(envs), f"argv/env logs misaligned: {subs} vs {envs}")
        pairs = list(zip(subs, envs))
        self.assertEqual(
            [e for s, e in pairs if s == "backfill"], [""],
            "backfill must run exactly once, trades-only (FETCH_RESOLUTIONS unset)",
        )
        self.assertEqual(
            [e for s, e in pairs if s == "resolutions"], ["1"],
            "resolutions must run exactly once with FETCH_RESOLUTIONS=1",
        )
        # winner-discovery + events run between the top-level unset and the resolutions export,
        # so neither should see a leaked refresh var.
        self.assertEqual([e for s, e in pairs if s == "winner-discovery"], [""], "discovery saw a leaked var")
        self.assertEqual([e for s, e in pairs if s == "events"], [""], "events saw a leaked var")
        print("PASS: FETCH_RESOLUTIONS unset for discovery/backfill/events, =1 only for resolutions")

    def test_backfill_partial_exit2_does_not_abort(self):
        r = self._run(exit_env={"STUB_EXIT_backfill": "2"})
        self.assertEqual(r.returncode, 0, f"exit 2 aborted the run\nstderr={r.stderr}")
        self.assertIsNotNone(self._log("rank.log"), "ranking did not run after a partial backfill")
        print("PASS: backfill exit 2 (partial) → run continues to ranking")

    def test_resolutions_partial_exit2_does_not_abort(self):
        # resolutions is the other Step-0 stage that returns 2 routinely at scale.
        r = self._run(exit_env={"STUB_EXIT_resolutions": "2"})
        self.assertEqual(r.returncode, 0, f"exit 2 aborted the run\nstderr={r.stderr}")
        self.assertIsNotNone(self._log("rank.log"), "ranking did not run after a partial resolutions")
        print("PASS: resolutions exit 2 (partial) → run continues to ranking")

    def test_resolutions_temporary_failure_exit75_aborts_and_retains_cycle_pointer(self):
        # Exit 75 from the resolutions stage covers both temporary conditions:
        # the incomplete resolution audit AND an exhausted-transient CLOB page
        # walk (#534). The wrapper seam is identical for both.
        r = self._run(exit_env={"STUB_EXIT_resolutions": "75"})
        self.assertEqual(r.returncode, 75, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertIsNone(self._log("rank.log"), "ranking ran after a resolutions temporary failure")
        self.assertIsNone(self._log("push.log"), "publication ran after a resolutions temporary failure")
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        self.assertTrue(cycle.is_file(), "resolutions tempfail lost the cycle recovery pointer")
        # The stage label must reflect the temporary-failure semantics: the loop
        # retries exit 75, so the wrapper must not call it FATAL (#534).
        self.assertIn("[resolutions] TEMPFAIL exit 75", r.stderr)
        self.assertNotIn("[resolutions] FATAL", r.stderr)
        self.assertIn("cron-", cycle.read_text())
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertTrue(any(line.startswith("resolutions") for line in boot))
        self.assertFalse(any(line.startswith("purge-infra") for line in boot))
        print("PASS: resolutions exit 75 aborts before rank/push and retains cycle pointer")

    def test_reference_stages_flow_targets_manifest_and_push(self):
        # #536 stage 2a→2b→2c data flow: the emit call writes the targets file, the
        # targeted fetch consumes an EXISTING file, the full rerank writes the
        # manifest, and the push binds it via --manifest-file.
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        rerank = (self._log("rerank.log") or "").splitlines()
        self.assertEqual(len(rerank), 2, "expected the emit call then the full rerank")
        self.assertIn("--emit-targets", rerank[0])
        self.assertNotIn("--emit-targets", rerank[1])
        self.assertIn("--git-sha", rerank[1])
        # 2a applies 2c's survival bounds, so both evaluate the same wallets (#588).
        emit, full = rerank[0].split(), rerank[1].split()
        for flag in ("--min-trl", "--min-ttr-secs", "--ttr-max-secs"):
            self.assertEqual(emit[emit.index(flag) + 1], full[full.index(flag) + 1], flag)
        boot = self._log("pe_bootstrap.log") or ""
        self.assertIn("prices-history --targets-csv", boot)
        self.assertEqual((self._log("targets_seen.log") or "").strip(), "present",
                         "the targeted fetch must consume a real targets file")
        push = self._log("push.log") or ""
        self.assertIn("--manifest-file", push)
        print("PASS: 2a targets -> 2b fetch(consumed) -> 2c manifest -> push binding")

    def test_reference_fetch_partial_then_rerank_tempfail_holds_publication(self):
        # Fetch partial (exit 2 continues) then the coverage gate tempfails (75):
        # publication must not run and the cycle pointer must survive for the retry.
        r = self._run(exit_env={"STUB_EXIT_prices_history": "2", "STUB_EXIT_rerank": "75"})
        self.assertEqual(r.returncode, 75, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertIsNone(self._log("push.log"), "publication ran despite un-terminal coverage")
        rerank = (self._log("rerank.log") or "").splitlines()
        self.assertEqual(len(rerank), 2, "the coverage-gate rerank itself must have run")
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        self.assertTrue(cycle.is_file(), "tempfail lost the cycle recovery pointer")
        self.assertIn("[reference-fetch] WARN exit 2", r.stdout + r.stderr)
        print("PASS: fetch partial -> rerank 75 -> no publication, pointer retained")

    def test_reference_fetch_fatal_aborts_before_rerank(self):
        r = self._run(exit_env={"STUB_EXIT_prices_history": "1"})
        self.assertEqual(r.returncode, 1, f"stderr={r.stderr}")
        rerank = (self._log("rerank.log") or "").splitlines()
        self.assertEqual(len(rerank), 1, "only the emit call may precede a fatal fetch")
        self.assertIsNone(self._log("push.log"), "publication ran despite a fatal fetch")
        print("PASS: fatal reference fetch aborts before the full rerank and push")

    def test_forced_export_failure_stops_before_rank_and_publish(self):
        r = self._run("--engine", "duck", exit_env={"STUB_EXIT_export": "1"})
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIsNone(self._log("rank.log"))
        self.assertIsNone(self._log("push.log"))

    def test_pass1_failure_stops_before_rerank_and_publish(self):
        r = self._run(exit_env={"STUB_EXIT_rank": "1"})
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIsNone(self._log("rerank.log"))
        self.assertIsNone(self._log("push.log"))
        self.assertTrue(
            (self.root / "data/eval-results/rank_and_push.cycle").is_file()
        )

    def test_target_emission_failure_stops_before_fetch_and_publish(self):
        r = self._run(exit_env={"STUB_EXIT_emit": "1"})
        self.assertEqual(r.returncode, 1, r.stderr)
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertFalse(any(line.startswith("prices-history") for line in boot))
        self.assertIsNone(self._log("push.log"))
        self.assertTrue(
            (self.root / "data/eval-results/rank_and_push.cycle").is_file()
        )

    def test_fresh_run_without_manifest_fails_closed_before_push(self):
        # #536: a fresh rerank always writes the manifest before success, so exit 0
        # with no manifest is corruption -- never a silent config_hash = null publish.
        r = self._run(exit_env={"STUB_NO_MANIFEST": "1"})
        self.assertEqual(r.returncode, 1, f"stderr={r.stderr}")
        self.assertIn("refusing provenance-less publish", r.stderr)
        self.assertIsNone(self._log("push.log"), "published without an oracle manifest")
        print("PASS: fresh run with missing manifest fails closed before publication")

    def test_backfill_fatal_exit1_aborts_before_ranking(self):
        r = self._run(exit_env={"STUB_EXIT_backfill": "1"})
        self.assertNotEqual(r.returncode, 0, "fatal backfill should abort the run")
        self.assertIsNone(self._log("rank.log"), "ranking ran despite a fatal backfill")
        boot = self._log("pe_bootstrap.log").splitlines()
        self.assertTrue(any(l.startswith("backfill") for l in boot))
        # `events` is the stage immediately after backfill; a fatal backfill must abort before it.
        self.assertFalse(any(l.startswith("events") for l in boot), "continued past a fatal stage")
        self.assertFalse(any(l.startswith("resolutions") for l in boot), "continued past a fatal stage")
        print("PASS: backfill exit 1 (fatal) → run aborts before ranking")

    def test_discovery_fatal_exit1_aborts_before_backfill(self):
        r = self._run(exit_env={"STUB_EXIT_winner_discovery": "1"})
        self.assertNotEqual(r.returncode, 0, "fatal discovery should abort the run")
        boot = self._log("pe_bootstrap.log").splitlines()
        self.assertTrue(any(l.startswith("winner-discovery") for l in boot))
        self.assertFalse(any(l.startswith("backfill") for l in boot), "ran backfill after fatal discovery")
        print("PASS: winner-discovery exit 1 (leaderboard fatal) → run aborts before backfill")

    def test_activation_fatal_aborts_before_backfill(self):
        r = self._run(exit_env={"STUB_EXIT_activate_next": "1"})
        self.assertNotEqual(r.returncode, 0)
        boot = self._log("pe_bootstrap.log").splitlines()
        self.assertTrue(any(l.startswith("activate-next") for l in boot))
        self.assertFalse(any(l.startswith("backfill") for l in boot))

    def test_skip_discovery_skips_activation_and_still_defers_backfill(self):
        r = self._run("--skip-discovery")
        self.assertEqual(r.returncode, 0, r.stderr)
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertFalse(any(l.startswith("winner-discovery") for l in boot))
        self.assertFalse(any(l.startswith("activate-next") for l in boot))
        backfill = [l for l in boot if l.startswith("backfill")]
        self.assertEqual(len(backfill), 1)
        self.assertIn("--defer-activation", backfill[0])

    def test_concurrent_run_blocked_by_live_lock(self):
        lock = self.root / "data" / "eval-results" / ".rank_and_push.lock"
        holder = subprocess.Popen(
            [
                "bash",
                "-c",
                'exec 9<>"$1"; flock -n 9; printf "%s\\n" "$$" > "$1"; '
                "echo ready; read -r _",
                "holder",
                str(lock),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(holder.stdout.readline().strip(), "ready")
            inode = lock.stat().st_ino
            live_pid = lock.read_text()
            r = self._run()
            self.assertEqual(
                r.returncode,
                3,
                f"a live kernel lock must block with exit 3\nstderr={r.stderr}",
            )
            self.assertIsNone(self._log("pe_bootstrap.log"), "ran Step 0 despite a held lock")
            self.assertEqual(lock.read_text(), live_pid, "clobbered the holder's PID")
            self.assertEqual(lock.stat().st_ino, inode, "replaced the persistent lock inode")
        finally:
            holder.kill()
            holder.wait(timeout=5)
            holder.stdin.close()
            holder.stdout.close()

        released = self._run()
        self.assertEqual(released.returncode, 0, released.stderr)
        self.assertTrue(lock.is_file(), "completed run removed the persistent lock inode")
        self.assertEqual(lock.stat().st_ino, inode, "post-kill acquisition replaced the inode")
        print("PASS: real holder blocks; kill releases kernel lock without stale-file reclamation")

    def test_pure_repush_skips_step0_and_ranking(self):
        out = self.root / "data" / "eval-results" / "prior"
        out.mkdir()
        (out / "latency_shift_ranked.csv").write_text("wallet\n0xabc\n")
        r = self._run("--skip-discovery", "--skip-backfill", "--skip-rank", "--out-dir", str(out))
        self.assertEqual(r.returncode, 0, f"stderr={r.stderr}")
        self.assertIsNone(self._log("pe_bootstrap.log"), "Step 0 ran during a pure re-push")
        self.assertIsNone(self._log("rank.log"), "ranking ran during a pure re-push")
        push = self._log("push.log") or ""
        self.assertTrue(push, "re-push did not push")
        self.assertNotIn("--manifest-file", push,
                         "legacy pre-cutover re-push must publish config_hash = null")
        print("PASS: --skip-discovery --skip-backfill --skip-rank → re-push only")

    def test_unique_notes_repush_publishes_exactly_once_without_purge(self):
        out = self.root / "data" / "eval-results" / "prior"
        out.mkdir()
        (out / "latency_shift_ranked.csv").write_text("wallet\n0xabc\n")
        result = self._run(
            "--skip-discovery",
            "--skip-backfill",
            "--skip-rank",
            "--out-dir",
            str(out),
            "--notes",
            "forge-544-unique-proof",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        pushes = (self._log("push.log") or "").splitlines()
        self.assertEqual(len(pushes), 1)
        self.assertIn("--notes forge-544-unique-proof", pushes[0])
        self.assertIsNone(self._log("pe_bootstrap.log"))
        self.assertNotIn("purge", result.stdout + result.stderr)
        print("PASS: unique-notes re-push publishes exactly once with no purge path")

    def test_transient_push_retains_request_and_resume_runs_only_tail(self):
        first = self._run(exit_env={"STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr)
        pending = self.root / "data" / "eval-results" / "rank_and_push.pending"
        self.assertTrue(pending.is_file(), "transient push did not retain pending pointer")
        request = self.root / pending.read_text().strip()
        self.assertTrue(request.is_file(), "pending pointer target was not persisted first")

        boot_before = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertEqual(sum(line.startswith("activate-next") for line in boot_before), 1)
        self.assertFalse(
            any(line.startswith("purge ") or line == "purge" for line in boot_before),
            "post-publish purge ran after a failed push",
        )

        resumed = self._run("--resume-pending")
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        boot_after = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertEqual(
            sum(line.startswith("activate-next") for line in boot_after),
            1,
            "resume activated a second wallet cohort",
        )
        self.assertEqual(
            sum(line.startswith("backfill") for line in boot_after),
            1,
            "resume reran the expensive refresh",
        )
        self.assertEqual(
            boot_after,
            boot_before,
            "publication resume invoked a cache-mutating maintenance stage",
        )
        self.assertEqual(len((self._log("rank.log") or "").splitlines()), 1)
        self.assertIn("--resume-request", (self._log("push.log") or "").splitlines()[-1])
        self.assertFalse(pending.exists(), "completed tail did not clear its pending pointer")
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.cycle").exists(),
            "completed publication tail did not clear the logical-cycle pointer",
        )
        print("PASS: transient push resume replays only the exact publication request")

    def test_zero_arg_transient_cycle_reuses_run_directory_and_activation_batch(self):
        first = self._run(exit_env={"STUB_EXIT_events": "75"})
        self.assertEqual(first.returncode, 75, first.stderr)
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        self.assertTrue(cycle.is_file(), "temporary events failure lost logical-cycle identity")
        first_out = cycle.read_text().strip()

        first_boot = (self._log("pe_bootstrap.log") or "").splitlines()
        first_activation = next(line for line in first_boot if line.startswith("activate-next"))
        first_args = first_activation.split()
        first_batch = first_args[first_args.index("--batch-id") + 1]

        resumed = self._run()
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertIn(f"RANK_AND_PUSH_CYCLE_RESUME={first_out}", resumed.stdout)
        self.assertIn(f"RANK_AND_PUSH_RUN_DIR={first_out}", resumed.stdout)

        activations = [
            line
            for line in (self._log("pe_bootstrap.log") or "").splitlines()
            if line.startswith("activate-next")
        ]
        self.assertEqual(len(activations), 2)
        second_args = activations[1].split()
        second_batch = second_args[second_args.index("--batch-id") + 1]
        self.assertEqual(first_batch, second_batch, "cycle retry selected another activation batch")
        self.assertEqual(
            len(list((self.root / "data" / "eval-results").glob("cron-*"))),
            1,
            "cycle retry created another production run directory",
        )
        self.assertFalse(cycle.exists(), "completed retried cycle did not clear its pointer")

    def test_direct_zero_arg_prefers_pending_publication_over_cycle_replay(self):
        first = self._run(exit_env={"STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr)
        boot_before = (self._log("pe_bootstrap.log") or "").splitlines()

        resumed = self._run()
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertIn("RANK_AND_PUSH_AUTO_RESUME_PENDING=", resumed.stdout)
        self.assertEqual(
            (self._log("pe_bootstrap.log") or "").splitlines(),
            boot_before,
            "direct zero-argument recovery reran pre-publication stages",
        )
        self.assertIn("--resume-request", (self._log("push.log") or "").splitlines()[-1])

    def test_successful_zero_arg_cycle_clears_its_pending_pointer(self):
        result = self._run()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.pending").exists()
        )
        self.assertIn("[recovery] cleared completed pending", result.stdout)
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.cycle").exists()
        )

    def test_unchanged_daily_watermark_exits_before_refresh_or_publish(self):
        first = self._run()
        self.assertEqual(first.returncode, 0, first.stderr)
        boot_before = self._log("pe_bootstrap.log")
        rank_before = self._log("rank.log")
        push_before = self._log("push.log")
        cron_before = sorted((self.root / "data/eval-results").glob("cron-*"))

        second = self._run()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", second.stdout)
        self.assertEqual(self._log("pe_bootstrap.log"), boot_before)
        self.assertEqual(self._log("rank.log"), rank_before)
        self.assertEqual(self._log("push.log"), push_before)
        self.assertEqual(sorted((self.root / "data/eval-results").glob("cron-*")), cron_before)
        self.assertFalse(
            (self.root / "data/eval-results/rank_and_push.cycle").exists(),
            "watermark no-op invented a recovery pointer",
        )

    def test_marker_set_changes_fingerprint_even_for_inactive_wallets(self):
        db = self.root / "data" / "wallet_cache.db"
        with sqlite3.connect(db) as conn:
            conn.execute("ALTER TABLE wallets ADD COLUMN backfill_partial INTEGER NOT NULL DEFAULT 0")
            conn.execute("INSERT INTO wallets VALUES ('inactive',0,0,0)")
        first = self._run()
        self.assertEqual(first.returncode, 0, first.stderr)
        before = self._log("pe_bootstrap.log")
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE wallets SET backfill_partial = 1 WHERE wallet_hex = 'inactive'")
        second = self._run()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertNotIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", second.stdout)
        self.assertNotEqual(self._log("pe_bootstrap.log"), before)
        third = self._run()
        self.assertEqual(third.returncode, 0, third.stderr)
        self.assertIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", third.stdout)

    def test_unchanged_active_partial_wallet_forces_same_day_refresh(self):
        db = self.root / "data" / "wallet_cache.db"
        with sqlite3.connect(db) as conn:
            conn.execute("ALTER TABLE wallets ADD COLUMN backfill_partial INTEGER NOT NULL DEFAULT 0")
            conn.execute("UPDATE wallets SET backfill_partial = 1 WHERE wallet_hex = '0xabc'")
        first = self._run()
        self.assertEqual(first.returncode, 0, first.stderr)
        before = self._log("pe_bootstrap.log")
        # The stub leaves both the marker and source watermark unchanged.
        second = self._run()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertNotIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", second.stdout)
        self.assertNotEqual(self._log("pe_bootstrap.log"), before)
        manifests = sorted((self.root / "data/eval-results").glob("cron-*/accepted_cycle_manifest.json"))
        for path in manifests:
            manifest = json.loads(path.read_text())
            self.assertEqual(manifest["universe"]["backfill_partial_wallets"], ["0xabc"])

    def test_v2_cycle_manifest_uses_only_completed_generations_and_stamps_versions(self):
        db = self.root / "data" / "wallet_cache.db"
        db.unlink()
        with sqlite3.connect(db) as connection:
            connection.executescript(
                """
                PRAGMA user_version = 2;
                CREATE TABLE wallets (
                    wallet_hex TEXT PRIMARY KEY, is_active INTEGER NOT NULL,
                    is_infra INTEGER NOT NULL
                );
                CREATE VIEW active_tradeable_wallets AS
                    SELECT * FROM wallets WHERE is_active = 1 AND is_infra = 0;
                CREATE TABLE activity_coverage_manifests_v2 (
                    generation INTEGER PRIMARY KEY, cursors_json TEXT,
                    completed_at_unix INTEGER, reference_sha256 TEXT,
                    wallet_count INTEGER, receipt_set_digest TEXT,
                    aggregate_digest TEXT, source_row_count INTEGER
                );
                CREATE TABLE activity_groups_v2 (
                    wallet_hex TEXT, source_time_unix INTEGER, activity_type TEXT,
                    coverage_generation INTEGER
                );
                CREATE TABLE clob_payout_coverage_manifests_v2 (
                    generation INTEGER PRIMARY KEY, terminal_kind TEXT,
                    completed_at_unix INTEGER, manifest_json TEXT,
                    terminal_page_sha256 TEXT
                );
                CREATE TABLE clob_payout_evidence_v2 (
                    coverage_generation INTEGER, fetched_at_unix INTEGER
                );
                CREATE TABLE cache_v2_migration_state (
                    singleton INTEGER PRIMARY KEY, ranker_projection_count INTEGER,
                    ranker_projection_digest TEXT, ranker_classifier_version INTEGER
                );
                INSERT INTO wallets VALUES ('0xabc', 1, 0);
                INSERT INTO activity_coverage_manifests_v2 (generation, cursors_json, completed_at_unix, reference_sha256, wallet_count, receipt_set_digest, aggregate_digest, source_row_count) VALUES
                    (1, '{"0xabc":10}', 20, 'aa', 1, 'bb', 'cc', 2);
                INSERT INTO activity_groups_v2 VALUES ('0xabc', 10, 'TRADE', 1);
                INSERT INTO activity_groups_v2 VALUES ('0xignored', 99, 'TRADE', 2);
                INSERT INTO activity_groups_v2 VALUES ('0xabc', 98, 'REDEEM', 1);
                INSERT INTO clob_payout_coverage_manifests_v2 VALUES
                    (3, 'end_cursor', 30, '{}', 'dd');
                INSERT INTO clob_payout_evidence_v2 VALUES (3, 29);
                INSERT INTO clob_payout_evidence_v2 VALUES (2, 97);
                INSERT INTO cache_v2_migration_state VALUES (1, 1, 'ee', 1);
                """
            )

        # Direct test of the snapshot owner: an installed schema-two cache now runs
        # the candidate lane in the wrapper, so the decoy rows are checked here.
        sys.path.insert(0, str(self.root / "scripts"))
        try:
            import importlib
            rank_cycle_manifest = importlib.import_module("rank_cycle_manifest")
        finally:
            sys.path.pop(0)
        manifest = rank_cycle_manifest.snapshot(
            db, "2026-09-15",
            {"activity_schema": 2, "activity_parser": 2, "clob_resolution_schema": 2,
             "clob_resolution_parser": 2, "cache_schema": 2, "configuration": 1},
            {"top_n": "200"},
        )
        self.assertEqual(manifest["universe"]["backfill_partial_wallets"], [])
        self.assertEqual(manifest["source_watermark"]["activity"]["generation"], 1)
        self.assertEqual(manifest["source_watermark"]["activity"]["count"], 2)
        self.assertEqual(manifest["source_watermark"]["activity"]["newest_source_unix"], 10)
        self.assertEqual(manifest["source_watermark"]["activity"]["receipt_set_digest"], "bb")
        self.assertEqual(
            manifest["source_watermark"]["activity"]["ranker_projection"]["digest"], "ee"
        )
        self.assertEqual(manifest["source_watermark"]["resolution"]["generation"], 3)
        self.assertEqual(manifest["source_watermark"]["resolution"]["count"], 1)
        self.assertEqual(manifest["source_watermark"]["resolution"]["newest_fetch_unix"], 29)
        self.assertEqual(
            manifest["source_watermark"]["resolution"]["terminal_page_sha256"], "dd"
        )
        self.assertEqual(manifest["versions"]["activity_parser"], 2)
        self.assertEqual(manifest["versions"]["clob_resolution_schema"], 2)
        self.assertEqual(manifest["versions"]["ranker"], 1)

    def test_recovered_schema_two_prior_displaces_the_schema_one_installed_cache(self):
        """PASS: a legacy cycle whose immutable prior is a recovered schema-two
        candidate beside the schema-one installed cache collects the prior's
        next generation, binds the displaced path as the installed cache's
        backup, and activation preserves the installed bytes there. FAIL: the
        prior named as the backup, which activation refuses because its bytes
        are not the installed cache's."""
        fixed = self._install_candidate_layout(schema=1)
        self._install_candidate_stub()
        (self.root / ".env").write_text(
            "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
            "PE_RANK_SCHEMA_TWO_CUTOVER=1\n"
        )
        result = self._run(exit_env={"STUB_PRIOR_SCHEMA": "2"})
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        out = next((self.root / "data/eval-results").glob("cron-*"))
        prior = self.root / "phys" / f"wallet_cache.{out.name}.prior.db"
        displaced = self.root / "phys" / f"wallet_cache.{out.name}.displaced.db"
        self.assertEqual(
            self._bootstrap_ops(),
            ["cache-stage-v2", "winner-discovery", "activate-next", "cache-populate-activity-v2",
             "cache-populate-payout-v2", "cache-finalize-v2", "prices-history", "cache-finalize-v2",
             "cache-activate"],
            "a schema-two prior is not sealed again",
        )
        self.assertIn("--fresh-generation 2", self._bootstrap_lines("cache-populate-activity-v2")[0])
        self.assertIn(f"--backup {displaced}", self._bootstrap_lines("cache-activate")[0])
        request = json.loads((out / "ranking_publish_request.json").read_text())
        self.assertEqual(request["cache_activation"]["prior_cache_backup_path"], str(displaced))
        self.assertEqual(request["cache_activation"]["fixed_path"], str(fixed))
        with sqlite3.connect(fixed) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
        accepted = json.loads((out / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(accepted["source_watermark"]["activity"]["generation"], 2)
        self.assertFalse(prior.exists(), "retirement removes the recovered prior")
        self.assertFalse(displaced.exists(), "retirement removes the displaced installed cache")

    def test_installed_schema_two_cache_runs_the_candidate_lane_and_captures_its_generation(self):
        """An installed schema-two cache runs the private-candidate lane; the
        accepted capture reads only the newly completed generation of the
        installed file and stamps the pipeline versions."""
        self._install_candidate_layout(schema=2)
        self._install_candidate_stub()

        result = self._run()

        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        out = next((self.root / "data/eval-results").glob("cron-*"))
        manifest = json.loads((out / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(manifest["cache_schema"], 2)
        self.assertEqual(manifest["configuration"]["cache_lane"], "fresh_v2")
        self.assertEqual(manifest["universe"]["backfill_partial_wallets"], [])
        self.assertEqual(manifest["source_watermark"]["activity"]["generation"], 2)
        self.assertEqual(manifest["source_watermark"]["activity"]["count"], 2)
        self.assertRegex(manifest["source_watermark"]["activity"]["reference_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(
            manifest["source_watermark"]["activity"]["ranker_projection"]["classifier_version"], 2
        )
        self.assertEqual(manifest["source_watermark"]["resolution"]["generation"], 2)
        self.assertEqual(manifest["source_watermark"]["resolution"]["count"], 1)
        self.assertEqual(manifest["versions"]["activity_parser"], 2)
        self.assertEqual(manifest["versions"]["clob_resolution_schema"], 2)
        self.assertEqual(manifest["versions"]["ranker"], 1)

    def test_schema_two_prepares_then_activates_then_resumes_exact_request(self):
        """The corrected batch request is durable before cache activation and the
        same request is resumed only after the idempotent activation command."""
        side = self.root / "data" / "wallet_cache.side.db"
        with sqlite3.connect(side) as connection:
            connection.executescript(
                """
                PRAGMA user_version = 2;
                CREATE TABLE wallets (wallet_hex TEXT PRIMARY KEY, is_active INTEGER, is_infra INTEGER);
                CREATE VIEW active_tradeable_wallets AS
                    SELECT * FROM wallets WHERE is_active = 1 AND is_infra = 0;
                CREATE TABLE activity_coverage_manifests_v2 (
                    generation INTEGER PRIMARY KEY, cursors_json TEXT, completed_at_unix INTEGER,
                    reference_sha256 TEXT, wallet_count INTEGER, receipt_set_digest TEXT,
                    aggregate_digest TEXT, source_row_count INTEGER, collection_identity_json TEXT);
                CREATE TABLE activity_groups_v2 (
                    wallet_hex TEXT, source_time_unix INTEGER, activity_type TEXT,
                    coverage_generation INTEGER);
                CREATE TABLE clob_payout_coverage_manifests_v2 (
                    generation INTEGER PRIMARY KEY, terminal_kind TEXT, completed_at_unix INTEGER,
                    manifest_json TEXT, terminal_page_sha256 TEXT);
                CREATE TABLE clob_payout_evidence_v2 (
                    coverage_generation INTEGER, fetched_at_unix INTEGER);
                CREATE TABLE cache_v2_migration_state (
                    singleton INTEGER PRIMARY KEY, ranker_projection_count INTEGER,
                    ranker_projection_digest TEXT, ranker_classifier_version INTEGER);
                INSERT INTO wallets VALUES ('0xabc', 1, 0);
                INSERT INTO activity_coverage_manifests_v2 (generation, cursors_json, completed_at_unix, reference_sha256, wallet_count, receipt_set_digest, aggregate_digest, source_row_count) VALUES
                    (1, '[]', 20, 'aa', 1, 'bb', 'cc', 1);
                INSERT INTO activity_groups_v2 VALUES ('0xabc', 10, 'TRADE', 1);
                INSERT INTO clob_payout_coverage_manifests_v2 VALUES
                    (1, 'end_cursor', 30, '{}', 'dd');
                INSERT INTO clob_payout_evidence_v2 VALUES (1, 29);
                INSERT INTO cache_v2_migration_state VALUES (1, 1, 'ee', 1);
                """
            )
        stage = self.root / "data" / "cache-stage.json"
        stage.write_text(json.dumps({
            "cache_path": str(side.resolve()), "cache_sha256": "a" * 64,
        }))
        out = "data/eval-results/cron-20260905T000000Z"
        result = self._run(
            "--db", str(side), "--out-dir", out,
            "--cache-stage-record", str(stage),
            "--fixed-db", "data/wallet_cache.db",
            "--prior-cache-backup", "data/wallet_cache.prior.db",
            "--skip-discovery", "--skip-backfill", "--keep-intermediates",
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        push_lines = (self._log("push.log") or "").splitlines()
        self.assertIn("--snapshot-current", push_lines[0])
        self.assertIn("--prepare-only", push_lines[1])
        self.assertIn("--cache-stage-record", push_lines[1])
        self.assertIn("--validate-request", push_lines[2])
        self.assertIn("--resume-request", push_lines[3])
        bootstrap = (self._log("pe_bootstrap.log") or "").splitlines()
        operations = [line.split()[0] for line in bootstrap]
        self.assertEqual(
            operations,
            ["prices-history", "cache-finalize-v2", "cache-activate"],
        )
        print("PASS: schema-two request preparation precedes activation and exact resume")

    def test_parameterized_research_run_never_owns_production_pending_pointer(self):
        result = self._run("--skip-purge")
        self.assertEqual(result.returncode, 0, result.stderr)
        push = self._log("push.log") or ""
        self.assertIn("--request-file", push)
        self.assertNotIn("--pending-file", push)
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.pending").exists()
        )
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.cycle").exists()
        )

    def test_parameterized_run_ignores_existing_production_cycle_pointer(self):
        cycle_dir = self.root / "data" / "eval-results" / "cron-20260728T092155Z"
        cycle_dir.mkdir()
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        cycle.write_text(f"{cycle_dir.relative_to(self.root)}\n")
        research_out = self.root / "research-output"

        result = self._run("--skip-purge", "--out-dir", str(research_out))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(cycle.read_text().strip(), str(cycle_dir.relative_to(self.root)))
        self.assertIn(str(research_out), self._log("rank.log") or "")

    def test_cycle_pointer_validation_fails_closed_before_cache_mutation(self):
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        invalid_values = (
            "/tmp/cron-20260728T092155Z\n",
            "data/eval-results/not-cron\n",
            "data/eval-results/cron-20260728T092155Z\nextra\n",
            "data/eval-results/cron-20260728T092155Z\n",
        )
        for value in invalid_values:
            with self.subTest(value=value):
                if (self.root / "pe_bootstrap.log").exists():
                    (self.root / "pe_bootstrap.log").unlink()
                cycle.write_text(value)
                result = self._run()
                self.assertEqual(result.returncode, 2)
                self.assertIsNone(self._log("pe_bootstrap.log"))

    def test_cycle_pointer_symlink_fails_closed_before_cache_mutation(self):
        target_dir = self.root / "data" / "eval-results" / "cron-20260728T092155Z"
        target_dir.mkdir()
        target = self.root / "cycle-pointer-target"
        target.write_text(f"{target_dir.relative_to(self.root)}\n")
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        cycle.symlink_to(target)

        result = self._run()

        self.assertEqual(result.returncode, 2)
        self.assertIn("missing or not a regular file", result.stderr)
        self.assertIsNone(self._log("pe_bootstrap.log"))

    def test_success_compare_clear_preserves_changed_cycle_pointer(self):
        result = self._run(exit_env={"STUB_REPLACE_CYCLE": "1"})
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            cycle.read_text().strip(),
            "data/eval-results/cron-20990101T000000Z",
        )
        self.assertIn("cycle pointer changed; leaving it intact", result.stderr)

    def test_permanent_failure_preserves_cycle_pointer(self):
        result = self._run(exit_env={"STUB_EXIT_events": "1"})
        self.assertEqual(result.returncode, 1)
        cycle = self.root / "data" / "eval-results" / "rank_and_push.cycle"
        self.assertTrue(cycle.is_file())
        self.assertIn("cron-", cycle.read_text())

    def test_resume_rejects_pointer_outside_production_cron_directory(self):
        request = self.root / "outside.json"
        request.write_text("{}\n")
        pending = self.root / "data" / "eval-results" / "rank_and_push.pending"
        pending.write_text("outside.json\n")
        result = self._run("--resume-pending")
        self.assertEqual(result.returncode, 2)
        self.assertIn("escaped the production cron directory", result.stderr)
        self.assertIsNone(self._log("pe_bootstrap.log"))
        self.assertFalse(
            (self.root / "data" / "eval-results" / ".rank_and_push.lock").exists()
        )

    def test_resume_rejects_tampered_request_before_cache_activation(self):
        """PASS: the publisher validates publish_key before the wrapper invokes activation.
        FAIL: a modified activation tuple reaches pe-bootstrap."""
        request_dir = self.root / "data" / "eval-results" / "cron-tampered"
        request_dir.mkdir()
        request = request_dir / "ranking_publish_request.json"
        activation = {
            "side_path": "data/side.db",
            "fixed_path": "data/wallet_cache.db",
            "prior_cache_backup_path": "data/prior.db",
            "expected_sha256": "a" * 64,
        }
        identity = {
            "batch": {},
            "entries": [{"rank": 1, "wallet_hex": "0xabc"}],
            "cache_activation": activation,
        }
        payload = {
            "version": 1,
            "batch": identity["batch"],
            "entries": identity["entries"],
            "keep_batches": 1080,
            "cache_activation": activation,
            "publish_key": __import__("hashlib").sha256(
                json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
            ).hexdigest(),
        }
        payload["cache_activation"]["side_path"] = "data/tampered.db"
        request.write_text(json.dumps(payload) + "\n")
        pending = self.root / "data" / "eval-results" / "rank_and_push.pending"
        pending.write_text(
            "data/eval-results/cron-tampered/ranking_publish_request.json\n"
        )

        result = self._run("--resume-pending")
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIsNone(self._log("pe_bootstrap.log"))
        self.assertTrue(pending.is_file())
        print("PASS: tampered pending request is rejected before activation")

    def test_default_half_life_threaded_to_both_passes(self):
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stderr={r.stderr}")
        flag = f"--half-life-days {EXPECTED_DEFAULT_HALF_LIFE}"
        self.assertIn(flag, self._log("rank.log"), "pass-1 missing the default half-life")
        self.assertIn(flag, self._log("rerank.log"), "pass-2 missing the default half-life")
        print(f"PASS: production default half-life ({EXPECTED_DEFAULT_HALF_LIFE}) threaded to both passes")

    def test_run28_shape_threaded_to_stages(self):
        """run28 cutover (#417): TTR 48h + MinTRL-20 (per-month gates zeroed) reach both
        ranking passes, and the push receives the matching --ttr-max-secs provenance."""
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stderr={r.stderr}")
        for logname in ("rank.log", "rerank.log"):
            log = self._log(logname)
            self.assertIn("--min-trl 20", log, f"{logname} missing the MinTRL-20 gate")
            self.assertIn("--min-avg-per-month 0", log, f"{logname} per-month gate not zeroed")
            self.assertIn("--min-active-months 0", log, f"{logname} per-month gate not zeroed")
        self.assertIn("--ttr-hours 48", self._log("rank.log"), "pass-1 missing TTR 48h")
        self.assertIn("--ttr-max-secs 172800", self._log("push.log"),
                      "push missing the 48h ttr_max_secs provenance")
        print("PASS: run28 shape (ttr48 + trl20, per-month zeroed) threaded to rank/rerank/push")

    def test_missing_bootstrap_binary_is_fatal(self):
        (self.root / "target" / "release" / "pe-bootstrap").unlink()
        r = self._run()
        self.assertNotEqual(r.returncode, 0, "missing pe-bootstrap should be fatal")
        self.assertIn("pe-bootstrap", r.stderr)
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.cycle").exists(),
            "dependency preflight must fail before publishing a logical cycle",
        )
        self.assertEqual(
            list((self.root / "data" / "eval-results").glob("cron-*")),
            [],
            "dependency preflight must fail before creating the run directory",
        )
        print("PASS: missing pe-bootstrap binary → fatal before any stage")

    def test_automatic_physical_maintenance_is_absent(self):
        # Even failure-injected direct-command stubs must be unreachable from a
        # production publication. The static assertions catch hidden branches.
        r = self._run(
            exit_env={"STUB_EXIT_purge": "91", "STUB_EXIT_purge_infra": "92"}
        )
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        subs = [
            ln.split()[0]
            for ln in (self._log("pe_bootstrap.log") or "").splitlines()
            if ln.strip()
        ]
        self.assertNotIn("purge", subs)
        self.assertNotIn("purge-infra", subs)
        source = WRAPPER.read_text()
        self.assertNotIn('"$PE_BOOTSTRAP_BIN" purge', source)
        self.assertNotIn("wal_checkpoint", source)
        self.assertNotIn("incremental_vacuum", source)
        self.assertNotIn("VACUUM", source)
        self.assertIsNotNone(self._log("push.log"), "purge-free path did not publish")
        print("PASS: production publication performs no purge, reclamation, index rebuild, or checkpoint")

    def test_wrapper_never_opts_into_first_install_cache_creation(self):
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        bootstrap_log = self._log("pe_bootstrap.log")
        self.assertIsNotNone(bootstrap_log)
        self.assertNotIn("--create-cache", bootstrap_log)
        # As with the physical-maintenance guard above, inspect all branches,
        # including the supervisor's resume paths, beyond the executed cycle.
        for script in (WRAPPER, WRAPPER.with_name("rank_and_push_loop.sh")):
            self.assertNotIn("--create-cache", script.read_text(), str(script))
        print("PASS: wrapper and supervisor never opt into first-install cache creation")

    def test_skip_purge_remains_a_backward_compatible_noop(self):
        r = self._run("--skip-purge")
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        subs = [
            ln.split()[0]
            for ln in (self._log("pe_bootstrap.log") or "").splitlines()
            if ln.strip()
        ]
        self.assertNotIn("purge", subs)
        self.assertNotIn("purge-infra", subs)
        self.assertIsNotNone(self._log("push.log"), "legacy flag prevented publication")
        print("PASS: --skip-purge remains accepted while automatic purge stays retired")

    def test_auto_prune_removes_positions_csv(self):
        # After pass-2 consumes it, the multi-GB qualifying_positions_72hr.csv is pruned; the ranked
        # (purge decision/audit) and latency (push input) CSVs are kept.
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        crons = list((self.root / "data" / "eval-results").glob("cron-*"))
        self.assertEqual(len(crons), 1, f"expected one auto out-dir, got {crons}")
        out = crons[0]
        self.assertFalse((out / "qualifying_positions_72hr.csv").exists(),
                         "pass-1 positions intermediate was not auto-pruned")
        self.assertTrue((out / "ranked_72hr_buyandhold.csv").exists(),
                        "ranked CSV (purge decision/audit) must be kept")
        self.assertTrue((out / "latency_shift_ranked.csv").exists(),
                        "latency CSV (push input) must be kept")
        self.assertIn("Pruned pass-1 intermediate", r.stdout)
        print("PASS: auto-prune removes qualifying_positions_72hr.csv, keeps ranked + latency CSVs")

    def test_keep_intermediates_retains_positions(self):
        r = self._run("--keep-intermediates")
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        out = list((self.root / "data" / "eval-results").glob("cron-*"))[0]
        self.assertTrue((out / "qualifying_positions_72hr.csv").exists(),
                        "--keep-intermediates must retain the positions intermediate")
        self.assertNotIn("Pruned pass-1 intermediate", r.stdout)
        print("PASS: --keep-intermediates retains qualifying_positions_72hr.csv")

    def test_skip_rank_does_not_prune_positions(self):
        # A pure re-push didn't generate the positions file this run, so it must not be pruned.
        out = self.root / "data" / "eval-results" / "prior"
        out.mkdir()
        (out / "latency_shift_ranked.csv").write_text("wallet\n0xabc\n")
        (out / "qualifying_positions_72hr.csv").write_text("wallet,outcome_id\n0xabc,1\n")
        r = self._run("--skip-discovery", "--skip-backfill", "--skip-rank", "--out-dir", str(out))
        self.assertEqual(r.returncode, 0, f"stderr={r.stderr}")
        self.assertTrue((out / "qualifying_positions_72hr.csv").exists(),
                        "--skip-rank must not prune a positions file it did not generate")
        print("PASS: --skip-rank leaves a reused positions file untouched")

    # ── #544: retired purge stages impose no ionice dependency ──────────────────────
    def _install_ionice_stub(self):
        """PATH-prepended ionice stub: logs argv to ./ionice.log, then either exits with
        STUB_IONICE_EXIT_<subcommand> (subcommand = first arg after the pe-bootstrap
        path, dashes as underscores) or execs the wrapped command unchanged — so every
        downstream stub log stays byte-identical to an un-prefixed invocation."""
        bindir = self.root / "ionice-bin"
        bindir.mkdir(exist_ok=True)
        _write_exec(
            bindir / "ionice",
            "#!/usr/bin/env bash\n"
            'echo "$*" >> ionice.log\n'
            'if [[ "${1:-}" != "-c3" ]]; then echo "ionice-stub: expected -c3, got: $*" >&2; exit 64; fi\n'
            "shift\n"
            'sub="${2:-}"\n'
            'key="STUB_IONICE_EXIT_${sub//-/_}"\n'
            'code="${!key:-}"\n'
            'if [[ -n "$code" ]]; then exit "$code"; fi\n'
            'exec "$@"\n',
        )
        return {"PATH": f"{bindir}:{os.environ['PATH']}"}

    def _path_without_ionice(self):
        """A PATH mirroring every tool on the ambient PATH except ionice, so `command -v
        ionice` genuinely fails while everything else the wrapper needs still resolves."""
        bindir = self.root / "no-ionice-bin"
        bindir.mkdir(exist_ok=True)
        seen = set()
        for entry in os.environ.get("PATH", "").split(os.pathsep):
            directory = Path(entry)
            if not directory.is_dir():
                continue
            for tool in directory.iterdir():
                if tool.name in seen or tool.name == "ionice":
                    continue
                seen.add(tool.name)
                try:
                    (bindir / tool.name).symlink_to(tool)
                except OSError:
                    continue
        return {"PATH": str(bindir)}

    def test_publication_never_invokes_ionice(self):
        r = self._run(exit_env=self._install_ionice_stub())
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertEqual(
            (self._log("ionice.log") or "").splitlines(),
            [],
            "purge-free publication unexpectedly invoked ionice",
        )
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertNotIn("purge-infra", boot)
        self.assertNotIn("purge", boot)
        print("PASS: purge-free publication never invokes ionice")

    def test_missing_ionice_allows_normal_publication(self):
        r = self._run(exit_env=self._path_without_ionice())
        self.assertEqual(r.returncode, 0, f"missing ionice blocked publication\nstderr={r.stderr}")
        self.assertIsNotNone(self._log("push.log"))
        print("PASS: ionice is no longer a publication dependency")

    def test_missing_ionice_ok_for_pure_repush(self):
        out = self.root / "data" / "eval-results" / "prior"
        out.mkdir()
        (out / "latency_shift_ranked.csv").write_text("wallet\n0xabc\n")
        r = self._run(
            "--skip-discovery", "--skip-backfill", "--skip-rank", "--out-dir", str(out),
            exit_env=self._path_without_ionice(),
        )
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertIsNone(self._log("ionice.log"), "re-push invoked ionice")
        self.assertIsNotNone(self._log("push.log"), "re-push did not push")
        print("PASS: absent ionice allows a purge-free re-push")

    def test_ionice_failure_injection_cannot_reach_retired_infra_purge(self):
        env = self._install_ionice_stub()
        env["STUB_IONICE_EXIT_purge_infra"] = "9"
        r = self._run(exit_env=env)
        self.assertEqual(r.returncode, 0, f"retired infra purge was reached\nstderr={r.stderr}")
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertNotIn("purge-infra", boot, "infra purge ran directly despite ionice failure")
        self.assertIsNotNone(self._log("rank.log"))
        self.assertIsNotNone(self._log("push.log"))
        print("PASS: retired infra-purge injection cannot affect publication")

    def test_infra_purge_exit_injection_is_unreachable(self):
        env = self._install_ionice_stub()
        env["STUB_EXIT_purge_infra"] = "2"
        r = self._run(exit_env=env)
        self.assertEqual(r.returncode, 0, f"partial infra purge must not abort\nstderr={r.stderr}")
        self.assertNotIn("[purge-infra]", r.stderr)
        self.assertIsNotNone(self._log("rank.log"))
        self.assertIsNotNone(self._log("push.log"))
        print("PASS: direct purge-infra exit injection is unreachable from publication")

    def test_ordinary_purge_ionice_failure_injection_is_unreachable(self):
        env = self._install_ionice_stub()
        env["STUB_IONICE_EXIT_purge"] = "9"
        r = self._run(exit_env=env)
        self.assertEqual(r.returncode, 0, f"ordinary ionice failure must stay nonfatal\nstderr={r.stderr}")
        self.assertNotIn("[purge]", r.stderr)
        self.assertIsNotNone(self._log("push.log"), "publish did not complete")
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertNotIn("purge-infra", boot)
        self.assertNotIn("purge", boot)
        print("PASS: direct purge exit injection is unreachable from publication")

    def test_resume_pending_has_no_ionice_maintenance_tail(self):
        env = self._install_ionice_stub()
        first = self._run(exit_env={**env, "STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr)
        self.assertEqual(
            (self._log("ionice.log") or "").splitlines(),
            [],
            "failed-push run invoked retired physical maintenance",
        )
        resumed = self._run("--resume-pending", exit_env=env)
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertEqual(
            (self._log("ionice.log") or "").splitlines(),
            [],
            "resume invoked a retired physical-maintenance tail",
        )
        print("PASS: publication resume has no purge/ionice maintenance tail")

    def test_wrapper_passes_bash_syntax_check(self):
        r = subprocess.run(["bash", "-n", str(WRAPPER)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, f"bash -n failed: {r.stderr}")
        print("PASS: bash -n syntax check")



    # ── #588 schema-two private-candidate lane ───────────────────────────────────────
    V2_TABLES_SQL = """
        CREATE TABLE IF NOT EXISTS activity_coverage_manifests_v2 (
            generation INTEGER PRIMARY KEY, cursors_json TEXT, completed_at_unix INTEGER,
            reference_sha256 TEXT, wallet_count INTEGER, receipt_set_digest TEXT,
            aggregate_digest TEXT, source_row_count INTEGER, collection_identity_json TEXT);
        CREATE TABLE IF NOT EXISTS activity_groups_v2 (
            wallet_hex TEXT, source_time_unix INTEGER, activity_type TEXT,
            coverage_generation INTEGER, source_trade_id TEXT);
        CREATE TABLE IF NOT EXISTS clob_payout_walk_state_v2 (
            singleton INTEGER PRIMARY KEY, generation INTEGER);
        CREATE TABLE IF NOT EXISTS clob_payout_coverage_manifests_v2 (
            generation INTEGER PRIMARY KEY, terminal_kind TEXT, completed_at_unix INTEGER,
            manifest_json TEXT, terminal_page_sha256 TEXT);
        CREATE TABLE IF NOT EXISTS clob_payout_evidence_v2 (
            coverage_generation INTEGER, fetched_at_unix INTEGER);
        CREATE TABLE IF NOT EXISTS cache_v2_migration_state (
            singleton INTEGER PRIMARY KEY, ranker_projection_count INTEGER,
            ranker_projection_digest TEXT, ranker_classifier_version INTEGER,
            fresh_collection_json TEXT, phase TEXT DEFAULT 'schema_sealed',
            ranker_projection_inputs_json TEXT);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_groups_v2_source_trade_id
            ON activity_groups_v2(source_trade_id);
        CREATE TABLE IF NOT EXISTS activity_wallet_coverage_staging_v2 (generation INTEGER);
        CREATE TABLE IF NOT EXISTS cache_frozen_payload_verifications (activity_generation INTEGER);
        CREATE TABLE IF NOT EXISTS ranker_entries_v2 (source_trade_id TEXT);
    """

    def _install_candidate_layout(self, *, schema):
        """Physical fixed file with the two repository aliases (docs/26 §588 layout)."""
        phys = self.root / "phys"
        phys.mkdir()
        repo_db = self.root / "data" / "wallet_cache.db"
        fixed = phys / "wallet_cache.db"
        repo_db.rename(fixed)
        repo_db.symlink_to(fixed)
        (phys / "eval-results").symlink_to(self.root / "data" / "eval-results")
        (phys / "wallet_cache.db.lock").symlink_to(self.root / "data" / "wallet_cache.db.lock")
        with sqlite3.connect(fixed) as connection:
            connection.executescript(self.V2_TABLES_SQL)
            if schema == 2:
                connection.executescript(
                    """
                    PRAGMA user_version = 2;
                    DROP TABLE trades; DROP TABLE market_resolutions; DROP TABLE source_cursor;
                    INSERT INTO activity_coverage_manifests_v2 (generation, cursors_json, completed_at_unix, reference_sha256, wallet_count, receipt_set_digest, aggregate_digest, source_row_count) VALUES
                        (1, '[]', 20, 'fresh-1', 1, 'bb', 'cc', 1);
                    INSERT INTO activity_groups_v2 (wallet_hex, source_time_unix, activity_type, coverage_generation) VALUES ('0xabc', 10, 'TRADE', 1);
                    INSERT INTO clob_payout_coverage_manifests_v2 VALUES
                        (1, 'end_cursor', 30, '{}', 'dd');
                    INSERT INTO clob_payout_evidence_v2 VALUES (1, 29);
                    INSERT INTO cache_v2_migration_state (singleton, ranker_projection_count, ranker_projection_digest, ranker_classifier_version, fresh_collection_json) VALUES (1, 1, 'ee', 2, NULL);
                    """
                )
        return fixed

    def _install_candidate_stub(self, *, two_file=False):
        """Replace the logging bootstrap stub with one that emulates the cache
        owners' durable effects on the fixture files (copy, seal, collect,
        payout, finalize, activate) so the wrapper's orchestration is exercised
        against real files; exit injection keeps the STUB_EXIT_<sub> contract."""
        stub = self.root / "target" / "release" / "pe-bootstrap"
        body = stub.read_text()
        marker = 'sub="$1"\n'
        assert marker in body
        emulate = (
            'case "$1" in cache-stage-v2|cache-migrate-v2|cache-populate-activity-v2'
            '|cache-populate-payout-v2|cache-finalize-v2|cache-activate)\n'
            f'  {shlex.quote(sys.executable)} scripts/_stub_cache_ops.py "$@" || exit $?\n'
            'esac\n'
        )
        # Injected exits fire before any durable effect, like a refused owner.
        body = body.replace(marker, 'sub="$1"\nkey="STUB_EXIT_${sub//-/_}"\ncode="${!key:-0}"\n'
                            '[[ "$code" == 0 ]] || exit "$code"\n' + emulate)
        _write_exec(stub, body)
        _write_exec(
            self.root / "scripts" / "_stub_cache_ops.py",
            "#!/usr/bin/env python3\n"
            "import hashlib, json, os, re, shutil, sqlite3, sys, time\n"
            "from pathlib import Path\n"
            f"TWO_FILE = {two_file!r}\n"
            "a = sys.argv[1:]\n"
            "def opt(name):\n"
            "    return a[a.index(name) + 1] if name in a else None\n"
            "def sha(path):\n"
            "    return hashlib.sha256(open(path, 'rb').read()).hexdigest()\n"
            "def schema(path):\n"
            "    with sqlite3.connect(path) as c: return int(c.execute('PRAGMA user_version').fetchone()[0])\n"
            "now = int(time.time())\n"
            "sub, db = a[0], opt('--db')\n"
            "if sub == 'cache-stage-v2' and TWO_FILE:\n"
            "    prior, side, manifest = opt('--prior'), opt('--side'), opt('--manifest')\n"
            "    evidence_path = Path(side).with_suffix('.stage.json')\n"
            '    resumed = os.path.exists(side)\n'
            '    if evidence_path.exists():\n'
            '        evidence = json.loads(evidence_path.read_text())\n'
            '    else:\n'
            '        assert not resumed\n'
            "        with sqlite3.connect('file:' + db + '?mode=ro', uri=True) as c:\n"
            "            previous = c.execute('SELECT COALESCE(MAX(generation), 0) FROM activity_coverage_manifests_v2').fetchone()[0]\n"
            "            row = c.execute('SELECT fresh_collection_json FROM cache_v2_migration_state').fetchone()\n"
            '            identity = json.loads(row[0]) if row and row[0] else None\n'
            "            active = c.execute('SELECT generation FROM clob_payout_walk_state_v2').fetchone()\n"
            "            payout = active[0] if active else c.execute('SELECT COALESCE(MAX(generation), 0) + 1 FROM clob_payout_coverage_manifests_v2').fetchone()[0]\n"
            "        evidence = dict(version=1, fixed_path=db, prior_path=prior, side_path=side, displaced_path=side.replace('.side.db', '.displaced.db'), source_sha256=sha(db), source_size=os.path.getsize(db), source_schema=schema(db), activity_generation=previous, fresh_identity=identity, payout_generation=payout)\n"
            '        evidence_path.write_text(json.dumps(evidence))\n'
            '    if not resumed:\n'
            '        for p in Path(db).parent.iterdir():\n'
            "            assert not re.fullmatch(r'wallet_cache\\.cron-[0-9]{8}T[0-9]{6}Z\\.(prior|displaced)\\.db', p.name), 'previous cycle backup remains'\n"
            '        shutil.copyfile(db, side)\n'
            "    if evidence['source_schema'] != 2 and manifest and not os.path.exists(manifest):\n"
            "        json.dump({'manifest_version': 1, 'backup_sha256': evidence['source_sha256'], 'source_bounds': {}, 'cursors': {}, 'hashes': {}, 'sealed_at_unix': now}, open(manifest, 'w'))\n"
            "    print(json.dumps({'prior_path': prior, 'side_path': side, 'prior_schema': evidence['source_schema'], 'side_schema': schema(side), 'prior_sha256': evidence['source_sha256'], 'side_sha256': None if resumed else sha(side), 'resumed': resumed}))\n"
            "elif sub == 'cache-stage-v2':\n"
            "    prior, side, manifest = opt('--prior'), opt('--side'), opt('--manifest')\n"
            "    if os.path.exists(side):\n"
            "        assert os.path.exists(prior), 'candidate without prior'\n"
            "        assert schema(side) != -2, 'unfinished bulk root must bypass staging'\n"
            "        print(json.dumps({'prior_path': prior, 'side_path': side, 'prior_schema': schema(prior), 'side_schema': schema(side), 'prior_sha256': None, 'side_sha256': None, 'resumed': True}))\n"
            "        raise SystemExit(0)\n"
            "    if not os.path.exists(prior):\n"
            "        shutil.copyfile(db, prior)\n"
            "        if os.environ.get('STUB_PRIOR_SCHEMA') == '2':\n"
            "            with sqlite3.connect(prior) as c:\n"
            "                c.executescript('PRAGMA user_version = 2; DROP TABLE trades; DROP TABLE market_resolutions; DROP TABLE source_cursor;')\n"
            "                recorded = {'version': 2, 'generation': 1, 'fixed_end_unix': now - int(os.environ.get('STUB_PRIOR_AGE', '100000')), 'wallets': ['0xabc'], 'base_generation': None, 'base_manifest_sha256': None, 'start_exclusive': 0, 'full_read_wallets': ['0xabc']}\n"
            "                recorded['digest'] = hashlib.sha256(json.dumps(recorded, sort_keys=True, separators=(',', ':')).encode()).hexdigest()\n"
            "                c.execute('INSERT INTO activity_groups_v2 (wallet_hex, source_time_unix, activity_type, coverage_generation) VALUES (?, ?, ?, ?)', ('0xabc', recorded['fixed_end_unix'], 'TRADE', 1))\n"
            "                c.execute('INSERT INTO activity_coverage_manifests_v2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (1, '{}', now, recorded['digest'], 1, 'bb', 'cc', 1, json.dumps(recorded)))\n"
            "                c.execute('INSERT OR IGNORE INTO cache_v2_migration_state (singleton, ranker_projection_count, ranker_projection_digest, ranker_classifier_version, fresh_collection_json) VALUES (1, NULL, NULL, NULL, NULL)')\n"
            "                c.execute('UPDATE cache_v2_migration_state SET fresh_collection_json = ?', (json.dumps(recorded),))\n"
            "    shutil.copyfile(prior, side)\n"
            "    if schema(prior) != 2 and manifest and not os.path.exists(manifest):\n"
            "        json.dump({'manifest_version': 1, 'backup_sha256': sha(prior), 'source_bounds': {}, 'cursors': {}, 'hashes': {}, 'sealed_at_unix': now}, open(manifest, 'w'))\n"
            "    print(json.dumps({'prior_path': prior, 'side_path': side, 'prior_schema': schema(prior), 'side_schema': schema(side), 'prior_sha256': sha(prior), 'side_sha256': sha(side), 'resumed': False}))\n"
            "elif sub == 'cache-migrate-v2':\n"
            "    assert schema(db) != -2, 'unfinished bulk root must bypass migration'\n"
            "    manifest = json.load(open(opt('--manifest')))\n"
            "    if schema(db) != 2:\n"
            "        assert manifest['backup_sha256'] == sha(db), 'build manifest is not bound to the candidate bytes'\n"
            "        with sqlite3.connect(db) as c:\n"
            "            c.executescript('PRAGMA user_version = 2; DROP TABLE trades; DROP TABLE market_resolutions; DROP TABLE source_cursor;')\n"
            "            c.execute('INSERT OR IGNORE INTO cache_v2_migration_state (singleton, ranker_projection_count, ranker_projection_digest, ranker_classifier_version, fresh_collection_json) VALUES (1, NULL, NULL, NULL, NULL)')\n"
            "    print(json.dumps({'resumed': True}))\n"
            "elif sub == 'cache-populate-activity-v2':\n"
            "    generation = int(opt('--fresh-generation'))\n"
            "    with sqlite3.connect(db) as c:\n"
            "        row = c.execute('SELECT fresh_collection_json FROM cache_v2_migration_state').fetchone()\n"
            "        recorded = json.loads(row[0]) if row and row[0] else None\n"
            "        if recorded is None or recorded['generation'] != generation:\n"
            "            base = recorded['generation'] if recorded else None\n"
            "            end = now - int(os.environ.get('STUB_ACTIVITY_AGE_' + str(generation), '120'))\n"
            "            recorded = {'version': 2, 'generation': generation, 'fixed_end_unix': end, 'wallets': ['0xabc'], 'base_generation': base, 'base_manifest_sha256': 'a'*64 if base else None, 'start_exclusive': recorded['fixed_end_unix'] if base else 0, 'full_read_wallets': [] if base else ['0xabc']}\n"
            "            recorded['digest'] = hashlib.sha256(json.dumps(recorded, sort_keys=True, separators=(',', ':')).encode()).hexdigest()\n"
            "            c.execute('UPDATE cache_v2_migration_state SET fresh_collection_json = ?, ranker_projection_count = NULL, ranker_projection_digest = NULL, ranker_classifier_version = NULL', (json.dumps(recorded),))\n"
            "        if '--bulk-root' in a:\n"
            "            assert generation == 1 and recorded['base_generation'] is None\n"
            "            assert opt('--fixed-db') and (opt('--prior') or Path(db).with_suffix('.stage.json').exists())\n"
            "            c.execute('DROP INDEX IF EXISTS idx_activity_groups_v2_source_trade_id')\n"
            "            c.execute('PRAGMA user_version = -2')\n"
            "        else: assert schema(db) != -2, 'fenced resume needs --bulk-root'\n"
            "        if os.environ.get('STUB_FAIL_ACTIVITY_GENERATION') == str(generation): c.commit(); raise SystemExit(75)\n"
            "        if not c.execute('SELECT 1 FROM activity_coverage_manifests_v2 WHERE generation = ?', (generation,)).fetchone():\n"
            "            c.execute('UPDATE activity_groups_v2 SET coverage_generation = ?', (generation,))\n"
            "            c.execute('INSERT INTO activity_groups_v2 (wallet_hex, source_time_unix, activity_type, coverage_generation) VALUES (?, ?, ?, ?)', ('0xabc', recorded['fixed_end_unix'], 'TRADE', generation))\n"
            "            c.execute('INSERT INTO activity_coverage_manifests_v2 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (generation, '{}', now, recorded['digest'], 1, 'bb', 'cc', 1, json.dumps(recorded)))\n"
            "        if '--bulk-root' in a:\n"
            "            c.execute('CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id)')\n"
            "            c.execute('PRAGMA user_version = 2')\n"
            "    print(json.dumps({'generation': generation}))\n"
            "elif sub == 'cache-populate-payout-v2':\n"
            "    with sqlite3.connect(db) as c:\n"
            "        active = c.execute('SELECT generation FROM clob_payout_walk_state_v2 WHERE singleton = 1').fetchone()\n"
            "        generation = int(active[0]) if active else int(c.execute('SELECT COALESCE(MAX(generation), 0) + 1 FROM clob_payout_coverage_manifests_v2').fetchone()[0])\n"
            "        c.execute('DELETE FROM clob_payout_walk_state_v2')\n"
            "        c.execute('INSERT INTO clob_payout_coverage_manifests_v2 VALUES (?, ?, ?, ?, ?)', (generation, 'end_cursor', now, '{}', 'dd'))\n"
            "        c.execute('DELETE FROM clob_payout_evidence_v2'); c.execute('INSERT INTO clob_payout_evidence_v2 VALUES (?, ?)', (generation, now))\n"
            "    print(json.dumps({'generation': generation}))\n"
            "elif sub == 'cache-finalize-v2':\n"
            "    with sqlite3.connect(db) as c:\n"
            "        generation = json.loads(c.execute('SELECT fresh_collection_json FROM cache_v2_migration_state').fetchone()[0])['generation']\n"
            "        c.execute('INSERT OR IGNORE INTO activity_coverage_manifests_v2 (generation, cursors_json, completed_at_unix, reference_sha256, wallet_count, receipt_set_digest, aggregate_digest, source_row_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (generation, '[]', now, f'fresh-{generation}', 1, 'bb', 'cc', 1))\n"
            "        c.execute('UPDATE cache_v2_migration_state SET ranker_projection_count = 1, ranker_projection_digest = ?, ranker_classifier_version = 2', (f'digest-{generation}',))\n"
            "    json.dump({'cache_path': os.path.abspath(db), 'cache_sha256': sha(db)}, open(opt('--stage-record'), 'w'))\n"
            "elif sub == 'cache-activate':\n"
            "    fixed, backup = opt('--fixed-db'), opt('--backup')\n"
            "    evidence_path = Path(db).with_suffix('.stage.json')\n"
            "    record = opt('--final-stage-record')\n"
            "    if record: assert json.load(open(record))['cache_sha256'] == opt('--expected-sha256'), 'final-stage record describes other bytes'\n"
            '    if os.path.exists(db):\n'
            "        assert sha(db) == opt('--expected-sha256'), 'side hash changed after finalization'\n"
            '        if evidence_path.exists():\n'
            "            assert sha(evidence_path) == opt('--stage-evidence-sha256')\n"
            '            evidence = json.loads(evidence_path.read_text())\n'
            '            if os.path.exists(fixed):\n'
            '                assert not os.path.exists(backup)\n'
            "                assert sha(fixed) == evidence['source_sha256']\n"
            '                os.rename(fixed, backup)\n'
            '            else:\n'
            "                assert sha(backup) == evidence['source_sha256']\n"
            '            os.rename(db, fixed)\n'
            '        else:\n'
            '            if os.path.exists(backup): assert sha(backup) == sha(fixed), "existing prior-cache backup differs from the fixed cache"\n'
            '            else: shutil.copyfile(fixed, backup)\n'
            '            os.replace(db, fixed)\n'
            "    print(json.dumps({'installed': fixed}))\n",
        )

    def test_two_file_pending_resume_finishes_activation_gap_before_cache_openers(self):
        """Proves both loop resume entries reach activation with F absent and preserve exact request bytes."""
        for args in [(), ("--resume-pending",)]:
            with self.subTest(args=args):
                self.tearDown(); self.setUp()
                fixed = self._prepare_incremental_fixture(two_file=True)
                prepared = self._run()
                self.assertEqual(prepared.returncode, 2, prepared.stderr)
                pending = self.root / "data/eval-results/rank_and_push.pending"
                request_path = self.root / pending.read_text().strip()
                request_bytes = request_path.read_bytes()
                binding = json.loads(request_bytes)["cache_activation"]
                side, displaced = Path(binding["side_path"]), Path(binding["prior_cache_backup_path"])
                old_bytes = fixed.read_bytes()
                fixed.rename(displaced)
                self.assertFalse(fixed.exists())
                self.assertEqual(len(list(fixed.parent.glob("*.db"))), 2)
                before = self._bootstrap_ops()
                with (self.root / ".env").open("a") as handle:
                    handle.write("PE_RANK_SCHEMA_TWO_CUTOVER=1\n")
                record = request_path.parent / "cache_stage_record.json"
                self.assertTrue(record.is_file())
                if args:
                    record.unlink()  # a resume without the record recomputes the digest
                resumed = self._run(*args, exit_env={"STUB_PUSH_EXIT": "75"})
                self.assertEqual(resumed.returncode, 75, resumed.stderr + resumed.stdout)
                self.assertEqual(self._bootstrap_ops()[len(before):], ["cache-activate"])
                self.assertFalse(side.exists())
                self.assertEqual(displaced.read_bytes(), old_bytes)
                self.assertEqual(request_path.read_bytes(), request_bytes)
                self.assertIn("--stage-evidence-sha256", self._bootstrap_lines("cache-activate")[-1])
                self.assertEqual(
                    "--final-stage-record" in self._bootstrap_lines("cache-activate")[-1], not args
                )
                self.assertEqual(len(list(fixed.parent.glob("*.db"))), 2)
                activations = len(self._bootstrap_lines("cache-activate"))
                completed = self._run("--resume-pending")
                self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
                final = self._bootstrap_lines("cache-activate")
                self.assertGreater(len(final), activations)
                self.assertEqual("--final-stage-record" in final[-1], not args)
                self.assertFalse(displaced.exists())
                self.assertFalse(pending.exists())
                self.assertEqual(len(list(fixed.parent.glob("*.db"))), 1)

    def test_schema_two_export_failure_stops_before_ranking(self):
        """Proves a failed schema-two export never ranks whatever older export remains."""
        self._prepare_incremental_fixture(two_file=True)
        failed = self._run(exit_env={"STUB_EXIT_export": "1"})
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("FATAL: the Parquet export failed", failed.stderr)
        self.assertTrue(self._log("export.log"))
        self.assertFalse(self._log("rank.log"))

    def test_two_file_staging_refuses_previous_backup_before_candidate_allocation(self):
        """Proves skipped retirement blocks another full candidate allocation without deleting evidence."""
        fixed = self._prepare_incremental_fixture(two_file=True)
        old = fixed.parent / "wallet_cache.cron-20000101T000000Z.displaced.db"
        old.write_bytes(b"retained rollback cache")
        original = fixed.read_bytes()
        refused = self._run()
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("previous cycle backup remains", refused.stderr)
        self.assertEqual(old.read_bytes(), b"retained rollback cache")
        self.assertEqual(fixed.read_bytes(), original)
        self.assertEqual(list(fixed.parent.glob("*.side.db")), [])

    def _bootstrap_ops(self):
        return [line.split()[0] for line in (self._log("pe_bootstrap.log") or "").splitlines()]

    def _bootstrap_lines(self, op):
        return [line for line in (self._log("pe_bootstrap.log") or "").splitlines()
                if line.startswith(op + " ")]

    def _prepare_incremental_fixture(self, *, two_file=False):
        fixed = self._install_candidate_layout(schema=1)
        self._install_candidate_stub(two_file=two_file)
        with (self.root / ".env").open("a") as handle:
            handle.write("PE_RANK_SCHEMA_TWO_CUTOVER=prepare\n")
        return fixed

    def test_stale_initial_head_gets_one_top_up_then_prepares(self):
        fixed = self._prepare_incremental_fixture()
        before = fixed.read_bytes()
        result = self._run(exit_env={"STUB_ACTIVITY_AGE_1": "90000"})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", result.stdout)
        lines = self._bootstrap_lines("cache-populate-activity-v2")
        self.assertEqual(len(lines), 2)
        self.assertIn("--fresh-generation 1", lines[0])
        self.assertIn("--fresh-generation 2", lines[1])
        self.assertIn("--bulk-root", lines[0])
        self.assertIn(f"--fixed-db {fixed}", lines[0])
        self.assertIn("--prior", lines[0])
        self.assertNotIn("--bulk-root", lines[1])
        self.assertEqual(fixed.read_bytes(), before)
        self.assertTrue((self.root / "data/eval-results/rank_and_push.pending").is_file())

    def test_interrupted_top_up_resumes_recorded_head_and_stale_top_up_never_gets_third(self):
        self._prepare_incremental_fixture()
        first = self._run(exit_env={"STUB_ACTIVITY_AGE_1": "180000", "STUB_FAIL_ACTIVITY_GENERATION": "2"})
        self.assertEqual(first.returncode, 75, first.stderr)
        before = len(self._bootstrap_lines("cache-populate-activity-v2"))
        resumed = self._run()
        self.assertEqual(resumed.returncode, 2, resumed.stderr)
        self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", resumed.stdout)
        self.assertEqual(len(self._bootstrap_lines("cache-populate-activity-v2")), before + 1)
        self.assertIn("--fresh-generation 2", self._bootstrap_lines("cache-populate-activity-v2")[-1])
        self.assertNotIn("--bulk-root", self._bootstrap_lines("cache-populate-activity-v2")[-1])

    def test_bulk_root_transient_exit_resumes_without_repeating_setup(self):
        fixed = self._prepare_incremental_fixture()
        before = fixed.read_bytes()
        first = self._run(exit_env={"STUB_FAIL_ACTIVITY_GENERATION": "1"})
        self.assertEqual(first.returncode, 75, first.stderr)
        side = next(fixed.parent.glob("*.side.db"))
        with sqlite3.connect(side) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], -2)
        pointer = self.root / "data/eval-results/rank_and_push.cycle"
        cycle = pointer.read_bytes()
        operations = self._bootstrap_ops()
        resumed = self._run()
        self.assertEqual(resumed.returncode, 2, resumed.stderr)
        self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", resumed.stdout)
        self.assertEqual(pointer.read_bytes(), cycle)
        self.assertEqual(fixed.read_bytes(), before)
        self.assertEqual(self._bootstrap_ops()[len(operations):], [
            "cache-populate-activity-v2", "cache-populate-payout-v2", "cache-finalize-v2", "prices-history",
            "cache-finalize-v2",
        ])
        for line in self._bootstrap_lines("cache-populate-activity-v2"):
            self.assertIn("--bulk-root", line)
            self.assertIn("--fresh-generation 1", line)

    def test_interrupted_ordinary_root_with_rows_resumes_without_bulk_flag(self):
        fixed = self._prepare_incremental_fixture()
        first = self._run(exit_env={"STUB_FAIL_ACTIVITY_GENERATION": "1"})
        self.assertEqual(first.returncode, 75, first.stderr)
        side = next(fixed.parent.glob("*.side.db"))
        # Represent a pre-existing ordinary indexed root, with its frozen identity.
        with sqlite3.connect(side) as c:
            c.executescript("""PRAGMA user_version=2;
                CREATE UNIQUE INDEX idx_activity_groups_v2_source_trade_id ON activity_groups_v2(source_trade_id);
                INSERT INTO activity_groups_v2(source_trade_id, coverage_generation) VALUES ('ordinary', 1);""")
        resumed = self._run()
        self.assertEqual(resumed.returncode, 2, resumed.stderr)
        self.assertNotIn("--bulk-root", self._bootstrap_lines("cache-populate-activity-v2")[-1])

    def test_cutover_distinguishes_bulk_recovery_from_schema_mismatch(self):
        fixed = self._prepare_incremental_fixture()
        prepared = self._run()
        self.assertEqual(prepared.returncode, 2, prepared.stderr)
        out = self.root / (self.root / "data/eval-results/rank_and_push.cycle").read_text().strip()
        side = next(fixed.parent.glob("*.side.db"))
        prior = next(fixed.parent.glob("*.prior.db"))
        (self.root / "data/eval-results/rank_and_push.pending").unlink()
        for version, message in ((-2, "resume cache-populate-activity-v2 --bulk-root before cutover"),
                                 (1, "has schema 1, expected PRAGMA user_version=2")):
            with self.subTest(version=version):
                with sqlite3.connect(side) as c:
                    c.execute(f"PRAGMA user_version={version}")
                result = self._run("--db", str(side), "--out-dir", str(out),
                    "--skip-discovery", "--skip-backfill", "--skip-rank", "--skip-export",
                    "--cache-stage-record", str(out / "cache_stage_record.json"),
                    "--fixed-db", str(fixed), "--prior-cache-backup", str(prior))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)

    def test_manually_completed_top_up_is_adopted_and_uses_exact_freshness_boundary(self):
        self._prepare_incremental_fixture()
        first = self._run(exit_env={"STUB_ACTIVITY_AGE_1": "180000", "STUB_FAIL_ACTIVITY_GENERATION": "2"})
        self.assertEqual(first.returncode, 75, first.stderr)
        cycle = self.root / (self.root / "data/eval-results/rank_and_push.cycle").read_text().strip()
        stage = json.loads((cycle / "cache_stage.json").read_text())
        side = Path(stage["side_path"])
        prior = Path(stage["prior_path"])
        manual = subprocess.run([str(self.root / "target/release/pe-bootstrap"), "cache-populate-activity-v2", "--db", str(side), "--fresh-generation", "2"], cwd=self.root, capture_output=True, text=True)
        self.assertEqual(manual.returncode, 0, manual.stderr)
        # Keep the real owner isolated from this suite's importable stub modules.
        check = subprocess.run([sys.executable, "-c", """
import hashlib, json, sqlite3, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from rank_cycle_manifest import candidate_targets
prior, side = map(Path, sys.argv[2:])
with sqlite3.connect(side) as connection:
    raw = connection.execute("SELECT fresh_collection_json FROM cache_v2_migration_state").fetchone()[0]
identity = json.loads(raw)
end = identity["fixed_end_unix"]
assert candidate_targets(prior, side, after_collection=True, now=end + 24 * 3600, max_staleness_hours=24)[0] == 2
try:
    candidate_targets(prior, side, after_collection=True, now=end + 24 * 3600 + 1, max_staleness_hours=24)
except ValueError as error:
    assert "top-up is stale" in str(error)
else:
    raise AssertionError("stale top-up accepted")
try:
    identity.update(generation=3, base_generation=2)
    identity.pop("digest")
    identity["digest"] = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    with sqlite3.connect(side) as connection:
        connection.execute("UPDATE cache_v2_migration_state SET fresh_collection_json = ?", (json.dumps(identity),))
    try:
        candidate_targets(prior, side)
    except ValueError as error:
        assert "single top-up allowance" in str(error)
    else:
        raise AssertionError("unrelated third head accepted")
finally:
    with sqlite3.connect(side) as connection:
        connection.execute("UPDATE cache_v2_migration_state SET fresh_collection_json = ?", (raw,))
""", str(WRAPPER.parent), str(prior), str(side)], capture_output=True, text=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        before = len(self._bootstrap_lines("cache-populate-activity-v2"))
        resumed = self._run()
        self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", resumed.stdout, resumed.stderr)
        self.assertEqual(len(self._bootstrap_lines("cache-populate-activity-v2")), before + 1)
        self.assertIn("--fresh-generation 2", self._bootstrap_lines("cache-populate-activity-v2")[-1])

    def test_stale_completed_top_up_stops_before_ranking_on_every_restart(self):
        self._prepare_incremental_fixture()
        for _ in range(2):
            result = self._run(exit_env={"STUB_ACTIVITY_AGE_1": "180000", "STUB_ACTIVITY_AGE_2": "90000"})
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("RANK_AND_PUSH_PREPARED_ONLY=", result.stdout)
            self.assertIn("top-up is stale", result.stderr)
        self.assertFalse(any("--fresh-generation 3" in line for line in self._bootstrap_lines("cache-populate-activity-v2")))
        self.assertIsNone(self._log("rank.log"))
        self.assertFalse((self.root / "data/eval-results/rank_and_push.pending").exists())

    def test_durable_request_without_pointer_recovers_before_discovery_for_both_entries(self):
        fixed = self._prepare_incremental_fixture()
        prepared = self._run()
        self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", prepared.stdout, prepared.stderr)
        before = self._bootstrap_ops()
        fixed_bytes = fixed.read_bytes()
        pending = self.root / "data/eval-results/rank_and_push.pending"
        request = pending.read_text()
        for args in [(), ("--resume-pending",)]:
            pending.unlink()
            result = self._run(*args)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("RANK_AND_PUSH_RECOVERED_REQUEST=", result.stdout)
            self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", result.stdout)
            self.assertEqual(pending.read_text(), request)
            self.assertEqual(self._bootstrap_ops(), before)
            self.assertEqual(fixed.read_bytes(), fixed_bytes)

    def test_missing_pointer_recovery_publishes_and_runs_retention_for_both_entries(self):
        for args in [(), ("--resume-pending",)]:
            with self.subTest(args=args):
                self.tearDown(); self.setUp()
                fixed = self._prepare_incremental_fixture()
                older = self._seed_old_cache_copies(fixed.parent)
                prepared = self._run()
                self.assertEqual(prepared.returncode, 2, prepared.stderr)
                self._assert_cache_copies(older)
                pending = self.root / "data/eval-results/rank_and_push.pending"
                cycle = self.root / Path(pending.read_text().strip()).parent
                before = self._bootstrap_ops()
                pending.unlink()
                with (self.root / ".env").open("a") as handle:
                    handle.write("PE_RANK_SCHEMA_TWO_CUTOVER=1\n")
                result = self._run(*args)
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertIn("RANK_AND_PUSH_RECOVERED_REQUEST=", result.stdout)
                self.assertEqual(self._bootstrap_ops()[len(before):], ["cache-activate"])
                accepted = json.loads((cycle / "accepted_cycle_manifest.json").read_text())
                self.assertEqual(accepted["cache_schema"], 2)
                self.assertFalse(pending.exists())
                self.assertFalse((self.root / "data/eval-results/rank_and_push.cycle").exists())
                self.assertTrue(all(not path.exists() for path in older))
                self.assertIn("[cache-retention] deleted", result.stdout)
                self.assertFalse((fixed.parent / f"wallet_cache.{cycle.name}.prior.db").exists())

    def _seed_old_cache_copies(self, physical):
        copies = {}
        for role in ("prior", "side", "displaced"):
            for suffix in ("", "-wal", "-shm"):
                path = physical / f"wallet_cache.cron-20000101T000000Z.{role}.db{suffix}"
                copies[path] = f"old {role}{suffix}".encode()
                path.write_bytes(copies[path])
        return copies

    def _assert_cache_copies(self, copies):
        for path, content in copies.items():
            self.assertEqual(path.read_bytes(), content, str(path))

    def test_completed_cycle_retention_is_exact_and_repeatable(self):
        fixed = self._install_candidate_layout(schema=2)
        self._install_candidate_stub()
        older = self._seed_old_cache_copies(fixed.parent)
        kept = {}
        for name in ("wallet_cache.db.bak", "wallet_cache.cron-old.prior.db",
                     "wallet_cache.cron-20000101T000000Z.prior.db.pending",
                     "wallet_cache.cron-20000101T000000Z.prior.db-journal",
                     "wallet_cache.cron-20000101T000000Z.side.db.lock",
                     "wallet_cache..prior.db"):
            path = fixed.parent / name
            kept[path] = b"unrelated"
            path.write_bytes(kept[path])
        outside = self.root / "outside.db"
        outside.write_bytes(b"outside target")
        link = fixed.parent / "wallet_cache.cron-20000102T000000Z.prior.db"
        link.symlink_to(outside)
        alias = fixed.parent / "wallet_cache.cron-20000102T000000Z.side.db"
        # Link the installed inode after activation below: it must never be deleted.
        directory = fixed.parent / "wallet_cache.cron-20000102T000000Z.displaced.db"
        directory.mkdir()
        (directory / "keep").write_bytes(b"directory")

        first = self._run(exit_env={"STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr + first.stdout)
        self._assert_cache_copies(older)
        pending = self.root / "data/eval-results/rank_and_push.pending"
        pointer_bytes = pending.read_bytes()
        out = self.root / Path(pointer_bytes.decode().strip()).parent
        for role in ("prior", "side", "displaced"):
            for suffix in ("", "-wal", "-shm"):
                if role == "side" and not suffix:
                    continue  # Activation consumes the candidate main before retention.
                path = fixed.parent / f"wallet_cache.{out.name}.{role}.db{suffix}"
                if not path.exists():
                    path.write_bytes(b"current cycle")
                older[path] = path.read_bytes()
        resumed = self._run("--resume-pending")
        self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
        self.assertTrue(all(not path.exists() for path in older))
        for path, content in older.items():
            self.assertIn(f"deleted {path} freed_size_bytes={len(content)}", resumed.stdout)
        self._assert_cache_copies(kept)
        self.assertTrue(link.is_symlink())
        self.assertEqual(outside.read_bytes(), b"outside target")
        self.assertEqual((directory / "keep").read_bytes(), b"directory")
        self.assertTrue(fixed.is_file())

        os.link(fixed, alias)
        # Replay the exact completed publication to exercise the cleanup tail twice.
        pending.write_bytes(pointer_bytes)
        repeated = self._run("--resume-pending")
        self.assertEqual(repeated.returncode, 0, repeated.stderr + repeated.stdout)
        self.assertIn("[cache-retention] nothing deleted", repeated.stdout)
        self.assertNotIn("[cache-retention] deleted", repeated.stdout)
        self.assertTrue(alias.samefile(fixed))
        self._assert_cache_copies(kept)
        self.assertEqual(outside.read_bytes(), b"outside target")

    def test_retention_holds_all_files_when_recovery_or_pause_records_remain(self):
        for guard in ("cycle", "pending", "pause", "pause_symlink"):
            with self.subTest(guard=guard):
                self.tearDown(); self.setUp()
                fixed = self._install_candidate_layout(schema=2)
                self._install_candidate_stub()
                older = self._seed_old_cache_copies(fixed.parent)
                env = {}
                if guard == "cycle":
                    env["STUB_REPLACE_CYCLE"] = "1"
                elif guard == "pending":
                    # Keep the injected exit under __main__: candidate-targets
                    # imports this module for the real publisher defaults.
                    # Simulate a newer pointer arriving during successful publication.
                    publisher = self.root / "scripts/push_ranking_to_supabase.py"
                    body = publisher.read_text().replace(
                        'raise SystemExit(int(os.environ.get("STUB_PUSH_EXIT", "0")))',
                        'Path("data/eval-results/rank_and_push.pending").write_text("changed\\n")\n'
                        '    raise SystemExit(0)',
                    )
                    publisher.write_text(body)
                else:
                    pause = self.root / "data/eval-results/.forge_pause.json"
                    if guard == "pause_symlink":
                        pause.symlink_to(self.root / "missing-pause-record")
                    else:
                        pause.write_text("malformed records hold retention too")
                result = self._run(exit_env=env)
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertIn("[cache-retention] nothing deleted: recovery pointer or Forge pause record remains", result.stdout)
                self._assert_cache_copies(older)

    def test_paused_publication_retirement_resumes_before_watermark_and_next_cycle(self):
        for two_file, args in ((True, ()), (True, ("--resume-pending",)), (False, ())):
            with self.subTest(two_file=two_file, args=args):
                self.tearDown(); self.setUp()
                fixed = self._install_candidate_layout(schema=2)
                self._install_candidate_stub(two_file=two_file)
                pause = self.root / "data/eval-results/.forge_pause.json"
                pause.write_text("presence holds cleanup, including malformed records")
                published = self._run()
                self.assertEqual(published.returncode, 0, published.stderr + published.stdout)
                out = next((self.root / "data/eval-results").glob("cron-*"))
                request = json.loads((out / "ranking_publish_request.json").read_text())
                backup = Path(request["cache_activation"]["prior_cache_backup_path"])
                old_bytes = backup.read_bytes()
                installed = fixed.read_bytes()
                evidence = {path: path.read_bytes() for path in (
                    out / "ranking_publish_request.json", out / "accepted_cycle_manifest.json")}
                if two_file:
                    stage = fixed.parent / f"wallet_cache.{out.name}.side.stage.json"
                    evidence[stage] = stage.read_bytes()
                ops = self._bootstrap_ops()
                pushes = self._log("push.log")
                for name in ("rank_and_push.pending", "rank_and_push.cycle"):
                    self.assertFalse((out.parent / name).exists())

                held = self._run(*args)
                self.assertEqual(held.returncode, 0, held.stderr + held.stdout)
                self.assertIn("Forge pause record remains", held.stdout)
                self.assertEqual(backup.read_bytes(), old_bytes)
                self.assertEqual(self._bootstrap_ops(), ops)
                pause.unlink()
                resumed = self._run(*args)
                self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
                self.assertFalse(backup.exists())
                self.assertEqual(fixed.read_bytes(), installed)
                self.assertEqual(self._bootstrap_ops(), ops)
                self.assertEqual(self._log("push.log"), pushes, "cleanup republished the request")
                self._assert_cache_copies(evidence)
                if not args:
                    self.assertLess(resumed.stdout.index("[cache-retention] deleted"),
                                    resumed.stdout.index("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1"))

                # Advance the installed watermark: the following cycle must stage
                # successfully without the previous rollback copy occupying space.
                with sqlite3.connect(fixed) as connection:
                    connection.execute("INSERT INTO activity_groups_v2 (wallet_hex, source_time_unix, activity_type, coverage_generation) VALUES ('0xabc', 99, 'TRADE', 2)")
                following = self._run()
                self.assertEqual(following.returncode, 0, following.stderr + following.stdout)
                self.assertEqual(self._bootstrap_ops()[len(ops)], "cache-stage-v2")
                self.assertEqual(len(list(out.parent.glob("cron-*"))), 2)

    def test_retirement_interruption_resumes_before_during_and_after_unlink(self):
        for seam in ("before_unlink", "during_unlinks", "before_sync", "after_sync"):
            for args in ((), ("--resume-pending",)):
                with self.subTest(seam=seam, args=args):
                    self.tearDown(); self.setUp()
                    fixed = self._install_candidate_layout(schema=2)
                    self._install_candidate_stub(two_file=True)
                    pause = self.root / "data/eval-results/.forge_pause.json"
                    pause.write_text("paused")
                    published = self._run()
                    self.assertEqual(published.returncode, 0, published.stderr + published.stdout)
                    out = next((self.root / "data/eval-results").glob("cron-*"))
                    backup = fixed.parent / f"wallet_cache.{out.name}.displaced.db"
                    sidecar = Path(str(backup) + "-wal")
                    sidecar.write_bytes(b"retained sidecar")
                    installed = fixed.read_bytes()
                    evidence = {path: path.read_bytes() for path in (
                        out / "ranking_publish_request.json", out / "accepted_cycle_manifest.json",
                        fixed.parent / f"wallet_cache.{out.name}.side.stage.json")}
                    pause.unlink()
                    manifest_script = self.root / "scripts/rank_cycle_manifest.py"
                    original = manifest_script.read_text()
                    sync_start = "    directory = os.open(fixed.parent, os.O_RDONLY | os.O_DIRECTORY)\n"
                    sync_end = "    if not deleted:\n"
                    if seam == "before_unlink":
                        broken = original.replace("        path.unlink()\n", "        raise SystemExit(75)\n        path.unlink()\n")
                    elif seam == "during_unlinks":
                        broken = original.replace("        path.unlink()\n", "        path.unlink()\n        raise SystemExit(75)\n")
                    elif seam == "before_sync":
                        broken = original.replace(sync_start, "    raise SystemExit(75)\n" + sync_start)
                    else:
                        broken = original.replace(sync_end, "    raise SystemExit(75)\n" + sync_end)
                    self.assertNotEqual(broken, original)
                    manifest_script.write_text(broken)
                    ops = self._bootstrap_ops()
                    pushes = self._log("push.log")
                    interrupted = self._run(*args)
                    self.assertEqual(interrupted.returncode, 75, interrupted.stderr + interrupted.stdout)
                    self.assertEqual(backup.exists(), seam == "before_unlink")
                    self.assertEqual(sidecar.exists(), seam in ("before_unlink", "during_unlinks"))
                    self._assert_cache_copies(evidence)

                    # Observe the real directory fsync on retry, including when
                    # the previous attempt unlinked every eligible file already.
                    manifest_script.write_text(original.replace(sync_end,
                        '    Path("retention_synced").write_text(str(fixed.parent))\n' + sync_end))
                    resumed = self._run(*args)
                    self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
                    self.assertEqual((self.root / "retention_synced").read_text(), str(fixed.parent))
                    self.assertFalse(backup.exists())
                    self.assertFalse(sidecar.exists())
                    self.assertEqual(fixed.read_bytes(), installed)
                    self._assert_cache_copies(evidence)
                    self.assertEqual(self._bootstrap_ops(), ops)
                    self.assertEqual(self._log("push.log"), pushes)

    def test_pointerless_retirement_refuses_changed_request_or_staging_evidence(self):
        for artifact in ("request", "stage"):
            with self.subTest(artifact=artifact):
                self.tearDown(); self.setUp()
                fixed = self._install_candidate_layout(schema=2)
                self._install_candidate_stub(two_file=True)
                pause = self.root / "data/eval-results/.forge_pause.json"
                pause.write_text("paused")
                published = self._run()
                self.assertEqual(published.returncode, 0, published.stderr + published.stdout)
                out = next((self.root / "data/eval-results").glob("cron-*"))
                backup = fixed.parent / f"wallet_cache.{out.name}.displaced.db"
                preserved = {path: path.read_bytes() for path in (fixed, backup)}
                if artifact == "request":
                    path = out / "ranking_publish_request.json"
                    value = json.loads(path.read_text())
                    value["publish_key"] = "0" * 64
                else:
                    path = fixed.parent / f"wallet_cache.{out.name}.side.stage.json"
                    value = json.loads(path.read_text())
                    value["source_sha256"] = "0" * 64
                path.write_text(json.dumps(value))
                pause.unlink()
                ops = self._bootstrap_ops()
                refused = self._run()
                self.assertNotEqual(refused.returncode, 0)
                self._assert_cache_copies(preserved)
                self.assertEqual(self._bootstrap_ops(), ops)

    def test_initial_cutover_lane_stages_seals_collects_and_publishes_from_schema_one(self):
        """PASS: with the opt-in, a zero-argument schema-one cycle stages one candidate without a prior beside the physical file, seals the candidate once, collects
        generation 1 for the union, walks payout, finalizes twice, publishes and
        activates, then the accepted capture reads the installed file and the
        next same-day run skips before staging. FAIL: any legacy refresh stage,
        a second cohort, or a repeated collection."""
        fixed = self._install_candidate_layout(schema=1)
        self._install_candidate_stub(two_file=True)
        (self.root / ".env").write_text(
            "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
            "PE_RANK_SCHEMA_TWO_CUTOVER=1\n"
        )
        result = self._run()
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        out = next((self.root / "data/eval-results").glob("cron-*"))
        cycle = out.name
        self.assertIn(f"RANK_AND_PUSH_CACHE_SIDE={self.root}/phys/wallet_cache.{cycle}.side.db", result.stdout)
        self.assertEqual(
            self._bootstrap_ops(),
            ["cache-stage-v2", "cache-migrate-v2", "winner-discovery", "activate-next",
             "cache-populate-activity-v2", "cache-populate-payout-v2", "cache-finalize-v2",
             "prices-history", "cache-finalize-v2", "cache-activate"],
        )
        self.assertNotIn("backfill", result.stdout)
        side = f"{self.root}/phys/wallet_cache.{cycle}.side.db"
        prior = self.root / "phys" / f"wallet_cache.{cycle}.prior.db"
        self.assertIn(f"--db {side} --fresh-generation 1", self._bootstrap_lines("cache-populate-activity-v2")[0])
        self.assertIn(
            f"--final-stage-record data/eval-results/{cycle}/cache_stage_record.json",
            self._bootstrap_lines("cache-activate")[0],
        )
        self.assertIn(f"--db {self.root}/phys/wallet_cache.db --prior {prior} --side {side}", self._bootstrap_lines("cache-stage-v2")[0])
        build = json.loads((out / "cache_build_manifest.json").read_text())
        self.assertEqual(build["backup_sha256"], json.loads(Path(side).with_suffix(".stage.json").read_text())["source_sha256"])
        self.assertFalse(Path(side).exists(), "activation must move the candidate onto the fixed path")
        self.assertFalse(prior.exists())
        self.assertFalse(Path(side.replace(".side.db", ".displaced.db")).exists())
        with sqlite3.connect(fixed) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
        accepted = json.loads((out / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(accepted["cache_schema"], 2)
        self.assertEqual(accepted["source_watermark"]["activity"]["generation"], 1)
        self.assertEqual(accepted["configuration"]["cache_lane"], "fresh_v2")
        self.assertTrue((out / "candidate_cycle_manifest.json").is_file())
        self.assertEqual(json.loads((out / "cycle_manifest.json").read_text())["cache_schema"], 1)
        self.assertIn(f"--cycle-manifest-file data/eval-results/{cycle}/candidate_cycle_manifest.json", self._log("rerank.log"))
        self.assertFalse((self.root / "data/eval-results/rank_and_push.pending").exists())
        self.assertFalse((self.root / "data/eval-results/rank_and_push.cycle").exists())

        ops_before = self._bootstrap_ops()
        second = self._run()
        self.assertEqual(second.returncode, 0, second.stderr + second.stdout)
        self.assertIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", second.stdout)
        self.assertEqual(self._bootstrap_ops(), ops_before, "unchanged day restaged or recollected")

        # A changed installed watermark starts a complete second cycle from the
        # installed schema-two result with no opt-in: generation 2, a new
        # candidate beside the same physical file, a second publication.
        (self.root / ".env").write_text(
            "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
        )
        with sqlite3.connect(fixed) as connection:
            connection.execute("INSERT INTO activity_groups_v2 (wallet_hex, source_time_unix, activity_type, coverage_generation) VALUES ('0xabc', 99, 'TRADE', 1)")
        third = self._run()
        self.assertEqual(third.returncode, 0, third.stderr + third.stdout)
        self.assertNotIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", third.stdout)
        later_ops = self._bootstrap_ops()[len(ops_before):]
        self.assertEqual(
            later_ops,
            ["cache-stage-v2", "winner-discovery", "activate-next", "cache-populate-activity-v2",
             "cache-populate-payout-v2", "cache-finalize-v2", "prices-history",
             "cache-finalize-v2", "cache-activate"],
        )
        self.assertIn("--fresh-generation 2", self._bootstrap_lines("cache-populate-activity-v2")[-1])
        cycles = sorted((self.root / "data/eval-results").glob("cron-*"))
        self.assertEqual(len(cycles), 2)
        self.assertEqual(len(list((self.root / "phys").glob("wallet_cache.*.prior.db"))), 0)
        self.assertFalse(prior.exists(), "completed successor must retire the older rollback copy")
        self.assertEqual(list((self.root / "phys").glob("wallet_cache.*.side.db")), [])
        accepted = json.loads((cycles[-1] / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(accepted["source_watermark"]["activity"]["generation"], 2)
        self.assertEqual(len((self._log("push.log") or "").splitlines()), 8)
        print("PASS: initial schema-two cutover lane completes, skips unchanged, then runs a second full cycle")

    def test_explicit_resume_pending_in_the_candidate_lane_captures_from_the_physical_fixed_path(self):
        """The request binds the physical fixed path; explicit recovery activates
        it, captures the accepted watermark from that path (not the default
        repository name), and the next zero-argument run skips."""
        fixed = self._install_candidate_layout(schema=2)
        self._install_candidate_stub()
        older = self._seed_old_cache_copies(fixed.parent)
        first = self._run(exit_env={"STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr + first.stdout)
        pending = self.root / "data/eval-results/rank_and_push.pending"
        self.assertTrue(pending.is_file())
        self._assert_cache_copies(older)
        self.assertNotIn("[cache-retention] deleted", first.stdout)
        out = Path(pending.read_text().strip()).parent
        request_bytes = (self.root / out / "ranking_publish_request.json").read_bytes()
        request = json.loads(request_bytes)
        self.assertEqual(request["cache_activation"]["fixed_path"], str(fixed))
        ops_before = self._bootstrap_ops()
        resumed = self._run("--resume-pending")
        self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
        self.assertEqual(self._bootstrap_ops(), ops_before + ["cache-activate"])
        self.assertIn(f"capture --db {fixed}", self._log("python_invocations.log"))
        accepted = json.loads((self.root / out / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(accepted["source_watermark"]["activity"]["generation"], 2)
        self.assertFalse(pending.exists())
        self.assertTrue(all(not path.exists() for path in older))
        self.assertEqual((self.root / out / "ranking_publish_request.json").read_bytes(), request_bytes)
        skipped = self._run()
        self.assertEqual(skipped.returncode, 0, skipped.stderr + skipped.stdout)
        self.assertIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", skipped.stdout)
        print("PASS: explicit candidate-lane recovery captures from the request's fixed path")

    def test_recurring_lane_advances_generation_and_reuses_a_completed_payout_walk(self):
        """PASS: an installed schema-two cache selects the lane without the opt-in,
        requests generation prior+1, and after a transient finalization the retry
        reuses the same names, resumes the same generation, skips the completed
        payout walk and publishes. FAIL: a migration, a new payout walk, another
        candidate, or a changed generation on retry."""
        fixed = self._install_candidate_layout(schema=2)
        self._install_candidate_stub()
        # The installed cache carries an interrupted payout walk: the prior fixes
        # the cycle's payout target to that generation, not to newest+1.
        with sqlite3.connect(fixed) as connection:
            connection.execute("INSERT INTO clob_payout_walk_state_v2 VALUES (1, 4)")
        first = self._run(exit_env={"STUB_EXIT_cache_finalize_v2": "75"})
        self.assertEqual(first.returncode, 75, first.stderr + first.stdout)
        self.assertIn("[targets] activity generation 2; payout generation 4 (complete=0)", first.stdout)
        cycle = self.root / "data/eval-results/rank_and_push.cycle"
        self.assertTrue(cycle.is_file())
        out = Path(cycle.read_text().strip()).name
        self.assertEqual(
            self._bootstrap_ops(),
            ["cache-stage-v2", "winner-discovery", "activate-next",
             "cache-populate-activity-v2", "cache-populate-payout-v2", "cache-finalize-v2"],
        )
        self.assertIn("--fresh-generation 2", self._bootstrap_lines("cache-populate-activity-v2")[0])
        side = self.root / "phys" / f"wallet_cache.{out}.side.db"
        self.assertTrue(side.is_file())

        second = self._run()
        self.assertEqual(second.returncode, 0, second.stderr + second.stdout)
        self.assertIn("[cache-retention] deleted", second.stdout)
        self.assertIn(f"RANK_AND_PUSH_CYCLE_RESUME=data/eval-results/{out}", second.stdout)
        ops = self._bootstrap_ops()
        self.assertEqual(ops.count("cache-stage-v2"), 2)
        self.assertEqual(ops.count("cache-populate-payout-v2"), 1, "completed payout walk was restarted")
        self.assertEqual(ops.count("cache-populate-activity-v2"), 2)
        self.assertNotIn("cache-migrate-v2", ops)
        self.assertEqual({line.split()[4] for line in self._bootstrap_lines("cache-populate-activity-v2")}, {"2"})
        self.assertEqual(len(list((self.root / "phys").glob("wallet_cache.*.side.db"))), 0)
        self.assertEqual(len(list((self.root / "phys").glob("wallet_cache.*.prior.db"))), 0)
        accepted = json.loads((self.root / "data/eval-results" / out / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(accepted["source_watermark"]["activity"]["generation"], 2)
        self.assertEqual(accepted["source_watermark"]["resolution"]["generation"], 4)
        self.assertIn("[payout] generation 4 already complete on the candidate; reused", second.stdout)
        print("PASS: recurring lane resumes its own candidate and reuses the completed payout walk")

    def test_lane_is_frozen_with_the_cycle_across_opt_in_changes(self):
        """PASS: a cycle keeps the lane it was created with when the opt-in flips
        between attempts, in both directions. FAIL: an interrupted fresh cycle
        resumes as legacy refresh, or an interrupted legacy cycle switches lanes."""
        self._install_candidate_layout(schema=1)
        self._install_candidate_stub()
        env_path = self.root / ".env"
        base = "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
        env_path.write_text(base + "PE_RANK_SCHEMA_TWO_CUTOVER=1\n")
        first = self._run(exit_env={"STUB_EXIT_cache_populate_activity_v2": "75"})
        self.assertEqual(first.returncode, 75, first.stderr + first.stdout)
        env_path.write_text(base)
        resumed = self._run()
        self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
        ops = self._bootstrap_ops()
        self.assertNotIn("backfill", ops)
        self.assertEqual(ops.count("cache-populate-activity-v2"), 2)
        self.assertEqual(ops[-1], "cache-activate")

        # Opposite direction on a fresh sandbox state: a legacy cycle interrupted
        # before publication completes as legacy after the opt-in is enabled.
        self.tearDown(); self.setUp()
        (self.root / ".env").write_text(base)
        legacy = self._run(exit_env={"STUB_EXIT_events": "75"})
        self.assertEqual(legacy.returncode, 75, legacy.stderr)
        (self.root / ".env").write_text(base + "PE_RANK_SCHEMA_TWO_CUTOVER=1\n")
        finished = self._run()
        self.assertEqual(finished.returncode, 0, finished.stderr + finished.stdout)
        ops = self._bootstrap_ops()
        self.assertNotIn("cache-stage-v2", ops)
        self.assertEqual(ops.count("events"), 2)
        self.assertNotIn("cache_lane", (self.root / "data/eval-results" / Path(
            next((self.root / "data/eval-results").glob("cron-*")).name) / "cycle_configuration.json").read_text())
        print("PASS: the lane is a frozen cycle property")

    def test_prepare_boundary_stops_before_activation_and_recovery_publishes_the_exact_request(self):
        """PASS: with PE_RANK_SCHEMA_TWO_CUTOVER=prepare the lane stops after
        exact request preparation with the pending pointer retained and the
        installed cache untouched; the next zero-argument run resumes that
        request (activate, publish), captures the accepted watermark from the
        request's fixed path, clears the pointers, and a further run skips.
        FAIL: activation before the boundary, recollection on recovery, or a
        repeated cycle after recovery."""
        fixed = self._install_candidate_layout(schema=1)
        self._install_candidate_stub()
        older = self._seed_old_cache_copies(fixed.parent)
        (self.root / ".env").write_text(
            "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
            "PE_RANK_SCHEMA_TWO_CUTOVER=prepare\n"
        )
        first = self._run()
        self.assertEqual(first.returncode, 2, first.stderr + first.stdout)
        self.assertIn("RANK_AND_PUSH_PREPARED_ONLY=", first.stdout)
        pending = self.root / "data/eval-results/rank_and_push.pending"
        self.assertTrue(pending.is_file())
        self._assert_cache_copies(older)
        self.assertNotIn("[cache-retention] deleted", first.stdout)
        self.assertNotIn("cache-activate", self._bootstrap_ops())
        with sqlite3.connect(fixed) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
        out = Path(pending.read_text().strip()).parent
        self.assertFalse((self.root / out / "accepted_cycle_manifest.json").exists())
        request_bytes = (self.root / out / "ranking_publish_request.json").read_bytes()
        ops_before = self._bootstrap_ops()

        # While the value stays `prepare`, neither recovery entry may activate.
        for args in ((), ("--resume-pending",)):
            held = self._run(*args)
            self.assertEqual(held.returncode, 2, held.stderr + held.stdout)
            self.assertIn("holds the prepared request", held.stderr)
            self.assertEqual(self._bootstrap_ops(), ops_before)
            self.assertTrue(pending.is_file())
            self._assert_cache_copies(older)
            self.assertEqual((self.root / out / "ranking_publish_request.json").read_bytes(), request_bytes)
        with sqlite3.connect(fixed) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)

        (self.root / ".env").write_text(
            "SUPABASE_URL=http://127.0.0.1:9\nSUPABASE_SECRET_KEY=test-secret\n"
            "PE_RANK_SCHEMA_TWO_CUTOVER=1\n"
        )
        recovered = self._run()
        self.assertEqual(recovered.returncode, 0, recovered.stderr + recovered.stdout)
        self.assertIn("RANK_AND_PUSH_AUTO_RESUME_PENDING=", recovered.stdout)
        self.assertEqual(self._bootstrap_ops(), ops_before + ["cache-activate"])
        self.assertIn("--resume-request", (self._log("push.log") or "").splitlines()[-1])
        with sqlite3.connect(fixed) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
        accepted = json.loads((self.root / out / "accepted_cycle_manifest.json").read_text())
        self.assertEqual(accepted["cache_schema"], 2)
        self.assertFalse(pending.exists())
        self.assertTrue(all(not path.exists() for path in older))
        self.assertEqual((self.root / out / "ranking_publish_request.json").read_bytes(), request_bytes)
        self.assertFalse((self.root / "data/eval-results/rank_and_push.cycle").exists())

        third = self._run()
        self.assertEqual(third.returncode, 0, third.stderr + third.stdout)
        self.assertIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", third.stdout)
        print("PASS: prepare boundary, exact recovery, accepted capture and same-day skip")

    def test_explicit_resume_pending_captures_accepted_cycle_so_the_next_run_skips(self):
        """The legacy lane's explicit --resume-pending entry also records the
        accepted watermark; before #588 the next same-day run repeated the
        whole refresh."""
        first = self._run(exit_env={"STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr)
        out = next((self.root / "data/eval-results").glob("cron-*"))
        self.assertFalse((out / "accepted_cycle_manifest.json").exists())
        resumed = self._run("--resume-pending")
        self.assertEqual(resumed.returncode, 0, resumed.stderr + resumed.stdout)
        self.assertTrue((out / "accepted_cycle_manifest.json").is_file())
        ops_before = self._bootstrap_ops()
        third = self._run()
        self.assertEqual(third.returncode, 0, third.stderr + third.stdout)
        self.assertIn("RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1", third.stdout)
        self.assertEqual(self._bootstrap_ops(), ops_before)
        print("PASS: explicit pending recovery captures the accepted cycle")

    def test_lane_preflight_refuses_missing_physical_aliases_before_staging(self):
        self._install_candidate_layout(schema=2)
        self._install_candidate_stub()
        (self.root / "phys" / "eval-results").unlink()
        result = self._run()
        self.assertEqual(result.returncode, 2, result.stderr + result.stdout)
        self.assertIn("must resolve to data/eval-results", result.stderr)
        self.assertIsNone(self._log("pe_bootstrap.log"))
        self.assertEqual(list((self.root / "phys").glob("wallet_cache.*.db")), [])


class QuarantineEntryPointsTest(unittest.TestCase):
    """Real ranking/publisher entry points behind the real wrapper and supervisor.

    Only external refresh/publication transport and time are deterministic fakes.
    The wrapper owns the disposition, pointers and current-state queries.
    """
    setUp = RankAndPushScenario.setUp
    tearDown = RankAndPushScenario.tearDown
    _run = RankAndPushScenario._run
    _log = RankAndPushScenario._log
    _write_python_shim = RankAndPushScenario._write_python_shim
    NOW = 1_775_001_600

    def prepare(self, case, *, marked=True, active=True, transition=None):
        from test_rank_72hr_consolidated import build_core_cache, WA, WB
        self.wa, self.wb = WA, WB
        scripts = self.root / "scripts"
        for name in ("ranker_decay.py", "ranker_duck.py", "partial_backfill_wallets.py"):
            shutil.copy(WRAPPER.parent / name, scripts / name)
        db = self.root / "data/wallet_cache.db"
        db.unlink()
        build_core_cache(str(db))
        with sqlite3.connect(db) as con:
            con.executescript("PRAGMA user_version=1;"
                "ALTER TABLE market_resolutions ADD COLUMN fetched_at_unix INTEGER;"
                "CREATE TABLE source_cursor(key TEXT, value TEXT, updated_at INTEGER);"
                "CREATE TABLE wallets(wallet_hex TEXT PRIMARY KEY, is_active INTEGER, is_infra INTEGER, backfill_partial INTEGER, last_polymarket_fetch_at INTEGER);"
                "CREATE VIEW active_tradeable_wallets AS SELECT * FROM wallets WHERE is_active=1 AND is_infra=0;")
            con.execute("UPDATE market_resolutions SET fetched_at_unix=?", (self.NOW,))
            con.execute("INSERT INTO source_cursor VALUES ('clob_closed','',?)", (self.NOW,))
            con.executemany("INSERT INTO wallets VALUES (?,?,0,?,?)",
                            [(WA, int(active), int(marked), self.NOW), (WB, 1, int(case == "all"), self.NOW)])
            if case == "all" and not marked:
                con.execute("UPDATE wallets SET backfill_partial=0")
                con.execute("DELETE FROM trades")
            if case in ("publish_empty", "publish_stale", "publish_partial", "integrity"):
                con.execute("UPDATE trades SET timestamp_unix=? WHERE wallet_hex=?", (self.NOW-4*86400, WB))
                con.execute("UPDATE trades SET timestamp_unix=? WHERE wallet_hex=?", (self.NOW-3600, WA))
                if case == "publish_empty" and not marked:
                    con.execute("UPDATE trades SET timestamp_unix=?", (self.NOW-4*86400,))
                    con.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
                                ("0x"+"f"*40,"buy","fresh",0,"0.5",1,self.NOW,"fresh"))
                if case == "publish_stale":
                    con.execute("UPDATE trades SET timestamp_unix=? WHERE wallet_hex=?",(self.NOW-30*3600,WA))
                    con.execute("UPDATE trades SET timestamp_unix=? WHERE wallet_hex=?",(self.NOW-25*3600,WB))
                if case in ("publish_partial", "integrity"):
                    con.execute("UPDATE trades SET timestamp_unix=? WHERE wallet_hex=?", (self.NOW-3600,WB))
        # The refresh fake logs the same active/non-infra due predicate and changes
        # state AFTER capture, proving the guard never consults the frozen set.
        refresh = scripts / "fixture_refresh.py"
        refresh.write_text(f'''
import json, sqlite3
from pathlib import Path
con=sqlite3.connect("data/wallet_cache.db")
transition={transition!r}
if transition == "partial":
    con.execute("UPDATE wallets SET backfill_partial=1")
elif transition == "recovered_empty":
    con.execute("UPDATE wallets SET backfill_partial=0")
    con.execute("DELETE FROM trades")
con.commit()
due=[r[0] for r in con.execute("SELECT wallet_hex FROM active_tradeable_wallets WHERE backfill_partial=1 OR last_polymarket_fetch_at IS NULL OR last_polymarket_fetch_at < ?", ({self.NOW}-86400,))]
with open("due.log","a") as f: f.write(json.dumps(due)+"\\n")
count=len(Path("due.log").read_text().splitlines())
if Path("stop_after_two").exists() and count >= 2:
    Path("data/eval-results/rank_and_push.loop").write_text("stop\\n")
''')
        boot = self.root / "target/release/pe-bootstrap"
        boot.write_text(boot.read_text().replace('sub="$1"',
            f'if [[ "$1" == "backfill" ]]; then {shlex.quote(sys.executable)} scripts/fixture_refresh.py; fi\nsub="$1"'))
        if case.startswith("publish") or case == "integrity":
            # Keep the upstream ranking fakes; publication preparation below is real.
            publisher = scripts / "push_ranking_to_supabase.py"
            shutil.copy(WRAPPER.parent / "push_ranking_to_supabase.py", scripts / "real_publisher.py")
            # Input fixture is installed before the real prepare function runs.
            publisher.write_text(f'''
import csv, json, hashlib, sys
from pathlib import Path
import real_publisher as pub
pub.time.time=lambda: {self.NOW}
args=sys.argv[1:]
if "--ranked-csv" in args:
    path=Path(args[args.index("--ranked-csv")+1])
    path.write_text("wallet,survives,tstat_net_ls,hit_rate,n_filled\\n{WA},True,4,0.8,40\\n{WB},True,3,0.8,40\\n")
    if "--manifest-file" in args:
        manifest=Path(args[args.index("--manifest-file")+1])
        manifest.write_text(json.dumps({{"outputs":{{"latency_shift_ranked_sha256":hashlib.sha256(path.read_bytes()).hexdigest()}}}}))
entries=[]
config_hash=None
def transport(method,url,key,body=None,**kwargs):
    global entries,config_hash
    with open("transport.log","a") as f: f.write(method+" "+url+"\\n")
    if method=="POST":
        entries=body["p_entries"];config_hash=body["p_batch"]["config_hash"]
        return 200,1
    if "latest_ranking" in url:
        return 200,[dict(batch_id=1,rank=r["rank"],survives=r["survives"]) for r in entries]
    if "select=config_hash" in url:
        return 200,[{{"config_hash":"wrong" if {case!r}=="integrity" else config_hash}}]
    return 200,[]
pub._req=transport
raise SystemExit(pub.main())
''')
        elif case == "pass2":
            shutil.copy(WRAPPER.parent / "latency_shift_rerank.py", scripts / "latency_shift_rerank.py")
            # A legitimate empty candidate CSV reaches the real pass-2a owner.
            ranker = scripts / "rank_72hr_buyandhold.py"
            ranker.write_text(ranker.read_text().replace('wallet\\n0xabc\\n', 'wallet,eligible,tstat_net,mean_net\\n'+WB+',False,0,-1\\n'))
        else:
            shutil.copy(WRAPPER.parent / "rank_72hr_buyandhold.py", scripts / "real_ranker.py")
            floor = "999" if case == "floor" else "0.1"
            min_trl = "99" if case == "eligible" else "0"
            (scripts / "rank_72hr_buyandhold.py").write_text(f'''
import sys
import real_ranker
sys.argv += ["--win-start","2026-01-01","--win-end","2026-04-01","--as-of","2026-04-01","--min-trl",{min_trl!r},"--floor-tstat",{floor!r},"--min-avg-per-month","0","--min-active-months","0"]
raise SystemExit(real_ranker.main())
''')
        return db

    def assert_unprepared(self):
        self.assertFalse(list((self.root / "data/eval-results").glob("cron-*/ranking_publish_request.json")))
        self.assertFalse((self.root / "data/eval-results/rank_and_push.pending").exists())
        self.assertFalse((self.root / "transport.log").exists())
        self.assertTrue((self.root / "data/eval-results/rank_and_push.cycle").exists())

    def test_real_empty_owners_retry_with_partial_and_fail_without(self):
        for case in ("all", "eligible", "floor", "pass2", "publish_empty", "publish_stale"):
            for marked in (True, False):
                with self.subTest(case=case, marked=marked):
                    self.tearDown(); self.setUp()
                    self.prepare(case, marked=marked)
                    result = self._run()
                    self.assertEqual(result.returncode, 75 if marked else 1, result.stdout+result.stderr)
                    self.assertIn("exit 76", result.stderr)
                    self.assert_unprepared()
                    due = json.loads((self.root / "due.log").read_text().splitlines()[0])
                    self.assertEqual(self.wa in due, marked)

    def test_current_state_both_directions_including_reused_manifest(self):
        for transition, marked, expected in [("partial",False,75), ("recovered_empty",True,1)]:
            with self.subTest(transition=transition):
                self.tearDown(); self.setUp()
                self.prepare("all", marked=marked, transition=transition)
                first=self._run()
                self.assertEqual(first.returncode,expected,first.stdout+first.stderr)
                pointer=self.root/"data/eval-results/rank_and_push.cycle"
                manifest=self.root/pointer.read_text().strip()/"cycle_manifest.json"
                frozen=manifest.read_bytes()
                second=self._run()
                self.assertEqual(second.returncode,expected,second.stdout+second.stderr)
                self.assertEqual(manifest.read_bytes(),frozen)
                self.assert_unprepared()

    def test_inactive_excluded_but_does_not_trigger_retry_then_activation_recovers_retry(self):
        db=self.prepare("publish_empty",active=False)
        first=self._run()
        self.assertEqual(first.returncode,1,first.stdout+first.stderr)
        self.assert_unprepared()
        with sqlite3.connect(db) as con: con.execute("UPDATE wallets SET is_active=1")
        second=self._run()
        self.assertEqual(second.returncode,75,second.stdout+second.stderr)

    def test_integrity_failure_stays_permanent_after_request_is_persisted(self):
        self.prepare("integrity")
        result=self._run()
        self.assertEqual(result.returncode,1,result.stdout+result.stderr)
        self.assertIn("stored config_hash",result.stderr)
        self.assertNotIn("exit 76",result.stderr)
        requests=list((self.root/"data/eval-results").glob("cron-*/ranking_publish_request.json"))
        self.assertEqual(len(requests),1)
        self.assertTrue((self.root/"data/eval-results/rank_and_push.pending").exists())

    def test_real_pure_repush_excludes_before_limit_and_resume_is_immutable_without_analytics(self):
        db=self.prepare("publish_partial",active=False)
        out=self.root/"data/eval-results/cron-retained";out.mkdir()
        (out/"latency_shift_ranked.csv").write_text("wallet\nplaceholder\n")
        # Pure re-push and its guard must import no analytics, including transitively.
        blocker=self.root/"blocked";blocker.mkdir()
        (blocker/"sitecustomize.py").write_text('import sys\nclass Block:\n def find_spec(self,name,*args):\n  if name.split(".")[0] in ("numpy","pandas"): raise ImportError("analytics forbidden")\nsys.meta_path.insert(0,Block())\n')
        args=("--skip-rank","--skip-discovery","--skip-backfill","--out-dir",str(out),"--top-n","1")
        result=self._run(*args,exit_env={"PYTHONPATH":str(blocker)})
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        request=out/"ranking_publish_request.json";before=request.read_bytes()
        self.assertEqual(json.loads(before)["entries"][0]["wallet_hex"],self.wb)
        with sqlite3.connect(db) as con: con.execute("UPDATE wallets SET backfill_partial=1")
        unavailable=self._run(*args,exit_env={"PYTHONPATH":str(blocker)})
        self.assertEqual(unavailable.returncode,75,unavailable.stdout+unavailable.stderr)
        self.assertEqual(request.read_bytes(),before)
        (self.root/"data/eval-results/rank_and_push.pending").write_text(str(request.relative_to(self.root))+"\n")
        result=self._run("--resume-pending",exit_env={"PYTHONPATH":str(blocker)})
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual(request.read_bytes(),before)

    def run_real_loop(self, case):
        self.prepare(case)
        loop=self.root/"scripts/rank_and_push_loop.sh"
        loop.write_text((WRAPPER.parent/"rank_and_push_loop.sh").read_text().replace("TRANSIENT_RETRY_DELAY_SECS=60","TRANSIENT_RETRY_DELAY_SECS=1"))
        (self.root/"data/eval-results/rank_and_push.loop").write_text("run\n")
        (self.root/"stop_after_two").touch()
        result=subprocess.run(["bash",str(loop)],cwd=self.root,capture_output=True,text=True,timeout=60)
        return result

    def test_supervisor_retries_same_cycle_and_reselects_each_empty_owner(self):
        for case in ("all","eligible","floor","pass2","publish_empty","publish_stale"):
            with self.subTest(case=case):
                self.tearDown();self.setUp()
                result=self.run_real_loop(case)
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertIn("LOOP_TEMPFAIL",result.stdout)
                self.assertIn("kind=cycle-resume",result.stdout)
                due=(self.root/"due.log").read_text().splitlines()
                self.assertEqual(len(due),2)
                self.assertTrue(all(self.wa in json.loads(line) for line in due))
                self.assert_unprepared()

    def test_supervisor_stops_on_persisted_integrity_failure(self):
        result=self.run_real_loop("integrity")
        self.assertEqual(result.returncode,1,result.stdout+result.stderr)
        self.assertNotIn("LOOP_TEMPFAIL",result.stdout)
        self.assertEqual(len((self.root/"due.log").read_text().splitlines()),1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
