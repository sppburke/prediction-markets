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
    resolutions → infrastructure purge, in that order (issue #383 removed
    the redundant trailing `schedules` stage — fetch_resolutions_and_schedules already covers it);
  - PE_BOOTSTRAP_FETCH_RESOLUTIONS is unset for `backfill` (trades-only) and =1 for `resolutions`,
    so the CLOB→Gamma refresh runs exactly once, not twice (issue #383);
  - a pe-bootstrap stage exiting 2 (partial soft-fail) does NOT abort the run — backfill
    and resolutions return 2 routinely at full scale;
  - a stage exiting 1 (fatal) DOES abort, before ranking;
  - the PID lock blocks a concurrent run and reclaims a stale one;
  - the skip flags bypass Step 0 / ranking for a pure re-push;
  - zero-arg invocation auto-creates a timestamped out-dir;
  - a clean cron/nohup PATH still selects the repository Python environment;
  - PE_PYTHON overrides repository environments and accepts executable paths with spaces;
  - a missing interpreter or dependency fails before the lock, output dir, or data refresh;
  - the production half-life default is threaded to both passes.
  - a durable logical-cycle pointer exists before activation and a zero-argument retry
    reuses its exact run directory / activation batch instead of admitting another cohort;
  - the final WAL checkpoint runs after purge, truncates committed WAL bytes, and warns
    without failing an already-published run when checkpointing is unavailable.

Run: `python3 scripts/test_rank_and_push.py`
  or: `pytest scripts/test_rank_and_push.py -v`
"""
import os
import shutil
import shlex
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

WRAPPER = Path(__file__).resolve().parent / "rank_and_push.sh"

# Production half-life default baked into the wrapper. Pinned here so an intentional change
# co-updates the wrapper literal AND this constant in the same commit (issue #370 PR2 lands
# the 0→30 flip as its own commit).
EXPECTED_DEFAULT_HALF_LIFE = "30"


class _SupabaseStub(BaseHTTPRequestHandler):
    """Answers the wrapper's verify GET with a non-zero row count so verify passes."""

    def do_GET(self):  # noqa: N802 (http.server API)
        self.send_response(200)
        self.send_header("Content-Range", "0-0/5")
        self.end_headers()
        self.wfile.write(b"[{}]")

    def log_message(self, *_):  # silence per-request stderr noise
        pass


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


class RankAndPushScenario(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), _SupabaseStub)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

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

        # The final checkpoint opens the existing cache with URI mode=rw so a typo can never
        # create an empty database. Most scenarios only need a valid empty cache; the WAL
        # reclamation scenario below turns this into a WAL-mode fixture.
        sqlite3.connect(self.root / "data" / "wallet_cache.db").close()

        # Copy the wrapper-under-test into the sandbox so its `cd "$(dirname "$0")/.."`
        # lands in <tmp>, where the stubs / .env / target/release live.
        self.wrapper = self.root / "scripts" / "rank_and_push.sh"
        shutil.copy(WRAPPER, self.wrapper)

        # .env → point the verify step at the localhost stub.
        (self.root / ".env").write_text(
            f"SUPABASE_URL=http://127.0.0.1:{self.port}\nSUPABASE_SECRET_KEY=test-secret\n"
        )

        # Fake pe-bootstrap: log argv to ./pe_bootstrap.log (cwd is the repo root the wrapper
        # cd's into) and the per-invocation PE_BOOTSTRAP_FETCH_RESOLUTIONS value to a sibling
        # pe_bootstrap_env.log (same line order as the argv log, so they zip by index — issue
        # #383 asserts the var is unset for `backfill`, =1 for `resolutions`). Exit code per
        # subcommand via STUB_EXIT_<sub_with_underscores> (default 0).
        _write_exec(
            self.root / "target" / "release" / "pe-bootstrap",
            '#!/usr/bin/env bash\n'
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
            "import sys\n"
            'open("export.log", "a").write(" ".join(sys.argv[1:]) + "\\n")\n',
        )
        _write_exec(
            self.root / "scripts" / "rank_72hr_buyandhold.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "a = sys.argv[1:]\n"
            'open("rank.log", "a").write(" ".join(a) + "\\n")\n'
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
            "a = sys.argv[1:]\n"
            'open("rerank.log", "a").write(" ".join(a) + "\\n")\n'
            'out = a[a.index("--out-dir") + 1] if "--out-dir" in a else "."\n'
            "os.makedirs(out, exist_ok=True)\n"
            'if "--emit-targets" in a:\n'
            '    tpath = a[a.index("--emit-targets") + 1]\n'
            '    open(tpath, "w").write("token_id,start_ts,end_ts\\nTOK,1,100\\n")\n'
            '    sys.exit(int(os.environ.get("STUB_EXIT_emit", "0")))\n'
            'rc = int(os.environ.get("STUB_EXIT_rerank", "0"))\n'
            "if rc:\n"
            "    sys.exit(rc)  # the real coverage gate exits before writing any output\n"
            'open(os.path.join(out, "latency_shift_ranked.csv"), "w").write("wallet\\n0xabc\\n")\n'
            'if not os.environ.get("STUB_NO_MANIFEST"):\n'
            '    import hashlib, json\n'
            '    ranked = open(os.path.join(out, "latency_shift_ranked.csv"), "rb").read()\n'
            '    open(os.path.join(out, "oracle_manifest.json"), "w").write(json.dumps(\n'
            '        {"oracle": "clob-minute-reference",\n'
            '         "outputs": {"latency_shift_ranked_sha256": hashlib.sha256(ranked).hexdigest()}}))\n'
            "sys.exit(0)\n",
        )
        # Fake push: log argv, emulate durable request/pending writes, and optionally
        # return the requested status (including EX_TEMPFAIL=75).
        _write_exec(
            self.root / "scripts" / "push_ranking_to_supabase.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "from pathlib import Path\n"
            "a = sys.argv[1:]\n"
            'open("push.log", "a").write(" ".join(a) + "\\n")\n'
            'if "--request-file" in a:\n'
            '    request = Path(a[a.index("--request-file") + 1])\n'
            '    request.parent.mkdir(parents=True, exist_ok=True)\n'
            '    request.write_text("{}\\n")\n'
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
                "purge-infra",
                # #536: the targeted reference fetch runs between the rank passes
                # (stage 2b) — after purge-infra, before the post-publish purge.
                "prices-history",
                "purge",
            ],
            "Step-0 stages ran out of canonical order, or the final purge stage (#385) is missing",
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

    def test_fresh_run_without_manifest_fails_closed_before_push(self):
        # #536: a fresh rerank always writes the manifest before success, so exit 0
        # with no manifest is corruption -- never a silent config_hash = null publish.
        r = self._run(exit_env={"STUB_NO_MANIFEST": "1"})
        self.assertEqual(r.returncode, 1, f"stderr={r.stderr}")
        self.assertIn("refusing provenance-less publish", r.stderr)
        self.assertIsNone(self._log("push.log"), "published without an oracle manifest")
        print("PASS: fresh run with missing manifest fails closed before publication")

    def _purge_status_lines(self):
        # #538: one JSONL line per purge invocation, in the per-run artifact dir.
        import glob as _glob
        import json as _json
        lines = []
        for path in _glob.glob(str(self.root / "data/eval-results/cron-*/purge_status.jsonl")):
            with open(path) as fh:
                lines += [_json.loads(l) for l in fh if l.strip()]
        return lines

    def test_purge_status_jsonl_records_every_outcome(self):
        # #538: journald died mid-incident and took the only purge failure signal
        # with it. Every purge invocation must land one durable JSONL record in
        # the run dir — success AND failure, both entry points, without altering
        # any existing exit policy.
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stderr={r.stderr}")
        recs = self._purge_status_lines()
        self.assertEqual(
            [(x["stage"], x["exit_code"], x["level"]) for x in recs],
            [("purge-infra", 0, "info"), ("purge", 0, "info")],
            f"clean run records both purge stages: {recs}",
        )
        for x in recs:
            self.assertIn("ts", x)
            self.assertEqual(x["message"], "purge stage exit")
        print("PASS: clean run appends purge-infra + purge status lines (exit 0)")

    def test_purge_status_jsonl_on_infra_tempfail_and_fatal(self):
        # Exit 75 aborts inside run_refresh_stage's case arm — the record must
        # exist anyway (append happens before dispatch).
        r = self._run(exit_env={"STUB_EXIT_purge_infra": "75"})
        self.assertEqual(r.returncode, 75)
        recs = self._purge_status_lines()
        self.assertEqual(
            [(x["stage"], x["exit_code"], x["level"]) for x in recs],
            [("purge-infra", 75, "warn")],
            f"tempfail recorded before the abort: {recs}",
        )
        print("PASS: purge-infra exit 75 recorded despite the in-arm abort")

    def test_purge_status_jsonl_on_infra_partial_and_fatal(self):
        # Exit 2 (partial: WARN + continue) and exit 1 (fatal: in-arm abort) must
        # both be durably recorded (#538 review: suppressing records on exactly
        # these arms would otherwise pass).
        r = self._run(exit_env={"STUB_EXIT_purge_infra": "2"})
        self.assertEqual(r.returncode, 0, "infra exit 2 is WARN + continue")
        recs = self._purge_status_lines()
        self.assertEqual(
            [(x["stage"], x["exit_code"], x["level"]) for x in recs],
            [("purge-infra", 2, "warn"), ("purge", 0, "info")],
            f"partial infra recorded, run continued to the ordinary purge: {recs}",
        )

        self.setUp()  # fresh sandbox for the fatal case
        r = self._run(exit_env={"STUB_EXIT_purge_infra": "1"})
        self.assertNotEqual(r.returncode, 0)
        recs = self._purge_status_lines()
        self.assertEqual(
            [(x["stage"], x["exit_code"], x["level"]) for x in recs],
            [("purge-infra", 1, "warn")],
            f"fatal infra recorded before the in-arm abort: {recs}",
        )
        print("PASS: purge-infra exits 2 and 1 both leave durable status records")

    def test_purge_status_jsonl_on_ordinary_purge_failure(self):
        # The ordinary purge is non-fatal (post-publication); its failure must
        # still be durably recorded and the run must still succeed.
        r = self._run(exit_env={"STUB_EXIT_purge": "1"})
        self.assertEqual(r.returncode, 0, "ordinary purge failure stays non-fatal")
        recs = self._purge_status_lines()
        self.assertEqual(
            [(x["stage"], x["exit_code"], x["level"]) for x in recs],
            [("purge-infra", 0, "info"), ("purge", 1, "warn")],
            f"ordinary purge failure recorded: {recs}",
        )
        self.assertIn("[purge] WARN exit 1", r.stderr)
        print("PASS: ordinary purge failure recorded as warn; run still exits 0")

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

    def test_infra_purge_fatal_aborts_before_export_and_ranking(self):
        r = self._run(exit_env={"STUB_EXIT_purge_infra": "1"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIsNone(self._log("export.log"))
        self.assertIsNone(self._log("rank.log"))

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
        lock.write_text(str(os.getpid()))  # our own PID is alive → held lock
        r = self._run()
        self.assertEqual(r.returncode, 3, f"a live lock must block with exit 3\nstderr={r.stderr}")
        self.assertIsNone(self._log("pe_bootstrap.log"), "ran Step 0 despite a held lock")
        self.assertEqual(lock.read_text(), str(os.getpid()), "clobbered the holder's lockfile")
        print("PASS: live lock → second run aborts (exit 3), holder's lock intact")

    def test_stale_lock_is_reclaimed(self):
        dead = subprocess.Popen(["sh", "-c", "exit 0"])
        dead.wait()  # dead.pid now refers to a terminated process
        lock = self.root / "data" / "eval-results" / ".rank_and_push.lock"
        lock.write_text(str(dead.pid))
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stale lock should be reclaimed\nstderr={r.stderr}")
        self.assertIn("reclaiming stale lock", r.stderr)
        print("PASS: stale lock (dead PID) → reclaimed, run proceeds")

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
            sum(line.startswith("purge") and not line.startswith("purge-infra")
                for line in boot_after),
            1,
            "resume did not finish the original purge tail exactly once",
        )
        self.assertEqual(len((self._log("rank.log") or "").splitlines()), 1)
        self.assertIn("--resume-request", (self._log("push.log") or "").splitlines()[-1])
        self.assertFalse(pending.exists(), "completed tail did not clear its pending pointer")
        self.assertFalse(
            (self.root / "data" / "eval-results" / "rank_and_push.cycle").exists(),
            "completed publication tail did not clear the logical-cycle pointer",
        )
        print("PASS: transient push resume replays request + purge/checkpoint, no activation/rank")

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
            boot_before + ["purge"],
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

    def test_purge_runs_after_push_with_decision_csv(self):
        # Issue #385: the final purge stage runs after the push, last in pe_bootstrap.log,
        # with PE_BOOTSTRAP_PURGE_DECISION_CSV pointing at this run's ranked CSV.
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        subs = [ln.split()[0] for ln in (self._log("pe_bootstrap.log") or "").splitlines() if ln.strip()]
        self.assertEqual(subs[-1], "purge", f"purge was not the final pe-bootstrap stage: {subs}")
        self.assertIsNotNone(self._log("push.log"), "push must run before purge")
        # The purge invocation saw the run's RANKED_CSV via env (last non-empty line).
        csvs = [ln for ln in (self._log("pe_bootstrap_purge_csv.log") or "").splitlines() if ln.strip()]
        self.assertTrue(csvs, "purge never saw PE_BOOTSTRAP_PURGE_DECISION_CSV")
        self.assertTrue(
            csvs[-1].endswith("ranked_72hr_buyandhold.csv") and "cron-" in csvs[-1],
            f"purge decision CSV not the cron run's ranked CSV: {csvs[-1]}",
        )
        print("PASS: purge runs last, after push, with the run's RANKED_CSV as the decision CSV")

    def test_checkpoint_runs_after_purge_and_truncates_wal(self):
        db = self.root / "data" / "wallet_cache.db"
        connection = sqlite3.connect(db)
        try:
            self.assertEqual(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            connection.execute("PRAGMA wal_autocheckpoint=0")
            connection.execute("CREATE TABLE checkpoint_fixture (value TEXT NOT NULL)")
            connection.execute("INSERT INTO checkpoint_fixture VALUES ('committed')")
            connection.commit()

            wal = Path(f"{db}-wal")
            self.assertTrue(wal.is_file() and wal.stat().st_size > 0, "fixture did not create a WAL")

            r = self._run()
            self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
            self.assertEqual(wal.stat().st_size if wal.exists() else 0, 0, "final checkpoint did not truncate WAL")
            self.assertGreater(r.stdout.index("Stage 5/5"), r.stdout.index("Stage 4/5"))
            self.assertIn("[checkpoint] ok", r.stdout)
        finally:
            connection.close()
        print("PASS: final checkpoint runs after purge and truncates committed WAL bytes")

    def test_skip_purge_bypasses_purge(self):
        r = self._run("--skip-purge")
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        subs = [ln.split()[0] for ln in (self._log("pe_bootstrap.log") or "").splitlines() if ln.strip()]
        self.assertNotIn("purge", subs, "purge ran despite --skip-purge")
        self.assertIsNotNone(self._log("push.log"), "push must still run with --skip-purge")
        self.assertIn("[checkpoint] ok", r.stdout, "checkpoint must still run when purge is skipped")
        print("PASS: --skip-purge bypasses the purge stage; push still runs")

    def test_purge_failure_is_non_fatal(self):
        # purge runs after the (already-complete) push, so a purge failure must not fail the run.
        r = self._run(exit_env={"STUB_EXIT_purge": "1"})
        self.assertEqual(r.returncode, 0, f"a failing purge aborted the run\nstderr={r.stderr}")
        subs = [ln.split()[0] for ln in (self._log("pe_bootstrap.log") or "").splitlines() if ln.strip()]
        self.assertIn("purge", subs, "purge stage did not run")
        self.assertIn("WARN", r.stderr)
        print("PASS: purge exit 1 → WARN, run still succeeds (push already published)")

    def test_checkpoint_failure_is_non_fatal(self):
        (self.root / "data" / "wallet_cache.db").unlink()
        r = self._run()
        self.assertEqual(r.returncode, 0, f"checkpoint failure changed run status\nstderr={r.stderr}")
        self.assertIn("database is not a file", r.stderr)
        self.assertIn("[checkpoint] WARN", r.stderr)
        self.assertIsNotNone(self._log("push.log"), "push must complete before checkpoint warning")
        print("PASS: checkpoint failure → WARN, run still succeeds (push already published)")

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

    # ── #527: both purge entry points run at idle I/O priority, fail closed ──────────
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

    def test_purge_stages_run_under_idle_ionice(self):
        r = self._run(exit_env=self._install_ionice_stub())
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")
        self.assertEqual(
            (self._log("ionice.log") or "").splitlines(),
            [
                "-c3 target/release/pe-bootstrap purge-infra",
                "-c3 target/release/pe-bootstrap purge",
            ],
            "exactly the two purge stages must run under ionice -c3, nothing else",
        )
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertIn("purge-infra", boot)
        self.assertIn("purge", boot)
        print("PASS: both purge stages — and only they — run under ionice -c3")

    def test_missing_ionice_fails_before_lock_when_purge_reachable(self):
        r = self._run(exit_env=self._path_without_ionice())
        self.assertEqual(r.returncode, 2, f"missing ionice should exit 2\nstderr={r.stderr}")
        self.assertIn("ionice not found", r.stderr)
        self._assert_preflight_left_no_pipeline_state()
        print("PASS: absent ionice fails closed before lock, output directory, or refresh")

    def test_missing_ionice_ok_for_pure_repush(self):
        # Purge is unreachable on the re-push shape, so the ionice preflight must not fire.
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
        print("PASS: ionice is a conditional dependency — absent tool still allows a re-push")

    def test_infra_ionice_failure_is_fatal_before_ranking(self):
        env = self._install_ionice_stub()
        env["STUB_IONICE_EXIT_purge_infra"] = "9"
        r = self._run(exit_env=env)
        self.assertEqual(r.returncode, 9, f"infra ionice failure must abort\nstderr={r.stderr}")
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertNotIn("purge-infra", boot, "infra purge ran directly despite ionice failure")
        self.assertIsNone(self._log("rank.log"), "ranking ran after fatal infra-purge failure")
        self.assertIsNone(self._log("push.log"), "publish ran after fatal infra-purge failure")
        print("PASS: infra-purge ionice failure is fatal before ranking, no unprioritized fallback")

    def test_infra_purge_partial_exit2_continues_through_ionice(self):
        # Child (not ionice) soft-fails with the pe-bootstrap partial code 2: the stage
        # must WARN and the run must still rank and publish — ionice's exec preserves
        # the child's exit code, and run_refresh_stage's 2-branch must keep owning it.
        env = self._install_ionice_stub()
        env["STUB_EXIT_purge_infra"] = "2"
        r = self._run(exit_env=env)
        self.assertEqual(r.returncode, 0, f"partial infra purge must not abort\nstderr={r.stderr}")
        self.assertIn("[purge-infra] WARN exit 2", r.stderr)
        self.assertIsNotNone(self._log("rank.log"), "ranking did not run after partial infra purge")
        self.assertIsNotNone(self._log("push.log"), "publish did not run after partial infra purge")
        print("PASS: infra-purge partial (exit 2) still warns and continues under ionice")

    def test_ordinary_ionice_failure_warns_after_publish(self):
        env = self._install_ionice_stub()
        env["STUB_IONICE_EXIT_purge"] = "9"
        r = self._run(exit_env=env)
        self.assertEqual(r.returncode, 0, f"ordinary ionice failure must stay nonfatal\nstderr={r.stderr}")
        self.assertIn("[purge] WARN exit 9", r.stderr)
        self.assertIsNotNone(self._log("push.log"), "publish did not complete")
        boot = (self._log("pe_bootstrap.log") or "").splitlines()
        self.assertIn("purge-infra", boot)
        self.assertNotIn("purge", boot, "ordinary purge ran directly despite ionice failure")
        print("PASS: ordinary-purge ionice failure warns after publish, no unprioritized fallback")

    def test_resume_pending_purge_tail_is_prioritized(self):
        env = self._install_ionice_stub()
        first = self._run(exit_env={**env, "STUB_PUSH_EXIT": "75"})
        self.assertEqual(first.returncode, 75, first.stderr)
        self.assertEqual(
            (self._log("ionice.log") or "").splitlines(),
            ["-c3 target/release/pe-bootstrap purge-infra"],
            "failed-push run should have prioritized only the infra purge",
        )
        resumed = self._run("--resume-pending", exit_env=env)
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertEqual(
            (self._log("ionice.log") or "").splitlines(),
            [
                "-c3 target/release/pe-bootstrap purge-infra",
                "-c3 target/release/pe-bootstrap purge",
            ],
            "resume must prioritize exactly the one ordinary purge tail, no infra rerun",
        )
        print("PASS: publication resume runs its single ordinary-purge tail under ionice -c3")

    def test_wrapper_passes_bash_syntax_check(self):
        r = subprocess.run(["bash", "-n", str(WRAPPER)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, f"bash -n failed: {r.stderr}")
        print("PASS: bash -n syntax check")


if __name__ == "__main__":
    unittest.main(verbosity=2)
