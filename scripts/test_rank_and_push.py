#!/usr/bin/env python3
"""Use-case + regression guard for the cron-ready `scripts/rank_and_push.sh` wrapper
(issue #370 PR2).

The wrapper orchestrates a 30–90 min pipeline over a 162 GB cache and live APIs, so it
cannot be run for real in CI. This test instead stands up a fully *stubbed* repo root —
a fake `target/release/pe-bootstrap`, fake pass-1/pass-2/push Python scripts, and a
localhost HTTP stub for the Supabase verify — and drives the real wrapper against it. No
network (localhost only), no real DB, deterministic.

What it locks (the things that would silently break cron if wired wrong):
  - default universe source is `--universe-from-trades`, never `--universe ""` (the ranker
    hard-errors on neither/both);
  - Step 0 runs discover → backfill → events → resolutions, in that order (issue #383 removed
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
        _write_exec(
            self.root / "scripts" / "latency_shift_rerank.py",
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "a = sys.argv[1:]\n"
            'open("rerank.log", "a").write(" ".join(a) + "\\n")\n'
            'out = a[a.index("--out-dir") + 1] if "--out-dir" in a else "."\n'
            "os.makedirs(out, exist_ok=True)\n"
            'open(os.path.join(out, "latency_shift_ranked.csv"), "w").write("wallet\\n0xabc\\n")\n',
        )
        # Fake push: log argv, succeed (no real Supabase write).
        _write_exec(
            self.root / "scripts" / "push_ranking_to_supabase.py",
            "#!/usr/bin/env python3\n"
            "import sys\n"
            'open("push.log", "a").write(" ".join(sys.argv[1:]) + "\\n")\n',
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
            ["winner-discovery", "backfill", "events", "resolutions", "purge"],
            "Step-0 stages ran out of canonical order, or the final purge stage (#385) is missing",
        )

        rank = self._log("rank.log")
        self.assertIn("--universe-from-trades", rank)
        self.assertNotIn("--universe ", rank)  # never the curated-file form by default

        # Auto-timestamped out-dir was created.
        crons = list((self.root / "data" / "eval-results").glob("cron-*"))
        self.assertEqual(len(crons), 1, f"expected one auto out-dir, got {crons}")
        print("PASS: happy path — default --universe-from-trades, Step-0 order, auto out-dir")

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
        self.assertIsNotNone(self._log("push.log"), "re-push did not push")
        print("PASS: --skip-discovery --skip-backfill --skip-rank → re-push only")

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

    def test_wrapper_passes_bash_syntax_check(self):
        r = subprocess.run(["bash", "-n", str(WRAPPER)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, f"bash -n failed: {r.stderr}")
        print("PASS: bash -n syntax check")


if __name__ == "__main__":
    unittest.main(verbosity=2)
