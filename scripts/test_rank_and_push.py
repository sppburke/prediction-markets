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
  - Step 0 runs discover → backfill → events → resolutions → schedules, in that order;
  - a pe-bootstrap stage exiting 2 (partial soft-fail) does NOT abort the run — backfill
    and resolutions return 2 routinely at full scale;
  - a stage exiting 1 (fatal) DOES abort, before ranking;
  - the PID lock blocks a concurrent run and reclaims a stale one;
  - the skip flags bypass Step 0 / ranking for a pure re-push;
  - zero-arg invocation auto-creates a timestamped out-dir;
  - the production half-life default is threaded to both passes.

Run: `python3 scripts/test_rank_and_push.py`
  or: `pytest scripts/test_rank_and_push.py -v`
"""
import os
import shutil
import stat
import subprocess
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

        # Copy the wrapper-under-test into the sandbox so its `cd "$(dirname "$0")/.."`
        # lands in <tmp>, where the stubs / .env / target/release live.
        self.wrapper = self.root / "scripts" / "rank_and_push.sh"
        shutil.copy(WRAPPER, self.wrapper)

        # .env → point the verify step at the localhost stub.
        (self.root / ".env").write_text(
            f"SUPABASE_URL=http://127.0.0.1:{self.port}\nSUPABASE_SECRET_KEY=test-secret\n"
        )

        # Fake pe-bootstrap: log argv to ./pe_bootstrap.log (cwd is the repo root the wrapper
        # cd's into); exit code per subcommand via STUB_EXIT_<sub_with_underscores> (default 0).
        _write_exec(
            self.root / "target" / "release" / "pe-bootstrap",
            '#!/usr/bin/env bash\n'
            'echo "$*" >> pe_bootstrap.log\n'
            'sub="$1"\n'
            'key="STUB_EXIT_${sub//-/_}"\n'
            'code="${!key:-0}"\n'
            'exit "$code"\n',
        )

        # Fake pass-1 ranker: log argv, emit the two CSVs the wrapper expects in --out-dir.
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
    def _run(self, *args, exit_env=None):
        env = dict(os.environ)
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
    def test_happy_path_default_universe_and_step0_order(self):
        r = self._run()
        self.assertEqual(r.returncode, 0, f"stdout={r.stdout}\nstderr={r.stderr}")

        boot = self._log("pe_bootstrap.log")
        self.assertIsNotNone(boot, "Step 0 never invoked pe-bootstrap")
        subs = [ln.split()[0] for ln in boot.splitlines() if ln.strip()]
        self.assertEqual(
            subs,
            ["winner-discovery", "backfill", "events", "resolutions", "schedules"],
            "Step-0 stages ran out of canonical order",
        )

        rank = self._log("rank.log")
        self.assertIn("--universe-from-trades", rank)
        self.assertNotIn("--universe ", rank)  # never the curated-file form by default

        # Auto-timestamped out-dir was created.
        crons = list((self.root / "data" / "eval-results").glob("cron-*"))
        self.assertEqual(len(crons), 1, f"expected one auto out-dir, got {crons}")
        print("PASS: happy path — default --universe-from-trades, Step-0 order, auto out-dir")

    def test_backfill_partial_exit2_does_not_abort(self):
        r = self._run(exit_env={"STUB_EXIT_backfill": "2"})
        self.assertEqual(r.returncode, 0, f"exit 2 aborted the run\nstderr={r.stderr}")
        self.assertIsNotNone(self._log("rank.log"), "ranking did not run after a partial backfill")
        print("PASS: backfill exit 2 (partial) → run continues to ranking")

    def test_backfill_fatal_exit1_aborts_before_ranking(self):
        r = self._run(exit_env={"STUB_EXIT_backfill": "1"})
        self.assertNotEqual(r.returncode, 0, "fatal backfill should abort the run")
        self.assertIsNone(self._log("rank.log"), "ranking ran despite a fatal backfill")
        boot = self._log("pe_bootstrap.log").splitlines()
        self.assertTrue(any(l.startswith("backfill") for l in boot))
        self.assertFalse(any(l.startswith("schedules") for l in boot), "continued past a fatal stage")
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

    def test_missing_bootstrap_binary_is_fatal(self):
        (self.root / "target" / "release" / "pe-bootstrap").unlink()
        r = self._run()
        self.assertNotEqual(r.returncode, 0, "missing pe-bootstrap should be fatal")
        self.assertIn("pe-bootstrap", r.stderr)
        print("PASS: missing pe-bootstrap binary → fatal before any stage")

    def test_wrapper_passes_bash_syntax_check(self):
        r = subprocess.run(["bash", "-n", str(WRAPPER)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, f"bash -n failed: {r.stderr}")
        print("PASS: bash -n syntax check")


if __name__ == "__main__":
    unittest.main(verbosity=2)
