#!/usr/bin/env python3
"""Deterministic process-lifecycle tests for rank_and_push_loop.sh."""

import os
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


LOOP = Path(__file__).resolve().parent / "rank_and_push_loop.sh"


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_dead(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return not _alive(pid)


class RankAndPushLoopScenario(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "scripts").mkdir()
        (self.root / "data" / "eval-results").mkdir(parents=True)
        loop_copy = self.root / "scripts" / "rank_and_push_loop.sh"
        shutil.copy(LOOP, loop_copy)
        loop_copy.write_text(
            loop_copy.read_text().replace("TERM_GRACE_SECS=30", "TERM_GRACE_SECS=1")
        )
        loop_copy.chmod(0o755)
        (self.root / "scripts" / "rank_and_push.sh").write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "printf '%s\\n' \"$#\" > child_argc\n"
            "printf '%s\\n' \"$$\" > child.pid\n"
            "echo RANK_AND_PUSH_RUN_DIR=data/eval-results/cron-test\n"
            "case \"${STUB_MODE:-once}\" in\n"
            "  once) printf 'stop\\n' > data/eval-results/rank_and_push.loop ;;\n"
            "  fail) exit 7 ;;\n"
            "  hold) sleep 300 & printf '%s\\n' \"$!\" > descendant.pid; wait ;;\n"
            "  resist) sh -c 'trap \"\" TERM; printf \"%s\\n\" \"$$\" > descendant.pid; "
            "while :; do sleep 1; done' & wait ;;\n"
            "esac\n"
        )
        (self.root / "scripts" / "rank_and_push.sh").chmod(0o755)
        sqlite3.connect(self.root / "data" / "wallet_cache.db").close()

    def tearDown(self):
        self._tmp.cleanup()

    @property
    def flag(self):
        return self.root / "data" / "eval-results" / "rank_and_push.loop"

    def _run(self, *, mode="once", timeout=10):
        env = os.environ.copy()
        env["STUB_MODE"] = mode
        return subprocess.run(
            ["bash", "scripts/rank_and_push_loop.sh"],
            cwd=self.root,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def test_missing_stop_invalid_and_zero_argument_cycle(self):
        missing = self._run()
        self.assertEqual(missing.returncode, 0)
        self.assertIn("reason=flag_missing", missing.stdout)

        self.flag.write_text("invalid\n")
        invalid = self._run()
        self.assertEqual(invalid.returncode, 2)
        self.assertFalse((self.root / "child_argc").exists())

        self.flag.write_text("run\n\n")
        multiline = self._run()
        self.assertEqual(multiline.returncode, 2)
        self.assertFalse((self.root / "child_argc").exists())

        self.flag.write_text("stop\n")
        stopped = self._run()
        self.assertEqual(stopped.returncode, 0)
        self.assertIn("reason=flag_stop", stopped.stdout)

        self.flag.write_text("run\n")
        ran = self._run()
        self.assertEqual(ran.returncode, 0, ran.stderr)
        self.assertEqual((self.root / "child_argc").read_text().strip(), "0")
        self.assertIn("RANK_AND_PUSH_RUN_DIR=data/eval-results/cron-test", ran.stdout)
        self.assertIn("LOOP_CYCLE_END cycle=1", ran.stdout)

    def test_child_failure_stops_without_retry(self):
        self.flag.write_text("run\n")
        failed = self._run(mode="fail")
        self.assertEqual(failed.returncode, 7)
        self.assertEqual(failed.stdout.count("LOOP_CYCLE_START"), 1)

    def test_singleton_preserves_pid_and_term_kills_resistant_child_group(self):
        self.flag.write_text("run\n")
        env = os.environ.copy()
        env["STUB_MODE"] = "resist"
        first = subprocess.Popen(
            ["bash", "scripts/rank_and_push_loop.sh"],
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            if (self.root / "descendant.pid").exists():
                break
            time.sleep(0.05)
        self.assertTrue((self.root / "descendant.pid").exists(), "child never started")

        second = self._run(mode="once")
        self.assertEqual(second.returncode, 3)
        lock = self.root / "data" / "eval-results" / ".rank_and_push_loop.lock"
        self.assertEqual(
            lock.read_text().strip(),
            str(first.pid),
            "rejected second launch erased or replaced the live supervisor PID",
        )

        child_pid = int((self.root / "child.pid").read_text())
        descendant_pid = int((self.root / "descendant.pid").read_text())
        first.send_signal(signal.SIGTERM)
        first.communicate(timeout=10)
        self.assertEqual(first.returncode, 143)
        self.assertTrue(_wait_dead(child_pid), "one-shot shell survived shutdown")
        self.assertTrue(
            _wait_dead(descendant_pid),
            "TERM-resistant descendant survived process-group KILL escalation",
        )
        self.assertFalse(
            (self.root / "data" / "eval-results" / ".rank_and_push_loop.lock").exists()
        )
        with sqlite3.connect(self.root / "data" / "wallet_cache.db") as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_missing_flock_fails_before_lock_or_child(self):
        bin_dir = self.root / "preflight-bin"
        bin_dir.mkdir()
        for name in ("dirname", "setsid"):
            source = shutil.which(name)
            self.assertIsNotNone(source)
            (bin_dir / name).symlink_to(source)
        env = os.environ.copy()
        env["PATH"] = str(bin_dir)
        result = subprocess.run(
            ["/bin/bash", "scripts/rank_and_push_loop.sh"],
            cwd=self.root,
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("required command is unavailable: flock", result.stderr)
        self.assertFalse((self.root / "child_argc").exists())
        self.assertFalse(
            (self.root / "data" / "eval-results" / ".rank_and_push_loop.lock").exists()
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
