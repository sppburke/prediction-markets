#!/usr/bin/env python3
"""Run the existing CI suites in isolated processes, bounded by nproc.

Add future drift suites to DRIFT_SUITES. Their mutable fixtures are temporary or
in-memory; repository paths are read-only inputs. No current suites need a serial
group. Scenario phases within a suite stay in their original order.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit, urlunsplit
import uuid


ROOT = Path(__file__).resolve().parent.parent
DRIFT_SUITES = (
    "check_sql_unfiltered_dml.py",
    "test_push_ranking_filter.py",
    "test_ranker_decay.py",
    "test_rank_72hr_consolidated.py",
    "test_audit_wallet_history.py",
    "test_clob_reconciliation.py",
    "test_rank_and_push.py",
    "test_rank_cycle_manifest.py",
    "test_rank_and_push_loop.py",
    "test_ranker_duck_parity.py",
    "test_latency_shift_ref_oracle.py",
    "test_probe_gamma_ua.py",
    "test_ranker_interfaces.py",
    "test_ranker_suff_stats.py",
    "test_ranker_estimators.py",
    "test_ranker_deflation.py",
    "test_ranker_oos_validation.py",
    "test_ranker_selectors.py",
    "test_ranker_demotion.py",
    "test_ranker_policies.py",
    "test_ranker_bakeoff.py",
    "test_ranker_confirmatory.py",
    "test_ranker_bench_composition.py",
    "test_ranker_clv_source_comparison.py",
    "verify_bakeoff_components.py",
)


def run_suite(command, overrides, cwd):
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="pe-ci-suite-") as directory:
        work = Path(directory)
        tmp = work / "tmp"
        tmp.mkdir()
        env = dict(os.environ, TMPDIR=str(tmp), RUNNER_TEMP=str(tmp),
                   PYTHONDONTWRITEBYTECODE="1", NUMBA_CACHE_DIR=str(tmp / "numba"))
        # Match the bakeoff suites' pre-import BLAS caps when running suites together.
        for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
                     "NUMEXPR_NUM_THREADS"):
            env[name] = "1"
        env.update(overrides)
        with tempfile.TemporaryFile() as log:
            try:
                result = subprocess.run(command, cwd=cwd or work, env=env,
                                        stdout=log, stderr=subprocess.STDOUT)
                status = result.returncode
            except OSError as error:
                log.write(str(error).encode())
                status = 1
            log.seek(0)
            output = log.read().decode("utf-8", errors="replace")
    return status, time.monotonic() - started, output


def run_suites(suites):
    started = time.monotonic()
    workers = int(subprocess.check_output(["nproc"], text=True).strip())
    print(f"Running {len(suites)} suites with at most {workers} workers", flush=True)
    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(run_suite, command, env, cwd): name
                   for name, command, env, cwd in suites}
        for future in as_completed(pending):
            status, elapsed, output = future.result()
            name = pending[future]
            print(f"{'FAIL' if status else 'PASS'}: {name} (exit {status}, {elapsed:.2f}s)",
                  flush=True)
            if status:
                failures += 1
                print(output, end="" if output.endswith("\n") else "\n", flush=True)
            elif output.strip():
                print(output.strip().splitlines()[-1], flush=True)
    print(f"Suites: {len(suites) - failures} passed, {failures} failed; "
          f"wall time {time.monotonic() - started:.2f}s", flush=True)
    return 1 if failures else 0


def psql(url, sql):
    return subprocess.run(["psql", url, "-X", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                          check=True, capture_output=True, text=True).stdout.strip()


def database_url(url, name):
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{name}"))


def postgres_suites(stack):
    admin = os.environ["PG_ADMIN_URL"]
    template = os.environ["PE_TEST_PG_URL"]
    template_name = urlsplit(template).path.removeprefix("/")
    quoted_template = template_name.replace('"', '""')
    suffix = uuid.uuid4().hex
    urls = {}
    for name in ("multi_account", "ranking"):
        database = f"pe_ci_{name}_{suffix}"
        psql(admin, f'CREATE DATABASE "{database}" TEMPLATE "{quoted_template}"')
        stack.callback(psql, admin, f'DROP DATABASE "{database}" WITH (FORCE)')
        urls[name] = database_url(template, database)

    # This existing scenario owns a fixed database and builds the real service.
    # Refuse a reused fixture, and remove only the database this run creates.
    if psql(admin, "SELECT count(*) FROM pg_database WHERE datname = 'pe_legacy_545'") != "0":
        raise RuntimeError("pe_legacy_545 must be absent before the Postgres suites")
    stack.callback(psql, admin, "DROP DATABASE IF EXISTS pe_legacy_545 WITH (FORCE)")
    return [
        ("legacy-to-financial", ["bash", str(ROOT / "scripts/deploy/test_legacy_to_financial_pg.sh")],
         {"PG_LEGACY_URL": database_url(admin, "pe_legacy_545")}, None),
        ("multi-account", ["bash", str(ROOT / "scripts/test_multi_account_schema.sh"),
                           urls["multi_account"]], {}, None),
        ("pg_parity", ["cargo", "nextest", "run", "-p", "pe-service", "--features",
                       "scenario", "--test", "pg_parity", "--test-threads", "8"], {}, ROOT),
        ("ranking-publication", [sys.executable, str(ROOT / "scripts/test_ranking_publish_rpc.py")],
         {"PE_TEST_PG_URL": urls["ranking"]}, None),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=("drift", "postgres"))
    args = parser.parse_args()
    with ExitStack() as stack:
        if args.group == "postgres":
            suites = postgres_suites(stack)
        else:
            suites = [(name, [sys.executable, str(ROOT / "scripts" / name)], {}, None)
                      for name in DRIFT_SUITES]
        return run_suites(suites)


if __name__ == "__main__":
    raise SystemExit(main())
