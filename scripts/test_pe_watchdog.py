"""Deterministic, offline contract tests for pe_watchdog.py."""

import copy
import datetime as dt
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("w", Path(__file__).with_name("pe_watchdog.py"))
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 11, 17, 20, 0, tzinfo=UTC)  # 2 PM CT
LOOP = "pe-rank-loop-15m5m.service"
SERVICE = "pe-service.service"
SITE = "pe-site.service"
NGINX = "nginx.service"
QUIET = {"summary_date": "2026-11-17"}
TEMPFAIL = "   [collect] TEMPFAIL exit 75 (temporary failure); aborting run for supervised retry"
SUCCESS = "LOOP_CYCLE_END cycle=3 utc=2026-11-17T20:00:00Z status=0"
DATADASH = "winner_discovery: datadash source failed — continuing with zero counts"
PRUNE = ("WARNING: ranking_batches prune failed (RuntimeError: unavailable); history not trimmed this run — "
         "bounded and self-heals on the next successful push")
FAILURE = "pe-service.service: Failed with result 'exit-code'."
KERNEL_IO = "I/O error, dev sda, sector 123456 op 0x0:(READ) flags 0x0 phys_seg 1 prio class 2"


def record(message, unit=SERVICE, host="vps", cursor="c1", **fields):
    field = "_SYSTEMD_USER_UNIT" if host == "forge" else "_SYSTEMD_UNIT"
    return {"MESSAGE": message, "__CURSOR": cursor, field: unit, **fields}


def service(message, level="WARN", **fields):
    return record(json.dumps({"message": message, "level": level, **fields}))


def forge(message, level=None, **fields):
    if level:
        message = json.dumps({"level": level, "fields": {"message": message, **fields}})
    return record(message, LOOP, "forge")


def host_result(host, records=(), **states):
    return {"records": list(records), "units": {u: states.get(u, "active") for u in w.HOST_UNITS[host]},
            "error": None}


def obs(forge_records=(), vps_records=(), forge_state="active", vps_states=None,
        ok=True, batch=159, created=T0 - dt.timedelta(hours=1), err=None, errors=None):
    hosts = {"forge": host_result("forge", forge_records, **{LOOP: forge_state}),
             "vps": host_result("vps", vps_records, **(vps_states or {}))}
    for host, error in (errors or {}).items():
        hosts[host] = {"error": error}
    return {"hosts": hosts, "health_ok": ok,
            "health_detail": "HTTP 200, ready=True" if ok else "HTTP 503, ready=False",
            "batch_id": None if err else batch, "created": None if err else created, "batch_error": err}


def alerts(messages):
    return [m for m in messages if not m.startswith("  ")]


def access(status=500, method="GET", path="/api/config?private=one", client="203.0.113.7"):
    return record(f'{client} - - [17/Nov/2026:20:00:00 +0000] "{method} {path} HTTP/1.1" '
                  f'{status} 123 "-" "test-agent"', NGINX)


class Rules(unittest.TestCase):
    def step(self, state=None, observation=None, minutes=0):
        return w.evaluate(QUIET if state is None else state, obs() if observation is None else observation,
                          T0 + dt.timedelta(minutes=minutes))

    def test_healthy_is_silent_and_evaluate_is_pure(self):
        state, observation = copy.deepcopy(QUIET), obs()
        before = copy.deepcopy((state, observation))
        new, messages = self.step(state, observation)
        self.assertEqual(messages, [])
        self.assertEqual((state, observation), before)
        self.assertIsNot(new, state)
        json.dumps(new)

    def test_every_loop_failure_trigger_opens_and_success_resolves(self):
        cases = [TEMPFAIL, "LOOP_TEMPFAIL kind=cycle status=75 retry_in=60s",
                 "   [rank] FATAL exit 1 — aborting run",
                 "FATAL: transient exit 75 retained neither recovery pointer; loop stopped",
                 "thread 'main' panicked at src/main.rs:10:2",
                 "Traceback (most recent call last):", "Failed with result 'exit-code'."]
        records = [forge(m) for m in cases] + [forge("bootstrap failed", "ERROR")]
        for r in records:
            with self.subTest(message=r["MESSAGE"]):
                s, m = self.step(observation=obs(forge_records=[r]))
                self.assertTrue(m[0].startswith("PROBLEM: Forge loop failing"))
                s, m = self.step(s, obs(forge_records=[r]), 1)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(forge_records=[forge(SUCCESS)]), 2)
                self.assertTrue(m[0].startswith("RESOLVED: Forge loop successful again"))

    def test_all_loop_success_markers_close_and_nonzero_does_not(self):
        for success in [SUCCESS, "LOOP_RESUME_END kind=resume utc=2026-11-17T20:00:00Z status=0",
                        "LOOP_SUCCESS kind=cycle retry_in=60s"]:
            with self.subTest(success=success):
                s, _ = self.step(observation=obs(forge_records=[forge(TEMPFAIL)]))
                s, m = self.step(s, obs(forge_records=[forge(SUCCESS.replace("status=0", "status=75"))]), 1)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(forge_records=[forge(success)]), 2)
                self.assertIn("RESOLVED: Forge loop successful again (lasted 0 h 2 min)", m)

    def test_loop_failure_and_recovery_order_within_one_read(self):
        s, m = self.step(observation=obs(forge_records=[forge(TEMPFAIL), forge(SUCCESS), forge(TEMPFAIL)]))
        self.assertEqual([x.split(":", 1)[0] for x in m], ["PROBLEM", "RESOLVED", "PROBLEM"])
        self.assertIn("loop_failing", s["open"])

    def test_loop_not_running_opens_immediately_and_resolves(self):
        s, m = self.step(observation=obs(forge_state="inactive"))
        self.assertTrue(m[0].startswith("PROBLEM: Forge loop not running"))
        s, m = self.step(s, obs(forge_state="failed"), 1)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 2)
        self.assertTrue(m[0].startswith("RESOLVED: Forge loop running again"))

    def test_planned_stop_one_note_persists_until_active_and_rearms(self):
        for stop in ["LOOP_STOP reason=flag_stop", "LOOP_STOP reason=flag_stop_during_success_wait"]:
            with self.subTest(stop=stop):
                s, m = self.step(observation=obs(forge_records=[forge(stop)], forge_state="inactive"))
                self.assertEqual(len(m), 1)
                self.assertTrue(m[0].startswith("NOTE: Forge loop stopped by flag_stop"))
                self.assertNotIn("loop_down", s["open"])
                s, m = self.step(s, obs(forge_state="inactive"), 1)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 2)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(forge_records=[forge(stop)], forge_state="inactive"), 3)
                self.assertEqual(len(m), 1)
                self.assertTrue(m[0].startswith("NOTE:"))

    def test_missing_flag_is_not_a_planned_stop(self):
        _, m = self.step(observation=obs(forge_records=[forge("LOOP_STOP reason=flag_missing")], forge_state="inactive"))
        self.assertTrue(m[0].startswith("PROBLEM: Forge loop not running"))

    def test_no_new_batch_strict_eleven_hours_and_newer_resolves(self):
        old = T0 - dt.timedelta(hours=11)
        s, m = self.step(observation=obs(batch=158, created=old))
        self.assertEqual(m, [])
        s, m = self.step(s, obs(batch=158, created=old), 1)
        self.assertTrue(m[0].startswith("PROBLEM: no new ranking batch for 11.0 h"))
        s, m = self.step(s, obs(batch=158, created=old), 2)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(batch=159, created=T0), 3)
        self.assertTrue(m[0].startswith("RESOLVED: new ranking batch 159"))

    def test_newer_batch_resolves_even_if_it_is_still_old(self):
        s, _ = self.step(observation=obs(batch=158, created=T0 - dt.timedelta(hours=20)))
        s, m = self.step(s, obs(batch=159, created=T0 - dt.timedelta(hours=12)), 1)
        self.assertEqual(len(m), 1)
        self.assertTrue(m[0].startswith("RESOLVED:"))
        s, m = self.step(s, obs(batch=159, created=T0 - dt.timedelta(hours=12)), 2)
        self.assertTrue(m[0].startswith("PROBLEM: no new ranking batch"))

    def test_batch_query_failure_preserves_last_known_and_open_problem(self):
        s, _ = self.step(observation=obs(batch=158, created=T0 - dt.timedelta(hours=12)))
        last = copy.deepcopy(s["last_batch"])
        for minute in range(1, 5):
            s, m = self.step(s, obs(err="URLError: timed out"), minute)
            self.assertEqual(m, [])
            self.assertEqual(s["last_batch"], last)
            self.assertIn("late", s["open"])

    def test_batch_query_failure_checks_eleven_hours_against_last_known(self):
        s, _ = self.step(observation=obs(batch=158, created=T0 - dt.timedelta(hours=11)))
        last = copy.deepcopy(s["last_batch"])
        s, m = self.step(s, obs(err="URLError: timed out"))
        self.assertEqual(m, [])
        s, m = self.step(s, obs(err="URLError: timed out"), 1)
        self.assertEqual(len(m), 1)
        self.assertTrue(m[0].startswith("PROBLEM: no new ranking batch for 11.0 h"))
        self.assertIn("last: batch 158 at 3:00 AM Nov 17", m[0])
        self.assertIn("newest batch unreadable: URLError: timed out", m[0])
        self.assertEqual(s["last_batch"], last)
        s, m = self.step(s, obs(batch=159, created=T0), 2)
        self.assertTrue(m[0].startswith("RESOLVED: new ranking batch 159"))

    def test_batch_query_failure_without_last_known_does_not_invent_staleness(self):
        s, m = self.step(observation=obs(err="timeout"))
        self.assertEqual(m, [])
        self.assertNotIn("late", s["open"])
        self.assertNotIn("last_batch", s)

    def test_every_vps_unit_down_needs_two_polls_then_two_active_to_resolve(self):
        for unit in (SERVICE, SITE, NGINX):
            with self.subTest(unit=unit):
                s, m = self.step(observation=obs(vps_states={unit: "inactive"}))
                self.assertEqual(m, [])
                s, m = self.step(s, obs(vps_states={unit: "failed"}), 1)
                self.assertTrue(m[0].startswith(f"PROBLEM: {unit.removesuffix('.service')} down"))
                s, m = self.step(s, obs(vps_states={unit: "failed"}), 2)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 3)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 4)
                self.assertTrue(m[0].startswith(f"RESOLVED: {unit.removesuffix('.service')} running again"))

    def test_every_systemd_failure_opens_immediately_even_when_currently_active(self):
        for unit in (SERVICE, SITE, NGINX):
            with self.subTest(unit=unit):
                failed = record(f"{unit}: Failed with result 'exit-code'.", unit)
                s, m = self.step(observation=obs(vps_records=[failed]))
                self.assertTrue(m[0].startswith("PROBLEM:"))
                s, m = self.step(s, obs(), 1)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(vps_records=[failed]), 2)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 3)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 4)
                self.assertTrue(m[0].startswith("RESOLVED:"))

    def test_release_restart_of_29_seconds_stays_silent(self):
        s, m = self.step(observation=obs(vps_states={SERVICE: "inactive"}, ok=False))
        self.assertEqual(m, [])
        s, m = w.evaluate(s, obs(), T0 + dt.timedelta(seconds=29))
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 1)
        self.assertEqual(m, [])

    def test_unit_streaks_break_on_opposite_state_or_unreachable_host(self):
        s, _ = self.step(observation=obs(vps_states={SERVICE: "inactive"}))
        s, m = self.step(s, obs(), 1)
        s, m = self.step(s, obs(vps_states={SERVICE: "inactive"}), 2)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(errors={"vps": "timeout"}), 3)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(vps_states={SERVICE: "inactive"}), 4)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(vps_states={SERVICE: "inactive"}), 5)
        self.assertTrue(m[0].startswith("PROBLEM: pe-service down"))
        s, m = self.step(s, obs(), 6)
        s, m = self.step(s, obs(errors={"vps": "timeout"}), 7)
        s, m = self.step(s, obs(), 8)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 9)
        self.assertTrue(m[0].startswith("RESOLVED: pe-service running again"))

    def test_health_two_bad_polls_and_immediate_ready_recovery(self):
        s, m = self.step(observation=obs(ok=False))
        self.assertEqual(m, [])
        s, m = self.step(s, obs(ok=False), 1)
        self.assertTrue(m[0].startswith("PROBLEM: copy service not ready"))
        s, m = self.step(s, obs(ok=False), 2)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 3)
        self.assertTrue(m[0].startswith("RESOLVED: copy service ready again"))

    def test_one_bad_health_poll_is_reset_by_ready(self):
        s, _ = self.step(observation=obs(ok=False))
        s, _ = self.step(s, obs(), 1)
        s, m = self.step(s, obs(ok=False), 2)
        self.assertEqual(m, [])

    def test_each_unreachable_host_needs_two_failures_and_one_good_read(self):
        for host in ("forge", "vps"):
            with self.subTest(host=host):
                bad = obs(errors={host: "TimeoutExpired: ssh"})
                s, m = self.step(observation=bad)
                self.assertEqual(m, [])
                s, m = self.step(s, bad, 1)
                self.assertTrue(m[0].startswith(f"PROBLEM: {host} host unreachable"))
                s, m = self.step(s, bad, 2)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 3)
                self.assertTrue(m[0].startswith(f"RESOLVED: {host} host reachable again"))

    def test_good_host_read_resets_failure_streak(self):
        s, _ = self.step(observation=obs(errors={"forge": "timeout"}))
        s, _ = self.step(s, obs(), 1)
        s, m = self.step(s, obs(errors={"forge": "timeout"}), 2)
        self.assertEqual(m, [])

    def test_no_reminders_for_open_problems(self):
        bad = obs(ok=False, errors={"forge": "timeout"})
        s, _ = self.step(observation=bad)
        s, _ = self.step(s, bad, 1)
        s, m = self.step(s, obs(ok=False, created=T0, errors={"forge": "timeout"}), 241)
        self.assertEqual(m, [])

    def test_daily_summary_once_after_nine_central(self):
        morning = dt.datetime(2026, 11, 18, 14, 59, tzinfo=UTC)
        s, m = w.evaluate(QUIET, obs(created=morning), morning)
        self.assertEqual(m, [])
        s, m = w.evaluate(s, obs(created=morning), morning + dt.timedelta(minutes=1))
        self.assertEqual(m, ["DAILY: last batch 159 at 8:59 AM Nov 18; copy service ready; no open problems"])
        s, m = w.evaluate(s, obs(created=morning), morning + dt.timedelta(minutes=2))
        self.assertEqual(m, [])

    def test_daily_includes_open_problems_and_last_known_batch_on_query_failure(self):
        s, _ = self.step(observation=obs())
        s.pop("summary_date")
        s, _ = self.step(s, obs(ok=False), 1)
        s.pop("summary_date")
        _, m = self.step(s, obs(ok=False, err="timeout"), 2)
        daily = next(x for x in m if x.startswith("DAILY:"))
        self.assertIn("last batch 159 at 1:00 PM Nov 17 (newest batch unreadable)", daily)
        self.assertIn("copy service NOT ready", daily)
        self.assertIn("copy service not ready on the cloud server", daily)

    def test_central_time_uses_daylight_saving_time(self):
        self.assertEqual(w.ct(dt.datetime(2026, 7, 1, 14, 0, tzinfo=UTC)), "9:00 AM Jul 1")
        self.assertEqual(w.ct(dt.datetime(2026, 11, 17, 15, 0, tzinfo=UTC)), "9:00 AM Nov 17")


class Events(unittest.TestCase):
    step = Rules.step

    def test_all_forge_routine_warnings_and_partial_exit_are_silent(self):
        routine = ["market participant skipped: infra tombstone", "activity wallet excluded from generation",
                   "activity wallets excluded from the generation; see per-wallet exclusion reasons",
                   "activate-next: candidate pile depleted; activated remaining wallets",
                   "clob: transient fetch error — retrying page", "events: transient fetch error — retrying page",
                   "market-scope: transient fetch error — retrying page"]
        self.assertEqual(set(routine), w.FORGE_ROUTINE)
        records = [forge(m, "WARN") for m in routine]
        records += [forge("   [collect] WARN exit 2 (partial); cache durable, continuing"),
                    forge("WARNING: transient Supabase POST failure; retry 1/4 in 2s: HTTP 503")]
        _, m = self.step(observation=obs(forge_records=records))
        self.assertEqual(m, [])

    def test_datadash_soft_failure_and_prune_warning_report(self):
        _, m = self.step(observation=obs(forge_records=[forge(DATADASH, "WARN"), forge(PRUNE)]))
        self.assertEqual(len(alerts(m)), 2)
        self.assertTrue(all(x.startswith("EVENT: Forge warning") for x in alerts(m)))
        self.assertTrue(any(DATADASH in x for x in m))
        self.assertTrue(any(PRUNE in x for x in m))

    def test_other_forge_text_warning_and_exhausted_retry_are_not_suppressed(self):
        _, m = self.step(observation=obs(forge_records=[forge("WARN: ranking degraded"),
                            forge("WARNING: transient Supabase GET failure; retries exhausted")]))
        self.assertEqual(len(alerts(m)), 2)

    def test_all_service_routine_warnings_are_silent(self):
        routine = [
            # The two bounded-retry warnings of the linked source crate.
            "rate limited by the venue; retrying after the requested wait",
            "venue reply is not one complete JSON document",
            "activity ws connect failed", "activity ws closed by peer", "activity ws read error",
            "activity ws reader produced no normalized activity row; dropping socket",
            "unparseable activity frame", "activity frames without payload", "canary control connection failed",
            "wallet scope problem ignored", "wallet scope revision ignored", "wallet scope ambiguity ignored",
            "wallet scope dropped", "live accounts poll attempt failed; retrying inside the tick",
            "admission invalidated by durable wallet fence", "position bracket: activity assets recorded raw-only",
            "snapshot worker: requests dropped (channel full)",
            "supabase sink: skipping non-winner-follow fill (no leader in idempotency key)",
            "supabase sink: skipping non-winner-follow fill (no leader)",
            "supabase sink: events dropped since last reconcile (self-healing)",
            "anchor refresh skipped because wallet is durably fenced",
            "anchor refresh skipped because causal validation fenced the wallet",
            "live reentry: capacity changed before locked apply; retrying next tick",
            "knockout: capacity changed during live reentry preparation; retrying batch",
            "maintenance: candidate fetch returned 0; backfill paused (no surviving bench rows outside the live set, or the bench predates last_trade_unix)",
        ]
        self.assertEqual(len(routine), 25)
        self.assertEqual(set(routine), w.SERVICE_ROUTINE)
        _, m = self.step(observation=obs(vps_records=[service(x) for x in routine]))
        self.assertEqual(m, [])

    def test_intervening_activity_silent_but_other_kind_and_missing_kind_report(self):
        _, m = self.step(observation=obs(vps_records=[
            service("anchor refresh deferred", kind="validation.intervening_activity")]))
        self.assertEqual(m, [])
        s, m = self.step(observation=obs(vps_records=[
            service("anchor refresh deferred", kind="validation.position_revision")]))
        self.assertEqual(len(alerts(m)), 1)
        self.assertIn("kind=validation.position_revision", m[0])
        s, m = self.step(s, obs(vps_records=[service("anchor refresh deferred")]), 1)
        self.assertEqual(len(alerts(m)), 1)

    def test_error_and_panic_report_even_with_a_routine_message(self):
        _, m = self.step(observation=obs(vps_records=[service("activity ws connect failed", "ERROR"),
                            record("thread 'tokio-runtime-worker' panicked at src/main.rs:10:2")]))
        self.assertEqual(len(alerts(m)), 2)

    def test_each_event_category_reports_once_per_24_hours(self):
        observations = [obs(forge_records=[forge(DATADASH, "WARN")]),
                        obs(vps_records=[service("supabase refresh failed; keeping current watchlist")]),
                        obs(vps_records=[record("⨯ Error: server failed", SITE)]),
                        obs(vps_records=[access()]),
                        obs(forge_records=[record(KERNEL_IO, "", "forge", PRIORITY="3", SYSLOG_IDENTIFIER="kernel")])]
        for o in observations:
            with self.subTest(observation=o):
                s, m = self.step(observation=o)
                self.assertEqual(len(alerts(m)), 1)
                s, m = self.step(s, o, 1)
                self.assertEqual(m, [])
                later = copy.deepcopy(o)
                later["created"] = T0 + dt.timedelta(hours=24)
                s["summary_date"] = "2026-11-18"
                s, m = self.step(s, later, 24 * 60 - 1)
                self.assertEqual(m, [])
                s, m = self.step(s, later, 24 * 60)
                self.assertEqual(len(alerts(m)), 1)

    def test_each_recovery_resolves_once_and_rearms_all_failure_kinds(self):
        pairs = [
            ("paper-state WAL checkpoint failed; retrying every tick", "paper-state WAL checkpoint error cleared", "ERROR"),
            ("disk free space below warning threshold", "disk free space recovered", "ERROR"),
            ("supabase refresh failed; keeping current watchlist", "live watchlist refreshed from supabase", "WARN"),
        ]
        for failure, recovery, level in pairs:
            with self.subTest(failure=failure):
                failed = [service(failure, level, kind="one"), service(failure, level, kind="two")]
                s, m = self.step(observation=obs(vps_records=failed))
                self.assertEqual(len(alerts(m)), 2)
                s, m = self.step(s, obs(vps_records=failed), 1)
                self.assertEqual(m, [])
                recovered = service(recovery, "INFO")
                s, m = self.step(s, obs(vps_records=[recovered, recovered]), 2)
                self.assertEqual(m, [f"RESOLVED: copy service {failure} cleared at 2:02 PM Nov 17 ({recovery})"])
                self.assertEqual(s["events"], {})
                s, m = self.step(s, obs(vps_records=[recovered]), 3)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(vps_records=failed), 4)
                self.assertEqual(len(alerts(m)), 2)

    def test_failure_recovery_failure_in_same_poll_reports_in_order(self):
        failure = service("disk free space below warning threshold", "ERROR")
        _, m = self.step(observation=obs(vps_records=[failure, service("disk free space recovered", "INFO"), failure]))
        self.assertEqual([x.split(":", 1)[0] for x in alerts(m)], ["EVENT", "RESOLVED", "EVENT"])

    def test_recovery_without_a_reported_failure_is_silent(self):
        for recovery in w.RECOVERIES:
            for level in ("INFO", "WARN"):
                with self.subTest(recovery=recovery, level=level):
                    s, m = self.step(observation=obs(vps_records=[service(recovery, level)]))
                    self.assertEqual(m, [])
                    self.assertEqual(s["events"], {})

    def test_events_older_than_24_hours_are_pruned_even_when_hosts_unreadable(self):
        keys = [json.dumps(["vps", SERVICE, f"failure {i}"]) for i in range(3)]
        state = {**QUIET, "events": {
            keys[0]: (T0 - dt.timedelta(hours=24, microseconds=1)).isoformat(),
            keys[1]: (T0 - dt.timedelta(hours=24)).isoformat(),
            keys[2]: (T0 - dt.timedelta(hours=1)).isoformat(),
        }}
        s, m = self.step(state, obs(errors={"forge": "timeout", "vps": "timeout"}))
        self.assertEqual(m, [])
        self.assertEqual(s["events"], {key: state["events"][key] for key in keys[1:]})
        self.assertEqual(len(state["events"]), 3)

    def test_recovery_for_a_failure_reported_over_24_hours_ago_is_silent(self):
        for recovery, failure in w.RECOVERIES.items():
            with self.subTest(recovery=recovery):
                s, _ = self.step(observation=obs(vps_records=[service(failure, "ERROR")]))
                later = T0 + dt.timedelta(hours=24, seconds=1)
                s["summary_date"] = "2026-11-18"
                s, m = w.evaluate(s, obs(vps_records=[service(recovery, "INFO")], created=later), later)
                self.assertEqual(m, [])
                self.assertEqual(s["events"], {})

    def test_recovery_does_not_rearm_unrelated_events(self):
        records = [service("disk free space below warning threshold", "ERROR"),
                   service("supabase refresh failed; keeping current watchlist")]
        s, _ = self.step(observation=obs(vps_records=records))
        s, m = self.step(s, obs(vps_records=[service("disk free space recovered", "INFO"), records[1]]), 1)
        self.assertEqual(m, ["RESOLVED: copy service disk free space below warning threshold cleared at "
                             "2:01 PM Nov 17 (disk free space recovered)"])
        self.assertEqual([json.loads(key)[2] for key in s["events"]],
                         ["supabase refresh failed; keeping current watchlist"])

    def test_json_key_uses_exact_message_and_kind_but_not_other_fields(self):
        s, _ = self.step(observation=obs(vps_records=[service("anchor refresh deferred", kind="validation.position_revision", wallet="0x123")]))
        s, m = self.step(s, obs(vps_records=[service("anchor refresh deferred", kind="validation.position_revision", wallet="0x456")]), 1)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(vps_records=[service("anchor refresh deferred", kind="validation.ledger_revision")]), 2)
        self.assertEqual(len(alerts(m)), 1)
        s, m = self.step(s, obs(vps_records=[service("failure 123"), service("failure 456")]), 3)
        self.assertEqual(len(alerts(m)), 2)

    def test_nested_bootstrap_kind_is_part_of_event_key(self):
        s, _ = self.step(observation=obs(forge_records=[forge(DATADASH, "WARN", kind="one")]))
        s, m = self.step(s, obs(forge_records=[forge(DATADASH, "WARN", kind="two")]), 1)
        self.assertEqual(len(alerts(m)), 1)

    def test_text_digits_and_hex_runs_are_masked_but_host_and_unit_are_distinct(self):
        a = "WARNING: read 123 failed for 0xabcdef deadbeef flags=0xab"
        b = "WARNING: read 456 failed for 0xfeedab 012345ab flags=0xcd"
        s, _ = self.step(observation=obs(forge_records=[forge(a)]))
        s, m = self.step(s, obs(forge_records=[forge(b)]), 1)
        self.assertEqual(m, [])
        # The same host error text belongs to separate keys on another host/unit.
        r = record("device error 123", "disk.service", PRIORITY="3")
        s, m = self.step(s, obs(vps_records=[r, {**r, "_SYSTEMD_UNIT": "other.service"}],
                              forge_records=[record("device error 123", "disk.service", "forge", PRIORITY="3")]), 2)
        self.assertEqual(len(alerts(m)), 3)

    def test_site_error_prefixes_report_and_auth_client_errors_are_silent(self):
        _, m = self.step(observation=obs(vps_records=[
            record("[auth][error] MissingCSRF: CSRF token was missing during an action callback.", SITE),
            record("[auth][error] UnknownAction: Cannot parse action at /api/auth/session", SITE)]))
        self.assertEqual(m, [])
        _, m = self.step(observation=obs(vps_records=[record("⨯ Error: server failed", SITE),
            record("[auth][error] CallbackRouteError: callback failed", SITE),
            record("[error] UnknownAction in server code", SITE)]))
        self.assertEqual(len(alerts(m)), 3)

    def test_nginx_5xx_key_is_status_method_and_path_without_query(self):
        s, m = self.step(observation=obs(vps_records=[access()]))
        self.assertIn("nginx 500 GET /api/config", m[0])
        s, m = self.step(s, obs(vps_records=[access(path="/api/config?private=two", client="198.51.100.4")]), 1)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(vps_records=[access(status=502), access(method="POST"),
                            access(path="/api/other"), access(path="/api/config/123"), access(path="/api/config/456")]), 2)
        self.assertEqual(len(alerts(m)), 5)

    def test_nginx_other_responses_silent_and_error_log_entries_report(self):
        _, m = self.step(observation=obs(vps_records=[access(status=s) for s in (200, 301, 400, 404, 499)]))
        self.assertEqual(m, [])
        clean = access(status=200)
        clean["MESSAGE"] = clean["MESSAGE"].replace("test-agent", "[error] browser")
        _, m = self.step(observation=obs(vps_records=[clean]))
        self.assertEqual(m, [])
        _, m = self.step(observation=obs(vps_records=[record(
            '2026/11/17 20:00:00 [error] 123#123: *4 connect() failed (111: Connection refused) while connecting to upstream', NGINX)]))
        self.assertEqual(len(alerts(m)), 1)

    def test_nginx_reload_notice_and_warn_silent_but_crit_and_emerg_report(self):
        quiet = [record("2026/11/18 16:40:30 [notice] 1856204#1856204: signal process started", NGINX,
                        SYSLOG_IDENTIFIER="nginx", _TRANSPORT="stdout"),
                 record('2026/11/18 16:40:30 [warn] 1856204#1856204: conflicting server name "polymarketpe.duckdns.org" '
                        "on 0.0.0.0:3000, ignored", NGINX)]
        _, m = self.step(observation=obs(vps_records=quiet))
        self.assertEqual(m, [])
        _, m = self.step(observation=obs(vps_records=[
            record("fxvps332192 nginx: 2026/11/18 16:45:00 [crit] 717#717: *9 SSL_do_handshake() failed", NGINX),
            record('2026/11/18 16:45:00 [emerg] 1856300#1856300: unknown directive "acces_log" in /etc/nginx/nginx.conf:40',
                   NGINX)]))
        self.assertEqual(len(alerts(m)), 2)

    def test_host_kernel_io_error_reports_but_sshd_and_wait_online_timeouts_do_not(self):
        noise = [record("Failed password for invalid user admin from 192.0.2.5 port 42100 ssh2", "ssh.service", PRIORITY="3", SYSLOG_IDENTIFIER="sshd"),
                 record("error: kex_exchange_identification: Connection closed by remote host", "", PRIORITY="3", SYSLOG_IDENTIFIER="sshd"),
                 record("Timeout occurred while waiting for network connectivity.", "systemd-networkd-wait-online.service", PRIORITY="3")]
        _, m = self.step(observation=obs(vps_records=noise))
        self.assertEqual(m, [])
        _, m = self.step(observation=obs(forge_records=[record(KERNEL_IO, "", "forge", PRIORITY="3", SYSLOG_IDENTIFIER="kernel")]))
        self.assertEqual(len(alerts(m)), 1)
        self.assertIn(KERNEL_IO, m[-1])

    def test_only_outside_watched_units_priorities_zero_through_three_are_host_errors(self):
        for priority in ("0", "1", "2", "3", "4", "6", "bad", None):
            with self.subTest(priority=priority):
                _, m = self.step(observation=obs(vps_records=[record("host issue", "other.service", PRIORITY=priority)]))
                self.assertEqual(len(alerts(m)), int(priority in ("0", "1", "2", "3")))
        _, m = self.step(observation=obs(vps_records=[service("disk free space below warning threshold", "ERROR") | {"PRIORITY": "3"}]))
        self.assertEqual(len(alerts(m)), 1)
        self.assertIn("copy service warning/error", m[0])

    def test_unit_alias_fields_route_systemd_records_to_unit_rules(self):
        f = forge(TEMPFAIL)
        f.pop("_SYSTEMD_USER_UNIT")
        f.update({"USER_UNIT": LOOP, "_SYSTEMD_UNIT": "user@1000.service"})
        v = record(FAILURE, "init.scope", PRIORITY="3", UNIT=SERVICE)
        _, m = self.step(observation=obs(forge_records=[f], vps_records=[v]))
        self.assertEqual(len(alerts(m)), 2)
        self.assertTrue(m[0].startswith("PROBLEM: Forge loop failing"))
        self.assertTrue(m[1].startswith("PROBLEM: pe-service down"))

    def test_large_warn_is_parsed_in_full_before_presentation_is_bounded(self):
        r = service("watchlist projection failed; retrying on refresh cadence", error="x" * 6000,
                    kind="protocol.body")
        self.assertGreater(len(r["MESSAGE"]), 4096)
        observation = obs()
        observation["hosts"]["vps"] = w.parse_host_output(
            "vps", json.dumps(r) + "\n@@ 0\nactive\nactive\nactive\n")
        s, m = self.step(observation=observation)
        self.assertEqual(len(alerts(m)), 1)
        self.assertIn("protocol.body", m[0])
        self.assertIn("watchlist projection failed; retrying on refresh cadence", next(iter(s["events"])))

    def test_non_utf8_message_byte_list_reports_with_replacement(self):
        r = record(list(b"\xff kernel read failed"), "", PRIORITY="3", SYSLOG_IDENTIFIER="kernel")
        _, m = self.step(observation=obs(vps_records=[r]))
        self.assertIn("\ufffd kernel read failed", m[0])

    def test_at_most_five_single_line_evidence_rows_without_losing_events(self):
        records = [service(f"failure {i}\nsecond line", kind=str(i)) for i in range(8)]
        s, m = self.step(observation=obs(vps_records=records))
        self.assertEqual(len(alerts(m)), 8)
        self.assertEqual(len(m) - len(alerts(m)), 5)
        self.assertEqual(len(s["events"]), 8)
        self.assertTrue(all("\n" not in line for line in m))


class Reads(unittest.TestCase):
    def test_remote_argv_initial_commands_are_constant(self):
        options = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"]
        with patch.object(w, "HOME", Path("/home/merlin")):
            self.assertEqual(w.remote_argv("forge"), options + ["pi@forge.tail72525f.ts.net",
                'journalctl --all -o json --no-pager --since=-15min | head -n 20000; echo "@@ ${PIPESTATUS[0]}"; systemctl --user is-active pe-rank-loop-15m5m.service'])
            self.assertEqual(w.remote_argv("vps"), options + ["-i", "/home/merlin/.ssh/id_personal", "sean@82.22.32.225",
                'journalctl --all -o json --no-pager --since=-15min | head -n 20000; echo "@@ ${PIPESTATUS[0]}"; systemctl is-active pe-service pe-site nginx'])
        with self.assertRaises(ValueError):
            w.remote_argv("host; id")

    def test_hostile_cursor_is_shell_quoted_in_exact_expected_argv(self):
        cursor = "c'; touch /tmp/owned; $(id); \"quoted\""
        quoted = "'c'\"'\"'; touch /tmp/owned; $(id); \"quoted\"'"
        options = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"]
        prefix = ("journalctl --all -o json --no-pager --after-cursor=" + quoted +
                  ' | head -n 20000; echo "@@ ${PIPESTATUS[0]}"; ')
        with patch.object(w, "HOME", Path("/home/merlin")):
            self.assertEqual(w.remote_argv("forge", cursor), options + ["pi@forge.tail72525f.ts.net",
                prefix + "systemctl --user is-active pe-rank-loop-15m5m.service"])
            self.assertEqual(w.remote_argv("vps", cursor), options + ["-i", "/home/merlin/.ssh/id_personal", "sean@82.22.32.225",
                prefix + "systemctl is-active pe-service pe-site nginx"])

    def test_parse_json_records_divider_and_unit_states(self):
        r = service("disk free space below warning threshold", "ERROR")
        parsed = w.parse_host_output("vps", json.dumps(r) + "\n@@ 0\nactive\ninactive\nfailed\n")
        self.assertEqual(parsed["records"], [r])
        self.assertEqual(parsed["units"], {SERVICE: "active", SITE: "inactive", NGINX: "failed"})
        self.assertEqual(w.parse_host_output("forge", "@@ 0\nactive\n")["records"], [])

    def test_parse_rejects_partial_or_malformed_output(self):
        for output in ["", "@@\nactive\n", "@@ invalid\nactive\n", "@@ 0\n",
                       "@@ 0\nactive\nactive\n", "not json\n@@ 0\nactive\n",
                       '{"MESSAGE":"lost cursor"}\n@@ 0\nactive\n']:
            with self.subTest(output=output), self.assertRaises((ValueError, KeyError)):
                w.parse_host_output("forge", output)

    def test_read_host_accepts_inactive_exit_and_sets_timeout_without_local_shell(self):
        for host, units in w.HOST_UNITS.items():
            result = subprocess.CompletedProcess([], 3, stdout="@@ 0\n" + "inactive\n" * len(units), stderr="")
            with self.subTest(host=host), patch.object(w.subprocess, "run", return_value=result) as run:
                self.assertIsNone(w.read_host(host)["error"])
                self.assertEqual(run.call_args.args, (w.remote_argv(host),))
                self.assertEqual(run.call_args.kwargs["timeout"], 120)
                self.assertNotIn("shell", run.call_args.kwargs)

    def test_stderr_noise_with_good_journal_and_unit_states_succeeds(self):
        for host, units in w.HOST_UNITS.items():
            for returncode in (0, 1, 3, 4):
                result = subprocess.CompletedProcess([], returncode,
                    stdout="@@ 0\n" + "active\n" * len(units),
                    stderr="Journal file /var/log/journal/system.journal is truncated, ignoring file.\nWelcome!\n")
                with self.subTest(host=host, returncode=returncode), \
                     patch.object(w.subprocess, "run", return_value=result):
                    parsed = w.read_host(host, "previous")
                self.assertIsNone(parsed["error"])
                self.assertEqual(parsed["units"], dict.fromkeys(units, "active"))

    def test_ssh_timeout_failure_and_masked_journal_error_are_failed_reads(self):
        cases = [subprocess.TimeoutExpired(["ssh"], 120, stderr=b"timeout notice"),
                 subprocess.CompletedProcess([], 255, stdout="", stderr="Connection refused"),
                 subprocess.CompletedProcess([], 255, stdout="@@ 0\nactive\n", stderr="Connection lost"),
                 subprocess.CompletedProcess([], 0, stdout="@@ 1\nactive\n", stderr="Failed to seek to cursor: Invalid argument"),
                 subprocess.CompletedProcess([], 0, stdout="bad JSON\n@@ 0\nactive\n", stderr="")]
        for result in cases:
            with self.subTest(result=result):
                kw = {"side_effect": result} if isinstance(result, Exception) else {"return_value": result}
                with patch.object(w.subprocess, "run", **kw):
                    error = w.read_host("forge")["error"]
                self.assertTrue(error)
                stderr = result.stderr
                if stderr:
                    self.assertIn(stderr.decode() if isinstance(stderr, bytes) else stderr, error)

    def test_failed_read_errors_include_trimmed_bounded_stderr(self):
        for output, returncode in [("", 255), ("missing status\n", 0),
                                   ("bad JSON\n@@ 0\nactive\n", 0),
                                   ("@@ 0\n", 0), ("@@ 1\nactive\n", 0)]:
            result = subprocess.CompletedProcess([], returncode, stdout=output,
                                                 stderr=" \n diagnostic: journal notice\n" + "x" * 500 + " \n")
            with self.subTest(output=output, returncode=returncode), \
                 patch.object(w.subprocess, "run", return_value=result):
                error = w.read_host("forge")["error"]
            self.assertIn("; stderr: diagnostic: journal notice ", error)
            self.assertTrue(error.endswith("…"))
            self.assertLessEqual(len(error), 200)
            self.assertNotIn("\n", error)

    def test_journal_failure_drops_only_its_host_cursor_and_next_read_uses_since(self):
        for host, units in w.HOST_UNITS.items():
            with self.subTest(host=host):
                s = {**QUIET, "cursors": {"forge": "f0", "vps": "v0"}}
                original = s["cursors"][host]
                states = "active\n" * len(units)
                failed = subprocess.CompletedProcess([], 0,
                    stdout=json.dumps(record("partial", units[0], host, "partial")) + "\n@@ 1\n" + states,
                    stderr="Failed to seek to cursor: Invalid argument")
                good = subprocess.CompletedProcess([], 0,
                    stdout=json.dumps(record("healthy", units[0], host, "fresh")) + "\n@@ 0\n" + states, stderr="")
                with patch.object(w.subprocess, "run", side_effect=[failed, failed, good]) as run:
                    for minute in (0, 1):
                        observation = obs()
                        observation["hosts"][host] = w.read_host(host, s["cursors"].get(host))
                        self.assertEqual(observation["hosts"][host]["reset_cursor"], minute == 0)
                        s, m = w.evaluate(s, observation, T0 + dt.timedelta(minutes=minute))
                        self.assertNotIn(host, s["cursors"])
                        self.assertEqual(s["cursors"], {h: c for h, c in {"forge": "f0", "vps": "v0"}.items() if h != host})
                        self.assertEqual(s["host_failures"][host], minute + 1)
                        if minute == 0:
                            self.assertEqual(m, [])
                        else:
                            self.assertTrue(m[0].startswith(f"PROBLEM: {host} host unreachable"))
                    observation = obs()
                    observation["hosts"][host] = w.read_host(host, s["cursors"].get(host))
                    s, m = w.evaluate(s, observation, T0 + dt.timedelta(minutes=2))
                self.assertEqual(run.call_args_list[0].args[0], w.remote_argv(host, original))
                self.assertEqual(run.call_args_list[1].args[0], w.remote_argv(host))
                self.assertIn("--since=-15min", run.call_args_list[1].args[0][-1])
                self.assertEqual(s["cursors"][host], "fresh")
                self.assertTrue(m[0].startswith(f"RESOLVED: {host} host reachable again"))

    def test_transport_and_parse_failures_preserve_the_cursor(self):
        cases = [subprocess.TimeoutExpired(["ssh"], 120),
                 subprocess.CompletedProcess([], 255, stdout="@@ 1\nactive\n", stderr="lost connection"),
                 subprocess.CompletedProcess([], 0, stdout="@@\nactive\n", stderr="notice"),
                 subprocess.CompletedProcess([], 0, stdout="not JSON\n@@ 0\nactive\n", stderr=""),
                 subprocess.CompletedProcess([], 0, stdout="@@ 0\n", stderr="")]
        for result in cases:
            with self.subTest(result=result):
                kw = {"side_effect": result} if isinstance(result, Exception) else {"return_value": result}
                with patch.object(w.subprocess, "run", **kw):
                    read = w.read_host("forge", "previous")
                self.assertTrue(read["error"])
                self.assertFalse(read["reset_cursor"])
                observation = obs()
                observation["hosts"]["forge"] = read
                s, _ = w.evaluate({**QUIET, "cursors": {"forge": "previous"}}, observation, T0)
                self.assertEqual(s["cursors"]["forge"], "previous")

    def test_only_a_cursor_seek_failure_resets_the_cursor(self):
        for output in ("@@ 1\n", '{"MESSAGE":"missing cursor"}\n@@ 1\n', "bad JSON\n@@ 1\n"):
            for stderr, reset in (("Failed to seek to cursor: Invalid argument", True),
                                  ("Failed to iterate through journal: Input/output error", False),
                                  ("journalctl failed", False)):
                result = subprocess.CompletedProcess([], 0, stdout=output, stderr=stderr)
                with self.subTest(output=output, stderr=stderr), \
                        patch.object(w.subprocess, "run", return_value=result):
                    read = w.read_host("forge", "previous")
                self.assertIn("journalctl exit 1", read["error"])
                self.assertEqual(read["reset_cursor"], reset)
                observation = obs()
                observation["hosts"]["forge"] = read
                s, _ = w.evaluate({**QUIET, "cursors": {"forge": "previous"}}, observation, T0)
                self.assertEqual("forge" in s["cursors"], not reset)

    def test_only_exactly_20000_records_accepts_nonzero_status_and_advances_cursor(self):
        for host, units in w.HOST_UNITS.items():
            rows = [record("healthy", units[0], host, f"c{i}") for i in range(20001)]
            for count in (19999, 20000, 20001):
                with self.subTest(host=host, count=count):
                    result = subprocess.CompletedProcess([], 3,
                        stdout="\n".join(json.dumps(r) for r in rows[:count]) + "\n@@ 141\n" + "active\n" * len(units),
                        stderr="journalctl: Broken pipe")
                    with patch.object(w.subprocess, "run", return_value=result):
                        read = w.read_host(host, "previous")
                    if count == 20000:
                        self.assertIsNone(read["error"])
                        self.assertEqual(len(read["records"]), 20000)
                        observation = obs()
                        observation["hosts"][host] = read
                        s, m = w.evaluate({**QUIET, "cursors": {host: "previous"},
                                           "host_failures": {host: 1}}, observation, T0)
                        self.assertEqual(s["cursors"][host], "c19999")
                        self.assertEqual(s["host_failures"][host], 0)
                        self.assertEqual(m, [])
                    else:
                        self.assertIn("journalctl exit 141", read["error"])
                        self.assertFalse(read["reset_cursor"])  # "Broken pipe", not a seek failure

    def test_cursors_advance_per_host_and_resume_after_failed_reads(self):
        s = {**QUIET, "cursors": {"forge": "f0", "vps": "v0"}}
        s, _ = w.evaluate(s, obs(forge_records=[{**forge(SUCCESS), "__CURSOR": "f1"},
                                              {**forge(SUCCESS), "__CURSOR": "f2"}], errors={"vps": "timeout"}), T0)
        self.assertEqual(s["cursors"], {"forge": "f2", "vps": "v0"})
        with patch.object(w, "read_host", side_effect=lambda h, c: host_result(h)) as read, \
             patch.object(w, "fetch_json", side_effect=[(200, {"ready": True}), (200, [{"batch_id": 159, "created_at": T0.isoformat()}])]):
            w.observe({"PE_HEALTH_URL": "health", "PE_SUPABASE_URL": "supabase", "PE_SUPABASE_ANON_KEY": "anon"}, s)
        self.assertCountEqual(read.call_args_list, [unittest.mock.call("forge", "f2"), unittest.mock.call("vps", "v0")])
        s, _ = w.evaluate(s, obs(vps_records=[{**service("informational", "INFO"), "__CURSOR": "v1"}]), T0)
        self.assertEqual(s["cursors"], {"forge": "f2", "vps": "v1"})
        s, _ = w.evaluate(s, obs(), T0)
        self.assertEqual(s["cursors"], {"forge": "f2", "vps": "v1"})

    def test_host_reads_run_in_parallel(self):
        barrier = threading.Barrier(2)

        def read(host, cursor):
            barrier.wait(timeout=3)
            return host_result(host)

        with patch.object(w, "read_host", side_effect=read), patch.object(w, "fetch_json", side_effect=[
                (200, {"ready": True}), (200, [{"batch_id": 159, "created_at": T0.isoformat()}])]):
            o = w.observe({"PE_HEALTH_URL": "health", "PE_SUPABASE_URL": "supabase", "PE_SUPABASE_ANON_KEY": "anon"}, {})
        self.assertTrue(o["health_ok"])
        self.assertIsNone(o["hosts"]["forge"]["error"])
        self.assertIsNone(o["hosts"]["vps"]["error"])

    def test_health_requires_http_200_and_boolean_true(self):
        cases = [(200, {"ready": True}, True), (503, {"ready": True}, False),
                 (200, {"ready": False}, False), (200, {"ready": "true"}, False),
                 (200, {"ready": 1}, False), (200, [], False)]
        for status, body, expected in cases:
            with self.subTest(status=status, body=body), patch.object(w, "read_host", side_effect=lambda h, c: host_result(h)), \
                 patch.object(w, "fetch_json", side_effect=[(status, body), (200, [{"batch_id": 159, "created_at": T0.isoformat()}])]):
                o = w.observe({"PE_HEALTH_URL": "health", "PE_SUPABASE_URL": "supabase", "PE_SUPABASE_ANON_KEY": "anon"}, {})
                self.assertEqual(o["health_ok"], expected)

    def test_observe_preserves_quick_batch_request_and_handles_fetch_errors(self):
        conf = {"PE_HEALTH_URL": "http://ready", "PE_SUPABASE_URL": "https://db/", "PE_SUPABASE_ANON_KEY": "anon"}
        with patch.object(w, "read_host", side_effect=lambda h, c: host_result(h)), patch.object(w, "fetch_json", side_effect=[
                (200, {"ready": True}), (200, [{"batch_id": "159", "created_at": "2026-11-17T20:00:00Z"}])]) as fetch:
            o = w.observe(conf, {})
        self.assertEqual(o["batch_id"], 159)
        self.assertEqual(o["created"], T0)
        self.assertEqual(fetch.call_args_list, [unittest.mock.call("http://ready", timeout=5),
            unittest.mock.call("https://db/rest/v1/ranking_batches?select=batch_id,created_at&order=batch_id.desc&limit=1",
                               {"apikey": "anon", "Authorization": "Bearer anon"}, timeout=10)])
        with patch.object(w, "read_host", side_effect=lambda h, c: host_result(h)), \
             patch.object(w, "fetch_json", side_effect=[TimeoutError("health"), ValueError("batch")]):
            o = w.observe(conf, {})
        self.assertFalse(o["health_ok"])
        self.assertIn("TimeoutError", o["health_detail"])
        self.assertIn("ValueError", o["batch_error"])
        self.assertIsNone(o["created"])


class Main(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.conf, self.state, self.flag = root / "watchdog.env", root / "state/state.json", root / "send-test-alert"
        self.conf.write_text("PE_SUPABASE_URL=https://db\nPE_SUPABASE_ANON_KEY=anon=value\n")
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, path in (("CONF", self.conf), ("STATE", self.state), ("TEST_FLAG", self.flag)):
            self.stack.enter_context(patch.object(w, name, path))
        self.clock = self.stack.enter_context(patch.object(w.dt, "datetime", wraps=dt.datetime))
        self.clock.now.return_value = T0
        self.observer = self.stack.enter_context(patch.object(w, "observe", return_value=obs()))

    def run_main(self, dry=False):
        output = io.StringIO()
        with patch.object(sys, "argv", ["pe_watchdog.py"] + (["--dry-run"] if dry else [])), redirect_stdout(output):
            w.main()
        return output.getvalue()

    def test_normal_writes_state_atomically_no_lock_and_prints_header(self):
        replace = w.os.replace
        with patch.object(w.os, "replace", wraps=replace) as atomic:
            output = self.run_main()
        self.assertTrue(output.startswith("pe-watchdog\nDAILY:"))
        atomic.assert_called_once_with(self.state.with_suffix(".tmp"), self.state)
        self.assertEqual(json.loads(self.state.read_text())["summary_date"], "2026-11-17")
        self.assertFalse(self.state.with_suffix(".tmp").exists())
        self.assertFalse(self.state.with_suffix(".lock").exists())
        conf = self.observer.call_args.args[0]
        self.assertEqual(conf["PE_SUPABASE_ANON_KEY"], "anon=value")
        self.assertEqual(conf["PE_HEALTH_URL"], "http://82.22.32.225:8080/health/ready")
        self.assertEqual(output.splitlines()[0], "pe-watchdog")

    def test_healthy_subsequent_run_stdout_is_empty(self):
        self.run_main()
        self.assertEqual(self.run_main(), "")

    def test_flag_sends_test_and_is_removed_once(self):
        self.run_main()
        self.flag.touch()
        self.assertEqual(self.run_main(), "pe-watchdog\nTEST: pe-watchdog is running on merlin (2:00 PM Nov 17)\n")
        self.assertFalse(self.flag.exists())
        self.assertEqual(self.run_main(), "")

    def test_dry_run_prints_would_be_message_without_writes_or_flag_removal(self):
        self.flag.touch()
        with patch.object(w.os, "replace") as replace:
            output = self.run_main(dry=True)
        self.assertTrue(output.startswith("pe-watchdog\nDAILY:"))
        self.assertIn("\nTEST: pe-watchdog is running on merlin", output)
        self.assertTrue(self.flag.exists())
        self.assertFalse(self.state.parent.exists())
        replace.assert_not_called()
        self.assertEqual(output, self.run_main(dry=True))

    def test_dry_run_preserves_existing_state_bytes_and_can_be_silent(self):
        self.run_main()
        before = self.state.read_bytes()
        self.assertEqual(self.run_main(dry=True), "")
        self.assertEqual(self.state.read_bytes(), before)


if __name__ == "__main__":
    sys.exit(unittest.main())
