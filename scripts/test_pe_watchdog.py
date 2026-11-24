"""Deterministic, offline contract tests for pe_watchdog.py."""

import copy
import datetime as dt
import importlib.util
import io
import json
import re
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
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
QUIET = {}
TEMPFAIL = "   [collect] TEMPFAIL exit 75 (temporary failure); aborting run for supervised retry"
SUCCESS = "LOOP_CYCLE_END cycle=3 utc=2026-11-17T20:00:00Z status=0"
DATADASH = "winner_discovery: datadash source failed — continuing with zero counts"
PRUNE = ("WARNING: ranking_batches prune failed (RuntimeError: unavailable); history not trimmed this run — "
         "bounded and self-heals on the next successful push")
FAILURE = "pe-service.service: Failed with result 'exit-code'."
KERNEL_IO = "I/O error, dev sda, sector 123456 op 0x0:(READ) flags 0x0 phys_seg 1 prio class 2"
FEED = ["activity_ws_unavailable"]
DEGRADED = ["activity_ws_redundancy_degraded"]
FEED_WORDS = "no live trade-feed connection receiving trades"
NOT_READY = "PROBLEM: copy service not ready on the cloud server since 2:00 PM Nov 17"


def record(message, unit=SERVICE, host="vps", cursor="c1", **fields):
    field = "_SYSTEMD_USER_UNIT" if host == "forge" else "_SYSTEMD_UNIT"
    return {"MESSAGE": message, "__CURSOR": cursor, field: unit, **fields}


def service(message, level="WARN", **fields):
    return record(json.dumps({"message": message, "level": level, **fields}))


CONFIG_ERROR = ("supabase request failed: error sending request for url "
                "(https://example.supabase.co/rest/v1/service_config?select=key,value,value_type)")


def at(r, when):
    return {**r, "__REALTIME_TIMESTAMP": str(int(when.timestamp() * 1_000_000))}


def config_read(when, message="service_config poll failed; keeping last-known-good config"):
    return at(service(message, error=CONFIG_ERROR), when)


def skipped(reason, level="INFO", cursor="c1"):
    return at({**service("signal did not produce order", level, reason=reason, market="0xabc"), "__CURSOR": cursor},
              T0 - dt.timedelta(minutes=3))


def probe_line(label="clob", code=0, seconds=0.05):
    return f"{label} {code} {seconds:.6f}"


def forge(message, level=None, **fields):
    if level:
        message = json.dumps({"level": level, "fields": {"message": message, **fields}})
    return record(message, LOOP, "forge")


def host_result(host, records=(), **states):
    return {"records": list(records), "units": {u: states.get(u, "active") for u in w.HOST_UNITS[host]},
            "error": None}


def obs(forge_records=(), vps_records=(), forge_state="active", vps_states=None,
        ok=True, batch=159, created=T0 - dt.timedelta(hours=1), err=None, errors=None, issues=None, probe=None):
    hosts = {"forge": host_result("forge", forge_records, **{LOOP: forge_state}),
             "vps": host_result("vps", vps_records, **(vps_states or {}))}
    for host, error in (errors or {}).items():
        hosts[host] = {"error": error}
    detail = ("HTTP 200, ready=True" if ok else
              "; ".join(w.READY_ISSUES.get(i, i) for i in issues) if issues else "HTTP 503, ready=False")
    return {"hosts": hosts, "health_ok": ok, "health_issues": None if ok else issues, "health_detail": detail,
            "batch_id": None if err else batch, "created": None if err else created, "batch_error": err,
            "probe": probe or {}}


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

    def test_planned_stop_is_silent_until_active_and_rearms(self):
        for stop in ["LOOP_STOP reason=flag_stop", "LOOP_STOP reason=flag_stop_during_success_wait"]:
            with self.subTest(stop=stop):
                s, m = self.step(observation=obs(forge_records=[forge(stop)], forge_state="inactive"))
                self.assertEqual(m, [])
                self.assertNotIn("loop_down", s["open"])
                s, m = self.step(s, obs(forge_state="inactive"), 1)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(), 2)
                self.assertEqual(m, [])
                s, m = self.step(s, obs(forge_state="inactive"), 3)
                self.assertTrue(m[0].startswith("PROBLEM: Forge loop not running"))

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

    def test_healthy_days_send_nothing(self):
        s = QUIET
        for minutes in range(0, 48 * 60, 30):  # across two 9 AM CT mornings
            now = T0 + dt.timedelta(minutes=minutes)
            s, m = w.evaluate(s, obs(created=now - dt.timedelta(hours=1)), now)
            self.assertEqual(m, [])

    def test_central_time_uses_daylight_saving_time(self):
        self.assertEqual(w.ct(dt.datetime(2026, 7, 1, 14, 0, tzinfo=UTC)), "9:00 AM Jul 1")
        self.assertEqual(w.ct(dt.datetime(2026, 11, 17, 15, 0, tzinfo=UTC)), "9:00 AM Nov 17")


class Connections(unittest.TestCase):
    TARGET = "new connections from the cloud server to Polymarket's order service (clob.polymarket.com)"

    def run_samples(self, samples, state=None, start=0):
        """One probe a minute; returns the state and the (minute, message) list."""
        sent = []
        for minute, sample in enumerate(samples, start):
            state, messages = w.evaluate(state or QUIET, obs(probe={"clob": sample} if sample else {}),
                                         T0 + dt.timedelta(minutes=minute))
            sent += [(minute, m) for m in messages]
        return state, sent

    def slow(self, minute):
        return minute, f"PROBLEM: {self.TARGET} slow or failing since 2:{minute:02d} PM Nov 17"

    def fast(self, minute, lasted):
        return minute, f"RESOLVED: {self.TARGET} fast again (lasted {lasted})"

    def test_one_bad_sample_is_silent_and_a_second_within_thirty_minutes_opens_once(self):
        self.assertEqual(self.run_samples(["bad"] + ["ok"] * 50)[1], [])
        self.assertEqual(self.run_samples(["bad"] + ["ok"] * 28 + ["bad"])[1], [self.slow(29)])
        self.assertEqual(self.run_samples(["bad"] + ["ok"] * 29 + ["bad"])[1], [])
        self.assertEqual(self.run_samples(["bad"] * 5)[1], [self.slow(1)])

    def test_thirty_good_samples_in_a_row_resolve(self):
        state, sent = self.run_samples(["bad", "bad"] + ["ok"] * 29)
        self.assertEqual(sent, [self.slow(1)])
        state, sent = self.run_samples(["ok"], state, 31)
        self.assertEqual(sent, [self.fast(31, "0 h 30 min")])
        self.assertEqual(self.run_samples(["ok", "bad", "ok", "bad"], state, 32)[1], [self.slow(35)])

    def test_half_slow_connections_are_one_problem_and_one_recovery(self):
        self.assertEqual(self.run_samples(["bad", "ok"] * 90 + ["ok"] * 35)[1], [self.slow(2), self.fast(208, "3 h 26 min")])

    def test_a_missing_sample_neither_opens_nor_resolves(self):
        state, sent = self.run_samples(["bad"] + [None] * 29 + ["bad"])  # thirty minutes apart
        self.assertEqual(sent, [])
        state, sent = self.run_samples(["bad"] + ["ok"] * 29 + [None] * 60, state, 31)  # an hour without samples
        self.assertEqual(sent, [self.slow(31)])
        self.assertEqual(self.run_samples(["ok"], state, 121)[1], [self.fast(121, "1 h 30 min")])

    def test_endpoints_are_independent_of_each_other_and_of_the_host_reads(self):
        state, sent = QUIET, []
        for minute, (clob, data) in enumerate((("bad", "ok"), ("ok", "bad"), ("ok", "bad"))):
            state, messages = w.evaluate(state, obs(probe={"clob": clob, "data": data}, errors={"vps": "timeout"}),
                                         T0 + dt.timedelta(minutes=minute))
            sent += messages
        self.assertEqual(sent, [
            "PROBLEM: vps host unreachable since 2:01 PM Nov 17 (timeout)",
            "PROBLEM: new connections from the cloud server to Polymarket's data service (data-api.polymarket.com)"
            " slow or failing since 2:02 PM Nov 17"])

    def test_state_survives_json(self):
        state, _ = self.run_samples(["bad"])
        self.assertEqual(self.run_samples(["bad"], json.loads(json.dumps(state)), 1)[1], [self.slow(1)])


class Readiness(unittest.TestCase):
    step = Rules.step

    def answers(self, observations, state=None):
        """Apply {minute: observation} in order; return (state, {minute: messages} for minutes that sent any)."""
        s, sent = QUIET if state is None else state, {}
        for minute, observation in sorted(observations.items()):
            s, m = self.step(s, observation, minute)
            if m:
                sent[minute] = m
        return s, sent

    def test_feed_only_waits_twenty_minutes_names_reasons_and_resolves_from_the_start(self):
        s, sent = self.answers({minute: obs(ok=False, issues=FEED) for minute in range(20)})
        self.assertEqual(sent, {})
        s, m = self.step(s, obs(ok=False, issues=FEED), 20)
        self.assertEqual(m, [f"{NOT_READY} ({FEED_WORDS})"])
        s, m = self.step(s, obs(ok=False, issues=FEED), 21)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 25)
        self.assertEqual(m, ["RESOLVED: copy service ready again (lasted 0 h 25 min)"])
        self.assertEqual(s["not_ready"], 0)
        self.assertNotIn("not_ready_since", s)
        self.assertNotIn("not_ready_other", s)

    def test_replayed_november_20_night_is_silent(self):
        # Not ready, feed only, 12:03-12:05 and 12:09-12:13 AM CT; ready otherwise.
        _, sent = self.answers({minute: obs(ok=False, issues=FEED) if minute in (3, 4, 5, 9, 10, 11, 12, 13) else obs()
                                for minute in range(20)})
        self.assertEqual(sent, {})

    def test_feed_only_episode_ending_before_twenty_minutes_is_silent(self):
        observations = {minute: obs(ok=False, issues=FEED) for minute in range(19)}
        observations.update({19: obs(), 20: obs()})
        _, sent = self.answers(observations)
        self.assertEqual(sent, {})

    def test_switching_between_feed_reasons_keeps_the_clock(self):
        _, sent = self.answers({minute: obs(ok=False, issues=FEED if minute % 2 else DEGRADED) for minute in range(21)})
        self.assertEqual(sent, {20: [f"{NOT_READY} (only one live trade-feed connection receiving trades)"]})

    def test_mixed_reasons_message_at_the_second_check_naming_each(self):
        mixed = ["activity_ws_unavailable", "copy_admission_blocked"]
        _, sent = self.answers({0: obs(ok=False, issues=mixed), 1: obs(ok=False, issues=mixed)})
        self.assertEqual(sent, {1: [f"{NOT_READY} ({FEED_WORDS}; copying paused: live trade feed and polling backup"
                                    " both down)"]})

    def test_other_then_feed_only_messages_at_the_second_answer_naming_the_earlier_reason(self):
        _, sent = self.answers({0: obs(ok=False), 1: obs(ok=False, issues=FEED)})
        self.assertEqual(sent, {1: [f"{NOT_READY} ({FEED_WORDS}; earlier: HTTP 503, ready=False)"]})

    def test_feed_only_then_other_messages_at_the_second_answer(self):
        _, sent = self.answers({0: obs(ok=False, issues=FEED), 1: obs(ok=False, issues=["critical_task_failed"])})
        self.assertEqual(sent, {1: [f"{NOT_READY} (a critical task failed)"]})

    def test_other_arriving_mid_grace_messages_at_once_with_the_episode_start(self):
        observations = {minute: obs(ok=False, issues=FEED) for minute in range(5)}
        observations[5] = obs(ok=False, issues=["brand_new_reason"])
        _, sent = self.answers(observations)
        self.assertEqual(sent, {5: [f"{NOT_READY} (brand_new_reason)"]})

    def test_unexplained_failures_keep_the_two_check_rule_and_text(self):
        _, sent = self.answers({0: obs(ok=False), 1: obs(ok=False)})
        self.assertEqual(sent, {1: [f"{NOT_READY} (HTTP 503, ready=False)"]})

    def test_counter_matches_the_previous_version(self):
        # The previous version reads not_ready (plus the shared open problems): +1 per not-ready answer, 0 when ready.
        observations = [obs(ok=False, issues=FEED), obs(ok=False), obs(), obs(ok=False, issues=["copy_admission_blocked"]),
                        obs(ok=False, issues=DEGRADED), obs(ok=False, issues=FEED), obs()]
        s = QUIET
        for minute, (observation, count) in enumerate(zip(observations, [1, 2, 0, 1, 2, 3, 0])):
            s, _ = self.step(s, observation, minute)
            self.assertEqual(s["not_ready"], count)

    def test_episode_state_round_trips_through_json_mid_grace(self):
        s, _ = self.answers({minute: obs(ok=False, issues=FEED) for minute in range(10)})
        _, sent = self.answers({minute: obs(ok=False, issues=FEED) for minute in range(10, 21)}, json.loads(json.dumps(s)))
        self.assertEqual(list(sent), [20])

    def test_previous_version_state_with_a_zero_count_gets_the_full_grace(self):
        _, sent = self.answers({minute: obs(ok=False, issues=FEED) for minute in range(21)}, {"not_ready": 0})
        self.assertEqual(list(sent), [20])

    def test_previous_version_positive_count_messages_at_the_next_not_ready_answer(self):
        _, sent = self.answers({0: obs(ok=False, issues=FEED)}, {"not_ready": 1})
        self.assertEqual(sent, {0: [f"{NOT_READY} ({FEED_WORDS}; earlier: {w.UNCLASSIFIED})"]})

    def test_previous_version_open_problem_is_kept_and_resolves_from_its_start(self):
        opened = {"since": (T0 - dt.timedelta(minutes=10)).isoformat(), "text": "copy service not ready"}
        s, sent = self.answers({0: obs(ok=False, issues=FEED)}, {"not_ready": 3, "open": {"health": opened}})
        self.assertEqual(sent, {})
        s, m = self.step(s, obs(), 1)
        self.assertEqual(m, ["RESOLVED: copy service ready again (lasted 0 h 11 min)"])

    def test_stale_episode_keys_with_a_zero_count_start_a_fresh_episode(self):
        stale = {"not_ready": 0, "not_ready_since": (T0 - dt.timedelta(hours=1)).isoformat(), "not_ready_other": "old"}
        _, sent = self.answers({minute: obs(ok=False, issues=FEED) for minute in range(21)}, stale)
        self.assertEqual(sent, {20: [f"{NOT_READY} ({FEED_WORDS})"]})


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
                s, m = self.step(s, later, 24 * 60 - 1)
                self.assertEqual(m, [])
                s, m = self.step(s, later, 24 * 60)
                self.assertEqual(len(alerts(m)), 1)

    def test_every_skipped_trade_with_an_unusable_order_book_is_one_line(self):
        for why in ("/book fetch timed out", "/book fetch failed", "stale snapshot", "corrupt ask levels",
                    "missing CLOB token", "book tick contradicts admission"):
            with self.subTest(why=why):
                reason = f"price-impact book unusable: {why} (fail closed)"
                line = f"EVENT: copy service skipped a trade at 1:57 PM Nov 17 (order book: {why})"
                s, m = self.step(observation=obs(vps_records=[skipped(reason)]))
                self.assertEqual(m, [line])
                s, m = self.step(s, obs(vps_records=[skipped(reason)]), 1)  # the same record read again
                self.assertEqual(m, [])
                s, m = self.step(s, obs(vps_records=[skipped(reason, cursor="c2"), skipped(reason, cursor="c3")]), 2)
                self.assertEqual(m, [line, line])

    def test_skipped_trades_leave_the_evidence_to_other_events(self):
        reason = "price-impact book unusable: stale snapshot (fail closed)"
        many = [skipped(reason, cursor=f"c{n}") for n in range(6)] + [service("unexpected failure", "ERROR")]
        messages = self.step(observation=obs(vps_records=many))[1]
        self.assertEqual(len(alerts(messages)), 7)
        self.assertEqual(messages[7:], ["  vps pe-service.service: " + many[6]["MESSAGE"]])

    def test_normal_skip_reasons_and_other_records_give_no_skip_line(self):
        book = "price-impact book unusable: /book fetch timed out (fail closed)"
        other = [skipped(reason) for reason in (
            "series_recurrence", "price-impact ladder walk failed (fail closed)", "market resolves too far out",
            "price-impact book unusable: empty ask book (fail closed)", "current market price unavailable",
            "paper budget afforded no atomic share within the impact band (paper-only)", [book], {"a": book}, None)]
        other += [skipped(book, level="DEBUG"), service("signal produced order", "INFO", reason=book),
                  record(json.dumps({"message": "signal did not produce order", "level": "INFO", "reason": book}), SITE),
                  record("signal did not produce order " + book)]
        self.assertEqual(self.step(observation=obs(vps_records=other))[1], [])
        self.assertEqual(alerts(self.step(observation=obs(vps_records=[skipped(book, level="WARN")]))[1]),
                         ["EVENT: copy service warning/error at 2:00 PM Nov 17: signal did not produce order"])

    def test_skip_reasons_match_the_copy_service(self):
        source = (Path(__file__).resolve().parents[1] / "crates/service/src/orchestrator.rs").read_text()
        reasons = set(re.findall(r'"(price-impact book unusable: [^"]+)"', source))
        normal = {reason for reason in reasons if not w.BOOK_SKIP.fullmatch(reason)}
        self.assertEqual(normal, {"price-impact book unusable: empty ask book (fail closed)"})

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

    def test_isolated_failed_settings_reads_are_silent(self):
        # The 14 days before 2026-11-19 held five failed reads, each alone (45 min to 11 days apart).
        timed_out = "service_config poll timed out; keeping last-known-good config"
        s = QUIET
        for minutes, extra in ((0, ()), (45, (timed_out,)), (24 * 60, ())):
            now = T0 + dt.timedelta(minutes=minutes)
            s, m = w.evaluate(s, obs(vps_records=[config_read(now - dt.timedelta(seconds=20), *extra)],
                                     created=now - dt.timedelta(hours=1)), now)
            self.assertEqual(m, [])
        _, m = self.step(observation=obs(vps_records=[config_read(T0), config_read(T0 + dt.timedelta(seconds=30))]),
                         minutes=1)
        self.assertEqual(m, [])

    def test_three_failed_settings_reads_in_five_minutes_report_until_five_quiet_minutes(self):
        def sec(n):
            return T0 + dt.timedelta(seconds=n)

        s, m = self.step(observation=obs(vps_records=[config_read(sec(0)), config_read(sec(30))]), minutes=1)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(vps_records=[config_read(sec(60))]), 2)
        self.assertEqual(alerts(m), ["PROBLEM: copy service cannot read its settings from Supabase since "
                                     "2:00 PM Nov 17 (3 failed reads in 5 min; it keeps its last settings)"])
        self.assertEqual(len(m), 2)
        self.assertIn(CONFIG_ERROR, m[1])
        s, m = self.step(s, obs(vps_records=[config_read(sec(90)), config_read(sec(120))]), 3)
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 7)  # the 2:02:00 failure is still inside the window
        self.assertEqual(m, [])
        s, m = self.step(s, obs(), 8)
        self.assertEqual(m, ["RESOLVED: copy service reads its settings again (lasted 0 h 6 min)"])
        self.assertEqual(s["config_read_failures"], [])

    def test_a_backlog_of_old_failed_settings_reads_is_silent(self):
        old = [config_read(T0 - dt.timedelta(minutes=n)) for n in (30, 20, 10, 6)]
        s, m = self.step(observation=obs(vps_records=old))
        self.assertEqual(m, [])
        self.assertEqual(s["config_read_failures"], [])

    def test_unreadable_vps_keeps_the_settings_problem_open(self):
        reads = [config_read(T0 + dt.timedelta(seconds=n)) for n in (0, 30, 60)]
        s, m = self.step(observation=obs(vps_records=reads), minutes=1)
        self.assertTrue(alerts(m)[0].startswith("PROBLEM: copy service cannot read its settings"))
        for minutes in range(2, 12):
            s, m = self.step(s, obs(errors={"vps": "timeout"}), minutes)
            self.assertFalse(any("settings again" in x for x in m))
        s, m = self.step(s, obs(), 12)
        self.assertIn("RESOLVED: copy service reads its settings again (lasted 0 h 11 min)", m)

    def test_degraded_best_effort_owner_is_silent_only_during_a_coordinated_stop(self):
        degraded = "best-effort owner degraded"
        stop = at(service("shutdown signal received", "INFO"), T0)
        within = at(service(degraded, task="liquidity_snapshot_worker"), T0 + dt.timedelta(seconds=1))
        s, m = self.step(observation=obs(vps_records=[stop, within]))
        self.assertEqual(m, [])
        late = at(service(degraded, task="liquidity_snapshot_worker"), T0 + dt.timedelta(minutes=2, seconds=1))
        _, m = self.step(s, obs(vps_records=[late]), 3)
        self.assertEqual(alerts(m), ["EVENT: copy service warning/error at 2:03 PM Nov 17: " + degraded])
        # A stop seen in one read covers its degraded owners in the next read.
        s, _ = self.step(observation=obs(vps_records=[stop]))
        _, m = self.step(s, obs(vps_records=[at(service(degraded), T0 + dt.timedelta(seconds=50))]), 1)
        self.assertEqual(m, [])
        for records in ([within], [at(service(degraded), T0 - dt.timedelta(seconds=1)), stop]):
            with self.subTest(records=len(records)):
                _, m = self.step(observation=obs(vps_records=records))
                self.assertEqual(alerts(m), ["EVENT: copy service warning/error at 2:00 PM Nov 17: " + degraded])


class Probe(unittest.TestCase):
    def test_argv_is_constant(self):
        with patch.object(w, "HOME", Path("/home/merlin")):
            self.assertEqual(w.probe_argv(), [
                "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes",
                "-i", "/home/merlin/.ssh/id_personal", "sean@82.22.32.225",
                "curl -s -o /dev/null -m 10 -w 'clob %{exitcode} %{time_appconnect}\\n' https://clob.polymarket.com/"
                " & curl -s -o /dev/null -m 10 -w 'data %{exitcode} %{time_appconnect}\\n' https://data-api.polymarket.com/"
                " & wait"])

    def test_samples_from_curl_output(self):
        cases = [(probe_line(seconds=0.05), {"clob": "ok"}), (probe_line(seconds=2.0), {"clob": "ok"}),
                 (probe_line(seconds=2.000001), {"clob": "bad"}), (probe_line(seconds=6.4), {"clob": "bad"}),
                 (probe_line(code=28, seconds=0.05), {"clob": "ok"}),  # the handshake was fast, the reply was not
                 (probe_line(code=28, seconds=3.1), {"clob": "bad"}),
                 *((probe_line(code=code, seconds=0), {"clob": "bad"}) for code in (6, 7, 28, 35, 60)),
                 *((probe_line(code=code, seconds=0), {}) for code in (0, 2, 23, 127)),
                 (probe_line("github"), {}), ("clob 0", {}), ("clob 0 fast", {}), ("clob 0 -1.0", {}), ("", {}),
                 ("Welcome to Ubuntu\n" + probe_line("data", 0, 0.04) + "\n" + probe_line("clob", 35, 0), {"data": "ok", "clob": "bad"})]
        for output, expected in cases:
            with self.subTest(output=output):
                self.assertEqual(w.parse_probe(output), expected)

    def test_a_failed_call_gives_no_sample(self):
        printed = probe_line("data", 0, 0.04) + "\n"
        with patch.object(w.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, printed, "")) as run:
            self.assertEqual(w.read_probe(), {"data": "ok"})
        self.assertEqual((run.call_args.args, run.call_args.kwargs["timeout"]), ((w.probe_argv(),), 30))
        dropped = subprocess.CalledProcessError(255, "ssh", printed)
        for failure in (dropped, subprocess.TimeoutExpired("ssh", 30, printed), OSError("no ssh")):
            with self.subTest(failure=failure), patch.object(w.subprocess, "run", side_effect=failure):
                self.assertEqual(w.read_probe(), {})
        self.assertTrue(run.call_args.kwargs["check"])  # a dropped SSH session raises, like the others

    def test_observe_reports_the_probe_and_runs_it_beside_the_host_reads(self):
        barrier = threading.Barrier(3)

        def wait(*_):
            barrier.wait(timeout=3)
            return {}

        with patch.object(w, "read_host", side_effect=lambda h, c: wait() or host_result(h)), \
             patch.object(w, "read_probe", side_effect=lambda: wait() or {"clob": "bad"}), \
             patch.object(w, "fetch_json", side_effect=[(200, {"ready": True}),
                                                        (200, [{"batch_id": 159, "created_at": T0.isoformat()}])]):
            o = w.observe({"PE_HEALTH_URL": "health", "PE_SUPABASE_URL": "supabase", "PE_SUPABASE_ANON_KEY": "anon"}, {})
        self.assertEqual(o["probe"], {"clob": "bad"})
        self.assertIsNone(o["hosts"]["vps"]["error"])


class Reads(unittest.TestCase):
    def setUp(self):
        probe = patch.object(w, "read_probe", return_value={})
        probe.start()
        self.addCleanup(probe.stop)

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
        self.assertEqual(fetch.call_args_list, [unittest.mock.call("http://ready", timeout=5, error_body=True),
            unittest.mock.call("https://db/rest/v1/ranking_batches?select=batch_id,created_at&order=batch_id.desc&limit=1",
                               {"apikey": "anon", "Authorization": "Bearer anon"}, timeout=10)])
        with patch.object(w, "read_host", side_effect=lambda h, c: host_result(h)), \
             patch.object(w, "fetch_json", side_effect=[TimeoutError("health"), ValueError("batch")]):
            o = w.observe(conf, {})
        self.assertFalse(o["health_ok"])
        self.assertIn("TimeoutError", o["health_detail"])
        self.assertIn("ValueError", o["batch_error"])
        self.assertIsNone(o["created"])

    def test_fetch_json_returns_error_bodies_only_on_request_and_closes_them(self):
        feed_body = json.dumps({"ready": False, "issues": FEED}).encode()

        def fetch(body, error_body):
            error = urllib.error.HTTPError("http://ready", 503, "Service Unavailable", {}, io.BytesIO(body))
            opener = unittest.mock.Mock()
            opener.open.side_effect = error
            with patch.object(w.urllib.request, "build_opener", return_value=opener):
                try:
                    return w.fetch_json("http://ready", timeout=5, error_body=error_body), error
                except urllib.error.HTTPError as raised:
                    return raised, error

        result, error = fetch(feed_body, True)
        self.assertEqual(result, (503, {"ready": False, "issues": FEED}))
        self.assertTrue(error.fp.closed)
        hidden_suffix = feed_body + b" " * (65536 - len(feed_body)) + b"x"  # first 65,536 bytes parse
        for body in (b"", b"<html>unavailable</html>", feed_body[:30], b"[" * 10000, hidden_suffix):
            with self.subTest(size=len(body)):
                result, error = fetch(body, True)
                self.assertIs(result, error)
                self.assertTrue(error.fp.closed)
        result, error = fetch(feed_body, False)  # the Supabase read keeps raising
        self.assertIs(result, error)

    def test_observe_validates_the_readiness_reasons(self):
        conf = {"PE_HEALTH_URL": "health", "PE_SUPABASE_URL": "supabase", "PE_SUPABASE_ANON_KEY": "anon"}
        batch = (200, [{"batch_id": 159, "created_at": T0.isoformat()}])
        cases = [((503, {"ready": False, "issues": FEED}), FEED, FEED_WORDS),
                 ((503, {"ready": False, "issues": ["brand_new_reason"]}), ["brand_new_reason"], "brand_new_reason"),
                 ((503, {"ready": False, "issues": "activity_ws_unavailable"}), None, "HTTP 503, ready=False"),
                 ((503, {"ready": False, "issues": []}), None, "HTTP 503, ready=False"),
                 ((503, {"ready": False, "issues": [1]}), None, "HTTP 503, ready=False"),
                 ((503, {"ready": False, "issues": [""]}), None, "HTTP 503, ready=False"),
                 ((503, {"ready": True, "issues": FEED}), None, "HTTP 503, ready=True"),
                 ((500, {"ready": False, "issues": FEED}), None, "HTTP 500, ready=False"),
                 ((503, {"error": "maintenance"}), None, "HTTP 503, ready=None"),
                 (urllib.error.HTTPError("health", 503, "Service Unavailable", {}, io.BytesIO(b"")), None,
                  "HTTPError: HTTP Error 503: Service Unavailable")]
        for reply, issues, detail in cases:
            with self.subTest(reply=reply), patch.object(w, "read_host", side_effect=lambda h, c: host_result(h)), \
                 patch.object(w, "fetch_json", side_effect=[reply, batch]):
                o = w.observe(conf, {})
                self.assertFalse(o["health_ok"])
                self.assertEqual(o["health_issues"], issues)
                self.assertEqual(o["health_detail"], detail)

    def test_reasons_table_matches_the_copy_service(self):
        source = (Path(__file__).resolve().parents[1] / "crates/service/src/health.rs").read_text()
        reported = set(re.findall(r'issues\.push\("([a-z_]+)"\)', source))
        self.assertEqual(reported, set(w.READY_ISSUES))
        self.assertLessEqual(w.FEED_ISSUES, reported)


class Main(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.conf, self.state, self.flag = root / "watchdog.env", root / "state/state.json", root / "send-test-alert"
        self.reset = root / "reset-health-episode"
        self.conf.write_text("PE_SUPABASE_URL=https://db\nPE_SUPABASE_ANON_KEY=anon=value\n")
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, path in (("CONF", self.conf), ("STATE", self.state), ("TEST_FLAG", self.flag),
                           ("RESET_FLAG", self.reset)):
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
        self.observer.return_value = obs(vps_records=[service("unexpected failure", "ERROR")])
        replace = w.os.replace
        with patch.object(w.os, "replace", wraps=replace) as atomic:
            output = self.run_main()
        self.assertTrue(output.startswith("pe-watchdog\nEVENT: copy service warning/error"))
        atomic.assert_called_once_with(self.state.with_suffix(".tmp"), self.state)
        self.assertEqual(json.loads(self.state.read_text())["last_batch"]["id"], 159)
        self.assertFalse(self.state.with_suffix(".tmp").exists())
        self.assertFalse(self.state.with_suffix(".lock").exists())
        conf = self.observer.call_args.args[0]
        self.assertEqual(conf["PE_SUPABASE_ANON_KEY"], "anon=value")
        self.assertEqual(conf["PE_HEALTH_URL"], "http://82.22.32.225:8080/health/ready")
        self.assertEqual(output.splitlines()[0], "pe-watchdog")

    def test_healthy_runs_print_nothing(self):
        self.assertEqual(self.run_main(), "")
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
        self.assertEqual(output, "pe-watchdog\nTEST: pe-watchdog is running on merlin (2:00 PM Nov 17)\n")
        self.assertTrue(self.flag.exists())
        self.assertFalse(self.state.parent.exists())
        replace.assert_not_called()
        self.assertEqual(output, self.run_main(dry=True))

    def test_dry_run_preserves_existing_state_bytes_and_can_be_silent(self):
        self.run_main()
        before = self.state.read_bytes()
        self.assertEqual(self.run_main(dry=True), "")
        self.assertEqual(self.state.read_bytes(), before)

    def test_reset_flag_drops_earlier_episode_keys_and_is_removed_after_the_write(self):
        # State an earlier install leaves when the install fence is followed: episode keys from a run before the flag,
        # then previous-version runs that saw ready and another failure (not_ready 1).
        stale = json.dumps({"not_ready": 1, "not_ready_since": (T0 - dt.timedelta(minutes=5)).isoformat()})
        self.observer.return_value = obs(ok=False, issues=FEED)
        self.state.parent.mkdir(parents=True)
        self.state.write_text(stale)
        self.assertEqual(self.run_main(), "")  # without the flag the 5-minute-old episode is trusted
        self.state.write_text(stale)
        self.reset.touch()
        output = self.run_main()
        self.assertEqual(output, f"pe-watchdog\n{NOT_READY} ({FEED_WORDS}; earlier: {w.UNCLASSIFIED})\n")
        self.assertFalse(self.reset.exists())
        self.assertEqual(json.loads(self.state.read_text())["not_ready"], 2)

    def test_reset_flag_drops_earlier_connection_samples_and_problems_only(self):
        self.observer.return_value = obs(probe={"clob": "bad"})
        old = {"since": T0.isoformat(), "text": "from before the install"}
        self.state.parent.mkdir(parents=True)
        self.state.write_text(json.dumps({"probe": {"clob": {"bad": T0.isoformat(), "good": 0}},
                                          "open": {"slow:clob": old, "loop_failing": old}}))
        self.reset.touch()
        self.assertEqual(self.run_main(), "")  # one bad sample, none from before
        self.assertEqual(set(json.loads(self.state.read_text())["open"]), {"loop_failing"})
        self.assertIn("clob.polymarket.com", self.run_main())  # the second opens it again

    def test_failed_state_write_keeps_the_reset_flag_and_the_previous_state(self):
        self.run_main()
        before = self.state.read_bytes()
        self.reset.touch()
        for target in ("write_text", "replace"):
            failing = (patch.object(w.Path, "write_text", side_effect=OSError("disk full")) if target == "write_text"
                       else patch.object(w.os, "replace", side_effect=OSError("disk full")))
            with self.subTest(target=target), failing, self.assertRaises(OSError):
                self.run_main()
            self.assertTrue(self.reset.exists())
            self.assertEqual(self.state.read_bytes(), before)

    def test_dry_run_keeps_the_reset_flag_and_writes_nothing(self):
        self.reset.touch()
        self.assertEqual(self.run_main(dry=True), "")
        self.assertTrue(self.reset.exists())
        self.assertFalse(self.state.parent.exists())


if __name__ == "__main__":
    sys.exit(unittest.main())
