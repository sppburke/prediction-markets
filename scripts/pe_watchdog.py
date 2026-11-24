#!/usr/bin/env python3
"""Read-only Forge/VPS watchdog, run every minute by Hermes --no-agent.

Stdout goes to Telegram verbatim, only for a problem, its resolution, an event or a requested test
(owner, 2026-11-18: no routine status); otherwise no output. Python 3.11+, stdlib only.
Config: ~/.config/pe-watchdog/watchdog.env (KEY=VALUE: PE_SUPABASE_URL,
PE_SUPABASE_ANON_KEY, PE_HEALTH_URL). State: ~/.local/state/pe-watchdog/state.json.
Create ~/.config/pe-watchdog/send-test-alert for a TEST line. Create
~/.config/pe-watchdog/reset-health-episode just before installing a new version (docs/35): the
next run drops what an earlier version left in the state. --dry-run prints only what would be sent,
without writing state or removing either flag. Runs never overlap.
"""

import copy
import datetime as dt
import json
import os
import re
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from zoneinfo import ZoneInfo

HOME = Path.home()
CONF = HOME / ".config/pe-watchdog/watchdog.env"
TEST_FLAG = HOME / ".config/pe-watchdog/send-test-alert"
RESET_FLAG = HOME / ".config/pe-watchdog/reset-health-episode"
STATE = HOME / ".local/state/pe-watchdog/state.json"
CT = ZoneInfo("America/Chicago")
LATE_HOURS = 11
HOST_TIMEOUT = 120
PROBE_TIMEOUT = 30
JOURNAL_LIMIT = 20000
EVENT_WINDOW = dt.timedelta(hours=24)
LOOP = "pe-rank-loop-15m5m.service"
VPS_UNITS = ("pe-service.service", "pe-site.service", "nginx.service")
HOST_UNITS = {"forge": (LOOP,), "vps": VPS_UNITS}
SSH_OPTIONS = ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               "-o", "StrictHostKeyChecking=yes")

FORGE_ROUTINE = frozenset({
    "market participant skipped: infra tombstone",
    "activity wallet excluded from generation",
    "activity wallets excluded from the generation; see per-wallet exclusion reasons",
    "activate-next: candidate pile depleted; activated remaining wallets",
    "clob: transient fetch error — retrying page",
    "events: transient fetch error — retrying page",
    "market-scope: transient fetch error — retrying page",
})
SERVICE_ROUTINE = frozenset({
    "activity ws connect failed",
    "activity ws closed by peer",
    "activity ws read error",
    "activity ws reader produced no normalized activity row; dropping socket",
    "unparseable activity frame",
    "activity frames without payload",
    "canary control connection failed",
    "wallet scope problem ignored",
    "wallet scope revision ignored",
    "wallet scope ambiguity ignored",
    "wallet scope dropped",
    "live accounts poll attempt failed; retrying inside the tick",
    "admission invalidated by durable wallet fence",
    "position bracket: activity assets recorded raw-only",
    "snapshot worker: requests dropped (channel full)",
    "supabase sink: skipping non-winner-follow fill (no leader in idempotency key)",
    "supabase sink: skipping non-winner-follow fill (no leader)",
    "supabase sink: events dropped since last reconcile (self-healing)",
    "anchor refresh skipped because wallet is durably fenced",
    "anchor refresh skipped because causal validation fenced the wallet",
    "live reentry: capacity changed before locked apply; retrying next tick",
    "knockout: capacity changed during live reentry preparation; retrying batch",
    "maintenance: candidate fetch returned 0; backfill paused (no surviving bench rows outside the live set, or the bench predates last_trade_unix)",
    # Logged by the linked source crate before a bounded retry (crates/source-polymarket-public/src/fetcher.rs).
    "venue reply is not one complete JSON document",
})
RECOVERIES = {
    "paper-state WAL checkpoint error cleared":
        "paper-state WAL checkpoint failed; retrying every tick",
    "disk free space recovered": "disk free space below warning threshold",
    "live watchlist refreshed from supabase":
        "supabase refresh failed; keeping current watchlist",
}
# pe-service reads service_config every 30 s and keeps its last settings after a failed read
# (crates/service/src/config_poller.rs), so only repeated failures are a problem.
CONFIG_READ_FAILURES = frozenset({
    "service_config poll failed; keeping last-known-good config",
    "service_config poll timed out; keeping last-known-good config",
})
CONFIG_READ_WINDOW = dt.timedelta(minutes=5)
CONFIG_READ_PROBLEM = 3
# A coordinated pe-service stop ends within SHUTDOWN_DEADLINE = 45 s (crates/service/src/supervisor.rs);
# meanwhile a best-effort task whose input channel closed first logs "best-effort owner degraded".
SHUTDOWN_WINDOW = dt.timedelta(minutes=2)
# pe-service answers /health/ready with 503 while fewer than two of its live trade-feed connections
# deliver trades (crates/service/src/health.rs readiness_issues), and copying continues through its
# polling backup meanwhile. Polymarket's announced trading-engine restarts silence that feed for
# minutes, so a 503 listing only these reasons is a problem once it lasts FEED_QUIET_WINDOW
# (docs/_GLOSSARY.md watchdog_feed_quiet_window).
FEED_ISSUES = frozenset({"activity_ws_unavailable", "activity_ws_redundancy_degraded"})
FEED_QUIET_WINDOW = dt.timedelta(minutes=20)
# Plain words for every reason readiness_issues can report; an unknown reason is shown as named.
READY_ISSUES = {
    "polygon_source_dead": "Polygon chain source down",
    "polygon_source_stale": "no Polygon chain data for over a minute",
    "polymarket_source_stale": "no fresh Polymarket data for over a minute",
    "event_log_not_writable": "event log not writable",
    "paper_durability_uncertain": "paper ledger write failed, paper copying stopped",
    "live_journal_durability_uncertain": "live order journal write failed",
    "configuration_generation_pending": "follow-list size change still being applied",
    "critical_task_failed": "a critical task failed",
    "coordinated_shutdown": "shutting down",
    "activity_ws_sink_poisoned": "live trade feed paused by a failed source-log write",
    "activity_ws_unavailable": "no live trade-feed connection receiving trades",
    "activity_ws_redundancy_degraded": "only one live trade-feed connection receiving trades",
    "copy_admission_blocked": "copying paused: live trade feed and polling backup both down",
}
UNCLASSIFIED = "not classified (recorded before an install)"
# pe-service skips a signal whose order book it cannot use and logs it only at INFO
# (crates/service/src/orchestrator.rs). An empty ask book is a normal market state.
BOOK_SKIP = re.compile(r"price-impact book unusable: (?!empty ask book \()(.+) \(fail closed\)")
# A new connection from the VPS to each of these is through TLS in about 0.05 s. One that takes longer than
# SLOW_CONNECT_SECONDS, or never gets there, is a bad sample. Two within SLOW_WINDOW are a problem until
# SLOW_CLEAR good ones in a row (docs/_GLOSSARY.md watchdog_slow_connect).
PROBES = {"clob": ("order service", "clob.polymarket.com"), "data": ("data service", "data-api.polymarket.com")}
SLOW_CONNECT_SECONDS = 2.0
SLOW_WINDOW = dt.timedelta(minutes=30)
SLOW_CLEAR = 30
# curl exits before or during the TLS handshake: DNS, connect, timeout, TLS, certificate.
CONNECT_FAILURES = frozenset({6, 7, 28, 35, 60})
# Polymarket refused a request: an HTTP 429 that the fetcher retries, or a data-service read given up after its
# retries over an HTTP status (docs/_GLOSSARY.md watchdog_venue_refusals). The given-up messages carry other errors too.
VENUE_RETRY = "rate limited by the venue; retrying after the requested wait"
VENUE_GIVEN_UP = frozenset({"fixed-end activity reconciliation will retry", "anchor refresh deferred"})
VENUE_ERROR = re.compile(r"[^:]+: (?:activity|positions) fetch failed for \S+: "
                         r"(?:transient error: HTTP \d+|rate limited: retry after \d+s)")
VENUE_REFUSALS = 12
VENUE_WINDOW = dt.timedelta(minutes=30)
VENUE_CLEAR = 20
# Linux logs at information level that it reset a USB device. Forge's drives by USB port:
DRIVE_RESET = re.compile(r"usb (\S+): reset .* USB device number \d+ ")
FORGE_DRIVES = {"3-1": "cache drive", "5-1": "second drive"}
PUBLISH_RETRY = re.compile(
    r"WARNING: transient Supabase \S+ failure; retry \d+/\d+ in \d+s:"
)
LOOP_SUCCESS = re.compile(r"\bLOOP_(?:CYCLE_END|RESUME_END)\b.*\bstatus=0(?:\s|$)")
# Match combined access logs before checking error-log severity markers: a user agent
# or request path can itself contain '[error]'. Syslog may prepend a program name.
NGINX_ACCESS = re.compile(
    r'\S+ \S+ \S+ \[[^\]]+\] "(?P<method>\S+) (?P<path>\S+) [^"]+" '
    r'(?P<status>\d{3}) (?:\d+|-) "[^"]*" "[^"]*"'
)
# Only error-level entries are site errors: error_log defaults to level error, and every
# `nginx -s reload` prints "[notice] ... signal process started" on stdout.
NGINX_ERROR = re.compile(r"\[(?:emerg|alert|crit|error)\]")


def fetch_json(url, headers=None, timeout=8, error_body=False):
    """(status, parsed JSON). error_body=True also returns an HTTP error reply's JSON body (the
    readiness reply lists its reasons); one over 65,536 bytes or not JSON re-raises the HTTPError."""
    req = urllib.request.Request(url, headers=headers or {})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read(65536) or b"null")
    except urllib.error.HTTPError as error:
        if not error_body:
            raise
        with error:
            raw = error.read(65537)
            try:
                if len(raw) > 65536:
                    raise ValueError("reply over 65,536 bytes")
                return error.code, json.loads(raw)
            except (ValueError, RecursionError):  # deep nesting overflows the parser
                raise error from None


def remote_argv(host, cursor=None):
    """Only the stored cursor is variable in the remote shell command."""
    resume = "--after-cursor=" + shlex.quote(cursor) if cursor else "--since=-15min"
    command = ("journalctl --all -o json --no-pager " + resume +
               f" | head -n {JOURNAL_LIMIT}; " + 'echo "@@ ${PIPESTATUS[0]}"; ')
    if host == "forge":
        return [*SSH_OPTIONS, "pi@forge.tail72525f.ts.net",
                command + "systemctl --user is-active pe-rank-loop-15m5m.service"]
    if host == "vps":
        return [*SSH_OPTIONS, "-i", str(HOME / ".ssh/id_personal"), "sean@82.22.32.225",
                command + "systemctl is-active pe-service pe-site nginx"]
    raise ValueError("unknown watchdog host")


def parse_host_output(host, output):
    lines = output.splitlines()
    divider = next((i for i, line in enumerate(lines) if re.fullmatch(r"@@ [0-9]+", line)), None)
    if divider is None:
        raise ValueError("missing journalctl exit status")
    journal_status = int(lines[divider].split()[1])
    record_lines = [line for line in lines[:divider] if line.strip()]
    if journal_status != 0 and len(record_lines) != JOURNAL_LIMIT:
        return {"error": f"journalctl exit {journal_status} ({len(record_lines)} records)",
                "journal_failed": len(record_lines) < JOURNAL_LIMIT}
    records = [json.loads(line) for line in record_lines]
    if any(not isinstance(r, dict) or not isinstance(r.get("__CURSOR"), str)
           or not r["__CURSOR"] for r in records):
        raise ValueError("journal record missing __CURSOR")
    states = lines[divider + 1:]
    if len(states) != len(HOST_UNITS[host]) or any(not s.strip() for s in states):
        raise ValueError("incomplete systemctl states")
    return {"records": records, "units": dict(zip(HOST_UNITS[host], states)), "error": None}


def read_host(host, cursor=None):
    stderr, reset_cursor = "", False
    try:
        result = subprocess.run(remote_argv(host, cursor), capture_output=True, text=True,
                                encoding="utf-8", errors="replace", timeout=HOST_TIMEOUT,
                                check=False)
        stderr = result.stderr
        # The final systemctl status is independent of journalctl's pipeline status.
        # SSH transport failures use 255; stderr may contain harmless remote notices.
        if result.returncode == 255:
            raise ValueError("SSH exit 255")
        parsed = parse_host_output(host, result.stdout)
        if parsed["error"]:
            # Only a cursor journalctl cannot seek to is unrecoverable; any other journal
            # error keeps the cursor so the next read resumes without a gap.
            reset_cursor = bool(cursor and parsed["journal_failed"]
                                and "Failed to seek to cursor" in stderr)
            raise ValueError(parsed["error"])
        return parsed
    except Exception as error:
        stderr = getattr(error, "stderr", None) or stderr
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        detail = one_line(f"{type(error).__name__}: {error}", 100)
        if stderr.strip():
            detail += "; stderr: " + one_line(stderr.strip(), 90)
        return {"error": detail, "reset_cursor": reset_cursor}


def probe_argv():
    """remote_argv's VPS login, running two anonymous HTTPS requests at once; their bodies are discarded."""
    curls = " & ".join(
        f"curl -s -o /dev/null -m 10 -w '{label} %{{exitcode}} %{{time_appconnect}}\\n' https://{host}/"
        for label, (_, host) in PROBES.items())
    return [*remote_argv("vps")[:-1], f"{curls} & wait"]


def parse_probe(output):
    """label -> "ok" or "bad" for each endpoint with a usable `label exitcode seconds` line."""
    samples = {}
    for line in output.splitlines():
        match = re.fullmatch(r"(\w+) (\d+) (\d+(?:\.\d+)?)", line.strip())
        if not match or match[1] not in PROBES:
            continue
        exitcode, seconds = int(match[2]), float(match[3])
        if seconds > 0:
            samples[match[1]] = "ok" if seconds <= SLOW_CONNECT_SECONDS else "bad"
        elif exitcode in CONNECT_FAILURES:
            samples[match[1]] = "bad"
    return samples


def read_probe():
    """The samples of one probe; a failed call gives none."""
    try:
        result = subprocess.run(probe_argv(), capture_output=True, text=True, encoding="utf-8", errors="replace",
                                timeout=PROBE_TIMEOUT, check=True)
        return parse_probe(result.stdout)
    except Exception:
        return {}


def observe(conf, state):
    """Parsed hosts plus health_ok/issues/detail, batch_id/created, batch_error and probe (label -> sample).

    Each successful host has records (journal dictionaries) and units (canonical
    unit -> is-active state); a failed host has an error string and may request
    reset_cursor. health_issues is the reason list of a 503 ready=false reply, else None.
    created is an aware UTC datetime or None. All I/O is outside evaluate.
    """
    with ThreadPoolExecutor(max_workers=3) as pool:
        reads = {h: pool.submit(read_host, h, state.get("cursors", {}).get(h))
                 for h in HOST_UNITS}
        probe = pool.submit(read_probe)
        try:
            status, body = fetch_json(conf["PE_HEALTH_URL"], timeout=5, error_body=True)
            is_object = isinstance(body, dict)
            ready = body.get("ready") if is_object else body
            issues = body.get("issues") if is_object else None
            health_ok = status == 200 and is_object and ready is True
            health_issues = (issues if status == 503 and ready is False and isinstance(issues, list)
                             and issues and all(isinstance(i, str) and i for i in issues) else None)
            health_detail = ("; ".join(READY_ISSUES.get(i, i) for i in health_issues) if health_issues
                             else f"HTTP {status}, ready={ready!r}")
        except Exception as error:
            health_ok, health_issues = False, None
            health_detail = f"{type(error).__name__}: {error}"[:200]
        batch_id = created = batch_error = None
        try:
            key = conf["PE_SUPABASE_ANON_KEY"]
            url = (conf["PE_SUPABASE_URL"].rstrip("/") +
                   "/rest/v1/ranking_batches?select=batch_id,created_at&order=batch_id.desc&limit=1")
            status, rows = fetch_json(url, {"apikey": key, "Authorization": f"Bearer {key}"}, timeout=10)
            row = rows[0]
            created = dt.datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            batch_id = int(row["batch_id"])
        except Exception as error:
            batch_id = created = None
            batch_error = f"{type(error).__name__}: {error}"[:200]
        return {"hosts": {h: f.result() for h, f in reads.items()}, "probe": probe.result(),
                "health_ok": health_ok, "health_issues": health_issues, "health_detail": health_detail,
                "batch_id": batch_id, "created": created, "batch_error": batch_error}


def ct(t):
    return t.astimezone(CT).strftime("%-I:%M %p %b %-d")


def one_line(text, limit=500):
    text = " ".join(text.splitlines())
    return text if len(text) <= limit else text[:limit - 1] + "…"


def message_parts(record):
    raw = record.get("MESSAGE", "")
    if isinstance(raw, list):
        raw = bytes(raw).decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        raw = ""
    try:
        body = json.loads(raw)
    except ValueError:
        body = None
    if isinstance(body, dict):
        fields = body.get("fields")
        fields = fields if isinstance(fields, dict) else body
        message = fields.get("message", "")
        return raw, str(message), str(body.get("level", "")), fields.get("kind"), "kind" in fields, True, fields
    return raw, raw, "", None, False, False, {}


def record_time(record):
    return dt.datetime.fromtimestamp(int(record["__REALTIME_TIMESTAMP"]) / 1_000_000, dt.timezone.utc)


def record_unit(host, record):
    fields = ("_SYSTEMD_USER_UNIT", "USER_UNIT") if host == "forge" else ("_SYSTEMD_UNIT", "UNIT")
    for field in fields:
        if record.get(field) in HOST_UNITS[host]:
            return record[field]
    for field in (*fields, "_SYSTEMD_UNIT", "UNIT"):
        if record.get(field):
            return record[field]
    return ""


def masked_text(text):
    # Short bare words such as 'bad' are not identifiers; prefixed hex always is.
    text = re.sub(r"\b(?:0x[0-9a-fA-F]+|[0-9a-fA-F]{6,})\b", "#", text)
    return re.sub(r"\d+", "#", text)


def host_noise(record, unit, message):
    identities = (unit, record.get("SYSLOG_IDENTIFIER", ""), record.get("_COMM", ""))
    if any(i.startswith("sshd") or i == "ssh.service" for i in identities):
        return True
    return (any("systemd-networkd-wait-online" in i for i in identities)
            and re.search(r"timeout|timed out", message, re.IGNORECASE) is not None)


def evaluate(state, observation, now):
    """Pure rule step: (new JSON-serializable state, plain-text message lines).

    See observe's observation schema. now is an aware UTC datetime. Failed reads
    break unit poll streaks; a failed cursor seek requests a cursor reset. Recovery
    events resolve matching recent message keys (including every kind variant).
    Failed settings reads count by journal time, so a backlog of old ones stays silent; refused
    Polymarket requests are compared with each other by it, so a backlog of them is reported.
    A not-ready episode whose every answer lists only FEED_ISSUES stays silent until it lasts
    FEED_QUIET_WINDOW; any other not-ready answer in it keeps the two-answer rule. not_ready is
    the counter the previous version also keeps; not_ready_since/not_ready_other describe the episode.
    """
    s = copy.deepcopy(state)
    for key in ("open", "cursors", "events", "host_failures", "unit_polls", "probe"):
        s.setdefault(key, {})
    s.setdefault("config_read_failures", [])
    venue = s.setdefault("venue", {"refusals": {}, "quiet": 0})  # cursor -> journal time
    s["events"] = {key: last for key, last in s["events"].items()
                   if now - dt.datetime.fromisoformat(last) <= EVENT_WINDOW}
    messages, evidence = [], []

    def open_problem(key, text, **details):
        if key not in s["open"]:
            s["open"][key] = {"since": now.isoformat(), "text": text, **details}
            messages.append(f"PROBLEM: {text}")

    def close_problem(key, text):
        if key in s["open"]:
            since = dt.datetime.fromisoformat(s["open"].pop(key)["since"])
            mins = int((now - since).total_seconds() // 60)
            messages.append(f"RESOLVED: {text} (lasted {mins // 60} h {mins % 60} min)")

    def event(host, unit, parts, label, identity=None):
        raw, message, level, kind, has_kind, structured, _ = parts
        if identity is None:
            identity = [message] + ([kind] if has_kind else []) if structured else [masked_text(raw)]
        key = json.dumps([host, unit, *identity], ensure_ascii=False)
        last = s["events"].get(key)
        if last is None or now - dt.datetime.fromisoformat(last) >= EVENT_WINDOW:
            s["events"][key] = now.isoformat()
            detail = message + (f" (kind={kind})" if has_kind else "")
            messages.append(f"EVENT: {label} at {ct(now)}: {one_line(detail, 200)}")
            if len(evidence) < 5:
                evidence.append(f"  {host} {unit or 'host'}: {one_line(raw)}")

    def event_once(host, unit, message, record, text):
        key = json.dumps([host, unit, message, record["__CURSOR"]])
        if key not in s["events"]:
            s["events"][key] = now.isoformat()
            messages.append(f"EVENT: {text}")

    for host, units in HOST_UNITS.items():
        result = observation["hosts"][host]
        host_key = "host:" + host
        if result.get("error"):
            if result.get("reset_cursor"):
                s["cursors"].pop(host, None)
                if host == "vps":
                    venue["quiet"] = 0  # the next read reaches back only 15 minutes
            s["host_failures"][host] = s["host_failures"].get(host, 0) + 1
            if s["host_failures"][host] >= 2:
                open_problem(host_key, f"{host} host unreachable since {ct(now)} ({one_line(result['error'], 200)})")
            for unit in units:
                s["unit_polls"][unit] = {"down": 0, "active": 0}
            continue
        s["host_failures"][host] = 0
        close_problem(host_key, f"{host} host reachable again")
        records = result["records"]
        if records:
            s["cursors"][host] = records[-1]["__CURSOR"]
        failed_units = set()
        config_read_raw = None
        refused = False
        for record in records:
            unit = record_unit(host, record)
            parts = message_parts(record)
            raw, message, level, kind, has_kind, structured, fields = parts
            if unit not in units:
                reset = host == "forge" and record.get("_TRANSPORT") == "kernel" and DRIVE_RESET.match(message)
                if reset:
                    drive = FORGE_DRIVES.get(reset[1], "USB device")
                    event_once(host, unit, message, record, f"Linux reset Forge's {drive} (USB port {reset[1]}) at"
                               f" {ct(record_time(record))}")
                try:
                    priority = int(record.get("PRIORITY", 6))
                except (TypeError, ValueError):
                    continue
                if 0 <= priority <= 3 and not host_noise(record, unit, message):
                    event(host, unit, parts, f"{host} host error")
                continue
            if host == "forge":
                if (any(marker in raw for marker in ("TEMPFAIL", "FATAL", "panicked", "Traceback", "Failed with result"))
                        or level == "ERROR"):
                    open_problem("loop_failing", f"Forge loop failing since {ct(now)} ({one_line(message, 200)})")
                if LOOP_SUCCESS.search(message) or re.search(r"\bLOOP_SUCCESS\b", message):
                    close_problem("loop_failing", "Forge loop successful again")
                if re.search(r"\bLOOP_STOP reason=flag_stop\S*", message):
                    s["planned_stop"] = True
                warning = (level == "WARN" and message not in FORGE_ROUTINE if structured else
                           ("WARNING:" in raw or "WARN:" in raw) and not PUBLISH_RETRY.search(raw))
                if warning:
                    event(host, unit, parts, "Forge warning")
                continue
            if "Failed with result" in message:
                failed_units.add(unit)
                open_problem("down:" + unit, f"{unit.removesuffix('.service')} down on the cloud server since {ct(now)}")
            if unit == "pe-service.service":
                if message == "shutdown signal received":
                    s["service_shutdown"] = record_time(record).isoformat()
                skip = BOOK_SKIP.fullmatch(str(fields.get("reason", "")))
                if skip and level == "INFO" and message == "signal did not produce order":
                    event_once(host, unit, message, record, f"copy service skipped a trade at {ct(record_time(record))}"
                               f" (order book: {skip[1]})")
                if level == "WARN" and (message == VENUE_RETRY or message in VENUE_GIVEN_UP
                                        and VENUE_ERROR.fullmatch(str(fields.get("error", "")))):
                    refused = True
                    refusals = {**venue["refusals"], record["__CURSOR"]: record_time(record).isoformat()}
                    venue["refusals"] = dict(list(refusals.items())[-VENUE_REFUSALS:])
                    times = sorted(map(dt.datetime.fromisoformat, venue["refusals"].values()))
                    if len(times) == VENUE_REFUSALS and times[-1] - times[0] <= VENUE_WINDOW:
                        open_problem("venue", "Polymarket repeatedly refused requests from the copy service"
                                     f" ({VENUE_REFUSALS} refusals logged from {ct(times[0])} to {ct(times[-1])}); the"
                                     " service retries by itself, but a copy could be late or skipped")
                    continue
                if message in CONFIG_READ_FAILURES:
                    s["config_read_failures"].append(record_time(record).isoformat())
                    config_read_raw = raw
                    continue
                failure = RECOVERIES.get(message)
                if failure:
                    cleared = False
                    for key in list(s["events"]):
                        if json.loads(key)[:3] == [host, unit, failure]:
                            del s["events"][key]
                            cleared = True
                    if cleared:
                        messages.append(f"RESOLVED: copy service {failure} cleared at {ct(now)} ({message})")
                    continue
                stopping = (message == "best-effort owner degraded" and "service_shutdown" in s and
                            dt.timedelta(0) <= record_time(record) - dt.datetime.fromisoformat(s["service_shutdown"])
                            <= SHUTDOWN_WINDOW)
                routine = message in SERVICE_ROUTINE or stopping or (
                    message == "anchor refresh deferred" and kind == "validation.intervening_activity")
                if level == "ERROR" or "panicked" in raw or (level == "WARN" and not routine):
                    event(host, unit, parts, "copy service warning/error")
            elif unit == "pe-site.service":
                auth_noise = ("[auth][error]" in raw and
                              re.search(r"\b(?:MissingCSRF|UnknownAction)\b", raw))
                if ("⨯" in raw or "[error]" in raw) and not auth_noise:
                    event(host, unit, parts, "site error")
            elif unit == "nginx.service":
                access = NGINX_ACCESS.search(raw)
                if access:
                    status = access["status"]
                    if 500 <= int(status) <= 599:
                        identity = [status, access["method"], access["path"].split("?", 1)[0]]
                        event(host, unit, parts, "site error (nginx " + " ".join(identity) + ")", identity)
                elif NGINX_ERROR.search(raw):
                    event(host, unit, parts, "site error (nginx error log)")
        if host == "vps":
            reads = [t for t in s["config_read_failures"]
                     if now - dt.datetime.fromisoformat(t) <= CONFIG_READ_WINDOW]
            s["config_read_failures"] = reads
            if len(reads) >= CONFIG_READ_PROBLEM and "config_reads" not in s["open"]:
                open_problem("config_reads", "copy service cannot read its settings from Supabase since "
                             f"{ct(dt.datetime.fromisoformat(reads[0]))} ({len(reads)} failed reads in 5 min;"
                             " it keeps its last settings)")
                if config_read_raw:
                    evidence.append(f"  vps pe-service.service: {one_line(config_read_raw)}")
            elif not reads:
                close_problem("config_reads", "copy service reads its settings again")
            if refused:
                venue["quiet"] = 0
            # A full read may have stopped before newer refusals, and a stopped service logs none.
            elif ("venue" in s["open"] and len(records) < JOURNAL_LIMIT
                  and result["units"]["pe-service.service"] == "active"):
                venue["quiet"] += 1
                if venue["quiet"] >= VENUE_CLEAR:
                    venue["refusals"] = {}
                    close_problem("venue", "the copy service logged no more of these Polymarket refusals in"
                                  f" {VENUE_CLEAR} completed checks")
        for unit in units:
            active = result["units"][unit] == "active"
            if host == "forge":
                if active:
                    close_problem("loop_down", "Forge loop running again")
                    s["planned_stop"] = False
                elif not s.get("planned_stop"):
                    # A flag_stop is planned and silent; if it lasts, the no-new-batch check below reports it.
                    open_problem("loop_down", f"Forge loop not running since {ct(now)}")
            else:
                polls = s["unit_polls"].setdefault(unit, {"down": 0, "active": 0})
                polls["down"] = 0 if active else polls["down"] + 1
                polls["active"] = polls["active"] + 1 if active and unit not in failed_units else 0
                if polls["down"] >= 2:
                    open_problem("down:" + unit, f"{unit.removesuffix('.service')} down on the cloud server since {ct(now)}")
                if polls["active"] >= 2:
                    close_problem("down:" + unit, f"{unit.removesuffix('.service')} running again")

    for label, sample in observation["probe"].items():
        target = "new connections from the cloud server to Polymarket's {} ({})".format(*PROBES[label])
        probe = s["probe"].setdefault(label, {"bad": None, "good": 0})
        if sample == "bad":
            if probe["bad"] and now - dt.datetime.fromisoformat(probe["bad"]) < SLOW_WINDOW:
                open_problem("slow:" + label, f"{target} slow or failing since {ct(now)}")
            probe.update(bad=now.isoformat(), good=0)
        else:
            probe["good"] += 1
            if probe["good"] >= SLOW_CLEAR:
                close_problem("slow:" + label, f"{target} fast again")

    previous = s.get("not_ready", 0)
    if observation["health_ok"]:
        s["not_ready"] = 0
        s.pop("not_ready_since", None)
        s.pop("not_ready_other", None)
        close_problem("health", "copy service ready again")
    else:
        s["not_ready"] = previous + 1
        if previous == 0:
            s["not_ready_since"] = now.isoformat()
            s.pop("not_ready_other", None)
        elif "not_ready_since" not in s:
            # Earlier answers of this episode were seen by another version: treat them as other.
            s["not_ready_since"] = s["open"].get("health", {}).get("since", now.isoformat())
            s["not_ready_other"] = UNCLASSIFIED
        issues = observation["health_issues"]
        feed_only = bool(issues) and FEED_ISSUES.issuperset(issues)
        if not feed_only:
            s["not_ready_other"] = observation["health_detail"]
        since = dt.datetime.fromisoformat(s["not_ready_since"])
        if s["not_ready"] >= 2 and (s.get("not_ready_other") or now - since >= FEED_QUIET_WINDOW):
            detail = observation["health_detail"]
            if feed_only and s.get("not_ready_other"):
                detail += f"; earlier: {s['not_ready_other']}"
            open_problem("health", f"copy service not ready on the cloud server since {ct(since)}"
                         f" ({one_line(detail, 200)})", since=since.isoformat())

    if not observation["batch_error"]:
        batch_id, created = observation["batch_id"], observation["created"]
        s["last_batch"] = {"id": batch_id, "created": created.isoformat()}
    last = s.get("last_batch")
    if last:
        batch_id, created = last["id"], dt.datetime.fromisoformat(last["created"])
        late = s["open"].get("late")
        age_h = (now - created).total_seconds() / 3600
        if not observation["batch_error"] and late and batch_id > late["batch_id"]:
            close_problem("late", f"new ranking batch {batch_id} published at {ct(created)}")
        elif age_h > LATE_HOURS:
            unreadable = (f"; newest batch unreadable: {one_line(observation['batch_error'], 200)}"
                          if observation["batch_error"] else "")
            open_problem("late", f"no new ranking batch for {age_h:.1f} h (last: batch {batch_id} at {ct(created)});"
                         " the Forge loop may be stalled or stopped" + unreadable, batch_id=batch_id)
    return s, messages + evidence


def main():
    dry = "--dry-run" in sys.argv[1:]
    conf = dict(line.split("=", 1) for line in CONF.read_text().splitlines() if "=" in line)
    conf.setdefault("PE_HEALTH_URL", "http://82.22.32.225:8080/health/ready")
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    reset = RESET_FLAG.exists()
    if reset:
        # First run after an install: episode keys written before it are never trusted.
        state.pop("not_ready_since", None)
        state.pop("not_ready_other", None)
        state.pop("probe", None)
        state.pop("venue", None)
        for key in ("venue", *("slow:" + label for label in PROBES)):
            state.get("open", {}).pop(key, None)
    now = dt.datetime.now(dt.timezone.utc)
    new_state, messages = evaluate(state, observe(conf, state), now)
    test = TEST_FLAG.exists()
    if test:
        messages.append(f"TEST: pe-watchdog is running on merlin ({ct(now)})")
    if not dry:
        STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(new_state))
        os.replace(tmp, STATE)
        if test:
            TEST_FLAG.unlink()
        if reset:
            RESET_FLAG.unlink()
    if messages:
        print("pe-watchdog\n" + "\n".join(messages))


if __name__ == "__main__":
    main()
