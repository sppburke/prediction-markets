#!/usr/bin/env python3
"""Measure Polymarket `/activity` attribution latency.

The latency a copy-trader actually experiences = the gap between when a leader's
trade happens (its own `timestamp`, server clock) and when our poller first sees
it via `GET /activity?user=<wallet>` (the only wallet-attributed Polymarket feed;
the CLOB WebSocket print is wallet-anonymous — see docs/15-SOURCES.md). This sets
the lowest reliable time-to-resolution floor for copying near-resolution bets.

Method (self-contained, no DB):
  1. Seed an "active-today" wallet basket from the VOL/PNL DAY leaderboard.
  2. Establish a `baseline` = measurement start (server clock). Only trades with
     `timestamp > baseline` are counted (fresh trades during the window, never a
     backlog).
  3. Round-robin poll `/activity?user=W&type=TRADE&start=<baseline>` at a fixed
     aggregate request rate. For each newly-seen `transactionHash`:
       * upper_bound_lag = first_seen_wallclock - trade.timestamp
       * if the PRIOR poll of this wallet was already after trade.timestamp and
         did NOT contain the trade, then the trade was provably not yet visible
         at that prior poll, giving a proven
         lower_bound_lag = prior_poll_wallclock - trade.timestamp.
     The (lower, upper] interval brackets the true indexing lag to within one
     wallet-revisit interval, correcting for poll jitter.
  4. Clock skew (server - local) is read from the HTTP `Date` header and smoothed.

`trade.timestamp` is second-granular, so individual lags carry +-1 s rounding
noise; percentiles over many observations wash it out.
"""
from __future__ import annotations

import argparse
import csv
import email.utils
import json
import os
import statistics
import time
import urllib.error
import urllib.request

DATA_API = "https://data-api.polymarket.com"


def http_get(url: str, timeout: float = 10.0):
    """Return (parsed_json, server_unix_ts_from_Date_header_or_None)."""
    req = urllib.request.Request(url, headers={"User-Agent": "pe-activity-latency/1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        date_hdr = r.headers.get("Date")
        server_ts = None
        if date_hdr:
            parsed = email.utils.parsedate_tz(date_hdr)
            if parsed:
                server_ts = email.utils.mktime_tz(parsed)
    return json.loads(body), server_ts


def seed_wallets(categories: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for cat in categories:
        for ob in ("VOL", "PNL"):
            try:
                d, _ = http_get(
                    f"{DATA_API}/v1/leaderboard?orderBy={ob}&timePeriod=DAY"
                    f"&category={cat}&limit={limit}"
                )
            except (urllib.error.URLError, ValueError, TimeoutError):
                continue
            for e in d if isinstance(d, list) else []:
                w = e.get("proxyWallet")
                if w and w not in seen:
                    seen.add(w)
                    out.append(w)
            time.sleep(0.06)
    return out


def pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[k]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration-secs", type=int, default=900)
    ap.add_argument("--req-per-sec", type=float, default=10.0,
                    help="aggregate /activity poll rate; keep <=10 to coexist with a running backfill")
    ap.add_argument("--max-wallets", type=int, default=60)
    ap.add_argument("--categories", default="OVERALL,CRYPTO,SPORTS,POLITICS")
    ap.add_argument("--seed-limit", type=int, default=25)
    ap.add_argument("--out-dir", default="data/activity-latency")
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)

    cats = [c.strip() for c in a.categories.split(",") if c.strip()]
    wallets = seed_wallets(cats, a.seed_limit)[: a.max_wallets]
    if not wallets:
        print("[fatal] no seed wallets from leaderboard")
        return 1
    revisit = len(wallets) / a.req_per_sec
    print(f"[seed] {len(wallets)} active-today wallets; revisit interval ~{revisit:.1f}s")

    _, server_ts = http_get(
        f"{DATA_API}/v1/leaderboard?orderBy=VOL&timePeriod=DAY&category=OVERALL&limit=1"
    )
    local = time.time()
    skew = (server_ts - local) if server_ts else 0.0
    baseline = int((server_ts or local))
    print(f"[clock] skew(server-local)={skew:.1f}s  baseline={baseline}")

    seen_tx: set[str] = set()
    last_poll_srv: dict[str, float] = {}  # wallet -> server-aligned wallclock of prior poll
    obs: list[dict] = []
    interval = 1.0 / a.req_per_sec
    t_end = time.time() + a.duration_secs
    i = polls = errors = 0

    while time.time() < t_end:
        w = wallets[i % len(wallets)]
        i += 1
        try:
            d, server_ts = http_get(
                f"{DATA_API}/activity?user={w}&type=TRADE&start={baseline}&limit=100"
            )
            polls += 1
            recv_local = time.time()
            if server_ts:
                skew = 0.7 * skew + 0.3 * (server_ts - recv_local)
            recv = recv_local + skew  # server-aligned receive wallclock
            prior = last_poll_srv.get(w)
            for t in d if isinstance(d, list) else []:
                tx = t.get("transactionHash")
                ts = t.get("timestamp")
                if not tx or ts is None or tx in seen_tx:
                    continue
                ts = int(ts)
                # Polymarket /activity sometimes returns millisecond timestamps;
                # normalise to seconds (matches crates/service/src/trade_parser.rs:97,
                # 10-digit max = 9_999_999_999). Without this a ms response would
                # produce large negative lags and silently corrupt the stats. This
                # run's data was already seconds (verified against the Date header).
                if ts > 9_999_999_999:
                    ts //= 1000
                if ts <= baseline:
                    continue
                seen_tx.add(tx)
                upper = recv - ts
                lower = (prior - ts) if (prior is not None and prior > ts) else None
                obs.append({
                    "wallet": w, "tx": tx, "trade_ts": ts,
                    "first_seen_srv": round(recv, 3),
                    "lower_lag_s": round(lower, 3) if lower is not None else "",
                    "upper_lag_s": round(upper, 3),
                    "side": t.get("side"), "price": t.get("price"),
                    "condition_id": t.get("conditionId"),
                })
            last_poll_srv[w] = recv
        except (urllib.error.URLError, OSError, ValueError, TimeoutError):
            # OSError covers ConnectionResetError / RemoteDisconnected / socket
            # errors that urllib can raise un-wrapped mid-stream; a single reset
            # must not crash the whole measurement run (errors are counted).
            errors += 1
        time.sleep(interval)
        if polls and polls % 300 == 0:
            print(f"[progress] polls={polls} trades={len(obs)} errors={errors}")

    raw = os.path.join(a.out_dir, "activity_latency_raw.csv")
    with open(raw, "w", newline="") as f:
        wcsv = csv.DictWriter(f, fieldnames=list(obs[0].keys()) if obs else
                              ["wallet", "tx", "trade_ts", "first_seen_srv",
                               "lower_lag_s", "upper_lag_s", "side", "price", "condition_id"])
        wcsv.writeheader()
        wcsv.writerows(obs)

    uppers = [o["upper_lag_s"] for o in obs]
    lowers = [o["lower_lag_s"] for o in obs if o["lower_lag_s"] != ""]
    achieved_rps = polls / a.duration_secs if a.duration_secs else 0.0
    summary = {
        "wallets_seeded": len(wallets),
        "duration_secs": a.duration_secs,
        "target_req_per_sec": a.req_per_sec,
        "achieved_req_per_sec": round(achieved_rps, 2),
        # actual wallet-revisit interval = wallets / achieved rate (HTTP latency
        # makes this larger than the nominal target; it inflates upper_bound_lag
        # but NOT lower_bound_lag_proven, which is jitter-independent).
        "actual_revisit_interval_s": round(len(wallets) / achieved_rps, 1) if achieved_rps else None,
        "polls": polls, "errors": errors,
        "trades_observed": len(obs),
        "upper_bound_lag": {
            "n": len(uppers),
            "p50": round(pct(uppers, .50), 2), "p90": round(pct(uppers, .90), 2),
            "p95": round(pct(uppers, .95), 2), "p99": round(pct(uppers, .99), 2),
            "max": round(max(uppers), 2) if uppers else None,
            "mean": round(statistics.fmean(uppers), 2) if uppers else None,
        },
        "lower_bound_lag_proven": {
            "n": len(lowers),
            "p50": round(pct(lowers, .50), 2), "p90": round(pct(lowers, .90), 2),
            "p95": round(pct(lowers, .95), 2), "p99": round(pct(lowers, .99), 2),
            "max": round(max(lowers), 2) if lowers else None,
            "mean": round(statistics.fmean(lowers), 2) if lowers else None,
        },
        "frac_upper_le_60s": round(sum(1 for x in uppers if x <= 60) / len(uppers), 3) if uppers else None,
        "frac_upper_le_120s": round(sum(1 for x in uppers if x <= 120) / len(uppers), 3) if uppers else None,
    }
    out_json = os.path.join(a.out_dir, "activity_latency_summary.json")
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    print(f"[done] raw={raw}  summary={out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
