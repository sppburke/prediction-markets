#!/usr/bin/env python3
"""Push a latency-shifted wallet ranking (pass-2 output) to Supabase as one append-only
EPOCH batch: insert a `ranking_batches` row (config + provenance), then the top-N
`ranking_entries`. The VPS reads `latest_ranking` and runs the absolute-loss demotion
test (project_copytrade_knockout_policy).

REST-only (PostgREST) via the SUPABASE_SECRET_KEY in env — no DB driver, no committed
secret. Idempotent batches are NOT enforced (append-only by design: every run = a new
epoch, so the "wholesale-swap-at-frequency-X" replay has full history).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.request


def _req(method: str, url: str, key: str, body=None, prefer: str | None = None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ranked-csv", required=True, help="pass-2 latency_shift_ranked.csv")
    ap.add_argument("--top-n", type=int, default=200)
    ap.add_argument("--band-lo", type=float, default=0.15)
    ap.add_argument("--band-hi", type=float, default=0.85)
    ap.add_argument("--ttr-floor-secs", type=int, default=30)
    ap.add_argument("--ttr-max-secs", type=int, default=259200)
    ap.add_argument("--latency-shift-secs", type=int, default=20)
    ap.add_argument("--universe-size", type=int, default=0)
    ap.add_argument("--git-sha", default="")
    ap.add_argument("--notes", default="")
    a = ap.parse_args()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SECRET_KEY")
    if not url or not key:
        print("FATAL: set SUPABASE_URL and SUPABASE_SECRET_KEY in env (.env)", file=sys.stderr)
        return 1
    url = url.rstrip("/")

    # Read pass-2 ranking; keep survivors first, then by latency-shifted t-stat desc.
    rows = list(csv.DictReader(open(a.ranked_csv, newline="")))
    def key_fn(r):
        t = r.get("tstat_net_ls", "")
        return (r.get("survives", "").lower() == "true", float(t) if t not in ("", None) else -9.0)
    rows.sort(key=key_fn, reverse=True)
    top = rows[: a.top_n]
    if not top:
        print("FATAL: no rows in ranked CSV", file=sys.stderr)
        return 1

    batch = {
        "git_sha": a.git_sha or None,
        "band_lo": a.band_lo, "band_hi": a.band_hi,
        "ttr_floor_secs": a.ttr_floor_secs, "ttr_max_secs": a.ttr_max_secs,
        "latency_shift_secs": a.latency_shift_secs,
        "universe_size": a.universe_size or len(rows),
        "notes": a.notes or None,
    }
    try:
        st, rep = _req("POST", f"{url}/rest/v1/ranking_batches", key, body=batch,
                       prefer="return=representation")
    except urllib.error.HTTPError as e:
        print(f"FATAL: batch insert failed {e.code}: {e.read().decode()[:300]}", file=sys.stderr)
        return 1
    batch_id = rep[0]["batch_id"]
    print(f"created batch_id={batch_id} ({st})")

    def num(r, k):
        v = r.get(k, "")
        return float(v) if v not in ("", None) else None

    entries = []
    for i, r in enumerate(top, start=1):
        entries.append({
            "batch_id": batch_id, "rank": i, "wallet_hex": r["wallet"],
            "ls_edge": num(r, "mean_net_ls"), "ls_tstat": num(r, "tstat_net_ls"),
            "fill_rate": num(r, "fill_rate"),
            "n_trades": int(float(r["n_filled"])) if r.get("n_filled") else None,
            "hit_rate": num(r, "hit_rate"), "avg_price": num(r, "avg_price"),
        })
    # PostgREST accepts a JSON array for bulk insert; chunk to stay under limits.
    CHUNK = 500
    for j in range(0, len(entries), CHUNK):
        try:
            _req("POST", f"{url}/rest/v1/ranking_entries", key, body=entries[j:j + CHUNK],
                 prefer="return=minimal")
        except urllib.error.HTTPError as e:
            print(f"FATAL: entries insert failed {e.code}: {e.read().decode()[:300]}", file=sys.stderr)
            return 1
    print(f"inserted {len(entries)} entries into batch {batch_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
