#!/usr/bin/env python3
"""
Backfill market_schedules.end_date_unix for resolved markets that have no schedule row,
using Gamma's `/markets?condition_ids=...&closed=true` (the only endpoint that returns
`endDate` for closed markets — gamma.rs:142-151). Repeated `condition_ids=` params batch
many markets per request; comma-separated does NOT work. Browser UA required (else 403).

This removes the look-ahead-leakage source in the 72hr analysis: with a real (known-at-entry)
end_date, the TTR filter no longer falls back to on-chain resolved_at (settlement time).

Idempotent: targets only resolved markets with NO schedule row; not-found markets get a NULL
row (source='gamma') so re-runs skip them. INSERT OR IGNORE never overwrites existing data.
"""
from __future__ import annotations
import sqlite3, urllib.request, urllib.error, json, time, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

DB = "data/wallet_cache.db"
BASE = "https://gamma-api.polymarket.com/markets"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) pe-bootstrap-endDate-backfill"}
BATCH = 50            # condition_ids per request
WORKERS = 10
FLUSH_EVERY = 8000    # markets between DB commits


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def parse_end(s):
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def fetch_batch(ids, attempt=0):
    url = BASE + "?" + "&".join(f"condition_ids={i}" for i in ids) + "&closed=true&limit=500"
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=45) as r:
            data = json.load(r)
        out = {}
        for m in data:
            cid = m.get("conditionId")
            if cid:
                out[cid] = parse_end(m.get("endDate"))
        return out  # may omit ids Gamma doesn't know
    except urllib.error.HTTPError as e:
        if e.code in (429, 500, 502, 503, 504) and attempt < 5:
            time.sleep(2 ** attempt)
            return fetch_batch(ids, attempt + 1)
        return {"__error__": f"HTTP {e.code}"}
    except Exception as e:
        if attempt < 3:
            time.sleep(1 + attempt)
            return fetch_batch(ids, attempt + 1)
        return {"__error__": str(e)[:80]}


def main():
    conn = sqlite3.connect(DB, timeout=60)
    if int(conn.execute("PRAGMA user_version").fetchone()[0]) == -2:
        conn.close()
        raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    conn.execute("PRAGMA busy_timeout=60000;")
    log("enumerating resolved markets with no schedule row ...")
    targets = [m for (m,) in conn.execute(
        "SELECT r.market_id FROM market_resolutions r "
        "LEFT JOIN market_schedules s USING(market_id) "
        "WHERE r.winning_outcome_id IS NOT NULL AND s.market_id IS NULL")]
    total = len(targets)
    log(f"targets: {total:,} markets")
    if not total:
        log("nothing to do"); return 0

    batches = [targets[i:i + BATCH] for i in range(0, total, BATCH)]
    log(f"{len(batches):,} batches of {BATCH}, {WORKERS} workers")
    now = int(time.time())
    buf = []          # (market_id, end_date_unix_or_None)
    done = found = errors = 0
    t0 = time.time()

    def flush():
        nonlocal buf
        if not buf:
            return
        conn.executemany(
            "INSERT OR IGNORE INTO market_schedules (market_id, end_date_unix, fetched_at_unix, source) "
            "VALUES (?, ?, ?, 'gamma')",
            [(mid, ed, now) for mid, ed in buf])
        conn.commit()
        buf = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_batch, b): b for b in batches}
        for fut in as_completed(futs):
            b = futs[fut]
            res = fut.result()
            if "__error__" in res:
                errors += 1
            for mid in b:                       # every requested id gets a row (NULL if not returned)
                ed = res.get(mid)
                buf.append((mid, ed))
                if ed is not None:
                    found += 1
            done += len(b)
            if len(buf) >= FLUSH_EVERY:
                flush()
            if done % 50000 < BATCH:
                rate = done / max(1e-6, time.time() - t0)
                log(f"  {done:,}/{total:,} ({100*done/total:.0f}%)  found_end={found:,}  "
                    f"batch_errors={errors}  {rate:.0f} mkt/s")
    flush()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.commit()
    log(f"DONE: processed {done:,}, end_date found {found:,} ({100*found/max(1,done):.1f}%), "
        f"batch_errors {errors}, {time.time()-t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
