#!/usr/bin/env python3
"""Push a latency-shifted wallet ranking (pass-2 output) to Supabase as one append-only
EPOCH batch: insert a `ranking_batches` row (config + provenance), then the top-N
`ranking_entries`. The VPS reads `latest_ranking` and runs the absolute-loss demotion
test (project_copytrade_knockout_policy).

REST-only (PostgREST) via the SUPABASE_SECRET_KEY in env — no DB driver, no committed
secret. Idempotent batches are NOT enforced (append-only by design: every run = a new
epoch). After a successful push the script prunes `ranking_batches` to the newest
`--keep-batches` (default 180 ≈ 6 months; CASCADE drops their entries), bounding the
append-only growth while retaining enough epochs for the wholesale-swap-at-frequency-X
replay (#411). `--keep-batches 0` disables the prune.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request


class CacheStaleError(Exception):
    """The trade cache's newest trade is older than the staleness bound, i.e. the
    backfill-before-push contract (docs/26) was not honoured. Pushing anyway would
    filter out *every* wallet (none look "active"), so we abort instead."""


def _newest_trade_unix(con: sqlite3.Connection) -> int | None:
    """Global ``MAX(timestamp_unix)`` over the whole trade cache (the freshness probe).
    Scans the covering index ``idx_trades_wallet_ts`` (~1 min on the production cache;
    no index leads with ``timestamp_unix``, so a full index scan is unavoidable)."""
    row = con.execute("SELECT MAX(timestamp_unix) FROM trades").fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _wallet_last_trade(con: sqlite3.Connection, wallets_lower: list[str]) -> dict[str, int]:
    """Map lowercased ``wallet_hex`` -> its most recent ``timestamp_unix`` in the cache.

    ``wallet_hex`` is stored lowercase (verified 2026-06-16), so the IN-list is matched
    against the raw column and served by the covering index ``idx_trades_wallet_ts``.
    Wrapping ``lower(wallet_hex)`` would defeat that index and force a full-table scan of
    ~269M rows. Chunked to stay under SQLite's bound-parameter limit.
    """
    out: dict[str, int] = {}
    chunk_size = 500
    for i in range(0, len(wallets_lower), chunk_size):
        chunk = wallets_lower[i : i + chunk_size]
        placeholders = ",".join("?" * len(chunk))
        q = (
            f"SELECT wallet_hex, MAX(timestamp_unix) FROM trades "
            f"WHERE wallet_hex IN ({placeholders}) GROUP BY wallet_hex"
        )
        for hexv, ts in con.execute(q, chunk):
            if ts is not None:
                out[hexv.lower()] = int(ts)
    return out


def filter_active_rows(rows, db_path, active_window_hours, max_staleness_hours, now):
    """Drop ranked rows whose wallet has no cached trade within ``active_window_hours``.

    Aborts with :class:`CacheStaleError` when the cache's global newest trade is older
    than ``max_staleness_hours`` — a stale cache would spuriously drop every wallet, so we
    refuse to push rather than silently empty the ranking. The ``wallet_hex`` comparison is
    case-insensitive (both sides lowercased). Returns ``(kept_rows, dropped_count, last_trade_map)``
    where ``last_trade_map`` is lowercased ``wallet_hex`` -> last ``timestamp_unix`` for every
    queried wallet that has a cached trade (reused to stamp each pushed entry with its real last
    trade, #357; a wallet with no cached trade is absent -> its entry's last_trade_unix is NULL).
    """
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        newest = _newest_trade_unix(con)
        if newest is None or now - newest > max_staleness_hours * 3600:
            age = "unknown" if newest is None else f"{(now - newest) / 3600:.1f}"
            raise CacheStaleError(
                f"cache {db_path!r} newest trade is {age}h old "
                f"(> {max_staleness_hours}h bound) — backfill before pushing (docs/26)"
            )
        wallets_lower = sorted({r["wallet"].lower() for r in rows})
        last = _wallet_last_trade(con, wallets_lower)
    finally:
        con.close()
    cutoff = now - active_window_hours * 3600
    kept = [r for r in rows if last.get(r["wallet"].lower(), -1) >= cutoff]
    return kept, len(rows) - len(kept), last


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


def _num(r, k):
    """Parse a CSV cell to float, mapping blank/missing to ``None`` (SQL NULL)."""
    v = r.get(k, "")
    return float(v) if v not in ("", None) else None


def build_entries(top, batch_id, last_trade_map):
    """Build the `ranking_entries` rows for one batch.

    Each entry carries the wallet's real last-trade timestamp (#357) from ``last_trade_map``
    (lowercased ``wallet_hex`` -> ``timestamp_unix``); ``None`` when no cache was supplied or
    the wallet had no cached trade. The VPS seeds each admitted wallet's poll cursor — the
    inactivity clock — from this value, so it must be keyed by the lowercased wallet."""
    entries = []
    for i, r in enumerate(top, start=1):
        entries.append({
            "batch_id": batch_id, "rank": i, "wallet_hex": r["wallet"],
            "ls_edge": _num(r, "mean_net_ls"), "ls_tstat": _num(r, "tstat_net_ls"),
            "fill_rate": _num(r, "fill_rate"),
            "n_trades": int(float(r["n_filled"])) if r.get("n_filled") else None,
            "hit_rate": _num(r, "hit_rate"), "avg_price": _num(r, "avg_price"),
            "last_trade_unix": last_trade_map.get(r["wallet"].lower()),
        })
    return entries


def build_parser() -> argparse.ArgumentParser:
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
    # Batch retention (#411): after a successful push, prune ranking_batches to the newest
    # N (CASCADE drops their entries), bounding the append-only history. 0 disables it.
    ap.add_argument("--keep-batches", type=int, default=1080,
                    help="keep only the newest N ranking_batches after a successful push "
                         "(0 disables; docs/_GLOSSARY ranking_batches_retention; default 1080 "
                         "= ~6 months at the 4h production cadence, 6 pushes/day)")
    # Active-only upload filter (issue #350 WS3). Off unless --db is given.
    ap.add_argument("--db", default=None,
                    help="wallet_cache.db; when set, drop ranked wallets idle beyond "
                         "--active-window-hours and abort if the cache itself is stale")
    ap.add_argument("--active-window-hours", type=int, default=72,
                    help="drop ranked wallets with no cached trade in the last N hours "
                         "(docs/_GLOSSARY upload_active_window_hours; default 72)")
    ap.add_argument("--max-cache-staleness-hours", type=int, default=24,
                    help="abort the push if the cache's newest trade is older than N hours "
                         "(docs/_GLOSSARY upload_max_cache_staleness_hours; default 24)")
    return ap


def prune_old_batches(url: str, key: str, keep: int) -> int | None:
    """Prune `ranking_batches` to the newest `keep` rows (CASCADE drops their entries),
    bounding the append-only history (#411). Returns the deleted cutoff `batch_id`, or
    ``None`` when nothing was pruned (``keep <= 0`` disables it, or ≤ keep batches exist).

    Count-based, not id-range: `batch_id` is `bigserial` with possible gaps from orphan-batch
    deletes (a failed entries-insert deletes its just-created batch), so the cutoff is the
    (keep+1)-th newest id — ordering desc and skipping the
    newest `keep` means the live `max(batch_id)` (what `latest_ranking` reads) is never in the
    delete range. PostgREST cannot express ``NOT IN (SELECT … LIMIT N)``, hence GET-cutoff +
    DELETE-below-it. Raises on transport/HTTP error; the caller treats prune as best-effort
    (the push already succeeded)."""
    if keep <= 0:
        return None  # disabled — history-preserving research re-push
    _, rows = _req(
        "GET",
        f"{url}/rest/v1/ranking_batches?select=batch_id&order=batch_id.desc&offset={keep}&limit=1",
        key,
    )
    if not rows:  # PostgREST returns [] (not null) when the offset is past the end
        return None  # ≤ keep batches exist; nothing to prune
    cutoff = rows[0]["batch_id"]
    _req("DELETE", f"{url}/rest/v1/ranking_batches?batch_id=lte.{cutoff}", key,
         prefer="return=minimal")
    return cutoff


def main() -> int:
    a = build_parser().parse_args()
    process_now = int(time.time())  # single time anchor: active filter + last-trade stamps

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SECRET_KEY")
    if not url or not key:
        print("FATAL: set SUPABASE_URL and SUPABASE_SECRET_KEY in env (.env)", file=sys.stderr)
        return 1
    url = url.rstrip("/")

    # Read pass-2 ranking; keep survivors first, then by latency-shifted t-stat desc.
    rows = list(csv.DictReader(open(a.ranked_csv, newline="")))
    universe_count = len(rows)  # full ranked universe, recorded before the active filter

    # Active-only upload filter (issue #350 WS3): when a cache is provided, drop ranked
    # wallets with no trade in the last --active-window-hours and abort outright if the
    # cache itself is stale (a stale cache would otherwise filter out *everyone*). Runs
    # before the Supabase write so a stale cache never creates an orphaned batch. Filtering
    # before the top-N cut fills the uploaded bench with active wallets rather than padding
    # it with idle ones.
    last_trade_map: dict[str, int] = {}
    if a.db:
        try:
            rows, dropped, last_trade_map = filter_active_rows(
                rows, a.db, a.active_window_hours, a.max_cache_staleness_hours, process_now
            )
        except CacheStaleError as e:
            print(f"FATAL: {e}", file=sys.stderr)
            return 1
        except (sqlite3.Error, OSError) as e:
            print(f"FATAL: could not read cache {a.db!r}: {e}", file=sys.stderr)
            return 1
        print(f"active-filter: dropped {dropped} wallet(s) idle > {a.active_window_hours}h "
              f"per {a.db}; {len(rows)} remain")

    def key_fn(r):
        t = r.get("tstat_net_ls", "")
        return (r.get("survives", "").lower() == "true", float(t) if t not in ("", None) else -9.0)
    rows.sort(key=key_fn, reverse=True)
    top = rows[: a.top_n]
    if not top:
        print("FATAL: no rows to push (empty CSV, or the active filter removed all)",
              file=sys.stderr)
        return 1

    batch = {
        "git_sha": a.git_sha or None,
        "band_lo": a.band_lo, "band_hi": a.band_hi,
        "ttr_floor_secs": a.ttr_floor_secs, "ttr_max_secs": a.ttr_max_secs,
        "latency_shift_secs": a.latency_shift_secs,
        "universe_size": a.universe_size or universe_count,
        "notes": a.notes or None,
    }
    try:
        st, rep = _req("POST", f"{url}/rest/v1/ranking_batches", key, body=batch,
                       prefer="return=representation")
    except (urllib.error.URLError, OSError) as e:
        detail = e.read().decode()[:300] if isinstance(e, urllib.error.HTTPError) else str(e)
        print(f"FATAL: batch insert failed: {detail}", file=sys.stderr)
        return 1
    batch_id = rep[0]["batch_id"]
    print(f"created batch_id={batch_id} ({st})")

    entries = build_entries(top, batch_id, last_trade_map)
    # PostgREST accepts a JSON array for bulk insert; chunk to stay under limits.
    CHUNK = 500
    try:
        for j in range(0, len(entries), CHUNK):
            _req("POST", f"{url}/rest/v1/ranking_entries", key, body=entries[j:j + CHUNK],
                 prefer="return=minimal")
    except (urllib.error.URLError, OSError) as e:
        # HTTPError carries a body; URLError/OSError (incl. ConnectionReset mid-read) don't.
        detail = e.read().decode()[:300] if isinstance(e, urllib.error.HTTPError) else str(e)
        print(f"FATAL: entries insert failed: {detail}; deleting orphaned batch {batch_id}",
              file=sys.stderr)
        try:
            _req("DELETE", f"{url}/rest/v1/ranking_batches?batch_id=eq.{batch_id}", key,
                 prefer="return=minimal")
        except (urllib.error.URLError, OSError):
            print(f"WARNING: could not delete orphaned batch {batch_id} — clean up manually",
                  file=sys.stderr)
        return 1
    print(f"inserted {len(entries)} entries into batch {batch_id}")

    # Bound the append-only ranking_batches history (#411): keep the newest N, CASCADE drops
    # their entries. Best-effort — the push already succeeded, so ANY prune failure (transport,
    # PostgREST error, or a malformed response) only warns (with HTTP status + body when
    # available) and never fails the run; growth stays bounded and self-heals on the next push.
    try:
        cutoff = prune_old_batches(url, key, a.keep_batches)
        if cutoff is not None:
            print(f"pruned ranking_batches with batch_id <= {cutoff} (kept newest {a.keep_batches})")
    except Exception as e:  # noqa: BLE001 — best-effort cleanup must never fail a succeeded push
        if isinstance(e, urllib.error.HTTPError):
            detail = f"HTTP {e.code}: {e.read().decode()[:300]}"
        else:
            detail = f"{type(e).__name__}: {e}"
        print(f"WARNING: ranking_batches prune failed ({detail}); history not trimmed this "
              f"run — bounded and self-heals on the next successful push", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
