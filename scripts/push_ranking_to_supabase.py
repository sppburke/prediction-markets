#!/usr/bin/env python3
"""Publish one latency-shifted wallet ranking to Supabase.

The exact request is durably recorded before the first network write. The
``publish_ranking_batch`` RPC owns batch creation/reuse and entry insertion in
one database transaction, so transient retries cannot expose a partial ranking
or create duplicate epochs. The VPS reads ``latest_ranking`` and applies the
absolute-loss demotion test.

REST-only (PostgREST) via ``SUPABASE_SECRET_KEY``. After successful publication,
old batch history is pruned to ``--keep-batches`` (0 disables pruning).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


PUBLISH_REQUEST_VERSION = 1
SUPABASE_MAX_RETRIES = 5
SUPABASE_RETRY_BASE_SECS = 1
SUPABASE_RETRY_MAX_SECS = 30
TEMPFAIL_EXIT = 75


class CacheStaleError(Exception):
    """The trade, resolution-content, or completed-CLOB-sweep freshness contract
    (docs/26) was not honoured, so publication must fail closed."""


class SupabaseRequestError(Exception):
    """One classified Supabase HTTP/transport failure."""

    def __init__(
        self,
        detail: str,
        *,
        retryable: bool,
        status: int | None = None,
        retry_after_secs: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.retryable = retryable
        self.status = status
        self.retry_after_secs = retry_after_secs


class TransientRetriesExhausted(Exception):
    """A retryable Supabase request exhausted its bounded retry budget."""


def _newest_trade_unix(con: sqlite3.Connection) -> int | None:
    """Global ``MAX(timestamp_unix)`` over the whole trade cache (the freshness probe).
    Scans the covering index ``idx_trades_wallet_ts`` (~1 min on the production cache;
    no index leads with ``timestamp_unix``, so a full index scan is unavoidable)."""
    row = con.execute("SELECT MAX(timestamp_unix) FROM trades").fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _newest_resolution_fetch(con: sqlite3.Connection) -> int | None:
    """Global resolution-content heartbeat from ``market_resolutions``."""
    row = con.execute("SELECT MAX(fetched_at_unix) FROM market_resolutions").fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _clob_sweep_completed_at(con: sqlite3.Connection) -> tuple[str, int] | None:
    """Return the CLOB sweep cursor value and its last update time, if present."""
    row = con.execute(
        "SELECT value, updated_at FROM source_cursor WHERE key = 'clob_closed'"
    ).fetchone()
    if row is None:
        return None
    return str(row[0]), int(row[1])


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

    Aborts with :class:`CacheStaleError` unless the newest trade, newest resolution
    fetch, and successful CLOB completion marker are all within
    ``max_staleness_hours``. The cursor probe accepts only a present row whose
    value is exactly ``''``; a non-empty value proves an interrupted walk. The
    ``wallet_hex`` comparison is case-insensitive (both sides lowercased).
    Returns ``(kept_rows, dropped_count, last_trade_map)`` where
    ``last_trade_map`` is lowercased ``wallet_hex`` -> last ``timestamp_unix``
    for every queried wallet that has a cached trade (reused to stamp each
    pushed entry with its real last trade, #357; a wallet with no cached trade
    is absent -> its entry's last_trade_unix is NULL).
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
        newest_resolution = _newest_resolution_fetch(con)
        if (
            newest_resolution is None
            or now - newest_resolution > max_staleness_hours * 3600
        ):
            age = (
                "unknown"
                if newest_resolution is None
                else f"{(now - newest_resolution) / 3600:.1f}"
            )
            raise CacheStaleError(
                f"cache {db_path!r} newest resolution fetch is {age}h old "
                f"(> {max_staleness_hours}h bound) — refresh resolutions before pushing "
                "(docs/26)"
            )
        sweep = _clob_sweep_completed_at(con)
        if sweep is None:
            raise CacheStaleError(
                f"cache {db_path!r} has no completed CLOB sweep marker — refresh "
                "resolutions before pushing (docs/26)"
            )
        cursor_value, completed_at = sweep
        if cursor_value != "":
            raise CacheStaleError(
                f"cache {db_path!r} CLOB sweep is incomplete (cursor={cursor_value!r}) — "
                "refresh resolutions before pushing (docs/26)"
            )
        if now - completed_at > max_staleness_hours * 3600:
            age = f"{(now - completed_at) / 3600:.1f}"
            raise CacheStaleError(
                f"cache {db_path!r} completed CLOB sweep is {age}h old "
                f"(> {max_staleness_hours}h bound) — refresh resolutions before pushing "
                "(docs/26)"
            )
        wallets_lower = sorted({r["wallet"].lower() for r in rows})
        last = _wallet_last_trade(con, wallets_lower)
    finally:
        con.close()
    cutoff = now - active_window_hours * 3600
    kept = [r for r in rows if last.get(r["wallet"].lower(), -1) >= cutoff]
    return kept, len(rows) - len(kept), last


def _retry_after_seconds(error: urllib.error.HTTPError) -> int | None:
    if error.headers is None:
        return None
    raw = error.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        parsed = int(raw)
    except ValueError:
        return None
    return max(0, parsed)


def _request_once(method: str, url: str, key: str, body=None, prefer: str | None = None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:300]
        retryable = error.code in (408, 425, 429) or 500 <= error.code <= 599
        raise SupabaseRequestError(
            f"HTTP {error.code}: {detail}",
            retryable=retryable,
            status=error.code,
            retry_after_secs=_retry_after_seconds(error),
        ) from error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise SupabaseRequestError(
            f"{type(error).__name__}: {error}",
            retryable=True,
        ) from error


def _req(
    method: str,
    url: str,
    key: str,
    body=None,
    prefer: str | None = None,
    *,
    max_retries: int = SUPABASE_MAX_RETRIES,
    sleep=time.sleep,
):
    """Run one idempotent request with bounded transient-only retry."""
    for retry in range(max_retries + 1):
        try:
            return _request_once(method, url, key, body=body, prefer=prefer)
        except SupabaseRequestError as error:
            if not error.retryable:
                raise
            if retry == max_retries:
                raise TransientRetriesExhausted(str(error)) from error
            exponential = min(
                SUPABASE_RETRY_BASE_SECS * (2**retry),
                SUPABASE_RETRY_MAX_SECS,
            )
            delay = (
                max(1, min(error.retry_after_secs, SUPABASE_RETRY_MAX_SECS))
                if error.retry_after_secs is not None
                else exponential
            )
            print(
                f"WARNING: transient Supabase {method} failure; "
                f"retry {retry + 1}/{max_retries} in {delay}s: {error}",
                file=sys.stderr,
            )
            sleep(delay)


def _num(r, k):
    """Parse a CSV cell to float, mapping blank/missing to ``None`` (SQL NULL)."""
    v = r.get(k, "")
    return float(v) if v not in ("", None) else None


def _bool(r, k):
    """Parse a CSV cell to bool, mapping blank/missing to ``None`` (SQL NULL).

    The ranker stores a Python bool (`latency_shift_rerank.py`), which ``csv.DictWriter``
    renders as ``True``/``False`` — normalise case before matching. Anything else raises:
    an unparseable verdict must never degrade silently to ``False``, which would drop a
    genuine survivor out of the live watchlist."""
    v = r.get(k, "")
    if v in ("", None):
        return None
    normalized = str(v).strip().lower()
    if normalized in ("true", "false"):
        return normalized == "true"
    raise ValueError(f"{k}: expected 'true' or 'false', got {v!r}")


def build_entries(top, last_trade_map):
    """Build the `ranking_entries` rows for one batch.

    Each entry carries the wallet's real last-trade timestamp (#357) from ``last_trade_map``
    (lowercased ``wallet_hex`` -> ``timestamp_unix``); ``None`` when no cache was supplied or
    the wallet had no cached trade. The VPS seeds each admitted wallet's poll cursor — the
    inactivity clock — from this value, so it must be keyed by the lowercased wallet."""
    entries = []
    for i, r in enumerate(top, start=1):
        entries.append({
            "rank": i, "wallet_hex": r["wallet"],
            "ls_edge": _num(r, "mean_net_ls"), "ls_tstat": _num(r, "tstat_net_ls"),
            "fill_rate": _num(r, "fill_rate"),
            "n_trades": int(float(r["n_filled"])) if r.get("n_filled") else None,
            "hit_rate": _num(r, "hit_rate"), "avg_price": _num(r, "avg_price"),
            "last_trade_unix": last_trade_map.get(r["wallet"].lower()),
            # The ranker's eligibility verdict (#518): pe-service admits only `true` rows.
            "survives": _bool(r, "survives"),
        })
    return entries


def publication_key(batch: dict, entries: list[dict]) -> str:
    """Content-address one immutable ranking publication."""
    canonical = json.dumps(
        {"batch": batch, "entries": entries},
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def build_publish_request(batch: dict, entries: list[dict], keep_batches: int) -> dict:
    request = {
        "version": PUBLISH_REQUEST_VERSION,
        "batch": batch,
        "entries": entries,
        "keep_batches": keep_batches,
    }
    request["publish_key"] = publication_key(batch, entries)
    validate_publish_request(request)
    return request


def validate_publish_request(request: dict) -> None:
    """Reject incomplete or modified durable retry state before any network write."""
    if request.get("version") != PUBLISH_REQUEST_VERSION:
        raise ValueError(
            f"unsupported publish request version: {request.get('version')!r}"
        )
    batch = request.get("batch")
    entries = request.get("entries")
    keep_batches = request.get("keep_batches")
    if not isinstance(batch, dict):
        raise ValueError("publish request batch must be an object")
    if not isinstance(entries, list) or not entries:
        raise ValueError("publish request entries must be a non-empty array")
    if not isinstance(keep_batches, int) or keep_batches < 0:
        raise ValueError("publish request keep_batches must be a non-negative integer")
    expected_ranks = list(range(1, len(entries) + 1))
    actual_ranks = [entry.get("rank") for entry in entries if isinstance(entry, dict)]
    if len(actual_ranks) != len(entries) or actual_ranks != expected_ranks:
        raise ValueError("publish request entry ranks must be contiguous from 1")
    if any(not entry.get("wallet_hex") for entry in entries):
        raise ValueError("publish request entries require wallet_hex")
    expected_key = publication_key(batch, entries)
    if request.get("publish_key") != expected_key:
        raise ValueError("publish request content hash mismatch")


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temp_name = temporary.name
            os.chmod(temp_name, 0o600)
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass


def save_publish_request(path: str, request: dict) -> None:
    validate_publish_request(request)
    rendered = json.dumps(
        request,
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    _atomic_write_text(Path(path), rendered + "\n")


def load_publish_request(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        request = json.load(handle)
    if not isinstance(request, dict):
        raise ValueError("publish request root must be an object")
    validate_publish_request(request)
    return request


def save_pending_pointer(path: str, request_path: str) -> None:
    request = Path(request_path)
    if not request.is_file():
        raise ValueError(f"publish request is not a file: {request_path}")
    try:
        rendered = str(request.resolve().relative_to(Path.cwd().resolve()))
    except ValueError as error:
        raise ValueError("publish request must be inside the repository") from error
    _atomic_write_text(Path(path), rendered + "\n")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ranked-csv", help="pass-2 latency_shift_ranked.csv")
    ap.add_argument(
        "--request-file",
        help="atomically persist the exact publication request before network I/O",
    )
    ap.add_argument(
        "--pending-file",
        help="atomically point the production supervisor at --request-file",
    )
    ap.add_argument(
        "--resume-request",
        help="replay an existing validated publication request without rebuilding it",
    )
    ap.add_argument(
        "--prepare-only",
        action="store_true",
        help="persist request/pending state without contacting Supabase",
    )
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


def prepare_publish_request(a: argparse.Namespace, process_now: int) -> dict:
    if not a.ranked_csv:
        raise ValueError("--ranked-csv is required unless --resume-request is used")

    # Read pass-2 ranking; keep survivors first, then by latency-shifted t-stat desc.
    with open(a.ranked_csv, newline="", encoding="utf-8") as ranked_file:
        rows = list(csv.DictReader(ranked_file))
    universe_count = len(rows)  # full ranked universe, recorded before the active filter

    # Active-only upload filter (issue #350 WS3): when a cache is provided, drop ranked
    # wallets with no trade in the last --active-window-hours and abort outright if the
    # cache itself is stale (a stale cache would otherwise filter out *everyone*). Runs
    # before the Supabase write so a stale cache never creates an orphaned batch. Filtering
    # before the top-N cut fills the uploaded bench with active wallets rather than padding
    # it with idle ones.
    last_trade_map: dict[str, int] = {}
    if a.db:
        rows, dropped, last_trade_map = filter_active_rows(
            rows, a.db, a.active_window_hours, a.max_cache_staleness_hours, process_now
        )
        print(f"active-filter: dropped {dropped} wallet(s) idle > {a.active_window_hours}h "
              f"per {a.db}; {len(rows)} remain")

    # #518: the ranker's verdict is the single quality authority for live admission, so a
    # batch that cannot state it must never be published. Checked BEFORE the sort — a sort key
    # mixing None with bools raises at comparison time — and before any durable/network write.
    unverdicted = [r.get("wallet", "?") for r in rows if _bool(r, "survives") is None]
    if unverdicted:
        raise ValueError(
            f"ranked CSV has no `survives` verdict for {len(unverdicted)} row(s) "
            f"(first: {unverdicted[0]}); refusing to publish a batch that cannot state "
            f"eligibility"
        )

    def key_fn(row):
        tstat = row.get("tstat_net_ls", "")
        return (
            _bool(row, "survives"),
            float(tstat) if tstat not in ("", None) else -9.0,
        )

    rows.sort(key=key_fn, reverse=True)
    top = rows[: a.top_n]
    if not top:
        raise ValueError("no rows to push (empty CSV, or the active filter removed all)")

    batch = {
        "git_sha": a.git_sha or None,
        "band_lo": a.band_lo,
        "band_hi": a.band_hi,
        "ttr_floor_secs": a.ttr_floor_secs,
        "ttr_max_secs": a.ttr_max_secs,
        "latency_shift_secs": a.latency_shift_secs,
        "universe_size": a.universe_size or universe_count,
        "notes": a.notes or None,
    }
    entries = build_entries(top, last_trade_map)
    return build_publish_request(batch, entries, a.keep_batches)


def _batch_id_from_rpc_response(response) -> int:
    if isinstance(response, int) and not isinstance(response, bool):
        return response
    if isinstance(response, str) and response.isdigit():
        return int(response)
    raise ValueError(f"publish_ranking_batch returned invalid batch id: {response!r}")


def publish_request_to_supabase(request: dict, url: str, key: str) -> int:
    validate_publish_request(request)
    _, response = _req(
        "POST",
        f"{url}/rest/v1/rpc/publish_ranking_batch",
        key,
        body={
            "p_publish_key": request["publish_key"],
            "p_batch": request["batch"],
            "p_entries": request["entries"],
        },
    )
    batch_id = _batch_id_from_rpc_response(response)
    entries = request["entries"]
    print(
        f"published batch_id={batch_id} entries={len(entries)} "
        f"publish_key={request['publish_key'][:16]}"
    )

    # Verify the exact committed epoch, not merely that an older ranking exists.
    _, latest = _req(
        "GET",
        f"{url}/rest/v1/latest_ranking"
        f"?select=batch_id,rank,survives&order=rank.asc&limit={len(entries)}",
        key,
    )
    expected_ranks = list(range(1, len(entries) + 1))
    if (
        not isinstance(latest, list)
        or [row.get("rank") for row in latest if isinstance(row, dict)] != expected_ranks
        or any(row.get("batch_id") != batch_id for row in latest if isinstance(row, dict))
    ):
        raise ValueError(
            f"latest_ranking did not expose exact batch {batch_id} "
            f"with ranks 1..{len(entries)}"
        )
    # The rank/batch assertion above already proves an exact, ordered 1..N response, so each
    # row lines up with its submitted entry by position. Confirm the verdict round-tripped
    # (#518): a legacy replayed request submitted no verdict, so `None` vs stored SQL NULL
    # matches and passes; a fresh batch must store exactly what it sent.
    mismatched = [
        row.get("rank")
        for row, entry in zip(latest, entries)
        if row.get("survives") != entry.get("survives")
    ]
    if mismatched:
        raise ValueError(
            f"batch {batch_id} stored the wrong `survives` verdict for "
            f"{len(mismatched)} rank(s) (first: {mismatched[0]})"
        )
    print(f"latest_ranking rows: {len(latest)} (batch_id={batch_id})")

    # Bound the append-only ranking_batches history (#411). Best-effort — the atomic
    # publication and exact verification already succeeded, so cleanup self-heals later.
    keep_batches = request["keep_batches"]
    try:
        cutoff = prune_old_batches(url, key, keep_batches)
        if cutoff is not None:
            print(
                f"pruned ranking_batches with batch_id <= {cutoff} "
                f"(kept newest {keep_batches})"
            )
    except Exception as error:  # noqa: BLE001 — post-publish cleanup is explicitly best-effort
        print(
            f"WARNING: ranking_batches prune failed "
            f"({type(error).__name__}: {error}); history not trimmed this run — "
            "bounded and self-heals on the next successful push",
            file=sys.stderr,
        )
    return batch_id


def main() -> int:
    a = build_parser().parse_args()
    process_now = int(time.time())  # single time anchor: active filter + last-trade stamps

    try:
        if a.resume_request:
            if a.ranked_csv or a.request_file or a.pending_file or a.prepare_only:
                raise ValueError(
                    "--resume-request cannot be combined with ranking preparation arguments"
                )
            request = load_publish_request(a.resume_request)
        else:
            if a.pending_file and not a.request_file:
                raise ValueError("--pending-file requires --request-file")
            if a.prepare_only and not a.request_file:
                raise ValueError("--prepare-only requires --request-file")
            request = prepare_publish_request(a, process_now)
            if a.request_file:
                save_publish_request(a.request_file, request)
                print(
                    f"saved publish request: {a.request_file} "
                    f"(publish_key={request['publish_key'][:16]})"
                )
            if a.pending_file:
                save_pending_pointer(a.pending_file, a.request_file)
                print(f"saved pending pointer: {a.pending_file}")
            if a.prepare_only:
                print("publish request prepared; network publication skipped")
                return 0
    except (CacheStaleError, json.JSONDecodeError, OSError, sqlite3.Error, ValueError) as error:
        print(f"FATAL: could not prepare publication: {error}", file=sys.stderr)
        return 1

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SECRET_KEY")
    if not url or not key:
        print("FATAL: set SUPABASE_URL and SUPABASE_SECRET_KEY in env (.env)", file=sys.stderr)
        return 1

    try:
        publish_request_to_supabase(request, url.rstrip("/"), key)
    except TransientRetriesExhausted as error:
        print(
            f"TEMPFAIL: transient Supabase retries exhausted: {error}",
            file=sys.stderr,
        )
        return TEMPFAIL_EXIT
    except (SupabaseRequestError, ValueError) as error:
        print(f"FATAL: Supabase publication failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
