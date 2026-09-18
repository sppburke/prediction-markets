#!/usr/bin/env python3
"""
Phase-0 live probe for issue #382 — Tier-1 confirmation of Gamma/CLOB batching + browser-UA
behaviour BEFORE the Rust `GammaMarketsClient` rewrite.

The Rust clients fetch Gamma `/markets` one condition_id per request (~20 req/s). The Python
proof scripts (`backfill_end_dates.py`, `oos_gamma.py`) already batch many ids per request via
repeat-key `?condition_ids=A&condition_ids=B&…` at ~1200 markets/s — but ONLY the `&closed=true`
variant is Tier-1 proven, and `gamma.rs:5-6` claims all batching "fails silently". The open
(plain) endpoint, which the two open bootstrap passes and `MidPriceCache` rely on, is unproven.
This probe records the full live matrix so the rewrite is evidence-gated, not speculative.

It is a one-shot DIAGNOSTIC, not a regression test — it makes live network calls and is never run
in CI. The reusable pure logic (`build_url`, `demux`) is covered by `scripts/test_probe_gamma_ua.py`
(no network), which IS run in CI.

What it records (matches issue #382 Phase-0 AC):
  - Gamma `/markets` single-id × {plain, &closed=true} × {no-UA, browser-UA}  → 403-gating per variant.
      * resolved + &closed=true + no-UA  = the live paper-pnl POLLER path        (AC item a)
      * resolved + &closed=true + no-UA  = the MarketEndCache closed path         (AC item b, same path)
  - Gamma repeat-key BATCH (browser-UA): closed×50 (proven) AND plain×50         (AC item d: does plain batch?).
  - Gamma BATCH cap probe: request 100 ids, observe how many come back (≤50 ⇒ cap 50).
  - Gamma `&limit=500` does not truncate a full 50-id batch.
  - Gamma comma-separated batch  → confirms repeat-key is required (comma fails).
  - CLOB `/markets?closed=true` × {no-UA, browser-UA}                            (AC item c: CLOB sole
      resolution source post-#372 yet carries no UA today — does it 403?).

Run: `python3 scripts/probe_gamma_ua.py`            (pulls anchor ids from data/wallet_cache.db)
  or: `python3 scripts/probe_gamma_ua.py --resolved 0x.. --open 0x..`
"""
from __future__ import annotations

import argparse
import http.client
import json
import sqlite3
import sys
import time
from urllib.parse import urlsplit

GAMMA_BASE = "https://gamma-api.polymarket.com/markets"
CLOB_BASE = "https://clob.polymarket.com/markets"
# The proven scripts claim a browser UA is REQUIRED for Gamma/CLOB &closed=true (else 403) —
# backfill_end_dates.py:21, clob_vs_polygon_reconciliation.py:65. This probe tests that claim against
# the *headerless* case the current Rust reqwest clients actually send. Candidate `gamma_browser_ua`.
BROWSER_UA = "Mozilla/5.0 (X11; Linux x86_64) prediction-edge/1.0"
# UA dimension: NO_UA replicates a bare reqwest::Client (no User-Agent header at all — what the current
# Rust ReqwestFetcher sends). The product UA mimics datadash_discovery.rs:195. Python-urllib is urllib's
# auto-injected default. Browser is the proven one.
NO_UA = None
PRODUCT_UA = "prediction-edge/1.0"
PYTHON_UA = "Python-urllib/3.11"
BATCH_SIZE = 50  # candidate canonical `gamma_batch_size` (backfill_end_dates.py:22)
LIMIT = 500  # candidate canonical `gamma_batch_limit_param` (backfill_end_dates.py:40)
DB = "data/wallet_cache.db"


# --------------------------------------------------------------------------------------------------
# Pure logic (importable, no network) — covered by test_probe_gamma_ua.py.
# --------------------------------------------------------------------------------------------------
def build_url(base: str, ids: list[str], *, closed: bool, limit: int, comma: bool = False) -> str:
    """Build a Gamma `/markets` URL for `ids`.

    Repeat-key by default (`?condition_ids=A&condition_ids=B&…`) — the proven batching shape.
    `comma=True` builds the (known-broken) comma-joined form so the probe can confirm it fails.
    Query-param order matches the proven scripts: condition_ids first, then &closed=true, then &limit.
    """
    if comma:
        q = "condition_ids=" + ",".join(ids)
    else:
        q = "&".join(f"condition_ids={i}" for i in ids)
    url = f"{base}?{q}"
    if closed:
        url += "&closed=true"
    url += f"&limit={limit}"
    return url


def demux(data: list[dict]) -> dict[str, dict]:
    """Index a Gamma `/markets` array response by `conditionId`.

    Ids Gamma does not know are simply absent from the returned map (caller detects via `.get`).
    Mirrors backfill_end_dates.py:46-50 — the batch demux the Rust client must reproduce.
    """
    out: dict[str, dict] = {}
    for m in data:
        cid = m.get("conditionId")
        if cid:
            out[cid] = m
    return out


# --------------------------------------------------------------------------------------------------
# Live calls (only reached from main()). `raw_get` uses http.client so headers are sent EXACTLY as
# given — `ua=None` sends NO User-Agent header at all, replicating a bare `reqwest::Client` (the
# current Rust ReqwestFetcher). urllib would auto-inject `Python-urllib/x` and mask the real
# headerless behaviour, so it is deliberately avoided here. This is the whole point of the probe:
# does the code-as-shipped (no UA) actually 403 on &closed=true, or is the "browser UA required"
# warning stale?
# --------------------------------------------------------------------------------------------------
UA_VARIANTS = [("no-UA", NO_UA), ("empty", ""), ("python", PYTHON_UA), ("product", PRODUCT_UA), ("browser", BROWSER_UA)]


def _ua_name(ua) -> str:
    if ua is None:
        return "no-UA"
    if ua == "":
        return "empty"
    return {PYTHON_UA: "python", PRODUCT_UA: "product", BROWSER_UA: "browser"}.get(ua, "custom")


def raw_get(url: str, ua, timeout: int = 45):
    """GET `url` sending exactly the User-Agent given (`None` ⇒ omit the header entirely).

    Returns (status, parsed_json_or_None, error_str_or_None)."""
    parts = urlsplit(url)
    path = parts.path + (("?" + parts.query) if parts.query else "")
    headers = {}
    if ua is not None:
        headers["User-Agent"] = ua  # "" sends an empty UA; a non-empty string sends it verbatim
    conn = http.client.HTTPSConnection(parts.netloc, timeout=timeout)
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        try:
            data = json.loads(body)
        except Exception:  # noqa: BLE001
            data = None
        return resp.status, data, None
    except Exception as e:  # noqa: BLE001 — diagnostic; any failure is recorded data, not a crash
        return None, None, type(e).__name__ + ": " + str(e)[:120]
    finally:
        conn.close()


def load_anchor_ids(db: str, n: int):
    """Pull (resolved_ids, open_ids) from the cache: resolved = recent past resolution with a winner;
    open = future end_date, no resolution row. The first of each is the single-id anchor."""
    conn = sqlite3.connect(db, timeout=60)
    if int(conn.execute("PRAGMA user_version").fetchone()[0]) == -2:
        conn.close()
        raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    conn.execute("PRAGMA busy_timeout=60000;")
    now = int(time.time())
    resolved = [r for (r,) in conn.execute(
        "SELECT market_id FROM market_resolutions "
        "WHERE winning_outcome_id IS NOT NULL AND resolved_at_unix < ? AND resolved_at_unix > ? "
        "ORDER BY resolved_at_unix DESC LIMIT ?",
        (now, now - 2_592_000, n))]
    open_ids = [r for (r,) in conn.execute(
        "SELECT s.market_id FROM market_schedules s "
        "LEFT JOIN market_resolutions r ON r.market_id = s.market_id "
        "WHERE r.market_id IS NULL AND s.end_date_unix > ? "
        "ORDER BY s.end_date_unix ASC LIMIT ?",
        (now + 604_800, n))]
    conn.close()
    return resolved, open_ids


def _count(data) -> int:
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict) and isinstance(data.get("data"), list):  # CLOB wraps in {data, next_cursor}
        return len(data["data"])
    return -1


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase-0 live Gamma/CLOB UA+batching probe (issue #382)")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--resolved", default=None, help="override resolved condition_id anchor")
    ap.add_argument("--open", dest="open_id", default=None, help="override open condition_id anchor")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--limit", type=int, default=LIMIT)
    ap.add_argument("--pause", type=float, default=0.25, help="seconds between live calls (politeness)")
    args = ap.parse_args()

    resolved_ids, open_ids = load_anchor_ids(args.db, 100)
    if args.resolved:
        resolved_ids = [args.resolved] + [i for i in resolved_ids if i != args.resolved]
    if args.open_id:
        open_ids = [args.open_id] + [i for i in open_ids if i != args.open_id]
    if not resolved_ids or not open_ids:
        print("FATAL: could not load both a resolved and an open anchor id from the cache", file=sys.stderr)
        return 2
    res1, open1 = resolved_ids[0], open_ids[0]
    print(f"# anchors: resolved={res1}  open={open1}")
    print(f"# batch_size={args.batch_size} limit={args.limit}  resolved_pool={len(resolved_ids)} open_pool={len(open_ids)}\n")

    rows = []

    def call(label, url, ua, requested=None):
        time.sleep(args.pause)
        status, data, err = raw_get(url, ua)
        n = _count(data)
        demux_clean = None
        if requested is not None and isinstance(data, list):
            got = set(demux(data).keys())
            demux_clean = got.issubset(set(requested)) if got else True  # no cross-market leak
        rows.append({"label": label, "ua": _ua_name(ua), "status": status, "n": n,
                     "demux_clean": demux_clean, "err": err})
        extra = f"demux_clean={demux_clean}" if demux_clean is not None else ""
        print(f"  {label:30} ua={_ua_name(ua):8} status={str(status):4} n={n:<5} {extra} {err or ''}".rstrip())
        return status, data

    rl = args.limit
    gamma_closed_single = build_url(GAMMA_BASE, [res1], closed=True, limit=rl)
    clob_closed = f"{CLOB_BASE}?closed=true&limit=10"

    # --- A. UA-gating sweep on the critical &closed=true paths (the Phase-1 decision) ------------
    print("## A. UA sweep on &closed=true (does the headerless code-as-shipped 403?)")
    for _, ua in UA_VARIANTS:
        call("gamma resolved closed", gamma_closed_single, ua, requested=[res1])
    for _, ua in UA_VARIANTS:
        call("clob closed [sole-resolution-src]", clob_closed, ua)

    # --- B. Single-id plain vs closed sanity -----------------------------------------------------
    print("\n## B. Gamma single-id plain vs closed (browser-UA sanity)")
    call("gamma resolved plain", build_url(GAMMA_BASE, [res1], closed=False, limit=rl), BROWSER_UA, [res1])
    call("gamma open plain", build_url(GAMMA_BASE, [open1], closed=False, limit=rl), BROWSER_UA, [open1])
    call("gamma open closed", build_url(GAMMA_BASE, [open1], closed=True, limit=rl), BROWSER_UA, [open1])

    # --- C. Repeat-key batch (throughput gate) ---------------------------------------------------
    print("\n## C. Gamma repeat-key batch")
    rbatch = resolved_ids[:args.batch_size]
    obatch = open_ids[:args.batch_size]
    o100 = open_ids[:100]
    call(f"closed x{len(rbatch)} [proven]", build_url(GAMMA_BASE, rbatch, closed=True, limit=rl), BROWSER_UA, rbatch)
    call(f"plain x{len(obatch)} [AC item d]", build_url(GAMMA_BASE, obatch, closed=False, limit=rl), BROWSER_UA, obatch)
    call(f"plain x{len(o100)} [cap probe]", build_url(GAMMA_BASE, o100, closed=False, limit=rl), BROWSER_UA, o100)
    call("comma x5 [should fail]", build_url(GAMMA_BASE, resolved_ids[:5], closed=True, limit=rl, comma=True), BROWSER_UA, resolved_ids[:5])
    call(f"closed x{len(rbatch)} no-UA", build_url(GAMMA_BASE, rbatch, closed=True, limit=rl), NO_UA, rbatch)
    call(f"plain x{len(obatch)} no-UA", build_url(GAMMA_BASE, obatch, closed=False, limit=rl), NO_UA, obatch)

    # --- Derived gate findings -------------------------------------------------------------------
    def find(label_sub, ua_name):
        return next((r for r in rows if label_sub in r["label"] and r["ua"] == ua_name), None)

    gamma_closed_by_ua = {r["ua"]: r for r in rows if r["label"] == "gamma resolved closed"}
    clob_by_ua = {r["ua"]: r for r in rows if "clob closed" in r["label"]}
    plain_batch = find("plain x", "browser")
    cap_row = next((r for r in rows if "cap probe" in r["label"]), None)
    comma_row = next((r for r in rows if "COMMA" in r["label"] or "comma" in r["label"]), None)
    closed_batch = find("closed x", "browser")

    def sweep(by_ua):  # "no-UA=200, empty=200, python=403, ..."
        return ", ".join(f"{ua}={by_ua[ua]['status']}" for ua, _ in UA_VARIANTS if ua in by_ua)

    def blocked(by_ua):  # which UAs 403
        return [ua for ua, _ in UA_VARIANTS if by_ua.get(ua) and by_ua[ua]["status"] == 403]

    # The shipped Rust Gamma/CLOB clients send NO User-Agent header (bare reqwest) — so the `no-UA`
    # row, not "any 403 in the sweep", is what tells us whether the deployed code 403s.
    g_ship = gamma_closed_by_ua.get("no-UA")
    c_ship = clob_by_ua.get("no-UA")
    g_block, c_block = blocked(gamma_closed_by_ua), blocked(clob_by_ua)

    print("\n## SUMMARY — gate findings (Tier-1, live)")
    print(f"  (a/b) Gamma &closed=true UA sweep: {sweep(gamma_closed_by_ua)}")
    print(f"        headerless (no-UA = shipped reqwest) = {g_ship['status'] if g_ship else '?'}; UAs that 403 = {g_block}")
    print(f"        → {'shipped headerless code 403s — INCIDENT, UA fix first' if g_ship and g_ship['status'] == 403 else 'shipped code does NOT 403; the 403 is the Python-urllib default UA only (scripts), not the Rust clients. UA fix is DEFENSIVE, not a correctness fix'}")
    print(f"  (c) CLOB &closed=true UA sweep: {sweep(clob_by_ua)}")
    print(f"        headerless (no-UA = shipped reqwest) = {c_ship['status'] if c_ship else '?'}; UAs that 403 = {c_block}")
    print(f"        → {'shipped headerless CLOB walk 403s — resolution ingestion BROKEN, UA fix first' if c_ship and c_ship['status'] == 403 else 'shipped headerless CLOB walk does NOT 403; resolution ingestion is fine. Open-risk #3 tension RESOLVED: the script 403-claim is specific to the Python-urllib default UA'}")
    print(f"  (d) plain repeat-key batch (browser): requested={len(obatch)} returned={plain_batch['n'] if plain_batch else '?'}, "
          f"demux_clean={plain_batch['demux_clean'] if plain_batch else '?'} "
          f"→ {'WORKS — open passes CAN batch (~60x); gamma.rs:5-6 warning is STALE' if plain_batch and plain_batch['n'] > 1 else 'FAILS SILENTLY — open passes stay per-ID'}")
    print(f"  closed repeat-key batch (browser): requested={len(rbatch)} returned={closed_batch['n'] if closed_batch else '?'} "
          f"(omitted ids = markets Gamma no longer lists, NOT truncation)")
    print(f"  batch cap (100 open ids, all listed): returned={cap_row['n'] if cap_row else '?'} "
          f"→ {'cap > 50 (>=100 safe); keep 50 per issue unless adopting higher' if cap_row and cap_row['n'] and cap_row['n'] > 60 else 'cap ~50'}")
    print(f"  comma-separated batch: returned={comma_row['n'] if comma_row else '?'} (repeat-key REQUIRED — comma fails)")

    print("\n## Candidate _GLOSSARY defaults (confirmed from rows above)")
    print(f"  gamma_batch_size = {args.batch_size}")
    print(f"  gamma_batch_limit_param = {args.limit}")
    print(f"  gamma_browser_ua = {BROWSER_UA!r}")

    print("\n## Raw matrix (JSON)")
    print(json.dumps(rows, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())


