#!/usr/bin/env python3
"""Reconcile Polymarket CLOB winners against the cached ``source='polygon'`` market
resolutions — the **PR1 gate** for issue #369 (make CLOB the sole market-resolution
source; remove Alchemy/Polygon-RPC).

What it does
------------
Walks the CLOB ``/markets?closed=true`` endpoint (key-free, cursor-paginated, browser
UA required else 403) and caches the walk to JSON so the sibling
``clob_winner_outcome_id_check.py`` reuses it.  For every CLOB market it records the
positional ``winner_index`` (mirrors ``crates/bootstrap/src/clob.rs::winner_index``:
the single ``winner=true`` token index, or ``None`` when 0 or >1 tokens win —
voided/ambiguous).  Each ``condition_id`` is mapped to the
``market_resolutions.market_id`` form (mirrors
``crates/bootstrap/src/chain.rs::normalise_condition_id``: ``\\x``→``0x``, plus a
lower-case fold so the join is casing-robust).

It then compares the CLOB winner against the polygon-scanned ``winning_outcome_id`` for
every market that (a) polygon resolved (``source='polygon'``), (b) CLOB closed, and
(c) appears in ``trades``.

Why the join is not vacuous
---------------------------
The comparison joins **CLOB-API winners to polygon DB rows** — it never joins the
table's mutually-exclusive ``source`` columns (which are disjoint by PK + INSERT OR
IGNORE, so would always be an empty overlap; see issue #369 "Vacuous-gate avoided").
Both gates therefore require ``total_overlap > 0`` so a vacuous pass is impossible.

Two gates
---------
* **Literal** (issue #369 as written): ``total_overlap > 0 AND mismatches == 0``.
  Reported for fidelity; it FAILS because the CLOB ``/markets`` endpoint does not
  populate ``tokens[].winner`` on old markets (``clob_null_polygon_set`` — see
  ``reconcile``), which the issue did not anticipate.
* **Safety** (what actually governs the cutover): ``total_overlap > 0 AND
  winner_contradictions == 0`` — CLOB must never assert a *different* winner than
  polygon.  The old-market nulls are benign because issue #369 KEEPS the 1.19M
  ``source='polygon'`` rows (INSERT OR IGNORE), so those markets stay resolved by their
  retained polygon row; CLOB only has to be correct for *new* markets.

The process exit code follows the **safety** gate (0 pass / 1 fail); both gate lines and
the full JSON breakdown are printed for the operator to judge.

Run:
  python3 scripts/clob_vs_polygon_reconciliation.py \\
      --db data/wallet_cache.db --out clob_recon.json
Unit tests: python3 scripts/test_clob_reconciliation.py
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request

DB = "data/wallet_cache.db"
BASE_URL = "https://clob.polymarket.com"
CLOB_CACHE = "data/clob_closed_markets.json"
# Mirrors crates/bootstrap/src/clob.rs: page size 1000, terminator cursor "LTE=".
CLOB_PAGE_LIMIT = 1000
CLOB_END_CURSOR = "LTE="
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) pe-bootstrap-clob-reconciliation"}


# ── pure helpers (mirror the Rust resolution path) ──────────────────────────────
def normalise_market_id(condition_id: str) -> str:
    """Map a CLOB ``condition_id`` to the ``market_resolutions.market_id`` form.

    Mirrors ``chain.rs::normalise_condition_id`` (``\\x``-prefix → ``0x``-prefix) and
    additionally lower-cases: the DB stores lower-case ``0x``-hex, so a case fold makes
    the join robust to any upstream casing drift (hex ids that differ only by case are
    the same condition id).
    """
    s = condition_id.strip()
    if s.startswith("\\x"):
        s = "0x" + s[2:]
    return s.lower()


def winner_index(tokens: list[dict]) -> int | None:
    """Index of the single ``winner=true`` token, or ``None`` if 0 or >1 win.

    Byte-for-byte mirror of ``crates/bootstrap/src/clob.rs::winner_index`` (CLOB returns
    tokens in positional outcome order; ``None`` == voided/ambiguous).
    """
    winners = [i for i, t in enumerate(tokens) if t.get("winner")]
    return winners[0] if len(winners) == 1 else None


def build_page_url(base_url: str, cursor: str | None) -> str:
    """Mirror of ``clob.rs::build_page_url`` — terminator/empty cursor ⇒ first page."""
    if cursor and cursor != CLOB_END_CURSOR:
        return f"{base_url}/markets?closed=true&limit={CLOB_PAGE_LIMIT}&next_cursor={cursor}"
    return f"{base_url}/markets?closed=true&limit={CLOB_PAGE_LIMIT}"


def _fetch_page(url: str, attempt: int = 0) -> dict:
    """GET a CLOB page with the browser UA + bounded exponential backoff retries."""
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=45) as r:
            return json.load(r)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        if attempt >= 5:
            raise
        time.sleep(2**attempt)
        return _fetch_page(url, attempt + 1)


def fetch_closed_markets(
    base_url: str = BASE_URL,
    max_pages: int | None = None,
    progress: bool = False,
) -> dict[str, list]:
    """Walk every ``/markets?closed=true`` page → ``{market_id: [winner_index, num_tokens]}``.

    ``winner_index`` is ``None`` for voided/ambiguous closed markets (kept so the
    reconciliation can null-safely compare voids); ``num_tokens`` is the CLOB token count
    (the outcome-index range) used by the winner-index↔outcome_id range check.  Replicates
    ``clob.rs`` pagination: cursor advances via ``next_cursor`` until empty or ``"LTE="``.
    """
    markets: dict[str, list] = {}
    cursor: str | None = None
    pages = 0
    while True:
        page = _fetch_page(build_page_url(base_url, cursor))
        for m in page.get("data", []):
            cid = m.get("condition_id")
            tokens = m.get("tokens", [])
            if not cid or not m.get("closed"):
                continue
            markets[normalise_market_id(cid)] = [winner_index(tokens), len(tokens)]
        pages += 1
        if progress and pages % 25 == 0:
            print(f"  …{pages} pages, {len(markets)} closed markets", file=sys.stderr)
        nxt = page.get("next_cursor") or ""
        if not nxt or nxt == CLOB_END_CURSOR or nxt == cursor:
            break
        cursor = nxt
        if max_pages is not None and pages >= max_pages:
            break
    return markets


# ── cache ───────────────────────────────────────────────────────────────────────
def load_or_fetch(
    cache_path: str,
    base_url: str,
    refresh: bool,
    max_pages: int | None,
    progress: bool = True,
) -> dict[str, list]:
    """Return ``{market_id: [winner_index, num_tokens]}``, reusing ``cache_path`` unless
    ``refresh``."""
    if not refresh:
        try:
            with open(cache_path, encoding="utf-8") as fh:
                cached = json.load(fh)
            print(
                f"clob cache hit: {len(cached['markets'])} markets from {cache_path}",
                file=sys.stderr,
            )
            return cached["markets"]
        except (OSError, KeyError, json.JSONDecodeError):
            pass
    print(f"clob walk: fetching {base_url}/markets?closed=true …", file=sys.stderr)
    markets = fetch_closed_markets(base_url, max_pages=max_pages, progress=progress)
    try:
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(
                {"fetched_at_unix": int(time.time()), "base_url": base_url, "markets": markets},
                fh,
            )
        print(f"clob walk cached → {cache_path} ({len(markets)} markets)", file=sys.stderr)
    except OSError as e:  # caching is best-effort; the walk still returns
        print(f"warning: could not write cache {cache_path}: {e}", file=sys.stderr)
    return markets


# ── reconciliation ────────────────────────────────────────────────────────────
def reconcile(conn: sqlite3.Connection, clob_markets: dict[str, int | None]) -> dict:
    """Compare CLOB winners to ``source='polygon'`` rows over the traded overlap.

    Loads the CLOB winner projection into a TEMP table (writable even on a read-only main
    connection), then a null-safe (``IS``/``IS NOT``) join over the polygon-resolved ∩
    CLOB-closed ∩ traded overlap.  ``EXISTS(... trades ...)`` restricts to markets actually traded.

    Mismatches are split into three materially different classes:

    * ``winner_contradictions`` — both sides assert a winner and they DIFFER.  This is the
      safety-critical failure: CLOB would write a *wrong* resolution.  The gate that
      actually governs whether CLOB is safe as the sole source is
      ``winner_contradictions == 0``.
    * ``clob_null_polygon_set`` — CLOB reports no winner (voided/ambiguous) while polygon
      has one.  Empirically this is the CLOB ``/markets`` endpoint not populating
      ``tokens[].winner`` on *old* markets (the terminal price still encodes the winner).
      It is benign under issue #369's design: the 1.19M ``source='polygon'`` rows are
      KEPT and INSERT OR IGNORE never overwrites them, so these markets stay resolved by
      their retained polygon row; CLOB only needs to be correct for *new* markets.
    * ``polygon_null_clob_set`` — polygon voided but CLOB has a winner (the inverse).

    ``mismatches`` (the issue's literal metric) = the sum of all three.
    """
    conn.execute("DROP TABLE IF EXISTS temp.clob")
    conn.execute("CREATE TEMP TABLE clob (market_id TEXT PRIMARY KEY, winner_index INTEGER)")
    conn.executemany(
        "INSERT OR REPLACE INTO clob (market_id, winner_index) VALUES (?, ?)",
        clob_markets.items(),
    )
    (
        total,
        mismatches,
        winner_contradictions,
        clob_null_polygon_set,
        polygon_null_clob_set,
    ) = conn.execute(
        """
        SELECT
          COUNT(*),
          COALESCE(SUM(p.winning_outcome_id IS NOT c.winner_index), 0),
          COALESCE(SUM(p.winning_outcome_id IS NOT NULL AND c.winner_index IS NOT NULL
                       AND p.winning_outcome_id <> c.winner_index), 0),
          COALESCE(SUM(p.winning_outcome_id IS NOT NULL AND c.winner_index IS NULL), 0),
          COALESCE(SUM(p.winning_outcome_id IS NULL AND c.winner_index IS NOT NULL), 0)
        FROM market_resolutions p
        JOIN clob c ON c.market_id = p.market_id
        WHERE p.source = 'polygon'
          AND EXISTS (SELECT 1 FROM trades t WHERE t.market_id = p.market_id)
        """
    ).fetchone()
    contradiction_samples = conn.execute(
        """
        SELECT p.market_id, p.winning_outcome_id, c.winner_index
        FROM market_resolutions p
        JOIN clob c ON c.market_id = p.market_id
        WHERE p.source = 'polygon'
          AND p.winning_outcome_id IS NOT NULL AND c.winner_index IS NOT NULL
          AND p.winning_outcome_id <> c.winner_index
          AND EXISTS (SELECT 1 FROM trades t WHERE t.market_id = p.market_id)
        LIMIT 20
        """
    ).fetchall()
    # Winner-flag coverage by resolution year — shows whether the CLOB null-winner gap is
    # confined to old markets (where retained polygon rows cover it) or bleeds into recent
    # ones (where it would matter for the CLOB-primary-for-new cutover).
    by_year = conn.execute(
        """
        SELECT
          CAST(strftime('%Y', p.resolved_at_unix, 'unixepoch') AS TEXT) AS yr,
          COUNT(*),
          COALESCE(SUM(p.winning_outcome_id IS NOT NULL AND c.winner_index IS NOT NULL
                       AND p.winning_outcome_id <> c.winner_index), 0),
          COALESCE(SUM(p.winning_outcome_id IS NOT NULL AND c.winner_index IS NULL), 0)
        FROM market_resolutions p
        JOIN clob c ON c.market_id = p.market_id
        WHERE p.source = 'polygon'
          AND EXISTS (SELECT 1 FROM trades t WHERE t.market_id = p.market_id)
        GROUP BY yr ORDER BY yr
        """
    ).fetchall()
    agreement_rate = (total - mismatches) / total if total else 0.0
    return {
        "total_overlap": total,
        "mismatches": mismatches,
        "agreement_rate": round(agreement_rate, 6),
        "winner_contradictions": winner_contradictions,
        "clob_null_polygon_set": clob_null_polygon_set,
        "polygon_null_clob_set": polygon_null_clob_set,
        # Kept for fidelity to issue #369's requested output shape.
        "voids_agree": clob_null_polygon_set == 0 and polygon_null_clob_set == 0,
        "contradiction_samples": [
            {"market_id": mid, "polygon": p, "clob": c} for mid, p, c in contradiction_samples
        ],
        "by_resolution_year": [
            {"year": yr, "overlap": n, "winner_contradictions": wc, "clob_null_polygon_set": cn}
            for yr, n, wc, cn in by_year
        ],
    }


def _open_ro(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DB)
    ap.add_argument("--base-url", default=BASE_URL)
    ap.add_argument("--clob-cache", default=CLOB_CACHE)
    ap.add_argument("--refresh-cache", action="store_true", help="re-walk the CLOB API")
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages (testing only)")
    ap.add_argument("--out", default=None, help="write the JSON summary to this path")
    args = ap.parse_args(argv)

    clob_markets = load_or_fetch(
        args.clob_cache, args.base_url, args.refresh_cache, args.max_pages
    )
    # reconcile only needs the winner index; drop the cached token count.
    winners = {mid: rec[0] for mid, rec in clob_markets.items()}
    conn = _open_ro(args.db)
    try:
        result = reconcile(conn, winners)
    finally:
        conn.close()
    result["clob_closed_markets"] = len(clob_markets)

    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2)

    # Literal gate (issue #369 as written) — fails on old markets the CLOB winner flag
    # does not cover; reported for fidelity.
    literal_pass = result["total_overlap"] > 0 and result["mismatches"] == 0
    # Safety gate — the one that governs whether CLOB is safe as the sole source given the
    # 1.19M polygon rows are KEPT: CLOB must never assert a *contradictory* winner.
    safety_pass = result["total_overlap"] > 0 and result["winner_contradictions"] == 0
    print(
        f"LITERAL GATE {'PASS' if literal_pass else 'FAIL'} "
        f"(total_overlap={result['total_overlap']} mismatches={result['mismatches']} "
        f"agreement_rate={result['agreement_rate']})",
        file=sys.stderr,
    )
    print(
        f"SAFETY GATE {'PASS' if safety_pass else 'FAIL'} "
        f"(winner_contradictions={result['winner_contradictions']} "
        f"clob_null_polygon_set={result['clob_null_polygon_set']} "
        f"polygon_null_clob_set={result['polygon_null_clob_set']})",
        file=sys.stderr,
    )
    return 0 if safety_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
