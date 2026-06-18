#!/usr/bin/env python3
"""Validate that the CLOB positional ``winner_index`` indexes the same outcome space as
``trades.outcome_id`` — the second PR1 proof for issue #369 (#1 open risk: multi-outcome
winner-index mapping).

CLOB resolutions store the winner as a **positional** token index (``winner_index``); the
cache DB keys trades by ``outcome_id``.  The risk is that for traded *multi-outcome*
markets (>2 tokens, which the polygon binary-only scan skipped) those two index spaces
might not coincide.

Why "is there a trade at the winner outcome?" is the WRONG test
--------------------------------------------------------------
The ``trades`` table holds only *our tracked wallets'* trades, not all Polymarket trades.
So a market with no trade at its winning outcome usually just means our wallets only
bought the losing side (one-sided trading) — NOT a broken mapping.  Empirically that
"no-trade-at-winner" set is ~56k binary markets whose ``winner_index`` matches polygon's
``winning_outcome_id`` exactly; gating on it would be a false alarm.

The gate that actually proves alignment: RANGE VALIDITY
-------------------------------------------------------
Every traded ``outcome_id`` must fall within ``[0, num_tokens)`` for its market.  An
out-of-range outcome would prove the trade index space does not fit the CLOB token index
space (so ``winner_index`` could not be trusted).  This is robust to one-sided trading.
Combined with ``clob_vs_polygon_reconciliation.py`` (winner_index == polygon
``winning_outcome_id`` for 848k binary markets, 0 contradictions) it establishes the
mapping.  Limitation: with no independent ground truth for multi-outcome winners, a pure
*permutation* within range is undetectable from our data alone (issue #369 fallback:
``token_id``-based mapping) — reported honestly, not gated.

``winner_outcome_traded`` / ``one_sided_no_winner_trade`` are reported as corroboration
only (subject to the one-sided confound), never gated.

Reuses the cached CLOB walk + helpers from ``clob_vs_polygon_reconciliation.py``.

Gate: ``out_of_range_markets == 0``.  Exit 0 on pass, 1 on fail.

Run:
  python3 scripts/clob_winner_outcome_id_check.py --db data/wallet_cache.db --out clob_winner_check.json
Unit tests: python3 scripts/test_clob_reconciliation.py
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys

import clob_vs_polygon_reconciliation as recon


def check(conn: sqlite3.Connection, clob_markets: dict[str, list]) -> dict:
    """Range-validate the winner-index ↔ outcome_id mapping over single-winner markets.

    ``clob_markets`` is ``{market_id: [winner_index, num_tokens]}``.  Single-winner markets
    only (``winner_index is None`` == voided/ambiguous, no positional winner).  Returns the
    traded-candidate count, the gate metric ``out_of_range_markets``, and the corroborating
    (non-gated) winner-present / one-sided breakdown.
    """
    rows = [
        (mid, rec[0], rec[1]) for mid, rec in clob_markets.items() if rec[0] is not None
    ]
    conn.execute("DROP TABLE IF EXISTS temp.cw")
    conn.execute(
        "CREATE TEMP TABLE cw "
        "(market_id TEXT PRIMARY KEY, winner_index INTEGER, num_tokens INTEGER)"
    )
    conn.executemany(
        "INSERT OR REPLACE INTO cw (market_id, winner_index, num_tokens) VALUES (?, ?, ?)",
        rows,
    )
    # One pass over cw; each aggregate is a correlated EXISTS over the trades index.
    candidates, winner_traded, out_of_range, multi_traded = conn.execute(
        """
        SELECT
          COALESCE(SUM(EXISTS(
            SELECT 1 FROM trades t WHERE t.market_id = c.market_id)), 0),
          COALESCE(SUM(EXISTS(
            SELECT 1 FROM trades t WHERE t.market_id = c.market_id
              AND t.outcome_id = c.winner_index)), 0),
          COALESCE(SUM(EXISTS(
            SELECT 1 FROM trades t WHERE t.market_id = c.market_id
              AND (t.outcome_id < 0 OR t.outcome_id >= c.num_tokens))), 0),
          COALESCE(SUM(CASE WHEN c.num_tokens > 2 AND EXISTS(
            SELECT 1 FROM trades t WHERE t.market_id = c.market_id) THEN 1 ELSE 0 END), 0)
        FROM cw c
        """
    ).fetchone()
    out_of_range_samples = []
    if out_of_range:
        out_of_range_samples = conn.execute(
            """
            SELECT c.market_id, c.winner_index, c.num_tokens FROM cw c
            WHERE EXISTS (SELECT 1 FROM trades t WHERE t.market_id = c.market_id
                          AND (t.outcome_id < 0 OR t.outcome_id >= c.num_tokens))
            LIMIT 20
            """
        ).fetchall()
    return {
        "candidates_in_trades": candidates,
        "out_of_range_markets": out_of_range,
        "multi_outcome_traded": multi_traded,
        "winner_outcome_traded": winner_traded,
        "one_sided_no_winner_trade": candidates - winner_traded,
        "out_of_range_samples": [
            {"market_id": mid, "winner_index": w, "num_tokens": n}
            for mid, w, n in out_of_range_samples
        ],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=recon.DB)
    ap.add_argument("--base-url", default=recon.BASE_URL)
    ap.add_argument("--clob-cache", default=recon.CLOB_CACHE)
    ap.add_argument("--refresh-cache", action="store_true", help="re-walk the CLOB API")
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages (testing only)")
    ap.add_argument("--out", default=None, help="write the JSON summary to this path")
    args = ap.parse_args(argv)

    clob_markets = recon.load_or_fetch(
        args.clob_cache, args.base_url, args.refresh_cache, args.max_pages
    )
    single_winner = sum(1 for rec in clob_markets.values() if rec[0] is not None)

    conn = recon._open_ro(args.db)
    try:
        result = check(conn, clob_markets)
    finally:
        conn.close()
    result["single_winner_markets"] = single_winner

    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2)

    passed = result["out_of_range_markets"] == 0
    print(
        f"GATE {'PASS' if passed else 'FAIL'}: out_of_range_markets="
        f"{result['out_of_range_markets']} "
        f"(candidates_in_trades={result['candidates_in_trades']} "
        f"multi_outcome_traded={result['multi_outcome_traded']} "
        f"winner_outcome_traded={result['winner_outcome_traded']})",
        file=sys.stderr,
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
