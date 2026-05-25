"""SQLite reads against `data/wallet_cache.db`.

Two responsibilities:
1. `load_wallet_features(cutoff_unix)` — feature rows for one cutoff. We only
   need the wallet_hex + the gate fields (skill_pvalue_bps, trading_days) +
   the 12 ranker features. Used to verify a cohort exists at a cutoff.
2. `load_oos_positions(cutoff_unix, fwd_end_unix, selected_wallets)` — the
   forward-edge ground truth. Joins trades + market_resolutions; VWAP-collapses
   per (wallet, market, outcome) so each scored position is one observation.

Per #238's design we do NOT call `pe-skill-select forward-test` per trial
(too slow). The Rust binary is the rank oracle; OOS edge is computed in
Python directly from the cache.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OosPosition:
    """One post-cutoff resolved buy, VWAP-collapsed."""
    wallet_hex: str
    vwap_entry: float  # mean entry price across this market/outcome buys, in (0, 1)
    outcome: float     # 1.0 won, 0.0 lost
    resolved_at_unix: int


def distinct_cutoffs(db_path: str | Path) -> list[int]:
    """All `cutoff_unix` values present in `wallet_features`, sorted ascending."""
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as c:
        rows = c.execute(
            "SELECT DISTINCT cutoff_unix FROM wallet_features ORDER BY cutoff_unix"
        ).fetchall()
    return [r[0] for r in rows]


def wallet_count_at_cutoff(db_path: str | Path, cutoff_unix: int) -> int:
    """How many `wallet_features` rows at this cutoff (sanity check)."""
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as c:
        row = c.execute(
            "SELECT COUNT(*) FROM wallet_features WHERE cutoff_unix = ?",
            (cutoff_unix,),
        ).fetchone()
    return row[0]


def load_oos_positions(
    db_path: str | Path,
    cutoff_unix: int,
    fwd_end_unix: int,
    selected_wallets: frozenset[str],
) -> list[OosPosition]:
    """Post-cutoff resolved buys for the selected cohort, VWAP-collapsed per
    (wallet, market, outcome). Filters:
    - t.wallet_hex IN selected
    - t.side = 'buy'
    - t.timestamp_unix in (cutoff_unix, fwd_end_unix]
    - resolution row exists for the market AND r.winning_outcome_id IS NOT NULL
    - r.resolved_at_unix >= t.timestamp_unix (drop data anomalies)
    - VWAP in (0, 1) — exclude $0 / $1 prints

    Returns one OosPosition per (wallet, market, outcome) group.
    Empty selected_wallets returns []. Sparse cohorts return []. SQLite
    `IN (?, ?, ?, …)` has a default 999-parameter cap so we batch by
    chunks of 900 wallets.
    """
    if not selected_wallets:
        return []
    out: list[OosPosition] = []
    wallets_list = list(selected_wallets)
    chunk = 900
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        for start in range(0, len(wallets_list), chunk):
            piece = wallets_list[start : start + chunk]
            qmarks = ",".join("?" * len(piece))
            sql = f"""
                SELECT
                    t.wallet_hex,
                    t.market_id,
                    t.outcome_id,
                    SUM(CAST(t.price_str AS REAL) * t.contracts) /
                        NULLIF(SUM(t.contracts), 0) AS vwap_entry,
                    CASE WHEN r.winning_outcome_id = t.outcome_id THEN 1.0 ELSE 0.0 END AS outcome,
                    MIN(r.resolved_at_unix) AS resolved_at_unix
                FROM trades t
                JOIN market_resolutions r USING (market_id)
                WHERE t.wallet_hex IN ({qmarks})
                  AND t.side = 'buy'
                  AND t.timestamp_unix > ?
                  AND t.timestamp_unix <= ?
                  AND r.winning_outcome_id IS NOT NULL
                  AND r.resolved_at_unix >= t.timestamp_unix
                GROUP BY t.wallet_hex, t.market_id, t.outcome_id
                HAVING vwap_entry > 0.0 AND vwap_entry < 1.0
            """
            params = [*piece, cutoff_unix, fwd_end_unix]
            for w, _mid, _oid, vwap, outcome, resolved_at in conn.execute(sql, params):
                out.append(
                    OosPosition(
                        wallet_hex=w,
                        vwap_entry=float(vwap),
                        outcome=float(outcome),
                        resolved_at_unix=int(resolved_at),
                    )
                )
    return out
