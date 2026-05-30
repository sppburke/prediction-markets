"""Data-access layer for Stage-2 portfolio construction.

numpy/local — requires .venv-analysis.  Not CI-tested.

Re-exports composite_tuner.data symbols used by validate/constructor,
and adds load_wallet_market_sets for the Jaccard overlap computation.
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from composite_tuner import data as _ct_data  # noqa: E402

# Re-exports from composite_tuner.data
OosPosition = _ct_data.OosPosition
load_oos_positions = _ct_data.load_oos_positions
distinct_cutoffs = _ct_data.distinct_cutoffs
POLYMARKET_FEE_RATE_BPS = _ct_data.POLYMARKET_FEE_RATE_BPS
SLIPPAGE_RATE_BPS = _ct_data.SLIPPAGE_RATE_BPS


def load_wallet_market_sets(db, cutoff_unix, wallets, lookback_secs):
    """Return per-wallet frozensets of (market_id, outcome_id) buy positions.

    Query window: (cutoff_unix - lookback_secs, cutoff_unix] — strictly
    trailing, no look-ahead.  Served by idx_trades_wallet_ts (wallet_hex,
    timestamp_unix).  Chunked at 900 wallets like load_oos_positions.

    Args:
        db: path to wallet_cache.db.
        cutoff_unix: upper bound (inclusive) for trade timestamps.
        wallets: iterable of wallet hex addresses.
        lookback_secs: window width in seconds (e.g. 90*86400 for 90 days).

    Returns:
        dict[wallet_hex -> frozenset[(market_id, outcome_id)]]
    """
    wallets_list = list(wallets)
    window_start = cutoff_unix - lookback_secs
    result = {w: set() for w in wallets_list}

    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as con:
        for start in range(0, len(wallets_list), 900):
            piece = wallets_list[start:start + 900]
            qmarks = ','.join('?' * len(piece))
            rows = con.execute(
                f"""SELECT wallet_hex, market_id, outcome_id
                    FROM trades
                    WHERE wallet_hex IN ({qmarks})
                      AND side = 'buy'
                      AND timestamp_unix > ?
                      AND timestamp_unix <= ?""",
                [*piece, window_start, cutoff_unix],
            ).fetchall()
            for w, mid, oid in rows:
                if w in result:
                    result[w].add((mid, oid))

    return {w: frozenset(v) for w, v in result.items()}
