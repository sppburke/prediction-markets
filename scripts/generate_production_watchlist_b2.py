#!/usr/bin/env python3
"""Generate production watchlist using the B2 throughput-label GBM.

Trains GBM on total_pnl_per_wallet (n_positions × mean_edge) using all
training cutoffs whose forward window has elapsed before 2026-05-01.
Scores wallets at 2026-05-01 and writes top-5k and top-10k watchlists.

The throughput label was found to beat per-pos-edge label by 20% mean across
7 walk-forward anchors (B2 finding).
"""
import sys, json
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb

sys.path.insert(0, '/home/sean/git/pm-ranker-iter3/scripts')
sys.path.insert(0, '/home/sean/git/prediction-markets/scripts')
from composite_tuner import data

DB = '/home/sean/git/prediction-markets/data/wallet_cache.db'

FEATURE_COLS = [
    "reconstruction_quality", "closed_trades", "distinct_markets", "distinct_events",
    "total_pnl_usd", "roi_bps", "win_rate_bps", "avg_hold_secs", "trading_days",
    "mean_daily_return_bps", "std_daily_return_bps", "sharpe_bps",
    "skewness_bps", "excess_kurtosis_bps", "lcb_5pct_bps",
    "skill_pnl_usd", "skill_pvalue_bps", "skill_permutations",
    "ev_mean_bps", "ev_tstat_bps", "bb_shrunk_edge_bps", "kelly_log_growth_bps",
    "brier_score_bps", "brier_resolution_bps",
    "concentration_hhi_bps", "concentration_n_eff_bps", "concentration_rpc_bps",
    "first_entries_per_active_day_bps", "median_first_entry_to_resolution_secs",
]
FWD_SECS = 30 * 86400
MIN_FWD_POS = 3
MIN_TRADING_DAYS = 20
MIN_DISTINCT_EVENTS = 10
TOP_N_5K = 5_000
TOP_N_10K = 10_000

OUT_5K = Path('/home/sean/git/pm-ranker-iter3/data/production-watchlist-b2-throughput-5k.txt')
OUT_10K = Path('/home/sean/git/pm-ranker-iter3/data/production-watchlist-b2-throughput-10k.txt')

import sqlite3

def load_features(c):
    sql = """
        SELECT wallet_hex, reconstruction_quality, closed_trades, distinct_markets,
               distinct_events, total_pnl_usd_str, roi_bps, win_rate_bps,
               avg_hold_secs, trading_days, mean_daily_return_bps,
               std_daily_return_bps, sharpe_bps, skewness_bps, excess_kurtosis_bps,
               lcb_5pct_bps, skill_pnl_usd_str, skill_pvalue_bps, skill_permutations,
               ev_mean_bps, ev_tstat_bps, bb_shrunk_edge_bps, kelly_log_growth_bps,
               brier_score_bps, brier_resolution_bps,
               concentration_hhi_bps, concentration_n_eff_bps, concentration_rpc_bps,
               first_entries_per_active_day_bps, median_first_entry_to_resolution_secs
        FROM wallet_features
        WHERE cutoff_unix = ?
          AND trading_days >= ?
          AND distinct_events >= ?
    """
    with sqlite3.connect(f'file:{DB}?mode=ro', uri=True) as con:
        df = pd.read_sql_query(sql, con, params=(c, MIN_TRADING_DAYS, MIN_DISTINCT_EVENTS))
    df['total_pnl_usd'] = pd.to_numeric(df['total_pnl_usd_str'], errors='coerce').fillna(0)
    df['skill_pnl_usd'] = pd.to_numeric(df['skill_pnl_usd_str'], errors='coerce').fillna(0)
    df.drop(columns=['total_pnl_usd_str', 'skill_pnl_usd_str'], inplace=True)
    return df


def per_wallet_edge(c, fwd_end, wallet_hexes):
    pos = data.load_oos_positions(DB, c, fwd_end, frozenset(wallet_hexes))
    by = {}
    for p in pos:
        e = (p.outcome - p.vwap_entry) / p.vwap_entry
        if p.wallet_hex not in by:
            by[p.wallet_hex] = [0.0, 0]
        by[p.wallet_hex][0] += e
        by[p.wallet_hex][1] += 1
    return {w: (s / n, n) for w, (s, n) in by.items()}


def main():
    cutoffs = data.distinct_cutoffs(DB)
    scoring_cutoff = cutoffs[-1]  # 2026-05-01
    scoring_str = datetime.utcfromtimestamp(scoring_cutoff).date().isoformat()
    print(f"=== B2 Production Watchlist Generator ===")
    print(f"Scoring cutoff: {scoring_str} ({scoring_cutoff})")

    # Training cutoffs: strictly before scoring, with fully elapsed forward windows
    train_cuts = [c for c in cutoffs if c < scoring_cutoff and c + FWD_SECS <= scoring_cutoff]
    print(f"Safe training cutoffs: {[datetime.utcfromtimestamp(c).date().isoformat() for c in train_cuts]}")

    print(f"\nLoading training data ({len(train_cuts)} cutoffs)...")
    train_dfs = []
    for c in train_cuts:
        df = load_features(c)
        fwd_end = c + FWD_SECS
        edge_map = per_wallet_edge(c, fwd_end, df['wallet_hex'].tolist())
        # Throughput label: n_positions × mean_edge (total forward PnL per wallet)
        df['label'] = df['wallet_hex'].map(
            lambda w: edge_map[w][0] * edge_map[w][1]
            if w in edge_map and edge_map[w][1] >= MIN_FWD_POS else None
        )
        tr = df[df['label'].notna()].copy()
        train_dfs.append(tr)
        print(f"  {datetime.utcfromtimestamp(c).date()}  labeled={len(tr)}")

    tr = pd.concat(train_dfs, ignore_index=True)
    X_tr = tr[FEATURE_COLS].values.astype(np.float64)
    y_tr = tr['label'].values.astype(np.float64)
    print(f"\nTotal training rows: {len(tr)}")

    print(f"\nTraining GBM (throughput label)...")
    m = lgb.LGBMRegressor(
        n_estimators=400, learning_rate=0.05, num_leaves=31,
        min_data_in_leaf=200, objective='regression',
        verbose=-1, n_jobs=-1, random_state=42,
    )
    m.fit(X_tr, y_tr)
    print("Training complete.")

    print(f"\nScoring {scoring_str} pool...")
    test = load_features(scoring_cutoff)
    X_te = test[FEATURE_COLS].values.astype(np.float64)
    test['score'] = m.predict(X_te)
    test_sorted = test.sort_values('score', ascending=False)

    print(f"Pool size: {len(test_sorted)}")

    # Write top-5k
    top5k = test_sorted['wallet_hex'].iloc[:TOP_N_5K].tolist()
    OUT_5K.write_text('\n'.join(top5k) + '\n')
    print(f"\nWrote top-{TOP_N_5K}: {OUT_5K}")

    # Write top-10k
    top10k = test_sorted['wallet_hex'].iloc[:min(TOP_N_10K, len(test_sorted))].tolist()
    OUT_10K.write_text('\n'.join(top10k) + '\n')
    print(f"Wrote top-{len(top10k)}: {OUT_10K}")

    # Print score distribution
    print(f"\nScore distribution (throughput label):")
    print(f"  top-1k mean score: {test_sorted['score'].iloc[:1000].mean():.4f}")
    print(f"  top-5k mean score: {test_sorted['score'].iloc[:5000].mean():.4f}")
    print(f"  top-10k mean score: {test_sorted['score'].iloc[:10000].mean():.4f}")
    print(f"  rank 5001 score:    {test_sorted['score'].iloc[5000]:.4f}")
    print(f"  rank 10001 score:   {test_sorted['score'].iloc[min(10000, len(test_sorted)-1)]:.4f}")


if __name__ == '__main__':
    main()
