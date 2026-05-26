#!/usr/bin/env python3
"""Monthly production re-rank using the GBM ranker (per iter 2/3 finding).

Replaces scripts/monthly_rerank.py for the GBM workflow:
- Trains LightGBM on ALL historical (features, forward-edge) labels
  from cutoffs BEFORE the most-recent cutoff in wallet_features.
- Scores wallets at the most-recent cutoff.
- Optionally takes intersection_K across K most-recent cutoffs (default K=3).
- Writes a wallet hex list to --out.

Run after each `pe-skill-select extract` at a new monthly cutoff.

USAGE:
    .venv-analysis/bin/python3 scripts/monthly_rerank_gbm.py \
        --db-path data/wallet_cache.db \
        --strategy gbm_intersection_3 \
        --out data/production-watchlist-gbm.txt

STRATEGIES:
    gbm_single          GBM-top-N at most-recent cutoff only (highest Sharpe)
    gbm_intersection_3  GBM-top-N intersection across 3 most-recent cutoffs (highest mean)
    gbm_intersection_5  GBM-top-N intersection across 5 most-recent cutoffs (most selective)
"""
import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner import data as data_mod

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
DEFAULT_TOP_N = 5000
DEFAULT_FWD_DAYS = 30
DEFAULT_MIN_TRADING_DAYS = 20
DEFAULT_MIN_DISTINCT_EVENTS = 10
DEFAULT_MIN_FWD_POS = 3


def load_features(db, cutoff_unix, min_trading_days, min_distinct_events):
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
    with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as con:
        df = pd.read_sql_query(sql, con, params=(cutoff_unix, min_trading_days, min_distinct_events))
    df['total_pnl_usd'] = pd.to_numeric(df['total_pnl_usd_str'], errors='coerce').fillna(0)
    df['skill_pnl_usd'] = pd.to_numeric(df['skill_pnl_usd_str'], errors='coerce').fillna(0)
    df.drop(columns=['total_pnl_usd_str', 'skill_pnl_usd_str'], inplace=True)
    return df


def per_wallet_fwd_edge(db, cutoff_unix, fwd_end_unix, wallets):
    positions = data_mod.load_oos_positions(db, cutoff_unix, fwd_end_unix, frozenset(wallets))
    by = {}
    for p in positions:
        e = (p.outcome - p.vwap_entry) / p.vwap_entry
        if p.wallet_hex not in by:
            by[p.wallet_hex] = [0.0, 0]
        by[p.wallet_hex][0] += e
        by[p.wallet_hex][1] += 1
    return {w: (s / n, n) for w, (s, n) in by.items()}


def gbm_rank_at(db, cutoff_unix, fwd_secs, top_n, min_trading_days,
                min_distinct_events, min_fwd_pos, train_cutoffs):
    """Train GBM on train_cutoffs (labeled by fwd edge), score at cutoff_unix.
    Returns top-N wallet_hex list.
    """
    print(f"  training on {len(train_cutoffs)} prior cutoffs", file=sys.stderr)
    train_dfs = []
    for c in train_cutoffs:
        df = load_features(db, c, min_trading_days, min_distinct_events)
        labels = per_wallet_fwd_edge(db, c, c + fwd_secs, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= min_fwd_pos else None
        )
        train_dfs.append(df[df['label'].notna()].copy())

    tr = pd.concat(train_dfs, ignore_index=True)
    print(f"  train_rows={len(tr)}", file=sys.stderr)
    X = tr[FEATURE_COLS].values.astype(np.float64)
    y = tr['label'].values.astype(np.float64)

    test = load_features(db, cutoff_unix, min_trading_days, min_distinct_events)
    X_test = test[FEATURE_COLS].values.astype(np.float64)

    m = lgb.LGBMRegressor(
        n_estimators=400, learning_rate=0.05, num_leaves=31,
        min_data_in_leaf=200, objective='regression',
        verbose=-1, n_jobs=-1, random_state=42,
    )
    m.fit(X, y)
    test['s'] = m.predict(X_test)
    return test.nlargest(top_n, 's')['wallet_hex'].tolist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db-path', required=True)
    ap.add_argument('--strategy', default='gbm_intersection_3',
                    choices=['gbm_single', 'gbm_intersection_3', 'gbm_intersection_5'])
    ap.add_argument('--top-n', type=int, default=DEFAULT_TOP_N)
    ap.add_argument('--fwd-days', type=int, default=DEFAULT_FWD_DAYS)
    ap.add_argument('--min-trading-days', type=int, default=DEFAULT_MIN_TRADING_DAYS)
    ap.add_argument('--min-distinct-events', type=int, default=DEFAULT_MIN_DISTINCT_EVENTS)
    ap.add_argument('--min-fwd-pos', type=int, default=DEFAULT_MIN_FWD_POS)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    fwd_secs = args.fwd_days * 86400

    cutoffs = data_mod.distinct_cutoffs(args.db_path)
    if len(cutoffs) < 2:
        print(f"ERROR: need >=2 cutoffs, got {len(cutoffs)}", file=sys.stderr)
        sys.exit(1)

    K = {'gbm_single': 1, 'gbm_intersection_3': 3, 'gbm_intersection_5': 5}[args.strategy]
    if K > len(cutoffs):
        print(f"WARN: requested K={K} but only {len(cutoffs)} cutoffs available", file=sys.stderr)
        K = len(cutoffs)

    score_cutoffs = cutoffs[-K:]
    print(f"strategy={args.strategy} K={K}", file=sys.stderr)
    print(f"scoring cutoffs: {[datetime.utcfromtimestamp(c).date().isoformat() for c in score_cutoffs]}", file=sys.stderr)

    rankings = {}
    for sc in score_cutoffs:
        train = [c for c in cutoffs if c < sc]
        if not train:
            print(f"ERROR: no training data before {sc}", file=sys.stderr)
            sys.exit(1)
        print(f"\nRanking at {datetime.utcfromtimestamp(sc).date().isoformat()}", file=sys.stderr)
        rankings[sc] = gbm_rank_at(args.db_path, sc, fwd_secs, args.top_n,
                                    args.min_trading_days, args.min_distinct_events,
                                    args.min_fwd_pos, train)

    if K == 1:
        out_set = list(rankings[score_cutoffs[0]])
    else:
        out_set = set(rankings[score_cutoffs[0]])
        for sc in score_cutoffs[1:]:
            out_set &= set(rankings[sc])
        out_set = list(out_set)

    print(f"\nselected n={len(out_set)} wallets", file=sys.stderr)
    Path(args.out).write_text('\n'.join(out_set) + '\n')
    print(f"wrote {args.out}", file=sys.stderr)


if __name__ == '__main__':
    main()
