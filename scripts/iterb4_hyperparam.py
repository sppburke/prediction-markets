#!/usr/bin/env python3
"""Batch-3 Iter B4: LightGBM hyperparameter tuning for throughput.

Batch-2 used fixed params: n_estimators=400, learning_rate=0.05, num_leaves=31,
min_data_in_leaf=200. These were arbitrary defaults.

This iter runs a coarse grid search over key hyperparameters and measures
total_pnl (throughput) at the B1-identified optimal N.

Grid:
  n_estimators:    200, 400, 800
  learning_rate:   0.01, 0.05, 0.1
  num_leaves:      15, 31, 63, 127
  min_data_in_leaf: 50, 200, 500

Due to walk-forward with 8 cutoffs, we can't do nested CV. Instead:
  - Use anchors [cutoffs[4], cutoffs[5], cutoffs[6]] as validation anchors
  - For each config, train on train_cuts and evaluate throughput at 3 val anchors
  - Pick winner config, then evaluate on ALL anchors for final comparison

This is NOT a proper CV — the validation anchors overlap with the test set.
It's a coarse check to distinguish clearly-bad from clearly-good configs.
"""
import sys, json
from pathlib import Path
from itertools import product
from statistics import mean
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

# Coarse param grid
PARAM_GRID = {
    'n_estimators': [200, 400, 800],
    'learning_rate': [0.01, 0.05, 0.10],
    'num_leaves': [15, 31, 63],
    'min_data_in_leaf': [50, 200, 500],
}

LOG = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb4-hyperparam.log')

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


def total_pnl_for_topn(test_sorted, edge_map, top_n):
    pool_size = len(test_sorted)
    n = pool_size if top_n is None else min(top_n, pool_size)
    total = 0.0
    for w in test_sorted['wallet_hex'].iloc[:n]:
        if w in edge_map:
            me, np_ = edge_map[w]
            total += me * np_
    return total


def train_and_score(X_tr, y_tr, test_df, params):
    m = lgb.LGBMRegressor(
        objective='regression', verbose=-1, n_jobs=-1, random_state=42,
        **params
    )
    m.fit(X_tr, y_tr)
    test = test_df.copy()
    test['s'] = m.predict(test[FEATURE_COLS].values.astype(np.float64))
    return test.sort_values('s', ascending=False)


def main():
    lines = []
    def log(s):
        print(s, flush=True)
        lines.append(s)

    cutoffs = data.distinct_cutoffs(DB)
    log(f"=== Iter B4: Hyperparameter tuning ===\n")
    log(f"Param grid size: {sum(1 for _ in product(*PARAM_GRID.values()))} configs\n")

    log("Loading data...")
    all_data = {}
    for c in cutoffs:
        df = load_features(c)
        fwd_end = c + FWD_SECS
        edge_map = per_wallet_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: edge_map[w][0] if w in edge_map and edge_map[w][1] >= MIN_FWD_POS else None
        )
        all_data[c] = {'df': df, 'edge_map': edge_map}
        log(f"  {datetime.utcfromtimestamp(c).date()}  wallets={len(df):6d}")

    # Use cutoffs[3], [4], [5] as validation anchors (have enough training data)
    val_anchors = cutoffs[3:6]
    log(f"\nValidation anchors: {[datetime.utcfromtimestamp(c).date().isoformat() for c in val_anchors]}")

    # B1's optimal N — use 10k as placeholder (update after B1 result)
    TOP_N = 10_000
    log(f"Throughput N for tuning: {TOP_N}\n")

    # Grid search
    configs = list(product(
        PARAM_GRID['n_estimators'],
        PARAM_GRID['learning_rate'],
        PARAM_GRID['num_leaves'],
        PARAM_GRID['min_data_in_leaf'],
    ))

    config_scores = []
    log(f"{'n_est':>6s}  {'lr':>5s}  {'leaves':>6s}  {'min_leaf':>8s}  {'val_mean':>10s}")
    log("-" * 50)

    for n_est, lr, leaves, min_leaf in configs:
        params = {'n_estimators': n_est, 'learning_rate': lr,
                  'num_leaves': leaves, 'min_data_in_leaf': min_leaf}
        val_totals = []
        for anchor in val_anchors:
            i = cutoffs.index(anchor)
            train_cuts = [c for c in cutoffs[:i] if c + FWD_SECS <= anchor]
            if not train_cuts:
                continue
            train_dfs = [all_data[c]['df'][all_data[c]['df']['label'].notna()].copy()
                        for c in train_cuts]
            tr = pd.concat(train_dfs, ignore_index=True)
            X_tr = tr[FEATURE_COLS].values.astype(np.float64)
            y_tr = tr['label'].values.astype(np.float64)

            sorted_test = train_and_score(X_tr, y_tr, all_data[anchor]['df'], params)
            total = total_pnl_for_topn(sorted_test, all_data[anchor]['edge_map'], TOP_N)
            val_totals.append(total)

        if val_totals:
            val_mean = mean(val_totals)
            config_scores.append((val_mean, params))
            log(f"{n_est:>6d}  {lr:>5.2f}  {leaves:>6d}  {min_leaf:>8d}  ${val_mean:>+9.2f}")

    config_scores.sort(key=lambda x: x[0], reverse=True)
    log(f"\n=== Top 5 configs by validation mean throughput ===")
    for val_mean, params in config_scores[:5]:
        log(f"  ${val_mean:+.2f}  {params}")

    best_params = config_scores[0][1]
    log(f"\nBest config: {best_params}")

    # Evaluate best config vs default on all anchors
    log(f"\n=== Walk-forward evaluation: best vs default ===")
    log(f"{'anchor':12s}  {'default_$':>10s}  {'best_$':>10s}  {'gain':>8s}")
    default_params = {'n_estimators': 400, 'learning_rate': 0.05,
                      'num_leaves': 31, 'min_data_in_leaf': 200}
    def_totals, best_totals = [], []
    for i, anchor in enumerate(cutoffs):
        anchor_str = datetime.utcfromtimestamp(anchor).date().isoformat()
        if i < 1:
            continue
        train_cuts = [c for c in cutoffs[:i] if c + FWD_SECS <= anchor]
        if not train_cuts:
            continue
        train_dfs = [all_data[c]['df'][all_data[c]['df']['label'].notna()].copy()
                    for c in train_cuts]
        tr = pd.concat(train_dfs, ignore_index=True)
        X_tr = tr[FEATURE_COLS].values.astype(np.float64)
        y_tr = tr['label'].values.astype(np.float64)

        def_sorted = train_and_score(X_tr, y_tr, all_data[anchor]['df'], default_params)
        best_sorted = train_and_score(X_tr, y_tr, all_data[anchor]['df'], best_params)
        edge_map = all_data[anchor]['edge_map']

        d_t = total_pnl_for_topn(def_sorted, edge_map, TOP_N)
        b_t = total_pnl_for_topn(best_sorted, edge_map, TOP_N)
        def_totals.append(d_t)
        best_totals.append(b_t)
        gain = (b_t - d_t) / abs(d_t) if abs(d_t) > 0.01 else float('inf')
        log(f"{anchor_str:12s}  ${d_t:>+9.2f}  ${b_t:>+9.2f}  {gain:>+7.1%}")

    log(f"\nDefault mean: ${mean(def_totals):+.2f}  Best mean: ${mean(best_totals):+.2f}")
    log(f"Gain: ${mean(best_totals) - mean(def_totals):+.2f}")

    LOG.write_text('\n'.join(lines))
    log(f"\nLog written to {LOG}")


if __name__ == '__main__':
    main()
