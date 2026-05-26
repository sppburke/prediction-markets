#!/usr/bin/env python3
"""Batch-3 Iter B2: train GBM on throughput-aligned label.

Batch-2 trained GBM on mean per-position edge per wallet.
This trains on total_pnl_per_wallet = n_positions * mean_edge.
If a wallet takes 100 positions at $+0.01 each, its label is $+1.00.
If a wallet takes 1 position at $+0.05, its label is $+0.05.

The hypothesis: training on total PnL per wallet teaches GBM to rank
high-activity + positive-edge wallets above low-activity + high-edge wallets.
This may increase total throughput compared to the per-pos-edge label.

Comparison: B2 (throughput label) vs B1 winner at same N values.
"""
import sys, json
from pathlib import Path
from statistics import mean, stdev
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
N_SWEEP = [5_000, 10_000, 20_000, 50_000, None]  # Compare vs B1 at same N

LOG = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb2-throughput-label.log')


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


def total_pnl_for_cohort(wallet_hexes, edge_map):
    total = 0.0
    n_pos = 0
    for w in wallet_hexes:
        if w in edge_map:
            me, np_ = edge_map[w]
            total += me * np_
            n_pos += np_
    return total, n_pos


def main():
    lines = []
    def log(s):
        print(s)
        lines.append(s)

    cutoffs = data.distinct_cutoffs(DB)
    log(f"=== Iter B2: throughput-aligned label (n_pos * mean_edge) ===\n")

    log("Loading all feature/label data...")
    all_data = {}
    for c in cutoffs:
        df = load_features(c)
        fwd_end = c + FWD_SECS
        edge_map = per_wallet_edge(c, fwd_end, df['wallet_hex'].tolist())
        # Throughput label: total PnL per wallet (n_positions * mean_edge)
        # Only include wallets with >= MIN_FWD_POS forward positions
        df['label_throughput'] = df['wallet_hex'].map(
            lambda w: edge_map[w][0] * edge_map[w][1]
            if w in edge_map and edge_map[w][1] >= MIN_FWD_POS else None
        )
        # Per-pos-edge label for comparison (same as batch-2 iter2)
        df['label_perpos'] = df['wallet_hex'].map(
            lambda w: edge_map[w][0]
            if w in edge_map and edge_map[w][1] >= MIN_FWD_POS else None
        )
        all_data[c] = {'df': df, 'edge_map': edge_map, 'fwd_end': fwd_end}
        log(f"  {datetime.utcfromtimestamp(c).date()}  wallets={len(df):6d}  "
            f"labeled={df['label_throughput'].notna().sum():6d}")

    log("")
    n_labels = ['5k', '10k', '20k', '50k', 'ALL']
    header = (f"{'anchor':12s}  {'pool':>6s}  "
              + "  ".join(f"{'B2-'+lbl:>10s}" for lbl in n_labels)
              + "  "
              + "  ".join(f"{'B1-'+lbl:>10s}" for lbl in n_labels))
    log(header)
    log("-" * len(header))

    results_b2 = {n: [] for n in N_SWEEP}
    results_b1 = {n: [] for n in N_SWEEP}  # re-run B1 (per-pos label) for direct comparison

    for i, anchor in enumerate(cutoffs):
        anchor_str = datetime.utcfromtimestamp(anchor).date().isoformat()
        if i < 1:
            continue

        train_cuts = [c for c in cutoffs[:i] if c + FWD_SECS <= anchor]
        if not train_cuts:
            log(f"{anchor_str:12s}  (no safe training cutoffs — skip)")
            continue

        edge_map = all_data[anchor]['edge_map']
        pool_size = len(all_data[anchor]['df'])

        b2_vals = []
        b1_vals = []
        for label_col, res_dict, label_list in [
            ('label_throughput', results_b2, b2_vals),
            ('label_perpos', results_b1, b1_vals),
        ]:
            train_dfs = []
            for c in train_cuts:
                tr = all_data[c]['df'][all_data[c]['df'][label_col].notna()].copy()
                tr['label'] = tr[label_col]
                train_dfs.append(tr)
            tr = pd.concat(train_dfs, ignore_index=True)
            X_tr = tr[FEATURE_COLS].values.astype(np.float64)
            y_tr = tr['label'].values.astype(np.float64)

            m = lgb.LGBMRegressor(
                n_estimators=400, learning_rate=0.05, num_leaves=31,
                min_data_in_leaf=200, objective='regression',
                verbose=-1, n_jobs=-1, random_state=42,
            )
            m.fit(X_tr, y_tr)

            test = all_data[anchor]['df'].copy()
            X_te = test[FEATURE_COLS].values.astype(np.float64)
            test['s'] = m.predict(X_te)
            test_sorted = test.sort_values('s', ascending=False)

            for n_val in N_SWEEP:
                n_actual = pool_size if n_val is None else min(n_val, pool_size)
                cohort = test_sorted['wallet_hex'].iloc[:n_actual].tolist()
                total, _ = total_pnl_for_cohort(cohort, edge_map)
                res_dict[n_val].append(total)
                label_list.append(f"${total:>+8.2f}")

        log(f"{anchor_str:12s}  {pool_size:>6d}  "
            + "  ".join(f"{v:>10s}" for v in b2_vals)
            + "  "
            + "  ".join(f"{v:>10s}" for v in b1_vals))

    log("")
    log("=== Summary: B2 (throughput label) vs B1 (per-pos label) ===")
    log(f"{'N':>8s}  {'B2_mean':>10s}  {'B1_mean':>10s}  {'B2/B1':>8s}")
    log("-" * 45)

    for n_val, lbl in zip(N_SWEEP, n_labels):
        b2_v = results_b2[n_val]
        b1_v = results_b1[n_val]
        if not b2_v or not b1_v:
            continue
        b2_m = mean(b2_v)
        b1_m = mean(b1_v)
        ratio = b2_m / b1_m if abs(b1_m) > 0.01 else float('inf')
        winner = "B2" if b2_m > b1_m else "B1"
        log(f"{lbl:>8s}  ${b2_m:>+9.2f}  ${b1_m:>+9.2f}  {ratio:>6.2f}×  ({winner})")

    LOG.write_text('\n'.join(lines))
    log(f"\nLog written to {LOG}")


if __name__ == '__main__':
    main()
