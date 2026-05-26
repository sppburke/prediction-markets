#!/usr/bin/env python3
"""Batch-3 Iter B1: top-N sweep to find throughput-optimal N.

Batch-2 fixed top-N=5000. This iter trains the same GBM (iter2 methodology)
but scores ALL wallets and computes total_pnl at N = 1k, 2k, 5k, 10k, 20k,
50k, and full pool. Identifies the N that maximizes total PnL (throughput).

Key insight: if GBM correctly orders wallets, total PnL vs N rises then falls
(the marginal wallet's edge goes negative). Finding the peak N is the core
throughput question.
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

# N values to sweep — includes batch-2's 5k plus larger values
N_SWEEP = [1_000, 2_000, 5_000, 10_000, 20_000, 50_000, None]  # None = full pool

LOG = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb1-topn-sweep.log')
OUT = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb1-ranked-wallets.json')


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
    """Returns {wallet_hex: (mean_edge, n_positions)}."""
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
    """Total flat-$1 PnL for a cohort: sum of all position-level edges."""
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
    log(f"=== Iter B1: Top-N sweep (throughput optimization) ===\n")
    log(f"N values: {N_SWEEP}\n")
    log(f"Anchors available: {len(cutoffs)}\n")

    # Pre-load all features and labels
    log("Loading all feature/label data...")
    all_data = {}
    for c in cutoffs:
        df = load_features(c)
        fwd_end = c + FWD_SECS
        edge_map = per_wallet_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: edge_map.get(w, (None, 0))[0] if edge_map.get(w, (None, 0))[1] >= MIN_FWD_POS else None
        )
        all_data[c] = {'df': df, 'edge_map': edge_map, 'fwd_end': fwd_end}
        log(f"  {datetime.utcfromtimestamp(c).date()}  wallets={len(df):6d}  labeled={df['label'].notna().sum():6d}")

    log("")

    # Header
    n_labels = ['1k', '2k', '5k', '10k', '20k', '50k', 'ALL']
    header = f"{'anchor':12s}  {'pool':>6s}  " + "  ".join(f"{lbl:>10s}" for lbl in n_labels)
    log(header)
    log("-" * len(header))

    # Walk-forward
    results_by_n = {n: [] for n in N_SWEEP}
    ranked_wallets = {}  # anchor -> ordered list of wallet_hex (full ranking)

    for i, anchor in enumerate(cutoffs):
        anchor_str = datetime.utcfromtimestamp(anchor).date().isoformat()
        if i < 1:
            continue

        # Training: cutoffs strictly before anchor AND fwd window fully elapsed
        train_cuts = [c for c in cutoffs[:i] if c + FWD_SECS <= anchor]
        if not train_cuts:
            log(f"{anchor_str:12s}  (no safe training cutoffs — skip)")
            continue

        # Build training set
        train_dfs = []
        for c in train_cuts:
            tr = all_data[c]['df'][all_data[c]['df']['label'].notna()].copy()
            train_dfs.append(tr)
        tr = pd.concat(train_dfs, ignore_index=True)
        X_tr = tr[FEATURE_COLS].values.astype(np.float64)
        y_tr = tr['label'].values.astype(np.float64)

        # Train GBM
        m = lgb.LGBMRegressor(
            n_estimators=400, learning_rate=0.05, num_leaves=31,
            min_data_in_leaf=200, objective='regression',
            verbose=-1, n_jobs=-1, random_state=42,
        )
        m.fit(X_tr, y_tr)

        # Score all wallets at anchor
        test = all_data[anchor]['df'].copy()
        X_te = test[FEATURE_COLS].values.astype(np.float64)
        test['gbm_score'] = m.predict(X_te)
        test_sorted = test.sort_values('gbm_score', ascending=False)

        # Save full ranking
        ranked_wallets[anchor_str] = test_sorted['wallet_hex'].tolist()

        # Edge map at this anchor (full pool)
        edge_map = all_data[anchor]['edge_map']
        fwd_end = all_data[anchor]['fwd_end']
        pool_size = len(test_sorted)

        # Compute total PnL at each N
        row_vals = []
        for n_val in N_SWEEP:
            n_actual = pool_size if n_val is None else min(n_val, pool_size)
            cohort = test_sorted['wallet_hex'].iloc[:n_actual].tolist()
            total, n_pos = total_pnl_for_cohort(cohort, edge_map)
            results_by_n[n_val].append(total)
            row_vals.append(f"${total:>+8.2f}" if n_pos > 0 else "  (empty)")

        log(f"{anchor_str:12s}  {pool_size:>6d}  " + "  ".join(f"{v:>10s}" for v in row_vals))

    log("")
    log("=== Summary: mean total PnL ± std across anchors ===")
    log(f"{'N':>8s}  {'n_anchors':>9s}  {'mean_$':>10s}  {'std_$':>10s}  {'vs_5k':>8s}")
    log("-" * 55)

    ref_5k_mean = None
    best_n = None
    best_mean = None

    for n_val, lbl in zip(N_SWEEP, n_labels):
        vals = [v for v in results_by_n[n_val] if v != 0 or len(results_by_n[n_val]) > 0]
        # filter out anchors where we had 0 results (empty pool)
        vals = results_by_n[n_val]
        if not vals:
            continue
        m_val = mean(vals)
        s_val = stdev(vals) if len(vals) > 1 else 0.0

        if n_val == 5_000:
            ref_5k_mean = m_val
        vs = f"{m_val/ref_5k_mean:.2f}×" if ref_5k_mean else "—"
        log(f"{lbl:>8s}  {len(vals):>9d}  ${m_val:>+9.2f}  ${s_val:>+9.2f}  {vs:>8s}")

        if best_mean is None or m_val > best_mean:
            best_mean = m_val
            best_n = lbl

    log(f"\nThroughput winner: N={best_n} with mean total PnL ${best_mean:+.2f}")

    # Save ranked wallets JSON
    OUT.write_text(json.dumps(ranked_wallets))
    log(f"\nFull ranked wallet lists written to {OUT}")

    LOG.write_text('\n'.join(lines))
    log(f"Log written to {LOG}")


if __name__ == '__main__':
    main()
