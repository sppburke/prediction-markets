#!/usr/bin/env python3
"""Batch-3 Iter B5: gate relaxation — does lowering min_trading_days/min_distinct_events
increase total throughput by including more net-positive wallets?

Batch-2 used min_trading_days=20, min_distinct_events=10. These gates were set
to reduce noise from short-history wallets. But for throughput, we might want to
include wallets with 10+ days of history if they have positive expected edge.

Tests:
  A. min_trading_days=20, min_distinct_events=10  (baseline)
  B. min_trading_days=10, min_distinct_events=5   (relaxed)
  C. min_trading_days=5,  min_distinct_events=3   (very relaxed)

For each gate config, train GBM on the gated pool, then measure total PnL at
N = 5k, 10k, 20k, ALL (within the gated pool).
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
N_SWEEP = [5_000, 10_000, 20_000, None]

GATE_CONFIGS = [
    ('baseline', 20, 10),
    ('relaxed',  10,  5),
    ('vrelaxed',  5,  3),
]

LOG = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb5-gate-relax.log')

import sqlite3

def load_features(c, min_td, min_de):
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
        df = pd.read_sql_query(sql, con, params=(c, min_td, min_de))
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


def run_gate_config(cutoffs, gate_name, min_td, min_de):
    print(f"\n=== Gate config: {gate_name} (min_trading_days={min_td}, min_distinct_events={min_de}) ===")

    all_data = {}
    for c in cutoffs:
        df = load_features(c, min_td, min_de)
        fwd_end = c + FWD_SECS
        edge_map = per_wallet_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: edge_map[w][0] if w in edge_map and edge_map[w][1] >= MIN_FWD_POS else None
        )
        all_data[c] = {'df': df, 'edge_map': edge_map, 'fwd_end': fwd_end}
        print(f"  {datetime.utcfromtimestamp(c).date()}  wallets={len(df):6d}  labeled={df['label'].notna().sum():6d}")

    n_labels = ['5k', '10k', '20k', 'ALL']
    results = {n: [] for n in N_SWEEP}

    for i, anchor in enumerate(cutoffs):
        anchor_str = datetime.utcfromtimestamp(anchor).date().isoformat()
        if i < 1:
            continue
        train_cuts = [c for c in cutoffs[:i] if c + FWD_SECS <= anchor]
        if not train_cuts:
            continue

        train_dfs = []
        for c in train_cuts:
            tr = all_data[c]['df'][all_data[c]['df']['label'].notna()].copy()
            train_dfs.append(tr)
        tr = pd.concat(train_dfs, ignore_index=True)
        X_tr = tr[FEATURE_COLS].values.astype(np.float64)
        y_tr = tr['label'].values.astype(np.float64)

        m = lgb.LGBMRegressor(
            n_estimators=400, learning_rate=0.05, num_leaves=31,
            min_data_in_leaf=max(20, len(tr) // 200),
            objective='regression', verbose=-1, n_jobs=-1, random_state=42,
        )
        m.fit(X_tr, y_tr)

        test = all_data[anchor]['df'].copy()
        X_te = test[FEATURE_COLS].values.astype(np.float64)
        test['s'] = m.predict(X_te)
        test_sorted = test.sort_values('s', ascending=False)
        edge_map = all_data[anchor]['edge_map']
        pool_size = len(test_sorted)

        row = []
        for n_val in N_SWEEP:
            n_actual = pool_size if n_val is None else min(n_val, pool_size)
            cohort = test_sorted['wallet_hex'].iloc[:n_actual].tolist()
            total, _ = total_pnl_for_cohort(cohort, edge_map)
            results[n_val].append(total)
            row.append(f"${total:>+8.2f}")

        print(f"  {anchor_str}  pool={pool_size:6d}  " + "  ".join(f"{lbl}={v}" for lbl, v in zip(n_labels, row)))

    print(f"\n  Summary:")
    gate_summary = {}
    for n_val, lbl in zip(N_SWEEP, n_labels):
        vals = results[n_val]
        if vals:
            m_val = mean(vals)
            s_val = stdev(vals) if len(vals) > 1 else 0.0
            gate_summary[lbl] = m_val
            print(f"    {lbl:>5s}: mean=${m_val:+.2f}  std=${s_val:+.2f}")
    return gate_summary


def main():
    import io, sys as _sys
    buf = io.StringIO()
    original_stdout = _sys.stdout

    cutoffs = data.distinct_cutoffs(DB)
    print(f"=== Iter B5: Gate relaxation sweep ===\n")
    print(f"Cutoffs: {len(cutoffs)}\n")

    summaries = {}
    for gate_name, min_td, min_de in GATE_CONFIGS:
        summaries[gate_name] = run_gate_config(cutoffs, gate_name, min_td, min_de)

    print("\n=== Final comparison: baseline vs relaxed vs vrelaxed ===")
    print(f"{'N':>8s}  {'baseline':>10s}  {'relaxed':>10s}  {'vrelaxed':>10s}")
    print("-" * 45)
    n_labels = ['5k', '10k', '20k', 'ALL']
    for lbl in n_labels:
        b = summaries.get('baseline', {}).get(lbl, 0)
        r = summaries.get('relaxed', {}).get(lbl, 0)
        v = summaries.get('vrelaxed', {}).get(lbl, 0)
        winner = 'baseline' if b >= r and b >= v else ('relaxed' if r >= v else 'vrelaxed')
        print(f"{lbl:>8s}  ${b:>+9.2f}  ${r:>+9.2f}  ${v:>+9.2f}  ({winner} wins)")

    print(f"\nLog written to {LOG}")


if __name__ == '__main__':
    import sys
    lines = []
    original_write = sys.stdout.write
    def capturing_write(s):
        lines.append(s)
        return original_write(s)
    sys.stdout.write = capturing_write

    main()

    LOG.write_text(''.join(lines))
