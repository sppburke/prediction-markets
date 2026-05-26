#!/usr/bin/env python3
"""Iter 8: multi-seed variance estimate for GBM.

Question: how much of the GBM forward-edge variance is from LightGBM
stochastic training vs from actual signal? Trains GBM with N=5 different
seeds at each anchor, measures cross-seed std on the same forward-edge metric.

If cross-seed std >> across-anchor std, the per-anchor numbers are dominated
by training noise. If cross-seed std << across-anchor std, we have real signal.
"""
import sys, sqlite3, json
from pathlib import Path
from statistics import mean, stdev
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb

sys.path.insert(0, '/home/sean/git/pm-ranker-iter2/scripts')
from composite_tuner import objective, data

DB = '/home/sean/git/prediction-markets/data/wallet_cache.db'
BIN = '/home/sean/git/pm-ranker-iter2/target/release/pe-skill-select'

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
TOP_N = 5000
FWD_SECS = 30 * 86400
MIN_FWD_POS = 3
MIN_TRADING_DAYS = 20
MIN_DISTINCT_EVENTS = 10
SEEDS = [42, 100, 200, 300, 400]


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


def per_wallet_edge(c, fwd_end, ws):
    p = data.load_oos_positions(DB, c, fwd_end, frozenset(ws))
    by = {}
    for x in p:
        e = (x.outcome - x.vwap_entry) / x.vwap_entry
        if x.wallet_hex not in by:
            by[x.wallet_hex] = [0.0, 0]
        by[x.wallet_hex][0] += e
        by[x.wallet_hex][1] += 1
    return {w: (s/n, n) for w, (s, n) in by.items()}


def per_pos_flat(c, fwd_end, hexes):
    p = data.load_oos_positions(DB, c, fwd_end, frozenset(hexes))
    if not p:
        return 0.0, 0
    e = sum((x.outcome - x.vwap_entry) / x.vwap_entry for x in p) / len(p)
    return e, len(p)


def main():
    cutoffs = data.distinct_cutoffs(DB)
    print(f"=== Iter 8: multi-seed variance ({len(SEEDS)} seeds) ===\n")

    # Pick 3 anchors (avoid first 1 needing prior, avoid build-all-cutoffs slowness)
    target_anchors = [cutoffs[-3], cutoffs[-2], cutoffs[-1]]
    train_cutoffs = cutoffs[:-3] + [cutoffs[-3], cutoffs[-2]]  # include intermediate

    # Build training data for ALL train cutoffs once
    print("Building training datasets...")
    train_data = {}
    for c in cutoffs:
        fwd_end = c + FWD_SECS
        df = load_features(c)
        labels = per_wallet_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= MIN_FWD_POS else None
        )
        train_data[c] = df
        print(f"  {datetime.utcfromtimestamp(c).date()}  labeled={df['label'].notna().sum()}")

    print()
    print(f"{'anchor':12s}  {'seed':>4s}  {'top5000_$':>10s}  {'inter3_n':>8s}  {'inter3_$':>10s}")

    results = {}
    for anchor in target_anchors:
        anchor_str = datetime.utcfromtimestamp(anchor).date().isoformat()
        anchor_idx = cutoffs.index(anchor)
        if anchor_idx < 2:
            continue
        train_cuts = cutoffs[:anchor_idx]
        recent_3 = cutoffs[anchor_idx-2:anchor_idx+1]  # 3 most recent including anchor
        fwd_end = anchor + FWD_SECS

        train_dfs = []
        for c in train_cuts:
            train_dfs.append(train_data[c][train_data[c]['label'].notna()].copy())
        tr = pd.concat(train_dfs, ignore_index=True)
        X_tr = tr[FEATURE_COLS].values.astype(np.float64)
        y_tr = tr['label'].values.astype(np.float64)

        seed_results = []
        for seed in SEEDS:
            # GBM at anchor with this seed
            m = lgb.LGBMRegressor(
                n_estimators=400, learning_rate=0.05, num_leaves=31,
                min_data_in_leaf=200, objective='regression',
                verbose=-1, n_jobs=-1, random_state=seed,
            )
            m.fit(X_tr, y_tr)

            # Get rank at each of the 3 recent cutoffs
            cohort_per_cutoff = {}
            for rc in recent_3:
                te = train_data[rc]
                X_te = te[FEATURE_COLS].values.astype(np.float64)
                scores = m.predict(X_te)
                te2 = te.copy(); te2['s'] = scores
                cohort_per_cutoff[rc] = te2.nlargest(TOP_N, 's')['wallet_hex'].tolist()

            # Single (just anchor)
            ce, _ = per_pos_flat(anchor, fwd_end, cohort_per_cutoff[anchor])
            # Intersection_3
            inter = set(cohort_per_cutoff[recent_3[0]])
            for rc in recent_3[1:]:
                inter &= set(cohort_per_cutoff[rc])
            ie, in_n = per_pos_flat(anchor, fwd_end, list(inter))
            print(f"{anchor_str:12s}  {seed:>4d}  ${ce:>+9.4f}  {in_n:>8d}  ${ie:>+9.4f}")
            seed_results.append((ce, ie))

        if seed_results:
            ces = [s[0] for s in seed_results]
            ies = [s[1] for s in seed_results]
            print(f"  {'SEED SD':>16s}  ${stdev(ces):>+9.4f}  {'(any)':>8s}  ${stdev(ies):>+9.4f}")
            results[anchor_str] = {
                'single_seed_std': stdev(ces),
                'single_seed_mean': mean(ces),
                'inter3_seed_std': stdev(ies),
                'inter3_seed_mean': mean(ies),
            }
            print()

    print(f"=== Summary: cross-seed std vs cross-anchor std ===")
    if results:
        # Cross-anchor std from these 3 anchors
        single_means = [results[a]['single_seed_mean'] for a in results]
        inter3_means = [results[a]['inter3_seed_mean'] for a in results]
        print(f"Single (gbm_top_5000):")
        print(f"  cross-anchor mean=${mean(single_means):+.4f}  std=${stdev(single_means) if len(single_means) > 1 else 0:+.4f}")
        for a, r in results.items():
            print(f"  {a}: cross-seed std=${r['single_seed_std']:+.4f}  mean=${r['single_seed_mean']:+.4f}")

        print(f"\nIntersection_3 (gbm_top_5000 ∩ K=3):")
        print(f"  cross-anchor mean=${mean(inter3_means):+.4f}  std=${stdev(inter3_means) if len(inter3_means) > 1 else 0:+.4f}")
        for a, r in results.items():
            print(f"  {a}: cross-seed std=${r['inter3_seed_std']:+.4f}  mean=${r['inter3_seed_mean']:+.4f}")


if __name__ == '__main__':
    main()
