#!/usr/bin/env python3
"""Batch-3 Iter B2b: activity-adjusted re-ranking of B1's GBM scores.

Proxy experiment for B2: instead of retraining GBM with throughput label,
apply a post-hoc activity multiplier to B1's per-pos-edge GBM scores.

For each wallet, compute: adjusted_score = gbm_score * activity_weight
where activity_weight = first_entries_per_active_day_bps (scaled to [0,1])
or closed_trades (normalized), etc.

If this beats B1 at N=5k, it validates the throughput-label hypothesis.
If not, it means the GBM score already captures activity implicitly.

Much faster than B2 since it uses B1's saved rankings as a starting point.
Only need to load features (not positions) for the re-ranking.
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
TOP_N = 5_000

LOG = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb2b-activity-rerank.log')
B1_RANKED = Path('/home/sean/git/pm-ranker-iter3/data/ranker-iter-logs-b3/iterb1-ranked-wallets.json')

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
    total, n_pos = 0.0, 0
    for w in wallet_hexes:
        if w in edge_map:
            me, np_ = edge_map[w]
            total += me * np_
            n_pos += np_
    return total, n_pos


def main():
    lines = []
    def log(s):
        print(s, flush=True)
        lines.append(s)

    if not B1_RANKED.exists():
        log(f"ERROR: {B1_RANKED} not found. Run iterb1_topn_sweep.py first.")
        return

    b1_rankings = json.loads(B1_RANKED.read_text())
    cutoffs = data.distinct_cutoffs(DB)

    log(f"=== Iter B2b: Activity-adjusted re-ranking (proxy for B2) ===\n")
    log(f"B1 rankings available for: {list(b1_rankings.keys())}\n")

    # Activity weight strategies to test.
    # score_fn(df) returns an activity score (higher = more active).
    # The main loop computes adjusted_rank = gbm_rank / activity, so higher activity
    # pushes wallets to lower (better) adjusted rank. gbm_rank must NOT appear in
    # score_fn — it is applied as the numerator separately.
    strategies = {
        'gbm_only': lambda df: df['gbm_rank'].rank(ascending=True),  # baseline (B1)
        'gbm_x_ct': lambda df: np.log1p(df['closed_trades'].clip(lower=0)),
        'gbm_x_fpd': lambda df: np.log1p(df['first_entries_per_active_day_bps'].clip(lower=0)),
        'gbm_x_td': lambda df: np.log1p(df['trading_days'].clip(lower=0)),
    }

    results = {s: [] for s in strategies}

    log(f"{'anchor':12s}  {'pool':>6s}  " + "  ".join(f"{'$'+s:>15s}" for s in strategies))
    log("-" * (20 + 17 * len(strategies)))

    for anchor_str, ranked_wallets in b1_rankings.items():
        # Find matching cutoff
        anchor_dt = datetime.strptime(anchor_str, '%Y-%m-%d')
        anchor_unix = None
        for c in cutoffs:
            if datetime.utcfromtimestamp(c).date().isoformat() == anchor_str:
                anchor_unix = c
                break
        if anchor_unix is None:
            continue

        fwd_end = anchor_unix + FWD_SECS
        df = load_features(anchor_unix)

        # Add B1 GBM rank (1=best, len=worst) based on position in ranked_wallets list
        rank_map = {w: i+1 for i, w in enumerate(ranked_wallets)}
        df['gbm_rank'] = df['wallet_hex'].map(lambda w: rank_map.get(w, len(ranked_wallets)+1))
        # For activity-adjusted scores, higher = better → negate to turn score into rank
        # gbm_rank is already 1=best, so smaller = better wallet

        edge_map = per_wallet_edge(anchor_unix, fwd_end, df['wallet_hex'].tolist())

        row = []
        for strat_name, score_fn in strategies.items():
            # Score: lower = better (it's already a rank)
            # For activity-adjusted: divide rank by activity (higher activity → lower adjusted rank → selected)
            if strat_name == 'gbm_only':
                adjusted_rank = df['gbm_rank']
            else:
                activity = score_fn(df)  # higher activity = higher score
                # Adjusted rank = gbm_rank / log(1 + activity) -- lower = better
                # But we need to handle zero activity
                activity = activity.clip(lower=1e-6)
                adjusted_rank = df['gbm_rank'] / activity

            df_sorted = df.assign(adjusted_rank=adjusted_rank).sort_values('adjusted_rank')
            cohort = df_sorted['wallet_hex'].iloc[:min(TOP_N, len(df))].tolist()
            total, _ = total_pnl_for_cohort(cohort, edge_map)
            results[strat_name].append(total)
            row.append(f"${total:>+8.2f}")

        log(f"{anchor_str:12s}  {len(ranked_wallets):>6d}  " + "  ".join(f"{v:>15s}" for v in row))

    log(f"\n=== Summary: mean total PnL at N={TOP_N} ===")
    log(f"{'strategy':20s}  {'mean_$':>10s}  {'std_$':>10s}  {'vs_baseline':>12s}")
    log("-" * 60)
    baseline_mean = mean(results['gbm_only']) if results['gbm_only'] else 0
    for strat_name, vals in results.items():
        if not vals:
            continue
        m = mean(vals)
        s = stdev(vals) if len(vals) > 1 else 0
        vs = m / baseline_mean if abs(baseline_mean) > 0.01 else float('inf')
        winner = " ← WINNER" if m == max(mean(v) for v in results.values() if v) else ""
        log(f"{strat_name:20s}  ${m:>+9.2f}  ${s:>+9.2f}  {vs:>10.2f}×{winner}")

    LOG.write_text('\n'.join(lines))
    log(f"\nLog written to {LOG}")


if __name__ == '__main__':
    main()
