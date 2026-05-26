#!/usr/bin/env python3
"""Iter 7: LightGBM LambdaRank objective (vs regression).

Regression (iter 2) optimizes MSE on forward edge per wallet.
LambdaRank directly optimizes rank-NDCG — it cares about whether the
top-N wallets are the highest-edge wallets, not the absolute value.

If our production goal is "find the top-5000 wallets to copy", LambdaRank
is theoretically the right objective. Test if it actually helps in practice.
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
DEFAULT_WEIGHTS = {
    "SHARPE_BPS": 1500, "EV_MEAN_BPS": 833, "EV_TSTAT_BPS": 833,
    "BB_SHRUNK_EDGE_BPS": 833, "KELLY_LOG_GROWTH_BPS": 833,
    "BRIER_SCORE_BPS": -833, "BRIER_RESOLUTION_BPS": 833,
    "CONCENTRATION_HHI_BPS": -500, "CONCENTRATION_N_EFF_BPS": 500,
    "CONCENTRATION_RPC_BPS": -500,
    "FIRST_ENTRIES_PER_ACTIVE_DAY_BPS": 1000,
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS": -1000,
}
TOP_N = 5000
FWD_SECS = 30 * 86400
MIN_FWD_POS = 3
MIN_TRADING_DAYS = 20
MIN_DISTINCT_EVENTS = 10


def load_features(cutoff_unix):
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
        df = pd.read_sql_query(sql, con, params=(cutoff_unix, MIN_TRADING_DAYS, MIN_DISTINCT_EVENTS))
    df['total_pnl_usd'] = pd.to_numeric(df['total_pnl_usd_str'], errors='coerce').fillna(0)
    df['skill_pnl_usd'] = pd.to_numeric(df['skill_pnl_usd_str'], errors='coerce').fillna(0)
    df.drop(columns=['total_pnl_usd_str', 'skill_pnl_usd_str'], inplace=True)
    return df


def per_wallet_fwd_edge(cutoff, fwd_end, wallets):
    positions = data.load_oos_positions(DB, cutoff, fwd_end, frozenset(wallets))
    by = {}
    for p in positions:
        e = (p.outcome - p.vwap_entry) / p.vwap_entry
        if p.wallet_hex not in by:
            by[p.wallet_hex] = [0.0, 0]
        by[p.wallet_hex][0] += e
        by[p.wallet_hex][1] += 1
    return {w: (s / n, n) for w, (s, n) in by.items()}


def per_pos_flat(cutoff, fwd_end, hexes):
    pos = data.load_oos_positions(DB, cutoff, fwd_end, frozenset(hexes))
    if not pos:
        return 0.0, 0
    e = sum((p.outcome - p.vwap_entry) / p.vwap_entry for p in pos) / len(pos)
    return e, len(pos)


def discretize_labels(y, n_bins=31):
    """LambdaRank wants integer relevance labels [0, n_bins). Bin by quantile."""
    ranks = pd.Series(y).rank(method='dense').values
    bin_size = max(1, len(y) // n_bins)
    labels = np.minimum((ranks - 1) // bin_size, n_bins - 1).astype(int)
    return labels


def main():
    cutoffs = data.distinct_cutoffs(DB)
    print(f"=== Iter 7: LambdaRank objective ===\n")

    train_data = {}
    for i, c in enumerate(cutoffs):
        fwd_end = c + FWD_SECS
        df = load_features(c)
        labels = per_wallet_fwd_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= MIN_FWD_POS else None
        )
        df['n_fwd_pos'] = df['wallet_hex'].map(lambda w: labels.get(w, (None, 0))[1])
        print(f"  [{i+1}] {datetime.utcfromtimestamp(c).date()}  wallets={len(df):6d}  labeled={df['label'].notna().sum():6d}")
        train_data[c] = df

    comp_cache = {}
    def comp_top(cc):
        if cc not in comp_cache:
            _, hexes = objective.invoke_composite(BIN, DB, cc, DEFAULT_WEIGHTS, top_n=TOP_N)
            comp_cache[cc] = hexes
        return comp_cache[cc]

    lr_top = {}
    print("\n=== Walk-forward training (LambdaRank) ===")
    for i, anchor in enumerate(cutoffs):
        if i < 1:
            continue
        train_dfs = []
        for j in range(i):
            df = train_data[cutoffs[j]]
            tr = df[df['label'].notna()].copy()
            tr['__group__'] = j  # one group per cutoff
            train_dfs.append(tr)
        train_df = pd.concat(train_dfs, ignore_index=True)
        X = train_df[FEATURE_COLS].values.astype(np.float64)
        y_real = train_df['label'].values
        y = discretize_labels(y_real, n_bins=31)
        groups = train_df.groupby('__group__').size().values

        test = train_data[anchor]
        X_test = test[FEATURE_COLS].values.astype(np.float64)

        m = lgb.LGBMRanker(
            objective='lambdarank',
            n_estimators=400, learning_rate=0.05, num_leaves=31,
            min_data_in_leaf=200,
            verbose=-1, n_jobs=-1, random_state=42,
            label_gain=list(range(31)),
            lambdarank_truncation_level=100000,  # default 10000 too small for our group sizes
        )
        m.fit(X, y, group=groups)
        scores = m.predict(X_test)
        test = test.copy(); test['s'] = scores
        lr_top[anchor] = test.nlargest(TOP_N, 's')['wallet_hex'].tolist()
        print(f"  anchor [{i+1}] {datetime.utcfromtimestamp(anchor).date()}  "
              f"train_n={len(train_df):>7d}  test_n={len(test):>6d}  ranker top-{TOP_N}")

    print()
    print("=== Forward-edge comparison ===")
    print(f"{'anchor':12s}  {'comp_$':>9s}  {'gbm_lr_$':>9s}  {'lr/comp':>8s}")
    ce_l, le_l = [], []
    for i, anchor in enumerate(cutoffs):
        if i < 1 or anchor not in lr_top:
            continue
        fwd_end = anchor + FWD_SECS
        ce, _ = per_pos_flat(anchor, fwd_end, comp_top(anchor))
        le, _ = per_pos_flat(anchor, fwd_end, lr_top[anchor])
        r = le/ce if abs(ce) > 1e-9 else float('inf')
        print(f"{datetime.utcfromtimestamp(anchor).date().isoformat():12s}  "
              f"${ce:>+8.4f}  ${le:>+8.4f}  {r:>7.2f}x")
        ce_l.append(ce); le_l.append(le)
    if ce_l:
        print(f"\n  comp mean=${mean(ce_l):+.4f}  std=${stdev(ce_l) if len(ce_l) > 1 else 0:+.4f}")
        print(f"  LR   mean=${mean(le_l):+.4f}  std=${stdev(le_l) if len(le_l) > 1 else 0:+.4f}")

    # Save
    out = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter7-lambdarank-topn.json')
    out.write_text(json.dumps({str(c): v for c, v in lr_top.items()}))
    print(f"\nWrote {out}")


if __name__ == '__main__':
    main()
