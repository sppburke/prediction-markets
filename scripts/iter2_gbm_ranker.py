#!/usr/bin/env python3
"""Iter 2: LightGBM ranker over ALL wallet_features columns.

Walk-forward CV:
  For each anchor cutoff t_i (i >= 2):
    Train on data from cutoffs {t_1, ..., t_{i-1}} with labels = forward edge
    Predict scores at t_i for all wallets
    Take top-N, measure forward edge in [t_i, t_i + 30d]

Compare per-anchor:
  - Default composite top-5000 (baseline)
  - GBM top-5000
  - intersection_K (composite, K recent)
  - intersection_K (GBM, K recent)

Hypothesis: GBM beats composite within-cutoff because it can use the
extra features (closed_trades, distinct_markets, lcb_5pct_bps, etc.) and
non-linear interactions. Temporal-ensemble stacking on top of GBM may
yield additional lift.
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

# All numeric features in wallet_features (except wallet_hex/cutoff_unix/extracted_at_unix/strings).
# Strings (total_pnl_usd_str, skill_pnl_usd_str) parsed to floats.
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
MIN_FWD_POS_FOR_LABEL = 3  # require ≥3 fwd positions to label a wallet
MIN_TRADING_DAYS = 20      # match composite gate
MIN_DISTINCT_EVENTS = 10   # match composite gate


def load_features_for_cutoff(cutoff_unix):
    """Read wallet_features at cutoff, return DataFrame with FEATURE_COLS + wallet_hex.
    Applies the same gates the composite uses (min_trading_days, min_distinct_events).
    """
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


def per_wallet_forward_edge(cutoff_unix, fwd_end_unix, wallet_hexes):
    """Compute per-wallet mean per-position flat-$1 forward edge for given wallets.
    Returns dict {wallet_hex: (mean_edge, n_positions)}.
    """
    positions = data.load_oos_positions(DB, cutoff_unix, fwd_end_unix,
                                         frozenset(wallet_hexes))
    by_wallet = {}
    for p in positions:
        edge = (p.outcome - p.vwap_entry) / p.vwap_entry
        if p.wallet_hex not in by_wallet:
            by_wallet[p.wallet_hex] = [0.0, 0]
        by_wallet[p.wallet_hex][0] += edge
        by_wallet[p.wallet_hex][1] += 1
    return {w: (s / n, n) for w, (s, n) in by_wallet.items()}


def per_pos_flat(cutoff, fwd_end, hexes):
    pos = data.load_oos_positions(DB, cutoff, fwd_end, frozenset(hexes))
    if not pos:
        return 0.0, 0
    e = sum((p.outcome - p.vwap_entry) / p.vwap_entry for p in pos) / len(pos)
    return e, len(pos)


def main():
    cutoffs = data.distinct_cutoffs(DB)
    print(f"=== Iter 2: LightGBM ranker ===")
    print(f"=== {len(cutoffs)} cutoffs, {len(FEATURE_COLS)} features ===")
    for i, c in enumerate(cutoffs):
        print(f"  [{i+1}] {c} = {datetime.utcfromtimestamp(c).date()}")
    print()

    # Step 1: build training datasets for each cutoff.
    #   features = wallet_features at cutoff
    #   label = mean per-pos flat-$1 forward edge in next 30 days
    #   Only wallets with >=MIN_FWD_POS_FOR_LABEL positions are labeled.
    train_data = {}
    print("=== Building training rows per cutoff ===")
    for i, c in enumerate(cutoffs):
        fwd_end = c + FWD_SECS
        feats = load_features_for_cutoff(c)
        labels = per_wallet_forward_edge(c, fwd_end, feats['wallet_hex'].tolist())
        feats['label'] = feats['wallet_hex'].map(
            lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= MIN_FWD_POS_FOR_LABEL else None
        )
        feats['n_fwd_pos'] = feats['wallet_hex'].map(lambda w: labels.get(w, (None, 0))[1])
        print(f"  [{i+1}] {datetime.utcfromtimestamp(c).date()}  "
              f"wallets={len(feats):6d}  labeled={feats['label'].notna().sum():6d}")
        train_data[c] = feats

    # Step 2: walk-forward — for each anchor i (i >= 2), train on cutoffs 1..i-1, score at i.
    rank_cache_composite = {}
    def composite_top(c):
        if c not in rank_cache_composite:
            _, hexes = objective.invoke_composite(BIN, DB, c, DEFAULT_WEIGHTS, top_n=TOP_N)
            rank_cache_composite[c] = hexes
        return rank_cache_composite[c]

    gbm_top_n = {}
    print()
    print("=== Training & scoring walk-forward ===")
    for i, anchor in enumerate(cutoffs):
        if i < 1:
            continue  # need ≥1 prior cutoff for training
        train_dfs = []
        for j in range(i):
            df = train_data[cutoffs[j]]
            df_lab = df[df['label'].notna()].copy()
            train_dfs.append(df_lab)
        train_df = pd.concat(train_dfs, ignore_index=True)
        X_train = train_df[FEATURE_COLS].values.astype(np.float64)
        y_train = train_df['label'].values.astype(np.float64)

        # Score wallets at anchor
        test_df = train_data[anchor]
        X_test = test_df[FEATURE_COLS].values.astype(np.float64)

        model = lgb.LGBMRegressor(
            n_estimators=400,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=-1,
            min_data_in_leaf=200,
            objective='regression',
            verbose=-1,
            n_jobs=-1,
            random_state=42,
        )
        model.fit(X_train, y_train)
        scores = model.predict(X_test)
        test_df = test_df.copy()
        test_df['gbm_score'] = scores
        top = test_df.nlargest(TOP_N, 'gbm_score')['wallet_hex'].tolist()
        gbm_top_n[anchor] = top
        print(f"  anchor [{i+1}] {datetime.utcfromtimestamp(anchor).date()}  "
              f"train_n={len(train_df):>7d}  test_n={len(test_df):>6d}  "
              f"gbm top-{TOP_N} selected")

    # Step 3: evaluate at each anchor.
    print()
    print("=== Forward-edge comparison ===")
    print(f"{'anchor':12s} {'composite_n':>11s} {'comp_edge':>10s}  "
          f"{'gbm_n':>7s} {'gbm_edge':>10s}  "
          f"{'gbm/comp':>9s}")
    print("-" * 75)
    comp_edges, gbm_edges = [], []
    for i, anchor in enumerate(cutoffs):
        if i < 1:
            continue
        if anchor not in gbm_top_n:
            continue
        fwd_end = anchor + FWD_SECS
        ce, cn = per_pos_flat(anchor, fwd_end, composite_top(anchor))
        ge, gn = per_pos_flat(anchor, fwd_end, gbm_top_n[anchor])
        ratio = ge / ce if abs(ce) > 1e-9 else float('inf')
        print(f"{datetime.utcfromtimestamp(anchor).date().isoformat():12s} "
              f"{cn:>11d} ${ce:>+9.4f}  {gn:>7d} ${ge:>+9.4f}  {ratio:>8.2f}x")
        comp_edges.append(ce)
        gbm_edges.append(ge)

    print("-" * 75)
    if comp_edges:
        print(f"  COMPOSITE  mean=${mean(comp_edges):+.4f}  "
              f"std=${stdev(comp_edges) if len(comp_edges) > 1 else 0:+.4f}")
        print(f"  GBM        mean=${mean(gbm_edges):+.4f}  "
              f"std=${stdev(gbm_edges) if len(gbm_edges) > 1 else 0:+.4f}")

    # Step 4: write GBM top-N per cutoff for downstream ensemble stacking (iter 3).
    out_path = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2-gbm-topn.json')
    out = {str(c): top for c, top in gbm_top_n.items()}
    out_path.write_text(json.dumps(out))
    print(f"\nWrote {out_path}")


if __name__ == '__main__':
    main()
