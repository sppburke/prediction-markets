#!/usr/bin/env python3
"""Iter 4: add trajectory features to the GBM ranker.

Trajectory features (computed from trades table, NOT in wallet_features):
  - trades_30d_pre, trades_90d_pre  (recency volume)
  - days_since_last_trade  (recency)
  - distinct_markets_30d  (recent breadth)
  - pnl_30d_pre_implied (sum of realized PnL signal in last 30d, where resolved)
  - velocity_ratio = trades_30d / trades_90d  (accelerating or decelerating)

These extend the iter-2 feature set from 29 to 35 columns.

Same walk-forward CV as iter 2; compare per-anchor edges and pick winner.
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

BASE_FEATURE_COLS = [
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
TRAJ_FEATURE_COLS = [
    "trades_30d_pre", "trades_90d_pre", "days_since_last_trade",
    "distinct_markets_30d", "velocity_ratio_30_90",
]
ALL_FEATURES = BASE_FEATURE_COLS + TRAJ_FEATURE_COLS

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


def load_base_features(cutoff_unix):
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


def load_trajectory_features(cutoff_unix, wallets):
    """Compute trajectory features for given wallets at cutoff."""
    if not wallets:
        return pd.DataFrame(columns=['wallet_hex'] + TRAJ_FEATURE_COLS)
    cutoff_30 = cutoff_unix - 30 * 86400
    cutoff_90 = cutoff_unix - 90 * 86400
    wallets_list = list(wallets)
    chunks = [wallets_list[i:i+900] for i in range(0, len(wallets_list), 900)]
    parts = []
    with sqlite3.connect(f'file:{DB}?mode=ro', uri=True) as conn:
        for piece in chunks:
            qmarks = ",".join("?" * len(piece))
            sql = f"""
                SELECT
                    wallet_hex,
                    SUM(CASE WHEN timestamp_unix > ? THEN 1 ELSE 0 END) AS trades_30d_pre,
                    SUM(CASE WHEN timestamp_unix > ? THEN 1 ELSE 0 END) AS trades_90d_pre,
                    MAX(timestamp_unix) AS last_trade_unix,
                    COUNT(DISTINCT CASE WHEN timestamp_unix > ? THEN market_id END) AS distinct_markets_30d
                FROM trades
                WHERE wallet_hex IN ({qmarks})
                  AND timestamp_unix <= ?
                GROUP BY wallet_hex
            """
            params = [cutoff_30, cutoff_90, cutoff_30, *piece, cutoff_unix]
            for w, t30, t90, last_ts, dm30 in conn.execute(sql, params):
                days_since = (cutoff_unix - (last_ts or cutoff_unix)) / 86400.0
                velocity = (t30 or 0) / max(t90 or 1, 1) if (t90 or 0) > 0 else 0.0
                parts.append({
                    'wallet_hex': w,
                    'trades_30d_pre': t30 or 0,
                    'trades_90d_pre': t90 or 0,
                    'days_since_last_trade': days_since,
                    'distinct_markets_30d': dm30 or 0,
                    'velocity_ratio_30_90': velocity,
                })
    return pd.DataFrame(parts)


def per_wallet_forward_edge(cutoff_unix, fwd_end_unix, wallets):
    positions = data.load_oos_positions(DB, cutoff_unix, fwd_end_unix, frozenset(wallets))
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


def main():
    cutoffs = data.distinct_cutoffs(DB)
    print(f"=== Iter 4: trajectory features + GBM ===")
    print(f"=== {len(cutoffs)} cutoffs, {len(ALL_FEATURES)} features (base {len(BASE_FEATURE_COLS)} + traj {len(TRAJ_FEATURE_COLS)}) ===\n")

    train_data = {}
    for i, c in enumerate(cutoffs):
        fwd_end = c + FWD_SECS
        base = load_base_features(c)
        traj = load_trajectory_features(c, base['wallet_hex'].tolist())
        df = base.merge(traj, on='wallet_hex', how='left').fillna(0)
        labels = per_wallet_forward_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= MIN_FWD_POS else None
        )
        df['n_fwd_pos'] = df['wallet_hex'].map(lambda w: labels.get(w, (None, 0))[1])
        print(f"  [{i+1}] {datetime.utcfromtimestamp(c).date()}  "
              f"wallets={len(df):6d}  labeled={df['label'].notna().sum():6d}")
        train_data[c] = df

    # Walk-forward
    comp_cache = {}
    def comp_top(cc):
        if cc not in comp_cache:
            _, hexes = objective.invoke_composite(BIN, DB, cc, DEFAULT_WEIGHTS, top_n=TOP_N)
            comp_cache[cc] = hexes
        return comp_cache[cc]

    gbm_top = {}
    print("\n=== Walk-forward training & scoring ===")
    for i, anchor in enumerate(cutoffs):
        if i < 1:
            continue
        train_dfs = []
        for j in range(i):
            df = train_data[cutoffs[j]]
            train_dfs.append(df[df['label'].notna()].copy())
        tr = pd.concat(train_dfs, ignore_index=True)
        X_tr = tr[ALL_FEATURES].values.astype(np.float64)
        y_tr = tr['label'].values.astype(np.float64)

        te = train_data[anchor]
        X_te = te[ALL_FEATURES].values.astype(np.float64)

        m = lgb.LGBMRegressor(
            n_estimators=400, learning_rate=0.05, num_leaves=31,
            min_data_in_leaf=200, objective='regression',
            verbose=-1, n_jobs=-1, random_state=42,
        )
        m.fit(X_tr, y_tr)
        scores = m.predict(X_te)
        te = te.copy(); te['s'] = scores
        gbm_top[anchor] = te.nlargest(TOP_N, 's')['wallet_hex'].tolist()
        print(f"  anchor [{i+1}] {datetime.utcfromtimestamp(anchor).date()}  "
              f"train_n={len(tr):>7d}  test_n={len(te):>6d}")

    # Compare
    print()
    print("=== Comparison ===")
    print(f"{'anchor':12s}  {'comp_alone':>10s}  {'gbm_traj_alone':>14s}  "
          f"{'compK_e':>9s}  {'gbm_traj_K_e':>12s}")
    print("-" * 75)
    for K in [3, 5]:
        print(f"\n  K = {K}")
        ce_list, ge_list = [], []
        for i, anchor in enumerate(cutoffs):
            if i < K - 1:
                continue
            if anchor not in gbm_top:
                continue
            fwd_end = anchor + FWD_SECS
            window = cutoffs[i - K + 1: i + 1]
            ca, _ = per_pos_flat(anchor, fwd_end, comp_top(anchor))
            ga, _ = per_pos_flat(anchor, fwd_end, gbm_top[anchor])

            ci = set(comp_top(window[0]))
            for c in window[1:]:
                ci &= set(comp_top(c))
            ce, _ = per_pos_flat(anchor, fwd_end, list(ci))

            gi = set(gbm_top[window[0]]) if window[0] in gbm_top else set()
            if gi:
                for c in window[1:]:
                    if c in gbm_top:
                        gi &= set(gbm_top[c])
                    else:
                        gi = set(); break
            if gi:
                ge, _ = per_pos_flat(anchor, fwd_end, list(gi))
            else:
                ge = 0.0
            ce_list.append(ce); ge_list.append(ge)
            print(f"{datetime.utcfromtimestamp(anchor).date().isoformat():12s}  "
                  f"${ca:>+9.4f}  ${ga:>+13.4f}  "
                  f"${ce:>+8.4f}  ${ge:>+11.4f}")
        if ce_list:
            print(f"  CompK{K} mean=${mean(ce_list):+.4f}  GBMtrajK{K} mean=${mean(ge_list):+.4f}")

    out = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter4-gbm-traj-topn.json')
    out.write_text(json.dumps({str(c): v for c, v in gbm_top.items()}))
    print(f"\nWrote {out}")


if __name__ == '__main__':
    main()
