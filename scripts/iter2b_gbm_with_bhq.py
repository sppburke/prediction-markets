#!/usr/bin/env python3
"""Iter 2b: GBM ranker applied to the SAME BHq-significant pool that composite uses.

iter 2 found GBM underperforms composite on per-position edge, mostly because
composite's BHq pre-filter is doing strong wallet selection. iter 2b applies
BHq pre-filter to the GBM cohort, so both methods rank within the same
gated pool.

If GBM wins here: the ranking function matters within BHq.
If GBM ties: BHq + ANY reasonable scoring is good enough.
If GBM loses: composite's specific feature weights matter within BHq.
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
BHQ_Q_BPS = 1000  # 0.10 — same as composite default


def bhq_significant_mask(skill_pvalue_bps_array, q_bps=BHQ_Q_BPS):
    """Benjamini-Hochberg FDR gate; returns boolean mask of significant entries.
    Mirrors composite.rs:bhq_gate.
    """
    m = len(skill_pvalue_bps_array)
    if m == 0:
        return np.array([], dtype=bool)
    order = np.argsort(skill_pvalue_bps_array, kind='stable')
    sorted_p = skill_pvalue_bps_array[order]
    k_star = 0
    for rank0, p in enumerate(sorted_p):
        # p ≤ (k/m)·q ⇔ p·m ≤ k·q
        if p * m <= (rank0 + 1) * q_bps:
            k_star = rank0 + 1
    mask = np.zeros(m, dtype=bool)
    if k_star > 0:
        mask[order[:k_star]] = True
    return mask


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
    df['bhq_sig'] = bhq_significant_mask(df['skill_pvalue_bps'].values)
    return df


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
    print(f"=== Iter 2b: GBM + BHq pre-filter ===\n")

    train_data = {}
    for i, c in enumerate(cutoffs):
        fwd_end = c + FWD_SECS
        df = load_features(c)
        labels = per_wallet_forward_edge(c, fwd_end, df['wallet_hex'].tolist())
        df['label'] = df['wallet_hex'].map(
            lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= MIN_FWD_POS else None
        )
        bhq_n = df['bhq_sig'].sum()
        print(f"  [{i+1}] {datetime.utcfromtimestamp(c).date()}  "
              f"total={len(df):6d}  bhq_sig={bhq_n:6d}  labeled={df['label'].notna().sum():6d}")
        train_data[c] = df

    comp_cache = {}
    def comp_top(cc):
        if cc not in comp_cache:
            _, hexes = objective.invoke_composite(BIN, DB, cc, DEFAULT_WEIGHTS, top_n=TOP_N)
            comp_cache[cc] = hexes
        return comp_cache[cc]

    gbm_bhq_top = {}
    print("\n=== Walk-forward (BHq-significant only) ===")
    for i, anchor in enumerate(cutoffs):
        if i < 1:
            continue
        train_dfs = []
        for j in range(i):
            df = train_data[cutoffs[j]]
            # Train on BHq-significant + labeled wallets only
            train_dfs.append(df[df['bhq_sig'] & df['label'].notna()].copy())
        tr = pd.concat(train_dfs, ignore_index=True)
        if len(tr) < 20:
            print(f"  anchor [{i+1}] {datetime.utcfromtimestamp(anchor).date()} — only {len(tr)} train rows, skipping")
            continue
        X_tr = tr[FEATURE_COLS].values.astype(np.float64)
        y_tr = tr['label'].values.astype(np.float64)

        te = train_data[anchor]
        te_bhq = te[te['bhq_sig']].copy()
        if len(te_bhq) == 0:
            continue
        X_te = te_bhq[FEATURE_COLS].values.astype(np.float64)

        m = lgb.LGBMRegressor(
            n_estimators=400, learning_rate=0.05, num_leaves=31,
            min_data_in_leaf=max(20, len(tr) // 100),
            objective='regression', verbose=-1, n_jobs=-1, random_state=42,
        )
        m.fit(X_tr, y_tr)
        scores = m.predict(X_te)
        te_bhq['s'] = scores
        gbm_bhq_top[anchor] = te_bhq.nlargest(min(TOP_N, len(te_bhq)), 's')['wallet_hex'].tolist()
        print(f"  anchor [{i+1}] {datetime.utcfromtimestamp(anchor).date()}  "
              f"bhq_test={len(te_bhq):>6d}  train_n={len(tr):>6d}  "
              f"gbm_selected={len(gbm_bhq_top[anchor]):>5d}")

    print()
    print("=== Forward-edge comparison (composite vs GBM-within-BHq) ===")
    print(f"{'anchor':12s}  {'comp_n':>7s}  {'comp_$':>9s}  "
          f"{'gbm_n':>7s}  {'gbm_$':>9s}  {'gbm/comp':>9s}")
    comp_es, gbm_es = [], []
    for i, anchor in enumerate(cutoffs):
        if i < 1 or anchor not in gbm_bhq_top:
            continue
        fwd_end = anchor + FWD_SECS
        ce, cn = per_pos_flat(anchor, fwd_end, comp_top(anchor))
        ge, gn = per_pos_flat(anchor, fwd_end, gbm_bhq_top[anchor])
        r = ge/ce if abs(ce) > 1e-9 else float('inf')
        print(f"{datetime.utcfromtimestamp(anchor).date().isoformat():12s}  "
              f"{cn:>7d}  ${ce:>+8.4f}  {gn:>7d}  ${ge:>+8.4f}  {r:>8.2f}x")
        comp_es.append(ce); gbm_es.append(ge)
    if comp_es:
        print(f"\n  COMP mean=${mean(comp_es):+.4f}  std=${stdev(comp_es) if len(comp_es) > 1 else 0:+.4f}")
        print(f"  GBM  mean=${mean(gbm_es):+.4f}  std=${stdev(gbm_es) if len(gbm_es) > 1 else 0:+.4f}")
        wins = sum(1 for g, c in zip(gbm_es, comp_es) if g > c)
        print(f"  GBM wins {wins}/{len(comp_es)} anchors")

    out = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2b-gbm-bhq-topn.json')
    out.write_text(json.dumps({str(c): v for c, v in gbm_bhq_top.items()}))
    print(f"\nWrote {out}")


if __name__ == '__main__':
    main()
