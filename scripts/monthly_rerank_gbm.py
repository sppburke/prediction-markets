#!/usr/bin/env python3
"""Monthly production re-rank using the GBM ranker (per iter 2/3/3b/B2 findings).

Replaces scripts/monthly_rerank.py for the GBM workflow:
- Trains LightGBM on historical (features, forward-label) pairs from cutoffs
  whose full 30-day forward window has elapsed before the scoring cutoff.
- Scores wallets at each scoring cutoff.
- Optionally takes intersection_K across K most-recent scoring cutoffs.
- Writes a wallet hex list to --out.

Run after each `pe-skill-select extract` at a new monthly cutoff.

USAGE:
    .venv-analysis/bin/python3 scripts/monthly_rerank_gbm.py \
        --db-path data/wallet_cache.db \
        --strategy gbm_throughput_single \
        --out data/production-watchlist-gbm.txt

STRATEGIES (throughput label — B2 finding, +20% vs per-pos):
    gbm_throughput_single        GBM trained on n_pos×mean_edge, top-N single cutoff (RECOMMENDED)
    gbm_throughput_intersection_3 GBM throughput label, intersection across 3 most-recent cutoffs

STRATEGIES (per-pos edge label — batch-2 methods):
    gbm_single              GBM-top-N at most-recent cutoff only
    gbm_intersection_3      GBM-top-N intersection across 3 most-recent cutoffs (best per-pos flat edge)
    gbm_intersection_5      GBM-top-N intersection across 5 most-recent cutoffs (most selective)
    gbm_bhq_intersection_3  GBM-top-N within BHq pool, intersection_3 (best Sharpe)
    gbm_bhq_single          GBM-top-N within BHq pool, single cutoff

Training cutoffs are strictly filtered to c + fwd_secs <= scoring_cutoff so
no forward-window labels bleed into the evaluation period.

B2 result (7-anchor walk-forward, N=5k): throughput label mean $+10,232 vs per-pos $+8,554 (+20%).
"""
import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb

BHQ_Q_BPS = 1000  # q=0.10 — mirrors composite.rs bhq_gate default


def safe_workers(requested, n_items, gb_per_worker=3.0, reserve_gb=3.0):
    """Cap concurrency to what free RAM allows, to prevent OOM.

    Each parallel anchor holds a training DataFrame + LightGBM model(s); on a
    memory-constrained box (or when sharing the machine with another job) running
    the full requested fan-out can exhaust RAM and OOM-kill the process. Read
    MemAvailable from /proc/meminfo, reserve `reserve_gb`, and allow one worker
    per `gb_per_worker` of the remainder. Falls back to the item-bounded request
    if /proc/meminfo is unreadable (non-Linux). Always returns >= 1.
    """
    try:
        with open("/proc/meminfo") as f:
            avail_kb = next(int(l.split()[1]) for l in f if l.startswith("MemAvailable"))
        budget_gb = max(0.0, avail_kb / 1048576.0 - reserve_gb)
        by_mem = max(1, int(budget_gb // gb_per_worker))
    except Exception:
        by_mem = requested
    return max(1, min(requested, n_items, by_mem))


def bhq_significant_mask(skill_pvalue_bps_array, q_bps=BHQ_Q_BPS):
    """Benjamini-Hochberg FDR gate; returns boolean mask of BHq-significant rows.
    Mirrors composite.rs:bhq_gate and iter2b implementation.
    """
    m = len(skill_pvalue_bps_array)
    if m == 0:
        return np.array([], dtype=bool)
    order = np.argsort(skill_pvalue_bps_array, kind='stable')
    sorted_p = skill_pvalue_bps_array[order]
    k_star = 0
    for rank0, p in enumerate(sorted_p):
        if p * m <= (rank0 + 1) * q_bps:
            k_star = rank0 + 1
    mask = np.zeros(m, dtype=bool)
    if k_star > 0:
        mask[order[:k_star]] = True
    return mask

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner import data as data_mod

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
    "longshot_bias_ratio_bps",
    "hold_to_resolution_rate_bps",
    "position_sizing_cv_bps",
    "first_mover_percentile_bps",
]
DEFAULT_TOP_N = 5000
DEFAULT_FWD_DAYS = 30
DEFAULT_MIN_TRADING_DAYS = 20
DEFAULT_MIN_DISTINCT_EVENTS = 10
DEFAULT_MIN_FWD_POS = 3


def load_features(db, cutoff_unix, min_trading_days, min_distinct_events):
    sql = """
        SELECT wallet_hex, reconstruction_quality, closed_trades, distinct_markets,
               distinct_events, total_pnl_usd_str, roi_bps, win_rate_bps,
               avg_hold_secs, trading_days, mean_daily_return_bps,
               std_daily_return_bps, sharpe_bps, skewness_bps, excess_kurtosis_bps,
               lcb_5pct_bps, skill_pnl_usd_str, skill_pvalue_bps, skill_permutations,
               ev_mean_bps, ev_tstat_bps, bb_shrunk_edge_bps, kelly_log_growth_bps,
               brier_score_bps, brier_resolution_bps,
               concentration_hhi_bps, concentration_n_eff_bps, concentration_rpc_bps,
               first_entries_per_active_day_bps, median_first_entry_to_resolution_secs,
               longshot_bias_ratio_bps, hold_to_resolution_rate_bps,
               position_sizing_cv_bps, first_mover_percentile_bps
        FROM wallet_features
        WHERE cutoff_unix = ?
          AND trading_days >= ?
          AND distinct_events >= ?
    """
    with sqlite3.connect(f'file:{db}?mode=ro', uri=True) as con:
        df = pd.read_sql_query(sql, con, params=(cutoff_unix, min_trading_days, min_distinct_events))
    df['total_pnl_usd'] = pd.to_numeric(df['total_pnl_usd_str'], errors='coerce').fillna(0)
    df['skill_pnl_usd'] = pd.to_numeric(df['skill_pnl_usd_str'], errors='coerce').fillna(0)
    df.drop(columns=['total_pnl_usd_str', 'skill_pnl_usd_str'], inplace=True)
    return df


def per_wallet_fwd_edge(db, cutoff_unix, fwd_end_unix, wallets):
    positions = data_mod.load_oos_positions(db, cutoff_unix, fwd_end_unix, frozenset(wallets))
    by = {}
    for p in positions:
        e = (p.outcome - p.vwap_entry) / p.vwap_entry
        if p.wallet_hex not in by:
            by[p.wallet_hex] = [0.0, 0]
        by[p.wallet_hex][0] += e
        by[p.wallet_hex][1] += 1
    return {w: (s / n, n) for w, (s, n) in by.items()}


def _gbm_train_and_score(db, cutoff_unix, fwd_secs, min_trading_days,
                         min_distinct_events, min_fwd_pos, safe_train,
                         use_bhq=False, label_type='perpos',
                         n_seeds=1, random_state=42):
    """Train GBM on safe_train and return the scored test DataFrame.

    safe_train must already satisfy c + fwd_secs <= cutoff_unix (caller's
    responsibility).  Returns the test DataFrame with column 's' (mean score
    across seeds) in the same row order as load_features(cutoff_unix).
    """
    print(f"  training on {len(safe_train)} prior cutoffs (label={label_type})", file=sys.stderr)
    train_dfs = []
    for c in safe_train:
        df = load_features(db, c, min_trading_days, min_distinct_events)
        labels = per_wallet_fwd_edge(db, c, c + fwd_secs, df['wallet_hex'].tolist())
        if label_type == 'throughput':
            df['label'] = df['wallet_hex'].map(
                lambda w: labels[w][0] * labels[w][1]
                if w in labels and labels[w][1] >= min_fwd_pos else None
            )
        else:
            df['label'] = df['wallet_hex'].map(
                lambda w: labels.get(w, (None, 0))[0] if labels.get(w, (None, 0))[1] >= min_fwd_pos else None
            )
        train_dfs.append(df[df['label'].notna()].copy())

    tr = pd.concat(train_dfs, ignore_index=True)
    print(f"  train_rows={len(tr)}", file=sys.stderr)
    X = tr[FEATURE_COLS].values.astype(np.float64)
    y = tr['label'].values.astype(np.float64)

    test = load_features(db, cutoff_unix, min_trading_days, min_distinct_events)
    if use_bhq:
        mask = bhq_significant_mask(test['skill_pvalue_bps'].values)
        test = test[mask].copy()
        print(f"  BHq pool: {len(test)} wallets", file=sys.stderr)

    X_test = test[FEATURE_COLS].values.astype(np.float64)

    seeds = list(range(random_state, random_state + n_seeds))
    if n_seeds > 1:
        print(f"  multi-seed: n_seeds={n_seeds} seeds={seeds}", file=sys.stderr)
    per_seed_preds = []
    for seed in seeds:
        m = lgb.LGBMRegressor(
            n_estimators=400, learning_rate=0.05, num_leaves=31,
            min_data_in_leaf=200, objective='regression',
            verbose=-1, n_jobs=-1, random_state=seed,
        )
        m.fit(X, y)
        per_seed_preds.append(m.predict(X_test))
    test['s'] = np.mean(per_seed_preds, axis=0)
    return test


def gbm_rank_at(db, cutoff_unix, fwd_secs, top_n, min_trading_days,
                min_distinct_events, min_fwd_pos, train_cutoffs, use_bhq=False,
                label_type='perpos', *, n_seeds: int = 1, random_state: int = 42):
    """Train GBM on train_cutoffs (labeled by fwd edge), score at cutoff_unix.

    train_cutoffs must satisfy c + fwd_secs <= cutoff_unix (no forward-label bleed).
    Returns top-N wallet_hex list; if use_bhq, only BHq-significant wallets are scored.
    label_type: 'perpos' = mean edge per position (batch-2 default);
                'throughput' = n_positions × mean_edge (B2 finding, +20% throughput).
    n_seeds: number of GBM models to train with seeds [random_state, ..., random_state+n_seeds-1];
             predictions are averaged before top-N selection. n_seeds=1 (default) reproduces
             prior single-seed behaviour exactly. Compute scales linearly with n_seeds.
    """
    # Guard: drop any training cutoff whose forward window extends past the scoring cutoff.
    safe_train = [c for c in train_cutoffs if c + fwd_secs <= cutoff_unix]
    if len(safe_train) < len(train_cutoffs):
        dropped = len(train_cutoffs) - len(safe_train)
        print(f"  WARN: dropped {dropped} training cutoff(s) whose fwd window bleeds past {datetime.utcfromtimestamp(cutoff_unix).date()}", file=sys.stderr)
    if not safe_train:
        raise ValueError(f"No safe training cutoffs available for scoring cutoff {cutoff_unix}")

    test = _gbm_train_and_score(
        db, cutoff_unix, fwd_secs, min_trading_days, min_distinct_events,
        min_fwd_pos, safe_train, use_bhq=use_bhq, label_type=label_type,
        n_seeds=n_seeds, random_state=random_state,
    )
    effective_top_n = min(top_n, len(test))
    return test.nlargest(effective_top_n, 's')['wallet_hex'].tolist()


def gbm_scores_at(db, cutoff_unix, fwd_secs, min_trading_days,
                  min_distinct_events, min_fwd_pos, train_cutoffs,
                  use_bhq=False, label_type='perpos',
                  *, n_seeds: int = 1, random_state: int = 42):
    """Train GBM and return per-wallet scores as a dict at cutoff_unix.

    Identical training path to gbm_rank_at; returns all scored wallets (not
    just top-N) so the caller can apply its own selection logic.

    Returns:
        dict[wallet_hex -> float]  (mean score across seeds; higher = better)
    """
    safe_train = [c for c in train_cutoffs if c + fwd_secs <= cutoff_unix]
    if len(safe_train) < len(train_cutoffs):
        dropped = len(train_cutoffs) - len(safe_train)
        print(f"  WARN: dropped {dropped} training cutoff(s) whose fwd window bleeds past {datetime.utcfromtimestamp(cutoff_unix).date()}", file=sys.stderr)
    if not safe_train:
        raise ValueError(f"No safe training cutoffs available for scoring cutoff {cutoff_unix}")

    test = _gbm_train_and_score(
        db, cutoff_unix, fwd_secs, min_trading_days, min_distinct_events,
        min_fwd_pos, safe_train, use_bhq=use_bhq, label_type=label_type,
        n_seeds=n_seeds, random_state=random_state,
    )
    return dict(zip(test['wallet_hex'], test['s']))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db-path', required=True)
    ap.add_argument('--strategy', default='gbm_throughput_single',
                    choices=['gbm_throughput_single', 'gbm_throughput_intersection_3',
                             'gbm_single', 'gbm_intersection_3', 'gbm_intersection_5',
                             'gbm_bhq_intersection_3', 'gbm_bhq_single'])
    ap.add_argument('--top-n', type=int, default=DEFAULT_TOP_N)
    ap.add_argument('--fwd-days', type=int, default=DEFAULT_FWD_DAYS)
    ap.add_argument('--min-trading-days', type=int, default=DEFAULT_MIN_TRADING_DAYS)
    ap.add_argument('--min-distinct-events', type=int, default=DEFAULT_MIN_DISTINCT_EVENTS)
    ap.add_argument('--min-fwd-pos', type=int, default=DEFAULT_MIN_FWD_POS)
    ap.add_argument('--n-seeds', type=int, default=1,
                    help='Number of GBM seeds to ensemble (scores averaged). '
                         'Compute scales linearly. Default 1 = single-seed (prior behaviour).')
    ap.add_argument('--random-state', type=int, default=42,
                    help='Starting random seed. Seeds used: [random_state, ..., random_state+n_seeds-1].')
    ap.add_argument('--out', required=True)
    args = ap.parse_args()

    if args.n_seeds < 1:
        ap.error('--n-seeds must be >= 1')

    fwd_secs = args.fwd_days * 86400

    cutoffs = data_mod.distinct_cutoffs(args.db_path)
    if len(cutoffs) < 2:
        print(f"ERROR: need >=2 cutoffs, got {len(cutoffs)}", file=sys.stderr)
        sys.exit(1)

    use_bhq = args.strategy.startswith('gbm_bhq')
    label_type = 'throughput' if args.strategy.startswith('gbm_throughput') else 'perpos'
    K = {
        'gbm_throughput_single': 1,
        'gbm_throughput_intersection_3': 3,
        'gbm_single': 1,
        'gbm_intersection_3': 3,
        'gbm_intersection_5': 5,
        'gbm_bhq_single': 1,
        'gbm_bhq_intersection_3': 3,
    }[args.strategy]
    if K > len(cutoffs):
        print(f"WARN: requested K={K} but only {len(cutoffs)} cutoffs available", file=sys.stderr)
        K = len(cutoffs)

    score_cutoffs = cutoffs[-K:]
    print(f"strategy={args.strategy} K={K}", file=sys.stderr)
    print(f"scoring cutoffs: {[datetime.utcfromtimestamp(c).date().isoformat() for c in score_cutoffs]}", file=sys.stderr)

    rankings = {}
    for sc in score_cutoffs:
        train = [c for c in cutoffs if c < sc]
        if not train:
            print(f"ERROR: no training data before {sc}", file=sys.stderr)
            sys.exit(1)
        print(f"\nRanking at {datetime.utcfromtimestamp(sc).date().isoformat()}", file=sys.stderr)
        rankings[sc] = gbm_rank_at(args.db_path, sc, fwd_secs, args.top_n,
                                    args.min_trading_days, args.min_distinct_events,
                                    args.min_fwd_pos, train, use_bhq=use_bhq,
                                    label_type=label_type, n_seeds=args.n_seeds,
                                    random_state=args.random_state)

    if K == 1:
        out_set = list(rankings[score_cutoffs[0]])
    else:
        out_set = set(rankings[score_cutoffs[0]])
        for sc in score_cutoffs[1:]:
            out_set &= set(rankings[sc])
        out_set = list(out_set)
        if not out_set:
            print(f"ERROR: intersection of top-{args.top_n} across {K} cutoffs is empty — "
                  f"rankings are too unstable or K is too large for the available data.", file=sys.stderr)
            sys.exit(1)

    print(f"\nselected n={len(out_set)} wallets", file=sys.stderr)
    Path(args.out).write_text('\n'.join(out_set) + '\n')
    print(f"wrote {args.out}", file=sys.stderr)


if __name__ == '__main__':
    main()
