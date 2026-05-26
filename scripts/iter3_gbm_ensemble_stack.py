#!/usr/bin/env python3
"""Iter 3: stack temporal ensembling on top of the GBM ranker (iter 2 output).

For each anchor cutoff t_i (i >= K):
  cohort = intersection of GBM-top-N at {t_{i-K+1}, ..., t_i}
  forward edge in [t_i, t_i + 30d]

Compares to:
  - GBM-top-N at t_i alone (iter 2 single-cutoff)
  - Composite-top-N at t_i alone (baseline)
  - Composite-top-N intersection_K (iter 1)

The question: does GBM + intersection_K beat composite + intersection_K?
If yes, the production answer changes; if no, the composite-default stays.
"""
import sys, sqlite3, json
from pathlib import Path
from statistics import mean, stdev
from datetime import datetime

sys.path.insert(0, '/home/sean/git/pm-ranker-iter2/scripts')
from composite_tuner import objective, data

DB = '/home/sean/git/prediction-markets/data/wallet_cache.db'
BIN = '/home/sean/git/pm-ranker-iter2/target/release/pe-skill-select'

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
GBM_JSON = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2-gbm-topn.json')


def per_pos_flat(cutoff, fwd_end, hexes):
    pos = data.load_oos_positions(DB, cutoff, fwd_end, frozenset(hexes))
    if not pos:
        return 0.0, 0
    e = sum((p.outcome - p.vwap_entry) / p.vwap_entry for p in pos) / len(pos)
    return e, len(pos)


def main():
    if not GBM_JSON.exists():
        print(f"ERROR: GBM ranks not found at {GBM_JSON}. Run iter2 first.")
        sys.exit(1)
    gbm_top = {int(k): v for k, v in json.loads(GBM_JSON.read_text()).items()}
    cutoffs = sorted(gbm_top.keys())

    # Composite ranks (default weights)
    comp_cache = {}
    def composite_top(c):
        if c not in comp_cache:
            _, hexes = objective.invoke_composite(BIN, DB, c, DEFAULT_WEIGHTS, top_n=TOP_N)
            comp_cache[c] = hexes
        return comp_cache[c]

    print(f"=== Iter 3: GBM + temporal ensemble vs composite + temporal ensemble ===")
    print(f"=== {len(cutoffs)} cutoffs with GBM rankings ===\n")

    results = {}
    for K in [3, 5, 7]:
        print(f"{'='*85}")
        print(f"  K = {K}")
        print(f"{'='*85}")
        print(f"{'anchor':12s}  {'comp_alone':>10s}  {'gbm_alone':>10s}  "
              f"{'compK_n':>7s} {'compK_edge':>11s}  "
              f"{'gbmK_n':>6s} {'gbmK_edge':>11s}  "
              f"{'gbmK/compK':>11s}")
        print("-" * 85)
        comp_K, gbm_K = [], []
        comp_alones, gbm_alones = [], []
        for i, anchor in enumerate(cutoffs):
            if i < K - 1:
                continue
            window = cutoffs[i - K + 1: i + 1]
            fwd_end = anchor + FWD_SECS

            # single-cutoff comparisons
            ca, _ = per_pos_flat(anchor, fwd_end, composite_top(anchor))
            ga, _ = per_pos_flat(anchor, fwd_end, gbm_top[anchor])

            # intersection_K
            comp_inter = set(composite_top(window[0]))
            for c in window[1:]:
                comp_inter &= set(composite_top(c))
            ce, cn = per_pos_flat(anchor, fwd_end, list(comp_inter))

            gbm_inter = set(gbm_top[window[0]])
            for c in window[1:]:
                gbm_inter &= set(gbm_top[c])
            ge, gn = per_pos_flat(anchor, fwd_end, list(gbm_inter))

            ratio = ge / ce if abs(ce) > 1e-9 else float('inf')
            print(f"{datetime.utcfromtimestamp(anchor).date().isoformat():12s}  "
                  f"${ca:>+9.4f}  ${ga:>+9.4f}  "
                  f"{cn:>7d} ${ce:>+10.4f}  "
                  f"{gn:>6d} ${ge:>+10.4f}  {ratio:>10.2f}x")
            comp_K.append(ce); gbm_K.append(ge)
            comp_alones.append(ca); gbm_alones.append(ga)

        if comp_K:
            print("-" * 85)
            print(f"  comp_alone mean=${mean(comp_alones):+.4f}  "
                  f"gbm_alone mean=${mean(gbm_alones):+.4f}")
            print(f"  COMPOSITE_K{K}  mean=${mean(comp_K):+.4f}  "
                  f"std=${stdev(comp_K) if len(comp_K) > 1 else 0:.4f}")
            print(f"  GBM_K{K}        mean=${mean(gbm_K):+.4f}  "
                  f"std=${stdev(gbm_K) if len(gbm_K) > 1 else 0:.4f}")
            results[K] = {
                "comp_alone_mean": mean(comp_alones),
                "gbm_alone_mean": mean(gbm_alones),
                "comp_K_mean": mean(comp_K),
                "gbm_K_mean": mean(gbm_K),
                "n_anchors": len(comp_K),
            }

    print(f"\n{'='*85}")
    print("  ITER 3 VERDICT")
    print(f"{'='*85}")
    for K, r in results.items():
        print(f"  K={K}  comp_K=${r['comp_K_mean']:+.4f}  gbm_K=${r['gbm_K_mean']:+.4f}  "
              f"=> {'GBM' if r['gbm_K_mean'] > r['comp_K_mean'] else 'COMPOSITE'} wins by "
              f"${abs(r['gbm_K_mean']-r['comp_K_mean']):.4f}")


if __name__ == '__main__':
    main()
