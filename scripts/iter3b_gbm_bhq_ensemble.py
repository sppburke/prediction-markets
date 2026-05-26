#!/usr/bin/env python3
"""Iter 3b: GBM-within-BHq (iter 2b cohort) + temporal ensembling.

Compares:
  - GBM-within-BHq single (iter 2b: $+0.054 mean)
  - GBM-within-BHq intersection_K
  - composite intersection_K (iter 3 baseline)

The best result from iter 3 was GBM-without-BHq + intersection_3 = $+0.097.
This iteration tests whether the BHq pre-filter helps or hurts the ensemble.
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


def per_pos_flat(cutoff, fwd_end, hexes):
    pos = data.load_oos_positions(DB, cutoff, fwd_end, frozenset(hexes))
    if not pos:
        return 0.0, 0
    e = sum((p.outcome - p.vwap_entry) / p.vwap_entry for p in pos) / len(pos)
    return e, len(pos)


def main():
    gbm_no_bhq = {int(k): v for k, v in json.loads(
        Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2-gbm-topn.json').read_text()
    ).items()}
    gbm_bhq = {int(k): v for k, v in json.loads(
        Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2b-gbm-bhq-topn.json').read_text()
    ).items()}
    cutoffs = sorted(set(gbm_no_bhq.keys()) & set(gbm_bhq.keys()))
    print(f"=== Iter 3b: GBM-within-BHq + ensemble vs GBM-no-BHq + ensemble ===")
    print(f"=== {len(cutoffs)} common cutoffs ===\n")

    comp_cache = {}
    def comp_top(c):
        if c not in comp_cache:
            _, hexes = objective.invoke_composite(BIN, DB, c, DEFAULT_WEIGHTS, top_n=TOP_N)
            comp_cache[c] = hexes
        return comp_cache[c]

    for K in [3, 5]:
        print(f"\n=== K={K} ===")
        print(f"{'anchor':12s}  {'comp_K':>9s}  {'gbm_K_noBHq':>12s}  {'gbm_K_BHq':>10s}")
        cK, gNK, gBK = [], [], []
        for i, anchor in enumerate(cutoffs):
            if i < K - 1:
                continue
            window = cutoffs[i - K + 1: i + 1]
            fwd_end = anchor + FWD_SECS

            ci = set(comp_top(window[0]))
            for c in window[1:]:
                ci &= set(comp_top(c))
            ce, _ = per_pos_flat(anchor, fwd_end, list(ci))

            gni = set(gbm_no_bhq[window[0]])
            for c in window[1:]:
                gni &= set(gbm_no_bhq[c])
            gne, _ = per_pos_flat(anchor, fwd_end, list(gni))

            gbi = set(gbm_bhq[window[0]])
            for c in window[1:]:
                gbi &= set(gbm_bhq[c])
            gbe, _ = per_pos_flat(anchor, fwd_end, list(gbi))

            print(f"{datetime.utcfromtimestamp(anchor).date().isoformat():12s}  "
                  f"${ce:>+8.4f}  ${gne:>+11.4f}  ${gbe:>+9.4f}")
            cK.append(ce); gNK.append(gne); gBK.append(gbe)
        if cK:
            print(f"  comp_K mean=${mean(cK):+.4f}  std=${stdev(cK) if len(cK) > 1 else 0:+.4f}")
            print(f"  gbm_noBHq_K mean=${mean(gNK):+.4f}  std=${stdev(gNK) if len(gNK) > 1 else 0:+.4f}")
            print(f"  gbm_BHq_K mean=${mean(gBK):+.4f}  std=${stdev(gBK) if len(gBK) > 1 else 0:+.4f}")


if __name__ == '__main__':
    main()
