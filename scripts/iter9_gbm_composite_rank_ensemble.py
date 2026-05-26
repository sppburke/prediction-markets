#!/usr/bin/env python3
"""Iter 9: rank-averaged ensemble of GBM and composite.

Both rankers produce a per-cutoff top-5000 (with full ranking implicit).
Test if averaging their RANK positions gives a better cohort than either alone.

Method:
  For each cutoff:
    - Get composite_top_5000 (with ranks 1-5000)
    - Get gbm_top_5000 (with ranks 1-5000)
    - Compute combined_rank[wallet] = avg(composite_rank, gbm_rank) — UNRANKED wallets get rank 5001 (penalty)
    - Take top-5000 by combined_rank

Hypothesis: rank ensembling smooths out per-anchor variance.
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
    gbm = {int(k): v for k, v in json.loads(
        Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2-gbm-topn.json').read_text()
    ).items()}
    cutoffs = sorted(gbm.keys())

    comp = {}
    for c in cutoffs:
        _, hexes = objective.invoke_composite(BIN, DB, c, DEFAULT_WEIGHTS, top_n=TOP_N)
        comp[c] = hexes
        print(f"  composite ranked at {datetime.utcfromtimestamp(c).date()}: {len(hexes)} wallets")

    # Build rank ensemble per cutoff
    rank_ensemble = {}
    for c in cutoffs:
        c_rank = {w: i for i, w in enumerate(comp[c])}
        g_rank = {w: i for i, w in enumerate(gbm[c])}
        all_w = set(c_rank.keys()) | set(g_rank.keys())
        # Wallets in one but not the other get rank N (max for that ranker)
        N_c = len(c_rank)
        N_g = len(g_rank)
        combined = []
        for w in all_w:
            cr = c_rank.get(w, N_c)  # missing = bottom
            gr = g_rank.get(w, N_g)
            combined.append((w, (cr + gr) / 2))
        combined.sort(key=lambda x: x[1])
        rank_ensemble[c] = [w for w, _ in combined[:TOP_N]]

    print()
    print("=== Iter 9: rank ensemble (composite ∪ gbm, avg-rank top-N) ===")
    print(f"{'anchor':12s}  {'comp_n':>7s} {'comp_$':>9s}  {'gbm_n':>7s} {'gbm_$':>9s}  "
          f"{'ens_n':>6s} {'ens_$':>9s}")
    print("-" * 78)
    ce_l, ge_l, ee_l = [], [], []
    for c in cutoffs:
        fwd_end = c + FWD_SECS
        ce, cn = per_pos_flat(c, fwd_end, comp[c])
        ge, gn = per_pos_flat(c, fwd_end, gbm[c])
        ee, en = per_pos_flat(c, fwd_end, rank_ensemble[c])
        print(f"{datetime.utcfromtimestamp(c).date().isoformat():12s}  "
              f"{cn:>7d} ${ce:>+8.4f}  {gn:>7d} ${ge:>+8.4f}  {en:>6d} ${ee:>+8.4f}")
        ce_l.append(ce); ge_l.append(ge); ee_l.append(ee)
    print("-" * 78)
    print(f"  comp mean=${mean(ce_l):+.4f}  std=${stdev(ce_l) if len(ce_l) > 1 else 0:+.4f}")
    print(f"  gbm  mean=${mean(ge_l):+.4f}  std=${stdev(ge_l) if len(ge_l) > 1 else 0:+.4f}")
    print(f"  ENSEMBLE  mean=${mean(ee_l):+.4f}  std=${stdev(ee_l) if len(ee_l) > 1 else 0:+.4f}")

    # Also test ensemble + intersection_K
    print()
    print("=== Ensemble + intersection_K ===")
    for K in [3, 5]:
        eK = []
        for i, anchor in enumerate(cutoffs):
            if i < K - 1:
                continue
            fwd_end = anchor + FWD_SECS
            window = cutoffs[i - K + 1: i + 1]
            inter = set(rank_ensemble[window[0]])
            for c in window[1:]:
                inter &= set(rank_ensemble[c])
            ee, en = per_pos_flat(anchor, fwd_end, list(inter))
            eK.append(ee)
            print(f"  K={K} anchor={datetime.utcfromtimestamp(anchor).date().isoformat()}  "
                  f"n={en}  edge=${ee:+.4f}")
        if eK:
            print(f"  K={K} mean=${mean(eK):+.4f}  std=${stdev(eK) if len(eK) > 1 else 0:+.4f}")

    out = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter9-rank-ensemble-topn.json')
    out.write_text(json.dumps({str(c): v for c, v in rank_ensemble.items()}))
    print(f"\nWrote {out}")


if __name__ == '__main__':
    main()
