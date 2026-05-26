#!/usr/bin/env python3
"""Iter 1: honest walk-forward of intersection_K + baseline at each anchor cutoff.

Tests whether the 74× headline at 2026-03-31 is anomalous or representative.

For each anchor cutoff t_i (i >= K) in the 8 available monthly cutoffs:
  - Build intersection_K cohort from {t_{i-K+1}, ..., t_i} using DEFAULT weights
  - Build single-cutoff baseline (top 5000 at t_i alone)
  - Measure per-position flat-$1 forward edge in the next 30 days
  - Report side-by-side

Output: per-anchor table + mean ± std summary for K ∈ {3, 5, 7}.
"""
import sys
import sqlite3
from pathlib import Path
from statistics import mean, stdev

sys.path.insert(0, '/home/sean/git/pm-ranker-iter2/scripts')
from composite_tuner import objective, data

DB = '/home/sean/git/prediction-markets/data/wallet_cache.db'
BIN = '/home/sean/git/pm-ranker-iter2/target/release/pe-skill-select'

DEFAULT = {
    "SHARPE_BPS": 1500, "EV_MEAN_BPS": 833, "EV_TSTAT_BPS": 833,
    "BB_SHRUNK_EDGE_BPS": 833, "KELLY_LOG_GROWTH_BPS": 833,
    "BRIER_SCORE_BPS": -833, "BRIER_RESOLUTION_BPS": 833,
    "CONCENTRATION_HHI_BPS": -500, "CONCENTRATION_N_EFF_BPS": 500,
    "CONCENTRATION_RPC_BPS": -500,
    "FIRST_ENTRIES_PER_ACTIVE_DAY_BPS": 1000,
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS": -1000,
}

TOP_N = 5000
FWD_DAYS = 30
FWD_SECS = FWD_DAYS * 86400


def get_cutoffs():
    with sqlite3.connect(f'file:{DB}?mode=ro', uri=True) as con:
        return [r[0] for r in con.execute(
            "SELECT DISTINCT cutoff_unix FROM wallet_features ORDER BY cutoff_unix")]


def rank_at(cutoff, weights, top_n=TOP_N):
    """Return list of top-N wallet_hex strings at cutoff using given weights."""
    _, hexes = objective.invoke_composite(BIN, DB, cutoff, weights, top_n=top_n)
    return hexes


def per_pos_flat(cutoff, fwd_end, hexes):
    """Per-position flat-$1 forward edge for given cohort."""
    pos = data.load_oos_positions(DB, cutoff, fwd_end, frozenset(hexes))
    if not pos:
        return 0.0, 0
    edge = sum((p.outcome - p.vwap_entry) / p.vwap_entry for p in pos) / len(pos)
    return edge, len(pos)


def main():
    cutoffs = get_cutoffs()
    print(f"=== Iter 1: walk-forward intersection_K vs baseline ===")
    print(f"=== {len(cutoffs)} cutoffs available ===")
    for i, c in enumerate(cutoffs):
        from datetime import datetime
        print(f"  [{i+1}] {c} = {datetime.utcfromtimestamp(c).date()}")
    print()

    # Cache rankings per (cutoff, top_n) so we only invoke composite once per cutoff.
    rank_cache = {}
    def rank_cached(cutoff):
        if cutoff not in rank_cache:
            rank_cache[cutoff] = rank_at(cutoff, DEFAULT, TOP_N)
        return rank_cache[cutoff]

    summary = {}
    for K in [3, 5, 7]:
        print(f"\n{'='*70}")
        print(f"  K = {K}")
        print(f"{'='*70}")
        print(f"{'anchor_date':12s}  {'base_n':>8s} {'base_edge':>10s}  "
              f"{'inter_n':>8s} {'inter_edge':>11s}  {'ratio':>8s}")
        print("-" * 70)

        base_edges = []
        inter_edges = []
        ratios = []

        for i in range(K - 1, len(cutoffs)):
            anchor = cutoffs[i]
            from datetime import datetime
            anchor_date = datetime.utcfromtimestamp(anchor).date().isoformat()
            fwd_end = anchor + FWD_SECS

            # Single-cutoff baseline
            base_hexes = rank_cached(anchor)
            base_edge, base_n = per_pos_flat(anchor, fwd_end, base_hexes)

            # Intersection_K cohort
            window = cutoffs[i - K + 1: i + 1]
            inter = set(rank_cached(window[0]))
            for c in window[1:]:
                inter &= set(rank_cached(c))
            inter_edge, inter_n = per_pos_flat(anchor, fwd_end, list(inter))

            ratio = inter_edge / base_edge if base_edge > 1e-9 else float('inf')

            print(f"{anchor_date:12s}  {base_n:>8d} ${base_edge:>+9.4f}  "
                  f"{inter_n:>8d} ${inter_edge:>+10.4f}  {ratio:>8.1f}x")

            base_edges.append(base_edge)
            inter_edges.append(inter_edge)
            if base_edge > 1e-9:
                ratios.append(ratio)

        print("-" * 70)
        if base_edges:
            print(f"  BASELINE  mean=${mean(base_edges):+.4f}  "
                  f"std=${stdev(base_edges) if len(base_edges) > 1 else 0:+.4f}")
            print(f"  INTER_K{K}  mean=${mean(inter_edges):+.4f}  "
                  f"std=${stdev(inter_edges) if len(inter_edges) > 1 else 0:+.4f}")
            if ratios:
                print(f"  RATIO     mean= {mean(ratios):.1f}x  "
                      f"std= {stdev(ratios) if len(ratios) > 1 else 0:.1f}x  "
                      f"min= {min(ratios):.1f}x  max= {max(ratios):.1f}x")

            summary[K] = {
                "base_mean": mean(base_edges),
                "inter_mean": mean(inter_edges),
                "n_anchors": len(base_edges),
                "ratio_mean": mean(ratios) if ratios else 0,
                "ratio_min": min(ratios) if ratios else 0,
                "ratio_max": max(ratios) if ratios else 0,
            }

    print(f"\n{'='*70}")
    print("  HONEST SUMMARY")
    print(f"{'='*70}")
    print(f"{'K':>4s}  {'anchors':>8s}  {'base_mean':>10s}  {'inter_mean':>11s}  "
          f"{'ratio_mean':>11s}  {'ratio_range':>15s}")
    for K, s in summary.items():
        print(f"{K:>4d}  {s['n_anchors']:>8d}  ${s['base_mean']:>+9.4f}  "
              f"${s['inter_mean']:>+10.4f}  {s['ratio_mean']:>10.1f}x  "
              f"{s['ratio_min']:>5.1f}-{s['ratio_max']:>5.1f}x")


if __name__ == '__main__':
    main()
