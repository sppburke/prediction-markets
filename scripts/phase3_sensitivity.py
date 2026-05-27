#!/usr/bin/env python3
"""Phase 3 #13: sensitivity sweep — min_trading_days and min_distinct_events.

Focus on the March 2026-03-02 anchor which is the only consistently negative one.
Tests whether gate changes rescue it, or whether it's a market-regime effect.
"""
import sys, statistics
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner.data import distinct_cutoffs, load_oos_positions
from monthly_rerank_gbm import gbm_rank_at, BHQ_Q_BPS, DEFAULT_MIN_FWD_POS

DB = Path("data/wallet_cache.db")
TOP_N = 5000
INTERSECTION_K = 3
FWD_14D = 14 * 24 * 3600
FWD_30D = 30 * 24 * 3600

cuts = distinct_cutoffs(DB)

MARCH  = 1772495999  # 2026-03-02 (the problematic anchor)
JAN    = 1769903999  # 2026-01-31 (baseline comparison)

def build_cohort(anchor, fwd_secs, min_td, min_de, seed=42):
    at_or_before = [c for c in cuts if c <= anchor]
    if len(at_or_before) < INTERSECTION_K:
        return frozenset()
    scoring_cutoffs = at_or_before[-INTERSECTION_K:]
    rankings = {}
    for sc in scoring_cutoffs:
        train = [c for c in cuts if c < sc]
        if not train:
            return frozenset()
        rankings[sc] = gbm_rank_at(
            DB, sc, fwd_secs, TOP_N,
            min_td, min_de, DEFAULT_MIN_FWD_POS,
            train, use_bhq=True, label_type='perpos',
            n_seeds=1, random_state=seed,
        )
    result = set(rankings[scoring_cutoffs[0]])
    for sc in scoring_cutoffs[1:]:
        result &= set(rankings[sc])
    return frozenset(result)

def eval_cohort(cohort, anchor, fwd_secs):
    if not cohort:
        return None, None, None, 0
    positions = load_oos_positions(DB, anchor, anchor + fwd_secs, cohort)
    if not positions:
        return None, None, None, 0
    edges = [(p.outcome - p.vwap_entry) / p.vwap_entry for p in positions]
    mean_e = statistics.mean(edges)
    std_e  = statistics.stdev(edges) if len(edges) > 1 else 0.0
    sharpe = mean_e / std_e if std_e > 0 else 0.0
    return mean_e, std_e, sharpe, len(edges)

print("Sensitivity sweep: March + January anchors at fwd=14d")
print(f"{'min_td':>6} {'min_de':>6} {'anchor':>12} {'n_cohort':>9} {'n_pos':>7} {'mean_edge':>10} {'sharpe':>8}")
print("-" * 65)

for min_td in [10, 15, 20, 25]:
    for min_de in [5, 10, 15]:
        for anchor, label in [(MARCH, "2026-03-02"), (JAN, "2026-01-31")]:
            cohort = build_cohort(anchor, FWD_14D, min_td, min_de)
            mean_e, std_e, sharpe, n_pos = eval_cohort(cohort, anchor, FWD_14D)
            if mean_e is None:
                print(f"{min_td:>6} {min_de:>6} {label:>12} {len(cohort):>9}       0        n/a      n/a")
            else:
                print(f"{min_td:>6} {min_de:>6} {label:>12} {len(cohort):>9} {n_pos:>7} {mean_e:>+10.4f} {sharpe:>+8.3f}")

print("\n[phase3_sensitivity.py complete]")
