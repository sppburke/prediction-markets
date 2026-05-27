#!/usr/bin/env python3
"""Phase 3 diagnostics: #10 March postmortem, #11 cohort stability, #12 Sharpe/drawdown.

Runs against data/wallet_cache.db. Uses the same BHq pre-filter as gbm_walkforward.
"""
import sys
import sqlite3
import statistics
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner.data import distinct_cutoffs, load_oos_positions
from monthly_rerank_gbm import (
    gbm_rank_at, BHQ_Q_BPS,
    DEFAULT_MIN_FWD_POS,
)

DB = Path("data/wallet_cache.db")
TOP_N = 5000
MIN_TRADING_DAYS = 20
MIN_DISTINCT_EVENTS = 10
INTERSECTION_K = 3
FWD_14D = 14 * 24 * 3600
FWD_30D = 30 * 24 * 3600
FWD_7D  =  7 * 24 * 3600

# Cutoff unix → date string
def fmt(unix: int) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d")


def bhq_wallets(db: Path, cutoff: int, top_n: int = TOP_N) -> frozenset[str]:
    """BHq-significant wallets at this cutoff, same gate as gbm_walkforward."""
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        rows = conn.execute("""
            SELECT wallet_hex FROM wallet_features
            WHERE cutoff_unix = ?
              AND skill_pvalue_bps <= ?
              AND trading_days >= ?
              AND distinct_events >= ?
            ORDER BY skill_pvalue_bps ASC
            LIMIT ?
        """, (cutoff, BHQ_Q_BPS, MIN_TRADING_DAYS, MIN_DISTINCT_EVENTS, top_n * 4)).fetchall()
    return frozenset(r[0] for r in rows)


def gbm_cohort_at(db: Path, cutoff: int, all_cutoffs: list[int],
                  fwd_secs: int = FWD_14D, seed: int = 42) -> frozenset[str]:
    """GBM-ranked intersection_3 cohort at this anchor cutoff (mirrors gbm_walkforward logic)."""
    at_or_before = [c for c in all_cutoffs if c <= cutoff]
    if len(at_or_before) < INTERSECTION_K:
        return frozenset()
    scoring_cutoffs = at_or_before[-INTERSECTION_K:]
    rankings = {}
    for sc in scoring_cutoffs:
        train = [c for c in all_cutoffs if c < sc]
        if not train:
            return frozenset()
        rankings[sc] = gbm_rank_at(
            db, sc, fwd_secs, TOP_N,
            MIN_TRADING_DAYS, MIN_DISTINCT_EVENTS, DEFAULT_MIN_FWD_POS,
            train, use_bhq=True, label_type='perpos',
            n_seeds=1, random_state=seed,
        )
    result = set(rankings[scoring_cutoffs[0]])
    for sc in scoring_cutoffs[1:]:
        result &= set(rankings[sc])
    return frozenset(result)


# ─── #11 Cohort stability ────────────────────────────────────────────────────
print("\n" + "="*60)
print("DIAGNOSTIC #11: Cohort stability (BHq pool + GBM intersection_3)")
print("="*60)

cuts = distinct_cutoffs(DB)

# BHq pool stability
print("\n--- BHq pool Jaccard between consecutive cutoffs ---")
prev_pool = None
prev_cut = None
for c in cuts:
    pool = bhq_wallets(DB, c)
    if prev_pool is not None:
        inter = len(pool & prev_pool)
        union = len(pool | prev_pool)
        jaccard = inter / union if union > 0 else 0
        print(f"  {fmt(prev_cut)} → {fmt(c)}: |prev|={len(prev_pool)} |cur|={len(pool)} "
              f"intersection={inter} jaccard={jaccard:.3f}")
    prev_pool = pool
    prev_cut = c

# GBM intersection_3 cohort stability — only run for anchors that actually had results
print("\n--- GBM intersection_3 cohort Jaccard (seed=42, fwd=14d) ---")
# Only anchors from 2025-12-31 onward (need INTERSECTION_K=3 prior cutoffs)
anchor_cuts = [c for c in cuts if c >= 1767225599]
prev_cohort = None
prev_cut = None
for c in anchor_cuts:
    cohort = gbm_cohort_at(DB, c, cuts, fwd_secs=FWD_14D, seed=42)
    if not cohort:
        print(f"  {fmt(c)}: cohort empty (insufficient prior cutoffs)")
        prev_cohort = cohort
        prev_cut = c
        continue
    if prev_cohort is not None and prev_cohort:
        inter = len(cohort & prev_cohort)
        union = len(cohort | prev_cohort)
        jaccard = inter / union if union > 0 else 0
        print(f"  {fmt(prev_cut)} → {fmt(c)}: |prev|={len(prev_cohort)} |cur|={len(cohort)} "
              f"intersection={inter} jaccard={jaccard:.3f}")
    else:
        print(f"  {fmt(c)}: cohort size={len(cohort)} (first eligible anchor)")
    prev_cohort = cohort
    prev_cut = c


# ─── #10 March postmortem ────────────────────────────────────────────────────
print("\n" + "="*60)
print("DIAGNOSTIC #10: March 2026-03-02 anchor postmortem")
print("="*60)

march_cut = 1772495999   # 2026-03-02
jan_cut   = 1769903999   # 2026-01-31
mar31_cut = 1775001599   # 2026-03-31

# fwd windows: 7d=604800, 14d=1209600, 30d=2592000
fwd_windows = [(7, 604800), (14, 1209600), (30, 2592000)]

march_cohort = gbm_cohort_at(DB, march_cut, cuts, seed=42)
jan_cohort   = gbm_cohort_at(DB, jan_cut,   cuts, seed=42)
mar31_cohort = gbm_cohort_at(DB, mar31_cut, cuts, seed=42)

print(f"\nCohort sizes: Jan={len(jan_cohort)}, Mar02={len(march_cohort)}, Mar31={len(mar31_cohort)}")
print(f"March vs Jan overlap: {len(march_cohort & jan_cohort)} wallets in common")
print(f"March vs Mar31 overlap: {len(march_cohort & mar31_cohort)} wallets in common")

print("\n--- Forward edge by fwd window (March anchor) ---")
for fwd_label, fwd_secs in fwd_windows:
    fwd_end = march_cut + fwd_secs
    positions = load_oos_positions(DB, march_cut, fwd_end, march_cohort)
    if not positions:
        print(f"  fwd={fwd_label}d: no positions")
        continue
    edges = [(p.outcome - p.vwap_entry) / p.vwap_entry for p in positions]
    mean_e = statistics.mean(edges)
    std_e  = statistics.stdev(edges) if len(edges) > 1 else 0.0
    sharpe = mean_e / std_e if std_e > 0 else 0.0
    avg_price = statistics.mean(p.vwap_entry for p in positions)
    win_rate  = sum(1 for p in positions if p.outcome > 0.5) / len(positions)
    print(f"  fwd={fwd_label}d: n={len(edges)} mean_edge={mean_e:+.4f} std={std_e:.4f} "
          f"sharpe={sharpe:+.3f} avg_price={avg_price:.3f} win_rate={win_rate:.3f}")

# Compare Jan forward edge at same fwd windows
print("\n--- Forward edge by fwd window (Jan 2026-01-31 anchor, for comparison) ---")
for fwd_label, fwd_secs in fwd_windows:
    fwd_end = jan_cut + fwd_secs
    positions = load_oos_positions(DB, jan_cut, fwd_end, jan_cohort)
    if not positions:
        print(f"  fwd={fwd_label}d: no positions")
        continue
    edges = [(p.outcome - p.vwap_entry) / p.vwap_entry for p in positions]
    mean_e = statistics.mean(edges)
    std_e  = statistics.stdev(edges) if len(edges) > 1 else 0.0
    sharpe = mean_e / std_e if std_e > 0 else 0.0
    avg_price = statistics.mean(p.vwap_entry for p in positions)
    win_rate  = sum(1 for p in positions if p.outcome > 0.5) / len(positions)
    print(f"  fwd={fwd_label}d: n={len(edges)} mean_edge={mean_e:+.4f} std={std_e:.4f} "
          f"sharpe={sharpe:+.3f} avg_price={avg_price:.3f} win_rate={win_rate:.3f}")

# Market-regime check: vwap distribution at March vs Jan
print("\n--- March anchor: vwap price distribution in 30d forward window ---")
positions_m30 = load_oos_positions(DB, march_cut, march_cut + 2592000, march_cohort)
positions_j30 = load_oos_positions(DB, jan_cut,   jan_cut   + 2592000, jan_cohort)
if positions_m30 and positions_j30:
    def price_buckets(positions):
        buckets = {"<0.10": 0, "0.10-0.30": 0, "0.30-0.70": 0, "0.70-0.90": 0, ">0.90": 0}
        for p in positions:
            v = p.vwap_entry
            if v < 0.10:   buckets["<0.10"] += 1
            elif v < 0.30: buckets["0.10-0.30"] += 1
            elif v < 0.70: buckets["0.30-0.70"] += 1
            elif v < 0.90: buckets["0.70-0.90"] += 1
            else:          buckets[">0.90"] += 1
        n = len(positions)
        return {k: f"{v/n:.1%}" for k, v in buckets.items()}
    print(f"  March: {price_buckets(positions_m30)}  (n={len(positions_m30)})")
    print(f"  Jan:   {price_buckets(positions_j30)}  (n={len(positions_j30)})")


# ─── #12 Sharpe + max drawdown per anchor (from JSON) ───────────────────────
print("\n" + "="*60)
print("DIAGNOSTIC #12: Sharpe + cumulative edge by anchor across fwd windows")
print("="*60)
import json, glob
files = sorted(glob.glob("data/eval-results/202605*.json"))[-3:]
by_fwd = {}
for f in files:
    d = json.load(open(f))
    fwd = d["params"]["fwd_days"]
    by_fwd[fwd] = {a["anchor_date"]: a for a in d["per_anchor"]}

anchors_all = sorted(set(k for v in by_fwd.values() for k in v))
header = f"{'Anchor':<14}" + "".join(f"  fwd={w}d_edge  sharpe" for w in [7, 14, 30])
print("\n" + header)
for a in anchors_all:
    row = f"{a:<14}"
    for fwd in [7, 14, 30]:
        if fwd in by_fwd and a in by_fwd[fwd]:
            rec = by_fwd[fwd][a]
            row += f"  {rec['mean_edge']:+.4f}  {rec['sharpe']:+.3f}"
        else:
            row += "      n/a      n/a"
    print(row)

print("\n[phase3_diagnostics.py complete]")
