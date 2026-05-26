#!/usr/bin/env python3
"""Production monthly re-rank pipeline (autonomous-iteration goal hand-off).

Generates the production wallet watchlist by:
1. Listing the N most-recent cutoffs in wallet_features.
2. Running `pe-skill-select composite` at each (any reasonable weights;
   default is fine — see project_temporal_ensemble_breakthrough.md memory
   for the weight-invariance evidence).
3. Computing the INTERSECTION (or majority, or weighted-recency vote) of
   the top-K rankings → production cohort.
4. Writing the cohort hex-set to a versioned artifact.

The temporal-ensemble approach gives 2-200× better per-position forward
edge than any single-cutoff ranking, and is weight-invariant. The
monthly cadence keeps the cohort fresh as new wallets emerge or old
ones drift.

Usage:
    .venv-analysis/bin/python3 scripts/monthly_rerank.py \\
        --db-path data/wallet_cache.db \\
        --strategy intersection_7 \\
        --out data/production-watchlist.txt
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner import objective  # noqa: E402

# Default weights mirror crates/skill-select/src/composite.rs::CompositeWeights::default().
# Empirically weight-invariant at the temporal-ensemble level (see memory) but
# `informed_combo` gives slightly higher baseline edge if not ensembling.
DEFAULT_WEIGHTS = {
    "SHARPE_BPS": 1500, "EV_MEAN_BPS": 833, "EV_TSTAT_BPS": 833,
    "BB_SHRUNK_EDGE_BPS": 833, "KELLY_LOG_GROWTH_BPS": 833,
    "BRIER_SCORE_BPS": -833, "BRIER_RESOLUTION_BPS": 833,
    "CONCENTRATION_HHI_BPS": -500, "CONCENTRATION_N_EFF_BPS": 500,
    "CONCENTRATION_RPC_BPS": -500,
    "FIRST_ENTRIES_PER_ACTIVE_DAY_BPS": 1000,
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS": -1000,
}

INFORMED_WEIGHTS = {n: 0 for n in objective.WEIGHT_NAMES} | {
    "EV_MEAN_BPS": 1000, "EV_TSTAT_BPS": 500,
    "BB_SHRUNK_EDGE_BPS": 1000, "KELLY_LOG_GROWTH_BPS": 1000,
    "BRIER_RESOLUTION_BPS": 500,
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS": -500,
}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", required=True)
    p.add_argument("--binary", default="./target/release/pe-skill-select")
    p.add_argument(
        "--strategy",
        default="intersection_3",
        choices=[
            "intersection_2", "intersection_3", "intersection_4", "intersection_5",
            "intersection_7", "majority_3", "majority_5", "weighted_recency_top5000",
            "single_latest",
        ],
        help="Temporal-ensemble strategy. See temporal_ensemble_breakthrough memory "
             "for evidence and per-strategy quality/coverage trade-offs.",
    )
    p.add_argument(
        "--weights",
        default="default",
        choices=["default", "informed"],
        help="Per-cutoff composite weights. Empirically weight-invariant at "
             "ensemble level — default is fine.",
    )
    p.add_argument("--top-n", type=int, default=5000, help="per-cutoff top-N before ensembling")
    p.add_argument("--out", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    weights = DEFAULT_WEIGHTS if args.weights == "default" else INFORMED_WEIGHTS

    with sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True) as conn:
        cutoffs = sorted(
            r[0] for r in conn.execute(
                "SELECT DISTINCT cutoff_unix FROM wallet_features ORDER BY cutoff_unix"
            )
        )
    if not cutoffs:
        print("ERROR: no cutoffs in wallet_features", file=sys.stderr)
        return 1

    # Determine how many cutoffs we need for the chosen strategy.
    strategy_K = {
        "intersection_2": 2, "intersection_3": 3, "intersection_4": 4,
        "intersection_5": 5, "intersection_7": 7,
        "majority_3": 3, "majority_5": 5,
        "weighted_recency_top5000": len(cutoffs),  # use all available
        "single_latest": 1,
    }[args.strategy]
    if len(cutoffs) < strategy_K:
        print(
            f"WARNING: strategy {args.strategy} needs {strategy_K} cutoffs but only "
            f"{len(cutoffs)} present. Falling back to single_latest.",
            file=sys.stderr,
        )
        args.strategy = "single_latest"
        strategy_K = 1

    recent = cutoffs[-strategy_K:]
    print(f"Strategy={args.strategy}; using {len(recent)} cutoffs:", file=sys.stderr)
    for c in recent:
        d = datetime.fromtimestamp(c, tz=timezone.utc).strftime("%Y-%m-%d")
        print(f"  {d} (unix={c})", file=sys.stderr)

    # Rank at each.
    rankings = {}
    for c in recent:
        n, hexes = objective.invoke_composite(
            args.binary, args.db_path, c, weights, top_n=args.top_n
        )
        d = datetime.fromtimestamp(c, tz=timezone.utc).strftime("%Y-%m-%d")
        print(f"  {d}: n_selected={n}", file=sys.stderr)
        rankings[c] = hexes

    # Combine per strategy.
    if args.strategy == "single_latest":
        cohort = rankings[recent[-1]]
    elif args.strategy.startswith("intersection_"):
        cohort = set(rankings[recent[0]])
        for c in recent[1:]:
            cohort &= set(rankings[c])
        cohort = frozenset(cohort)
    elif args.strategy.startswith("majority_"):
        K = int(args.strategy.split("_")[1])
        threshold = (K // 2) + 1
        vote = Counter()
        for c in recent:
            for h in rankings[c]:
                vote[h] += 1
        cohort = frozenset(h for h, v in vote.items() if v >= threshold)
    elif args.strategy == "weighted_recency_top5000":
        weights_by_age = {c: (i + 1) for i, c in enumerate(recent)}
        score = Counter()
        for c in recent:
            w = weights_by_age[c]
            for h in rankings[c]:
                score[h] += w
        cohort = frozenset(h for h, _ in score.most_common(args.top_n))
    else:
        raise ValueError(f"unhandled strategy: {args.strategy}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cohort_sorted = sorted(cohort)
    out_path.write_text(
        f"# Production watchlist — {args.strategy} ({args.weights} weights)\n"
        f"# Generated {datetime.now(timezone.utc).isoformat()}; n_wallets={len(cohort_sorted)}\n"
        f"# Cutoffs used: {[datetime.fromtimestamp(c, tz=timezone.utc).strftime('%Y-%m-%d') for c in recent]}\n"
        + "\n".join(cohort_sorted) + "\n"
    )
    print(f"Wrote {len(cohort_sorted)} wallets → {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
