"""Forward-bench composition from a FROZEN bake-off return matrix (item 3.4 of the
2026-07-01 decision record on issue #417; registered in docs/32).

Consumes an already-produced ``return_matrix.csv`` (no new backtests — the certification
program is terminated; this is bench-COMPOSITION, never certification) and applies the
pre-registered selection rule:

1. Zero-fill the panel (post-A2 semantics: a no-signal period is an economic $0; older
   matrices produced before #475 carry NaN for those periods, which this zero-fill maps to
   the same meaning).
2. For every challenger column, the paired per-period t vs the baseline column.
3. ``true_clv``-ranked arms are INELIGIBLE for bench slots (amendment A3: the stale-tick
   artifact ranks wallets on a corrupt signal until the tick-ordering fix has minute-fidelity
   data behind it).
4. Slots are FIXED by family — no argmax across the whole grid (amendment A5: run24's own
   uncertainty block put 614/960 configs in the top-1 confidence set; a global argmax is a
   selection accident):
     - 1 slot: the max-paired-t ``policy_hybrid_displacement`` arm (low per-period variance
       -> fastest forward convergence),
     - 1 slot: the max-paired-t ``policy_online_weighting`` arm (preserves the registered
       family bet; capped at one slot because its per-period sd is ~20x the hybrids',
       maximizing time-to-signal),
     - plus the incumbent production ranker as control (not selected here; it is the live
       instance).

The FINAL selection among the benched arms is the forward gate (docs/32), never this script.
"""
import argparse
import sys

import numpy as np
import pandas as pd

BENCH_FAMILIES = ("policy_hybrid_displacement", "policy_online_weighting")
INELIGIBLE_ESTIMATORS = ("true_clv",)


def paired_t_vs_baseline(matrix: pd.DataFrame, baseline_key: str) -> pd.DataFrame:
    """Per-config paired per-period t vs the baseline on the zero-filled panel.

    Returns a frame indexed by config with ``paired_t``, ``periods_positive``, ``cum_return``
    and ``period_sd`` (the config's own per-period sd, NaN-aware before zero-fill so a
    no-signal period does not fake stability), sorted by ``paired_t`` descending.
    """
    if baseline_key not in matrix.columns:
        raise ValueError(f"baseline column {baseline_key!r} not in matrix")
    z = matrix.fillna(0.0)
    out = []
    for cfg in matrix.columns:
        if cfg == baseline_key:
            continue
        diff = z[cfg] - z[baseline_key]
        sd = float(diff.std(ddof=1))
        n = len(diff)
        t = float(diff.mean() / (sd / np.sqrt(n))) if sd > 0 else float("nan")
        out.append({
            "config": cfg,
            "paired_t": t,
            "periods_positive": int((diff > 0).sum()),
            "n_periods": n,
            "cum_return": float(z[cfg].sum()),
            "period_sd": float(matrix[cfg].std(ddof=1)),
        })
    return (pd.DataFrame(out).set_index("config")
            .sort_values("paired_t", ascending=False))


def eligible(config: str) -> bool:
    """Bench-slot eligibility: not ranked by an ineligible (artifact) estimator."""
    estimator = config.split("|", 1)[0]
    return estimator not in INELIGIBLE_ESTIMATORS


def compose_bench(matrix: pd.DataFrame, baseline_key: str) -> dict:
    """Apply the registered rule: per family, the max-paired-t eligible arm."""
    table = paired_t_vs_baseline(matrix, baseline_key)
    bench = {}
    for family in BENCH_FAMILIES:
        arms = table[[family in c and eligible(c) for c in table.index]]
        if arms.empty:
            bench[family] = None
            continue
        top = arms.index[0]
        bench[family] = {"config": top, **{k: arms.loc[top, k] for k in arms.columns}}
    return {"bench": bench, "table": table}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("matrix_csv", help="frozen return_matrix.csv (index=period, cols=configs)")
    ap.add_argument("--baseline", required=True, help="baseline column key")
    args = ap.parse_args()
    m = pd.read_csv(args.matrix_csv, index_col=0)
    result = compose_bench(m, args.baseline)
    print(result["table"].to_string(max_colwidth=90))
    print()
    for family, arm in result["bench"].items():
        if arm is None:
            print(f"{family}: NO ELIGIBLE ARM")
        else:
            print(f"BENCH [{family}]: {arm['config']}")
            print(f"  paired_t={arm['paired_t']:.2f}  positive={arm['periods_positive']}/"
                  f"{arm['n_periods']}  cum={arm['cum_return']:.0f}  sd={arm['period_sd']:.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
