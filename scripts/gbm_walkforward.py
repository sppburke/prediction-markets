#!/usr/bin/env python3
"""Multi-anchor walk-forward GBM evaluation harness (tracker #248 sub-task #1).

Promotes the ad-hoc iter3b_gbm_bhq_ensemble.py logic into a canonical, parameterised
script. For each eligible anchor cutoff, builds the `gbm_bhq_intersection_3` cohort
(K=3 most-recent scoring cutoffs, each trained walk-forward on prior cutoffs, BHq
pre-filter on `skill_pvalue_bps`), then evaluates the cohort's forward edge over the
next `--fwd-days` via `composite_tuner.data.load_oos_positions`.

Output: per-anchor + aggregate + pbo JSON written to
`data/eval-results/<utc-ts>-<strategy>.json`.

Baseline metric (matches `crates/skill-select/src/forward.rs::ForwardReport.gross_of_fees == true`):
    flat-$1 edge = (outcome - vwap_entry) / vwap_entry, hold-to-resolution, gross of fees.
The optional fee+slippage haircut from tracker #248 sub-task #2 lands as a follow-up.

Output schema (schema_version=2):
    {
      "schema_version": 2,
      "strategy": "gbm_bhq_intersection_3",
      "generated_at_unix": <int>,
      "params": { ... },
      "per_anchor": [ ... ],
      "aggregate": { "n_anchors": N, "mean_of_mean_edge": ..., ... },
      "pbo": {
        "pbo": 0.312,            # fraction of IS/OOS half-splits where best-IS < OOS median
        "verdict": "OK",         # "OK" | "OVERFIT" | "undefined"
        "n_perms": 10,
        "n_trials": 5,           # n_seeds (one trial = one stochastic GBM seed)
        "n_windows": 5,          # n_anchors
        "median_oos_rank": 0.61
      }
    }
    verdict="OK" when pbo <= 0.5 AND n_seeds >= 2 AND n_anchors >= 4;
    verdict="undefined" otherwise (raw pbo still present for < 4 anchors).

USAGE:
    .venv-analysis/bin/python3 scripts/gbm_walkforward.py \\
        --db-path data/wallet_cache.db \\
        --n-anchors 5 --n-seeds 5

REUSE NOTE: imports `gbm_rank_at` and friends from `monthly_rerank_gbm.py` so any
future changes to GBM training stay in one place. The composite-ranker walk-forward
loop already lives in `scripts/composite_tuner/objective.py::evaluate_weights` — this
script is the GBM analogue, with intersection_K added.
"""
import argparse
import json
import math
import sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np

# Reuse the production GBM helpers (single source of truth for ranking logic).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from monthly_rerank_gbm import (  # noqa: E402
    gbm_rank_at,
    DEFAULT_TOP_N,
    DEFAULT_FWD_DAYS,
    DEFAULT_MIN_TRADING_DAYS,
    DEFAULT_MIN_DISTINCT_EVENTS,
    DEFAULT_MIN_FWD_POS,
    BHQ_Q_BPS,
)
from composite_tuner import data as data_mod  # noqa: E402
from composite_tuner.data import (  # noqa: E402
    POLYMARKET_FEE_RATE_BPS,
    SLIPPAGE_RATE_BPS,
)
from composite_tuner.pbo import compute_pbo, pbo_summary  # noqa: E402

SCHEMA_VERSION = 2
DEFAULT_OUTPUT_DIR = Path("data/eval-results")
MAX_DEFAULT_ANCHORS = 10  # safety cap when --n-anchors not specified
INTERSECTION_K = 3  # gbm_bhq_intersection_3


def eligible_anchors(cutoffs, fwd_secs, max_n=None):
    """Anchors with (a) at least K=3 cutoffs at-or-before for intersection, (b) at
    least one prior cutoff for GBM training of the earliest scoring cutoff, and
    (c) a fully-elapsed forward window (`anchor + fwd_secs <= max(cutoffs)`,
    which approximates "DB has resolution data covering the forward window").
    Returns most recent `max_n` (if specified) in ascending order.
    """
    if not cutoffs:
        return []
    latest = cutoffs[-1]
    eligible = []
    for a in cutoffs:
        if a + fwd_secs > latest:
            continue
        at_or_before = [c for c in cutoffs if c <= a]
        if len(at_or_before) < INTERSECTION_K:
            continue
        # Earliest scoring cutoff in the intersection needs at least one prior cutoff
        # whose forward window doesn't bleed into it.
        earliest_score = at_or_before[-INTERSECTION_K]
        priors = [c for c in cutoffs if c < earliest_score and c + fwd_secs <= earliest_score]
        if not priors:
            continue
        eligible.append(a)
    if max_n is not None and len(eligible) > max_n:
        eligible = eligible[-max_n:]
    return eligible


def evaluate_anchor(db, anchor, fwd_secs, all_cutoffs, top_n,
                    min_trading_days, min_distinct_events, min_fwd_pos,
                    price_haircut_bps=0, n_seeds=1, random_state=42):
    """Build gbm_bhq_intersection_3 cohort at `anchor` and measure forward edge.

    K=3 scoring cutoffs ending at the anchor; each ranks via GBM trained on its
    own prior cutoffs (with no forward-label bleed); intersection of the three
    top-N lists is the cohort. Forward edge is computed over `(anchor, anchor+fwd_secs]`.

    `price_haircut_bps` (default 0 = gross-of-fees, matching forward.rs) is
    passed through to `load_oos_positions` for the net-edge stress test.
    `n_seeds`/`random_state` are forwarded to `gbm_rank_at` for multi-seed ensembling.
    """
    at_or_before = [c for c in all_cutoffs if c <= anchor]
    score_cutoffs = at_or_before[-INTERSECTION_K:]

    rankings = {}
    for sc in score_cutoffs:
        train = [c for c in all_cutoffs if c < sc]
        rankings[sc] = gbm_rank_at(
            db, sc, fwd_secs, top_n,
            min_trading_days, min_distinct_events, min_fwd_pos,
            train, use_bhq=True, label_type='perpos',
            n_seeds=n_seeds, random_state=random_state,
        )

    cohort = set(rankings[score_cutoffs[0]])
    for sc in score_cutoffs[1:]:
        cohort &= set(rankings[sc])

    base = {
        'anchor_date': datetime.utcfromtimestamp(anchor).date().isoformat(),
        'anchor_unix': anchor,
        'n_cohort': len(cohort),
    }
    if not cohort:
        return {**base, 'n_positions': 0, 'mean_edge': 0.0, 'std_edge': 0.0,
                'sharpe': 0.0, 'flat_pnl': 0.0}

    positions = data_mod.load_oos_positions(
        db, anchor, anchor + fwd_secs, frozenset(cohort),
        price_haircut_bps=price_haircut_bps,
    )
    if not positions:
        return {**base, 'n_positions': 0, 'mean_edge': 0.0, 'std_edge': 0.0,
                'sharpe': 0.0, 'flat_pnl': 0.0}

    edges = np.array([(p.outcome - p.vwap_entry) / p.vwap_entry for p in positions])
    mean = float(edges.mean())
    std = float(edges.std(ddof=1)) if len(edges) > 1 else 0.0
    sharpe = mean / std if std > 0 else 0.0
    return {**base, 'n_positions': len(edges), 'mean_edge': mean,
            'std_edge': std, 'sharpe': sharpe, 'flat_pnl': float(edges.sum())}


def aggregate(rows):
    if not rows:
        return {'n_anchors': 0, 'mean_of_mean_edge': 0.0, 'std_of_mean_edge': 0.0,
                'total_flat_pnl': 0.0, 'n_anchors_negative': 0}
    means = np.array([r['mean_edge'] for r in rows])
    return {
        'n_anchors': len(rows),
        'mean_of_mean_edge': float(means.mean()),
        'std_of_mean_edge': float(means.std(ddof=1)) if len(means) > 1 else 0.0,
        'total_flat_pnl': float(sum(r['flat_pnl'] for r in rows)),
        'n_anchors_negative': int((means < 0).sum()),
    }


def pbo_result_to_dict(result, verdict: str) -> dict:
    """Serialise a PboResult to a JSON-safe dict (drops logit_values).

    verdict is passed in explicitly so callers can override it (e.g. force
    'undefined' on thin trial axis) without re-running compute_pbo.
    """
    return {
        'pbo': result.pbo,
        'verdict': verdict,
        'n_perms': result.n_perms,
        'n_trials': result.n_trials,
        'n_windows': result.n_windows,
        'median_oos_rank': result.median_oos_rank,
    }


def main():
    ap = argparse.ArgumentParser(
        description='Multi-anchor walk-forward GBM evaluation harness (v1: gbm_bhq_intersection_3 only).',
    )
    ap.add_argument('--db-path', required=True)
    ap.add_argument('--strategy', default='gbm_bhq_intersection_3',
                    choices=['gbm_bhq_intersection_3'],
                    help='v1 ships gbm_bhq_intersection_3 only (current production champion). '
                         'Other strategies in follow-ups.')
    ap.add_argument('--n-anchors', type=int, default=None,
                    help=f'Number of most recent eligible anchors to evaluate. '
                         f'Default: all eligible, capped at {MAX_DEFAULT_ANCHORS}.')
    ap.add_argument('--fwd-days', type=int, default=DEFAULT_FWD_DAYS)
    ap.add_argument('--top-n', type=int, default=DEFAULT_TOP_N)
    ap.add_argument('--min-trading-days', type=int, default=DEFAULT_MIN_TRADING_DAYS)
    ap.add_argument('--min-distinct-events', type=int, default=DEFAULT_MIN_DISTINCT_EVENTS)
    ap.add_argument('--min-fwd-pos', type=int, default=DEFAULT_MIN_FWD_POS)
    ap.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR)
    default_combined_haircut = POLYMARKET_FEE_RATE_BPS + SLIPPAGE_RATE_BPS
    ap.add_argument(
        '--price-haircut-bps', type=int, default=0,
        help=(
            f'Combined fee + slippage haircut (bps) applied as '
            f'vwap_entry *= (1 + bps/10000), clamped to 0.999. '
            f'Default 0 = gross-of-fees (matches '
            f'forward.rs::ForwardReport.gross_of_fees). '
            f'Pass {default_combined_haircut} to match production net edge per '
            f'evaluate.rs:95-112; see docs/_GLOSSARY.md '
            f'polymarket_fee_rate and slippage_rate for canonical defaults.'
        ),
    )
    ap.add_argument('--n-seeds', type=int, default=1,
                    help='Number of GBM seeds to ensemble (scores averaged before top-N). '
                         'Compute scales linearly with n_seeds. '
                         'Default 1 = single-seed (prior behaviour). Recommended production: 5.')
    ap.add_argument('--random-state', type=int, default=42,
                    help='Starting random seed. Seeds used: [random_state, ..., random_state+n_seeds-1].')
    ap.add_argument('--pbo-perms', type=int, default=16,
                    help='Upper bound on PBO permutation count. Effective n_perms = '
                         'min(C(n_anchors, n_anchors//2), --pbo-perms). At 4-5 anchors '
                         'the combinatorial cap is C(4,2)=6 or C(5,2)=10 regardless of '
                         'this flag. Ignored when n_seeds < 2 or n_anchors < 2.')
    args = ap.parse_args()

    if args.price_haircut_bps < 0:
        ap.error('--price-haircut-bps must be >= 0 (negative values do not model anything realistic)')
    if args.n_seeds < 1:
        ap.error('--n-seeds must be >= 1')

    fwd_secs = args.fwd_days * 86400

    all_cutoffs = data_mod.distinct_cutoffs(args.db_path)
    if len(all_cutoffs) < INTERSECTION_K + 1:
        print(f"ERROR: need >= {INTERSECTION_K + 1} cutoffs ({INTERSECTION_K} for intersection "
              f"+ 1 prior for training), got {len(all_cutoffs)}", file=sys.stderr)
        sys.exit(1)

    max_n = args.n_anchors if args.n_anchors is not None else MAX_DEFAULT_ANCHORS
    anchors = eligible_anchors(all_cutoffs, fwd_secs, max_n=max_n)
    if not anchors:
        print("ERROR: no eligible anchors. Need both fully-elapsed forward window "
              f"(anchor + {args.fwd_days}d) and >= {INTERSECTION_K} cutoffs at-or-before.",
              file=sys.stderr)
        sys.exit(1)

    print(f"strategy={args.strategy} top_n={args.top_n} fwd_days={args.fwd_days} "
          f"min_trading_days={args.min_trading_days} min_distinct_events={args.min_distinct_events}",
          file=sys.stderr)
    if args.n_seeds > 1:
        print(f"multi-seed ensemble: n_seeds={args.n_seeds} → ~{args.n_seeds}× compute per scoring cutoff",
              file=sys.stderr)
    print(f"evaluating {len(anchors)} anchors: "
          f"{[datetime.utcfromtimestamp(a).date().isoformat() for a in anchors]}",
          file=sys.stderr)

    rows = []
    for i, a in enumerate(anchors, 1):
        print(f"\n[{i}/{len(anchors)}] anchor={datetime.utcfromtimestamp(a).date().isoformat()}",
              file=sys.stderr)
        row = evaluate_anchor(
            args.db_path, a, fwd_secs, all_cutoffs, args.top_n,
            args.min_trading_days, args.min_distinct_events, args.min_fwd_pos,
            price_haircut_bps=args.price_haircut_bps,
            n_seeds=args.n_seeds, random_state=args.random_state,
        )
        print(f"  n_cohort={row['n_cohort']} n_pos={row['n_positions']} "
              f"mean_edge={row['mean_edge']:+.4f} std={row['std_edge']:.4f} "
              f"sharpe={row['sharpe']:+.3f} flat_pnl={row['flat_pnl']:+,.2f}",
              file=sys.stderr)
        rows.append(row)

    agg = aggregate(rows)

    # Build (n_seeds, n_anchors) score matrix for PBO. Each cell is the mean_edge
    # for that (seed, anchor) pair, produced by re-running evaluate_anchor with
    # n_seeds=1 for each seed. The main multi-seed loop (above) averages predictions
    # before cohort selection and exposes only one mean_edge per anchor, so we
    # cannot reuse those values — the per-seed PBO-data loop is separate.
    n_anchors = len(anchors)
    if args.n_seeds >= 2 and n_anchors >= 2:
        print(f"\nbuilding PBO score matrix ({args.n_seeds} seeds × {n_anchors} anchors) "
              f"— adds ~{args.n_seeds * n_anchors} single-seed evaluate_anchor calls",
              file=sys.stderr)
        pbo_matrix = np.zeros((args.n_seeds, n_anchors), dtype=float)
        for si, seed in enumerate(range(args.random_state,
                                        args.random_state + args.n_seeds)):
            for ai, anchor in enumerate(anchors):
                r = evaluate_anchor(
                    args.db_path, anchor, fwd_secs, all_cutoffs, args.top_n,
                    args.min_trading_days, args.min_distinct_events, args.min_fwd_pos,
                    price_haircut_bps=args.price_haircut_bps,
                    n_seeds=1, random_state=seed,
                )
                pbo_matrix[si, ai] = r['mean_edge']
        n_perms = min(math.comb(n_anchors, n_anchors // 2), args.pbo_perms)
        pbo_result = compute_pbo(pbo_matrix, n_perms=n_perms, rng_seed=42)
        # Plan-gate: raw pbo value preserved even at n_anchors < 4, but verdict
        # is forced to "undefined" — too thin a trial axis to commit to OK/OVERFIT.
        if n_anchors < 4 and not math.isnan(pbo_result.pbo):
            verdict = 'undefined'
            print(f"PBO={pbo_result.pbo:.3f} (verdict=undefined — need ≥4 anchors "
                  f"for a committed verdict; got n_anchors={n_anchors})", file=sys.stderr)
        else:
            verdict = 'undefined' if math.isnan(pbo_result.pbo) else (
                'OVERFIT' if pbo_result.pbo > 0.5 else 'OK'
            )
            print(pbo_summary(pbo_result), file=sys.stderr)
        pbo_dict = pbo_result_to_dict(pbo_result, verdict)
    else:
        pbo_dict = {
            'pbo': float('nan'),
            'verdict': 'undefined',
            'n_perms': 0,
            'n_trials': args.n_seeds,
            'n_windows': n_anchors,
            'median_oos_rank': float('nan'),
        }
        print(f"PBO=undefined — need ≥2 anchors and ≥2 seeds "
              f"(got n_anchors={n_anchors}, n_seeds={args.n_seeds})", file=sys.stderr)

    # Serialise NaN as null so the JSON is valid.
    pbo_json = {k: (None if isinstance(v, float) and math.isnan(v) else v)
                for k, v in pbo_dict.items()}

    out = {
        'schema_version': SCHEMA_VERSION,
        'strategy': args.strategy,
        'generated_at_unix': int(datetime.now(timezone.utc).timestamp()),
        'params': {
            'top_n': args.top_n,
            'fwd_days': args.fwd_days,
            'min_trading_days': args.min_trading_days,
            'min_distinct_events': args.min_distinct_events,
            'min_fwd_pos': args.min_fwd_pos,
            'intersection_k': INTERSECTION_K,
            'bhq_q_bps': BHQ_Q_BPS,
            'price_haircut_bps': args.price_haircut_bps,
            'pbo_perms': args.pbo_perms,
            'gbm': {
                'n_estimators': 400, 'learning_rate': 0.05, 'num_leaves': 31,
                'min_data_in_leaf': 200, 'random_state': args.random_state,
                'n_seeds': args.n_seeds,
                'seeds': list(range(args.random_state, args.random_state + args.n_seeds)),
            },
        },
        'per_anchor': rows,
        'aggregate': agg,
        'pbo': pbo_json,
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    utc_ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out_path = args.output_dir / f"{utc_ts}-{args.strategy}.json"
    out_path.write_text(json.dumps(out, indent=2) + '\n')

    print(f"\nwrote {out_path}", file=sys.stderr)
    print(f"  n_anchors={agg['n_anchors']} "
          f"mean_of_mean_edge={agg['mean_of_mean_edge']:+.4f} "
          f"std_of_mean_edge={agg['std_of_mean_edge']:.4f} "
          f"total_flat_pnl={agg['total_flat_pnl']:+,.2f} "
          f"n_anchors_negative={agg['n_anchors_negative']}",
          file=sys.stderr)


if __name__ == '__main__':
    main()
