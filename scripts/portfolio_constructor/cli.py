"""CLI entry point for Stage-2 portfolio construction.

Run with .venv-analysis/bin/python3 scripts/portfolio_constructor/cli.py --help
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow `python3 scripts/portfolio_constructor/cli.py` from repo root.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_SCRIPTS_DIR))

from portfolio_constructor import data as _data  # noqa: E402
from portfolio_constructor.constructor import PortfolioConfig, run_constructor  # noqa: E402

DEFAULT_OUTPUT_DIR = Path('data/eval-results')
DEFAULT_MIN_CREDIBLE_ANCHORS = 4   # portfolio_min_credible_anchors


def _ts():
    return datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def main():
    ap = argparse.ArgumentParser(
        description='Stage-2 greedy portfolio constructor: max-edge / min-overlap '
                    'wallet selection with walk-forward + PBO validation.',
    )
    ap.add_argument('--db-path', required=True, help='Path to wallet_cache.db')
    ap.add_argument('--watchlist', required=True,
                    help='Required .txt path of wallet hexes for deploy-cutoff filter '
                         '(e.g. data/watchlist-*-gbm_bhq_intersection_3.txt). '
                         'Applied only at the deploy cutoff, never to historical anchors.')
    ap.add_argument('--starting-capital', type=float, default=1000.0,
                    dest='bankroll_usd', help='Starting bankroll in USD (default 1000)')
    ap.add_argument('--min-position', type=float, default=5.0,
                    dest='min_position_usd', help='Min position USD floor (default 5)')
    ap.add_argument('--max-n', type=int, default=50,
                    help='Max wallets in the greedy portfolio (default 50)')
    ap.add_argument('--overlap-lambda', type=float, default=1.0,
                    help='Overlap penalty weight lambda (default 1.0; 0 = pure top-N)')
    ap.add_argument('--haircut-bps', type=int, default=500,
                    help='Fee+slippage haircut in bps for net-edge stress (default 500)')
    ap.add_argument('--fwd-days', type=int, default=7,
                    help='Forward evaluation window in days (default 7; use 14 or 30 '
                         'for higher-alpha but fewer eligible anchors)')
    ap.add_argument('--lookback-days', type=int, default=90,
                    help='Trailing overlap + ex-ante Kelly window in days (default 90)')
    ap.add_argument('--use-bhq', action='store_true', default=True,
                    help='Apply BHq gate to GBM candidate universe (default True)')
    ap.add_argument('--no-bhq', action='store_false', dest='use_bhq')
    ap.add_argument('--label-type', default='perpos',
                    choices=['perpos', 'throughput'],
                    help='GBM training label (default perpos)')
    ap.add_argument('--n-seeds', type=int, default=5,
                    help='GBM multi-seed ensemble size (default 5)')
    ap.add_argument('--n-anchors', type=int, default=None,
                    help='Max eligible anchors to evaluate (default all eligible)')
    ap.add_argument('--pbo-perms', type=int, default=100,
                    help='Max PBO permutations (default 100)')
    ap.add_argument('--sizing-mode', default='fractional',
                    choices=['fractional', 'flat'],
                    help='Kelly sizing mode (default fractional)')
    ap.add_argument('--kelly-fraction', type=float, default=0.25,
                    help='Kelly fraction multiplier (default 0.25; '
                         'distinct from Winner-Follow Kelly in docs/19-)')
    ap.add_argument('--top-n', type=int, default=5000,
                    help='GBM top-N candidate pool (default 5000)')
    ap.add_argument('--min-trading-days', type=int, default=20)
    ap.add_argument('--min-distinct-events', type=int, default=10)
    ap.add_argument('--min-fwd-pos', type=int, default=3)
    ap.add_argument('--min-edge-score', type=float, default=0.0,
                    help='Greedy objective threshold (default 0.0)')
    ap.add_argument('--max-candidates', type=int, default=None,
                    dest='max_candidates',
                    help='Cap the BHq pool to top-K before loading market sets '
                         '(default max-n * 4; None = full pool, original behaviour). '
                         'Major speedup when BHq pool is large (>500 wallets).')
    ap.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR)
    ap.add_argument('--label', default='portfolio_greedy',
                    help='Strategy label for output filenames')
    args = ap.parse_args()

    cfg = PortfolioConfig(
        db=args.db_path,
        fwd_secs=args.fwd_days * 86400,
        top_n=args.top_n,
        min_trading_days=args.min_trading_days,
        min_distinct_events=args.min_distinct_events,
        min_fwd_pos=args.min_fwd_pos,
        overlap_lambda=args.overlap_lambda,
        max_n=args.max_n,
        min_edge_score=args.min_edge_score,
        lookback_secs=args.lookback_days * 86400,
        haircut_bps=args.haircut_bps,
        sizing_mode=args.sizing_mode,
        kelly_fraction=args.kelly_fraction,
        min_position_usd=args.min_position_usd,
        bankroll_usd=args.bankroll_usd,
        use_bhq=args.use_bhq,
        label_type=args.label_type,
        n_seeds=args.n_seeds,
        random_state=42,
        max_anchors=args.n_anchors,
        pbo_perms=args.pbo_perms,
        watchlist_path=args.watchlist,
        max_candidates=args.max_candidates,
    )

    result = run_constructor(cfg)

    n_anchors = result.walk_forward['aggregate']['n_eligible_anchors']
    if n_anchors < DEFAULT_MIN_CREDIBLE_ANCHORS:
        print(
            f"WARNING: n_eligible_anchors={n_anchors} < portfolio_min_credible_anchors="
            f"{DEFAULT_MIN_CREDIBLE_ANCHORS}. Verdict is 'insufficient evidence'; "
            "deploy set flagged for informational use only.",
            file=sys.stderr,
        )
        result.walk_forward['aggregate']['credible'] = False
        result.walk_forward['aggregate']['credible_note'] = (
            f'n_eligible_anchors={n_anchors} < portfolio_min_credible_anchors='
            f'{DEFAULT_MIN_CREDIBLE_ANCHORS}'
        )
    else:
        result.walk_forward['aggregate']['credible'] = True

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ts = _ts()
    json_path = args.output_dir / f'{ts}-{args.label}.json'
    watchlist_path = args.output_dir / f'watchlist-{ts}-{args.label}.txt'

    with open(json_path, 'w') as f:
        json.dump(result.walk_forward, f, indent=2, default=str)
    print(f"Eval JSON: {json_path}")

    with open(watchlist_path, 'w') as f:
        f.write(f'# portfolio_constructor greedy deploy set\n')
        f.write(f'# deploy_cutoff_unix={result.deploy_cutoff_unix}\n')
        f.write(f'# n_wallets={len(result.deploy_wallets)}\n')
        for w in result.deploy_wallets:
            f.write(f'{w}\n')
    print(f"Deploy watchlist: {watchlist_path} ({len(result.deploy_wallets)} wallets)")

    agg = result.walk_forward['aggregate']
    print(
        f"\nWalk-forward summary:\n"
        f"  n_eligible_anchors:   {agg['n_eligible_anchors']}\n"
        f"  mean_of_mean_edge:    {agg['mean_of_mean_edge']:+.4f}\n"
        f"  std_of_mean_edge:     {agg['std_of_mean_edge']:.4f}\n"
        f"  n_anchors_negative:   {agg['n_anchors_negative']}\n"
        f"  PBO verdict:          {result.walk_forward['pbo']['verdict']}\n"
        f"  credible:             {agg['credible']}"
    )


if __name__ == '__main__':
    main()
