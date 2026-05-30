"""Walk-forward validation for Stage-2 portfolio construction.

numpy/local — requires .venv-analysis.  Not CI-tested.
"""
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gbm_walkforward import eligible_anchors, aggregate, pbo_result_to_dict  # noqa: E402
from composite_tuner.pbo import compute_pbo  # noqa: E402
from . import data as _data  # noqa: E402
from . import edge as _edge  # noqa: E402
from .overlap import marginal_overlap  # noqa: E402
from .selector import GreedySelector  # noqa: E402
from .sizing import SizingConfig, size_portfolio  # noqa: E402

# Copied from gbm_walkforward.py to avoid coupling to its internals.
_COMMITTED_VERDICT_N_SEEDS = 2
_COMMITTED_VERDICT_N_ANCHORS = 4


@dataclass
class AnchorResult:
    anchor_date: str
    anchor_unix: int
    n_cohort: int
    n_positions: int
    mean_edge: float
    std_edge: float
    sharpe: float
    flat_pnl: float
    # sizing results (gross-of-haircut and net)
    sizing_gross: object   # SizingResult or None
    sizing_net: object     # SizingResult or None


def evaluate_anchor_portfolio(
    db, anchor, fwd_secs, all_cutoffs,
    top_n, min_trading_days, min_distinct_events, min_fwd_pos,
    overlap_lambda, max_n, min_edge_score,
    lookback_secs, haircut_bps, sizing_cfg_gross, sizing_cfg_net,
    use_bhq=True, label_type='perpos', n_seeds=5, random_state=42,
):
    """Build a greedy portfolio at `anchor` and measure forward edge.

    Universe = gbm_scores_at(anchor) — as-of-cutoff candidate set.
    Overlap = trailing (anchor - lookback_secs, anchor] Jaccard market sets.
    Forward positions = (anchor, anchor + fwd_secs].
    Ex-ante Kelly = prior-window returns from (anchor - lookback_secs, anchor].
    """
    train_cutoffs = [c for c in all_cutoffs if c < anchor]
    scores = _edge.gbm_edge_scores(
        db, anchor, fwd_secs, min_trading_days, min_distinct_events,
        min_fwd_pos, train_cutoffs, use_bhq=use_bhq, label_type=label_type,
        n_seeds=n_seeds, random_state=random_state,
    )

    base = {
        'anchor_date': datetime.fromtimestamp(anchor, tz=timezone.utc).date().isoformat(),
        'anchor_unix': anchor,
    }

    if not scores:
        return AnchorResult(**base, n_cohort=0, n_positions=0,
                            mean_edge=0.0, std_edge=0.0, sharpe=0.0, flat_pnl=0.0,
                            sizing_gross=None, sizing_net=None)

    market_sets = _data.load_wallet_market_sets(db, anchor, list(scores.keys()), lookback_secs)
    selector = GreedySelector(overlap_fn=marginal_overlap, overlap_lambda=overlap_lambda)
    result = selector.select(scores, market_sets, max_n=max_n, min_edge_score=min_edge_score)

    cohort = frozenset(result.wallets)
    n_cohort = len(cohort)

    positions = _data.load_oos_positions(
        db, anchor, anchor + fwd_secs, cohort, price_haircut_bps=0,
    )
    if not positions:
        return AnchorResult(**base, n_cohort=n_cohort, n_positions=0,
                            mean_edge=0.0, std_edge=0.0, sharpe=0.0, flat_pnl=0.0,
                            sizing_gross=None, sizing_net=None)

    edges = np.array([(p.outcome - p.vwap_entry) / p.vwap_entry for p in positions])
    mean = float(edges.mean())
    std = float(edges.std(ddof=1)) if len(edges) > 1 else 0.0
    sharpe = mean / std if std > 0 else 0.0

    # Ex-ante returns: prior window positions for the same cohort.
    exante_pos = _data.load_oos_positions(
        db, anchor - lookback_secs, anchor, cohort, price_haircut_bps=0,
    )
    exante_returns = [(p.outcome - p.vwap_entry) / p.vwap_entry for p in exante_pos]
    fwd_returns = edges.tolist()

    net_positions = _data.load_oos_positions(
        db, anchor, anchor + fwd_secs, cohort, price_haircut_bps=haircut_bps,
    )
    net_returns = [(p.outcome - p.vwap_entry) / p.vwap_entry for p in net_positions]

    sizing_gross = size_portfolio(fwd_returns, exante_returns, sizing_cfg_gross)
    sizing_net = size_portfolio(net_returns, exante_returns, sizing_cfg_net)

    return AnchorResult(
        **base,
        n_cohort=n_cohort,
        n_positions=len(edges),
        mean_edge=mean,
        std_edge=std,
        sharpe=sharpe,
        flat_pnl=float(edges.sum()),
        sizing_gross=sizing_gross,
        sizing_net=sizing_net,
    )


def run_walk_forward(
    db, fwd_secs, all_cutoffs, top_n, min_trading_days, min_distinct_events,
    min_fwd_pos, overlap_lambda, max_n, min_edge_score, lookback_secs,
    haircut_bps, sizing_mode, kelly_fraction, min_position_usd, bankroll_usd,
    use_bhq=True, label_type='perpos', n_seeds=5, random_state=42,
    max_anchors=None,
):
    """Evaluate the greedy portfolio across all eligible anchors, compute PBO.

    Returns a dict matching the gbm_walkforward schema_version=2 shape, with
    an extra 'n_eligible_anchors' field and per-anchor sizing results.
    """
    anchors = eligible_anchors(all_cutoffs, fwd_secs, max_n=max_anchors)
    n_eligible = len(anchors)

    sizing_cfg_gross = SizingConfig(
        mode=sizing_mode, kelly_fraction=kelly_fraction,
        min_position_usd=min_position_usd, bankroll_usd=bankroll_usd,
    )
    sizing_cfg_net = SizingConfig(
        mode=sizing_mode, kelly_fraction=kelly_fraction,
        min_position_usd=min_position_usd, bankroll_usd=bankroll_usd,
    )

    anchor_rows = []
    for anchor in anchors:
        print(f"\nPortfolio anchor {datetime.fromtimestamp(anchor, tz=timezone.utc).date()}", flush=True)
        row = evaluate_anchor_portfolio(
            db, anchor, fwd_secs, all_cutoffs,
            top_n=top_n, min_trading_days=min_trading_days,
            min_distinct_events=min_distinct_events, min_fwd_pos=min_fwd_pos,
            overlap_lambda=overlap_lambda, max_n=max_n, min_edge_score=min_edge_score,
            lookback_secs=lookback_secs, haircut_bps=haircut_bps,
            sizing_cfg_gross=sizing_cfg_gross, sizing_cfg_net=sizing_cfg_net,
            use_bhq=use_bhq, label_type=label_type, n_seeds=n_seeds,
            random_state=random_state,
        )
        anchor_rows.append(row)

    # Build rows for aggregate() — matches gbm_walkforward shape.
    agg_rows = [
        {
            'mean_edge': r.mean_edge,
            'std_edge': r.std_edge,
            'sharpe': r.sharpe,
            'flat_pnl': r.flat_pnl,
            'n_positions': r.n_positions,
        }
        for r in anchor_rows
    ]
    agg = aggregate(agg_rows)
    agg['n_eligible_anchors'] = n_eligible

    # PBO: seeds × anchors score matrix.
    # We use per-anchor mean_edge as a single-seed scalar.
    scores_matrix = np.array([[r.mean_edge] for r in anchor_rows]).T  # (1, n_anchors)
    pbo_result = compute_pbo(scores_matrix, n_perms=min(100, 10))
    if n_seeds >= _COMMITTED_VERDICT_N_SEEDS and n_eligible >= _COMMITTED_VERDICT_N_ANCHORS:
        verdict = 'OK' if pbo_result.pbo <= 0.5 else 'OVERFIT'
    else:
        verdict = 'undefined'

    pbo_dict = pbo_result_to_dict(pbo_result, verdict)

    per_anchor = [
        {
            'anchor_date': r.anchor_date,
            'anchor_unix': r.anchor_unix,
            'n_cohort': r.n_cohort,
            'n_positions': r.n_positions,
            'mean_edge': r.mean_edge,
            'std_edge': r.std_edge,
            'sharpe': r.sharpe,
            'flat_pnl': r.flat_pnl,
        }
        for r in anchor_rows
    ]

    return {
        'schema_version': 2,
        'strategy': 'portfolio_constructor_greedy',
        'aggregate': agg,
        'pbo': pbo_dict,
        'anchors': per_anchor,
    }
