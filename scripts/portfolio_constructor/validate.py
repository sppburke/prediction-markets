"""Walk-forward validation for Stage-2 portfolio construction.

numpy/local — requires .venv-analysis.  Not CI-tested.
"""
import gc
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from gbm_walkforward import eligible_anchors, aggregate, pbo_result_to_dict  # noqa: E402
from monthly_rerank_gbm import safe_workers  # noqa: E402
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
    max_candidates=None,
):
    """Build a greedy portfolio at `anchor` and measure forward edge.

    Universe = gbm_scores_at(anchor) — as-of-cutoff candidate set.
    Overlap = trailing (anchor - lookback_secs, anchor] Jaccard market sets.
    Forward positions = (anchor, anchor + fwd_secs].
    Ex-ante Kelly = prior-window returns from (anchor - lookback_secs, anchor].

    max_candidates: cap the GBM score pool to the top-K before loading market
    sets.  The greedy selector never picks a wallet outside the top-(max_n*4)
    unless every higher-scored wallet is excluded by overlap — very unlikely for
    max_n=50.  Defaults to max_n * 4 (e.g. 200 for max_n=50).  Set to None to
    use the full BHq pool (original behaviour; much slower on large pools).
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

    # Prefilter to top-K candidates before the expensive market-sets DB load.
    # load_wallet_market_sets is O(n_wallets) queries; cutting 5000 → 200 wallets
    # gives ~25x fewer rows fetched with negligible effect on greedy selection.
    _max_cands = max_candidates if max_candidates is not None else max_n * 4
    if len(scores) > _max_cands:
        top_keys = sorted(scores, key=scores.__getitem__, reverse=True)[:_max_cands]
        scores = {k: scores[k] for k in top_keys}
        print(f"  candidates: {_max_cands} (prefiltered from BHq pool)", flush=True)
    else:
        print(f"  candidates: {len(scores)}", flush=True)

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

    # Net returns use the SAME forward window/cohort as `positions`; the haircut
    # is a pure post-fetch transform in load_oos_positions
    # (vwap' = min(vwap * (1 + bps/1e4), 0.999)). Derive net in-memory instead of
    # re-querying — bit-identical because the clamp composes: for f >= 1,
    # min(min(raw, 0.999) * f, 0.999) == min(raw * f, 0.999). Saves one
    # forward-window scan per anchor.
    _hf = 1.0 + haircut_bps / 10_000.0
    net_returns = []
    for p in positions:
        nv = min(p.vwap_entry * _hf, 0.999)
        net_returns.append((p.outcome - nv) / nv)

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
    max_anchors=None, pbo_perms=100, max_candidates=None, max_workers=1,
):
    """Evaluate the greedy portfolio across all eligible anchors, compute PBO.

    Returns a dict matching the gbm_walkforward schema_version=2 shape, with
    an extra 'n_eligible_anchors' field and per-anchor sizing results.

    PBO matrix is (n_seeds, n_anchors): each cell is the mean_edge for that
    (seed, anchor) pair from a single-seed re-run, matching the gbm_walkforward
    pattern (gbm_walkforward.py:297-307).  The main walk-forward above uses the
    full n_seeds ensemble — the PBO data loop is separate.

    max_workers: anchors are independent (read-only DB, own GBM fit, results
    aggregated order-independently), so the main walk-forward loop runs them
    concurrently on a thread pool. sqlite3 and LightGBM both release the GIL
    (during query execution and fit), so threads overlap the dominant disk-read
    waits. Output is bit-identical to sequential: each anchor's GBM fit keeps
    n_jobs=-1 (num_threads fixed → same float-reduction order regardless of
    concurrency), and per-anchor rows are reassembled in anchor order before
    aggregation. Default 1 = sequential (original behaviour). The PBO data loop
    below stays sequential — it only runs at n_seeds >= 2.
    """
    import math as _math
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

    def _eval(anchor):
        print(f"\nPortfolio anchor {datetime.fromtimestamp(anchor, tz=timezone.utc).date()}", flush=True)
        row = evaluate_anchor_portfolio(
            db, anchor, fwd_secs, all_cutoffs,
            top_n=top_n, min_trading_days=min_trading_days,
            min_distinct_events=min_distinct_events, min_fwd_pos=min_fwd_pos,
            overlap_lambda=overlap_lambda, max_n=max_n, min_edge_score=min_edge_score,
            lookback_secs=lookback_secs, haircut_bps=haircut_bps,
            sizing_cfg_gross=sizing_cfg_gross, sizing_cfg_net=sizing_cfg_net,
            use_bhq=use_bhq, label_type=label_type, n_seeds=n_seeds,
            random_state=random_state, max_candidates=max_candidates,
        )
        gc.collect()  # release this anchor's training frame + GBM models promptly
        return row

    # Concurrent across anchors; reassemble in anchor order (executor.map
    # preserves input order) so the result is identical to sequential. Workers
    # are capped by free RAM (each anchor holds a training frame + GBM models)
    # to prevent OOM when memory is tight or the box is shared.
    n_workers = safe_workers(max_workers, n_eligible)
    if n_workers < min(max_workers, n_eligible):
        print(f"  (memory guard: {n_workers} workers, requested {max_workers})", flush=True)
    if n_workers > 1:
        print(f"\nwalk-forward: {n_eligible} anchors on {n_workers} threads", flush=True)
        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            anchor_rows = list(ex.map(_eval, anchors))
    else:
        anchor_rows = [_eval(a) for a in anchors]

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

    # PBO: build (n_seeds, n_anchors) matrix by re-running each anchor with
    # n_seeds=1 per seed — same pattern as gbm_walkforward.py:297-307.
    # The main walk-forward above averages all seeds before selection so we
    # cannot reuse those per-anchor mean_edge values for PBO.
    if n_seeds >= _COMMITTED_VERDICT_N_SEEDS and n_eligible >= 2:
        print(f"\nbuilding PBO score matrix ({n_seeds} seeds × {n_eligible} anchors) "
              f"— adds ~{n_seeds * n_eligible} single-seed anchor re-runs", flush=True)
        pbo_matrix = np.zeros((n_seeds, n_eligible), dtype=float)
        for si, seed in enumerate(range(random_state, random_state + n_seeds)):
            for ai, anchor in enumerate(anchors):
                r = evaluate_anchor_portfolio(
                    db, anchor, fwd_secs, all_cutoffs,
                    top_n=top_n, min_trading_days=min_trading_days,
                    min_distinct_events=min_distinct_events, min_fwd_pos=min_fwd_pos,
                    overlap_lambda=overlap_lambda, max_n=max_n,
                    min_edge_score=min_edge_score,
                    lookback_secs=lookback_secs, haircut_bps=0,
                    sizing_cfg_gross=sizing_cfg_gross,
                    sizing_cfg_net=sizing_cfg_net,
                    use_bhq=use_bhq, label_type=label_type,
                    n_seeds=1, random_state=seed,
                    max_candidates=max_candidates,
                )
                pbo_matrix[si, ai] = r.mean_edge
        n_perms_eff = min(_math.comb(n_eligible, n_eligible // 2), pbo_perms)
        pbo_result = compute_pbo(pbo_matrix, n_perms=n_perms_eff, rng_seed=42)
        if n_eligible < _COMMITTED_VERDICT_N_ANCHORS or _math.isnan(pbo_result.pbo):
            verdict = 'undefined'
        else:
            verdict = 'OK' if pbo_result.pbo <= 0.5 else 'OVERFIT'
    else:
        pbo_result = compute_pbo(np.zeros((1, max(n_eligible, 1))), n_perms=1)
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
