"""Orchestration layer: build the deploy-cutoff portfolio.

numpy/local — requires .venv-analysis.  Not CI-tested.
"""
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from . import data as _data  # noqa: E402
from . import edge as _edge  # noqa: E402
from .overlap import marginal_overlap  # noqa: E402
from .selector import GreedySelector, SelectionResult  # noqa: E402
from .sizing import SizingConfig  # noqa: E402
from .validate import run_walk_forward  # noqa: E402


@dataclass
class PortfolioConfig:
    db: str
    fwd_secs: int
    top_n: int                  = 5000
    min_trading_days: int       = 20
    min_distinct_events: int    = 10
    min_fwd_pos: int            = 3
    overlap_lambda: float       = 1.0
    max_n: int                  = 50
    min_edge_score: float       = 0.0
    lookback_secs: int          = 90 * 86400
    haircut_bps: int            = 500
    sizing_mode: str            = 'fractional'
    kelly_fraction: float       = 0.25
    min_position_usd: float     = 5.0
    bankroll_usd: float         = 1000.0
    use_bhq: bool               = True
    label_type: str             = 'perpos'
    n_seeds: int                = 5
    random_state: int           = 42
    max_anchors: int            = None
    pbo_perms: int              = 100
    watchlist_path: str         = None   # required .txt path; deploy filter only
    max_candidates: int         = None   # prefilter BHq pool before market-sets load
    max_workers: int            = 1      # concurrent anchors in walk-forward loop


@dataclass
class PortfolioResult:
    walk_forward: dict          # schema_version=2 dict from run_walk_forward
    deploy_wallets: list        # selected wallet hexes at latest eligible cutoff
    deploy_cutoff_unix: int


def run_constructor(cfg):
    """Run walk-forward validation and build the deploy portfolio.

    The --watchlist filter is applied only to the deploy-cutoff selection.
    Walk-forward historical anchors always use the full gbm_scores_at universe
    (no watchlist filter) to prevent look-ahead via a future-derived watchlist.
    """
    all_cutoffs = _data.distinct_cutoffs(cfg.db)

    wf = run_walk_forward(
        db=cfg.db, fwd_secs=cfg.fwd_secs, all_cutoffs=all_cutoffs,
        top_n=cfg.top_n, min_trading_days=cfg.min_trading_days,
        min_distinct_events=cfg.min_distinct_events, min_fwd_pos=cfg.min_fwd_pos,
        overlap_lambda=cfg.overlap_lambda, max_n=cfg.max_n,
        min_edge_score=cfg.min_edge_score, lookback_secs=cfg.lookback_secs,
        haircut_bps=cfg.haircut_bps, sizing_mode=cfg.sizing_mode,
        kelly_fraction=cfg.kelly_fraction, min_position_usd=cfg.min_position_usd,
        bankroll_usd=cfg.bankroll_usd, use_bhq=cfg.use_bhq,
        label_type=cfg.label_type, n_seeds=cfg.n_seeds,
        random_state=cfg.random_state, max_anchors=cfg.max_anchors,
        pbo_perms=cfg.pbo_perms, max_candidates=cfg.max_candidates,
        max_workers=cfg.max_workers,
    )

    # Deploy: select at the latest cutoff.
    deploy_cutoff = all_cutoffs[-1]
    train_cutoffs = [c for c in all_cutoffs if c < deploy_cutoff]

    scores = _edge.gbm_edge_scores(
        cfg.db, deploy_cutoff, cfg.fwd_secs,
        cfg.min_trading_days, cfg.min_distinct_events, cfg.min_fwd_pos,
        train_cutoffs, use_bhq=cfg.use_bhq, label_type=cfg.label_type,
        n_seeds=cfg.n_seeds, random_state=cfg.random_state,
    )

    # Apply watchlist filter (deploy-cutoff run only).
    if cfg.watchlist_path:
        allowed = _load_watchlist(cfg.watchlist_path)
        scores = {w: s for w, s in scores.items() if w in allowed}

    # Same candidate prefilter as evaluate_anchor_portfolio.
    _max_cands = cfg.max_candidates if cfg.max_candidates is not None else cfg.max_n * 4
    if len(scores) > _max_cands:
        top_keys = sorted(scores, key=scores.__getitem__, reverse=True)[:_max_cands]
        scores = {k: scores[k] for k in top_keys}

    market_sets = _data.load_wallet_market_sets(
        cfg.db, deploy_cutoff, list(scores.keys()), cfg.lookback_secs
    )
    selector = GreedySelector(overlap_fn=marginal_overlap, overlap_lambda=cfg.overlap_lambda)
    sel = selector.select(scores, market_sets, max_n=cfg.max_n, min_edge_score=cfg.min_edge_score)

    return PortfolioResult(
        walk_forward=wf,
        deploy_wallets=sel.wallets,
        deploy_cutoff_unix=deploy_cutoff,
    )


def _load_watchlist(path):
    """Return a set of wallet hex addresses from a .txt watchlist file."""
    result = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            result.add(line)
    return result
