"""GBM edge provider for Stage-2 portfolio construction.

lightgbm/local — requires .venv-analysis.  Not CI-tested.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from monthly_rerank_gbm import gbm_scores_at  # noqa: E402

from . import data as _data  # noqa: E402


def gbm_edge_scores(
    db,
    cutoff_unix,
    fwd_secs,
    min_trading_days,
    min_distinct_events,
    min_fwd_pos,
    train_cutoffs,
    use_bhq=True,
    label_type='perpos',
    n_seeds=5,
    random_state=42,
):
    """Return as-of-cutoff GBM edge scores for all eligible wallets.

    Thin wrapper around gbm_scores_at.  The returned dict IS the candidate
    universe for the greedy selector at this cutoff (gated + optional BHq).

    No look-ahead: training is strictly c + fwd_secs <= cutoff_unix (enforced
    by gbm_scores_at's safe_train guard in monthly_rerank_gbm.py:143).

    Returns:
        dict[wallet_hex -> float]  (score; higher = better predicted edge)
    """
    all_cutoffs = _data.distinct_cutoffs(db)
    safe_train = [c for c in train_cutoffs if c + fwd_secs <= cutoff_unix]
    if not safe_train:
        return {}
    return gbm_scores_at(
        db, cutoff_unix, fwd_secs,
        min_trading_days, min_distinct_events, min_fwd_pos,
        safe_train, use_bhq=use_bhq, label_type=label_type,
        n_seeds=n_seeds, random_state=random_state,
    )
