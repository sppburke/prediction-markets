"""Probability of Backtest Overfitting (Bailey & López de Prado, 2014/2015).

Combinatorially symmetric crossvalidation:
- N trial configs × T CV windows score matrix.
- For each of n_perms permutations of the T windows:
  - Randomly partition windows into IS half and OOS half.
  - For each trial, IS_score = mean over IS windows; OOS_score = mean over OOS.
  - Find best IS trial (argmax over trials).
  - OOS rank ratio = rank(OOS_score of best-IS trial) / N  (0=worst, 1=best).
  - logit(rank ratio), clipped away from 0/1.
- PBO = fraction of permutations where logit < 0 (best-IS trial is below the
  OOS median). PBO > 0.5 → systematic overfit; PBO < 0.5 → real signal.

Falls back to Monte-Carlo sampling of permutations when C(T, T/2) is huge.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass(frozen=True)
class PboResult:
    pbo: float                # P(best-IS trial is below OOS median); want ≤ 0.5
    n_perms: int
    n_trials: int
    n_windows: int
    median_oos_rank: float    # 0..1; expectation under no skill is 0.5
    logit_values: list[float] # full permutation distribution (diagnostic)


def compute_pbo(
    scores_matrix: np.ndarray,
    n_perms: int = 100,
    rng_seed: int = 42,
) -> PboResult:
    """scores_matrix shape (n_trials, n_windows). NaN/-inf treated as worst score.

    Algorithm (Bailey & LdP 2014/2015):
    Sample n_perms random IS/OOS half-splits of windows. For each split:
    - IS rank trial scores by mean(IS windows); pick best.
    - Compute that best trial's OOS rank ratio.
    - logit = ln(r / (1 − r)) with r clipped to [eps, 1−eps].
    PBO = mean(logit < 0).
    """
    if scores_matrix.ndim != 2:
        raise ValueError("scores_matrix must be 2D (trials, windows)")
    n_trials, n_windows = scores_matrix.shape
    if n_trials < 2 or n_windows < 2:
        return PboResult(
            pbo=float("nan"),
            n_perms=0,
            n_trials=n_trials,
            n_windows=n_windows,
            median_oos_rank=float("nan"),
            logit_values=[],
        )
    # Clean: replace -inf / NaN with the column min minus 1 (treat as worst).
    finite = np.where(np.isfinite(scores_matrix), scores_matrix, np.nan)
    col_min = np.nanmin(finite, axis=0)
    clean = np.where(
        np.isfinite(scores_matrix), scores_matrix, col_min - 1.0
    )
    rng = np.random.default_rng(rng_seed)
    n_is = n_windows // 2
    logit_values: list[float] = []
    eps = 1.0 / (n_trials + 1)
    for _ in range(n_perms):
        perm = rng.permutation(n_windows)
        is_idx = perm[:n_is]
        oos_idx = perm[n_is:]
        is_means = clean[:, is_idx].mean(axis=1)
        oos_means = clean[:, oos_idx].mean(axis=1)
        best_is = int(np.argmax(is_means))
        # OOS rank ratio of best-IS trial: how it ranks among all trials' OOS
        oos_ranks = np.argsort(np.argsort(oos_means))  # 0..n_trials-1 ascending
        r = (oos_ranks[best_is] + 1) / (n_trials + 1)
        r = min(max(r, eps), 1.0 - eps)
        logit_values.append(math.log(r / (1.0 - r)))
    pbo = float(sum(1 for v in logit_values if v < 0) / len(logit_values))
    median_rank = float(np.median([1.0 / (1.0 + math.exp(-v)) for v in logit_values]))
    return PboResult(
        pbo=pbo,
        n_perms=n_perms,
        n_trials=n_trials,
        n_windows=n_windows,
        median_oos_rank=median_rank,
        logit_values=logit_values,
    )


def pbo_summary(result: PboResult) -> str:
    if math.isnan(result.pbo):
        return (
            f"PBO=N/A — too few trials ({result.n_trials}) or windows "
            f"({result.n_windows}) to compute."
        )
    verdict = "OVERFIT (>0.50)" if result.pbo > 0.5 else "OK"
    return (
        f"PBO={result.pbo:.3f} ({verdict})  "
        f"median_OOS_rank={result.median_oos_rank:.3f}  "
        f"[n_trials={result.n_trials}, n_windows={result.n_windows}, "
        f"n_perms={result.n_perms}]"
    )
