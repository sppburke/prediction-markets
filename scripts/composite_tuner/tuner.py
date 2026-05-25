"""Optuna study orchestration. Owns the trial loop; delegates scoring to
`objective.evaluate_weights` and PBO calibration to `pbo.compute_pbo`.

The Optuna sampler is TPE by default (Bayesian); --sampler=cmaes optionally
swaps to CMA-ES for the continuous space. Trial failures (subprocess
non-zero, parse error) record -inf and continue — they don't abort the study.
"""
from __future__ import annotations

import dataclasses
import logging
from typing import Optional

import numpy as np
import optuna

from . import cv, objective, pbo

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class TunerConfig:
    db_path: str
    binary: str
    n_trials: int
    n_cutoffs: Optional[int] = None  # None = all available cutoffs
    pbo_perms: int = 100
    fwd_days: int = 30
    z_lcb: float = 1.645
    top_n: int = 5000
    bhq_q_bps: int = 1000
    min_trading_days: int = 20
    optuna_sampler: str = "tpe"      # "tpe" or "cmaes"
    optuna_seed: int = 42
    study_name: str = "composite_weight_tuner"
    storage: Optional[str] = None    # None = in-memory


@dataclasses.dataclass
class TunerResult:
    config: TunerConfig
    best_weights: dict[str, int]
    best_mean_lcb: float
    best_trial_number: int
    windows: list[cv.CvWindow]
    window_scores_best: list[objective.WindowScore]
    n_trials_completed: int
    scores_matrix: np.ndarray         # (n_trials, n_windows) per-window LCB
    pbo: pbo.PboResult
    trial_history: list[tuple[int, float, dict[str, int]]]  # (trial#, mean_lcb, weights)


def run_tuner(cfg: TunerConfig) -> TunerResult:
    """Build windows, run Optuna study, post-compute PBO. Returns full result."""
    windows = cv.windows_from_db(cfg.db_path, n_cutoffs=cfg.n_cutoffs, fwd_days=cfg.fwd_days)
    if not windows:
        raise RuntimeError(f"no cutoffs in wallet_features at {cfg.db_path}")
    log.info("composite-tuner: %d CV windows", len(windows))
    for w in windows:
        log.info("  %s (cutoff=%d, fwd_end=%d)", w.label, w.train_cutoff_unix, w.fwd_end_unix)

    # Accumulate per-trial-per-window scores for PBO.
    n_windows = len(windows)
    scores_rows: list[list[float]] = []
    trial_log: list[tuple[int, float, dict[str, int]]] = []

    def objective_fn(trial: optuna.Trial) -> float:
        weights = {
            name: trial.suggest_int(name, *objective.WEIGHT_BOUNDS[name])
            for name in objective.WEIGHT_NAMES
        }
        mean_lcb, per_window = objective.evaluate_weights(
            binary=cfg.binary,
            db_path=cfg.db_path,
            windows=windows,
            weights=weights,
            top_n=cfg.top_n,
            bhq_q_bps=cfg.bhq_q_bps,
            min_trading_days=cfg.min_trading_days,
            z=cfg.z_lcb,
        )
        # Record per-window LCB for PBO (replace -inf with NaN sentinel; pbo.py handles).
        row = [s.edge_lcb for s in per_window]
        scores_rows.append(row)
        trial_log.append((trial.number, mean_lcb, weights))
        if trial.number % 10 == 0:
            log.info(
                "trial %d: mean_lcb=%.6f windows=%s",
                trial.number, mean_lcb,
                [f"{s.n_positions}@{s.edge_lcb:.4f}" for s in per_window],
            )
        return mean_lcb if mean_lcb != float("-inf") else -1e9

    # Quiet Optuna's per-trial INFO chatter; we log our own.
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler: optuna.samplers.BaseSampler
    if cfg.optuna_sampler == "cmaes":
        sampler = optuna.samplers.CmaEsSampler(seed=cfg.optuna_seed)
    else:
        sampler = optuna.samplers.TPESampler(seed=cfg.optuna_seed)
    study = optuna.create_study(
        study_name=cfg.study_name,
        direction="maximize",
        sampler=sampler,
        storage=cfg.storage,
        load_if_exists=cfg.storage is not None,
    )
    study.optimize(objective_fn, n_trials=cfg.n_trials, show_progress_bar=False)

    scores_matrix = np.array(scores_rows, dtype=float)
    pbo_result = pbo.compute_pbo(scores_matrix, n_perms=cfg.pbo_perms, rng_seed=cfg.optuna_seed)

    # Re-score the best trial's weights to attach per-window detail.
    best_w = study.best_trial.params
    best_w = {k: int(v) for k, v in best_w.items()}
    _best_mean, best_window_scores = objective.evaluate_weights(
        binary=cfg.binary,
        db_path=cfg.db_path,
        windows=windows,
        weights=best_w,
        top_n=cfg.top_n,
        bhq_q_bps=cfg.bhq_q_bps,
        min_trading_days=cfg.min_trading_days,
        z=cfg.z_lcb,
    )

    return TunerResult(
        config=cfg,
        best_weights=best_w,
        best_mean_lcb=float(study.best_value),
        best_trial_number=int(study.best_trial.number),
        windows=windows,
        window_scores_best=best_window_scores,
        n_trials_completed=len(study.trials),
        scores_matrix=scores_matrix,
        pbo=pbo_result,
        trial_history=trial_log,
    )
