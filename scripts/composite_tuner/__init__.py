"""composite_tuner — Optuna+PBO weight-optimisation harness for pe-skill-select.

Tunes the 12 `CompositeWeights` of `pe-skill-select composite` against a
walk-forward CV objective (realized edge-LCB across the selected cohort's
forward window), then runs Bailey & López de Prado PBO calibration on the
trial scores.

Prerequisites:
- `pe-skill-select` Rust binary built (./target/release/pe-skill-select)
- `wallet_cache.db` has `wallet_features` rows at multiple cutoff_unix values
  (PBO needs ≥4 folds)
- Python venv with `optuna`, `numpy`, `scipy` (.venv-analysis/ is the host)

Usage (from repo root):
    .venv-analysis/bin/python3 scripts/composite_tuner/cli.py \\
        --db-path data/wallet_cache.db \\
        --smoke   # or --n-trials 300

Issue: https://github.com/sppburke/prediction-markets/issues/238
"""

__version__ = "0.1.0"

from .tuner import TunerConfig, TunerResult, run_tuner
from .objective import WEIGHT_NAMES, ENV_KEYS, WEIGHT_BOUNDS

__all__ = [
    "TunerConfig",
    "TunerResult",
    "run_tuner",
    "WEIGHT_NAMES",
    "ENV_KEYS",
    "WEIGHT_BOUNDS",
]
