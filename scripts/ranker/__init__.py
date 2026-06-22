"""Modular wallet-ranking evaluation harness — typed plugin contracts (issue #421, PR1).

Permanent backbone for the Phase-0 ranker bake-off. Every estimator / signal-combiner /
deflator / selector / set-transition policy / integrity-filter / capacity-filter / demoter /
validator is a swappable module behind one of the typed ``Protocol``s defined here, so the
bake-off can sweep ``(estimator x combiner x deflation x selector x filters x criteria x
policy)`` configs without re-architecture. The cited menu and seams live in issue #421
(see "THE BAKE-OFF MENU" and "Module contracts").

``scripts/`` is a flat (non-package) module collection, so importing this sub-package
bootstraps its parent dir onto ``sys.path`` — submodules can then reuse the production
helpers ``ranker_duck`` / ``ranker_decay`` / ``rank_72hr_buyandhold`` directly (no drift).
"""
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd

# scripts/ onto sys.path so submodules can `import ranker_duck` / `from ranker_decay import ...`.
_SCRIPTS_DIR = str(Path(__file__).resolve().parent.parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

# --- Frame aliases (columns/dtypes pinned in suff_stats.py and issue #421 "Module contracts"). ---
SuffStats = pd.DataFrame      # one row per QUALIFYING first-buy position; every module reads it
WalletScores = pd.DataFrame   # indexed by wallet; cols: score:float (higher=better), rank:int (1=best)
FollowSet = pd.DataFrame      # {wallet, weight}: hard top-k -> 1.0, soft/online -> continuous


@runtime_checkable
class Estimator(Protocol):
    """Per-wallet skill score. MUST read only rows with ``resolved_at <= as_of`` (LANDMINE-2
    look-ahead guard; enforced by PR2's ``oos_validation``)."""

    name: str

    def score(self, ss: SuffStats, *, as_of: int, weights: np.ndarray) -> WalletScores: ...


@runtime_checkable
class SignalCombiner(Protocol):
    """Combine several estimator signals into one score."""

    name: str

    def combine(self, signals: dict[str, WalletScores], *, as_of: int) -> WalletScores: ...


@runtime_checkable
class Deflator(Protocol):
    """Multiple-testing deflation. Per-wallet (``n_trials`` = candidate count) AND grid-level
    (``n_trials`` = the pre-registered ``N_GRID``; issue #421 "Grid pre-registration")."""

    name: str

    def deflate(self, scores: WalletScores, *, n_trials: int, as_of: int) -> WalletScores: ...


@runtime_checkable
class Selector(Protocol):
    """Pick/weight the followed set. Stateful: ``state`` carries across walk-forward steps
    (TTTS / BOA / sleeping-experts); full-rerank ignores it."""

    name: str

    def select(self, scores: WalletScores, *, k: int,
               state: object | None) -> tuple[FollowSet, object]: ...


@runtime_checkable
class SetTransitionPolicy(Protocol):
    """First-class axis: how the followed set EVOLVES period-to-period; owns churn accounting.
    ``live_pnl`` schema = the pe-backtest P&L handoff (issue #421 "pe-backtest P&L handoff")."""

    name: str

    def step(self, prev: FollowSet, fresh: WalletScores, live_pnl: pd.DataFrame, *,
             as_of: int) -> FollowSet: ...


@runtime_checkable
class IntegrityFilter(Protocol):
    """Wash / arb / single-whale integrity mask. Returns a bool array (True = keep)."""

    name: str

    def mask(self, ss: SuffStats, scores: WalletScores, *, as_of: int) -> np.ndarray: ...


@runtime_checkable
class CapacityFilter(Protocol):
    """Liquidity/impact haircut: down-weight wallets whose edge lives in markets too thin
    for a ``dollar`` copy."""

    name: str

    def haircut(self, ss: SuffStats, scores: WalletScores, *, dollar: float) -> WalletScores: ...


@runtime_checkable
class Demoter(Protocol):
    """Per-wallet demotion decision from accumulated live copy P&L."""

    name: str

    def should_demote(self, wallet: str, live_pnl: pd.DataFrame, *, as_of: int) -> bool: ...


@runtime_checkable
class Validator(Protocol):
    """Walk-forward / winner's-curse inference over the leaderboard (not per-wallet).
    ``n_configs`` = the pre-registered ``N_GRID``."""

    name: str

    def assess(self, leaderboard: pd.DataFrame, *, n_configs: int) -> pd.DataFrame: ...


@dataclass(frozen=True)
class Criteria:
    """One settable point on the criteria grid (issue #421 "Criteria grid")."""

    active_within_secs: int   # active_as_of_T: 24/48/72h per-wallet last-trade clock (#357)
    ttr_hours: float          # TTR horizon: 24/48/72
    price_min: float
    price_max: float
    half_life_days: float     # decay; <= 0 = flat/legacy
    min_trl: int              # MinTRL gate


__all__ = [
    "SuffStats", "WalletScores", "FollowSet",
    "Estimator", "SignalCombiner", "Deflator", "Selector", "SetTransitionPolicy",
    "IntegrityFilter", "CapacityFilter", "Demoter", "Validator", "Criteria",
]
