"""The pre-registered confirmatory small-grid for the #466 bake-off mode.

This module IS the pre-registration: committing it (its git SHA) ties every ``--confirmatory`` run
to the exact 13-config family it declares. A new family ⇒ a new commit. The grid is drawn from the
run17 exploratory bake-off's strongest signal (issue #451): **MinTRL-20** (an established track
record) appeared in 34/50 top configs, and **stateful policies** (``policy_online_weighting`` /
``policy_hybrid_displacement``) dominated the memoryless ``policy_full_rerank``; the estimator axis
barely mattered. Scoring fewer, coherent configs gives a far gentler PBO/SPA/Romano-Wolf correction,
so the honesty layer can give a real verdict on the one question that matters: **does MinTRL-20 +
a stateful policy beat the MinTRL-0 baseline?**

The family (13 configs), all ``deflator="none"`` to match the baseline:
  * baseline — ``t_stat_baseline | policy_full_rerank | ttr72 / 0.15-0.85 / MinTRL-0``, churn 0.0;
  * 12 challengers — ``{t_stat_baseline, eb_shrinkage_skill, true_clv}``
                     × ``{policy_online_weighting, policy_hybrid_displacement}``
                     × ``{ttr24 / 0.30-0.70 / MinTRL-20, ttr72 / 0.15-0.85 / MinTRL-20}``,
                     churn ``ranker_churn_cost_usd`` (0.75).

The honest multiplicity is preserved by design (Bailey-López de Prado): the Deflated-Sharpe bar
still deflates for the FULL pre-screen ``n_grid_full`` (960 on a full-coverage cache, built by the
shared ``build_default_axes``), while the bootstrap validators run on the executed 13-config matrix.
This is hypothesis-LOCKING on the same data, not independent confirmation — #417 should treat a
WINNER here as necessary-not-sufficient, pending the forward paper-trade. No import-time side effects.
"""
from . import Criteria
from .bakeoff import (
    BASELINE_ESTIMATOR,
    BASELINE_POLICY,
    NO_DEFLATION,
    RANKER_CHURN_COST_USD,
    GridPoint,
    _baseline_key,
    build_default_axes,
)
from .policies import HybridDisplacement, OnlineWeighting

# The anchored per-admission churn cost the challengers carry (the baseline is churn-free, 0.0).
CONFIRMATORY_CHURN = RANKER_CHURN_COST_USD

# The three pre-registered criteria levels — keyword-constructed, all six Criteria fields set. Each is
# a member of the committed default criteria sweep (build_criteria_grid: TTR {72,24,48} × band
# {0.15-0.85, 0.30-0.70} × MinTRL {0,20}); validate_confirmatory_grid re-checks membership.
_CRIT_BASELINE = Criteria(active_within_secs=0, ttr_hours=72.0, price_min=0.15, price_max=0.85,
                          half_life_days=0.0, min_trl=0)          # == build_criteria_grid()[0]
_CRIT_TTR24_TRL20 = Criteria(active_within_secs=0, ttr_hours=24.0, price_min=0.30, price_max=0.70,
                             half_life_days=0.0, min_trl=20)
_CRIT_TTR72_TRL20 = Criteria(active_within_secs=0, ttr_hours=72.0, price_min=0.15, price_max=0.85,
                             half_life_days=0.0, min_trl=20)

# The MinTRL-0 baseline (the §Acceptance benchmark; churn-free, deflator "none").
_BASELINE_GP = GridPoint(estimator=BASELINE_ESTIMATOR, deflator=NO_DEFLATION,
                         policy=BASELINE_POLICY, criteria=_CRIT_BASELINE, churn_cost=0.0)

_CHALLENGER_ESTIMATORS = (BASELINE_ESTIMATOR, "eb_shrinkage_skill", "true_clv")
_CHALLENGER_POLICIES = (OnlineWeighting.name, HybridDisplacement.name)   # the two stateful policies
_CHALLENGER_CRITERIA = (_CRIT_TTR24_TRL20, _CRIT_TTR72_TRL20)

# 3 estimators × 2 stateful policies × 2 MinTRL-20 criteria = 12 challengers, every one deflator
# "none" + churn ranker_churn_cost_usd.
_CHALLENGERS = [
    GridPoint(estimator=e, deflator=NO_DEFLATION, policy=p, criteria=c, churn_cost=CONFIRMATORY_CHURN)
    for e in _CHALLENGER_ESTIMATORS
    for p in _CHALLENGER_POLICIES
    for c in _CHALLENGER_CRITERIA
]

_CONFIRMATORY_GRID = [_BASELINE_GP] + _CHALLENGERS          # 13 configs


def build_confirmatory_grid() -> list:
    """The 13 pre-registered ``GridPoint``s (a fresh list each call — callers may filter it on a
    low-coverage cache without mutating the module constant)."""
    return list(_CONFIRMATORY_GRID)


def build_confirmatory_axes(estimators) -> "object":
    """The full default ``BakeoffAxes`` for the confirmatory run — delegates to the shared
    ``build_default_axes`` (single source of truth for the DSR multiplicity) on the POST-preflight
    ``estimators`` tuple, so ``n_grid_full`` is the honest full pre-screen count (960 with all five
    estimators present). ``args_like`` is left ``None`` so the committed full default sweep is used
    regardless of any operator CLI narrowing."""
    return build_default_axes(estimators=estimators)


def validate_confirmatory_grid(axes) -> None:
    """Pre-flight integrity check for the confirmatory grid against the full pre-registered ``axes``:
    exactly 13 configs, every one a member of ``axes.enumerate_grid()`` (so the leaderboard never
    scores a config the DSR bar did not count), unique keys, AND the baseline GridPoint's key equals
    ``_baseline_key(axes)`` — so the criteria[0]/churn[0] baseline reconstruction can never silently
    diverge from the pre-registered baseline. Raises ``ValueError`` on any violation. (Only valid on
    a full-coverage cache where all 13 ∈ axes; on a ``<30%``-true_clv cache the caller filters the
    grid to the survivors instead — see ``bakeoff.main`` --confirmatory.)"""
    grid = build_confirmatory_grid()
    if len(grid) != 13:
        raise ValueError(f"confirmatory grid must be 13 configs, got {len(grid)}")
    keys = [g.key for g in grid]
    if len(set(keys)) != len(keys):
        raise ValueError("confirmatory grid has duplicate config keys")
    full = {g.key for g in axes.enumerate_grid()}
    missing = [k for k in keys if k not in full]
    if missing:
        raise ValueError(
            f"confirmatory grid has {len(missing)} config(s) absent from the full axes: "
            f"{missing[:3]} — the axes estimator tuple must include every challenger estimator "
            "(check the true_clv preflight on a low-coverage cache)")
    if _BASELINE_GP.key != _baseline_key(axes):
        raise ValueError(
            f"confirmatory baseline {_BASELINE_GP.key!r} != axes baseline {_baseline_key(axes)!r}")
