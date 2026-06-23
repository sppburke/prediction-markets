"""Selectors for the ranker bake-off — pick/weight the followed set (issue #421).

Each selector conforms to the ``Selector`` Protocol (``ranker/__init__.py``):
``select(scores, *, k, state) -> (FollowSet, state)``. ``FollowSet`` is a ``{wallet, weight}``
frame (hard top-k -> weight 1.0; soft/online -> continuous). ``state`` is opaque and carries
across walk-forward steps for the stateful/online members; the hard selectors ignore it.

PR5 ships ``top_k`` (the greedy baseline) and ``online_exp_weights`` (the online soft selector
backing ``policy_online_weighting``); the corr-aware / conformal / TTTS menu members are
pluggable drop-ins behind the same Protocol (issue #421 "v1 build scope").
"""
import numpy as np
import pandas as pd

from . import FollowSet, WalletScores


def _follow_set(wallets: list, weights: np.ndarray) -> FollowSet:
    return pd.DataFrame({"wallet": list(wallets), "weight": np.asarray(weights, dtype=float)})


class TopK:
    """Hard top-k by rank (issue #421 ``greedy_max_group_sharpe`` baseline shape): take the ``k``
    best-ranked wallets, each at weight 1.0. Stateless — ``state`` passes through untouched."""

    name = "top_k"

    def select(self, scores: WalletScores, *, k: int,
               state: "object | None") -> "tuple[FollowSet, object]":
        top = scores.nsmallest(min(k, len(scores)), "rank")
        return _follow_set(top.index.to_list(), np.ones(len(top))), state


class OnlineExpWeights:
    """Online soft selector (issue #421 ``boa_weighting`` / online-aggregation family): carry an
    EWMA of each wallet's score across walk-forward steps, then weight the top-k by an
    exponential (softmax) of the smoothed score, scaled to sum to the set size so the soft set's
    total mass matches a hard top-k of ``k`` wallets at weight 1.0.

    ``state`` is the ``dict[wallet -> smoothed_score]`` carried between steps (a fresh wallet
    seeds at its current score). This is the selector that ``policy_online_weighting`` IS.

    # Note: a simplified stand-in (its two knobs eta/alpha aside) for the full BOA /
    # sleeping-experts members, which remain menu items. Deterministic — no RNG.
    """

    name = "online_exp_weights"

    def __init__(self, eta: float = 4.0, alpha: float = 0.5):
        self.eta = eta          # softmax temperature (higher -> more concentrated)
        self.alpha = alpha      # EWMA weight on the current step's score

    def select(self, scores: WalletScores, *, k: int,
               state: "object | None") -> "tuple[FollowSet, object]":
        prev: dict = dict(state) if state else {}
        smoothed = {
            wallet: self.alpha * float(sc) + (1.0 - self.alpha) * prev.get(wallet, float(sc))
            for wallet, sc in scores["score"].items()
        }
        if not smoothed:
            return _follow_set([], np.empty(0)), smoothed
        s = pd.Series(smoothed)
        top = s.nlargest(min(k, len(s)))
        z = np.exp(self.eta * (top.to_numpy() - top.to_numpy().max()))
        weights = z / z.sum() * len(top)        # sum to len(top) (~k), matching hard-set mass
        return _follow_set(top.index.to_list(), weights), smoothed


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
SELECTOR_REGISTRY: "dict[str, type]" = {
    cls.name: cls for cls in (TopK, OnlineExpWeights)
}
