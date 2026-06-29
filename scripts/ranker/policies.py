"""Set-transition policies — the FIRST-CLASS bake-off axis (issue #421).

The selectors pick a set at one cutoff T; a ``SetTransitionPolicy`` decides how the followed set
EVOLVES period-to-period and owns churn accounting. It conforms to the Protocol
(``ranker/__init__.py``): ``step(prev, fresh, live_pnl, *, as_of) -> FollowSet``. ``prev`` is the
previous followed set, ``fresh`` the new ``WalletScores``, ``live_pnl`` the accumulated
``pe-backtest`` injected-set per-period copy P&L. The bake-off scores each policy as a full
walk-forward TRAJECTORY by cumulative forward copy P&L net of churn (issue #421 "Set-transition
policy axis").

All four required policies (issue #421 v1 build scope) live here:
  * ``policy_full_rerank`` (A, baseline) — memoryless wholesale replace.
  * ``policy_knockout_backfill`` (B, current live) — keep + evict-proven-losers + backfill.
  * ``policy_hybrid_displacement`` (B + ossification cure) — B + fresh-displaces-unproven.
  * ``policy_online_weighting`` — the online selector IS the policy (soft, state-carrying).

# Note: ``KnockoutBackfill`` / ``HybridDisplacement`` take an injected ``Demoter``;
# ``OnlineWeighting`` takes an injected online ``Selector`` and carries its state on the instance,
# so the driver constructs ONE policy instance per trajectory.
"""
import numpy as np
import pandas as pd

from . import FollowSet, WalletScores


def _hard_set(wallets: list) -> FollowSet:
    return pd.DataFrame({"wallet": list(wallets), "weight": np.ones(len(wallets))})


class FullRerank:
    """A (baseline): each refresh, replace the set wholesale with the fresh top-k. Memoryless —
    ignores ``prev`` and ``live_pnl``; churns on cutoff noise."""

    name = "policy_full_rerank"

    def __init__(self, k: int):
        self.k = k

    def step(self, prev: FollowSet, fresh: WalletScores, live_pnl: pd.DataFrame, *,
             as_of: int) -> FollowSet:
        top = fresh.nsmallest(min(self.k, len(fresh)), "rank")
        return _hard_set(top.index.to_list())


class KnockoutBackfill:
    """B (current live): keep the set, evict only PROVEN losers (the injected ``Demoter``), then
    backfill freed slots from the freshest top-ranked bench. Per-batch hysteresis = a wallet stays
    until proven a loser. On a cold start (empty ``prev``) it degenerates to the fresh top-k."""

    name = "policy_knockout_backfill"

    def __init__(self, k: int, demoter):
        self.k = k
        self.demoter = demoter

    def step(self, prev: FollowSet, fresh: WalletScores, live_pnl: pd.DataFrame, *,
             as_of: int) -> FollowSet:
        demoted = self.demoter.demote_set(live_pnl, as_of=as_of)   # one groupby, not a per-incumbent rescan
        kept = [w for w in (prev["wallet"].to_list() if len(prev) else []) if w not in demoted]
        for wallet in fresh.sort_values("rank").index:          # backfill, best-ranked first
            if len(kept) >= self.k:
                break
            if wallet not in kept:
                kept.append(wallet)
        return _hard_set(kept[:self.k])


class HybridDisplacement:
    """B + cure for ossification: knockout+backfill, PLUS a sufficiently-higher-ranked FRESH
    wallet may DISPLACE a not-yet-proven incumbent. An incumbent is "not-yet-proven" when it has
    fewer than the demoter's ``min_periods`` of live evidence (so it is neither demotable nor
    established). A challenger displaces the worst-ranked unproven incumbent when its fresh rank
    beats that incumbent's by more than ``displacement_margin`` (issue #421: the threshold is a
    grid knob)."""

    name = "policy_hybrid_displacement"

    def __init__(self, k: int, demoter, displacement_margin: int):
        self.k = k
        self.demoter = demoter
        self.displacement_margin = displacement_margin
        self._base = KnockoutBackfill(k, demoter)
        self._proven_min = int(getattr(demoter, "min_periods", 5))

    def step(self, prev: FollowSet, fresh: WalletScores, live_pnl: pd.DataFrame, *,
             as_of: int) -> FollowSet:
        kept = self._base.step(prev, fresh, live_pnl, as_of=as_of)["wallet"].to_list()
        rank = fresh["rank"].to_dict()
        worst_rank = (int(fresh["rank"].max()) if len(fresh) else 0) + 1

        # B2 (#436): mirror the demoter's as-of filter (demotion.py). live_pnl accumulates each step's
        # forward horizon window, so by this step it holds rows with period_end > as_of from PRIOR
        # steps; counting them would let FUTURE periods mark an incumbent "proven" and shield it from
        # displacement. Only periods CLOSED by as_of count. Precomputed in ONE value_counts (was an
        # object-dtype `wallet ==` rescan of live_pnl per (challenger, incumbent) — O(n²) at scale).
        _closed_w = live_pnl.loc[live_pnl["period_end"] <= as_of, "wallet"] if len(live_pnl) else None
        n_periods_map = _closed_w.value_counts().to_dict() if _closed_w is not None else {}

        def n_periods(wallet: str) -> int:
            return int(n_periods_map.get(wallet, 0))

        for challenger in fresh.sort_values("rank").index:      # best challengers first
            if challenger in kept:
                continue
            ch_rank = rank.get(challenger, worst_rank)
            # worst-ranked unproven (displaceable) incumbent
            target, target_rank = None, -1
            for inc in kept:
                if n_periods(inc) >= self._proven_min:          # proven -> protected
                    continue
                inc_rank = rank.get(inc, worst_rank)
                if inc_rank > target_rank:
                    target, target_rank = inc, inc_rank
            if target is not None and ch_rank + self.displacement_margin < target_rank:
                kept = [w for w in kept if w != target] + [challenger]
        return _hard_set(kept[:self.k])


class OnlineWeighting:
    """The online selector IS the policy (issue #421 ``policy_online_weighting``): state-carrying
    continuous weights, a soft set rather than a hard top-k. Holds the selector's state on the
    instance across steps — the driver builds one instance per trajectory."""

    name = "policy_online_weighting"

    def __init__(self, k: int, selector):
        self.k = k
        self.selector = selector
        self._state: "object | None" = None

    def step(self, prev: FollowSet, fresh: WalletScores, live_pnl: pd.DataFrame, *,
             as_of: int) -> FollowSet:
        follow_set, self._state = self.selector.select(fresh, k=self.k, state=self._state)
        return follow_set


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
POLICY_REGISTRY: "dict[str, type]" = {
    cls.name: cls
    for cls in (FullRerank, KnockoutBackfill, HybridDisplacement, OnlineWeighting)
}
