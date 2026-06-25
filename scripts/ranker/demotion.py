"""Per-wallet demotion for the ranker bake-off (issue #421).

A ``Demoter`` conforms to the Protocol (``ranker/__init__.py``):
``should_demote(wallet, live_pnl, *, as_of) -> bool``. It reads the accumulated forward copy
P&L (the ``pe-backtest`` injected-set ``pnl_by_period`` handoff) and decides whether a wallet is
a PROVEN money-loser. ``policy_knockout_backfill`` / ``policy_hybrid_displacement`` own which
wallets to evict; this owns the statistical evidence bar.

PR5 ships the empirical-Bernstein demoter (the live-layer copy-trade knockout policy): demote only
when even the optimistic upper confidence bound on the wallet's per-period edge is negative AND
realized P&L is negative — i.e. statistically proven to lose, not merely unlucky.
The Wood-Zohren ``wallet_skill_cpd`` change-point demoter is a pluggable menu drop-in.
"""
import math

import pandas as pd


class EmpiricalBernsteinDemoter:
    """Demote iff a wallet is a PROVEN loser: the empirical-Bernstein (Maurer-Pontil) UPPER
    confidence bound on its mean per-period copy P&L is < 0 AND its cumulative realized P&L is < 0,
    over at least ``min_periods`` periods. The bound uses the observed per-period range as the
    bounded-support proxy (P&L per period is bounded by the flat stake x positions).

    ``upper = mean + sqrt(2 V ln(2/delta) / n) + 3 R ln(2/delta) / n`` (V = sample variance,
    R = observed range). Demotion requires ``upper < 0`` — the optimistic estimate is still a loss.

    # Optional-stopping caveat (#436 C4, accept-and-document): this is a FIXED-n Maurer-Pontil bound,
    # but ``should_demote`` is queried every walk-forward step on a GROWING n. Re-checking a fixed-n
    # bound at each step is an optional-stopping / multiple-testing problem, so the nominal ``delta``
    # is NOT a time-uniform ("anytime-valid") error guarantee over the whole trajectory — the
    # family-wise false-demotion rate inflates with the number of checks. This is accepted rather than
    # fixed here: (1) demotion is AND-gated by ``cumulative realized P&L < 0`` and ``min_periods``, so
    # a single optimistic-CB excursion cannot demote a wallet that is actually making money; (2) a
    # delayed demotion is low-cost (the wallet keeps a small flat stake until the evidence is
    # unambiguous); and (3) a time-uniform / stitched confidence sequence (Howard et al.) would be a
    # live knockout-policy behaviour change, out of scope for this bake-off-harness phase. A
    # confidence-sequence upgrade is the principled future fix.
    """

    name = "empirical_bernstein"

    def __init__(self, delta: float = 0.05, min_periods: int = 5):
        self.delta = delta
        self.min_periods = min_periods

    def should_demote(self, wallet: str, live_pnl: pd.DataFrame, *, as_of: int) -> bool:
        if len(live_pnl) == 0:
            return False
        rows = live_pnl[(live_pnl["wallet"] == wallet) & (live_pnl["period_end"] <= as_of)]
        pnl = rows["realized_pnl"].to_numpy(dtype=float)
        n = len(pnl)
        if n < self.min_periods:
            return False
        mean = float(pnl.mean())
        if mean >= 0.0 or float(pnl.sum()) >= 0.0:
            return False
        var = float(pnl.var(ddof=1)) if n > 1 else 0.0
        rng = float(pnl.max() - pnl.min())
        rng = rng if rng > 0 else 1.0
        ln = math.log(2.0 / self.delta)
        deviation = math.sqrt(2.0 * var * ln / n) + 3.0 * rng * ln / n
        return bool(mean + deviation < 0.0)


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
DEMOTER_REGISTRY: "dict[str, type]" = {EmpiricalBernsteinDemoter.name: EmpiricalBernsteinDemoter}
