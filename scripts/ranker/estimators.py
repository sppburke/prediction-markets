"""Per-wallet skill estimators for the ranker bake-off (issue #421).

Each estimator conforms to the ``Estimator`` Protocol (``ranker/__init__.py``): it reads the
shared ``suff_stats`` substrate and returns a ``WalletScores`` frame (``score``: higher =
better, ``rank``: 1 = best). PR1 ships the reference template ``eb_shrinkage_skill``; the
remaining menu estimators are pluggable drop-ins behind the same Protocol (issue #421
"v1 build scope").
"""
import numpy as np
import pandas as pd
from scipy.stats import norm

from ranker_decay import weighted_stats  # reuse #366 weighted statistics — no drift


class EBShrinkageSkill:
    """Empirical-Bayes posterior ``P(edge > 0)`` per wallet (Jensen-Kelly-Pedersen).

    Shrink each wallet's net-edge mean toward the cross-sectional prior by precision, score by
    the posterior tail probability. Net edge per position = ``(payoff - _eff) / _eff``.

    # Preconditions (the harness guarantees these; this is the reference template):
    #   * ``ss`` is already filtered to ``resolved_at <= as_of`` (LANDMINE-2) and to the active
    #     criteria / MinTRL.
    #   * ``weights`` is a positional ndarray aligned to the ORIGINAL (un-sliced) suff_stats
    #     RangeIndex; ``ss`` MAY be a label-preserving row-slice of it, so ``weights[g.index]``
    #     stays correct. Do NOT ``reset_index`` after slicing ``ss`` (that would mis-align it).
    #   * >= 2 candidate wallets — the cross-sectional EB prior is otherwise undefined (a single
    #     candidate degenerates to rank 1). Wallets with < 2 effective observations have an
    #     undefined posterior SD and are dropped from the ranking.
    """

    name = "eb_shrinkage_skill"

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        rows = []
        for wallet, g in ss.groupby("wallet", sort=False):
            net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
            mean, sd, n_eff, _ = weighted_stats(net, weights[g.index.to_numpy()])
            rows.append((wallet, mean, sd, max(n_eff, 1.0)))
        df = pd.DataFrame(rows, columns=["wallet", "mean", "sd", "n_eff"]).set_index("wallet")
        se2 = (df["sd"] ** 2) / df["n_eff"]                      # sampling variance of the mean
        # EB prior var (normal-normal). `var(ddof=1)` is NaN for < 2 wallets, and
        # `max(NaN, 1e-9)` keeps the NaN — which would silently empty the result; floor
        # explicitly so a degenerate <2-candidate input stays deterministic.
        prior_var = df["mean"].var(ddof=1) - se2.mean()
        tau2 = max(prior_var, 1e-9) if np.isfinite(prior_var) else 1e-9
        mu0 = df["mean"].mean()                                  # EB prior mean
        shrink = tau2 / (tau2 + se2)
        post_mean = mu0 + shrink * (df["mean"] - mu0)
        post_sd = np.sqrt(shrink * se2).replace(0, np.nan)
        df["score"] = norm.cdf(post_mean / post_sd)             # P(edge > 0)
        # Drop wallets with an undefined posterior (n_eff <= 1 -> NaN SD): unscoreable, not
        # low-scoring. (Hardening over issue #421's reference snippet, whose `.astype(int)`
        # would raise on NaN ranks; the upstream MinTRL gate normally removes these first.)
        df = df[df["score"].notna()].copy()
        df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
        return df[["score", "rank"]]


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
REGISTRY: "dict[str, type]" = {EBShrinkageSkill.name: EBShrinkageSkill}
