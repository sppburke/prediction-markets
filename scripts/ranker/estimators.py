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


class TStatBaseline:
    """Raw net-edge t-statistic per wallet — the literature-confirmed winner's-curse BASELINE
    (issue #421 ``t_stat_baseline``; the bar every challenger must beat, and the §Acceptance
    benchmark). Net edge per position = ``(payoff - _eff) / _eff``; ``score = mean / SE =
    mean * sqrt(n_eff) / sd``. Deliberately NO shrinkage and NO deflation — it is the OLD ranker's
    core, so the bake-off can measure each challenger's lift over it.

    # Precondition: as EBShrinkageSkill (resolved_at <= as_of, positional ``weights`` aligned to
    # the un-sliced index). Wallets with < 2 effective obs or zero dispersion (undefined SE) drop.
    """

    name = "t_stat_baseline"

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        rows = []
        for wallet, g in ss.groupby("wallet", sort=False):
            net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
            mean, sd, n_eff, _ = weighted_stats(net, weights[g.index.to_numpy()])
            rows.append((wallet, mean, sd, n_eff))
        df = pd.DataFrame(rows, columns=["wallet", "mean", "sd", "n_eff"]).set_index("wallet")
        se = df["sd"] / np.sqrt(df["n_eff"])
        df["score"] = (df["mean"] / se).where((df["n_eff"] >= 2) & (se > 0))
        df = df[df["score"].notna()].copy()
        df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
        return df[["score", "rank"]]


class GuKoenkerNPMLE:
    """NPMLE compound-decision ranking (Gu-Koenker, Econometrica 2023) — issue #421 LIKELY
    HEADLINE. Estimate the latent skill distribution ``G`` non-parametrically (Kiefer-Wolfowitz
    MLE on a fixed grid, by EM — pure ``scipy``/``numpy``, NO R ``REBayes`` bridge so the drift
    guard runs in CI's Python-only env), then score each wallet by the posterior tail probability
    ``P(edge > 0 | data)``. Beats the parametric normal-normal EB prior on the fat-tailed,
    heteroskedastic-precision regime (per-wallet SE varies widely with trade count).

    Per wallet: net-edge mean ``x_i`` with SE ``s_i`` (``x_i ~ N(theta_i, s_i^2)``). The mixing
    weights ``pi`` over the grid are the NPMLE of ``G``; the score is the posterior mass on
    ``theta > 0``: ``sum_k pi_k N(x_i; g_k, s_i^2) [g_k>0] / sum_k pi_k N(x_i; g_k, s_i^2)``.

    # Precondition: as EBShrinkageSkill. Wallets with < 2 effective obs (undefined SE) drop.
    # Deterministic: fixed data-driven grid + fixed EM iteration cap, no RNG.
    """

    name = "gu_koenker_npmle"

    def __init__(self, grid_size: int = 64, max_iter: int = 500, tol: float = 1e-8):
        self.grid_size = grid_size
        self.max_iter = max_iter
        self.tol = tol

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        rows = []
        for wallet, g in ss.groupby("wallet", sort=False):
            net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
            mean, sd, n_eff, _ = weighted_stats(net, weights[g.index.to_numpy()])
            se = sd / np.sqrt(n_eff) if (n_eff >= 2 and sd > 0) else np.nan
            rows.append((wallet, mean, se))
        df = pd.DataFrame(rows, columns=["wallet", "x", "se"]).set_index("wallet")
        df = df[df["se"].notna()].copy()
        if df.empty:
            df["score"], df["rank"] = [], []
            return df[["score", "rank"]]
        x = df["x"].to_numpy()
        se = df["se"].to_numpy()
        # Fixed support grid spanning the observed means (single point for a lone candidate).
        grid = np.linspace(x.min(), x.max(), self.grid_size) if len(df) > 1 else x[:1]
        # L[i, k] = N(x_i; grid_k, se_i^2).
        likelihood = norm.pdf((x[:, None] - grid[None, :]) / se[:, None]) / se[:, None]
        pi = np.full(len(grid), 1.0 / len(grid))
        for _ in range(self.max_iter):                          # EM for the Kiefer-Wolfowitz MLE
            num = likelihood * pi[None, :]
            row_sum = num.sum(axis=1, keepdims=True)
            new_pi = (num / np.where(row_sum > 0, row_sum, 1.0)).mean(axis=0)
            if np.abs(new_pi - pi).max() < self.tol:
                pi = new_pi
                break
            pi = new_pi
        marginal = likelihood @ pi
        marginal = np.where(marginal > 0, marginal, 1.0)
        pos_mass = likelihood @ (pi * (grid > 0))
        df["score"] = pos_mass / marginal
        df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
        return df[["score", "rank"]]


class ProxyCLV:
    """Closing-Line-Value skill per wallet (issue #421 ``proxy_clv``): ``CLV = close - entry`` on
    the bought outcome, where ``close`` is the last pre-resolution trade price (the suff_stats
    ``close_proxy`` column). CLV reaches significance in TENS of trades vs THOUSANDS for the
    realized $1/$0 payoff, so it directly attacks the small-sample winner's-curse. ``score`` = the
    weighted-mean-CLV t-statistic (higher = better).

    # Precondition: ``ss`` carries ``close_proxy`` (from suff_stats.materialize). Positions whose
    # outcome never traded pre-resolution (NaN close_proxy) and wallets with < 2 valid CLV
    # positions or zero dispersion are dropped — so a frame built without the close-proxy merge
    # (all-NaN) yields an empty ranking, not a crash.
    """

    name = "proxy_clv"

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        rows = []
        for wallet, g in ss.groupby("wallet", sort=False):
            valid = g["close_proxy"].notna().to_numpy()
            if valid.sum() < 2:
                continue
            clv = (g["close_proxy"] - g["price"]).to_numpy()[valid]
            mean, sd, n_eff, _ = weighted_stats(clv, weights[g.index.to_numpy()][valid])
            if not (n_eff >= 2 and sd > 0):
                continue
            rows.append((wallet, mean * np.sqrt(n_eff) / sd))
        df = pd.DataFrame(rows, columns=["wallet", "score"]).set_index("wallet")
        if df.empty:
            df["rank"] = []
            return df[["score", "rank"]]
        df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
        return df[["score", "rank"]]


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
REGISTRY: "dict[str, type]" = {
    cls.name: cls
    for cls in (EBShrinkageSkill, TStatBaseline, GuKoenkerNPMLE, ProxyCLV)
}
