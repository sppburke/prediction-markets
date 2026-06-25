"""Per-wallet skill estimators for the ranker bake-off (issue #421).

Each estimator conforms to the ``Estimator`` Protocol (``ranker/__init__.py``): it reads the
shared ``suff_stats`` substrate and returns a ``WalletScores`` frame (``score``: higher =
better, ``rank``: 1 = best). PR1 ships the reference template ``eb_shrinkage_skill``; the
remaining menu estimators are pluggable drop-ins behind the same Protocol (issue #421
"v1 build scope").
"""
import numpy as np
import pandas as pd
from scipy.special import logsumexp
from scipy.stats import norm

from ranker_decay import weighted_stats  # reuse #366 weighted statistics — no drift

# Per-position net-edge / CLV dispersion floor (ranker_sd_floor, glossary). np.std(ddof=1) of a
# mathematically-constant net series is exactly 0.0 at n=5 but ~1e-16 at n>=6 (mean-rounding float
# error), so a bare `sd > 0` admits a 6/6 win streak with a t-stat ~2e16, ranking it #1. A wallet's
# genuine per-position dispersion is O(1e-3) or larger (>= one price tick), so 1e-9 cleanly separates
# float noise from real signal: at or below it a wallet has undefined dispersion and is DROPPED (not
# scored maximal), extending the deliberate n=5 zero-dispersion drop to the n>=6 float-noise case
# uniformly across all five estimators (#436 A10 follow-up).
_SD_FLOOR = 1e-9

# Empirical-Bayes prior-variance floors for eb_shrinkage_skill (#436 C1).
# `ranker_eb_prior_var_floor` is the ABSOLUTE degenerate fallback (a <2-valid-candidate input has an
# undefined cross-section). `ranker_eb_tau2_floor_frac` floors the DerSimonian-Laird tau^2 estimate to
# a small positive FRACTION of the cross-sectional Var(wallet means) so that even when the
# precision-weighted tau^2 collapses to ~0 the shrinkage cannot fully flatten the ranking (the old
# `max(prior_var, 1e-9)` collapsed shrink to ~0 -> every posterior == the prior mean). 0.05 is a 5%
# backstop: it binds ONLY when tau^2 ~ 0 (in normal operation the DSL estimate exceeds it and it never
# binds), and is small enough to preserve aggressive shrinkage.
_EB_PRIOR_VAR_FLOOR = 1e-9
_EB_TAU2_FLOOR_FRAC = 0.05


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
        # NaN a zero-dispersion wallet's sampling variance (constant net series -> sd 0, or ~1e-16
        # float noise at n>=6) so its posterior is undefined and it drops, not scores a spurious max
        # (#436 A10 follow-up; same `_SD_FLOOR` drop as t_stat / gu_koenker / clv).
        se2 = ((df["sd"] ** 2) / df["n_eff"]).where(df["sd"] > _SD_FLOOR)  # sampling var of the mean
        # EB prior variance tau^2 by the DerSimonian-Laird precision-weighted (1/se^2) positive-part
        # moment estimator (#436 C1), over the VALID (finite-se^2) wallets only — this reconciles the
        # B-phase se2 NaN-mask: the zero-dispersion wallets that drop downstream must not enter the
        # tau^2 cross-section either. The old `Var(means) - mean(se^2)` subtracted the UNWEIGHTED mean
        # sampling variance, which a handful of short-track (n<=4, huge se^2) wallets inflate until it
        # drives prior_var negative; `max(., 1e-9)` then collapsed shrink to ~0 and flattened the whole
        # ranking. DSL weights each wallet by its precision so the noisy short-track wallets cannot drag
        # tau^2 down, and the positive-part keeps it >= 0. Floor to a small fraction of the cross-
        # sectional Var(means) so a genuine-zero tau^2 still leaves a usable (non-degenerate) ranking;
        # fall back to the absolute floor for a <2-valid-candidate input (undefined cross-section). mu0
        # stays the unweighted cross-sectional mean — a location estimate is well-defined for every
        # candidate; only tau^2 (which needs se^2) restricts to the valid subset.
        valid = se2.notna().to_numpy()
        xv = df["mean"].to_numpy()[valid]
        s2 = se2.to_numpy()[valid]
        if xv.size >= 2:
            w = 1.0 / s2                                         # precision weights
            sw = w.sum()
            xbar = (w * xv).sum() / sw                           # precision-weighted grand mean
            q = (w * (xv - xbar) ** 2).sum()                     # weighted SS; E[Q] = k-1 under tau^2=0
            c = sw - (w ** 2).sum() / sw                         # > 0 for k >= 2
            tau2_dsl = max((q - (xv.size - 1)) / c, 0.0)         # DerSimonian-Laird positive-part MoM
            var_means = float(np.var(xv, ddof=1))
            floor = (_EB_TAU2_FLOOR_FRAC * var_means
                     if np.isfinite(var_means) and var_means > 0 else _EB_PRIOR_VAR_FLOOR)
            tau2 = max(tau2_dsl, floor)
        else:
            tau2 = _EB_PRIOR_VAR_FLOOR                           # <2 valid -> deterministic fallback
        mu0 = df["mean"].mean()                                  # EB prior mean (all candidates)
        shrink = tau2 / (tau2 + se2)
        post_mean = mu0 + shrink * (df["mean"] - mu0)
        post_sd = np.sqrt(shrink * se2)            # NaN se2 (zero-dispersion) -> NaN score -> dropped
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
        # A10 (#436): zero-dispersion wallets (sd <= _SD_FLOOR) are DROPPED here, not ranked #1 with
        # +inf. This is deliberate and winner's-curse-robust: a constant streak (5/5 -> sd 0.0, 6/6
        # -> sd ~1e-16 float noise) has an UNDEFINED — not maximal — t-stat, and admitting it top
        # would reinstate exactly the small-sample curse this harness exists to kill. `_SD_FLOOR`
        # (not a bare `> 0`) is what catches the n>=6 float-noise case. The downstream DSR gate
        # mirrors this (bakeoff._zero_dispersion_positive only force-keeps an already-scored wallet).
        df["score"] = (df["mean"] / se).where((df["n_eff"] >= 2) & (df["sd"] > _SD_FLOOR))
        df = df[df["score"].notna()].copy()
        df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
        return df[["score", "rank"]]


def _npmle_em(log_like: np.ndarray, *, max_iter: int, tol: float) -> "tuple[np.ndarray, list]":
    """Kiefer-Wolfowitz NPMLE mixing weights by EM in LOG space (#436 C2).

    ``log_like[i, k] = log N(x_i; grid_k, se_i^2)``. Returns ``(pi, ll_history)`` — the grid mixing
    distribution and the per-iteration marginal log-likelihood. The E-step normalises via log-sum-exp
    so a wallet whose mean falls many ``se`` from every grid node keeps well-defined responsibilities
    instead of underflowing its whole likelihood row to 0.0 (which the old linear EM then scored 0.0
    regardless of skill). The EM marginal log-likelihood is monotone non-decreasing — the drift guard
    asserts ``ll_history`` never falls, catching a broken E/M step (a separate guard pins the verdict
    invariant to the iteration cap).
    """
    pi = np.full(log_like.shape[1], 1.0 / log_like.shape[1])
    ll_history: list = []
    for _ in range(max_iter):
        with np.errstate(divide="ignore"):
            log_pi = np.log(pi)                                  # -inf where pi == 0 (NPMLE is sparse)
        log_post = log_like + log_pi[None, :]
        log_row = logsumexp(log_post, axis=1)                   # log marginal per wallet
        ll_history.append(float(log_row.sum()))
        resp = np.exp(log_post - log_row[:, None])              # E-step responsibilities
        new_pi = resp.mean(axis=0)                              # M-step
        if np.abs(new_pi - pi).max() < tol:
            pi = new_pi
            break
        pi = new_pi
    return pi, ll_history


def _npmle_scores(x: np.ndarray, se: np.ndarray, grid: np.ndarray, *, max_iter: int,
                  tol: float) -> "tuple[np.ndarray, list]":
    """Posterior ``P(theta > 0 | data)`` per wallet under the EM-fit NPMLE prior (#436 C2).

    Builds the log-likelihood matrix, fits ``pi`` via ``_npmle_em``, and forms the positive-tail
    posterior in LOG space (so it never underflows a far-from-grid precise wallet to 0.0). The
    ``grid == 0`` boundary is made deterministic by SPLITTING its mass 0.5/0.5 across the positive and
    non-positive sides (``pos_w``), so a grid node landing exactly on 0 cannot silently flip the score.
    Returns ``(scores, ll_history)``; a wallet with no positive grid mass scores 0, all-positive scores 1.
    """
    log_like = norm.logpdf((x[:, None] - grid[None, :]) / se[:, None]) - np.log(se[:, None])
    pi, ll_history = _npmle_em(log_like, max_iter=max_iter, tol=tol)
    pos_w = np.where(grid > 0, 1.0, np.where(grid < 0, 0.0, 0.5))   # split boundary mass at grid == 0
    with np.errstate(divide="ignore", invalid="ignore"):
        log_pi = np.log(pi)
        log_posw = np.log(pos_w)                                # -inf where pos_w == 0
        log_post = log_like + log_pi[None, :]
        log_marg = logsumexp(log_post, axis=1)                  # finite (pi sums to 1)
        log_pos = logsumexp(log_post + log_posw[None, :], axis=1)   # -inf if no positive mass
        scores = np.exp(log_pos - log_marg)                    # in [0, 1]: 0 = no positive mass, 1 = all
    return scores, ll_history


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
    The EM E-step and the posterior are computed in LOG space (log-sum-exp) so a far-from-grid precise
    wallet is not underflowed to score 0.0, and the ``g_k == 0`` boundary splits its mass 0.5/0.5 so
    ``[g_k > 0]`` is deterministic (#436 C2). ``max_iter`` is a safety ceiling — the ``tol`` break
    converges well before it on real data; the drift guard pins the verdict invariant to the cap.

    # Precondition: as EBShrinkageSkill. Wallets with < 2 effective obs (undefined SE) drop.
    # Deterministic: fixed data-driven grid + fixed EM iteration cap, no RNG.
    """

    name = "gu_koenker_npmle"

    def __init__(self, grid_size: int = 64, max_iter: int = 2000, tol: float = 1e-8):
        self.grid_size = grid_size
        self.max_iter = max_iter
        self.tol = tol

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        rows = []
        for wallet, g in ss.groupby("wallet", sort=False):
            net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
            mean, sd, n_eff, _ = weighted_stats(net, weights[g.index.to_numpy()])
            se = sd / np.sqrt(n_eff) if (n_eff >= 2 and sd > _SD_FLOOR) else np.nan
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
        scores, _ = _npmle_scores(x, se, grid, max_iter=self.max_iter, tol=self.tol)
        df["score"] = scores
        df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
        return df[["score", "rank"]]


def _weighted_clv_tstat(
    ss: pd.DataFrame, weights: np.ndarray, close_col: str
) -> pd.DataFrame:
    """Shared per-wallet weighted-CLV t-stat ranking for the CLV estimators (``proxy_clv`` /
    ``true_clv``). ``CLV = close - entry`` on the bought outcome, ``close`` read from ``close_col``.

    Positions with a NaN ``close`` are dropped; wallets with < 2 valid positions or zero dispersion
    are skipped — so an all-NaN ``close_col`` (the close source absent for the run) yields an empty
    ranking, not a crash. ``score`` = ``mean·√n_eff / sd`` (the weighted t-statistic; higher =
    better). CLV reaches significance in TENS of trades vs THOUSANDS for the realized $1/$0 payoff,
    so it directly attacks the small-sample winner's-curse.
    """
    rows = []
    for wallet, g in ss.groupby("wallet", sort=False):
        valid = g[close_col].notna().to_numpy()
        if valid.sum() < 2:
            continue
        clv = (g[close_col] - g["price"]).to_numpy()[valid]
        mean, sd, n_eff, _ = weighted_stats(clv, weights[g.index.to_numpy()][valid])
        if not (n_eff >= 2 and sd > _SD_FLOOR):     # zero-dispersion (constant CLV) -> undefined t
            continue
        rows.append((wallet, mean * np.sqrt(n_eff) / sd))
    df = pd.DataFrame(rows, columns=["wallet", "score"]).set_index("wallet")
    if df.empty:
        df["rank"] = []
        return df[["score", "rank"]]
    df["rank"] = df["score"].rank(ascending=False, method="first").astype(int)
    return df[["score", "rank"]]


class ProxyCLV:
    """Closing-Line-Value skill per wallet (issue #421 ``proxy_clv``): ``CLV = close - entry`` on
    the bought outcome, where ``close`` is the last pre-resolution trade price (the suff_stats
    ``close_proxy`` column).

    # Precondition: ``ss`` carries ``close_proxy`` (from suff_stats.materialize). Positions whose
    # outcome never traded pre-resolution (NaN close_proxy) and wallets with < 2 valid CLV
    # positions or zero dispersion are dropped — so a frame built without the close-proxy merge
    # (all-NaN) yields an empty ranking, not a crash.
    """

    name = "proxy_clv"

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        return _weighted_clv_tstat(ss, weights, "close_proxy")


class TrueCLV:
    """True closing-line-value skill per wallet (issue #429 PR4): ``CLV = close - entry`` on the
    bought outcome, where ``close`` is the CLOB mid at/just-before the market CLOSE (the suff_stats
    ``true_clv_close`` column) — pinned to ``t ≤ LEAST(COALESCE(end_date_unix, resolved_at_unix),
    resolved_at_unix)`` (issue #436 B5: capped at resolution, so an early-resolved in-sample market
    cannot pull a post-``as_of`` close) rather than ``proxy_clv``'s last pre-resolution *trade*
    price. The CLOB series is best-effort (PR2's
    measured ~63.6% ceiling), so positions with no series get a NaN ``true_clv_close`` and are
    dropped; an all-NaN column (CLOB views absent / pre-backfill) yields an empty ranking,
    eliminated downstream — not a crash.

    # Precondition: ``ss`` carries ``true_clv_close`` (from suff_stats.materialize).
    """

    name = "true_clv"

    def score(self, ss: pd.DataFrame, *, as_of: int, weights: np.ndarray) -> pd.DataFrame:
        return _weighted_clv_tstat(ss, weights, "true_clv_close")


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
REGISTRY: "dict[str, type]" = {
    cls.name: cls
    for cls in (EBShrinkageSkill, TStatBaseline, GuKoenkerNPMLE, ProxyCLV, TrueCLV)
}
