"""The honesty layer for the ranker bake-off (issue #421).

Makes the bake-off's results trustworthy:
  * ``uniqueness_weights`` (Lopez de Prado, AFML Ch.4) — down-weight co-trading-overlapping
    labels so co-trading-aware estimators don't learn "crowded = good" (LOAD-BEARING prereq).
  * ``split_walkforward`` / ``assert_no_lookahead`` — the per-``as_of`` look-ahead guard
    (LANDMINE-2): the in-sample track only sees labels ALREADY resolved by T.
  * grid-level Validators ``PBO`` (CSCV) / ``RomanoWolf`` (StepM) / ``HansenSPA`` — the bake-off
    over N configs is itself multiple-testing (LANDMINE-1); these deflate the leaderboard at the
    pre-registered ``N_GRID``. ``RomanoWolf`` / ``HansenSPA`` wrap ``arch.bootstrap``.
  * ``brown_goetzmann_cpr`` — a 1-number persistence go/no-go on the whole premise (scaffolding).
  * ``paper_fills_crosscheck`` — compare a harness leaderboard against the live deployed cohort.

Numerics here are statistical-method parameters (significance level, bootstrap reps/seed, CSCV
group count) or honest sentinels (``embargo_secs=0``), NOT strategy thresholds; the selection
defaults (MinTRL, embargo) live in ``Criteria`` / the bake-off grid and are glossary'd in PR6.
"""
import math
from itertools import combinations

import numpy as np
import pandas as pd
from scipy.stats import norm

from . import SuffStats


def uniqueness_weights(ss: SuffStats) -> np.ndarray:
    """Average-uniqueness label weights (Lopez de Prado, AFML Ch.4).

    ``w_i ~ avg(1/c_t)`` over label i's live span ``[entry_ts, ttr_ref]``, where ``c_t`` is the
    number of THIS wallet's labels live at t. Concurrency is piecewise-constant (changes only at
    label boundaries), so integrate ``1/c_t`` over the boundary grid. Normalised to mean 1, so a
    wallet whose labels heavily overlap (high concurrency) gets per-label weight < 1.

    # Precondition: ``ss`` has a contiguous RangeIndex (0..n-1) — call on the full materialized
    # frame or a ``reset_index(drop=True)`` slice (e.g. ``split_walkforward`` output). Per-wallet
    # cost is O(labels x segments); cap pathological whale wallets if memory-bound (#421 note).
    """
    if len(ss) and not ss.index.equals(pd.RangeIndex(len(ss))):
        raise ValueError("uniqueness_weights requires a contiguous RangeIndex (0..n-1); call on "
                         "the full materialized frame or a reset_index(drop=True) slice")
    w = np.empty(len(ss))
    for _, g in ss.groupby("wallet", sort=False):
        idx = g.index.to_numpy()
        s = g["entry_ts"].to_numpy(np.int64)
        e = g["ttr_ref"].to_numpy(np.int64)
        bp = np.unique(np.concatenate([s, e]))                 # boundary grid
        if bp.size < 2:                                        # all labels collapse to one instant
            w[idx] = 1.0
            continue
        lo, hi = bp[:-1], bp[1:]                               # half-open segments [lo, hi)
        mid = (lo + hi) / 2.0
        c = np.maximum(((s[:, None] <= mid) & (mid < e[:, None])).sum(0), 1)  # concurrency / segment
        seg = (hi - lo).astype(np.float64)
        for k in range(len(idx)):
            m = (lo >= s[k]) & (hi <= e[k])                    # segments covered by label k
            span = float(e[k] - s[k])
            w[idx[k]] = float((seg[m] / c[m]).sum()) / span if span > 0 else 1.0
    return w / w.mean()


def assert_no_lookahead(ss: SuffStats, *, as_of: int) -> None:
    """Raise if any row resolves after ``as_of`` (LANDMINE-2). The ``Estimator`` Protocol relies
    on the harness having already filtered the in-sample track to ``resolved_at <= as_of``."""
    bad = int((ss["resolved_at"] > as_of).sum())
    if bad:
        raise ValueError(f"look-ahead leak: {bad} rows with resolved_at > as_of={as_of}")


def split_walkforward(ss: SuffStats, *, as_of: int, train_secs: int, horizon_secs: int,
                      embargo_secs: int = 0) -> "tuple[SuffStats, SuffStats]":
    """Rolling-origin walk-forward split at cutoff ``as_of`` with the LANDMINE-2 look-ahead guard.

    in_sample = labels entered in ``[as_of - train_secs, as_of)`` AND already resolved by
    ``as_of`` (no in-sample outcome is unknowable as of T; this is also the purge — a label
    resolving after T would overlap the forward period).
    forward    = labels entered in ``[as_of + embargo_secs, as_of + horizon_secs)`` (the embargo
    is a gap after T to avoid boundary leakage; default 0 = standard walk-forward, an honest
    no-op sentinel).

    Both are returned with a fresh RangeIndex so per-step ``uniqueness_weights`` and the
    estimator's positional ``weights[g.index]`` align.
    """
    entry = ss["entry_ts"]
    in_sample = ss[(entry >= as_of - train_secs) & (entry < as_of) & (ss["resolved_at"] <= as_of)]
    forward = ss[(entry >= as_of + embargo_secs) & (entry < as_of + horizon_secs)]
    return in_sample.reset_index(drop=True), forward.reset_index(drop=True)


class PBO:
    """Probability of Backtest Overfitting via CSCV (Bailey-Borwein-LdP-Zhu, J.Comp.Fin 2017).

    Splits the per-period performance matrix into ``s_groups`` even contiguous sub-periods; for
    every way to choose half as in-sample (IS) vs the complement out-of-sample (OOS): rank configs
    by IS mean, take the IS-best, and record the logit of its OOS rank. PBO = fraction of
    combinations where the IS-best lands below the OOS median (logit < 0). High PBO => the
    selection procedure overfits (the IS winner does not generalise).

    ``assess`` input: ``leaderboard`` = per-period return matrix (index = period, columns =
    config). ``n_configs`` is part of the Protocol but PBO reads the matrix shape directly.
    """

    name = "pbo"

    def __init__(self, s_groups: int = 16):
        self.s_groups = s_groups

    def assess(self, leaderboard: pd.DataFrame, *, n_configs: int) -> pd.DataFrame:
        matrix = leaderboard.to_numpy(dtype=float)
        n_periods, n_cols = matrix.shape
        s = min(self.s_groups, n_periods)
        if s % 2 == 1:
            s -= 1
        if s < 2:
            raise ValueError("PBO needs >= 2 periods")
        groups = np.array_split(np.arange(n_periods), s)
        logits = []
        for is_groups in combinations(range(s), s // 2):
            is_set = set(is_groups)
            is_rows = np.concatenate([groups[g] for g in is_groups])
            oos_rows = np.concatenate([groups[g] for g in range(s) if g not in is_set])
            is_perf = matrix[is_rows].mean(axis=0)
            oos_perf = matrix[oos_rows].mean(axis=0)
            best = int(np.argmax(is_perf))
            oos_rank = float((oos_perf <= oos_perf[best]).sum())   # 1 = worst .. n_cols = best
            omega = min(max(oos_rank / (n_cols + 1.0), 1e-6), 1.0 - 1e-6)
            logits.append(math.log(omega / (1.0 - omega)))
        logits = np.asarray(logits)
        return pd.DataFrame({"pbo": [float((logits < 0).mean())],
                             "n_combinations": [int(logits.size)], "s_groups": [int(s)]})


class RomanoWolf:
    """Romano-Wolf stepwise multiple testing (StepM) via ``arch.bootstrap.StepM``.

    Identifies which configs significantly beat ``benchmark`` (higher mean forward return) while
    controlling the family-wise error rate at ``size`` over all configs. arch operates on LOSSES
    (lower = better), so per-period returns are negated.

    ``assess`` input: ``leaderboard`` = per-period return matrix (index = period, columns =
    config), including the ``benchmark`` column.
    """

    name = "romano_wolf"

    def __init__(self, benchmark: str, *, size: float = 0.05, reps: int = 1000,
                 block_size: "int | None" = None, seed: int = 0):
        self.benchmark = benchmark
        self.size = size
        self.reps = reps
        self.block_size = block_size
        self.seed = seed

    def assess(self, leaderboard: pd.DataFrame, *, n_configs: int) -> pd.DataFrame:
        from arch.bootstrap import StepM

        bench = -leaderboard[self.benchmark]                       # loss = -return
        models = -leaderboard.drop(columns=[self.benchmark])
        kwargs = dict(size=self.size, reps=self.reps, seed=self.seed)
        if self.block_size is not None:
            kwargs["block_size"] = self.block_size
        stepm = StepM(bench, models, **kwargs)
        stepm.compute()
        superior = set(stepm.superior_models)
        return pd.DataFrame({"config": list(models.columns),
                             "beats_benchmark": [c in superior for c in models.columns]})


class HansenSPA:
    """Hansen's Superior Predictive Ability test via ``arch.bootstrap.SPA``.

    Tests H0: no config beats ``benchmark`` once all configs are accounted for. Reports the
    'consistent' p-value (plus 'lower'/'upper' bounds). arch operates on LOSSES, so returns are
    negated. A low p-value => at least one config genuinely beats the benchmark.
    """

    name = "hansen_spa"

    def __init__(self, benchmark: str, *, reps: int = 1000,
                 block_size: "int | None" = None, seed: int = 0):
        self.benchmark = benchmark
        self.reps = reps
        self.block_size = block_size
        self.seed = seed

    def assess(self, leaderboard: pd.DataFrame, *, n_configs: int) -> pd.DataFrame:
        from arch.bootstrap import SPA

        bench = -leaderboard[self.benchmark]
        models = -leaderboard.drop(columns=[self.benchmark])
        kwargs = dict(reps=self.reps, seed=self.seed)
        if self.block_size is not None:
            kwargs["block_size"] = self.block_size
        spa = SPA(bench, models, **kwargs)
        spa.compute()
        pv = spa.pvalues
        return pd.DataFrame({"spa_pvalue_consistent": [float(pv["consistent"])],
                             "spa_pvalue_lower": [float(pv["lower"])],
                             "spa_pvalue_upper": [float(pv["upper"])]})


def brown_goetzmann_cpr(period1: pd.Series, period2: pd.Series) -> dict:
    """Cross-Product Ratio performance-persistence test (Brown-Goetzmann, RFS 1995).

    Classify each wallet as Winner/Loser vs the cross-sectional median in two consecutive periods.
    ``CPR = (WW*LL)/(WL*LW)``; CPR > 1 => persistence (winners stay winners). Significance via the
    log-CPR z-test (Christensen): ``sigma = sqrt(1/WW + 1/WL + 1/LW + 1/LL)``, ``z = ln(CPR)/sigma``.
    ``period1`` / ``period2`` are per-wallet performance Series sharing a wallet index. Returns a
    dict; ``go`` is the 1-number premise check (persistence at the 5% level).
    """
    df = pd.concat([period1.rename("p1"), period2.rename("p2")], axis=1).dropna()
    w1 = df["p1"] > df["p1"].median()
    w2 = df["p2"] > df["p2"].median()
    ww = int((w1 & w2).sum())
    ll = int((~w1 & ~w2).sum())
    wl = int((w1 & ~w2).sum())
    lw = int((~w1 & w2).sum())
    result = {"ww": ww, "wl": wl, "lw": lw, "ll": ll}
    if wl == 0 or lw == 0 or ww == 0 or ll == 0:
        # Degenerate contingency table: the z-test is undefined (no p-value). cpr is +inf for
        # zero reversals (wl=lw=0 -> maximal persistence -> go) or 0 for a zero same-state cell;
        # `cpr > 1.0` is correct for both (inf > 1 is True), so do NOT gate `go` on isfinite.
        cpr = (ww * ll) / (wl * lw) if wl > 0 and lw > 0 else float("inf")
        result.update(cpr=cpr, z=float("nan"), p_value=float("nan"), go=bool(cpr > 1.0))
        return result
    cpr = (ww * ll) / (wl * lw)
    sigma = math.sqrt(1.0 / ww + 1.0 / wl + 1.0 / lw + 1.0 / ll)
    z = math.log(cpr) / sigma
    p = 2.0 * (1.0 - float(norm.cdf(abs(z))))
    result.update(cpr=cpr, z=z, p_value=p, go=bool(cpr > 1.0 and p < 0.05))
    return result


def paper_fills_crosscheck(leaderboard: pd.DataFrame, realized_pnl: pd.DataFrame, *,
                           wallet_col: str = "wallet", pnl_col: str = "realized_pnl") -> pd.DataFrame:
    """Cross-check a harness leaderboard against the live deployed cohort's realized P&L.

    For each leaderboard wallet the live cohort actually traded, report its live realized P&L and
    flag a disagreement when it is negative (a candidate winner's-curse the harness should have
    caught). Pure compare; the live ``realized_pnl`` fetch (Supabase ``paper_fills``) is wired in
    PR5. Returns one row per overlapping wallet.
    """
    lb = leaderboard.set_index(wallet_col) if wallet_col in leaderboard.columns else leaderboard
    live = realized_pnl.set_index(wallet_col)[pnl_col]
    overlap = lb.index.intersection(live.index)
    out = pd.DataFrame({wallet_col: list(overlap)})
    out["live_realized_pnl"] = live.loc[overlap].to_numpy()
    out["harness_rank"] = (lb.loc[overlap, "rank"].to_numpy() if "rank" in lb.columns
                           else np.full(len(overlap), np.nan))
    out["disagree"] = out["live_realized_pnl"] < 0
    return out


def _truncated_normal_cdf(y: float, theta: float, sigma: float, lower: float) -> float:
    """CDF at ``y`` of ``N(theta, sigma^2)`` truncated to ``[lower, inf)``. Monotonically
    DECREASING in ``theta`` (more skill shifts mass right), which is what makes the conditional
    estimate/CI invertible by a 1-D root find. The degenerate ``0/0`` (winner essentially
    impossible absent truncation) returns 1.0, keeping the root bracket's low end positive."""
    a = (lower - theta) / sigma
    den = 1.0 - float(norm.cdf(a))
    if den <= 0.0:
        return 1.0
    return (float(norm.cdf((y - theta) / sigma)) - float(norm.cdf(a))) / den


def akm_inference_on_winners(estimates, ses, *, alpha: float = 0.05) -> dict:
    """Winner's-curse-corrected inference on the SELECTED winner (Andrews-Kitagawa-McCloskey,
    QJE 2024) — issue #421 ``akm_inference_on_winners``.

    The naive estimate of the argmax is upward-biased (the winner's curse this harness exists to
    kill). Conditioning the winner's estimate on having been selected (``Y_w >= max_{k != w} Y_k``)
    gives a truncated-normal law with lower truncation = the runner-up estimate; the
    median-unbiased point estimate and the equal-tailed conditional CI invert it. Treats the
    per-candidate estimates as approximately independent ``N(theta_k, se_k^2)`` (the conditional
    variant; the full hybrid is a pluggable menu extension).

    Returns ``{winner, naive_estimate, median_unbiased, ci_lo, ci_hi, truncation}``.
    """
    estimates = np.asarray(estimates, dtype=float)
    ses = np.asarray(ses, dtype=float)
    if estimates.size == 0:
        raise ValueError("akm_inference_on_winners needs >= 1 estimate")
    w = int(np.argmax(estimates))
    y, s = float(estimates[w]), float(ses[w])
    z = float(norm.ppf(1.0 - alpha / 2.0))
    if estimates.size == 1:                                     # no competitors -> no truncation
        return {"winner": w, "naive_estimate": y, "median_unbiased": y,
                "ci_lo": y - z * s, "ci_hi": y + z * s, "truncation": float("-inf")}
    from scipy.optimize import brentq

    lower = float(np.max(np.delete(estimates, w)))             # runner-up = truncation bound
    lo_b, hi_b = y - 20.0 * s, y + 20.0 * s

    def solve(target: float) -> float:                         # F decreasing in theta -> bracketed
        return float(brentq(lambda th: _truncated_normal_cdf(y, th, s, lower) - target,
                            lo_b, hi_b, maxiter=200, xtol=1e-10))

    return {"winner": w, "naive_estimate": y,
            "median_unbiased": solve(0.5),
            "ci_lo": solve(1.0 - alpha / 2.0), "ci_hi": solve(alpha / 2.0),
            "truncation": lower}


def mrsw_rank_cs(estimates, ses, *, tau: int, alpha: float = 0.05) -> pd.DataFrame:
    """Top-``tau`` rank confidence set (Mogstad-Romano-Shaikh-Wilhelm, ReStud 2024) — issue #421
    ``mrsw_rank_cs``. A wallet ``j`` is in the top-``tau`` CS iff we cannot conclude that ``tau`` or
    more others are strictly better: it stays in unless ``n_sig_better >= tau``, where a competitor
    ``k`` counts as significantly better when ``(est_k - est_j)/sqrt(se_k^2+se_j^2)`` exceeds a
    Bonferroni one-sided critical value over the ``m-1`` comparisons. Guards against hard-cutting a
    candidate whose rank is statistically indistinct from the top (issue #421: "if the top-25 CS
    holds 400 wallets -> widen/weight, don't hard-cut").

    Returns one row per candidate ``{index, point_rank, n_sig_better, in_top_tau_cs}``.

    # Note: the marginal CS, implemented directly from the MRSW construction — the cited
    # ``csranks`` package is not available on PyPI, so there is no bridge to reproduce.
    """
    est = np.asarray(estimates, dtype=float)
    se = np.asarray(ses, dtype=float)
    m = est.size
    if m == 0:
        return pd.DataFrame(columns=["index", "point_rank", "n_sig_better", "in_top_tau_cs"])
    crit = float(norm.ppf(1.0 - alpha / max(m - 1, 1)))
    point_rank = np.empty(m, dtype=int)
    n_better = np.empty(m, dtype=int)
    in_cs = np.empty(m, dtype=bool)
    for j in range(m):
        sd = np.sqrt(se ** 2 + se[j] ** 2)
        sd[j] = np.inf                                          # exclude self (z_jj = 0)
        z = (est - est[j]) / sd
        n_sig = int((z > crit).sum())
        point_rank[j] = 1 + int((est > est[j]).sum())
        n_better[j] = n_sig
        in_cs[j] = n_sig < tau
    return pd.DataFrame({"index": np.arange(m), "point_rank": point_rank,
                         "n_sig_better": n_better, "in_top_tau_cs": in_cs})


def fcr_selected_ci(estimates, ses, selected, *, q: float = 0.05,
                    m: "int | None" = None) -> pd.DataFrame:
    """False-Coverage-Rate-adjusted CIs for a SELECTED set (Benjamini-Yekutieli, JASA 2005) —
    issue #421 ``fcr_selected_ci``. Reporting a CI only for the ``R`` selected of ``m`` candidates
    inflates non-coverage; the FCR fix widens each selected CI to level ``1 - R*q/m`` (so more
    selections -> wider intervals). ``selected`` is a boolean mask or an index array; ``m`` defaults
    to ``len(estimates)``. Returns ``{index, estimate, ci_lo, ci_hi, fcr_level}`` per selected.
    """
    estimates = np.asarray(estimates, dtype=float)
    ses = np.asarray(ses, dtype=float)
    sel = np.asarray(selected)
    idx = np.where(sel)[0] if sel.dtype == bool else sel.astype(int)
    m = estimates.size if m is None else m
    r = idx.size
    if r == 0:
        return pd.DataFrame(columns=["index", "estimate", "ci_lo", "ci_hi", "fcr_level"])
    level = 1.0 - (r * q) / m
    z = float(norm.ppf(1.0 - (1.0 - level) / 2.0))
    return pd.DataFrame({"index": idx, "estimate": estimates[idx],
                         "ci_lo": estimates[idx] - z * ses[idx],
                         "ci_hi": estimates[idx] + z * ses[idx],
                         "fcr_level": level})


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
VALIDATOR_REGISTRY: "dict[str, type]" = {
    PBO.name: PBO, RomanoWolf.name: RomanoWolf, HansenSPA.name: HansenSPA,
}
