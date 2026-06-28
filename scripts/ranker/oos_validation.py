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
from numba import njit
from scipy.stats import norm

from . import SuffStats


# cache=False is REQUIRED, not an oversight: numba SEGFAULTS on a *self-recursive* njit compiled with
# cache=True (the on-disk cache mishandles the self-reference). The recompile cost is a non-issue —
# the bake-off's process pool forks AFTER the parent's first call (the 8a screen) has JIT-compiled
# this, so workers inherit the compiled code copy-on-write; serial/thread runs compile once.
@njit(cache=False, fastmath=False)
def _uw_pairwise(a, start, n):
    """numpy's pairwise (divide-and-conquer) float64 reduction, replicated EXACTLY (8-accumulator
    unroll, 128-element block, half rounded down to a multiple of 8). This is the ONLY subtlety in
    making the JIT path bit-identical to the reference: a naive sequential sum would drift ~1e-16 from
    ``ndarray.sum()``; matching the reduction TREE reproduces it to the last bit."""
    if n < 8:
        s = 0.0
        for i in range(n):
            s += a[start + i]
        return s
    if n <= 128:
        r0 = a[start]; r1 = a[start+1]; r2 = a[start+2]; r3 = a[start+3]
        r4 = a[start+4]; r5 = a[start+5]; r6 = a[start+6]; r7 = a[start+7]
        i = 8
        end = n - (n % 8)
        while i < end:
            r0 += a[start+i]; r1 += a[start+i+1]; r2 += a[start+i+2]; r3 += a[start+i+3]
            r4 += a[start+i+4]; r5 += a[start+i+5]; r6 += a[start+i+6]; r7 += a[start+i+7]
            i += 8
        res = ((r0 + r1) + (r2 + r3)) + ((r4 + r5) + (r6 + r7))
        while i < n:
            res += a[start + i]
            i += 1
        return res
    half = n // 2
    half -= half % 8
    return _uw_pairwise(a, start, half) + _uw_pairwise(a, start + half, n - half)


# cache=False here too (it calls the recursive `_uw_pairwise` — see that function's note; a cached
# kernel calling an uncached recursive one is the same numba-cache footgun).
@njit(cache=False, fastmath=False)
def _uw_all_groups(s_all, e_all, bounds, w):
    """Per-wallet average-uniqueness over the contiguous wallet groups ``[bounds[g], bounds[g+1])`` of
    the wallet-sorted ``(entry_ts, ttr_ref)`` arrays; writes each label's weight into ``w`` at its
    sorted position. Mirrors the reference loop's arithmetic EXACTLY — concurrency counts are exact
    integers (held in a float64 ``c`` array — small enough to be exact), the same ``seg/c`` terms,
    summed with ``_uw_pairwise`` — so the result is bit-identical."""
    for gi in range(bounds.shape[0] - 1):
        a = bounds[gi]
        b = bounds[gi + 1]
        n = b - a
        s = s_all[a:b]
        e = e_all[a:b]
        bp = np.unique(np.concatenate((s, e)))
        if bp.size < 2:
            for k in range(n):
                w[a + k] = 1.0
            continue
        m = bp.size - 1
        lo = bp[:-1].astype(np.float64)
        hi = bp[1:].astype(np.float64)
        mid = (lo + hi) / 2.0
        c = np.empty(m)
        for j in range(m):
            cnt = 0
            for i in range(n):
                if s[i] <= mid[j] and mid[j] < e[i]:
                    cnt += 1
            c[j] = cnt if cnt >= 1 else 1
        seg = hi - lo
        sj = s.astype(np.float64)
        ej = e.astype(np.float64)
        buf = np.empty(m)
        for k in range(n):
            span = ej[k] - sj[k]
            if span > 0:
                cnt = 0
                for j in range(m):
                    if lo[j] >= sj[k] and hi[j] <= ej[k]:
                        buf[cnt] = seg[j] / c[j]
                        cnt += 1
                w[a + k] = _uw_pairwise(buf, 0, cnt) / span
            else:
                w[a + k] = 1.0


def uniqueness_weights(ss: SuffStats) -> np.ndarray:
    """Average-uniqueness label weights (Lopez de Prado, AFML Ch.4).

    ``w_i ~ avg(1/c_t)`` over label i's live span ``[entry_ts, ttr_ref]``, where ``c_t`` is the
    number of THIS wallet's labels live at t. Concurrency is piecewise-constant (changes only at
    label boundaries), so integrate ``1/c_t`` over the boundary grid. Normalised to mean 1, so a
    wallet whose labels heavily overlap (high concurrency) gets per-label weight < 1.

    Numba-accelerated: the per-wallet O(labels x segments) work runs in a JIT-compiled kernel
    (``_uw_all_groups``) over wallet-sorted flat arrays — ~10-17x over the reference Python loop on a
    large in-sample, which is the dominant cost of the bake-off at scale. **Bit-identical** to
    :func:`_uniqueness_weights_pyloop` (asserted by test): a STABLE sort by first-appearance wallet
    code preserves the reference's per-wallet row order, the concurrency counts are exact integers, and
    the per-label ``seg/c`` sum reproduces numpy's pairwise reduction (``_uw_pairwise``) — so the JIT
    changes only WHERE the identical float ops run, never the result. NOTE: ``_uw_pairwise`` replicates
    numpy's *internal* pairwise-summation tree (8-acc unroll, 128 block), so strict bit-identity is
    tied to the pinned numpy; ``test_numba_is_bit_identical_to_reference_loop`` (``np.array_equal``,
    incl. the recursive >128-segment path) fails loudly on any numpy change that alters it.

    # Precondition: ``ss`` has a contiguous RangeIndex (0..n-1) — call on the full materialized
    # frame or a ``reset_index(drop=True)`` slice (e.g. ``split_walkforward`` output).
    """
    if len(ss) and not ss.index.equals(pd.RangeIndex(len(ss))):
        raise ValueError("uniqueness_weights requires a contiguous RangeIndex (0..n-1); call on "
                         "the full materialized frame or a reset_index(drop=True) slice")
    if len(ss) == 0:
        return np.empty(0, dtype=np.float64)
    codes = pd.factorize(ss["wallet"].to_numpy(), sort=False)[0]   # first-appearance order
    order = np.argsort(codes, kind="stable")                       # group rows by wallet, keep row order
    s_all = ss["entry_ts"].to_numpy(np.int64)[order]
    e_all = ss["ttr_ref"].to_numpy(np.int64)[order]
    sorted_codes = codes[order]
    bounds = np.concatenate((np.array([0], dtype=np.int64),
                             (np.flatnonzero(np.diff(sorted_codes)) + 1).astype(np.int64),
                             np.array([len(ss)], dtype=np.int64)))
    w_sorted = np.empty(len(ss), dtype=np.float64)
    _uw_all_groups(s_all, e_all, bounds, w_sorted)
    w = np.empty(len(ss), dtype=np.float64)
    w[order] = w_sorted                                           # restore ORIGINAL index order
    return w / w.mean()


def _uniqueness_weights_pyloop(ss: SuffStats) -> np.ndarray:
    """Reference pure-Python implementation of :func:`uniqueness_weights` (the pre-numba loop). Kept
    private as the bit-identity ORACLE the test pins the JIT version against; not used in production.
    Per-wallet cost is O(labels x segments)."""
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
    in_sample = in_sample.reset_index(drop=True)
    forward = ss[(entry >= as_of + embargo_secs) & (entry < as_of + horizon_secs)]
    # B1 (#436): arm the LANDMINE-2 guard on the hot path, not just in tests. This is the single
    # producer of the in-sample track for both ``screen_estimators`` and ``run_trajectory``; the
    # filter above already enforces ``resolved_at <= as_of``, so this asserts the invariant and any
    # future refactor that reintroduced a post-``as_of`` row fails loudly HERE, not as a silent leak.
    assert_no_lookahead(in_sample, as_of=as_of)
    return in_sample, forward.reset_index(drop=True)


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
        # A7 (#436): refuse PBO under near-zero cross-config dispersion. When every config performs
        # ~identically the IS-best / OOS-rank machinery is meaningless, and the old `<=`-rank tie
        # rule silently returned PBO=0 (reads as "not overfit"). Emit NaN + a `degenerate` flag so
        # the winner gate fails SAFE (a flat/all-equal leaderboard cannot be certified) rather than
        # treating an undefined PBO as a pass.
        degenerate = n_cols < 2 or bool(np.ptp(np.nanmean(matrix, axis=0)) < 1e-12)
        if degenerate:
            return pd.DataFrame({"pbo": [float("nan")], "n_combinations": [0],
                                 "s_groups": [int(s)], "degenerate": [True]})
        groups = np.array_split(np.arange(n_periods), s)
        logits = []
        for is_groups in combinations(range(s), s // 2):
            is_set = set(is_groups)
            is_rows = np.concatenate([groups[g] for g in is_groups])
            oos_rows = np.concatenate([groups[g] for g in range(s) if g not in is_set])
            is_perf = matrix[is_rows].mean(axis=0)
            oos_perf = matrix[oos_rows].mean(axis=0)
            best = int(np.argmax(is_perf))
            # A7 (#436): strict-better + fractional-tie MIDRANK (1 = worst .. n_cols = best). The old
            # `(oos_perf <= oos_perf[best]).sum()` counted every config TIED with the IS-best as
            # "beaten", inflating its OOS rank to n_cols and masking overfit on tie-heavy data. A
            # tie block now shares the average rank (worse + (equal+1)/2); on tie-free data
            # equal==1 so this reduces exactly to the old `worse + 1`.
            worse = int((oos_perf < oos_perf[best]).sum())
            equal = int((oos_perf == oos_perf[best]).sum())       # includes best itself (>= 1)
            oos_rank = worse + (equal + 1) / 2.0
            omega = min(max(oos_rank / (n_cols + 1.0), 1e-6), 1.0 - 1e-6)
            logits.append(math.log(omega / (1.0 - omega)))
        logits = np.asarray(logits)
        return pd.DataFrame({"pbo": [float((logits < 0).mean())],
                             "n_combinations": [int(logits.size)], "s_groups": [int(s)],
                             "degenerate": [False]})


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

    Classify each wallet as Winner/Loser vs the cross-sectional median in two consecutive periods
    (wallets at exactly either period's median are dropped first — see the C3 note below).
    ``CPR = (WW*LL)/(WL*LW)``; CPR > 1 => persistence (winners stay winners). Significance via the
    log-CPR z-test (Christensen): ``sigma = sqrt(1/WW + 1/WL + 1/LW + 1/LL)``, ``z = ln(CPR)/sigma``.
    ``period1`` / ``period2`` are per-wallet performance Series sharing a wallet index. Returns a
    dict; ``go`` is the 1-number premise check (persistence at the 5% level).
    """
    df = pd.concat([period1.rename("p1"), period2.rename("p2")], axis=1).dropna()
    m1, m2 = df["p1"].median(), df["p2"].median()
    # C3 (#436): drop wallets sitting at EXACTLY either period's median before tabulating. Net-edge
    # performance has a zero mass-point (many wallets at exactly 0 = the median), and a bare
    # `> median` lumps every median-tied wallet into the LOSER cell — inflating LL and manufacturing a
    # spurious persistence GO (in the >50%-at-median case the WINNER cell empties and the table routes
    # to the `cpr=inf` perfect-persistence path). Dropping the ambiguous tie block balances the W/L
    # split; on tie-free continuous data it removes <= 1 wallet (the median wallet at odd n, none at
    # even n) and leaves the verdict unchanged.
    df = df[(df["p1"] != m1) & (df["p2"] != m2)]
    w1 = df["p1"] > m1
    w2 = df["p2"] > m2
    ww = int((w1 & w2).sum())
    ll = int((~w1 & ~w2).sum())
    wl = int((w1 & ~w2).sum())
    lw = int((~w1 & w2).sum())
    result = {"ww": ww, "wl": wl, "lw": lw, "ll": ll}
    if wl == 0 or lw == 0 or ww == 0 or ll == 0:
        # Degenerate contingency: the log-CPR z-test is undefined (no p-value). Certify persistence
        # (go) ONLY for genuine perfect persistence — real winners AND losers with zero reversals
        # (ww>0, ll>0, wl=lw=0 -> cpr=inf). Any EMPTY winner/loser CLASS (ww==0 or ll==0 — e.g. a
        # zero-mass-point collapsing one side even after the tie-drop) is no-signal, not persistence,
        # so go=False; the old `cpr>1` rule manufactured a spurious GO whenever wl==0 OR lw==0 (C3).
        perfect = ww > 0 and ll > 0 and wl == 0 and lw == 0
        cpr = float("inf") if perfect else ((ww * ll) / (wl * lw) if wl > 0 and lw > 0 else 0.0)
        result.update(cpr=cpr, z=float("nan"), p_value=float("nan"), go=bool(perfect))
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


def akm_inference_on_winners(estimates, ses, *, winner: "int | None" = None,
                             alpha: float = 0.05) -> dict:
    """Winner's-curse-corrected inference on the SELECTED winner (Andrews-Kitagawa-McCloskey,
    QJE 2024) — issue #421 ``akm_inference_on_winners``.

    The naive estimate of the argmax is upward-biased (the winner's curse this harness exists to
    kill). Conditioning the winner's estimate on having been selected (``Y_w >= max_{k != w} Y_k``)
    gives a truncated-normal law with lower truncation = the runner-up estimate; the
    median-unbiased point estimate and the equal-tailed conditional CI invert it. Treats the
    per-candidate estimates as approximately independent ``N(theta_k, se_k^2)`` (the conditional
    variant; the full hybrid is a pluggable menu extension).

    ``winner`` selects which arm to condition on (default ``argmax``). Issue #436 A5: the bake-off
    awards the highest-cumulative-return RW-superior config, which need not be the mean-argmax; when
    the named winner is NOT STRICTLY the max — either below it, OR an EXACT top tie ``y == lower``
    (#445 defect 7) — its selection event is not ``Y_w >= max others`` (a tie pins the truncated CDF
    at its bound), so the conditioning is ill-posed and this falls back to the honest UNCONDITIONAL
    normal CI (``conditional=False``) instead of a spurious shrinkage.

    Returns ``{winner, naive_estimate, median_unbiased, ci_lo, ci_hi, truncation, conditional}``.
    """
    estimates = np.asarray(estimates, dtype=float)
    ses = np.asarray(ses, dtype=float)
    if estimates.size == 0:
        raise ValueError("akm_inference_on_winners needs >= 1 estimate")
    w = int(np.argmax(estimates)) if winner is None else int(winner)
    y, s = float(estimates[w]), float(ses[w])
    z = float(norm.ppf(1.0 - alpha / 2.0))
    if estimates.size == 1:                                     # no competitors -> no truncation
        return {"winner": w, "naive_estimate": y, "median_unbiased": y,
                "ci_lo": y - z * s, "ci_hi": y + z * s, "truncation": float("-inf"),
                "conditional": False}
    lower = float(np.max(np.delete(estimates, w)))             # runner-up = truncation bound
    if y <= lower:                                             # A5 + #445 defect 7: not STRICTLY the
        # max (named winner below max, OR an EXACT top tie y == lower). The "selected == strict max"
        # event does not hold, so the truncated-normal law is ill-posed at the boundary (y == lower
        # pins its CDF at the truncation point); fall back to the honest UNCONDITIONAL normal CI
        # rather than a spurious brentq solve. Exact ties stay unconditional unless a tie-aware
        # selection event is implemented (out of #445 scope).
        return {"winner": w, "naive_estimate": y, "median_unbiased": y,
                "ci_lo": y - z * s, "ci_hi": y + z * s, "truncation": lower,
                "conditional": False}
    if s <= 0.0:                                               # F3 guard (#436 Phase F): a zero-SE
        return {"winner": w, "naive_estimate": y, "median_unbiased": y,   # winner makes the truncated-
                "ci_lo": y, "ci_hi": y, "truncation": lower,              # normal law a step function
                "conditional": False}                                     # (ill-posed for brentq)
    from scipy.optimize import brentq

    lo_b, hi_b = y - 20.0 * s, y + 20.0 * s

    def solve(target: float) -> float:                         # F decreasing in theta -> bracketed
        return float(brentq(lambda th: _truncated_normal_cdf(y, th, s, lower) - target,
                            lo_b, hi_b, maxiter=200, xtol=1e-10))

    return {"winner": w, "naive_estimate": y,
            "median_unbiased": solve(0.5),
            "ci_lo": solve(1.0 - alpha / 2.0), "ci_hi": solve(alpha / 2.0),
            "truncation": lower, "conditional": True}


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
    inflates non-coverage; the FCR fix sets each selected CI to level ``1 - R*q/m``, always at or
    above the nominal ``1 - q`` (so each is at least as WIDE as the unadjusted interval). The
    correction is harshest for the single cherry-picked pick (``R=1`` is widest) and relaxes toward
    the nominal as ``R -> m`` (selecting everything is no selection). ``selected`` is a boolean mask
    or an index array; ``m`` defaults to ``len(estimates)``. Returns the per-selected
    ``{index, estimate, ci_lo, ci_hi, fcr_level}``.
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
