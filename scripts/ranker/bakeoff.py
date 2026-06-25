"""The bake-off driver — staged sweep over the pre-registered grid (issue #421, PR5, step 8).

Composes the harness modules (estimators / deflation / selectors / demotion / policies /
oos_validation) into the three-stage sweep that emits the §Deliverable:

  (8a) estimator screen on cheap point-in-time forward copy P&L — eliminate most estimators;
  (8b) PRE-REGISTER ``N_GRID`` over the surviving axes x criteria x the 4 policies (LANDMINE-1:
       the grid-level deflation is only valid against a trial count fixed BEFORE scoring);
  (8c) run the survivors as full walk-forward TRAJECTORIES (the policy axis calls ``pe-backtest``
       per step via an injected ``BacktestRunner``) -> a grid-deflated leaderboard -> a recommended
       winner (with AKM/MRSW/FCR winner's-curse uncertainty) or an explicit NO-GO, cross-checked
       against the live cohort's ``paper_fills`` realized P&L.

The bake-off RUN is an OPERATOR step (it shells out to the Rust ``pe-backtest`` over the 136GB
cache); ``main`` is that entry point. CI exercises the stage functions on synthetic inputs with a
fake ``BacktestRunner`` (no cache, no network) — every stage is a plain function over frames.

Honest sentinels (issue #421 "stub/deferred"): the per-wallet activity / MinTRL gates default to
permissive (``active_within_secs=0`` -> no recency gate; ``min_trl=0`` -> no length gate) so a
default-constructed grid point does not silently drop candidates; the canonical defaults land in
PR6's ``_GLOSSARY``.
"""
import argparse
import itertools
import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import kurtosis, skew

from ranker_decay import weighted_stats  # reuse #366 weighted statistics — gate agrees with ranker

from . import Criteria, FollowSet, SuffStats, WalletScores
from .demotion import EmpiricalBernsteinDemoter
from .deflation import DeflatedSharpe
from .estimators import REGISTRY as ESTIMATOR_REGISTRY
from .oos_validation import (
    HansenSPA,
    PBO,
    RomanoWolf,
    akm_inference_on_winners,
    brown_goetzmann_cpr,
    fcr_selected_ci,
    mrsw_rank_cs,
    paper_fills_crosscheck,
    split_walkforward,
    uniqueness_weights,
)
from .policies import FullRerank, HybridDisplacement, KnockoutBackfill, OnlineWeighting
from .selectors import OnlineExpWeights

_PNL_COLUMNS = ["wallet", "period_end", "realized_pnl", "unrealized_pnl", "n_fills", "notional"]
# The §Acceptance benchmark estimator + baseline policy (issue #421: beat t_stat_baseline).
BASELINE_ESTIMATOR = "t_stat_baseline"
BASELINE_POLICY = "policy_full_rerank"
NO_DEFLATION = "none"

# Glossary-canonical selection thresholds (docs/_GLOSSARY.md is the single source of truth; these
# named constants mirror it so the gate reads a documented name, never a bare literal — issue #436
# A6, which found `ranker_pbo_max`/`ranker_dsr_min` were glossary'd but hardcoded here and ignored).
RANKER_FDR_Q = 0.05          # ranker_fdr_q     — family significance / alpha for the winner gate
RANKER_PBO_MAX = 0.5         # ranker_pbo_max   — leaderboard PBO (overfit) ceiling [HARD GATE]
RANKER_DSR_MIN = 0.5         # ranker_dsr_min   — per-step per-wallet Deflated-Sharpe gate [HARD]
# ranker_grid_dsr_min is ADVISORY (reported, not gated): the RW/SPA/PBO panel already deflates the
# leaderboard, so gating the per-config grid-DSR on top double-counts the correction and rejects
# genuinely-good configs (a +0.8/period config deflates to DSR≈0.93 at N=20 — below 0.95). The
# winner's grid-DSR is surfaced + flagged when below this bar so the operator reads it (#436 A6).
RANKER_GRID_DSR_MIN = 0.95   # ranker_grid_dsr_min — leaderboard grid-DSR advisory floor [REPORT]


# ───────────────────────── grid (8b: pre-registration) ─────────────────────────
@dataclass(frozen=True)
class GridPoint:
    """One pre-registered config. The selector axis is coupled into the policy in v1 (hard
    policies select top-k by rank; ``policy_online_weighting`` uses the online selector), so the
    grid axes are estimator x deflator x policy x criteria x churn_cost."""

    estimator: str
    deflator: str
    policy: str
    criteria: Criteria
    churn_cost: float

    @property
    def key(self) -> str:
        c = self.criteria
        return (f"{self.estimator}|{self.deflator}|{self.policy}"
                f"|ttr{c.ttr_hours}_pb{c.price_min}-{c.price_max}_act{c.active_within_secs}"
                f"_trl{c.min_trl}_hl{c.half_life_days}|churn{self.churn_cost}")


@dataclass(frozen=True)
class BakeoffAxes:
    """The discrete levels of every grid axis — enumerated ONCE up front so ``N_GRID`` is fixed
    before any config is scored (issue #421 §Grid pre-registration)."""

    estimators: tuple
    deflators: tuple
    policies: tuple
    criteria: tuple
    churn_costs: tuple

    def enumerate_grid(self) -> list:
        return [GridPoint(e, d, p, c, ch) for e, d, p, c, ch in itertools.product(
            self.estimators, self.deflators, self.policies, self.criteria, self.churn_costs)]


def pre_register_grid(axes: BakeoffAxes, *, created_at: int = 0) -> dict:
    """8b — enumerate the grid and freeze ``N_GRID`` into a run manifest. ``N_GRID`` is the
    ``n_trials`` every grid-level Validator/Deflator is later given; nothing may be added to the
    SCORED grid afterwards (LANDMINE-1)."""
    grid = axes.enumerate_grid()
    keys = [g.key for g in grid]
    if len(set(keys)) != len(keys):
        # A8 (#436): a key collision silently merges two distinct configs into one matrix column,
        # corrupting N_GRID and every grid-level Validator. Fail loudly at pre-registration.
        from collections import Counter
        dupes = sorted(k for k, c in Counter(keys).items() if c > 1)
        raise ValueError(f"grid key collision: {len(dupes)} duplicate config key(s), e.g. "
                         f"{dupes[:3]} — distinct GridPoints must produce distinct keys")
    return {
        "created_at": created_at,
        "n_grid": len(grid),
        "axes": {
            "estimators": list(axes.estimators), "deflators": list(axes.deflators),
            "policies": list(axes.policies), "churn_costs": list(axes.churn_costs),
            "n_criteria": len(axes.criteria),
        },
        "grid_keys": [g.key for g in grid],
    }


# ───────────────────────── criteria + eligibility ─────────────────────────
def slice_by_criteria(ss: SuffStats, criteria: Criteria) -> SuffStats:
    """Apply the STATIC criteria filters (TTR horizon, price band) to the materialized superset.
    The per-``as_of`` recency / MinTRL gates are applied per step (they depend on the cutoff)."""
    ttr_secs = criteria.ttr_hours * 3600.0
    horizon = (ss["ttr_ref"] - ss["entry_ts"]) <= ttr_secs
    band = (ss["price"] >= criteria.price_min) & (ss["price"] <= criteria.price_max)
    return ss[horizon & band]


def eligible_wallets(in_sample: SuffStats, criteria: Criteria, *, as_of: int) -> set:
    """Wallets passing the per-``as_of`` recency + MinTRL gates (issue #421 ``active_as_of_T`` /
    ``mintrl``): last in-sample trade within ``active_within_secs`` of ``as_of`` (0 = no gate) and
    at least ``min_trl`` in-sample positions (0 = no gate)."""
    if in_sample.empty:
        return set()
    grp = in_sample.groupby("wallet", sort=False)
    last = grp["entry_ts"].max()
    count = grp.size()
    ok = count >= max(criteria.min_trl, 0)
    if criteria.active_within_secs > 0:
        ok &= last >= as_of - criteria.active_within_secs
    return set(last.index[ok.to_numpy()])


# ───────────────────────── deflation gate (per-wallet) ─────────────────────────
def _wallet_sharpe_moments(in_sample: SuffStats, wallets, weights=None) -> pd.DataFrame:
    """Per-wallet net-edge Sharpe moments ``[sr, n_obs, skew, kurt]`` (non-excess kurtosis) for the
    Deflated-Sharpe gate. A4 (#436): the SR mean/sd are the UNIQUENESS-WEIGHTED
    ``ranker_decay.weighted_stats`` the estimators use (so the gate and the ranker agree on a
    wallet's edge); ``n_obs`` is the Kish effective sample size; skew/kurt are the (unweighted)
    shape corrections for the DSR. ``weights`` is positional, aligned to ``in_sample``'s
    RangeIndex; ``None`` -> uniform (bitwise-identical to the legacy unweighted gate). Wallets with
    < 2 effective positions or zero dispersion are omitted (zero-dispersion positives are handled by
    the caller — see A10)."""
    rows = []
    for wallet in wallets:
        g = in_sample[in_sample["wallet"] == wallet]
        net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
        if net.size < 2:
            continue
        w = np.ones(net.size) if weights is None else weights[g.index.to_numpy()]
        mean, sd, n_eff, _ = weighted_stats(net, w)
        if not (n_eff >= 2 and sd > 0):
            continue
        rows.append((wallet, mean / sd, n_eff,
                     float(skew(net)), float(kurtosis(net, fisher=False))))
    return pd.DataFrame(rows, columns=["wallet", "sr", "n_obs", "skew", "kurt"]).set_index("wallet")


def _zero_dispersion_positive(in_sample: SuffStats, wallets) -> set:
    """A10 (#436): wallets with >= 2 positions, exactly zero net-edge dispersion, and a positive
    mean. Their Sharpe is UNDEFINED (not low), so the DSR gate must not silently veto an estimator's
    pick for one — it bypasses the bar instead of being dropped as sub-threshold. (Rare in practice:
    sd==0 requires identical net edge across positions, which differing entry prices preclude — so
    this only ever force-keeps a wallet an estimator already chose to score.)"""
    keep = set()
    for wallet in wallets:
        g = in_sample[in_sample["wallet"] == wallet]
        net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
        # isclose, not == 0: a mathematically-constant series can carry ~1e-16 std from float
        # accumulation (np.std of 6 identical values != 0.0 exactly), so an exact test is fragile.
        if net.size >= 2 and bool(np.isclose(net.std(ddof=1), 0.0, atol=1e-12)) \
                and float(net.mean()) > 0.0:
            keep.add(wallet)
    return keep


def apply_deflation_gate(scores: WalletScores, in_sample: SuffStats, deflator: str, *,
                         n_trials: int, weights=None,
                         threshold: float = RANKER_DSR_MIN) -> WalletScores:
    """Per-wallet deflation gate. ``none`` -> passthrough. ``deflated_sharpe`` -> keep only wallets
    whose Deflated Sharpe (``P(true SR > expected-max-null SR)`` at ``n_trials`` candidates) is at
    least ``threshold`` (``ranker_dsr_min``) — the winner's-curse correction applied at selection
    time. ``weights`` (A4) makes the gate's Sharpe agree with the ranker's uniqueness-weighted edge;
    zero-dispersion positive wallets bypass the bar rather than being vetoed (A10)."""
    if deflator == NO_DEFLATION or scores.empty:
        return scores
    if deflator != DeflatedSharpe.name:
        raise ValueError(f"unknown deflator {deflator!r}")
    keep = _zero_dispersion_positive(in_sample, scores.index)
    moments = _wallet_sharpe_moments(in_sample, scores.index, weights)
    if not moments.empty:
        deflated = DeflatedSharpe().deflate(moments, n_trials=n_trials, as_of=0)
        keep |= set(deflated.index[deflated["dsr"].fillna(0.0) >= threshold])
    return scores.loc[scores.index.intersection(list(keep))]


# ───────────────────────── backtest runner seam (8c) ─────────────────────────
class SubprocessBacktestRunner:
    """Real ``BacktestRunner``: shells out to the Rust ``pe-backtest`` injected-set path (PR3) and
    parses ``pnl_by_period.ndjson``. Writes a newline-delimited lowercase-hex wallet file, sets the
    figment env (``PE_BACKTEST_INJECTED_WALLETS_PATH`` / ``PE_BACKTEST_FLAT_USD`` (required) /
    ``PE_BACKTEST_MAX_TRADE_COUNT=0`` (bounded-load guard off) / ``PE_BACKTEST_OUTPUT_DIR`` /
    ``PE_BOOTSTRAP_CACHE_PATH``), runs the binary, and reads the per-(wallet, day) P&L it emits.

    # Note: each call re-runs the full injected-set backtest (the dominant 8c cost; the operator's
    # compute-budget feasibility check gates locking N_GRID). Used only in the operator run — CI
    # injects a fake runner.
    """

    def __init__(self, binary: str, cache_path: str, output_dir: str,
                 extra_env: "dict | None" = None):
        self.binary = binary
        self.cache_path = cache_path
        self.output_dir = Path(output_dir)
        self.extra_env = extra_env or {}

    def run(self, wallets: list, *, flat_usd: float) -> pd.DataFrame:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        wallet_file = self.output_dir / "injected_wallets.txt"
        wallet_file.write_text("\n".join(wallets) + "\n")
        env = {
            **os.environ,
            "PE_BACKTEST_INJECTED_WALLETS_PATH": str(wallet_file),
            "PE_BACKTEST_FLAT_USD": str(flat_usd),
            "PE_BACKTEST_MAX_TRADE_COUNT": "0",
            "PE_BACKTEST_OUTPUT_DIR": str(self.output_dir),
            "PE_BOOTSTRAP_CACHE_PATH": self.cache_path,
            **self.extra_env,
        }
        subprocess.run([self.binary], env=env, check=True)
        return parse_pnl_by_period(self.output_dir / "pnl_by_period.ndjson")


def parse_pnl_by_period(path) -> pd.DataFrame:
    """Parse a ``pnl_by_period.ndjson`` (PR3 emit) into a frame with the pinned columns. An empty
    or absent file yields an empty frame (an injected set may have no fills)."""
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=_PNL_COLUMNS)
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        return pd.DataFrame(columns=_PNL_COLUMNS)
    return pd.DataFrame(rows)[_PNL_COLUMNS]


# ───────────────────────── policy construction ─────────────────────────
def build_policy(name: str, *, k: int, demoter, displacement_margin: int):
    """Construct a fresh stateful policy instance (issue #421: one instance per trajectory)."""
    if name == FullRerank.name:
        return FullRerank(k)
    if name == KnockoutBackfill.name:
        return KnockoutBackfill(k, demoter)
    if name == HybridDisplacement.name:
        return HybridDisplacement(k, demoter, displacement_margin)
    if name == OnlineWeighting.name:
        return OnlineWeighting(k, OnlineExpWeights())
    raise ValueError(f"unknown policy {name!r}")


# ───────────────────────── 8a: estimator screen ─────────────────────────
def _forward_copy_pnl(forward: SuffStats, wallets) -> float:
    """Cheap point-in-time forward copy P&L proxy: the followed wallets' mean forward net edge
    ``(payoff - _eff)/_eff`` over their positions in the forward window (no ``pe-backtest``)."""
    fwd = forward[forward["wallet"].isin(wallets)]
    if fwd.empty:
        return 0.0
    return float(((fwd["payoff"] - fwd["_eff"]) / fwd["_eff"]).mean())


def screen_estimators(ss: SuffStats, estimators: list, *, as_of_points: list,
                      train_secs: int, horizon_secs: int, k: int, keep: int,
                      criteria: "Criteria | None" = None) -> list:
    """8a — rank estimators by mean point-in-time forward copy P&L of their top-k pick and keep the
    best ``keep`` (the §Acceptance benchmark ``t_stat_baseline`` is always retained). Cheap: scores
    once per ``as_of`` and reads the forward net edge directly, eliminating most of the menu before
    the expensive policy trajectories.

    A3 (#436): the screen runs on the SAME gated universe the trajectories do — the static
    ``slice_by_criteria`` plus the per-``as_of`` ``eligible_wallets`` recency/MinTRL gate — under the
    canonical ``criteria`` (the singleton at Phase A; the chosen canonical level once the criteria
    axis goes plural, D1). Without it the screen ranked estimators on a wider universe than the one
    they were later scored on. ``criteria=None`` is the fully-permissive no-op default (back-compat).
    """
    if criteria is None:
        criteria = Criteria(0, float("inf"), 0.0, 1.0, 0.0, 0)
    ss_c = slice_by_criteria(ss, criteria)
    scoreboard = {}
    for name in estimators:
        estimator = ESTIMATOR_REGISTRY[name]()
        fwd_pnls = []
        for as_of in as_of_points:
            in_sample, forward = split_walkforward(
                ss_c, as_of=as_of, train_secs=train_secs, horizon_secs=horizon_secs)
            if in_sample.empty or forward.empty:
                continue
            eligible = eligible_wallets(in_sample, criteria, as_of=as_of)
            candidates = in_sample[in_sample["wallet"].isin(eligible)]   # label-preserving slice
            if candidates.empty:
                continue
            weights = uniqueness_weights(in_sample)                      # full-frame, aligned index
            scores = estimator.score(candidates, as_of=as_of, weights=weights)
            if scores.empty:
                continue
            picked = scores.nsmallest(min(k, len(scores)), "rank").index
            fwd_pnls.append(_forward_copy_pnl(forward, picked))
        scoreboard[name] = float(np.mean(fwd_pnls)) if fwd_pnls else float("-inf")
    ranked = sorted(scoreboard, key=lambda n: scoreboard[n], reverse=True)
    survivors = ranked[:keep]
    if BASELINE_ESTIMATOR in estimators and BASELINE_ESTIMATOR not in survivors:
        survivors.append(BASELINE_ESTIMATOR)
    return survivors


# ───────────────────────── 8c: policy trajectory ─────────────────────────
def _churn(prev: FollowSet, follow: FollowSet) -> int:
    """Admissions = wallets newly in the followed set (issue #421: turnover count x per-admission
    cost; under hold-to-resolution an eviction halts new copies only, so only admissions cost)."""
    prev_set = set(prev["wallet"]) if len(prev) else set()
    return len(set(follow["wallet"]) - prev_set)


@dataclass
class TrajectoryResult:
    """A1 (#436): one config's trajectory output. ``returns`` is the per-period forward copy P&L net
    of churn; ``final_follow`` / ``final_scores`` are the LAST real (non-empty) followed set and its
    per-wallet scores — the deliverable substrate. Three of four policies are stateful, so this set
    is captured DURING the run: it cannot be reproduced by a cold re-application of the policy at the
    latest ``as_of`` (an empty ``prev`` degenerates knockout/hybrid/online into a memoryless top-k).
    """

    returns: pd.Series
    final_follow: FollowSet
    final_scores: WalletScores


def run_trajectory(grid_point: GridPoint, ss: SuffStats, runner, *, as_of_points: list,
                   train_secs: int, horizon_secs: int, k: int,
                   displacement_margin: int, demoter_kwargs: "dict | None" = None,
                   flat_usd: float = 25.0) -> "TrajectoryResult":
    """Run one config as a full walk-forward trajectory; return its per-period forward copy P&L net
    of churn (indexed by ``as_of``) PLUS the final-step followed set (A1). Sequential by
    construction: ``set_t -> pe-backtest(set_t) -> live_pnl_t -> policy.step -> set_{t+1}``
    (issue #421 8c).

    A9 (#436): a period with no eligible set / no surviving signal / an empty followed set yields
    ``NaN`` (the config held nothing — EXCLUDED from its moments), NOT ``0.0`` (which would be a
    real, low-variance "traded and made $0" and could out-rank a live config)."""
    demoter = EmpiricalBernsteinDemoter(**(demoter_kwargs or {}))
    policy = build_policy(grid_point.policy, k=k, demoter=demoter,
                          displacement_margin=displacement_margin)
    estimator = ESTIMATOR_REGISTRY[grid_point.estimator]()
    ss_c = slice_by_criteria(ss, grid_point.criteria)

    prev: FollowSet = pd.DataFrame({"wallet": [], "weight": []})
    live_pnl = pd.DataFrame(columns=_PNL_COLUMNS)
    returns = []
    final_follow: FollowSet = pd.DataFrame({"wallet": [], "weight": []})
    final_scores: WalletScores = pd.DataFrame(columns=["score", "rank"])
    for as_of in as_of_points:
        in_sample, _forward = split_walkforward(
            ss_c, as_of=as_of, train_secs=train_secs, horizon_secs=horizon_secs)
        eligible = eligible_wallets(in_sample, grid_point.criteria, as_of=as_of)
        candidates = in_sample[in_sample["wallet"].isin(eligible)]   # label-preserving slice
        if candidates.empty:
            returns.append(np.nan)                                   # A9: no eligible set
            continue
        weights = uniqueness_weights(in_sample)                      # full-frame, aligned index
        scores = estimator.score(candidates, as_of=as_of, weights=weights)
        # Per-wallet deflation: n_trials = the CANDIDATE count being selected among (deflation.py
        # contract), NOT N_GRID — the grid trial count is the grid-level Validators' bar (8c).
        scores = apply_deflation_gate(scores, in_sample, grid_point.deflator,
                                      n_trials=len(scores), weights=weights)
        if scores.empty:
            returns.append(np.nan)                                   # A9: no surviving signal
            continue
        follow = policy.step(prev, scores, live_pnl, as_of=as_of)
        if follow.empty:
            returns.append(np.nan)                                   # A9: policy holds nothing
            continue
        admissions = _churn(prev, follow)

        pnl = runner.run(list(follow["wallet"]), flat_usd=flat_usd)
        window = pnl[(pnl["period_end"] > as_of) & (pnl["period_end"] <= as_of + horizon_secs)]
        gross = _weighted_window_pnl(window, follow)
        returns.append(gross - grid_point.churn_cost * admissions)
        live_pnl = pd.concat([live_pnl, window], ignore_index=True)
        prev = follow
        final_follow, final_scores = follow, scores                 # A1: most recent real set
    return TrajectoryResult(
        returns=pd.Series(returns, index=list(as_of_points), name=grid_point.key, dtype=float),
        final_follow=final_follow, final_scores=final_scores)


def _weighted_window_pnl(window: pd.DataFrame, follow: FollowSet) -> float:
    """Followed-set forward copy P&L over a window = sum_w weight_w x realized_pnl_w."""
    if window.empty or follow.empty:
        return 0.0
    per_wallet = window.groupby("wallet")["realized_pnl"].sum()
    weight = follow.set_index("wallet")["weight"]
    common = per_wallet.index.intersection(weight.index)
    if common.empty:
        return 0.0
    return float((per_wallet.loc[common] * weight.loc[common]).sum())


def run_trajectories(grid: list, ss: SuffStats, runner, **kwargs) -> "tuple[pd.DataFrame, dict]":
    """8c — run every grid point's trajectory. Returns ``(return_matrix, results)`` where
    ``return_matrix`` is the per-period return matrix (index = ``as_of``, columns = config keys) for
    the grid-level Validators and ``results`` maps each config key -> its ``TrajectoryResult`` (A1:
    so the winning config's frozen final-step followed set is selected directly, not re-derived)."""
    results = {gp.key: run_trajectory(gp, ss, runner, **kwargs) for gp in grid}
    return pd.DataFrame({key: res.returns for key, res in results.items()}), results


# ───────────────────────── leaderboard + winner / NO-GO ─────────────────────────
def _config_moments(return_matrix: pd.DataFrame) -> pd.DataFrame:
    """Per-config performance: cumulative return, per-period Sharpe moments, and the mean/SE used
    as the AKM/MRSW/FCR 'arm' estimates."""
    rows = []
    for cfg in return_matrix.columns:
        series = return_matrix[cfg].to_numpy(dtype=float)
        n = series.size
        sd = series.std(ddof=1) if n > 1 else 0.0
        se = sd / np.sqrt(n) if (n > 0 and sd > 0) else np.inf
        rows.append((cfg, float(series.sum()), float(series.mean()),
                     float(series.mean() / sd) if sd > 0 else 0.0, n,
                     float(skew(series)) if n > 2 else 0.0,
                     float(kurtosis(series, fisher=False)) if n > 3 else 3.0, float(se)))
    return pd.DataFrame(rows, columns=["config", "cum_return", "mean", "sr", "n_obs",
                                       "skew", "kurt", "se"]).set_index("config")


def grid_deflate(return_matrix: pd.DataFrame, *, benchmark: str, n_grid: int,
                 n_trials_dsr: "int | None" = None, seed: int = 0) -> dict:
    """Apply the grid-level honesty layer (LANDMINE-1): Deflated-Sharpe per config, PBO, Romano-Wolf,
    Hansen-SPA. Returns the assembled results.

    A2/A8 (#436) — the two trial counts are DIFFERENT by construction, so do not force them equal:
      * the scalar Deflated-Sharpe ``expected_max_sharpe`` bar takes the FULL pre-screen
        ``n_trials_dsr`` (every config the menu could have produced, incl. the estimators 8a dropped)
        — the honest multiplicity of the search;
      * ``PBO`` / ``RomanoWolf`` / ``HansenSPA`` are bootstrap Validators over the matrix COLUMNS and
        structurally cannot reflect screened-out / unrun configs, so they take ``n_grid`` (the
        run-set columns, incl. the benchmark). Documented asymmetry, not a bug (docs/31)."""
    n_trials_dsr = n_grid if n_trials_dsr is None else n_trials_dsr
    moments = _config_moments(return_matrix)
    deflated = DeflatedSharpe().deflate(
        moments[["sr", "n_obs", "skew", "kurt"]], n_trials=n_trials_dsr, as_of=0)
    pbo = PBO().assess(return_matrix, n_configs=n_grid)
    romano = RomanoWolf(benchmark, seed=seed).assess(return_matrix, n_configs=n_grid)
    hansen = HansenSPA(benchmark, seed=seed).assess(return_matrix, n_configs=n_grid)
    return {"moments": moments, "deflated": deflated, "pbo": pbo,
            "romano_wolf": romano, "hansen_spa": hansen}


def wallet_persistence_cpr(ss: SuffStats, *, split_at: int) -> dict:
    """``brown_goetzmann_cpr`` premise check — do WALLETS persist? Each wallet's mean net edge
    ``(payoff - _eff)/_eff`` in the early period (``entry_ts < split_at``) vs the late period
    (``entry_ts >= split_at``); CPR > 1 at p < 0.05 => skilled wallets stay skilled => following is
    a viable premise (issue #421: the upstream/meta "is wallet-following viable at all?" go-gate).
    Cross-sectional over the wallet universe — NOT a single config's time series."""
    early, late = ss[ss["entry_ts"] < split_at], ss[ss["entry_ts"] >= split_at]
    p1 = ((early["payoff"] - early["_eff"]) / early["_eff"]).groupby(early["wallet"]).mean()
    p2 = ((late["payoff"] - late["_eff"]) / late["_eff"]).groupby(late["wallet"]).mean()
    if len(p1) < 4 or len(p2) < 4:
        return {"go": False, "reason": "insufficient wallets per period for CPR"}
    return brown_goetzmann_cpr(p1, p2)


def winner_uncertainty(moments: pd.DataFrame, *, winner_config: "str | None" = None,
                       top_q: float = RANKER_FDR_Q) -> dict:
    """AKM/MRSW/FCR winner's-curse uncertainty on the config leaderboard (each config is an 'arm'
    with estimate = mean per-period return, SE from its dispersion). Reports the conditional
    median-unbiased estimate + CI for the winning config, the top-1 rank confidence set, and
    FCR-adjusted CIs for the selected (positive-mean) configs.

    A5 (#436): ``winner_config`` points the AKM block at the config the bake-off actually AWARDED
    (highest cum-return among RW-superior), which need not be the mean-argmax (default). The caller
    passes the leaderboard WITH THE BASELINE DROPPED. A config with non-finite SE (zero/degenerate
    dispersion) cannot carry a conditional CI — handled explicitly rather than crashing."""
    finite = moments[np.isfinite(moments["se"]) & (moments["se"] > 0)]
    if finite.empty:
        return {"available": False}
    configs = list(finite.index)
    if winner_config is not None and winner_config not in configs:
        return {"available": True, "winner_config": winner_config, "akm": None,
                "note": "awarded winner has non-finite SE (degenerate dispersion); no conditional CI"}
    estimates = finite["mean"].to_numpy()
    ses = finite["se"].to_numpy()
    w_idx = configs.index(winner_config) if winner_config is not None else None
    akm = akm_inference_on_winners(estimates, ses, winner=w_idx)
    mrsw = mrsw_rank_cs(estimates, ses, tau=1)
    selected = estimates > 0
    fcr = fcr_selected_ci(estimates, ses, selected, q=top_q)
    return {
        "available": True,
        "winner_config": configs[akm["winner"]],
        "akm": akm,
        "mrsw_top1_cs": [configs[i] for i in mrsw.index[mrsw["in_top_tau_cs"]]],
        "fcr": fcr.assign(config=[configs[i] for i in fcr["index"]]),
    }


def _clean_return_matrix(return_matrix: pd.DataFrame, *,
                         baseline_key: str) -> "tuple[pd.DataFrame | None, dict]":
    """A9 (#436): exclude no-signal configs/periods before the leaderboard. Drop configs that never
    produced a signal (all-NaN columns) and make the panel rectangular by dropping periods any
    surviving config missed — so a dead config is EXCLUDED, not scored a low-variance 0 that
    out-ranks a live config, and the bootstrap Validators (PBO/RW/SPA) get a dense matrix. Returns
    ``(clean, info)``, or ``(None, info)`` with a ``reason`` when no verdict is supportable."""
    non_dead = return_matrix.dropna(axis=1, how="all")
    info = {"dropped_configs": [c for c in return_matrix.columns if c not in non_dead.columns]}
    if baseline_key not in non_dead.columns:
        info["reason"] = "baseline config produced no signal in any period"
        return None, info
    clean = non_dead.dropna(axis=0, how="any")
    info["dropped_periods"] = int(return_matrix.shape[0] - clean.shape[0])
    if clean.shape[0] < 2:
        info["reason"] = "insufficient periods after excluding no-signal periods (< 2)"
        return None, info
    if clean.shape[1] < 2:
        info["reason"] = "insufficient configs after excluding no-signal configs (< 2)"
        return None, info
    return clean, info


def select_winner_or_nogo(return_matrix: pd.DataFrame, *, baseline_key: str, n_grid: int,
                          n_grid_full: "int | None" = None, cpr: dict,
                          alpha: float = RANKER_FDR_Q, seed: int = 0) -> dict:
    """The §Acceptance bar (issue #421): a config is the recommended winner only if it beats
    ``t_stat_baseline`` on cumulative forward copy P&L net of churn, AND that margin survives
    grid-deflated significance (Romano-Wolf superior / Hansen-SPA at ``alpha`` / ``PBO`` below
    ``ranker_pbo_max``), AND the ``brown_goetzmann_cpr`` go-check passes. Otherwise the deliverable
    is an explicit NO-GO.

    A5/A6/A9/A11 (#436): the verdict iterates challengers by descending cum-return and awards the
    FIRST that is RW-superior and beats the baseline (not the cum-argmax, which may be an uncertified
    high-variance config); ``ranker_pbo_max`` / ``ranker_fdr_q`` are wired (were hardcoded); the
    no-signal panel is cleaned first; and the winner's grid-DSR is reported as an advisory."""
    n_grid_full = n_grid if n_grid_full is None else n_grid_full
    clean, info = _clean_return_matrix(return_matrix, baseline_key=baseline_key)
    base = {"n_grid": n_grid, "n_grid_full_pre_screen": n_grid_full, "baseline_key": baseline_key,
            "cpr_go": bool(cpr.get("go", False)),
            "dropped_configs": info.get("dropped_configs", []),
            "dropped_periods": info.get("dropped_periods", 0)}
    if clean is None:
        # Always carry a (here empty) leaderboard so the operator-run output writer is uniform.
        return {**base, "leaderboard": pd.DataFrame(), "winner": None, "status": "NO-GO",
                "reason": info["reason"]}

    deflation = grid_deflate(clean, benchmark=baseline_key, n_grid=n_grid,
                             n_trials_dsr=n_grid_full, seed=seed)
    moments = deflation["moments"]
    deflated = deflation["deflated"]
    baseline_cum = float(moments.loc[baseline_key, "cum_return"])
    challengers = moments.drop(index=baseline_key)
    pbo_val = float(deflation["pbo"]["pbo"].iloc[0])
    spa_val = float(deflation["hansen_spa"]["spa_pvalue_consistent"].iloc[0])
    decision = {
        **base, "baseline_cum_return": baseline_cum,
        "pbo": pbo_val, "hansen_spa_pvalue": spa_val,
        "leaderboard": moments.sort_values("cum_return", ascending=False),
    }
    if challengers.empty:
        return {**decision, "winner": None, "status": "NO-GO", "reason": "no challenger configs"}

    superior = set(deflation["romano_wolf"].loc[
        deflation["romano_wolf"]["beats_benchmark"], "config"])
    spa_ok = spa_val < alpha
    pbo_ok = pbo_val < RANKER_PBO_MAX            # NaN < x is False -> degenerate PBO fails safe
    # A5 (#436): award the highest-cum-return RW-superior config that beats the baseline.
    winner = None
    for cfg in challengers.sort_values("cum_return", ascending=False).index:
        if float(moments.loc[cfg, "cum_return"]) > baseline_cum and cfg in superior:
            winner = cfg
            break
    if winner is not None and spa_ok and pbo_ok and decision["cpr_go"]:
        est_name = winner.split("|")[0]
        winner_dsr = float(deflated.loc[winner, "dsr"]) if winner in deflated.index else float("nan")
        baseline_estimator_win = est_name == BASELINE_ESTIMATOR
        return {**decision, "winner": winner, "status": "WINNER",
                "winner_estimator": est_name,
                # A11 (#436): a baseline-ESTIMATOR config can only win on its policy/criteria/churn.
                "baseline_estimator_win": bool(baseline_estimator_win),
                # A6 (#436): grid-DSR is advisory — surfaced + flagged, not gated (see RANKER_*).
                "winner_grid_dsr": winner_dsr,
                "grid_dsr_advisory_low": bool(not np.isnan(winner_dsr)
                                              and winner_dsr < RANKER_GRID_DSR_MIN),
                "uncertainty": winner_uncertainty(challengers, winner_config=winner),
                "reason": ("policy/criteria-only win (baseline estimator); RW-superior, "
                           "PBO/SPA clear, CPR go" if baseline_estimator_win
                           else "beats baseline, RW-superior, PBO/SPA clear, CPR go")}
    reason = []
    if winner is None:
        reason.append("no challenger is Romano-Wolf superior and beats baseline cum return")
    if not spa_ok:
        reason.append(f"Hansen-SPA p={spa_val:.3f} >= {alpha}")
    if not pbo_ok:
        reason.append("PBO undefined (degenerate leaderboard dispersion)" if np.isnan(pbo_val)
                      else f"PBO={pbo_val:.2f} >= {RANKER_PBO_MAX}")
    if not decision["cpr_go"]:
        reason.append("CPR no-go")
    return {**decision, "winner": None, "status": "NO-GO", "reason": "; ".join(reason)}


# ───────────────────────── Supabase paper_fills cross-check ─────────────────────────
def _parse_paper_fills_rows(rows: list, *, wallet_col: str, pnl_col: str) -> pd.DataFrame:
    """Coerce the Supabase REST JSON rows (numeric P&L arrives as strings) into the
    ``[wallet, realized_pnl]`` frame ``paper_fills_crosscheck`` consumes."""
    return pd.DataFrame({
        "wallet": [str(r[wallet_col]).lower() for r in rows],
        "realized_pnl": [float(r[pnl_col]) for r in rows],
    })


def fetch_paper_fills_realized_pnl(*, url_env: str = "SUPABASE_URL",
                                   key_env: str = "SUPABASE_SECRET_KEY",
                                   table: str = "wallet_live_stats",
                                   wallet_col: str = "wallet",
                                   pnl_col: str = "live_realized_pnl") -> pd.DataFrame:
    """Fetch per-wallet realized P&L from Supabase (the live cohort's precomputed
    ``wallet_live_stats`` view) via PostgREST + urllib — the same access pattern as
    ``push_ranking_to_supabase.py``. Returns ``[wallet, realized_pnl]`` for the cross-check.

    # Operator-only (needs creds); CI tests the parser on synthetic rows.
    """
    import urllib.request

    base = os.environ.get(url_env)
    key = os.environ.get(key_env)
    if not base or not key:
        raise ValueError(f"set {url_env} and {key_env} in env (.env) for the paper_fills fetch")
    url = f"{base.rstrip('/')}/rest/v1/{table}?select={wallet_col},{pnl_col}"
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json",
    }, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:        # noqa: S310 (trusted Supabase URL)
        rows = json.loads(resp.read())
    return _parse_paper_fills_rows(rows, wallet_col=wallet_col, pnl_col=pnl_col)


# ───────────────────────── orchestrator ─────────────────────────
@dataclass
class BakeoffParams:
    """Walk-forward + sizing knobs shared across stages (the operator pins these per run)."""

    as_of_points: list
    train_secs: int
    horizon_secs: int
    k: int = 25
    screen_keep: int = 4
    displacement_margin: int = 5
    flat_usd: float = 25.0
    demoter_kwargs: dict = field(default_factory=dict)


def run_bakeoff(ss: SuffStats, runner, axes: BakeoffAxes, params: BakeoffParams, *,
                created_at: int = 0, seed: int = 0) -> dict:
    """Full staged sweep (8a -> 8b -> 8c -> leaderboard -> winner/NO-GO). 8a prunes the estimator
    axis BEFORE ``N_GRID`` is frozen (it only ever shrinks the grid — never grows it post-hoc)."""
    n_grid_full = len(axes.enumerate_grid())     # A2: FULL pre-screen trial count for the DSR bar
    survivors = screen_estimators(
        ss, list(axes.estimators), as_of_points=params.as_of_points,
        train_secs=params.train_secs, horizon_secs=params.horizon_secs,
        k=params.k, keep=params.screen_keep, criteria=axes.criteria[0])   # A3: canonical criteria
    pruned_axes = BakeoffAxes(
        estimators=tuple(e for e in axes.estimators if e in survivors),
        deflators=axes.deflators, policies=axes.policies,
        criteria=axes.criteria, churn_costs=axes.churn_costs)
    manifest = pre_register_grid(pruned_axes, created_at=created_at)
    grid = pruned_axes.enumerate_grid()
    grid_by_key = {g.key: g for g in grid}        # A1: thread GridPoints, never re-parse a key
    baseline_key = _baseline_key(pruned_axes)
    return_matrix, results = run_trajectories(
        grid, ss, runner, as_of_points=params.as_of_points,
        train_secs=params.train_secs, horizon_secs=params.horizon_secs,
        k=params.k, displacement_margin=params.displacement_margin,
        demoter_kwargs=params.demoter_kwargs, flat_usd=params.flat_usd)
    split_at = int(np.median(params.as_of_points))
    cpr = wallet_persistence_cpr(ss, split_at=split_at)
    decision = select_winner_or_nogo(
        return_matrix, baseline_key=baseline_key, n_grid=manifest["n_grid"],
        n_grid_full=n_grid_full, cpr=cpr, seed=seed)
    # A1 (#436): the deliverable is the WINNING trajectory's frozen final-step followed set (the set
    # the winner actually rode), selected directly from the captured results — never a cold re-score
    # at the latest as_of (which degenerates stateful policies). Falls back to the baseline on NO-GO.
    winner_key = decision["winner"] or baseline_key
    winner_res = results.get(winner_key)
    deliverable = {
        "winner_key": winner_key,
        "grid_point": grid_by_key.get(winner_key),
        "follow": winner_res.final_follow if winner_res is not None
        else pd.DataFrame({"wallet": [], "weight": []}),
        "scores": winner_res.final_scores if winner_res is not None
        else pd.DataFrame(columns=["score", "rank"]),
    }
    return {"manifest": manifest, "survivors": survivors, "return_matrix": return_matrix,
            "cpr": cpr, "decision": decision, "deliverable": deliverable}


def _baseline_key(axes: BakeoffAxes) -> str:
    """The §Acceptance benchmark config (t_stat_baseline + full-rerank + no deflation + the first
    criteria/churn level). Must be present in the pre-registered grid — asserted so a misconfigured
    axis set fails loudly rather than silently scoring against a missing benchmark."""
    key = GridPoint(BASELINE_ESTIMATOR, NO_DEFLATION, BASELINE_POLICY,
                    axes.criteria[0], axes.churn_costs[0]).key
    if key not in {g.key for g in axes.enumerate_grid()}:
        raise ValueError("baseline config absent from the grid: axes must include "
                         f"{BASELINE_ESTIMATOR}/{NO_DEFLATION}/{BASELINE_POLICY}")
    return key


def _build_arg_parser() -> argparse.ArgumentParser:
    """The operator-run CLI, extracted from ``main`` so a drift guard can assert the engine
    wiring (issue #421 PR6 follow-up). ``--engine`` is the ``ranker_duck`` engine MODE and
    defaults to ``duck``: the bake-off requires the Parquet engine because
    ``suff_stats.materialize`` needs a live DuckDB connection — there is no SQLite fallback for it.
    ``--cache`` is the ``wallet_cache.db`` path forwarded to ``pe-backtest`` as the trade cache,
    NOT the engine selector (the original ``main`` passed ``--cache`` into ``get_engine``'s
    ``force`` argument, which fell back to SQLite and crashed ``materialize(None)``)."""
    ap = argparse.ArgumentParser(description="issue #421 ranker bake-off (operator run)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--pe-backtest", required=True, help="path to the pe-backtest binary")
    ap.add_argument("--cache", required=True,
                    help="path to wallet_cache.db (forwarded to pe-backtest as the trade cache)")
    ap.add_argument("--engine", default="duck", choices=("duck", "auto", "sqlite"),
                    help="ranker_duck engine mode for the suff_stats snapshot (default: duck — the "
                         "bake-off requires the Parquet engine)")
    ap.add_argument("--parquet-dir", default=None,
                    help="Parquet snapshot directory (default: ranker_duck's data/parquet, "
                         "overridable via PE_RANKER_PARQUET_DIR)")
    ap.add_argument("--train-days", type=int, default=180)
    ap.add_argument("--horizon-days", type=int, default=30)
    ap.add_argument("--steps", type=int, default=6)
    ap.add_argument("--step-days", type=int, default=30)
    ap.add_argument("--start-unix", type=int, required=True)
    return ap


def _open_engine(args: argparse.Namespace):
    """Open the DuckDB/Parquet engine the bake-off requires (``--engine`` is ``ranker_duck``'s
    ``force`` mode, default ``duck``; ``--parquet-dir`` optional). Passes the engine MODE to
    ``ranker_duck.get_engine`` — never the ``--cache`` path (issue #421 regression guard).

    ``get_engine`` returns ``None`` for ``--engine sqlite`` (always) or ``--engine auto`` with a
    stale/absent snapshot; the bake-off has no SQLite path (``suff_stats.materialize`` needs a live
    connection), so fail loudly here rather than crash later in ``materialize(None)``."""
    import ranker_duck  # lazy: duckdb is not on every dev box (issue #421 runtime note)

    con = ranker_duck.get_engine(args.engine, args.parquet_dir)
    if con is None:
        raise SystemExit(
            f"bake-off requires a DuckDB/Parquet engine but --engine={args.engine} yielded none "
            "(SQLite has no suff_stats path); use --engine duck after running "
            "scripts/export_trades_parquet.py to (re)build the snapshot"
        )
    return con


def main() -> None:  # pragma: no cover (operator entry; CI exercises the stage functions)
    """Operator entry: materialize suff_stats from the cache, run the bake-off against the real
    ``pe-backtest``, write the leaderboard / manifest / decision, cross-check ``paper_fills``."""
    import time

    from . import suff_stats as suff_stats_mod

    args = _build_arg_parser().parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = _open_engine(args)
    ss = suff_stats_mod.materialize(con)

    day = 86_400
    as_of_points = [args.start_unix + i * args.step_days * day for i in range(args.steps)]
    params = BakeoffParams(as_of_points=as_of_points, train_secs=args.train_days * day,
                           horizon_secs=args.horizon_days * day)
    axes = BakeoffAxes(
        estimators=tuple(ESTIMATOR_REGISTRY), deflators=(NO_DEFLATION, DeflatedSharpe.name),
        policies=(FullRerank.name, KnockoutBackfill.name, HybridDisplacement.name,
                  OnlineWeighting.name),
        criteria=(Criteria(0, 72.0, 0.15, 0.85, 0.0, 0),), churn_costs=(0.0, 1.0))
    runner = SubprocessBacktestRunner(args.pe_backtest, args.cache, str(out_dir / "bt"))
    result = run_bakeoff(ss, runner, axes, params, created_at=int(time.time()))

    (out_dir / "manifest.json").write_text(json.dumps(result["manifest"], indent=2))
    result["return_matrix"].to_csv(out_dir / "return_matrix.csv")
    result["decision"]["leaderboard"].to_csv(out_dir / "leaderboard.csv")
    status = {k: v for k, v in result["decision"].items() if k != "leaderboard"}
    (out_dir / "decision.json").write_text(json.dumps(status, indent=2, default=str))
    print(f"bake-off {result['decision']['status']}: {result['decision']['reason']}")

    # Cross-check the deliverable — the WINNING config's frozen final-step followed set (A1, #436;
    # the set the winner actually rode, applying its criteria/eligibility/deflation/policy), NOT a
    # cold re-score on the un-sliced in-sample — against the live cohort's realized P&L from Supabase
    # (issue #421 §Deliverable). Falls back to the baseline config's set on a NO-GO.
    try:
        deliverable = result["deliverable"]
        scores, follow = deliverable["scores"], deliverable["follow"]
        followed = (scores.loc[scores.index.intersection(list(follow["wallet"]))]
                    if not scores.empty else scores)
        crosscheck = paper_fills_crosscheck(followed.reset_index(), fetch_paper_fills_realized_pnl())
        crosscheck.to_csv(out_dir / "paper_fills_crosscheck.csv", index=False)
        print(f"paper_fills cross-check ({deliverable['winner_key']}): "
              f"{int(crosscheck['disagree'].sum())} live-loser flags of {len(crosscheck)} "
              "overlapping wallets")
    except Exception as exc:  # advisory: an operator env without Supabase creds must not abort
        print(f"paper_fills cross-check skipped: {exc}")


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    main()
