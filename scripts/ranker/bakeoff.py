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
import itertools
import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import kurtosis, skew

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
def _wallet_sharpe_moments(in_sample: SuffStats, wallets) -> pd.DataFrame:
    """Per-wallet net-edge Sharpe moments ``[sr, n_obs, skew, kurt]`` (non-excess kurtosis) for
    the Deflated-Sharpe gate. Wallets with < 2 positions or zero dispersion are omitted."""
    rows = []
    for wallet in wallets:
        g = in_sample[in_sample["wallet"] == wallet]
        net = ((g["payoff"] - g["_eff"]) / g["_eff"]).to_numpy()
        if net.size < 2:
            continue
        sd = net.std(ddof=1)
        if sd <= 0:
            continue
        rows.append((wallet, net.mean() / sd, net.size,
                     float(skew(net)), float(kurtosis(net, fisher=False))))
    return pd.DataFrame(rows, columns=["wallet", "sr", "n_obs", "skew", "kurt"]).set_index("wallet")


def apply_deflation_gate(scores: WalletScores, in_sample: SuffStats, deflator: str, *,
                         n_trials: int, threshold: float = 0.5) -> WalletScores:
    """Per-wallet deflation gate. ``none`` -> passthrough. ``deflated_sharpe`` -> keep only wallets
    whose Deflated Sharpe (``P(true SR > expected-max-null SR)`` at ``n_trials`` candidates) is at
    least ``threshold`` — the winner's-curse correction applied at selection time."""
    if deflator == NO_DEFLATION or scores.empty:
        return scores
    if deflator != DeflatedSharpe.name:
        raise ValueError(f"unknown deflator {deflator!r}")
    moments = _wallet_sharpe_moments(in_sample, scores.index)
    if moments.empty:
        return scores.iloc[0:0]
    deflated = DeflatedSharpe().deflate(moments, n_trials=n_trials, as_of=0)
    keep = deflated.index[deflated["dsr"].fillna(0.0) >= threshold]
    return scores.loc[scores.index.intersection(keep)]


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
                      train_secs: int, horizon_secs: int, k: int, keep: int) -> list:
    """8a — rank estimators by mean point-in-time forward copy P&L of their top-k pick and keep the
    best ``keep`` (the §Acceptance benchmark ``t_stat_baseline`` is always retained). Cheap: scores
    once per ``as_of`` and reads the forward net edge directly, eliminating most of the menu before
    the expensive policy trajectories."""
    scoreboard = {}
    for name in estimators:
        estimator = ESTIMATOR_REGISTRY[name]()
        fwd_pnls = []
        for as_of in as_of_points:
            in_sample, forward = split_walkforward(
                ss, as_of=as_of, train_secs=train_secs, horizon_secs=horizon_secs)
            if in_sample.empty or forward.empty:
                continue
            weights = uniqueness_weights(in_sample)
            scores = estimator.score(in_sample, as_of=as_of, weights=weights)
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


def run_trajectory(grid_point: GridPoint, ss: SuffStats, runner, *, as_of_points: list,
                   train_secs: int, horizon_secs: int, k: int,
                   displacement_margin: int, demoter_kwargs: "dict | None" = None,
                   flat_usd: float = 25.0) -> pd.Series:
    """Run one config as a full walk-forward trajectory; return its per-period forward copy P&L net
    of churn (indexed by ``as_of``). Sequential by construction:
    ``set_t -> pe-backtest(set_t) -> live_pnl_t -> policy.step -> set_{t+1}`` (issue #421 8c)."""
    demoter = EmpiricalBernsteinDemoter(**(demoter_kwargs or {}))
    policy = build_policy(grid_point.policy, k=k, demoter=demoter,
                          displacement_margin=displacement_margin)
    estimator = ESTIMATOR_REGISTRY[grid_point.estimator]()
    ss_c = slice_by_criteria(ss, grid_point.criteria)

    prev: FollowSet = pd.DataFrame({"wallet": [], "weight": []})
    live_pnl = pd.DataFrame(columns=_PNL_COLUMNS)
    returns = []
    for as_of in as_of_points:
        in_sample, _forward = split_walkforward(
            ss_c, as_of=as_of, train_secs=train_secs, horizon_secs=horizon_secs)
        eligible = eligible_wallets(in_sample, grid_point.criteria, as_of=as_of)
        candidates = in_sample[in_sample["wallet"].isin(eligible)]   # label-preserving slice
        if candidates.empty:
            returns.append(0.0)
            continue
        weights = uniqueness_weights(in_sample)                      # full-frame, aligned index
        scores = estimator.score(candidates, as_of=as_of, weights=weights)
        # Per-wallet deflation: n_trials = the CANDIDATE count being selected among (deflation.py
        # contract), NOT N_GRID — the grid trial count is the grid-level Validators' bar (8c).
        scores = apply_deflation_gate(scores, in_sample, grid_point.deflator, n_trials=len(scores))
        if scores.empty:
            returns.append(0.0)
            continue
        follow = policy.step(prev, scores, live_pnl, as_of=as_of)
        admissions = _churn(prev, follow)

        pnl = runner.run(list(follow["wallet"]), flat_usd=flat_usd)
        window = pnl[(pnl["period_end"] > as_of) & (pnl["period_end"] <= as_of + horizon_secs)]
        gross = _weighted_window_pnl(window, follow)
        returns.append(gross - grid_point.churn_cost * admissions)
        live_pnl = pd.concat([live_pnl, window], ignore_index=True)
        prev = follow
    return pd.Series(returns, index=list(as_of_points), name=grid_point.key)


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


def run_trajectories(grid: list, ss: SuffStats, runner, **kwargs) -> pd.DataFrame:
    """8c — run every grid point's trajectory into a per-period return matrix (index = ``as_of``,
    columns = config keys) for the grid-level Validators."""
    return pd.DataFrame({gp.key: run_trajectory(gp, ss, runner, **kwargs) for gp in grid})


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
                 seed: int = 0) -> dict:
    """Apply the grid-level honesty layer at the pre-registered ``N_GRID`` (LANDMINE-1):
    Deflated-Sharpe per config, PBO, Romano-Wolf, Hansen-SPA. Returns the assembled results."""
    moments = _config_moments(return_matrix)
    deflated = DeflatedSharpe().deflate(
        moments[["sr", "n_obs", "skew", "kurt"]], n_trials=n_grid, as_of=0)
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


def winner_uncertainty(moments: pd.DataFrame, *, top_q: float = 0.05) -> dict:
    """AKM/MRSW/FCR winner's-curse uncertainty on the config leaderboard (each config is an 'arm'
    with estimate = mean per-period return, SE from its dispersion). Reports the conditional
    median-unbiased estimate + CI for the winning config, the top-1 rank confidence set, and
    FCR-adjusted CIs for the selected (positive-mean) configs."""
    finite = moments[np.isfinite(moments["se"])]
    if finite.empty:
        return {"available": False}
    estimates = finite["mean"].to_numpy()
    ses = finite["se"].to_numpy()
    akm = akm_inference_on_winners(estimates, ses)
    mrsw = mrsw_rank_cs(estimates, ses, tau=1)
    selected = finite["mean"].to_numpy() > 0
    fcr = fcr_selected_ci(estimates, ses, selected, q=top_q)
    configs = list(finite.index)
    return {
        "available": True,
        "winner_config": configs[akm["winner"]],
        "akm": akm,
        "mrsw_top1_cs": [configs[i] for i in mrsw.index[mrsw["in_top_tau_cs"]]],
        "fcr": fcr.assign(config=[configs[i] for i in fcr["index"]]),
    }


def select_winner_or_nogo(return_matrix: pd.DataFrame, *, baseline_key: str, n_grid: int,
                          cpr: dict, alpha: float = 0.05, seed: int = 0) -> dict:
    """The §Acceptance bar (issue #421): a config is the recommended winner only if it beats
    ``t_stat_baseline`` on cumulative forward copy P&L net of churn, AND that margin survives
    grid-deflated significance (low PBO / Romano-Wolf superior / Hansen-SPA at ``N_GRID``), AND the
    ``brown_goetzmann_cpr`` go-check passes (``cpr``, the wallet-persistence premise). Otherwise the
    deliverable is an explicit NO-GO."""
    deflation = grid_deflate(return_matrix, benchmark=baseline_key, n_grid=n_grid, seed=seed)
    moments = deflation["moments"]

    baseline_cum = float(moments.loc[baseline_key, "cum_return"])
    challengers = moments.drop(index=baseline_key)
    decision = {
        "n_grid": n_grid, "baseline_key": baseline_key, "baseline_cum_return": baseline_cum,
        "cpr_go": bool(cpr.get("go", False)),
        "pbo": float(deflation["pbo"]["pbo"].iloc[0]),
        "hansen_spa_pvalue": float(deflation["hansen_spa"]["spa_pvalue_consistent"].iloc[0]),
        "leaderboard": moments.sort_values("cum_return", ascending=False),
    }
    if challengers.empty:
        return {**decision, "winner": None, "status": "NO-GO", "reason": "no challenger configs"}

    superior = set(deflation["romano_wolf"].loc[
        deflation["romano_wolf"]["beats_benchmark"], "config"])
    best = challengers["cum_return"].idxmax()
    spa_ok = decision["hansen_spa_pvalue"] < alpha
    pbo_ok = decision["pbo"] < 0.5
    beats = float(moments.loc[best, "cum_return"]) > baseline_cum
    survives = (best in superior) and spa_ok and pbo_ok
    if beats and survives and decision["cpr_go"]:
        return {**decision, "winner": best, "status": "WINNER",
                "uncertainty": winner_uncertainty(moments),
                "reason": "beats baseline, survives grid deflation, CPR go"}
    reason = []
    if not beats:
        reason.append("does not beat baseline cum return")
    if best not in superior:
        reason.append("not Romano-Wolf superior")
    if not spa_ok:
        reason.append(f"Hansen-SPA p={decision['hansen_spa_pvalue']:.3f} >= {alpha}")
    if not pbo_ok:
        reason.append(f"PBO={decision['pbo']:.2f} >= 0.5")
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
    survivors = screen_estimators(
        ss, list(axes.estimators), as_of_points=params.as_of_points,
        train_secs=params.train_secs, horizon_secs=params.horizon_secs,
        k=params.k, keep=params.screen_keep)
    pruned_axes = BakeoffAxes(
        estimators=tuple(e for e in axes.estimators if e in survivors),
        deflators=axes.deflators, policies=axes.policies,
        criteria=axes.criteria, churn_costs=axes.churn_costs)
    manifest = pre_register_grid(pruned_axes, created_at=created_at)
    grid = pruned_axes.enumerate_grid()
    baseline_key = _baseline_key(pruned_axes)
    return_matrix = run_trajectories(
        grid, ss, runner, as_of_points=params.as_of_points,
        train_secs=params.train_secs, horizon_secs=params.horizon_secs,
        k=params.k, displacement_margin=params.displacement_margin,
        demoter_kwargs=params.demoter_kwargs, flat_usd=params.flat_usd)
    split_at = int(np.median(params.as_of_points))
    cpr = wallet_persistence_cpr(ss, split_at=split_at)
    decision = select_winner_or_nogo(
        return_matrix, baseline_key=baseline_key, n_grid=manifest["n_grid"], cpr=cpr, seed=seed)
    return {"manifest": manifest, "survivors": survivors, "return_matrix": return_matrix,
            "cpr": cpr, "decision": decision}


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


def main() -> None:  # pragma: no cover (operator entry; CI exercises the stage functions)
    """Operator entry: materialize suff_stats from the cache, run the bake-off against the real
    ``pe-backtest``, write the leaderboard / manifest / decision, cross-check ``paper_fills``."""
    import argparse
    import time

    import ranker_duck  # lazy: duckdb is not on every dev box (issue #421 runtime note)

    from . import suff_stats as suff_stats_mod

    ap = argparse.ArgumentParser(description="issue #421 ranker bake-off (operator run)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--pe-backtest", required=True, help="path to the pe-backtest binary")
    ap.add_argument("--cache", required=True, help="path to wallet_cache.db / parquet root")
    ap.add_argument("--train-days", type=int, default=180)
    ap.add_argument("--horizon-days", type=int, default=30)
    ap.add_argument("--steps", type=int, default=6)
    ap.add_argument("--step-days", type=int, default=30)
    ap.add_argument("--start-unix", type=int, required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = ranker_duck.get_engine(args.cache)
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

    # Cross-check the recommended wallets (the winning — or baseline — estimator's latest ranking)
    # against the live cohort's realized P&L from Supabase (issue #421 §Deliverable).
    try:
        winner = result["decision"]["winner"]
        est_name = winner.split("|")[0] if winner else BASELINE_ESTIMATOR
        in_sample, _ = split_walkforward(ss, as_of=as_of_points[-1], train_secs=params.train_secs,
                                         horizon_secs=params.horizon_secs)
        scores = ESTIMATOR_REGISTRY[est_name]().score(
            in_sample, as_of=as_of_points[-1], weights=uniqueness_weights(in_sample))
        crosscheck = paper_fills_crosscheck(scores.reset_index(), fetch_paper_fills_realized_pnl())
        crosscheck.to_csv(out_dir / "paper_fills_crosscheck.csv", index=False)
        print(f"paper_fills cross-check: {int(crosscheck['disagree'].sum())} live-loser flags "
              f"of {len(crosscheck)} overlapping wallets")
    except Exception as exc:  # advisory: an operator env without Supabase creds must not abort
        print(f"paper_fills cross-check skipped: {exc}")


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    main()
