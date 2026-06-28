"""The bake-off driver — staged sweep over the pre-registered grid (issue #421, PR5, step 8).

Composes the harness modules (estimators / deflation / selectors / demotion / policies /
oos_validation) into the three-stage sweep that emits the §Deliverable:

  (8a) estimator screen on cheap point-in-time forward copy P&L — ADVISORY ranking only (#445
       defect 4: it no longer PRUNES; its forward-payoff proxy mismatches the realized+MTM objective);
  (8b) PRE-REGISTER ``N_GRID`` over ALL estimator axes x criteria x the 4 policies (LANDMINE-1:
       the grid-level deflation is only valid against a trial count fixed BEFORE scoring);
  (8c) run every registered config as a full walk-forward TRAJECTORY (the policy axis calls ``pe-backtest``
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
# Operator ergonomics: allow ``python scripts/ranker/bakeoff.py …`` in addition to the
# ``python -m ranker.bakeoff …`` form. Run as a bare script, this file's own directory
# (``scripts/ranker/``) is ``sys.path[0]``, so the stdlib ``import selectors`` performed
# by ``subprocess`` (below) resolves to the harness ``scripts/ranker/selectors.py`` and
# crashes at import (a package-relative import with no parent). Re-exec as the proper
# package module — ``scripts/`` on ``sys.path``, ``scripts/ranker/`` off it — so both the
# relative harness imports and the stdlib imports resolve correctly. Skipped under
# ``-m``/``import`` (then ``__package__`` is ``"ranker"``, so this is a no-op).
if __name__ == "__main__" and not __package__:
    import os as _os
    import runpy as _runpy
    import sys as _sys

    _here = _os.path.dirname(_os.path.abspath(__file__))
    _scripts = _os.path.dirname(_here)
    _sys.path[:] = [p for p in _sys.path if _os.path.abspath(p or ".") != _here]
    if _scripts not in _sys.path:
        _sys.path.insert(0, _scripts)
    _runpy.run_module("ranker.bakeoff", run_name="__main__", alter_sys=True)
    raise SystemExit(0)

import argparse
import concurrent.futures
import itertools
import json
import os
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import kurtosis, skew

from ranker_decay import weighted_stats  # reuse #366 weighted statistics — gate agrees with ranker

from . import Criteria, FollowSet, SuffStats, WalletScores
from .demotion import EmpiricalBernsteinDemoter
from .deflation import DeflatedSharpe
from .estimators import REGISTRY as ESTIMATOR_REGISTRY, _SD_FLOOR
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

# issue #436 Phase E: `is_horizon_mtm` flags the single per-wallet forward-MTM row
# (`unrealized_pnl` = the window flow; realized day rows carry `is_horizon_mtm=False`,
# `unrealized_pnl=0`). `open_at_horizon`/`marked_at_horizon` are MTM coverage counts on
# the MTM row (0 on day rows). The objective sums realized+unrealized over the window;
# the realized-only demoter/live_pnl path filters `~is_horizon_mtm`.
# Phase F / F3a: `positions_in_window` (open-at-horizon + closed-in-window) is the
# open-fraction denominator, and `resolution_lags_secs` (per open-at-horizon position,
# `resolved_at − as_of`; `-1` = censored) is the resolution-lag distribution — ADVISORY
# diagnostics carried on the MTM row only, never inputs to the objective or the verdict.
_PNL_COLUMNS = ["wallet", "period_end", "realized_pnl", "unrealized_pnl", "n_fills", "notional",
                "is_horizon_mtm", "open_at_horizon", "marked_at_horizon",
                "positions_in_window", "resolution_lags_secs"]
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
# B4 (#436): minimum walk-forward periods (as_of cutoffs) before the CSCV / Romano-Wolf / Hansen-SPA
# / DSR panel is trustworthy; below it those bootstrap gates degenerate to a SILENT always-NO-GO, so
# the winner gate emits an EXPLICIT "insufficient periods" verdict and `--steps` defaults to it.
RANKER_MIN_PERIODS = 24      # ranker_min_periods — min as_of points for a trustworthy verdict [GATE]
# Phase D (#436) calibrated knobs. The per-admission copy cost (D3) anchors to a real entry cost:
# the copy-trader is a TAKER on entry and holds to resolution (no exit), so per new $25 position the
# cost ≈ the Polymarket taker fee `25·feeRate·(1−p̄)` ≈ $0.625 at a blended ~0.05 rate and band-center
# p̄≈0.5, plus ~$0.12 slippage ≈ $0.75 (docs/reference; swept {0, this} for sensitivity, glossary-
# tunable). The compute ceiling (D2) bounds the deliberate search surface: `n_grid_full × steps`
# (the naive injected-set backtest count; the in-run memo collapses it far below this) may not exceed
# it — the default sweep is 5·2·4·12·2 = 960 configs × 24 steps = 23 040, under the 30 000 ceiling.
RANKER_CHURN_COST_USD = 0.75       # ranker_churn_cost_usd — per-admission copy entry cost (USD) [D3]
RANKER_BAKEOFF_MAX_BACKTESTS = 30_000  # ranker_bakeoff_max_backtests — n_grid_full × steps cap [D2]

# Operator-run criteria SWEEP levels (D1). These are research-grid levels — the winning level feeds
# #417 — so they live here + docs/31, NOT as single glossary defaults (the glossary'd swept knob is
# `ranker_min_trl`). Each tuple is ordered OPERATOR-DEFAULT-FIRST, so `build_criteria_grid()[0]` —
# the canonical level the 8a estimator screen runs under (A3↔D1) and the baseline config's criteria —
# is the operator default by construction (itertools.product varies the LAST axis fastest).
TTR_HOURS_LEVELS = (72.0, 24.0, 48.0)          # TTR horizon sweep (hours); 72h = operator default
PRICE_BANDS = ((0.15, 0.85), (0.30, 0.70))     # entry-band sweep; the wide 0.15–0.85 band first
MIN_TRL_LEVELS = (0, 20)                        # ranker_min_trl sweep; 0 = no gate (default), 20 = small
OPERATOR_DEFAULT_CRITERIA = Criteria(
    active_within_secs=0, ttr_hours=72.0, price_min=0.15, price_max=0.85,
    half_life_days=0.0, min_trl=0)              # == build_criteria_grid()[0]; the canonical level


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


def build_criteria_grid(*, ttr_hours_levels=TTR_HOURS_LEVELS, price_bands=PRICE_BANDS,
                        min_trl_levels=MIN_TRL_LEVELS, active_within_secs: int = 0,
                        half_life_days: float = 0.0) -> tuple:
    """D1 (#436): the swept criteria grid = the cartesian product of the TTR / entry-band / MinTRL
    levels. The level tuples are ordered operator-default-FIRST (72h TTR, the wide 0.15–0.85 band,
    no-MinTRL-gate) and ``itertools.product`` varies the last axis fastest, so ``[0]`` — the
    canonical level the 8a estimator screen runs under (A3↔D1) and the baseline config's criteria —
    is the operator default by construction. ``active_within_secs`` and ``half_life_days`` are held
    constant across the grid: ``half_life_days`` is carried-but-INERT in v1 (consumed by no
    estimator/filter — it only labels the config key), so sweeping it would merely duplicate grid
    columns; wiring recency decay into estimator scoring is deferred to #417/#418. A level list with
    a repeated entry produces a colliding config key, caught loudly by ``pre_register_grid`` (A8)."""
    return tuple(
        Criteria(active_within_secs, ttr, pmin, pmax, half_life_days, trl)
        for ttr, (pmin, pmax), trl in itertools.product(
            ttr_hours_levels, price_bands, min_trl_levels))


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
        if not (n_eff >= 2 and sd > _SD_FLOOR):    # D2 (#436): float-noise sd (~1e-16) is not signal
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

    def run(self, wallets: list, *, flat_usd: float,
            window: "tuple | None" = None) -> pd.DataFrame:
        # Per-CALL scratch dir: `pe-backtest` writes a FIXED filename (`pnl_by_period.ndjson`) into
        # `PE_BACKTEST_OUTPUT_DIR`, so the parallel `run_trajectories` seam (concurrent backtests)
        # requires a DISTINCT dir per call or they clobber one another's wallet file / output. A fresh
        # dir also subsumes the #445 defect-9 stale-output unlink: a binary that emits nothing this run
        # yields an empty frame (missing file), never a re-parse of a prior run's P&L. The result frame
        # is independent of the (random) dir name, so determinism is preserved; cleaned up after parse.
        self.output_dir.mkdir(parents=True, exist_ok=True)
        call_dir = Path(tempfile.mkdtemp(dir=self.output_dir))
        try:
            wallet_file = call_dir / "injected_wallets.txt"
            # #445 defect 9: the cache stores lowercase wallet_hex and pe-backtest matches injected
            # wallets against it, so a stray upper/mixed-case id would silently match NOTHING.
            # Lowercase defensively (the docstring's "lowercase-hex" contract) so a casing slip
            # upstream cannot zero out the fills.
            wallet_file.write_text("\n".join(w.lower() for w in wallets) + "\n")
            out_file = call_dir / "pnl_by_period.ndjson"
            env = {
                **os.environ,
                "PE_BACKTEST_INJECTED_WALLETS_PATH": str(wallet_file),
                "PE_BACKTEST_FLAT_USD": str(flat_usd),
                "PE_BACKTEST_MAX_TRADE_COUNT": "0",
                "PE_BACKTEST_OUTPUT_DIR": str(call_dir),
                "PE_BOOTSTRAP_CACHE_PATH": self.cache_path,
                **self.extra_env,
            }
            # issue #436 Phase E: pass the forward-MTM window (as_of, horizon) so the
            # binary marks still-open positions at the horizon (PE_BACKTEST_MTM_WINDOW_*).
            # Without it the binary keeps the unrealized_pnl=0.0 sentinel.
            if window is not None:
                as_of, horizon_end = window
                env["PE_BACKTEST_MTM_WINDOW_START_UNIX"] = str(int(as_of))
                env["PE_BACKTEST_MTM_WINDOW_END_UNIX"] = str(int(horizon_end))
            subprocess.run([self.binary], env=env, check=True)
            return parse_pnl_by_period(out_file)
        finally:
            shutil.rmtree(call_dir, ignore_errors=True)


def parse_pnl_by_period(path) -> pd.DataFrame:
    """Parse a ``pnl_by_period.ndjson`` (PR3 emit) into a frame with the pinned columns. An empty
    or absent file yields an empty frame (an injected set may have no fills)."""
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=_PNL_COLUMNS)
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        return pd.DataFrame(columns=_PNL_COLUMNS)
    df = pd.DataFrame(rows)
    # Back-compat (Phase F / F3a): a pre-F3a `pe-backtest` emits no resolution diagnostics. Backfill
    # safe defaults so the parse never KeyErrors — `positions_in_window` falls back to the open count
    # and `resolution_lags_secs` to empty (no lags surfaced) rather than fabricating data.
    if "positions_in_window" not in df.columns:
        df["positions_in_window"] = df.get("open_at_horizon", 0)
    if "resolution_lags_secs" not in df.columns:
        df["resolution_lags_secs"] = [[] for _ in range(len(df))]
    return df[_PNL_COLUMNS]


def _lag_distribution(resolved_secs: list, censored: int) -> dict:
    """Summarize the open-at-horizon resolution-lag distribution (issue #436 Phase F / F3a): the
    spread of ``resolved_at − as_of`` (in DAYS) for positions still open at the horizon that
    eventually settled, plus the ``censored`` count of positions whose market never resolved in the
    cache. Quantifies *how slow* the unresolved forward edge is — the resolution-speed residual that
    coverage (``marked / open``) alone does not measure. ADVISORY: never feeds the verdict."""
    n = len(resolved_secs)
    total = n + max(int(censored), 0)
    if n == 0:
        return {"n": 0, "censored": int(max(censored, 0)),
                "censored_frac": (1.0 if total > 0 else None),
                "p50_days": None, "p90_days": None, "max_days": None}
    a = np.sort(np.asarray(resolved_secs, dtype=float)) / 86_400.0     # seconds → days
    return {"n": n, "censored": int(max(censored, 0)),
            "censored_frac": (float(censored / total) if total > 0 else None),
            "p50_days": float(np.percentile(a, 50)),
            "p90_days": float(np.percentile(a, 90)),
            "max_days": float(a[-1])}


class _SingleFlightCache:
    """Thread-safe memo with single-flight: concurrent ``get_or_compute(key, factory)`` for the SAME
    key invokes ``factory`` exactly once; the other callers block on the in-flight result. The cached
    value is independent of WHICH thread computed it or in what order — ``factory`` must be a pure
    function of ``key`` — so it preserves bit-identical results when ``run_trajectories`` runs
    trajectories concurrently (the parallel seam needs a thread-safe backtest memo AND uniqueness
    cache, both of which are pure-of-key). With one thread it degenerates to a plain memo. ``calls``
    counts distinct SUCCESSFUL ``factory`` invocations (a raising factory is re-raised to every waiter
    and not counted); ``lookups`` counts total requests (so the realized dedup speedup is
    ``lookups / calls``). ``factory`` runs OUTSIDE the lock, so a slow compute (a ``pe-backtest``
    subprocess) never blocks a hit on a different key."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict = {}
        self._inflight: dict = {}                 # key -> {"event", "exc"} record while one thread computes
        self.calls = 0
        self.lookups = 0

    def get_or_compute(self, key, factory):
        with self._lock:
            self.lookups += 1
            if key in self._cache:
                return self._cache[key]
            rec = self._inflight.get(key)
            owner = rec is None
            if owner:
                rec = {"event": threading.Event(), "exc": None}
                self._inflight[key] = rec
        if not owner:                             # another thread owns this key — wait for its result
            rec["event"].wait()
            with self._lock:
                if key in self._cache:
                    return self._cache[key]
            # The owner failed. Re-raise ITS exception (the real pe-backtest CalledProcessError, not a
            # generic stand-in) so a parallel run fails with the same diagnostics as a serial one; the
            # captured `exc` is read from the record this waiter already holds, so a NEW owner that has
            # since started recomputing for future callers cannot mask it.
            if rec["exc"] is not None:
                raise rec["exc"]
            raise RuntimeError(f"single-flight: owner failed to compute {key!r}")
        try:
            value = factory()
        except BaseException as exc:              # publish to the waiters, wake them, then re-raise
            rec["exc"] = exc
            with self._lock:
                self._inflight.pop(key, None)
            rec["event"].set()
            raise
        with self._lock:
            self._cache[key] = value
            self._inflight.pop(key, None)
            self.calls += 1
        rec["event"].set()
        return value


class MemoizingBacktestRunner:
    """D2 (#436): an in-run memo over an inner ``BacktestRunner``, keyed on
    ``(tuple(sorted(wallets)), flat_usd, window)``. The injected-set backtest is a deterministic,
    order-independent pure function of the followed SET and the forward-MTM ``window`` (issue #436
    Phase E: the Rust path marks still-open positions at the window's horizon, so the emitted
    ``unrealized_pnl`` flow depends on ``(as_of, horizon)`` — hence ``window`` is part of the key; the
    churn deduction still happens in Python AFTER). Two grid points sharing a followed set AT THE SAME
    STEP re-run the SAME backtest: GUARANTEED for the two churn-cost levels (churn never changes the
    set or the window, only the post-hoc subtraction) and common across estimators/deflators, so
    caching collapses the churn axis exactly 2× plus every repeated (followed set, window) within ONE
    bake-off run. (Pre-E the output was window-agnostic so identical sets deduped across steps too;
    adding ``window`` trades that rare cross-step coincidence for the per-step horizon mark — the
    churn-axis 2× win is preserved.)

    Per-run only — instantiated fresh in ``run_bakeoff`` with NO persistence or eviction. The
    persistent cross-run cache is the exact key/version/eviction-correctness risk this epic guards,
    so it is deferred (#436 D2) until iteration cost proves prohibitive. The cached frame is never
    mutated downstream (the window boolean-slice and ``pd.concat`` both copy), so it is returned
    directly. Backed by a thread-safe ``_SingleFlightCache`` so the parallel ``run_trajectories`` seam
    can share ONE memo across concurrent trajectories without double-running a followed set or racing
    the counters; ``calls`` (distinct followed sets = inner invocations) and ``lookups`` (total
    requests) read through to it and give the realized speedup ``lookups / calls``."""

    def __init__(self, inner):
        self.inner = inner
        self._cache = _SingleFlightCache()

    def run(self, wallets: list, *, flat_usd: float,
            window: "tuple | None" = None) -> pd.DataFrame:
        key = (tuple(sorted(wallets)), flat_usd, window)
        return self._cache.get_or_compute(
            key, lambda: self.inner.run(list(wallets), flat_usd=flat_usd, window=window))

    @property
    def calls(self) -> int:
        return self._cache.calls

    @property
    def lookups(self) -> int:
        return self._cache.lookups


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


def _uniqueness_cached(in_sample, *, key, cache):
    """``uniqueness_weights`` memoized per ``key`` (#451). Within a single ``run_bakeoff`` the
    in-sample is fully determined by ``(criteria, as_of)`` — ``ss``, ``train_secs`` and
    ``horizon_secs`` are run-invariant (``BakeoffParams`` scalars, not grid axes), and the criteria
    slice + walk-forward split are deterministic in them — so ``screen_estimators`` and every config's
    ``run_trajectory`` otherwise recompute the SAME O(positions × segments) statistic up to
    ``n_grid × steps`` times. A per-run ``cache`` dict keyed on ``(criteria, as_of)`` collapses that
    to once per distinct in-sample, BIT-IDENTICALLY (same rows/order -> same positional weights, used
    read-only downstream). The cache MUST stay scoped to one ``run_bakeoff``; ``cache=None`` disables
    it (back-compat for direct callers / tests). ``run_bakeoff`` passes a thread-safe
    ``_SingleFlightCache`` (so the parallel ``run_trajectories`` seam computes each ``(criteria,
    as_of)`` cold pass exactly once even under concurrency); a plain ``dict`` is still accepted for
    single-threaded callers."""
    if cache is None:
        return uniqueness_weights(in_sample)
    if hasattr(cache, "get_or_compute"):          # thread-safe single-flight (parallel run_bakeoff)
        return cache.get_or_compute(key, lambda: uniqueness_weights(in_sample))
    weights = cache.get(key)                       # back-compat: a plain dict (serial callers / tests)
    if weights is None:
        weights = uniqueness_weights(in_sample)
        cache[key] = weights
    return weights


def screen_estimators(ss: SuffStats, estimators: list, *, as_of_points: list,
                      train_secs: int, horizon_secs: int, k: int, keep: int,
                      criteria: "Criteria | None" = None,
                      uniqueness_cache: "dict | None" = None) -> list:
    """8a — rank estimators by mean point-in-time forward copy P&L of their top-k pick and return the
    best ``keep`` (the §Acceptance benchmark ``t_stat_baseline`` is always retained). Cheap: scores
    once per ``as_of`` and reads the forward net edge directly. **#445 defect 4: this ranking is
    ADVISORY ONLY** — it no longer prunes the run-set (its forward-payoff proxy mismatches the
    realized + CLOB-MTM horizon objective), so ``run_bakeoff`` carries ALL estimators into the grid
    and uses this return value as diagnostics, not as the run-set filter it once was.

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
            weights = _uniqueness_cached(                                # full-frame, aligned index
                in_sample, key=(criteria, as_of), cache=uniqueness_cache)  # #451 memo
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


# ───────────────────────── true-CLV coverage preflight (#445 defect 10) ─────────────────────────
def true_clv_preflight(ss: SuffStats, *, views_present: bool, estimators,
                       coverage_warn_pct: "int | None" = None) -> "tuple[tuple, dict]":
    """#445 defect 10: gate the ``true_clv`` estimator on real CLOB coverage so it cannot silently
    contribute an all-NaN ranking to the bake-off grid. Returns ``(filtered_estimators, report)``.

    Excludes ``true_clv`` (and flags it in ``report``) when it is requested AND either the CLOB views
    are absent (``views_present=False``) OR position-level ``true_clv_close`` coverage is below
    ``coverage_warn_pct``. The exclusion is VISIBLE — the estimator drops out of the pre-registered
    grid and the report carries a hard flag — never a silent all-NaN arm. Raises ``ValueError`` when
    the exclusion leaves NO estimator candidate, so the abort happens UPSTREAM of ``run_bakeoff``
    (before ``N_GRID`` is frozen). The run may continue for the remaining non-CLV estimators.

    ``coverage_warn_pct`` defaults to the canonical ``true_clv_coverage_warn_pct`` (``_GLOSSARY.md`` /
    ``suff_stats.TRUE_CLV_COVERAGE_WARN_PCT``); it is resolved lazily so importing ``bakeoff`` does not
    eagerly pull ``suff_stats``/duckdb. ``views_present`` is ``market_price_history`` AND
    ``token_conditions`` both registered. No second threshold is introduced (issue contract 6)."""
    if coverage_warn_pct is None:
        from .suff_stats import TRUE_CLV_COVERAGE_WARN_PCT
        coverage_warn_pct = TRUE_CLV_COVERAGE_WARN_PCT
    ests = list(estimators)
    report = {"true_clv_requested": "true_clv" in ests, "true_clv_excluded": False, "reason": None,
              "views_present": bool(views_present), "coverage_pct": None,
              "coverage_warn_pct": int(coverage_warn_pct)}
    if "true_clv" not in ests:
        return tuple(ests), report
    total = int(len(ss))
    covered = (int(ss["true_clv_close"].notna().sum())
               if ("true_clv_close" in ss.columns and total) else 0)
    pct = (100 * covered // total) if total else 0
    report["coverage_pct"] = pct
    if not views_present:
        report["reason"] = "CLOB views absent (need market_price_history + token_conditions)"
    elif pct < coverage_warn_pct:
        report["reason"] = (f"true_clv_close coverage {pct}% < "
                            f"true_clv_coverage_warn_pct={coverage_warn_pct}%")
    if report["reason"] is not None:
        report["true_clv_excluded"] = True
        ests = [e for e in ests if e != "true_clv"]
        if not ests:
            raise ValueError(
                "true_clv preflight excluded the only estimator candidate "
                f"({report['reason']}); no estimator remains — enable the CLOB views or include a "
                "non-CLV estimator before freezing the grid")
    return tuple(ests), report


# ───────────────────────── 8c: policy trajectory ─────────────────────────
def _churn(prev: FollowSet, follow: FollowSet) -> float:
    """Turnover charged per step = the POSITIVE-PART L1 weight movement
    ``Σ max(weight_followᵢ − weight_prevᵢ, 0)`` over the union of wallets (a wallet absent on a side
    has weight 0). This is the capital newly deployed into a new-or-larger position — the only move
    that incurs an entry cost under hold-to-resolution; a weight DECREASE / eviction halts new copies
    but is not itself charged.

    D3 (#436): for the three hard-set policies (every followed wallet at weight 1.0) this reduces
    EXACTLY to the old count of newly-admitted wallets — each admission contributes ``+1.0``,
    incumbents ``0``, evictions clamp to ``0`` — so their per-admission semantics are unchanged. For
    ``policy_online_weighting``, whose continuous weights are normalized to sum to the set size (the
    same total mass as a hard top-k of ``k`` — ``selectors.py``), it ALSO charges a wallet ramping
    its weight UP, which the old admission count missed (the wallet was already in the set). So
    ``churn_cost`` is USD per unit of weight increase = USD per newly admitted/enlarged unit ($25)
    position, scale-consistent across both policy families."""
    p = prev.set_index("wallet")["weight"] if len(prev) else pd.Series(dtype=float)
    f = follow.set_index("wallet")["weight"] if len(follow) else pd.Series(dtype=float)
    return float(f.subtract(p, fill_value=0.0).clip(lower=0.0).sum())


@dataclass
class TrajectoryResult:
    """A1 (#436): one config's trajectory output. ``returns`` is the per-period forward copy P&L net
    of churn; ``final_follow`` / ``final_scores`` are the LAST real (non-empty) followed set and its
    per-wallet scores — the deliverable substrate. Three of four policies are stateful, so this set
    is captured DURING the run: it cannot be reproduced by a cold re-application of the policy at the
    latest ``as_of`` (an empty ``prev`` degenerates knockout/hybrid/online into a memoryless top-k).
    """

    returns: pd.Series
    # #445 defect 1: ``final_follow`` / ``final_scores`` reflect the LATEST cutoff — the set the
    # config held entering the final period. They are the last non-empty real set ONLY while the run
    # ends on a live period; a no-signal FINAL cutoff leaves them EMPTY ("held nothing"), never a
    # fall-back to an earlier non-empty set. Captured during the run (a stateful policy's set cannot
    # be reproduced by a cold re-application at the latest ``as_of``).
    final_follow: FollowSet
    final_scores: WalletScores
    # E3 (#436): per-period CLOB forward-MTM coverage over the followed set — index =
    # as_of, columns = [open_at_horizon, marked_at_horizon, positions_in_window,
    # censored]. Empty when no MTM window is run. The harness surfaces coverage =
    # marked / open and (Phase F / F3a) the open-at-horizon fraction
    # open / positions_in_window per (config, period) beside the winner.
    coverage: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(
            columns=["open_at_horizon", "marked_at_horizon", "positions_in_window", "censored"]))
    # Phase F / F3a: per-period resolved lags `resolved_at − as_of` (seconds, ≥ 0; the
    # censored -1s are tallied in coverage["censored"], not here) for open-at-horizon
    # positions — the resolution-lag distribution. ADVISORY reporting only.
    lags_by_period: dict = field(default_factory=dict)


def run_trajectory(grid_point: GridPoint, ss: SuffStats, runner, *, as_of_points: list,
                   train_secs: int, horizon_secs: int, k: int,
                   displacement_margin: int, demoter_kwargs: "dict | None" = None,
                   flat_usd: float = 25.0,
                   uniqueness_cache: "dict | None" = None) -> "TrajectoryResult":
    """Run one config as a full walk-forward trajectory; return its per-period forward copy P&L net
    of churn (indexed by ``as_of``) PLUS the final-step followed set (A1). Sequential by
    construction: ``set_t -> pe-backtest(set_t) -> live_pnl_t -> policy.step -> set_{t+1}``
    (issue #421 8c).

    A9 (#436): a period with no eligible set / no surviving signal / an empty followed set yields
    ``NaN`` (the config held nothing — EXCLUDED from its moments), NOT ``0.0`` (which would be a
    real, low-variance "traded and made $0" and could out-rank a live config).

    #445 defect 1: "held nothing" also RESETS carry — ``prev`` returns to an empty set and the policy
    is rebuilt via ``build_policy`` (clearing online weights / incumbency) so a later re-entry pays
    full churn rather than inheriting the pre-gap set, and the final deliverable is cleared so a
    no-signal FINAL cutoff reports an empty set rather than stale history. ``live_pnl`` (realized-only
    evidence) is preserved across the gap; no synthetic P&L is fabricated for the no-signal period."""
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
    coverage_rows: dict = {}                                          # E3: as_of -> (open, marked, ...)
    lags_by_period: dict = {}                                         # F3a: as_of -> resolved lags

    def _no_signal() -> None:
        # #445 defect 1: the config held nothing this period. Record NaN, reset `prev` to empty and
        # REBUILD the policy via the build_policy seam (clears OnlineWeighting's online state; with an
        # empty `prev` the knockout/hybrid incumbency also resets), and clear the final deliverable so
        # a no-signal FINAL cutoff is reported empty — never a fall-back to the most recent real set.
        nonlocal prev, policy, final_follow, final_scores
        returns.append(np.nan)
        prev = pd.DataFrame({"wallet": [], "weight": []})
        policy = build_policy(grid_point.policy, k=k, demoter=demoter,
                              displacement_margin=displacement_margin)
        final_follow = pd.DataFrame({"wallet": [], "weight": []})
        final_scores = pd.DataFrame(columns=["score", "rank"])

    for as_of in as_of_points:
        in_sample, _forward = split_walkforward(
            ss_c, as_of=as_of, train_secs=train_secs, horizon_secs=horizon_secs)
        eligible = eligible_wallets(in_sample, grid_point.criteria, as_of=as_of)
        candidates = in_sample[in_sample["wallet"].isin(eligible)]   # label-preserving slice
        if candidates.empty:
            _no_signal()                                             # A9 + #445: no eligible set
            continue
        weights = _uniqueness_cached(                                # full-frame, aligned index
            in_sample, key=(grid_point.criteria, as_of), cache=uniqueness_cache)  # #451 memo
        scores = estimator.score(candidates, as_of=as_of, weights=weights)
        # Per-wallet deflation: n_trials = the CANDIDATE count being selected among (deflation.py
        # contract), NOT N_GRID — the grid trial count is the grid-level Validators' bar (8c).
        scores = apply_deflation_gate(scores, in_sample, grid_point.deflator,
                                      n_trials=len(scores), weights=weights)
        if scores.empty:
            _no_signal()                                             # A9 + #445: no surviving signal
            continue
        follow = policy.step(prev, scores, live_pnl, as_of=as_of)
        if follow.empty:
            _no_signal()                                             # A9 + #445: policy holds nothing
            continue
        turnover = _churn(prev, follow)          # D3 (#436): positive-part L1 weight movement

        # E (#436): pass the forward-MTM window so the binary marks still-open positions at
        # the horizon. The objective sums realized + unrealized (the MTM flow) over the window.
        pnl = runner.run(list(follow["wallet"]), flat_usd=flat_usd,
                         window=(as_of, as_of + horizon_secs))
        window = pnl[(pnl["period_end"] > as_of) & (pnl["period_end"] <= as_of + horizon_secs)]
        is_mtm = window["is_horizon_mtm"].astype(bool)
        gross = _weighted_window_pnl(window, follow)                 # realized + unrealized (E2b)
        returns.append(gross - grid_point.churn_cost * turnover)
        # E2b: the demoter / live_pnl accumulation stays REALIZED-only (the Maurer-Pontil bound
        # assumes settled per-period P&L, and counts each row as a period) — drop the MTM rows.
        live_pnl = pd.concat([live_pnl, window[~is_mtm]], ignore_index=True)
        # E3 / F3a: per-period MTM coverage + resolution-lag diagnostics over the followed set (the
        # backtest ran exactly these wallets). `resolution_lags_secs` is a per-row list; flatten it
        # across the period's MTM rows, then split resolved (≥0) from censored (-1) lags.
        mtm_rows = window[is_mtm]
        period_lags = [x for arr in mtm_rows["resolution_lags_secs"]
                       if isinstance(arr, (list, tuple, np.ndarray)) for x in arr]
        resolved = [int(x) for x in period_lags if x >= 0]
        censored = sum(1 for x in period_lags if x < 0)
        coverage_rows[as_of] = {
            "open_at_horizon": int(mtm_rows["open_at_horizon"].sum()),
            "marked_at_horizon": int(mtm_rows["marked_at_horizon"].sum()),
            "positions_in_window": int(mtm_rows["positions_in_window"].sum()),
            "censored": censored,
        }
        lags_by_period[as_of] = resolved
        prev = follow
        final_follow, final_scores = follow, scores                 # A1: most recent real set
    coverage = pd.DataFrame.from_dict(coverage_rows, orient="index") if coverage_rows else \
        pd.DataFrame(columns=["open_at_horizon", "marked_at_horizon", "positions_in_window", "censored"])
    return TrajectoryResult(
        returns=pd.Series(returns, index=list(as_of_points), name=grid_point.key, dtype=float),
        final_follow=final_follow, final_scores=final_scores, coverage=coverage,
        lags_by_period=lags_by_period)


def _weighted_window_pnl(window: pd.DataFrame, follow: FollowSet) -> float:
    """Followed-set forward copy P&L over a window = sum_w weight_w x (realized + unrealized)_w.

    E2b (#436): include ``unrealized_pnl`` — the CLOB forward mark-to-market *flow* the binary
    emits on the per-wallet horizon row (``is_horizon_mtm``). Day rows carry ``unrealized_pnl=0``
    and the horizon row carries ``realized_pnl=0``, so summing BOTH columns per wallet adds the
    marked open-position edge to the realized closes without double-counting. Without the MTM
    window the horizon rows are absent and ``unrealized_pnl`` is all-zero (realized-only, as before).
    """
    if window.empty or follow.empty:
        return 0.0
    per_wallet = window.groupby("wallet")[["realized_pnl", "unrealized_pnl"]].sum().sum(axis=1)
    weight = follow.set_index("wallet")["weight"]
    common = per_wallet.index.intersection(weight.index)
    if common.empty:
        return 0.0
    return float((per_wallet.loc[common] * weight.loc[common]).sum())


def run_trajectories(grid: list, ss: SuffStats, runner, *, max_workers: int = 1,
                     **kwargs) -> "tuple[pd.DataFrame, dict]":
    """8c — run every grid point's trajectory. Returns ``(return_matrix, results)`` where
    ``return_matrix`` is the per-period return matrix (index = ``as_of``, columns = config keys) for
    the grid-level Validators and ``results`` maps each config key -> its ``TrajectoryResult`` (A1:
    so the winning config's frozen final-step followed set is selected directly, not re-derived).

    ``max_workers > 1`` runs the (mutually independent) trajectories concurrently on a thread pool.
    Each ``run_trajectory`` stays sequential WITHIN itself (``set_t -> pe-backtest -> live_pnl ->
    policy -> set_{t+1}``); configs share no mutable state except the ``runner`` memo and the
    ``uniqueness_cache``, both thread-safe single-flight (``_SingleFlightCache``). The dominant cost is
    the ``pe-backtest`` subprocess, which releases the GIL, so threads overlap it (the GIL still
    serialises the Python scoring — a deliberate, low-risk speedup vs the fork/BLAS footguns of a
    process pool; the latter is the future lever for a Python-scoring-dominated full-universe run). The
    parallel result is BIT-IDENTICAL to serial: each trajectory is a deterministic pure function of
    ``(grid_point, ss, params)``, and the matrix columns are assembled in fixed GRID order below — not
    completion order. ``max_workers <= 1`` is the original serial path."""
    if max_workers and max_workers > 1 and len(grid) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {gp.key: pool.submit(run_trajectory, gp, ss, runner, **kwargs) for gp in grid}
            results = {key: fut.result() for key, fut in futures.items()}
    else:
        results = {gp.key: run_trajectory(gp, ss, runner, **kwargs) for gp in grid}
    # Assemble in GRID order (not results-insertion / thread-completion order) so the column order —
    # and every downstream Validator that reads the matrix positionally — is identical serial vs
    # parallel. (`results` is already grid-ordered, but iterate `grid` to make the invariant explicit.)
    return pd.DataFrame({gp.key: results[gp.key].returns for gp in grid}), results


# ───────────────────────── leaderboard + winner / NO-GO ─────────────────────────
def _config_moments(return_matrix: pd.DataFrame) -> pd.DataFrame:
    """Per-config performance: cumulative return, per-period Sharpe moments, and the mean/SE used
    as the AKM/MRSW/FCR 'arm' estimates."""
    rows = []
    for cfg in return_matrix.columns:
        series = return_matrix[cfg].to_numpy(dtype=float)
        n = series.size
        sd = series.std(ddof=1) if n > 1 else 0.0
        # D2 (#436): `sd > _SD_FLOOR`, not `sd > 0` — a near-constant config (sub-1e-9 float-noise
        # dispersion) would otherwise emit a spurious huge SR / tiny finite SE into the deflators and
        # the winner_uncertainty AKM CI. Treat sub-floor dispersion as undefined (se=inf, sr=0).
        se = sd / np.sqrt(n) if (n > 0 and sd > _SD_FLOOR) else np.inf
        rows.append((cfg, float(series.sum()), float(series.mean()),
                     float(series.mean() / sd) if sd > _SD_FLOOR else 0.0, n,
                     float(skew(series)) if n > 2 else 0.0,
                     float(kurtosis(series, fisher=False)) if n > 3 else 3.0, float(se)))
    return pd.DataFrame(rows, columns=["config", "cum_return", "mean", "sr", "n_obs",
                                       "skew", "kurt", "se"]).set_index("config")


def grid_deflate(return_matrix: pd.DataFrame, *, benchmark: str, n_grid: int,
                 n_trials_dsr: "int | None" = None, seed: int = 0) -> dict:
    """Apply the grid-level honesty layer (LANDMINE-1): Deflated-Sharpe per config, PBO, Romano-Wolf,
    Hansen-SPA. Returns the assembled results.

    A2/A8 (#436) — the two trial counts are kept SEPARATE by construction (do not force them equal):
      * the scalar Deflated-Sharpe ``expected_max_sharpe`` bar takes the FULL pre-screen
        ``n_trials_dsr`` (every config the menu could have produced) — the honest multiplicity;
      * ``PBO`` / ``RomanoWolf`` / ``HansenSPA`` are bootstrap Validators over the matrix COLUMNS and
        structurally cannot reflect screened-out / unrun configs, so they take ``n_grid`` (the
        run-set columns, incl. the benchmark).
    #445 defect 4: with the 8a estimator screen no longer PRUNING, the run-set IS the full pre-screen
    grid, so the two counts now COINCIDE in the current driver (the ``true_clv`` preflight applies
    UPSTREAM, before ``N_GRID`` is frozen, so ``n_grid_full`` already reflects any exclusion). The
    parameter split is retained so a future horizon-consistent screen that drops configs keeps the
    honest DSR multiplicity (docs/31)."""
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
                          alpha: float = RANKER_FDR_Q, seed: int = 0,
                          min_periods: int = RANKER_MIN_PERIODS) -> dict:
    """The §Acceptance bar (issue #421): a config is the recommended winner only if it beats
    ``t_stat_baseline`` on cumulative forward copy P&L net of churn, AND that margin survives
    grid-deflated significance (Romano-Wolf superior / Hansen-SPA at ``alpha`` / ``PBO`` below
    ``ranker_pbo_max``), AND the ``brown_goetzmann_cpr`` go-check passes. Otherwise the deliverable
    is an explicit NO-GO.

    A5/A6/A9/A11 (#436): the verdict iterates challengers by descending cum-return and awards the
    FIRST that is RW-superior and beats the baseline (not the cum-argmax, which may be an uncertified
    high-variance config); ``ranker_pbo_max`` / ``ranker_fdr_q`` are wired (were hardcoded); the
    no-signal panel is cleaned first; and the winner's grid-DSR is reported as an advisory.

    B4 (#436) + #445 defect 2: a verdict needs ``>= min_periods`` CLEANED walk-forward cutoffs
    (``ranker_min_periods``) or it is an explicit insufficient-periods NO-GO — below that the
    bootstrap gates degenerate silently. The floor applies to the dense matrix AFTER no-signal
    cleaning (not the raw row count); the payload reports raw/clean/dropped period counts."""
    n_grid_full = n_grid if n_grid_full is None else n_grid_full
    # B4 (#436) + #445 defect 2: the CSCV / Romano-Wolf / Hansen-SPA / DSR panel needs
    # >= min_periods walk-forward periods to be trustworthy; below it those gates degenerate to a
    # SILENT always-NO-GO. The floor applies to the CLEANED DENSE matrix (`_clean_return_matrix`
    # drops all-NaN configs + every no-signal period), NOT the raw row count: a panel with enough
    # RAW rows that cleans down below the floor must still be an EXPLICIT insufficient-periods NO-GO,
    # not reach leaderboard evaluation on a too-short dense matrix (the #445 raw-24/clean-2 leak).
    raw_periods = int(return_matrix.shape[0])
    clean, info = _clean_return_matrix(return_matrix, baseline_key=baseline_key)
    dropped_periods = int(info.get("dropped_periods", 0))
    base = {"n_grid": n_grid, "n_grid_full_pre_screen": n_grid_full, "baseline_key": baseline_key,
            "cpr_go": bool(cpr.get("go", False)),
            "dropped_configs": info.get("dropped_configs", []),
            "dropped_periods": dropped_periods, "raw_periods": raw_periods,
            # clean_periods = raw - dropped on the paths where cleaning counted periods; None only when
            # cleaning bailed before counting (e.g. baseline produced no signal at all).
            "clean_periods": (raw_periods - dropped_periods if "dropped_periods" in info else None)}
    if clean is None:
        # Always carry a (here empty) leaderboard so the operator-run output writer is uniform.
        return {**base, "leaderboard": pd.DataFrame(), "winner": None, "status": "NO-GO",
                "reason": info["reason"]}
    clean_periods = int(clean.shape[0])
    base["clean_periods"] = clean_periods
    if clean_periods < min_periods:
        return {**base, "leaderboard": pd.DataFrame(), "winner": None, "status": "NO-GO",
                "reason": f"insufficient periods: {clean_periods} clean (of {raw_periods} raw, "
                          f"{dropped_periods} dropped as no-signal) < "
                          f"ranker_min_periods={min_periods} (raise --steps)"}

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


def _followed_frame(follow: FollowSet, scores: WalletScores) -> pd.DataFrame:
    """Build the cross-check frame from the followed set's wallets (#445 defect 5), left-joining
    rank/score metadata from ``scores`` when present. Identity is ``follow.wallet`` — a stateful
    policy can hold an incumbent that is ABSENT from the fresh ``scores``, and intersecting on
    ``scores`` (the pre-#445 behaviour) would silently drop it from the live-loser check. One row per
    followed wallet; wallets absent from ``scores`` keep NaN rank/score but remain in the frame."""
    base = follow[["wallet"]].copy() if "wallet" in follow.columns else pd.DataFrame({"wallet": []})
    if scores is not None and not scores.empty:
        meta = scores.rename_axis("wallet").reset_index()        # wallet + score/rank columns
        base = base.merge(meta, on="wallet", how="left")         # left-join keeps absent-from-scores
    return base


def crosscheck_deliverable(follow: FollowSet, scores: WalletScores,
                           realized_pnl: pd.DataFrame) -> "tuple[pd.DataFrame, dict]":
    """Cross-check the deliverable's followed set against the live cohort's realized P&L (#445
    defect 5). Identity follows ``follow``: the frame is built from ``follow.wallet`` with ``scores``
    metadata left-joined, so a held incumbent missing from the fresh scores still appears in the
    check. Returns ``(crosscheck, summary)`` with ``overlap_count`` / ``overlap_fraction`` (vs
    ``len(follow)``) / ``n_follow`` / ``live_loser_flags``.

    HARD FAIL (raises ``ValueError``): a SUCCESSFUL live fetch (``realized_pnl`` already in hand) with
    a non-empty followed set but ZERO overlap is a verification failure — the deliverable wallets are
    entirely absent from the live cohort (identity/casing/source drift), NOT a clean zero-row result.
    A missing-credentials fetch is the caller's advisory skip, handled BEFORE this is called."""
    followed = _followed_frame(follow, scores)
    crosscheck = paper_fills_crosscheck(followed, realized_pnl)
    overlap_count = int(len(crosscheck))
    n_follow = int(len(follow))
    if n_follow > 0 and overlap_count == 0:
        raise ValueError(
            f"paper_fills cross-check FAILED: 0 of {n_follow} followed wallets overlap the live "
            "cohort after a successful fetch — deliverable/live identity drift (check wallet "
            "casing/source); not a clean zero-row result")
    summary = {
        "overlap_count": overlap_count, "n_follow": n_follow,
        "overlap_fraction": (overlap_count / n_follow if n_follow else 0.0),
        "live_loser_flags": int(crosscheck["disagree"].sum()) if not crosscheck.empty else 0,
    }
    return crosscheck, summary


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
    min_periods: int = RANKER_MIN_PERIODS    # B4: verdict floor passed to select_winner_or_nogo
    max_backtests: int = RANKER_BAKEOFF_MAX_BACKTESTS  # D2: n_grid_full × steps compute ceiling

    def __post_init__(self) -> None:
        # B3 (#436): forward windows ``[as_of, as_of + horizon_secs)`` must not overlap across steps,
        # or the trajectory double-counts P&L and (with B2) over-counts an incumbent's proven periods.
        # Enforce that consecutive cutoffs are spaced by >= horizon_secs — the non-overlap invariant.
        # ``main`` spaces ``as_of_points`` by ``step_days``, so this rejects --step-days < --horizon-days.
        pts = sorted(self.as_of_points)
        gaps = [b - a for a, b in zip(pts, pts[1:])]
        if gaps and min(gaps) < self.horizon_secs:
            raise ValueError(
                f"overlapping forward windows: min as_of gap {min(gaps)}s < horizon_secs "
                f"{self.horizon_secs}s — space cutoffs by >= horizon (issue #436 B3 non-overlap "
                "invariant; raise --step-days to >= --horizon-days)")


def run_bakeoff(ss: SuffStats, runner, axes: BakeoffAxes, params: BakeoffParams, *,
                created_at: int = 0, seed: int = 0, max_workers: int = 1) -> dict:
    """Full staged sweep (8a -> 8b -> 8c -> leaderboard -> winner/NO-GO). #445 defect 4: 8a no longer
    PRUNES the estimator axis (its cheap forward-payoff proxy mismatches the realized + CLOB-MTM
    horizon objective the trajectories optimize, so it could drop an estimator the true objective
    would rank the winner). ALL registered estimators are carried into the pre-registered grid, so
    ``n_grid`` == the full pre-screen trial count; the 8a screen is retained as advisory diagnostics
    only. (Subject to the #445 defect-10 true-CLV preflight, which excludes `true_clv` UPSTREAM in
    `main` when CLOB coverage is absent/low — see `true_clv_preflight`.)"""
    n_grid_full = len(axes.enumerate_grid())     # A2: FULL pre-screen trial count for the DSR bar
    # D2 (#436): cap the deliberate search surface BEFORE any expensive work. n_grid_full × steps is
    # the NAIVE injected-set backtest count (a conservative upper bound — the memo below collapses the
    # churn axis + repeated followed sets, and screened-out estimators never run a trajectory), so a
    # grid that would blow past the committed ceiling is refused loudly: "keep N bounded" is an
    # enforced number, not a hope. Tunable per run via params.max_backtests / ranker_bakeoff_max_backtests.
    naive_backtests = n_grid_full * len(params.as_of_points)
    if naive_backtests > params.max_backtests:
        raise ValueError(
            f"bake-off grid too large: n_grid_full({n_grid_full}) × steps"
            f"({len(params.as_of_points)}) = {naive_backtests} naive backtests > "
            f"ranker_bakeoff_max_backtests={params.max_backtests}; narrow the criteria/churn sweep "
            "(fewer --ttr-hours/--bands/--min-trl levels) or raise the ceiling")
    # #451: one per-run AFML-uniqueness memo shared by the 8a screen AND every config's trajectory.
    # The uniqueness of an in-sample is fixed by (criteria, as_of), so this collapses up to
    # n_grid × steps recomputations to once per distinct in-sample (bit-identical, see
    # `_uniqueness_cached`) — the dominant Python cost on a large candidate universe. Thread-safe
    # single-flight so the parallel `run_trajectories` seam computes each cold pass exactly once.
    uniqueness_cache = _SingleFlightCache()
    # #445 defect 4: the 8a screen runs for its ADVISORY ranking only — it no longer prunes the
    # estimator axis (its forward-payoff proxy mismatches the realized + CLOB-MTM horizon objective).
    screen_advisory = screen_estimators(
        ss, list(axes.estimators), as_of_points=params.as_of_points,
        train_secs=params.train_secs, horizon_secs=params.horizon_secs,
        k=params.k, keep=params.screen_keep, criteria=axes.criteria[0],   # A3: canonical criteria
        uniqueness_cache=uniqueness_cache)
    run_axes = axes                              # no pruning: the run-set IS the full pre-screen grid
    manifest = pre_register_grid(run_axes, created_at=created_at)
    grid = run_axes.enumerate_grid()
    grid_by_key = {g.key: g for g in grid}        # A1: thread GridPoints, never re-parse a key
    baseline_key = _baseline_key(run_axes)
    memo_runner = MemoizingBacktestRunner(runner)   # D2: in-run dedup of repeated followed sets
    return_matrix, results = run_trajectories(
        grid, ss, memo_runner, max_workers=max_workers, as_of_points=params.as_of_points,
        train_secs=params.train_secs, horizon_secs=params.horizon_secs,
        k=params.k, displacement_margin=params.displacement_margin,
        demoter_kwargs=params.demoter_kwargs, flat_usd=params.flat_usd,
        uniqueness_cache=uniqueness_cache)
    split_at = int(np.median(params.as_of_points))
    cpr = wallet_persistence_cpr(ss, split_at=split_at)
    decision = select_winner_or_nogo(
        return_matrix, baseline_key=baseline_key, n_grid=manifest["n_grid"],
        n_grid_full=n_grid_full, cpr=cpr, seed=seed, min_periods=params.min_periods)
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
    # E3 (#436): CLOB forward-MTM coverage so the resolution-speed residual is reported beside the
    # winner. Per config: total open-at-horizon vs marked (covered) positions across its trajectory;
    # plus the WINNER's per-(config, period) detail (coverage thins at older as_of, the depth-coverage
    # interaction B4 surfaces). A position open at the horizon with no CLOB series contributes 0 to the
    # MTM and is uncovered here — coverage = marked / open is the fraction of forward edge actually valued.
    def _cov_total(res, col):
        return (int(res.coverage[col].sum())
                if (not res.coverage.empty and col in res.coverage.columns) else 0)

    def _config_overall(res):
        # F3a: + open-at-horizon fraction (open / positions_in_window) and the resolution-lag
        # distribution over the config's whole trajectory.
        open_n = _cov_total(res, "open_at_horizon")
        positions = _cov_total(res, "positions_in_window")
        resolved = [x for lags in res.lags_by_period.values() for x in lags]
        return {
            "open_at_horizon": open_n,
            "marked_at_horizon": _cov_total(res, "marked_at_horizon"),
            "positions_in_window": positions,
            "open_at_horizon_frac": (open_n / positions if positions else None),
            "resolution_lag": _lag_distribution(resolved, _cov_total(res, "censored")),
        }

    def _period_detail(idx, r, resolved):
        open_n, positions = int(r["open_at_horizon"]), int(r["positions_in_window"])
        return {
            "as_of": int(idx),
            "open_at_horizon": open_n,
            "marked_at_horizon": int(r["marked_at_horizon"]),
            "positions_in_window": positions,
            "open_at_horizon_frac": (open_n / positions if positions else None),
            "resolution_lag": _lag_distribution(resolved, int(r["censored"])),
        }

    mtm_coverage = {
        "by_config_overall": {key: _config_overall(res) for key, res in results.items()},
        "winner_by_period": (
            [_period_detail(idx, r, winner_res.lags_by_period.get(int(idx), []))
             for idx, r in winner_res.coverage.iterrows()]
            if winner_res is not None and not winner_res.coverage.empty else []),
    }
    return {"manifest": manifest, "survivors": screen_advisory, "return_matrix": return_matrix,
            "cpr": cpr, "decision": decision, "deliverable": deliverable,
            "mtm_coverage": mtm_coverage,
            # D2 (#436): the realized memo speedup — distinct injected-set backtests actually run vs
            # the naive per-(config, step) request count. Lets the operator size the real run.
            "backtest_calls": {"distinct": memo_runner.calls, "total": memo_runner.lookups,
                               "naive_ceiling": naive_backtests}}


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


def _parse_band(s: str) -> "tuple[float, float]":
    """Parse a ``lo:hi`` entry-band CLI level into a ``(price_min, price_max)`` pair (D1, #436)."""
    lo, sep, hi = s.partition(":")
    if not sep:
        raise argparse.ArgumentTypeError(f"band {s!r} must be 'lo:hi' (e.g. 0.15:0.85)")
    return (float(lo), float(hi))


def _positive_workers(s: str) -> int:
    """Parse ``--workers`` and fail FAST (at argparse, before the expensive materialize — like
    ``--bands``/``--engine``) on a nonsensical value, rather than silently falling back to serial."""
    n = int(s)
    if n < 1:
        raise argparse.ArgumentTypeError(f"--workers must be >= 1 (1 = serial), got {n}")
    return n


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
    ap.add_argument("--horizon-days", type=int, default=30,
                    help="forward window length (days); --step-days must be >= this so forward "
                         "windows do not overlap (issue #436 B3 non-overlap invariant)")
    ap.add_argument("--steps", type=int, default=24,
                    help="walk-forward cutoffs; >= ranker_min_periods (24) for a trustworthy "
                         "PBO/RW/SPA/DSR verdict (issue #436 B4)")
    ap.add_argument("--step-days", type=int, default=30)
    ap.add_argument("--start-unix", type=int, required=True)
    # D1 (#436): the criteria sweep. Each level list is ordered operator-default-first so criteria[0]
    # stays the canonical 8a-screen / baseline level (build_criteria_grid). Narrow these to bound the
    # compute ceiling (ranker_bakeoff_max_backtests). half_life is held (carried-but-inert in v1).
    ap.add_argument("--ttr-hours", type=float, nargs="+", default=list(TTR_HOURS_LEVELS),
                    help="criteria sweep: TTR horizons (hours); default '72 24 48' (72h canonical)")
    ap.add_argument("--bands", type=_parse_band, nargs="+", default=list(PRICE_BANDS),
                    help="criteria sweep: entry bands as lo:hi; default '0.15:0.85 0.30:0.70'")
    ap.add_argument("--min-trl", type=int, nargs="+", default=list(MIN_TRL_LEVELS),
                    help="criteria sweep: ranker_min_trl levels; default '0 20' (0 = no gate)")
    ap.add_argument("--churn-cost", type=float, default=RANKER_CHURN_COST_USD,
                    help="D3: per-admission copy cost USD, swept as {0, this}; "
                         "default ranker_churn_cost_usd (0.75)")
    # #451: bound the candidate universe + the policy sweep so the run is tractable on a constrained
    # box. All default to the committed full-universe / all-4-policy behaviour.
    ap.add_argument("--max-wallets", type=int, default=None,
                    help="cap the candidate universe to the top-N most-active wallets within the "
                         "position band (default: no cap -> full universe; #451)")
    ap.add_argument("--universe-pos-min", type=int, default=None,
                    help="keep only wallets with >= this many first-buy positions (drops the noise "
                         "tail; default: no floor)")
    ap.add_argument("--universe-pos-max", type=int, default=None,
                    help="keep only wallets with <= this many first-buy positions (drops hyperactive "
                         "uncopyable bots; default: no cap)")
    _all_policies = [FullRerank.name, KnockoutBackfill.name,
                     HybridDisplacement.name, OnlineWeighting.name]
    ap.add_argument("--policies", nargs="+", choices=_all_policies, default=list(_all_policies),
                    help="set-transition policy names to sweep (default: all 4; fewer = far fewer "
                         "distinct backtests for a tractable run). A bad name fails fast at argparse "
                         "rather than after the expensive materialize, like --engine.")
    ap.add_argument("--workers", type=_positive_workers, default=1,
                    help="parallel trajectory workers (thread pool). 1 = serial (default). >1 runs the "
                         "mutually-independent configs concurrently — results are BIT-IDENTICAL (the "
                         "pe-backtest subprocess dominates and releases the GIL). Recommend "
                         "~min(16, cores-2) on a dedicated box; the Python scoring is GIL-serialised so "
                         "the realised speedup is ~2x on a large universe, more on a smaller pool.")
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


def bounded_universe(con, *, pos_min: "int | None" = None, pos_max: "int | None" = None,
                     max_wallets: "int | None" = None) -> "list | None":
    """Candidate-wallet shortlist bounded by first-buy position count, or ``None`` for the full
    universe (#451). The committed full-universe ``materialize(con)`` extracts every wallet's
    first-buy positions (~411k wallets / ~71M rows in the production parquet) — a pandas frame that
    OOMs a memory-constrained box. When any bound is set, restrict the materialize input to wallets
    with ``pos_min <= distinct (market, outcome) first-buys <= pos_max`` — a copyable shortlist that
    drops hyperactive position-hoarding bots (``> pos_max``, uncopyable) and the thin noise tail
    (``< pos_min``), optionally the top-``max_wallets`` by activity. Queries the engine's ``trades``
    view; returns ``wallet_hex`` strings (the materialize universe contract)."""
    if pos_min is None and pos_max is None and max_wallets is None:
        return None
    lo = 0 if pos_min is None else int(pos_min)
    hi = (2 ** 63 - 1) if pos_max is None else int(pos_max)
    # `wallet_hex` tiebreaks the activity sort so the top-N membership is DETERMINISTIC across runs
    # (positions can tie at the LIMIT boundary; the bake-off is reproducible by contract).
    cap = f"ORDER BY positions DESC, wallet_hex LIMIT {int(max_wallets)}" if max_wallets else ""
    rows = con.execute(
        "SELECT wallet_hex FROM (SELECT wallet_hex, "
        "count(DISTINCT (market_id, outcome_id)) AS positions FROM trades GROUP BY wallet_hex) "
        f"WHERE positions BETWEEN {lo} AND {hi} {cap}"
    ).fetchall()
    return [r[0] for r in rows]


def main() -> None:  # pragma: no cover (operator entry; CI exercises the stage functions)
    """Operator entry: materialize suff_stats from the cache, run the bake-off against the real
    ``pe-backtest``, write the leaderboard / manifest / decision, cross-check ``paper_fills``."""
    import time

    from . import suff_stats as suff_stats_mod

    args = _build_arg_parser().parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = _open_engine(args)
    # #451: optionally bound the candidate universe so the full-universe materialize does not OOM a
    # constrained box; None -> the full universe (committed behaviour).
    universe = bounded_universe(con, pos_min=args.universe_pos_min,
                                pos_max=args.universe_pos_max, max_wallets=args.max_wallets)
    if universe is not None:
        print(f"candidate universe bounded to {len(universe):,} wallets "
              f"(positions in [{args.universe_pos_min}, {args.universe_pos_max}]"
              f"{f', top {args.max_wallets} by activity' if args.max_wallets else ''})")
    ss = suff_stats_mod.materialize(con, wallets=universe)

    day = 86_400
    as_of_points = [args.start_unix + i * args.step_days * day for i in range(args.steps)]
    params = BakeoffParams(as_of_points=as_of_points, train_secs=args.train_days * day,
                           horizon_secs=args.horizon_days * day)
    # #445 defect 10: true-CLV preflight BEFORE the grid is frozen. Exclude `true_clv` from the
    # estimator axis when the CLOB views are absent or position-level `true_clv_close` coverage is
    # below `true_clv_coverage_warn_pct`, so it cannot contribute a silent all-NaN arm; abort if the
    # exclusion leaves no estimator. The exclusion is visible in true_clv_preflight.json + decision.
    clv_views = (suff_stats_mod._relation_exists(con, "market_price_history")
                 and suff_stats_mod._relation_exists(con, "token_conditions"))
    estimators, clv_preflight = true_clv_preflight(
        ss, views_present=clv_views, estimators=tuple(ESTIMATOR_REGISTRY))
    axes = BakeoffAxes(
        estimators=estimators, deflators=(NO_DEFLATION, DeflatedSharpe.name),
        policies=tuple(args.policies),                    # #451: operator-selectable policy sweep
        criteria=build_criteria_grid(                     # D1 (#436): the swept criteria grid
            ttr_hours_levels=tuple(args.ttr_hours), price_bands=tuple(args.bands),
            min_trl_levels=tuple(args.min_trl)),
        churn_costs=(0.0, args.churn_cost))               # D3 (#436): {free, anchored} cost sweep
    runner = SubprocessBacktestRunner(args.pe_backtest, args.cache, str(out_dir / "bt"))
    result = run_bakeoff(ss, runner, axes, params, created_at=int(time.time()),
                         max_workers=args.workers)

    # #445 defect 10: surface the true-CLV preflight in the decision payload + a dedicated file so the
    # `true_clv` exclusion is VISIBLE in operator output, never a silent all-NaN ranking.
    result["decision"]["true_clv_preflight"] = clv_preflight
    (out_dir / "true_clv_preflight.json").write_text(json.dumps(clv_preflight, indent=2))
    if clv_preflight["true_clv_excluded"]:
        print(f"true_clv EXCLUDED from the grid: {clv_preflight['reason']} "
              f"(coverage {clv_preflight['coverage_pct']}%); see true_clv_preflight.json")

    (out_dir / "manifest.json").write_text(json.dumps(result["manifest"], indent=2))
    result["return_matrix"].to_csv(out_dir / "return_matrix.csv")
    result["decision"]["leaderboard"].to_csv(out_dir / "leaderboard.csv")
    status = {k: v for k, v in result["decision"].items() if k != "leaderboard"}
    (out_dir / "decision.json").write_text(json.dumps(status, indent=2, default=str))
    print(f"bake-off {result['decision']['status']}: {result['decision']['reason']}")
    bc = result["backtest_calls"]                          # D2 (#436): realized memo speedup
    print(f"compute: {len(axes.criteria)} criteria × {result['manifest']['n_grid']} run-set configs; "
          f"{bc['distinct']} distinct backtests of {bc['total']} requested "
          f"({bc['naive_ceiling']} naive ceiling)")

    # F1 (#436): survivorship exposure. The bake-off universe is the CURRENT wallet_cache.db snapshot
    # sliced per-as_of; #385 HARD-DELETES purged wallets (proven losers + dead-weight), so historical
    # as_of slices are missing them and a point-in-time roster is unreconstructable. The directional
    # bias is OPTIMISTIC (the losers a copy strategy would have followed-and-bled-on are absent). A
    # precise purge count is NOT cheaply available from the DuckDB positions view (the purge leaves no
    # in-extract ledger), so the current universe size is the cheap exposure proxy. See docs/31.
    surv = {"universe_wallets": int(ss["wallet"].nunique()),
            "universe_positions": int(len(ss)),
            "point_in_time_roster": False, "purge_hard_deletes_wallets": True,
            "directional_bias": "optimistic",
            "note": "Current-snapshot universe; #385-purged wallets are absent from historical as_of "
                    "slices. Hard-delete makes a point-in-time roster unreconstructable. See docs/31."}
    (out_dir / "survivorship.json").write_text(json.dumps(surv, indent=2))
    print(f"survivorship: universe {surv['universe_wallets']} wallets / {surv['universe_positions']} "
          f"positions (current snapshot; #385-purged wallets absent at historical as_of → optimistic "
          f"bias; survivorship.json)")

    # E3 (#436): forward-MTM coverage beside the winner — what fraction of the winner's
    # open-at-horizon forward edge the CLOB series actually valued (the resolution-speed residual;
    # an uncovered open position contributes 0 to the MTM). Per-(config, period) detail on disk.
    (out_dir / "mtm_coverage.json").write_text(json.dumps(result["mtm_coverage"], indent=2))
    wbp = result["mtm_coverage"]["winner_by_period"]
    w_open = sum(p["open_at_horizon"] for p in wbp)
    w_marked = sum(p["marked_at_horizon"] for p in wbp)
    cov_pct = f"{100.0 * w_marked / w_open:.1f}%" if w_open else "n/a"
    print(f"MTM coverage (winner): {w_marked}/{w_open} open-at-horizon positions marked "
          f"({cov_pct}); per-(config, period) detail in mtm_coverage.json")

    # F3a (#436): the winner's resolution-lag distribution — how slowly its still-open forward edge
    # eventually settled (advisory; quantifies the resolution-speed residual coverage alone misses).
    wco = result["mtm_coverage"]["by_config_overall"].get(result["deliverable"]["winner_key"], {})
    rl = wco.get("resolution_lag", {})
    of = wco.get("open_at_horizon_frac")
    of_str = f"{100.0 * of:.1f}%" if of is not None else "n/a"
    p50_str = f"{rl['p50_days']:.1f}d" if rl.get("p50_days") is not None else "n/a"
    p90_str = f"{rl['p90_days']:.1f}d" if rl.get("p90_days") is not None else "n/a"
    cens = rl.get("censored_frac")
    cens_str = f"{100.0 * cens:.1f}%" if cens is not None else "n/a"
    print(f"resolution lag (winner, open-at-horizon): open frac {of_str}; "
          f"median {p50_str}, p90 {p90_str}, censored {cens_str} — advisory, in mtm_coverage.json")

    # Cross-check the deliverable — the WINNING config's frozen final-step followed set (A1, #436;
    # the set the winner actually rode, applying its criteria/eligibility/deflation/policy), NOT a
    # cold re-score on the un-sliced in-sample — against the live cohort's realized P&L from Supabase
    # (issue #421 §Deliverable). Falls back to the baseline config's set on a NO-GO.
    # #445 defect 5: the LIVE FETCH is the only advisory-skippable step (missing creds / network on a
    # local/dev run). A SUCCESSFUL fetch then runs the cross-check, whose hard-fail — zero overlap of a
    # non-empty followed set after a successful fetch — PROPAGATES and fails the operator run; it is
    # NOT swallowed as advisory. Identity follows `final_follow` (crosscheck_deliverable), so a held
    # incumbent absent from the fresh scores still appears in the live-loser check.
    try:
        realized = fetch_paper_fills_realized_pnl()
    except Exception as exc:        # missing creds / network: advisory skip for local/dev runs only
        print(f"paper_fills cross-check skipped (live fetch unavailable): {exc}")
    else:
        deliverable = result["deliverable"]
        crosscheck, summary = crosscheck_deliverable(
            deliverable["follow"], deliverable["scores"], realized)
        crosscheck.to_csv(out_dir / "paper_fills_crosscheck.csv", index=False)
        print(f"paper_fills cross-check ({deliverable['winner_key']}): "
              f"{summary['live_loser_flags']} live-loser flags; overlap "
              f"{summary['overlap_count']}/{summary['n_follow']} "
              f"({100.0 * summary['overlap_fraction']:.1f}% of followed wallets)")


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    main()
