"""Subprocess boundary + realized OOS edge-LCB scorer.

Each Optuna trial:
1. Builds env with 12 PE_SKILL_COMPOSITE_W_* + PE_SKILL_CACHE_PATH +
   PE_SKILL_CUTOFF_UNIX (one trial = N subprocess calls, one per CV window).
2. Subprocess `pe-skill-select composite`, parse selected wallet_hex set
   from stdout.
3. For each window: load OOS positions via data.load_oos_positions, compute
   edge-LCB = mean(outcome − vwap_entry) − Z · pstdev/√n.
4. Return mean LCB across windows (Optuna maximises).

Sign convention matches `composite.rs`: positive weights = higher-is-better,
negative = lower-is-better. Bounds enforce semantic direction for the four
"lower-is-better" features so Optuna doesn't waste budget discovering them.
"""
from __future__ import annotations

import math
import os
import re
import statistics
import subprocess
from dataclasses import dataclass
from typing import Optional

from .cv import CvWindow
from .data import load_oos_positions

# Mirror crates/skill-select/src/composite.rs::CompositeWeights field order.
WEIGHT_NAMES: tuple[str, ...] = (
    "SHARPE_BPS",
    "EV_MEAN_BPS",
    "EV_TSTAT_BPS",
    "BB_SHRUNK_EDGE_BPS",
    "KELLY_LOG_GROWTH_BPS",
    "BRIER_SCORE_BPS",
    "BRIER_RESOLUTION_BPS",
    "CONCENTRATION_HHI_BPS",
    "CONCENTRATION_N_EFF_BPS",
    "CONCENTRATION_RPC_BPS",
    "FIRST_ENTRIES_PER_ACTIVE_DAY_BPS",
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS",
)
ENV_KEYS: tuple[str, ...] = tuple(f"PE_SKILL_COMPOSITE_W_{n}" for n in WEIGHT_NAMES)

# Sign-constrained bounds (per docs/24- §3.1 + the 2026-05-25 probe priors).
# The 4 "lower-is-better" features are clamped to ≤ 0; everything else is
# symmetric ± 5000 bps.
_NEG_ONLY = {
    "BRIER_SCORE_BPS",
    "CONCENTRATION_HHI_BPS",
    "CONCENTRATION_RPC_BPS",
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS",
}
WEIGHT_BOUNDS: dict[str, tuple[int, int]] = {
    n: (-5000, 0) if n in _NEG_ONLY else (-5000, 5000) for n in WEIGHT_NAMES
}

# Pattern matches one selected-wallet line from `pe-skill-select composite`:
#   0xabc123...\tcomposite_bps=...\tpvalue_bps=...\tsharpe_bps=...
_WALLET_LINE = re.compile(r"^(0x[0-9a-fA-F]+)\t")


@dataclass(frozen=True)
class WindowScore:
    window_label: str
    selected_wallets: frozenset[str]
    n_selected: int
    n_positions: int
    edge_lcb: float


def parse_composite_stdout(stdout: str) -> tuple[int, frozenset[str]]:
    """Parse the `pe-skill-select composite` stdout into (n_selected, hex_set).

    Format (main.rs:175-189):
      Line 0: 'composite: candidates=N selected=K (cutoff_unix=..., ...)'
      Lines 1..K: '<hex>\\tcomposite_bps=...\\tpvalue_bps=...\\tsharpe_bps=...'
    Raises ValueError on malformed first line.
    """
    lines = [l for l in stdout.splitlines() if l.strip()]
    if not lines:
        raise ValueError("composite stdout empty")
    header = lines[0]
    m = re.match(r"composite: candidates=\d+ selected=(\d+)", header)
    if not m:
        raise ValueError(f"unexpected composite header: {header!r}")
    n_selected = int(m.group(1))
    hexes = []
    for line in lines[1:]:
        wm = _WALLET_LINE.match(line)
        if wm:
            hexes.append(wm.group(1))
    return n_selected, frozenset(hexes)


def invoke_composite(
    binary: str,
    db_path: str,
    cutoff_unix: int,
    weights: dict[str, int],
    top_n: int,
    bhq_q_bps: int = 1000,
    min_trading_days: int = 20,
) -> tuple[int, frozenset[str]]:
    """Run pe-skill-select composite with the given weights. Sets all 12
    PE_SKILL_COMPOSITE_W_* env vars explicitly (overwriting any inherited
    values from the caller's env). Returns (n_selected, hex_set).
    """
    env = os.environ.copy()
    env["PE_SKILL_CACHE_PATH"] = str(db_path)
    env["PE_SKILL_CUTOFF_UNIX"] = str(cutoff_unix)
    env["PE_SKILL_FORWARD_SOURCE"] = "composite"
    env["PE_SKILL_TOP_N"] = str(top_n)
    env["PE_SKILL_BHQ_Q_BPS"] = str(bhq_q_bps)
    env["PE_SKILL_MIN_TRADING_DAYS"] = str(min_trading_days)
    env["RUST_LOG"] = "warn"
    # Explicit 12 weights — every trial must overwrite.
    for name in WEIGHT_NAMES:
        env[f"PE_SKILL_COMPOSITE_W_{name}"] = str(weights.get(name, 0))
    result = subprocess.run(
        [binary, "composite"], env=env, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"pe-skill-select composite exit={result.returncode}: "
            f"stderr={result.stderr[:500]}"
        )
    return parse_composite_stdout(result.stdout)


def edge_lcb(
    positions: list,
    z: float = 1.645,
    metric: str = "edge",
) -> Optional[float]:
    """Per-bet LCB = mean(score) − z·pstdev(score)/√n.

    `metric`:
    - "edge":  score = (outcome − vwap_entry)  ∈ [-1, +1]  (probability-space)
    - "flat":  score = (outcome − vwap_entry) / vwap_entry  (per-$1 staked
               return — matches `pe-skill-select forward-test`'s flat_pnl_usd
               per-position contribution, modulo the VWAP collapse over
               (wallet, market, outcome) groups)

    Returns None if n < 2 (no stderr possible) or any vwap_entry is 0
    (would divide by zero for flat metric).
    """
    if len(positions) < 2:
        return None
    if metric == "edge":
        scores = [p.outcome - p.vwap_entry for p in positions]
    elif metric == "flat":
        scores = []
        for p in positions:
            if p.vwap_entry == 0:
                return None
            scores.append((p.outcome - p.vwap_entry) / p.vwap_entry)
    else:
        raise ValueError(f"unknown metric {metric!r}; expected 'edge' or 'flat'")
    mu = statistics.fmean(scores)
    sigma = statistics.pstdev(scores)
    n = len(scores)
    return mu - z * sigma / math.sqrt(n)


def score_window(
    db_path: str,
    cutoff_unix: int,
    fwd_end_unix: int,
    selected_wallets: frozenset[str],
    z: float = 1.645,
    metric: str = "edge",
) -> WindowScore:
    """Load OOS positions for the cohort and compute the cohort's LCB."""
    positions = load_oos_positions(db_path, cutoff_unix, fwd_end_unix, selected_wallets)
    lcb = edge_lcb(positions, z=z, metric=metric)
    return WindowScore(
        window_label=f"{cutoff_unix}_{fwd_end_unix}",
        selected_wallets=selected_wallets,
        n_selected=len(selected_wallets),
        n_positions=len(positions),
        edge_lcb=lcb if lcb is not None else float("-inf"),
    )


def evaluate_weights(
    binary: str,
    db_path: str,
    windows: list[CvWindow],
    weights: dict[str, int],
    top_n: int = 5000,
    bhq_q_bps: int = 1000,
    min_trading_days: int = 20,
    z: float = 1.645,
    metric: str = "edge",
) -> tuple[float, list[WindowScore]]:
    """For each window: subprocess composite -> score_window. Returns
    (mean_lcb_across_windows, per_window_scores). Sentinel -inf for windows
    with sparse OOS data. Mean weighted equally across windows (each window
    is one calendar period).
    """
    per_window: list[WindowScore] = []
    for w in windows:
        try:
            n_sel, hexes = invoke_composite(
                binary,
                db_path,
                w.train_cutoff_unix,
                weights,
                top_n=top_n,
                bhq_q_bps=bhq_q_bps,
                min_trading_days=min_trading_days,
            )
        except (RuntimeError, ValueError):
            per_window.append(
                WindowScore(
                    window_label=w.label,
                    selected_wallets=frozenset(),
                    n_selected=0,
                    n_positions=0,
                    edge_lcb=float("-inf"),
                )
            )
            continue
        score = score_window(
            db_path, w.train_cutoff_unix, w.fwd_end_unix, hexes, z=z, metric=metric
        )
        per_window.append(
            WindowScore(
                window_label=w.label,
                selected_wallets=score.selected_wallets,
                n_selected=score.n_selected,
                n_positions=score.n_positions,
                edge_lcb=score.edge_lcb,
            )
        )
    finite = [s.edge_lcb for s in per_window if s.edge_lcb != float("-inf")]
    if not finite:
        return float("-inf"), per_window
    return sum(finite) / len(finite), per_window
