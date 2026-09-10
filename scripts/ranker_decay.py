#!/usr/bin/env python3
"""Shared recency-decay + weighted-statistics helpers for the 72hr buy-and-hold ranker.

Pure math, no I/O and no pandas (numpy + stdlib `math`/`datetime` only) so it imports
cleanly from BOTH ranking passes:

  * pass-1 `rank_72hr_buyandhold.py` (pandas) passes `g["entry_ts"].to_numpy()`,
  * pass-2 `latency_shift_rerank.py` (stdlib) passes a plain `list[float]`.

One source of truth for the non-obvious weighted-t-stat math (reliability-weighted
mean, unbiased weighted variance, Kish effective-N) and for the `--as-of` parser, so
the two passes weight every trade identically against the same anchor and can never
silently drift (issue #366).

Recency model: each trade gets `weight = exp(-ln2 · max(0, as_of − entry_ts) /
(half_life_days · 86400))`. A trade exactly one half-life old weighs 0.5; future
timestamps clip to weight 1.0; `half_life_days <= 0` ⇒ all-ones (flat = legacy,
no decay).

Uniform-weight short-circuit (the `half_life <= 0` flat path, and any set whose
trades share one timestamp): `weighted_stats` delegates to `np.mean` / `np.std(ddof=1)`
so ordinary vectors above the dispersion floor remain **bitwise-identical** to the
legacy unweighted statistic and avoid a candidate flip at a `floor_tstat` boundary.
Degenerate vectors at or below the shared dispersion floor instead return a NaN
t-statistic (#588); their reported mean, standard deviation and effective N are
unchanged.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

import numpy as np

# Canonical defaults (docs/_GLOSSARY.md: ranker_half_life_days, ranker_window_days).
# Both passes read their argparse defaults from these constants so they cannot drift.
DEFAULT_HALF_LIFE_DAYS = 30.0
DEFAULT_WINDOW_DAYS = 180
SECS_PER_DAY = 86_400

# Per-position net-edge / CLV dispersion floor (ranker_sd_floor, glossary). A
# mathematically-constant series can have sd ~1e-16 from mean-rounding float noise,
# so a bare `sd > 0` admits a spurious t-stat ~1e16. At or below the existing floor,
# t-statistics are undefined; preserve the reported moments for estimator consumers
# and diagnostics (#436 A10 follow-up, shared by both ranking passes since #588).
_SD_FLOOR = 1e-9


def decay_weights(entry_ts, as_of: int, half_life_days: float) -> np.ndarray:
    """Exponential recency weight per trade.

    `entry_ts` is array-like (numpy array or `list[float]`). Returns all-ones when
    `half_life_days <= 0` (flat = no decay). Otherwise
    `exp(-ln2 · max(0, as_of − entry_ts) / (half_life_days · SECS_PER_DAY))`: a trade
    exactly one half-life old weighs 0.5, older trades weigh less, and a future trade
    (`entry_ts > as_of`) clips to age 0 ⇒ weight 1.0.
    """
    e = np.asarray(entry_ts, dtype=float)
    if half_life_days is None or half_life_days <= 0:
        return np.ones(e.shape, dtype=float)
    age = np.maximum(0.0, float(as_of) - e)
    half_life_secs = half_life_days * SECS_PER_DAY
    return np.exp(-math.log(2.0) * age / half_life_secs)


def weighted_stats(values, weights) -> tuple[float, float, float, float]:
    """Reliability-weighted (wmean, wstd, n_eff, tstat).

    * wmean = Σwᵢvᵢ / Σwᵢ.
    * Unbiased weighted variance `wvar = (W / (W² − V2)) · Σ wᵢ(vᵢ − wmean)²`
      with `W = Σw`, `V2 = Σw²`; reduces exactly to `ddof=1` when weights are equal.
    * Kish effective sample size `n_eff = W² / V2`.
    * tstat = wmean / wstd · √n_eff.

    Any of `n ≤ 1`, `W ≤ 0`, `W² − V2 ≤ 0`, `n_eff ≤ 1` ⇒ `tstat` is NaN
    (wmean still returned when defined). Dispersion `wstd ≤ _SD_FLOOR` also makes
    `tstat` NaN, while preserving mean, standard deviation and effective N. The
    legacy zero-variance reporting is unchanged: the uniform path returns
    `wstd = 0.0`; the general weighted path returns NaN for nonpositive variance.

    Uniform-weight short-circuit: when every weight is equal (the flat `half_life ≤ 0`
    path, or all trades sharing a timestamp) delegate to `np.mean` / `np.std(ddof=1)`
    so ordinary vectors above the dispersion floor remain bitwise-identical to the
    legacy unweighted statistic. The `W ≤ 0` guard precedes this short-circuit
    (#445), so an all-zero weight vector — which is also "uniform" — returns NaN
    rather than a spurious unweighted statistic.
    """
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    n = int(v.size)
    if n == 0:
        return (float("nan"), float("nan"), 0.0, float("nan"))
    # F3 guard (#436 Phase F): reliability weights are non-negative by contract (uniqueness / recency
    # weights). A negative weight is a caller bug that would corrupt wmean/wvar — fail safe to NaN
    # (the wallet is dropped) rather than emit a silently wrong statistic.
    if bool(np.any(w < 0.0)):
        return (float("nan"), float("nan"), 0.0, float("nan"))

    # #445: the sum-of-weights (W <= 0) guard must PRECEDE the uniform-weight short-circuit. An
    # all-zero weight vector is "uniform" (every weight equal), so without checking W first it would
    # wrongly delegate to np.mean/np.std(ddof=1) and return a finite UNWEIGHTED statistic —
    # masquerading as a valid sample when there are effectively no observations. W <= 0 => NaN.
    big_w = float(w.sum())
    if big_w <= 0.0:
        return (float("nan"), float("nan"), 0.0, float("nan"))

    # Uniform-weight moments retain bitwise parity; the dispersion floor gates only tstat.
    if bool(np.all(w == w[0])):
        wmean = float(v.mean())
        if n <= 1:
            return (wmean, float("nan"), float(n), float("nan"))
        wstd = float(v.std(ddof=1))
        n_eff = float(n)
        tstat = (wmean / wstd * math.sqrt(n_eff)) if wstd > _SD_FLOOR else float("nan")
        return (wmean, wstd, n_eff, tstat)

    # General reliability-weighted path.
    v2 = float((w * w).sum())
    wmean = float((w * v).sum() / big_w)
    n_eff = (big_w * big_w / v2) if v2 > 0.0 else float("nan")
    denom = big_w * big_w - v2
    if n <= 1 or denom <= 0.0 or not (n_eff > 1.0):
        return (wmean, float("nan"), n_eff if math.isfinite(n_eff) else 0.0, float("nan"))
    wvar = (big_w / denom) * float((w * (v - wmean) ** 2).sum())
    if not (wvar > 0.0):
        return (wmean, float("nan"), n_eff, float("nan"))
    wstd = math.sqrt(wvar)
    tstat = (wmean / wstd * math.sqrt(n_eff)) if wstd > _SD_FLOOR else float("nan")
    return (wmean, wstd, n_eff, tstat)


def parse_as_of(s: str | None) -> int | None:
    """Parse `--as-of` (ISO date/datetime OR bare unix epoch) to unix seconds.

    `None`/empty ⇒ `None` (caller supplies its own fallback anchor). A bare integer
    string is taken as epoch seconds; otherwise `datetime.fromisoformat` parses an
    ISO date or datetime, treating naive input as UTC. Stdlib-only so pass-2 (which
    imports neither pandas nor datetime of its own) resolves `--as-of` identically
    to pass-1.
    """
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        pass
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def today_midnight_unix() -> int:
    """Unix seconds of today's UTC midnight (matches `date -u +%Y-%m-%d` resolved to unix)."""
    now = datetime.now(timezone.utc)
    midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return int(midnight.timestamp())


def window_start_unix(win_end: int, window_days: int) -> int:
    """Window start = `window_days` before `win_end` (both unix seconds)."""
    return int(win_end) - int(window_days) * SECS_PER_DAY
