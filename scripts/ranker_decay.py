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
so the flat path is **bitwise-identical** to the legacy unweighted statistic, not
merely equal-after-rounding — this keeps a `half_life=0` run byte-for-byte equal to
the pre-#366 code (pass-1 writes its CSV at full precision) and avoids a candidate
flip at a `floor_tstat` boundary.
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

    NaN guards mirror the legacy `tstat`: any of `n ≤ 1`, `W ≤ 0`, `W² − V2 ≤ 0`,
    `wstd ≤ 0`, `n_eff ≤ 1` ⇒ `tstat` is NaN (wmean still returned when defined). `wstd`
    is NaN in those cases too, EXCEPT the flat zero-variance path, which returns
    `wstd = 0.0` for bitwise parity with the legacy `np.std(ddof=1)`.

    Uniform-weight short-circuit: when every weight is equal (the flat `half_life ≤ 0`
    path, or all trades sharing a timestamp) delegate to `np.mean` / `np.std(ddof=1)`
    so the result is bitwise-identical to the legacy unweighted statistic.
    """
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    n = int(v.size)
    if n == 0:
        return (float("nan"), float("nan"), 0.0, float("nan"))

    # Uniform-weight short-circuit -> bitwise-identical to legacy np.mean/np.std(ddof=1).
    if bool(np.all(w == w[0])):
        wmean = float(v.mean())
        if n <= 1:
            return (wmean, float("nan"), float(n), float("nan"))
        wstd = float(v.std(ddof=1))
        n_eff = float(n)
        tstat = (wmean / wstd * math.sqrt(n_eff)) if wstd > 0 else float("nan")
        return (wmean, wstd, n_eff, tstat)

    # General reliability-weighted path.
    big_w = float(w.sum())
    v2 = float((w * w).sum())
    if big_w <= 0.0:
        return (float("nan"), float("nan"), 0.0, float("nan"))
    wmean = float((w * v).sum() / big_w)
    n_eff = (big_w * big_w / v2) if v2 > 0.0 else float("nan")
    denom = big_w * big_w - v2
    if n <= 1 or denom <= 0.0 or not (n_eff > 1.0):
        return (wmean, float("nan"), n_eff if math.isfinite(n_eff) else 0.0, float("nan"))
    wvar = (big_w / denom) * float((w * (v - wmean) ** 2).sum())
    if not (wvar > 0.0):
        return (wmean, float("nan"), n_eff, float("nan"))
    wstd = math.sqrt(wvar)
    tstat = wmean / wstd * math.sqrt(n_eff)
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
