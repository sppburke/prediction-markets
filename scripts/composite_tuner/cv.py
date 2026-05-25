"""Walk-forward CV window generator. Pure arithmetic — no I/O.

Each `CvWindow` pairs a `train_cutoff_unix` (must already exist in
`wallet_features`) with a forward window `[train_cutoff_unix, fwd_end_unix)`
of `fwd_days` length. Optuna scores each weight set as the mean realized
edge-LCB across all windows.
"""
from __future__ import annotations

import dataclasses
from typing import Optional

from . import data

DAY_SECS = 86_400
DEFAULT_FWD_DAYS = 30


@dataclasses.dataclass(frozen=True)
class CvWindow:
    train_cutoff_unix: int
    fwd_end_unix: int       # exclusive
    label: str              # e.g. "2026-03-31 -> 2026-04-30"


def windows_from_db(
    db_path: str,
    n_cutoffs: Optional[int] = None,
    fwd_days: int = DEFAULT_FWD_DAYS,
) -> list[CvWindow]:
    """Build one window per distinct cutoff_unix present in wallet_features.
    `n_cutoffs=None` uses all; otherwise takes the most-recent N cutoffs.
    """
    cutoffs = data.distinct_cutoffs(db_path)
    if n_cutoffs is not None and n_cutoffs > 0:
        cutoffs = cutoffs[-n_cutoffs:]
    out: list[CvWindow] = []
    for c in cutoffs:
        fwd_end = c + fwd_days * DAY_SECS
        out.append(
            CvWindow(
                train_cutoff_unix=c,
                fwd_end_unix=fwd_end,
                label=_label(c, fwd_end),
            )
        )
    return out


def _label(cutoff_unix: int, fwd_end_unix: int) -> str:
    from datetime import datetime, timezone
    a = datetime.fromtimestamp(cutoff_unix, tz=timezone.utc).strftime("%Y-%m-%d")
    b = datetime.fromtimestamp(fwd_end_unix, tz=timezone.utc).strftime("%Y-%m-%d")
    return f"{a} -> {b}"
