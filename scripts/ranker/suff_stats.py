"""The shared ``suff_stats`` substrate — one row per QUALIFYING first-buy position (issue #421).

Every ranker module reads this frame. It is materialized ONCE by REUSING the production
first-buy join ``ranker_duck.duck_extract_positions`` (scripts/ranker_duck.py:154 — first-buy
INNER market_resolutions LEFT market_schedules) so the position universe cannot drift from the
live ranker, then a pure pandas tail derives the remaining typed columns.

Schema (issue #421 "Module contracts" -> suff_stats):
  wallet:str(lc-hex)  market:str  outcome_id:int64  entry_ts:int64  ttr_ref:int64
  resolved_at:int64   price:float64(0,1)  payoff:float64{0,1}  dollar_size:float64>=0
  _eff:float64        c_t:float64>=1       close_proxy:float64|NaN

``close_proxy`` is the last-observed pre-resolution trade price on the bought outcome (issue
#421 menu ``proxy_clv``: ``CLV = close - entry``). It is derived once by ``materialize`` from a
DuckDB join (``trades`` last price before ``resolved_at``), so the ``proxy_clv`` estimator —
which only receives the ``ss`` frame, never a DB connection — can read it as a column. It is
NaN when the bought outcome has no pre-resolution trade; ``proxy_clv`` drops those positions.

The materialized frame is band/window-AGNOSTIC (permissive bands): each ``Criteria`` grid
point slices ttr / price-band / window downstream in pandas, which is what makes "materialize
once" compatible with the criteria grid (issue #421 "Architecture").
"""
import numpy as np
import pandas as pd

import ranker_duck

from . import SuffStats

# Mirrors rank_72hr_buyandhold.py: --slip-cents default 1.0 -> 0.01 absolute (see :140-141, :178).
DEFAULT_SLIP = 0.01
# Effective-entry price cap, matches rank_72hr_buyandhold.py:308 (`min(price + slip, 0.999)`).
_EFF_PRICE_CAP = 0.999
# Permissive bands for the materialize-once superset (issue #421 "suff_stats reuse contract"):
# full window, ttr >= 1s, full price range, scheduled-close TTR clock.
_PERMISSIVE = {
    "win_start": 0,
    "win_end": 2**63 - 1,
    "ttr_lo": 1,
    "ttr_secs": 2**63 - 1,
    "scheduled_only": True,
    "price_min": 0.0,
    "price_max": 1.0,
}

SUFF_STATS_COLUMNS = [
    "wallet", "market", "outcome_id", "entry_ts", "ttr_ref", "resolved_at",
    "price", "payoff", "dollar_size", "_eff", "c_t", "close_proxy",
]

# Per (market, outcome) last trade price strictly before the market's resolution — the
# ``proxy_clv`` close proxy (issue #421 line 38/106: "our last-observed pre-resolution trade
# price"). ``arg_max(price, ts)`` returns the price at the latest qualifying trade; the price is
# a VARCHAR in the cache, hence ``TRY_CAST`` (and the NULL guard so a malformed price never wins).
_CLOSE_PROXY_SQL = """
SELECT t.market_id AS market_id, t.outcome_id AS outcome_id,
       arg_max(TRY_CAST(t.price_str AS DOUBLE), t.timestamp_unix) AS close_proxy
FROM trades t
JOIN market_resolutions r ON r.market_id = t.market_id
WHERE t.timestamp_unix < r.resolved_at_unix
  AND TRY_CAST(t.price_str AS DOUBLE) IS NOT NULL
GROUP BY t.market_id, t.outcome_id
"""


def all_wallets(con) -> list[str]:
    """Every distinct (lowercase-hex) wallet in the ``trades`` view — the full universe."""
    return con.execute("SELECT DISTINCT wallet_hex FROM trades").df()["wallet_hex"].tolist()


def close_proxy_prices(con) -> pd.DataFrame:
    """Per (market, outcome) last pre-resolution trade price — the ``proxy_clv`` close proxy.

    Returns a frame ``[market_id, outcome_id, close_proxy]`` (one row per traded outcome that
    resolved). Joined onto the position rows by ``materialize``; positions whose outcome never
    traded before resolution get a NaN ``close_proxy``.

    # Precondition: ``con`` exposes the ``trades`` / ``market_resolutions`` views.
    """
    return con.execute(_CLOSE_PROXY_SQL).df()


def materialize(con, wallets: "list[str] | None" = None, *,
                slip: float = DEFAULT_SLIP, with_concurrency: bool = True) -> SuffStats:
    """Materialize the QUALIFYING first-buy superset for ``wallets`` (default: full universe).

    Reuses ``ranker_duck.duck_extract_positions`` with PERMISSIVE bands (no drift), then
    ``derive_columns``. ``with_concurrency=False`` skips the per-wallet ``c_t`` (a convenience
    column with no current consumer; issue #421 "Module contracts") for the large run.

    # Precondition: ``con`` exposes the ``trades`` / ``market_resolutions`` /
    # ``market_schedules`` views (e.g. via ``ranker_duck.get_engine``).
    """
    if wallets is None:
        wallets = all_wallets(con)
    raw = ranker_duck.duck_extract_positions(con, wallets, **_PERMISSIVE)
    # Left-join the per-(market, outcome) close proxy so the band/window-agnostic superset
    # carries it for the proxy_clv estimator (NaN where the outcome never traded pre-resolution).
    raw = raw.merge(close_proxy_prices(con), on=["market_id", "outcome_id"], how="left")
    return derive_columns(raw, slip=slip, with_concurrency=with_concurrency)


def derive_columns(raw: pd.DataFrame, *,
                   slip: float = DEFAULT_SLIP, with_concurrency: bool = True) -> SuffStats:
    """Pure tail: the raw ``duck_extract_positions`` columns -> the 12-col suff_stats schema.

    Pure (no DuckDB / no I/O) so it is unit-testable on a hand-built frame. ``close_proxy`` is
    read from ``raw`` when present (``materialize`` merges it in) else defaulted to NaN. Returns a
    default RangeIndex so a positional ``weights`` ndarray aligns with ``g.index`` inside
    estimators.
    """
    entry_ts = raw["entry_ts"].to_numpy(np.int64)
    ttr_secs = raw["ttr_secs"].to_numpy(np.int64)
    price = raw["price"].to_numpy(np.float64)
    contracts = raw["contracts"].to_numpy(np.float64)
    close_proxy = (raw["close_proxy"].to_numpy(np.float64)
                   if "close_proxy" in raw.columns else np.full(len(raw), np.nan))

    ss = pd.DataFrame({
        "wallet": raw["wallet"].astype(str).to_numpy(),
        "market": raw["market_id"].astype(str).to_numpy(),
        "outcome_id": raw["outcome_id"].to_numpy(np.int64),
        "entry_ts": entry_ts,
        # ttr_ref = absolute per-MARKET scheduled close; the SQL emits the DURATION ttr_secs,
        # not the absolute close (issue #421 "suff_stats reuse contract").
        "ttr_ref": entry_ts + ttr_secs,
        "resolved_at": raw["resolved_at"].to_numpy(np.int64),
        "price": price,
        "payoff": raw["payoff"].to_numpy(np.float64),
        # dollar_size = price x contracts (no notional column; contracts = whole shares).
        "dollar_size": price * contracts,
        # _eff slip-adjusted effective entry, matches rank_72hr_buyandhold.py:308.
        "_eff": np.minimum(price + slip, _EFF_PRICE_CAP),
        # close_proxy = last pre-resolution trade price on the bought outcome (proxy_clv).
        "close_proxy": close_proxy,
    })
    ss["c_t"] = concurrency(ss) if with_concurrency else np.nan
    return ss[SUFF_STATS_COLUMNS]


def concurrency(ss: SuffStats) -> np.ndarray:
    """Entry-instant concurrency ``c_t``: # of THIS wallet's labels live at each row's ``entry_ts``.

    A label j is live over ``[entry_ts_j, ttr_ref_j]``; ``c_t_i = #{j in same wallet :
    entry_ts_j <= entry_ts_i <= ttr_ref_j}`` (includes i itself, so ``c_t >= 1``). Computed per
    wallet via two sorted-array binary searches (O(n log n) time, O(n) memory) — whale-safe.

    Convenience column only: ``uniqueness_weights`` recomputes concurrency itself over label
    spans, so nothing currently consumes ``c_t`` (issue #421 "Module contracts" note).
    """
    c = np.ones(len(ss), dtype=np.float64)
    for _, g in ss.groupby("wallet", sort=False):
        idx = g.index.to_numpy()
        starts = np.sort(g["entry_ts"].to_numpy(np.int64))
        ends = np.sort(g["ttr_ref"].to_numpy(np.int64))
        t = g["entry_ts"].to_numpy(np.int64)
        # live at instant t = (# labels started by t) - (# labels closed before t).
        started = np.searchsorted(starts, t, side="right")
        closed = np.searchsorted(ends, t, side="left")
        c[idx] = (started - closed).astype(np.float64)
    return c
