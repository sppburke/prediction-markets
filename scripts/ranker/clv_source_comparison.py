#!/usr/bin/env python3
"""CLV source comparison — the issue #429 PR2 "investigate fuller sources first" gate.

Over a liquidity x recency-stratified sample of resolved-with-winner markets, measure whether
the true-CLV pre-resolution price series should come from CLOB ``/prices-history``, a
trades-derived hourly-bucket series, both, or neither — and whether ``true_clv`` adds wallet-
ranking signal over the existing ``proxy_clv``. Emits a structured JSON/CSV report and a
markdown policy memo that **gates PR3** (PR3 builds only the price-series source(s) this memo
selects; "CLOB-only, skip the trades pass" is an explicit, expected outcome).

Reads the **live SQLite cache directly** rather than the ``ranker_duck`` Parquet snapshot
because PR2 needs ``token_conditions`` (token_id <-> outcome_index, #429 PR1) and
``market_liquidity`` — neither is in the Parquet export — plus current resolutions; the
analysis is sample-scoped, so ``idx_trades_market_id`` keeps the per-market trade reads fast.
The CLOB series is live-fetched because ``market_price_history`` is not populated yet (PR3).

The five measurements (issue #429 PR2):
  (a) CLOB coverage     — markets with a usable >=3-point CLOB series in [close_ref-72h, close_ref].
  (b) trades coverage   — markets with a >=3 hourly-bucket trades series in the same window.
  (c) source agreement  — on the intersection, CLOB-vs-trades-last correlation + median abs diff
                          (flag if median abs diff > ~0.05: trade-price-vs-mid bias).
  (d) unrecoverable     — markets with an empty CLOB series AND < 3 trades (proxy_clv-only floor).
  (e) incremental signal — Spearman of per-wallet mean-CLV ranking under true_clv (CLOB close)
                          vs proxy_clv (last pre-resolution trade); low rank-corr => true_clv adds
                          signal worth building, high => proxy_clv already suffices.

``proxy_clv`` reference price matches ``scripts/ranker/suff_stats.py`` (``close_proxy`` =
last trade price strictly before ``resolved_at``). ``true_clv`` reference matches the issue #429
PR4 plan (last CLOB point at ``t <= close_ref``, ``close_ref = end_date_unix ?? resolved_at_unix``).

Run:
    .venv-analysis/bin/python -m scripts.ranker.clv_source_comparison \\
        --cache data/wallet_cache.db --out-dir runs/clv-compare [--sample-size 10000] [--seed 42]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# ── Canonical constants (docs/_GLOSSARY.md "Bootstrap defaults") ──────────────────────────────
CLV_WINDOW_SECS = 259_200  # prices_history_window_secs (72h pre-close window)
FIDELITY_MIN = 60  # clob_prices_history_fidelity_minutes (hourly buckets)
BUCKET_SECS = FIDELITY_MIN * 60
MIN_SERIES_POINTS = 3  # PR2(a): a "usable" series needs >=3 points
MIN_TRADES_FLOOR = 3  # PR2(d): "<3 trades" is the unrecoverable floor
MID_BIAS_FLAG = (
    0.05  # clv_compare_mid_bias_max: median abs diff above this = trade-vs-mid bias
)
PROXY_SUFFICES_SPEARMAN = (
    0.95  # clv_compare_proxy_suffices_spearman: true~=proxy => skip building
)
TRADES_DOMINATES_PP = (
    20.0  # clv_compare_trades_dominates_pp: trades>>CLOB coverage => trades_only
)
TRADES_FILL_MIN_PP = (
    5.0  # clv_compare_trades_fill_min_pp: min trades coverage uplift to add a fill
)
MIN_WALLET_POSITIONS = 2  # mirror ProxyCLV: a wallet needs >=2 CLV positions to rank
DEFAULT_CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_SAMPLE_SIZE = 10_000
DEFAULT_CLOB_WORKERS = 8
DEFAULT_CLOB_MIN_INTERVAL_MS = (
    15  # ~<=66 req/s aggregate, well under the 1000 req/10s limit
)
DEFAULT_HTTP_TIMEOUT_S = 20.0
LIQUIDITY_STRATA = 4  # liquidity quartiles
RECENCY_STRATA = 6  # recency sextiles (by close_ref)


# ── Pure functions (no I/O — covered by the drift-guard test) ─────────────────────────────────
def hourly_bucket_series(
    ts: np.ndarray, price: np.ndarray, start: int, end: int
) -> dict[int, float]:
    """Last trade price per hourly bucket within ``[start, end]`` (inclusive).

    Bucket index = ``(t - start) // BUCKET_SECS``. "Last in bucket" is the price at the max ``t``
    in that bucket — the trades analogue of a CLOB hourly point. Trades outside the window are
    ignored. Returns ``{bucket_index: price}``.
    """
    out: dict[int, float] = {}
    last_ts: dict[int, int] = {}
    for t_i, p_i in zip(ts.tolist(), price.tolist(), strict=True):
        if t_i < start or t_i > end:
            continue
        b = int((t_i - start) // BUCKET_SECS)
        if b not in last_ts or t_i >= last_ts[b]:
            last_ts[b] = int(t_i)
            out[b] = float(p_i)
    return out


def clob_bucket_series(
    points: list[tuple[int, float]], start: int, end: int
) -> dict[int, float]:
    """Bucket a CLOB ``[(t, p)]`` series the same way as the trades series, for aligned diffing."""
    if not points:
        return {}
    ts = np.array([t for t, _ in points], dtype=np.int64)
    price = np.array([p for _, p in points], dtype=np.float64)
    return hourly_bucket_series(ts, price, start, end)


def usable(n_points: int) -> bool:
    """A series is usable for CLV iff it has at least ``MIN_SERIES_POINTS`` points."""
    return n_points >= MIN_SERIES_POINTS


def aligned_pairs(
    clob_buckets: dict[int, float], trade_buckets: dict[int, float]
) -> list[tuple[float, float]]:
    """(CLOB price, trades-last price) on buckets present in BOTH series — for measurement (c)."""
    shared = clob_buckets.keys() & trade_buckets.keys()
    return [(clob_buckets[b], trade_buckets[b]) for b in sorted(shared)]


def last_at_or_before(points: list[tuple[int, float]], cutoff: int) -> float | None:
    """Price of the last series point with ``t <= cutoff`` (the true-CLV close reference)."""
    best_t: int | None = None
    best_p: float | None = None
    for t_i, p_i in points:
        if t_i <= cutoff and (best_t is None or t_i >= best_t):
            best_t, best_p = t_i, p_i
    return best_p


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation of two equal-length vectors (NaN if < 2 points or no variance)."""
    if a.size < 2 or b.size < 2:
        return float("nan")
    ra = pd.Series(a).rank().to_numpy()
    rb = pd.Series(b).rank().to_numpy()
    if np.std(ra) == 0 or np.std(rb) == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def pearson(diffs_a: np.ndarray, diffs_b: np.ndarray) -> float:
    """Pearson correlation of two equal-length vectors (NaN if < 2 points or no variance)."""
    if diffs_a.size < 2 or diffs_b.size < 2:
        return float("nan")
    if np.std(diffs_a) == 0 or np.std(diffs_b) == 0:
        return float("nan")
    return float(np.corrcoef(diffs_a, diffs_b)[0, 1])


def topk_overlap(rank_a: dict[str, float], rank_b: dict[str, float], k: int) -> float:
    """Fraction of the top-k under ranking A (higher=better) that is also top-k under B."""
    if k <= 0:
        return float("nan")
    top_a = {
        w for w, _ in sorted(rank_a.items(), key=lambda kv: kv[1], reverse=True)[:k]
    }
    top_b = {
        w for w, _ in sorted(rank_b.items(), key=lambda kv: kv[1], reverse=True)[:k]
    }
    if not top_a:
        return float("nan")
    return len(top_a & top_b) / len(top_a)


@dataclass
class PolicyDecision:
    """The PR3-gating verdict + the rationale derived from the measured metrics."""

    policy: str  # clob_only | clob_primary_with_trades_fill | trades_only | proxy_clv_suffices
    rationale: str


def decide_policy(
    *,
    clob_coverage_pct: float,
    trades_coverage_pct: float,
    clob_plus_trades_coverage_pct: float,
    median_abs_diff: float,
    true_vs_proxy_spearman: float,
) -> PolicyDecision:
    """Map the measured metrics to a PR3 price-series policy (deterministic, documented).

    Order of questions:
      1. Does true_clv even differ from proxy_clv? If the wallet ranking is near-identical
         (Spearman >= ``PROXY_SUFFICES_SPEARMAN``), the CLOB series buys ~nothing over the
         proxy_clv we already have -> ``proxy_clv_suffices`` (PR3's price series is low priority).
      2. **Bias guard (precondition for using trades AT ALL):** trades-last is a usable SOURCE
         only if it tracks the CLOB series — ``median_abs_diff <= MID_BIAS_FLAG``. A NaN diff (no
         intersection to verify against) counts as unverified, i.e. not sound. This gates BOTH
         trades branches below, so a biased/unverified trades series is never recommended even if
         it has more coverage.
      3. If trades is sound AND dominates CLOB by a wide margin (>= ``TRADES_DOMINATES_PP``) ->
         ``trades_only``.
      4. If trades is sound AND adds usable coverage CLOB lacks (>= ``TRADES_FILL_MIN_PP``) ->
         ``clob_primary_with_trades_fill``.
      5. Otherwise -> ``clob_only`` (build the CLOB series, skip the trades pass — the issue's
         expected outcome; also the verdict when trades is biased/unverified).
    """
    near_identical = (
        not np.isnan(true_vs_proxy_spearman)
    ) and true_vs_proxy_spearman >= PROXY_SUFFICES_SPEARMAN
    if near_identical:
        return PolicyDecision(
            "proxy_clv_suffices",
            f"true_clv vs proxy_clv wallet-rank Spearman={true_vs_proxy_spearman:.3f} >= "
            f"{PROXY_SUFFICES_SPEARMAN}: the CLOB series barely changes the ranking proxy_clv "
            "already produces, so building the price-series pipeline is low-value — keep proxy_clv "
            "as the CLV signal.",
        )
    trades_uplift = clob_plus_trades_coverage_pct - clob_coverage_pct
    trades_dominates = trades_coverage_pct - clob_coverage_pct >= TRADES_DOMINATES_PP
    # Bias guard FIRST: trades is a usable source only if it tracks CLOB (and there was an
    # intersection to check). NaN diff -> unverified -> not sound. Gates both trades branches.
    trades_sound = (not np.isnan(median_abs_diff)) and median_abs_diff <= MID_BIAS_FLAG
    if trades_sound and trades_dominates:
        return PolicyDecision(
            "trades_only",
            f"trades-bucket coverage ({trades_coverage_pct:.1f}%) exceeds CLOB "
            f"({clob_coverage_pct:.1f}%) by >={TRADES_DOMINATES_PP}pp and tracks the CLOB series "
            f"(median abs diff={median_abs_diff:.4f} <= {MID_BIAS_FLAG}); build the trades series.",
        )
    if trades_sound and trades_uplift >= TRADES_FILL_MIN_PP:
        return PolicyDecision(
            "clob_primary_with_trades_fill",
            f"CLOB covers {clob_coverage_pct:.1f}%; the trades fill adds {trades_uplift:.1f}pp "
            f"(to {clob_plus_trades_coverage_pct:.1f}%) and is a sound proxy (median abs "
            f"diff={median_abs_diff:.4f} <= {MID_BIAS_FLAG}). Build CLOB-primary + trades fill.",
        )
    reason = f"CLOB covers {clob_coverage_pct:.1f}%; the trades fill adds only {trades_uplift:.1f}pp"
    if not trades_sound:
        diff_s = (
            "no overlap"
            if np.isnan(median_abs_diff)
            else f"median abs diff={median_abs_diff:.4f}"
        )
        reason += f" and trades-last is not a verified CLV proxy ({diff_s}; needs <= {MID_BIAS_FLAG})"
    reason += ". Build the CLOB series only; skip PR3's trades pass."
    return PolicyDecision("clob_only", reason)


# ── CLOB live fetch (rate-limited, concurrent) ────────────────────────────────────────────────
class RateLimiter:
    """Global minimum-interval gate shared across fetch threads (mirrors the Rust dedicated gate)."""

    def __init__(self, min_interval_s: float) -> None:
        self._min = min_interval_s
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        # Reserve this request's slot under the lock (fast), then sleep OUTSIDE it so concurrent
        # workers don't serialize on the sleep — starts stay spaced by `min_interval`, HTTP
        # requests overlap.
        with self._lock:
            start = max(time.monotonic(), self._next)
            self._next = start + self._min
        delay = start - time.monotonic()
        if delay > 0:
            time.sleep(delay)


def fetch_clob_series(
    base: str,
    token_id: str,
    start: int,
    end: int,
    fidelity: int,
    limiter: RateLimiter,
    timeout: float,
) -> list[tuple[int, float]]:
    """Fetch one token's CLOB ``/prices-history`` window as ``[(t, p)]``.

    Mirrors ``crates/source-polymarket-public/src/clob_prices_history.rs``: an empty list means
    "no series in the window" OR a 4xx for an unknown/closed token — both the empty case, never an
    error (so one bad token cannot abort the analysis). Other network errors also degrade to empty
    with a stderr note (a best-effort measurement, not a pipeline).
    """
    url = (
        f"{base}/prices-history?market={token_id}"
        f"&startTs={start}&endTs={end}&fidelity={fidelity}"
    )
    limiter.wait()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "pe-clv-compare/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError:
        return []  # 4xx/5xx — unknown/closed token or no series; the empty case
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        print(
            f"  clob fetch {token_id[:12]}.. degraded to empty: {exc}", file=sys.stderr
        )
        return []
    out: list[tuple[int, float]] = []
    for pt in payload.get("history", []):
        try:
            out.append((int(pt["t"]), float(pt["p"])))
        except (KeyError, TypeError, ValueError):
            continue
    return out


# ── SQLite reads (sample-scoped; use indexes) ─────────────────────────────────────────────────
def open_cache(path: str) -> sqlite3.Connection:
    """Open the wallet cache read-only (URI mode), so a concurrent writer cannot be blocked."""
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def stratified_sample(conn: sqlite3.Connection, size: int, seed: int) -> pd.DataFrame:
    """Liquidity x recency-stratified sample of resolved-with-winner markets.

    Returns ``[market_id, resolved_at, close_ref, liquidity]``. ``close_ref`` =
    ``COALESCE(market_schedules.end_date_unix, market_resolutions.resolved_at_unix)`` (the issue
    #429 PR4 close reference). Stratifies into ``LIQUIDITY_STRATA x RECENCY_STRATA`` cells and
    draws proportionally so thin/old markets are not under-represented.
    """
    rows = conn.execute(
        """
        SELECT mr.market_id,
               mr.resolved_at_unix AS resolved_at,
               COALESCE(ms.end_date_unix, mr.resolved_at_unix) AS close_ref,
               ml.liquidity_usd_str AS liquidity_str
        FROM market_resolutions mr
        LEFT JOIN market_schedules ms ON ms.market_id = mr.market_id
        LEFT JOIN market_liquidity ml ON ml.market_id = mr.market_id
        WHERE mr.winning_outcome_id IS NOT NULL
          AND COALESCE(ms.end_date_unix, mr.resolved_at_unix) > 0
        """
    ).fetchall()
    df = pd.DataFrame(
        rows, columns=["market_id", "resolved_at", "close_ref", "liquidity_str"]
    )
    if df.empty:
        return df.drop(columns=["liquidity_str"]).assign(
            liquidity=pd.Series(dtype=float)
        )
    df["liquidity"] = pd.to_numeric(df["liquidity_str"], errors="coerce").fillna(0.0)
    df = df.drop(columns=["liquidity_str"])
    if len(df) <= size:
        return df.reset_index(drop=True)

    liq_bin = pd.qcut(
        df["liquidity"].rank(method="first"), LIQUIDITY_STRATA, labels=False
    )
    rec_bin = pd.qcut(
        df["close_ref"].rank(method="first"), RECENCY_STRATA, labels=False
    )
    df = df.assign(_cell=liq_bin.astype(str) + "_" + rec_bin.astype(str))
    frac = size / len(df)
    rng = np.random.default_rng(seed)
    parts = [
        g.sample(
            n=max(1, round(len(g) * frac)), random_state=int(rng.integers(0, 2**31))
        )
        for _, g in df.groupby("_cell", sort=False)
    ]
    out = pd.concat(parts).drop(columns=["_cell"])
    if len(out) > size:
        out = out.sample(n=size, random_state=seed)
    return out.reset_index(drop=True)


def tokens_for_markets(
    conn: sqlite3.Connection, market_ids: list[str]
) -> dict[str, list[tuple[int, str]]]:
    """``{market_id: [(outcome_index, token_id), ...]}`` from ``token_conditions`` (#429 PR1).

    Skips rows with a NULL ``outcome_index`` (legacy/unmapped) — the PR3/PR4 join skips them too.
    """
    out: dict[str, list[tuple[int, str]]] = {}
    for chunk in _chunks(market_ids, 900):
        ph = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT condition_id, outcome_index, token_id FROM token_conditions "  # noqa: S608
            f"WHERE condition_id IN ({ph}) AND outcome_index IS NOT NULL",
            chunk,
        ).fetchall()
        for cond, idx, tok in rows:
            out.setdefault(cond, []).append((int(idx), str(tok)))
    return out


def market_trades(conn: sqlite3.Connection, market_id: str) -> pd.DataFrame:
    """All trades for one market (uses ``idx_trades_market_id``).

    Columns ``[wallet, outcome_id, side, price, ts]`` (price = parsed ``price_str``; unparseable
    rows dropped).
    """
    rows = conn.execute(
        "SELECT wallet_hex, outcome_id, side, price_str, timestamp_unix "
        "FROM trades WHERE market_id = ?",
        (market_id,),
    ).fetchall()
    df = pd.DataFrame(rows, columns=["wallet", "outcome_id", "side", "price_str", "ts"])
    if df.empty:
        return df.assign(price=pd.Series(dtype=float)).drop(columns=["price_str"])
    df["price"] = pd.to_numeric(df["price_str"], errors="coerce")
    return df.drop(columns=["price_str"]).dropna(subset=["price"])


def _chunks(seq: list[str], n: int) -> list[list[str]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


# ── Per-market measurement ────────────────────────────────────────────────────────────────────
@dataclass
class MarketResult:
    """One sampled market's coverage outcome + the per-position CLV rows it contributes to (e)."""

    market_id: str
    clob_usable: bool
    trades_usable: bool
    trade_count: int
    pairs: list[
        tuple[float, float]
    ]  # (CLOB price, trades-last price) on shared buckets


def measure_market(
    market_id: str,
    resolved_at: int,
    close_ref: int,
    tokens: list[tuple[int, str]],
    clob_series: dict[str, list[tuple[int, float]]],
    trades: pd.DataFrame,
    clv_rows: list[tuple[str, float, float]],
) -> MarketResult:
    """Compute one market's CLOB/trades coverage + diffs, and append its (wallet, proxy, true) CLV
    rows to ``clv_rows`` for the cross-market measurement (e).

    A market is CLOB-usable if ANY of its outcome tokens has a >=3-point series in the window;
    trades-usable if the bucketed trades series has >=3 buckets (on any outcome). The window is
    ``[close_ref - 72h, close_ref]``.
    """
    win_start = close_ref - CLV_WINDOW_SECS
    token_by_outcome = {idx: tok for idx, tok in tokens}

    clob_buckets_by_outcome: dict[int, dict[int, float]] = {}
    clob_usable = False
    for idx, tok in tokens:
        pts = clob_series.get(tok, [])
        if usable(len(pts)):
            clob_usable = True
        clob_buckets_by_outcome[idx] = clob_bucket_series(pts, win_start, close_ref)

    trades_usable = False
    pairs: list[tuple[float, float]] = []
    for oid, g in trades.groupby("outcome_id", sort=False):
        tb = hourly_bucket_series(
            g["ts"].to_numpy(), g["price"].to_numpy(), win_start, close_ref
        )
        if usable(len(tb)):
            trades_usable = True
        cb = clob_buckets_by_outcome.get(int(oid), {})
        pairs.extend(aligned_pairs(cb, tb))

    _append_clv_rows(
        resolved_at, close_ref, token_by_outcome, clob_series, trades, clv_rows
    )
    return MarketResult(
        market_id=market_id,
        clob_usable=clob_usable,
        trades_usable=trades_usable,
        trade_count=int(len(trades)),
        pairs=pairs,
    )


def _append_clv_rows(
    resolved_at: int,
    close_ref: int,
    token_by_outcome: dict[int, str],
    clob_series: dict[str, list[tuple[int, float]]],
    trades: pd.DataFrame,
    clv_rows: list[tuple[str, float, float]],
) -> None:
    """For each (wallet, outcome) first BUY on this market, append ``(wallet, proxy_clv, true_clv)``.

    ``proxy_clv`` = ``close_proxy - entry`` (close_proxy = last trade price with ts < resolved_at;
    matches suff_stats). ``true_clv`` = ``true_close - entry`` (true_close = last CLOB point with
    t <= close_ref; matches the #429 PR4 plan). Rows with no proxy or no CLOB close are skipped.
    """
    pre = trades[trades["ts"] < resolved_at]
    if pre.empty:
        return
    # close_proxy per outcome = price of the last pre-resolution trade (any side).
    proxy_close = pre.sort_values("ts").groupby("outcome_id")["price"].last().to_dict()
    buys = trades[trades["side"] == "buy"]
    if buys.empty:
        return
    first_buys = (
        buys.sort_values("ts").groupby(["wallet", "outcome_id"], as_index=False).first()
    )
    for row in first_buys.itertuples(index=False):
        oid = int(row.outcome_id)
        entry = float(row.price)
        proxy = proxy_close.get(oid)
        tok = token_by_outcome.get(oid)
        true_close = (
            last_at_or_before(clob_series.get(tok, []), close_ref) if tok else None
        )
        if proxy is None or true_close is None:
            continue
        clv_rows.append(
            (str(row.wallet), float(proxy) - entry, float(true_close) - entry)
        )


def wallet_rank_signal(clv_rows: list[tuple[str, float, float]]) -> dict[str, float]:
    """Measurement (e): does true_clv re-rank wallets vs proxy_clv?

    Aggregates per-wallet mean proxy_clv and mean true_clv over wallets with >= MIN_WALLET_POSITIONS
    sample positions, then reports the Spearman rank-corr and top-decile overlap of the two
    rankings. Returns a metrics dict (NaN-friendly when the sample is too thin).
    """
    if not clv_rows:
        return {
            "wallets_compared": 0,
            "spearman": float("nan"),
            "top_decile_overlap": float("nan"),
        }
    df = pd.DataFrame(clv_rows, columns=["wallet", "proxy_clv", "true_clv"])
    agg = df.groupby("wallet").agg(
        n=("proxy_clv", "size"),
        proxy=("proxy_clv", "mean"),
        true=("true_clv", "mean"),
    )
    agg = agg[agg["n"] >= MIN_WALLET_POSITIONS]
    if len(agg) < 2:
        return {
            "wallets_compared": int(len(agg)),
            "spearman": float("nan"),
            "top_decile_overlap": float("nan"),
        }
    rho = spearman(agg["proxy"].to_numpy(), agg["true"].to_numpy())
    k = max(1, len(agg) // 10)
    overlap = topk_overlap(agg["proxy"].to_dict(), agg["true"].to_dict(), k)
    return {
        "wallets_compared": int(len(agg)),
        "spearman": rho,
        "top_decile_overlap": overlap,
        "top_decile_k": k,
    }


# ── Orchestration ─────────────────────────────────────────────────────────────────────────────
def run(args: argparse.Namespace) -> dict:
    """Execute the full comparison and return the metrics dict (also written to disk by main)."""
    conn = open_cache(args.cache)
    print(
        f"sampling up to {args.sample_size} resolved-with-winner markets...",
        file=sys.stderr,
    )
    sample = stratified_sample(conn, args.sample_size, args.seed)
    n = len(sample)
    print(f"  sampled {n} markets", file=sys.stderr)
    if n == 0:
        raise SystemExit(
            "no resolved-with-winner markets in the cache; nothing to compare"
        )

    market_ids = sample["market_id"].tolist()
    tokens = tokens_for_markets(conn, market_ids)
    all_tokens = sorted({tok for toks in tokens.values() for _, tok in toks})
    print(
        f"  {len(all_tokens)} distinct outcome tokens to fetch from CLOB",
        file=sys.stderr,
    )

    clob_series = _fetch_all_clob(args, sample, tokens, all_tokens)

    print("measuring per-market coverage + CLV rows...", file=sys.stderr)
    clv_rows: list[tuple[str, float, float]] = []
    results: list[MarketResult] = []
    for row in sample.itertuples(index=False):
        mid = str(row.market_id)
        res = measure_market(
            mid,
            int(row.resolved_at),
            int(row.close_ref),
            tokens.get(mid, []),
            clob_series,
            market_trades(conn, mid),
            clv_rows,
        )
        results.append(res)
    conn.close()

    return _summarize(n, results, clv_rows, args)


def _fetch_all_clob(
    args: argparse.Namespace,
    sample: pd.DataFrame,
    tokens: dict[str, list[tuple[int, str]]],
    all_tokens: list[str],
) -> dict[str, list[tuple[int, float]]]:
    """Concurrently fetch the CLOB window for every sampled token (per its market's close_ref)."""
    from concurrent.futures import ThreadPoolExecutor

    close_ref_by_market = dict(
        zip(sample["market_id"], sample["close_ref"], strict=True)
    )
    token_window: dict[str, tuple[int, int]] = {}
    for mid, toks in tokens.items():
        cr = int(close_ref_by_market.get(mid, 0))
        if cr <= 0:
            continue
        for _, tok in toks:
            token_window[tok] = (cr - CLV_WINDOW_SECS, cr)

    limiter = RateLimiter(args.clob_min_interval_ms / 1000.0)
    series: dict[str, list[tuple[int, float]]] = {}
    done = 0
    total = len(all_tokens)
    print(
        f"fetching {total} CLOB series ({args.clob_workers} workers)...",
        file=sys.stderr,
    )

    def fetch_one(tok: str) -> tuple[str, list[tuple[int, float]]]:
        start, end = token_window.get(tok, (0, 0))
        if end <= 0:
            return tok, []
        return tok, fetch_clob_series(
            args.clob_base, tok, start, end, FIDELITY_MIN, limiter, args.http_timeout
        )

    with ThreadPoolExecutor(max_workers=args.clob_workers) as ex:
        for tok, pts in ex.map(fetch_one, all_tokens):
            series[tok] = pts
            done += 1
            if done % 1000 == 0:
                print(f"  fetched {done}/{total}", file=sys.stderr)
    return series


def _summarize(
    n: int,
    results: list[MarketResult],
    clv_rows: list[tuple[str, float, float]],
    args: argparse.Namespace,
) -> dict:
    """Aggregate per-market results into the five PR2 measurements + the policy decision."""
    clob_usable = sum(r.clob_usable for r in results)
    trades_usable = sum(r.trades_usable for r in results)
    either_usable = sum(r.clob_usable or r.trades_usable for r in results)
    thin = sum(r.trade_count < MIN_TRADES_FLOOR for r in results)
    unrecoverable = sum(
        (not r.clob_usable) and r.trade_count < MIN_TRADES_FLOOR for r in results
    )
    all_pairs = [(c, t) for r in results for (c, t) in r.pairs]
    intersection_markets = sum(1 for r in results if r.pairs)
    if all_pairs:
        clob_arr = np.array([c for c, _ in all_pairs], dtype=np.float64)
        trade_arr = np.array([t for _, t in all_pairs], dtype=np.float64)
        median_abs_diff = float(np.median(np.abs(clob_arr - trade_arr)))
        correlation = pearson(clob_arr, trade_arr)
    else:
        median_abs_diff = float("nan")
        correlation = float("nan")

    clob_cov = 100.0 * clob_usable / n
    trades_cov = 100.0 * trades_usable / n
    either_cov = 100.0 * either_usable / n
    rank = wallet_rank_signal(clv_rows)
    decision = decide_policy(
        clob_coverage_pct=clob_cov,
        trades_coverage_pct=trades_cov,
        clob_plus_trades_coverage_pct=either_cov,
        median_abs_diff=median_abs_diff,
        true_vs_proxy_spearman=rank["spearman"],
    )
    return {
        "sample_size": n,
        "clob_base": args.clob_base,
        "window_secs": CLV_WINDOW_SECS,
        "fidelity_minutes": FIDELITY_MIN,
        "measurement_a_clob_coverage_pct": round(clob_cov, 2),
        "measurement_b_trades_coverage_pct": round(trades_cov, 2),
        "clob_or_trades_coverage_pct": round(either_cov, 2),
        "measurement_c_intersection_markets": intersection_markets,
        "measurement_c_correlation": (
            None if np.isnan(correlation) else round(correlation, 4)
        ),
        "measurement_c_median_abs_diff": (
            None if np.isnan(median_abs_diff) else round(median_abs_diff, 5)
        ),
        "measurement_c_mid_bias_flag": (
            bool(median_abs_diff > MID_BIAS_FLAG)
            if not np.isnan(median_abs_diff)
            else None
        ),
        "measurement_d_thin_markets_pct": round(100.0 * thin / n, 2),
        "measurement_d_unrecoverable_pct": round(100.0 * unrecoverable / n, 2),
        "measurement_e_wallet_rank": rank,
        "policy": decision.policy,
        "rationale": decision.rationale,
    }


def render_memo(metrics: dict) -> str:
    """Render the committed policy memo (``clv_source_comparison_results.md``) from the metrics."""
    m = metrics
    rank = m["measurement_e_wallet_rank"]
    sp = rank.get("spearman")
    sp_s = (
        "n/a" if sp is None or (isinstance(sp, float) and np.isnan(sp)) else f"{sp:.3f}"
    )
    mad = m["measurement_c_median_abs_diff"]
    mad_s = "n/a" if mad is None else f"{mad:.5f}"
    corr = m["measurement_c_correlation"]
    corr_s = "n/a" if corr is None else f"{corr:.4f}"
    overlap = rank.get("top_decile_overlap")
    overlap_s = (
        "n/a"
        if overlap is None or (isinstance(overlap, float) and np.isnan(overlap))
        else f"{overlap:.3f}"
    )
    return f"""# CLV source comparison — PR3 policy memo (issue #429 PR2)

Generated by `scripts/ranker/clv_source_comparison.py` (the "investigate fuller sources first"
gate). This memo **gates PR3**: PR3 builds only the price-series source(s) selected below.

## Decision

**Policy: `{m["policy"]}`**

{m["rationale"]}

## Measurements (sample: {m["sample_size"]} liquidity x recency-stratified resolved-with-winner markets)

| # | Measurement | Value |
|---|---|---|
| a | CLOB coverage (>=3-pt series in [close_ref-72h, close_ref]) | **{m["measurement_a_clob_coverage_pct"]}%** |
| b | Trades hourly-bucket coverage (>=3 buckets) | **{m["measurement_b_trades_coverage_pct"]}%** |
|   | CLOB OR trades coverage | {m["clob_or_trades_coverage_pct"]}% |
| c | Intersection markets (both sources) | {m["measurement_c_intersection_markets"]} |
| c | CLOB-vs-trades-last correlation (shared buckets) | {corr_s} |
| c | Median \\|CLOB - trades-last\\| on shared buckets | {mad_s} (flag if > {MID_BIAS_FLAG}: {m["measurement_c_mid_bias_flag"]}) |
| d | Thin markets (< {MIN_TRADES_FLOOR} trades) | {m["measurement_d_thin_markets_pct"]}% |
| d | **Unrecoverable floor** (empty CLOB AND < {MIN_TRADES_FLOOR} trades) | **{m["measurement_d_unrecoverable_pct"]}%** |
| e | true_clv vs proxy_clv wallet-rank Spearman | **{sp_s}** ({rank.get("wallets_compared", 0)} wallets) |
| e | top-decile overlap | {overlap_s} |

## Interpretation

- **(a)/(b) coverage** decides the price-series SOURCE if `true_clv` is worth building.
- **(c)** checks whether trades-last is a sound stand-in for the CLOB series (bias flag at
  {MID_BIAS_FLAG}).
- **(d)** is the hard ceiling: these markets get `proxy_clv` only, regardless of source — `true_clv`
  is additive, never universal (issue #429 design).
- **(e)** is the value question: a high Spearman means the CLOB series barely re-ranks wallets vs
  the `proxy_clv` we already have, so the price-series pipeline is low-value; a low Spearman means
  `true_clv` adds genuine signal and is worth building from the source (a)/(b) selects. **Caveat:**
  (e) is a sample-scoped estimate — per-wallet CLV is averaged over only the wallet's positions in
  the sampled markets, so it is noisier than the coverage stats; treat it as directional, with
  (a)-(d) as the primary drivers. The `proxy_clv_suffices` gate fires only at Spearman >= 0.95
  (sampling noise pushes the statistic toward 0, not 1, so it cannot spuriously trigger that gate).

_Reproduce:_ `.venv-analysis/bin/python -m scripts.ranker.clv_source_comparison --cache
data/wallet_cache.db --out-dir <dir> --sample-size {m["sample_size"]}`
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="CLV source comparison (issue #429 PR2)")
    ap.add_argument("--cache", required=True, help="path to wallet_cache.db")
    ap.add_argument(
        "--out-dir", required=True, help="output directory for the report + memo"
    )
    ap.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--clob-base", default=DEFAULT_CLOB_BASE)
    ap.add_argument("--clob-workers", type=int, default=DEFAULT_CLOB_WORKERS)
    ap.add_argument(
        "--clob-min-interval-ms", type=int, default=DEFAULT_CLOB_MIN_INTERVAL_MS
    )
    ap.add_argument("--http-timeout", type=float, default=DEFAULT_HTTP_TIMEOUT_S)
    args = ap.parse_args()

    metrics = run(args)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "clv_source_comparison.json").write_text(json.dumps(metrics, indent=2))
    (out / "clv_source_comparison_results.md").write_text(render_memo(metrics))
    print(json.dumps(metrics, indent=2))
    print(
        f"\npolicy={metrics['policy']}  "
        f"(memo: {out / 'clv_source_comparison_results.md'})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
