#!/usr/bin/env python3
"""
72hr buy-and-hold wallet ranking + variance-minimizing group selection.

The single authoritative pass-1 ranker (issue #370 consolidated the former
streaming/non-streaming pair into this one file — "streaming" is no longer a
variant, it is just how the ranker works).

Memory-safe inline scan: each wallet's summary stats are computed during a
per-wallet indexed `trades` scan; every qualifying first-buy position is streamed
to qualifying_positions_72hr.csv on disk (including `outcome_id`, which pass-2
`latency_shift_rerank.py` requires); and compact per-wallet arrays are retained in
RAM ONLY for wallets that clear eligibility + the edge floor (a few hundred) — all
the group-Sharpe stage needs. Peak RAM ~= market maps + per-wallet summaries
(~1 GB), roughly flat in the universe size, instead of the tens of GB an
all-positions DataFrame would need over the full ~496 K-wallet universe (which
OOMs at production scale), so it scales to the entire trade history.

Pipeline (see docs/26 and docs/_GLOSSARY.md):

  Universe  : EITHER `--universe <file>` (one 0x-wallet per line) OR
              `--universe-from-trades` (every distinct wallet_hex in `trades`;
              have-trade-data => in-universe, issue #370 — the ranker's own filters
              then decide the cohort). Exactly one of the two is required.
  Stage 1   : per (wallet, market) take the FIRST-EVER buy (the entry). Keep it
              only if, at entry time, min_ttr <= time-to-resolution < TTR_HOURS, the
              market is resolved, the entry date is in the analysis window, and the
              entry price is a valid 0<p<1 outcome price inside the price band.
  Stage 2   : hold-to-resolution per-position edge, expressed as UNCAPPED return on
              stake:  gross = (payoff - price)/price, payoff=1 if the bought outcome
              won else 0.  net = (payoff - eff)/eff, eff = min(price+slip, 0.999).
  Stage 3   : eligibility = avg >= MIN_AVG_PER_MONTH qualifying entries per ACTIVE
              month AND active in >= MIN_ACTIVE_MONTHS of the window.  Rank eligible
              wallets by recency-weighted net t-stat (issue #366 decay: a trade one
              `--half-life-days` old weighs 0.5; <= 0 disables decay = flat = legacy,
              bitwise-identical via the weighted_stats uniform-weight short-circuit).
              -> ranked_72hr_buyandhold
  Stage 4   : from the edge-floored eligible pool, greedily select TARGET_N wallets
              that MAXIMISE the equal-weight copy-portfolio's group Sharpe over a
              weekly (by resolution date) return series.
              -> 250_72hr_buyandhold_variance

TTR reference for "<72hr to resolution": scheduled end_date_unix, or (without
--scheduled-only) COALESCE(scheduled end_date_unix, actual resolved_at_unix); the
resolved_at fallback is look-ahead, so --scheduled-only drops it.

This is deliberately self-contained (stdlib sqlite3 + pandas/numpy) so it can be
re-run and independently verified.
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import sqlite3
import sys
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd

import ranker_duck
from ranker_decay import (
    DEFAULT_HALF_LIFE_DAYS,
    DEFAULT_WINDOW_DAYS,
    decay_weights,
    parse_as_of,
    today_midnight_unix,
    weighted_stats,
    window_start_unix,
)

SECS_PER_DAY = 86_400
WEEK_SECS = 7 * SECS_PER_DAY


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


@dataclass
class Params:
    db: str
    universe: str | None      # universe-file path; None when --universe-from-trades
    universe_from_trades: bool  # if True, universe = all distinct wallet_hex in `trades` (#370)
    out_dir: str
    win_start: int          # unix, inclusive (entry date)
    win_end: int            # unix, exclusive
    ttr_secs: int
    min_ttr_secs: int       # lower TTR bound: drop first-buys entered < this close to resolution (copyability floor)
    min_avg_per_month: float
    min_active_months: int
    target_n: int
    slip: float             # absolute price slippage (e.g. 0.01 = 1 cent)
    floor_tstat: float      # Stage-4 net-edge floor: keep wallets with net t-stat >= this
    scheduled_only: bool    # if True, TTR ref = scheduled end_date_unix ONLY (no resolved_at fallback = no look-ahead)
    limit_wallets: int      # 0 = all
    price_min: float        # entry-price band lower bound (inclusive)
    price_max: float        # entry-price band upper bound (inclusive)
    half_life_days: float   # recency-decay half-life in days; <= 0 disables decay (flat = legacy)
    as_of: int              # decay age anchor (unix); trades older than this decay, future clip to weight 1.0


def parse_args() -> Params:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/wallet_cache.db")
    p.add_argument("--universe", default=None,
                   help="universe file: one 0x-wallet per line. Mutually exclusive with "
                        "--universe-from-trades; exactly one is required.")
    p.add_argument("--universe-from-trades", action="store_true",
                   help="use every distinct wallet_hex in `trades` as the universe "
                        "(have-trade-data => in-universe, issue #370). Mutually exclusive "
                        "with --universe; exactly one is required.")
    p.add_argument("--out-dir", default="data/eval-results")
    p.add_argument("--win-start", default=None,
                   help="entry-date window start (ISO). Default: win_end - "
                        f"{DEFAULT_WINDOW_DAYS}d (relative).")
    p.add_argument("--win-end", default=None,
                   help="entry-date window end (ISO), exclusive. Default: today UTC-midnight (relative).")
    p.add_argument("--half-life-days", type=float, default=DEFAULT_HALF_LIFE_DAYS,
                   help="exponential recency-decay half-life in days for the edge/t-stat score "
                        "(a trade one half-life old weighs 0.5). <= 0 disables decay (flat = legacy). "
                        "Eligibility/activity counts and hit_rate stay raw.")
    p.add_argument("--as-of", default=None,
                   help="decay age anchor (ISO date/datetime or unix epoch). Default: win_end.")
    p.add_argument("--ttr-hours", type=float, default=72.0)
    p.add_argument("--min-ttr-hours", type=float, default=30.0 / 3600.0,
                   help="lower TTR bound: drop first-buys entered < this many hours before "
                        "resolution. Default 30s = physical-feasibility floor: end-to-end "
                        "copy latency is ~10-20s (/activity indexes in ~1-4s + ~5s poll + "
                        "~5s fill; measured 2026-06-14, docs/29). Inclusion below ~1min is "
                        "only honest when paired with latency-shifted fill pricing "
                        "(scripts/latency_shift_rerank.py) which scores each copy at the "
                        "price we'd actually get, not the leader's.")
    p.add_argument("--min-avg-per-month", type=float, default=20.0)
    p.add_argument("--min-active-months", type=int, default=3)
    p.add_argument("--target-n", type=int, default=250)
    p.add_argument("--slip-cents", type=float, default=1.0,
                   help="entry slippage in cents of price (capped near $1); net = (payoff-eff)/eff")
    p.add_argument("--floor-tstat", type=float, default=2.0,
                   help="Stage-4 floor: only wallets with net t-stat >= this enter group selection")
    p.add_argument("--scheduled-only", action="store_true",
                   help="TTR ref = scheduled end_date_unix ONLY; drop the resolved_at fallback (removes look-ahead leakage)")
    p.add_argument("--limit-wallets", type=int, default=0)
    p.add_argument("--price-min", type=float, default=0.15,
                   help="entry-price band lower bound (inclusive); first-buys below are dropped")
    p.add_argument("--price-max", type=float, default=0.85,
                   help="entry-price band upper bound (inclusive); first-buys above are dropped")
    a = p.parse_args()

    # Exactly one universe source. --universe defaults to None so argparse can tell an
    # explicit --universe from an omitted one; supplying both, or neither, is an error.
    if (a.universe is not None) == bool(a.universe_from_trades):
        p.error("provide exactly one of --universe <file> or --universe-from-trades")

    def to_unix(d: str) -> int:
        return int(pd.Timestamp(d, tz="UTC").timestamp())

    # Relative window defaults (overridable): win_end -> today UTC-midnight;
    # win_start -> win_end - DEFAULT_WINDOW_DAYS. Anchor decay at as_of (default win_end).
    win_end = to_unix(a.win_end) if a.win_end else today_midnight_unix()
    win_start = to_unix(a.win_start) if a.win_start else window_start_unix(win_end, DEFAULT_WINDOW_DAYS)
    as_of = parse_as_of(a.as_of)
    if as_of is None:
        as_of = win_end

    return Params(
        db=a.db, universe=a.universe, universe_from_trades=a.universe_from_trades,
        out_dir=a.out_dir,
        win_start=win_start, win_end=win_end,
        ttr_secs=int(a.ttr_hours * 3600),
        min_ttr_secs=int(a.min_ttr_hours * 3600),
        min_avg_per_month=a.min_avg_per_month,
        min_active_months=a.min_active_months,
        target_n=a.target_n,
        slip=a.slip_cents / 100.0,
        floor_tstat=a.floor_tstat,
        scheduled_only=a.scheduled_only,
        limit_wallets=a.limit_wallets,
        price_min=a.price_min,
        price_max=a.price_max,
        half_life_days=a.half_life_days,
        as_of=as_of,
    )


def load_universe(path: str, limit: int) -> list[str]:
    wallets = []
    with open(path) as f:
        for line in f:
            s = line.strip()
            if s.startswith("0x") and len(s) == 42:
                wallets.append(s.lower())
    if limit > 0:
        wallets = wallets[:limit]
    return wallets


def load_universe_from_trades(conn: sqlite3.Connection, limit: int) -> list[str]:
    """Universe = every distinct wallet_hex in `trades` (have-trade-data => in-universe,
    issue #370). Same validation as load_universe (0x-prefixed, length 42, lowercased);
    malformed rows are dropped. The ranker's own filters then decide the cohort."""
    wallets: list[str] = []
    for (wh,) in conn.execute("SELECT DISTINCT wallet_hex FROM trades"):
        if wh is None:
            continue
        s = str(wh).strip()
        if s.startswith("0x") and len(s) == 42:
            wallets.append(s.lower())
    if limit > 0:
        wallets = wallets[:limit]
    return wallets


def load_market_maps(conn: sqlite3.Connection):
    """market_id -> winning_outcome_id, resolved_at_unix ; market_id -> end_date_unix."""
    log("loading market_resolutions ...")
    res = {}
    for mid, win, rat in conn.execute(
        "SELECT market_id, winning_outcome_id, resolved_at_unix FROM market_resolutions "
        "WHERE winning_outcome_id IS NOT NULL"
    ):
        res[mid] = (win, rat)
    log(f"  {len(res):,} resolved markets")

    log("loading market_schedules ...")
    sched = {}
    for mid, end in conn.execute(
        "SELECT market_id, end_date_unix FROM market_schedules WHERE end_date_unix IS NOT NULL"
    ):
        sched[mid] = end
    log(f"  {len(sched):,} scheduled markets")
    return res, sched


def group_sharpe(sum_t: np.ndarray, cnt_t: np.ndarray) -> float:
    """Annualisation-free Sharpe of the weekly copy-basket return series."""
    active = cnt_t > 0
    if active.sum() < 2:
        return -np.inf
    r = sum_t[active] / cnt_t[active]
    sd = r.std(ddof=1)
    if sd <= 0:
        return -np.inf
    return float(r.mean() / sd * np.sqrt(len(r)))


def greedy_max_group_sharpe(sumg: np.ndarray, npos: np.ndarray, target_n: int,
                            seed_order: list[int]) -> list[int]:
    """Forward selection maximising group Sharpe of the position-weighted weekly series.
    seed_order = wallet indices sorted by individual t-stat (first pick + tie context)."""
    n_wal, n_wk = sumg.shape
    selected: list[int] = []
    in_sel = np.zeros(n_wal, dtype=bool)
    sum_t = np.zeros(n_wk)
    cnt_t = np.zeros(n_wk)

    first = seed_order[0]
    selected.append(first); in_sel[first] = True
    sum_t += sumg[first]; cnt_t += npos[first]

    target = min(target_n, n_wal)
    while len(selected) < target:
        best_i, best_s = -1, -np.inf
        for c in np.where(~in_sel)[0]:
            s = group_sharpe(sum_t + sumg[c], cnt_t + npos[c])
            if s > best_s:
                best_s, best_i = s, c
        if best_i < 0:
            break
        selected.append(best_i); in_sel[best_i] = True
        sum_t += sumg[best_i]; cnt_t += npos[best_i]
        if len(selected) % 25 == 0:
            log(f"  group select {len(selected)}/{target}  groupSharpe={best_s:.3f}")
    return selected


def process_wallet_positions(w, positions, prm, writer, summaries, floor_pos):
    """Score one wallet's QUALIFYING first-buy positions and emit its CSV rows +
    summary + floor entry. Shared by the SQLite and DuckDB extraction paths so
    `eff`/`gross`/`net` and the decayed `weighted_stats` run through exactly one code
    path (issue #375 bit-parity). `positions` is a list of dicts with keys market_id,
    outcome_id, entry_ts, ttr, price (float), contracts, payoff (float), resolved_at —
    already filtered. Returns the number of positions scored.

    # Precondition: positions are pre-filtered (window / ttr / resolved / price / band).
    """
    if not positions:
        return 0
    nets: list[float] = []
    grosses: list[float] = []
    payoffs: list[float] = []
    prices: list[float] = []
    ttrs: list[int] = []
    resolveds: list[int] = []
    entry_tss: list[int] = []  # raw entry timestamps -> recency-decay weights (#366)
    months: set[tuple[int, int]] = set()
    rows_out: list[tuple] = []
    for p in positions:
        price = p["price"]
        payoff = p["payoff"]
        gross = (payoff - price) / price
        # realistic price-aware entry slippage (module docstring Stage 2): cross the
        # spread by up to `slip`, never paying more than ~$1; hold-to-resolution has
        # no exit cost (settles at $1/$0).
        eff = min(price + prm.slip, 0.999)
        net = (payoff - eff) / eff
        nets.append(net)
        grosses.append(gross)
        payoffs.append(payoff)
        prices.append(price)
        ttrs.append(p["ttr"])
        resolveds.append(p["resolved_at"])
        entry_tss.append(p["entry_ts"])
        g = time.gmtime(p["entry_ts"])
        months.add((g.tm_year, g.tm_mon))
        rows_out.append((w, p["market_id"], p["outcome_id"], p["entry_ts"], p["ttr"],
                         price, int(p["contracts"]), payoff, gross, net, p["resolved_at"]))

    writer.writerows(rows_out)

    n = len(nets)
    net_arr = np.asarray(nets, dtype=float)
    gross_arr = np.asarray(grosses, dtype=float)
    active_months = len(months)
    # Recency-decay weights (a function only of entry_ts, as_of, half_life).
    # half_life <= 0 -> all-ones -> weighted_stats is bitwise-identical to legacy.
    wdecay = decay_weights(np.asarray(entry_tss, dtype=float), prm.as_of, prm.half_life_days)
    mean_gross, std_gross, _, tstat_gross = weighted_stats(gross_arr, wdecay)
    mean_net, std_net, n_eff, tstat_net = weighted_stats(net_arr, wdecay)
    summaries.append({
        "wallet": w,
        "n": n,
        "active_months": active_months,
        "avg_per_active_month": n / active_months,
        "mean_gross": mean_gross,
        "std_gross": std_gross,
        "tstat_gross": tstat_gross,
        "mean_net": mean_net,
        "std_net": std_net,
        "tstat_net": tstat_net,
        "n_eff": n_eff,
        "hit_rate": float(np.asarray(payoffs).mean()),
        "avg_price": float(np.asarray(prices).mean()),
        "avg_ttr_hours": float(np.asarray(ttrs, dtype=float).mean() / 3600.0),
    })

    # Inline floor-retention uses the SAME decayed tstat_net/mean_net as the
    # DataFrame floor below, so the two can never diverge.
    eligible = (
        (n / active_months >= prm.min_avg_per_month)
        and (active_months >= prm.min_active_months)
        and (n > 1)
        and not math.isnan(tstat_net)
    )
    if eligible and (tstat_net >= prm.floor_tstat) and (mean_net > 0):
        floor_pos[w] = (net_arr, np.asarray(resolveds, dtype=np.int64))
    return n


def scan_and_filter_sqlite(conn, w, prm, res, sched, diag):
    """SQLite path: per-wallet first-buy scan + the qualification filters, returning
    the list of qualifying position dicts that `process_wallet_positions` consumes.
    Identical filtering to the pre-#375 inline loop — the only change is that the
    qualifying rows are collected into a list instead of being scored in place."""
    cur = conn.execute(
        "SELECT market_id, outcome_id, price_str, contracts, timestamp_unix "
        "FROM trades WHERE wallet_hex = ? AND side = 'buy' ORDER BY timestamp_unix ASC",
        (w,),
    )
    # first-ever buy per market
    first: dict = {}
    for mid, oid, price_str, contracts, ts in cur:
        if mid not in first:
            first[mid] = (oid, price_str, contracts, ts)
    diag["first_buys"] += len(first)

    positions: list[dict] = []
    for mid, (oid, price_str, contracts, ts) in first.items():
        # entry date must be in window
        if not (prm.win_start <= ts < prm.win_end):
            diag["out_of_window"] += 1
            continue
        end = sched.get(mid)
        r = res.get(mid)
        # TTR reference. scheduled_only=True uses ONLY the scheduled end (known at entry);
        # the resolved_at fallback is look-ahead (actual resolution time is unknown at entry).
        if prm.scheduled_only:
            ref = end
        else:
            ref = end if end is not None else (r[1] if r is not None else None)
        if ref is None:
            diag["no_ref"] += 1
            continue
        ttr = ref - ts
        if ttr < max(prm.min_ttr_secs, 1) or ttr >= prm.ttr_secs:
            diag["ttr_fail"] += 1
            continue
        if r is None:
            diag["unresolved"] += 1
            continue
        try:
            price = float(price_str)
        except (TypeError, ValueError):
            diag["bad_price"] += 1
            continue
        if not (0.0 < price < 1.0):
            diag["bad_price"] += 1
            continue
        # entry-price band filter: copy-trade only mid-priced first-buys
        if not (prm.price_min <= price <= prm.price_max):
            diag["out_of_band"] += 1
            continue
        win_oid, resolved_at = r
        payoff = 1.0 if int(oid) == int(win_oid) else 0.0
        positions.append({
            "market_id": mid, "outcome_id": oid, "entry_ts": ts, "ttr": ttr,
            "price": price, "contracts": contracts, "payoff": payoff,
            "resolved_at": resolved_at,
        })
        diag["qualified"] += 1
    return positions


def main() -> int:
    prm = parse_args()
    os.makedirs(prm.out_dir, exist_ok=True)
    log(f"params: {prm}")

    # Open the read-only cache first: --universe-from-trades enumerates from it.
    conn = sqlite3.connect(f"file:{prm.db}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON;")

    if prm.universe_from_trades:
        wallets = load_universe_from_trades(conn, prm.limit_wallets)
        universe_label = "trades-distinct"
        log(f"universe: {len(wallets)} wallets (all distinct trade wallets, #370)")
    else:
        wallets = load_universe(prm.universe, prm.limit_wallets)
        universe_label = os.path.basename(prm.universe)
        log(f"universe: {len(wallets)} wallets ({universe_label})")

    # Pick the extraction engine (DuckDB Parquet read-layer or SQLite fallback, #375).
    # Market maps (resolutions + schedules) are only needed by the SQLite path; the
    # DuckDB path joins them in SQL over the Parquet snapshot.
    engine = ranker_duck.get_engine()
    res, sched = (None, None) if engine is not None else load_market_maps(conn)

    decay = "flat (no decay)" if prm.half_life_days <= 0 else f"half_life={prm.half_life_days}d"
    log(f"scoring: {decay}, as_of={prm.as_of} (window [{prm.win_start},{prm.win_end}))")

    pos_path = os.path.join(prm.out_dir, "qualifying_positions_72hr.csv")
    pos_fh = open(pos_path, "w", newline="")
    writer = csv.writer(pos_fh)
    writer.writerow(["wallet", "market_id", "outcome_id", "entry_ts", "ttr_secs", "price",
                     "contracts", "payoff", "gross", "net", "resolved_at"])

    summaries: list[dict] = []
    floor_pos: dict[str, tuple[np.ndarray, np.ndarray]] = {}  # wallet -> (net, resolved_at)
    diag = dict(wallets_seen=0, first_buys=0, no_ref=0, ttr_fail=0, unresolved=0,
                bad_price=0, out_of_band=0, out_of_window=0, qualified=0)
    t0 = time.time()
    total_qualified = 0

    if engine is not None:
        # DuckDB read-layer (#375): the heavy scan + first-buy dedup + filters run in
        # SQL over the Parquet snapshot; the SHARED Python tail scores every wallet, so
        # eff/gross/net + weighted_stats are byte-identical to the SQLite path.
        log("extraction engine: DuckDB (Parquet read-layer, #375)")
        ttr_lo = max(prm.min_ttr_secs, 1)
        df = ranker_duck.duck_extract_positions(
            engine, wallets, prm.win_start, prm.win_end, ttr_lo, prm.ttr_secs,
            prm.scheduled_only, prm.price_min, prm.price_max,
        )
        diag["wallets_seen"] = len(wallets)
        diag["qualified"] = len(df)
        # Stable per-wallet order so summation is deterministic run-to-run; the stats
        # are order-invariant beyond sub-ULP FP (absorbed by the parity rtol).
        df = df.sort_values(["wallet", "entry_ts"], kind="stable")
        for w, grp in df.groupby("wallet", sort=True):
            positions = [{
                "market_id": row.market_id, "outcome_id": int(row.outcome_id),
                "entry_ts": int(row.entry_ts), "ttr": int(row.ttr_secs),
                "price": float(row.price), "contracts": int(row.contracts),
                "payoff": float(row.payoff), "resolved_at": int(row.resolved_at),
            } for row in grp.itertuples(index=False)]
            total_qualified += process_wallet_positions(
                w, positions, prm, writer, summaries, floor_pos)
    else:
        log("extraction engine: SQLite (per-wallet inline scan)")
        for i, w in enumerate(wallets):
            diag["wallets_seen"] += 1
            positions = scan_and_filter_sqlite(conn, w, prm, res, sched, diag)
            total_qualified += process_wallet_positions(
                w, positions, prm, writer, summaries, floor_pos)
            if (i + 1) % 5000 == 0:
                log(f"  {i+1}/{len(wallets)} wallets ({time.time()-t0:.0f}s, "
                    f"{total_qualified:,} qualifying, {len(floor_pos)} floor)")

    pos_fh.close()
    log(f"extraction done in {time.time()-t0:.0f}s")
    log(f"  diagnostics: {diag}")
    log(f"wrote {pos_path}  ({total_qualified:,} positions)")

    if not summaries:
        log("no qualifying positions; aborting")
        return 1

    stats = pd.DataFrame(summaries)
    stats["eligible"] = (
        (stats["avg_per_active_month"] >= prm.min_avg_per_month)
        & (stats["active_months"] >= prm.min_active_months)
        & (stats["n"] > 1)
        & stats["tstat_net"].notna()
    )
    # primary ranking = net t-stat (realistic price-aware net edge, risk-adjusted)
    stats = stats.sort_values("tstat_net", ascending=False, na_position="last").reset_index(drop=True)
    n_elig = int(stats["eligible"].sum())
    log(f"eligible wallets (avg>={prm.min_avg_per_month}/mo, >={prm.min_active_months} active months): {n_elig}")

    ranked_path = os.path.join(prm.out_dir, "ranked_72hr_buyandhold.csv")
    stats.to_csv(ranked_path, index=False)
    log(f"wrote {ranked_path}")

    elig = stats[stats["eligible"]].copy().reset_index(drop=True)
    if n_elig == 0:
        log("no eligible wallets; stopping after ranking")
        return 0

    # intermediate deliverable: ranked_72hr_buyandhold (eligible, ranked by net t-stat)
    ranked_txt = os.path.join(prm.out_dir, "ranked_72hr_buyandhold.txt")
    with open(ranked_txt, "w") as f:
        f.write("# ranked_72hr_buyandhold — eligible wallets ranked by t-stat(net, price-aware)\n")
        f.write(f"# n_eligible={n_elig}\n")
        for wlt in elig["wallet"].tolist():
            f.write(wlt + "\n")
    log(f"wrote {ranked_txt}")

    # Stage 4 — EDGE FLOOR then max group Sharpe (on net returns).
    floor = elig[(elig["tstat_net"] >= prm.floor_tstat) & (elig["mean_net"] > 0)].reset_index(drop=True)
    log(f"edge floor (net t-stat >= {prm.floor_tstat} & mean_net > 0): {len(floor)} winners")
    if len(floor) < prm.target_n:
        log(f"WARNING: only {len(floor)} wallets clear the floor (< target {prm.target_n}); "
            f"selecting all of them. Lower --floor-tstat to widen the pool.")
    floor_wallets = floor["wallet"].tolist()
    if not floor_wallets:
        log("no wallets clear the edge floor; stopping")
        return 0

    # Weekly (sum, count) matrices of net return per (wallet, resolution-week), built from
    # the retained per-wallet arrays. Group return for a set S in week t is
    #   R_t = sum_{w in S} sum_return[w,t] / sum_{w in S} n_pos[w,t]
    # i.e. equal-$-per-position return on deployed capital that week (copy-basket return).
    week_of: dict[str, np.ndarray] = {}
    all_weeks: set[int] = set()
    for w in floor_wallets:
        _net, _res = floor_pos[w]
        wk = (_res // WEEK_SECS).astype(int)
        week_of[w] = wk
        all_weeks.update(wk.tolist())
    weeks = np.sort(np.array(sorted(all_weeks)))
    week_idx = {int(wk): j for j, wk in enumerate(weeks)}
    sumr = np.zeros((len(floor_wallets), len(weeks)))
    npos = np.zeros((len(floor_wallets), len(weeks)))
    for i, w in enumerate(floor_wallets):
        net_arr, _res = floor_pos[w]
        wk = week_of[w]
        cols = np.array([week_idx[int(x)] for x in wk])
        np.add.at(sumr[i], cols, net_arr)
        np.add.at(npos[i], cols, 1.0)
    log(f"weekly matrix (floored pool): {sumr.shape[0]} wallets x {sumr.shape[1]} weeks")

    seed_order = list(range(len(floor_wallets)))  # floor already sorted by tstat_net desc
    sel_idx = greedy_max_group_sharpe(sumr, npos, prm.target_n, seed_order)
    sel_wallets = [floor_wallets[i] for i in sel_idx]

    sum_t = sumr[sel_idx].sum(axis=0)
    cnt_t = npos[sel_idx].sum(axis=0)
    gs = group_sharpe(sum_t, cnt_t)
    seldf = elig[elig["wallet"].isin(set(sel_wallets))]
    log(f"selected {len(sel_wallets)} wallets, net group Sharpe = {gs:.3f}; "
        f"mean_net={seldf['mean_net'].mean():.4f} hit_rate={seldf['hit_rate'].mean():.3f} "
        f"avg_price={seldf['avg_price'].mean():.3f}")

    out_list = os.path.join(prm.out_dir, "250_72hr_buyandhold_variance.txt")
    with open(out_list, "w") as f:
        f.write("# 250_72hr_buyandhold_variance — edge-floor (net t-stat>="
                f"{prm.floor_tstat}, mean_net>0) then max group Sharpe on net returns\n")
        f.write(f"# universe={universe_label} eligible={n_elig} "
                f"floor_pool={len(floor)} selected={len(sel_wallets)}\n")
        f.write(f"# net_group_sharpe={gs:.4f} slip={prm.slip}\n")
        for w in sel_wallets:
            f.write(w + "\n")
    log(f"wrote {out_list}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
