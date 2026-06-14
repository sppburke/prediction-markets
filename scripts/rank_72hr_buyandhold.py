#!/usr/bin/env python3
"""
72hr buy-and-hold wallet ranking + variance-minimizing group selection.

Pipeline (see docs/ and the session methodology lock-in):

  Universe  : the newest GBM/BHq intersection-3 list (default 2,307 wallets).
  Stage 1   : per (wallet, market) take the FIRST-EVER buy (the entry). Keep it
              only if, at entry time, time-to-resolution < TTR_HOURS, the market
              is resolved, the entry date is in the analysis window, and the
              entry price is a valid 0<p<1 outcome price.
  Stage 2   : hold-to-resolution per-position edge, expressed as UNCAPPED return
              on stake:  gross = (payoff - price)/price, payoff=1 if the bought
              outcome won else 0.  net = gross - haircut_frac (diagnostic).
  Stage 3   : eligibility = avg >= MIN_AVG_PER_MONTH qualifying entries per ACTIVE
              month AND active in >= MIN_ACTIVE_MONTHS of the window.  Rank the
              eligible wallets by t-stat = mean(gross)/std(gross) * sqrt(n).
              -> ranked_72hr_buyandhold
  Stage 4   : from the eligible pool, greedily select TARGET_N wallets that
              MAXIMISE the equal-weight copy-portfolio's group Sharpe over a
              weekly (by resolution date) return series.
              -> 250_72hr_buyandhold_variance

TTR reference for "<72hr to resolution": COALESCE(scheduled end_date_unix,
actual resolved_at_unix).

This is deliberately self-contained (stdlib sqlite3 + pandas/numpy) so it can be
re-run and independently verified.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd

SECS_PER_DAY = 86_400
WEEK_SECS = 7 * SECS_PER_DAY


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


@dataclass
class Params:
    db: str
    universe: str
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


def parse_args() -> Params:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/wallet_cache.db")
    p.add_argument("--universe",
                   default="data/archive/research-2026-05/watchlist-20260528T194034Z-gbm_bhq_intersection_3.txt")
    p.add_argument("--out-dir", default="data/eval-results")
    p.add_argument("--win-start", default="2025-12-01")
    p.add_argument("--win-end", default="2026-06-01")
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

    def to_unix(d: str) -> int:
        return int(pd.Timestamp(d, tz="UTC").timestamp())

    return Params(
        db=a.db, universe=a.universe, out_dir=a.out_dir,
        win_start=to_unix(a.win_start), win_end=to_unix(a.win_end),
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


def extract_positions(conn, wallets, res, sched, prm: Params) -> pd.DataFrame:
    """One qualifying first-buy position per (wallet, market). Per-wallet indexed query."""
    rows = []
    diag = dict(wallets_seen=0, first_buys=0, no_ref=0, ttr_fail=0, unresolved=0,
                bad_price=0, out_of_band=0, out_of_window=0, qualified=0)
    t0 = time.time()
    for i, w in enumerate(wallets):
        diag["wallets_seen"] += 1
        cur = conn.execute(
            "SELECT market_id, outcome_id, price_str, contracts, timestamp_unix "
            "FROM trades WHERE wallet_hex = ? AND side = 'buy' ORDER BY timestamp_unix ASC",
            (w,),
        )
        # first-ever buy per market
        first = {}
        for mid, oid, price_str, contracts, ts in cur:
            if mid not in first:
                first[mid] = (oid, price_str, contracts, ts)
        diag["first_buys"] += len(first)
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
            gross = (payoff - price) / price
            # realistic price-aware entry slippage: you cross the spread by up to
            # `slip`, but can never pay more than ~$1 (favorites pay only the headroom).
            # Buy-and-hold-to-resolution has NO exit cost (settles at $1/$0).
            eff = min(price + prm.slip, 0.999)
            net = (payoff - eff) / eff
            rows.append((w, mid, ts, ttr, price, int(contracts), payoff,
                         gross, net, resolved_at))
            diag["qualified"] += 1
        if (i + 1) % 250 == 0:
            log(f"  {i+1}/{len(wallets)} wallets  ({time.time()-t0:.0f}s, "
                f"{diag['qualified']:,} qualifying positions)")
    log(f"extraction done in {time.time()-t0:.0f}s")
    log(f"  diagnostics: {diag}")
    df = pd.DataFrame(rows, columns=[
        "wallet", "market_id", "entry_ts", "ttr_secs", "price", "contracts",
        "payoff", "gross", "net", "resolved_at"])
    return df


def rank_wallets(df: pd.DataFrame, prm: Params) -> pd.DataFrame:
    """Per-wallet stats + eligibility + t-stat ranking."""
    df = df.copy()
    df["month"] = pd.to_datetime(df["entry_ts"], unit="s").dt.to_period("M").astype(str)

    def tstat(x: np.ndarray) -> float:
        n = len(x)
        sd = float(x.std(ddof=1)) if n > 1 else float("nan")
        m = float(x.mean())
        return (m / sd * np.sqrt(n)) if (sd and sd > 0 and n > 1) else float("nan")

    def agg(g: pd.DataFrame) -> pd.Series:
        n = len(g)
        gross = g["gross"].to_numpy()
        net = g["net"].to_numpy()
        active_months = g["month"].nunique()
        return pd.Series({
            "n": n,
            "active_months": active_months,
            "avg_per_active_month": n / active_months,
            "mean_gross": float(gross.mean()),
            "std_gross": float(gross.std(ddof=1)) if n > 1 else float("nan"),
            "tstat_gross": tstat(gross),
            "mean_net": float(net.mean()),
            "std_net": float(net.std(ddof=1)) if n > 1 else float("nan"),
            "tstat_net": tstat(net),
            "hit_rate": float(g["payoff"].mean()),
            "avg_price": float(g["price"].mean()),
            "avg_ttr_hours": float(g["ttr_secs"].mean() / 3600.0),
        })

    stats = df.groupby("wallet", sort=False).apply(agg, include_groups=False).reset_index()
    stats["eligible"] = (
        (stats["avg_per_active_month"] >= prm.min_avg_per_month)
        & (stats["active_months"] >= prm.min_active_months)
        & (stats["n"] > 1)
        & stats["tstat_net"].notna()
    )
    # primary ranking = net t-stat (realistic price-aware net edge, risk-adjusted)
    stats = stats.sort_values("tstat_net", ascending=False, na_position="last").reset_index(drop=True)
    return stats


def build_weekly_matrix(df: pd.DataFrame, wallets: list[str], col: str = "net") -> tuple[np.ndarray, np.ndarray]:
    """Return (sum_return, n_pos) matrices of shape (len(wallets), n_weeks), bucketed by
    resolution week, using `col` (default net return). Group return for a set S in week t is
        R_t = sum_{w in S} sum_return[w,t] / sum_{w in S} n_pos[w,t]
    i.e. equal-$-per-position return on deployed capital that week (copy-basket return)."""
    d = df[df["wallet"].isin(set(wallets))].copy()
    d["week"] = (d["resolved_at"].to_numpy() // WEEK_SECS).astype(int)
    weeks = np.sort(d["week"].unique())
    week_idx = {w: i for i, w in enumerate(weeks)}
    wal_idx = {w: i for i, w in enumerate(wallets)}
    sumr = np.zeros((len(wallets), len(weeks)))
    npos = np.zeros((len(wallets), len(weeks)))
    g = d.groupby(["wallet", "week"])[col].agg(["sum", "count"])
    for (w, wk), row in g.iterrows():
        i, j = wal_idx[w], week_idx[wk]
        sumr[i, j] = row["sum"]
        npos[i, j] = row["count"]
    return sumr, npos


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


def main() -> int:
    prm = parse_args()
    os.makedirs(prm.out_dir, exist_ok=True)
    log(f"params: {prm}")
    wallets = load_universe(prm.universe, prm.limit_wallets)
    log(f"universe: {len(wallets)} wallets")

    conn = sqlite3.connect(f"file:{prm.db}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON;")
    res, sched = load_market_maps(conn)

    df = extract_positions(conn, wallets, res, sched, prm)
    log(f"qualifying positions: {len(df):,} across {df['wallet'].nunique()} wallets")
    if df.empty:
        log("no qualifying positions; aborting")
        return 1
    pos_path = os.path.join(prm.out_dir, "qualifying_positions_72hr.csv")
    df.to_csv(pos_path, index=False)
    log(f"wrote {pos_path}")

    stats = rank_wallets(df, prm)
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
    elig_wallets = elig["wallet"].tolist()
    ranked_txt = os.path.join(prm.out_dir, "ranked_72hr_buyandhold.txt")
    with open(ranked_txt, "w") as f:
        f.write("# ranked_72hr_buyandhold — eligible wallets ranked by t-stat(net, price-aware)\n")
        f.write(f"# n_eligible={n_elig}\n")
        for w in elig_wallets:
            f.write(w + "\n")
    log(f"wrote {ranked_txt}")

    # Stage 4 — EDGE FLOOR then max group Sharpe (on net returns).
    floor = elig[(elig["tstat_net"] >= prm.floor_tstat) & (elig["mean_net"] > 0)].reset_index(drop=True)
    log(f"edge floor (net t-stat >= {prm.floor_tstat} & mean_net > 0): {len(floor)} winners")
    if len(floor) < prm.target_n:
        log(f"WARNING: only {len(floor)} wallets clear the floor (< target {prm.target_n}); "
            f"selecting all of them. Lower --floor-tstat to widen the pool.")
    floor_wallets = floor["wallet"].tolist()
    sumr, npos = build_weekly_matrix(df, floor_wallets, col="net")
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
        f.write(f"# universe={os.path.basename(prm.universe)} eligible={n_elig} "
                f"floor_pool={len(floor)} selected={len(sel_wallets)}\n")
        f.write(f"# net_group_sharpe={gs:.4f} slip={prm.slip}\n")
        for w in sel_wallets:
            f.write(w + "\n")
    log(f"wrote {out_list}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
