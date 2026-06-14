#!/usr/bin/env python3
"""Memory-safe streaming variant of rank_72hr_buyandhold.py.

Identical semantics to rank_72hr_buyandhold.py (first-buy-per-market, <72h TTR,
price band, hold-to-resolution net edge, rank by net t-stat, edge floor + greedy
max-group-Sharpe) but does NOT materialise all qualifying positions in a single
pandas DataFrame. Instead it:

  * computes each wallet's summary stats inline during the per-wallet scan,
  * streams every qualifying position to qualifying_positions_72hr.csv on disk,
  * retains compact (net, resolved_at) arrays in RAM ONLY for wallets that clear
    the eligibility + edge-floor test (a few hundred), which is all the
    group-Sharpe stage needs.

This keeps peak RAM ~= market maps + per-wallet summaries (~1 GB) instead of the
~14 GB the all-positions DataFrame would need over the full 128 K active universe
on the 359 GB cache. Pure helpers (arg parsing, universe/market loading,
group_sharpe, greedy) are imported from the original so the math cannot drift.
"""
from __future__ import annotations

import csv
import math
import os
import sqlite3
import sys
import time

import numpy as np
import pandas as pd

# Reuse the canonical helpers so semantics stay identical to the reference script.
from rank_72hr_buyandhold import (  # noqa: E402
    WEEK_SECS,
    greedy_max_group_sharpe,
    group_sharpe,
    load_market_maps,
    load_universe,
    parse_args,
)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _tstat(arr: np.ndarray) -> float:
    n = len(arr)
    if n <= 1:
        return float("nan")
    sd = float(arr.std(ddof=1))
    m = float(arr.mean())
    return (m / sd * math.sqrt(n)) if sd > 0 else float("nan")


def main() -> int:
    prm = parse_args()
    os.makedirs(prm.out_dir, exist_ok=True)
    log(f"params: {prm}")
    wallets = load_universe(prm.universe, prm.limit_wallets)
    log(f"universe: {len(wallets)} wallets")

    conn = sqlite3.connect(f"file:{prm.db}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON;")
    res, sched = load_market_maps(conn)

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

    for i, w in enumerate(wallets):
        diag["wallets_seen"] += 1
        cur = conn.execute(
            "SELECT market_id, outcome_id, price_str, contracts, timestamp_unix "
            "FROM trades WHERE wallet_hex = ? AND side = 'buy' ORDER BY timestamp_unix ASC",
            (w,),
        )
        first: dict = {}
        for mid, oid, price_str, contracts, ts in cur:
            if mid not in first:
                first[mid] = (oid, price_str, contracts, ts)
        diag["first_buys"] += len(first)

        nets: list[float] = []
        grosses: list[float] = []
        payoffs: list[float] = []
        prices: list[float] = []
        ttrs: list[int] = []
        resolveds: list[int] = []
        months: set[tuple[int, int]] = set()
        rows_out: list[tuple] = []

        for mid, (oid, price_str, contracts, ts) in first.items():
            if not (prm.win_start <= ts < prm.win_end):
                diag["out_of_window"] += 1
                continue
            end = sched.get(mid)
            r = res.get(mid)
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
            if not (prm.price_min <= price <= prm.price_max):
                diag["out_of_band"] += 1
                continue
            win_oid, resolved_at = r
            payoff = 1.0 if int(oid) == int(win_oid) else 0.0
            gross = (payoff - price) / price
            eff = min(price + prm.slip, 0.999)
            net = (payoff - eff) / eff

            nets.append(net)
            grosses.append(gross)
            payoffs.append(payoff)
            prices.append(price)
            ttrs.append(ttr)
            resolveds.append(resolved_at)
            g = time.gmtime(ts)
            months.add((g.tm_year, g.tm_mon))
            rows_out.append((w, mid, oid, ts, ttr, price, int(contracts), payoff,
                             gross, net, resolved_at))
            diag["qualified"] += 1

        n = len(nets)
        if n == 0:
            if (i + 1) % 5000 == 0:
                log(f"  {i+1}/{len(wallets)} wallets ({time.time()-t0:.0f}s, "
                    f"{total_qualified:,} qualifying, {len(floor_pos)} floor)")
            continue

        writer.writerows(rows_out)
        total_qualified += n

        net_arr = np.asarray(nets, dtype=float)
        gross_arr = np.asarray(grosses, dtype=float)
        active_months = len(months)
        mean_net = float(net_arr.mean())
        std_net = float(net_arr.std(ddof=1)) if n > 1 else float("nan")
        tstat_net = _tstat(net_arr)
        summaries.append({
            "wallet": w,
            "n": n,
            "active_months": active_months,
            "avg_per_active_month": n / active_months,
            "mean_gross": float(gross_arr.mean()),
            "std_gross": float(gross_arr.std(ddof=1)) if n > 1 else float("nan"),
            "tstat_gross": _tstat(gross_arr),
            "mean_net": mean_net,
            "std_net": std_net,
            "tstat_net": tstat_net,
            "hit_rate": float(np.asarray(payoffs).mean()),
            "avg_price": float(np.asarray(prices).mean()),
            "avg_ttr_hours": float(np.asarray(ttrs, dtype=float).mean() / 3600.0),
        })

        eligible = (
            (n / active_months >= prm.min_avg_per_month)
            and (active_months >= prm.min_active_months)
            and (n > 1)
            and not math.isnan(tstat_net)
        )
        if eligible and (tstat_net >= prm.floor_tstat) and (mean_net > 0):
            floor_pos[w] = (net_arr, np.asarray(resolveds, dtype=np.int64))

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

    ranked_txt = os.path.join(prm.out_dir, "ranked_72hr_buyandhold.txt")
    with open(ranked_txt, "w") as f:
        f.write("# ranked_72hr_buyandhold — eligible wallets ranked by t-stat(net, price-aware)\n")
        f.write(f"# n_eligible={n_elig}\n")
        for wlt in elig["wallet"].tolist():
            f.write(wlt + "\n")
    log(f"wrote {ranked_txt}")

    floor = elig[(elig["tstat_net"] >= prm.floor_tstat) & (elig["mean_net"] > 0)].reset_index(drop=True)
    log(f"edge floor (net t-stat >= {prm.floor_tstat} & mean_net > 0): {len(floor)} winners")
    if len(floor) < prm.target_n:
        log(f"WARNING: only {len(floor)} wallets clear the floor (< target {prm.target_n}); "
            f"selecting all of them. Lower --floor-tstat to widen the pool.")
    floor_wallets = floor["wallet"].tolist()
    if not floor_wallets:
        log("no wallets clear the edge floor; stopping")
        return 0

    # Weekly (sum, count) matrices of net return per (wallet, resolution-week),
    # mirroring build_weekly_matrix but sourced from retained per-wallet arrays.
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
        f.write(f"# universe={os.path.basename(prm.universe)} eligible={n_elig} "
                f"floor_pool={len(floor)} selected={len(sel_wallets)}\n")
        f.write(f"# net_group_sharpe={gs:.4f} slip={prm.slip}\n")
        for w in sel_wallets:
            f.write(w + "\n")
    log(f"wrote {out_list}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
