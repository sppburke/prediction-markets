#!/usr/bin/env python3
"""Pass 2: latency-shifted re-rank of the 72h buy-and-hold edge-floor candidates.

The pass-1 ranker (`rank_72hr_buyandhold.py`) scores each first-buy at the
LEADER's entry price. But when we copy, we observe the leader's trade ~Δ seconds late
(/activity indexes in ~1-4s + 5-10s poll + ~5s fill ≈ 10-20s; measured docs/29) and
enter at whatever the market is then. Near resolution the price has moved toward the
outcome, so our captured edge < the leader's. This pass re-prices every candidate
position at the fill we'd ACTUALLY get — the first trade in the same (market, outcome)
at `entry_ts + Δ`, before resolution — and re-ranks on that. Positions with no such
trade are dropped (illiquid / no book to copy into) and counted against the wallet's
fill-rate. Honest "reliable copy" selection: a wallet survives only if its edge holds
at our real entry AND we can actually fill it.

Two-pass is sound because latency-shifted edge <= leader edge, so the pass-1
leader-price edge floor is a valid (generous) candidate superset.

Inputs: pass-1 `ranked_72hr_buyandhold.csv` (per-wallet, picks candidates) +
`qualifying_positions_72hr.csv` (must include `outcome_id`) + the trade cache (as-of
fills). Output: `latency_shift_ranked.csv` + `latency_shift_basket.txt`.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import math
import os
import sqlite3
import statistics
import sys
import time

import ranker_duck
from ranker_decay import (
    DEFAULT_HALF_LIFE_DAYS,
    decay_weights,
    parse_as_of,
    weighted_stats,
)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/wallet_cache.db")
    p.add_argument("--ranked-csv", required=True, help="pass-1 ranked_72hr_buyandhold.csv")
    p.add_argument("--positions-csv", required=True,
                   help="pass-1 qualifying_positions_72hr.csv (must include outcome_id)")
    p.add_argument("--out-dir", default="data/eval-results")
    p.add_argument("--latency-shift-secs", type=float, default=20.0,
                   help="Δ: re-price at the first same-(market,outcome) trade at >= entry+Δ "
                        "(default 20s ~ p95 end-to-end copy latency, docs/29)")
    p.add_argument("--fill-window-secs", type=float, default=120.0,
                   help="the next same-(market,outcome) trade must fall within entry+Δ .. "
                        "entry+Δ+this; later => UNFILLABLE (illiquid, no realistic copy). "
                        "Prevents stale fills (minutes/days later, near resolution) from "
                        "inflating edge/fill-rate. 0 = no cap.")
    p.add_argument("--slip-cents", type=float, default=1.0,
                   help="entry slippage in cents on the latency-shifted fill price")
    p.add_argument("--half-life-days", type=float, default=DEFAULT_HALF_LIFE_DAYS,
                   help="exponential recency-decay half-life in days for the net edge/t-stat "
                        "(a fill one half-life old weighs 0.5). <= 0 disables decay (flat = legacy). "
                        "fill_rate / hit_rate / activity gates stay raw. Same weight as pass-1.")
    p.add_argument("--as-of", default=None,
                   help="decay age anchor (ISO date/datetime or unix epoch). Default: max filled entry_ts.")
    p.add_argument("--floor-tstat", type=float, default=2.0,
                   help="pass-1 candidate floor AND pass-2 survival floor on net t-stat")
    p.add_argument("--min-fill-rate", type=float, default=0.5,
                   help="drop wallets whose fillable fraction is below this")
    p.add_argument("--min-active-months", type=int, default=3)
    p.add_argument("--min-avg-per-month", type=float, default=20.0)
    p.add_argument("--target-n", type=int, default=25)
    return p.parse_args()


def load_candidates(ranked_csv: str, floor_tstat: float) -> set[str]:
    """Edge-floor candidates from pass 1: eligible & tstat_net>=floor & mean_net>0."""
    out: set[str] = set()
    with open(ranked_csv, newline="") as f:
        for r in csv.DictReader(f):
            try:
                if (r.get("eligible", "").strip().lower() in ("true", "1")
                        and float(r["tstat_net"]) >= floor_tstat
                        and float(r["mean_net"]) > 0):
                    out.add(r["wallet"])
            except (ValueError, KeyError):
                continue
    return out


def main() -> int:
    a = parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    slip = a.slip_cents / 100.0
    cand = load_candidates(a.ranked_csv, a.floor_tstat)
    log(f"pass-1 edge-floor candidates: {len(cand)} wallets")
    if not cand:
        log("no candidates; nothing to re-rank")
        return 1

    # Group candidate positions by (market, outcome) so each tape is loaded once.
    by_mo: dict[tuple[str, str], list[dict]] = {}
    npos = 0
    with open(a.positions_csv, newline="") as f:
        rd = csv.DictReader(f)
        if "outcome_id" not in rd.fieldnames:
            log("FATAL: positions CSV lacks outcome_id — re-run pass 1 with the "
                "rank_72hr_buyandhold.py ranker (it writes outcome_id).")
            return 1
        for r in rd:
            w = r["wallet"]
            if w not in cand:
                continue
            key = (r["market_id"], r["outcome_id"])
            by_mo.setdefault(key, []).append({
                "wallet": w,
                "entry_ts": int(r["entry_ts"]),
                "resolved_at": int(r["resolved_at"]),
                "payoff": float(r["payoff"]),
                "leader_price": float(r["price"]),
            })
            npos += 1
    log(f"candidate positions: {npos} across {len(by_mo)} (market,outcome) pairs")

    conn = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000;")

    # Per-wallet accumulators
    net_ls: dict[str, list[float]] = {}     # latency-shifted net per filled position
    entry_ts_ls: dict[str, list[int]] = {}  # entry_ts per FILLED position (parallel to net_ls) -> decay weight
    months: dict[str, set] = {}             # active months among FILLED positions
    n_total: dict[str, int] = {}
    n_filled: dict[str, int] = {}
    payoffs: dict[str, list[float]] = {}    # payoff (1/0) per FILLED position -> hit_rate (Kelly p)
    fill_delays: list[float] = []
    shift = a.latency_shift_secs
    fill_window = a.fill_window_secs

    # DuckDB read-layer (#375): load every candidate (market,outcome) tape from the
    # Parquet snapshot in one query (price returned as RAW strings, so the float()/
    # 0<p<1 fill logic below is byte-identical); else per-pair SQLite scan. The
    # bisect + fill loop is unchanged regardless of engine.
    engine = ranker_duck.get_engine()
    if engine is not None:
        log("tape engine: DuckDB (Parquet read-layer, #375)")
        duck_tapes = ranker_duck.duck_load_tapes(engine, list(by_mo.keys()))
    else:
        log("tape engine: SQLite (per-(market,outcome) scan)")
        duck_tapes = None

    for i, ((mid, oid), positions) in enumerate(by_mo.items()):
        if duck_tapes is not None:
            ts_arr, px_arr = duck_tapes.get((mid, oid), ([], []))
        else:
            cur = conn.execute(
                "SELECT timestamp_unix, price_str FROM trades "
                "WHERE market_id = ? AND outcome_id = ? ORDER BY timestamp_unix ASC",
                (mid, oid),
            )
            tape = cur.fetchall()
            ts_arr = [row[0] for row in tape]
            px_arr = [row[1] for row in tape]
        for pos in positions:
            w = pos["wallet"]
            n_total[w] = n_total.get(w, 0) + 1
            target = pos["entry_ts"] + shift
            idx = bisect.bisect_left(ts_arr, target)
            filled = False
            within_window = fill_window <= 0 or (idx < len(ts_arr) and ts_arr[idx] <= target + fill_window)
            if idx < len(ts_arr) and ts_arr[idx] < pos["resolved_at"] and within_window:
                try:
                    fill_price = float(px_arr[idx])
                except (TypeError, ValueError):
                    fill_price = None
                if fill_price is not None and 0.0 < fill_price < 1.0:
                    eff = min(fill_price + slip, 0.999)
                    net = (pos["payoff"] - eff) / eff
                    net_ls.setdefault(w, []).append(net)
                    entry_ts_ls.setdefault(w, []).append(pos["entry_ts"])
                    g = time.gmtime(pos["entry_ts"])
                    months.setdefault(w, set()).add((g.tm_year, g.tm_mon))
                    n_filled[w] = n_filled.get(w, 0) + 1
                    payoffs.setdefault(w, []).append(pos["payoff"])
                    fill_delays.append(ts_arr[idx] - target)
                    filled = True
            if not filled:
                pass  # unfillable: no same-outcome trade between entry+Δ and resolution
        if (i + 1) % 2000 == 0:
            log(f"  {i+1}/{len(by_mo)} market-outcomes processed")

    # Shared decay anchor: explicit --as-of (parsed identically to pass-1), else the
    # latest filled entry, so every wallet decays against one anchor. half_life <= 0
    # -> flat weights -> weighted_stats matches the legacy stdlib t-stat after this pass's
    # 4dp/6dp output rounding (the flat short-circuit is exactly np.mean/np.std(ddof=1),
    # which differs from the old statistics.stdev only at sub-ULP, absorbed by the rounding).
    as_of = parse_as_of(a.as_of)
    if as_of is None:
        all_entry = [t for lst in entry_ts_ls.values() for t in lst]
        as_of = max(all_entry) if all_entry else 0
    decay = "flat (no decay)" if a.half_life_days <= 0 else f"half_life={a.half_life_days}d"
    log(f"scoring: {decay}, as_of={as_of}")

    # Per-wallet latency-shifted stats + survival gate
    rows = []
    for w in n_total:
        nf = n_filled.get(w, 0)
        nets = net_ls.get(w, [])
        ets = entry_ts_ls.get(w, [])
        am = len(months.get(w, set()))
        fr = nf / n_total[w] if n_total[w] else 0.0
        mean, _, n_eff, t = weighted_stats(nets, decay_weights(ets, as_of, a.half_life_days))
        hr = statistics.fmean(payoffs.get(w, [])) if payoffs.get(w) else float("nan")
        eligible = (
            nf > 1 and am >= a.min_active_months
            and (nf / am if am else 0) >= a.min_avg_per_month
            and fr >= a.min_fill_rate
            and not math.isnan(t) and t >= a.floor_tstat and mean > 0
        )
        rows.append({
            "wallet": w, "n_total": n_total[w], "n_filled": nf,
            "fill_rate": round(fr, 4), "active_months": am,
            "mean_net_ls": round(mean, 6) if not math.isnan(mean) else "",
            "tstat_net_ls": round(t, 4) if not math.isnan(t) else "",
            "n_eff": round(n_eff, 4),
            "hit_rate": round(hr, 4) if not math.isnan(hr) else "",
            "survives": eligible,
        })
    rows.sort(key=lambda r: (r["survives"], r["tstat_net_ls"] if r["tstat_net_ls"] != "" else -9),
              reverse=True)

    ranked_path = os.path.join(a.out_dir, "latency_shift_ranked.csv")
    # Static fieldnames: never index rows[0] (empty when candidates had no positions
    # overlapping the positions CSV — still write a header-only file, don't crash).
    fields = ["wallet", "n_total", "n_filled", "fill_rate", "active_months",
              "mean_net_ls", "tstat_net_ls", "n_eff", "hit_rate", "survives"]
    with open(ranked_path, "w", newline="") as f:
        wcsv = csv.DictWriter(f, fieldnames=fields)
        wcsv.writeheader()
        wcsv.writerows(rows)
    if not rows:
        log("no candidate positions overlapped the positions CSV — wrote empty ranking")
        return 0
    survivors = [r for r in rows if r["survives"]]
    log(f"latency-shifted survivors (fill_rate>={a.min_fill_rate}, t>={a.floor_tstat}, "
        f"mean>0, >={a.min_active_months}mo, >={a.min_avg_per_month}/mo): {len(survivors)}")
    basket = survivors[: a.target_n]
    basket_path = os.path.join(a.out_dir, "latency_shift_basket.txt")
    with open(basket_path, "w") as f:
        f.write(f"# latency_shift_basket — Δ={shift}s slip={slip} floor_t={a.floor_tstat} "
                f"half_life={a.half_life_days}d min_fill_rate={a.min_fill_rate} "
                f"candidates={len(cand)} survivors={len(survivors)}\n")
        for r in basket:
            f.write(r["wallet"] + "\n")
    if fill_delays:
        fd = sorted(fill_delays)
        log(f"fill-delay (s past entry+Δ): p50={fd[len(fd)//2]:.0f} "
            f"p90={fd[int(len(fd)*0.9)]:.0f} max={fd[-1]:.0f}")
    if basket:
        log(f"basket={len(basket)}  mean fill_rate={statistics.fmean([r['fill_rate'] for r in basket]):.3f}  "
            f"mean tstat_ls={statistics.fmean([r['tstat_net_ls'] for r in basket]):.2f}  "
            f"mean meanNet_ls={statistics.fmean([r['mean_net_ls'] for r in basket]):+.4f}")
    log(f"wrote {ranked_path}  and  {basket_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
