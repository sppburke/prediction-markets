#!/usr/bin/env python3
"""
PnL decomposition: where does the backtest's money actually come from?

Tests #1 and #2 showed entry-selection edge is net-negative after costs, yet the
backtest shows large positive PnL. This script splits the backtest's realized PnL
into three EXACT, ADDITIVE components so we can see which one carries the result.

Per closed position i:
  e_i = VWAP entry (buy fill_price)
  c_i = contracts (actual backtest size)
  r_i = resolution outcome in {0,1}  (from cache market_resolutions - authoritative)
  x_i = exit price: sell fill_price if sold early, else r_i (held to resolution)

  Total PnL = Σ c_i (x_i - e_i)
            = Σ c_i (r_i - e_i)                          [hold-to-resolution @ actual size]
              + Σ c_i (x_i - r_i)                        [EXIT-TIMING]

  Σ c_i (r_i - e_i)
            = (C/N) Σ (r_i - e_i)                        [SELECTION: equal-weight, hold-to-res]
              + Σ (c_i - C/N)(r_i - e_i)                 [SIZING: cov(size, per-contract edge)]

So:  Total PnL  =  SELECTION  +  SIZING  +  EXIT-TIMING   (exact, additive)

  SELECTION  : pure entry quality - equal-weight, held to resolution
  SIZING     : extra PnL from putting more contracts on better-edge trades
  EXIT-TIMING: extra PnL from following the leader's early sell vs holding

Run: .venv-analysis/bin/python scripts/pnl_decomposition.py <ndjson> <report> <db>

Example:
  .venv-analysis/bin/python scripts/pnl_decomposition.py \\
    /path/to/trades.ndjson /path/to/report.json /path/to/wallet_cache.db
"""

import argparse
import json
import sqlite3
import statistics
from collections import defaultdict


def parse_args():
    p = argparse.ArgumentParser(description="Three-way PnL decomposition for a backtest run.")
    p.add_argument("ndjson", help="Path to trades.ndjson from the backtest run")
    p.add_argument("report", help="Path to report.json from the backtest run")
    p.add_argument("db", help="Path to wallet_cache.db")
    return p.parse_args()


def main():
    args = parse_args()
    NDJSON = args.ndjson
    REPORT = args.report
    DB = args.db

    # ── load fills, group by position key (leader, market, outcome) ────────
    buys = defaultdict(list)    # key -> [(contracts, fill_price, simulated_at)]
    sells = defaultdict(list)   # key -> [(contracts, fill_price, simulated_at)]
    resolutions = defaultdict(list)  # key -> [(contracts, close_price)]
    for line in open(NDJSON):
        t = json.loads(line)
        key = (t["leader_wallet"], t["market_id"], t["outcome_id"])
        c = float(t["contracts"])
        fp = float(t["fill_price"])
        if t["side"] == "buy":
            buys[key].append((c, fp, t["simulated_at"]))
        elif t["side"] == "sell":
            sells[key].append((c, fp, t["simulated_at"]))
        elif t["side"] == "resolution":
            resolutions[key].append((c, fp))

    # ── authoritative resolution outcomes from the cache ───────────────────
    con = sqlite3.connect(DB)
    res = {m: w for m, w in con.execute(
        "SELECT market_id, winning_outcome_id FROM market_resolutions")}
    con.close()

    # ── build closed positions ─────────────────────────────────────────────
    positions = []   # dict per position
    skipped = 0
    for key, blist in buys.items():
        leader, market, outcome = key
        c_buy = sum(b[0] for b in blist)
        if c_buy <= 0:
            skipped += 1
            continue
        e = sum(b[0] * b[1] for b in blist) / c_buy        # VWAP entry
        entry_dt = min(b[2] for b in blist)

        win = res.get(market)
        if win is None:
            skipped += 1                                    # unresolved / voided
            continue
        r = 1.0 if win == outcome else 0.0

        has_sell = key in sells
        if has_sell:
            c_sell = sum(s[0] for s in sells[key])
            x = sum(s[0] * s[1] for s in sells[key]) / c_sell   # VWAP exit
            c = c_sell                                           # size that was actually round-tripped
            exit_kind = "sold"
        else:
            # held to resolution
            c = c_buy
            x = r
            exit_kind = "held"

        positions.append({
            "leader": leader, "market": market, "outcome": outcome,
            "e": e, "c": c, "r": r, "x": x, "exit": exit_kind,
            "entry_dt": entry_dt,
        })

    N = len(positions)
    C = sum(p["c"] for p in positions)
    cbar = C / N
    print(f"Closed positions: {N}   (skipped {skipped} unresolved/zero)   "
          f"total contracts {C:,.0f}   mean size {cbar:,.1f}\n")

    # ── exact additive decomposition ───────────────────────────────────────
    total      = sum(p["c"] * (p["x"] - p["e"]) for p in positions)
    hold_res   = sum(p["c"] * (p["r"] - p["e"]) for p in positions)
    exit_time  = sum(p["c"] * (p["x"] - p["r"]) for p in positions)
    selection  = cbar * sum((p["r"] - p["e"]) for p in positions)
    sizing     = sum((p["c"] - cbar) * (p["r"] - p["e"]) for p in positions)

    print("=== EXACT ADDITIVE DECOMPOSITION ===\n")
    print(f"  {'SELECTION   (equal-weight, hold-to-resolution)':<48} {selection:>+15,.0f}")
    print(f"  {'SIZING      (cov of size and per-contract edge)':<48} {sizing:>+15,.0f}")
    print(f"  {'EXIT-TIMING (following leader sells vs holding)':<48} {exit_time:>+15,.0f}")
    print(f"  {'-'*48} {'-'*15}")
    print(f"  {'TOTAL realized PnL (from fills)':<48} {total:>+15,.0f}")

    # reconcile vs report.json
    rep = json.load(open(REPORT))
    rep_pnl = float(rep["total_pnl_usd"])
    print(f"\n  report.json total_pnl_usd: {rep_pnl:>+15,.0f}")
    print(f"  decomposition residual:    {total - rep_pnl:>+15,.0f}  "
          f"({100*(total-rep_pnl)/rep_pnl:+.2f}% — open-at-horizon positions + rounding)")

    print(f"\n  share of total:  SELECTION {100*selection/total:+6.1f}%   "
          f"SIZING {100*sizing/total:+6.1f}%   EXIT-TIMING {100*exit_time/total:+6.1f}%")

    # ── sub-analyses ───────────────────────────────────────────────────────
    print("\n=== SELECTION detail (pure entry quality) ===")
    edges = [p["r"] - p["e"] for p in positions]
    print(f"  mean per-contract gross edge (r - e): {statistics.fmean(edges):+.4f}")
    print(f"  positions with positive edge: {sum(1 for x in edges if x>0)}/{N} "
          f"({100*sum(1 for x in edges if x>0)/N:.0f}%)")
    # net of the 5% fee+slippage drag (what test #1/#2 measured)
    net_edges = [p["r"] - p["e"]*1.05 for p in positions]
    print(f"  mean per-contract NET edge (r - 1.05e): {statistics.fmean(net_edges):+.4f}")
    sel_net = cbar * sum(net_edges)
    print(f"  SELECTION net-of-fee (equal-weight):  {sel_net:>+15,.0f}")

    print("\n=== SIZING detail ===")
    # correlation between size and per-contract edge
    cs = [p["c"] for p in positions]
    if statistics.pstdev(cs) > 0 and statistics.pstdev(edges) > 0:
        mc, me = statistics.fmean(cs), statistics.fmean(edges)
        cov = sum((cs[i]-mc)*(edges[i]-me) for i in range(N)) / N
        corr = cov / (statistics.pstdev(cs) * statistics.pstdev(edges))
        print(f"  corr(size, per-contract edge): {corr:+.4f}")
    print(f"  -> {'positive: bigger bets ON better trades' if sizing>0 else 'negative: bigger bets on WORSE trades'}")

    print("\n=== EXIT-TIMING detail ===")
    sold = [p for p in positions if p["exit"] == "sold"]
    held = [p for p in positions if p["exit"] == "held"]
    print(f"  positions sold early: {len(sold)}   held to resolution: {len(held)}")
    if sold:
        sold_contrib = sum(p["c"] * (p["x"] - p["r"]) for p in sold)
        # how often did the early sell beat what holding would have given?
        better = sum(1 for p in sold if p["x"] > p["r"])
        print(f"  exit-timing PnL from sold positions: {sold_contrib:+,.0f}")
        print(f"  sells that beat holding: {better}/{len(sold)} "
              f"({100*better/len(sold):.0f}%)")
        print(f"  mean (x - r) on sold positions: "
              f"{statistics.fmean([p['x']-p['r'] for p in sold]):+.4f}")

    # ── decomposition by year ──────────────────────────────────────────────
    print("\n=== DECOMPOSITION BY YEAR (entry year) ===")
    by_year = defaultdict(list)
    for p in positions:
        by_year[p["entry_dt"][:4]].append(p)
    print(f"  {'year':<6}{'N':>6}{'SELECTION':>14}{'SIZING':>14}{'EXIT':>14}{'TOTAL':>14}")
    for y in sorted(by_year):
        ps = by_year[y]
        n = len(ps); cb = sum(q["c"] for q in ps)/n
        sel = cb * sum(q["r"]-q["e"] for q in ps)
        siz = sum((q["c"]-cb)*(q["r"]-q["e"]) for q in ps)
        ext = sum(q["c"]*(q["x"]-q["r"]) for q in ps)
        print(f"  {y:<6}{n:>6}{sel:>+14,.0f}{siz:>+14,.0f}{ext:>+14,.0f}{sel+siz+ext:>+14,.0f}")

    print("\nVERDICT GUIDE:")
    print("  If SELECTION (esp. net-of-fee) is ~0 or negative and SIZING/EXIT carry the")
    print("  total, the strategy 'works' but NOT via 'copy what winners buy' — the edge")
    print("  is in sizing and/or exit-timing, which are different strategies needing")
    print("  their own validation.")


if __name__ == "__main__":
    main()
