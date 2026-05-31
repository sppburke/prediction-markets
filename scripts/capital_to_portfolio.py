#!/usr/bin/env python3
"""Capital -> optimal wallet portfolio.

Input: starting capital. Output: the cohort size N (and its watchlist) that is
optimal for that capital, under the friction + notional-capacity model.

This is a pure post-processor over cohort_size_sweep.json — it does not touch the
DB. The sweep already computed, per cohort size N: walk-forward edge, Sharpe,
credibility, ex-ante quarter-Kelly fraction, and deploy-cutoff conservative
capacity ($/mo). Here we apply the capital-dependent selection rule:

  per_bet      = capital * quarter_kelly_frac(N)        # stake per copied signal
  floor_ok     = per_bet >= min_position                # can actually place bets
  monthly_vol  = capital * turnover                      # $ you cycle through/mo
  capacity_ok  = conservative_capacity(N) >= monthly_vol # small vs the leaders
  feasible     = floor_ok and capacity_ok and credible(N)

Among feasible N, pick the one maximising Sharpe (risk-adjusted), tie-break on
higher mean edge. If none feasible, report the binding constraint and the
best-effort fallback (largest-capacity credible N) with an explicit warning.

LIMIT: capacity is a notional proxy (no order-book depth). turnover is a coarse
model of capital recycling (default 4x/mo for ~7-day holds). Treat the chosen N
as a defensible optimum WITHIN this model, not a depth-aware fill guarantee.
"""
import argparse
import json
import sys
from pathlib import Path


def _key(d, n):
    return d.get(n, d.get(str(n)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, required=True, help="starting capital USD")
    ap.add_argument("--sweep-json", type=Path,
                    default=Path("data/eval-results/cohort_size_sweep.json"))
    ap.add_argument("--turnover", type=float, default=4.0,
                    help="monthly capital turns (default 4; ~7-day holds)")
    ap.add_argument("--min-position", type=float, default=5.0)
    ap.add_argument("--objective", choices=["sharpe", "edge"], default="sharpe",
                    help="risk-adjusted (sharpe, default) or raw edge")
    args = ap.parse_args()

    if not args.sweep_json.exists():
        sys.exit(f"sweep JSON not found: {args.sweep_json} (run cohort_size_sweep.py first)")
    data = json.load(open(args.sweep_json))
    agg = data["aggregate"]
    grid = data["grid"]
    monthly_vol = args.capital * args.turnover

    rows = []
    for n in grid:
        a = _key(agg, n)
        if a is None:
            continue
        qk = a["quarter_kelly_frac"]
        per_bet = args.capital * qk
        cap = a["capacity_conservative_usd_mo"]
        floor_ok = per_bet >= args.min_position
        cap_ok = cap >= monthly_vol
        feasible = floor_ok and cap_ok and a["credible"]
        rows.append(dict(
            n=n, per_bet=per_bet, capacity=cap, floor_ok=floor_ok, cap_ok=cap_ok,
            credible=a["credible"], sharpe=a["mean_sharpe"], edge=a["mean_of_mean_edge"],
            neg=a["n_anchors_negative"], feasible=feasible,
        ))

    print(f"\nCAPITAL → PORTFOLIO   capital=${args.capital:,.0f}  "
          f"monthly_volume≈${monthly_vol:,.0f} (turnover {args.turnover}x)  min_pos=${args.min_position:.0f}")
    print("-" * 96)
    print(f"{'N':>4} {'per_bet$':>9} {'capacity$/mo':>13} {'floor':>6} {'cap':>5} {'cred':>5} "
          f"{'sharpe':>7} {'edge':>9} {'FEASIBLE':>9}")
    for r in rows:
        print(f"{r['n']:>4} {r['per_bet']:>9.2f} {r['capacity']:>13,.0f} "
              f"{str(r['floor_ok']):>6} {str(r['cap_ok']):>5} {str(r['credible']):>5} "
              f"{r['sharpe']:>+7.3f} {r['edge']:>+9.4f} {str(r['feasible']):>9}")
    print("-" * 96)

    feasible = [r for r in rows if r["feasible"]]
    keyf = (lambda r: (r["sharpe"], r["edge"])) if args.objective == "sharpe" \
        else (lambda r: (r["edge"], r["sharpe"]))
    if feasible:
        best = max(feasible, key=keyf)
        wl = f"data/eval-results/watchlist-cohort-N{best['n']:03d}-portfolio_greedy.txt"
        print(f"\n✅ OPTIMAL for ${args.capital:,.0f}:  N = {best['n']} wallets")
        print(f"   per-bet stake : ${best['per_bet']:.2f}  (¼-Kelly; ≥ ${args.min_position:.0f} floor)")
        print(f"   capacity room : ${best['capacity']:,.0f}/mo conservative  (your vol ≈ ${monthly_vol:,.0f})")
        print(f"   walk-forward  : Sharpe {best['sharpe']:+.3f}, mean edge {best['edge']:+.4f}, "
              f"{best['neg']} negative anchors")
        print(f"   watchlist     : {wl}")
    else:
        # report binding constraint + best-effort fallback
        cred = [r for r in rows if r["credible"]]
        floor_fail = all(not r["floor_ok"] for r in rows)
        cap_fail = all(not r["cap_ok"] for r in cred) if cred else False
        print(f"\n⚠️  No cohort size fully feasible at ${args.capital:,.0f}.")
        if floor_fail:
            print("   binding: per-bet < $5 floor at every N — capital too small for ¼-Kelly. "
                  "Either raise capital or accept flat $5/bet (above ¼-Kelly).")
        if cap_fail:
            print(f"   binding: conservative capacity < your ${monthly_vol:,.0f}/mo volume at every credible N. "
                  "Lower per-bet, lower turnover, or accept higher price-impact.")
        if cred:
            fb = max(cred, key=lambda r: r["capacity"])
            wl = f"data/eval-results/watchlist-cohort-N{fb['n']:03d}-portfolio_greedy.txt"
            print(f"   best-effort   : N={fb['n']} (max capacity ${fb['capacity']:,.0f}/mo), "
                  f"Sharpe {fb['sharpe']:+.3f}; watchlist {wl}")
            print(f"   NOTE: at this N your volume exceeds conservative capacity — expect edge erosion from impact.")


if __name__ == "__main__":
    main()
