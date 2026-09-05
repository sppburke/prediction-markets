#!/usr/bin/env python3
"""
Price-bucket analysis: is the Winner-Follow SELECTION edge real skill, or just
favorite-buying? (Priority-1 check before any further validation work.)

The candidate universe is built by a Dune filter `win_rate > 0.95` -- a filter
that mechanically selects wallets which buy near-certain ($0.90+) outcomes. If
the strategy's positive SELECTION PnL is concentrated in high-entry-price
buckets AND the per-contract edge there vanishes once realistic cost is applied,
the "edge" is favorite-buying: high hit-rate, thin edge, fat left tail -- and it
will not survive production.

This buckets every resolved round-trip by entry price `e` (= buy fill_price,
already slippage-loaded) and reports, per bucket:
  n          round-trip count
  mean_e     mean entry price
  mean_r     mean outcome in {0,1} == realized hit rate
  gross_edge mean(r - e)            -- per-contract edge, slippage included
  net_edge   mean(r - e - e*fee)    -- after the codebase's modelled fee rate
  sum_gross  sum(r - e)             -- drives SELECTION = cbar * sum(r - e)
  %sel       share of the total positive sum_gross

Run:  python3 scripts/price_bucket_analysis.py <trades.ndjson> <report.json> <db>
        [--fee-rate 0.04] [--window all|2026|2026-04]
"""

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import holdout_baseline as hb  # noqa: E402

# Research-local sensitivity assumption only. Production economics use the compact
# CLOB fee schedule and exact venue fee function; this calculation intentionally
# remains a historical gross/net comparison. Override with --fee-rate.
DEFAULT_FEE_RATE = 0.04

BUCKET_EDGES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.85,
                0.90, 0.95, 1.0001]


def bucket_label(e):
    for lo, hi in zip(BUCKET_EDGES, BUCKET_EDGES[1:]):
        if lo <= e < hi:
            hi_disp = min(hi, 1.0)
            return f"[{lo:.2f},{hi_disp:.2f})"
    return "[1.00,+]"


def in_window(buy_dt, window):
    if window == "all":
        return True
    if window == "2026":
        return buy_dt.year == 2026
    if window == "2026-04":
        return buy_dt.year == 2026 and buy_dt.month == 4
    raise ValueError(f"unknown window {window!r}")


def analyse(records, fee_rate, window):
    resolved = [
        r for r in records
        if r["exit_kind"] != "unresolved" and in_window(r["buy_dt"], window)
    ]
    if not resolved:
        print(f"\n[{window}] no resolved round-trips in window.")
        return

    buckets = defaultdict(list)
    for rec in resolved:
        buckets[bucket_label(rec["e"])].append(rec)

    # SELECTION ties to sum(r - e); positive-share denominator is the total of
    # the positive per-bucket sums (favorite-buying => one or two top buckets
    # dominate this).
    sum_gross_by_bucket = {
        b: sum(r["r"] - r["e"] for r in recs) for b, recs in buckets.items()
    }
    total_pos = sum(s for s in sum_gross_by_bucket.values() if s > 0) or 1.0

    n_all = len(resolved)
    gross_all = sum(r["r"] - r["e"] for r in resolved)
    net_all = sum(r["r"] - r["e"] - r["e"] * fee_rate for r in resolved)

    print(f"\n=== PRICE-BUCKET SELECTION [{window}]  "
          f"(fee_rate={fee_rate:.2%}, n={n_all}) ===")
    print(f"  {'bucket':<14}{'n':>6}{'mean_e':>9}{'mean_r':>9}"
          f"{'gross_edge':>12}{'net_edge':>11}{'sum_gross':>12}{'%sel':>8}")
    print("  " + "-" * 80)
    for b in sorted(buckets):
        recs = buckets[b]
        n = len(recs)
        mean_e = sum(r["e"] for r in recs) / n
        mean_r = sum(r["r"] for r in recs) / n
        gross = sum(r["r"] - r["e"] for r in recs) / n
        net = sum(r["r"] - r["e"] - r["e"] * fee_rate for r in recs) / n
        sg = sum_gross_by_bucket[b]
        pct = 100.0 * sg / total_pos if sg > 0 else 0.0
        print(f"  {b:<14}{n:>6}{mean_e:>9.3f}{mean_r:>9.3f}"
              f"{gross:>+12.4f}{net:>+11.4f}{sg:>+12.2f}{pct:>7.1f}%")
    print("  " + "-" * 80)
    print(f"  {'TOTAL':<14}{n_all:>6}{'':>9}{'':>9}"
          f"{gross_all / n_all:>+12.4f}{net_all / n_all:>+11.4f}"
          f"{gross_all:>+12.2f}")

    # Verdict heuristic: how much of the positive edge sits at e >= 0.85, and
    # does net-of-fee edge survive there?
    hi = [r for r in resolved if r["e"] >= 0.85]
    hi_pos_share = (
        sum(s for b, s in sum_gross_by_bucket.items()
            if s > 0 and float(b.split(",")[0][1:]) >= 0.85) / total_pos
    )
    if hi:
        hi_net = sum(r["r"] - r["e"] - r["e"] * fee_rate for r in hi) / len(hi)
        print(f"\n  e>=0.85: {len(hi)}/{n_all} round-trips "
              f"({100*len(hi)/n_all:.0f}%), "
              f"{100*hi_pos_share:.0f}% of positive SELECTION sum, "
              f"net-of-fee edge {hi_net:+.4f}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("ndjson")
    p.add_argument("report")  # accepted for call-signature parity; not read here
    p.add_argument("db")
    p.add_argument("--fee-rate", type=float, default=DEFAULT_FEE_RATE)
    p.add_argument("--window", default="all",
                   choices=["all", "2026", "2026-04"])
    args = p.parse_args()

    resolutions = hb.load_resolutions(args.db)
    fills = hb.read_fills(args.ndjson)
    round_trips = hb.pair_round_trips(fills)
    records = [hb.classify_round_trip(rt, resolutions) for rt in round_trips]

    if args.window == "all":
        for w in ("all", "2026", "2026-04"):
            analyse(records, args.fee_rate, w)
    else:
        analyse(records, args.fee_rate, args.window)


if __name__ == "__main__":
    main()
