#!/usr/bin/env python3
"""Post-selection trade-activity analysis for a production watchlist.

Answers three questions for a generated watchlist (one wallet_hex per line):
  1) Expected trades/day for the top {50,100,200,300,400,500} wallets — the
     daily copy-trade throughput you'd execute mirroring that prefix.
  2) Histogram buckets of per-wallet trades/day across the whole list.
  3) Capital-fragmentation curve for a $1,000 bankroll: concurrent open
     positions vs per-position capital as the follow-count N grows, used to
     pick the largest N whose per-position capital stays above a viability
     floor (fees + the 500 bps haircut make sub-$X positions uneconomic).

"trades/day" is measured over a trailing window (default 30d) ending at the
scoring cutoff, so it reflects *recent* activity — a wallet dormant in that
window scores 0 and is flagged (directly addresses the dormancy concern).

Top-N analysis assumes the watchlist file is in RANK order (true for the
`*_single` strategies, which write `nlargest` order; intersection strategies
write set order — top-N is then arbitrary and the script warns).

Usage:
  python3 scripts/watchlist_trade_analysis.py --db-path data/wallet_cache.db \
      --watchlist data/watchlist-...-gbm_throughput_single.txt \
      [--cutoff-unix 1779839999] [--window-days 30] [--min-position-usd 5] \
      [--per-wallet-out /tmp/wl_perwallet.tsv]
"""
import argparse
import sqlite3
import statistics
import sys
from pathlib import Path

PREFIXES = [50, 100, 200, 300, 400, 500]
BUCKETS = [
    ("dormant (0)", lambda x: x == 0),
    ("(0, 0.1]", lambda x: 0 < x <= 0.1),
    ("(0.1, 0.5]", lambda x: 0.1 < x <= 0.5),
    ("(0.5, 1]", lambda x: 0.5 < x <= 1),
    ("(1, 2]", lambda x: 1 < x <= 2),
    ("(2, 5]", lambda x: 2 < x <= 5),
    ("(5, 10]", lambda x: 5 < x <= 10),
    (">10", lambda x: x > 10),
]


def latest_cutoff(con):
    return con.execute("SELECT MAX(cutoff_unix) FROM wallet_features").fetchone()[0]


def load_watchlist(path):
    return [ln.strip() for ln in Path(path).read_text().splitlines() if ln.strip()]


def per_wallet_stats(con, wallets, cutoff_unix, window_days):
    """Return {hex: {tpd, avg_pos_usd, hold_days, n_trades_window}} via a temp
    table join (avoids the SQLite bound-parameter limit on a 5k IN-list)."""
    win_start = cutoff_unix - window_days * 86400
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _wl (wallet_hex TEXT PRIMARY KEY)")
    con.execute("DELETE FROM _wl")
    con.executemany("INSERT OR IGNORE INTO _wl VALUES (?)", [(w,) for w in wallets])

    # Recent-window trade counts + avg notional (buys), per wallet.
    rows = con.execute(
        """
        SELECT t.wallet_hex,
               COUNT(*) AS n,
               AVG(CASE WHEN t.side='buy'
                        THEN CAST(t.price_str AS REAL) * t.contracts END) AS avg_buy_usd
        FROM trades t JOIN _wl w ON w.wallet_hex = t.wallet_hex
        WHERE t.timestamp_unix > ? AND t.timestamp_unix <= ?
        GROUP BY t.wallet_hex
        """,
        (win_start, cutoff_unix),
    ).fetchall()
    win = {r[0]: (r[1], r[2]) for r in rows}

    # avg_hold_secs from features at the cutoff (concurrency model input).
    hold = dict(
        con.execute(
            "SELECT wallet_hex, avg_hold_secs FROM wallet_features WHERE cutoff_unix=?",
            (cutoff_unix,),
        ).fetchall()
    )

    out = {}
    for w in wallets:
        n, avg_usd = win.get(w, (0, None))
        hs = hold.get(w, 0) or 0
        out[w] = {
            "tpd": n / window_days,
            "avg_pos_usd": avg_usd if avg_usd else 0.0,
            "hold_days": hs / 86400.0,
            "n_trades_window": n,
        }
    return out


def q1_prefixes(wallets, stats):
    print("\n=== Q1: trades/day by ranked prefix ===")
    print(f"{'top-N':>7} {'sum tpd':>9} {'mean tpd':>9} {'median':>8} {'active%':>8}")
    for n in PREFIXES:
        if n > len(wallets):
            continue
        sub = [stats[w]["tpd"] for w in wallets[:n]]
        active = sum(1 for x in sub if x > 0)
        print(f"{n:>7} {sum(sub):>9.1f} {statistics.mean(sub):>9.3f} "
              f"{statistics.median(sub):>8.3f} {100*active/len(sub):>7.1f}%")


def q2_buckets(wallets, stats):
    print("\n=== Q2: trades/day distribution (whole list) ===")
    tpds = [stats[w]["tpd"] for w in wallets]
    n = len(tpds)
    print(f"  total wallets: {n}")
    for name, pred in BUCKETS:
        c = sum(1 for x in tpds if pred(x))
        bar = "#" * round(40 * c / n) if n else ""
        print(f"  {name:>12}: {c:>5} ({100*c/n:>5.1f}%) {bar}")


def q3_capital(wallets, stats, bankroll, min_pos_usd):
    """Little's law: concurrent open positions L = sum_i(tpd_i * hold_days_i).
    Per-position capital = bankroll / L_cumulative. Largest N whose per-position
    capital >= min_pos_usd is the capital-feasible follow count."""
    print(f"\n=== Q3: $%.0f bankroll — capital fragmentation ===" % bankroll)
    print(f"  (min viable position = ${min_pos_usd:.0f}; L = concurrent open positions)")
    print(f"{'top-N':>7} {'cum L':>9} {'$/position':>11} {'feasible':>9}")
    cumL = 0.0
    best_feasible = 0
    for i, w in enumerate(wallets, 1):
        cumL += stats[w]["tpd"] * stats[w]["hold_days"]
        if i in PREFIXES or i == len(wallets):
            per = bankroll / cumL if cumL > 0 else float("inf")
            feas = "yes" if per >= min_pos_usd else "NO"
            print(f"{i:>7} {cumL:>9.1f} {per:>11.2f} {feas:>9}")
    # find exact crossover
    cumL = 0.0
    for i, w in enumerate(wallets, 1):
        cumL += stats[w]["tpd"] * stats[w]["hold_days"]
        if cumL > 0 and bankroll / cumL >= min_pos_usd:
            best_feasible = i
    print(f"  → largest capital-feasible follow count: {best_feasible} wallets "
          f"(~{bankroll/ max(1e-9, sum(stats[w]['tpd']*stats[w]['hold_days'] for w in wallets[:best_feasible])):.2f}/position)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--cutoff-unix", type=int, default=None)
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--bankroll-usd", type=float, default=1000.0)
    ap.add_argument("--min-position-usd", type=float, default=5.0)
    ap.add_argument("--per-wallet-out", default=None)
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    cutoff = args.cutoff_unix or latest_cutoff(con)
    wallets = load_watchlist(args.watchlist)
    print(f"watchlist={args.watchlist}")
    print(f"n_wallets={len(wallets)} cutoff_unix={cutoff} window_days={args.window_days}")

    stats = per_wallet_stats(con, wallets, cutoff, args.window_days)
    q1_prefixes(wallets, stats)
    q2_buckets(wallets, stats)
    q3_capital(wallets, stats, args.bankroll_usd, args.min_position_usd)

    if args.per_wallet_out:
        with open(args.per_wallet_out, "w") as f:
            f.write("wallet_hex\ttrades_per_day\tavg_pos_usd\thold_days\tn_trades_window\n")
            for w in wallets:
                s = stats[w]
                f.write(f"{w}\t{s['tpd']:.4f}\t{s['avg_pos_usd']:.2f}\t"
                        f"{s['hold_days']:.3f}\t{s['n_trades_window']}\n")
        print(f"\nwrote per-wallet TSV: {args.per_wallet_out}")
    con.close()


if __name__ == "__main__":
    main()
