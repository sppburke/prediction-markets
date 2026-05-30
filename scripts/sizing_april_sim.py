#!/usr/bin/env python3
"""Q4/Q5: position-sizing comparison on the April-2026 holdout.

Cohort: top-N at the 2026-03-31 cutoff (no lookahead — trained on prior cutoffs
only), throughput label. Their April resolved buys are the return stream.

Per-position return r_i = (outcome - vwap_entry) / vwap_entry, with the 500 bps
fee+slippage haircut applied (production-net, matches evaluate.rs). Positions are
ordered by `resolved_at_unix` — the only time field available — and the bankroll
compounds as each resolves. NOTE: OosPosition exposes neither entry time nor
stake size, so this is a SEQUENTIAL model (one bet resolves before the next is
sized). Real copy-trading runs many concurrent positions; sequential
compounding overstates how often the bankroll turns over. Treat the magnitudes
as directional, the FLAT-vs-KELLY ranking as the robust signal.

Q4  flat-$ (fixed stake, no compounding) vs 0.25-Kelly (fraction of current
    bankroll, compounding). Kelly fraction estimated EX-ANTE from pre-April
    return moments (continuous Kelly f* = mu/sigma^2 on the March cohort),
    applied to April. Full-Kelly and in-hindsight Kelly reported as bounds.

Q5  sweep starting bankroll $250..$10k. Min-position friction: a bet whose
    stake < --min-position-usd is skipped (can't be placed) — this penalises
    small bankrolls at tiny Kelly fractions. Reports terminal $, net profit,
    and monthly return % per starting amount, for flat-$ and 0.25-Kelly.
    LIMITATION: no per-trade size/liquidity data, so the UPPER bound (can $10k
    actually be deployed into these markets) is NOT modelled — the true
    optimum may be lower than this friction-only model implies.
"""
import argparse
import math
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner import data as data_mod
from monthly_rerank_gbm import gbm_rank_at

CUTOFF_MAR02 = 1772495999  # 2026-03-02
CUTOFF_MAR31 = 1775001599  # 2026-03-31  (April holdout cutoff)
CUTOFF_MAY01 = 1777679999  # 2026-05-01  (May holdout cutoff)
BANKROLL_GRID = [250, 500, 1000, 2500, 5000, 10000]

# (label, holdout cutoff, ex-ante Kelly estimation cutoff). The holdout's forward
# 30d window is the month being scored; the ex-ante cutoff is one step earlier so
# the Kelly fraction is set without lookahead into the scored month.
HOLDOUTS = [
    ("April", CUTOFF_MAR31, CUTOFF_MAR02),
    ("May", CUTOFF_MAY01, CUTOFF_MAR31),
]


def returns_for(db, cutoff, fwd_secs, cohort, haircut_bps):
    pos = data_mod.load_oos_positions(
        db, cutoff, cutoff + fwd_secs, frozenset(cohort), price_haircut_bps=haircut_bps
    )
    pos.sort(key=lambda p: p.resolved_at_unix)
    return [(p.outcome - p.vwap_entry) / p.vwap_entry for p in pos]


def cohort_buy_notional(db, cutoff, fwd_secs, cohort):
    """Total April buy-side notional (price×contracts) the cohort itself
    deployed into resolved markets — the natural capacity ceiling: you can mirror
    up to ~what the leaders bet before your orders become the marginal price and
    erode the measured edge. Also returns distinct-market count and the top-market
    share (concentration → per-market depth is the real binding constraint)."""
    import sqlite3
    win_start, win_end = cutoff, cutoff + fwd_secs
    wl = list(cohort)
    total = 0.0
    per_market = {}
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as con:
        for s in range(0, len(wl), 900):
            piece = wl[s:s + 900]
            q = ",".join("?" * len(piece))
            rows = con.execute(
                f"""SELECT t.market_id, SUM(CAST(t.price_str AS REAL)*t.contracts)
                    FROM trades t JOIN market_resolutions r USING (market_id)
                    WHERE t.wallet_hex IN ({q}) AND t.side='buy'
                      AND t.timestamp_unix > ? AND t.timestamp_unix <= ?
                      AND r.winning_outcome_id IS NOT NULL
                    GROUP BY t.market_id""",
                [*piece, win_start, win_end],
            ).fetchall()
            for mid, notional in rows:
                per_market[mid] = per_market.get(mid, 0.0) + (notional or 0.0)
    total = sum(per_market.values())
    n_mkts = len(per_market)
    top = sorted(per_market.values(), reverse=True)
    top10_share = sum(top[:10]) / total if total else 0.0
    return total, n_mkts, top10_share


def full_kelly_fraction(returns):
    """Continuous-approx growth-optimal fraction f* = mu/sigma^2, clipped [0,1].
    Robust to the fat right tail of longshot payoffs (where exact binary Kelly
    per-bet is undefined without a per-bet probability)."""
    if not returns:
        return 0.0
    mu = float(np.mean(returns))
    var = float(np.var(returns))
    if var <= 0:
        return 0.0
    return max(0.0, min(1.0, mu / var))


def sim_fractional(returns, f, b0, min_pos):
    """Compound b0 at fraction f of current bankroll per bet; skip bets whose
    stake < min_pos (cannot be placed)."""
    b = b0
    placed = skipped = 0
    for r in returns:
        stake = f * b
        if stake < min_pos:
            skipped += 1
            continue
        b += stake * r
        placed += 1
        if b <= 0:
            return 0.0, placed, skipped
    return b, placed, skipped


def sim_flat(returns, stake, b0, min_pos):
    """Fixed-dollar stake per bet (no compounding); skip if stake < min_pos or
    bankroll can't cover it."""
    if stake < min_pos:
        return b0, 0, len(returns)
    b = b0
    placed = skipped = 0
    for r in returns:
        if b < stake:
            skipped += 1
            continue
        b += stake * r
        placed += 1
    return b, placed, skipped


def _load_watchlist_cohort(path):
    """Return list of wallet hex addresses from a .txt watchlist file."""
    result = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                result.append(line)
    return result


def run_holdout(db, args, label, cutoff, exante_cutoff, all_cutoffs):
    """Run Q4/Q5/Q6 for one monthly holdout (cohort selected at `cutoff`,
    forward 30d returns, Kelly fraction estimated ex-ante from `exante_cutoff`)."""
    fwd_secs = args.fwd_days * 86400
    print(f"\n{'#'*70}\n# HOLDOUT: {label}  (cutoff {cutoff}, ex-ante {exante_cutoff})\n{'#'*70}")

    if args.watchlist:
        cohort = _load_watchlist_cohort(args.watchlist)
        print(f"using watchlist cohort: {len(cohort)} wallets from {args.watchlist}",
              file=sys.stderr)
    else:
        train = [c for c in all_cutoffs if c < cutoff]
        if not train:
            print(f"  no training cutoffs before {cutoff} — skip {label}"); return
        print(f"ranking {label} cohort (top-{args.top_n}, throughput)...", file=sys.stderr)
        cohort = gbm_rank_at(db, cutoff, fwd_secs, args.top_n,
                             args.min_trading_days, args.min_distinct_events,
                             args.min_fwd_pos, train, use_bhq=False,
                             label_type="throughput", n_seeds=args.n_seeds)
    rets = returns_for(db, cutoff, fwd_secs, cohort, args.haircut_bps)
    print(f"{label}: cohort n={len(cohort)}, resolved fwd positions={len(rets)}")
    if not rets:
        print(f"  no resolved {label} positions yet (month may be open/unresolved) — skip"); return
    mu, sd = float(np.mean(rets)), float(np.std(rets))
    print(f"{label} per-position return: mean={mu:+.4f} std={sd:.4f} "
          f"sum={sum(rets):+.2f}  (haircut={args.haircut_bps}bps, RESOLVED only)")

    # Ex-ante Kelly from the prior month's cohort/returns (no lookahead).
    # When using a fixed watchlist, the same wallets serve as the ex-ante cohort.
    train_ex = [c for c in all_cutoffs if c < exante_cutoff]
    f_exante = 0.0
    if train_ex:
        if args.watchlist:
            cohort_ex = cohort
        else:
            cohort_ex = gbm_rank_at(db, exante_cutoff, fwd_secs, args.top_n,
                                    args.min_trading_days, args.min_distinct_events,
                                    args.min_fwd_pos, train_ex, use_bhq=False,
                                    label_type="throughput", n_seeds=args.n_seeds)
        ex_rets = returns_for(db, exante_cutoff, fwd_secs, cohort_ex, args.haircut_bps)
        f_exante = full_kelly_fraction(ex_rets)
    f_hind = full_kelly_fraction(rets)
    print(f"full-Kelly f*: ex-ante={f_exante:.4f}  in-hindsight={f_hind:.4f}")
    fk = args.kelly_fraction * f_exante
    print(f"applied fractional Kelly = {args.kelly_fraction}× ex-ante = {fk:.4f} of bankroll/bet")

    # ── Q4: four sizing schemes, $1,000 start ─────────────────────────────────
    b0 = 1000.0
    PCTS = [0.01, 0.02, 0.05]
    print(f"\n=== Q4 [{label}]: $1,000 start — four sizing schemes ===")
    print("  (A) FLAT FIXED-$ per bet (no compounding):")
    for s in (10.0, 20.0, 50.0, 100.0):
        fb, fp, fsk = sim_flat(rets, s, b0, args.min_position_usd)
        print(f"        ${s:>6.0f}/bet -> ${fb:>10,.2f} ({fb/b0-1:+8.1%})  placed={fp} skipped={fsk}")
    lo, hi = 0.0, b0
    for _ in range(40):
        mid = (lo + hi) / 2
        bal, mn = b0, b0
        for r in rets:
            bal += mid * r
            mn = min(mn, bal)
        lo, hi = (mid, hi) if mn > 0 else (lo, mid)
    fb, _, _ = sim_flat(rets, lo, b0, args.min_position_usd)
    print(f"      → max-safe flat stake (never ruins on {label} path): ${lo:,.2f}/bet "
          f"-> ${fb:,.2f} ({fb/b0-1:+.1%})")
    print("  (B) % of INITIAL bankroll (constant $, no compounding):")
    for p in PCTS:
        fb, fp, fsk = sim_flat(rets, p * b0, b0, args.min_position_usd)
        print(f"        {p:>5.0%} (${p*b0:>5.0f}/bet) -> ${fb:>10,.2f} ({fb/b0-1:+8.1%})  placed={fp} skipped={fsk}")
    print("  (C) % of CURRENT bankroll (compounds):")
    for p in PCTS:
        kb, kp, ksk = sim_fractional(rets, p, b0, args.min_position_usd)
        print(f"        {p:>5.0%} of B_t -> ${kb:>10,.2f} ({kb/b0-1:+8.1%})  placed={kp} skipped={ksk}")
    kb, kp, ksk = sim_fractional(rets, fk, b0, args.min_position_usd)
    full_b, _, _ = sim_fractional(rets, f_exante, b0, args.min_position_usd)
    print("  (D) KELLY (compounds):")
    print(f"        0.25-Kelly ({fk:.4f} of B_t) -> ${kb:>10,.2f} ({kb/b0-1:+8.1%})  placed={kp} skipped={ksk}")
    print(f"        [ref] full-Kelly ({f_exante:.4f})  -> ${full_b:>10,.2f} ({full_b/b0-1:+8.1%})")

    # ── Q5: starting-bankroll sweep ───────────────────────────────────────────
    print(f"\n=== Q5 [{label}]: starting-bankroll sweep (min position ${args.min_position_usd:.0f}) ===")
    print(f"{'start $':>9} {'flat end $':>12} {'flat %':>8} {'Kelly end $':>12} {'Kelly %':>8} {'K placed':>9}")
    best_rate = (None, -1e9); best_profit = (None, -1e9)
    for bk in BANKROLL_GRID:
        fs = max(args.min_position_usd, fk * bk)
        fb, _, _ = sim_flat(rets, fs, float(bk), args.min_position_usd)
        kb, kp, _ = sim_fractional(rets, fk, float(bk), args.min_position_usd)
        krate = kb / bk - 1
        print(f"{bk:>9,} {fb:>12,.0f} {fb/bk-1:>7.1%} {kb:>12,.0f} {krate:>7.1%} {kp:>9}")
        if krate > best_rate[1]:
            best_rate = (bk, krate)
        if (kb - bk) > best_profit[1]:
            best_profit = (bk, kb - bk)
    print(f"  → best monthly RATE: ${best_rate[0]:,} ({best_rate[1]:+.1%})")
    print(f"  → best $ PROFIT:     ${best_profit[0]:,} (+${best_profit[1]:,.0f})")
    print("  (friction-only model — upper-bound deployment into market liquidity NOT modelled)")

    # ── Q6: strategy capacity ─────────────────────────────────────────────────
    total_notional, n_mkts, top10 = cohort_buy_notional(db, cutoff, fwd_secs, cohort)
    print(f"\n=== Q6 [{label}]: strategy capacity (cohort buy-side notional) ===")
    print(f"  cohort buy notional: ${total_notional:,.0f} across {n_mkts:,} resolved markets")
    print(f"  top-10-market share: {top10:.1%}  (higher = more concentrated = lower real capacity)")
    print("  capacity bands (fraction of leaders' own size you can mirror before price impact):")
    for frac, lbl in [(0.10, "conservative"), (0.25, "moderate"), (1.00, "aggressive (≈match leaders)")]:
        print(f"    {lbl:<28} ${total_notional*frac:,.0f}/month")
    print("  NOTE: notional-based upper bound (no order-book depth); edge erodes as size → leaders'.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--top-n", type=int, default=5000)
    ap.add_argument("--fwd-days", type=int, default=30)
    ap.add_argument("--min-trading-days", type=int, default=20)
    ap.add_argument("--min-distinct-events", type=int, default=10)
    ap.add_argument("--min-fwd-pos", type=int, default=3)
    ap.add_argument("--haircut-bps", type=int, default=500)
    ap.add_argument("--kelly-fraction", type=float, default=0.25)
    ap.add_argument("--min-position-usd", type=float, default=5.0)
    ap.add_argument("--n-seeds", type=int, default=5)
    ap.add_argument("--holdouts", default="April,May",
                    help="comma list of holdout labels to run (April, May)")
    ap.add_argument("--watchlist", default=None,
                    help="path to a .txt watchlist file; when provided, skips GBM "
                         "ranking and uses the listed wallets as the fixed cohort")
    args = ap.parse_args()

    all_cutoffs = data_mod.distinct_cutoffs(args.db_path)
    want = {h.strip() for h in args.holdouts.split(",")}
    for label, cutoff, exante in HOLDOUTS:
        if label in want:
            run_holdout(args.db_path, args, label, cutoff, exante, all_cutoffs)


if __name__ == "__main__":
    main()
