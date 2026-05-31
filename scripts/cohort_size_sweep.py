#!/usr/bin/env python3
"""Cohort-size sweep: find the wallet-count N that matches a given bankroll.

The greedy selector builds the cohort in order (each pick depends only on prior
picks), so the first-N wallets of a max_n=100 selection IS the N-wallet
portfolio. We therefore run ONE walk-forward at max_n=100 and evaluate forward
edge at prefixes N in GRID — the whole cohort-size curve from a single pass.

For each N we report, aggregated across the walk-forward anchors:
  - mean_of_mean_edge, mean Sharpe, total forward positions, n_anchors_negative
  - ex-ante full-Kelly fraction (mean across anchors)
and, at the deploy cutoff:
  - capacity = cohort buy-side notional ($/month) for the first-N wallets
  - implied min bankroll so 1/4-Kelly per-bet clears the $5 minimum position
  - recommended capital band [min bankroll, conservative capacity ceiling]

Anchors run concurrently (ThreadPoolExecutor) — same pattern as validate.py.
Writes per-N deploy watchlists + a JSON summary to data/eval-results/.
"""
import argparse
import gc
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPTS))

from portfolio_constructor import data as _data
from portfolio_constructor import edge as _edge
from portfolio_constructor.overlap import marginal_overlap
from portfolio_constructor.selector import GreedySelector
from gbm_walkforward import eligible_anchors
from monthly_rerank_gbm import safe_workers

GRID = [5, 10, 15, 20, 30, 40, 50, 75, 100]
MIN_POSITION_USD = 5.0
KELLY_FRACTION = 0.25       # quarter-Kelly (matches sizing_april_sim default)
HAIRCUT_BPS = 500
CONSERVATIVE_CAPACITY = 0.10   # fraction of cohort notional deployable (matches sizing Q6)


def full_kelly_fraction(returns):
    """Continuous Kelly f* = mu / var, clamped >= 0 (matches sizing_april_sim)."""
    if len(returns) < 2:
        return 0.0
    mu = float(np.mean(returns))
    var = float(np.var(returns, ddof=1))
    if var <= 0:
        return 0.0
    return max(0.0, mu / var)


def _metrics(returns):
    a = np.array(returns, dtype=float)
    if a.size == 0:
        return dict(n=0, mean=0.0, std=0.0, sharpe=0.0, total=0.0)
    mean = float(a.mean())
    std = float(a.std(ddof=1)) if a.size > 1 else 0.0
    return dict(n=int(a.size), mean=mean, std=std,
                sharpe=(mean / std if std > 0 else 0.0), total=float(a.sum()))


def _net_returns(positions):
    """Forward net returns (500 bps) keyed per position, with originating wallet."""
    hf = 1.0 + HAIRCUT_BPS / 10_000.0
    out = []
    for p in positions:
        nv = min(p.vwap_entry * hf, 0.999)
        out.append((p.wallet_hex, (p.outcome - nv) / nv))
    return out


def evaluate_anchor(db, anchor, fwd_secs, all_cutoffs, lookback_secs, max_n, top_n):
    """Return per-N forward + ex-ante metrics for one anchor (ordered prefixes)."""
    train = [c for c in all_cutoffs if c < anchor]
    scores = _edge.gbm_edge_scores(db, anchor, fwd_secs, 20, 10, 3, train,
                                   use_bhq=True, label_type="perpos",
                                   n_seeds=1, random_state=42)
    date = datetime.fromtimestamp(anchor, tz=timezone.utc).date().isoformat()
    if not scores:
        return date, {}
    # prefilter (same as production) then greedy-select ordered top-max_n
    cand = dict(sorted(scores.items(), key=lambda kv: -kv[1])[:max(max_n * 4, 200)])
    msets = _data.load_wallet_market_sets(db, anchor, list(cand), lookback_secs)
    sel = GreedySelector(overlap_fn=marginal_overlap, overlap_lambda=1.0).select(
        cand, msets, max_n=max_n, min_edge_score=0.0)
    ordered = sel.wallets
    print(f"  {date}: selected {len(ordered)} wallets (ordered)", flush=True)
    if not ordered:
        return date, {}

    cohort = frozenset(ordered)
    fwd = _data.load_oos_positions(db, anchor, anchor + fwd_secs, cohort, price_haircut_bps=0)
    exa = _data.load_oos_positions(db, anchor - lookback_secs, anchor, cohort, price_haircut_bps=0)
    fwd_net = _net_returns(fwd)                                   # (wallet, net_ret)
    exa_ret = [(p.wallet_hex, (p.outcome - p.vwap_entry) / p.vwap_entry) for p in exa]

    per_n = {}
    for n in [g for g in GRID if g <= len(ordered)] + ([len(ordered)] if len(ordered) not in GRID else []):
        first = set(ordered[:n])
        fret = [r for (w, r) in fwd_net if w in first]
        eret = [r for (w, r) in exa_ret if w in first]
        m = _metrics(fret)
        m["kelly_exante"] = full_kelly_fraction(eret)
        m["n_cohort"] = n
        per_n[n] = m
    del scores, cand, msets, fwd, exa, fwd_net, exa_ret  # free before next anchor
    gc.collect()
    return date, per_n


def deploy_capacity(db, cutoff, fwd_secs, lookback_secs, ordered, grid):
    """Cohort buy-side notional for first-N wallets, measured over the TRAILING
    `lookback_secs` window before the deploy cutoff (a proxy for go-forward
    capacity). NOT the forward window: at the deploy cutoff the forward window is
    in the future and empty, which would report capacity = 0 for every N."""
    import sqlite3
    win_s, win_e = cutoff - lookback_secs, cutoff
    notional_by_wallet = {}
    wl = list(ordered)
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as con:
        for s in range(0, len(wl), 900):
            piece = wl[s:s + 900]
            q = ",".join("?" * len(piece))
            for w, notion in con.execute(
                f"""SELECT t.wallet_hex, SUM(CAST(t.price_str AS REAL)*t.contracts)
                    FROM trades t
                    WHERE t.wallet_hex IN ({q}) AND t.side='buy'
                      AND t.timestamp_unix > ? AND t.timestamp_unix <= ?
                    GROUP BY t.wallet_hex""", [*piece, win_s, win_e]):
                notional_by_wallet[w] = float(notion or 0.0)
    cum = {}
    for n in grid:
        cum[n] = sum(notional_by_wallet.get(w, 0.0) for w in ordered[:n])
    return cum


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--watchlist", required=True, help="deploy-cutoff candidate filter")
    ap.add_argument("--fwd-days", type=int, default=7)
    ap.add_argument("--lookback-days", type=int, default=90)
    ap.add_argument("--max-n", type=int, default=100)
    ap.add_argument("--top-n", type=int, default=5000)
    ap.add_argument("--max-workers", type=int, default=4)
    ap.add_argument("--output-dir", type=Path, default=Path("data/eval-results"))
    args = ap.parse_args()

    fwd = args.fwd_days * 86400
    lookback = args.lookback_days * 86400
    all_cutoffs = _data.distinct_cutoffs(args.db_path)
    anchors = eligible_anchors(all_cutoffs, fwd)

    def _ev(a):
        row = evaluate_anchor(args.db_path, a, fwd, all_cutoffs, lookback, args.max_n, args.top_n)
        gc.collect()
        return row

    # Cap workers by free RAM (each anchor holds a training frame + GBM) to
    # prevent OOM when memory is tight or the box is shared with another job.
    nw = safe_workers(args.max_workers, len(anchors))
    print(f"walk-forward anchors: {len(anchors)} on {nw} threads"
          + (f" (memory guard capped from {args.max_workers})" if nw < min(args.max_workers, len(anchors)) else ""),
          flush=True)
    with ThreadPoolExecutor(max_workers=nw) as ex:
        results = list(ex.map(_ev, anchors))

    # aggregate per-N across anchors
    agg = {}
    for n in GRID:
        means, sharpes, totals, npos, kellys, negs, present = [], [], [], [], [], 0, 0
        for _date, per_n in results:
            if n in per_n and per_n[n]["n"] > 0:
                present += 1
                means.append(per_n[n]["mean"]); sharpes.append(per_n[n]["sharpe"])
                totals.append(per_n[n]["total"]); npos.append(per_n[n]["n"])
                kellys.append(per_n[n]["kelly_exante"])
                if per_n[n]["mean"] < 0:
                    negs += 1
        if present:
            agg[n] = dict(
                anchors=present,
                mean_of_mean_edge=float(np.mean(means)),
                mean_sharpe=float(np.mean(sharpes)),
                total_positions=int(sum(npos)),
                mean_kelly_exante=float(np.mean(kellys)),
                n_anchors_negative=negs,
                credible=(present >= 4 and negs == 0 and float(np.mean(means)) > 0),
            )

    # deploy cutoff: ordered selection filtered to watchlist, capacity per N
    deploy = all_cutoffs[-1]
    train = [c for c in all_cutoffs if c < deploy]
    dscores = _edge.gbm_edge_scores(args.db_path, deploy, fwd, 20, 10, 3, train,
                                    use_bhq=True, label_type="perpos", n_seeds=1, random_state=42)
    allow = {l.strip() for l in open(args.watchlist) if l.strip() and not l.startswith("#")}
    dscores = {w: s for w, s in dscores.items() if w in allow}
    dcand = dict(sorted(dscores.items(), key=lambda kv: -kv[1])[:max(args.max_n * 4, 200)])
    dmsets = _data.load_wallet_market_sets(args.db_path, deploy, list(dcand), lookback)
    dsel = GreedySelector(overlap_fn=marginal_overlap, overlap_lambda=1.0).select(
        dcand, dmsets, max_n=args.max_n, min_edge_score=0.0)
    dordered = dsel.wallets
    grid_eff = [n for n in GRID if n <= len(dordered)]
    cap = deploy_capacity(args.db_path, deploy, fwd, lookback, dordered, grid_eff)

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ") if False else "SWEEP"
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # ─── report table ───────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print(f"COHORT-SIZE SWEEP  (deploy cohort size = {len(dordered)} wallets available)")
    print("=" * 100)
    hdr = f"{'N':>4} {'wf_anchors':>10} {'mean_edge':>10} {'sharpe':>7} {'pos':>7} {'neg':>4} {'cred':>5} {'1/4Kelly%':>10} {'capacity$/mo':>14} {'min_bankroll':>12} {'capital_band':>22}"
    print(hdr)
    print("-" * len(hdr))
    table = {}
    for n in GRID:
        if n not in agg:
            continue
        a = agg[n]
        capN = cap.get(n, 0.0)
        cons_cap = capN * CONSERVATIVE_CAPACITY
        qk = a["mean_kelly_exante"] * KELLY_FRACTION              # quarter-Kelly fraction
        min_bank = (MIN_POSITION_USD / qk) if qk > 0 else float("inf")
        band_lo = min_bank
        band_hi = cons_cap
        band = (f"${band_lo:,.0f}–${band_hi:,.0f}" if band_lo <= band_hi and band_lo != float("inf")
                else ("n/a (floor>cap)" if band_lo != float("inf") else "n/a (Kelly<=0)"))
        print(f"{n:>4} {a['anchors']:>10} {a['mean_of_mean_edge']:>+10.4f} {a['mean_sharpe']:>+7.3f} "
              f"{a['total_positions']:>7} {a['n_anchors_negative']:>4} {str(a['credible']):>5} "
              f"{qk*100:>9.3f}% {cons_cap:>13,.0f} {('inf' if min_bank==float('inf') else f'{min_bank:,.0f}'):>12} {band:>22}")
        table[n] = dict(**a, capacity_conservative_usd_mo=cons_cap,
                        quarter_kelly_frac=qk, min_bankroll_usd=min_bank,
                        capital_band=[band_lo if band_lo != float("inf") else None, band_hi])
        # write per-N deploy watchlist
        wlpath = args.output_dir / f"watchlist-cohort-N{n:03d}-portfolio_greedy.txt"
        with open(wlpath, "w") as f:
            f.write(f"# cohort-size sweep deploy set, N={n}\n# deploy_cutoff_unix={deploy}\n")
            for w in dordered[:n]:
                f.write(w + "\n")
    print("=" * 100)
    print("capacity = first-N cohort buy-notional × 10% (conservative, matches sizing Q6).")
    print("min_bankroll = $5 min-position / quarter-Kelly fraction. capital_band = [min_bankroll, conservative capacity].")
    print("per-N watchlists: data/eval-results/watchlist-cohort-N*.txt")

    out = dict(grid=GRID, deploy_cohort_available=len(dordered),
               per_anchor=[{"date": d, "by_n": p} for d, p in results],
               aggregate=table)
    jpath = args.output_dir / "cohort_size_sweep.json"
    json.dump(out, open(jpath, "w"), indent=2, default=str)
    print(f"JSON: {jpath}")


if __name__ == "__main__":
    main()
