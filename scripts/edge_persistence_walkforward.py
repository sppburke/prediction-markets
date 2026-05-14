#!/usr/bin/env python3
"""
Edge-persistence walk-forward experiment (test #2 + edge-metric validation).

Answers two questions on the EXISTING cache (no Dune re-pull needed):

  Q1 (the copy-trading thesis): does leader edge PERSIST out-of-sample?
      If the top-K wallets ranked on a trailing window have ~0 or negative
      realized edge in the forward window, copy-trading has no edge to capture.

  Q2 (the framework): does price-normalized calibration-edge LCB rank more
      persistently than the current return-on-cost LCB, or than raw win rate?

Method
------
Unit = a resolved POSITION: one (wallet, market, outcome) with VWAP entry price
and a binary outcome (did the wallet's side win). Positions are windowed by
RESOLUTION date — at decision time T you only know positions resolved before T.

Per walk-forward step T (monthly):
  train  = positions resolved in [T-180d, T)   -> rank wallets by each metric
  fwd    = positions resolved in [T, T+30d)    -> measure realized edge of top-K
Eligibility: >= MIN_TRADES resolved positions in the training window.

Metrics compared (all per-wallet, computed on the training window):
  edge_lcb  : mean(outcome - entry) - z * stderr     <- proposed
  ret_lcb   : mean((outcome-entry)/entry) - z*stderr  <- current score.rs basis
  win_rate  : mean(outcome)                           <- the naive metric
  (baseline): no ranking - mean fwd edge of ALL eligible wallets

Outputs: per-window table + summary (mean forward edge of top-K by each metric,
mean Spearman rank-corr between metric and forward realized edge), a decay curve,
and a favorite-buyer down-ranking sanity check.

Pure stdlib. Run: python3 scripts/edge_persistence_walkforward.py
"""

import sqlite3
import statistics
from collections import defaultdict

DB = "/home/sean/backtest-data/wallet_cache.db"
DAY = 86_400
TRAIN_DAYS = 180
FWD_DAYS = 30
STEP_DAYS = 30
MIN_TRADES = 15          # matches dune_min_closed_markets / ranker active_min_closed
TOP_K = 50               # matches active_watchlist_size
Z = 1.645                # one-tailed 5th percentile
MIN_ELIGIBLE = 30        # skip windows with too few eligible wallets to be meaningful


def load_positions(db_path):
    """Collapse buy trades into resolved (wallet, market, outcome) positions."""
    con = sqlite3.connect(db_path)
    q = """
        SELECT t.wallet_hex,
               SUM(CAST(t.price_str AS REAL) * t.contracts) / SUM(t.contracts) AS vwap,
               CASE WHEN r.winning_outcome_id = t.outcome_id THEN 1 ELSE 0 END  AS won,
               r.resolved_at_unix
        FROM trades t
        JOIN market_resolutions r ON t.market_id = r.market_id
        WHERE t.side = 'buy'
          AND r.winning_outcome_id IS NOT NULL
          AND t.contracts > 0
        GROUP BY t.wallet_hex, t.market_id, t.outcome_id,
                 r.winning_outcome_id, r.resolved_at_unix
    """
    rows = con.execute(q).fetchall()
    con.close()
    # keep only sane prices in (0,1)
    out = []
    for w, vwap, won, resolved in rows:
        if vwap is None or vwap <= 0.0 or vwap >= 1.0:
            continue
        out.append((w, float(vwap), int(won), int(resolved)))
    return out


def lcb(values):
    """mean - Z * stderr; None if n < 2."""
    n = len(values)
    if n < 2:
        return None
    m = statistics.fmean(values)
    sd = statistics.pstdev(values)
    return m - Z * sd / (n ** 0.5)


def spearman(pairs):
    """Spearman rank correlation on a list of (x, y). None if n < 3."""
    n = len(pairs)
    if n < 3:
        return None

    def ranks(vals):
        order = sorted(range(n), key=lambda i: vals[i])
        r = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and vals[order[j + 1]] == vals[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r

    xs = ranks([p[0] for p in pairs])
    ys = ranks([p[1] for p in pairs])
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    dx = sum((xs[i] - mx) ** 2 for i in range(n)) ** 0.5
    dy = sum((ys[i] - my) ** 2 for i in range(n)) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def wallet_window_stats(positions):
    """positions: list of (vwap, won) for one wallet in one window.
    Returns dict of the three metrics, or None if ineligible."""
    if len(positions) < MIN_TRADES:
        return None
    edges = [won - vwap for vwap, won in positions]
    rets = [(won - vwap) / vwap for vwap, won in positions]
    wins = [float(won) for vwap, won in positions]
    e_lcb = lcb(edges)
    r_lcb = lcb(rets)
    if e_lcb is None or r_lcb is None:
        return None
    return {
        "edge_lcb": e_lcb,
        "ret_lcb": r_lcb,
        "win_rate": statistics.fmean(wins),
        "mean_edge": statistics.fmean(edges),
        "mean_entry": statistics.fmean([vwap for vwap, _ in positions]),
        "n": len(positions),
    }


def main():
    print("Loading resolved positions from cache ...")
    pos = load_positions(DB)
    print(f"  {len(pos):,} resolved (wallet, market, outcome) positions\n")

    tmin = min(p[3] for p in pos)
    tmax = max(p[3] for p in pos)
    # first decision point: enough history for a full training window
    t = tmin + TRAIN_DAYS * DAY
    last = tmax - FWD_DAYS * DAY

    # accumulators
    agg = {m: {"fwd_edge": [], "spearman": []} for m in ("edge_lcb", "ret_lcb", "win_rate")}
    baseline_fwd = []
    decay = {0: [], 1: [], 2: []}   # forward sub-windows 0-30d, 30-60d, 60-90d
    fav_checks = []                  # (avg win_rate of top-K-by-edge, avg of bottom-K-by-edge)

    print(f"{'train_end':<12}{'elig':>6}"
          f"{'edgeLCB_fwd':>13}{'retLCB_fwd':>12}{'winR_fwd':>11}{'base_fwd':>11}"
          f"{'rho_edge':>10}{'rho_ret':>9}{'rho_win':>9}")
    print("-" * 104)

    n_windows = 0
    while t <= last:
        train_lo, train_hi = t - TRAIN_DAYS * DAY, t
        fwd_lo, fwd_hi = t, t + FWD_DAYS * DAY

        train_by_w = defaultdict(list)
        fwd_by_w = defaultdict(list)
        for w, vwap, won, resolved in pos:
            if train_lo <= resolved < train_hi:
                train_by_w[w].append((vwap, won))
            elif fwd_lo <= resolved < fwd_hi:
                fwd_by_w[w].append((vwap, won))

        # eligible wallets + their training metrics
        elig = {}
        for w, ps in train_by_w.items():
            s = wallet_window_stats(ps)
            if s is not None:
                elig[w] = s

        if len(elig) < MIN_ELIGIBLE:
            t += STEP_DAYS * DAY
            continue

        # forward realized edge per wallet
        fwd_edge = {}
        for w, ps in fwd_by_w.items():
            fwd_edge[w] = statistics.fmean([won - vwap for vwap, won in ps])

        # baseline: mean fwd edge across ALL eligible wallets that appear in fwd
        base_vals = [fwd_edge[w] for w in elig if w in fwd_edge]
        base = statistics.fmean(base_vals) if base_vals else None
        if base is not None:
            baseline_fwd.append(base)

        row = {"edge_lcb": None, "ret_lcb": None, "win_rate": None}
        rho = {"edge_lcb": None, "ret_lcb": None, "win_rate": None}
        for metric in ("edge_lcb", "ret_lcb", "win_rate"):
            ranked = sorted(elig.items(), key=lambda kv: kv[1][metric], reverse=True)
            topk = [w for w, _ in ranked[:TOP_K]]
            tk_vals = [fwd_edge[w] for w in topk if w in fwd_edge]
            if tk_vals:
                fe = statistics.fmean(tk_vals)
                row[metric] = fe
                agg[metric]["fwd_edge"].append(fe)
            # rank correlation: training metric vs forward realized edge
            pairs = [(elig[w][metric], fwd_edge[w]) for w in elig if w in fwd_edge]
            rc = spearman(pairs)
            if rc is not None:
                rho[metric] = rc
                agg[metric]["spearman"].append(rc)

        # favorite down-ranking check: win rate of top-K vs bottom-K by edge_lcb
        ranked_e = sorted(elig.items(), key=lambda kv: kv[1]["edge_lcb"], reverse=True)
        top_wr = statistics.fmean([s["win_rate"] for _, s in ranked_e[:TOP_K]])
        bot_wr = statistics.fmean([s["win_rate"] for _, s in ranked_e[-TOP_K:]])
        fav_checks.append((top_wr, bot_wr))

        # decay: forward edge of top-K-by-edge_lcb at 0-30 / 30-60 / 60-90
        topk_e = [w for w, _ in ranked_e[:TOP_K]]
        for sub in (0, 1, 2):
            lo = t + sub * FWD_DAYS * DAY
            hi = lo + FWD_DAYS * DAY
            vals = []
            for w in topk_e:
                wv = [won - vwap for ww, vwap, won, r in pos
                      if ww == w and lo <= r < hi]
                if wv:
                    vals.append(statistics.fmean(wv))
            if vals:
                decay[sub].append(statistics.fmean(vals))

        def f(x):
            return f"{x:+.4f}" if x is not None else "    n/a"

        from datetime import datetime, timezone
        te = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
        print(f"{te:<12}{len(elig):>6}"
              f"{f(row['edge_lcb']):>13}{f(row['ret_lcb']):>12}{f(row['win_rate']):>11}{f(base):>11}"
              f"{f(rho['edge_lcb']):>10}{f(rho['ret_lcb']):>9}{f(rho['win_rate']):>9}")
        n_windows += 1
        t += STEP_DAYS * DAY

    # ── summary ────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"SUMMARY  ({n_windows} substantive windows, "
          f"{TRAIN_DAYS}d train / {FWD_DAYS}d fwd / top-{TOP_K})")
    print("=" * 60)
    print(f"{'metric':<14}{'mean fwd edge':>16}{'mean Spearman':>16}{'windows +ve':>14}")
    for m in ("edge_lcb", "ret_lcb", "win_rate"):
        fe = agg[m]["fwd_edge"]
        sp = agg[m]["spearman"]
        pos_frac = sum(1 for x in fe if x > 0) / len(fe) if fe else 0
        print(f"{m:<14}"
              f"{statistics.fmean(fe) if fe else 0:>+16.4f}"
              f"{statistics.fmean(sp) if sp else 0:>+16.4f}"
              f"{pos_frac:>13.0%}")
    if baseline_fwd:
        print(f"{'(baseline)':<14}{statistics.fmean(baseline_fwd):>+16.4f}"
              f"{'—':>16}{'—':>14}   (no ranking; all eligible wallets)")

    print("\nDECAY — top-50-by-edge_lcb realized edge as forward window moves out:")
    for sub, label in [(0, "[0-30d]"), (1, "[30-60d]"), (2, "[60-90d]")]:
        v = decay[sub]
        print(f"  {label:<10} {statistics.fmean(v):+.4f}" if v else f"  {label:<10} n/a")

    print("\nFAVORITE DOWN-RANKING CHECK (does edge_lcb push favorite-buyers down?):")
    if fav_checks:
        tw = statistics.fmean([c[0] for c in fav_checks])
        bw = statistics.fmean([c[1] for c in fav_checks])
        print(f"  avg win-rate of top-50 by edge_lcb:    {tw:.1%}")
        print(f"  avg win-rate of bottom-50 by edge_lcb: {bw:.1%}")
        print("  (if bottom >> top, edge_lcb is correctly down-ranking high-win-rate "
              "favorite-buyers)")

    print("\nVERDICT GUIDE:")
    print("  Q1 thesis: edge_lcb 'mean fwd edge' > 0 AND > baseline  -> edge persists, GO")
    print("             ~0 or < baseline                            -> thesis dead, STOP")
    print("  Q2 frame:  edge_lcb beats ret_lcb & win_rate on fwd edge + Spearman")
    print("             -> the price-normalized metric is the better ranking key")


if __name__ == "__main__":
    main()
