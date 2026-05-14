#!/usr/bin/env python3
"""
Combined caveat-check for the edge-persistence walk-forward (test #2, final verdict).

v1 (edge_persistence_walkforward.py) tested WALLET-level, ALL-price-band, GROSS edge
and found no persistence (Spearman ~0, top-50 forward edge < baseline). Two caveats
could rescue it; this script closes both, plus a third correctness fix:

  1. OPERATOR POOLING   - pool wallets into operators via union-find over funder_edges
                          (production pools by operator; testing wallets tests the
                          wrong unit). Funding-hub mega-components are split back to
                          singletons above MAX_OP_SIZE - they are not real operators.
  2. COPYABLE BAND ONLY - restrict BOTH ranking and forward measurement to positions
                          we could actually copy: signal price such that the
                          slippage-adjusted fill clears the max_signal_price=0.85 gate
                          -> vwap < 0.85 / 1.05.
  3. NET OF FEE         - edge = won - vwap * (1 + 0.04 fee + 0.01 slippage).
                          Gross edge rewards low-variance favorite-buying; net is what
                          we actually capture.

Proper statistics (scipy): per-window Spearman with p-value; one-sample t-test on the
per-window rho series and on the per-window (top50 - baseline) forward-edge gap;
bootstrap 95% CI on pooled top-50 forward edge.

VERDICT:
  GO   - top-50 forward NET edge > 0, > baseline, and the gap is significant (p<0.05),
         AND mean Spearman significantly > 0.
  STOP - otherwise: operator-level edge does not persist in the copyable band; the
         Winner-Follow leader-selection premise is unsupported.

Run: .venv-analysis/bin/python scripts/edge_persistence_operator_check.py
"""

import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
from scipy import stats

DB = "/home/sean/backtest-data/wallet_cache.db"
DAY = 86_400
TRAIN_DAYS = 180
FWD_DAYS = 30
STEP_DAYS = 30
MIN_TRADES = 15
TOP_K = 50
Z = 1.645
MIN_ELIGIBLE = 30
FEE_SLIP = 1.05                       # 1 + 0.04 fee + 0.01 slippage
COPYABLE_MAX_VWAP = 0.85 / FEE_SLIP   # ~0.8095 - clears the max_signal_price gate
MAX_OP_SIZE = 25                      # components larger than this are funding hubs, not operators


# ── operator clustering: union-find over funder_edges ──────────────────────
def build_operators(con):
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for funder, funded in con.execute("SELECT funder_hex, funded_hex FROM funder_edges"):
        union(funder, funded)

    comp = defaultdict(list)
    for node in list(parent):
        comp[find(node)].append(node)

    # wallet -> operator_id. Split funding-hub mega-components back to singletons.
    w2op = {}
    sizes = []
    for root, members in comp.items():
        if len(members) <= MAX_OP_SIZE:
            sizes.append(len(members))
            for m in members:
                w2op[m] = f"op:{root}"
        else:
            for m in members:
                w2op[m] = f"solo:{m}"   # hub artifact - treat each as its own operator
    return w2op, sizes, len(comp)


def load_positions(con):
    q = """
        SELECT t.wallet_hex,
               SUM(CAST(t.price_str AS REAL) * t.contracts) / SUM(t.contracts) AS vwap,
               CASE WHEN r.winning_outcome_id = t.outcome_id THEN 1 ELSE 0 END  AS won,
               r.resolved_at_unix
        FROM trades t
        JOIN market_resolutions r ON t.market_id = r.market_id
        WHERE t.side = 'buy' AND r.winning_outcome_id IS NOT NULL AND t.contracts > 0
        GROUP BY t.wallet_hex, t.market_id, t.outcome_id,
                 r.winning_outcome_id, r.resolved_at_unix
    """
    out = []
    for w, vwap, won, resolved in con.execute(q):
        if vwap is None or vwap <= 0.0 or vwap >= 1.0:
            continue
        out.append((w, float(vwap), int(won), int(resolved)))
    return out


def edge_lcb(net_edges):
    n = len(net_edges)
    if n < 2:
        return None
    m = statistics.fmean(net_edges)
    sd = statistics.pstdev(net_edges)
    return m - Z * sd / (n ** 0.5)


def boot_ci(values, iters=2000, seed=1):
    rng = np.random.default_rng(seed)
    arr = np.asarray(values, dtype=float)
    if len(arr) < 2:
        return (float("nan"), float("nan"))
    means = rng.choice(arr, size=(iters, len(arr)), replace=True).mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def main():
    con = sqlite3.connect(DB)
    print("Building operator clusters from funder_edges ...")
    w2op, sizes, n_comp = build_operators(con)
    if sizes:
        print(f"  {n_comp:,} components | kept-as-operator size: "
              f"median {statistics.median(sizes)}, mean {statistics.fmean(sizes):.1f}, "
              f"max {max(sizes)} | {sum(1 for s in sizes if s>1):,} multi-wallet operators")
    print("Loading resolved positions ...")
    pos = load_positions(con)
    con.close()
    print(f"  {len(pos):,} resolved positions")

    # index positions by operator; copyable flag + net edge precomputed
    by_op = defaultdict(list)   # op -> list of (resolved_unix, net_edge, copyable)
    n_copyable = 0
    for w, vwap, won, resolved in pos:
        op = w2op.get(w, f"solo:{w}")     # wallets with no funder edge -> singleton operator
        copyable = vwap < COPYABLE_MAX_VWAP
        if copyable:
            n_copyable += 1
        net_edge = won - vwap * FEE_SLIP
        by_op[op].append((resolved, net_edge, copyable))
    print(f"  {n_copyable:,} positions in copyable band (vwap < {COPYABLE_MAX_VWAP:.3f}) "
          f"| {len(by_op):,} operators\n")

    tmin = min(p[3] for p in pos)
    tmax = max(p[3] for p in pos)
    t = tmin + TRAIN_DAYS * DAY
    last = tmax - FWD_DAYS * DAY

    per_window_rho = []
    per_window_gap = []          # top50 fwd edge - baseline fwd edge
    per_window_top = []
    per_window_base = []
    all_top_fwd_positions = []   # pooled, for bootstrap CI
    n_windows = 0

    print(f"{'train_end':<12}{'elig_ops':>9}{'top50_fwd_net':>15}"
          f"{'baseline_fwd':>14}{'gap':>10}{'rho':>9}{'rho_p':>9}")
    print("-" * 78)

    while t <= last:
        tr_lo, tr_hi = t - TRAIN_DAYS * DAY, t
        fw_lo, fw_hi = t, t + FWD_DAYS * DAY

        train_metric = {}     # op -> edge_lcb on copyable training positions
        fwd_edge = {}         # op -> mean net edge on copyable forward positions
        fwd_positions = {}    # op -> list of net edges (copyable, forward)

        for op, recs in by_op.items():
            tr = [ne for (r, ne, cp) in recs if cp and tr_lo <= r < tr_hi]
            fw = [ne for (r, ne, cp) in recs if cp and fw_lo <= r < fw_hi]
            if len(tr) >= MIN_TRADES:
                lcb = edge_lcb(tr)
                if lcb is not None:
                    train_metric[op] = lcb
            if fw:
                fwd_edge[op] = statistics.fmean(fw)
                fwd_positions[op] = fw

        if len(train_metric) < MIN_ELIGIBLE:
            t += STEP_DAYS * DAY
            continue

        # baseline: mean forward net edge of ALL eligible operators present in fwd
        base_vals = [fwd_edge[o] for o in train_metric if o in fwd_edge]
        if not base_vals:
            t += STEP_DAYS * DAY
            continue
        baseline = statistics.fmean(base_vals)

        ranked = sorted(train_metric.items(), key=lambda kv: kv[1], reverse=True)
        top = [o for o, _ in ranked[:TOP_K]]
        top_vals = [fwd_edge[o] for o in top if o in fwd_edge]
        if not top_vals:
            t += STEP_DAYS * DAY
            continue
        top_fwd = statistics.fmean(top_vals)
        for o in top:
            all_top_fwd_positions.extend(fwd_positions.get(o, []))

        # Spearman: training metric vs forward realized net edge
        pairs = [(train_metric[o], fwd_edge[o]) for o in train_metric if o in fwd_edge]
        if len(pairs) >= 5:
            rho, rho_p = stats.spearmanr([p[0] for p in pairs], [p[1] for p in pairs])
        else:
            rho, rho_p = float("nan"), float("nan")

        per_window_rho.append(rho)
        per_window_gap.append(top_fwd - baseline)
        per_window_top.append(top_fwd)
        per_window_base.append(baseline)
        n_windows += 1

        te = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
        print(f"{te:<12}{len(train_metric):>9}{top_fwd:>+15.4f}"
              f"{baseline:>+14.4f}{top_fwd-baseline:>+10.4f}{rho:>+9.3f}{rho_p:>9.3f}")
        t += STEP_DAYS * DAY

    # ── verdict statistics ─────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print(f"FINAL VERDICT STATISTICS  ({n_windows} windows | operator-pooled | "
          f"copyable-band | net-of-fee)")
    print("=" * 64)

    rho_arr = np.array([r for r in per_window_rho if not np.isnan(r)])
    gap_arr = np.array(per_window_gap)
    top_arr = np.array(per_window_top)

    # one-sample t-tests vs 0
    t_rho, p_rho = stats.ttest_1samp(rho_arr, 0.0)
    t_gap, p_gap = stats.ttest_1samp(gap_arr, 0.0)
    t_top, p_top = stats.ttest_1samp(top_arr, 0.0)
    lo, hi = boot_ci(all_top_fwd_positions)

    print(f"\nTop-50 forward NET edge (per-window mean): "
          f"{top_arr.mean():+.4f}  (t={t_top:.2f}, p={p_top:.3f} vs 0)")
    print(f"  pooled bootstrap 95% CI on all top-50 fwd positions: [{lo:+.4f}, {hi:+.4f}]")
    print(f"Baseline forward NET edge (per-window mean): "
          f"{np.array(per_window_base).mean():+.4f}")
    print(f"Top-50 minus baseline GAP: {gap_arr.mean():+.4f}  "
          f"(t={t_gap:.2f}, p={p_gap:.3f} vs 0)  "
          f"-- positive+significant => ranking adds value")
    print(f"Mean per-window Spearman: {rho_arr.mean():+.4f}  "
          f"(t={t_rho:.2f}, p={p_rho:.3f} vs 0)  "
          f"-- positive+significant => metric is predictive")
    print(f"Windows with positive gap: {(gap_arr > 0).sum()}/{len(gap_arr)}")
    print(f"Windows with positive rho: {(rho_arr > 0).sum()}/{len(rho_arr)}")

    go = (top_arr.mean() > 0 and gap_arr.mean() > 0 and p_gap < 0.05
          and rho_arr.mean() > 0 and p_rho < 0.05 and lo > 0)
    print("\n" + ("VERDICT: GO  - operator-level edge persists in the copyable band."
                  if go else
                  "VERDICT: STOP - operator-level edge does NOT persist in the "
                  "copyable band.\n          The Winner-Follow leader-selection premise "
                  "is unsupported by the data."))


if __name__ == "__main__":
    main()
