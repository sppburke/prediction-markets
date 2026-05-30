#!/usr/bin/env python3
"""Hindsight-optimal curated subset of the bhq watchlist for $1,000 over April-May.

⚠️  HINDSIGHT / OVERFIT BY CONSTRUCTION. This selects wallets on the realized
April-May outcome it then measures — so the "optimal" set is curve-fit to one
path and has ~no forward predictive value. It answers "what was the ceiling",
not "what should I follow". The forward-honest baseline (GBM file order, no
outcome sort) is printed alongside to show the overfit gap.

Window: entries in (2026-03-31, 2026-05-31], resolved, 500 bps haircut.
May is PARTIAL (month not closed; resolutions not refreshed) — fewer May
positions than will eventually resolve.

Model: follow a wallet = copy ALL its positions. Combined bet stream sorted by
resolved_at_unix, bankroll compounds sequentially (ignores concurrency — a known
simplification; magnitudes are indicative, the flat-vs-compound and the
overfit-gap rankings are the robust signals). Min position $5 (skip below).
"""
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner import data as data_mod

CUTOFF = 1775001599      # 2026-03-31 (window start)
FWD_END = 1780291199     # 2026-05-31T23:59:59Z
HAIRCUT = 500
B0 = 1000.0
MIN_POS = 5.0
KS = [3, 5, 10, 20, 30, 50, 75, 100, 150, 200, 300, 500]


def positions_by_wallet(db, wallets):
    pos = data_mod.load_oos_positions(db, CUTOFF, FWD_END, frozenset(wallets),
                                      price_haircut_bps=HAIRCUT)
    by = {}
    for p in pos:
        by.setdefault(p.wallet_hex, []).append(
            (p.resolved_at_unix, (p.outcome - p.vwap_entry) / p.vwap_entry))
    return by


def sim_stream(stream, rule, frac, b0=B0, min_pos=MIN_POS):
    """stream = list of (resolved_at, r) sorted by time. rule in
    {flat,pct_init,pct_cur,kelly}. Returns terminal bankroll."""
    b = b0
    flat_stake = frac * b0  # for flat / pct_init: constant dollars
    for _, r in stream:
        if rule in ("flat", "pct_init"):
            stake = flat_stake
        else:  # pct_cur / kelly: fraction of CURRENT bankroll
            stake = frac * b
        if stake < min_pos:
            continue
        b += stake * r
        if b <= 0:
            return 0.0
    return b


def full_kelly(stream):
    rs = np.array([r for _, r in stream])
    if len(rs) == 0:
        return 0.0
    v = float(np.var(rs))
    return max(0.0, min(1.0, float(np.mean(rs)) / v)) if v > 0 else 0.0


def evaluate(label, ranked_wallets, by):
    """ranked_wallets: wallet hexes in selection priority order. Sweep K and
    sizing rules; return best (terminal, K, rule, frac)."""
    best = (B0, 0, "none", 0.0)
    print(f"\n--- {label} ---")
    print(f"{'K':>5} {'flat$20':>11} {'%init2%':>11} {'%cur5%':>11} {'%cur10%':>11} {'0.25Kelly':>11} {'fullKelly':>11}")
    for K in KS:
        if K > len(ranked_wallets):
            break
        stream = []
        for w in ranked_wallets[:K]:
            stream.extend(by.get(w, []))
        if not stream:
            continue
        stream.sort(key=lambda x: x[0])
        fk = full_kelly(stream)
        results = {
            "flat$20": sim_stream(stream, "flat", 20.0 / B0),
            "%init2%": sim_stream(stream, "pct_init", 0.02),
            "%cur5%": sim_stream(stream, "pct_cur", 0.05),
            "%cur10%": sim_stream(stream, "pct_cur", 0.10),
            "0.25Kelly": sim_stream(stream, "kelly", 0.25 * fk),
            "fullKelly": sim_stream(stream, "kelly", fk),
        }
        print(f"{K:>5} " + " ".join(f"{results[k]:>11,.0f}" for k in
              ["flat$20","%init2%","%cur5%","%cur10%","0.25Kelly","fullKelly"]))
        for rule, term in results.items():
            if term > best[0]:
                fr = {"flat$20":20/B0,"%init2%":0.02,"%cur5%":0.05,"%cur10%":0.10,
                      "0.25Kelly":0.25*fk,"fullKelly":fk}[rule]
                best = (term, K, rule, fr)
    return best


def main():
    db = sys.argv[1] if len(sys.argv) > 1 else "data/wallet_cache.db"
    wl_path = sys.argv[2]
    wallets = [l.strip() for l in Path(wl_path).read_text().splitlines() if l.strip()]
    print(f"watchlist={wl_path}  n={len(wallets)}  window=2026-04-01..05-31 (May partial)")

    by = positions_by_wallet(db, wallets)
    n_with_pos = len(by)
    tot_pos = sum(len(v) for v in by.values())
    print(f"wallets with >=1 resolved April-May position: {n_with_pos}/{len(wallets)}  total positions={tot_pos}")

    # Per-wallet realized total return (flat, sizing-agnostic) for HINDSIGHT rank.
    wallet_sum = {w: sum(r for _, r in ps) for w, ps in by.items()}
    pos_wallets = [w for w, s in wallet_sum.items() if s > 0]
    print(f"wallets with positive April-May Σr: {len(pos_wallets)}/{n_with_pos}")

    # HINDSIGHT-optimal ranking: by realized total return desc.
    hindsight_rank = sorted(by.keys(), key=lambda w: wallet_sum[w], reverse=True)
    # FORWARD-honest baseline: original watchlist (file) order — no outcome peek.
    forward_order = [w for w in wallets if w in by]

    best_h = evaluate("HINDSIGHT (ranked by realized April-May return — OVERFIT)", hindsight_rank, by)
    best_f = evaluate("FORWARD-HONEST (watchlist file order — no outcome peek)", forward_order, by)

    # Flat additive ceiling: follow ALL positive-Σr wallets, flat $20/bet.
    stream_pos = []
    for w in pos_wallets:
        stream_pos.extend(by[w])
    stream_pos.sort(key=lambda x: x[0])
    flat_all_pos = sim_stream(stream_pos, "flat", 20.0 / B0)

    print("\n================ SUMMARY ================")
    print(f"HINDSIGHT best:  ${best_h[0]:,.0f}  (K={best_h[1]} wallets, rule={best_h[2]}, frac={best_h[3]:.3f})  → {best_h[0]/B0-1:+.0%} on $1,000")
    print(f"FORWARD-honest:  ${best_f[0]:,.0f}  (K={best_f[1]} wallets, rule={best_f[2]}, frac={best_f[3]:.3f})  → {best_f[0]/B0-1:+.0%}")
    print(f"overfit gap: {best_h[0]/max(best_f[0],1e-9):.1f}x")
    print(f"[ref] follow ALL {len(pos_wallets)} positive-Σr wallets, flat $20: ${flat_all_pos:,.0f}")

    # Write the hindsight-optimal subset hexes.
    out = f"data/curated-subset-april-may-hindsight-top{best_h[1]}.txt"
    Path(out).write_text("\n".join(hindsight_rank[:best_h[1]]) + "\n")
    print(f"\nwrote hindsight-optimal subset ({best_h[1]} wallets): {out}")
    print("⚠️  hindsight/overfit — ceiling only, NOT a forward recommendation.")


if __name__ == "__main__":
    main()
