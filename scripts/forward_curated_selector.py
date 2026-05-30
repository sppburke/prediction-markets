#!/usr/bin/env python3
"""Forward-valid $1,000 curated-set selector + walk-forward validation, with a
DATA-DRIVEN Kelly fraction (no assumed 0.25).

Selection funnel (as-of cutoff C, all inputs ≤ C — no outcome peeking):
  1. Universe = gated wallets at C (trading_days≥20 AND distinct_events≥10)
  2. ∩ BHq-significant (skill_pvalue_bps FDR gate, q=0.10)
  3. Recency gate: ≥MIN_RECENT trades in (C-30d, C]
  4. Frequency band: trades/day ≤ FREQ_CAP   (keep positions ≥ $5 at $1k)
  5. Rank by bb_shrunk_edge_bps; capital-cap the count so projected concurrent
     positions keep $1,000 above $5/position.

Sizing fraction is DATA-DRIVEN, not assumed:
  - full-Kelly base f* = mean/var estimated from the cohort's PRE-cutoff
    (trailing-90d) per-bet returns — forward-valid (no lookahead).
  - shrinkage multiplier m = t²/(t²+1), t = pre-cutoff edge t-stat
    (signal/(signal+noise)): confident edge → m→1 (full Kelly); noisy → m→0.
    This is the principled forward pick; 0.25 is shown only as a reference point.
  - We also SWEEP f ∈ {0.10,0.25,0.50,1.0}×f* and report forward terminal AND
    max-drawdown per anchor — the growth-vs-risk frontier. (Picking the fraction
    that maximizes forward terminal would be overfit; the frontier is descriptive,
    the shrinkage fraction is the recommendation.)

Validation: select as-of C, compound $1,000 through (C, C+30d] resolved positions
(500 bps haircut), resolution-ordered. Two anchors (April, May-partial). Deploy
anchor emits the wallet hexes + the recommended shrinkage fraction.
"""
import sys
from pathlib import Path
import numpy as np
import sqlite3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from composite_tuner import data as data_mod
from monthly_rerank_gbm import load_features, bhq_significant_mask

B0 = 1000.0
MIN_POS = 5.0
MIN_RECENT = 3
FREQ_CAP = 5.0
LOOKBACK = 90 * 86400
WINDOW = 30 * 86400
ABS_FRACS = [0.005, 0.01, 0.02, 0.05, 0.10, 0.25]  # absolute fractions of current bankroll
ANCHORS = [("April-val", 1775001599, True),
           ("May-val",   1777679999, True),
           ("DEPLOY",     1779839999, False)]


def recent_tpd(db, wallets, cutoff):
    out = {}
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as c:
        c.execute("CREATE TEMP TABLE _u(w TEXT PRIMARY KEY)")
        c.executemany("INSERT OR IGNORE INTO _u VALUES(?)", [(w,) for w in wallets])
        for wh, n in c.execute(
            """SELECT t.wallet_hex, COUNT(*) FROM trades t JOIN _u ON _u.w=t.wallet_hex
               WHERE t.timestamp_unix>? AND t.timestamp_unix<=? GROUP BY t.wallet_hex""",
            (cutoff - WINDOW, cutoff)).fetchall():
            out[wh] = n / 30.0
    return out


def returns_in(db, start, end, wallets):
    pos = data_mod.load_oos_positions(db, start, end, frozenset(wallets), price_haircut_bps=500)
    return sorted(((p.resolved_at_unix, (p.outcome - p.vwap_entry) / p.vwap_entry)
                   for p in pos), key=lambda x: x[0])


def kelly_base_and_shrink(rets):
    """full-Kelly f*=mu/var and shrinkage multiplier m=t^2/(t^2+1) from a
    per-bet return sample (pre-cutoff). Returns (f_star, m, n)."""
    if len(rets) < 5:
        return 0.0, 0.0, len(rets)
    a = np.array([r for _, r in rets]); n = len(a)
    mu, var = float(a.mean()), float(a.var(ddof=1))
    if var <= 0:
        return 0.0, 0.0, n
    f_star = max(0.0, mu / var)
    t = mu / (np.sqrt(var) / np.sqrt(n))      # edge t-stat
    m = float(t * t / (t * t + 1.0)) if t > 0 else 0.0
    return f_star, m, n


def sim(rets, f):
    """Compound %-current at fraction f; return (terminal, max_drawdown)."""
    b, peak, mdd = B0, B0, 0.0
    for _, r in rets:
        stake = f * b
        if stake < MIN_POS:
            continue
        b += stake * r
        if b <= 0:
            return 0.0, 1.0
        peak = max(peak, b)
        mdd = max(mdd, (peak - b) / peak)
    return b, mdd


def select_set(db, cutoff):
    df = load_features(db, cutoff, 20, 10)
    if df.empty:
        return []
    df = df[bhq_significant_mask(df["skill_pvalue_bps"].values)].copy()
    tpd = recent_tpd(db, df["wallet_hex"].tolist(), cutoff)
    df["tpd"] = df["wallet_hex"].map(lambda w: tpd.get(w, 0.0))
    df = df[(df["tpd"] * 30 >= MIN_RECENT) & (df["tpd"] <= FREQ_CAP)].copy()
    df["hold_days"] = (df["avg_hold_secs"].clip(lower=0) / 86400.0).clip(lower=0.05)
    df = df.sort_values("bb_shrunk_edge_bps", ascending=False)
    picked, cumL = [], 0.0
    for _, row in df.iterrows():
        cumL += row["tpd"] * row["hold_days"]
        if cumL > 0 and B0 / cumL < MIN_POS:
            break
        picked.append(row["wallet_hex"])
    return picked


def main():
    db = sys.argv[1] if len(sys.argv) > 1 else "data/wallet_cache.db"
    print(f"FORWARD-VALID $1k selector + DATA-DRIVEN Kelly  "
          f"(recency≥{MIN_RECENT}/30d, freq≤{FREQ_CAP}/d, ${MIN_POS:.0f} floor)\n")
    for label, cutoff, has_fwd in ANCHORS:
        sel = select_set(db, cutoff)
        # data-driven Kelly DIAGNOSTIC from PRE-cutoff (trailing 90d) returns
        pre = returns_in(db, cutoff - LOOKBACK, cutoff, sel)
        pa = np.array([r for _, r in pre]) if pre else np.array([])
        mu_pre = float(pa.mean()) if pa.size else 0.0
        var_pre = float(pa.var(ddof=1)) if pa.size > 1 else 0.0
        f_star, m_shrink, n_pre = kelly_base_and_shrink(pre)
        f_reco = m_shrink * f_star
        print(f"=== {label} (cutoff={cutoff}) === selected {len(sel)} wallets")
        print(f"  pre-cutoff: n={n_pre} mean={mu_pre:+.4f} var={var_pre:.3f} → full-Kelly f*={f_star:.4f} "
              f"shrink={m_shrink:.3f} reco_f={f_reco:.4f}  (f*=0 means pre-edge ≤0)")
        if has_fwd:
            fwd = returns_in(db, cutoff, cutoff + WINDOW, sel)
            if not fwd:
                print("  no resolved forward positions yet"); continue
            fa = np.array([r for _, r in fwd])
            print(f"  FORWARD edge: n_pos={len(fwd)} mean={fa.mean():+.4f} sum={fa.sum():+.1f} "
                  f"(>0 ⇒ cohort made money)  [the actual signal]")
            print(f"  absolute-fraction sweep ($1,000 → terminal / maxDD):")
            for f in ABS_FRACS:
                term, mdd = sim(fwd, f)
                print(f"    f={f:>5.1%} of bankroll: ${term:>11,.0f} ({term/B0-1:+8.1%})  maxDD={mdd:.0%}")
        else:
            out = "data/curated-1k-forward-deploy.txt"
            Path(out).write_text("\n".join(sel) + "\n")
            print(f"  → deployable set: {out}")
    print("\nRead: FORWARD mean>0 ⇒ funnel profitable that month. The abs-fraction sweep is the")
    print("growth/drawdown frontier; pick the fraction by your DD tolerance. f* (Kelly) is a")
    print("separate diagnostic — f*=0 just means the cohort's PRIOR-90d realized edge was ≤0.")
    print("RULE credible only if FORWARD mean edge >0 across BOTH val anchors.")


if __name__ == "__main__":
    main()
