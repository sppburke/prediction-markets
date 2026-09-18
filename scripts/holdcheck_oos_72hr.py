#!/usr/bin/env python3
"""
(a) Adverse-selection check + (b) walk-forward OOS for the 72hr buy-and-hold cohort,
on the CLEAN, leak-free reference (TTR vs scheduled end_date only).

For each wallet's FIRST buy into a market entered <72h before the SCHEDULED end_date
(market resolved, valid price, in window), we compute:
  - hold_ret  = (payoff - eff)/eff, eff=min(price+slip,0.999)   [copy-and-hold edge = option 2]
  - whether the wallet SOLD that outcome before resolution (exit_frac), and their realized return.

(a) Compare hold_ret of HELD vs SOLD positions (per group). sold>=held => no adverse selection
    from the wallet's exit timing (safe to copy-and-hold their entries).
(b) Train(entry month <= --train-end) select / Test(> ) measure: pick a cohort by train hold-edge
    t-stat, report the cohort's copy-and-hold edge on the held-out test window.
"""
from __future__ import annotations
import argparse, math, time
import numpy as np, pandas as pd, sqlite3

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def tstat(x):
    x=np.asarray(x,float); n=len(x)
    if n<2: return float("nan")
    sd=x.std(ddof=1)
    return float(x.mean()/sd*math.sqrt(n)) if sd>0 else float("nan")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", default="data/wallet_cache.db")
    ap.add_argument("--universe", default="data/archive/research-2026-05/watchlist-20260528T194034Z-gbm_bhq_intersection_3.txt")
    ap.add_argument("--win-start", default="2025-06-01"); ap.add_argument("--win-end", default="2026-05-26")
    ap.add_argument("--ttr-hours", type=float, default=72.0); ap.add_argument("--slip", type=float, default=0.01)
    ap.add_argument("--exit-threshold", type=float, default=0.5)
    ap.add_argument("--band-lo", type=float, default=0.40); ap.add_argument("--band-hi", type=float, default=0.80)
    ap.add_argument("--train-end", default="2026-02")   # entries in month <= this = TRAIN
    ap.add_argument("--min-n", type=int, default=30); ap.add_argument("--min-split-n", type=int, default=10)
    a=ap.parse_args()
    WS=int(pd.Timestamp(a.win_start,tz="UTC").timestamp()); WE=int(pd.Timestamp(a.win_end,tz="UTC").timestamp())
    TTR=int(a.ttr_hours*3600); SLIP=a.slip
    wallets=[l.strip().lower() for l in open(a.universe) if l.startswith("0x")]
    c=sqlite3.connect(f"file:{a.db}?mode=ro",uri=True)
    if int(c.execute("PRAGMA user_version").fetchone()[0]) == -2:
        c.close()
        raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    log("loading resolutions + schedules ...")
    res={m:(w,r) for m,w,r in c.execute("SELECT market_id,winning_outcome_id,resolved_at_unix FROM market_resolutions WHERE winning_outcome_id IS NOT NULL")}
    sch={m:e for m,e in c.execute("SELECT market_id,end_date_unix FROM market_schedules WHERE end_date_unix IS NOT NULL")}
    log(f"  res={len(res):,} sched={len(sch):,}")

    rows=[]; t0=time.time()
    for i,w in enumerate(wallets):
        bymkt={}
        for mid,oid,side,ps,contracts,ts in c.execute(
            "SELECT market_id,outcome_id,side,price_str,contracts,timestamp_unix FROM trades WHERE wallet_hex=? ORDER BY timestamp_unix, source_trade_id",(w,)):
            bymkt.setdefault(mid,[]).append((ts,side,int(oid),ps,int(contracts)))
        for mid,trs in bymkt.items():
            end=sch.get(mid); r=res.get(mid)
            if end is None or r is None: continue
            # first buy = entry
            entry=next((t for t in trs if t[1]=='buy'), None)
            if entry is None: continue
            ts0,_,oid0,ps0,c0=entry
            if not (WS<=ts0<WE): continue
            ttr=end-ts0
            if not (0<ttr<TTR): continue
            try: p0=float(ps0)
            except: continue
            if not (0.0<p0<1.0) or c0<=0: continue
            win,rat=r
            payoff=1.0 if oid0==int(win) else 0.0
            eff=min(p0+SLIP,0.999); hold=(payoff-eff)/eff
            # sells of the SAME outcome after entry, before resolution
            sold_c=0; proceeds=0.0
            for ts,side,oid,ps,cc in trs:
                if side=='sell' and oid==oid0 and ts0<ts<=rat:
                    try: sp=float(ps)
                    except: continue
                    sold_c+=cc; proceeds+=cc*sp
            sold_eff=min(sold_c,c0); held_c=c0-sold_eff
            exit_frac=sold_eff/c0
            avg_sp=(proceeds/sold_c) if sold_c>0 else 0.0
            cost=c0*p0
            realized=((sold_eff*avg_sp + held_c*payoff) - cost)/cost if cost>0 else float("nan")
            rows.append((w,p0,payoff,hold,realized,exit_frac,
                         pd.to_datetime(ts0,unit="s").to_period("M").strftime("%Y-%m")))
    df=pd.DataFrame(rows,columns=["wallet","price","payoff","hold","realized","exit_frac","month"])
    log(f"qualifying first-buys: {len(df):,} across {df.wallet.nunique()} wallets ({time.time()-t0:.0f}s)")

    band=df[(df.price>=a.band_lo)&(df.price<=a.band_hi)].copy()
    surv=set(l.strip().lower() for l in open("data/eval-results/robust-clean/survivors_clean-band-0.40-0.80.txt") if l.startswith("0x"))

    def report_group(name, d):
        held=d[d.exit_frac<a.exit_threshold]; sold=d[d.exit_frac>=a.exit_threshold]
        print(f"\n### (a) {name}: {len(d):,} positions, {d.wallet.nunique()} wallets")
        print(f"  sold-before-resolution: {100*len(sold)/max(1,len(d)):.1f}% of positions")
        print(f"  HELD : n={len(held):,} hold_edge={held.hold.mean():+.4f} hit={held.payoff.mean():.3f}")
        print(f"  SOLD : n={len(sold):,} hold_edge={sold.hold.mean():+.4f} hit={sold.payoff.mean():.3f}")
        print(f"  adverse-selection delta (sold-held hold_edge) = {sold.hold.mean()-held.hold.mean():+.4f}  "
              f"({'SAFE: exits not worse to hold' if sold.hold.mean()>=held.hold.mean() else 'CAUTION: their exits predict worse holds'})")
        print(f"  copy-and-hold edge (option 2) = {d.hold.mean():+.4f} | wallet REALIZED edge (option 1) = {d.realized.mean():+.4f}")

    print("="*72); print("(a) ADVERSE-SELECTION / HELD-vs-SOLD  (band %.2f-%.2f)"%(a.band_lo,a.band_hi))
    report_group("ALL band-eligible positions", band)
    report_group("13 band SURVIVORS only", band[band.wallet.isin(surv)])

    # (b) walk-forward: select on train, measure on test
    print("\n"+"="*72); print(f"(b) WALK-FORWARD OOS  (train month<= {a.train_end}, test >)")
    band["is_test"]=band.month> a.train_end
    g=band.groupby("wallet")
    tr=g.apply(lambda x: pd.Series({"n_tr":(~x.is_test).sum(),"e_tr":x.loc[~x.is_test,"hold"].mean(),
                                    "t_tr":tstat(x.loc[~x.is_test,"hold"]),
                                    "n_te":x.is_test.sum(),"e_te":x.loc[x.is_test,"hold"].mean()}), include_groups=False).reset_index()
    elig=tr[(tr.n_tr>=a.min_n)&(tr.n_te>=a.min_split_n)].copy()
    # cohort selected ON TRAIN: positive train edge & train t-stat>=2
    cohort=elig[(elig.e_tr>0)&(elig.t_tr>=2.0)]
    print(f"  wallets with enough train&test: {len(elig)} | train-selected cohort (e_tr>0 & t_tr>=2): {len(cohort)}")
    if len(cohort):
        # measure cohort's copy-and-hold edge on TEST positions
        te=band[band.wallet.isin(set(cohort.wallet))&band.is_test]
        print(f"  TRAIN-selected cohort on TEST: n_pos={len(te):,} wallets={te.wallet.nunique()} "
              f"hold_edge={te.hold.mean():+.4f} hit={te.payoff.mean():.3f} tstat={tstat(te.hold):+.2f}")
        # baseline: all band-eligible on test
        allte=band[band.is_test]
        print(f"  baseline (all band-eligible) on TEST: hold_edge={allte.hold.mean():+.4f}")
        print(f"  >> {'FORWARD-POSITIVE: train selection carried positive test edge' if te.hold.mean()>0 else 'FORWARD-NEGATIVE: train edge did not persist'}")
        print(f"  train→test wallet edge corr (Spearman) = {elig.e_tr.corr(elig.e_te,method='spearman'):+.3f}")
    print("\nNOTE: resolutions end ~2026-05-25, so this TEST window is ~2026-03..mid-May, NOT post-2026-05-28.")
    print("A true post-selection OOS test requires refreshing resolutions forward.")

if __name__=="__main__": main()
