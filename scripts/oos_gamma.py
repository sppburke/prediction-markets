#!/usr/bin/env python3
"""
True post-selection OOS test for the 72hr buy-and-hold band survivors.

Dune/CLOB resolution sources are unavailable on this setup (Dune subscription tier 400;
CLOB cursor at terminator) and Polygon RPC stalls on the free-tier 10-block cap. But Gamma
exposes the winner for closed markets (umaResolutionStatus=resolved, outcomePrices index ==1),
validated 64/64 vs cache. So: fetch winners for markets the universe entered AFTER the
2026-05-28 universe-selection date that aren't resolved here yet, insert them (source='gamma',
resolved_at≈end_date), then measure the SELECTED cohort's copy-and-hold edge on those
genuinely out-of-sample entries.
"""
from __future__ import annotations
import sqlite3, urllib.request, json, time, math
import numpy as np, pandas as pd

DB="data/wallet_cache.db"; UA={"User-Agent":"Mozilla/5.0 pe-oos"}
UNIV="data/archive/research-2026-05/watchlist-20260528T194034Z-gbm_bhq_intersection_3.txt"
SURV="data/eval-results/robust-clean/survivors_clean-band-0.40-0.80.txt"
ELIG="data/eval-results/robust-clean/robust_eligible_clean-band-0.40-0.80.csv"
SELECT_CUT=int(pd.Timestamp("2026-05-28",tz="UTC").timestamp())   # universe selected here
WIN_END  =int(pd.Timestamp("2026-06-05",tz="UTC").timestamp())
ENTER_AFTER=int(pd.Timestamp("2026-05-20",tz="UTC").timestamp())  # scan recent buys from here
TTR=72*3600; SLIP=0.01; BLO,BHI=0.40,0.80

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
def tstat(x):
    x=np.asarray(x,float); n=len(x)
    if n<2: return float("nan")
    sd=x.std(ddof=1); return float(x.mean()/sd*math.sqrt(n)) if sd>0 else float("nan")
def gfetch(ids):
    url="https://gamma-api.polymarket.com/markets?"+"&".join(f"condition_ids={i}" for i in ids)+"&closed=true&limit=200"
    return json.load(urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=45))
def winner_from(m):
    if m.get("umaResolutionStatus")!="resolved": return None
    op=m.get("outcomePrices")
    if isinstance(op,str): op=json.loads(op)
    if not op: return None
    for i,v in enumerate(op):
        try:
            if float(v)>=0.99: return i
        except: pass
    return None

conn=sqlite3.connect(DB,timeout=120)
if int(conn.execute("PRAGMA user_version").fetchone()[0]) == -2:
    conn.close()
    raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
conn.execute("PRAGMA busy_timeout=120000;")
wallets=[l.strip().lower() for l in open(UNIV) if l.startswith("0x")]
sched={m:e for m,e in conn.execute("SELECT market_id,end_date_unix FROM market_schedules WHERE end_date_unix IS NOT NULL")}
resolved=set(m for (m,) in conn.execute("SELECT market_id FROM market_resolutions WHERE winning_outcome_id IS NOT NULL"))

log("finding recently-entered unresolved markets ...")
recent=set()
for w in wallets:
    for (mid,) in conn.execute("SELECT DISTINCT market_id FROM trades WHERE wallet_hex=? AND side='buy' AND timestamp_unix>?",(w,ENTER_AFTER)):
        recent.add(mid)
target=[m for m in recent if m in sched and m not in resolved]
log(f"recent distinct markets={len(recent):,} | OOS fetch target (end_date known, unresolved)={len(target):,}")

# fetch winners from Gamma, insert as resolutions
now=int(time.time()); ins=[]; t0=time.time()
for j in range(0,len(target),50):
    batch=target[j:j+50]
    try: data=gfetch(batch)
    except Exception as e:
        log(f"  batch {j} error {e}"); continue
    for m in data:
        cid=m.get("conditionId"); w=winner_from(m)
        if cid and w is not None:
            ins.append((cid,int(w),int(sched.get(cid,now)),now))
    if (j//50)%10==0: log(f"  fetched {min(j+50,len(target))}/{len(target)} ...")
log(f"gamma resolved {len(ins):,} of {len(target):,} target markets ({time.time()-t0:.0f}s)")
conn.executemany("INSERT OR IGNORE INTO market_resolutions (market_id,winning_outcome_id,resolved_at_unix,fetched_at_unix,source) VALUES (?,?,?,?,'gamma')",ins)
conn.commit()
res={m:(w,r) for m,w,r in conn.execute("SELECT market_id,winning_outcome_id,resolved_at_unix FROM market_resolutions WHERE winning_outcome_id IS NOT NULL")}
log(f"resolutions now total {len(res):,}")

# measure copy-and-hold edge on entries STRICTLY AFTER the selection date
rows=[]
for w in wallets:
    bymkt={}
    for mid,oid,side,ps,ts in conn.execute("SELECT market_id,outcome_id,side,price_str,timestamp_unix FROM trades WHERE wallet_hex=? AND side='buy' AND timestamp_unix>?",(w,SELECT_CUT)):
        if mid not in bymkt: bymkt[mid]=(int(oid),ps,ts)
    for mid,(oid,ps,ts) in bymkt.items():
        end=sched.get(mid); r=res.get(mid)
        if end is None or r is None: continue
        if not (SELECT_CUT<ts<WIN_END): continue
        if not (0<end-ts<TTR): continue
        try: p=float(ps)
        except: continue
        if not (0<p<1): continue
        win,_=r; eff=min(p+SLIP,0.999)
        payoff=1.0 if oid==int(win) else 0.0; hold=(payoff-eff)/eff
        rows.append((w,p,payoff,hold))
df=pd.DataFrame(rows,columns=["wallet","price","payoff","hold"])
band=df[(df.price>=BLO)&(df.price<=BHI)]
surv=set(l.strip().lower() for l in open(SURV) if l.startswith("0x"))
elig=set(pd.read_csv(ELIG).wallet.str.lower())
print("\n=== TRUE OOS: entries AFTER 2026-05-28 (resolved via Gamma) ===")
print(f"post-selection band positions: {len(band):,} across {band.wallet.nunique()} wallets")
for name,wset in [("13 band SURVIVORS",surv),("all band-eligible",elig),("whole universe(band)",set(wallets))]:
    d=band[band.wallet.isin(wset)]
    if len(d):
        print(f"  {name:24s}: n={len(d):4d} wallets={d.wallet.nunique():3d} hold_edge={d.hold.mean():+.4f} hit={d.payoff.mean():.3f} tstat={tstat(d.hold):+.2f}")
    else:
        print(f"  {name:24s}: no positions")
