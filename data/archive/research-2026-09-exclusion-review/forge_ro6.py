import sqlite3, json, time, sys, os, glob
out={"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
csv_addrs=set(l.strip() for l in sys.stdin if l.strip())
out["csv_addr_count"]=len(csv_addrs)
c=sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.db?mode=ro", uri=True); c.row_factory=sqlite3.Row
tomb={r[0]:(r[1],r[2]) for r in c.execute("select wallet_hex, reason, purged_at_unix from purged_wallets")}
infra=[w for w,(r,_) in tomb.items() if r=='infra']
in_csv=[w for w in csv_addrs if w in tomb]
out["csv_tombstoned"]=len(in_csv); out["csv_tombstoned_by_reason"]={}
for w in in_csv: out["csv_tombstoned_by_reason"][tomb[w][0]]=out["csv_tombstoned_by_reason"].get(tomb[w][0],0)+1
out["csv_not_tombstoned"]=len(csv_addrs)-len(in_csv)
out["infra_tombstones_total"]=len(infra); out["infra_tombstones_outside_csv"]=sum(1 for w in infra if w not in csv_addrs)
exch=[w for w in ["0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e","0xc5d563a36ae78145c45a50134d48a1215220f80a","0xe111180000d2663c0091e4f400237545b87b996b","0xe2222d279d744050d28e00520010520000310f59"]]
out["exchange_tombstones"]={w: tomb.get(w) for w in exch}
flagged=[r[0] for r in c.execute("select wallet_hex from wallets where is_infra=1")]
out["flagged_count"]=len(flagged)
# archive: exact infra-CSV class cost + five largest CSV members
a=sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.purge-archive.db?mode=ro", uri=True); a.row_factory=sqlite3.Row
rows=[(r[0], r[1], r[2]) for r in a.execute("select wallet_hex, trades_archived, purged_at_unix from purge_manifest where reason='infra'")]
csv_rows=[r for r in rows if r[0] in csv_addrs]; non=[r for r in rows if r[0] not in csv_addrs]
out["archive_infra_csv_members"]={"n": len(csv_rows), "with_trades": sum(1 for r in csv_rows if r[1]>0), "trades": sum(r[1] for r in csv_rows), "zero_trades": sum(1 for r in csv_rows if r[1]==0)}
out["archive_infra_non_csv"]={"n": len(non), "with_trades": sum(1 for r in non if r[1]>0), "trades": sum(r[1] for r in non)}
big=sorted(csv_rows, key=lambda r:-r[1])[:5]
def windows(w):
    ts=[r[0] for r in a.execute("select timestamp_unix from trades where wallet_hex=? order by timestamp_unix asc",(w,))]
    n=len(ts)
    if n<500: return {"n":n}
    sp=[ts[i+499]-ts[i] for i in range(n-499)]
    return {"n":n,"min_window_span":min(sp),"dense_windows":sum(1 for s in sp if s<3600),"first_ts":ts[0],"last_ts":ts[-1]}
out["biggest_csv_members"]=[{"wallet":r[0],"trades_archived":r[1], **windows(r[0])} for r in big]
# probe-shape controls: infra manifest rows NOT in csv with trades
ctrl=[r for r in non if r[1]>0]
out["non_csv_infra_with_trades"]=[{"wallet":r[0],"trades_archived":r[1], **windows(r[0])} for r in ctrl]
# pending publication request check
pend="/home/pi/prediction-markets/data/eval-results/rank_and_push.pending"
out["pending_marker_exists"]=os.path.exists(pend)
if os.path.exists(pend):
    ref=open(pend).read().strip(); out["pending_marker"]=ref
    cand=[ref] if os.path.isabs(ref) else glob.glob("/home/pi/prediction-markets/"+ref)+glob.glob("/home/pi/prediction-markets/data/eval-results/"+ref)
    for f in cand:
        if os.path.isfile(f):
            try:
                d=json.load(open(f)); ents=d.get("entries", d if isinstance(d,list) else [])
                ws=[str(e.get("wallet_hex","")).lower() for e in ents]
                out["pending_request"]={"file":f,"entries":len(ws),"flagged_overlap":sum(1 for w in ws if w in set(flagged)),"tombstone_overlap":sum(1 for w in ws if w in tomb)}
            except Exception as e: out["pending_request_err"]=str(e)
# newest run dir ranked csv overlap
dirs=sorted(glob.glob("/home/pi/prediction-markets/data/eval-results/cron-*"))
if dirs:
    d=dirs[-1]; out["newest_run_dir"]=d; out["newest_run_files"]=sorted(os.listdir(d))[:40]
    import csv as _csv
    for fn in ("ranking_latency_2s.csv","ranking.csv","ranked_wallets.csv"):
        p=os.path.join(d,fn)
        if os.path.exists(p):
            ws=[ (r.get("wallet_hex") or r.get("wallet") or "").lower() for r in _csv.DictReader(open(p))]
            out["newest_ranked"]={"file":p,"rows":len(ws),"flagged_overlap":sum(1 for w in ws if w in set(flagged)),"tombstone_overlap":sum(1 for w in ws if w in tomb)}
            break
print(json.dumps(out))
