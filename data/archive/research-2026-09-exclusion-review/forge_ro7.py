import sqlite3, json, time, os, glob, csv
out={"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
c=sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.db?mode=ro", uri=True)
tomb={r[0] for r in c.execute("select wallet_hex from purged_wallets")}
flagged={r[0] for r in c.execute("select wallet_hex from wallets where is_infra=1")}
out["flagged_count"]=len(flagged); out["tombstones"]=len(tomb)
dirs=sorted(glob.glob("/home/pi/prediction-markets/data/eval-results/cron-*"))
checked=[]
for d in dirs[-3:]:
    for p in sorted(glob.glob(d+"/*.csv")):
        try:
            with open(p) as f:
                r=csv.DictReader(f); cols=r.fieldnames or []
                col=next((k for k in cols if k and k.lower() in ("wallet_hex","wallet","address")), None)
                if not col: continue
                ws=[(row.get(col) or "").lower() for row in r]
            checked.append({"file":p,"rows":len(ws),"flagged_overlap":sum(1 for w in ws if w in flagged),"tombstone_overlap":sum(1 for w in ws if w in tomb)})
        except Exception as e:
            checked.append({"file":p,"err":str(e)})
out["ranked_csv_overlap"]=checked
out["pending_marker_exists"]=os.path.exists("/home/pi/prediction-markets/data/eval-results/rank_and_push.pending")
print(json.dumps(out))
