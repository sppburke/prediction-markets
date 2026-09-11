import sqlite3, json, time
out={"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
c=sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.db?mode=ro", uri=True); c.row_factory=sqlite3.Row
ws=[r[0] for r in c.execute("select wallet_hex from wallets where is_infra=1")]
tot=0; per=[]
for w in ws:
    n=c.execute("select count(*) from trades where wallet_hex=?", (w,)).fetchone()[0]; tot+=n
    if n: per.append((w,n))
out["flagged_wallets"]=len(ws); out["flagged_trade_rows"]=tot; out["flagged_with_trades"]=per
out["flagged_discovered_range"]=[dict(r) for r in c.execute("select min(discovered_at_unix) mn, max(discovered_at_unix) mx from wallets where is_infra=1")]
print(json.dumps(out))
