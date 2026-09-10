import sqlite3, json, time, random
out = {"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "threshold_secs": 3600, "seed": 589}
c = sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.db?mode=ro", uri=True); c.row_factory = sqlite3.Row
q = lambda s, p=(): [dict(r) for r in c.execute(s, p).fetchall()]
out["trades_indexes"] = [r["name"] for r in q("select name from sqlite_master where type='index' and tbl_name='trades'")]
out["eligible_pop"] = q("select count(*) n from wallets where is_active=1 and is_infra=0 and trade_count>=500")
# deterministic sample: order by wallet_hex hash-ish via substr; use python random over a bounded id list
ids = [r["wallet_hex"] for r in q("select wallet_hex from wallets where is_active=1 and is_infra=0 and trade_count>=500 order by wallet_hex")]
random.seed(589); sample = random.sample(ids, 300)
t0 = time.time(); res = []
for w in sample:
    ts = [r[0] for r in c.execute("select timestamp_unix from trades where wallet_hex=? order by timestamp_unix asc", (w,))]
    n = len(ts)
    if n < 500: res.append({"w": w, "n": n}); continue
    spans = [ts[i+499]-ts[i] for i in range(n-499)]
    res.append({"w": w, "n": n, "min_win": min(spans), "dense": sum(1 for s in spans if s < 3600), "oldest500": ts[499]-ts[0], "newest500": ts[-1]-ts[-500]})
out["elapsed_s"] = round(time.time()-t0, 1)
out["sample"] = res
print(json.dumps(out))
