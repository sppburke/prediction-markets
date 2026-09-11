import sqlite3, json, time
W = """0x1b912b17581b3d544431d13d5a060155a80127dc
0xfd8e46519d0a8f9c35e5010ef4e7f56f7583aea4
0x72e1597864456eda62878413cf3e60c332e4a45d
0x0bf96a1c55e6f47ea84335bc3fc08a89653efd90
0x5a3354a0a35d00d2a1dbe6cea0e37e9c30a2ca0d
0xdf6539d1fadb951a02a999d31a72e0cd7fd9c36d
0xb7ab821f037a4c8deb9b23b8001a4be985d116d0
0x057621bea5fc03a53af38530c95b17a510716232
0xc561b14904d769eda31628082c005362ba60dc22
0x9fbfe50bf171adaa347a5cb2b789b4a6e12ef003
0x31646b754f77b973910e7376b7015cb81fe83c65
0x38d812aff0b79f3bf5da2a477f780bcc163eea7c
0x9c76cdb43fb46454da005fbc82047a64a18ec926
0xdc5bd11896bfb0fb335dad88d99a7e9a6bc3f102
0x330f6bf24e33d8348593bca54017ce83423791b1
0x1f19c48aee80ec95396d91f0d21ac249b8a7f57a
0x06dc51826bc524d9a83770e7de9dd7e005b04524
0x88ec5ba618625d744988f87ab577be53b382ec61
0xdfe29d6ef2a44606cf58967e1fd5854d9e594902
0xaf17116ae2b1476032785a67bd5b7c8c05905c20
0xc33d6aa3eb972639f31e46a6a02201cece380d40
0xa8ae2fb989de545c42c376d61ec887868ca57f3e
0x4e9e342ff236323b43f79c0da642a82bd12f0c30
0xdb5ad26b68d77ae966d29e7180147272ab7a3965
0x5215b36ebe0f78b3114eb998ca8ade49402ab02f
0x8f1dfd0868d056f11f84e0233e1b89527c262fb6
0xc7d02944a76b9f83b199e9090ecc92c82d241f8a
""".split()
out = {"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "threshold_secs": 3600}
a = sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.purge-archive.db?mode=ro", uri=True); a.row_factory = sqlite3.Row
qa = lambda s, p=(): [dict(r) for r in a.execute(s, p).fetchall()]
# controls: infra-tombstoned wallets NOT from any CSV bit that had archived trades, plus the 5 largest CSV-bit infra wallets by trades
ctrl = qa("""select m.wallet_hex, m.trades_archived, w.source_bits, w.is_active from purge_manifest m join wallets w on w.wallet_hex=m.wallet_hex
             where m.reason='infra' and (w.source_bits & 4)=0 and m.trades_archived>0""")
big = qa("""select m.wallet_hex, m.trades_archived, w.source_bits, w.is_active from purge_manifest m join wallets w on w.wallet_hex=m.wallet_hex
            where m.reason='infra' and (w.source_bits & 4)!=0 and m.trades_archived>0 order by m.trades_archived desc limit 5""")
out["controls_probe_flagged"] = ctrl; out["controls_biggest_csv"] = big
def windows(wallet):
    ts = [r[0] for r in a.execute("select timestamp_unix from trades where wallet_hex=? order by timestamp_unix asc", (wallet,))]
    n = len(ts)
    if n < 500: return {"n": n, "min_window_span": None, "windows": 0}
    spans = [ts[i+499]-ts[i] for i in range(n-499)]
    m = min(spans); k = spans.index(m)
    dense = sum(1 for s in spans if s < 3600)
    return {"n": n, "windows": len(spans), "min_window_span": m, "min_window_start_ts": ts[k], "dense_windows": dense,
            "oldest500_span": ts[499]-ts[0], "newest500_span": ts[-1]-ts[-500], "first_ts": ts[0], "last_ts": ts[-1]}
out["cohort"] = {w: windows(w) for w in W}
out["controls"] = {r["wallet_hex"]: windows(r["wallet_hex"]) for r in ctrl + big}
print(json.dumps(out, default=str))
