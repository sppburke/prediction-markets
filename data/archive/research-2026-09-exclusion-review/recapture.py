"""Read-only public recapture: for never-fetched cohort wallets, sample 500-trade activity pages at monthly `end` anchors and compute page spans."""
import json, time, urllib.request, hashlib, datetime, sys, os
W = ["0xdf6539d1fadb951a02a999d31a72e0cd7fd9c36d", "0xc561b14904d769eda31628082c005362ba60dc22",
     "0x38d812aff0b79f3bf5da2a477f780bcc163eea7c", "0xdb5ad26b68d77ae966d29e7180147272ab7a3965"]
OUT = "recapture"
now = int(time.time())
anchors = [now - 30*86400*k for k in range(0, 13)]  # now, then monthly back 12 months
res = {"observed_at": datetime.datetime.utcnow().isoformat()+"Z", "endpoint": "https://data-api.polymarket.com/activity?user=<w>&type=TRADE&limit=500&offset=0&end=<unix>", "wallets": {}}
for w in W:
    rows = []
    for k, end in enumerate(anchors):
        url = f"https://data-api.polymarket.com/activity?user={w}&type=TRADE&limit=500&offset=0&end={end}"
        req = urllib.request.Request(url, headers={"User-Agent": "pe-research-589/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                body = r.read()
        except Exception as e:
            rows.append({"end": end, "error": str(e)}); time.sleep(1.0); continue
        h = hashlib.sha256(body).hexdigest()
        fn = f"{OUT}/{w}-end{end}.json"
        open(fn, "wb").write(body)
        try:
            data = json.loads(body)
        except Exception as e:
            rows.append({"end": end, "error": f"json: {e}", "sha256": h}); time.sleep(1.0); continue
        ts = [int(x.get("timestamp")) for x in data if x.get("timestamp") is not None]
        ident_ok = all(str(x.get("proxyWallet","")).lower() == w for x in data)
        n = len(data)
        span = (max(ts) - min(ts)) if ts else None
        buys = sum(1 for x in data if str(x.get("side","")).upper()=="BUY")
        rows.append({"end": end, "end_iso": datetime.datetime.utcfromtimestamp(end).isoformat()+"Z", "n": n, "identity_ok": ident_ok,
                     "min_ts": min(ts) if ts else None, "max_ts": max(ts) if ts else None, "span_secs": span,
                     "span_hours": round(span/3600, 3) if span is not None else None, "buys": buys, "sells": n-buys,
                     "max_le_end": (max(ts) <= end) if ts else None, "rule_fires": (n == 500 and span is not None and span < 3600), "sha256": h, "file": fn})
        time.sleep(0.6)
    res["wallets"][w] = rows
json.dump(res, open(f"{OUT}/summary.json", "w"), indent=1)
for w, rows in res["wallets"].items():
    print(w)
    for r in rows:
        if "error" in r: print("  ", r["end"], "ERROR", r["error"]); continue
        print("  ", r["end_iso"][:10], "n=", r["n"], "span_h=", r["span_hours"], "buys/sells=", r["buys"], r["sells"], "fires=", r["rule_fires"], "ident=", r["identity_ok"], "max<=end=", r["max_le_end"])
