"""Measure live-data feed latency: trade timestamp -> frame receipt (#530 Phase 3)."""
import asyncio, json, time
import websockets

async def main():
    lags = []
    payload_keys = set()
    sample = None
    async with websockets.connect("wss://ws-live-data.polymarket.com", open_timeout=15, ping_interval=10) as ws:
        await ws.send(json.dumps({"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades"}]}))
        end = time.time() + 120
        while time.time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(1, end - time.time()))
            except asyncio.TimeoutError:
                break
            now = time.time()
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for it in (msg if isinstance(msg, list) else [msg]):
                if not isinstance(it, dict) or it.get("topic") != "activity":
                    continue
                p = it.get("payload") or {}
                payload_keys.update(p.keys())
                ts = p.get("timestamp")
                if sample is None and p.get("proxyWallet"):
                    sample = {k: str(v)[:70] for k, v in p.items()}
                if isinstance(ts, (int, float)) and ts > 0:
                    t = ts / 1000.0 if ts > 1e12 else float(ts)
                    lag = now - t
                    if -5 < lag < 600:
                        lags.append(lag)
    print("payload keys:", sorted(payload_keys))
    print("sample trade:", json.dumps(sample, indent=1)[:900])
    lags.sort()
    n = len(lags)
    if n:
        q = lambda p: lags[min(n - 1, int(n * p))]
        print(f"n={n} lag_s p50={q(.5):.2f} p90={q(.9):.2f} p95={q(.95):.2f} p99={q(.99):.2f} max={lags[-1]:.2f} min={lags[0]:.2f}")
    else:
        print("no timestamped trades captured")

asyncio.run(main())
