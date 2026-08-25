"""Overnight soak of wss://ws-live-data.polymarket.com (#530 evidence).

Usage: soak_activity_ws.py [out_dir] [hours] [bench_wallets.json]
The optional bench file maps lowercased wallet hex -> bool (survivor); matching
trades are logged in full as reaction-time evidence. Original 14h run (2026-08-24):
stream live only ~113/840 min on ping-alive sockets - the zombie-subscription
finding behind the resubscribe policy in source-polymarket-public/activity_ws.rs.

Measures: connection stability (disconnects, reconnect time, silence gaps), schema drift
(payload key-set changes), latency drift (per-minute aggregates of trade ts -> receipt),
and REAL reaction-time capture: any trade by a batch-66 bench wallet is logged in full.
JSONL heartbeat per minute; runs until killed or --hours elapse.
"""
import asyncio, json, sys, time
from pathlib import Path

import websockets

OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("soak530")
HOURS = float(sys.argv[2]) if len(sys.argv) > 2 else 14.0
OUT.mkdir(exist_ok=True)
BENCH_PATH = Path(sys.argv[3]) if len(sys.argv) > 3 else None
BENCH = json.load(open(BENCH_PATH)) if BENCH_PATH and BENCH_PATH.exists() else {}
LOG = open(OUT / f"soak-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl", "a", buffering=1)

def emit(kind, **kw):
    LOG.write(json.dumps({"t": round(time.time(), 3), "kind": kind, **kw}) + "\n")

async def run():
    deadline = time.time() + HOURS * 3600
    known_keys = None
    connects = 0
    while time.time() < deadline:
        t0 = time.time()
        try:
            async with websockets.connect(
                "wss://ws-live-data.polymarket.com", open_timeout=15, ping_interval=20, ping_timeout=20
            ) as ws:
                connects += 1
                emit("connect", n=connects, dial_secs=round(time.time() - t0, 3))
                await ws.send(json.dumps({"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades"}]}))
                minute = int(time.time() // 60)
                lags, count, last_frame = [], 0, time.time()
                while time.time() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        emit("silence", gap_secs=round(time.time() - last_frame, 1))
                        continue
                    now = time.time()
                    last_frame = now
                    try:
                        msg = json.loads(raw)
                    except (ValueError, TypeError):
                        emit("bad_frame", head=str(raw)[:80]); continue
                    for it in (msg if isinstance(msg, list) else [msg]):
                        if not isinstance(it, dict) or it.get("topic") != "activity":
                            continue
                        p = it.get("payload") or {}
                        count += 1
                        ks = set(p.keys())
                        if known_keys is None:
                            known_keys = set(ks)
                            emit("schema", keys=sorted(ks))
                        elif not ks <= known_keys:
                            emit("schema_new_keys", added=sorted(ks - known_keys))
                            known_keys |= ks
                        ts = p.get("timestamp")
                        try:
                            t = float(ts); t = t / 1000.0 if t > 1e12 else t
                            if -5 < now - t < 600:
                                lags.append(now - t)
                        except (TypeError, ValueError):
                            pass
                        w = str(p.get("proxyWallet", "")).lower()
                        if w in BENCH:
                            emit("bench_hit", survivor=BENCH[w], wallet=w,
                                 lag_secs=round(now - t, 3) if lags else None,
                                 side=p.get("side"), price=p.get("price"), size=p.get("size"),
                                 outcome=p.get("outcome"), condition=p.get("conditionId"),
                                 title=str(p.get("title", ""))[:60], tx=p.get("transactionHash"))
                    m = int(time.time() // 60)
                    if m != minute:
                        if lags:
                            lags.sort()
                            n = len(lags)
                            emit("minute", trades=count, p50=round(lags[n // 2], 3),
                                 p95=round(lags[int(n * 0.95)], 3), max=round(lags[-1], 3))
                        else:
                            emit("minute", trades=count)
                        minute, lags, count = m, [], 0
        except Exception as e:
            emit("disconnect", error=f"{type(e).__name__}: {e}")
            await asyncio.sleep(3)
    emit("done", connects=connects)

asyncio.run(run())
