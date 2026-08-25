"""Read-only probe: do Polymarket real-time feeds carry wallet attribution? (#530 Phase 3)"""
import asyncio, json, sys
import websockets

ASSETS = [
    "114685525251284192849243166589757282977015929729810490473148181460119477526193",
    "12053056217068383476156887718679104051518485295387603291161271603503170433366",
]
WALLETISH = ("wallet", "user", "maker", "taker", "address", "proxy", "owner", "account", "pseudonym", "name", "profile")

def scan(obj, path=""):
    """Yield (path, key, value-preview) for wallet-ish keys anywhere in a JSON tree."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if any(w in k.lower() for w in WALLETISH):
                yield (path, k, str(v)[:60])
            yield from scan(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:3]):
            yield from scan(v, f"{path}[{i}]")

async def probe(name, url, subscriptions, seconds):
    print(f"\n=== {name} :: {url}")
    types = {}
    hits = {}
    try:
        async with websockets.connect(url, open_timeout=15, ping_interval=10) as ws:
            for sub in subscriptions:
                await ws.send(json.dumps(sub))
            end = asyncio.get_event_loop().time() + seconds
            while asyncio.get_event_loop().time() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(1, end - asyncio.get_event_loop().time()))
                except asyncio.TimeoutError:
                    break
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    print("  non-json frame:", str(raw)[:100]); continue
                items = msg if isinstance(msg, list) else [msg]
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    et = it.get("event_type") or it.get("type") or it.get("topic") or "?"
                    types[et] = types.get(et, 0) + 1
                    if types[et] <= 2:
                        print(f"  [{et}] keys: {sorted(it.keys())[:14]}")
                    for p, k, v in scan(it):
                        hits.setdefault(f"{et}{p}.{k}", set()).add(v)
    except Exception as e:
        print(f"  CONNECT/RECV ERROR: {type(e).__name__}: {e}")
    print(f"  event counts: {types}")
    if hits:
        print("  WALLET-ISH FIELDS FOUND:")
        for k, vs in sorted(hits.items()):
            print(f"    {k} -> {list(vs)[:2]}")
    else:
        print("  no wallet-ish fields observed")

async def main():
    await probe(
        "CLOB market channel",
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        [{"assets_ids": ASSETS, "type": "market"}],
        75,
    )
    for url, subs, label in [
        ("wss://ws-live-data.polymarket.com", [
            {"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades"}]},
            {"type": "subscribe", "channel": "activity"},
            {"topics": ["activity/trades"], "type": "subscribe"},
        ], "UI live-data (guessed protocols)"),
    ]:
        await probe(label, url, subs, 60)

asyncio.run(main())
