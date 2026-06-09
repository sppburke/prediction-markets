#!/usr/bin/env python3
"""Read-only BTC feed bake-off v2 + Polymarket CLOB book — PRECISE TIMINGS.

Every record carries (a) our receipt time on a single high-res monotonic clock,
stamped at byte-receipt BEFORE JSON parse (so cross-feed comparison is apples to
apples), and (b) the venue's own source timestamp where provided. Persists all
ticks to CSV; reports cadence, basis, per-venue staleness, feed<->feed lead-lag,
move-triggered first-arrival, and BTC-move -> Polymarket-book reprice lag.

stdlib only + curl for SSE. Writes only under ~/feed-bakeoff.
Usage:  python3 feed_bakeoff_v2.py            # collect DUR secs then analyze
        python3 feed_bakeoff_v2.py analyze     # re-analyze existing CSVs only
"""
import asyncio, ssl, os, base64, struct, json, time, sys
from datetime import datetime

DUR = float(os.environ.get("DUR", "7200"))
OUT = os.environ.get("OUT", os.path.expanduser("~/feed-bakeoff/run"))
os.makedirs(OUT, exist_ok=True)
TICKS, BOOK, MKTS = (os.path.join(OUT, x) for x in ("ticks.csv", "book.csv", "markets.csv"))
T0 = time.monotonic(); WALL0 = time.time()
def now_ms(): return (time.monotonic() - T0) * 1000.0  # high-res, single clock

PYTH_BTC = "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
CLOB_HOST, CLOB_PORT, CLOB_PATH = "ws-subscriptions-clob.polymarket.com", 443, "/ws/market"
GAMMA = "https://gamma-api.polymarket.com/events?series_slug=btc-up-or-down-5m&closed=false&limit=40"

def iso_ms(s):
    try: return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception: return None
def _i(v):
    try: return int(v)
    except Exception: return None

_tf = _bf = None
def open_files():
    global _tf, _bf
    _tf = open(TICKS, "w", buffering=1 << 20); _bf = open(BOOK, "w", buffering=1 << 20)
    _tf.write(f"# wall0_ms={WALL0*1000:.0f}\nsrc,t_recv_ms,price,t_exch_ms\n")
    _bf.write(f"# wall0_ms={WALL0*1000:.0f}\nt_recv_ms,token,bid,ask,t_exch_ms\n")
def tick(src, price, t, te=None):
    try: p = float(price)
    except Exception: return
    if p <= 0: return
    _tf.write(f"{src},{t:.3f},{p},{'' if te is None else te}\n")
def book_row(token, bid, ask, t, te=None):
    _bf.write(f"{t:.3f},{token},{'' if bid is None else bid},{'' if ask is None else ask},{'' if te is None else te}\n")

def _mask(d):
    m = os.urandom(4); return m + bytes(b ^ m[i & 3] for i, b in enumerate(d))
async def _send(w, opcode, data=b""):
    ln = len(data); h = bytes([0x80 | opcode])
    if ln < 126: h += bytes([0x80 | ln])
    elif ln < 65536: h += bytes([0x80 | 126]) + struct.pack(">H", ln)
    else: h += bytes([0x80 | 127]) + struct.pack(">Q", ln)
    w.write(h + _mask(data)); await w.drain()
async def connect_ws(host, port, path):
    ctx = ssl.create_default_context()
    r, w = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=ctx, server_hostname=host), 12)
    key = base64.b64encode(os.urandom(16)).decode()
    w.write((f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
             f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
    await w.drain()
    st = await r.readline()
    if b"101" not in st: raise RuntimeError(f"handshake {st!r}")
    while True:
        h = await r.readline()
        if h in (b"\r\n", b"\n", b""): break
    return r, w
async def read_loop(r, w, on_json):
    cur = b""
    while True:
        hd = await r.readexactly(2); b1, b2 = hd[0], hd[1]
        fin, opcode, ln = b1 & 0x80, b1 & 0x0f, b2 & 0x7f
        if ln == 126: ln = struct.unpack(">H", await r.readexactly(2))[0]
        elif ln == 127: ln = struct.unpack(">Q", await r.readexactly(8))[0]
        pl = await r.readexactly(ln) if ln else b""
        t = now_ms()                       # stamp at byte-receipt, before parse
        if opcode == 0x9: await _send(w, 0xA, pl); continue
        if opcode == 0x8: return
        if opcode == 0x0:
            cur += pl
            if fin:
                try: on_json(json.loads(cur.decode("utf-8", "ignore")), t)
                except Exception: pass
                cur = b""
        elif opcode in (0x1, 0x2):
            if fin:
                try: on_json(json.loads(pl.decode("utf-8", "ignore")), t)
                except Exception: pass
            else: cur = pl

async def feed(name, host, port, path, sub, on_json):
    while True:
        try:
            r, w = await connect_ws(host, port, path)
            if sub is not None: await _send(w, 0x1, json.dumps(sub).encode())
            await read_loop(r, w, on_json)
        except asyncio.CancelledError: return
        except Exception as e: print(f"[{name}] {e}", file=sys.stderr)
        await asyncio.sleep(2)

async def pyth():
    while True:
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "curl", "-sN", "-H", "accept: text/event-stream",
                f"https://hermes.pyth.network/v2/updates/price/stream?ids[]={PYTH_BTC}&parsed=true",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            async for line in proc.stdout:
                t = now_ms()
                s = line.decode("utf-8", "ignore").strip()
                if not s.startswith("data:"): continue
                try:
                    pr = json.loads(s[5:])["parsed"][0]["price"]
                    tick("pyth", float(pr["price"]) * (10 ** int(pr["expo"])), t, int(pr["publish_time"]) * 1000)
                except Exception: pass
        except asyncio.CancelledError:
            if proc:
                try: proc.kill()
                except Exception: pass
            return
        except Exception as e: print(f"[pyth] {e}", file=sys.stderr)
        await asyncio.sleep(2)

def _best(levels, hi):
    b = None
    for lv in levels:
        try: pr = float(lv.get("price"))
        except Exception: continue
        if b is None or (hi and pr > b) or (not hi and pr < b): b = pr
    return b
def _popt(v):
    try: return float(v)
    except Exception: return None

class Clob:
    def __init__(self): self.w = None; self.subs = set(); self.last = {}
    def _bk(self, token, bid, ask, t, te=None):
        if token is None: return
        key = (bid, ask)
        if self.last.get(token) == key: return    # log only quote CHANGES (reprices)
        self.last[token] = key
        book_row(token, bid, ask, t, te)
    def on_json(self, j, t):
        if isinstance(j, list):
            for el in j:
                if isinstance(el, dict) and ("bids" in el or "asks" in el):
                    self._bk(el.get("asset_id"), _best(el.get("bids", []), True), _best(el.get("asks", []), False), t, _i(el.get("timestamp")))
        elif isinstance(j, dict) and "price_changes" in j:
            for pc in j["price_changes"]:
                self._bk(pc.get("asset_id"), _popt(pc.get("best_bid")), _popt(pc.get("best_ask")), t, None)
    async def run(self):
        while True:
            try:
                r, w = await connect_ws(CLOB_HOST, CLOB_PORT, CLOB_PATH)
                self.w = w
                if self.subs: await self._sub()
                await read_loop(r, w, self.on_json)
            except asyncio.CancelledError: return
            except Exception as e: print(f"[clob] {e}", file=sys.stderr)
            self.w = None; await asyncio.sleep(2)
    async def _sub(self):
        await _send(self.w, 0x1, json.dumps({"type": "market", "assets_ids": list(self.subs)}).encode())
    async def add(self, tokens):
        if set(tokens) - self.subs:
            self.subs |= set(tokens)
            if self.w:
                try: await self._sub()
                except Exception as e: print(f"[clob sub] {e}", file=sys.stderr)

async def roller(clob):
    mf = open(MKTS, "w", buffering=1); mf.write(f"# wall0_ms={WALL0*1000:.0f}\nt_recv_ms,token,start_epoch\n")
    seen = set()
    while True:
        try:
            p = await asyncio.create_subprocess_exec("curl", "-s", "-H", "accept: application/json", GAMMA,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await p.communicate()
            evs = json.loads(out.decode("utf-8", "ignore")); evs = evs if isinstance(evs, list) else evs.get("data", [])
            now = time.time(); toks = []
            for ev in evs:
                slug = ev.get("slug", "")
                try: start = int(slug.rsplit("-", 1)[1])
                except Exception: continue
                if not (start - 120 <= now <= start + 360): continue
                for m in ev.get("markets", []):
                    try: yes = json.loads(m.get("clobTokenIds", "[]"))[0]
                    except Exception: continue
                    toks.append(yes)
                    if yes not in seen: seen.add(yes); mf.write(f"{now_ms():.3f},{yes},{start}\n")
            if toks: await clob.add(toks)
            print(f"[roller] active={len(toks)} seen={len(seen)}", file=sys.stderr)
        except asyncio.CancelledError: mf.close(); return
        except Exception as e: print(f"[roller] {e}", file=sys.stderr)
        await asyncio.sleep(90)

async def flusher():
    while True:
        try:
            await asyncio.sleep(5)
            if _tf: _tf.flush()
            if _bf: _bf.flush()
        except asyncio.CancelledError: return

def h_binance(j, t):
    if "p" in j: tick("binance", j["p"], t, _i(j.get("T")))
def h_coinbase(j, t):
    if j.get("type") == "ticker" and j.get("price"): tick("coinbase", j["price"], t, iso_ms(j.get("time")))
def h_bybit(j, t):
    for x in j.get("data", []):
        if "p" in x: tick("bybit", x["p"], t, _i(x.get("T")))
def h_okx(j, t):
    for x in j.get("data", []):
        if "px" in x: tick("okx", x["px"], t, _i(x.get("ts")))
def h_kraken(j, t):
    if j.get("channel") == "trade":
        for x in j.get("data", []):
            if "price" in x: tick("kraken", x["price"], t, iso_ms(x.get("timestamp")))

FEEDS = [
    ("binance", "stream.binance.com", 9443, "/ws/btcusdt@trade", None, h_binance),
    ("coinbase", "ws-feed.exchange.coinbase.com", 443, "/", {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}, h_coinbase),
    ("bybit", "stream.bybit.com", 443, "/v5/public/spot", {"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}, h_bybit),
    ("okx", "ws.okx.com", 8443, "/ws/v5/public", {"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"}]}, h_okx),
    ("kraken", "ws.kraken.com", 443, "/v2", {"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}, h_kraken),
]

async def collect():
    open_files(); clob = Clob()
    tasks = [asyncio.create_task(feed(*f)) for f in FEEDS]
    tasks += [asyncio.create_task(pyth()), asyncio.create_task(clob.run()),
              asyncio.create_task(roller(clob)), asyncio.create_task(flusher())]
    print(f"collecting {DUR:.0f}s -> {OUT}", file=sys.stderr)
    try: await asyncio.sleep(DUR)
    finally:
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _tf.flush(); _bf.flush(); _tf.close(); _bf.close()
    open(os.path.join(OUT, "DONE"), "w").write(str(time.time()))

def load_ticks():
    feeds = {}; wall0 = 0.0
    with open(TICKS) as f:
        for ln in f:
            if ln.startswith("# wall0_ms="): wall0 = float(ln.split("=")[1]); continue
            if ln.startswith("src,"): continue
            try:
                s, t, p, te = (ln.rstrip("\n").split(",") + [""])[:4]
                feeds.setdefault(s, []).append((float(t), float(p), float(te) if te else None))
            except Exception: pass
    return feeds, wall0
def load_book():
    bk = {}
    try:
        with open(BOOK) as f:
            for ln in f:
                if ln.startswith("#") or ln.startswith("t_recv"): continue
                parts = ln.rstrip("\n").split(",")
                if len(parts) < 4: continue
                t, tok, bid, ask = parts[0], parts[1], parts[2], parts[3]
                if bid and ask: bk.setdefault(tok, []).append((float(t), (float(bid) + float(ask)) / 2))
    except FileNotFoundError: pass
    return bk
def _med(a): return sorted(a)[len(a) // 2] if a else None

def analyze():
    feeds, wall0 = load_ticks(); book = load_book()
    names = sorted(feeds)
    dur = max((feeds[n][-1][0] for n in names), default=0) / 1000.0 or DUR
    print(f"=== window {dur/60:.1f} min | feeds: {', '.join(names)} ===")
    print("\ncadence | last px | median venue-staleness (our_recv - exch_ts):")
    for n in names:
        st = [(wall0 + t) - te for (t, p, te) in feeds[n] if te]
        ms = _med(st)
        print(f"  {n:9s} {len(feeds[n])/dur:6.1f}/s  ${feeds[n][-1][1]:.2f}  stale={ms:.0f}ms" if ms is not None else f"  {n:9s} {len(feeds[n])/dur:6.1f}/s  ${feeds[n][-1][1]:.2f}  stale=n/a")
    nbt = sum(len(v) for v in book.values())
    print(f"  book: {len(book)} tokens, {nbt} reprices ({nbt/dur:.2f}/s)")

    STEP = 100.0; N = int(dur * 1000 / STEP) + 1
    def ser(evs):
        s = [None] * N; hi = 0
        for i in range(N):
            tt = i * STEP
            while hi < len(evs) and evs[hi][0] <= tt: s[i] = evs[hi][1]; hi += 1
        return s
    series = {n: ser(feeds[n]) for n in names}
    usable = [n for n in names if len(feeds[n]) > dur * 0.5]
    if len(usable) >= 2:
        diffs = {}
        for n in usable:
            s = series[n]; d = [None] * N
            for i in range(1, N):
                if s[i] is not None and s[i-1] is not None: d[i] = s[i]-s[i-1]
            diffs[n] = d
        K = 5
        print("\n=== feed<->feed lead-lag (positive ms = ROW leads COL) ===")
        print("        " + "".join(f"{n[:8]:>9s}" for n in usable))
        for a in usable:
            row = f"{a[:7]:7s}"
            for b in usable:
                if a == b: row += "        ."; continue
                best, bc = 0, -1e9
                for k in range(-K, K+1):
                    num = da = db = 0.0; A, B = diffs[a], diffs[b]
                    for i in range(K+1, N-K-1):
                        x = A[i]; y = B[i-k]
                        if x is None or y is None: continue
                        num += x*y; da += x*x; db += y*y
                    c = num/((da*db)**0.5) if da > 0 and db > 0 else -1e9
                    if c > bc: bc, best = c, k
                row += f"{int(best*STEP):9d}"
            print(row)

    det = "binance" if "binance" in feeds else (usable[0] if usable else None)
    if det:
        evs = [(t, p) for (t, p, te) in feeds[det]]; THRESH = 3.0
        moves = []; i = 0
        while i < len(evs):
            t0, p0 = evs[i]; j = i + 1
            while j < len(evs) and evs[j][0] - t0 <= 500:
                if abs(evs[j][1]-p0)/p0*1e4 >= THRESH:
                    moves.append((evs[j][0], 1 if evs[j][1] > p0 else -1, abs(evs[j][1]-p0))); i = j; break
                j += 1
            i += 1
        coll = []
        for m in moves:
            if coll and m[0]-coll[-1][0] < 1000 and m[1] == coll[-1][1]: continue
            coll.append(m)
        print(f"\n=== {len(coll)} outsized moves (>= {THRESH}bps in <=500ms on {det}) ===")
        if coll:
            ev2 = {n: [(t, p) for (t, p, te) in feeds[n]] for n in usable}
            led = {n: 0 for n in usable}; behind = {n: [] for n in usable}; booklags = []
            for (tm, d, mag) in coll:
                detect = {}
                for n in usable:
                    s = ev2[n]; b0 = next((p for (t, p) in reversed(s) if t <= tm-200), None)
                    if b0 is None: continue
                    dt = next((t for (t, p) in s if tm-200 <= t <= tm+1500 and d*(p-b0) >= mag/2), None)
                    if dt is not None: detect[n] = dt
                if detect:
                    lead = min(detect.values())
                    for n, dt in detect.items():
                        behind[n].append(dt-lead)
                        if dt == lead: led[n] += 1
                nb = None
                for tok, bs in book.items():
                    b0 = next((m for (t, m) in reversed(bs) if t <= tm), None)
                    if b0 is None: continue
                    dt = next((t for (t, m) in bs if tm <= t <= tm+3000 and d*(m-b0) > 0), None)
                    if dt is not None: nb = dt if nb is None else min(nb, dt)
                if nb is not None: booklags.append(nb-tm)
            print("\nfeed first-arrival on moves (led / median ms behind leader):")
            for n in usable:
                m = _med(behind[n])
                print(f"  {n:9s} led={led[n]:4d}/{len(coll)}  behind={m:.0f}ms" if m is not None else f"  {n:9s} led={led[n]:4d}/{len(coll)}  n/a")
            if booklags:
                bl = sorted(booklags)
                print(f"\n*** EDGE WINDOW: BTC move -> Polymarket book reprice (n={len(bl)}) ***")
                print(f"  lag ms: p10={bl[len(bl)//10]:.0f}  p25={bl[len(bl)//4]:.0f}  median={bl[len(bl)//2]:.0f}  p75={bl[3*len(bl)//4]:.0f}  p90={bl[9*len(bl)//10]:.0f}")
            else:
                print("\n(no book reprices matched to moves)")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "analyze": analyze()
    else:
        try: asyncio.run(collect())
        except KeyboardInterrupt: pass
        try: analyze()
        except Exception as e: print("analyze error:", e, file=sys.stderr)
