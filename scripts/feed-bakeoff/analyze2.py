#!/usr/bin/env python3
"""Corrected re-analysis (read-only) of the bake-off CSVs:
 - SYMMETRIC move detection on the cross-exchange median (kills the single-trigger bias)
 - EDGE WINDOW matched against the ACTIVE 5-min market's book (via markets.csv windows)
Run: OUT=~/feed-bakeoff/run python3 analyze2.py
"""
import os, bisect, statistics
OUT = os.environ.get("OUT", os.path.expanduser("~/feed-bakeoff/run"))

def load_ticks():
    feeds = {}; wall0 = 0.0
    with open(os.path.join(OUT, "ticks.csv")) as f:
        for ln in f:
            if ln.startswith("# wall0_ms="): wall0 = float(ln.split("=")[1]); continue
            if ln.startswith("src,"): continue
            try:
                s, t, p, te = (ln.rstrip().split(",") + [""])[:4]
                feeds.setdefault(s, []).append((float(t), float(p)))
            except Exception: pass
    return feeds, wall0
def load_book():
    bk = {}
    try:
        with open(os.path.join(OUT, "book.csv")) as f:
            for ln in f:
                if ln.startswith("#") or ln.startswith("t_recv"): continue
                pr = ln.rstrip().split(",")
                if len(pr) >= 4 and pr[2] and pr[3]:
                    bk.setdefault(pr[1], []).append((float(pr[0]), (float(pr[2]) + float(pr[3])) / 2))
    except FileNotFoundError: pass
    return bk
def load_mkts():
    m = {}
    try:
        with open(os.path.join(OUT, "markets.csv")) as f:
            for ln in f:
                if ln.startswith("#") or ln.startswith("t_recv"): continue
                pr = ln.rstrip().split(",")
                if len(pr) >= 3:
                    try: m[pr[1]] = int(pr[2])
                    except Exception: pass
    except FileNotFoundError: pass
    return m

feeds, wall0 = load_ticks(); book = load_book(); mkts = load_mkts()
names = sorted(feeds)
EXCH = [n for n in ["binance", "coinbase", "bybit", "okx", "kraken"] if n in feeds]
dur = max((feeds[n][-1][0] for n in names), default=0) / 1000.0 or 1.0
T = {n: [t for t, p in feeds[n]] for n in names}
P = {n: [p for t, p in feeds[n]] for n in names}
def val_at(n, t):
    i = bisect.bisect_right(T[n], t) - 1
    return P[n][i] if i >= 0 else None

STEP = 50.0; N = int(dur * 1000 / STEP) + 1
cons = [None] * N
for i in range(N):
    vals = [v for v in (val_at(n, i * STEP) for n in EXCH) if v is not None]
    if vals: cons[i] = statistics.median(vals)
THRESH = 3.0; W = 6
moves = []
for i in range(W, N):
    a, b = cons[i - W], cons[i]
    if a and b and abs(b - a) / a * 1e4 >= THRESH:
        moves.append((i * STEP, 1 if b > a else -1, abs(b - a)))
coll = []
for m in moves:
    if coll and m[0] - coll[-1][0] < 1000 and m[1] == coll[-1][1]: continue
    coll.append(m)
print(f"window {dur/60:.1f}min | symmetric moves (>= {THRESH}bps/300ms on EXCH-median): {len(coll)}")

BT = {tok: [t for t, m in bs] for tok, bs in book.items()}
BM = {tok: [m for t, m in bs] for tok, bs in book.items()}
def active_tokens(wall_ms):
    return [tok for tok, st in mkts.items() if st * 1000 <= wall_ms <= (st + 300) * 1000 and tok in book]

led = {n: 0 for n in names}; behind = {n: [] for n in names}
booklags = []; had_active = 0
lead_over_pm = {n: [] for n in names}   # Polymarket reprice time - source detect time = head start
for (tm, d, M) in coll:
    detect = {}
    for n in names:
        b0 = val_at(n, tm - 200)
        if b0 is None: continue
        lo = bisect.bisect_left(T[n], tm - 200); hi = bisect.bisect_right(T[n], tm + 1500)
        for k in range(lo, hi):
            if d * (P[n][k] - b0) >= M / 2: detect[n] = T[n][k]; break
    if detect:
        lead = min(detect.values())
        for n, dt in detect.items():
            behind[n].append(dt - lead)
            if dt == lead: led[n] += 1
    acts = active_tokens(wall0 + tm)
    if acts: had_active += 1
    nb = None
    for tok in acts:
        i = bisect.bisect_right(BT[tok], tm) - 1
        if i < 0: continue
        b0 = BM[tok][i]
        lo = bisect.bisect_left(BT[tok], tm); hi = bisect.bisect_right(BT[tok], tm + 3000)
        for k in range(lo, hi):
            if d * (BM[tok][k] - b0) > 0: nb = BT[tok][k] if nb is None else min(nb, BT[tok][k]); break
    if nb is not None:
        booklags.append(nb - tm)
        for n, dt in detect.items():
            lead_over_pm[n].append(nb - dt)   # how long this source saw the move before PM repriced

def med(a): return sorted(a)[len(a) // 2] if a else None
print("\nSYMMETRIC first-arrival on moves (led / median ms behind leader):")
for n in sorted(names, key=lambda n: (-led[n], med(behind[n]) if behind[n] else 9e9)):
    mm = med(behind[n])
    print(f"  {n:9s} led={led[n]:3d}/{len(coll)}  behind={mm:.0f}ms" if mm is not None else f"  {n:9s} led={led[n]:3d}/{len(coll)}  n/a")

print(f"\nEDGE WINDOW (active-market matched): moves={len(coll)} had_active_token={had_active} matched={len(booklags)}")
print(f"  book tokens={len(book)} reprices={sum(len(v) for v in book.values())} markets_seen={len(mkts)}")
if booklags:
    bl = sorted(booklags); q = lambda f: bl[min(len(bl) - 1, int(len(bl) * f))]
    print(f"  consensus move -> PM book reprice lag ms: p25={q(.25):.0f} median={q(.5):.0f} p75={q(.75):.0f}")

print("\n*** HEAD START vs POLYMARKET: ms each BTC source sees the move BEFORE Polymarket reprices ***")
for n in sorted(names, key=lambda n: -(med(lead_over_pm[n]) if lead_over_pm[n] else -9e9)):
    mm = med(lead_over_pm[n])
    if mm is not None: print(f"  {n:9s} head_start median={mm:.0f}ms  (n={len(lead_over_pm[n])})")
