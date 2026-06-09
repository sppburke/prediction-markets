#!/usr/bin/env python3
"""How wide is the Polymarket 5-min book? Spread (ask-bid) + mid distribution
for the ACTIVE market only, from the bake-off book.csv. Read-only."""
import os
OUT = os.environ.get("OUT", os.path.expanduser("~/feed-bakeoff/run"))
mkts = {}
for ln in open(os.path.join(OUT, "markets.csv")):
    if ln.startswith("#") or ln.startswith("t_recv"): continue
    p = ln.rstrip().split(",")
    if len(p) >= 3:
        try: mkts[p[1]] = int(p[2])
        except Exception: pass
wall0 = 0.0; rows = []
for ln in open(os.path.join(OUT, "book.csv")):
    if ln.startswith("# wall0_ms="): wall0 = float(ln.split("=")[1]); continue
    if ln.startswith("t_recv"): continue
    p = ln.rstrip().split(",")
    if len(p) < 4 or not p[2] or not p[3]: continue
    try: rows.append((float(p[0]), p[1], float(p[2]), float(p[3])))
    except Exception: pass
def active(tok, t):
    st = mkts.get(tok)
    return st is not None and st * 1000 <= wall0 + t <= (st + 300) * 1000
spreads = []; mids = []
for (t, tok, bid, ask) in rows:
    if not active(tok, t): continue
    sp = ask - bid
    if 0 <= sp <= 1: spreads.append(sp); mids.append((bid + ask) / 2)
spreads.sort(); mids.sort()
def q(a, f): return a[min(len(a) - 1, int(len(a) * f))] if a else float("nan")
print(f"active-market quote samples: {len(spreads)}  (of {len(rows)} book rows, {len(mkts)} markets seen)")
if spreads:
    print(f"SPREAD ask-bid (cents): p10={q(spreads,.1)*100:.1f}  p25={q(spreads,.25)*100:.1f}  median={q(spreads,.5)*100:.1f}  p75={q(spreads,.75)*100:.1f}  p90={q(spreads,.9)*100:.1f}  max={spreads[-1]*100:.1f}")
    le = lambda c: sum(1 for s in spreads if s <= c) / len(spreads)
    print(f"  frac spread <= 1c: {le(0.01):.2f}   <= 2c: {le(0.02):.2f}   <= 5c: {le(0.05):.2f}   <= 10c: {le(0.10):.2f}")
    print(f"MID = P(up) (prob): p10={q(mids,.1):.2f}  median={q(mids,.5):.2f}  p90={q(mids,.9):.2f}")
    print("  (near 0.50 = mid-window/uncertain; near 0/1 = close to settlement)")
    # crude hurdle: to profit you must clear half-spread (cross to ask) + ~1.75c fee at p~0.5
    print(f"ENTRY HURDLE est. (half-spread + ~1.75c fee @ p~0.5): ~{q(spreads,.5)/2*100+1.75:.1f}c of mispricing needed at the median spread")
    # spread vs position within the 5-min window
    bk = {i: [] for i in range(5)}; bm = {i: [] for i in range(5)}
    for (t, tok, bid, ask) in rows:
        st = mkts.get(tok)
        if st is None: continue
        wall = wall0 + t
        if not (st * 1000 <= wall <= (st + 300) * 1000): continue
        sp = ask - bid
        if not (0 <= sp <= 1): continue
        b = min(4, int((wall - st * 1000) / 300000.0 * 5))
        bk[b].append(sp); bm[b].append((bid + ask) / 2)
    print("\nSPREAD vs WINDOW PROGRESS (5-min market, 20% bins):")
    labels = ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"]
    for i in range(5):
        a = sorted(bk[i]); m = sorted(bm[i])
        if a:
            print(f"  {labels[i]:8s} n={len(a):5d}  median_spread={a[len(a)//2]*100:.1f}c  p90={a[min(len(a)-1,int(len(a)*.9))]*100:.1f}c  median_mid(Pup)={m[len(m)//2]:.2f}")
        else:
            print(f"  {labels[i]:8s} n=0")
