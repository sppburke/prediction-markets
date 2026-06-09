# 27 — BTC Latency-Arb Feed Bake-off (vantage, feed leadership, and latency budget)

**Status:** LIVING DOCUMENT. Methodology is final; results below are from a
**preliminary ~70-minute window (2026-06-08/09, calm market, n=13 outsized
moves)**. A **9-hour** collection is in progress to enlarge the sample; the
results section will be updated from that run's final analysis. Treat all
specific numbers as directional until the 9 h sample lands.

Relates to: issue **#297** (BTC latency-arb shadow harness `pe-crypto-shadow`),
issue **#300** (Phase-2 venue integration), `crates/crypto-shadow/`,
`docs/08-VENUE-POLYMARKET.md`, `docs/15-SOURCES.md`.

---

## 1. Why this experiment exists

The Strategy 1+ BTC latency-arb thesis (`#297`) is: on Polymarket's 5-/15-minute
BTC up/down markets, a fast mover can buy `YES`/`NO` the instant BTC moves, before
Polymarket's market-makers reprice the book — provided the resulting mispricing
exceeds the `crypto_fees_v2` taker fee (~3% near 50/50, verified in
`crates/crypto-shadow/src/fees.rs`).

Two prerequisites had to be settled before building Phase 2:

1. **Which BTC price feed to use.** The 5-/15-min markets settle on the
   **Chainlink BTC/USD Data Stream**, but that feed is gated behind a
   **sponsored Chainlink API key** whose *fast* (sub-second) tier the Polymarket
   request form prices at **$5,000/month** (free tier = "slightly delayed",
   useless for latency). So the harness must use a **free** BTC reference.
2. **Is there actually a latency edge from our deployment vantage?** "Who sees
   the move first" and "how long until Polymarket reprices" are **vantage-
   dependent** (network distance to each venue), so they must be measured from
   the **real box**, not a developer laptop or sandbox.

This bake-off answers both, empirically, from the production VPS.

---

## 2. Methodology

### 2.1 Vantage
- **Host:** the production VPS in **Ireland** (`82.22.32.225`, the same box that
  runs `pe-service`). Work isolated under `~/feed-bakeoff/`, **read-only** (WS
  subscriptions + HTTP GETs only — no orders, no writes outside that dir).
- This vantage matters: Polymarket's order endpoint fronts on **Cloudflare's
  Dublin edge** from here (see §4), and exchange distances differ markedly
  (Coinbase/Kraken near; Binance's matching engine is in Tokyo).

### 2.2 Data collected (concurrently, one process)
Free BTC reference feeds (direct exchange WS trade streams unless noted):

| Source | Endpoint | Quote |
|---|---|---|
| binance | `wss://stream.binance.com:9443/ws/btcusdt@trade` | USDT |
| coinbase | `wss://ws-feed.exchange.coinbase.com` (ticker BTC-USD) | USD |
| bybit | `wss://stream.bybit.com/v5/public/spot` (publicTrade) | USDT |
| okx | `wss://ws.okx.com:8443/ws/v5/public` (trades BTC-USDT) | USDT |
| kraken | `wss://ws.kraken.com/v2` (trade BTC/USD) | USD |
| pyth | Hermes SSE `hermes.pyth.network/v2/updates/price/stream` (BTC/USD id `e62df6…415b43`) | USD oracle |

Plus the **Polymarket CLOB book** for the active 5-min market's `YES` token
(`wss://ws-subscriptions-clob.polymarket.com/ws/market`), with the token set
**rolled every 90 s** by querying Gamma
(`/events?series_slug=btc-up-or-down-5m&closed=false`) so the book stays live as
5-min markets expire. Book reprices are deduped to **quote changes only**.

### 2.3 Precise timing (critical)
- **One high-resolution monotonic clock**, stamped at **byte-receipt in the WS
  read loop, *before* JSON parse** — so cross-feed "who's first" comparisons are
  apples-to-apples and free of parse/dispatch jitter (µs precision).
- Every row **also** stores the **venue's own source timestamp** (Binance `T`,
  Coinbase `time`, OKX `ts`, Bybit `T`, Kraken `timestamp`, Pyth `publish_time`),
  enabling two distinct metrics: **receipt-latency leadership** (what matters for
  acting) and **per-venue staleness** (`our_recv − venue_stamp`).
- CSV schema: `src,t_recv_ms,price,t_exch_ms` (ticks) and
  `t_recv_ms,token,bid,ask,t_exch_ms` (book). Flushed every 5 s (crash-safe,
  re-analyzable). The header records `wall0_ms` to convert the monotonic clock to
  absolute time.

### 2.4 Metrics
- **Cadence** (updates/s) and **median venue staleness**.
- **Symmetric move detection:** an "outsized move" = the **cross-exchange median**
  price moving **≥ 3 bps within ≤ 300 ms** (deduped within 1 s). Defining moves on
  the *median*, not a single feed, removes single-feed trigger bias.
- **First-arrival leadership:** per move, the first feed to cross half the move
  magnitude in the move direction → tally of "led" + median ms behind the leader.
- **Edge window:** per move, the lag until the **active 5-min market's** book mid
  reprices in the same direction (matched to the active token via the market
  window from the event slug timestamp, not all tokens).
- **Head start vs Polymarket:** `PM_book_reprice_time − source_detect_time` per
  source = how long that source sees the move before Polymarket reprices.

### 2.5 Methodology fixes made mid-run (recorded so they are not repeated)
The first analyzer pass was **wrong** in two ways; both were corrected on the
saved raw data (read-only re-analysis), which is exactly why raw ticks are
persisted:

1. **Trigger bias** — moves were detected *on Binance* and others timed against
   it, which made Binance look like the leader by construction. **Fix:** detect
   on the cross-exchange median (symmetric). Result flipped (see §3).
2. **Edge-window matcher too loose** — it matched book reprices across *all*
   tokens, freezing at a bogus `n=2, ~21 ms`. **Fix:** match only the **active**
   5-min market token via `markets.csv` windows. Result: realistic ~161 ms.

> **Lesson for Phase 2 / any future measurement:** define the move signal
> symmetrically (consensus), and join book reactions to the *specific active
> market*, not the union of subscribed tokens.

---

## 3. Results (preliminary — ~70 min, n=13 moves, n=4 book-matched)

All venues — **including Binance** — are reachable from the Ireland box (Binance
was blocked from the dev sandbox; the real box matters).

### 3.1 Cadence & staleness
| Feed | Cadence | Median staleness (`recv − venue_stamp`) | Quote |
|---|---|---|---|
| binance | ~46/s | ~123 ms | USDT |
| bybit | ~9/s | ~94 ms | USDT |
| coinbase | ~7/s | ~49 ms | USD |
| okx | ~5/s | ~118 ms | USDT |
| kraken | ~1/s | ~14–19 ms | USD |
| pyth | ~2/s | **~2,100–2,300 ms** | USD oracle |

USDT venues (binance/okx/bybit) trade **~4.7–5 bps above** USD venues
(coinbase/kraken/pyth) — the USDT/USD basis. Irrelevant to move *detection*
(cancels in price changes); relevant only to absolute settlement comparison, so
prefer a **USD-quoted** reference or basis-correct.

### 3.2 Move-leadership (symmetric, n=13) — the counterintuitive result
| Feed | Led | Median ms behind leader |
|---|---|---|
| **bybit** | 6/13 | **0** |
| coinbase | 4/13 | 38 |
| okx | 3/13 | 44 |
| kraken | 0/13 | 236 (freshest-when-it-ticks but too sparse at ~1/s) |
| **binance** | 0/13 | **441** |
| pyth | 0/13 | 1,496 |

**Binance is *last* among exchanges from Ireland, not first.** The naive
"Binance is the global BTC price leader, use it" intuition is **wrong for this
vantage** — its Tokyo matching engine is too far; the move reaches
Bybit/Coinbase/OKX (and the consensus) before Binance's packets arrive. Pyth
Hermes (~2 s stale) is decisively unusable for a speed strategy.

### 3.3 Edge window & head start vs Polymarket (n=4 matched)
- **Edge window** (consensus move → active-market book reprice): **median
  ~161 ms** (p25 137, p75 184).
- **Only ~4 of 13 (≈30%) of 3 bps moves repriced the book within 3 s** — the
  5-min book is **sticky** on small moves. §3.4 resolves which kind: the book is
  **tight (~2 ¢)**, so this is the **stale-tight-quote pickoff** regime, *not* a
  wide/illiquid book.
- **Head start** (`PM reprice − source detect`):

| Source | Head start over Polymarket |
|---|---|
| bybit | **+257 ms** |
| coinbase | +211 ms |
| okx | +206 ms |
| kraken | −10 ms |
| binance | **−179 ms** (sees it *after* PM reprices) |
| pyth | −1,239 ms |

Negative = the source shows you the move *after* Polymarket already moved.
**From Ireland, only Bybit/Coinbase/OKX put you ahead of Polymarket; Binance and
Pyth put you behind.**

### 3.4 Book spread — how wide is the market (n=1,644 active-market quote samples)
The "sticky book" of §3.3 is a **tight** book, not an illiquid one:

| metric | value |
|---|---|
| spread (ask − bid) | p10 **1 ¢**, median **2 ¢**, p90 **3 ¢**, max 15 ¢ |
| frac spread ≤ 2 ¢ / ≤ 5 ¢ | **54% / 97%** |
| mid = P(up) | p10 0.34, median **0.53**, p90 0.86 |

Min tick is 1 ¢, so the median 2 ¢ spread is just **two ticks**. The book stays
tight **across the entire 5-min window** — it does *not* widen toward settlement:

| window progress | median spread | p90 | median P(up) |
|---|---|---|---|
| 0–20% | 2 ¢ | 3 ¢ | 0.54 |
| 20–40% | 2 ¢ | 3 ¢ | 0.54 |
| 40–60% | 2 ¢ | 3 ¢ | 0.54 |
| 60–80% | 2 ¢ | 3 ¢ | 0.63 |
| 80–100% | 2 ¢ | 5 ¢ | 0.38 |

Spread is ~flat at 2 ¢ throughout; only the p90 ticks up (3→5 ¢) in the final
20%. The mid stays near 0.50 (uncertain) for the first ~60% then drifts off as
the outcome decides — so the **uncertain, latency-relevant regime is the first
~60% of the window**, and it is consistently tight.

**Economics implication.** To profit you must clear roughly **half-spread (~1 ¢)
+ the `crypto_fees_v2` taker fee (~1.75 ¢ at p≈0.5) ≈ ~2.7 ¢** of mispricing —
the **fee is ~65% of the hurdle**. A 3 bps BTC move shifts fair `P(up)` by ≈
`φ(0)·(Δ/σ_5m)` ≈ ~3 ¢ mid-window (5-min terminal σ ≈ ~36 bps) — **only
marginally above the ~2.7 ¢ hurdle**; sensitivity *grows* later in the window
(smaller residual σ) until `P(up)` saturates near settlement. So the exploitable
regime is **larger moves and/or later in the window**, against a book that stays
tight — exactly what `pe-crypto-shadow` must measure to settle the thesis. (This
is a back-of-envelope; realized edge is the harness's job, not the bake-off's.)
Computed by `scripts/feed-bakeoff/spread.py`.

### 3.5 Provisional feed pick
**Coinbase** or **Bybit** as the BTC reference. Coinbase is USD (no USDT basis)
with solid cadence and ~+211 ms head start; Bybit leads most often (+257 ms) but
is USDT. Decision pending the 9 h sample.

---

## 4. Order-submission latency budget (measured from Ireland)

Order submit path (Tier-1, repo): `POST /order` to `https://clob.polymarket.com`
(`crates/venue-polymarket/src/adapter.rs:123`).

- **Endpoint is Cloudflare-fronted, Dublin (`DUB`) edge** (`cf-ray …-DUB`, IPs in
  Cloudflare ranges). **Not the UK.** TCP-connect to the edge ~16–20 ms.
- **Order-POST RTT** (no-auth POST → origin `401 {"error":"Unauthorized/Invalid
  api key"}`, confirming the request reached Polymarket's CLOB app, not an edge
  block):
  - **Cold** (fresh TLS each request): **~77–100 ms** (median ~85; includes the
    TLS handshake).
  - **Warm** (reused TLS — what a real bot uses): **~20–28 ms**.
  - Confirmed the warm number is a real origin round-trip, not an edge fake:
    `GET /time` (DYNAMIC, returns live server time) warm RTT **~19–23 ms**, same
    as the 401. A *successful* order adds a few ms of matching-engine processing
    → **~25–50 ms to an ack**.

### Budget
| | |
|---|---|
| Best-source head start over PM reprice | **~210–257 ms** |
| Warm order RTT from Ireland | **~20–28 ms** (~25–50 ms to ack) |
| **Net slack** | **~180–235 ms** ✅ |

**The latency leg of the thesis is viable from Ireland.** Cold RTT (~85 ms) only
bites the first order after (re)connecting — hold a keep-alive HTTP/2 connection.
The ~55–65 ms Cloudflare-edge→origin component is irreducible on the public API,
so Ireland is already near-optimal; there is no UK/colo move that meaningfully
beats it short of a Polymarket-direct order path.

---

## 5. Caveats & limitations

- **Small sample (preliminary).** ~70 min, calm market, **13 moves / 4
  book-matched**. The leadership ordering among the top cluster
  (bybit/coinbase/okx, within ~44 ms) may shuffle; the edge-window and
  head-start medians are n=4. The 9 h run addresses this.
- **Vantage-specific.** All leadership/head-start/RTT numbers are for **Ireland**.
  A different region would reorder the feeds (e.g., Binance would lead from
  Tokyo). Re-measure if the deployment box moves.
- **Warm order RTT is a lower bound** — it's the round-trip to a `401` auth
  response; a *filled* order adds matching-engine processing. Still tiny vs the
  head start.
- **Staleness metric** mixes per-venue clock skew vs our box (NTP-dependent);
  read it as indicative, not exact, network latency.
- **USDT basis** (~5 bps) on bybit/okx/binance — only matters for absolute
  settlement comparison, not move detection.
- **The fee/economics gate is NOT addressed here.** This experiment only proves
  the *latency* leg. Whether a fast move's `P(up)` mispricing exceeds the
  `crypto_fees_v2` taker fee is the separate, decisive question — measured by the
  `pe-crypto-shadow` harness (#297/#300).

---

## 6. Reproduction

Scripts are committed under **`scripts/feed-bakeoff/`** (copied verbatim from the
run): `feed_bakeoff_v2.py` (collector), `analyze2.py` (corrected analyzer),
`spread.py` (book-width / window-progress), `launch9.sh` (9 h detached launcher).
On the VPS they live at `~/feed-bakeoff/`
with output CSVs under `~/feed-bakeoff/run/`.

```bash
# On a deployment-vantage box (Python 3.12 + curl; no pip/node needed):
mkdir -p ~/feed-bakeoff && cd ~/feed-bakeoff
# copy feed_bakeoff_v2.py, analyze2.py, launch9.sh here
# launch a detached N-hour run (DUR seconds):
setsid bash launch9.sh </dev/null >/dev/null 2>&1 &     # launch9.sh sets DUR=32400 (9h)
# re-analyze the flushed CSVs at any time (read-only):
OUT=$HOME/feed-bakeoff/run python3 analyze2.py
```

**Launch gotcha (recorded):** a plain `nohup … &`/`& exit 0` over SSH kept dying
with exit 255 because the backgrounded job held the SSH channel open. The fix is
a launcher script that redirects **all** fds (`>log 2>&1 </dev/null`) under
`setsid` (see `launch9.sh`), launched from a short-lived SSH that returns
immediately, with a **separate** SSH for verification.

---

## 7. Conclusions & next steps

1. **Latency: solved and favorable from Ireland.** Best free feed (Bybit/Coinbase)
   sees the move ~210–257 ms before Polymarket reprices; warm order RTT ~20–28 ms
   → ~180–235 ms slack. **Pyth Hermes and Binance are unsuitable from this
   vantage** (too slow / too far) — overturning the earlier plan to use Pyth.
2. **Phase-2 reference (#300):** use a **fast, USD-quoted exchange feed
   (Coinbase, or Bybit with USDT basis-correction)** as the live BTC reference —
   *not* Pyth, *not* Chainlink Data Streams ($5k/mo). Realized ground truth comes
   from actual market resolutions.
3. **Open / decisive question:** the **economics** — does a fast move's
   mispricing beat the ~3% crypto fee? Only ~30% of small (3 bps) moves repriced
   the book here, hinting at sticky quotes (possible pickoff) — but this must be
   measured properly by `pe-crypto-shadow`, which is what #300 builds toward.
4. **Pending:** finalize §3 from the 9 h run; optionally a move-threshold sweep
   (2/2.5/3 bps) on the raw data for statistical power.

_Last updated: 2026-06-09 (preliminary ~70 min window; 9 h run in progress)._
