# 27 — BTC Latency-Arb Feed Bake-off (vantage, feed leadership, and latency budget)

**Status:** FINAL. Results below are from the **complete 9-hour run
(2026-06-08/09, window 540.0 min, n=134 outsized moves, 37 book-matched)**; the
collector exited cleanly at the full duration. The earlier ~70-minute
preliminary cut is superseded. A multi-day repeat (different volatility regime)
would tighten the head-start confidence intervals but is not required to act on
the qualitative ranking.

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

## 3. Results (final — 9 h run, window 540.0 min, n=134 symmetric moves, n=37 book-matched)

All venues — **including Binance** — are reachable from the Ireland box (Binance
was blocked from the dev sandbox; the real box matters). Numbers below are the
**complete 9 h run** (collector exited cleanly at 540.0 min; 1.83 M ticks, 54,944
book reprices over 218 market windows). The earlier ~70 min preliminary cut is
superseded; where the larger sample moved a number, it is called out.

### 3.1 Cadence & staleness
| Feed | Cadence | Median staleness (`recv − venue_stamp`) | Quote |
|---|---|---|---|
| binance | ~35.5/s | ~131 ms | USDT |
| bybit | ~9.0/s | ~97 ms | USDT |
| coinbase | ~5.9/s | ~54 ms | USD |
| okx | ~3.4/s | ~123 ms | USDT |
| kraken | ~0.5/s | ~14 ms | USD |
| pyth | ~2.0/s | **~2,828 ms** | USD oracle |

USDT venues (binance/okx/bybit) trade **~4.7–5 bps above** USD venues
(coinbase/kraken/pyth) — the USDT/USD basis. Irrelevant to move *detection*
(cancels in price changes); relevant only to absolute settlement comparison, so
prefer a **USD-quoted** reference or basis-correct.

### 3.2 Move-leadership (symmetric, n=134) — the counterintuitive result
| Feed | Led | Median ms behind leader |
|---|---|---|
| **bybit** | 51/134 (38%) | **11** |
| coinbase | 28/134 | 64 |
| okx | 23/134 | 65 |
| **binance** | 22/134 | **182** |
| kraken | 8/134 | 192 (freshest-when-it-ticks but too sparse at ~0.5/s) |
| pyth | 1/134 | 1,202 |

**Binance is *near-last* among exchanges from Ireland, not first.** The naive
"Binance is the global BTC price leader, use it" intuition is **wrong for this
vantage** — its Tokyo matching engine is too far; the move reaches
Bybit/Coinbase/OKX (and the consensus) before Binance's packets arrive. Binance
*occasionally* leads (22/134) but is bimodal — when it doesn't lead it is a full
**182 ms behind** the leader. Pyth Hermes (~2.8 s stale) is decisively unusable
for a speed strategy. **bybit** is the front-runner: leads 38% of moves and is
only **11 ms** behind on the ones it doesn't.

> **Methodology cross-check (live demonstration of the §2.5 fix).** The
> collector's *own* built-in detector — which triggers on Binance moves — reports
> "binance led 85/179" in `run.log` for this same run. That is the **biased**
> artifact §2.5 warns about (detect-on-binance ⇒ binance-wins-by-construction).
> The unbiased symmetric analyzer (cross-exchange median) gives bybit 51 / binance
> 22. **Cite the symmetric numbers; the `run.log` 85/179 is retained only as the
> bias demonstration.**

### 3.3 Edge window & head start vs Polymarket (n=37 matched)
- **Edge window** (consensus move → active-market book reprice): **median
  ~77 ms** (p25 31, p75 235).
- **Only 37 of 134 (≈28%) of 3 bps moves repriced the book in-window** — the
  5-min book is **sticky** on small moves. §3.4 resolves which kind: the book is
  **tight (~2 ¢)**, so this is the **stale-tight-quote pickoff** regime, *not* a
  wide/illiquid book.
- **Head start** (`PM reprice − source detect`), median (n matched):

| Source | Head start over Polymarket |
|---|---|
| okx | **+188 ms** (n=32) |
| bybit | +164 ms (n=32) |
| coinbase | +135 ms (n=34) |
| binance | +43 ms (n=32) |
| kraken | +23 ms (n=28) |
| pyth | **−723 ms** (n=5) |

Negative = the source shows you the move *after* Polymarket already moved. On the
9 h sample the head-start medians **compressed** vs the n=4 preliminary cut
(bybit +257→+164) but stayed **solidly positive and well-separated** for the
top three. **From Ireland, Bybit/OKX/Coinbase put you ~135–188 ms ahead of
Polymarket; Pyth puts you ~0.7 s behind, and Binance's edge (+43 ms) is too thin
to rely on.**

### 3.4 Book spread — how wide is the market (n=27,199 active-market quote samples)
The "sticky book" of §3.3 is a **tight** book, not an illiquid one:

| metric | value |
|---|---|
| spread (ask − bid) | p10 **1 ¢**, median **2 ¢**, p90 **5 ¢**, max 63 ¢ |
| frac spread ≤ 2 ¢ / ≤ 5 ¢ | **50% / 91%** |
| mid = P(up) | p10 0.13, median **0.51**, p90 0.90 |

Min tick is 1 ¢, so the median 2 ¢ spread is just **two ticks**. The book stays
tight **across the entire 5-min window** — it does *not* widen toward settlement
until the very end:

| window progress | median spread | p90 | median P(up) |
|---|---|---|---|
| 0–20% | 2 ¢ | 2 ¢ | 0.51 |
| 20–40% | 2 ¢ | 3 ¢ | 0.47 |
| 40–60% | 2 ¢ | 3 ¢ | 0.49 |
| 60–80% | 2 ¢ | 3 ¢ | 0.57 |
| 80–100% | 2 ¢ | 8 ¢ | 0.52 |

Spread is ~flat at 2 ¢ throughout; only the p90 widens (3→8 ¢) in the final 20%
as settlement nears. The mid stays near 0.50 (uncertain) through most of the
window — so the **uncertain, latency-relevant regime is the first ~80% of the
window**, and it is consistently tight. (The 27 k-sample tail is fatter than the
preliminary cut — max 63 ¢ — but those are rare wide ticks; the median/p90 are
unchanged at the order of ~2/5 ¢.)

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

### 3.5 Feed pick (final)
**Move trigger = cross-exchange median of {bybit, okx, coinbase}** — what the
analyzer already keys on, and more robust than any single feed (no single venue's
packet jitter or brief outage can fire or miss a move alone). For a **single
primary** reference, **Coinbase**: USD-quoted (matches Chainlink BTC/USD
settlement, no USDT basis), freshest staleness (~54 ms), +135 ms head start over
Polymarket. **Bybit** leads most often (38%, +164 ms) but is USDT → basis-correct
if used. **OKX** has the single largest head start (+188 ms) but lower cadence
(3.4/s). **Binance, Kraken, Pyth are out** — Binance's +43 ms edge is too thin
and bimodal, Kraken too sparse (0.5/s), Pyth ~0.7 s *behind* Polymarket.

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
| Best-source head start over PM reprice (9 h, n≥32) | **~135–188 ms** |
| Warm order RTT from Ireland | **~20–28 ms** (~25–50 ms to ack) |
| **Net slack** | **~110–165 ms** ✅ |

**The latency leg of the thesis is viable from Ireland.** Cold RTT (~85 ms) only
bites the first order after (re)connecting — hold a keep-alive HTTP/2 connection.
The ~55–65 ms Cloudflare-edge→origin component is irreducible on the public API,
so Ireland is already near-optimal; there is no UK/colo move that meaningfully
beats it short of a Polymarket-direct order path.

---

## 5. Caveats & limitations

- **Sample (final).** 9 h, **134 symmetric moves / 37 book-matched**; head-start
  medians are n=28–34 per feed. The top-3 ordering (bybit by lead-count;
  okx/bybit/coinbase by head start) is stable across the last six 25-min
  snapshots. Still a **single 9 h session in one volatility regime** — a
  multi-day repeat would tighten the head-start CIs, but the qualitative ranking
  (USDT/USD exchanges ahead, Pyth/Binance unusable) is firm.
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

1. **Latency: solved and favorable from Ireland.** Best free feeds (Bybit/OKX/
   Coinbase) see the move ~135–188 ms before Polymarket reprices; warm order RTT
   ~20–28 ms → **~110–165 ms slack**. **Pyth Hermes and Binance are unsuitable
   from this vantage** (Pyth ~0.7 s behind PM; Binance +43 ms, bimodal) —
   overturning the earlier plan to use Pyth.
2. **Phase-2 reference (#300):** trigger on the **cross-exchange median of
   {bybit, okx, coinbase}**; if a single primary is needed, **Coinbase**
   (USD-quoted, freshest, +135 ms) — *not* Pyth, *not* Chainlink Data Streams
   ($5k/mo). Realized ground truth comes from actual market resolutions.
3. **Open / decisive question:** the **economics** — does a fast move's
   mispricing beat the ~2.8 ¢ fee+spread hurdle (fee ≈ 65% of it)? Only ~28% of
   small (3 bps) moves repriced the tight ~2 ¢ book in-window, the
   stale-tight-quote pickoff regime — but realized edge must be measured by
   `pe-crypto-shadow`, which is what #300 builds toward.
4. **Optional follow-ups:** a move-threshold sweep (2/2.5/3 bps) on the raw CSVs
   for statistical power, and a multi-day repeat to tighten the head-start CIs.

_Last updated: 2026-06-09 (final — complete 9 h run, window 540.0 min, n=134
moves / 37 book-matched; supersedes the preliminary ~70 min cut)._
