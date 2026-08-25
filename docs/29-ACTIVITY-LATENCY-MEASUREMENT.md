# 29 — Polymarket `/activity` Attribution-Latency Measurement

**What & why.** *(2026-08-25, #530: the REST-only premise is superseded — the
live-data activity websocket carries attributed trades; see the addendum at the end.
The CLOB market channel remains wallet-anonymous, and this file's REST measurement
stays authoritative for the fallback path.)* Copy-trading a *named* leader on
Polymarket historically used the REST `data-api.polymarket.com/activity?user=<wallet>`
feed — the CLOB WebSocket print is wallet-anonymous (`docs/15-SOURCES.md`, issues
#282/#300). The binding question for
copying **near-resolution first-bets** (first buy in a market placed minutes before it
resolves — a high-conviction signal) is: *how long after a leader's trade can our poller
actually see it?* That visibility lag sets the lowest reliable time-to-resolution (TTR)
floor for the Winner-Follow 72h buy-and-hold cohort.

Harness: `scripts/measure_activity_latency.py` (stdlib-only; re-runnable).

## Method

1. Seed an "active-today" basket from the VOL/PNL `timePeriod=DAY` leaderboard (wallets
   trading right now).
2. Fix `baseline = measurement start` (server clock); only count trades with
   `timestamp > baseline` (fresh during the window, never a backlog).
3. Round-robin poll `/activity?user=W&type=TRADE&start=<baseline>`. For each new
   `transactionHash`:
   - **upper bound** = `first_seen_wallclock − trade.timestamp` (includes our poll jitter);
   - **proven lower bound** = `prior_poll_wallclock − trade.timestamp`, recorded only when
     the *previous* poll of that wallet was already after the trade timestamp yet did not
     contain the trade — i.e. the trade was provably not yet visible then. This is
     **jitter-independent** and is the clean infra-latency number.
4. Clock skew (server − local) read from the HTTP `Date` header and smoothed (~0 here;
   NTP-synced).

`trade.timestamp` is second-granular (±1s rounding noise on individual lags; washes out
over percentiles).

## Result — run 2026-06-14 (15 min, 60 wallets, 4,655 trades, 0 errors)

| metric | n | p50 | p90 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| **Proven indexing lag** (lower bound, jitter-free) | 559 | 1.2s | 3.2s | **3.8s** | 5.2s | 23s |
| Upper bound (incl. poll jitter) | 4,655 | 16s | 26s | 28s | 40s | 66s |

- 99.9% of trades seen ≤ 60s; 100% ≤ 120s — even with the inflated upper bound.
- Achieved poll rate was **~2.3 req/s** (not the 10 target — HTTP RTT dominated), so the
  real wallet-revisit interval was **~26s**, which is what inflates the upper bound. The
  **proven lower bound (~1–4s) is the true `/activity` indexing latency** and is unaffected
  by our cadence.

**Conclusion: Polymarket `/activity` indexes trades in ~1–4s (p95 3.8s, p99 5.2s, rare tail
to ~23s).**

## Implication for the copy-trade TTR floor

End-to-end observe→act latency on the POLL path = `/activity` indexing (~1–4s, p99 5s)
+ the poller's revisit time (round duration + `trade_poll_interval_secs`, 30s in
production) + order placement ≈ **~20–35s typical**. This is the fallback path's
budget; the websocket path below is the primary (#530).

- A **1-minute TTR floor is reliably copyable** (≥ ~40s of margin after worst-case latency).
- Sub-minute breaks down (a 30s-TTR trade observed at +15–20s leaves too little to fill).

→ The 72h buy-and-hold ranking uses **`--min-ttr-hours 0.0167` (60s)** as the reliability
floor. (An earlier exploratory re-run this session passed a 6h floor on the command line —
before this latency was measured — which is far stricter than the ~10–20s copy latency
warrants; the script default has always been 60s.) The *capturable* edge near resolution is governed separately
by latency-shifted fill pricing in the ranking, not by this floor.

## Caveats

- Indexing latency was measured on liquid VOL/DAY-leader markets; the indexing *pipeline* is
  infra-level and not expected to vary by market, but fill *liquidity* near resolution is a
  separate question handled in the ranking.
- Measured concurrently with a running `pe-bootstrap backfill` (~20 req/s from the same IP);
  the added ~2.3 req/s caused 0 errors. If anything this biases the latency *high*.
- Re-run before trusting for a new regime: `python3 scripts/measure_activity_latency.py
  --duration-secs 900`.


## Addendum (2026-08-25, issue #530): attributed websocket measurements

`wss://ws-live-data.polymarket.com`, subscription
`{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}`,
streams every platform trade with `proxyWallet` (docs/15 entry + re-check policy).

- **Latency** (trade `timestamp` → local receipt, NTP-synced, 120s capture,
  6,293 trades): p50 0.80s / p90 1.25s / p95 1.32s / p99 1.41s / max 1.52s.
  Stable across a 14h soak (median minute-p95 1.31s; worst single minute 6.49s).
- **Continuity**: the stream was live only ~113/840 soak minutes; sockets stay
  ping-alive while the subscription silently lapses (1,442 thirty-second silences
  vs 16 hard disconnects). Consequence: silence-triggered resubscribe/reconnect
  (`source-polymarket-public::activity_ws` policy) and the always-on REST poll
  fallback are load-bearing.
- **Payload**: `proxyWallet`, `conditionId`, `asset`, `outcome`/`outcomeIndex`,
  `price`, `size`, `side`, `timestamp` (string seconds), `transactionHash`;
  `fee` optional per trade; schema otherwise stable all night.
- **End-to-end websocket-primary paper copy** ≈ feed (p95 1.32s) + best-ask fetch
  (p50 64ms from the VPS) + commit round-trip (p50 163ms) ≈ **1.0s p50 / 1.6s p95**,
  the basis for `LATENCY_SHIFT_SECS = 2` (conservative rounding; +1-week re-check
  per `_GLOSSARY.md`).

Harnesses: `scripts/probe_activity_ws.py` (feed discovery/attribution re-check),
`scripts/measure_activity_ws_latency.py` (latency), `scripts/soak_activity_ws.py`
(continuity + bench-wallet reaction capture). Re-run the probe before each deploy
relying on the feed (unofficial contract).
