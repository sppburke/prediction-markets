# 29 — Polymarket `/activity` Attribution-Latency Measurement

**What & why.** Copy-trading a *named* leader on Polymarket can only use the REST
`data-api.polymarket.com/activity?user=<wallet>` feed — the CLOB WebSocket print is
wallet-anonymous (`docs/15-SOURCES.md`, issues #282/#300). The binding question for
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

End-to-end observe→act latency = `/activity` indexing (~1–4s, p99 5s) + the live poller's
interval (5–10s, `service.trade_poll_interval_secs`) + order placement (~1–5s) ≈ **~10–20s
typical, < ~30s at the tail.**

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
