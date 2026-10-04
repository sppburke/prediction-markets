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
  vs 16 hard disconnects). 2026-08-31 (#546): the stall is connection-local — one
  of several parallel connections freezes while the others deliver every watched
  row; re-subscribing does not revive it; a fresh connection delivers within ~0.8 s;
  the largest activity gap on a healthy connection was 4.775 s. Consequence: three
  independent readers per process with a 30 s normalized-row timeout and direct
  reconnect (`pe-service::activity_ingest`; constants in `_GLOSSARY.md`) and the
  always-on REST poll fallback are load-bearing.
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
relying on the feed (officially listed endpoint; the first-party client publishes
the subscription/payload contract; no published continuity guarantee — `docs/15`).

## #730 Part 1 acceptance measurement

Use [#730 AC10](https://github.com/sppburke/prediction-markets/issues/730) for the fill-cohort
size and latency acceptance bounds. The measurements below are the audit recipe, not a claim
that the deployed service has passed. Keep three populations distinct:

- The first post-deploy websocket-fill cohort, with source identity, continuation version and
  applied configuration fixed per row. Report each stage's available count, median, p90 and
  maximum, plus the slowest decision in every bucket containing multiple decisions.
- Every proven first-entry BUY in the same window from a wallet in the service's **recorded
  membership when it traded**, including wallets subsequently removed and every leader price.
  A current-watchlist join or price-band screen cannot define this population.
- BUY units whose first-entry status depends on ordering inside one source second. List these
  separately; each requires a recorded ambiguity refusal or a justified causal suppression.

Join `decision_pending` by the canonical source group id to its authenticated websocket receipt,
complete activity-page receipts, `activity_groups` and `entry_gate_results`. Use the gateway
receipt's `received_at` as the websocket origin, not the venue's second-granular timestamp.
Measure history-page receipt, `book.fetched_at_unix_ms` and durable fill completion from that
origin. For a fill, durable completion is the recorded `terminal_transition` clock: the paper
owner renders it **after** the synchronized `FinancialFinal`, through
`supabase_state::terminalize_final_fill_decision` and `orchestrator::render_pending_evidence`.
The Final envelope's `received_at` is sampled before sync and is not that endpoint.
No-copy/no-fill terminal clocks do not establish a synchronized financial fill.

Book receipt and book use are separate endpoints. The early `/book` read can finish before
admission; use `book_staleness_check` for the recorded use check, and
`initial_staleness_gate` for the available decision-start boundary. Report their span separately
from websocket-to-book receipt. A continuation lacking either clock has a missing span; do not
substitute an admission receipt, Prepared timestamp, bucket timestamp or a zero. Report missing
clocks, future/negative spans, provenance exclusions and restart-recovered decisions explicitly.
Report 429 retry time, reads per confirmation, retry-interval waits and urgent-slot waits
separately from page transport and decision work; unavailable wait clocks remain unknown.

For a paired projection, freeze the same recorded fill identities and original stage endpoints
on both sides. Attach the benchmarked scan costs and measured gate waits/serial reads to each
row, subtract only the work the implemented change removes or overlaps, then recompute each
row's projected endpoints before taking population percentiles. Keep queue delay and the
unmeasured tail unchanged; do not subtract one aggregate median from another. Report inputs,
sample counts, missing spans and the matched before/projected distributions. New confirmations
and recovered first entries are separate populations; they are not gains measured by this paired
fill projection. Deployed measurements remain the acceptance authority.

Run the acceptance audit read-only on captured verified log prefixes and a consistent database
snapshot; record deployment revision, config hash, era, window and prefix identities. Prove the
first BUY from complete attributable public `/activity` history and join it to the recorded
membership and local group/gate/decision evidence. Classify every member of the audit population
as filled, correctly refused, refused by a retained Part 2 limitation (thin book, opposite outcome
or VWAP rounding), or suppressed by a required causal re-anchor. Trace a suppression to the
causing groups in `activity_groups` insertion order, not only the recorded trigger id: a mixed
bucket may name a twin while a genuinely new or late group requires the flag. Covered non-twin
arrivals, unknown-condition redemptions and unresolved non-combo identities retain their
[canonical causal rule](_GLOSSARY.md#causal-re-anchor-and-rehearsal-rules-557).
All-twin buckets and known-condition redemptions/combos reaching ordinary routing must not
create re-anchors. Any other miss fails acceptance; an unproved first-entry status is reported
as unknown rather than silently removed.

Inspect the next daily marks against recorded closure proof and available samples under the
canonical closed-mark rule. A usable in-lookback sample with valid closure proof must not produce
an invalid midnight mark; cases lacking that evidence retain their recorded fail-closed cause.
Post the audit results to #730, then the final results to #588 and #530; issue closure waits for
AC16. This recipe authorizes no production mutation or live order.
