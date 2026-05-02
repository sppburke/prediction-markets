# 07 — Venue Playbook: Kalshi

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for type aliases and Kalshi rate-limit defaults.

## Objective

Implement Kalshi as a dedicated Rust venue adapter. Kalshi has official-source-heavy markets, queue position, order groups, combos, live WebSocket data, REST market data, and venue-specific settlement/operational rules.

## Crate

`venue-kalshi`

Responsibilities:

- REST client;
- WebSocket client;
- authentication/signing;
- market discovery;
- order book snapshots/deltas;
- trade/fill/position streams;
- queue-position integration;
- order groups;
- combo handling behind feature flag;
- exchange schedule/maintenance awareness;
- local order journal and reconciliation;
- venue-specific errors.

## Types

Authoritative in `core-types`; illustrative subset:

```rust
pub struct KalshiMarketTicker(pub String);
pub struct KalshiEventTicker(pub String);
pub struct KalshiOrderId(pub String);
pub struct KalshiPriceCents(pub u8);
pub struct KalshiCountFp(pub Decimal);
pub enum KalshiSide { Yes, No }
pub enum KalshiOrderStatus { Resting, PartiallyFilled, Filled, Cancelled, Rejected, Unknown }
```

## WebSocket rules

- Build book from snapshot then deltas.
- Detect gaps and resubscribe.
- Emit typed `VenueEvent`s.
- Track heartbeat and staleness against `_GLOSSARY.md` "Source freshness defaults" (Kalshi WS stale = 2 s, block = 6 s).
- Reconcile after reconnect.
- No strategy reads raw JSON.

## Queue-aware execution

Kalshi queue data is a strategy feature. Passive orders require fill probability, queue position, source freshness, and adverse-selection checks. Cancel when resolver state moves, source health fails, queue deteriorates, or venue state becomes stale.

## Order groups and combos

Order groups are used for fast related orders once live size exceeds tiny mode. Combos are feature-gated until liquidity, fill, and unwind behavior are tested.

## Strong Kalshi verticals

- Weather: NWS final report plus station nowcast.
- Crypto: benchmark-window accumulator.
- Macro: official release parser.
- Charts: Spotify/Netflix/Apple official page snapshots.
- Sports: official status/result source with correction handling.
- Official-stat pages: gas, fertilizer, rankings, other published benchmarks.

## Winner-Follow implications for Kalshi

Kalshi is not equivalent to Polymarket for trader-level copy trading. The public trade feed gives market-level executions, not a public trader identity per trade message. Kalshi's leaderboard is opt-in, but leaderboard appearance alone is not enough to reconstruct live trades for copying.

Therefore:

- enable Kalshi Winner-Follow only for traders who explicitly authorize portfolio/API access or where Kalshi publishes official public trader-level data sufficient for lawful attribution;
- otherwise use Kalshi data for market-flow signals, liquidity/queue analytics, and resolver/source strategies;
- never infer that a public Kalshi trade belongs to a leaderboard trader without evidence;
- never attempt credential sharing, scraping private data, or bypassing platform privacy controls.

`KalshiLeaderSignal::Authorized` and `KalshiMarketFlowSignal::Anonymous` are kept as separate types so the strategy cannot accidentally copy unidentified users.
