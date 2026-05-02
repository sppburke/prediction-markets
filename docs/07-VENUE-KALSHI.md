# 07 — Venue Playbook: Kalshi

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

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

```rust
pub struct KalshiMarketTicker(String);
pub struct KalshiEventTicker(String);
pub struct KalshiOrderId(String);
pub struct KalshiPriceCents(u8);
pub struct KalshiCountFp(Decimal);
pub enum KalshiSide { Yes, No }
pub enum KalshiOrderStatus { Resting, PartiallyFilled, Filled, Cancelled, Rejected, Unknown }
```

## WebSocket rules

- Build book from snapshot then deltas.
- Detect gaps and resubscribe.
- Emit typed `VenueEvent`s.
- Track heartbeat and staleness.
- Reconcile after reconnect.
- No strategy reads raw JSON.

## Queue-aware execution

Kalshi queue data is a strategy feature. Passive orders require fill probability, queue position, source freshness, and adverse-selection checks. Cancel when resolver state moves, source health fails, queue deteriorates, or venue state becomes stale.

## Order groups and combos

Order groups should be used for fast related orders once live size exceeds tiny mode. Combos are feature-gated until liquidity, fill, and unwind behavior are tested.

## Strong Kalshi verticals

- Weather: NWS final report plus station nowcast.
- Crypto: benchmark-window accumulator.
- Macro: official release parser.
- Charts: Spotify/Netflix/Apple official page snapshots.
- Sports: official status/result source with correction handling.
- Official-stat pages: gas, fertilizer, rankings, other published benchmarks.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow implications for Kalshi

Kalshi is not treated as equivalent to Polymarket for trader-level copy trading. The public trade feed gives market-level executions, not a public trader identity in each trade message. Kalshi's leaderboard is an opt-in performance feature, but leaderboard appearance alone is not enough to reconstruct live trades for copying.

Therefore:

- enable Kalshi Winner-Follow only for traders who explicitly authorize portfolio/API access or where Kalshi publishes official public trader-level data sufficient for lawful attribution;
- otherwise use Kalshi data for market-flow signals, liquidity/queue analytics, and resolver/source strategies;
- never infer that a public Kalshi trade belongs to a leaderboard trader without evidence;
- never attempt credential sharing, scraping private data, or bypassing platform privacy controls.
