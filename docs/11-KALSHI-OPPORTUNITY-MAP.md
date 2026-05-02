# 11 — Kalshi Opportunity Map

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Map Kalshi market families to Rust modules and edge types.

## Build priority

1. Weather.
2. Crypto benchmark windows.
3. Macro releases.
4. Chart/ranking pages.
5. Sports official-state markets.
6. Official-stat benchmarks.
7. Text/mention/document markets.
8. Politics/elections only after compliance review.

## Weather

**Edge:** station/report source edge.  
**Build:** station bus, NWS finalizer, local-standard-time engine, report watcher, replay corpus.  
**Trade:** late-day nowcast and post-report reprice.

## Crypto

**Edge:** benchmark-window path edge.  
**Build:** sample collector, window accumulator, exchange microstructure predictor.  
**Trade:** pre-window anticipation, mid-window path probability, post-window stale price.

## Macro

**Edge:** official release parser.  
**Build:** Rust release scheduler, file watcher, typed parser, revision detector.  
**Trade:** immediate post-release parsing.

## Spotify/Netflix/Apple charts

**Edge:** official ranking publication.  
**Build:** page watcher, rank snapshot hash, entity normalization, replay archive.  
**Trade:** stale price after page publication; predictive rank movement only after calibration.

## Sports

**Edge:** official state plus venue lag.  
**Build:** finite-state game engine, official/live status, stat correction detector, start/cancel handling.  
**Trade:** official-status repricing with conservative fill model.

## Official-stat pages

**Edge:** official page/table publication.  
**Build:** table parser, value hash, cadence detector, revision policy.  
**Trade:** first-published value repricing.

## Opportunity score

```rust
pub struct OpportunityScore {
    pub resolver_clarity: u8,
    pub source_speed_edge: u8,
    pub parser_reliability: u8,
    pub liquidity: u8,
    pub backtestability: u8,
    pub compliance_safety: u8,
    pub engineering_cost: u8,
}
```

Start where resolver clarity and backtestability are highest.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow Kalshi opportunity map

Kalshi's main role in Strategy 0 is not anonymous trader copying. It is:

1. authorized signal-provider copying where a trader consents;
2. leaderboard research where public data is sufficient but not assumed to map to live trades;
3. market-flow analytics from public trades;
4. cross-venue confirmation of Polymarket leader trades;
5. future expansion if Kalshi publishes official public trader-level endpoints.

The implementation must keep `KalshiLeaderSignal::Authorized` separate from `KalshiMarketFlowSignal::Anonymous` so the strategy cannot accidentally copy unidentified users.
