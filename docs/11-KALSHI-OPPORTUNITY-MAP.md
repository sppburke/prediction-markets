# 11 — Kalshi Opportunity Map

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.

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
**Build:** sample collector, window accumulator (`SamplePolicy` from `_GLOSSARY.md`), exchange microstructure predictor.
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
**Build:** table parser, value hash, cadence detector, revision policy (`RevisionPolicy` from `_GLOSSARY.md`).
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

## Winner-Follow Kalshi opportunity map

Kalshi's main role in Strategy 0 is not anonymous trader copying. It is:

1. authorized signal-provider copying where a trader consents;
2. leaderboard research where public data is sufficient but not assumed to map to live trades;
3. market-flow analytics from public trades;
4. cross-venue confirmation of Polymarket leader trades (per the compatibility rules in `09-`);
5. future expansion if Kalshi publishes official public trader-level endpoints.

`KalshiLeaderSignal::Authorized` is kept distinct from `KalshiMarketFlowSignal::Anonymous` so the strategy cannot accidentally copy unidentified users.
