# 00 — Project Overview

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule, toolchain pin, lints, and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for vocabulary, type aliases, latency budget, rate limits, and configuration defaults.
> See [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md) for canonical risk caps, Kelly fractions, eligibility thresholds, and promotion ladders.

## Mission

Build a **Rust-native, resolver-first, cross-venue prediction-market trading system** for Polymarket and Kalshi. The system seeks edge from faster and more accurate interpretation of settlement sources, not from generic sentiment or title matching.

The system optimizes:

1. settlement accuracy;
2. source speed;
3. venue-aware execution;
4. cross-venue mismatch detection;
5. operational and legal safety;
6. deterministic replay.

## Architecture thesis

The system is a **resolver mesh** implemented as a Rust workspace.

```text
Official/upstream sources  ─┬─> Rust source gateways ─┬─> append-only event log ─┬─> resolver engines
Venue APIs/WebSockets      ─┼─> Rust venue adapters ──┼─> hot state caches ──────┼─> model workers
Operator config/risk       ─┴─> Rust risk supervisor ─┴─> execution router ──────┴─> venue orders
```

All production decisions must be reproducible from:

- event log hash;
- git SHA;
- config hash;
- resolver-card version;
- model artifact version;
- venue adapter version.

## Strategy 0 — Winner-Follow Copy Engine (summary)

Winner-Follow is the first deployable strategy. Full specification — including venue support, eligibility thresholds, ranking objective, Kelly sizing, risk caps, and the promotion ladder — lives in [`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md). This file does not restate those values; if a number appears here that conflicts with `19`, `19` wins.

Strategy 0 starts here because it can be built using public venue/profile/trade data, deterministic analysis, and speed. Resolver-source strategies remain Strategy 1+ and are used later to validate whether copied trades have independent fundamental support.

Note: "Strategy 0" (strategy index) and "Phase 0" / "Phase 0A" (build phase) are different axes; see `_GLOSSARY.md`.

## Core trading thesis

For every market, answer:

1. What exact source settles this market?
2. What is the fastest permitted upstream source for that resolver?
3. What orthogonal source can confirm or challenge the signal?
4. How does Kalshi or Polymarket expose tradability, book state, fees, queue, and settlement?
5. Does the edge survive latency, fees, slippage, and fill probability?
6. Is there a related market on the other venue, and is it a real hedge or a fake hedge (see `09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md`)?

## Why Rust

Rust is used because this system needs low-latency networked services, strict type safety, high concurrency, no garbage collector pauses, reproducible binaries, and a testing ecosystem that can validate concurrent systems. Rust also lets the code encode the domain: prices, probabilities, order states, resolver states, time windows, finality, and market IDs are distinct types, not loose strings/floats. See `_GLOSSARY.md` for the canonical type aliases.

## Core event model

Every external and internal state transition is wrapped in an event envelope:

```rust
pub struct EventEnvelope<T> {
    pub event_id: uuid::Uuid,
    pub schema_version: u16,
    pub source_id: SourceId,
    pub observed_at: time::OffsetDateTime,
    pub received_at: time::OffsetDateTime,
    pub monotonic_nanos: u128,
    pub parser_version: semver::Version,
    pub payload_hash: blake3::Hash,
    pub data: T,
}
```

## Venue thesis

### Kalshi

Kalshi is especially attractive for official-source markets: weather reports, benchmark crypto windows, government releases, official charts/rankings, and queue-aware passive strategies. Kalshi has a dedicated Rust adapter (`venue-kalshi`) that understands order books, queue position, order groups, combos, and exchange schedule.

### Polymarket

Polymarket is especially attractive for broad coverage, short-horizon crypto, live sports, market/event structure, and rapid repricing gaps. Polymarket has a dedicated Rust adapter (`venue-polymarket`) that understands market/user/sports/RTDS sockets, signed orders, condition/token IDs, category costs, and market-specific resolution sources.

## ResolverCard requirement

A market is not tradable until it compiles into a reviewed `ResolverCard`. Sub-types (`MarketFamily`, `ResolverSource`, `OutputSpace`, `TimingRule`, `WindowSpec`, `SamplePolicy`, `RoundingRule`, `TieRule`, `FinalityRule`, `RevisionPolicy`) are defined in `_GLOSSARY.md`, with a fully populated example resolver card.

```rust
pub struct ResolverCard {
    pub venue: Venue,
    pub market_id: VenueMarketId,
    pub family: MarketFamily,
    pub resolver_source: ResolverSource,
    pub upstream_sources: Vec<ResolverSource>,
    pub output_space: OutputSpace,
    pub timing: TimingRule,
    pub rounding: RoundingRule,
    pub tie_rule: TieRule,
    pub finality: FinalityRule,
    pub revision_policy: RevisionPolicy,
}
```

## Edge taxonomy

| Edge | Definition |
|---|---|
| **Exact reprice** | Official source moved; venue lagged. |
| **Predictive upstream** | Upstream source implies future resolver state. |
| **Cross-venue mismatch** | Venues price related but non-identical outcomes incorrectly. Refined by `CompatibilityClass` in `09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md`. |
| **Microstructure** | Queue, fees, rebates, stale orders, or venue-specific mechanics create tradable edge. |
| **Finality** | Market underprices certainty after a result is functionally finalized but before formal resolution. |

`09-CROSS-VENUE-MISMATCHES-AND-HEDGES.md` extends the cross-venue mismatch entry with the full `CompatibilityClass` enum.
