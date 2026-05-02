# 09 — Cross-Venue Mismatches and Hedges

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Use Kalshi and Polymarket together without pretending similar titles are equivalent.

## Compatibility classes

```rust
pub enum CompatibilityClass {
    SameResolver,
    SameUnderlyingDifferentResolver,
    CorrelatedUnderlying,
    TitleOnlySimilarity,
    Incompatible,
}
```

## Compatibility dimensions

Compare resolver source, time window, time zone, rounding, tie/equality, finality, revisions, outcome definition, fee model, liquidity, and settlement delay.

## Examples

### Crypto

Polymarket short-horizon crypto can resolve to Chainlink stream data while Kalshi crypto can resolve to benchmark averages. Same underlying, different resolver. Useful for lead-lag and basis; unsafe as a perfect hedge.

### Weather

Kalshi may use NWS final climate report while Polymarket may use a station history source. Same broad weather outcome, different finalizer. Use shared raw observations but separate settlement engines.

### Sports

Same game can differ by official source, start/void/cancel behavior, and stat correction policy. Often correlated rather than identical.

## Cross-venue strategies

- Lead/lag detector.
- Resolver basis trade.
- Hedge-like risk reduction only when compatibility is high.
- Stale-side repricing after official source event.

## Risk

Cross-venue risk engine applies correlation haircuts, venue-specific exposure caps, source disagreement rules, and unwind plans.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow cross-venue rule

A leader trade on Polymarket is not automatically a Kalshi trade signal, and a Kalshi market-flow move is not automatically a Polymarket copy signal. Cross-venue use of Winner-Follow requires a compatibility card:

- same underlying event;
- compatible resolution source;
- compatible settlement window;
- compatible contract payoff;
- sufficient liquidity on the target venue;
- no regulatory/account limitation;
- no fake hedge caused by different resolver language.

If compatibility is incomplete, the system may use the leader trade as a research alert but not as an automatic hedge or copy order.
