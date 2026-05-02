# 09 — Cross-Venue Mismatches and Hedges

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`00-PROJECT-OVERVIEW.md`](00-PROJECT-OVERVIEW.md) "Edge taxonomy" — `CompatibilityClass` below refines the **Cross-venue mismatch** edge.

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

Each class maps to allowed cross-venue strategies:

| Class | Hedge OK | Lead/lag OK | Reprice OK |
|---|---|---|---|
| `SameResolver` | ✅ | ✅ | ✅ |
| `SameUnderlyingDifferentResolver` | ⚠ size-haircut | ✅ | ✅ |
| `CorrelatedUnderlying` | ❌ | ✅ | ❌ |
| `TitleOnlySimilarity` | ❌ | research only | ❌ |
| `Incompatible` | ❌ | ❌ | ❌ |

## Compatibility dimensions

Compare resolver source, time window, time zone, rounding, tie/equality, finality, revisions, outcome definition, fee model, liquidity, and settlement delay. The resolver-card sub-types used for these comparisons (`TimingRule`, `WindowSpec`, `RoundingRule`, `TieRule`, `FinalityRule`, `RevisionPolicy`, `OutputSpace`) are defined in `_GLOSSARY.md`.

## Examples

### Crypto

Polymarket short-horizon crypto can resolve to Chainlink stream data while Kalshi crypto can resolve to benchmark averages. `SameUnderlyingDifferentResolver`. Useful for lead-lag and basis; unsafe as a perfect hedge.

### Weather

Kalshi may use NWS final climate report while Polymarket may use a station history source. `SameUnderlyingDifferentResolver` in most cases. Use shared raw observations but separate settlement engines.

### Sports

Same game can differ by official source, start/void/cancel behavior, and stat correction policy. Often `CorrelatedUnderlying` rather than identical.

## Cross-venue strategies

- Lead/lag detector.
- Resolver basis trade.
- Hedge-like risk reduction only when `class = SameResolver`.
- Stale-side repricing after official source event.

## Risk

Cross-venue risk engine applies correlation haircuts, venue-specific exposure caps, source disagreement rules, and unwind plans.

## Winner-Follow cross-venue rule

A leader trade on Polymarket is not automatically a Kalshi trade signal, and a Kalshi market-flow move is not automatically a Polymarket copy signal. Cross-venue use of Winner-Follow requires a compatibility card with `class ∈ {SameResolver, SameUnderlyingDifferentResolver}` AND:

- same underlying event;
- compatible settlement window;
- compatible contract payoff;
- sufficient liquidity on the target venue (see "very liquid market" threshold in `_GLOSSARY.md` if a market order is contemplated);
- no regulatory/account limitation;
- no fake hedge caused by different resolver language.

If compatibility is incomplete, the system uses the leader trade as a research alert but not as an automatic hedge or copy order.
