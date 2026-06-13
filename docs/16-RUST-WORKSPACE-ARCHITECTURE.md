# 16 — Rust Workspace Architecture

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule, lints, and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for type aliases.

## Objective

Define concrete crate boundaries and dependency rules for the Rust implementation.

## Dependency direction

```text
core-types
  -> config, event-log, resolver-card, source-core, venue-core
      -> source-* and venue-*
      -> model-core and model-*
      -> strategy-core, trader-index, copy-signal-engine, kelly-sizer,
         strategy-winner-follow, execution-core, risk-engine
      -> replay, backtest, service, cli
```

Forbidden:

- `venue-kalshi` depends on `venue-polymarket`;
- venue crates depend on strategy crates;
- source crates depend on venue crates;
- model crates submit orders;
- risk engine calls network APIs.

## Core crates

### `core-types`

Authoritative home for all newtypes listed in `_GLOSSARY.md` ("Type aliases"): IDs, prices, ticks, quantities, probabilities, basis points, timestamps, hashes, source IDs, venue IDs, and market families.

### `event-log`

Append/read event envelopes; local framed log; BLAKE3 hash chain; zstd compression; Parquet export.

### `resolver-card`

Resolver schema, parser/validator, JSON Schema, property tests for timing/rounding/tie/finality. Sub-types defined in `_GLOSSARY.md` ("Resolver-card sub-types"); a fully populated example is in `_GLOSSARY.md` ("Example resolver card").

### `source-core`

Connector trait, source health, source manifests, retry/backoff, source event envelope. Source-freshness defaults are in `_GLOSSARY.md`.

### `venue-core`

Venue adapter trait, book state, order intent, order lifecycle, venue health.

### `execution-core`

Order journal, idempotency keys (key definition in `_GLOSSARY.md`), order typestate, router abstractions.

### `risk-engine`

Pure risk checks with typed inputs and deterministic decisions. The risk-block taxonomy and halt scope is canonical in `19-WINNER-FOLLOW-STRATEGY.md`.

## Internal APIs

`axum` for control endpoints:

- `/health/live`
- `/health/ready`
- `/metrics`
- `/risk/kill-switch`
- `/sources`
- `/venues`
- `/orders`
- `/replay/status`

## Event log evolution

1. Local framed event log for MVP.
2. `async-nats` JetStream or Kafka-compatible stream for production durability.
3. Parquet/Arrow lakehouse queried by DataFusion and Polars.

## Build profile

Authoritative rule and rationale: see `_BASELINE.md` "Build profile". Thin LTO applies to every production-tier profile (`release`, `bench`, `profiling`); dev and test keep Cargo defaults.

```toml
[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"
strip = "symbols"

[profile.bench]
inherits = "release"

[profile.profiling]
inherits = "release"
strip = "none"
debug = "line-tables-only"
```

`profiling` is the build used for flamegraphs and latency work — same codegen as `release` so numbers stay representative, but with symbols retained.

## Winner-Follow crate boundaries

- `trader-index` may depend on venue data types and event-log types but not execution. Ranking and concentration accounting are per-wallet.
- `copy-signal-engine` may classify leader trades but cannot size or route orders.
- `kelly-sizer` is pure math with deterministic inputs and property tests.
- `strategy-winner-follow` consumes ranked leader signals and emits `OrderIntent` only.
- `execution-core` owns submission, cancellation, reconciliation, and idempotency.

This separation prevents a public-data scanner from becoming an unreviewed trading bot.
