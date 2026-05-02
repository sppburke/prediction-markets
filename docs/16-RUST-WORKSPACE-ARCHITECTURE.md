# 16 — Rust Workspace Architecture

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Define concrete crate boundaries and dependency rules for the Rust implementation.

## Dependency direction

```text
core-types
  -> config, event-log, resolver-card, source-core, venue-core
      -> source-* and venue-*
      -> model-core and model-*
      -> strategy-core, trader-index, copy-signal-engine, kelly-sizer, strategy-winner-follow, execution-core, risk-engine
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

Newtypes for IDs, prices, ticks, quantities, probabilities, basis points, timestamps, hashes, source IDs, venue IDs, market families.

### `event-log`

Append/read event envelopes; local framed log; BLAKE3 hash chain; zstd compression; Parquet export.

### `resolver-card`

Resolver schema, parser/validator, JSON Schema, property tests for timing/rounding/tie/finality.

### `source-core`

Connector trait, source health, source manifests, retry/backoff, source event envelope.

### `venue-core`

Venue adapter trait, book state, order intent, order lifecycle, venue health.

### `execution-core`

Order journal, idempotency keys, order typestate, router abstractions.

### `risk-engine`

Pure risk checks with typed inputs and deterministic decisions.

## Internal APIs

Use Axum for control endpoints:

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

```toml
[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"
strip = "symbols"
```

Keep a profiling build with symbols for latency work.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow crate boundaries

- `trader-index` may depend on venue data types and event-log types but not execution.
- `copy-signal-engine` may classify leader trades but cannot size or route orders.
- `kelly-sizer` is pure math with deterministic inputs and property tests.
- `strategy-winner-follow` consumes ranked leader signals and emits `OrderIntent` only.
- `execution-core` owns submission, cancellation, reconciliation, and idempotency.

This separation prevents a public-data scanner from becoming an unreviewed trading bot.
