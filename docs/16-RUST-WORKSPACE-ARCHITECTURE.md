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
      -> operator-graph
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
- risk engine calls network APIs;
- `operator-graph` calls network APIs or depends on execution/strategy crates.

## Core crates

### `core-types`

Authoritative home for all newtypes listed in `_GLOSSARY.md` ("Type aliases"): IDs, prices, ticks, quantities, probabilities, basis points, timestamps, hashes, source IDs, venue IDs, market families, operator/funder identities.

### `event-log`

Append/read event envelopes; local framed log; BLAKE3 hash chain; zstd compression; Parquet export.

### `resolver-card`

Resolver schema, parser/validator, JSON Schema, property tests for timing/rounding/tie/finality. Sub-types defined in `_GLOSSARY.md` ("Resolver-card sub-types"); a fully populated example is in `_GLOSSARY.md` ("Example resolver card").

### `source-core`

Connector trait, source health, source manifests, retry/backoff, source event envelope. Source-freshness defaults are in `_GLOSSARY.md`.

### `source-onchain-polygon`

Public Polygon event connector for Winner-Follow identity research. Ingests pUSD, USDC/USDC.e, proxy-wallet, deposit/onramp, and funding-path evidence where publicly derivable. Emits normalized source events with raw hashes and parser versions; does not perform clustering or ranking.

### `operator-graph`

Pure logic for wallet-to-operator clustering, funder-root identity, inherited priors, cluster-coordination features, and anti-gaming flags (concrete thresholds in `_GLOSSARY.md`). Consumes event snapshots and config, emits deterministic `OperatorIdentity` and `OperatorTrackRecord` snapshots, has no I/O.

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

```toml
[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"
strip = "symbols"
```

Keep a profiling build with symbols for latency work.

## Winner-Follow crate boundaries

- `source-onchain-polygon` may ingest public chain/collateral events but cannot classify leaders, size trades, or submit orders.
- `operator-graph` may build deterministic identities, reputations, priors, and anti-gaming flags but cannot call external systems.
- `trader-index` may depend on venue data types and event-log types but not execution.
- `trader-index` consumes operator snapshots to collapse wallets into `OperatorId` views for ranking and concentration accounting.
- `copy-signal-engine` may classify leader trades but cannot size or route orders.
- `kelly-sizer` is pure math with deterministic inputs and property tests.
- `strategy-winner-follow` consumes ranked leader signals and emits `OrderIntent` only.
- `execution-core` owns submission, cancellation, reconciliation, and idempotency.

This separation prevents a public-data scanner from becoming an unreviewed trading bot.
