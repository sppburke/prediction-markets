# 01 — Phase Foundation

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Create the Rust substrate: workspace, crates, types, schemas, event log, fake connectors, CI, and deterministic replay. No live trading should be built before this foundation exists.

## Workspace layout

```text
prediction-edge/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── deny.toml
├── crates/
│   ├── core-types/
│   ├── config/
│   ├── event-log/
│   ├── resolver-card/
│   ├── source-core/
│   ├── source-trader/
│   ├── source-weather/
│   ├── source-crypto/
│   ├── source-sports/
│   ├── source-macro/
│   ├── source-charts/
│   ├── source-events/
│   ├── venue-core/
│   ├── venue-kalshi/
│   ├── venue-polymarket/
│   ├── model-core/
│   ├── model-weather/
│   ├── model-crypto/
│   ├── strategy-core/
│   ├── strategy-winner-follow/
│   ├── trader-index/
│   ├── copy-signal-engine/
│   ├── kelly-sizer/
│   ├── execution-core/
│   ├── risk-engine/
│   ├── replay/
│   ├── backtest/
│   ├── observability/
│   ├── cli/
│   └── service/
├── schemas/
├── services/
├── tests/
└── docs/
```

## Toolchain

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.95.0"
components = ["rustfmt", "clippy", "llvm-tools-preview"]
profile = "minimal"
```

Workspace baseline:

```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
edition = "2024"
rust-version = "1.95"

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
float_arithmetic = "warn"
```

## Core types

Do not allow primitive obsession:

```rust
pub struct VenueMarketId(String);
pub struct ResolverCardId(uuid::Uuid);
pub struct SourceId(String);
pub struct EventSeq(u64);
pub struct KalshiPriceCents(u8);
pub struct PolymarketPriceDecimal(rust_decimal::Decimal);
pub struct ContractQty(u64);
pub struct ProbabilityPpm(u32);
pub struct BasisPoints(i32);
pub struct SourceTimestamp(time::OffsetDateTime);
pub struct ReceivedAt(time::OffsetDateTime);
```

## Dependency rules

- Source crates cannot depend on venue crates.
- Venue crates cannot depend on strategy crates.
- Strategy crates cannot submit orders.
- Risk engine cannot call external APIs.
- Model crates cannot mutate venue state.
- Replay uses production crates, not duplicate research logic.

## CI gates

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features`
- `cargo test --workspace`
- `cargo nextest run --workspace`
- dependency audit/deny
- schema generation check
- replay fixture check
- hot-path benchmark regression check where applicable


## Rust baseline

- **Edition/toolchain:** Rust 2024 Edition, stable Rust 1.95.0 toolchain, `Cargo.lock` committed, reproducible builds.
- **Async:** `tokio`, `tokio-tungstenite`, `reqwest`/`hyper`, `tower`, `axum`.
- **Serialization/parsing:** `serde`, `serde_json`, `simd-json` for benchmarked hot paths, `csv`, `quick-xml`, `scraper`, `schemars`.
- **Numerics:** `rust_decimal` and integer ticks/cents/basis-points. No raw `f64` for venue prices, money, contract counts, or probabilities.
- **Analytics:** Rust `polars`, Apache Arrow/Parquet, `datafusion`.
- **ML/inference:** Rust `linfa`, `burn`, `candle`, and/or `ort` for ONNX inference. No production Python model server.
- **Messaging/storage:** local append-only framed event log first; `async-nats` JetStream or Kafka/Redpanda-compatible streams later; Parquet lakehouse for replay.
- **Observability:** `tracing`, `tracing-opentelemetry`, OpenTelemetry metrics/logs/traces.
- **Testing:** `proptest`, `loom`, `shuttle`, `cargo-nextest`, `criterion`, `insta`, fake source/venue servers.


## Foundation milestone

Build fake source and fake venue connectors first. A fake source emits source events; a fake venue emits book events and accepts test `OrderIntent`s. The initial strategy should run completely offline and be replayable.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Strategy 0 foundation crates

Winner-Follow is built as separate crates so it cannot leak venue-specific shortcuts into the core engine:

```text
crates/
├── trader-index/             # public trader discovery, ledgers, identity-safe mappings
├── copy-signal-engine/       # entry/add/trim/exit/flip classification and watchlist scanning
├── kelly-sizer/              # calibrated fractional-Kelly and portfolio caps
└── strategy-winner-follow/   # strategy implementation that emits OrderIntent only
```

### Rust 1.95.0 toolchain pin

`rust-toolchain.toml` should pin stable 1.95.0 exactly for reproducible CI and AWS builds:

```toml
[toolchain]
channel = "1.95.0"
components = ["rustfmt", "clippy", "llvm-tools-preview"]
profile = "minimal"
```

Workspace baseline:

```toml
[workspace.package]
edition = "2024"
rust-version = "1.95"
```

Every GitHub Actions runner, Docker image, and AWS build step must verify `rustc --version` contains `1.95.0` before compiling.

## Required root instruction files

The repository root must include:

- `AGENTS.md` — mandatory instructions for Codex/Claude/other coding agents.
- `SKILLS.md` — project-specific implementation skills, quality bars, and source lookup rules.

Coding agents must read both files before writing code. CI should include a docs-check job that fails when either file is absent, empty, or not referenced from `README.md`.
