# 01 — Phase Foundation

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule, toolchain pin, lints, and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for the authoritative `core-types` definitions.

## Objective

Create the Rust substrate: workspace, crates, types, schemas, event log, fake connectors, CI, and deterministic replay. No live trading is built before this foundation exists.

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
│   ├── source-trader/                # consolidated; venue-specific behavior in submodules
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

`source-trader` is one crate with venue-specific submodules (`polymarket`, `kalshi`); it does not split per venue at the crate boundary because identity-safety logic must be cross-venue.

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
```

Lints are in `_BASELINE.md` ("Required workspace lints"). Per-crate `float_arithmetic = "deny"` overrides apply to `venue-*`, `risk-engine`, `kelly-sizer`, and `execution-core`.

Every GitHub Actions runner, Docker image, and AWS build step must verify `rustc --version` contains `1.95.0` before compiling.

## Core types

Authoritative definitions are in `_GLOSSARY.md` ("Type aliases") and live in `crates/core-types`. Primitive obsession is forbidden. The illustrative subset most relevant to foundation work:

```rust
pub struct VenueMarketId(pub String);
pub struct ResolverCardId(pub uuid::Uuid);
pub struct SourceId(pub String);
pub struct EventSeq(pub u64);
pub struct KalshiPriceCents(pub u8);
pub struct PolymarketPriceDecimal(pub rust_decimal::Decimal);
pub struct ContractQty(pub u64);
pub struct ProbabilityPpm(pub u32);
pub struct BasisPoints(pub i32);
pub struct SourceTimestamp(pub time::OffsetDateTime);
pub struct ReceivedAt(pub time::OffsetDateTime);
pub struct WalletAddress(pub [u8; 20]);
```

If a doc uses a type that is not in `_GLOSSARY.md`, that is a glossary bug; fix the glossary, not the doc.

## Dependency rules

- Source crates cannot depend on venue crates.
- Venue crates cannot depend on strategy crates.
- Strategy crates cannot submit orders.
- Risk engine cannot call external APIs.
- Model crates cannot mutate venue state.
- Replay uses production crates, not duplicate research logic.

## CI gates

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo nextest run --workspace`
- `cargo deny check`
- `cargo audit`
- schema generation check
- replay fixture check
- hot-path benchmark regression check (where applicable)
- docs-check: `_BASELINE.md`, `_GLOSSARY.md`, `AGENTS.md`, `SKILLS.md` exist and are referenced from `README.md`

## Foundation milestone

Build fake source and fake venue connectors first. A fake source emits source events; a fake venue emits book events and accepts test `OrderIntent`s. The initial strategy runs completely offline and is replayable.

## Strategy 0 foundation crates

Winner-Follow is built as separate crates so it cannot leak venue-specific shortcuts into the core engine:

```text
crates/
├── trader-index/             # public trader discovery, ledgers, identity-safe mappings
├── copy-signal-engine/       # entry/add/trim/exit/flip classification and watchlist scanning
├── kelly-sizer/              # calibrated fractional-Kelly and portfolio caps
└── strategy-winner-follow/   # strategy implementation that emits OrderIntent only
```

## Required root instruction files

The repository root must include:

- `AGENTS.md` — mandatory instructions for Codex/Claude/other coding agents.
- `SKILLS.md` — project-specific implementation skills, quality bars, and source lookup rules.

Coding agents must read both files (and `_BASELINE.md`, `_GLOSSARY.md`, `19-WINNER-FOLLOW-STRATEGY.md`) before writing code. The CI docs-check job fails when any of these are absent, empty, or not referenced from `README.md`.
