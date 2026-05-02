# Baseline — Rust toolchain, conventions, and acceptance gate

This document is referenced by every other file in `docs/`. Update it once; do not duplicate its contents inline.

## Rust-only implementation rule

All first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

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

## Required workspace lints

```toml
[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
float_arithmetic = "warn"

# Per-crate override (in venue-*, risk-engine, kelly-sizer, execution-core):
#   [lints.clippy]
#   float_arithmetic = "deny"
```

`unsafe` is forbidden by default at the workspace level (`#![forbid(unsafe_code)]`); a per-crate override requires a separate review.

## Common acceptance gate

A document or its corresponding implementation is complete only when:

1. it compiles as Rust 2024 with `rustc --version` containing `1.95.0`;
2. it uses the typed IDs, prices, probabilities, quantities, timestamps, and resolver states defined in `_GLOSSARY.md` and `core-types`;
3. it writes replayable events with raw payload hashes, schema versions, and parser versions;
4. it has fixture tests and deterministic replay (the same crates run in production and replay);
5. it blocks live execution when source, resolver, venue, or risk state is invalid.

## Code-block convention

Rust snippets in these docs are illustrative. Field visibility, derives, error variants, and trait bounds are elided for readability. Authoritative definitions live in the named crate (see `_GLOSSARY.md`) and in `core-types`. Where a snippet diverges from the canonical type, the canonical type wins.
