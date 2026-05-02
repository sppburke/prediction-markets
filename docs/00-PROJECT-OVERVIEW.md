# 00 — Project Overview

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

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


## Strategy 0 — Winner-Follow Copy Engine

The first production strategy is **Winner-Follow**: continuously discover, rank, watch, and selectively copy the fastest-compounding public traders before the broader market has fully incorporated their action. This is not treated as a risk-free or "edge-free" system. The edge is the combination of public trader intelligence, rigorous skill filtering, latency-optimized copy execution, fractional-Kelly sizing, portfolio-level drawdown control, and continuous decay monitoring.

Winner-Follow ships before weather, crypto, macro, sports, and chart/source-arbitrage strategies because it can be built using public venue/profile/trade data, deterministic analysis, and speed. Resolver-source strategies remain Strategy 1+ and are used later to validate whether copied trades have independent fundamental support.

### Venue support

- **Polymarket:** first-class support. The public Data API exposes leaderboard, user trades, positions, activity, and related profile data, making trader-level reconstruction feasible.
- **Kalshi:** limited trader-copy support. Kalshi exposes public trades and a leaderboard feature, but public trade messages do not identify the trader and leaderboard participation is opt-in. Kalshi is therefore used for copy-trading only when a lawful public identity-to-trade mapping exists, when a trader explicitly authorizes API/portfolio access, or when future official endpoints expose sufficient public trader-level data. Otherwise, Kalshi remains a venue for resolver-source strategies, market-flow analytics, and cross-venue checks.

### Ranking objective

Rank traders by **walk-forward lower-confidence expected log-growth per day** for a follower account after simulated latency, spread, slippage, fees, partial fills, and position caps. Raw PnL, win rate, and leaderboard rank are inputs, not the final ranking target.

### Default eligibility thresholds

The user-proposed `average hold < 5 days` and `>= 15 trades` are too loose for production. Use adaptive thresholds instead:

- at least **60 closed trades** or **30 resolved markets** in the rolling 180-day audit window;
- at least **12 closed trades in the last 30 days** for active-copy eligibility;
- median capital-weighted holding period **<= 72 hours**;
- 75th percentile holding period **<= 7 days**;
- minimum simulated follower turnover of **0.35 bankroll-equivalent per day** after caps;
- positive lower 5% bootstrap estimate of daily log growth after copy delay and costs;
- no single resolved market contributes more than **20%** of audited profit;
- no more than **35%** of audited profit comes from positions too illiquid for the follower to enter within the latency/slippage budget.

### Default sizing

For a binary contract with current entry price `c` and calibrated copied-trade win probability `p`, the full-Kelly bankroll allocation to stake cost is:

```text
f_full = max(0, (p - c) / (1 - c))
f_live = kelly_fraction * f_full
```

Use **quarter Kelly** by default during live-tiny and scale to half Kelly only after a statistically meaningful live audit. `p` is not the trader's naive win rate. It is a calibrated, shrinkage-adjusted probability conditional on trader, market family, odds bucket, liquidity, holding-period bucket, side, recency, and observed copy latency.

Hard caps override Kelly: max 0.25% bankroll per copied trade in live-tiny, max 1.00% after promotion, max 3.00% per trader, max 8.00% per market family, max 25.00% total open copy exposure, and stop new entries after 2.00% intraday drawdown or 6.00% rolling 7-day drawdown until review.


## Core trading thesis

For every market, answer:

1. What exact source settles this market?
2. What is the fastest permitted upstream source for that resolver?
3. What orthogonal source can confirm or challenge the signal?
4. How does Kalshi or Polymarket expose tradability, book state, fees, queue, and settlement?
5. Does the edge survive latency, fees, slippage, and fill probability?
6. Is there a related market on the other venue, and is it a real hedge or a fake hedge?

## Why Rust

Rust is used because this system needs low-latency networked services, strict type safety, high concurrency, no garbage collector pauses, reproducible binaries, and a testing ecosystem that can validate concurrent systems. Rust also lets the code encode the domain: prices, probabilities, order states, resolver states, time windows, finality, and market IDs should be distinct types, not loose strings/floats.


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

Kalshi is especially attractive for official-source markets: weather reports, benchmark crypto windows, government releases, official charts/rankings, and queue-aware passive strategies. Kalshi must have a dedicated Rust adapter that understands order books, queue position, order groups, combos, and exchange schedule.

### Polymarket

Polymarket is especially attractive for broad coverage, short-horizon crypto, live sports, market/event structure, and rapid repricing gaps. Polymarket must have a dedicated Rust adapter that understands market/user/sports/RTDS sockets, signed orders, condition/token IDs, category costs, and market-specific resolution sources.

## ResolverCard requirement

A market is not tradable until it compiles into a reviewed `ResolverCard`:

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

- **Exact reprice:** official source moved; venue lagged.
- **Predictive upstream:** upstream source implies future resolver state.
- **Cross-venue mismatch:** venues price related but non-identical outcomes incorrectly.
- **Microstructure:** queue, fees, rebates, stale orders, or venue-specific mechanics create tradable edge.
- **Finality:** market underprices certainty after a result is functionally finalized but before formal resolution.


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.
