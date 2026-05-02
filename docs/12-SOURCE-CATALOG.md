# 12 — Source Catalog

> **Rust-only implementation rule:** all first-party production services, clients, parsers, models, replay tools, CLIs, and test harnesses are implemented in **Rust 2024 Edition pinned to stable Rust 1.95.0**. Non-Rust components are permitted only as external infrastructure daemons, vendor APIs, operating-system services, managed databases, or public data sources. No production hot-path Python, Node, or browser automation is allowed.

## Objective

Treat every data source as a typed, versioned Rust connector with access rules, timing, parser behavior, health, provenance, and replay fixtures.

## Source grading

```rust
pub struct SourceGrade {
    pub resolver_closeness: u8,
    pub speed: u8,
    pub reliability: u8,
    pub revision_risk: u8,
    pub parser_fragility: u8,
    pub license_safety: u8,
    pub replayability: u8,
    pub cost: u8,
}
```

## Tier A — named resolver or direct official source

- NWS final climate reports and station observations.
- Exact station history source named by market rules.
- Chainlink or benchmark source named by crypto market rules.
- Official league status/result/stat source.
- BLS/BEA/Census/Fed/Treasury releases.
- Spotify/Netflix/Apple official ranking pages.
- USGS/NHC/NASA FIRMS and official public agency feeds.

## Tier B — upstream precursor

- Exchange books/trades.
- METAR/station observations and forecast models.
- Official file directory and page hash watchers.
- Licensed live sports data.
- Rank movement and update cadence telemetry.

## Tier C — confirmation/replay

- independent station networks;
- multiple exchange feeds;
- archive snapshots;
- official mirrors;
- alternate licensed vendors.

## Source manifest

```rust
pub struct SourceManifest {
    pub source_id: SourceId,
    pub family: SourceFamily,
    pub access_method: AccessMethod,
    pub license_notes: String,
    pub expected_cadence: Option<Duration>,
    pub max_stale: Duration,
    pub parser_version: semver::Version,
    pub replay_fixture_path: String,
}
```

## Access policy

Edge must come from legitimate faster engineering, not access abuse. Do not bypass controls, violate license terms, use non-public material, overwhelm public systems, or evade rate limits.

## Redundancy

Each live source has a primary, confirmation source where possible, stale-source behavior, disagreement behavior, and replay fallback.

```rust
pub enum SourceDisagreementPolicy {
    TradePrimaryOnly,
    ReduceSize,
    BlockTrading,
    RequireManualReview,
}
```


## Common acceptance gate

This file is complete only when the implementation:
1. compiles as Rust 2024;
2. uses typed IDs, prices, probabilities, quantities, timestamps, and resolver states;
3. writes replayable events with raw payload hashes;
4. has fixture tests and deterministic replay;
5. blocks live execution when source, resolver, venue, or risk state is invalid.


## Winner-Follow trader-intelligence sources

### Polymarket public trader sources

- Leaderboard API.
- User trades API.
- User activity API.
- Current/closed positions APIs.
- Market/orderbook websocket for followed markets.
- Public transaction hashes for timing verification.

### Kalshi trader/flow sources

- Public trades REST and websocket for market-flow only.
- Historical trades for aggregate market reconstruction.
- Leaderboard/help-center information for research, not direct trade attribution.
- Authorized account/portfolio imports for consenting traders.

### Data ethics

Use public APIs, official exports, and user-authorized data. Do not scrape private pages, bypass access controls, impersonate users, or join data in a way that violates venue terms or privacy commitments.
