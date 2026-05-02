# 12 — Source Catalog

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for source-freshness defaults.

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
    pub max_stale: Duration,                           // see _GLOSSARY.md "Source freshness defaults"
    pub parser_version: semver::Version,
    pub replay_fixture_path: String,
}
```

## Per-source SLA defaults

These are starting defaults for `max_stale`. They are overridden by the `_GLOSSARY.md` defaults where applicable; venue WebSocket / on-chain values come from `_GLOSSARY.md`.

| Source class | `max_stale` | Block threshold |
|---|---:|---:|
| NWS final climate report | publication cadence × 2 | publication cadence × 4 |
| NWS observations / METAR | 90 min | 4 h |
| Chainlink stream | 30 s | 2 min |
| Exchange WS trades/books | 2 s | 6 s |
| Polymarket Data API poll | 1.5× polling interval | 4× polling interval |
| `source-onchain-polygon` | `onchain_block_lag_warn` blocks | `onchain_block_lag_block` blocks |
| BLS/BEA/Census release watcher | 5 min after expected | 30 min after expected |
| Spotify/Netflix/Apple chart page | publication cadence × 2 | publication cadence × 4 |
| USGS/NHC event feed | 10 min | 60 min |

## Access policy

Edge must come from legitimate faster engineering, not access abuse. Do not bypass controls, violate license terms, use non-public material, overwhelm public systems, or evade rate limits (production budgets in `_GLOSSARY.md`).

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

## Winner-Follow trader-intelligence sources

### Polymarket public trader sources

- Leaderboard API.
- User trades API.
- User activity API.
- Current/closed positions APIs.
- Market/orderbook websocket for followed markets.
- Public transaction hashes for timing verification.

### Polygon public chain sources

- Polymarket proxy-wallet, pUSD, USDC/USDC.e, deposit/onramp, and collateral-flow event logs where publicly derivable.
- Wallet funding path and funder-root events for operator identity (`funding_max_hops` in `_GLOSSARY.md`).
- Publicly versioned exchange/bridge/hot-wallet boundary labels.
- Polygon JSON-RPC/archive node or equivalent licensed public-chain provider.

These sources feed `source-onchain-polygon` and `operator-graph`. They are not venue execution APIs and must remain replayable from raw logs plus parser/config versions.

### Kalshi trader/flow sources

- Public trades REST and websocket for market-flow only.
- Historical trades for aggregate market reconstruction.
- Leaderboard/help-center information for research, not direct trade attribution.
- Authorized account/portfolio imports for consenting traders.

### Data ethics

Use public APIs, official exports, and user-authorized data. Do not scrape private pages, bypass access controls, impersonate users, or join data in a way that violates venue terms or privacy commitments.

CrowdIntel and similar products may be used as research references for methodology and manual validation, but their UI-only data, opaque scores, and proprietary cluster labels are not production decision inputs unless an authorized, stable, replayable export/API is available and reviewed.
