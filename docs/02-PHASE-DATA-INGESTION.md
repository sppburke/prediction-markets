# 02 — Phase Data Ingestion

> See [`_BASELINE.md`](_BASELINE.md) for the Rust-only implementation rule, toolchain pin, and common acceptance gate.
> See [`_GLOSSARY.md`](_GLOSSARY.md) for venue rate-limit defaults and source-freshness defaults.

## Objective

Build a Rust source bus that captures resolver-relevant information faster than normal retail workflows while preserving provenance, parser versions, raw hashes, and replayability.

## Source hierarchy

### Tier 0 — direct resolver or named official source

Examples: NWS climate report, exact Wunderground station page named in rules, Chainlink stream named by Polymarket rules, Kalshi benchmark source, official league result page, BLS/BEA/Census/Fed release, Spotify/Netflix/Apple official ranking page, USGS/NHC/NASA official event feed.

### Tier 1 — upstream precursor

Examples: exchange L2/trades ahead of oracle/benchmark prints, METAR/station observations ahead of final weather reports, official release directory/file watchers, licensed live sports data, page hash/CDN change detectors.

### Tier 2 — orthogonal confirmation

Examples: independent station networks, multiple exchange feeds, archived official snapshots, alternate official mirrors, multiple sports data vendors.

## SourceConnector trait

```rust
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync + 'static {
    type Config;
    type Raw;
    type Normalized;
    type Error: std::error::Error + Send + Sync + 'static;

    fn source_id(&self) -> SourceId;
    fn parser_version(&self) -> semver::Version;
    async fn run(&self, ctx: SourceContext<Self::Normalized>) -> Result<(), Self::Error>;
    fn normalize(&self, raw: Self::Raw) -> Result<Self::Normalized, Self::Error>;
}
```

`normalize` must be pure and fixture-tested. The `run` loop handles network I/O, timeout, retry, backoff, and source health.

## Timing discipline

Every event stores:

- source timestamp, if available;
- received wall-clock timestamp;
- monotonic ingest timestamp;
- first-seen timestamp for publication pages;
- last-modified/ETag/header metadata when available;
- parser version and raw hash.

For serious deployment, monitor host clock offset with chrony/PTP. Do not compare monotonic clocks across hosts.

## Backpressure

Only bounded channels are allowed. Each connector declares a policy:

| Policy | Behavior | Example |
|---|---|---|
| `LosslessCritical` | Block and alert | Chainlink resolver-shadow, NWS final |
| `LatestWinsState` | Drop older non-critical updates and count drops | Polymarket book WS |
| `SampledTelemetry` | Downsample explicitly | Source health pings |
| `CircuitBreak` | Disable strategies on backlog | Polygon RPC under reorg |

## Rate limits

Per-venue and per-source budgets, retry/backoff, and 429/5xx handling are first-class events. Authoritative defaults live in `_GLOSSARY.md` ("Venue rate limits"); any change requires updating that table and a research pass per `21-RESEARCH-AND-SOURCE-DISCOVERY.md`. Connectors must:

1. enforce the production-budget column from `_GLOSSARY.md` as a token-bucket;
2. emit a `RateLimitObserved` event on every 429/5xx, with retry-after headers normalized;
3. halve the local budget on a sustained breach (≥ 3 events in 60 s) and recover linearly over 5 minutes;
4. never silently swallow a 429 in retry logic.

## Connector families

### `source-weather`

- NWS climate report connector
- NWS observations connector
- Aviation Weather/METAR connector
- Wunderground-compatible station history finalizer where market rules use it
- HRRR/NBM model connector where permitted and useful

### `source-crypto`

- exchange trades and L2 books
- Chainlink stream shadow
- benchmark-window accumulator for Kalshi-style crypto contracts
- Pyth/reference feed connector when permitted
- venue RTDS/price feeds where useful

### `source-onchain-polygon`

- Polygon JSON-RPC/archive-node ingestion
- Polymarket proxy wallet, pUSD, USDC/USDC.e, deposit/onramp, and collateral-flow event logs where publicly derivable
- wallet funding-path events for operator identity research
- maintained exchange/bridge/hot-wallet boundary labels with config hashes
- block-lag and reorg-aware health state, using `onchain_block_lag_warn` / `onchain_block_lag_block` from `_GLOSSARY.md`

### `source-sports`

- official league result/status connectors
- licensed live data connectors if terms allow trading use
- venue sports state connectors
- finite-state game model

### `source-macro`

- BLS/BEA/Census/Fed/Treasury release watchers
- official file/table parsers
- revision detector
- release calendar scheduler

### `source-charts`

- Spotify/Netflix/Apple rank snapshot parsers
- page hash watcher
- title/app/entity normalization
- archive/replay snapshots

### `source-events`

- USGS, NHC, NASA FIRMS, court/regulatory/public agency feeds
- geospatial filter
- preliminary vs final revision handling

## Hot parsing

Start with correctness using `serde_json`, `csv`, `quick-xml`, or `scraper`. Move hot JSON feeds to `simd-json` only after benchmark evidence. Unknown fields on resolver-critical sources must be logged and schema-drift counted.

## Source health

```rust
pub struct SourceHealth {
    pub source_id: SourceId,
    pub status: HealthStatus,
    pub last_success_at: OffsetDateTime,
    pub p50_latency_ms: u32,
    pub p99_latency_ms: u32,
    pub stale_for_ms: u64,
    pub schema_drift_count: u64,
    pub parse_error_count: u64,
}
```

Stale and block thresholds come from `_GLOSSARY.md` ("Source freshness defaults"). Strategies may only trade if required sources are healthy, or if a pre-approved degraded-source mode exists.

## Winner-Follow ingestion priority

Before specialized weather/crypto/sports/macro source gateways, build the trader-intelligence ingestion path. The crate is `source-trader` (not `source-trader-polymarket`/`source-trader-kalshi`); venue-specific behavior lives in submodules `source_trader::polymarket` and `source_trader::kalshi`.

### `source-trader::polymarket`

- leaderboard snapshots by category, offset, and time window;
- public user trades by wallet/profile address;
- public current positions and closed positions;
- public user activity;
- market metadata for every copied trade;
- orderbook snapshots and price history for copy-cost reconstruction;
- websocket market trade/orderbook events for markets currently held or newly traded by watched leaders;
- optional on-chain transaction hash enrichment when needed for timing validation.

### `source-onchain-polygon` for Winner-Follow

Build the native funding/collateral graph instead of depending on CrowdIntel output or UI scraping.

Responsibilities:

- ingest normalized Polygon events such as `CollateralTransfer`, `UsdcTransfer`, `PusdMintOrWrap`, `ProxyWalletCreated`, `DepositAddressCreated`, and `BridgeDepositObserved` where supported by official/public contract evidence;
- record transaction hash, block number, log index, contract address, event signature, parser version, raw payload hash, observed timestamp, and received timestamp;
- maintain a first-funding index for each observed Polymarket wallet/proxy using the first public inbound collateral event, the first non-exchange direct funder where available, and an N-hop funding path (`funding_max_hops` in `_GLOSSARY.md`) with stop rules at exchanges, bridges, mixers, or unknown high-risk contracts;
- preserve both strict single-root clusters and optional secondary transitive clusters; only strict clusters are eligible for sizing in v1;
- keep a versioned CEX/bridge/hot-wallet boundary list in config so changes are replayable by config hash;
- expose source health with RPC block lag, reorg depth, parse-error count, schema drift, and label-version state.

Polymarket proxy-wallet reality is a research gate. The implementation must prove, with replay fixtures, how public `proxyWallet`/funder/collateral/deposit evidence maps to the economic actor for representative historical accounts (per `21-RESEARCH-AND-SOURCE-DISCOVERY.md`). If this mapping is incomplete, inherited-prior and cluster-coordination signals remain shadow-only.

### `source-trader::kalshi`

- public trade stream and historical trades for market-flow analysis;
- leaderboard pages or official endpoints only where public/allowed;
- no trader-level copy attribution unless an official public mapping or explicit trader consent exists;
- support authorized portfolio import for the user's own accounts or consenting signal providers.

### Polling and streaming discipline

- Polymarket leader watchlist trade polling uses adaptive intervals: sub-second only for top active leaders and markets that have just printed; slower intervals for dormant leaders. Concrete defaults: 500 ms for top-10 active leaders with markets traded in the last 60 s; 5 s for top-50; 30 s for tail watchlist.
- Every poll result is diffed against the last event hash; duplicate trade events are ignored by the deterministic idempotency key in `_GLOSSARY.md` ("Idempotency").
- WebSocket streams are used for market-state latency, but public trader identification is reconstructed from Data API/profile endpoints and public transaction metadata when needed.
- All scanner loops respect the budgets in `_GLOSSARY.md` ("Venue rate limits"); rate-limit handling is a first-class event, not an exception swallowed by retry logic.
- If `source-onchain-polygon` is unhealthy or lags beyond `onchain_block_lag_block`, degrade gracefully: block fresh-wallet first-trade and cluster-coordination signals, but do not block ordinary leaderboard-based Winner-Follow when its required Polymarket sources are healthy.
