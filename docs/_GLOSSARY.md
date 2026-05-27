# Glossary — vocabulary, types, acronyms, defaults

Single source of truth for terms used across `docs/`. When a definition changes, update it here and let other files reference back.

## Vocabulary: wallet vs trader vs operator vs leader vs candidate

| Term | Definition |
|---|---|
| **Wallet** | A single Polygon address (or venue equivalent). Observable, public, but not necessarily a distinct economic actor. |
| **Trader** | A venue-account-level identity. On Polymarket, currently 1:1 with a public proxy wallet. On Kalshi, a venue user (anonymous in public trade messages). |
| **Operator** | A clustered economic actor, typically composed of one or more wallets/proxies, derived deterministically by `operator-graph` from public funding/collateral evidence. Identified by `OperatorId`. |
| **Funder** | A wallet that supplied initial collateral to one or more proxy wallets. The earliest non-exchange direct funder along the funding path becomes the candidate `funder_root`. |
| **Cluster** | The set of wallets that share a `funder_root` under the active cluster rule version. |
| **Candidate** | An operator/trader currently being evaluated for inclusion in the watchlist. |
| **Leader** | A candidate that has passed eligibility thresholds and is in the active top-`active_watchlist_size` watchlist. |

When the docs say "trader" and the system has confident operator identity, the operator-level aggregation is preferred for ranking, sizing, and risk caps; the wallet-level ledger remains as a sub-aggregation for replay and audit.

## Phase vs Strategy index

Two orthogonal numbering axes are used:

- **Phase N** (in `17-RUST-IMPLEMENTATION-ROADMAP.md`) — build/sequencing order. Phase 0 = toolchain, Phase 0A = Winner-Follow first milestone, Phase 1+ = remaining work.
- **Strategy N** (everywhere else) — strategy index. Strategy 0 = Winner-Follow. Strategy 1+ = resolver/source arbitrage, cross-venue, microstructure.

A "Phase 0A" engineer is *building* Winner-Follow infrastructure. A "Strategy 0" trader is *running* Winner-Follow. They are not the same number.

## Production latency budget

End-to-end target from `leader_trade_observed_at` (gateway receive) to `follower_order_submitted_at` (venue ack received):

| Percentile | Target |
|---:|---:|
| p50 | ≤ 800 ms |
| p95 | ≤ 2.0 s |
| p99 | ≤ 3.0 s |

If running p95 over the prior hour exceeds budget by 50 % for two consecutive 5-minute windows, the **copy-latency kill switch** (see `19-WINNER-FOLLOW-STRATEGY.md`) blocks new entries until p95 returns under budget.

Per-stage budgets are illustrative and refined by `latency-attribution-profiler`:

| Stage | p95 budget |
|---:|---:|
| Source poll/socket → normalized event | 250 ms |
| Normalized event → leader signal classification | 150 ms |
| Classification → fair-value/Kelly | 80 ms |
| Sizing → risk gates | 30 ms |
| Risk pass → venue submission | 200 ms |
| Venue submission → ack | 250 ms |
| Slack/jitter buffer | 1 040 ms |

## Venue rate limits

Production budgets are conservative; they reduce automatically on 429/5xx. The "documented limit" column is filled by the protocol in `21-RESEARCH-AND-SOURCE-DISCOVERY.md`; values below are operator defaults until a research pass updates them.

| Venue / surface | Documented limit | Production budget | Burst |
|---|---|---|---|
| Polymarket Data API | 200 req/10s on `/activity`, 100 req/s general (Cloudflare-queued, no 429) | ≤ 20 req/s sustained on `/activity` | n/a — bursts queued, not rejected |
| Polymarket Gamma API | verify | ≤ 2 req/s sustained | 10-req burst |
| Polymarket CLOB REST | verify | ≤ 5 req/s sustained | 10-req burst |
| Polymarket WebSocket | per-account socket cap | ≤ 4 concurrent sockets | n/a |
| Kalshi REST | verify | ≤ 10 req/s sustained | 30-req burst |
| Kalshi WebSocket | per-account socket cap | ≤ 4 concurrent sockets | n/a |
| Polygon archive RPC | provider-specific | ≤ 25 req/s sustained per worker | provider |

## Acronyms

| Acronym | Expansion |
|---|---|
| AMM | Automated Market Maker |
| AWC | Aviation Weather Center |
| BEA | Bureau of Economic Analysis |
| BLS | Bureau of Labor Statistics |
| bps | basis points (1 bp = 0.01 %) |
| CDK | AWS Cloud Development Kit |
| CEX | Centralized exchange |
| CLOB | Central Limit Order Book |
| ECR | AWS Elastic Container Registry |
| ECS | AWS Elastic Container Service |
| EDGAR | SEC Electronic Data Gathering, Analysis, and Retrieval |
| EKS | AWS Elastic Kubernetes Service |
| EOA | Externally Owned Account (Ethereum/Polygon) |
| FIRMS | NASA Fire Information for Resource Management System |
| HRRR | High-Resolution Rapid Refresh (NOAA model) |
| KMS | AWS Key Management Service |
| L1/L2 (Polymarket auth) | Layer 1 (EOA signature) / Layer 2 (CLOB API key) authentication |
| LCB_5pct | Lower Confidence Bound at 5th percentile |
| METAR | Meteorological Aerodrome Report |
| NBM | National Blend of Models (NOAA) |
| NHC | National Hurricane Center |
| NWS | National Weather Service |
| OIDC | OpenID Connect |
| PTP | Precision Time Protocol |
| ppm | parts per million (1 ppm = 0.0001 %) |
| pUSD | Polymarket USDC representation; collateral token used on Polygon for Polymarket markets |
| RDS | AWS Relational Database Service |
| RTDS | Real-Time Data Stream (Polymarket) |
| SEV | Severity (incident grading) |
| SLA | Service Level Agreement / target |
| TTL | Time To Live |
| USDC.e | bridged USDC on Polygon (legacy, multichain) |
| USGS | United States Geological Survey |

## Type aliases (illustrative; canonical in `core-types`)

These types are referenced across the docs but defined once in `crates/core-types`. Bodies are illustrative.

```rust
// Identifiers
pub struct VenueId(&'static str);
pub struct VenueMarketId(pub String);
pub struct MarketId(pub VenueMarketId);
pub struct OutcomeId(pub u8);                 // 0 = Yes / first outcome, 1 = No / second outcome, ...
pub struct MarketOutcomeId(pub MarketId, pub OutcomeId);
pub struct ResolverCardId(pub uuid::Uuid);
pub struct SourceId(pub String);
pub struct StrategyId(pub String);
pub struct ModelId(pub String);
pub struct OrderLocalId(pub uuid::Uuid);
pub struct SourceTradeId(pub String);
pub struct EventSeq(pub u64);

// Identities
pub struct WalletAddress(pub [u8; 20]);
pub struct TraderId(pub WalletAddress);       // Polymarket; Kalshi traders use VenueAccountId
pub struct VenueAccountId(pub String);
pub struct OperatorId(pub blake3::Hash);
pub struct FunderRootId(pub WalletAddress);

// Quantities, prices, probabilities
pub struct ContractQty(pub u64);
pub struct Quantity(pub ContractQty);
pub struct KalshiPriceCents(pub u8);          // 0..=100
pub struct PolymarketPriceDecimal(pub rust_decimal::Decimal); // 0.0..=1.0, 4dp
pub struct Price(pub rust_decimal::Decimal);  // 0.0..=1.0
pub struct Probability(pub rust_decimal::Decimal); // 0.0..=1.0; model/strategy estimate, distinct from market price
pub struct PriceDelta(pub rust_decimal::Decimal);
pub struct ProbabilityPpm(pub u32);           // 0..=1_000_000
pub struct BasisPoints(pub i32);
pub struct KellyFraction(pub rust_decimal::Decimal); // 0.0..=1.0

// Time
pub struct SourceTimestamp(pub time::OffsetDateTime);
pub struct ReceivedAt(pub time::OffsetDateTime);
pub struct ObservedAtBucket(pub i64);         // floor(observed_at_ms / BUCKET_MS); BUCKET_MS = 1_000

// Operator-graph
pub struct FundingHopCount(pub u8);
pub struct WalletAgeSeconds(pub u32);
pub struct ClusterSize(pub u16);
pub struct ReconstructionQuality(pub u8);     // 0..=100

// Sides
pub enum Side { Buy, Sell }                   // venue adapters map Yes/No → Side per outcome
```

## Resolver-card sub-types

Defined in `resolver-card`. Each variant has explicit semantics.

```rust
pub enum MarketFamily {
    Weather,
    CryptoBenchmarkWindow,
    CryptoSpotPath,
    SportsOfficial,
    MacroRelease,
    ChartRanking,
    Documents,
    EventFeed,
    PoliticsElections,
    Other,
}

pub enum ResolverSource {
    NamedOfficialPage(String),                // resolver URL exact
    OfficialApi(String),                      // resolver API endpoint
    ChainlinkStream { feed_id: String },
    KalshiBenchmark { source: String, window: WindowSpec },
    StationHistory { station_id: String, page: String },
    LeagueOfficial { league: String, game_id: String },
    Custom { description: String },
}

pub enum OutputSpace {
    Binary,                                   // YES/NO
    Categorical { n: u8 },
    NumericRange { min: rust_decimal::Decimal, max: rust_decimal::Decimal, ticks: u32 },
    Threshold { value: rust_decimal::Decimal, direction: Direction },
}

pub enum TimingRule {
    PointInTime(SourceTimestamp),
    Window(WindowSpec),
    BusinessDayClose { tz: String },
    OnFirstPublicationAfter(SourceTimestamp),
    OnNthOccurrence { n: u32 },
}

pub struct WindowSpec {
    pub start: SourceTimestamp,
    pub end: SourceTimestamp,
    pub tz: String,
    pub sample_policy: SamplePolicy,
}

pub enum SamplePolicy { ArithmeticMean, TwapByMillisecond, MedianOfSamples, FirstObserved, LastObserved }

pub enum RoundingRule {
    None,
    HalfEven { dp: u8 },
    DownToTick { tick: rust_decimal::Decimal },
    AsPublished,
}

pub enum TieRule {
    NoTradePastEqual,
    YesOnEqual,
    NoOnEqual,
    SourceDefined,
}

pub enum FinalityRule {
    AsPublished,
    AfterStablePeriod { hours: u32 },
    AfterRevisionsCleared,
    AfterOfficialMark,
}

pub enum RevisionPolicy {
    NoRevisions,
    AcceptUntilFinal,
    AcceptOnlyOfficialCorrections,
    HumanReviewRequired,
}

pub enum Direction { GreaterEqual, Greater, LessEqual, Less, Equal }
```

## Example resolver card

Fully populated, illustrative.

```rust
let card = ResolverCard {
    venue: Venue::Kalshi,
    market_id: VenueMarketId("KXBTCD-26MAY02-100000".to_string()),
    family: MarketFamily::CryptoBenchmarkWindow,
    resolver_source: ResolverSource::KalshiBenchmark {
        source: "Coinbase BTC-USD trades".to_string(),
        window: WindowSpec {
            start: parse_iso("2026-05-02T11:55:00Z"),
            end:   parse_iso("2026-05-02T12:00:00Z"),
            tz: "UTC".to_string(),
            sample_policy: SamplePolicy::ArithmeticMean,
        },
    },
    upstream_sources: vec![
        ResolverSource::Custom { description: "Coinbase Pro WS trades stream".into() },
        ResolverSource::Custom { description: "Pyth BTC/USD reference feed (shadow)".into() },
    ],
    output_space: OutputSpace::Threshold {
        value: dec!(100_000),
        direction: Direction::GreaterEqual,
    },
    timing: TimingRule::Window(WindowSpec {
        start: parse_iso("2026-05-02T11:55:00Z"),
        end:   parse_iso("2026-05-02T12:00:00Z"),
        tz: "UTC".to_string(),
        sample_policy: SamplePolicy::ArithmeticMean,
    }),
    rounding: RoundingRule::HalfEven { dp: 2 },
    tie_rule: TieRule::YesOnEqual,
    finality: FinalityRule::AsPublished,
    revision_policy: RevisionPolicy::NoRevisions,
};
```

## Configuration defaults — concrete values

Where the docs use vague qualifiers, these are the canonical defaults. They live in code as `WinnerFollowConfig` and `OperatorGraphConfig` and are restated here for cross-reference.

### Polymarket public source (`PollingConfig`)

| Key | Default | Meaning |
|---|---:|---|
| `polymarket_base_url` | `https://data-api.polymarket.com` | Base URL for all five public REST endpoints |
| `polymarket_request_timeout_secs` | 10 | Per-request HTTP timeout before the request is abandoned |
| `polymarket_max_retries` | 3 | Retries on network errors and 5xx (4 total attempts: initial + 3 retries) |
| `polymarket_channel_capacity` | 256 | Bounded mpsc channel capacity between trade poller and orchestrator |
| `trade_poll_interval_secs` | 30 | Seconds between Polymarket trade poll rounds (one round = all watchlisted wallets) |
| `polymarket_clob_base_url` | `https://clob.polymarket.com` | Polymarket CLOB REST API base URL for order submission and status polling |
| `polymarket_clob_min_interval_ms` | 200 | Minimum interval between CLOB requests (5 req/s sustained limit per rate-limit table above) |
| `polymarket_clob_poll_interval_ms` | 100 | Interval between GET /order/{id} polls while waiting for terminal status |

### Polygon on-chain source (`PolygonConnectorConfig`)

| Key | Default | Meaning |
|---|---:|---|
| `polygon_http_url` | `""` | Alchemy (or compatible) HTTPS endpoint for `eth_getLogs` backfill. **Required for `funder_source = "eth_logs"`; ignored for `"etherscan"`.** |
| `polygon_ws_url` | `""` | Alchemy (or compatible) WSS endpoint for live `eth_subscribe` logs. Required for `funder_source = "eth_logs"`. When empty and `funder_source = "etherscan"`, the live WS subscription is skipped; only funder discovery runs. |
| `polygon_checkpoint_path` | `"./polygon_checkpoint.json"` | Path to the JSON block-checkpoint file used to resume backfill across restarts. |
| `polygon_backfill_blocks` | 21_000_000 | Blocks to backfill from current head on first run (≈ 16 months at ~2 s/block). **eth_logs only.** |
| `polygon_backfill_page_size` | 10 | Max blocks per `eth_getLogs` page during discovery/backfill. Alchemy free tier hard-caps this at 10; paid/dedicated tiers allow ~2_000+. **eth_logs only.** |
| `polygon_channel_capacity` | 256 | Bounded mpsc channel capacity between backfill/WS workers and `next_event` consumer |
| `funder_source` | `"eth_logs"` | Funder discovery backend: `"eth_logs"` (default, Alchemy CU) or `"etherscan"` (Etherscan V2 free tier). With `"etherscan"` + empty Polygon URLs, funder discovery runs via Etherscan and no live WS subscription is started. |
| `etherscan_funder_rps` | 3 | Etherscan V2 free-tier rate limit: 3 requests per second. Verified via in-band `Max calls per sec rate limit reached (3/sec)` responses; the historical doc value of 5 was optimistic. |
| `etherscan_funder_concurrency` | 4 | Concurrent per-wallet funder-discovery tasks (`PE_BOOTSTRAP_FUNDER_CONCURRENCY`). Rate is enforced by the token-bucket limiter (`etherscan_funder_rps`), not by concurrency, so both can be tuned independently. N=4 covers HTTP RTT variance and 429 backoff windows at the 3 req/s free-tier ceiling. |
| `etherscan_funder_max_pages` | 100 | Safety cap on paginated cursor iterations per (wallet, contract) pair. Stops the page loop after 100 × 10,000 = 1,000,000 transfers; emits `warn!` if hit so hub-like wallets are visible in logs. Cursor pages are sequential (each query depends on the previous), so worst-case wall time per capped contract is ~33s at 3 req/s regardless of concurrency. Wallets exceeding 1M transfers (rare) get partial coverage. |
| `etherscan_funder_max_backoff_secs` | 60 | Cap on retry backoff for transient Etherscan errors. |
| `etherscan_funder_max_attempts` | 6 | Maximum retry attempts before failing the discovery hop. |
| `etherscan_funder_http_timeout_secs` | 30 | Per-request HTTP timeout for Etherscan calls. |

### Wallet enumeration defaults

Issue #186: wallet enumeration migrated off Etherscan REST (100k requests/day
free-tier cap) onto an alloy [`Provider`] against an Alchemy-compatible RPC.
Bisect-on-cap + HTTP 429 retry now live in
`pe_source_onchain_polygon::eth_logs::eth_get_logs_bisect`; `wallet_enumeration`
only orchestrates the `(contract, topic0, chunk)` loop and operator filtering.

| Key | Default | Meaning |
|---|---:|---|
| `wallet_enum_scan_chunk_blocks` | 50_000 | Top-level `eth_getLogs` chunk size. Also the **persistence granularity** for `pe-bootstrap`: the bootstrap upserts after each chunk so a crash loses at most one chunk's worth of work. Tuned down from 500_000 → 50_000 on 2026-05-17 after observing V2-topic dense regions drive `eth_get_logs_bisect` to ~9 recursion levels and ~700 MB/5min RSS growth; 50k caps the bisect tree at ~6 levels and bounds peak memory to sub-GB per chunk. Exported as `pe_source_onchain_polygon::wallet_enumeration::SCAN_CHUNK_BLOCKS`. |
| `wallet_enum_decode_retry` | (classifier in `eth_get_logs_bisect`) | Transient deserialization failures from alloy (`error decoding response body`, truncated streams, gateway 5xx served as HTML, EOF mid-parse) are treated as transient and retried with the same exponential backoff as rate-limit errors (1→2→4→…→32s, max 6 attempts). Observed in production 2026-05-17 during the Polymarket V2 dense-region sweep — without this widening a single Alchemy partial response fails the entire bootstrap and costs a ~12-min `refresh_trade_counts` replay on wrapper restart. |
| `wallet_enum_transport_retry` | (classifier in `eth_get_logs_bisect`) | Transport-level transient failures (`connection reset`, `broken pipe`, `connection closed before message completed`, `operation timed out` / `request timeout`, `early eof`) are treated as transient and retried with the same exponential backoff profile (max 6 attempts). Issue #191 Item 1. **Critical**: bare `"timeout"` is EXCLUDED — it collides with Alchemy's `"Query timeout exceeded"` cap-hit error and would prevent the bisect-on-cap branch from firing. The narrower `"timed out"` is used instead (different conjugation; verified against the production Alchemy error). |
| `wallet_enum_backfill_v1_attribution` | `pe-bootstrap --backfill-v1-attribution [config.toml] [--dry-run]` | One-shot operator subcommand (issue #191 Item 2) that clears the V1 topic from `enumerated_topic_hashes` and V1-keyed entries from `chunk_progress`, so the next normal `pe-bootstrap` run re-enumerates V1 and populates `polymarket_contracts_seen` bit 0 for legacy-ingested wallets (the 38,790 from `wallet_set.json` migration that were ingested before the column existed). Acquires `lock::CacheMutationLock` to refuse concurrent execution against an in-flight sweep. `--dry-run` previews what would be cleared without modifying. |
| `wallet_cache_mutation_lock` | `<cache_path>.lock` | PID-based RAII lock file (issue #191 Item 2) at `<cache_path>.lock`. Acquired by **both** enumeration arms in `lib.rs::run()` (OnChain since #192, Dune since #193 for symmetric mutex semantics) and by the `--backfill-v1-attribution` subcommand to serialize cursor-state mutations. Stale-PID reclaim handles the case where a previous holder crashed without dropping the lock. Module: `pe_bootstrap::lock`. |
| `wallet_enum_from_block` | 33_605_403 | Earliest block to scan — approximate CTFExchange V1 deployment on Polygon. |
| `wallet_enum_min_chunk` | 1 | `AlloyChainLogFetcher.min_chunk` for the bootstrap-owned enumerator. Floor at which `eth_get_logs_bisect` stops halving and propagates the underlying RPC error instead. **Behavioural shift from the legacy Etherscan path** (issue #186): the old path accepted-with-warn on a single-block cap hit and continued the sweep; the alloy path hard-fails the chunk and aborts the bootstrap. On a paid Alchemy tier this is the correct posture (errors are real, not cap-hits), but operators should know the sweep no longer absorbs single-block anomalies silently. |
| `bootstrap_all_order_filled_topics` | `pe_source_onchain_polygon::contracts::ALL_ORDER_FILLED_TOPICS` | Canonical "what to scan" set used by every `OrderFilled` consumer (`polygon_ctf_delta::scan_active_wallets`, `wallet_enumeration::PolymarketTraderEnumeration`). Currently `[V1, V2]`. Hex values live in `contracts.rs` with self-validating keccak tests — the canonical source. Extending this array adds the new topic to every consumer automatically and triggers an additive re-sweep on the next bootstrap run (issue #179). |
| `wallet_enum_contract_version_bit_v1` | 0b01 | `polymarket_contracts_seen` bit set on wallets discovered via `TOPIC_ORDER_FILLED_V1`. Hardcoded as `pe_source_onchain_polygon::contracts::CONTRACT_VERSION_BIT_V1`. |
| `wallet_enum_contract_version_bit_v2` | 0b10 | `polymarket_contracts_seen` bit set on wallets discovered via `TOPIC_ORDER_FILLED_V2`. Hardcoded as `pe_source_onchain_polygon::contracts::CONTRACT_VERSION_BIT_V2`. |
| `wallets.polymarket_contracts_seen` (column) | `i64`, default `0` | OR-merged bitmask of `CONTRACT_VERSION_BIT_V1` / `_V2` for every wallet, populated only on the on-chain enumeration path (Dune-CSV / trade-fetch / Dune-incremental upserts pass `0` and rely on the OR-merge to preserve any prior attribution). Used at go-live to route trades to the correct CTFExchange contract. |
| `wallet_enum_completed_contracts` (cursor) | `"wallet_enum_completed_contracts"` | `source_cursor` table key holding JSON-encoded `Vec<String>` of lowercase-hex contract addresses fully enumerated. Issue #181. Equivalent to the now-deleted `WalletSetState.completed_contracts` field. |
| `wallet_enum_topic_hashes` (cursor) | `"wallet_enum_topic_hashes"` | `source_cursor` table key holding JSON-encoded `Vec<String>` of B256-Display topic hashes (each prefixed `0x`) fully enumerated. Issue #181. Equivalent to the now-deleted `WalletSetState.enumerated_topic_hashes` field. |
| `wallet_enum_chunk_progress` (cursor) | `"wallet_enum_chunk_progress"` | `source_cursor` table key holding a JSON-encoded `HashMap<String, u64>` keyed by `"{topic_hex}\|{contract_hex}"` mapping to the last block successfully scanned for that `(topic, contract)` pair. Mid-topic crash recovery resumes from `last_completed_chunk_to + 1` instead of `wallet_from_block`, saving up to ~168 redundant `eth_getLogs` calls per mid-topic crash on a 21M-block historical sweep. Issue #188. Missing key (pre-#188 deployed cache) is treated as "no chunks done" — backward-compat additive. |

**Enumeration progress migration (issue #181).**
The `wallet_set.json` checkpoint file is consolidated into the SQLite cache on
first post-deploy run via `migrate::auto_migrate_legacy`. The progress fields
(`completed_contracts`, `enumerated_topic_hashes`) move from the JSON file to
two `source_cursor` rows (keys above). Wallet hexes move into the `wallets`
table with `SRC_WALLET_SET_JSON` source bit. Then the JSON file is **deleted**.
Subsequent runs read enumeration progress via `migrate::load_enum_state`,
which returns `(vec![], vec![])` on missing keys (fresh install). The
"legacy V1-done" detection (pre-#179 checkpoint had full
`completed_contracts` + empty `enumerated_topic_hashes`) is preserved across
the migration by `auto_migrate_legacy` synthesizing the V1-topic-done state
when ingesting bare-array files, and by `lib.rs::run()`'s set-membership
check over `ALL_EXCHANGE_CONTRACTS` for pre-#179 checkpoints.

**Trade-fetch scope (issue #181).**
After consolidation, `lib.rs::run()` reads the per-wallet trade-fetch list via
`cache.wallets_with_source_bit(SRC_WALLET_SET_JSON)` — narrowly scoped to
wallets discovered via Etherscan/Dune-SQL/legacy migration. Do NOT use
`cache.all_pile_wallet_hexes()` (the full 2.7M-row pile including Dune CSV
imports) for trade fetch — that path would explode the per-wallet Polymarket
API call count by ~54×.

### Operator graph

| Key | Default | Meaning |
|---|---:|---|
| `funding_max_hops` | 3 | Hops along funding path before traversal stops. Surfaced in `ServiceConfig` so the value participates in the config-hash. |
| `funder_root_min_confidence_ppm` | 850_000 | Minimum identity confidence (= 0.85) to attribute a funder root |
| `cluster_min_size` | 1 | Minimum wallets in cluster |
| `cluster_max_size` | 25 | Cluster sizes above this require manual review |
| `seeding_velocity_warn_per_week` | 5 | New seeded wallets per week before flag |
| `seeding_velocity_block_per_week` | 12 | Hard block threshold |
| `cluster_membership_stability_window_d` | 30 | Window for membership-instability checks |
| `cluster_membership_max_churn_pct` | 30 | % membership change above which cluster is "unstable" |
| `family_concentration_warn_pct` | 60 | Single-`MarketFamily` % beyond which `MarketNarrowness` flag fires |
| `onchain_block_lag_warn` | 8 | Polygon blocks of lag before degraded health |
| `onchain_block_lag_block` | 25 | Polygon blocks of lag before inherited-prior + cluster modes are blocked |
| `reorg_depth_block` | 12 | Reorg depth that invalidates pending events |

### Fresh-wallet inherited-prior incubator

| Key | Default | Meaning |
|---|---:|---|
| `fresh_wallet_max_closed_trades` | 2 | Wallet still counts as "fresh" if closed-trade count ≤ this |
| `fresh_wallet_min_age_seconds` | 0 | Age lower bound (none) |
| `fresh_wallet_max_age_seconds` | 1_209_600 | 14 days; older wallets are not "fresh" |
| `inherited_prior_max_effective_n` | 12 | Cap on effective sample size after shrinkage |
| `inherited_prior_min_position_usd` | 50 | Below this, signal is debounced (noise) |
| `inherited_prior_max_position_usd_paper` | 5_000 | Above this in paper, treat as research alert only |

### Cluster coordination

| Key | Default | Meaning |
|---|---:|---|
| `cluster_coord_min_members_K` | 3 | Minimum coordinating wallets |
| `cluster_coord_window_seconds_W` | 300 | Window during which coordinating entries count |
| `cluster_coord_min_aggregate_usd` | 1_000 | Minimum aggregate notional across members |
| `cluster_coord_dedup_window_seconds` | 600 | Debounce duplicate signals per `(operator,market,outcome,side)` |
| `cluster_observation_window_secs` | 300 | How long `ClusterObservationTracker` retains entries; must be ≥ `cluster_coord_window_seconds_W` |
| `operator_graph_rebuild_cadence_secs` | 60 | How often `OperatorGraphScheduler` calls `build_operator_identities`; controls operator-ID freshness |

### Trade classification

| Key | Default | Meaning |
|---|---:|---|
| `add_high_confidence_threshold_ppm` | 700_000 | "high-confidence Add" classification threshold (= 0.70) |
| `exit_high_confidence_threshold_ppm` | 700_000 | "high-confidence Exit/Trim" |
| `near_close_remaining_pct` | 10 | Position fraction remaining after Trim that flips it to Exit |
| `unknown_classification_blocks` | true | An `Unknown` action is never copied |
| `flip_requires_human_approval` | true | Default-deny flip until `flip_human_approved = true` is set in config |

### Winner-Follow evaluation (`WinnerFollowConfig`)

| Key | Default | Meaning |
|---|---:|---|
| `polymarket_fee_rate` | 0.04 | Polymarket BUY taker fee rate applied in the fee model: `fee_per_share = price × rate` (flat taker fee on notional). Added to `c` to obtain net cost. March 2026 schedule. |
| `slippage_rate` | 0.01 | Expected proportional fill slippage for BUY orders; `slippage_per_share = price × rate`. Added to `c` alongside the taker fee. Backtest fill is `price × (1 + rate)`; SELL fill is `price × (1 − rate)`. Canonical default: 100 bps. |

### Service health (`HealthState`)

| Key | Default | Meaning |
|---|---:|---|
| `source_freshness_window_seconds` | 60 | Seconds without an event before a source is considered stale in `/health/ready` |

### JSONL observability sidecar schema

Written to `jsonl_log_path` (default: `./paper.jsonl`). One JSON object per line.

**Required fields on every line:**

| Field | Type | Description |
|---|---|---|
| `ts` | RFC-3339 UTC string | When the event was emitted |
| `level` | string | `INFO`, `WARN`, `ERROR`, `DEBUG` |
| `message` | string | Human-readable description |

**Per-kind structured fields** (present when `kind` is set via `tracing::info!(kind = "...", ...)`):

| `kind` | Extra fields | Description |
|---|---|---|
| `paper_fill` | `idempotency_key`, `market`, `side`, `contracts`, `fill_price` | A paper-mode simulated fill |
| `polygon_event` | _(implicit in body)_ | Decoded on-chain Polygon event |

### Logging conventions (issue #184)

Every log line in the workspace is JSONL, emitted via the `tracing` crate. Subscribers
are configured to `.json()` in `pe-bootstrap`, `pe-backtest`, and `pe-service` (both
stderr and file layers for service). Operators wanting human-readable console output
pipe through `jq`. No production code path uses `eprintln!`/`println!` for logging.

**Canonical field shapes:**

| Pattern | Use |
|---|---|
| `error = %e` | The canonical field name for any error value displayed via `Display`. Never use positional `%e` (becomes `fields.e` not `fields.error`). |
| `wallet = %wallet_hex` | Wallet entity context (40-char lowercase hex). |
| `market_id = %market_id`, `contract = %contract_hex` | Market/contract entity context. |
| `count = n`, `progress = n`, `total = n` | Numeric counts as structured fields, NEVER embedded in the format-string message. |

**Field-name collisions to avoid:** `tracing`'s JSON formatter uses `fields.message` for the
log message string. Passing `%message` as a field name aliases the message field and most
JSON parsers will take the last value, **silently overwriting the log message text**. Always
rename to `error = %message` or `text = %message` etc. The same applies to `%level`,
`%target`, `%timestamp` — none should appear as field names.

**Anti-pattern** (do not do this — values appear redundantly in message AND fields):
```rust
tracing::info!(progress = n, total = total_pending, "funder discovery {}/{}", n, total_pending);
```

**Correct shape** (values are queryable fields; message is a static label):
```rust
tracing::info!(progress = n, total = total_pending, "funder discovery progress");
```

### Watchlist auto-fetcher (`WatchlistFetchConfig`)

| Key | Default | Meaning |
|---|---:|---|
| `watchlist_size` | 20 | Top-N leaderboard entries fetched by `WatchlistFetcher` |
| `watchlist_lookback_window_days` | 7 | Days of leaderboard history considered when selecting candidates |
| `seed_watchlist_path` | `""` | Path to pe-bootstrap Watchlist JSON. Empty = disabled. Missing file warns and falls back to leaderboard. |

### Watchlist sizes

| Key | Default | Meaning |
|---|---:|---|
| `active_watchlist_size` | 50 | Top-N active leaders/operators |
| `incubator_watchlist_size` | 250 | Candidates under research |

### Ranker eligibility thresholds

Active tier (LCB_5pct > 0 required in addition):

| Key | Default | Meaning |
|---|---:|---|
| `active_window_days` | 180 | Look-back window for active-tier scoring |
| `active_min_closed_trades` | 15 | Minimum closed trades in window. Lowered from 60: N_eff shrinkage now handles statistical rigor. |
| `active_min_distinct_markets` | 1 | Minimum distinct markets traded in window. Lowered from 30: N_eff replaces the hard filter; specialists are priced correctly via shrinkage. |

Incubator tier:

| Key | Default | Meaning |
|---|---:|---|
| `incubator_window_days` | 90 | Look-back window for incubator-tier scoring |
| `incubator_min_closed_trades` | 5 | Minimum closed trades in window. Lowered from 20: N_eff handles quality. |
| `incubator_min_distinct_markets` | 1 | Minimum distinct markets traded in window. Lowered from 10: N_eff replaces the hard filter. |

### Idempotency

`observed_at_bucket = floor(observed_at_ms / 1_000)` — 1-second buckets. The tuple `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` is the unique idempotency key. Two events with the same key are the same trade. Operator-aware idempotency adds `operator_id` for cluster-coordination signals.

### Approval mechanism

Two flags are first-class:

- `flip_human_approved: bool` — must be set in config (and audit-logged) before `LeaderAction::Flip` becomes a copy-eligible action.
- `kelly_fraction_above_default_human_approved: bool` — must be set before any mode's `kelly_fraction` exceeds the table in `19-WINNER-FOLLOW-STRATEGY.md` (cap 0.50 absolute).

Both are read by `risk-engine` as part of its pure inputs; flipping them at runtime requires a signed config change.

### Promotion criteria — quantified

A leader/strategy promotes from one mode to the next only when ALL of:

| Comparison | Threshold |
|---|---|
| Walk-forward LCB_5pct of follower daily log-growth (after costs) | > 0 |
| Paper-vs-backtest two-sample KS p-value on daily PnL | ≥ 0.10 |
| Paper-vs-backtest mean-PnL z-score | abs(z) ≤ 2.0 |
| Paper-mode observation length | ≥ 30 calendar days AND ≥ 90 closed copied trades |
| Realized fill rate vs simulated | within 15 % absolute |
| Observed copy delay p95 | ≤ p95 in production-latency-budget table above |
| Demotion incidents in window | 0 (a demotion resets the clock) |

Live-tiny → promoted requires the same gates over a fresh 30-day window with live-tiny capital.

The `inherited_prior_first_trade` and `cluster_coordination` modes use the same gate template but each maintains its own ladder. Promotion of one mode does not promote another.

### Demotion criteria

A leader/operator is demoted (mode steps down: promoted → live-tiny → paper → off) when ANY of:

- live copied PnL underperforms simulation by ≥ 2 standard errors over a 14-day window;
- p95 copy delay drifts > 1.5× the production budget for two consecutive hourly windows;
- reconstruction quality drops by ≥ 20 points (out of 100);
- profit concentration (max single-market %) increases above the eligibility threshold;
- trader becomes inactive (no trades for ≥ 14 days);
- copied exits become unreliable (≥ 3 missed/late exits in 30 d);
- operator identity confidence falls below `funder_root_min_confidence_ppm`;
- `BaitWalletSuspect`, `DilutionAttack`, `LaunderedFunder`, or `WashCluster` flag fires.

### Anti-gaming flag thresholds

| Flag | Concrete rule |
|---|---|
| `BaitWalletSuspect` | Operator seeded `> seeding_velocity_warn_per_week` wallets in any 7-day window AND one fresh wallet from that batch posts a position ≥ `inherited_prior_max_position_usd_paper` |
| `DilutionAttack` | Cluster size grew by `> cluster_membership_max_churn_pct` in `cluster_membership_stability_window_d` AND new members have aggregate negative realized PnL OR each has < 5 closed trades |
| `LaunderedFunder` | Funder root is < 7 days old AND its first inbound funding is from a CEX/bridge AND it fans out to ≥ 5 wallets within 72 h of funding |
| `WashCluster` | ≥ 60 % of cluster member trades over a 30-day window match counterpart trades from another cluster member within 60 s, OR intra-cluster trade volume / total cluster volume > 0.40 |
| `MarketNarrowness` | A single `MarketFamily` accounts for `> family_concentration_warn_pct` of the operator's audited PnL |

### "Very liquid market" threshold (when market orders are permitted)

A market is "very liquid" only when ALL of:

- top-of-book depth ≥ $50_000 notional within 50 bps of mid;
- 10-minute trailing volume ≥ $20_000 notional;
- spread ≤ 50 bps for ≥ 80 % of the prior 10 minutes.

Otherwise the engine submits limit orders.

### Source freshness defaults

| Source class | Stale threshold | Block threshold |
|---|---:|---:|
| Polymarket market WS | 2 s | 6 s |
| Kalshi market WS | 2 s | 6 s |
| Polymarket Data API (poll) | 1.5× polling interval | 4× polling interval |
| `source-onchain-polygon` | `onchain_block_lag_warn` blocks | `onchain_block_lag_block` blocks |
| Resolver source (NWS final, BLS release, etc.) | per-source SLA in `12-SOURCE-CATALOG.md` | per-source |

### Backtest-vs-paper "close behavior" definition

"Behavior is close to simulation" means BOTH:

- KS p-value ≥ 0.10 on daily-PnL distributions;
- abs(z) ≤ 2.0 on mean-PnL difference, where the standard error is computed by stationary bootstrap with 1_000 resamples.

This applies anywhere the docs say "matches", "close to", or "drift acceptable".

### Bootstrap defaults (`pe-bootstrap`)

| Key | Default | Meaning |
|---|---:|---|
| `bootstrap_min_closed_trades` | 15 | Minimum closed trades for a wallet to pass the bootstrap post-filter (exclusive: must have > 15) |
| `bootstrap_min_win_rate_pct` | 95 | Minimum win-rate (percent, integer) to pass the bootstrap post-filter (exclusive: must exceed 95%) |
| `bootstrap_post_filter_active_window_days` | 30 | Recency window: wallet must have at least one trade opened within this many days of snapshot time |
| `bootstrap_post_filter_max_avg_hours_to_resolution` | 72 | Maximum average hours from first entry on a market to that market's resolution; filters out late entrants (exclusive: < 72 h) |
| `bootstrap_dune_min_closed_markets` | 15 | Minimum distinct resolved binary markets a wallet must have traded (exclusive: > 15) for Dune discovery |
| `bootstrap_dune_min_win_rate_pct` | 95 | Minimum win-rate (percent, integer) on those markets for Dune discovery (exclusive: > 95%) |
| `bootstrap_dune_active_window_days` | 30 | Wallet must have at least one trade on a resolved market within this many calendar days for Dune discovery |
| `bootstrap_dune_max_avg_hours_to_resolution` | 72 | Maximum average hours from a wallet's first entry on a condition to that condition's resolution; filters out late entrants (exclusive: < 72 h) |
| `bootstrap_polymarket_audit_window_days` | `None` (unlimited) | Trade lookback window. `None` means all available history; set `PE_BOOTSTRAP_AUDIT_WINDOW_DAYS` to an integer or `unlimited`/empty for no limit. Passed as `u32::MAX` to `TradeSnapshot` internally. |
| `bootstrap_incremental_fetch_known_id_threshold` | 3 | Number of consecutive already-cached `source_trade_id`s that signals incremental fetch is complete for a wallet |
| `bootstrap_dune_poll_interval_secs` | 3 | Seconds between Dune execution result polling attempts |
| `bootstrap_dune_max_wait_secs` | 300 | Maximum seconds to wait for a Dune query to complete before aborting |
| `bootstrap_trade_fetch_limit` | 500 | Trades per page when fetching wallet history from the Polymarket `/activity?type=TRADE` endpoint |
| `bootstrap_polymarket_pagination` | `"end-cursor"` | Pagination strategy for per-wallet trade fetch. Uses `end=<ts>` and `start=<ts>` cursor parameters on `/activity?type=TRADE`; no hard cap on history depth. |
| `bootstrap_polymarket_min_retry_after_secs` | 1 | Minimum sleep duration (seconds) when the Polymarket Data API returns HTTP 429. Floors the `Retry-After` header value so a zero or absent header does not cause a tight retry loop. |
| `bootstrap_polymarket_concurrency` | 16 | Concurrent per-wallet trade fetches against the Polymarket Data API; the `ReqwestFetcher` rate-limit gate caps aggregate throughput at ≤ 20 req/s regardless. Set via `PE_BOOTSTRAP_POLYMARKET_CONCURRENCY`. |
| `bootstrap_polymarket_wallet_timeout_secs` | 300 | Per-wallet wall-clock budget (seconds) for `pe-bootstrap backfill` / `run`'s Polymarket fetch loop (issue #173). `0` disables the timeout; positive values wrap each `fetch_wallet_incremental` call in `tokio::time::timeout`. Wallets that trip the budget are soft-failed (added to `FetchOutcome::failed`); the post-fetch pipeline still runs and `last_polymarket_fetch_at` remains NULL so the next backfill re-queues them. Set via `PE_BOOTSTRAP_POLYMARKET_WALLET_TIMEOUT_SECS`. |
| `bootstrap_wallet_cache_path` | `"wallet_cache.db"` | SQLite trade cache. WAL mode provides per-commit durability — at most one in-flight wallet's transaction is lost on crash. Set via `PE_BOOTSTRAP_CACHE_PATH`. |
| `bootstrap_wallet_source` | `"etherscan"` | Wallet discovery backend (`"etherscan"` or `"dune"`); set via `PE_WALLET_SOURCE` |
| `bootstrap_wallet_from_block` | `CTF_EXCHANGE_V1_DEPLOY_BLOCK` (33_605_403) | Start block for Etherscan wallet scan; set via `PE_WALLET_FROM_BLOCK` |
| `bootstrap_wallet_to_block` | current chain head | End block for Etherscan wallet scan; set via `PE_WALLET_TO_BLOCK` (fetched from Etherscan if absent) |
| `bootstrap_wallet_set_path` | `"wallet_set.json"` | Path to the enumerated wallet address list. If the file exists, Etherscan/Dune enumeration is skipped entirely. Delete the file to force a fresh scan; set via `PE_BOOTSTRAP_WALLET_SET_PATH` |
| `bootstrap_seed_as_of_dates` | unset | Comma-separated `YYYY-MM-DD` UTC dates. When set, `pe-bootstrap` switches to historical-seed mode: runs the parameterized Dune query for each date and inserts the results into `leaderboard_snapshots`. Idempotent on `(snapshot_at_unix, wallet_hex)`. Mutually exclusive with the regular pipeline. Set via `PE_SEED_AS_OF_DATES`. |
| `bootstrap_fetch_resolutions` | `false` | When `true`, `pe-bootstrap` fetches resolution data from the Polymarket Gamma API after the trade-fetch phase and stores it in `market_resolutions`. Set `PE_BOOTSTRAP_FETCH_RESOLUTIONS=1` to enable. |
| `bootstrap_rebuild_resolutions` | `false` | One-shot retroactive correction (issue #149 follow-up): when `true`, stage 6 deletes every `market_resolutions` row tagged with an imprecise source (`'gamma'`, `'clob'`) before any fetcher runs. Stage 6a (Polygon RPC, if configured) and 6b (Dune) then re-populate those markets with block-timestamp `resolved_at_unix` values via `INSERT OR IGNORE`. Idempotent — safe to set on every run; once all rows are precision-sourced subsequent runs delete 0 rows and skip the re-fetch. Set `PE_BOOTSTRAP_REBUILD_RESOLUTIONS=1` to enable. |
| `bootstrap_fetch_funder_graph` | `false` | When `true`, `pe-bootstrap` queries Etherscan for funder edges for every wallet not yet in `funder_lookup_done` and persists them in `funder_edges`. Per-wallet atomic commit enables resume after failure. One-time ~4–5 h for ~23k wallets (concurrent N=4, token-bucket 3 req/s); all subsequent runs are near-instant. Requires `PE_ETHERSCAN_API_KEY`. Set `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1` to enable. |
| `bootstrap_skip_trade_fetch` | `false` | When `true`, `pe-bootstrap` skips the Polymarket trade-fetch step entirely. Safe when the trade cache is already fully populated and only subsequent steps (funder graph, resolutions, filters) need to run. Emits a warn-level log. Set `PE_BOOTSTRAP_SKIP_TRADE_FETCH=1` to enable. |
| `infra_probe_span_secs` | `3600` | Maximum span (newest − oldest, seconds) across the first 500 trades of a cold-start wallet for it to be classified as infrastructure (issue #197). 500 trades in < 1 h ⇒ > 8 trades/min ⇒ market-maker / treasury / arbitrage bot. Below threshold: wallet is flagged `is_infra = 1`, the probe page is discarded, and downstream consumers skip via the `active_tradeable_wallets` view. The same threshold drives the `pe-bootstrap classify-infra` retroactive sweep over already-cached trades. Override via `PE_BOOTSTRAP_INFRA_SPAN_SECS`; canonical const lives in `pe_bootstrap::infra_probe::DEFAULT_INFRA_SPAN_SECS`. |
| `bootstrap_write_snapshot` | `false` | When `true`, the main `pe-bootstrap` pipeline persists a `(snapshot_at_unix, wallet)` row-set to `leaderboard_snapshots` at run time, stamped with the current `snapshot_at`. Default `false` keeps ad-hoc bootstrap runs (resolutions watchdog retries, funder-graph reruns, dev shells) from polluting the snapshot timeline with near-duplicate intra-day rows — only the official weekly refresh path should opt in. Historical seeding via `PE_SEED_AS_OF_DATES` is independent of this flag and always writes its target rows. Set `PE_BOOTSTRAP_WRITE_SNAPSHOT=1` to enable. |
| `bootstrap_gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for the Polymarket Gamma API. Override via `PE_GAMMA_BASE_URL` (useful for testing against a stub). |
| `bootstrap_gamma_min_interval_ms` | 50 | Minimum milliseconds between Gamma API requests (20 req/s). Live-tested ceiling is ≥ 27 req/s; 50 ms keeps ~25% margin. Enforced globally by `ReqwestFetcher`'s shared mutex regardless of caller concurrency. |
| `bootstrap_gamma_concurrency` | 10 | Number of in-flight Gamma requests issued concurrently per fetch loop (`buffer_unordered`). With ~300 ms per-request RTT, ~6 in-flight saturates the 20 req/s rate limit; 10 leaves headroom for latency spikes. The global rate cap is still enforced by `bootstrap_gamma_min_interval_ms`. |
| `bootstrap_event_orphan_warn_pct` | 99 | `pe-bootstrap events` coverage gate (issue #206): warn if more than 99% of distinct traded markets are orphans (self-mapped). Gamma's /events covers only a curated subset of all condition IDs; a 90–99% orphan rate on a large historical cache is expected and correct. The format-break hard-fail (abort if `events_seen > 0` but `conditions_mapped == 0`) replaces the old percentage-based abort. Const `ORPHAN_WARN_PCT` in `pe_bootstrap::events`. |
| `market_fee_missing_default_bps` | 0 | Default fee in bps when Gamma sets `feesEnabled = false`, omits `feeSchedule`, or omits `feeSchedule.rate`. Zero is the safe sentinel: it never over-discounts PnL and treats pre-fee-era markets correctly. Used by `fees_for_market` / `rate_to_bps` in `pe_bootstrap::events`. |
| `market_fee_max_bps` | 10_000 | Upper clamp for `market_fees.taker_base_fee_bps` / `maker_base_fee_bps`. Polymarket's live `feeSchedule.rate` is `0.04` (= 400 bps); the 10 000 bps ceiling (100%) is a hard guard against malformed API responses, not a normal value. |
| `bootstrap_polygon_rpc_url` | `None` | Polygon JSON-RPC URL for the CTF `eth_getLogs` resolution scan (issue #149 multi-source pipeline). When unset the Polygon stage is skipped — daily backfills then rely on CLOB + Dune. Set via `PE_BOOTSTRAP_POLYGON_RPC_URL`. |
| `bootstrap_polygon_ctf_chunk_blocks` | 10_000 | Block-range chunk size for the Polygon CTF scan. Larger chunks issue fewer RPC calls but are more likely to hit provider response-size caps and trigger the bisect-on-cap fallback. Set via `PE_BOOTSTRAP_POLYGON_CTF_CHUNK_BLOCKS`. |
| `bootstrap_clob_base_url` | `https://clob.polymarket.com` | Base URL for the Polymarket CLOB API (`/markets?closed=true` paginated listing). Override via `PE_CLOB_BASE_URL` for testing against a stub. |
| `bootstrap_clob_concurrency` | 8 | Number of in-flight CLOB requests issued concurrently per fetch loop, mirroring the Gamma `buffer_unordered` pattern. Set via `PE_BOOTSTRAP_CLOB_CONCURRENCY`. |
| `bootstrap_pile_activation_min_trades` | 100 | Minimum trade count (DB `trade_count` OR Dune `dune_closed_markets`) for a non-infra wallet to be activated in the pile (issue #166). Curation-list membership (Polymarket leaderboard / Radion / 502-gap) bypasses this gate. Hardcoded as `pe_bootstrap::pile::PILE_ACTIVATION_MIN_TRADES`; changing it requires re-migrating the pile. |
| `bootstrap_discovery_lookback_days` | 2 | Cold-start lookback (days) for `pe-bootstrap discovery` when the `source_cursor.dune_discovery_last_run` row is absent. Matches the 48h timer interval so warm restarts pick up where the previous run left off via the cursor. Set via `PE_BOOTSTRAP_DISCOVERY_LOOKBACK_DAYS`. |
| `bootstrap_backfill_limit` | 0 (no limit) | Per-run cap on `pe-bootstrap backfill`. `0` processes every wallet whose `last_polymarket_fetch_at` is NULL or older than 1 day. Initial deployment runs with `0` to drain the bulk catch-up queue; steady-state daily timers may set a positive value if daily run time grows unmanageable. Set via `PE_BOOTSTRAP_BACKFILL_LIMIT`. |
| `bootstrap_weekly_limit` | 200 | Per-run cap on `pe-bootstrap weekly`. `0` removes the cap. Weekly funder refresh runs against Etherscan; the cap throttles API budget on the Sunday timer. Set via `PE_BOOTSTRAP_WEEKLY_LIMIT`. |
| `bootstrap_funder_limit` | 200 | Per-run cap on the one-shot `pe-bootstrap funder` lookup (issue #201). Mirrors `bootstrap_weekly_limit` (the Etherscan-budget-throttle precedent), NOT `bootstrap_backfill_limit`'s `0`: the funder hits Etherscan, so a bounded default keeps the one-shot short and avoids the ~15h full-backlog surprise. `0` = no limit (explicit opt-in for a full run). The funder candidate set is scoped to `active_tradeable_wallets` (is_active=1 AND is_infra=0). Set via `PE_BOOTSTRAP_FUNDER_LIMIT`. |
| `bootstrap_funder_rate_limit_rps` | 3 | Etherscan request-rate cap (req/s) for funder discovery (issue #201). `3` = free-tier budget. Raise on a paid Etherscan tier to shorten a large funder backlog (at 3 req/s a full ~164k-wallet active backlog is ~15h). `0` or out-of-range falls back to 3. Set via `PE_BOOTSTRAP_FUNDER_RATE_LIMIT_RPS`. |
| `bootstrap_funder_topic_batch_size` | 1_000 | Wallets per `topic[2]` filter in the batched `eth_getLogs` funder backend (issue #203 — the bulk-path counterpart to the Etherscan funder). Larger batches mean fewer block-range passes but bigger request payloads; Alchemy caps topic-array size, so validate before raising. Total scan cost ≈ `block_pages × ceil(pending / batch_size)`. Set via `PE_BOOTSTRAP_FUNDER_TOPIC_BATCH_SIZE`. |
| `bootstrap_funder_block_chunk` | 10_000 | Block-range chunk size for the batched `eth_getLogs` funder scan (issue #203). Independent of `bootstrap_polygon_ctf_chunk_blocks` (a separate scan); the bisect-on-cap fallback subdivides dense ranges that exceed the provider response-size cap. Set via `PE_BOOTSTRAP_FUNDER_BLOCK_CHUNK`. |
| `bootstrap_known_wallets_dune_table` | `"apexurellc.known_wallets"` | Dune user table (under `dune_namespace`) where `pe-bootstrap discovery` uploads the current pile for the anti-join. Replaced on every run (DELETE → CREATE → INSERT). Set via `PE_BOOTSTRAP_KNOWN_WALLETS_DUNE_TABLE`. |
| `bootstrap_backfill_staleness_secs` | 86_400 (1 day) | Per-wallet staleness window for `pe-bootstrap backfill` (issue #166). A wallet is eligible for re-fetch when `last_polymarket_fetch_at IS NULL OR < now - 86_400`. Hardcoded as `pe_bootstrap::pile::BACKFILL_STALENESS_SECS`; matches the daily systemd timer cadence. |
| `bootstrap_weekly_staleness_secs` | 604_800 (7 days) | Per-wallet staleness window for `pe-bootstrap weekly` (issue #166). A wallet is eligible for funder re-fetch when `last_funder_fetch_at IS NULL OR < now - 604_800`. Hardcoded as `pe_bootstrap::pile::WEEKLY_STALENESS_SECS`; matches the Sunday systemd timer cadence. |
| `bootstrap_funder_fallback_to_block` | 200_000_000 | Fallback upper-bound Polygon block for `pe-bootstrap weekly` when Etherscan's `eth_blockNumber` is unavailable (issue #166). Set well above the chain head as of 2026-05; advance manually if Polygon catches up. Hardcoded as `pe_bootstrap::weekly::FALLBACK_TO_BLOCK`. |
| `bootstrap_polymarket_delta_mode` | `"shadow"` | Delta-backfill mode for `pe-bootstrap backfill` (issue #176). `"off"` = legacy fetch-all-due. `"shadow"` (default) = run on-chain CTF `OrderFilled` scan + legacy full fetch on every run; classifications written to `delta_audit` table. `"delta"` = use the scan to filter the fetch set; weekly paranoia backstops. Set via `PE_BOOTSTRAP_POLYMARKET_DELTA_MODE`. |
| `bootstrap_polygon_ctf_confirmations` | 256 | Polygon confirmation depth (blocks ≈ 8.5 min at 2 s blocktime) the delta scanner subtracts from chain head to derive `to_block` (issue #176). Covers worst-case observed reorg depth. Set via `PE_BOOTSTRAP_POLYGON_CTF_CONFIRMATIONS`. |
| `bootstrap_polymarket_full_fetch_staleness_secs` | 604_800 (7 days) | Paranoia staleness window for the weekly full-fetch backstop in delta mode (issue #176). Wallets whose `last_polymarket_full_at` is NULL or older than this many seconds are auto-unioned into the fetch set regardless of the on-chain scan result — bounds worst-case staleness if the scanner ever misses a wallet. Set via `PE_BOOTSTRAP_POLYMARKET_FULL_FETCH_STALENESS_SECS`. |
| `bootstrap_delta_cold_start_lookback_blocks` | 43_200 | Cold-start lookback (Polygon blocks ≈ 24 h at 2 s blocktime) used by `polygon_ctf_delta::scan_active_wallets` when no `polygon_ctf_backfill_last_block` cursor is present in `source_cursor` (issue #176). Keeps the first delta-scan run from walking the entire chain history. Hardcoded as `pe_bootstrap::polygon_ctf_delta::COLD_START_LOOKBACK_BLOCKS`. |
| `bootstrap_delta_rate_limit_max_backoff_secs` | 32 | Maximum per-attempt backoff (seconds) for `eth_get_logs_bisect`'s rate-limit retry loop (issue #176 follow-up). Exponential backoff `1 → 2 → 4 → 8 → 16 → 32` s; once the next backoff would exceed this cap, the retry loop exits and the error propagates so `backfill::run_backfill` can fall back to legacy fetch. Mirrors the pattern in `funder_discovery::EthGetLogsLookup` but bounded (the bisect helper is single-shot per call frame, not a BFS). Hardcoded as `pe_source_onchain_polygon::eth_logs::RATE_LIMIT_MAX_BACKOFF_SECS`. |

#### Wallet pile (`wallets` table, issue #166)

Canonical wallet identity store maintained by the `migrate` / `discovery` /
`backfill` / `weekly` subcommands. `wallet_hex` form is `"0x" + 40 lowercase
hex chars` (matches `WalletAddress::Display` in `crates/core-types`).

`source_bits` masks (defined in `crates/bootstrap/src/pile.rs`):

| Bit | Mask | Source |
|---:|---:|---|
| 0 | 0b0000001 | `wallet_set.json` |
| 1 | 0b0000010 | `trades` table (DB-resident) |
| 2 | 0b0000100 | Dune CSV (generic) |
| 3 | 0b0001000 | Dune incremental discovery |
| 4 | 0b0010000 | Polymarket leaderboard |
| 5 | 0b0100000 | Radion |
| 6 | 0b1000000 | 502-gap |

Activation rule (`is_infra = 0` gates every branch — a wallet listed in both
the infra CSV and a curation list stays inactive):

```sql
UPDATE wallets SET is_active = 1
WHERE is_active = 0 AND is_infra = 0 AND (
    COALESCE(trade_count, 0) >= 100
 OR COALESCE(dune_closed_markets, 0) >= 100
 OR (source_bits & 16) != 0   -- in_leaderboard
 OR (source_bits & 32) != 0   -- in_radion
 OR (source_bits & 64) != 0   -- in_502_gap
)
```

`is_active` and `is_infra` are sticky once set (never decay). Re-running
`migrate` after edits to the input CSVs only adds source bits and infra flags;
it never removes them. To "un-mark" a wallet, delete its row.

#### Leaderboard snapshots (`leaderboard_snapshots` table)

Persists weekly leaderboard state in `wallet_cache.db`. One row-set per `pe-bootstrap` run (live path uses `as_of = NOW()`; historical seed path uses each date in `PE_SEED_AS_OF_DATES`). Read by `pe-backtest` at simulation startup to constrain the candidate-wallet pool at each weekly boundary.

Schema:
```sql
CREATE TABLE leaderboard_snapshots (
    snapshot_at_unix INTEGER NOT NULL,
    wallet_hex       TEXT    NOT NULL,
    PRIMARY KEY (snapshot_at_unix, wallet_hex)
);
```

**Look-ahead invariant.** The Dune wallet-discovery SQL is parameterized with an `as_of` cutoff that fences three forward-looking surfaces: the `resolved` CTE (resolutions before `as_of` only), the `recently_active` CTE (trades in `[as_of - active_window, as_of)`), and the `wallet_condition` join (trades before `as_of` only). Without all three, a snapshot taken "as of" a past date would still leak future market outcomes through the win-rate computation. Verified by `tests::rendered_sql_fences_all_three_forward_surfaces` and `tests::rendered_sql_contains_no_now_call` in `crates/bootstrap/src/dune.rs`.

**Backtest semantics.** At each simulated day `D`, the simulation looks up the most-recent snapshot ≤ `D` and filters reconstructed `TraderLedger`s to that wallet set BEFORE the ranker groups by operator. Strict (wallet-level) filter: even if wallet `X` shares an `operator_id` with `Y, Z` that are in the snapshot, `X`'s ledger does not contribute to the operator group's score because we wouldn't have known about `X` that week. When the table is empty the simulation falls back to "all wallets in trade history" with a single warning at start (legacy behavior; survivorship-biased).

### Backtest defaults (`pe-backtest`)

| Key | Default | Meaning |
|---|---:|---|
| `backtest_slippage_bps` | 100 | Conservative fill-cost assumption per trade; converted to a rate (`bps / 10_000`) and applied proportionally: BUY fill = `price × (1 + rate)`, SELL fill = `price × (1 − rate)`. The same rate feeds `WinnerFollowConfig.slippage_rate` so Kelly sizing and fill accounting are consistent. |
| `backtest_step_days` | 1 | Walk-forward simulation step in days; set via `PE_BACKTEST_STEP_DAYS` |
| `backtest_bankroll_usd` | 10000 | Starting bankroll in USD; set via `PE_BANKROLL_USD` |
| `backtest_audit_window_days` | 90 | Trade lookback window for ledger reconstruction during simulation; set via `PE_BACKTEST_AUDIT_WINDOW_DAYS` |
| `backtest_min_reconstruction_quality` | 0 | Minimum reconstruction quality (0–100) for watchlist eligibility in backtest. Default 0 (not 60) because Polymarket's CLOB API omits market-resolution redemption events; most positions appear "open" even when settled. The leaderboard snapshot serves as the quality proxy instead. Set via `PE_BACKTEST_MIN_QUALITY`. |
| `backtest_active_min_closed_trades` | 10 | Min closed trades in 180-day window for active tier. Relaxed from live-system default (60) due to missing resolution data. Set via `PE_BACKTEST_ACTIVE_MIN_CLOSED`. |
| `backtest_active_min_distinct_markets` | 1 | Min distinct markets in 180-day window for active tier. Lowered from 5 (was 30 in live): N_eff replaces the hard filter. Set via `PE_BACKTEST_ACTIVE_MIN_MARKETS`. |
| `backtest_incubator_min_closed_trades` | 3 | Min closed trades in 90-day window for incubator tier. Unchanged. Set via `PE_BACKTEST_INCUBATOR_MIN_CLOSED`. |
| `backtest_incubator_min_distinct_markets` | 1 | Min distinct markets in 90-day window for incubator tier. Lowered from 2 (was 10 in live): N_eff replaces the hard filter. Set via `PE_BACKTEST_INCUBATOR_MIN_MARKETS`. |
| `backtest_kelly_sweep_fractions_default` | `"0.10,0.25,0.50,0.75,1.0"` | Default sweep fractions when `PE_BACKTEST_KELLY_SWEEP` is set but empty. Each value must be in `(0.0, 1.0]`; 1.0 = full Kelly. Research only — production never sets this env var. |
| `kelly_p_prior_alpha_default` | 10 | α of the Beta(α,β) prior on leader win-rate `p`. Prior strength = α+β = 20 trades centred at 0.5. `(α=0, β=0)` reproduces the raw empirical-rate path. Set via `PE_BACKTEST_KELLY_P_PRIOR_ALPHA`. |
| `kelly_p_prior_beta_default` | 10 | β of the Beta(α,β) prior on leader win-rate `p`. See `kelly_p_prior_alpha_default`. Set via `PE_BACKTEST_KELLY_P_PRIOR_BETA`. |
| `kelly_p_k_per_market_default` | 6 | Effective-sample-size scaling factor for N_eff. `N_eff = min(total, distinct_markets × k)`. Starting point: each market ≈ 6 independent observations. `k=0` bypasses N_eff entirely (uses total directly). Set via `PE_BACKTEST_KELLY_P_K_PER_MARKET`. |
| `kelly_p_min_snapshots_default` | 4 | Snapshot-aware Beta prior threshold (issue #129). Leaders appearing in fewer historical leaderboards receive extra symmetric pseudo-observations: `extra = min_snapshots.saturating_sub(n_snapshots).saturating_mul(extra_per_missing_snapshot)`, applied as `p_shrunk = (scaled_wins + α + extra) / (N_eff + α + β + 2·extra)`. As `extra` grows the effective prior point migrates from `α/(α+β)` toward 0.5. Default 4 ≈ 1 month at weekly snapshot cadence. Inclusive cutoff matching `for_date`. Worked example: leader with 11 wins / 1 loss, k=6, n_snapshots=1 → `extra = (4-1)×5 = 15`, `p_shrunk = 36/62 ≈ 0.581` (vs `21/32 ≈ 0.656` without strengthening). Setting to `0` disables the prior entirely. Set via `PE_BACKTEST_KELLY_P_MIN_SNAPSHOTS`. |
| `kelly_p_extra_per_missing_snapshot_default` | 5 | Pseudo-observations added per missing snapshot. See `kelly_p_min_snapshots_default` for the full formula. Both ops use saturating arithmetic. Set via `PE_BACKTEST_KELLY_P_EXTRA_PER_MISSING_SNAPSHOT`. |
| `liquidity_take_fraction_default` | `0.05` | Fraction of cached Gamma `liquidity` USD the sizer may take per BUY. Applied as `max_contracts = floor(take_fraction × liquidity_usd / fill_price)` in `simulation.rs` after `evaluate()` returns. `0` disables the gate (silent passthrough). Worked example: at `liquidity_usd = $5,000`, `take_fraction = 0.05`, `fill_price = $0.50` → max contracts = `floor(5000 × 0.05 / 0.50) = 500`. Set via `PE_BACKTEST_LIQUIDITY_TAKE_FRACTION`. **Backtest staleness caveat:** stored value is depth-at-last-bootstrap-refresh (≈ now), applied uniformly across the entire historical sim window — markets that *grew* in depth get over-clamped, markets that *shrank* get under-clamped. |
| `liquidity_min_required_usd_default` | 200 | Minimum Gamma `liquidity` USD required to apply the clamp. Below this floor, depth data is too noisy to act on — clamp is bypassed (passthrough with `tracing::warn!`). Tracked via report counter `liquidity_below_floor_bypasses`. Set via `PE_BACKTEST_LIQUIDITY_MIN_REQUIRED_USD`. |
| `per_trade_cap_default` | `mode_default` | Default `PerTradeCap` variant: resolves to 25 bps for LiveTiny, 100 bps for Promoted. Override with `PE_BACKTEST_PER_TRADE_CAP=bps:N` or `PE_BACKTEST_PER_TRADE_CAP=unlimited` in backtest. |
| `per_trade_cap_unlimited_resolved_bps` | 10 000 | Effective cap in basis points when `PerTradeCap::Unlimited` is selected. Full bankroll — Kelly fraction is the only size constraint. |
| `flat_usd_per_trade_default` | `None` | Default for `WinnerFollowConfig.flat_usd_per_trade`. `None` = Kelly sizing (default). Set to a positive USD notional (e.g. `25.00`) to bypass Kelly sizing (steps 4–5 of `evaluate`) and size each BUY as `max(1, floor(flat / leader_price))` contracts. Per-trade cap and risk gate remain active in both paths. Eliminates bankroll compounding — position size does not grow with bankroll. Use only when the Kelly `p` input is a per-leader-constant with no per-trade signal (issue #161). |
| `backtest_suppression_warn_threshold_pct` | 30 | Single warn threshold shared by every per-quarter BUY-signal suppression diagnostic (`expiry_filter_suppression_pct`, `high_price_suppression_pct`, …). Logged as a warning when any quarter exceeds it. Hardcoded as `SUPPRESSION_WARN_THRESHOLD` in `crates/backtest/src/simulation.rs`. |
| `backtest_require_known_expiry_default` | `true` | Strict-mode flag for the `max_hours_to_expiry` filter. `true` (default after #137 Sub-PR 3, gated on PR #154 stage 6f raising trade-set schedule coverage to 99.64%) — when both schedule and resolution are absent for a market, the BUY signal fails closed (suppressed). `false` (rollback / legacy) — both-absent allows the trade through. The fallback chain (schedule → resolution → flag) was unified in PR #139; pre-#139 the NULL-schedule path short-circuited to allow regardless of resolution. Set via `PE_BACKTEST_REQUIRE_KNOWN_EXPIRY`. |
| `backtest_max_positions_per_market_default` | `Some(1)` | Cap on concurrent open positions per `market_id` (issue #138). `None` disables the cap entirely. `Some(n)` blocks new BUY signals on any market that already has ≥ `n` open positions across every leader and outcome — leader A on outcome 0 and leader B on outcome 1 of the same binary market count against the same slot. Slot reopens when positions close via SELL or resolution sweep. `NonZeroU32` rejects `0` at deserialize-time so `PE_BACKTEST_MAX_POSITIONS_PER_MARKET=0` is an explicit error. Set via `PE_BACKTEST_MAX_POSITIONS_PER_MARKET`. |
| `backtest_skip_unknown_operator_default` | `true` | Suppress BUY signals from watchlisted leaders whose `op_identity` is unresolved in the funder graph (issue #141). Discriminator is `op_identity.is_none()` — fires uniformly when the funder-edge cache is empty/stale (the "no Etherscan data" production case) and for any individual wallet that the clustering does not attach to an operator. Production-correct by default; scenarios that don't supply funder edges opt out via `skip_unknown_operator: false`. Suppression rate is reported as `unknown_operator_suppression_pct` (and per-quarter breakdown). Set via `PE_BACKTEST_SKIP_UNKNOWN_OPERATOR`. |
| `backtest_max_signal_price_default` | `Some(0.85)` | Upper-bound cap on slippage-adjusted `fill_price` for BUY copies (issue #142). `None` disables the cap. `Some(cap)` skips any BUY where `fill_price >= cap` — comparison is `>=` (not `>`), so a fill at exactly `cap` is suppressed; strictly conservative. Gates on `fill_price = signal_price × (1 + slippage_rate)` rather than the leader's signal price so a 0.849 + 1% slippage = 0.857 cannot squeak past. High-price contracts have catastrophic payoff geometry (100 bps slippage on a $0.99 contract burns nearly all upside; binary $0/$1 payoff means any miss is total loss). A3 analysis showed +$20.60 oracle lift in-sample at this threshold. Set via `PE_BACKTEST_MAX_SIGNAL_PRICE`. |

### Skill-selection defaults (`pe-skill-select`)

Config for the skill-based wallet-selection pipeline (issue #212; epic #209). Loaded via `figment` from `PE_SKILL_*` env overlaid on an optional TOML; defaults live in `crates/skill-select/src/config.rs`.

| Key | Default | Meaning |
|---|---:|---|
| `skill_cutoff_unix` | `1_775_001_599` (2026-03-31T23:59:59Z) | Train/forward split: features + skill test use trades with `closed_at ≤ cutoff`; the forward test (later slice) uses the post-cutoff window. **Derived from the date** at compile time (`time::macros::datetime!`), never a hand-typed constant — guards against the off-by-one-year unix mis-encoding. Set via `PE_SKILL_CUTOFF_UNIX`. |
| `skill_min_closed_trades` | 20 | Minimum train-window closed trades for a wallet to be scored; below this `extract_features` returns `None` and the wallet is skipped. Set via `PE_SKILL_MIN_CLOSED_TRADES`. |
| `skill_min_trading_days` | 20 | Minimum distinct UTC trading days (= daily-return series length `n`) for a wallet to be eligible for the **deflated-Sharpe ranking**. The Sharpe of a few-point series is degenerate (population std of 2 points is `\|Δ\|/2`, so `mean/std` is unbounded — observed Sharpes of 180+ on 2-day histories); below this gate the wallet stays BHq-significant but is excluded from `selected`. Gates the *ranking* only, not the skill-significance test (the event-level sign-randomization p-value is valid at any day count). Set via `PE_SKILL_MIN_TRADING_DAYS`. |
| `skill_permutations` | 999 | Sign-randomization permutation count for the event-level skill test (`sign_randomization_test`). Set via `PE_SKILL_PERMUTATIONS`. |
| `skill_rng_seed` | 42 | Fixed `SplitMix64` seed for the permutation test — makes the p-values reproducible bit-for-bit. Set via `PE_SKILL_RNG_SEED`. |
| `skill_bhq_q_bps` | 1000 (q = 0.10) | Benjamini–Hochberg false-discovery-rate `q` (basis points) for the primary selection gate over the permutation p-values. Set via `PE_SKILL_BHQ_Q_BPS`. |
| `skill_top_n` | 50 | Watchlist cap: the BHq-significant set ranked by deflated Sharpe is truncated to this many wallets. Set via `PE_SKILL_TOP_N`. |
| `skill_kelly_fraction_bps` | 1000 (f = 0.10) | Kelly fraction `f` for the forward-test secondary PnL; matches the paper-backtest convention (f≈0.1). Per resolved post-cutoff buy the stake is `f · max(0, (p−c)/(1−c))` of $1. Set via `PE_SKILL_KELLY_FRACTION_BPS`. |
| `skill_forward_min_bucket_trades` | 5 | Minimum ≤cutoff resolved buys in an entry-price bucket for a reliable calibration `p`; below this the Kelly position takes the **neutral base stake `f`** (flagged), not a full $1 — a full-$1 fallback gave uncalibrated positions 10× the stake of calibrated ones (`f·edge ≤ f`), letting the sparse set dominate the Kelly book. Set via `PE_SKILL_FORWARD_MIN_BUCKET_TRADES`. |
| `skill_forward_price_bucket_width_bps` | 1000 (0.10) | Entry-price bucket width for the calibration win-rate table (`bucket = floor(price / width)`). Set via `PE_SKILL_FORWARD_PRICE_BUCKET_WIDTH_BPS`. |
| `skill_min_distinct_events` | 10 | Minimum distinct events traded for a wallet to clear the SSRN 6617059 §C event-count gate; below this `extract_features` returns `None` and the wallet is skipped. The paper's skilled cohort averages 61 events / 79 markets — 10 is the lower bound at which the sign-randomization permutation test has meaningful power. Set via `PE_SKILL_MIN_DISTINCT_EVENTS`. |
| `skill_beta_binomial_alpha` | 1 | Beta-binomial conjugate-prior `α` for the shrunk-edge candidate feature (`bb_shrunk_edge_bps`): `p̂ = (x + α) / (n + α + β)` where `x` = wins among resolved buys, `n` = resolved-buy count. Default 1 is Laplace — mildest non-degenerate shrinkage. Set via `PE_SKILL_BETA_BINOMIAL_ALPHA`. |
| `skill_beta_binomial_beta` | 1 | Beta-binomial conjugate-prior `β`; default 1 (Laplace), matching `α`. Set via `PE_SKILL_BETA_BINOMIAL_BETA`. |
| `skill_extract_threads` | 0 | Rayon worker count for the per-wallet extraction loop. `0` means rayon's default (`rayon::current_num_threads()` — honours `RAYON_NUM_THREADS`, else CPU count); positive values force a dedicated pool of that size. Output is bit-identical regardless of thread count (set membership + per-row equality); only on-disk row insertion order varies. Set via `PE_SKILL_EXTRACT_THREADS`. |
| `skill_extract_clean_prior` | false | Opt-in (issue #236): when `true`, `pe-skill-select extract` deletes every `wallet_features` row at the chosen `cutoff_unix` before the new extract begins. The default is "additive" — `INSERT OR REPLACE` rewrites rows for wallets that pass the new extract's gates but leaves rows for wallets the new extract chose not to write (the "ghost row" problem: after PR #234 the new `min_distinct_events ≥ 10` gate ejected 8,943 wallets that the prior extract had written, requiring a manual `DELETE FROM wallet_features WHERE cutoff_unix=… AND extracted_at_unix < <run_start>` before `select` / `composite` behaved). With `clean_prior=true` the cleanup is automatic. Set via `PE_SKILL_EXTRACT_CLEAN_PRIOR=1`. |
| `skill_forward_source` | `select` | Which ranker drives the `forward-test` subcommand's selected-wallet list: `select` (v1 deflated-Sharpe via `select_wallets`, default for backward compat) or `composite` (the docs/24- PR-4-MVP weighted-z-score ranker via `rank_by_composite`). Same `bhq_q_bps` / `top_n` / `min_trading_days` gates apply to both paths — only the rank function differs. Enables A/B comparison of the two rankers on the same holdout window without modifying either ranker's code path. Set via `PE_SKILL_FORWARD_SOURCE=select` or `PE_SKILL_FORWARD_SOURCE=composite`. |
| **Production ranking strategy** | temporal ensemble | The 2026-05-26 autonomous-iteration finding: **wallet rank consistency across monthly cutoffs is a stronger forward-edge signal than within-cutoff weight tuning** by 1-2 orders of magnitude. At full scale (top_n=5000) the cohort turns over ~50% per month and ~95% over 9 months. The intersection of top-5000 across N most-recent monthly cutoffs delivers 2-200× better per-position forward edge than any single-cutoff weighted ranker, and is empirically weight-invariant (`default` composite weights ≈ informed/EV-tuned weights at the ensemble level). The Optuna+PBO `scripts/composite_tuner/` tool (PR #242, #243) remains valid as a research diagnostic but is NOT the production recommendation — Optuna-deflated weight gains over informed_combo are ~$30k flat-$1; ensembling delivers 12-200× per-position gains. **To generate the production watchlist:** `.venv-analysis/bin/python3 scripts/monthly_rerank.py --db-path data/wallet_cache.db --strategy intersection_5 --weights default --out data/production-watchlist.txt`. Strategy choices: `intersection_K` for K∈{2,3,4,5,7} (highest quality / smallest cohort at higher K), `majority_K` (≥K/2+1 of K cutoffs; balanced), `weighted_recency_top5000` (5000 wallets sized to "watch up to 5000" target). Refresh monthly after a fresh `pe-skill-select extract` at the new month's cutoff. Memory: `project_temporal_ensemble_breakthrough.md` + `project_autonomous_iteration_final.md`. |
| **PBO (Probability of Backtest Overfitting)** | threshold 0.5 | Bailey & López de Prado (2014/2015) combinatorially symmetric cross-validation metric. Score matrix shape: (n_trials, n_windows). For each of n_perms random IS/OOS half-splits of windows: find the best-IS trial; compute its OOS rank ratio; logit-transform. PBO = fraction of splits where logit < 0 (best-IS trial is below the OOS median). **PBO ≤ 0.5 → real signal ("OK"); PBO > 0.5 → systematic overfit ("OVERFIT").** Canonical implementation: `scripts/composite_tuner/pbo.py::compute_pbo`. In `gbm_walkforward.py`, seeds are the trial axis and anchors are the window axis (one stochastic GBM seed per trial — a defensible approximation of the original per-configuration framing). The harness gate for a committed verdict: **n_seeds ≥ 2 AND n_anchors ≥ 4**; below this threshold the raw PBO value is preserved in the output JSON but `verdict = "undefined"`. Effective `n_perms = min(C(n_anchors, n_anchors // 2), --pbo-perms)` — at 4–5 anchors the combinatorial cap is C(4,2)=6 or C(5,2)=10 regardless of the flag. Output JSON key: `pbo` (top-level sibling of `aggregate` in schema_version=2). |
| **Composite-ranker weights** | per-bet quality dominant; signed bps | The `composite` subcommand z-score-standardises each of the 12 features (`sharpe_bps` + the 11 PR-1 columns) over the BHq-significant cohort, then ranks by `Σ w_i · z_i`. Defaults total ~10000 bps (1.0 in raw units; only relative magnitude matters after z-scoring): per-bet quality 5000 (six features × 833, `brier_score_bps` negated for "lower is better"), `sharpe_bps` 1500, concentration 1500 (`hhi −500`, `n_eff +500`, `rpc −500` — diversification preferred), activity (`first_entries_per_active_day`) 1000, capital velocity (`median_first_entry_to_resolution_secs` negated) −1000. Sign encodes direction (positive = higher-better, negative = lower-better). Override individually via `PE_SKILL_COMPOSITE_W_<NAME>` (e.g. `PE_SKILL_COMPOSITE_W_SHARPE_BPS=2000`). This is the **MVP** docs/24- PR 4 — the full ONC clustering / clustered MDA / non-negative elastic-net / PBO-deflated version lands as a follow-up. |
| **`longshot_bias_ratio_bps`** | `int((frac(c<0.20) − frac(c>0.80)) × 10_000)`, clamped `[−10_000, +10_000]` | Outcome-side specialization feature (#248 §4). Computed over resolved closed trades (same window / resolution-gating as the per-bet quality block). `c` is the entry price (`vwap_entry`) from `per_bet_outcomes` in `features.rs`. Positive = long-shot bias (wallet buys low-probability outcomes); negative = favourite bias. `0` when no resolved closed trades exist — the empty-set sentinel is indistinguishable from a perfectly balanced wallet, but the `min_distinct_events ≥ 10` gate upstream filters under-powered wallets before the GBM sees them. Thresholds are strict (`< 0.20`, `> 0.80`; exactly 0.20 / 0.80 is neither). Side filter: `per_bet_outcomes` iterates all resolved closed trades regardless of buy/sell-to-open side (mirrors the PR #234 per-bet quality convention; a side-filtered variant is a future follow-up). SQLite column: `INTEGER NOT NULL DEFAULT 0` (migration-safe). Consumer: `scripts/monthly_rerank_gbm.py` `FEATURE_COLS` + SELECT; composite-ranker integration deferred per tracker #248 GBM-only consumer strategy. |
| **`hold_to_resolution_rate_bps`** | `int((held_positions / qualifying_positions) × 10_000)`, clamped `[0, 10_000]` | Fraction of `(market, outcome)` positions where `cum_sells < cum_buys` at `market.resolved_at_unix`, computed over resolved markets with `resolved_at_unix ≤ cutoff_unix` that have at least one buy before/at resolution. Captures whether a wallet's pattern is to hold contracts through resolution vs. sell early. Computed from raw `RawTrade` events (not from `ClosedTrade`), so partial sells and multiple lots on the same outcome are handled correctly: buys and sells are accumulated in timestamp order up to the resolution timestamp. `0` sentinel when no qualifying position exists. SQLite column: `INTEGER NOT NULL DEFAULT 0` (migration-safe). Consumer: `scripts/monthly_rerank_gbm.py` `FEATURE_COLS` + SELECT; composite-ranker integration deferred per tracker #248 GBM-only consumer strategy. |
| **`position_sizing_cv_bps`** | `int(min(std / mean, 10) × 10_000)`, saturating at 100 000 | Sample coefficient of variation (ddof=1) of per-buy position size (`entry_price × contracts`) over buy-side closed trades in the train window, in basis points. Measures position-sizing consistency: a low CV wallet sizes bets predictably; a high CV wallet has erratic sizing (which may be informative of either opportunism or noise). Uses only `Side::Buy` closed trades; `0` when fewer than 2 buy-side trades exist or when the mean size is zero. Capped at 10× (= 100 000 bps) to prevent extreme outliers from dominating GBM splits. Computed from the `windowed` `ClosedTrade` slice (no `raw_trades` needed). SQLite column: `INTEGER NOT NULL DEFAULT 0` (migration-safe). Consumer: `scripts/monthly_rerank_gbm.py` `FEATURE_COLS` + SELECT; composite-ranker integration deferred per tracker #248 GBM-only consumer strategy. |
| **Candidate-features resolution-coverage bias** | resolved-markets-only for group A/D | The per-bet quality features (`ev_mean_bps`, `ev_tstat_bps`, `bb_shrunk_edge_bps`, `kelly_log_growth_bps`, `brier_score_bps`, `brier_resolution_bps`) and the capital-velocity feature (`median_first_entry_to_resolution_secs`) are computed over **closed trades whose market has a resolution row** — unresolved-market trades are excluded from those statistics (mirroring `forward.rs`). Sample-size fields (`closed_trades`, `distinct_markets`, `distinct_events`) and concentration (group F) are unchanged. This biases the per-bet estimates toward markets that have actually settled, which is the right empirical object for "did this wallet earn edge on resolved bets" but slightly under-represents long-tail unsettled positions. |
| **Forward-test fee posture** | gross-of-fees (v1) | The forward test reports PnL **gross of taker fees** (`ForwardReport::gross_of_fees == true`). The April-2026 holdout is entirely post-fee (Polymarket fees since 2026-03-30, Akey et al. SSRN 6443103); the headline overstates net edge by ≈ the taker fee. Netting awaits a per-market `takerBaseFee` backfill (not yet in the cache). |
| **`gbm_walkforward_min_mean_of_mean_edge`** | +0.01 (flat-$1 per position) | Minimum acceptable `mean_of_mean_edge` (arithmetic mean of per-anchor mean edges, gross of fees) across all anchors in a `gbm_walkforward.py` run. Derived from Phase 3 empirical results (2026-05-27): fwd=7d achieved +$0.066, fwd=14d +$0.085, fwd=30d +$0.066, all well above this floor. A run producing a mean-of-mean below +$0.01 should be treated as producing no statistically meaningful alpha until root-cause is understood (regime change vs. model degradation). |
| **`gbm_walkforward_min_anchor_positive_fraction`** | 0.60 | Minimum fraction of evaluated anchors with positive `mean_edge` in a `gbm_walkforward.py` run. Derived empirically: fwd=7d achieved 5/5 (100%), fwd=14d 4/5 (80%), fwd=30d 3/4 (75%). A single negative anchor in a 4–5 anchor run (i.e. 75–80% positive) is consistent with a transient market-regime effect and does not disqualify the strategy — Phase 3 confirmed the March 2026 failure is regime (fewer binary longshot opportunities), not wallet selection. Two or more consecutive negative anchors would require postmortem before proceeding. |
| **`gbm_walkforward_min_cohort_jaccard`** | 0.40 | Alert threshold for GBM `intersection_3` cohort Jaccard between consecutive monthly cutoffs (`|A∩B| / |A∪B|`). Phase 3 observed minimum: 0.494 (2026-03-31 → 2026-05-01). Below 0.40, cohort instability indicates the intersection is selecting near-randomly and the ensemble is unlikely to generalise. Monitor monthly after each re-extract + re-rank. BHq pool Jaccard (a looser signal) should stay above 0.60; empirical range 0.624–0.825. |
| **`gbm_walkforward_production_fwd_days`** | 7 | Preferred forward-evaluation window for the most robust walk-forward results. Phase 3 (2026-05-27, 5 anchors): fwd=7d is the only window with 0 negative anchors (March near-flat at +$0.003 vs negative at 14d/30d); std of mean_edge is tightest (0.064). fwd=14d has higher mean_edge (+$0.085 vs +$0.066) but March is worst there (−$0.031). fwd=30d has the largest total PnL but 4 anchors only (March −$0.019). Use fwd=14d for higher-alpha-potential runs with awareness of one historically bad month; use fwd=7d as the conservative validation gate. |
| **`gbm_walkforward_default_n_seeds`** | 5 | Number of GBM seeds for multi-seed ensemble in `gbm_walkforward.py` (`--n-seeds`). Seeds [42, 43, 44, 45, 46] are averaged before intersection. At n_seeds=5, PBO verdict uses seeds as the trial axis (n_trials=5) and anchors as the window axis. PBO=1.0 is expected at this small scale (C(4,2)=6 or C(5,2)=10 perms, n_trials=5) and does not disqualify results when we average all seeds rather than selecting the best — the multi-seed average mitigates the single-best-seed overfitting that PBO measures. |
