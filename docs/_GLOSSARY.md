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

| Key | Default | Meaning |
|---|---:|---|
| `wallet_enum_logs_page_cap` | 1_000 | Etherscan free-tier per-call `eth_getLogs` result cap. If a range returns exactly this many logs, the range is bisected and re-fetched. |
| `wallet_enum_from_block` | 33_605_403 | Earliest block to scan — approximate CTFExchange V1 deployment on Polygon. |
| `wallet_enum_rate_limit_delay_ms` | 200 | Delay between successive Etherscan calls for enumeration (shared 5 req/s budget). |
| `wallet_enum_max_backoff_secs` | 60 | Cap on retry backoff for transient enumeration errors. |
| `wallet_enum_max_attempts` | 6 | Maximum retry attempts per `eth_getLogs` call before failing. |

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
| `bootstrap_wallet_cache_path` | `"wallet_cache.db"` | SQLite trade cache. WAL mode provides per-commit durability — at most one in-flight wallet's transaction is lost on crash. Set via `PE_BOOTSTRAP_CACHE_PATH`. |
| `bootstrap_wallet_source` | `"etherscan"` | Wallet discovery backend (`"etherscan"` or `"dune"`); set via `PE_WALLET_SOURCE` |
| `bootstrap_wallet_from_block` | `CTF_EXCHANGE_V1_DEPLOY_BLOCK` (33_605_403) | Start block for Etherscan wallet scan; set via `PE_WALLET_FROM_BLOCK` |
| `bootstrap_wallet_to_block` | current chain head | End block for Etherscan wallet scan; set via `PE_WALLET_TO_BLOCK` (fetched from Etherscan if absent) |
| `bootstrap_wallet_set_path` | `"wallet_set.json"` | Path to the enumerated wallet address list. If the file exists, Etherscan/Dune enumeration is skipped entirely. Delete the file to force a fresh scan; set via `PE_BOOTSTRAP_WALLET_SET_PATH` |
| `bootstrap_seed_as_of_dates` | unset | Comma-separated `YYYY-MM-DD` UTC dates. When set, `pe-bootstrap` switches to historical-seed mode: runs the parameterized Dune query for each date and inserts the results into `leaderboard_snapshots`. Idempotent on `(snapshot_at_unix, wallet_hex)`. Mutually exclusive with the regular pipeline. Set via `PE_SEED_AS_OF_DATES`. |
| `bootstrap_fetch_resolutions` | `false` | When `true`, `pe-bootstrap` fetches resolution data from the Polymarket Gamma API after the trade-fetch phase and stores it in `market_resolutions`. Set `PE_BOOTSTRAP_FETCH_RESOLUTIONS=1` to enable. |
| `bootstrap_fetch_funder_graph` | `false` | When `true`, `pe-bootstrap` queries Etherscan for funder edges for every wallet not yet in `funder_lookup_done` and persists them in `funder_edges`. Per-wallet atomic commit enables resume after failure. One-time ~4–5 h for ~23k wallets (concurrent N=4, token-bucket 3 req/s); all subsequent runs are near-instant. Requires `PE_ETHERSCAN_API_KEY`. Set `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1` to enable. |
| `bootstrap_skip_trade_fetch` | `false` | When `true`, `pe-bootstrap` skips the Polymarket trade-fetch step entirely. Safe when the trade cache is already fully populated and only subsequent steps (funder graph, resolutions, filters) need to run. Emits a warn-level log. Set `PE_BOOTSTRAP_SKIP_TRADE_FETCH=1` to enable. |
| `bootstrap_gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for the Polymarket Gamma API. Override via `PE_GAMMA_BASE_URL` (useful for testing against a stub). |
| `bootstrap_gamma_min_interval_ms` | 100 | Minimum milliseconds between sequential Gamma API requests (10 req/s). Live-tested ceiling is ≥ 27 req/s; 100 ms is a conservative gate. |

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
| `expiry_filter_suppression_warn_threshold` | 30 | Warn threshold for `expiry_filter_suppression_pct` (percent of buy signals suppressed by `max_hours_to_expiry`). Logged as a warning when exceeded. |
| `backtest_require_known_expiry_default` | `false` | Strict-mode flag for the `max_hours_to_expiry` filter. `false` (default, behavioural no-op for Sub-PR 1 of issue #137) — when both schedule and resolution are absent the trade is allowed through. `true` (target after Sub-PR 3 of #137 lands CLOB-sourced schedules + coverage ≥95%) — both-absent fails closed. The fallback chain (schedule → resolution → flag) is unified post-Sub-PR 1; pre-fix the NULL-schedule path short-circuited to allow regardless of resolution. Set via `PE_BACKTEST_REQUIRE_KNOWN_EXPIRY`. |
| `backtest_max_positions_per_market_default` | `Some(1)` | Cap on concurrent open positions per `market_id` (issue #138). `None` disables the cap entirely. `Some(n)` blocks new BUY signals on any market that already has ≥ `n` open positions across every leader and outcome — leader A on outcome 0 and leader B on outcome 1 of the same binary market count against the same slot. Slot reopens when positions close via SELL or resolution sweep. `NonZeroU32` rejects `0` at deserialize-time so `PE_BACKTEST_MAX_POSITIONS_PER_MARKET=0` is an explicit error. Set via `PE_BACKTEST_MAX_POSITIONS_PER_MARKET`. |
