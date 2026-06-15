# Glossary — vocabulary, types, acronyms, defaults

Single source of truth for terms used across `docs/`. When a definition changes, update it here and let other files reference back.

## Vocabulary: wallet vs trader vs leader vs candidate

| Term | Definition |
|---|---|
| **Wallet** | A single Polygon address (or venue equivalent). Observable, public, but not necessarily a distinct economic actor. |
| **Trader** | A venue-account-level identity. On Polymarket, currently 1:1 with a public proxy wallet. On Kalshi, a venue user (anonymous in public trade messages). |
| **Candidate** | A trader currently being evaluated for inclusion in the watchlist. |
| **Leader** | A candidate that has passed eligibility thresholds and is in the active top-`active_watchlist_size` watchlist. |

Ranking, sizing, and risk caps apply per wallet. (The wallet→operator clustering layer was removed in #326 — see `docs/28-OPERATOR-GRAPH-ARCHIVE.md`.)

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

// Ledger reconstruction
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

Where the docs use vague qualifiers, these are the canonical defaults. They live in code as `WinnerFollowConfig` and are restated here for cross-reference.

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
| `position_reseed_interval_secs` | 300 | `ServiceConfig` field. Seconds between periodic leader-ledger reseeds from the positions API. 0 disables periodic reseeds (startup seed still runs). |
| `position_page_limit` | 500 | `ServiceConfig` field. Maximum positions to fetch per page when seeding the leader ledger. |
| `position_size_threshold` | 1 | `ServiceConfig` field. Minimum position size (contracts) to include; positions below this are treated as dust. |
| `position_max_pages` | 20 | **Module const** in `crates/service/src/position_seeder.rs` (not a TOML/env key). Safety backstop: pagination stops after this many pages per wallet; a `warn!` is emitted if hit. |

### Paper trading state (`paper-state`, issue #282)

| Key | Default | Meaning |
|---|---:|---|
| `paper_state_db_path` | `./paper_state.db` | Path to the crash-safe paper-state SQLite mirror (`seen_trades`, `fills`, `positions`, `leader_positions`, `bankroll`, `poll_cursors`, `meta`) |
| `paper_fill_haircut_bps` | 500 | BUY-side paper fill haircut (fee + slippage), basis points. Recorded fill `= min(limit·(1 + bps/10_000), 0.999)`. Mirrors the sizing cost `c` in `evaluate` |
| `paper_fill_slippage_bps` | 100 | SELL-side paper fill slippage (no taker fee), basis points. Recorded fill `= max(limit·(1 − bps/10_000), 0.001)`. Fill-realism only — diverges from `evaluate` sizing (which charges SELL nothing) and has no research analog |
| `paper_resolutions_path` | `./paper_resolutions.json` | Path to the JSON sidecar tracking settled-market resolution prices and bankroll credits. Loaded by `PnlLedger` and the `--report` flag; crash-safe atomic write |
| `gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for the Polymarket Gamma API used by the paper-pnl resolution poller. Shares the same 50 ms / 20 req/s rate limit as `bootstrap_gamma_min_interval_ms` |
| `gamma_resolution_poll_interval_secs` | 120 | Seconds between Gamma resolution poll rounds in the live service. 2-minute cadence (issue #343) keeps the settled-markets set and "just resolved" wins within ≤2 min of actual resolution; the poll is gated to markets with open unsettled positions and rate-limited (50 ms min-interval), so the frequency increase is bounded by the open-position set, not the full universe |
| `max_resolution_horizon_secs` | 259_200 (72 h) | `ServiceConfig` field. Drop entry signals whose market resolves further than this many seconds into the future. 0 disables the upper bound. Guards against locking capital in months-long markets (issue #290). Paired with `min_resolution_horizon_secs` — one resolution lookup serves both. |
| `min_resolution_horizon_secs` | 60 | `ServiceConfig` field. Drop entry signals whose market resolves *sooner* than this many seconds from now — a copy cannot realistically fill and hold a market about to resolve. 0 disables the lower bound. `docs/29`: the 1-minute copy floor; sub-minute "breaks down" (issue #339). |

### Copy-entry gate (first-ever-entry; issues #290, #339)

Copies only a leader's first-ever entry into a market that resolves within the configured horizon. The leader-price band was removed in #339 — live sizing is re-based on the current market price instead (see `max_fill_price` below and `docs/19-WINNER-FOLLOW-STRATEGY.md` "Copy-scope gates" for the full gate sequence and fail posture).

| Key | Default | Meaning |
|---|---:|---|
| `wallet_market_history_path` | `./wallet_market_history.json` | `ServiceConfig` field. Path to the JSON sidecar tracking each leader's previously-entered markets (loaded/merged/persisted at startup by `crate::wallet_history`). Drives the first-entry gate. |
| `max_fill_price` | `0.85` | `ServiceConfig` field (decimal string). Skip a BUY copy whose **current** market price is `>=` this (catastrophic payoff geometry near $1). `0` disables. Mirrors the issue-#142 backtest `max_signal_price` cap so live sizing matches backtest. A safety rail, not the old leader-price band; adjustable up to ~0.90–0.95 (issue #339). |
| `entry_gate_fail_closed` | `false` | `ServiceConfig` field. Posture for a wallet absent from the history map (fetch failed, no stale sidecar): `false` fails open (copies allowed, treat as new), `true` fails closed (blocked). The loader warns per absent wallet either way. |
| `history_max_pages` | 200 | **Module const** in `crates/service/src/wallet_history.rs` (not a TOML/env key). Safety backstop: per-wallet history pagination stops after this many 500-trade pages; a `warn!` is emitted if hit (older markets may be missed → possible false first-entry). |

### Live wallet source (Supabase ranking handoff, issue #339)

The local latency-shift ranker pushes append-only ranking batches to Supabase (`scripts/push_ranking_to_supabase.py`); `pe-service` reads the `latest_ranking` view on an interval and additively swaps in the live wallet set (`crate::live_watchlist::LiveWatchlist`, an `ArcSwap`). When `supabase_url` is empty the service falls back to `seed_watchlist_path`. The Supabase keys follow the secret precedent (plain `String`, empty default, never logged).

| Key | Default | Meaning |
|---|---:|---|
| `supabase_url` | `""` | `ServiceConfig` field. Supabase project REST base URL (e.g. `https://<ref>.supabase.co`). Empty disables the live source. `PE_SUPABASE_URL`. |
| `supabase_secret_key` | `""` | `ServiceConfig` field (secret). The service-role key, sent in **both** the `apikey` and `Authorization: Bearer` headers — Supabase's `sb_` keys are not JWTs, so PostgREST 401s (`PGRST301`) if the two headers differ. Bypasses RLS for the server-side read. `PE_SUPABASE_SECRET_KEY` from `.env`. |
| `supabase_anon_key` | `""` | `ServiceConfig` field (secret). Publishable/anon fallback used for both headers **only when `supabase_secret_key` is empty**; ignored otherwise. `PE_SUPABASE_ANON_KEY` from `.env`. |
| `supabase_refresh_interval_secs` | 300 | `ServiceConfig` field. Seconds between live-watchlist refresh polls. The refresh loop is spawned only when `supabase_url` is non-empty and this is `> 0`. |
| `supabase_sink_enabled` | `false` | `ServiceConfig` field. Enables the best-effort paper-fill/settlement sink to Supabase (issue #343). The sink task is spawned only when this is `true` **and** `supabase_url` is non-empty. Requires `supabase_secret_key` — under RLS the anon key can only read, so anon-only writes 403. `PE_SUPABASE_SINK_ENABLED`. |
| `supabase_sink_channel_capacity` | 256 | `ServiceConfig` field. Bounded mpsc capacity for the trade-path → sink event channel. Backpressure: drop-on-full (the periodic reconcile re-derives dropped fills/settlements from `paper_state`, so a drop self-heals). `PE_SUPABASE_SINK_CHANNEL_CAPACITY`. |
| `supabase_sink_reconcile_interval_secs` | 300 | `ServiceConfig` field. Seconds between periodic sink reconciles: a contiguous-prefix fill HWM catch-up over `list_fills()` plus a full re-upsert of `list_settled_markets()`, healing any dropped or failed live writes. `PE_SUPABASE_SINK_RECONCILE_INTERVAL_SECS`. |
| `SUPABASE_FETCH_LIMIT` | 25 | **Module const** in `crates/service/src/supabase_reader.rs`. Top-N wallets fetched per refresh (the live copy set; `?limit=`). The ranker pushes a deeper top-200 batch; #3 widens the fetch. |
| `SUPABASE_LIVE_CAP` | 200 | **Module const** in `supabase_reader.rs`. Upper bound on the accumulated (additive, never-evicted) live set across refreshes; matches the ranker's top-200 push. Eviction/demotion is deferred to the online policy (#3). |
| `LS_TSTAT_BPS_SCALE` | 1_000 | **Module const** in `supabase_reader.rs`. t-stat → `leader_score_bps` scale (ordering only, not a gate). A t-stat of 2.5 maps to 2500 bps. |

### Wallet enumeration and relocated chain primitives

Wallet discovery is now the **all-category Polymarket leaderboard** sweep
(#335). The Dune client was deleted in #335; the on-chain `eth_getLogs`
enumeration path, the delta scan, and funder discovery were deleted with the
`pe-source-onchain-polygon` crate in #326 — see `docs/28-OPERATOR-GRAPH-ARCHIVE.md`. The
small set of Polygon-RPC primitives the surviving paths still need was relocated
into `crates/bootstrap/src/chain.rs` (each constant keeps its `verified <date>
from <source>` comment and a self-validating keccak test):

| Symbol (`pe_bootstrap::chain::*`) | Purpose |
|---|---|
| `CTF`, `CTF_DEPLOY_BLOCK`, `TOPIC_CONDITION_RESOLUTION` | Precise market-resolution scan (`polygon_ctf::scan_resolutions`, issue #149). |
| `ALL_EXCHANGE_CONTRACTS`, `ALL_ORDER_FILLED_TOPICS`, `TOPIC_ORDER_FILLED_V1` | Legacy enum-state synthesis in `migrate::auto_migrate_legacy` and the Dune-arm enumeration completion marker. |
| `eth_get_logs_bisect` | Bisect-on-cap helper with a transient-error retry classifier (`TransientErrorKind`: rate-limit / decode / transport, exponential backoff, max 6 attempts) used by the resolution scan. |

Surviving cache/cursor artifacts — legacy, **read-only** on the Dune path:

| Key | Default | Meaning |
|---|---|---|
| `wallet_cache_mutation_lock` | `<cache_path>.lock` | PID-based RAII lock file (`pe_bootstrap::lock::CacheMutationLock`). Acquired by the cache-mutating subcommands (`winner-discovery`, `backfill`, `--backfill-v1-attribution`) to serialize cache mutations; stale-PID reclaim handles a crashed prior holder. |
| `wallets.polymarket_contracts_seen` (column) | `i64`, default `0` | Legacy OR-merged V1/V2 CTF-exchange attribution bitmask. Its on-chain enumeration writer was removed in #326, so every wallet is now left `0`; the column persists for schema backward-compat. |
| `wallet_enum_completed_contracts` / `wallet_enum_topic_hashes` / `wallet_enum_chunk_progress` (cursors) | `source_cursor` keys (`pe_bootstrap::migrate::CURSOR_WALLET_ENUM_*`) | Legacy on-chain enumeration progress. Still written once by the `wallet_set.json` → SQLite migration (`migrate::auto_migrate_legacy` → `save_enum_state`) as a migration-audit marker, but **no longer read** — the reader (`load_enum_state`) and the `enumerate` subcommand were removed in #335. |

**Trade-fetch scope (issue #181).** The per-wallet trade-fetch list comes from
`cache.wallets_with_source_bit(SRC_LEADERBOARD)` — wallets discovered via the
leaderboard sweep / legacy migration — not `cache.all_pile_wallet_hexes()` (the
full multi-million-row pile), which would explode the per-wallet Polymarket API
call count.

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
tracing::info!(progress = n, total = total_pending, "wallet backfill {}/{}", n, total_pending);
```

**Correct shape** (values are queryable fields; message is a static label):
```rust
tracing::info!(progress = n, total = total_pending, "wallet backfill progress");
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
| `active_watchlist_size` | 50 | Top-N active leaders |
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

`observed_at_bucket = floor(observed_at_ms / 1_000)` — 1-second buckets. The tuple `(leader, source_trade_id, market, outcome, side, observed_at_bucket)` is the unique idempotency key. Two events with the same key are the same trade.

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

### Demotion criteria

A leader is demoted (mode steps down: promoted → live-tiny → paper → off) when ANY of:

- live copied PnL underperforms simulation by ≥ 2 standard errors over a 14-day window;
- p95 copy delay drifts > 1.5× the production budget for two consecutive hourly windows;
- reconstruction quality drops by ≥ 20 points (out of 100);
- profit concentration (max single-market %) increases above the eligibility threshold;
- trader becomes inactive (no trades for ≥ 14 days);
- copied exits become unreliable (≥ 3 missed/late exits in 30 d).

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
| `bootstrap_polymarket_audit_window_days` | `None` (unlimited) | Trade lookback window. `None` means all available history; set `PE_BOOTSTRAP_AUDIT_WINDOW_DAYS` to an integer or `unlimited`/empty for no limit. Passed as `u32::MAX` to `TradeSnapshot` internally. |
| `bootstrap_incremental_fetch_known_id_threshold` | 3 | Number of consecutive already-cached `source_trade_id`s that signals incremental fetch is complete for a wallet |
| `bootstrap_trade_fetch_limit` | 500 | Trades per page when fetching wallet history from the Polymarket `/activity?type=TRADE` endpoint |
| `bootstrap_polymarket_pagination` | `"end-cursor"` | Pagination strategy for per-wallet trade fetch. Uses `end=<ts>` and `start=<ts>` cursor parameters on `/activity?type=TRADE`; no hard cap on history depth. |
| `bootstrap_polymarket_min_retry_after_secs` | 1 | Minimum sleep duration (seconds) when the Polymarket Data API returns HTTP 429. Floors the `Retry-After` header value so a zero or absent header does not cause a tight retry loop. |
| `bootstrap_polymarket_concurrency` | 16 | Concurrent per-wallet trade fetches against the Polymarket Data API; the `ReqwestFetcher` rate-limit gate caps aggregate throughput at ≤ 20 req/s regardless. Set via `PE_BOOTSTRAP_POLYMARKET_CONCURRENCY`. |
| `bootstrap_polymarket_wallet_timeout_secs` | 300 | Per-wallet wall-clock budget (seconds) for `pe-bootstrap backfill` / `run`'s Polymarket fetch loop (issue #173). `0` disables the timeout; positive values wrap each `fetch_wallet_incremental` call in `tokio::time::timeout`. Wallets that trip the budget are soft-failed (added to `FetchOutcome::failed`); the post-fetch pipeline still runs and `last_polymarket_fetch_at` remains NULL so the next backfill re-queues them. Set via `PE_BOOTSTRAP_POLYMARKET_WALLET_TIMEOUT_SECS`. |
| `bootstrap_wallet_cache_path` | `"wallet_cache.db"` | SQLite trade cache. WAL mode provides per-commit durability — at most one in-flight wallet's transaction is lost on crash. Set via `PE_BOOTSTRAP_CACHE_PATH`. |
| `bootstrap_wallet_set_path` | `"wallet_set.json"` | Path to a legacy enumerated wallet address list. Consumed once by `migrate::auto_migrate_legacy` (ingested into the pile, then deleted). Not produced by any current path; set via `PE_BOOTSTRAP_WALLET_SET_PATH` |
| `bootstrap_fetch_resolutions` | `false` | When `true`, `pe-bootstrap` fetches resolution data from the Polymarket Gamma API after the trade-fetch phase and stores it in `market_resolutions`. Set `PE_BOOTSTRAP_FETCH_RESOLUTIONS=1` to enable. |
| `bootstrap_rebuild_resolutions` | `false` | One-shot retroactive correction (issue #149 follow-up): when `true`, stage 6 deletes every `market_resolutions` row tagged with an imprecise source (`'gamma'`, `'clob'`) before any fetcher runs. Stage 6a (Polygon RPC, if configured) then re-populates those markets with block-timestamp `resolved_at_unix` values via `INSERT OR IGNORE`. Idempotent — safe to set on every run; once all rows are precision-sourced subsequent runs delete 0 rows and skip the re-fetch. Set `PE_BOOTSTRAP_REBUILD_RESOLUTIONS=1` to enable. |
| `bootstrap_skip_trade_fetch` | `false` | When `true`, `pe-bootstrap` skips the Polymarket trade-fetch step entirely. Safe when the trade cache is already fully populated and only subsequent steps (resolutions, filters) need to run. Emits a warn-level log. Set `PE_BOOTSTRAP_SKIP_TRADE_FETCH=1` to enable. |
| `infra_probe_span_secs` | `3600` | Maximum span (newest − oldest, seconds) across the first 500 trades of a cold-start wallet for it to be classified as infrastructure (issue #197). 500 trades in < 1 h ⇒ > 8 trades/min ⇒ market-maker / treasury / arbitrage bot. Below threshold: wallet is flagged `is_infra = 1`, the probe page is discarded, and downstream consumers skip via the `active_tradeable_wallets` view. The same threshold drives the `pe-bootstrap classify-infra` retroactive sweep over already-cached trades. Override via `PE_BOOTSTRAP_INFRA_SPAN_SECS`; canonical const lives in `pe_bootstrap::infra_probe::DEFAULT_INFRA_SPAN_SECS`. |
| `bootstrap_write_snapshot` | `false` | When `true`, the main `pe-bootstrap` pipeline persists a `(snapshot_at_unix, wallet)` row-set to `leaderboard_snapshots` at run time, stamped with the current `snapshot_at`. Default `false` keeps ad-hoc bootstrap runs (resolutions watchdog retries, dev shells) from polluting the snapshot timeline with near-duplicate intra-day rows — only the official weekly refresh path should opt in. Set `PE_BOOTSTRAP_WRITE_SNAPSHOT=1` to enable. |
| `bootstrap_gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for the Polymarket Gamma API. Override via `PE_GAMMA_BASE_URL` (useful for testing against a stub). |
| `bootstrap_gamma_min_interval_ms` | 50 | Minimum milliseconds between Gamma API requests (20 req/s). Live-tested ceiling is ≥ 27 req/s; 50 ms keeps ~25% margin. Enforced globally by `ReqwestFetcher`'s shared mutex regardless of caller concurrency. |
| `bootstrap_gamma_concurrency` | 10 | Number of in-flight Gamma requests issued concurrently per fetch loop (`buffer_unordered`). With ~300 ms per-request RTT, ~6 in-flight saturates the 20 req/s rate limit; 10 leaves headroom for latency spikes. The global rate cap is still enforced by `bootstrap_gamma_min_interval_ms`. |
| `bootstrap_event_orphan_warn_pct` | 99 | `pe-bootstrap events` coverage gate (issue #206): warn if more than 99% of distinct traded markets are orphans (self-mapped). Gamma's /events covers only a curated subset of all condition IDs; a 90–99% orphan rate on a large historical cache is expected and correct. The format-break hard-fail (abort if `events_seen > 0` but `conditions_mapped == 0`) replaces the old percentage-based abort. Const `ORPHAN_WARN_PCT` in `pe_bootstrap::events`. |
| `market_fee_missing_default_bps` | 0 | Default fee in bps when Gamma sets `feesEnabled = false`, omits `feeSchedule`, or omits `feeSchedule.rate`. Zero is the safe sentinel: it never over-discounts PnL and treats pre-fee-era markets correctly. Used by `fees_for_market` / `rate_to_bps` in `pe_bootstrap::events`. |
| `market_fee_max_bps` | 10_000 | Upper clamp for `market_fees.taker_base_fee_bps` / `maker_base_fee_bps`. Polymarket's live `feeSchedule.rate` is `0.04` (= 400 bps); the 10 000 bps ceiling (100%) is a hard guard against malformed API responses, not a normal value. |
| `bootstrap_polygon_rpc_url` | `None` | Polygon JSON-RPC URL for the CTF `eth_getLogs` resolution scan (issue #149 multi-source pipeline). When unset the Polygon stage is skipped — daily backfills then rely on CLOB + Gamma. Set via `PE_BOOTSTRAP_POLYGON_RPC_URL`. |
| `bootstrap_polygon_ctf_chunk_blocks` | 10_000 | Block-range chunk size for the Polygon CTF scan. Larger chunks issue fewer RPC calls but are more likely to hit provider response-size caps and trigger the bisect-on-cap fallback. Set via `PE_BOOTSTRAP_POLYGON_CTF_CHUNK_BLOCKS`. |
| `bootstrap_clob_base_url` | `https://clob.polymarket.com` | Base URL for the Polymarket CLOB API (`/markets?closed=true` paginated listing). Override via `PE_CLOB_BASE_URL` for testing against a stub. |
| `bootstrap_clob_concurrency` | 8 | Number of in-flight CLOB requests issued concurrently per fetch loop, mirroring the Gamma `buffer_unordered` pattern. Set via `PE_BOOTSTRAP_CLOB_CONCURRENCY`. |
| `bootstrap_pile_activation_min_trades` | 100 | Minimum trade count (DB `trade_count` OR Dune `dune_closed_markets`) for a non-infra wallet to be activated in the pile (issue #166). Curation-list membership (Polymarket leaderboard / Radion / 502-gap) bypasses this gate. Hardcoded as `pe_bootstrap::pile::PILE_ACTIVATION_MIN_TRADES`; changing it requires re-migrating the pile. |
| `bootstrap_backfill_limit` | 0 (no limit) | Per-run cap on `pe-bootstrap backfill`. `0` processes every wallet whose `last_polymarket_fetch_at` is NULL or older than 1 day. Initial deployment runs with `0` to drain the bulk catch-up queue; steady-state daily timers may set a positive value if daily run time grows unmanageable. Set via `PE_BOOTSTRAP_BACKFILL_LIMIT`. |
| `bootstrap_backfill_staleness_secs` | 86_400 (1 day) | Per-wallet staleness window for `pe-bootstrap backfill` (issue #166). A wallet is eligible for re-fetch when `last_polymarket_fetch_at IS NULL OR < now - 86_400`. Hardcoded as `pe_bootstrap::pile::BACKFILL_STALENESS_SECS`; matches the daily systemd timer cadence. |
| `bootstrap_leaderboard_request_interval_ms` | 500 | Minimum milliseconds between Polymarket leaderboard API requests during `pe-bootstrap winner-discovery` (issue #324). Applied per-fetch via `ReqwestFetcher::with_min_interval_ms`. Set via `PE_BOOTSTRAP_LEADERBOARD_REQUEST_INTERVAL_MS`. |
| `bootstrap_leaderboard_top_n` | 50 | Maximum wallets fetched per leaderboard slice during `pe-bootstrap winner-discovery` (#324; all-category in #335). The `/v1/leaderboard` API hard-caps `limit` at 50; larger values are silently truncated server-side (verified live 2026-06-14). Set via `PE_BOOTSTRAP_LEADERBOARD_TOP_N`. |
| `bootstrap_leaderboard_categories` | all 10 | Leaderboard categories swept by `winner-discovery` (#335). Default = `OVERALL, POLITICS, SPORTS, CRYPTO, CULTURE, MENTIONS, WEATHER, ECONOMICS, TECH, FINANCE`. Each is crossed with `{PNL,VOL} × {DAY,WEEK,MONTH,ALL}` (≤ 80 slices); a category the API rejects (4xx) is skipped with a `warn!`. Results dedupe before the pile upsert. Set via `PE_BOOTSTRAP_LEADERBOARD_CATEGORIES` (TOML array of category names). |
| `bootstrap_radion_request_interval_ms` | 500 | Minimum milliseconds between Radion REST API requests during `pe-bootstrap winner-discovery` (issue #324). Stub only until the Radion REST contract is finalised; ignored when `radion_api_url` is unset. Set via `PE_BOOTSTRAP_RADION_REQUEST_INTERVAL_MS`. |

#### Wallet pile (`wallets` table, issue #166)

Canonical wallet identity store maintained by the `migrate` / `discovery` /
`backfill` subcommands. `wallet_hex` form is `"0x" + 40 lowercase
hex chars` (matches `WalletAddress::Display` in `crates/core-types`).

`source_bits` masks (defined in `crates/bootstrap/src/pile.rs`):

| Bit | Mask | Source |
|---:|---:|---|
| 0 | 0b0000001 | `wallet_set.json` |
| 1 | 0b0000010 | `trades` table (DB-resident) |
| 2 | 0b0000100 | _(removed #335 — was Dune CSV; gap kept, persisted)_ |
| 3 | 0b0001000 | _(removed #335 — was Dune incremental; gap kept, persisted)_ |
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

Persists weekly leaderboard state in `wallet_cache.db`. One row-set per `pe-bootstrap` run, stamped `as_of = NOW()`. Read by `pe-backtest` at simulation startup to constrain the candidate-wallet pool at each weekly boundary.

Schema:
```sql
CREATE TABLE leaderboard_snapshots (
    snapshot_at_unix INTEGER NOT NULL,
    wallet_hex       TEXT    NOT NULL,
    PRIMARY KEY (snapshot_at_unix, wallet_hex)
);
```

**Backtest semantics.** At each simulated day `D`, the simulation looks up the most-recent snapshot ≤ `D` and filters reconstructed `TraderLedger`s to that wallet set before the ranker scores them. The filter is wallet-level: a wallet absent from the snapshot that week does not contribute to any score because we wouldn't have known about it then. When the table is empty the simulation falls back to "all wallets in trade history" with a single warning at start (legacy behavior; survivorship-biased).

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
| `backtest_max_signal_price_default` | `Some(0.85)` | Upper-bound cap on slippage-adjusted `fill_price` for BUY copies (issue #142). `None` disables the cap. `Some(cap)` skips any BUY where `fill_price >= cap` — comparison is `>=` (not `>`), so a fill at exactly `cap` is suppressed; strictly conservative. Gates on `fill_price = signal_price × (1 + slippage_rate)` rather than the leader's signal price so a 0.849 + 1% slippage = 0.857 cannot squeak past. High-price contracts have catastrophic payoff geometry (100 bps slippage on a $0.99 contract burns nearly all upside; binary $0/$1 payoff means any miss is total loss). A3 analysis showed +$20.60 oracle lift in-sample at this threshold. Set via `PE_BACKTEST_MAX_SIGNAL_PRICE`. |
| `backtest_max_trade_count_default` | `25_000_000` | Pre-flight guard against loading the full production cache (~269M trades) into RAM. `pe-backtest` counts trades before calling `all_trades()` and refuses with a clear error if the cache exceeds this limit, directing users to `pe-skill-select` for full-cohort work. `0` disables the guard. Set via `PE_BACKTEST_MAX_TRADE_COUNT` (issue #241). |

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
| `skill_export_watchlist_input_path` | `""` (empty) | Path to the `.txt` watchlist consumed by `pe-skill-select export-watchlist`. One `0x`-prefixed hex address per line; lines starting with `#` and blank lines are skipped. Set via `PE_SKILL_EXPORT_WATCHLIST_INPUT_PATH`. An empty default causes an `Io` error on read (safe sentinel — the subcommand is explicit-invocation-only). |
| `skill_export_watchlist_output_path` | `""` (empty) | Output path for the `pe_trader_index::Watchlist` JSON file produced by `pe-skill-select export-watchlist`. Written atomically (tmp + rename, matching bootstrap `watchlist_phase.rs:202` precedent). Set via `PE_SKILL_EXPORT_WATCHLIST_OUTPUT_PATH`. |
| **export-watchlist output schema** | `pe_trader_index::Watchlist` JSON | Schema written by `pe-skill-select export-watchlist`. Top-level fields: `entries` (array of `WatchlistEntry`), `snapshot_at` (RFC3339, set to `cutoff_unix`), `active_count` (= `entries.len()`), `incubator_count` (= 0). Each `WatchlistEntry`: `wallet` (20-byte hex), `tier` (`Active`), `leader_score_bps` (= `lcb_5pct_bps` from `wallet_features`), `lcb_5pct_bps` (same), `win_rate_bps`, `closed_trades_in_window` (= all-time `closed_trades` — closest available proxy for the eligibility-window count), `reconstruction_quality`. Entries are sorted descending by `leader_score_bps`. Consumed by `pe-service` via `seed_watchlist_path` config key. |
| **`first_mover_percentile_bps`** | `median over the wallet's (market_id, outcome_id) positions of: 10_000 if singleton, else 10_000 − round(count_ahead / (total − 1) × 10_000)`, clamped `[0, 10_000]` | First-mover signal (#248 §3). For each buy-side position by the wallet (`side='buy'` AND `timestamp_unix ≤ cutoff_unix`), the wallet's first-buy timestamp is ranked against every other wallet's first-buy timestamp on the same `(market_id, outcome_id)`. The cross-wallet rank index is built once per `run_extract` invocation via `WalletCache::load_first_mover_rank_index(cutoff_unix)` (a single `GROUP BY market_id, outcome_id, wallet_hex` SQL scan covered by `idx_trades_buy_market_outcome_wallet_ts`) and `Arc`-shared across rayon workers. **Direction**: high bps = first-mover = good (the raw count_ahead/total ratio is inverted so the `0` sentinel stays on the bad-direction side, matching `hold_to_resolution_rate_bps` / `longshot_bias_ratio_bps` precedent). **Tie-breaking**: `partition_point` uses strict `<`, so wallets sharing a `first_buy_ts` all get the same `count_ahead`. **Singleton fallback**: a group with `total == 1` returns `10_000` (sole-participant convention). **Median**: mean-of-middle-two for even-sized position sets; middle element for odd. **Sentinel**: `0` when the wallet has no qualifying buys. SQLite column: `INTEGER NOT NULL DEFAULT 0` (migration-safe). Consumer: `scripts/monthly_rerank_gbm.py` `FEATURE_COLS` + SELECT; composite-ranker integration deferred per tracker #248 GBM-only consumer strategy. |
| **Candidate-features resolution-coverage bias** | resolved-markets-only for group A/D | The per-bet quality features (`ev_mean_bps`, `ev_tstat_bps`, `bb_shrunk_edge_bps`, `kelly_log_growth_bps`, `brier_score_bps`, `brier_resolution_bps`) and the capital-velocity feature (`median_first_entry_to_resolution_secs`) are computed over **closed trades whose market has a resolution row** — unresolved-market trades are excluded from those statistics (mirroring `forward.rs`). Sample-size fields (`closed_trades`, `distinct_markets`, `distinct_events`) and concentration (group F) are unchanged. This biases the per-bet estimates toward markets that have actually settled, which is the right empirical object for "did this wallet earn edge on resolved bets" but slightly under-represents long-tail unsettled positions. |
| **Forward-test fee posture** | gross-of-fees (v1) | The forward test reports PnL **gross of taker fees** (`ForwardReport::gross_of_fees == true`). The April-2026 holdout is entirely post-fee (Polymarket fees since 2026-03-30, Akey et al. SSRN 6443103); the headline overstates net edge by ≈ the taker fee. Netting awaits a per-market `takerBaseFee` backfill (not yet in the cache). |
| **`gbm_walkforward_min_mean_of_mean_edge`** | +0.01 (flat-$1 per position) | Minimum acceptable `mean_of_mean_edge` (arithmetic mean of per-anchor mean edges, gross of fees) across all anchors in a `gbm_walkforward.py` run. Derived from Phase 3 empirical results (2026-05-27): fwd=7d achieved +$0.066, fwd=14d +$0.085, fwd=30d +$0.066, all well above this floor. A run producing a mean-of-mean below +$0.01 should be treated as producing no statistically meaningful alpha until root-cause is understood (regime change vs. model degradation). |
| **`gbm_walkforward_min_anchor_positive_fraction`** | 0.60 | Minimum fraction of evaluated anchors with positive `mean_edge` in a `gbm_walkforward.py` run. Derived empirically: fwd=7d achieved 5/5 (100%), fwd=14d 4/5 (80%), fwd=30d 3/4 (75%). A single negative anchor in a 4–5 anchor run (i.e. 75–80% positive) is consistent with a transient market-regime effect and does not disqualify the strategy — Phase 3 confirmed the March 2026 failure is regime (fewer binary longshot opportunities), not wallet selection. Two or more consecutive negative anchors would require postmortem before proceeding. |
| **`gbm_walkforward_min_cohort_jaccard`** | 0.40 | Alert threshold for GBM `intersection_3` cohort Jaccard between consecutive monthly cutoffs (`|A∩B| / |A∪B|`). Phase 3 observed minimum: 0.494 (2026-03-31 → 2026-05-01). Below 0.40, cohort instability indicates the intersection is selecting near-randomly and the ensemble is unlikely to generalise. Monitor monthly after each re-extract + re-rank. BHq pool Jaccard (a looser signal) should stay above 0.60; empirical range 0.624–0.825. |
| **`gbm_walkforward_production_fwd_days`** | 7 | Preferred forward-evaluation window for the most robust walk-forward results. Phase 3 (2026-05-27, 5 anchors): fwd=7d is the only window with 0 negative anchors (March near-flat at +$0.003 vs negative at 14d/30d); std of mean_edge is tightest (0.064). fwd=14d has higher mean_edge (+$0.085 vs +$0.066) but March is worst there (−$0.031). fwd=30d has the largest total PnL but 4 anchors only (March −$0.019). Use fwd=14d for higher-alpha-potential runs with awareness of one historically bad month; use fwd=7d as the conservative validation gate. |
| **`gbm_walkforward_default_n_seeds`** | 5 | Number of GBM seeds for multi-seed ensemble in `gbm_walkforward.py` (`--n-seeds`). Seeds [42, 43, 44, 45, 46] are averaged before intersection. At n_seeds=5, PBO verdict uses seeds as the trial axis (n_trials=5) and anchors as the window axis. PBO=1.0 is expected at this small scale (C(4,2)=6 or C(5,2)=10 perms, n_trials=5) and does not disqualify results when we average all seeds rather than selecting the best — the multi-seed average mitigates the single-best-seed overfitting that PBO measures. |

### Portfolio constructor defaults (`scripts/portfolio_constructor/`)

Stage-2 portfolio construction (issue #276). See `docs/25-PORTFOLIO-CONSTRUCTOR.md`.

| Key | Default | Meaning |
|---|---:|---|
| `portfolio_max_n` | 50 | Maximum wallets in the greedy-selected portfolio (`--max-n`). |
| `portfolio_overlap_lambda` | 1.0 | Overlap penalty weight λ in the greedy objective `score × (1 − λ·overlap)`. λ=0 degenerates to pure top-N by GBM score; λ=1 (default) balances edge and diversity. |
| `portfolio_min_edge_score` | 0.0 | Greedy objective threshold: stop adding wallets when the best remaining objective ≤ this value. Default 0.0 stops at zero or negative objective. |
| `portfolio_lookback_days` | 90 | Trailing window (days) for the Jaccard market-overlap computation and ex-ante Kelly estimation (`--lookback-days`). |
| `portfolio_sizing_kelly_fraction` | 0.25 | Multiplier on the ex-ante Kelly fraction: applied_f = portfolio_sizing_kelly_fraction × exante_kelly_fraction(prior_returns). Research default; **distinct from** the Winner-Follow Kelly fractions in `docs/19-` (do not confuse). |
| `portfolio_sizing_min_position_usd` | 5.0 | Minimum position size in USD; bets whose stake < this floor are skipped (`--min-position`). |
| `portfolio_sizing_bankroll_usd` | 1000.0 | Starting bankroll for the sizing simulation (`--starting-capital`). |
| `portfolio_pbo_min_anchors` | 4 | Minimum anchor count for a committed PBO verdict (reuses the `gbm_walkforward.py` rule: `pbo<=0.5 AND n_seeds>=2 AND n_anchors>=4`; below this threshold, `verdict="undefined"`). **Intentionally the same value as `portfolio_min_credible_anchors`** at launch; they are distinct concepts and may diverge. |
| `portfolio_min_credible_anchors` | 4 | Minimum eligible anchors for the deploy set to be considered credible. When `n_eligible_anchors < 4`, the aggregate is flagged `credible=false` and the deploy set is labelled "insufficient evidence". **Intentionally the same value as `portfolio_pbo_min_anchors`** at launch; they gate different things (PBO verdict vs. overall credibility). |

### BTC shadow harness defaults (`pe-crypto-shadow`)

Measurement-only shadow harness for BTC up/down latency-arb (issue #297, Strategy 1+). Places no orders. See `crates/crypto-shadow/`.

| Key | Default | Meaning |
|---|---:|---|
| `crypto_fees_v2_rate` | 0.07 | Polymarket Crypto-category taker fee rate (`feeSchedule.rate`). Per-share taker fee = `rate · p · (1−p)` (exponent=1), **verified 2026-06-08** against docs.polymarket.com/trading/fees — matches the published table to the cent (100 sh @ p=0.50 → $1.75). The maker-rebates page's 0.072 is pooled/stale; the per-market `feeSchedule` value governs. Taker/entry-buy only; sell-side fee is ambiguous between two official Polymarket sources. Single source of truth: `crates/crypto-shadow/src/fees.rs`. |
| `crypto_shadow_channel_capacity` | 8192 | Bounded `mpsc` capacity for WS frames → join loop. Backpressure: drop-newest on full (dropped frames are **counted** per source — bounded periodic summary + `frames_dropped_*` `meta` stamps at run end; any non-zero value invalidates the tape). Raised 4096→8192 with the issue #311 batched flush: the consumer now drains far faster than it writes, and the deeper buffer absorbs a full flush window of bursty book+trade activity around a BTC move. |
| `crypto_shadow_clob_subscribe_channel_capacity` | 16 | Bounded `mpsc` capacity for CLOB subscription-set commands (`drive`'s refresh arm → CLOB task). One slot per `SubCmd` (`Add`/`Prune`/`ForceReconnect`). An undelivered `Add`/`Prune` self-retries on the next refresh tick via delivery gating in the runner (issue #311); a reconnect re-subscribes the current (pruned) set, never a cumulative one. |
| `crypto_shadow_flush_interval_ms` | 250 | Frame-buffer flush cadence in `drive` (issue #311): buffered raw frames + trade prints are batched every this-many ms, or as soon as `crypto_shadow_flush_max_frames` are buffered, whichever first (plus once at loop exit), and the batch is **enqueued to the decoupled writer** (`crypto_shadow_write_channel_capacity`, issue #317), not committed inline — so `drive` never blocks on SQLite. Replaces the per-frame auto-commit (~450–1800 ms/s of blocking at ~900 frames/s — the v2 saturation cause). Crash-loss ≤ `crypto_shadow_write_channel_capacity` in-flight batches + one flush window (steady-state the channel stays shallow as the writer outpaces the ~4-batch/s producer rate, and a crashed capture is not-clean regardless). |
| `crypto_shadow_flush_max_frames` | 256 | Buffered-frame count that triggers an immediate flush ahead of the interval — the size trigger carries the load under burst; the interval bounds staleness when quiet. |
| `crypto_shadow_write_channel_capacity` | 64 | Bounded `mpsc` capacity for the decoupled DB-writer channel (issue #317): `drive` sends batched `WriteCmd`s to a dedicated `spawn_blocking` writer task that owns all hot-path SQLite inserts, so the socket-drain task is pure CPU and never blocks on a `rusqlite` transaction (the post-#311 saturation root cause — head-of-line blocking on the single drain task). Backpressure is `send().await` (never a silent drop inside the pipeline); a writer `DbError` is fail-fast and stops the run. A shallow buffer suffices because the writer (~tens of k inserts/s) far outpaces the ~4-batch/s producer rate. |
| `crypto_shadow_prune_grace_ms` | 120000 | Grace after a market's `range_end_ms` before it is (a) pruned from the join state + CLOB subscription set and (b) refused by the admission guard at startup/refresh. One shared predicate (`join::market_expired`) backs both, so Gamma's `closed=false` lag cannot re-admit a pruned market. The grace keeps late settlement prints series-attributable. |
| `crypto_shadow_tape_validity_max_gap_secs` | 300 | Tape-validity PASS gate (issue #317): the largest gap between consecutive CLOB frames over a run (`drive` tracks it; the loop-exit tail `now − last` is folded in so a feed that dies near run-end is caught) must be **under** this, stamped to `meta` as `max_clob_gap_secs`. One of three hard-zero gate conditions alongside `frames_dropped_* = 0` and 5m-market `clob_trades` staleness < 300s. |
| `crypto_shadow_force_reconnect_after_ms` | 900000 | Minimum elapsed time since the last CLOB (re)connect before the task honors a `ForceReconnect` (sent by the refresh arm after ≥1 delivered prune). Natural reconnects (observed ~4 min cadence) apply the pruned set for free in the common case; the force fires only when reconnects stall. |
| `crypto_shadow_clob_ping_interval_secs` | 10 | CLOB market-channel application-level heartbeat cadence (issue #317): the client sends the text `PING` every this-many seconds; the server replies the text `PONG` (filtered in the read arm, never persisted). The venue-documented anti-reaping contract (`docs/15-SOURCES.md`, wss-overview) — missing heartbeats are the documented cause of connection drops. Protocol-level server pings are separately auto-ponged by tungstenite; `ws.rs` (exchange/chainlink) is untouched. |
| `crypto_shadow_clob_read_idle_limit_secs` | 120 | CLOB read-idle reconnect deadline (issue #317): if no inbound frame arrives for this long the socket is treated as half-open and the task reconnects + resubscribes. The actual half-open-peer detector (a heartbeat *send* into a vanished-but-not-RST socket succeeds for ~minutes); 12× the heartbeat cadence, so a healthy connection never trips it while bounding `max_clob_gap_secs` under the 300s gate. |
| `crypto_shadow_market_refresh_interval_secs` | 60 | Period for re-enumerating open BTC markets from Gamma during a `run`. |
| `crypto_shadow_max_open_markets` | 64 | Cap on simultaneously tracked markets per run. |
| `crypto_shadow_ws_max_backoff_secs` | 60 | Exponential-backoff cap for WS reconnect. |
| `crypto_shadow_rtt_probe_pings` | 5 | TCP-connect samples per endpoint for the startup vantage RTT probe; p50 stamped into `meta` so the measured edge carries the location it was taken from. |
| `crypto_shadow_gamma_base_url` | `https://gamma-api.polymarket.com` | Gamma API root for market enumeration. |
| `crypto_shadow_chainlink_ws_url` | `wss://ws-live-data.polymarket.com` | RTDS feed root for the Chainlink **settlement** value (`btc/usd`). Subscribe topic is `crypto_prices` + `type:update` + symbol under stringified `filters` (issue #300 fix 3); the live `btc/usd` source needs a sponsored Chainlink key (`crypto_shadow_chainlink_api_key`, deferred — AC2.3). Captured to `raw_ticks`; does not drive observations. |
| `crypto_shadow_clob_ws_url` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | CLOB market book WS. |
| `crypto_shadow_bybit_ws_url` | `wss://stream.bybit.com/v5/public/spot` | Bybit public spot trade stream (`publicTrade.BTCUSDT`) — a consensus trigger feed. |
| `crypto_shadow_okx_ws_url` | `wss://ws.okx.com:8443/ws/v5/public` | OKX public trades stream (`trades`/`BTC-USDT`) — a consensus trigger feed. |
| `crypto_shadow_coinbase_ws_url` | `wss://ws-feed.exchange.coinbase.com` | Coinbase ticker stream (`ticker`/`BTC-USD`, USD-quoted) — a consensus trigger feed. |
| `crypto_shadow_move_threshold_bps` | 3.0 | Consensus-median move size (basis points) that triggers an observation. From the feed bake-off (`docs/27`): outsized moves were defined as ≥3 bps. |
| `crypto_shadow_move_window_ms` | 300 | Look-back window (ms) over which the move is measured (reference = median ≈this long ago). |
| `crypto_shadow_move_cooldown_ms` | 1000 | Minimum gap (ms) between two move fires (debounce), so a single move emits once until it sustains past the cooldown. |
| `crypto_shadow_min_venues` | 2 | Minimum exchanges with a price before the consensus median is computed (no median ⇒ no move ⇒ no observation). |
| `crypto_shadow_chainlink_api_key` | _(unset)_ | Sponsored Chainlink Data Streams key for the `btc/usd` settlement feed (Polymarket onboarding: pm-ds-request.streams.chain.link). **Deferred** (issue #300 AC2.3): off by default; without it the Chainlink leg yields no live settlement value. |
| `crypto_shadow_sweep_grid_axes` | 480 cells | #310 offline `sweep` grid: `threshold_bps ∈ {1..10}` × `window_ms ∈ {50,100,150,200,300,500,750,1000}` × `cooldown_ms ∈ {500,1000,2000}` × `top_n_venues ∈ {2,3}`. Every cell runs `min_venues` = `crypto_shadow_min_venues` (cross-reference — no second numeric copy; with `top_n = 2` the median then requires both retained venues). The 50 ms window is near-degenerate (~40 ms aggregate inter-tick floor) and is flagged in the output, never silently excluded. Axes canonical in `crates/crypto-shadow/src/sweep/grid.rs`. |
| `crypto_shadow_sweep_venue_priority` | coinbase > okx > bybit | Fixed top-N venue ranking for the sweep (USD-proximity + freshness: coinbase is the USD-quoted venue; bybit/okx carry a ~5 bps USDT basis; ordering per the `docs/27` bake-off). No config surface in v1. `crates/crypto-shadow/src/sweep/ranking.rs`. |
| `book_index_per_token_cap` | 1000000 | Per-token cap on the #310 sweep's **per-frame** `BookIndex` entries — one entry per decoded `BookUpdate`, sides stored verbatim (`None` included, never filled from a previous entry), mirroring live book-receipt recency so `feed_to_book_lag_ms` reproduces exactly (live `on_book_update` re-stamps `received_ms` on every frame, including unchanged top-of-book `price_change`s). On overflow the token stops indexing, the drop count lands in `DecodeStats`, and lookups past the cap horizon return `None` → legs unscored, never silently wrong. |
| `crypto_shadow_scalp_horizons_s` | [10, 30, 60, 120] | #310 sweep scalp exit horizons (seconds): buy the direction-side ask at fire, exit at **that same token's** best bid at `fire+H` on the node receive clock. Unscored when `fire+H > range_end_ms`, the direction-side entry ask is absent, or no exit book/bid exists at `fire+H`. Canonical in code: `crates/crypto-shadow/src/sweep/scorers.rs`. |
| `crypto_shadow_scalp_exit_fee_model` | conservative-if-charged | Scalp exit-leg fee = `taker_fee(exit_bid)`, charged **at the exit price**: the official sell-side taker fee is ambiguous between two Polymarket sources (see `crypto_fees_v2_rate` / `fees.rs` provenance), so charging it is the conservative branch — the assumption is stamped into the sweep output (`SCALP_FEE_PROVENANCE`). |
| `maker_rebate_rate` | 0.20 | Crypto-category maker rebate rate. Source (in governing order): **(1)** the per-market Gamma `feeSchedule.rebateRate` — verified live 2026-06-09 on a `btc-up-or-down-5m` market: `feeSchedule = {"exponent": 1, "rate": 0.07, "takerOnly": true, "rebateRate": 0.2}` (`feeType = crypto_fees_v2`); the per-market value governs over the maker-rebates docs page per the `crypto_fees_v2_rate` 0.072 precedent; **(2)** the Polymarket maker-rebates docs page for the **pool structure** — rebates are a daily pro-rata, liquidity-weighted pool of collected fees, not a per-fill credit. The #310 sweep's `maker_rebate_per_share = 0.20 · taker_fee_per_share(fill)` is therefore an **upper-bound idealization** (read the MM column as a ceiling). Single source of truth: `crates/crypto-shadow/src/fees.rs`. |
| `mm_fill_window_ms` | 1000 | #310 sweep MM fill window after a fire (node clock): the resting maker buy at the direction-side pre-move best bid fills iff a `taker_is_buy = false` print crosses at `price ≤ bid` within this window. **Deliberately decoupled from the cell's `window_ms`** so the MM column is comparable across cells. Front-of-queue assumed (v1). |
