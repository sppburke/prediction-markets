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
| `clob_book_request_timeout_secs` | 5 | Fixed `crates/service/src/clob_book.rs` constant `CLOB_REQUEST_TIMEOUT_SECS` (no env override): per-request timeout for the public CLOB `/book` liquidity-capture fetch (issue #350 WS2). Deliberately shorter than `polymarket_request_timeout_secs` (10) — the fetch runs off the fill hot path, so a slow book degrades to a partial snapshot rather than blocking a trade. Reuses the existing `polymarket_clob_min_interval_ms` (200) gate. |
| `absorbable_depth_bps` | 100 | Fixed `crates/service/src/snapshot_worker.rs` constant `ABSORBABLE_DEPTH_BPS` (no env override): ask-depth band for `absorbable_usd_100bps` (issue #350 WS2 PR-H). Σ price·size over ask levels priced within this many basis points of the best ask. 100 bps = 1 %; baked into the column name `absorbable_usd_100bps`, so it is a constant rather than an operator knob. |
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

The local latency-shift ranker pushes append-only ranking batches to Supabase (`scripts/push_ranking_to_supabase.py`); `pe-service` reads the `latest_ranking` view on an interval and refreshes the scores of a fixed maintained live working set of `MAINTAINED_SET_SIZE` wallets (`crate::live_watchlist::LiveWatchlist`, an `ArcSwap`) — the refresh is score-update-only (no add/evict); the maintenance tick is the sole evictor/backfiller (issue #350 WS1). Supabase is the sole wallet source (issue #370): there is no leaderboard/seed fallback, so the service hard-fails at boot if `latest_ranking` is empty or unreachable. The Supabase keys follow the secret precedent (plain `String`, empty default, never logged). After each refresh (and once at bootstrap) the service best-effort publishes its current live-set size to the `service_runtime` table so the analytics site can show "N watched" — the count is in-memory only and the site cannot derive it from `latest_ranking` (it does not know `SUPABASE_FETCH_LIMIT`). The publish needs `supabase_secret_key`; it is skipped when absent and never blocks the refresh.

| Key | Default | Meaning |
|---|---:|---|
| `supabase_url` | `""` | `ServiceConfig` field. Supabase project REST base URL (e.g. `https://<ref>.supabase.co`); the **sole** wallet source (#370). If it resolves empty or unreachable the service hard-fails at boot (no fallback). Set via `PE_SUPABASE_URL`. |
| `supabase_secret_key` | `""` | `ServiceConfig` field (secret). The service-role key, sent in **both** the `apikey` and `Authorization: Bearer` headers — Supabase's `sb_` keys are not JWTs, so PostgREST 401s (`PGRST301`) if the two headers differ. Bypasses RLS for the server-side read. `PE_SUPABASE_SECRET_KEY` from `.env`. |
| `supabase_anon_key` | `""` | `ServiceConfig` field (secret). Publishable/anon fallback used for both headers **only when `supabase_secret_key` is empty**; ignored otherwise. `PE_SUPABASE_ANON_KEY` from `.env`. |
| `supabase_refresh_interval_secs` | 300 | `ServiceConfig` field. Seconds between live-watchlist refresh polls. The refresh loop is spawned only when `supabase_url` is non-empty and this is `> 0`. |
| `config_poll_interval_secs` | 60 | Seconds between `service_config` runtime-config polls (issue #398 WS1, `config_poller::CONFIG_POLL_INTERVAL_SECS`). Boot-frozen const (the poll cadence cannot govern itself); the loop is spawned only when `supabase_url` is non-empty. A config edit takes effect within this window with no restart. |
| `supabase_sink_enabled` | `false` | `ServiceConfig` field. Enables the best-effort paper-fill/settlement sink to Supabase (issue #343). The sink task is spawned only when this is `true` **and** `supabase_url` is non-empty. Requires `supabase_secret_key` — under RLS the anon key can only read, so anon-only writes 403. `PE_SUPABASE_SINK_ENABLED`. |
| `supabase_sink_channel_capacity` | 256 | `ServiceConfig` field. Bounded mpsc capacity for the trade-path → sink event channel. Backpressure: drop-on-full (the periodic reconcile re-derives dropped fills/settlements from `paper_state`, so a drop self-heals). `PE_SUPABASE_SINK_CHANNEL_CAPACITY`. |
| `supabase_sink_reconcile_interval_secs` | 300 | `ServiceConfig` field. Seconds between periodic sink reconciles: a contiguous-prefix fill HWM catch-up over `list_fills()` plus a full re-upsert of `list_settled_markets()`, healing any dropped or failed live writes. `PE_SUPABASE_SINK_RECONCILE_INTERVAL_SECS`. |
| `supabase_authoritative` | `false` | `ServiceConfig` field (issue #397). Makes Supabase the **authoritative system of record** for paper-state's money + book: `paper_bankroll` + `paper_positions` (new tables, anon-read) and the reused `paper_fills` + `settled_markets`. When `true`, a paper fill writes the `commit_fill` RPC first (fail-closed — on error the trade is skipped, the local event log holds the fill and replays on restart), then mirrors to SQLite; resolutions go through the `apply_resolution` RPC; boot runs a catch-up-then-pull; and `run_sink` is **not** spawned (the RPCs are the sole writer of `paper_fills`/`settled_markets`). `seen_trades`/`leader_positions`/`poll_cursors`/`meta` stay local-only. Both RPCs mutate `paper_bankroll` via a single self-referencing UPDATE so the concurrent fill and resolution tasks cannot lose an update. Requires the service-role `supabase_secret_key`. Off by default; set explicitly in `.env`. Schema + RPCs in `scripts/supabase_paper_state_schema.sql`; one-time `--backfill-supabase` before cutover. `PE_SUPABASE_AUTHORITATIVE`. |
| `snapshot_channel_capacity` | 256 | `ServiceConfig` field (issue #350 WS2 PR-H). Bounded mpsc capacity for the trade-path → liquidity-snapshot worker channel. Drop-on-full: a full channel drops the snapshot request so the BUY fill path never blocks (capture is best-effort analytics). The snapshot worker is spawned under the same gate as the Supabase sink (`supabase_sink_enabled` + non-empty `supabase_url`). `PE_SNAPSHOT_CHANNEL_CAPACITY`. |
| `SUPABASE_FETCH_LIMIT` | 25 | **Module const** in `crates/service/src/supabase_reader.rs`. Top-N wallets fetched per refresh (`?limit=`); equals `MAINTAINED_SET_SIZE` — the refresh fetches exactly the maintained set. The ranker pushes a deeper top-200 bench; only the live set is capped. |
| `MAINTAINED_SET_SIZE` | 25 | **Module const** in `supabase_reader.rs` (issue #350 WS1; replaces `SUPABASE_LIVE_CAP`=200). Fixed size of the maintained live working set. The periodic refresh is score-update-only (no add/evict); the maintenance tick (#350 WS1 PR-D) evicts inactive/underperforming wallets and backfills freed slots from the bench up to this width. The bench (`latest_ranking`) still holds the ranker's top-200 push; only the live set is capped. |
| `LS_TSTAT_BPS_SCALE` | 1_000 | **Module const** in `supabase_reader.rs`. t-stat → `leader_score_bps` scale (ordering only, not a gate). A t-stat of 2.5 maps to 2500 bps. |
| `upload_active_window_hours` | 72 | `--active-window-hours` in `scripts/push_ranking_to_supabase.py` (issue #350 WS3). The ranking push drops wallets with no cached trade (`wallet_cache.db`) in the last N hours so idle wallets never reach the live set. Off unless `--db` is given; `scripts/rank_and_push.sh` always passes it. Drift-guarded by `scripts/test_push_ranking_filter.py`. |
| `ACTIVE_WINDOW_HOURS` | 72 | **Module const** (`i64`) in `crates/service/src/supabase_reader.rs` (#357). Candidate-freshness filter: `fetch_candidates` only returns bench wallets whose `last_trade_unix ≥ now − 72 h`, mirroring `upload_active_window_hours` on the push side so the maintenance-tick backfill never admits a wallet the upload would have dropped. |
| `upload_max_cache_staleness_hours` | 24 | `--max-cache-staleness-hours` in `scripts/push_ranking_to_supabase.py` (issue #350 WS3). The push aborts (non-zero exit, no Supabase write) when the cache's global `MAX(timestamp_unix)` is older than N hours — a stale cache would spuriously filter out *every* wallet. Backfill before pushing (docs/26). Drift-guarded by `scripts/test_push_ranking_filter.py`. |
| `ranking_batches_retention` | 180 | `--keep-batches` in `scripts/push_ranking_to_supabase.py` (issue #411). After a successful push, prune `ranking_batches` to the newest N rows (CASCADE drops their `ranking_entries`), bounding the append-only epoch history (~6 months at the ~daily cron cadence; matches `ranker_window_days`). `latest_ranking` reads only `max(batch_id)`, so pruning older batches never affects the live read path or the `wallet_live_stats_mv` matview. `0` disables the prune (history-preserving research re-push). Best-effort: a prune failure logs HTTP status+body and does **not** fail the push (bounded + self-heals next run). Drift-guarded by `scripts/test_push_ranking_filter.py`. |
| `ranker_window_days` | 180 | `DEFAULT_WINDOW_DAYS` in `scripts/ranker_decay.py` (issue #366). Relative entry-date window: when `--win-end`/`--win-start` are omitted, the ranker (`scripts/rank_72hr_buyandhold.py`) scores `win_end = today UTC-midnight`, `win_start = win_end − this`. Replaces the old fixed calendar window (`2025-12-01 → 2026-06-01`); the relative default **does** take effect on the next production `scripts/rank_and_push.sh` (which leaves `WIN_START`/`WIN_END` empty). Override via `--win-start`/`--win-end` (Python) or `WIN_START`/`WIN_END` (shell). Drift-guarded by `scripts/test_ranker_decay.py`. |
| `ranker_half_life_days` | 30 | `DEFAULT_HALF_LIFE_DAYS` in `scripts/ranker_decay.py` (issue #366). Exponential recency-decay half-life (days) for the edge/t-stat score in BOTH ranking passes (`rank_72hr_buyandhold.py`, `latency_shift_rerank.py`): a trade one half-life old weighs 0.5. `0` disables decay (flat weights = legacy behaviour, bitwise-identical). Both the standalone Python scripts and the production wrapper `scripts/rank_and_push.sh` default to **30** (issue #370 adopted 30-day decay after the half-life sweep, flipping the #366 flat wrapper default). Override with `--half-life-days N`; for the first full-universe run, stage with `--half-life-days 0` so a cohort shift is attributable to the wider universe vs decay. Eligibility/activity gates and `hit_rate` stay raw. Drift-guarded by `scripts/test_ranker_decay.py`. |
| `ranker_universe_source` | all-trade-wallets | Production ranking universe (#370): `scripts/rank_and_push.sh` passes `--universe-from-trades`, so every wallet with trade history in `wallet_cache.db` is scored (have-data ⇒ in-universe) and the ranker's own eligibility filters decide the cohort — there is no curated pre-gate file. Override with `--universe <file>` for research. |
| `ranker_fdr_q` | 0.05 | Family significance / false-coverage-rate target for the #421 bake-off harness honesty layer (`docs/31-RANKER-BAKEOFF-METHODOLOGY.md`). The canonical strategy-level significance level — the `select_winner_or_nogo` winner gate and `winner_uncertainty` FCR `top_q` now read the named constant `RANKER_FDR_Q` (issue #436 A6); the Romano-Wolf StepM, Hansen-SPA, and AKM/MRSW inference in `scripts/ranker/oos_validation.py` still take it as their own method-level `size`/`alpha` default `0.05`. Bootstrap reps/seeds and CSCV group counts are method-internal parameters, **not** glossary'd (see `oos_validation.py` header). |
| `ranker_pbo_max` | 0.5 | Probability-of-Backtest-Overfitting ceiling in the #421 winner gate (`select_winner_or_nogo`, `scripts/ranker/bakeoff.py`): a config can only be declared WINNER when its CSCV `PBO < 0.5` (the IS-best config lands above the OOS median more often than not). Wired to the named constant `RANKER_PBO_MAX` (issue #436 A6 — it was glossary'd but the gate read a hardcoded `0.5`). A **degenerate** PBO (`NaN` from near-zero cross-config dispersion — #436 A7) fails the gate *safe* (`NaN < 0.5` is False → NO-GO). |
| `ranker_dsr_min` | 0.5 | Per-step Deflated-Sharpe survival gate (`apply_deflation_gate` threshold, `scripts/ranker/bakeoff.py`): within a trajectory step, candidate configs with deflated-Sharpe probability `< 0.5` are dropped before the policy acts. Wired to `RANKER_DSR_MIN` (issue #436 A6 — previously the hardcoded `threshold=0.5` default, never passed this value). Distinct from `ranker_pbo_max` (a leaderboard-level overfit ceiling) though both are 0.5. |
| `ranker_grid_dsr_min` | 0.95 | Leaderboard grid Deflated-Sharpe **advisory** floor (`RANKER_GRID_DSR_MIN`, `scripts/ranker/bakeoff.py`, issue #436 A6). The winning config's grid-DSR (`P(true SR > expected-max-of-N-nulls SR)` at the full pre-screen `N_GRID`) is surfaced in the decision (`winner_grid_dsr`) and flagged (`grid_dsr_advisory_low`) when below this bar, but is **NOT a hard gate**: the Romano-Wolf / Hansen-SPA / PBO panel already deflates the leaderboard, so gating the per-config grid-DSR on top double-counts the multiple-testing correction and would reject genuinely-good configs (a clean `+0.8`/period config deflates to grid-DSR ≈ 0.93 at `N=20`, below 0.95). `0.95` is the textbook Bailey-López-de-Prado deflated-significance level. Distinct from the per-step hard gate `ranker_dsr_min`. |
| `ranker_min_periods` | 24 | Minimum walk-forward periods (`as_of` cutoffs) the #421 winner gate requires before trusting the bootstrap panel (`RANKER_MIN_PERIODS`, `select_winner_or_nogo`, `scripts/ranker/bakeoff.py`, issue #436 B4). Below it the CSCV-PBO / Romano-Wolf / Hansen-SPA / Deflated-Sharpe gates degenerate to a **silent** always-NO-GO, so the verdict short-circuits to an **explicit** `insufficient periods: N < ranker_min_periods` NO-GO instead. `len(as_of_points)` equals the per-period return-matrix row count; `main`'s `--steps` default is raised to this and `BakeoffParams.min_periods` carries it (tests override to a low value for short synthetic trajectories). 24 ≈ two years of monthly cutoffs — the textbook minimum for stable CSCV/SPA inference; raising it pushes `as_of` further back where CLOB coverage thins (a #436 Phase E concern, surfaced not gated). |
| `ranker_eb_prior_var_floor` | 1e-9 | **Absolute** variance floor for the empirical-Bayes prior in the `eb_shrinkage_skill` estimator (`_EB_PRIOR_VAR_FLOOR`, `scripts/ranker/estimators.py`). The EB prior mean μ0 and variance τ² are **estimated from the cross-section** each scoring step (unlike the fixed `kelly_p_prior_*` Beta priors in `19-WINNER-FOLLOW-STRATEGY.md`). Since issue #436 C1, τ² is the **DerSimonian-Laird precision-weighted positive-part** estimate (not the old unweighted `Var(means) − mean(se²)`), floored to `ranker_eb_tau2_floor_frac · Var(means)`; this absolute `1e-9` is the deterministic fallback for a `<2`-finite-precision-candidate input (an undefined cross-section). |
| `ranker_eb_tau2_floor_frac` | 0.05 | Fraction-of-`Var(wallet means)` floor on the DerSimonian-Laird empirical-Bayes prior variance τ² in `eb_shrinkage_skill` (`_EB_TAU2_FLOOR_FRAC`, `scripts/ranker/estimators.py`, issue #436 C1). The precision-weighted (`1/se²`) positive-part MoM keeps short-track (`n ≤ 4`, huge `se²`) wallets from driving τ² negative, but its positive-part can still return ≈0 on a genuinely homogeneous cross-section; flooring τ² to `0.05 · Var(means)` keeps the shrinkage from fully collapsing (the old `max(prior_var, 1e-9)` sent `shrink → 0`, saturating every posterior onto μ0 and degenerating the ranking to insertion order). A **5% backstop**: it binds only when the DSL estimate is ≈0 — in normal operation the DSL value exceeds it and it never binds — and is small enough to preserve aggressive shrinkage. |
| `ranker_sd_floor` | 1e-9 | Per-position net-edge / CLV **dispersion** floor applied across all five bake-off estimators (`_SD_FLOOR`, `scripts/ranker/estimators.py`, issue #436 A10 follow-up): a wallet whose weighted `sd <= 1e-9` has undefined dispersion and is **dropped**, not ranked maximal. `np.std(ddof=1)` (via `weighted_stats`) of a mathematically-constant net series is exactly `0.0` at some position counts but `~1e-16` at others (mean-rounding in the computational-variance formula), so a bare `sd > 0` admitted a 6-position win streak with a t-stat `~2e16`; a genuine per-position dispersion is `O(1e-3)` or larger (≥ one price tick), so `1e-9` cleanly separates float noise from real signal. Distinct from `ranker_eb_prior_var_floor` (the EB *prior* variance floor) — both are `1e-9` but gate different quantities. |
| `ranker_min_trl` | 0 | Minimum track-record-length eligibility gate (`Criteria.min_trl`, `scripts/ranker/__init__.py`): a wallet is a follow-now candidate at cutoff `as_of` only with `≥ min_trl` in-sample positions. `0` = no gate (the honest permissive sentinel); the bake-off **sweeps** this in the criteria grid and the winning value feeds #417. |
| `ranker_embargo_secs` | 0 | Walk-forward embargo (`split_walkforward(embargo_secs=…)`, `scripts/ranker/oos_validation.py`): the forward track starts at `as_of + embargo_secs`, breaking leakage across the cutoff. `0` = no-op sentinel; swept in the grid like `ranker_min_trl`. The recency-decay anchor for the out-of-sample window is the existing `ranker_half_life_days` (#366). |
| `ranker_churn_cost_usd` | 0.75 | Per-admission copy entry cost (USD) for the #421 bake-off churn axis (`RANKER_CHURN_COST_USD`, `scripts/ranker/bakeoff.py`, issue #436 D3), **swept `{0.0, this}`** so the policy ranking's churn sensitivity is measured, not assumed. Anchored to a real entry cost: the copy-trader is a Polymarket **taker** on entry and **holds to resolution** (no exit leg), so per new $25 position the cost ≈ the taker fee `25·feeRate·(1−p̄) ≈ $0.625` at a blended ~`0.05` category rate and band-center `p̄≈0.5` (`fee = C·feeRate·p·(1−p)`; docs.polymarket.com/trading/fees, verified 2026-06-09), plus ~`$0.12` slippage ≈ `$0.75`. Charged on the **positive-part L1 weight movement** `Σ max(Δweightᵢ, 0)` (`_churn`): for the hard-set policies (weight 1.0) this equals the count of newly-admitted wallets; for `policy_online_weighting` it also charges a weight ramp-up. Operator-tunable (it is a sweep anchor, not a hard threshold). |
| `ranker_bakeoff_max_backtests` | 30_000 | Compute-budget ceiling on the #421 bake-off search surface (`RANKER_BAKEOFF_MAX_BACKTESTS`, `scripts/ranker/bakeoff.py`, issue #436 D2): `run_bakeoff` refuses (raises `ValueError`) a grid whose **`n_grid_full × steps`** — the naive injected-set backtest count — exceeds this, so "keep N bounded" is an enforced number, not a hope. The committed 12-level criteria sweep gives `n_grid_full = 5·2·4·12·2 = 960` configs × `ranker_min_periods`=24 steps = `23 040`, under the ceiling; the in-run `MemoizingBacktestRunner` collapses the **realized** count far below the naive bound (the churn axis 2× exactly, plus every repeated followed set). Per-run overridable via `BakeoffParams.max_backtests`. A conservative bound — screened-out estimators never run a trajectory, so real compute is lower still. |
| `clv_compare_mid_bias_max` | 0.05 | CLV source-comparison gate (issue #429 PR2, `scripts/ranker/clv_source_comparison.py` const `MID_BIAS_FLAG`): max acceptable median \|CLOB price − trades-last price\| on shared hourly buckets for the trades series to count as a sound CLV proxy. Above it (or no intersection to verify), trades is never recommended as a price-series source — the verdict falls to `clob_only`. Mirrors the issue's "flag if median abs diff > ~0.05". |
| `clv_compare_proxy_suffices_spearman` | 0.95 | CLV source-comparison gate (#429 PR2, const `PROXY_SUFFICES_SPEARMAN`): if the per-wallet `true_clv`-vs-`proxy_clv` rank Spearman is ≥ this, the CLOB series barely re-ranks vs the `proxy_clv` we already have → verdict `proxy_clv_suffices` (don't build the price-series pipeline). Sampling noise pushes the statistic toward 0, so it cannot spuriously trigger this gate. |
| `clv_compare_trades_dominates_pp` | 20 | CLV source-comparison gate (#429 PR2, const `TRADES_DOMINATES_PP`): trades-bucket coverage must exceed CLOB coverage by ≥ this many percentage points (AND be a sound proxy per `clv_compare_mid_bias_max`) to select `trades_only`. |
| `clv_compare_trades_fill_min_pp` | 5 | CLV source-comparison gate (#429 PR2, const `TRADES_FILL_MIN_PP`): min coverage (percentage points) the trades series must add beyond CLOB (AND be a sound proxy) to select `clob_primary_with_trades_fill`; below it the trades pass is dropped (`clob_only`). |
| `true_clv_coverage_warn_pct` | 30 | Warn floor (percent) for the `true_clv` estimator's *position-level* CLOB coverage, checked in `suff_stats.materialize` when the optional `market_price_history` + `token_conditions` views ARE registered (issue #429 PR4): the share of materialized positions carrying a non-NaN `true_clv_close`. `true_clv` is best-effort (PR2's ~63.6% market-level ceiling, less at position level), so 30 catches the degenerate/mis-wired case (≈0% — empty backfill or a broken join) without false-warning on the expected partial coverage. Distinct from the bootstrap-side `prices_history_coverage_warn_pct` (market-level, Rust). Const `TRUE_CLV_COVERAGE_WARN_PCT` in `scripts/ranker/suff_stats.py`. |
| `maintenance_interval_secs` | 600 | `ServiceConfig` field (issue #350 WS1 PR-D). Seconds between maintenance ticks (inactivity + underperformance knockout + atomic backfill). `0` disables the tick entirely (skipped, not a zero-duration loop). The task is spawned only when `supabase_url` is non-empty and this is `> 0`. `PE_MAINTENANCE_INTERVAL_SECS`. |
| `inactivity_threshold_secs` | 259_200 | `ServiceConfig` field (#350 WS1 PR-D). A live wallet idle (no observed trade) ≥ this many seconds is evicted, unless it is a proven winner (then spared up to `inactivity_hard_cap_secs`). 72 h. The clock is the wallet's **real last-trade time** (#357): the poll cursor is seeded from `ranking_entries.last_trade_unix` at admission/bootstrap and advanced forward-only by the poller, so `idle = now − cursor = now − real_last_trade` (no admission grace — a stale wallet is not reprieved by being freshly admitted). `PE_INACTIVITY_THRESHOLD_SECS`. |
| `inactivity_hard_cap_secs` | 604_800 | `ServiceConfig` field (#350 WS1 PR-D). Hard ceiling on sparing a proven winner from inactivity eviction: past this idle span the wallet is evicted unconditionally (a winner silent for a week is more likely abandoned than patient). 7 d. `PE_INACTIVITY_HARD_CAP_SECS`. |
| `bench_overfetch` | 10 | `ServiceConfig` field (#350 WS1 PR-D). Extra bench candidates fetched beyond the freed-slot count when backfilling, so a server-side casing/dedup miss still leaves enough rows to refill the set. `PE_BENCH_OVERFETCH`. |
| `demotion_min_trades` | 10 | `ServiceConfig` field (#350 WS1 PR-D). Minimum settled fills before either the underperformance demotion (`WalletEdgeStats::should_demote`) or the proven-winner inactivity exception (`is_proven_winner`) applies — no judgement on small samples. `PE_DEMOTION_MIN_TRADES`. |
| `demotion_cb_alpha` | "0.10" | `ServiceConfig` field (#350 WS1 PR-D), a decimal string parsed to `Decimal` at startup (never `f64`). Empirical-Bernstein confidence level α for the demotion upper-CB and the proven-winner lower-CB over per-share edge ∈ [-1, 1]. `PE_DEMOTION_CB_ALPHA`. |

### Wallet enumeration and relocated chain primitives

Wallet discovery is now the **all-category Polymarket leaderboard** sweep
(#335). The Dune client was deleted in #335; the on-chain `eth_getLogs`
enumeration path, the delta scan, and funder discovery were deleted with the
`pe-source-onchain-polygon` crate in #326 — see `docs/28-OPERATOR-GRAPH-ARCHIVE.md`.
The on-chain CTF market-resolution scan and its `eth_getLogs` bisect primitive
were removed in #369 when CLOB became the **sole** market-resolution source. The
few Polygon PoS contract constants the surviving migration/events paths still
need were relocated into `crates/bootstrap/src/chain.rs` (each keeps its
`verified <date> from <source>` comment and a self-validating keccak test):

| Symbol (`pe_bootstrap::chain::*`) | Purpose |
|---|---|
| `ALL_EXCHANGE_CONTRACTS`, `ALL_ORDER_FILLED_TOPICS`, `TOPIC_ORDER_FILLED_V1`, `TOPIC_ORDER_FILLED_V2` | Legacy enum-state synthesis in `migrate::auto_migrate_legacy`. |
| `normalise_condition_id` | `0x` condition-id canonicalisation shared by the Gamma `/events` sweep (relocated from `dune.rs` in #335). |

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

### Agent-friendly log layout

pe-service writes three bounded JSONL artifacts (so an agent reads one tiny file for health,
a focused file for problems, and a bounded stream for detail — never an unbounded firehose):

| Artifact | Shape | Use |
|---|---|---|
| `status.json` (`status_path`) | single file, atomically rewritten every `status_interval_secs` | **current health snapshot** — `updated_at, uptime_secs, mode, authoritative, bankroll, open_positions, fills_total, settled_total, last_event_seq, watchlist_size, supabase_rpc_calls`. Read this first; no grep. |
| `<stem>.<date>.jsonl` (from `jsonl_log_path`) | full stream, rotated **daily**, keeps `log_retention_days` | full detail; grep one day's file |
| `errors.<date>.jsonl` (same dir) | **WARN+ERROR only**, rotated daily | the clean "what broke" tape (no INFO chatter) |

| Config | Default | Description |
|---|---|---|
| `status_path` | `./status.json` | `ServiceConfig` field. Path of the health snapshot. `PE_STATUS_PATH`. |
| `status_interval_secs` | 30 | `ServiceConfig` field. Seconds between `status.json` writes; `0` disables. `PE_STATUS_INTERVAL_SECS`. |
| `log_retention_days` | 7 | `ServiceConfig` field. Dated JSONL files kept per sink (full + errors); bounds disk. `PE_LOG_RETENTION_DAYS`. |

### JSONL observability sidecar schema

Written to the rolling full-stream files derived from `jsonl_log_path` (default base:
`./paper.jsonl` → `paper.<date>.jsonl`). One JSON object per line.

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
| `watchlist_lookback_window_days` | 7 | Days of leaderboard history considered when selecting candidates |

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

- `flip_human_approved: bool` — must be set before `LeaderAction::Flip` becomes a copy-eligible action.
- `kelly_fraction_above_default_human_approved: bool` — must be set before any mode's `kelly_fraction` exceeds the table in `19-WINNER-FOLLOW-STRATEGY.md` (cap 0.50 absolute).

Both are read by `risk-engine` as part of its pure inputs. As of issue #398 (Decision #2) they are **admin-mutable at runtime** via the Supabase `service_config` table (the single-email-gated admin panel), default-deny, with each edit audit-logged in `service_config.updated_by`/`updated_at` and applied on the next ≤60s config poll. This reverses the prior "signed config change only" rule. `kelly_fraction_above_default_human_approved` is re-checked against the mode ceiling on every poll in `runtime_config::parse_config`, so an above-ceiling override without the flag is cleared rather than applied.

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
| `bootstrap_rebuild_resolutions` | `false` | One-shot retroactive correction (issue #149 follow-up): when `true`, stage 6 deletes every `market_resolutions` row tagged with an imprecise source (`'gamma'`, `'clob'`) before any fetcher runs; the CLOB stage then re-populates the `'clob'` rows via `INSERT OR IGNORE`. Retained `'polygon'` rows are **not** deleted — they keep their exact block-timestamp `resolved_at_unix`. **#369:** with the on-chain scan removed, CLOB is the only source that re-derives `'clob'` rows (with `end_date_iso`-approximate timestamps); there is no precise on-chain backfill. Idempotent — safe to set on every run. Set `PE_BOOTSTRAP_REBUILD_RESOLUTIONS=1` to enable. |
| `bootstrap_skip_trade_fetch` | `false` | When `true`, `pe-bootstrap` skips the Polymarket trade-fetch step entirely. Safe when the trade cache is already fully populated and only subsequent steps (resolutions, filters) need to run. Emits a warn-level log. Set `PE_BOOTSTRAP_SKIP_TRADE_FETCH=1` to enable. |
| `infra_probe_span_secs` | `3600` | Maximum span (newest − oldest, seconds) across the first 500 trades of a cold-start wallet for it to be classified as infrastructure (issue #197). 500 trades in < 1 h ⇒ > 8 trades/min ⇒ market-maker / treasury / arbitrage bot. Below threshold: wallet is flagged `is_infra = 1`, the probe page is discarded, and downstream consumers skip via the `active_tradeable_wallets` view. The same threshold drives the `pe-bootstrap classify-infra` retroactive sweep over already-cached trades. Override via `PE_BOOTSTRAP_INFRA_SPAN_SECS`; canonical const lives in `pe_bootstrap::infra_probe::DEFAULT_INFRA_SPAN_SECS`. |
| `bootstrap_write_snapshot` | `false` | When `true`, the main `pe-bootstrap` pipeline persists a `(snapshot_at_unix, wallet)` row-set to `leaderboard_snapshots` at run time, stamped with the current `snapshot_at`. Default `false` keeps ad-hoc bootstrap runs (resolutions watchdog retries, dev shells) from polluting the snapshot timeline with near-duplicate intra-day rows — only the official weekly refresh path should opt in. Set `PE_BOOTSTRAP_WRITE_SNAPSHOT=1` to enable. |
| `bootstrap_gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for the Polymarket Gamma API. Override via `PE_GAMMA_BASE_URL` (useful for testing against a stub). |
| `bootstrap_gamma_min_interval_ms` | 50 | Minimum milliseconds between Gamma API requests (20 req/s). Live-tested ceiling is ≥ 27 req/s; 50 ms keeps ~25% margin. Enforced globally by `ReqwestFetcher`'s shared mutex regardless of caller concurrency. |
| `bootstrap_gamma_concurrency` | 10 | Number of in-flight Gamma requests issued concurrently per fetch loop (`buffer_unordered`). With ~300 ms per-request RTT, ~6 in-flight saturates the 20 req/s rate limit; 10 leaves headroom for latency spikes. The global rate cap is still enforced by `bootstrap_gamma_min_interval_ms`. |
| `gamma_batch_size` | 50 | condition_ids per repeat-key Gamma `/markets?condition_ids=A&condition_ids=B…` request (issue #382). Tier-1 live probe (`scripts/probe_gamma_ua.py`, 2026-06-20) confirmed repeat-key batching works for **both** the plain (open) and `&closed=true` variants — `n=50/50` and `n=100/100` returned, demux-by-`conditionId` clean, no cross-market leak; comma-separated joining returns 0 (repeat-key is mandatory). Observed cap ≥ 100, but 50 is kept (≈1000 markets/s at the 20 req/s gate) unless a higher cap is separately adopted. |
| `gamma_batch_limit_param` | 500 | `&limit=` value appended to batched Gamma `/markets` requests (issue #382). Live-confirmed (2026-06-20) not to truncate a full batch — a 100-id request returned all 100. Matches the proven `scripts/backfill_end_dates.py:40`. |
| `gamma_browser_ua` | `Mozilla/5.0 (X11; Linux x86_64) prediction-edge/1.0` | Defensive non-bot User-Agent for Gamma/CLOB clients (issue #382). **The 403 gate is the literal `Python-urllib/*` default UA (an anti-bot blocklist), NOT "missing browser UA":** the 2026-06-20 probe showed Gamma and CLOB `&closed=true` return 200 for a headerless request (a bare `reqwest::Client`, = what the shipped Rust clients send), an empty UA, a product UA, and a browser UA — and 403 **only** for `Python-urllib/3.11`. So the deployed Rust clients do not 403; this UA is hardening against a future bot-flagged default, not a correctness fix. |
| `bootstrap_event_orphan_warn_pct` | 99 | `pe-bootstrap events` coverage gate (issue #206): warn if more than 99% of distinct traded markets are orphans (self-mapped). Gamma's /events covers only a curated subset of all condition IDs; a 90–99% orphan rate on a large historical cache is expected and correct. The format-break hard-fail (abort if `events_seen > 0` but `conditions_mapped == 0`) replaces the old percentage-based abort. Const `ORPHAN_WARN_PCT` in `pe_bootstrap::events`. |
| `market_fee_missing_default_bps` | 0 | Default fee in bps when Gamma sets `feesEnabled = false`, omits `feeSchedule`, or omits `feeSchedule.rate`. Zero is the safe sentinel: it never over-discounts PnL and treats pre-fee-era markets correctly. Used by `fees_for_market` / `rate_to_bps` in `pe_bootstrap::events`. |
| `market_fee_max_bps` | 10_000 | Upper clamp for `market_fees.taker_base_fee_bps` / `maker_base_fee_bps`. Polymarket's live `feeSchedule.rate` is `0.04` (= 400 bps); the 10 000 bps ceiling (100%) is a hard guard against malformed API responses, not a normal value. |
| `bootstrap_clob_base_url` | `https://clob.polymarket.com` | Base URL for the Polymarket CLOB API (`/markets?closed=true` paginated listing) — the **sole** market-resolution source (#369). Override via `PE_CLOB_BASE_URL` for testing against a stub. |
| `bootstrap_clob_concurrency` | 8 | Number of in-flight CLOB requests issued concurrently per fetch loop, mirroring the Gamma `buffer_unordered` pattern. Set via `PE_BOOTSTRAP_CLOB_CONCURRENCY`. Reused as the concurrency for the `prices-history` CLOB backfill. |
| `clob_prices_history_min_interval_ms` | 10 | Minimum milliseconds between CLOB `/prices-history` requests (≈100 req/s, under the documented 1000 req/10s limit), enforced on a **dedicated** `ReqwestFetcher` so it does not loosen the 50 ms Data-API (`/trades`,`/activity`) gate (issue #421 PR4). Canonical constant `CLOB_PRICES_HISTORY_MIN_INTERVAL_MS` in `pe-source-polymarket-public`; override via `PE_BOOTSTRAP_PRICES_HISTORY_MIN_INTERVAL_MS`. |
| `clob_prices_history_fidelity_minutes` | 60 | CLOB `/prices-history` series granularity in minutes — hourly, the coarse pre-resolution series the CLV bake-off needs (testing CLV at 1h/6h/24h before close, not the final tick) (issue #421 PR4). Canonical constant `CLOB_PRICES_HISTORY_FIDELITY_MINUTES` in `pe-source-polymarket-public`; override via `PE_BOOTSTRAP_PRICES_HISTORY_FIDELITY_MINUTES`. |
| `prices_history_window_secs` | 259_200 (72h) | Pre-resolution window fetched per market by the `prices-history` backfill: the CLOB series is pulled over `[close_ref − this, close_ref]`, where `close_ref = market_schedules.end_date_unix ?? market_resolutions.resolved_at_unix`. 72h covers the 1h/6h/24h CLV horizons with margin (issue #421 PR4). Override via `PE_BOOTSTRAP_PRICES_HISTORY_WINDOW_SECS`. |
| `prices_history_token_limit` | 0 (no limit) | Per-run cap on `(market, token)` targets fetched by `pe-bootstrap prices-history` (issue #421 PR4). `0` is unbounded; a positive value bounds one run's memory/time over the ~1.4M-market universe. The backfill is resumable (skips `(market, token)` pairs already in `market_price_history`), so a capped run is re-run to continue. Override via `PE_BOOTSTRAP_PRICES_HISTORY_TOKEN_LIMIT`. |
| `clob_token_coverage_warn_pct` | 90 | Warn floor (percent) for CLOB token→condition coverage of resolved-with-winner markets, checked after a `pe-bootstrap resolutions` run (issue #429). The CLOB closed-markets sweep maps `tokens[].token_id → condition_id` with a positional `outcome_index` for the full resolved universe; a full re-walk maps ~94%+ of winner markets, so coverage below this floor flags genuine token-map starvation (e.g. a pre-re-walk cache — run `pe-bootstrap resolutions --reset-clob-cursor`). `0` disables the warn. Const `DEFAULT_CLOB_TOKEN_COVERAGE_WARN_PCT` in `pe_bootstrap::config`; override via `PE_BOOTSTRAP_CLOB_TOKEN_COVERAGE_WARN_PCT`. |
| `token_conditions.outcome_index` | NULL until mapped | Schema (issue #429): 0-based positional outcome ordinal on the `token_conditions` map (`0`=YES, `1`=NO for binary; positional for multi-outcome). Written by both the CLOB closed-markets sweep (`tokens[]` array position) and the Gamma `events` sweep (`clobTokenIds` array position) — the same authoritative outcome→token order. Nullable: rows written before this migration stay NULL until a re-run; the `true_clv` join (#421/#418) skips NULL. The CLOB sweep cross-checks its `tokens[]` order against any non-NULL stored value and quarantines a market on divergence rather than overwriting it. |
| `prices_history_coverage_warn_pct` | 60 | Warn floor (percent) for *usable* CLOB price-series coverage of resolved-with-winner markets, checked after a `pe-bootstrap prices-history` run (issue #429 PR3). "Usable" = a market with ≥1 token carrying ≥`prices_history_min_series_points` points. 60 sits just under the ~63.6% usable-series ceiling PR2's `clv_source_comparison` memo measured, so a complete CLOB-only backfill clears it while a starved/partial run (e.g. a pre-re-walk token map) trips it. `0` disables the warn. Const `DEFAULT_PRICES_HISTORY_COVERAGE_WARN_PCT` in `pe_bootstrap::config`; override via `PE_BOOTSTRAP_PRICES_HISTORY_COVERAGE_WARN_PCT`. |
| `prices_history_min_series_points` | 3 | Minimum points in a `(market, token)` series for it to count as "usable" in the price-series coverage ledger (issue #429 PR3). Matches the ≥3-point bar PR2's `clv_source_comparison` memo used (its `MIN_SERIES_POINTS`), so the bootstrap ledger's `usable / total` is comparable to that memo's measured CLOB ceiling. Const `MIN_USABLE_SERIES_POINTS` in `pe_bootstrap::cache`. |
| `market_price_history.source` | `'clob'` | Schema (issue #429 PR3): series provenance on each `market_price_history` row. `'clob'` is the only writer today — the CLOB `/prices-history` backfill — since PR2's `clob_only` verdict dropped the planned trades pass. The column exists so a future trades-derived series is distinguishable and so the coverage ledger can attribute points; `INSERT OR IGNORE` on the `(market_id, token_id, t)` PK makes the write **write-once**, so a later source never overwrites a captured point. Nullable-free (`DEFAULT 'clob'` on the additive migration). |
| `clob_page_max_retries` | 5 | Walk-level retries on a transient / rate-limited CLOB `/markets?closed=true` page fetch before the closed-markets walk aborts (issue #429 follow-up). Sits *on top of* `ReqwestFetcher`'s internal fast retries (`polymarket_max_retries`); rides through *sustained* flakiness (e.g. a minute of `error decoding response body`) so one bad page does not abort the ~1,457-page re-walk. `Transient` and `RateLimited` share this budget (`RateLimited` waits `retry_after`, floored at 1s); `Fatal` (4xx) errors still abort immediately, and the error propagates once the budget is exhausted. Const `CLOB_PAGE_MAX_RETRIES` in `pe_bootstrap::clob`. |
| `clob_page_retry_base_ms` | 1_000 | Base backoff (ms) for the CLOB page retry; exponential (`base · 2^attempt`) capped at 30s. With `clob_page_max_retries=5` the inter-attempt backoff sums to ~31s (1+2+4+8+16) before giving up — total wall-time per page is higher because each attempt also spends `ReqwestFetcher`'s own retries/timeout; `RateLimited` instead waits the server's `retry_after` (floored at 1s). Const `CLOB_PAGE_RETRY_BASE_MS` in `pe_bootstrap::clob`. |
| `bootstrap_pile_activation_min_trades` | 100 | Minimum trade count (DB `trade_count` OR Dune `dune_closed_markets`) for a non-infra wallet to be activated in the pile (issue #166). Curation-list membership (Polymarket leaderboard / Radion / 502-gap / datadash) bypasses this gate. Hardcoded as `pe_bootstrap::pile::PILE_ACTIVATION_MIN_TRADES`; changing it requires re-migrating the pile. |
| `bootstrap_backfill_limit` | 0 (no limit) | Per-run cap on `pe-bootstrap backfill`. `0` processes every wallet whose `last_polymarket_fetch_at` is NULL or older than 1 day. Initial deployment runs with `0` to drain the bulk catch-up queue; steady-state daily timers may set a positive value if daily run time grows unmanageable. Set via `PE_BOOTSTRAP_BACKFILL_LIMIT`. |
| `bootstrap_backfill_staleness_secs` | 86_400 (1 day) | Per-wallet staleness window for `pe-bootstrap backfill` (issue #166). A wallet is eligible for re-fetch when `last_polymarket_fetch_at IS NULL OR < now - 86_400`. Hardcoded as `pe_bootstrap::pile::BACKFILL_STALENESS_SECS`; matches the daily systemd timer cadence. Reused by `pe-bootstrap purge` (#385) as the rule-B freshness gate. |
| `bootstrap_purge_enabled` | `false` | Arms the `pe-bootstrap purge` DELETE (issue #385). Default `false` keeps the stage report-only — a dry-run that deletes nothing, though the would-purge report still runs each cycle (it reuses the Step-0 backfill's refresh, never re-running `refresh_trade_counts`). Set `PE_BOOTSTRAP_PURGE_ENABLED=1` to arm; even when armed, `--dry-run` still only reports. |
| `bootstrap_purge_inactivity_secs` | 1_209_600 (14 days) | Rule-B (dead-weight) dormancy threshold (issue #385): an `is_active=1`, not-eligible wallet refreshed this run is deleted (no tombstone) when its newest trade is older than this. A zero-trade wallet (`MAX(timestamp_unix) IS NULL`) is never matched. `PE_BOOTSTRAP_PURGE_INACTIVITY_SECS` overrides. |
| `bootstrap_purge_loser_tstat_max` | -2.0 | Rule-A (proven-loser) net t-stat ceiling (issue #385): an eligible wallet is deleted **and tombstoned** when `tstat_net <= -2.0 AND mean_net < 0 AND n_eff >= bootstrap_purge_loser_neff_min`. The one `f64` bootstrap config (a t-stat is a statistic, outside the "no raw f64" money/price/probability rule). `PE_BOOTSTRAP_PURGE_LOSER_TSTAT_MAX` overrides. |
| `bootstrap_purge_loser_neff_min` | 20 | Rule-A minimum effective sample size (`n_eff`, Kish) for a proven-loser verdict (issue #385) — guards against tombstoning on a tiny sample. `PE_BOOTSTRAP_PURGE_LOSER_NEFF_MIN` overrides. |
| `bootstrap_purge_bulk_min_wallets` | 20_000 | Delete-set size (wallet count) at/above which an armed `pe-bootstrap purge` runs in **bulk mode** (issue #401): drop the two non-lookup `trades` indexes → delete → VACUUM → rebuild. Below it the armed purge runs **incremental** — indexes stay live, no VACUUM — so cheap daily purges (hundreds of wallets) plateau the file while a rare backlog clear stays fast. `PE_BOOTSTRAP_PURGE_BULK_MIN_WALLETS` overrides. |
| `tombstone_override_sources` | `SRC_LEADERBOARD \| SRC_RADION` (= 48) | The source bits whose re-discovery of a tombstoned wallet *lifts* the tombstone and re-admits it (issue #385): Polymarket leaderboard (16) + Radion (32) only. Datadash (128), 502-gap (64), trades (2), and wallet-set-json (1) leave the tombstone intact (`bits & 48 == 0`). Read from each `upsert_wallets_bulk` row's own `source_bits`; hardcoded as `pe_bootstrap::pile::TOMBSTONE_OVERRIDE_SOURCES`. |
| `bootstrap_leaderboard_request_interval_ms` | 500 | Minimum milliseconds between Polymarket leaderboard API requests during `pe-bootstrap winner-discovery` (issue #324). Applied per-fetch via `ReqwestFetcher::with_min_interval_ms`. Set via `PE_BOOTSTRAP_LEADERBOARD_REQUEST_INTERVAL_MS`. |
| `bootstrap_leaderboard_top_n` | 50 | Maximum wallets fetched per leaderboard slice during `pe-bootstrap winner-discovery` (#324; all-category in #335). The `/v1/leaderboard` API hard-caps `limit` at 50; larger values are silently truncated server-side (verified live 2026-06-14). Set via `PE_BOOTSTRAP_LEADERBOARD_TOP_N`. |
| `bootstrap_leaderboard_categories` | all 10 | Leaderboard categories swept by `winner-discovery` (#335). Default = `OVERALL, POLITICS, SPORTS, CRYPTO, CULTURE, MENTIONS, WEATHER, ECONOMICS, TECH, FINANCE`. Each is crossed with `{PNL,VOL} × {DAY,WEEK,MONTH,ALL}` (≤ 80 slices); a category the API rejects (4xx) is skipped with a `warn!`. Results dedupe before the pile upsert. Set via `PE_BOOTSTRAP_LEADERBOARD_CATEGORIES` (TOML array of category names). |
| `bootstrap_radion_api_url` | `https://api.radion.app` | Radion trader-analysis API base URL — third wallet-discovery source for `winner-discovery` (issue #373). **On by default**, but inert until `radion_api_key` is set (the API mandates an `X-API-Key`). Set to `""` (or null) to disable the source (the kill switch). Set via `PE_BOOTSTRAP_RADION_API_URL`. |
| `bootstrap_radion_request_interval_ms` | 500 | Minimum milliseconds between Radion REST API requests during `pe-bootstrap winner-discovery` (issues #324, #373). Applied per-fetch via the `ReqwestTraderFetcher` rate-limit gate. Set via `PE_BOOTSTRAP_RADION_REQUEST_INTERVAL_MS`. |
| `bootstrap_radion_max_requests_per_run` | 8 | Per-run cap on `traders/analysis` requests (issue #373) — the Free-tier budget gate. The sweep is resumable (the `nextCursor` is persisted in `source_cursor` under `radion_traders_analysis`, resuming deeper each run and resetting on exhaustion), so this bounds spend without losing breadth: 8 req/run × 30 runs/mo = 240/mo, under the Free **300/mo** account cap (also 50/hr); at the API's 10-wallets/request max that is ≤80 wallets/run (~2,400/mo on a daily cron). Raise on a higher Radion plan. Set via `PE_BOOTSTRAP_RADION_MAX_REQUESTS_PER_RUN`. |
| `bootstrap_datadash_api_url` | `https://api.datadash.xyz` | datadash.xyz cohort API base URL — third wallet-discovery source for `winner-discovery` (issue #365). **On by default.** Set to `""` (or null) to disable the source (the kill switch). Set via `PE_BOOTSTRAP_DATADASH_API_URL`. |
| `bootstrap_datadash_request_interval_ms` | 500 | Minimum milliseconds between datadash Connect-RPC requests during `pe-bootstrap winner-discovery` (issue #365). Applied per-fetch via the `ReqwestCohortFetcher` rate-limit gate. Set via `PE_BOOTSTRAP_DATADASH_REQUEST_INTERVAL_MS`. |
| `bootstrap_datadash_exclude_ids` | `["07NQHFRAGB6HV"]` | datadash cohort ids excluded from ingest, matched **exactly** (never substring; issue #365). Default drops the ~103k-wallet `Polymarket Twitter/X Linked Traders` cohort. Set via `PE_BOOTSTRAP_DATADASH_EXCLUDE_IDS` (TOML array of ids). |
| `bootstrap_datadash_exclude_titles` | `["Polymarket Twitter/X Linked Traders"]` | datadash cohort titles excluded from ingest, matched **exactly** (issue #365). Default drops `Polymarket Twitter/X Linked Traders` while keeping the distinct `Polymarket Twitter/X Linked with PnL >$100k` cohort. Set via `PE_BOOTSTRAP_DATADASH_EXCLUDE_TITLES` (TOML array of titles). |
| `bootstrap_datadash_max_cohort_wallets` | 10_000 | Magnitude cap: any datadash cohort whose advertised `numWallets` exceeds this is skipped with a `warn!` before its wallets are fetched (issue #365). Belt-and-braces safety net so a recreated/misnamed mega-cohort cannot flood the pile even if the id/title guards drift (largest legitimate cohort is currently 706). Set via `PE_BOOTSTRAP_DATADASH_MAX_COHORT_WALLETS`. |

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
| 7 | 0b10000000 | datadash.xyz cohorts (#365) |

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
 OR (source_bits & 128) != 0  -- in_datadash (#365)
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
| `sizing_mode_default` | `kelly` | Default for `WinnerFollowConfig.sizing_mode` (#398 WS2; replaced `flat_usd_per_trade`). `kelly` = fractional-Kelly sizing. `dollar` (`sizing_dollar_usd`) sizes each BUY as `max(1, floor(usd / current_price))` contracts — the former flat path, eliminating bankroll compounding; use when the Kelly `p` input is a per-leader constant with no per-trade signal (issue #161). `contract` (`sizing_contracts`) sizes exactly N contracts. The per-trade cap, risk gate, and price-impact book cap (`price_impact_cap_bps`) remain active in all modes. The live boot default is `dollar` / `sizing_dollar_usd = 25` (`smoke-test/service.toml`). KV layer: three flat `service_config` keys `sizing_mode` / `sizing_dollar_usd` / `sizing_contracts`. |
| `price_impact_cap_bps_default` | `0` | Default for the live price-impact gate (`RuntimeConfig.price_impact_cap_bps`, #398 WS2). `0` disables the gate (fail-open). When `> 0`, the orchestrator fetches the CLOB `/book` per BUY and `min`s the size to the contracts absorbable within this many bps of best ask (`snapshot_worker::absorbable_contracts_within_bps`); a `/book` error or missing token fails open (no cap), while a successful read with 0 absorbable yields `Some(0)` and skips the trade. Admin-mutable via `service_config`. |
| `clob_book_hot_path_timeout_secs` | `2` | Timeout for the orchestrator's hot-path CLOB `/book` fetch in the price-impact gate (#398 WS2; `orchestrator::CLOB_BOOK_HOT_PATH_TIMEOUT_SECS`). Tighter than the snapshot worker's 5 s per-request timeout so a slow book fails open without stalling the trade. |
| `backtest_suppression_warn_threshold_pct` | 30 | Single warn threshold shared by every per-quarter BUY-signal suppression diagnostic (`expiry_filter_suppression_pct`, `high_price_suppression_pct`, …). Logged as a warning when any quarter exceeds it. Hardcoded as `SUPPRESSION_WARN_THRESHOLD` in `crates/backtest/src/simulation.rs`. |
| `backtest_require_known_expiry_default` | `true` | Strict-mode flag for the `max_hours_to_expiry` filter. `true` (default after #137 Sub-PR 3, gated on PR #154 stage 6f raising trade-set schedule coverage to 99.64%) — when both schedule and resolution are absent for a market, the BUY signal fails closed (suppressed). `false` (rollback / legacy) — both-absent allows the trade through. The fallback chain (schedule → resolution → flag) was unified in PR #139; pre-#139 the NULL-schedule path short-circuited to allow regardless of resolution. Set via `PE_BACKTEST_REQUIRE_KNOWN_EXPIRY`. |
| `backtest_max_positions_per_market_default` | `Some(1)` | Cap on concurrent open positions per `market_id` (issue #138). `None` disables the cap entirely. `Some(n)` blocks new BUY signals on any market that already has ≥ `n` open positions across every leader and outcome — leader A on outcome 0 and leader B on outcome 1 of the same binary market count against the same slot. Slot reopens when positions close via SELL or resolution sweep. `NonZeroU32` rejects `0` at deserialize-time so `PE_BACKTEST_MAX_POSITIONS_PER_MARKET=0` is an explicit error. Set via `PE_BACKTEST_MAX_POSITIONS_PER_MARKET`. |
| `backtest_max_signal_price_default` | `Some(0.85)` | Upper-bound cap on slippage-adjusted `fill_price` for BUY copies (issue #142). `None` disables the cap. `Some(cap)` skips any BUY where `fill_price >= cap` — comparison is `>=` (not `>`), so a fill at exactly `cap` is suppressed; strictly conservative. Gates on `fill_price = signal_price × (1 + slippage_rate)` rather than the leader's signal price so a 0.849 + 1% slippage = 0.857 cannot squeak past. High-price contracts have catastrophic payoff geometry (100 bps slippage on a $0.99 contract burns nearly all upside; binary $0/$1 payoff means any miss is total loss). A3 analysis showed +$20.60 oracle lift in-sample at this threshold. Set via `PE_BACKTEST_MAX_SIGNAL_PRICE`. |
| `backtest_max_trade_count_default` | `25_000_000` | Pre-flight guard against loading the full production cache (~269M trades) into RAM. `pe-backtest` counts trades before calling `all_trades()` and refuses with a clear error if the cache exceeds this limit. `0` disables the guard. Set via `PE_BACKTEST_MAX_TRADE_COUNT` (issue #241). |

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
