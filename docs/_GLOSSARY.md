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

The **copy-latency kill switch** uses nearest-rank p95 values for the last two completed
prior UTC clock hours (see the canonical `copy_latency_kill_switch_ms` and
`copy_latency_release_ms` values in `19-WINNER-FOLLOW-STRATEGY.md`). Two consecutive available
values strictly above the engage threshold activate it. While active, a missing value or a value
above the release threshold holds it; the first available value at or below the release threshold
releases it. A missing hour breaks the engage pair while inactive.

Paper samples span the chosen source envelope's `received_at` to the synchronized
`FinancialFinal` envelope's `received_at`. Live samples span request start to every
transport-successful `OrderPosted` response's `received_at`, irrespective of HTTP status or later
classification; transport failures and chain-finality time are excluded. Samples belong to the
hour containing that endpoint. The paper owner and every live account derive the switch locally;
any active owner blocks strategy-wide new entries.

> **Two latency metrics, deliberately distinct (#530).** The budget above measures
> `gateway receive → venue ack` (the service's internal span). The ranker's latency
> shift (Δ, `LATENCY_SHIFT_SECS`) calibrates against a LONGER span: `leader trade
> timestamp → durable paper fill`, which additionally includes the venue's own feed
> delay (measured p50 0.80s / p95 1.32s on the activity websocket). Δ is set to that
> full span's measured p95, conservatively rounded (currently 2s), never below
> measurement; the +1-week re-check artifact reports the full denominator (admitted
> copies, fills, no-fill dispositions, missing spans, clock exclusions; NTP-checked;
> nearest-rank p95; rounded up to whole seconds).

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
| Polymarket Data API | 200 req/10s on `/activity`, 100 req/s general | ≤ 20 req/s sustained on `/activity` | page bursts DO return HTTP 429 with `Retry-After: 1` (observed 2026-09-02, #555); the reconciliation fetcher alone waits out `Retry-After` ≤ `reconciliation_rate_limit_retry_secs` (1 s) inside its retry budget |
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
pub struct SourceTradeId(pub String);        // v2: "g2:" + 64 lowercase BLAKE3 hex digits
pub enum SourceTradeIdentityVersion { TransactionHashV1, ReconciledGroupV2 }
pub struct EventSeq(pub u64);

// Identities
pub struct WalletAddress(pub [u8; 20]);
pub struct TraderId(pub WalletAddress);       // Polymarket; Kalshi traders use VenueAccountId
pub struct VenueAccountId(pub String);

// Quantities, prices, probabilities
pub struct ContractQty(pub u64);
pub struct Quantity(pub ContractQty);
pub struct ShareAmount(u64);                 // exact 6-dp shares, stored as integer atomics
pub struct CollateralAmount(u64);            // exact 6-dp collateral, stored as integer atomics
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

`SourceTradeId` is generation-aware (#544). Historical version-one frames retain their
transaction-hash identity. A version-two normalized activity group is keyed by
`g2:<lowercase BLAKE3>` over the canonical length-prefixed group components; its
`transaction_hash` remains separate audit evidence and never participates in deduplication or
causal ordering. Only the exact 67-byte lowercase `g2:` encoding is version two. `ShareAmount`
and `CollateralAmount` preserve venue decimals exactly to six places as checked `u64` atomics;
lossy fractional input, negative input, and overflow are rejected rather than rounded.

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
| `reconciliation_rate_limit_retry_secs` | 1 | **Module const** `RECONCILIATION_RATE_LIMIT_RETRY_SECS` in `service` (not a TOML/env key). HTTP 429 `Retry-After` at or below this is waited out inside `polymarket_max_retries` by the Data-API reconciliation fetcher only; every other fetcher returns 429 to its caller unretried |
| `trade_reconciliation_concurrency` | 2 | **Module const** `TRADE_RECONCILIATION_CONCURRENCY` in `service::trade_poller` (not a TOML/env key). Bounded in-flight poller operations: one urgent wallet reconciliation and one backstop/anchor operation, with at most one operation per wallet across both slots. |
| `polymarket_channel_capacity` | 256 | Bounded mpsc channel capacity between trade poller and orchestrator |
| `trade_poll_interval_secs` | 30 | Seconds from completion of a Polymarket backstop round to the next round. Trigger-driven urgent wallet reconciliation wakes without waiting for this cadence and runs alongside backstop/anchor work under `trade_reconciliation_concurrency`; duplicate triggers coalesce and a wallet never has overlapping operations. The backstop retains its existing cadence. BOOT-OWNED (TOML/env only; the former runtime-config surface was inert and removed in #530). With the activity websocket enabled, periodic polling remains the always-on correctness backstop. |
| `polymarket_activity_ws_enabled` | false | #530: websocket-primary trade observation via the officially listed real-time data endpoint whose activity subscription and payload are published by the first-party client, without published completeness, uptime, ordering, continuity, or resume guarantees (`docs/15` entry + re-check policy). #546 runs `activity_ws_reader_count` independent readers per process. Boot-owned; false = poll-only, byte-identical to pre-#530 (the rollback posture). Never disable while a Δ=2 batch is latest — reverse-order rollback: restore a Δ=20 batch first |
| `copy_latency_budget_secs` | 2 | #530/#546/#588: while websocket-primary, strict `age > budget` rejects copying from either REST poll or activity websocket at the early admission gate and shared pre-dispatch gate (`no_copy_dispositions`: `stale_fallback_past_copy_budget` / `stale_activity_ws_past_copy_budget`). Continuation 5 freezes `PaperFreshnessPolicy { activity_ws_enabled, copy_latency_budget_secs }` at bucket commit and uses the earliest verified bound source time. Its final paper-only freshness decision at the Prepared boundary records the precise `paper_prepared_staleness_gate` clock; expiry becomes `paper_stale_before_prepared`, consumes the entry, and releases staged live targets with a no-fill paper outcome. Matches the ranker `LATENCY_SHIFT_SECS`; re-checked at +1 week against the measured span artifact. |
| `activity_ws_reader_count` | 3 | #546: independent activity-websocket reader connections per `pe-service` process, each owning its socket and reconnect backoff; two survive the measured connection-local silent failure. Code constant in `source-polymarket-public::activity_ws` |
| `activity_ws_normalized_activity_timeout_secs` | 30 | #546: a reader with no normalizer-accepted activity row for this long is not live — while reading it drops its socket at the deadline and re-dials after its own backoff (1, 2, 4 … 60 s, reset only by a normalized row); while blocked on a full fan-in send it keeps the socket and its retained row, derives non-live in health, and drops only after that frame drains. Acknowledgements, keepalives, control frames, unrelated topics, envelope errors, and parser-rejected payloads never refresh it (they advance wire health only). Readiness: 0 live readers or a poisoned sink ⇒ `activity_ws_unavailable`; exactly 1 ⇒ `activity_ws_redundancy_degraded`; unavailable + unhealthy poll ⇒ `copy_admission_blocked` |
| `poll_unhealthy_error_streak` | 3 | #530: consecutive all-error poll rounds at which the REST source counts unhealthy (round-age bound: 3 × `trade_poll_interval_secs`) |
| `polymarket_clob_base_url` | `https://clob.polymarket.com` | Polymarket CLOB REST API base URL for order submission and status polling |
| `polygon_receipt_rpc_url` | `https://polygon.publicnode.com` | Boot-owned, key-free Polygon JSON-RPC endpoint used only for ordinary-live receipt finality. `ServiceConfig` field set by `PE_POLYGON_RECEIPT_RPC_URL`; the code default is owned by [`default_polygon_receipt_rpc_url`](../crates/service/src/config.rs). |
| `polymarket_clob_min_interval_ms` | 200 | Minimum interval between CLOB requests (5 req/s sustained limit per rate-limit table above) |
| `polymarket_clob_poll_interval_ms` | 100 | Interval between GET /order/{id} polls while waiting for terminal status |
| `clob_book_request_timeout_secs` | 5 | Fixed `crates/service/src/clob_book.rs` constant `CLOB_REQUEST_TIMEOUT_SECS` (no env override): per-request timeout for the public CLOB `/book` liquidity-capture fetch (issue #350 WS2). Deliberately shorter than `polymarket_request_timeout_secs` (10) — the fetch runs off the fill hot path, so a slow book degrades to a partial snapshot rather than blocking a trade. Reuses the existing `polymarket_clob_min_interval_ms` (200) gate. |
| `absorbable_depth_bps` | 100 | Fixed `crates/service/src/snapshot_worker.rs` constant `ABSORBABLE_DEPTH_BPS` (no env override): ask-depth band for `absorbable_usd_100bps` (issue #350 WS2 PR-H). Σ price·size over ask levels priced within this many basis points of the best ask. 100 bps = 1 %; baked into the column name `absorbable_usd_100bps`, so it is a constant rather than an operator knob. |
| `reconciliation_page_limit` | 500 | **Module const** in `source-polymarket-public` (not a TOML/env key). Fixed page size for the #544 activity and current-position proof readers. |
| `activity_max_offset` | 5,000 | **Module const** in `source-polymarket-public` (not a TOML/env key). A full terminal `/activity` page at this offset is split at an integer-second boundary; a still-full one-second terminal window is typed-incomplete and blocks reconciliation (#544). |
| `positions_max_offset` | 10,000 | **Module const** in `source-polymarket-public` (not a TOML/env key). Each explicit `redeemable=false` and `redeemable=true` current-position partition is walked independently through this offset. A full terminal page is typed-incomplete (#544). |
| `anchor_refresh_secs` | 3,600 | **Module const** `ANCHOR_REFRESH_SECS` in `service` (not a TOML/env key). Seconds between best-effort per-wallet position re-anchors; an owner-selected operational default, not a calibrated value. |
| `bracket_concurrency` | 4 | **Module const** `BRACKET_CONCURRENCY` in `service` (not a TOML/env key). Maximum wallet brackets in flight at once during the boot bracket and runtime admission batches (#555 addendum D9). Chosen from the measured per-wallet peak of ~304 MB resident on the largest wallet against the 2 GB production host; every bracket still reads the wallet's full history three times. |
| `redeem_residual_limit_atomic` | 100 | **Exclusive module const** `REDEEM_RESIDUAL_LIMIT_ATOMIC` in `position-ledger` (#557). A REDEEM underflow residual of 1..=99 atomic units clamps the position closed and is recorded in the version-3 effect document; 100 or more fences. `ShareAmount` has six decimal places, so 100 atomic units equal one ten-thousandth of a position: the limit is one four-decimal `/positions` reporting quantum and accepts venue-display quantization residue without hiding a full reported quantum. SELL and MERGE remain strict, and the tolerance cannot stack within a bucket. |
| `rehearsal_quiescence` | production unit policy at run time | The #545 rehearsal reads the production `pe-service` unit's `TimeoutStopSec` as its exact quiescence bound and requires `KillSignal` to be SIGINT, the signal handled by the service. The harness owns no separate numeric shutdown bound (#586). |

### Causal re-anchor and rehearsal rules (#557)

**Late-group re-anchor.** Previously unseen activity groups arriving for a wallet at or before an
already-committed source epoch are recorded raw-only as `reanchor_required_late_group`; they do not
mutate the ledger or produce a decision. The existing `reanchor_required` flag selects the wallet at
its class's next fair refresh turn for a fresh complete history/positions bracket. A bucket mixing
durable and unseen groups, or a revision of an already-durable group, still fences.

**Installed-boot anchor reuse.** An ordinary installed boot reuses a wallet's `leader_positions`
mirror only when the wallet is unfenced and history-complete, has a delivery cursor and non-null
activity cutoff, has an installed anchor no older than `ANCHOR_REFRESH_SECS`, and has
`reanchor_required = false`. Any failed condition walks the wallet through `validate_direct`.
`position_validation_current` is not a reuse prerequisite. When a required `validate_direct` walk
encounters a `TRADE` row with a price outside the unit range (issue #594; observed 2026-09-11), the
wallet is left out of the accepted set like a transient read failure (unvalidated for runtime
admission; a wallet reused on a fresh anchor is not re-read at boot), and a live wallet's periodic
anchor refresh reports it deferred instead of ending the poll round, keeping the anchor it already
holds (issue #597). Malformed pages, window invalidations, other row validations, aggregation and
identity failures remain boot-fatal.

**Rehearsal isolation.** A #557 rehearsal uses a copy of production durable state at dedicated
paths, an exclusive loopback bind, and a complete environment that explicitly sets `PE_BIND` plus
every state/log/history path override. It talks to the real read sources but puts the Supabase
publishable key in `PE_SUPABASE_SECRET_KEY`; no service-role key may exist in its environment or
process. The database privilege matrix and representative refused HTTP canaries prove that every
reachable service write site remains unavailable. Isolation is an operational credential/path
boundary, not an application mode or write-suppression code path.

**Generation verification.** A #557 generation reaches `verified` only on the ranking batch frozen in
`prechecked`, with `status.json.updated_at` newer than the recorded systemd invocation's
`ActiveEnterTimestamp`. The signed-in site check is a recorded operator confirmation because Google
single sign-on prevents automation: the activation manifest stores `site_confirmed_by` and
`site_confirmed_at`, and non-interactive driver runs require `--site-confirmed` before Forge is restored.

### Isolated Polymarket V2 canary (`pe-service-live-canary`)

The canary is a boot-frozen campaign role, not an `ExecutionMode` or promotion state. It is
installed inactive and remains isolated from ordinary `pe-service`, whose #508 path instead uses
per-account sealed credentials and ships dark until an account is armed. The dedicated canary actor
owns its credentialed client, one mode-0600 event log, command serialization, reservation, one-shot
POST, reconciliation, and recovery.

| Key | Canonical value | Meaning |
|---|---:|---|
| `canary_command_queue_capacity` | 1 | Capacity of the normal actor mailbox; saturation rejects before acceptance. |
| `canary_post_timeout_secs` | 10 | Total budget for the one allowed POST attempt; timeout is ambiguous and closes admission. |
| `canary_reconciliation_timeout_secs` | 30 | Budget for one authenticated reconciliation pass. |
| `canary_reconciliation_interval_secs` | 30 | Non-overlapping observation cadence while armed or closed with unresolved state. |
| `canary_ranking_max_age_secs` | 21,600 | Maximum age of the one strictly matched Supabase ranking batch used for organic admission (6 hours). |
| `canary_shutdown_work_deadline_secs` | 40 | Signal-to-work cutoff for POST completion and the one designated final reconciliation; the final 5 seconds are reserved for durable terminal state/status. |
| `canary_shutdown_deadline_secs` | 45 | Application drain deadline; systemd's 50-second guard is outer-only. |

Campaign financial limits and eligibility are canonical in
[`19-WINNER-FOLLOW-STRATEGY.md`](19-WINNER-FOLLOW-STRATEGY.md#isolated-polymarket-v2-canary).

### Ordinary multi-account live execution (`pe-service`, issue #508)

| Key | Canonical value | Meaning |
|---|---:|---|
| `live_armed_accounts_max` | 2 | v1 maximum simultaneously armed accounts. The service refuses a third; raising this requires re-validating the production p95 latency and sustained CLOB request budgets. |
| `live_accounts_stale_after_secs` | 120 | Accounts-snapshot freshness bound (#514): 4 × `config_poll_interval_secs`, the polled-source block threshold (see Source freshness defaults). While the last successful accounts poll is older than this (or never happened, or is future-dated), the service stages no new dispatch aggregates, pauses `pending` targets, and writes no effective-mode transitions; in-flight order recovery and redemption reconciliation are deliberately not gated. Module const `live_accounts::LIVE_ACCOUNTS_STALE_AFTER_SECS`. |
| `account_id` | `[a-z0-9_-]{1,32}` | Immutable lowercase account slug grammar, enforced by `core-types::AccountId` and `accounts.account_id`. |
| `live_price_impact_cap_bps_default` | 100 | Per-account default in `accounts.live_price_impact_cap_bps`; the database accepts `1..=10_000`. This is distinct from the shared paper `price_impact_cap_bps_default`. |
| `dispatch_seed_retention_days` | 30 | Retain terminal dispatch aggregates for this many days after finalization, then prune seed and target rows together. |
| `live_redemption_surface_after_attempts` | 3 | Compiled threshold after which an unresolved automatic redemption is surfaced prominently; it is not an operator knob. |

### Paper trading state (`paper-state`, issue #282)

| Key | Default | Meaning |
|---|---:|---|
| `paper_state_db_path` | `./paper_state.db` | Path to the crash-safe paper-state SQLite mirror. Schema three stores quantities, principal, and fees as exact decimal strings and binds each financial mutation to its prepared sequence and `QualificationStarted` identity. It also durably owns reconciled activity revisions, first-entry evidence, terminal `decision_pending`, monotonic wallet fences, position validations, and machine-owned migration metadata. |
| `gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for Polymarket Gamma market metadata and mark-price reads. Resolution payout evidence comes from the CLOB market endpoint. Shares the same 50 ms / 20 req/s rate limit as `bootstrap_gamma_min_interval_ms`. |
| `gamma_resolution_poll_interval_secs` | 120 | Legacy-named cadence for the service's CLOB resolution poll. The poll is limited to conditions with open unsettled positions; Gamma is not a payout authority. |
| `max_resolution_horizon_secs` | 172_800 (48 h) | `ServiceConfig` field. Drop entry signals whose market resolves further than this many seconds into the future. 0 disables the upper bound. Guards against locking capital in months-long markets (issue #290). **172_800 since the 2026-07-03 run28 cutover** — the copy-time twin of `ranker_ttr_hours` (48 h ≈ 72 h on paired weekly P&L, `docs/33` §5; was 259_200/72 h). Paired with `min_resolution_horizon_secs` — one resolution lookup serves both. NOTE: the live `service_config` row must be PATCHed at deploy (`on conflict do nothing` never updates an already-seeded row). |
| `min_resolution_horizon_secs` | 60 | `ServiceConfig` field. Drop entry signals whose market resolves *sooner* than this many seconds from now — a copy cannot realistically fill and hold a market about to resolve. 0 disables the lower bound. `docs/29`: the 1-minute copy floor; sub-minute "breaks down" (issue #339). |

### Copy-entry gate (first-ever BUY entry; issues #290, #339)

Copies only a leader's first-ever BUY entry into a market that resolves within the configured horizon. Version-two `wallet_market_history_v2`, `entry_gate_results`, and `wallet_history_status_v2` rows in paper-state are the sole runtime history owner; `CopyEntryGate` is rebuilt from them before producers. Missing or incomplete reconciled history blocks membership publication. The captured legacy history file is a one-time migration input selected by boot-owned `legacy_wallet_history_path`: its source hash and import result are durable; it must stay present and unchanged until the migration reaches phase `installed`, after which file edits are inert. It is not a runtime sidecar. SELLs remain non-consuming.

“History complete” means complete over attributable rows: rows whose asset no configured metadata authority can verify are recorded `raw_only` and cannot contribute a market to first-entry history.

For validator-backed runtime admission, a successful full-history bracket carries
`WalletHistoryStatusRecord` through `AnchorInstall.history_status` into the accepted anchor
transaction. That transaction inserts missing or completes incomplete `wallet_history_status_v2`
rows. After commit, `BucketCommitEngine::apply_history_projection` publishes completion into
`complete_history` and `CopyEntryGate` before the installer acknowledges preparation. Failed,
deferred, invalidated, or fenced brackets and failed anchor installs publish no completion.
Validator-free admission still requires complete history. Direct boot retains seeded-only
promotion through `mark_seeded_history_validated`; installing an anchor alone does not complete
an unseeded wallet's history.

Activity groups in one wallet/epoch bucket commit atomically against immutable pre-bucket gate
state. A single first BUY consumes history even if a later copy gate rejects it; multiple
same-market candidates in one second are all recorded as ambiguous. `decision_pending` bridges
the durable ledger/gate/history commit to the later production-only continuation: `open` is
closed only by a terminal disposition, and replay consumes the recorded transition without
executing the continuation. Receipt-bearing continuations pair `ActivityWs` provenance with a
websocket source receipt exactly; REST provenance has no websocket receipt. A changed semantic
revision, unprovable activity, invalid mapping,
or ledger arithmetic failure creates a monotonic `wallet_fences` row. Fenced wallets are removed
from effective membership/projection and cannot copy; there is no delete owner for a fence.

#### Continuation and commitment compatibility (#588)

| Continuation wire version | Activity-page envelope | Complete-read commitment | Decision/replay contract |
|---|---|---|---|
| 2 | Legacy receipt-free continuation | None | Exact pre-#545 `RuntimeConfig` decoding as `Legacy17`; legacy complete-second proof. |
| 3 | Historical activity schema 2 / parser 2 | None | Receipt-bearing historical activity; legacy complete-second proof. |
| 4 | Activity-page schema 3 / parser 2 | Envelope schema 1 / parser 1 / payload 1 / domain `prediction-edge/activity-read-commitment/v1` | Legacy complete-second proof. |
| 5 | Activity-page schema 3 / parser 2 | Envelope schema 2 / parser 1 / payload 2 / domain `prediction-edge/activity-read-commitment/v2`, with observation bindings | Frozen `PaperFreshnessPolicy`, repaired complete-second proof, and precise final paper Prepared freshness clock. |

“Continuation 5” names the wire generation decoded by `DecisionContinuationV3`, distinct from
`decision_replay::TERMINAL_EVIDENCE_VERSION = 5`. Historical continuations keep their authentic
decoding and `classify_complete_second_legacy` proof; continuation 5 uses
`classify_complete_second`. Its `ObservationBinding` records authenticate the stream receipt and
history target, semantic revision, page occurrence, and any identity correction. Freshness uses
`DecisionContinuationV3::verified_source_time`, preserving the earliest bound source timestamp;
the canonical history epoch and financial operation identity remain unchanged. The final
`DecisionClockEvidence` retains `unix_millis` plus `submillisecond_nanos` for exact replay of the
strict budget comparison.

**History-only bracket disposition.** `history_only_bracket` (`HISTORY_ONLY_BRACKET`) records an
admitted first entry whose copying a causal bracket suppresses. It is an applied activity
disposition and a `no_copy_dispositions` reason with `reconciled_rest` provenance: the entry's
history is consumed, with no copy continuation. Wallet-ledger replay and qualification both
recognize it as applied. Historical `not_copy_eligible` rows retain their recorded meaning and
are not relabeled. Its first durable write independently requires a reader that recognizes it,
even before any continuation-5 write.

### Hot runtime configuration (`service_config`, issues #544 and #545)

The service derives `ConfigEra` once from the verified paper log. Before
`QualificationStarted`, `Legacy17` accepts the historical complete snapshot only to preserve its
canonical pre-Start hash; its two superseded values are private compatibility data and never enter
corrected economics. After Start, `Financial15` accepts exactly the economic names below. In either
era every era-owned key is mandatory exactly once except `kelly_fraction_override`, which may be
absent. A separate optional `risk_halt_release_hash` row is incident control and is excluded from
the economic configuration hash.

`active_watchlist_size`, `mode`, `max_fill_price`, `min_fill_price`,
`min_resolution_horizon_secs`, `max_resolution_horizon_secs`, `price_impact_cap_bps`,
`flip_human_approved`, `kelly_fraction_above_default_human_approved`,
`kelly_fraction_override`, `per_trade_cap`, `slippage_rate`, `sizing_mode`,
`sizing_dollar_usd`, and `sizing_contracts`.

A version-2 decision continuation written before #545 carries the pre-#545 `RuntimeConfig` shape (no
`era`, with `fill_mode` and `polymarket_fee_rate` at the top level); readers decode exactly that
shape as `Legacy17` with those values as compatibility data, and every later continuation version
carries the era-bearing shape.

Missing, duplicate, unknown, malformed, cross-era, or cross-field-inconsistent rows reject the
whole proposal and retain the whole last-good snapshot. The one canonical applied identity is a
BLAKE3 hash of that era's values actually applied; a pending watchlist-capacity transition
continues to hash the old applied capacity. Before a changed economic hash or capacity publishes,
the orchestrator synchronizes an insufficient-evidence qualification seal. Rejected raw rows and
their typed error are status evidence, not a second revision.

After `QualificationStarted`, optional text row `risk_halt_release_hash` is incident control, not
economic configuration. The boot and poll paths partition it before exact-key parsing and exclude
it from the applied economic hash. A value must be exactly 64 lowercase hexadecimal characters
and name the append hash of the currently active `RiskHaltChanged` engagement. It may release only
that same absolute-loss cause, or a latency cause held by a missing sample; the synchronized
release consumes it. Missing, empty, malformed, stale, already-consumed, or cause-mismatched
values only warn and change neither economics nor halt state.

The guarded operator migration removes these database rows while preserving the corresponding
restart-owned `ServiceConfig` TOML/environment contracts where they still exist:
`bankroll_usd`, `bench_overfetch`, `demotion_cb_alpha`, `demotion_min_trades`,
`demotion_pnl_window_secs`, `gamma_resolution_poll_interval_secs`,
`inactivity_hard_cap_secs`, `inactivity_threshold_secs`, `log_retention_days`,
`maintenance_interval_secs`, `status_interval_secs`, `supabase_refresh_interval_secs`, and
`supabase_sink_reconcile_interval_secs`. It also deletes the retired keys
`entry_gate_fail_closed`, `position_page_limit`, `position_reseed_interval_secs`,
`position_size_threshold`, `wallet_market_history_path`,
and `trade_poll_interval_secs`. Any database key outside
the exact hot or removal sets stops `scripts/migrate_service_config_544.sql` before mutation.

| Key | Default | Meaning |
|---|---:|---|
| `max_fill_price` | `0.85` | Hot decimal value. The signed ladder's worst accepted tick must be below this ceiling after the no-chase and price-impact ceilings. `0` disables this band edge, not the mandatory book gate. |
| `min_fill_price` | `0.15` | Hot decimal value, added at the 2026-07-03 run28 cutover. Skip a BUY copy whose resolved fill basis is `<` this so selection and deployment share the entry band. The boundary itself fills (strict `<` skip). `0` disables this band edge, not the mandatory book gate. |

### Live wallet source (Supabase ranking handoff, issue #339)

The local latency-shift ranker pushes append-only ranking batches to Supabase (`scripts/push_ranking_to_supabase.py`); `pe-service` reads the `latest_ranking` view on an interval — filtered to `survives=is.true` since #518, so the published 200-row bench admits only the wallets the ranker's own eligibility gate passed, and a batch with no verdict admits nobody (fail-closed) — and refreshes the scores of the live working set (`crate::live_watchlist::LiveWatchlist`, an `ArcSwap`) up to the runtime `active_watchlist_size` cap. MEMBERSHIP follows `watchlist_membership_mode`: maintenance-tick knockout/backfill (`knockout`, issue #350 WS1) or wholesale per-batch replacement (`full_rerank`, the 2026-07-03 cutover production mode). Every post-boot addition — capacity grow, full-rerank swap, or knockout backfill — is first prepared through one shared serialized preparer (`crate::watchlist_admission`, #542): validator-backed preparation completes the wallet's prior-market history and validates current positions through the five-step causal bracket; the orchestrator persists completion in the anchor transaction and publishes the running history projection before acknowledgement and membership publication (see [Copy-entry gate](#copy-entry-gate-first-ever-buy-entry-issues-290-339)). Validator-free preparation still requires complete history. A fenced or unavailable candidate publishes no additions; capacity and full-rerank retry unchanged, while knockout applies its decided evictions without backfill. A full-rerank transition reads `ranking_entries` pinned to the batch identifier that triggered it, so the applied rows and the committed batch marker always name one batch. Supabase is the sole wallet source (issue #370): there is no leaderboard/seed fallback, so the service hard-fails at boot if `latest_ranking` is empty or unreachable. The Supabase keys follow the secret precedent (plain `String`, empty default, never logged).

The refresh task is also the sole serialized public-projection worker (#544). Boot, a successful
score/rank refresh, and every membership or durable-fence transition coalesce into one bounded
dirty signal. The worker snapshots effective membership (live membership minus durable fences)
and invokes service-role-only `service_watchlist_replace_v1(expected_token, entries)`. The RPC
locks `service_runtime`, compare-and-swaps its `updated_at` token, validates unique lowercase
wallet/rank/`leader_score_bps` rows, replaces the whole `service_watchlist`, updates the matching
count, and returns the new token. A stale writer loses without exposing a partial set; projection
failure records typed analytics degradation and retries without failing trading readiness. The
site reads runtime token → watchlist → runtime token and accepts even an empty set only when
both tokens and row count agree, retrying one token race before returning typed unavailable.

| Key | Default | Meaning |
|---|---:|---|
| `supabase_url` | `""` | `ServiceConfig` field. Supabase project REST base URL (e.g. `https://<ref>.supabase.co`); the **sole** wallet source (#370). If it resolves empty or unreachable the service hard-fails at boot (no fallback). Set via `PE_SUPABASE_URL`. |
| `supabase_secret_key` | `""` | `ServiceConfig` field (secret). The service-role key, sent in **both** the `apikey` and `Authorization: Bearer` headers — Supabase's `sb_` keys are not JWTs, so PostgREST 401s (`PGRST301`) if the two headers differ. Bypasses RLS for the server-side read. `PE_SUPABASE_SECRET_KEY` from `.env`. |
| `supabase_anon_key` | `""` | `ServiceConfig` field (secret). Publishable/anon fallback used for both headers **only when `supabase_secret_key` is empty**; ignored otherwise. `PE_SUPABASE_ANON_KEY` from `.env`. |
| `supabase_refresh_interval_secs` | 300 | `ServiceConfig` field. Seconds between live-watchlist refresh polls. The refresh loop is spawned only when `supabase_url` is non-empty and this is `> 0`. |
| `config_poll_interval_secs` | 30 | Seconds between `service_config` runtime-config polls (issue #398 WS1, `config_poller::CONFIG_POLL_INTERVAL_SECS`). Boot-frozen const (the poll cadence cannot govern itself); the loop is spawned only when `supabase_url` is non-empty. A valid config edit is observed within this window with no restart; a watchlist-capacity change is published only after its safe admission preparation finishes. |
| `supabase_sink_enabled` | `false` | `ServiceConfig` field. Enables the best-effort paper-fill/settlement sink to Supabase (issue #343). The sink task is spawned only when this is `true` **and** `supabase_url` is non-empty. Requires `supabase_secret_key` — under RLS the anon key can only read, so anon-only writes 403. `PE_SUPABASE_SINK_ENABLED`. |
| `supabase_sink_channel_capacity` | 256 | `ServiceConfig` field. Bounded mpsc capacity for the trade-path → sink event channel. Backpressure: drop-on-full (the periodic reconcile re-derives dropped fills/settlements from `paper_state`, so a drop self-heals). `PE_SUPABASE_SINK_CHANNEL_CAPACITY`. |
| `supabase_sink_reconcile_interval_secs` | 300 | `ServiceConfig` field. Seconds between periodic sink reconciles: a contiguous-prefix fill HWM catch-up over `list_fills()` plus a full re-upsert of `list_settled_markets()`, healing any dropped or failed live writes. `PE_SUPABASE_SINK_RECONCILE_INTERVAL_SECS`. |
| `supabase_authoritative` | `false` | `ServiceConfig` field (issue #397). Makes Supabase the **authoritative system of record** for paper-state's money + book: `paper_bankroll`, `paper_positions`, `paper_fills`, and `settled_markets`. After `QualificationStarted`, the orchestrator serializes each paper financial mutation as `FinancialPrepared` → Start- and predecessor-bound `commit_fill_v2` or `apply_resolution_v2` → local SQLite projection → `FinancialFinal`. Both RPC requests bind the exact Start receipt and completed prior Prepared sequence; an unmatched Prepared is recovered before any successor. `run_sink` is not spawned in authoritative mode. Requires the service-role `supabase_secret_key`. Off by default; set explicitly in `.env`. Schema + RPCs live in `scripts/supabase_paper_state_schema.sql`; the one-time backfill and legacy frame walk are pre-Start only. `PE_SUPABASE_AUTHORITATIVE`. |
| `snapshot_channel_capacity` | 256 | `ServiceConfig` field (issue #350 WS2 PR-H). Bounded mpsc capacity for the trade-path → liquidity-snapshot worker channel. Drop-on-full: a full channel drops the snapshot request so the BUY fill path never blocks (capture is best-effort analytics). The snapshot worker is spawned under the same gate as the Supabase sink (`supabase_sink_enabled` + non-empty `supabase_url`). `PE_SNAPSHOT_CHANNEL_CAPACITY`. |
| `supabase_paper_positions_page_limit` | 1_000 | **Module const** in `supabase_state.rs` (#516). Requested page size for the authoritative boot `paper_positions` pull; the walk advances by the ACTUAL returned length and terminates only on an empty page, so a server `db-max-rows` below this value still pulls everything. |
| `supabase_paper_positions_max_rows` | 500_000 | **Module const** in `supabase_state.rs` (#516). Inclusive bound on total pulled position rows — turns an ignored/repeating offset into a loud boot failure instead of an unbounded loop. |
| `LS_TSTAT_BPS_SCALE` | 1_000 | **Module const** in `supabase_reader.rs`. t-stat → `leader_score_bps` scale (ordering only, not a gate). A t-stat of 2.5 maps to 2500 bps. |
| `upload_active_window_hours` | 72 | `--active-window-hours` in `scripts/push_ranking_to_supabase.py` (issue #350 WS3). The ranking push drops wallets with no cached trade (`wallet_cache.db`) in the last N hours so idle wallets never reach the live set. Off unless `--db` is given; `scripts/rank_and_push.sh` always passes it. Drift-guarded by `scripts/test_push_ranking_filter.py`. |
| `ACTIVE_WINDOW_HOURS` | 72 | **Module const** (`i64`) in `crates/service/src/supabase_reader.rs` (#357). Candidate-freshness filter: `fetch_candidates` only returns bench wallets whose `last_trade_unix ≥ now − 72 h`, mirroring `upload_active_window_hours` on the push side so the maintenance-tick backfill never admits a wallet the upload would have dropped. |
| `upload_max_cache_staleness_hours` | 24 | `--max-cache-staleness-hours` in `scripts/push_ranking_to_supabase.py` (issues #350/#519). The push aborts (non-zero exit, no Supabase write) unless the newest trade, newest resolution fetch, and present completed CLOB sweep marker (`value=''`) are all within this bound. Backfill before pushing (docs/26). Drift-guarded by `scripts/test_push_ranking_filter.py`. |
| `ranking_publish_max_retries` | 5 | Maximum transient Supabase retries for each atomic ranking-publication request in `scripts/push_ranking_to_supabase.py`; the initial attempt is additional. Only transport/timeouts and HTTP 408/425/429/5xx retry. |
| `ranking_publish_retry_base_secs` | 1 | Initial exponential-backoff delay for transient ranking-publication retries. Backoff doubles to `ranking_publish_retry_max_secs`; a numeric `Retry-After` overrides the exponential delay within the same bound. |
| `ranking_publish_retry_max_secs` | 30 | Maximum per-request transient retry delay for ranking publication. |
| `rank_and_push_tempfail_exit` | 75 | Exit code used when a bounded retryable rank-cycle operation exhausts its in-process retries or the resolution audit is blocked (`blocked > 0 \|\| clipped > 0` — a market whose available venue truth could not be recorded, or a repair-cap clip; venue-side incompleteness such as lagged/extended/inactive/pending markets is counted and retried without blocking, issue #523). Emitters are Gamma `events` page exhaustion, CLOB closed-markets page exhaustion in the `resolutions` command (`Transient`/`RateLimited` exhausted after `clob_page_max_retries`, issue #534), a blocked resolution audit, and ranking publication. The production loop preserves and resumes the applicable durable logical-cycle or exact-publication pointer; permanent failures remain non-75 and stop the loop. |
| `rank_and_push_loop_retry_secs` | 60 | Delay between loop-level retries of a durable logical cycle or pending ranking publication. The loop checks its run flag once per second during this delay so an operator stop remains prompt. |
| `rank_and_push_cycle_pointer` | `data/eval-results/rank_and_push.cycle` | One-line repository-relative pointer to the active zero-argument production `cron-<UTC>` run directory. Written atomically after preflight/run-lock acquisition and directory creation but before discovery or activation mutates the cache. A retry reuses the same path and therefore the same deterministic activation batch; compare-and-cleared only after exact publication verification and accepted-watermark capture. Parameterized research runs neither create nor consume it. |
| `ranking_batches_retention` | 1080 | `--keep-batches` in `scripts/push_ranking_to_supabase.py` (issue #411; raised 180 → 1080 at the 2026-07-03 run28 cutover). After a successful push, prune `ranking_batches` to the newest N rows (CASCADE drops their `ranking_entries`), bounding the append-only epoch history (~6 months at the 4h production cadence, 6 pushes/day; preserves the original ~`ranker_window_days` of replay depth the 180-at-daily default was sized for). `latest_ranking` reads only `max(batch_id)`, so pruning older batches never affects the live read path or the `wallet_live_stats_mv` matview. `0` disables the prune (history-preserving research re-push). Best-effort: a prune failure logs HTTP status+body and does **not** fail the push (bounded + self-heals next run). Drift-guarded by `scripts/test_push_ranking_filter.py`. |
| `ranker_window_days` | 180 | `DEFAULT_WINDOW_DAYS` in `scripts/ranker_decay.py` (issue #366). Relative entry-date window: when `--win-end`/`--win-start` are omitted, the ranker (`scripts/rank_72hr_buyandhold.py`) scores `win_end = today UTC-midnight`, `win_start = win_end − this`. Replaces the old fixed calendar window (`2025-12-01 → 2026-06-01`); the relative default **does** take effect on the next production `scripts/rank_and_push.sh` (which leaves `WIN_START`/`WIN_END` empty). Override via `--win-start`/`--win-end` (Python) or `WIN_START`/`WIN_END` (shell). Drift-guarded by `scripts/test_ranker_decay.py`. |
| `ranker_half_life_days` | 30 | `DEFAULT_HALF_LIFE_DAYS` in `scripts/ranker_decay.py` (issue #366). Exponential recency-decay half-life (days) for the edge/t-stat score in BOTH ranking passes (`rank_72hr_buyandhold.py`, `latency_shift_rerank.py`): a trade one half-life old weighs 0.5. `0` disables decay (flat weights = legacy behaviour, bitwise-identical). Both the standalone Python scripts and the production wrapper `scripts/rank_and_push.sh` default to **30** (issue #370 adopted 30-day decay after the half-life sweep, flipping the #366 flat wrapper default). Override with `--half-life-days N`; for the first full-universe run, stage with `--half-life-days 0` so a cohort shift is attributable to the wider universe vs decay. Eligibility/activity gates and `hit_rate` stay raw. Drift-guarded by `scripts/test_ranker_decay.py`. |
| `ranker_universe_source` | all-trade-wallets | Production ranking universe (#370): `scripts/rank_and_push.sh` passes `--universe-from-trades`, so every wallet with trade history in `wallet_cache.db` is scored (have-data ⇒ in-universe) and the ranker's own eligibility filters decide the cohort — there is no curated pre-gate file. Override with `--universe <file>` for research. |
| `ranker_ttr_hours` | 48 | Production TTR ceiling for qualifying first-buys: `TTR_HOURS` in `scripts/rank_and_push.sh` → `--ttr-hours` in `scripts/rank_72hr_buyandhold.py` (whose own argparse default stays 72 — the "72hr" filename is historical). **48 since the 2026-07-03 run28 cutover** (48h ≈ 72h on paired weekly P&L — NW-t 0.11 — so the capital-velocity preference for 48h is free; 24h measurably worse, `docs/33` §5). The shell also derives `--ttr-max-secs` for the push from this value so `ranking_batches.ttr_max_secs` provenance matches the ranked shape. The live copy-time twin is `max_resolution_horizon_secs`. |
| `ranker_prod_min_trl` | 20 | Production MinTRL eligibility: `MIN_TRL` in `scripts/rank_and_push.sh` → `--min-trl` in both ranking passes (pass-1 qualifying-position count `n`, pass-2 filled count `n_filled`; Python argparse defaults stay 0 = off). **The 2026-07-03 run28 cutover value** — MinTRL-20 was the single dominant fix across the run28 grid (`docs/33` §4). It REPLACES the per-month activity gates (run28's `trl20` axis has no per-month component): production passes `--min-avg-per-month 0 --min-active-months 0` alongside it (the Python defaults 20/3 apply only when the flags are omitted, i.e. research invocations). Distinct from `ranker_min_trl` (the bake-off grid's swept sentinel, below). Purge note: widening eligibility grows rule-B's protected set (fewer dead-weight deletions) and newly-eligible provable losers become rule-A tombstone candidates — both by design (`crates/bootstrap/src/purge.rs`). |
| `ranker_fdr_q` | 0.05 | Family significance / false-coverage-rate target for the #421 bake-off harness honesty layer (`docs/31-RANKER-BAKEOFF-METHODOLOGY.md`). The canonical strategy-level significance level — the `select_winner_or_nogo` winner gate and `winner_uncertainty` FCR `top_q` now read the named constant `RANKER_FDR_Q` (issue #436 A6); the Romano-Wolf StepM, Hansen-SPA, and AKM/MRSW inference in `scripts/ranker/oos_validation.py` still take it as their own method-level `size`/`alpha` default `0.05`. Bootstrap reps/seeds and CSCV group counts are method-internal parameters, **not** glossary'd (see `oos_validation.py` header). |
| `ranker_pbo_max` | 0.5 | Probability-of-Backtest-Overfitting ceiling in the #421 winner gate (`select_winner_or_nogo`, `scripts/ranker/bakeoff.py`): a config can only be declared WINNER when its CSCV `PBO < 0.5` (the IS-best config lands above the OOS median more often than not). Wired to the named constant `RANKER_PBO_MAX` (issue #436 A6 — it was glossary'd but the gate read a hardcoded `0.5`). A **degenerate** PBO (`NaN` from near-zero cross-config dispersion — #436 A7) fails the gate *safe* (`NaN < 0.5` is False → NO-GO). |
| `ranker_dsr_min` | 0.5 | Per-step Deflated-Sharpe survival gate (`apply_deflation_gate` threshold, `scripts/ranker/bakeoff.py`): within a trajectory step, candidate configs with deflated-Sharpe probability `< 0.5` are dropped before the policy acts. Wired to `RANKER_DSR_MIN` (issue #436 A6 — previously the hardcoded `threshold=0.5` default, never passed this value). Distinct from `ranker_pbo_max` (a leaderboard-level overfit ceiling) though both are 0.5. |
| `ranker_grid_dsr_min` | 0.95 | Leaderboard grid Deflated-Sharpe **advisory** floor (`RANKER_GRID_DSR_MIN`, `scripts/ranker/bakeoff.py`, issue #436 A6). The winning config's grid-DSR (`P(true SR > expected-max-of-N-nulls SR)` at the full pre-screen `N_GRID`) is surfaced in the decision (`winner_grid_dsr`) and flagged (`grid_dsr_advisory_low`) when below this bar, but is **NOT a hard gate**: the Romano-Wolf / Hansen-SPA / PBO panel already deflates the leaderboard, so gating the per-config grid-DSR on top double-counts the multiple-testing correction and would reject genuinely-good configs (a clean `+0.8`/period config deflates to grid-DSR ≈ 0.93 at `N=20`, below 0.95). `0.95` is the textbook Bailey-López-de-Prado deflated-significance level. Distinct from the per-step hard gate `ranker_dsr_min`. |
| `ranker_min_periods` | 24 | Minimum walk-forward periods (`as_of` cutoffs) the #421 winner gate requires before trusting the bootstrap panel (`RANKER_MIN_PERIODS`, `select_winner_or_nogo`, `scripts/ranker/bakeoff.py`, issue #436 B4). Below it the CSCV-PBO / Romano-Wolf / Hansen-SPA / Deflated-Sharpe gates degenerate to a **silent** always-NO-GO, so the verdict short-circuits to an **explicit** `insufficient periods: N < ranker_min_periods` NO-GO instead. `len(as_of_points)` equals the per-period return-matrix row count; `main`'s `--steps` default is raised to this and `BakeoffParams.min_periods` carries it (tests override to a low value for short synthetic trajectories). 24 ≈ two years of monthly cutoffs — the textbook minimum for stable CSCV/SPA inference; raising it pushes `as_of` further back where CLOB coverage thins (a #436 Phase E concern, surfaced not gated). |
| `ranker_eb_prior_var_floor` | 1e-9 | **Absolute** variance floor for the empirical-Bayes prior in the `eb_shrinkage_skill` estimator (`_EB_PRIOR_VAR_FLOOR`, `scripts/ranker/estimators.py`). The EB prior mean μ0 and variance τ² are **estimated from the cross-section** each scoring step (unlike the fixed `kelly_p_prior_*` Beta priors in `19-WINNER-FOLLOW-STRATEGY.md`). Since issue #436 C1, τ² is the **DerSimonian-Laird precision-weighted positive-part** estimate (not the old unweighted `Var(means) − mean(se²)`), floored to `ranker_eb_tau2_floor_frac · Var(means)`; this absolute `1e-9` is the deterministic fallback for a `<2`-finite-precision-candidate input (an undefined cross-section). |
| `ranker_eb_tau2_floor_frac` | 0.05 | Fraction-of-`Var(wallet means)` floor on the DerSimonian-Laird empirical-Bayes prior variance τ² in `eb_shrinkage_skill` (`_EB_TAU2_FLOOR_FRAC`, `scripts/ranker/estimators.py`, issue #436 C1). The precision-weighted (`1/se²`) positive-part MoM keeps short-track (`n ≤ 4`, huge `se²`) wallets from driving τ² negative, but its positive-part can still return ≈0 on a genuinely homogeneous cross-section; flooring τ² to `0.05 · Var(means)` keeps the shrinkage from fully collapsing (the old `max(prior_var, 1e-9)` sent `shrink → 0`, saturating every posterior onto μ0 and degenerating the ranking to insertion order). A **5% backstop**: it binds only when the DSL estimate is ≈0 — in normal operation the DSL value exceeds it and it never binds — and is small enough to preserve aggressive shrinkage. |
| `ranker_sd_floor` | 1e-9 | Shared per-position net-edge / CLV **dispersion** floor owned by `_SD_FLOOR` in `scripts/ranker_decay.py` (#436/#588). Both the uniform-weight and general reliability-weighted paths in `weighted_stats` return an undefined (`NaN`) t-stat when `sd <= _SD_FLOOR`, retaining their existing mean, dispersion, and effective-N reporting. Production consumers are `scripts/rank_72hr_buyandhold.py` and `scripts/latency_shift_rerank.py`; all five bake-off estimators in `scripts/ranker/estimators.py` import the same floor and drop undefined-dispersion candidates. This prevents floating-point noise in a constant-return series from becoming an extreme finite score. Distinct from `ranker_eb_prior_var_floor`, which gates the empirical-Bayes prior variance. |
| `ranker_min_trl` | 0 | Minimum track-record-length eligibility gate (`Criteria.min_trl`, `scripts/ranker/__init__.py`): a wallet is a follow-now candidate at cutoff `as_of` only with `≥ min_trl` in-sample positions. `0` = no gate (the honest permissive sentinel); the bake-off **sweeps** this in the criteria grid. The winning value fed #417: production adopted **20** at the 2026-07-03 run28 cutover — see `ranker_prod_min_trl`. |
| `ranker_embargo_secs` | 0 | Walk-forward embargo (`split_walkforward(embargo_secs=…)`, `scripts/ranker/oos_validation.py`): the forward track starts at `as_of + embargo_secs`, breaking leakage across the cutoff. `0` = no-op sentinel; swept in the grid like `ranker_min_trl`. The recency-decay anchor for the out-of-sample window is the existing `ranker_half_life_days` (#366). |
| `ranker_akm_near_tie_sigma` | 0.01 | AKM winner-inference near-tie guard (`scripts/ranker/oos_validation.py` `AKM_NEAR_TIE_SIGMA`, amendment A5 of the 2026-07-01 decision record on #417; **widened 1e-3 → 0.01 after run28**, `docs/33` §2): when the winner-vs-runner-up gap is below this many σ of the winner's SE, the conditional truncated-normal law is numerically ill-posed (CDF underflow → degenerate point CI; run24 hit this at a 0.00049σ gap, and run28's 0.003σ gap sat ABOVE the old 1e-3 guard yet still underflowed). Sized from measurement (2026-07-03): width-0 degeneracy observed at 0.001–0.003σ with jitter to ~0.005σ, well-posed by 0.01σ — 0.01 = the observed zone with 2–3× margin (docs/33's earlier '~0.08σ' estimate overstated it ~20×). Below the guard, `akm_inference_on_winners` returns the honest UNCONDITIONAL normal CI (`conditional=false`). |
| `bench_gate_eval_periods` | 6 | Forward-bench promotion gate (docs/32 §3, pre-registered): the SINGLE evaluation look happens at T0 + this many 30-day periods, using the fixed-n empirical-Bernstein bound (valid at one look). |
| `bench_gate_max_periods` | 12 | Forward-bench gate: the one pre-registered extension horizon (taken iff the CI straddles 0 at the first look). Forced decision here; default = keep incumbent. |
| `bench_gate_alpha` | "0.05" | Forward-bench gate: total error budget, Bonferroni-split evenly across the challenger arms (two arms → 0.025 each). Decimal string, parsed to `Decimal` where consumed. |
| `bench_gate_regime_guard_days` | 60 | Forward-bench gate: a challenger is adoptable only if its trailing-this-many-days paired difference is ≥ 0 at the look (regime guard — a stale early lead cannot promote). |
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
| `bench_overfetch` | 10 | `ServiceConfig` field (#350 WS1 PR-D). Accepted for configuration compatibility only; since #588 it has no runtime effect because membership maintenance reads the latest ranking batch bounded by `MAX_ACTIVE_WATCHLIST_SIZE` (fence-before-cap selection). `PE_BENCH_OVERFETCH`. |
| `demotion_min_trades` | 10 | `ServiceConfig` field (#350 WS1 PR-D). Minimum settled fills before either the underperformance demotion (`WalletEdgeStats::should_demote`) or the proven-winner inactivity exception (`is_proven_winner`) applies — no judgement on small samples. `PE_DEMOTION_MIN_TRADES`. |
| `demotion_cb_alpha` | "0.10" | `ServiceConfig` field (#350 WS1 PR-D), a decimal string parsed to `Decimal` at startup (never `f64`). Empirical-Bernstein confidence level α for the demotion upper-CB and the proven-winner lower-CB over the per-fill **dollar** realized edge with the observed range `R̂ = max − min` as the bounded-support proxy (Maurer-Pontil empirical variant; in formula parity with the ranker-harness template `scripts/ranker/demotion.py` per #440). `PE_DEMOTION_CB_ALPHA`. |
| `demotion_pnl_window_secs` | 2_592_000 | `ServiceConfig` field (item 3.1 of the 2026-07-01 decision record on issue #417). Trailing window (seconds; 30 d) for the demotion realized-P&L conjunct: `WalletEdgeStats::should_demote` requires the dollar P&L over fills **settled** within this window to be negative, so a large historical winner carries no unbounded bleed allowance. Lifetime `realized_pnl` remains an audit field. `PE_DEMOTION_PNL_WINDOW_SECS`; seeded in `service_config` (admin-editable) since the 2026-07-03 cutover PR. |
| `watchlist_membership_mode` | `knockout` | `ServiceConfig` field (text), **added at the 2026-07-03 run28 cutover**. Who owns watchlist MEMBERSHIP between ranking batches: `knockout` (legacy — the push is score-update-only; the maintenance tick's knockout+backfill is the sole membership path) or `full_rerank` (each batch transition wholesale-replaces the live set with the triggering batch's `ranking_entries` top-`active_watchlist_size` **survivors**, pinned by `batch_id` (#542) — the read is verdict-filtered since #518, so the cap bounds survivors rather than selecting a raw top-N — wallets re-earn their slot every push; run28 `docs/33` §5 found knockout-only the worst policy and full re-rank the most robust — note the POLICY evidence is at the weekly cadence run28 tested; the 4h push cadence itself is a data-freshness choice per the docs/32 §1 adoption entry, not an evidence-backed cadence). Run28 fixed `k=25` and did not sweep width. The earlier top-50 paper width was an operator-directed experiment adopted 2026-07-13; the current default of 100 supersedes it and is likewise an operator choice, not a run28-backed choice of N. Memoryless by design: the ranker's verdict overrides live demotion memory at each batch; the #473 demotion gate still runs BETWEEN batches as the intra-cycle rail. **Boot-frozen** (`PE_WATCHLIST_MEMBERSHIP_MODE`; the maintenance loop is built once at startup — deliberately NOT in `service_config`); parse is fail-fast when the maintenance loop is enabled (Supabase configured + non-zero interval; the mode is inert otherwise). The cutover deploy sets `full_rerank`. |

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

| Symbol | Purpose |
|---|---|
| `pe_venue_polymarket::{CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2, TOPIC_ORDER_FILLED_V2}` | Sole owner of the surviving V2 exchange identities and topic. Bootstrap re-exports these names for legacy enum-state synthesis rather than duplicating their bytes. |
| `pe_bootstrap::chain::{ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1}` | Legacy enum-state synthesis in `migrate::auto_migrate_legacy`; the aggregates include the venue-owned V2 constants. |
| `normalise_condition_id` | `0x` condition-id canonicalisation shared by the Gamma `/events` sweep (relocated from `dune.rs` in #335). |

Surviving cache/cursor artifacts — legacy, **read-only** on the Dune path:

| Key | Default | Meaning |
|---|---|---|
| `wallet_cache_mutation_lock` | `<cache_path>.lock` | Persistent kernel-held `flock(2)`/`fs2` inode (`pe_bootstrap::lock::CacheMutationLock`) acquired centrally before every read-write `WalletCache::open`, including named and no-argument `all`; true readers use `open_read_only`. Acquisition opens without truncation, takes the exclusive nonblocking lock, then writes only the live holder's decimal PID. Process death releases the kernel lock but never removes the inode; PID text is diagnostic and is never reclaimed. Forge activation acquires persistent loop → one-shot run → cache locks in that sole order. |
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
| `slippage_rate` | 0.01 | Expected proportional BUY slippage applied once to the all-in per-share cost after signed principal and the compact-schedule fee. Canonical default: 100 bps. |

### Service health (`HealthState`)

| Key | Default | Meaning |
|---|---:|---|
| `source_freshness_window_seconds` | 60 | Seconds without an event before a source is considered stale in `/health/ready` |

### Agent-friendly log layout

pe-service writes three bounded JSONL artifacts (so an agent reads one tiny file for health,
a focused file for problems, and a bounded stream for detail — never an unbounded firehose):

| Artifact | Shape | Use |
|---|---|---|
| `status.json` (`status_path`) | single file, atomically rewritten every `status_interval_secs` | **current health snapshot** — includes the embedded source `revision`, top-level `applied_config_hash`, sticky named `tasks` with class/state/typed failure, `status_error`, the prior financial/source/live fields, optional `runtime_config` (applied hash plus separately typed rejected raw proposal), and optional `watchlist_projection` (`pending`/`applied` token, count, time plus `last_error`). `watchlist_size` is actual membership; `watchlist_target_size` is the last safely applied runtime cap. Read this first; no grep. |
| `<stem>.<date>.jsonl` (from `jsonl_log_path`) | full stream, rotated **daily**, keeps `log_retention_days` | full detail; grep one day's file |
| `errors.<date>.jsonl` (same dir) | **WARN+ERROR only**, rotated daily | the clean "what broke" tape (no INFO chatter) |

| Config | Default | Description |
|---|---|---|
| `status_path` | `./status.json` | `ServiceConfig` field. Path of the health snapshot. `PE_STATUS_PATH`. |
| `status_interval_secs` | 30 | `ServiceConfig` field. Seconds between `status.json` writes; `0` disables. `PE_STATUS_INTERVAL_SECS`. |
| `log_retention_days` | 7 | `ServiceConfig` field. Dated JSONL files kept per sink (full + errors); bounds disk. `PE_LOG_RETENTION_DAYS`. |

### Durable log, migration, and supervisor boundaries (#544)

`pe-event-log::Scanner` is the single physical verifier for source and paper logs. It reports the
resolved path, physical verified-tail offset, last sequence, and last hash while checking file and
frame boundaries, size, checksum, decompression, envelope decode, sequence, stored raw-payload
hash, and BLAKE3 chain. Only a scanner-proven incomplete final frame may be truncated and
synchronized under the exclusive writer lock; interior corruption and every other mismatch are
fatal. Append, flush, or synchronization uncertainty poisons the writer. The account-tagged
`live_journal.log` uses its native verified replay for the same binding fields. An ordinary
installed boot walks the source log once under that lock, binds the recorded activation prefix
during the same walk, and publishes the boot projections built from it only after the walk and
every reducer succeeded; the walked binding is reused only while that writer stays the sole
appender and its synchronized tail equals its byte cursor, which detects external length drift but
not an equal-length rewrite of already-verified bytes (#572).
The runtime qualification seal verifies the sealed prefix with one scanner walk bounded by the
caller's candidate (the just-recorded mark tail at a completion boundary, the receipt-index tail on
configuration drift), reads the frames its decision rows reference exactly through the receipt
index, and retains no payload map (#574).

Paper schema-v1-to-v2 migration is a machine-owned roll-forward state machine:
`boundary_recorded → version_two_inputs_appending → side_state_built →
activation_tails_recorded → installed`. The record binds canonical paths, physical tails,
sequences, hashes, the immutable v1 main and backup, captured legacy-history input, exact side-main
path, binary identity, and final activation tails. Restart resumes the recorded phase and exact
side main; path, prefix, hash, identity, or phase drift fails closed. Pre-boundary v1 frames remain
audit/replay history and cannot create v2 state. Once v2 input has appended or active state has
committed, rollback to v1 is refused; restart the v2-compatible binary to resume roll-forward.

Ordinary production has one named supervisor over 16 retained owners. Activity ingest, public
poll/reconciliation, orchestrator, resolution poller, configured live-account/fan-out owners,
watchlist refresh/projection, maintenance, capacity/config workers, status writer, and HTTP server
are critical: an unexpected typed error, early return, channel close, or join failure sticks in
readiness/status and initiates ordered shutdown. Supabase analytics, liquidity snapshots, and JSON
tracing appenders are best-effort and degrade status without failing trading readiness. Shutdown
orders producers → orchestrator drain → healthy sinks → HTTP → final status → tracing;
synchronization-uncertain work remains unseen for restart recovery. Expected failures are typed.
Production release profiles use `panic = "abort"`; a panic is recovered by systemd restart from
durable state, never by unwinding through service owners. Transient market-end fetch failure is not
cached and is retried on the next request.

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
| `paper_financial_prepared` / `paper_financial_final` | Prepared authority, exact economic record, synchronized receipt, canonical result | The two-frame paper financial protocol. A Final alone proves a completed fill or resolution. Legacy `paper_fill` frames remain readable only before `QualificationStarted`. |
| `membership_changed` | reason, removed/added wallets, capacity, ranking batch, structural evidence | A synchronized structural membership snapshot. Score-only refresh is unjournaled; durable wallet fences are applied independently and monotonically. |
| `portfolio_mark` / `qualification_sealed` | mark evidence or sealed-prefix/digest evidence | Qualification observation and immutable seal records. An insufficient-evidence outcome is a seal reason, not another command or state. |

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
| `active_watchlist_size` | 100 | **Cap** on how many ranker-surviving leaders `pe-service` follows — not the followed count. Since #518 every watchlist read is filtered to the ranker's `survives` verdict, so actual membership is the survivor count when that is smaller (23-36 against this cap in 2026-08), and drifts lower still between batches as evictions go unbackfilled. `status.json.watchlist_size` is the truth; this is `watchlist_target_size`. Supabase `service_config` is authoritative; this key intentionally has no TOML/env surface. Valid range: `1..=200`, matching the published ranking bench. The service loads it at boot and polls every 30 seconds. A missing row, fetch failure, malformed value, `0`, or value above `200` keeps the last-known-good target (never clamps or empties the live set). A grow completes every newly admitted wallet's runtime history and validates current positions through the five-step causal bracket, persisting and publishing history completion before preparation acknowledgement and the atomic membership swap (see [Copy-entry gate](#copy-entry-gate-first-ever-buy-entry-issues-290-339)); a shrink uses the same atomic membership swap. Any preparation failure leaves both membership and the applied target unchanged and retries independently on the capacity worker's next 30-second retry. Installing the supporting binary requires one normal `pe-service` restart; later valid edits hot-swap without a restart. The previous top-50 setting was a 2026-07-13 operator-directed paper experiment, not run28 evidence; 100 supersedes it as the operator default. |
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

Both are read by `risk-engine` as part of its pure inputs. As of issue #398 (Decision #2) they are **admin-mutable at runtime** via the Supabase `service_config` table (the single-email-gated admin panel), default-deny, with each edit audit-logged in `service_config.updated_by`/`updated_at` and applied on the next ≤30s config poll. This reverses the prior "signed config change only" rule. `kelly_fraction_above_default_human_approved` is re-checked against the mode ceiling on every poll in `runtime_config::parse_config`, so an above-ceiling override without the flag is cleared rather than applied.

### Paper-to-live-tiny qualification — quantified

The one sealed observed paper system is eligible for a single manual promotion review only when
ALL of:

| Comparison | Threshold |
|---|---|
| Walk-forward LCB_5pct of follower daily log-growth (after costs) | > 0 |
| Exact replay through the recorded seal | Pass with complete evidence |
| Nonnegative peak-to-trough paper drawdown | strictly less than 0.10 |
| Observation after the current anchor | ≥ 30 complete UTC days AND ≥ 90 closed copied trades |
| Paper copy delay | nearest-rank p95 ≤ 2,000 ms |
| Manual approval | one review after a `Pass` report |

Quiet days count. The initial partial day does not. An underperformance or inactivity membership
demotion moves the anchor to the next valid mark and resets the promotion growth, drawdown, close,
delay, and no-demotion vectors. Normal ranker rotation, capacity change, and score refresh do not.
There is no extension, interim look, second window, or automatic promotion.

### Demotion criteria

Qualification consumes the typed structural membership reasons. Only
`knockout_underperformance`, `knockout_inactivity`, and `knockout_inactivity_hard_cap` are
demotion anchors. Risk halts remain owner- and cause-scoped controls; they are not inferred as
membership demotions. The maintenance statistics and thresholds remain owned by their existing
configuration and implementation, rather than being redefined by the qualification report.

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

### Qualification close behavior

A closed copy is one unique successful Fill Final after the current qualification anchor whose
exposure has a causal Resolution Final by the seal. Open positions and missing or noncausal
resolutions are not samples. Backtest-versus-paper distribution comparisons, resampling, interim
looks, extensions, and second windows are not promotion inputs.

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
| `bootstrap_fetch_resolutions` | `false` | When `true`, `pe-bootstrap` runs the Polymarket CLOB closed-market payout walk after trade fetch and stores validated resolution data in `market_resolutions`. Gamma schedule enrichment is separate. Set `PE_BOOTSTRAP_FETCH_RESOLUTIONS=1` to enable. |
| `bootstrap_rebuild_resolutions` | `false` | One-shot retroactive correction (issue #149 follow-up): when `true`, stage 6 deletes every `market_resolutions` row tagged with an imprecise source (`'gamma'`, `'clob'`) before any fetcher runs; the CLOB stage then re-populates the `'clob'` rows via `INSERT OR IGNORE`. Retained `'polygon'` rows are **not** deleted — they keep their exact block-timestamp `resolved_at_unix`. **#369:** with the on-chain scan removed, CLOB is the only source that re-derives `'clob'` rows (with `end_date_iso`-approximate timestamps); there is no precise on-chain backfill. Idempotent — safe to set on every run. Set `PE_BOOTSTRAP_REBUILD_RESOLUTIONS=1` to enable. |
| `bootstrap_skip_trade_fetch` | `false` | When `true`, `pe-bootstrap` skips the Polymarket trade-fetch step entirely. Safe when the trade cache is already fully populated and only subsequent steps (resolutions, filters) need to run. Emits a warn-level log. Set `PE_BOOTSTRAP_SKIP_TRADE_FETCH=1` to enable. |
| `infra_probe_span_secs` | `3600` | Maximum span (newest − oldest, seconds) across the first 500 trades of a cold-start wallet for it to be classified as infrastructure (issue #197). 500 trades in < 1 h ⇒ > 8 trades/min ⇒ market-maker / treasury / arbitrage bot. Below threshold: wallet is flagged `is_infra = 1` (no tombstone is written), the probe page is discarded, backfill selection skips it via the `active_tradeable_wallets` view and activation requires `is_infra = 0` explicitly; rank/export/publication do not consult the flag (`37-WALLET-EXCLUSION-DECISION-RECORD.md`). The same threshold drives the `pe-bootstrap classify-infra` retroactive sweep over already-cached trades. Override via `PE_BOOTSTRAP_INFRA_SPAN_SECS`; canonical const lives in `pe_bootstrap::infra_probe::DEFAULT_INFRA_SPAN_SECS`. |
| `bootstrap_write_snapshot` | `false` | When `true`, the main `pe-bootstrap` pipeline persists a `(snapshot_at_unix, wallet)` row-set to `leaderboard_snapshots` at run time, stamped with the current `snapshot_at`. Default `false` keeps ad-hoc bootstrap runs (resolutions watchdog retries, dev shells) from polluting the snapshot timeline with near-duplicate intra-day rows — only a deliberate snapshot-producing run should opt in. Set `PE_BOOTSTRAP_WRITE_SNAPSHOT=1` to enable. |
| `bootstrap_gamma_base_url` | `https://gamma-api.polymarket.com` | Base URL for the Polymarket Gamma API. Override via `PE_GAMMA_BASE_URL` (useful for testing against a stub). |
| `bootstrap_gamma_min_interval_ms` | 50 | Minimum milliseconds between Gamma API requests (20 req/s). Live-tested ceiling is ≥ 27 req/s; 50 ms keeps ~25% margin. Enforced globally by `ReqwestFetcher`'s shared mutex regardless of caller concurrency. |
| `bootstrap_gamma_concurrency` | 10 | Number of in-flight Gamma requests issued concurrently per fetch loop (`buffer_unordered`). With ~300 ms per-request RTT, ~6 in-flight saturates the 20 req/s rate limit; 10 leaves headroom for latency spikes. The global rate cap is still enforced by `bootstrap_gamma_min_interval_ms`. |
| `gamma_batch_size` | 50 | IDs per repeat-key Gamma `/markets` request. Condition lookups use `/markets?condition_ids=A&condition_ids=B…`; token-identity lookups use `/markets?clob_token_ids=A&clob_token_ids=B…`, with the resolver owning chunking and each client call issuing one request of at most this size. Tier-1 live probe (`scripts/probe_gamma_ua.py`, 2026-06-20) confirmed condition repeat-key batching works for **both** the plain (open) and `&closed=true` variants — `n=50/50` and `n=100/100` returned, demux-by-`conditionId` clean, no cross-market leak; comma-separated joining returns 0 (repeat-key is mandatory). Observed cap ≥ 100, but 50 is kept (≈1000 markets/s at the 20 req/s gate) unless a higher cap is separately adopted. |
| `gamma_batch_limit_param` | 500 | `&limit=` value appended to batched Gamma `/markets` requests (issue #382). Live-confirmed (2026-06-20) not to truncate a full batch — a 100-id request returned all 100. Matches the proven `scripts/backfill_end_dates.py:40`. |
| `gamma_browser_ua` | `Mozilla/5.0 (X11; Linux x86_64) prediction-edge/1.0` | Defensive non-bot User-Agent for Gamma/CLOB clients (issue #382). **The 403 gate is the literal `Python-urllib/*` default UA (an anti-bot blocklist), NOT "missing browser UA":** the 2026-06-20 probe showed Gamma and CLOB `&closed=true` return 200 for a headerless request (a bare `reqwest::Client`, = what the shipped Rust clients send), an empty UA, a product UA, and a browser UA — and 403 **only** for `Python-urllib/3.11`. So the deployed Rust clients do not 403; this UA is hardening against a future bot-flagged default, not a correctness fix. |
| `bootstrap_event_orphan_warn_pct` | 99 | `pe-bootstrap events` coverage gate (issue #206): warn if more than 99% of distinct traded markets are orphans (self-mapped). Gamma's /events covers only a curated subset of all condition IDs; a 90–99% orphan rate on a large historical cache is expected and correct. The format-break hard-fail (abort if `events_seen > 0` but `conditions_mapped == 0`) replaces the old percentage-based abort. Const `ORPHAN_WARN_PCT` in `pe_bootstrap::events`. |
| `bootstrap_events_page_max_retries` | 5 | Walk-level retries on a transient / rate-limited Gamma `/events` page after `ReqwestFetcher` exhausts its fast internal attempts. The sequential sweep never skips the failed offset. Exhaustion preserves a typed temporary error and exits with `rank_and_push_tempfail_exit`; fatal HTTP, malformed payload/schema, cache, and invariant errors remain permanent. Const `EVENTS_PAGE_MAX_RETRIES` in `pe_bootstrap::events`. |
| `bootstrap_events_page_retry_base_ms` | 1_000 | Base delay for Gamma `/events` page retry. Exponential delays are capped at 30s; `RateLimited` uses the server's `Retry-After` with a one-second floor. Const `EVENTS_PAGE_RETRY_BASE_MS` in `pe_bootstrap::events`. |
| `bootstrap_clob_base_url` | `https://clob.polymarket.com` | Base URL for the Polymarket CLOB API (`/markets?closed=true` paginated listing) — the **sole** market-resolution source (#369). Override via `PE_CLOB_BASE_URL` for testing against a stub. |
| `bootstrap_clob_concurrency` | 8 | Number of in-flight CLOB requests issued concurrently per fetch loop, mirroring the Gamma `buffer_unordered` pattern. Set via `PE_BOOTSTRAP_CLOB_CONCURRENCY`. Reused as the concurrency for the `prices-history` CLOB backfill. |
| `clob_prices_history_min_interval_ms` | 10 | Minimum milliseconds between CLOB `/prices-history` requests (≈100 req/s, under the documented 1000 req/10s limit), enforced on a **dedicated** `ReqwestFetcher` so it does not loosen the 50 ms Data-API (`/trades`,`/activity`) gate (issue #421 PR4). Canonical constant `CLOB_PRICES_HISTORY_MIN_INTERVAL_MS` in `pe-source-polymarket-public`; override via `PE_BOOTSTRAP_PRICES_HISTORY_MIN_INTERVAL_MS`. |
| `clob_prices_history_fidelity_minutes` | 60 | CLOB `/prices-history` series granularity in minutes — hourly, the coarse pre-resolution series the CLV bake-off needs (testing CLV at 1h/6h/24h before close, not the final tick) (issue #421 PR4). Canonical constant `CLOB_PRICES_HISTORY_FIDELITY_MINUTES` in `pe-source-polymarket-public`; override via `PE_BOOTSTRAP_PRICES_HISTORY_FIDELITY_MINUTES`. |
| `prices_history_window_secs` | 259_200 (72h) | Pre-resolution window fetched per market by the `prices-history` backfill: the CLOB series is pulled over `[close_ref − this, close_ref]`, where `close_ref = market_schedules.end_date_unix ?? market_resolutions.resolved_at_unix`. 72h covers the 1h/6h/24h CLV horizons with margin (issue #421 PR4). Override via `PE_BOOTSTRAP_PRICES_HISTORY_WINDOW_SECS`. |
| `prices_history_token_limit` | 0 (no limit) | Per-run cap on `(market, token)` targets fetched by `pe-bootstrap prices-history` (issue #421 PR4). `0` is unbounded; a positive value bounds one run's memory/time over the ~1.4M-market universe. The backfill is resumable (skips `(market, token)` pairs already in `market_price_history`), so a capped run is re-run to continue. Override via `PE_BOOTSTRAP_PRICES_HISTORY_TOKEN_LIMIT`. |
| `clob_token_coverage_warn_pct` | 90 | Warn floor (percent) for CLOB token→condition coverage of resolved-with-winner markets, checked after a `pe-bootstrap resolutions` run (issue #429). The CLOB closed-markets sweep maps `tokens[].token_id → condition_id` with a positional `outcome_index` for the full resolved universe; a full re-walk maps ~94%+ of winner markets, so coverage below this floor flags genuine token-map starvation and self-heals on the next cycle's automatic full walk. `--reset-clob-cursor` is emergency-only and requires competing writers stopped. `0` disables the warn. Const `DEFAULT_CLOB_TOKEN_COVERAGE_WARN_PCT` in `pe_bootstrap::config`; override via `PE_BOOTSTRAP_CLOB_TOKEN_COVERAGE_WARN_PCT`. |
| `token_conditions.outcome_index` | NULL until mapped | Schema (issue #429): 0-based positional outcome ordinal on the `token_conditions` map (`0`=YES, `1`=NO for binary; positional for multi-outcome). Written by both the CLOB closed-markets sweep (`tokens[]` array position) and the Gamma `events` sweep (`clobTokenIds` array position) — the same authoritative outcome→token order. Nullable: legacy NULL rows upgrade through the conditional upsert on the next cycle's full CLOB walk; the `true_clv` join (#421/#418) skips NULL until then. The CLOB sweep cross-checks its `tokens[]` order against any non-NULL stored value and quarantines a market on divergence rather than overwriting it. |
| `prices_history_coverage_warn_pct` | 60 | Warn floor (percent) for *usable* CLOB price-series coverage of resolved-with-winner markets, checked after a `pe-bootstrap prices-history` run (issue #429 PR3). "Usable" = a market with ≥1 token carrying ≥`prices_history_min_series_points` points. 60 sits just under the ~63.6% usable-series ceiling PR2's `clv_source_comparison` memo measured, so a complete CLOB-only backfill clears it while a starved/partial run (e.g. a pre-re-walk token map) trips it. `0` disables the warn. Const `DEFAULT_PRICES_HISTORY_COVERAGE_WARN_PCT` in `pe_bootstrap::config`; override via `PE_BOOTSTRAP_PRICES_HISTORY_COVERAGE_WARN_PCT`. |
| `prices_history_min_series_points` | 3 | Minimum points in a `(market, token)` series for it to count as "usable" in the price-series coverage ledger (issue #429 PR3). Matches the ≥3-point bar PR2's `clv_source_comparison` memo used (its `MIN_SERIES_POINTS`), so the bootstrap ledger's `usable / total` is comparable to that memo's measured CLOB ceiling. Const `MIN_USABLE_SERIES_POINTS` in `pe_bootstrap::cache`. |
| `market_price_history.source` | `'clob'` | Schema (issue #429 PR3): series provenance on each `market_price_history` row. `'clob'` is the only writer today — the CLOB `/prices-history` backfill — since PR2's `clob_only` verdict dropped the planned trades pass. The column exists so a future trades-derived series is distinguishable and so the coverage ledger can attribute points; `INSERT OR IGNORE` on the `(market_id, token_id, t)` PK makes the write **write-once**, so a later source never overwrites a captured point. Nullable-free (`DEFAULT 'clob'` on the additive migration). |
| `clob_page_max_retries` | 5 | Walk-level retries on a transient / rate-limited CLOB `/markets?closed=true` page fetch before the closed-markets walk aborts (issue #429 follow-up). Sits *on top of* `ReqwestFetcher`'s internal fast retries (`polymarket_max_retries`); rides through *sustained* flakiness (e.g. a minute of `error decoding response body`) so one bad page does not abort the ~1,457-page re-walk. `Transient` and `RateLimited` share this budget (`RateLimited` waits `retry_after`, floored at 1s); `Fatal` (4xx) errors still abort immediately as the permanent `Clob` error (exit 1), as do walk-level response-parse failures. Once the budget is exhausted, the still-transient error returns as the typed temporary `TransientSource` → `rank_and_push_tempfail_exit` (75), so the loop supervisor retries a sustained upstream outage instead of stopping (issue #534). Const `CLOB_PAGE_MAX_RETRIES` in `pe_bootstrap::clob`. |
| `clob_page_retry_base_ms` | 1_000 | Base backoff (ms) for the CLOB page retry; exponential (`base · 2^attempt`) capped at 30s. With `clob_page_max_retries=5` the inter-attempt backoff sums to ~31s (1+2+4+8+16) before giving up — total wall-time per page is higher because each attempt also spends `ReqwestFetcher`'s own retries/timeout; `RateLimited` instead waits the server's `retry_after` (floored at 1s). Const `CLOB_PAGE_RETRY_BASE_MS` in `pe_bootstrap::clob`. |
| `ranker_price_fidelity_minutes` | 1 | Fidelity of the targeted ranker fill-oracle fetch (#536): `RANKER_PRICE_FIDELITY_MINUTES` in `pe_bootstrap::prices_history`. Part of the `ranker_price_pages` coverage identity — a fidelity change invalidates coverage; a code deploy (parser version, provenance-only) does not. |
| `ranker_price_page_max_span_secs` | 80_000 | Maximum requested span per targeted `/prices-history` page (#536): safely under the measured ~1,437-point (~24 h at minute fidelity) END-anchored response cap, so silent truncation cannot occur (80,000 s → ≤ 1,334 points). Const `RANKER_PAGE_MAX_SPAN_SECS`. The endpoint also rejects spans somewhere above 14 days with HTTP 400 at any fidelity; the `interval` enum mode returns empty on resolved markets and is never used. |
| `ranker_price_store` | `ranker_price_points` + `ranker_price_pages` | Isolated minute price-reference store for the pass-2 fill oracle (#536) — deliberately separate from `market_price_history`, whose every `source='clob'` row feeds true-CLV and the mark index. Points are write-once `(token_id, t)`; pages are an append-only validated ledger (`complete`/`empty`, full per-page provenance incl. `raw_sha256`) committed atomically with their points. Coverage = range algebra over terminal pages; a conflicting duplicate point rolls back its whole page. Written only by `pe-bootstrap prices-history --targets-csv` (cache-mutation-locked); read by `latency_shift_rerank.py` (the sole pass-2 oracle since the #536 cutover), whose binary publication gate exits 75 (supervised retry) while any needed window is un-terminal. Pass-2 also writes `oracle_outcomes.csv` (per-position provenance) and `oracle_manifest.json`, whose canonical sha256 the push stores as `ranking_batches.config_hash` with a round-trip check. |
| `ranker_classifier_version` | 2 | **Module const** `RANKER_CLASSIFIER_VERSION` in `bootstrap::cache_migration`. Generation of the derived `ranker_entries_v2` first-entry projection rebuilt from retained complete activity. Distinct from `CACHE_SCHEMA_VERSION_V2` (cache schema) and `FINAL_STAGE_RECORD_VERSION` (finalization receipt format), both unchanged. Newly finalized/installed candidates require the current classifier; hash-bound historical caches retain their authentic classifier generation, with projection rows required to match their certified state. |
| `bootstrap_pile_activation_min_trades` | 100 | Minimum trade count (DB `trade_count` OR Dune `dune_closed_markets`) for a non-infra wallet to be activated in the pile (issue #166). Curation-list membership (Polymarket leaderboard / 502-gap / datadash) bypasses this gate. Hardcoded as `pe_bootstrap::pile::PILE_ACTIVATION_MIN_TRADES`; changing it requires re-migrating the pile. |
| `bootstrap_pipeline_activation_batch_wallets` | 20,000 | Maximum inactive, non-infra, non-tombstoned wallets activated by one zero-argument `rank_and_push.sh` cycle. Discovery and backfill defer the legacy global rule inside this wrapper; `activate-next` owns the single deterministic, transactionally audited batch. If fewer remain it activates all and warns; if none remain it warns and skips. Hardcoded as `pe_bootstrap::pile::PIPELINE_ACTIVATION_BATCH_WALLETS`. |
| `bootstrap_backfill_limit` | 0 (no limit) | Per-run cap on `pe-bootstrap backfill`. `0` processes every wallet whose `last_polymarket_fetch_at` is NULL or older than 1 day. The loop's zero-argument cycles run with `0`; set a positive value only to bound an ad-hoc run. Set via `PE_BOOTSTRAP_BACKFILL_LIMIT`. |
| `bootstrap_backfill_staleness_secs` | 86_400 (1 day) | Per-wallet staleness window for `pe-bootstrap backfill` (issue #166). A wallet is eligible for re-fetch when `last_polymarket_fetch_at IS NULL OR < now - 86_400`. Hardcoded as `pe_bootstrap::pile::BACKFILL_STALENESS_SECS`; each loop cycle's backfill stage re-fetches wallets stale beyond this window. Reused by `pe-bootstrap purge` (#385) as the rule-B freshness gate. |
| `bootstrap_purge_enabled` | `false` | Explicitly arms either direct `pe-bootstrap purge` or direct `purge-infra`; with the default false, both commands are report-only even without `--dry-run` (#544). The production `rank_and_push.sh` never invokes either command. Set `PE_BOOTSTRAP_PURGE_ENABLED=true` only under separate operator authorization; there is no automatic deletion owner. |
| `bootstrap_purge_inactivity_secs` | 1_209_600 (14 days) | Rule-B (dead-weight) dormancy threshold (issue #385): an `is_active=1`, not-eligible wallet refreshed this run is deleted (no tombstone) when its newest trade is older than this. A zero-trade wallet (`MAX(timestamp_unix) IS NULL`) is never matched. `PE_BOOTSTRAP_PURGE_INACTIVITY_SECS` overrides. |
| `bootstrap_purge_loser_tstat_max` | -2.0 | Rule-A (proven-loser) net t-stat ceiling (issue #385): an eligible wallet is deleted **and tombstoned** when `tstat_net <= -2.0 AND mean_net < 0 AND n_eff >= bootstrap_purge_loser_neff_min`. The one `f64` bootstrap config (a t-stat is a statistic, outside the "no raw f64" money/price/probability rule). `PE_BOOTSTRAP_PURGE_LOSER_TSTAT_MAX` overrides. |
| `bootstrap_purge_loser_neff_min` | 20 | Rule-A minimum effective sample size (`n_eff`, Kish) for a proven-loser verdict (issue #385) — guards against tombstoning on a tiny sample. `PE_BOOTSTRAP_PURGE_LOSER_NEFF_MIN` overrides. |
| `bootstrap_purge_bulk_min_wallets` | 5_000 | Delete-set size at/above which a separately authorized armed direct purge uses bulk mode: marker → index drop → delete → reclaim → index rebuild. Normal publication never enters this path. A pre-existing `reclamation_pending` marker is handled only by the recovery-only `pe-bootstrap recover-reclamation` command after operator inspection; that command cannot select or delete wallets and recreates required indexes before clearing the marker. |
| `bootstrap_purge_archive_enabled` | true | Archive-before-DELETE for the armed purge (item 3.7, 2026-07-01 decision record, issue #417): every doomed wallet's `trades`/`wallets`/`leaderboard_snapshots` rows + a both-rules `purge_manifest` census are copied into the sibling archive DB before any destructive step; an archive failure ABORTS the purge (fail-closed). Disable only on a disk-constrained box. `PE_BOOTSTRAP_PURGE_ARCHIVE_ENABLED`. |
| `bootstrap_purge_archive_path` | "" (derived) | Archive DB path; empty derives `<cache_path stem>.purge-archive.db` beside the cache. `PE_BOOTSTRAP_PURGE_ARCHIVE_PATH`. |
| `tombstone_override_sources` | `SRC_LEADERBOARD` (= 16) | The source bits whose re-discovery of a non-infrastructure tombstoned wallet *lifts* the tombstone and re-admits it (issue #385): Polymarket leaderboard (16) only. An `infra` tombstone is never lifted by ordinary discovery, including leaderboard discovery; only the explicit operator command `clear-infra-exclusion --wallet <hex> --confirm` may remove it; the same command also clears a live `wallets.is_infra = 1` flag (the shape the cold probe writes since purge retirement) without recreating or activating the wallet. Datadash (128), 502-gap (64), trades (2), wallet-set-json (1), and the retired Radion bit (32) leave every tombstone intact. |
| `bootstrap_leaderboard_request_interval_ms` | 500 | Minimum milliseconds between Polymarket leaderboard API requests during `pe-bootstrap winner-discovery` (issue #324). Applied per-fetch via `ReqwestFetcher::with_min_interval_ms`. Set via `PE_BOOTSTRAP_LEADERBOARD_REQUEST_INTERVAL_MS`. |
| `bootstrap_leaderboard_top_n` | 50 | Maximum wallets fetched per leaderboard slice during `pe-bootstrap winner-discovery` (#324; all-category in #335). The `/v1/leaderboard` API hard-caps `limit` at 50; larger values are silently truncated server-side (verified live 2026-06-14). Set via `PE_BOOTSTRAP_LEADERBOARD_TOP_N`. |
| `bootstrap_leaderboard_categories` | all 10 | Leaderboard categories swept by `winner-discovery` (#335). Default = `OVERALL, POLITICS, SPORTS, CRYPTO, CULTURE, MENTIONS, WEATHER, ECONOMICS, TECH, FINANCE`. Each is crossed with `{PNL,VOL} × {DAY,WEEK,MONTH,ALL}` (≤ 80 slices); a category the API rejects (4xx) is skipped with a `warn!`. Results dedupe before the pile upsert. Set via `PE_BOOTSTRAP_LEADERBOARD_CATEGORIES` (TOML array of category names). |
| `bootstrap_datadash_api_url` | `https://api.datadash.xyz` | datadash.xyz cohort API base URL — the second wallet-discovery source for `winner-discovery` alongside the Polymarket leaderboard (issue #365; the retired Radion source is gone). **On by default.** Set to `""` (or null) to disable the source (the kill switch). Set via `PE_BOOTSTRAP_DATADASH_API_URL`. |
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
| 5 | 0b0100000 | _(removed — was Radion; gap kept, persisted)_ |
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
 OR (source_bits & 64) != 0   -- in_502_gap
 OR (source_bits & 128) != 0  -- in_datadash (#365)
)
-- bit 32 (in_radion) was dropped when the Radion source was retired; existing
-- radion-tagged rows keep the bit but no longer bypass the activation gate.
```

`is_active` and `is_infra` are sticky under repository-owned automatic rules
(they never decay). Re-running `migrate` (today: `wallet_set.json` plus resident trades; the
Dune CSV importer that set infra flags was removed in #335) only adds source bits; it never
removes a bit or a flag. Exclusions gate acquisition, not ranking: discovery skips tombstoned
wallets, activation requires `is_infra = 0` and no tombstone, backfill selects through
`active_tradeable_wallets`, and a cold-probe flag discards the fetched page, so an excluded wallet
accrues no trades. Rank/export/publication apply only trade-recency and current-eligibility
filters; they do not consult `is_infra` or `purged_wallets`
(`37-WALLET-EXCLUSION-DECISION-RECORD.md`). Wallet fences are enforced by service admission.

The zero-argument rank-and-push pipeline is a controlled exception to the
legacy unbounded activation call sites: `winner-discovery` and `backfill` run
with deferred activation, then one `activate-next` transaction records the
batch in `wallet_activation_batches` and `wallet_activation_batch_wallets`
before setting those exact wallets active. The per-run `activated_wallets.csv`
is derived from that durable batch and can be regenerated with the same batch
ID without consuming another batch. Direct standalone `winner-discovery` and
`backfill` retain the legacy immediate activation behavior.

Direct `purge-infra` remains an operator-only maintenance command. It is report-only while
`bootstrap_purge_enabled=false`; if separately armed, its historical archive/delete and
non-liftable `purged_wallets(reason='infra')` semantics still apply. It is never a publication
stage.

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
| `modeled_polymarket_fee_rate` | 0.04 | Backtest-only, non-promotional exponent-one CLOB fee model. Every BUY calls the venue fee owner once for the final aggregate quantity and signed price, stores the exact modeled fee and aggregate all-in debit, and closes from that aggregate cost. Runtime and qualification never read this setting. Set via `PE_BACKTEST_MODELED_POLYMARKET_FEE_RATE`. |
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
| `per_trade_cap_default` | `mode_default` | Default `PerTradeCap` variant for `WinnerFollowConfig` and backtest: resolves to 25 bps for LiveTiny, 100 bps for Promoted. Override with `PE_BACKTEST_PER_TRADE_CAP=bps:N` or `PE_BACKTEST_PER_TRADE_CAP=unlimited` in backtest. Ordinary service production requires the complete initial hot snapshot before producers; its reviewed `service_config.per_trade_cap` value is `unlimited`. |
| `per_trade_cap_unlimited_resolved_bps` | 10 000 | Effective cap in basis points when `PerTradeCap::Unlimited` is selected (the reviewed #544 activation value). Full bankroll — Kelly fraction is the only size constraint. After activation any valid row (`unlimited`, `mode_default`, `bps:1..=10000`) is accepted; live risk snapshots record the resolved cap and strict replay reuses the recorded value. |
| `sizing_mode_default` | `kelly` | Default for `WinnerFollowConfig.sizing_mode`. `kelly` uses the venue planner's aggregate-fee convergence chain; `dollar` derives conservative principal from `sizing_dollar_usd`; `contract` passes exactly `sizing_contracts` shares and declines rather than shrinking when any cap is exceeded. No mode has a one-contract fallback. The deployed service's reviewed hot snapshot uses `dollar` / `sizing_dollar_usd = 25`; an ordinary-live account with NULL `accounts.live_sizing_mode` falls back to this shared mode, otherwise `accounts.live_sizing_*` overrides it (#516). |
| `price_impact_cap_bps_default` | `100` | Mandatory hot `RuntimeConfig.price_impact_cap_bps` and new-install seed (#544). One recorded CLOB `/book` read feeds the sole tick-aware sized-buy planner. Every monetary cap bounds signed principal plus the conservative fee reserve, whose quantity is derived from that principal and signed limit price rather than accepted independently; unusable evidence, insufficient depth, venue dust, or a cap excess is a typed decline. Boot requires the row; edits accept only `1..=10_000`. |
| `clob_book_hot_path_timeout_secs` | `2` | Timeout for the orchestrator's single mandatory hot-path CLOB `/book` fetch. A timeout fails closed; no cap bypass or haircut fallback remains. |
| `backtest_suppression_warn_threshold_pct` | 30 | Single warn threshold shared by every per-quarter BUY-signal suppression diagnostic (`expiry_filter_suppression_pct`, `high_price_suppression_pct`, …). Logged as a warning when any quarter exceeds it. Hardcoded as `SUPPRESSION_WARN_THRESHOLD` in `crates/backtest/src/simulation.rs`. |
| `backtest_require_known_expiry_default` | `true` | Strict-mode flag for the `max_hours_to_expiry` filter. `true` (default after #137 Sub-PR 3, gated on PR #154 stage 6f raising trade-set schedule coverage to 99.64%) — when both schedule and resolution are absent for a market, the BUY signal fails closed (suppressed). `false` (rollback / legacy) — both-absent allows the trade through. The fallback chain (schedule → resolution → flag) was unified in PR #139; pre-#139 the NULL-schedule path short-circuited to allow regardless of resolution. Set via `PE_BACKTEST_REQUIRE_KNOWN_EXPIRY`. |
| `backtest_max_positions_per_market_default` | `Some(1)` | Cap on concurrent open positions per `market_id` (issue #138). `None` disables the cap entirely. `Some(n)` blocks new BUY signals on any market that already has ≥ `n` open positions across every leader and outcome — leader A on outcome 0 and leader B on outcome 1 of the same binary market count against the same slot. Slot reopens when positions close via SELL or resolution sweep. `NonZeroU32` rejects `0` at deserialize-time so `PE_BACKTEST_MAX_POSITIONS_PER_MARKET=0` is an explicit error. Set via `PE_BACKTEST_MAX_POSITIONS_PER_MARKET`. |
| `backtest_max_signal_price_default` | `Some(0.85)` | Upper-bound cap on slippage-adjusted `fill_price` for BUY copies (issue #142). `None` disables the cap. `Some(cap)` skips any BUY where `fill_price >= cap` — comparison is `>=` (not `>`), so a fill at exactly `cap` is suppressed; strictly conservative. Gates on `fill_price = signal_price × (1 + slippage_rate)` rather than the leader's signal price so a 0.849 + 1% slippage = 0.857 cannot squeak past. High-price contracts have catastrophic payoff geometry (100 bps slippage on a $0.99 contract burns nearly all upside; binary $0/$1 payoff means any miss is total loss). A3 analysis showed +$20.60 oracle lift in-sample at this threshold. Set via `PE_BACKTEST_MAX_SIGNAL_PRICE`. |
| `backtest_min_signal_price_default` | `None` | Lower-bound floor on slippage-adjusted `fill_price` for BUY copies (issue #466 follow-up) — the symmetric companion to `backtest_max_signal_price_default`. `None` (default) disables the floor (legacy behaviour: no lower band). `Some(floor)` skips any BUY where `fill_price < floor` (strict `<`, so a fill at exactly `floor` passes). Gates on the same `fill_price` the cap uses, in the BUY arm before the flat-USD / Kelly sizing branches. Its purpose is to let the #421 bake-off's `--forward-criteria-filter` confine the forward copy to a config's criteria price band `[price_min, price_max]`, so the forward evaluation copies under the SAME band the wallets were selected with. Set via `PE_BACKTEST_MIN_SIGNAL_PRICE`. |
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

- **Held delivery cursor (#511)** — `poll_cursors.last_ts_unix` never advances past a trade
  the orchestrator has not durably marked seen (`min(unseen) − 1`; MAX-upsert holds by not
  writing). Unprovable windows (unparseable row, failed/oversized paged rescan) freeze it.
  Unseen trades WARN after 1h and are never abandoned.
- **Activity clock (#511)** — `poll_cursors.last_activity_unix`: newest trade timestamp ever
  observed per wallet (MAX-only, advances even while the delivery cursor holds). Feeds the
  inactivity knockout; `NULL` falls back to the delivery cursor.
