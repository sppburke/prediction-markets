//! `BootstrapConfig` — loaded from an optional TOML file with `PE_*` env var overlay.

use std::path::{Path, PathBuf};

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use pe_source_polymarket_public::LeaderboardCategory;
use serde::{Deserialize, Serialize};

use crate::{
    error::BootstrapError,
    filter::{
        DEFAULT_ACTIVE_WINDOW_DAYS, DEFAULT_MAX_AVG_HOURS_TO_RESOLUTION, DEFAULT_MIN_CLOSED_TRADES,
        DEFAULT_MIN_WIN_RATE_PCT,
    },
};

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const DEFAULT_POLYMARKET_BASE_URL: &str = "https://data-api.polymarket.com";
const DEFAULT_POLYMARKET_CONCURRENCY: usize = 16;
const DEFAULT_POLYMARKET_WALLET_TIMEOUT_SECS: u64 = 300;
const DEFAULT_FUNDER_CONCURRENCY: usize = 4;
const DEFAULT_CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const DEFAULT_CLOB_CONCURRENCY: usize = 8;
// Issue #429: CLOB token→condition coverage warn floor (percent).
const DEFAULT_CLOB_TOKEN_COVERAGE_WARN_PCT: u8 = 90;
// Issue #429 PR3: usable CLOB price-series coverage warn floor (percent). 60 sits just under the
// ~63.6% usable-series ceiling PR2's source-comparison memo measured, so a complete CLOB-only
// backfill clears it while a starved/partial run trips it.
const DEFAULT_PRICES_HISTORY_COVERAGE_WARN_PCT: u8 = 60;
// Issue #324: winner-discovery pipeline defaults.
const DEFAULT_LEADERBOARD_REQUEST_INTERVAL_MS: u64 = 500;
const DEFAULT_LEADERBOARD_TOP_N: u32 = 50;
const DEFAULT_RADION_REQUEST_INTERVAL_MS: u64 = 500;
// Issue #373: Radion trader-analysis discovery defaults.
const DEFAULT_RADION_API_URL: &str = "https://api.radion.app";
const DEFAULT_RADION_MAX_REQUESTS_PER_RUN: u32 = 8;
// Issue #365: datadash.xyz cohort-discovery defaults.
const DEFAULT_DATADASH_API_URL: &str = "https://api.datadash.xyz";
const DEFAULT_DATADASH_REQUEST_INTERVAL_MS: u64 = 500;
const DEFAULT_DATADASH_MAX_COHORT_WALLETS: u64 = 10_000;
// Issue #385: `pe-bootstrap purge` defaults.
const DEFAULT_PURGE_INACTIVITY_SECS: i64 = 1_209_600; // 14 days
const DEFAULT_PURGE_LOSER_TSTAT_MAX: f64 = -2.0;
const DEFAULT_PURGE_LOSER_NEFF_MIN: f64 = 20.0;
const DEFAULT_PURGE_BULK_MIN_WALLETS: u64 = 20_000;

/// Bootstrap configuration loaded from an optional TOML file with `PE_*` env var overlay.
///
/// ## Loading order (lowest → highest priority)
/// 1. Struct defaults (`#[serde(default)]`).
/// 2. TOML file (when a path is provided as the first CLI argument).
/// 3. `PE_*` environment variables.
/// 4. `PE_BOOTSTRAP_*` environment variables (highest priority).
///
/// ## TOML structure
/// ```toml
/// output_path = "/home/user/watchlist.json"
/// cache_path = "/home/user/backtest-data/wallet_cache.db"
/// ```
///
/// Run `pe-bootstrap --print-config` to emit the full default configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    /// Output path for the watchlist JSON. **Required.**
    ///
    /// Set via TOML `output_path = "..."` or env `PE_BOOTSTRAP_OUTPUT`.
    /// Aliases: `output` (PE_BOOTSTRAP_ prefix), `bootstrap_output` (PE_ prefix).
    #[serde(alias = "output", alias = "bootstrap_output")]
    pub output_path: PathBuf,

    /// Path to the wallet trade SQLite cache. `PE_BOOTSTRAP_CACHE_PATH` overrides.
    #[serde(default = "default_cache_path", alias = "bootstrap_cache_path")]
    pub cache_path: PathBuf,

    /// Path to the enumerated wallet address list. `PE_BOOTSTRAP_WALLET_SET_PATH` overrides.
    #[serde(
        default = "default_wallet_set_path",
        alias = "bootstrap_wallet_set_path"
    )]
    pub wallet_set_path: PathBuf,

    /// Trade lookback in days (`None` = unlimited).
    /// Env `PE_BOOTSTRAP_AUDIT_WINDOW_DAYS`: integer, `"unlimited"`, or `"none"`.
    #[serde(
        default,
        alias = "bootstrap_audit_window_days",
        deserialize_with = "deserialize_audit_window"
    )]
    pub audit_window_days: Option<u32>,

    /// Minimum closed trades for post-filter. `PE_BOOTSTRAP_MIN_CLOSED_TRADES` overrides.
    #[serde(
        default = "default_min_closed_trades",
        alias = "bootstrap_min_closed_trades"
    )]
    pub min_closed_trades: usize,

    /// Minimum win-rate percent for post-filter. `PE_BOOTSTRAP_MIN_WIN_RATE_PCT` overrides.
    #[serde(
        default = "default_min_win_rate_pct",
        alias = "bootstrap_min_win_rate_pct"
    )]
    pub min_win_rate_pct: u8,

    /// Recency window in days for post-filter.
    /// `PE_BOOTSTRAP_POST_FILTER_ACTIVE_DAYS` overrides.
    #[serde(
        default = "default_post_filter_active_window_days",
        alias = "post_filter_active_days",
        alias = "bootstrap_post_filter_active_days"
    )]
    pub post_filter_active_window_days: u32,

    /// Maximum average hours to resolution for post-filter.
    /// `PE_BOOTSTRAP_POST_FILTER_MAX_AVG_HOURS` overrides.
    #[serde(
        default = "default_post_filter_max_avg_hours",
        alias = "post_filter_max_avg_hours",
        alias = "bootstrap_post_filter_max_avg_hours"
    )]
    pub post_filter_max_avg_hours_to_resolution: u32,

    /// Base URL for the Polymarket Data API. `PE_POLYMARKET_BASE_URL` overrides.
    #[serde(default = "default_polymarket_base_url")]
    pub polymarket_base_url: String,

    /// Concurrent wallet fetches against the Polymarket Data API.
    /// `PE_BOOTSTRAP_POLYMARKET_CONCURRENCY` overrides.
    #[serde(
        default = "default_polymarket_concurrency",
        alias = "bootstrap_polymarket_concurrency"
    )]
    pub polymarket_concurrency: usize,

    /// Per-wallet wall-clock budget (seconds) for the Polymarket `fetch_all`
    /// loop. `0` disables the timeout; positive values wrap each
    /// `fetch_wallet_incremental` call in `tokio::time::timeout`. Wallets that
    /// trip the timeout are soft-failed (added to `FetchOutcome::failed`) so
    /// the post-fetch pipeline still runs and `last_polymarket_fetch_at`
    /// remains NULL → next backfill re-queues them. Canonical default in
    /// `docs/_GLOSSARY.md` "Bootstrap defaults" section.
    /// `PE_BOOTSTRAP_POLYMARKET_WALLET_TIMEOUT_SECS` overrides.
    #[serde(
        default = "default_polymarket_wallet_timeout_secs",
        alias = "bootstrap_polymarket_wallet_timeout_secs"
    )]
    pub polymarket_wallet_timeout_secs: u64,

    /// Fetch market resolutions from Gamma after trade fetch (off by default).
    /// Env `PE_BOOTSTRAP_FETCH_RESOLUTIONS`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_fetch_resolutions",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub fetch_resolutions: bool,

    /// One-shot retroactive rebuild of `market_resolutions` rows tagged with
    /// imprecise sources (`'gamma'`, `'clob'`). When `true`, the resolution
    /// pipeline deletes those rows before any fetcher runs so the CLOB stage
    /// re-populates the `'clob'` rows. Retained `source='polygon'` rows are NOT
    /// deleted — they keep their exact block-timestamp `resolved_at_unix`.
    ///
    /// **Warning (issue #369):** with the on-chain Polygon scan removed, CLOB is
    /// the only source that re-derives `'clob'` rows — there is no precise
    /// on-chain backfill safety net, so a rebuild re-fetches them from CLOB with
    /// `end_date_iso`-approximate timestamps. Idempotent — safe to set on every run.
    /// Env `PE_BOOTSTRAP_REBUILD_RESOLUTIONS`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_rebuild_resolutions",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub rebuild_resolutions: bool,

    /// Gamma API base URL. `PE_GAMMA_BASE_URL` overrides.
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,

    /// Polymarket CLOB API base URL. Override via `PE_CLOB_BASE_URL` (useful
    /// for testing against a stub).
    #[serde(default = "default_clob_base_url")]
    pub clob_base_url: String,

    /// Number of in-flight CLOB requests issued concurrently per fetch loop.
    /// Mirrors the Gamma fetcher's `buffer_unordered` pattern. Set via
    /// `PE_BOOTSTRAP_CLOB_CONCURRENCY`.
    #[serde(
        default = "default_clob_concurrency",
        alias = "bootstrap_clob_concurrency"
    )]
    pub clob_concurrency: usize,

    /// Warn when the CLOB token→condition coverage of resolved-with-winner
    /// markets falls below this percent after a `resolutions` run (issue #429).
    /// Default 90: a full CLOB closed-markets re-walk maps ~94%+ of winner
    /// markets, so a value below 90 flags genuine token-map starvation (e.g. a
    /// pre-re-walk cache — run `resolutions --reset-clob-cursor`). `0` disables
    /// the warn. Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_CLOB_TOKEN_COVERAGE_WARN_PCT` overrides.
    #[serde(
        default = "default_clob_token_coverage_warn_pct",
        alias = "bootstrap_clob_token_coverage_warn_pct"
    )]
    pub clob_token_coverage_warn_pct: u8,

    /// CLV price-history backfill (issue #421 PR4): the pre-resolution window fetched per market, in
    /// seconds. The CLOB series is pulled over `[close_ref − this, close_ref]` so CLV can be measured
    /// at 1h/6h/24h before close. Default 72h. Env `PE_BOOTSTRAP_PRICES_HISTORY_WINDOW_SECS`.
    #[serde(default = "default_prices_history_window_secs")]
    pub prices_history_window_secs: i64,

    /// CLV price-history series granularity in minutes (issue #421 PR4). Default 60 (hourly, coarse).
    /// Env `PE_BOOTSTRAP_PRICES_HISTORY_FIDELITY_MINUTES`.
    #[serde(default = "default_prices_history_fidelity_minutes")]
    pub prices_history_fidelity_minutes: u32,

    /// Minimum interval (ms) between CLOB `/prices-history` requests for the dedicated fetcher
    /// (issue #421 PR4). Default ~10ms (≈100 req/s, under the 1000 req/10s limit) — kept separate
    /// from the 50ms Data-API gate. Env `PE_BOOTSTRAP_PRICES_HISTORY_MIN_INTERVAL_MS`.
    #[serde(default = "default_prices_history_min_interval_ms")]
    pub prices_history_min_interval_ms: u64,

    /// Max `(market, token)` targets fetched in one `prices-history` run (issue #421 PR4); `0` =
    /// unbounded. Bounds one run's memory/time on the ~1.4M-market universe; the backfill is
    /// resumable, so re-run to continue. Env `PE_BOOTSTRAP_PRICES_HISTORY_TOKEN_LIMIT`.
    #[serde(default = "default_prices_history_token_limit")]
    pub prices_history_token_limit: usize,

    /// Warn when the usable CLOB price-series coverage of resolved-with-winner markets falls below
    /// this percent after a `prices-history` run (issue #429 PR3). "Usable" = ≥1 token with
    /// ≥`MIN_USABLE_SERIES_POINTS` points. Default 60: just under the ~63.6% ceiling PR2's
    /// source-comparison memo measured, so a complete CLOB-only backfill clears it and a
    /// starved/partial one flags. `0` disables the warn. Canonical default in `docs/_GLOSSARY.md`
    /// "Bootstrap defaults". Env `PE_BOOTSTRAP_PRICES_HISTORY_COVERAGE_WARN_PCT`.
    #[serde(
        default = "default_prices_history_coverage_warn_pct",
        alias = "bootstrap_prices_history_coverage_warn_pct"
    )]
    pub prices_history_coverage_warn_pct: u8,

    /// Fetch funder edges via Etherscan after trade fetch (off by default).
    /// Env `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_fetch_funder_graph",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub fetch_funder_graph: bool,

    /// Skip the Polymarket trade-fetch step (off by default).
    /// Env `PE_BOOTSTRAP_SKIP_TRADE_FETCH`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_skip_trade_fetch",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub skip_trade_fetch: bool,

    /// Write a `leaderboard_snapshots` row at run time (off by default).
    ///
    /// When `false` (default), the main bootstrap pipeline still builds the
    /// watchlist for the current run but does **not** persist a row keyed at
    /// `now`. Keeps ad-hoc bootstrap runs (retries from the resolutions
    /// watchdog, dev shells, funder-graph reruns) from polluting the snapshot
    /// timeline with near-duplicate intra-day rows.
    ///
    /// Set to `true` only in the official weekly refresh path, where a single
    /// canonical `(snapshot_at_unix, wallet)` row-set per Sunday is the
    /// intent. Backwards-compatible callers that want the old behavior can
    /// opt in.
    ///
    /// Env `PE_BOOTSTRAP_WRITE_SNAPSHOT`: `"1"` or `"true"` to enable.
    /// (Field is named `write_snapshot` rather than `write_live_snapshot` so
    /// the `PE_BOOTSTRAP_WRITE_SNAPSHOT` env var resolves to a key that
    /// matches the field name after the loader strips its `PE_BOOTSTRAP_`
    /// prefix — see `load()`.)
    #[serde(
        default,
        alias = "bootstrap_write_snapshot",
        alias = "write_live_snapshot",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub write_snapshot: bool,

    /// Concurrent per-wallet funder-discovery fetches against the Etherscan API.
    /// `PE_BOOTSTRAP_FUNDER_CONCURRENCY` overrides.
    #[serde(
        default = "default_funder_concurrency",
        alias = "bootstrap_funder_concurrency"
    )]
    pub funder_concurrency: usize,

    /// Per-run cap on the one-shot `pe-bootstrap funder` lookup (issue #201).
    /// Mirrors `weekly_limit` (the Etherscan-API-budget throttle precedent), NOT
    /// `backfill_limit`'s `0`: a bounded default keeps the one-shot funder short
    /// and avoids the ~15h full-backlog surprise. `0` = no limit (explicit opt-in
    /// for a full run). `PE_BOOTSTRAP_FUNDER_LIMIT` overrides.
    #[serde(default = "default_funder_limit", alias = "bootstrap_funder_limit")]
    pub funder_limit: usize,

    /// Etherscan request-rate cap (req/s) for funder discovery (issue #201).
    /// Default `3` = free-tier budget. Raise on a paid Etherscan tier to shorten
    /// a full funder backlog. `PE_BOOTSTRAP_FUNDER_RATE_LIMIT_RPS` overrides.
    #[serde(
        default = "default_funder_rate_limit_rps",
        alias = "bootstrap_funder_rate_limit_rps"
    )]
    pub funder_rate_limit_rps: u32,

    /// Wallets per `topic[2]` filter in the batched `eth_getLogs` funder scan
    /// (issue #203). Larger batches mean fewer block-range passes but bigger
    /// request payloads (Alchemy caps topic-array size — validate before
    /// raising). `PE_BOOTSTRAP_FUNDER_TOPIC_BATCH_SIZE` overrides.
    #[serde(
        default = "default_funder_topic_batch_size",
        alias = "bootstrap_funder_topic_batch_size"
    )]
    pub funder_topic_batch_size: usize,

    /// Block-range chunk size for the batched `eth_getLogs` funder scan
    /// (issue #203). The bisect-on-cap fallback subdivides dense ranges that
    /// exceed the provider response cap. `PE_BOOTSTRAP_FUNDER_BLOCK_CHUNK` overrides.
    #[serde(
        default = "default_funder_block_chunk",
        alias = "bootstrap_funder_block_chunk"
    )]
    pub funder_block_chunk: u64,

    // ── Wallet pile (issue #166) ─────────────────────────────────────────────
    /// Per-run cap on `pe-bootstrap backfill`. `0` = no limit (process every
    /// due wallet in the queue). Initial deployment runs with `0`; steady-state
    /// daily timers set a positive value. `PE_BOOTSTRAP_BACKFILL_LIMIT` overrides.
    #[serde(default = "default_backfill_limit", alias = "bootstrap_backfill_limit")]
    pub backfill_limit: usize,

    /// Per-run cap on `pe-bootstrap weekly`. `0` = no limit.
    /// `PE_BOOTSTRAP_WEEKLY_LIMIT` overrides.
    #[serde(default = "default_weekly_limit", alias = "bootstrap_weekly_limit")]
    pub weekly_limit: usize,

    // ── Winner-discovery (issue #324) ─────────────────────────────────────────
    /// Base URL for the Polymarket leaderboard endpoint. When absent, falls back
    /// to `polymarket_base_url`. `PE_BOOTSTRAP_LEADERBOARD_BASE_URL` overrides.
    #[serde(default)]
    pub leaderboard_base_url: Option<String>,

    /// Minimum interval (ms) between leaderboard HTTP requests. Default 500.
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_LEADERBOARD_REQUEST_INTERVAL_MS` overrides.
    #[serde(
        default = "default_leaderboard_request_interval_ms",
        alias = "bootstrap_leaderboard_request_interval_ms"
    )]
    pub leaderboard_request_interval_ms: u64,

    /// Top-N entries requested per leaderboard (category × sort × window) slice.
    /// Default 50 — the `/v1/leaderboard` API hard-caps `limit` at 50; larger
    /// values are silently truncated server-side (verified live 2026-06-14).
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_LEADERBOARD_TOP_N` overrides.
    #[serde(
        default = "default_leaderboard_top_n",
        alias = "bootstrap_leaderboard_top_n"
    )]
    pub leaderboard_top_n: u32,

    /// Leaderboard categories to sweep. Default = all ten. Each category is
    /// crossed with {PNL,VOL} × {DAY,WEEK,MONTH,ALL}; results are deduped before
    /// the pile upsert. `PE_BOOTSTRAP_LEADERBOARD_CATEGORIES` overrides (a TOML
    /// array of category names, e.g. `["OVERALL", "CRYPTO"]`).
    #[serde(
        default = "default_leaderboard_categories",
        alias = "bootstrap_leaderboard_categories"
    )]
    pub leaderboard_categories: Vec<LeaderboardCategory>,

    /// Radion REST API base URL. Defaults to `https://api.radion.app` (the source
    /// is **on by default**). Set to `""` (or null) to disable it. Because the API
    /// mandates a key, "on" means *enabled but inert until `radion_api_key` is set*.
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_RADION_API_URL` overrides.
    #[serde(
        default = "default_radion_api_url",
        alias = "bootstrap_radion_api_url",
        deserialize_with = "deserialize_opt_string_empty_none"
    )]
    pub radion_api_url: Option<String>,

    /// Radion API key. Skipped silently when unset/empty — an empty
    /// `PE_BOOTSTRAP_RADION_API_KEY` collapses to `None` (rather than sending an
    /// empty `X-API-Key` and 401-soft-failing every run).
    /// `PE_BOOTSTRAP_RADION_API_KEY` overrides.
    #[serde(
        default,
        alias = "bootstrap_radion_api_key",
        deserialize_with = "deserialize_opt_string_empty_none"
    )]
    pub radion_api_key: Option<String>,

    /// Minimum interval (ms) between Radion HTTP requests. Default 500.
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_RADION_REQUEST_INTERVAL_MS` overrides.
    #[serde(
        default = "default_radion_request_interval_ms",
        alias = "bootstrap_radion_request_interval_ms"
    )]
    pub radion_request_interval_ms: u64,

    /// Max `traders/analysis` requests per discovery run — the Free-tier budget
    /// cap. Default 8 → 8×30 = 240/mo, under the Free 300/mo account cap; at ≤10
    /// wallets/request that is ≤80 wallets/run. Canonical default in
    /// `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_RADION_MAX_REQUESTS_PER_RUN` overrides.
    #[serde(
        default = "default_radion_max_requests_per_run",
        alias = "bootstrap_radion_max_requests_per_run"
    )]
    pub radion_max_requests_per_run: u32,

    // ── datadash.xyz cohort discovery (issue #365) ────────────────────────────
    /// datadash cohort API base URL. Defaults to `https://api.datadash.xyz`
    /// (the source is **on by default**). Set to `""` (or null) to disable it.
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_DATADASH_API_URL` overrides.
    #[serde(
        default = "default_datadash_api_url",
        alias = "bootstrap_datadash_api_url",
        deserialize_with = "deserialize_opt_string_empty_none"
    )]
    pub datadash_api_url: Option<String>,

    /// Minimum interval (ms) between datadash HTTP requests. Default 500.
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_DATADASH_REQUEST_INTERVAL_MS` overrides.
    #[serde(
        default = "default_datadash_request_interval_ms",
        alias = "bootstrap_datadash_request_interval_ms"
    )]
    pub datadash_request_interval_ms: u64,

    /// Cohort ids excluded from ingest (exact match, never substring). Default
    /// drops the ~103k-wallet `Polymarket Twitter/X Linked Traders` cohort.
    /// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    /// `PE_BOOTSTRAP_DATADASH_EXCLUDE_IDS` overrides (a TOML array of ids).
    #[serde(
        default = "default_datadash_exclude_ids",
        alias = "bootstrap_datadash_exclude_ids"
    )]
    pub datadash_exclude_ids: Vec<String>,

    /// Cohort titles excluded from ingest (exact match, never substring). Default
    /// drops `Polymarket Twitter/X Linked Traders` while keeping the distinct
    /// `… with PnL >$100k` cohort. Canonical default in `docs/_GLOSSARY.md`.
    /// `PE_BOOTSTRAP_DATADASH_EXCLUDE_TITLES` overrides (a TOML array of titles).
    #[serde(
        default = "default_datadash_exclude_titles",
        alias = "bootstrap_datadash_exclude_titles"
    )]
    pub datadash_exclude_titles: Vec<String>,

    /// Skip any cohort whose advertised `numWallets` exceeds this cap — a
    /// magnitude safety net so a recreated/misnamed mega-cohort cannot flood the
    /// pile even if the id/title guards drift. Default 10_000. Canonical default
    /// in `docs/_GLOSSARY.md`. `PE_BOOTSTRAP_DATADASH_MAX_COHORT_WALLETS` overrides.
    #[serde(
        default = "default_datadash_max_cohort_wallets",
        alias = "bootstrap_datadash_max_cohort_wallets"
    )]
    pub datadash_max_cohort_wallets: u64,

    // ── pe-bootstrap purge (issue #385) ───────────────────────────────────────
    /// Arm the `pe-bootstrap purge` DELETE. Default `false` keeps the stage
    /// report-only (a dry-run that deletes nothing); the would-purge report still
    /// runs each cycle. Set `PE_BOOTSTRAP_PURGE_ENABLED=1` to arm. Canonical
    /// default in `docs/_GLOSSARY.md` "Bootstrap defaults".
    #[serde(
        default,
        alias = "bootstrap_purge_enabled",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub purge_enabled: bool,

    /// Rule-B dormancy threshold (seconds): a not-eligible active wallet is dead
    /// weight when its newest trade is older than this. Default 1_209_600 (14 d).
    /// Canonical default in `docs/_GLOSSARY.md`. `PE_BOOTSTRAP_PURGE_INACTIVITY_SECS`.
    #[serde(
        default = "default_purge_inactivity_secs",
        alias = "bootstrap_purge_inactivity_secs"
    )]
    pub purge_inactivity_secs: i64,

    /// Rule-A proven-loser net t-stat ceiling: an eligible wallet is a loser when
    /// `tstat_net <= this` (with `mean_net < 0` and `n_eff >= purge_loser_neff_min`).
    /// Default -2.0. This is the first `f64` config field — a t-stat is a statistic,
    /// outside the "no raw f64" money/price/probability rule; `f64` is the natural
    /// type for comparing the CSV's float `tstat_net`. Canonical default in
    /// `docs/_GLOSSARY.md`. `PE_BOOTSTRAP_PURGE_LOSER_TSTAT_MAX`.
    #[serde(
        default = "default_purge_loser_tstat_max",
        alias = "bootstrap_purge_loser_tstat_max"
    )]
    pub purge_loser_tstat_max: f64,

    /// Rule-A minimum effective sample size (`n_eff`) for a proven-loser verdict —
    /// guards against tombstoning on a tiny sample. Default 20. Canonical default in
    /// `docs/_GLOSSARY.md`. `PE_BOOTSTRAP_PURGE_LOSER_NEFF_MIN`.
    #[serde(
        default = "default_purge_loser_neff_min",
        alias = "bootstrap_purge_loser_neff_min"
    )]
    pub purge_loser_neff_min: f64,

    /// Delete-set size (wallet count) at/above which an armed `pe-bootstrap purge`
    /// runs in **bulk mode** — drop the two non-lookup `trades` indexes, delete,
    /// VACUUM, rebuild (issue #401). Below it, an armed purge runs **incremental**:
    /// indexes stay live and no VACUUM runs, so cheap daily purges plateau the
    /// file. Default 20_000. Canonical default in `docs/_GLOSSARY.md`.
    /// `PE_BOOTSTRAP_PURGE_BULK_MIN_WALLETS`.
    #[serde(
        default = "default_purge_bulk_min_wallets",
        alias = "bootstrap_purge_bulk_min_wallets"
    )]
    pub purge_bulk_min_wallets: u64,

    /// Decision CSV (`ranked_72hr_buyandhold.csv`) for the purge verdict. When
    /// unset, the newest `data/eval-results/cron-*/` run is resolved. The pipeline
    /// passes the run's CSV explicitly via `PE_BOOTSTRAP_PURGE_DECISION_CSV`.
    /// `""` (or null) collapses to `None`.
    #[serde(
        default,
        alias = "bootstrap_purge_decision_csv",
        deserialize_with = "deserialize_opt_string_empty_none"
    )]
    pub purge_decision_csv: Option<String>,
}

// ── Default helpers ───────────────────────────────────────────────────────────

fn default_cache_path() -> PathBuf {
    PathBuf::from("wallet_cache.db")
}

fn default_wallet_set_path() -> PathBuf {
    PathBuf::from("wallet_set.json")
}

fn default_min_closed_trades() -> usize {
    DEFAULT_MIN_CLOSED_TRADES
}

const fn default_min_win_rate_pct() -> u8 {
    DEFAULT_MIN_WIN_RATE_PCT
}

const fn default_post_filter_active_window_days() -> u32 {
    DEFAULT_ACTIVE_WINDOW_DAYS
}

const fn default_post_filter_max_avg_hours() -> u32 {
    DEFAULT_MAX_AVG_HOURS_TO_RESOLUTION
}

fn default_polymarket_base_url() -> String {
    DEFAULT_POLYMARKET_BASE_URL.to_owned()
}

const fn default_polymarket_concurrency() -> usize {
    DEFAULT_POLYMARKET_CONCURRENCY
}

const fn default_polymarket_wallet_timeout_secs() -> u64 {
    DEFAULT_POLYMARKET_WALLET_TIMEOUT_SECS
}

const fn default_funder_concurrency() -> usize {
    DEFAULT_FUNDER_CONCURRENCY
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" (issue #201).
const fn default_funder_limit() -> usize {
    200
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" (issue #201).
/// `3` = Etherscan free-tier req/s budget.
const fn default_funder_rate_limit_rps() -> u32 {
    3
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" (issue #203).
const fn default_funder_topic_batch_size() -> usize {
    1_000
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" (issue #203).
const fn default_funder_block_chunk() -> u64 {
    10_000
}

fn default_gamma_base_url() -> String {
    crate::gamma::DEFAULT_GAMMA_BASE_URL.to_owned()
}

// ── Wallet pile (issue #166) ──────────────────────────────────────────────────

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults". `0` = unlimited.
const fn default_backfill_limit() -> usize {
    0
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults".
const fn default_weekly_limit() -> usize {
    200
}

fn default_clob_base_url() -> String {
    DEFAULT_CLOB_BASE_URL.to_owned()
}

const fn default_clob_concurrency() -> usize {
    DEFAULT_CLOB_CONCURRENCY
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" (issue #429).
const fn default_clob_token_coverage_warn_pct() -> u8 {
    DEFAULT_CLOB_TOKEN_COVERAGE_WARN_PCT
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults": 72h pre-resolution CLV window.
const fn default_prices_history_window_secs() -> i64 {
    72 * 60 * 60
}

/// References the canonical `clob_prices_history_fidelity_minutes` constant (no duplicate default).
fn default_prices_history_fidelity_minutes() -> u32 {
    pe_source_polymarket_public::CLOB_PRICES_HISTORY_FIDELITY_MINUTES
}

/// References the canonical `clob_prices_history_min_interval_ms` constant (no duplicate default).
fn default_prices_history_min_interval_ms() -> u64 {
    pe_source_polymarket_public::CLOB_PRICES_HISTORY_MIN_INTERVAL_MS
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults": `0` = unbounded per-run targets.
const fn default_prices_history_token_limit() -> usize {
    0
}

/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" (issue #429 PR3): usable
/// price-series coverage warn floor, just under PR2's measured ~63.6% CLOB ceiling.
const fn default_prices_history_coverage_warn_pct() -> u8 {
    DEFAULT_PRICES_HISTORY_COVERAGE_WARN_PCT
}

const fn default_leaderboard_request_interval_ms() -> u64 {
    DEFAULT_LEADERBOARD_REQUEST_INTERVAL_MS
}

const fn default_leaderboard_top_n() -> u32 {
    DEFAULT_LEADERBOARD_TOP_N
}

/// Default leaderboard sweep set — all ten categories.
fn default_leaderboard_categories() -> Vec<LeaderboardCategory> {
    LeaderboardCategory::ALL.to_vec()
}

const fn default_radion_request_interval_ms() -> u64 {
    DEFAULT_RADION_REQUEST_INTERVAL_MS
}

fn default_radion_api_url() -> Option<String> {
    Some(DEFAULT_RADION_API_URL.to_owned())
}

const fn default_radion_max_requests_per_run() -> u32 {
    DEFAULT_RADION_MAX_REQUESTS_PER_RUN
}

fn default_datadash_api_url() -> Option<String> {
    Some(DEFAULT_DATADASH_API_URL.to_owned())
}

const fn default_datadash_request_interval_ms() -> u64 {
    DEFAULT_DATADASH_REQUEST_INTERVAL_MS
}

/// Default datadash cohort-id exclusions — the ~103k-wallet linked-traders cohort.
fn default_datadash_exclude_ids() -> Vec<String> {
    vec!["07NQHFRAGB6HV".to_owned()]
}

/// Default datadash cohort-title exclusions — the exact linked-traders title (the
/// distinct `… with PnL >$100k` cohort is kept).
fn default_datadash_exclude_titles() -> Vec<String> {
    vec!["Polymarket Twitter/X Linked Traders".to_owned()]
}

const fn default_datadash_max_cohort_wallets() -> u64 {
    DEFAULT_DATADASH_MAX_COHORT_WALLETS
}

const fn default_purge_inactivity_secs() -> i64 {
    DEFAULT_PURGE_INACTIVITY_SECS
}

const fn default_purge_loser_tstat_max() -> f64 {
    DEFAULT_PURGE_LOSER_TSTAT_MAX
}

const fn default_purge_loser_neff_min() -> f64 {
    DEFAULT_PURGE_LOSER_NEFF_MIN
}

const fn default_purge_bulk_min_wallets() -> u64 {
    DEFAULT_PURGE_BULK_MIN_WALLETS
}

// ── Default impl ──────────────────────────────────────────────────────────────

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            output_path: PathBuf::from("./watchlist.json"),
            cache_path: default_cache_path(),
            wallet_set_path: default_wallet_set_path(),
            audit_window_days: None,
            min_closed_trades: default_min_closed_trades(),
            min_win_rate_pct: default_min_win_rate_pct(),
            post_filter_active_window_days: default_post_filter_active_window_days(),
            post_filter_max_avg_hours_to_resolution: default_post_filter_max_avg_hours(),
            polymarket_base_url: default_polymarket_base_url(),
            polymarket_concurrency: default_polymarket_concurrency(),
            polymarket_wallet_timeout_secs: default_polymarket_wallet_timeout_secs(),
            fetch_resolutions: false,
            rebuild_resolutions: false,
            gamma_base_url: default_gamma_base_url(),
            clob_base_url: default_clob_base_url(),
            clob_concurrency: default_clob_concurrency(),
            clob_token_coverage_warn_pct: default_clob_token_coverage_warn_pct(),
            prices_history_window_secs: default_prices_history_window_secs(),
            prices_history_fidelity_minutes: default_prices_history_fidelity_minutes(),
            prices_history_min_interval_ms: default_prices_history_min_interval_ms(),
            prices_history_token_limit: default_prices_history_token_limit(),
            prices_history_coverage_warn_pct: default_prices_history_coverage_warn_pct(),
            fetch_funder_graph: false,
            skip_trade_fetch: false,
            write_snapshot: false,
            funder_concurrency: default_funder_concurrency(),
            funder_limit: default_funder_limit(),
            funder_rate_limit_rps: default_funder_rate_limit_rps(),
            funder_topic_batch_size: default_funder_topic_batch_size(),
            funder_block_chunk: default_funder_block_chunk(),
            backfill_limit: default_backfill_limit(),
            weekly_limit: default_weekly_limit(),
            leaderboard_base_url: None,
            leaderboard_request_interval_ms: default_leaderboard_request_interval_ms(),
            leaderboard_top_n: default_leaderboard_top_n(),
            leaderboard_categories: default_leaderboard_categories(),
            radion_api_url: default_radion_api_url(),
            radion_api_key: None,
            radion_request_interval_ms: default_radion_request_interval_ms(),
            radion_max_requests_per_run: default_radion_max_requests_per_run(),
            datadash_api_url: default_datadash_api_url(),
            datadash_request_interval_ms: default_datadash_request_interval_ms(),
            datadash_exclude_ids: default_datadash_exclude_ids(),
            datadash_exclude_titles: default_datadash_exclude_titles(),
            datadash_max_cohort_wallets: default_datadash_max_cohort_wallets(),
            purge_enabled: false,
            purge_inactivity_secs: default_purge_inactivity_secs(),
            purge_loser_tstat_max: default_purge_loser_tstat_max(),
            purge_loser_neff_min: default_purge_loser_neff_min(),
            purge_bulk_min_wallets: default_purge_bulk_min_wallets(),
            purge_decision_csv: None,
        }
    }
}

// ── Loader ────────────────────────────────────────────────────────────────────

/// Load `BootstrapConfig` from an optional TOML file with `PE_*` env vars overlaid.
///
/// When `path` is `Some`, the TOML file is read first; env vars override individual fields.
/// When `path` is `None`, only env vars and struct defaults apply.
///
/// `PE_BOOTSTRAP_*` env vars take priority over `PE_*` env vars; both are supported.
/// Loads config from an optional TOML file plus the `PE_*` / `PE_BOOTSTRAP_*`
/// env overlay. No API key is required to load config: wallet discovery now runs
/// against the public Polymarket leaderboard (keyless) via
/// `winner_discovery::run_winner_discovery`.
pub fn load(path: Option<&Path>) -> Result<BootstrapConfig, BootstrapError> {
    let mut fig = Figment::new();
    if let Some(p) = path {
        fig = fig.merge(Toml::file(p));
    }
    let cfg: BootstrapConfig = fig
        .merge(
            Env::prefixed("PE_")
                .lowercase(true)
                .filter(|k| !k.starts_with("BOOTSTRAP_")),
        )
        .merge(Env::prefixed("PE_BOOTSTRAP_").lowercase(true))
        .extract()?;
    Ok(cfg)
}

// ── Custom deserializers ──────────────────────────────────────────────────────

fn deserialize_bool_or_01<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = bool;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bool, '0', or '1'")
        }

        fn visit_bool<E: de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<bool, E> {
            match v {
                1 => Ok(true),
                0 => Ok(false),
                other => Err(de::Error::custom(format!(
                    "expected 0 or 1 for bool; got {other}"
                ))),
            }
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<bool, E> {
            match v {
                1 => Ok(true),
                0 => Ok(false),
                other => Err(de::Error::custom(format!(
                    "expected 0 or 1 for bool; got {other}"
                ))),
            }
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<bool, E> {
            match v.trim() {
                "1" | "true" => Ok(true),
                "0" | "false" | "" => Ok(false),
                other => Err(de::Error::custom(format!(
                    "expected '0', '1', 'true', or 'false'; got '{other}'"
                ))),
            }
        }
    }

    d.deserialize_any(V)
}

fn deserialize_audit_window<'de, D>(d: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<u32>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("integer, null, 'unlimited', or 'none'")
        }

        fn visit_none<E: de::Error>(self) -> Result<Option<u32>, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Option<u32>, E> {
            Ok(None)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Option<u32>, E> {
            u32::try_from(v)
                .map(Some)
                .map_err(|_| de::Error::custom(format!("{v} overflows u32")))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Option<u32>, E> {
            u32::try_from(v)
                .map(Some)
                .map_err(|_| de::Error::custom(format!("{v} is not a valid positive integer")))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Option<u32>, E> {
            let v = v.trim().to_lowercase();
            if v.is_empty() || v == "none" || v == "unlimited" {
                return Ok(None);
            }
            v.parse::<u32>()
                .map(Some)
                .map_err(|_| de::Error::custom(format!("'{v}' is not a valid u32 or 'unlimited'")))
        }
    }

    d.deserialize_any(V)
}

/// Deserialize an `Option<String>` where an empty/whitespace string (or null)
/// becomes `None`. Mirrors [`deserialize_audit_window`]'s empty-disables
/// convention so `PE_BOOTSTRAP_DATADASH_API_URL=""` disables the datadash source.
fn deserialize_opt_string_empty_none<'de, D>(d: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<String>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a string, null, or empty string")
        }

        fn visit_none<E: de::Error>(self) -> Result<Option<String>, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Option<String>, E> {
            Ok(None)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Option<String>, E> {
            let t = v.trim();
            if t.is_empty() {
                Ok(None)
            } else {
                Ok(Some(t.to_owned()))
            }
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Option<String>, E> {
            self.visit_str(&v)
        }
    }

    d.deserialize_any(V)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::result_large_err // figment::Error is ~208 bytes; only on test-only Jail closures.
)]
mod tests {
    use super::*;

    /// datadash defaults: the source is on by default (`Some(url)`), the magnitude
    /// cap and exclusions match the canonical `_GLOSSARY.md` values.
    #[test]
    fn datadash_defaults() {
        let cfg = BootstrapConfig::default();
        assert_eq!(
            cfg.datadash_api_url.as_deref(),
            Some("https://api.datadash.xyz"),
            "datadash is on by default"
        );
        assert_eq!(cfg.datadash_request_interval_ms, 500);
        assert_eq!(cfg.datadash_max_cohort_wallets, 10_000);
        assert_eq!(cfg.datadash_exclude_ids, vec!["07NQHFRAGB6HV".to_owned()]);
        assert_eq!(
            cfg.datadash_exclude_titles,
            vec!["Polymarket Twitter/X Linked Traders".to_owned()]
        );
    }

    /// An empty `datadash_api_url` (TOML) deserializes to `None`, disabling the
    /// source — the kill switch.
    #[test]
    fn datadash_empty_url_disables_via_toml() {
        let toml = r#"
            output_path = "/tmp/watchlist.json"
            datadash_api_url = ""
        "#;
        let cfg: BootstrapConfig = Figment::new().merge(Toml::string(toml)).extract().unwrap();
        assert!(
            cfg.datadash_api_url.is_none(),
            "empty datadash_api_url must disable the source"
        );
    }

    /// **Env-var integration**: `PE_BOOTSTRAP_DATADASH_API_URL=""` disables the
    /// source through the production `load()` path (the documented kill switch).
    #[test]
    fn datadash_empty_url_disables_via_env() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
            jail.set_env("PE_BOOTSTRAP_DATADASH_API_URL", "");
            let cfg = load(Some(std::path::Path::new("config.toml")))
                .map_err(|e| figment::Error::from(e.to_string()))?;
            assert!(
                cfg.datadash_api_url.is_none(),
                "PE_BOOTSTRAP_DATADASH_API_URL=\"\" must disable the source"
            );
            Ok(())
        });
    }

    /// CLOB token-coverage warn floor defaults to 90% (issue #429) and round-trips
    /// from `PE_BOOTSTRAP_CLOB_TOKEN_COVERAGE_WARN_PCT`.
    #[test]
    fn clob_token_coverage_warn_pct_default_and_env() {
        assert_eq!(BootstrapConfig::default().clob_token_coverage_warn_pct, 90);
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
            jail.set_env("PE_BOOTSTRAP_CLOB_TOKEN_COVERAGE_WARN_PCT", "75");
            let cfg = load(Some(std::path::Path::new("config.toml")))
                .map_err(|e| figment::Error::from(e.to_string()))?;
            assert_eq!(cfg.clob_token_coverage_warn_pct, 75);
            Ok(())
        });
    }

    /// Price-series coverage warn floor defaults to 60% (issue #429 PR3) and round-trips from
    /// `PE_BOOTSTRAP_PRICES_HISTORY_COVERAGE_WARN_PCT`.
    #[test]
    fn prices_history_coverage_warn_pct_default_and_env() {
        assert_eq!(
            BootstrapConfig::default().prices_history_coverage_warn_pct,
            60
        );
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
            jail.set_env("PE_BOOTSTRAP_PRICES_HISTORY_COVERAGE_WARN_PCT", "55");
            let cfg = load(Some(std::path::Path::new("config.toml")))
                .map_err(|e| figment::Error::from(e.to_string()))?;
            assert_eq!(cfg.prices_history_coverage_warn_pct, 55);
            Ok(())
        });
    }

    /// Default config has the snapshot write gate OFF — keeps ad-hoc bootstrap
    /// runs from polluting `leaderboard_snapshots`. The Sunday weekly refresh
    /// path must explicitly opt in.
    #[test]
    fn write_snapshot_defaults_false() {
        let cfg = BootstrapConfig::default();
        assert!(
            !cfg.write_snapshot,
            "write_snapshot must default to false to keep ad-hoc bootstrap runs \
             from polluting leaderboard_snapshots"
        );
    }

    /// TOML round-trip: `write_snapshot = true` flips the field.
    #[test]
    fn write_snapshot_parses_from_toml_true() {
        let toml = r#"
            output_path = "/tmp/watchlist.json"
            write_snapshot = true
        "#;
        let cfg: BootstrapConfig = Figment::new().merge(Toml::string(toml)).extract().unwrap();
        assert!(cfg.write_snapshot);
    }

    /// TOML round-trip: `write_snapshot = false` is the explicit-opt-out path;
    /// matches the implicit default but exercised here for symmetry.
    #[test]
    fn write_snapshot_parses_from_toml_false() {
        let toml = r#"
            output_path = "/tmp/watchlist.json"
            write_snapshot = false
        "#;
        let cfg: BootstrapConfig = Figment::new().merge(Toml::string(toml)).extract().unwrap();
        assert!(!cfg.write_snapshot);
    }

    /// Backwards-compat alias `write_live_snapshot` (the field's pre-rename
    /// name inside this PR) still parses. Guards against silently dropping
    /// the alias on a future cleanup pass.
    #[test]
    fn write_snapshot_accepts_legacy_alias() {
        let toml = r#"
            output_path = "/tmp/watchlist.json"
            write_live_snapshot = true
        "#;
        let cfg: BootstrapConfig = Figment::new().merge(Toml::string(toml)).extract().unwrap();
        assert!(cfg.write_snapshot);
    }

    /// **Env-var integration**: `PE_BOOTSTRAP_WRITE_SNAPSHOT=1` flips the field
    /// through the same `load()` path production callers use. Guards against
    /// the class of bug where the loader's `PE_BOOTSTRAP_`-prefix strip leaves
    /// a key that does not match the field name (the prior `write_live_snapshot`
    /// field name in this PR was a silent no-op under this env var — caught in
    /// code review on PR #158). Uses `figment::Jail` for deterministic
    /// env-var + cwd isolation.
    #[test]
    fn write_snapshot_set_via_env_var() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
            jail.set_env(
                "PE_BOOTSTRAP_POLYGON_RPC_URL",
                "https://example.invalid/rpc",
            );
            jail.set_env("PE_BOOTSTRAP_WRITE_SNAPSHOT", "1");
            let cfg = load(Some(std::path::Path::new("config.toml")))
                .map_err(|e| figment::Error::from(e.to_string()))?;
            assert!(
                cfg.write_snapshot,
                "PE_BOOTSTRAP_WRITE_SNAPSHOT=1 must flip cfg.write_snapshot to true"
            );
            Ok(())
        });
    }

    /// Env-var integration (off path): no `PE_BOOTSTRAP_WRITE_SNAPSHOT` set →
    /// field stays false. Jail isolates env from the parent process.
    #[test]
    fn write_snapshot_unset_keeps_false() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
            jail.set_env(
                "PE_BOOTSTRAP_POLYGON_RPC_URL",
                "https://example.invalid/rpc",
            );
            let cfg = load(Some(std::path::Path::new("config.toml")))
                .map_err(|e| figment::Error::from(e.to_string()))?;
            assert!(
                !cfg.write_snapshot,
                "no env var set → cfg.write_snapshot must remain false"
            );
            Ok(())
        });
    }
}
