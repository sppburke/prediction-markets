//! `ServiceConfig` — loaded from an optional TOML file with `PE_*` env var overlay.

use std::path::{Path, PathBuf};

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use pe_strategy_winner_follow::WinnerFollowConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Top-level service configuration.
///
/// ## Loading order (lowest → highest priority)
/// 1. Struct defaults (`#[serde(default)]`).
/// 2. TOML file (when a path is provided as the first CLI argument).
/// 3. `PE_*` environment variables.
///
/// ## TOML structure
/// ```toml
/// bind = "0.0.0.0:8080"
/// bankroll_usd = "5000"
/// mode = "paper"
///
/// [strategy]
/// slippage_rate = "0.01"
/// ```
///
/// Run `pe-service --print-config` to emit the full default configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    // ── HTTP server ──────────────────────────────────────────────────────────
    #[serde(default = "default_bind")]
    pub bind: String,

    // ── Polymarket public source ─────────────────────────────────────────────
    /// Base URL for the Polymarket Data API (no trailing slash).
    #[serde(default = "default_polymarket_base_url")]
    pub polymarket_base_url: String,

    /// Bounded channel capacity for Polymarket trade events.
    /// See `docs/_GLOSSARY.md`: `polymarket_channel_capacity`.
    #[serde(default = "default_channel_capacity")]
    pub polymarket_channel_capacity: usize,

    // ── Wallet polling & position seeding ─────────────────────────────────────
    /// Seconds between Polymarket trade poll rounds.
    /// See `docs/_GLOSSARY.md`: `trade_poll_interval_secs`.
    #[serde(default = "default_trade_poll_interval_secs")]
    pub trade_poll_interval_secs: u64,

    // ── Logging / persistence ────────────────────────────────────────────────
    /// Path to the BLAKE3-chained binary event log.
    #[serde(default = "default_event_log_path")]
    pub event_log_path: PathBuf,

    /// #530: enable the live-data activity websocket as the primary trade
    /// observation path (`wss://ws-live-data.polymarket.com`, attributed
    /// firehose). Boot-owned; default OFF — disabled mode is byte-identical to
    /// poll-only operation, which is also the rollback path. NEVER disable
    /// while a Δ=2 ranking batch is latest (reverse-order rollback: restore a
    /// Δ=20 batch first — see issue #530).
    #[serde(default)]
    pub polymarket_activity_ws_enabled: bool,

    /// #530: append-only event log for raw websocket source frames (watchlist-
    /// filtered), separate from the paper fill log so existing consumers stay
    /// byte-identical. Written durably (append+sync) BEFORE decision delivery.
    #[serde(default = "default_source_event_log_path")]
    pub source_event_log_path: PathBuf,

    /// #530/#546: the calibrated copy budget in seconds. While the websocket path
    /// is enabled, an observation from EITHER source (REST poll or activity
    /// websocket) older than this is admitted for bookkeeping with a typed
    /// no-copy disposition and stages no copy — checked at the early admission
    /// gate and again immediately before dispatch staging, so channel or gate
    /// delay cannot turn a fresh observation into a stale copy. The ranker's
    /// latency shift assumes copies happen at websocket speed, so copying older
    /// observations is the padded-watchlist loss class. Matches the deployed
    /// `LATENCY_SHIFT_SECS`; re-checked at +1 week (issue #530).
    #[serde(default = "default_copy_latency_budget_secs")]
    pub copy_latency_budget_secs: u64,

    /// Base path for the rolling JSONL observability logs. Its directory + file stem name the
    /// full-stream files (`<stem>.<date>.jsonl`); an `errors.<date>.jsonl` (WARN+ERROR only) is
    /// written alongside. Both rotate daily, keeping `log_retention_days` files.
    #[serde(default = "default_jsonl_log_path")]
    pub jsonl_log_path: PathBuf,

    /// Path to the atomically-rewritten `status.json` health snapshot (current bankroll,
    /// positions/fills/settled counts, watchlist size, Supabase RPC count, uptime). The
    /// agent-friendly "how is it doing?" file. See `docs/_GLOSSARY.md`: `status_path`.
    #[serde(default = "default_status_path")]
    pub status_path: PathBuf,

    /// Seconds between `status.json` snapshots. `0` disables the writer. Default: 30.
    /// See `docs/_GLOSSARY.md`: `status_interval_secs`.
    #[serde(default = "default_status_interval_secs")]
    pub status_interval_secs: u64,

    /// Daily-rotated JSONL files kept per sink (full stream + errors). Bounds disk; older
    /// files are deleted. Default: 7. See `docs/_GLOSSARY.md`: `log_retention_days`.
    #[serde(default = "default_log_retention_days")]
    pub log_retention_days: usize,

    // ── Paper trading state ──────────────────────────────────────────────────
    /// Path to the crash-safe paper-state SQLite database.
    /// See `docs/_GLOSSARY.md`: `paper_state_db_path`.
    #[serde(default = "default_paper_state_db_path")]
    pub paper_state_db_path: PathBuf,

    /// One-time captured v1 wallet/market history input used only by the
    /// schema-v2 migration path (#544).
    #[serde(default = "default_legacy_wallet_history_path")]
    pub legacy_wallet_history_path: PathBuf,

    // ── Gamma / resolution polling ───────────────────────────────────────────
    /// Gamma API base URL (no trailing slash). See `docs/_GLOSSARY.md`.
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,

    /// CLOB resolution-discovery cadence in seconds. The public field and `PE_*` environment
    /// key retain their deployed legacy name; Gamma is not a payout source.
    #[serde(default = "default_gamma_resolution_poll_interval_secs")]
    pub gamma_resolution_poll_interval_secs: u64,

    /// Drop entry signals whose market `endDate` is further than this many seconds
    /// into the future. Set to 0 to disable. Default: 172_800 (48 h) — the run28
    /// production TTR ceiling (2026-07-03 cutover, `docs/33` §5: 48 h ≈ 72 h on paired
    /// weekly P&L, so the capital-velocity preference is free; was 72 h per issue #290).
    /// Selection twin: `ranker_ttr_hours` in `docs/_GLOSSARY.md`.
    #[serde(default = "default_max_resolution_horizon_secs")]
    pub max_resolution_horizon_secs: u64,

    /// Drop entry signals whose market resolves *sooner* than this many seconds from
    /// now — a copy cannot realistically fill and hold a market about to resolve. Set
    /// to 0 to disable. Default: 60 (docs/29: the 1-minute copy floor; sub-minute
    /// "breaks down"). See `docs/_GLOSSARY.md`: `min_resolution_horizon_secs`.
    #[serde(default = "default_min_resolution_horizon_secs")]
    pub min_resolution_horizon_secs: u64,

    // ── Copy-entry gate (first-ever-entry; issues #290, #339) ─────────────────
    /// Maximum *current* market price at which a BUY copy will fill, as a decimal string.
    /// Mirrors the issue-#142 backtest `max_signal_price` cap so live sizing matches
    /// backtest: a BUY whose current price is `>=` this is skipped (catastrophic payoff
    /// geometry near $1). Set to `"0"` to disable. See `docs/_GLOSSARY.md`: `max_fill_price`.
    #[serde(default = "default_max_fill_price")]
    pub max_fill_price: String,

    /// Minimum *current* market price at which a BUY copy will fill, as a decimal string —
    /// the run28 entry-band lower bound (0.15), enforced at copy time so selection and
    /// deployment share the filter (the #468 lesson). Mirrors the backtest
    /// `min_signal_price` floor semantics exactly: a BUY whose current price is `<` this
    /// is skipped (the boundary value itself fills). Set to `"0"` to disable.
    /// See `docs/_GLOSSARY.md`: `min_fill_price`.
    #[serde(default = "default_min_fill_price")]
    pub min_fill_price: String,

    /// Watchlist MEMBERSHIP owner between ranking batches (2026-07-03 run28 cutover):
    /// `knockout` (legacy default — membership changes only via knockout + backfill) or
    /// `full_rerank` (the newest ranking batch's configured top-N replaces the live set each batch
    /// transition). Boot-frozen: the maintenance loop is built once at startup, so a mode
    /// change needs a restart — deliberately NOT in `service_config` (that table carries
    /// runtime-mutable knobs only). Parsed fail-fast by `MembershipMode::parse` in
    /// `main.rs`. See `docs/_GLOSSARY.md`: `watchlist_membership_mode`.
    #[serde(default = "default_watchlist_membership_mode")]
    pub watchlist_membership_mode: String,

    // ── Live wallet source (Supabase ranking handoff, issues #339, #370) ──────
    /// Supabase project REST base URL (e.g. `https://<ref>.supabase.co`). This is the
    /// **sole** wallet source (#370): there is no leaderboard/seed fallback, so the service
    /// hard-fails at boot if it resolves empty or unreachable. Set via `PE_SUPABASE_URL`.
    /// See `docs/_GLOSSARY.md`: `supabase_url`.
    #[serde(default)]
    pub supabase_url: String,

    /// Supabase anon (publishable) API key — sent as the `apikey` header. Injected via
    /// `PE_SUPABASE_ANON_KEY` from `.env`; never committed, never logged.
    #[serde(default)]
    pub supabase_anon_key: String,

    /// Supabase service-role (secret) API key — sent as the `Authorization: Bearer`
    /// token, bypassing RLS for the server-side read. Injected via `PE_SUPABASE_SECRET_KEY`
    /// from `.env`; never committed, never logged.
    #[serde(default)]
    pub supabase_secret_key: String,

    /// Seconds between live-watchlist refresh polls against Supabase. The refresh loop is
    /// spawned only when `supabase_url` is non-empty and this is `> 0`. Default: 300.
    /// See `docs/_GLOSSARY.md`: `supabase_refresh_interval_secs`.
    #[serde(default = "default_supabase_refresh_interval_secs")]
    pub supabase_refresh_interval_secs: u64,

    // ── Paper-trade Supabase sink (issue #343) ───────────────────────────────
    /// Enable the best-effort paper-fill / settlement sink to Supabase. Off by default;
    /// the sink is spawned only when this is `true` **and** `supabase_url` is non-empty.
    /// Requires the service-role `supabase_secret_key` — under RLS the anon key can only
    /// read, so anon-only writes 403 (the sink would never persist anything).
    /// `PE_SUPABASE_SINK_ENABLED`. See `docs/_GLOSSARY.md`: `supabase_sink_enabled`.
    #[serde(default)]
    pub supabase_sink_enabled: bool,

    /// Bounded capacity of the trade-path → sink event channel. Drop-on-full (the periodic
    /// reconcile heals drops). Default: 256. See `docs/_GLOSSARY.md`:
    /// `supabase_sink_channel_capacity`.
    #[serde(default = "default_supabase_sink_channel_capacity")]
    pub supabase_sink_channel_capacity: usize,

    /// Seconds between periodic sink reconciles (fill HWM catch-up + full settled re-upsert,
    /// healing any dropped/failed live writes). Default: 300. See `docs/_GLOSSARY.md`:
    /// `supabase_sink_reconcile_interval_secs`.
    #[serde(default = "default_supabase_sink_reconcile_interval_secs")]
    pub supabase_sink_reconcile_interval_secs: u64,

    // ── Supabase authoritative paper-state (issue #397) ───────────────────────
    /// Make Supabase the active financial-era authority (issue #545). After a verified Start,
    /// every fill/resolution uses the Prepared → authority → exact local projection → Final
    /// protocol and an unmatched Prepared is reconciled before successor financial work.
    /// Before Start, financial entry and resolution fail closed. The best-effort analytics sink
    /// is never an active-era financial writer.
    /// Requires the service-role `supabase_secret_key`. Off by default; set explicitly in
    /// `.env`. `PE_SUPABASE_AUTHORITATIVE`. See `docs/_GLOSSARY.md`: `supabase_authoritative`.
    #[serde(default)]
    pub supabase_authoritative: bool,

    // ── Liquidity-at-fill capture (#350 WS2 PR-H) ─────────────────────────────
    /// Bounded capacity of the trade-path → liquidity-snapshot worker channel. Drop-on-full:
    /// a full channel drops the snapshot request so the BUY fill path never blocks (capture
    /// is best-effort analytics). The snapshot worker is spawned under the same gate as the
    /// Supabase sink (`supabase_sink_enabled` + non-empty `supabase_url`). Default: 256.
    /// `PE_SNAPSHOT_CHANNEL_CAPACITY`. See `docs/_GLOSSARY.md`: `snapshot_channel_capacity`.
    #[serde(default = "default_snapshot_channel_capacity")]
    pub snapshot_channel_capacity: usize,

    // ── Watchlist maintenance (#350 WS1 PR-D) ─────────────────────────────────
    /// Seconds between maintenance ticks (inactivity + underperformance knockout + atomic
    /// backfill). `0` disables the tick entirely (skipped, not a zero-duration loop).
    /// Default: 600. See `docs/_GLOSSARY.md`: `maintenance_interval_secs`.
    #[serde(default = "default_maintenance_interval_secs")]
    pub maintenance_interval_secs: u64,

    /// A live wallet idle (no observed trade) for at least this many seconds is evicted,
    /// unless it is a proven winner (then spared up to `inactivity_hard_cap_secs`). The
    /// clock is the admission clock — `max(admission_time, last_observed_trade)` — because
    /// the poll cursor is seeded to `now` at admission. Default: 259_200 (72 h). See
    /// `docs/_GLOSSARY.md`: `inactivity_threshold_secs`.
    #[serde(default = "default_inactivity_threshold_secs")]
    pub inactivity_threshold_secs: u64,

    /// Hard ceiling on sparing a proven winner from inactivity eviction: past this idle
    /// span the wallet is evicted unconditionally (a winner silent for a week is more
    /// likely abandoned than patient). Default: 604_800 (7 d). See `docs/_GLOSSARY.md`:
    /// `inactivity_hard_cap_secs`.
    #[serde(default = "default_inactivity_hard_cap_secs")]
    pub inactivity_hard_cap_secs: u64,

    /// Accepted for configuration compatibility only. Since #588 it has no runtime effect:
    /// membership maintenance reads the latest ranking batch bounded by
    /// `MAX_ACTIVE_WATCHLIST_SIZE` instead of over-fetching freed slots. Default: 10. See
    /// `docs/_GLOSSARY.md`: `bench_overfetch`.
    #[serde(default = "default_bench_overfetch")]
    pub bench_overfetch: usize,

    /// Minimum settled fills before either the underperformance demotion or the
    /// proven-winner inactivity exception applies (no judgement on small samples).
    /// Default: 10. See `docs/_GLOSSARY.md`: `demotion_min_trades`.
    #[serde(default = "default_demotion_min_trades")]
    pub demotion_min_trades: usize,

    /// Empirical-Bernstein confidence level α for the demotion upper-CB and the
    /// proven-winner lower-CB, as a decimal string (parsed to `Decimal` at startup; never
    /// `f64`). Default: `"0.10"`. See `docs/_GLOSSARY.md`: `demotion_cb_alpha`.
    #[serde(default = "default_demotion_cb_alpha")]
    pub demotion_cb_alpha: String,

    /// Trailing window (seconds) for the demotion realized-P&L conjunct: a wallet is
    /// only demotable when its realized P&L over fills settled within this window is
    /// negative, so a big historical winner carries no unbounded bleed allowance.
    /// Default: 2_592_000 (30 d). See `docs/_GLOSSARY.md`: `demotion_pnl_window_secs`.
    #[serde(default = "default_demotion_pnl_window_secs")]
    pub demotion_pnl_window_secs: u64,

    // ── Strategy ─────────────────────────────────────────────────────────────
    /// Initial bankroll as a decimal string (e.g. `"10000"`). Parsed to `Decimal` at startup.
    #[serde(default = "default_bankroll_usd")]
    pub bankroll_usd: String,

    /// Execution mode: `shadow` | `paper`.
    #[serde(default = "default_mode")]
    pub mode: String,

    /// Winner-Follow strategy parameters — all mode fractions, caps, and slippage.
    /// TOML sub-table `[strategy]`. When absent, `WinnerFollowConfig::default()` applies.
    #[serde(default)]
    pub strategy: WinnerFollowConfig,

    // ── Polymarket CLOB (venue-polymarket) ────────────────────────────────────
    /// Polymarket CLOB REST API base URL. Set via `PE_POLYMARKET_CLOB_BASE_URL`.
    #[serde(default = "default_clob_base_url")]
    pub polymarket_clob_base_url: String,

    /// Key-free Polygon JSON-RPC endpoint used only for ordinary-live receipt finality.
    /// Set via `PE_POLYGON_RECEIPT_RPC_URL`.
    #[serde(default = "default_polygon_receipt_rpc_url")]
    pub polygon_receipt_rpc_url: String,
}

// ── Default helpers ───────────────────────────────────────────────────────────

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

const fn default_channel_capacity() -> usize {
    256
}

fn default_polymarket_base_url() -> String {
    "https://data-api.polymarket.com".to_string()
}

const fn default_trade_poll_interval_secs() -> u64 {
    30
}

const fn default_max_resolution_horizon_secs() -> u64 {
    48 * 3600 // 172_800 s = 48 h (run28 production TTR ceiling, 2026-07-03 cutover)
}

const fn default_min_resolution_horizon_secs() -> u64 {
    60 // docs/29: the 1-minute copy floor; sub-minute "breaks down"
}

fn default_max_fill_price() -> String {
    "0.85".to_string()
}

fn default_watchlist_membership_mode() -> String {
    "knockout".to_string() // legacy hold-until-knockout; the cutover sets full_rerank via env
}

fn default_min_fill_price() -> String {
    "0.15".to_string() // run28 entry-band lower bound (2026-07-03 cutover)
}

const fn default_supabase_refresh_interval_secs() -> u64 {
    300
}

const fn default_supabase_sink_channel_capacity() -> usize {
    256
}

const fn default_supabase_sink_reconcile_interval_secs() -> u64 {
    300
}

const fn default_snapshot_channel_capacity() -> usize {
    256
}

const fn default_maintenance_interval_secs() -> u64 {
    600
}

const fn default_inactivity_threshold_secs() -> u64 {
    259_200 // 72 h
}

const fn default_inactivity_hard_cap_secs() -> u64 {
    604_800 // 7 d
}

const fn default_bench_overfetch() -> usize {
    10
}

const fn default_demotion_min_trades() -> usize {
    10
}

fn default_demotion_cb_alpha() -> String {
    "0.10".to_string()
}

const fn default_demotion_pnl_window_secs() -> u64 {
    2_592_000 // 30 d
}

fn default_source_event_log_path() -> PathBuf {
    PathBuf::from("source_events.log")
}

const fn default_copy_latency_budget_secs() -> u64 {
    2
}

fn default_event_log_path() -> PathBuf {
    PathBuf::from("./paper.log")
}

fn default_jsonl_log_path() -> PathBuf {
    PathBuf::from("./paper.jsonl")
}

fn default_status_path() -> PathBuf {
    PathBuf::from("./status.json")
}

const fn default_status_interval_secs() -> u64 {
    30
}

const fn default_log_retention_days() -> usize {
    7
}

fn default_paper_state_db_path() -> PathBuf {
    PathBuf::from("./paper_state.db")
}

fn default_legacy_wallet_history_path() -> PathBuf {
    PathBuf::from("./wallet_market_history.json")
}

fn default_bankroll_usd() -> String {
    "10000".to_string()
}

fn default_mode() -> String {
    "paper".to_string()
}

fn default_clob_base_url() -> String {
    "https://clob.polymarket.com".to_string()
}

fn default_polygon_receipt_rpc_url() -> String {
    "https://polygon.publicnode.com".to_string()
}

fn default_gamma_base_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}

const fn default_gamma_resolution_poll_interval_secs() -> u64 {
    // 2 minutes (issue #343 step 12): settled markets and "just resolved" wins lag
    // actual resolution by ≤2 min instead of ≤1 h. The poll is gated to markets with
    // open unsettled positions and rate-limited (50 ms min-interval), so the ~30×
    // frequency rise is bounded by the open-position set, not the full universe.
    // Canonical default lives in `docs/_GLOSSARY.md`.
    120
}

// ── Default impl ──────────────────────────────────────────────────────────────

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            polymarket_activity_ws_enabled: false,
            source_event_log_path: default_source_event_log_path(),
            copy_latency_budget_secs: default_copy_latency_budget_secs(),
            polymarket_base_url: default_polymarket_base_url(),
            polymarket_channel_capacity: default_channel_capacity(),
            trade_poll_interval_secs: default_trade_poll_interval_secs(),
            event_log_path: default_event_log_path(),
            jsonl_log_path: default_jsonl_log_path(),
            status_path: default_status_path(),
            status_interval_secs: default_status_interval_secs(),
            log_retention_days: default_log_retention_days(),
            paper_state_db_path: default_paper_state_db_path(),
            legacy_wallet_history_path: default_legacy_wallet_history_path(),
            gamma_base_url: default_gamma_base_url(),
            gamma_resolution_poll_interval_secs: default_gamma_resolution_poll_interval_secs(),
            max_resolution_horizon_secs: default_max_resolution_horizon_secs(),
            min_resolution_horizon_secs: default_min_resolution_horizon_secs(),
            max_fill_price: default_max_fill_price(),
            min_fill_price: default_min_fill_price(),
            watchlist_membership_mode: default_watchlist_membership_mode(),
            supabase_url: String::new(),
            supabase_anon_key: String::new(),
            supabase_secret_key: String::new(),
            supabase_refresh_interval_secs: default_supabase_refresh_interval_secs(),
            supabase_sink_enabled: false,
            supabase_sink_channel_capacity: default_supabase_sink_channel_capacity(),
            supabase_sink_reconcile_interval_secs: default_supabase_sink_reconcile_interval_secs(),
            supabase_authoritative: false,
            snapshot_channel_capacity: default_snapshot_channel_capacity(),
            maintenance_interval_secs: default_maintenance_interval_secs(),
            inactivity_threshold_secs: default_inactivity_threshold_secs(),
            inactivity_hard_cap_secs: default_inactivity_hard_cap_secs(),
            bench_overfetch: default_bench_overfetch(),
            demotion_min_trades: default_demotion_min_trades(),
            demotion_cb_alpha: default_demotion_cb_alpha(),
            demotion_pnl_window_secs: default_demotion_pnl_window_secs(),
            bankroll_usd: default_bankroll_usd(),
            mode: default_mode(),
            strategy: WinnerFollowConfig::default(),
            polymarket_clob_base_url: default_clob_base_url(),
            polygon_receipt_rpc_url: default_polygon_receipt_rpc_url(),
        }
    }
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ServiceConfigError {
    #[error("load config: {0}")]
    Figment(Box<figment::Error>),
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl From<figment::Error> for ServiceConfigError {
    fn from(e: figment::Error) -> Self {
        ServiceConfigError::Figment(Box::new(e))
    }
}

// ── Loader ────────────────────────────────────────────────────────────────────

/// Load `ServiceConfig` from an optional TOML file with `PE_*` env vars overlaid.
///
/// When `path` is `Some`, the TOML file is read first; env vars override individual fields.
/// When `path` is `None`, only env vars and struct defaults apply.
pub fn load(path: Option<&Path>) -> Result<ServiceConfig, ServiceConfigError> {
    let mut fig = Figment::new();
    if let Some(p) = path {
        fig = fig.merge(Toml::file(p));
    }
    // Only forward env vars that map to known ServiceConfig fields.
    // PE_BACKTEST_*, PE_BOOTSTRAP_*, PE_DUNE_*, and per-account polygon variants
    // are set in .env for sibling binaries and must not reach the service config
    // (which uses deny_unknown_fields).
    let env = Env::prefixed("PE_").lowercase(true).only(&[
        "bind",
        "polymarket_base_url",
        "polymarket_channel_capacity",
        "trade_poll_interval_secs",
        "event_log_path",
        "polymarket_activity_ws_enabled",
        "source_event_log_path",
        "copy_latency_budget_secs",
        "jsonl_log_path",
        "status_path",
        "status_interval_secs",
        "log_retention_days",
        "paper_state_db_path",
        "legacy_wallet_history_path",
        "gamma_base_url",
        "gamma_resolution_poll_interval_secs",
        "max_resolution_horizon_secs",
        "min_resolution_horizon_secs",
        "max_fill_price",
        "min_fill_price",
        "watchlist_membership_mode",
        "supabase_url",
        "supabase_anon_key",
        "supabase_secret_key",
        "supabase_refresh_interval_secs",
        "supabase_sink_enabled",
        "supabase_sink_channel_capacity",
        "supabase_sink_reconcile_interval_secs",
        "supabase_authoritative",
        "bankroll_usd",
        "mode",
        "strategy",
        "polymarket_clob_base_url",
        "polygon_receipt_rpc_url",
    ]);
    let cfg: ServiceConfig = fig.merge(env).extract()?;
    // #530: the copy budget parameterizes a fail-closed admission rule; an absurd
    // value is a config error, not a posture. One hour is far beyond any honest
    // calibration (the ranker's latency shift is 2s).
    if !valid_copy_latency_budget_secs(cfg.copy_latency_budget_secs) {
        return Err(ServiceConfigError::Invalid(format!(
            "copy_latency_budget_secs must be in 1..=3600, got {}",
            cfg.copy_latency_budget_secs
        )));
    }
    Ok(cfg)
}

/// Shared bound for boot configuration and frozen paper freshness evidence.
#[must_use]
pub fn valid_copy_latency_budget_secs(seconds: u64) -> bool {
    (1..=3_600).contains(&seconds)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn copy_latency_budget_bounds_are_enforced() {
        // #530 review F6: the budget parameterizes a fail-closed rule; absurd
        // values are config errors (0 disables it silently; >1h is nonsense and
        // the huge-u64 wrap class).
        for bad in ["0", "5000", "99999999999999999999"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("svc.toml");
            std::fs::write(&path, format!("copy_latency_budget_secs = {bad}\n")).unwrap();
            assert!(load(Some(&path)).is_err(), "budget {bad} must be rejected");
        }
    }

    #[test]
    fn default_values() {
        let cfg = ServiceConfig::default();
        assert_eq!(cfg.bind, "127.0.0.1:8080");
        assert_eq!(cfg.status_path, PathBuf::from("./status.json"));
        assert_eq!(cfg.status_interval_secs, 30);
        assert_eq!(cfg.log_retention_days, 7);
        assert_eq!(cfg.polymarket_channel_capacity, 256);
        assert_eq!(
            cfg.polygon_receipt_rpc_url,
            "https://polygon.publicnode.com"
        );
        assert_eq!(cfg.trade_poll_interval_secs, 30);
        assert_eq!(cfg.bankroll_usd, "10000");
        assert_eq!(cfg.mode, "paper");
        assert_eq!(cfg.paper_state_db_path, PathBuf::from("./paper_state.db"));
        assert_eq!(
            cfg.legacy_wallet_history_path,
            PathBuf::from("./wallet_market_history.json")
        );
        assert_eq!(cfg.max_resolution_horizon_secs, 172_800);
        assert_eq!(cfg.min_resolution_horizon_secs, 60);
        assert_eq!(cfg.max_fill_price, "0.85");
        assert_eq!(cfg.min_fill_price, "0.15");
        assert_eq!(cfg.watchlist_membership_mode, "knockout");
        assert_eq!(cfg.supabase_url, "");
        assert_eq!(cfg.supabase_anon_key, "");
        assert_eq!(cfg.supabase_secret_key, "");
        assert_eq!(cfg.supabase_refresh_interval_secs, 300);
        assert!(!cfg.supabase_authoritative);
        assert_eq!(cfg.maintenance_interval_secs, 600);
        assert_eq!(cfg.inactivity_threshold_secs, 259_200);
        assert_eq!(cfg.inactivity_hard_cap_secs, 604_800);
        assert_eq!(cfg.bench_overfetch, 10);
        assert_eq!(cfg.demotion_min_trades, 10);
        assert_eq!(cfg.demotion_cb_alpha, "0.10");
    }

    #[test]
    fn figment_loads_toml() {
        use std::io::Write as _;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"bind = "0.0.0.0:9000"
bankroll_usd = "5000"
mode = "shadow"
"#
        )
        .unwrap();
        let cfg = load(Some(f.path())).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0:9000");
        assert_eq!(cfg.bankroll_usd, "5000");
        assert_eq!(cfg.mode, "shadow");
    }

    #[test]
    fn checked_in_service_config_names_the_legacy_history_file() {
        // The version-two migration imports the captured legacy history once
        // from this boot-owned path; the production service keeps the file
        // under `smoke-test/`, not at the working-directory default (#555).
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../smoke-test/service.toml");
        // The checked-in file alone, without the environment overlay `load` applies.
        let cfg: ServiceConfig = Figment::new().merge(Toml::file(&path)).extract().unwrap();
        assert_eq!(
            cfg.legacy_wallet_history_path,
            PathBuf::from("smoke-test/wallet_market_history.json")
        );
    }

    #[test]
    fn supabase_authoritative_flag_loads_from_toml() {
        use std::io::Write as _;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "supabase_authoritative = true").unwrap();
        let cfg = load(Some(f.path())).unwrap();
        assert!(cfg.supabase_authoritative);
    }

    #[test]
    fn retired_watchlist_keys_are_rejected() {
        use std::io::Write as _;
        // #370: `watchlist_size` and `seed_watchlist_path` were removed when Supabase became
        // the sole wallet source. `deny_unknown_fields` makes a stale config carrying either
        // key fail loudly at load — a deploy that forgets to drop them from the live TOML
        // hard-fails fast instead of silently ignoring a now-meaningless setting.
        for stale in ["watchlist_size = 0", "seed_watchlist_path = \"x.json\""] {
            let mut f = tempfile::NamedTempFile::new().unwrap();
            writeln!(f, "{stale}").unwrap();
            assert!(
                load(Some(f.path())).is_err(),
                "stale key must be rejected by deny_unknown_fields: {stale}"
            );
        }
    }

    /// Parse the `service_config` seed rows from the committed schema SQL into key -> value.
    /// Seed values carry no apostrophes, so splitting each row on `'` yields the key at index
    /// 1 and the value at index 3 regardless of the (comma-bearing) description that follows.
    fn parse_service_config_seed(sql: &str) -> std::collections::BTreeMap<String, String> {
        let mut map = std::collections::BTreeMap::new();
        let mut in_block = false;
        for line in sql.lines() {
            let t = line.trim();
            if t.starts_with("insert into service_config") {
                in_block = true;
                continue;
            }
            if in_block {
                if t.starts_with("on conflict") {
                    break;
                }
                if t.starts_with("('") {
                    let parts: Vec<&str> = t.split('\'').collect();
                    if parts.len() >= 4 {
                        map.insert(parts[1].to_string(), parts[3].to_string());
                    }
                }
            }
        }
        map
    }

    #[test]
    fn service_config_seed_matches_boot_defaults() {
        // #544: new installs seed exactly the mandatory hot snapshot. Restart-owned values retain
        // their ServiceConfig TOML/env contracts but have no service_config rows.
        let manifest = env!("CARGO_MANIFEST_DIR");
        let sql = std::fs::read_to_string(format!("{manifest}/../../scripts/supabase_schema.sql"))
            .unwrap();
        let seed = parse_service_config_seed(&sql);
        let expected: std::collections::BTreeSet<_> = crate::runtime_config::HOT_CONFIG_KEYS
            .into_iter()
            .filter(|key| *key != "kelly_fraction_override")
            .map(str::to_owned)
            .collect();
        assert_eq!(
            seed.keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            seed.get("price_impact_cap_bps").map(String::as_str),
            Some("100")
        );
    }
}
