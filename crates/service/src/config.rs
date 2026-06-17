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

    // ── Watchlist ────────────────────────────────────────────────────────────
    /// Top-N leaderboard entries to include in the watchlist.
    /// See `docs/_GLOSSARY.md`: `watchlist_size`.
    #[serde(default = "default_watchlist_size")]
    pub watchlist_size: usize,

    /// Path to the pe-bootstrap-generated Watchlist JSON file.
    /// Empty string means disabled. See `docs/_GLOSSARY.md`: `seed_watchlist_path`.
    #[serde(default)]
    pub seed_watchlist_path: String,

    /// Seconds between Polymarket trade poll rounds.
    /// See `docs/_GLOSSARY.md`: `trade_poll_interval_secs`.
    #[serde(default = "default_trade_poll_interval_secs")]
    pub trade_poll_interval_secs: u64,

    /// Seconds between periodic leader-ledger reseeds from the positions API.
    /// 0 disables periodic reseeds (startup seed still runs).
    /// See `docs/_GLOSSARY.md`: `position_reseed_interval_secs`.
    #[serde(default = "default_position_reseed_interval_secs")]
    pub position_reseed_interval_secs: u64,

    /// Maximum positions to fetch per page when seeding the leader ledger.
    /// See `docs/_GLOSSARY.md`: `position_page_limit`.
    #[serde(default = "default_position_page_limit")]
    pub position_page_limit: u32,

    /// Minimum position size (in contracts) to include in the leader ledger seed.
    /// Positions smaller than this are treated as dust and dropped.
    /// See `docs/_GLOSSARY.md`: `position_size_threshold`.
    #[serde(default = "default_position_size_threshold")]
    pub position_size_threshold: u32,

    // ── Logging / persistence ────────────────────────────────────────────────
    /// Path to the BLAKE3-chained binary event log.
    #[serde(default = "default_event_log_path")]
    pub event_log_path: PathBuf,

    /// Path to the JSONL observability sidecar.
    #[serde(default = "default_jsonl_log_path")]
    pub jsonl_log_path: PathBuf,

    // ── Paper trading state ──────────────────────────────────────────────────
    /// Path to the crash-safe paper-state SQLite database.
    /// See `docs/_GLOSSARY.md`: `paper_state_db_path`.
    #[serde(default = "default_paper_state_db_path")]
    pub paper_state_db_path: PathBuf,

    /// BUY-side paper fill haircut (fee + slippage) in basis points.
    /// See `docs/_GLOSSARY.md`: `paper_fill_haircut_bps`.
    #[serde(default = "default_paper_fill_haircut_bps")]
    pub paper_fill_haircut_bps: u32,

    /// SELL-side paper fill slippage (no taker fee) in basis points.
    /// See `docs/_GLOSSARY.md`: `paper_fill_slippage_bps`.
    #[serde(default = "default_paper_fill_slippage_bps")]
    pub paper_fill_slippage_bps: u32,

    // ── Gamma / resolution polling ───────────────────────────────────────────
    /// Gamma API base URL (no trailing slash). See `docs/_GLOSSARY.md`.
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,

    /// Seconds between Gamma resolution poll rounds.
    /// See `docs/_GLOSSARY.md`: `gamma_resolution_poll_interval_secs`.
    #[serde(default = "default_gamma_resolution_poll_interval_secs")]
    pub gamma_resolution_poll_interval_secs: u64,

    /// Drop entry signals whose market `endDate` is further than this many seconds
    /// into the future. Set to 0 to disable. Default: 259_200 (72 h) — aligned with
    /// the band-cohort "<72 h before resolution" selection criterion (issue #290).
    #[serde(default = "default_max_resolution_horizon_secs")]
    pub max_resolution_horizon_secs: u64,

    /// Drop entry signals whose market resolves *sooner* than this many seconds from
    /// now — a copy cannot realistically fill and hold a market about to resolve. Set
    /// to 0 to disable. Default: 60 (docs/29: the 1-minute copy floor; sub-minute
    /// "breaks down"). See `docs/_GLOSSARY.md`: `min_resolution_horizon_secs`.
    #[serde(default = "default_min_resolution_horizon_secs")]
    pub min_resolution_horizon_secs: u64,

    // ── Copy-entry gate (first-ever-entry; issues #290, #339) ─────────────────
    /// Path to the JSON sidecar tracking each leader's previously-entered markets,
    /// used by the first-entry gate. See `docs/_GLOSSARY.md`: `wallet_market_history_path`.
    #[serde(default = "default_wallet_market_history_path")]
    pub wallet_market_history_path: PathBuf,

    /// First-entry gate posture for wallets whose history could not be loaded:
    /// `false` (default) fails open (copies allowed), `true` fails closed (blocked).
    /// See `docs/_GLOSSARY.md`: `entry_gate_fail_closed`.
    #[serde(default)]
    pub entry_gate_fail_closed: bool,

    /// Maximum *current* market price at which a BUY copy will fill, as a decimal string.
    /// Mirrors the issue-#142 backtest `max_signal_price` cap so live sizing matches
    /// backtest: a BUY whose current price is `>=` this is skipped (catastrophic payoff
    /// geometry near $1). Set to `"0"` to disable. See `docs/_GLOSSARY.md`: `max_fill_price`.
    #[serde(default = "default_max_fill_price")]
    pub max_fill_price: String,

    // ── Live wallet source (Supabase ranking handoff, issue #339) ─────────────
    /// Supabase project REST base URL (e.g. `https://<ref>.supabase.co`). Empty (the
    /// default) disables the live source; the service falls back to `seed_watchlist_path`.
    /// Set via `PE_SUPABASE_URL`. See `docs/_GLOSSARY.md`: `supabase_url`.
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

    /// Extra bench candidates fetched beyond the freed-slot count when backfilling, so a
    /// server-side casing/dedup miss still leaves enough rows to refill the set. Default:
    /// 10. See `docs/_GLOSSARY.md`: `bench_overfetch`.
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

    // ── Strategy ─────────────────────────────────────────────────────────────
    /// Initial bankroll as a decimal string (e.g. `"10000"`). Parsed to `Decimal` at startup.
    #[serde(default = "default_bankroll_usd")]
    pub bankroll_usd: String,

    /// Execution mode: `shadow` | `paper` | `live_tiny` | `promoted`.
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

    /// Funder (EOA) wallet address. Set via `PE_POLYMARKET_FUNDER_ADDRESS`; never committed.
    #[serde(default)]
    pub polymarket_funder_address: String,

    /// Funder EOA private key. Set via `PE_POLYMARKET_PRIVATE_KEY`; never committed.
    #[serde(default)]
    pub polymarket_private_key: String,

    /// Polymarket CLOB API key. Set via `PE_POLYMARKET_CLOB_API_KEY`; never committed.
    #[serde(default)]
    pub polymarket_clob_api_key: String,

    /// Polymarket CLOB API secret, base64-encoded. Set via `PE_POLYMARKET_CLOB_API_SECRET`.
    #[serde(default)]
    pub polymarket_clob_api_secret: String,

    /// Polymarket CLOB API passphrase. Set via `PE_POLYMARKET_CLOB_API_PASSPHRASE`.
    #[serde(default)]
    pub polymarket_clob_api_passphrase: String,
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

const fn default_watchlist_size() -> usize {
    20
}

const fn default_trade_poll_interval_secs() -> u64 {
    30
}

const fn default_max_resolution_horizon_secs() -> u64 {
    72 * 3600 // 259_200 s = 72 h (band-cohort "<72 h before resolution" criterion)
}

fn default_wallet_market_history_path() -> PathBuf {
    PathBuf::from("./wallet_market_history.json")
}

const fn default_min_resolution_horizon_secs() -> u64 {
    60 // docs/29: the 1-minute copy floor; sub-minute "breaks down"
}

fn default_max_fill_price() -> String {
    "0.85".to_string()
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

const fn default_position_reseed_interval_secs() -> u64 {
    300
}

const fn default_position_page_limit() -> u32 {
    500
}

const fn default_position_size_threshold() -> u32 {
    1
}

fn default_event_log_path() -> PathBuf {
    PathBuf::from("./paper.log")
}

fn default_jsonl_log_path() -> PathBuf {
    PathBuf::from("./paper.jsonl")
}

fn default_paper_state_db_path() -> PathBuf {
    PathBuf::from("./paper_state.db")
}

const fn default_paper_fill_haircut_bps() -> u32 {
    500
}

const fn default_paper_fill_slippage_bps() -> u32 {
    100
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
            polymarket_base_url: default_polymarket_base_url(),
            polymarket_channel_capacity: default_channel_capacity(),
            watchlist_size: default_watchlist_size(),
            seed_watchlist_path: String::new(),
            trade_poll_interval_secs: default_trade_poll_interval_secs(),
            position_reseed_interval_secs: default_position_reseed_interval_secs(),
            position_page_limit: default_position_page_limit(),
            position_size_threshold: default_position_size_threshold(),
            event_log_path: default_event_log_path(),
            jsonl_log_path: default_jsonl_log_path(),
            paper_state_db_path: default_paper_state_db_path(),
            paper_fill_haircut_bps: default_paper_fill_haircut_bps(),
            paper_fill_slippage_bps: default_paper_fill_slippage_bps(),
            gamma_base_url: default_gamma_base_url(),
            gamma_resolution_poll_interval_secs: default_gamma_resolution_poll_interval_secs(),
            max_resolution_horizon_secs: default_max_resolution_horizon_secs(),
            min_resolution_horizon_secs: default_min_resolution_horizon_secs(),
            wallet_market_history_path: default_wallet_market_history_path(),
            entry_gate_fail_closed: false,
            max_fill_price: default_max_fill_price(),
            supabase_url: String::new(),
            supabase_anon_key: String::new(),
            supabase_secret_key: String::new(),
            supabase_refresh_interval_secs: default_supabase_refresh_interval_secs(),
            supabase_sink_enabled: false,
            supabase_sink_channel_capacity: default_supabase_sink_channel_capacity(),
            supabase_sink_reconcile_interval_secs: default_supabase_sink_reconcile_interval_secs(),
            snapshot_channel_capacity: default_snapshot_channel_capacity(),
            maintenance_interval_secs: default_maintenance_interval_secs(),
            inactivity_threshold_secs: default_inactivity_threshold_secs(),
            inactivity_hard_cap_secs: default_inactivity_hard_cap_secs(),
            bench_overfetch: default_bench_overfetch(),
            demotion_min_trades: default_demotion_min_trades(),
            demotion_cb_alpha: default_demotion_cb_alpha(),
            bankroll_usd: default_bankroll_usd(),
            mode: default_mode(),
            strategy: WinnerFollowConfig::default(),
            polymarket_clob_base_url: default_clob_base_url(),
            polymarket_funder_address: String::new(),
            polymarket_private_key: String::new(),
            polymarket_clob_api_key: String::new(),
            polymarket_clob_api_secret: String::new(),
            polymarket_clob_api_passphrase: String::new(),
        }
    }
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ServiceConfigError {
    #[error("load config: {0}")]
    Figment(Box<figment::Error>),
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
        "watchlist_size",
        "seed_watchlist_path",
        "trade_poll_interval_secs",
        "position_reseed_interval_secs",
        "position_page_limit",
        "position_size_threshold",
        "event_log_path",
        "jsonl_log_path",
        "paper_state_db_path",
        "paper_fill_haircut_bps",
        "paper_fill_slippage_bps",
        "gamma_base_url",
        "gamma_resolution_poll_interval_secs",
        "max_resolution_horizon_secs",
        "min_resolution_horizon_secs",
        "wallet_market_history_path",
        "entry_gate_fail_closed",
        "max_fill_price",
        "supabase_url",
        "supabase_anon_key",
        "supabase_secret_key",
        "supabase_refresh_interval_secs",
        "supabase_sink_enabled",
        "supabase_sink_channel_capacity",
        "supabase_sink_reconcile_interval_secs",
        "bankroll_usd",
        "mode",
        "strategy",
        "polymarket_clob_base_url",
        "polymarket_funder_address",
        "polymarket_private_key",
        "polymarket_clob_api_key",
        "polymarket_clob_api_secret",
        "polymarket_clob_api_passphrase",
    ]);
    let cfg: ServiceConfig = fig.merge(env).extract()?;
    Ok(cfg)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let cfg = ServiceConfig::default();
        assert_eq!(cfg.bind, "127.0.0.1:8080");
        assert_eq!(cfg.polymarket_channel_capacity, 256);
        assert_eq!(cfg.watchlist_size, 20);
        assert_eq!(cfg.trade_poll_interval_secs, 30);
        assert_eq!(cfg.bankroll_usd, "10000");
        assert_eq!(cfg.mode, "paper");
        assert_eq!(cfg.paper_fill_haircut_bps, 500);
        assert_eq!(cfg.paper_fill_slippage_bps, 100);
        assert_eq!(cfg.paper_state_db_path, PathBuf::from("./paper_state.db"));
        assert_eq!(cfg.position_reseed_interval_secs, 300);
        assert_eq!(cfg.position_page_limit, 500);
        assert_eq!(cfg.position_size_threshold, 1);
        assert_eq!(cfg.max_resolution_horizon_secs, 259_200);
        assert_eq!(cfg.min_resolution_horizon_secs, 60);
        assert_eq!(
            cfg.wallet_market_history_path,
            PathBuf::from("./wallet_market_history.json")
        );
        assert!(!cfg.entry_gate_fail_closed);
        assert_eq!(cfg.max_fill_price, "0.85");
        assert_eq!(cfg.supabase_url, "");
        assert_eq!(cfg.supabase_anon_key, "");
        assert_eq!(cfg.supabase_secret_key, "");
        assert_eq!(cfg.supabase_refresh_interval_secs, 300);
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
}
