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

    /// Path to the JSON sidecar that tracks settled-market resolutions.
    /// See `docs/_GLOSSARY.md`: `paper_resolutions_path`.
    #[serde(default = "default_paper_resolutions_path")]
    pub paper_resolutions_path: PathBuf,

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

    // ── Copy-entry gate (band-cohort alignment, issue #290) ───────────────────
    /// Path to the JSON sidecar tracking each leader's previously-entered markets,
    /// used by the first-entry gate. See `docs/_GLOSSARY.md`: `wallet_market_history_path`.
    #[serde(default = "default_wallet_market_history_path")]
    pub wallet_market_history_path: PathBuf,

    /// Inclusive lower bound on the leader's entry price for a copy. Decimal string.
    /// See `docs/_GLOSSARY.md`: `entry_gate_price_band_lo`.
    #[serde(default = "default_entry_gate_price_band_lo")]
    pub entry_gate_price_band_lo: String,

    /// Inclusive upper bound on the leader's entry price for a copy. Decimal string.
    /// See `docs/_GLOSSARY.md`: `entry_gate_price_band_hi`.
    #[serde(default = "default_entry_gate_price_band_hi")]
    pub entry_gate_price_band_hi: String,

    /// First-entry gate posture for wallets whose history could not be loaded:
    /// `false` (default) fails open (copies allowed), `true` fails closed (blocked).
    /// See `docs/_GLOSSARY.md`: `entry_gate_fail_closed`.
    #[serde(default)]
    pub entry_gate_fail_closed: bool,

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

fn default_entry_gate_price_band_lo() -> String {
    "0.40".to_string()
}

fn default_entry_gate_price_band_hi() -> String {
    "0.80".to_string()
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

fn default_paper_resolutions_path() -> PathBuf {
    PathBuf::from("./paper_resolutions.json")
}

fn default_gamma_base_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}

const fn default_gamma_resolution_poll_interval_secs() -> u64 {
    3600
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
            paper_resolutions_path: default_paper_resolutions_path(),
            gamma_base_url: default_gamma_base_url(),
            gamma_resolution_poll_interval_secs: default_gamma_resolution_poll_interval_secs(),
            max_resolution_horizon_secs: default_max_resolution_horizon_secs(),
            wallet_market_history_path: default_wallet_market_history_path(),
            entry_gate_price_band_lo: default_entry_gate_price_band_lo(),
            entry_gate_price_band_hi: default_entry_gate_price_band_hi(),
            entry_gate_fail_closed: false,
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
        "paper_resolutions_path",
        "gamma_base_url",
        "gamma_resolution_poll_interval_secs",
        "max_resolution_horizon_secs",
        "wallet_market_history_path",
        "entry_gate_price_band_lo",
        "entry_gate_price_band_hi",
        "entry_gate_fail_closed",
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
        assert_eq!(
            cfg.wallet_market_history_path,
            PathBuf::from("./wallet_market_history.json")
        );
        assert_eq!(cfg.entry_gate_price_band_lo, "0.40");
        assert_eq!(cfg.entry_gate_price_band_hi, "0.80");
        assert!(!cfg.entry_gate_fail_closed);
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
