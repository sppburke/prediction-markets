#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::Deserialize;

/// Top-level service configuration.
///
/// TOML values are the canonical defaults; `PE_*` environment variables override them
/// at runtime (e.g. `PE_POLYGON_HTTP_URL` overrides `polygon_http_url`). Secrets such
/// as RPC endpoints must be supplied via env vars — never committed in a TOML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    // ── HTTP server ──────────────────────────────────────────────────────────
    #[serde(default = "default_bind")]
    pub bind: String,

    // ── Polygon on-chain source ──────────────────────────────────────────────
    /// Alchemy (or compatible) HTTPS endpoint for `eth_getLogs` backfill.
    /// Set via `PE_POLYGON_HTTP_URL`; never committed.
    #[serde(default)]
    pub polygon_http_url: String,

    /// Alchemy (or compatible) WSS endpoint for `eth_subscribe` live logs.
    /// Set via `PE_POLYGON_WS_URL`; never committed.
    #[serde(default)]
    pub polygon_ws_url: String,

    /// Blocks to backfill on first run (≈ 16 months).
    /// See `docs/_GLOSSARY.md`: `polygon_backfill_blocks`.
    #[serde(default = "default_backfill_blocks")]
    pub backfill_blocks: u64,

    /// Path to the Polygon block-checkpoint file.
    #[serde(default = "default_checkpoint_path")]
    pub polygon_checkpoint_path: PathBuf,

    /// Bounded channel capacity for Polygon events.
    /// See `docs/_GLOSSARY.md`: `polygon_channel_capacity`.
    #[serde(default = "default_channel_capacity")]
    pub polygon_channel_capacity: usize,

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

    /// Seconds between Polymarket trade poll rounds (one round = all wallets).
    /// See `docs/_GLOSSARY.md`: `trade_poll_interval_secs`.
    #[serde(default = "default_trade_poll_interval_secs")]
    pub trade_poll_interval_secs: u64,

    // ── Logging / persistence ────────────────────────────────────────────────
    /// Path to the BLAKE3-chained binary event log.
    #[serde(default = "default_event_log_path")]
    pub event_log_path: PathBuf,

    /// Path to the JSONL observability sidecar.
    #[serde(default = "default_jsonl_log_path")]
    pub jsonl_log_path: PathBuf,

    // ── Operator graph ───────────────────────────────────────────────────────
    /// How often (seconds) `OperatorGraphScheduler` rebuilds operator clusters.
    /// See `docs/_GLOSSARY.md`: `operator_graph_rebuild_cadence_secs`.
    #[serde(default = "default_operator_graph_rebuild_cadence_secs")]
    pub operator_graph_rebuild_cadence_secs: u64,

    // ── Strategy ─────────────────────────────────────────────────────────────
    /// Initial bankroll as a decimal string (e.g. `"10000"`). Parsed to
    /// `rust_decimal::Decimal` at startup — no f64.
    #[serde(default = "default_bankroll_usd")]
    pub bankroll_usd: String,

    /// Execution mode: `shadow` | `paper` | `live_tiny` | `promoted`.
    #[serde(default = "default_mode")]
    pub mode: String,
}

// ── Default helpers ───────────────────────────────────────────────────────────

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_backfill_blocks() -> u64 {
    21_000_000
}

fn default_checkpoint_path() -> PathBuf {
    PathBuf::from("./polygon_checkpoint.json")
}

fn default_channel_capacity() -> usize {
    256
}

fn default_polymarket_base_url() -> String {
    "https://data-api.polymarket.com".to_string()
}

fn default_watchlist_size() -> usize {
    20
}

fn default_trade_poll_interval_secs() -> u64 {
    30
}

fn default_event_log_path() -> PathBuf {
    PathBuf::from("./paper.log")
}

fn default_jsonl_log_path() -> PathBuf {
    PathBuf::from("./paper.jsonl")
}

fn default_bankroll_usd() -> String {
    "10000".to_string()
}

fn default_operator_graph_rebuild_cadence_secs() -> u64 {
    60
}

fn default_mode() -> String {
    "paper".to_string()
}

// ── Default impl ──────────────────────────────────────────────────────────────

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            polygon_http_url: String::new(),
            polygon_ws_url: String::new(),
            backfill_blocks: default_backfill_blocks(),
            polygon_checkpoint_path: default_checkpoint_path(),
            polygon_channel_capacity: default_channel_capacity(),
            polymarket_base_url: default_polymarket_base_url(),
            polymarket_channel_capacity: default_channel_capacity(),
            watchlist_size: default_watchlist_size(),
            trade_poll_interval_secs: default_trade_poll_interval_secs(),
            event_log_path: default_event_log_path(),
            jsonl_log_path: default_jsonl_log_path(),
            operator_graph_rebuild_cadence_secs: default_operator_graph_rebuild_cadence_secs(),
            bankroll_usd: default_bankroll_usd(),
            mode: default_mode(),
        }
    }
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("load config: {0}")]
    Figment(Box<figment::Error>),
}

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        ConfigError::Figment(Box::new(e))
    }
}

// ── Loader ────────────────────────────────────────────────────────────────────

/// Load `ServiceConfig` from a TOML file, with `PE_*` env vars overlaid on top.
///
/// `PE_POLYGON_HTTP_URL` overrides `polygon_http_url`, `PE_BANKROLL_USD` overrides
/// `bankroll_usd`, etc. Key matching is case-insensitive after stripping the prefix.
pub fn load(path: &Path) -> Result<ServiceConfig, ConfigError> {
    let cfg: ServiceConfig = Figment::new()
        .merge(Toml::file(path))
        .merge(Env::prefixed("PE_").lowercase(true))
        .extract()?;
    Ok(cfg)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let cfg = ServiceConfig::default();
        assert_eq!(cfg.bind, "127.0.0.1:8080");
        assert_eq!(cfg.backfill_blocks, 21_000_000);
        assert_eq!(cfg.polygon_channel_capacity, 256);
        assert_eq!(cfg.polymarket_channel_capacity, 256);
        assert_eq!(cfg.watchlist_size, 20);
        assert_eq!(cfg.trade_poll_interval_secs, 30);
        assert_eq!(cfg.operator_graph_rebuild_cadence_secs, 60);
        assert_eq!(cfg.bankroll_usd, "10000");
        assert_eq!(cfg.mode, "paper");
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
        let cfg = load(f.path()).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0:9000");
        assert_eq!(cfg.bankroll_usd, "5000");
        assert_eq!(cfg.mode, "shadow");
    }

    #[test]
    fn parses_polygon_urls() {
        use std::io::Write as _;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"polygon_http_url = "https://polygon-mainnet.g.alchemy.com/v2/key"
polygon_ws_url = "wss://polygon-mainnet.g.alchemy.com/v2/key"
"#
        )
        .unwrap();
        let cfg = load(f.path()).unwrap();
        assert!(cfg.polygon_http_url.starts_with("https://"));
        assert!(cfg.polygon_ws_url.starts_with("wss://"));
    }
}
