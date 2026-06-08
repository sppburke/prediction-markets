//! Figment-based configuration (TOML + `PE_CRYPTO_SHADOW_*` env), mirroring the
//! repo's config-loading convention. Numeric defaults are mirrored in
//! `docs/_GLOSSARY.md` ("BTC shadow harness defaults") per the
//! new-numeric-threshold rule.

use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::{Deserialize, Serialize};

use crate::types::BtcSeriesKind;

fn default_db_path() -> String {
    "crypto_shadow.db".to_string()
}
fn default_gamma_base_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}
fn default_chainlink_ws_url() -> String {
    "wss://ws-live-data.polymarket.com".to_string()
}
fn default_clob_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
}
fn default_channel_capacity() -> usize {
    1024
}
fn default_market_refresh_interval_secs() -> u64 {
    60
}
fn default_max_open_markets() -> usize {
    64
}
fn default_vantage_label() -> String {
    "local".to_string()
}
fn default_rtt_probe_pings() -> u32 {
    5
}
fn default_true() -> bool {
    true
}

/// Harness configuration. See `docs/_GLOSSARY.md` for the canonical defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowConfig {
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,
    #[serde(default = "default_chainlink_ws_url")]
    pub chainlink_ws_url: String,
    #[serde(default = "default_clob_ws_url")]
    pub clob_ws_url: String,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_market_refresh_interval_secs")]
    pub market_refresh_interval_secs: u64,
    #[serde(default = "default_max_open_markets")]
    pub max_open_markets: usize,
    /// Free-text region/host tag stamped into `meta` and the report header so
    /// runs from different locations (local baseline vs colo re-run) compare.
    #[serde(default = "default_vantage_label")]
    pub vantage_label: String,
    #[serde(default = "default_rtt_probe_pings")]
    pub rtt_probe_pings: u32,
    #[serde(default = "default_true")]
    pub track_5m: bool,
    #[serde(default = "default_true")]
    pub track_15m: bool,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            db_path: default_db_path(),
            gamma_base_url: default_gamma_base_url(),
            chainlink_ws_url: default_chainlink_ws_url(),
            clob_ws_url: default_clob_ws_url(),
            channel_capacity: default_channel_capacity(),
            market_refresh_interval_secs: default_market_refresh_interval_secs(),
            max_open_markets: default_max_open_markets(),
            vantage_label: default_vantage_label(),
            rtt_probe_pings: default_rtt_probe_pings(),
            track_5m: default_true(),
            track_15m: default_true(),
        }
    }
}

impl ShadowConfig {
    /// The series this run tracks.
    pub fn series(&self) -> Vec<BtcSeriesKind> {
        let mut v = Vec::new();
        if self.track_5m {
            v.push(BtcSeriesKind::Five);
        }
        if self.track_15m {
            v.push(BtcSeriesKind::Fifteen);
        }
        v
    }
}

/// Configuration load error.
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

/// Load config from an optional TOML file overlaid with `PE_CRYPTO_SHADOW_*`
/// env vars. A missing TOML path is tolerated (figment default).
pub fn load(path: Option<&Path>) -> Result<ShadowConfig, ConfigError> {
    let mut fig = Figment::new();
    if let Some(p) = path {
        fig = fig.merge(Toml::file(p));
    }
    let cfg: ShadowConfig = fig
        .merge(Env::prefixed("PE_CRYPTO_SHADOW_").lowercase(true))
        .extract()?;
    Ok(cfg)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load_without_a_file() {
        let cfg = load(None).unwrap();
        assert_eq!(cfg.channel_capacity, 1024);
        assert_eq!(cfg.max_open_markets, 64);
        assert_eq!(cfg.vantage_label, "local");
        assert_eq!(cfg.series().len(), 2);
    }

    #[test]
    #[allow(clippy::result_large_err)] // figment::Jail::expect_with dictates the closure's Result type
    fn env_overrides_default() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("PE_CRYPTO_SHADOW_VANTAGE_LABEL", "aws-us-east-1");
            jail.set_env("PE_CRYPTO_SHADOW_MAX_OPEN_MARKETS", "8");
            let cfg = load(None).unwrap();
            assert_eq!(cfg.vantage_label, "aws-us-east-1");
            assert_eq!(cfg.max_open_markets, 8);
            Ok(())
        });
    }

    #[test]
    fn series_selection_respects_flags() {
        let cfg = ShadowConfig {
            track_5m: true,
            track_15m: false,
            ..ShadowConfig::default()
        };
        assert_eq!(cfg.series(), vec![BtcSeriesKind::Five]);
    }
}
