#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Alchemy (or compatible) HTTPS endpoint for Polygon PoS `eth_getLogs` backfill.
    /// Set via `PE_POLYGON_HTTP_URL` environment variable; never committed.
    #[serde(default)]
    pub polygon_http_url: String,

    /// Alchemy (or compatible) WSS endpoint for Polygon PoS `eth_subscribe` live logs.
    /// Set via `PE_POLYGON_WS_URL` environment variable; never committed.
    #[serde(default)]
    pub polygon_ws_url: String,

    /// Blocks to backfill from current head on first run.
    /// Default ≈ 16 months. See `docs/_GLOSSARY.md`: `polygon_backfill_blocks`.
    #[serde(default = "default_backfill_blocks")]
    pub backfill_blocks: u64,

    /// Path to the Polygon block-checkpoint file.
    #[serde(default = "default_checkpoint_path")]
    pub polygon_checkpoint_path: PathBuf,
}

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_backfill_blocks() -> u64 {
    21_000_000
}

fn default_checkpoint_path() -> PathBuf {
    PathBuf::from("./polygon_checkpoint.json")
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            polygon_http_url: String::new(),
            polygon_ws_url: String::new(),
            backfill_blocks: default_backfill_blocks(),
            polygon_checkpoint_path: default_checkpoint_path(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse config: {0}")]
    Toml(#[from] toml::de::Error),
}

pub fn load(path: &Path) -> Result<ServiceConfig, ConfigError> {
    let body = std::fs::read_to_string(path)?;
    let cfg: ServiceConfig = toml::from_str(&body)?;
    Ok(cfg)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_bind_address() {
        let cfg = ServiceConfig::default();
        assert_eq!(cfg.bind, "127.0.0.1:8080");
    }

    #[test]
    fn default_backfill_blocks_value() {
        let cfg = ServiceConfig::default();
        assert_eq!(cfg.backfill_blocks, 21_000_000);
    }

    #[test]
    fn parses_bind_override() {
        let cfg: ServiceConfig = toml::from_str(r#"bind = "0.0.0.0:9000""#).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0:9000");
    }

    #[test]
    fn parses_polygon_urls() {
        let toml = r#"
            polygon_http_url = "https://polygon-mainnet.g.alchemy.com/v2/key"
            polygon_ws_url = "wss://polygon-mainnet.g.alchemy.com/v2/key"
        "#;
        let cfg: ServiceConfig = toml::from_str(toml).unwrap();
        assert!(cfg.polygon_http_url.starts_with("https://"));
        assert!(cfg.polygon_ws_url.starts_with("wss://"));
    }

    #[test]
    fn parses_backfill_override() {
        let cfg: ServiceConfig = toml::from_str(r#"backfill_blocks = 1000000"#).unwrap();
        assert_eq!(cfg.backfill_blocks, 1_000_000);
    }

    #[test]
    fn rejects_unknown_field() {
        let result: Result<ServiceConfig, _> = toml::from_str(r#"unknown = 1"#);
        assert!(result.is_err());
    }
}
