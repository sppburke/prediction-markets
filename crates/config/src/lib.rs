#![forbid(unsafe_code)]

use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
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
    fn parses_bind_override() {
        let cfg: ServiceConfig = toml::from_str(r#"bind = "0.0.0.0:9000""#).unwrap();
        assert_eq!(cfg.bind, "0.0.0.0:9000");
    }

    #[test]
    fn rejects_unknown_field() {
        let result: Result<ServiceConfig, _> = toml::from_str(r#"unknown = 1"#);
        assert!(result.is_err());
    }
}
