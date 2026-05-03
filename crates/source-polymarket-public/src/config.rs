//! Polling configuration for the Polymarket public source.

use std::collections::HashMap;

use crate::endpoint::PolymarketEndpoint;

/// Per-endpoint polling configuration.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// How often to poll this endpoint (seconds).
    pub interval_secs: u64,
}

/// Top-level polling configuration for the Polymarket public source.
#[derive(Debug, Clone)]
pub struct PollingConfig {
    /// API base URL (e.g. `"https://data-api.polymarket.com"`).
    pub base_url: String,
    /// Per-endpoint overrides; endpoints not listed use `default_interval_secs`.
    pub endpoint_configs: HashMap<&'static str, EndpointConfig>,
    /// Default interval when no per-endpoint config exists.
    pub default_interval_secs: u64,
}

impl PollingConfig {
    /// Return the polling interval for `endpoint`, falling back to the default.
    pub fn interval_secs(&self, endpoint: &PolymarketEndpoint) -> u64 {
        self.endpoint_configs
            .get(endpoint.key())
            .map(|c| c.interval_secs)
            .unwrap_or(self.default_interval_secs)
    }
}
