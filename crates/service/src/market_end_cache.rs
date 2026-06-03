//! In-memory cache for Gamma `endDate` lookups.
//!
//! On every new `(market_id)` the orchestrator sees, it fetches
//! `GET {gamma_base_url}/markets?condition_ids={id}` and stores the result so
//! subsequent signals for the same market skip the network round-trip.
//!
//! Cache semantics:
//! - Absent from map → not yet fetched.
//! - `Some(unix)` → known end date (Unix timestamp, seconds).
//! - `None` → Gamma returned no `endDate`; caller decides whether to allow or block.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::MarketId;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketRaw {
    /// RFC 3339 scheduled close date, e.g. `"2024-11-04T00:00:00Z"`.
    end_date: Option<String>,
}

/// Thread-safe end-date cache shared with the orchestrator.
#[derive(Clone)]
pub struct MarketEndCache {
    inner: Arc<Mutex<HashMap<MarketId, Option<i64>>>>,
    client: reqwest::Client,
    gamma_base_url: String,
}

impl MarketEndCache {
    pub fn new(gamma_base_url: String) -> Self {
        // build() only fails on invalid TLS config; our config has none.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            client,
            gamma_base_url,
        }
    }

    /// Return the cached end date Unix timestamp for `market_id`, fetching from Gamma
    /// if not yet seen.
    ///
    /// Returns `None` when Gamma omits `endDate` or when the network request fails.
    pub async fn end_date_unix(&self, market_id: &MarketId) -> Option<i64> {
        // Fast path: already in cache.
        {
            let map = self.inner.lock().await;
            if let Some(cached) = map.get(market_id) {
                return *cached;
            }
        }

        // Slow path: fetch from Gamma.
        let unix = self.fetch(market_id).await;

        // Store result (even None) so we don't re-fetch on every signal.
        self.inner.lock().await.insert(market_id.clone(), unix);
        unix
    }

    async fn fetch(&self, market_id: &MarketId) -> Option<i64> {
        let url = format!(
            "{}/markets?condition_ids={}",
            self.gamma_base_url, market_id
        );
        let bytes = match self.client.get(&url).send().await {
            Ok(r) => match r.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(%market_id, error = %e, "market-end-cache: response body error");
                    return None;
                }
            },
            Err(e) => {
                warn!(%market_id, error = %e, "market-end-cache: fetch error");
                return None;
            }
        };

        let markets: Vec<GammaMarketRaw> = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                warn!(%market_id, error = %e, "market-end-cache: JSON parse error");
                return None;
            }
        };

        let end_date_str = markets.into_iter().next()?.end_date?;

        time::OffsetDateTime::parse(
            &end_date_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map(|dt| dt.unix_timestamp())
        .map_err(|e| warn!(%market_id, date = %end_date_str, error = %e, "market-end-cache: endDate parse error"))
        .ok()
    }
}
