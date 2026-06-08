//! Polymarket Gamma API client for market resolution data.
//!
//! Endpoint: `GET https://gamma-api.polymarket.com/markets?condition_ids={ID}&closed=true`
//!
//! `closed=true` is required — the plain endpoint silently returns an empty list for
//! resolved markets (see bootstrap `gamma.rs` for the empirical evidence).
//! Rate limit mirrors `bootstrap_gamma_min_interval_ms` in `docs/_GLOSSARY.md`: 50 ms / 20 req/s.

use futures::stream::{self, StreamExt};
use pe_core_types::MarketId;
use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::{info, warn};

/// Canonical Gamma base URL. See `docs/_GLOSSARY.md`.
pub const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
const GAMMA_CONCURRENCY: usize = 10;

/// Resolution prices indexed by outcome_id.
/// YES-wins: `[Decimal::ONE, Decimal::ZERO]`; NO-wins: `[Decimal::ZERO, Decimal::ONE]`.
#[derive(Debug, Clone)]
pub struct MarketResolution {
    pub market_id: MarketId,
    pub outcome_prices: Vec<Decimal>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
    closed: bool,
    /// JSON-encoded decimal-string array, e.g. `"[\"1\",\"0\"]"`.
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<String>,
}

/// Fetches closed-market resolution data from the Polymarket Gamma API.
pub struct GammaResolutionFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> GammaResolutionFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// Fetch resolutions for `market_ids`. Returns only markets that are closed
    /// with valid `outcomePrices`; others are silently skipped.
    pub async fn fetch_closed(
        &self,
        market_ids: &[MarketId],
    ) -> Result<Vec<MarketResolution>, GammaError> {
        if market_ids.is_empty() {
            return Ok(vec![]);
        }
        let total = market_ids.len();
        info!(total, "gamma-pnl: fetching resolutions");

        let fetcher = &self.fetcher;
        let base_url = self.base_url.as_str();
        let mut results = Vec::new();

        let mut stream = stream::iter(market_ids.iter().cloned())
            .map(|mid| async move {
                let url = format!("{base_url}/markets?condition_ids={mid}&closed=true");
                let bytes = fetcher.fetch_page(&url).await;
                (mid, bytes)
            })
            .buffer_unordered(GAMMA_CONCURRENCY);

        while let Some((market_id, result)) = stream.next().await {
            let bytes = match result {
                Ok(b) => b,
                Err(e) => {
                    warn!(%market_id, error = %e, "gamma-pnl: fetch error, skipping");
                    continue;
                }
            };
            if let Some(res) = parse_resolution(&bytes, &market_id) {
                results.push(res);
            }
        }

        info!(
            resolved = results.len(),
            total, "gamma-pnl: resolution fetch done"
        );
        Ok(results)
    }
}

fn parse_resolution(bytes: &[u8], market_id: &MarketId) -> Option<MarketResolution> {
    let markets: Vec<GammaMarket> = serde_json::from_slice(bytes)
        .map_err(|e| warn!(%market_id, error = %e, "gamma-pnl: response parse error"))
        .ok()?;
    let m = markets.first()?;
    if !m.closed {
        return None;
    }
    let prices_str = m.outcome_prices.as_deref()?;
    let outcome_prices = parse_outcome_prices(prices_str)?;
    // Confirm condition_id matches (Gamma may return unrelated rows).
    if m.condition_id != market_id.to_string() {
        warn!(%market_id, returned = %m.condition_id, "gamma-pnl: condition_id mismatch");
        return None;
    }
    Some(MarketResolution {
        market_id: market_id.clone(),
        outcome_prices,
    })
}

/// Parse Gamma's `outcomePrices` field — a JSON-encoded decimal-string array such
/// as `"[\"0.62\",\"0.38\"]"` (open-market mids) or `"[\"1\",\"0\"]"` (resolved) —
/// into `Vec<Decimal>` indexed by `outcome_id`.
///
/// Returns `None` on malformed JSON (logged), so callers skip the market rather
/// than mis-valuing it; individual non-decimal entries fall back to zero. Shared
/// by the resolution parser and the service-tier mid-price cache so the decimal
/// decoding lives in one place.
pub fn parse_outcome_prices(prices_str: &str) -> Option<Vec<Decimal>> {
    let raw: Vec<String> = serde_json::from_str(prices_str)
        .map_err(|e| warn!(error = %e, "gamma: outcomePrices parse error"))
        .ok()?;
    Some(
        raw.iter()
            .map(|s| Decimal::from_str(s).unwrap_or(Decimal::ZERO))
            .collect(),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum GammaError {
    #[error("gamma fetch: {0}")]
    Fetch(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::FixtureFetcher;
    use std::collections::HashMap;

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn parses_closed_market() {
        let mut fixtures = HashMap::new();
        fixtures.insert(
            "https://gamma-api.polymarket.com/markets?condition_ids=0xcond&closed=true".to_string(),
            br#"[{"conditionId":"0xcond","closed":true,"outcomePrices":"[\"1\",\"0\"]","outcomes":"[\"Yes\",\"No\"]"}]"#.to_vec(),
        );
        let fetcher = GammaResolutionFetcher::new(
            DEFAULT_GAMMA_BASE_URL.to_string(),
            FixtureFetcher::new(fixtures),
        );
        let results = fetcher.fetch_closed(&[mid("0xcond")]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome_prices[0], Decimal::ONE);
        assert_eq!(results[0].outcome_prices[1], Decimal::ZERO);
    }

    #[tokio::test]
    async fn skips_open_market() {
        let mut fixtures = HashMap::new();
        fixtures.insert(
            "https://gamma-api.polymarket.com/markets?condition_ids=0xcond&closed=true".to_string(),
            br#"[{"conditionId":"0xcond","closed":false,"outcomePrices":null}]"#.to_vec(),
        );
        let fetcher = GammaResolutionFetcher::new(
            DEFAULT_GAMMA_BASE_URL.to_string(),
            FixtureFetcher::new(fixtures),
        );
        let results = fetcher.fetch_closed(&[mid("0xcond")]).await.unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn parse_outcome_prices_decodes_open_mids() {
        let parsed = parse_outcome_prices(r#"["0.62","0.38"]"#).unwrap();
        assert_eq!(parsed, vec![Decimal::new(62, 2), Decimal::new(38, 2)]);
    }

    #[test]
    fn parse_outcome_prices_none_on_malformed_json() {
        assert!(parse_outcome_prices("not-json").is_none());
    }

    #[test]
    fn parse_outcome_prices_non_decimal_entry_falls_back_to_zero() {
        let parsed = parse_outcome_prices(r#"["x","0.5"]"#).unwrap();
        assert_eq!(parsed, vec![Decimal::ZERO, Decimal::new(5, 1)]);
    }

    #[tokio::test]
    async fn empty_input_returns_empty() {
        let fetcher = GammaResolutionFetcher::new(
            DEFAULT_GAMMA_BASE_URL.to_string(),
            FixtureFetcher::new(HashMap::new()),
        );
        let results = fetcher.fetch_closed(&[]).await.unwrap();
        assert!(results.is_empty());
    }
}
