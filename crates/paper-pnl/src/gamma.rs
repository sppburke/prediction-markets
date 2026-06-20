//! Polymarket Gamma API client for market resolution data.
//!
//! Endpoint: `GET https://gamma-api.polymarket.com/markets?condition_ids={ID…}&closed=true`.
//! `&closed=true` is required — the plain endpoint silently returns an empty list for resolved
//! markets. As of issue #382 Phase 3a this delegates to the shared batched
//! [`GammaMarketsClient`](pe_source_polymarket_public::GammaMarketsClient), which fetches many
//! condition_ids per request via repeat-key batching and demuxes by `conditionId`. Throughput is
//! gated at 20 req/s by the injected [`ReqwestFetcher`](pe_source_polymarket_public::ReqwestFetcher)
//! exactly as before; batching cuts the per-tick request count.

use pe_core_types::MarketId;
use pe_source_polymarket_public::{GammaMarketsClient, MarketFilter, PageFetcher};
use rust_decimal::Decimal;
use tracing::{info, warn};

/// Canonical Gamma base URL. See `docs/_GLOSSARY.md`.
pub const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";

/// Resolution prices indexed by outcome_id.
/// YES-wins: `[Decimal::ONE, Decimal::ZERO]`; NO-wins: `[Decimal::ZERO, Decimal::ONE]`.
#[derive(Debug, Clone)]
pub struct MarketResolution {
    pub market_id: MarketId,
    pub outcome_prices: Vec<Decimal>,
}

/// Fetches closed-market resolution data from the Polymarket Gamma API.
pub struct GammaResolutionFetcher<F: PageFetcher> {
    client: GammaMarketsClient<F>,
}

impl<F: PageFetcher + Send + Sync> GammaResolutionFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            client: GammaMarketsClient::new(base_url, fetcher),
        }
    }

    /// Fetch resolutions for `market_ids` via one batched `&closed=true` pass. Returns only markets
    /// that are **closed** with valid `outcomePrices`; open markets, unknown markets, and markets
    /// with malformed prices are skipped.
    ///
    /// Resilient by design: `fetch_closed` never returns `Err` (the always-`Ok` contract the pre-#382
    /// per-ID loop also upheld). A batch-level failure (a transient fetch error or a corrupt response)
    /// logs and yields an empty result for this whole tick — coarser than the old per-market skip, but
    /// the 2-minute poller retries so no resolution is lost. Ids in a 4xx chunk are likewise skipped
    /// and retried; their count is surfaced via `unfetched` in the completion log.
    pub async fn fetch_closed(
        &self,
        market_ids: &[MarketId],
    ) -> Result<Vec<MarketResolution>, GammaError> {
        if market_ids.is_empty() {
            return Ok(vec![]);
        }
        let total = market_ids.len();
        info!(total, "gamma-pnl: fetching resolutions (batched)");

        let ids: Vec<String> = market_ids.iter().map(|m| m.to_string()).collect();
        let fetched = match self
            .client
            .fetch_markets(&ids, MarketFilter::ClosedOnly)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                // The poller must never abort — skip this tick, retry next. `e` is either a transient
                // fetch failure or a corrupt-response parse error; both are non-fatal here.
                warn!(error = %e, "gamma-pnl: batch resolution fetch error, skipping this tick");
                return Ok(vec![]);
            }
        };

        let mut results = Vec::new();
        for market_id in market_ids {
            // `markets` is keyed by the echoed `conditionId`, so a hit is structurally the requested
            // market — a cross-market row would be keyed under its own id and never matched here
            // (the per-ID code's explicit condition_id check, now enforced by the demux).
            let Some(m) = fetched.markets.get(&market_id.to_string()) else {
                continue;
            };
            // Defensive: `&closed=true` should guarantee this, but preserve the original guard.
            if !m.closed {
                continue;
            }
            // No / malformed `outcomePrices` → skip rather than mis-value the market.
            let Some(outcome_prices) = m.outcome_prices.clone() else {
                continue;
            };
            results.push(MarketResolution {
                market_id: market_id.clone(),
                outcome_prices,
            });
        }

        info!(
            resolved = results.len(),
            unfetched = fetched.unfetched.len(),
            total,
            "gamma-pnl: resolution fetch done"
        );
        Ok(results)
    }
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

    const BASE: &str = "https://gamma-api.polymarket.com";

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    /// The shared client requests the batched `&closed=true&limit=500` URL — these helpers build the
    /// exact keys FixtureFetcher expects for a single-id batch.
    fn closed_url(id: &str) -> String {
        format!("{BASE}/markets?condition_ids={id}&closed=true&limit=500")
    }

    fn fetcher(fx: HashMap<String, Vec<u8>>) -> GammaResolutionFetcher<FixtureFetcher> {
        GammaResolutionFetcher::new(BASE.to_string(), FixtureFetcher::new(fx))
    }

    #[tokio::test]
    async fn parses_closed_market() {
        let mut fx = HashMap::new();
        fx.insert(
            closed_url("0xcond"),
            br#"[{"conditionId":"0xcond","closed":true,"outcomePrices":"[\"1\",\"0\"]","outcomes":"[\"Yes\",\"No\"]"}]"#.to_vec(),
        );
        let results = fetcher(fx).fetch_closed(&[mid("0xcond")]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome_prices[0], Decimal::ONE);
        assert_eq!(results[0].outcome_prices[1], Decimal::ZERO);
    }

    #[tokio::test]
    async fn skips_open_market() {
        let mut fx = HashMap::new();
        fx.insert(
            closed_url("0xcond"),
            br#"[{"conditionId":"0xcond","closed":false,"outcomePrices":null}]"#.to_vec(),
        );
        let results = fetcher(fx).fetch_closed(&[mid("0xcond")]).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn cross_market_row_is_not_attributed() {
        // The batch URL is for 0xwant, but Gamma returns a row for 0xother. The demux keys it under
        // 0xother, so 0xwant has no entry and nothing is attributed — a wrong-market resolution would
        // otherwise mis-credit P&L.
        let mut fx = HashMap::new();
        fx.insert(
            closed_url("0xwant"),
            br#"[{"conditionId":"0xother","closed":true,"outcomePrices":"[\"1\",\"0\"]"}]"#
                .to_vec(),
        );
        let results = fetcher(fx).fetch_closed(&[mid("0xwant")]).await.unwrap();
        assert!(
            results.is_empty(),
            "a row for a different conditionId must not resolve 0xwant"
        );
    }

    #[tokio::test]
    async fn empty_input_returns_empty() {
        let results = fetcher(HashMap::new()).fetch_closed(&[]).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn missing_market_is_skipped_not_error() {
        // No fixture for the URL → batch Fatal → unfetched → skipped; the call still returns Ok.
        let results = fetcher(HashMap::new())
            .fetch_closed(&[mid("0xabsent")])
            .await
            .unwrap();
        assert!(results.is_empty());
    }
}
