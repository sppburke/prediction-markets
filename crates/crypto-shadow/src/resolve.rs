//! Gamma market-resolution fetch for **realized-edge** scoring — the key-free
//! ground truth that closes the BTC latency-arb thesis without the sponsored
//! Chainlink settlement key (issue #300 / #297).
//!
//! The 5m/15m markets settle on Chainlink BTC/USD, but Gamma records the
//! resolved `outcomePrices` once a market closes, so the winning side is
//! readable for free: `GET /markets?condition_ids={ID}&closed=true` →
//! `outcomePrices[0]` is the Up/YES token's settled value (`"1"` = Up won, `"0"`
//! = Down won). Verified live 2026-06-09 against a resolved `btc-updown-5m`
//! market. As of issue #382 Phase 4 this delegates to the shared batched
//! [`GammaMarketsClient`](pe_source_polymarket_public::GammaMarketsClient)
//! (`ClosedOnly`): many condition_ids per request, demuxed by `conditionId`.
//! `closed=true` is required — the plain endpoint returns an empty list for resolved markets.

use pe_source_polymarket_public::{GammaMarketsClient, MarketFilter, PageFetcher};
use rust_decimal::Decimal;
use tracing::warn;

/// Error fetching resolutions. Retained as the public error surface
/// (`crate::Error::Resolve`); [`BtcResolutionFetcher::fetch_resolutions`] itself
/// is resilient and returns `Ok` even when a batch fails (see its docs).
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("fetch: {0}")]
    Fetch(String),
}

/// Resolved outcome for one market: did the YES (Up) token win?
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketResolution {
    pub condition_id: String,
    pub yes_won: bool,
}

/// Fetches closed-market resolutions for the harness's observed markets via the
/// shared batched Gamma client.
pub struct BtcResolutionFetcher<F: PageFetcher> {
    client: GammaMarketsClient<F>,
}

impl<F: PageFetcher + Send + Sync> BtcResolutionFetcher<F> {
    /// `base_url` is the Gamma API root, e.g. `https://gamma-api.polymarket.com`.
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            client: GammaMarketsClient::new(base_url, fetcher),
        }
    }

    /// Fetch resolutions for `condition_ids` via one batched `&closed=true` pass.
    /// Returns only markets that are **closed** with parseable `outcomePrices`;
    /// open markets, unknown markets (Gamma `200` without the id), and ids in a
    /// 4xx chunk are skipped, so an in-progress 5m market simply yields no row yet.
    ///
    /// This wrapper deliberately degrades a shared-client batch failure (a transient fetch error
    /// or a corrupt response) to an empty result for this run: `resolve` is a re-runnable
    /// idempotent upsert, so the markets are retried next time. The `Result`/[`ResolveError`] is
    /// retained for the public error surface.
    ///
    /// The YES/Up side wins when the first settled price exceeds `0.5` (resolved
    /// markets settle to exactly `1`/`0`). Limitation (unchanged, shadow-only): a
    /// non-decisive settlement (`["0.5","0.5"]`) classifies as a YES loss; no such
    /// settlement has been observed for these binary BTC up/down markets. Since
    /// issue #382 Phase 4 the shared `outcomePrices` parser is lenient — a
    /// non-decimal entry falls back to `0` rather than skipping the market — which
    /// is harmless here as live prices are exactly `1`/`0`, and shadow-only besides.
    pub async fn fetch_resolutions(
        &self,
        condition_ids: &[String],
    ) -> Result<Vec<MarketResolution>, ResolveError> {
        if condition_ids.is_empty() {
            return Ok(Vec::new());
        }
        let fetched = match self
            .client
            .fetch_markets(condition_ids, MarketFilter::ClosedOnly)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "shadow: batch resolution fetch error, skipping this run");
                return Ok(Vec::new());
            }
        };

        let mut out = Vec::new();
        for cid in condition_ids {
            // Demux by the echoed `conditionId`: a hit is structurally the requested
            // market, so a cross-market row (keyed under its own id) is never
            // attributed here.
            let Some(m) = fetched.markets.get(cid) else {
                continue;
            };
            if !m.closed {
                continue;
            }
            let Some(prices) = m.outcome_prices.as_ref() else {
                continue;
            };
            let Some(yes) = prices.first().copied() else {
                continue;
            };
            out.push(MarketResolution {
                condition_id: cid.clone(),
                yes_won: yes > Decimal::new(5, 1), // > 0.5
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::FixtureFetcher;
    use std::collections::HashMap;

    const BASE: &str = "https://g.test";

    /// The exact `&closed=true&limit=500` batch URL the shared client builds for
    /// `ids` (repeat-key, input order preserved).
    fn closed_url(ids: &[&str]) -> String {
        let keys: Vec<String> = ids.iter().map(|id| format!("condition_ids={id}")).collect();
        format!("{BASE}/markets?{}&closed=true&limit=500", keys.join("&"))
    }

    fn fetcher(fx: HashMap<String, Vec<u8>>) -> BtcResolutionFetcher<FixtureFetcher> {
        BtcResolutionFetcher::new(BASE.to_string(), FixtureFetcher::new(fx))
    }

    #[tokio::test]
    async fn up_won_is_yes_won() {
        let mut fx = HashMap::new();
        fx.insert(
            closed_url(&["0xc"]),
            br#"[{"conditionId":"0xc","closed":true,"outcomePrices":"[\"1\",\"0\"]"}]"#.to_vec(),
        );
        let res = fetcher(fx)
            .fetch_resolutions(&["0xc".to_string()])
            .await
            .unwrap();
        assert_eq!(
            res,
            vec![MarketResolution {
                condition_id: "0xc".to_string(),
                yes_won: true,
            }]
        );
    }

    #[tokio::test]
    async fn down_won_is_not_yes_won() {
        let mut fx = HashMap::new();
        fx.insert(
            closed_url(&["0xc"]),
            br#"[{"conditionId":"0xc","closed":true,"outcomePrices":"[\"0\",\"1\"]"}]"#.to_vec(),
        );
        let res = fetcher(fx)
            .fetch_resolutions(&["0xc".to_string()])
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].yes_won);
    }

    #[tokio::test]
    async fn open_market_yields_no_resolution() {
        let mut fx = HashMap::new();
        fx.insert(
            closed_url(&["0xc"]),
            br#"[{"conditionId":"0xc","closed":false,"outcomePrices":"[\"0.55\",\"0.45\"]"}]"#
                .to_vec(),
        );
        let res = fetcher(fx)
            .fetch_resolutions(&["0xc".to_string()])
            .await
            .unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn unknown_market_is_skipped() {
        // Gamma returns `200 []` for an id it does not know → no row, skipped.
        let mut fx = HashMap::new();
        fx.insert(closed_url(&["0xc"]), b"[]".to_vec());
        let res = fetcher(fx)
            .fetch_resolutions(&["0xc".to_string()])
            .await
            .unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn empty_input_returns_empty() {
        let res = fetcher(HashMap::new())
            .fetch_resolutions(&[])
            .await
            .unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn missing_batch_is_skipped_not_error() {
        // No fixture for the batch URL → FixtureFetcher returns a fatal (4xx) error →
        // the chunk's ids are reported unfetched → empty result, but the call still
        // returns Ok (resilient; the markets are retried on the next run).
        let res = fetcher(HashMap::new())
            .fetch_resolutions(&["0xabsent".to_string()])
            .await
            .unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn multi_id_batch_demuxes_and_rejects_cross_market_row() {
        // All ids batch into ONE request; the response arrives out of order and
        // carries an unrelated row. Each requested market resolves to its own
        // outcome; the intruder (`0xZ`) and the still-open `0xopen` yield no row.
        let mut fx = HashMap::new();
        fx.insert(
            closed_url(&["0xA", "0xB", "0xopen"]),
            br#"[{"conditionId":"0xB","closed":true,"outcomePrices":"[\"0\",\"1\"]"},
                 {"conditionId":"0xZ","closed":true,"outcomePrices":"[\"1\",\"0\"]"},
                 {"conditionId":"0xopen","closed":false,"outcomePrices":"[\"0.5\",\"0.5\"]"},
                 {"conditionId":"0xA","closed":true,"outcomePrices":"[\"1\",\"0\"]"}]"#
                .to_vec(),
        );
        let res = fetcher(fx)
            .fetch_resolutions(&["0xA".to_string(), "0xB".to_string(), "0xopen".to_string()])
            .await
            .unwrap();
        let by_id: HashMap<&str, bool> = res
            .iter()
            .map(|r| (r.condition_id.as_str(), r.yes_won))
            .collect();
        assert_eq!(by_id.len(), 2);
        assert_eq!(by_id.get("0xA"), Some(&true));
        assert_eq!(by_id.get("0xB"), Some(&false));
        assert!(
            !by_id.contains_key("0xZ"),
            "an unrequested cross-market row must not be attributed"
        );
        assert!(
            !by_id.contains_key("0xopen"),
            "an open market yields no resolution"
        );
    }
}
