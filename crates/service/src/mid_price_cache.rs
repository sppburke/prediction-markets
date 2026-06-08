//! Lazy 60 s-TTL cache of Polymarket Gamma mid prices for **open** markets.
//!
//! Sibling to [`MarketEndCache`](crate::market_end_cache), but for *mutable* mids:
//! the end-date cache is write-once because a resolution time is immutable, whereas
//! mids move, so entries here expire after [`TTL`] and the dashboard mark-to-market
//! tracks the live mid. Fetches go through their own rate-limited [`ReqwestFetcher`]
//! (per-instance ≤ 20 req/s, matching the other Gamma fetchers in this binary); a
//! market whose fetch fails or lacks `outcomePrices` is simply omitted, so the
//! caller marks that position's unrealized P&L as null.
//!
//! Open vs settled classification is the caller's job (via the resolution store) —
//! this cache only fetches mids for the open markets it is handed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use pe_core_types::MarketId;
use pe_paper_pnl::parse_outcome_prices;
use pe_source_polymarket_public::{PageFetcher, ReqwestFetcher};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::warn;

/// How long a cached mid stays fresh before a refetch.
const TTL: Duration = Duration::from_secs(60);
/// Concurrent in-flight fetches; the rate gate (below) bounds actual throughput.
const FETCH_CONCURRENCY: usize = 10;
/// Min spacing between requests on this fetcher: 50 ms ⇒ ≤ 20 req/s. See `_GLOSSARY`.
const GAMMA_MIN_INTERVAL_MS: u64 = 50;

/// A cached mid-price vector with the instant it was fetched (for TTL expiry).
type CachedMids = (Vec<Decimal>, Instant);

/// Thread-safe TTL cache of open-market mid prices. Generic over the fetcher so
/// tests can inject a `FixtureFetcher`; production uses [`ReqwestFetcher`].
pub struct MidPriceCache<F = ReqwestFetcher> {
    inner: Arc<Mutex<HashMap<MarketId, CachedMids>>>,
    fetcher: Arc<F>,
    gamma_base_url: String,
}

// Manual Clone: `Arc<F>` is cloneable regardless of whether `F: Clone`
// (`ReqwestFetcher` is not — it holds a `Mutex`).
impl<F> Clone for MidPriceCache<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fetcher: self.fetcher.clone(),
            gamma_base_url: self.gamma_base_url.clone(),
        }
    }
}

impl MidPriceCache<ReqwestFetcher> {
    /// Production cache over a rate-limited Gamma client.
    pub fn new(gamma_base_url: String) -> Self {
        Self::with_fetcher(
            ReqwestFetcher::new(reqwest::Client::new()).with_min_interval_ms(GAMMA_MIN_INTERVAL_MS),
            gamma_base_url,
        )
    }
}

impl<F: PageFetcher + Send + Sync> MidPriceCache<F> {
    /// Build a cache over an arbitrary fetcher (test seam).
    pub fn with_fetcher(fetcher: F, gamma_base_url: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            fetcher: Arc::new(fetcher),
            gamma_base_url,
        }
    }

    /// Current mids (per `outcome_id`) for `market_ids`, served from cache within
    /// [`TTL`] and otherwise fetched concurrently through the rate gate. Markets
    /// whose fetch fails or lacks `outcomePrices` are omitted from the result.
    pub async fn fetch_mids(&self, market_ids: &[MarketId]) -> HashMap<MarketId, Vec<Decimal>> {
        let mut out: HashMap<MarketId, Vec<Decimal>> = HashMap::new();
        let mut stale: Vec<MarketId> = Vec::new();

        // Brief lock: serve fresh entries, collect the rest. Never held across a fetch.
        {
            let now = Instant::now();
            let map = self.inner.lock().await;
            for id in market_ids {
                match map.get(id) {
                    Some((prices, at)) if now.duration_since(*at) < TTL => {
                        out.insert(id.clone(), prices.clone());
                    }
                    _ => stale.push(id.clone()),
                }
            }
        }

        if stale.is_empty() {
            return out;
        }

        let fetcher = self.fetcher.clone();
        let base = self.gamma_base_url.clone();
        let fetched: Vec<(MarketId, Vec<Decimal>)> = stream::iter(stale)
            .map(|id| {
                let fetcher = fetcher.clone();
                let base = base.clone();
                async move {
                    // Open query (no `&closed=true`) → live mids in `outcomePrices`.
                    let url = format!("{base}/markets?condition_ids={id}");
                    match fetcher.fetch_page(&url).await {
                        Ok(bytes) => parse_mid(&bytes, &id).map(|prices| (id, prices)),
                        Err(e) => {
                            warn!(market_id = %id, error = %e, "mid-cache: fetch error, omitting");
                            None
                        }
                    }
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .filter_map(|r| async move { r })
            .collect()
            .await;

        let now = Instant::now();
        let mut map = self.inner.lock().await;
        for (id, prices) in fetched {
            map.insert(id.clone(), (prices.clone(), now));
            out.insert(id, prices);
        }
        out
    }
}

/// Minimal view of a Gamma `/markets` row — only the fields the mid cache needs.
#[derive(Deserialize)]
struct MidMarketRaw {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<String>,
}

/// Parse a Gamma open-market response into mids, reusing the shared decimal-string
/// decoder. Returns `None` (logged) on a parse error, missing prices, or a
/// condition-id mismatch (Gamma may return unrelated rows).
fn parse_mid(bytes: &[u8], market_id: &MarketId) -> Option<Vec<Decimal>> {
    let markets: Vec<MidMarketRaw> = serde_json::from_slice(bytes)
        .map_err(|e| warn!(market_id = %market_id, error = %e, "mid-cache: response parse error"))
        .ok()?;
    let m = markets.first()?;
    if m.condition_id != market_id.to_string() {
        warn!(market_id = %market_id, returned = %m.condition_id, "mid-cache: condition_id mismatch");
        return None;
    }
    let prices_str = m.outcome_prices.as_deref()?;
    parse_outcome_prices(prices_str)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::FixtureFetcher;

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    fn url(base: &str, id: &str) -> String {
        format!("{base}/markets?condition_ids={id}")
    }

    const BASE: &str = "https://gamma-api.polymarket.com";

    #[tokio::test]
    async fn fetches_and_parses_open_mid() {
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            out.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(62, 2), Decimal::new(38, 2)]
        );
    }

    #[tokio::test]
    async fn ttl_hit_serves_without_refetch() {
        // Second call uses an empty fetcher; the value must come from the cache.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.50\",\"0.50\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let _ = cache.fetch_mids(&[mid("0xcond")]).await; // populate
        let out = cache.fetch_mids(&[mid("0xcond")]).await; // within TTL → cache
        assert_eq!(
            out.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(5, 1), Decimal::new(5, 1)]
        );
    }

    #[tokio::test]
    async fn missing_market_is_softly_omitted() {
        // Fetcher has no fixture for this url → fetch error → omitted, not a panic.
        let cache =
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xabsent")]).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn condition_id_mismatch_is_rejected() {
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xwant"),
            br#"[{"conditionId":"0xother","outcomePrices":"[\"0.9\",\"0.1\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xwant")]).await;
        assert!(out.is_empty());
    }
}
