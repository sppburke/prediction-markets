//! Lazy 60 s-TTL cache of Polymarket Gamma mid prices for **open** markets.
//!
//! Sibling to [`MarketEndCache`](crate::market_end_cache), but for *mutable* mids:
//! the end-date cache is write-once because a resolution time is immutable, whereas
//! mids move, so entries here expire after [`TTL`] and the dashboard mark-to-market
//! tracks the live mid. Fetches delegate to the shared batched
//! [`GammaMarketsClient`](pe_source_polymarket_public::GammaMarketsClient) over a rate-limited
//! [`ReqwestFetcher`](pe_source_polymarket_public::ReqwestFetcher) (per-instance ≤ 20 req/s,
//! matching the other Gamma fetchers in this binary); a market whose fetch fails or lacks
//! `outcomePrices` is simply omitted, so the caller marks that position's unrealized P&L as null.
//!
//! Open vs settled classification is the caller's job (via the resolution store) —
//! this cache only fetches mids for the open markets it is handed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pe_core_types::MarketId;
use pe_source_polymarket_public::{GammaMarketsClient, MarketFilter, PageFetcher, ReqwestFetcher};
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tracing::warn;

/// How long a cached mid stays fresh before a refetch.
const TTL: Duration = Duration::from_secs(60);
/// Min spacing between requests on this fetcher: 50 ms ⇒ ≤ 20 req/s. See `_GLOSSARY`.
/// (Batch concurrency is the client's `GAMMA_CONCURRENCY`; this gate still bounds actual throughput.)
const GAMMA_MIN_INTERVAL_MS: u64 = 50;

/// Per-market Gamma liquidity metadata captured alongside the mids, for the
/// fill-time market-snapshot rows of WS2 (issue #350). It rides on the same
/// `/markets` fetch as the mids (these fields are free), is cached with them, and
/// is **not yet consumed** by the orchestrator — wired in a later PR — so it adds
/// zero fill-path risk.
#[derive(Debug, Clone, Default)]
pub struct MidMarketSnapshot {
    /// Gamma `liquidity` (USD order-book depth indicator). `None` when the field
    /// is absent or unparseable.
    pub liquidity: Option<Decimal>,
    /// Gamma `volume` (USD cumulative). `None` when absent or unparseable.
    pub volume: Option<Decimal>,
    /// Gamma `clobTokenIds`, ordered by outcome so `clob_token_ids[outcome_id]`
    /// is the filled outcome's CLOB token. Empty when absent or malformed.
    pub clob_token_ids: Vec<String>,
}

/// A cached market row: the mids (per `outcome_id`), the liquidity
/// [`MidMarketSnapshot`], and the instant it was fetched (for TTL expiry).
#[derive(Clone)]
struct CachedEntry {
    mids: Vec<Decimal>,
    snapshot: MidMarketSnapshot,
    at: Instant,
}

/// Thread-safe TTL cache of open-market mid prices. Generic over the fetcher so
/// tests can inject a `FixtureFetcher`; production uses [`ReqwestFetcher`].
pub struct MidPriceCache<F: PageFetcher = ReqwestFetcher> {
    inner: Arc<Mutex<HashMap<MarketId, CachedEntry>>>,
    /// Shared batched Gamma `/markets` client. Held behind an `Arc` so the cache stays `Clone`
    /// even though the underlying [`ReqwestFetcher`] (rate-limit `Mutex`) is not `Clone`.
    client: Arc<GammaMarketsClient<F>>,
}

// Manual Clone: the `Arc`s clone regardless of whether `F: Clone` (`ReqwestFetcher` is not — it
// holds a `Mutex`), so all clones share one rate-gated client.
impl<F: PageFetcher> Clone for MidPriceCache<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            client: self.client.clone(),
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
            client: Arc::new(GammaMarketsClient::new(gamma_base_url, fetcher)),
        }
    }

    /// Current mids (per `outcome_id`) for `market_ids`, served from cache within
    /// [`TTL`] and otherwise fetched concurrently through the rate gate. Markets
    /// whose fetch fails or lacks `outcomePrices` are omitted from the result.
    pub async fn fetch_mids(&self, market_ids: &[MarketId]) -> HashMap<MarketId, Vec<Decimal>> {
        self.ensure_entries(market_ids)
            .await
            .into_iter()
            .map(|(id, entry)| (id, entry.mids))
            .collect()
    }

    /// Current liquidity [`MidMarketSnapshot`] (per market) for `market_ids`,
    /// sharing the same TTL cache and rate gate as [`fetch_mids`](Self::fetch_mids).
    /// A market whose fetch fails or lacks `outcomePrices` is omitted — the
    /// snapshot rides on the same row as the mids. Snapshot scalars/token-ids are
    /// best-effort: a missing or malformed `liquidity`/`volume`/`clobTokenIds`
    /// surfaces as `None`/empty rather than dropping the market.
    pub async fn fetch_snapshots(
        &self,
        market_ids: &[MarketId],
    ) -> HashMap<MarketId, MidMarketSnapshot> {
        self.ensure_entries(market_ids)
            .await
            .into_iter()
            .map(|(id, entry)| (id, entry.snapshot))
            .collect()
    }

    /// Serve fresh [`CachedEntry`]s for `market_ids` from the cache and fetch the stale/missing ones
    /// via the shared batched [`GammaMarketsClient`] (one `OpenOnly` `/markets` request per
    /// [`GAMMA_BATCH_SIZE`](pe_source_polymarket_public::GAMMA_BATCH_SIZE)-id chunk, demuxed by
    /// `conditionId`), storing the results. Both [`fetch_mids`](Self::fetch_mids) and
    /// [`fetch_snapshots`](Self::fetch_snapshots) project from this single fetch path, so the mids
    /// surface is byte-identical whether or not snapshots are read.
    ///
    /// Best-effort: a client error (transient fetch / corrupt response) is logged and this tick
    /// serves only what was cached — coarser than the pre-#382 per-ID skip, but the orchestrator
    /// fetches one market per call (so a single failing market still omits only itself) and the
    /// dashboard self-heals on the next [`TTL`] refresh. A market that is unknown, in a 4xx chunk,
    /// or lacks `outcomePrices` is omitted, so the caller marks its unrealized P&L null.
    async fn ensure_entries(&self, market_ids: &[MarketId]) -> HashMap<MarketId, CachedEntry> {
        let mut out: HashMap<MarketId, CachedEntry> = HashMap::new();
        let mut stale: Vec<MarketId> = Vec::new();

        // Brief lock: serve fresh entries, collect the rest. Never held across a fetch.
        {
            let now = Instant::now();
            let map = self.inner.lock().await;
            for id in market_ids {
                match map.get(id) {
                    Some(entry) if now.duration_since(entry.at) < TTL => {
                        out.insert(id.clone(), entry.clone());
                    }
                    _ => stale.push(id.clone()),
                }
            }
        }

        if stale.is_empty() {
            return out;
        }

        // Open query (no `&closed=true`) → live mids in `outcomePrices`. The client batches and
        // demuxes by `conditionId`; the whole call is best-effort (see method doc).
        let ids: Vec<String> = stale.iter().map(|m| m.to_string()).collect();
        let markets = match self
            .client
            .fetch_markets(&ids, MarketFilter::OpenOnly)
            .await
        {
            Ok(fetched) => fetched.markets,
            Err(e) => {
                warn!(error = %e, stale = stale.len(), "mid-cache: batch fetch error, omitting this tick");
                return out;
            }
        };

        let now = Instant::now();
        let mut map = self.inner.lock().await;
        for id in stale {
            // Demux by the echoed `conditionId`: a hit is structurally the requested market, so a
            // cross-market row (keyed under its own id) can never be attributed here.
            let Some(m) = markets.get(&id.to_string()) else {
                continue; // unknown / 4xx-unfetched / absent → omit (P&L stays null)
            };
            // No / malformed `outcomePrices` → skip rather than mis-value the market.
            let Some(mids) = m.outcome_prices.clone() else {
                continue;
            };
            let snapshot = MidMarketSnapshot {
                liquidity: m.liquidity,
                volume: m.volume,
                clob_token_ids: m.clob_token_ids.clone(),
            };
            let entry = CachedEntry {
                mids,
                snapshot,
                at: now,
            };
            map.insert(id.clone(), entry.clone());
            out.insert(id, entry);
        }
        out
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::FixtureFetcher;

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    /// The exact URL the shared client builds for a single-id `OpenOnly` batch
    /// (`condition_ids={id}&limit=500`, no `&closed=true`). The orchestrator/snapshot worker
    /// fetch one market per call, so every live mid fetch is a batch-of-one keyed like this.
    fn url(base: &str, id: &str) -> String {
        format!("{base}/markets?condition_ids={id}&limit=500")
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

    #[tokio::test]
    async fn fetch_snapshots_parses_liquidity_volume_and_ordered_token_ids() {
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]","liquidity":"6434.84","volume":"99995.018095","clobTokenIds":"[\"111\",\"222\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = out.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, Some(Decimal::new(643484, 2)));
        assert_eq!(snap.volume, Some(Decimal::new(99995018095, 6)));
        // Ordered by outcome: index 0 = first outcome's token, index 1 = second.
        assert_eq!(
            snap.clob_token_ids,
            vec!["111".to_string(), "222".to_string()]
        );
    }

    #[tokio::test]
    async fn snapshot_fields_absent_yield_none_and_empty_without_dropping_mids() {
        // A row with no liquidity/volume/clobTokenIds still yields its mids; the
        // snapshot is simply empty. Proves the new fields are non-regressive.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.7\",\"0.3\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = snaps.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, None);
        assert_eq!(snap.volume, None);
        assert!(snap.clob_token_ids.is_empty());
        // Same cache row still serves the mids.
        let mids = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            mids.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(7, 1), Decimal::new(3, 1)]
        );
    }

    #[tokio::test]
    async fn liquidity_accepts_numeric_form() {
        // Gamma occasionally serializes the scalar as a JSON number; the lenient
        // decoder accepts it (`liquidityNum`-style fixtures).
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","liquidity":6434}]"#
                .to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        assert_eq!(
            snaps.get(&mid("0xcond")).unwrap().liquidity,
            Some(Decimal::from(6434))
        );
    }

    #[tokio::test]
    async fn malformed_liquidity_does_not_drop_mids() {
        // An unparseable liquidity string (and a malformed token array) must not
        // fail the row: the scalar is None / tokens empty, but the mids survive —
        // the zero-fill-path-risk guarantee.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.4\",\"0.6\"]","liquidity":"not-a-number","clobTokenIds":"oops"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = snaps.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, None);
        assert!(snap.clob_token_ids.is_empty());
        let mids = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            mids.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(4, 1), Decimal::new(6, 1)]
        );
    }

    #[tokio::test]
    async fn fetch_mids_unchanged_when_snapshot_fields_present() {
        // Regression guard: the extra Gamma fields do not alter the mids result.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]","liquidity":"100","volume":"200","clobTokenIds":"[\"a\",\"b\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            out.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(62, 2), Decimal::new(38, 2)]
        );
    }

    #[tokio::test]
    async fn snapshot_served_from_cache_within_ttl() {
        // Second call uses an empty fetcher; the snapshot must come from the cache.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","liquidity":"12.5","clobTokenIds":"[\"t0\",\"t1\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let _ = cache.fetch_snapshots(&[mid("0xcond")]).await; // populate
        let out = cache.fetch_snapshots(&[mid("0xcond")]).await; // within TTL → cache
        let snap = out.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, Some(Decimal::new(125, 1)));
        assert_eq!(
            snap.clob_token_ids,
            vec!["t0".to_string(), "t1".to_string()]
        );
    }

    #[tokio::test]
    async fn clob_token_ids_preserve_blank_positions() {
        // A blank id must NOT be compacted away — `clob_token_ids[outcome_id]` is
        // indexed positionally, so dropping index 0 would misalign every outcome.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","clobTokenIds":"[\"\",\"222\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        assert_eq!(
            snaps.get(&mid("0xcond")).unwrap().clob_token_ids,
            vec![String::new(), "222".to_string()]
        );
    }

    #[tokio::test]
    async fn clob_token_ids_accept_native_json_array() {
        // Robustness: a native JSON array (not the stringified form) is accepted.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","clobTokenIds":["111","222"]}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        assert_eq!(
            snaps.get(&mid("0xcond")).unwrap().clob_token_ids,
            vec!["111".to_string(), "222".to_string()]
        );
    }

    #[tokio::test]
    async fn unexpected_json_types_do_not_drop_mids() {
        // bool liquidity, object volume, native-array-of-numbers token ids — all
        // unexpected types. None must fail the row: scalars → None, and the mids
        // still parse (the zero-fill-path-risk guarantee, for any JSON shape).
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.4\",\"0.6\"]","liquidity":true,"volume":{"x":1},"clobTokenIds":42}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = snaps.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, None);
        assert_eq!(snap.volume, None);
        assert!(snap.clob_token_ids.is_empty());
        let mids = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            mids.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(4, 1), Decimal::new(6, 1)]
        );
    }

    #[tokio::test]
    async fn multi_id_batch_demuxes_and_rejects_cross_market_row() {
        // The dashboard path (`paper_api::fetch_mids(&open_markets)`) fetches many markets in one
        // batch. The client builds a single repeat-key URL; the response arrives out of order and
        // carries an unrelated row. Each requested market gets its own mids; the intruder (`0xZ`) is
        // keyed under its own id and never attributed to a requested market.
        let mut fx = HashMap::new();
        fx.insert(
            format!("{BASE}/markets?condition_ids=0xA&condition_ids=0xB&limit=500"),
            br#"[{"conditionId":"0xB","outcomePrices":"[\"0.3\",\"0.7\"]"},
                 {"conditionId":"0xZ","outcomePrices":"[\"0.99\",\"0.01\"]"},
                 {"conditionId":"0xA","outcomePrices":"[\"0.6\",\"0.4\"]"}]"#
                .to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xA"), mid("0xB")]).await;
        assert_eq!(out.len(), 2);
        assert_eq!(
            out.get(&mid("0xA")).unwrap(),
            &vec![Decimal::new(6, 1), Decimal::new(4, 1)]
        );
        assert_eq!(
            out.get(&mid("0xB")).unwrap(),
            &vec![Decimal::new(3, 1), Decimal::new(7, 1)]
        );
        assert!(
            !out.contains_key(&mid("0xZ")),
            "an unrequested cross-market row must not be attributed"
        );
    }
}
