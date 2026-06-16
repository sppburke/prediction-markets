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
pub struct MidPriceCache<F = ReqwestFetcher> {
    inner: Arc<Mutex<HashMap<MarketId, CachedEntry>>>,
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

    /// Serve fresh [`CachedEntry`]s for `market_ids` from the cache and fetch the
    /// stale/missing ones concurrently through the rate gate, storing the results.
    /// Both [`fetch_mids`](Self::fetch_mids) and
    /// [`fetch_snapshots`](Self::fetch_snapshots) project from this single fetch
    /// path, so the mids surface is byte-identical whether or not snapshots are read.
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

        let fetcher = self.fetcher.clone();
        let base = self.gamma_base_url.clone();
        let fetched: Vec<(MarketId, (Vec<Decimal>, MidMarketSnapshot))> = stream::iter(stale)
            .map(|id| {
                let fetcher = fetcher.clone();
                let base = base.clone();
                async move {
                    // Open query (no `&closed=true`) → live mids in `outcomePrices`.
                    let url = format!("{base}/markets?condition_ids={id}");
                    match fetcher.fetch_page(&url).await {
                        Ok(bytes) => parse_market_row(&bytes, &id).map(|row| (id, row)),
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
        for (id, (mids, snapshot)) in fetched {
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

/// Minimal view of a Gamma `/markets` row — the fields the mid cache needs plus
/// the WS2 liquidity-snapshot scalars (`liquidity`/`volume`/`clobTokenIds`), which
/// are free on the same fetch.
#[derive(Deserialize)]
struct MidMarketRaw {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<String>,
    /// Gamma `liquidity` (USD). Live `/markets` sends it as a decimal string; the
    /// lenient decoder also accepts a number and yields `None` on anything
    /// unparseable, so a bad scalar never fails the row and drops its mids.
    #[serde(default, deserialize_with = "deserialize_decimal_lenient")]
    liquidity: Option<Decimal>,
    /// Gamma `volume` (USD cumulative). Same encoding and leniency as `liquidity`.
    #[serde(default, deserialize_with = "deserialize_decimal_lenient")]
    volume: Option<Decimal>,
    /// Gamma `clobTokenIds`, outcome-ordered, e.g. the stringified JSON array
    /// `"[\"123\",\"456\"]"` (the live `/markets` form; a native array is also
    /// accepted). Decoded to outcome-aligned ids via [`deserialize_clob_token_ids`].
    #[serde(
        rename = "clobTokenIds",
        default,
        deserialize_with = "deserialize_clob_token_ids"
    )]
    clob_token_ids: Vec<String>,
}

/// Parse a Gamma open-market response into its mids and liquidity snapshot,
/// reusing the shared decimal-string decoder for the mids. Returns `None`
/// (logged) on a parse error, missing prices, or a condition-id mismatch (Gamma
/// may return unrelated rows). The snapshot scalars/token-ids are best-effort — a
/// missing or malformed one yields `None`/empty without dropping the row, so this
/// never regresses the mids path.
fn parse_market_row(
    bytes: &[u8],
    market_id: &MarketId,
) -> Option<(Vec<Decimal>, MidMarketSnapshot)> {
    let markets: Vec<MidMarketRaw> = serde_json::from_slice(bytes)
        .map_err(|e| warn!(market_id = %market_id, error = %e, "mid-cache: response parse error"))
        .ok()?;
    let m = markets.first()?;
    if m.condition_id != market_id.to_string() {
        warn!(market_id = %market_id, returned = %m.condition_id, "mid-cache: condition_id mismatch");
        return None;
    }
    let prices_str = m.outcome_prices.as_deref()?;
    let mids = parse_outcome_prices(prices_str)?;
    let snapshot = MidMarketSnapshot {
        liquidity: m.liquidity,
        volume: m.volume,
        clob_token_ids: m.clob_token_ids.clone(),
    };
    Some((mids, snapshot))
}

/// Decode Gamma's `clobTokenIds` into outcome-ordered token ids. Gamma sends a
/// stringified JSON array (`"[\"id0\",\"id1\"]"` — the live `/markets` form); a
/// native JSON array is also accepted. Any other shape, malformed inner JSON, a
/// null, or a missing field yields an empty vec — token mapping is best-effort and
/// must never drop a market's mids. **Positions are preserved** (no compaction or
/// blank-dropping) so `clob_token_ids[outcome_id]` stays aligned with the outcome.
fn deserialize_clob_token_ids<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde_json::Value;

    Ok(match Option::<Value>::deserialize(d)? {
        // Stringified JSON array — the live `/markets` encoding.
        Some(Value::String(s)) => serde_json::from_str::<Vec<String>>(&s).unwrap_or_default(),
        // Native JSON array; coerce each entry to its string form, order preserved.
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|v| match v {
                Value::String(s) => s,
                other => other.to_string(),
            })
            .collect(),
        _ => Vec::new(),
    })
}

/// Deserialize Gamma's `liquidity`/`volume` whether they arrive as a JSON string
/// (`"6434.84"` — the live `/markets` form) or a JSON number. Any other shape — a
/// null, bool, array, object, missing field, or unparseable string — yields
/// `None`, so a malformed depth scalar can never fail the row and drop its mids.
fn deserialize_decimal_lenient<'de, D>(d: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use rust_decimal::prelude::FromPrimitive;
    use serde_json::Value;

    // Capture as an untyped value first: `Value` deserialization is total over any
    // JSON shape, so an unexpected type degrades to `None` instead of erroring.
    Ok(match Option::<Value>::deserialize(d)? {
        Some(Value::String(s)) => s.trim().parse::<Decimal>().ok(),
        Some(Value::Number(n)) => n.as_f64().and_then(Decimal::from_f64),
        _ => None,
    })
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
}
