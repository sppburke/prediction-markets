//! Polymarket bulk trade fetcher — incremental, cache-first, concurrent.
//!
//! For each wallet: paginates from offset 0, stops when
//! `INCREMENTAL_STOP_THRESHOLD` consecutive already-known `source_trade_id`s
//! are encountered, then appends only the new trades to the cache.
//! Each fetched page is sorted newest-first (defensive: API normally does this
//! but the sort removes the silent-failure mode if ordering ever changes).
//!
//! `fetch_all` runs up to `concurrency` per-wallet fetches in parallel; the
//! shared `PageFetcher` enforces the global rate-limit gate so the aggregate
//! throughput stays inside the documented Polymarket Data API limit
//! (200 req/10s on `/trades`, ≈ 20 req/s).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::cache::{INCREMENTAL_STOP_THRESHOLD, WalletCache};
use crate::error::BootstrapError;

const TRADE_FETCH_LIMIT: u32 = 500;
/// Polymarket Data API rejects `/trades` requests with `offset >= 3000` (HTTP 400,
/// "max historical activity offset of 3000 exceeded"). Stop paging before that limit.
/// Canonical default: `docs/_GLOSSARY.md` `bootstrap_polymarket_max_offset`.
const MAX_POLYMARKET_OFFSET: u32 = 3000;

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

type TradeResponse = Vec<PolymarketTrade>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PolymarketTrade {
    transaction_hash: String,
    condition_id: String,
    side: String,
    size: Decimal,
    price: Decimal,
    timestamp: i64,
    #[serde(default)]
    outcome_index: Option<u8>,
}

// ── Fetcher ───────────────────────────────────────────────────────────────────

/// Default per-run concurrency for `PolymarketBulkFetcher::fetch_all`.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub const DEFAULT_CONCURRENCY: usize = 16;

/// Incremental trade fetcher for Polymarket wallets.
pub struct PolymarketBulkFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
    concurrency: usize,
}

impl<F: PageFetcher> PolymarketBulkFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            base_url,
            fetcher,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Override the wallet-fetch concurrency (clamped to `>= 1`).
    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Fetch new trades for all `wallets`, updating `cache` incrementally.
    ///
    /// Up to `self.concurrency` wallets are fetched in parallel; the underlying
    /// `PageFetcher` enforces the global rate-limit gate. HTTP 429 responses are
    /// retried indefinitely at the page level (sleeping `Retry-After` seconds)
    /// so no wallet is ever skipped due to rate limiting.
    ///
    /// Returns `Err(BootstrapError::PartialFetch)` if any wallet's trades could
    /// not be fetched (non-recoverable network/parse error) or written to SQLite.
    /// All wallets are attempted before the error is returned; re-running the
    /// bootstrap will retry only the wallets that are missing from the cache.
    ///
    /// Trades are committed to SQLite per-wallet inside a single transaction.
    /// SQLite WAL mode provides per-commit durability — no manual checkpointing
    /// or final flush is needed.
    pub async fn fetch_all(
        &self,
        wallets: &[WalletAddress],
        cache: &mut WalletCache,
    ) -> Result<(), BootstrapError> {
        let cache_mutex: Mutex<&mut WalletCache> = Mutex::new(cache);
        let failed = Arc::new(AtomicUsize::new(0));

        stream::iter(wallets.iter().copied())
            .for_each_concurrent(self.concurrency, |wallet| {
                let cache_mutex = &cache_mutex;
                let failed = Arc::clone(&failed);
                async move {
                    let wallet_hex = wallet.to_string();
                    let known_ids: HashSet<SourceTradeId> = {
                        let guard = cache_mutex.lock().await;
                        guard.known_trade_ids(&wallet_hex).into_iter().collect()
                    };

                    match self.fetch_wallet_incremental(wallet, &known_ids).await {
                        Ok(new_trades) if !new_trades.is_empty() => {
                            let mut guard = cache_mutex.lock().await;
                            if let Err(e) = guard.insert_new(&wallet_hex, new_trades) {
                                tracing::error!(
                                    wallet = %wallet_hex,
                                    error = %e,
                                    "polymarket: cache insert failed — trades for this wallet will be missing"
                                );
                                failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!(
                                wallet = %wallet_hex,
                                error = %e,
                                "polymarket: fetch failed — trades for this wallet will be missing"
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
            .await;

        let n = failed.load(Ordering::Relaxed);
        if n > 0 {
            return Err(BootstrapError::PartialFetch { failed_wallets: n });
        }
        Ok(())
    }

    /// Fetch only trades not already in `known_ids` for a single wallet.
    ///
    /// Pages from offset 0; stops after `INCREMENTAL_STOP_THRESHOLD` consecutive
    /// trades whose `source_trade_id` appears in `known_ids`. Each page is sorted
    /// newest-first before checking (defensive against API ordering changes).
    async fn fetch_wallet_incremental(
        &self,
        wallet: WalletAddress,
        known_ids: &HashSet<SourceTradeId>,
    ) -> Result<Vec<RawTrade>, BootstrapError> {
        let wallet_hex = wallet.to_string();
        let endpoint = PolymarketEndpoint::UserTrades {
            user: wallet_hex.clone(),
        }
        .url(&self.base_url);

        let mut new_trades: Vec<RawTrade> = Vec::new();
        let mut offset: u32 = 0;

        'pages: loop {
            if offset >= MAX_POLYMARKET_OFFSET {
                break;
            }
            let url = format!("{endpoint}&limit={TRADE_FETCH_LIMIT}&offset={offset}");

            // Retry indefinitely on HTTP 429 — rate limiting is transient and
            // skipping a wallet on rate limit would permanently lose its trades.
            let bytes = loop {
                match self.fetcher.fetch_page(&url).await {
                    Ok(b) => break b,
                    Err(SourceError::RateLimited { retry_after_secs }) => {
                        let wait = Duration::from_secs(u64::from(retry_after_secs).max(1));
                        tracing::warn!(
                            wallet = %wallet_hex,
                            retry_after_secs,
                            "polymarket: rate limited, retrying page after backoff"
                        );
                        tokio::time::sleep(wait).await;
                    }
                    Err(e) => {
                        return Err(BootstrapError::Polymarket {
                            wallet: wallet_hex.clone(),
                            message: e.to_string(),
                        });
                    }
                }
            };

            let (mut page, raw_count) = parse_trades_with_count(&bytes, wallet).map_err(|e| {
                BootstrapError::TradeParse {
                    wallet: wallet_hex.clone(),
                    message: e,
                }
            })?;

            // Sort newest-first (defensive; API normally returns this order).
            page.sort_by_key(|t| std::cmp::Reverse(t.timestamp.0));

            let mut consecutive_known: usize = 0;
            for trade in page {
                if known_ids.contains(&trade.source_trade_id) {
                    consecutive_known += 1;
                    if consecutive_known >= INCREMENTAL_STOP_THRESHOLD {
                        break 'pages;
                    }
                } else {
                    consecutive_known = 0;
                    new_trades.push(trade);
                }
            }

            if raw_count < TRADE_FETCH_LIMIT as usize {
                break;
            }
            offset += TRADE_FETCH_LIMIT;
        }

        Ok(new_trades)
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse `bytes` into trades, returning the raw JSON array count for
/// pagination termination. Uses raw count so individual parse failures
/// do not cause early pagination termination.
fn parse_trades_with_count(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<(Vec<RawTrade>, usize), String> {
    let response: TradeResponse =
        serde_json::from_slice(bytes).map_err(|e| format!("json: {e}"))?;
    let raw_count = response.len();
    let mut out = Vec::with_capacity(raw_count);
    for raw in response {
        match convert_trade(raw, wallet) {
            Ok(t) => out.push(t),
            Err(e) => tracing::warn!(wallet = %wallet, error = %e, "skipping unparseable trade"),
        }
    }
    Ok((out, raw_count))
}

fn convert_trade(raw: PolymarketTrade, wallet: WalletAddress) -> Result<RawTrade, String> {
    let price_dec = raw.price;
    let price = Price::new(price_dec).map_err(|e| format!("invalid price {price_dec}: {e}"))?;

    // Fractional fills (size < 1) count as 1 contract — win/loss signal matters,
    // not exact size.
    let contracts = if raw.size >= Decimal::ONE {
        raw.size
            .floor()
            .to_u64()
            .map(ContractQty)
            .ok_or_else(|| format!("size {} overflows u64", raw.size))?
    } else if raw.size > Decimal::ZERO {
        ContractQty(1)
    } else {
        return Err(format!("size {} is zero or negative", raw.size));
    };

    let side = match raw.side.to_uppercase().as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => return Err(format!("unknown side '{other}'")),
    };

    // Normalise to seconds; Polymarket sometimes uses milliseconds.
    let ts_secs = if raw.timestamp > 9_999_999_999 {
        raw.timestamp / 1_000
    } else {
        raw.timestamp
    };
    let dt = OffsetDateTime::from_unix_timestamp(ts_secs)
        .map_err(|_| format!("invalid timestamp {ts_secs}"))?;

    Ok(RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId(raw.condition_id)),
        outcome_id: OutcomeId(raw.outcome_index.unwrap_or(0)),
        side,
        price,
        contracts,
        timestamp: SourceTimestamp(dt),
        source_trade_id: SourceTradeId(raw.transaction_hash),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use pe_source_core::SourceError;
    use pe_source_polymarket_public::FixtureFetcher;
    use tempfile::TempDir;

    use super::*;
    use crate::cache::WalletCache;

    /// A fetcher that returns `RateLimited` for the first `rate_limit_count` calls,
    /// then delegates to an inner `FixtureFetcher`. Tests that rate-limited pages
    /// are retried rather than causing the wallet to be skipped.
    struct RateLimitThenSucceedFetcher {
        inner: FixtureFetcher,
        calls_remaining: Arc<AtomicU32>,
    }

    impl RateLimitThenSucceedFetcher {
        fn new(inner: FixtureFetcher, rate_limit_count: u32) -> Self {
            Self {
                inner,
                calls_remaining: Arc::new(AtomicU32::new(rate_limit_count)),
            }
        }
    }

    impl PageFetcher for RateLimitThenSucceedFetcher {
        async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
            if self
                .calls_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    if n > 0 { Some(n - 1) } else { None }
                })
                .is_ok()
            {
                return Err(SourceError::RateLimited {
                    retry_after_secs: 0,
                });
            }
            self.inner.fetch_page(url).await
        }
    }

    const BASE_URL: &str = "https://data-api.polymarket.com";

    fn wallet_a() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    fn trade_json(hash: &str, ts: i64) -> String {
        format!(
            r#"{{"transactionHash":"{hash}","conditionId":"0xcond","side":"BUY","size":1,"price":0.60,"timestamp":{ts}}}"#,
        )
    }

    fn page_json_hashes(hashes: &[(&str, i64)]) -> Vec<u8> {
        let entries: Vec<String> = hashes.iter().map(|(h, ts)| trade_json(h, *ts)).collect();
        format!("[{}]", entries.join(",")).into_bytes()
    }

    fn page_json_n(n: usize, hash_offset: usize, ts_base: i64) -> Vec<u8> {
        let entries: Vec<String> = (0..n)
            .map(|i| trade_json(&format!("0xhash{:06}", hash_offset + i), ts_base - i as i64))
            .collect();
        format!("[{}]", entries.join(",")).into_bytes()
    }

    fn trade_url(wallet: WalletAddress, offset: u32) -> String {
        format!(
            "{}&limit={TRADE_FETCH_LIMIT}&offset={offset}",
            PolymarketEndpoint::UserTrades {
                user: wallet.to_string()
            }
            .url(BASE_URL)
        )
    }

    fn parse(bytes: &[u8]) -> Vec<RawTrade> {
        parse_trades_with_count(bytes, wallet_a()).unwrap().0
    }

    #[test]
    fn parse_valid_buy_trade() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":10,"price":0.60,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].side, Side::Buy);
        assert_eq!(trades[0].contracts.0, 10);
    }

    #[test]
    fn parse_millisecond_timestamp() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"SELL","size":5,"price":0.40,"timestamp":1704067200000}]"#;
        let trades = parse(json);
        assert_eq!(trades[0].timestamp.0.unix_timestamp(), 1_704_067_200);
    }

    #[test]
    fn parse_unknown_side_skipped() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"UNKNOWN","size":5,"price":0.40,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_fractional_size_counts_as_one_contract() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":0.75,"price":0.40,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].contracts, ContractQty(1));
    }

    #[test]
    fn parse_zero_size_skipped() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":0,"price":0.40,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_empty_array() {
        let trades = parse(b"[]");
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_outcome_index_propagated() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":1,"price":0.50,"timestamp":1704067200,"outcomeIndex":1}]"#;
        let trades = parse(json);
        assert_eq!(trades[0].outcome_id, OutcomeId(1));
    }

    #[test]
    fn raw_count_independent_of_parse_failures() {
        let json = br#"[
            {"transactionHash":"0xhash1","conditionId":"0xcond","side":"BUY","size":1,"price":0.60,"timestamp":1704067200},
            {"transactionHash":"0xhash2","conditionId":"0xcond","side":"UNKNOWN","size":1,"price":0.60,"timestamp":1704067200}
        ]"#;
        let (trades, raw_count) = parse_trades_with_count(json, wallet_a()).unwrap();
        assert_eq!(raw_count, 2);
        assert_eq!(trades.len(), 1);
    }

    #[tokio::test]
    async fn pagination_concatenates_two_full_pages() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        // Each page uses a distinct hash range to avoid dedup across pages.
        responses.insert(trade_url(wallet, 0), page_json_n(500, 0, 2_000_000));
        responses.insert(trade_url(wallet, 500), page_json_n(500, 500, 1_500_000));
        responses.insert(trade_url(wallet, 1000), page_json_n(0, 1000, 0));

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 1000);
    }

    #[tokio::test]
    async fn pagination_stops_on_partial_page() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url(wallet, 0), page_json_n(499, 0, 2_000_000));

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 499);
    }

    #[tokio::test]
    async fn incremental_fetch_stops_after_k_consecutive_known_ids() {
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Cold run: populate cache with 5 old trades.
        let old_page = page_json_hashes(&[
            ("0xold1", 1_000_005),
            ("0xold2", 1_000_004),
            ("0xold3", 1_000_003),
            ("0xold4", 1_000_002),
            ("0xold5", 1_000_001),
        ]);
        let mut r_cold = HashMap::new();
        r_cold.insert(trade_url(wallet, 0), old_page);
        let b_cold = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_cold));
        b_cold.fetch_all(&[wallet], &mut cache).await.unwrap();
        assert_eq!(cache.trade_count(), 5);

        // Incremental run: 2 new trades, then old1..old5 (3 consecutive known → stop).
        let incr_page = page_json_hashes(&[
            ("0xnew1", 2_000_002),
            ("0xnew2", 2_000_001),
            ("0xold1", 1_000_005), // known — 1
            ("0xold2", 1_000_004), // known — 2
            ("0xold3", 1_000_003), // known — 3: stop
            ("0xold4", 1_000_002), // not reached
            ("0xold5", 1_000_001), // not reached
        ]);
        let mut r_incr = HashMap::new();
        r_incr.insert(trade_url(wallet, 0), incr_page);
        let b_incr = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_incr));
        b_incr.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 7, "5 old + 2 new = 7");
    }

    #[tokio::test]
    async fn incremental_fetch_cold_start_fetches_everything() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url(wallet, 0), page_json_n(10, 0, 2_000_000));

        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 10);
    }

    #[tokio::test]
    async fn rate_limited_page_is_retried_not_skipped() {
        // PASS: wallet's trades appear in cache after 3 rate-limit responses.
        // FAIL: cache is empty (wallet was skipped on rate limit).
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url(wallet, 0), page_json_n(5, 0, 2_000_000));

        let inner = FixtureFetcher::new(responses);
        let fetcher = RateLimitThenSucceedFetcher::new(inner, 3);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(
            cache.trade_count(),
            5,
            "rate-limited pages must be retried — wallet trades must not be skipped"
        );
    }

    #[tokio::test]
    async fn fetch_all_returns_partial_fetch_error_when_wallet_fails() {
        // PASS: fetch_all returns Err(PartialFetch) when a wallet's page URL has no fixture
        //       (simulates a network/API failure after all retries).
        // FAIL: fetch_all returns Ok — data loss is silently swallowed.
        let wallet = wallet_a();
        let fetcher = FixtureFetcher::new(HashMap::new()); // no fixture → Fatal error
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        let result = bulk.fetch_all(&[wallet], &mut cache).await;
        assert!(
            matches!(
                result,
                Err(BootstrapError::PartialFetch { failed_wallets: 1 })
            ),
            "non-recoverable fetch error must surface as PartialFetch, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn page_sort_handles_out_of_order_api_response() {
        // API returns page in ascending order (oldest-first). After defensive sort
        // (newest-first), new trade appears before known trades → gets appended.
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Cold run: store 3 old trades.
        let old_page = page_json_hashes(&[
            ("0xold1", 1_000_003),
            ("0xold2", 1_000_002),
            ("0xold3", 1_000_001),
        ]);
        let mut r1 = HashMap::new();
        r1.insert(trade_url(wallet, 0), old_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        // Incremental: page arrives oldest-first (ascending timestamps).
        // After sort → new1(2M), old1(1_000_003), old2(1_000_002), old3(1_000_001).
        // new1 unknown → append; old1 known(1), old2 known(2), old3 known(3) → stop.
        let reversed_page = page_json_hashes(&[
            ("0xold3", 1_000_001),
            ("0xold2", 1_000_002),
            ("0xold1", 1_000_003),
            ("0xnew1", 2_000_000),
        ]);
        let mut r2 = HashMap::new();
        r2.insert(trade_url(wallet, 0), reversed_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        assert_eq!(
            cache.trade_count(),
            4,
            "new trade before known sequence must be appended"
        );
    }
}
