//! Polymarket bulk trade fetcher — incremental, cache-first, concurrent.
//!
//! For each wallet: performs a two-phase cursor walk against
//! `/activity?type=TRADE`:
//!   - **Phase 1 (backward fill)** — walks from the oldest cached timestamp
//!     backward, filling any historical gap. Cold-start wallets use an
//!     unbounded initial request and walk until an empty or partial page.
//!   - **Phase 2 (forward fill)** — walks forward from the newest cached
//!     timestamp, collecting new trades. `INCREMENTAL_STOP_THRESHOLD`
//!     consecutive already-known IDs act as an overlap safety net.
//!
//! Each fetched page is sorted newest-first (defensive: API normally does this
//! but the sort removes the silent-failure mode if ordering changes).
//!
//! `fetch_all` runs up to `concurrency` per-wallet fetches in parallel; the
//! shared `PageFetcher` enforces the global rate-limit gate so aggregate
//! throughput stays inside the Polymarket Data API limit (200 req/10s, ≈ 20 req/s).

use std::collections::HashSet;
use std::sync::Arc;
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
    outcome_index: Option<u16>,
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
    wallet_timeout_secs: u64,
    stamp_on_success: bool,
}

/// Per-batch result of [`PolymarketBulkFetcher::fetch_all`].
///
/// Per-wallet errors are in-band; the caller decides whether partial success is
/// acceptable. Catastrophic errors (cache mutex poison, etc.) still propagate
/// as `Err` on the outer `Result`.
#[derive(Debug, Clone)]
pub struct FetchOutcome {
    /// Number of wallets in the input batch.
    pub attempted: usize,
    /// Wallets whose trades could not be fetched or written to SQLite. Their
    /// progress was rolled back per-wallet; the rest of the batch succeeded.
    pub failed: Vec<WalletAddress>,
}

impl FetchOutcome {
    /// Number of wallets whose trades were successfully fetched and persisted.
    pub fn succeeded_count(&self) -> usize {
        self.attempted.saturating_sub(self.failed.len())
    }
}

impl<F: PageFetcher> PolymarketBulkFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            base_url,
            fetcher,
            concurrency: DEFAULT_CONCURRENCY,
            // `0` (disabled) is the test-friendly default — existing fixture-based
            // tests (29 call sites of `new()` across this file and `tests/`) rely
            // on no per-wallet timeout. Production callers in `lib.rs::run()` and
            // `backfill::run_backfill` activate the timeout by chaining
            // `.with_wallet_timeout(config.polymarket_wallet_timeout_secs)` whose
            // canonical default is 300s (see `docs/_GLOSSARY.md`).
            wallet_timeout_secs: 0,
            // `false` preserves the venue-agnostic posture — callers that want
            // the pile's `last_polymarket_fetch_at` updated per-wallet opt in
            // via `with_stamp_on_success(true)` (set by `backfill::run_backfill`).
            stamp_on_success: false,
        }
    }

    /// Override the wallet-fetch concurrency (clamped to `>= 1`).
    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Override the per-wallet wall-clock timeout (in seconds). `0` disables the
    /// timeout entirely (the default in [`Self::new`]); any positive value wraps
    /// each `fetch_wallet_incremental` call in `tokio::time::timeout`. Wallets
    /// whose total fetch wall-clock exceeds the budget are added to
    /// [`FetchOutcome::failed`] via the standard soft-fail path (no SQLite write,
    /// `last_polymarket_fetch_at` not stamped → re-queued by the next backfill).
    pub fn with_wallet_timeout(mut self, secs: u64) -> Self {
        self.wallet_timeout_secs = secs;
        self
    }

    /// Enable per-wallet incremental stamping of `last_polymarket_fetch_at`.
    ///
    /// When `true`, each wallet's successful fetch (including the empty-page
    /// case where there are no new trades) immediately writes
    /// `last_polymarket_fetch_at = now()` to the `wallets` table under the same
    /// `cache_mutex` guard that committed its trades. Failures (fetch error,
    /// timeout, or `insert_new` error) do **not** stamp the wallet.
    ///
    /// This makes SIGINT mid-run preserve all completed work — succeeded
    /// wallets keep their stamps, in-flight ones stay NULL and are re-queued
    /// by the next `select_backfill_due`. Without this flag, callers must
    /// stamp in a post-fetch loop (the legacy `pe-bootstrap::run` path) which
    /// loses the stamps if the process exits before `fetch_all` returns.
    ///
    /// Default `false` to preserve the venue-agnostic stance of `fetch_all`
    /// for the legacy seed-bootstrap path; `backfill::run_backfill` opts in.
    pub fn with_stamp_on_success(mut self, on: bool) -> Self {
        self.stamp_on_success = on;
        self
    }

    /// Fetch new trades for all `wallets`, updating `cache` incrementally.
    ///
    /// Up to `self.concurrency` wallets are fetched in parallel; the underlying
    /// `PageFetcher` enforces the global rate-limit gate. HTTP 429 responses are
    /// retried indefinitely at the page level (sleeping `Retry-After` seconds);
    /// the only mechanism that skips a wallet on rate limiting is the per-wallet
    /// wall-clock budget set via [`Self::with_wallet_timeout`] — when configured,
    /// a wallet whose total fetch time exceeds the budget is soft-failed via
    /// [`FetchOutcome::failed`] (no SQLite write happens, `last_polymarket_fetch_at`
    /// stays NULL → re-queued by the next backfill).
    ///
    /// Per-wallet errors (non-recoverable network/parse error, SQLite insert failure,
    /// or timeout-elapsed) are **not** returned as `Err` — they're captured in
    /// [`FetchOutcome::failed`]. This lets callers decide per-use-case how to
    /// handle partial success:
    ///
    /// - The legacy `lib.rs::run()` pipeline checks `outcome.failed.is_empty()` and
    ///   errors on partial fetch (preserving prior behaviour).
    /// - `backfill.rs::run_backfill` continues the post-fetch pipeline
    ///   (resolutions + activation) and surfaces the partial-fetch error only at the
    ///   very end. It also chains [`Self::with_stamp_on_success`] so per-wallet
    ///   `last_polymarket_fetch_at` stamping happens inline as each wallet completes,
    ///   making SIGINT-mid-run preserve all completed work.
    ///
    /// `Err` is reserved for catastrophic failures the caller cannot recover from
    /// (none are currently emitted from this method; future internal-bug surfaces).
    ///
    /// Trades are committed to SQLite per-wallet inside a single transaction.
    /// SQLite WAL mode provides per-commit durability — no manual checkpointing
    /// or final flush is needed. When [`Self::with_stamp_on_success`] is enabled,
    /// the stamp write occurs under the same `cache_mutex` guard that committed
    /// the trades so a successful wallet's `(trades, stamp)` pair is atomic from
    /// the operator's point of view.
    pub async fn fetch_all(
        &self,
        wallets: &[WalletAddress],
        cache: &mut WalletCache,
    ) -> Result<FetchOutcome, BootstrapError> {
        let cache_mutex: Mutex<&mut WalletCache> = Mutex::new(cache);
        let failed: Arc<Mutex<Vec<WalletAddress>>> = Arc::new(Mutex::new(Vec::new()));

        stream::iter(wallets.iter().copied())
            .for_each_concurrent(self.concurrency, |wallet| {
                let cache_mutex = &cache_mutex;
                let failed = Arc::clone(&failed);
                async move {
                    let wallet_hex = wallet.to_string();
                    let (known_ids, ts_bounds) = {
                        let guard = cache_mutex.lock().await;
                        let ids: HashSet<SourceTradeId> =
                            guard.known_trade_ids(&wallet_hex).into_iter().collect();
                        let bounds = guard.trade_ts_bounds(&wallet_hex).ok().flatten();
                        (ids, bounds)
                    };

                    let fetch_future =
                        self.fetch_wallet_incremental(wallet, &known_ids, ts_bounds);
                    let fetch_result = if self.wallet_timeout_secs > 0 {
                        match tokio::time::timeout(
                            Duration::from_secs(self.wallet_timeout_secs),
                            fetch_future,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(_elapsed) => {
                                tracing::error!(
                                    wallet = %wallet_hex,
                                    timeout_secs = self.wallet_timeout_secs,
                                    "polymarket: fetch timeout exceeded — trades for this wallet will be missing"
                                );
                                failed.lock().await.push(wallet);
                                return;
                            }
                        }
                    } else {
                        fetch_future.await
                    };

                    match fetch_result {
                        Ok(new_trades) => {
                            let mut guard = cache_mutex.lock().await;
                            // Empty pages still count as a successful fetch and
                            // are stamped below; skip the no-op insert call.
                            if !new_trades.is_empty()
                                && let Err(e) = guard.insert_new(&wallet_hex, new_trades)
                            {
                                tracing::error!(
                                    wallet = %wallet_hex,
                                    error = %e,
                                    "polymarket: cache insert failed — trades for this wallet will be missing"
                                );
                                failed.lock().await.push(wallet);
                                return;
                            }
                            if self.stamp_on_success {
                                let now_unix = OffsetDateTime::now_utc().unix_timestamp();
                                if let Err(e) =
                                    guard.update_last_polymarket_fetch(&wallet_hex, now_unix)
                                {
                                    // Trades are durable; missing stamp just means
                                    // the next `select_backfill_due` will re-queue
                                    // this wallet and the 3-known-IDs early-stop
                                    // makes the re-fetch cheap. Not a hard failure.
                                    tracing::warn!(
                                        wallet = %wallet_hex,
                                        error = %e,
                                        "polymarket: stamp failed — wallet will be re-queued next backfill (trades are persisted)"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                wallet = %wallet_hex,
                                error = %e,
                                "polymarket: fetch failed — trades for this wallet will be missing"
                            );
                            failed.lock().await.push(wallet);
                        }
                    }
                }
            })
            .await;

        let failed_vec = Arc::try_unwrap(failed)
            .map_err(|_| BootstrapError::Internal)?
            .into_inner();
        Ok(FetchOutcome {
            attempted: wallets.len(),
            failed: failed_vec,
        })
    }

    /// Fetch trades not already in `known_ids` for a single wallet via two-phase cursor walk.
    ///
    /// Phase 1 (backward fill): walks backward from `oldest_cached_ts - 1` (or unbounded for
    /// cold-start) until an empty or partial page signals no more history.
    /// Phase 2 (forward fill): walks forward from `newest_cached_ts` (incremental only) until
    /// empty or partial page; `INCREMENTAL_STOP_THRESHOLD` consecutive known IDs act as an
    /// overlap safety net.
    ///
    /// Cursor semantics (empirically verified): `end` is inclusive (trade at `end` returned);
    /// `start` is exclusive (trade at `start` not returned).
    async fn fetch_wallet_incremental(
        &self,
        wallet: WalletAddress,
        known_ids: &HashSet<SourceTradeId>,
        ts_bounds: Option<(i64, i64)>,
    ) -> Result<Vec<RawTrade>, BootstrapError> {
        let wallet_hex = wallet.to_string();
        let mut new_trades: Vec<RawTrade> = Vec::new();

        // ── Phase 1: Backward fill ────────────────────────────────────────────
        // Cold start: no initial end cursor (fetch from present backward).
        // Incremental: begin at oldest_cached_ts - 1 to fill any gap below cache floor.
        // end is inclusive: advance cursor to min(page_timestamps) - 1 each page.
        let mut end_cursor: Option<i64> = ts_bounds.map(|(oldest, _)| oldest - 1);

        'backward: loop {
            let url = PolymarketEndpoint::UserTradeActivity {
                user: wallet_hex.clone(),
                end: end_cursor,
                start: None,
            }
            .url(&self.base_url);

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

            if page.is_empty() {
                break 'backward;
            }

            // Sort newest-first (defensive; API normally returns this order).
            page.sort_by_key(|t| std::cmp::Reverse(t.timestamp.0));

            let min_ts = page
                .iter()
                .map(|t| t.timestamp.0.unix_timestamp())
                .min()
                .unwrap_or(0);

            for trade in page {
                if !known_ids.contains(&trade.source_trade_id) {
                    new_trades.push(trade);
                }
            }

            if raw_count < TRADE_FETCH_LIMIT as usize {
                break 'backward;
            }

            // Advance cursor: end is inclusive, subtract 1 to exclude current minimum.
            end_cursor = Some(min_ts - 1);
        }

        // ── Phase 2: Forward fill (incremental only) ──────────────────────────
        // Walk forward from newest cached trade to pick up new trades.
        // start is exclusive: advance to max(page_timestamps) each page so the next
        // request returns only trades strictly newer than those already seen.
        if let Some((_, newest_ts)) = ts_bounds {
            let mut start_cursor = newest_ts;
            let mut consecutive_known: usize = 0;

            'forward: loop {
                let url = PolymarketEndpoint::UserTradeActivity {
                    user: wallet_hex.clone(),
                    end: None,
                    start: Some(start_cursor),
                }
                .url(&self.base_url);

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

                let (mut page, raw_count) =
                    parse_trades_with_count(&bytes, wallet).map_err(|e| {
                        BootstrapError::TradeParse {
                            wallet: wallet_hex.clone(),
                            message: e,
                        }
                    })?;

                if page.is_empty() {
                    break 'forward;
                }

                // Sort newest-first (defensive).
                page.sort_by_key(|t| std::cmp::Reverse(t.timestamp.0));

                let max_ts = page
                    .iter()
                    .map(|t| t.timestamp.0.unix_timestamp())
                    .max()
                    .unwrap_or(0);

                for trade in page {
                    if known_ids.contains(&trade.source_trade_id) {
                        consecutive_known += 1;
                        if consecutive_known >= INCREMENTAL_STOP_THRESHOLD {
                            break 'forward;
                        }
                    } else {
                        consecutive_known = 0;
                        new_trades.push(trade);
                    }
                }

                if raw_count < TRADE_FETCH_LIMIT as usize {
                    break;
                }

                // Advance cursor: start is exclusive, so max_ts excludes the current page max.
                start_cursor = max_ts;
            }
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

    /// A `PageFetcher` that returns a pending future, simulating an infinitely
    /// slow API (or one stuck in a sustained rate-limit episode). Used to
    /// exercise the per-wallet timeout path without touching the network.
    struct HangFetcher;

    impl PageFetcher for HangFetcher {
        async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
            std::future::pending::<Result<Vec<u8>, SourceError>>().await
        }
    }

    /// A `PageFetcher` that delegates to a `FixtureFetcher` for some URLs and
    /// hangs forever for others. Drives the mixed-wallet operator scenario:
    /// fast wallets finish, slow ones time out, soft-fail bookkeeping survives.
    struct PartialHangFetcher {
        inner: FixtureFetcher,
    }

    impl PartialHangFetcher {
        fn new(inner: FixtureFetcher) -> Self {
            Self { inner }
        }
    }

    impl PageFetcher for PartialHangFetcher {
        async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
            match self.inner.fetch_page(url).await {
                Ok(b) => Ok(b),
                // No fixture → treat as "this wallet's API is hung indefinitely".
                Err(_) => std::future::pending::<Result<Vec<u8>, SourceError>>().await,
            }
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

    /// Cold-start URL: no end/start cursor.
    fn trade_url_cold(wallet: WalletAddress) -> String {
        PolymarketEndpoint::UserTradeActivity {
            user: wallet.to_string(),
            end: None,
            start: None,
        }
        .url(BASE_URL)
    }

    /// Backward-fill URL with an inclusive `end` timestamp cursor.
    fn trade_url_end(wallet: WalletAddress, end: i64) -> String {
        PolymarketEndpoint::UserTradeActivity {
            user: wallet.to_string(),
            end: Some(end),
            start: None,
        }
        .url(BASE_URL)
    }

    /// Forward-fill URL with an exclusive `start` timestamp cursor.
    fn trade_url_start(wallet: WalletAddress, start: i64) -> String {
        PolymarketEndpoint::UserTradeActivity {
            user: wallet.to_string(),
            end: None,
            start: Some(start),
        }
        .url(BASE_URL)
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

    // Issue #159: Polymarket multi-outcome markets observed with outcomeIndex > 255.
    // Pre-#159 this body would parse-fail with `invalid value: integer '999', expected u8`.
    #[test]
    fn parse_outcome_index_above_u8_max_propagated() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":1,"price":0.50,"timestamp":1704067200,"outcomeIndex":999}]"#;
        let trades = parse(json);
        assert_eq!(trades.len(), 1, "multi-outcome trade must parse");
        assert_eq!(trades[0].outcome_id, OutcomeId(999));
    }

    #[test]
    fn parse_outcome_index_at_u16_max_propagated() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":1,"price":0.50,"timestamp":1704067200,"outcomeIndex":65535}]"#;
        let trades = parse(json);
        assert_eq!(trades[0].outcome_id, OutcomeId(u16::MAX));
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

    // ── Pagination: cold start ────────────────────────────────────────────────

    #[tokio::test]
    async fn pagination_concatenates_two_full_pages() {
        // PASS: two full pages fetched via backward cursor walk → 1000 trades in cache.
        // page_json_n(500, i, ts_base): hash_offset i, ts decreasing from ts_base.
        // min_ts of page N = ts_base - 499 → next end = ts_base - 500.
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        // Page 1: ts 3500..3001, min=3001, next end=3000
        responses.insert(trade_url_cold(wallet), page_json_n(500, 0, 3500));
        // Page 2: ts 3000..2501, min=2501, next end=2500
        responses.insert(trade_url_end(wallet, 3000), page_json_n(500, 500, 3000));
        // Page 3: empty → stop
        responses.insert(trade_url_end(wallet, 2500), page_json_n(0, 1000, 0));

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
        responses.insert(trade_url_cold(wallet), page_json_n(499, 0, 2_000_000));

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 499);
    }

    #[tokio::test]
    async fn incremental_fetch_cold_start_fetches_everything() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet), page_json_n(10, 0, 2_000_000));

        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 10);
    }

    // ── Pagination: incremental / forward fill ────────────────────────────────

    #[tokio::test]
    async fn incremental_fetch_stops_after_k_consecutive_known_ids() {
        // PASS: forward fill appends 2 new trades then stops on 3 consecutive known IDs.
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Cold run: 5 old trades (partial page → stops, no further backward cursor).
        let old_page = page_json_hashes(&[
            ("0xold1", 1_000_005),
            ("0xold2", 1_000_004),
            ("0xold3", 1_000_003),
            ("0xold4", 1_000_002),
            ("0xold5", 1_000_001),
        ]);
        // oldest=1_000_001, newest=1_000_005
        let mut r_cold = HashMap::new();
        r_cold.insert(trade_url_cold(wallet), old_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_cold))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();
        assert_eq!(cache.trade_count(), 5);

        // Incremental run:
        //   Phase 1 backward (end = 1_000_001 - 1 = 1_000_000) → empty → stop.
        //   Phase 2 forward (start = 1_000_005): page with 2 new then 3 consecutive known.
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
        r_incr.insert(trade_url_end(wallet, 1_000_000), page_json_n(0, 0, 0));
        r_incr.insert(trade_url_start(wallet, 1_000_005), incr_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_incr))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        assert_eq!(cache.trade_count(), 7, "5 old + 2 new = 7");
    }

    // ── Pagination: backward fill fills history below cache floor ─────────────

    #[tokio::test]
    async fn incremental_backward_fill_fetches_history_below_cache_floor() {
        // PASS: second run's backward phase appends 3 historical trades older than the cache
        //       floor while the forward phase finds no new trades.
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Cold run: 3 trades (partial page). oldest=998, newest=1000.
        let cold_page =
            page_json_hashes(&[("0xrecent1", 1000), ("0xrecent2", 999), ("0xrecent3", 998)]);
        let mut r_cold = HashMap::new();
        r_cold.insert(trade_url_cold(wallet), cold_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_cold))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();
        assert_eq!(cache.trade_count(), 3);

        // Incremental run:
        //   Phase 1 backward (end = 998 - 1 = 997) → 3 historical trades (partial) → stop.
        //   Phase 2 forward (start = 1000) → empty → stop.
        let hist_page = page_json_hashes(&[("0xhist1", 500), ("0xhist2", 499), ("0xhist3", 498)]);
        let mut r_incr = HashMap::new();
        r_incr.insert(trade_url_end(wallet, 997), hist_page);
        r_incr.insert(trade_url_start(wallet, 1000), page_json_n(0, 0, 0));
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_incr))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        assert_eq!(cache.trade_count(), 6, "3 recent + 3 historical = 6");
    }

    // ── Pagination: beyond old offset-3000 cap ────────────────────────────────

    #[tokio::test]
    async fn incremental_walk_walks_past_offset_3000_equivalent() {
        // PASS: cursor walk fetches 3500 trades — 500 more than the old offset=3000 hard cap.
        // page_json_n(500, hash_off, ts_base): min_ts = ts_base - 499, next end = ts_base - 500.
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        // 7 full pages + 1 terminating empty page.
        responses.insert(trade_url_cold(wallet), page_json_n(500, 0, 3500));
        responses.insert(trade_url_end(wallet, 3000), page_json_n(500, 500, 3000));
        responses.insert(trade_url_end(wallet, 2500), page_json_n(500, 1000, 2500));
        responses.insert(trade_url_end(wallet, 2000), page_json_n(500, 1500, 2000));
        responses.insert(trade_url_end(wallet, 1500), page_json_n(500, 2000, 1500));
        responses.insert(trade_url_end(wallet, 1000), page_json_n(500, 2500, 1000));
        responses.insert(trade_url_end(wallet, 500), page_json_n(500, 3000, 500));
        // min_ts of last page = 500 - 499 = 1, next end = 0
        responses.insert(trade_url_end(wallet, 0), page_json_n(0, 3500, 0));

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        assert_eq!(
            cache.trade_count(),
            3500,
            "all 3500 trades must be fetched without offset cap"
        );
    }

    // ── Rate limiting ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn rate_limited_page_is_retried_not_skipped() {
        // PASS: wallet's trades appear in cache after 3 rate-limit responses.
        // FAIL: cache is empty (wallet was skipped on rate limit).
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet), page_json_n(5, 0, 2_000_000));

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
    async fn fetch_all_returns_outcome_with_failed_wallet() {
        // PASS: fetch_all returns Ok(FetchOutcome) listing the failed wallet by address.
        //       Per-wallet failures are in-band so callers (run_backfill) can decide
        //       whether to fail-soft or hard.
        // FAIL: fetch_all returns Err on per-wallet failure (legacy behaviour) — would
        //       abort callers that want to continue the pipeline.
        let wallet = wallet_a();
        let fetcher = FixtureFetcher::new(HashMap::new()); // no fixture → Fatal error
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        let outcome = bulk
            .fetch_all(&[wallet], &mut cache)
            .await
            .expect("Ok with FetchOutcome");
        assert_eq!(outcome.attempted, 1);
        assert_eq!(
            outcome.failed,
            vec![wallet],
            "failed wallet must be exposed"
        );
        assert_eq!(outcome.succeeded_count(), 0);
    }

    #[tokio::test]
    async fn fetch_all_mixed_success_returns_only_failed_wallet() {
        // PASS: in a 2-wallet batch where one wallet has a fixture and one does not,
        //       FetchOutcome.failed contains only the missing-fixture wallet, the
        //       successful wallet's trades are persisted, succeeded_count == 1.
        // FAIL: failed list contains the successful wallet, OR trades from the
        //       successful wallet are missing from the cache.
        let wallet_ok = wallet_a();
        let wallet_fail =
            WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();

        // Cold-start URL only for wallet_ok with one tiny page (partial → stops).
        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet_ok), page_json_n(3, 0, 1_700_000_000));
        // wallet_fail's URL has no fixture → FixtureFetcher returns Fatal.

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);

        let outcome = bulk
            .fetch_all(&[wallet_ok, wallet_fail], &mut cache)
            .await
            .expect("Ok with FetchOutcome");

        assert_eq!(outcome.attempted, 2);
        assert_eq!(
            outcome.failed,
            vec![wallet_fail],
            "only wallet_fail must fail"
        );
        assert_eq!(outcome.succeeded_count(), 1);
        // Successful wallet's trades persisted.
        assert_eq!(
            cache.trade_count(),
            3,
            "wallet_ok's 3 trades must be cached"
        );
        // Failed wallet has no trades.
        let fail_ids = cache.known_trade_ids(&wallet_fail.to_string());
        assert!(
            fail_ids.is_empty(),
            "wallet_fail must have no cached trades"
        );
    }

    // ── Defensive sort ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn page_sort_handles_out_of_order_api_response() {
        // Forward-fill page arrives oldest-first (ascending). After defensive sort
        // (newest-first): new1 appears before known trades → appended; then 3
        // consecutive known IDs trigger the stop threshold.
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Cold run: 3 old trades (partial). oldest=1_000_001, newest=1_000_003.
        let old_page = page_json_hashes(&[
            ("0xold1", 1_000_003),
            ("0xold2", 1_000_002),
            ("0xold3", 1_000_001),
        ]);
        let mut r1 = HashMap::new();
        r1.insert(trade_url_cold(wallet), old_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        // Incremental run:
        //   Phase 1 backward (end = 1_000_001 - 1 = 1_000_000) → empty.
        //   Phase 2 forward (start = 1_000_003): page arrives ascending (oldest-first).
        //   After sort → [new1(2M), old1(1M+3), old2(1M+2), old3(1M+1)] → append new1,
        //   then 3 consecutive known → stop.
        let reversed_page = page_json_hashes(&[
            ("0xold3", 1_000_001),
            ("0xold2", 1_000_002),
            ("0xold1", 1_000_003),
            ("0xnew1", 2_000_000),
        ]);
        let mut r2 = HashMap::new();
        r2.insert(trade_url_end(wallet, 1_000_000), page_json_n(0, 0, 0));
        r2.insert(trade_url_start(wallet, 1_000_003), reversed_page);
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

    // ── Per-wallet timeout (issue #173) ───────────────────────────────────────

    /// PASS: a wallet whose fetcher never returns is soft-failed via
    ///       `outcome.failed`, the cache stays empty (no partial write), and
    ///       the timeout fires within ~1 s rather than hanging the test.
    /// FAIL: `outcome.failed` is empty (wallet was treated as a success), OR
    ///       the cache contains trades (cancellation-safety regression), OR
    ///       elapsed wall-clock exceeds the 5 s upper bound.
    #[tokio::test]
    async fn wallet_timeout_soft_fails_hanging_wallet() {
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let bulk =
            PolymarketBulkFetcher::new(BASE_URL.to_owned(), HangFetcher).with_wallet_timeout(1);

        let start = std::time::Instant::now();
        let outcome = bulk
            .fetch_all(&[wallet], &mut cache)
            .await
            .expect("Ok with FetchOutcome");
        let elapsed = start.elapsed();

        assert_eq!(outcome.attempted, 1);
        assert_eq!(
            outcome.failed,
            vec![wallet],
            "hanging wallet must be soft-failed"
        );
        assert_eq!(outcome.succeeded_count(), 0);
        assert_eq!(
            cache.trade_count(),
            0,
            "cancellation-safety regression: timeout-cancelled walk must not write to cache"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "timeout must fire promptly; elapsed = {elapsed:?}"
        );
    }

    /// PASS: with `with_wallet_timeout(0)` the timeout is disabled and a
    ///       fixture-backed wallet completes normally — proves the
    ///       config-disabled path doesn't break the success flow.
    /// FAIL: timeout fires anyway, cache is empty, or `outcome.failed` is
    ///       non-empty when the fixture returns a clean response.
    #[tokio::test]
    async fn wallet_timeout_disabled_passes_through_success() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet), page_json_n(5, 0, 2_000_000));

        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
            .with_wallet_timeout(0);
        let outcome = bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert!(
            outcome.failed.is_empty(),
            "disabled timeout must not soft-fail"
        );
        assert_eq!(outcome.succeeded_count(), 1);
        assert_eq!(cache.trade_count(), 5);
    }

    /// PASS: in a 2-wallet batch where the fast wallet has a fixture and the
    ///       slow one hangs forever, the fast wallet is fully persisted, the
    ///       slow one is soft-failed, and overall wall-clock is bounded by
    ///       the timeout (not by the hanging fetcher).
    /// FAIL: fast wallet's trades are missing, OR slow wallet is treated as
    ///       success, OR test exceeds 5 s (queue tail not freed by timeout).
    #[tokio::test]
    async fn wallet_timeout_mixed_batch_isolates_stragglers() {
        let fast = wallet_a();
        let slow = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();

        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(fast), page_json_n(3, 0, 1_700_000_000));
        // slow's URL has no fixture → PartialHangFetcher returns pending().

        let fetcher = PartialHangFetcher::new(FixtureFetcher::new(responses));
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher).with_wallet_timeout(1);

        let start = std::time::Instant::now();
        let outcome = bulk.fetch_all(&[fast, slow], &mut cache).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(outcome.attempted, 2);
        assert_eq!(
            outcome.failed,
            vec![slow],
            "only the hanging wallet must be soft-failed"
        );
        assert_eq!(outcome.succeeded_count(), 1);
        assert_eq!(
            cache.trade_count(),
            3,
            "fast wallet's trades must be persisted"
        );
        let slow_ids = cache.known_trade_ids(&slow.to_string());
        assert!(
            slow_ids.is_empty(),
            "slow wallet must have no cached trades"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "queue tail freed promptly by timeout; elapsed = {elapsed:?}"
        );
    }

    /// PASS: `new()` constructs a fetcher with timeout disabled by default —
    ///       proves the 29 existing fixture-based tests continue to behave as
    ///       before (the disabled-default protects them).
    /// FAIL: a non-zero default leaks into `new()` and breaks unrelated tests.
    #[tokio::test]
    async fn new_constructor_disables_timeout_by_default() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet), page_json_n(2, 0, 1_700_000_000));

        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // No `with_wallet_timeout(_)` chain — exactly the shape used by the
        // 29 existing scenario and unit tests.
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
        let outcome = bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert!(outcome.failed.is_empty());
        assert_eq!(cache.trade_count(), 2);
    }

    // ── Incremental stamping (issue #175) ─────────────────────────────────────

    /// Insert the wallet into the pile as an active leaderboard wallet so
    /// `select_backfill_due` returns it (until stamped) and
    /// `update_last_polymarket_fetch` has a row to update.
    fn upsert_active_pile_row(cache: &mut WalletCache, wallet: WalletAddress) {
        cache
            .upsert_wallet(
                &wallet.to_string(),
                crate::pile::SRC_LEADERBOARD,
                false,
                None,
                None,
                None,
            )
            .unwrap();
        crate::pile::apply_activation_rules(cache).unwrap();
    }

    /// PASS: with `with_stamp_on_success(true)`, a successful fetch writes
    ///       `last_polymarket_fetch_at` to the wallet's pile row inline.
    /// FAIL: stamp column stays NULL after a successful fetch.
    #[tokio::test]
    async fn stamp_on_success_persists_timestamp_inline() {
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        upsert_active_pile_row(&mut cache, wallet);

        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet), page_json_n(3, 0, 1_700_000_000));

        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
            .with_stamp_on_success(true);
        let outcome = bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert!(outcome.failed.is_empty());
        assert_eq!(cache.trade_count(), 3);

        let due = crate::pile::select_backfill_due(&cache, 1_700_000_001, 0).unwrap();
        assert!(
            due.is_empty(),
            "stamped wallet must not be re-queued (stamp = now)"
        );
    }

    /// PASS: with `with_stamp_on_success(true)`, an empty-page fetch (no new
    ///       trades — e.g. cache already current) also stamps the wallet.
    /// FAIL: empty-page success does not stamp (operator sees the wallet
    ///       re-queued every run when nothing has changed).
    #[tokio::test]
    async fn stamp_on_success_persists_timestamp_on_empty_page() {
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        upsert_active_pile_row(&mut cache, wallet);

        let mut responses = HashMap::new();
        // Empty response on cold-start URL = "wallet has no Polymarket trades".
        responses.insert(trade_url_cold(wallet), page_json_n(0, 0, 0));

        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
            .with_stamp_on_success(true);
        let outcome = bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert!(outcome.failed.is_empty());
        assert_eq!(cache.trade_count(), 0);

        let due = crate::pile::select_backfill_due(&cache, 1_700_000_001, 0).unwrap();
        assert!(
            due.is_empty(),
            "empty-page success must still stamp the wallet (otherwise zero-trade wallets are re-queued every run)"
        );
    }

    /// PASS: a failed fetch with `with_stamp_on_success(true)` leaves the wallet
    ///       NULL — next `select_backfill_due` re-queues it.
    /// FAIL: failed wallet is stamped (false success), breaking the re-queue
    ///       semantics that PR #171 + #173 established.
    #[tokio::test]
    async fn stamp_on_success_skips_failed_wallet() {
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        upsert_active_pile_row(&mut cache, wallet);

        // No fixture → FixtureFetcher returns Fatal → wallet lands in `failed`.
        let bulk =
            PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(HashMap::new()))
                .with_stamp_on_success(true);
        let outcome = bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(outcome.failed, vec![wallet]);

        let due = crate::pile::select_backfill_due(&cache, 1_700_000_001, 0).unwrap();
        assert_eq!(
            due,
            vec![wallet.to_string()],
            "failed wallet must remain NULL and re-queue on next backfill"
        );
    }

    /// PASS: default `new()` does NOT stamp — preserves the venue-agnostic
    ///       behaviour for the 29 existing fixture-based call sites + the
    ///       legacy `lib.rs::run()` path.
    /// FAIL: a non-opt-in fetcher silently writes to the pile table.
    #[tokio::test]
    async fn default_no_stamp_preserves_legacy_behavior() {
        let wallet = wallet_a();
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        upsert_active_pile_row(&mut cache, wallet);

        let mut responses = HashMap::new();
        responses.insert(trade_url_cold(wallet), page_json_n(2, 0, 1_700_000_000));

        // No `with_stamp_on_success` — matches existing scenario callers.
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
        let outcome = bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert!(outcome.failed.is_empty());

        let due = crate::pile::select_backfill_due(&cache, 1_700_000_001, 0).unwrap();
        assert_eq!(
            due,
            vec![wallet.to_string()],
            "without opt-in, wallet must remain NULL (caller is responsible for stamping)"
        );
    }
}
