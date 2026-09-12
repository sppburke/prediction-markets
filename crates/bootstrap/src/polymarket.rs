//! Polymarket bulk trade fetcher — incremental, cache-first, concurrent.
//!
//! Backward history is committed page by page, completing each boundary second
//! before moving below it. Forward history uses complete fixed-end windows and
//! is committed whole-or-nothing (#609 incremental forward commits are separate).
//! A durable partial marker quarantines interrupted histories from schema-one
//! ranking; successful completion clears it atomically with the optional stamp.
//!
//! `fetch_all` runs up to `concurrency` wallets in parallel. The shared
//! `PageFetcher` enforces the venue-wide rate-limit gate.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::reconciliation::ACTIVITY_MAX_OFFSET;
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::cache::WalletCache;
use crate::error::BootstrapError;
use crate::infra_probe::{InfraProbe, ProbeClassification};

const TRADE_FETCH_LIMIT: u32 = 500;

#[derive(Debug)]
enum WalletFetchResult {
    Complete,
    Infra { span_secs: i64 },
}

#[derive(Default)]
struct CommitProgress {
    pages_committed: u64,
    trades_committed: u64,
}

enum WindowResult {
    Complete(Vec<RawTrade>),
    Saturated { boundary: i64 },
}

enum ForwardPiece {
    Window { lo: i64, hi: i64 },
    Second(i64),
}

struct TradePage {
    rows: Vec<RawTrade>,
    raw_count: usize,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

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
    /// Infra-wallet probe (issue #197). Stashed at construction so the env
    /// var is read once per fetcher, keeping the threshold deterministic
    /// across all wallets in a run.
    infra_probe: InfraProbe,
    #[cfg(any(test, feature = "scenario"))]
    clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
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
    /// completed backward pages remain durable and the wallet stays partial.
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
            infra_probe: InfraProbe::default(),
            #[cfg(any(test, feature = "scenario"))]
            clock: None,
        }
    }

    /// Inject the wall clock for deterministic scenario requests and stamps.
    #[cfg(any(test, feature = "scenario"))]
    pub fn with_clock_for_test(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Some(Arc::new(clock));
        self
    }

    fn now_unix(&self) -> i64 {
        #[cfg(any(test, feature = "scenario"))]
        if let Some(clock) = &self.clock {
            return clock();
        }
        OffsetDateTime::now_utc().unix_timestamp()
    }

    /// Override the wallet-fetch concurrency (clamped to `>= 1`).
    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Set the per-wallet timeout; zero disables it. On timeout, committed
    /// backward progress survives, the marker stays partial, and the old stamp
    /// is preserved so due wallets remain due.
    pub fn with_wallet_timeout(mut self, secs: u64) -> Self {
        self.wallet_timeout_secs = secs;
        self
    }

    /// Stamp each completed wallet in the same transaction that clears its
    /// partial marker. With stamping disabled, completion still clears the marker
    /// and preserves the old (including NULL) stamp. Errors never stamp.
    pub fn with_stamp_on_success(mut self, on: bool) -> Self {
        self.stamp_on_success = on;
        self
    }

    /// Override the infra-wallet probe (issue #197). Default reads
    /// `PE_BOOTSTRAP_INFRA_SPAN_SECS` once at construction. Set
    /// `threshold_secs = 0` to effectively disable the probe (span >= 0
    /// always, so `span < 0` is never true → never classifies Infra).
    /// Used by tests with dense fixture data that would otherwise trigger
    /// the probe.
    pub fn with_infra_probe(mut self, probe: InfraProbe) -> Self {
        self.infra_probe = probe;
        self
    }

    /// Fetch wallets concurrently, retaining completed backward pages on failure.
    /// HTTP 429 retries stay inside the wallet timeout. Network, parse, insert,
    /// and completion errors are reported in `FetchOutcome::failed`; callers
    /// choose whether to continue their post-fetch pipeline.
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
                    let hi = self.now_unix();
                    let mut progress = CommitProgress::default();
                    let fetch_future = self.fetch_wallet_incremental(
                        wallet, hi, cache_mutex, &mut progress,
                    );
                    let fetch_result = if self.wallet_timeout_secs > 0 {
                        match tokio::time::timeout(
                            Duration::from_secs(self.wallet_timeout_secs), fetch_future,
                        ).await {
                            Ok(result) => result,
                            Err(_) => {
                                tracing::warn!(
                                    wallet = %wallet_hex,
                                    timeout_secs = self.wallet_timeout_secs,
                                    pages_committed = progress.pages_committed,
                                    trades_committed = progress.trades_committed,
                                    "polymarket: fetch timeout — committed progress kept; wallet stays due"
                                );
                                failed.lock().await.push(wallet);
                                return;
                            }
                        }
                    } else {
                        fetch_future.await
                    };

                    match fetch_result {
                        Ok(WalletFetchResult::Complete) => {}
                        Ok(WalletFetchResult::Infra { span_secs }) => {
                            // Issue #197: probe fired. Mark wallet infra under
                            // the same mutex guard. Do NOT insert trades, do
                            // NOT stamp last_polymarket_fetch_at — wallet stays
                            // out of all re-queue paths via the
                            // active_tradeable_wallets view +
                            // apply_activation_rules gate.
                            let mut guard = cache_mutex.lock().await;
                            if let Err(e) = guard.mark_infra(&wallet_hex)
                                .and_then(|()| guard.finish_backfill(&wallet_hex, None)) {
                                tracing::error!(
                                    wallet = %wallet_hex,
                                    error = %e,
                                    "polymarket: mark_infra failed after probe — wallet will be re-probed next run"
                                );
                                failed.lock().await.push(wallet);
                                return;
                            }
                            tracing::info!(
                                wallet = %wallet_hex,
                                span_secs,
                                "polymarket: wallet flagged infra by probe"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                wallet = %wallet_hex,
                                error = %e,
                                "polymarket: fetch failed — committed progress kept; wallet stays due"
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

    /// Freeze one upper bound for both phases. No cache guard crosses network I/O.
    async fn fetch_wallet_incremental(
        &self,
        wallet: WalletAddress,
        hi: i64,
        cache: &Mutex<&mut WalletCache>,
        progress: &mut CommitProgress,
    ) -> Result<WalletFetchResult, BootstrapError> {
        let wallet_hex = wallet.to_string();
        let (known_ids, ts_bounds) = {
            let mut guard = cache.lock().await;
            let bounds = guard.trade_ts_bounds(&wallet_hex)?;
            let ids: HashSet<_> = guard.known_trade_ids(&wallet_hex).into_iter().collect();
            guard.begin_backfill(&wallet_hex)?;
            (ids, bounds)
        };
        let mut cursor = ts_bounds.map_or(hi, |(oldest, _)| oldest - 1);
        let mut first_page = true;
        while cursor >= 1 {
            let page = self.fetch_page(wallet, 1, cursor, 0).await?;
            if first_page
                && ts_bounds.is_none()
                && let ProbeClassification::Infra { span_secs } =
                    self.infra_probe.classify(&page.rows, page.raw_count)
            {
                return Ok(WalletFetchResult::Infra { span_secs });
            }
            first_page = false;
            if page.raw_count
                < usize::try_from(TRADE_FETCH_LIMIT).map_err(|_| BootstrapError::Internal)?
            {
                Self::commit_rows(cache, &wallet_hex, &known_ids, page.rows, progress).await?;
                break;
            }
            let boundary = page.min_ts.ok_or(BootstrapError::Internal)?;
            let above = page
                .rows
                .into_iter()
                .filter(|trade| trade.timestamp.0.unix_timestamp() > boundary)
                .collect();
            Self::commit_rows(cache, &wallet_hex, &known_ids, above, progress).await?;
            let second = self.fetch_second(wallet, boundary).await?;
            Self::commit_rows(cache, &wallet_hex, &known_ids, second, progress).await?;
            cursor = boundary - 1;
        }

        if let Some((_, newest)) = ts_bounds {
            // Re-acquire the former maximum second before extending above it:
            // indexing may have exposed additional rows since the previous walk.
            let mut forward = self.fetch_second(wallet, newest).await?;
            forward.extend(self.fetch_forward(wallet, newest, hi).await?);
            // #608 scope: ALL forward pieces remain buffered until complete.
            Self::commit_rows(cache, &wallet_hex, &known_ids, forward, progress).await?;
        }
        cache
            .lock()
            .await
            .finish_backfill(&wallet_hex, self.stamp_on_success.then(|| self.now_unix()))?;
        Ok(WalletFetchResult::Complete)
    }

    async fn commit_rows(
        cache: &Mutex<&mut WalletCache>,
        wallet: &str,
        known_ids: &HashSet<SourceTradeId>,
        mut rows: Vec<RawTrade>,
        progress: &mut CommitProgress,
    ) -> Result<(), BootstrapError> {
        rows.retain(|trade| !known_ids.contains(&trade.source_trade_id));
        if rows.is_empty() {
            return Ok(());
        }
        let inserted = cache.lock().await.insert_new(wallet, rows)?;
        progress.pages_committed += 1;
        progress.trades_committed += inserted;
        Ok(())
    }

    async fn fetch_page(
        &self,
        wallet: WalletAddress,
        start: i64,
        end: i64,
        offset: u32,
    ) -> Result<TradePage, BootstrapError> {
        let url = PolymarketEndpoint::UserTradeActivityPage {
            user: wallet.to_string(),
            start: Some(start),
            end,
            offset,
        }
        .url(&self.base_url);
        let bytes = loop {
            match self.fetcher.fetch_page(&url).await {
                Ok(bytes) => break bytes,
                Err(SourceError::RateLimited { retry_after_secs }) => {
                    tracing::warn!(%wallet, retry_after_secs, "polymarket: rate limited, retrying page after backoff");
                    tokio::time::sleep(Duration::from_secs(u64::from(retry_after_secs).max(1)))
                        .await;
                }
                Err(error) => {
                    return Err(BootstrapError::Polymarket {
                        wallet: wallet.to_string(),
                        message: error.to_string(),
                    });
                }
            }
        };
        let page =
            parse_trade_page(&bytes, wallet).map_err(|message| BootstrapError::TradeParse {
                wallet: wallet.to_string(),
                message,
            })?;
        if page.raw_count
            > usize::try_from(TRADE_FETCH_LIMIT).map_err(|_| BootstrapError::Internal)?
            || page.min_ts.is_some_and(|ts| ts < start)
            || page.max_ts.is_some_and(|ts| ts > end)
        {
            return Err(BootstrapError::Polymarket {
                wallet: wallet.to_string(),
                message: format!(
                    "activity page outside requested bounds [{start}, {end}] or page limit"
                ),
            });
        }
        Ok(page)
    }

    /// A window buffers at most 5,500 rows (offsets 0 through 5,000 inclusive).
    async fn fetch_window(
        &self,
        wallet: WalletAddress,
        lo: i64,
        hi: i64,
    ) -> Result<WindowResult, BootstrapError> {
        if lo >= hi {
            return Ok(WindowResult::Complete(Vec::new()));
        }
        let mut rows = Vec::new();
        let mut boundary = hi;
        for offset in (0..=ACTIVITY_MAX_OFFSET)
            .step_by(usize::try_from(TRADE_FETCH_LIMIT).map_err(|_| BootstrapError::Internal)?)
        {
            let page = self.fetch_page(wallet, lo + 1, hi, offset).await?;
            if let Some(min) = page.min_ts {
                boundary = boundary.min(min);
            }
            rows.extend(page.rows);
            if page.raw_count
                < usize::try_from(TRADE_FETCH_LIMIT).map_err(|_| BootstrapError::Internal)?
            {
                return Ok(WindowResult::Complete(rows));
            }
        }
        Ok(WindowResult::Saturated { boundary })
    }

    async fn fetch_second(
        &self,
        wallet: WalletAddress,
        second: i64,
    ) -> Result<Vec<RawTrade>, BootstrapError> {
        let lo = second.checked_sub(1).ok_or(BootstrapError::Internal)?;
        match self.fetch_window(wallet, lo, second).await? {
            WindowResult::Complete(rows) => Ok(rows),
            WindowResult::Saturated { .. } => Err(BootstrapError::SaturatedSecond {
                wallet: wallet.to_string(),
                second,
            }),
        }
    }

    /// Ordered three-way splitting with strict progress and no recursive futures.
    /// Completed pieces remain buffered here until the entire forward walk succeeds.
    async fn fetch_forward(
        &self,
        wallet: WalletAddress,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<RawTrade>, BootstrapError> {
        let mut pending = vec![ForwardPiece::Window { lo, hi }];
        let mut rows = Vec::new();
        while let Some(piece) = pending.pop() {
            match piece {
                ForwardPiece::Second(second) => {
                    rows.extend(self.fetch_second(wallet, second).await?)
                }
                ForwardPiece::Window { lo, hi } => match self.fetch_window(wallet, lo, hi).await? {
                    WindowResult::Complete(complete) => rows.extend(complete),
                    WindowResult::Saturated { boundary } => {
                        if boundary <= lo || boundary > hi {
                            return Err(BootstrapError::Polymarket {
                                wallet: wallet.to_string(),
                                message: format!("invalid split ({lo}, {hi}] at {boundary}"),
                            });
                        }
                        // Push in reverse order: lower, seam second, then upper.
                        if boundary < hi {
                            pending.push(ForwardPiece::Window { lo: boundary, hi });
                        }
                        pending.push(ForwardPiece::Second(boundary));
                        if lo < boundary - 1 {
                            pending.push(ForwardPiece::Window {
                                lo,
                                hi: boundary - 1,
                            });
                        }
                    }
                },
            }
        }
        Ok(rows)
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse `bytes` into trades, returning the raw JSON array count for
/// pagination termination. Uses raw count so individual parse failures
/// do not cause early pagination termination.
fn parse_trade_page(bytes: &[u8], wallet: WalletAddress) -> Result<TradePage, String> {
    let response: TradeResponse =
        serde_json::from_slice(bytes).map_err(|e| format!("json: {e}"))?;
    let raw_count = response.len();
    // Bounds come from RAW rows, including rows the legacy converter rejects.
    // A full page of rejected trades must never masquerade as an empty page.
    let min_ts = response
        .iter()
        .map(|raw| timestamp_seconds(raw.timestamp))
        .min();
    let max_ts = response
        .iter()
        .map(|raw| timestamp_seconds(raw.timestamp))
        .max();
    let mut rows = Vec::with_capacity(raw_count);
    for raw in response {
        match convert_trade(raw, wallet) {
            Ok(trade) => rows.push(trade),
            Err(error) => tracing::warn!(%wallet, %error, "skipping unparseable trade"),
        }
    }
    Ok(TradePage {
        rows,
        raw_count,
        min_ts,
        max_ts,
    })
}

#[cfg(test)]
fn parse_trades_with_count(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<(Vec<RawTrade>, usize), String> {
    let page = parse_trade_page(bytes, wallet)?;
    Ok((page.rows, page.raw_count))
}

fn timestamp_seconds(timestamp: i64) -> i64 {
    if timestamp > 9_999_999_999 {
        timestamp / 1_000
    } else {
        timestamp
    }
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
    let ts_secs = timestamp_seconds(raw.timestamp);
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
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet.to_string(),
            end: 2_000_000_000,
            start: Some(1),
            offset: 0,
        }
        .url(BASE_URL)
    }

    /// Backward-fill URL with an inclusive `end` timestamp cursor.
    fn trade_url_end(wallet: WalletAddress, end: i64) -> String {
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet.to_string(),
            end,
            start: Some(1),
            offset: 0,
        }
        .url(BASE_URL)
    }

    /// Forward-fill URL with an exclusive `start` timestamp cursor.
    fn trade_url_start(wallet: WalletAddress, start: i64) -> String {
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet.to_string(),
            end: 2_000_000_000,
            start: Some(start + 1),
            offset: 0,
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
        for (second, id) in [
            (3001, 499),
            (2501, 999),
            (2001, 1499),
            (1501, 1999),
            (1001, 2499),
            (501, 2999),
            (1, 3499),
        ] {
            responses.insert(
                PolymarketEndpoint::UserTradeActivityPage {
                    user: wallet.to_string(),
                    start: Some(second),
                    end: second,
                    offset: 0,
                }
                .url(BASE_URL),
                page_json_n(1, id, second),
            );
        }

        // Page 2: ts 3000..2501, min=2501, next end=2500
        responses.insert(trade_url_end(wallet, 3000), page_json_n(500, 500, 3000));
        // Page 3: empty → stop
        responses.insert(trade_url_end(wallet, 2500), page_json_n(0, 1000, 0));

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        // Disable probe: this test's dense fixture (ts ∈ [3001, 3500], span = 499s)
        // would trigger the probe. The test exercises pagination, not infra logic.
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000)
            .with_infra_probe(InfraProbe { threshold_secs: 0 });
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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000);
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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
            .with_clock_for_test(|| 2_000_000_000);
        bulk.fetch_all(&[wallet], &mut cache).await.unwrap();

        assert_eq!(cache.trade_count(), 10);
    }

    // ── Pagination: incremental / forward fill ────────────────────────────────

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
            .with_clock_for_test(|| 2_000_000_000)
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
        r_incr.insert(
            PolymarketEndpoint::UserTradeActivityPage {
                user: wallet.to_string(),
                end: 1000,
                start: Some(1000),
                offset: 0,
            }
            .url(BASE_URL),
            b"[]".to_vec(),
        );
        r_incr.insert(trade_url_start(wallet, 1000), page_json_n(0, 0, 0));
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r_incr))
            .with_clock_for_test(|| 2_000_000_000)
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
        for (second, id) in [
            (3001, 499),
            (2501, 999),
            (2001, 1499),
            (1501, 1999),
            (1001, 2499),
            (501, 2999),
            (1, 3499),
        ] {
            responses.insert(
                PolymarketEndpoint::UserTradeActivityPage {
                    user: wallet.to_string(),
                    start: Some(second),
                    end: second,
                    offset: 0,
                }
                .url(BASE_URL),
                page_json_n(1, id, second),
            );
        }

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
        // Disable probe: this test's dense fixture would trigger it. The test
        // exercises pagination across 7 full pages, not infra logic.
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000)
            .with_infra_probe(InfraProbe { threshold_secs: 0 })
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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000);
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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000);
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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000);

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
        // A complete window accepts ascending API rows without a cursor jump.
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
            .with_clock_for_test(|| 2_000_000_000)
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        let reversed_page = page_json_hashes(&[("0xnew1", 2_000_000), ("0xnew2", 2_000_001)]);
        let mut r2 = HashMap::new();
        r2.insert(trade_url_end(wallet, 1_000_000), page_json_n(0, 0, 0));
        r2.insert(
            PolymarketEndpoint::UserTradeActivityPage {
                user: wallet.to_string(),
                end: 1_000_003,
                start: Some(1_000_003),
                offset: 0,
            }
            .url(BASE_URL),
            b"[]".to_vec(),
        );
        r2.insert(trade_url_start(wallet, 1_000_003), reversed_page);
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
            .with_clock_for_test(|| 2_000_000_000)
            .fetch_all(&[wallet], &mut cache)
            .await
            .unwrap();

        assert_eq!(
            cache.trade_count(),
            5,
            "both rows in the complete window must be appended"
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

        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), HangFetcher)
            .with_clock_for_test(|| 2_000_000_000)
            .with_wallet_timeout(1);

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
            .with_clock_for_test(|| 2_000_000_000)
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

        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher)
            .with_clock_for_test(|| 2_000_000_000)
            .with_wallet_timeout(1);

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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
            .with_clock_for_test(|| 2_000_000_000);
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
            .with_clock_for_test(|| 2_000_000_000)
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
            .with_clock_for_test(|| 2_000_000_000)
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
                .with_clock_for_test(|| 2_000_000_000)
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
        let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
            .with_clock_for_test(|| 2_000_000_000);
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
