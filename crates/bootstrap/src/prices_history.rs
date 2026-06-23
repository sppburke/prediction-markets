//! `pe-bootstrap prices-history` — the CLV/Gamma data backfill for the ranker bake-off (issue #421
//! PR4). A standalone, resumable, additive-only backfill; **not** a stage of the daily
//! `rank_and_push.sh` refresh (that pipeline is discover → backfill → events → resolutions → purge).
//!
//! Two passes over the resolved-market universe, both idempotent:
//!
//! 1. **Gamma `createdAt` → `market_schedules.start_date_unix`** (the `entry_timing_vs_creation`
//!    feature). UPDATE-only via [`GammaFetcher::backfill_start_dates`]; markets already populated or
//!    absent are skipped.
//! 2. **CLOB `/prices-history` → `market_price_history`** (the true-CLV series). For each resolved
//!    market's mapped CLOB tokens with no rows yet, fetch the hourly series over the pre-resolution
//!    window `[close_ref − window, close_ref]` and write it. Fetches run concurrently (to overlap
//!    network RTT under the dedicated ~100 req/s gate); writes are serial, batched transactions.
//!
//! The heavy second pass is bounded by `prices_history_token_limit` (0 = unbounded) and is resumable
//! — a re-run skips `(market, token)` pairs that already have rows.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use pe_source_polymarket_public::{ClobPricesHistoryClient, GAMMA_BROWSER_UA, ReqwestFetcher};

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::gamma::{self, GammaFetcher};

/// Outcome of [`run_prices_history`].
#[derive(Debug, Default, Clone, Copy)]
pub struct PricesHistoryReport {
    /// `market_schedules.start_date_unix` rows populated from Gamma `createdAt` (pass 1).
    pub start_dates_updated: usize,
    /// `(market, token)` series fetched OK in pass 2 (includes those that returned 0 points).
    pub tokens_fetched: usize,
    /// `(market, token)` series whose fetch hit a non-fatal error and were skipped (retry next run).
    pub tokens_failed: usize,
    /// Price points inserted into `market_price_history` (pre-`INSERT OR IGNORE` count).
    pub points_written: usize,
}

/// Chunk size for the Gamma createdAt pass — bounds the in-memory result map per `fetch_markets`
/// call on the ~1.4M-market universe.
const START_DATE_CHUNK: usize = 20_000;

/// Flush threshold for the price-history write batch — one transaction per this many points.
const PRICE_FLUSH_BATCH: usize = 10_000;

/// Run both backfill passes. See the module docs for semantics.
pub async fn run_prices_history(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<PricesHistoryReport, BootstrapError> {
    let mut report = PricesHistoryReport::default();

    // ── Pass 1: Gamma createdAt → start_date_unix ────────────────────────────────
    // Scope to decided-outcome markets (same filter as pass 2's price-series targets): a voided
    // market yields no qualifying first-buy positions, so fetching its createdAt would be wasted.
    let missing_start = cache.market_ids_missing_start_date();
    let resolved = cache.resolved_market_ids_with_winner();
    let start_targets: Vec<String> = missing_start.intersection(&resolved).cloned().collect();
    if start_targets.is_empty() {
        tracing::info!("prices-history: no markets missing start_date — skipping createdAt pass");
    } else {
        let gamma_client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(15))
            .user_agent(GAMMA_BROWSER_UA)
            .build()
            .map_err(|_| BootstrapError::Internal)?;
        let gamma_fetcher = GammaFetcher::new(
            config.gamma_base_url.clone(),
            ReqwestFetcher::new(gamma_client).with_min_interval_ms(gamma::GAMMA_MIN_INTERVAL_MS),
        );
        tracing::info!(
            candidates = start_targets.len(),
            "prices-history: createdAt → start_date backfill starting"
        );
        for chunk in start_targets.chunks(START_DATE_CHUNK) {
            report.start_dates_updated += gamma_fetcher.backfill_start_dates(chunk, cache).await?;
        }
        tracing::info!(
            updated = report.start_dates_updated,
            "prices-history: createdAt → start_date backfill complete"
        );
    }

    // ── Pass 2: CLOB /prices-history → market_price_history ───────────────────────
    let targets = cache.price_history_backfill_targets(config.prices_history_token_limit)?;
    if targets.is_empty() {
        tracing::info!("prices-history: no (market, token) targets need a price series");
        return Ok(report);
    }

    let clob_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let client = Arc::new(
        ClobPricesHistoryClient::new(
            config.clob_base_url.clone(),
            ReqwestFetcher::new(clob_client)
                .with_min_interval_ms(config.prices_history_min_interval_ms),
        )
        .with_fidelity_minutes(config.prices_history_fidelity_minutes),
    );
    let window = config.prices_history_window_secs;
    let concurrency = config.clob_concurrency.max(1);

    tracing::info!(
        targets = targets.len(),
        concurrency,
        window_secs = window,
        fidelity_minutes = config.prices_history_fidelity_minutes,
        "prices-history: CLOB price-series backfill starting"
    );

    // Concurrent fetch (overlaps RTT under the shared gate), serial batched writes.
    let mut stream = stream::iter(targets)
        .map(|target| {
            let client = Arc::clone(&client);
            async move {
                let start_ts = target.close_ref_unix.saturating_sub(window);
                let res = client
                    .fetch_prices_history(&target.token_id, start_ts, target.close_ref_unix)
                    .await;
                (target, res)
            }
        })
        .buffer_unordered(concurrency);

    let mut batch: Vec<(String, String, i64, String)> = Vec::with_capacity(PRICE_FLUSH_BATCH);
    while let Some((target, res)) = stream.next().await {
        match res {
            Ok(points) => {
                for p in points {
                    batch.push((
                        target.market_id.clone(),
                        target.token_id.clone(),
                        p.t,
                        p.price.to_string(),
                    ));
                }
                report.tokens_fetched += 1;
            }
            Err(e) => {
                tracing::warn!(
                    token_id = %target.token_id,
                    market_id = %target.market_id,
                    error = %e,
                    "prices-history: token fetch failed — skipping (retry next run)"
                );
                report.tokens_failed += 1;
            }
        }
        if batch.len() >= PRICE_FLUSH_BATCH {
            report.points_written += batch.len();
            cache.insert_price_history_batch(&batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        report.points_written += batch.len();
        cache.insert_price_history_batch(&batch)?;
    }

    tracing::info!(
        tokens_fetched = report.tokens_fetched,
        tokens_failed = report.tokens_failed,
        points_written = report.points_written,
        "prices-history: CLOB price-series backfill complete"
    );
    Ok(report)
}
