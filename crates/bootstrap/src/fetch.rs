//! Polymarket trade-fetch phase — `pe-bootstrap fetch`.
//!
//! Fetches the per-wallet trade history from the Polymarket Data API and persists
//! it to the SQLite cache. Incremental: only trades newer than the newest cached
//! trade ID are fetched on subsequent runs.

use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_source_polymarket_public::ReqwestFetcher;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::polymarket::PolymarketBulkFetcher;

/// Result of the Polymarket trade-fetch phase.
#[derive(Debug, Default, Clone, Copy)]
pub struct FetchReport {
    /// Total wallets submitted to the fetcher.
    pub attempted: usize,
    /// Wallets that could not be fetched or cached; will be retried on next run.
    pub failed: usize,
}

/// Build a `PolymarketBulkFetcher` from `config`.
///
/// Extracted so both `run_fetch` and `backfill::run_backfill` use the same
/// construction logic.
///
/// # Precondition
/// Caller must ensure `config.polymarket_base_url` is a valid URL.
pub fn build_fetcher(
    config: &BootstrapConfig,
) -> Result<PolymarketBulkFetcher<ReqwestFetcher>, BootstrapError> {
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    Ok(PolymarketBulkFetcher::new(
        config.polymarket_base_url.clone(),
        ReqwestFetcher::new(client),
    )
    .with_concurrency(config.polymarket_concurrency)
    .with_wallet_timeout(config.polymarket_wallet_timeout_secs))
}

/// Fetch trade history for `wallets` and persist to `cache`.
///
/// Returns `Err(BootstrapError::PartialFetch)` when any wallet fails — callers
/// apply soft-fail or hard-fail policy based on context (`--strict` flag).
///
/// Honours `config.skip_trade_fetch`: when `true`, logs a warning and returns
/// immediately with a zero-count report (no-op).
pub async fn run_fetch(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    wallets: &[WalletAddress],
) -> Result<FetchReport, BootstrapError> {
    if config.skip_trade_fetch {
        tracing::warn!(
            wallets = wallets.len(),
            "fetch: PE_BOOTSTRAP_SKIP_TRADE_FETCH=1 — skipping Polymarket fetch; \
             cache may not reflect trades after the last full run"
        );
        return Ok(FetchReport::default());
    }

    let fetcher = build_fetcher(config)?;
    let outcome = fetcher.fetch_all(wallets, cache).await?;
    tracing::info!(
        attempted = outcome.attempted,
        failed = outcome.failed.len(),
        "fetch: trade fetch complete"
    );

    if !outcome.failed.is_empty() {
        return Err(BootstrapError::PartialFetch {
            failed_wallets: outcome.failed.len(),
        });
    }

    Ok(FetchReport {
        attempted: outcome.attempted,
        failed: 0,
    })
}
