//! Daily Polymarket backfill (`pe-bootstrap backfill`, issue #166).
//!
//! 1. `select_backfill_due` — `is_active = 1` wallets with `last_polymarket_fetch_at`
//!    NULL or stale (>1 day). `backfill_limit = 0` returns all due wallets;
//!    a positive limit caps the per-run batch.
//! 2. `PolymarketBulkFetcher::fetch_all` — incremental two-phase cursor walk
//!    with per-wallet `last_polymarket_fetch_at` stamping. Returns a
//!    [`FetchOutcome`] with the failed list AND a `new_trades` map of
//!    wallets with at least one new inserted row.
//! 3. `fetch_resolutions_and_schedules` — multi-source pipeline (Polygon RPC →
//!    CLOB → Gamma) on the full cache market set so newly-discovered
//!    market_ids get their resolution / schedule rows.
//! 4. `refresh_trade_counts` + `apply_activation_rules` — newly-qualifying
//!    wallets flip to `is_active=1`.
//! 5. Return `Err(PartialFetch)` at the very end so `pe-bootstrap` exits
//!    non-zero when any wallet failed, without aborting the pipeline.

use std::collections::HashSet;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_source_polymarket_public::ReqwestFetcher;
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::polymarket::PolymarketBulkFetcher;
use crate::{fetch_resolutions_and_schedules, pile};

#[derive(Debug, Default, Clone, Copy)]
pub struct BackfillReport {
    pub due: usize,
    pub fetched: usize,
    pub failed: usize,
    pub activated: usize,
}

pub async fn run_backfill(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<BackfillReport, BootstrapError> {
    // ── 1. Due set ─────────────────────────────────────────────────────────────
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let due_hexes = pile::select_backfill_due(cache, now_unix, config.backfill_limit)?;
    let due = due_hexes.len();
    let fetch_set: Vec<WalletAddress> = due_hexes
        .iter()
        .filter_map(|h| WalletAddress::from_hex(h).ok())
        .collect();
    tracing::info!(
        due,
        limit = config.backfill_limit,
        "backfill: wallets selected"
    );

    if fetch_set.is_empty() {
        tracing::info!("backfill: no wallets due — nothing to do");
        return Ok(BackfillReport {
            due,
            ..Default::default()
        });
    }

    // ── 2. Fetch trades ────────────────────────────────────────────────────────
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = PolymarketBulkFetcher::new(
        config.polymarket_base_url.clone(),
        ReqwestFetcher::new(client),
    )
    .with_concurrency(config.polymarket_concurrency)
    .with_wallet_timeout(config.polymarket_wallet_timeout_secs)
    .with_stamp_on_success(true);
    let outcome = fetcher.fetch_all(&fetch_set, cache).await?;
    let failed_count = outcome.failed.len();
    let failed_set: HashSet<WalletAddress> = outcome.failed.iter().copied().collect();
    if failed_count > 0 {
        tracing::warn!(
            attempted = outcome.attempted,
            failed = failed_count,
            "backfill: partial fetch — continuing pipeline; failed wallets will be retried on next run"
        );
    }

    // ── 3. Stamp last_polymarket_full_at for wallets that got a full fetch ──────
    let full_fetch_run_at = OffsetDateTime::now_utc().unix_timestamp();
    for wallet in &fetch_set {
        if failed_set.contains(wallet) {
            continue;
        }
        cache.update_last_polymarket_full_at(&wallet.to_string(), full_fetch_run_at)?;
    }

    // ── 4. Resolutions + activation tail ───────────────────────────────────────
    if config.fetch_resolutions {
        let market_ids = cache.all_market_ids();
        // Issue #201: optional resolution stages soft-fail; a partial result is
        // logged but does not change backfill's own exit accounting (a Polygon
        // primary failure still propagates as Err via `?`).
        let report = fetch_resolutions_and_schedules(config, cache, &market_ids).await?;
        if report.has_failures() {
            tracing::warn!(
                stages_failed = ?report.stages_failed,
                "backfill: resolutions partial — optional stages soft-failed"
            );
        }
    }

    cache.refresh_trade_counts()?;
    let activated = pile::apply_activation_rules(cache)?;

    let fetched = fetch_set.len().saturating_sub(failed_count);
    tracing::info!(
        due,
        fetched,
        failed = failed_count,
        activated,
        "backfill: complete"
    );

    // Soft-fail return.
    if failed_count > 0 {
        return Err(BootstrapError::PartialFetch {
            failed_wallets: failed_count,
        });
    }

    Ok(BackfillReport {
        due,
        fetched,
        failed: failed_count,
        activated,
    })
}
