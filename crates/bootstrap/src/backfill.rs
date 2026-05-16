//! Daily Polymarket backfill (`pe-bootstrap backfill`, issue #166).
//!
//! 1. `select_backfill_due` — `is_active = 1` wallets with `last_polymarket_fetch_at`
//!    NULL or stale (>1 day). `backfill_limit = 0` returns all due wallets;
//!    a positive limit caps the per-run batch.
//! 2. `PolymarketBulkFetcher::fetch_all` — incremental two-phase cursor walk
//!    that appends only new trades for each wallet. Returns a [`FetchOutcome`]
//!    with the failed-wallet list; per-wallet errors are **soft** — the rest
//!    of the pipeline runs regardless. With `with_stamp_on_success(true)`,
//!    `last_polymarket_fetch_at` is written inline per-wallet under the same
//!    `cache_mutex` guard that committed the trades, so SIGINT mid-run
//!    preserves all completed work.
//! 3. `fetch_resolutions_and_schedules` — multi-source pipeline (Polygon RPC →
//!    Dune → CLOB → Gamma) on the full cache market set so newly-discovered
//!    market_ids get their resolution / schedule rows.
//! 4. `refresh_trade_counts` — recompute `wallets.trade_count` from the trades
//!    table.
//! 5. `apply_activation_rules` — newly-qualifying wallets flip to `is_active=1`.
//! 6. Return `Err(PartialFetch)` at the very end so `pe-bootstrap` exits non-zero
//!    when any wallet failed, without aborting the pipeline.

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
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let due_hexes = pile::select_backfill_due(cache, now_unix, config.backfill_limit)?;
    let due = due_hexes.len();
    tracing::info!(
        due,
        limit = config.backfill_limit,
        "backfill: wallets selected"
    );
    if due_hexes.is_empty() {
        return Ok(BackfillReport::default());
    }

    let wallets: Vec<WalletAddress> = due_hexes
        .iter()
        .filter_map(|h| WalletAddress::from_hex(h).ok())
        .collect();

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
    // Per-wallet stamping: each succeeded wallet's `last_polymarket_fetch_at`
    // is written inline under the same `cache_mutex` guard that commits its
    // trades. SIGINT mid-run preserves all completed work — failed and
    // in-flight wallets stay NULL and are re-queued by the next backfill.
    .with_stamp_on_success(true);
    let outcome = fetcher.fetch_all(&wallets, cache).await?;
    let failed_count = outcome.failed.len();
    if failed_count > 0 {
        tracing::warn!(
            attempted = outcome.attempted,
            failed = failed_count,
            "backfill: partial fetch — continuing pipeline; failed wallets will be retried on next run"
        );
    }

    // Resolutions + schedules for any newly-seen market_ids. Existing per-source
    // cursors (`polygon_ctf_last_block`, `clob_closed`) keep this incremental.
    if config.fetch_resolutions {
        let market_ids = cache.all_market_ids();
        fetch_resolutions_and_schedules(config, cache, &market_ids).await?;
    }

    // Refresh trade counts and run activation — Dune-discovered wallets that
    // just gained ≥ pile_activation_min_trades trades flip to active here.
    cache.refresh_trade_counts()?;
    let activated = pile::apply_activation_rules(cache)?;

    let fetched = wallets.len().saturating_sub(failed_count);
    tracing::info!(
        due,
        fetched,
        failed = failed_count,
        activated,
        "backfill: complete"
    );

    // Soft-fail return: pipeline ran, successful wallets stamped, but exit code
    // signals partial failure so operators / systemd notice.
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
