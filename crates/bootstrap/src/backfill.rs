//! Daily Polymarket backfill (`pe-bootstrap backfill`, issue #166).
//!
//! 1. `select_backfill_due` — `is_active = 1` wallets with `last_polymarket_fetch_at`
//!    NULL or stale (>1 day). `backfill_limit = 0` returns all due wallets;
//!    a positive limit caps the per-run batch.
//! 2. `PolymarketBulkFetcher::fetch_all` — incremental two-phase cursor walk
//!    that appends only new trades for each wallet.
//! 3. `fetch_resolutions_and_schedules` — multi-source pipeline (Polygon RPC →
//!    Dune → CLOB → Gamma) on the full cache market set so newly-discovered
//!    market_ids get their resolution / schedule rows.
//! 4. `refresh_trade_counts` — recompute `wallets.trade_count` from the trades
//!    table.
//! 5. `apply_activation_rules` — newly-qualifying wallets flip to `is_active=1`.
//! 6. Update `last_polymarket_fetch_at = now` for every processed wallet.

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
    .with_concurrency(config.polymarket_concurrency);
    fetcher.fetch_all(&wallets, cache).await?;

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

    // Stamp last_polymarket_fetch_at = now for every wallet we attempted, even
    // ones that errored — they're stale enough that re-tries in the next run
    // would re-burn the same Polymarket request budget.
    let stamp_now = OffsetDateTime::now_utc().unix_timestamp();
    for hex in &due_hexes {
        cache.update_last_polymarket_fetch(hex, stamp_now)?;
    }

    tracing::info!(due, activated, "backfill: complete");
    Ok(BackfillReport {
        due,
        fetched: wallets.len(),
        activated,
    })
}
