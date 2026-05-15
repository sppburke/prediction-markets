//! Weekly funder refresh (`pe-bootstrap weekly`, issue #166).
//!
//! 1. `select_weekly_due` — `is_active = 1` wallets with `last_funder_fetch_at`
//!    NULL or stale (>7 days).
//! 2. Resolve the upper block range dynamically: query Etherscan's
//!    `eth_blockNumber`. Falls back to a sentinel if the call fails — see the
//!    [`FALLBACK_TO_BLOCK`] constant for the rationale.
//! 3. `EtherscanFunderLookup::funders_of_with_timestamps` for each due wallet.
//! 4. `insert_funder_edges` persists the new edges; per-wallet atomic commit.
//! 5. `update_last_funder_fetch_at = now` whether or not funders were found —
//!    a wallet with zero discovered funders is still considered "queried".

use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::{
    BlockRange, EtherscanFunderLookup, contracts::CTF_EXCHANGE_V1_DEPLOY_BLOCK,
};
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::pile;

/// Fallback upper block when Etherscan's `eth_blockNumber` is unavailable.
/// Set well above the historical `FUNDER_DISCOVERY_TO_BLOCK = 80_000_000` so
/// the weekly job is useful even without RPC access. Operators should monitor
/// the warn-level log and either supply an Etherscan API key or update this
/// constant if Polygon advances past it.
pub const FALLBACK_TO_BLOCK: u64 = 200_000_000;

#[derive(Debug, Default, Clone, Copy)]
pub struct WeeklyReport {
    pub due: usize,
    pub processed: usize,
    pub failed: usize,
    pub to_block: u64,
}

pub async fn run_weekly(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<WeeklyReport, BootstrapError> {
    let api_key = config
        .etherscan_api_key
        .clone()
        .ok_or(BootstrapError::Cache {
            message: "weekly requires PE_ETHERSCAN_API_KEY".to_string(),
        })?;

    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let due_hexes = pile::select_weekly_due(cache, now_unix, config.weekly_limit)?;
    let due = due_hexes.len();
    if due_hexes.is_empty() {
        tracing::info!("weekly: no wallets due");
        return Ok(WeeklyReport::default());
    }

    let lookup = EtherscanFunderLookup::new(api_key);
    let to_block = match lookup.current_block().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                error = %e,
                fallback = FALLBACK_TO_BLOCK,
                "weekly: eth_blockNumber failed, falling back"
            );
            FALLBACK_TO_BLOCK
        }
    };
    let block_range = BlockRange {
        from: CTF_EXCHANGE_V1_DEPLOY_BLOCK,
        to: to_block,
    };

    let processed: usize;
    let failed: usize;
    {
        let cache_mutex: Mutex<&mut WalletCache> = Mutex::new(cache);
        let lookup_ref = &lookup;
        let processed_ref = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed_ref = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let block_range_local = block_range;
        let fetched_at = now_unix;

        stream::iter(due_hexes.iter().cloned())
            .for_each_concurrent(config.funder_concurrency, |hex| {
                let cache_mutex = &cache_mutex;
                let processed_ref = Arc::clone(&processed_ref);
                let failed_ref = Arc::clone(&failed_ref);
                async move {
                    let wallet = match WalletAddress::from_hex(&hex) {
                        Ok(w) => w,
                        Err(_) => {
                            failed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }
                    };
                    let wallet_set: HashSet<WalletAddress> =
                        std::iter::once(wallet).collect();
                    let result = lookup_ref
                        .funders_of_with_timestamps(&wallet_set, block_range_local)
                        .await;
                    match result {
                        Ok(funders) => {
                            let funders_vec: Vec<(WalletAddress, i64)> =
                                funders.into_iter().collect();
                            let mut guard = cache_mutex.lock().await;
                            if let Err(e) =
                                guard.insert_funder_edges(wallet, &funders_vec, fetched_at)
                            {
                                tracing::error!(wallet = %wallet, error = %e, "weekly: cache insert failed");
                                failed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                return;
                            }
                            if let Err(e) = guard.update_last_funder_fetch(&hex, fetched_at) {
                                tracing::error!(wallet = %wallet, error = %e, "weekly: timestamp update failed");
                                failed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                return;
                            }
                            processed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            tracing::warn!(wallet = %wallet, error = %e, "weekly: funder fetch failed");
                            failed_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            })
            .await;

        processed = processed_ref.load(std::sync::atomic::Ordering::Relaxed);
        failed = failed_ref.load(std::sync::atomic::Ordering::Relaxed);
    }

    tracing::info!(due, processed, failed, to_block, "weekly: complete");
    Ok(WeeklyReport {
        due,
        processed,
        failed,
        to_block,
    })
}
