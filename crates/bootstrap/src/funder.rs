//! Funder-graph discovery phase — `pe-bootstrap funder`.
//!
//! Looks up the on-chain funder (depositor) for each wallet via Etherscan's
//! `funders_of_with_timestamps` API, persists edges to `funder_edges`, and
//! optionally stamps `last_funder_fetch_at` per wallet (used by the weekly path).
//!
//! Shared between the initial-fetch path (`enumerate` → `fetch` → `funder`) and
//! the `weekly` steady-state refresh. The callers differ only in how they compute
//! `pending` and `block_range`, and whether they need per-wallet timestamp updates.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::stream::{self, StreamExt};
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::{BlockRange, EtherscanFunderLookup};
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::cache::WalletCache;
use crate::error::BootstrapError;

/// Result of one funder-discovery run.
#[derive(Debug, Default, Clone, Copy)]
pub struct FunderReport {
    /// Wallets submitted for funder lookup.
    pub pending: usize,
    /// Wallets successfully processed (edges inserted, timestamp updated if requested).
    pub processed: usize,
    /// Wallets that failed; partial state is durable — retry on next run.
    pub failed: usize,
    /// Total edges in `funder_edges` after this run.
    pub edges_total: usize,
}

/// Resolve funder edges for `pending` wallets and persist them to `cache`.
///
/// When `update_fetch_timestamps` is `true`, stamps `last_funder_fetch_at` per wallet
/// immediately after a successful `insert_funder_edges` call (required by the `weekly`
/// path; the initial-fetch path leaves timestamps NULL so the weekly job picks them up).
///
/// Returns `Ok(FunderReport)` in all cases, including partial failure — callers
/// decide on soft-fail vs hard-fail policy. Only returns `Err` for fatal errors
/// (cache open failure, etc.).
///
/// # Precondition
/// `pending` must not be empty (callers should skip the call when the list is empty).
pub async fn run_funder(
    cache: &mut WalletCache,
    pending: &[WalletAddress],
    block_range: BlockRange,
    api_key: &str,
    concurrency: usize,
    update_fetch_timestamps: bool,
) -> Result<FunderReport, BootstrapError> {
    let total_pending = pending.len();
    tracing::info!(
        pending = total_pending,
        update_timestamps = update_fetch_timestamps,
        "funder: discovery starting"
    );

    let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
    let lookup = EtherscanFunderLookup::new(api_key.to_owned());

    let (processed, failed) = {
        let cache_mutex: Mutex<&mut WalletCache> = Mutex::new(cache);
        let failed_ctr = Arc::new(AtomicUsize::new(0));
        let processed_ctr = Arc::new(AtomicUsize::new(0));
        let progress_ctr = Arc::new(AtomicUsize::new(0));

        stream::iter(pending.iter().copied())
            .for_each_concurrent(concurrency, |wallet| {
                let cache_mutex = &cache_mutex;
                let lookup_ref = &lookup;
                let failed = Arc::clone(&failed_ctr);
                let processed = Arc::clone(&processed_ctr);
                let progress = Arc::clone(&progress_ctr);
                async move {
                    let wallet_set: HashSet<WalletAddress> = std::iter::once(wallet).collect();
                    match lookup_ref
                        .funders_of_with_timestamps(&wallet_set, block_range)
                        .await
                    {
                        Ok(funders) => {
                            let funders_vec: Vec<(WalletAddress, i64)> =
                                funders.into_iter().collect();
                            let mut guard = cache_mutex.lock().await;
                            if let Err(e) =
                                guard.insert_funder_edges(wallet, &funders_vec, fetched_at)
                            {
                                tracing::error!(
                                    wallet = %wallet,
                                    error = %e,
                                    "funder: cache insert failed"
                                );
                                failed.fetch_add(1, Ordering::Relaxed);
                            } else if update_fetch_timestamps {
                                if let Err(e) =
                                    guard.update_last_funder_fetch(&wallet.to_string(), fetched_at)
                                {
                                    tracing::error!(
                                        wallet = %wallet,
                                        error = %e,
                                        "funder: timestamp update failed"
                                    );
                                    failed.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    processed.fetch_add(1, Ordering::Relaxed);
                                }
                            } else {
                                processed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                wallet = %wallet,
                                error = %e,
                                "funder: fetch failed"
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    let n = progress.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_multiple_of(500) || n == total_pending {
                        tracing::info!(
                            progress = n,
                            total = total_pending,
                            "funder: discovery progress"
                        );
                    }
                }
            })
            .await;
        // cache_mutex drops here, releasing &mut WalletCache before edges query.
        (
            processed_ctr.load(Ordering::Relaxed),
            failed_ctr.load(Ordering::Relaxed),
        )
    };

    let edges_total = cache.load_funder_edges()?.len();

    tracing::info!(
        pending = total_pending,
        processed,
        failed,
        edges_total,
        "funder: discovery complete"
    );

    Ok(FunderReport {
        pending: total_pending,
        processed,
        failed,
        edges_total,
    })
}
