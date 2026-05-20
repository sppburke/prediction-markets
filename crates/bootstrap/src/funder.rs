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
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::stream::{self, StreamExt};
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::{
    BlockRange, ChainLogFetcher, EtherscanFunderLookup, funder_edges_with_timestamps,
};
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
    rate_limit_rps: u32,
    update_fetch_timestamps: bool,
) -> Result<FunderReport, BootstrapError> {
    let total_pending = pending.len();
    // Issue #201: `0` (or an out-of-range value) falls back to the 3 req/s
    // free-tier default; a paid Etherscan tier can raise this via config.
    let rps = NonZeroU32::new(rate_limit_rps).unwrap_or(NonZeroU32::MIN.saturating_add(2));
    tracing::info!(
        pending = total_pending,
        rate_limit_rps = rps.get(),
        update_timestamps = update_fetch_timestamps,
        "funder: discovery starting"
    );

    let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
    let lookup = EtherscanFunderLookup::new(api_key.to_owned()).with_rate_limit_rps(rps);

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

/// Bulk funder-graph discovery via batched `eth_getLogs` (issue #203).
///
/// The throughput counterpart to [`run_funder`]: instead of one rate-limited
/// Etherscan call per wallet (~3 req/s ⇒ ~15h for the active backlog), it runs
/// a single batched [`funder_edges_with_timestamps`] scan against an
/// Alchemy-class RPC (any [`ChainLogFetcher`]) and inserts the resulting
/// per-wallet edges. Used by the bulk paths (`pe-bootstrap funder` and the
/// `all` pipeline); the incremental `weekly` refresh stays on [`run_funder`]
/// because Etherscan's address-indexed lookup is cheaper for small staleness
/// sets than a full-history block scan.
///
/// A scan failure is fatal (`Err`): no wallet is marked done, so the run
/// retries cleanly on the next invocation. On success every wallet in `pending`
/// is written to `funder_lookup_done` — **including wallets with zero
/// discovered funders** — so they are not re-queried indefinitely.
///
/// # Precondition
/// `pending` must not be empty (callers skip the call when the list is empty).
pub async fn run_funder_eth_logs<F: ChainLogFetcher>(
    cache: &mut WalletCache,
    pending: &[WalletAddress],
    block_range: BlockRange,
    fetcher: &F,
    chunk_blocks: u64,
    topic_batch_size: usize,
) -> Result<FunderReport, BootstrapError> {
    let total_pending = pending.len();
    tracing::info!(
        pending = total_pending,
        chunk_blocks,
        topic_batch_size,
        "funder(eth_logs): discovery starting"
    );

    let funded: HashSet<WalletAddress> = pending.iter().copied().collect();
    let edges = funder_edges_with_timestamps(
        fetcher,
        &funded,
        block_range,
        chunk_blocks,
        topic_batch_size,
    )
    .await
    .map_err(|e| BootstrapError::Funder {
        message: e.to_string(),
    })?;

    let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
    let mut processed = 0usize;
    let mut failed = 0usize;
    // Iterate ALL pending wallets, not just those with discovered funders, so a
    // zero-funder wallet is still marked done in `funder_lookup_done` and not
    // re-queried (parity with `run_funder`'s per-wallet insert).
    for wallet in pending {
        let funders_vec: &[(WalletAddress, i64)] = edges.get(wallet).map_or(&[], Vec::as_slice);
        match cache.insert_funder_edges(*wallet, funders_vec, fetched_at) {
            Ok(()) => processed += 1,
            Err(e) => {
                tracing::error!(wallet = %wallet, error = %e, "funder(eth_logs): cache insert failed");
                failed += 1;
            }
        }
    }

    let edges_total = cache.load_funder_edges()?.len();
    tracing::info!(
        pending = total_pending,
        processed,
        failed,
        edges_total,
        "funder(eth_logs): discovery complete"
    );
    Ok(FunderReport {
        pending: total_pending,
        processed,
        failed,
        edges_total,
    })
}
