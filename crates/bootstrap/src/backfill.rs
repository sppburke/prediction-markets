//! Daily Polymarket backfill (`pe-bootstrap backfill`, issues #166 + #176).
//!
//! 1. `select_backfill_due` — `is_active = 1` wallets with `last_polymarket_fetch_at`
//!    NULL or stale (>1 day). `backfill_limit = 0` returns all due wallets;
//!    a positive limit caps the per-run batch.
//! 2. **Delta-backfill (issue #176)**: when `polymarket_delta_mode != Off`
//!    AND `polygon_rpc_url` is set, runs an on-chain `eth_getLogs` scan of
//!    `OrderFilled` events to derive the set of wallets that traded on-chain.
//!    - `Shadow` (default): legacy fetch set unchanged; the delta scan
//!      result is used only to populate the `delta_audit` table.
//!    - `Delta`: fetch set = `(due ∩ delta_set) ∪ paranoia_set` where
//!      `paranoia_set` is wallets with `last_polymarket_full_at` older than
//!      `polymarket_full_fetch_staleness_secs` (canonical: 7 days).
//! 3. `PolymarketBulkFetcher::fetch_all` — incremental two-phase cursor walk
//!    with per-wallet `last_polymarket_fetch_at` stamping. Returns a
//!    [`FetchOutcome`] with the failed list AND a `new_trades` map of
//!    wallets with at least one new inserted row.
//! 4. `delta_audit::classify_and_record` (shadow only) — writes one row per
//!    wallet in `(new_trades ∪ delta_set)` to the `delta_audit` table.
//! 5. Cursor advance: writes `POLYGON_CTF_BACKFILL_CURSOR_KEY` to the
//!    scanner's returned `new_cursor` value if and only if the scan
//!    succeeded (pattern match on `Option<u64>`).
//! 6. `fetch_resolutions_and_schedules` — multi-source pipeline (Polygon RPC →
//!    Dune → CLOB → Gamma) on the full cache market set so newly-discovered
//!    market_ids get their resolution / schedule rows.
//! 7. `refresh_trade_counts` + `apply_activation_rules` — newly-qualifying
//!    wallets flip to `is_active=1`.
//! 8. Return `Err(PartialFetch)` at the very end so `pe-bootstrap` exits
//!    non-zero when any wallet failed, without aborting the pipeline.

use std::collections::HashSet;
use std::time::Duration;

use alloy::providers::ProviderBuilder;
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::AlloyChainLogFetcher;
use pe_source_onchain_polygon::ChainLogFetcher;
use pe_source_polymarket_public::ReqwestFetcher;
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::polygon_ctf_delta::{POLYGON_CTF_BACKFILL_CURSOR_KEY, scan_active_wallets};
use crate::polymarket::PolymarketBulkFetcher;
use crate::{DeltaMode, delta_audit, fetch_resolutions_and_schedules, pile};

/// Floor below which `eth_get_logs_bisect` propagates instead of bisecting
/// further. One block is the strict floor.
const DELTA_SCAN_MIN_CHUNK: u64 = 1;

#[derive(Debug, Default, Clone, Copy)]
pub struct BackfillReport {
    pub due: usize,
    pub fetched: usize,
    pub failed: usize,
    pub activated: usize,
    /// Issue #176: count of wallets the on-chain scan flagged as active.
    /// `0` when delta mode is off or the scan failed.
    pub delta_active_wallets: usize,
    /// Issue #176: count of rows written to `delta_audit` this run.
    /// `0` when shadow mode is off or the scan failed.
    pub audit_rows_written: usize,
}

pub async fn run_backfill(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<BackfillReport, BootstrapError> {
    // ── 1. Legacy due set ─────────────────────────────────────────────────────
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let due_hexes = pile::select_backfill_due(cache, now_unix, config.backfill_limit)?;
    let due = due_hexes.len();
    let full_due_set: HashSet<WalletAddress> = due_hexes
        .iter()
        .filter_map(|h| WalletAddress::from_hex(h).ok())
        .collect();
    tracing::info!(
        due,
        delta_mode = ?config.polymarket_delta_mode,
        limit = config.backfill_limit,
        "backfill: wallets selected"
    );

    // ── 2. Optional on-chain delta scan ───────────────────────────────────────
    let (delta_set, new_cursor): (HashSet<WalletAddress>, Option<u64>) =
        if config.polymarket_delta_mode != DeltaMode::Off
            && let Some(rpc_url) = config.polygon_rpc_url.as_deref()
        {
            run_delta_scan(rpc_url, config.polygon_ctf_confirmations, cache).await
        } else {
            (HashSet::new(), None)
        };
    let delta_active_wallets = delta_set.len();

    // ── 3. Paranoia full-fetch set (issue #176 backstop) ──────────────────────
    let paranoia_set: HashSet<WalletAddress> = if config.polymarket_delta_mode == DeltaMode::Delta {
        pile::select_full_fetch_due(cache, now_unix, config.polymarket_full_fetch_staleness_secs)?
            .into_iter()
            .filter_map(|h| WalletAddress::from_hex(&h).ok())
            .collect()
    } else {
        HashSet::new()
    };

    // ── 4. Compute fetch_set per delta mode ───────────────────────────────────
    let fetch_set: Vec<WalletAddress> = match config.polymarket_delta_mode {
        DeltaMode::Off | DeltaMode::Shadow => {
            // Legacy fetch behaviour: every due wallet.
            full_due_set.iter().copied().collect()
        }
        DeltaMode::Delta => {
            // Delta-narrowed: intersection of due + delta + paranoia backstop.
            let intersected: HashSet<WalletAddress> =
                full_due_set.intersection(&delta_set).copied().collect();
            intersected.union(&paranoia_set).copied().collect()
        }
    };

    if fetch_set.is_empty() {
        tracing::info!("backfill: fetch_set empty after delta narrowing — nothing to do");
        // Still advance cursor on a successful (empty) scan so the next run
        // resumes after this range.
        if let Some(target) = new_cursor {
            cache.set_source_cursor(POLYGON_CTF_BACKFILL_CURSOR_KEY, &target.to_string())?;
        }
        return Ok(BackfillReport {
            due,
            delta_active_wallets,
            ..Default::default()
        });
    }

    // ── 5. Fetch trades ───────────────────────────────────────────────────────
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

    // ── 6. Stamp last_polymarket_full_at for wallets that got a full fetch ────
    let full_fetch_run_at = OffsetDateTime::now_utc().unix_timestamp();
    let stamp_full_set: HashSet<WalletAddress> = match config.polymarket_delta_mode {
        DeltaMode::Off | DeltaMode::Shadow => {
            // Every wallet in the fetch_set got a full fetch (legacy behaviour).
            fetch_set.iter().copied().collect()
        }
        DeltaMode::Delta => {
            // Only wallets that came from the paranoia set got a full fetch
            // (the rest got a delta-narrowed fetch; their last_polymarket_full_at
            // does not advance).
            paranoia_set.clone()
        }
    };
    for wallet in &stamp_full_set {
        if failed_set.contains(wallet) {
            continue;
        }
        cache.update_last_polymarket_full_at(&wallet.to_string(), full_fetch_run_at)?;
    }

    // ── 7. Shadow-mode audit write ────────────────────────────────────────────
    let audit_rows_written =
        if config.polymarket_delta_mode == DeltaMode::Shadow && new_cursor.is_some() {
            delta_audit::classify_and_record(cache, now_unix, &delta_set, &outcome.new_trades)?
        } else {
            0
        };

    // ── 8. Cursor advance ────────────────────────────────────────────────────
    // Only advance when the scan succeeded (new_cursor.is_some()). Pattern
    // match encodes the invariant.
    if let Some(target) = new_cursor {
        cache.set_source_cursor(POLYGON_CTF_BACKFILL_CURSOR_KEY, &target.to_string())?;
    }

    // ── 9. Resolutions + activation tail (unchanged) ─────────────────────────
    if config.fetch_resolutions {
        let market_ids = cache.all_market_ids();
        fetch_resolutions_and_schedules(config, cache, &market_ids).await?;
    }

    cache.refresh_trade_counts()?;
    let activated = pile::apply_activation_rules(cache)?;

    let fetched = fetch_set.len().saturating_sub(failed_count);
    tracing::info!(
        due,
        fetched,
        failed = failed_count,
        activated,
        delta_active_wallets,
        audit_rows_written,
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
        delta_active_wallets,
        audit_rows_written,
    })
}

/// Runs the Polygon CTF delta scan against the given RPC URL. On any failure
/// (RPC down, get_logs error, etc.) returns `(empty_set, None)` and logs a
/// warning; the caller falls back to legacy full-fetch.
async fn run_delta_scan(
    rpc_url: &str,
    confirmations: u64,
    cache: &WalletCache,
) -> (HashSet<WalletAddress>, Option<u64>) {
    let http_url: reqwest::Url = match rpc_url.parse() {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(
                rpc_url,
                error = %e,
                "backfill: invalid polygon_rpc_url; falling back to legacy full fetch"
            );
            return (HashSet::new(), None);
        }
    };
    let provider = ProviderBuilder::new().connect_http(http_url);
    let fetcher = AlloyChainLogFetcher {
        provider,
        min_chunk: DELTA_SCAN_MIN_CHUNK,
    };
    run_delta_scan_with_fetcher(&fetcher, confirmations, cache).await
}

/// Trait-generic delta-scan invocation. Production wraps an alloy provider
/// via `run_delta_scan`; tests (scenario) substitute an in-memory fetcher.
pub async fn run_delta_scan_with_fetcher<F: ChainLogFetcher>(
    fetcher: &F,
    confirmations: u64,
    cache: &WalletCache,
) -> (HashSet<WalletAddress>, Option<u64>) {
    let result = scan_active_wallets(fetcher, None, None, confirmations, cache).await;
    if let Some(err) = &result.scan_error {
        tracing::warn!(
            error = %err,
            "backfill: delta scan failed; falling back to legacy full fetch"
        );
    }
    (result.active_wallets, result.new_cursor)
}
