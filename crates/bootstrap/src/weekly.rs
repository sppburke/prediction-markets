//! Weekly funder refresh (`pe-bootstrap weekly`, issue #166).
//!
//! 1. `select_weekly_due` — `is_active = 1` wallets with `last_funder_fetch_at`
//!    NULL or stale (>7 days).
//! 2. Resolve the upper block range dynamically: query Etherscan's
//!    `eth_blockNumber`. Falls back to a sentinel if the call fails — see the
//!    [`FALLBACK_TO_BLOCK`] constant for the rationale.
//! 3. `funder::run_funder` — shared inner loop (fetch + insert + timestamp update).
//! 4. Returns `Err(BootstrapError::PartialFetch)` when any wallets fail so the
//!    caller can exit 2 (soft-fail) rather than 0, making the partial visible to
//!    operators and systemd.

use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::{
    BlockRange, EtherscanFunderLookup, contracts::CTF_EXCHANGE_V1_DEPLOY_BLOCK,
};
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::funder;
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
    if due_hexes.is_empty() {
        tracing::info!("weekly: no wallets due");
        return Ok(WeeklyReport::default());
    }

    let lookup = EtherscanFunderLookup::new(api_key.clone());
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

    let due_wallets: Vec<WalletAddress> = due_hexes
        .iter()
        .filter_map(|h| {
            WalletAddress::from_hex(h)
                .map_err(|e| tracing::warn!(address = %h, error = %e, "weekly: skipping unparseable wallet"))
                .ok()
        })
        .collect();

    // `due` reflects only parseable wallets so processed + failed == due.
    let unparseable = due_hexes.len() - due_wallets.len();
    if unparseable > 0 {
        tracing::warn!(
            unparseable,
            total = due_hexes.len(),
            "weekly: skipped unparseable wallets; check DB for corrupted addresses"
        );
    }
    let due = due_wallets.len();
    if due == 0 {
        tracing::info!("weekly: all selected wallets unparseable, nothing to process");
        return Ok(WeeklyReport::default());
    }

    let report = funder::run_funder(
        cache,
        &due_wallets,
        block_range,
        &api_key,
        config.funder_concurrency,
        config.funder_rate_limit_rps,
        true, // weekly stamps last_funder_fetch_at per wallet
    )
    .await?;

    tracing::info!(
        due,
        processed = report.processed,
        failed = report.failed,
        to_block,
        "weekly: complete"
    );

    // Issue #195: return PartialFetch (exit 2) rather than Ok (exit 0) when any
    // wallets failed, matching the exit-code convention of `backfill`.
    if report.failed > 0 {
        return Err(BootstrapError::PartialFetch {
            failed_wallets: report.failed,
        });
    }

    Ok(WeeklyReport {
        due,
        processed: report.processed,
        failed: 0,
        to_block,
    })
}
