//! Winner-discovery orchestrator: leaderboard + Radion wallet ingest (issue #324).
//!
//! `run_winner_discovery` runs all enabled discovery sources in sequence and
//! returns aggregate counts. [`CacheMutationLock`] is scoped inside each
//! [`crate::wallet_discovery::run_source_discovery`] call and released before
//! this function returns — callers may safely invoke `pe-bootstrap backfill` and
//! `pe-skill-select` afterwards without a lock conflict.

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::wallet_discovery::{WalletDiscoverySource, run_source_discovery};

/// Aggregate counts across all discovery sources.
#[derive(Debug, Default, Clone, Copy)]
pub struct WinnerDiscoveryReport {
    pub leaderboard_unique: usize,
    pub leaderboard_activated: usize,
    pub radion_unique: usize,
}

/// Run all enabled discovery sources in sequence and return aggregate counts.
pub async fn run_winner_discovery(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    let lb = run_source_discovery(WalletDiscoverySource::Leaderboard, config, cache).await?;
    let rd = run_source_discovery(WalletDiscoverySource::Radion, config, cache).await?;

    let report = WinnerDiscoveryReport {
        leaderboard_unique: lb.unique_wallets,
        leaderboard_activated: lb.activated,
        radion_unique: rd.unique_wallets,
    };
    tracing::info!(
        leaderboard_unique = report.leaderboard_unique,
        leaderboard_activated = report.leaderboard_activated,
        radion_unique = report.radion_unique,
        "winner_discovery: complete"
    );
    Ok(report)
}
