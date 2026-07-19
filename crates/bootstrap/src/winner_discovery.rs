//! Winner-discovery orchestrator: leaderboard + datadash wallet ingest
//! (issues #324, #365).
//!
//! `run_winner_discovery` runs all enabled discovery sources in sequence and
//! returns aggregate counts. [`CacheMutationLock`] is scoped inside each
//! [`crate::wallet_discovery::run_source_discovery`] call and released before
//! this function returns — callers may safely invoke `pe-bootstrap backfill`
//! afterwards without a lock conflict.
//!
//! Failure policy: the leaderboard source propagates errors (fatal to
//! `pe-bootstrap all`). The **datadash** source **soft-fails** — a
//! [`BootstrapError::Datadash`] is logged and treated as zero counts so a
//! third-party outage never breaks the run (issue #365 AC9). Non-datadash errors
//! (e.g. a broken cache) still propagate. (The Radion source was retired once its
//! upstream `traders/analysis` endpoint was removed.)
//!
//! [`CacheMutationLock`]: crate::lock::CacheMutationLock

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::pile::ActivationPolicy;
use crate::wallet_discovery::{
    SourceDiscoveryResult, WalletDiscoverySource, run_source_discovery_with_policy,
};

/// Aggregate counts across all discovery sources.
#[derive(Debug, Default, Clone, Copy)]
pub struct WinnerDiscoveryReport {
    pub leaderboard_unique: usize,
    pub leaderboard_activated: usize,
    pub datadash_unique: usize,
    pub datadash_activated: usize,
}

/// Run all enabled discovery sources in sequence and return aggregate counts.
pub async fn run_winner_discovery(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    run_winner_discovery_with_policy(config, cache, ActivationPolicy::Immediate).await
}

/// Winner discovery with an explicit activation policy. The full rank-and-push
/// wrapper defers both sources to one controlled batch.
pub async fn run_winner_discovery_with_policy(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    activation_policy: ActivationPolicy,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    let lb = run_source_discovery_with_policy(
        WalletDiscoverySource::Leaderboard,
        config,
        cache,
        activation_policy,
    )
    .await?;
    // Datadash soft-fails: a third-party outage must not break `pe-bootstrap all`.
    // Only `BootstrapError::Datadash` is swallowed; any other variant (e.g. a
    // shared-cache failure) still propagates as fatal.
    let dd = match run_source_discovery_with_policy(
        WalletDiscoverySource::Datadash,
        config,
        cache,
        activation_policy,
    )
    .await
    {
        Ok(r) => r,
        Err(BootstrapError::Datadash { message }) => {
            tracing::warn!(
                error = %message,
                "winner_discovery: datadash source failed — continuing with zero counts"
            );
            SourceDiscoveryResult::default()
        }
        Err(e) => return Err(e),
    };

    let report = WinnerDiscoveryReport {
        leaderboard_unique: lb.unique_wallets,
        leaderboard_activated: lb.activated,
        datadash_unique: dd.unique_wallets,
        datadash_activated: dd.activated,
    };
    tracing::info!(
        leaderboard_unique = report.leaderboard_unique,
        leaderboard_activated = report.leaderboard_activated,
        datadash_unique = report.datadash_unique,
        datadash_activated = report.datadash_activated,
        "winner_discovery: complete"
    );
    Ok(report)
}
