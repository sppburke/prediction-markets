//! Winner-discovery orchestrator: leaderboard + Radion + datadash wallet ingest
//! (issues #324, #365, #373).
//!
//! `run_winner_discovery` runs all enabled discovery sources in sequence and
//! returns aggregate counts. [`CacheMutationLock`] is scoped inside each
//! [`crate::wallet_discovery::run_source_discovery`] call and released before
//! this function returns — callers may safely invoke `pe-bootstrap backfill` and
//! `pe-skill-select` afterwards without a lock conflict.
//!
//! Failure policy: the leaderboard source propagates errors (fatal to
//! `pe-bootstrap all`). The **Radion and datadash** sources **soft-fail** — a
//! [`BootstrapError::Radion`] / [`BootstrapError::Datadash`] is logged and treated
//! as zero counts so a third-party outage or quota wall never breaks the run
//! (issues #373 / #365 AC9). Non-Radion/datadash errors (e.g. a broken cache)
//! still propagate.
//!
//! [`CacheMutationLock`]: crate::lock::CacheMutationLock

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::wallet_discovery::{SourceDiscoveryResult, WalletDiscoverySource, run_source_discovery};

/// Aggregate counts across all discovery sources.
#[derive(Debug, Default, Clone, Copy)]
pub struct WinnerDiscoveryReport {
    pub leaderboard_unique: usize,
    pub leaderboard_activated: usize,
    pub radion_unique: usize,
    pub radion_activated: usize,
    pub datadash_unique: usize,
    pub datadash_activated: usize,
}

/// Apply the Radion soft-fail policy: swallow a [`BootstrapError::Radion`]
/// (warn + zero counts) so a Radion outage or monthly-quota wall never breaks
/// `pe-bootstrap all`; propagate every other error variant unchanged
/// (issue #373 AC9).
fn soft_fail_radion(
    result: Result<SourceDiscoveryResult, BootstrapError>,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    match result {
        Ok(r) => Ok(r),
        Err(BootstrapError::Radion { message }) => {
            tracing::warn!(
                error = %message,
                "winner_discovery: radion source failed — continuing with zero counts"
            );
            Ok(SourceDiscoveryResult::default())
        }
        Err(e) => Err(e),
    }
}

/// Run all enabled discovery sources in sequence and return aggregate counts.
pub async fn run_winner_discovery(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    let lb = run_source_discovery(WalletDiscoverySource::Leaderboard, config, cache).await?;
    // Radion soft-fails: an outage/quota wall must not break `pe-bootstrap all`.
    // Only `BootstrapError::Radion` is swallowed; any other variant still propagates.
    let rd =
        soft_fail_radion(run_source_discovery(WalletDiscoverySource::Radion, config, cache).await)?;
    // Datadash soft-fails: a third-party outage must not break `pe-bootstrap all`.
    // Only `BootstrapError::Datadash` is swallowed; any other variant (e.g. a
    // shared-cache failure) still propagates as fatal.
    let dd = match run_source_discovery(WalletDiscoverySource::Datadash, config, cache).await {
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
        radion_unique: rd.unique_wallets,
        radion_activated: rd.activated,
        datadash_unique: dd.unique_wallets,
        datadash_activated: dd.activated,
    };
    tracing::info!(
        leaderboard_unique = report.leaderboard_unique,
        leaderboard_activated = report.leaderboard_activated,
        radion_unique = report.radion_unique,
        radion_activated = report.radion_activated,
        datadash_unique = report.datadash_unique,
        datadash_activated = report.datadash_activated,
        "winner_discovery: complete"
    );
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// AC9 — a `BootstrapError::Radion` is swallowed into zero counts.
    #[test]
    fn soft_fail_radion_swallows_radion_error() {
        let r = soft_fail_radion(Err(BootstrapError::Radion {
            message: "429 (retry-after: 3600)".to_owned(),
        }));
        let pass = matches!(r, Ok(v) if v.unique_wallets == 0 && v.activated == 0);
        println!(
            "{}: soft_fail_radion_swallows_radion_error",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected Ok(default) for Radion err; got {r:?}");
    }

    /// AC9 — a non-Radion error still propagates (only Radion is softened here).
    #[test]
    fn soft_fail_radion_propagates_other_error() {
        let r = soft_fail_radion(Err(BootstrapError::Datadash {
            message: "boom".to_owned(),
        }));
        let pass = matches!(r, Err(BootstrapError::Datadash { .. }));
        println!(
            "{}: soft_fail_radion_propagates_other_error",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected Err(Datadash) to propagate; got {r:?}");
    }

    /// AC9 — a success passes through unchanged.
    #[test]
    fn soft_fail_radion_passes_through_ok() {
        let ok = SourceDiscoveryResult {
            unique_wallets: 5,
            activated: 3,
        };
        let r = soft_fail_radion(Ok(ok));
        assert!(matches!(r, Ok(v) if v.unique_wallets == 5 && v.activated == 3));
    }
}
