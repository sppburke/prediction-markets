//! Winner-discovery orchestrator: leaderboard, datadash and market participant ingest
//! (issues #324, #365, #800).
//!
//! `run_winner_discovery` runs all enabled discovery sources in sequence and
//! returns aggregate counts. The binary owns one central cache mutation lock
//! across the complete read-write command (#544).
//!
//! Failure policy: the leaderboard source propagates errors (fatal to
//! `pe-bootstrap all`). The **datadash** source **soft-fails** — a
//! [`BootstrapError::Datadash`] is logged and treated as zero counts so a
//! third-party outage never breaks the run (issue #365 AC9). Non-datadash errors
//! (e.g. a broken cache) still propagate. Participant collection errors also soft-fail
//! with a visible status; participant cache and activation errors remain fatal.
//! (The Radion source was retired once its upstream `traders/analysis` endpoint
//! was removed.)
//!
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
    pub participants_unique: usize,
    pub participants_activated: usize,
    pub participants_status: &'static str,
}

/// Run all enabled discovery sources in sequence and return aggregate counts.
pub async fn run_winner_discovery(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    run_winner_discovery_with_policy(config, cache, ActivationPolicy::Immediate).await
}

/// Winner discovery with an explicit activation policy. The full rank-and-push
/// wrapper defers all sources to one controlled batch.
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

    let participants = run_source_discovery_with_policy(
        WalletDiscoverySource::MarketParticipants,
        config,
        cache,
        activation_policy,
    )
    .await?;

    let report = WinnerDiscoveryReport {
        leaderboard_unique: lb.unique_wallets,
        leaderboard_activated: lb.activated,
        datadash_unique: dd.unique_wallets,
        datadash_activated: dd.activated,
        participants_unique: participants.unique_wallets,
        participants_activated: participants.activated,
        participants_status: participants.status,
    };
    tracing::info!(
        leaderboard_unique = report.leaderboard_unique,
        leaderboard_activated = report.leaderboard_activated,
        datadash_unique = report.datadash_unique,
        datadash_activated = report.datadash_activated,
        participants_unique = report.participants_unique,
        participants_activated = report.participants_activated,
        participants_status = report.participants_status,
        "winner_discovery: complete"
    );
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn leaderboard_zero_continues_to_enabled_participants() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let config = BootstrapConfig {
            leaderboard_top_n: 0,
            leaderboard_base_url: Some("invalid URL must never be fetched".into()),
            datadash_api_url: None,
            gamma_base_url: "invalid URL fails before network I/O".into(),
            participant_lookback_secs: 5,
            ..BootstrapConfig::default()
        };
        let report = run_winner_discovery(&config, &mut cache).await.unwrap();
        assert_eq!(
            (report.leaderboard_unique, report.leaderboard_activated),
            (0, 0)
        );
        assert_eq!((report.datadash_unique, report.datadash_activated), (0, 0));
        assert_eq!(report.participants_status, "failed:gamma");
    }
}
