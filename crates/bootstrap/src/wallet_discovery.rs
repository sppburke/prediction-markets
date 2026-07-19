//! Source-agnostic winner-discovery dispatch (issue #324).
//!
//! [`WalletDiscoverySource`] enumerates discovery sources.
//! [`run_source_discovery`] acquires [`CacheMutationLock`] for the DB-mutation
//! window, dispatches to the appropriate source, and returns aggregate counts.
//! The lock is RAII-dropped before returning — callers may shell out to long
//! subprocesses (e.g. backfill) without holding it.

use std::time::Duration;

use pe_source_polymarket_public::ReqwestFetcher;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::datadash_discovery::{
    DatadashDiscoveryReport, ReqwestCohortFetcher, run_datadash_discovery_with_policy,
};
use crate::error::BootstrapError;
use crate::leaderboard_discovery::{
    LeaderboardDiscoveryReport, LeaderboardFetcher, run_leaderboard_discovery_with_policy,
};
use crate::lock::CacheMutationLock;
use crate::pile::ActivationPolicy;

/// The supported wallet-discovery sources for `winner-discovery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletDiscoverySource {
    Leaderboard,
    Datadash,
}

/// Aggregate per-source counts returned by [`run_source_discovery`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceDiscoveryResult {
    pub unique_wallets: usize,
    pub activated: usize,
}

/// Run discovery for `source`, holding [`CacheMutationLock`] only for the
/// DB-mutation window. The lock is released before this function returns.
pub async fn run_source_discovery(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    run_source_discovery_with_policy(source, config, cache, ActivationPolicy::Immediate).await
}

/// Source discovery with an explicit activation policy. The rank-and-push
/// pipeline uses `Deferred`; standalone callers retain `Immediate`.
pub async fn run_source_discovery_with_policy(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    activation_policy: ActivationPolicy,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    match source {
        WalletDiscoverySource::Leaderboard => {
            let base_url = config
                .leaderboard_base_url
                .as_deref()
                .unwrap_or(&config.polymarket_base_url)
                .to_owned();
            let client = reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(15))
                .build()
                .map_err(|_| BootstrapError::Internal)?;
            let fetcher = LeaderboardFetcher::new(
                base_url,
                ReqwestFetcher::new(client)
                    .with_min_interval_ms(config.leaderboard_request_interval_ms),
            );
            let _lock = CacheMutationLock::acquire(&config.cache_path)?;
            let r: LeaderboardDiscoveryReport = run_leaderboard_discovery_with_policy(
                &fetcher,
                &config.leaderboard_categories,
                config.leaderboard_top_n,
                cache,
                activation_policy,
            )
            .await?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
            })
        }
        WalletDiscoverySource::Datadash => {
            // Datadash is on by default; an empty/unset URL disables it.
            let Some(base_url) = config.datadash_api_url.as_deref() else {
                tracing::debug!("wallet_discovery: Datadash skipped — datadash_api_url not set");
                return Ok(SourceDiscoveryResult::default());
            };
            // Network-facing failures in this arm map to
            // `BootstrapError::Datadash` (client build, lock acquire, and every
            // fetch/parse/empty path inside `run_datadash_discovery`) so the
            // caller's soft-fail (`winner_discovery`) catches every datadash
            // *outage*. A genuine DB failure (upsert/activation) still propagates
            // as its native `Sqlite`/`Cache` variant (fatal) — see that
            // function's docs.
            let client = reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(15))
                .build()
                .map_err(|e| BootstrapError::Datadash {
                    message: format!("client build: {e}"),
                })?;
            let fetcher = ReqwestCohortFetcher::new(
                base_url.to_owned(),
                client,
                config.datadash_request_interval_ms,
            );
            let _lock = CacheMutationLock::acquire(&config.cache_path).map_err(|e| {
                BootstrapError::Datadash {
                    message: format!("lock: {e}"),
                }
            })?;
            let r: DatadashDiscoveryReport = run_datadash_discovery_with_policy(
                &fetcher,
                &config.datadash_exclude_ids,
                &config.datadash_exclude_titles,
                config.datadash_max_cohort_wallets,
                cache,
                activation_policy,
            )
            .await?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Datadash is on by default (URL set from the compiled default) but skips
    /// silently with **no HTTP call** when the URL is unset. Mirrors the retired
    /// Radion gate test: an unset URL must yield `Ok(default())`, not a fetch.
    #[tokio::test]
    async fn datadash_skips_without_api_url() {
        let dir = TempDir::new().unwrap();
        let cache_path = dir.path().join("cache.db");
        let mut cache = WalletCache::open(&cache_path).unwrap();
        let config = BootstrapConfig {
            cache_path,
            datadash_api_url: None,
            ..BootstrapConfig::default()
        };

        let r = run_source_discovery(WalletDiscoverySource::Datadash, &config, &mut cache)
            .await
            .unwrap();
        assert_eq!(r.unique_wallets, 0);
        assert_eq!(r.activated, 0);
    }
}
