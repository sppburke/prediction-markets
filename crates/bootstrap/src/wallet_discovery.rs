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
    DatadashDiscoveryReport, ReqwestCohortFetcher, run_datadash_discovery,
};
use crate::error::BootstrapError;
use crate::leaderboard_discovery::{
    LeaderboardDiscoveryReport, LeaderboardFetcher, run_leaderboard_discovery,
};
use crate::lock::CacheMutationLock;
use crate::radion::{ReqwestTraderFetcher, run_radion_discovery};

/// The supported wallet-discovery sources for `winner-discovery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletDiscoverySource {
    Leaderboard,
    Radion,
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
            let r: LeaderboardDiscoveryReport = run_leaderboard_discovery(
                &fetcher,
                &config.leaderboard_categories,
                config.leaderboard_top_n,
                cache,
            )
            .await?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
            })
        }
        WalletDiscoverySource::Radion => {
            // Radion is on by default; an empty/unset URL *or* API key disables it.
            // The API mandates a key, so "on" means inert until a key is provided
            // (an empty `PE_BOOTSTRAP_RADION_API_KEY` collapses to `None` and skips
            // silently rather than 401-soft-failing every run).
            let Some(base_url) = config.radion_api_url.as_deref() else {
                tracing::debug!("wallet_discovery: Radion skipped — radion_api_url not set");
                return Ok(SourceDiscoveryResult::default());
            };
            let Some(api_key) = config.radion_api_key.as_deref() else {
                tracing::debug!("wallet_discovery: Radion skipped — radion_api_key not set");
                return Ok(SourceDiscoveryResult::default());
            };
            // Network-facing failures in this arm map to `BootstrapError::Radion`
            // (client build, lock acquire, and every fetch/parse/empty path inside
            // `run_radion_discovery`) so the caller's soft-fail (`winner_discovery`)
            // catches every Radion *outage*. A genuine DB failure (cursor write,
            // upsert/activation) still propagates as its native `Sqlite`/`Cache`
            // variant (fatal) — see that function's docs.
            let client = reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(15))
                .build()
                .map_err(|e| BootstrapError::Radion {
                    message: format!("client build: {e}"),
                })?;
            let fetcher = ReqwestTraderFetcher::new(
                base_url.to_owned(),
                client,
                api_key.to_owned(),
                config.radion_request_interval_ms,
            );
            let _lock = CacheMutationLock::acquire(&config.cache_path).map_err(|e| {
                BootstrapError::Radion {
                    message: format!("lock: {e}"),
                }
            })?;
            run_radion_discovery(&fetcher, config.radion_max_requests_per_run, cache).await
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
            let r: DatadashDiscoveryReport = run_datadash_discovery(
                &fetcher,
                &config.datadash_exclude_ids,
                &config.datadash_exclude_titles,
                config.datadash_max_cohort_wallets,
                cache,
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

    /// AC10 — Radion is on by default (URL set) but skips silently with **no HTTP
    /// call** when no API key is configured. The URL points at an unroutable
    /// address: if the gate failed to skip, the arm would attempt a connection and
    /// return `Err`/hang instead of `Ok(default())`.
    #[tokio::test]
    async fn radion_skips_without_api_key() {
        let dir = TempDir::new().unwrap();
        let cache_path = dir.path().join("cache.db");
        let mut cache = WalletCache::open(&cache_path).unwrap();
        let config = BootstrapConfig {
            cache_path: cache_path.clone(),
            radion_api_url: Some("http://127.0.0.1:1".to_owned()),
            radion_api_key: None,
            ..BootstrapConfig::default()
        };

        let r = run_source_discovery(WalletDiscoverySource::Radion, &config, &mut cache)
            .await
            .unwrap();

        let pass = r.unique_wallets == 0 && r.activated == 0;
        println!(
            "{}: radion_skips_without_api_key (unique={}, activated={})",
            if pass { "PASS" } else { "FAIL" },
            r.unique_wallets,
            r.activated,
        );
        assert!(pass, "expected zero counts (skipped); got {r:?}");
    }

    /// AC10 (variant) — Radion skips silently when the URL is unset, even with a key.
    #[tokio::test]
    async fn radion_skips_without_api_url() {
        let dir = TempDir::new().unwrap();
        let cache_path = dir.path().join("cache.db");
        let mut cache = WalletCache::open(&cache_path).unwrap();
        let config = BootstrapConfig {
            cache_path,
            radion_api_url: None,
            radion_api_key: Some("rk_test".to_owned()),
            ..BootstrapConfig::default()
        };

        let r = run_source_discovery(WalletDiscoverySource::Radion, &config, &mut cache)
            .await
            .unwrap();
        assert_eq!(r.unique_wallets, 0);
        assert_eq!(r.activated, 0);
    }
}
