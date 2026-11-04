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
//! (e.g. a broken cache) still propagate. Participant collection errors return temporary failure;
//! participant cache and activation errors remain fatal.
//! (The Radion source was retired once its upstream `traders/analysis` endpoint
//! was removed.)
//!
use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::pile::ActivationPolicy;
use crate::wallet_discovery::{
    CollectedSource, SourceDiscoveryResult, WalletDiscoverySource, apply_source_discovery,
    collect_source_discovery, run_source_discovery_with_policy,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

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
    run_winner_discovery_with_policy(config, cache, ActivationPolicy::Immediate, None).await
}

/// Winner discovery with an explicit activation policy. The full rank-and-push
/// wrapper defers all sources to one controlled batch.
pub async fn run_winner_discovery_with_policy(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    activation_policy: ActivationPolicy,
    market_floor: Option<i64>,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    let lb = run_source_discovery_with_policy(
        WalletDiscoverySource::Leaderboard,
        config,
        cache,
        activation_policy,
        market_floor,
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
        market_floor,
    )
    .await
    {
        Ok(r) => r,
        Err(BootstrapError::Datadash { message }) => datadash_soft_failure(&message),
        Err(e) => return Err(e),
    };

    let participants = run_source_discovery_with_policy(
        WalletDiscoverySource::MarketParticipants,
        config,
        cache,
        activation_policy,
        market_floor,
    )
    .await?;

    Ok(complete_report(lb, dd, participants))
}

fn datadash_soft_failure(message: &str) -> SourceDiscoveryResult {
    tracing::warn!(error = %message, "winner_discovery: datadash source failed — continuing with zero counts");
    SourceDiscoveryResult::default()
}

fn complete_report(
    lb: SourceDiscoveryResult,
    dd: SourceDiscoveryResult,
    participants: SourceDiscoveryResult,
) -> WinnerDiscoveryReport {
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
    report
}

/// Only request/collection settings belong in the hand-off. Cache tuning and paths
/// are deliberately absent: application reads tombstones from its own candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoverySettings {
    leaderboard_categories: Vec<String>,
    leaderboard_top_n: u32,
    leaderboard_request_interval_ms: u64,
    leaderboard_url: String,
    polymarket_base_url: String,
    gamma_base_url: String,
    datadash_api_url: Option<String>,
    datadash_request_interval_ms: u64,
    datadash_exclude_ids: Vec<String>,
    datadash_exclude_titles: Vec<String>,
    datadash_max_cohort_wallets: u64,
    participant_lookback_secs: u64,
    market_recurrences: Vec<String>,
    market_floor: Option<i64>,
    activation_policy: String,
}

fn discovery_settings(config: &BootstrapConfig, market_floor: Option<i64>) -> DiscoverySettings {
    DiscoverySettings {
        leaderboard_categories: config
            .leaderboard_categories
            .iter()
            .map(|c| c.as_param().to_owned())
            .collect(),
        leaderboard_top_n: config.leaderboard_top_n,
        leaderboard_request_interval_ms: config.leaderboard_request_interval_ms,
        leaderboard_url: config
            .leaderboard_base_url
            .as_ref()
            .unwrap_or(&config.polymarket_base_url)
            .clone(),
        polymarket_base_url: config.polymarket_base_url.clone(),
        gamma_base_url: config.gamma_base_url.clone(),
        datadash_api_url: config.datadash_api_url.clone(),
        datadash_request_interval_ms: config.datadash_request_interval_ms,
        datadash_exclude_ids: config.datadash_exclude_ids.clone(),
        datadash_exclude_titles: config.datadash_exclude_titles.clone(),
        datadash_max_cohort_wallets: config.datadash_max_cohort_wallets,
        participant_lookback_secs: config.participant_lookback_secs,
        market_recurrences: config.market_recurrences.clone(),
        market_floor,
        activation_policy: "deferred".to_owned(),
    }
}

const SOURCES: [WalletDiscoverySource; 3] = [
    WalletDiscoverySource::Leaderboard,
    WalletDiscoverySource::Datadash,
    WalletDiscoverySource::MarketParticipants,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum DiscoveryOutcome {
    Collected { data: CollectedSource },
    Disabled,
    SoftFailed { message: String },
    Terminal { exit_code: i32, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceOutcome {
    source: WalletDiscoverySource,
    result: DiscoveryOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverySpool {
    version: u32,
    settings: DiscoverySettings,
    coverage_read: bool,
    prior_coverage_raw: Option<String>,
    outcomes: Vec<SourceOutcome>,
}

fn refused(error: impl std::fmt::Display) -> BootstrapError {
    BootstrapError::DiscoverySpoolRefused {
        message: error.to_string(),
    }
}

impl DiscoverySpool {
    fn validate(
        &self,
        config: &BootstrapConfig,
        market_floor: Option<i64>,
    ) -> Result<(), BootstrapError> {
        if self.version != 1 || self.settings != discovery_settings(config, market_floor) {
            return Err(refused("version or discovery settings differ"));
        }
        if self.outcomes.is_empty() || self.outcomes.len() > SOURCES.len() {
            return Err(refused("invalid outcome prefix"));
        }
        let reached_participants = self.outcomes.len() == 3;
        if self.coverage_read != (reached_participants && config.participant_lookback_secs > 0)
            || (!self.coverage_read && self.prior_coverage_raw.is_some())
        {
            return Err(refused(
                "coverage_read contradicts outcome prefix or settings",
            ));
        }
        for (index, entry) in self.outcomes.iter().enumerate() {
            if entry.source != SOURCES[index] {
                return Err(refused("source order differs"));
            }
            let disabled = match entry.source {
                WalletDiscoverySource::Leaderboard => config.leaderboard_top_n == 0,
                WalletDiscoverySource::Datadash => config.datadash_api_url.is_none(),
                WalletDiscoverySource::MarketParticipants => config.participant_lookback_secs == 0,
            };
            if disabled != matches!(entry.result, DiscoveryOutcome::Disabled) {
                return Err(refused("disabled outcome contradicts settings"));
            }
            match &entry.result {
                DiscoveryOutcome::Collected { data } => {
                    let wallets = match (entry.source, data) {
                        (WalletDiscoverySource::Leaderboard, CollectedSource::Leaderboard(c)) => {
                            let r = c.report;
                            if r.unique_wallets != c.wallets.len()
                                || r.raw_entries < r.unique_wallets
                                || r.slices_attempted
                                    != config.leaderboard_categories.len()
                                        * crate::leaderboard_discovery::SORT_WINDOW_SLICES.len()
                                || r.slices_fetched == 0
                                || r.slices_fetched > r.slices_attempted
                                || r.activated != 0
                                || c.wallets.is_empty()
                            {
                                return Err(refused("invalid leaderboard counts"));
                            }
                            &c.wallets
                        }
                        (WalletDiscoverySource::Datadash, CollectedSource::Datadash(c)) => {
                            let r = c.report;
                            if r.unique_wallets != c.wallets.len()
                                || r.raw_addresses < r.unique_wallets
                                || r.cohorts_kept == 0
                                || r.cohorts_kept > r.cohorts_listed
                                || r.activated != 0
                                || c.wallets.is_empty()
                            {
                                return Err(refused("invalid datadash counts"));
                            }
                            &c.wallets
                        }
                        (
                            WalletDiscoverySource::MarketParticipants,
                            CollectedSource::MarketParticipants(c),
                        ) => {
                            c.coverage.validate().map_err(refused)?;
                            let recurrences = crate::market_scope::canonical_recurrences(
                                &config.market_recurrences,
                            )
                            .map_err(refused)?;
                            if c.coverage.scope.recurrences != recurrences
                                || c.coverage.scope.series.is_none()
                                || c.wallets.iter().any(|wallet| {
                                    pe_core_types::WalletAddress::from_hex(wallet).is_err()
                                })
                            {
                                return Err(refused("invalid participant scope or wallet"));
                            }
                            if c.now < 0
                                || c.coverage.covered_from_unix >= c.now
                                || c.coverage.covered_through_unix < c.now
                            {
                                return Err(refused("invalid participant clock"));
                            }
                            &c.wallets
                        }
                        _ => return Err(refused("collected source differs")),
                    };
                    if wallets.windows(2).any(|pair| pair[0] >= pair[1])
                        || wallets.iter().any(|w| {
                            w.len() != 42 || !w.starts_with("0x") || *w != w.to_ascii_lowercase()
                        })
                    {
                        return Err(refused(
                            "wallets must be sorted, unique normalized addresses",
                        ));
                    }
                }
                DiscoveryOutcome::Disabled => {}
                DiscoveryOutcome::SoftFailed { message } => {
                    if entry.source != WalletDiscoverySource::Datadash || message.is_empty() {
                        return Err(refused("invalid soft failure"));
                    }
                }
                DiscoveryOutcome::Terminal { exit_code, message } => {
                    if index + 1 != self.outcomes.len()
                        || ![1, 75].contains(exit_code)
                        || message.is_empty()
                        || (*exit_code == 75
                            && entry.source != WalletDiscoverySource::MarketParticipants)
                    {
                        return Err(refused("invalid terminal outcome"));
                    }
                }
            }
        }
        if self.outcomes.len() < SOURCES.len()
            && !matches!(
                self.outcomes.last().map(|e| &e.result),
                Some(DiscoveryOutcome::Terminal { .. })
            )
        {
            return Err(refused("incomplete nonterminal prefix"));
        }
        Ok(())
    }
}

async fn wait_not_before(
    not_before: i64,
    mut now: impl FnMut() -> i64,
    mut sleep: impl AsyncFnMut(std::time::Duration),
) -> i64 {
    loop {
        let current = now();
        if current >= not_before {
            return current;
        }
        let seconds = u64::try_from(not_before.saturating_sub(current)).unwrap_or(u64::MAX);
        sleep(std::time::Duration::from_secs(seconds)).await;
    }
}

/// Collect without a WalletCache opener or mutation lock. Only immutable local
/// read failures abort without a spool; source errors are replayed by application.
pub async fn collect_winner_discovery(
    config: &BootstrapConfig,
    spool_path: &Path,
    coverage_from: &Path,
    not_before: i64,
    market_floor: Option<i64>,
) -> Result<(), BootstrapError> {
    pe_event_log::run_blocking(|| {
        crate::cache_migration::require_discovery_spool_roles(coverage_from, spool_path)?;
        if spool_path.try_exists()? || std::fs::symlink_metadata(spool_path).is_ok() {
            return Err(refused("collector spool already exists"));
        }
        if crate::cache_migration::discovery_immutable_version(coverage_from)? != 2 {
            return Err(refused("collector requires schema two"));
        }
        Ok(())
    })?;
    let mut spool = DiscoverySpool {
        version: 1,
        settings: discovery_settings(config, market_floor),
        coverage_read: false,
        prior_coverage_raw: None,
        outcomes: Vec::new(),
    };
    for source in SOURCES {
        let now = if source == WalletDiscoverySource::MarketParticipants
            && config.participant_lookback_secs > 0
        {
            wait_not_before(
                not_before,
                || time::OffsetDateTime::now_utc().unix_timestamp(),
                async |duration| tokio::time::sleep(duration).await,
            )
            .await
        } else {
            time::OffsetDateTime::now_utc().unix_timestamp()
        };
        let mut local_error = None;
        let result = collect_source_discovery(source, config, market_floor, now, || {
            let raw = pe_event_log::run_blocking(|| {
                crate::cache_migration::discovery_immutable_coverage(coverage_from)
            });
            match raw {
                Ok(raw) => {
                    spool.coverage_read = true;
                    spool.prior_coverage_raw = raw.clone();
                    crate::cache::decode_participant_discovery_coverage(raw)
                }
                Err(error) => {
                    local_error = Some(error);
                    Err(BootstrapError::Internal)
                }
            }
        })
        .await;
        if let Some(error) = local_error {
            return Err(error);
        }
        let outcome = match result {
            Ok(CollectedSource::Disabled) => DiscoveryOutcome::Disabled,
            Ok(data) => DiscoveryOutcome::Collected { data },
            Err(BootstrapError::Datadash { message })
                if source == WalletDiscoverySource::Datadash =>
            {
                DiscoveryOutcome::SoftFailed { message }
            }
            Err(error) => DiscoveryOutcome::Terminal {
                exit_code: error.exit_code(),
                message: error.to_string(),
            },
        };
        let terminal = matches!(outcome, DiscoveryOutcome::Terminal { .. });
        spool.outcomes.push(SourceOutcome {
            source,
            result: outcome,
        });
        if terminal {
            break;
        }
    }
    spool.validate(config, market_floor)?;
    pe_event_log::run_blocking(|| {
        crate::cache_migration::atomic_write_json(spool_path, &spool)?;
        log_spool(spool_path, &std::fs::read(spool_path)?);
        Ok(())
    })
}

fn log_spool(path: &Path, bytes: &[u8]) {
    tracing::info!(path = %path.display(), sha256 = %format!("{:x}", Sha256::digest(bytes)), "winner-discovery: spool");
}

/// Step one, before taking the candidate lock or opening SQLite writably.
pub fn read_discovery_spool(
    config: &BootstrapConfig,
    path: &Path,
    market_floor: Option<i64>,
) -> Result<DiscoverySpool, BootstrapError> {
    pe_event_log::run_blocking(|| {
        crate::cache_migration::require_discovery_spool_roles(&config.cache_path, path)
            .map_err(refused)?;
        let bytes = std::fs::read(path).map_err(refused)?;
        let spool: DiscoverySpool = serde_json::from_slice(&bytes).map_err(refused)?;
        spool.validate(config, market_floor)?;
        log_spool(path, &bytes);
        Ok(spool)
    })
}

/// Called with the candidate lock held, before its writable opener/migrations.
pub fn bind_discovery_spool(
    spool: &DiscoverySpool,
    candidate: &Path,
) -> Result<(), BootstrapError> {
    pe_event_log::run_blocking(|| {
        if spool.coverage_read {
            let cache = WalletCache::open_read_only(candidate).map_err(refused)?;
            let raw = cache
                .raw_participant_discovery_coverage()
                .map_err(refused)?;
            drop(cache);
            if raw != spool.prior_coverage_raw {
                return Err(refused("candidate raw coverage differs"));
            }
        }
        Ok(())
    })
}

/// Apply the same commits as plain discovery, stopping at its first error.
pub fn apply_discovery_spool(
    spool: DiscoverySpool,
    cache: &mut WalletCache,
) -> Result<WinnerDiscoveryReport, BootstrapError> {
    pe_event_log::run_blocking(|| {
        let mut results = [SourceDiscoveryResult::default(); 3];
        for (index, entry) in spool.outcomes.into_iter().enumerate() {
            results[index] = match entry.result {
                DiscoveryOutcome::Collected { data } => {
                    apply_source_discovery(cache, ActivationPolicy::Deferred, data)?
                }
                DiscoveryOutcome::Disabled => SourceDiscoveryResult {
                    status: if entry.source == WalletDiscoverySource::MarketParticipants {
                        "disabled"
                    } else {
                        ""
                    },
                    ..SourceDiscoveryResult::default()
                },
                DiscoveryOutcome::SoftFailed { message } => datadash_soft_failure(&message),
                DiscoveryOutcome::Terminal { exit_code, message } => {
                    return Err(BootstrapError::DiscoveryRecorded { exit_code, message });
                }
            };
        }
        let [lb, dd, participants] = results;
        Ok(complete_report(lb, dd, participants))
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
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
        let error = run_winner_discovery(&config, &mut cache).await.unwrap_err();
        assert_eq!(error.exit_code(), 75);
        assert!(error.to_string().contains("failed:gamma"));
    }

    use crate::datadash_discovery::{CohortMeta, FixtureCohortFetcher, collect_datadash};
    use crate::leaderboard_discovery::{
        LeaderboardFetcher, SORT_WINDOW_SLICES, collect_leaderboard,
    };
    use crate::market_scope::{FetchedPage, MarketScope, ScopeFetcher, ScopeSeries};
    use crate::wallet_discovery::{
        ParticipantDiscoveryCoverage, ParticipantScope, collect_participants,
    };
    use pe_source_polymarket_public::{FixtureFetcher, endpoint::PolymarketEndpoint};
    use serde_json::json;
    use std::collections::HashMap;

    const NOW: i64 = 1_800_057_600; // UTC midnight: the not-before test crosses it.

    fn wallet(n: u8) -> String {
        format!("0x{n:040x}")
    }
    fn fixture_config(path: &Path) -> BootstrapConfig {
        BootstrapConfig {
            cache_path: path.to_owned(),
            leaderboard_top_n: 50,
            participant_lookback_secs: 100,
            polymarket_base_url: "https://fixture".into(),
            ..BootstrapConfig::default()
        }
    }
    fn coverage() -> ParticipantDiscoveryCoverage {
        ParticipantDiscoveryCoverage {
            version: 1,
            scope: ParticipantScope::new(
                &["15m".into()],
                Some(&[ScopeSeries {
                    id: "7".into(),
                    slug: Some("seven".into()),
                }]),
            )
            .unwrap(),
            market_floor_unix: 0,
            covered_from_unix: NOW - 200,
            covered_through_unix: NOW - 50,
        }
    }
    struct TradesFixture;
    impl ScopeFetcher for TradesFixture {
        async fn fetch(
            &self,
            _: &str,
            _: &'static str,
            _: &'static str,
        ) -> Result<FetchedPage, pe_source_core::SourceError> {
            Ok(FetchedPage { body: serde_json::to_vec(&json!({"data": [{"proxy_wallet": wallet(3), "timestamp": NOW - 1}, {"proxy_wallet": wallet(4), "timestamp": NOW - 2}], "pagination": {"limit":1000,"offset":0,"has_more":false,"next_cursor":null}})).unwrap(), observed_at_unix: NOW, received_at_unix: NOW, source_at_unix: Some(NOW) })
        }
    }
    async fn fixture_collect(
        source: WalletDiscoverySource,
        config: &BootstrapConfig,
        raw: Option<String>,
        case: &str,
        calls: &mut Vec<WalletDiscoverySource>,
        now: i64,
    ) -> Result<CollectedSource, BootstrapError> {
        calls.push(source);
        match source {
            WalletDiscoverySource::Leaderboard => {
                if config.leaderboard_top_n == 0 {
                    return Ok(CollectedSource::Disabled);
                }
                let body = if case == "leaderboard_empty" || case == "terminal_existing" {
                    json!([])
                } else {
                    json!([{"proxyWallet":wallet(1)}, {"proxyWallet":wallet(1)}])
                };
                let mut fixtures = HashMap::new();
                for &category in &config.leaderboard_categories {
                    for (sort, window) in SORT_WINDOW_SLICES {
                        fixtures.insert(
                            PolymarketEndpoint::Leaderboard {
                                sort,
                                window,
                                category,
                                limit: 50,
                                offset: 0,
                            }
                            .url(&config.polymarket_base_url),
                            serde_json::to_vec(&body).unwrap(),
                        );
                    }
                }
                collect_leaderboard(
                    &LeaderboardFetcher::new(
                        config.polymarket_base_url.clone(),
                        FixtureFetcher::new(fixtures),
                    ),
                    &config.leaderboard_categories,
                    config.leaderboard_top_n,
                )
                .await
                .map(CollectedSource::Leaderboard)
            }
            WalletDiscoverySource::Datadash => {
                if config.datadash_api_url.is_none() {
                    return Ok(CollectedSource::Disabled);
                }
                let cohorts = if case == "soft_datadash" {
                    vec![]
                } else {
                    vec![CohortMeta {
                        id: "fixture".into(),
                        title: "fixture".into(),
                        num_wallets: 2,
                    }]
                };
                let fetcher = FixtureCohortFetcher::new(
                    cohorts,
                    HashMap::from([("fixture".into(), vec![wallet(1), wallet(2)])]),
                );
                collect_datadash(&fetcher, &[], &[], 10_000)
                    .await
                    .map(CollectedSource::Datadash)
            }
            WalletDiscoverySource::MarketParticipants => {
                collect_participants(
                    config,
                    || crate::cache::decode_participant_discovery_coverage(raw),
                    now,
                    || Ok(TradesFixture),
                    || async {
                        if case == "participant_75" || case == "earlier_upsert" {
                            return Err(BootstrapError::Gamma {
                                message: "fixture failure".into(),
                            });
                        }
                        Ok((coverage().scope.series.unwrap(), Vec::new()))
                    },
                    |_, series| async move {
                        Ok(MarketScope {
                            version: 1,
                            source_id: "fixture",
                            schema_version: 1,
                            parser_version: 1,
                            recurrence: "15m".into(),
                            end_date_min_unix: 0,
                            started_at_unix: NOW,
                            completed_at_unix: NOW,
                            series,
                            pages: vec![],
                            condition_ids: vec![format!("0x{:064x}", 1)],
                            count: 1,
                            digest: "fixture".into(),
                        })
                    },
                )
                .await
            }
        }
    }

    fn outcome(result: Result<CollectedSource, BootstrapError>) -> DiscoveryOutcome {
        match result {
            Ok(CollectedSource::Disabled) => DiscoveryOutcome::Disabled,
            Ok(data) => DiscoveryOutcome::Collected { data },
            Err(BootstrapError::Datadash { message }) => DiscoveryOutcome::SoftFailed { message },
            Err(error) => DiscoveryOutcome::Terminal {
                exit_code: error.exit_code(),
                message: error.to_string(),
            },
        }
    }
    fn snapshot(cache: &WalletCache) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
        ["wallets", "purged_wallets", "meta"]
            .iter()
            .map(|table| {
                let mut statement = cache
                    .raw_conn_for_test()
                    .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))
                    .unwrap();
                let columns: Vec<_> = statement
                    .column_names()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, n)| (*n != "discovered_at_unix").then_some(i))
                    .collect();
                statement
                    .query_map([], |row| columns.iter().map(|i| row.get(*i)).collect())
                    .unwrap()
                    .map(Result::unwrap)
                    .collect()
            })
            .collect()
    }
    async fn parity(case: &str, expected_code: i32, expected_sources: &[i64]) {
        let dir = tempfile::tempdir().unwrap();
        let paths = [dir.path().join("plain.db"), dir.path().join("split.db")];
        for path in &paths {
            let mut cache = WalletCache::open(path).unwrap();
            cache
                .set_participant_discovery_coverage(&coverage())
                .unwrap();
            cache
                .raw_conn_for_test()
                .execute(
                    "INSERT INTO purged_wallets VALUES (?1, 1, 'proven_loser')",
                    [wallet(3)],
                )
                .unwrap();
            cache
                .raw_conn_for_test()
                .execute(
                    "INSERT INTO purged_wallets VALUES (?1, 1, 'infra')",
                    [wallet(4)],
                )
                .unwrap();
            if ["bad_coverage", "disabled_malformed", "same_malformed"].contains(&case) {
                cache.raw_conn_for_test().execute_batch("UPDATE meta SET value='malformed' WHERE key='participant_discovery_coverage'").unwrap();
            }
            if case == "coverage_trigger" {
                cache.raw_conn_for_test().execute_batch("CREATE TRIGGER stop_coverage BEFORE INSERT ON meta WHEN NEW.key='participant_discovery_coverage' BEGIN SELECT RAISE(ABORT, 'coverage fixture'); END;").unwrap();
            }
            let fail = match case {
                "leaderboard_upsert" | "earlier_upsert" => Some(16),
                "datadash_upsert" => Some(128),
                "participant_upsert" => Some(256),
                _ => None,
            };
            if let Some(bits) = fail {
                cache.raw_conn_for_test().execute_batch(&format!("CREATE TRIGGER stop_upsert BEFORE INSERT ON wallets WHEN NEW.source_bits={bits} BEGIN SELECT RAISE(ABORT, 'upsert fixture'); END;")).unwrap();
            }
        }
        let mut config = fixture_config(&paths[0]);
        if case == "disabled" {
            config.leaderboard_top_n = 0;
            config.datadash_api_url = None;
            config.participant_lookback_secs = 0;
        }
        if case == "disabled_malformed" {
            config.participant_lookback_secs = 0;
        }
        let mut plain = WalletCache::open(&paths[0]).unwrap();
        let mut plain_calls = Vec::new();
        let plain_start = time::OffsetDateTime::now_utc().unix_timestamp();
        let mut code = 0;
        for source in SOURCES {
            let raw = if source == WalletDiscoverySource::MarketParticipants
                && config.participant_lookback_secs > 0
            {
                plain.raw_participant_discovery_coverage().unwrap()
            } else {
                None
            };
            match fixture_collect(source, &config, raw, case, &mut plain_calls, NOW).await {
                Ok(data) => {
                    if let Err(error) =
                        apply_source_discovery(&mut plain, ActivationPolicy::Deferred, data)
                    {
                        code = error.exit_code();
                        break;
                    }
                }
                Err(BootstrapError::Datadash { .. }) => {}
                Err(error) => {
                    code = error.exit_code();
                    break;
                }
            }
        }
        assert_eq!(code, expected_code, "plain {case}");
        let plain_end = time::OffsetDateTime::now_utc().unix_timestamp();
        config.cache_path = paths[1].clone();
        let mut split = WalletCache::open(&paths[1]).unwrap();
        let mut spool = DiscoverySpool {
            version: 1,
            settings: discovery_settings(&config, Some(0)),
            coverage_read: false,
            prior_coverage_raw: None,
            outcomes: vec![],
        };
        let mut split_calls = Vec::new();
        for source in SOURCES {
            let raw = if source == WalletDiscoverySource::MarketParticipants
                && config.participant_lookback_secs > 0
            {
                spool.coverage_read = true;
                spool.prior_coverage_raw = split.raw_participant_discovery_coverage().unwrap();
                spool.prior_coverage_raw.clone()
            } else {
                None
            };
            let result =
                outcome(fixture_collect(source, &config, raw, case, &mut split_calls, NOW).await);
            let terminal = matches!(result, DiscoveryOutcome::Terminal { .. });
            spool.outcomes.push(SourceOutcome { source, result });
            if terminal {
                break;
            }
        }
        spool.validate(&config, Some(0)).unwrap();
        let file = dir.path().join("spool.json");
        crate::cache_migration::atomic_write_json(&file, &spool).unwrap();
        let spool = read_discovery_spool(&config, &file, Some(0)).unwrap();
        let _lock = crate::lock::CacheMutationLock::acquire(&paths[1]).unwrap();
        bind_discovery_spool(&spool, &paths[1]).unwrap();
        let split_start = time::OffsetDateTime::now_utc().unix_timestamp();
        let result = apply_discovery_spool(spool, &mut split);
        assert_eq!(
            result.err().map_or(0, |e| e.exit_code()),
            expected_code,
            "split {case}"
        );
        let split_end = time::OffsetDateTime::now_utc().unix_timestamp();
        assert_eq!(snapshot(&plain), snapshot(&split), "{case}");
        for (cache, lo, hi) in [
            (&plain, plain_start, plain_end),
            (&split, split_start, split_end),
        ] {
            let mut stmt = cache
                .raw_conn_for_test()
                .prepare("SELECT discovered_at_unix FROM wallets")
                .unwrap();
            for at in stmt.query_map([], |r| r.get::<_, i64>(0)).unwrap() {
                assert!((lo..=hi).contains(&at.unwrap()));
            }
        }
        let bits: Vec<i64> = plain
            .raw_conn_for_test()
            .prepare("SELECT source_bits FROM wallets ORDER BY wallet_hex")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(bits, expected_sources, "pinned plain rows {case}");
        if case == "success" {
            let rows = &snapshot(&plain)[0];
            use rusqlite::types::Value::{Integer as I, Null as N, Text as T};
            assert_eq!(
                rows,
                &vec![
                    vec![
                        T(wallet(1)),
                        I(0),
                        I(0),
                        I(0),
                        I(144),
                        N,
                        I(0),
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        I(0)
                    ],
                    vec![
                        T(wallet(2)),
                        I(0),
                        I(0),
                        I(0),
                        I(128),
                        N,
                        I(0),
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        I(0)
                    ],
                    vec![
                        T(wallet(3)),
                        I(0),
                        I(0),
                        I(0),
                        I(256),
                        N,
                        I(0),
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        I(0)
                    ]
                ]
            );
            assert!(!plain.is_purged(&wallet(3)).unwrap());
            assert!(plain.is_purged(&wallet(4)).unwrap());
            assert_eq!(
                plain
                    .participant_discovery_coverage()
                    .unwrap()
                    .unwrap()
                    .covered_through_unix,
                NOW
            );
        }
        if case == "leaderboard_empty" || case == "terminal_existing" {
            assert_eq!(plain_calls, vec![SOURCES[0]]);
            assert_eq!(split_calls, plain_calls);
        }
    }

    #[tokio::test]
    async fn discovery_spool_same_writes_success_and_apply_timestamps() {
        parity("success", 0, &[144, 128, 256]).await;
    }
    #[tokio::test]
    async fn discovery_spool_same_writes_soft_failure_and_disabled() {
        parity("soft_datadash", 0, &[16, 256]).await;
        parity("disabled", 0, &[]).await;
    }
    #[tokio::test]
    async fn discovery_spool_participant_failure_preserves_earlier_commits() {
        parity("participant_75", 75, &[144, 128]).await;
    }
    #[tokio::test]
    async fn discovery_spool_leaderboard_terminal_stops_requests_and_skips_binding() {
        parity("leaderboard_empty", 1, &[]).await;
        parity("terminal_existing", 1, &[]).await;
    }
    #[tokio::test]
    async fn discovery_spool_malformed_coverage_terminal_and_disabled_parity() {
        for case in ["bad_coverage", "same_malformed"] {
            parity(case, 1, &[144, 128]).await;
        }
        parity("disabled_malformed", 0, &[144, 128]).await;
    }
    #[tokio::test]
    async fn discovery_spool_coverage_commit_failure_keeps_participant_upsert() {
        parity("coverage_trigger", 1, &[144, 128, 256]).await;
    }
    #[tokio::test]
    async fn discovery_spool_each_upsert_failure_and_earlier_error_precedence() {
        for (case, bits) in [
            ("leaderboard_upsert", vec![]),
            ("datadash_upsert", vec![16]),
            ("participant_upsert", vec![144, 128]),
            ("earlier_upsert", vec![]),
        ] {
            parity(case, 1, &bits).await;
        }
    }

    fn disabled_spool(config: &BootstrapConfig) -> DiscoverySpool {
        DiscoverySpool {
            version: 1,
            settings: discovery_settings(config, None),
            coverage_read: false,
            prior_coverage_raw: None,
            outcomes: SOURCES
                .into_iter()
                .map(|source| SourceOutcome {
                    source,
                    result: DiscoveryOutcome::Disabled,
                })
                .collect(),
        }
    }
    fn disabled_config(path: &Path) -> BootstrapConfig {
        BootstrapConfig {
            cache_path: path.into(),
            leaderboard_top_n: 0,
            datadash_api_url: None,
            participant_lookback_secs: 0,
            ..BootstrapConfig::default()
        }
    }

    #[tokio::test]
    async fn discovery_spool_collector_immutable_no_lock_or_sidecars_and_schema_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        let cache = WalletCache::open(&fixed).unwrap();
        cache
            .raw_conn_for_test()
            .execute_batch("PRAGMA user_version=2")
            .unwrap();
        drop(cache);
        let _lock = crate::lock::CacheMutationLock::acquire(&fixed).unwrap();
        let before = std::fs::read(&fixed).unwrap();
        let modified = std::fs::metadata(&fixed).unwrap().modified().unwrap();
        let list = || {
            let mut files: Vec<_> = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            files.sort();
            files
        };
        let listing = list();
        let spool = dir.path().join("spool.json");
        collect_winner_discovery(&disabled_config(&fixed), &spool, &fixed, i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&fixed).unwrap(), before);
        assert_eq!(
            std::fs::metadata(&fixed).unwrap().modified().unwrap(),
            modified
        );
        std::fs::remove_file(&spool).unwrap();
        assert_eq!(list(), listing);
        let mut enabled = disabled_config(&fixed);
        enabled.participant_lookback_secs = 100;
        enabled.gamma_base_url = "invalid URL fails before network".into();
        collect_winner_discovery(&enabled, &spool, &fixed, 0, None)
            .await
            .unwrap();
        let collected: DiscoverySpool =
            serde_json::from_slice(&std::fs::read(&spool).unwrap()).unwrap();
        assert!(collected.coverage_read);
        assert_eq!(std::fs::read(&fixed).unwrap(), before);
        assert_eq!(
            std::fs::metadata(&fixed).unwrap().modified().unwrap(),
            modified
        );
        std::fs::remove_file(&spool).unwrap();
        assert_eq!(list(), listing);
        let conn = rusqlite::Connection::open(&fixed).unwrap();
        conn.execute_batch("PRAGMA user_version=1").unwrap();
        drop(conn);
        assert!(
            collect_winner_discovery(&fixture_config(&fixed), &spool, &fixed, 0, None)
                .await
                .is_err()
        );
        assert!(!spool.exists());
    }

    #[tokio::test]
    async fn discovery_spool_collisions_and_existing_spool_refuse_before_requests() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        let cache = WalletCache::open(&fixed).unwrap();
        cache
            .raw_conn_for_test()
            .execute_batch("PRAGMA user_version=2")
            .unwrap();
        drop(cache);
        let mut config = fixture_config(&fixed);
        config.leaderboard_base_url = Some("invalid URL fails before network".into());
        let before = std::fs::read(&fixed).unwrap();
        let hard = dir.path().join("hard");
        std::fs::hard_link(&fixed, &hard).unwrap();
        let sym = dir.path().join("sym");
        std::os::unix::fs::symlink(&fixed, &sym).unwrap();
        let existing = dir.path().join("existing");
        std::fs::write(&existing, b"keep").unwrap();
        for file in [
            fixed.clone(),
            fixed.with_extension("db-wal"),
            fixed.with_extension("db-shm"),
            fixed.with_extension("db-journal"),
            crate::lock::lock_path_for(&fixed),
            hard,
            sym,
            existing.clone(),
        ] {
            assert!(
                collect_winner_discovery(&config, &file, &fixed, 0, None)
                    .await
                    .is_err(),
                "{}",
                file.display()
            );
            assert_eq!(std::fs::read(&fixed).unwrap(), before);
        }
        assert_eq!(std::fs::read(existing).unwrap(), b"keep");
        let spool = dir.path().join("temp_collision.json");
        let temp = dir
            .path()
            .join(format!(".temp_collision.json.{}.tmp", std::process::id()));
        std::fs::hard_link(&fixed, temp).unwrap();
        assert!(
            collect_winner_discovery(&config, &spool, &fixed, 0, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn discovery_spool_local_immutable_read_error_has_no_spool() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        let cache = WalletCache::open(&fixed).unwrap();
        cache
            .raw_conn_for_test()
            .execute_batch("PRAGMA user_version=2; INSERT INTO meta VALUES ('participant_discovery_coverage', '{}')")
            .unwrap();
        let root: u64 = cache
            .raw_conn_for_test()
            .query_row(
                "SELECT rootpage FROM sqlite_master WHERE name='meta'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let page_size: u64 = cache
            .raw_conn_for_test()
            .pragma_query_value(None, "page_size", |r| r.get(0))
            .unwrap();
        drop(cache);
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&fixed)
            .unwrap();
        file.seek(SeekFrom::Start((root - 1) * page_size)).unwrap();
        file.write_all(&[0xff; 16]).unwrap();
        drop(file);
        let mut config = disabled_config(&fixed);
        config.participant_lookback_secs = 100;
        config.gamma_base_url = "invalid URL fails before network".into();
        let spool = dir.path().join("spool.json");
        assert!(
            collect_winner_discovery(&config, &spool, &fixed, 0, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("malformed")
        );
        assert!(!spool.exists());
    }

    #[test]
    fn discovery_spool_binding_refusals_preserve_old_schema_two_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let candidate = dir.path().join("candidate.db");
        let cache = WalletCache::open(&candidate).unwrap();
        cache
            .raw_conn_for_test()
            .execute_batch(
                "PRAGMA user_version=2; ALTER TABLE clob_payout_evidence_v2 DROP COLUMN neg_risk;",
            )
            .unwrap();
        drop(cache);
        let config = disabled_config(&candidate);
        let spool = disabled_spool(&config);
        let path = dir.path().join("spool.json");
        let before = std::fs::read(&candidate).unwrap();
        let mut invalid = Vec::new();
        let value = serde_json::to_value(&spool).unwrap();
        let mut version = value.clone();
        version["version"] = json!(2);
        invalid.push(version);
        let mut flag = value.clone();
        flag["coverage_read"] = json!(true);
        invalid.push(flag);
        for (key, old) in value["settings"].as_object().unwrap() {
            let mut changed = value.clone();
            changed["settings"][key] = match old {
                serde_json::Value::String(s) => json!(format!("{s}changed")),
                serde_json::Value::Number(_) => json!(old.as_u64().unwrap() + 1),
                serde_json::Value::Array(_) => json!(["changed"]),
                serde_json::Value::Null => json!("changed"),
                _ => panic!("unexpected setting"),
            };
            invalid.push(changed);
        }
        for changed in invalid {
            std::fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
            assert_eq!(
                read_discovery_spool(&config, &path, None)
                    .unwrap_err()
                    .exit_code(),
                77,
                "{changed}"
            );
            assert_eq!(std::fs::read(&candidate).unwrap(), before);
        }
        std::fs::write(&path, b"not JSON").unwrap();
        assert_eq!(
            read_discovery_spool(&config, &path, None)
                .unwrap_err()
                .exit_code(),
            77
        );
        for path in [
            candidate.clone(),
            candidate.with_extension("db-wal"),
            candidate.with_extension("db-shm"),
            candidate.with_extension("db-journal"),
            crate::lock::lock_path_for(&candidate),
        ] {
            assert_eq!(
                read_discovery_spool(&config, &path, None)
                    .unwrap_err()
                    .exit_code(),
                77
            );
        }
        let mut bound = spool;
        bound.coverage_read = true;
        bound.prior_coverage_raw = Some("different".into());
        assert_eq!(
            bind_discovery_spool(&bound, &candidate)
                .unwrap_err()
                .exit_code(),
            77
        );
        assert_eq!(std::fs::read(&candidate).unwrap(), before);
    }

    #[tokio::test]
    async fn discovery_spool_not_before_crosses_midnight_with_fixed_clock() {
        let clock = std::cell::Cell::new(NOW - 1);
        let waits = std::cell::Cell::new(0);
        wait_not_before(
            NOW + 120,
            || clock.get(),
            async |duration| {
                waits.set(waits.get() + 1);
                clock.set(clock.get() + i64::try_from(duration.as_secs()).unwrap());
            },
        )
        .await;
        assert_eq!(clock.get(), NOW + 120);
        assert_eq!(waits.get(), 1);
        let mut calls = Vec::new();
        let config = fixture_config(Path::new("unused"));
        let data = fixture_collect(
            SOURCES[2],
            &config,
            None,
            "success",
            &mut calls,
            clock.get(),
        )
        .await
        .unwrap();
        if let CollectedSource::MarketParticipants(data) = data {
            assert!(data.now >= NOW + 120);
            assert_eq!(data.coverage.covered_through_unix, clock.get());
            // Exercise the exact hold owner used by candidate-targets, using this
            // collected coverage at the midnight print-tail boundary.
            let script = r#"import json, sqlite3, sys
sys.path.insert(0, 'scripts')
from rank_cycle_manifest import _participant_coverage_hold
coverage = json.loads(sys.argv[1])
window_end = int(sys.argv[2])
with sqlite3.connect(':memory:') as c:
    c.execute('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)')
    for tail in (119, 120, 121):
        coverage['covered_through_unix'] = window_end + tail
        c.execute('INSERT OR REPLACE INTO meta VALUES (?, ?)', ('participant_discovery_coverage', json.dumps(coverage)))
        hold = _participant_coverage_hold(c, coverage['scope'], coverage['covered_from_unix'], window_end, coverage['market_floor_unix'], 120)
        assert bool(hold) == (tail == 119), (tail, hold)
"#;
            let result = std::process::Command::new("python3")
                .args([
                    "-B",
                    "-c",
                    script,
                    &serde_json::to_string(&data.coverage).unwrap(),
                    &NOW.to_string(),
                ])
                .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        } else {
            panic!("missing participants");
        }
    }

    #[tokio::test]
    async fn discovery_spool_wal_values_bind_only_to_main_file_and_refuse_checkpointed_candidate() {
        for malformed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let fixed = dir.path().join("fixed.db");
            let candidate = dir.path().join("candidate.db");
            let mut cache = WalletCache::open(&fixed).unwrap();
            cache
                .set_participant_discovery_coverage(&coverage())
                .unwrap();
            cache
                .raw_conn_for_test()
                .execute_batch("PRAGMA user_version=2")
                .unwrap();
            if malformed {
                cache.raw_conn_for_test().execute_batch("UPDATE meta SET value='malformed' WHERE key='participant_discovery_coverage'").unwrap();
            }
            cache
                .raw_conn_for_test()
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA wal_autocheckpoint=0;")
                .unwrap();
            let main_raw = crate::cache_migration::discovery_immutable_coverage(&fixed).unwrap();
            cache
                .raw_conn_for_test()
                .execute(
                    "UPDATE meta SET value=?1 WHERE key='participant_discovery_coverage'",
                    [serde_json::to_string(&coverage()).unwrap() + " "],
                )
                .unwrap();
            let mut config = disabled_config(&fixed);
            config.participant_lookback_secs = 100;
            config.gamma_base_url = "invalid URL fails before network".into();
            let spool = dir.path().join("spool.json");
            collect_winner_discovery(&config, &spool, &fixed, 0, None)
                .await
                .unwrap();
            let collected: DiscoverySpool =
                serde_json::from_slice(&std::fs::read(&spool).unwrap()).unwrap();
            assert!(collected.coverage_read);
            assert_eq!(collected.prior_coverage_raw, main_raw);
            if malformed {
                assert!(matches!(
                    &collected.outcomes[2].result,
                    DiscoveryOutcome::Terminal { exit_code: 1, .. }
                ));
            }
            cache
                .raw_conn_for_test()
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            drop(cache);
            std::fs::copy(&fixed, &candidate).unwrap();
            let before = std::fs::read(&candidate).unwrap();
            assert_eq!(
                bind_discovery_spool(&collected, &candidate)
                    .unwrap_err()
                    .exit_code(),
                77
            );
            assert_eq!(std::fs::read(&candidate).unwrap(), before);
        }
    }

    #[test]
    fn discovery_spool_coverage_flag_outcome_and_count_validation() {
        let config = fixture_config(Path::new("unused"));
        let mut spool = DiscoverySpool {
            version: 1,
            settings: discovery_settings(&config, None),
            coverage_read: false,
            prior_coverage_raw: None,
            outcomes: vec![SourceOutcome {
                source: SOURCES[0],
                result: DiscoveryOutcome::Terminal {
                    exit_code: 1,
                    message: "zero entries".into(),
                },
            }],
        };
        spool.validate(&config, None).unwrap();
        spool.coverage_read = true;
        assert_eq!(spool.validate(&config, None).unwrap_err().exit_code(), 77);
        spool.coverage_read = false;
        spool.outcomes[0].result = DiscoveryOutcome::Disabled;
        assert!(spool.validate(&config, None).is_err());
        spool.outcomes[0].result = DiscoveryOutcome::Terminal {
            exit_code: 77,
            message: "bad code".into(),
        };
        assert!(spool.validate(&config, None).is_err());
        spool.outcomes[0].result = DiscoveryOutcome::SoftFailed {
            message: "only datadash".into(),
        };
        assert!(spool.validate(&config, None).is_err());
        spool.outcomes[0].result = DiscoveryOutcome::Collected {
            data: CollectedSource::Leaderboard(
                crate::leaderboard_discovery::CollectedLeaderboard {
                    wallets: vec![wallet(1), wallet(1)],
                    report: crate::leaderboard_discovery::LeaderboardDiscoveryReport {
                        slices_attempted: 80,
                        slices_fetched: 80,
                        raw_entries: 2,
                        unique_wallets: 2,
                        activated: 0,
                    },
                },
            ),
        };
        assert!(spool.validate(&config, None).is_err());
        let mut config = disabled_config(Path::new("unused"));
        let mut spool = disabled_spool(&config);
        config.participant_lookback_secs = 100;
        spool.settings = discovery_settings(&config, None);
        spool.outcomes[2].result = DiscoveryOutcome::Terminal {
            exit_code: 75,
            message: "failed".into(),
        };
        assert!(spool.validate(&config, None).is_err());
        spool.coverage_read = true;
        spool.validate(&config, None).unwrap();
        let mut bad = coverage();
        bad.covered_from_unix = bad.covered_through_unix;
        spool.outcomes[2].result = DiscoveryOutcome::Collected {
            data: CollectedSource::MarketParticipants(
                crate::wallet_discovery::CollectedParticipants {
                    wallets: vec![],
                    now: NOW,
                    coverage: bad,
                },
            ),
        };
        assert!(spool.validate(&config, None).is_err());
    }
}
