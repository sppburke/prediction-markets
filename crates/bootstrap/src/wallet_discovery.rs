//! Source-agnostic winner-discovery dispatch (issue #324).
//!
//! [`WalletDiscoverySource`] enumerates discovery sources.
//! The binary acquires the cache mutation lock centrally before its read-write
//! open (#544); discovery must not nest another acquisition.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use futures::{StreamExt, stream};
use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{
    RECONCILIATION_RATE_LIMIT_RETRY_SECS, ReqwestFetcher, decode_v2_page,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::datadash_discovery::{
    CollectedDatadash, ReqwestCohortFetcher, apply_datadash, collect_datadash,
};
use crate::error::BootstrapError;
use crate::leaderboard_discovery::{
    CollectedLeaderboard, LeaderboardFetcher, apply_leaderboard, collect_leaderboard,
};
use crate::market_scope::{
    MarketScope, ScopeFetcher, ScopePage, ScopeSeries, canonical_recurrences, list_market_series,
    recent_market_scope,
};
use crate::pile::{self, ActivationPolicy, SRC_MARKET_PARTICIPANT};

const PARTICIPANT_SOURCE_ID: &str = "polymarket-data-api-trades";
const PARTICIPANT_MIN_INTERVAL_MS: u64 = 100;
const PARTICIPANT_MARKET_CHUNK: usize = 20;
const PARTICIPANT_PAGE_LIMIT: usize = 1_000;
const PARTICIPANT_CONCURRENCY: usize = 8;
const PARTICIPANT_WINDOW_SECS: i64 = 1_065 * 86_400;

/// Normalized ranking scope shared by participant discovery and accepted sweep imports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantScope {
    pub recurrences: Vec<String>,
    pub series: Option<Vec<ScopeSeries>>,
}

impl ParticipantScope {
    pub fn new(
        recurrences: &[String],
        pinned: Option<&[ScopeSeries]>,
    ) -> Result<Self, BootstrapError> {
        let mut series = pinned.map(<[ScopeSeries]>::to_vec);
        if let Some(series) = &mut series {
            series.sort_by(|a, b| a.id.cmp(&b.id));
            let text = series
                .iter()
                .map(|s| format!("{}:{}", s.id, s.slug.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>()
                .join(",");
            crate::market_scope::parse_pinned_series(&text)?;
        }
        Ok(Self {
            recurrences: canonical_recurrences(recurrences)?,
            series,
        })
    }
}

/// Completed participant reads over every scope market ending at or after the floor,
/// for the half-open interval [covered_from_unix, covered_through_unix). Stored as
/// one JSON object in `meta.participant_discovery_coverage`, including on schema two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantDiscoveryCoverage {
    pub version: u32,
    pub scope: ParticipantScope,
    pub market_floor_unix: i64,
    pub covered_from_unix: i64,
    pub covered_through_unix: i64,
}

impl ParticipantDiscoveryCoverage {
    pub(crate) fn validate(&self) -> Result<(), BootstrapError> {
        if self.version != 1
            || self.scope
                != ParticipantScope::new(&self.scope.recurrences, self.scope.series.as_deref())?
            || self.covered_from_unix < 0
            || self.covered_from_unix >= self.covered_through_unix
        {
            return Err(BootstrapError::Invalid {
                message: "invalid participant discovery coverage".to_owned(),
            });
        }
        Ok(())
    }

    fn merge_import(&self, prior: Option<&Self>) -> Result<Self, BootstrapError> {
        self.validate()?;
        if let Some(covered) = prior
            .filter(|prior| prior.scope.recurrences == self.scope.recurrences)
            .and_then(|prior| prior.scope.series.as_ref())
            && covered.iter().any(|series| {
                !self
                    .scope
                    .series
                    .as_ref()
                    .is_some_and(|imported| imported.contains(series))
            })
        {
            return Err(BootstrapError::Invalid {
                message: "participant import must retain every covered id:slug".to_owned(),
            });
        }
        let Some(prior) = prior.filter(|prior| prior.scope == self.scope) else {
            return Ok(self.clone());
        };
        if self.market_floor_unix < prior.market_floor_unix {
            if self.covered_from_unix <= prior.covered_from_unix
                && self.covered_through_unix >= prior.covered_through_unix
            {
                return Ok(self.clone());
            }
            return Err(BootstrapError::Invalid {
                message: format!(
                    "earlier market floor requires coverage of [{}, {}); supplied [{}, {})",
                    prior.covered_from_unix,
                    prior.covered_through_unix,
                    self.covered_from_unix,
                    self.covered_through_unix
                ),
            });
        }
        if self.covered_from_unix > prior.covered_through_unix
            || self.covered_through_unix < prior.covered_from_unix
        {
            return Err(BootstrapError::Invalid {
                message: format!(
                    "participant coverage missing bridge [{}, {})",
                    self.covered_through_unix.min(prior.covered_through_unix),
                    self.covered_from_unix.max(prior.covered_from_unix)
                ),
            });
        }
        Ok(Self {
            covered_from_unix: self.covered_from_unix.min(prior.covered_from_unix),
            covered_through_unix: self.covered_through_unix.max(prior.covered_through_unix),
            market_floor_unix: self.market_floor_unix.max(prior.market_floor_unix),
            ..self.clone()
        })
    }
}

/// Validate a sweep file and all coverage inputs before changing either wallets or
/// coverage. The caller holds the cache mutation lock through both commits.
pub fn import_participants(
    cache: &mut WalletCache,
    file: &std::path::Path,
    coverage: ParticipantDiscoveryCoverage,
    now: i64,
) -> Result<usize, BootstrapError> {
    coverage.validate()?;
    if coverage.covered_through_unix > now {
        return Err(BootstrapError::Invalid {
            message: "participant coverage must not end in the future".to_owned(),
        });
    }
    let bytes = std::fs::read(file)?;
    let text = std::str::from_utf8(&bytes).map_err(|error| BootstrapError::Invalid {
        message: format!("participants-import: {error}"),
    })?;
    let wallets = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            WalletAddress::from_hex(line.trim())
                .map(|wallet| wallet.to_string())
                .map_err(|error| BootstrapError::Invalid {
                    message: format!("participants-import: invalid wallet {line:?}: {error}"),
                })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let merged = coverage.merge_import(cache.participant_discovery_coverage()?.as_ref())?;
    merged.validate()?;
    upsert_participants(cache, wallets.iter().cloned())?;
    cache.set_participant_discovery_coverage(&merged)?;
    tracing::info!(file_sha256 = %format!("{:x}", Sha256::digest(&bytes)), wallet_count = wallets.len(), "participants-import: complete");
    Ok(wallets.len())
}

fn upsert_participants(
    cache: &mut WalletCache,
    wallets: impl IntoIterator<Item = String>,
) -> Result<(), BootstrapError> {
    let rows: Vec<_> = wallets
        .into_iter()
        .map(|hex| (hex, SRC_MARKET_PARTICIPANT, false, None, None, None, 0))
        .collect();
    cache.upsert_wallets_bulk(&rows)
}

/// The supported wallet-discovery sources for `winner-discovery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletDiscoverySource {
    Leaderboard,
    Datadash,
    MarketParticipants,
}

/// Aggregate per-source counts returned by [`run_source_discovery`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceDiscoveryResult {
    pub unique_wallets: usize,
    pub activated: usize,
    pub status: &'static str,
}

/// Run discovery for `source`. Production callers already hold the central
/// cache mutation lock for the lifetime of `cache` (#544).
pub async fn run_source_discovery(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    run_source_discovery_with_policy(source, config, cache, ActivationPolicy::Immediate, None).await
}

/// Source discovery with an explicit activation policy. The rank-and-push
/// pipeline uses `Deferred`; standalone callers retain `Immediate`.
pub async fn run_source_discovery_with_policy(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    activation_policy: ActivationPolicy,
    market_floor: Option<i64>,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let collected = collect_source_discovery(source, config, market_floor, now, || {
        pe_event_log::run_blocking(|| cache.participant_discovery_coverage())
    })
    .await?;
    let disabled = source == WalletDiscoverySource::MarketParticipants
        && matches!(collected, CollectedSource::Disabled);
    let mut result =
        pe_event_log::run_blocking(|| apply_source_discovery(cache, activation_policy, collected))?;
    if disabled {
        result.status = "disabled";
    }
    Ok(result)
}

/// Collected data has no cache handle; application owns every durable change.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "source",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum CollectedSource {
    Leaderboard(CollectedLeaderboard),
    Datadash(CollectedDatadash),
    MarketParticipants(CollectedParticipants),
    Disabled,
}

pub(crate) async fn collect_source_discovery(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    market_floor: Option<i64>,
    now: i64,
    prior: impl FnOnce() -> Result<Option<ParticipantDiscoveryCoverage>, BootstrapError>,
) -> Result<CollectedSource, BootstrapError> {
    match source {
        WalletDiscoverySource::Leaderboard => {
            if config.leaderboard_top_n == 0 {
                return Ok(CollectedSource::Disabled);
            }
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
            let collected = collect_leaderboard(
                &fetcher,
                &config.leaderboard_categories,
                config.leaderboard_top_n,
            )
            .await?;
            Ok(CollectedSource::Leaderboard(collected))
        }
        WalletDiscoverySource::Datadash => {
            // Datadash is on by default; an empty/unset URL disables it.
            let Some(base_url) = config.datadash_api_url.as_deref() else {
                tracing::debug!("wallet_discovery: Datadash skipped — datadash_api_url not set");
                return Ok(CollectedSource::Disabled);
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
            let collected = collect_datadash(
                &fetcher,
                &config.datadash_exclude_ids,
                &config.datadash_exclude_titles,
                config.datadash_max_cohort_wallets,
            )
            .await?;
            Ok(CollectedSource::Datadash(collected))
        }
        WalletDiscoverySource::MarketParticipants => {
            collect_participants(
                config,
                prior,
                now,
                participant_fetcher,
                || list_market_series(config, &config.market_recurrences),
                |lo, subset| async move {
                    recent_market_scope(
                        config,
                        market_floor.unwrap_or(lo),
                        &config.market_recurrences,
                        Some(&subset),
                    )
                    .await
                },
            )
            .await
        }
    }
}

pub(crate) fn apply_source_discovery(
    cache: &mut WalletCache,
    policy: ActivationPolicy,
    collected: CollectedSource,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    match collected {
        CollectedSource::Leaderboard(data) => {
            let r = apply_leaderboard(cache, policy, data)?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
                ..SourceDiscoveryResult::default()
            })
        }
        CollectedSource::Datadash(data) => {
            let r = apply_datadash(cache, policy, data)?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
                ..SourceDiscoveryResult::default()
            })
        }
        CollectedSource::MarketParticipants(data) => {
            let count = data.wallets.len();
            upsert_participants(cache, data.wallets)?;
            cache.set_participant_discovery_coverage(&data.coverage)?;
            let activated = pile::apply_activation_policy(cache, policy)?;
            Ok(SourceDiscoveryResult {
                unique_wallets: count,
                activated,
                status: "ok",
            })
        }
        CollectedSource::Disabled => Ok(SourceDiscoveryResult::default()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CollectedParticipants {
    pub wallets: Vec<String>,
    pub now: i64,
    pub coverage: ParticipantDiscoveryCoverage,
}

struct ParticipantRecord {
    source_id: &'static str,
    schema_version: u32,
    parser_version: u32,
    window_start_unix: Option<i64>,
    window_end_unix: i64,
    market_scope: Option<MarketScope>,
    trades_pages: Vec<ScopePage>,
    chunks: Vec<ParticipantChunk>,
    wallet_count: usize,
    status: &'static str,
}

#[cfg(test)]
async fn run_participant_discovery<F, L, S, Listed, Swept>(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    policy: ActivationPolicy,
    now: i64,
    make_fetcher: impl FnOnce() -> Result<F, BootstrapError>,
    list: L,
    sweep: S,
) -> Result<SourceDiscoveryResult, BootstrapError>
where
    F: ScopeFetcher + Sync,
    L: FnOnce() -> Listed,
    Listed: Future<Output = Result<(Vec<ScopeSeries>, Vec<ScopePage>), BootstrapError>>,
    S: FnMut(i64, Vec<ScopeSeries>) -> Swept,
    Swept: Future<Output = Result<MarketScope, BootstrapError>>,
{
    let collected = collect_participants(
        config,
        || cache.participant_discovery_coverage(),
        now,
        make_fetcher,
        list,
        sweep,
    )
    .await?;
    let disabled = matches!(collected, CollectedSource::Disabled);
    let mut result = apply_source_discovery(cache, policy, collected)?;
    if disabled {
        result.status = "disabled";
    }
    Ok(result)
}

pub(crate) async fn collect_participants<F, L, S, Listed, Swept>(
    config: &BootstrapConfig,
    read_prior: impl FnOnce() -> Result<Option<ParticipantDiscoveryCoverage>, BootstrapError>,
    now: i64,
    make_fetcher: impl FnOnce() -> Result<F, BootstrapError>,
    list: L,
    mut sweep: S,
) -> Result<CollectedSource, BootstrapError>
where
    F: ScopeFetcher + Sync,
    L: FnOnce() -> Listed,
    Listed: Future<Output = Result<(Vec<ScopeSeries>, Vec<ScopePage>), BootstrapError>>,
    S: FnMut(i64, Vec<ScopeSeries>) -> Swept,
    Swept: Future<Output = Result<MarketScope, BootstrapError>>,
{
    let recurrences = canonical_recurrences(&config.market_recurrences)?;
    let prior = if config.participant_lookback_secs == 0 {
        None
    } else {
        read_prior()?
    };
    let matching = prior
        .as_ref()
        .filter(|prior| prior.scope.recurrences == recurrences && prior.scope.series.is_some());
    let lo = i64::try_from(config.participant_lookback_secs)
        .ok()
        .and_then(|lookback| {
            matching
                .map_or(now, |prior| prior.covered_through_unix)
                .checked_sub(lookback)
        });
    let mut record = ParticipantRecord {
        source_id: PARTICIPANT_SOURCE_ID,
        schema_version: 2,
        parser_version: 2,
        window_start_unix: lo,
        window_end_unix: now,
        market_scope: None,
        trades_pages: Vec::new(),
        chunks: Vec::new(),
        wallet_count: 0,
        status: "disabled",
    };
    let mut normalized_scope = None;
    let collected = async {
        if config.participant_lookback_secs == 0 {
            return Ok(Vec::new());
        }
        let lo = lo
            .filter(|lo| valid_participant_window(*lo, now))
            .ok_or("failed:window")?;
        let (listed, listing_pages) = list()
            .await
            .map_err(|error| participant_failure("failed:gamma", &error))?;
        let covered = matching
            .and_then(|prior| prior.scope.series.clone())
            .unwrap_or_default();
        let additions: Vec<_> = listed
            .into_iter()
            .filter(|series| !covered.iter().any(|old| old.id == series.id))
            .collect();
        let mut series = covered.clone();
        series.extend(additions.iter().cloned());
        normalized_scope = Some(
            ParticipantScope::new(&recurrences, Some(&series))
                .map_err(|error| participant_failure("failed:gamma", &error))?,
        );
        let addition_lo = matching.map_or(lo, |prior| lo.min(prior.covered_from_unix));
        if !additions.is_empty() && !valid_participant_window(addition_lo, now) {
            return Err("failed:window");
        }
        if series.is_empty() {
            return Err("failed:gamma");
        }
        let fetcher =
            make_fetcher().map_err(|error| participant_failure("failed:client", &error))?;
        let mut wallets = BTreeSet::new();
        for (start, subset) in [(lo, covered), (addition_lo, additions)] {
            if subset.is_empty() {
                continue;
            }
            let market_scope = sweep(start, subset)
                .await
                .map_err(|error| participant_failure("failed:gamma", &error))?;
            // Keep both subset sweeps in the summary's union, with all Gamma provenance.
            if let Some(combined) = &mut record.market_scope {
                combined.series.extend(market_scope.series.iter().cloned());
                combined.pages.extend(market_scope.pages.iter().cloned());
                combined
                    .condition_ids
                    .extend(market_scope.condition_ids.iter().cloned());
                combined.condition_ids.sort();
                combined.condition_ids.dedup();
                combined.count =
                    u64::try_from(combined.condition_ids.len()).map_err(|_| "failed:page-count")?;
                combined.digest = format!(
                    "{:x}",
                    Sha256::digest(combined.condition_ids.join("\n").as_bytes())
                );
                combined.end_date_min_unix = combined
                    .end_date_min_unix
                    .max(market_scope.end_date_min_unix);
                combined.completed_at_unix = market_scope.completed_at_unix;
            } else {
                let mut combined = market_scope.clone();
                combined.pages.splice(0..0, listing_pages.iter().cloned());
                record.market_scope = Some(combined);
            }
            wallets.extend(
                collect_participant_trades(
                    &fetcher,
                    &config.polymarket_base_url,
                    &market_scope,
                    start,
                    now,
                    &mut record.trades_pages,
                    &mut record.chunks,
                )
                .await?,
            );
        }
        if let Some(scope) = &mut record.market_scope {
            scope.series.sort_by(|a, b| a.id.cmp(&b.id));
            for (ordinal, page) in scope.pages.iter_mut().enumerate() {
                page.ordinal = u64::try_from(ordinal).map_err(|_| "failed:page-count")?;
            }
        }
        record.status = "ok";
        Ok(wallets.into_iter().collect())
    }
    .await;
    let wallets = match collected {
        Ok(wallets) => wallets,
        Err(status) => {
            tracing::warn!(status, "winner_discovery: participant collection stopped");
            record.status = status;
            Vec::new()
        }
    };
    record.wallet_count = wallets.len();
    // One summary line plus one line per page and chunk: the whole record measured 179 KB, past journald's
    // 48 KB LineMax. Each trades page URL lists its condition ids; the scope digest covers them all.
    let scope = record.market_scope.as_ref();
    let summary = serde_json::json!({
        "source_id": record.source_id,
        "schema_version": record.schema_version,
        "parser_version": record.parser_version,
        "window_start_unix": record.window_start_unix,
        "window_end_unix": record.window_end_unix,
        "status": record.status,
        "wallet_count": record.wallet_count,
        "scope": scope.map(|s| serde_json::json!({
            "version": s.version,
            "source_id": s.source_id,
            "schema_version": s.schema_version,
            "parser_version": s.parser_version,
            "recurrence": s.recurrence,
            "end_date_min_unix": s.end_date_min_unix,
            "started_at_unix": s.started_at_unix,
            "completed_at_unix": s.completed_at_unix,
            "series": s.series,
            "count": s.count,
            "digest": s.digest,
        })),
        "gamma_pages": scope.map_or(0, |s| s.pages.len()),
        "trades_pages": record.trades_pages.len(),
        "chunks": record.chunks.len(),
    });
    tracing::info!(record = %summary, "winner_discovery: participants");
    for chunk in &record.chunks {
        tracing::info!(chunk = %serde_json::to_string(chunk)?, "winner_discovery: participant chunk");
    }
    for page in scope
        .into_iter()
        .flat_map(|s| &s.pages)
        .chain(&record.trades_pages)
    {
        tracing::info!(page = %serde_json::to_string(page)?, "winner_discovery: participant page");
    }
    if record.status == "disabled" {
        return Ok(CollectedSource::Disabled);
    }
    if record.status != "ok" {
        return Err(BootstrapError::TransientSource {
            source_name: PARTICIPANT_SOURCE_ID,
            message: record.status.to_owned(),
        });
    }
    let mut coverage = ParticipantDiscoveryCoverage {
        version: 1,
        scope: normalized_scope.ok_or(BootstrapError::Internal)?,
        market_floor_unix: record
            .market_scope
            .as_ref()
            .ok_or(BootstrapError::Internal)?
            .end_date_min_unix,
        covered_from_unix: lo.ok_or(BootstrapError::Internal)?,
        covered_through_unix: now,
    };
    if let Some(prior) = matching {
        coverage.covered_from_unix = coverage.covered_from_unix.min(prior.covered_from_unix);
        coverage.covered_through_unix = coverage
            .covered_through_unix
            .max(prior.covered_through_unix);
        coverage.market_floor_unix = coverage.market_floor_unix.max(prior.market_floor_unix);
    }
    coverage.validate()?;
    Ok(CollectedSource::MarketParticipants(CollectedParticipants {
        wallets,
        now,
        coverage,
    }))
}

fn participant_fetcher() -> Result<ReqwestFetcher, BootstrapError> {
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    // A venue `Retry-After: 1` answer to a `/trades` page is waited out in-line rather than
    // ending the source for the whole cycle (#800).
    Ok(ReqwestFetcher::new(client)
        .with_min_interval_ms(PARTICIPANT_MIN_INTERVAL_MS)
        .with_rate_limit_retry_max_secs(RECONCILIATION_RATE_LIMIT_RETRY_SECS))
}

fn participant_failure(reason: &'static str, error: &impl std::fmt::Display) -> &'static str {
    tracing::warn!(error = %error, status = reason, "winner_discovery: participant source failed");
    reason
}

pub(crate) fn valid_participant_window(lo: i64, now: i64) -> bool {
    lo >= 0 && lo < now && lo >= now.saturating_sub(PARTICIPANT_WINDOW_SECS)
}

#[derive(Serialize)]
struct ParticipantChunk {
    condition_ids: Vec<String>,
    lo: i64,
    newest_timestamp: Option<i64>,
    oldest_timestamp: Option<i64>,
    page_count: usize,
    stop_reason: Option<&'static str>,
}

async fn collect_participant_trades<F: ScopeFetcher + Sync>(
    fetcher: &F,
    base_url: &str,
    scope: &MarketScope,
    lo: i64,
    now: i64,
    pages: &mut Vec<ScopePage>,
    chunks: &mut Vec<ParticipantChunk>,
) -> Result<Vec<String>, &'static str> {
    if !valid_participant_window(lo, now) {
        return Err("failed:window");
    }
    let mut reads = stream::iter(
        scope
            .condition_ids
            .chunks(PARTICIPANT_MARKET_CHUNK)
            .map(|ids| collect_participant_chunk(fetcher, base_url, ids, lo)),
    )
    .buffered(PARTICIPANT_CONCURRENCY);
    let mut wallets = BTreeSet::new();
    while let Some((result, chunk_pages, chunk)) = reads.next().await {
        for mut page in chunk_pages {
            page.ordinal = u64::try_from(pages.len()).map_err(|_| "failed:page-count")?;
            pages.push(page);
        }
        chunks.push(chunk);
        wallets.extend(result?);
    }
    Ok(wallets.into_iter().collect())
}

async fn collect_participant_chunk<F: ScopeFetcher>(
    fetcher: &F,
    base_url: &str,
    ids: &[String],
    lo: i64,
) -> (
    Result<BTreeSet<String>, &'static str>,
    Vec<ScopePage>,
    ParticipantChunk,
) {
    let mut pages = Vec::new();
    let mut chunk = ParticipantChunk {
        condition_ids: ids.to_vec(),
        lo,
        newest_timestamp: None,
        oldest_timestamp: None,
        page_count: 0,
        stop_reason: None,
    };
    let result = async {
        let mut wallets = BTreeSet::new();
        let mut cursor: Option<String> = None;
        let mut seen = BTreeSet::new();
        let mut decreased = false;
        loop {
            let mut url = reqwest::Url::parse(&format!("{base_url}/v2/trades"))
                .map_err(|error| participant_failure("failed:trades-url", &error))?;
            url.query_pairs_mut()
                .append_pair("condition", &ids.join(","))
                .append_pair("taker_only", "false")
                .append_pair("filter_amount", "0.0000001");
            if let Some(cursor) = &cursor {
                url.query_pairs_mut().append_pair("cursor", cursor);
            } else {
                url.query_pairs_mut()
                    .append_pair("limit", &PARTICIPANT_PAGE_LIMIT.to_string());
            }
            let url = url.to_string();
            let page = fetcher
                .fetch(&url, "trades", PARTICIPANT_SOURCE_ID)
                .await
                .map_err(|error| participant_failure("failed:trades", &error))?;
            let parsed = decode_v2_page(&page.body);
            pages.push(ScopePage {
                ordinal: u64::try_from(pages.len()).map_err(|_| "failed:page-count")?,
                kind: "trades",
                series_id: None,
                url,
                request_cursor: cursor.clone(),
                returned_next_cursor: parsed
                    .as_ref()
                    .ok()
                    .and_then(|page| page.pagination.next_cursor.clone()),
                raw_sha256: format!("{:x}", Sha256::digest(&page.body)),
                rows: u64::try_from(parsed.as_ref().map_or(0, |page| page.data.len()))
                    .map_err(|_| "failed:page-count")?,
                observed_at_unix: page.observed_at_unix,
                received_at_unix: page.received_at_unix,
                source_at_unix: page.source_at_unix,
            });
            chunk.page_count += 1;
            let parsed =
                parsed.map_err(|error| participant_failure("failed:trades-json", &error))?;
            if parsed.pagination.has_more && parsed.data.is_empty() {
                return Err("incomplete:empty-continuation");
            }
            if let Some(next) = &parsed.pagination.next_cursor
                && !seen.insert(next.clone())
            {
                return Err("incomplete:repeated-cursor");
            }
            for raw in parsed.data {
                let row: Value = serde_json::from_str(raw.get())
                    .map_err(|error| participant_failure("failed:trades-json", &error))?;
                let timestamp = row
                    .get("timestamp")
                    .and_then(Value::as_i64)
                    .ok_or("incomplete:timestamp")?;
                if let Some(previous) = chunk.oldest_timestamp {
                    if timestamp > previous {
                        return Err("incomplete:timestamp-order");
                    }
                    decreased |= timestamp < previous;
                }
                chunk.newest_timestamp.get_or_insert(timestamp);
                chunk.oldest_timestamp = Some(timestamp);
                let wallet = row
                    .get("proxy_wallet")
                    .and_then(Value::as_str)
                    .ok_or("incomplete:proxy-wallet")?
                    .to_ascii_lowercase();
                if wallet.len() != 42
                    || !wallet.starts_with("0x")
                    || !wallet[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err("incomplete:proxy-wallet");
                }
                if timestamp >= lo {
                    wallets.insert(wallet);
                }
            }
            if !parsed.pagination.has_more {
                chunk.stop_reason = Some("terminal");
                break;
            }
            if decreased && chunk.oldest_timestamp.is_some_and(|oldest| oldest < lo) {
                chunk.stop_reason = Some("threshold");
                break;
            }
            cursor = parsed.pagination.next_cursor;
        }
        Ok(wallets)
    }
    .await;
    (result, pages, chunk)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use pe_source_core::SourceError;
    use serde_json::json;

    use crate::market_scope::{FetchedPage, ScopeSeries};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn leaderboard_zero_skips_and_participants_still_run() {
        let (_dir, mut cache) = tmp_cache();
        let config = BootstrapConfig {
            leaderboard_top_n: 0,
            leaderboard_base_url: Some("invalid URL must never be fetched".into()),
            participant_lookback_secs: 5,
            ..BootstrapConfig::default()
        };
        let result = run_source_discovery(WalletDiscoverySource::Leaderboard, &config, &mut cache)
            .await
            .unwrap();
        assert_eq!((result.unique_wallets, result.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        let fetcher = fetcher(|_: &str| {
            Ok(envelope(
                json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                None,
            ))
        });
        let participants = run_participant_discovery(
            &config,
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            listing,
            |_, _| async { Ok(scope(1)) },
        )
        .await
        .unwrap();
        assert_eq!(participants.status, "ok");
        assert_eq!(
            (participants.unique_wallets, participants.activated),
            (1, 1)
        );
    }

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

    const BASE: &str = "http://trades.test";
    const NOW: i64 = 105;

    struct TestFetcher<G> {
        serve: G,
        urls: Arc<Mutex<Vec<String>>>,
    }

    impl<G: Fn(&str) -> Result<Value, SourceError> + Sync> ScopeFetcher for TestFetcher<G> {
        async fn fetch(
            &self,
            url: &str,
            endpoint_kind: &'static str,
            source_id: &'static str,
        ) -> Result<FetchedPage, SourceError> {
            assert_eq!(endpoint_kind, "trades");
            assert_eq!(source_id, PARTICIPANT_SOURCE_ID);
            self.urls.lock().unwrap().push(url.to_owned());
            Ok(FetchedPage {
                body: serde_json::to_vec(&(self.serve)(url)?).unwrap(),
                observed_at_unix: 200,
                received_at_unix: 201,
                source_at_unix: Some(199),
            })
        }
    }

    fn fetcher<G>(serve: G) -> TestFetcher<G> {
        TestFetcher {
            serve,
            urls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn query(url: &str) -> BTreeMap<String, String> {
        reqwest::Url::parse(url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect()
    }

    fn wallet(n: u8) -> String {
        format!("0x{n:040x}")
    }

    fn scope(markets: usize) -> MarketScope {
        MarketScope {
            version: 1,
            source_id: "polymarket-gamma",
            schema_version: 1,
            parser_version: 1,
            recurrence: "15m".into(),
            end_date_min_unix: 100,
            started_at_unix: NOW,
            completed_at_unix: NOW,
            series: vec![ScopeSeries {
                id: "2".into(),
                slug: Some("two".into()),
            }],
            pages: Vec::new(),
            condition_ids: (0..markets).map(|n| format!("0x{n:064x}")).collect(),
            count: u64::try_from(markets).unwrap(),
            digest: "fixture".into(),
        }
    }

    fn config(lookback: u64) -> BootstrapConfig {
        BootstrapConfig {
            polymarket_base_url: BASE.into(),
            participant_lookback_secs: lookback,
            ..BootstrapConfig::default()
        }
    }

    fn envelope(rows: Value, next: Option<&str>) -> Value {
        json!({"data": rows, "pagination": {"limit": 1000, "offset": 0,
            "has_more": next.is_some(), "next_cursor": next}})
    }

    async fn listing() -> Result<(Vec<ScopeSeries>, Vec<ScopePage>), BootstrapError> {
        Ok((scope(1).series, Vec::new()))
    }

    fn tmp_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    #[tokio::test]
    async fn participants_chunk_at_twenty_and_repeat_filters_with_cursor_evidence() {
        for markets in [20, 21] {
            let first = envelope(
                json!([
                    {"proxy_wallet": wallet(0xbb).to_uppercase(), "timestamp": 104, "size": "0.000001"},
                    {"proxy_wallet": wallet(0xaa), "timestamp": 104}
                ]),
                Some("next /+?"),
            );
            let terminal = envelope(
                json!([
                    {"proxy_wallet": wallet(0xbb), "timestamp": 100}
                ]),
                None,
            );
            let fetcher = fetcher(|url: &str| {
                Ok(if query(url).contains_key("cursor") {
                    terminal.clone()
                } else {
                    first.clone()
                })
            });
            let mut pages = Vec::new();
            let mut chunks = Vec::new();
            let wallets = collect_participant_trades(
                &fetcher,
                BASE,
                &scope(markets),
                100,
                NOW,
                &mut pages,
                &mut chunks,
            )
            .await
            .unwrap();
            assert_eq!(wallets, [wallet(0xaa), wallet(0xbb)]);
            assert_eq!(chunks.len(), markets.div_ceil(20));
            let ids: Vec<_> = pages
                .chunks(2)
                .flat_map(|pair| {
                    let first_query = query(&pair[0].url);
                    assert_eq!(
                        reqwest::Url::parse(&pair[0].url).unwrap().path(),
                        "/v2/trades"
                    );
                    assert_eq!(
                        first_query["condition"].split(',').count(),
                        if markets == 21 && pair[0].ordinal == 2 {
                            1
                        } else {
                            20
                        }
                    );
                    assert_eq!(first_query["limit"], "1000");
                    let mut next_query = query(&pair[1].url);
                    assert_eq!(next_query.remove("cursor").as_deref(), Some("next /+?"));
                    let mut expected = first_query.clone();
                    expected.remove("limit");
                    assert_eq!(next_query, expected);
                    assert_eq!(first_query["taker_only"], "false");
                    assert_eq!(first_query["filter_amount"], "0.0000001");
                    assert!(
                        !first_query.contains_key("start")
                            && !first_query.contains_key("end")
                            && !first_query.contains_key("offset")
                    );
                    assert_eq!(pair[0].request_cursor, None);
                    assert_eq!(pair[0].returned_next_cursor.as_deref(), Some("next /+?"));
                    assert_eq!(pair[1].request_cursor.as_deref(), Some("next /+?"));
                    assert_eq!(pair[1].returned_next_cursor, None);
                    assert_eq!(
                        pair[0].raw_sha256,
                        format!("{:x}", Sha256::digest(serde_json::to_vec(&first).unwrap()))
                    );
                    first_query["condition"]
                        .split(',')
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(ids, scope(markets).condition_ids);
            for (i, page) in pages.iter().enumerate() {
                assert_eq!(page.ordinal, u64::try_from(i).unwrap());
                assert_eq!(
                    (
                        page.observed_at_unix,
                        page.received_at_unix,
                        page.source_at_unix
                    ),
                    (200, 201, Some(199))
                );
            }
        }
    }

    #[tokio::test]
    async fn participants_threshold_requires_a_strict_decrease_and_filters_old_rows() {
        for equal_first_page in [false, true] {
            let fetcher = fetcher(|url: &str| {
                let cursor = query(url).get("cursor").cloned();
                Ok(match cursor.as_deref() {
                    None if equal_first_page => envelope(
                        json!([
                            {"proxy_wallet": wallet(1), "timestamp": 99},
                            {"proxy_wallet": wallet(2), "timestamp": 99}
                        ]),
                        Some("second"),
                    ),
                    None => envelope(
                        json!([
                            {"proxy_wallet": wallet(1), "timestamp": 101},
                            {"proxy_wallet": wallet(2), "timestamp": 100},
                            {"proxy_wallet": wallet(3), "timestamp": 99}
                        ]),
                        Some("never-read"),
                    ),
                    Some("second") => envelope(
                        json!([
                            {"proxy_wallet": wallet(3), "timestamp": 99}
                        ]),
                        None,
                    ),
                    _ => panic!("unexpected page {url}"),
                })
            });
            let mut pages = Vec::new();
            let mut chunks = Vec::new();
            let wallets = collect_participant_trades(
                &fetcher,
                BASE,
                &scope(1),
                100,
                NOW,
                &mut pages,
                &mut chunks,
            )
            .await
            .unwrap();
            if equal_first_page {
                assert!(wallets.is_empty());
                assert_eq!(pages.len(), 2, "equal timestamps cannot prove direction");
                assert_eq!(chunks[0].stop_reason, Some("terminal"));
                assert_eq!(
                    (chunks[0].newest_timestamp, chunks[0].oldest_timestamp),
                    (Some(99), Some(99))
                );
            } else {
                assert_eq!(wallets, [wallet(1), wallet(2)]);
                assert_eq!(pages.len(), 1);
                assert_eq!(chunks[0].stop_reason, Some("threshold"));
                assert_eq!(
                    (chunks[0].newest_timestamp, chunks[0].oldest_timestamp),
                    (Some(101), Some(99))
                );
            }
            assert_eq!(chunks[0].lo, 100);
            assert_eq!(chunks[0].page_count, pages.len());
        }
    }

    #[tokio::test]
    async fn participants_bad_cursor_walks_leave_wallets_and_coverage_unchanged() {
        for failure in [
            "within-page",
            "across-pages",
            "cursor",
            "empty",
            "wallet",
            "envelope",
            "fetch",
        ] {
            let (_dir, mut cache) = tmp_cache();
            let prior = coverage(10, 100, 0);
            cache.set_participant_discovery_coverage(&prior).unwrap();
            let fetcher = fetcher(|url: &str| {
                if !query(url).contains_key("cursor") {
                    return Ok(envelope(
                        json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                        Some("second"),
                    ));
                }
                Ok(match failure {
                    "within-page" => envelope(
                        json!([
                            {"proxy_wallet": wallet(2), "timestamp": 99},
                            {"proxy_wallet": wallet(3), "timestamp": 100}
                        ]),
                        None,
                    ),
                    "across-pages" => {
                        envelope(json!([{"proxy_wallet": wallet(2), "timestamp": 101}]), None)
                    }
                    "cursor" => envelope(
                        json!([{"proxy_wallet": wallet(2), "timestamp": 100}]),
                        Some("second"),
                    ),
                    "empty" => envelope(json!([]), Some("third")),
                    "wallet" => envelope(json!([{"proxy_wallet": "bad", "timestamp": 100}]), None),
                    "envelope" => {
                        json!({"data": [], "pagination": {"limit":1000, "offset":0, "has_more":true, "next_cursor":null}})
                    }
                    "fetch" => {
                        return Err(SourceError::Fatal {
                            message: "fixture failed page".into(),
                        });
                    }
                    _ => unreachable!(),
                })
            });
            let error = run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Deferred,
                NOW,
                || Ok(fetcher),
                listing,
                |_, _| async { Ok(scope(1)) },
            )
            .await
            .unwrap_err();
            assert_eq!(error.exit_code(), 75, "{failure}: {error}");
            assert_eq!(cache.wallet_pile_size().unwrap(), 0, "{failure}");
            assert_eq!(
                cache.participant_discovery_coverage().unwrap(),
                Some(prior),
                "{failure}"
            );
        }
    }

    #[tokio::test]
    async fn participants_window_guard_fails_before_listing_and_keeps_coverage() {
        let (_dir, mut cache) = tmp_cache();
        let prior = coverage(10, 100, 0);
        cache.set_participant_discovery_coverage(&prior).unwrap();
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            100 + PARTICIPANT_WINDOW_SECS,
            || Ok(fetcher(|_: &str| panic!("guarded trades"))),
            || async { panic!("guarded listing") },
            |_, _| async { panic!("guarded sweep") },
        )
        .await
        .unwrap_err();
        assert_eq!(error.exit_code(), 75);
        assert!(error.to_string().contains("failed:window"));
        assert_eq!(cache.participant_discovery_coverage().unwrap(), Some(prior));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        assert!(valid_participant_window(100, 100 + PARTICIPANT_WINDOW_SECS));
    }

    #[tokio::test(start_paused = true)]
    async fn participants_read_eight_chunks_concurrently_but_record_in_chunk_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct ConcurrentFetcher {
            active: AtomicUsize,
            peak: AtomicUsize,
        }
        impl ScopeFetcher for ConcurrentFetcher {
            async fn fetch(
                &self,
                url: &str,
                _: &'static str,
                _: &'static str,
            ) -> Result<FetchedPage, SourceError> {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                let first = query(url)["condition"]
                    .split(',')
                    .next()
                    .unwrap()
                    .to_owned();
                tokio::time::sleep(Duration::from_millis(
                    if first == scope(1).condition_ids[0] {
                        100
                    } else {
                        1
                    },
                ))
                .await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(FetchedPage {
                    body: serde_json::to_vec(&envelope(json!([]), None)).unwrap(),
                    observed_at_unix: 200,
                    received_at_unix: 201,
                    source_at_unix: None,
                })
            }
        }
        let fetcher = ConcurrentFetcher {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        };
        let mut pages = Vec::new();
        collect_participant_trades(
            &fetcher,
            BASE,
            &scope(181),
            100,
            NOW,
            &mut pages,
            &mut Vec::new(),
        )
        .await
        .unwrap();
        assert_eq!(fetcher.peak.load(Ordering::SeqCst), PARTICIPANT_CONCURRENCY);
        assert_eq!(pages.len(), 10);
        let ids: Vec<_> = pages
            .iter()
            .flat_map(|page| {
                query(&page.url)["condition"]
                    .split(',')
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, scope(181).condition_ids);
    }

    #[tokio::test]
    async fn participants_gamma_failure_is_tempfail_and_does_not_fetch_or_upsert() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(envelope(json!([]), None)));
        let urls = Arc::clone(&fetcher.urls);
        let r = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            listing,
            |lo, _| async move {
                assert_eq!(lo, 100);
                Err(BootstrapError::Gamma {
                    message: "fixture outage".into(),
                })
            },
        )
        .await
        .unwrap_err();
        assert_eq!(r.exit_code(), 75);
        assert!(r.to_string().contains("failed:gamma"));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        assert!(urls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn participants_disabled_never_fetches_scope_or_trades() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(envelope(json!([]), None)));
        let built = std::cell::Cell::new(false);
        let r = run_participant_discovery(
            &config(0),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || {
                built.set(true);
                Ok(fetcher)
            },
            listing,
            |_, _| async { panic!("disabled must not fetch Gamma") },
        )
        .await
        .unwrap();
        assert_eq!(r.status, "disabled");
        assert!(!built.get());
        assert_eq!((r.unique_wallets, r.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }

    #[tokio::test]
    async fn participants_future_markets_without_trades_are_ok() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(envelope(json!([]), None)));
        let urls = Arc::clone(&fetcher.urls);
        let r = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            listing,
            |_, _| async { Ok(scope(1)) },
        )
        .await
        .unwrap();
        assert_eq!(r.status, "ok");
        assert_eq!((r.unique_wallets, r.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        assert_eq!(urls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn participants_trades_failures_discard_earlier_chunks() {
        for non_array in [true, false] {
            let (_dir, mut cache) = tmp_cache();
            let fetcher = fetcher(|url: &str| {
                if query(url)["condition"].split(',').count() == 20 {
                    Ok(envelope(
                        json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                        None,
                    ))
                } else if non_array {
                    Ok(json!({"error": "not an array"}))
                } else {
                    Err(SourceError::Fatal {
                        message: "HTTP 400".into(),
                    })
                }
            });
            let r = run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Immediate,
                NOW,
                || Ok(fetcher),
                listing,
                |_, _| async { Ok(scope(21)) },
            )
            .await
            .unwrap_err();
            assert_eq!(r.exit_code(), 75);
            assert!(r.to_string().contains("failed:trades"));
            assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        }
    }

    /// The production fetcher waits out the venue's `Retry-After: 1` (#800); the page, its hash
    /// and its times come from the successful reply only.
    #[tokio::test]
    async fn participants_wait_out_a_short_rate_limit_answer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let ok_body = serde_json::to_vec(&envelope(
            json!([{"proxy_wallet": wallet(0xaa), "timestamp": 100}]),
            None,
        ))
        .unwrap();
        let replies = [
            (
                "429 Too Many Requests\r\nRetry-After: 1\r\nDate: Mon, 26 Oct 2026 00:00:00 GMT",
                serde_json::to_vec(&json!([{"proxy_wallet": wallet(0xcc)}])).unwrap(),
            ),
            (
                "200 OK\r\nDate: Mon, 26 Oct 2026 00:00:05 GMT",
                ok_body.clone(),
            ),
        ];
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        // On the test's runtime, so the stub ends with the test even after a single request.
        tokio::spawn(async move {
            for (head, body) in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                let line = String::from_utf8(request).unwrap();
                seen.lock()
                    .unwrap()
                    .push(line.lines().next().unwrap().to_owned());
                let head = format!(
                    "HTTP/1.1 {head}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(head.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let mut pages = Vec::new();
        let wallets = collect_participant_trades(
            &participant_fetcher().unwrap(),
            &base,
            &scope(1),
            100,
            NOW,
            &mut pages,
            &mut Vec::new(),
        )
        .await
        .unwrap();
        assert_eq!(wallets, vec![wallet(0xaa)]);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].rows, 1);
        assert_eq!(
            pages[0].raw_sha256,
            format!("{:x}", Sha256::digest(&ok_body))
        );
        assert_eq!(pages[0].source_at_unix, Some(1_792_972_805));
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert!(requests[0].starts_with("GET /v2/trades?condition="));
    }

    #[tokio::test]
    async fn participants_rediscovery_tags_existing_wallet_and_honors_policy() {
        for policy in [ActivationPolicy::Immediate, ActivationPolicy::Deferred] {
            let (_dir, mut cache) = tmp_cache();
            cache
                .upsert_wallet(&wallet(1), pile::SRC_TRADES, false, None, None, None)
                .unwrap();
            let fetcher = fetcher(|_: &str| {
                Ok(envelope(
                    json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                    None,
                ))
            });
            let r = run_participant_discovery(
                &config(5),
                &mut cache,
                policy,
                NOW,
                || Ok(fetcher),
                listing,
                |_, _| async { Ok(scope(1)) },
            )
            .await
            .unwrap();
            assert_eq!(r.status, "ok");
            assert_eq!(r.unique_wallets, 1);
            assert_eq!(
                r.activated,
                usize::from(policy == ActivationPolicy::Immediate)
            );
            assert_eq!(
                cache.conn_for_test_source_bits(&wallet(1)),
                pile::SRC_TRADES | SRC_MARKET_PARTICIPANT
            );
            assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        }
    }

    #[tokio::test]
    async fn participants_cache_failures_propagate() {
        let (dir, cache) = tmp_cache();
        drop(cache);
        let mut cache = WalletCache::open_read_only(&dir.path().join("cache.db")).unwrap();
        let fetcher = fetcher(|_: &str| {
            Ok(envelope(
                json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                None,
            ))
        });
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            listing,
            |_, _| async { Ok(scope(1)) },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::Sqlite(_) | BootstrapError::Cache { .. }
        ));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }
    #[derive(Clone, Default)]
    struct AuditLog(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for AuditLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn participants_audit_is_a_versioned_summary_plus_one_line_per_page() {
        let (_dir, mut cache) = tmp_cache();
        let log = AuditLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let body = envelope(json!([{"proxy_wallet": wallet(1), "timestamp": 100}]), None);
        let fetcher = fetcher(|_: &str| Ok(body.clone()));
        let mut gamma = scope(1);
        (gamma.version, gamma.schema_version, gamma.parser_version) = (7, 8, 9);
        gamma.pages.push(ScopePage {
            ordinal: 0,
            kind: "events",
            series_id: Some("2".into()),
            url: "http://gamma.test/events/keyset?series_id=2".into(),
            request_cursor: None,
            returned_next_cursor: None,
            raw_sha256: format!("{:x}", Sha256::digest(b"fixture Gamma page")),
            rows: 1,
            observed_at_unix: 110,
            received_at_unix: 111,
            source_at_unix: Some(109),
        });
        let gamma_page = serde_json::to_value(&gamma.pages[0]).unwrap();
        let (digest, count) = (gamma.digest.clone(), gamma.count);
        let versions = (gamma.version, gamma.schema_version, gamma.parser_version);
        run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            NOW,
            || Ok(fetcher),
            listing,
            |_, _| {
                let gamma = gamma.clone();
                async { Ok(gamma) }
            },
        )
        .await
        .unwrap();
        let bytes = log.0.lock().unwrap();
        let lines: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect();
        let field = |message: &str, key: &str| -> Vec<Value> {
            lines
                .iter()
                .filter(|line| line["fields"]["message"] == message)
                .map(|line| serde_json::from_str(line["fields"][key].as_str().unwrap()).unwrap())
                .collect()
        };
        let records = field("winner_discovery: participants", "record");
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record["source_id"], PARTICIPANT_SOURCE_ID);
        assert_eq!(record["schema_version"], 2);
        assert_eq!(record["parser_version"], 2);
        assert_eq!(record["window_start_unix"], 100);
        assert_eq!(record["window_end_unix"], NOW);
        assert_eq!(record["status"], "ok");
        assert_eq!(record["wallet_count"], 1);
        assert_eq!(record["chunks"], 1);
        let chunks = field("winner_discovery: participant chunk", "chunk");
        assert_eq!(
            chunks,
            [
                json!({"condition_ids":scope(1).condition_ids, "lo":100, "newest_timestamp":100, "oldest_timestamp":100, "page_count":1, "stop_reason":"terminal"})
            ]
        );
        assert_eq!(record["scope"]["digest"], digest);
        assert_eq!(record["scope"]["count"], count);
        assert_eq!(record["scope"]["source_id"], "polymarket-gamma");
        assert_eq!(
            (
                record["scope"]["version"].clone(),
                record["scope"]["schema_version"].clone(),
                record["scope"]["parser_version"].clone()
            ),
            (json!(versions.0), json!(versions.1), json!(versions.2)),
            "Gamma's own versions stay in the summary"
        );
        assert!(
            record["scope"].get("condition_ids").is_none(),
            "ids stay out of the summary"
        );
        assert_eq!(
            (
                record["gamma_pages"].clone(),
                record["trades_pages"].clone()
            ),
            (json!(1), json!(1))
        );
        let pages = field("winner_discovery: participant page", "page");
        assert_eq!(
            pages.len(),
            2,
            "every Gamma and trades page gets its own line"
        );
        assert_eq!(pages[0], gamma_page);
        assert_eq!(pages[1]["kind"], "trades");
        assert_eq!(pages[1]["rows"], 1);
        assert_eq!(
            pages[1]["raw_sha256"],
            format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap()))
        );
        assert_eq!(pages[1]["observed_at_unix"], 200);
        assert_eq!(pages[1]["received_at_unix"], 201);
        assert_eq!(pages[1]["source_at_unix"], 199);
    }

    #[tokio::test]
    async fn participants_activation_errors_propagate_after_upsert() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .raw_conn_for_test()
            .execute_batch(
                "CREATE TRIGGER fail_activation BEFORE UPDATE OF is_active ON wallets \
             BEGIN SELECT RAISE(ABORT, 'fixture activation failure'); END;",
            )
            .unwrap();
        let fetcher = fetcher(|_: &str| {
            Ok(envelope(
                json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                None,
            ))
        });
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            listing,
            |_, _| async { Ok(scope(1)) },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("fixture activation failure"));
        assert_eq!(
            cache.conn_for_test_source_bits(&wallet(1)),
            SRC_MARKET_PARTICIPANT
        );
        assert_eq!(cache.active_wallet_count().unwrap(), 0);
    }
    fn coverage(from: i64, through: i64, floor: i64) -> ParticipantDiscoveryCoverage {
        ParticipantDiscoveryCoverage {
            version: 1,
            scope: ParticipantScope::new(&["15m".into()], Some(&scope(1).series)).unwrap(),
            market_floor_unix: floor,
            covered_from_unix: from,
            covered_through_unix: through,
        }
    }

    #[tokio::test]
    async fn participants_invalid_wallet_refuses_the_chunk_and_logs_its_hash() {
        for invalid in [
            json!({"timestamp": 100}),
            json!({"proxy_wallet": null, "timestamp": 100}),
            json!({"proxy_wallet": "0x123", "timestamp": 100}),
            json!({"proxy_wallet": format!("0x{}", "g".repeat(40)), "timestamp": 100}),
            json!({"proxy_wallet": "é".repeat(21), "timestamp": 100}),
            json!({"proxy_wallet": format!("1x{}", "a".repeat(40)), "timestamp": 100}),
        ] {
            let (_dir, mut cache) = tmp_cache();
            let prior = coverage(50, 90, 20);
            cache.set_participant_discovery_coverage(&prior).unwrap();
            let log = AuditLog::default();
            let writer = log.clone();
            let _guard = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .with_ansi(false)
                    .with_writer(move || writer.clone())
                    .finish(),
            );
            let body = envelope(
                json!([{"proxy_wallet": wallet(1), "timestamp": 100}, invalid]),
                None,
            );
            let hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap()));
            let error = run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Deferred,
                NOW,
                || Ok(fetcher(|_: &str| Ok(body.clone()))),
                listing,
                |_, _| async { Ok(scope(1)) },
            )
            .await
            .unwrap_err();
            assert_eq!(error.exit_code(), 75);
            assert_eq!(cache.wallet_pile_size().unwrap(), 0);
            assert_eq!(cache.participant_discovery_coverage().unwrap(), Some(prior));
            assert!(
                String::from_utf8(log.0.lock().unwrap().clone())
                    .unwrap()
                    .contains(&hash)
            );
        }
    }

    #[test]
    fn participants_coverage_accessor_fails_closed() {
        let (_dir, mut cache) = tmp_cache();
        assert_eq!(cache.participant_discovery_coverage().unwrap(), None);
        let valid = coverage(10, 20, 0);
        cache.set_participant_discovery_coverage(&valid).unwrap();
        assert_eq!(cache.participant_discovery_coverage().unwrap(), Some(valid));
        for malformed in [
            "not json",
            "{}",
            r#"{"version":2,"scope":{"recurrences":["15m"],"series":null},"market_floor_unix":0,"covered_from_unix":10,"covered_through_unix":20}"#,
        ] {
            cache
                .raw_conn_for_test()
                .execute(
                    "UPDATE meta SET value=?1 WHERE key='participant_discovery_coverage'",
                    [malformed],
                )
                .unwrap();
            assert!(cache.participant_discovery_coverage().is_err());
        }
        cache
            .raw_conn_for_test()
            .execute_batch("DROP TABLE meta")
            .unwrap();
        assert!(matches!(
            cache.participant_discovery_coverage(),
            Err(BootstrapError::Sqlite(_))
        ));
    }

    #[tokio::test]
    async fn participants_long_gap_failure_and_interrupted_record_repeat_the_tail() {
        let (_dir, mut cache) = tmp_cache();
        let prior = coverage(10, 105, 20);
        cache.set_participant_discovery_coverage(&prior).unwrap();
        let broken = fetcher(|_url: &str| {
            Err(SourceError::Fatal {
                message: "fixture page failure".into(),
            })
        });
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            1_000,
            || Ok(broken),
            listing,
            |lo, _| async move {
                assert_eq!(lo, 100);
                Ok(scope(1))
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.exit_code(), 75);
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(prior.clone())
        );
        cache.raw_conn_for_test().execute_batch("CREATE TRIGGER stop_coverage BEFORE INSERT ON meta WHEN NEW.key='participant_discovery_coverage' BEGIN SELECT RAISE(ABORT, 'interrupted coverage commit'); END;").unwrap();
        let good = || {
            Ok(fetcher(|_url: &str| {
                Ok(envelope(
                    json!([{"proxy_wallet": wallet(1), "timestamp": 100}]),
                    None,
                ))
            }))
        };
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            1_000,
            good,
            listing,
            |lo, _| async move {
                assert_eq!(lo, 100);
                Ok(scope(1))
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("interrupted coverage commit"));
        assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        assert_eq!(cache.participant_discovery_coverage().unwrap(), Some(prior));
        cache
            .raw_conn_for_test()
            .execute_batch("DROP TRIGGER stop_coverage")
            .unwrap();
        run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            1_000,
            good,
            listing,
            |lo, _| async move {
                assert_eq!(lo, 100);
                Ok(scope(1))
            },
        )
        .await
        .unwrap();
        assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(coverage(10, 1_000, 100))
        );
        run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            1_005,
            || Ok(fetcher(|_url: &str| Ok(envelope(json!([]), None)))),
            listing,
            |lo, _| async move {
                assert_eq!(lo, 995);
                let mut scope = scope(1);
                scope.end_date_min_unix = 20;
                Ok(scope)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(coverage(10, 1_005, 100))
        );
    }

    #[tokio::test]
    async fn participants_disabled_preserves_coverage_and_bad_records_stop_before_reads() {
        let (_dir, mut cache) = tmp_cache();
        let prior = coverage(10, 20, 0);
        cache.set_participant_discovery_coverage(&prior).unwrap();
        run_participant_discovery(
            &config(0),
            &mut cache,
            ActivationPolicy::Deferred,
            NOW,
            || Ok(fetcher(|_: &str| panic!("disabled fetch"))),
            listing,
            |_, _| async { panic!("disabled scope") },
        )
        .await
        .unwrap();
        assert_eq!(cache.participant_discovery_coverage().unwrap(), Some(prior));
        cache
            .raw_conn_for_test()
            .execute(
                "UPDATE meta SET value='malformed' WHERE key='participant_discovery_coverage'",
                [],
            )
            .unwrap();
        assert!(
            run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Deferred,
                NOW,
                || Ok(fetcher(|_: &str| panic!("bad coverage trades"))),
                listing,
                |_, _| async { panic!("bad coverage scope") }
            )
            .await
            .is_err()
        );
        cache
            .raw_conn_for_test()
            .execute_batch("DROP TABLE meta")
            .unwrap();
        assert!(
            run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Deferred,
                NOW,
                || Ok(fetcher(|_: &str| panic!("bad query trades"))),
                listing,
                |_, _| async { panic!("bad query scope") }
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn participants_scope_change_replaces_coverage_on_schema_two_without_source_cursor() {
        let (_dir, mut cache) = tmp_cache();
        cache.raw_conn_for_test().execute_batch("ALTER TABLE source_cursor RENAME TO source_cursor_v1_sealed; PRAGMA user_version=2;").unwrap();
        let mut legacy = coverage(10, 20, 0);
        legacy.scope.series = None;
        cache.set_participant_discovery_coverage(&legacy).unwrap();
        let pin = crate::market_scope::parse_pinned_series("8:eight,7:seven").unwrap();
        run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            NOW,
            || Ok(fetcher(|_url: &str| Ok(envelope(json!([]), None)))),
            || async { Ok((pin.clone(), Vec::new())) },
            |lo, subset| async move {
                assert_eq!(lo, 100);
                let mut scope = scope(1);
                scope.series = subset;
                Ok(scope)
            },
        )
        .await
        .unwrap();
        let mut expected = coverage(100, NOW, 100);
        expected.scope = ParticipantScope::new(&["15m".into()], Some(&pin)).unwrap();
        assert_eq!(expected.scope.series.as_ref().unwrap()[0].id, "7");
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn participants_union_preserves_ten_series_and_reads_additions_back_to_covered_from() {
        let mut covered = crate::market_scope::parse_pinned_series(
            &(1..=10)
                .map(|id| format!("{id}:s{id}"))
                .collect::<Vec<_>>()
                .join(","),
        )
        .unwrap();
        covered.sort_by(|a, b| a.id.cmp(&b.id));
        for mode in ["same", "addition", "omission"] {
            let (_dir, mut cache) = tmp_cache();
            let mut prior = coverage(10, 100, 20);
            prior.scope = ParticipantScope::new(&["15m".into()], Some(&covered)).unwrap();
            cache.set_participant_discovery_coverage(&prior).unwrap();
            let mut listed = covered.clone();
            match mode {
                "addition" => listed.push(ScopeSeries {
                    id: "11".into(),
                    slug: Some("s11".into()),
                }),
                "omission" => {
                    listed.pop();
                }
                _ => {}
            }
            let sweeps = std::cell::RefCell::new(Vec::new());
            let fetcher = fetcher(|url: &str| {
                let ids = query(url)["condition"].clone();
                let addition = ids == format!("0x{:064x}", 11);
                Ok(envelope(
                    json!([{"proxy_wallet": wallet(if addition { 2 } else { 1 }), "timestamp": if addition { 50 } else { 99 }}]),
                    None,
                ))
            });
            let result = run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Deferred,
                200,
                || Ok(fetcher),
                || async { Ok((listed.clone(), Vec::new())) },
                |lo, subset| {
                    sweeps.borrow_mut().push((lo, subset.clone()));
                    let mut scope = scope(0);
                    scope.series = subset;
                    scope.condition_ids = scope
                        .series
                        .iter()
                        .map(|series| format!("0x{:064x}", series.id.parse::<u64>().unwrap()))
                        .collect();
                    scope.end_date_min_unix = 20;
                    async { Ok(scope) }
                },
            )
            .await
            .unwrap();
            let sweeps = sweeps.borrow();
            assert_eq!(sweeps[0], (95, covered.clone()), "{mode}");
            assert_eq!(sweeps.len(), if mode == "addition" { 2 } else { 1 });
            let current = cache.participant_discovery_coverage().unwrap().unwrap();
            let mut expected = prior;
            expected.covered_through_unix = 200;
            if mode == "addition" {
                assert_eq!(
                    sweeps[1],
                    (
                        10,
                        vec![ScopeSeries {
                            id: "11".into(),
                            slug: Some("s11".into())
                        }]
                    )
                );
                expected.scope = ParticipantScope::new(&["15m".into()], Some(&listed)).unwrap();
                assert_eq!(
                    result.unique_wallets, 2,
                    "the historical addition participant is collected"
                );
            } else {
                assert_eq!(result.unique_wallets, 1);
            }
            assert_eq!(current, expected, "{mode}");
        }
    }

    #[tokio::test]
    async fn participants_addition_outside_v2_window_cannot_extend_coverage() {
        let (_dir, mut cache) = tmp_cache();
        let now = PARTICIPANT_WINDOW_SECS + 200;
        let prior = coverage(10, now - 10, 0);
        cache.set_participant_discovery_coverage(&prior).unwrap();
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            now,
            || {
                Ok(fetcher(|_: &str| {
                    panic!("old addition must not read trades")
                }))
            },
            || async {
                Ok((
                    crate::market_scope::parse_pinned_series("2:two,3:three").unwrap(),
                    Vec::new(),
                ))
            },
            |_, _| async { panic!("old addition must not sweep") },
        )
        .await
        .unwrap_err();
        assert_eq!(error.exit_code(), 75);
        assert!(error.to_string().contains("failed:window"));
        assert_eq!(cache.participant_discovery_coverage().unwrap(), Some(prior));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }

    #[test]
    fn participants_import_merges_intervals_and_validates_before_writes() {
        let (dir, mut cache) = tmp_cache();
        let file = dir.path().join("participants.txt");
        std::fs::write(
            &file,
            format!(
                "0x{}\n\n{}\n",
                wallet(0xab)[2..].to_uppercase(),
                wallet(0xab)
            ),
        )
        .unwrap();
        assert_eq!(
            import_participants(&mut cache, &file, coverage(10, 20, 5), NOW).unwrap(),
            1
        );
        assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        for (input, expected) in [
            (coverage(20, 30, 6), coverage(10, 30, 6)),
            (coverage(15, 25, 7), coverage(10, 30, 7)),
            (coverage(0, 10, 7), coverage(0, 30, 7)),
            (coverage(0, 30, 6), coverage(0, 30, 6)),
        ] {
            import_participants(&mut cache, &file, input, NOW).unwrap();
            assert_eq!(
                cache.participant_discovery_coverage().unwrap(),
                Some(expected.clone())
            );
            import_participants(&mut cache, &file, expected.clone(), NOW).unwrap();
            assert_eq!(
                cache.participant_discovery_coverage().unwrap(),
                Some(expected)
            );
            assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        }
        let prior = cache.participant_discovery_coverage().unwrap();
        std::fs::write(&file, wallet(2)).unwrap();
        for (bad, reason) in [
            (coverage(31, 40, 6), "missing bridge [30, 31)"),
            (coverage(10, 20, 5), "requires coverage of [0, 30)"),
            (coverage(-1, 10, 6), "invalid"),
            (coverage(10, 10, 6), "invalid"),
            (coverage(20, 10, 6), "invalid"),
            (coverage(10, NOW + 1, 6), "future"),
        ] {
            let error = import_participants(&mut cache, &file, bad, NOW).unwrap_err();
            assert!(error.to_string().contains(reason), "{error}");
            assert_eq!(cache.participant_discovery_coverage().unwrap(), prior);
            assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        }
        std::fs::write(&file, format!("{}\ninvalid\n{}", wallet(2), wallet(3))).unwrap();
        assert!(import_participants(&mut cache, &file, coverage(30, 40, 6), NOW).is_err());
        assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        assert_eq!(cache.participant_discovery_coverage().unwrap(), prior);
        std::fs::write(&file, wallet(2)).unwrap();
        let mut replacement = coverage(50, 60, 1);
        replacement.scope.recurrences = vec!["5m".into()];
        import_participants(&mut cache, &file, replacement.clone(), NOW).unwrap();
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(replacement)
        );
    }

    #[tokio::test]
    async fn participants_import_repairs_old_coverage_then_discovery_reads_only_recent_tail() {
        let (dir, mut cache) = tmp_cache();
        cache
            .set_participant_discovery_coverage(&coverage(10, 20, 100))
            .unwrap();
        let file = dir.path().join("participants.txt");
        std::fs::write(&file, wallet(1)).unwrap();
        import_participants(&mut cache, &file, coverage(0, 100, 20), NOW).unwrap();
        run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            NOW,
            || Ok(fetcher(|_url: &str| Ok(envelope(json!([]), None)))),
            listing,
            |lo, _| async move {
                assert_eq!(lo, 95);
                let mut scope = scope(1);
                scope.end_date_min_unix = 20;
                Ok(scope)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(coverage(0, NOW, 20))
        );
    }

    #[test]
    fn participants_import_commits_wallets_before_coverage_and_reports_exclusions() {
        let (dir, mut cache) = tmp_cache();
        let file = dir.path().join("participants.txt");
        let exchange = format!("{:#x}", crate::chain::ALL_EXCHANGE_CONTRACTS[0]);
        std::fs::write(
            &file,
            format!("{}\n{}\n{}\n{exchange}", wallet(1), wallet(2), wallet(3)),
        )
        .unwrap();
        cache
            .upsert_wallet(&wallet(2), pile::SRC_TRADES, true, None, None, None)
            .unwrap();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO purged_wallets VALUES (?1, 1, 'proven_loser')",
                [wallet(1)],
            )
            .unwrap();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO purged_wallets VALUES (?1, 1, 'infra')",
                [wallet(3)],
            )
            .unwrap();
        let log = AuditLog::default();
        let writer = log.clone();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
        );
        cache.raw_conn_for_test().execute_batch("CREATE TRIGGER stop_coverage BEFORE INSERT ON meta WHEN NEW.key='participant_discovery_coverage' BEGIN SELECT RAISE(ABORT, 'interrupted import'); END;").unwrap();
        assert!(
            import_participants(&mut cache, &file, coverage(10, 20, 0), NOW)
                .unwrap_err()
                .to_string()
                .contains("interrupted import")
        );
        assert_eq!(cache.participant_discovery_coverage().unwrap(), None);
        assert!(!cache.is_purged(&wallet(1)).unwrap());
        assert!(cache.is_purged(&wallet(3)).unwrap());
        assert_eq!(cache.wallet_pile_size().unwrap(), 2);
        // An inherited infra flag is permanent, participants included (#826).
        assert_eq!(
            cache
                .raw_conn_for_test()
                .query_row::<i64, _, _>(
                    "SELECT is_infra FROM wallets WHERE wallet_hex=?1",
                    [wallet(2)],
                    |row| row.get(0)
                )
                .unwrap(),
            1
        );
        cache
            .raw_conn_for_test()
            .execute_batch("DROP TRIGGER stop_coverage")
            .unwrap();
        import_participants(&mut cache, &file, coverage(10, 20, 0), NOW).unwrap();
        let log = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        assert!(
            log.contains(&wallet(3)) && log.contains("skipped_infra=1"),
            "{log}"
        );
        assert!(log.contains(&format!(
            "{:x}",
            Sha256::digest(std::fs::read(&file).unwrap())
        )));
        assert!(log.contains("wallet_count=4"));
        assert_eq!(
            cache.participant_discovery_coverage().unwrap(),
            Some(coverage(10, 20, 0))
        );
    }
    #[tokio::test]
    async fn participants_discovery_names_infra_skips_on_every_run() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO purged_wallets VALUES (?1, 1, 'infra')",
                [wallet(3)],
            )
            .unwrap();
        let log = AuditLog::default();
        let writer = log.clone();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
        );
        for _ in 0..2 {
            run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Deferred,
                NOW,
                || {
                    Ok(fetcher(|_: &str| {
                        Ok(envelope(
                            json!([{"proxy_wallet": wallet(3), "timestamp": 100}]),
                            None,
                        ))
                    }))
                },
                listing,
                |_, _| async { Ok(scope(1)) },
            )
            .await
            .unwrap();
        }
        let log = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        assert_eq!(
            log.matches("market participant skipped: infra tombstone")
                .count(),
            2
        );
        assert_eq!(log.matches(&wallet(3)).count(), 2);
        assert_eq!(log.matches("skipped_infra=1").count(), 2);
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }

    #[test]
    fn participants_normalize_scope_and_block_all_exchange_contracts() {
        let (_dir, mut cache) = tmp_cache();
        let scope = ParticipantScope::new(&["5m".into(), "15m".into()], None).unwrap();
        assert_eq!(scope.recurrences, ["15m", "5m"]);
        assert_eq!(scope.series, None);
        let contracts: Vec<_> = crate::chain::ALL_EXCHANGE_CONTRACTS
            .iter()
            .map(|address| format!("{address:#x}"))
            .collect();
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO purged_wallets VALUES (?1, 1, 'proven_loser')",
                [&contracts[0]],
            )
            .unwrap();
        upsert_participants(&mut cache, contracts).unwrap();
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }
}
