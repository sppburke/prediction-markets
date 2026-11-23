//! Gamma market scope for the ranker and participant discovery (#784, #800).
//!
//! This command lists every Gamma series, keeps those with a requested recurrence and gate F's
//! usable non-test slug (#871), walks each one's events through the documented `/events/keyset`
//! endpoint back to an end-date floor, leaves out every event whose own slug fails the same rule
//! (#892), and writes the markets' condition ids with every page's hash, cursor chain and times.
//! Any surprise in a reply fails the command (exit 1); exhausted transient fetches and Gamma
//! inconsistencies exit 75 so the loop supervisor retries the retained cycle. Nothing touches the
//! cache.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use pe_core_types::RawHttpAttempt;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    FIFTEEN_MINUTE_RECURRENCE, HttpRequestContext, POLYMARKET_TEST_SERIES_SLUG_PREFIX,
    ReqwestFetcher, event_series_recurrence,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::{info, warn};

use crate::config::BootstrapConfig;
use crate::error::BootstrapError;

/// Gamma's request budget is at most 2 requests/s sustained (`docs/_GLOSSARY.md`, rate limits).
const SCOPE_MIN_INTERVAL_MS: u64 = 500;
/// `/series` returns at most 50 rows per page whatever `limit` asks for (probed 2026-10-15).
const SERIES_PAGE_LIMIT: usize = 50;
const SERIES_MAX_PAGES: usize = 200;
/// `/events/keyset` documents `limit` at most 100, but Gamma serves pages above 50 gzip-compressed
/// only (HTTP 406 `gzip_required` since 2026-11-12, `docs/15-SOURCES.md`) and our client sends none.
const EVENTS_PAGE_LIMIT: usize = 50;
const EVENTS_MAX_PAGES_PER_SERIES: usize = 2_000;
/// Page-level retries after the fetcher's own, as in the `/events` sweep (`events.rs`).
const PAGE_MAX_RETRIES: u32 = 5;
const PAGE_RETRY_BASE: Duration = Duration::from_secs(1);
const SOURCE_ID: &str = "polymarket-gamma";

/// One fetched page with its request and receipt times and the reply's `Date` (source) time.
pub(crate) struct FetchedPage {
    pub(crate) body: Vec<u8>,
    pub(crate) observed_at_unix: i64,
    pub(crate) received_at_unix: i64,
    pub(crate) source_at_unix: Option<i64>,
}

/// Page transport; production observes each attempt, tests supply pages directly.
pub(crate) trait ScopeFetcher {
    fn fetch(
        &self,
        url: &str,
        endpoint_kind: &'static str,
        source_id: &'static str,
    ) -> impl std::future::Future<Output = Result<FetchedPage, SourceError>> + Send;
}

impl ScopeFetcher for ReqwestFetcher {
    async fn fetch(
        &self,
        url: &str,
        endpoint_kind: &'static str,
        source_id: &'static str,
    ) -> Result<FetchedPage, SourceError> {
        let mut times = None;
        let body = self
            .fetch_page_observed(
                url,
                HttpRequestContext {
                    source_id,
                    endpoint_kind,
                },
                |attempt| {
                    if let RawHttpAttempt::Response(response) = attempt {
                        times = Some((
                            response.observed_at,
                            response.received_at,
                            response.source_at,
                        ));
                    }
                    Ok(())
                },
            )
            .await?;
        let (observed_at, received_at, source_at) = times.ok_or_else(|| SourceError::Fatal {
            message: "reply returned without an observed response".to_owned(),
        })?;
        Ok(FetchedPage {
            body,
            observed_at_unix: observed_at.unix_timestamp(),
            received_at_unix: received_at.unix_timestamp(),
            source_at_unix: source_at.map(OffsetDateTime::unix_timestamp),
        })
    }
}

/// A listed series with a requested recurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeSeries {
    pub id: String,
    pub slug: Option<String>,
}

/// Why a series of a requested recurrence, or an event of a kept series, stays out of the scope: gate F's slug rule
/// (#869, #871), which each event's own slug must also pass (#892).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    /// The slug starts with Polymarket's test-series prefix.
    TestSeries,
    /// The slug is absent, null, not a string or blank.
    NoSlug,
    /// An event's own slug starts with Polymarket's test prefix (#892).
    TestEvent,
}

/// A requested-recurrence series the scope excludes, with the slug it was judged by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExcludedSeries {
    pub id: String,
    pub slug: Option<String>,
    pub reason: ExclusionReason,
}

/// Gate F's series rule after its recurrence check: a usable slug outside Polymarket's test namespace. A blank slug
/// identifies nothing, as in admission's read (`str::trim`).
pub(crate) fn series_exclusion(slug: Option<&str>) -> Option<ExclusionReason> {
    match slug.filter(|slug| !slug.trim().is_empty()) {
        None => Some(ExclusionReason::NoSlug),
        Some(slug) if slug.starts_with(POLYMARKET_TEST_SERIES_SLUG_PREFIX) => {
            Some(ExclusionReason::TestSeries)
        }
        Some(_) => None,
    }
}

/// The same rule on an event's own slug (#892): Polymarket lists test events inside production series.
pub(crate) fn event_exclusion(slug: Option<&str>) -> Option<ExclusionReason> {
    series_exclusion(slug).map(|reason| match reason {
        ExclusionReason::TestSeries => ExclusionReason::TestEvent,
        other => other,
    })
}

/// An event of a kept series that the slug rule leaves out (#892), with the markets it listed. `event_id` is Gamma's
/// id as canonical decimal text; `slug` is the judged value when it is a string, blank included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExcludedEvent {
    pub series_id: String,
    pub event_id: String,
    pub slug: Option<String>,
    pub reason: ExclusionReason,
    pub condition_ids: Vec<String>,
}

/// The kept markets and excluded events of one or more sweeps (#892). A market is never both kept and excluded, and
/// one event id always carries one record: a repeat coalesces only when it is identical. A breach is Gamma
/// inconsistency, so it fails temporarily (exit 75), as a disagreeing embedded slug does, rather than stopping the loop
/// or silently dropping or keeping a market. Participant discovery merges its two sweeps through the same checks.
#[derive(Debug, Default)]
pub(crate) struct ScopeMembership {
    kept: BTreeSet<String>,
    /// The first kept event of each kept market, as named in a conflict.
    kept_by: HashMap<String, String>,
    excluded: Vec<ExcludedEvent>,
    by_event: HashMap<String, usize>,
    /// Each excluded market's excluded event id.
    excluded_ids: HashMap<String, String>,
}

impl ScopeMembership {
    fn keep(
        &mut self,
        condition_id: String,
        kept_event: &str,
        at: &str,
    ) -> Result<(), BootstrapError> {
        if let Some(excluded) = self.excluded_ids.get(&condition_id) {
            return Err(membership_conflict(
                at,
                &format!(
                    "market {condition_id} is listed by kept event {kept_event} and excluded event {excluded}"
                ),
            ));
        }
        self.kept_by
            .entry(condition_id.clone())
            .or_insert_with(|| kept_event.to_owned());
        self.kept.insert(condition_id);
        Ok(())
    }

    fn exclude(&mut self, event: ExcludedEvent, at: &str) -> Result<(), BootstrapError> {
        if let Some(&first) = self.by_event.get(&event.event_id) {
            if self.excluded[first] != event {
                return Err(membership_conflict(
                    at,
                    &format!(
                        "excluded event {} is listed again with a different series, slug, reason or markets",
                        event.event_id
                    ),
                ));
            }
            return Ok(());
        }
        if let Some(kept) = event
            .condition_ids
            .iter()
            .find(|id| self.kept.contains(*id))
        {
            let kept_event = self.kept_by.get(kept).map_or("?", String::as_str);
            return Err(membership_conflict(
                at,
                &format!(
                    "market {kept} is listed by kept event {kept_event} and excluded event {}",
                    event.event_id
                ),
            ));
        }
        for condition_id in &event.condition_ids {
            self.excluded_ids
                .insert(condition_id.clone(), event.event_id.clone());
        }
        self.by_event
            .insert(event.event_id.clone(), self.excluded.len());
        self.excluded.push(event);
        Ok(())
    }

    /// Merge a finished sweep's kept markets and excluded events through the same checks.
    pub(crate) fn absorb(&mut self, scope: &MarketScope) -> Result<(), BootstrapError> {
        for event in &scope.excluded_events {
            self.exclude(event.clone(), "participant sweeps")?;
        }
        for condition_id in &scope.condition_ids {
            self.keep(
                condition_id.clone(),
                "(another participant sweep)",
                "participant sweeps",
            )?;
        }
        Ok(())
    }
}

fn membership_conflict(at: &str, message: &str) -> BootstrapError {
    BootstrapError::TransientSource {
        source_name: "gamma-events",
        message: format!("market-scope {at}: {message}"),
    }
}

/// Split series into those the scope keeps and those gate F's slug rule excludes, preserving order.
pub(crate) fn partition_series(
    series: impl IntoIterator<Item = ScopeSeries>,
) -> (Vec<ScopeSeries>, Vec<ExcludedSeries>) {
    let mut kept = Vec::new();
    let mut excluded = Vec::new();
    for listed in series {
        match series_exclusion(listed.slug.as_deref()) {
            None => kept.push(listed),
            Some(reason) => excluded.push(ExcludedSeries {
                id: listed.id,
                slug: listed.slug,
                reason,
            }),
        }
    }
    (kept, excluded)
}

/// Order exclusions by numeric series id (ids are decimal) and warn for each series without a usable slug: it
/// could hide a would-be production series, which pe-service also refuses (`series_slug:none`).
pub(crate) fn settle_exclusions(excluded: &mut [ExcludedSeries]) {
    excluded.sort_by_key(|series| (series.id.parse::<u64>().ok(), series.id.clone()));
    for series in excluded.iter() {
        if series.reason == ExclusionReason::NoSlug {
            warn!(series = %series.id, slug = ?series.slug,
                "market scope: series of a requested recurrence has no usable slug; excluded");
        }
    }
}

/// One fetched page's provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScopePage {
    pub ordinal: u64,
    pub kind: &'static str,
    pub series_id: Option<String>,
    pub url: String,
    pub request_cursor: Option<String>,
    pub returned_next_cursor: Option<String>,
    pub raw_sha256: String,
    pub rows: u64,
    pub observed_at_unix: i64,
    pub received_at_unix: i64,
    pub source_at_unix: Option<i64>,
}

/// The scope file `scripts/ranker_duck.py` `load_market_scope` reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketScope {
    pub version: u32,
    pub source_id: &'static str,
    pub schema_version: u32,
    pub parser_version: u32,
    pub recurrence: String,
    pub end_date_min_unix: i64,
    pub started_at_unix: i64,
    pub completed_at_unix: i64,
    pub series: Vec<ScopeSeries>,
    /// Requested-recurrence series that gate F's slug rule keeps out (#871); omitted when there are none.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded_series: Vec<ExcludedSeries>,
    /// Events of kept series whose own slug the rule keeps out (#892), in walk order; omitted when there are none.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded_events: Vec<ExcludedEvent>,
    pub pages: Vec<ScopePage>,
    pub condition_ids: Vec<String>,
    pub count: u64,
    pub digest: String,
}

pub(crate) fn canonical_recurrences(recurrences: &[String]) -> Result<Vec<String>, BootstrapError> {
    if recurrences.is_empty()
        || recurrences
            .iter()
            .any(|value| !matches!(value.as_str(), FIFTEEN_MINUTE_RECURRENCE | "5m"))
    {
        return Err(BootstrapError::Invalid {
            message: "market recurrences must be a non-empty list of 15m or 5m".to_owned(),
        });
    }
    let mut sorted = recurrences.to_vec();
    sorted.sort();
    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(BootstrapError::Invalid {
            message: "market recurrences must not contain duplicates".to_owned(),
        });
    }
    Ok(sorted)
}

/// Build the production fetcher, sweep, and write the scope file atomically.
pub async fn run_market_scope(
    config: &BootstrapConfig,
    out: &Path,
    end_date_min_unix: i64,
    recurrences: &[String],
    pinned: Option<&[ScopeSeries]>,
) -> Result<MarketScope, BootstrapError> {
    let scope = recent_market_scope(config, end_date_min_unix, recurrences, pinned).await?;
    write_market_scope(out, scope)
}

/// The stage's ranking list must hold markets: an empty scope fails before anything is written.
fn write_market_scope(out: &Path, scope: MarketScope) -> Result<MarketScope, BootstrapError> {
    require_markets(&scope)?;
    crate::cache_migration::atomic_write_json(out, &scope)?;
    Ok(scope)
}

/// A sweep reports what Gamma lists, even no market; a caller that needs markets fails closed here.
pub(crate) fn require_markets(scope: &MarketScope) -> Result<(), BootstrapError> {
    if scope.condition_ids.is_empty() {
        return Err(no_markets());
    }
    Ok(())
}

pub(crate) fn no_markets() -> BootstrapError {
    BootstrapError::Gamma {
        message: "market-scope: the requested series list no markets".to_owned(),
    }
}

/// Sweep recent markets with page provenance, without writing a file.
pub(crate) async fn recent_market_scope(
    config: &BootstrapConfig,
    end_date_min_unix: i64,
    recurrences: &[String],
    pinned: Option<&[ScopeSeries]>,
) -> Result<MarketScope, BootstrapError> {
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = ReqwestFetcher::new(client).with_min_interval_ms(SCOPE_MIN_INTERVAL_MS);
    sweep_market_scope(
        &fetcher,
        &config.gamma_base_url,
        end_date_min_unix,
        recurrences,
        pinned,
        PAGE_RETRY_BASE,
    )
    .await
}

/// List the current recurrence catalog, retaining its page provenance for discovery.
pub(crate) async fn list_market_series(
    config: &BootstrapConfig,
    recurrences: &[String],
) -> Result<(Vec<ScopeSeries>, Vec<ScopePage>), BootstrapError> {
    let recurrences = canonical_recurrences(recurrences)?;
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = ReqwestFetcher::new(client).with_min_interval_ms(SCOPE_MIN_INTERVAL_MS);
    let mut pages = Vec::new();
    let series = list_series(
        &fetcher,
        &config.gamma_base_url,
        &recurrences,
        PAGE_RETRY_BASE,
        &mut pages,
    )
    .await?;
    Ok((series.into_values().collect(), pages))
}

/// Parse `id:slug,...`, the exact series a scope reads by id (schema two passes discovery's covered set).
pub fn parse_pinned_series(text: &str) -> Result<Vec<ScopeSeries>, BootstrapError> {
    let mut pinned: Vec<ScopeSeries> = Vec::new();
    for item in text.split(',') {
        let invalid = || BootstrapError::Invalid {
            message: format!("market series must be id:slug pairs; got {item:?}"),
        };
        let (id, slug) = item.split_once(':').ok_or_else(invalid)?;
        let number: u64 = id.parse().map_err(|_| invalid())?;
        if slug.is_empty() || number.to_string() != id {
            return Err(invalid());
        }
        if pinned.iter().any(|series| series.id == id) {
            return Err(BootstrapError::Invalid {
                message: format!("market series {id} is pinned twice"),
            });
        }
        pinned.push(ScopeSeries {
            id: id.to_owned(),
            slug: Some(slug.to_owned()),
        });
    }
    Ok(pinned)
}

/// List the requested series (or read exactly the pinned ones by id), then walk each one's events
/// ending at or after the floor. Gate F's slug rule excludes series after the listing and before any
/// by-id read, so an excluded series is never read or walked; the scope records each exclusion.
async fn sweep_market_scope<F: ScopeFetcher + Sync>(
    fetcher: &F,
    base_url: &str,
    end_date_min_unix: i64,
    recurrences: &[String],
    pinned: Option<&[ScopeSeries]>,
    retry_base: Duration,
) -> Result<MarketScope, BootstrapError> {
    let recurrences = canonical_recurrences(recurrences)?;
    let started_at_unix = OffsetDateTime::now_utc().unix_timestamp();
    let floor = OffsetDateTime::from_unix_timestamp(end_date_min_unix)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .ok_or_else(|| BootstrapError::Invalid {
            message: format!("market-scope: unusable --end-date-min {end_date_min_unix}"),
        })?;
    let mut pages = Vec::new();
    let (series, mut excluded_series) = match pinned {
        Some(pinned) => {
            let (kept, excluded) = partition_series(pinned.iter().cloned());
            let read = pinned_series(
                fetcher,
                base_url,
                &kept,
                &recurrences,
                retry_base,
                &mut pages,
            )
            .await?;
            (read.into_values().collect::<Vec<_>>(), excluded)
        }
        None => partition_series(
            list_series(fetcher, base_url, &recurrences, retry_base, &mut pages)
                .await?
                .into_values(),
        ),
    };
    settle_exclusions(&mut excluded_series);
    if series.is_empty() {
        return Err(BootstrapError::Gamma {
            message: "market-scope: Gamma lists no requested series".to_owned(),
        });
    }

    let mut membership = ScopeMembership::default();
    for listed_series in &series {
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut walked = 0usize;
        loop {
            if walked == EVENTS_MAX_PAGES_PER_SERIES {
                return Err(BootstrapError::Gamma {
                    message: format!(
                        "market-scope: series {} exceeded {EVENTS_MAX_PAGES_PER_SERIES} pages",
                        listed_series.id
                    ),
                });
            }
            let url = events_url(base_url, &listed_series.id, &floor, cursor.as_deref())?;
            let page = fetch_with_retry(fetcher, &url, "events", retry_base).await?;
            let value: Value = parse_page(&url, &page.body)?;
            let events = value
                .get("events")
                .and_then(Value::as_array)
                .ok_or_else(|| anomaly_error(&url, "reply has no events array"))?;
            let next_cursor = match value.get("next_cursor") {
                None | Some(Value::Null) => None,
                Some(Value::String(next)) if !next.is_empty() => Some(next.clone()),
                Some(_) => return anomaly(&url, "next_cursor is not a non-empty string"),
            };
            for event in events {
                collect_event(
                    &url,
                    &listed_series.id,
                    event,
                    &recurrences,
                    &mut membership,
                )?;
            }
            pages.push(page_record(
                &pages,
                "events",
                Some(listed_series.id.clone()),
                url.clone(),
                cursor.clone(),
                next_cursor.clone(),
                &page,
                events.len(),
            )?);
            walked += 1;
            match next_cursor {
                None => break,
                Some(next) => {
                    if !seen.insert(next.clone()) {
                        return anomaly(&url, "next_cursor repeats");
                    }
                    cursor = Some(next);
                }
            }
        }
        info!(
            series = %listed_series.id,
            pages = walked,
            "market-scope: series walked"
        );
    }
    let condition_ids: Vec<String> = membership.kept.into_iter().collect();
    let digest = format!("{:x}", Sha256::digest(condition_ids.join("\n").as_bytes()));
    Ok(MarketScope {
        version: 1,
        source_id: SOURCE_ID,
        schema_version: 1,
        parser_version: 1,
        recurrence: recurrences.join(","),
        end_date_min_unix,
        started_at_unix,
        completed_at_unix: OffsetDateTime::now_utc().unix_timestamp(),
        series,
        excluded_series,
        excluded_events: membership.excluded,
        pages,
        count: u64::try_from(condition_ids.len()).map_err(|_| BootstrapError::Internal)?,
        condition_ids,
        digest,
    })
}

/// Each pinned series read by id, in id order; its page joins the scope's provenance. A series that
/// is missing, renamed or no longer of a requested recurrence fails the stage temporarily (the loop
/// retries; a lasting change is the owner's research decision).
async fn pinned_series<F: ScopeFetcher + Sync>(
    fetcher: &F,
    base_url: &str,
    pinned: &[ScopeSeries],
    recurrences: &[String],
    retry_base: Duration,
    pages: &mut Vec<ScopePage>,
) -> Result<BTreeMap<u64, ScopeSeries>, BootstrapError> {
    let mut wanted = BTreeMap::new();
    for series in pinned {
        let number = series
            .id
            .parse::<u64>()
            .map_err(|_| BootstrapError::Internal)?;
        wanted.insert(number, series.clone());
    }
    for (number, want) in &wanted {
        let url = format!("{base_url}/series/{}", want.id);
        let changed = |reason: &str| BootstrapError::TransientSource {
            source_name: "gamma-series",
            message: format!("market-scope: pinned series {} {reason}", want.id),
        };
        let page = match fetch_with_retry(fetcher, &url, "series", retry_base).await {
            Err(BootstrapError::Gamma { message }) => return Err(changed(&message)),
            fetched => fetched?,
        };
        let row: Value = parse_page(&url, &page.body)?;
        if decimal_id(row.get("id")).map(|(value, _)| value) != Some(*number) {
            return Err(changed("is answered under another id"));
        }
        if row.get("slug").and_then(Value::as_str) != want.slug.as_deref() {
            return Err(changed("has another slug"));
        }
        if !row
            .get("recurrence")
            .and_then(Value::as_str)
            .is_some_and(|value| recurrences.iter().any(|requested| requested == value))
        {
            return Err(changed("has no requested recurrence"));
        }
        pages.push(page_record(
            pages,
            "series",
            Some(want.id.clone()),
            url,
            None,
            None,
            &page,
            1,
        )?);
    }
    Ok(wanted)
}

/// One complete `/series` listing, keeping the requested recurrences' series; its pages join the
/// scope's provenance.
async fn list_series<F: ScopeFetcher + Sync>(
    fetcher: &F,
    base_url: &str,
    recurrences: &[String],
    retry_base: Duration,
    pages: &mut Vec<ScopePage>,
) -> Result<BTreeMap<u64, ScopeSeries>, BootstrapError> {
    let mut series = BTreeMap::new();
    for page_index in 0..SERIES_MAX_PAGES {
        let offset = page_index * SERIES_PAGE_LIMIT;
        let url = format!(
            "{base_url}/series?exclude_events=true&limit={SERIES_PAGE_LIMIT}&offset={offset}"
        );
        let page = fetch_with_retry(fetcher, &url, "series", retry_base).await?;
        let rows: Vec<Value> = parse_page(&url, &page.body)?;
        for row in &rows {
            if !row.is_object() {
                return anomaly(&url, "series row is not an object");
            }
            if !row
                .get("recurrence")
                .and_then(Value::as_str)
                .is_some_and(|value| recurrences.iter().any(|requested| requested == value))
            {
                continue;
            }
            let (number, id) = decimal_id(row.get("id"))
                .ok_or_else(|| anomaly_error(&url, "requested series id is not decimal"))?;
            let slug = row.get("slug").and_then(Value::as_str).map(str::to_owned);
            series.entry(number).or_insert(ScopeSeries { id, slug });
        }
        let rows_len = rows.len();
        pages.push(page_record(
            pages, "series", None, url, None, None, &page, rows_len,
        )?);
        if rows_len < SERIES_PAGE_LIMIT {
            return Ok(series);
        }
    }
    Err(BootstrapError::Gamma {
        message: format!("market-scope: series listing exceeded {SERIES_MAX_PAGES} pages"),
    })
}

/// Label one event's markets after proving it belongs to a requested, walked series whose embedded
/// slug passes gate F's rule. Gate F reads exactly this first embedded series, so a disagreement with
/// the kept series fails the walk temporarily (exit 75), as a changed by-id slug does, rather than
/// stopping the loop or silently shrinking the scope. The event's markets are validated whatever its
/// own slug; an event whose own slug fails the rule is recorded with its markets instead of keeping
/// them (#892), and needs a decimal id to be recorded.
fn collect_event(
    url: &str,
    series_id: &str,
    event: &Value,
    recurrences: &[String],
    membership: &mut ScopeMembership,
) -> Result<(), BootstrapError> {
    let first_series = event
        .get("series")
        .and_then(Value::as_array)
        .and_then(|series| series.first());
    if first_series
        .and_then(|series| decimal_id(series.get("id")))
        .map(|(_, id)| id)
        != Some(series_id.to_owned())
    {
        return anomaly(url, "event's first series is not the requested series");
    }
    if !event_series_recurrence(event)
        .is_some_and(|value| recurrences.iter().any(|requested| requested == value))
    {
        return anomaly(
            url,
            "event's first series does not have a requested recurrence",
        );
    }
    let embedded_slug = first_series
        .and_then(|series| series.get("slug"))
        .and_then(Value::as_str);
    if let Some(reason) = series_exclusion(embedded_slug) {
        return Err(BootstrapError::TransientSource {
            source_name: "gamma-events",
            message: format!(
                "market-scope {url}: event {} of series {series_id} embeds a series slug {embedded_slug:?} \
                 that gate F refuses ({reason:?})",
                event.get("id").unwrap_or(&Value::Null)
            ),
        });
    }
    let markets = event
        .get("markets")
        .and_then(Value::as_array)
        .ok_or_else(|| anomaly_error(url, "event has no markets array"))?;
    let mut condition_ids = Vec::with_capacity(markets.len());
    for market in markets {
        let condition_id = market
            .get("conditionId")
            .and_then(Value::as_str)
            .filter(|value| is_condition_id(value))
            .ok_or_else(|| anomaly_error(url, "market conditionId is not 0x + 64 lowercase hex"))?;
        condition_ids.push(condition_id.to_owned());
    }
    let slug = event.get("slug").and_then(Value::as_str);
    let Some(reason) = event_exclusion(slug) else {
        // A kept event needs no id; a conflict names whatever it has.
        let kept_event = decimal_id(event.get("id")).map_or_else(
            || {
                event
                    .get("id")
                    .map_or_else(|| "(no id)".to_owned(), Value::to_string)
            },
            |(number, _)| number.to_string(),
        );
        for condition_id in condition_ids {
            membership.keep(condition_id, &kept_event, url)?;
        }
        return Ok(());
    };
    let (number, _) = decimal_id(event.get("id"))
        .ok_or_else(|| anomaly_error(url, "an excluded event has no decimal id"))?;
    condition_ids.sort();
    condition_ids.dedup();
    membership.exclude(
        ExcludedEvent {
            series_id: series_id.to_owned(),
            event_id: number.to_string(),
            slug: slug.map(str::to_owned),
            reason,
            condition_ids,
        },
        url,
    )
}

async fn fetch_with_retry<F: ScopeFetcher>(
    fetcher: &F,
    url: &str,
    endpoint_kind: &'static str,
    retry_base: Duration,
) -> Result<FetchedPage, BootstrapError> {
    let mut retries = 0u32;
    loop {
        match fetcher.fetch(url, endpoint_kind, SOURCE_ID).await {
            Ok(page) => return Ok(page),
            Err(SourceError::Fatal { message }) => {
                return Err(BootstrapError::Gamma {
                    message: format!("market-scope {url}: {message}"),
                });
            }
            Err(error) => {
                if retries >= PAGE_MAX_RETRIES {
                    return Err(BootstrapError::TransientSource {
                        source_name: "gamma-market-scope",
                        message: format!("{url}: {error} (after {retries} page retries)"),
                    });
                }
                let wait = match &error {
                    SourceError::RateLimited { retry_after_secs } => {
                        Duration::from_secs(u64::from(*retry_after_secs).max(1))
                    }
                    _ => crate::events::events_retry_backoff(retry_base, retries),
                };
                retries += 1;
                warn!(
                    url,
                    error = %error,
                    attempt = retries,
                    backoff = ?wait,
                    "market-scope: transient fetch error — retrying page"
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// The keyset events URL for one series page.
fn events_url(
    base_url: &str,
    series_id: &str,
    floor: &str,
    cursor: Option<&str>,
) -> Result<String, BootstrapError> {
    let mut url = reqwest::Url::parse(&format!("{base_url}/events/keyset")).map_err(|error| {
        BootstrapError::Invalid {
            message: format!("market-scope: gamma base URL: {error}"),
        }
    })?;
    url.query_pairs_mut()
        .append_pair("series_id", series_id)
        .append_pair("end_date_min", floor)
        .append_pair("limit", &EVENTS_PAGE_LIMIT.to_string());
    if let Some(cursor) = cursor {
        url.query_pairs_mut().append_pair("after_cursor", cursor);
    }
    Ok(url.into())
}

#[allow(clippy::too_many_arguments)]
fn page_record(
    pages: &[ScopePage],
    kind: &'static str,
    series_id: Option<String>,
    url: String,
    request_cursor: Option<String>,
    returned_next_cursor: Option<String>,
    page: &FetchedPage,
    rows: usize,
) -> Result<ScopePage, BootstrapError> {
    Ok(ScopePage {
        ordinal: u64::try_from(pages.len()).map_err(|_| BootstrapError::Internal)?,
        kind,
        series_id,
        url,
        request_cursor,
        returned_next_cursor,
        raw_sha256: format!("{:x}", Sha256::digest(&page.body)),
        rows: u64::try_from(rows).map_err(|_| BootstrapError::Internal)?,
        observed_at_unix: page.observed_at_unix,
        received_at_unix: page.received_at_unix,
        source_at_unix: page.source_at_unix,
    })
}

fn parse_page<T: serde::de::DeserializeOwned>(url: &str, body: &[u8]) -> Result<T, BootstrapError> {
    serde_json::from_slice(body).map_err(|error| anomaly_error(url, &error.to_string()))
}

/// A series id given as a decimal string or a non-negative integer, as `(number, text)`.
fn decimal_id(value: Option<&Value>) -> Option<(u64, String)> {
    let text = match value? {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.as_u64()?.to_string(),
        _ => return None,
    };
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((text.parse().ok()?, text))
}

fn is_condition_id(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..]
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn anomaly_error(url: &str, message: &str) -> BootstrapError {
    BootstrapError::Gamma {
        message: format!("market-scope {url}: {message}"),
    }
}

fn anomaly<T>(url: &str, message: &str) -> Result<T, BootstrapError> {
    Err(anomaly_error(url, message))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;

    use super::*;

    const BASE: &str = "http://gamma.test";
    const FLOOR: i64 = 1_776_384_000;
    const FLOOR_ISO: &str = "2026-04-17T00:00:00Z";

    /// Serves each request from a function of its URL and call count.
    struct FnFetcher<G>(G, AtomicUsize);

    impl<G: Fn(&str, usize) -> Result<Vec<u8>, SourceError> + Sync> ScopeFetcher for FnFetcher<G> {
        async fn fetch(
            &self,
            url: &str,
            _endpoint_kind: &'static str,
            source_id: &'static str,
        ) -> Result<FetchedPage, SourceError> {
            assert_eq!(source_id, SOURCE_ID);
            let call = self.1.fetch_add(1, Ordering::SeqCst);
            Ok(FetchedPage {
                body: (self.0)(url, call)?,
                observed_at_unix: 990 + i64::try_from(call).unwrap(),
                received_at_unix: 1_000 + i64::try_from(call).unwrap(),
                source_at_unix: Some(900),
            })
        }
    }

    fn fetcher<G: Fn(&str, usize) -> Result<Vec<u8>, SourceError> + Sync>(
        serve: G,
    ) -> FnFetcher<G> {
        FnFetcher(serve, AtomicUsize::new(0))
    }

    fn condition(n: u8) -> String {
        format!("0x{}", format!("{n:02x}").repeat(32))
    }

    /// One keyset event whose first embedded series and own slug are usable production slugs, as Gamma serves it.
    fn event(series_id: &str, recurrence: &str, markets: &[String]) -> Value {
        json!({
            "id": "e",
            "slug": format!("series-{series_id}-event"),
            "series": [{"id": series_id, "slug": format!("series-{series_id}"), "recurrence": recurrence}],
            "markets": markets.iter().map(|id| json!({"conditionId": id})).collect::<Vec<_>>(),
        })
    }

    fn series_url(offset: usize) -> String {
        format!("{BASE}/series?exclude_events=true&limit=50&offset={offset}")
    }

    /// Two 15-minute series (ids "11" and 2) among 52 rows over two series pages; series 2 walks
    /// two keyset pages, series 11 one.
    fn serve_ok(url: &str, _call: usize) -> Result<Vec<u8>, SourceError> {
        let body = if url == series_url(0) {
            let mut rows: Vec<Value> = (0..48)
                .map(|n| json!({"id": format!("{}", 100 + n), "recurrence": "daily"}))
                .collect();
            rows.push(json!({"id": "11", "slug": "eth-up-or-down-15m", "recurrence": "15m"}));
            rows.push(json!({"id": 2, "slug": "btc-up-or-down-15m", "recurrence": "15m"}));
            json!(rows)
        } else if url == series_url(50) {
            json!([{"id": "300", "recurrence": null}, {"id": "11", "recurrence": "15m"}])
        } else if url == events_url(BASE, "2", FLOOR_ISO, None).unwrap() {
            json!({"events": [event("2", "15m", &[condition(3)]), event("2", "15m", &[condition(1)])],
                   "next_cursor": "c1"})
        } else if url == events_url(BASE, "2", FLOOR_ISO, Some("c1")).unwrap() {
            json!({"events": [event("2", "15m", &[])]})
        } else if url == events_url(BASE, "11", FLOOR_ISO, None).unwrap() {
            json!({"events": [event("11", "15m", &[condition(2), condition(1)])], "next_cursor": null})
        } else {
            panic!("unexpected URL {url}");
        };
        Ok(serde_json::to_vec(&body).unwrap())
    }

    async fn sweep<G: Fn(&str, usize) -> Result<Vec<u8>, SourceError> + Sync>(
        serve: G,
    ) -> Result<MarketScope, BootstrapError> {
        sweep_market_scope(
            &fetcher(serve),
            BASE,
            FLOOR,
            &BootstrapConfig::default().market_recurrences,
            None,
            Duration::ZERO,
        )
        .await
    }

    async fn sweep_pinned<G: Fn(&str, usize) -> Result<Vec<u8>, SourceError> + Sync>(
        serve: G,
        pin: &str,
    ) -> (Result<MarketScope, BootstrapError>, usize) {
        sweep_pinned_with(serve, pin, &BootstrapConfig::default().market_recurrences).await
    }

    async fn sweep_pinned_with<G: Fn(&str, usize) -> Result<Vec<u8>, SourceError> + Sync>(
        serve: G,
        pin: &str,
        recurrences: &[String],
    ) -> (Result<MarketScope, BootstrapError>, usize) {
        let fetcher = fetcher(serve);
        let pinned = parse_pinned_series(pin).unwrap();
        let scope = sweep_market_scope(
            &fetcher,
            BASE,
            FLOOR,
            recurrences,
            Some(&pinned),
            Duration::ZERO,
        )
        .await;
        (scope, fetcher.1.load(Ordering::SeqCst))
    }

    /// The pinned series by id, as Gamma answers `/series/{id}`; a catalog listing request fails the test; anything
    /// else is `serve_ok`'s.
    fn serve_pinned(url: &str, call: usize) -> Result<Vec<u8>, SourceError> {
        let row = match url.strip_prefix(&format!("{BASE}/series/")) {
            Some("2") => json!({"id": 2, "slug": "btc-up-or-down-15m", "recurrence": "15m"}),
            Some("11") => json!({"id": "11", "slug": "eth-up-or-down-15m", "recurrence": "15m"}),
            Some("12") => json!({"id": "12", "slug": "sol-up-or-down-5m", "recurrence": "5m"}),
            Some(_) => {
                return Err(SourceError::Fatal {
                    message: "HTTP 404".to_owned(),
                });
            }
            None if url.starts_with(&format!("{BASE}/series?")) => {
                panic!("a pinned scope requested the catalog listing: {url}")
            }
            None if url == events_url(BASE, "12", FLOOR_ISO, None).unwrap() => {
                json!({"events": [event("12", "5m", &[condition(4)])]})
            }
            None => return serve_ok(url, call),
        };
        Ok(serde_json::to_vec(&row).unwrap())
    }

    /// FAIL: a pin of both recurrences (the configuration after 11/30) loses a pinned series that the catalog listing
    /// omits, or consults the listing at all.
    #[tokio::test]
    async fn a_pin_of_both_recurrences_keeps_a_series_the_catalog_omits() {
        let both = ["15m".to_owned(), "5m".to_owned()];
        // Series 12 (5m) is absent from serve_ok's catalog pages; the pin reads it by id.
        let (scope, _) = sweep_pinned_with(
            serve_pinned,
            "12:sol-up-or-down-5m,2:btc-up-or-down-15m",
            &both,
        )
        .await;
        let scope = scope.unwrap();
        let ids: Vec<&str> = scope
            .series
            .iter()
            .map(|series| series.id.as_str())
            .collect();
        assert_eq!(ids, ["2", "12"]);
        assert_eq!(scope.recurrence, "15m,5m");
        assert!(
            scope
                .pages
                .iter()
                .all(|page| page.kind != "series" || page.series_id.is_some()),
            "a pinned scope never lists the catalog"
        );
        assert_eq!(
            scope.condition_ids,
            vec![condition(1), condition(3), condition(4)]
        );
    }

    /// FAIL: a pinned scope lists the catalog, keeps a series outside the pin, or accepts a
    /// pinned series that is missing, renamed or of another recurrence.
    #[tokio::test]
    async fn pinned_series_are_read_by_id_exactly() {
        let (scope, _) =
            sweep_pinned(serve_pinned, "11:eth-up-or-down-15m,2:btc-up-or-down-15m").await;
        let scope = scope.unwrap();
        let ids = |scope: &MarketScope| -> Vec<String> {
            scope
                .series
                .iter()
                .map(|series| series.id.clone())
                .collect()
        };
        assert_eq!(ids(&scope), vec!["2".to_owned(), "11".to_owned()]);
        let reads: Vec<(Option<&str>, &str)> = scope
            .pages
            .iter()
            .filter(|page| page.kind == "series")
            .map(|page| (page.series_id.as_deref(), page.url.as_str()))
            .collect();
        assert_eq!(
            reads,
            vec![
                (Some("2"), "http://gamma.test/series/2"),
                (Some("11"), "http://gamma.test/series/11")
            ]
        );
        assert_eq!(
            scope.condition_ids,
            vec![condition(1), condition(2), condition(3)]
        );
        // The pin keeps series 2 only; series 11's markets stay out.
        let (scope, _) = sweep_pinned(serve_pinned, "2:btc-up-or-down-15m").await;
        assert_eq!(
            scope.unwrap().condition_ids,
            vec![condition(1), condition(3)]
        );
        // Renamed, missing or of another recurrence: a temporary failure after one read.
        for pin in [
            "11:eth-15m",
            "13:doge-up-or-down-15m",
            "12:sol-up-or-down-5m",
        ] {
            let (error, calls) = sweep_pinned(serve_pinned, pin).await;
            let error = error.unwrap_err();
            assert_eq!(
                error.exit_code(),
                BootstrapError::TEMPFAIL_EXIT_CODE,
                "{pin}: {error}"
            );
            assert_eq!(calls, 1, "{pin}");
        }
    }

    /// FAIL: a malformed or duplicated pin is accepted.
    #[test]
    fn pinned_series_parse_only_decimal_ids_with_slugs_once() {
        assert_eq!(
            parse_pinned_series("10192:btc-up-or-down-15m,12518:zec-up-or-down-15m")
                .unwrap()
                .len(),
            2
        );
        for bad in ["", "10192", "10192:", "x:btc", "010192:btc", "1:a,1:b"] {
            assert!(parse_pinned_series(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn sweeps_series_and_walks_each_keyset_chain() {
        let scope = sweep(serve_ok).await.unwrap();
        assert_eq!(
            scope.series,
            vec![
                ScopeSeries {
                    id: "2".to_owned(),
                    slug: Some("btc-up-or-down-15m".to_owned())
                },
                ScopeSeries {
                    id: "11".to_owned(),
                    slug: Some("eth-up-or-down-15m".to_owned())
                },
            ]
        );
        assert_eq!(
            scope.condition_ids,
            vec![condition(1), condition(2), condition(3)]
        );
        assert_eq!(scope.count, 3);
        assert_eq!(
            scope.digest,
            format!(
                "{:x}",
                Sha256::digest(scope.condition_ids.join("\n").as_bytes())
            )
        );
        let shape: Vec<_> = scope
            .pages
            .iter()
            .map(|page| {
                (
                    page.ordinal,
                    page.kind,
                    page.series_id.as_deref(),
                    page.request_cursor.as_deref(),
                    page.returned_next_cursor.as_deref(),
                    page.rows,
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                (0, "series", None, None, None, 50),
                (1, "series", None, None, None, 2),
                (2, "events", Some("2"), None, Some("c1"), 2),
                (3, "events", Some("2"), Some("c1"), None, 1),
                (4, "events", Some("11"), None, None, 1),
            ]
        );
        let first = serve_ok(&series_url(0), 0).unwrap();
        assert_eq!(
            scope.pages[0].raw_sha256,
            format!("{:x}", Sha256::digest(&first))
        );
        assert_eq!(scope.pages[4].observed_at_unix, 994);
        assert_eq!(scope.pages[4].received_at_unix, 1_004);
        assert_eq!(scope.pages[4].source_at_unix, Some(900));
        assert_eq!(scope.end_date_min_unix, FLOOR);
        assert_eq!(
            (scope.version, scope.source_id, scope.recurrence.as_str()),
            (1, "polymarket-gamma", "15m")
        );
        assert!(scope.started_at_unix <= scope.completed_at_unix);
        assert!(
            events_url(BASE, "2", FLOOR_ISO, Some("a=b&c"))
                .unwrap()
                .ends_with("after_cursor=a%3Db%26c")
        );
    }

    #[tokio::test]
    async fn default_scope_file_keeps_the_15m_bytes() {
        let mut scope = sweep(|url: &str, _| {
            let body = if url.contains("/series?") {
                r#"[{"id":"7","slug":"btc-up-or-down-15m","recurrence":"15m"}]"#.to_owned()
            } else {
                format!(
                    r#"{{"events":[{{"id":"70","slug":"btc-updown-15m-1","series":[{{"id":"7","slug":"btc-up-or-down-15m","recurrence":"15m"}}],"markets":[{{"conditionId":"{}"}}]}}]}}"#,
                    condition(0xab)
                )
            };
            Ok(body.into_bytes())
        })
        .await
        .unwrap();
        scope.started_at_unix = 1_000;
        scope.completed_at_unix = 1_001;
        let dir = tempfile::TempDir::new().unwrap();
        let out = dir.path().join("scope.json");
        crate::cache_migration::atomic_write_json(&out, &scope).unwrap();
        assert_eq!(
            std::fs::read(out).unwrap(),
            include_bytes!("../tests/fixtures/market_scope_15m.json")
        );
        // A scope with no exclusions writes no `excluded_series` or `excluded_events` key, so earlier files and
        // readers stay compatible.
        let written = serde_json::to_value(&scope).unwrap();
        assert!(!written.as_object().unwrap().contains_key("excluded_series"));
        assert!(!written.as_object().unwrap().contains_key("excluded_events"));
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn requested_recurrences_select_series_and_events_in_canonical_order() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for requested in [vec!["5m"], vec!["15m", "5m"], vec!["5m", "15m"]] {
            figment::Jail::expect_with(|jail| {
                let calls = fetcher(|url: &str, _| {
                    let value = if url.contains("/series?") {
                        json!([
                            {"id": "2", "slug": "btc-up-or-down-15m", "recurrence": "15m"},
                            {"id": "3", "slug": "btc-up-or-down-5m", "recurrence": "5m"},
                            {"id": "4", "slug": "btc-up-or-down-daily", "recurrence": "daily"}
                        ])
                    } else if url.contains("series_id=2&") {
                        json!({"events": [event("2", "15m", &[condition(2)])]})
                    } else if url.contains("series_id=3&") {
                        json!({"events": [event("3", "5m", &[condition(3)])]})
                    } else {
                        panic!("unexpected URL {url}");
                    };
                    Ok(serde_json::to_vec(&value).unwrap())
                });
                jail.set_env("PE_BOOTSTRAP_OUTPUT", "/tmp/watchlist.json");
                jail.set_env(
                    "PE_BOOTSTRAP_MARKET_RECURRENCES",
                    serde_json::to_string(&requested).unwrap(),
                );
                let config = crate::config::load(None).unwrap();
                let scope = runtime
                    .block_on(sweep_market_scope(
                        &calls,
                        BASE,
                        FLOOR,
                        &config.market_recurrences,
                        None,
                        Duration::ZERO,
                    ))
                    .unwrap();
                let both = requested.len() == 2;
                assert_eq!(scope.recurrence, if both { "15m,5m" } else { "5m" });
                assert_eq!(
                    scope
                        .series
                        .iter()
                        .map(|series| series.id.as_str())
                        .collect::<Vec<_>>(),
                    if both { vec!["2", "3"] } else { vec!["3"] }
                );
                assert_eq!(
                    scope.condition_ids,
                    if both {
                        vec![condition(2), condition(3)]
                    } else {
                        vec![condition(3)]
                    }
                );
                assert_eq!(calls.1.load(Ordering::SeqCst), if both { 3 } else { 2 });
                Ok(())
            });
        }
    }

    #[tokio::test]
    async fn invalid_recurrences_fail_before_fetching() {
        let calls = fetcher(|_: &str, _| panic!("invalid recurrences must not fetch"));
        for requested in [vec![], vec!["hourly"], vec!["15m", "15m"], vec![""]] {
            let recurrences: Vec<String> = requested.into_iter().map(str::to_owned).collect();
            let error = sweep_market_scope(&calls, BASE, FLOOR, &recurrences, None, Duration::ZERO)
                .await
                .unwrap_err();
            assert!(matches!(error, BootstrapError::Invalid { .. }));
        }
        assert_eq!(calls.1.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn every_reply_anomaly_fails_with_exit_one() {
        let first_events = events_url(BASE, "2", FLOOR_ISO, None).unwrap();
        let cases: Vec<(&str, Value)> = vec![
            (
                "wrong series",
                json!({"events": [event("3", "15m", &[condition(1)])]}),
            ),
            (
                "not 15m",
                json!({"events": [event("2", "5m", &[condition(1)])]}),
            ),
            (
                "uppercase id",
                json!({"events": [event("2", "15m", &[condition(0xab).to_uppercase().replace("0X", "0x")])]}),
            ),
            (
                "short id",
                json!({"events": [event("2", "15m", &["0xab".to_owned()])]}),
            ),
            (
                "no markets",
                json!({"events": [{"slug": "btc-updown-15m-1",
                                   "series": [{"id": "2", "slug": "btc-up-or-down-15m", "recurrence": "15m"}]}]}),
            ),
            ("no series", json!({"events": [{"markets": []}]})),
            ("array reply", json!([event("2", "15m", &[condition(1)])])),
            ("empty cursor", json!({"events": [], "next_cursor": ""})),
            ("numeric cursor", json!({"events": [], "next_cursor": 7})),
        ];
        for (name, reply) in cases {
            let first_events = first_events.clone();
            let error = sweep(move |url: &str, call| {
                if url == first_events {
                    Ok(serde_json::to_vec(&reply).unwrap())
                } else if url.contains("/series") {
                    serve_ok(url, call)
                } else {
                    Ok(serde_json::to_vec(&json!({"events": []})).unwrap())
                }
            })
            .await
            .expect_err(name);
            assert_eq!(error.exit_code(), 1, "{name}: {error}");
        }
    }

    #[tokio::test]
    async fn listing_and_label_anomalies_fail_with_exit_one() {
        let no_series = |url: &str, _| {
            assert!(url.contains("/series"), "{url}");
            Ok(serde_json::to_vec(&json!([{"id": "1", "recurrence": "hourly"}])).unwrap())
        };
        let bad_row = |_: &str, _| Ok(serde_json::to_vec(&json!(["15m"])).unwrap());
        let bad_id = |_: &str, _| {
            Ok(serde_json::to_vec(&json!([{"id": "1a", "recurrence": "15m"}])).unwrap())
        };
        let no_labels = |url: &str, call| {
            if url.contains("/series") {
                serve_ok(url, call)
            } else {
                let series = if url.contains("series_id=11") {
                    "11"
                } else {
                    "2"
                };
                Ok(serde_json::to_vec(&json!({"events": [event(series, "15m", &[])]})).unwrap())
            }
        };
        for error in [
            sweep(no_series).await.unwrap_err(),
            sweep(bad_row).await.unwrap_err(),
            sweep(bad_id).await.unwrap_err(),
            require_markets(&sweep(no_labels).await.unwrap()).unwrap_err(),
        ] {
            assert_eq!(error.exit_code(), 1, "{error}");
        }
    }

    /// A newly listed series with no events yet sweeps to an empty scope that keeps its pages; only a caller
    /// that needs markets (the market-scope stage, discovery's covered sweep) turns that into exit 1.
    #[tokio::test]
    async fn empty_series_sweep_keeps_its_pages_and_only_require_markets_fails() {
        let serve = |url: &str, _| {
            let body = if url == series_url(0) {
                json!([{"id": "13100", "slug": "newcoin-up-or-down-15m", "recurrence": "15m"}])
            } else if url == events_url(BASE, "13100", FLOOR_ISO, None).unwrap() {
                json!({"events": [], "next_cursor": null})
            } else {
                panic!("unexpected URL {url}");
            };
            Ok(serde_json::to_vec(&body).unwrap())
        };
        let scope = sweep(serve).await.unwrap();
        assert!(scope.condition_ids.is_empty());
        assert_eq!(scope.count, 0);
        assert_eq!(scope.digest, format!("{:x}", Sha256::digest(b"")));
        assert_eq!(
            scope.series,
            vec![ScopeSeries {
                id: "13100".into(),
                slug: Some("newcoin-up-or-down-15m".into()),
            }]
        );
        assert_eq!(
            scope.pages.len(),
            2,
            "the listing page and the empty events page"
        );
        let error = require_markets(&scope).unwrap_err();
        assert_eq!(error.exit_code(), 1);
        assert_eq!(error.to_string(), no_markets().to_string());

        // The market-scope stage refuses it before writing: no new file, and an existing one is untouched.
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh.json");
        assert_eq!(
            write_market_scope(&fresh, scope.clone())
                .unwrap_err()
                .exit_code(),
            1
        );
        assert!(!fresh.exists());
        let existing = dir.path().join("market_scope.json");
        std::fs::write(&existing, b"prior scope").unwrap();
        assert_eq!(
            write_market_scope(&existing, scope.clone())
                .unwrap_err()
                .exit_code(),
            1
        );
        assert_eq!(std::fs::read(&existing).unwrap(), b"prior scope");
        let mut listed = scope;
        listed.condition_ids = vec![condition(1)];
        write_market_scope(&existing, listed.clone()).unwrap();
        let written: Value = serde_json::from_slice(&std::fs::read(&existing).unwrap()).unwrap();
        assert_eq!(written["condition_ids"], json!(listed.condition_ids));
    }

    #[tokio::test]
    async fn runaway_walks_stop_at_their_caps() {
        let full_listing = |_: &str, _| {
            let rows: Vec<Value> = (0..50).map(|n| json!({"id": format!("{n}")})).collect();
            Ok(serde_json::to_vec(&rows).unwrap())
        };
        assert_eq!(sweep(full_listing).await.unwrap_err().exit_code(), 1);

        let endless = |url: &str, call: usize| {
            if url.contains("/series") {
                return Ok(serde_json::to_vec(
                    &json!([{"id": "2", "slug": "btc-up-or-down-15m", "recurrence": "15m"}]),
                )
                .unwrap());
            }
            Ok(
                serde_json::to_vec(&json!({"events": [event("2", "15m", &[condition(1)])],
                                          "next_cursor": format!("c{call}")}))
                .unwrap(),
            )
        };
        let calls = fetcher(endless);
        let error = sweep_market_scope(
            &calls,
            BASE,
            FLOOR,
            &BootstrapConfig::default().market_recurrences,
            None,
            Duration::ZERO,
        )
        .await
        .unwrap_err();
        assert_eq!(error.exit_code(), 1);
        assert_eq!(
            calls.1.load(Ordering::SeqCst),
            1 + EVENTS_MAX_PAGES_PER_SERIES
        );

        let repeating = |url: &str, _| {
            if url.contains("/series") {
                return Ok(serde_json::to_vec(
                    &json!([{"id": "2", "slug": "btc-up-or-down-15m", "recurrence": "15m"}]),
                )
                .unwrap());
            }
            Ok(serde_json::to_vec(&json!({"events": [], "next_cursor": "same"})).unwrap())
        };
        assert_eq!(sweep(repeating).await.unwrap_err().exit_code(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_and_rate_limited_pages_retry_then_exit_75() {
        let started = tokio::time::Instant::now();
        let limited = |_: &str, _| {
            Err(SourceError::RateLimited {
                retry_after_secs: 3,
            })
        };
        let error = sweep(limited).await.unwrap_err();
        assert_eq!(
            error.exit_code(),
            BootstrapError::TEMPFAIL_EXIT_CODE,
            "{error}"
        );
        assert!(
            started.elapsed() >= Duration::from_secs(15),
            "{:?}",
            started.elapsed()
        );

        let started = tokio::time::Instant::now();
        let zero = |_: &str, _| {
            Err(SourceError::RateLimited {
                retry_after_secs: 0,
            })
        };
        assert_eq!(sweep(zero).await.unwrap_err().exit_code(), 75);
        assert!(started.elapsed() >= Duration::from_secs(5));

        let transient = |_: &str, _| {
            Err(SourceError::Transient {
                message: "HTTP 503".to_owned(),
            })
        };
        assert_eq!(sweep(transient).await.unwrap_err().exit_code(), 75);

        let recovers = |url: &str, call| {
            if call < 2 {
                Err(SourceError::Transient {
                    message: "reset".to_owned(),
                })
            } else {
                serve_ok(url, call)
            }
        };
        assert_eq!(sweep(recovers).await.unwrap().count, 3);
    }

    #[tokio::test]
    async fn fatal_replies_exit_one() {
        let fatal = |_: &str, _| {
            Err(SourceError::Fatal {
                message: "HTTP 422".to_owned(),
            })
        };
        assert_eq!(sweep(fatal).await.unwrap_err().exit_code(), 1);
    }

    /// Captures JSON log lines for assertions on warnings.
    #[derive(Clone, Default)]
    struct LogCapture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for LogCapture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl LogCapture {
        fn lines(&self) -> Vec<Value> {
            let bytes = self.0.lock().unwrap();
            std::str::from_utf8(&bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect()
        }
    }

    fn excluded(id: &str, slug: Option<&str>, reason: ExclusionReason) -> ExcludedSeries {
        ExcludedSeries {
            id: id.into(),
            slug: slug.map(str::to_owned),
            reason,
        }
    }

    /// Gate F's slug rule (#871): a usable slug outside Polymarket's test namespace is kept; a test slug, or an
    /// absent, null, non-string or blank one (admission's `str::trim` rule), is excluded.
    #[test]
    fn series_exclusion_mirrors_gate_f() {
        for (slug, expected) in [
            (Some("btc-up-or-down-15m"), None),
            (
                Some("polyv2-soak-xrp-up-or-down-15m"),
                Some(ExclusionReason::TestSeries),
            ),
            (Some("polyv2bkbsl"), Some(ExclusionReason::TestSeries)),
            (None, Some(ExclusionReason::NoSlug)),
            (Some(""), Some(ExclusionReason::NoSlug)),
            (Some("  "), Some(ExclusionReason::NoSlug)),
        ] {
            assert_eq!(series_exclusion(slug), expected, "{slug:?}");
        }
        let (kept, dropped) = partition_series([
            ScopeSeries {
                id: "13005".into(),
                slug: Some("polyv2-soak-btc-up-or-down-15m".into()),
            },
            ScopeSeries {
                id: "10192".into(),
                slug: Some("btc-up-or-down-15m".into()),
            },
            ScopeSeries {
                id: "9".into(),
                slug: None,
            },
        ]);
        assert_eq!(
            kept,
            [ScopeSeries {
                id: "10192".into(),
                slug: Some("btc-up-or-down-15m".into()),
            }]
        );
        assert_eq!(
            dropped,
            [
                excluded(
                    "13005",
                    Some("polyv2-soak-btc-up-or-down-15m"),
                    ExclusionReason::TestSeries
                ),
                excluded("9", None, ExclusionReason::NoSlug),
            ]
        );
    }

    /// FAIL: a listed test or slugless series of a requested recurrence is read, walked or left unrecorded, a row of
    /// another recurrence is recorded (recurrence comes first, as in gate F), or no warning names the slugless one.
    #[tokio::test]
    async fn listing_excludes_test_and_slugless_series_without_reading_them() {
        let log = LogCapture::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let scope = sweep(|url: &str, _| {
            let body = if url == series_url(0) {
                json!([
                    {"id": "2", "slug": "btc-up-or-down-15m", "recurrence": "15m"},
                    {"id": "13005", "slug": "polyv2-soak-btc-up-or-down-15m", "recurrence": "15m"},
                    {"id": "14", "slug": null, "recurrence": "15m"},
                    {"id": "15", "slug": 15, "recurrence": "15m"},
                    {"id": "16", "slug": "  ", "recurrence": "15m"},
                    {"id": "13006", "slug": "polyv2-soak-btc-up-or-down-5m", "recurrence": "5m"}
                ])
            } else if url == events_url(BASE, "2", FLOOR_ISO, None).unwrap() {
                json!({"events": [event("2", "15m", &[condition(1)])]})
            } else {
                panic!("an excluded series must not be read or walked: {url}");
            };
            Ok(serde_json::to_vec(&body).unwrap())
        })
        .await
        .unwrap();
        assert_eq!(
            scope.series,
            [ScopeSeries {
                id: "2".into(),
                slug: Some("btc-up-or-down-15m".into()),
            }]
        );
        assert_eq!(
            scope.excluded_series,
            [
                excluded("14", None, ExclusionReason::NoSlug),
                excluded("15", None, ExclusionReason::NoSlug),
                excluded("16", Some("  "), ExclusionReason::NoSlug),
                excluded(
                    "13005",
                    Some("polyv2-soak-btc-up-or-down-15m"),
                    ExclusionReason::TestSeries
                ),
            ]
        );
        assert_eq!(scope.condition_ids, [condition(1)]);
        let written = serde_json::to_value(&scope).unwrap();
        assert_eq!(written["excluded_series"][3]["reason"], "test_series");
        assert_eq!(written["excluded_series"][0]["reason"], "no_slug");
        let warned: Vec<String> = log
            .lines()
            .into_iter()
            .filter(|line| line["level"] == "WARN")
            .map(|line| line["fields"]["series"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(warned, ["14", "15", "16"]);
    }

    /// FAIL: a pinned test series is read by id or walked, its exclusion is not recorded, the written file breaks the
    /// reader's one-read-per-series rule, or an all-test pin produces a scope.
    #[tokio::test]
    async fn pinned_test_series_are_recorded_and_never_read() {
        let (scope, calls) = sweep_pinned(
            |url: &str, call| {
                assert!(
                    !url.contains("13005"),
                    "an excluded pinned series must not be read or walked: {url}"
                );
                serve_pinned(url, call)
            },
            "13005:polyv2-soak-btc-up-or-down-15m,2:btc-up-or-down-15m",
        )
        .await;
        let scope = scope.unwrap();
        assert_eq!(
            scope.series,
            [ScopeSeries {
                id: "2".into(),
                slug: Some("btc-up-or-down-15m".into()),
            }]
        );
        assert_eq!(
            scope.excluded_series,
            [excluded(
                "13005",
                Some("polyv2-soak-btc-up-or-down-15m"),
                ExclusionReason::TestSeries
            )]
        );
        let reads: Vec<&str> = scope
            .pages
            .iter()
            .filter(|page| page.kind == "series")
            .map(|page| page.url.as_str())
            .collect();
        assert_eq!(reads, [format!("{BASE}/series/2")]);
        assert_eq!(calls, 3, "one by-id read and series 2's two keyset pages");

        let (all_test, calls) = sweep_pinned(
            |url: &str, _| panic!("an all-test pin must not fetch: {url}"),
            "13005:polyv2-soak-btc-up-or-down-15m",
        )
        .await;
        assert_eq!(all_test.unwrap_err().exit_code(), 1);
        assert_eq!(calls, 0);
    }

    /// FAIL: a kept series' event whose first embedded series carries a test or unusable slug is recorded, or fails any
    /// way other than a temporary failure (exit 75) that names the event; a production embedded slug fails the walk.
    #[tokio::test]
    async fn embedded_test_or_unusable_slug_fails_the_walk_temporarily() {
        let first_events = events_url(BASE, "2", FLOOR_ISO, None).unwrap();
        for embedded in [
            json!({"id": "2", "recurrence": "15m"}),
            json!({"id": "2", "recurrence": "15m", "slug": null}),
            json!({"id": "2", "recurrence": "15m", "slug": 15}),
            json!({"id": "2", "recurrence": "15m", "slug": ""}),
            json!({"id": "2", "recurrence": "15m", "slug": "  "}),
            json!({"id": "2", "recurrence": "15m", "slug": "polyv2-soak-btc-up-or-down-15m"}),
            json!({"id": "2", "recurrence": "15m", "slug": "polyv2bkbsl"}),
        ] {
            let first_events = first_events.clone();
            let reply = json!({"events": [{"id": "e9", "series": [embedded.clone()],
                                           "markets": [{"conditionId": condition(1)}]}]});
            let error = sweep(move |url: &str, call| {
                if url == first_events {
                    Ok(serde_json::to_vec(&reply).unwrap())
                } else {
                    serve_ok(url, call)
                }
            })
            .await
            .unwrap_err();
            assert_eq!(error.exit_code(), 75, "{embedded}: {error}");
            assert!(
                matches!(error, BootstrapError::TransientSource { .. }),
                "{error}"
            );
            assert!(error.to_string().contains("\"e9\""), "{error}");
        }
        let healthy = sweep(serve_ok).await.unwrap();
        assert_eq!(
            healthy.condition_ids,
            [condition(1), condition(2), condition(3)]
        );
    }

    /// A kept-series keyset event with an id and an own slug of any JSON shape.
    fn named_event(series_id: &str, id: Value, slug: Value, markets: &[String]) -> Value {
        json!({
            "id": id,
            "slug": slug,
            "series": [{"id": series_id, "slug": format!("series-{series_id}"), "recurrence": "15m"}],
            "markets": markets.iter().map(|id| json!({"conditionId": id})).collect::<Vec<_>>(),
        })
    }

    /// Sweep a catalog of the named 15-minute series, serving each events page by series and request cursor.
    async fn sweep_pages(
        pages: Vec<(&'static str, Option<&'static str>, Value)>,
    ) -> Result<MarketScope, BootstrapError> {
        let listing: Vec<Value> = pages
            .iter()
            .map(|(series_id, _, _)| *series_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|id| json!({"id": id, "slug": format!("series-{id}"), "recurrence": "15m"}))
            .collect();
        sweep(move |url: &str, _| {
            if url.contains("/series?") {
                return Ok(serde_json::to_vec(&listing).unwrap());
            }
            let (_, _, body) = pages
                .iter()
                .find(|(series_id, cursor, _)| {
                    url == events_url(BASE, series_id, FLOOR_ISO, *cursor).unwrap()
                })
                .unwrap_or_else(|| panic!("unexpected URL {url}"));
            Ok(serde_json::to_vec(body).unwrap())
        })
        .await
    }

    /// FAIL (#892): an event of a kept series whose own slug starts with `polyv2`, or is absent, null, not a string or
    /// blank, adds a condition id, is not recorded with its reason, canonical id, verbatim string slug and sorted
    /// markets, or changes a page's row count or a production event elsewhere.
    #[tokio::test]
    async fn events_with_test_or_unusable_slugs_are_recorded_and_their_markets_left_out() {
        let mut absent = named_event("7", json!(6), json!(null), &[condition(7)]);
        absent.as_object_mut().unwrap().remove("slug");
        let scope = sweep_pages(vec![
            (
                "7",
                None,
                json!({"events": [
                    named_event("7", json!("1"), json!("zec-updown-15m-1"), &[condition(1)]),
                    named_event("7", json!("2"), json!("polyv2zec-updown-15m-2"),
                                &[condition(3), condition(2), condition(3)]),
                    named_event("7", json!("03"), json!("  "), &[condition(4)]),
                    named_event("7", json!(4), json!(null), &[condition(5)]),
                    named_event("7", json!(5), json!(15), &[condition(6)]),
                    absent,
                ], "next_cursor": "c1"}),
            ),
            (
                "7",
                Some("c1"),
                json!({"events": [named_event("7", json!("7"), json!("zec-updown-15m-7"), &[condition(8)])]}),
            ),
        ])
        .await
        .unwrap();
        assert_eq!(scope.condition_ids, [condition(1), condition(8)]);
        assert_eq!(scope.count, 2);
        let recorded: Vec<_> = scope
            .excluded_events
            .iter()
            .map(|e| {
                (
                    e.series_id.as_str(),
                    e.event_id.as_str(),
                    e.slug.as_deref(),
                    e.reason,
                    e.condition_ids.clone(),
                )
            })
            .collect();
        assert_eq!(
            recorded,
            vec![
                (
                    "7",
                    "2",
                    Some("polyv2zec-updown-15m-2"),
                    ExclusionReason::TestEvent,
                    vec![condition(2), condition(3)]
                ),
                (
                    "7",
                    "3",
                    Some("  "),
                    ExclusionReason::NoSlug,
                    vec![condition(4)]
                ),
                ("7", "4", None, ExclusionReason::NoSlug, vec![condition(5)]),
                ("7", "5", None, ExclusionReason::NoSlug, vec![condition(6)]),
                ("7", "6", None, ExclusionReason::NoSlug, vec![condition(7)]),
            ]
        );
        let rows: Vec<u64> = scope
            .pages
            .iter()
            .filter(|page| page.kind == "events")
            .map(|page| page.rows)
            .collect();
        assert_eq!(rows, [6, 1]);
        let written = serde_json::to_value(&scope).unwrap();
        assert_eq!(written["excluded_events"][0]["reason"], "test_event");
        assert_eq!(written["excluded_events"][1]["reason"], "no_slug");
        assert_eq!(written["excluded_events"][1]["slug"], "  ");
        assert_eq!(written["excluded_events"][2]["slug"], Value::Null);
    }

    /// FAIL (#892): a repeated excluded event (one canonical id, "09" and 9 alike) that is identical is recorded twice,
    /// or one that differs in slug, reason, markets or series is accepted.
    #[tokio::test]
    async fn repeated_exclusions_coalesce_only_when_identical() {
        let test =
            |id: Value, slug: &str, markets: &[String]| named_event("7", id, json!(slug), markets);
        let scope = sweep_pages(vec![
            (
                "7",
                None,
                json!({"events": [
                    test(json!("09"), "polyv2x-9", &[condition(9)]),
                    test(json!(9), "polyv2x-9", &[condition(9)]),
                    named_event("7", json!(1), json!("ok-1"), &[condition(1)]),
                ], "next_cursor": "c1"}),
            ),
            (
                "7",
                Some("c1"),
                json!({"events": [test(json!("9"), "polyv2x-9", &[condition(9)])]}),
            ),
        ])
        .await
        .unwrap();
        assert_eq!(scope.excluded_events.len(), 1);
        assert_eq!(scope.excluded_events[0].event_id, "9");
        assert_eq!(scope.condition_ids, [condition(1)]);
        for (name, pages) in [
            (
                "slug",
                vec![(
                    "7",
                    None,
                    json!({"events": [test(json!(9), "polyv2x-9", &[condition(9)]),
                                      test(json!(9), "polyv2y-9", &[condition(9)])]}),
                )],
            ),
            (
                "reason",
                vec![(
                    "7",
                    None,
                    json!({"events": [test(json!(9), "polyv2x-9", &[condition(9)]),
                                      named_event("7", json!(9), json!(" "), &[condition(9)])]}),
                )],
            ),
            (
                "markets",
                vec![
                    (
                        "7",
                        None,
                        json!({"events": [test(json!(9), "polyv2x-9", &[condition(9)])], "next_cursor": "c1"}),
                    ),
                    (
                        "7",
                        Some("c1"),
                        json!({"events": [test(json!(9), "polyv2x-9", &[condition(10)])]}),
                    ),
                ],
            ),
            (
                "series",
                vec![
                    (
                        "7",
                        None,
                        json!({"events": [test(json!(9), "polyv2x-9", &[condition(9)])]}),
                    ),
                    (
                        "8",
                        None,
                        json!({"events": [named_event("8", json!(9), json!("polyv2x-9"), &[condition(9)])]}),
                    ),
                ],
            ),
        ] {
            let error = sweep_pages(pages).await.unwrap_err();
            assert_eq!(error.exit_code(), 75, "{name}: {error}");
            assert!(
                matches!(error, BootstrapError::TransientSource { .. }),
                "{name}: {error}"
            );
            assert!(
                error.to_string().contains("excluded event 9"),
                "{name}: {error}"
            );
        }
    }

    /// FAIL (#892): a market listed by both a kept and an excluded event, in either order, on one page, across pages or
    /// across series, is kept, dropped or anything but a temporary failure (exit 75) naming the market and both events.
    #[tokio::test]
    async fn a_market_is_never_both_kept_and_excluded() {
        let market = condition(5);
        let kept = |series: &str| named_event(series, json!(1), json!("ok-1"), &[condition(5)]);
        let excluded =
            |series: &str| named_event(series, json!(2), json!("polyv2x-2"), &[condition(5)]);
        let one_page =
            |first: Value, second: Value| vec![("7", None, json!({"events": [first, second]}))];
        let two_pages = |first: Value, second: Value| {
            vec![
                ("7", None, json!({"events": [first], "next_cursor": "c1"})),
                ("7", Some("c1"), json!({"events": [second]})),
            ]
        };
        for (name, pages) in [
            (
                "kept, then excluded on one page",
                one_page(kept("7"), excluded("7")),
            ),
            (
                "excluded, then kept on one page",
                one_page(excluded("7"), kept("7")),
            ),
            (
                "kept, then excluded across pages",
                two_pages(kept("7"), excluded("7")),
            ),
            (
                "excluded, then kept across pages",
                two_pages(excluded("7"), kept("7")),
            ),
            (
                "kept, then excluded across series",
                vec![
                    ("7", None, json!({"events": [kept("7")]})),
                    ("8", None, json!({"events": [excluded("8")]})),
                ],
            ),
            (
                "excluded, then kept across series",
                vec![
                    ("7", None, json!({"events": [excluded("7")]})),
                    ("8", None, json!({"events": [kept("8")]})),
                ],
            ),
        ] {
            let error = sweep_pages(pages).await.unwrap_err();
            assert_eq!(error.exit_code(), 75, "{name}: {error}");
            assert!(
                matches!(error, BootstrapError::TransientSource { .. }),
                "{name}: {error}"
            );
            let text = error.to_string();
            assert!(
                text.contains(&format!(
                    "market {market} is listed by kept event 1 and excluded event 2"
                )),
                "{name}: {text}"
            );
        }
    }

    /// FAIL (#892): an excluded event without a decimal id, or with a malformed market, is recorded or fails any way
    /// but the reply-anomaly exit 1; a kept event now needs an id.
    #[tokio::test]
    async fn an_excluded_event_needs_a_decimal_id_and_valid_markets() {
        let mut no_id = named_event("7", json!(null), json!("polyv2x"), &[condition(1)]);
        no_id.as_object_mut().unwrap().remove("id");
        for (name, event) in [
            ("absent id", no_id),
            (
                "word id",
                named_event("7", json!("e9"), json!("polyv2x"), &[condition(1)]),
            ),
            (
                "negative id",
                named_event("7", json!(-9), json!("polyv2x"), &[condition(1)]),
            ),
            (
                "malformed market",
                named_event("7", json!(9), json!("polyv2x"), &["0xAB".to_owned()]),
            ),
        ] {
            let error = sweep_pages(vec![("7", None, json!({"events": [event]}))])
                .await
                .unwrap_err();
            assert_eq!(error.exit_code(), 1, "{name}: {error}");
        }
        let scope = sweep_pages(vec![(
            "7",
            None,
            json!({"events": [named_event("7", json!("e"), json!("ok"), &[condition(1)])]}),
        )])
        .await
        .unwrap();
        assert_eq!(scope.condition_ids, [condition(1)]);
        assert!(scope.excluded_events.is_empty());
    }

    /// FAIL (#892): on real Gamma event shapes, a test event's market is kept, a production one dropped, the digest or
    /// page provenance wrong, an excluded event's ids not its own, or a competing error reported in another order.
    /// Fixture `gamma_keyset_zec_mixed.json`: two production and two `polyv2zec` events (fields trimmed, extra fields
    /// kept) from the reply to
    /// `https://gamma-api.polymarket.com/events/keyset?series_id=12518&end_date_min=2026-05-26T00%3A00%3A00Z&limit=50&after_cursor=G4UOe7vzghe3R2KGEzHefxd4KvhoMHhxK7C84PCQLKJ7InYiOjEsImsiOiJldmVudHMiLCJvaCI6IjRmNTNjZGExOGMyYmFhMGMwMzU0YmI1ZjlhM2VjYmU1ZWQxMmFiNGQ4ZTExYmE4NzNjMmYxMTE2MTIwMmI5NDUiLCJrZXlzIjpbeyJ0Ijoic3RyaW5nIiwidiI6IjExODIzNjcifV19`
    /// fetched 2026-11-23 09:00 CST (reply sha256 bf2057d5349e318ce32cdda89c2b4fa0344c14740337173d59a0b12e612f7bd4).
    #[tokio::test]
    async fn real_mixed_zec_page_keeps_production_events_and_records_test_ones() {
        let page: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/gamma_keyset_zec_mixed.json"
        ))
        .unwrap();
        let events = page["events"].as_array().unwrap().clone();
        let ids_of = |test: bool| -> Vec<(String, Vec<String>)> {
            events
                .iter()
                .filter(|e| e["slug"].as_str().unwrap().starts_with("polyv2") == test)
                .map(|e| {
                    let markets = e["markets"].as_array().unwrap();
                    let ids = markets
                        .iter()
                        .map(|m| m["conditionId"].as_str().unwrap().to_owned())
                        .collect();
                    (e["id"].as_str().unwrap().to_owned(), ids)
                })
                .collect()
        };
        let serve_page = |page: Value| {
            let body = serde_json::to_vec(&page).unwrap();
            move |url: &str, _| -> Result<Vec<u8>, SourceError> {
                Ok(if url.contains("/series?") {
                    br#"[{"id":"12518","slug":"zec-up-or-down-15m","recurrence":"15m"}]"#.to_vec()
                } else {
                    body.clone()
                })
            }
        };
        let body = serde_json::to_vec(&page).unwrap();
        let scope = sweep(serve_page(page.clone())).await.unwrap();
        let mut kept: Vec<String> = ids_of(false).into_iter().flat_map(|(_, ids)| ids).collect();
        kept.sort();
        assert_eq!(scope.condition_ids, kept);
        assert_eq!(
            scope.digest,
            format!("{:x}", Sha256::digest(kept.join("\n").as_bytes()))
        );
        let recorded: Vec<(String, Option<String>, Vec<String>)> = scope
            .excluded_events
            .iter()
            .map(|e| (e.event_id.clone(), e.slug.clone(), e.condition_ids.clone()))
            .collect();
        let expected: Vec<(String, Option<String>, Vec<String>)> = ids_of(true)
            .into_iter()
            .map(|(id, ids)| {
                let slug = events.iter().find(|e| e["id"] == id.as_str()).unwrap()["slug"]
                    .as_str()
                    .map(str::to_owned);
                (id, slug, ids)
            })
            .collect();
        assert_eq!(recorded, expected);
        assert_eq!(
            recorded
                .iter()
                .map(|(id, _, _)| id.as_str())
                .collect::<Vec<_>>(),
            ["1183821", "1183864"]
        );
        assert!(
            scope
                .excluded_events
                .iter()
                .all(|e| e.reason == ExclusionReason::TestEvent && e.series_id == "12518")
        );
        let events_page = scope.pages.last().unwrap();
        assert_eq!(events_page.kind, "events");
        assert_eq!(events_page.series_id.as_deref(), Some("12518"));
        assert_eq!(
            events_page.url,
            events_url(BASE, "12518", FLOOR_ISO, None).unwrap()
        );
        assert_eq!(events_page.request_cursor, None);
        assert_eq!(events_page.returned_next_cursor, None);
        assert_eq!(events_page.rows, 4);
        assert_eq!(
            events_page.raw_sha256,
            format!("{:x}", Sha256::digest(&body))
        );
        // Competing errors keep their order: the embedded series, then the markets, then the excluded event's id.
        let first_test = events
            .iter()
            .position(|e| e["slug"].as_str().unwrap().starts_with("polyv2"))
            .unwrap();
        let mutate = |change: &dyn Fn(&mut Value)| {
            let mut page = page.clone();
            change(&mut page["events"][first_test]);
            page
        };
        let embedded_test = mutate(&|e| {
            e["series"][0]["slug"] = json!("polyv2-soak-zec");
            e["markets"][0]["conditionId"] = json!("0xAB");
        });
        let error = sweep(serve_page(embedded_test)).await.unwrap_err();
        assert_eq!(error.exit_code(), 75, "{error}");
        let malformed_market = mutate(&|e| {
            e["markets"][0]["conditionId"] = json!("0xAB");
            e.as_object_mut().unwrap().remove("id");
        });
        let error = sweep(serve_page(malformed_market)).await.unwrap_err();
        assert_eq!(error.exit_code(), 1, "{error}");
        assert!(error.to_string().contains("conditionId"), "{error}");
        let no_id = mutate(&|e| {
            e.as_object_mut().unwrap().remove("id");
        });
        let error = sweep(serve_page(no_id)).await.unwrap_err();
        assert_eq!(error.exit_code(), 1, "{error}");
        assert!(error.to_string().contains("no decimal id"), "{error}");
    }
}
