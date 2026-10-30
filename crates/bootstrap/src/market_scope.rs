//! Gamma market scope for the ranker and participant discovery (#784, #800).
//!
//! This command lists every Gamma series, keeps those with a requested recurrence,
//! walks each one's events through the documented `/events/keyset` endpoint back
//! to an end-date floor, and writes the markets' condition ids with every page's hash, cursor
//! chain and times. Any surprise in a reply fails the command (exit 1); exhausted transient
//! fetches exit 75 so the loop supervisor retries the retained cycle. Nothing touches the cache.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::time::Duration;

use pe_core_types::RawHttpAttempt;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    FIFTEEN_MINUTE_RECURRENCE, HttpRequestContext, ReqwestFetcher, event_series_recurrence,
};
use serde::Serialize;
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
/// `/events/keyset` documents `limit` at most 100.
const EVENTS_PAGE_LIMIT: usize = 100;
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScopeSeries {
    pub id: String,
    pub slug: Option<String>,
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
    crate::cache_migration::atomic_write_json(out, &scope)?;
    Ok(scope)
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

/// Parse `id:slug,...`, the series a scope must list exactly (the research period pins gen 26's).
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
/// ending at or after the floor.
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
    let series = match pinned {
        Some(pinned) => {
            pinned_series(
                fetcher,
                base_url,
                pinned,
                &recurrences,
                retry_base,
                &mut pages,
            )
            .await?
        }
        None => list_series(fetcher, base_url, &recurrences, retry_base, &mut pages).await?,
    };
    if series.is_empty() {
        return Err(BootstrapError::Gamma {
            message: "market-scope: Gamma lists no requested series".to_owned(),
        });
    }

    let mut condition_ids = BTreeSet::new();
    for listed_series in series.values() {
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
                    &mut condition_ids,
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
    if condition_ids.is_empty() {
        return Err(BootstrapError::Gamma {
            message: "market-scope: the requested series list no markets".to_owned(),
        });
    }

    let condition_ids: Vec<String> = condition_ids.into_iter().collect();
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
        series: series.into_values().collect(),
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

/// Label one event's markets after proving it belongs to a requested, walked series.
fn collect_event(
    url: &str,
    series_id: &str,
    event: &Value,
    recurrences: &[String],
    condition_ids: &mut BTreeSet<String>,
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
    let markets = event
        .get("markets")
        .and_then(Value::as_array)
        .ok_or_else(|| anomaly_error(url, "event has no markets array"))?;
    for market in markets {
        let condition_id = market
            .get("conditionId")
            .and_then(Value::as_str)
            .filter(|value| is_condition_id(value))
            .ok_or_else(|| anomaly_error(url, "market conditionId is not 0x + 64 lowercase hex"))?;
        condition_ids.insert(condition_id.to_owned());
    }
    Ok(())
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

    fn event(series_id: &str, recurrence: &str, markets: &[String]) -> Value {
        json!({
            "id": "e",
            "series": [{"id": series_id, "recurrence": recurrence}],
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

    /// The pinned series by id, as Gamma answers `/series/{id}`; anything else is `serve_ok`'s.
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
                    r#"{{"events":[{{"series":[{{"id":"7","recurrence":"15m"}}],"markets":[{{"conditionId":"{}"}}]}}]}}"#,
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
                            {"id": "2", "recurrence": "15m"},
                            {"id": "3", "recurrence": "5m"},
                            {"id": "4", "recurrence": "daily"}
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
                json!({"events": [{"series": [{"id": "2", "recurrence": "15m"}]}]}),
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
            sweep(no_labels).await.unwrap_err(),
        ] {
            assert_eq!(error.exit_code(), 1, "{error}");
        }
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
                return Ok(serde_json::to_vec(&json!([{"id": "2", "recurrence": "15m"}])).unwrap());
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
                return Ok(serde_json::to_vec(&json!([{"id": "2", "recurrence": "15m"}])).unwrap());
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
}
