//! Gamma 15-minute market scope for the ranker (#784).
//!
//! Gate F copies only markets whose Gamma event's first series recurs every 15 minutes (#782).
//! The ranker scores the same markets: this command lists every Gamma series, keeps those with
//! that recurrence, walks each one's events through the documented `/events/keyset` endpoint back
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

/// One fetched Gamma page with its request and receipt times and the reply's `Date` (source) time.
struct FetchedPage {
    body: Vec<u8>,
    observed_at_unix: i64,
    received_at_unix: i64,
    source_at_unix: Option<i64>,
}

/// Page transport; production observes each attempt, tests supply pages directly.
trait ScopeFetcher {
    fn fetch(
        &self,
        url: &str,
        endpoint_kind: &'static str,
    ) -> impl std::future::Future<Output = Result<FetchedPage, SourceError>> + Send;
}

impl ScopeFetcher for ReqwestFetcher {
    async fn fetch(
        &self,
        url: &str,
        endpoint_kind: &'static str,
    ) -> Result<FetchedPage, SourceError> {
        let mut times = None;
        let body = self
            .fetch_page_observed(
                url,
                HttpRequestContext {
                    source_id: SOURCE_ID,
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

/// A listed 15-minute series.
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
    pub recurrence: &'static str,
    pub end_date_min_unix: i64,
    pub started_at_unix: i64,
    pub completed_at_unix: i64,
    pub series: Vec<ScopeSeries>,
    pub pages: Vec<ScopePage>,
    pub condition_ids: Vec<String>,
    pub count: u64,
    pub digest: String,
}

/// Build the production fetcher, sweep, and write the scope file atomically.
pub async fn run_market_scope(
    config: &BootstrapConfig,
    out: &Path,
    end_date_min_unix: i64,
) -> Result<MarketScope, BootstrapError> {
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = ReqwestFetcher::new(client).with_min_interval_ms(SCOPE_MIN_INTERVAL_MS);
    let scope = sweep_market_scope(
        &fetcher,
        &config.gamma_base_url,
        end_date_min_unix,
        PAGE_RETRY_BASE,
    )
    .await?;
    crate::cache_migration::atomic_write_json(out, &scope)?;
    Ok(scope)
}

/// List the 15-minute series, then walk each one's events ending at or after the floor.
async fn sweep_market_scope<F: ScopeFetcher + Sync>(
    fetcher: &F,
    base_url: &str,
    end_date_min_unix: i64,
    retry_base: Duration,
) -> Result<MarketScope, BootstrapError> {
    let started_at_unix = OffsetDateTime::now_utc().unix_timestamp();
    let floor = OffsetDateTime::from_unix_timestamp(end_date_min_unix)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .ok_or_else(|| BootstrapError::Invalid {
            message: format!("market-scope: unusable --end-date-min {end_date_min_unix}"),
        })?;
    let mut pages = Vec::new();

    let mut series = BTreeMap::new();
    let mut listed = false;
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
            if row.get("recurrence").and_then(Value::as_str) != Some(FIFTEEN_MINUTE_RECURRENCE) {
                continue;
            }
            let (number, id) = decimal_id(row.get("id"))
                .ok_or_else(|| anomaly_error(&url, "15-minute series id is not decimal"))?;
            let slug = row.get("slug").and_then(Value::as_str).map(str::to_owned);
            series.entry(number).or_insert(ScopeSeries { id, slug });
        }
        let rows_len = rows.len();
        pages.push(page_record(
            &pages, "series", None, url, None, None, &page, rows_len,
        )?);
        if rows_len < SERIES_PAGE_LIMIT {
            listed = true;
            break;
        }
    }
    if !listed {
        return Err(BootstrapError::Gamma {
            message: format!("market-scope: series listing exceeded {SERIES_MAX_PAGES} pages"),
        });
    }
    if series.is_empty() {
        return Err(BootstrapError::Gamma {
            message: "market-scope: Gamma lists no 15-minute series".to_owned(),
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
                collect_event(&url, &listed_series.id, event, &mut condition_ids)?;
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
            message: "market-scope: the 15-minute series list no markets".to_owned(),
        });
    }

    let condition_ids: Vec<String> = condition_ids.into_iter().collect();
    let digest = format!("{:x}", Sha256::digest(condition_ids.join("\n").as_bytes()));
    Ok(MarketScope {
        version: 1,
        source_id: SOURCE_ID,
        schema_version: 1,
        parser_version: 1,
        recurrence: FIFTEEN_MINUTE_RECURRENCE,
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

/// Label one event's markets after proving it belongs to the walked 15-minute series.
fn collect_event(
    url: &str,
    series_id: &str,
    event: &Value,
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
    if event_series_recurrence(event) != Some(FIFTEEN_MINUTE_RECURRENCE) {
        return anomaly(url, "event's first series does not recur every 15 minutes");
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
        match fetcher.fetch(url, endpoint_kind).await {
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
        ) -> Result<FetchedPage, SourceError> {
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
        sweep_market_scope(&fetcher(serve), BASE, FLOOR, Duration::ZERO).await
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
            (scope.version, scope.source_id, scope.recurrence),
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
        let error = sweep_market_scope(&calls, BASE, FLOOR, Duration::ZERO)
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
