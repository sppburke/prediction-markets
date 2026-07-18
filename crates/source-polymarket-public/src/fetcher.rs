//! HTTP page-fetching abstraction for the Polymarket public source.
//!
//! [`PageFetcher`] is implemented by [`ReqwestFetcher`] for production and
//! [`FixtureFetcher`] for hermetic tests.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pe_core_types::{RawHttpAttempt, RawHttpResponse, RawTransportFailure, TransportErrorClass};
use pe_source_core::SourceError;
use time::OffsetDateTime;

// Defaults — canonical values live in `docs/_GLOSSARY.md` "Polymarket public source" section.
const REQUEST_TIMEOUT_SECS: u64 = 10;
const MAX_RETRIES: u32 = 3;
/// Enforces ≤ 20 req/s per the Polymarket Data API documented limit (200 req/10s on `/trades`).
const MIN_INTERVAL_MS: u64 = 50;

/// Stable semantic identity for an observed public request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRequestContext {
    pub source_id: &'static str,
    pub endpoint_kind: &'static str,
}

const UNOBSERVED_CONTEXT: HttpRequestContext = HttpRequestContext {
    source_id: "polymarket-public",
    endpoint_kind: "unobserved-page",
};

/// Abstracts HTTP page fetching so production and test connectors share the same logic.
///
/// `&self` (not `&mut self`) so a single fetcher can be shared across concurrent tasks;
/// implementations must use interior mutability for any per-call state.
#[allow(async_fn_in_trait)]
pub trait PageFetcher {
    /// Fetch a page from `url`. Returns raw response bytes.
    ///
    /// - Returns [`SourceError::RateLimited`] on HTTP 429 with the `Retry-After` value.
    /// - Returns [`SourceError::Transient`] on 5xx or network errors (retried internally).
    /// - Returns [`SourceError::Fatal`] on 4xx (non-429) errors.
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError>;
}

// ── Production fetcher ────────────────────────────────────────────────────────

/// A [`PageFetcher`] backed by a [`reqwest::Client`].
///
/// - Per-request timeout: `polymarket_request_timeout_secs = 10`.
/// - Retry with exponential backoff for network errors and 5xx responses
///   (`polymarket_max_retries = 3` retries; 4 total attempts).
/// - Rate limiting: enforces ≤ 1 / `min_interval_ms` req/ms (defaults to ≤ 20 req/s
///   at 50 ms) by reserving a future slot before each call. The reservation is
///   shared via `Mutex<Option<Instant>>` so concurrent callers all observe the
///   serial gate (see `last_request_at` and the gate logic in `fetch_page`).
///   Override via [`Self::with_min_interval_ms`] for APIs with different rate limits.
/// - HTTP 429 → [`SourceError::RateLimited`] (returned to caller, not retried).
/// - HTTP 4xx (non-429) → [`SourceError::Fatal`].
pub struct ReqwestFetcher {
    client: reqwest::Client,
    timeout: Duration,
    max_retries: u32,
    initial_backoff_ms: u64,
    min_interval_ms: u64,
    /// Shared rate-limit clock: serializes the gate across concurrent callers
    /// so the global throughput stays under `min_interval_ms` even when a single
    /// fetcher is shared by many tasks (e.g. via `Arc<ReqwestFetcher>`).
    last_request_at: Mutex<Option<Instant>>,
}

impl ReqwestFetcher {
    /// Wrap an existing `reqwest::Client` using the documented Polymarket defaults.
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            max_retries: MAX_RETRIES,
            initial_backoff_ms: 200,
            min_interval_ms: MIN_INTERVAL_MS,
            last_request_at: Mutex::new(None),
        }
    }

    /// Override the per-request timeout.
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout = Duration::from_secs(secs);
        self
    }

    /// Override the maximum number of retries (default `polymarket_max_retries = 3`).
    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Override the initial retry backoff; useful in tests to keep them fast.
    pub fn with_initial_backoff_ms(mut self, ms: u64) -> Self {
        self.initial_backoff_ms = ms;
        self
    }

    /// Override the minimum interval between requests (default `polymarket_min_interval_ms = 50`).
    ///
    /// Use a higher value when calling APIs with stricter rate limits than the Polymarket
    /// Data API. Example: `with_min_interval_ms(100)` for ≤ 10 req/s.
    pub fn with_min_interval_ms(mut self, ms: u64) -> Self {
        self.min_interval_ms = ms;
        self
    }

    /// Fetch while returning every raw attempt to the caller-owned observer.
    /// The observer is synchronous so a single-owner campaign actor can append evidence before
    /// this method decides to retry; it must not perform network I/O.
    pub async fn fetch_page_observed(
        &self,
        url: &str,
        context: HttpRequestContext,
        mut observe: impl FnMut(RawHttpAttempt) -> Result<(), SourceError>,
    ) -> Result<Vec<u8>, SourceError> {
        self.fetch_page_inner(url, context, None, &mut observe)
            .await
    }

    /// Fetch with a workflow deadline enforced after the rate slot and at the request seam.
    pub async fn fetch_page_observed_until(
        &self,
        url: &str,
        context: HttpRequestContext,
        deadline: Instant,
        mut observe: impl FnMut(RawHttpAttempt) -> Result<(), SourceError>,
    ) -> Result<Vec<u8>, SourceError> {
        self.fetch_page_inner(url, context, Some(deadline), &mut observe)
            .await
    }

    async fn fetch_page_inner(
        &self,
        url: &str,
        context: HttpRequestContext,
        deadline: Option<Instant>,
        observe: &mut impl FnMut(RawHttpAttempt) -> Result<(), SourceError>,
    ) -> Result<Vec<u8>, SourceError> {
        if !self.wait_for_rate_slot(deadline).await {
            return Err(SourceError::Transient {
                message: "request deadline elapsed before send".to_owned(),
            });
        }

        let mut attempt = 0u32;
        let parsed_url = reqwest::Url::parse(url).map_err(|error| SourceError::Fatal {
            message: error.to_string(),
        })?;
        let path = parsed_url.path().to_owned();
        let ordered_query = parsed_url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        loop {
            let ordinal = attempt + 1;
            let request_timeout = match deadline {
                Some(deadline) => deadline
                    .checked_duration_since(Instant::now())
                    .map(|remaining| remaining.min(self.timeout))
                    .ok_or_else(|| SourceError::Transient {
                        message: "request deadline elapsed before send".to_owned(),
                    })?,
                None => self.timeout,
            };
            let observed_at = OffsetDateTime::now_utc();
            let send_result = self.client.get(url).timeout(request_timeout).send().await;

            match send_result {
                Err(e) => {
                    observe(RawHttpAttempt::TransportFailure(RawTransportFailure {
                        source_id: context.source_id.to_owned(),
                        endpoint_kind: context.endpoint_kind.to_owned(),
                        method: "GET".to_owned(),
                        path: path.clone(),
                        ordered_query: ordered_query.clone(),
                        attempt_ordinal: ordinal,
                        observed_at,
                        received_at: OffsetDateTime::now_utc(),
                        error_class: if e.is_timeout() {
                            TransportErrorClass::Timeout
                        } else if e.is_connect() {
                            TransportErrorClass::Connect
                        } else {
                            TransportErrorClass::Other
                        },
                        schema_version: 1,
                        parser_version: 1,
                        adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
                    }))?;
                    if attempt >= self.max_retries {
                        return Err(SourceError::Transient {
                            message: e.to_string(),
                        });
                    }
                    attempt += 1;
                    tokio::time::sleep(backoff(self.initial_backoff_ms, attempt)).await;
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let headers = observed_headers(resp.headers());
                    let retry_after = header_value(&headers, "retry-after");
                    let body = match resp.bytes().await {
                        Ok(bytes) => bytes.to_vec(),
                        Err(e) => {
                            observe(RawHttpAttempt::TransportFailure(RawTransportFailure {
                                source_id: context.source_id.to_owned(),
                                endpoint_kind: context.endpoint_kind.to_owned(),
                                method: "GET".to_owned(),
                                path: path.clone(),
                                ordered_query: ordered_query.clone(),
                                attempt_ordinal: ordinal,
                                observed_at,
                                received_at: OffsetDateTime::now_utc(),
                                error_class: TransportErrorClass::BodyRead,
                                schema_version: 1,
                                parser_version: 1,
                                adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
                            }))?;
                            if attempt >= self.max_retries {
                                return Err(SourceError::Transient {
                                    message: e.to_string(),
                                });
                            }
                            attempt += 1;
                            tokio::time::sleep(backoff(self.initial_backoff_ms, attempt)).await;
                            continue;
                        }
                    };
                    observe(RawHttpAttempt::Response(RawHttpResponse {
                        source_id: context.source_id.to_owned(),
                        endpoint_kind: context.endpoint_kind.to_owned(),
                        method: "GET".to_owned(),
                        path: path.clone(),
                        ordered_query: ordered_query.clone(),
                        observed_at,
                        received_at: OffsetDateTime::now_utc(),
                        status,
                        headers: headers.clone(),
                        body: body.clone(),
                        attempt_ordinal: ordinal,
                        source_at: source_at(&headers),
                        schema_version: 1,
                        parser_version: 1,
                        adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
                    }))?;

                    if status == 429 {
                        let retry_after_secs = retry_after
                            .as_deref()
                            .and_then(|value| value.parse::<u32>().ok())
                            .unwrap_or(30);
                        return Err(SourceError::RateLimited { retry_after_secs });
                    }
                    if is_retryable_status(status) {
                        if attempt >= self.max_retries {
                            return Err(SourceError::Transient {
                                message: format!("HTTP {status}"),
                            });
                        }
                        attempt += 1;
                        tokio::time::sleep(backoff(self.initial_backoff_ms, attempt)).await;
                        continue;
                    }
                    if status >= 400 {
                        return Err(SourceError::Fatal {
                            message: format!("HTTP {status}"),
                        });
                    }
                    return Ok(body);
                }
            }
        }
    }

    async fn wait_for_rate_slot(&self, deadline: Option<Instant>) -> bool {
        let min_interval = Duration::from_millis(self.min_interval_ms);
        let sleep_for = {
            let mut guard = self
                .last_request_at
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            let next_slot = match *guard {
                None => now,
                Some(last) => last.max(now) + min_interval,
            };
            if deadline.is_some_and(|deadline| next_slot >= deadline) {
                return false;
            }
            *guard = Some(next_slot);
            next_slot.checked_duration_since(now)
        };
        if let Some(duration) = sleep_for {
            tokio::time::sleep(duration).await;
        }
        true
    }
}

impl PageFetcher for ReqwestFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.fetch_page_inner(url, UNOBSERVED_CONTEXT, None, &mut |_| Ok(()))
            .await
    }
}

fn observed_headers(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    [
        "content-type",
        "date",
        "etag",
        "retry-after",
        "x-request-id",
    ]
    .into_iter()
    .filter_map(|name| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(|value| (name.to_owned(), value.to_owned()))
    })
    .collect()
}

fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

fn source_at(headers: &[(String, String)]) -> Option<OffsetDateTime> {
    header_value(headers, "date").and_then(|value| {
        OffsetDateTime::parse(&value, &time::format_description::well_known::Rfc2822).ok()
    })
}

/// Exponential backoff: `initial_ms * 2^(attempt-1)`, capped at 30 s.
fn backoff(initial_ms: u64, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1);
    let ms = initial_ms.saturating_mul(1u64 << shift.min(10));
    Duration::from_millis(ms.min(30_000))
}

/// HTTP status codes that should be retried with backoff. `408 Request Timeout`
/// is added to the 5xx set (issue #159) — empirically transient on Polymarket's
/// gamma-api edge. `429` is handled separately via [`SourceError::RateLimited`];
/// `425 Too Early` is deferred until observed in logs.
fn is_retryable_status(status: u16) -> bool {
    status == 408 || status >= 500
}

// ── Test fixture fetcher ──────────────────────────────────────────────────────

/// A [`PageFetcher`] that returns pre-loaded fixture bytes keyed by URL.
///
/// Used in hermetic tests; returns [`SourceError::Fatal`] for unknown URLs.
pub struct FixtureFetcher {
    responses: HashMap<String, Vec<u8>>,
}

impl FixtureFetcher {
    /// Create a `FixtureFetcher` from a URL → bytes map.
    pub fn new(responses: HashMap<String, Vec<u8>>) -> Self {
        Self { responses }
    }
}

impl PageFetcher for FixtureFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| SourceError::Fatal {
                message: format!("no fixture for URL: {url}"),
            })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observed_response_retains_retry_headers() {
        let app = axum::Router::new().route(
            "/limited",
            axum::routing::get(|| async {
                (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "17"), ("content-type", "application/json")],
                    "{}",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let fetcher = ReqwestFetcher::new(reqwest::Client::new()).with_max_retries(0);
        let mut attempts = Vec::new();
        let result = fetcher
            .fetch_page_observed(
                &format!("http://{address}/limited"),
                HttpRequestContext {
                    source_id: "test-server",
                    endpoint_kind: "limited",
                },
                |attempt| {
                    attempts.push(attempt);
                    Ok(())
                },
            )
            .await;
        assert!(matches!(result, Err(SourceError::RateLimited { .. })));
        assert!(matches!(attempts.as_slice(), [RawHttpAttempt::Response(_)]));
        let Some(RawHttpAttempt::Response(response)) = attempts.first() else {
            return;
        };
        assert!(
            response
                .headers
                .iter()
                .any(|(name, value)| name == "retry-after" && value == "17")
        );
    }

    #[tokio::test]
    async fn deadline_before_reserved_rate_slot_emits_no_attempt() {
        let app = axum::Router::new().route("/ok", axum::routing::get(|| async { "{}" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let fetcher = ReqwestFetcher::new(reqwest::Client::new())
            .with_max_retries(0)
            .with_min_interval_ms(200);
        let url = format!("http://{address}/ok?token_id=11");
        fetcher
            .fetch_page_observed(
                &url,
                HttpRequestContext {
                    source_id: "test-server",
                    endpoint_kind: "book",
                },
                |_| Ok(()),
            )
            .await
            .unwrap();

        let mut attempts = Vec::new();
        let result = fetcher
            .fetch_page_observed_until(
                &url,
                HttpRequestContext {
                    source_id: "test-server",
                    endpoint_kind: "book",
                },
                Instant::now() + Duration::from_millis(10),
                |attempt| {
                    attempts.push(attempt);
                    Ok(())
                },
            )
            .await;
        assert!(matches!(result, Err(SourceError::Transient { .. })));
        assert!(attempts.is_empty());
    }

    #[tokio::test]
    async fn active_deadline_emits_exact_transport_identity() {
        let app = axum::Router::new().route(
            "/hang",
            axum::routing::get(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                "{}"
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let fetcher = ReqwestFetcher::new(reqwest::Client::new()).with_max_retries(0);
        let mut attempts = Vec::new();
        let result = fetcher
            .fetch_page_observed_until(
                &format!("http://{address}/hang?next_cursor=abc&limit=500"),
                HttpRequestContext {
                    source_id: "test-server",
                    endpoint_kind: "activity-page",
                },
                Instant::now() + Duration::from_millis(20),
                |attempt| {
                    attempts.push(attempt);
                    Ok(())
                },
            )
            .await;
        assert!(matches!(result, Err(SourceError::Transient { .. })));
        assert!(matches!(
            attempts.as_slice(),
            [RawHttpAttempt::TransportFailure(_)]
        ));
        let failure = attempts
            .iter()
            .find_map(|attempt| match attempt {
                RawHttpAttempt::TransportFailure(failure) => Some(failure),
                RawHttpAttempt::Response(_) => None,
            })
            .expect("transport failure was asserted above");
        assert_eq!(failure.path, "/hang");
        assert_eq!(
            failure.ordered_query,
            [
                ("next_cursor".to_owned(), "abc".to_owned()),
                ("limit".to_owned(), "500".to_owned())
            ]
        );
        assert_eq!(failure.error_class, TransportErrorClass::Timeout);
    }

    #[test]
    fn retryable_status_includes_408_and_5xx() {
        // Issue #159: 408 was previously fatal; now retried.
        assert!(is_retryable_status(408));
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(502));
        assert!(is_retryable_status(503));
        assert!(is_retryable_status(504));
        assert!(is_retryable_status(599));
    }

    #[test]
    fn retryable_status_excludes_429_and_unrelated_4xx() {
        // 429 has its own RateLimited path; the rest of 4xx is fatal.
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(401));
        assert!(!is_retryable_status(403));
        assert!(!is_retryable_status(404));
        assert!(!is_retryable_status(425));
        assert!(!is_retryable_status(429));
        // And the success / redirect ranges remain non-retryable.
        assert!(!is_retryable_status(200));
        assert!(!is_retryable_status(204));
        assert!(!is_retryable_status(301));
        assert!(!is_retryable_status(304));
    }
}
