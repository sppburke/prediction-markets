//! HTTP page-fetching abstraction for the Polymarket public source.
//!
//! [`PageFetcher`] is implemented by [`ReqwestFetcher`] for production and
//! [`FixtureFetcher`] for hermetic tests.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pe_source_core::SourceError;

// Defaults — canonical values live in `docs/_GLOSSARY.md` "Polymarket public source" section.
const REQUEST_TIMEOUT_SECS: u64 = 10;
const MAX_RETRIES: u32 = 3;
/// Enforces ≤ 20 req/s per the Polymarket Data API documented limit (200 req/10s on `/trades`).
const MIN_INTERVAL_MS: u64 = 50;

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
/// - Rate limiting: enforces ≤ 1 / `MIN_INTERVAL_MS` req/ms (defaults to ≤ 20 req/s
///   at 50 ms) by reserving a future slot before each call. The reservation is
///   shared via `Mutex<Option<Instant>>` so concurrent callers all observe the
///   serial gate (see `last_request_at` and the gate logic in `fetch_page`).
/// - HTTP 429 → [`SourceError::RateLimited`] (returned to caller, not retried).
/// - HTTP 4xx (non-429) → [`SourceError::Fatal`].
pub struct ReqwestFetcher {
    client: reqwest::Client,
    timeout: Duration,
    max_retries: u32,
    initial_backoff_ms: u64,
    /// Shared rate-limit clock: serializes the gate across concurrent callers
    /// so the global throughput stays under `MIN_INTERVAL_MS` even when a single
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
}

impl PageFetcher for ReqwestFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        // Rate-limit gate: each call claims the next available slot,
        // computed as max(now, last_slot + MIN_INTERVAL_MS), and stamps it
        // before releasing the lock so concurrent callers observe the
        // reservation rather than the pre-sleep `now`. Wall-clock throughput
        // stays at ≤ 1 / MIN_INTERVAL_MS even when many tasks share this fetcher.
        let min_interval = Duration::from_millis(MIN_INTERVAL_MS);
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
            *guard = Some(next_slot);
            next_slot.checked_duration_since(now)
        };
        if let Some(d) = sleep_for {
            tokio::time::sleep(d).await;
        }

        let mut attempt = 0u32;
        loop {
            let send_result = self.client.get(url).timeout(self.timeout).send().await;

            match send_result {
                Err(e) => {
                    // Network or timeout error — retryable.
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

                    if status == 429 {
                        // Rate-limited by server — return to caller to apply Retry-After.
                        let retry_after = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u32>().ok())
                            .unwrap_or(30);
                        return Err(SourceError::RateLimited {
                            retry_after_secs: retry_after,
                        });
                    }

                    if status >= 500 {
                        // 5xx — retryable.
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
                        // 4xx (non-429) — unrecoverable.
                        return Err(SourceError::Fatal {
                            message: format!("HTTP {status}"),
                        });
                    }

                    // 2xx / 3xx — success.
                    return resp.bytes().await.map(|b| b.to_vec()).map_err(|e| {
                        SourceError::Transient {
                            message: e.to_string(),
                        }
                    });
                }
            }
        }
    }
}

/// Exponential backoff: `initial_ms * 2^(attempt-1)`, capped at 30 s.
fn backoff(initial_ms: u64, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1);
    let ms = initial_ms.saturating_mul(1u64 << shift.min(10));
    Duration::from_millis(ms.min(30_000))
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
