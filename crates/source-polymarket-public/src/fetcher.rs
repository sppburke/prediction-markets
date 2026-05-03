//! HTTP page-fetching abstraction for the Polymarket public source.
//!
//! [`PageFetcher`] is implemented by [`ReqwestFetcher`] for production and
//! [`FixtureFetcher`] for hermetic tests.

use std::collections::HashMap;

use pe_source_core::SourceError;

/// Abstracts HTTP page fetching so production and test connectors share the same logic.
#[allow(async_fn_in_trait)]
pub trait PageFetcher {
    /// Fetch a page from `url`. Returns raw response bytes.
    ///
    /// - Returns [`SourceError::RateLimited`] on HTTP 429 with the `Retry-After` value.
    /// - Returns [`SourceError::Transient`] on other transient HTTP errors.
    /// - Returns [`SourceError::Fatal`] on unrecoverable errors.
    async fn fetch_page(&mut self, url: &str) -> Result<Vec<u8>, SourceError>;
}

// ── Production fetcher ────────────────────────────────────────────────────────

/// A [`PageFetcher`] backed by a [`reqwest::Client`].
pub struct ReqwestFetcher {
    client: reqwest::Client,
}

impl ReqwestFetcher {
    /// Wrap an existing `reqwest::Client`.
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl PageFetcher for ReqwestFetcher {
    async fn fetch_page(&mut self, url: &str) -> Result<Vec<u8>, SourceError> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| SourceError::Transient {
                message: e.to_string(),
            })?;

        if response.status().as_u16() == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(30);
            return Err(SourceError::RateLimited {
                retry_after_secs: retry_after,
            });
        }

        if !response.status().is_success() {
            return Err(SourceError::Transient {
                message: format!("HTTP {}", response.status()),
            });
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| SourceError::Transient {
                message: e.to_string(),
            })
    }
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
    async fn fetch_page(&mut self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| SourceError::Fatal {
                message: format!("no fixture for URL: {url}"),
            })
    }
}
