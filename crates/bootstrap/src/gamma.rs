//! Polymarket Gamma API client — fetches market resolution data.
//!
//! Endpoint: `GET https://gamma-api.polymarket.com/markets?condition_ids={ID}`
//!
//! Multi-ID batching is not supported (comma-separated, bracket, and repeat-key
//! strategies all fail silently). Per-ID sequential fetch is required.
//!
//! Rate limit: live-tested at ≥27 req/s; we gate at 10 req/s (100ms) to stay
//! conservative. See `bootstrap_gamma_min_interval_ms` in `docs/_GLOSSARY.md`.

use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::info;

use crate::cache::{ResolutionIndex, WalletCache};
use crate::error::BootstrapError;

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
pub(crate) const GAMMA_MIN_INTERVAL_MS: u64 = 100; // 10 req/s; live-tested limit ≥27 req/s

/// Fetches market resolution data from the Polymarket Gamma API.
///
/// Generic over [`PageFetcher`] so production code uses [`ReqwestFetcher`] and
/// tests use [`FixtureFetcher`] with no live network calls.
pub struct GammaFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher> GammaFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// Fetch resolutions for every market ID not already present in `cache`.
    ///
    /// Calls [`WalletCache::resolved_market_ids`] once up front to build the skip set;
    /// markets already in `market_resolutions` are not re-fetched. Each successful
    /// resolution is inserted immediately via `INSERT OR IGNORE` (WAL durability).
    ///
    /// Returns the count of newly inserted rows. Markets not yet closed, or markets
    /// where the Gamma API returns no result, are silently skipped and will be retried
    /// on the next run.
    pub async fn fetch_resolutions(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        let already_resolved = cache.resolved_market_ids();
        let to_fetch: Vec<&String> = market_ids
            .iter()
            .filter(|id| !already_resolved.contains(*id))
            .collect();

        let total = to_fetch.len();
        info!(
            total,
            already_cached = already_resolved.len(),
            "gamma: starting resolution fetch"
        );

        let mut inserted = 0usize;
        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();

        for (i, market_id) in to_fetch.iter().enumerate() {
            if i > 0 && i % 1_000 == 0 {
                info!(
                    progress = i,
                    total, inserted, "gamma: resolution fetch progress"
                );
            }

            let url = format!("{}/markets?condition_ids={}", self.base_url, market_id);
            let bytes = match self.fetcher.fetch_page(&url).await {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    tracing::warn!(%market_id, %message, "gamma: fetch error, skipping");
                    continue;
                }
                Err(e) => {
                    return Err(BootstrapError::Gamma {
                        message: format!("fetch {market_id}: {e}"),
                    });
                }
            };

            let parsed = match parse_gamma_response(&bytes) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%market_id, %e, "gamma: parse error, skipping");
                    continue;
                }
            };

            let Some((winner, resolved_at_unix)) = parsed else {
                continue; // market not yet closed — skip silently
            };

            cache.insert_resolution(market_id, winner, resolved_at_unix, fetched_at)?;
            inserted += 1;
        }

        info!(inserted, total, "gamma: resolution fetch complete");
        Ok(inserted)
    }
}

/// Serde DTO for a single element of the `/markets` response array.
///
/// Only the fields needed for resolution detection are mapped; extra fields ignored.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketRaw {
    #[allow(dead_code)]
    condition_id: String,
    closed: bool,
    closed_time: Option<String>,
    outcome_prices: Option<String>,
}

/// Parse a single-element Gamma `/markets` response.
///
/// Returns:
/// - `Some((Some(idx), unix))` — binary market resolved; `idx` is the winning outcome index.
/// - `Some((None, unix))` — market closed but winner ambiguous (voided / multi-outcome / parse fail).
/// - `None` — market not yet closed; caller should skip and retry later.
///
/// `bytes` must be a JSON array. An empty array returns `None` (market not found).
pub fn parse_gamma_response(bytes: &[u8]) -> Result<Option<(Option<u8>, i64)>, String> {
    let markets: Vec<GammaMarketRaw> =
        serde_json::from_slice(bytes).map_err(|e| format!("JSON parse: {e}"))?;

    let Some(m) = markets.into_iter().next() else {
        return Ok(None); // empty array — market not found on Gamma
    };

    if !m.closed {
        return Ok(None); // not yet resolved
    }

    let Some(closed_time) = m.closed_time else {
        return Ok(None); // closed flag set but no timestamp — treat as unresolved
    };

    let resolved_at_unix = parse_closed_time(&closed_time)?;

    let Some(prices_str) = m.outcome_prices else {
        return Ok(Some((None, resolved_at_unix))); // closed, no price data — voided
    };

    // outcomePrices is a JSON-string-encoded array of decimal strings, e.g. "[\"1\", \"0\"]".
    let prices: Vec<String> =
        serde_json::from_str(&prices_str).map_err(|e| format!("outcomePrices parse: {e}"))?;

    if prices.len() != 2 {
        // Multi-outcome / scalar — out of scope for v1; treat as voided.
        return Ok(Some((None, resolved_at_unix)));
    }

    // Winning outcome: the index whose price > 0.5.
    let winner = prices.iter().enumerate().find_map(|(i, s)| {
        let p: Decimal = s.parse().ok()?;
        if p > Decimal::new(5, 1) {
            u8::try_from(i).ok()
        } else {
            None
        }
    });

    Ok(Some((winner, resolved_at_unix)))
}

/// Parse Gamma's `closedTime` format: `"YYYY-MM-DD HH:MM:SS+00"`.
///
/// The offset is always `+00` (UTC), so we truncate to the 19-char datetime
/// prefix and parse as UTC.
fn parse_closed_time(s: &str) -> Result<i64, String> {
    let prefix = s
        .get(..19)
        .ok_or_else(|| format!("closedTime too short: {s:?}"))?;
    let fmt = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
        .map_err(|e| format!("format description: {e}"))?;
    time::PrimitiveDateTime::parse(prefix, &fmt)
        .map(|dt| dt.assume_utc().unix_timestamp())
        .map_err(|e| format!("parse closedTime {s:?}: {e}"))
}

// ── Convenience: build a ResolutionIndex without running a full bootstrap ─────

/// Load all resolutions from `cache` into a [`ResolutionIndex`].
///
/// Thin wrapper that delegates to [`WalletCache::load_all_resolutions`]; provided
/// here so callers don't need to import both `gamma` and `cache`.
pub fn load_resolutions(cache: &WalletCache) -> Result<ResolutionIndex, BootstrapError> {
    cache.load_all_resolutions()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use pe_source_polymarket_public::FixtureFetcher;
    use tempfile::TempDir;

    use super::*;
    use crate::cache::WalletCache;

    fn fixture_bytes(json: &str) -> Vec<u8> {
        json.as_bytes().to_vec()
    }

    fn resolved_yes_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"1\",\"0\"]","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn resolved_no_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"0\",\"1\"]","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn voided_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"0\",\"0\"]","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn unclosed_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":false,"closedTime":null,"outcomePrices":null,"outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn empty_array_fixture() -> Vec<u8> {
        fixture_bytes("[]")
    }

    #[test]
    fn parse_resolved_yes_winner() {
        let (winner, _ts) = parse_gamma_response(&resolved_yes_fixture())
            .unwrap()
            .unwrap();
        assert_eq!(winner, Some(0), "YES (index 0) must win");
    }

    #[test]
    fn parse_resolved_no_winner() {
        let (winner, _ts) = parse_gamma_response(&resolved_no_fixture())
            .unwrap()
            .unwrap();
        assert_eq!(winner, Some(1), "NO (index 1) must win");
    }

    #[test]
    fn parse_voided_market_returns_none_winner() {
        let (winner, _ts) = parse_gamma_response(&voided_fixture()).unwrap().unwrap();
        assert_eq!(winner, None, "voided market must have None winner");
    }

    #[test]
    fn parse_unclosed_market_returns_none() {
        let result = parse_gamma_response(&unclosed_fixture()).unwrap();
        assert!(result.is_none(), "unclosed market must return None");
    }

    #[test]
    fn parse_empty_array_returns_none() {
        let result = parse_gamma_response(&empty_array_fixture()).unwrap();
        assert!(result.is_none(), "empty array must return None");
    }

    #[test]
    fn parse_closed_time_format() {
        // Verify the space-separated format with +00 suffix parses correctly.
        let ts = parse_closed_time("2020-11-02 16:31:01+00").unwrap();
        // 2020-11-02 16:31:01 UTC = 1604334661
        assert_eq!(
            ts, 1_604_334_661,
            "closedTime must round-trip to unix seconds"
        );
    }

    #[test]
    fn fetcher_skips_already_cached_ids() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Pre-insert one market as already-resolved.
        cache
            .insert_resolution("0xa", Some(0), 1_700_000_000, 1_700_000_001)
            .unwrap();

        // Fixture only handles "0xb"; if "0xa" were fetched it would return Fatal.
        let url_b = "https://gamma-api.polymarket.com/markets?condition_ids=0xb";
        let mut responses = HashMap::new();
        responses.insert(url_b.to_owned(), resolved_yes_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inserted = rt
            .block_on(gamma.fetch_resolutions(&["0xa".to_owned(), "0xb".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 1, "only 0xb must be fetched (0xa was cached)");
        assert_eq!(cache.load_all_resolutions().unwrap().len(), 2);
    }

    #[test]
    fn fetcher_skips_unclosed_markets() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let url = "https://gamma-api.polymarket.com/markets?condition_ids=0xcond";
        let mut responses = HashMap::new();
        responses.insert(url.to_owned(), unclosed_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inserted = rt
            .block_on(gamma.fetch_resolutions(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 0, "unclosed market must not be inserted");
        assert!(cache.load_all_resolutions().unwrap().is_empty());
    }

    #[test]
    fn fetcher_inserts_voided_market_as_null_winner() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let url = "https://gamma-api.polymarket.com/markets?condition_ids=0xcond";
        let mut responses = HashMap::new();
        responses.insert(url.to_owned(), voided_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inserted = rt
            .block_on(gamma.fetch_resolutions(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        // Voided market is inserted (winner=NULL) but excluded from ResolutionIndex.
        assert_eq!(inserted, 1);
        assert_eq!(
            cache.resolved_market_ids().len(),
            1,
            "voided row must be in resolved_market_ids (skip-set)"
        );
        assert!(
            cache.load_all_resolutions().unwrap().is_empty(),
            "voided row must be excluded from ResolutionIndex (NULL winner filtered)"
        );
    }
}
