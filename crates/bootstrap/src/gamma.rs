//! Polymarket Gamma API client — fetches market resolution data.
//!
//! Endpoint: `GET https://gamma-api.polymarket.com/markets?condition_ids={ID}`
//!
//! Multi-ID batching is not supported (comma-separated, bracket, and repeat-key
//! strategies all fail silently). Per-ID requests are required, but the client
//! fires `GAMMA_CONCURRENCY` of them in parallel via `buffer_unordered` to
//! amortise the per-request RTT (~250–300 ms each).
//!
//! Rate limit: live-tested at ≥27 req/s; we gate at 20 req/s (50 ms) to stay
//! conservative. The cap is enforced globally by [`ReqwestFetcher`]'s shared
//! mutex regardless of caller concurrency. See
//! `bootstrap_gamma_min_interval_ms` and `bootstrap_gamma_concurrency` in
//! `docs/_GLOSSARY.md`.

use futures::stream::{self, StreamExt};
use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::info;

use crate::cache::{LiquidityIndex, ResolutionIndex, ScheduleIndex, WalletCache};
use crate::error::BootstrapError;

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
pub(crate) const GAMMA_MIN_INTERVAL_MS: u64 = 50; // 20 req/s; live-tested limit ≥27 req/s
/// Number of in-flight Gamma requests issued concurrently per fetch loop.
/// With ~300 ms per-request RTT, ~6 in-flight saturates the 20 req/s rate
/// limit; 10 leaves headroom for latency spikes without burning CPU on idle
/// tasks.
pub(crate) const GAMMA_CONCURRENCY: usize = 10;

/// Fetches market resolution data from the Polymarket Gamma API.
///
/// Generic over [`PageFetcher`] so production code uses [`ReqwestFetcher`] and
/// tests use [`FixtureFetcher`] with no live network calls.
pub struct GammaFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> GammaFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// Fetch resolutions for every market ID not already present in `cache`.
    ///
    /// Calls [`WalletCache::resolved_market_ids`] once up front to build the skip set;
    /// markets already in `market_resolutions` are not re-fetched. Each successful
    /// resolution is inserted immediately via `INSERT OR IGNORE` (WAL durability).
    ///
    /// Requests are issued with `buffer_unordered(GAMMA_CONCURRENCY)`. The shared
    /// rate-limit mutex inside [`ReqwestFetcher`] enforces the global throughput
    /// cap (`GAMMA_MIN_INTERVAL_MS`) regardless of concurrency. Results are
    /// inserted serially as they arrive, so `cache` is never accessed from
    /// multiple tasks.
    ///
    /// Returns the count of newly inserted rows. Markets not yet closed, or markets
    /// Fetch scheduled `endDate` for every market ID not already present in `cache`.
    ///
    /// Calls [`WalletCache::scheduled_market_ids`] once up front to build the skip set.
    /// Unlike [`Self::fetch_resolutions`], this method fetches endDates regardless of
    /// whether the market is closed — a live trader sees `endDate` at trade time.
    ///
    /// Each market ID is inserted via `INSERT OR IGNORE` whether or not `end_date_unix`
    /// is `Some`, so it enters the skip-set and will not be re-fetched on the next run.
    ///
    /// Returns the count of newly inserted rows.
    pub async fn fetch_schedules(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        let already_scheduled = cache.scheduled_market_ids();
        let to_fetch: Vec<String> = market_ids
            .iter()
            .filter(|id| !already_scheduled.contains(*id))
            .cloned()
            .collect();

        let total = to_fetch.len();
        info!(
            total,
            already_cached = already_scheduled.len(),
            concurrency = GAMMA_CONCURRENCY,
            "gamma: starting schedule fetch"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let fetcher = &self.fetcher;
        let base_url = self.base_url.as_str();

        let mut stream = stream::iter(to_fetch)
            .map(|market_id| async move {
                let url = format!("{base_url}/markets?condition_ids={market_id}");
                let result = fetcher.fetch_page(&url).await;
                (market_id, result)
            })
            .buffer_unordered(GAMMA_CONCURRENCY);

        let mut inserted = 0usize;
        let mut processed = 0usize;
        while let Some((market_id, result)) = stream.next().await {
            processed += 1;
            if processed.is_multiple_of(1_000) {
                info!(processed, total, inserted, "gamma: schedule fetch progress");
            }

            let bytes = match result {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    tracing::warn!(%market_id, error = %message, "gamma: schedule fetch error, skipping");
                    continue;
                }
                Err(e) => {
                    return Err(BootstrapError::Gamma {
                        message: format!("fetch schedule {market_id}: {e}"),
                    });
                }
            };

            let end_date_unix = match parse_gamma_schedule(&bytes) {
                Ok(ts) => ts,
                Err(e) => {
                    tracing::warn!(%market_id, error = %e, "gamma: schedule parse error, inserting NULL");
                    None
                }
            };

            cache.insert_schedule(&market_id, end_date_unix, fetched_at)?;
            inserted += 1;
        }

        info!(inserted, total, "gamma: schedule fetch complete");
        Ok(inserted)
    }

    /// Rewrite NULL `end_date_unix` rows in `market_schedules` by re-fetching with
    /// the `&closed=true` URL variant, then updating each row via
    /// [`WalletCache::update_schedule_end_date`].
    ///
    /// Background: Gamma's plain `/markets?condition_ids={id}` endpoint silently
    /// returns an empty list for closed markets, which is why pre-PR #137 ~98% of
    /// `source='gamma'` rows had NULL `end_date_unix`. The `&closed=true` query
    /// parameter surfaces closed markets with `endDate` populated. Empirical curl
    /// against 100 random closed trade-set markets: plain URL 0/100 populated,
    /// `&closed=true` 99/100 populated.
    ///
    /// Caller is responsible for filtering `market_ids` to the trade-set scope.
    /// Returns the count of rows actually rewritten — markets where Gamma returns
    /// `endDate=null` (the 1/100 case) leave the row at NULL and contribute 0.
    /// Markets already populated by another source are likewise no-ops, guarded by
    /// the cache method's `WHERE end_date_unix IS NULL` clause.
    pub async fn rewrite_null_schedules(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        let total = market_ids.len();
        info!(
            total,
            concurrency = GAMMA_CONCURRENCY,
            "gamma: starting null-schedule rewrite pass"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let fetcher = &self.fetcher;
        let base_url = self.base_url.as_str();

        let mut stream = stream::iter(market_ids.iter().cloned())
            .map(|market_id| async move {
                let url = format!("{base_url}/markets?condition_ids={market_id}&closed=true");
                let result = fetcher.fetch_page(&url).await;
                (market_id, result)
            })
            .buffer_unordered(GAMMA_CONCURRENCY);

        let mut rewritten = 0usize;
        let mut processed = 0usize;
        while let Some((market_id, result)) = stream.next().await {
            processed += 1;
            if processed.is_multiple_of(1_000) {
                info!(
                    processed,
                    total, rewritten, "gamma: null-schedule rewrite progress"
                );
            }

            let bytes = match result {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    tracing::warn!(%market_id, error = %message, "gamma: rewrite fetch error, skipping");
                    continue;
                }
                Err(e) => {
                    return Err(BootstrapError::Gamma {
                        message: format!("rewrite schedule {market_id}: {e}"),
                    });
                }
            };

            let end_date_unix = match parse_gamma_schedule(&bytes) {
                Ok(Some(ts)) => ts,
                Ok(None) => continue, // closed market with no endDate; leave row NULL
                Err(e) => {
                    tracing::warn!(%market_id, error = %e, "gamma: rewrite parse error, leaving NULL");
                    continue;
                }
            };

            if cache.update_schedule_end_date(&market_id, end_date_unix, fetched_at)? {
                rewritten += 1;
            }
        }

        info!(rewritten, total, "gamma: null-schedule rewrite complete");
        Ok(rewritten)
    }

    /// Fetch Gamma `liquidity` (current order-book depth indicator) for every market ID
    /// not already present in `cache`.
    ///
    /// Calls [`WalletCache::liquid_market_ids`] once up front to build the skip set.
    /// Unlike resolution/schedule fetches, the `liquidity` value changes over time —
    /// re-fetches use `INSERT OR REPLACE`, but the skip-set still suppresses re-fetch
    /// within a single bootstrap run.
    ///
    /// Markets where Gamma returns no `liquidity` field, or a non-numeric value, are
    /// silently skipped (no row inserted; market will be re-attempted on the next run).
    ///
    /// Returns the count of newly inserted rows.
    pub async fn fetch_market_liquidity(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        let already_fetched = cache.liquid_market_ids();
        let to_fetch: Vec<String> = market_ids
            .iter()
            .filter(|id| !already_fetched.contains(*id))
            .cloned()
            .collect();

        let total = to_fetch.len();
        info!(
            total,
            already_cached = already_fetched.len(),
            concurrency = GAMMA_CONCURRENCY,
            "gamma: starting liquidity fetch"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let fetcher = &self.fetcher;
        let base_url = self.base_url.as_str();

        let mut stream = stream::iter(to_fetch)
            .map(|market_id| async move {
                let url = format!("{base_url}/markets?condition_ids={market_id}");
                let result = fetcher.fetch_page(&url).await;
                (market_id, result)
            })
            .buffer_unordered(GAMMA_CONCURRENCY);

        let mut inserted = 0usize;
        let mut processed = 0usize;
        while let Some((market_id, result)) = stream.next().await {
            processed += 1;
            if processed.is_multiple_of(1_000) {
                info!(
                    processed,
                    total, inserted, "gamma: liquidity fetch progress"
                );
            }

            let bytes = match result {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    tracing::warn!(%market_id, error = %message, "gamma: liquidity fetch error, skipping");
                    continue;
                }
                Err(e) => {
                    return Err(BootstrapError::Gamma {
                        message: format!("fetch liquidity {market_id}: {e}"),
                    });
                }
            };

            let liquidity_usd = match parse_gamma_liquidity(&bytes) {
                Ok(Some(v)) => v,
                Ok(None) => continue, // no liquidity field — skip; retry next run
                Err(e) => {
                    tracing::warn!(%market_id, error = %e, "gamma: liquidity parse error, skipping");
                    continue;
                }
            };

            cache.upsert_market_liquidity(&market_id, liquidity_usd, fetched_at)?;
            inserted += 1;
        }

        info!(inserted, total, "gamma: liquidity fetch complete");
        Ok(inserted)
    }
}

/// Serde DTO for a single element of the `/markets` response array.
///
/// Only the fields needed for schedule and liquidity detection are mapped
/// (resolution data has moved to Polygon RPC + CLOB per issue #149).
/// Extra fields are ignored by serde.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketRaw {
    #[allow(dead_code)]
    condition_id: String,
    /// Scheduled close date in RFC 3339 format, e.g. `"2024-11-04T00:00:00Z"`.
    /// Present on open and closed markets. `None` only when Gamma omits the field.
    end_date: Option<String>,
    /// Current order-book depth indicator (USD). Gamma returns this as a JSON
    /// number; the custom deserializer accepts integers, floats, and decimal
    /// strings so callers can write fixtures in either form. `None` when Gamma
    /// omits the field entirely.
    #[serde(default, deserialize_with = "deserialize_decimal_flexible")]
    liquidity: Option<Decimal>,
}

/// Deserialize a JSON value (number or string) into `Option<Decimal>`.
///
/// Gamma returns `liquidity` as a JSON number, but fixtures and other API surfaces
/// sometimes serialize Decimal as a string. Accepting both keeps the DTO robust to
/// upstream format changes without losing precision.
pub(crate) fn deserialize_decimal_flexible<'de, D>(d: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use rust_decimal::prelude::FromPrimitive;
    use serde::de::Error as DeError;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flex {
        Str(String),
        Float(f64),
        Int(i64),
    }

    let Some(v) = Option::<Flex>::deserialize(d)? else {
        return Ok(None);
    };
    match v {
        Flex::Str(s) => s.parse::<Decimal>().map(Some).map_err(DeError::custom),
        Flex::Float(f) => Decimal::from_f64(f)
            .map(Some)
            .ok_or_else(|| DeError::custom(format!("liquidity f64 {f} → Decimal failed"))),
        Flex::Int(i) => Ok(Some(Decimal::from(i))),
    }
}

/// Parse a single-element Gamma `/markets` response and extract the `liquidity` field.
///
/// Returns:
/// - `Some(decimal)` — the market has a parseable `liquidity` value.
/// - `None` — the market was not found (empty array) or Gamma omitted the field.
///
/// Does not require the market to be closed; depth is meaningful only on open markets,
/// but we don't filter at parse time — caller decides whether to use stale closed-market
/// values.
pub fn parse_gamma_liquidity(bytes: &[u8]) -> Result<Option<Decimal>, String> {
    let markets: Vec<GammaMarketRaw> =
        serde_json::from_slice(bytes).map_err(|e| format!("JSON parse: {e}"))?;

    let Some(m) = markets.into_iter().next() else {
        return Ok(None);
    };
    Ok(m.liquidity)
}

/// Parse a single-element Gamma `/markets` response and extract the scheduled `endDate`.
///
/// Returns:
/// - `Some(unix)` — the market has a parseable `endDate` field.
/// - `None` — the market was not found (empty array) or has no `endDate`.
///
/// This function does not require the market to be closed; it extracts the
/// scheduled end time regardless of `closed` status so that the backtest can
/// gate on the date the trader would have seen, not the eventual resolution time.
pub fn parse_gamma_schedule(bytes: &[u8]) -> Result<Option<i64>, String> {
    let markets: Vec<GammaMarketRaw> =
        serde_json::from_slice(bytes).map_err(|e| format!("JSON parse: {e}"))?;

    let Some(m) = markets.into_iter().next() else {
        return Ok(None); // empty array — market not found on Gamma
    };

    let Some(end_date_str) = m.end_date else {
        return Ok(None);
    };

    parse_end_date(&end_date_str).map(Some)
}

/// Parse Gamma's `endDate` RFC 3339 format: `"YYYY-MM-DDTHH:MM:SSZ"`.
///
/// Logs a warning and returns an error string if parsing fails so the caller can
/// insert a NULL row rather than aborting the bootstrap run.
fn parse_end_date(s: &str) -> Result<i64, String> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map(|dt| dt.unix_timestamp())
        .map_err(|e| format!("parse endDate {s:?}: {e}"))
}

// (`parse_closed_time` was removed in issue #149 Cycle 2 — its only caller
// `fetch_resolutions` is gone now that Polygon RPC + CLOB own resolution
// data, and Polygon's block-timestamp / CLOB's `end_date_iso` are richer
// than Gamma's `closedTime` ever was.)

// ── Convenience: build a ResolutionIndex without running a full bootstrap ─────

/// Load all resolutions from `cache` into a [`ResolutionIndex`].
///
/// Thin wrapper that delegates to [`WalletCache::load_all_resolutions`]; provided
/// here so callers don't need to import both `gamma` and `cache`.
pub fn load_resolutions(cache: &WalletCache) -> Result<ResolutionIndex, BootstrapError> {
    cache.load_all_resolutions()
}

/// Load all schedule rows from `cache` into a [`ScheduleIndex`].
///
/// Thin wrapper that delegates to [`WalletCache::load_all_schedules`]; provided
/// here so callers don't need to import both `gamma` and `cache`.
pub fn load_schedules(cache: &WalletCache) -> Result<ScheduleIndex, BootstrapError> {
    cache.load_all_schedules()
}

/// Load all liquidity rows from `cache` into a [`LiquidityIndex`].
///
/// Thin wrapper that delegates to [`WalletCache::load_all_liquidity`]; provided
/// here so callers don't need to import both `gamma` and `cache`.
pub fn load_liquidity(cache: &WalletCache) -> Result<LiquidityIndex, BootstrapError> {
    cache.load_all_liquidity()
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

    fn empty_array_fixture() -> Vec<u8> {
        fixture_bytes("[]")
    }

    // ── schedule-specific tests ───────────────────────────────────────────────

    fn open_with_end_date_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":false,"closedTime":null,"outcomePrices":null,"endDate":"2026-07-31T12:00:00Z","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn open_no_end_date_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":false,"closedTime":null,"outcomePrices":null,"outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn closed_with_end_date_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"1\",\"0\"]","endDate":"2024-01-15T00:00:00Z","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    #[test]
    fn parse_gamma_schedule_open_market_with_end_date() {
        // 2026-07-31T12:00:00Z = 1785499200
        let ts = parse_gamma_schedule(&open_with_end_date_fixture())
            .unwrap()
            .unwrap();
        assert_eq!(
            ts, 1_785_499_200,
            "RFC 3339 endDate must parse to correct unix seconds"
        );
    }

    #[test]
    fn parse_gamma_schedule_missing_end_date_returns_none() {
        let result = parse_gamma_schedule(&open_no_end_date_fixture()).unwrap();
        assert!(result.is_none(), "missing endDate field must return None");
    }

    #[test]
    fn parse_gamma_schedule_closed_market_returns_end_date() {
        // 2024-01-15T00:00:00Z = 1705276800
        let ts = parse_gamma_schedule(&closed_with_end_date_fixture())
            .unwrap()
            .unwrap();
        assert_eq!(
            ts, 1_705_276_800,
            "closed market endDate must parse correctly"
        );
    }

    #[test]
    fn parse_gamma_schedule_empty_array_returns_none() {
        let result = parse_gamma_schedule(&empty_array_fixture()).unwrap();
        assert!(result.is_none(), "empty array must return None");
    }

    #[test]
    fn parse_end_date_rfc3339() {
        // Verify the RFC 3339 format parses correctly.
        // 2020-11-04T00:00:00Z = 1604448000
        let ts = parse_end_date("2020-11-04T00:00:00Z").unwrap();
        assert_eq!(ts, 1_604_448_000, "endDate must round-trip to unix seconds");
    }

    #[test]
    fn fetch_schedules_skips_already_cached_ids() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Pre-insert one market as already-scheduled.
        cache
            .insert_schedule("0xa", Some(1_700_000_000), 1_700_000_001)
            .unwrap();

        // Fixture only handles "0xb"; if "0xa" were fetched it would return Fatal.
        let url_b = "https://gamma-api.polymarket.com/markets?condition_ids=0xb";
        let mut responses = HashMap::new();
        responses.insert(url_b.to_owned(), open_with_end_date_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inserted = rt
            .block_on(gamma.fetch_schedules(&["0xa".to_owned(), "0xb".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 1, "only 0xb must be fetched (0xa was cached)");
        assert_eq!(cache.load_all_schedules().unwrap().len(), 2);
    }

    fn closed_no_end_date_fixture() -> Vec<u8> {
        // Closed market that genuinely has endDate=null — the 1/100 case in the
        // empirical probe. `rewrite_null_schedules` must leave the row at NULL.
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    #[test]
    fn rewrite_null_schedules_populates_via_closed_endpoint() {
        // End-to-end happy path: NULL row exists, &closed=true returns endDate,
        // cache row is updated. Verifies the URL change + cache wiring.
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", None, 1_700_000_000)
            .unwrap();

        let closed_url =
            "https://gamma-api.polymarket.com/markets?condition_ids=0xcond&closed=true";
        let mut responses = HashMap::new();
        responses.insert(closed_url.to_owned(), closed_with_end_date_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let rewritten = rt
            .block_on(gamma.rewrite_null_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(rewritten, 1, "single populated response must rewrite 1 row");

        let idx = cache.load_all_schedules().unwrap();
        let sched = idx.values().next().unwrap();
        assert_eq!(
            sched.end_date_unix,
            Some(1_705_276_800),
            "end_date must be the parsed closed-market value"
        );
        // No more NULL rows.
        assert!(cache.null_schedule_market_ids().is_empty());
    }

    #[test]
    fn rewrite_null_schedules_leaves_row_null_when_gamma_returns_no_enddate() {
        // The 1/100 case: closed market that Gamma returns but with endDate=null.
        // Row must stay NULL, rewritten counter must be 0.
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", None, 1_700_000_000)
            .unwrap();

        let closed_url =
            "https://gamma-api.polymarket.com/markets?condition_ids=0xcond&closed=true";
        let mut responses = HashMap::new();
        responses.insert(closed_url.to_owned(), closed_no_end_date_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let rewritten = rt
            .block_on(gamma.rewrite_null_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(rewritten, 0, "no endDate in response → no row rewritten");

        let idx = cache.load_all_schedules().unwrap();
        let sched = idx.values().next().unwrap();
        assert_eq!(sched.end_date_unix, None, "NULL must be preserved");
    }

    #[test]
    fn rewrite_null_schedules_uses_closed_true_url_not_plain() {
        // Regression guard: the URL MUST include &closed=true, otherwise Gamma
        // returns empty list for closed markets and we silently get rewritten=0.
        // We map only the &closed=true URL; a fetch against the plain URL would
        // miss and return SourceError::Fatal (which the loop skips with a warn).
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", None, 1_700_000_000)
            .unwrap();

        let closed_url =
            "https://gamma-api.polymarket.com/markets?condition_ids=0xcond&closed=true";
        // NOTE: deliberately NOT mapping the plain URL — if the implementation
        // regresses to that URL, the request 404s under FixtureFetcher and
        // rewritten stays 0.
        let mut responses = HashMap::new();
        responses.insert(closed_url.to_owned(), closed_with_end_date_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let rewritten = rt
            .block_on(gamma.rewrite_null_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(
            rewritten, 1,
            "&closed=true URL must be used; rewritten=0 here would indicate the \
             implementation regressed to the plain URL"
        );
    }

    #[test]
    fn fetch_schedules_inserts_null_for_missing_end_date() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let url = "https://gamma-api.polymarket.com/markets?condition_ids=0xcond";
        let mut responses = HashMap::new();
        responses.insert(url.to_owned(), open_no_end_date_fixture());
        let fetcher = FixtureFetcher::new(responses);
        let gamma = GammaFetcher::new("https://gamma-api.polymarket.com".to_owned(), fetcher);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inserted = rt
            .block_on(gamma.fetch_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 1, "missing endDate must still insert a row");
        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(idx.len(), 1, "NULL row must be present in index");
        let sched = idx.values().next().unwrap();
        assert_eq!(sched.end_date_unix, None, "end_date_unix must be NULL");
        // NULL row must also be in the skip-set.
        assert_eq!(cache.scheduled_market_ids().len(), 1);
    }
}
