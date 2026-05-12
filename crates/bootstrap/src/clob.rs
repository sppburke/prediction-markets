//! Polymarket CLOB paginated client — closed markets only.
//!
//! Issue #149: the CLOB `/markets?closed=true` endpoint exposes every
//! closed Polymarket market with its `tokens[].winner` flag and
//! `end_date_iso`. Unlike Gamma it does not purge resolved markets, so it
//! serves as the gap-filler for resolutions that Polygon RPC missed
//! (multi-outcome markets, oracle redirections, etc.).
//!
//! Pagination is sequential because each page returns the cursor for the
//! next; `buffer_unordered` does not apply. The `source_cursor.clob_closed`
//! row persists the cursor so daily re-runs resume from the last page
//! instead of re-walking ~200 pages each tick.
//!
//! **Approximation:** `resolved_at_unix` is set to the parsed
//! `end_date_iso` because CLOB does not expose a block-timestamp
//! resolution time. Polygon RPC + Dune provide the authoritative value
//! and win on `INSERT OR IGNORE` ordering, so CLOB's approximation only
//! sticks for markets neither of those sources catches.

use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::cache::WalletCache;
use crate::error::BootstrapError;

/// `source_cursor` key for the CLOB closed-market pagination checkpoint.
/// Value is the next-page `next_cursor` string returned by the CLOB API,
/// or the empty string when the cursor has not yet been initialised.
pub const CLOB_CLOSED_CURSOR_KEY: &str = "clob_closed";

/// CLOB pagination terminator returned by `/markets` when the cursor has
/// reached the end of the result set.
///
/// Documented by the CLOB API as the base64 encoding of `-1`. The exact
/// string is verified at runtime: if the terminator semantics change, the
/// next-page fetch will simply return an empty `data` array and the loop
/// exits naturally (over-pagination is benign because every insert is
/// `INSERT OR IGNORE`).
const CLOB_END_CURSOR: &str = "LTE=";

/// Page size requested from the CLOB `/markets` endpoint. The API caps
/// `limit` at 1000.
const CLOB_PAGE_LIMIT: usize = 1000;

/// Paginated client for the Polymarket CLOB `/markets` endpoint.
///
/// Generic over [`PageFetcher`] so production uses [`ReqwestFetcher`] and
/// tests use a fixture map with no live network. Mirrors the
/// [`crate::gamma::GammaFetcher`] shape so the two paginators share the
/// same testing infrastructure.
pub struct ClobFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> ClobFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// Paginate `/markets?closed=true` and insert every market's schedule
    /// (always) and resolution (when a winner is set) into `cache`.
    ///
    /// Pagination resumes from `source_cursor.clob_closed` when present,
    /// so daily re-runs only walk pages that have appeared since the
    /// previous tick. The cursor advances after each successful page;
    /// a crash mid-page replays that page on restart, idempotent via
    /// `INSERT OR IGNORE`.
    ///
    /// Returns `(schedules_inserted, resolutions_inserted)`. Counts
    /// reflect only newly-inserted rows; rows already present (e.g.
    /// from an earlier Polygon RPC pass) silently no-op.
    pub async fn fetch_closed_markets(
        &self,
        cache: &mut WalletCache,
    ) -> Result<(usize, usize), BootstrapError> {
        let mut cursor: Option<String> = cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY);

        // Early-out when the stored cursor is already the terminator: a prior
        // run reached end-of-pages, or an operator manually advanced past a
        // broken page. Without this check the loop would feed the terminator
        // into [`build_page_url`], which deliberately treats it like "no
        // cursor" (first page), causing a full re-walk. To force a re-walk
        // intentionally, clear the cursor row:
        // `cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, "")`.
        if cursor.as_deref() == Some(CLOB_END_CURSOR) {
            info!(
                "clob: stored cursor is at terminator — skipping closed-market fetch \
                 (clear source_cursor.clob_closed to re-walk)"
            );
            return Ok((0, 0));
        }

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let mut schedules = 0usize;
        let mut resolutions = 0usize;
        let mut page_count = 0usize;

        info!(
            base_url = self.base_url.as_str(),
            resume_from = cursor.as_deref().unwrap_or("<start>"),
            "clob: starting closed-market fetch"
        );

        loop {
            let url = build_page_url(&self.base_url, cursor.as_deref());
            let bytes = match self.fetcher.fetch_page(&url).await {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    warn!(%url, %message, "clob: fatal fetch error — aborting page");
                    return Err(BootstrapError::Clob {
                        message: format!("fetch {url}: {message}"),
                    });
                }
                Err(e) => {
                    return Err(BootstrapError::Clob {
                        message: format!("fetch {url}: {e}"),
                    });
                }
            };

            let page: ClobMarketsPage =
                serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Clob {
                    message: format!("parse page: {e}"),
                })?;
            page_count += 1;
            let markets_in_page = page.data.len();

            for market in page.data {
                let Some(condition_id) = market.condition_id else {
                    continue;
                };
                let end_date_unix = market.end_date_iso.as_deref().and_then(parse_iso_8601);

                // Schedule: always insert; NULL end_date for malformed/missing.
                cache.insert_schedule_with_source(
                    &condition_id,
                    end_date_unix,
                    fetched_at,
                    "clob",
                )?;
                schedules += 1;

                // Resolution: only when the market is closed AND `end_date_iso`
                // parsed cleanly. CLOB's `end_date_iso` is the best timestamp we
                // have (the API does not expose a block-timestamp resolution
                // time), so a malformed value would otherwise stamp the row
                // with `resolved_at = now()`, lying about when the market
                // actually settled. Skipping leaves the market unresolved so
                // Polygon RPC or Dune can fill in an accurate timestamp later.
                let winner = winner_index(&market.tokens);
                if market.closed
                    && let Some(resolved_at) = end_date_unix
                {
                    cache.insert_resolution_with_source(
                        &condition_id,
                        winner,
                        resolved_at,
                        fetched_at,
                        "clob",
                    )?;
                    resolutions += 1;
                }
            }

            // Advance cursor. Treat empty/missing/terminator as end-of-pages.
            let next = page.next_cursor.as_deref().unwrap_or("");
            if next.is_empty() || next == CLOB_END_CURSOR {
                info!(
                    page_count,
                    schedules, resolutions, "clob: reached end of pages"
                );
                // Persist terminator so the next daily run knows we're caught up.
                cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, next)?;
                break;
            }
            cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, next)?;
            cursor = Some(next.to_owned());

            if page_count.is_multiple_of(10) {
                info!(
                    page_count,
                    markets_in_page, schedules, resolutions, "clob: pagination progress"
                );
            }
        }

        Ok((schedules, resolutions))
    }
}

fn build_page_url(base_url: &str, cursor: Option<&str>) -> String {
    match cursor {
        Some(c) if !c.is_empty() && c != CLOB_END_CURSOR => {
            format!("{base_url}/markets?closed=true&limit={CLOB_PAGE_LIMIT}&next_cursor={c}")
        }
        _ => format!("{base_url}/markets?closed=true&limit={CLOB_PAGE_LIMIT}"),
    }
}

/// Walk `tokens` and return the index of the first `winner=true` entry, or
/// `None` if every token has `winner=false` (market closed but voided).
///
/// Index is taken positionally because CLOB returns tokens in YES/NO order
/// for binary markets; multi-outcome markets follow the same convention.
fn winner_index(tokens: &[ClobToken]) -> Option<u8> {
    let winners: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| t.winner)
        .map(|(i, _)| i)
        .collect();
    if winners.len() == 1 {
        u8::try_from(winners[0]).ok()
    } else {
        None
    }
}

/// Parse an ISO-8601 timestamp into a unix-seconds value, or return `None`
/// for malformed/missing input.
fn parse_iso_8601(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

// ── Serde DTOs ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ClobMarketsPage {
    #[serde(default)]
    data: Vec<ClobMarket>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct ClobMarket {
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    end_date_iso: Option<String>,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    tokens: Vec<ClobToken>,
}

#[derive(Deserialize)]
struct ClobToken {
    #[serde(default)]
    winner: bool,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn winner_index_yes_picks_zero() {
        let tokens = vec![ClobToken { winner: true }, ClobToken { winner: false }];
        assert_eq!(winner_index(&tokens), Some(0));
    }

    #[test]
    fn winner_index_no_picks_one() {
        let tokens = vec![ClobToken { winner: false }, ClobToken { winner: true }];
        assert_eq!(winner_index(&tokens), Some(1));
    }

    #[test]
    fn winner_index_voided_is_none() {
        let tokens = vec![ClobToken { winner: false }, ClobToken { winner: false }];
        assert_eq!(winner_index(&tokens), None);
    }

    #[test]
    fn winner_index_two_winners_is_none() {
        // Should never happen in production but guard against the
        // first-non-zero pitfall analogous to polygon_ctf's tied case.
        let tokens = vec![ClobToken { winner: true }, ClobToken { winner: true }];
        assert_eq!(winner_index(&tokens), None);
    }

    #[test]
    fn parse_iso_round_trips_known_value() {
        // 2024-11-04T00:00:00Z = 1730678400
        assert_eq!(parse_iso_8601("2024-11-04T00:00:00Z"), Some(1_730_678_400));
    }

    #[test]
    fn parse_iso_returns_none_for_malformed() {
        assert!(parse_iso_8601("not a date").is_none());
    }

    #[test]
    fn build_page_url_first_page_has_no_cursor() {
        let url = build_page_url("https://clob.example", None);
        assert_eq!(url, "https://clob.example/markets?closed=true&limit=1000");
    }

    #[test]
    fn build_page_url_subsequent_page_appends_cursor() {
        let url = build_page_url("https://clob.example", Some("MQ=="));
        assert_eq!(
            url,
            "https://clob.example/markets?closed=true&limit=1000&next_cursor=MQ=="
        );
    }

    #[test]
    fn build_page_url_treats_terminator_as_first_page() {
        // Defensive: if cursor==LTE= leaks back through cache, don't request
        // it (the API would return an empty page).
        let url = build_page_url("https://clob.example", Some(CLOB_END_CURSOR));
        assert_eq!(url, "https://clob.example/markets?closed=true&limit=1000");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_closed_markets_early_returns_when_cursor_is_terminator() {
        // Regression guard for the finish run of issue #149: the stored
        // terminator must short-circuit the loop so a previous run's
        // end-of-pages signal (or operator-set skip past a broken page) does
        // not trigger a full re-walk on the next invocation.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .set_source_cursor(CLOB_CLOSED_CURSOR_KEY, CLOB_END_CURSOR)
            .unwrap();

        // Empty fixture: any URL request would Fatal-error. If the early-out
        // were missing, the loop would try to fetch page 1 here and the test
        // would fail with a Fatal-fetch BootstrapError.
        let fetcher = pe_source_polymarket_public::FixtureFetcher::new(HashMap::new());
        let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

        let (schedules, resolutions) = clob.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(schedules, 0);
        assert_eq!(resolutions, 0);
        // Cursor stays at the terminator — caller controls re-walk by clearing it.
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some(CLOB_END_CURSOR)
        );
    }
}
