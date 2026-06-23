//! Polymarket CLOB paginated client — closed markets only.
//!
//! Issue #149 / #369: the CLOB `/markets?closed=true` endpoint exposes every
//! closed Polymarket market with its `tokens[].winner` flag and `end_date_iso`.
//! Unlike Gamma it does not purge resolved markets, so since #369 it is the
//! **sole, primary** market-resolution source (the on-chain Polygon RPC scan was
//! removed). Existing `source='polygon'` rows are retained — `INSERT OR IGNORE`
//! never overwrites them — so CLOB is authoritative for markets polygon never
//! resolved and for all new markets ("primary-for-new").
//!
//! Pagination is sequential because each page returns the cursor for the
//! next; `buffer_unordered` does not apply. The `source_cursor.clob_closed`
//! row persists the cursor so daily re-runs resume from the last page
//! instead of re-walking ~200 pages each tick.
//!
//! **Approximation:** `resolved_at_unix` is set to the parsed `end_date_iso`
//! because CLOB does not expose a block-timestamp resolution time. The retained
//! legacy `source='polygon'` rows keep their exact block timestamps (and win on
//! `INSERT OR IGNORE` ordering); only new markets carry the CLOB approximation.

use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::{error, info, warn};

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

/// Outcome of a CLOB closed-markets sweep.
///
/// `tokens_mapped` counts `token_conditions` rows written this run (issue #429).
/// A market whose CLOB `tokens[]` order diverges from the authoritative Gamma
/// `clob_token_ids` order is **quarantined** — its token rows are skipped (never
/// written) and counted in `order_mismatches` — so a real divergence cannot
/// silently misprice the downstream `true_clv` outcome→token join. Counts reflect
/// only this run's newly-processed pages.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClobReport {
    pub schedules: usize,
    pub resolutions: usize,
    pub tokens_mapped: usize,
    pub order_mismatches: usize,
}

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
    /// Also maps every market's CLOB `tokens[]` into `token_conditions`
    /// (`token_id → condition_id`, with the positional `outcome_index`) so the
    /// trades / price-series join can resolve outcome → token (issue #429). The
    /// closed-markets sweep is the only full-universe token source — Gamma
    /// `events` covers a curated slice — so this maps the universe at zero extra
    /// network cost. See [`ClobReport`] for the order-divergence quarantine.
    ///
    /// Returns a [`ClobReport`]. Schedule/resolution counts reflect only
    /// newly-inserted rows; rows already present (e.g. a retained
    /// `source='polygon'` row) silently no-op.
    pub async fn fetch_closed_markets(
        &self,
        cache: &mut WalletCache,
    ) -> Result<ClobReport, BootstrapError> {
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
            return Ok(ClobReport::default());
        }

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let mut schedules = 0usize;
        let mut resolutions = 0usize;
        let mut tokens_mapped = 0usize;
        let mut order_mismatches = 0usize;
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
                    warn!(%url, error = %message, "clob: fatal fetch error — aborting page");
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

            // `(token_id, condition_id, outcome_index)` rows for this page,
            // flushed in one transaction after the per-market loop (issue #429).
            let mut page_token_rows: Vec<(String, String, u16)> = Vec::new();

            for market in page.data {
                let Some(condition_id) = market.condition_id.filter(|c| !c.is_empty()) else {
                    // CLOB returns draft/undeployed entries with an empty (or
                    // absent) conditionId; skip them entirely — a non-market has no
                    // schedule, resolution, or token map. Mirrors events.rs's
                    // empty-cond skip; an empty id would otherwise write junk rows
                    // and (issue #429) collide every empty token on the `""` PK.
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
                // actually settled. Skipping leaves the market unresolved until a
                // later run parses a clean `end_date_iso` (issue #369: CLOB is the
                // sole resolution source — there is no on-chain backfill).
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

                // Map this market's CLOB tokens (token_id → condition_id, with the
                // positional outcome_index) so the trades / price-series join can
                // resolve outcome → token (issue #429).
                //
                // Order cross-check: the join's correctness rests on CLOB
                // `tokens[]` order matching the authoritative Gamma
                // `clob_token_ids` order. `token_id` is the on-chain CTF
                // positionId — a globally-unique (condition, outcome) anchor — so
                // if a token already mapped by the Gamma `events` sweep carries an
                // `outcome_index` that differs from its CLOB array position, the
                // two orderings disagree. Quarantine the whole market (write none
                // of its tokens) so a real divergence fails loudly instead of
                // silently mispricing the true_clv join. Checked against the prior
                // stored row *before* the batch's `INSERT OR REPLACE` overwrites it.
                let mut market_tokens: Vec<(String, String, u16)> = Vec::new();
                let mut quarantined = false;
                for (idx, token) in market.tokens.iter().enumerate() {
                    let Some(token_id) = token.token_id.as_deref().filter(|t| !t.is_empty()) else {
                        // No usable token id — absent (≈5% of markets) or empty `""`
                        // (draft outcomes). Skip this outcome but keep `idx` so later
                        // outcomes stay aligned with the authoritative order, and so
                        // empty ids never collide on the `""` PK / trip the cross-check
                        // (issue #429 follow-up).
                        continue;
                    };
                    let Ok(pos) = u16::try_from(idx) else {
                        // Outcome cardinality > u16::MAX is impossible for a real
                        // market; skip rather than truncate.
                        continue;
                    };
                    if let Some((stored_cond, stored_idx)) = cache.token_condition_outcome(token_id)
                    {
                        let cond_mismatch = stored_cond != condition_id;
                        let order_mismatch = stored_idx.is_some_and(|s| s != i64::from(pos));
                        if cond_mismatch || order_mismatch {
                            error!(
                                condition_id = condition_id.as_str(),
                                token_id,
                                clob_position = pos,
                                stored_condition = stored_cond.as_str(),
                                stored_outcome_index = ?stored_idx,
                                "clob: token order/condition divergence vs stored Gamma map — \
                                 quarantining market (token rows skipped to avoid mispricing the \
                                 true_clv join, issue #429)"
                            );
                            quarantined = true;
                            break;
                        }
                    }
                    market_tokens.push((token_id.to_owned(), condition_id.clone(), pos));
                }
                if quarantined {
                    order_mismatches += 1;
                } else {
                    page_token_rows.extend(market_tokens);
                }
            }

            // Flush this page's token map in one transaction (issue #429).
            if !page_token_rows.is_empty() {
                cache.upsert_token_conditions_batch(&page_token_rows, fetched_at)?;
                tokens_mapped += page_token_rows.len();
            }

            // Advance cursor. Treat empty/missing/terminator as end-of-pages.
            let next = page.next_cursor.as_deref().unwrap_or("");
            if next.is_empty() || next == CLOB_END_CURSOR {
                info!(
                    page_count,
                    schedules,
                    resolutions,
                    tokens_mapped,
                    order_mismatches,
                    "clob: reached end of pages"
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
                    markets_in_page,
                    schedules,
                    resolutions,
                    tokens_mapped,
                    order_mismatches,
                    "clob: pagination progress"
                );
            }
        }

        Ok(ClobReport {
            schedules,
            resolutions,
            tokens_mapped,
            order_mismatches,
        })
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
fn winner_index(tokens: &[ClobToken]) -> Option<u16> {
    let winners: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| t.winner)
        .map(|(i, _)| i)
        .collect();
    if winners.len() == 1 {
        u16::try_from(winners[0]).ok()
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
    /// ERC-1155 CTF positionId (decimal-string uint256) — the same id Gamma
    /// returns in `clobTokenIds` and the on-chain `OrderFilled` logs carry.
    /// Absent on ~5% of markets (issue #429 live probe); those tokens are skipped
    /// for mapping but the market's resolution/schedule still insert.
    #[serde(default)]
    token_id: Option<String>,
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
        let tokens = vec![
            ClobToken {
                winner: true,
                token_id: None,
            },
            ClobToken {
                winner: false,
                token_id: None,
            },
        ];
        assert_eq!(winner_index(&tokens), Some(0));
    }

    #[test]
    fn winner_index_no_picks_one() {
        let tokens = vec![
            ClobToken {
                winner: false,
                token_id: None,
            },
            ClobToken {
                winner: true,
                token_id: None,
            },
        ];
        assert_eq!(winner_index(&tokens), Some(1));
    }

    #[test]
    fn winner_index_voided_is_none() {
        let tokens = vec![
            ClobToken {
                winner: false,
                token_id: None,
            },
            ClobToken {
                winner: false,
                token_id: None,
            },
        ];
        assert_eq!(winner_index(&tokens), None);
    }

    #[test]
    fn winner_index_two_winners_is_none() {
        // Should never happen in production but guard against the first-non-zero
        // pitfall: two `winner=true` tokens must yield None, not the first index.
        let tokens = vec![
            ClobToken {
                winner: true,
                token_id: None,
            },
            ClobToken {
                winner: true,
                token_id: None,
            },
        ];
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

        let report = clob.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(report.schedules, 0);
        assert_eq!(report.resolutions, 0);
        // Cursor stays at the terminator — caller controls re-walk by clearing it.
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some(CLOB_END_CURSOR)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_closed_markets_writes_token_conditions_with_outcome_index() {
        // Parser/ingestion fixture (issue #429): a CLOB page carrying `token_id`s
        // writes `token_conditions` rows with the correct positional
        // `outcome_index` for BOTH a binary and a multi-outcome market, and the
        // schedule/resolution rows still insert.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let page = br#"{
          "data": [
            {"condition_id":"0xbin","end_date_iso":"2024-01-15T00:00:00Z","closed":true,
             "tokens":[{"token_id":"100","winner":true},{"token_id":"200","winner":false}]},
            {"condition_id":"0xmulti","end_date_iso":"2024-02-01T00:00:00Z","closed":true,
             "tokens":[{"token_id":"300","winner":false},{"token_id":"400","winner":true},{"token_id":"500","winner":false}]}
          ],
          "next_cursor":"LTE="
        }"#.to_vec();
        let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
        responses.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            page,
        );
        let clob = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(responses),
        );

        let report = clob.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(report.tokens_mapped, 5, "2 binary + 3 multi tokens mapped");
        assert_eq!(report.order_mismatches, 0, "no prior map ⇒ no divergence");

        // Binary: YES=0, NO=1. Multi-outcome: positional 0/1/2.
        assert_eq!(
            cache.token_condition_outcome("100"),
            Some(("0xbin".to_owned(), Some(0)))
        );
        assert_eq!(
            cache.token_condition_outcome("200"),
            Some(("0xbin".to_owned(), Some(1)))
        );
        assert_eq!(
            cache.token_condition_outcome("300"),
            Some(("0xmulti".to_owned(), Some(0)))
        );
        assert_eq!(
            cache.token_condition_outcome("400"),
            Some(("0xmulti".to_owned(), Some(1)))
        );
        assert_eq!(
            cache.token_condition_outcome("500"),
            Some(("0xmulti".to_owned(), Some(2)))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_closed_markets_skips_empty_ids_without_quarantine() {
        // Issue #429 PR1 follow-up: CLOB returns draft/undeployed entries with an
        // empty condition_id and outcomes with an empty token_id. Empties must be
        // SKIPPED — never treated as valid ids that collide on the `""` PK and trip
        // the order cross-check (which produced a spurious ~5.6% quarantine storm
        // on the live re-walk before this fix).
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let page = br#"{
          "data": [
            {"condition_id":"0xreal","end_date_iso":"2024-01-15T00:00:00Z","closed":true,
             "tokens":[{"token_id":"R0","winner":true},{"token_id":"","winner":false}]},
            {"condition_id":"","end_date_iso":"2024-02-01T00:00:00Z","closed":true,
             "tokens":[{"token_id":"X","winner":true},{"token_id":"Y","winner":false}]},
            {"condition_id":"0xreal2","end_date_iso":"2024-03-01T00:00:00Z","closed":true,
             "tokens":[{"token_id":"","winner":false},{"token_id":"S1","winner":true}]}
          ],
          "next_cursor":"LTE="
        }"#
        .to_vec();
        let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
        responses.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            page,
        );
        let clob = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(responses),
        );

        let report = clob.fetch_closed_markets(&mut cache).await.unwrap();

        // Empty ids skipped ⇒ no `""`-PK collision ⇒ no spurious quarantine.
        assert_eq!(
            report.order_mismatches, 0,
            "empty ids must be skipped, not quarantined"
        );
        // Only the two real tokens map (R0, S1); the stray empty tokens and the
        // entire empty-condition market are skipped.
        assert_eq!(report.tokens_mapped, 2);
        // R0 keeps index 0; S1 keeps index 1 even though a leading empty token was
        // skipped (position preserved).
        assert_eq!(
            cache.token_condition_outcome("R0"),
            Some(("0xreal".to_owned(), Some(0)))
        );
        assert_eq!(
            cache.token_condition_outcome("S1"),
            Some(("0xreal2".to_owned(), Some(1)))
        );
        // The empty token id is never written (no `""` PK row), and the
        // empty-condition market is skipped entirely (its tokens never map).
        assert_eq!(cache.token_condition_outcome(""), None);
        assert_eq!(
            cache.token_condition_outcome("X"),
            None,
            "empty-condition market is skipped entirely"
        );
        // The empty-condition market also writes no schedule.
        assert_eq!(
            report.schedules, 2,
            "only the two real markets insert schedules"
        );
    }
}
