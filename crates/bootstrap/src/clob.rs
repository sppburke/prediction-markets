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
//! next; `buffer_unordered` does not apply. The legacy `source_cursor.clob_closed`
//! remains a sealed v1 resume cursor. Version-two payout evidence owns an
//! independent staging cursor and is installed only with a complete coverage
//! manifest from page one through a terminal response (#544).
//!
//! **Approximation:** `resolved_at_unix` is set to the parsed `end_date_iso`
//! because CLOB does not expose a block-timestamp resolution time. The retained
//! legacy `source='polygon'` rows keep their exact block timestamps (and win on
//! `INSERT OR IGNORE` ordering); only new markets carry the CLOB approximation.

use std::time::Duration;

use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    CLOB_END_CURSOR, ClobCoverageManifest, ClobCoveragePage, ClobMarket, ClobMarketsPage,
    PageFetcher, is_clob_terminal_cursor, parse_clob_market, parse_clob_markets_page,
};
use time::OffsetDateTime;
use tracing::{error, info, warn};

use crate::cache::WalletCache;
use crate::error::BootstrapError;

/// `source_cursor` key for the CLOB closed-market pagination checkpoint.
/// Value is the next-page `next_cursor` string returned by the CLOB API,
/// or the empty string when no walk has started or the last walk completed.
pub const CLOB_CLOSED_CURSOR_KEY: &str = "clob_closed";

/// CLOB pagination terminator returned by `/markets` when the cursor has
/// reached the end of the result set.
///
/// Documented by the CLOB API as the base64 encoding of `-1`. The exact
/// string is verified at runtime: if the terminator semantics change, the
/// next-page fetch will simply return an empty `data` array and the loop
/// exits naturally (over-pagination is benign because every insert is
/// `INSERT OR IGNORE`).
pub(crate) use pe_source_polymarket_public::{
    ClobWinnerVerdict as WinnerVerdict, analyze_clob_winners as analyze_winners,
    parse_clob_end_date as parse_iso_8601,
};

/// Page size requested from the CLOB `/markets` endpoint. The API caps
/// `limit` at 1000.
const CLOB_PAGE_LIMIT: usize = 1000;

/// Max page-level retries on a transient / rate-limited CLOB fetch error before
/// aborting the walk (issue #429 follow-up). This sits *on top of*
/// [`pe_source_polymarket_public::ReqwestFetcher`]'s internal fast retries — it
/// rides through *sustained* flakiness (e.g. a minute of `error decoding
/// response body`) so one bad page does not abort a ~1,457-page walk. Canonical
/// default in `docs/_GLOSSARY.md` "Bootstrap defaults".
const CLOB_PAGE_MAX_RETRIES: u32 = 5;

/// Base backoff (ms) for the page-level retry; exponential (`base · 2^attempt`),
/// capped at 30s. With the default 5 retries the inter-attempt backoff sums to
/// ~31s (1+2+4+8+16) before giving up; total wall-time per page is higher because
/// each attempt also spends `ReqwestFetcher`'s own retries/timeout. Canonical
/// default in `docs/_GLOSSARY.md`.
const CLOB_PAGE_RETRY_BASE_MS: u64 = 1_000;

/// Floor (seconds) applied to a `RateLimited` `retry_after` so a `0`/missing
/// value cannot busy-loop the page retry. Mirrors the
/// `bootstrap_polymarket_min_retry_after_secs` floor — a defensive bound (like
/// the 30s `clob_retry_backoff` cap), not a separately tuned threshold.
const CLOB_RATE_LIMIT_MIN_WAIT_SECS: u64 = 1;

/// Outcome of a CLOB closed-markets sweep.
///
/// `schedules`, `resolutions`, and `tokens_mapped` count candidates processed
/// this run, not rows changed; idempotent inserts and identical conditional
/// token upserts can leave the database unchanged (issue #519).
/// A market whose CLOB `tokens[]` order diverges from the authoritative Gamma
/// `clob_token_ids` order is **quarantined** — its token rows are skipped (never
/// written) and counted in `order_mismatches` — so a real divergence cannot
/// silently misprice the downstream `true_clv` outcome→token join. Counts reflect
/// only this run's newly-processed pages.
#[derive(Debug, Default, Clone)]
pub struct ClobReport {
    pub schedules: usize,
    pub resolutions: usize,
    pub tokens_mapped: usize,
    pub order_mismatches: usize,
    /// Present only when this invocation installed a complete v2 payout walk.
    pub coverage_manifest: Option<ClobCoverageManifest>,
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

    /// Fetch one response, retrying transient / rate-limited errors with backoff
    /// before giving up (issue #429 follow-up).
    ///
    /// [`pe_source_polymarket_public::ReqwestFetcher`] already retries fast
    /// transient blips internally; this rides through *sustained* flakiness so a
    /// single bad page (e.g. a minute of `error decoding response body`) does not
    /// abort a ~1,457-page walk. `Fatal` errors abort immediately (a 4xx is not
    /// retryable); `Transient` backs off exponentially and `RateLimited` waits the
    /// server's `retry_after` (floored at [`CLOB_RATE_LIMIT_MIN_WAIT_SECS`]). Both
    /// retryable arms share one budget: after [`CLOB_PAGE_MAX_RETRIES`] the
    /// exhausted `Transient`/`RateLimited` error returns as
    /// [`BootstrapError::TransientSource`] — the typed temporary error whose
    /// generic exit-code mapping is the tempfail 75 the loop supervisor retries
    /// (#534; mirrors the events-walk precedent). Fatal fetch and response-parse
    /// failures remain [`BootstrapError::Clob`] (permanent, exit 1).
    async fn fetch_page_with_retry(&self, url: &str) -> Result<Vec<u8>, BootstrapError> {
        let mut attempt: u32 = 0;
        loop {
            match self.fetcher.fetch_page(url).await {
                Ok(bytes) => return Ok(bytes),
                Err(SourceError::Fatal { message }) => {
                    warn!(%url, error = %message, "clob: fatal fetch error — aborting page");
                    return Err(BootstrapError::Clob {
                        message: format!("fetch {url}: {message}"),
                    });
                }
                Err(e) => {
                    if attempt >= CLOB_PAGE_MAX_RETRIES {
                        // Exhausted transient/rate-limited retries: a sustained upstream
                        // outage, not a permanent contract failure. The typed temporary
                        // error carries exit 75 so the loop supervisor retries the cycle
                        // instead of stopping (#534; the 2026-08-26 CLOB degradation
                        // killed the loop through the old `Clob` mapping here).
                        return Err(BootstrapError::TransientSource {
                            source_name: "polymarket-clob",
                            message: format!("fetch {url}: {e} (after {attempt} page retries)"),
                        });
                    }
                    let wait = match &e {
                        SourceError::RateLimited { retry_after_secs } => Duration::from_secs(
                            u64::from(*retry_after_secs).max(CLOB_RATE_LIMIT_MIN_WAIT_SECS),
                        ),
                        _ => clob_retry_backoff(attempt),
                    };
                    attempt += 1;
                    warn!(
                        %url,
                        error = %e,
                        attempt,
                        backoff = ?wait,
                        "clob: transient fetch error — retrying page"
                    );
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    /// Fetch one market by condition id through the same retry and pacing
    /// envelope as the closed-market pagination walk.
    pub(crate) async fn fetch_market(
        &self,
        condition_id: &str,
    ) -> Result<ClobMarket, BootstrapError> {
        let url = format!("{}/markets/{condition_id}", self.base_url);
        let bytes = self.fetch_page_with_retry(&url).await?;
        parse_clob_market(&bytes).map_err(|e| BootstrapError::Clob {
            message: format!("parse market {condition_id}: {e}"),
        })
    }

    /// Paginate `/markets?closed=true` and process every market's schedule,
    /// resolution, and token mapping into `cache`.
    ///
    /// A non-empty `source_cursor.clob_closed` resumes an interrupted walk. A
    /// missing or empty cursor starts at page 1, including after a successfully
    /// completed prior invocation. The cursor advances after each successful
    /// non-terminal page; a crash mid-page replays that page on restart,
    /// idempotent through the cache write contracts. Only the successful
    /// terminal branch writes the empty completion marker.
    ///
    /// Also maps every market's CLOB `tokens[]` into `token_conditions`
    /// (`token_id → condition_id`, with the positional `outcome_index`) so the
    /// trades / price-series join can resolve outcome → token (issue #429). The
    /// closed-markets sweep is the only full-universe token source — Gamma
    /// `events` covers a curated slice — so this maps the universe at zero extra
    /// network cost. See [`ClobReport`] for the order-divergence quarantine.
    ///
    /// Returns a [`ClobReport`]. Schedule/resolution/token counts reflect
    /// processed candidates, not changed rows; existing rows can no-op.
    pub async fn fetch_closed_markets(
        &self,
        cache: &mut WalletCache,
    ) -> Result<ClobReport, BootstrapError> {
        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let legacy_cursor = cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY);
        let legacy_resume = legacy_cursor
            .as_deref()
            .is_some_and(|value| !value.is_empty() && value != CLOB_END_CURSOR);
        let mut payout_walk = cache.clob_payout_walk_state_v2()?;
        if payout_walk.is_none() && !legacy_resume {
            payout_walk = Some(cache.begin_or_resume_clob_payout_walk_v2(fetched_at)?);
        }
        let mut cursor = payout_walk
            .as_ref()
            .map(|state| state.next_cursor.clone())
            .unwrap_or(legacy_cursor);
        let mut schedules = 0usize;
        let mut resolutions = 0usize;
        let mut tokens_mapped = 0usize;
        let mut order_mismatches = 0usize;
        let mut page_count = 0usize;
        let mut pending_winners = 0usize;
        let mut coverage_manifest = None;

        // A crash after the terminal page transaction but before installation
        // resumes by validating and installing the already-durable page chain;
        // it must not manufacture another network page after a terminal proof.
        if let Some(state) = payout_walk.as_ref()
            && state.next_page_ordinal > 0
            && is_clob_terminal_cursor(state.next_cursor.as_deref())
        {
            let manifest = complete_payout_walk(cache, state.generation, fetched_at)?;
            cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, "")?;
            return Ok(ClobReport {
                schedules,
                resolutions,
                tokens_mapped,
                order_mismatches,
                coverage_manifest: Some(manifest),
            });
        }

        info!(
            base_url = self.base_url.as_str(),
            resume_from = cursor.as_deref().unwrap_or("<start>"),
            payout_generation = payout_walk.as_ref().map(|state| state.generation),
            "clob: starting closed-market fetch"
        );

        loop {
            let url = build_page_url(&self.base_url, cursor.as_deref());
            let bytes = self.fetch_page_with_retry(&url).await?;

            let page: ClobMarketsPage =
                parse_clob_markets_page(&bytes).map_err(|e| BootstrapError::Clob {
                    message: format!("parse page: {e}"),
                })?;
            page_count += 1;
            let markets_in_page = page.data.len();
            let coverage_page = payout_walk
                .as_ref()
                .map(|state| {
                    ClobCoveragePage::from_response(
                        state.next_page_ordinal,
                        state.next_cursor.clone(),
                        &bytes,
                        &page,
                    )
                })
                .transpose()
                .map_err(|error| BootstrapError::Clob {
                    message: format!("build payout coverage page: {error}"),
                })?;
            let payout_evidence = coverage_page.as_ref().map(|_| {
                page.data
                    .iter()
                    .map(ClobMarket::resolution_evidence)
                    .collect::<Vec<_>>()
            });

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
                //
                // An identity-valid record with the `closed` field ABSENT warns
                // and suppresses only this resolution insertion (issue #523) —
                // the schedule insert above and the token mapping below proceed
                // unchanged, preserving coverage.
                if market.closed.is_none() {
                    warn!(
                        market_id = condition_id,
                        "clob: paginated record missing `closed` — resolution suppressed"
                    );
                }
                if market.closed == Some(true)
                    && let Some(resolved_at) = end_date_unix
                {
                    match analyze_winners(&market.tokens).verdict {
                        WinnerVerdict::Resolved(idx) => {
                            cache.insert_resolution_with_source(
                                &condition_id,
                                Some(idx),
                                resolved_at,
                                fetched_at,
                                "clob",
                            )?;
                            resolutions += 1;
                        }
                        WinnerVerdict::Voided => {
                            cache.insert_resolution_with_source(
                                &condition_id,
                                None,
                                resolved_at,
                                fetched_at,
                                "clob",
                            )?;
                            resolutions += 1;
                        }
                        // Winner flags not posted yet, or a contradictory payload:
                        // record NOTHING so the market stays missing and the next
                        // walk / audit pass retries it (issue #519 review — a
                        // delayed flag must never be frozen as a terminal void).
                        WinnerVerdict::Pending | WinnerVerdict::Invalid => pending_winners += 1,
                    }
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
                // stored row before the batch's conditional upsert runs.
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

            if let (Some(state), Some(coverage_page), Some(payout_evidence)) = (
                payout_walk.as_ref(),
                coverage_page.as_ref(),
                payout_evidence.as_deref(),
            ) {
                payout_walk = Some(cache.commit_clob_payout_page_v2(
                    state.generation,
                    coverage_page,
                    payout_evidence,
                    fetched_at,
                )?);
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
                    pending_winners,
                    "clob: reached end of pages"
                );
                if let Some(state) = payout_walk.as_ref() {
                    coverage_manifest =
                        Some(complete_payout_walk(cache, state.generation, fetched_at)?);
                }
                cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, "")?;
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
            coverage_manifest,
        })
    }
}

fn complete_payout_walk(
    cache: &mut WalletCache,
    generation: u64,
    completed_at_unix: i64,
) -> Result<ClobCoverageManifest, BootstrapError> {
    let pages = cache.clob_payout_coverage_pages_v2(generation)?;
    let manifest = ClobCoverageManifest::complete(generation, pages).map_err(|error| {
        BootstrapError::Clob {
            message: format!("complete payout coverage manifest: {error}"),
        }
    })?;
    cache.complete_clob_payout_walk_v2(&manifest, completed_at_unix)?;
    Ok(manifest)
}

/// Exponential page-retry backoff: `CLOB_PAGE_RETRY_BASE_MS · 2^attempt`, capped
/// at 30s. `attempt` is 0-based (first retry waits the base interval).
fn clob_retry_backoff(attempt: u32) -> Duration {
    let ms = CLOB_PAGE_RETRY_BASE_MS.saturating_mul(1u64 << attempt.min(10));
    Duration::from_millis(ms.min(30_000))
}

fn build_page_url(base_url: &str, cursor: Option<&str>) -> String {
    match cursor {
        Some(c) if !c.is_empty() && c != CLOB_END_CURSOR => {
            format!("{base_url}/markets?closed=true&limit={CLOB_PAGE_LIMIT}&next_cursor={c}")
        }
        _ => format!("{base_url}/markets?closed=true&limit={CLOB_PAGE_LIMIT}"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::{ClobToken, ClobTokenPrice};
    use std::collections::HashMap;

    fn tok(winner: Option<bool>) -> ClobToken {
        ClobToken {
            winner,
            token_id: None,
            outcome: None,
            price: ClobTokenPrice::Missing,
        }
    }

    #[test]
    fn classify_yes_picks_zero() {
        let tokens = vec![tok(Some(true)), tok(Some(false))];
        assert_eq!(analyze_winners(&tokens).verdict, WinnerVerdict::Resolved(0));
        assert!(analyze_winners(&tokens).has_explicit_winner);
    }

    #[test]
    fn classify_no_picks_one() {
        let tokens = vec![tok(Some(false)), tok(Some(true))];
        assert_eq!(analyze_winners(&tokens).verdict, WinnerVerdict::Resolved(1));
    }

    #[test]
    fn classify_all_explicit_false_is_voided() {
        let tokens = vec![tok(Some(false)), tok(Some(false))];
        let analysis = analyze_winners(&tokens);
        assert_eq!(analysis.verdict, WinnerVerdict::Voided);
        assert!(!analysis.has_explicit_winner);
    }

    #[test]
    fn classify_absent_flag_is_pending_not_voided() {
        // Issue #519 review: a closed market whose winner flags have not been
        // posted yet must classify Pending — recording it as Voided would freeze
        // a NULL row that INSERT-time idempotency could never upgrade.
        let tokens = vec![tok(None), tok(Some(false))];
        assert_eq!(analyze_winners(&tokens).verdict, WinnerVerdict::Pending);
        assert_eq!(analyze_winners(&[]).verdict, WinnerVerdict::Pending);
    }

    #[test]
    fn classify_sole_winner_with_absent_flag_is_pending_never_resolved() {
        // Issue #523: a partial vector must NOT resolve — a wrongly recorded
        // winner is permanent (the resolution upsert upgrades only NULL
        // winners), so `[true, absent]` stays Pending and retries.
        let tokens = vec![tok(Some(true)), tok(None)];
        let analysis = analyze_winners(&tokens);
        assert_eq!(analysis.verdict, WinnerVerdict::Pending);
        assert!(analysis.has_explicit_winner);
    }

    #[test]
    fn classify_two_winners_is_invalid_even_with_absent_flags() {
        // Issue #523: contradiction detection runs BEFORE absent-flag handling,
        // so `[true, true, absent]` cannot hide behind Pending.
        let tokens = vec![tok(Some(true)), tok(Some(true))];
        assert_eq!(analyze_winners(&tokens).verdict, WinnerVerdict::Invalid);
        let with_absent = vec![tok(Some(true)), tok(Some(true)), tok(None)];
        assert_eq!(
            analyze_winners(&with_absent).verdict,
            WinnerVerdict::Invalid
        );
    }

    #[test]
    fn classify_sole_winner_beyond_u16_range_is_invalid() {
        // Issue #523: the promised index-overflow branch, proved rather than
        // assumed — a sole explicit winner whose position exceeds `u16` blocks.
        let mut tokens: Vec<ClobToken> = (0..usize::from(u16::MAX) + 2)
            .map(|_| tok(Some(false)))
            .collect();
        tokens[usize::from(u16::MAX) + 1] = tok(Some(true));
        assert_eq!(analyze_winners(&tokens).verdict, WinnerVerdict::Invalid);
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
    async fn legacy_terminator_cursor_triggers_full_walk() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .set_source_cursor(CLOB_CLOSED_CURSOR_KEY, CLOB_END_CURSOR)
            .unwrap();

        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            br#"{"data":[],"next_cursor":"LTE="}"#.to_vec(),
        );
        let fetcher = pe_source_polymarket_public::FixtureFetcher::new(responses);
        let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

        let report = clob.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(report.schedules, 0);
        assert_eq!(report.resolutions, 0);
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_walk_restarts_from_page_one_on_next_invocation() {
        // Behavioral proof (issue #519 review): the second invocation must
        // actually REQUEST page 1 again — asserted by giving walk 2 a page-1
        // fixture whose market only exists there, then observing its row.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let mut first = HashMap::new();
        first.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            br#"{"data":[],"next_cursor":"LTE="}"#.to_vec(),
        );
        let walk1 = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(first),
        );
        walk1.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("")
        );

        let mut second = HashMap::new();
        second.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            br#"{"data":[{"condition_id":"0xsecondwalk","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"1","winner":true},{"token_id":"2","winner":false}]}],"next_cursor":"LTE="}"#.to_vec(),
        );
        let walk2 = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(second),
        );
        let report = walk2.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(
            report.resolutions, 1,
            "page 1 must be refetched, not skipped"
        );
        assert!(cache.resolution_record("0xsecondwalk").is_some());
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_winner_flags_are_not_recorded_then_upgrade_on_next_walk() {
        // Issue #519 review falsifier: a closed market whose winner flags are
        // absent records NOTHING; a later walk with explicit flags records the
        // real winner. A voided-then-flagged market upgrades its NULL row.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let mut first = HashMap::new();
        first.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            br#"{"data":[{"condition_id":"0xpending","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"1"},{"token_id":"2"}]},{"condition_id":"0xvoided","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"3","winner":false},{"token_id":"4","winner":false}]}],"next_cursor":"LTE="}"#.to_vec(),
        );
        let walk1 = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(first),
        );
        let report = walk1.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(report.resolutions, 1, "only the explicit void records");
        assert!(cache.resolution_record("0xpending").is_none());
        assert_eq!(cache.resolution_record("0xvoided").unwrap().0, None);

        let mut second = HashMap::new();
        second.insert(
            "https://clob.example/markets?closed=true&limit=1000".to_owned(),
            br#"{"data":[{"condition_id":"0xpending","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"1","winner":true},{"token_id":"2","winner":false}]},{"condition_id":"0xvoided","closed":true,"end_date_iso":"2024-11-04T00:00:00Z","tokens":[{"token_id":"3","winner":false},{"token_id":"4","winner":true}]}],"next_cursor":"LTE="}"#.to_vec(),
        );
        let walk2 = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(second),
        );
        walk2.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(
            cache.resolution_record("0xpending").unwrap().0,
            Some(0),
            "pending market records once flags post"
        );
        assert_eq!(
            cache.resolution_record("0xvoided").unwrap().0,
            Some(1),
            "a NULL (voided-looking) row upgrades when an explicit winner arrives"
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
