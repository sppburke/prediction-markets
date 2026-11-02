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
//! next; `buffer_unordered` does not apply. The walk requests the next page as
//! soon as the current one is parsed, so that request overlaps the current
//! page's database writes; pages are still written in order, and at most one
//! request is in flight. The legacy `source_cursor.clob_closed`
//! remains a sealed v1 resume cursor. Version-two payout evidence owns an
//! independent staging cursor and is installed only with a complete coverage
//! manifest from page one through a terminal response (#544).
//!
//! **Approximation:** `resolved_at_unix` is set to the parsed `end_date_iso`
//! because CLOB does not expose a block-timestamp resolution time. The retained
//! legacy `source='polygon'` rows keep their exact block timestamps (and win on
//! `INSERT OR IGNORE` ordering); only new markets carry the CLOB approximation.

use std::sync::Arc;
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
    /// Shared with the task that requests the next page while the walk writes
    /// the current one.
    fetcher: Arc<F>,
}

impl<F: PageFetcher + Send + Sync + 'static> ClobFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            base_url,
            fetcher: Arc::new(fetcher),
        }
    }

    #[cfg(test)]
    pub(crate) fn page_fetcher(&self) -> &F {
        &self.fetcher
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
    /// failures remain [`BootstrapError::Clob`] (permanent, exit 1). It takes the
    /// page fetcher rather than `self`, so the walk's next-page request can run
    /// on its own task.
    async fn fetch_page_with_retry(fetcher: &F, url: &str) -> Result<Vec<u8>, BootstrapError> {
        let mut attempt: u32 = 0;
        loop {
            match fetcher.fetch_page(url).await {
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
        let bytes = Self::fetch_page_with_retry(&self.fetcher, &url).await?;
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
        let v2_only = cache.schema_version()? == crate::cache::CACHE_SCHEMA_VERSION_V2;
        let legacy_cursor = if v2_only {
            None
        } else {
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY)
        };
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
            if !v2_only {
                cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, "")?;
            }
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

        // The next page's request, started while the current page is written.
        let mut prefetched: Option<PrefetchedPage> = None;
        loop {
            let bytes = match prefetched.take() {
                Some(request) => request.join().await?,
                None => {
                    let url = build_page_url(&self.base_url, cursor.as_deref());
                    Self::fetch_page_with_retry(&self.fetcher, &url).await?
                }
            };

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

            // Treat empty/missing/terminator as end-of-pages. Otherwise request
            // the next page now, so it arrives while this page is written.
            let next = page.next_cursor.clone().unwrap_or_default();
            let terminal = next.is_empty() || next == CLOB_END_CURSOR;
            if !terminal {
                let fetcher = Arc::clone(&self.fetcher);
                let url = build_page_url(&self.base_url, Some(next.as_str()));
                prefetched = Some(PrefetchedPage(tokio::spawn(async move {
                    Self::fetch_page_with_retry(&fetcher, &url).await
                })));
            }

            let written = pe_event_log::run_blocking(|| -> Result<(), BootstrapError> {
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
                    if !v2_only
                        && market.closed == Some(true)
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
                        let Some(token_id) = token.token_id.as_deref().filter(|t| !t.is_empty())
                        else {
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
                        if let Some((stored_cond, stored_idx)) =
                            cache.token_condition_outcome(token_id)
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
                if !terminal && !v2_only {
                    cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, &next)?;
                }
                Ok(())
            });
            if let Err(error) = written {
                // Writing the page or its legacy cursor failed: stop the next
                // page's request and discard its outcome before reporting it.
                if let Some(request) = prefetched.take() {
                    request.cancel().await;
                }
                return Err(error);
            }

            if terminal {
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
                if !v2_only {
                    cache.set_source_cursor(CLOB_CLOSED_CURSOR_KEY, "")?;
                }
                break;
            }
            cursor = Some(next);

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

/// The next page's request, running on its own task while the walk writes the
/// current page. It owns the task until joined or cancelled; dropping it
/// unjoined (the walk future itself was dropped) aborts the request.
struct PrefetchedPage(tokio::task::JoinHandle<Result<Vec<u8>, BootstrapError>>);

impl PrefetchedPage {
    /// The response, or the request's own error after its retries.
    async fn join(mut self) -> Result<Vec<u8>, BootstrapError> {
        (&mut self.0).await.map_err(|error| BootstrapError::Clob {
            message: format!("next-page request task failed: {error}"),
        })?
    }

    /// Stop the request and wait until it has stopped, discarding its outcome.
    async fn cancel(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for PrefetchedPage {
    fn drop(&mut self) {
        self.0.abort();
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

    #[tokio::test]
    async fn payout_walk_installs_group_ids_and_marks_group_coverage() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = crate::cache::WalletCache::open(&path).unwrap();
        let group = format!("0x{}", "ab".repeat(32));
        let initial_group = format!("0x{}", "cd".repeat(32));
        let mut responses = HashMap::new();
        responses.insert(build_page_url("https://clob.example", None), serde_json::to_vec(&serde_json::json!({
            "data": [
                {"condition_id": "0xgrouped", "closed": false, "neg_risk": true, "neg_risk_market_id": initial_group},
                {"condition_id": "0xordinary", "closed": false, "neg_risk": false, "neg_risk_market_id": ""},
                {"condition_id": "0xmissinggroup", "closed": false, "neg_risk": true}
            ], "next_cursor": "PAGE2"
        })).unwrap());
        responses.insert(build_page_url("https://clob.example", Some("PAGE2")), serde_json::to_vec(&serde_json::json!({
            "data": [{"condition_id": "0xgrouped", "closed": false, "neg_risk": true, "neg_risk_market_id": group}],
            "next_cursor": "LTE="
        })).unwrap());
        let fetcher = ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(responses),
        );
        let report = fetcher.fetch_closed_markets(&mut cache).await.unwrap();
        let manifest = report.coverage_manifest.unwrap();
        assert_eq!(manifest.counts.pages, 2);
        let conn = rusqlite::Connection::open(&path).unwrap();
        let (version, count): (Option<i64>, Option<i64>) = conn.query_row(
            "SELECT group_version, evidence_count FROM clob_payout_coverage_manifests_v2 WHERE generation = ?1",
            [i64::try_from(manifest.generation).unwrap()], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert_eq!(version, Some(1));
        assert_eq!(count, Some(3));
        for (market, expected, ordinal) in [
            ("0xgrouped", Some(group.clone()), 1),
            ("0xordinary", None, 0),
            ("0xmissinggroup", None, 0),
        ] {
            let (stored, page_ordinal): (Option<String>, i64) = conn.query_row(
                "SELECT neg_risk_market_id, page_ordinal FROM clob_payout_evidence_v2 WHERE market_id = ?1",
                [market], |row| Ok((row.get(0)?, row.get(1)?)),
            ).unwrap();
            assert_eq!(stored, expected);
            assert_eq!(page_ordinal, ordinal);
            let flag: Option<i64> = conn
                .query_row(
                    "SELECT neg_risk FROM clob_payout_evidence_v2 WHERE market_id = ?1",
                    [market],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(flag, Some(i64::from(market != "0xordinary")));
            assert!(cache.clob_payout_evidence_v2(market).unwrap().is_some());
        }
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

    const PREFETCH_BASE: &str = "https://clob.example";

    fn prefetch_url(cursor: Option<&str>) -> String {
        build_page_url(PREFETCH_BASE, cursor)
    }

    fn prefetch_page(markets: &[&str], next_cursor: &str) -> Vec<u8> {
        let data: Vec<_> = markets
            .iter()
            .map(|id| serde_json::json!({"condition_id": id, "closed": false, "neg_risk": false}))
            .collect();
        serde_json::to_vec(&serde_json::json!({"data": data, "next_cursor": next_cursor})).unwrap()
    }

    /// Pages A → B → C (terminal), two markets each.
    fn three_pages() -> HashMap<String, Vec<u8>> {
        HashMap::from([
            (
                prefetch_url(None),
                prefetch_page(&["0xa1", "0xa2"], "PAGEB"),
            ),
            (
                prefetch_url(Some("PAGEB")),
                prefetch_page(&["0xb1", "0xb2"], "PAGEC"),
            ),
            (
                prefetch_url(Some("PAGEC")),
                prefetch_page(&["0xc1", "0xc2"], CLOB_END_CURSOR),
            ),
        ])
    }

    /// What a scripted request does after `on_request` ran.
    enum Scripted {
        FailOnce,
        Transient,
        Fatal,
        /// Never answers; `Dropped` is marked once the request's future is dropped.
        Hang(Arc<Dropped>),
        /// The request task itself fails (a fetcher fault).
        Fault,
    }

    /// Marked (and notified) when a hanging request's future is dropped.
    #[derive(Default)]
    struct Dropped {
        flag: std::sync::atomic::AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl Dropped {
        fn get(&self) -> bool {
            self.flag.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Marks its `Dropped` when the future holding it is dropped.
    struct DropFlag(Arc<Dropped>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.flag.store(true, std::sync::atomic::Ordering::SeqCst);
            self.0.notify.notify_one();
        }
    }

    /// Serves fixed pages, records every request URL and runs `on_request`
    /// before answering as scripted for that URL.
    struct ScriptedFetcher {
        pages: HashMap<String, Vec<u8>>,
        script: std::sync::Mutex<HashMap<String, Scripted>>,
        requests: Arc<std::sync::Mutex<Vec<String>>>,
        on_request: Box<dyn Fn(&str) + Send + Sync>,
    }

    // `Scripted::Fault` panics on purpose: it is the request task's own failure.
    #[allow(clippy::panic)]
    impl PageFetcher for ScriptedFetcher {
        async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
            self.requests.lock().unwrap().push(url.to_owned());
            (self.on_request)(url);
            let hang = {
                let mut script = self.script.lock().unwrap();
                match script.get(url) {
                    None => None,
                    Some(Scripted::FailOnce) => {
                        script.remove(url);
                        return Err(SourceError::Transient {
                            message: format!("blip {url}"),
                        });
                    }
                    Some(Scripted::Transient) => {
                        return Err(SourceError::Transient {
                            message: format!("blip {url}"),
                        });
                    }
                    Some(Scripted::Fatal) => {
                        return Err(SourceError::Fatal {
                            message: format!("refused {url}"),
                        });
                    }
                    Some(Scripted::Hang(dropped)) => Some(DropFlag(Arc::clone(dropped))),
                    Some(Scripted::Fault) => panic!("scripted fetcher fault"),
                }
            };
            if let Some(flag) = hang {
                std::future::pending::<()>().await;
                drop(flag);
            }
            self.pages
                .get(url)
                .cloned()
                .ok_or_else(|| SourceError::Fatal {
                    message: format!("unexpected {url}"),
                })
        }
    }

    /// A walk over pages A → B → C with `script` and `on_request`, plus its
    /// request log.
    fn scripted(
        script: Vec<(String, Scripted)>,
        on_request: impl Fn(&str) + Send + Sync + 'static,
    ) -> (
        ClobFetcher<ScriptedFetcher>,
        Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let requests: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let fetcher = ScriptedFetcher {
            pages: three_pages(),
            script: std::sync::Mutex::new(script.into_iter().collect()),
            requests: Arc::clone(&requests),
            on_request: Box::new(on_request),
        };
        (
            ClobFetcher::new(PREFETCH_BASE.to_owned(), fetcher),
            requests,
        )
    }

    /// The stored walk position, read through another connection.
    fn walk_state(path: &std::path::Path) -> (Option<String>, i64) {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT next_cursor, next_page_ordinal FROM clob_payout_walk_state_v2 WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    /// A cache whose walk committed page A in an earlier invocation that then
    /// failed on page B; the next invocation resumes at page B.
    async fn cache_with_page_a(path: &std::path::Path) -> crate::cache::WalletCache {
        let mut cache = crate::cache::WalletCache::open(path).unwrap();
        let (first, _) = scripted(vec![(prefetch_url(Some("PAGEB")), Scripted::Fatal)], |_| {});
        first.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert_eq!(walk_state(path), (Some("PAGEB".to_owned()), 1));
        cache
    }

    /// Another connection holding the write lock until `release` runs.
    fn hold_write_lock(
        path: &std::path::Path,
    ) -> Arc<std::sync::Mutex<Option<rusqlite::Connection>>> {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        Arc::new(std::sync::Mutex::new(Some(conn)))
    }

    /// FAIL if page C is requested only after page B commits: only the page-C
    /// request releases the write lock, so page B's writes would wait out the
    /// 5 s busy timeout and the walk would fail.
    async fn assert_next_page_requested_while_writing(on_a_worker: bool) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let cache = cache_with_page_a(&path).await;
        let held = hold_write_lock(&path);
        let ordinal_at_c: Arc<std::sync::Mutex<Option<i64>>> = Arc::default();
        let on_request = {
            let (path, held, ordinal_at_c) =
                (path.clone(), Arc::clone(&held), Arc::clone(&ordinal_at_c));
            move |url: &str| {
                if url == prefetch_url(Some("PAGEC")) {
                    *ordinal_at_c.lock().unwrap() = Some(walk_state(&path).1);
                    if let Some(conn) = held.lock().unwrap().take() {
                        conn.execute_batch("ROLLBACK").unwrap();
                    }
                }
            }
        };
        let (clob, requests) = scripted(Vec::new(), on_request);
        let walk = async move {
            let mut cache = cache;
            clob.fetch_closed_markets(&mut cache).await
        };
        let report = if on_a_worker {
            tokio::spawn(walk).await.unwrap()
        } else {
            walk.await
        }
        .unwrap();
        assert_eq!(report.coverage_manifest.unwrap().counts.pages, 3);
        assert_eq!(
            *ordinal_at_c.lock().unwrap(),
            Some(1),
            "page B had not committed"
        );
        assert_eq!(
            *requests.lock().unwrap(),
            vec![prefetch_url(Some("PAGEB")), prefetch_url(Some("PAGEC"))]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn payout_walk_requests_the_next_page_while_writing_the_current_one() {
        assert_next_page_requested_while_writing(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn payout_walk_overlap_holds_when_the_walk_runs_on_a_worker() {
        assert_next_page_requested_while_writing(true).await;
    }

    /// Page B's payout transaction fails part-way (its second staged market is
    /// refused) while the page-C request is already running (it releases the
    /// lock, then hangs or fails): the walk returns page B's error after the
    /// request has stopped, the transaction rolled back, and both resume cursors
    /// still name page B.
    async fn assert_write_failure_wins(page_c: Scripted, hung: Option<Arc<Dropped>>) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = cache_with_page_a(&path).await;
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse_page_b_staging BEFORE INSERT ON clob_payout_evidence_staging_v2 \
                 WHEN NEW.market_id = '0xb2' BEGIN SELECT RAISE(ABORT, 'injected staging failure'); END;",
            )
            .unwrap();
        let held = hold_write_lock(&path);
        let on_request = {
            let held = Arc::clone(&held);
            move |url: &str| {
                if url == prefetch_url(Some("PAGEC"))
                    && let Some(conn) = held.lock().unwrap().take()
                {
                    conn.execute_batch("ROLLBACK").unwrap();
                }
            }
        };
        let (clob, requests) = scripted(vec![(prefetch_url(Some("PAGEC")), page_c)], on_request);
        let error = clob.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert!(
            error.to_string().contains("injected staging failure"),
            "{error}"
        );
        if let Some(dropped) = hung {
            assert!(dropped.get(), "page-C request still running");
        }
        assert_eq!(
            *requests.lock().unwrap(),
            vec![prefetch_url(Some("PAGEB")), prefetch_url(Some("PAGEC"))]
        );
        assert_eq!(walk_state(&path), (Some("PAGEB".to_owned()), 1));
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("PAGEB")
        );
        let (pages, staged): (i64, i64) = rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM clob_payout_walk_pages_v2 WHERE page_ordinal = 1), \
                        (SELECT COUNT(*) FROM clob_payout_evidence_staging_v2 \
                         WHERE market_id IN ('0xb1', '0xb2'))",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            (pages, staged),
            (0, 0),
            "page B's payout transaction rolled back"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn payout_walk_write_failure_stops_a_running_next_page_request() {
        let dropped = Arc::new(Dropped::default());
        assert_write_failure_wins(Scripted::Hang(Arc::clone(&dropped)), Some(dropped)).await;
    }

    /// The legacy cursor update failing after page B's payout commit wins over
    /// the running page-C request, which is stopped; the v2 checkpoint is at
    /// page C, the legacy cursor keeps page B, and a restart resumes at page C.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn payout_walk_legacy_cursor_failure_after_the_commit_keeps_the_v2_checkpoint() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = cache_with_page_a(&path).await;
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse_page_c_cursor BEFORE INSERT ON source_cursor \
                 WHEN NEW.value = 'PAGEC' BEGIN SELECT RAISE(ABORT, 'injected cursor failure'); END;",
            )
            .unwrap();
        let held = hold_write_lock(&path);
        let dropped = Arc::new(Dropped::default());
        let on_request = {
            let held = Arc::clone(&held);
            move |url: &str| {
                if url == prefetch_url(Some("PAGEC"))
                    && let Some(conn) = held.lock().unwrap().take()
                {
                    conn.execute_batch("ROLLBACK").unwrap();
                }
            }
        };
        let (clob, _) = scripted(
            vec![(
                prefetch_url(Some("PAGEC")),
                Scripted::Hang(Arc::clone(&dropped)),
            )],
            on_request,
        );
        let error = clob.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert!(
            error.to_string().contains("injected cursor failure"),
            "{error}"
        );
        assert!(dropped.get(), "page-C request still running");
        assert_eq!(walk_state(&path), (Some("PAGEC".to_owned()), 2));
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("PAGEB")
        );

        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("DROP TRIGGER refuse_page_c_cursor")
            .unwrap();
        let (restart, requests) = scripted(Vec::new(), |_| {});
        let report = restart.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(*requests.lock().unwrap(), vec![prefetch_url(Some("PAGEC"))]);
        assert_eq!(report.coverage_manifest.unwrap().counts.pages, 3);
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn payout_walk_write_failure_outranks_a_failed_next_page_request() {
        assert_write_failure_wins(Scripted::Fatal, None).await;
    }

    /// On a current-thread runtime the next page's request never starts before
    /// a failed write, so it is cancelled unsent.
    #[tokio::test]
    async fn payout_walk_write_failure_cancels_an_unsent_next_page_request() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = crate::cache::WalletCache::open(&path).unwrap();
        let on_request = {
            let path = path.clone();
            move |url: &str| {
                if url == prefetch_url(Some("PAGEB")) {
                    rusqlite::Connection::open(&path)
                        .unwrap()
                        .execute(
                            "UPDATE clob_payout_walk_state_v2 SET next_cursor = 'MOVED' WHERE singleton = 1",
                            [],
                        )
                        .unwrap();
                }
            }
        };
        let (clob, requests) = scripted(Vec::new(), on_request);
        let error = clob.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert!(
            error.to_string().contains("does not match active v2 walk"),
            "{error}"
        );
        assert_eq!(
            *requests.lock().unwrap(),
            vec![prefetch_url(None), prefetch_url(Some("PAGEB"))]
        );
        assert_eq!(walk_state(&path), (Some("MOVED".to_owned()), 1));
    }

    /// A failed next-page request is reported only after the current page
    /// committed, so the stored walk and the legacy cursor resume at it.
    #[tokio::test]
    async fn payout_walk_next_page_failure_surfaces_after_the_current_commit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = crate::cache::WalletCache::open(&path).unwrap();
        let (clob, requests) =
            scripted(vec![(prefetch_url(Some("PAGEC")), Scripted::Fatal)], |_| {});
        let error = clob.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert!(matches!(error, BootstrapError::Clob { .. }), "{error}");
        assert_eq!(requests.lock().unwrap().len(), 3);
        assert_eq!(walk_state(&path), (Some("PAGEC".to_owned()), 2));
        assert_eq!(
            cache.get_source_cursor(CLOB_CLOSED_CURSOR_KEY).as_deref(),
            Some("PAGEC")
        );
    }

    /// Exhausted retries of a next-page request keep the temporary-failure
    /// mapping (exit 75) and the committed position.
    #[tokio::test(start_paused = true)]
    async fn payout_walk_next_page_retry_exhaustion_is_temporary() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = crate::cache::WalletCache::open(&path).unwrap();
        let (clob, requests) = scripted(
            vec![(prefetch_url(Some("PAGEC")), Scripted::Transient)],
            |_| {},
        );
        let error = clob.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert!(
            matches!(error, BootstrapError::TransientSource { .. }),
            "{error}"
        );
        let page_c_requests = requests.lock().unwrap().len() - 2;
        assert_eq!(
            page_c_requests,
            usize::try_from(CLOB_PAGE_MAX_RETRIES).unwrap() + 1
        );
        assert_eq!(walk_state(&path), (Some("PAGEC".to_owned()), 2));
    }

    /// Walk-level retries apply to a next-page request, and nothing is
    /// requested after the terminal page.
    #[tokio::test(start_paused = true)]
    async fn payout_walk_retries_a_next_page_request() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let (clob, requests) = scripted(
            vec![(prefetch_url(Some("PAGEB")), Scripted::FailOnce)],
            |_| {},
        );
        let report = clob.fetch_closed_markets(&mut cache).await.unwrap();
        assert_eq!(report.coverage_manifest.unwrap().counts.pages, 3);
        assert_eq!(report.schedules, 6);
        assert_eq!(
            *requests.lock().unwrap(),
            vec![
                prefetch_url(None),
                prefetch_url(Some("PAGEB")),
                prefetch_url(Some("PAGEB")),
                prefetch_url(Some("PAGEC")),
            ]
        );
    }

    /// A request task that fails (a fetcher fault) is a permanent CLOB error.
    #[tokio::test]
    async fn payout_walk_maps_a_failed_request_task_to_a_permanent_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = crate::cache::WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let (clob, _) = scripted(vec![(prefetch_url(Some("PAGEB")), Scripted::Fault)], |_| {});
        let error = clob.fetch_closed_markets(&mut cache).await.unwrap_err();
        assert!(matches!(error, BootstrapError::Clob { .. }), "{error}");
        assert!(
            error.to_string().contains("next-page request task failed"),
            "{error}"
        );
    }

    /// Dropping the walk while it waits for the next page stops that request.
    #[tokio::test(start_paused = true)]
    async fn payout_walk_dropped_while_waiting_stops_the_next_page_request() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = crate::cache::WalletCache::open(&path).unwrap();
        let dropped = Arc::new(Dropped::default());
        let (clob, requests) = scripted(
            vec![(
                prefetch_url(Some("PAGEC")),
                Scripted::Hang(Arc::clone(&dropped)),
            )],
            |_| {},
        );
        let walk = clob.fetch_closed_markets(&mut cache);
        assert!(
            tokio::time::timeout(Duration::from_secs(60), walk)
                .await
                .is_err()
        );
        // Dropping the walk only schedules the request's cancellation.
        tokio::time::timeout(Duration::from_secs(60), dropped.notify.notified())
            .await
            .expect("page-C request still running");
        assert!(dropped.get());
        assert_eq!(requests.lock().unwrap().len(), 3);
        assert_eq!(walk_state(&path), (Some("PAGEC".to_owned()), 2));
    }
}
