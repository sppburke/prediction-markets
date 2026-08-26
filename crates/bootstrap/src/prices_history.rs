//! `pe-bootstrap prices-history` — the CLV/Gamma data backfill for the ranker bake-off (issue #421
//! PR4). A standalone, resumable, additive-only backfill; **not** a stage of the daily
//! `rank_and_push.sh` refresh (that pipeline is discover → backfill → events → resolutions → purge).
//!
//! Two passes over the resolved-market universe, both idempotent:
//!
//! 1. **Gamma `createdAt` → `market_schedules.start_date_unix`** (the `entry_timing_vs_creation`
//!    feature). UPDATE-only via [`GammaFetcher::backfill_start_dates`]; markets already populated or
//!    absent are skipped.
//! 2. **CLOB `/prices-history` → `market_price_history`** (the true-CLV series). For each resolved
//!    market's mapped CLOB tokens with no rows yet, fetch the hourly series over the pre-resolution
//!    window `[close_ref − window, close_ref]` and write it. Fetches run concurrently (to overlap
//!    network RTT under the dedicated ~100 req/s gate); writes are serial, batched transactions.
//!
//! The heavy second pass is bounded by `prices_history_token_limit` (0 = unbounded) and is resumable
//! — a re-run skips `(market, token)` pairs that already have rows.
//!
//! Every pass-2 row is stamped `source = 'clob'` (issue #429 PR3); the CLOB `/prices-history` series
//! is the only writer today (PR2's `clob_only` verdict dropped the planned trades pass). `source`
//! exists so a future trades-derived series can be distinguished and so the coverage ledger
//! (`price_series_coverage_report`) can attribute points.
//!
//! **Run before `purge`, never after.** `pe-bootstrap purge` hard-deletes `trades` by wallet. The
//! CLOB series is purge-independent (it never reads `trades`), so today the ordering is moot — but
//! the `INSERT OR IGNORE` write-once guarantee plus this ordering rule are what keep a captured
//! series stable if a trades-derived `source` is ever added (#429 PR3 step 6).

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use pe_source_polymarket_public::{ClobPricesHistoryClient, GAMMA_BROWSER_UA, ReqwestFetcher};

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::gamma::{self, GammaFetcher};

/// Outcome of [`run_prices_history`].
#[derive(Debug, Default, Clone, Copy)]
pub struct PricesHistoryReport {
    /// `market_schedules.start_date_unix` rows populated from Gamma `createdAt` (pass 1).
    pub start_dates_updated: usize,
    /// `(market, token)` series fetched OK in pass 2 (includes those that returned 0 points).
    pub tokens_fetched: usize,
    /// `(market, token)` series whose fetch hit a non-fatal error and were skipped (retry next run).
    pub tokens_failed: usize,
    /// Price points inserted into `market_price_history` (pre-`INSERT OR IGNORE` count).
    pub points_written: usize,
}

/// Chunk size for the Gamma createdAt pass — bounds the in-memory result map per `fetch_markets`
/// call on the ~1.4M-market universe.
const START_DATE_CHUNK: usize = 20_000;

/// Flush threshold for the price-history write batch — one transaction per this many points.
const PRICE_FLUSH_BATCH: usize = 10_000;

/// Run both backfill passes. See the module docs for semantics.
pub async fn run_prices_history(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<PricesHistoryReport, BootstrapError> {
    let mut report = PricesHistoryReport::default();

    // ── Pass 1: Gamma createdAt → start_date_unix ────────────────────────────────
    // Scope to decided-outcome markets (same filter as pass 2's price-series targets): a voided
    // market yields no qualifying first-buy positions, so fetching its createdAt would be wasted.
    let missing_start = cache.market_ids_missing_start_date();
    let resolved = cache.resolved_market_ids_with_winner();
    let start_targets: Vec<String> = missing_start.intersection(&resolved).cloned().collect();
    if start_targets.is_empty() {
        tracing::info!("prices-history: no markets missing start_date — skipping createdAt pass");
    } else {
        let gamma_client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(15))
            .user_agent(GAMMA_BROWSER_UA)
            .build()
            .map_err(|_| BootstrapError::Internal)?;
        let gamma_fetcher = GammaFetcher::new(
            config.gamma_base_url.clone(),
            ReqwestFetcher::new(gamma_client).with_min_interval_ms(gamma::GAMMA_MIN_INTERVAL_MS),
        );
        tracing::info!(
            candidates = start_targets.len(),
            "prices-history: createdAt → start_date backfill starting"
        );
        for chunk in start_targets.chunks(START_DATE_CHUNK) {
            report.start_dates_updated += gamma_fetcher.backfill_start_dates(chunk, cache).await?;
        }
        tracing::info!(
            updated = report.start_dates_updated,
            "prices-history: createdAt → start_date backfill complete"
        );
    }

    // ── Pass 2: CLOB /prices-history → market_price_history ───────────────────────
    let targets = cache.price_history_backfill_targets(config.prices_history_token_limit)?;
    if targets.is_empty() {
        tracing::info!("prices-history: no (market, token) targets need a price series");
        return Ok(report);
    }

    let clob_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let client = Arc::new(
        ClobPricesHistoryClient::new(
            config.clob_base_url.clone(),
            ReqwestFetcher::new(clob_client)
                .with_min_interval_ms(config.prices_history_min_interval_ms),
        )
        .with_fidelity_minutes(config.prices_history_fidelity_minutes),
    );
    let window = config.prices_history_window_secs;
    let concurrency = config.clob_concurrency.max(1);

    tracing::info!(
        targets = targets.len(),
        concurrency,
        window_secs = window,
        fidelity_minutes = config.prices_history_fidelity_minutes,
        "prices-history: CLOB price-series backfill starting"
    );

    // Concurrent fetch (overlaps RTT under the shared gate), serial batched writes.
    let mut stream = stream::iter(targets)
        .map(|target| {
            let client = Arc::clone(&client);
            async move {
                let start_ts = target.close_ref_unix.saturating_sub(window);
                let res = client
                    .fetch_prices_history(&target.token_id, start_ts, target.close_ref_unix)
                    .await;
                (target, res)
            }
        })
        .buffer_unordered(concurrency);

    let mut batch: Vec<(String, String, i64, String)> = Vec::with_capacity(PRICE_FLUSH_BATCH);
    while let Some((target, res)) = stream.next().await {
        match res {
            Ok(points) => {
                for p in points {
                    batch.push((
                        target.market_id.clone(),
                        target.token_id.clone(),
                        p.t,
                        p.price.to_string(),
                    ));
                }
                report.tokens_fetched += 1;
            }
            Err(e) => {
                tracing::warn!(
                    token_id = %target.token_id,
                    market_id = %target.market_id,
                    error = %e,
                    "prices-history: token fetch failed — skipping (retry next run)"
                );
                report.tokens_failed += 1;
            }
        }
        if batch.len() >= PRICE_FLUSH_BATCH {
            report.points_written += batch.len();
            cache.insert_price_history_batch(&batch, "clob")?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        report.points_written += batch.len();
        cache.insert_price_history_batch(&batch, "clob")?;
    }

    tracing::info!(
        tokens_fetched = report.tokens_fetched,
        tokens_failed = report.tokens_failed,
        points_written = report.points_written,
        "prices-history: CLOB price-series backfill complete"
    );
    Ok(report)
}

// ─── Targeted ranker-oracle fetch (#536) ─────────────────────────────────────────────────

/// Fidelity for the targeted ranker-oracle fetch: 1 minute (the endpoint default and the
/// granularity the pass-2 staleness bound reasons in). Part of the coverage identity.
pub const RANKER_PRICE_FIDELITY_MINUTES: u32 = 1;

/// Maximum requested span per page, seconds: safely under the measured ~1,437-point
/// (~24 h at minute fidelity) END-anchored response cap, so silent truncation cannot
/// occur (80,000 s → ≤ 1,334 points). Canonical `ranker_price_page_max_span_secs` in
/// `docs/_GLOSSARY.md`.
pub const RANKER_PAGE_MAX_SPAN_SECS: i64 = 80_000;

/// Page-ledger provenance identity for this source and code path.
pub const RANKER_PRICE_SOURCE_ID: &str = "polymarket-clob-prices-history";
pub const RANKER_PRICE_SCHEMA_VERSION: u32 = 1;
pub const RANKER_PRICE_PARSER_VERSION: u32 = 1;

/// Outcome of [`run_targeted_prices_history`].
#[derive(Debug, Default, Clone, Copy)]
pub struct TargetedPricesReport {
    /// Distinct tokens in the targets file.
    pub tokens: usize,
    /// Uncovered ranges remaining after subtracting validated coverage.
    pub needed_ranges: usize,
    /// Pages committed with points.
    pub pages_complete: usize,
    /// Pages committed as valid-empty (durable no-series truth).
    pub pages_empty: usize,
    /// Points written (pre-duplicate-skip count of rows offered to the store).
    pub points_written: usize,
    /// Pages that failed transiently (no ledger row; re-requested on the next run).
    pub transient_failures: usize,
}

/// Merge unsorted, possibly-overlapping inclusive ranges into a sorted disjoint union.
/// Adjacent ranges (`end + 1 == next start`) merge: integer seconds are a discrete domain.
fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
    for (lo, hi) in ranges {
        match merged.last_mut() {
            Some((_, last_hi)) if lo <= last_hi.saturating_add(1) => {
                *last_hi = (*last_hi).max(hi);
            }
            _ => merged.push((lo, hi)),
        }
    }
    merged
}

/// `needed` minus the union of `covered`, all bounds inclusive. Both inputs may be
/// unsorted and overlapping. The result is the exact uncovered remainder — independent
/// of how any cycle's targets happened to merge (#536: coverage identity must never
/// depend on merge boundaries).
pub(crate) fn subtract_covered(needed: &[(i64, i64)], covered: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let needed = merge_ranges(needed.to_vec());
    let covered = merge_ranges(covered.to_vec());
    let mut out = Vec::new();
    for (lo, hi) in needed {
        let mut cursor = lo;
        for &(c_lo, c_hi) in &covered {
            if c_hi < cursor {
                continue;
            }
            if c_lo > hi {
                break;
            }
            if c_lo > cursor {
                out.push((cursor, c_lo - 1));
            }
            cursor = cursor.max(c_hi.saturating_add(1));
            if cursor > hi {
                break;
            }
        }
        if cursor <= hi {
            out.push((cursor, hi));
        }
    }
    out
}

/// Split one inclusive range into CONTIGUOUS logical pages sharing their boundary
/// seconds (`…(a,b),(b,c)…`), each spanning at most `max_span` seconds. The venue
/// documents `startTs` as "after" and `endTs` as "before" (exclusive bounds), so the
/// fetch pads every REQUEST one second beyond the logical page on both sides — the
/// ledger records only the logical page, and shared boundaries mean no second falls
/// between pages (#536 review: disjoint pages left a two-second seam hole that the
/// ledger nevertheless claimed as covered).
fn paginate(lo: i64, hi: i64, max_span: i64) -> Vec<(i64, i64)> {
    let mut pages = Vec::new();
    let mut cursor = lo;
    while cursor <= hi {
        let end = (cursor.saturating_add(max_span)).min(hi);
        pages.push((cursor, end));
        if end == hi {
            break;
        }
        cursor = end; // shared boundary: the next logical page starts where this one ends
    }
    pages
}

/// Hex sha256 of one raw response body.
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Parse the targets file: a `token_id,start_ts,end_ts` header line then one row per
/// merged backward window. The emitter is our own pass-2, so a malformed row is a bug —
/// fatal, never skipped.
fn parse_targets_csv(
    content: &str,
) -> Result<std::collections::BTreeMap<String, Vec<(i64, i64)>>, BootstrapError> {
    let mut per_token: std::collections::BTreeMap<String, Vec<(i64, i64)>> =
        std::collections::BTreeMap::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || (idx == 0 && line.starts_with("token_id")) {
            continue;
        }
        let mut parts = line.split(',');
        let (token, start, end) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(t), Some(s), Some(e), None) if !t.is_empty() => (t, s, e),
            _ => {
                return Err(BootstrapError::Invalid {
                    message: format!("targets line {} malformed: {line:?}", idx + 1),
                });
            }
        };
        let (start, end) = match (start.parse::<i64>(), end.parse::<i64>()) {
            (Ok(s), Ok(e)) if s <= e => (s, e),
            _ => {
                return Err(BootstrapError::Invalid {
                    message: format!("targets line {} has invalid bounds: {line:?}", idx + 1),
                });
            }
        };
        per_token
            .entry(token.to_owned())
            .or_default()
            .push((start, end));
    }
    Ok(per_token)
}

/// Targeted minute-fidelity fetch into the isolated ranker price store (#536).
///
/// Reads the pass-2-emitted targets file, subtracts already-validated coverage per token
/// (range algebra over `ranker_price_pages`), fetches only the uncovered remainder in
/// bounded pages (concurrent fetch under the dedicated gate, serial atomic page commits),
/// and classifies every response exhaustively. A 4xx rejection or a conflicting duplicate
/// point is fatal; transient failures leave no ledger row and are re-requested next run
/// (partial, exit 2 at the command boundary). Never touches `market_price_history` and
/// never runs the Gamma start-date pass.
pub async fn run_targeted_prices_history(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    targets_csv: &std::path::Path,
) -> Result<TargetedPricesReport, BootstrapError> {
    let content = std::fs::read_to_string(targets_csv)?;
    let per_token = parse_targets_csv(&content)?;
    let mut report = TargetedPricesReport {
        tokens: per_token.len(),
        ..Default::default()
    };

    // Build the uncovered page work list token by token.
    let mut pages: Vec<(String, i64, i64)> = Vec::new();
    for (token, ranges) in &per_token {
        let covered = cache.ranker_price_covered_ranges(token, RANKER_PRICE_FIDELITY_MINUTES)?;
        let missing = subtract_covered(ranges, &covered);
        report.needed_ranges += missing.len();
        for (lo, hi) in missing {
            for page in paginate(lo, hi, RANKER_PAGE_MAX_SPAN_SECS) {
                pages.push((token.clone(), page.0, page.1));
            }
        }
    }
    if pages.is_empty() {
        tracing::info!(
            tokens = report.tokens,
            "prices-history targeted: coverage already complete — nothing to fetch"
        );
        return Ok(report);
    }
    tracing::info!(
        tokens = report.tokens,
        pages = pages.len(),
        "prices-history targeted: fetch starting"
    );

    let clob_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let client = Arc::new(
        ClobPricesHistoryClient::new(
            config.clob_base_url.clone(),
            ReqwestFetcher::new(clob_client)
                .with_min_interval_ms(config.prices_history_min_interval_ms),
        )
        .with_fidelity_minutes(RANKER_PRICE_FIDELITY_MINUTES),
    );

    // Concurrent fetch (overlaps RTT under the shared gate), serial atomic page commits —
    // the run_prices_history pattern.
    let concurrency = config.clob_concurrency.max(1);
    let mut stream = stream::iter(pages)
        .map(|(token, lo, hi)| {
            let client = Arc::clone(&client);
            async move {
                // Padded request envelope: the venue's after/before bounds are exclusive,
                // so fetch [lo-1, hi+1] to guarantee the logical page's edge seconds.
                let res = client
                    .fetch_prices_history_classified(&token, lo - 1, hi + 1)
                    .await;
                (token, lo, hi, res)
            }
        })
        .buffer_unordered(concurrency);

    use pe_source_polymarket_public::{ClassifiedPricesHistory, ClobPricesHistoryError};

    use crate::cache::{RankerPageStatus, RankerPricePage};
    while let Some((token, lo, hi, res)) = stream.next().await {
        let page = match res {
            Ok(p) => p,
            Err(ClobPricesHistoryError::Parse(message)) => {
                // A malformed body is a contract break, never a blip: retrying forever
                // under the transient lane would loop the supervisor (#536 review).
                return Err(BootstrapError::Invalid {
                    message: format!(
                        "prices-history targeted: malformed response for {token} \
                         [{lo},{hi}]: {message}"
                    ),
                });
            }
            Err(e) => {
                report.transient_failures += 1;
                tracing::warn!(token, lo, hi, error = %e,
                    "prices-history targeted: transient page failure — re-requested next run");
                continue;
            }
        };

        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        let (status, rows) = match page.outcome {
            ClassifiedPricesHistory::Rejected { message } => {
                return Err(BootstrapError::Invalid {
                    message: format!(
                        "prices-history targeted: request rejected (invalid request is our \
                         bug, never no-history): {message}"
                    ),
                });
            }
            ClassifiedPricesHistory::Points(points) => {
                // Envelope guard: samples must lie inside the PADDED request
                // [lo-1, hi+1]; anything further out is an anomaly — no ledger row,
                // retry later. Padding samples at exactly lo-1/hi+1 belong to the
                // neighbouring logical page and are clamped out of this one (the
                // shared-boundary seconds lo/hi themselves are kept).
                if points.iter().any(|p| p.t < lo - 1 || p.t > hi + 1) {
                    report.transient_failures += 1;
                    tracing::warn!(
                        token,
                        lo,
                        hi,
                        "prices-history targeted: out-of-envelope sample — page not recorded"
                    );
                    continue;
                }
                let rows: Vec<(i64, String)> = points
                    .iter()
                    .filter(|p| p.t >= lo && p.t <= hi)
                    .map(|p| (p.t, p.price.to_string()))
                    .collect();
                let status = if rows.is_empty() {
                    RankerPageStatus::Empty
                } else {
                    RankerPageStatus::Complete
                };
                (status, rows)
            }
            ClassifiedPricesHistory::Empty => (RankerPageStatus::Empty, Vec::new()),
        };
        let record = RankerPricePage {
            token_id: token,
            start_ts: lo,
            end_ts: hi,
            fidelity_minutes: RANKER_PRICE_FIDELITY_MINUTES,
            status,
            point_count: rows.len(),
            raw_sha256: sha256_hex(&page.body),
            source_id: RANKER_PRICE_SOURCE_ID.to_owned(),
            schema_version: RANKER_PRICE_SCHEMA_VERSION,
            parser_version: RANKER_PRICE_PARSER_VERSION,
            observed_at_unix: now_unix,
            fetched_at_unix: now_unix,
            request_envelope: page.url,
        };
        // A conflicting duplicate propagates fatal: upstream serving two values for one
        // sample is data corruption, never silently resolved.
        cache.commit_ranker_price_page(&record, &rows)?;
        match status {
            RankerPageStatus::Complete => {
                report.pages_complete += 1;
                report.points_written += rows.len();
            }
            RankerPageStatus::Empty => report.pages_empty += 1,
        }
    }

    tracing::info!(
        tokens = report.tokens,
        needed_ranges = report.needed_ranges,
        pages_complete = report.pages_complete,
        pages_empty = report.pages_empty,
        points_written = report.points_written,
        transient_failures = report.transient_failures,
        "prices-history targeted: complete"
    );
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod targeted_tests {
    use super::*;

    #[test]
    fn subtract_covered_is_merge_shape_independent() {
        // The same needed span expressed as one range or two overlapping ranges yields the
        // identical remainder against the same coverage.
        let covered = vec![(100, 200), (350, 400)];
        let one = subtract_covered(&[(50, 500)], &covered);
        let split = subtract_covered(&[(50, 320), (300, 500)], &covered);
        assert_eq!(one, vec![(50, 99), (201, 349), (401, 500)]);
        assert_eq!(one, split);
        // Fully covered → empty; disjoint coverage → untouched.
        assert!(subtract_covered(&[(120, 180)], &covered).is_empty());
        assert_eq!(subtract_covered(&[(600, 700)], &covered), vec![(600, 700)]);
        // Inclusive boundaries: coverage ending at 200 leaves 201 uncovered.
        assert_eq!(subtract_covered(&[(200, 202)], &covered), vec![(201, 202)]);
    }

    #[test]
    fn paginate_shares_boundaries_and_bounds_every_page() {
        let pages = paginate(0, 200_000, RANKER_PAGE_MAX_SPAN_SECS);
        assert_eq!(
            pages,
            vec![(0, 80_000), (80_000, 160_000), (160_000, 200_000)]
        );
        // Contiguous shared boundaries: no second can fall between logical pages even
        // under the venue's exclusive request bounds (#536 review seam-hole fix).
        for w in pages.windows(2) {
            assert_eq!(w[0].1, w[1].0);
        }
        assert!(
            pages
                .iter()
                .all(|(lo, hi)| hi - lo <= RANKER_PAGE_MAX_SPAN_SECS)
        );
        assert_eq!(paginate(5, 5, RANKER_PAGE_MAX_SPAN_SECS), vec![(5, 5)]);
    }

    #[test]
    fn targets_csv_parses_and_rejects_malformed() {
        let parsed =
            parse_targets_csv("token_id,start_ts,end_ts\nA,10,20\nA,30,40\nB,5,5\n").unwrap();
        assert_eq!(parsed["A"], vec![(10, 20), (30, 40)]);
        assert_eq!(parsed["B"], vec![(5, 5)]);
        assert!(
            parse_targets_csv("A,20,10\n").is_err(),
            "inverted bounds are a bug"
        );
        assert!(parse_targets_csv("A,x,10\n").is_err());
        assert!(parse_targets_csv("A,1\n").is_err());
    }

    #[test]
    fn sha256_hex_is_stable() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
