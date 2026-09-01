//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline (invoked as `pe-bootstrap all` or no-arg):
//! 1. `migrate::auto_migrate_legacy` — one-shot SQLite consolidation.
//! 2. `winner_discovery::run_winner_discovery` — discover wallets via the
//!    Polymarket leaderboard (all categories) + datadash.
//! 3. `fetch::run_fetch` — fetch Polymarket trade history per wallet.
//! 4. `watchlist_phase::run_watchlist` — reconstruct ledgers, filter, write watchlist.json.
//! 5. `fetch_resolutions_and_schedules` — fetch market resolution data (opt-in).
//!
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.

pub mod backfill;
pub mod cache;
pub mod chain;
pub mod clob;
pub mod config;
pub mod coverage;
pub mod datadash_discovery;
pub mod error;
pub mod events;
pub mod fetch;
pub mod filter;
pub mod gamma;
pub mod infra_probe;
pub mod leaderboard_discovery;
pub mod lock;
pub mod migrate;
pub mod pile;
pub mod polymarket;
pub mod prices_history;
pub mod purge;
pub mod reclamation_evidence;
pub mod wallet_discovery;
pub mod wallet_set;
pub mod watchlist_phase;
pub mod winner_discovery;

pub use config::BootstrapConfig;
// Re-exports for callers that previously imported these from the crate root.
pub use watchlist_phase::build_seed_watchlist;

use std::collections::HashSet;
use std::time::Duration;

use cache::WalletCache;
use error::BootstrapError;
use pe_source_polymarket_public::PageFetcher;
use time::OffsetDateTime;

const RESOLUTION_AUDIT_REPAIR_LIMIT: usize = 5_000;
const RESOLUTION_AUDIT_END_BUFFER_SECS: i64 = 3_600;

/// Outcome of [`fetch_resolutions_and_schedules`] (issue #201).
///
/// The optional Gamma stages (schedules/liquidity, null-rewrite,
/// schedule-backfill) soft-fail independently: a failure logs a warning and
/// records the stage name here rather than aborting the pipeline. The primary
/// CLOB stage hard-fails (propagates `Err`) — see [`fetch_resolutions_and_schedules`].
/// A non-empty `stages_failed` ⇒ the caller should treat the run as partial
/// (exit 2), matching the backfill convention.
#[derive(Debug, Default, Clone)]
pub struct ResolutionsReport {
    /// Names of optional stages that soft-failed this run (e.g. `"clob"`).
    pub stages_failed: Vec<&'static str>,
    /// CLOB markets quarantined this run because their `tokens[]` order diverged
    /// from the stored authoritative Gamma `clob_token_ids` order (issue #429).
    /// Non-zero is a data-integrity anomaly the caller surfaces as partial (exit
    /// 2); the quarantined markets' token rows were skipped, never mispriced.
    pub clob_order_mismatches: usize,
}

impl ResolutionsReport {
    /// True when the run needs operator attention: an optional stage soft-failed,
    /// or a CLOB token-order divergence quarantined a market (issue #429). Callers
    /// surface this as an exit-2 partial.
    pub fn has_failures(&self) -> bool {
        !self.stages_failed.is_empty() || self.clob_order_mismatches > 0
    }
}

/// Multi-source resolution + schedule pipeline (issue #149, extracted in #166).
///
/// CLOB is the **primary, sole** market-resolution source (issue #369): the
/// on-chain Polygon RPC scan was removed. Existing `source='polygon'` rows are
/// retained (`INSERT OR IGNORE` never overwrites them), so CLOB is authoritative
/// for markets polygon never resolved and for all future markets —
/// "primary-for-new", not a rewrite of history.
///
/// - CLOB closed-market pagination → `source='clob'` (`end_date_iso` approx).
///   **Primary; hard-fails** so a CLOB outage aborts the run rather than silently
///   producing a resolutions pass missing the gold source.
/// - Gamma schedules → `source='gamma'` (open markets only).
/// - Gamma liquidity → only Gamma exposes liquidity (open markets).
/// - Gamma null-schedule rewrite pass (issue #137 Sub-PR 2).
/// - Gamma schedule backfill for resolved-but-unscheduled markets (issue #137
///   durable follow-up): inserts a `market_schedules` row for resolved markets that
///   never had their schedule fetched while open.
///
/// `open_ids` is computed AFTER CLOB so Gamma only fetches truly-still-open
/// markets. `market_ids` scopes the run — pass `cache.all_market_ids()` for the
/// full pipeline or just the wallets-newly-fetched market set for targeted backfill.
///
/// `config.rebuild_resolutions = true` first deletes the CLOB cursor row, then
/// drops every imprecise-source row (`'gamma'`, `'clob'`) so the CLOB stage
/// re-populates from page 1. Retained `source='polygon'` rows are NOT deleted —
/// they keep their exact block-timestamp `resolved_at_unix`. With the on-chain
/// scan gone there is no precise re-derivation, so a rebuild re-fetches `'clob'`
/// rows from CLOB only. Idempotent — safe on every run.
///
/// The final stage audits every traded, scheduled market whose end is more than
/// one hour past, repairing missing terminal rows through the per-market CLOB
/// endpoint and classifying every other missing market by venue truth (issue
/// #523). Only OUR-side inability to record available venue truth — fetch
/// failures, identity mismatches, contradictory payloads, unrecordable terminal
/// states, or a clipped population — returns
/// [`BootstrapError::ResolutionAuditIncomplete`] (exit 75). Venue-side
/// incompleteness (open lagged/extended/inactive markets, closed markets whose
/// winner flags are not posted yet) is counted, writes nothing, and is retried
/// automatically next cycle because audit membership derives from the absence
/// of a resolution row.
pub async fn fetch_resolutions_and_schedules(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<ResolutionsReport, BootstrapError> {
    if config.rebuild_resolutions {
        cache.delete_source_cursor(clob::CLOB_CLOSED_CURSOR_KEY)?;
        cache.reset_clob_payout_walk_v2()?;
        let deleted = cache.delete_resolutions_by_sources(&["gamma", "clob"])?;
        tracing::info!(
            deleted,
            "bootstrap: rebuild_resolutions=1 — deleted imprecise-source rows so precision stages repopulate"
        );
    }

    let mut stages_failed: Vec<&'static str> = Vec::new();
    let clob_fetcher = build_clob_fetcher(config)?;

    // CLOB closed-market pagination — primary resolution source; hard-fail
    // (propagate). A CLOB outage aborts the run rather than silently producing a
    // resolutions pass missing the gold source (issue #369).
    let clob_report = run_clob_closed_markets(&clob_fetcher, cache).await?;

    // The Gamma stages are fallback/auxiliary sources (issue #201): a failure in
    // one must NOT abort the others. Each is soft-failed — logged and recorded in
    // the report — and the pipeline continues. A non-empty `stages_failed` makes
    // the caller exit 2 (partial).
    if let Err(e) = run_gamma_schedules_liquidity(config, cache, market_ids).await {
        tracing::warn!(error = %e, stage = "gamma", "resolutions: stage soft-failed, continuing");
        stages_failed.push("gamma");
    }
    if let Err(e) = run_gamma_null_rewrite(config, cache, market_ids).await {
        tracing::warn!(error = %e, stage = "gamma_null_rewrite", "resolutions: stage soft-failed, continuing");
        stages_failed.push("gamma_null_rewrite");
    }
    if let Err(e) = run_schedule_backfill(config, cache, market_ids).await {
        tracing::warn!(error = %e, stage = "schedule_backfill", "resolutions: stage soft-failed, continuing");
        stages_failed.push("schedule_backfill");
    }

    run_resolution_audit(
        &clob_fetcher,
        cache,
        OffsetDateTime::now_utc().unix_timestamp(),
        RESOLUTION_AUDIT_REPAIR_LIMIT,
    )
    .await?;

    Ok(ResolutionsReport {
        stages_failed,
        clob_order_mismatches: clob_report.order_mismatches,
    })
}

/// CLOB closed-market pagination → `source='clob'` (`end_date_iso` approx).
/// Primary, sole market-resolution source (issue #369). Also maps the
/// full-universe token→condition map with positional `outcome_index` (issue #429).
fn build_clob_fetcher(
    config: &BootstrapConfig,
) -> Result<clob::ClobFetcher<pe_source_polymarket_public::ReqwestFetcher>, BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;
    let clob_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    Ok(clob::ClobFetcher::new(
        config.clob_base_url.clone(),
        ReqwestFetcher::new(clob_client).with_min_interval_ms(200),
    ))
}

async fn run_clob_closed_markets<F: PageFetcher + Send + Sync>(
    clob_fetcher: &clob::ClobFetcher<F>,
    cache: &mut WalletCache,
) -> Result<clob::ClobReport, BootstrapError> {
    let report = clob_fetcher.fetch_closed_markets(cache).await?;
    if report.order_mismatches > 0 {
        // Quarantined markets' token rows were skipped (never mispriced); surface
        // loudly so a real CLOB-order ≠ Gamma-order divergence is investigated
        // before trusting true_clv coverage (issue #429).
        tracing::error!(
            order_mismatches = report.order_mismatches,
            "bootstrap: CLOB token-order divergence vs Gamma map — markets quarantined (token rows skipped)"
        );
    }
    tracing::info!(
        clob_schedules = report.schedules,
        clob_resolutions = report.resolutions,
        clob_tokens_mapped = report.tokens_mapped,
        clob_order_mismatches = report.order_mismatches,
        "bootstrap: clob closed markets fetched"
    );
    Ok(report)
}

/// Per-market outcome of one audit pass (issue #523). Every attempted id maps
/// to exactly one disposition through one total match over the parsed response
/// state, so the fail-closed overflow is structural: any combination not
/// explicitly enumerated lands in `Blocked`. Non-blocking dispositions write
/// nothing — the market stays in the missing set and is retried next cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditDisposition {
    /// Venue closed with a complete winning vector — terminal row written.
    Repaired,
    /// Venue closed with a complete all-false vector — terminal NULL row written.
    Voided,
    /// Open (`active=true`), past its venue end date — ordinary venue lag.
    Lagged,
    /// Open (`active=true`), venue end date in the future — our stored schedule
    /// end is stale (write-once); not actually due. The schedule row is
    /// deliberately untouched (look-ahead contract, `docs/26`).
    Extended,
    /// Open, `active=false` — delisted/inactive venue inventory.
    Inactive,
    /// Open, but `active` absent or the venue end date unparseable — no terminal
    /// truth exists to record, so missing diagnostic metadata must not block.
    OpenUnknown,
    /// Closed, winner flags absent/incomplete — venue has not exposed a usable
    /// winner yet; never write a premature NULL (issue #519 review).
    Pending,
    /// Our-side inability to record available venue truth — fails the audit.
    Blocked,
}

/// Aggregate audit counters, returned for the caller's log line and for the
/// test-only accounting identity (counts sum to the attempted population).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct AuditCounts {
    repaired: usize,
    voided: usize,
    lagged: usize,
    extended: usize,
    inactive: usize,
    open_unknown: usize,
    pending: usize,
    blocked: usize,
}

impl AuditCounts {
    fn record(&mut self, disposition: AuditDisposition) {
        let slot = match disposition {
            AuditDisposition::Repaired => &mut self.repaired,
            AuditDisposition::Voided => &mut self.voided,
            AuditDisposition::Lagged => &mut self.lagged,
            AuditDisposition::Extended => &mut self.extended,
            AuditDisposition::Inactive => &mut self.inactive,
            AuditDisposition::OpenUnknown => &mut self.open_unknown,
            AuditDisposition::Pending => &mut self.pending,
            AuditDisposition::Blocked => &mut self.blocked,
        };
        *slot = slot.saturating_add(1);
    }

    #[cfg(test)]
    fn total(&self) -> usize {
        self.repaired
            + self.voided
            + self.lagged
            + self.extended
            + self.inactive
            + self.open_unknown
            + self.pending
            + self.blocked
    }
}

/// Audit and repair the resolution population consumed by production ranking.
/// Runs after every schedule writer so the query observes same-run Gamma rows.
///
/// Ordered classification (issue #523) — blocking is reserved for OUR-side
/// inability to record available venue truth; each blocked market warns
/// individually with a typed reason (the docs/26 operator escape needs the
/// identity), while non-blocking classes are summary-counted only. Cache write
/// errors propagate via `?` as fatal exit 1, unchanged.
async fn run_resolution_audit<F: PageFetcher + Send + Sync>(
    clob_fetcher: &clob::ClobFetcher<F>,
    cache: &mut WalletCache,
    now_unix: i64,
    repair_limit: usize,
) -> Result<AuditCounts, BootstrapError> {
    let cutoff = now_unix.saturating_sub(RESOLUTION_AUDIT_END_BUFFER_SECS);
    let initial = cache.missing_resolution_audit(cutoff, repair_limit)?;
    let missing = initial.total();
    let clipped = initial.clipped;
    let mut counts = AuditCounts::default();

    for condition_id in initial.market_ids {
        let disposition = match clob_fetcher.fetch_market(&condition_id).await {
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    market_id = condition_id,
                    reason = "fetch_failed",
                    "resolutions_audit: blocked"
                );
                AuditDisposition::Blocked
            }
            Ok(market) if market.condition_id.as_deref() != Some(condition_id.as_str()) => {
                tracing::warn!(
                    market_id = condition_id,
                    returned_market_id = ?market.condition_id,
                    reason = "identity_mismatch",
                    "resolutions_audit: blocked"
                );
                AuditDisposition::Blocked
            }
            Ok(market) => {
                let analysis = clob::analyze_winners(&market.tokens);
                let venue_end = market
                    .end_date_iso
                    .as_deref()
                    .and_then(clob::parse_iso_8601);
                match market.closed {
                    None => {
                        tracing::warn!(
                            market_id = condition_id,
                            reason = "closed_field_absent",
                            "resolutions_audit: blocked"
                        );
                        AuditDisposition::Blocked
                    }
                    Some(true) => match analysis.verdict {
                        clob::WinnerVerdict::Invalid => {
                            tracing::warn!(
                                market_id = condition_id,
                                reason = "invalid_winner_payload",
                                "resolutions_audit: blocked"
                            );
                            AuditDisposition::Blocked
                        }
                        clob::WinnerVerdict::Pending => AuditDisposition::Pending,
                        clob::WinnerVerdict::Resolved(_) | clob::WinnerVerdict::Voided => {
                            match venue_end {
                                // Terminal truth exists but we cannot faithfully
                                // record it — the wedge class the gate must catch.
                                None => {
                                    tracing::warn!(
                                        market_id = condition_id,
                                        reason = "unparseable_end_date",
                                        "resolutions_audit: blocked"
                                    );
                                    AuditDisposition::Blocked
                                }
                                Some(resolved_at_unix) => {
                                    let winner = match analysis.verdict {
                                        clob::WinnerVerdict::Resolved(idx) => Some(idx),
                                        _ => None,
                                    };
                                    cache.insert_resolution_with_source(
                                        &condition_id,
                                        winner,
                                        resolved_at_unix,
                                        now_unix,
                                        "clob",
                                    )?;
                                    if winner.is_some() {
                                        AuditDisposition::Repaired
                                    } else {
                                        AuditDisposition::Voided
                                    }
                                }
                            }
                        }
                    },
                    Some(false) if analysis.has_explicit_winner => {
                        tracing::warn!(
                            market_id = condition_id,
                            reason = "open_with_explicit_winner",
                            "resolutions_audit: blocked"
                        );
                        AuditDisposition::Blocked
                    }
                    Some(false) => match (market.active, venue_end) {
                        (Some(true), Some(end)) if end > now_unix => AuditDisposition::Extended,
                        (Some(true), Some(_)) => AuditDisposition::Lagged,
                        (Some(false), _) => AuditDisposition::Inactive,
                        (Some(true) | None, None) | (None, Some(_)) => {
                            AuditDisposition::OpenUnknown
                        }
                    },
                }
            }
        };
        counts.record(disposition);
    }

    let AuditCounts {
        repaired,
        voided,
        lagged,
        extended,
        inactive,
        open_unknown,
        pending,
        blocked,
    } = counts;
    tracing::info!(
        missing,
        repaired,
        voided,
        lagged,
        extended,
        inactive,
        open_unknown,
        pending,
        blocked,
        clipped,
        "resolutions_audit: missing={missing} repaired={repaired} voided={voided} \
         lagged={lagged} extended={extended} inactive={inactive} \
         open_unknown={open_unknown} pending={pending} blocked={blocked} clipped={clipped}"
    );
    if blocked > 0 || clipped > 0 {
        return Err(BootstrapError::ResolutionAuditIncomplete { blocked, clipped });
    }
    Ok(counts)
}

/// Gamma schedules + liquidity for still-open markets (computed AFTER the CLOB
/// stage so only truly-open markets are fetched).
async fn run_gamma_schedules_liquidity(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<(), BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;
    let open_ids: Vec<String> = unresolved_market_ids(market_ids, &cache.resolved_market_ids());
    let gamma_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .user_agent(pe_source_polymarket_public::GAMMA_BROWSER_UA)
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let gamma_fetcher = gamma::GammaFetcher::new(
        config.gamma_base_url.clone(),
        ReqwestFetcher::new(gamma_client).with_min_interval_ms(gamma::GAMMA_MIN_INTERVAL_MS),
    );
    // Fused open pass (#382): one batched `OpenOnly` request serves both schedule and liquidity,
    // halving the open-pass request volume vs the two separate passes on a cold cache.
    let (schedule_rows, liquidity_rows) = gamma_fetcher
        .fetch_schedules_and_liquidity(&open_ids, cache)
        .await?;
    tracing::info!(
        schedule_rows,
        liquidity_rows,
        open_markets = open_ids.len(),
        "bootstrap: gamma schedules + liquidity fetched (open markets only, fused)"
    );
    Ok(())
}

/// Gamma null-schedule rewrite pass (issue #137 Sub-PR 2). Scoped to the
/// trade-set ∩ null-schedule markets.
async fn run_gamma_null_rewrite(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<(), BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;
    let null_ids = cache.null_schedule_market_ids();
    let trade_set: HashSet<String> = market_ids.iter().cloned().collect();
    let rewrite_targets: Vec<String> = null_ids.intersection(&trade_set).cloned().collect();
    if !rewrite_targets.is_empty() {
        let gamma_client = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(15))
            .user_agent(pe_source_polymarket_public::GAMMA_BROWSER_UA)
            .build()
            .map_err(|_| BootstrapError::Internal)?;
        let gamma_fetcher = gamma::GammaFetcher::new(
            config.gamma_base_url.clone(),
            ReqwestFetcher::new(gamma_client).with_min_interval_ms(gamma::GAMMA_MIN_INTERVAL_MS),
        );
        let rewritten = gamma_fetcher
            .rewrite_null_schedules(&rewrite_targets, cache)
            .await?;
        tracing::info!(
            rewritten,
            candidates = rewrite_targets.len(),
            "bootstrap: gamma null-schedule rewrite complete"
        );
    }
    Ok(())
}

/// Gamma schedule backfill for resolved-but-unscheduled markets (issue #137
/// durable follow-up). The Gamma schedules stage fetches open markets only and the
/// null-rewrite stage rewrites only existing NULL rows, so a market that resolved
/// before its schedule was ever
/// fetched is left with NO `market_schedules` row forever. This stage fetches
/// `endDate` for those markets via Gamma's `&closed=true` variant (RPC-independent)
/// and INSERTs a row — even when `endDate` is absent — so the market is marked
/// attempted and not re-fetched. Scoped to `resolved ∩ market_ids − scheduled`.
pub async fn run_schedule_backfill(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<usize, BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;
    let resolved = cache.resolved_market_ids();
    let scheduled = cache.scheduled_market_ids();
    let candidate_set: HashSet<String> = market_ids.iter().cloned().collect();
    let missing: Vec<String> = resolved
        .iter()
        .filter(|id| !scheduled.contains(*id) && candidate_set.contains(*id))
        .cloned()
        .collect();
    if missing.is_empty() {
        tracing::info!("bootstrap: gamma schedule backfill — no missing-schedule markets");
        return Ok(0);
    }
    let gamma_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .user_agent(pe_source_polymarket_public::GAMMA_BROWSER_UA)
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let gamma_fetcher = gamma::GammaFetcher::new(
        config.gamma_base_url.clone(),
        ReqwestFetcher::new(gamma_client).with_min_interval_ms(gamma::GAMMA_MIN_INTERVAL_MS),
    );
    let inserted = gamma_fetcher
        .backfill_missing_schedules(&missing, cache)
        .await?;
    tracing::info!(
        inserted,
        candidates = missing.len(),
        "bootstrap: gamma schedule backfill complete"
    );
    Ok(inserted)
}

/// Return every market in `all` that is not present in `resolved`.
///
/// Pure data manipulation — no I/O.
pub(crate) fn unresolved_market_ids(all: &[String], resolved: &HashSet<String>) -> Vec<String> {
    all.iter()
        .filter(|id| !resolved.contains(*id))
        .cloned()
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn seed_audit_trade(cache: &WalletCache, id: &str, market_id: &str) {
        cache
            .raw_conn_for_test()
            .execute(
                "INSERT INTO trades (source_trade_id, wallet_hex, market_id, outcome_id, \
                 side, price_str, contracts, timestamp_unix) \
                 VALUES (?1, '0x0000000000000000000000000000000000000001', ?2, 0, \
                         'buy', '0.5', 1, 1)",
                rusqlite::params![id, market_id],
            )
            .unwrap();
    }

    fn audit_fetcher(
        responses: HashMap<String, Vec<u8>>,
    ) -> clob::ClobFetcher<pe_source_polymarket_public::FixtureFetcher> {
        clob::ClobFetcher::new(
            "https://clob.example".to_owned(),
            pe_source_polymarket_public::FixtureFetcher::new(responses),
        )
    }

    #[test]
    fn unresolved_market_ids_empty_when_all_resolved() {
        let all = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let resolved: HashSet<String> = all.iter().cloned().collect();
        assert!(unresolved_market_ids(&all, &resolved).is_empty());
    }

    #[test]
    fn unresolved_market_ids_returns_complement_partial() {
        let all = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let resolved: HashSet<String> = ["b".to_owned()].into_iter().collect();
        let got = unresolved_market_ids(&all, &resolved);
        assert_eq!(got, vec!["a".to_owned(), "c".to_owned()]);
    }

    #[test]
    fn unresolved_market_ids_all_when_none_resolved() {
        let all = vec!["a".to_owned(), "b".to_owned()];
        let resolved: HashSet<String> = HashSet::new();
        assert_eq!(unresolved_market_ids(&all, &resolved), all);
    }

    #[test]
    fn unresolved_market_ids_empty_input_is_empty_output() {
        let all: Vec<String> = Vec::new();
        let resolved: HashSet<String> = HashSet::new();
        assert!(unresolved_market_ids(&all, &resolved).is_empty());
    }

    #[tokio::test]
    // Also the issue #523 proof that `active` is irrelevant to terminal
    // handling: neither fixture carries the field, and both still record.
    async fn resolution_audit_repairs_winner_and_records_voided_terminal_row() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        for id in ["winner", "voided"] {
            seed_audit_trade(&cache, &format!("trade-{id}"), id);
            cache.insert_schedule(id, Some(1_000), 1).unwrap();
        }
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/winner".to_owned(),
            br#"{"condition_id":"winner","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"tokens":[{"winner":true},{"winner":false}]}"#.to_vec(),
        );
        responses.insert(
            "https://clob.example/markets/voided".to_owned(),
            br#"{"condition_id":"voided","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"tokens":[{"winner":false},{"winner":false}]}"#.to_vec(),
        );

        run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap();

        assert_eq!(
            cache.resolution_record("winner"),
            Some((Some(0), 1_000, 10_000, "clob".to_owned()))
        );
        assert_eq!(
            cache.resolution_record("voided"),
            Some((None, 1_000, 10_000, "clob".to_owned()))
        );
    }

    #[tokio::test]
    async fn resolution_audit_unfetchable_market_is_incomplete() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-missing", "missing");
        cache.insert_schedule("missing", Some(1_000), 1).unwrap();

        let error = run_resolution_audit(&audit_fetcher(HashMap::new()), &mut cache, 10_000, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0
            }
        ));
    }

    #[tokio::test]
    async fn resolution_audit_clipped_population_is_incomplete_without_fetching() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        for id in ["m1", "m2"] {
            seed_audit_trade(&cache, &format!("trade-{id}"), id);
            cache.insert_schedule(id, Some(1_000), 1).unwrap();
        }

        let error = run_resolution_audit(&audit_fetcher(HashMap::new()), &mut cache, 10_000, 0)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 0,
                clipped: 2
            }
        ));
    }

    #[tokio::test]
    async fn resolution_audit_partial_winner_vector_stays_pending_and_writes_nothing() {
        // Issue #523: `[true, absent]` must never resolve — a wrongly recorded
        // winner is permanent. Pending is non-blocking: the run succeeds.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-p", "partial");
        cache.insert_schedule("partial", Some(1_000), 1).unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/partial".to_owned(),
            br#"{"condition_id":"partial","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"active":false,"tokens":[{"winner":true},{}]}"#.to_vec(),
        );

        let counts = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap();
        assert_eq!(counts.pending, 1);
        assert_eq!(counts.total(), 1);
        assert!(cache.resolution_record("partial").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_invalid_winner_payload_blocks() {
        // Issue #523: `[true, true, absent]` is contradictory — Invalid, blocked.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-i", "invalid");
        cache.insert_schedule("invalid", Some(1_000), 1).unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/invalid".to_owned(),
            br#"{"condition_id":"invalid","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"tokens":[{"winner":true},{"winner":true},{}]}"#.to_vec(),
        );

        let error = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0
            }
        ));
        assert!(cache.resolution_record("invalid").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_open_market_with_explicit_winner_blocks() {
        // Issue #523: an open response carrying an explicit winner is
        // contradictory venue state — fail closed.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-ow", "open-winner");
        cache
            .insert_schedule("open-winner", Some(1_000), 1)
            .unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/open-winner".to_owned(),
            br#"{"condition_id":"open-winner","end_date_iso":"1970-01-01T00:16:40Z","closed":false,"active":true,"tokens":[{"winner":true},{"winner":false}]}"#.to_vec(),
        );

        let error = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0
            }
        ));
        assert!(cache.resolution_record("open-winner").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_terminal_with_unparseable_end_blocks() {
        // Issue #523: terminal venue truth we cannot faithfully record — the
        // wedge class the gate exists for.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-be", "bad-end");
        cache.insert_schedule("bad-end", Some(1_000), 1).unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/bad-end".to_owned(),
            br#"{"condition_id":"bad-end","end_date_iso":"not a date","closed":true,"tokens":[{"winner":true},{"winner":false}]}"#.to_vec(),
        );

        let error = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0
            }
        ));
        assert!(cache.resolution_record("bad-end").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_absent_closed_field_blocks() {
        // Issue #523: presence-preserving `closed` — venue schema drift fails
        // closed instead of collapsing into `false`.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-nc", "no-closed");
        cache.insert_schedule("no-closed", Some(1_000), 1).unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/no-closed".to_owned(),
            br#"{"condition_id":"no-closed","end_date_iso":"1970-01-01T00:16:40Z","tokens":[]}"#
                .to_vec(),
        );

        let error = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0
            }
        ));
    }

    #[tokio::test]
    async fn resolution_audit_open_classes_are_non_blocking_and_accounted() {
        // Issue #523: lagged / extended / inactive / open_unknown all pass,
        // write nothing, and the counters account for the whole attempted
        // population (the test-only accounting identity).
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let fixtures: [(&str, &[u8]); 4] = [
            (
                "lag",
                br#"{"condition_id":"lag","end_date_iso":"1970-01-01T00:16:40Z","closed":false,"active":true,"tokens":[{"winner":false},{}]}"#,
            ),
            (
                "ext",
                br#"{"condition_id":"ext","end_date_iso":"2100-01-01T00:00:00Z","closed":false,"active":true,"tokens":[]}"#,
            ),
            (
                "inact",
                br#"{"condition_id":"inact","end_date_iso":"1970-01-01T00:16:40Z","closed":false,"active":false,"tokens":[]}"#,
            ),
            (
                "unk",
                br#"{"condition_id":"unk","end_date_iso":"1970-01-01T00:16:40Z","closed":false,"tokens":[]}"#,
            ),
        ];
        let mut responses = HashMap::new();
        for (id, body) in fixtures {
            seed_audit_trade(&cache, &format!("trade-{id}"), id);
            cache.insert_schedule(id, Some(1_000), 1).unwrap();
            responses.insert(format!("https://clob.example/markets/{id}"), body.to_vec());
        }

        let counts = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap();
        assert_eq!(counts.lagged, 1);
        assert_eq!(counts.extended, 1);
        assert_eq!(counts.inactive, 1);
        assert_eq!(counts.open_unknown, 1);
        assert_eq!(
            counts.total(),
            4,
            "every attempted id maps to one disposition"
        );
        for (id, _) in fixtures {
            assert!(cache.resolution_record(id).is_none());
        }
    }

    #[tokio::test]
    async fn resolution_audit_open_active_with_unparseable_end_is_open_unknown() {
        // Issue #523: missing diagnostic metadata on an OPEN market never
        // blocks — active=true with a malformed venue end is open_unknown.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-ue", "unk-end");
        cache.insert_schedule("unk-end", Some(1_000), 1).unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/unk-end".to_owned(),
            br#"{"condition_id":"unk-end","end_date_iso":"not a date","closed":false,"active":true,"tokens":[]}"#.to_vec(),
        );

        let counts = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap();
        assert_eq!(counts.open_unknown, 1);
        assert!(cache.resolution_record("unk-end").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_closed_pending_with_unparseable_end_stays_pending() {
        // Issue #523 precedence proof: winner classification runs BEFORE the
        // end-date requirement, so incomplete flags stay non-blocking Pending
        // even when the end date is malformed.
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-pb", "pend-bad-end");
        cache
            .insert_schedule("pend-bad-end", Some(1_000), 1)
            .unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/pend-bad-end".to_owned(),
            br#"{"condition_id":"pend-bad-end","end_date_iso":"not a date","closed":true,"tokens":[{"winner":true},{}]}"#.to_vec(),
        );

        let counts = run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap();
        assert_eq!(counts.pending, 1);
        assert!(cache.resolution_record("pend-bad-end").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_excludes_null_schedule() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-null", "null");
        cache.insert_schedule("null", None, 1).unwrap();

        run_resolution_audit(&audit_fetcher(HashMap::new()), &mut cache, 10_000, 10)
            .await
            .unwrap();
        assert!(cache.resolution_record("null").is_none());
    }

    #[tokio::test]
    async fn resolution_audit_observes_schedule_inserted_immediately_before_stage() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        seed_audit_trade(&cache, "trade-same-run", "same-run");
        cache
            .insert_schedule_with_source("same-run", Some(1_000), 1, "gamma")
            .unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "https://clob.example/markets/same-run".to_owned(),
            br#"{"condition_id":"same-run","end_date_iso":"1970-01-01T00:16:40Z","closed":true,"tokens":[{"winner":false},{"winner":true}]}"#.to_vec(),
        );

        run_resolution_audit(&audit_fetcher(responses), &mut cache, 10_000, 10)
            .await
            .unwrap();
        assert_eq!(
            cache.resolution_record("same-run").map(|record| record.0),
            Some(Some(1))
        );
    }
}
