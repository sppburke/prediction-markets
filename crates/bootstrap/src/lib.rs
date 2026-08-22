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
/// one hour past and repairs missing terminal rows through the per-market CLOB
/// endpoint. Any remaining or clipped population returns
/// [`BootstrapError::ResolutionAuditIncomplete`].
pub async fn fetch_resolutions_and_schedules(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<ResolutionsReport, BootstrapError> {
    if config.rebuild_resolutions {
        cache.delete_source_cursor(clob::CLOB_CLOSED_CURSOR_KEY)?;
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

/// Audit and repair the resolution population consumed by production ranking.
/// Runs after every schedule writer so the query observes same-run Gamma rows.
async fn run_resolution_audit<F: PageFetcher + Send + Sync>(
    clob_fetcher: &clob::ClobFetcher<F>,
    cache: &mut WalletCache,
    now_unix: i64,
    repair_limit: usize,
) -> Result<(), BootstrapError> {
    let cutoff = now_unix.saturating_sub(RESOLUTION_AUDIT_END_BUFFER_SECS);
    let initial = cache.missing_resolution_audit(cutoff, repair_limit)?;
    let missing = initial.total();
    let clipped = initial.clipped;
    let mut repaired = 0usize;
    let mut voided = 0usize;

    for condition_id in initial.market_ids {
        let market = match clob_fetcher.fetch_market(&condition_id).await {
            Ok(market) => market,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    market_id = condition_id,
                    "resolutions_audit: per-market repair fetch failed"
                );
                continue;
            }
        };
        if market.condition_id.as_deref() != Some(condition_id.as_str()) {
            tracing::warn!(
                market_id = condition_id,
                returned_market_id = ?market.condition_id,
                "resolutions_audit: per-market response identity mismatch"
            );
            continue;
        }
        if !market.closed {
            tracing::warn!(
                market_id = condition_id,
                "resolutions_audit: per-market response is not closed"
            );
            continue;
        }
        let Some(resolved_at_unix) = market
            .end_date_iso
            .as_deref()
            .and_then(clob::parse_iso_8601)
        else {
            tracing::warn!(
                market_id = condition_id,
                "resolutions_audit: per-market response has no valid end_date_iso"
            );
            continue;
        };
        let winner = match clob::classify_winner(&market.tokens) {
            clob::WinnerVerdict::Resolved(idx) => Some(idx),
            clob::WinnerVerdict::Voided => None,
            clob::WinnerVerdict::Pending => {
                // Flags not posted yet (or invalid payload): leave the market
                // missing so this pass counts it in `still_missing` and the next
                // cycle retries — never freeze a premature NULL (issue #519 review).
                tracing::warn!(
                    market_id = condition_id,
                    "resolutions_audit: per-market winner flags pending"
                );
                continue;
            }
        };
        cache.insert_resolution_with_source(
            &condition_id,
            winner,
            resolved_at_unix,
            now_unix,
            "clob",
        )?;
        if winner.is_some() {
            repaired += 1;
        } else {
            voided += 1;
        }
    }

    let remaining = cache.missing_resolution_audit(cutoff, 0)?.total();
    let still_missing = remaining.saturating_sub(clipped);
    tracing::info!(
        missing,
        repaired,
        voided,
        still_missing,
        clipped,
        "resolutions_audit: missing={missing} repaired={repaired} voided={voided} \
         still_missing={still_missing} clipped={clipped}"
    );
    if still_missing > 0 || clipped > 0 {
        return Err(BootstrapError::ResolutionAuditIncomplete {
            still_missing,
            clipped,
        });
    }
    Ok(())
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
                still_missing: 1,
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
                still_missing: 0,
                clipped: 2
            }
        ));
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
