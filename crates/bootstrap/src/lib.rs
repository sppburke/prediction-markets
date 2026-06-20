//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline (invoked as `pe-bootstrap all` or no-arg):
//! 1. `migrate::auto_migrate_legacy` — one-shot SQLite consolidation.
//! 2. `winner_discovery::run_winner_discovery` — discover wallets via the
//!    Polymarket leaderboard (all categories) + Radion (when activated).
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
pub mod radion;
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

// Upper-bound block for funder discovery — both endpoints finalized, result is time-invariant.
pub const FUNDER_DISCOVERY_TO_BLOCK: u64 = 80_000_000;

/// Outcome of [`fetch_resolutions_and_schedules`] (issue #201).
///
/// The optional Gamma stages (schedules/liquidity, null-rewrite,
/// schedule-backfill) soft-fail independently: a failure logs a warning and
/// records the stage name here rather than aborting the pipeline. The primary
/// CLOB stage hard-fails (propagates `Err`) — see [`fetch_resolutions_and_schedules`].
/// A non-empty `stages_failed` ⇒ the caller should treat the run as partial
/// (exit 2), matching the backfill/weekly convention.
#[derive(Debug, Default, Clone)]
pub struct ResolutionsReport {
    /// Names of optional stages that soft-failed this run (e.g. `"clob"`).
    pub stages_failed: Vec<&'static str>,
}

impl ResolutionsReport {
    /// True when at least one optional stage soft-failed.
    pub fn has_failures(&self) -> bool {
        !self.stages_failed.is_empty()
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
/// `config.rebuild_resolutions = true` drops every imprecise-source row
/// (`'gamma'`, `'clob'`) up front so the CLOB stage re-populates the `'clob'`
/// rows. Retained `source='polygon'` rows are NOT deleted — they keep their exact
/// block-timestamp `resolved_at_unix`. With the on-chain scan gone there is no
/// precise re-derivation, so a rebuild re-fetches `'clob'` rows from CLOB only.
/// Idempotent — safe on every run.
pub async fn fetch_resolutions_and_schedules(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<ResolutionsReport, BootstrapError> {
    if config.rebuild_resolutions {
        let deleted = cache.delete_resolutions_by_sources(&["gamma", "clob"])?;
        tracing::info!(
            deleted,
            "bootstrap: rebuild_resolutions=1 — deleted imprecise-source rows so precision stages repopulate"
        );
    }

    let mut stages_failed: Vec<&'static str> = Vec::new();

    // CLOB closed-market pagination — primary resolution source; hard-fail
    // (propagate). A CLOB outage aborts the run rather than silently producing a
    // resolutions pass missing the gold source (issue #369).
    run_clob_closed_markets(config, cache).await?;

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

    Ok(ResolutionsReport { stages_failed })
}

/// CLOB closed-market pagination → `source='clob'` (`end_date_iso` approx).
/// Primary, sole market-resolution source (issue #369).
async fn run_clob_closed_markets(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<(), BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;
    let clob_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let clob_fetcher = clob::ClobFetcher::new(
        config.clob_base_url.clone(),
        ReqwestFetcher::new(clob_client),
    );
    let (clob_schedules, clob_resolutions) = clob_fetcher.fetch_closed_markets(cache).await?;
    tracing::info!(
        clob_schedules,
        clob_resolutions,
        "bootstrap: clob closed markets fetched"
    );
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
    let schedule_rows = gamma_fetcher.fetch_schedules(&open_ids, cache).await?;
    tracing::info!(
        schedule_rows,
        open_markets = open_ids.len(),
        "bootstrap: gamma schedules fetched (open markets only)"
    );
    let liquidity_rows = gamma_fetcher
        .fetch_market_liquidity(&open_ids, cache)
        .await?;
    tracing::info!(
        liquidity_rows,
        open_markets = open_ids.len(),
        "bootstrap: gamma liquidity fetched (open markets only)"
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
}
