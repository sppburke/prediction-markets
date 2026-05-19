//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline (invoked as `pe-bootstrap all` or no-arg):
//! 1. `migrate::auto_migrate_legacy` — one-shot SQLite consolidation.
//! 2. `enumerate::run_enumerate` — discover wallets via Dune or Polygon on-chain.
//! 3. `fetch::run_fetch` — fetch Polymarket trade history per wallet.
//! 4. `funder::run_funder` — resolve on-chain funder edges (opt-in).
//! 5. `watchlist_phase::run_watchlist` — reconstruct ledgers, filter, write watchlist.json.
//! 6. `fetch_resolutions_and_schedules` — fetch market resolution data (opt-in).
//!
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.

pub mod backfill;
pub mod cache;
pub mod clob;
pub mod config;
pub mod delta_audit;
pub mod discovery;
pub mod dune;
pub mod enumerate;
pub mod error;
pub mod fetch;
pub mod filter;
pub mod funder;
pub mod gamma;
pub mod lock;
pub mod migrate;
pub mod operator_audit;
pub mod pile;
pub mod polygon_ctf;
pub mod polygon_ctf_delta;
pub mod polymarket;
pub mod seed_historical;
pub mod wallet_set;
pub mod watchlist_phase;
pub mod weekly;

pub use config::BootstrapConfig;
// Re-exports for callers that previously imported these from the crate root.
pub use seed_historical::parse_seed_as_of_env;
pub use watchlist_phase::build_seed_watchlist;

use std::collections::HashSet;
use std::time::Duration;

use pe_source_onchain_polygon::contracts::CTF_DEPLOY_BLOCK;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use cache::WalletCache;
use dune::DuneClient;
use error::BootstrapError;

// Upper-bound block for funder discovery — both endpoints finalized, result is time-invariant.
pub const FUNDER_DISCOVERY_TO_BLOCK: u64 = 80_000_000;

/// Wallet discovery backend.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalletSource {
    /// Use Dune Analytics (legacy path; requires `PE_DUNE_API_KEY`).
    Dune,
    /// Use Polygon RPC `eth_getLogs` via alloy (requires `PE_BOOTSTRAP_POLYGON_RPC_URL`).
    /// `"etherscan"` is accepted as a serde alias for backward compat with existing config files.
    #[default]
    #[serde(alias = "etherscan")]
    OnChain,
}

/// Delta-backfill mode (issue #176).
///
/// Selects how `backfill::run_backfill` uses the Polygon CTF on-chain `eth_getLogs`
/// scan to narrow the per-day Polymarket API surface.
///
/// - [`DeltaMode::Off`] — legacy behaviour. Every due wallet from
///   `select_backfill_due` is fetched. No on-chain scan; no audit rows.
/// - [`DeltaMode::Shadow`] — runs the on-chain scan AND the legacy full fetch on
///   every backfill run; classifies each wallet into `DELTA_HIT` / `DELTA_MISS` /
///   `DELTA_EXTRA` rows in the `delta_audit` table.
/// - [`DeltaMode::Delta`] — the on-chain scan filters the fetch set down to
///   `(full_due_set ∩ delta_set) ∪ paranoia_set`.
///
/// `Default` is [`DeltaMode::Shadow`] — safe-by-default. The delta scanner is only
/// invoked inside `backfill::run_backfill`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaMode {
    /// Disable the on-chain scan entirely; legacy fetch-all-due behaviour.
    Off,
    /// Run scan + full fetch; populate `delta_audit` but do not change the fetch set.
    #[default]
    Shadow,
    /// Use the scan to filter the fetch set; weekly paranoia provides the backstop.
    Delta,
}

/// Multi-source resolution + schedule pipeline (issue #149, extracted in #166).
///
/// Stage ordering puts precision sources first so `INSERT OR IGNORE` keeps the
/// most accurate `resolved_at_unix`:
///
/// - 6a. Polygon RPC CTF scan → `source='polygon'` (block timestamp).
/// - 6b. Dune `ctf_evt_conditionresolution` → `source='dune'` (block timestamp).
/// - 6c. CLOB closed-market pagination → `source='clob'` (`end_date_iso` approx).
/// - 6d. Gamma schedules → `source='gamma'` (open markets only).
/// - 6e. Gamma liquidity → only Gamma exposes liquidity (open markets).
/// - 6f. Gamma null-schedule rewrite pass (issue #137 Sub-PR 2).
///
/// `open_ids` is computed AFTER 6a/6b/6c so Gamma only fetches truly-still-open
/// markets. `market_ids` scopes the run — pass `cache.all_market_ids()` for the
/// full pipeline or just the wallets-newly-fetched market set for targeted backfill.
///
/// `config.rebuild_resolutions = true` drops every imprecise-source row
/// (`'gamma'`, `'clob'`) up front so the precision stages can re-populate them
/// with block-timestamp accuracy. Idempotent — safe on every run.
pub async fn fetch_resolutions_and_schedules(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<(), BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;

    if config.rebuild_resolutions {
        let deleted = cache.delete_resolutions_by_sources(&["gamma", "clob"])?;
        tracing::info!(
            deleted,
            "bootstrap: rebuild_resolutions=1 — deleted imprecise-source rows so precision stages repopulate"
        );
    }

    // 6a. Polygon RPC scan.
    if let Some(rpc_url) = config.polygon_rpc_url.as_deref() {
        let from_block = cache
            .get_source_cursor(polygon_ctf::POLYGON_CTF_CURSOR_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(CTF_DEPLOY_BLOCK);
        let inserted = polygon_ctf::scan_resolutions(
            rpc_url,
            from_block,
            None,
            config.polygon_ctf_chunk_blocks,
            cache,
        )
        .await?;
        tracing::info!(
            inserted,
            from_block,
            "bootstrap: polygon_ctf resolutions fetched"
        );
    }

    // 6b. Dune `ctf_evt_conditionresolution`.
    if let Some(api_key) = &config.dune_api_key {
        let unresolved = unresolved_market_ids(market_ids, &cache.resolved_market_ids());
        if !unresolved.is_empty() {
            let dune_resolution_client = DuneClient::new(api_key.clone());
            let unresolved_set: HashSet<String> = unresolved.into_iter().collect();
            let rows = dune_resolution_client
                .fetch_resolutions(&unresolved_set, 0, config.dune_namespace.as_deref())
                .await?;
            let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
            let mut inserted = 0usize;
            for (market_id, winner, resolved_at_unix) in rows {
                cache.insert_resolution_with_source(
                    &market_id,
                    winner,
                    resolved_at_unix,
                    fetched_at,
                    "dune",
                )?;
                inserted += 1;
            }
            tracing::info!(
                inserted,
                unresolved = unresolved_set.len(),
                "bootstrap: dune resolutions fetched"
            );
        }
    }

    // 6c. CLOB closed-market pagination.
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

    // 6d/e. Gamma schedules + liquidity — open markets only.
    let open_ids: Vec<String> = unresolved_market_ids(market_ids, &cache.resolved_market_ids());
    let gamma_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
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

    // 6f. Null-schedule rewrite pass (issue #137 Sub-PR 2).
    let null_ids = cache.null_schedule_market_ids();
    let trade_set: HashSet<String> = market_ids.iter().cloned().collect();
    let rewrite_targets: Vec<String> = null_ids.intersection(&trade_set).cloned().collect();
    if !rewrite_targets.is_empty() {
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
