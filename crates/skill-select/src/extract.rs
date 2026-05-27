//! Phase-A extraction orchestration (issue #212).
//!
//! Streams the active-tradeable wallet universe one wallet at a time — never
//! holding all ~269M trades in memory (this is what dissolves the `pe-backtest`
//! OOM). For each wallet: read its trades, keep the train window (`≤ cutoff`),
//! reconstruct the FIFO ledger, compute the deterministic features
//! ([`crate::features`]) and the sign-randomization skill test
//! ([`crate::skill_test`]), then persist the assembled [`WalletFeatures`] row.
//!
//! The per-wallet loop runs **in parallel** via rayon — each worker opens its
//! own read-only `WalletCache` handle (SQLite needs per-thread connections) and
//! the shared inputs (`event_map`, `resolutions`) are `Arc`-shared. Worker count
//! is `extract_threads` (`0` = rayon's default — honours `RAYON_NUM_THREADS`,
//! else CPU count). The read pass (bootstrap `WalletCache`) and the single
//! batched write (`SkillCache`, read-write) do not interleave: every wallet has
//! been scored before the single write transaction opens, so concurrent SQLite
//! writers are not in scope.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pe_bootstrap::cache::{ResolutionIndex, WalletCache};
use pe_trader_index::{LedgerConfig, build_trader_ledgers};
use rayon::prelude::*;
use tracing::{info, warn};

use crate::db::{SkillCache, WalletFeatures};
use crate::error::SkillSelectError;
use crate::features::extract_features;
use crate::skill_test::sign_randomization_test;

/// Outcome of an extraction pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExtractReport {
    /// Active-tradeable wallets enumerated.
    pub wallets_scanned: usize,
    /// Wallets that produced a `wallet_features` row (≥ `min_closed_trades`).
    pub wallets_written: usize,
    /// Wallets skipped (no/insufficient train-window closed trades).
    pub wallets_skipped: usize,
}

/// Run Phase-A extraction over the active-tradeable universe and persist a
/// `wallet_features` row per eligible wallet at `cutoff_unix`.
///
/// Idempotent at a given cutoff (`INSERT OR REPLACE`). `extracted_at_unix` is
/// stamped on every row. Returns counts; never partially-fails — a wallet that
/// can't be scored is skipped, not fatal. The `min_distinct_events` /
/// `bb_alpha` / `bb_beta` knobs are the SSRN 6617059 §C event-count gate and
/// the beta-binomial conjugate prior for the per-bet shrunk-edge feature;
/// defaults live in [`crate::SkillConfig`]. `extract_threads = 0` accepts
/// rayon's default parallelism.
///
/// # Determinism note
/// Per-wallet results are independent of execution order — every wallet's
/// scoring is a pure function of its trades, the shared `event_map`, and the
/// shared `resolutions` map. The sign-randomization test uses the same fixed
/// `seed` per wallet regardless of which worker runs it. Set membership in the
/// resulting `wallet_features` rows is therefore bit-identical across thread
/// counts; only the on-disk row insertion order may differ.
#[allow(clippy::too_many_arguments)] // canonical pipeline orchestrator; one site.
pub fn run_extract(
    cache_path: &std::path::Path,
    cutoff_unix: i64,
    min_closed_trades: u32,
    min_distinct_events: u32,
    bb_alpha: u32,
    bb_beta: u32,
    permutations: u32,
    seed: u64,
    extracted_at_unix: i64,
    extract_threads: usize,
    clean_prior: bool,
) -> Result<ExtractReport, SkillSelectError> {
    // Opt-in pre-clean (issue #236): drop every row at `cutoff_unix` before the
    // extract begins. Runs in its own short write transaction so it doesn't
    // hold the SQLite write lock across the multi-minute extract body. The
    // brief "no rows" window between this and the single batched write at the
    // end is intentional — `select` / `composite` callers should not be
    // running concurrently during an extract anyway.
    if clean_prior {
        let mut skill_cache = SkillCache::open(cache_path)?;
        let deleted = skill_cache.delete_features_for_cutoff(cutoff_unix)?;
        info!(
            cutoff_unix,
            deleted_rows = deleted,
            "skill-select extract: pre-clean removed prior-cutoff rows"
        );
    }

    // Read-pass shared inputs: load once on the main thread, share to workers.
    let (event_map, resolutions, rank_index, wallets, rank_index_was_cached) = {
        let cache = WalletCache::open_read_only(cache_path)?;
        let event_map: HashMap<String, String> = cache.load_market_event_map()?;
        let resolutions = cache.load_all_resolutions()?;
        // Cross-wallet first-buy rank index for `first_mover_percentile_bps`
        // (#248 §3). Check the persistent cache first: the full GROUP BY scan
        // over ~135M buy-side trades takes O(minutes) on a cold page cache;
        // the cache table load is a simple indexed read of the already-computed
        // result, measured in seconds.
        let rank_index_start = std::time::Instant::now();
        let cached = cache
            .rank_index_cache_exists(cutoff_unix)
            .map_err(|e| SkillSelectError::Decode(format!("rank index cache check: {e}")))?;
        let (rank_index, was_cached) = if cached {
            let idx = cache
                .load_rank_index_cache(cutoff_unix)
                .map_err(|e| SkillSelectError::Decode(format!("rank index cache load: {e}")))?;
            (idx, true)
        } else {
            let idx = cache
                .load_first_mover_rank_index(cutoff_unix)
                .map_err(|e| SkillSelectError::Decode(format!("rank index build: {e}")))?;
            (idx, false)
        };
        info!(
            elapsed_ms = u64::try_from(rank_index_start.elapsed().as_millis()).unwrap_or(u64::MAX),
            groups_indexed = rank_index.len(),
            from_cache = was_cached,
            cutoff_unix,
            "skill-select extract: cross-wallet rank index ready"
        );
        let wallets = cache.active_tradeable_wallet_hexes()?;
        (event_map, resolutions, rank_index, wallets, was_cached)
    };

    // Persist a freshly-built rank index so the next re-extract at this cutoff
    // skips the full GROUP BY scan. Done after the read-only block closes so
    // the write transaction doesn't overlap with the long read pass.
    if !rank_index_was_cached {
        let mut write_cache = WalletCache::open(cache_path)
            .map_err(|e| SkillSelectError::Decode(format!("rank index cache open: {e}")))?;
        let saved = write_cache
            .save_rank_index_cache(cutoff_unix, &rank_index)
            .map_err(|e| SkillSelectError::Decode(format!("rank index cache save: {e}")))?;
        info!(
            saved_rows = saved,
            cutoff_unix, "skill-select extract: rank index cached for future re-extracts"
        );
    }
    let event_map = Arc::new(event_map);
    let resolutions = Arc::new(resolutions);
    let rank_index = Arc::new(rank_index);

    // Workers count: 0 = rayon's default (CPU count / RAYON_NUM_THREADS).
    // The default thread pool is global and lazy-initialised; a custom pool
    // exists only when the caller wants a specific worker count.
    let pool: Option<rayon::ThreadPool> = if extract_threads > 0 {
        Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(extract_threads)
                .build()
                .map_err(|e| SkillSelectError::Decode(format!("rayon pool: {e}")))?,
        )
    } else {
        None
    };
    let effective_threads = pool
        .as_ref()
        .map(rayon::ThreadPool::current_num_threads)
        .unwrap_or_else(rayon::current_num_threads);

    let wallets_scanned = wallets.len();
    let wallets_skipped = AtomicUsize::new(0);

    // Drive the per-wallet work either on the custom pool (if any) or the
    // global pool. `install` runs the closure inside the pool; outside callers
    // see the same return type.
    let per_wallet = || {
        wallets
            .par_iter()
            .filter_map(|hex| {
                process_wallet(
                    hex,
                    cache_path,
                    cutoff_unix,
                    min_closed_trades,
                    min_distinct_events,
                    bb_alpha,
                    bb_beta,
                    permutations,
                    seed,
                    extracted_at_unix,
                    &event_map,
                    &resolutions,
                    &rank_index,
                    &wallets_skipped,
                )
            })
            .collect::<Vec<WalletFeatures>>()
    };
    let batch: Vec<WalletFeatures> = if let Some(p) = pool.as_ref() {
        p.install(per_wallet)
    } else {
        per_wallet()
    };

    let report = ExtractReport {
        wallets_scanned,
        wallets_written: batch.len(),
        wallets_skipped: wallets_skipped.load(Ordering::Relaxed),
    };

    // Write pass: skill cache, read-write (single transaction). The parallel
    // section ends before this point — only the main thread writes.
    let mut skill_cache = SkillCache::open(cache_path)?;
    skill_cache.upsert_features_batch(&batch)?;

    info!(
        wallets_scanned = report.wallets_scanned,
        wallets_written = report.wallets_written,
        wallets_skipped = report.wallets_skipped,
        cutoff_unix,
        extract_threads = effective_threads,
        "skill-select extract: complete"
    );
    Ok(report)
}

/// Score one wallet. Opens its own read-only `WalletCache` handle so rayon
/// workers each get a private SQLite connection; this is cheap on a file DB
/// (~ms) and side-steps SQLite's "one connection per thread" rule. A wallet
/// that fails to open the cache is logged-and-skipped rather than aborting the
/// entire extract (matches the "never partially-fails" contract).
#[allow(clippy::too_many_arguments)] // matches `run_extract` orchestrator surface.
fn process_wallet(
    hex: &str,
    cache_path: &Path,
    cutoff_unix: i64,
    min_closed_trades: u32,
    min_distinct_events: u32,
    bb_alpha: u32,
    bb_beta: u32,
    permutations: u32,
    seed: u64,
    extracted_at_unix: i64,
    event_map: &HashMap<String, String>,
    resolutions: &ResolutionIndex,
    rank_index: &HashMap<(String, u16), Vec<i64>>,
    wallets_skipped: &AtomicUsize,
) -> Option<WalletFeatures> {
    let cache = match WalletCache::open_read_only(cache_path) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, wallet = %hex, "skill-select extract: read-only open failed; skipping");
            wallets_skipped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };

    let train: Vec<_> = cache
        .trades_for(hex)
        .into_iter()
        .filter(|t| t.timestamp.0.unix_timestamp() <= cutoff_unix)
        .collect();
    if train.is_empty() {
        wallets_skipped.fetch_add(1, Ordering::Relaxed);
        return None;
    }

    let ledger_config = LedgerConfig::default();
    let Some(ledger) = build_trader_ledgers(&train, 0, &[], None, &ledger_config)
        .into_iter()
        .next()
    else {
        wallets_skipped.fetch_add(1, Ordering::Relaxed);
        return None;
    };

    let Some(features) = extract_features(
        &ledger,
        &train,
        cutoff_unix,
        event_map,
        resolutions,
        rank_index,
        min_closed_trades,
        min_distinct_events,
        bb_alpha,
        bb_beta,
    ) else {
        wallets_skipped.fetch_add(1, Ordering::Relaxed);
        return None;
    };

    let skill = sign_randomization_test(&ledger.closed_trades, event_map, permutations, seed);
    Some(WalletFeatures {
        features,
        extracted_at_unix,
        skill_pnl_usd: skill.observed_pnl,
        skill_pvalue_bps: skill.pvalue_bps,
        skill_permutations: skill.permutations,
    })
}
