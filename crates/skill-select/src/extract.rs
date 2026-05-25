//! Phase-A extraction orchestration (issue #212).
//!
//! Streams the active-tradeable wallet universe one wallet at a time — never
//! holding all ~269M trades in memory (this is what dissolves the `pe-backtest`
//! OOM). For each wallet: read its trades, keep the train window (`≤ cutoff`),
//! reconstruct the FIFO ledger, compute the deterministic features
//! ([`crate::features`]) and the sign-randomization skill test
//! ([`crate::skill_test`]), then persist the assembled [`WalletFeatures`] row.
//!
//! v1 is **sequential**; parallelising the per-wallet loop with rayon (one
//! read-only connection per worker) is a deferred performance follow-up. Reads
//! (bootstrap `WalletCache`, read-only) and the single batched write (`SkillCache`,
//! read-write) do not interleave.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_core_types::SourceTimestamp;
use pe_trader_index::{LedgerConfig, TradeSnapshot, build_trader_ledgers};
use time::OffsetDateTime;
use tracing::info;

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
/// defaults live in [`crate::SkillConfig`].
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
) -> Result<ExtractReport, SkillSelectError> {
    let snapshot_at = SourceTimestamp(
        OffsetDateTime::from_unix_timestamp(cutoff_unix)
            .map_err(|e| SkillSelectError::Decode(format!("cutoff_unix {cutoff_unix}: {e}")))?,
    );

    // Read pass: bootstrap cache, read-only.
    let cache = WalletCache::open_read_only(cache_path)?;
    let event_map: HashMap<String, String> = cache.load_market_event_map()?;
    let resolutions = cache.load_all_resolutions()?;
    let wallets = cache.active_tradeable_wallet_hexes()?;
    let ledger_config = LedgerConfig::default();

    let mut report = ExtractReport {
        wallets_scanned: wallets.len(),
        ..ExtractReport::default()
    };
    let mut batch: Vec<WalletFeatures> = Vec::new();

    for hex in &wallets {
        // All trades for this wallet; keep the train window (≤ cutoff).
        let train: Vec<_> = cache
            .trades_for(hex)
            .into_iter()
            .filter(|t| t.timestamp.0.unix_timestamp() <= cutoff_unix)
            .collect();
        if train.is_empty() {
            report.wallets_skipped += 1;
            continue;
        }

        let snapshot = TradeSnapshot {
            trades: train,
            snapshot_at: snapshot_at.clone(),
            audit_window_days: 0,
        };
        // One wallet in → at most one ledger out.
        let Some(ledger) = build_trader_ledgers(&snapshot, &[], &ledger_config)
            .into_iter()
            .next()
        else {
            report.wallets_skipped += 1;
            continue;
        };

        let Some(features) = extract_features(
            &ledger,
            cutoff_unix,
            &event_map,
            &resolutions,
            min_closed_trades,
            min_distinct_events,
            bb_alpha,
            bb_beta,
        ) else {
            report.wallets_skipped += 1;
            continue;
        };

        let skill = sign_randomization_test(&ledger.closed_trades, &event_map, permutations, seed);
        batch.push(WalletFeatures {
            features,
            extracted_at_unix,
            skill_pnl_usd: skill.observed_pnl,
            skill_pvalue_bps: skill.pvalue_bps,
            skill_permutations: skill.permutations,
        });
    }
    report.wallets_written = batch.len();

    // Write pass: skill cache, read-write (single transaction).
    let mut skill_cache = SkillCache::open(cache_path)?;
    skill_cache.upsert_features_batch(&batch)?;

    info!(
        wallets_scanned = report.wallets_scanned,
        wallets_written = report.wallets_written,
        wallets_skipped = report.wallets_skipped,
        cutoff_unix,
        "skill-select extract: complete"
    );
    Ok(report)
}
