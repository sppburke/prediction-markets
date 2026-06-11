//! `export-watchlist` subcommand — converts a Python `.txt` watchlist of wallet
//! hex addresses into a `pe_trader_index::Watchlist` JSON file consumable by
//! `pe-service` via `seed_watchlist_path`.
//!
//! Schema and field semantics for the output JSON are documented in
//! `docs/_GLOSSARY.md` under "export-watchlist output schema". Operational
//! notes (refresh cadence, file-path conventions) are in
//! `docs/19-WINNER-FOLLOW-STRATEGY.md`.

use std::collections::HashMap;
use std::path::PathBuf;

use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use time::OffsetDateTime;

use crate::{SkillCache, SkillSelectError, WalletFeatures};

/// Config for the `export-watchlist` subcommand. Constructed in `main.rs` by
/// projecting the relevant [`crate::SkillConfig`] fields.
pub struct ExportWatchlistConfig {
    /// Path to `wallet_cache.db` (read-only). `PE_SKILL_CACHE_PATH`.
    pub cache_path: PathBuf,
    /// Input `.txt` watchlist (one `0x`-hex per line). `PE_SKILL_EXPORT_WATCHLIST_INPUT_PATH`.
    pub watchlist_txt_path: PathBuf,
    /// Cutoff unix timestamp used to select the `wallet_features` row set.
    /// `PE_SKILL_CUTOFF_UNIX`.
    pub cutoff_unix: i64,
    /// Output JSON file path (written atomically). `PE_SKILL_EXPORT_WATCHLIST_OUTPUT_PATH`.
    pub output_path: PathBuf,
}

/// Summary returned by [`run_export_watchlist`].
#[derive(Debug)]
pub struct ExportStats {
    pub wallets_requested: usize,
    pub wallets_written: usize,
    pub wallets_missing: usize,
    pub active_count: usize,
    pub incubator_count: usize,
}

/// Read a `.txt` watchlist, look up features at `cutoff_unix`, and emit a
/// `pe_trader_index::Watchlist` JSON file at `cfg.output_path`.
///
/// # Precondition
/// `cfg.cache_path` must point to an existing `wallet_cache.db` that was
/// populated via `pe-skill-select extract` at `cfg.cutoff_unix`.
pub fn run_export_watchlist(cfg: &ExportWatchlistConfig) -> Result<ExportStats, SkillSelectError> {
    // 1. Read .txt; skip blank lines and '#'-prefixed comment lines; validate
    //    remaining lines as wallet hex addresses (Io error on read failure,
    //    Decode error on malformed hex).
    let txt = std::fs::read_to_string(&cfg.watchlist_txt_path)?;
    let mut parsed: Vec<(WalletAddress, String)> = Vec::new();
    for line in txt.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let normalised = trimmed.to_lowercase();
        let addr = WalletAddress::from_hex(&normalised)
            .map_err(|_| SkillSelectError::Decode(format!("watchlist hex parse: {trimmed}")))?;
        parsed.push((addr, normalised));
    }
    let wallets_requested = parsed.len();

    // 2. Load wallet_features at cutoff_unix.
    let cache = SkillCache::open_read_only(&cfg.cache_path)?;
    let features: Vec<WalletFeatures> = cache.load_features_for_cutoff(cfg.cutoff_unix)?;
    let features_map: HashMap<&str, &WalletFeatures> = features
        .iter()
        .map(|wf| (wf.features.wallet_hex.as_str(), wf))
        .collect();

    // 3. Build snapshot timestamp from cutoff_unix.
    let snapshot_at = SourceTimestamp(
        OffsetDateTime::from_unix_timestamp(cfg.cutoff_unix).map_err(|e| {
            SkillSelectError::WatchlistMapping(format!("snapshot_at unix={}: {e}", cfg.cutoff_unix))
        })?,
    );

    // 4. Map each hex to a WatchlistEntry; warn and skip on missing wallets.
    let mut entries: Vec<WatchlistEntry> = Vec::new();
    let mut wallets_missing = 0usize;

    for (wallet, hex) in parsed {
        match features_map.get(hex.as_str()) {
            Some(wf) => {
                let rq = ReconstructionQuality::new(wf.features.reconstruction_quality).map_err(
                    |e| {
                        SkillSelectError::WatchlistMapping(format!(
                            "reconstruction_quality {}: {e}",
                            wf.features.reconstruction_quality
                        ))
                    },
                )?;
                entries.push(WatchlistEntry {
                    wallet,
                    tier: WatchlistTier::Active,
                    leader_score_bps: BasisPoints(wf.features.lcb_5pct_bps),
                    lcb_5pct_bps: BasisPoints(wf.features.lcb_5pct_bps),
                    win_rate_bps: BasisPoints(wf.features.win_rate_bps),
                    // all-time count from extraction; closest available proxy for the
                    // live ranker's eligibility-window count
                    closed_trades_in_window: wf.features.closed_trades,
                    reconstruction_quality: rq,
                });
            }
            None => {
                tracing::warn!(hex = %hex, "export-watchlist: wallet not in features at cutoff; skipping");
                wallets_missing += 1;
            }
        }
    }

    // 5. Sort descending by leader_score_bps (highest-scored first).
    entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));

    let active_count = entries.len();
    let wallets_written = active_count;

    // 6. Serialize and atomic-write (tmp + rename matches bootstrap precedent).
    let watchlist = Watchlist {
        entries,
        snapshot_at,
        active_count,
        incubator_count: 0,
    };
    let json = serde_json::to_string_pretty(&watchlist)
        .map_err(|e| SkillSelectError::WatchlistMapping(format!("serde_json: {e}")))?;
    let tmp_path = cfg.output_path.with_extension("json.tmp");
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, &cfg.output_path)?;

    Ok(ExportStats {
        wallets_requested,
        wallets_written,
        wallets_missing,
        active_count,
        incubator_count: 0,
    })
}
