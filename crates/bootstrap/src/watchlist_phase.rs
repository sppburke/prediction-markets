//! Watchlist-build phase — `pe-bootstrap watchlist`.
//!
//! Reconstructs per-wallet `TraderLedger`s from the SQLite trade cache, applies
//! the post-filter, builds the seed `Watchlist`, optionally persists a
//! `leaderboard_snapshots` row-set, and writes `watchlist.json`.
//!
//! Standalone contract: this phase reads whatever trades are currently in `cache`
//! — it does **not** re-fetch trades, re-enumerate wallets, or run the funder-graph
//! pass. Operators invoke `pe-bootstrap watchlist` when they want to recompute the
//! seed watchlist from the cache's current state (e.g., after a manual cache edit,
//! after `pe-bootstrap backfill` completes, or when debugging filter changes).

use std::path::{Path, PathBuf};

use pe_core_types::{BasisPoints, SourceTimestamp, WalletAddress};
use pe_trader_index::{
    TraderLedger, Watchlist, WatchlistEntry, WatchlistTier, build_trader_ledgers,
};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::filter::{FilterConfig, passes_filter, win_rate_bps};

/// Result of the watchlist-build phase.
#[derive(Debug, Clone)]
pub struct WatchlistReport {
    /// Number of wallets with at least one trade that had a ledger reconstructed.
    pub ledger_count: usize,
    /// Total trades across all reconstructed ledgers.
    pub total_trades: usize,
    /// Wallets that passed the post-filter and are in the `Active` tier.
    pub active_count: usize,
    /// Wallets assigned to the `Incubator` tier (always 0 in bootstrap output).
    pub incubator_count: usize,
    /// True when a `leaderboard_snapshots` row-set was persisted.
    pub snapshot_written: bool,
    /// Path where `watchlist.json` was written.
    pub output_path: PathBuf,
}

/// Build the seed watchlist from cached trades and write `watchlist.json`.
///
/// When `dump_ledgers_path` is `Some(path)`, the intermediate `Vec<TraderLedger>`
/// is serialised to JSON at that path before filtering (useful for debugging).
///
/// Honours `config.write_snapshot`: when `true`, persists a `leaderboard_snapshots`
/// row-set; when `false`, only writes `watchlist.json`.
///
/// # Precondition
/// `wallets` should be the set that had trades fetched (typically obtained via
/// `cache.wallets_with_source_bit(SRC_LEADERBOARD)` after winner-discovery and
/// `run_fetch` complete). An empty `wallets` slice produces an empty watchlist.
pub async fn run_watchlist(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    wallets: &[WalletAddress],
    dump_ledgers_path: Option<&Path>,
) -> Result<WatchlistReport, BootstrapError> {
    let snapshot_at = SourceTimestamp(OffsetDateTime::now_utc());
    let audit_window_days = config.audit_window_days.unwrap_or(u32::MAX);
    let mut ledgers: Vec<TraderLedger> = Vec::with_capacity(wallets.len());
    let mut total_trades: usize = 0;

    for wallet in wallets {
        let trades = cache.trades_for(&wallet.to_string());
        if trades.is_empty() {
            continue;
        }
        total_trades += trades.len();
        ledgers.extend(build_trader_ledgers(&trades, audit_window_days, None));
    }
    let ledger_count = ledgers.len();
    tracing::info!(
        ledgers = ledger_count,
        trades = total_trades,
        "watchlist: reconstructed ledgers"
    );

    if let Some(path) = dump_ledgers_path {
        let json = serde_json::to_vec_pretty(&ledgers)?;
        std::fs::write(path, &json)?;
        tracing::info!(path = %path.display(), "watchlist: ledgers dumped");
    }

    let filter = FilterConfig {
        min_closed_trades: config.min_closed_trades,
        min_win_rate_pct: config.min_win_rate_pct,
        active_window_days: config.post_filter_active_window_days,
        max_avg_hours_to_resolution: config.post_filter_max_avg_hours_to_resolution,
    };
    let snapshot_at_for_db = snapshot_at.clone();
    let watchlist = build_seed_watchlist(ledgers, snapshot_at, &filter);
    tracing::info!(
        active = watchlist.active_count,
        incubator = watchlist.incubator_count,
        "watchlist: built"
    );

    let snapshot_wallets: Vec<WalletAddress> = watchlist.entries.iter().map(|e| e.wallet).collect();
    let snapshot_written = if config.write_snapshot {
        cache.insert_snapshot(snapshot_at_for_db.0.unix_timestamp(), &snapshot_wallets)?;
        tracing::info!(
            snapshot_at = snapshot_at_for_db.0.unix_timestamp(),
            wallets = snapshot_wallets.len(),
            "watchlist: leaderboard snapshot persisted"
        );
        true
    } else {
        tracing::info!(
            snapshot_at = snapshot_at_for_db.0.unix_timestamp(),
            wallets = snapshot_wallets.len(),
            "watchlist: leaderboard snapshot write skipped \
             (set PE_BOOTSTRAP_WRITE_SNAPSHOT=true to enable)"
        );
        false
    };

    write_watchlist(&watchlist, &config.output_path)?;

    Ok(WatchlistReport {
        ledger_count,
        total_trades,
        active_count: watchlist.active_count,
        incubator_count: watchlist.incubator_count,
        snapshot_written,
        output_path: config.output_path.clone(),
    })
}

/// Build a seed [`Watchlist`] from reconstructed ledgers using the bootstrap post-filter.
///
/// Uses win-rate basis points as the score (no historical LCB_5pct available at bootstrap).
/// All passing wallets are assigned `Active` tier.
pub fn build_seed_watchlist(
    ledgers: Vec<TraderLedger>,
    snapshot_at: SourceTimestamp,
    filter: &FilterConfig,
) -> Watchlist {
    let snapshot_at_unix = snapshot_at.0.unix_timestamp();
    let mut entries: Vec<WatchlistEntry> = Vec::new();

    for ledger in &ledgers {
        if !passes_filter(ledger, snapshot_at_unix, filter) {
            continue;
        }

        let total = ledger.closed_trades.len();
        let wins = ledger
            .closed_trades
            .iter()
            .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
            .count();

        let win_rate = BasisPoints(win_rate_bps(wins, total));
        entries.push(WatchlistEntry {
            wallet: ledger.wallet,
            tier: WatchlistTier::Active,
            leader_score_bps: win_rate,
            lcb_5pct_bps: BasisPoints(0),
            win_rate_bps: win_rate,
            closed_trades_in_window: u32::try_from(total).unwrap_or(u32::MAX),
            reconstruction_quality: ledger.reconstruction_quality,
        });
    }

    entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));

    let active_count = entries.len();
    Watchlist {
        entries,
        snapshot_at,
        active_count,
        incubator_count: 0,
    }
}

pub(crate) fn write_watchlist(
    watchlist: &Watchlist,
    path: &std::path::Path,
) -> Result<(), BootstrapError> {
    let json = serde_json::to_vec_pretty(watchlist)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
