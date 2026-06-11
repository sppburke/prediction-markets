//! Walk-forward per-wallet ranker.
//!
//! [`build_watchlist`] takes a slice of [`TraderLedger`]s (one per wallet),
//! scores each using the LCB_5pct signal, applies eligibility gates, and
//! returns a size-capped [`Watchlist`].

use std::collections::HashMap;

use pe_core_types::{SourceTimestamp, WalletAddress};

use crate::{
    config::RankerConfig,
    ledger::{ClosedTrade, TraderLedger},
    score::compute_stats,
    watchlist::{Watchlist, WatchlistEntry, WatchlistTier},
};

/// Build a ranked [`Watchlist`] from a set of reconstructed ledgers.
///
/// `snapshot_at` is the timestamp of the underlying trade snapshot; it anchors the
/// look-back window (`now_unix = snapshot_at.0.unix_timestamp()`).
///
/// Pure and deterministic: same inputs always produce the same watchlist.
pub fn build_watchlist(
    ledgers: &[TraderLedger],
    snapshot_at: SourceTimestamp,
    config: &RankerConfig,
) -> Watchlist {
    let now_unix = snapshot_at.0.unix_timestamp();
    let active_window_start = now_unix - (config.active_window_days as i64) * 86_400;
    let incubator_window_start = now_unix - (config.incubator_window_days as i64) * 86_400;

    // Group ledgers by wallet (reconstruction emits one ledger per wallet).
    let groups = group_by_wallet(ledgers);

    let mut active_entries: Vec<WatchlistEntry> = Vec::new();
    let mut incubator_entries: Vec<WatchlistEntry> = Vec::new();

    for (wallet, group_ledgers) in &groups {
        let min_quality = group_ledgers
            .iter()
            .map(|l| l.reconstruction_quality.get())
            .min()
            .unwrap_or(0);

        if min_quality < config.min_reconstruction_quality {
            continue;
        }

        // Merge all closed trades from all ledgers in the group.
        let all_trades: Vec<&ClosedTrade> = group_ledgers
            .iter()
            .flat_map(|l| l.closed_trades.iter())
            .collect();
        let owned: Vec<ClosedTrade> = all_trades.iter().map(|t| (*t).clone()).collect();

        // Attempt active tier first.
        if let Some(stats) = compute_stats(&owned, active_window_start, now_unix)
            && stats.closed_trades_in_window >= config.active_min_closed_trades
            && stats.distinct_markets_in_window >= config.active_min_distinct_markets
            && stats.lcb_5pct_bps.0 > 0
        {
            let reconstruction_quality =
                match pe_core_types::ReconstructionQuality::new(min_quality) {
                    Ok(q) => q,
                    Err(_) => group_ledgers[0].reconstruction_quality,
                };
            active_entries.push(WatchlistEntry {
                wallet: *wallet,
                tier: WatchlistTier::Active,
                leader_score_bps: stats.leader_score_bps,
                lcb_5pct_bps: stats.lcb_5pct_bps,
                win_rate_bps: stats.win_rate_bps,
                closed_trades_in_window: stats.closed_trades_in_window,
                reconstruction_quality,
            });
            continue;
        }

        // Fall back to incubator tier.
        // Intentionally no `lcb_5pct_bps > 0` check: incubator is an observation pool for
        // research, not a copy-trading list. LCB sign is not an eligibility gate here per
        // _GLOSSARY.md "Ranker eligibility thresholds" (active tier carries the LCB > 0 gate).
        if let Some(stats) = compute_stats(&owned, incubator_window_start, now_unix)
            && stats.closed_trades_in_window >= config.incubator_min_closed_trades
            && stats.distinct_markets_in_window >= config.incubator_min_distinct_markets
        {
            let reconstruction_quality =
                match pe_core_types::ReconstructionQuality::new(min_quality) {
                    Ok(q) => q,
                    Err(_) => group_ledgers[0].reconstruction_quality,
                };
            incubator_entries.push(WatchlistEntry {
                wallet: *wallet,
                tier: WatchlistTier::Incubator,
                leader_score_bps: stats.leader_score_bps,
                lcb_5pct_bps: stats.lcb_5pct_bps,
                win_rate_bps: stats.win_rate_bps,
                closed_trades_in_window: stats.closed_trades_in_window,
                reconstruction_quality,
            });
        }
    }

    // Sort each tier descending by leader_score_bps, then cap.
    active_entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));
    active_entries.truncate(config.active_watchlist_size);

    incubator_entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));
    incubator_entries.truncate(config.incubator_watchlist_size);

    let active_count = active_entries.len();
    let incubator_count = incubator_entries.len();

    let mut entries = active_entries;
    entries.extend(incubator_entries);

    Watchlist {
        entries,
        snapshot_at,
        active_count,
        incubator_count,
    }
}

/// Group ledgers by wallet. Reconstruction emits one ledger per wallet, so each
/// group is a singleton; the grouping is retained to keep the scoring loop's
/// quality-merge structure uniform.
fn group_by_wallet(ledgers: &[TraderLedger]) -> HashMap<WalletAddress, Vec<&TraderLedger>> {
    let mut groups: HashMap<WalletAddress, Vec<&TraderLedger>> = HashMap::new();
    for l in ledgers {
        groups.entry(l.wallet).or_default().push(l);
    }
    groups
}
