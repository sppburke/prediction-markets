//! Lock-free, hot-swappable live watchlist (issue #339).
//!
//! Wraps a [`Watchlist`] in an [`ArcSwap`] so the trade-handling hot path reads a
//! cheap, consistent snapshot (one atomic load) while a single background refresh
//! task swaps in fresh rankings from Supabase. Cloning a [`LiveWatchlist`] shares the
//! same underlying cell, so every consumer (orchestrator, trade poller, refresh loop)
//! observes the same swaps.
//!
//! [`Self::apply_refresh`] is *additive*: it updates the scores of wallets already
//! present, appends new wallets up to `live_cap`, and never evicts. Eviction /
//! demotion is deferred to the online-policy work (issue #3 / copy-trade knockout).

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pe_core_types::WalletAddress;
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};

/// Hot-swappable handle to the current [`Watchlist`].
///
/// Clones share one underlying cell. Reads ([`Self::snapshot`]) are wait-free; the
/// single-writer [`Self::apply_refresh`] performs a load-modify-store and must not be
/// called concurrently with itself (one refresh task owns it).
#[derive(Clone)]
pub struct LiveWatchlist {
    inner: Arc<ArcSwap<Watchlist>>,
}

impl LiveWatchlist {
    /// Build a live watchlist seeded with `initial`.
    pub fn new(initial: Watchlist) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Cheap, consistent snapshot of the current watchlist (one atomic load).
    ///
    /// Callers in the hot path should take exactly one snapshot per event and read all
    /// fields from it, so every lookup within one event sees the same generation.
    pub fn snapshot(&self) -> Arc<Watchlist> {
        self.inner.load_full()
    }

    /// Additively merge `fresh` into the live set and atomically publish the result;
    /// returns the new total entry count.
    ///
    /// Semantics (all four are unit-tested):
    /// - **update-scores**: a wallet present in both is replaced by its `fresh` entry.
    /// - **backfill-up-to-cap**: a wallet only in `fresh` is appended while
    ///   `len < live_cap`.
    /// - **never-exceed-cap**: once `len == live_cap`, further new wallets are dropped.
    /// - **never-remove**: a wallet absent from `fresh` is retained unchanged.
    ///
    /// # Precondition
    /// Single-writer: must not be called concurrently with itself. One refresh task owns
    /// the write side; the hot path only reads via [`Self::snapshot`].
    pub fn apply_refresh(&self, fresh: &Watchlist, live_cap: usize) -> usize {
        let current = self.inner.load_full();
        let mut entries: Vec<WatchlistEntry> = current.entries.clone();
        let mut index: HashMap<WalletAddress, usize> = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.wallet, i))
            .collect();

        for fe in &fresh.entries {
            match index.get(&fe.wallet) {
                Some(&i) => entries[i] = fe.clone(), // update scores; never remove
                None => {
                    if entries.len() < live_cap {
                        index.insert(fe.wallet, entries.len());
                        entries.push(fe.clone());
                    }
                    // else: at cap — drop the new wallet (never-evict; #3 adds eviction).
                }
            }
        }

        // Maintain the `Watchlist` invariant: entries sorted descending by score.
        entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));
        let active_count = entries
            .iter()
            .filter(|e| e.tier == WatchlistTier::Active)
            .count();
        let total = entries.len();
        let updated = Watchlist {
            entries,
            snapshot_at: fresh.snapshot_at.clone(),
            active_count,
            incubator_count: total - active_count,
        };
        self.inner.store(Arc::new(updated));
        total
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};

    fn wallet(n: u8) -> WalletAddress {
        let mut bytes = [0u8; 20];
        bytes[19] = n;
        WalletAddress(bytes)
    }

    fn entry(w: WalletAddress, score_bps: i32, win_rate_bps: i32) -> WatchlistEntry {
        WatchlistEntry {
            wallet: w,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(score_bps),
            lcb_5pct_bps: BasisPoints(0),
            win_rate_bps: BasisPoints(win_rate_bps),
            closed_trades_in_window: 0,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }
    }

    fn watchlist(entries: Vec<WatchlistEntry>) -> Watchlist {
        let active_count = entries
            .iter()
            .filter(|e| e.tier == WatchlistTier::Active)
            .count();
        let total = entries.len();
        Watchlist {
            entries,
            snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            active_count,
            incubator_count: total - active_count,
        }
    }

    fn find_win_rate(wl: &Watchlist, w: WalletAddress) -> Option<i32> {
        wl.entries
            .iter()
            .find(|e| e.wallet == w)
            .map(|e| e.win_rate_bps.0)
    }

    #[test]
    fn snapshot_returns_seeded_entries() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let snap = live.snapshot();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(find_win_rate(&snap, wallet(1)), Some(5000));
    }

    #[test]
    fn apply_refresh_updates_scores_in_place() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let n = live.apply_refresh(&watchlist(vec![entry(wallet(1), 200, 6300)]), 10);
        assert_eq!(n, 1, "no new wallet added");
        let snap = live.snapshot();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(
            find_win_rate(&snap, wallet(1)),
            Some(6300),
            "win_rate updated to fresh value"
        );
    }

    #[test]
    fn apply_refresh_backfills_up_to_cap() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let n = live.apply_refresh(
            &watchlist(vec![entry(wallet(2), 90, 4000), entry(wallet(3), 80, 3000)]),
            3,
        );
        assert_eq!(n, 3, "both new wallets appended within cap");
        let snap = live.snapshot();
        for w in [wallet(1), wallet(2), wallet(3)] {
            assert!(snap.entries.iter().any(|e| e.wallet == w));
        }
    }

    #[test]
    fn apply_refresh_never_exceeds_cap() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
        ]));
        let n = live.apply_refresh(&watchlist(vec![entry(wallet(3), 80, 3000)]), 2);
        assert_eq!(n, 2, "new wallet dropped at cap");
        let snap = live.snapshot();
        assert!(
            !snap.entries.iter().any(|e| e.wallet == wallet(3)),
            "wallet 3 not admitted past cap"
        );
        assert!(snap.entries.iter().any(|e| e.wallet == wallet(1)));
        assert!(snap.entries.iter().any(|e| e.wallet == wallet(2)));
    }

    #[test]
    fn apply_refresh_never_removes_absent_wallet() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
        ]));
        // fresh omits wallet 2.
        let n = live.apply_refresh(&watchlist(vec![entry(wallet(1), 110, 5500)]), 10);
        assert_eq!(n, 2, "absent wallet retained");
        let snap = live.snapshot();
        assert_eq!(
            find_win_rate(&snap, wallet(1)),
            Some(5500),
            "present wallet updated"
        );
        assert_eq!(
            find_win_rate(&snap, wallet(2)),
            Some(4000),
            "absent wallet unchanged"
        );
    }
}
