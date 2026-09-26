//! Lock-free, hot-swappable live watchlist (issue #339).
//!
//! Wraps a [`Watchlist`] in an [`ArcSwap`] so the trade-handling hot path reads a
//! cheap, consistent snapshot (one atomic load) while a single background refresh
//! task swaps in fresh rankings from Supabase. Cloning a [`LiveWatchlist`] shares the
//! same underlying cell, so every consumer (orchestrator, trade poller, refresh loop)
//! observes the same swaps.
//!
//! [`Self::apply_refresh`] is *score-update-only* (issue #350 WS1): it refreshes the scores
//! of wallets already present and never adds or evicts — the live set is a fixed maintained
//! working set whose membership is changed only by [`Self::replace`].
//! [`Self::replace`] is the eviction+backfill primitive (issue #350 WS1): it atomically
//! drops a set of wallets and backfills replacements up to a cap. It is driven by the
//! maintenance tick in [`crate::watchlist_maintenance`] (issue #350 WS1 PR-D).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use pe_core_types::WalletAddress;
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use tokio::sync::watch;

/// Bounded latest-only dirty signal for the sole Supabase watchlist projection worker (#544).
/// Each mutation advances a process-local counter; a slow worker observes only the newest value.
#[derive(Clone)]
pub struct ProjectionDirty {
    tx: watch::Sender<u64>,
}

impl ProjectionDirty {
    pub fn mark(&self) {
        self.tx.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

pub fn projection_dirty_channel() -> (ProjectionDirty, watch::Receiver<u64>) {
    let (tx, rx) = watch::channel(0);
    (ProjectionDirty { tx }, rx)
}

/// Apply one structural eviction/backfill operation without publishing it.
pub(crate) fn replace_entries(
    current: &[WatchlistEntry],
    removed: &HashSet<WalletAddress>,
    replacements: &[WatchlistEntry],
    cap: usize,
) -> Vec<WatchlistEntry> {
    let mut entries = current
        .iter()
        .filter(|entry| !removed.contains(&entry.wallet))
        .cloned()
        .collect::<Vec<_>>();
    let mut present = entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();

    for replacement in replacements {
        if entries.len() >= cap {
            break;
        }
        if !removed.contains(&replacement.wallet) && present.insert(replacement.wallet) {
            entries.push(replacement.clone());
        }
    }

    entries.sort_by_key(|entry| std::cmp::Reverse(entry.leader_score_bps.0));
    entries.truncate(cap);
    entries
}

/// Hot-swappable handle to the current [`Watchlist`].
///
/// Clones share one underlying cell. Reads ([`Self::snapshot`]) are wait-free; the
/// single-writer mutators ([`Self::apply_refresh`] and [`Self::replace`]) each perform a
/// load-modify-store and must not run concurrently with one another or themselves — the
/// service serializes them with a shared writer mutex (see [`crate::watchlist_maintenance`]).
#[derive(Clone)]
pub struct LiveWatchlist {
    inner: Arc<ArcSwap<Watchlist>>,
    structural: Arc<RwLock<HashSet<WalletAddress>>>,
    projection_dirty: Option<ProjectionDirty>,
}

impl LiveWatchlist {
    /// Build a live watchlist seeded with `initial`.
    pub fn new(initial: Watchlist) -> Self {
        let structural = initial.entries.iter().map(|entry| entry.wallet).collect();
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
            structural: Arc::new(RwLock::new(structural)),
            projection_dirty: None,
        }
    }

    /// Production constructor that attaches the sole bounded projection signal.
    pub fn new_with_projection(initial: Watchlist, projection_dirty: ProjectionDirty) -> Self {
        let structural = initial.entries.iter().map(|entry| entry.wallet).collect();
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
            structural: Arc::new(RwLock::new(structural)),
            projection_dirty: Some(projection_dirty),
        }
    }

    /// Boot seeds this before fence and bracket filtering. The orchestrator is the runtime
    /// writer, after its synchronized membership append succeeds.
    pub fn structural_membership(&self) -> HashSet<WalletAddress> {
        self.structural
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn commit_structural_change(
        &self,
        removed: &[WalletAddress],
        added: &[WalletAddress],
    ) {
        let mut current = self
            .structural
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for wallet in removed {
            current.remove(wallet);
        }
        current.extend(added.iter().copied());
    }

    /// Mirror a successful paper append in an external scenario publisher.
    #[cfg(feature = "scenario")]
    pub fn scenario_commit_structural_change(
        &self,
        removed: &[WalletAddress],
        added: &[WalletAddress],
    ) {
        self.commit_structural_change(removed, added);
    }

    /// Cheap, consistent snapshot of the current watchlist (one atomic load).
    ///
    /// Callers in the hot path should take exactly one snapshot per event and read all
    /// fields from it, so every lookup within one event sees the same generation.
    pub fn snapshot(&self) -> Arc<Watchlist> {
        self.inner.load_full()
    }

    /// Refresh the scores of wallets already in the live set from `fresh`, atomically publish
    /// the result, and return the (unchanged) entry count.
    ///
    /// Score-update-only (issue #350 WS1): the live set is a fixed maintained working set, so
    /// a refresh **never adds** a wallet present only in `fresh` and **never removes** a wallet
    /// absent from it — membership is changed solely by [`Self::replace`] (the maintenance
    /// tick). Both behaviours are unit-tested.
    ///
    /// # Precondition
    /// Single-writer: must not be called concurrently with itself or [`Self::replace`]. One
    /// refresh task owns the write side; the hot path only reads via [`Self::snapshot`].
    pub fn apply_refresh(&self, fresh: &Watchlist) -> usize {
        let current = self.inner.load_full();
        let mut entries: Vec<WatchlistEntry> = current.entries.clone();
        let index: HashMap<WalletAddress, usize> = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.wallet, i))
            .collect();

        for fe in &fresh.entries {
            if let Some(&i) = index.get(&fe.wallet) {
                entries[i] = fe.clone(); // update scores in place; never add, never remove
            }
            // else: a wallet only in `fresh` is ignored — membership changes only via `replace`.
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
        self.mark_projection_dirty();
        total
    }

    /// Atomically evict the `removed` wallets and backfill from `replacements`, holding
    /// the result to at most `cap` entries, then publish; returns the new total count.
    ///
    /// This is the eviction+backfill primitive for the maintenance tick (issue #350 WS1).
    /// The caller reads the latest ranking batch (bounded by `MAX_ACTIVE_WATCHLIST_SIZE`) and
    /// supplies it rank-sorted in `replacements`; `replace` trims them to the working set.
    ///
    /// Semantics (all unit-tested):
    /// - **evict**: every wallet in `removed` is dropped from the live set (evicting a
    ///   wallet that is not present is a no-op).
    /// - **backfill-in-order**: `replacements` are considered in the given order and
    ///   appended while `len < cap`.
    /// - **never-readmit**: a replacement whose wallet is in `removed` is skipped, so a
    ///   just-evicted wallet can never be re-admitted by the same swap.
    /// - **dedup**: a replacement already present among the survivors (or earlier in
    ///   `replacements`) is skipped; the existing entry is left unchanged — `replace`
    ///   is a structural swap, not a score refresh (that is [`Self::apply_refresh`]).
    /// - **cap**: once `len == cap`, further replacements are dropped.
    ///
    /// The whole operation is a single load-modify-store on the `ArcSwap`: readers see
    /// either the pre- or the post-swap generation, never an intermediate. `snapshot_at`
    /// is preserved from the current generation (eviction/backfill is not a re-rank).
    ///
    /// # Precondition
    /// Single-writer: must not run concurrently with itself or [`Self::apply_refresh`].
    /// The service serializes the maintenance tick and the refresh loop with a shared writer
    /// mutex (see [`crate::watchlist_maintenance`]).
    pub fn replace(
        &self,
        removed: &HashSet<WalletAddress>,
        replacements: &[WatchlistEntry],
        cap: usize,
    ) -> usize {
        let current = self.inner.load_full();
        let entries = replace_entries(&current.entries, removed, replacements, cap);
        let active_count = entries
            .iter()
            .filter(|e| e.tier == WatchlistTier::Active)
            .count();
        let total = entries.len();
        let updated = Watchlist {
            entries,
            snapshot_at: current.snapshot_at.clone(),
            active_count,
            incubator_count: total - active_count,
        };
        self.inner.store(Arc::new(updated));
        self.mark_projection_dirty();
        total
    }

    /// Remove a monotonic durable-fence set without admitting replacements.
    /// Caller holds the structural writer lock (#544).
    pub fn remove_fenced(&self, fenced: &HashSet<WalletAddress>) -> usize {
        if fenced.is_empty() {
            return self.inner.load().entries.len();
        }
        let current = self.inner.load_full();
        let entries: Vec<_> = current
            .entries
            .iter()
            .filter(|entry| !fenced.contains(&entry.wallet))
            .cloned()
            .collect();
        let active_count = entries
            .iter()
            .filter(|entry| entry.tier == WatchlistTier::Active)
            .count();
        let total = entries.len();
        self.inner.store(Arc::new(Watchlist {
            entries,
            snapshot_at: current.snapshot_at.clone(),
            active_count,
            incubator_count: total.saturating_sub(active_count),
        }));
        self.mark_projection_dirty();
        total
    }

    fn mark_projection_dirty(&self) {
        if let Some(dirty) = &self.projection_dirty {
            dirty.mark();
        }
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
    fn live_filter_does_not_change_replay_membership() {
        let first = wallet(1);
        let deferred = wallet(2);
        let live = LiveWatchlist::new(watchlist(vec![
            entry(first, 100, 5000),
            entry(deferred, 90, 5000),
        ]));
        live.remove_fenced(&HashSet::from([deferred]));
        assert_eq!(live.snapshot().entries.len(), 1);
        assert_eq!(
            live.structural_membership(),
            HashSet::from([first, deferred])
        );
        live.commit_structural_change(&[first], &[wallet(3)]);
        assert_eq!(
            live.structural_membership(),
            HashSet::from([deferred, wallet(3)])
        );
        assert_eq!(live.snapshot().entries.len(), 1);
    }

    #[test]
    fn apply_refresh_updates_scores_in_place() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let n = live.apply_refresh(&watchlist(vec![entry(wallet(1), 200, 6300)]));
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
    fn apply_refresh_ignores_new_wallets() {
        // Score-update-only: a wallet present only in `fresh` is NOT admitted — membership
        // changes solely via `replace` (the maintenance tick). Replaces the old additive
        // backfill/cap tests removed in #350 WS1.
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let n = live.apply_refresh(&watchlist(vec![
            entry(wallet(1), 110, 5500),
            entry(wallet(2), 90, 4000),
        ]));
        assert_eq!(
            n, 1,
            "new wallet not admitted by a score-update-only refresh"
        );
        let snap = live.snapshot();
        assert!(
            !snap.entries.iter().any(|e| e.wallet == wallet(2)),
            "wallet 2 absent — refresh never adds"
        );
        assert_eq!(
            find_win_rate(&snap, wallet(1)),
            Some(5500),
            "present wallet's score still updated in place"
        );
    }

    #[test]
    fn apply_refresh_never_removes_absent_wallet() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
        ]));
        // fresh omits wallet 2.
        let n = live.apply_refresh(&watchlist(vec![entry(wallet(1), 110, 5500)]));
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

    fn has(wl: &Watchlist, w: WalletAddress) -> bool {
        wl.entries.iter().any(|e| e.wallet == w)
    }

    #[test]
    fn replace_evicts_removed_and_backfills() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
        ]));
        let n = live.replace(
            &HashSet::from([wallet(2)]),
            &[entry(wallet(3), 80, 3000)],
            2,
        );
        assert_eq!(n, 2, "one evicted, one backfilled; set returns to cap");
        let snap = live.snapshot();
        assert!(!has(&snap, wallet(2)), "evicted wallet removed");
        assert!(has(&snap, wallet(1)), "survivor retained");
        assert!(has(&snap, wallet(3)), "replacement admitted");
    }

    #[test]
    fn replace_caps_backfill_in_slice_order() {
        // One survivor, cap 2: only the first replacement is admitted, regardless of
        // later candidates' scores (the caller supplies them rank-sorted).
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let n = live.replace(
            &HashSet::new(),
            &[entry(wallet(3), 50, 3000), entry(wallet(4), 99, 4000)],
            2,
        );
        assert_eq!(n, 2, "held at cap");
        let snap = live.snapshot();
        assert!(has(&snap, wallet(3)), "first candidate admitted");
        assert!(
            !has(&snap, wallet(4)),
            "later candidate dropped at cap even with a higher score"
        );
    }

    #[test]
    fn replace_never_readmits_evicted_wallet() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
        ]));
        // wallet 2 is both evicted and offered back as a replacement; it must not return.
        let n = live.replace(
            &HashSet::from([wallet(2)]),
            &[entry(wallet(2), 95, 9999), entry(wallet(3), 80, 3000)],
            3,
        );
        assert_eq!(
            n, 2,
            "evicted wallet not re-admitted; only wallet 3 backfilled"
        );
        let snap = live.snapshot();
        assert!(!has(&snap, wallet(2)), "evicted wallet stays out");
        assert!(has(&snap, wallet(3)), "other replacement admitted");
    }

    #[test]
    fn replace_dedups_replacement_already_present_unchanged() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
        ]));
        // wallet 2 is already present; offering it as a replacement must not overwrite it.
        let n = live.replace(
            &HashSet::new(),
            &[entry(wallet(2), 90, 9999), entry(wallet(3), 80, 3000)],
            5,
        );
        assert_eq!(n, 3, "duplicate skipped, wallet 3 added");
        let snap = live.snapshot();
        assert_eq!(
            find_win_rate(&snap, wallet(2)),
            Some(4000),
            "existing entry left unchanged (replace is structural, not a score refresh)"
        );
        assert!(has(&snap, wallet(3)));
    }

    #[test]
    fn replace_evict_only_shrinks_set() {
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
            entry(wallet(3), 80, 3000),
        ]));
        let n = live.replace(&HashSet::from([wallet(2)]), &[], 3);
        assert_eq!(n, 2, "no replacements offered; set shrinks below cap");
        let snap = live.snapshot();
        assert!(!has(&snap, wallet(2)));
        assert!(has(&snap, wallet(1)) && has(&snap, wallet(3)));
    }

    #[test]
    fn replace_evicting_absent_wallet_is_noop() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let n = live.replace(&HashSet::from([wallet(99)]), &[], 5);
        assert_eq!(n, 1, "evicting a non-member changes nothing");
        assert!(has(&live.snapshot(), wallet(1)));
    }

    #[test]
    fn replace_keeps_entries_sorted_descending() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 50, 5000)]));
        // Backfill higher- and lower-scored wallets; result must be sorted desc by score.
        live.replace(
            &HashSet::new(),
            &[entry(wallet(2), 90, 4000), entry(wallet(3), 70, 3000)],
            5,
        );
        let snap = live.snapshot();
        let scores: Vec<i32> = snap.entries.iter().map(|e| e.leader_score_bps.0).collect();
        assert_eq!(
            scores,
            vec![90, 70, 50],
            "entries sorted descending by score"
        );
    }

    #[test]
    fn replace_truncates_survivors_over_cap() {
        // Survivor set already exceeds cap with no backfill offered: the result is trimmed
        // to the highest-scored `cap` entries (the deploy-time over-accumulation case).
        let live = LiveWatchlist::new(watchlist(vec![
            entry(wallet(1), 100, 5000),
            entry(wallet(2), 90, 4000),
            entry(wallet(3), 80, 3000),
        ]));
        let n = live.replace(&HashSet::new(), &[], 2);
        assert_eq!(n, 2, "result trimmed down to cap");
        let snap = live.snapshot();
        assert!(
            has(&snap, wallet(1)) && has(&snap, wallet(2)),
            "top two by score kept"
        );
        assert!(
            !has(&snap, wallet(3)),
            "lowest-scored survivor dropped to enforce the cap"
        );
    }

    #[test]
    fn replace_preserves_snapshot_at() {
        let live = LiveWatchlist::new(watchlist(vec![entry(wallet(1), 100, 5000)]));
        let before = live.snapshot().snapshot_at.clone();
        live.replace(&HashSet::new(), &[entry(wallet(2), 90, 4000)], 5);
        assert_eq!(
            live.snapshot().snapshot_at,
            before,
            "snapshot_at preserved (eviction/backfill is not a re-rank)"
        );
    }
}
