//! Scenario tests for the watched-count publish (dashboard "N watched" — issue #343 PR4
//! follow-up).
//!
//! Drives [`refresh_and_publish`] with an in-memory [`FakePublisher`] (no live network,
//! deterministic failure injection) over a real [`LiveWatchlist`]. Proves the number the
//! site shows equals the service's accumulated live-set size, and that a publish failure is
//! swallowed so it can never break the copy-path refresh.
//!
//! Scenarios:
//!   AC-PUBLISH    — after a refresh, the published size == the live-set size.
//!   AC-ACCUMULATE — additive refreshes publish the growing accumulated size (never-evict).
//!   AC-BESTEFFORT — a failing publish does not change the returned live-set size.
//!
//! Run with: cargo nextest run -p pe-service --features scenario
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Mutex;

use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::supabase_reader::{SUPABASE_LIVE_CAP, SupabaseError};
use pe_service::supabase_refresh::{WatchlistSizePublisher, refresh_and_publish};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use time::OffsetDateTime;

/// In-memory [`WatchlistSizePublisher`]: records every published size; optionally fails every
/// publish to exercise the best-effort path. Deterministic.
#[derive(Default)]
struct FakePublisher {
    published: Mutex<Vec<usize>>,
    fail: bool,
}

impl WatchlistSizePublisher for FakePublisher {
    async fn publish(&self, size: usize) -> Result<(), SupabaseError> {
        if self.fail {
            return Err(SupabaseError::Status(503));
        }
        self.published.lock().unwrap().push(size);
        Ok(())
    }
}

/// A unique, valid wallet address from a byte seed (`0x` + 40 hex).
fn entry(seed: u8) -> WatchlistEntry {
    WatchlistEntry {
        wallet: WalletAddress::from_hex(&format!("0x{seed:040x}")).unwrap(),
        tier: WatchlistTier::Active,
        leader_score_bps: BasisPoints(0),
        lcb_5pct_bps: BasisPoints(0),
        win_rate_bps: BasisPoints(0),
        closed_trades_in_window: 0,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
    }
}

/// Build a deterministic [`Watchlist`] (frozen epoch timestamp; all tier `Active`).
fn watchlist(entries: Vec<WatchlistEntry>) -> Watchlist {
    let total = entries.len();
    Watchlist {
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: total,
        incubator_count: 0,
    }
}

#[tokio::test]
async fn ac_publish_matches_live_set_size() {
    let live = LiveWatchlist::new(watchlist(vec![]));
    let publisher = FakePublisher::default();

    let n = refresh_and_publish(
        &live,
        &watchlist(vec![entry(1), entry(2), entry(3)]),
        SUPABASE_LIVE_CAP,
        &publisher,
    )
    .await;

    // PASS: the live set has 3 wallets and exactly 3 was published — the dashboard "watched"
    //       number is the service's live-set size, not the (larger) ranked universe.
    assert_eq!(n, 3);
    assert_eq!(*publisher.published.lock().unwrap(), vec![3]);
    println!("PASS: AC-PUBLISH — live set = 3, published [3]");
}

#[tokio::test]
async fn ac_accumulate_publishes_growing_size() {
    let live = LiveWatchlist::new(watchlist(vec![]));
    let publisher = FakePublisher::default();

    let n1 = refresh_and_publish(
        &live,
        &watchlist(vec![entry(1), entry(2), entry(3)]),
        SUPABASE_LIVE_CAP,
        &publisher,
    )
    .await;
    // A later refresh brings two *new* wallets; the live set is additive (never-evict).
    let n2 = refresh_and_publish(
        &live,
        &watchlist(vec![entry(4), entry(5)]),
        SUPABASE_LIVE_CAP,
        &publisher,
    )
    .await;

    // PASS: the accumulated live set grows 3 → 5 and each refresh publishes the new size, so
    //       the dashboard reflects the real followed set even as it accumulates.
    assert_eq!((n1, n2), (3, 5));
    assert_eq!(*publisher.published.lock().unwrap(), vec![3, 5]);
    println!("PASS: AC-ACCUMULATE — live set 3→5, published [3, 5]");
}

#[tokio::test]
async fn ac_best_effort_publish_failure_does_not_change_refresh() {
    let live = LiveWatchlist::new(watchlist(vec![]));
    let publisher = FakePublisher {
        fail: true,
        ..Default::default()
    };

    let n = refresh_and_publish(
        &live,
        &watchlist(vec![entry(1), entry(2)]),
        SUPABASE_LIVE_CAP,
        &publisher,
    )
    .await;

    // PASS: the publish errored (503) yet the refresh still returns the live-set size and
    //       nothing was recorded — the analytics write never affects the copy path.
    assert_eq!(n, 2);
    assert!(publisher.published.lock().unwrap().is_empty());
    println!("PASS: AC-BESTEFFORT — publish failed, refresh still returned live_total=2");
}
