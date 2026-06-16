//! Scenario tests for the watchlist maintenance tick (issue #350 WS1 PR-D).
//!
//! Drives the pure decision (`decide_evictions`) and the writer-locked apply
//! (`apply_evictions_and_backfill`) deterministically: fixed `now`, fixed cursors/stats,
//! tempfile-backed [`PaperStateDb`], no live network.
//!
//! Scenarios:
//!   backfilled-stale-history-wallet-immune-72h — a backfilled wallet whose real on-chain last
//!       trade is ancient is seeded to `now` at admission and is immune to inactivity eviction
//!       for a full 72h from admission.
//!   proven-winner-spared-under-72h            — a proven winner idle >72h (<7d) is spared.
//!   proven-winner-evicted-past-7d-cap         — a proven winner idle ≥7d is evicted (hard cap).
//!   negative-edge-evicted-at-72h              — a not-proven, not-demotable wallet idle ≥72h is
//!       evicted for inactivity.
//!   realized-pnl≥0-never-demoted              — realized P&L ≥ 0 is never demoted, even with a
//!       confidently-negative upper CB.
//!   writer-mutex-safety                       — concurrent refresh + replace serialized by the
//!       shared writer mutex never lose an update (evicted stay out, backfill stays in).
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_paper_state::PaperStateDb;
use pe_service::demotion_stat::WalletEdgeStats;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::watchlist_maintenance::{
    KnockoutReason, MaintenanceConfig, apply_evictions_and_backfill, decide_evictions,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::Mutex;

const NOW: i64 = 1_900_000_000;
const H72: i64 = 259_200;
const D7: i64 = 604_800;

fn cfg() -> MaintenanceConfig {
    MaintenanceConfig {
        interval_secs: 600,
        inactivity_threshold_secs: 259_200,
        inactivity_hard_cap_secs: 604_800,
        demotion_min_trades: 10,
        demotion_cb_alpha: dec!(0.10),
        bench_overfetch: 10,
        cap: 25,
    }
}

fn wallet(n: u8) -> WalletAddress {
    let mut b = [0u8; 20];
    b[0] = n;
    WalletAddress(b)
}

fn entry(w: WalletAddress, score_bps: i32) -> WatchlistEntry {
    WatchlistEntry {
        wallet: w,
        tier: WatchlistTier::Active,
        leader_score_bps: BasisPoints(score_bps),
        lcb_5pct_bps: BasisPoints(score_bps),
        win_rate_bps: BasisPoints(7_000),
        closed_trades_in_window: 0,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
    }
}

fn watchlist(entries: Vec<WatchlistEntry>) -> Watchlist {
    let active_count = entries.len();
    Watchlist {
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count,
        incubator_count: 0,
    }
}

fn stats(
    settled: usize,
    pnl: Decimal,
    lower: Option<Decimal>,
    upper: Option<Decimal>,
) -> WalletEdgeStats {
    WalletEdgeStats {
        settled_count: settled,
        realized_pnl: pnl,
        lower_cb: lower,
        upper_cb: upper,
    }
}

fn temp_db() -> (TempDir, Arc<PaperStateDb>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    (dir, db)
}

// ── proven-winner-spared-under-72h ──────────────────────────────────────────────
#[test]
fn proven_winner_spared_under_72h() {
    let w = wallet(1);
    let wl = watchlist(vec![entry(w, 100)]);
    let mut s = HashMap::new();
    s.insert(
        w.to_string(),
        stats(40, dec!(150), Some(dec!(0.05)), Some(dec!(0.30))),
    );
    let mut cur = HashMap::new();
    cur.insert(w, Some(NOW - (H72 + 1_000))); // idle > 72h, < 7d

    let ev = decide_evictions(&wl, &s, &cur, &cfg(), NOW);
    assert!(
        ev.is_empty(),
        "PASS: a proven winner idle >72h (<7d) is spared from inactivity eviction"
    );
    println!("PASS: proven-winner-spared-under-72h");
}

// ── proven-winner-evicted-past-7d-cap ───────────────────────────────────────────
#[test]
fn proven_winner_evicted_past_7d_cap() {
    let w = wallet(1);
    let wl = watchlist(vec![entry(w, 100)]);
    let mut s = HashMap::new();
    s.insert(
        w.to_string(),
        stats(40, dec!(150), Some(dec!(0.05)), Some(dec!(0.30))),
    );
    let mut cur = HashMap::new();
    cur.insert(w, Some(NOW - D7)); // idle == 7d hard cap

    let ev = decide_evictions(&wl, &s, &cur, &cfg(), NOW);
    assert_eq!(ev.len(), 1, "proven winner past the 7d hard cap is evicted");
    assert_eq!(
        ev[0].reason,
        KnockoutReason::InactivityHardCap,
        "PASS: hard-cap reason recorded"
    );
    println!("PASS: proven-winner-evicted-past-7d-cap");
}

// ── negative-edge-evicted-at-72h ────────────────────────────────────────────────
#[test]
fn negative_edge_evicted_at_72h() {
    let w = wallet(1);
    let wl = watchlist(vec![entry(w, 100)]);
    let mut s = HashMap::new();
    // Not demotable (upper CB ≥ 0, so the AND-gate fails) and not a proven winner (lower CB < 0):
    // a wallet with no demonstrated positive edge. Idle ≥ 72h → evicted for inactivity.
    s.insert(
        w.to_string(),
        stats(40, dec!(0), Some(dec!(-0.20)), Some(dec!(0.05))),
    );
    let mut cur = HashMap::new();
    cur.insert(w, Some(NOW - H72)); // idle == 72h threshold

    let ev = decide_evictions(&wl, &s, &cur, &cfg(), NOW);
    assert_eq!(ev.len(), 1, "non-proven wallet idle ≥72h is evicted");
    assert_eq!(
        ev[0].reason,
        KnockoutReason::Inactivity,
        "PASS: evicted via the inactivity trigger, not demotion"
    );
    println!("PASS: negative-edge-evicted-at-72h");
}

// ── realized-pnl≥0-never-demoted ────────────────────────────────────────────────
#[test]
fn realized_pnl_nonneg_never_demoted() {
    let w = wallet(1);
    let wl = watchlist(vec![entry(w, 100)]);
    let mut s = HashMap::new();
    // Confidently-negative upper CB WOULD demote, but realized P&L ≥ 0 blocks it (AND-gate).
    s.insert(
        w.to_string(),
        stats(40, dec!(0), Some(dec!(-0.30)), Some(dec!(-0.05))),
    );
    let mut cur = HashMap::new();
    cur.insert(w, Some(NOW - 10)); // recently active

    let ev = decide_evictions(&wl, &s, &cur, &cfg(), NOW);
    assert!(
        ev.is_empty(),
        "PASS: realized P&L ≥ 0 is never demoted, regardless of a negative upper CB"
    );
    println!("PASS: realized-pnl≥0-never-demoted");
}

// ── backfilled-stale-history-wallet-immune-72h ──────────────────────────────────
#[tokio::test]
async fn backfilled_stale_history_wallet_immune_72h() {
    let (_dir, db) = temp_db();
    let lock = Mutex::new(());

    let existing = wallet(1);
    let live = LiveWatchlist::new(watchlist(vec![entry(existing, 200)]));

    // A backfill candidate whose real on-chain last trade is ancient — but no cursor exists yet.
    let neww = wallet(2);
    assert_eq!(
        db.cursor(&neww).unwrap(),
        None,
        "no cursor before admission (a brand-new wallet)"
    );

    let candidates = vec![entry(neww, 150)];
    let size = apply_evictions_and_backfill(
        &live,
        &db,
        &lock,
        &HashSet::new(),
        &candidates,
        cfg().cap,
        NOW,
    )
    .await;
    assert_eq!(size, 2, "backfilled into the live set");

    // Admission seeded the cursor to `now` (the admission clock), NOT the stale on-chain history.
    assert_eq!(
        db.cursor(&neww).unwrap(),
        Some(NOW),
        "cursor seeded to `now` at admission, inside the writer-locked section"
    );

    // Build the next tick's cursor map from durable state.
    let snap = live.snapshot();
    let mut cur = HashMap::new();
    for w in snap.entries.iter().map(|e| e.wallet) {
        cur.insert(w, db.cursor(&w).unwrap());
    }

    // Just under 72h from admission → immune.
    let under = decide_evictions(&snap, &HashMap::new(), &cur, &cfg(), NOW + H72 - 1);
    assert!(
        !under.iter().any(|e| e.wallet == neww),
        "immune to inactivity eviction for a full 72h from admission"
    );

    // At 72h from admission with no observed trade → now evicted.
    let at = decide_evictions(&snap, &HashMap::new(), &cur, &cfg(), NOW + H72);
    assert!(
        at.iter().any(|e| e.wallet == neww),
        "PASS: evicted only once 72h elapses from admission (not from stale history)"
    );
    println!("PASS: backfilled-stale-history-wallet-immune-72h");
}

// ── writer-mutex-safety ─────────────────────────────────────────────────────────
#[tokio::test]
async fn writer_mutex_serializes_refresh_and_replace() {
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));
    let cap = cfg().cap;
    assert_eq!(cap, 25, "this scenario assumes the maintained-25 cap");

    // Live set starts full: wallets 1..=25.
    let initial: Vec<WatchlistEntry> = (1..=25u8)
        .map(|n| entry(wallet(n), 100 + i32::from(n)))
        .collect();
    let live = LiveWatchlist::new(watchlist(initial));

    // Concurrent refresh: re-scores wallets 1..=25 (score-update-only, never re-admits evicted).
    let live_r = live.clone();
    let lock_r = Arc::clone(&lock);
    let refresh = tokio::spawn(async move {
        let fresh = watchlist(
            (1..=25u8)
                .map(|n| entry(wallet(n), 9_000 + i32::from(n)))
                .collect(),
        );
        let _g = lock_r.lock().await;
        live_r.apply_refresh(&fresh)
    });

    // Concurrent maintenance: evict 1..=5, backfill 26..=30.
    let live_m = live.clone();
    let lock_m = Arc::clone(&lock);
    let db_m = Arc::clone(&db);
    let maint = tokio::spawn(async move {
        let removed: HashSet<WalletAddress> = (1..=5u8).map(wallet).collect();
        let candidates: Vec<WatchlistEntry> = (26..=30u8)
            .map(|n| entry(wallet(n), 50 + i32::from(n)))
            .collect();
        apply_evictions_and_backfill(&live_m, &db_m, &lock_m, &removed, &candidates, cap, NOW).await
    });

    let (_r, _m) = (refresh.await.unwrap(), maint.await.unwrap());

    // The shared writer mutex guarantees neither update is lost, regardless of interleaving:
    let snap = live.snapshot();
    let present: HashSet<WalletAddress> = snap.entries.iter().map(|e| e.wallet).collect();

    assert_eq!(present.len(), snap.entries.len(), "no duplicate wallets");
    assert_eq!(snap.entries.len(), cap, "live set stays at the cap");
    for n in 1..=5u8 {
        assert!(
            !present.contains(&wallet(n)),
            "evicted wallet {n} did not survive a concurrent refresh (no lost update)"
        );
    }
    for n in 26..=30u8 {
        assert!(
            present.contains(&wallet(n)),
            "backfilled wallet {n} was not clobbered by a concurrent refresh"
        );
        assert_eq!(
            db.cursor(&wallet(n)).unwrap(),
            Some(NOW),
            "backfilled wallet {n} cursor seeded at admission"
        );
    }
    println!("PASS: writer-mutex-safety");
}
