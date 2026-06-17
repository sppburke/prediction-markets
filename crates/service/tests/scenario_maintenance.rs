//! Scenario tests for the watchlist maintenance tick (issue #350 WS1 PR-D).
//!
//! Drives the pure decision (`decide_evictions`) and the writer-locked apply
//! (`apply_evictions_and_backfill`) deterministically: fixed `now`, fixed cursors/stats,
//! tempfile-backed [`PaperStateDb`], no live network.
//!
//! Scenarios:
//!   backfilled-wallet-seeded-from-real-last-trade — a backfilled candidate is seeded from its
//!       real `last_trade_unix` (#357), NOT `now`; a freshly-traded one (idle < 72h) is kept.
//!   stale-seeded-wallet-no-admission-grace    — a wallet seeded with a ≥72h-old real last trade
//!       is eviction-eligible immediately (#357 removes the old admission grace).
//!   proven-winner-spared-under-72h            — a proven winner idle >72h (<7d) is spared.
//!   proven-winner-evicted-past-7d-cap         — a proven winner idle ≥7d is evicted (hard cap).
//!   negative-edge-evicted-at-72h              — a not-proven, not-demotable wallet idle ≥72h is
//!       evicted for inactivity, and the eviction carries its real last-trade time for the audit.
//!   realized-pnl≥0-never-demoted              — realized P&L ≥ 0 is never demoted, even with a
//!       confidently-negative upper CB.
//!   writer-mutex-safety                       — concurrent refresh + replace serialized by the
//!       shared writer mutex never lose an update (evicted stay out, backfill seeded from real
//!       last trade stays in).
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
        "evicted via the inactivity trigger, not demotion"
    );
    // The eviction records the wallet's real last-trade time (its cursor) for the audit (#357).
    assert_eq!(
        ev[0].last_trade_unix,
        Some(NOW - H72),
        "PASS: the demote audit carries the real last-trade time, not `now`"
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

// ── backfilled-wallet-seeded-from-real-last-trade ───────────────────────────────
#[tokio::test]
async fn backfilled_wallet_seeded_from_real_last_trade() {
    let (_dir, db) = temp_db();
    let lock = Mutex::new(());

    let existing = wallet(1);
    let live = LiveWatchlist::new(watchlist(vec![entry(existing, 200)]));

    // A backfill candidate that passed the `gte.{now-72h}` freshness filter: its real last trade
    // is recent (idle ~1000s). The maintenance tick seeds its cursor from that real last-trade
    // time (carried in the candidate side-map), NOT from `now` — there is no admission clock (#357).
    let fresh = wallet(2);
    let fresh_last_trade = NOW - 1_000;
    assert_eq!(
        db.cursor(&fresh).unwrap(),
        None,
        "no cursor before admission (a brand-new wallet)"
    );

    let candidates = vec![entry(fresh, 150)];
    let mut candidate_last_trade = HashMap::new();
    candidate_last_trade.insert(fresh, fresh_last_trade);
    let size = apply_evictions_and_backfill(
        &live,
        &db,
        &lock,
        &HashSet::new(),
        &candidates,
        &candidate_last_trade,
        cfg().cap,
        NOW,
    )
    .await;
    assert_eq!(size, 2, "backfilled into the live set");

    // Cursor seeded from the REAL last trade, not `now` (#357 reverses the admission clock).
    assert_eq!(
        db.cursor(&fresh).unwrap(),
        Some(fresh_last_trade),
        "cursor seeded from real last_trade_unix at admission, inside the writer-locked section"
    );

    // idle = now − real_last_trade = 1000s < 72h → kept (it just traded).
    let snap = live.snapshot();
    let mut cur = HashMap::new();
    for w in snap.entries.iter().map(|e| e.wallet) {
        cur.insert(w, db.cursor(&w).unwrap());
    }
    let ev = decide_evictions(&snap, &HashMap::new(), &cur, &cfg(), NOW);
    assert!(
        !ev.iter().any(|e| e.wallet == fresh),
        "PASS: a freshly-traded backfill (idle < 72h) is kept"
    );
    println!("PASS: backfilled-wallet-seeded-from-real-last-trade");
}

// ── stale-seeded-wallet-no-admission-grace ──────────────────────────────────────
#[tokio::test]
async fn stale_seeded_wallet_no_admission_grace() {
    let (_dir, db) = temp_db();
    let lock = Mutex::new(());

    let existing = wallet(1);
    let live = LiveWatchlist::new(watchlist(vec![entry(existing, 200)]));

    // A wallet admitted with a real last trade already 72h old (e.g. a stale bootstrap admission
    // that slipped the candidate freshness filter). #357 seeds the cursor from that real time, so
    // there is NO admission grace: it is eviction-eligible on the very next tick.
    let stale = wallet(2);
    let stale_last_trade = NOW - H72;
    let candidates = vec![entry(stale, 150)];
    let mut candidate_last_trade = HashMap::new();
    candidate_last_trade.insert(stale, stale_last_trade);
    apply_evictions_and_backfill(
        &live,
        &db,
        &lock,
        &HashSet::new(),
        &candidates,
        &candidate_last_trade,
        cfg().cap,
        NOW,
    )
    .await;
    assert_eq!(
        db.cursor(&stale).unwrap(),
        Some(stale_last_trade),
        "cursor seeded from the stale real last trade, not `now`"
    );

    // At admission time `now`, idle == 72h with no stats → evicted immediately (no grace window).
    let snap = live.snapshot();
    let mut cur = HashMap::new();
    for w in snap.entries.iter().map(|e| e.wallet) {
        cur.insert(w, db.cursor(&w).unwrap());
    }
    let ev = decide_evictions(&snap, &HashMap::new(), &cur, &cfg(), NOW);
    assert!(
        ev.iter().any(|e| e.wallet == stale),
        "PASS: a wallet with a ≥72h-old real last trade is evicted on the first tick (no admission grace)"
    );
    println!("PASS: stale-seeded-wallet-no-admission-grace");
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
        // Each backfill candidate carries its real last-trade time (#357); the admission seed
        // must use that value, not `now`. Distinct per wallet so the assertion below is exact.
        let candidate_last_trade: HashMap<WalletAddress, i64> = (26..=30u8)
            .map(|n| (wallet(n), NOW - 100 - i64::from(n)))
            .collect();
        apply_evictions_and_backfill(
            &live_m,
            &db_m,
            &lock_m,
            &removed,
            &candidates,
            &candidate_last_trade,
            cap,
            NOW,
        )
        .await
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
            Some(NOW - 100 - i64::from(n)),
            "backfilled wallet {n} cursor seeded from its real last trade (#357), not `now`"
        );
    }
    println!("PASS: writer-mutex-safety");
}
