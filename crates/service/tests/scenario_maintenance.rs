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
//!   realized-pnl≥0-never-demoted              — windowed realized P&L ≥ 0 is never demoted, even
//!       with a confidently-negative upper CB.
//!   historic-winner-recent-bleeder-demoted    — lifetime-green wallet bleeding in the trailing
//!       window IS demoted (the dollar-gate rework's behaviour change).
//!   small-sample-consistent-bleeder-demoted   — end-to-end (fills → wallet_edge_stats →
//!       decide_evictions): n=12 consistent bleeder fires; impossible under the old R=2 gate.
//!   writer-mutex-safety                       — concurrent refresh + replace serialized by the
//!       shared writer mutex never lose an update (evicted stay out, backfill seeded from real
//!       last trade stays in).
//!
//!   full-rerank-swap-wholesale             — a batch transition in FullRerank mode replaces the
//!       live set with exactly the incoming top-N: survivors keep their entries, dropped
//!       wallets leave, admitted wallets get their poll cursor seeded from the incoming
//!       side-map (#357) — never `now`.
//!   full-rerank-swap-identity-noop         — incoming == live leaves membership byte-identical
//!       and drops nobody.
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
use pe_paper_state::{AnchorInstallRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_service::demotion_stat::WalletEdgeStats;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::MembershipReason;
use pe_service::runtime_config::{
    AppliedWatchlistCapacity, DEFAULT_ACTIVE_WATCHLIST_SIZE, WatchlistCapacityEpoch,
};
use pe_service::watchlist_admission::AdmissionPreparer;
use pe_service::watchlist_maintenance::{
    KnockoutReason, MaintenanceConfig, MembershipMode, MembershipPublication,
    apply_evictions_and_backfill, apply_full_rerank_swap, decide_evictions,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};

fn capacity(target: usize) -> (AppliedWatchlistCapacity, WatchlistCapacityEpoch) {
    let applied = AppliedWatchlistCapacity::new(target);
    let epoch = applied.load();
    (applied, epoch)
}

fn membership_preparer(
    live: LiveWatchlist,
    paper_state: Arc<PaperStateDb>,
    writer_lock: Arc<Mutex<()>>,
) -> AdmissionPreparer {
    let (control, mut commands) = mpsc::channel(1);
    let control_paper = paper_state.clone();
    tokio::spawn(async move {
        while let Some(command) = commands.recv().await {
            let OrchestratorControl::PublishMembership {
                change,
                replacements,
                checks,
                acknowledged,
            } = command
            else {
                panic!("membership scenario sent an unrelated control")
            };
            let _writer = writer_lock.lock().await;
            if let Err(error) =
                checks.recheck_and_seed(&control_paper, &live, &change, &replacements)
            {
                acknowledged.send(Err(error.to_string())).unwrap();
                continue;
            }

            let removed = change.removed.into_iter().collect::<HashSet<_>>();
            live.replace(&removed, &replacements, change.capacity);
            checks.commit_capacity();
            acknowledged
                .send(Ok(pe_event_log::AppendReceipt {
                    sequence: pe_core_types::EventSeq(1),
                    this_hash: blake3::hash(b"scenario-membership"),
                }))
                .unwrap();
        }
    });
    AdmissionPreparer::new(control, paper_state)
}

fn publication() -> MembershipPublication {
    MembershipPublication {
        reason: MembershipReason::CapacityChange,
        ranking_batch_id: None,
        evidence: serde_json::json!({"scenario": "membership"}),
    }
}

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
        demotion_pnl_window_secs: 2_592_000, // 30 d
        membership_mode: MembershipMode::default(),
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

/// `pnl` seeds BOTH the lifetime and the windowed sum; scenarios that need them to
/// diverge (historic winner, recent bleeder) build `WalletEdgeStats` directly.
fn stats(
    settled: usize,
    pnl: Decimal,
    lower: Option<Decimal>,
    upper: Option<Decimal>,
) -> WalletEdgeStats {
    WalletEdgeStats {
        settled_count: settled,
        realized_pnl: pnl,
        windowed_pnl: pnl,
        lower_cb: lower,
        upper_cb: upper,
    }
}

fn temp_db() -> (TempDir, Arc<PaperStateDb>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    // These scenarios isolate maintenance/cursor behavior. Give every synthetic
    // wallet the durable prerequisite now required at the publication boundary.
    for n in u8::MIN..=u8::MAX {
        db.record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: wallet(n),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    }
    (dir, db)
}

fn install_anchor(db: &PaperStateDb, wallet: WalletAddress, cursor: i64) {
    db.set_cursor(&wallet, cursor).unwrap();
    db.install_anchors(&[AnchorInstallRecord {
        history_status: None,
        wallet,
        balances: Vec::new(),
        activity_cutoff_unix: cursor,
        anchored_at_unix: cursor,
        ledger_hash_after: format!("ledger-{wallet}"),
        positions_proof_hash: format!("positions-{wallet}"),
        activity_bounds_json: "[]".to_owned(),
        source_log_generation: "scenario".to_owned(),
        proof_json: "{\"scenario\":true}".to_owned(),
        recorded_at_unix: cursor,
    }])
    .unwrap();
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

// ── historic-winner-recent-bleeder-demoted ──────────────────────────────────────
#[test]
fn historic_winner_recent_bleeder_demoted() {
    // The dollar-gate rework's behaviour change: lifetime P&L deep green, trailing
    // window red, CB proves the loss → Underperformance eviction. Under the old
    // lifetime conjunct this wallet was shielded indefinitely.
    let w = wallet(1);
    let wl = watchlist(vec![entry(w, 100)]);
    let mut s = HashMap::new();
    s.insert(
        w.to_string(),
        WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(4110), // lifetime green
            windowed_pnl: dec!(-60),  // trailing 30d red
            lower_cb: Some(dec!(-22)),
            upper_cb: Some(dec!(-3.5)),
        },
    );
    let mut cur = HashMap::new();
    cur.insert(w, Some(NOW - 10)); // recently active — demotion is idle-independent

    let ev = decide_evictions(&wl, &s, &cur, &cfg(), NOW);
    assert_eq!(ev.len(), 1);
    assert_eq!(
        ev[0].reason,
        KnockoutReason::Underperformance,
        "PASS: a lifetime winner bleeding in the trailing window is demoted"
    );
    println!("PASS: historic-winner-recent-bleeder-demoted");
}

// ── small-sample-consistent-bleeder-demoted (end-to-end via wallet_edge_stats) ──
#[test]
fn small_sample_consistent_bleeder_demoted() {
    // End-to-end through the REAL statistics path (fills → resolutions →
    // wallet_edge_stats → decide_evictions): 12 consistent −$12.5 settled fills fire
    // the knockout at a sample size where the old per-share R=2 gate was structurally
    // unable to fire (term2 = 13.98/(n−1) > 1 for all n ≤ 14).
    use pe_core_types::{
        CollateralAmount, EventSeq, MarketId, OutcomeId, Price, ShareAmount, Side, VenueMarketId,
    };
    use pe_paper_pnl::ResolutionStore;
    use pe_paper_state::FillRow;
    use pe_service::demotion_stat::wallet_edge_stats;

    let (_dir, db) = temp_db();
    let mut store = ResolutionStore::load(Arc::clone(&db)).unwrap();
    let mid = MarketId(VenueMarketId("0xm".to_string()));
    // YES resolved to 0, settled yesterday (in-window).
    store
        .mark_settled(mid.clone(), vec![dec!(0), dec!(1)], dec!(0), NOW - 86_400)
        .unwrap();

    let w = wallet(1);
    let leader = w.to_string();
    let fills: Vec<FillRow> = (0..12)
        .map(|i| FillRow {
            idempotency_key: format!("wf|{leader}|0xsrc|0xm|0|buy|170000{i:04}"),
            market_id: mid.clone(),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            quantity: ShareAmount::from_whole(25).unwrap(),
            fill_price: Price(dec!(0.50)),
            principal: CollateralAmount::from_decimal_exact(dec!(12.5)).unwrap(),
            fee: CollateralAmount::ZERO,
            event_seq: EventSeq(i),
            prepared_seq: EventSeq(i),
            source_receipt_seq: None,
        })
        .collect();

    let s = wallet_edge_stats(&fills, &store, dec!(0.10), NOW, 2_592_000);
    let wl = watchlist(vec![entry(w, 100)]);
    let mut cur = HashMap::new();
    cur.insert(w, Some(NOW - 10));

    let ev = decide_evictions(&wl, &s, &cur, &cfg(), NOW);
    assert_eq!(ev.len(), 1);
    assert_eq!(
        ev[0].reason,
        KnockoutReason::Underperformance,
        "PASS: n=12 consistent bleeder demoted end-to-end (impossible under old gate)"
    );
    assert_eq!(ev[0].live_pnl, Some(dec!(-150)));
    assert_eq!(ev[0].trades_observed, 12);
    println!("PASS: small-sample-consistent-bleeder-demoted");
}

// ── backfilled-wallet-seeded-from-real-last-trade ───────────────────────────────
#[tokio::test]
async fn backfilled_wallet_seeded_from_real_last_trade() {
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));

    let existing = wallet(1);
    let live = LiveWatchlist::new(watchlist(vec![entry(existing, 200)]));

    // A backfill candidate that passed the `gte.{now-72h}` freshness filter: its real last trade
    // is recent (idle ~1000s). The anchor fixture represents completed admission preparation;
    // structural publication must preserve that causal cursor rather than jump it to `now`.
    let fresh = wallet(2);
    let fresh_last_trade = NOW - 1_000;
    assert_eq!(
        db.cursor(&fresh).unwrap(),
        None,
        "no cursor before admission preparation (a brand-new wallet)"
    );
    install_anchor(&db, fresh, fresh_last_trade);

    let candidates = vec![entry(fresh, 150)];
    let mut candidate_last_trade = HashMap::new();
    candidate_last_trade.insert(fresh, fresh_last_trade);
    let (applied, epoch) = capacity(25);
    let size = apply_evictions_and_backfill(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        MembershipPublication {
            reason: MembershipReason::KnockoutInactivity,
            ..publication()
        },
        &applied,
        epoch,
        &HashSet::new(),
        &candidates,
        &candidate_last_trade,
    )
    .await
    .unwrap();
    assert_eq!(size, 2, "backfilled into the live set");

    // Structural publication preserves the prepared causal cursor rather than replacing it.
    assert_eq!(
        db.cursor(&fresh).unwrap(),
        Some(fresh_last_trade),
        "prepared cursor remains the real last_trade_unix through publication"
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
    let lock = Arc::new(Mutex::new(()));

    let existing = wallet(1);
    let live = LiveWatchlist::new(watchlist(vec![entry(existing, 200)]));

    // A wallet admitted with a real last trade already 72h old (e.g. a stale bootstrap admission
    // that slipped the candidate freshness filter). Its prepared anchor retains that real time, so
    // there is NO admission grace: it is eviction-eligible on the very next tick.
    let stale = wallet(2);
    let stale_last_trade = NOW - H72;
    install_anchor(&db, stale, stale_last_trade);
    let candidates = vec![entry(stale, 150)];
    let mut candidate_last_trade = HashMap::new();
    candidate_last_trade.insert(stale, stale_last_trade);
    let (applied, epoch) = capacity(25);
    apply_evictions_and_backfill(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        MembershipPublication {
            reason: MembershipReason::KnockoutInactivity,
            ..publication()
        },
        &applied,
        epoch,
        &HashSet::new(),
        &candidates,
        &candidate_last_trade,
    )
    .await
    .unwrap();
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
    let cap = DEFAULT_ACTIVE_WATCHLIST_SIZE;
    let (applied, epoch) = capacity(cap);
    assert_eq!(cap, 100, "production default must follow the top 100");

    // Live set starts full: wallets 1..=100.
    let initial: Vec<WatchlistEntry> = (1..=100u8)
        .map(|n| entry(wallet(n), 100 + i32::from(n)))
        .collect();
    let live = LiveWatchlist::new(watchlist(initial));

    // Concurrent refresh: re-scores wallets 1..=100 (score-update-only, never re-admits evicted).
    let live_r = live.clone();
    let lock_r = Arc::clone(&lock);
    let refresh = tokio::spawn(async move {
        let fresh = watchlist(
            (1..=100u8)
                .map(|n| entry(wallet(n), 9_000 + i32::from(n)))
                .collect(),
        );
        let _g = lock_r.lock().await;
        live_r.apply_refresh(&fresh)
    });

    // Concurrent maintenance: evict 1..=5, backfill 101..=105.
    let live_m = live.clone();
    let lock_m = Arc::clone(&lock);
    let db_m = Arc::clone(&db);
    let applied_m = applied.clone();
    let maint = tokio::spawn(async move {
        let removed: HashSet<WalletAddress> = (1..=5u8).map(wallet).collect();
        let candidates: Vec<WatchlistEntry> = (101..=105u8)
            .map(|n| entry(wallet(n), 50 + i32::from(n)))
            .collect();
        // Each backfill candidate carries its real last-trade time (#357); the admission seed
        // must use that value, not `now`. Distinct per wallet so the assertion below is exact.
        let candidate_last_trade: HashMap<WalletAddress, i64> = (101..=105u8)
            .map(|n| (wallet(n), NOW - 100 - i64::from(n)))
            .collect();
        for (wallet, cursor) in &candidate_last_trade {
            install_anchor(&db_m, *wallet, *cursor);
        }
        apply_evictions_and_backfill(
            &live_m,
            &db_m,
            &lock_m,
            &membership_preparer(live_m.clone(), Arc::clone(&db_m), lock_m.clone()),
            MembershipPublication {
                reason: MembershipReason::KnockoutInactivity,
                ..publication()
            },
            &applied_m,
            epoch,
            &removed,
            &candidates,
            &candidate_last_trade,
        )
        .await
        .unwrap()
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
    for n in 101..=105u8 {
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
    println!("PASS: writer-mutex-safety (production default=100)");
}

// ── full-rerank-swap-wholesale (2026-07-03 run28 cutover) ───────────────────────
/// PASS: after the swap, membership == exactly the incoming set; the admitted wallet's
///       cursor is seeded from the incoming side-map (its real last trade, #357).
/// FAIL: any pre-swap non-survivor remains, an incoming wallet is missing, or the
///       admitted wallet's cursor is `now`/unset.
#[tokio::test]
async fn full_rerank_swap_wholesale() {
    let (a, b, c, d) = (wallet(1), wallet(2), wallet(3), wallet(4));
    let live = LiveWatchlist::new(watchlist(vec![entry(a, 300), entry(b, 200), entry(c, 100)]));
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));

    // Incoming batch top-N: B survives, D is admitted, A and C fall out.
    let incoming = vec![entry(b, 250), entry(d, 240)];
    let mut incoming_last_trade = HashMap::new();
    let d_last_trade = NOW - 5_000;
    incoming_last_trade.insert(d, d_last_trade);
    incoming_last_trade.insert(b, NOW - 9_000);
    install_anchor(&db, d, d_last_trade);
    let (applied, epoch) = capacity(25);

    let (total, dropped) = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &applied,
        epoch,
        &incoming,
        &incoming_last_trade,
    )
    .await
    .unwrap();

    let members: HashSet<WalletAddress> =
        live.snapshot().entries.iter().map(|e| e.wallet).collect();
    assert_eq!(total, 2);
    assert_eq!(
        members,
        HashSet::from([b, d]),
        "membership must be exactly the incoming set"
    );
    let dropped_set: HashSet<WalletAddress> = dropped.into_iter().collect();
    assert_eq!(
        dropped_set,
        HashSet::from([a, c]),
        "dropped must be live \\ incoming"
    );
    // The ADMITTED wallet's inactivity clock starts at its real last trade, never `now`.
    assert_eq!(db.cursor(&d).unwrap(), Some(d_last_trade));
    // The SURVIVOR keeps its existing entry (structural swap, not a score refresh).
    let b_entry = live
        .snapshot()
        .entries
        .iter()
        .find(|e| e.wallet == b)
        .cloned()
        .unwrap();
    assert_eq!(
        b_entry.leader_score_bps.0, 200,
        "survivor entry left unchanged by the swap"
    );
    println!("PASS: full-rerank-swap-wholesale (set==incoming, cursor seeded, survivor intact)");
}

// ── full-rerank-swap-identity-noop ──────────────────────────────────────────────
/// PASS: incoming == live drops nobody and leaves membership identical.
/// FAIL: any eviction or membership change on an identity swap.
#[tokio::test]
async fn full_rerank_swap_identity_noop() {
    let (a, b) = (wallet(1), wallet(2));
    let live = LiveWatchlist::new(watchlist(vec![entry(a, 300), entry(b, 200)]));
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));
    let incoming = vec![entry(a, 310), entry(b, 210)];
    let side: HashMap<WalletAddress, i64> = HashMap::new();
    let (applied, epoch) = capacity(25);

    let (total, dropped) = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &applied,
        epoch,
        &incoming,
        &side,
    )
    .await
    .unwrap();

    assert_eq!(total, 2);
    assert!(dropped.is_empty(), "identity swap must drop nobody");
    let members: HashSet<WalletAddress> =
        live.snapshot().entries.iter().map(|e| e.wallet).collect();
    assert_eq!(members, HashSet::from([a, b]));
    // Mode default sanity rides along: the legacy mode remains the compiled default.
    assert_eq!(MembershipMode::default(), MembershipMode::Knockout);
    println!("PASS: full-rerank-swap-identity-noop");
}

// ── runtime-capacity-grow-shrink ────────────────────────────────────────
/// PASS: one process can atomically grow 50→100 and shrink 100→75; every newly admitted
///       wallet receives its ranking-provided inactivity cursor before the call returns.
/// FAIL: a restart is required, membership is not exactly the requested top-N, or a hot-grown
///       cursor is missing/seeded to `now`.
#[tokio::test]
async fn runtime_capacity_grows_and_shrinks_without_restart() {
    let initial: Vec<WatchlistEntry> = (1..=50u8)
        .map(|n| entry(wallet(n), 1_000 - i32::from(n)))
        .collect();
    let live = LiveWatchlist::new(watchlist(initial));
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));

    let top_100: Vec<WatchlistEntry> = (1..=100u8)
        .map(|n| entry(wallet(n), 2_000 - i32::from(n)))
        .collect();
    let side: HashMap<WalletAddress, i64> = (1..=100u8)
        .map(|n| (wallet(n), NOW - i64::from(n)))
        .collect();
    for n in 51..=100u8 {
        install_anchor(&db, wallet(n), NOW - i64::from(n));
    }
    let (grow_capacity, grow_epoch) = capacity(100);
    let (grown, dropped) = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &grow_capacity,
        grow_epoch,
        &top_100,
        &side,
    )
    .await
    .unwrap();
    assert_eq!(grown, 100);
    assert!(dropped.is_empty());
    for n in 51..=100u8 {
        assert_eq!(db.cursor(&wallet(n)).unwrap(), Some(NOW - i64::from(n)));
    }

    let top_75: Vec<WatchlistEntry> = top_100.into_iter().take(75).collect();
    let (shrink_capacity, shrink_epoch) = capacity(75);
    let (shrunk, dropped) = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &shrink_capacity,
        shrink_epoch,
        &top_75,
        &side,
    )
    .await
    .unwrap();
    assert_eq!(shrunk, 75);
    assert_eq!(dropped.len(), 25);
    let members: HashSet<WalletAddress> = live
        .snapshot()
        .entries
        .iter()
        .map(|item| item.wallet)
        .collect();
    assert_eq!(members, (1..=75u8).map(wallet).collect());
    println!("PASS: runtime-capacity hot grow 50->100 and shrink 100->75");
}

// ── stale-capacity-plan-rejected ──────────────────────────────────────
/// PASS: a maintenance plan captured at 50 cannot truncate a newer 100-wallet generation, and
///       an ABA target match with a different generation is still rejected.
#[tokio::test]
async fn stale_capacity_epoch_cannot_undo_a_newer_membership() {
    let live = LiveWatchlist::new(watchlist(
        (1..=100u8)
            .map(|n| entry(wallet(n), 2_000 - i32::from(n)))
            .collect(),
    ));
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));
    let applied = AppliedWatchlistCapacity::new(50);
    let stale_50 = applied.load();
    applied.store(WatchlistCapacityEpoch {
        generation: 1,
        target: 100,
    });
    let top_50: Vec<WatchlistEntry> = (1..=50u8)
        .map(|n| entry(wallet(n), 3_000 - i32::from(n)))
        .collect();
    let side: HashMap<WalletAddress, i64> = HashMap::new();
    let error = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &applied,
        stale_50,
        &top_50,
        &side,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("stale watchlist capacity plan"));
    assert_eq!(live.snapshot().entries.len(), 100);

    // ABA: target returns to 50, but generation 2 must still reject generation 0's stale plan.
    applied.store(WatchlistCapacityEpoch {
        generation: 2,
        target: 50,
    });
    let error = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &applied,
        stale_50,
        &top_50,
        &side,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("stale watchlist capacity plan"));
    assert_eq!(live.snapshot().entries.len(), 100);
    println!("PASS: stale capacity epochs cannot undo newer membership (including ABA)");
}

// ── cursor-prerequisite-fail-closed ───────────────────────────────────
/// PASS: a missing last-trade side-map rejects an anchored admission before ArcSwap publication.
#[tokio::test]
async fn missing_admission_cursor_leaves_membership_unchanged() {
    let original = wallet(1);
    let newcomer = wallet(2);
    let live = LiveWatchlist::new(watchlist(vec![entry(original, 100)]));
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));
    let (applied, epoch) = capacity(1);
    let incoming = vec![entry(newcomer, 200)];
    install_anchor(&db, newcomer, 0);

    let error = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &applied,
        epoch,
        &incoming,
        &HashMap::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("missing last_trade_unix"));
    let members: HashSet<WalletAddress> = live
        .snapshot()
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .collect();
    assert_eq!(members, HashSet::from([original]));
    assert_eq!(db.cursor(&newcomer).unwrap(), Some(0));
    println!("PASS: cursor prerequisite fails closed before membership publication");
}

// ── survivor-bench-exhausted-shrinks-below-cap ──────────────────────────────────
// Scenario: after #518 the bench is survivor-filtered, so between batch swaps every surviving
// wallet is already live and the candidate fetch legitimately returns nothing. The tick must
// still evict, leaving membership BELOW the applied cap until the next batch restores it.
// PASS: the evicted wallet is gone, membership is 2 of a cap of 25, and the applied target is
//       unchanged (a cap, not the followed count).
// FAIL: eviction is suppressed by the empty bench, or the applied target moves.
#[tokio::test]
async fn survivor_bench_exhausted_still_evicts_and_shrinks_below_cap() {
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));

    let keep_a = wallet(1);
    let keep_b = wallet(2);
    let leaving = wallet(3);
    let live = LiveWatchlist::new(watchlist(vec![
        entry(keep_a, 300),
        entry(keep_b, 200),
        entry(leaving, 100),
    ]));

    let (applied, epoch) = capacity(25);
    let removed: HashSet<WalletAddress> = [leaving].into_iter().collect();

    // The survivor-filtered bench yields nobody: all survivors are already live.
    let size = apply_evictions_and_backfill(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        MembershipPublication {
            reason: MembershipReason::KnockoutInactivity,
            ..publication()
        },
        &applied,
        epoch,
        &removed,
        &[],
        &HashMap::new(),
    )
    .await
    .unwrap();

    assert_eq!(size, 2, "eviction applied even with an empty candidate set");
    let snap = live.snapshot();
    assert!(
        !snap.entries.iter().any(|e| e.wallet == leaving),
        "the evicted wallet left the live set"
    );
    assert!(
        snap.entries.iter().any(|e| e.wallet == keep_a)
            && snap.entries.iter().any(|e| e.wallet == keep_b),
        "the remaining survivors stayed live"
    );
    assert_eq!(
        applied.load(),
        epoch,
        "`active_watchlist_size` is a pure cap: membership below it never moves the applied target"
    );
    println!("PASS: survivor-bench-exhausted-still-evicts-and-shrinks-below-cap");
}

// ── full-rerank-swap-on-a-batch-with-no-survivors ───────────────────────────────
// Scenario: #518 makes an empty survivor-filtered read reachable in normal operation — a batch
// whose rows all fail the gate, or one carrying no verdict at all. Retaining the previous set
// would keep copying wallets the CURRENT batch calls ineligible, and forever, because the batch
// marker never advances past it.
// PASS: the swap applies an empty membership, every previous wallet is dropped, and the applied
//       capacity target is untouched — matching the cold-boot stance for the same condition.
// FAIL: membership survives the swap, or the applied target moves.
#[tokio::test]
async fn full_rerank_swap_on_a_batch_with_no_survivors_empties_the_live_set() {
    let (_dir, db) = temp_db();
    let lock = Arc::new(Mutex::new(()));

    let before: Vec<WatchlistEntry> = (1..=27u8)
        .map(|n| entry(wallet(n), 2_000 - i32::from(n)))
        .collect();
    let live = LiveWatchlist::new(watchlist(before));
    let (applied, epoch) = capacity(100);

    let (size, dropped) = apply_full_rerank_swap(
        &live,
        &db,
        &lock,
        &membership_preparer(live.clone(), Arc::clone(&db), lock.clone()),
        publication(),
        &applied,
        epoch,
        &[],
        &HashMap::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        size, 0,
        "the ranker endorsed nobody, so the live set is empty"
    );
    assert_eq!(
        dropped.len(),
        27,
        "every previously live wallet was dropped"
    );
    assert!(
        live.snapshot().entries.is_empty(),
        "no wallet is still copied"
    );
    assert_eq!(
        applied.load(),
        epoch,
        "an empty batch never moves the applied capacity target"
    );
    println!("PASS: full-rerank-swap-on-a-batch-with-no-survivors-empties-the-live-set");
}
