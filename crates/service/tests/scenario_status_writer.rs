//! Scenario tests for the `status.json` health snapshot (agent-friendly logging).
//!
//! Verifies the snapshot reflects live paper-state, is written atomically as valid JSON, and
//! handles an uninitialised bankroll. Deterministic — `build_snapshot` takes its time/size
//! inputs as scalars, so no clock is read.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::str::FromStr as _;

use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, OutcomeId, Price, ShareAmount, Side, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_paper_state::{AnchorInstallRecord, FillRecord, LeaderPositionRow, PaperStateDb};
use pe_service::status_writer::{build_snapshot, write_snapshot};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;

fn market() -> MarketId {
    MarketId(VenueMarketId("0xmkt".to_string()))
}

/// A paper-state with 1 fill (bankroll 1000 → 996, 1 position, event_seq 7) and 1 settled market.
fn seeded_db() -> (TempDir, PaperStateDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
    db.init_bankroll(dec!(1000)).unwrap();
    let leader = LeaderPositionRow {
        wallet: WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        long_contracts: pe_core_types::ShareAmount::ZERO,
        short_contracts: pe_core_types::ShareAmount::ZERO,
    };
    let record = FillRecord {
        idempotency_key: "k1".to_string(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        quantity: ShareAmount::from_whole(10).unwrap(),
        fill_price: Price(dec!(0.40)),
        principal: CollateralAmount::from_decimal_exact(dec!(4)).unwrap(),
        fee: CollateralAmount::ZERO,
    };
    db.commit_fill(
        &SourceTradeId("s1".to_string()),
        &leader,
        &record,
        EventSeq(7),
    )
    .unwrap();
    db.record_settled_market(&market(), "[\"1\",\"0\"]", dec!(5), 1_700_000_000)
        .unwrap();
    (dir, db)
}

#[test]
fn ac_snapshot_reflects_state() {
    let (_dir, db) = seeded_db();
    let snap = build_snapshot(
        &db,
        "paper",
        true,
        123,
        1_700_000_500,
        25,
        &[],
        100,
        42,
        None,
    );

    // PASS: every field mirrors the seeded state (bankroll compared scale-insensitively).
    assert_eq!(
        snap.bankroll
            .as_deref()
            .map(|s| Decimal::from_str(s).unwrap()),
        Some(dec!(996))
    );
    // #516: `open_positions` counts genuinely open positions (net-nonzero AND market
    // not settled). The seeded fill's market IS settled, so the truthful count is 0.
    assert_eq!(snap.open_positions, 0);
    assert_eq!(snap.fills_total, 1);
    assert_eq!(snap.settled_total, 1);
    assert_eq!(snap.last_event_seq, 7);
    assert_eq!(snap.watchlist_size, 25);
    assert_eq!(snap.watchlist_target_size, 100);
    assert_eq!(snap.supabase_rpc_calls, 42);
    assert_eq!(snap.mode, "paper");
    assert!(snap.authoritative);
    assert_eq!(snap.uptime_secs, 123);
    // RFC-3339 formatting of the injected unix instant (1_700_000_500 = 2023-11-14T22:21:40Z).
    assert_eq!(snap.updated_at, "2023-11-14T22:21:40Z");
    println!(
        "PASS: status snapshot mirrors seeded paper-state (bankroll 996, 1 fill, settled ⇒ 0 open)"
    );
}

#[test]
fn ac_write_is_atomic_and_valid_json() {
    let (dir, db) = seeded_db();
    let path = dir.path().join("status.json");
    let snap = build_snapshot(&db, "paper", true, 1, 1_700_000_000, 25, &[], 100, 0, None);
    write_snapshot(&path, &snap).unwrap();

    // PASS: the file exists, no temp left behind, and parses to the expected shape.
    assert!(path.exists());
    assert!(
        !path.with_extension("json.tmp").exists(),
        "temp file cleaned up by the rename"
    );
    let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        Decimal::from_str(v["bankroll"].as_str().unwrap()).unwrap(),
        dec!(996)
    );
    assert_eq!(v["fills_total"], 1);
    assert_eq!(v["authoritative"], true);
    println!("PASS: status.json written atomically (temp+rename) as valid JSON");
}

#[test]
fn ac_live_block_reports_freshness() {
    use pe_service::live_accounts::{AccountRow, CredentialMetaRow, LiveAccountsSnapshot};
    let (_dir, db) = seeded_db();
    let rows = vec![AccountRow {
        account_id: "sppburke".to_string(),
        is_primary: true,
        enabled: true,
        execution_order: 0,
        requested_live_mode: "off".to_string(),
        effective_live_mode: "off".to_string(),
        live_price_impact_cap_bps: 100,
        custody_wallet_address: None,
        custody_wallet_kind: None,
    }];
    let creds = vec![CredentialMetaRow {
        account_id: "sppburke".to_string(),
        bundle_version: 1,
        key_id: "k".to_string(),
    }];
    let mut snapshot = LiveAccountsSnapshot::from_rows(rows, &creds);
    snapshot.fetched_at_unix = Some(1_700_000_400);

    // PASS: the exact live JSON contract (#514) — fetched_at_unix, stale, seed depths,
    // and per-account rows. Age 100 s < the 120 s bound ⇒ fresh.
    let snap = build_snapshot(
        &db,
        "paper",
        true,
        1,
        1_700_000_500,
        25,
        &[],
        100,
        0,
        Some(&snapshot),
    );
    let v = serde_json::to_value(&snap).unwrap();
    assert_eq!(v["live"]["fetched_at_unix"], 1_700_000_400);
    assert_eq!(v["live"]["stale"], false);
    assert_eq!(v["live"]["pending_dispatch_seeds"], 0);
    assert_eq!(v["live"]["ready_dispatch_seeds"], 0);
    assert_eq!(v["live"]["accounts"][0]["account_id"], "sppburke");
    assert_eq!(v["live"]["accounts"][0]["armed"], false);

    // Age exactly at the bound ⇒ stale.
    snapshot.fetched_at_unix = Some(1_700_000_500 - 120);
    let snap = build_snapshot(
        &db,
        "paper",
        true,
        1,
        1_700_000_500,
        25,
        &[],
        100,
        0,
        Some(&snapshot),
    );
    let v = serde_json::to_value(&snap).unwrap();
    assert_eq!(v["live"]["stale"], true);

    // Never-successful ⇒ null fetched_at_unix and stale.
    snapshot.fetched_at_unix = None;
    let snap = build_snapshot(
        &db,
        "paper",
        true,
        1,
        1_700_000_500,
        25,
        &[],
        100,
        0,
        Some(&snapshot),
    );
    let v = serde_json::to_value(&snap).unwrap();
    assert_eq!(v["live"]["fetched_at_unix"], serde_json::Value::Null);
    assert_eq!(v["live"]["stale"], true);
    println!("PASS: live block reports fetched_at_unix + stale across fresh/boundary/never");
}

#[test]
fn ac_uninitialised_bankroll_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("p.db")).unwrap(); // no init_bankroll
    let snap = build_snapshot(&db, "shadow", false, 0, 1_700_000_000, 0, &[], 100, 0, None);

    // PASS: an uninitialised bankroll is `None` (serializes as JSON null), counts are 0.
    assert!(snap.bankroll.is_none(), "uninitialised bankroll → None");
    assert_eq!(snap.fills_total, 0);
    assert_eq!(snap.open_positions, 0);
    assert_eq!(snap.settled_total, 0);
    println!("PASS: uninitialised bankroll → null; zero counts");
}

#[test]
fn anchor_age_is_none_before_install_and_tracks_the_oldest_latest_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
    let now = 1_700_000_000;
    let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let before = build_snapshot(&db, "paper", true, 0, now, 1, &[wallet], 100, 0, None);
    assert_eq!(before.oldest_anchor_age_secs, None);

    db.set_cursor(&wallet, 10).unwrap();
    db.install_anchors(&[AnchorInstallRecord {
        history_status: None,
        wallet,
        balances: Vec::new(),
        activity_cutoff_unix: 10,
        anchored_at_unix: now - 75,
        ledger_hash_after: "empty".to_owned(),
        positions_proof_hash: "positions".to_owned(),
        activity_bounds_json: "[]".to_owned(),
        source_log_generation: "scenario".to_owned(),
        proof_json: "{}".to_owned(),
        recorded_at_unix: now - 75,
    }])
    .unwrap();
    let after = build_snapshot(&db, "paper", true, 0, now, 1, &[wallet], 100, 0, None);
    assert_eq!(after.oldest_anchor_age_secs, Some(75));
}

#[test]
fn anchor_age_excludes_wallets_removed_from_the_live_watchlist() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
    let now = 1_700_000_000;
    let removed = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let active = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    for wallet in [removed, active] {
        db.set_cursor(&wallet, 10).unwrap();
    }
    db.install_anchors(&[
        AnchorInstallRecord {
            history_status: None,
            wallet: removed,
            balances: Vec::new(),
            activity_cutoff_unix: 10,
            anchored_at_unix: now - 100,
            ledger_hash_after: "removed".to_owned(),
            positions_proof_hash: "positions-removed".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: now - 100,
        },
        AnchorInstallRecord {
            history_status: None,
            wallet: active,
            balances: Vec::new(),
            activity_cutoff_unix: 10,
            anchored_at_unix: now - 5,
            ledger_hash_after: "active".to_owned(),
            positions_proof_hash: "positions-active".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: now - 5,
        },
    ])
    .unwrap();

    let live_only = build_snapshot(&db, "paper", true, 0, now, 1, &[active], 100, 0, None);
    assert_eq!(live_only.oldest_anchor_age_secs, Some(5));
    let historical = build_snapshot(
        &db,
        "paper",
        true,
        0,
        now,
        2,
        &[removed, active],
        100,
        0,
        None,
    );
    assert_eq!(historical.oldest_anchor_age_secs, Some(100));
}
