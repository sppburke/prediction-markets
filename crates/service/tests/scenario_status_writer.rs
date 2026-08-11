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
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_paper_state::{FillRecord, LeaderPositionRow, PaperStateDb};
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
        long_contracts: 0,
        short_contracts: 0,
    };
    let record = FillRecord {
        idempotency_key: "k1".to_string(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: 10,
        fill_price: Price(dec!(0.40)),
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
    let snap = build_snapshot(&db, "paper", true, 123, 1_700_000_500, 25, 100, 42, None);

    // PASS: every field mirrors the seeded state (bankroll compared scale-insensitively).
    assert_eq!(
        snap.bankroll
            .as_deref()
            .map(|s| Decimal::from_str(s).unwrap()),
        Some(dec!(996))
    );
    assert_eq!(snap.open_positions, 1);
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
        "PASS: status snapshot mirrors seeded paper-state (bankroll 996, 1 fill/pos, 1 settled)"
    );
}

#[test]
fn ac_write_is_atomic_and_valid_json() {
    let (dir, db) = seeded_db();
    let path = dir.path().join("status.json");
    let snap = build_snapshot(&db, "paper", true, 1, 1_700_000_000, 25, 100, 0, None);
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
fn ac_uninitialised_bankroll_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("p.db")).unwrap(); // no init_bankroll
    let snap = build_snapshot(&db, "shadow", false, 0, 1_700_000_000, 0, 100, 0, None);

    // PASS: an uninitialised bankroll is `None` (serializes as JSON null), counts are 0.
    assert!(snap.bankroll.is_none(), "uninitialised bankroll → None");
    assert_eq!(snap.fills_total, 0);
    assert_eq!(snap.open_positions, 0);
    assert_eq!(snap.settled_total, 0);
    println!("PASS: uninitialised bankroll → null; zero counts");
}
