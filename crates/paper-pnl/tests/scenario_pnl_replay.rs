#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Scenario: replay == snapshot.
//!
//! Build a paper-state DB with known fills; apply resolution credits manually;
//! verify the PnlLedger snapshot matches the expected values deterministically.
//! Fixed inputs, no live network, no SystemTime::now().

use std::collections::HashMap;
use std::sync::Arc;

use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, ShareAmount, Side, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_paper_pnl::{PnlLedger, ResolutionStore};
use pe_paper_state::{FillRecord, LeaderPositionRow, PaperStateDb};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn market(s: &str) -> MarketId {
    MarketId(VenueMarketId(s.to_string()))
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

fn leader(market_id: MarketId, long: u64) -> LeaderPositionRow {
    LeaderPositionRow {
        wallet: wallet(),
        market_id,
        outcome_id: OutcomeId(0),
        long_contracts: ShareAmount::from_whole(long).unwrap(),
        short_contracts: ShareAmount::ZERO,
    }
}

fn fill(key: &str, market_id: MarketId, side: Side, contracts: u64, price: Decimal) -> FillRecord {
    FillRecord {
        idempotency_key: key.to_string(),
        market_id,
        outcome_id: OutcomeId(0),
        side,
        contracts,
        fill_price: Price(price),
    }
}

/// Scenario: two fills in two markets; one resolves YES, one unresolved.
/// PASS: snapshot bankroll = initial - costs + resolution_credit, pnl = bankroll - initial.
/// FAIL: any numeric mismatch, or snapshot() returns Err.
#[test]
fn snapshot_matches_expected_after_resolution() {
    println!("Scenario: pnl_replay — replay == snapshot");

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("paper.db");

    let db = Arc::new(PaperStateDb::open(&db_path).unwrap());
    let initial = dec!(1000);
    db.init_bankroll(initial).unwrap();

    // Fill 1: BUY 10 YES in market-A at 0.40 → cost 4.00
    let mkt_a = market("0xmarket_a");
    db.commit_fill(
        &SourceTradeId("tx1".to_string()),
        &leader(mkt_a.clone(), 10),
        &fill("k1", mkt_a.clone(), Side::Buy, 10, dec!(0.40)),
        EventSeq(1),
    )
    .unwrap();

    // Fill 2: BUY 5 YES in market-B at 0.60 → cost 3.00
    let mkt_b = market("0xmarket_b");
    db.commit_fill(
        &SourceTradeId("tx2".to_string()),
        &leader(mkt_b.clone(), 5),
        &fill("k2", mkt_b.clone(), Side::Buy, 5, dec!(0.60)),
        EventSeq(2),
    )
    .unwrap();

    // bankroll after fills = 1000 - 4.00 - 3.00 = 993.00
    assert_eq!(db.bankroll().unwrap(), Some(dec!(993)));

    // Market-A resolves YES: credit = 10 * 1.0 = 10.00
    let positions_a: Vec<_> = db
        .paper_positions()
        .unwrap()
        .into_iter()
        .filter(|p| p.market_id == mkt_a)
        .collect();
    let credit = PnlLedger::resolution_credit(&positions_a, &[dec!(1), dec!(0)]);
    assert_eq!(credit, dec!(10), "resolution credit should be 10");

    db.credit_bankroll(credit).unwrap();

    let mut store = ResolutionStore::load(db.clone()).unwrap();
    store
        .mark_settled(mkt_a.clone(), vec![dec!(1), dec!(0)], credit, 1_700_000_000)
        .unwrap();

    // bankroll after resolution credit = 993 + 10 = 1003
    let snapshot = PnlLedger::snapshot(&db, &store, initial, &HashMap::new()).unwrap();

    assert_eq!(snapshot.current_bankroll, dec!(1003), "bankroll mismatch");
    assert_eq!(
        snapshot.total_pnl,
        dec!(3),
        "pnl should be +3 (10 credit - 7 cost)"
    );
    assert_eq!(snapshot.resolution_credits, dec!(10));
    assert_eq!(snapshot.settled_markets, 1);
    assert_eq!(
        snapshot.open_position_count, 1,
        "only the unsettled market-B is open; settled market-A is excluded"
    );
    assert_eq!(snapshot.fills_count, 2);

    println!("PASS: snapshot matches expected after resolution");
}

/// Scenario: replay from scratch matches first-run snapshot.
/// PASS: second PnlLedger::snapshot on the same DB + same resolutions gives same numbers.
/// FAIL: any value differs between the two calls.
#[test]
fn replay_equals_snapshot() {
    println!("Scenario: pnl_replay — idempotent snapshot");

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("paper.db");

    let db = Arc::new(PaperStateDb::open(&db_path).unwrap());
    let initial = dec!(500);
    db.init_bankroll(initial).unwrap();

    let mkt = market("0xmkt");
    db.commit_fill(
        &SourceTradeId("tx1".to_string()),
        &leader(mkt.clone(), 20),
        &fill("k1", mkt.clone(), Side::Buy, 20, dec!(0.50)),
        EventSeq(1),
    )
    .unwrap();

    let positions: Vec<_> = db
        .paper_positions()
        .unwrap()
        .into_iter()
        .filter(|p| p.market_id == mkt)
        .collect();
    let credit = PnlLedger::resolution_credit(&positions, &[dec!(1), dec!(0)]);
    db.credit_bankroll(credit).unwrap();

    let mut store = ResolutionStore::load(db.clone()).unwrap();
    store
        .mark_settled(mkt, vec![dec!(1), dec!(0)], credit, 1_700_000_000)
        .unwrap();

    let snap1 = PnlLedger::snapshot(&db, &store, initial, &HashMap::new()).unwrap();

    // Reload store to simulate restart.
    let store2 = ResolutionStore::load(db.clone()).unwrap();
    let snap2 = PnlLedger::snapshot(&db, &store2, initial, &HashMap::new()).unwrap();

    assert_eq!(
        snap1.current_bankroll, snap2.current_bankroll,
        "replay bankroll mismatch"
    );
    assert_eq!(snap1.total_pnl, snap2.total_pnl, "replay pnl mismatch");
    assert_eq!(
        snap1.resolution_credits, snap2.resolution_credits,
        "replay credits mismatch"
    );
    assert_eq!(
        snap1.fills_count, snap2.fills_count,
        "replay fills mismatch"
    );

    println!("PASS: replay equals snapshot");
}

/// Scenario: an open (unsettled) position marked to market via supplied mids.
/// PASS: realized = 0, open_market_value = mid × contracts, unrealized = MV − cost.
/// FAIL: any numeric mismatch, or snapshot() returns Err.
#[test]
fn open_position_marked_to_market() {
    println!("Scenario: pnl_replay — open position marked to market");

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("paper.db");

    let db = Arc::new(PaperStateDb::open(&db_path).unwrap());
    let initial = dec!(1000);
    db.init_bankroll(initial).unwrap();

    // BUY 100 YES @ 0.40 → cost 40, bankroll 960.
    let mkt = market("0xopen");
    db.commit_fill(
        &SourceTradeId("tx1".to_string()),
        &leader(mkt.clone(), 100),
        &fill("k1", mkt.clone(), Side::Buy, 100, dec!(0.40)),
        EventSeq(1),
    )
    .unwrap();
    assert_eq!(db.bankroll().unwrap(), Some(dec!(960)));

    // No settlement; supply a live mid of 0.55 for the open market.
    let store = ResolutionStore::load(db.clone()).unwrap();
    let mut mids = HashMap::new();
    mids.insert(mkt.clone(), vec![dec!(0.55), dec!(0.45)]);

    let snapshot = PnlLedger::snapshot(&db, &store, initial, &mids).unwrap();

    assert_eq!(
        snapshot.total_pnl,
        dec!(-40),
        "bankroll delta is −40 (cost debited)"
    );
    assert_eq!(
        snapshot.realized_pnl,
        dec!(0),
        "nothing settled → realized 0"
    );
    assert_eq!(snapshot.open_market_value, dec!(55), "100 × 0.55");
    assert_eq!(snapshot.unrealized_pnl, dec!(15), "55 − 40 cost");
    assert_eq!(
        snapshot.displayed_total(),
        dec!(15),
        "realized 0 + unrealized 15"
    );
    assert_eq!(snapshot.open_position_count, 1);

    println!("PASS: open position marked to market");
}
