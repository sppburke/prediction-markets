//! Scenario: --rebuild-state correctness (issue #282 Phase 4, AC12).
//!
//! Verifies that a fresh paper-state DB reconciled from the event log produces
//! the same fills/positions/bankroll as the original DB built via `commit_fill`.
//! This is the core guarantee of `--rebuild-state`.
//!
//! Run with: cargo nextest run -p pe-service --features scenario scenario_rebuild_state

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceId, SourceTimestamp, SourceTradeId,
    StrategyId, VenueMarketId, WalletAddress,
};
use pe_event_log::Writer;
use pe_paper_state::{FillRecord, LeaderPositionRow, PaperStateDb};
use pe_service::paper_recovery::reconcile_paper_state;
use pe_strategy_winner_follow::PaperExecutor;
use pe_venue_core::OrderIntent;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

fn market() -> MarketId {
    MarketId(VenueMarketId("0xmarket".to_string()))
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

fn leader(long: u64) -> LeaderPositionRow {
    LeaderPositionRow {
        wallet: wallet(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        long_contracts: long,
        short_contracts: 0,
    }
}

fn intent(key: &str, side: Side, contracts: u64, price: Decimal) -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId("winner-follow".to_string()),
        market_id: market(),
        outcome_id: OutcomeId(0),
        side,
        contracts: ContractQty(contracts),
        limit_price: Price(price),
        validity_seconds: 30,
        idempotency_key: key.to_string(),
    }
}

/// Scenario: rebuild from event log matches original DB state.
///
/// PASS: fresh DB reconciled from event log has identical fills, positions, bankroll.
/// FAIL: any field discrepancy between original and rebuilt DB.
#[test]
fn rebuild_state_matches_original() {
    println!("Scenario: rebuild_state — fresh DB replay matches original");

    let orig_dir = TempDir::new().unwrap();
    let rebuild_dir = TempDir::new().unwrap();
    let log_path = orig_dir.path().join("paper.log");
    let orig_db_path = orig_dir.path().join("paper_state.db");
    let rebuild_db_path = rebuild_dir.path().join("paper_state_rebuilt.db");
    let ts = SourceTimestamp(OffsetDateTime::UNIX_EPOCH);
    let initial = dec!(1000);

    // ── Build original state ───────────────────────────────────────────────────
    let orig = PaperStateDb::open(&orig_db_path).unwrap();
    orig.init_bankroll(initial).unwrap();

    let writer = Writer::open(&log_path).unwrap();
    let mut executor = PaperExecutor::new(writer, SourceId("test".into()), 0, 0);

    // BUY 10 @ 0.40
    let i1 = intent("k1", Side::Buy, 10, dec!(0.40));
    let (f1, seq1) = executor.execute(&i1, ts.clone()).unwrap();
    let r1 = FillRecord {
        idempotency_key: f1.intent.idempotency_key.clone(),
        market_id: f1.intent.market_id.clone(),
        outcome_id: f1.intent.outcome_id,
        side: f1.intent.side,
        contracts: f1.intent.contracts.0,
        fill_price: f1.simulated_fill_price,
    };
    orig.commit_fill(&SourceTradeId("tx1".to_string()), &leader(10), &r1, seq1)
        .unwrap();

    // BUY 5 @ 0.60
    let i2 = intent("k2", Side::Buy, 5, dec!(0.60));
    let (f2, seq2) = executor.execute(&i2, ts).unwrap();
    let r2 = FillRecord {
        idempotency_key: f2.intent.idempotency_key.clone(),
        market_id: f2.intent.market_id.clone(),
        outcome_id: f2.intent.outcome_id,
        side: f2.intent.side,
        contracts: f2.intent.contracts.0,
        fill_price: f2.simulated_fill_price,
    };
    orig.commit_fill(&SourceTradeId("tx2".to_string()), &leader(15), &r2, seq2)
        .unwrap();

    let orig_bankroll = orig.bankroll().unwrap().unwrap();
    let orig_fills = orig.fills_count().unwrap();
    let orig_seq = orig.last_applied_event_seq().unwrap();
    let orig_pos = orig.paper_positions().unwrap();

    // ── Rebuild from event log ─────────────────────────────────────────────────
    let rebuilt = PaperStateDb::open(&rebuild_db_path).unwrap();
    rebuilt.init_bankroll(initial).unwrap();
    let applied = reconcile_paper_state(&log_path, &rebuilt).unwrap();

    let rebuilt_bankroll = rebuilt.bankroll().unwrap().unwrap();
    let rebuilt_fills = rebuilt.fills_count().unwrap();
    let rebuilt_seq = rebuilt.last_applied_event_seq().unwrap();
    let rebuilt_pos = rebuilt.paper_positions().unwrap();

    assert_eq!(applied, 2, "should replay exactly 2 fills");
    assert_eq!(rebuilt_bankroll, orig_bankroll, "bankroll mismatch");
    assert_eq!(rebuilt_fills, orig_fills, "fills count mismatch");
    assert_eq!(rebuilt_seq, orig_seq, "last_applied_event_seq mismatch");
    assert_eq!(rebuilt_pos.len(), orig_pos.len(), "position count mismatch");
    if let (Some(rp), Some(op)) = (rebuilt_pos.first(), orig_pos.first()) {
        assert_eq!(
            rp.long_contracts, op.long_contracts,
            "long_contracts mismatch"
        );
    }

    println!(
        "PASS: bankroll={rebuilt_bankroll}, fills={rebuilt_fills}, seq={rebuilt_seq:?}, pos_count={}",
        rebuilt_pos.len()
    );
}

/// Scenario: rebuild from empty/missing event log leaves bankroll at configured initial.
///
/// PASS: fresh DB with no event log has bankroll = configured_bankroll, 0 fills.
/// FAIL: bankroll differs or reconcile errors.
#[test]
fn rebuild_state_empty_log_gives_initial_bankroll() {
    println!("Scenario: rebuild_state — missing log → initial bankroll");

    let d = TempDir::new().unwrap();
    let db_path = d.path().join("paper.db");
    let log_path = d.path().join("paper.log"); // intentionally absent

    let initial = dec!(5000);
    let db = PaperStateDb::open(&db_path).unwrap();
    db.init_bankroll(initial).unwrap();

    let applied = reconcile_paper_state(&log_path, &db).unwrap();

    assert_eq!(applied, 0, "no fills with missing log");
    assert_eq!(
        db.bankroll().unwrap(),
        Some(initial),
        "bankroll must be initial"
    );

    println!("PASS: missing log → bankroll={initial}, applied=0");
}
