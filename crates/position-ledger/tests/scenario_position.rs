//! Scenario: sequential trade ingestion → position state at each step.
//!
//! PASS: position fields match expected long/short at every checkpoint.
//! FAIL: any assertion mismatch, or the ledger panics.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{
    ContractQty, MarketId, MarketOutcomeId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_position_ledger::PositionLedger;
use rust_decimal_macros::dec;
use time::OffsetDateTime;

// ── Fixtures ──────────────────────────────────────────────────────────────────

fn wallet_a() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn wallet_b() -> WalletAddress {
    serde_json::from_str("\"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"").unwrap()
}

fn market_1() -> MarketId {
    MarketId(VenueMarketId("0xmarket0001".to_string()))
}

fn make_trade(wallet: WalletAddress, side: Side, contracts: u64, ts_unix: i64) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(ts_unix).unwrap();
    IncomingTrade {
        wallet,
        market_id: market_1(),
        outcome_id: OutcomeId(0),
        side,
        price: Price(dec!(0.55)),
        contracts: ContractQty(contracts),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(format!("{wallet:?}-{side:?}-{contracts}-{ts_unix}")),
        provenance: TradeProvenance::RestPoll,
    }
}

fn state(ledger: &PositionLedger, wallet: &WalletAddress) -> (u64, u64) {
    let key = MarketOutcomeId::new(market_1(), OutcomeId(0));
    ledger
        .position(wallet)
        .and_then(|snap| snap.positions.get(&key).copied())
        .map(|s| (s.long_contracts, s.short_contracts))
        .unwrap_or((0, 0))
}

// ── Scenario ──────────────────────────────────────────────────────────────────
//
// Wallet A:
//   t=1000 buy  10 → long=10, short=0
//   t=1001 sell  3 → long=7,  short=0  (partial trim)
//   t=1002 sell 10 → long=0,  short=3  (flip to short)
//   t=1003 buy   1 → long=0,  short=2  (cover 1 of the short)
//   t=1004 buy   5 → long=3,  short=0  (cover remainder, open long)
//
// Wallet B (independent):
//   t=1000 buy 5 → long=5, short=0
//   wallet A state unchanged by wallet B trades

#[test]
fn scenario_position_evolution() {
    let mut ledger = PositionLedger::new();
    let a = wallet_a();
    let b = wallet_b();

    // t=1000: wallet A opens long
    ledger.ingest(&make_trade(a, Side::Buy, 10, 1_000));
    assert_eq!(state(&ledger, &a), (10, 0), "t=1000 after buy 10");
    assert_eq!(state(&ledger, &b), (0, 0), "t=1000 wallet B unaffected");

    // t=1001: wallet A trims
    ledger.ingest(&make_trade(a, Side::Sell, 3, 1_001));
    assert_eq!(state(&ledger, &a), (7, 0), "t=1001 after sell 3");

    // t=1002: wallet A sells more than long — flips to short
    ledger.ingest(&make_trade(a, Side::Sell, 10, 1_002));
    assert_eq!(state(&ledger, &a), (0, 3), "t=1002 after sell 10 (flip)");

    // t=1003: wallet A covers 1 contract of short
    ledger.ingest(&make_trade(a, Side::Buy, 1, 1_003));
    assert_eq!(state(&ledger, &a), (0, 2), "t=1003 after buy 1 (cover 1)");

    // t=1004: wallet A buys 5 — covers remaining 2 shorts and opens 3 long
    ledger.ingest(&make_trade(a, Side::Buy, 5, 1_004));
    assert_eq!(
        state(&ledger, &a),
        (3, 0),
        "t=1004 after buy 5 (cover+open)"
    );

    // wallet B independent trade
    ledger.ingest(&make_trade(b, Side::Buy, 5, 1_000));
    assert_eq!(state(&ledger, &b), (5, 0), "wallet B long=5");
    // wallet A state unchanged by wallet B
    assert_eq!(
        state(&ledger, &a),
        (3, 0),
        "wallet A unaffected by wallet B"
    );
}

#[test]
fn scenario_unknown_wallet_returns_none() {
    let ledger = PositionLedger::new();
    let unknown = wallet_a();
    assert!(
        ledger.position(&unknown).is_none(),
        "unobserved wallet must return None"
    );
}
