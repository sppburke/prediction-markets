//! Scenario: cluster observation tracker — intra-operator coordination within window.
//!
//! Fixtures represent two wallets from the same operator buying the same outcome.
//!
//! PASS: ClusterObs includes all in-window entries; out-of-window entries pruned;
//!       different-operator entries never appear in another operator's ClusterObs.
//! FAIL: any assertion mismatch, or a panic.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_position_ledger::ClusterObservationTracker;
use rust_decimal_macros::dec;
use time::OffsetDateTime;

// ── Fixtures ──────────────────────────────────────────────────────────────────
// Operators are derived from blake3 hashes; use fixed test hashes.

fn operator_id(seed: u8) -> pe_core_types::OperatorId {
    let hash = blake3::hash(&[seed; 32]);
    pe_core_types::OperatorId(hash)
}

fn wallet(hex: u8) -> WalletAddress {
    let addr = format!("\"0x{:0>40}\"", hex);
    serde_json::from_str(&addr).unwrap()
}

fn market_1() -> MarketId {
    MarketId(VenueMarketId("0xmarket0001".to_string()))
}

fn make_trade(w: WalletAddress, side: Side, ts_unix: i64) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(ts_unix).unwrap();
    IncomingTrade {
        wallet: w,
        market_id: market_1(),
        outcome_id: OutcomeId(0),
        side,
        price: Price(dec!(0.65)),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(format!("t-{:?}-{ts_unix}", w)),
    }
}

// ── Scenario 1: two wallets of same operator within window ────────────────────
//
// PASS: ClusterObs returned after the second wallet's ingest contains both entries.
// FAIL: None returned or only 1 entry.

#[test]
fn scenario_cluster_two_wallets_same_operator() {
    let window_secs = 300_u64;
    let mut tracker = ClusterObservationTracker::new(window_secs);
    let op1 = operator_id(1);
    let wa = wallet(0xAA);
    let wb = wallet(0xBB);

    let trade_a = make_trade(wa, Side::Buy, 1_000_000);
    let trade_b = make_trade(wb, Side::Buy, 1_000_100); // 100 s later

    // Wallet A enters first
    tracker.ingest(&trade_a, op1);
    // Only 1 entry — obs is Some but single-entry
    let obs = tracker.cluster_obs_for(&trade_a, op1).unwrap();
    assert_eq!(obs.wallet_entries.len(), 1, "one entry after first wallet");

    // Wallet B enters — both should appear
    tracker.ingest(&trade_b, op1);
    let obs = tracker.cluster_obs_for(&trade_b, op1).unwrap();
    assert_eq!(
        obs.wallet_entries.len(),
        2,
        "two entries after second wallet"
    );
    assert_eq!(obs.operator_id, op1);
    assert_eq!(obs.side, Side::Buy);
}

// ── Scenario 2: entries outside the window are pruned ────────────────────────
//
// Window = 300 s. Trade A at t=0, Trade B at t=400. After B is ingested, A is pruned.
//
// PASS: ClusterObs after B only contains B's entry.
// FAIL: A still appears.

#[test]
fn scenario_cluster_old_entry_pruned() {
    let window_secs = 300_u64;
    let mut tracker = ClusterObservationTracker::new(window_secs);
    let op1 = operator_id(1);
    let wa = wallet(0xAA);
    let wb = wallet(0xBB);

    let trade_a = make_trade(wa, Side::Buy, 0); // t=0
    let trade_b = make_trade(wb, Side::Buy, 400); // t=400 (outside 300 s window from A)

    tracker.ingest(&trade_a, op1);
    tracker.ingest(&trade_b, op1); // prunes A (cutoff = 400 - 300 = 100 > 0)

    let obs = tracker.cluster_obs_for(&trade_b, op1).unwrap();
    assert_eq!(obs.wallet_entries.len(), 1, "old entry should be pruned");
    assert_eq!(obs.wallet_entries[0].wallet, wb);
}

// ── Scenario 3: different operators are isolated ──────────────────────────────
//
// PASS: op2's trade does not appear in op1's ClusterObs.
// FAIL: entries bleed across operators.

#[test]
fn scenario_cluster_operators_isolated() {
    let window_secs = 300_u64;
    let mut tracker = ClusterObservationTracker::new(window_secs);
    let op1 = operator_id(1);
    let op2 = operator_id(2);
    let wa = wallet(0xAA);
    let wb = wallet(0xBB);

    let trade_a = make_trade(wa, Side::Buy, 1_000_000);
    let trade_b = make_trade(wb, Side::Buy, 1_000_000);

    tracker.ingest(&trade_a, op1);
    tracker.ingest(&trade_b, op2);

    // op1 only sees wallet A
    let obs1 = tracker.cluster_obs_for(&trade_a, op1).unwrap();
    assert_eq!(obs1.wallet_entries.len(), 1);
    assert_eq!(obs1.wallet_entries[0].wallet, wa);

    // op2 only sees wallet B
    let obs2 = tracker.cluster_obs_for(&trade_b, op2).unwrap();
    assert_eq!(obs2.wallet_entries.len(), 1);
    assert_eq!(obs2.wallet_entries[0].wallet, wb);
}

// ── Scenario 4: different sides are isolated ──────────────────────────────────

#[test]
fn scenario_cluster_sides_isolated() {
    let window_secs = 300_u64;
    let mut tracker = ClusterObservationTracker::new(window_secs);
    let op1 = operator_id(1);
    let wa = wallet(0xAA);

    let buy_trade = make_trade(wa, Side::Buy, 1_000_000);
    let sell_trade = make_trade(wa, Side::Sell, 1_000_000);

    tracker.ingest(&buy_trade, op1);
    tracker.ingest(&sell_trade, op1);

    let buy_obs = tracker.cluster_obs_for(&buy_trade, op1).unwrap();
    let sell_obs = tracker.cluster_obs_for(&sell_trade, op1).unwrap();

    assert_eq!(buy_obs.wallet_entries.len(), 1);
    assert_eq!(sell_obs.wallet_entries.len(), 1);
    assert!(buy_obs.wallet_entries.iter().all(|e| e.wallet == wa));
    assert!(sell_obs.wallet_entries.iter().all(|e| e.wallet == wa));
}
