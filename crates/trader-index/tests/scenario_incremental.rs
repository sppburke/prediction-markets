// Scenario: IncrementalLedger produces byte-identical output to build_trader_ledgers.
//
// PASS: for every wallet, the closed-trade counts, open-position counts, contract
//       quantities, realized PnL, and reconstruction quality match exactly.
// FAIL: any field differs between the incremental and batch paths.
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::collections::{HashMap, HashSet};

use pe_core_types::{
    ContractQty, MarketId, OperatorId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_trader_index::{IncrementalLedger, LedgerConfig, RawTrade, build_trader_ledgers};
use rust_decimal_macros::dec;
use time::macros::datetime;

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn market(s: &str) -> MarketId {
    MarketId(VenueMarketId(s.to_string()))
}

fn ts(offset_secs: i64) -> SourceTimestamp {
    SourceTimestamp(datetime!(2024-01-01 00:00:00 UTC) + time::Duration::seconds(offset_secs))
}

fn raw_trade(
    w: WalletAddress,
    m: MarketId,
    outcome: u16,
    side: Side,
    price_dec: rust_decimal::Decimal,
    contracts: u64,
    offset_secs: i64,
    id: &str,
) -> RawTrade {
    RawTrade {
        wallet: w,
        market_id: m,
        outcome_id: OutcomeId(outcome),
        side,
        price: Price::new(price_dec).expect("price in [0,1]"),
        contracts: ContractQty(contracts),
        timestamp: ts(offset_secs),
        source_trade_id: SourceTradeId(id.to_string()),
    }
}

/// Three wallets, mixed closed and open positions, trades arriving in timestamp order.
///
/// PASS: IncrementalLedger output matches build_trader_ledgers on every field.
#[test]
fn incremental_matches_batch_reconstruction() {
    let w1 = wallet(0x01);
    let w2 = wallet(0x02);
    let w3 = wallet(0x03);
    let ma = market("mkt-A");
    let mb = market("mkt-B");

    // Sorted ascending by offset_secs (invariant for apply_batch).
    let trades = vec![
        // w1: 2 buys, 1 partial sell → 1 closed trade, 1 open position
        raw_trade(w1, ma.clone(), 0, Side::Buy, dec!(0.30), 10, 0, "w1-b1"),
        raw_trade(w1, ma.clone(), 0, Side::Buy, dec!(0.40), 5, 10, "w1-b2"),
        raw_trade(w2, mb.clone(), 0, Side::Buy, dec!(0.50), 8, 20, "w2-b1"),
        raw_trade(w1, ma.clone(), 0, Side::Sell, dec!(0.70), 10, 30, "w1-s1"),
        raw_trade(w3, ma.clone(), 1, Side::Buy, dec!(0.20), 20, 40, "w3-b1"),
        raw_trade(w2, mb.clone(), 0, Side::Sell, dec!(0.80), 8, 50, "w2-s1"),
        raw_trade(w3, ma.clone(), 1, Side::Sell, dec!(0.90), 10, 60, "w3-s1"),
        // w3 second market: fully open
        raw_trade(w3, mb.clone(), 0, Side::Buy, dec!(0.45), 6, 70, "w3-b2"),
    ];

    // ── batch path ──────────────────────────────────────────────────────────
    let batch_ledgers = build_trader_ledgers(&trades, 90, &[], None, &LedgerConfig::default());

    // ── incremental path ────────────────────────────────────────────────────
    let mut incr = IncrementalLedger::new();
    incr.apply_batch(&trades);

    let all_wallets: HashSet<WalletAddress> = [w1, w2, w3].into_iter().collect();
    let wallet_to_op: HashMap<WalletAddress, OperatorId> = HashMap::new();
    let mut incr_ledgers = incr.build_ledgers(Some(&all_wallets), &wallet_to_op, 90);
    incr_ledgers.sort_by_key(|l| l.wallet.0);

    // ── compare ─────────────────────────────────────────────────────────────
    assert_eq!(
        batch_ledgers.len(),
        incr_ledgers.len(),
        "ledger count must match"
    );

    for (b, i) in batch_ledgers.iter().zip(incr_ledgers.iter()) {
        assert_eq!(b.wallet, i.wallet, "wallet mismatch");
        assert_eq!(
            b.closed_trades.len(),
            i.closed_trades.len(),
            "closed_trades.len() mismatch for {:?}",
            b.wallet
        );
        assert_eq!(
            b.open_positions.len(),
            i.open_positions.len(),
            "open_positions.len() mismatch for {:?}",
            b.wallet
        );
        assert_eq!(
            b.reconstruction_quality.get(),
            i.reconstruction_quality.get(),
            "reconstruction_quality mismatch for {:?}",
            b.wallet
        );

        let b_closed_contracts: u64 = b.closed_trades.iter().map(|c| c.contracts.0).sum();
        let i_closed_contracts: u64 = i.closed_trades.iter().map(|c| c.contracts.0).sum();
        assert_eq!(
            b_closed_contracts, i_closed_contracts,
            "closed contract total mismatch for {:?}",
            b.wallet
        );

        let b_open_contracts: u64 = b.open_positions.iter().map(|o| o.contracts.0).sum();
        let i_open_contracts: u64 = i.open_positions.iter().map(|o| o.contracts.0).sum();
        assert_eq!(
            b_open_contracts, i_open_contracts,
            "open contract total mismatch for {:?}",
            b.wallet
        );

        let b_pnl: rust_decimal::Decimal = b.closed_trades.iter().map(|c| c.realized_pnl_usd).sum();
        let i_pnl: rust_decimal::Decimal = i.closed_trades.iter().map(|c| c.realized_pnl_usd).sum();
        assert_eq!(b_pnl, i_pnl, "total PnL mismatch for {:?}", b.wallet);
    }

    println!(
        "PASS: IncrementalLedger output matches build_trader_ledgers for {} wallets",
        batch_ledgers.len()
    );
}

/// Incremental apply in two separate batches produces the same result as one batch.
///
/// PASS: split-batch output is identical to single-batch output.
#[test]
fn incremental_split_batch_matches_single() {
    let w = wallet(0x04);
    let m = market("mkt-C");

    let trades = vec![
        raw_trade(w, m.clone(), 0, Side::Buy, dec!(0.30), 10, 0, "b1"),
        raw_trade(w, m.clone(), 0, Side::Buy, dec!(0.40), 5, 100, "b2"),
        raw_trade(w, m.clone(), 0, Side::Sell, dec!(0.70), 8, 200, "s1"),
        raw_trade(w, m.clone(), 0, Side::Sell, dec!(0.80), 7, 300, "s2"),
    ];

    let wallet_to_op: HashMap<WalletAddress, OperatorId> = HashMap::new();

    // Single batch.
    let mut incr_single = IncrementalLedger::new();
    incr_single.apply_batch(&trades);
    let single = incr_single.build_ledgers(None, &wallet_to_op, 90);

    // Two halves.
    let mut incr_split = IncrementalLedger::new();
    incr_split.apply_batch(&trades[..2]);
    incr_split.apply_batch(&trades[2..]);
    let split = incr_split.build_ledgers(None, &wallet_to_op, 90);

    assert_eq!(single.len(), split.len());
    let s = &single[0];
    let d = &split[0];
    assert_eq!(s.closed_trades.len(), d.closed_trades.len());
    assert_eq!(s.open_positions.len(), d.open_positions.len());
    assert_eq!(
        s.reconstruction_quality.get(),
        d.reconstruction_quality.get()
    );
    let s_pnl: rust_decimal::Decimal = s.closed_trades.iter().map(|c| c.realized_pnl_usd).sum();
    let d_pnl: rust_decimal::Decimal = d.closed_trades.iter().map(|c| c.realized_pnl_usd).sum();
    assert_eq!(s_pnl, d_pnl);

    println!("PASS: split-batch output identical to single-batch output");
}
