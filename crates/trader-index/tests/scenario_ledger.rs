// Scenario tests for ledger reconstruction.
// Run with: cargo nextest run -p pe-trader-index --features scenario
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::collections::HashMap;

use pe_core_types::{
    ContractQty, MarketId, OperatorId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_operator_graph::{
    clustering::ClusteringConfig,
    funding::{FundingEdge, FundingSnapshot},
};
use pe_trader_index::{LedgerConfig, RawTrade, TradeSnapshot, build_trader_ledgers};
use rust_decimal_macros::dec;
use time::macros::datetime;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn market(s: &str) -> MarketId {
    MarketId(VenueMarketId(s.to_string()))
}

fn trade_id(s: &str) -> SourceTradeId {
    SourceTradeId(s.to_string())
}

fn ts(offset_secs: i64) -> SourceTimestamp {
    // Frozen base: 2024-01-01 00:00:00 UTC
    SourceTimestamp(datetime!(2024-01-01 00:00:00 UTC) + time::Duration::seconds(offset_secs))
}

fn raw_trade(
    wallet: WalletAddress,
    market_id: MarketId,
    outcome: u16,
    side: Side,
    price_dec: rust_decimal::Decimal,
    contracts: u64,
    offset_secs: i64,
    id: &str,
) -> RawTrade {
    RawTrade {
        wallet,
        market_id,
        outcome_id: OutcomeId(outcome),
        side,
        price: Price::new(price_dec).expect("price in [0,1]"),
        contracts: ContractQty(contracts),
        timestamp: ts(offset_secs),
        source_trade_id: trade_id(id),
    }
}

// ─── scenario 1 ──────────────────────────────────────────────────────────────

/// Single wallet: 2 buys then 2 matching sells.
///
/// PASS: 2 ClosedTrade records with correct PnL, 0 open positions,
///       reconstruction_quality = 100.
#[test]
fn single_wallet_fully_closed() {
    let w = wallet(0x01);
    let m = market("mkt-A");

    let trades = vec![
        raw_trade(w, m.clone(), 0, Side::Buy, dec!(0.40), 10, 0, "buy-1"),
        raw_trade(w, m.clone(), 0, Side::Buy, dec!(0.50), 5, 100, "buy-2"),
        raw_trade(w, m.clone(), 0, Side::Sell, dec!(0.70), 10, 200, "sell-1"),
        raw_trade(w, m.clone(), 0, Side::Sell, dec!(0.80), 5, 300, "sell-2"),
    ];

    let snapshot = TradeSnapshot {
        trades,
        snapshot_at: ts(400),
        audit_window_days: 30,
    };

    let ledgers = build_trader_ledgers(&snapshot, &[], &LedgerConfig::default());

    assert_eq!(ledgers.len(), 1, "expected one ledger");
    let ledger = &ledgers[0];
    assert_eq!(ledger.wallet, w);
    assert_eq!(
        ledger.open_positions.len(),
        0,
        "all positions should be closed"
    );
    assert_eq!(ledger.closed_trades.len(), 2, "expected 2 closed trades");
    assert_eq!(ledger.reconstruction_quality.get(), 100);
    assert!(ledger.operator_id.is_none());

    // First closed trade: 10 contracts @ 0.40 entry, 0.70 exit → pnl = 0.30 * 10 = 3.00
    let ct0 = &ledger.closed_trades[0];
    assert_eq!(ct0.contracts.0, 10);
    assert_eq!(ct0.entry_price.0, dec!(0.40));
    assert_eq!(ct0.exit_price.0, dec!(0.70));
    assert_eq!(ct0.realized_pnl_usd, dec!(3.00));
    assert_eq!(ct0.hold_duration_seconds, 200);

    // Second closed trade: 5 contracts @ 0.50 entry, 0.80 exit → pnl = 0.30 * 5 = 1.50
    let ct1 = &ledger.closed_trades[1];
    assert_eq!(ct1.contracts.0, 5);
    assert_eq!(ct1.entry_price.0, dec!(0.50));
    assert_eq!(ct1.exit_price.0, dec!(0.80));
    assert_eq!(ct1.realized_pnl_usd, dec!(1.50));
}

// ─── scenario 2 ──────────────────────────────────────────────────────────────

/// Operator-merged cluster: 2 wallets share one OperatorIdentity with high confidence.
///
/// PASS: both TraderLedger records have operator_id = Some(expected_operator_id).
#[test]
fn operator_merged_cluster_annotated() {
    let root = wallet(0xA0);
    let w1 = wallet(0xA1);
    let w2 = wallet(0xA2);
    let m = market("mkt-B");

    // Build a minimal FundingSnapshot so the operator_graph can produce an OperatorIdentity.
    let edge = |funder: WalletAddress, funded: WalletAddress| FundingEdge {
        funder,
        funded,
        amount_usd: dec!(1000),
        timestamp: ts(-86_400), // 1 day before snapshot
    };

    let mut wallet_ages = HashMap::new();
    wallet_ages.insert(root, 365 * 86_400u32);
    wallet_ages.insert(w1, 180 * 86_400u32);
    wallet_ages.insert(w2, 180 * 86_400u32);

    let funding_snap = FundingSnapshot {
        edges: vec![edge(root, w1), edge(root, w2)],
        wallet_ages,
        known_external: HashMap::new(),
        closed_trade_counts: HashMap::new(),
        realized_pnl_usd: HashMap::new(),
        snapshot_at: ts(0),
    };

    let identities = pe_operator_graph::clustering::build_operator_identities(
        &funding_snap,
        &ClusteringConfig::default(),
    )
    .expect("clustering should succeed");

    assert_eq!(
        identities.len(),
        1,
        "should produce exactly one OperatorIdentity"
    );
    let expected_op_id: OperatorId = identities[0].operator_id;

    // Give each wallet one trade.
    let trades = vec![
        raw_trade(w1, m.clone(), 0, Side::Buy, dec!(0.55), 3, 0, "w1-buy"),
        raw_trade(w2, m.clone(), 0, Side::Buy, dec!(0.60), 4, 10, "w2-buy"),
    ];

    let snapshot = TradeSnapshot {
        trades,
        snapshot_at: ts(100),
        audit_window_days: 7,
    };

    let ledgers = build_trader_ledgers(&snapshot, &identities, &LedgerConfig::default());

    assert_eq!(ledgers.len(), 2, "expected two ledgers (one per wallet)");
    for ledger in &ledgers {
        assert_eq!(
            ledger.operator_id,
            Some(expected_op_id),
            "wallet {:?} should be annotated with the operator_id",
            ledger.wallet
        );
    }
}

// ─── scenario 3 ──────────────────────────────────────────────────────────────

/// Fresh wallet: fewer than 2 closed trades, some still open.
///
/// PASS: ledger has < 2 closed trades and reconstruction_quality < 100
///       (open positions exist so not all contracts are settled).
#[test]
fn fresh_wallet_partial_reconstruction() {
    let w = wallet(0x02);
    let m = market("mkt-C");

    // 3 buys, only 1 matching sell → 1 closed trade, 2 contracts still open.
    let trades = vec![
        raw_trade(w, m.clone(), 0, Side::Buy, dec!(0.30), 5, 0, "buy-a"),
        raw_trade(w, m.clone(), 0, Side::Buy, dec!(0.35), 3, 50, "buy-b"),
        raw_trade(w, m.clone(), 0, Side::Sell, dec!(0.60), 5, 100, "sell-a"),
    ];

    let snapshot = TradeSnapshot {
        trades,
        snapshot_at: ts(200),
        audit_window_days: 14,
    };

    let ledgers = build_trader_ledgers(&snapshot, &[], &LedgerConfig::default());

    assert_eq!(ledgers.len(), 1);
    let ledger = &ledgers[0];

    // Fewer than 2 closed trades — this is a "fresh wallet".
    assert!(
        (ledger.closed_trades.len() as u32)
            < LedgerConfig::default().fresh_wallet_max_closed_trades + 1,
        "should have ≤ fresh_wallet_max_closed_trades closed trades; got {}",
        ledger.closed_trades.len()
    );
    assert_eq!(
        ledger.closed_trades.len(),
        1,
        "only 1 sell, so only 1 closed trade"
    );

    // 3 remaining buy contracts are open.
    assert_eq!(
        ledger.open_positions.len(),
        1,
        "remaining buy contracts should be open"
    );
    assert_eq!(
        ledger.open_positions[0].contracts.0, 3,
        "3 contracts still open"
    );

    // quality = 5 closed / (5 + 3) total = 62 → < 100 but above research-only threshold
    let q = ledger.reconstruction_quality.get();
    assert!(
        q < 100,
        "quality should be < 100 with open positions; got {q}"
    );
    assert!(
        q > 0,
        "quality should be > 0 with at least one closed trade; got {q}"
    );
}
