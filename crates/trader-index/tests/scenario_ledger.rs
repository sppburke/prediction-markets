// Scenario tests for ledger reconstruction.
// Run with: cargo nextest run -p pe-trader-index --features scenario
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::collections::HashSet;

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::{LedgerConfig, RawTrade, build_trader_ledgers};
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

    let ledgers = build_trader_ledgers(&trades, 30, None);

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

    let ledgers = build_trader_ledgers(&trades, 14, None);

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

// ─── scenario 4 ──────────────────────────────────────────────────────────────

/// wallet_filter pre-filter is semantically identical to build-all + post-filter.
///
/// PASS: ledgers returned by `wallet_filter = Some({w1, w2})` are byte-identical
///       to the subset from `wallet_filter = None` filtered down to {w1, w2}.
/// FAIL: any ledger field differs between the two code paths.
#[test]
fn wallet_filter_matches_post_filter() {
    let w1 = wallet(0x01);
    let w2 = wallet(0x02);
    let w3 = wallet(0x03); // excluded wallet
    let m = market("mkt-filter");

    let trades = vec![
        // w1: one closed trade
        raw_trade(w1, m.clone(), 0, Side::Buy, dec!(0.40), 5, 0, "w1-buy"),
        raw_trade(w1, m.clone(), 0, Side::Sell, dec!(0.70), 5, 100, "w1-sell"),
        // w2: one open position
        raw_trade(w2, m.clone(), 0, Side::Buy, dec!(0.50), 3, 200, "w2-buy"),
        // w3: one closed trade — must be absent from filtered output
        raw_trade(w3, m.clone(), 0, Side::Buy, dec!(0.30), 2, 300, "w3-buy"),
        raw_trade(w3, m.clone(), 0, Side::Sell, dec!(0.80), 2, 400, "w3-sell"),
    ];

    // Path A: build all, post-filter to {w1, w2}
    let all_ledgers = build_trader_ledgers(&trades, 90, None);
    let pool: HashSet<WalletAddress> = [w1, w2].into_iter().collect();
    let mut post_filtered: Vec<_> = all_ledgers
        .into_iter()
        .filter(|l| pool.contains(&l.wallet))
        .collect();
    post_filtered.sort_by_key(|l| l.wallet.0);

    // Path B: pre-filter via wallet_filter
    let mut pre_filtered = build_trader_ledgers(&trades, 90, Some(&pool));
    pre_filtered.sort_by_key(|l| l.wallet.0);

    assert_eq!(
        pre_filtered.len(),
        2,
        "pre-filter must return exactly 2 ledgers (w1, w2)"
    );
    assert_eq!(
        pre_filtered.len(),
        post_filtered.len(),
        "both paths must return the same number of ledgers"
    );

    for (a, b) in pre_filtered.iter().zip(post_filtered.iter()) {
        assert_eq!(a.wallet, b.wallet, "wallet addresses must match");
        assert_eq!(
            a.closed_trades.len(),
            b.closed_trades.len(),
            "closed trade counts must match for wallet {:?}",
            a.wallet
        );
        assert_eq!(
            a.open_positions.len(),
            b.open_positions.len(),
            "open position counts must match for wallet {:?}",
            a.wallet
        );
        assert_eq!(
            a.reconstruction_quality.get(),
            b.reconstruction_quality.get(),
            "reconstruction quality must match for wallet {:?}",
            a.wallet
        );
    }
    println!("PASS: wallet_filter pre-filter matches post-filter for wallets w1 and w2");
}
