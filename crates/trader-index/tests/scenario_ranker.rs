// Scenario tests for the walk-forward ranker.
// Run with: cargo nextest run -p pe-trader-index --features scenario
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use pe_core_types::{
    ContractQty, MarketId, OperatorId, OutcomeId, Price, ReconstructionQuality, Side,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_trader_index::{ClosedTrade, RankerConfig, TraderLedger, WatchlistTier, build_watchlist};
use rust_decimal_macros::dec;
use time::macros::datetime;

// ─── helpers ─────────────────────────────────────────────────────────────────

// Frozen snapshot: 2024-07-01 00:00:00 UTC → unix = 1_719_792_000
const NOW_UNIX: i64 = 1_719_792_000;

fn snapshot_at() -> SourceTimestamp {
    SourceTimestamp(datetime!(2024-07-01 00:00:00 UTC))
}

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn market(n: u8) -> MarketId {
    MarketId(VenueMarketId(format!("mkt-{n:03}")))
}

fn trade_id(n: u32) -> SourceTradeId {
    SourceTradeId(format!("tid-{n:06}"))
}

fn quality(q: u8) -> ReconstructionQuality {
    ReconstructionQuality::new(q).expect("quality in 0..=100")
}

fn op_id(seed: &[u8]) -> OperatorId {
    OperatorId(blake3::hash(seed))
}

/// Construct a profitable closed trade within the active 180-day window.
///
/// `market_n` selects the market (0–255), `day_offset` places it `day_offset`
/// days before `NOW_UNIX`. Entry 0.40, exit 0.60, 10 contracts → pnl = +2.00.
fn closed_trade(market_n: u8, day_offset: u32, trade_n: u32) -> ClosedTrade {
    let closed_at_unix = NOW_UNIX - (day_offset as i64) * 86_400;
    let opened_at_unix = closed_at_unix - 3_600; // held 1 hour
    ClosedTrade {
        market_id: market(market_n),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        entry_price: Price::new(dec!(0.40)).expect("valid price"),
        exit_price: Price::new(dec!(0.60)).expect("valid price"),
        contracts: ContractQty(10),
        hold_duration_seconds: 3_600,
        realized_pnl_usd: dec!(2.00),
        opened_at_unix,
        closed_at_unix,
        source_trade_ids: vec![trade_id(trade_n), trade_id(trade_n + 1)],
    }
}

// ─── scenario 1 ──────────────────────────────────────────────────────────────

/// Single wallet: 65 closed trades across 32 distinct markets within 180 days, positive pnl.
///
/// PASS: wallet appears in the watchlist with `tier == WatchlistTier::Active`
///       and `leader_score_bps > 0`.
#[test]
fn fully_eligible_active_leader() {
    let w = wallet(0x01);

    // 65 trades spread evenly across 32 markets (markets 0–31 repeated) and 165 days.
    let trades: Vec<ClosedTrade> = (0u32..65)
        .map(|i| {
            let market_n = (i % 32) as u8;
            let day_offset = 1 + i * 2; // days 1, 3, 5, … (all within 180 d)
            closed_trade(market_n, day_offset, i * 2)
        })
        .collect();

    let ledger = TraderLedger {
        wallet: w,
        operator_id: None,
        reconstruction_quality: quality(100),
        closed_trades: trades,
        open_positions: Vec::new(),
        audit_window_days: 180,
    };

    let watchlist = build_watchlist(&[ledger], snapshot_at(), &RankerConfig::default());

    assert_eq!(watchlist.active_count, 1, "expected 1 active entry");
    assert_eq!(watchlist.incubator_count, 0, "expected 0 incubator entries");

    let entry = &watchlist.entries[0];
    assert_eq!(entry.tier, WatchlistTier::Active);
    assert!(
        entry.leader_score_bps.0 > 0,
        "leader_score_bps should be positive for consistently profitable trades, got {}",
        entry.leader_score_bps.0
    );
}

// ─── scenario 2 ──────────────────────────────────────────────────────────────

/// Single wallet: 8 trades across 3 markets — passes incubator thresholds
/// (≥ 5 trades, ≥ 1 market) but not active (needs ≥ 15 trades).
///
/// PASS: wallet appears with `tier == WatchlistTier::Incubator`.
#[test]
fn incubator_only_insufficient_trades() {
    let w = wallet(0x02);

    // 8 trades across 3 markets within 22 days (inside both 60-d incubator and 90-d active windows).
    let trades: Vec<ClosedTrade> = (0u32..8)
        .map(|i| {
            let market_n = (i % 3) as u8;
            let day_offset = 1 + i * 3; // days 1, 4, 7, … (all within 22 d)
            closed_trade(market_n, day_offset, i * 2)
        })
        .collect();

    let ledger = TraderLedger {
        wallet: w,
        operator_id: None,
        reconstruction_quality: quality(90),
        closed_trades: trades,
        open_positions: Vec::new(),
        audit_window_days: 180,
    };

    let watchlist = build_watchlist(&[ledger], snapshot_at(), &RankerConfig::default());

    assert_eq!(watchlist.active_count, 0, "should not qualify as active");
    assert_eq!(watchlist.incubator_count, 1, "expected 1 incubator entry");

    let entry = &watchlist.entries[0];
    assert_eq!(entry.tier, WatchlistTier::Incubator);
}

// ─── scenario 3 ──────────────────────────────────────────────────────────────

/// Two wallets share one operator_id. Each has 35 trades (above the 15-trade active
/// threshold individually, but treated as a single operator entry). Combined they have
/// 70 trades across 32 distinct markets.
///
/// PASS: the operator group appears once with `tier == WatchlistTier::Active`.
#[test]
fn operator_aggregated_crosses_threshold() {
    let oid = op_id(b"test-operator-1");
    let w1 = wallet(0x10);
    let w2 = wallet(0x11);

    // Wallet 1: markets 0–15 (16 markets), 35 trades.
    let trades_w1: Vec<ClosedTrade> = (0u32..35)
        .map(|i| {
            let market_n = (i % 16) as u8;
            let day_offset = 1 + i * 4;
            closed_trade(market_n, day_offset, i * 2)
        })
        .collect();

    // Wallet 2: markets 16–31 (16 distinct markets), 35 trades.
    let trades_w2: Vec<ClosedTrade> = (0u32..35)
        .map(|i| {
            let market_n = 16 + (i % 16) as u8;
            let day_offset = 2 + i * 4;
            closed_trade(market_n, day_offset, 10_000 + i * 2)
        })
        .collect();

    let ledger1 = TraderLedger {
        wallet: w1,
        operator_id: Some(oid),
        reconstruction_quality: quality(95),
        closed_trades: trades_w1,
        open_positions: Vec::new(),
        audit_window_days: 180,
    };

    let ledger2 = TraderLedger {
        wallet: w2,
        operator_id: Some(oid),
        reconstruction_quality: quality(95),
        closed_trades: trades_w2,
        open_positions: Vec::new(),
        audit_window_days: 180,
    };

    let watchlist = build_watchlist(&[ledger1, ledger2], snapshot_at(), &RankerConfig::default());

    assert_eq!(
        watchlist.active_count, 1,
        "operator group should produce exactly 1 active entry"
    );
    assert_eq!(
        watchlist.incubator_count, 0,
        "no separate incubator entry expected"
    );

    let entry = &watchlist.entries[0];
    assert_eq!(entry.tier, WatchlistTier::Active);
    assert_eq!(
        entry.operator_id,
        Some(oid),
        "entry should carry the operator_id"
    );
    // combined 70 trades in window
    assert_eq!(
        entry.closed_trades_in_window, 70,
        "combined trade count should be 70"
    );
}
