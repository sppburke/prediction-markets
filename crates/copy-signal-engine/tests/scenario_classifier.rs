// Scenario tests for the copy-signal classifier.
// Run with: cargo nextest run -p pe-copy-signal-engine --features scenario
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use pe_copy_signal_engine::{
    IncomingTrade, PositionSnapshot, PositionState, SignalConfig, classify_trade,
};
use pe_core_types::{BasisPoints, SourceTimestamp};
use pe_core_types::{
    ContractQty, LeaderAction, MarketId, MarketOutcomeId, OutcomeId, Price, ReconstructionQuality,
    Side, SourceTradeId, VenueId, VenueMarketId, WalletAddress,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal_macros::dec;
use time::macros::datetime;

// ─── helpers ─────────────────────────────────────────────────────────────────

// Frozen observation: 2024-07-01 00:00:00 UTC
const NOW: time::OffsetDateTime = datetime!(2024-07-01 00:00:00 UTC);

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn market(n: u8) -> MarketId {
    MarketId(VenueMarketId(format!("mkt-{n:03}")))
}

fn quality(q: u8) -> ReconstructionQuality {
    ReconstructionQuality::new(q).expect("quality in 0..=100")
}

fn trade_id(n: u32) -> SourceTradeId {
    SourceTradeId(format!("tid-{n:06}"))
}

fn price(d: rust_decimal::Decimal) -> Price {
    Price::new(d).expect("valid price")
}

/// Build a minimal active-watchlist with a single wallet entry.
fn active_watchlist(w: WalletAddress) -> Watchlist {
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet: w,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(500),
            lcb_5pct_bps: BasisPoints(200),
            win_rate_bps: BasisPoints(9_500),
            closed_trades_in_window: 65,
            reconstruction_quality: quality(100),
        }],
        snapshot_at: SourceTimestamp(datetime!(2024-07-01 00:00:00 UTC)),
        active_count: 1,
        incubator_count: 0,
    }
}

fn empty_watchlist() -> Watchlist {
    Watchlist {
        entries: vec![],
        snapshot_at: SourceTimestamp(datetime!(2024-07-01 00:00:00 UTC)),
        active_count: 0,
        incubator_count: 0,
    }
}

fn incoming(w: WalletAddress, mkt: MarketId, side: Side, contracts: u64) -> IncomingTrade {
    IncomingTrade {
        wallet: w,
        market_id: mkt,
        outcome_id: OutcomeId(0),
        side,
        price: price(dec!(0.50)),
        contracts: ContractQty(contracts),
        observed_at: NOW,
        received_at: NOW,
        source_trade_id: trade_id(1),
        provenance: TradeProvenance::RestPoll,
    }
}

// ─── scenario 1 ──────────────────────────────────────────────────────────────

/// Wallet on active watchlist, no prior position.
///
/// PASS: `action == Entry`.
#[test]
fn entry_new_position() {
    let w = wallet(0x01);
    let mkt = market(1);
    let trade = incoming(w, mkt.clone(), Side::Buy, 100);
    let watchlist = active_watchlist(w);

    let signal = classify_trade(
        &trade,
        None,
        &watchlist,
        quality(90),
        VenueId::polymarket(),
        &SignalConfig::default(),
    )
    .expect("active watchlist wallet should produce a signal");

    assert_eq!(signal.action, LeaderAction::Entry);
    assert_eq!(signal.action_confidence_ppm.0, 900_000);
}

// ─── scenario 2 ──────────────────────────────────────────────────────────────

/// Wallet has an existing long; buys more of the same outcome.
///
/// PASS: `action == Add`.
#[test]
fn add_to_existing() {
    let w = wallet(0x02);
    let mkt = market(2);
    let trade = incoming(w, mkt.clone(), Side::Buy, 50);
    let watchlist = active_watchlist(w);

    let mut positions = HashMap::new();
    positions.insert(
        MarketOutcomeId::new(mkt.clone(), OutcomeId(0)),
        PositionState {
            long_contracts: 200,
            short_contracts: 0,
        },
    );
    let position = PositionSnapshot {
        wallet: w,
        positions,
    };

    let signal = classify_trade(
        &trade,
        Some(&position),
        &watchlist,
        quality(100),
        VenueId::polymarket(),
        &SignalConfig::default(),
    )
    .expect("signal expected");

    assert_eq!(signal.action, LeaderAction::Add);
}

// ─── scenario 3 ──────────────────────────────────────────────────────────────

/// Wallet sells 30% of a 1000-contract long position (300 sold, 700 remaining).
///
/// PASS: `action == Trim` (700 / 1000 = 70% remaining > 10% threshold).
#[test]
fn trim_partial_close() {
    let w = wallet(0x03);
    let mkt = market(3);
    let trade = incoming(w, mkt.clone(), Side::Sell, 300);
    let watchlist = active_watchlist(w);

    let mut positions = HashMap::new();
    positions.insert(
        MarketOutcomeId::new(mkt.clone(), OutcomeId(0)),
        PositionState {
            long_contracts: 1_000,
            short_contracts: 0,
        },
    );
    let position = PositionSnapshot {
        wallet: w,
        positions,
    };

    let signal = classify_trade(
        &trade,
        Some(&position),
        &watchlist,
        quality(100),
        VenueId::polymarket(),
        &SignalConfig::default(),
    )
    .expect("signal expected");

    assert_eq!(signal.action, LeaderAction::Trim);
}

// ─── scenario 4 ──────────────────────────────────────────────────────────────

/// Wallet sells 950 of a 1000-contract long position (50 remaining = 5% ≤ 10% threshold).
///
/// PASS: `action == Exit`.
#[test]
fn exit_full_close() {
    let w = wallet(0x04);
    let mkt = market(4);
    let trade = incoming(w, mkt.clone(), Side::Sell, 950);
    let watchlist = active_watchlist(w);

    let mut positions = HashMap::new();
    positions.insert(
        MarketOutcomeId::new(mkt.clone(), OutcomeId(0)),
        PositionState {
            long_contracts: 1_000,
            short_contracts: 0,
        },
    );
    let position = PositionSnapshot {
        wallet: w,
        positions,
    };

    let signal = classify_trade(
        &trade,
        Some(&position),
        &watchlist,
        quality(100),
        VenueId::polymarket(),
        &SignalConfig::default(),
    )
    .expect("signal expected");

    assert_eq!(signal.action, LeaderAction::Exit);
}

// ─── scenario 5 ──────────────────────────────────────────────────────────────

/// Wallet sells 1500 contracts against a 1000-contract long (fully closes long + opens short).
///
/// PASS: `action == Flip`.
#[test]
fn flip_side_reversal() {
    let w = wallet(0x05);
    let mkt = market(5);
    let trade = incoming(w, mkt.clone(), Side::Sell, 1_500);
    let watchlist = active_watchlist(w);

    let mut positions = HashMap::new();
    positions.insert(
        MarketOutcomeId::new(mkt.clone(), OutcomeId(0)),
        PositionState {
            long_contracts: 1_000,
            short_contracts: 0,
        },
    );
    let position = PositionSnapshot {
        wallet: w,
        positions,
    };

    let signal = classify_trade(
        &trade,
        Some(&position),
        &watchlist,
        quality(100),
        VenueId::polymarket(),
        &SignalConfig::default(),
    )
    .expect("signal expected");

    assert_eq!(signal.action, LeaderAction::Flip);
}

// ─── scenario 6 ──────────────────────────────────────────────────────────────

/// Wallet is NOT on the active watchlist. The watchlist is now the sole eligibility
/// gate, so the trade is suppressed.
///
/// PASS: `classify_trade` returns `None`.
#[test]
fn non_watchlisted_wallet_suppressed() {
    let w = wallet(0x06);
    let mkt = market(6);
    let trade = incoming(w, mkt.clone(), Side::Buy, 100);
    let watchlist = empty_watchlist();

    let result = classify_trade(
        &trade,
        None,
        &watchlist,
        quality(90),
        VenueId::polymarket(),
        &SignalConfig::default(),
    );

    assert!(
        result.is_none(),
        "non-watchlisted wallet must be suppressed; got {result:?}"
    );
}

// ─── scenario 7 ──────────────────────────────────────────────────────────────

/// Zero reconstruction quality with no position data → Unknown action → signal suppressed.
///
/// PASS: `classify_trade` returns `None`.
#[test]
fn unknown_action_suppressed() {
    let w = wallet(0x08);
    let mkt = market(8);
    let trade = incoming(w, mkt.clone(), Side::Buy, 100);
    let watchlist = active_watchlist(w);

    let result = classify_trade(
        &trade,
        None,
        &watchlist,
        quality(0),
        VenueId::polymarket(),
        &SignalConfig::default(),
    );

    assert!(
        result.is_none(),
        "Unknown action must be suppressed; got {result:?}"
    );
}

// ─── scenario 8 ──────────────────────────────────────────────────────────────

/// Wallet adds to an existing long with reconstruction quality 60 (600_000 ppm < 700_000
/// threshold) → signal suppressed despite an eligible watchlist wallet.
///
/// PASS: `classify_trade` returns `None`.
#[test]
fn add_low_confidence_suppressed() {
    let w = wallet(0x09);
    let mkt = market(9);
    let trade = incoming(w, mkt.clone(), Side::Buy, 50);
    let watchlist = active_watchlist(w);

    let mut positions = HashMap::new();
    positions.insert(
        MarketOutcomeId::new(mkt.clone(), OutcomeId(0)),
        PositionState {
            long_contracts: 200,
            short_contracts: 0,
        },
    );
    let position = PositionSnapshot {
        wallet: w,
        positions,
    };

    // quality 60 → confidence_ppm = 600_000 < add_high_confidence_threshold_ppm (700_000)
    let result = classify_trade(
        &trade,
        Some(&position),
        &watchlist,
        quality(60),
        VenueId::polymarket(),
        &SignalConfig::default(),
    );

    assert!(
        result.is_none(),
        "Low-confidence Add must be suppressed; got {result:?}"
    );
}

// ─── proptest ────────────────────────────────────────────────────────────────

proptest::proptest! {
    /// classify_trade is deterministic: identical inputs always produce identical outputs.
    #[test]
    fn idempotency_key_stable(quality_val in 1u8..=100u8) {
        let w = wallet(0x20);
        let mkt = market(8);
        let trade = incoming(w, mkt.clone(), Side::Buy, 100);
        let watchlist = active_watchlist(w);
        let q = quality(quality_val);

        let s1 = classify_trade(
            &trade, None, &watchlist, q, VenueId::polymarket(), &SignalConfig::default(),
        );
        let s2 = classify_trade(
            &trade, None, &watchlist, q, VenueId::polymarket(), &SignalConfig::default(),
        );

        let (s1, s2) = (s1.expect("signal expected"), s2.expect("signal expected"));
        proptest::prop_assert_eq!(s1.action, s2.action);
        proptest::prop_assert_eq!(s1.action_confidence_ppm.0, s2.action_confidence_ppm.0);
        proptest::prop_assert_eq!(s1.source_trade_id, s2.source_trade_id);
    }
}
