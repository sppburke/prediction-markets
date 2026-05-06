//! Scenario: walk-forward backtest with synthetic 60-day fixture.
//!
//! PASS: a known-winner wallet (65 markets, 100% win rate) appears in the ranker and
//!       produces at least one copy trade with positive total PnL.
//! FAIL: no copies produced, or total PnL is not positive.
//!
//! No network calls — trades are generated in-process. Clock is fixed via hardcoded
//! timestamps. RNG is not used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{HashMap, HashSet};

use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_core_types::{
    ClusterSize, ContractQty, FunderRootId, FundingHopCount, MarketId, OperatorId, OutcomeId,
    Price, ReconstructionQuality, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_operator_graph::OperatorIdentity;
use pe_strategy_winner_follow::WinnerFollowConfig;
use pe_trader_index::{LedgerConfig, RankerConfig, snapshot::RawTrade};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const WINNER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xcccccccccccccccccccccccccccccccccccccccc";
// Base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

/// Build a RawTrade for the winner at a given day offset, market index, and side.
///
/// Winner buys at 0.35 (day 0..60 alternating) and sells at 0.75 two days later.
fn winner_trade(winner: WalletAddress, market_idx: u32, day_offset: u32, side: Side) -> RawTrade {
    let price = match side {
        Side::Buy => dec!(0.35),
        Side::Sell => dec!(0.75),
    };
    RawTrade {
        wallet: winner,
        market_id: MarketId(VenueMarketId(format!("0xcond{market_idx:04}"))),
        outcome_id: OutcomeId(0),
        side,
        price: Price::new(price).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(
            OffsetDateTime::from_unix_timestamp(
                BASE_UNIX + i64::from(day_offset) * 86_400 + i64::from(market_idx),
            )
            .unwrap(),
        ),
        source_trade_id: SourceTradeId(format!(
            "0xhash_w{market_idx}_{}",
            if side == Side::Buy { "buy" } else { "sell" }
        )),
    }
}

/// Construct a minimal OperatorIdentity for the winner wallet so that
/// `proxy_funder_mapping_proven = true` in the risk snapshot.
fn winner_operator_identity(winner: WalletAddress, funder: WalletAddress) -> OperatorIdentity {
    let operator_id = OperatorId(blake3::hash(b"test-operator"));
    let funder_root = FunderRootId(funder);
    OperatorIdentity {
        operator_id,
        funder_root,
        member_wallets: vec![winner],
        hop_counts: HashMap::from([(winner, FundingHopCount(1))]),
        confidence_ppm: 900_000,
        reconstruction_quality: ReconstructionQuality::new(80).unwrap(),
        cluster_size: ClusterSize(1),
        anti_gaming_flags: HashSet::new(),
    }
}

/// Generate 65 markets × 2 trades (BUY on day D, SELL on day D+2).
///
/// Days 0..65: BUY for each of the 65 markets.
/// Days 2..67: SELL for each of the 65 markets.
/// Total: 130 raw trades. Ledger reconstruction will produce ≥65 closed trades
/// once the SELL trades are visible to the ranker, satisfying `active_min_closed_trades = 60`.
fn generate_winner_trades(winner: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..65 {
        trades.push(winner_trade(winner, i, i, Side::Buy));
        trades.push(winner_trade(winner, i, i + 2, Side::Sell));
    }
    trades
}

#[tokio::test]
async fn winner_wallet_produces_positive_pnl() {
    let winner = wallet(WINNER_HEX);
    let funder = wallet(FUNDER_HEX);

    let all_trades = generate_winner_trades(winner);
    let operator_identities = vec![winner_operator_identity(winner, funder)];

    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
        step_days: 1,
        etherscan_api_key: None,
        audit_window_days: 90,
    };

    // Use relaxed ranker thresholds to allow the winner to qualify with 65 markets.
    let ranker_config = RankerConfig {
        active_min_closed_trades: 60,
        active_min_distinct_markets: 30,
        active_window_days: 90,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 10,
        incubator_min_distinct_markets: 5,
        incubator_window_days: 60,
        incubator_watchlist_size: 250,
        min_reconstruction_quality: 0,
    };

    let report = run_simulation(
        &config,
        all_trades,
        operator_identities,
        &ranker_config,
        &LedgerConfig::default(),
        &WinnerFollowConfig::default(),
    )
    .unwrap();

    // The winner should have been copied at least once after qualifying for the watchlist.
    assert!(
        report.total_copies > 0,
        "expected at least one copy trade; got 0. Winner may not have passed ranker thresholds."
    );

    // Each copy: BUY at (0.35 + slippage=0.01) = 0.36, SELL at (0.75 - 0.01) = 0.74.
    // PnL per closed round-trip = 100 contracts × (0.74 - 0.36) = $38. Must be positive.
    assert!(
        report.total_pnl_usd > Decimal::ZERO,
        "expected positive PnL; got {}",
        report.total_pnl_usd,
    );
}
