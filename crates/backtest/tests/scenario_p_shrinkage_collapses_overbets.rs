//! Scenario: Bayesian shrinkage on `p` collapses overbet positions.
//!
//! Structure:
//! - Phase 1 (days 0–4, markets 0–2): three qualifying wins.  Leader is NOT yet
//!   on the watchlist.  At day 5 the leader has 3 closed winning trades and
//!   qualifies for the active tier (`active_min_closed_trades = 3`).
//! - Phase 2 (days 5–26, markets 3–22): twenty winning round-trips.  Leader IS
//!   on the watchlist.  Both runs copy each buy; positions close two days later.
//!
//! At the time of first copy (day 5), with kelly_fraction_override = 0.01:
//!   - raw p  = 3/3  = 1.0    → f_full = 1.0  → f_live = 1.0 × 0.01 = 1% ≈ 100 bps
//!   - shrunk p = (3+10)/(3+20) = 13/23 ≈ 0.565 → f_full ≈ 0.330 → f_live ≈ 33 bps
//!
//! kelly_fraction_override scales both runs into the [0, 200 bps) market-cap window
//! so concentration caps do not block trades in either run.  The override is the
//! same for both; the observable difference is caused solely by p_shrunk < p_raw.
//!
//! Expected stake ratio ≈ 3×, so realized PnL from 20 wins should be ≥ 2× larger
//! in the raw run.
//!
//! PASS: raw_pnl ≥ 2 × shrunk_pnl (and both > 0).
//! FAIL: shrinkage had no measurable effect on realized PnL.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{LeaderboardSnapshots, ResolutionIndex, ScheduleIndex, WalletCache};
use pe_core_types::{
    ContractQty, KellyFraction, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_strategy_winner_follow::{PerTradeCap, WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{LedgerConfig, RankerConfig, snapshot::RawTrade};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

// Base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;
const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xcccccccccccccccccccccccccccccccccccccccc";

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn make_trade(
    w: WalletAddress,
    market_idx: u32,
    day_offset: u32,
    side: Side,
    price: Decimal,
) -> RawTrade {
    let tx_suffix = if side == Side::Buy { "buy" } else { "sell" };
    RawTrade {
        wallet: w,
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
        source_trade_id: SourceTradeId(format!("0xhash_{market_idx}_{tx_suffix}")),
    }
}

fn make_timeline(dir: &TempDir, pairs: &[(WalletAddress, WalletAddress)]) -> FunderGraphTimeline {
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    for &(funded, funder) in pairs {
        cache
            .insert_funder_edges(funded, &[(funder, 0)], 0)
            .unwrap();
    }
    FunderGraphTimeline::from_cache(&cache).unwrap()
}

/// Relaxed ranker: leader qualifies after 3 closed trades on ≥ 2 distinct markets.
fn relaxed_ranker() -> RankerConfig {
    RankerConfig {
        active_min_closed_trades: 3,
        active_min_distinct_markets: 2,
        active_window_days: 90,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 1,
        incubator_min_distinct_markets: 1,
        incubator_window_days: 60,
        incubator_watchlist_size: 250,
        min_reconstruction_quality: 0,
    }
}

fn base_config(dir: &TempDir) -> BacktestConfig {
    BacktestConfig {
        bootstrap_cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
        step_days: 1,
        dune_api_key: None,
        dune_namespace: None,
        max_hours_to_expiry: None,
        audit_window_days: 90,
        ranker_min_quality: 0,
        ranker_active_min_closed: 3,
        ranker_active_min_markets: 2,
        ranker_incubator_min_closed: 1,
        ranker_incubator_min_markets: 1,
        kelly_sweep_fractions: None,
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
        // Unlimited cap so Kelly fraction (not the bps cap) drives position size.
        strategy: WinnerFollowConfig {
            per_trade_cap: PerTradeCap::Unlimited,
            ..WinnerFollowConfig::default()
        },
    }
}

/// Build the leader's trade history.
///
/// Phase 1 — 3 qualifying wins (markets 0–2, days 0–4).
///   Not copied: leader is below the active tier threshold.
///   After day 5 the leader has 3 closed trades on 3 distinct markets → qualifies.
///
/// Phase 2 — 20 winning round-trips (markets 3–22, days 5–44).
///   Copied: leader is on the watchlist.  Each BUY is followed by a SELL two days later
///   so positions close within the simulation window, generating realized PnL.
fn generate_leader_trades(leader: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();

    // Phase 1: qualifying wins.  buy day i, sell day i+2.
    for i in 0u32..3 {
        trades.push(make_trade(leader, i, i, Side::Buy, dec!(0.35)));
        trades.push(make_trade(leader, i, i + 2, Side::Sell, dec!(0.75)));
    }

    // Phase 2: signal trades starting at day 5.  buy day 5+i, sell day 7+i.
    for i in 0u32..20 {
        let market = 3 + i;
        let buy_day = 5 + i;
        let sell_day = buy_day + 2;
        trades.push(make_trade(leader, market, buy_day, Side::Buy, dec!(0.35)));
        trades.push(make_trade(leader, market, sell_day, Side::Sell, dec!(0.75)));
    }

    trades
}

/// Run the simulation with the given prior and return realized PnL.
async fn run_with_prior(alpha: u32, beta: u32) -> Decimal {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let leader = wallet(LEADER_HEX);
    let funder = wallet(FUNDER_HEX);
    let trades = generate_leader_trades(leader);

    let timeline = make_timeline(&dir, &[(leader, funder)]);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let ranker_config = relaxed_ranker();
    let ledger_config = LedgerConfig::default();
    // kelly_fraction_override = 0.01 scales both runs into the [0, 200 bps) market-cap
    // window so concentration caps do not block either run.  The observable PnL
    // difference is caused solely by p_shrunk < p_raw (see module doc).
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        per_trade_cap: PerTradeCap::Unlimited,
        kelly_fraction_override: Some(KellyFraction(dec!(0.01))),
        ..WinnerFollowConfig::default()
    });

    let config = BacktestConfig {
        kelly_p_prior_alpha: alpha,
        kelly_p_prior_beta: beta,
        ..base_config(&dir)
    };

    run_simulation(
        &config,
        trades,
        &timeline,
        &snapshots,
        &resolutions,
        &ScheduleIndex::new(),
        &ranker_config,
        &ledger_config,
        &strategy,
        false,
    )
    .unwrap()
    .total_pnl_usd
}

/// PASS: raw_pnl ≥ 2 × shrunk_pnl and both are positive.
/// FAIL: shrinkage had no measurable effect on realized PnL.
#[tokio::test]
async fn shrinkage_collapses_overbets() {
    // Raw path: (0,0) → p = 3/3 = 1.0 → aggressive Kelly sizing.
    let raw_pnl = run_with_prior(0, 0).await;

    // Shrunk path: (10,10) → p = 13/23 ≈ 0.565 → much smaller positions.
    let shrunk_pnl = run_with_prior(10, 10).await;

    assert!(
        raw_pnl > Decimal::ZERO,
        "raw-rate run should generate positive PnL (got {raw_pnl})"
    );
    assert!(
        shrunk_pnl > Decimal::ZERO,
        "shrunk run should also generate positive PnL (got {shrunk_pnl})"
    );
    assert!(
        raw_pnl >= shrunk_pnl * dec!(2),
        "expected raw PnL ({raw_pnl}) ≥ 2× shrunk PnL ({shrunk_pnl})"
    );
}
