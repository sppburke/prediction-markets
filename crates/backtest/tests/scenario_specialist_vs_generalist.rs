//! Scenario: N_eff shrinkage differentiates specialist from generalist.
//!
//! A specialist with 30 wins on 1 market and a generalist with 30 wins on 30 distinct
//! markets have the same raw win rate (100%). With k=6, α=β=10:
//!
//!   specialist: N_eff = min(30, 1×6) = 6
//!               scaled_wins = 30×6/30 = 6.0
//!               shrunk_p = (6.0+10)/(6+20) = 16/26 ≈ 0.615
//!
//!   generalist: N_eff = min(30, 30×6) = 30 (not capped)
//!               scaled_wins = 30×30/30 = 30
//!               shrunk_p = (30+10)/(30+20) = 40/50 = 0.800
//!
//! Both leaders then run 20 shared winning Phase 2 round-trips. The generalist receives
//! a higher Kelly fraction at every copy → larger positions → larger realized PnL.
//!
//! PASS: generalist_pnl > specialist_pnl AND both > 0.
//! FAIL: N_eff shrinkage failed to differentiate the two leaders.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{
    LeaderboardSnapshots, LiquidityIndex, ResolutionIndex, ScheduleIndex, WalletCache,
};
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

// 2023-11-01 00:00:00 UTC
const BASE_UNIX: i64 = 1_698_796_800;
const SPECIALIST_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const GENERALIST_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
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
        source_trade_id: SourceTradeId(format!("0xhash_{market_idx}_{day_offset}_{tx_suffix}")),
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

/// Ranker config matching new canonical defaults: ≥15 closed trades, ≥1 distinct market.
fn relaxed_ranker() -> RankerConfig {
    RankerConfig {
        active_min_closed_trades: 15,
        active_min_distinct_markets: 1,
        active_window_days: 90,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 5,
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
        ranker_active_min_closed: 15,
        ranker_active_min_markets: 1,
        ranker_incubator_min_closed: 5,
        ranker_incubator_min_markets: 1,
        kelly_sweep_fractions: None,
        kelly_p_prior_alpha: 10,
        kelly_p_prior_beta: 10,
        kelly_p_k_per_market: 6,
        // Unlimited cap: Kelly fraction (not bps cap) drives position size.
        liquidity_take_fraction: rust_decimal::Decimal::new(5, 2),
        liquidity_min_required_usd: rust_decimal::Decimal::new(200, 0),
        strategy: WinnerFollowConfig {
            per_trade_cap: PerTradeCap::Unlimited,
            ..WinnerFollowConfig::default()
        },
    }
}

/// Phase 1: 30 wins, all on market 0 (1 distinct market, round-trips every 2 days).
/// Phase 2: 20 winning round-trips on markets 100–119, starting at day 62.
fn specialist_trades(leader: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..30 {
        trades.push(make_trade(leader, 0, i * 2, Side::Buy, dec!(0.35)));
        trades.push(make_trade(leader, 0, i * 2 + 1, Side::Sell, dec!(0.75)));
    }
    for i in 0u32..20 {
        let buy_day = 62 + i * 2;
        trades.push(make_trade(leader, 100 + i, buy_day, Side::Buy, dec!(0.35)));
        trades.push(make_trade(
            leader,
            100 + i,
            buy_day + 1,
            Side::Sell,
            dec!(0.75),
        ));
    }
    trades
}

/// Phase 1: 30 wins on 30 distinct markets (market i+1 for win i, round-trips every 2 days).
/// Phase 2: 20 winning round-trips on markets 100–119, starting at day 62.
fn generalist_trades(leader: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..30 {
        trades.push(make_trade(leader, i + 1, i * 2, Side::Buy, dec!(0.35)));
        trades.push(make_trade(leader, i + 1, i * 2 + 1, Side::Sell, dec!(0.75)));
    }
    for i in 0u32..20 {
        let buy_day = 62 + i * 2;
        trades.push(make_trade(leader, 100 + i, buy_day, Side::Buy, dec!(0.35)));
        trades.push(make_trade(
            leader,
            100 + i,
            buy_day + 1,
            Side::Sell,
            dec!(0.75),
        ));
    }
    trades
}

/// Run a single-leader simulation and return realized PnL.
///
/// `kelly_fraction_override = 0.01` scales both runs into [0, 200 bps) so
/// concentration caps do not block either leader. PnL differences arise solely
/// from the N_eff-derived shrunk_p.
async fn run_leader(
    trades: Vec<RawTrade>,
    leader: WalletAddress,
    funder: WalletAddress,
) -> Decimal {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let timeline = make_timeline(&dir, &[(leader, funder)]);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        per_trade_cap: PerTradeCap::Unlimited,
        kelly_fraction_override: Some(KellyFraction(dec!(0.01))),
        ..WinnerFollowConfig::default()
    });

    run_simulation(
        &base_config(&dir),
        trades,
        &timeline,
        &snapshots,
        &resolutions,
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &strategy,
        false,
    )
    .unwrap()
    .total_pnl_usd
}

/// PASS: generalist_pnl > specialist_pnl AND both > 0.
#[tokio::test]
async fn generalist_outperforms_specialist_via_n_eff() {
    let funder = wallet(FUNDER_HEX);
    let specialist = wallet(SPECIALIST_HEX);
    let generalist = wallet(GENERALIST_HEX);

    let specialist_pnl = run_leader(specialist_trades(specialist), specialist, funder).await;
    let generalist_pnl = run_leader(generalist_trades(generalist), generalist, funder).await;

    assert!(
        specialist_pnl > Decimal::ZERO,
        "specialist should still trade and generate positive PnL (got {specialist_pnl})"
    );
    assert!(
        generalist_pnl > Decimal::ZERO,
        "generalist should generate positive PnL (got {generalist_pnl})"
    );
    assert!(
        generalist_pnl > specialist_pnl,
        "generalist PnL ({generalist_pnl}) should exceed specialist PnL ({specialist_pnl}): \
         N_eff=30 vs N_eff=6 → higher shrunk_p → larger Kelly fraction"
    );
}
