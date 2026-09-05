//! Scenario tests for the `min_signal_price` BUY floor (issue #466 follow-up).
//!
//! `BacktestConfig::min_signal_price = Some(floor)` skips any BUY whose
//! slippage-adjusted `fill_price` is `< floor` — the symmetric LOWER band to
//! `max_signal_price`. Together they confine the #421 bake-off's forward copy to
//! the config's criteria price band `[price_min, price_max]`, so the forward
//! evaluation copies under the SAME band the wallets were selected with (without
//! it the forward copy ignored the lower band entirely). Gated on `fill_price`
//! (the price actually paid), in the BUY arm, before the flat-USD / Kelly paths.
//!
//! Scenarios:
//!  1. `floor_blocks_below_threshold` — floor 0.30, signal 0.20, 0% slippage → no BUY.
//!  2. `floor_allows_at_threshold` — floor 0.30, signal 0.30 exactly → BUY fills (`<` is strict).
//!  3. `floor_allows_above_threshold` — floor 0.30, signal 0.45 → BUY fills.
//!  4. `floor_none_allows_all` — `min_signal_price: None`, signal 0.05 → BUY fills (regression guard).
//!  5. `band_confines_to_range` — floor 0.30 + cap 0.70: signal 0.20 blocked, 0.50 fills, 0.80 blocked.
//!  6. `floor_does_not_affect_sells` — leader BUYs at 0.40 (allowed), SELLs at 0.10 (would be < floor
//!     if a BUY) → the SELL still closes the position (gate is BUY-arm only).

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::io::BufRead as _;

use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{LeaderboardSnapshots, LiquidityIndex, ResolutionIndex, ScheduleIndex};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{RankerConfig, snapshot::RawTrade};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const BASE_UNIX: i64 = 1_698_796_800; // 2023-11-01 00:00:00 UTC
const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn mkt(idx: u32) -> MarketId {
    MarketId(VenueMarketId(format!("0xcond{idx:04}")))
}

fn raw_trade(market_idx: u32, day_offset: u32, side: Side, price: Decimal, tag: &str) -> RawTrade {
    let ts = BASE_UNIX + i64::from(day_offset) * 86_400;
    RawTrade {
        wallet: wallet(LEADER_HEX),
        market_id: mkt(market_idx),
        outcome_id: OutcomeId(0),
        side,
        price: Price::new(price).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(format!("0x_{tag}_{market_idx}_{day_offset}")),
    }
}

/// 20 closed round-trips at mid prices (well inside any band under test) so the
/// leader clears the relaxed ranker's 15-closed-trade gate before the test BUY.
fn training_book() -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..20 {
        t.push(raw_trade(100 + i, i, Side::Buy, dec!(0.45), "train"));
        t.push(raw_trade(100 + i, i + 2, Side::Sell, dec!(0.55), "train"));
    }
    t
}

fn relaxed_ranker() -> RankerConfig {
    RankerConfig {
        active_min_closed_trades: 15,
        active_min_distinct_markets: 1,
        active_window_days: 365,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 5,
        incubator_min_distinct_markets: 1,
        incubator_window_days: 365,
        incubator_watchlist_size: 250,
        min_reconstruction_quality: 0,
    }
}

fn base_config(
    dir: &TempDir,
    min_signal_price: Option<Decimal>,
    max_signal_price: Option<Decimal>,
) -> BacktestConfig {
    BacktestConfig {
        bootstrap_cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
        modeled_polymarket_fee_rate: dec!(0.04),
        step_days: 1,
        max_hours_to_expiry: None,
        audit_window_days: 365,
        ranker_min_quality: 0,
        ranker_active_min_closed: 15,
        ranker_active_min_markets: 1,
        ranker_incubator_min_closed: 5,
        ranker_incubator_min_markets: 1,
        kelly_sweep_fractions: None,
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
        kelly_p_min_snapshots: 0,
        kelly_p_extra_per_missing_snapshot: 0,
        liquidity_take_fraction: dec!(0),
        liquidity_min_required_usd: dec!(200),
        flat_usd: Some(dec!(1)), // deterministic 1-contract fills
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: None,
        max_signal_price,
        min_signal_price,
        max_trade_count: 0,
        injected_wallets_path: None,
        mtm_window_start_unix: None,
        mtm_window_end_unix: None,
        strategy: WinnerFollowConfig {
            slippage_rate: dec!(0), // 0 slippage → fill_price == signal price (exact band checks)
            ..WinnerFollowConfig::default()
        },
    }
}

#[derive(Debug, serde::Deserialize)]
struct TradeFillJson {
    market_id: String,
    side: String,
}

/// Run with the training book + a single test BUY on `test_market` at `signal`
/// (on day 25, after the book closes). Returns the parsed BUY fills on that market.
fn buy_fills_for(
    min_signal_price: Option<Decimal>,
    max_signal_price: Option<Decimal>,
    test_market: u32,
    signal: Decimal,
) -> Vec<TradeFillJson> {
    let mut trades = training_book();
    trades.push(raw_trade(test_market, 25, Side::Buy, signal, "test"));
    trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();
    let config = base_config(&dir, min_signal_price, max_signal_price);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    run_simulation(
        &config,
        &trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &strategy,
        true,
    )
    .unwrap();

    let fills_path = dir.path().join("output").join("trades.ndjson");
    let target = mkt(test_market).0.0;
    if !fills_path.exists() {
        return Vec::new();
    }
    let file = std::fs::File::open(&fills_path).unwrap();
    std::io::BufReader::new(file)
        .lines()
        .map(|l| serde_json::from_str::<TradeFillJson>(&l.unwrap()).unwrap())
        .filter(|f| f.market_id == target && f.side == "buy")
        .collect()
}

#[test]
fn floor_blocks_below_threshold() {
    // PASS: floor 0.30, signal 0.20 → 0 BUY fills on the test market.
    let fills = buy_fills_for(Some(dec!(0.30)), None, 900, dec!(0.20));
    assert!(
        fills.is_empty(),
        "signal 0.20 < floor 0.30 must be blocked, got {fills:?}"
    );
}

#[test]
fn floor_allows_at_threshold() {
    // PASS: floor 0.30, signal 0.30 exactly → BUY fills (`<` floor is strict, so == passes).
    let fills = buy_fills_for(Some(dec!(0.30)), None, 901, dec!(0.30));
    assert_eq!(fills.len(), 1, "signal == floor must fill (strict <)");
}

#[test]
fn floor_allows_above_threshold() {
    let fills = buy_fills_for(Some(dec!(0.30)), None, 902, dec!(0.45));
    assert_eq!(fills.len(), 1, "signal 0.45 above floor must fill");
}

#[test]
fn floor_none_allows_all() {
    // Regression guard for the `if let Some(floor)` shape: None → no floor.
    let fills = buy_fills_for(None, None, 903, dec!(0.05));
    assert_eq!(fills.len(), 1, "floor None must allow even a 0.05 signal");
}

#[test]
fn band_confines_to_range() {
    // floor 0.30 + cap 0.70 = the criteria band [0.30, 0.70].
    assert!(
        buy_fills_for(Some(dec!(0.30)), Some(dec!(0.70)), 910, dec!(0.20)).is_empty(),
        "below band → blocked"
    );
    assert_eq!(
        buy_fills_for(Some(dec!(0.30)), Some(dec!(0.70)), 911, dec!(0.50)).len(),
        1,
        "in band → fills"
    );
    assert!(
        buy_fills_for(Some(dec!(0.30)), Some(dec!(0.70)), 912, dec!(0.80)).is_empty(),
        "above band → blocked"
    );
}

#[test]
fn floor_does_not_affect_sells() {
    // Leader BUYs at 0.40 (≥ floor → fills), then SELLs the same market at 0.10
    // (< floor, but the gate is BUY-arm only) → the SELL closes the position.
    let mut trades = training_book();
    trades.push(raw_trade(920, 25, Side::Buy, dec!(0.40), "open"));
    trades.push(raw_trade(920, 27, Side::Sell, dec!(0.10), "close"));
    trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();
    let config = base_config(&dir, Some(dec!(0.30)), None);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());
    run_simulation(
        &config,
        &trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &strategy,
        true,
    )
    .unwrap();

    let fills_path = dir.path().join("output").join("trades.ndjson");
    let target = mkt(920).0.0;
    let file = std::fs::File::open(&fills_path).unwrap();
    let sides: Vec<String> = std::io::BufReader::new(file)
        .lines()
        .map(|l| serde_json::from_str::<TradeFillJson>(&l.unwrap()).unwrap())
        .filter(|f| f.market_id == target)
        .map(|f| f.side)
        .collect();
    assert!(sides.contains(&"buy".to_string()), "BUY at 0.40 must fill");
    assert!(
        sides.contains(&"sell".to_string()),
        "SELL at 0.10 must still close (BUY-arm gate only)"
    );
}
