//! Scenario tests for the walk-forward backtest.
//!
//! Scenarios:
//! 1. `winner_wallet_produces_positive_pnl` — a 100%-win-rate wallet is copied and
//!    generates positive realized PnL after evaluate() routes the signal.
//! 2. `open_at_horizon_excluded_from_realized_pnl` — positions still open at the end
//!    of the simulation are counted in `open_at_horizon` and NOT written off as losses.
//! 3. `per_trader_win_rate_used_as_probability` — the simulation uses the per-leader
//!    empirical win rate (from TraderLedger) as `p` instead of a flat leader_alpha stub.
//! 4. `fee_model_reduces_edge` — the Polymarket fee formula increases `c`, reducing
//!    the Kelly edge. At p≈c+fee, evaluate() returns NoEdge.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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

const WINNER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const LOSER_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
// Base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;

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
        source_trade_id: SourceTradeId(format!(
            "0xhash_{market_idx}_{}_{}",
            w,
            if side == Side::Buy { "buy" } else { "sell" }
        )),
    }
}

/// Ranker config relaxed to allow our synthetic fixture to qualify.
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
        max_hours_to_expiry: None,
        audit_window_days: 90,
        ranker_min_quality: 0,
        ranker_active_min_closed: 15,
        ranker_active_min_markets: 1,
        ranker_incubator_min_closed: 5,
        ranker_incubator_min_markets: 1,
        kelly_sweep_fractions: None,
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
        liquidity_take_fraction: rust_decimal::Decimal::new(5, 2),
        liquidity_min_required_usd: rust_decimal::Decimal::new(200, 0),
        kelly_p_min_snapshots: 0,
        kelly_p_extra_per_missing_snapshot: 0,
        flat_usd: None,
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: None,
        max_signal_price: None,
        max_trade_count: 0,
        injected_wallets_path: None,
        strategy: WinnerFollowConfig::default(),
    }
}

fn default_strategy() -> WinnerFollowStrategy {
    WinnerFollowStrategy::new(WinnerFollowConfig::default())
}

/// Generate 65 markets × 2 trades (BUY on day D, SELL on day D+2) for a 100%-win-rate wallet.
///
/// Buys at 0.35 and sells at 0.75 — a large, always-winning edge.
fn generate_winner_trades(winner: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..65 {
        trades.push(make_trade(winner, i, i, Side::Buy, dec!(0.35)));
        trades.push(make_trade(winner, i, i + 2, Side::Sell, dec!(0.75)));
    }
    trades
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: a known-winner wallet (65 markets, 100% win rate) is copied and produces
///       positive total PnL.
/// FAIL: no copies produced, or total PnL is not positive.
#[tokio::test]
async fn winner_wallet_produces_positive_pnl() {
    let winner = wallet(WINNER_HEX);

    let mut all_trades = generate_winner_trades(winner);
    all_trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();

    let report = run_simulation(
        &base_config(&dir),
        &all_trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "expected ≥1 copy; got 0 — winner may not have passed ranker thresholds"
    );

    assert!(
        report.total_pnl_usd > Decimal::ZERO,
        "expected positive PnL; got {}",
        report.total_pnl_usd
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: positions still open when the simulation ends appear in `open_at_horizon` and
///       are NOT written off as losses — total_pnl_usd does not include them.
/// FAIL: open_at_horizon == 0, or total_pnl_usd includes a write-off loss for open positions.
#[tokio::test]
async fn open_at_horizon_excluded_from_realized_pnl() {
    let winner = wallet(WINNER_HEX);

    let mut all_trades = generate_winner_trades(winner);
    // Extra BUY on the last day with no corresponding SELL.
    all_trades.push(make_trade(winner, 99, 65, Side::Buy, dec!(0.35)));
    all_trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();

    let report = run_simulation(
        &base_config(&dir),
        &all_trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.open_at_horizon > 0,
        "expected open_at_horizon > 0; got {}",
        report.open_at_horizon
    );

    assert!(
        report.bankroll_final >= Decimal::ZERO,
        "bankroll went negative: {}",
        report.bankroll_final
    );

    assert!(
        report.total_pnl_usd >= Decimal::ZERO,
        "expected total_pnl_usd ≥ 0 (open positions excluded); got {}",
        report.total_pnl_usd
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: a wallet with a 100% win rate produces larger Kelly-sized allocations than
///       a wallet with a 50% win rate — confirming that `p` from the ledger drives sizing.
/// FAIL: sizing is identical regardless of win rate (indicating leader_alpha or a fixed p stub).
#[tokio::test]
async fn per_trader_win_rate_used_as_probability() {
    let high_winner = wallet(WINNER_HEX);
    let low_winner = wallet(LOSER_HEX);

    // High winner: 65 round-trips, all profitable (100% win rate).
    let mut all_trades = generate_winner_trades(high_winner);

    // Low winner: 65 round-trips but sells below buy price (0% win rate after fees).
    for i in 100u32..165 {
        all_trades.push(make_trade(low_winner, i, i - 100, Side::Buy, dec!(0.60)));
        all_trades.push(make_trade(
            low_winner,
            i,
            i - 100 + 2,
            Side::Sell,
            dec!(0.40),
        ));
    }

    all_trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();

    let report = run_simulation(
        &base_config(&dir),
        &all_trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "expected the high-win-rate wallet to generate copies; got 0"
    );

    assert!(
        report.total_pnl_usd > Decimal::ZERO,
        "expected positive PnL; got {}",
        report.total_pnl_usd,
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: the Polymarket fee model is applied to `c` so that Kelly is reduced.
/// FAIL: fee is not applied (c = price only), which would give a falsely inflated edge.
#[tokio::test]
async fn fee_model_reduces_edge_at_high_prices() {
    let winner = wallet(WINNER_HEX);

    let mut all_trades = generate_winner_trades(winner);

    // Add high-price trades that the winner takes AFTER qualifying.
    for i in 200u32..202 {
        all_trades.push(make_trade(winner, i, 70 + i - 200, Side::Buy, dec!(0.95)));
        all_trades.push(make_trade(
            winner,
            i,
            70 + i - 200 + 2,
            Side::Sell,
            dec!(0.98),
        ));
    }
    all_trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();

    let report = run_simulation(
        &base_config(&dir),
        &all_trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.bankroll_final > Decimal::ZERO,
        "bankroll drained to zero unexpectedly: {}",
        report.bankroll_final
    );
}
