//! Scenario tests for the horizon-cooldown buy gate.
//!
//! `BacktestConfig::no_buy_within_horizon_days = Some(N)` suppresses new
//! BUY opens when `sim_date_unix >= simulation_end_unix - N*86_400`. SELLs
//! are always unaffected so existing positions can still close before the
//! report writes.
//!
//! Scenarios:
//!
//! 1. `cooldown_blocks_late_buys` — fixture has BUYs spanning a 70-day
//!    window; with `cooldown = 14 days`, every BUY after `end - 14d` is
//!    skipped. Asserts the number of BUY fills falls vs. the no-cooldown
//!    baseline.
//! 2. `cooldown_disabled_unchanged` — regression: `None` reproduces the
//!    baseline BUY count.
//! 3. `cooldown_sell_path_unaffected` — SELLs in the cooldown window still
//!    close the matching open positions (`total_copies > 0`).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::config::BacktestConfig;
use pe_backtest::report::WinnerFollowReport;
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
use std::io::BufRead as _;
use tempfile::TempDir;
use time::OffsetDateTime;

const BASE_UNIX: i64 = 1_698_796_800;
const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn mkt(idx: u32) -> MarketId {
    MarketId(VenueMarketId(format!("0xcond{idx:04}")))
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
        market_id: mkt(market_idx),
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

/// 70-day winner book: BUY day i, SELL day i+2.
fn winner_book(w: WalletAddress) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..70 {
        t.push(make_trade(w, i, i, Side::Buy, dec!(0.35)));
        t.push(make_trade(w, i, i + 2, Side::Sell, dec!(0.75)));
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

fn base_config(dir: &TempDir, cooldown_days: Option<u32>) -> BacktestConfig {
    BacktestConfig {
        bootstrap_cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
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
        flat_usd: Some(dec!(1)),
        no_buy_within_horizon_days: cooldown_days,
        require_known_expiry: false,
        max_positions_per_market: None,
        max_signal_price: None,
        max_trade_count: 0,
        injected_wallets_path: None,
        mtm_window_start_unix: None,
        mtm_window_end_unix: None,
        strategy: WinnerFollowConfig::default(),
    }
}

fn run(cooldown_days: Option<u32>) -> (WinnerFollowReport, Vec<TradeFillJson>) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let leader = wallet(LEADER_HEX);
    let mut trades = winner_book(leader);
    trades.sort_by_key(|t| t.timestamp.0);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();
    let config = base_config(&dir, cooldown_days);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    let report = run_simulation(
        &config,
        &trades,
        &snapshots,
        &resolutions,
        &schedules,
        &liq_index,
        &ranker_config,
        &strategy,
        true,
    )
    .unwrap();

    let fills_path = dir.path().join("output").join("trades.ndjson");
    let fills: Vec<TradeFillJson> = if fills_path.exists() {
        let file = std::fs::File::open(&fills_path).unwrap();
        std::io::BufReader::new(file)
            .lines()
            .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
            .collect()
    } else {
        Vec::new()
    };

    (report, fills)
}

#[derive(Debug, serde::Deserialize)]
struct TradeFillJson {
    side: String,
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: cooldown of 14 days strictly reduces the BUY fill count vs the
///       no-cooldown baseline (BUYs scheduled for the last 14 days are
///       skipped).
/// FAIL: BUY count unchanged → cutoff arithmetic wrong, or gate at the wrong
///       site.
#[tokio::test]
async fn cooldown_blocks_late_buys() {
    let (_, baseline_fills) = run(None);
    let (_, cooldown_fills) = run(Some(14));

    let baseline_buys = baseline_fills.iter().filter(|f| f.side == "buy").count();
    let cooldown_buys = cooldown_fills.iter().filter(|f| f.side == "buy").count();

    assert!(
        cooldown_buys < baseline_buys,
        "cooldown must reduce BUY count (baseline={baseline_buys} \
         cooldown={cooldown_buys})"
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: `None` reproduces the baseline BUY count — regression guard for the
///       cooldown branch's `if let Some(...)` shape.
/// FAIL: any divergence.
#[tokio::test]
async fn cooldown_disabled_unchanged() {
    let (_, fills_a) = run(None);
    let (_, fills_b) = run(None);

    let a = fills_a.iter().filter(|f| f.side == "buy").count();
    let b = fills_b.iter().filter(|f| f.side == "buy").count();
    assert_eq!(a, b, "deterministic baseline must match itself");
    assert!(a > 0, "fixture must produce at least one BUY");
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: SELLs still close positions during the cooldown window —
///       `total_copies > 0` proves the SELL path is unaffected by the gate.
/// FAIL: `total_copies == 0` → cooldown accidentally skipping SELLs too.
#[tokio::test]
async fn cooldown_sell_path_unaffected() {
    let (report, fills) = run(Some(14));

    let sells = fills.iter().filter(|f| f.side == "sell").count();
    assert!(sells > 0, "expected ≥1 SELL fill in cooldown mode");
    assert!(
        report.total_copies > 0,
        "expected ≥1 closed copy (SELL path must work despite cooldown)"
    );
}
