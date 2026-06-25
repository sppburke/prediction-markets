//! Scenario tests for the per-market position cap (issue #138).
//!
//! `BacktestConfig::max_positions_per_market = Some(n)` blocks new BUY opens on
//! any `market_id` that already has `n` concurrent open positions across every
//! leader and outcome. The cap is structural — placed in the BUY arm before
//! flat-USD and Kelly sizing branches — so both sizing paths honor it. Once
//! existing positions close via SELL or resolution sweep, the slot reopens for
//! the next leader signal.
//!
//! Scenarios:
//!
//! 1. `cap_one_blocks_second_leader_same_market` — two leaders BUY the same
//!    market on the same day with `cap = Some(1)`. Only one fill lands.
//! 2. `cap_two_allows_both_leaders` — same setup with `cap = Some(2)`. Both
//!    fills land. Proves the cap is a configurable threshold, not a hardcoded 1.
//! 3. `cap_counts_across_outcomes` — leader A BUYs outcome 0, leader B BUYs
//!    outcome 1 of the same binary market. `cap = Some(1)` blocks the second.
//!    Proves the cap is keyed on `market_id`, not `(market_id, outcome_id)`.
//! 4. `cap_slot_reopens_after_sell` — A BUYs, A SELLs (line ~830 close path),
//!    B BUYs the same market on a later day. With `cap = Some(1)` both fill.
//! 5. `cap_slot_reopens_after_resolution` — A BUYs, market resolves (line ~371
//!    sweep path — structurally different from SELL), B BUYs the same market
//!    on a later day. With `cap = Some(1)` both fill.
//! 6. `cap_uncapped_preserves_baseline` — `cap = None` with two leaders on the
//!    same market produces two fills. Regression guard.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::io::BufRead as _;
use std::num::NonZeroU32;

use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{
    LeaderboardSnapshots, LiquidityIndex, MarketResolution, ResolutionIndex, ScheduleIndex,
};
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

// Base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;
const LEADER_A_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const LEADER_B_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn mkt(idx: u32) -> MarketId {
    MarketId(VenueMarketId(format!("0xcond{idx:04}")))
}

fn raw_trade(
    w: WalletAddress,
    market_idx: u32,
    outcome: u16,
    day_offset: u32,
    hour_offset: i64,
    side: Side,
    price: Decimal,
    tag: &str,
) -> RawTrade {
    let ts = BASE_UNIX + i64::from(day_offset) * 86_400 + hour_offset * 3600;
    RawTrade {
        wallet: w,
        market_id: mkt(market_idx),
        outcome_id: OutcomeId(outcome),
        side,
        price: Price::new(price).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(format!("0x_{tag}_{market_idx}_{day_offset}")),
    }
}

/// Generate a winner book on a per-leader, disjoint market range so two leaders
/// can both clear the watchlist threshold independently. `start_market` lets
/// the caller place each leader's training trades on a unique range. Each entry
/// is a (BUY day i, SELL day i+2) pair — 20 pairs total clears the 15-closed
/// minimum on the relaxed ranker.
fn training_book(w: WalletAddress, start_market: u32, tag: &str) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..20 {
        let m = start_market + i;
        t.push(raw_trade(w, m, 0, i, 0, Side::Buy, dec!(0.35), tag));
        t.push(raw_trade(w, m, 0, i + 2, 1, Side::Sell, dec!(0.75), tag));
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

fn base_config(dir: &TempDir, cap: Option<NonZeroU32>) -> BacktestConfig {
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
        // Flat-USD short-circuit gives deterministic 1-contract fills and
        // proves the cap gate fires before any sizing branch.
        flat_usd: Some(dec!(1)),
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: cap,
        max_signal_price: None,
        max_trade_count: 0,
        injected_wallets_path: None,
        mtm_window_start_unix: None,
        mtm_window_end_unix: None,
        strategy: WinnerFollowConfig::default(),
    }
}

#[derive(Debug, serde::Deserialize)]
struct TradeFillJson {
    market_id: String,
    side: String,
}

/// Drive `run_simulation` with a caller-supplied trade list and resolution index.
fn run_with(
    cap: Option<NonZeroU32>,
    mut trades: Vec<RawTrade>,
    resolutions: ResolutionIndex,
) -> Vec<TradeFillJson> {
    trades.sort_by_key(|t| t.timestamp.0);
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let snapshots = LeaderboardSnapshots::default();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();
    let config = base_config(&dir, cap);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    let _report = run_simulation(
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
    if fills_path.exists() {
        let file = std::fs::File::open(&fills_path).unwrap();
        std::io::BufReader::new(file)
            .lines()
            .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
            .collect()
    } else {
        Vec::new()
    }
}

/// Convenience: both leaders trained, then both BUY the same `(market, outcome)`
/// on the test day at distinct timestamps.
fn both_buy_same_market_same_day(test_market: u32, outcome: u16) -> Vec<RawTrade> {
    let leader_a = wallet(LEADER_A_HEX);
    let leader_b = wallet(LEADER_B_HEX);
    let mut trades = Vec::new();
    trades.extend(training_book(leader_a, 1, "A"));
    trades.extend(training_book(leader_b, 100, "B"));
    // Both BUY test_market on day 30 (well after all training closes by day 21).
    trades.push(raw_trade(
        leader_a,
        test_market,
        outcome,
        30,
        1,
        Side::Buy,
        dec!(0.40),
        "Abuy",
    ));
    trades.push(raw_trade(
        leader_b,
        test_market,
        outcome,
        30,
        2,
        Side::Buy,
        dec!(0.40),
        "Bbuy",
    ));
    trades
}

fn cap_one() -> Option<NonZeroU32> {
    Some(NonZeroU32::MIN)
}

fn cap_two() -> Option<NonZeroU32> {
    NonZeroU32::new(2)
}

fn buy_fills_on(fills: &[TradeFillJson], market_idx: u32) -> Vec<&TradeFillJson> {
    let m = mkt(market_idx).0.0;
    fills
        .iter()
        .filter(|f| f.market_id == m && f.side == "buy")
        .collect()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: with `cap = Some(1)`, two leaders both signaling BUY on the same
///       `(market, outcome)` on the same day produce exactly one BUY fill.
/// FAIL: 2 fills → cap gate skipped; 0 fills → unrelated suppression.
#[tokio::test]
async fn cap_one_blocks_second_leader_same_market() {
    let trades = both_buy_same_market_same_day(500, 0);
    let fills = run_with(cap_one(), trades, ResolutionIndex::new());
    let buys = buy_fills_on(&fills, 500);
    assert_eq!(
        buys.len(),
        1,
        "expected exactly 1 BUY on market 500 under cap=1; got {}",
        buys.len()
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: with `cap = Some(2)`, the same fixture produces both BUY fills.
///       Proves the cap is a parameter, not a hardcoded constant.
/// FAIL: not 2 → cap arithmetic wrong (off-by-one or hardcoded).
#[tokio::test]
async fn cap_two_allows_both_leaders() {
    let trades = both_buy_same_market_same_day(501, 0);
    let fills = run_with(cap_two(), trades, ResolutionIndex::new());
    let buys = buy_fills_on(&fills, 501);
    assert_eq!(
        buys.len(),
        2,
        "expected 2 BUYs on market 501 under cap=2; got {}",
        buys.len()
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: leader A BUYs outcome 0 and leader B BUYs outcome 1 of the same
///       `market_id`; with `cap = Some(1)` only one fill lands.
/// FAIL: 2 fills → cap was keyed on `(market_id, outcome_id)` not just
///       `market_id` (mis-design — would allow being on both sides of a binary).
#[tokio::test]
async fn cap_counts_across_outcomes() {
    let leader_a = wallet(LEADER_A_HEX);
    let leader_b = wallet(LEADER_B_HEX);
    let mut trades = Vec::new();
    trades.extend(training_book(leader_a, 1, "A"));
    trades.extend(training_book(leader_b, 100, "B"));
    // Same market_id (502), opposite outcomes, same day.
    trades.push(raw_trade(
        leader_a,
        502,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.40),
        "Abuy",
    ));
    trades.push(raw_trade(
        leader_b,
        502,
        1,
        30,
        2,
        Side::Buy,
        dec!(0.40),
        "Bbuy",
    ));

    let fills = run_with(cap_one(), trades, ResolutionIndex::new());
    let buys = buy_fills_on(&fills, 502);
    assert_eq!(
        buys.len(),
        1,
        "expected exactly 1 BUY across both outcomes of market 502 under cap=1; got {}",
        buys.len()
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: A BUYs market 503 on day 30, A SELLs day 32 (line ~830 close path
///       decrements the counter), B BUYs day 33 — both fills land under
///       `cap = Some(1)` because the slot reopened after the SELL.
/// FAIL: 1 fill → SELL close path failed to decrement `by_market_count`.
#[tokio::test]
async fn cap_slot_reopens_after_sell() {
    let leader_a = wallet(LEADER_A_HEX);
    let leader_b = wallet(LEADER_B_HEX);
    let mut trades = Vec::new();
    trades.extend(training_book(leader_a, 1, "A"));
    trades.extend(training_book(leader_b, 100, "B"));
    trades.push(raw_trade(
        leader_a,
        503,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.40),
        "Abuy",
    ));
    trades.push(raw_trade(
        leader_a,
        503,
        0,
        32,
        1,
        Side::Sell,
        dec!(0.80),
        "Asell",
    ));
    trades.push(raw_trade(
        leader_b,
        503,
        0,
        33,
        1,
        Side::Buy,
        dec!(0.40),
        "Bbuy",
    ));

    let fills = run_with(cap_one(), trades, ResolutionIndex::new());
    let buys = buy_fills_on(&fills, 503);
    assert_eq!(
        buys.len(),
        2,
        "expected 2 BUYs on market 503 (slot reopens after SELL); got {}",
        buys.len()
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: A BUYs market 504 on day 30; market resolves day 31 (resolution
///       sweep, line ~371 close path — structurally distinct from SELL); B
///       BUYs day 32. With `cap = Some(1)` both fills land because the sweep
///       reopened the slot.
/// FAIL: 1 fill → resolution sweep close path failed to decrement counter.
#[tokio::test]
async fn cap_slot_reopens_after_resolution() {
    let leader_a = wallet(LEADER_A_HEX);
    let leader_b = wallet(LEADER_B_HEX);
    let mut trades = Vec::new();
    trades.extend(training_book(leader_a, 1, "A"));
    trades.extend(training_book(leader_b, 100, "B"));
    trades.push(raw_trade(
        leader_a,
        504,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.40),
        "Abuy",
    ));
    // B's BUY is day 32, after resolution on day 31.
    trades.push(raw_trade(
        leader_b,
        504,
        0,
        32,
        1,
        Side::Buy,
        dec!(0.40),
        "Bbuy",
    ));

    let mut resolutions = ResolutionIndex::new();
    // Market 504 resolves at day-31 00:00 — outcome 0 wins.
    resolutions.insert(
        mkt(504),
        MarketResolution {
            resolved_at_unix: BASE_UNIX + 31 * 86_400,
            winning_outcome_id: OutcomeId(0),
        },
    );

    let fills = run_with(cap_one(), trades, resolutions);
    let buys = buy_fills_on(&fills, 504);
    assert_eq!(
        buys.len(),
        2,
        "expected 2 BUYs on market 504 (slot reopens after resolution); got {}",
        buys.len()
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: `cap = None` with two leaders on the same market produces two BUY
///       fills. Regression guard for the `if let Some(cap) = ...` shape — a
///       broken match arm that always blocks would trip this test.
/// FAIL: <2 fills → the uncapped path is no longer a no-op.
#[tokio::test]
async fn cap_uncapped_preserves_baseline() {
    let trades = both_buy_same_market_same_day(505, 0);
    let fills = run_with(None, trades, ResolutionIndex::new());
    let buys = buy_fills_on(&fills, 505);
    assert_eq!(
        buys.len(),
        2,
        "expected 2 BUYs on market 505 under cap=None (uncapped baseline); got {}",
        buys.len()
    );
}
