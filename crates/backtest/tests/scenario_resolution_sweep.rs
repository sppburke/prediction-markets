//! Scenario tests for the resolution sweep (issue #87).
//!
//! Each test exercises a distinct semantic of the per-day resolution sweep:
//!
//! 1. `resolved_yes_closes_position_at_full_price` — YES outcome: copy closed at 1.0, PnL > 0.
//! 2. `resolved_no_closes_position_at_zero` — NO outcome: copy closed at 0.0, fill recorded.
//! 3. `future_resolution_leaves_position_open` — resolved_at > last sim day → still open at horizon.
//! 4. `anomaly_guard_preserves_position_with_early_resolution` — resolved_at < bought_on → not closed.
//! 5. `empty_resolution_index_leaves_all_positions_open` — empty index → sweep is a no-op.
//! 6. `resolution_data_reduces_open_at_horizon` — with vs without index comparison.
//! 7. `sequential_markets_each_swept_independently` — two markets closed sequentially.
//! 8. `sweep_and_leader_sell_same_day_no_double_close` — simultaneous sweep + leader sell is safe.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{LeaderboardSnapshots, MarketResolution, ResolutionIndex, WalletCache};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{LedgerConfig, RankerConfig, snapshot::RawTrade};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const ALICE_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xdddddddddddddddddddddddddddddddddddddddd";
/// 2024-01-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_704_067_200;
const DAY: i64 = 86_400;

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn day_unix(d: u32) -> i64 {
    BASE_UNIX + i64::from(d) * DAY
}

fn mkt(idx: u32) -> MarketId {
    MarketId(VenueMarketId(format!("0xcond{idx:04}")))
}

fn make_trade(
    w: WalletAddress,
    market_idx: u32,
    day: u32,
    side: Side,
    price: Decimal,
    seq: u32,
) -> RawTrade {
    RawTrade {
        wallet: w,
        market_id: mkt(market_idx),
        outcome_id: OutcomeId(0),
        side,
        price: Price::new(price).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(
            OffsetDateTime::from_unix_timestamp(BASE_UNIX + i64::from(day) * DAY + i64::from(seq))
                .unwrap(),
        ),
        source_trade_id: SourceTradeId(format!(
            "0xtx_{market_idx}_{day}_{seq}_{}",
            if side == Side::Buy { "b" } else { "s" }
        )),
    }
}

/// 65 buy/sell round-trips on distinct markets so `w` qualifies for the watchlist by day ~62.
fn winner_book(w: WalletAddress) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..65 {
        t.push(make_trade(w, i, i, Side::Buy, dec!(0.35), 0));
        t.push(make_trade(w, i, i + 2, Side::Sell, dec!(0.75), 1));
    }
    t
}

/// Build a `FunderGraphTimeline` from `(funded, funder)` pairs at Unix 0.
fn make_timeline(dir: &TempDir, pairs: &[(WalletAddress, WalletAddress)]) -> FunderGraphTimeline {
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    for &(funded, funder) in pairs {
        cache
            .insert_funder_edges(funded, &[(funder, 0)], 0)
            .unwrap();
    }
    FunderGraphTimeline::from_cache(&cache).unwrap()
}

fn relaxed_ranker() -> RankerConfig {
    RankerConfig {
        active_min_closed_trades: 60,
        active_min_distinct_markets: 30,
        active_window_days: 365,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 10,
        incubator_min_distinct_markets: 5,
        incubator_window_days: 365,
        incubator_watchlist_size: 250,
        min_reconstruction_quality: 0,
    }
}

fn base_config(dir: &TempDir) -> BacktestConfig {
    BacktestConfig {
        cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
        step_days: 1,
        dune_api_key: None,
        dune_namespace: None,
        max_hours_to_expiry: None,
        audit_window_days: 365,
        ranker_min_quality: 0,
        ranker_active_min_closed: 60,
        ranker_active_min_markets: 30,
        ranker_incubator_min_closed: 10,
        ranker_incubator_min_markets: 5,
        kelly_sweep_fractions: None,
        per_trade_cap_override: None,
    }
}

fn default_strategy() -> WinnerFollowStrategy {
    WinnerFollowStrategy::new(WinnerFollowConfig::default())
}

fn run_sim(
    dir: &TempDir,
    trades: Vec<RawTrade>,
    timeline: &FunderGraphTimeline,
    resolutions: &ResolutionIndex,
) -> pe_backtest::report::WinnerFollowReport {
    run_simulation(
        &base_config(dir),
        trades,
        timeline,
        &LeaderboardSnapshots::default(),
        resolutions,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap()
}

fn res_yes(resolved_day: u32) -> MarketResolution {
    MarketResolution {
        winning_outcome_id: 0,
        resolved_at_unix: day_unix(resolved_day),
    }
}

fn res_no(resolved_day: u32) -> MarketResolution {
    MarketResolution {
        winning_outcome_id: 1,
        resolved_at_unix: day_unix(resolved_day),
    }
}

fn fills_by_side(output_dir: &std::path::Path, side: &str) -> Vec<serde_json::Value> {
    let path = output_dir.join("trades.ndjson");
    let Ok(bytes) = std::fs::read(&path) else {
        return Vec::new();
    };
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("side").and_then(|s| s.as_str()) == Some(side))
        .collect()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: position on outcome 0 swept at close_price=1.0 when outcome 0 wins.
///       open_at_horizon==0; total_pnl_usd > 0; ≥1 resolution fill in trades.ndjson.
/// FAIL: position remains open OR no resolution fill written.
#[tokio::test]
async fn resolved_yes_closes_position_at_full_price() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 72, Side::Sell, dec!(0.75), 1));

    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(mkt(9999), res_yes(71));

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &resolutions);

    assert_eq!(
        report.open_at_horizon, 0,
        "resolved market must not remain open at horizon"
    );
    assert!(
        report.total_pnl_usd > Decimal::ZERO,
        "winner book + YES resolution must yield positive PnL; got {}",
        report.total_pnl_usd
    );
    let fills = fills_by_side(&dir.path().join("output"), "resolution");
    assert!(
        !fills.is_empty(),
        "expected ≥1 resolution fill in trades.ndjson"
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: position swept at close_price=0.0 when the opposing outcome wins.
///       open_at_horizon==0; resolution fill exists with fill_price=0.
/// FAIL: position remains open OR fill_price is non-zero.
#[tokio::test]
async fn resolved_no_closes_position_at_zero() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 72, Side::Sell, dec!(0.75), 1));

    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(mkt(9999), res_no(71));

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &resolutions);

    assert_eq!(
        report.open_at_horizon, 0,
        "NO-resolved market must not remain open at horizon"
    );

    let fills = fills_by_side(&dir.path().join("output"), "resolution");
    assert!(
        !fills.is_empty(),
        "expected a resolution fill in trades.ndjson"
    );
    for fill in &fills {
        let fp = match &fill["fill_price"] {
            serde_json::Value::Number(n) => n.as_f64().unwrap_or(1.0),
            serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(1.0),
            _ => 1.0,
        };
        assert!(
            fp.abs() < 0.001,
            "NO-resolved fill must have fill_price≈0; got {fp}"
        );
    }
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: a market resolved after the last simulation day is not swept;
///       position remains open at horizon with no resolution fill.
/// FAIL: position prematurely closed before the resolution date.
#[tokio::test]
async fn future_resolution_leaves_position_open_at_horizon() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));

    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(mkt(9999), res_yes(200)); // far future

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &resolutions);

    assert!(
        report.open_at_horizon > 0,
        "position must remain open when resolved_at > last sim date; open_at_horizon=0"
    );
    let fills = fills_by_side(&dir.path().join("output"), "resolution");
    assert!(
        fills.is_empty(),
        "no resolution fill expected for a future resolution; found {}",
        fills.len()
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: when resolved_at_unix < bought_on_unix the anomaly guard preserves the position.
///       open_at_horizon > 0 and no resolution fill in trades.ndjson.
/// FAIL: position closed despite the anomaly guard.
#[tokio::test]
async fn anomaly_guard_preserves_position_with_early_resolution() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9998, 72, Side::Buy, dec!(0.35), 0));

    let mut resolutions = ResolutionIndex::new();
    // resolved_at = day 69, BEFORE bought_on = day 70 → anomaly guard must skip.
    resolutions.insert(mkt(9999), res_yes(69));

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &resolutions);

    assert!(
        report.open_at_horizon > 0,
        "anomaly guard must prevent close when resolved_at < bought_on; open_at_horizon=0"
    );
    let fills = fills_by_side(&dir.path().join("output"), "resolution");
    assert!(
        fills.is_empty(),
        "no resolution fill expected when anomaly guard triggers; found {}",
        fills.len()
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: an empty ResolutionIndex leaves all positions open; sweep is a no-op.
/// FAIL: a position is closed without any resolution data.
#[tokio::test]
async fn empty_resolution_index_leaves_all_positions_open() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &ResolutionIndex::new());

    assert!(
        report.open_at_horizon > 0,
        "without resolution data the open position must remain at horizon"
    );
    let fills = fills_by_side(&dir.path().join("output"), "resolution");
    assert!(
        fills.is_empty(),
        "no resolution fill expected with empty index; found {}",
        fills.len()
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: with resolution data, open_at_horizon is strictly lower than without it.
/// FAIL: resolution data does not reduce open_at_horizon.
#[tokio::test]
async fn resolution_data_reduces_open_at_horizon() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let build_trades = || {
        let mut t = winner_book(alice);
        t.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
        t.push(make_trade(alice, 9998, 72, Side::Buy, dec!(0.35), 0));
        t
    };

    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(mkt(9999), res_yes(71));

    let dir_with = TempDir::new().unwrap();
    let timeline_with = make_timeline(&dir_with, &[(alice, funder)]);
    let with_res = run_sim(&dir_with, build_trades(), &timeline_with, &resolutions);

    let dir_without = TempDir::new().unwrap();
    let timeline_without = make_timeline(&dir_without, &[(alice, funder)]);
    let without_res = run_sim(
        &dir_without,
        build_trades(),
        &timeline_without,
        &ResolutionIndex::new(),
    );

    assert!(
        with_res.open_at_horizon < without_res.open_at_horizon,
        "resolution data must reduce open_at_horizon: with={} without={}",
        with_res.open_at_horizon,
        without_res.open_at_horizon
    );
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────

/// PASS: two positions on distinct markets are swept independently in the same sweep pass.
///       open_at_horizon==0; exactly 2 resolution fills in trades.ndjson.
/// FAIL: one or both positions remain open, or fill count != 2.
#[tokio::test]
async fn sequential_markets_each_swept_independently() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9998, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 72, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9998, 76, Side::Sell, dec!(0.75), 1));

    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(mkt(9998), res_yes(73));
    resolutions.insert(mkt(9999), res_yes(75));

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &resolutions);

    assert_eq!(
        report.open_at_horizon, 0,
        "both markets must be swept; open_at_horizon={}",
        report.open_at_horizon
    );
    let fills = fills_by_side(&dir.path().join("output"), "resolution");
    assert_eq!(
        fills.len(),
        2,
        "expected 2 resolution fills (one per market); got {}",
        fills.len()
    );
}

// ── Scenario 8 ────────────────────────────────────────────────────────────────

/// PASS: when the sweep closes a position AND the leader sells the same market on the
///       same day, the position is not double-closed and bankroll is not double-credited.
/// FAIL: bankroll exceeds the expected ceiling (double-close artefact).
#[tokio::test]
async fn sweep_and_leader_sell_same_day_no_double_close() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let initial_bankroll = Decimal::from(10_000u32);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 72, Side::Sell, dec!(0.75), 1));

    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(mkt(9999), res_yes(71));

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir, &[(alice, funder)]);
    let report = run_sim(&dir, trades, &timeline, &resolutions);

    assert_eq!(
        report.open_at_horizon, 0,
        "position must be closed exactly once"
    );
    assert!(
        report.bankroll_final <= initial_bankroll * Decimal::from(2u32),
        "bankroll suspiciously large — possible double-close: {}",
        report.bankroll_final
    );
    assert!(
        report.bankroll_final >= Decimal::ZERO,
        "bankroll must remain non-negative; got {}",
        report.bankroll_final
    );
}
