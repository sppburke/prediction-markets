//! Scenario tests for the `max_signal_price` BUY cap (issue #142).
//!
//! `BacktestConfig::max_signal_price = Some(cap)` skips any BUY whose
//! slippage-adjusted `fill_price` is `>= cap`. The gate is placed in the BUY
//! arm immediately after the existing slippage clamp (`raw >= Decimal::ONE`)
//! and *before* the flat-USD short-circuit and Kelly path, so every sizing
//! branch honors it. Gating on `fill_price` (not the leader's signal price)
//! is the load-bearing design choice — a signal at 0.849 + 1% slippage =
//! 0.857 cannot squeak past a signal-price cap.
//!
//! Scenarios:
//!
//!  1. `cap_blocks_above_threshold` — signal 0.86 + 1% slippage = 0.8686 ≥
//!     0.85 with the default cap → no BUY fill.
//!  2. `cap_blocks_at_exact_threshold` — signal 0.85 + 0% slippage = 0.85
//!     exactly → blocked. `>=` semantics; strictly conservative.
//!  3. `cap_allows_just_below_threshold` — signal 0.84 + 0% slippage = 0.84
//!     < 0.85 → BUY fills.
//!  4. `cap_blocks_via_slippage_crossing` — signal 0.84 + 1.5% slippage =
//!     0.8526 ≥ 0.85 → blocked. **Critical test for the fill-price vs
//!     signal-price design choice** — a signal-price cap would have allowed
//!     this; the fill-price cap correctly catches the crossing.
//!  5. `cap_none_allows_all_prices` — `max_signal_price: None` with signal
//!     0.95 → BUY fills (regression guard for the `if let Some(cap)` shape).
//!  6. `cap_custom_threshold_applied` — `Some(dec!(0.70))`: signal 0.71 →
//!     blocked, signal 0.65 → allowed. Proves the cap is a parameter.
//!  7. `suppression_counter_matches_ratio` — 1 blocked + 1 allowed BUY →
//!     `report.high_price_suppression_pct == 50`.
//!  8. `suppression_zero_when_cap_disabled` — same fixture, `cap = None` →
//!     `report.high_price_suppression_pct == 0` and the per-quarter map is
//!     empty (no records emitted when the gate is off).
//!  9. `cap_does_not_affect_sells` — leader BUYs at 0.40 (allowed), leader
//!     SELLs same market at 0.95 (would be ≥ cap if BUY). The SELL closes
//!     the position normally — gate is BUY-arm only.
//! 10. `cap_composes_with_flat_usd` — `flat_usd: Some(dec!(1))` + default
//!     cap. Signal 0.84 + 0% slippage → fill lands via flat-USD path
//!     (cap allows it through). Regression guard that the new gate doesn't
//!     break sizing paths below the threshold.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::io::BufRead as _;

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
use tempfile::TempDir;
use time::OffsetDateTime;

// Base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;
const LEADER_A_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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

/// 20 closed round-trips (BUY day i at 0.35, SELL day i+2 at 0.75) on a
/// disjoint per-leader market range — clears the 15-closed-trade gate on the
/// relaxed ranker. Training prices stay well below every cap under test.
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

/// Watchlist-eligibility kicks in only *after* the full 20-trade training
/// book closes (last SELL on day 21). Used by the suppression-counter tests
/// so the only BUYs reaching the cap gate are the explicit test fixtures —
/// no training BUYs inflate the denominator.
fn strict_post_training_ranker() -> RankerConfig {
    RankerConfig {
        active_min_closed_trades: 20,
        active_min_distinct_markets: 1,
        active_window_days: 365,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 20,
        incubator_min_distinct_markets: 1,
        incubator_window_days: 365,
        incubator_watchlist_size: 250,
        min_reconstruction_quality: 0,
    }
}

/// Build a `BacktestConfig` parameterised on the cap and the strategy slippage
/// rate. Slippage is a `WinnerFollowConfig` field — exposing it here lets
/// tests construct exact-threshold fixtures (slippage = 0) and slippage-
/// crossing fixtures (slippage = 0.015) deterministically.
fn base_config(
    dir: &TempDir,
    max_signal_price: Option<Decimal>,
    slippage_rate: Decimal,
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
        // Flat-USD short-circuit gives deterministic 1-contract fills.
        flat_usd: Some(dec!(1)),
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: None,
        max_signal_price,
        min_signal_price: None,
        max_trade_count: 0,
        injected_wallets_path: None,
        mtm_window_start_unix: None,
        mtm_window_end_unix: None,
        strategy: WinnerFollowConfig {
            slippage_rate,
            ..WinnerFollowConfig::default()
        },
    }
}

#[derive(Debug, serde::Deserialize)]
struct TradeFillJson {
    market_id: String,
    side: String,
}

/// Drive `run_simulation` with a caller-supplied trade list, cap, slippage,
/// and ranker config. Returns both the parsed fills and the run report so
/// tests can assert on the suppression counters as well as the BUY/SELL
/// outcome.
fn run_with_ranker(
    cap: Option<Decimal>,
    slippage_rate: Decimal,
    ranker_config: RankerConfig,
    mut trades: Vec<RawTrade>,
) -> (Vec<TradeFillJson>, WinnerFollowReport) {
    trades.sort_by_key(|t| t.timestamp.0);
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let snapshots = LeaderboardSnapshots::default();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let config = base_config(&dir, cap, slippage_rate);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    let report = run_simulation(
        &config,
        &trades,
        &snapshots,
        &ResolutionIndex::new(),
        &schedules,
        &liq_index,
        &ranker_config,
        &strategy,
        true,
    )
    .unwrap();

    let fills_path = dir.path().join("output").join("trades.ndjson");
    let fills = if fills_path.exists() {
        let file = std::fs::File::open(&fills_path).unwrap();
        std::io::BufReader::new(file)
            .lines()
            .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
            .collect()
    } else {
        Vec::new()
    };
    (fills, report)
}

/// Convenience wrapper around `run_with_ranker` using the standard relaxed
/// ranker (incubator-min 5, active-min 15) — copies start firing as soon as
/// the leader has enough closed trades, which is most tests' setup.
fn run_with(
    cap: Option<Decimal>,
    slippage_rate: Decimal,
    trades: Vec<RawTrade>,
) -> (Vec<TradeFillJson>, WinnerFollowReport) {
    run_with_ranker(cap, slippage_rate, relaxed_ranker(), trades)
}

/// `leader_a` trained, then BUYs `(market, outcome=0)` at the given price on
/// day 30 (well after training closes on day 21).
fn trained_then_buy(test_market: u32, price: Decimal, tag: &str) -> Vec<RawTrade> {
    let leader_a = wallet(LEADER_A_HEX);
    let mut trades = training_book(leader_a, 1, "A");
    trades.push(raw_trade(
        leader_a,
        test_market,
        0,
        30,
        1,
        Side::Buy,
        price,
        tag,
    ));
    trades
}

fn buy_fills_on(fills: &[TradeFillJson], market_idx: u32) -> Vec<&TradeFillJson> {
    let m = mkt(market_idx).0.0;
    fills
        .iter()
        .filter(|f| f.market_id == m && f.side == "buy")
        .collect()
}

fn fills_on(fills: &[TradeFillJson], market_idx: u32) -> Vec<&TradeFillJson> {
    let m = mkt(market_idx).0.0;
    fills.iter().filter(|f| f.market_id == m).collect()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: signal 0.86 + 1% slippage = 0.8686 ≥ 0.85 default cap → no BUY fill.
/// FAIL: 1 BUY → gate skipped or threshold misapplied.
#[tokio::test]
async fn cap_blocks_above_threshold() {
    let trades = trained_then_buy(500, dec!(0.86), "Abuy");
    let (fills, _) = run_with(Some(dec!(0.85)), dec!(0.01), trades);
    assert_eq!(
        buy_fills_on(&fills, 500).len(),
        0,
        "expected 0 BUYs on market 500 (fill 0.8686 ≥ cap 0.85)",
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: signal 0.85 + 0% slippage = 0.85 exactly → blocked.
/// FAIL: 1 BUY → comparison is `>` not `>=` (off-by-one at the boundary).
#[tokio::test]
async fn cap_blocks_at_exact_threshold() {
    let trades = trained_then_buy(501, dec!(0.85), "Abuy");
    let (fills, _) = run_with(Some(dec!(0.85)), dec!(0), trades);
    assert_eq!(
        buy_fills_on(&fills, 501).len(),
        0,
        "expected 0 BUYs on market 501 (fill = cap exactly; `>=` blocks)",
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: signal 0.84 + 0% slippage = 0.84 < 0.85 → BUY fills.
/// FAIL: 0 BUYs → false positive at the boundary.
#[tokio::test]
async fn cap_allows_just_below_threshold() {
    let trades = trained_then_buy(502, dec!(0.84), "Abuy");
    let (fills, _) = run_with(Some(dec!(0.85)), dec!(0), trades);
    assert_eq!(
        buy_fills_on(&fills, 502).len(),
        1,
        "expected 1 BUY on market 502 (fill 0.84 < cap 0.85)",
    );
}

// ── Scenario 4 — load-bearing design test ─────────────────────────────────────

/// Validates the **fill-price vs signal-price** design choice.
///
/// PASS: signal 0.84 + 1.5% slippage = 0.8526 ≥ 0.85 cap → blocked. A
///       signal-price cap (comparing `trade.price.0 >= cap`) would have let
///       this through; the fill-price cap correctly captures slippage cost.
/// FAIL: 1 BUY → the gate is comparing against signal price not `fill_price`,
///       leaving the slippage-crossing hole the plan was designed to close.
#[tokio::test]
async fn cap_blocks_via_slippage_crossing() {
    let trades = trained_then_buy(503, dec!(0.84), "Abuy");
    let (fills, _) = run_with(Some(dec!(0.85)), dec!(0.015), trades);
    assert_eq!(
        buy_fills_on(&fills, 503).len(),
        0,
        "expected 0 BUYs on market 503 (fill 0.8526 ≥ cap 0.85 via slippage crossing)",
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: `max_signal_price: None` with signal 0.95 + 1% slippage = 0.9595
///       < `Decimal::ONE` (existing clamp passes) → BUY fills. Regression
///       guard for the `if let Some(cap)` shape: a broken match that always
///       blocked would trip this.
/// FAIL: 0 BUYs → the `None` path is no longer a no-op.
#[tokio::test]
async fn cap_none_allows_all_prices() {
    let trades = trained_then_buy(504, dec!(0.95), "Abuy");
    let (fills, _) = run_with(None, dec!(0.01), trades);
    assert_eq!(
        buy_fills_on(&fills, 504).len(),
        1,
        "expected 1 BUY on market 504 (cap disabled)",
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: custom cap 0.70 — signal 0.71 + 0% slippage = 0.71 ≥ 0.70 → blocked
///       on market 505; signal 0.65 + 0% slippage = 0.65 < 0.70 → fills on
///       market 506. Proves the cap is a configurable parameter, not a
///       hardcoded 0.85 constant.
/// FAIL: any other count → threshold is hardcoded.
#[tokio::test]
async fn cap_custom_threshold_applied() {
    let leader_a = wallet(LEADER_A_HEX);
    let mut trades = training_book(leader_a, 1, "A");
    trades.push(raw_trade(
        leader_a,
        505,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.71),
        "blocked",
    ));
    trades.push(raw_trade(
        leader_a,
        506,
        0,
        31,
        1,
        Side::Buy,
        dec!(0.65),
        "allowed",
    ));
    let (fills, _) = run_with(Some(dec!(0.70)), dec!(0), trades);
    assert_eq!(buy_fills_on(&fills, 505).len(), 0, "0.71 ≥ 0.70 cap blocks");
    assert_eq!(buy_fills_on(&fills, 506).len(), 1, "0.65 < 0.70 cap allows");
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────

/// PASS: 1 BUY at 0.86 (blocked) + 1 BUY at 0.65 (allowed) → suppression =
///       1 of 2 = 50%, and the per-quarter map contains exactly one entry
///       (both trades on the same day → same quarter) with value 50.
///
/// Uses `strict_post_training_ranker()` so the leader is not eligible until
/// after day 21 — only the 2 explicit test BUYs reach the cap gate. Under
/// the relaxed ranker, training BUYs would also fire signals and inflate
/// the denominator.
///
/// FAIL: a different pct → counter accounting is off (e.g. the "allowed"
///       record at `record(sim_date, false)` is missing → 100%, or the
///       blocked record is missing → 0%).
#[tokio::test]
async fn suppression_counter_matches_ratio() {
    let leader_a = wallet(LEADER_A_HEX);
    let mut trades = training_book(leader_a, 1, "A");
    trades.push(raw_trade(
        leader_a,
        507,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.86),
        "blocked",
    ));
    trades.push(raw_trade(
        leader_a,
        508,
        0,
        30,
        2,
        Side::Buy,
        dec!(0.65),
        "allowed",
    ));
    let (_, report) = run_with_ranker(
        Some(dec!(0.85)),
        dec!(0),
        strict_post_training_ranker(),
        trades,
    );
    assert_eq!(
        report.high_price_suppression_pct,
        dec!(50),
        "expected 50% suppression (1 of 2 BUYs); got {}",
        report.high_price_suppression_pct
    );
    assert_eq!(
        report.high_price_suppression_by_quarter.len(),
        1,
        "expected exactly 1 quarter entry; got {:?}",
        report.high_price_suppression_by_quarter
    );
    let q_pct = report
        .high_price_suppression_by_quarter
        .values()
        .next()
        .copied()
        .unwrap();
    assert_eq!(q_pct, dec!(50), "per-quarter pct mismatch");
}

// ── Scenario 8 ────────────────────────────────────────────────────────────────

/// PASS: same fixture as scenario 7 but `cap = None` → report's
///       `high_price_suppression_pct` is exactly 0 and the per-quarter map
///       is empty. Validates that `record(sim_date, _)` is *not* called when
///       the gate is disabled — no false positive in the diagnostic.
/// FAIL: non-zero pct or any quarter entries → the tracker was advanced
///       even though the cap was off.
#[tokio::test]
async fn suppression_zero_when_cap_disabled() {
    let leader_a = wallet(LEADER_A_HEX);
    let mut trades = training_book(leader_a, 1, "A");
    trades.push(raw_trade(
        leader_a,
        509,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.86),
        "buy1",
    ));
    trades.push(raw_trade(
        leader_a,
        510,
        0,
        30,
        2,
        Side::Buy,
        dec!(0.65),
        "buy2",
    ));
    let (_, report) = run_with_ranker(None, dec!(0), strict_post_training_ranker(), trades);
    assert_eq!(
        report.high_price_suppression_pct,
        Decimal::ZERO,
        "expected 0% suppression when cap is None; got {}",
        report.high_price_suppression_pct
    );
    assert!(
        report.high_price_suppression_by_quarter.is_empty(),
        "expected empty per-quarter map when cap is None; got {:?}",
        report.high_price_suppression_by_quarter
    );
}

// ── Scenario 9 ────────────────────────────────────────────────────────────────

/// PASS: leader BUYs market 511 at 0.40 (allowed) on day 30, then SELLs same
///       market at 0.95 on day 32 (would be ≥ cap if SELL were gated). The
///       SELL closes the position normally → 1 BUY fill + 1 SELL fill. Proves
///       the gate is BUY-arm only and never touches the SELL close path.
/// FAIL: missing SELL fill → gate leaked into the SELL arm; missing BUY fill
///       → unrelated regression in the BUY arm.
#[tokio::test]
async fn cap_does_not_affect_sells() {
    let leader_a = wallet(LEADER_A_HEX);
    let mut trades = training_book(leader_a, 1, "A");
    trades.push(raw_trade(
        leader_a,
        511,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.40),
        "Abuy",
    ));
    trades.push(raw_trade(
        leader_a,
        511,
        0,
        32,
        1,
        Side::Sell,
        dec!(0.95),
        "Asell",
    ));
    let (fills, _) = run_with(Some(dec!(0.85)), dec!(0.01), trades);
    let mkt_fills = fills_on(&fills, 511);
    assert_eq!(
        mkt_fills.iter().filter(|f| f.side == "buy").count(),
        1,
        "expected 1 BUY on market 511 (0.40 well below cap)",
    );
    assert_eq!(
        mkt_fills.iter().filter(|f| f.side == "sell").count(),
        1,
        "expected 1 SELL on market 511 (cap is BUY-arm only)",
    );
}

// ── Scenario 10 ───────────────────────────────────────────────────────────────

/// PASS: `flat_usd: Some(dec!(1))` + default cap + signal 0.84 + 0% slippage
///       = 0.84 < 0.85 → BUY fills via the flat-USD path. Regression guard
///       that the cap gate doesn't accidentally block traffic in either
///       sizing path when the price is below the threshold.
/// FAIL: 0 BUYs → gate is rejecting valid prices, breaking the flat-USD
///       integration.
#[tokio::test]
async fn cap_composes_with_flat_usd() {
    // `base_config` already sets `flat_usd: Some(dec!(1))` — this scenario
    // just exercises a sub-threshold BUY through that path.
    let trades = trained_then_buy(512, dec!(0.84), "Abuy");
    let (fills, _) = run_with(Some(dec!(0.85)), dec!(0), trades);
    assert_eq!(
        buy_fills_on(&fills, 512).len(),
        1,
        "expected 1 BUY on market 512 (cap allows 0.84; flat-USD path fills 1 contract)",
    );
}
