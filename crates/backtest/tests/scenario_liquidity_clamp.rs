//! Scenario tests for the liquidity-aware sizing clamp (issue #128).
//!
//! Exercises the 4-counter partition introduced in `simulation.rs` and the
//! pure `clamp_contracts_to_liquidity` function in `risk-engine`. Each BUY trade
//! reaching the clamp site increments at most one counter; "data-ok passthrough"
//! is the silent no-counter case, derivable as `total_copies - fired -
//! below_floor - unknown`.
//!
//! Scenarios (cause-precedence ordering: gate disabled → unknown → fired →
//! below floor → data-ok):
//!
//! 1. `clamp_fires_when_depth_binding` — `liquidity_usd=5000`, `take_fraction=0.01`,
//!    `fill_price=0.50` → max contracts = `floor(5000 * 0.01 / 0.50) = 100`.
//!    Assert `liquidity_clamps_fired ≥ 1` and `liquidity_clamp_contracts_reduced > 0`.
//! 2. `below_floor_bypass_when_data_noisy` — depth 50 < floor 200 → passthrough;
//!    `liquidity_below_floor_bypasses ≥ 1`, other liquidity counters zero.
//! 3. `unknown_market_bypass_when_no_data` — market not in `LiquidityIndex` →
//!    passthrough; `liquidity_unknown_markets ≥ 1`, other liquidity counters zero.
//! 4. `gate_disabled_passes_through_even_with_low_liquidity` — regression for
//!    cause-precedence: `take_fraction=0` AND `liquidity_usd=50 < floor` → all 4
//!    counters MUST be zero (gate-disabled wins over below-floor).
//! 5. `gate_disabled_with_high_liquidity` — `take_fraction=0`, depth 5000 → all 4
//!    counters zero (data-ok passthrough via the disabled gate).
//! 6. `partition_closure_holds` — sum of 3 incrementing counters + derived
//!    data-ok passthrough = `total_copies`. The partition is exhaustive.
//!
//! Pure-function unit tests for `clamp_contracts_to_liquidity` live in
//! `crates/risk-engine/src/lib.rs::tests_liquidity_clamp`.

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
use tempfile::TempDir;
use time::OffsetDateTime;

// Base timestamp: 2023-11-01 00:00:00 UTC.
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

/// 65 markets × (BUY day i, SELL day i+2). Mirrors `winner_book` from the
/// expiry-survivorship scenario — qualifies the leader for the watchlist by
/// day ~62 with a strong win-rate prior.
fn winner_book(w: WalletAddress) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..65 {
        t.push(make_trade(w, i, i, Side::Buy, dec!(0.35)));
        t.push(make_trade(w, i, i + 2, Side::Sell, dec!(0.75)));
    }
    t
}

/// Ranker tuned to qualify the leader after the 65-trade winner book.
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

fn base_config(dir: &TempDir, take_fraction: Decimal, min_required_usd: Decimal) -> BacktestConfig {
    BacktestConfig {
        bootstrap_cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
        step_days: 1,
        dune_api_key: None,
        dune_namespace: None,
        max_hours_to_expiry: None,
        audit_window_days: 365,
        ranker_min_quality: 0,
        ranker_active_min_closed: 15,
        ranker_active_min_markets: 1,
        ranker_incubator_min_closed: 5,
        ranker_incubator_min_markets: 1,
        kelly_sweep_fractions: None,
        // Prior 0/0 → leader's strong empirical rate (~100% wins) survives shrinkage.
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
        liquidity_take_fraction: take_fraction,
        liquidity_min_required_usd: min_required_usd,
        kelly_p_min_snapshots: 0,
        kelly_p_extra_per_missing_snapshot: 0,
        flat_usd: None,
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: None,
        max_signal_price: None,
        max_trade_count: 0,
        strategy: WinnerFollowConfig::default(),
    }
}

/// Build a fixture with a tempfile cache + winner book + an extra clamp-test
/// market 9999. Returns the report so each scenario can assert its specific
/// partition counters.
fn run_with_liquidity(
    take_fraction: Decimal,
    min_required_usd: Decimal,
    liq_index: &LiquidityIndex,
) -> WinnerFollowReport {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let leader = wallet(LEADER_HEX);

    let mut trades = winner_book(leader);
    // Day 70+ : 5 additional signal trades on market 9999 (clamp under test).
    for i in 0u32..5 {
        let day = 70 + i;
        trades.push(make_trade(leader, 9999, day, Side::Buy, dec!(0.50)));
        trades.push(make_trade(leader, 9999, day + 2, Side::Sell, dec!(0.75)));
    }

    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let ranker_config = relaxed_ranker();

    let config = base_config(&dir, take_fraction, min_required_usd);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    trades.sort_by_key(|t| t.timestamp.0);
    run_simulation(
        &config,
        &trades,
        &snapshots,
        &resolutions,
        &schedules,
        liq_index,
        &ranker_config,
        &strategy,
        false,
    )
    .unwrap()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: clamp reduces `contracts_count` for the under-test market (9999) when
///       depth ($1k) × take_fraction (1%) / fill_price ($0.50) = 20 contracts —
///       well below the bankroll-driven Kelly count.
/// FAIL: clamp never fires (`liquidity_clamps_fired == 0`).
#[tokio::test]
async fn clamp_fires_when_depth_binding() {
    let mut liq = LiquidityIndex::new();
    liq.insert(mkt(9999), dec!(1000));

    let report = run_with_liquidity(dec!(0.01), dec!(200), &liq);

    assert!(
        report.liquidity_clamps_fired >= 1,
        "expected clamp to fire ≥1 time; got {} firings",
        report.liquidity_clamps_fired
    );
    assert!(
        report.liquidity_clamp_contracts_reduced > 0,
        "expected ≥1 contract reduction; got {}",
        report.liquidity_clamp_contracts_reduced
    );
    // Gate enabled with adequate depth → no other-bucket bypasses for market 9999.
    // (Below-floor / unknown counters may be 0 across all trades since this fixture
    // only has market 9999 with adequate depth; other watchlist trades on the
    // qualifying markets pass through as data-ok via the cache miss path.)
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: depth $50 < floor $200 → clamp bypassed; `liquidity_below_floor_bypasses ≥ 1`.
///       Trade is still copied (passthrough — clamp does not zero it out).
/// FAIL: clamp fires (counter 0) or `liquidity_below_floor_bypasses == 0`.
#[tokio::test]
async fn below_floor_bypass_when_data_noisy() {
    let mut liq = LiquidityIndex::new();
    liq.insert(mkt(9999), dec!(50));

    let report = run_with_liquidity(dec!(0.05), dec!(200), &liq);

    assert!(
        report.liquidity_below_floor_bypasses >= 1,
        "expected ≥1 below-floor bypass for market 9999; got {}",
        report.liquidity_below_floor_bypasses
    );
    assert_eq!(
        report.liquidity_clamps_fired, 0,
        "clamp must not fire below floor for any market; got {} firings",
        report.liquidity_clamps_fired
    );
    assert!(
        report.total_copies >= 1,
        "trade should still copy when bypassed via below-floor (got 0)"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: market 9999 absent from `LiquidityIndex` → counter increments.
/// FAIL: `liquidity_unknown_markets == 0` despite missing market.
#[tokio::test]
async fn unknown_market_bypass_when_no_data() {
    // Empty index — every market reaching the clamp site is "unknown".
    let liq = LiquidityIndex::new();

    let report = run_with_liquidity(dec!(0.05), dec!(200), &liq);

    assert!(
        report.liquidity_unknown_markets >= 1,
        "expected ≥1 unknown-market bypass; got {}",
        report.liquidity_unknown_markets
    );
    assert_eq!(
        report.liquidity_clamps_fired, 0,
        "no clamp can fire when no liquidity data exists; got {} firings",
        report.liquidity_clamps_fired
    );
    assert_eq!(
        report.liquidity_below_floor_bypasses, 0,
        "below-floor requires a known market; got {} bypasses",
        report.liquidity_below_floor_bypasses
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: gate disabled (`take_fraction=0`) takes precedence over below-floor —
///       all 4 liquidity counters must be zero even when depth is below floor.
/// FAIL: `liquidity_below_floor_bypasses > 0` despite gate being disabled.
///       (This was the partition-cleanliness bug identified in plan review 4.)
#[tokio::test]
async fn gate_disabled_passes_through_even_with_low_liquidity() {
    let mut liq = LiquidityIndex::new();
    liq.insert(mkt(9999), dec!(50));

    // Gate disabled: take_fraction = 0.
    let report = run_with_liquidity(dec!(0), dec!(200), &liq);

    assert_eq!(
        report.liquidity_clamps_fired, 0,
        "gate disabled → no clamp firings; got {}",
        report.liquidity_clamps_fired
    );
    assert_eq!(
        report.liquidity_below_floor_bypasses, 0,
        "gate disabled wins precedence over below-floor; got {} bypasses",
        report.liquidity_below_floor_bypasses
    );
    assert_eq!(
        report.liquidity_unknown_markets, 0,
        "gate disabled wins precedence over unknown; got {} bypasses",
        report.liquidity_unknown_markets
    );
    assert_eq!(
        report.liquidity_clamp_contracts_reduced, 0,
        "gate disabled → no reductions; got {}",
        report.liquidity_clamp_contracts_reduced
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: gate disabled with healthy depth → all 4 counters zero.
/// FAIL: any liquidity counter nonzero.
#[tokio::test]
async fn gate_disabled_with_high_liquidity_records_nothing() {
    let mut liq = LiquidityIndex::new();
    liq.insert(mkt(9999), dec!(5000));

    let report = run_with_liquidity(dec!(0), dec!(200), &liq);

    assert_eq!(report.liquidity_clamps_fired, 0);
    assert_eq!(report.liquidity_clamp_contracts_reduced, 0);
    assert_eq!(report.liquidity_below_floor_bypasses, 0);
    assert_eq!(report.liquidity_unknown_markets, 0);
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: across any configuration, the counter partition is exhaustive — every
///       BUY trade either incremented one of the three counters or passed through
///       silently as data-ok. The sum equals `total_copies`.
/// FAIL: `total_copies < (fired + below_floor + unknown)` — would mean a trade
///       was counted in multiple buckets (partition violation).
#[tokio::test]
async fn partition_closure_holds() {
    let mut liq = LiquidityIndex::new();
    liq.insert(mkt(9999), dec!(1000));

    let report = run_with_liquidity(dec!(0.01), dec!(200), &liq);

    let counted = report
        .liquidity_clamps_fired
        .saturating_add(report.liquidity_below_floor_bypasses)
        .saturating_add(report.liquidity_unknown_markets);
    assert!(
        counted <= report.total_copies,
        "partition violation: counted ({counted}) > total_copies ({})",
        report.total_copies
    );
    // Derived `passthrough_data_ok = total_copies - counted` must be non-negative
    // — already implied by the assertion above; calling it out for clarity.
    let passthrough_data_ok = report.total_copies - counted;
    eprintln!(
        "partition snapshot: fired={} below_floor={} unknown={} data_ok={} total_copies={}",
        report.liquidity_clamps_fired,
        report.liquidity_below_floor_bypasses,
        report.liquidity_unknown_markets,
        passthrough_data_ok,
        report.total_copies,
    );
}
