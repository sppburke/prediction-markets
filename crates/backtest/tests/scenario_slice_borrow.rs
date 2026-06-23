//! Operator/regression tests for issue #156: `run_simulation` and the
//! Kelly-fraction sweep must share a single pre-sorted `&[RawTrade]` slice
//! across rayon workers instead of cloning a `Vec<RawTrade>` per fraction.
//!
//! Scenarios:
//! 1. `pre_sorted_slice_produces_valid_report` — sanity check that
//!    `run_simulation(&sorted_slice, ...)` returns a non-empty report on a
//!    small winner fixture (the happy path of the new contract).
//! 2. `sweep_with_shared_slice_produces_one_run_per_fraction` — a 3-fraction
//!    sweep using `SweepContext::all_trades = &[RawTrade]` and
//!    `run_one_kelly_fraction` produces exactly 3 distinct reports, proving
//!    the slice is shared safely across rayon workers without per-thread
//!    clones.
//! 3. `sweep_reports_are_deterministic_across_repeated_runs` — running the
//!    same sweep twice on the identical slice yields field-equal reports per
//!    Kelly fraction. Guards against accidental mutation of the shared slice
//!    or per-worker state bleeding through the borrowed input.
//! 4. `unsorted_input_triggers_debug_assert` (debug builds only) — confirms
//!    the precondition `all_trades.is_sorted_by_key(|t| t.timestamp.0)` is
//!    enforced via `debug_assert!`. The test is gated to `cfg(debug_assertions)`
//!    because release builds elide the assertion (this is the documented
//!    contract; see the `# Precondition` block on `run_simulation`).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::config::BacktestConfig;
use pe_backtest::report::KellySweepRun;
use pe_backtest::simulation::{SweepContext, run_one_kelly_fraction, run_simulation};
use pe_bootstrap::cache::{LeaderboardSnapshots, LiquidityIndex, ResolutionIndex, ScheduleIndex};
use pe_core_types::{
    ContractQty, KellyFraction, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{RankerConfig, snapshot::RawTrade};
use rayon::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

// ── fixtures ──────────────────────────────────────────────────────────────────

const WINNER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
// Fixed base timestamp (2023-11-01 UTC). Deterministic — no SystemTime::now().
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

/// 65 markets × 2 trades each (BUY day D, SELL day D+2). Always-profitable
/// fixture so the resulting report has non-zero `total_copies` once the
/// leader passes the relaxed ranker thresholds.
///
/// **Output order is intentionally interleaved** (buy(0,0), sell(0,2),
/// buy(1,1), sell(1,3), …), i.e. NOT sorted by `timestamp.0`. Callers must
/// sort before passing into `run_simulation`.
fn unsorted_winner_trades(winner: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..65 {
        trades.push(make_trade(winner, i, i, Side::Buy, dec!(0.35)));
        trades.push(make_trade(winner, i, i + 2, Side::Sell, dec!(0.75)));
    }
    trades
}

fn build_fixture() -> (TempDir, Vec<RawTrade>) {
    let winner = wallet(WINNER_HEX);
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();
    let mut trades = unsorted_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);
    (dir, trades)
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: `run_simulation(&sorted_slice, ...)` succeeds and reports at least
///       one copied trade.
/// FAIL: `run_simulation` errors, panics, or reports zero copies on a fixture
///       that is known to be eligible under the relaxed ranker.
#[tokio::test]
async fn pre_sorted_slice_produces_valid_report() {
    let (dir, trades) = build_fixture();
    let config = base_config(&dir);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    let report = run_simulation(
        &config,
        &trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &strategy,
        false,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "expected the winner fixture to produce ≥1 copy; got {}",
        report.total_copies
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: a 3-fraction parallel sweep on a shared `&[RawTrade]` slice produces
///       exactly 3 `KellySweepRun` entries with distinct kelly_fraction values.
/// FAIL: fewer/more runs returned, or any per-fraction run errors out.
#[tokio::test]
async fn sweep_with_shared_slice_produces_one_run_per_fraction() {
    let (dir, trades) = build_fixture();
    let config = base_config(&dir);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &schedules,
        liq_index: &liq_index,
        ranker_config: &ranker_config,
    };

    let fractions = [
        KellyFraction::new(dec!(0.10)).unwrap(),
        KellyFraction::new(dec!(0.50)).unwrap(),
        KellyFraction::new(dec!(1.00)).unwrap(),
    ];

    let runs: Vec<KellySweepRun> = fractions
        .par_iter()
        .map(|&kf| run_one_kelly_fraction(kf, &ctx))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(runs.len(), 3, "expected one run per Kelly fraction");
    let mut kfs: Vec<Decimal> = runs.iter().map(|r| r.kelly_fraction.0).collect();
    kfs.sort();
    assert_eq!(
        kfs,
        vec![dec!(0.10), dec!(0.50), dec!(1.00)],
        "each fraction must appear in the run set exactly once"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: two identical sweeps over the shared slice produce field-equal
///       per-fraction (`total_pnl_usd`, `total_copies`) results, proving that
///       no per-worker state mutates the shared slice or bleeds across runs.
/// FAIL: any per-fraction `(total_pnl_usd, total_copies)` differs between
///       the two sweeps.
#[tokio::test]
async fn sweep_reports_are_deterministic_across_repeated_runs() {
    let (dir, trades) = build_fixture();
    let config = base_config(&dir);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &schedules,
        liq_index: &liq_index,
        ranker_config: &ranker_config,
    };

    let fractions = [
        KellyFraction::new(dec!(0.10)).unwrap(),
        KellyFraction::new(dec!(0.50)).unwrap(),
        KellyFraction::new(dec!(1.00)).unwrap(),
    ];

    let run_once = || -> Vec<(Decimal, Decimal, u64)> {
        let mut runs: Vec<KellySweepRun> = fractions
            .par_iter()
            .map(|&kf| run_one_kelly_fraction(kf, &ctx))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        runs.sort_unstable_by_key(|r| r.kelly_fraction);
        runs.into_iter()
            .map(|r| {
                (
                    r.kelly_fraction.0,
                    r.report.total_pnl_usd,
                    r.report.total_copies,
                )
            })
            .collect()
    };

    let first = run_once();
    let second = run_once();

    assert_eq!(
        first, second,
        "per-fraction (kelly, total_pnl_usd, total_copies) must be field-equal across runs"
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS (debug builds): `run_simulation` panics via `debug_assert!` when handed
///       an unsorted slice. Documents the contract enforcement for the
///       precondition `all_trades.is_sorted_by_key(|t| t.timestamp.0)`.
/// FAIL: unsorted input is silently accepted in debug builds.
///
/// Skipped on release builds (`cfg(not(debug_assertions))`) because
/// `debug_assert!` is elided there — this is the documented contract: callers
/// take responsibility for sorting, and release builds will silently produce
/// incorrect results if the precondition is violated (see the `# Precondition`
/// block on `run_simulation`).
#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "all_trades must be sorted")]
async fn unsorted_input_triggers_debug_assert() {
    let winner = wallet(WINNER_HEX);
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    // Deliberately do NOT sort.
    let trades = unsorted_winner_trades(winner);

    let config = base_config(&dir);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    // This call must panic in debug builds due to `debug_assert!`.
    let _ = run_simulation(
        &config,
        &trades,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &strategy,
        false,
    );
}
