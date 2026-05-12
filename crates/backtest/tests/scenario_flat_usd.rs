//! Scenario tests for the flat-USD backtest sizing mode (issue #134).
//!
//! Exercises the short-circuit in `simulation.rs` that bypasses Kelly, the
//! per-trade cap, mode clamping, `risk-engine`, and the liquidity clamp when
//! `BacktestConfig::flat_usd` is `Some(_)`. The bypass branch must produce
//! fills with notional `≤ flat_usd` (modulo the degenerate `fill_price >
//! flat` case where one contract is opened best-effort), update bankroll /
//! exposure / open_positions identically to the Kelly path, and leave every
//! Kelly-path counter (`total_signals_evaluated`, `snapshot_prior_*`,
//! `liquidity_*`) at zero.
//!
//! Scenarios:
//!
//! 1. `flat_mode_caps_notional_at_one_dollar` — `flat_usd = $1`; every buy
//!    fill written to the report has `contracts × fill_price ≤ $1`.
//! 2. `flat_mode_bypasses_kelly_counters` — flat mode → all Kelly-path
//!    counters stay at zero (proves the bypass is real, not just a cap).
//! 3. `flat_mode_off_runs_kelly_path` — `flat_usd = None` is a no-op:
//!    `total_signals_evaluated > 0`, regression guard for the Kelly branch.
//! 4. `flat_mode_bankroll_floor_skips_continues` — bankroll cannot cover
//!    `flat_usd` for the full window; simulation completes without panic
//!    and `bankroll_final ≥ 0`.
//! 5. `flat_mode_sell_path_unchanged` — opens close on the leader's sells;
//!    `total_copies` reflects the closed positions.
//! 6. `flat_mode_min_one_contract_when_flat_below_fill_price` — `flat = $0.10`,
//!    `fill_price ≈ $0.355` (with 1% slippage from $0.35); `floor(0.10 /
//!    0.355) = 0`, but `.max(1)` produces 1 contract → fills still occur
//!    (best-effort, notional > flat).
//!
//! Note: the sweep-collapse branch in `main.rs:150` (suppress
//! `kelly_sweep_fractions` when `flat_usd.is_some()`) is exercised by an
//! integration check on the binary rather than a unit test — `main.rs` is not
//! exposed as a library function. Manual verification in the PR body.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::report::WinnerFollowReport;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{
    LeaderboardSnapshots, LiquidityIndex, ResolutionIndex, ScheduleIndex, WalletCache,
};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{LedgerConfig, RankerConfig, snapshot::RawTrade};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::io::BufRead as _;
use tempfile::TempDir;
use time::OffsetDateTime;

// Base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;
const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xcccccccccccccccccccccccccccccccccccccccc";

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

/// 65 markets × (BUY day i @ 0.35, SELL day i+2 @ 0.75) — qualifies the leader
/// for the watchlist with a strong win-rate prior.
fn winner_book(w: WalletAddress) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..65 {
        t.push(make_trade(w, i, i, Side::Buy, dec!(0.35)));
        t.push(make_trade(w, i, i + 2, Side::Sell, dec!(0.75)));
    }
    t
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

fn base_config(dir: &TempDir, flat_usd: Option<Decimal>, bankroll: Decimal) -> BacktestConfig {
    BacktestConfig {
        bootstrap_cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: bankroll,
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
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
        kelly_p_min_snapshots: 0,
        kelly_p_extra_per_missing_snapshot: 0,
        liquidity_take_fraction: dec!(0),
        liquidity_min_required_usd: dec!(200),
        flat_usd,
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: None,
        skip_unknown_operator: false,
        max_signal_price: None,
        strategy: WinnerFollowConfig::default(),
    }
}

/// Drive a simulation with the winner-book fixture and return both the report
/// and the per-fill JSONL trail (so individual notional values can be checked).
fn run(flat_usd: Option<Decimal>, bankroll: Decimal) -> (WinnerFollowReport, Vec<TradeFillJson>) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let leader = wallet(LEADER_HEX);
    let funder = wallet(FUNDER_HEX);

    let trades = winner_book(leader);

    let timeline = make_timeline(&dir, &[(leader, funder)]);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();
    let ledger_config = LedgerConfig::default();

    let config = base_config(&dir, flat_usd, bankroll);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    let report = run_simulation(
        &config,
        trades,
        &timeline,
        &snapshots,
        &resolutions,
        &schedules,
        &liq_index,
        &ranker_config,
        &ledger_config,
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
    contracts: u64,
    fill_price: Decimal,
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: with `flat_usd = $1` and BUY `fill_price = 0.35 × 1.01 ≈ 0.3535`,
///       `contracts = floor(1 / 0.3535) = 2`, `notional = 2 × 0.3535 ≈ 0.707`,
///       which is ≤ $1 for every BUY fill.
/// FAIL: any BUY fill produces `notional > $1`.
#[tokio::test]
async fn flat_mode_caps_notional_at_one_dollar() {
    let (_report, fills) = run(Some(dec!(1)), dec!(1000));

    let buys: Vec<&TradeFillJson> = fills.iter().filter(|f| f.side == "buy").collect();
    assert!(!buys.is_empty(), "expected ≥1 buy fill");

    for f in &buys {
        let notional = Decimal::from(f.contracts) * f.fill_price;
        assert!(
            notional <= dec!(1),
            "flat-mode buy notional must be ≤ $1; got {notional} (contracts={}, \
             fill_price={})",
            f.contracts,
            f.fill_price
        );
    }
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: flat mode bypasses every Kelly-path counter — they all stay at zero.
/// FAIL: any of `total_signals_evaluated`, `snapshot_prior_*`, or
///       `liquidity_*` is incremented while flat mode is active.
#[tokio::test]
async fn flat_mode_bypasses_kelly_counters() {
    let (report, _) = run(Some(dec!(1)), dec!(1000));

    assert_eq!(
        report.total_signals_evaluated, 0,
        "flat path must not reach the Kelly signal site"
    );
    assert_eq!(report.snapshot_prior_signals, 0);
    assert_eq!(report.snapshot_prior_extra_sum, 0);
    assert_eq!(report.liquidity_clamps_fired, 0);
    assert_eq!(report.liquidity_clamp_contracts_reduced, 0);
    assert_eq!(report.liquidity_below_floor_bypasses, 0);
    assert_eq!(report.liquidity_unknown_markets, 0);
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: `flat_usd = None` keeps the existing Kelly path — at least one signal
///       reaches `strategy.evaluate()` and `total_signals_evaluated > 0`.
/// FAIL: counter is zero, meaning the flat-mode branch is misrouted or the
///       Kelly path was accidentally removed.
#[tokio::test]
async fn flat_mode_off_runs_kelly_path() {
    let (report, _) = run(None, dec!(1000));

    assert!(
        report.total_signals_evaluated > 0,
        "Kelly path must execute when flat_usd is None"
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: with `bankroll = $0.30` and `flat_usd = $1` (per-fill notional ≈
///       $0.707 at `fill_price = 0.3535`), every BUY signal is below the
///       all-or-nothing floor and the trade is skipped. No fills, no copies,
///       `bankroll_final == bankroll_initial`.
/// FAIL: simulation panics, any fill is produced, or `bankroll_final !=
///       bankroll_initial`.
#[tokio::test]
async fn flat_mode_bankroll_floor_skips_continues() {
    let (report, fills) = run(Some(dec!(1)), dec!(0.30));

    assert_eq!(
        report.bankroll_final, report.bankroll_initial,
        "bankroll must not move when every BUY is skipped by the floor"
    );
    assert_eq!(
        report.total_copies, 0,
        "no copies should complete when every BUY is skipped"
    );
    assert!(
        fills.iter().filter(|f| f.side == "buy").count() == 0,
        "no buy fills should be written when every BUY is skipped"
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: opens followed by sells produce realized PnL — `total_copies > 0`.
/// FAIL: sells never close anything (e.g., the BUY branch broke open-position
///       insertion).
#[tokio::test]
async fn flat_mode_sell_path_unchanged() {
    let (report, fills) = run(Some(dec!(1)), dec!(1000));

    let sells = fills.iter().filter(|f| f.side == "sell").count();
    assert!(sells > 0, "expected ≥1 sell fill; got {sells}");
    assert!(
        report.total_copies > 0,
        "sells must register as completed copies"
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: `flat = $0.10`, `fill_price ≈ $0.3535` → `floor(0.10 / 0.3535) = 0`,
///       but `.max(1)` opens 1 contract (best-effort). Each fill has
///       `notional ≈ $0.3535 > flat` — the degenerate case explicitly
///       documented in the issue.
/// FAIL: no fills produced (would mean `.max(1)` was dropped).
#[tokio::test]
async fn flat_mode_min_one_contract_when_flat_below_fill_price() {
    let (_report, fills) = run(Some(dec!(0.10)), dec!(1000));

    let buys: Vec<&TradeFillJson> = fills.iter().filter(|f| f.side == "buy").collect();
    assert!(
        !buys.is_empty(),
        "expected ≥1 buy fill (1 contract, best-effort)"
    );

    for f in &buys {
        assert_eq!(
            f.contracts, 1,
            "best-effort 1-contract fill when flat < fill_price"
        );
    }
}
