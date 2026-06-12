//! Scenario and operator tests for the Kelly-fraction sweep feature.
//!
//! Scenarios:
//! 1. `override_changes_sizing_vs_default` — `kelly_fraction_override: Some(kf)` causes
//!    `WinnerFollowStrategy::evaluate()` to produce more contracts than the default fraction.
//!    Tested at the evaluate() layer to avoid the simulation's per-trade risk cap.
//! 2. `sweep_produces_correct_run_count` — a 3-fraction sweep via `run_one_kelly_fraction`
//!    produces exactly 3 runs in `KellySweepReport`.
//! 3. `higher_fraction_yields_more_contracts` — across [0.10, 0.25, 0.50, 0.75, 1.0],
//!    contract count from evaluate() increases monotonically with the Kelly fraction.
//! 4. `to_markdown_table_covers_all_runs` — the markdown table contains one row per run.
//! 5. `sweep_suppresses_per_run_output` — per-run report.json is NOT written in sweep mode.
//! 6. `parallel_sweep_matches_sequential` — equivalence: rayon `par_iter` sweep produces
//!    identical results (per-fraction PnL, copy count) to a sequential reference run on the
//!    same fixture, after sorting by `kelly_fraction`.
//! 7. `parallel_sweep_is_deterministic` — parallel sweep run twice on identical inputs
//!    yields field-equal results (no floating-point reordering, no race-induced state).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, HashMap};

use pe_backtest::config::BacktestConfig;
use pe_backtest::report::{KellySweepReport, KellySweepRun, WinnerFollowReport};
use pe_backtest::simulation::{SweepContext, run_one_kelly_fraction, run_simulation};
use pe_bootstrap::cache::{LeaderboardSnapshots, LiquidityIndex, ResolutionIndex, ScheduleIndex};
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, ContractQty, KellyFraction, LeaderAction, MarketId, OutcomeId, Price, Probability,
    ProbabilityPpm, Quantity, Side, SourceTimestamp, SourceTradeId, TraderId, VenueId,
    VenueMarketId, WalletAddress, WinnerFollowSignalKind,
};
use pe_risk_engine::{RiskSnapshot, TradingMode};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{RankerConfig, snapshot::RawTrade};
use rayon::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

// ── fixtures ──────────────────────────────────────────────────────────────────

const WINNER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
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
        strategy: WinnerFollowConfig::default(),
    }
}

/// 65 markets × 2 trades each (BUY day D, SELL day D+2). Buys at 0.35, sells at 0.75.
fn generate_winner_trades(winner: WalletAddress) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..65 {
        trades.push(make_trade(winner, i, i, Side::Buy, dec!(0.35)));
        trades.push(make_trade(winner, i, i + 2, Side::Sell, dec!(0.75)));
    }
    trades
}

fn kf(d: Decimal) -> KellyFraction {
    KellyFraction::new(d).unwrap()
}

/// Build a NormalLeaderFollow BUY signal at price 0.40.
fn thin_edge_signal() -> LeaderSignal {
    let leader = TraderId(wallet(WINNER_HEX));
    LeaderSignal {
        leader,
        operator_id: None,
        venue: VenueId::polymarket(),
        market_id: MarketId(VenueMarketId("sweep-mkt-001".to_string())),
        outcome_id: OutcomeId(0),
        action: LeaderAction::Entry,
        leader_side: Side::Buy,
        leader_price: Price::new(dec!(0.40)).unwrap(),
        leader_size: Quantity(ContractQty(100)),
        observed_at: OffsetDateTime::from_unix_timestamp(BASE_UNIX).unwrap(),
        received_at: OffsetDateTime::from_unix_timestamp(BASE_UNIX).unwrap(),
        reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
        signal_kind: WinnerFollowSignalKind::NormalLeaderFollow,
        inherited_prior: None,
        source_trade_id: SourceTradeId("sweep-tid-001".to_string()),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
    }
}

/// Clean risk snapshot: all exposures zero, source healthy, no anti-gaming flags.
fn clean_risk_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        onchain_source_status: SourceStatus::Healthy,
        copy_latency_p95_ms: 500,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(0),
        per_trade_cap_bps: 25,
    }
}

// ── Scenario 1 ─────────────────────────────────────────────────────────────────

/// PASS: `kelly_fraction_override: Some(1.0)` causes `evaluate()` to size more contracts
///       than the default `KELLY_PAPER_BACKTEST = 0.10` on an identical signal.
/// FAIL: both strategies return the same contract count.
#[test]
fn override_changes_sizing_vs_default() {
    let signal = thin_edge_signal();
    let p = Probability::new(dec!(0.421)).unwrap();
    let bankroll = Decimal::from(10_000u32);

    let default_strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());
    let override_strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        kelly_fraction_override: Some(KellyFraction::ONE),
        ..WinnerFollowConfig::default()
    });

    let intent_default = default_strategy
        .evaluate(
            &signal,
            p,
            clean_risk_snapshot(),
            bankroll,
            ExecutionMode::Paper,
        )
        .expect("default strategy should approve");

    let intent_override = override_strategy
        .evaluate(
            &signal,
            p,
            clean_risk_snapshot(),
            bankroll,
            ExecutionMode::Paper,
        )
        .expect("override strategy should approve");

    assert!(
        intent_override.contracts.0 > intent_default.contracts.0,
        "expected full-Kelly contracts ({}) > 0.10-Kelly contracts ({})",
        intent_override.contracts.0,
        intent_default.contracts.0,
    );
}

// ── Scenario 2 ─────────────────────────────────────────────────────────────────

/// PASS: a 3-fraction sweep via `run_one_kelly_fraction` produces a `KellySweepReport`
///       with exactly 3 runs, each tagged with its own fraction.
/// FAIL: run count ≠ 3, or fractions are mistagged.
#[tokio::test]
async fn sweep_produces_correct_run_count() {
    let winner = wallet(WINNER_HEX);
    let mut trades = generate_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);

    let fractions = vec![kf(dec!(0.10)), kf(dec!(0.50)), kf(dec!(1.0))];
    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        kelly_sweep_fractions: Some(fractions.clone()),
        ..base_config(&dir)
    };
    let resolutions = ResolutionIndex::new();
    let snapshots = LeaderboardSnapshots::default();
    let ranker = relaxed_ranker();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &ScheduleIndex::new(),
        liq_index: &LiquidityIndex::new(),
        ranker_config: &ranker,
    };

    let mut runs: Vec<KellySweepRun> = Vec::new();
    for &kf in &fractions {
        runs.push(run_one_kelly_fraction(kf, &ctx).unwrap());
    }

    assert_eq!(runs.len(), 3, "expected 3 sweep runs");
    assert_eq!(runs[0].kelly_fraction, kf(dec!(0.10)));
    assert_eq!(runs[1].kelly_fraction, kf(dec!(0.50)));
    assert_eq!(runs[2].kelly_fraction, kf(dec!(1.0)));
}

// ── Scenario 3 ─────────────────────────────────────────────────────────────────

/// PASS: across fractions [0.10, 0.25, 0.50, 0.75, 1.0], the contract count returned
///       by `evaluate()` increases strictly monotonically.
/// FAIL: any adjacent pair has equal or decreasing contracts.
#[test]
fn higher_fraction_yields_more_contracts() {
    let signal = thin_edge_signal();
    let p = Probability::new(dec!(0.421)).unwrap();
    let bankroll = Decimal::from(10_000u32);
    let fractions = [dec!(0.10), dec!(0.25), dec!(0.50), dec!(0.75), dec!(1.0)];

    let mut counts: Vec<u64> = Vec::with_capacity(fractions.len());

    for &frac in &fractions {
        let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
            kelly_fraction_override: Some(KellyFraction::new(frac).unwrap()),
            ..WinnerFollowConfig::default()
        });
        let intent = strategy
            .evaluate(
                &signal,
                p,
                clean_risk_snapshot(),
                bankroll,
                ExecutionMode::Paper,
            )
            .unwrap_or_else(|e| panic!("evaluate failed for fraction {frac}: {e}"));
        counts.push(intent.contracts.0);
    }

    for i in 1..counts.len() {
        assert!(
            counts[i] > counts[i - 1],
            "expected contracts[{i}] ({}) > contracts[{}] ({}) — contract count must increase with Kelly fraction",
            counts[i],
            i - 1,
            counts[i - 1],
        );
    }
}

// ── Scenario 4 ─────────────────────────────────────────────────────────────────

/// PASS: `KellySweepReport::to_markdown_table()` emits exactly one data row per run
///       plus two header rows.
/// FAIL: row count differs from expected.
#[test]
fn to_markdown_table_covers_all_runs() {
    let make_run = |frac: Decimal| KellySweepRun {
        kelly_fraction: KellyFraction::new(frac).unwrap(),
        report: WinnerFollowReport {
            total_pnl_usd: dec!(100),
            sharpe_ratio: dec!(1.5),
            max_drawdown_pct: dec!(10),
            total_copies: 50,
            win_rate_pct: dec!(60),
            per_operator_pnl: HashMap::new(),
            simulation_start: OffsetDateTime::from_unix_timestamp(BASE_UNIX).unwrap(),
            simulation_end: OffsetDateTime::from_unix_timestamp(BASE_UNIX + 86_400).unwrap(),
            bankroll_initial: dec!(10_000),
            bankroll_final: dec!(10_100),
            slippage_assumption_bps: 100,
            open_at_horizon: 0,
            expiry_filter_suppression_pct: dec!(0),
            expiry_suppression_by_quarter: BTreeMap::new(),
            high_price_suppression_pct: dec!(0),
            high_price_suppression_by_quarter: BTreeMap::new(),
            liquidity_clamps_fired: 0,
            liquidity_clamp_contracts_reduced: 0,
            liquidity_below_floor_bypasses: 0,
            liquidity_unknown_markets: 0,
            total_signals_evaluated: 0,
            snapshot_prior_signals: 0,
            snapshot_prior_extra_sum: 0,
            resolved_config: None,
        },
    };

    let sweep = KellySweepReport {
        runs: vec![
            make_run(dec!(0.10)),
            make_run(dec!(0.50)),
            make_run(dec!(1.0)),
        ],
        cache_path: std::path::PathBuf::from("/tmp/cache.db"),
        executed_at: OffsetDateTime::from_unix_timestamp(BASE_UNIX).unwrap(),
        resolved_config: None,
    };

    let table = sweep.to_markdown_table();
    let lines: Vec<&str> = table.lines().collect();
    assert_eq!(
        lines.len(),
        5,
        "expected 5 lines (2 header + 3 data), got {}: {table}",
        lines.len()
    );
    assert!(lines[2].contains("0.10"), "row 0 should have 0.10");
    assert!(lines[3].contains("0.50"), "row 1 should have 0.50");
    assert!(lines[4].contains("1.00"), "row 2 should have 1.00");
}

// ── Scenario 5 ─────────────────────────────────────────────────────────────────

/// PASS: with `write_output = false`, no `report.json` is written to the output directory.
/// FAIL: `report.json` exists after a `write_output = false` run.
#[tokio::test]
async fn sweep_suppresses_per_run_output() {
    let winner = wallet(WINNER_HEX);
    let mut trades = generate_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);

    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();
    let config = base_config(&dir);

    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        kelly_fraction_override: Some(KellyFraction::ONE),
        ..WinnerFollowConfig::default()
    });
    run_simulation(
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

    let report_path = dir.path().join("output").join("report.json");
    assert!(
        !report_path.exists(),
        "report.json must not be written when write_output=false (sweep mode)"
    );
}

// ── Scenario 6 ─────────────────────────────────────────────────────────────────

/// Equivalence: rayon `par_iter` sweep produces identical per-fraction results
/// to a sequential reference run on the same fixture.
///
/// PASS: after sort by `kelly_fraction`, each `(kelly_fraction, total_pnl_usd, total_copies)`
///       triple is identical between sequential and parallel result vectors.
/// FAIL: any field differs — implies parallelism introduced shared mutable state,
///       FP non-associativity in a sum reduction, or an order-dependent code path.
#[tokio::test]
async fn parallel_sweep_matches_sequential() {
    let winner = wallet(WINNER_HEX);
    let mut trades = generate_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);
    let fractions = vec![kf(dec!(0.10)), kf(dec!(0.50)), kf(dec!(1.0))];
    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        kelly_sweep_fractions: Some(fractions.clone()),
        ..base_config(&dir)
    };
    let resolutions = ResolutionIndex::new();
    let snapshots = LeaderboardSnapshots::default();
    let ranker = relaxed_ranker();
    let schedules = ScheduleIndex::new();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &schedules,
        liq_index: &LiquidityIndex::new(),
        ranker_config: &ranker,
    };

    // Sequential reference.
    let mut sequential: Vec<KellySweepRun> = Vec::new();
    for &kf in &fractions {
        sequential.push(run_one_kelly_fraction(kf, &ctx).unwrap());
    }
    sequential.sort_unstable_by_key(|r| r.kelly_fraction);

    // Parallel under test.
    let mut parallel: Vec<KellySweepRun> = fractions
        .par_iter()
        .map(|&kf| run_one_kelly_fraction(kf, &ctx))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    parallel.sort_unstable_by_key(|r| r.kelly_fraction);

    assert_eq!(
        sequential.len(),
        parallel.len(),
        "run count differs: sequential={}, parallel={}",
        sequential.len(),
        parallel.len()
    );
    for (seq, par) in sequential.iter().zip(parallel.iter()) {
        assert_eq!(
            seq.kelly_fraction, par.kelly_fraction,
            "kelly_fraction differs at sorted position"
        );
        assert_eq!(
            seq.report.total_pnl_usd, par.report.total_pnl_usd,
            "total_pnl_usd differs at kf={}: seq={}, par={}",
            seq.kelly_fraction.0, seq.report.total_pnl_usd, par.report.total_pnl_usd
        );
        assert_eq!(
            seq.report.total_copies, par.report.total_copies,
            "total_copies differs at kf={}: seq={}, par={}",
            seq.kelly_fraction.0, seq.report.total_copies, par.report.total_copies
        );
        assert_eq!(
            seq.report.win_rate_pct, par.report.win_rate_pct,
            "win_rate_pct differs at kf={}",
            seq.kelly_fraction.0
        );
        assert_eq!(
            seq.report.bankroll_final, par.report.bankroll_final,
            "bankroll_final differs at kf={}",
            seq.kelly_fraction.0
        );
        assert_eq!(
            seq.report.open_at_horizon, par.report.open_at_horizon,
            "open_at_horizon differs at kf={}",
            seq.kelly_fraction.0
        );
    }
}

// ── Scenario 7 ─────────────────────────────────────────────────────────────────

/// Determinism: parallel sweep run twice on identical inputs yields field-equal
/// results. Catches FP reordering and any race-induced state.
///
/// PASS: both runs produce the same `(kelly_fraction, total_pnl_usd, total_copies,
///       sharpe_ratio, max_drawdown_pct, bankroll_final)` after sorting by fraction.
/// FAIL: any field differs across the two runs.
///
/// Note: does NOT compare serialized JSON bytes — `WinnerFollowReport.per_operator_pnl`
/// is a `HashMap<String, Decimal>` which iterates in `RandomState`-driven order, making
/// byte-identical serialization impossible without a `BTreeMap` migration (out of scope).
#[tokio::test]
async fn parallel_sweep_is_deterministic() {
    let winner = wallet(WINNER_HEX);
    let mut trades = generate_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);
    let fractions = vec![
        kf(dec!(0.10)),
        kf(dec!(0.25)),
        kf(dec!(0.50)),
        kf(dec!(0.75)),
        kf(dec!(1.0)),
    ];
    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        kelly_sweep_fractions: Some(fractions.clone()),
        ..base_config(&dir)
    };
    let resolutions = ResolutionIndex::new();
    let snapshots = LeaderboardSnapshots::default();
    let ranker = relaxed_ranker();
    let schedules = ScheduleIndex::new();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &schedules,
        liq_index: &LiquidityIndex::new(),
        ranker_config: &ranker,
    };

    let run_parallel = || -> Vec<KellySweepRun> {
        let mut v: Vec<KellySweepRun> = fractions
            .par_iter()
            .map(|&kf| run_one_kelly_fraction(kf, &ctx))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        v.sort_unstable_by_key(|r| r.kelly_fraction);
        v
    };

    let first = run_parallel();
    let second = run_parallel();

    assert_eq!(first.len(), 5);
    assert_eq!(first.len(), second.len());
    for (a, b) in first.iter().zip(second.iter()) {
        assert_eq!(a.kelly_fraction, b.kelly_fraction);
        assert_eq!(
            a.report.total_pnl_usd, b.report.total_pnl_usd,
            "total_pnl_usd not deterministic at kf={}: first={}, second={}",
            a.kelly_fraction.0, a.report.total_pnl_usd, b.report.total_pnl_usd
        );
        assert_eq!(
            a.report.total_copies, b.report.total_copies,
            "total_copies not deterministic at kf={}",
            a.kelly_fraction.0
        );
        assert_eq!(
            a.report.sharpe_ratio, b.report.sharpe_ratio,
            "sharpe_ratio not deterministic at kf={}",
            a.kelly_fraction.0
        );
        assert_eq!(
            a.report.max_drawdown_pct, b.report.max_drawdown_pct,
            "max_drawdown_pct not deterministic at kf={}",
            a.kelly_fraction.0
        );
        assert_eq!(
            a.report.bankroll_final, b.report.bankroll_final,
            "bankroll_final not deterministic at kf={}",
            a.kelly_fraction.0
        );
        assert_eq!(
            a.report.win_rate_pct, b.report.win_rate_pct,
            "win_rate_pct not deterministic at kf={}",
            a.kelly_fraction.0
        );
    }
}

// ── Scenario 8 ─────────────────────────────────────────────────────────────────

/// Output ordering: regardless of the input fraction order or rayon completion order,
/// the post-sort output `runs` vector is in ascending `kelly_fraction` order.
///
/// PASS: `runs[i].kelly_fraction < runs[i+1].kelly_fraction` for all adjacent pairs.
/// FAIL: any pair is out of order — `sort_unstable_by_key` is not being applied,
///       or `KellyFraction`'s `Ord` impl is broken.
#[tokio::test]
async fn parallel_sweep_output_sorted_by_fraction() {
    let winner = wallet(WINNER_HEX);
    let mut trades = generate_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);
    // Deliberately scrambled input order — output must be sorted regardless.
    let fractions = vec![
        kf(dec!(0.75)),
        kf(dec!(0.10)),
        kf(dec!(1.0)),
        kf(dec!(0.25)),
        kf(dec!(0.50)),
    ];
    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        kelly_sweep_fractions: Some(fractions.clone()),
        ..base_config(&dir)
    };
    let resolutions = ResolutionIndex::new();
    let snapshots = LeaderboardSnapshots::default();
    let ranker = relaxed_ranker();
    let schedules = ScheduleIndex::new();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &schedules,
        liq_index: &LiquidityIndex::new(),
        ranker_config: &ranker,
    };

    let mut runs: Vec<KellySweepRun> = fractions
        .par_iter()
        .map(|&kf| run_one_kelly_fraction(kf, &ctx))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    runs.sort_unstable_by_key(|r| r.kelly_fraction);

    let expected = [
        kf(dec!(0.10)),
        kf(dec!(0.25)),
        kf(dec!(0.50)),
        kf(dec!(0.75)),
        kf(dec!(1.0)),
    ];
    assert_eq!(runs.len(), expected.len());
    for (run, exp) in runs.iter().zip(expected.iter()) {
        assert_eq!(run.kelly_fraction, *exp);
    }
    // Strictly increasing.
    for i in 1..runs.len() {
        assert!(
            runs[i - 1].kelly_fraction < runs[i].kelly_fraction,
            "runs out of order: runs[{}].kf={} >= runs[{}].kf={}",
            i - 1,
            runs[i - 1].kelly_fraction.0,
            i,
            runs[i].kelly_fraction.0,
        );
    }
}

// ── Scenario 9 ─────────────────────────────────────────────────────────────────

/// Single-fraction edge case: a sweep with exactly one fraction must produce
/// exactly one run; rayon par_iter over a 1-element slice degenerates to a single
/// task on the calling thread.
///
/// PASS: a 1-fraction sweep produces 1 run with the expected fraction.
/// FAIL: any other run count, or fraction mismatch.
#[tokio::test]
async fn parallel_sweep_single_fraction() {
    let winner = wallet(WINNER_HEX);
    let mut trades = generate_winner_trades(winner);
    trades.sort_by_key(|t| t.timestamp.0);
    let fractions = vec![kf(dec!(0.50))];
    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        kelly_sweep_fractions: Some(fractions.clone()),
        ..base_config(&dir)
    };
    let resolutions = ResolutionIndex::new();
    let snapshots = LeaderboardSnapshots::default();
    let ranker = relaxed_ranker();
    let schedules = ScheduleIndex::new();

    let ctx = SweepContext {
        config: &config,
        all_trades: &trades,
        snapshots: &snapshots,
        resolutions: &resolutions,
        schedules: &schedules,
        liq_index: &LiquidityIndex::new(),
        ranker_config: &ranker,
    };

    let runs: Vec<KellySweepRun> = fractions
        .par_iter()
        .map(|&kf| run_one_kelly_fraction(kf, &ctx))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].kelly_fraction, kf(dec!(0.50)));
}

// ── Scenario 10 ────────────────────────────────────────────────────────────────

/// Compile-time assertion that `SweepContext<'a>` is `Send + Sync`. This is what
/// makes `par_iter().map(|kf| run_one_kelly_fraction(kf, &ctx))` legal — without it,
/// rayon would refuse to share the borrow across worker threads.
///
/// The function never runs (it's `#[allow(dead_code)]`). If the type ever loses
/// `Send` or `Sync` (e.g. by adding a `Cell`, `RefCell`, `Rc`, or non-`Sync`
/// embedded handle), this won't compile — a louder, earlier failure than waiting
/// for the par_iter call site to break.
#[allow(dead_code)]
fn sweep_context_is_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<SweepContext<'_>>();
    assert_sync::<SweepContext<'_>>();
}
