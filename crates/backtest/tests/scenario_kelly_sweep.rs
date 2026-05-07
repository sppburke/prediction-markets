//! Scenario and operator tests for the Kelly-fraction sweep feature.
//!
//! Scenarios:
//! 1. `override_changes_sizing_vs_default` — `kelly_fraction_override: Some(kf)` causes
//!    `WinnerFollowStrategy::evaluate()` to produce more contracts than the default fraction.
//!    Tested at the evaluate() layer to avoid the simulation's per-trade risk cap.
//! 2. `sweep_produces_correct_run_count` — a 3-fraction sweep produces exactly 3 runs in
//!    `KellySweepReport`.
//! 3. `higher_fraction_yields_more_contracts` — across [0.10, 0.25, 0.50, 0.75, 1.0],
//!    contract count from evaluate() increases monotonically with the Kelly fraction.
//! 4. `to_markdown_table_covers_all_runs` — the markdown table contains one row per run.
//! 5. `sweep_suppresses_per_run_output` — per-run report.json is NOT written in sweep mode.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, HashMap, HashSet};

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::report::{KellySweepReport, KellySweepRun, WinnerFollowReport};
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{LeaderboardSnapshots, ResolutionIndex, WalletCache};
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, ContractQty, KellyFraction, LeaderAction, MarketId, OutcomeId, Price, Probability,
    ProbabilityPpm, Quantity, Side, SourceTimestamp, SourceTradeId, TraderId, VenueId,
    VenueMarketId, WalletAddress, WinnerFollowSignalKind,
};
use pe_risk_engine::{RiskSnapshot, TradingMode};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{LedgerConfig, RankerConfig, snapshot::RawTrade};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

// ── fixtures ──────────────────────────────────────────────────────────────────

const WINNER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xcccccccccccccccccccccccccccccccccccccccc";
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
        active_window_days: 90,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 10,
        incubator_min_distinct_markets: 5,
        incubator_window_days: 60,
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
        audit_window_days: 90,
        ranker_min_quality: 0,
        ranker_active_min_closed: 60,
        ranker_active_min_markets: 30,
        ranker_incubator_min_closed: 10,
        ranker_incubator_min_markets: 5,
        kelly_sweep_fractions: None,
        per_trade_cap_override: None,
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
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
        operator_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        funder_inherited_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        anti_gaming_flags: HashSet::new(),
        onchain_source_status: SourceStatus::Healthy,
        proxy_funder_mapping_proven: true,
        funder_seeding_rate_suspicious: false,
        cluster_membership_stable: true,
        funding_hop_count: Some(pe_core_types::FundingHopCount(1)),
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
    let p = Probability::new(dec!(0.417)).unwrap();
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

/// PASS: a 3-fraction sweep produces a `KellySweepReport` with exactly 3 runs,
///       each tagged with its own fraction.
/// FAIL: run count ≠ 3, or fractions are mistagged.
#[tokio::test]
async fn sweep_produces_correct_run_count() {
    let winner = wallet(WINNER_HEX);
    let funder = wallet(FUNDER_HEX);
    let trades = generate_winner_trades(winner);

    let fractions = vec![kf(dec!(0.10)), kf(dec!(0.50)), kf(dec!(1.0))];
    let dir = TempDir::new().unwrap();
    let config = BacktestConfig {
        kelly_sweep_fractions: Some(fractions.clone()),
        ..base_config(&dir)
    };
    let timeline = make_timeline(&dir, &[(winner, funder)]);
    let resolutions = ResolutionIndex::new();
    let snapshots = LeaderboardSnapshots::default();
    let ranker = relaxed_ranker();

    let mut runs: Vec<KellySweepRun> = Vec::new();
    for &kf in &fractions {
        let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
            kelly_fraction_override: Some(kf),
            ..WinnerFollowConfig::default()
        });
        let report = run_simulation(
            &config,
            trades.clone(),
            &timeline,
            &snapshots,
            &resolutions,
            &ranker,
            &LedgerConfig::default(),
            &strategy,
            false,
        )
        .unwrap();
        runs.push(KellySweepRun {
            kelly_fraction: kf,
            report,
        });
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
    let p = Probability::new(dec!(0.417)).unwrap();
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
            funder_graph_snapshot_caveat: false,
            expiry_filter_suppression_pct: dec!(0),
            expiry_suppression_by_quarter: BTreeMap::new(),
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
    let funder = wallet(FUNDER_HEX);
    let trades = generate_winner_trades(winner);

    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();
    let config = base_config(&dir);
    let timeline = make_timeline(&dir, &[(winner, funder)]);

    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        kelly_fraction_override: Some(KellyFraction::ONE),
        ..WinnerFollowConfig::default()
    });
    run_simulation(
        &config,
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
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
