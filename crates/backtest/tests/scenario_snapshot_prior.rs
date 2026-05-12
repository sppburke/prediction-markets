//! Scenario tests for the snapshot-aware Beta prior (issue #129).
//!
//! Exercises the end-to-end activation path: build a fixture with leaderboard
//! snapshots, run `run_simulation`, and assert the three new report counters
//! (`total_signals_evaluated`, `snapshot_prior_signals`, `snapshot_prior_extra_sum`)
//! reflect the expected activation pattern.
//!
//! Scenarios:
//!
//! 1. `prior_disabled_when_min_snapshots_zero` — `kelly_p_min_snapshots = 0`
//!    produces `extra = 0` for every wallet; `snapshot_prior_signals` is zero.
//!    Backwards-compat regression guard.
//! 2. `prior_fires_for_newly_entering_leader` — a leader present in fewer than
//!    `min_snapshots` historical snapshots triggers `extra > 0`;
//!    `snapshot_prior_signals` and `snapshot_prior_extra_sum` both > 0.
//! 3. `prior_silent_for_long_history_leader` — a leader present in ≥
//!    `min_snapshots` snapshots is past the threshold; no strengthening even
//!    though `total_signals_evaluated > 0`.
//! 4. `total_signals_evaluated_is_denominator` — the activation-rate denominator
//!    increments on every call to the win-rate path; partition closure
//!    `snapshot_prior_signals ≤ total_signals_evaluated` always holds.
//! 5. `empty_snapshots_skip_prior_path` — with `LeaderboardSnapshots::default()`
//!    (empty), `snapshot_counts` is built as an empty map and every leader
//!    falls into the `unwrap_or(0)` defensive fallback. Verifies the simulation
//!    runs to completion without panicking and produces consistent counters.
//!
//! Pure-function unit tests for `leader_win_rate_p_shrunk` (math correctness
//! across α/β/k/extra combinations) live in `simulation.rs::tests` —
//! `snapshot_aware_prior_*`.

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
use tempfile::TempDir;
use time::OffsetDateTime;

// Base timestamp: 2024-01-01 00:00:00 UTC. The leaderboard snapshots are
// constructed with timestamps anchored to this base.
const BASE_UNIX: i64 = 1_704_067_200;
const DAY: i64 = 86_400;
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
                BASE_UNIX + i64::from(day_offset) * DAY + i64::from(market_idx),
            )
            .unwrap(),
        ),
        source_trade_id: SourceTradeId(format!("0xhash_{market_idx}_{tx_suffix}")),
    }
}

/// 65 markets × (BUY day i, SELL day i+2). Leader qualifies for the watchlist
/// by ~day 16 (15 closed trades) and continues round-tripping through day 66.
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

fn base_config(dir: &TempDir, min_snapshots: u32, extra_per_missing: u32) -> BacktestConfig {
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
        // Use the default symmetric prior so the snapshot-aware `extra` is the
        // only knob the scenarios vary.
        kelly_p_prior_alpha: 10,
        kelly_p_prior_beta: 10,
        kelly_p_k_per_market: 6,
        kelly_p_min_snapshots: min_snapshots,
        kelly_p_extra_per_missing_snapshot: extra_per_missing,
        liquidity_take_fraction: Decimal::new(5, 2),
        liquidity_min_required_usd: Decimal::new(200, 0),
        flat_usd: None,
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        max_positions_per_market: None,
        skip_unknown_operator: false,
        strategy: WinnerFollowConfig::default(),
    }
}

/// Run the simulation against a fixture and return the report. `snapshots` is
/// the constructed `LeaderboardSnapshots` (the test specifies how many
/// snapshots the leader appears in, which drives `extra` at the call site).
fn run_scenario(
    snapshots: LeaderboardSnapshots,
    min_snapshots: u32,
    extra_per_missing: u32,
) -> WinnerFollowReport {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let leader = wallet(LEADER_HEX);
    let funder = wallet(FUNDER_HEX);
    let trades = winner_book(leader);

    let timeline = make_timeline(&dir, &[(leader, funder)]);
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();
    let ledger_config = LedgerConfig::default();

    let config = base_config(&dir, min_snapshots, extra_per_missing);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    run_simulation(
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
        false,
    )
    .unwrap()
}

/// Build a `LeaderboardSnapshots` where `leader` appears in `n` snapshots,
/// each placed at unix `(BASE_UNIX + offset_days × DAY)`. The first snapshot is
/// pre-BASE_UNIX (day −1) so qualifying-day trades are inside the snapshot
/// window.
fn snapshots_with_leader_appearances(n: u32) -> LeaderboardSnapshots {
    let leader = wallet(LEADER_HEX);
    let mut pairs: Vec<(i64, Vec<WalletAddress>)> = Vec::new();
    // Anchor first snapshot at unix 1 to be safely before BASE_UNIX (= day 0).
    for i in 0..n {
        let at = 1 + i64::from(i);
        pairs.push((at, vec![leader]));
    }
    LeaderboardSnapshots::from_pairs(pairs)
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: `kelly_p_min_snapshots = 0` → `extra = 0` for every wallet —
///       `snapshot_prior_signals == 0` despite `total_signals_evaluated > 0`.
/// FAIL: any strengthening events recorded.
#[tokio::test]
async fn prior_disabled_when_min_snapshots_zero() {
    let snapshots = snapshots_with_leader_appearances(1);
    let report = run_scenario(snapshots, 0, 5);
    assert!(
        report.total_signals_evaluated > 0,
        "expected ≥1 signal evaluated; got 0"
    );
    assert_eq!(
        report.snapshot_prior_signals, 0,
        "prior must be disabled when min_snapshots = 0; got {} firings",
        report.snapshot_prior_signals
    );
    assert_eq!(report.snapshot_prior_extra_sum, 0);
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: leader present in only 1 snapshot, `min_snapshots = 4`, `extra_per_missing = 5`
///       → every signal gets `extra = (4-1) × 5 = 15`. `snapshot_prior_signals` matches
///       `total_signals_evaluated` (every evaluation strengthened).
/// FAIL: no strengthening recorded.
#[tokio::test]
async fn prior_fires_for_newly_entering_leader() {
    let snapshots = snapshots_with_leader_appearances(1);
    let report = run_scenario(snapshots, 4, 5);
    assert!(
        report.total_signals_evaluated > 0,
        "expected ≥1 signal; got 0"
    );
    assert!(
        report.snapshot_prior_signals > 0,
        "expected ≥1 strengthening; got 0 for newly-entering leader"
    );
    // Every signal had n_snaps = 1 → extra = 15, so the activation rate is 100%.
    assert_eq!(
        report.snapshot_prior_signals, report.total_signals_evaluated,
        "all signals should be strengthened when leader has only 1 snapshot vs threshold 4"
    );
    let expected_extra_per_signal: u64 = 15;
    assert_eq!(
        report.snapshot_prior_extra_sum,
        expected_extra_per_signal * report.snapshot_prior_signals,
        "extra sum must equal 15 × signal count"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: leader present in ≥ `min_snapshots` snapshots → no strengthening.
///       `total_signals_evaluated > 0` but `snapshot_prior_signals == 0`.
/// FAIL: strengthening recorded despite full visibility.
#[tokio::test]
async fn prior_silent_for_long_history_leader() {
    // 4 snapshots, threshold 4 → saturating_sub yields 0 → extra = 0 always.
    let snapshots = snapshots_with_leader_appearances(4);
    let report = run_scenario(snapshots, 4, 5);
    assert!(
        report.total_signals_evaluated > 0,
        "expected ≥1 signal; got 0"
    );
    assert_eq!(
        report.snapshot_prior_signals, 0,
        "fully-visible leader should not be strengthened; got {} firings",
        report.snapshot_prior_signals
    );
    assert_eq!(report.snapshot_prior_extra_sum, 0);
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: partition closure holds — `snapshot_prior_signals ≤ total_signals_evaluated`
///       across all configurations. The activation rate ratio is well-defined.
/// FAIL: numerator exceeds denominator (would mean a strengthening was counted
///       without a corresponding evaluation).
#[tokio::test]
async fn total_signals_evaluated_is_denominator() {
    let snapshots = snapshots_with_leader_appearances(2);
    let report = run_scenario(snapshots, 4, 5);
    assert!(
        report.snapshot_prior_signals <= report.total_signals_evaluated,
        "partition violation: {} strengthenings > {} evaluations",
        report.snapshot_prior_signals,
        report.total_signals_evaluated,
    );
    // Sanity-check the formula at the call-site level: n_snaps=2 → extra = (4-2) × 5 = 10.
    let expected_extra_per_signal: u64 = 10;
    if report.snapshot_prior_signals > 0 {
        assert_eq!(
            report.snapshot_prior_extra_sum,
            expected_extra_per_signal * report.snapshot_prior_signals,
            "extra sum should equal 10 × signal count when n_snaps=2 vs threshold=4",
        );
    }
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: empty snapshots → prior is **disabled** (no visibility data → no
///       strengthening). Counters consistent. This is the regression test for
///       the call-site fix: without the `snapshots_have_data` gate,
///       `unwrap_or(0)` would feed `n_snaps = 0` to `saturating_sub`,
///       producing the *maximum* `extra` for every signal — the opposite of
///       the intended "no data → no penalty" semantics.
/// FAIL: panic on empty-snapshots path, OR `snapshot_prior_signals > 0`
///       (strengthening happened despite no visibility data).
#[tokio::test]
async fn empty_snapshots_disable_prior_no_max_penalty_silent_strengthening() {
    let snapshots = LeaderboardSnapshots::default();
    let report = run_scenario(snapshots, 4, 5);
    assert!(
        report.total_signals_evaluated > 0,
        "expected ≥1 signal; got 0"
    );
    assert_eq!(
        report.snapshot_prior_signals, 0,
        "empty snapshots → prior must be disabled; got {} strengthenings (the call-site gate is broken)",
        report.snapshot_prior_signals
    );
    assert_eq!(report.snapshot_prior_extra_sum, 0);
}
