//! Scenario tests for the `skip_unknown_operator` gate (issue #141).
//!
//! `BacktestConfig::skip_unknown_operator = true` suppresses BUY signals from
//! watchlisted leaders whose wallet has no resolved operator identity in the
//! funder graph (`op_identity.is_none()`). The gate sits in the BUY arm after
//! the per-market cap (#140) and before the horizon-cooldown filter, so both
//! flat-USD and Kelly paths honor it. The SELL arm is unaffected.
//!
//! Tracker semantics (mirroring the `max_hours_to_expiry` idiom): records only
//! when the gate is active. Disabled → tracker stays empty.
//!
//! ## Fixture strategy
//!
//! `op_identity.is_none()` requires `wallet_to_operator.get(wallet)` to return
//! `None`. The clustering algorithm builds singleton operators for any wallet
//! that has trades or wallet-age data, so giving a leader trade history and
//! omitting its funder edge still produces an operator identity. The only
//! reliable way to force `op_identity.is_none()` from a backtest fixture is the
//! empty funder timeline: `FunderGraphTimeline::empty()` short-circuits
//! `build_operator_identities_at` to return `Vec::new()`, leaving
//! `wallet_to_operator` empty for every wallet. That matches the production
//! pre-condition for the gate firing (caches with no Etherscan funder data).
//!
//! Tests therefore use two fixtures:
//!
//! - **Empty timeline** — every watchlisted leader is unmapped. Models the
//!   "no funder-graph data" production case the gate was designed to mitigate.
//! - **Populated timeline** — every watchlisted leader is mapped via its
//!   funder edge. Models the healthy production case where the gate should be
//!   a no-op.
//!
//! ## Scenarios
//!
//! 1. `empty_timeline_suppresses_under_skip` — empty timeline + `skip=true` →
//!    leader's test-market BUY does not fill.
//! 2. `populated_timeline_passes_under_skip` — funder edge present + `skip=true`
//!    → leader's BUY fills normally.
//! 3. `disabled_preserves_baseline_empty_timeline` — empty timeline +
//!    `skip=false` → BUY fills regardless of unmapped status (gate off).
//! 4. `tracker_full_suppression_when_unmapped` — empty timeline + `skip=true`
//!    → `unknown_operator_suppression_pct == 100`.
//! 5. `tracker_empty_when_gate_disabled` — `skip=false` → pct = 0 and
//!    per-quarter map is empty (mirrors `max_hours_to_expiry` tracker idiom).
//! 6. `tracker_per_quarter_groups_correctly` — signals in distinct quarters →
//!    each quarter appears in the per-quarter map with its own pct.
//! 7. `sell_arm_unaffected_by_gate` — mapped leader BUY + SELL both fill under
//!    active gate; proves `match Side::Buy` scope is strict.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_arguments
)]

use std::collections::BTreeMap;
use std::io::BufRead as _;

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

// 2023-11-01 00:00 UTC — falls in Q4. Day 70 lands in 2024-Q1.
const BASE_UNIX: i64 = 1_698_796_800;
const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xcccccccccccccccccccccccccccccccccccccccc";

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

/// 20 BUY/SELL pairs on disjoint markets — clears the 15-closed minimum on the
/// relaxed ranker so the leader appears on the watchlist by ~day 17.
fn training_book(w: WalletAddress, start_market: u32, tag: &str) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..20 {
        let m = start_market + i;
        t.push(raw_trade(w, m, 0, i, 0, Side::Buy, dec!(0.35), tag));
        t.push(raw_trade(w, m, 0, i + 2, 1, Side::Sell, dec!(0.75), tag));
    }
    t
}

/// Build a `FunderGraphTimeline` with the supplied `(funded, funder)` pairs.
/// Pass an empty slice to model the "no funder-graph data" production case.
fn make_timeline(dir: &TempDir, pairs: &[(WalletAddress, WalletAddress)]) -> FunderGraphTimeline {
    if pairs.is_empty() {
        return FunderGraphTimeline::empty();
    }
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

fn base_config(dir: &TempDir, skip_unknown_operator: bool) -> BacktestConfig {
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
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
        kelly_p_min_snapshots: 0,
        kelly_p_extra_per_missing_snapshot: 0,
        liquidity_take_fraction: dec!(0),
        liquidity_min_required_usd: dec!(200),
        // Flat-USD: deterministic 1-contract fills; exercises the gate's
        // "before sizing branch" placement.
        flat_usd: Some(dec!(1)),
        no_buy_within_horizon_days: None,
        require_known_expiry: false,
        // Disable the per-market cap so it never masks the gate under test.
        max_positions_per_market: None,
        skip_unknown_operator,
        max_signal_price: None,
        max_trade_count: 0,
        strategy: WinnerFollowConfig::default(),
    }
}

#[derive(Debug, serde::Deserialize)]
struct TradeFillJson {
    market_id: String,
    side: String,
}

struct RunOutput {
    fills: Vec<TradeFillJson>,
    report: WinnerFollowReport,
}

/// Drive `run_simulation` with the supplied funder topology and trades.
fn run_with(
    skip_unknown_operator: bool,
    funder_pairs: &[(WalletAddress, WalletAddress)],
    mut trades: Vec<RawTrade>,
) -> RunOutput {
    trades.sort_by_key(|t| t.timestamp.0);
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("output")).unwrap();

    let timeline = make_timeline(&dir, funder_pairs);
    let snapshots = LeaderboardSnapshots::default();
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();
    let liq_index = LiquidityIndex::new();
    let ranker_config = relaxed_ranker();
    let ledger_config = LedgerConfig::default();
    let config = base_config(&dir, skip_unknown_operator);
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());

    let report = run_simulation(
        &config,
        &trades,
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
    let fills = if fills_path.exists() {
        let file = std::fs::File::open(&fills_path).unwrap();
        std::io::BufReader::new(file)
            .lines()
            .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
            .collect()
    } else {
        Vec::new()
    };

    RunOutput { fills, report }
}

fn buy_fills_on(fills: &[TradeFillJson], market_idx: u32) -> Vec<&TradeFillJson> {
    let m = mkt(market_idx).0.0;
    fills
        .iter()
        .filter(|f| f.market_id == m && f.side == "buy")
        .collect()
}

fn sell_fills_on(fills: &[TradeFillJson], market_idx: u32) -> Vec<&TradeFillJson> {
    let m = mkt(market_idx).0.0;
    fills
        .iter()
        .filter(|f| f.market_id == m && f.side == "sell")
        .collect()
}

/// Standard fixture: one leader trains for ranker eligibility, then BUYs the
/// test market on day 30.
fn one_leader_buys_test_market(test_market: u32) -> Vec<RawTrade> {
    let a = wallet(LEADER_HEX);
    let mut t = training_book(a, 1, "A");
    t.push(raw_trade(
        a,
        test_market,
        0,
        30,
        1,
        Side::Buy,
        dec!(0.40),
        "buy",
    ));
    t
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: empty funder timeline + `skip = true` → leader's test-market BUY does
///       not fill because `op_identity.is_none()` for every wallet.
/// FAIL: ≥1 fill on the test market → gate skipped or unmapped check inverted.
#[tokio::test]
async fn empty_timeline_suppresses_under_skip() {
    let trades = one_leader_buys_test_market(500);
    let out = run_with(true, &[], trades);
    let buys = buy_fills_on(&out.fills, 500);
    assert_eq!(
        buys.len(),
        0,
        "unmapped leader's test BUY should be suppressed; got {}",
        buys.len()
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: funder edge present + `skip = true` → leader is mapped to an operator,
///       gate's `op_identity.is_none()` is false, test-market BUY fills.
/// FAIL: 0 fills → gate fires on mapped wallets (false positive).
#[tokio::test]
async fn populated_timeline_passes_under_skip() {
    let a = wallet(LEADER_HEX);
    let funder = wallet(FUNDER_HEX);
    let trades = one_leader_buys_test_market(501);
    let out = run_with(true, &[(a, funder)], trades);
    let buys = buy_fills_on(&out.fills, 501);
    assert_eq!(
        buys.len(),
        1,
        "mapped leader's test BUY should fill; got {}",
        buys.len()
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: empty funder timeline + `skip = false` → BUY fills regardless of
///       unmapped status (gate disabled is a true no-op).
/// FAIL: 0 fills → gate fires when disabled (would prove the wrong branch is
///       used) or the disabled path interferes with sizing.
#[tokio::test]
async fn disabled_preserves_baseline_empty_timeline() {
    let trades = one_leader_buys_test_market(502);
    let out = run_with(false, &[], trades);
    let buys = buy_fills_on(&out.fills, 502);
    assert_eq!(
        buys.len(),
        1,
        "disabled gate should let unmapped BUY fill; got {}",
        buys.len()
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: empty timeline + `skip = true`, with N watchlisted BUY signals reaching
///       the gate, `unknown_operator_suppression_pct == 100` (all suppressed).
/// FAIL: pct < 100 → some signal recorded as not-suppressed despite empty
///       operator map (would indicate a stale `op_identity` reference or
///       early-return bypass).
#[tokio::test]
async fn tracker_full_suppression_when_unmapped() {
    let trades = one_leader_buys_test_market(503);
    let out = run_with(true, &[], trades);
    assert_eq!(
        out.report.unknown_operator_suppression_pct,
        dec!(100),
        "expected 100% suppression under empty timeline; got {}",
        out.report.unknown_operator_suppression_pct
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: with `skip = false`, `unknown_operator_suppression_pct == 0` and the
///       per-quarter map is empty — the tracker never `record`s when the gate
///       is inactive (mirrors `max_hours_to_expiry` tracker semantics).
/// FAIL: non-zero pct or non-empty map → `record` is being called on the
///       disabled path (would conflate "no signals seen" with "all signals
///       passed an active gate").
#[tokio::test]
async fn tracker_empty_when_gate_disabled() {
    let trades = one_leader_buys_test_market(504);
    let out = run_with(false, &[], trades);

    assert_eq!(
        out.report.unknown_operator_suppression_pct,
        Decimal::ZERO,
        "disabled gate must report 0% suppression"
    );
    assert!(
        out.report
            .unknown_operator_suppression_by_quarter
            .is_empty(),
        "disabled gate must produce an empty per-quarter map; got {:?}",
        out.report.unknown_operator_suppression_by_quarter
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: signals span 2023-Q4 (day 30 ≈ 2023-12-01) and 2024-Q1 (day 90 ≈
///       2024-01-30); per-quarter map has both keys with 100% pcts.
/// FAIL: missing key, wrong key format, or wrong per-quarter pct.
#[tokio::test]
async fn tracker_per_quarter_groups_correctly() {
    let a = wallet(LEADER_HEX);
    let mut trades = training_book(a, 1, "A");
    trades.push(raw_trade(a, 600, 0, 30, 1, Side::Buy, dec!(0.40), "Q4"));
    trades.push(raw_trade(a, 601, 0, 90, 1, Side::Buy, dec!(0.40), "Q1"));

    let out = run_with(true, &[], trades);
    let by_q: &BTreeMap<String, Decimal> = &out.report.unknown_operator_suppression_by_quarter;

    assert_eq!(
        by_q.get("2023-Q4").copied(),
        Some(dec!(100)),
        "2023-Q4 should report 100% suppression; map = {by_q:?}"
    );
    assert_eq!(
        by_q.get("2024-Q1").copied(),
        Some(dec!(100)),
        "2024-Q1 should report 100% suppression; map = {by_q:?}"
    );
    assert!(
        by_q.len() >= 2,
        "expected at least 2 quarter buckets (Q4 + Q1); got {} keys = {:?}",
        by_q.len(),
        by_q.keys().collect::<Vec<_>>()
    );
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────

/// PASS: mapped leader BUYs market 700 day 30 and SELLs day 32 under
///       `skip = true` — both fills land. Proves the gate's `match Side::Buy`
///       scope is strict.
/// FAIL: SELL absent → the gate branch leaked into the SELL arm; BUY absent →
///       mapped wallet incorrectly suppressed.
#[tokio::test]
async fn sell_arm_unaffected_by_gate() {
    let a = wallet(LEADER_HEX);
    let funder = wallet(FUNDER_HEX);
    let mut trades = training_book(a, 1, "A");
    trades.push(raw_trade(a, 700, 0, 30, 1, Side::Buy, dec!(0.40), "Abuy"));
    trades.push(raw_trade(a, 700, 0, 32, 1, Side::Sell, dec!(0.80), "Asell"));

    let out = run_with(true, &[(a, funder)], trades);

    assert_eq!(
        buy_fills_on(&out.fills, 700).len(),
        1,
        "mapped BUY should fill under active gate"
    );
    assert_eq!(
        sell_fills_on(&out.fills, 700).len(),
        1,
        "SELL close path should be unaffected by the gate"
    );
}
