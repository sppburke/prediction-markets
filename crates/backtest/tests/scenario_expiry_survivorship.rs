//! Scenario tests for the expiry-filter survivorship bias fix (issue #102) and
//! the `require_known_expiry` strict-mode flag (issue #137, sub-PR 1).
//!
//! Pre-#102: when `max_hours_to_expiry` was configured and a market had no resolution
//! entry, the signal was silently skipped (survivorship bias — only markets we
//! "know" were short-lived would be traded). Post-#102: `None` resolution means
//! "allow" (unknown expiry → don't suppress).
//!
//! Pre-#137: a schedule row with `end_date_unix = None` returned `allow` regardless
//! of the resolution index — an inconsistency vs. the "row absent" path, which was
//! consulting the resolution. Post-#137: NULL `end_date_unix` falls through to the
//! resolution lookup; only when BOTH are absent does the `require_known_expiry` flag
//! decide (false → allow, true → suppress).
//!
//! Scenarios:
//! 1. `unknown_expiry_market_allowed_through` — market has no resolution entry and
//!    `max_hours_to_expiry` is set; signal must be copied (not suppressed) under
//!    default `require_known_expiry: false`.
//! 2. `known_far_expiry_is_suppressed` — market with a resolution time beyond the
//!    configured window IS suppressed; total_copies == 0 for that market.
//! 3. `suppression_pct_zero_without_filter` — `max_hours_to_expiry = None` yields
//!    `expiry_filter_suppression_pct == 0`.
//! 4. `expiry_filter_uses_schedule_over_resolution` — Some(end_date) wins over a
//!    contradicting resolution timestamp.
//! 5. `null_schedule_falls_through_to_resolution` — NULL `end_date_unix` is treated
//!    identically to "row absent" — both consult the resolution index. (Replaces
//!    the pre-#137 `expiry_filter_null_end_date_allows_through` test which asserted
//!    the buggy behaviour.)
//! 6. `expiry_filter_falls_back_to_resolution_when_no_schedule` — row absent +
//!    far resolution → suppressed.
//! 7. `require_known_expiry_suppresses_null_schedule_no_resolution` — strict mode:
//!    NULL end_date + no resolution → suppressed.
//! 8. `require_known_expiry_off_allows_null_schedule_no_resolution` — default mode:
//!    NULL end_date + no resolution → allowed (regression guard for the
//!    behavioural-no-op default).
//! 9. `require_known_expiry_suppresses_missing_market` — strict mode: row absent +
//!    no resolution → suppressed.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{
    LeaderboardSnapshots, LiquidityIndex, MarketResolution, MarketSchedule, ResolutionIndex,
    ScheduleIndex, WalletCache,
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

const ALICE_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_HEX: &str = "0xdddddddddddddddddddddddddddddddddddddddd";
/// 2024-01-01 00:00:00 UTC. Day 0.
const BASE_UNIX: i64 = 1_704_067_200;
const DAY: i64 = 86_400;

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
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

/// 65 buy/sell round-trips so `w` qualifies for the watchlist by day ~62.
fn winner_book(w: WalletAddress) -> Vec<RawTrade> {
    let mut t = Vec::new();
    for i in 0u32..65 {
        t.push(make_trade(w, i, i, Side::Buy, dec!(0.35), 0));
        t.push(make_trade(w, i, i + 2, Side::Sell, dec!(0.75), 1));
    }
    t
}

fn make_timeline(dir: &TempDir) -> FunderGraphTimeline {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    cache.insert_funder_edges(alice, &[(funder, 0)], 0).unwrap();
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

fn base_config(dir: &TempDir, max_hours_to_expiry: Option<u32>) -> BacktestConfig {
    base_config_with(dir, max_hours_to_expiry, false)
}

fn base_config_with(
    dir: &TempDir,
    max_hours_to_expiry: Option<u32>,
    require_known_expiry: bool,
) -> BacktestConfig {
    BacktestConfig {
        bootstrap_cache_path: dir.path().join("cache.db"),
        output_dir: dir.path().join("output"),
        bankroll_usd: Decimal::from(10_000u32),
        step_days: 1,
        dune_api_key: None,
        dune_namespace: None,
        max_hours_to_expiry,
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
        liquidity_take_fraction: rust_decimal::Decimal::new(5, 2),
        liquidity_min_required_usd: rust_decimal::Decimal::new(200, 0),
        kelly_p_min_snapshots: 0,
        kelly_p_extra_per_missing_snapshot: 0,
        flat_usd: None,
        no_buy_within_horizon_days: None,
        require_known_expiry,
        strategy: WinnerFollowConfig::default(),
    }
}

fn default_strategy() -> WinnerFollowStrategy {
    WinnerFollowStrategy::new(WinnerFollowConfig::default())
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: a market with no resolution entry is NOT suppressed when max_hours_to_expiry
///       is set. The post-fix behavior is `None => allow`.
/// FAIL: total_copies == 0 (old survivorship-bias behavior: `None => skip`).
#[tokio::test]
async fn unknown_expiry_market_allowed_through() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    // Extra signal BUY after qualification; market 9999 has no resolution entry.
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 72, Side::Sell, dec!(0.75), 1));

    // Empty resolution index — market 9999 has no entry.
    let resolutions = ResolutionIndex::new();

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    // Configure a tight 48-hour window. Market 9999 has unknown expiry → allowed.
    let report = run_simulation(
        &base_config(&dir, Some(48)),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "market with unknown expiry must be allowed through (post-fix); got 0 copies"
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: a market whose resolution is 200 days out is suppressed by a 48-hour window.
///       total_copies for that market is 0; suppression_pct > 0.
/// FAIL: the far-future market is copied despite the expiry filter.
#[tokio::test]
async fn known_far_expiry_is_suppressed() {
    let alice = wallet(ALICE_HEX);

    // Only the 65 qualifying trades, plus one BUY on a market that resolves 200 days out.
    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    let mut resolutions = ResolutionIndex::new();
    // resolved_at = day 270 → 200 days past day 70 → well beyond the 48-hour window.
    resolutions.insert(
        mkt(9999),
        MarketResolution {
            winning_outcome_id: 0,
            resolved_at_unix: BASE_UNIX + 270 * DAY,
        },
    );

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config(&dir, Some(48)),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.expiry_filter_suppression_pct > Decimal::ZERO,
        "far-expiry market must show non-zero suppression_pct; got {}",
        report.expiry_filter_suppression_pct
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: when `max_hours_to_expiry` is None (filter disabled), `expiry_filter_suppression_pct`
///       is exactly 0 and `expiry_suppression_by_quarter` is empty.
/// FAIL: suppression fields are non-zero without the filter configured.
#[tokio::test]
async fn suppression_pct_zero_without_filter() {
    let alice = wallet(ALICE_HEX);
    let trades = winner_book(alice);

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config(&dir, None), // no expiry filter
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &ResolutionIndex::new(),
        &ScheduleIndex::new(),
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert_eq!(
        report.expiry_filter_suppression_pct,
        Decimal::ZERO,
        "suppression_pct must be 0 when filter is disabled; got {}",
        report.expiry_filter_suppression_pct
    );
    assert!(
        report.expiry_suppression_by_quarter.is_empty(),
        "by_quarter map must be empty when filter is disabled"
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: when a market is in ScheduleIndex with a far-future endDate, it is
///       suppressed even if ResolutionIndex has a near resolved_at_unix.
/// FAIL: the schedule is ignored and the (near) resolved_at_unix allows the trade.
#[tokio::test]
async fn expiry_filter_uses_schedule_over_resolution() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    // Signal on market 9999 at day 70; endDate is 200 days out.
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    // ResolutionIndex says it resolved quickly (day 71) — would allow if used.
    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(
        mkt(9999),
        MarketResolution {
            winning_outcome_id: 0,
            resolved_at_unix: BASE_UNIX + 71 * DAY,
        },
    );

    // ScheduleIndex says scheduled endDate is 200 days out — should suppress.
    let mut schedules = ScheduleIndex::new();
    schedules.insert(
        mkt(9999),
        MarketSchedule {
            end_date_unix: Some(BASE_UNIX + 270 * DAY),
        },
    );

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config(&dir, Some(48)),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &schedules,
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.expiry_filter_suppression_pct > Decimal::ZERO,
        "schedule endDate must take priority over resolution timestamp; \
         suppression_pct must be > 0, got {}",
        report.expiry_filter_suppression_pct
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: when a market is in ScheduleIndex with a NULL `end_date_unix` AND the
///       resolution index has a far-future timestamp, the trade is suppressed
///       via the fallback chain. This is the issue #137 sub-PR 1 fix — NULL
///       end_date now behaves identically to "row absent" (both consult
///       resolution), eliminating the pre-fix asymmetry where a NULL row
///       allowed every trade through regardless of resolution data.
/// FAIL: NULL end_date short-circuits to allow (the pre-fix bug).
#[tokio::test]
async fn null_schedule_falls_through_to_resolution() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    // Signal on market 9999; schedule has NULL endDate.
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    // ResolutionIndex says it resolves 200 days out — fallback must suppress.
    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(
        mkt(9999),
        MarketResolution {
            winning_outcome_id: 0,
            resolved_at_unix: BASE_UNIX + 270 * DAY,
        },
    );

    // ScheduleIndex has the market with NULL endDate — fallback to resolution.
    let mut schedules = ScheduleIndex::new();
    schedules.insert(
        mkt(9999),
        MarketSchedule {
            end_date_unix: None,
        },
    );

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config(&dir, Some(48)),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &schedules,
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.expiry_filter_suppression_pct > Decimal::ZERO,
        "NULL end_date_unix must fall through to resolution; far resolution must \
         suppress. expiry_filter_suppression_pct must be > 0, got {}",
        report.expiry_filter_suppression_pct
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: when a market is absent from ScheduleIndex but present in ResolutionIndex
///       with a far resolved_at_unix, it IS suppressed (fallback path is active).
/// FAIL: absence from ScheduleIndex causes no suppression (fallback not working).
#[tokio::test]
async fn expiry_filter_falls_back_to_resolution_when_no_schedule() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    // Signal on market 9999; not in ScheduleIndex.
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    // ResolutionIndex says it resolves 200 days out — should suppress via fallback.
    let mut resolutions = ResolutionIndex::new();
    resolutions.insert(
        mkt(9999),
        MarketResolution {
            winning_outcome_id: 0,
            resolved_at_unix: BASE_UNIX + 270 * DAY,
        },
    );

    // Empty ScheduleIndex — fallback to ResolutionIndex must apply.
    let schedules = ScheduleIndex::new();

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config(&dir, Some(48)),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &schedules,
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.expiry_filter_suppression_pct > Decimal::ZERO,
        "absent-from-schedule market must fall back to resolved_at_unix suppression; \
         suppression_pct must be > 0, got {}",
        report.expiry_filter_suppression_pct
    );
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────

/// PASS: strict mode (`require_known_expiry: true`) + NULL `end_date_unix` + no
///       resolution → trade is suppressed. Tests the fail-closed path for the
///       genuinely-unknown case.
/// FAIL: trade is allowed through despite strict mode (flag not consulted).
#[tokio::test]
async fn require_known_expiry_suppresses_null_schedule_no_resolution() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    // No resolution data — both lookups will miss.
    let resolutions = ResolutionIndex::new();

    // Schedule row present with NULL endDate.
    let mut schedules = ScheduleIndex::new();
    schedules.insert(
        mkt(9999),
        MarketSchedule {
            end_date_unix: None,
        },
    );

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config_with(&dir, Some(48), true),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &schedules,
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.expiry_filter_suppression_pct > Decimal::ZERO,
        "require_known_expiry=true + unknown expiry must suppress; \
         expiry_filter_suppression_pct must be > 0, got {}",
        report.expiry_filter_suppression_pct
    );
}

// ── Scenario 8 ────────────────────────────────────────────────────────────────

/// PASS: default mode (`require_known_expiry: false`) + NULL `end_date_unix` + no
///       resolution → trade is allowed through. Regression guard ensuring Sub-PR
///       1 ships as a behavioural no-op for tests/data that lack schedule data.
/// FAIL: trade is suppressed despite the default-false flag (would break the
///       behavioural-no-op contract for Sub-PR 1).
#[tokio::test]
async fn require_known_expiry_off_allows_null_schedule_no_resolution() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    let resolutions = ResolutionIndex::new();

    let mut schedules = ScheduleIndex::new();
    schedules.insert(
        mkt(9999),
        MarketSchedule {
            end_date_unix: None,
        },
    );

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config_with(&dir, Some(48), false),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &schedules,
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "require_known_expiry=false + unknown expiry must allow; got 0 copies"
    );
}

// ── Scenario 9 ────────────────────────────────────────────────────────────────

/// PASS: strict mode (`require_known_expiry: true`) + market absent from both
///       schedule and resolution indices → trade is suppressed. Symmetric guard
///       for Scenario 7: confirms the strict-mode fail-closed semantics apply
///       to the "no row at all" path identically to the "NULL row" path.
/// FAIL: trade is allowed through despite strict mode (asymmetry would mean
///       the fallback chain isn't unified between NULL-row and missing-row).
#[tokio::test]
async fn require_known_expiry_suppresses_missing_market() {
    let alice = wallet(ALICE_HEX);

    let mut trades = winner_book(alice);
    trades.push(make_trade(alice, 9999, 70, Side::Buy, dec!(0.35), 0));
    trades.push(make_trade(alice, 9999, 75, Side::Sell, dec!(0.75), 1));

    // Neither schedule nor resolution has any entry for market 9999.
    let resolutions = ResolutionIndex::new();
    let schedules = ScheduleIndex::new();

    let dir = TempDir::new().unwrap();
    let timeline = make_timeline(&dir);

    let report = run_simulation(
        &base_config_with(&dir, Some(48), true),
        trades,
        &timeline,
        &LeaderboardSnapshots::default(),
        &resolutions,
        &schedules,
        &LiquidityIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.expiry_filter_suppression_pct > Decimal::ZERO,
        "require_known_expiry=true + missing market must suppress; \
         expiry_filter_suppression_pct must be > 0, got {}",
        report.expiry_filter_suppression_pct
    );
}
