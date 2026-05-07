//! Scenario tests for the expiry-filter survivorship bias fix (issue #102).
//!
//! Pre-fix: when `max_hours_to_expiry` was configured and a market had no resolution
//! entry, the signal was silently skipped (survivorship bias — only markets we
//! "know" were short-lived would be traded). Post-fix: `None` resolution means
//! "allow" (unknown expiry → don't suppress).
//!
//! Scenarios:
//! 1. `unknown_expiry_market_allowed_through` — market has no resolution entry and
//!    `max_hours_to_expiry` is set; signal must be copied (not suppressed).
//! 2. `known_far_expiry_is_suppressed` — market with a resolution time beyond the
//!    configured window IS suppressed; total_copies == 0 for that market.
//! 3. `suppression_pct_zero_without_filter` — `max_hours_to_expiry = None` yields
//!    `expiry_filter_suppression_pct == 0`.

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
    BacktestConfig {
        cache_path: dir.path().join("cache.db"),
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
        per_trade_cap_override: None,
        kelly_p_prior_alpha: 0,
        kelly_p_prior_beta: 0,
        kelly_p_k_per_market: 0,
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
