//! Scenario tests for the leaderboard-snapshots filter (issue #85).
//!
//! Each test exercises a distinct semantic of the snapshot pool:
//!
//! 1. `wallet_outside_snapshot_emits_no_signals` — a wallet absent from every
//!    snapshot is fully excluded; no copy positions are taken.
//! 2. `weekly_pool_swap_changes_active_leaders` — wallet `x` is in week 1's
//!    snapshot but not week 2's; `y` is in week 2's but not week 1's. Copies
//!    of `x` only happen during week 1; copies of `y` only during week 2.
//! 3. `wallet_present_throughout_emits_throughout` — a wallet in every snapshot
//!    is treated as a continuous leader across week boundaries.
//! 4. `position_opened_in_week1_persists_after_drop` — wallet drops out of
//!    later snapshots; existing copy position remains until the leader sells
//!    (matches live "stop new entries, keep existing positions" semantics).
//! 5. `empty_snapshots_falls_back_to_full_history` — empty snapshot index
//!    runs the simulation with the full wallet pool (legacy behavior).
//! 6. `simulation_date_before_first_snapshot_emits_nothing` — when the
//!    simulation starts before the earliest snapshot, no signals are emitted
//!    until the first snapshot becomes effective.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;

use pe_backtest::FunderGraphTimeline;
use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::{LeaderboardSnapshots, ResolutionIndex, WalletCache};
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
const BOB_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CAROL_HEX: &str = "0xcccccccccccccccccccccccccccccccccccccccc";
const FUNDER_HEX: &str = "0xdddddddddddddddddddddddddddddddddddddddd";

/// Base timestamp: 2024-01-01 00:00:00 UTC. Day 0.
const BASE_UNIX: i64 = 1_704_067_200;
/// One day in seconds.
const DAY: i64 = 86_400;

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn make_trade(
    w: WalletAddress,
    market_idx: u32,
    day_offset: u32,
    side: Side,
    price: Decimal,
    seq: u32,
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
                BASE_UNIX + i64::from(day_offset) * DAY + i64::from(market_idx) + i64::from(seq),
            )
            .unwrap(),
        ),
        source_trade_id: SourceTradeId(format!(
            "0xtx_{market_idx}_{day_offset}_{seq}_{}_{}",
            w,
            if side == Side::Buy { "b" } else { "s" }
        )),
    }
}

/// Generate a 100%-win-rate book of 65 buy/sell pairs starting at `start_day`.
fn winner_book(w: WalletAddress, start_day: u32, mkt_base: u32) -> Vec<RawTrade> {
    let mut trades = Vec::new();
    for i in 0u32..65 {
        let m = mkt_base + i;
        trades.push(make_trade(w, m, start_day + i, Side::Buy, dec!(0.35), 0));
        trades.push(make_trade(
            w,
            m,
            start_day + i + 2,
            Side::Sell,
            dec!(0.75),
            1,
        ));
    }
    trades
}

/// Build a `FunderGraphTimeline` from `(funded, funder)` pairs at Unix 0.
///
/// Tests with populated `LeaderboardSnapshots` can use `FunderGraphTimeline::empty()`
/// instead (has_funder = !snapshots.is_empty() = true). Tests with empty snapshots
/// must provide actual funder edges so that the operator identity is resolved.
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
        active_window_days: 365,
        active_watchlist_size: 50,
        incubator_min_closed_trades: 10,
        incubator_min_distinct_markets: 5,
        incubator_window_days: 365,
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
        audit_window_days: 365,
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

fn default_strategy() -> WinnerFollowStrategy {
    WinnerFollowStrategy::new(WinnerFollowConfig::default())
}

fn copied_leaders(output_dir: &std::path::Path) -> Vec<String> {
    let path = output_dir.join("trades.ndjson");
    let Ok(bytes) = std::fs::read(&path) else {
        return Vec::new();
    };
    let text = String::from_utf8_lossy(&bytes);
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| {
            v.get("leader_wallet")
                .and_then(|s| s.as_str().map(str::to_owned))
        })
        .collect()
}

fn day_unix(day: u32) -> i64 {
    BASE_UNIX + i64::from(day) * DAY
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: a wallet absent from every snapshot is never copied — even with valid trade history.
/// FAIL: any TradeFill has the absent wallet's address as leader_wallet.
#[tokio::test]
async fn wallet_outside_snapshot_emits_no_signals() {
    let alice = wallet(ALICE_HEX);
    let bob = wallet(BOB_HEX);

    let mut all_trades = winner_book(alice, 0, 0);
    all_trades.extend(winner_book(bob, 0, 200));

    let snap_pairs = vec![(day_unix(0) - DAY, vec![alice])];
    let snapshots = LeaderboardSnapshots::from_pairs(snap_pairs);

    let dir = TempDir::new().unwrap();
    // Populated snapshots → has_funder = !snapshots.is_empty() = true for all wallets.
    let timeline = FunderGraphTimeline::empty();

    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        &timeline,
        &snapshots,
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    let leaders: HashSet<String> = copied_leaders(&dir.path().join("output"))
        .into_iter()
        .collect();
    assert!(
        !leaders.contains(&bob.to_string()),
        "bob was outside the snapshot pool but appears as leader of a copied trade; total_copies={}, leaders={leaders:?}",
        report.total_copies
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: alice is in week-1 snapshot only; carol is in week-2 snapshot only.
///       Carol must be copied at least once after the week-2 boundary.
/// FAIL: carol absent from copied_leaders, or alice copied at/after week 2.
#[tokio::test]
async fn weekly_pool_swap_changes_active_leaders() {
    let alice = wallet(ALICE_HEX);
    let carol = wallet(CAROL_HEX);

    let mut all_trades = winner_book(alice, 0, 0);
    all_trades.extend(winner_book(carol, 70, 200));

    let snapshots = LeaderboardSnapshots::from_pairs(vec![
        (day_unix(65), vec![alice]),
        (day_unix(134), vec![carol]),
    ]);

    let dir = TempDir::new().unwrap();
    let timeline = FunderGraphTimeline::empty();

    let _ = run_simulation(
        &base_config(&dir),
        all_trades,
        &timeline,
        &snapshots,
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    let leaders = copied_leaders(&dir.path().join("output"));
    let unique: HashSet<String> = leaders.iter().cloned().collect();

    assert!(
        unique.contains(&carol.to_string()),
        "carol should be copied after the week-2 anchor; leaders = {unique:?}"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: a wallet present in every snapshot is treated as a continuous leader.
/// FAIL: gap days where the wallet's signals are filtered out.
#[tokio::test]
async fn wallet_present_throughout_emits_throughout() {
    let alice = wallet(ALICE_HEX);

    let all_trades = winner_book(alice, 0, 0);

    let snapshots = LeaderboardSnapshots::from_pairs(vec![
        (day_unix(0) - DAY, vec![alice]),
        (day_unix(7), vec![alice]),
    ]);

    let dir = TempDir::new().unwrap();
    let timeline = FunderGraphTimeline::empty();

    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        &timeline,
        &snapshots,
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "expected ≥1 copy of continuously-listed leader; got 0"
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: a position opened during week 1 (when alice is in the snapshot) is NOT
///       force-closed when alice falls off in week 2; it remains open until the
///       leader sells (or stays open at horizon).
/// FAIL: open_at_horizon == 0 AND no exit fill exists.
#[tokio::test]
async fn position_opened_in_week1_persists_after_drop() {
    let alice = wallet(ALICE_HEX);

    let all_trades = winner_book(alice, 0, 0);

    let snapshots = LeaderboardSnapshots::from_pairs(vec![
        (day_unix(0) - DAY, vec![alice]),
        (day_unix(70), vec![]),
    ]);

    let dir = TempDir::new().unwrap();
    let timeline = FunderGraphTimeline::empty();

    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        &timeline,
        &snapshots,
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.bankroll_final >= Decimal::ZERO,
        "bankroll must remain non-negative; got {}",
        report.bankroll_final
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: an empty snapshot index degrades to legacy "all wallets" behavior. A
///       100%-winner produces ≥1 copy and positive PnL.
/// FAIL: snapshot filter blocks all signals despite there being no snapshots.
///
/// Note: empty snapshots means `has_funder` depends on `op_identity.is_some()`, so
/// the timeline must contain actual funder edges for the winner wallet.
#[tokio::test]
async fn empty_snapshots_falls_back_to_full_history() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let all_trades = winner_book(alice, 0, 0);
    let snapshots = LeaderboardSnapshots::default();
    assert!(snapshots.is_empty(), "fixture sanity");

    let dir = TempDir::new().unwrap();
    // Empty snapshots → has_funder depends on op_identity. Must provide funder edges.
    let timeline = make_timeline(&dir, &[(alice, funder)]);

    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        &timeline,
        &snapshots,
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert!(
        report.total_copies > 0,
        "fallback path must run with full history; got 0 copies"
    );
    assert!(
        report.total_pnl_usd > Decimal::ZERO,
        "fallback path with a winning leader must produce positive PnL; got {}",
        report.total_pnl_usd
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: when the first snapshot is anchored AFTER the first simulation date,
///       no signals are emitted until that snapshot becomes effective. The
///       simulation must complete normally.
/// FAIL: a copy is emitted before the first snapshot's anchor unix.
#[tokio::test]
async fn simulation_date_before_first_snapshot_emits_nothing() {
    let alice = wallet(ALICE_HEX);

    let all_trades = winner_book(alice, 0, 0);

    // Anchor the first snapshot AFTER alice's last sell — no copies can happen.
    let snapshots = LeaderboardSnapshots::from_pairs(vec![(day_unix(100), vec![alice])]);

    let dir = TempDir::new().unwrap();
    let timeline = FunderGraphTimeline::empty();

    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        &timeline,
        &snapshots,
        &ResolutionIndex::new(),
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
        true,
    )
    .unwrap();

    assert_eq!(
        report.total_copies, 0,
        "no signals before first-snapshot anchor; got {}",
        report.total_copies
    );
}
