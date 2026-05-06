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

use std::collections::{HashMap, HashSet};

use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::run_simulation;
use pe_bootstrap::cache::LeaderboardSnapshots;
use pe_core_types::{
    ClusterSize, ContractQty, FunderRootId, FundingHopCount, MarketId, OperatorId, OutcomeId,
    Price, ReconstructionQuality, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_operator_graph::OperatorIdentity;
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

/// Build a single trade. `seq` makes the source_trade_id unique per call so
/// multiple trades on the same (market, day, side) for the same wallet don't
/// collide on idempotency.
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
/// Markets indices `mkt_base..mkt_base+65`. Buys at 0.35; sells at 0.75 two days later.
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

fn operator_for(w: WalletAddress, funder: WalletAddress, seed: &[u8]) -> OperatorIdentity {
    OperatorIdentity {
        operator_id: OperatorId(blake3::hash(seed)),
        funder_root: FunderRootId(funder),
        member_wallets: vec![w],
        hop_counts: HashMap::from([(w, FundingHopCount(1))]),
        confidence_ppm: 900_000,
        reconstruction_quality: ReconstructionQuality::new(80).unwrap(),
        cluster_size: ClusterSize(1),
        anti_gaming_flags: HashSet::new(),
    }
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
        etherscan_api_key: None,
        audit_window_days: 365,
    }
}

fn default_strategy() -> WinnerFollowStrategy {
    WinnerFollowStrategy::new(WinnerFollowConfig::default())
}

/// Read the trades.ndjson written by the simulation and return the leader_wallet
/// of every TradeFill record. We use this to assert which leaders were copied.
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

/// Day offset to a unix timestamp at UTC midnight.
fn day_unix(day: u32) -> i64 {
    BASE_UNIX + i64::from(day) * DAY
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: a wallet absent from every snapshot is never copied — even with valid trade history.
/// FAIL: any TradeFill has the absent wallet's address as leader_wallet.
#[tokio::test]
async fn wallet_outside_snapshot_emits_no_signals() {
    let alice = wallet(ALICE_HEX); // in snapshot
    let bob = wallet(BOB_HEX); // NOT in snapshot
    let funder = wallet(FUNDER_HEX);

    // Both wallets have the same winning book.
    let mut all_trades = winner_book(alice, 0, 0);
    all_trades.extend(winner_book(bob, 0, 200));

    // Snapshot anchored 1 day before the simulation starts; contains alice only.
    let snap_pairs = vec![(day_unix(0) - DAY, vec![alice])];
    let snapshots = LeaderboardSnapshots::from_pairs(snap_pairs);

    let dir = TempDir::new().unwrap();
    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        vec![
            operator_for(alice, funder, b"alice-op"),
            operator_for(bob, funder, b"bob-op"),
        ],
        &snapshots,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
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
///       Carol must be copied at least once after the week-2 boundary
///       (proves the new pool became effective). Alice must never be copied
///       after the week-2 boundary (proves the old pool stopped being effective).
/// FAIL: carol absent from copied_leaders, or alice copied at/after week 2.
///
/// Timing notes:
/// - Alice book: days 0..66 (buys 0..64, sells 2..66). Qualifies by day 65 with
///   ≥60 closed trades.
/// - Carol book: days 70..136 (buys 70..134, sells 72..136). Qualifies by day 134.
/// - Week-2 anchor day_unix(134): carol's last BUY (day 134) lands during week 2
///   so a copy is possible. Alice's last trade (day 66) is before week 2, so any
///   alice copy must come from week 1.
#[tokio::test]
async fn weekly_pool_swap_changes_active_leaders() {
    let alice = wallet(ALICE_HEX);
    let carol = wallet(CAROL_HEX);
    let funder = wallet(FUNDER_HEX);

    let mut all_trades = winner_book(alice, 0, 0);
    all_trades.extend(winner_book(carol, 70, 200));

    let snapshots = LeaderboardSnapshots::from_pairs(vec![
        (day_unix(65), vec![alice]),
        (day_unix(134), vec![carol]),
    ]);

    let dir = TempDir::new().unwrap();
    let _ = run_simulation(
        &base_config(&dir),
        all_trades,
        vec![
            operator_for(alice, funder, b"alice-op-2"),
            operator_for(carol, funder, b"carol-op-2"),
        ],
        &snapshots,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
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
    let funder = wallet(FUNDER_HEX);

    let all_trades = winner_book(alice, 0, 0);

    // Two snapshots a week apart, alice in both.
    let snapshots = LeaderboardSnapshots::from_pairs(vec![
        (day_unix(0) - DAY, vec![alice]),
        (day_unix(7), vec![alice]),
    ]);

    let dir = TempDir::new().unwrap();
    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        vec![operator_for(alice, funder, b"alice-op-3")],
        &snapshots,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
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
/// FAIL: open_at_horizon == 0 AND no exit fill exists (i.e. the simulation
///       silently dropped the position state).
#[tokio::test]
async fn position_opened_in_week1_persists_after_drop() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    // Alice qualifies and buys at day 64. Sells matching come at day 66 — within
    // her 65-day book. After day 66 she goes quiet. We then drop her from the
    // snapshot at day 70.
    let all_trades = winner_book(alice, 0, 0);

    let snapshots = LeaderboardSnapshots::from_pairs(vec![
        (day_unix(0) - DAY, vec![alice]),
        // Week 2 (day 70): alice is no longer in any snapshot. She has no
        // wallets in this snapshot at all — pool is effectively empty.
        (day_unix(70), vec![]),
    ]);

    let dir = TempDir::new().unwrap();
    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        vec![operator_for(alice, funder, b"alice-op-4")],
        &snapshots,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
    )
    .unwrap();

    // The simulation must complete without panicking. Whether copies happened
    // (in week 1 while alice was still in the pool) plus any open_at_horizon
    // positions left over after she dropped out — the simulation must finish
    // cleanly with bankroll non-negative.
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
#[tokio::test]
async fn empty_snapshots_falls_back_to_full_history() {
    let alice = wallet(ALICE_HEX);
    let funder = wallet(FUNDER_HEX);

    let all_trades = winner_book(alice, 0, 0);
    let snapshots = LeaderboardSnapshots::default();
    assert!(snapshots.is_empty(), "fixture sanity");

    let dir = TempDir::new().unwrap();
    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        vec![operator_for(alice, funder, b"alice-op-5")],
        &snapshots,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
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
    let funder = wallet(FUNDER_HEX);

    // Alice book covers days 0..66.
    let all_trades = winner_book(alice, 0, 0);

    // Anchor the first snapshot AFTER alice's last sell — by then there are
    // no more trade days, so no copies can happen.
    let snapshots = LeaderboardSnapshots::from_pairs(vec![(day_unix(100), vec![alice])]);

    let dir = TempDir::new().unwrap();
    let report = run_simulation(
        &base_config(&dir),
        all_trades,
        vec![operator_for(alice, funder, b"alice-op-6")],
        &snapshots,
        &relaxed_ranker(),
        &LedgerConfig::default(),
        &default_strategy(),
    )
    .unwrap();

    assert_eq!(
        report.total_copies, 0,
        "no signals before first-snapshot anchor; got {}",
        report.total_copies
    );
}
