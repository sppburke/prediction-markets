//! Scenario tests for `FunderGraphTimeline` and `build_operator_identities_at`.
//!
//! Scenarios:
//! 1. `empty_timeline_view_at_returns_empty` — empty timeline; view_at any t is empty.
//! 2. `edge_visible_at_and_after_fetch_timestamp` — one edge at t=100; view_at(99)=[], view_at(100)=[1].
//! 3. `multiple_edges_filtered_by_timestamp` — edges at t=100 and t=200; view_at(150) sees only the first.
//! 4. `from_cache_loads_edges_in_timestamp_order` — insert edges with different timestamps; timeline
//!    returns them sorted ascending.
//! 5. `build_operator_identities_at_excludes_future_edges` — insert edges at t=100 and t=200; build at
//!    t=150 produces one identity (first edge only); build at t=200 produces two.
//! 6. `empty_cache_yields_empty_timeline` — `from_cache` on a zero-edge cache returns `is_empty()`.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_backtest::FunderGraphTimeline;
use pe_backtest::funder_graph::build_operator_identities_at;
use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const FUNDER_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FUNDER_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FUNDED_1: &str = "0x1111111111111111111111111111111111111111";
const FUNDED_2: &str = "0x2222222222222222222222222222222222222222";

// Fixed base timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;

fn addr(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn tmp_cache(dir: &TempDir) -> WalletCache {
    WalletCache::open(&dir.path().join("cache.db")).unwrap()
}

fn dummy_trade(wallet: WalletAddress, id: &str) -> RawTrade {
    RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId("0xcond0001".to_owned())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price::new(dec!(0.50)).unwrap(),
        contracts: ContractQty(10),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(BASE_UNIX).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: `FunderGraphTimeline::empty().view_at(t)` returns an empty slice for any `t`.
/// FAIL: any non-empty slice returned, or panic.
#[test]
fn empty_timeline_view_at_returns_empty() {
    let timeline = FunderGraphTimeline::empty();
    assert!(timeline.is_empty());
    assert_eq!(timeline.view_at(i64::MIN).len(), 0);
    assert_eq!(timeline.view_at(0).len(), 0);
    assert_eq!(timeline.view_at(i64::MAX).len(), 0);
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: one edge at t=BASE_UNIX; view_at(t-1) is empty; view_at(t) has 1 edge.
/// FAIL: edge visible before its fetch timestamp, or absent at the exact timestamp.
#[test]
fn edge_visible_at_and_after_fetch_timestamp() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let funder = addr(FUNDER_A);
    let funded = addr(FUNDED_1);

    cache
        .insert_funder_edges(funded, &[funder], BASE_UNIX)
        .unwrap();
    let timeline = FunderGraphTimeline::from_cache(&cache).unwrap();

    assert_eq!(
        timeline.view_at(BASE_UNIX - 1).len(),
        0,
        "edge must not be visible one second before its fetch timestamp"
    );
    assert_eq!(
        timeline.view_at(BASE_UNIX).len(),
        1,
        "edge must be visible at exactly its fetch timestamp"
    );
    assert_eq!(
        timeline.view_at(BASE_UNIX + 86_400).len(),
        1,
        "edge must remain visible after its fetch timestamp"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: two edges at t=BASE_UNIX and t=BASE_UNIX+100; view_at(BASE_UNIX+50) sees 1 edge;
///       view_at(BASE_UNIX+100) sees 2.
/// FAIL: wrong count at either checkpoint.
#[test]
fn multiple_edges_filtered_by_timestamp() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let funder_a = addr(FUNDER_A);
    let funder_b = addr(FUNDER_B);
    let funded_1 = addr(FUNDED_1);
    let funded_2 = addr(FUNDED_2);

    cache
        .insert_funder_edges(funded_1, &[funder_a], BASE_UNIX)
        .unwrap();
    cache
        .insert_funder_edges(funded_2, &[funder_b], BASE_UNIX + 100)
        .unwrap();

    let timeline = FunderGraphTimeline::from_cache(&cache).unwrap();

    assert_eq!(
        timeline.view_at(BASE_UNIX + 50).len(),
        1,
        "only the first edge should be visible at t = BASE+50"
    );
    assert_eq!(
        timeline.view_at(BASE_UNIX + 100).len(),
        2,
        "both edges should be visible at t = BASE+100"
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: `from_cache` returns edges sorted ascending by `fetched_at_unix`, regardless
///       of insertion order.
/// FAIL: edges are unsorted, or `total_edge_count` is wrong.
#[test]
fn from_cache_loads_edges_in_timestamp_order() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let funder_a = addr(FUNDER_A);
    let funder_b = addr(FUNDER_B);
    let funded_1 = addr(FUNDED_1);
    let funded_2 = addr(FUNDED_2);

    // Insert later edge first.
    cache
        .insert_funder_edges(funded_2, &[funder_b], BASE_UNIX + 200)
        .unwrap();
    cache
        .insert_funder_edges(funded_1, &[funder_a], BASE_UNIX + 100)
        .unwrap();

    let timeline = FunderGraphTimeline::from_cache(&cache).unwrap();
    assert_eq!(timeline.total_edge_count(), 2, "expected 2 edges total");

    // The first edge visible at BASE+150 (where only the t=BASE+100 edge is included)
    // must be the one inserted with the earlier timestamp.
    let slice = timeline.view_at(BASE_UNIX + 150);
    assert_eq!(slice.len(), 1, "only 1 edge visible at BASE+150");
    // The visible edge is (ts=BASE+100, funder_a, funded_1).
    let (ts, _funder, funded) = slice[0];
    assert_eq!(
        ts,
        BASE_UNIX + 100,
        "visible edge must have the earlier timestamp"
    );
    assert_eq!(funded, funded_1, "visible edge's funded wallet must match");
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: `build_operator_identities_at` at t=BASE+150 uses only the edge at BASE+100,
///       producing 1 identity; at t=BASE+200 it uses both edges, producing 2 identities.
/// FAIL: wrong identity count at either checkpoint.
#[test]
fn build_operator_identities_at_excludes_future_edges() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let funder_a = addr(FUNDER_A);
    let funder_b = addr(FUNDER_B);
    let funded_1 = addr(FUNDED_1);
    let funded_2 = addr(FUNDED_2);

    cache
        .insert_funder_edges(funded_1, &[funder_a], BASE_UNIX + 100)
        .unwrap();
    cache
        .insert_funder_edges(funded_2, &[funder_b], BASE_UNIX + 200)
        .unwrap();

    let timeline = FunderGraphTimeline::from_cache(&cache).unwrap();

    // Pass only a trade for funded_1. funded_2 has no closed trades, so it only
    // becomes discoverable when the funder_b→funded_2 edge appears at BASE+200.
    let trades = vec![dummy_trade(funded_1, "t1")];

    let ids_at_150 = build_operator_identities_at(&timeline, &trades, BASE_UNIX + 150).unwrap();
    assert_eq!(
        ids_at_150.len(),
        1,
        "at t=BASE+150 only the first edge is visible → 1 identity, got {}",
        ids_at_150.len()
    );

    let ids_at_200 = build_operator_identities_at(&timeline, &trades, BASE_UNIX + 200).unwrap();
    assert_eq!(
        ids_at_200.len(),
        2,
        "at t=BASE+200 both edges are visible → 2 identities, got {}",
        ids_at_200.len()
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: `FunderGraphTimeline::from_cache` on a zero-edge cache returns `is_empty() = true`.
/// FAIL: error, panic, or `is_empty() = false`.
#[test]
fn empty_cache_yields_empty_timeline() {
    let dir = TempDir::new().unwrap();
    let cache = tmp_cache(&dir);
    let timeline = FunderGraphTimeline::from_cache(&cache).unwrap();
    assert!(
        timeline.is_empty(),
        "timeline from empty cache must report is_empty()"
    );
    assert_eq!(timeline.total_edge_count(), 0);
}
