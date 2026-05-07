//! Scenario tests for the `build_funder_graph` cache-read path.
//!
//! These tests verify the end-to-end wiring: funder edges in `WalletCache` →
//! `build_funder_graph` → `Vec<OperatorIdentity>`.
//!
//! Scenarios:
//! 1. `empty_cache_returns_empty_identities` — no edges in cache → empty vec (no panic).
//! 2. `populated_cache_builds_correct_identities` — one funder with two funded wallets →
//!    exactly one `OperatorIdentity` containing all three wallets.
//! 3. `two_independent_operators_produce_two_identities` — two separate funder→funded pairs
//!    with different funders → two distinct identities.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_backtest::funder_graph::build_funder_graph;
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
const FUNDED_1: &str = "0x1111111111111111111111111111111111111111";
const FUNDED_2: &str = "0x2222222222222222222222222222222222222222";
const FUNDER_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FUNDED_3: &str = "0x3333333333333333333333333333333333333333";
// Fixed timestamp: 2023-11-01 00:00:00 UTC.
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

/// PASS: `build_funder_graph` on an empty cache returns an empty `Vec<OperatorIdentity>`
///       without panicking or returning an error.
/// FAIL: error returned, panic, or non-empty result.
#[test]
fn empty_cache_returns_empty_identities() {
    let dir = TempDir::new().unwrap();
    let cache = tmp_cache(&dir);
    let trades: Vec<RawTrade> = Vec::new();

    let identities = build_funder_graph(&cache, &trades).unwrap();

    assert!(
        identities.is_empty(),
        "empty funder cache must produce empty identities, got {}",
        identities.len()
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: one funder (A) with two funded wallets (1, 2) → exactly one `OperatorIdentity`
///       containing all three wallets as members.
/// FAIL: zero identities, or identity does not include all three wallets.
#[test]
fn populated_cache_builds_correct_identities() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    let funder_a = addr(FUNDER_A);
    let funded_1 = addr(FUNDED_1);
    let funded_2 = addr(FUNDED_2);

    // Populate funder edges: A funds 1 and 2.
    cache
        .insert_funder_edges(funded_1, &[funder_a], BASE_UNIX)
        .unwrap();
    cache
        .insert_funder_edges(funded_2, &[funder_a], BASE_UNIX)
        .unwrap();

    let trades = vec![dummy_trade(funded_1, "t1"), dummy_trade(funded_2, "t2")];

    let identities = build_funder_graph(&cache, &trades).unwrap();

    // Exactly one operator identity (one funder root → one cluster).
    assert_eq!(
        identities.len(),
        1,
        "one funder with two funded wallets must produce exactly one identity"
    );

    let identity = &identities[0];
    // All three wallets (funder + 2 funded) must be in the cluster.
    assert!(
        identity.member_wallets.contains(&funder_a),
        "funder A must be a member"
    );
    assert!(
        identity.member_wallets.contains(&funded_1),
        "funded_1 must be a member"
    );
    assert!(
        identity.member_wallets.contains(&funded_2),
        "funded_2 must be a member"
    );
    assert_eq!(
        identity.member_wallets.len(),
        3,
        "cluster must have exactly 3 members"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: two independent funder→funded pairs (A→1 and B→3) produce two distinct identities.
/// FAIL: fewer than 2 identities, or both wallets end up in the same cluster.
#[test]
fn two_independent_operators_produce_two_identities() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    let funder_a = addr(FUNDER_A);
    let funded_1 = addr(FUNDED_1);
    let funder_b = addr(FUNDER_B);
    let funded_3 = addr(FUNDED_3);

    // Operator A: funder_a → funded_1
    cache
        .insert_funder_edges(funded_1, &[funder_a], BASE_UNIX)
        .unwrap();
    // Operator B: funder_b → funded_3 (no shared funder)
    cache
        .insert_funder_edges(funded_3, &[funder_b], BASE_UNIX)
        .unwrap();

    let trades = vec![dummy_trade(funded_1, "t1"), dummy_trade(funded_3, "t3")];

    let identities = build_funder_graph(&cache, &trades).unwrap();

    assert_eq!(
        identities.len(),
        2,
        "two independent operators must produce two identities, got {}",
        identities.len()
    );

    // Verify the two clusters don't share wallets.
    let all_members: Vec<_> = identities
        .iter()
        .flat_map(|id| &id.member_wallets)
        .collect();
    let total_members = all_members.len();
    let unique_members: std::collections::HashSet<_> = all_members.into_iter().collect();
    assert_eq!(
        total_members,
        unique_members.len(),
        "no wallet should appear in two different operator identities"
    );
}
