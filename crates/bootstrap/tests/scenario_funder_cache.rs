//! Scenario tests for the funder-edge cache (`funder_edges` + `funder_lookup_done` tables).
//!
//! Scenarios:
//! 1. `fresh_cache_all_wallets_pending` — fresh cache with N trade wallets returns all N as pending.
//! 2. `partial_lookup_returns_remaining` — after marking one wallet done, only the others are pending.
//! 3. `complete_lookup_returns_empty` — after marking all wallets done, pending list is empty.
//! 4. `funder_edges_persist_and_load` — edges inserted for multiple wallets are all returned by `load_funder_edges`.
//! 5. `zero_funder_wallet_is_still_done` — a wallet with no funders is still marked done.
//! 6. `idempotent_insert_no_duplicates` — inserting the same edge twice stores only one row.
//! 7. `edges_survive_cache_reopen` — edges written in one open survive a close+reopen.
//! 8. `end_to_end_bootstrap_flow` — full flow: trades in → get pending → insert edges → verify done + load count.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const WALLET_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WALLET_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const WALLET_C: &str = "0xcccccccccccccccccccccccccccccccccccccccc";
const FUNDER_1: &str = "0x1111111111111111111111111111111111111111";
const FUNDER_2: &str = "0x2222222222222222222222222222222222222222";
// Fixed timestamp: 2023-11-01 00:00:00 UTC.
const BASE_UNIX: i64 = 1_698_796_800;

fn addr(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

fn tmp_cache(dir: &TempDir) -> WalletCache {
    WalletCache::open(&dir.path().join("cache.db")).unwrap()
}

fn insert_trade(cache: &mut WalletCache, trade_id: &str, wallet: WalletAddress, ts_offset: i64) {
    let trade = RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId("0xcond0001".to_owned())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price::new(dec!(0.50)).unwrap(),
        contracts: ContractQty(10),
        timestamp: SourceTimestamp(
            OffsetDateTime::from_unix_timestamp(BASE_UNIX + ts_offset).unwrap(),
        ),
        source_trade_id: SourceTradeId(trade_id.to_owned()),
    };
    cache.insert_new(&wallet.to_string(), vec![trade]).unwrap();
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────

/// PASS: fresh cache with 3 wallets → all 3 are returned as pending.
/// FAIL: any wallet is missing or the list has unexpected entries.
#[test]
fn fresh_cache_all_wallets_pending() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let (wa, wb, wc) = (addr(WALLET_A), addr(WALLET_B), addr(WALLET_C));

    insert_trade(&mut cache, "t1", wa, 0);
    insert_trade(&mut cache, "t2", wb, 1);
    insert_trade(&mut cache, "t3", wc, 2);

    let mut pending = cache.wallets_needing_funder_lookup().unwrap();
    pending.sort_by_key(|w| w.0);

    assert_eq!(pending.len(), 3);
    assert!(pending.contains(&wa));
    assert!(pending.contains(&wb));
    assert!(pending.contains(&wc));
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────

/// PASS: after marking wallet A done, only B and C are returned as pending.
/// FAIL: A reappears in pending, or B/C are missing.
#[test]
fn partial_lookup_returns_remaining() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let (wa, wb, wc) = (addr(WALLET_A), addr(WALLET_B), addr(WALLET_C));
    let f1 = addr(FUNDER_1);

    insert_trade(&mut cache, "t1", wa, 0);
    insert_trade(&mut cache, "t2", wb, 1);
    insert_trade(&mut cache, "t3", wc, 2);

    cache
        .insert_funder_edges(wa, &[(f1, BASE_UNIX)], BASE_UNIX)
        .unwrap();

    let pending = cache.wallets_needing_funder_lookup().unwrap();
    assert_eq!(pending.len(), 2);
    assert!(
        !pending.contains(&wa),
        "wallet A must not appear after being marked done"
    );
    assert!(pending.contains(&wb));
    assert!(pending.contains(&wc));
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────

/// PASS: after marking all wallets done, pending list is empty.
/// FAIL: any wallet remains in pending.
#[test]
fn complete_lookup_returns_empty() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let (wa, wb, wc) = (addr(WALLET_A), addr(WALLET_B), addr(WALLET_C));

    insert_trade(&mut cache, "t1", wa, 0);
    insert_trade(&mut cache, "t2", wb, 1);
    insert_trade(&mut cache, "t3", wc, 2);

    cache.insert_funder_edges(wa, &[], BASE_UNIX).unwrap();
    cache.insert_funder_edges(wb, &[], BASE_UNIX).unwrap();
    cache.insert_funder_edges(wc, &[], BASE_UNIX).unwrap();

    let pending = cache.wallets_needing_funder_lookup().unwrap();
    assert!(
        pending.is_empty(),
        "all wallets done — pending must be empty"
    );
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────

/// PASS: edges inserted for 3 wallets (2 funders each) → `load_funder_edges` returns 6 pairs.
/// FAIL: any edge is missing or count is wrong.
#[test]
fn funder_edges_persist_and_load() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let (wa, wb, wc) = (addr(WALLET_A), addr(WALLET_B), addr(WALLET_C));
    let (f1, f2) = (addr(FUNDER_1), addr(FUNDER_2));

    cache
        .insert_funder_edges(wa, &[(f1, BASE_UNIX), (f2, BASE_UNIX)], BASE_UNIX)
        .unwrap();
    cache
        .insert_funder_edges(wb, &[(f1, BASE_UNIX), (f2, BASE_UNIX)], BASE_UNIX)
        .unwrap();
    cache
        .insert_funder_edges(wc, &[(f1, BASE_UNIX), (f2, BASE_UNIX)], BASE_UNIX)
        .unwrap();

    let edges = cache.load_funder_edges().unwrap();
    assert_eq!(edges.len(), 6);
    // Every pair has one of the two funders and one of the three funded wallets.
    for (funder, funded) in &edges {
        assert!(*funder == f1 || *funder == f2, "unexpected funder {funder}");
        assert!(
            *funded == wa || *funded == wb || *funded == wc,
            "unexpected funded {funded}"
        );
    }
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────

/// PASS: a wallet with zero funders is still written to `funder_lookup_done`
///       so it is not re-queried on subsequent runs.
/// FAIL: the wallet reappears in `wallets_needing_funder_lookup` after a zero-funder insert.
#[test]
fn zero_funder_wallet_is_still_done() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let wa = addr(WALLET_A);

    insert_trade(&mut cache, "t1", wa, 0);

    // Insert with no funders.
    cache.insert_funder_edges(wa, &[], BASE_UNIX).unwrap();

    let pending = cache.wallets_needing_funder_lookup().unwrap();
    assert!(
        pending.is_empty(),
        "zero-funder wallet must still be marked done"
    );

    let edges = cache.load_funder_edges().unwrap();
    assert!(edges.is_empty());
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────

/// PASS: inserting the same (funder, funded) pair twice produces exactly one edge row.
/// FAIL: duplicate insert doubles the edge count.
#[test]
fn idempotent_insert_no_duplicates() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let funded = addr(WALLET_A);
    let funder = addr(FUNDER_1);

    cache
        .insert_funder_edges(funded, &[(funder, BASE_UNIX)], BASE_UNIX)
        .unwrap();
    cache
        .insert_funder_edges(funded, &[(funder, BASE_UNIX + 1)], BASE_UNIX + 1)
        .unwrap();

    let edges = cache.load_funder_edges().unwrap();
    assert_eq!(
        edges.len(),
        1,
        "duplicate (funder, funded) must not produce two rows"
    );
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────

/// PASS: edges written in one `WalletCache::open()` session are present after close+reopen.
/// FAIL: edges are lost across sessions.
#[test]
fn edges_survive_cache_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    let funded = addr(WALLET_A);
    let funder = addr(FUNDER_1);

    {
        let mut cache = WalletCache::open(&path).unwrap();
        cache
            .insert_funder_edges(funded, &[(funder, BASE_UNIX)], BASE_UNIX)
            .unwrap();
    }
    {
        let cache = WalletCache::open(&path).unwrap();
        let edges = cache.load_funder_edges().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], (funder, funded));
    }
}

// ── Scenario 8 ────────────────────────────────────────────────────────────────

/// PASS: full bootstrap flow — insert trades → get pending list → insert all edges →
///       pending list is now empty, edge count matches.
/// FAIL: pending not empty after all wallets processed, or edge count wrong.
#[test]
fn end_to_end_bootstrap_flow() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    let (wa, wb, wc) = (addr(WALLET_A), addr(WALLET_B), addr(WALLET_C));
    let (f1, f2) = (addr(FUNDER_1), addr(FUNDER_2));

    // Step 1: populate trades (simulates pe-bootstrap trade fetch phase).
    insert_trade(&mut cache, "t1", wa, 0);
    insert_trade(&mut cache, "t2", wb, 1);
    insert_trade(&mut cache, "t3", wc, 2);

    // Step 2: get pending — all 3 wallets should be pending.
    let pending = cache.wallets_needing_funder_lookup().unwrap();
    assert_eq!(pending.len(), 3);

    // Step 3: insert edges (simulates the Etherscan loop in pe-bootstrap).
    // wa has 2 funders, wb has 1, wc has none.
    cache
        .insert_funder_edges(wa, &[(f1, BASE_UNIX), (f2, BASE_UNIX)], BASE_UNIX)
        .unwrap();
    cache
        .insert_funder_edges(wb, &[(f1, BASE_UNIX)], BASE_UNIX)
        .unwrap();
    cache.insert_funder_edges(wc, &[], BASE_UNIX).unwrap();

    // Step 4: pending list must now be empty.
    let pending_after = cache.wallets_needing_funder_lookup().unwrap();
    assert!(
        pending_after.is_empty(),
        "all wallets processed — pending must be empty"
    );

    // Step 5: edge count = 3 (2 for wa + 1 for wb + 0 for wc).
    let edges = cache.load_funder_edges().unwrap();
    assert_eq!(edges.len(), 3);
}
