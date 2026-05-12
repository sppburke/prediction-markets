#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: source-priority enforcement (issue #149).
//!
//! Stage 6a (Polygon RPC) runs before stage 6b (CLOB). When Polygon already
//! wrote the resolution for market X, CLOB's later `insert_resolution_with_source`
//! must no-op via `INSERT OR IGNORE` — neither overwriting the polygon-sourced
//! row nor double-counting in the inserted total.
//!
//! PASS criterion: after Polygon writes one row and CLOB attempts to write a
//! competing row for the same market, the row's `source` remains `'polygon'`
//! AND the column values reflect Polygon's data (not CLOB's).

use pe_bootstrap::cache::WalletCache;
use tempfile::TempDir;

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

#[test]
fn polygon_resolution_survives_clob_retry() {
    let (_dir, mut cache) = open_cache();
    let market_id = "0xcond_priority";

    // Stage 6a (simulated): Polygon writes the canonical resolution with the
    // authoritative block-timestamp resolved_at.
    cache
        .insert_resolution_with_source(market_id, Some(0), 1_700_000_000, 1_700_000_010, "polygon")
        .unwrap();

    // Stage 6b (simulated): CLOB tries to write the same market with the
    // end_date_iso approximation and a (hypothetically different) winner.
    // INSERT OR IGNORE must keep the first row.
    cache
        .insert_resolution_with_source(market_id, Some(1), 1_700_999_999, 1_700_999_999, "clob")
        .unwrap();

    let (winner, resolved_at, _fetched_at, source) =
        cache.resolution_record(market_id).expect("row must exist");

    assert_eq!(winner, Some(0), "polygon's winner (index 0) must survive");
    assert_eq!(
        resolved_at, 1_700_000_000,
        "polygon's resolved_at (block timestamp) must survive over CLOB's approximation"
    );
    assert_eq!(source, "polygon", "row source must remain polygon");
}

#[test]
fn clob_fills_gap_only_when_polygon_did_not_write() {
    // Two markets: M1 was written by Polygon, M2 was not. CLOB attempts both.
    // M1 must remain polygon-sourced; M2 must end up with source='clob'.
    let (_dir, mut cache) = open_cache();
    cache
        .insert_resolution_with_source("0xm1", Some(0), 1_700_000_000, 1_700_000_010, "polygon")
        .unwrap();

    // CLOB attempts both. (Real CLOB iterates over every closed market from
    // the API and calls insert_resolution_with_source; the cache enforces
    // priority via INSERT OR IGNORE.)
    cache
        .insert_resolution_with_source("0xm1", Some(1), 1_700_999_999, 1_700_999_999, "clob")
        .unwrap();
    cache
        .insert_resolution_with_source("0xm2", Some(1), 1_701_000_000, 1_701_000_000, "clob")
        .unwrap();

    let (_, _, _, s1) = cache.resolution_record("0xm1").expect("M1 must exist");
    let (_, _, _, s2) = cache.resolution_record("0xm2").expect("M2 must exist");
    assert_eq!(s1, "polygon", "M1 must remain polygon-tagged");
    assert_eq!(s2, "clob", "M2 (no prior writer) must be clob-tagged");
}
