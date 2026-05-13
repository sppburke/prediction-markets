//! Scenario tests for the Dune on-chain resolution pipeline.
//!
//! Scenarios exercise `max_resolved_at_unix`, the `parse → insert → load` pipeline,
//! and the incremental / idempotency semantics of the cache resolution layer.
//! No network calls; Dune HTTP responses are not mocked — the parse logic is covered
//! by unit tests in `dune.rs`. These tests verify the cache behaviour that the
//! wired-up pipeline relies on.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_core_types::{MarketId, OutcomeId, VenueMarketId};
use tempfile::TempDir;

fn tmp_cache(dir: &TempDir) -> WalletCache {
    WalletCache::open(&dir.path().join("cache.db")).unwrap()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────
//
// PASS: fresh cache has max_resolved_at_unix = 0, enabling a full cold-start query.
// FAIL: returns any value other than 0 on an empty table.

#[test]
fn fresh_cache_max_resolved_at_unix_is_zero() {
    let dir = TempDir::new().unwrap();
    let cache = tmp_cache(&dir);
    let ts = cache.max_resolved_at_unix().unwrap();
    assert_eq!(
        ts, 0,
        "empty market_resolutions must return 0 (full scan from epoch)"
    );
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────
//
// PASS: after inserting resolutions, max_resolved_at_unix returns the highest
//       resolved_at_unix across all rows (not fetched_at_unix).
// FAIL: returns 0, the wrong row's timestamp, or a fetched_at value.

#[test]
fn max_resolved_at_unix_returns_correct_maximum() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    cache
        .insert_resolution("0xmarket_a", Some(0), 1_700_000_100, 1_700_000_200)
        .unwrap();
    cache
        .insert_resolution("0xmarket_b", Some(1), 1_700_000_500, 1_700_000_600)
        .unwrap();
    cache
        .insert_resolution("0xmarket_c", None, 1_700_000_300, 1_700_000_999)
        .unwrap();

    // max resolved_at is 1_700_000_500 (market_b), not 1_700_000_999 (fetched_at of c).
    assert_eq!(cache.max_resolved_at_unix().unwrap(), 1_700_000_500);
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────
//
// PASS: YES-win resolution inserted via the Dune pipeline round-trips through
//       load_all_resolutions with winning_outcome_id = 0.
// FAIL: market absent from index, or wrong winner.

#[test]
fn yes_win_resolution_roundtrips_to_index() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    // Simulate what lib.rs step 6b would insert after fetch_resolutions returns.
    let rows: Vec<(String, Option<u16>, i64)> =
        vec![("0xcond_yes_win".to_owned(), Some(0), 1_700_001_000)];
    let fetched_at = 1_700_002_000i64;
    for (market_id, winner, resolved_at_unix) in rows {
        cache
            .insert_resolution(&market_id, winner, resolved_at_unix, fetched_at)
            .unwrap();
    }

    let idx = cache.load_all_resolutions().unwrap();
    let market = idx
        .get(&MarketId(VenueMarketId("0xcond_yes_win".to_owned())))
        .expect("YES-win market must appear in resolution index");
    assert_eq!(
        market.winning_outcome_id,
        OutcomeId(0),
        "YES win = outcome index 0"
    );
    assert_eq!(market.resolved_at_unix, 1_700_001_000);
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────
//
// PASS: NO-win resolution round-trips with winning_outcome_id = 1.
// FAIL: market absent or wrong winner.

#[test]
fn no_win_resolution_roundtrips_to_index() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    cache
        .insert_resolution("0xcond_no_win", Some(1), 1_700_001_000, 1_700_002_000)
        .unwrap();

    let idx = cache.load_all_resolutions().unwrap();
    let market = idx
        .get(&MarketId(VenueMarketId("0xcond_no_win".to_owned())))
        .expect("NO-win market must appear in resolution index");
    assert_eq!(
        market.winning_outcome_id,
        OutcomeId(1),
        "NO win = outcome index 1"
    );
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────
//
// PASS: voided market (winner = None) is stored in the DB but excluded from
//       load_all_resolutions (which filters WHERE winning_outcome_id IS NOT NULL),
//       yet visible in resolved_market_ids (so Gamma/Dune won't re-fetch it).
// FAIL: voided market appears in the resolution index, or is absent from resolved_market_ids.

#[test]
fn voided_market_excluded_from_index_but_tracked() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    cache
        .insert_resolution("0xcond_voided", None, 1_700_001_000, 1_700_002_000)
        .unwrap();

    let idx = cache.load_all_resolutions().unwrap();
    assert!(
        !idx.contains_key(&MarketId(VenueMarketId("0xcond_voided".to_owned()))),
        "voided market must be excluded from load_all_resolutions"
    );

    let tracked = cache.resolved_market_ids();
    assert!(
        tracked.contains("0xcond_voided"),
        "voided market must appear in resolved_market_ids to prevent re-fetch"
    );
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────
//
// PASS: Gamma inserts first; Dune INSERT OR IGNORE does not overwrite the Gamma result.
//       This ensures the "first fetch wins" idempotency contract is preserved across
//       both resolution sources.
// FAIL: Dune overwrites Gamma's winner, or the final winner is wrong.

#[test]
fn gamma_first_dune_second_first_insert_wins() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    // Gamma inserts YES win (winner = 0).
    cache
        .insert_resolution("0xcond_shared", Some(0), 1_700_001_000, 1_700_002_000)
        .unwrap();

    // Dune would also try to insert for the same market (with the same result, but
    // simulated here as a conflicting value to verify the first-write-wins invariant).
    cache
        .insert_resolution("0xcond_shared", Some(1), 1_700_001_000, 1_700_003_000)
        .unwrap();

    let idx = cache.load_all_resolutions().unwrap();
    let market = idx
        .get(&MarketId(VenueMarketId("0xcond_shared".to_owned())))
        .expect("market must be present");
    assert_eq!(
        market.winning_outcome_id,
        OutcomeId(0),
        "first insert (Gamma, winner=0) must not be overwritten by second (Dune, winner=1)"
    );
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────
//
// PASS: after run 1 inserts resolutions, max_resolved_at_unix advances, and
//       run 2 can use it as the incremental cursor so only newer resolutions
//       need to be fetched from Dune.
// FAIL: max_resolved_at_unix doesn't advance, or returns a stale value.

#[test]
fn incremental_cursor_advances_after_each_run() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    // Run 1: 3 resolutions.
    assert_eq!(cache.max_resolved_at_unix().unwrap(), 0, "cold start");
    for (id, ts) in [
        ("0xa", 1_700_000_100i64),
        ("0xb", 1_700_000_200),
        ("0xc", 1_700_000_300),
    ] {
        cache.insert_resolution(id, Some(0), ts, ts + 100).unwrap();
    }
    let cursor_after_run1 = cache.max_resolved_at_unix().unwrap();
    assert_eq!(
        cursor_after_run1, 1_700_000_300,
        "cursor must advance to max ts"
    );

    // Run 2: 2 more resolutions with higher timestamps.
    for (id, ts) in [("0xd", 1_700_000_400i64), ("0xe", 1_700_000_500)] {
        cache.insert_resolution(id, Some(1), ts, ts + 100).unwrap();
    }
    let cursor_after_run2 = cache.max_resolved_at_unix().unwrap();
    assert_eq!(
        cursor_after_run2, 1_700_000_500,
        "cursor must advance to new max"
    );
    assert!(
        cursor_after_run2 > cursor_after_run1,
        "incremental cursor must strictly advance"
    );
}

// ── Scenario 8 ────────────────────────────────────────────────────────────────
//
// PASS: a condition ID that arrives from Dune with \\x prefix is normalised to
//       0x before storage, and can be looked up in the resolution index using
//       the 0x-prefixed market ID that the trade cache would hold.
// FAIL: the market is absent from the index, or stored with the wrong key.

#[test]
fn normalised_condition_id_matches_trade_cache_format() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    // The Dune parse step normalises \xaabbcc... → 0xaabbcc...
    // Here we simulate the post-normalisation insert.
    let normalised_id = "0xaabbccddeeff1122334455667788990011223344556677889900aabbccddeeff";
    cache
        .insert_resolution(normalised_id, Some(0), 1_700_001_000, 1_700_002_000)
        .unwrap();

    let idx = cache.load_all_resolutions().unwrap();
    assert!(
        idx.contains_key(&MarketId(VenueMarketId(normalised_id.to_owned()))),
        "0x-prefixed normalised condition ID must be retrievable from resolution index"
    );
}

// ── Scenario 9 ────────────────────────────────────────────────────────────────
//
// PASS: markets already present in `resolved_market_ids` are correctly identified
//       as not needing re-fetch. The unresolved set (all_market_ids -
//       resolved_market_ids) is empty when every known market has a resolution.
//       This is the invariant the lib.rs wiring uses to skip the Dune query
//       on subsequent runs when nothing is new.
// FAIL: `resolved_market_ids` omits a previously-inserted market, causing a
//       spurious re-fetch on the next run.

#[test]
fn resolved_market_ids_covers_all_inserted_markets() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);

    let markets = ["0xcond_a", "0xcond_b", "0xcond_c"];
    for m in markets {
        cache
            .insert_resolution(m, Some(0), 1_700_001_000, 1_700_002_000)
            .unwrap();
    }

    let resolved = cache.resolved_market_ids();
    for m in markets {
        assert!(
            resolved.contains(m),
            "resolved_market_ids must contain {m} after insertion"
        );
    }

    // Simulate the unresolved-set computation from lib.rs step 6b.
    let all: std::collections::HashSet<&str> = markets.iter().copied().collect();
    let unresolved: Vec<&&str> = all.iter().filter(|id| !resolved.contains(**id)).collect();
    assert!(
        unresolved.is_empty(),
        "unresolved set must be empty when every known market has a resolution — \
         would cause a spurious Dune query on the next run"
    );
}
