//! Scenario: V1-attribution backfill subcommand semantics (issue #191 Item 2).
//!
//! Covers the end-to-end behaviour of `pe-bootstrap --backfill-v1-attribution`:
//! the cursor-clearing primitive [`migrate::reset_v1_topic_cursors`] must
//! produce exactly the cursor state that lets the next normal bootstrap run
//! re-enumerate V1 and populate `polymarket_contracts_seen` bit 0 for every
//! legacy-ingested wallet.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    // Used to assert "wallet_hex" -> contracts_seen mapping; safe in tests.
    clippy::needless_borrow
)]

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::migrate;
use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
};
use tempfile::TempDir;

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

fn v1_topic_hex() -> String {
    format!("{TOPIC_ORDER_FILLED_V1}")
}

fn v2_topic_hex() -> String {
    format!("{TOPIC_ORDER_FILLED_V2}")
}

fn contract_hex(idx: usize) -> String {
    format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[idx])
}

// ── Scenario 1 — V1 topic removed, V2 topic preserved ───────────────────────

/// PASS: when both V1 and V2 are in `enumerated_topic_hashes`, the backfill
///       removes only V1 and leaves V2 alone. Returns
///       `(topic_present=true, removed_chunks=0)` if no V1 chunk_progress
///       entries exist.
/// FAIL: V2 is also removed (would force a full V2 re-sweep), OR the function
///       returns a value inconsistent with the on-disk state.
#[test]
fn reset_v1_removes_v1_topic_preserves_v2() {
    let (_dir, mut cache) = open_cache();
    // Pre-populate the cursor as if a full V1+V2 sweep has completed.
    migrate::save_enum_state(
        &mut cache,
        &[contract_hex(0), contract_hex(1)],
        &[v1_topic_hex(), v2_topic_hex()],
    )
    .unwrap();

    let (topic_present, chunks_removed) = migrate::reset_v1_topic_cursors(&mut cache).unwrap();

    assert!(
        topic_present,
        "should report that the V1 topic was present in the cursor"
    );
    assert_eq!(
        chunks_removed, 0,
        "no chunk_progress entries existed, so zero should be removed"
    );

    let (_contracts, topics) = migrate::load_enum_state(&cache).unwrap();
    assert!(
        !topics.contains(&v1_topic_hex()),
        "V1 topic must be gone after reset; got: {topics:?}"
    );
    assert!(
        topics.contains(&v2_topic_hex()),
        "V2 topic must still be present after reset; got: {topics:?}"
    );
}

// ── Scenario 2 — V1-keyed chunk_progress entries pruned, V2 preserved ───────

/// PASS: when chunk_progress has entries for both V1 and V2 (topic|contract),
///       the backfill removes only the V1-keyed entries and reports the
///       count. V2-keyed entries survive untouched — this is critical because
///       today's V2 sweep is in-flight and clobbering V2 progress would cost
///       hours of re-enumeration.
/// FAIL: V2 chunk entries also removed, OR removal count is wrong.
#[test]
fn reset_v1_prunes_only_v1_keyed_chunk_progress() {
    let (_dir, mut cache) = open_cache();

    let mut progress = HashMap::new();
    let v1_key_a = migrate::chunk_progress_key(&v1_topic_hex(), &contract_hex(0));
    let v1_key_b = migrate::chunk_progress_key(&v1_topic_hex(), &contract_hex(1));
    let v2_key_a = migrate::chunk_progress_key(&v2_topic_hex(), &contract_hex(0));
    let v2_key_b = migrate::chunk_progress_key(&v2_topic_hex(), &contract_hex(1));
    progress.insert(v1_key_a.clone(), 50_000_000);
    progress.insert(v1_key_b.clone(), 60_000_000);
    progress.insert(v2_key_a.clone(), 80_000_000);
    progress.insert(v2_key_b.clone(), 81_000_000);
    migrate::save_chunk_progress(&mut cache, &progress).unwrap();
    migrate::save_enum_state(&mut cache, &[], &[v1_topic_hex()]).unwrap();

    let (topic_present, chunks_removed) = migrate::reset_v1_topic_cursors(&mut cache).unwrap();

    assert!(topic_present, "V1 topic was in enumerated_topic_hashes");
    assert_eq!(
        chunks_removed, 2,
        "exactly 2 V1-keyed entries should be removed; got {chunks_removed}"
    );

    let after = migrate::load_chunk_progress(&cache).unwrap();
    assert!(!after.contains_key(&v1_key_a), "V1 entry must be pruned");
    assert!(!after.contains_key(&v1_key_b), "V1 entry must be pruned");
    assert_eq!(after.get(&v2_key_a).copied(), Some(80_000_000));
    assert_eq!(after.get(&v2_key_b).copied(), Some(81_000_000));
    assert_eq!(after.len(), 2);
}

// ── Scenario 3 — idempotent on a cache where V1 is already absent ───────────

/// PASS: calling reset on a cache where V1 is already not in
///       `enumerated_topic_hashes` AND no V1 chunk_progress entries exist
///       returns `(false, 0)` without erroring. Important for the operator's
///       "did I already do this?" check — running the subcommand twice is
///       safe.
/// FAIL: idempotent call errors, OR mutates cursor state.
#[test]
fn reset_v1_is_idempotent_when_v1_already_cleared() {
    let (_dir, mut cache) = open_cache();
    // Only V2 in the cursor — simulates a post-backfill state.
    migrate::save_enum_state(&mut cache, &[], &[v2_topic_hex()]).unwrap();
    let v2_key = migrate::chunk_progress_key(&v2_topic_hex(), &contract_hex(0));
    let mut progress = HashMap::new();
    progress.insert(v2_key.clone(), 80_000_000);
    migrate::save_chunk_progress(&mut cache, &progress).unwrap();

    let (topic_present, chunks_removed) = migrate::reset_v1_topic_cursors(&mut cache).unwrap();
    assert!(!topic_present, "V1 was not present, so should report false");
    assert_eq!(chunks_removed, 0, "no V1 chunks existed, so 0 removed");

    // V2 state preserved.
    let (_, topics) = migrate::load_enum_state(&cache).unwrap();
    assert_eq!(topics, vec![v2_topic_hex()]);
    let progress = migrate::load_chunk_progress(&cache).unwrap();
    assert_eq!(progress.get(&v2_key).copied(), Some(80_000_000));
}

// ── Scenario 4 — cache mutation lock acquired by subcommand semantics ───────

/// PASS: while a `CacheMutationLock` is held, a second acquisition against
///       the same cache path returns `BootstrapError::Invalid` naming the
///       holder PID. This is the safety contract that prevents the
///       subcommand from racing against an in-flight `pe-bootstrap` sweep.
/// FAIL: the second acquisition succeeds (race window open), OR returns the
///       wrong error variant.
#[test]
fn cache_mutation_lock_blocks_concurrent_acquire_for_subcommand() {
    use pe_bootstrap::error::BootstrapError;
    use pe_bootstrap::lock::CacheMutationLock;

    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("wallet_cache.db");

    let _first = CacheMutationLock::acquire(&cache_path).unwrap();
    match CacheMutationLock::acquire(&cache_path) {
        Err(BootstrapError::Invalid { message }) => {
            assert!(
                message.contains("Stop the running pe-bootstrap"),
                "operator-facing message should guide them to stop the holder; got: {message}"
            );
        }
        other => panic!("expected Invalid error, got: {other:?}"),
    }
}

// ── Scenario 5 — full operator workflow: reset → re-enumerate populates bit 0 ─

/// PASS: the end-to-end backfill flow works as documented:
///       1. A legacy wallet exists at `polymarket_contracts_seen = 0`
///          (simulating pre-#186 wallet_set.json ingestion).
///       2. `reset_v1_topic_cursors` clears the V1 topic from the cursor.
///       3. A simulated "normal sweep" upserts the same wallet with
///          `CONTRACT_VERSION_BIT_V1`.
///       4. Final state: `polymarket_contracts_seen = 1` (V1 bit set).
///
/// This pins the OR-merge invariant the backfill plan depends on: existing
/// bit=0 rows OR-merge cleanly with new bit=1 upserts.
/// FAIL: any of the above steps breaks the invariant — for example if the
/// UPSERT replaced rather than OR-merged the column.
#[test]
fn end_to_end_backfill_populates_v1_bit_via_or_merge() {
    use pe_bootstrap::pile::SRC_WALLET_SET_JSON;
    use pe_source_onchain_polygon::contracts::CONTRACT_VERSION_BIT_V1;

    let (_dir, mut cache) = open_cache();
    let wallet_hex = "0xabcdef0123456789abcdef0123456789abcdef01";

    // 1. Simulate pre-#186 legacy ingestion: SRC_WALLET_SET_JSON bit set,
    //    polymarket_contracts_seen = 0.
    cache
        .upsert_wallets_bulk(&[(
            wallet_hex.to_owned(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0, // ← no V1/V2 attribution at ingest time
        )])
        .unwrap();
    assert_eq!(
        cache.conn_for_test_contracts_seen(wallet_hex),
        0,
        "pre-backfill: wallet must have no V1/V2 attribution"
    );

    // 2. Pre-populate cursor as if V1 was already enumerated; call reset.
    migrate::save_enum_state(&mut cache, &[], &[v1_topic_hex()]).unwrap();
    let (_, _) = migrate::reset_v1_topic_cursors(&mut cache).unwrap();
    let (_, topics) = migrate::load_enum_state(&cache).unwrap();
    assert!(!topics.contains(&v1_topic_hex()), "V1 cursor cleared");

    // 3. Simulate the next bootstrap run's per-chunk upsert (which uses
    //    CONTRACT_VERSION_BIT_V1 as the 7th tuple element when sweeping V1).
    cache
        .upsert_wallets_bulk(&[(
            wallet_hex.to_owned(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            CONTRACT_VERSION_BIT_V1,
        )])
        .unwrap();

    // 4. End state: the OR-merge UPSERT sets bit 0.
    assert_eq!(
        cache.conn_for_test_contracts_seen(wallet_hex),
        CONTRACT_VERSION_BIT_V1,
        "post-backfill: wallet must carry the V1 bit via OR-merge"
    );
}
