//! Operator-level scenarios for the issue #181 cursor-based enumeration
//! state and the narrowly-scoped trade-fetch wallet query.
//!
//! These exercise the SQLite source-of-truth that `lib.rs::run()` reads from
//! after `auto_migrate_legacy` has populated state:
//!   1. `migrate::load_enum_state` round-trip through `source_cursor`.
//!   2. `cache::wallets_with_source_bit` returns ONLY the discovered-wallet
//!      subset, NOT the full 2.7M-row pile (the critical issue #181 fix).
//!   3. UPSERT accumulation: same wallet re-inserted with the same bit is
//!      a no-op (no duplicate rows, no source_bits drift).
//!
//! Determinism: pure in-process; each test gets a fresh `TempDir`.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::{WalletCache, WalletUpsertRow};
use pe_bootstrap::migrate::{self, save_enum_state};
use pe_bootstrap::pile::{SRC_DUNE_CSV, SRC_TRADES, SRC_WALLET_SET_JSON};
use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
};
use tempfile::TempDir;

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

// ── Scenario 1 ───────────────────────────────────────────────────────────────
// PASS: save_enum_state → load_enum_state round-trips both vecs verbatim,
//       including ordering (preserves the per-topic crash-resume invariant).
// FAIL: vecs reordered, lost, or truncated.

#[test]
fn save_load_enum_state_round_trip() {
    let (_dir, mut cache) = open_cache();

    let contracts: Vec<String> = ALL_EXCHANGE_CONTRACTS
        .iter()
        .map(|c| format!("0x{c:x}"))
        .collect();
    let topics: Vec<String> = vec![
        format!("{TOPIC_ORDER_FILLED_V1}"),
        format!("{TOPIC_ORDER_FILLED_V2}"),
    ];

    save_enum_state(&mut cache, &contracts, &topics).unwrap();
    let (loaded_contracts, loaded_topics) = migrate::load_enum_state(&cache).unwrap();

    assert_eq!(loaded_contracts, contracts, "contracts must round-trip");
    assert_eq!(loaded_topics, topics, "topics must round-trip");
}

// ── Scenario 2 ───────────────────────────────────────────────────────────────
// PASS: on missing cursor keys (fresh install), load_enum_state returns
//       (vec![], vec![]) — the sentinel value that the run() skip-guard
//       interprets correctly as "no enumeration done yet."
// FAIL: returns an error, or some other default that the guard would
//       misinterpret as "everything done."

#[test]
fn load_enum_state_returns_empty_on_fresh_install() {
    let (_dir, cache) = open_cache();
    let (contracts, topics) = migrate::load_enum_state(&cache).unwrap();
    assert!(contracts.is_empty());
    assert!(topics.is_empty());
}

// ── Scenario 3 ───────────────────────────────────────────────────────────────
// PASS: wallets_with_source_bit(SRC_WALLET_SET_JSON) returns ONLY the
//       wallets that have that bit set — even when the cache contains many
//       additional wallets with other source bits (the issue #181 scope
//       blowup the trade-fetch path was vulnerable to).
// FAIL: returns the full pile (all_pile_wallet_hexes behavior) → trade-fetch
//       scope blowup → production multi-day API runs and rate-limit failures.

#[test]
fn wallets_with_source_bit_excludes_other_source_bits() {
    let (_dir, mut cache) = open_cache();

    // Insert a mix of source-bit populations that mirrors the production
    // shape from issue #181's audit:
    //   - 3 wallets with SRC_WALLET_SET_JSON (the trade-fetch target)
    //   - 5 wallets with SRC_DUNE_CSV only (the 2.5M-row pool that must
    //     be EXCLUDED — these have never traded)
    //   - 2 wallets with SRC_TRADES only
    let mut rows: Vec<WalletUpsertRow> = Vec::new();
    for i in 0..3u8 {
        rows.push((
            format!("0x{:040x}", 0xa0 + u64::from(i)),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
        ));
    }
    for i in 0..5u8 {
        rows.push((
            format!("0x{:040x}", 0xb0 + u64::from(i)),
            SRC_DUNE_CSV,
            false,
            None,
            None,
            None,
        ));
    }
    for i in 0..2u8 {
        rows.push((
            format!("0x{:040x}", 0xc0 + u64::from(i)),
            SRC_TRADES,
            false,
            None,
            None,
            None,
        ));
    }
    cache.upsert_wallets_bulk(&rows).unwrap();

    // Trade-fetch query — must return ONLY the 3 SRC_WALLET_SET_JSON wallets.
    let trade_fetch_scope = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON).unwrap();
    assert_eq!(
        trade_fetch_scope.len(),
        3,
        "trade-fetch scope MUST be narrowed to SRC_WALLET_SET_JSON; \
         got {} (would have caused production scope blowup)",
        trade_fetch_scope.len()
    );

    // Sanity: all 10 are present in the unscoped query.
    let all = cache.all_pile_wallet_hexes().unwrap();
    assert_eq!(all.len(), 10, "all 10 wallets present in the full pile");
}

// ── Scenario 4 ───────────────────────────────────────────────────────────────
// PASS: a wallet present in multiple source buckets (SRC_WALLET_SET_JSON +
//       SRC_TRADES + SRC_DUNE_CSV — the production-realistic combination)
//       appears in wallets_with_source_bit(SRC_WALLET_SET_JSON) exactly once.
// FAIL: duplicate rows returned, OR wallet missing because of bit-mask
//       ambiguity.

#[test]
fn wallets_with_source_bit_handles_multi_bit_wallets() {
    let (_dir, mut cache) = open_cache();
    let hex = "0xd000000000000000000000000000000000000000";

    // Three separate UPSERTs accumulate source bits via OR.
    let combined = SRC_WALLET_SET_JSON | SRC_TRADES | SRC_DUNE_CSV;
    for bit in [SRC_WALLET_SET_JSON, SRC_TRADES, SRC_DUNE_CSV] {
        cache
            .upsert_wallets_bulk(&[(hex.to_owned(), bit, false, None, None, None)])
            .unwrap();
    }

    let scoped = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON).unwrap();
    assert_eq!(
        scoped,
        vec![hex.to_owned()],
        "exactly one row, exactly once"
    );

    // The wallet must also match queries for any of its other bits.
    assert_eq!(
        cache.wallets_with_source_bit(SRC_TRADES).unwrap().len(),
        1,
        "multi-bit wallet must match SRC_TRADES query too"
    );
    assert_eq!(
        cache.wallets_with_source_bit(combined).unwrap().len(),
        1,
        "multi-bit wallet must match combined mask"
    );
}

// ── Scenario 5 ───────────────────────────────────────────────────────────────
// PASS: wallets_with_source_bit(0) returns an empty Vec (no wallet has
//       any bit in common with the empty mask). Critical edge case — a buggy
//       implementation that returns all rows on bit=0 would expand scope.
// FAIL: returns the full pile.

#[test]
fn wallets_with_source_bit_zero_returns_empty() {
    let (_dir, mut cache) = open_cache();
    cache
        .upsert_wallets_bulk(&[(
            "0xe000000000000000000000000000000000000000".to_owned(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
        )])
        .unwrap();

    let scoped = cache.wallets_with_source_bit(0).unwrap();
    assert!(
        scoped.is_empty(),
        "bit_mask=0 must match nothing (defensive against bit-mask off-by-one bugs)"
    );
}

// ── Scenario 6 ───────────────────────────────────────────────────────────────
// PASS: the post-migration "all topics + all contracts done" state (which
//       lib.rs::run()'s skip-guard reads) round-trips correctly. Mirror of
//       the guard's check in lib.rs.
// FAIL: the cursor-driven skip-guard misreads the state and triggers a
//       spurious re-enumeration (multi-hour Etherscan sweep on every run).

#[test]
fn cursor_state_matches_skip_guard_invariant() {
    let (_dir, mut cache) = open_cache();

    // Simulate the post-#181-migration steady state: all 4 contracts done,
    // both V1+V2 topics done.
    let contracts: Vec<String> = ALL_EXCHANGE_CONTRACTS
        .iter()
        .map(|c| format!("0x{c:x}"))
        .collect();
    let topics: Vec<String> = ALL_ORDER_FILLED_TOPICS
        .iter()
        .map(|h| format!("{h}"))
        .collect();
    save_enum_state(&mut cache, &contracts, &topics).unwrap();

    let (loaded_contracts, loaded_topics) = migrate::load_enum_state(&cache).unwrap();

    // Mirror of lib.rs::run()'s skip-guard exactly.
    let total_contracts = ALL_EXCHANGE_CONTRACTS.len();
    let all_topics_done = ALL_ORDER_FILLED_TOPICS
        .iter()
        .all(|h| loaded_topics.contains(&format!("{h}")));
    assert!(
        loaded_contracts.len() >= total_contracts && all_topics_done,
        "post-migration state must trigger the skip-guard"
    );

    // And the negative case: remove one topic, verify the guard does NOT fire.
    save_enum_state(
        &mut cache,
        &contracts,
        &topics[..1], // only V1 done
    )
    .unwrap();
    let (_, partial_topics) = migrate::load_enum_state(&cache).unwrap();
    let all_topics_done_partial = ALL_ORDER_FILLED_TOPICS
        .iter()
        .all(|h| partial_topics.contains(&format!("{h}")));
    assert!(
        !all_topics_done_partial,
        "partial migration (V2 missing) MUST NOT trigger the skip-guard"
    );
}
