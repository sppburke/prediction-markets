#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Scenario: the prices-history cache substrate (issue #421 PR4) — hermetic, no network.
//!
//! Covers the new DB-I/O surface the `prices-history` subcommand relies on: the backfill-targets
//! join (`token_conditions ⋈ market_resolutions ⋈ market_schedules`, close-ref COALESCE + the
//! resume skip + LIMIT) and the `start_date_unix` UPDATE/missing-query (the createdAt sink). The
//! network fetch paths are covered by the `FixtureFetcher` unit tests on the client + GammaFetcher.

use std::collections::HashSet;

use pe_bootstrap::cache::WalletCache;
use tempfile::TempDir;

fn open() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

// PASS: targets() returns exactly the mapped-token resolved markets; close_ref = end_date when
//       present else resolved_at; a backfilled token is skipped (resume); LIMIT bounds the count.
// FAIL: an unresolved/unmapped market appears, a close_ref is wrong, resume fails, or LIMIT ignored.
#[test]
fn backfill_targets_join_close_ref_and_resume() {
    let (_dir, mut cache) = open();

    // 0xm1: resolved + scheduled (end_date 1800) + two tokens → close_ref = end_date 1800.
    cache.insert_resolution("0xm1", Some(0), 2000, 9).unwrap();
    cache.insert_schedule("0xm1", Some(1800), 9).unwrap();
    // 0xm2: resolved (resolved_at 3000), NO schedule row, one token → close_ref = resolved_at 3000.
    cache.insert_resolution("0xm2", Some(1), 3000, 9).unwrap();
    // 0xm3: scheduled + token but NOT resolved → excluded from the targets.
    cache.insert_schedule("0xm3", Some(1234), 9).unwrap();
    cache
        .upsert_token_conditions_batch(
            &[
                ("t1".to_owned(), "0xm1".to_owned()),
                ("t2".to_owned(), "0xm1".to_owned()),
                ("t3".to_owned(), "0xm2".to_owned()),
                ("t9".to_owned(), "0xm3".to_owned()),
            ],
            9,
        )
        .unwrap();

    let got: HashSet<(String, String, i64)> = cache
        .price_history_backfill_targets(0)
        .unwrap()
        .iter()
        .map(|t| (t.market_id.clone(), t.token_id.clone(), t.close_ref_unix))
        .collect();
    let want: HashSet<(String, String, i64)> = [
        ("0xm1".to_owned(), "t1".to_owned(), 1800),
        ("0xm1".to_owned(), "t2".to_owned(), 1800),
        ("0xm2".to_owned(), "t3".to_owned(), 3000),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        got, want,
        "0xm3 (unresolved) excluded; close_ref = end_date ?? resolved_at"
    );

    // Resume: writing one token's series removes only that (market, token) from the next pass.
    cache
        .insert_price_history_batch(&[("0xm1".to_owned(), "t1".to_owned(), 1700, "0.5".to_owned())])
        .unwrap();
    let after: HashSet<(String, String)> = cache
        .price_history_backfill_targets(0)
        .unwrap()
        .iter()
        .map(|t| (t.market_id.clone(), t.token_id.clone()))
        .collect();
    assert!(
        !after.contains(&("0xm1".to_owned(), "t1".to_owned())),
        "a backfilled token must be skipped on the next pass"
    );
    assert!(
        after.contains(&("0xm1".to_owned(), "t2".to_owned())),
        "the sibling token is still pending"
    );
    assert_eq!(after.len(), 2);

    // LIMIT bounds one run's target count.
    assert_eq!(cache.price_history_backfill_targets(1).unwrap().len(), 1);

    println!("PASS: backfill_targets_join_close_ref_and_resume");
}

// PASS: a NULL start_date is reported missing, populated by one update (true), then guarded — a
//       second update returns false (no overwrite); an absent market matches nothing.
// FAIL: the NULL-guard overwrites a populated value, or the missing-set is wrong.
#[test]
fn start_date_update_and_null_guard() {
    let (_dir, mut cache) = open();
    cache.insert_schedule("0xm1", Some(1800), 9).unwrap();
    assert!(cache.market_ids_missing_start_date().contains("0xm1"));

    assert!(
        cache.update_schedule_start_date("0xm1", 1500).unwrap(),
        "first update populates the NULL start_date"
    );
    assert!(
        !cache.market_ids_missing_start_date().contains("0xm1"),
        "row is no longer missing a start_date"
    );

    // Guard: a second update on a now-populated row is a no-op (WHERE start_date_unix IS NULL).
    assert!(
        !cache.update_schedule_start_date("0xm1", 9999).unwrap(),
        "the NULL-guard must block an overwrite"
    );
    // An absent market matches no row.
    assert!(!cache.update_schedule_start_date("0xabsent", 1).unwrap());

    println!("PASS: start_date_update_and_null_guard");
}

// PASS: the createdAt pass-1 universe (decided-outcome markets) excludes voided markets, matching
//       pass-2's price-series target filter — so a voided market's createdAt is never fetched.
// FAIL: a voided (winning_outcome_id NULL) market appears in resolved_market_ids_with_winner.
#[test]
fn resolved_with_winner_excludes_voided() {
    let (_dir, mut cache) = open();
    cache.insert_resolution("0xwin", Some(0), 100, 9).unwrap();
    cache.insert_resolution("0xvoid", None, 200, 9).unwrap(); // voided / non-binary

    let all = cache.resolved_market_ids();
    assert!(
        all.contains("0xwin") && all.contains("0xvoid"),
        "all-resolutions set keeps both"
    );

    let decided = cache.resolved_market_ids_with_winner();
    assert!(decided.contains("0xwin"));
    assert!(
        !decided.contains("0xvoid"),
        "a voided market must be excluded from the decided-outcome set"
    );

    println!("PASS: resolved_with_winner_excludes_voided");
}
