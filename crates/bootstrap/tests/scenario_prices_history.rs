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
                ("t1".to_owned(), "0xm1".to_owned(), 0),
                ("t2".to_owned(), "0xm1".to_owned(), 1),
                ("t3".to_owned(), "0xm2".to_owned(), 0),
                ("t9".to_owned(), "0xm3".to_owned(), 0),
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
        .insert_price_history_batch(
            &[("0xm1".to_owned(), "t1".to_owned(), 1700, "0.5".to_owned())],
            "clob",
        )
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

// PASS: price_series_coverage_report() counts (total, with_series, usable) over the
//       resolved-with-winner universe — `usable` needs ≥3 CLOB points on a token — a voided market is
//       excluded, a same-PK re-insert (different source) is a no-op (write-once), a genuinely new 3rd
//       point flips a thin market to usable, and a trades-sourced series is excluded (CLOB-only).
// FAIL: a voided/trades market is counted, the usable bar is wrong, a same-PK re-insert moves the
//       ledger, or the ledger is stale after a new point.
#[test]
fn price_series_coverage_ledger_and_write_once() {
    let (_dir, mut cache) = open();
    // Two resolved-with-winner markets + one voided (excluded from the denominator).
    cache.insert_resolution("0xm1", Some(0), 2000, 9).unwrap();
    cache.insert_resolution("0xm2", Some(1), 2000, 9).unwrap();
    cache.insert_resolution("0xvoid", None, 2000, 9).unwrap();

    // 0xm1/t1: 3 points → usable. 0xm2/t2: 2 points → with_series, NOT usable.
    cache
        .insert_price_history_batch(
            &[
                ("0xm1".to_owned(), "t1".to_owned(), 100, "0.40".to_owned()),
                ("0xm1".to_owned(), "t1".to_owned(), 200, "0.50".to_owned()),
                ("0xm1".to_owned(), "t1".to_owned(), 300, "0.60".to_owned()),
                ("0xm2".to_owned(), "t2".to_owned(), 100, "0.70".to_owned()),
                ("0xm2".to_owned(), "t2".to_owned(), 200, "0.80".to_owned()),
            ],
            "clob",
        )
        .unwrap();
    let cov = cache.price_series_coverage_report();
    assert_eq!(
        (cov.total, cov.with_series, cov.usable),
        (2, 2, 1),
        "0xvoid excluded; 0xm1 usable (3 pts), 0xm2 has a series but <3 pts"
    );

    // Write-once: a same-PK re-insert with a different price AND source adds no row → ledger stable.
    cache
        .insert_price_history_batch(
            &[("0xm1".to_owned(), "t1".to_owned(), 100, "0.99".to_owned())],
            "trades",
        )
        .unwrap();
    let cov2 = cache.price_series_coverage_report();
    assert_eq!(
        (cov2.total, cov2.with_series, cov2.usable),
        (2, 2, 1),
        "a same-PK re-insert must not change the ledger (write-once)"
    );

    // A genuinely new 3rd point flips the thin market to usable.
    cache
        .insert_price_history_batch(
            &[("0xm2".to_owned(), "t2".to_owned(), 300, "0.85".to_owned())],
            "clob",
        )
        .unwrap();
    let cov3 = cache.price_series_coverage_report();
    assert_eq!(
        (cov3.total, cov3.with_series, cov3.usable),
        (2, 2, 2),
        "a real new point flips 0xm2 to usable"
    );

    // A non-CLOB (trades) series is excluded from the CLOB-specific ledger: 0xm3 is a resolved
    // winner with a fresh 3-point `source='trades'` series → `total` rises to 3 but with_series /
    // usable stay 2, because the report filters `source='clob'`.
    cache.insert_resolution("0xm3", Some(0), 2000, 9).unwrap();
    cache
        .insert_price_history_batch(
            &[
                ("0xm3".to_owned(), "t3".to_owned(), 100, "0.10".to_owned()),
                ("0xm3".to_owned(), "t3".to_owned(), 200, "0.20".to_owned()),
                ("0xm3".to_owned(), "t3".to_owned(), 300, "0.30".to_owned()),
            ],
            "trades",
        )
        .unwrap();
    let cov4 = cache.price_series_coverage_report();
    assert_eq!(
        (cov4.total, cov4.with_series, cov4.usable),
        (3, 2, 2),
        "a trades-sourced series is excluded from the CLOB coverage ledger"
    );

    println!("PASS: price_series_coverage_ledger_and_write_once");
}
