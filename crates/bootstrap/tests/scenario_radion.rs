#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: Radion `traders/analysis` cursor-resume discovery against the real
//! captured contract (issue #373).
//!
//! Loads the committed fixtures captured from the live `api.radion.app` contract
//! on 2026-06-18 (`tests/fixtures/radion_traders_analysis_page{1,2}.json`),
//! deserializes them through the *production* parser (`parse_traders_analysis`) so
//! any drift in the `{data:[{traderId}], nextCursor}` shape fails loudly here,
//! then drives the full `run_radion_discovery` resumable sweep through a
//! `FixtureTraderFetcher` against a temp cache. Deterministic; no network.

use std::collections::{HashMap, HashSet};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::pile::SRC_RADION;
use pe_bootstrap::radion::{
    CursorPageTraderAnalysis, FixtureTraderFetcher, RADION_CURSOR_KEY, parse_traders_analysis,
    run_radion_discovery,
};
use tempfile::TempDir;

const PAGE1: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/radion_traders_analysis_page1.json"
));
const PAGE2: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/radion_traders_analysis_page2.json"
));

fn tmp_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

/// Build the cursor→page map from the two captured fixtures: the start page (`""`)
/// is page1, and page1's `nextCursor` keys page2.
fn captured_pages() -> (HashMap<String, CursorPageTraderAnalysis>, String) {
    let page1 = parse_traders_analysis(PAGE1).unwrap();
    let page2 = parse_traders_analysis(PAGE2).unwrap();
    assert_eq!(page1.data.len(), 10, "captured page1 has 10 traders");
    assert_eq!(page2.data.len(), 3, "captured page2 has 3 traders");
    assert!(
        page2.next_cursor.is_none(),
        "captured page2 is the last page"
    );
    let cursor1 = page1
        .next_cursor
        .clone()
        .expect("captured page1 has a next cursor");
    let pages = HashMap::from([(String::new(), page1), (cursor1.clone(), page2)]);
    (pages, cursor1)
}

// PASS: a budget-1 run-1 collects page1's 10 wallets and persists page1's
//       nextCursor; run-2 resumes from that cursor (fetching ONLY page2, not
//       page1 again), exhausts (nextCursor=null) and resets the cursor to "";
//       cross-run dedup leaves exactly 12 distinct SRC_RADION wallets, all active
//       (the mixed-case page2 address is lowercased; the shared 0x9999 dedups).
// FAIL: run-2 re-fetches page1, the cursor is not persisted/reset, a duplicate row
//       appears, or a wallet is not activated.
#[tokio::test]
async fn resume_exhaust_dedup_activate_end_to_end() {
    let (_d, mut cache) = tmp_cache();
    let (pages, cursor1) = captured_pages();

    // ── Run 1: budget of 1 request → fetches page1 only, then stops on budget. ──
    let fetcher1 = FixtureTraderFetcher::new(pages.clone());
    let r1 = run_radion_discovery(&fetcher1, 1, &mut cache)
        .await
        .unwrap();
    let cursor_after_1 = cache.get_source_cursor(RADION_CURSOR_KEY);

    // ── Run 2: full budget → resumes at cursor1, fetches page2, exhausts. ──
    let fetcher2 = FixtureTraderFetcher::new(pages);
    let r2 = run_radion_discovery(&fetcher2, 8, &mut cache)
        .await
        .unwrap();
    let cursor_after_2 = cache.get_source_cursor(RADION_CURSOR_KEY);

    let distinct = cache.wallets_with_source_bit(SRC_RADION).unwrap().len();
    let active = cache.active_wallet_count().unwrap();

    let pass = r1.unique_wallets == 10
        && r1.activated == 10
        && fetcher1.call_count() == 1
        && cursor_after_1.as_deref() == Some(cursor1.as_str())
        && r2.unique_wallets == 3
        && r2.activated == 2 // 0x9999 already active from run 1; +2 new
        && fetcher2.call_count() == 1 // resumed: page2 only, no page1 re-fetch
        && cursor_after_2.as_deref() == Some("") // exhausted → reset
        && distinct == 12
        && active == 12;
    println!(
        "{}: resume_exhaust_dedup_activate_end_to_end \
         (r1.unique={}, r1.act={}, f1.calls={}, cur1={:?}, r2.unique={}, r2.act={}, \
          f2.calls={}, cur2={:?}, distinct={}, active={})",
        if pass { "PASS" } else { "FAIL" },
        r1.unique_wallets,
        r1.activated,
        fetcher1.call_count(),
        cursor_after_1,
        r2.unique_wallets,
        r2.activated,
        fetcher2.call_count(),
        cursor_after_2,
        distinct,
        active,
    );
    assert!(
        pass,
        "resume/exhaust/dedup/activate mismatch: r1={r1:?} cur1={cursor_after_1:?} \
         r2={r2:?} cur2={cursor_after_2:?} distinct={distinct} active={active}"
    );
}

// PASS: page1 succeeds, the resume cursor (page1's nextCursor) then fails (models a
//       429); the run persists that cursor and returns Err(Radion) WITHOUT upserting
//       this run's partial collection (0 SRC_RADION rows in the cache).
// FAIL: the run blocks/sleeps, succeeds, upserts the partial set, or loses the cursor.
#[tokio::test]
async fn error_persists_cursor_and_bails() {
    let (_d, mut cache) = tmp_cache();
    let (pages, cursor1) = captured_pages();

    // page1 (cursor "") succeeds; the resume cursor (cursor1) errors.
    let fail_keys = HashSet::from([cursor1.clone()]);
    let fetcher = FixtureTraderFetcher::with_failures(pages, fail_keys);

    let r = run_radion_discovery(&fetcher, 8, &mut cache).await;
    let cursor_after = cache.get_source_cursor(RADION_CURSOR_KEY);
    let distinct = cache.wallets_with_source_bit(SRC_RADION).unwrap().len();

    let pass = matches!(r, Err(pe_bootstrap::error::BootstrapError::Radion { .. }))
        && cursor_after.as_deref() == Some(cursor1.as_str())
        && fetcher.call_count() == 2 // page1 ok, cursor1 fails
        && distinct == 0; // partial collection discarded, not upserted
    println!(
        "{}: error_persists_cursor_and_bails (err={}, cur={:?}, calls={}, distinct={})",
        if pass { "PASS" } else { "FAIL" },
        r.is_err(),
        cursor_after,
        fetcher.call_count(),
        distinct,
    );
    assert!(
        pass,
        "expected Err(Radion), cursor={cursor1:?}, 2 calls, 0 rows; \
         got err={} cur={cursor_after:?} calls={} distinct={distinct}",
        r.is_err(),
        fetcher.call_count(),
    );
}
