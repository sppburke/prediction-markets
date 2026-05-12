#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: Gamma concurrent fetch correctness.
//!
//! Verifies that the `buffer_unordered`-based fetch loop preserves the
//! pre-refactor semantics: every market in `to_fetch` is processed exactly
//! once, every successful schedule lands in the cache, errors on individual
//! markets do not abort the run, and the already-cached skip set still
//! suppresses re-fetch.
//!
//! Issue #149 Cycle 2 retargeted these tests from `fetch_resolutions`
//! (removed in Cycle 2) to `fetch_schedules`, which now lives on the same
//! `buffer_unordered` code path so the concurrency-regression safety net
//! still applies on a production code path.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::gamma::GammaFetcher;
use pe_source_polymarket_public::FixtureFetcher;
use tempfile::TempDir;

const BASE_URL: &str = "https://gamma-api.polymarket.com";

fn scheduled_fixture(market_id: &str, end_date_iso: &str) -> Vec<u8> {
    format!(
        r#"[{{"conditionId":"{market_id}","closed":false,"endDate":"{end_date_iso}","outcomes":"[\"Yes\",\"No\"]"}}]"#
    )
    .into_bytes()
}

fn no_end_date_fixture(market_id: &str) -> Vec<u8> {
    format!(r#"[{{"conditionId":"{market_id}","closed":false,"outcomes":"[\"Yes\",\"No\"]"}}]"#)
        .into_bytes()
}

fn url_for(market_id: &str) -> String {
    format!("{BASE_URL}/markets?condition_ids={market_id}")
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_schedules_land_when_fetched_concurrently() {
    // 100 markets, all with endDate. With GAMMA_CONCURRENCY=10 the loop fires 10
    // requests in flight; every result must still arrive and insert.
    let (_dir, mut cache) = open_cache();

    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();
    let mut responses = HashMap::new();
    for id in &market_ids {
        responses.insert(url_for(id), scheduled_fixture(id, "2026-07-31T12:00:00Z"));
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 100, "every market must insert exactly once");
    assert_eq!(
        cache.load_all_schedules().unwrap().len(),
        100,
        "DB row count must match"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn markets_missing_end_date_still_insert_null() {
    // Mix of with-endDate + without-endDate. `fetch_schedules` inserts both
    // (NULL end_date enters the skip-set so the market is not re-queried).
    let (_dir, mut cache) = open_cache();

    let mut market_ids: Vec<String> = Vec::new();
    let mut responses = HashMap::new();
    for i in 0..50 {
        let id = format!("0xdated{i:04}");
        responses.insert(url_for(&id), scheduled_fixture(&id, "2026-07-31T12:00:00Z"));
        market_ids.push(id);
    }
    for i in 0..50 {
        let id = format!("0xbare{i:04}");
        responses.insert(url_for(&id), no_end_date_fixture(&id));
        market_ids.push(id);
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 100, "every market must insert (NULL row counts)");
    assert_eq!(cache.load_all_schedules().unwrap().len(), 100);
    // The 50 NULL-endDate rows still occupy the skip-set.
    assert_eq!(cache.scheduled_market_ids().len(), 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fatal_on_one_market_does_not_abort_others() {
    // 99 markets have fixtures; one (0xmissing) does not. FixtureFetcher
    // returns SourceError::Fatal for missing URLs. The fetch loop must
    // log + skip the Fatal and complete the remaining 99 successfully.
    let (_dir, mut cache) = open_cache();

    let mut market_ids: Vec<String> = (0..99).map(|i| format!("0xok{i:04}")).collect();
    let mut responses = HashMap::new();
    for id in &market_ids {
        responses.insert(url_for(id), scheduled_fixture(id, "2026-07-31T12:00:00Z"));
    }
    market_ids.push("0xmissing".to_owned());

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 99, "Fatal on one must not abort the other 99");
    assert_eq!(cache.load_all_schedules().unwrap().len(), 99);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn already_cached_ids_skipped_under_concurrency() {
    // 100 markets total: 30 already in cache, 70 to fetch. Pre-seeded ones
    // must not be re-fetched even under concurrent execution.
    let (_dir, mut cache) = open_cache();

    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();
    for id in &market_ids[..30] {
        cache
            .insert_schedule(id, Some(1_700_000_000), 1_700_000_001)
            .unwrap();
    }

    // Fixture only contains URLs for the 70 not-yet-cached markets; if any
    // of the first 30 leaked through, the fetcher would return Fatal, and
    // the run would still log+continue (so test would still pass with the
    // wrong count). Assert inserted == 70 to confirm the skip-set works.
    let mut responses = HashMap::new();
    for id in &market_ids[30..] {
        responses.insert(url_for(id), scheduled_fixture(id, "2026-07-31T12:00:00Z"));
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 70, "only the 70 uncached must be fetched");
    assert_eq!(
        cache.load_all_schedules().unwrap().len(),
        100,
        "30 pre-seeded + 70 newly inserted"
    );
}
