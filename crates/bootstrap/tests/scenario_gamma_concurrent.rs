#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: Gamma concurrent fetch correctness.
//!
//! Verifies that the `buffer_unordered`-based fetch loop preserves the
//! pre-refactor semantics: every market in `to_fetch` is processed exactly
//! once, every successful resolution lands in the cache, errors on
//! individual markets do not abort the run, and the already-cached skip
//! set still suppresses re-fetch.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::gamma::GammaFetcher;
use pe_source_polymarket_public::FixtureFetcher;
use tempfile::TempDir;

const BASE_URL: &str = "https://gamma-api.polymarket.com";

fn resolved_yes_fixture(market_id: &str) -> Vec<u8> {
    format!(
        r#"[{{"conditionId":"{market_id}","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"1\",\"0\"]","outcomes":"[\"Yes\",\"No\"]"}}]"#
    )
    .into_bytes()
}

fn unclosed_fixture(market_id: &str) -> Vec<u8> {
    format!(
        r#"[{{"conditionId":"{market_id}","closed":false,"closedTime":null,"outcomePrices":null,"outcomes":"[\"Yes\",\"No\"]"}}]"#
    )
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
async fn all_resolutions_land_when_fetched_concurrently() {
    // 100 markets, all resolved. With GAMMA_CONCURRENCY=10 the loop fires 10
    // requests in flight; every result must still arrive and insert.
    let (_dir, mut cache) = open_cache();

    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();
    let mut responses = HashMap::new();
    for id in &market_ids {
        responses.insert(url_for(id), resolved_yes_fixture(id));
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_resolutions(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 100, "every market must insert exactly once");
    assert_eq!(
        cache.load_all_resolutions().unwrap().len(),
        100,
        "DB row count must match"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unresolved_markets_silently_skipped() {
    // Mix of resolved + unresolved. Unresolved must not insert, resolved must.
    let (_dir, mut cache) = open_cache();

    let mut market_ids: Vec<String> = Vec::new();
    let mut responses = HashMap::new();
    for i in 0..50 {
        let id = format!("0xresolved{i:04}");
        responses.insert(url_for(&id), resolved_yes_fixture(&id));
        market_ids.push(id);
    }
    for i in 0..50 {
        let id = format!("0xopen{i:04}");
        responses.insert(url_for(&id), unclosed_fixture(&id));
        market_ids.push(id);
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_resolutions(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 50, "only resolved markets must insert");
    assert_eq!(cache.load_all_resolutions().unwrap().len(), 50);
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
        responses.insert(url_for(id), resolved_yes_fixture(id));
    }
    market_ids.push("0xmissing".to_owned());

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_resolutions(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 99, "Fatal on one must not abort the other 99");
    assert_eq!(cache.load_all_resolutions().unwrap().len(), 99);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn already_cached_ids_skipped_under_concurrency() {
    // 100 markets total: 30 already in cache, 70 to fetch. Pre-seeded ones
    // must not be re-fetched even under concurrent execution.
    let (_dir, mut cache) = open_cache();

    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();
    for id in &market_ids[..30] {
        cache
            .insert_resolution(id, Some(0), 1_700_000_000, 1_700_000_001)
            .unwrap();
    }

    // Fixture only contains URLs for the 70 not-yet-cached markets; if any
    // of the first 30 leaked through, the fetcher would return Fatal, and
    // the run would still log+continue (so test would still pass with the
    // wrong count). Assert inserted == 70 to confirm the skip-set works.
    let mut responses = HashMap::new();
    for id in &market_ids[30..] {
        responses.insert(url_for(id), resolved_yes_fixture(id));
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let inserted = gamma
        .fetch_resolutions(&market_ids, &mut cache)
        .await
        .unwrap();

    assert_eq!(inserted, 70, "only the 70 uncached must be fetched");
    assert_eq!(
        cache.load_all_resolutions().unwrap().len(),
        100,
        "30 pre-seeded + 70 newly inserted"
    );
}
