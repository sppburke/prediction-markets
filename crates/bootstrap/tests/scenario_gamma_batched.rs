#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: Gamma batched fetch correctness (issue #382).
//!
//! End-to-end over the production `GammaFetcher::fetch_schedules` path now that it delegates to the
//! shared batched `GammaMarketsClient` (repeat-key `condition_ids=` batching, 50/request). Verifies
//! the multi-batch wiring the per-loop unit tests (single batch) don't reach:
//! - every market across multiple batches lands exactly once in the cache;
//! - markets Gamma omits from a 200 batch (unknown markets) get a NULL schedule row and enter the
//!   skip-set — the batched analogue of the old per-ID empty-response case;
//! - the already-cached skip-set still suppresses re-fetch.
//!
//! Per-chunk fatal-skip and demux integrity are covered at the client level in
//! `pe-source-polymarket-public`'s `tests/scenario_gamma_markets.rs`.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::gamma::GammaFetcher;
use pe_source_polymarket_public::{FixtureFetcher, GAMMA_BATCH_SIZE};
use tempfile::TempDir;

const BASE_URL: &str = "https://gamma-api.polymarket.com";

/// Replicates `GammaMarketsClient`'s open-variant batch URL for an in-order chunk of ids.
fn batch_url(ids: &[String]) -> String {
    let mut url = format!("{BASE_URL}/markets?");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            url.push('&');
        }
        url.push_str("condition_ids=");
        url.push_str(id);
    }
    url.push_str("&limit=500");
    url
}

/// Build a `/markets` array response for `members` (id, optional endDate). `None` endDate → the
/// market is present but carries no `endDate`; omit a member entirely to simulate Gamma not listing
/// it (the unknown-market case).
fn array_response(members: &[(&str, Option<&str>)]) -> Vec<u8> {
    let items: Vec<String> = members
        .iter()
        .map(|(id, end)| match end {
            Some(e) => format!(
                r#"{{"conditionId":"{id}","closed":false,"endDate":"{e}","outcomes":"[\"Yes\",\"No\"]"}}"#
            ),
            None => format!(r#"{{"conditionId":"{id}","closed":false,"outcomes":"[\"Yes\",\"No\"]"}}"#),
        })
        .collect();
    format!("[{}]", items.join(",")).into_bytes()
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

/// Map the exact batch URLs the client will request for `to_fetch` (chunked by `GAMMA_BATCH_SIZE`,
/// input order), each returning the array produced by `member_of` for that chunk's ids.
fn responses_for(
    to_fetch: &[String],
    member_of: impl Fn(&str) -> Option<Option<String>>,
) -> HashMap<String, Vec<u8>> {
    let mut responses = HashMap::new();
    for chunk in to_fetch.chunks(GAMMA_BATCH_SIZE) {
        // (id, endDate?) for the ids Gamma returns in this chunk (filter_map drops omitted ids).
        let owned: Vec<(String, Option<String>)> = chunk
            .iter()
            .filter_map(|id| member_of(id).map(|end| (id.clone(), end)))
            .collect();
        let borrowed: Vec<(&str, Option<&str>)> = owned
            .iter()
            .map(|(id, end)| (id.as_str(), end.as_deref()))
            .collect();
        responses.insert(batch_url(chunk), array_response(&borrowed));
    }
    responses
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_schedules_land_across_multiple_batches() {
    // 100 markets → 2 batches of 50. Every market has an endDate; all must insert exactly once.
    let (_dir, mut cache) = open_cache();
    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();

    let responses = responses_for(&market_ids, |_| {
        Some(Some("2026-07-31T12:00:00Z".to_owned()))
    });
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();
    assert_eq!(
        inserted, 100,
        "every market across both batches must insert exactly once"
    );
    assert_eq!(
        cache.load_all_schedules().unwrap().len(),
        100,
        "DB row count must match"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn markets_missing_end_date_still_insert_null() {
    // Mix of with-endDate and without across 2 batches. Both insert (NULL enters the skip-set).
    let (_dir, mut cache) = open_cache();
    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();

    let responses = responses_for(&market_ids, |id| {
        // Even-indexed markets carry an endDate; odd ones don't.
        let dated = id.trim_start_matches("0xcond").parse::<u32>().unwrap_or(0) % 2 == 0;
        Some(dated.then(|| "2026-07-31T12:00:00Z".to_owned()))
    });
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();
    assert_eq!(inserted, 100, "every market must insert (NULL row counts)");
    assert_eq!(cache.load_all_schedules().unwrap().len(), 100);
    assert_eq!(
        cache.scheduled_market_ids().len(),
        100,
        "NULL rows still occupy the skip-set"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_ids_omitted_from_batch_get_null_rows() {
    // Gamma does not list 10 of the 100 markets → they are omitted from the 200 batch arrays. The
    // batched path inserts a NULL row for each (the analogue of the old per-ID empty-response case),
    // so they enter the skip-set and are not re-fetched.
    let (_dir, mut cache) = open_cache();
    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();

    let responses = responses_for(&market_ids, |id| {
        let n = id.trim_start_matches("0xcond").parse::<u32>().unwrap_or(0);
        if n < 10 {
            None // Gamma omits this id from the array entirely
        } else {
            Some(Some("2026-07-31T12:00:00Z".to_owned()))
        }
    });
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let inserted = gamma
        .fetch_schedules(&market_ids, &mut cache)
        .await
        .unwrap();
    assert_eq!(
        inserted, 100,
        "every fetched id gets a row (NULL for the 10 Gamma omitted)"
    );
    assert_eq!(cache.load_all_schedules().unwrap().len(), 100);
    assert_eq!(
        cache.scheduled_market_ids().len(),
        100,
        "omitted ids are in the skip-set"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn already_cached_ids_skipped_under_batching() {
    // 100 markets: 30 pre-cached, 70 to fetch. Only the 70 uncached are batched.
    let (_dir, mut cache) = open_cache();
    let market_ids: Vec<String> = (0..100).map(|i| format!("0xcond{i:04}")).collect();
    for id in &market_ids[..30] {
        cache
            .insert_schedule(id, Some(1_700_000_000), 1_700_000_001)
            .unwrap();
    }

    // The client fetches market_ids[30..] in order; map exactly those batch URLs.
    let to_fetch: Vec<String> = market_ids[30..].to_vec();
    let responses = responses_for(&to_fetch, |_| Some(Some("2026-07-31T12:00:00Z".to_owned())));
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

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
