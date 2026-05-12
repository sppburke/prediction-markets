#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: CLOB pagination resumes from the persisted cursor (issue #149).
//!
//! `ClobFetcher::fetch_closed_markets` writes
//! `source_cursor.clob_closed` after every successful page so a crash
//! mid-pagination does not waste the next daily run on re-fetching
//! already-processed pages.
//!
//! PASS criterion: with a two-page fixture, the first invocation advances
//! the cursor to page 2's `next_cursor`. A second invocation, starting from
//! that cursor, fetches only page 2 — proven by the fixture's URL
//! coverage: page 1's URL is omitted from the second-run fixture so any
//! attempt to fetch it would Fatal-error.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::clob::ClobFetcher;
use pe_source_polymarket_public::FixtureFetcher;
use tempfile::TempDir;

const BASE_URL: &str = "https://clob.example";

fn page1_url() -> String {
    format!("{BASE_URL}/markets?closed=true&limit=1000")
}

fn page2_url() -> String {
    format!("{BASE_URL}/markets?closed=true&limit=1000&next_cursor=PAGE2")
}

/// Page-1 fixture: 2 markets, advances cursor to "PAGE2".
fn page1_body() -> Vec<u8> {
    br#"{
      "data": [
        {"condition_id": "0xa1", "end_date_iso": "2024-01-15T00:00:00Z", "closed": true, "tokens": [{"winner": true}, {"winner": false}]},
        {"condition_id": "0xa2", "end_date_iso": "2024-02-01T00:00:00Z", "closed": true, "tokens": [{"winner": false}, {"winner": true}]}
      ],
      "next_cursor": "PAGE2"
    }"#.to_vec()
}

/// Page-2 fixture: 2 more markets, terminates with the documented "LTE=" cursor.
fn page2_body() -> Vec<u8> {
    br#"{
      "data": [
        {"condition_id": "0xb1", "end_date_iso": "2024-03-01T00:00:00Z", "closed": true, "tokens": [{"winner": true}, {"winner": false}]},
        {"condition_id": "0xb2", "end_date_iso": "2024-04-01T00:00:00Z", "closed": true, "tokens": [{"winner": false}, {"winner": true}]}
      ],
      "next_cursor": "LTE="
    }"#.to_vec()
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clob_first_run_walks_both_pages_advances_cursor_to_terminator() {
    let (_dir, mut cache) = open_cache();

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
    responses.insert(page1_url(), page1_body());
    responses.insert(page2_url(), page2_body());
    let clob = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let (schedules, resolutions) = clob.fetch_closed_markets(&mut cache).await.unwrap();
    assert_eq!(schedules, 4, "all four markets must insert schedules");
    assert_eq!(
        resolutions, 4,
        "all four markets must insert resolutions (each has a winner)"
    );
    // After the second page, the cursor is the terminator.
    assert_eq!(
        cache.get_source_cursor("clob_closed").as_deref(),
        Some("LTE=")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clob_resume_after_partial_run_skips_page1() {
    // Simulate a crash after page 1: cursor advanced to "PAGE2" but page 1's
    // 2 rows are already in the cache.
    let (_dir, mut cache) = open_cache();
    cache.set_source_cursor("clob_closed", "PAGE2").unwrap();
    cache
        .insert_resolution_with_source("0xa1", Some(0), 1_705_276_800, 1_705_276_800, "clob")
        .unwrap();
    cache
        .insert_resolution_with_source("0xa2", Some(1), 1_706_745_600, 1_706_745_600, "clob")
        .unwrap();
    cache
        .insert_schedule_with_source("0xa1", Some(1_705_276_800), 1_705_276_800, "clob")
        .unwrap();
    cache
        .insert_schedule_with_source("0xa2", Some(1_706_745_600), 1_706_745_600, "clob")
        .unwrap();

    // Resume fixture: page 1's URL is intentionally missing — if the fetcher
    // tried to refetch it, FixtureFetcher would Fatal-error and the assertion
    // below would never be reached.
    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
    responses.insert(page2_url(), page2_body());
    let clob = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let (schedules, resolutions) = clob.fetch_closed_markets(&mut cache).await.unwrap();
    assert_eq!(
        schedules, 2,
        "only page 2's 2 schedules must be newly inserted"
    );
    assert_eq!(resolutions, 2, "only page 2's 2 resolutions");

    // After the resume, the cursor is the terminator and all 4 markets are present.
    assert_eq!(
        cache.get_source_cursor("clob_closed").as_deref(),
        Some("LTE=")
    );
    let all_resolved = cache.resolved_market_ids();
    assert_eq!(all_resolved.len(), 4, "all 4 markets must be resolved now");
    for id in ["0xa1", "0xa2", "0xb1", "0xb2"] {
        assert!(all_resolved.contains(id), "{id} must be in resolved set");
    }
}
