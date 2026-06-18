//! Scenario: CLOB is the sole, primary market-resolution source (issue #369).
//!
//! After the Polygon/Alchemy on-chain scan was removed, `ClobFetcher::
//! fetch_closed_markets` is the only path that writes `market_resolutions` rows
//! for newly-resolved markets. This scenario proves that a closed CLOB market
//! with a single winner produces a `market_resolutions` row tagged
//! `source='clob'` carrying the positional winner index — i.e. CLOB resolutions
//! are authoritative for new markets.
//!
//! Deterministic: an in-process fixture page, no live network.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::clob::ClobFetcher;
use pe_source_polymarket_public::FixtureFetcher;
use tempfile::TempDir;

const BASE_URL: &str = "https://clob.example";

fn page_url() -> String {
    format!("{BASE_URL}/markets?closed=true&limit=1000")
}

/// One closed binary market whose first token is the winner (winner_index 0);
/// `next_cursor: "LTE="` terminates pagination after this single page.
fn page_body() -> Vec<u8> {
    br#"{
      "data": [
        {"condition_id": "0xclob1", "end_date_iso": "2026-01-15T00:00:00Z", "closed": true, "tokens": [{"winner": true}, {"winner": false}]}
      ],
      "next_cursor": "LTE="
    }"#
    .to_vec()
}

/// PASS: fetching the CLOB closed-markets page returns `Ok` and writes one
/// `market_resolutions` row tagged `source='clob'` whose `winning_outcome_id`
/// equals the positional index of the single winner token (0).
/// FAIL: the fetch errors, no row is written, or the row's source/winner is wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clob_closed_market_writes_clob_sourced_resolution() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
    responses.insert(page_url(), page_body());
    let clob = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let (schedules, resolutions) = clob
        .fetch_closed_markets(&mut cache)
        .await
        .expect("CLOB closed-markets fetch must succeed on the fixture page");
    assert_eq!(
        resolutions, 1,
        "the single winning market must insert one resolution"
    );
    assert_eq!(
        schedules, 1,
        "the closed market must also insert one schedule"
    );

    let (winner, _resolved_at, _fetched, source) = cache
        .resolution_record("0xclob1")
        .expect("the CLOB market must have a market_resolutions row");
    assert_eq!(
        source, "clob",
        "the resolution must be tagged source='clob' (CLOB is the sole source)"
    );
    assert_eq!(
        winner,
        Some(0),
        "winning_outcome_id must be the positional index of the winner token"
    );
    println!("PASS: CLOB closed market ⇒ source='clob' resolution row, winner_index=0");
}
