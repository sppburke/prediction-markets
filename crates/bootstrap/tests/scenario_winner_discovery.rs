#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: all-category leaderboard discovery (issue #335).
//!
//! Exercises `leaderboard_discovery::run_leaderboard_discovery` — the pile-ingest
//! path that `winner_discovery::run_winner_discovery` drives — against a
//! fixture-backed fetcher. Deterministic; no network. (`run_winner_discovery`
//! itself builds a non-injectable `ReqwestFetcher`, so the matrix/dedup contract
//! is verified one layer down, where a `FixtureFetcher` can be supplied.)

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::leaderboard_discovery::{
    LeaderboardFetcher, SORT_WINDOW_SLICES, run_leaderboard_discovery,
};
use pe_source_polymarket_public::endpoint::LeaderboardCategory;
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use tempfile::TempDir;

const BASE: &str = "https://data-api.polymarket.com";

/// Build a fixture map: every `(category × slice)` URL → the same JSON body.
fn fixtures(
    categories: &[LeaderboardCategory],
    wallets: &[&str],
    top_n: u32,
) -> HashMap<String, Vec<u8>> {
    let body: Vec<serde_json::Value> = wallets
        .iter()
        .map(|w| serde_json::json!({ "proxyWallet": w }))
        .collect();
    let bytes = serde_json::to_vec(&body).unwrap();
    let mut map = HashMap::new();
    for &c in categories {
        for (sort, window) in SORT_WINDOW_SLICES {
            let ep = PolymarketEndpoint::Leaderboard {
                sort,
                window,
                category: c,
                limit: top_n,
            };
            map.insert(ep.url(BASE), bytes.clone());
        }
    }
    map
}

// PASS: the full category × {PNL,VOL} × {DAY,WEEK,MONTH,ALL} matrix is swept
//       (slices_attempted = 10 categories × 8 = 80) and the two wallets present
//       in every slice are deduplicated to a single upsert each, then activated
//       by the leaderboard rule.
// FAIL: fewer slices attempted/fetched, duplicate rows survive, or no activation.
#[tokio::test]
async fn all_category_matrix_is_swept_and_deduped() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    let categories = LeaderboardCategory::ALL;
    let wallets = [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ];
    let responses = fixtures(&categories, &wallets, 50);

    let lb = LeaderboardFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses));
    let report = run_leaderboard_discovery(&lb, &categories, 50, &mut cache)
        .await
        .unwrap();

    let expected_slices = categories.len() * SORT_WINDOW_SLICES.len();
    let pass = report.slices_attempted == expected_slices
        && report.slices_fetched == expected_slices
        && report.unique_wallets == 2
        && report.activated == 2;
    println!(
        "{}: all_category_matrix_is_swept_and_deduped \
         (attempted={}, fetched={}, unique={}, activated={})",
        if pass { "PASS" } else { "FAIL" },
        report.slices_attempted,
        report.slices_fetched,
        report.unique_wallets,
        report.activated,
    );
    assert!(
        pass,
        "expected {expected_slices} slices, 2 unique wallets, 2 activated; got {report:?}"
    );
}
