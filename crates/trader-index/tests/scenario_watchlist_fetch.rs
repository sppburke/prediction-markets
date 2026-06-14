//! Scenario: [`WatchlistFetcher`] end-to-end fetch → [`Watchlist`] conversion.
//!
//! Gated behind the `scenario` cargo feature — runs automatically when
//! `cargo nextest run --all-features` is used; excluded from quick local
//! iteration that omits `--all-features`.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_core_types::{BasisPoints, WalletAddress};
use pe_source_polymarket_public::{
    FixtureFetcher, LeaderboardCategory, LeaderboardSort, LeaderboardWindow, PolymarketEndpoint,
};
use pe_trader_index::{WatchlistFetchConfig, WatchlistFetcher, WatchlistTier};

fn addr(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).expect("test address")
}

fn make_fetcher(fixture_bytes: Vec<u8>) -> WatchlistFetcher<FixtureFetcher> {
    let base = "https://data-api.polymarket.com";
    let url = PolymarketEndpoint::Leaderboard {
        sort: LeaderboardSort::Profit,
        window: LeaderboardWindow::AllTime,
        category: LeaderboardCategory::Overall,
        limit: 5,
    }
    .url(base);
    let mut responses = HashMap::new();
    responses.insert(url, fixture_bytes);
    let ff = FixtureFetcher::new(responses);
    let config = WatchlistFetchConfig {
        base_url: base.to_owned(),
        watchlist_size: 5,
    };
    WatchlistFetcher::new(config, ff)
}

/// Scenario: leaderboard_top5.json → Watchlist with 5 Active entries.
///
/// PASS: 5 Active entries in leaderboard order, rank-inverted scores,
///       first entry has wallet 0xaaa..., score 500 bps.
/// FAIL: wrong count, wrong wallet order, or wrong score.
#[tokio::test]
async fn scenario_top5_leaderboard_produces_watchlist() {
    // leaderboard_top5.json: 5 entries, alpha_trader rank 1 = 0xaaa...
    let fixture = include_bytes!("fixtures/leaderboard_top5.json").to_vec();
    let mut fetcher = make_fetcher(fixture);

    let watchlist = fetcher
        .fetch_watchlist()
        .await
        .expect("fetch should succeed");

    assert_eq!(watchlist.entries.len(), 5, "expected 5 entries");
    assert_eq!(watchlist.active_count, 5);
    assert_eq!(watchlist.incubator_count, 0);

    // All entries are Active tier.
    for entry in &watchlist.entries {
        assert_eq!(entry.tier, WatchlistTier::Active);
    }

    // Rank-inverted scores: rank 1 → 5×100=500, rank 5 → 1×100=100.
    assert_eq!(
        watchlist.entries[0].wallet,
        addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "first entry should be alpha_trader"
    );
    assert_eq!(
        watchlist.entries[0].leader_score_bps,
        BasisPoints(500),
        "rank 1 score should be 500 bps"
    );

    assert_eq!(
        watchlist.entries[4].wallet,
        addr("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        "last entry should be epsilon_trader"
    );
    assert_eq!(
        watchlist.entries[4].leader_score_bps,
        BasisPoints(100),
        "rank 5 score should be 100 bps"
    );
}

/// Scenario: watchlist_size cap is respected when leaderboard returns more entries.
///
/// PASS: fetcher with watchlist_size=3 returns exactly 3 entries from a 5-entry fixture.
/// FAIL: more than 3 entries returned.
#[tokio::test]
async fn scenario_watchlist_size_cap() {
    let fixture = include_bytes!("fixtures/leaderboard_top5.json").to_vec();
    let url = PolymarketEndpoint::Leaderboard {
        sort: LeaderboardSort::Profit,
        window: LeaderboardWindow::AllTime,
        category: LeaderboardCategory::Overall,
        limit: 3,
    }
    .url("https://data-api.polymarket.com");
    let mut responses = HashMap::new();
    responses.insert(url, fixture);
    let ff = FixtureFetcher::new(responses);
    let config = WatchlistFetchConfig {
        base_url: "https://data-api.polymarket.com".to_owned(),
        watchlist_size: 3,
    };
    let mut fetcher = WatchlistFetcher::new(config, ff);

    let watchlist = fetcher
        .fetch_watchlist()
        .await
        .expect("fetch should succeed");

    assert_eq!(
        watchlist.entries.len(),
        3,
        "size cap should limit to 3 entries"
    );
    assert_eq!(watchlist.active_count, 3);
    // Scores: 3×100=300, 2×100=200, 1×100=100.
    assert_eq!(watchlist.entries[0].leader_score_bps, BasisPoints(300));
    assert_eq!(watchlist.entries[2].leader_score_bps, BasisPoints(100));
}

/// Scenario: invalid JSON in response returns a Parse error.
///
/// PASS: fetch_watchlist returns Err(WatchlistFetchError::Parse { .. }).
/// FAIL: call succeeds or returns a different error variant.
#[tokio::test]
async fn scenario_parse_error_on_bad_json() {
    use pe_trader_index::WatchlistFetchError;

    let bad_bytes = b"not valid json at all".to_vec();
    let url = PolymarketEndpoint::Leaderboard {
        sort: LeaderboardSort::Profit,
        window: LeaderboardWindow::AllTime,
        category: LeaderboardCategory::Overall,
        limit: 20,
    }
    .url("https://data-api.polymarket.com");
    let mut responses = HashMap::new();
    responses.insert(url, bad_bytes);
    let ff = FixtureFetcher::new(responses);
    let config = WatchlistFetchConfig::default();
    let mut fetcher = WatchlistFetcher::new(config, ff);

    let result = fetcher.fetch_watchlist().await;
    assert!(
        matches!(result, Err(WatchlistFetchError::Parse { .. })),
        "expected Parse error, got: {result:?}"
    );
}
