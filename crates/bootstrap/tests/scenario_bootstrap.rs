//! Scenario: seed watchlist filter correctness.
//!
//! PASS: winner wallet (>15 closed trades, >95% win rate, recently active, short avg hold)
//!       appears in the watchlist.
//!       loser wallet (<10% win rate) does NOT appear.
//! FAIL: either condition is wrong, or the pipeline panics.
//!
//! No network calls — fixture JSON files under `tests/fixtures/` are loaded from disk.
//! Clock is fixed via hardcoded `snapshot_at`; RNG is not used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::build_seed_watchlist;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::filter::FilterConfig;
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::{SourceTimestamp, WalletAddress};
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use pe_trader_index::build_trader_ledgers;
use std::collections::HashMap;
use tempfile::TempDir;
use time::OffsetDateTime;

const WINNER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const LOSER_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_URL: &str = "https://data-api.polymarket.com";
// Fixed timestamp well after all fixture trade timestamps (~2023-11-15 + 2 days).
const SNAPSHOT_UNIX: i64 = 1_700_200_000;

fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read(&path).expect("fixture file missing")
}

fn fixture_fetcher() -> FixtureFetcher {
    let winner = WalletAddress::from_hex(WINNER_HEX).unwrap();
    let loser = WalletAddress::from_hex(LOSER_HEX).unwrap();

    let winner_url = PolymarketEndpoint::UserTradeActivityPage {
        user: winner.to_string(),
        end: 2_000_000_000,
        start: Some(1),
        offset: 0,
    }
    .url(BASE_URL);
    let loser_url = PolymarketEndpoint::UserTradeActivityPage {
        user: loser.to_string(),
        end: 2_000_000_000,
        start: Some(1),
        offset: 0,
    }
    .url(BASE_URL);

    let mut responses = HashMap::new();
    responses.insert(winner_url, fixture("polymarket_trades_winner.json"));
    responses.insert(loser_url, fixture("polymarket_trades_loser.json"));
    FixtureFetcher::new(responses)
}

#[tokio::test]
async fn seed_watchlist_passes_winner_and_rejects_loser() {
    let winner = WalletAddress::from_hex(WINNER_HEX).unwrap();
    let loser = WalletAddress::from_hex(LOSER_HEX).unwrap();
    let wallets = vec![winner, loser];

    // Fetch trades using fixture fetcher (no network).
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let fetcher = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fixture_fetcher())
        .with_clock_for_test(|| 2_000_000_120);
    fetcher.fetch_all(&wallets, &mut cache).await.unwrap();

    // Reconstruct ledgers per-wallet (mirrors production rank phase).
    let snapshot_at = SourceTimestamp(OffsetDateTime::from_unix_timestamp(SNAPSHOT_UNIX).unwrap());
    let mut ledgers = Vec::new();
    for wallet in &wallets {
        let trades = cache.trades_for(&wallet.to_string());
        if trades.is_empty() {
            continue;
        }
        ledgers.extend(build_trader_ledgers(&trades, u32::MAX, None));
    }

    // Build seed watchlist with default filter thresholds.
    let watchlist = build_seed_watchlist(ledgers, snapshot_at, &FilterConfig::default());

    // Winner must be in the watchlist.
    let winner_in = watchlist.entries.iter().any(|e| e.wallet == winner);
    assert!(winner_in, "winner wallet should be in the seed watchlist");

    // Loser must NOT be in the watchlist.
    let loser_in = watchlist.entries.iter().any(|e| e.wallet == loser);
    assert!(
        !loser_in,
        "loser wallet should be excluded from the seed watchlist"
    );

    // Sanity: exactly one active entry.
    assert_eq!(watchlist.active_count, 1);
    assert_eq!(watchlist.incubator_count, 0);
}
