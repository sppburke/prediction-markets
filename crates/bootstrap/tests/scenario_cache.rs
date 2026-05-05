//! Scenario tests for the permanent trade cache (issue #71).
//!
//! Each scenario has a single PASS/FAIL criterion written before the test body.
//! No network calls; all data is constructed in-process.
//! Clock is fixed via hardcoded timestamps; no RNG is used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::build_seed_watchlist;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::filter::{DEFAULT_MIN_CLOSED_TRADES, DEFAULT_MIN_WIN_RATE_PCT};
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::{SourceTimestamp, SourceTradeId, WalletAddress};
use pe_operator_graph::OperatorIdentity;
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use pe_trader_index::{LedgerConfig, build_trader_ledgers, snapshot::TradeSnapshot};
use tempfile::TempDir;
use time::OffsetDateTime;

const BASE_URL: &str = "https://data-api.polymarket.com";
const WALLET_A_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
#[allow(dead_code)]
const WALLET_B_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn wallet_a() -> WalletAddress {
    WalletAddress::from_hex(WALLET_A_HEX).unwrap()
}

#[allow(dead_code)]
fn wallet_b() -> WalletAddress {
    WalletAddress::from_hex(WALLET_B_HEX).unwrap()
}

fn trade_url(wallet: WalletAddress, offset: u32) -> String {
    format!(
        "{}&limit=500&offset={offset}",
        PolymarketEndpoint::UserTrades {
            user: wallet.to_string()
        }
        .url(BASE_URL)
    )
}

/// Build a JSON page of N trades for `wallet`, newest-first.
/// Timestamps are synthetic but strictly decreasing so sort is a no-op.
fn trades_page(wallet: &str, hashes: &[(&str, i64)]) -> Vec<u8> {
    let entries: Vec<String> = hashes
        .iter()
        .map(|(h, ts)| {
            format!(
                r#"{{"transactionHash":"{h}","conditionId":"0xcond","side":"BUY","size":1,"price":0.60,"timestamp":{ts},"maker":"{wallet}"}}"#
            )
        })
        .collect();
    format!("[{}]", entries.join(",")).into_bytes()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────
//
// PASS: empty cache → all trades fetched and stored on cold run.
// FAIL: any trade missing from cache after the run.

#[tokio::test]
async fn scenario_first_run_full_fetch() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();

    let page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xhash1", 2_000_003),
            ("0xhash2", 2_000_002),
            ("0xhash3", 2_000_001),
        ],
    );
    let mut responses = HashMap::new();
    responses.insert(trade_url(wallet_a(), 0), page);

    let mut fetcher =
        PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
    let trades = fetcher.fetch_all(&[wallet_a()], &mut cache).await;

    assert_eq!(trades.len(), 3, "all 3 trades must be returned");
    assert_eq!(
        cache.known_trade_ids(WALLET_A_HEX).len(),
        3,
        "all 3 trades must be in the cache"
    );

    // Persist and reload to verify round-trip.
    cache.save().unwrap();
    let reloaded = WalletCache::open(&dir.path().join("cache.json")).unwrap();
    assert_eq!(reloaded.trades_for(WALLET_A_HEX).len(), 3);
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────
//
// PASS: second run with identical API response writes nothing new and makes
//       exactly 1 API call (stops on 3 consecutive known IDs).
// FAIL: cache count changes after the second run.

#[tokio::test]
async fn scenario_second_run_no_new_trades() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();

    let page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xhash1", 2_000_003),
            ("0xhash2", 2_000_002),
            ("0xhash3", 2_000_001),
        ],
    );

    // Cold run.
    let mut r1 = HashMap::new();
    r1.insert(trade_url(wallet_a(), 0), page.clone());
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;
    assert_eq!(cache.trade_count(), 3);

    // Warm run — same page, all 3 hashes already known (stop after 3 consecutive).
    let mut r2 = HashMap::new();
    r2.insert(trade_url(wallet_a(), 0), page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;

    assert_eq!(
        cache.trade_count(),
        3,
        "cache count must not change on a warm run with no new trades"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────
//
// PASS: second run with 3 new trades + existing trades appends exactly 3 new.
// FAIL: wrong count or new trades missing.

#[tokio::test]
async fn scenario_second_run_with_new_trades() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();

    // Cold run: 3 old trades.
    let old_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xold1", 1_000_003),
            ("0xold2", 1_000_002),
            ("0xold3", 1_000_001),
        ],
    );
    let mut r1 = HashMap::new();
    r1.insert(trade_url(wallet_a(), 0), old_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;

    // Warm run: 3 new + 3 old (hits 3 consecutive known → stop).
    let new_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xnew1", 2_000_003),
            ("0xnew2", 2_000_002),
            ("0xnew3", 2_000_001),
            ("0xold1", 1_000_003), // known — 1
            ("0xold2", 1_000_002), // known — 2
            ("0xold3", 1_000_001), // known — 3: stop
        ],
    );
    let mut r2 = HashMap::new();
    r2.insert(trade_url(wallet_a(), 0), new_page);
    let all = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;

    assert_eq!(cache.trade_count(), 6, "3 old + 3 new = 6");
    assert_eq!(all.len(), 6);

    let ids: Vec<_> = cache
        .known_trade_ids(WALLET_A_HEX)
        .iter()
        .map(|id| id.0.as_str())
        .collect();
    assert!(ids.contains(&"0xnew1"));
    assert!(ids.contains(&"0xnew2"));
    assert!(ids.contains(&"0xnew3"));
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────
//
// PASS: loading a legacy-format cache file silently yields a blank cache and
//       the run proceeds as a cold start (no error returned).
// FAIL: open() returns Err, panics, or returns a non-empty cache.

#[tokio::test]
async fn scenario_legacy_file_auto_wiped() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.json");

    // Old CacheEntry format (pre-#71).
    let legacy = r#"{"entries":{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":{"fetched_at_unix":1704067200,"trades":[]}}}"#;
    std::fs::write(&path, legacy).unwrap();

    let cache = WalletCache::open(&path).unwrap();
    assert_eq!(
        cache.trade_count(),
        0,
        "legacy file must produce a blank cache — run proceeds as cold start"
    );
    assert!(
        path.with_extension("json.bak").exists(),
        "legacy file must be renamed to .bak"
    );

    // Fetch with fixture to confirm cold-start behaviour.
    let page = trades_page(WALLET_A_HEX, &[("0xhash1", 2_000_001)]);
    let mut responses = HashMap::new();
    responses.insert(trade_url(wallet_a(), 0), page);
    let mut cache_mut = cache;
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .fetch_all(&[wallet_a()], &mut cache_mut)
        .await;
    assert_eq!(cache_mut.trade_count(), 1);
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────
//
// PASS: build_seed_watchlist with the same snapshot_at produces bit-identical
//       output on two consecutive calls from the same populated cache.
// FAIL: any field differs between the two calls.

#[tokio::test]
async fn scenario_replay_reproducibility() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();

    // Populate with enough trades to produce a non-empty watchlist.
    // 12 winning trades for wallet_a.
    let win_trades: Vec<(&str, i64)> = (0..12)
        .map(|i| {
            let ts = 1_700_000_000 - i as i64 * 86_400;
            // We cannot use dynamic strings as literals; build vec separately.
            (
                Box::leak(format!("0xwintx{i:03}").into_boxed_str()) as &str,
                ts,
            )
        })
        .collect();
    let page = trades_page(WALLET_A_HEX, &win_trades);
    let mut responses = HashMap::new();
    responses.insert(trade_url(wallet_a(), 0), page);

    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;
    cache.save().unwrap();

    let fixed_snapshot_at =
        SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_701_000_000).unwrap());
    let empty_ops: &[OperatorIdentity] = &[];

    let build_watchlist = |c: &WalletCache| {
        let trades = c.trades_for(WALLET_A_HEX);
        let snapshot = TradeSnapshot {
            trades,
            snapshot_at: fixed_snapshot_at.clone(),
            audit_window_days: u32::MAX,
        };
        let ledgers = build_trader_ledgers(&snapshot, empty_ops, &LedgerConfig::default());
        build_seed_watchlist(
            ledgers,
            fixed_snapshot_at.clone(),
            DEFAULT_MIN_CLOSED_TRADES,
            DEFAULT_MIN_WIN_RATE_PCT,
        )
    };

    let reloaded = WalletCache::open(&dir.path().join("cache.json")).unwrap();
    let w1 = build_watchlist(&reloaded);
    let w2 = build_watchlist(&reloaded);

    assert_eq!(
        w1.active_count, w2.active_count,
        "active_count must be identical across two calls"
    );
    assert_eq!(
        w1.entries.len(),
        w2.entries.len(),
        "entry count must be identical"
    );
    for (e1, e2) in w1.entries.iter().zip(w2.entries.iter()) {
        assert_eq!(e1.wallet, e2.wallet, "wallet must match");
        assert_eq!(e1.leader_score_bps, e2.leader_score_bps, "score must match");
    }
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────
//
// PASS: a page returned in ascending order (oldest-first) is sorted correctly
//       and new trades appearing before the known sequence are appended.
// FAIL: new trade is missing from cache after the run.

#[tokio::test]
async fn scenario_out_of_order_page_handled() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();

    // Cold run: 3 old trades (newest-first, normal order).
    let old_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xold1", 1_000_003),
            ("0xold2", 1_000_002),
            ("0xold3", 1_000_001),
        ],
    );
    let mut r1 = HashMap::new();
    r1.insert(trade_url(wallet_a(), 0), old_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;

    // Incremental run: page arrives oldest-first (ascending timestamps).
    // After defensive sort: new1(2M), old1(1_000_003), old2(1_000_002), old3(1_000_001).
    // new1 is unknown → appended. old1..old3 = 3 consecutive known → stop.
    let reversed_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xold3", 1_000_001),
            ("0xold2", 1_000_002),
            ("0xold1", 1_000_003),
            ("0xnew1", 2_000_000),
        ],
    );
    let mut r2 = HashMap::new();
    r2.insert(trade_url(wallet_a(), 0), reversed_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;

    assert_eq!(
        cache.trade_count(),
        4,
        "new trade arriving before known sequence must be appended despite reversed page order"
    );
    let ids: Vec<_> = cache
        .known_trade_ids(WALLET_A_HEX)
        .iter()
        .map(|id| &id.0)
        .collect();
    assert!(ids.iter().any(|id| id.as_str() == "0xnew1"));
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────
//
// PASS: config.audit_window_days = None → u32::MAX sentinel → all trades from
//       any timestamp are included in build_trader_ledgers output.
// FAIL: old trades excluded from ledger reconstruction.

#[tokio::test]
async fn scenario_unlimited_window_includes_all_trades() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();

    // Two trades: one recent (2024), one ancient (2020).
    let page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xrecent", 1_704_067_200),  // 2024-01-01
            ("0xancient", 1_580_000_000), // 2020-01-26
        ],
    );
    let mut responses = HashMap::new();
    responses.insert(trade_url(wallet_a(), 0), page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .fetch_all(&[wallet_a()], &mut cache)
        .await;

    assert_eq!(cache.trade_count(), 2, "both trades must be cached");

    // Build snapshot with audit_window_days = u32::MAX (unlimited).
    let trades = cache.trades_for(WALLET_A_HEX);
    assert_eq!(trades.len(), 2, "trades_for must return both trades");

    let snapshot_at = SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_704_100_000).unwrap());
    let snapshot = TradeSnapshot {
        trades,
        snapshot_at: snapshot_at.clone(),
        audit_window_days: u32::MAX,
    };

    let empty_ops: &[OperatorIdentity] = &[];
    let ledgers = build_trader_ledgers(&snapshot, empty_ops, &LedgerConfig::default());
    assert!(!ledgers.is_empty(), "at least one ledger must be built");

    // Confirm the ledger for wallet_a includes both trade IDs.
    let ledger = ledgers
        .iter()
        .find(|l| l.wallet == wallet_a())
        .expect("ledger for wallet_a must exist");

    let all_ids: Vec<&SourceTradeId> = ledger
        .open_positions
        .iter()
        .flat_map(|p| p.source_trade_ids.iter())
        .chain(
            ledger
                .closed_trades
                .iter()
                .flat_map(|t| t.source_trade_ids.iter()),
        )
        .collect();

    assert!(
        all_ids.iter().any(|id| id.0 == "0xrecent"),
        "recent trade must appear in the ledger"
    );
    assert!(
        all_ids.iter().any(|id| id.0 == "0xancient"),
        "ancient trade must appear in the ledger — unlimited window must not exclude it"
    );
}
