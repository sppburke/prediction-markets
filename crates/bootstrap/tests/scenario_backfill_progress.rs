//! #608: durable complete seconds, restart equivalence, and whole forward writes.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::infra_probe::InfraProbe;
use pe_bootstrap::pile::{self, SRC_LEADERBOARD};
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::WalletAddress;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{FixtureFetcher, PageFetcher, PolymarketEndpoint};
use serde_json::{Value, json};
use tempfile::TempDir;

const BASE: &str = "https://data-api.polymarket.com";
const NOW: i64 = 2_000_000_000;
type Pages = HashMap<String, Vec<u8>>;

fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

fn url(start: i64, end: i64, offset: u32) -> String {
    PolymarketEndpoint::UserTradeActivityPage {
        user: wallet().to_string(),
        start: Some(start),
        end,
        offset,
    }
    .url(BASE)
}

fn trade(id: usize, ts: i64) -> Value {
    json!({"transactionHash": format!("0x{id:064x}"), "conditionId": "market",
           "side": "BUY", "size": "1", "price": "0.5", "outcomeIndex": 0, "timestamp": ts})
}

fn history(n: usize, top: i64) -> Vec<Value> {
    (0..n)
        .map(|i| trade(i, top - i64::try_from(i).unwrap()))
        .collect()
}

fn put(pages: &mut Pages, start: i64, end: i64, offset: u32, rows: &[Value]) {
    pages.insert(url(start, end, offset), serde_json::to_vec(rows).unwrap());
}

// Explicitly serve every requested offset, including the empty terminal page.
fn window(pages: &mut Pages, start: i64, end: i64, rows: &[Value]) {
    for offset in (0..=5000).step_by(500) {
        let first = usize::try_from(offset).unwrap();
        let page = &rows[first.min(rows.len())..(first + 500).min(rows.len())];
        put(pages, start, end, offset, page);
        if page.len() < 500 {
            break;
        }
    }
}

fn backward(pages: &mut Pages, mut end: i64, rows: &[Value]) {
    let mut remaining = rows;
    while end >= 1 {
        let count = remaining.len().min(500);
        let page = &remaining[..count];
        put(pages, 1, end, 0, page);
        if count < 500 {
            break;
        }
        let second = page
            .iter()
            .map(|r| r["timestamp"].as_i64().unwrap())
            .min()
            .unwrap();
        let seam: Vec<_> = remaining
            .iter()
            .filter(|r| r["timestamp"] == second)
            .cloned()
            .collect();
        window(pages, second, second, &seam);
        let consumed = remaining
            .iter()
            .take_while(|r| r["timestamp"].as_i64().unwrap() >= second)
            .count();
        remaining = &remaining[consumed..];
        end = second - 1;
    }
}

struct PartialHangFetcher {
    inner: FixtureFetcher,
    requests: Arc<Mutex<Vec<String>>>,
}

impl PageFetcher for PartialHangFetcher {
    async fn fetch_page(&self, request: &str) -> Result<Vec<u8>, SourceError> {
        self.requests.lock().unwrap().push(request.to_owned());
        match self.inner.fetch_page(request).await {
            Ok(bytes) => Ok(bytes),
            Err(_) => std::future::pending().await,
        }
    }
}

fn bulk(
    pages: Pages,
) -> (
    PolymarketBulkFetcher<PartialHangFetcher>,
    Arc<Mutex<Vec<String>>>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let fetcher = PartialHangFetcher {
        inner: FixtureFetcher::new(pages),
        requests: requests.clone(),
    };
    (
        PolymarketBulkFetcher::new(BASE.to_owned(), fetcher)
            .with_clock_for_test(|| NOW)
            .with_wallet_timeout(1)
            .with_infra_probe(InfraProbe { threshold_secs: 0 })
            .with_stamp_on_success(true),
        requests,
    )
}

fn cache(dir: &TempDir) -> WalletCache {
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    cache
        .upsert_wallet(
            &wallet().to_string(),
            SRC_LEADERBOARD,
            false,
            None,
            None,
            None,
        )
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();
    cache
}

fn state(cache: &WalletCache) -> (i64, Option<i64>) {
    cache
        .raw_conn_for_test()
        .query_row(
            "SELECT backfill_partial, last_polymarket_fetch_at FROM wallets WHERE wallet_hex = ?1",
            [wallet().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

fn row_bytes(cache: &WalletCache) -> Vec<u8> {
    let mut stmt = cache.raw_conn_for_test().prepare(
        "SELECT json_array(source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) FROM trades ORDER BY source_trade_id"
    ).unwrap();
    let rows: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows.join("\n").into_bytes()
}

#[tokio::test(start_paused = true)]
async fn two_pages_survive_timeout_reopen_and_resume_matches_uninterrupted_bytes() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    let rows = history(1200, 10_000);
    let mut first = Pages::new();
    put(&mut first, 1, NOW, 0, &rows[..500]);
    window(&mut first, 9501, 9501, &rows[499..500]);
    put(&mut first, 1, 9500, 0, &rows[500..1000]);
    window(&mut first, 9001, 9001, &rows[999..1000]);
    let (fetcher, requests) = bulk(first);
    assert_eq!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed,
        [wallet()]
    );
    assert_eq!(cache.trade_count(), 1000);
    assert_eq!(state(&cache), (1, None));
    assert_eq!(requests.lock().unwrap().last(), Some(&url(1, 9000, 0)));
    assert_eq!(
        pile::select_backfill_due(&cache, NOW, 0).unwrap(),
        [wallet().to_string()]
    );
    drop(cache);
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    assert_eq!(cache.trade_count(), 1000);
    assert_eq!(state(&cache), (1, None));
    let mut rest = Pages::new();
    backward(&mut rest, 9000, &rows[1000..]);
    window(&mut rest, 10_000, 10_000, &rows[..1]);
    window(&mut rest, 10_001, NOW, &[]);
    assert!(
        bulk(rest)
            .0
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    drop(cache);
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    assert_eq!(state(&cache), (0, Some(NOW)));
    let mut uninterrupted = WalletCache::open(&dir.path().join("uninterrupted.db")).unwrap();
    let mut all = Pages::new();
    backward(&mut all, NOW, &rows);
    assert!(
        bulk(all)
            .0
            .fetch_all(&[wallet()], &mut uninterrupted)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(row_bytes(&cache), row_bytes(&uninterrupted));
}

#[tokio::test(start_paused = true)]
async fn resume_with_600_newer_trades_cannot_jump_over_the_older_forward_page() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    let rows = history(700, 10_000);
    let mut first = Pages::new();
    put(&mut first, 1, NOW, 0, &rows[..500]);
    window(&mut first, 9501, 9501, &rows[499..500]);
    assert_eq!(
        bulk(first)
            .0
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed,
        [wallet()]
    );
    let newer: Vec<_> = (0..600)
        .map(|i| trade(1000 + i, 20_000 - i64::try_from(i).unwrap()))
        .collect();
    let mut rest = Pages::new();
    backward(&mut rest, 9500, &rows[500..]);
    window(&mut rest, 10_000, 10_000, &rows[..1]);
    window(&mut rest, 10_001, NOW, &newer);
    let (fetcher, requests) = bulk(rest);
    assert!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(cache.trade_count(), 1300);
    assert!(requests.lock().unwrap().contains(&url(10_001, NOW, 500)));
    assert_eq!(state(&cache), (0, Some(NOW)));
    let mut all = rows;
    all.extend(newer);
    let ids = cache.known_trade_ids(&wallet().to_string());
    assert!(all.iter().all(|r| {
        ids.iter()
            .any(|id| id.0 == r["transactionHash"].as_str().unwrap())
    }));
}

#[tokio::test(start_paused = true)]
async fn boundary_second_is_complete_before_requesting_older_history() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    let mut rows = history(499, 10_000);
    let seam: Vec<_> = (0..620).map(|i| trade(499 + i, 9000)).collect();
    rows.extend(seam.clone());
    rows.push(trade(2000, 8999));
    let mut pages = Pages::new();
    backward(&mut pages, NOW, &rows);
    let (fetcher, requests) = bulk(pages);
    assert!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(cache.trade_count(), 1120);
    let requests = requests.lock().unwrap();
    let seam_end = requests
        .iter()
        .position(|u| *u == url(9000, 9000, 500))
        .unwrap();
    let older = requests.iter().position(|u| *u == url(1, 8999, 0)).unwrap();
    assert!(seam_end < older);
}

#[tokio::test(start_paused = true)]
async fn saturated_backward_second_commits_only_above_it_and_stays_due() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    let stale = NOW - 200_000;
    cache
        .update_last_polymarket_fetch(&wallet().to_string(), stale)
        .unwrap();
    let mut page = history(499, 10_000);
    let seam: Vec<_> = (0..5500).map(|i| trade(499 + i, 9000)).collect();
    page.push(seam[0].clone());
    let mut pages = Pages::new();
    put(&mut pages, 1, NOW, 0, &page);
    window(&mut pages, 9000, 9000, &seam);
    let (fetcher, requests) = bulk(pages);
    assert_eq!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed,
        [wallet()]
    );
    assert_eq!(cache.trade_count(), 499);
    assert_eq!(state(&cache), (1, Some(stale)));
    assert_eq!(
        requests.lock().unwrap().last(),
        Some(&url(9000, 9000, 5000))
    );
    assert_eq!(
        cache.trade_ts_bounds(&wallet().to_string()).unwrap(),
        Some((9502, 10_000))
    );
    assert_eq!(
        pile::select_backfill_due(&cache, NOW, 0).unwrap(),
        [wallet().to_string()]
    );
}

async fn seed(cache: &mut WalletCache, top: i64) {
    let mut pages = Pages::new();
    put(&mut pages, 1, NOW, 0, &[trade(100_000, top)]);
    assert!(
        bulk(pages)
            .0
            .fetch_all(&[wallet()], cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
}

#[tokio::test(start_paused = true)]
async fn saturated_5500_row_forward_window_splits_lower_seam_upper_in_order() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    seed(&mut cache, 1000).await;
    let rows = history(5500, 10_000);
    let boundary = 4501;
    let mut pages = Pages::new();
    put(&mut pages, 1, 999, 0, &[]);
    window(&mut pages, 1000, 1000, &[trade(100_000, 1000)]);
    window(&mut pages, 1001, NOW, &rows);
    window(&mut pages, 1001, boundary - 1, &[]);
    window(&mut pages, boundary, boundary, &rows[5499..]);
    window(&mut pages, boundary + 1, NOW, &rows[..5499]);
    let (fetcher, requests) = bulk(pages);
    assert!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(cache.trade_count(), 5501);
    let mut actual: Vec<_> = cache
        .known_trade_ids(&wallet().to_string())
        .into_iter()
        .map(|id| id.0)
        .collect();
    let mut expected: Vec<_> = rows
        .iter()
        .map(|r| r["transactionHash"].as_str().unwrap().to_owned())
        .collect();
    expected.push(
        trade(100_000, 1000)["transactionHash"]
            .as_str()
            .unwrap()
            .to_owned(),
    );
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    assert_eq!(state(&cache), (0, Some(NOW)));
    let req = requests.lock().unwrap();
    let lower = req
        .iter()
        .position(|u| *u == url(1001, boundary - 1, 0))
        .unwrap();
    assert_eq!(req[lower + 1], url(boundary, boundary, 0));
    assert_eq!(req[lower + 2], url(boundary + 1, NOW, 0));
}

#[tokio::test(start_paused = true)]
async fn forward_split_at_lo_plus_one_and_hi_stops_on_saturated_seam_without_writing() {
    for boundary in [1001, NOW] {
        let dir = TempDir::new().unwrap();
        let mut cache = cache(&dir);
        seed(&mut cache, 1000).await;
        let before = row_bytes(&cache);
        let rows: Vec<_> = (0..5500).map(|i| trade(i, boundary)).collect();
        let mut pages = Pages::new();
        put(&mut pages, 1, 999, 0, &[]);
        window(&mut pages, 1000, 1000, &[trade(100_000, 1000)]);
        window(&mut pages, 1001, NOW, &rows);
        if boundary > 1001 {
            window(&mut pages, 1001, boundary - 1, &[trade(90000, 1002)]);
        }
        window(&mut pages, boundary, boundary, &rows);
        let (fetcher, requests) = bulk(pages);
        assert_eq!(
            fetcher
                .fetch_all(&[wallet()], &mut cache)
                .await
                .unwrap()
                .failed,
            [wallet()]
        );
        assert_eq!(
            row_bytes(&cache),
            before,
            "even a completed lower forward piece must remain uncommitted"
        );
        assert_eq!(state(&cache), (1, Some(NOW)));
        assert_eq!(
            requests.lock().unwrap().last(),
            Some(&url(boundary, boundary, 5000))
        );
        assert!(!requests.lock().unwrap().contains(&url(1001, 1000, 0)));
    }
}

#[tokio::test(start_paused = true)]
async fn later_json_and_insert_errors_keep_prior_pages_and_rollback_failed_batch() {
    for insert_failure in [false, true] {
        let dir = TempDir::new().unwrap();
        let mut cache = cache(&dir);
        if insert_failure {
            cache.raw_conn_for_test().execute_batch(
                "CREATE TRIGGER fail_trade BEFORE INSERT ON trades WHEN NEW.timestamp_unix = 9400 BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END;"
            ).unwrap();
        }
        let rows = history(750, 10_000);
        let mut pages = Pages::new();
        put(&mut pages, 1, NOW, 0, &rows[..500]);
        window(&mut pages, 9501, 9501, &rows[499..500]);
        if insert_failure {
            put(&mut pages, 1, 9500, 0, &rows[500..]);
        } else {
            pages.insert(url(1, 9500, 0), b"{broken json".to_vec());
        }
        let (fetcher, requests) = bulk(pages);
        assert_eq!(
            fetcher
                .fetch_all(&[wallet()], &mut cache)
                .await
                .unwrap()
                .failed,
            [wallet()]
        );
        assert_eq!(cache.trade_count(), 500);
        assert_eq!(state(&cache), (1, None));
        assert_eq!(requests.lock().unwrap().len(), 3);
        drop(cache);
        let reopened = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        assert_eq!(reopened.trade_count(), 500);
        assert_eq!(state(&reopened), (1, None));
    }
}

#[tokio::test(start_paused = true)]
async fn failed_marker_write_blocks_all_trade_writes() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    cache.raw_conn_for_test().execute_batch(
        "CREATE TRIGGER fail_marker BEFORE UPDATE OF backfill_partial ON wallets BEGIN SELECT RAISE(ABORT, 'marker failure'); END;"
    ).unwrap();
    let mut pages = Pages::new();
    put(&mut pages, 1, NOW, 0, &[trade(1, 1000)]);
    let (fetcher, requests) = bulk(pages);
    assert_eq!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed,
        [wallet()]
    );
    assert_eq!(cache.trade_count(), 0);
    assert_eq!(state(&cache), (0, None));
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn frozen_hi_excludes_new_arrivals_and_rechecks_previous_maximum_second() {
    struct AdvancingFetcher {
        inner: FixtureFetcher,
        clock: Arc<AtomicI64>,
    }
    impl PageFetcher for AdvancingFetcher {
        async fn fetch_page(&self, request: &str) -> Result<Vec<u8>, SourceError> {
            self.clock.store(NOW + 100, Ordering::SeqCst);
            self.inner.fetch_page(request).await
        }
    }
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    seed(&mut cache, 1000).await;
    let clock = Arc::new(AtomicI64::new(NOW));
    let mut pages = Pages::new();
    put(&mut pages, 1, 999, 0, &[]);
    // Duplicate cached ID ignored; newly indexed sibling at the old max retained.
    window(
        &mut pages,
        1000,
        1000,
        &[trade(100_000, 1000), trade(1, 1000), trade(1, 1000)],
    );
    window(&mut pages, 1001, NOW, &[trade(2, NOW)]);
    let reader_clock = clock.clone();
    let fetcher = PolymarketBulkFetcher::new(
        BASE.to_owned(),
        AdvancingFetcher {
            inner: FixtureFetcher::new(pages),
            clock,
        },
    )
    .with_clock_for_test(move || reader_clock.load(Ordering::SeqCst))
    .with_stamp_on_success(true);
    assert!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(cache.trade_count(), 3);
    assert_eq!(
        cache
            .trade_ts_bounds(&wallet().to_string())
            .unwrap()
            .unwrap()
            .1,
        NOW
    );
    let mut next = Pages::new();
    put(&mut next, 1, 999, 0, &[]);
    window(&mut next, NOW, NOW, &[trade(2, NOW)]);
    window(&mut next, NOW + 1, NOW + 100, &[trade(3, NOW + 1)]);
    assert!(
        bulk(next)
            .0
            .with_clock_for_test(|| NOW + 100)
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(cache.trade_count(), 4);
}

#[tokio::test(start_paused = true)]
async fn failed_completion_commit_is_failed_and_preserves_marker_and_stamp() {
    for stamping in [false, true] {
        let dir = TempDir::new().unwrap();
        let mut cache = cache(&dir);
        cache
            .update_last_polymarket_fetch(&wallet().to_string(), 100)
            .unwrap();
        cache.raw_conn_for_test().execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE final_parent (id INTEGER PRIMARY KEY);
             CREATE TABLE final_child (id INTEGER REFERENCES final_parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER fail_completion AFTER UPDATE OF backfill_partial ON wallets
             WHEN NEW.backfill_partial = 0 BEGIN INSERT INTO final_child VALUES (1); END;"
        ).unwrap();
        let mut pages = Pages::new();
        put(&mut pages, 1, NOW, 0, &[trade(1, 1000)]);
        assert_eq!(
            bulk(pages)
                .0
                .with_stamp_on_success(stamping)
                .fetch_all(&[wallet()], &mut cache)
                .await
                .unwrap()
                .failed,
            [wallet()]
        );
        assert_eq!(cache.trade_count(), 1);
        drop(cache);
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        assert_eq!(state(&cache), (1, Some(100)));
        cache
            .raw_conn_for_test()
            .execute_batch("DROP TRIGGER fail_completion;")
            .unwrap();
        let mut pages = Pages::new();
        put(&mut pages, 1, 999, 0, &[]);
        window(&mut pages, 1000, 1000, &[trade(1, 1000)]);
        window(&mut pages, 1001, NOW, &[]);
        assert!(
            bulk(pages)
                .0
                .with_stamp_on_success(stamping)
                .fetch_all(&[wallet()], &mut cache)
                .await
                .unwrap()
                .failed
                .is_empty()
        );
        drop(cache);
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        assert_eq!(state(&cache), (0, Some(if stamping { NOW } else { 100 })));
    }
}

#[tokio::test(start_paused = true)]
async fn full_history_uses_positive_start_including_the_cold_probe() {
    let dir = TempDir::new().unwrap();
    let mut cache = cache(&dir);
    let rows = history(550, 1_000_000_000); // Decades before the frozen upper bound.
    let mut pages = Pages::new();
    backward(&mut pages, NOW, &rows);
    // A default-window request would succeed but silently hide this old history.
    pages.insert(
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet().to_string(),
            start: None,
            end: NOW,
            offset: 0,
        }
        .url(BASE),
        b"[]".to_vec(),
    );
    let (fetcher, requests) = bulk(pages);
    assert!(
        fetcher
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap()
            .failed
            .is_empty()
    );
    assert_eq!(cache.trade_count(), 550);
    assert_eq!(requests.lock().unwrap()[0], url(1, NOW, 0));
    assert_eq!(requests.lock().unwrap()[2], url(1, 999_999_500, 0));
}
