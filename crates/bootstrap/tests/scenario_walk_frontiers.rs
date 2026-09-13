//! Approved #608/#609 coverage and convergence contracts under deterministic interruption.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "support/activity.rs"]
mod activity;
use activity::HistoryFetcher;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::infra_probe::InfraProbe;
use pe_bootstrap::pile::{self, SRC_LEADERBOARD};
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{FixtureFetcher, PageFetcher, PolymarketEndpoint};
use serde_json::{Value, json};
use tempfile::TempDir;

const BASE: &str = "https://data-api.polymarket.com";
fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}
fn trade(id: i64, ts: i64) -> Value {
    json!({"transactionHash": format!("t{id}"), "conditionId": "market", "side": "BUY", "size": "1", "price": "0.5", "timestamp": ts})
}
fn setup(dir: &TempDir) -> WalletCache {
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
fn bounds(cache: &WalletCache) -> (i64, Option<i64>, Option<i64>, Option<i64>) {
    cache.raw_conn_for_test().query_row("SELECT backfill_partial, backward_floor_unix, forward_frontier_unix, last_polymarket_fetch_at FROM wallets WHERE wallet_hex = ?1", [wallet().to_string()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).unwrap()
}
fn set_bounds(cache: &WalletCache, floor: Option<i64>, frontier: Option<i64>) {
    cache.raw_conn_for_test().execute("UPDATE wallets SET backfill_partial = 1, backward_floor_unix = ?1, forward_frontier_unix = ?2", rusqlite::params![floor, frontier]).unwrap();
}
fn insert(cache: &WalletCache, id: &str, ts: i64) {
    cache.raw_conn_for_test().execute("INSERT INTO trades (source_trade_id, wallet_hex, market_id, outcome_id, side, price_str, contracts, timestamp_unix) VALUES (?1, ?2, 'market', 0, 'buy', '0.5', 1, ?3)", rusqlite::params![id, wallet().to_string(), ts]).unwrap();
}
async fn walk<F: PageFetcher>(cache: &mut WalletCache, venue: F, hi: i64) -> bool {
    PolymarketBulkFetcher::new(BASE.to_owned(), venue)
        .with_clock_for_test(move || hi + 120)
        .with_wallet_timeout(1)
        .with_infra_probe(InfraProbe { threshold_secs: 0 })
        .with_stamp_on_success(true)
        .fetch_all(&[wallet()], cache)
        .await
        .unwrap()
        .failed
        .is_empty()
}
fn request(start: i64, end: i64, offset: u32) -> String {
    PolymarketEndpoint::UserTradeActivityPage {
        user: wallet().to_string(),
        start: Some(start),
        end,
        offset,
    }
    .url(BASE)
}

#[tokio::test(start_paused = true)]
async fn forward_200000_rows_advance_across_three_request_interruptions_then_complete() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 100);
    set_bounds(&cache, Some(1), Some(100));
    let rows: Vec<_> = (101..=200_100).map(|t| trade(t, t)).collect();
    let mut frontier = 100;
    for _ in 0..3 {
        let mut venue = HistoryFetcher::new(rows.clone());
        venue.request_budget = 15;
        assert!(!walk(&mut cache, venue, 200_100).await);
        let state = bounds(&cache);
        assert!(state.2.unwrap() > frontier);
        frontier = state.2.unwrap();
        assert_eq!((state.0, state.3), (1, None));
    }
    assert!(walk(&mut cache, HistoryFetcher::new(rows), 200_100).await);
    assert_eq!(cache.trade_count(), 200_001);
    assert_eq!(bounds(&cache), (0, Some(1), Some(200_100), Some(200_220)));
}

#[tokio::test(start_paused = true)]
async fn forward_200000_rows_converge_using_only_window_budgeted_walks() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 100);
    set_bounds(&cache, Some(1), Some(100));
    let rows: Vec<_> = (101..=200_100).map(|t| trade(t, t)).collect();
    let mut frontier = 100;
    for attempt in 0..30 {
        let mut venue = HistoryFetcher::new(rows.clone());
        venue.window_budget = 10;
        let complete = walk(&mut cache, venue, 200_100).await;
        let next = bounds(&cache).2.unwrap();
        assert!(next > frontier);
        frontier = next;
        if complete {
            break;
        }
        assert!(attempt < 29, "bounded-window walks must converge");
    }
    assert_eq!(frontier, 200_100);
    assert_eq!(cache.trade_count(), 200_001);
}

#[tokio::test(start_paused = true)]
async fn first_forward_second_499_500_501_5499_requires_every_response_before_commit() {
    for count in [499, 500, 501, 5499] {
        let dir = TempDir::new().unwrap();
        let mut cache = setup(&dir);
        insert(&cache, "seed", 100);
        set_bounds(&cache, Some(1), Some(100));
        let rows: Vec<_> = (0..count).map(|i| trade(i, 101)).collect();
        let mut interrupted = HistoryFetcher::new(rows.clone());
        interrupted.request_budget = usize::try_from(count / 500).unwrap();
        assert!(!walk(&mut cache, interrupted, 101).await);
        assert_eq!(bounds(&cache).2, Some(100));
        assert_eq!(cache.trade_count(), 1);
        let venue = HistoryFetcher::new(rows);
        let requests = venue.requests.clone();
        assert!(walk(&mut cache, venue, 101).await);
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            (0..=count / 500)
                .map(|i| (101, 101, usize::try_from(i * 500).unwrap()))
                .collect::<Vec<_>>()
        );
        assert_eq!(bounds(&cache).2, Some(101));
        assert_eq!(cache.trade_count(), usize::try_from(count + 1).unwrap());
    }
}

#[tokio::test(start_paused = true)]
async fn raw_density_widths_and_saturation_shrink_eightfold_without_committing_attempt() {
    for (count, expected) in [(0, 8), (499, 4), (501, 3), (5499, 1)] {
        let dir = TempDir::new().unwrap();
        let mut cache = setup(&dir);
        insert(&cache, "seed", 100);
        set_bounds(&cache, Some(1), Some(100));
        let rows = (0..count)
            .map(|i| {
                let mut r = trade(i, 101);
                if i > 0 {
                    r["size"] = json!("0");
                }
                r
            })
            .collect();
        let mut venue = HistoryFetcher::new(rows);
        venue.window_budget = 1;
        let requests = venue.requests.clone();
        assert!(!walk(&mut cache, venue, 200).await);
        assert_eq!(
            requests.lock().unwrap().last(),
            Some(&(102, 101 + expected, 0))
        );
        assert_eq!(bounds(&cache).2, Some(101));
    }
    // Empty width one grows to eight; 5499 raw at width eight shrinks to two.
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 100);
    set_bounds(&cache, Some(1), Some(100));
    let rows = (0..5499)
        .map(|i| {
            let mut r = trade(i, 109);
            if i > 0 {
                r["size"] = json!("0");
            }
            r
        })
        .collect();
    let mut venue = HistoryFetcher::new(rows);
    venue.window_budget = 2;
    let req = venue.requests.clone();
    assert!(!walk(&mut cache, venue, 200).await);
    assert_eq!(req.lock().unwrap().last(), Some(&(110, 111, 0)));
    assert_eq!(cache.trade_count(), 2);
    assert_eq!(bounds(&cache).2, Some(109));
    // Saturation spends eleven responses, then retries width eight as width one.
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 100);
    set_bounds(&cache, Some(1), Some(100));
    let rows = (0..5500).map(|i| trade(i, 102 + i % 8)).collect();
    let mut venue = HistoryFetcher::new(rows);
    venue.request_budget = 12;
    let req = venue.requests.clone();
    assert!(!walk(&mut cache, venue, 200).await);
    let req = req.lock().unwrap();
    assert_eq!(req[0], (101, 101, 0));
    assert_eq!(
        &req[1..12],
        (0..11).map(|i| (102, 109, i * 500)).collect::<Vec<_>>()
    );
    assert_eq!(req[12], (102, 102, 0));
    assert_eq!(bounds(&cache).2, Some(101));
}

#[tokio::test(start_paused = true)]
async fn saturated_forward_second_never_advances_frontier() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 100);
    set_bounds(&cache, Some(1), Some(100));
    let venue = HistoryFetcher::new((0..5500).map(|i| trade(i, 101)).collect());
    let req = venue.requests.clone();
    assert!(!walk(&mut cache, venue, 101).await);
    assert_eq!(req.lock().unwrap().len(), 11);
    assert_eq!(bounds(&cache).2, Some(100));
    assert_eq!(cache.trade_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn backward_boundary_floor_is_502_until_second_501_is_acquired_after_reopen() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    let mut rows: Vec<_> = (501..=1000).map(|t| trade(t, t)).collect();
    rows.extend((2000..2499).map(|id| trade(id, 501)));
    let mut venue = HistoryFetcher::new(rows.clone());
    venue.request_budget = 1;
    assert!(!walk(&mut cache, venue, 1000).await);
    assert_eq!(bounds(&cache), (1, Some(502), Some(1000), None));
    assert_eq!(cache.trade_count(), 499);
    drop(cache);
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let mut venue = HistoryFetcher::new(rows.clone());
    venue.request_budget = 3;
    let req = venue.requests.clone();
    assert!(!walk(&mut cache, venue, 1000).await);
    assert_eq!(req.lock().unwrap()[0], (1, 501, 0));
    assert_eq!(cache.trade_count(), 999);
    assert_eq!(bounds(&cache).1, Some(501));
    assert!(walk(&mut cache, HistoryFetcher::new(rows), 1000).await);
    assert_eq!(bounds(&cache).1, Some(1));
}

#[tokio::test(start_paused = true)]
async fn unconvertible_pages_advance_floor_across_three_interruptions() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 1000);
    let mut rows: Vec<_> = (500..=999)
        .map(|t| {
            let mut r = trade(t, t);
            r["size"] = json!("0");
            r
        })
        .collect();
    rows.push(trade(400, 400));
    let mut venue = HistoryFetcher::new(rows.clone());
    venue.request_budget = 1;
    assert!(!walk(&mut cache, venue, 1000).await);
    assert_eq!(bounds(&cache).1, Some(501));
    // A fixture full page at the boundary forces completion of the entire second,
    // even though every raw row in that second is rejected by conversion.
    let mut pages = std::collections::HashMap::new();
    let zero: Vec<_> = (0..500)
        .map(|i| {
            let mut r = trade(i, 500);
            r["size"] = json!("0");
            r
        })
        .collect();
    pages.insert(request(1, 500, 0), serde_json::to_vec(&zero).unwrap());
    pages.insert(request(500, 500, 0), serde_json::to_vec(&zero).unwrap());
    pages.insert(request(500, 500, 500), b"[]".to_vec());
    assert!(!walk(&mut cache, FixtureFetcher::new(pages), 1000).await);
    assert_eq!(bounds(&cache).1, Some(500));
    assert_eq!(cache.trade_count(), 1);
    let mut venue = HistoryFetcher::new(rows.clone());
    venue.request_budget = 1;
    assert!(!walk(&mut cache, venue, 1000).await);
    assert_eq!(cache.trade_count(), 2);
    assert_eq!(bounds(&cache).1, Some(1));
    assert!(walk(&mut cache, HistoryFetcher::new(rows), 1000).await);
}

#[tokio::test(start_paused = true)]
async fn paired_interval_does_not_claim_unacquired_older_rows_and_resume_gets_600_newer() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    let mut rows: Vec<_> = (9501..=10000).map(|t| trade(t, t)).collect();
    rows.push(trade(9000, 9000));
    let mut venue = HistoryFetcher::new(rows.clone());
    venue.request_budget = 2;
    assert!(!walk(&mut cache, venue, 10000).await);
    assert_eq!(bounds(&cache), (1, Some(9501), Some(10000), None));
    assert_eq!(cache.trade_count(), 500);
    rows.extend((10001..=10600).map(|t| trade(t, t)));
    assert!(walk(&mut cache, HistoryFetcher::new(rows), 10600).await);
    assert_eq!(cache.trade_count(), 1101);
    assert_eq!(bounds(&cache).2, Some(10600));
    assert!(
        cache
            .known_trade_ids(&wallet().to_string())
            .unwrap()
            .iter()
            .any(|t| t.0 == "t9000")
    );
}

#[tokio::test(start_paused = true)]
async fn zero_rows_with_persisted_coverage_and_completed_empty_wallet_still_forward_walk() {
    for floor in [500, 1] {
        let dir = TempDir::new().unwrap();
        let mut cache = setup(&dir);
        set_bounds(&cache, Some(floor), Some(1000));
        // Completed-empty and inactive-to-active are deliberate supported states.
        if floor == 1 {
            cache
                .raw_conn_for_test()
                .execute_batch("UPDATE wallets SET is_active=0, backfill_partial=0")
                .unwrap();
            pile::apply_activation_rules(&mut cache).unwrap();
        }
        let venue = HistoryFetcher::new(vec![trade(1500, 1500)]);
        let req = venue.requests.clone();
        assert!(walk(&mut cache, venue, 2000).await);
        assert_eq!(cache.trade_count(), 1);
        assert!(req.lock().unwrap().contains(&(1001, 1001, 0)));
        assert_eq!(bounds(&cache).2, Some(2000));
    }
}

#[tokio::test(start_paused = true)]
async fn legacy_anchor_insert_branch_cap_and_failed_walk_preserve_gap_repair_start() {
    for absent_wallet_row in [false, true] {
        let dir = TempDir::new().unwrap();
        let mut cache = setup(&dir);
        for t in 1500..2000 {
            insert(&cache, &format!("t{t}"), t);
        }
        insert(&cache, "old", 2000);
        if absent_wallet_row {
            cache
                .raw_conn_for_test()
                .execute_batch("DELETE FROM wallets")
                .unwrap();
        }
        let mut older = HistoryFetcher::new((1000..1500).map(|t| trade(t, t)).collect());
        older.request_budget = 2;
        assert!(!walk(&mut cache, older, 3000).await);
        assert_eq!(bounds(&cache).2, Some(1999));
        assert_eq!(bounds(&cache).1, Some(1000));
        assert_eq!(cache.trade_count(), 1001);
        if absent_wallet_row {
            assert!(cache.active_tradeable_wallet_hexes().unwrap().is_empty());
        }
        for t in 2101..=2600 {
            insert(&cache, &format!("t{t}"), t);
        }
        let rows = (1000..=2600).map(|t| trade(t, t)).collect();
        assert!(walk(&mut cache, HistoryFetcher::new(rows), 3000).await);
        assert_eq!(cache.trade_count(), 1602);
        assert_eq!(bounds(&cache).2, Some(3000));
    }
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "new", 2000);
    let venue = HistoryFetcher::new(vec![trade(900, 900)]);
    let req = venue.requests.clone();
    assert!(walk(&mut cache, venue, 1000).await);
    assert_eq!(bounds(&cache).2, Some(1000));
    assert!(req.lock().unwrap().iter().all(|(_, hi, _)| *hi <= 1000));
}

#[tokio::test(start_paused = true)]
async fn operator_empty_coverage_reset_respects_hi_and_recovers_minimum_second_sibling() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "old", 1000);
    set_bounds(&cache, Some(1), Some(0));
    let rows = vec![trade(900, 900), trade(950, 950), trade(1000, 1000)];
    let venue = HistoryFetcher::new(rows.clone());
    let req = venue.requests.clone();
    assert!(walk(&mut cache, venue, 900).await);
    assert_eq!(bounds(&cache).2, Some(900));
    assert_eq!(bounds(&cache).0, 0);
    assert!(req.lock().unwrap().iter().all(|(_, end, _)| *end <= 900));
    assert_eq!(req.lock().unwrap()[0], (1, 1, 0));
    assert!(walk(&mut cache, HistoryFetcher::new(rows), 1100).await);
    assert_eq!(cache.trade_count(), 4);
    assert_eq!(bounds(&cache).2, Some(1100));
}

#[tokio::test(start_paused = true)]
async fn invalid_full_page_after_commit_and_whole_page_dto_failure_keep_marker() {
    for malformed in [false, true] {
        let dir = TempDir::new().unwrap();
        let mut cache = setup(&dir);
        let mut pages = std::collections::HashMap::new();
        let rows: Vec<_> = (501..=1000).rev().map(|t| trade(t, t)).collect();
        pages.insert(request(1, 1000, 0), serde_json::to_vec(&rows).unwrap());
        pages.insert(
            request(501, 501, 0),
            serde_json::to_vec(&[trade(501, 501)]).unwrap(),
        );
        let mut bad: Vec<_> = (0..500).map(|i| trade(i, i64::MIN)).collect();
        if malformed {
            bad[0]["price"] = json!("malformed");
        }
        pages.insert(request(1, 500, 0), serde_json::to_vec(&bad).unwrap());
        assert!(!walk(&mut cache, FixtureFetcher::new(pages), 1000).await);
        assert_eq!(cache.trade_count(), 500);
        assert_eq!(bounds(&cache), (1, Some(501), Some(1000), None));
    }
}

#[tokio::test(start_paused = true)]
async fn known_ids_read_failure_is_not_an_empty_history() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    cache.raw_conn_for_test().execute("INSERT INTO trades (source_trade_id,wallet_hex,market_id,outcome_id,side,price_str,contracts,timestamp_unix) VALUES (x'ff',?1,'m',0,'buy','0.5',1,100)",[wallet().to_string()]).unwrap();
    assert!(cache.known_trade_ids(&wallet().to_string()).is_err());
    let venue = HistoryFetcher::new(vec![trade(1, 1)]);
    let req = venue.requests.clone();
    assert!(!walk(&mut cache, venue, 1000).await);
    assert!(req.lock().unwrap().is_empty());
    assert_eq!(cache.trade_count(), 1);
    assert_eq!(bounds(&cache).0, 0);
}

#[tokio::test(start_paused = true)]
async fn absent_pile_rows_marker_and_finalization_failures_stamping_on_and_off() {
    for stamp in [false, true] {
        for fail in ["begin", "finish", "none"] {
            let dir = TempDir::new().unwrap();
            let mut cache = setup(&dir);
            cache
                .raw_conn_for_test()
                .execute_batch("DELETE FROM wallets")
                .unwrap();
            if fail == "begin" {
                cache.raw_conn_for_test().execute_batch("CREATE TRIGGER fail BEFORE INSERT ON wallets BEGIN SELECT RAISE(ABORT,'begin'); END;").unwrap();
            }
            if fail == "finish" {
                cache.raw_conn_for_test().execute_batch("CREATE TRIGGER fail BEFORE UPDATE OF backfill_partial ON wallets WHEN NEW.backfill_partial=0 BEGIN SELECT RAISE(ABORT,'finish'); END;").unwrap();
            }
            let outcome = PolymarketBulkFetcher::new(
                BASE.to_owned(),
                HistoryFetcher::new(vec![trade(1, 900)]),
            )
            .with_clock_for_test(|| 1120)
            .with_stamp_on_success(stamp)
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap();
            assert_eq!(outcome.failed.is_empty(), fail == "none");
            assert!(cache.active_tradeable_wallet_hexes().unwrap().is_empty());
            drop(cache);
            let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
            assert_eq!(cache.trade_count(), usize::from(fail != "begin"));
            if fail != "begin" {
                assert_eq!(bounds(&cache).0, i64::from(fail == "finish"));
                assert_eq!(bounds(&cache).3, (stamp && fail == "none").then_some(1120));
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn settled_head_probe_and_completed_empty_probe_preserve_accepted_verdicts() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    let mut rows: Vec<_> = (0..500).map(|i| trade(i, 10000 - i * 10)).collect();
    rows.extend((0..500).map(|i| trade(1000 + i, 10160 - i / 10)));
    // The unbounded head would have a 49s span. At hi=10000 it is sparse.
    let outcome = PolymarketBulkFetcher::new(BASE.to_owned(), HistoryFetcher::new(rows))
        .with_clock_for_test(|| 10120)
        .with_stamp_on_success(true)
        .fetch_all(&[wallet()], &mut cache)
        .await
        .unwrap();
    assert!(outcome.failed.is_empty());
    assert_eq!(cache.trade_count(), 500);
    assert_eq!(cache.active_tradeable_wallet_hexes().unwrap().len(), 1);
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    assert!(walk(&mut cache, HistoryFetcher::new(vec![]), 1000).await);
    assert_eq!(bounds(&cache), (0, Some(1), Some(1000), Some(1120)));
    let outcome = PolymarketBulkFetcher::new(
        BASE.to_owned(),
        HistoryFetcher::new((0..500).map(|i| trade(i, 2000 - i)).collect()),
    )
    .with_clock_for_test(|| 2120)
    .with_stamp_on_success(true)
    .fetch_all(&[wallet()], &mut cache)
    .await
    .unwrap();
    assert!(outcome.failed.is_empty());
    assert_eq!(cache.trade_count(), 0);
    assert_eq!(bounds(&cache).3, Some(1120));
    assert!(cache.active_tradeable_wallet_hexes().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn sparse_partial_then_dense_retry_keeps_rows_and_does_not_repeat_cold_probe() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    let mut rows: Vec<_> = (0..500).map(|i| trade(i, 10000 - i * 10)).collect();
    let mut first = HistoryFetcher::new(rows.clone());
    first.request_budget = 2;
    let outcome = PolymarketBulkFetcher::new(BASE.to_owned(), first)
        .with_clock_for_test(|| 10120)
        .with_wallet_timeout(1)
        .fetch_all(&[wallet()], &mut cache)
        .await
        .unwrap();
    assert_eq!(outcome.failed, [wallet()]);
    assert_eq!(cache.trade_count(), 500);
    rows.extend((0..500).map(|i| trade(1000 + i, 11000 - i)));
    let outcome = PolymarketBulkFetcher::new(BASE.to_owned(), HistoryFetcher::new(rows))
        .with_clock_for_test(|| 11120)
        .fetch_all(&[wallet()], &mut cache)
        .await
        .unwrap();
    assert!(outcome.failed.is_empty());
    assert_eq!(cache.trade_count(), 1000);
    assert_eq!(cache.active_tradeable_wallet_hexes().unwrap().len(), 1);
}

#[test]
fn retroactive_infra_ignores_partial_middle_slice_in_preview_and_apply() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    for t in 10000..10500 {
        insert(&cache, &format!("middle{t}"), t);
    }
    set_bounds(&cache, Some(10000), Some(10499));
    for preview in [true, false] {
        let result = cache.classify_infra_retroactive(3600, preview).unwrap();
        assert_eq!((result.scanned, result.flagged), (0, 0));
        assert_eq!(
            pile::select_backfill_due(&cache, 20000, 0).unwrap(),
            [wallet().to_string()]
        );
    }
    for i in 0..500 {
        insert(&cache, &format!("old{i}"), 1000 + i * 10);
    }
    cache
        .raw_conn_for_test()
        .execute_batch("UPDATE wallets SET backfill_partial=0")
        .unwrap();
    assert_eq!(
        cache
            .classify_infra_retroactive(3600, false)
            .unwrap()
            .flagged,
        0
    );
    // Existing completed dense behavior remains unchanged.
    cache
        .raw_conn_for_test()
        .execute_batch("DELETE FROM trades WHERE source_trade_id LIKE 'old%'")
        .unwrap();
    assert_eq!(
        cache
            .classify_infra_retroactive(3600, true)
            .unwrap()
            .flagged,
        1
    );
    assert_eq!(
        cache
            .classify_infra_retroactive(3600, false)
            .unwrap()
            .flagged,
        1
    );
    assert!(cache.active_tradeable_wallet_hexes().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn frozen_clock_bound_does_not_move_with_requests_and_stamp_uses_same_clock_owner() {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    struct Advancing {
        inner: HistoryFetcher,
        clock: Arc<AtomicI64>,
    }
    impl PageFetcher for Advancing {
        async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, pe_source_core::SourceError> {
            self.clock.store(3120, Ordering::SeqCst);
            self.inner.fetch_page(url).await
        }
    }
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    insert(&cache, "seed", 100);
    set_bounds(&cache, Some(1), Some(100));
    let clock = Arc::new(AtomicI64::new(2120));
    let reader = clock.clone();
    let venue = HistoryFetcher::new(vec![trade(1500, 1500), trade(2100, 2100)]);
    let req = venue.requests.clone();
    let result = PolymarketBulkFetcher::new(
        BASE.to_owned(),
        Advancing {
            inner: venue,
            clock,
        },
    )
    .with_clock_for_test(move || reader.load(Ordering::SeqCst))
    .with_stamp_on_success(true)
    .fetch_all(&[wallet()], &mut cache)
    .await
    .unwrap();
    assert!(result.failed.is_empty());
    assert_eq!(cache.trade_count(), 2);
    assert_eq!(bounds(&cache), (0, Some(1), Some(2000), Some(3120)));
    assert!(req.lock().unwrap().iter().all(|(_, end, _)| *end <= 2000));
    assert!(
        walk(
            &mut cache,
            HistoryFetcher::new(vec![trade(2100, 2100)]),
            3000
        )
        .await
    );
    assert_eq!(cache.trade_count(), 3);
}

#[tokio::test(start_paused = true)]
async fn finalization_fault_preserves_prior_stamp_on_and_off_then_reopen_completes() {
    for stamp in [false, true] {
        for prior in [None, Some(700)] {
            let dir = TempDir::new().unwrap();
            let mut cache = setup(&dir);
            if let Some(prior) = prior {
                cache
                    .update_last_polymarket_fetch(&wallet().to_string(), prior)
                    .unwrap();
            }
            cache.raw_conn_for_test().execute_batch("CREATE TRIGGER fail_finish BEFORE UPDATE OF backfill_partial ON wallets WHEN NEW.backfill_partial=0 BEGIN SELECT RAISE(ABORT,'finish'); END").unwrap();
            let result = PolymarketBulkFetcher::new(
                BASE.to_owned(),
                HistoryFetcher::new(vec![trade(900, 900)]),
            )
            .with_clock_for_test(|| 1120)
            .with_stamp_on_success(stamp)
            .fetch_all(&[wallet()], &mut cache)
            .await
            .unwrap();
            assert_eq!(result.failed, [wallet()]);
            drop(cache);
            let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
            assert_eq!(bounds(&cache), (1, Some(1), Some(1000), prior));
            assert_eq!(cache.trade_count(), 1);
            cache
                .raw_conn_for_test()
                .execute_batch("DROP TRIGGER fail_finish")
                .unwrap();
            let result = PolymarketBulkFetcher::new(BASE.to_owned(), HistoryFetcher::new(vec![]))
                .with_clock_for_test(|| 1120)
                .with_stamp_on_success(stamp)
                .fetch_all(&[wallet()], &mut cache)
                .await
                .unwrap();
            assert!(result.failed.is_empty());
            drop(cache);
            let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
            assert_eq!(
                bounds(&cache),
                (
                    0,
                    Some(1),
                    Some(1000),
                    if stamp { Some(1120) } else { prior }
                )
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cold_all_unconvertible_full_page_still_acquires_valid_older_history() {
    let dir = TempDir::new().unwrap();
    let mut cache = setup(&dir);
    let mut rows: Vec<_> = (501..=1000)
        .map(|t| {
            let mut r = trade(t, t);
            r["size"] = json!("0");
            r
        })
        .collect();
    rows.push(trade(400, 400));
    assert!(walk(&mut cache, HistoryFetcher::new(rows), 1000).await);
    assert_eq!(cache.trade_count(), 1);
    assert_eq!(bounds(&cache).0, 0);
    assert_eq!(
        cache.known_trade_ids(&wallet().to_string()).unwrap()[0].0,
        "t400"
    );
}
