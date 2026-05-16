//! Scenario tests for per-wallet incremental stamping (issue #175).
//!
//! Verifies that `with_stamp_on_success(true)` writes `last_polymarket_fetch_at`
//! inline per-wallet, so a mid-run interruption preserves all completed work
//! and `select_backfill_due` reflects real-time progress.
//!
//! Each scenario has a single PASS/FAIL criterion stated before the test body.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::pile::{self, SRC_LEADERBOARD};
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use tempfile::TempDir;

const BASE_URL: &str = "https://data-api.polymarket.com";

fn wallet_hex(byte: u8) -> String {
    format!("0x{:040x}", byte)
}

fn wallet(byte: u8) -> WalletAddress {
    WalletAddress::from_hex(&wallet_hex(byte)).unwrap()
}

fn trade_url_cold(w: WalletAddress) -> String {
    PolymarketEndpoint::UserTradeActivity {
        user: w.to_string(),
        end: None,
        start: None,
    }
    .url(BASE_URL)
}

fn page_json(trades: &[(&str, &str, i64)]) -> Vec<u8> {
    let mut s = String::from("[");
    for (i, (tid, mid, ts)) in trades.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            r#"{{"transactionHash":"{tid}","conditionId":"{mid}","outcomeIndex":0,"side":"BUY","price":"0.5","size":"1","timestamp":{ts}}}"#
        ));
    }
    s.push(']');
    s.into_bytes()
}

// ── Scenario 1: succeeded wallet is stamped inline, never re-queued ────────────
//
// PASS: After a single fetch_all run with `with_stamp_on_success(true)`, the
//       successfully fetched wallet's `last_polymarket_fetch_at` is non-NULL
//       and `select_backfill_due` no longer returns it.
// FAIL: stamp stays NULL despite a successful fetch (the legacy post-loop
//       stamping was the only writer; nothing inside fetch_all wrote it).

#[tokio::test]
async fn scenario_succeeded_wallet_stamped_inline() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let w = wallet(0xaa);

    cache
        .upsert_wallet(&w.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    let now_unix = 1_700_000_000_i64;
    let due_before = pile::select_backfill_due(&cache, now_unix, 0).unwrap();
    assert_eq!(due_before.len(), 1);

    let trade_id = format!("0x{:064x}", 1);
    let market_id = format!("0xm{:063x}", 1);
    let trades = vec![(trade_id.as_str(), market_id.as_str(), 1_699_999_999_i64)];
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(w), page_json(&trades));

    let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_stamp_on_success(true);
    let outcome = bulk.fetch_all(&[w], &mut cache).await.unwrap();
    assert!(outcome.failed.is_empty());

    // Operator did NOT run a post-fetch stamp loop. Inline stamping must have
    // landed the timestamp during fetch_all itself.
    let due_after = pile::select_backfill_due(&cache, now_unix, 0).unwrap();
    assert!(
        due_after.is_empty(),
        "inline stamp must remove the wallet from the next due queue"
    );
}

// ── Scenario 2: mixed batch — succeeded wallets are stamped, failed ones aren't ──
//
// PASS: In a 2-wallet batch where one wallet has a fixture and one doesn't,
//       only the fixture-backed wallet is removed from the due queue. The
//       no-fixture wallet stays NULL → re-queued on the next backfill.
// FAIL: both wallets are removed (failed wallet falsely stamped) OR the
//       successful wallet stays NULL (inline stamp didn't fire).

#[tokio::test]
async fn scenario_mixed_batch_only_stamps_succeeded() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let ok_w = wallet(0xbb);
    let fail_w = wallet(0xcc);

    cache
        .upsert_wallet(&ok_w.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    cache
        .upsert_wallet(
            &fail_w.to_string(),
            SRC_LEADERBOARD,
            false,
            None,
            None,
            None,
        )
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    let trade_id = format!("0x{:064x}", 2);
    let market_id = format!("0xm{:063x}", 2);
    let trades = vec![(trade_id.as_str(), market_id.as_str(), 1_699_999_999_i64)];
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(ok_w), page_json(&trades));
    // fail_w has no fixture → FixtureFetcher returns Fatal.

    let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_stamp_on_success(true);
    let outcome = bulk.fetch_all(&[ok_w, fail_w], &mut cache).await.unwrap();

    assert_eq!(outcome.failed, vec![fail_w]);

    let due = pile::select_backfill_due(&cache, 1_700_000_000, 0).unwrap();
    assert_eq!(
        due,
        vec![fail_w.to_string()],
        "only the failed wallet must be re-queued; succeeded wallet stays stamped"
    );
}

// ── Scenario 3: SIGINT-mid-run preserves completed wallets ────────────────────
//
// PASS: After fetching wallet A successfully, a SECOND call to fetch_all over
//       both wallets [A, B] (simulating the "supervisor relaunches after
//       SIGINT" case) only re-fetches B. Wallet A's API would not need to be
//       called — verified by removing its fixture from the second call's
//       fetcher and confirming the second run still completes without
//       failure (because select_backfill_due in a real flow would skip A).
// FAIL: A's stamp wasn't written by the first call → second call would re-
//       fetch A and either succeed (if fixture available) or fail.
//
// We approximate the "supervisor pattern" by: run #1 fetches A only, stamps A
// inline. Re-query `select_backfill_due`. Only B should be due. The operator's
// next invocation runs fetch_all over just the due wallets.

#[tokio::test]
async fn scenario_sigint_mid_run_preserves_completed_via_inline_stamp() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let a = wallet(0xdd);
    let b = wallet(0xee);

    cache
        .upsert_wallet(&a.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    cache
        .upsert_wallet(&b.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    // Run #1: only A's fixture is set; the run completes before reaching B.
    // (Simulating the case where SIGINT interrupted before B was processed.)
    let trade_id = format!("0x{:064x}", 3);
    let market_id = format!("0xm{:063x}", 3);
    let trades = vec![(trade_id.as_str(), market_id.as_str(), 1_699_999_999_i64)];
    let mut responses_1 = HashMap::new();
    responses_1.insert(trade_url_cold(a), page_json(&trades));

    let bulk_1 = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses_1))
        .with_stamp_on_success(true);
    bulk_1.fetch_all(&[a], &mut cache).await.unwrap();

    // Supervisor relaunches: re-query due wallets. With inline stamping, A is
    // gone from the queue, only B remains. Pre-#175 behaviour would have lost
    // A's stamp (because the post-loop never ran on SIGINT) → both would be due.
    let due_after_partial = pile::select_backfill_due(&cache, 1_700_000_000, 0).unwrap();
    assert_eq!(
        due_after_partial,
        vec![b.to_string()],
        "post-SIGINT supervisor must see A as already-stamped (preserves prior work)"
    );

    // Run #2: process the remaining due wallet B.
    let trade_id_b = format!("0x{:064x}", 4);
    let market_id_b = format!("0xm{:063x}", 4);
    let trades_b = vec![(trade_id_b.as_str(), market_id_b.as_str(), 1_699_999_998_i64)];
    let mut responses_2 = HashMap::new();
    responses_2.insert(trade_url_cold(b), page_json(&trades_b));

    let bulk_2 = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses_2))
        .with_stamp_on_success(true);
    bulk_2.fetch_all(&[b], &mut cache).await.unwrap();

    let due_final = pile::select_backfill_due(&cache, 1_700_000_000, 0).unwrap();
    assert!(due_final.is_empty(), "both wallets stamped");
}
