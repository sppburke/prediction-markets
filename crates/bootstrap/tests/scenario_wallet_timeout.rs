//! Scenario tests for the per-wallet Polymarket fetch timeout (issue #173).
//!
//! Operator-level checks that the timeout converts "stuck on a giant wallet"
//! into "fail-soft and move on" without corrupting the wallet pile's bookkeeping.
//! No network calls; deterministic.
//!
//! Each scenario has a single PASS/FAIL criterion stated before the test body.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::time::Duration;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::pile::{self, SRC_LEADERBOARD};
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::WalletAddress;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{FixtureFetcher, PageFetcher, PolymarketEndpoint};
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

/// `PageFetcher` that returns a pending future, simulating the indefinite
/// rate-limit-retry loop that 173 is built to bound.
struct HangFetcher;

impl PageFetcher for HangFetcher {
    async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
        std::future::pending::<Result<Vec<u8>, SourceError>>().await
    }
}

/// Delegates to `FixtureFetcher` for known URLs; hangs forever for unknown ones.
/// Drives the "fast wallet succeeds, slow wallet times out" operator flow.
struct PartialHangFetcher {
    inner: FixtureFetcher,
}

impl PartialHangFetcher {
    fn new(inner: FixtureFetcher) -> Self {
        Self { inner }
    }
}

impl PageFetcher for PartialHangFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        match self.inner.fetch_page(url).await {
            Ok(b) => Ok(b),
            Err(_) => std::future::pending::<Result<Vec<u8>, SourceError>>().await,
        }
    }
}

// ── Scenario 1: timeout fires, wallet stays NULL, next select_backfill_due picks it up ──
//
// PASS: A wallet whose fetcher hangs forever times out under
//       `with_wallet_timeout(1)`, gets soft-failed (no stamp), and is returned
//       again by `select_backfill_due` on the next run. Cache contains no
//       trades for the hung wallet (cancellation-safety invariant).
// FAIL: wallet is stamped despite timing out (lost re-queue), OR cache has
//       partial trades for the hung wallet, OR test exceeds the 5 s budget.

#[tokio::test]
async fn scenario_timed_out_wallet_is_requeued_on_next_run() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let w = wallet(0xaa);

    // Insert as an active leaderboard wallet; activation makes it eligible for
    // `select_backfill_due`.
    cache
        .upsert_wallet(&w.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    let now = 1_700_000_000_i64;
    let due_before = pile::select_backfill_due(&cache, now, 0).unwrap();
    assert_eq!(due_before.len(), 1, "wallet must be due before run");

    // Drive the fetch with a HangFetcher; timeout fires after 1s.
    let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), HangFetcher).with_wallet_timeout(1);

    let start = std::time::Instant::now();
    let outcome = bulk.fetch_all(&[w], &mut cache).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(outcome.failed, vec![w], "hung wallet must be soft-failed");
    assert_eq!(outcome.succeeded_count(), 0);
    assert!(
        elapsed < Duration::from_secs(5),
        "timeout did not free the queue tail; elapsed = {elapsed:?}"
    );

    // Cache integrity: no trades, no funder edges written from the cancelled walk.
    let ids = cache.known_trade_ids(&w.to_string());
    assert!(
        ids.is_empty(),
        "cancellation-safety: no partial cache writes"
    );

    // Operator post-fetch step: stamp only succeeded wallets. The hung wallet
    // stays NULL, so the next `select_backfill_due` re-queues it.
    let failed_set: std::collections::HashSet<WalletAddress> =
        outcome.failed.iter().copied().collect();
    for wallet in &[w] {
        if failed_set.contains(wallet) {
            continue;
        }
        pile::update_last_polymarket_fetch(&mut cache, &wallet.to_string(), now).unwrap();
    }

    let due_after = pile::select_backfill_due(&cache, now, 0).unwrap();
    assert_eq!(
        due_after,
        vec![w.to_string()],
        "timed-out wallet must be re-queued on next backfill (NULL fetch timestamp preserved)"
    );
}

// ── Scenario 2: mixed batch, only the hung wallet is soft-failed, queue tail freed ──
//
// PASS: With a 2-wallet batch (one fast fixture, one hung), the fast wallet's
//       trades are persisted and stamped; the hung wallet is soft-failed and
//       returned to the next `select_backfill_due`; the overall wall-clock is
//       bounded by the timeout (≤ 5 s) not the hang.
// FAIL: fast wallet's trades or stamp missing, OR slow wallet wrongly stamped,
//       OR overall wall-clock exceeds the timeout budget.

#[tokio::test]
async fn scenario_mixed_batch_timeout_isolates_stragglers() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let fast = wallet(0xbb);
    let slow = wallet(0xcc);

    cache
        .upsert_wallet(&fast.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    cache
        .upsert_wallet(&slow.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    // Fixture only for fast; slow's URL falls through to pending().
    let trade_id = format!("0x{:064x}", 1);
    let market_id = format!("0xm{:063x}", 1);
    let trades = vec![(trade_id.as_str(), market_id.as_str(), 1_699_999_999_i64)];
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(fast), page_json(&trades));

    let fetcher = PartialHangFetcher::new(FixtureFetcher::new(responses));
    let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher).with_wallet_timeout(1);

    let now = 1_700_000_000_i64;

    let start = std::time::Instant::now();
    let outcome = bulk.fetch_all(&[fast, slow], &mut cache).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(outcome.attempted, 2);
    assert_eq!(outcome.failed, vec![slow], "only the hung wallet times out");
    assert_eq!(outcome.succeeded_count(), 1);
    assert!(
        elapsed < Duration::from_secs(5),
        "queue tail not freed by timeout; elapsed = {elapsed:?}"
    );

    // Verify fast wallet got its trade persisted; slow has none.
    let fast_ids = cache.known_trade_ids(&fast.to_string());
    let slow_ids = cache.known_trade_ids(&slow.to_string());
    assert_eq!(
        fast_ids.len(),
        1,
        "fast wallet must have its trade persisted"
    );
    assert!(
        slow_ids.is_empty(),
        "slow wallet must have no cached trades"
    );

    // Operator post-fetch: stamp only the successful wallet.
    let failed_set: std::collections::HashSet<WalletAddress> =
        outcome.failed.iter().copied().collect();
    for w in &[fast, slow] {
        if failed_set.contains(w) {
            continue;
        }
        pile::update_last_polymarket_fetch(&mut cache, &w.to_string(), now).unwrap();
    }

    // Next backfill: only the hung wallet should still be due.
    let due_next = pile::select_backfill_due(&cache, now, 0).unwrap();
    assert_eq!(
        due_next,
        vec![slow.to_string()],
        "next backfill must re-queue only the timed-out wallet"
    );
}

// ── Scenario 3: timeout=0 (disabled) preserves the legacy "no skip" guarantee ──
//
// PASS: With `with_wallet_timeout(0)`, a fixture-backed wallet completes
//       normally; the disabled path is a clean no-op for the success flow.
// FAIL: timeout fires anyway when explicitly disabled.

#[tokio::test]
async fn scenario_timeout_disabled_does_not_interfere_with_success() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let w = wallet(0xdd);

    cache
        .upsert_wallet(&w.to_string(), SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    let trade_id = format!("0x{:064x}", 2);
    let market_id = format!("0xm{:063x}", 2);
    let trades = vec![(trade_id.as_str(), market_id.as_str(), 1_699_999_999_i64)];
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(w), page_json(&trades));

    let bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_wallet_timeout(0);
    let outcome = bulk.fetch_all(&[w], &mut cache).await.unwrap();

    assert!(
        outcome.failed.is_empty(),
        "disabled timeout must not soft-fail"
    );
    assert_eq!(outcome.succeeded_count(), 1);
    assert_eq!(cache.known_trade_ids(&w.to_string()).len(), 1);
}
