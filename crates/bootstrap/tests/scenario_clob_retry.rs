#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: the CLOB closed-markets walk rides through transient fetch errors
//! instead of aborting the whole pagination (issue #429 follow-up).
//!
//! `ReqwestFetcher` retries fast transient blips internally; this guards the
//! *walk-level* retry that survives sustained flakiness — a single transient
//! `error decoding response body` on one page previously killed the entire
//! ~1,457-page re-walk and propagated a fatal exit.
//!
//! Deterministic: a custom in-process `PageFetcher` (no network); the retry
//! backoff sleeps are virtual via `#[tokio::test(start_paused = true)]`, so the
//! tests assert real retry behaviour without real wall-clock delay.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::clob::ClobFetcher;
use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use tempfile::TempDir;

/// A `PageFetcher` whose first `fail_n` calls fail; thereafter it serves `body`.
/// `fatal = true` fails with the non-retryable `Fatal` variant (a 4xx); else
/// `Transient` (network/decode). `calls` is shared so the test can assert how
/// many fetch attempts the walk made.
struct FlakyFetcher {
    fail_n: u32,
    fatal: bool,
    calls: Arc<AtomicU32>,
    body: Vec<u8>,
}

impl PageFetcher for FlakyFetcher {
    async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_n {
            let message = "error decoding response body".to_owned();
            return Err(if self.fatal {
                SourceError::Fatal { message }
            } else {
                SourceError::Transient { message }
            });
        }
        Ok(self.body.clone())
    }
}

/// One closed binary market, terminating cursor.
fn one_page_body() -> Vec<u8> {
    br#"{"data":[{"condition_id":"0xc","end_date_iso":"2024-01-15T00:00:00Z","closed":true,"tokens":[{"token_id":"T0","winner":true},{"token_id":"T1","winner":false}]}],"next_cursor":"LTE="}"#.to_vec()
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

// PASS: a page that fails transiently twice (within the retry budget) then
//       succeeds ⇒ the walk completes and maps the page.
// FAIL: the walk aborts on the first transient error.
#[tokio::test(start_paused = true)]
async fn clob_walk_rides_through_transient_errors() {
    let (_dir, mut cache) = open_cache();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = FlakyFetcher {
        fail_n: 2,
        fatal: false,
        calls: Arc::clone(&calls),
        body: one_page_body(),
    };
    let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

    let report = clob.fetch_closed_markets(&mut cache).await.unwrap();
    assert_eq!(report.resolutions, 1, "walk survived 2 transient errors");
    assert_eq!(report.tokens_mapped, 2);
    assert_eq!(
        cache.token_condition_outcome("T0"),
        Some(("0xc".to_owned(), Some(0)))
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "2 transient failures + 1 success"
    );
    println!("PASS: CLOB walk rode through 2 transient fetch errors and mapped the page");
}

// PASS: a persistently transient page aborts (Err) after the retry budget — but
//       only after retrying more than the single initial attempt.
// FAIL: it aborts on the first attempt, or never aborts.
#[tokio::test(start_paused = true)]
async fn clob_walk_aborts_after_exhausting_retries() {
    let (_dir, mut cache) = open_cache();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = FlakyFetcher {
        fail_n: u32::MAX,
        fatal: false,
        calls: Arc::clone(&calls),
        body: Vec::new(),
    };
    let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

    let res = clob.fetch_closed_markets(&mut cache).await;
    assert!(
        res.is_err(),
        "a persistently transient page must abort after the retry budget"
    );
    assert!(
        calls.load(Ordering::SeqCst) > 1,
        "must have retried before aborting (not a single attempt)"
    );
    println!("PASS: CLOB walk aborts after exhausting page-level retries");
}

// PASS: a `Fatal` (4xx) error aborts immediately with exactly one fetch attempt.
// FAIL: a Fatal error is retried.
#[tokio::test(start_paused = true)]
async fn clob_walk_aborts_immediately_on_fatal() {
    let (_dir, mut cache) = open_cache();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = FlakyFetcher {
        fail_n: u32::MAX,
        fatal: true,
        calls: Arc::clone(&calls),
        body: Vec::new(),
    };
    let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

    let res = clob.fetch_closed_markets(&mut cache).await;
    assert!(res.is_err(), "a Fatal fetch error must abort the walk");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "Fatal must abort immediately — never retried"
    );
    println!("PASS: CLOB walk aborts immediately on a Fatal error (no retry)");
}
