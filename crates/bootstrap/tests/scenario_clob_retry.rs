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
use pe_bootstrap::error::BootstrapError;
use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use tempfile::TempDir;

/// How the fetcher fails for its first `fail_n` calls.
#[derive(Clone, Copy)]
enum FailKind {
    /// Network/decode blip — retryable with backoff.
    Transient,
    /// 4xx — non-retryable, aborts immediately.
    Fatal,
    /// HTTP 429 — retryable, waits `retry_after`.
    RateLimited,
}

/// A `PageFetcher` whose first `fail_n` calls fail with `kind`; thereafter it
/// serves `body`. `calls` is shared so the test can assert how many fetch
/// attempts the walk made.
struct FlakyFetcher {
    fail_n: u32,
    kind: FailKind,
    calls: Arc<AtomicU32>,
    body: Vec<u8>,
}

impl PageFetcher for FlakyFetcher {
    async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_n {
            return Err(match self.kind {
                FailKind::Transient => SourceError::Transient {
                    message: "error decoding response body".to_owned(),
                },
                FailKind::Fatal => SourceError::Fatal {
                    message: "http 404".to_owned(),
                },
                FailKind::RateLimited => SourceError::RateLimited {
                    retry_after_secs: 2,
                },
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
        kind: FailKind::Transient,
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

// PASS: a page that returns HTTP 429 twice (within the budget) then succeeds ⇒
//       the walk waits `retry_after` and completes.
// FAIL: a RateLimited response aborts the walk.
#[tokio::test(start_paused = true)]
async fn clob_walk_rides_through_rate_limited() {
    let (_dir, mut cache) = open_cache();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = FlakyFetcher {
        fail_n: 2,
        kind: FailKind::RateLimited,
        calls: Arc::clone(&calls),
        body: one_page_body(),
    };
    let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

    let report = clob.fetch_closed_markets(&mut cache).await.unwrap();
    assert_eq!(
        report.resolutions, 1,
        "walk survived 2 rate-limited responses"
    );
    assert_eq!(report.tokens_mapped, 2);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "2 rate-limited responses + 1 success"
    );
    println!("PASS: CLOB walk rode through 2 RateLimited responses (honoured retry_after)");
}

// PASS: a persistently transient page aborts after the retry budget with the
//       typed temporary error (`TransientSource`, exit 75), exactly six fetch
//       attempts (1 initial + CLOB_PAGE_MAX_RETRIES), the "(after N page
//       retries)" suffix, and an unchanged seeded page cursor.
// FAIL: any other variant/exit code, a different attempt count, or a mutated
//       cursor (#534: transience must survive exhaustion so the loop
//       supervisor retries instead of stopping).
#[tokio::test(start_paused = true)]
async fn clob_walk_exhausted_transient_is_tempfail_and_preserves_cursor() {
    let (_dir, mut cache) = open_cache();
    cache
        .set_source_cursor("clob_closed", "mid-walk-cursor")
        .unwrap();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = FlakyFetcher {
        fail_n: u32::MAX,
        kind: FailKind::Transient,
        calls: Arc::clone(&calls),
        body: Vec::new(),
    };
    let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

    let err = clob
        .fetch_closed_markets(&mut cache)
        .await
        .expect_err("a persistently transient page must abort after the retry budget");
    assert!(
        matches!(
            err,
            BootstrapError::TransientSource {
                source_name: "polymarket-clob",
                ..
            }
        ),
        "expected TransientSource from polymarket-clob, got {err:?}"
    );
    assert!(
        err.to_string().ends_with("(after 5 page retries)"),
        "message must preserve the retry-count suffix: {err}"
    );
    assert_eq!(
        err.exit_code(),
        BootstrapError::TEMPFAIL_EXIT_CODE,
        "exhausted transience must map to the supervised tempfail exit"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        6,
        "1 initial attempt + 5 budgeted retries"
    );
    assert_eq!(
        cache.get_source_cursor("clob_closed").as_deref(),
        Some("mid-walk-cursor"),
        "the failed page's cursor must be preserved for resume"
    );
    println!(
        "PASS: exhausted-transient CLOB walk returns TransientSource (exit 75), \
         6 attempts, cursor preserved"
    );
}

// PASS: a `Fatal` (4xx) error aborts immediately with exactly one fetch
//       attempt, as the permanent `Clob` variant (exit 1).
// FAIL: a Fatal error is retried, or maps to the temporary lane.
#[tokio::test(start_paused = true)]
async fn clob_walk_aborts_immediately_on_fatal() {
    let (_dir, mut cache) = open_cache();
    let calls = Arc::new(AtomicU32::new(0));
    let fetcher = FlakyFetcher {
        fail_n: u32::MAX,
        kind: FailKind::Fatal,
        calls: Arc::clone(&calls),
        body: Vec::new(),
    };
    let clob = ClobFetcher::new("https://clob.example".to_owned(), fetcher);

    let err = clob
        .fetch_closed_markets(&mut cache)
        .await
        .expect_err("a Fatal fetch error must abort the walk");
    assert!(
        matches!(err, BootstrapError::Clob { .. }),
        "fatal fetch failures stay the permanent Clob variant, got {err:?}"
    );
    assert_eq!(err.exit_code(), 1, "fatal failures stay exit 1");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "Fatal must abort immediately — never retried"
    );
    println!("PASS: CLOB walk aborts immediately on a Fatal error (Clob, exit 1, no retry)");
}
