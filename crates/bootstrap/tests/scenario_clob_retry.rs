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

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pe_bootstrap::BootstrapConfig;
use pe_bootstrap::cache::{CACHE_SCHEMA_VERSION_V2, WalletCache};
use pe_bootstrap::clob::ClobFetcher;
use pe_bootstrap::error::BootstrapError;
use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use tempfile::TempDir;

const HALF: &str =
    include_str!("../../source-polymarket-public/tests/fixtures/clob_market_5050.json");
const WINNER: &str =
    include_str!("../../source-polymarket-public/tests/fixtures/clob_market_winner.json");

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

/// A schema-two cache, as the payout walk runs on: the legacy owners are gone.
fn open_v2_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    {
        let cache = WalletCache::open(&path).unwrap();
        cache
            .raw_conn_for_test()
            .execute_batch(
                "DROP TABLE trades; DROP TABLE market_resolutions; DROP TABLE source_cursor;",
            )
            .unwrap();
        cache
            .raw_conn_for_test()
            .pragma_update(None, "user_version", CACHE_SCHEMA_VERSION_V2)
            .unwrap();
    }
    let cache = WalletCache::open(&path).unwrap();
    assert_eq!(cache.schema_version().unwrap(), CACHE_SCHEMA_VERSION_V2);
    (dir, cache)
}

/// A closed-markets page holding one recorded market and the given next cursor.
fn payout_page(market: &str, next_cursor: &str) -> Vec<u8> {
    format!(r#"{{"data":[{market}],"next_cursor":"{next_cursor}"}}"#).into_bytes()
}

/// A `PageFetcher` that serves its scripted replies in order, then fails
/// transiently, recording every requested URL.
struct ScriptedFetcher {
    replies: Mutex<VecDeque<Vec<u8>>>,
    urls: Arc<Mutex<Vec<String>>>,
}

impl PageFetcher for ScriptedFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.urls.lock().unwrap().push(url.to_owned());
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| SourceError::Transient {
                message: "error decoding response body".to_owned(),
            })
    }
}

/// An in-process HTTP/1.1 server that waits `delay` before answering every
/// request with `body`. Write failures (an abandoned request) are ignored.
fn start_slow_server(delay: Duration, body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming().take(64) {
            let Ok(mut stream) = stream else { continue };
            let mut request = [0u8; 4_096];
            let _ = stream.read(&mut request);
            thread::sleep(delay);
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    format!("http://{address}")
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

// PASS: a schema-two payout walk commits page one, then exhausts its page
//       retries on page two (1 + 6 fetches, 31 s of virtual backoff,
//       TransientSource, exit 75) and keeps its v2 checkpoint (generation, next
//       cursor, page ordinal); a fresh walk resumes at that cursor and installs
//       the coverage manifest for the same generation.
// FAIL: the exhausted walk loses or advances its checkpoint, or the resume
//       restarts at page one (#788).
#[tokio::test(start_paused = true)]
async fn schema_two_payout_walk_keeps_its_checkpoint_through_retry_exhaustion() {
    let (_dir, mut cache) = open_v2_cache();
    let urls = Arc::new(Mutex::new(Vec::new()));
    let first = ClobFetcher::new(
        "https://clob.example".to_owned(),
        ScriptedFetcher {
            replies: Mutex::new(VecDeque::from([payout_page(HALF, "c1")])),
            urls: Arc::clone(&urls),
        },
    );
    let started = tokio::time::Instant::now();
    let err = first
        .fetch_closed_markets(&mut cache)
        .await
        .expect_err("page two fails transiently on every try");
    let backoff = started.elapsed();
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
    assert_eq!(err.exit_code(), BootstrapError::TEMPFAIL_EXIT_CODE);
    assert_eq!(
        urls.lock().unwrap().len(),
        7,
        "page one, then 1 initial attempt + 5 page retries on page two"
    );
    assert!(
        backoff >= Duration::from_secs(31) && backoff < Duration::from_secs(32),
        "page backoff 1+2+4+8+16 s, got {backoff:?}"
    );
    let state = cache
        .clob_payout_walk_state_v2()
        .unwrap()
        .expect("the walk state survives exhaustion");
    assert_eq!(state.next_cursor.as_deref(), Some("c1"));
    assert_eq!(state.next_page_ordinal, 1);

    let resumed_urls = Arc::new(Mutex::new(Vec::new()));
    let second = ClobFetcher::new(
        "https://clob.example".to_owned(),
        ScriptedFetcher {
            replies: Mutex::new(VecDeque::from([payout_page(WINNER, "LTE=")])),
            urls: Arc::clone(&resumed_urls),
        },
    );
    let manifest = second
        .fetch_closed_markets(&mut cache)
        .await
        .unwrap()
        .coverage_manifest
        .expect("the terminal page installs coverage");
    assert_eq!(manifest.generation, state.generation);
    assert_eq!(manifest.pages.len(), 2);
    let resumed_urls = resumed_urls.lock().unwrap();
    assert_eq!(resumed_urls.len(), 1);
    assert!(
        resumed_urls[0].ends_with("next_cursor=c1"),
        "the resume starts at the checkpoint: {}",
        resumed_urls[0]
    );
    println!("PASS: schema-two payout walk kept its checkpoint through exhaustion and resumed");
}

// PASS: the payout walk completes a page whose reply arrives after the shared
//       10 s request timeout, because it waits up to its own 120 s (#788).
// FAIL: the walk abandons the slow reply (exhausting its retries), or does not
//       finish within the 60 s test deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payout_walk_waits_out_a_reply_slower_than_the_shared_timeout() {
    let (dir, mut cache) = open_v2_cache();
    let config = BootstrapConfig {
        cache_path: dir.path().join("cache.db"),
        clob_base_url: start_slow_server(Duration::from_secs(11), payout_page(WINNER, "LTE=")),
        ..BootstrapConfig::default()
    };
    let manifest = tokio::time::timeout(
        Duration::from_secs(60),
        pe_bootstrap::populate_clob_payout_v2(&config, &mut cache),
    )
    .await
    .expect("the walk finishes within the deadline")
    .expect("the slow page completes");
    assert_eq!(manifest.pages.len(), 1);
    println!("PASS: payout walk completed a reply slower than the shared 10 s timeout");
}
