//! Scenario: ReqwestFetcher against an in-process axum mock server.
//!
//! Scenarios:
//!   1. happy_path             — 200 OK → bytes returned.
//!   2. retry_on_5xx           — 500×2 then 200 → success; 3 total requests.
//!   3. rate_limited_429       — 429 with Retry-After:5 → RateLimited error.
//!   4. fatal_4xx              — 404 → Fatal error.
//!   5. exhaust_retries        — always 500, max_retries=2 → Transient; 3 total requests.
//!   6. retry_on_408           — 408×2 then 200 → success; 3 total requests. (issue #159)
//!   7. exhaust_retries_408    — always 408, max_retries=2 → Transient; 3 total requests. (issue #159)
//!   8. unreadable_reply      — incomplete JSON retries, retains responses, and shares the retry budget.
//!   9. unexpected_json_shape — complete JSON passes through unchanged.
//!  10. timeout_bounds_body   — headers at once, body one byte per 200 ms, 2 s timeout → BodyRead, Transient. (#788)

#![cfg(feature = "scenario")]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]

use std::io::{Read as _, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use pe_core_types::{RawHttpAttempt, TransportErrorClass};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{HttpRequestContext, PageFetcher, ReqwestFetcher};

// ── Mock server helpers ───────────────────────────────────────────────────────

async fn start_mock_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failed");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("server error");
    });
    format!("http://{addr}")
}

fn make_fetcher() -> ReqwestFetcher {
    ReqwestFetcher::new(reqwest::Client::new()).with_initial_backoff_ms(1)
}

async fn start_reply_server(bodies: Vec<&'static [u8]>) -> (String, Arc<AtomicU32>) {
    let hits = Arc::new(AtomicU32::new(0));
    let router = Router::new()
        .route(
            "/data",
            get(
                |State((hits, bodies)): State<(Arc<AtomicU32>, Vec<&'static [u8]>)>| async move {
                    let hit =
                        usize::try_from(hits.fetch_add(1, Ordering::SeqCst)).expect("hit index");
                    (StatusCode::OK, bodies[hit.min(bodies.len() - 1)].to_vec())
                },
            ),
        )
        .with_state((hits.clone(), bodies));
    (start_mock_server(router).await, hits)
}

// ── Scenario 1: happy_path ────────────────────────────────────────────────────

/// PASS: a complete JSON success body is returned unchanged.
#[tokio::test]
async fn scenario_happy_path() {
    async fn handler() -> impl IntoResponse {
        (StatusCode::OK, b"[1]".to_vec())
    }

    let router = Router::new().route("/data", get(handler));
    let base = start_mock_server(router).await;

    let fetcher = make_fetcher();
    let bytes = fetcher
        .fetch_page(&format!("{base}/data"))
        .await
        .expect("expected Ok");

    assert_eq!(bytes, b"[1]");
}

// ── Scenario 2: retry_on_5xx ─────────────────────────────────────────────────
//
// PASS: fetch_page returns Ok after 3 total requests (2 × 500, 1 × 200).
// FAIL: fewer or more than 3 requests, or fetch_page returns an error.

/// PASS: two 500 replies retry to a complete JSON body after exactly three requests.
#[tokio::test]
async fn scenario_retry_on_5xx() {
    #[derive(Clone)]
    struct S {
        count: Arc<AtomicU32>,
    }

    async fn handler(State(s): State<S>) -> impl IntoResponse {
        let prev = s.count.fetch_add(1, Ordering::SeqCst);
        if prev < 2 {
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        } else {
            (StatusCode::OK, b"[1]".to_vec()).into_response()
        }
    }

    let state = S {
        count: Arc::new(AtomicU32::new(0)),
    };
    let count_ref = state.count.clone();
    let router = Router::new()
        .route("/flaky", get(handler))
        .with_state(state);
    let base = start_mock_server(router).await;

    let fetcher = make_fetcher();
    let bytes = fetcher
        .fetch_page(&format!("{base}/flaky"))
        .await
        .expect("expected Ok after retries");

    assert_eq!(bytes, b"[1]");
    assert_eq!(
        count_ref.load(Ordering::SeqCst),
        3,
        "expected 3 total requests"
    );
}

// ── Scenario 3: rate_limited_429 ─────────────────────────────────────────────
//
// PASS: fetch_page returns RateLimited { retry_after_secs: 5 } after exactly one request;
//       a fetcher that has not opted in never retries a 429 (issue #555).
// FAIL: any other error variant or Ok.

#[tokio::test]
async fn scenario_rate_limited_429() {
    async fn handler() -> impl IntoResponse {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "5".parse().expect("header value"));
        (StatusCode::TOO_MANY_REQUESTS, headers, "")
    }

    let router = Router::new().route("/limited", get(handler));
    let base = start_mock_server(router).await;

    let fetcher = make_fetcher();
    let err = fetcher
        .fetch_page(&format!("{base}/limited"))
        .await
        .expect_err("expected Err");

    match err {
        SourceError::RateLimited { retry_after_secs } => {
            assert_eq!(retry_after_secs, 5);
        }
        other => panic!("expected RateLimited, got {other}"),
    }
}

// ── Scenario 4: fatal_4xx ────────────────────────────────────────────────────
//
// PASS: fetch_page returns Fatal on 404; no retry.
// FAIL: any other error variant or Ok.

#[tokio::test]
async fn scenario_fatal_4xx() {
    async fn handler() -> impl IntoResponse {
        StatusCode::NOT_FOUND
    }

    let router = Router::new().route("/missing", get(handler));
    let base = start_mock_server(router).await;

    let fetcher = make_fetcher();
    let err = fetcher
        .fetch_page(&format!("{base}/missing"))
        .await
        .expect_err("expected Err");

    assert!(
        matches!(err, SourceError::Fatal { .. }),
        "expected Fatal, got {err}"
    );
}

// ── Scenario 5: exhaust_retries ──────────────────────────────────────────────
//
// PASS: fetch_page returns Transient; exactly max_retries+1 requests made.
// FAIL: any other error variant, Ok, or wrong request count.

#[tokio::test]
async fn scenario_exhaust_retries() {
    #[derive(Clone)]
    struct S {
        count: Arc<AtomicU32>,
    }

    async fn handler(State(s): State<S>) -> impl IntoResponse {
        s.count.fetch_add(1, Ordering::SeqCst);
        StatusCode::INTERNAL_SERVER_ERROR
    }

    let state = S {
        count: Arc::new(AtomicU32::new(0)),
    };
    let count_ref = state.count.clone();
    let router = Router::new()
        .route("/error", get(handler))
        .with_state(state);
    let base = start_mock_server(router).await;

    // max_retries = 2 → 3 total attempts.
    let fetcher = make_fetcher().with_max_retries(2);
    let err = fetcher
        .fetch_page(&format!("{base}/error"))
        .await
        .expect_err("expected Err");

    assert!(
        matches!(err, SourceError::Transient { .. }),
        "expected Transient, got {err}"
    );
    assert_eq!(
        count_ref.load(Ordering::SeqCst),
        3,
        "expected 3 total requests (initial + 2 retries)"
    );
}

// ── Scenario 6: retry_on_408 ─────────────────────────────────────────────────
//
// Issue #159: HTTP 408 Request Timeout was previously routed to Fatal by the
// `>= 400 && != 429` branch. After widening `is_retryable_status` to include
// 408, the fetcher must back off and succeed when the next attempt is 200.
//
// PASS: fetch_page returns Ok after two 408s, exactly 3 total requests.
// FAIL: any error, or a request count other than 3.

/// PASS: two 408 replies retry to a complete JSON body after exactly three requests.
#[tokio::test]
async fn scenario_retry_on_408() {
    #[derive(Clone)]
    struct S {
        count: Arc<AtomicU32>,
    }

    async fn handler(State(s): State<S>) -> impl IntoResponse {
        let prev = s.count.fetch_add(1, Ordering::SeqCst);
        if prev < 2 {
            StatusCode::REQUEST_TIMEOUT.into_response()
        } else {
            (StatusCode::OK, b"[1]".to_vec()).into_response()
        }
    }

    let state = S {
        count: Arc::new(AtomicU32::new(0)),
    };
    let count_ref = state.count.clone();
    let router = Router::new()
        .route("/timeout", get(handler))
        .with_state(state);
    let base = start_mock_server(router).await;

    let fetcher = make_fetcher();
    let bytes = fetcher
        .fetch_page(&format!("{base}/timeout"))
        .await
        .expect("408s must be retried until success");

    assert_eq!(bytes, b"[1]");
    assert_eq!(
        count_ref.load(Ordering::SeqCst),
        3,
        "expected 3 total requests (2x 408 + 1x 200)"
    );
}

// ── Scenario 7: exhaust_retries_408 ──────────────────────────────────────────
//
// Issue #159: when 408 persists past max_retries, the fetcher must surface
// Transient (not Fatal) so upstream BootstrapError treats the wallet as
// retry-eligible rather than aborting the whole run.
//
// PASS: fetch_page returns Transient; exactly max_retries+1 requests made.
// FAIL: any other error variant, Ok, or wrong request count.

#[tokio::test]
async fn scenario_exhaust_retries_408() {
    #[derive(Clone)]
    struct S {
        count: Arc<AtomicU32>,
    }

    async fn handler(State(s): State<S>) -> impl IntoResponse {
        s.count.fetch_add(1, Ordering::SeqCst);
        StatusCode::REQUEST_TIMEOUT
    }

    let state = S {
        count: Arc::new(AtomicU32::new(0)),
    };
    let count_ref = state.count.clone();
    let router = Router::new()
        .route("/timeout-forever", get(handler))
        .with_state(state);
    let base = start_mock_server(router).await;

    // max_retries = 2 → 3 total attempts.
    let fetcher = make_fetcher().with_max_retries(2);
    let err = fetcher
        .fetch_page(&format!("{base}/timeout-forever"))
        .await
        .expect_err("expected Err after exhausting retries");

    assert!(
        matches!(err, SourceError::Transient { .. }),
        "408 exhaustion must surface Transient, got {err}"
    );
    assert_eq!(
        count_ref.load(Ordering::SeqCst),
        3,
        "expected 3 total requests (initial + 2 retries)"
    );
}

// ── Scenario 8: unreadable_reply ──────────────────────────────────────────────

/// PASS: a cut JSON string retries once and both 200 responses are observed in order.
#[tokio::test]
async fn scenario_unreadable_reply_retries_and_observes_both_responses() {
    let cut = br#"[{"x":"cut"#;
    let complete = br#"[{"x":"complete"}]"#;
    let (base, hits) = start_reply_server(vec![cut, complete]).await;
    let mut attempts = Vec::new();
    let bytes = make_fetcher()
        .with_max_retries(1)
        .with_min_interval_ms(0)
        .fetch_page_observed(
            &format!("{base}/data"),
            HttpRequestContext {
                source_id: "test-server",
                endpoint_kind: "unreadable-reply",
            },
            |attempt| {
                attempts.push(attempt);
                Ok(())
            },
        )
        .await
        .expect("complete reply after retry");

    assert_eq!(bytes, complete);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    let [
        RawHttpAttempt::Response(first),
        RawHttpAttempt::Response(second),
    ] = attempts.as_slice()
    else {
        panic!("expected two responses, got {attempts:?}");
    };
    assert_eq!(first.body, cut);
    assert_eq!(second.body, complete);
    assert_eq!((first.status, first.attempt_ordinal), (200, 1));
    assert_eq!((second.status, second.attempt_ordinal), (200, 2));
}

/// PASS: every unreadable 200 exhausts one retry as Transient after exactly two requests.
#[tokio::test]
async fn scenario_unreadable_replies_exhaust_retry_budget() {
    let cases: [(&str, &[u8]); 5] = [
        ("cut string", br#"[{"x":"cut"#),
        ("invalid UTF-8", b"[{\"x\":\"\xff\"}]"),
        ("trailing document", b"[] []"),
        ("empty", b""),
        ("HTML", b"<html>unavailable</html>"),
    ];
    for (case, body) in cases {
        let (base, hits) = start_reply_server(vec![body]).await;
        let result = make_fetcher()
            .with_max_retries(1)
            .with_min_interval_ms(0)
            .fetch_page(&format!("{base}/data"))
            .await;
        assert!(
            matches!(result, Err(SourceError::Transient { ref message }) if message.contains("reply is not one complete JSON document")),
            "{case}: expected unreadable-reply Transient, got {result:?}"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2, "{case}");
    }
}

/// PASS: zero retries returns Transient after one unreadable 200 and retains its Response.
#[tokio::test]
async fn scenario_unreadable_reply_zero_retries_observes_response() {
    let cut = br#"[{"x":"cut"#;
    let (base, hits) = start_reply_server(vec![cut]).await;
    let mut attempts = Vec::new();
    let result = make_fetcher()
        .with_max_retries(0)
        .fetch_page_observed(
            &format!("{base}/data"),
            HttpRequestContext {
                source_id: "test-server",
                endpoint_kind: "unreadable-reply",
            },
            |attempt| {
                attempts.push(attempt);
                Ok(())
            },
        )
        .await;

    assert!(
        matches!(result, Err(SourceError::Transient { ref message }) if message.contains("reply is not one complete JSON document")),
        "expected unreadable-reply Transient, got {result:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let [RawHttpAttempt::Response(response)] = attempts.as_slice() else {
        panic!("expected one response, got {attempts:?}");
    };
    assert_eq!(response.body, cut);
    assert_eq!((response.status, response.attempt_ordinal), (200, 1));
}

// ── Scenario 9: unexpected_json_shape ─────────────────────────────────────────

/// PASS: valid JSON of an unexpected shape is returned unchanged after one request.
#[tokio::test]
async fn scenario_unexpected_json_shape_passes_through() {
    let body = br#"{"not":"an array"}"#;
    let (base, hits) = start_reply_server(vec![body]).await;
    let bytes = make_fetcher()
        .fetch_page(&format!("{base}/data"))
        .await
        .expect("valid JSON passes through");

    assert_eq!(bytes, body);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

// ── Scenario 10: timeout_bounds_body ──────────────────────────────────────────

/// Reads one request head (a GET carries no body) and reports whether it arrived complete.
fn read_request_head(stream: &mut std::net::TcpStream) -> bool {
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1_024];
    loop {
        if head.windows(4).any(|window| window == b"\r\n\r\n") {
            return true;
        }
        if head.len() >= 16_384 {
            return false;
        }
        match stream.read(&mut chunk) {
            Ok(0) => return false,
            Ok(read) => head.extend_from_slice(&chunk[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }
}

/// A raw HTTP/1.1 server that answers each complete request with its headers at
/// once, then sends `body` one byte every 200 ms; it counts those requests.
fn start_trickle_server(body: &'static [u8]) -> (String, Arc<AtomicU32>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let addr = listener.local_addr().expect("local_addr");
    let hits = Arc::new(AtomicU32::new(0));
    let counter = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().take(8) {
            let Ok(mut stream) = stream else { continue };
            if !read_request_head(&mut stream) {
                continue;
            }
            counter.fetch_add(1, Ordering::SeqCst);
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if stream.write_all(head.as_bytes()).is_err() {
                continue;
            }
            for byte in body {
                std::thread::sleep(Duration::from_millis(200));
                if stream.write_all(std::slice::from_ref(byte)).is_err() {
                    break;
                }
            }
        }
    });
    (format!("http://{addr}"), hits)
}

/// PASS: the request timeout bounds the whole reply, body included: headers
/// arrive at once but the body takes about 13 s, so a 2 s timeout ends the one
/// request while its body is being read (a `BodyRead` failure, then Transient).
/// FAIL: the fetch returns the page or fails elsewhere, because the timeout
/// covered only the headers or restarted on every read (the #788 payout
/// timeout relies on it bounding the body).
#[tokio::test]
async fn scenario_timeout_bounds_a_trickling_body() {
    let (base, hits) =
        start_trickle_server(br#"{"data":[],"next_cursor":"LTE=","note":"one byte every 200 ms"}"#);
    let mut attempts = Vec::new();
    let result = make_fetcher()
        .with_timeout(2)
        .with_max_retries(0)
        .fetch_page_observed(
            &format!("{base}/data"),
            HttpRequestContext {
                source_id: "test-server",
                endpoint_kind: "trickling-body",
            },
            |attempt| {
                attempts.push(attempt);
                Ok(())
            },
        )
        .await;

    assert!(
        matches!(result, Err(SourceError::Transient { .. })),
        "expected the timeout to end the trickling reply, got {result:?}"
    );
    let [RawHttpAttempt::TransportFailure(failure)] = attempts.as_slice() else {
        panic!("expected one transport failure, got {attempts:?}");
    };
    assert_eq!(failure.error_class, TransportErrorClass::BodyRead);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}
