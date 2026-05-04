//! Scenario: ReqwestFetcher against an in-process axum mock server.
//!
//! Scenarios:
//!   1. happy_path          — 200 OK → bytes returned.
//!   2. retry_on_5xx        — 500×2 then 200 → success; 3 total requests.
//!   3. rate_limited_429    — 429 with Retry-After:5 → RateLimited error.
//!   4. fatal_4xx           — 404 → Fatal error.
//!   5. exhaust_retries     — always 500, max_retries=2 → Transient; 3 total requests.

#![cfg(feature = "scenario")]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{PageFetcher, ReqwestFetcher};

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

// ── Scenario 1: happy_path ────────────────────────────────────────────────────

#[tokio::test]
async fn scenario_happy_path() {
    async fn handler() -> impl IntoResponse {
        (StatusCode::OK, b"hello world".to_vec())
    }

    let router = Router::new().route("/data", get(handler));
    let base = start_mock_server(router).await;

    let mut fetcher = make_fetcher();
    let bytes = fetcher
        .fetch_page(&format!("{base}/data"))
        .await
        .expect("expected Ok");

    assert_eq!(bytes, b"hello world");
}

// ── Scenario 2: retry_on_5xx ─────────────────────────────────────────────────
//
// PASS: fetch_page returns Ok after 3 total requests (2 × 500, 1 × 200).
// FAIL: fewer or more than 3 requests, or fetch_page returns an error.

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
            (StatusCode::OK, b"ok".to_vec()).into_response()
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

    let mut fetcher = make_fetcher();
    let bytes = fetcher
        .fetch_page(&format!("{base}/flaky"))
        .await
        .expect("expected Ok after retries");

    assert_eq!(bytes, b"ok");
    assert_eq!(
        count_ref.load(Ordering::SeqCst),
        3,
        "expected 3 total requests"
    );
}

// ── Scenario 3: rate_limited_429 ─────────────────────────────────────────────
//
// PASS: fetch_page returns RateLimited { retry_after_secs: 5 }; no retry.
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

    let mut fetcher = make_fetcher();
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

    let mut fetcher = make_fetcher();
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
    let mut fetcher = make_fetcher().with_max_retries(2);
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
