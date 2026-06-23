//! Scenario: `ClobPricesHistoryClient` against an in-process axum mock server (issue #421 PR4).
//!
//! Proves the real reqwest GET path end-to-end — URL construction, the `{"history":[{t,p}]}` parse,
//! and the 4xx→empty contract — which the in-crate `FixtureFetcher` unit tests cannot exercise.
//!
//! Scenarios:
//!   1. parses_real_http — 200 with a history body → ordered PricePoint series, decimals intact.
//!   2. 404_yields_empty — an unknown token's 404 → empty series (never aborts a backfill).
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use axum::Router;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::routing::get;
use rust_decimal::Decimal;

use pe_source_polymarket_public::{ClobPricesHistoryClient, ReqwestFetcher};

async fn start_mock_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("server error");
    });
    format!("http://{addr}")
}

fn client(base: String) -> ClobPricesHistoryClient<ReqwestFetcher> {
    // Tiny backoff keeps any retry fast; the real reqwest path is what we're exercising.
    ClobPricesHistoryClient::new(
        base,
        ReqwestFetcher::new(reqwest::Client::new()).with_initial_backoff_ms(1),
    )
}

// PASS: fetch_prices_history returns the two points in order with exact decimals; the `market`
//       query param reached the server.
// FAIL: wrong points/order/decimals, missing market param, or any transport/parse error.
#[tokio::test]
async fn parses_real_http() {
    async fn handler(Query(params): Query<HashMap<String, String>>) -> &'static str {
        assert_eq!(
            params.get("market").map(String::as_str),
            Some("tok123"),
            "the token id must arrive as the `market` query param"
        );
        assert_eq!(params.get("fidelity").map(String::as_str), Some("60"));
        r#"{"history":[{"t":1000,"p":0.42},{"t":1060,"p":0.5}]}"#
    }
    let router = Router::new().route("/prices-history", get(handler));
    let base = start_mock_server(router).await;

    let points = client(base)
        .fetch_prices_history("tok123", 1000, 2000)
        .await
        .expect("fetch ok");

    assert_eq!(points.len(), 2);
    assert_eq!(points[0].t, 1000);
    assert_eq!(points[0].price, Decimal::new(42, 2));
    assert_eq!(points[1].t, 1060);
    assert_eq!(points[1].price, Decimal::new(5, 1));
    println!("PASS: parses_real_http — 2 points parsed over real HTTP, decimals intact");
}

// PASS: a 404 yields Ok(empty) so one bad token never aborts a million-token backfill.
// FAIL: an error is returned, or any points come back.
#[tokio::test]
async fn fourohfour_yields_empty() {
    async fn handler() -> StatusCode {
        StatusCode::NOT_FOUND
    }
    let router = Router::new().route("/prices-history", get(handler));
    let base = start_mock_server(router).await;

    let points = client(base)
        .fetch_prices_history("missing", 0, 1)
        .await
        .expect("4xx must be Ok(empty), not Err");

    assert!(points.is_empty(), "404 must yield an empty series");
    println!("PASS: fourohfour_yields_empty — unknown token 404 → empty series");
}
