#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Scenario: CLOB `/book` fetcher round-trip through the deterministic fixture
//! fetcher — no network, no clock, no RNG. Proves the public cross-crate surface
//! the PR-H snapshot worker depends on (`pe_service::clob_book::*`): a configured
//! token returns its parsed ask book with the correct best ask, and an
//! unconfigured token surfaces `MissingFixture` rather than panicking.

use std::collections::HashMap;

use pe_service::clob_book::{
    BookLevel, ClobBookError, ClobBookFetcher, FixtureClobBookFetcher, OrderBook,
};
use rust_decimal_macros::dec;

// PASS: configured token → 2 ask levels, best_ask = 0.61 (lowest price);
//       unconfigured token → ClobBookError::MissingFixture (no panic, no network).
// FAIL: wrong asks, wrong best_ask, wrong error variant, or any panic.
#[tokio::test]
async fn clob_book_fixture_roundtrip() {
    let mut books = HashMap::new();
    books.insert(
        "winning-outcome-token".to_string(),
        OrderBook {
            asks: vec![
                BookLevel {
                    price: dec!(0.99),
                    size: dec!(12476.68),
                },
                BookLevel {
                    price: dec!(0.61),
                    size: dec!(250.5),
                },
            ],
            response_blake3: String::new(),
            fetched_at_ms: 0,
        },
    );
    let fetcher = FixtureClobBookFetcher::new(books);

    let book = fetcher.fetch_book("winning-outcome-token").await.unwrap();
    assert_eq!(book.asks.len(), 2, "expected the two configured ask levels");
    assert_eq!(
        book.best_ask(),
        Some(dec!(0.61)),
        "best ask is the lowest price"
    );

    let missing = fetcher.fetch_book("never-traded-token").await.unwrap_err();
    assert!(
        matches!(missing, ClobBookError::MissingFixture(_)),
        "expected MissingFixture for an unconfigured token, got {missing:?}"
    );

    println!("PASS clob_book_fixture_roundtrip");
}
