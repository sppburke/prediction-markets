//! Polymarket CLOB `/book` order-book fetcher (public, no auth).
//!
//! Captures ask-side depth for the liquidity-at-fill snapshot worker (WS2 of
//! issue #350). This module is the **fetcher only**: it fetches a token's order
//! book over the public CLOB REST endpoint and parses the ask side into
//! `Decimal` levels. The downstream `absorbable_usd_100bps` derivation (Σ
//! price·size within 1% of best ask) lives in the snapshot worker that consumes
//! [`OrderBook`] (PR-H), so the fetcher carries no money math beyond a faithful
//! string→`Decimal` parse.
//!
//! Endpoint: `GET https://clob.polymarket.com/book?token_id=<id>` — re-confirmed
//! public/no-auth, HTTP 200 with `asks`/`bids` as arrays of `{price, size}`
//! **string** levels and a bogus token id returning `404` (2026-06-16;
//! `docs/15-SOURCES.md`). Only the ask side is parsed — liquidity capture is
//! buy-only and a BUY consumes the ask side.

use std::collections::HashMap;
use std::str::FromStr as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;
use serde::Deserialize;

/// Live Polymarket CLOB REST base URL.
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";
/// Minimum interval between `/book` requests — ≤ 5 req/s sustained, the
/// canonical Polymarket CLOB REST limit in `docs/_GLOSSARY.md`
/// (`polymarket_clob_min_interval_ms` = 200; "Venue rate limits" table). Mirrors
/// `ReqwestCLOBClient::MIN_INTERVAL_MS` in `crates/venue-polymarket`.
const CLOB_MIN_INTERVAL_MS: u64 = 200;
/// Per-request timeout for the `/book` call — `clob_book_request_timeout_secs`
/// in `docs/_GLOSSARY.md`. Shorter than the order-submission client's 10s
/// (`polymarket_request_timeout_secs`) because the fetch runs off the fill hot
/// path (PR-H's snapshot worker), so a slow book is dropped to a partial
/// snapshot rather than blocking a trade.
const CLOB_REQUEST_TIMEOUT_SECS: u64 = 5;

/// Errors from a `/book` fetch or parse.
#[derive(Debug, thiserror::Error)]
pub enum ClobBookError {
    /// Transport failure or timeout reaching the CLOB endpoint.
    #[error("clob /book request failed: {0}")]
    Request(String),
    /// Non-success HTTP status (e.g. `404` — no order book for the token id).
    #[error("clob /book returned status {0}")]
    Status(u16),
    /// Body was not valid book JSON, or a price/size was not a valid `Decimal`.
    #[error("clob /book decode failed: {0}")]
    Decode(String),
    /// No fixture configured for the requested token id (test fetcher only).
    #[error("no fixture book for token id {0}")]
    MissingFixture(String),
}

/// A single ask-side order-book level, parsed to `Decimal` (never `f64`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevel {
    /// Price per share, in `[0, 1]`.
    pub price: Decimal,
    /// Size available at this price, in shares.
    pub size: Decimal,
}

/// The ask side of a token's order book, parsed from a CLOB `/book` response.
///
/// Only asks are retained — liquidity-at-fill capture is buy-only, and a BUY
/// consumes the ask side. Levels are in the venue's returned order (observed
/// high→low price); callers needing the best price should use
/// [`OrderBook::best_ask`] rather than assuming a position.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    /// Ask levels `{price, size}` as returned by `/book`.
    pub asks: Vec<BookLevel>,
    /// BLAKE3 identity of the exact `/book` response bytes. Fixture books that
    /// were constructed as typed values use a deterministic canonical ask hash.
    pub response_blake3: String,
    /// Client-side fetch completion time (Unix ms), stamped by the
    /// [`ClobBookFetcher`] implementations (#508 Phase A). The impact-gate
    /// planner refuses to price an order off a snapshot older than the shared
    /// ladder staleness bound. `from_book_json` leaves it `0` (parse-only).
    pub fetched_at_ms: u64,
}

impl OrderBook {
    /// Parse a raw `/book` JSON body into the ask-side book.
    ///
    /// Unknown fields (`bids`, `market`, `tick_size`, …) are ignored and an
    /// absent `asks` field parses to an empty book. Returns
    /// [`ClobBookError::Decode`] if the body is not book JSON or any ask
    /// `price`/`size` is not a valid `Decimal`.
    pub fn from_book_json(bytes: &[u8]) -> Result<Self, ClobBookError> {
        let raw: RawBook =
            serde_json::from_slice(bytes).map_err(|e| ClobBookError::Decode(e.to_string()))?;
        let mut asks = Vec::with_capacity(raw.asks.len());
        for level in &raw.asks {
            let price = Decimal::from_str(&level.price).map_err(|_| {
                ClobBookError::Decode(format!("ask price {:?} not a decimal", level.price))
            })?;
            let size = Decimal::from_str(&level.size).map_err(|_| {
                ClobBookError::Decode(format!("ask size {:?} not a decimal", level.size))
            })?;
            asks.push(BookLevel { price, size });
        }
        Ok(Self {
            asks,
            response_blake3: blake3::hash(bytes).to_hex().to_string(),
            fetched_at_ms: 0,
        })
    }

    /// Best (lowest-price) ask, or `None` for an empty book. Computed as the
    /// minimum price so the result is correct regardless of the order the venue
    /// returns levels in.
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.iter().map(|level| level.price).min()
    }

    /// The positive-size, positive-price ask ladder sorted ascending by price —
    /// the planner input (#508 Phase A). Zero-size dust and non-positive prices
    /// are filtered (they must not anchor a fill basis); sizes are truncated to
    /// the 6-dp exact share scale (never overstating depth); a level whose
    /// price is not a valid [`pe_core_types::Price`] (out of `[0, 1]`) yields
    /// `None` — a corrupt book the impact gate treats as unusable (fail-closed).
    pub fn ladder(&self) -> Option<Vec<pe_venue_polymarket::AskLevel>> {
        let mut out = Vec::with_capacity(self.asks.len());
        for level in &self.asks {
            let size = level
                .size
                .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero);
            if size <= Decimal::ZERO || level.price <= Decimal::ZERO {
                continue;
            }
            let price = pe_core_types::Price::new(level.price).ok()?;
            let shares = pe_core_types::ShareAmount::from_decimal_exact(size).ok()?;
            out.push(pe_venue_polymarket::AskLevel { price, shares });
        }
        out.sort_by_key(|level| level.price);
        Some(out)
    }
}

#[derive(Debug, Deserialize)]
struct RawBook {
    #[serde(default)]
    asks: Vec<RawLevel>,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

/// Fetches a token's CLOB order book. Implemented by [`ReqwestClobBookFetcher`]
/// (production) and [`FixtureClobBookFetcher`] (deterministic tests), mirroring
/// the `PageFetcher` / `CLOBClient` trait-plus-fixture pattern used elsewhere in
/// the workspace. `&self` (not `&mut`) so an `Arc` can be shared across tasks;
/// rate limiting uses interior mutability.
#[allow(async_fn_in_trait)]
pub trait ClobBookFetcher {
    /// Fetch and parse the ask side of `token_id`'s order book.
    async fn fetch_book(&self, token_id: &str) -> Result<OrderBook, ClobBookError>;
}

/// Production [`ClobBookFetcher`] over the public CLOB REST `/book` endpoint.
///
/// Enforces a `CLOB_MIN_INTERVAL_MS` (200 ms, ≤ 5 req/s) min-interval gate via
/// interior mutability — matching `ReqwestCLOBClient::rate_limit_gate` — and a
/// `CLOB_REQUEST_TIMEOUT_SECS` (5 s) per-request timeout. No auth headers: the
/// `/book` endpoint is public.
pub struct ReqwestClobBookFetcher {
    client: reqwest::Client,
    base_url: String,
    min_interval: Duration,
    timeout: Duration,
    last_request_at: Mutex<Option<Instant>>,
}

impl ReqwestClobBookFetcher {
    /// Build a fetcher over the live CLOB host.
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            base_url: CLOB_BASE_URL.to_string(),
            min_interval: Duration::from_millis(CLOB_MIN_INTERVAL_MS),
            timeout: Duration::from_secs(CLOB_REQUEST_TIMEOUT_SECS),
            last_request_at: Mutex::new(None),
        }
    }

    /// Override the base URL (e.g. a local mock server in tests). Defaults to
    /// the live CLOB host.
    #[must_use]
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    /// Reserve the next request slot at `min_interval` past the later of the
    /// last reserved slot and now, then sleep until it. Successive `/book` calls
    /// are spaced ≥ `min_interval` apart and concurrent callers are serialized
    /// via the interior mutex (poison-safe); the very first call (no prior slot)
    /// fires immediately. Mirrors `ReqwestCLOBClient::rate_limit_gate`
    /// (`crates/venue-polymarket/src/clob_client.rs`).
    async fn rate_limit_gate(&self) {
        let sleep_for = {
            let mut guard = self
                .last_request_at
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            let next_slot = match *guard {
                None => now,
                Some(last) => last.max(now) + self.min_interval,
            };
            *guard = Some(next_slot);
            next_slot.checked_duration_since(now)
        };
        if let Some(d) = sleep_for {
            tokio::time::sleep(d).await;
        }
    }
}

impl ClobBookFetcher for ReqwestClobBookFetcher {
    async fn fetch_book(&self, token_id: &str) -> Result<OrderBook, ClobBookError> {
        self.rate_limit_gate().await;
        let url = format!("{}/book?token_id={token_id}", self.base_url);
        let resp = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| ClobBookError::Request(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(ClobBookError::Status(status.as_u16()));
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| ClobBookError::Request(e.to_string()))?;
        let mut book = OrderBook::from_book_json(&body)?;
        book.fetched_at_ms = now_unix_ms();
        Ok(book)
    }
}

/// Current Unix time in milliseconds (`0` before the epoch — unreachable on a live host).
pub(crate) fn now_unix_ms() -> u64 {
    u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
}

/// Deterministic [`ClobBookFetcher`] returning pre-loaded books keyed by token
/// id. Used by unit/scenario tests and the PR-H snapshot-worker scenarios; no
/// network, no clock.
pub struct FixtureClobBookFetcher {
    books: HashMap<String, OrderBook>,
}

impl FixtureClobBookFetcher {
    /// Build a fixture fetcher from a `token_id -> OrderBook` map.
    pub fn new(books: HashMap<String, OrderBook>) -> Self {
        Self { books }
    }
}

impl ClobBookFetcher for FixtureClobBookFetcher {
    async fn fetch_book(&self, token_id: &str) -> Result<OrderBook, ClobBookError> {
        let mut book = self
            .books
            .get(token_id)
            .cloned()
            .ok_or_else(|| ClobBookError::MissingFixture(token_id.to_string()))?;
        // Mirror production: stamp fetch completion so a fixture book is fresh at plan time.
        // A fixture that pre-sets a non-zero `fetched_at_ms` keeps it (staleness tests).
        if book.fetched_at_ms == 0 {
            book.fetched_at_ms = now_unix_ms();
        }
        if book.response_blake3.is_empty() {
            let canonical = serde_json::json!({
                "asks": book.asks.iter().map(|level| serde_json::json!({
                    "price": level.price.normalize().to_string(),
                    "size": level.size.normalize().to_string(),
                })).collect::<Vec<_>>(),
            });
            book.response_blake3 = blake3::hash(canonical.to_string().as_bytes())
                .to_hex()
                .to_string();
        }
        Ok(book)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A body mirroring the live `/book` shape captured 2026-06-16: `asks`/`bids`
    /// as `{price, size}` string levels plus scalar meta fields we ignore. Asks
    /// are listed high→low as the venue returns them.
    const REAL_SHAPE: &str = r#"{
        "market": "0x5b534e0f41523ad9cce972e32e223b33fc8180cba7c1d9b849f62a6c00848eb1",
        "asset_id": "40346312610026057659615542747852379545591818571871292040155657292951781680108",
        "asks": [
            {"price": "0.99", "size": "12476.68"},
            {"price": "0.62", "size": "100"},
            {"price": "0.61", "size": "250.5"}
        ],
        "bids": [
            {"price": "0.01", "size": "113133.4"}
        ],
        "tick_size": "0.01",
        "neg_risk": false,
        "timestamp": "1781655409736"
    }"#;

    #[test]
    fn parses_real_book_shape_asks_only() {
        let book = OrderBook::from_book_json(REAL_SHAPE.as_bytes()).unwrap();
        assert_eq!(
            book.asks,
            vec![
                BookLevel {
                    price: dec!(0.99),
                    size: dec!(12476.68)
                },
                BookLevel {
                    price: dec!(0.62),
                    size: dec!(100)
                },
                BookLevel {
                    price: dec!(0.61),
                    size: dec!(250.5)
                },
            ]
        );
        assert_eq!(
            book.response_blake3,
            blake3::hash(REAL_SHAPE.as_bytes()).to_hex().to_string(),
            "decision evidence identifies the exact response bytes"
        );
    }

    #[test]
    fn best_ask_is_minimum_price_regardless_of_order() {
        // Real `/book` lists asks high→low; best ask is the lowest price,
        // computed via min (not position) so PR-H's "within 1% of best ask"
        // holds whatever order the venue returns.
        let book = OrderBook::from_book_json(REAL_SHAPE.as_bytes()).unwrap();
        assert_eq!(book.best_ask(), Some(dec!(0.61)));
    }

    #[test]
    fn empty_asks_yield_empty_book_and_no_best_ask() {
        let book = OrderBook::from_book_json(br#"{"asks": []}"#).unwrap();
        assert!(book.asks.is_empty());
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn missing_asks_field_defaults_to_empty() {
        // A body lacking `asks` parses to an empty book rather than erroring.
        let book = OrderBook::from_book_json(br#"{"bids": []}"#).unwrap();
        assert!(book.asks.is_empty());
    }

    #[test]
    fn malformed_price_is_a_decode_error() {
        let err =
            OrderBook::from_book_json(br#"{"asks":[{"price":"abc","size":"1"}]}"#).unwrap_err();
        assert!(matches!(err, ClobBookError::Decode(_)), "got {err:?}");
    }

    #[test]
    fn malformed_size_is_a_decode_error() {
        let err =
            OrderBook::from_book_json(br#"{"asks":[{"price":"0.5","size":"x"}]}"#).unwrap_err();
        assert!(matches!(err, ClobBookError::Decode(_)), "got {err:?}");
    }

    #[test]
    fn non_json_body_is_a_decode_error() {
        let err = OrderBook::from_book_json(b"not json").unwrap_err();
        assert!(matches!(err, ClobBookError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn fixture_fetcher_returns_configured_book() {
        let mut books = HashMap::new();
        books.insert(
            "tok-1".to_string(),
            OrderBook {
                asks: vec![BookLevel {
                    price: dec!(0.6),
                    size: dec!(10),
                }],
                response_blake3: String::new(),
                fetched_at_ms: 0,
            },
        );
        let fetcher = FixtureClobBookFetcher::new(books);
        let book = fetcher.fetch_book("tok-1").await.unwrap();
        assert_eq!(book.best_ask(), Some(dec!(0.6)));
    }

    #[tokio::test]
    async fn fixture_fetcher_unknown_token_is_missing_fixture() {
        let fetcher = FixtureClobBookFetcher::new(HashMap::new());
        let err = fetcher.fetch_book("nope").await.unwrap_err();
        assert!(
            matches!(err, ClobBookError::MissingFixture(_)),
            "got {err:?}"
        );
    }
}
