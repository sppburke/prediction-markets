//! Polymarket CLOB price-history client (issue #421 PR4 — the CLV data pipeline).
//!
//! Fetches the historical price series for one CLOB token (asset id) via
//! `GET {base}/prices-history?market={token}&startTs=&endTs=&fidelity=`. The
//! `pe-bootstrap prices-history` subcommand uses it to materialize a coarse
//! pre-resolution series per market into `market_price_history`, which the ranker
//! bake-off's Closing-Line-Value estimator reads (the proxy-CLV axis runs off the
//! trades parquet without it; this client backs the *true*-CLV axis).
//!
//! ## API contract (docs.polymarket.com OpenAPI, last checked 2026-06-23)
//! - Host: `https://clob.polymarket.com` (the shared `clob_base_url`).
//! - `GET /prices-history` query params: `market` (the token/asset id, **not** the
//!   `0x` condition id), `startTs`/`endTs` (unix seconds), `fidelity` (granularity
//!   in MINUTES; default 1). Response: `{"history":[{"t":<unix u32>,"p":<float>}]}`.
//! - Rate limit: 1000 req/10s on the GET. The subcommand drives this client with a
//!   **dedicated** [`ReqwestFetcher`](crate::ReqwestFetcher) whose
//!   `with_min_interval_ms` is lowered to ~10ms (≈100 req/s) so it does not loosen
//!   the 50ms gate that protects the stricter Data-API (`/trades`/`/activity`) loops.
//!
//! ## Scope note
//! Only the singular `GET /prices-history` is implemented here. The batch
//! `POST /batch-prices-history` (`maxItems:20`) is deferred to #418 (its consumer —
//! the order-flow pipeline that builds on `market_price_history`): adding it would
//! mean a POST extension to the GET-only [`PageFetcher`] trait, and PR4's CLV need
//! is fully served by per-token GET over each market's distinct pre-resolution
//! window (a single batch window cannot match per-market close times).

use pe_source_core::SourceError;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::fetcher::PageFetcher;

/// Lowered request gate for the CLOB `/prices-history` endpoint: ~100 req/s, under the documented
/// 1000 req/10s limit. Pass to [`ReqwestFetcher::with_min_interval_ms`](crate::ReqwestFetcher::with_min_interval_ms)
/// on the *dedicated* fetcher for this client. Canonical `clob_prices_history_min_interval_ms` in
/// `docs/_GLOSSARY.md`.
pub const CLOB_PRICES_HISTORY_MIN_INTERVAL_MS: u64 = 10;

/// Default series granularity in MINUTES. 60 = hourly — the coarse pre-resolution series the CLV
/// bake-off needs (testing CLV at 1h/6h/24h-before-close, not the final tick, defuses steam-chasing
/// noise). Canonical `clob_prices_history_fidelity_minutes` in `docs/_GLOSSARY.md`.
pub const CLOB_PRICES_HISTORY_FIDELITY_MINUTES: u32 = 60;

/// One sample of a CLOB token's price series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PricePoint {
    /// Sample time, unix seconds.
    pub t: i64,
    /// Mid price at `t` (in `[0,1]`). Held as [`Decimal`] — never `f64` — so it round-trips into the
    /// decimal-string `market_price_history.price` column without precision loss.
    pub price: Decimal,
}

/// Errors from [`ClobPricesHistoryClient::fetch_prices_history`].
///
/// A per-token HTTP `4xx` ([`SourceError::Fatal`]) is **not** an error: an unknown/closed token or a
/// market with no series yields an empty `Vec` so a single bad token never aborts a million-token
/// backfill. Only non-fatal fetch failures (transient/5xx after retries, or rate-limited) and gross
/// response corruption surface here.
#[derive(Debug, thiserror::Error)]
pub enum ClobPricesHistoryError {
    /// A non-fatal fetch failure (transient/5xx after retries, or rate-limited after the gate). The
    /// backfill loop logs and skips the token; the row is absent, so a re-run retries it.
    #[error("clob prices-history fetch: {0}")]
    Fetch(String),
    /// A response body was not the expected `{"history":[{"t","p"}]}` JSON shape.
    #[error("clob prices-history parse: {0}")]
    Parse(String),
}

/// CLOB `/prices-history` client. Generic over [`PageFetcher`] so production uses
/// [`ReqwestFetcher`](crate::ReqwestFetcher) and tests use [`FixtureFetcher`](crate::FixtureFetcher).
pub struct ClobPricesHistoryClient<F: PageFetcher> {
    base_url: String,
    fetcher: F,
    fidelity_minutes: u32,
}

impl<F: PageFetcher + Send + Sync> ClobPricesHistoryClient<F> {
    /// Build a client with the canonical hourly fidelity.
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            base_url,
            fetcher,
            fidelity_minutes: CLOB_PRICES_HISTORY_FIDELITY_MINUTES,
        }
    }

    /// Override the series granularity in minutes (clamped to ≥ 1).
    pub fn with_fidelity_minutes(mut self, minutes: u32) -> Self {
        self.fidelity_minutes = minutes.max(1);
        self
    }

    /// Fetch the price series for `token_id` over `[start_ts, end_ts]` at the configured fidelity.
    ///
    /// Returns the points in the order Gamma/CLOB returns them (ascending `t` in practice; the caller
    /// must not assume sortedness). An empty `Vec` means either the market had no series in the window
    /// or the token 4xx-ed (unknown/closed) — both are the empty case, not an error.
    ///
    /// # Errors
    /// [`ClobPricesHistoryError::Fetch`] on a non-fatal source error (transient/rate-limited);
    /// [`ClobPricesHistoryError::Parse`] on a malformed response body.
    pub async fn fetch_prices_history(
        &self,
        token_id: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<PricePoint>, ClobPricesHistoryError> {
        let url = build_prices_history_url(
            &self.base_url,
            token_id,
            start_ts,
            end_ts,
            self.fidelity_minutes,
        );
        let bytes = match self.fetcher.fetch_page(&url).await {
            Ok(b) => b,
            Err(SourceError::Fatal { message }) => {
                // 4xx: unknown/closed token or no series — empty, not a hard failure (so the backfill
                // never aborts on one bad token). Distinct from a non-fatal error, which is surfaced.
                tracing::debug!(token_id, error = %message, "clob_prices_history: fatal → empty series");
                return Ok(Vec::new());
            }
            Err(e) => return Err(ClobPricesHistoryError::Fetch(e.to_string())),
        };

        let parsed: PricesHistoryResponseRaw = serde_json::from_slice(&bytes)
            .map_err(|e| ClobPricesHistoryError::Parse(e.to_string()))?;
        Ok(parsed
            .history
            .into_iter()
            .map(|r| PricePoint { t: r.t, price: r.p })
            .collect())
    }
}

/// Build the `GET /prices-history` URL: `{base}/prices-history?market=&startTs=&endTs=&fidelity=`.
///
/// `market` is the CLOB token/asset id. Param order is fixed so [`FixtureFetcher`](crate::FixtureFetcher)
/// exact-URL keying is deterministic in tests.
pub(crate) fn build_prices_history_url(
    base: &str,
    token_id: &str,
    start_ts: i64,
    end_ts: i64,
    fidelity_minutes: u32,
) -> String {
    format!(
        "{base}/prices-history?market={token_id}&startTs={start_ts}&endTs={end_ts}&fidelity={fidelity_minutes}"
    )
}

/// Serde DTO for the `/prices-history` response: `{"history":[{"t","p"}]}`. `history` defaults to empty
/// so a `{}` or `{"history":null}`-style body degrades to no points rather than erroring.
#[derive(Deserialize)]
struct PricesHistoryResponseRaw {
    #[serde(default)]
    history: Vec<MarketPriceRaw>,
}

/// One `{"t":<unix>,"p":<float>}` point. `p` is decoded into [`Decimal`] at the boundary via
/// [`deserialize_price`] (CLOB sends it as a JSON float) so no `f64` enters the price domain.
#[derive(Deserialize)]
struct MarketPriceRaw {
    t: i64,
    #[serde(deserialize_with = "deserialize_price")]
    p: Decimal,
}

/// Deserialize the CLOB `p` field — a JSON float in practice, but a string or int is also accepted —
/// into [`Decimal`]. Mirrors `gamma_markets::deserialize_decimal_flexible` but is non-optional (a
/// price point with no parseable price is a malformed body, so it errors rather than dropping silently).
fn deserialize_price<'de, D>(d: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use rust_decimal::prelude::FromPrimitive;
    use serde::de::Error as DeError;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flex {
        Str(String),
        Float(f64),
        Int(i64),
    }

    match Flex::deserialize(d)? {
        Flex::Str(s) => s.parse::<Decimal>().map_err(DeError::custom),
        Flex::Float(f) => Decimal::from_f64(f)
            .ok_or_else(|| DeError::custom(format!("price f64 {f} → Decimal failed"))),
        Flex::Int(i) => Ok(Decimal::from(i)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::fetcher::FixtureFetcher;

    const BASE: &str = "https://clob.polymarket.com";

    #[test]
    fn build_url_has_fixed_param_order() {
        let url = build_prices_history_url(BASE, "12345", 1_000, 2_000, 60);
        assert_eq!(
            url,
            "https://clob.polymarket.com/prices-history?market=12345&startTs=1000&endTs=2000&fidelity=60"
        );
    }

    #[test]
    fn deserialize_price_accepts_float_string_int() {
        // CLOB sends `p` as a float; a string or int is tolerated. All decode to Decimal (no f64
        // leaks into the price domain).
        let raw: MarketPriceRaw = serde_json::from_str(r#"{"t":1,"p":0.62}"#).unwrap();
        assert_eq!(raw.p, Decimal::new(62, 2));
        let raw: MarketPriceRaw = serde_json::from_str(r#"{"t":2,"p":"0.38"}"#).unwrap();
        assert_eq!(raw.p, Decimal::new(38, 2));
        let raw: MarketPriceRaw = serde_json::from_str(r#"{"t":3,"p":1}"#).unwrap();
        assert_eq!(raw.p, Decimal::ONE);
    }

    #[test]
    fn response_parses_history_and_defaults_empty() {
        let resp: PricesHistoryResponseRaw =
            serde_json::from_str(r#"{"history":[{"t":100,"p":0.5},{"t":160,"p":0.55}]}"#).unwrap();
        assert_eq!(resp.history.len(), 2);
        let empty: PricesHistoryResponseRaw = serde_json::from_str("{}").unwrap();
        assert!(
            empty.history.is_empty(),
            "missing history → empty, not error"
        );
    }

    fn client(map: HashMap<String, Vec<u8>>) -> ClobPricesHistoryClient<FixtureFetcher> {
        ClobPricesHistoryClient::new(BASE.to_owned(), FixtureFetcher::new(map))
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn fetch_returns_points_on_happy_path() {
        let mut map = HashMap::new();
        map.insert(
            build_prices_history_url(BASE, "tok", 1_000, 2_000, 60),
            br#"{"history":[{"t":1000,"p":0.4},{"t":1600,"p":0.7}]}"#.to_vec(),
        );
        let points = rt()
            .block_on(client(map).fetch_prices_history("tok", 1_000, 2_000))
            .unwrap();
        assert_eq!(
            points,
            vec![
                PricePoint {
                    t: 1000,
                    price: Decimal::new(4, 1)
                },
                PricePoint {
                    t: 1600,
                    price: Decimal::new(7, 1)
                },
            ]
        );
    }

    #[test]
    fn fetch_treats_unknown_token_4xx_as_empty() {
        // FixtureFetcher returns SourceError::Fatal for an unmapped URL → empty series, not an error.
        let points = rt()
            .block_on(client(HashMap::new()).fetch_prices_history("missing", 0, 1))
            .unwrap();
        assert!(points.is_empty(), "4xx token must yield an empty series");
    }
}
