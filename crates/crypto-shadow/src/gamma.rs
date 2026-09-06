//! BTC up/down market enumeration via the Polymarket Gamma API, built over the
//! reusable [`PageFetcher`] abstraction shared with the surviving
//! [`GammaMarketsClient`](pe_source_polymarket_public::GammaMarketsClient) owner.
//!
//! Production wires a `ReqwestFetcher`; tests inject a `FixtureFetcher` for
//! deterministic, no-live-network coverage.
//!
//! **Issue #300 fix 2 (AC2.1):** the 5m/15m settlement window is derived from
//! the **event slug** unix timestamp (`btc-updown-5m-<unix_start>`), *not* from
//! the market's `startDate`/`endDate` — live Gamma reports those as a ≈1-day
//! span for these markets, so the old ISO-date derivation produced day-long
//! windows. The slug's trailing unix-seconds value is the window open.

use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::types::{BtcMarketMeta, BtcSeriesKind};

/// Default order price tick when Gamma omits it (per issue: `tick = 0.01`).
fn default_tick() -> Decimal {
    Decimal::new(1, 2) // 0.01
}

/// Parse a tick size that Gamma encodes as either a JSON number (`0.01`, the
/// live `/events` shape) or a string (`"0.01"`). Both go through the value's
/// *string* form into `Decimal` — never `f64` (prices are exact decimals per
/// `CLAUDE.md`). Any other JSON shape yields `None`, so `parse_market` falls
/// back to [`default_tick`].
fn decimal_from_json_number_or_string(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::String(s) => Decimal::from_str_exact(s).ok(),
        serde_json::Value::Number(n) => Decimal::from_str_exact(&n.to_string()).ok(),
        _ => None,
    }
}

/// Derive `(range_start_ms, range_end_ms)` from a BTC up/down **event slug**
/// (`btc-updown-5m-<unix_start>`): the trailing integer is the window-open time
/// in unix **seconds**; the span is 300 s (5m) or 900 s (15m). Returns `None`
/// for any slug whose trailing token is not a plausible epoch-seconds value, so
/// a non-time slug is skipped rather than producing a bogus window.
fn window_from_event_slug(slug: &str, series: BtcSeriesKind) -> Option<(i64, i64)> {
    let last = slug.rsplit('-').next()?;
    let unix_start: i64 = last.parse().ok()?;
    // Plausible epoch-seconds sanity (≈2017-07 .. ≈2099) — rejects non-time slugs.
    if !(1_500_000_000..=4_100_000_000).contains(&unix_start) {
        return None;
    }
    let span_secs: i64 = match series {
        BtcSeriesKind::Five => 300,
        BtcSeriesKind::Fifteen => 900,
    };
    Some((unix_start * 1000, (unix_start + span_secs) * 1000))
}

/// Error enumerating markets.
#[derive(Debug, thiserror::Error)]
pub enum GammaError {
    #[error("fetch: {0}")]
    Fetch(String),
    #[error("parse: {0}")]
    Parse(String),
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    /// Event slug, e.g. `btc-updown-5m-1765192500`; carries the window-open time.
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    markets: Vec<GammaMarketJson>,
}

#[derive(Debug, Deserialize)]
struct GammaMarketJson {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    /// JSON-encoded array string, e.g. `"[\"0xyes\",\"0xno\"]"`.
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    /// `orderPriceMinTickSize` arrives as a JSON number (`0.01`) on live
    /// `/events` and as a string (`"0.01"`) elsewhere; hold the raw value and
    /// convert in `parse_market` (issue #300 fix 1).
    #[serde(rename = "orderPriceMinTickSize")]
    tick: Option<serde_json::Value>,
}

/// Build a [`BtcMarketMeta`] from one market JSON plus the window the parent
/// event slug resolved to. Markets missing required identity fields yield
/// `None` (skipped, not fatal).
fn parse_market(
    m: &GammaMarketJson,
    series: BtcSeriesKind,
    range_start_ms: i64,
    range_end_ms: i64,
) -> Option<BtcMarketMeta> {
    let condition_id = m.condition_id.clone()?;
    let token_ids_raw = m.clob_token_ids.as_deref()?;
    let token_ids: Vec<String> = serde_json::from_str(token_ids_raw).ok()?;
    // Require both outcome tokens: [0] = YES (Up), [1] = NO (Down). A market
    // missing either token is skipped (the NO book is needed to price down-moves).
    let mut it = token_ids.into_iter();
    let yes_token_id = it.next()?;
    let no_token_id = it.next()?;
    let tick = m
        .tick
        .as_ref()
        .and_then(decimal_from_json_number_or_string)
        .unwrap_or_else(default_tick);
    Some(BtcMarketMeta {
        condition_id,
        yes_token_id,
        no_token_id,
        series,
        range_start_ms,
        range_end_ms,
        tick,
    })
}

/// Parse a Gamma `/events` response body into the markets for one series.
/// Events whose slug does not resolve to a 5m/15m window are skipped; within a
/// kept event, markets missing required fields are skipped (not fatal).
pub fn parse_events(bytes: &[u8], series: BtcSeriesKind) -> Result<Vec<BtcMarketMeta>, GammaError> {
    let events: Vec<GammaEvent> =
        serde_json::from_slice(bytes).map_err(|e| GammaError::Parse(e.to_string()))?;
    let mut out = Vec::new();
    for ev in events {
        let Some((range_start_ms, range_end_ms)) = ev
            .slug
            .as_deref()
            .and_then(|s| window_from_event_slug(s, series))
        else {
            continue;
        };
        for m in &ev.markets {
            if let Some(meta) = parse_market(m, series, range_start_ms, range_end_ms) {
                out.push(meta);
            }
        }
    }
    Ok(out)
}

/// Enumerates open BTC up/down markets for a configured set of series.
pub struct BtcMarketFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> BtcMarketFetcher<F> {
    /// `base_url` is the Gamma API root, e.g. `https://gamma-api.polymarket.com`.
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// URL for one series' open-events query.
    fn url(&self, series: BtcSeriesKind) -> String {
        format!(
            "{}/events?series_slug={}&closed=false",
            self.base_url,
            series.gamma_series_slug()
        )
    }

    /// Fetch and parse all open markets across the given series.
    pub async fn fetch_markets(
        &self,
        kinds: &[BtcSeriesKind],
    ) -> Result<Vec<BtcMarketMeta>, GammaError> {
        let mut out = Vec::new();
        for &kind in kinds {
            let url = self.url(kind);
            let bytes = self
                .fetcher
                .fetch_page(&url)
                .await
                .map_err(|e| GammaError::Fetch(e.to_string()))?;
            out.extend(parse_events(&bytes, kind)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use pe_source_polymarket_public::FixtureFetcher;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    // Live-shaped event: the slug carries the 5-min window-open (unix seconds),
    // while `startDate`/`endDate` span ≈1 day (Gamma's actual shape) and are now
    // ignored. 1765192500 = 2025-12-08T12:35:00Z.
    const EVENTS_5M: &str = r#"[
      {"slug":"btc-updown-5m-1765192500",
       "startDate":"2025-12-08T00:00:00Z","endDate":"2025-12-09T00:00:00Z",
       "markets":[
        {"conditionId":"0xcond5","clobTokenIds":"[\"0xyes5\",\"0xno5\"]",
         "startDate":"2025-12-08T00:00:00Z","endDate":"2025-12-09T00:00:00Z",
         "orderPriceMinTickSize":"0.01"}
      ]}
    ]"#;

    #[test]
    fn window_comes_from_slug_not_iso_dates() {
        // AC2.1: the window is the 5-min slug span, NOT the ≈1-day ISO span.
        let markets = parse_events(EVENTS_5M.as_bytes(), BtcSeriesKind::Five).unwrap();
        assert_eq!(markets.len(), 1);
        let m = &markets[0];
        assert_eq!(m.condition_id, "0xcond5");
        assert_eq!(m.yes_token_id, "0xyes5");
        assert_eq!(m.no_token_id, "0xno5");
        assert_eq!(m.series, BtcSeriesKind::Five);
        assert_eq!(m.tick, dec!(0.01));
        assert_eq!(m.range_start_ms, 1_765_192_500_000);
        assert_eq!(m.range_end_ms - m.range_start_ms, 5 * 60 * 1000);
    }

    #[test]
    fn fifteen_minute_span_from_slug() {
        let body = r#"[{"slug":"btc-updown-15m-1765192500","markets":[
          {"conditionId":"0xc15","clobTokenIds":"[\"0xy15\",\"0xn15\"]","orderPriceMinTickSize":"0.01"}
        ]}]"#;
        let markets = parse_events(body.as_bytes(), BtcSeriesKind::Fifteen).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(
            markets[0].range_end_ms - markets[0].range_start_ms,
            15 * 60 * 1000
        );
    }

    #[test]
    fn parses_numeric_tick_size() {
        // Live Gamma sends `orderPriceMinTickSize` as a JSON number, not a
        // string (issue #300 fix 1 / AC1.1).
        let body = r#"[{"slug":"btc-updown-5m-1765192500","markets":[
          {"conditionId":"0xcondN","clobTokenIds":"[\"0xyesN\",\"0xnoN\"]","orderPriceMinTickSize":0.01}
        ]}]"#;
        let markets = parse_events(body.as_bytes(), BtcSeriesKind::Five).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].tick, dec!(0.01));
    }

    #[test]
    fn event_with_non_time_slug_is_skipped() {
        let body = r#"[{"slug":"some-other-market","markets":[
          {"conditionId":"0xx","clobTokenIds":"[\"0xy\",\"0xn\"]","orderPriceMinTickSize":"0.01"}
        ]}]"#;
        assert!(
            parse_events(body.as_bytes(), BtcSeriesKind::Five)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn skips_market_missing_required_fields() {
        // Event slug is valid, but the market lacks clobTokenIds → market skipped.
        let body = r#"[{"slug":"btc-updown-5m-1765192500","markets":[{"conditionId":"0xonly"}]}]"#;
        assert!(
            parse_events(body.as_bytes(), BtcSeriesKind::Five)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn skips_market_with_only_one_token() {
        // Both YES and NO tokens are required (the NO book prices down-moves); a
        // market exposing a single token is skipped.
        let body = r#"[{"slug":"btc-updown-5m-1765192500","markets":[
          {"conditionId":"0xc1","clobTokenIds":"[\"0xonlyyes\"]","orderPriceMinTickSize":"0.01"}
        ]}]"#;
        assert!(
            parse_events(body.as_bytes(), BtcSeriesKind::Five)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn fetcher_hits_expected_url_and_parses() {
        let mut fixtures = HashMap::new();
        fixtures.insert(
            "https://gamma.test/events?series_slug=btc-up-or-down-5m&closed=false".to_string(),
            EVENTS_5M.as_bytes().to_vec(),
        );
        let fetcher = BtcMarketFetcher::new(
            "https://gamma.test".to_string(),
            FixtureFetcher::new(fixtures),
        );
        let markets = fetcher.fetch_markets(&[BtcSeriesKind::Five]).await.unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].condition_id, "0xcond5");
    }
}
