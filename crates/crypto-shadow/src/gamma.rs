//! BTC up/down market enumeration via the Polymarket Gamma API, built over the
//! reusable [`PageFetcher`] abstraction (mirrors `pe-paper-pnl`'s
//! `GammaResolutionFetcher<F>` / `pe-bootstrap`'s `GammaFetcher<F>`).
//!
//! Production wires a `ReqwestFetcher`; tests inject a `FixtureFetcher` for
//! deterministic, no-live-network coverage.

use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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
    #[serde(rename = "startDate")]
    start_date: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    /// `orderPriceMinTickSize` arrives as a JSON number (`0.01`) on live
    /// `/events` and as a string (`"0.01"`) elsewhere; hold the raw value and
    /// convert in `parse_market` (issue #300 fix 1).
    #[serde(rename = "orderPriceMinTickSize")]
    tick: Option<serde_json::Value>,
}

fn iso_to_ms(s: &str) -> Option<i64> {
    let odt = OffsetDateTime::parse(s, &Rfc3339).ok()?;
    i64::try_from(odt.unix_timestamp_nanos() / 1_000_000).ok()
}

fn parse_market(m: &GammaMarketJson, series: BtcSeriesKind) -> Option<BtcMarketMeta> {
    let condition_id = m.condition_id.clone()?;
    let token_ids_raw = m.clob_token_ids.as_deref()?;
    let token_ids: Vec<String> = serde_json::from_str(token_ids_raw).ok()?;
    let yes_token_id = token_ids.into_iter().next()?;
    let range_start_ms = iso_to_ms(m.start_date.as_deref()?)?;
    let range_end_ms = iso_to_ms(m.end_date.as_deref()?)?;
    let tick = m
        .tick
        .as_ref()
        .and_then(decimal_from_json_number_or_string)
        .unwrap_or_else(default_tick);
    Some(BtcMarketMeta {
        condition_id,
        yes_token_id,
        series,
        range_start_ms,
        range_end_ms,
        tick,
    })
}

/// Parse a Gamma `/events` response body into the markets for one series.
/// Markets missing required fields are skipped (not fatal).
pub fn parse_events(bytes: &[u8], series: BtcSeriesKind) -> Result<Vec<BtcMarketMeta>, GammaError> {
    let events: Vec<GammaEvent> =
        serde_json::from_slice(bytes).map_err(|e| GammaError::Parse(e.to_string()))?;
    let mut out = Vec::new();
    for ev in events {
        for m in &ev.markets {
            if let Some(meta) = parse_market(m, series) {
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

    const EVENTS_5M: &str = r#"[
      {"markets":[
        {"conditionId":"0xcond5","clobTokenIds":"[\"0xyes5\",\"0xno5\"]",
         "startDate":"2026-06-08T12:00:00Z","endDate":"2026-06-08T12:05:00Z",
         "orderPriceMinTickSize":"0.01"}
      ]}
    ]"#;

    #[test]
    fn parses_events_into_market_meta() {
        let markets = parse_events(EVENTS_5M.as_bytes(), BtcSeriesKind::Five).unwrap();
        assert_eq!(markets.len(), 1);
        let m = &markets[0];
        assert_eq!(m.condition_id, "0xcond5");
        assert_eq!(m.yes_token_id, "0xyes5");
        assert_eq!(m.series, BtcSeriesKind::Five);
        assert_eq!(m.tick, dec!(0.01));
        assert_eq!(m.range_end_ms - m.range_start_ms, 5 * 60 * 1000);
    }

    #[test]
    fn parses_numeric_tick_size() {
        // Live Gamma sends `orderPriceMinTickSize` as a JSON number, not a
        // string (issue #300 fix 1 / AC1.1).
        let body = r#"[{"markets":[
          {"conditionId":"0xcondN","clobTokenIds":"[\"0xyesN\",\"0xnoN\"]",
           "startDate":"2026-06-08T12:00:00Z","endDate":"2026-06-08T12:05:00Z",
           "orderPriceMinTickSize":0.01}
        ]}]"#;
        let markets = parse_events(body.as_bytes(), BtcSeriesKind::Five).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].tick, dec!(0.01));
    }

    #[test]
    fn skips_markets_missing_required_fields() {
        let body = r#"[{"markets":[{"conditionId":"0xonly"}]}]"#;
        let markets = parse_events(body.as_bytes(), BtcSeriesKind::Five).unwrap();
        assert!(markets.is_empty());
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
