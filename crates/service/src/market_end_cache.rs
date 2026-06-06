//! In-memory cache for a market's **resolution time** (when the contract expires).
//!
//! "Resolution time" is, in priority order:
//! 1. `umaEndDate` — the on-chain UMA resolution timestamp. The exact actual
//!    resolution, but present only once the market has resolved.
//! 2. `endDate` — the scheduled market close. Empirically present on every market
//!    (100/100 non-sports and 78/78 sports sampled), so it is the always-available
//!    forward expiration while a market is still open. For sports it is the game's
//!    scheduled end (occasionally approximate), which is immaterial to a 72h gate.
//!
//! Gamma's plain `/markets?condition_ids={id}` returns only OPEN markets; resolved
//! markets require `&closed=true`. We try the plain query first (the live case the
//! gate cares about), then fall back to `&closed=true` (resolved markets, for the
//! dashboard).
//!
//! Cache semantics: absent from map → not yet fetched; a stored [`MarketResolution`]
//! with `resolution_unix = None` means we could not determine a resolution time.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::MarketId;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketRaw {
    /// On-chain UMA resolution timestamp, RFC 3339 (e.g. `"2026-06-06T03:45:40Z"`).
    /// Present only once the market has resolved.
    uma_end_date: Option<String>,
    /// Scheduled market close, RFC 3339 (e.g. `"2026-06-06T00:30:00Z"`). Present on
    /// every market — the always-available forward expiration before resolution.
    end_date: Option<String>,
    /// `open` / `proposed` / `resolved` (or absent).
    uma_resolution_status: Option<String>,
}

/// A market's resolution timing, as cached.
#[derive(Clone, Default, Debug)]
pub struct MarketResolution {
    /// When the contract expires/resolves (Unix seconds), or `None` if unknown.
    pub resolution_unix: Option<i64>,
    /// UMA resolution status (`resolved`/`proposed`/…), for display.
    pub status: Option<String>,
}

/// Thread-safe market-resolution cache shared between the orchestrator gate and
/// the dashboard so both reference the same resolution time.
#[derive(Clone)]
pub struct MarketEndCache {
    inner: Arc<Mutex<HashMap<MarketId, MarketResolution>>>,
    client: reqwest::Client,
    gamma_base_url: String,
}

impl MarketEndCache {
    pub fn new(gamma_base_url: String) -> Self {
        // build() only fails on invalid TLS config; our config has none.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            client,
            gamma_base_url,
        }
    }

    /// Resolution timing for `market_id`, fetching from Gamma if not yet seen.
    pub async fn resolution(&self, market_id: &MarketId) -> MarketResolution {
        // Fast path: already cached.
        {
            let map = self.inner.lock().await;
            if let Some(cached) = map.get(market_id) {
                return cached.clone();
            }
        }

        let res = self.fetch(market_id).await;

        // Cache the result (even an unknown) so we don't re-fetch on every signal.
        self.inner
            .lock()
            .await
            .insert(market_id.clone(), res.clone());
        res
    }

    /// Resolution time (Unix seconds) for `market_id`, or `None` if unknown.
    /// Convenience for the horizon gate.
    pub async fn resolution_unix(&self, market_id: &MarketId) -> Option<i64> {
        self.resolution(market_id).await.resolution_unix
    }

    async fn fetch(&self, market_id: &MarketId) -> MarketResolution {
        // Open markets answer the plain query; resolved markets need `&closed=true`.
        if let Some(m) = self.query(market_id, false).await {
            return extract(&m);
        }
        if let Some(m) = self.query(market_id, true).await {
            return extract(&m);
        }
        MarketResolution::default()
    }

    /// Fetch a single market object by condition id. `closed` toggles the
    /// `&closed=true` filter required to surface resolved markets.
    async fn query(&self, market_id: &MarketId, closed: bool) -> Option<GammaMarketRaw> {
        let url = if closed {
            format!(
                "{}/markets?condition_ids={}&closed=true",
                self.gamma_base_url, market_id
            )
        } else {
            format!(
                "{}/markets?condition_ids={}",
                self.gamma_base_url, market_id
            )
        };
        let bytes = match self.client.get(&url).send().await {
            Ok(r) => match r.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(%market_id, error = %e, "market-resolution: response body error");
                    return None;
                }
            },
            Err(e) => {
                warn!(%market_id, error = %e, "market-resolution: fetch error");
                return None;
            }
        };

        let markets: Vec<GammaMarketRaw> = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                warn!(%market_id, error = %e, "market-resolution: JSON parse error");
                return None;
            }
        };
        markets.into_iter().next()
    }
}

/// Build a [`MarketResolution`] from a raw market: prefer the exact resolution
/// (`umaEndDate`), fall back to the always-present scheduled close (`endDate`).
fn extract(m: &GammaMarketRaw) -> MarketResolution {
    let resolution_unix = m
        .uma_end_date
        .as_deref()
        .and_then(parse_timestamp)
        .or_else(|| m.end_date.as_deref().and_then(parse_timestamp));
    MarketResolution {
        resolution_unix,
        status: m.uma_resolution_status.clone(),
    }
}

/// Parse a Gamma timestamp to Unix seconds. `umaEndDate`/`endDate` are RFC 3339
/// (`...Z`); we also accept the Postgres form (`2026-06-06 00:30:00+00`) that some
/// Gamma date fields use, so the parser is robust to either.
fn parse_timestamp(s: &str) -> Option<i64> {
    use time::format_description::well_known::Rfc3339;

    if let Ok(dt) = time::OffsetDateTime::parse(s, &Rfc3339) {
        return Some(dt.unix_timestamp());
    }
    // Normalise Postgres form: space → 'T', bare "+00" → "+00:00".
    let mut norm = s.replace(' ', "T");
    if norm.ends_with("+00") {
        norm.push_str(":00");
    }
    time::OffsetDateTime::parse(&norm, &Rfc3339)
        .map(|dt| dt.unix_timestamp())
        .ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn prefers_uma_end_date_over_end_date() {
        let m = GammaMarketRaw {
            uma_end_date: Some("2026-06-06T03:45:40Z".into()),
            end_date: Some("2026-06-06T00:30:00Z".into()),
            uma_resolution_status: Some("resolved".into()),
        };
        // 2026-06-06T03:45:40Z (the exact resolution, not the scheduled close)
        assert_eq!(extract(&m).resolution_unix, Some(1_780_717_540));
    }

    #[test]
    fn falls_back_to_end_date_when_unresolved() {
        let m = GammaMarketRaw {
            uma_end_date: None,
            end_date: Some("2026-06-06T00:30:00Z".into()),
            uma_resolution_status: Some("proposed".into()),
        };
        // 2026-06-06T00:30:00Z (always-present scheduled close)
        assert_eq!(extract(&m).resolution_unix, Some(1_780_705_800));
    }

    #[test]
    fn unknown_when_neither_present() {
        let m = GammaMarketRaw {
            uma_end_date: None,
            end_date: None,
            uma_resolution_status: None,
        };
        assert_eq!(extract(&m).resolution_unix, None);
    }

    #[test]
    fn parses_both_timestamp_forms() {
        assert_eq!(parse_timestamp("2026-06-06T00:30:00Z"), Some(1_780_705_800));
        assert_eq!(
            parse_timestamp("2026-06-06 00:30:00+00"),
            Some(1_780_705_800)
        );
        assert_eq!(parse_timestamp("not-a-date"), None);
    }
}
