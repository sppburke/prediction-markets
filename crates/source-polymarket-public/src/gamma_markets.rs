//! Shared batched Polymarket Gamma `/markets` client (issue #382).
//!
//! Fetches many markets per request via **repeat-key batching**:
//! `GET {base}/markets?condition_ids=A&condition_ids=B&…&limit=500[&closed=true]`,
//! fanning out [`GAMMA_CONCURRENCY`] requests at once and demuxing each response array by
//! `conditionId` into a `HashMap`.
//!
//! A Tier-1 live probe (issue #382 Phase 0, `scripts/probe_gamma_ua.py`, 2026-06-20) established:
//! - Repeat-key batching works for **both** the plain (open) and `&closed=true` variants — up to
//!   ≥ 100 ids per request, demuxed cleanly with no cross-market leak. Comma-separated joining
//!   (`condition_ids=A,B`) fails (returns 0), so repeat-key is mandatory. This supersedes the old
//!   per-ID-only assumption (the pre-#382 bootstrap client wrongly claimed all batching "fails
//!   silently").
//! - The `&closed=true` 403 is triggered by the literal `Python-urllib/*` default User-Agent (an
//!   anti-bot blocklist), **not** by a missing browser UA: a bare `reqwest::Client` (no UA header)
//!   returns 200. [`GAMMA_BROWSER_UA`] is therefore a defensive, self-identifying UA, not a
//!   correctness requirement.
//!
//! The client is **pure fetch + parse + demux**. Cache writes, TTL, skip-sets, and
//! resolution/`yes_won` logic stay at each call site.

use std::collections::{HashMap, HashSet};

use futures::stream::{self, StreamExt};
use pe_source_core::SourceError;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::fetcher::PageFetcher;

/// condition_ids per repeat-key request. Canonical default `gamma_batch_size` in `docs/_GLOSSARY.md`.
pub const GAMMA_BATCH_SIZE: usize = 50;
/// `&limit=` value appended to batched requests. Canonical `gamma_batch_limit_param` in `_GLOSSARY.md`.
pub const GAMMA_BATCH_LIMIT_PARAM: u32 = 500;
/// In-flight batched requests per fetch (the global 20 req/s gate still applies via `ReqwestFetcher`).
pub const GAMMA_CONCURRENCY: usize = 10;
/// Defensive self-identifying User-Agent. Canonical `gamma_browser_ua` in `_GLOSSARY.md`. NOT a
/// correctness requirement — the probe showed a UA-less request returns 200; only `Python-urllib/*`
/// 403s. Setting it guards against a future bot-flagged default.
pub const GAMMA_BROWSER_UA: &str = "Mozilla/5.0 (X11; Linux x86_64) prediction-edge/1.0";

/// Which slice of the market universe a [`GammaMarketsClient::fetch_markets`] call targets.
///
/// `OpenOnly` is the plain endpoint (open markets, current `liquidity`); `ClosedOnly` adds
/// `&closed=true` (the only variant that returns `endDate` for resolved markets).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketFilter {
    OpenOnly,
    ClosedOnly,
}

impl MarketFilter {
    /// The query-string fragment appended after the `condition_ids=` keys.
    fn closed_param(self) -> &'static str {
        match self {
            MarketFilter::OpenOnly => "",
            MarketFilter::ClosedOnly => "&closed=true",
        }
    }
}

/// A demuxed Gamma `/markets` row.
///
/// Minimal fields for the bootstrap schedule + liquidity passes. Extended in issue #382 Phase 3
/// (`outcome_prices` / `clob_token_ids`) when `pe-service` / `pe-paper-pnl` / `pe-crypto-shadow`
/// migrate onto this client.
#[derive(Clone, Debug)]
pub struct GammaMarket {
    /// The market's condition id (the demux key — echoed by Gamma as `conditionId`).
    pub condition_id: String,
    /// Scheduled close time as unix seconds, parsed from `endDate`. `None` when Gamma omits or
    /// returns an unparseable `endDate` (the caller writes a NULL schedule row in that case).
    pub end_date_unix: Option<i64>,
    /// Current order-book depth indicator (USD). `None` when Gamma omits the field.
    pub liquidity: Option<Decimal>,
}

/// Errors from [`GammaMarketsClient::fetch_markets`].
///
/// A per-chunk HTTP `4xx` (`SourceError::Fatal`) is **not** an error: those ids are skipped (absent
/// from the returned map), mirroring the pre-#382 per-ID `Fatal → skip` behaviour. Only non-fatal
/// fetch failures (transient/5xx after retries, or rate-limited) and gross response corruption abort.
#[derive(Debug, thiserror::Error)]
pub enum GammaMarketsError {
    /// A non-fatal fetch failure (transient/5xx after retries, or rate-limited). The pass should
    /// abort — the pre-#382 per-ID loops likewise returned `Err` on any non-`Fatal` source error.
    #[error("gamma batch fetch: {0}")]
    Fetch(String),
    /// A batch response was not a valid `/markets` JSON array — surfaced rather than silently
    /// dropped. Per-market missing/optional fields stay lenient (`None`), so this fires only on
    /// gross corruption of the whole array.
    #[error("gamma batch parse: {0}")]
    Parse(String),
}

/// Batched Gamma `/markets` client. Generic over [`PageFetcher`] so production uses
/// [`ReqwestFetcher`](crate::ReqwestFetcher) and tests use [`FixtureFetcher`](crate::FixtureFetcher).
pub struct GammaMarketsClient<F: PageFetcher> {
    base_url: String,
    fetcher: F,
    batch_size: usize,
    concurrency: usize,
    limit: u32,
}

impl<F: PageFetcher + Send + Sync> GammaMarketsClient<F> {
    /// Build a client with the canonical batch size / concurrency / limit.
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            base_url,
            fetcher,
            batch_size: GAMMA_BATCH_SIZE,
            concurrency: GAMMA_CONCURRENCY,
            limit: GAMMA_BATCH_LIMIT_PARAM,
        }
    }

    /// Override the batch size (clamped to ≥ 1). Used by tests to force chunk boundaries with few ids.
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    /// Fetch every id in `ids` under `filter`, returning `conditionId → GammaMarket` for each market
    /// Gamma returns. Ids Gamma omits (unknown markets — Gamma answers `200 []`) are simply absent
    /// from the map; the caller decides what a miss means (e.g. write a NULL schedule row).
    ///
    /// Input order is preserved through dedup and chunking so the batch URLs are deterministic
    /// (important for `FixtureFetcher` exact-URL keying).
    ///
    /// # Errors
    /// Returns [`GammaMarketsError::Fetch`] on a non-fatal source error (transient/rate-limited) and
    /// [`GammaMarketsError::Parse`] on a malformed batch array. A per-chunk `4xx` is skipped, not an
    /// error (its ids are absent from the map).
    pub async fn fetch_markets(
        &self,
        ids: &[String],
        filter: MarketFilter,
    ) -> Result<HashMap<String, GammaMarket>, GammaMarketsError> {
        // Dedup preserving first-seen order — deterministic batch URLs, no sort.
        let mut seen: HashSet<&str> = HashSet::with_capacity(ids.len());
        let unique: Vec<&str> = ids
            .iter()
            .map(String::as_str)
            .filter(|id| seen.insert(id))
            .collect();

        let chunks: Vec<Vec<String>> = unique
            .chunks(self.batch_size)
            .map(|c| c.iter().map(|s| (*s).to_owned()).collect())
            .collect();

        let base = self.base_url.as_str();
        let fetcher = &self.fetcher;
        let closed = filter.closed_param();
        let limit = self.limit;

        let mut stream = stream::iter(chunks)
            .map(|chunk| async move {
                let url = build_batch_url(base, &chunk, closed, limit);
                let result = fetcher.fetch_page(&url).await;
                (chunk, result)
            })
            .buffer_unordered(self.concurrency);

        let mut out: HashMap<String, GammaMarket> = HashMap::new();
        while let Some((chunk, result)) = stream.next().await {
            let bytes = match result {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    // 4xx on the whole chunk — skip its ids (absent from map), as the per-ID path did.
                    tracing::warn!(chunk_len = chunk.len(), error = %message, "gamma_markets: batch fatal, skipping chunk");
                    continue;
                }
                Err(e) => return Err(GammaMarketsError::Fetch(e.to_string())),
            };

            let markets: Vec<GammaMarketRaw> = serde_json::from_slice(&bytes)
                .map_err(|e| GammaMarketsError::Parse(e.to_string()))?;
            for m in markets {
                let end_date_unix = m.end_date.as_deref().and_then(parse_end_date_unix);
                out.insert(
                    m.condition_id.clone(),
                    GammaMarket {
                        condition_id: m.condition_id,
                        end_date_unix,
                        liquidity: m.liquidity,
                    },
                );
            }
        }
        Ok(out)
    }
}

/// Build the repeat-key batch URL: `{base}/markets?condition_ids=A&condition_ids=B[&closed=true]&limit=N`.
///
/// Query-param order is `condition_ids…` → `&closed=true` → `&limit=` (matches the proven
/// `scripts/backfill_end_dates.py`). Input order is preserved so the URL is deterministic.
pub(crate) fn build_batch_url(base: &str, ids: &[String], closed: &str, limit: u32) -> String {
    let mut url = String::with_capacity(base.len() + ids.len() * 80 + closed.len() + 16);
    url.push_str(base);
    url.push_str("/markets?");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            url.push('&');
        }
        url.push_str("condition_ids=");
        url.push_str(id);
    }
    url.push_str(closed);
    url.push_str("&limit=");
    url.push_str(&limit.to_string());
    url
}

/// Parse Gamma's `endDate` (RFC 3339, e.g. `"2024-11-04T00:00:00Z"`) to unix seconds. `None` on any
/// parse failure — the caller treats an unparseable `endDate` the same as a missing one (NULL row).
fn parse_end_date_unix(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map(|dt| dt.unix_timestamp())
        .ok()
}

/// Serde DTO for one element of the `/markets` response array. Extra fields are ignored.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketRaw {
    condition_id: String,
    /// Scheduled close date, RFC 3339. Present on open and closed markets; `None` only when omitted.
    end_date: Option<String>,
    /// Order-book depth indicator (USD). Gamma sends a JSON number; fixtures may use a string —
    /// [`deserialize_decimal_flexible`] accepts both.
    #[serde(default, deserialize_with = "deserialize_decimal_flexible")]
    liquidity: Option<Decimal>,
}

/// Deserialize a JSON value (number or string) into `Option<Decimal>`.
///
/// Gamma returns numeric fields as JSON numbers, but fixtures and some surfaces serialize `Decimal`
/// as a string. Accepting both keeps DTOs robust to upstream format drift without losing precision.
/// Shared across the workspace (e.g. `pe-bootstrap`'s events sweep) — see issue #382.
pub fn deserialize_decimal_flexible<'de, D>(d: D) -> Result<Option<Decimal>, D::Error>
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

    let Some(v) = Option::<Flex>::deserialize(d)? else {
        return Ok(None);
    };
    match v {
        Flex::Str(s) => s.parse::<Decimal>().map(Some).map_err(DeError::custom),
        Flex::Float(f) => Decimal::from_f64(f)
            .map(Some)
            .ok_or_else(|| DeError::custom(format!("decimal f64 {f} → Decimal failed"))),
        Flex::Int(i) => Ok(Some(Decimal::from(i))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn build_batch_url_repeat_key_open() {
        let url = build_batch_url("https://g", &ids(&["0xA", "0xB"]), "", 500);
        assert_eq!(
            url,
            "https://g/markets?condition_ids=0xA&condition_ids=0xB&limit=500"
        );
    }

    #[test]
    fn build_batch_url_repeat_key_closed_param_order() {
        let url = build_batch_url("https://g", &ids(&["0xA"]), "&closed=true", 500);
        assert_eq!(
            url,
            "https://g/markets?condition_ids=0xA&closed=true&limit=500"
        );
        // condition_ids before closed before limit — deterministic for fixture keying.
        assert!(url.find("condition_ids=").unwrap() < url.find("closed=true").unwrap());
        assert!(url.find("closed=true").unwrap() < url.find("limit=").unwrap());
    }

    #[test]
    fn build_batch_url_preserves_input_order() {
        let url = build_batch_url("https://g", &ids(&["0xC", "0xA", "0xB"]), "", 500);
        assert_eq!(
            url,
            "https://g/markets?condition_ids=0xC&condition_ids=0xA&condition_ids=0xB&limit=500"
        );
    }

    #[test]
    fn parse_end_date_unix_rfc3339() {
        // 2020-11-04T00:00:00Z = 1604448000
        assert_eq!(
            parse_end_date_unix("2020-11-04T00:00:00Z"),
            Some(1_604_448_000)
        );
        assert_eq!(parse_end_date_unix("not-a-date"), None);
    }

    #[test]
    fn gamma_market_raw_parses_number_and_string_liquidity() {
        let json = r#"[{"conditionId":"0xA","endDate":"2024-01-15T00:00:00Z","liquidity":12345.6},
                       {"conditionId":"0xB","liquidity":"7.5"},
                       {"conditionId":"0xC"}]"#;
        let raws: Vec<GammaMarketRaw> = serde_json::from_slice(json.as_bytes()).unwrap();
        assert_eq!(raws.len(), 3);
        assert_eq!(raws[0].liquidity, Some(Decimal::new(123_456, 1)));
        assert_eq!(raws[1].liquidity, Some(Decimal::new(75, 1)));
        assert_eq!(raws[2].liquidity, None);
        assert_eq!(raws[2].end_date, None);
    }
}
