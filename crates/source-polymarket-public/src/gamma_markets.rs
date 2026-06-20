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
/// Carries the fields the bootstrap schedule/liquidity passes and the paper-pnl resolution poller
/// need (issue #382 Phase 2/3a), plus the service mid-price cache + WS2 liquidity-snapshot fields
/// (`volume` / `clob_token_ids`, added in Phase 3b).
#[derive(Clone, Debug)]
pub struct GammaMarket {
    /// The market's condition id (the demux key — echoed by Gamma as `conditionId`).
    pub condition_id: String,
    /// Scheduled close time as unix seconds, parsed from `endDate`. `None` when Gamma omits or
    /// returns an unparseable `endDate` (the caller writes a NULL schedule row in that case).
    pub end_date_unix: Option<i64>,
    /// Current order-book depth indicator (USD). `None` when Gamma omits the field or sends an
    /// unparseable value (lenient decode — a bad scalar never fails the row).
    pub liquidity: Option<Decimal>,
    /// Whether Gamma reports the market as resolved (`closed`). `false` when the field is omitted.
    pub closed: bool,
    /// Outcome prices indexed by `outcome_id`, parsed from Gamma's `outcomePrices` JSON-string array
    /// via [`parse_outcome_prices`] — resolved markets give `[1,0]`/`[0,1]`, open markets give live
    /// mids. `None` when Gamma omits the field or the array is malformed. Individual non-decimal
    /// entries fall back to `0` (the lenient paper-pnl semantic, issue #382 Q7).
    pub outcome_prices: Option<Vec<Decimal>>,
    /// Cumulative traded volume (USD). `None` when Gamma omits or sends an unparseable value.
    /// Consumed by the service mid-price cache's WS2 liquidity snapshot (issue #382 Phase 3b).
    pub volume: Option<Decimal>,
    /// Gamma `clobTokenIds`, ordered by `outcome_id` so `clob_token_ids[outcome_id]` is that
    /// outcome's CLOB token. Empty when Gamma omits the field or it is malformed. **Positions are
    /// preserved** (no compaction) so the index stays aligned with the outcome (issue #382 Phase 3b).
    pub clob_token_ids: Vec<String>,
}

/// The result of a [`GammaMarketsClient::fetch_markets`] call.
pub struct GammaMarkets {
    /// `conditionId → GammaMarket` for every market Gamma returned across the successful batches.
    pub markets: HashMap<String, GammaMarket>,
    /// Ids whose batch hit a `4xx` ([`SourceError::Fatal`]) and were skipped — *not fetched*, so the
    /// caller should leave them untouched and retry next run (the pre-#382 per-ID `Fatal → skip`
    /// behaviour). This is deliberately distinct from an id merely absent from `markets` because
    /// Gamma returned `200` without it: that is a definitive unknown market (Gamma's `200 []`), which
    /// the caller treats as the empty-response case (e.g. a NULL schedule row).
    pub unfetched: Vec<String>,
}

/// Errors from [`GammaMarketsClient::fetch_markets`].
///
/// A per-chunk HTTP `4xx` (`SourceError::Fatal`) is **not** an error: those ids are reported in
/// [`GammaMarkets::unfetched`] so the caller can retry them (mirroring the pre-#382 per-ID
/// `Fatal → skip`). Only non-fatal fetch failures (transient/5xx after retries, or rate-limited) and
/// gross response corruption abort.
#[derive(Debug, thiserror::Error)]
pub enum GammaMarketsError {
    /// A non-fatal fetch failure (transient/5xx after retries, or rate-limited). Callers decide how to
    /// react: `pe-bootstrap`'s cold passes abort (the pre-#382 per-ID loops also returned `Err` on a
    /// non-`Fatal` source error), while `pe-paper-pnl`'s resolution poller logs and skips the tick.
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

    /// Fetch every id in `ids` under `filter`. Returns [`GammaMarkets`]: a `conditionId → GammaMarket`
    /// map for the markets Gamma returned, plus the [`GammaMarkets::unfetched`] ids whose batch hit a
    /// `4xx` (skipped, retry-able). An id that is *known-absent* (Gamma returned `200` without it) is
    /// simply missing from the map and is **not** in `unfetched` — the caller treats that as the
    /// empty-response case (e.g. a NULL schedule row).
    ///
    /// Input order is preserved through dedup and chunking so the batch URLs are deterministic
    /// (important for `FixtureFetcher` exact-URL keying).
    ///
    /// # Errors
    /// Returns [`GammaMarketsError::Fetch`] on a non-fatal source error (transient/rate-limited) and
    /// [`GammaMarketsError::Parse`] on a malformed batch array. A per-chunk `4xx` is not an error — it
    /// is reported via `unfetched`.
    pub async fn fetch_markets(
        &self,
        ids: &[String],
        filter: MarketFilter,
    ) -> Result<GammaMarkets, GammaMarketsError> {
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
        let mut unfetched: Vec<String> = Vec::new();
        while let Some((chunk, result)) = stream.next().await {
            let bytes = match result {
                Ok(b) => b,
                Err(SourceError::Fatal { message }) => {
                    // 4xx on the whole chunk — report its ids as unfetched (retry-able), as the per-ID
                    // path skipped a Fatal id without writing a row. Distinct from a 200 that omits an
                    // id (an unknown market), which the caller treats as the empty-response case.
                    tracing::warn!(chunk_len = chunk.len(), error = %message, "gamma_markets: batch fatal, marking chunk unfetched");
                    unfetched.extend(chunk);
                    continue;
                }
                Err(e) => return Err(GammaMarketsError::Fetch(e.to_string())),
            };

            let markets: Vec<GammaMarketRaw> = serde_json::from_slice(&bytes)
                .map_err(|e| GammaMarketsError::Parse(e.to_string()))?;
            for m in markets {
                let end_date_unix = m.end_date.as_deref().and_then(parse_end_date_unix);
                let outcome_prices = m.outcome_prices.as_deref().and_then(parse_outcome_prices);
                out.insert(
                    m.condition_id.clone(),
                    GammaMarket {
                        condition_id: m.condition_id,
                        end_date_unix,
                        liquidity: m.liquidity,
                        closed: m.closed,
                        outcome_prices,
                        volume: m.volume,
                        clob_token_ids: m.clob_token_ids,
                    },
                );
            }
        }
        Ok(GammaMarkets {
            markets: out,
            unfetched,
        })
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
///
/// `#[serde(rename_all = "camelCase")]` maps the snake_case fields to Gamma's camelCase keys
/// (`condition_id → conditionId`, `end_date → endDate`, `outcome_prices → outcomePrices`,
/// `clob_token_ids → clobTokenIds`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarketRaw {
    condition_id: String,
    /// Scheduled close date, RFC 3339. Present on open and closed markets; `None` only when omitted.
    end_date: Option<String>,
    /// Order-book depth indicator (USD). Gamma may send a JSON number or a decimal string;
    /// [`deserialize_decimal_lenient`] accepts both and yields `None` on anything unparseable, so a
    /// bad scalar never fails the row (issue #382 Phase 3b — the mid-price cache requires this
    /// leniency, and a malformed value no longer aborts the bootstrap batch the way the stricter
    /// [`deserialize_decimal_flexible`] did).
    #[serde(default, deserialize_with = "deserialize_decimal_lenient")]
    liquidity: Option<Decimal>,
    /// Whether the market is resolved. Defaults `false` when omitted (open markets / lean fixtures).
    #[serde(default)]
    closed: bool,
    /// Resolved/mid prices as a JSON-encoded decimal-string array, e.g. `"[\"1\",\"0\"]"`. Parsed in
    /// the demux via [`parse_outcome_prices`].
    outcome_prices: Option<String>,
    /// Cumulative traded volume (USD). Same encoding and leniency as `liquidity` (issue #382 Phase 3b).
    #[serde(default, deserialize_with = "deserialize_decimal_lenient")]
    volume: Option<Decimal>,
    /// `clobTokenIds`, outcome-ordered. Gamma sends a stringified JSON array (`"[\"a\",\"b\"]"`); a
    /// native array is also accepted. Decoded via [`deserialize_clob_token_ids`]; any other shape
    /// yields an empty vec (issue #382 Phase 3b).
    #[serde(default, deserialize_with = "deserialize_clob_token_ids")]
    clob_token_ids: Vec<String>,
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

/// Deserialize Gamma's `liquidity`/`volume` whether they arrive as a JSON string (`"6434.84"` — the
/// live `/markets` form) or a JSON number. Any other shape — a null, bool, array, object, missing
/// field, or unparseable string — yields `None`, so a malformed depth scalar can never fail the row
/// and drop its mids. This is the lenient counterpart of [`deserialize_decimal_flexible`] (which
/// *errors* on a malformed value); the service mid-price cache requires this leniency (issue #382
/// Phase 3b), and adopting it for `liquidity`/`volume` also stops a malformed scalar from aborting a
/// whole bootstrap batch.
fn deserialize_decimal_lenient<'de, D>(d: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use rust_decimal::prelude::FromPrimitive;
    use serde_json::Value;

    // Capture as an untyped value first: `Value` deserialization is total over any JSON shape, so an
    // unexpected type degrades to `None` instead of erroring.
    Ok(match Option::<Value>::deserialize(d)? {
        Some(Value::String(s)) => s.trim().parse::<Decimal>().ok(),
        Some(Value::Number(n)) => n.as_f64().and_then(Decimal::from_f64),
        _ => None,
    })
}

/// Decode Gamma's `clobTokenIds` into outcome-ordered token ids. Gamma sends a stringified JSON array
/// (`"[\"id0\",\"id1\"]"` — the live `/markets` form); a native JSON array is also accepted. Any other
/// shape, malformed inner JSON, a null, or a missing field yields an empty vec — token mapping is
/// best-effort and must never drop a market's mids. **Positions are preserved** (no compaction or
/// blank-dropping) so `clob_token_ids[outcome_id]` stays aligned with the outcome (issue #382 Phase 3b).
fn deserialize_clob_token_ids<'de, D>(d: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde_json::Value;

    Ok(match Option::<Value>::deserialize(d)? {
        // Stringified JSON array — the live `/markets` encoding.
        Some(Value::String(s)) => serde_json::from_str::<Vec<String>>(&s).unwrap_or_default(),
        // Native JSON array; coerce each entry to its string form, order preserved.
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|v| match v {
                Value::String(s) => s,
                other => other.to_string(),
            })
            .collect(),
        _ => Vec::new(),
    })
}

/// Parse Gamma's `outcomePrices` field — a JSON-encoded decimal-string array such as
/// `"[\"0.62\",\"0.38\"]"` (open-market mids) or `"[\"1\",\"0\"]"` (resolved) — into `Vec<Decimal>`
/// indexed by `outcome_id`.
///
/// Returns `None` on malformed JSON (logged), so callers skip the market rather than mis-valuing it.
/// Individual non-decimal entries fall back to `Decimal::ZERO` — the lenient semantic shared by the
/// resolution poller (`pe-paper-pnl`) and the service mid-price cache (issue #382 Q7), kept so the
/// decimal decoding lives in one place. Relocated here from `pe-paper-pnl::gamma` in Phase 3a.
pub fn parse_outcome_prices(prices_str: &str) -> Option<Vec<Decimal>> {
    let raw: Vec<String> = serde_json::from_str(prices_str)
        .map_err(|e| tracing::warn!(error = %e, "gamma: outcomePrices parse error"))
        .ok()?;
    Some(
        raw.iter()
            .map(|s| s.parse::<Decimal>().unwrap_or(Decimal::ZERO))
            .collect(),
    )
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

    #[test]
    fn gamma_market_raw_parses_closed_and_outcome_prices() {
        let json = r#"[{"conditionId":"0xA","closed":true,"outcomePrices":"[\"1\",\"0\"]"},
                       {"conditionId":"0xB"}]"#;
        let raws: Vec<GammaMarketRaw> = serde_json::from_slice(json.as_bytes()).unwrap();
        assert!(raws[0].closed);
        assert_eq!(raws[0].outcome_prices.as_deref(), Some(r#"["1","0"]"#));
        assert!(!raws[1].closed, "closed defaults false when omitted");
        assert_eq!(raws[1].outcome_prices, None);
    }

    #[test]
    fn parse_outcome_prices_decodes_open_mids() {
        let parsed = parse_outcome_prices(r#"["0.62","0.38"]"#).unwrap();
        assert_eq!(parsed, vec![Decimal::new(62, 2), Decimal::new(38, 2)]);
    }

    #[test]
    fn parse_outcome_prices_decodes_resolved() {
        let parsed = parse_outcome_prices(r#"["1","0"]"#).unwrap();
        assert_eq!(parsed, vec![Decimal::ONE, Decimal::ZERO]);
    }

    #[test]
    fn parse_outcome_prices_none_on_malformed_json() {
        assert!(parse_outcome_prices("not-json").is_none());
    }

    #[test]
    fn parse_outcome_prices_non_decimal_entry_falls_back_to_zero() {
        // The lenient paper-pnl semantic (issue #382 Q7): a bad entry → 0, the array still parses.
        let parsed = parse_outcome_prices(r#"["x","0.5"]"#).unwrap();
        assert_eq!(parsed, vec![Decimal::ZERO, Decimal::new(5, 1)]);
    }

    #[test]
    fn gamma_market_raw_parses_volume_and_clob_token_ids() {
        // Phase 3b mid-cache fields: volume (string or number) + clobTokenIds (stringified or native
        // array), order preserved. Omitted fields default to None / empty.
        let json = r#"[{"conditionId":"0xA","volume":"99995.018095","clobTokenIds":"[\"111\",\"222\"]"},
                       {"conditionId":"0xB","volume":6434,"clobTokenIds":["333","444"]},
                       {"conditionId":"0xC"}]"#;
        let raws: Vec<GammaMarketRaw> = serde_json::from_slice(json.as_bytes()).unwrap();
        assert_eq!(raws[0].volume, Some(Decimal::new(99_995_018_095, 6)));
        assert_eq!(
            raws[0].clob_token_ids,
            vec!["111".to_string(), "222".to_string()]
        );
        assert_eq!(raws[1].volume, Some(Decimal::from(6434)));
        assert_eq!(
            raws[1].clob_token_ids,
            vec!["333".to_string(), "444".to_string()]
        );
        assert_eq!(raws[2].volume, None);
        assert!(raws[2].clob_token_ids.is_empty());
    }

    #[test]
    fn lenient_liquidity_tolerates_malformed_without_aborting_batch() {
        // Phase 3b: the liquidity decoder is now lenient, so a malformed value yields `None` and the
        // *whole array still parses* — `fetch_markets` will not abort the batch on it (the pre-3b
        // flexible decoder errored here, which would have failed the bootstrap pass). Well-formed
        // siblings (string + number) are unaffected.
        let json = r#"[{"conditionId":"0xA","liquidity":"not-a-number"},
                       {"conditionId":"0xB","liquidity":"7.5"},
                       {"conditionId":"0xC","liquidity":12345.6},
                       {"conditionId":"0xD","liquidity":true}]"#;
        let raws: Vec<GammaMarketRaw> = serde_json::from_slice(json.as_bytes()).unwrap();
        assert_eq!(raws.len(), 4, "array parses despite a malformed liquidity");
        assert_eq!(raws[0].liquidity, None);
        assert_eq!(raws[1].liquidity, Some(Decimal::new(75, 1)));
        assert_eq!(raws[2].liquidity, Some(Decimal::new(123_456, 1)));
        assert_eq!(raws[3].liquidity, None);
    }
}
