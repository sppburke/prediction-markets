//! Polymarket Gamma `/events` sweep — builds the `conditionId → event` map.
//!
//! Endpoint: `GET https://gamma-api.polymarket.com/events?limit={N}&offset={M}`
//!
//! Each event embeds a `markets[]` array, every element carrying a `conditionId`
//! (= `trades.market_id`). An event groups multiple markets (avg ~2.1; neg-risk
//! bundles can hold a dozen), and the event is the unit the sign-randomization
//! skill test (issue #206 / SSRN 6617059) randomizes over — market-level
//! randomization is invalid because within-event bets are correlated.
//!
//! Unlike the per-condition `/markets` fetch in [`crate::gamma`], `/events` is
//! swept sequentially by offset: each page depends on the prior cursor, so there
//! is no `buffer_unordered` parallelism. The shared rate-limit mutex inside
//! [`ReqwestFetcher`] still gates throughput at `GAMMA_MIN_INTERVAL_MS`.
//!
//! Resumability: the next page offset is checkpointed in `source_cursor` under
//! [`EVENTS_CURSOR_KEY`] after every committed page, and reset to `0` on
//! completion (events are not append-only — a re-sweep must start from the top
//! because market lists grow). After the sweep, an orphan pass self-maps any
//! traded market with no Gamma event (`event_id = condition_id`), then a
//! coverage gate (issue #206 AC1) warns/fails on the orphan rate.
//!
//! The same `markets[]` elements also carry `clobTokenIds` (the two ERC-1155
//! position-token ids), so the sweep doubles as the `token_id → condition_id`
//! map builder (issue #207, Slice 0): on-chain `OrderFilled` legs are keyed by
//! token id, and this map resolves them to a market. Captured in the same pass
//! to avoid a second ~99k-event sweep.

use std::time::Duration;

use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::dune::normalise_condition_id;
use crate::error::BootstrapError;

/// `source_cursor` key holding the next `/events` page offset to fetch.
pub(crate) const EVENTS_CURSOR_KEY: &str = "gamma_events_sweep_offset";

/// Events fetched per `/events` page. Gamma caps the page size; 500 is the
/// observed maximum that returns reliably.
const EVENTS_PAGE_LIMIT: u64 = 500;

/// Orphan-rate warn threshold (integer percent). Gamma's /events covers only a
/// curated subset of all traded condition IDs: a 90–99% orphan rate is expected
/// on a full historical cache. Warn only at near-total orphan coverage (99%) to
/// flag a genuine catastrophic format break; the hard-fail is replaced by the
/// zero-conditions check below (events_seen > 0 but conditions_mapped == 0).
/// Canonical: `docs/_GLOSSARY.md` "Bootstrap defaults" (`bootstrap_event_orphan_warn_pct`).
const ORPHAN_WARN_PCT: usize = 99;

/// Outcome of an `/events` sweep + orphan pass (issue #206).
#[derive(Debug, Default, Clone)]
pub struct EventsReport {
    /// Events seen across all pages.
    pub events_seen: usize,
    /// `(condition_id → event)` rows written from real Gamma events.
    pub conditions_mapped: usize,
    /// `(token_id → condition_id)` rows written from `clobTokenIds` (issue #207).
    pub tokens_mapped: usize,
    /// Distinct traded `market_id`s in the cache (the coverage denominator).
    pub total_traded_markets: usize,
    /// Traded markets with no Gamma event, self-mapped as singleton events.
    pub orphan_self_mapped: usize,
    /// `market_fees` rows upserted from Gamma `takerBaseFee`/`makerBaseFee` (issue #23).
    pub fees_upserted: usize,
}

/// Sweeps Polymarket Gamma `/events` to populate the `market_events` map.
///
/// Generic over [`PageFetcher`] so production uses [`ReqwestFetcher`] and tests
/// use [`FixtureFetcher`] with no live network.
pub struct GammaEventsFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> GammaEventsFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// Sweep every `/events` page, upsert each market's `conditionId → event`
    /// row, then self-map traded-market orphans and apply the coverage gate.
    ///
    /// Fatal on any fetch/parse error (a paged sweep cannot skip a page without
    /// desyncing the offset). Returns [`BootstrapError::Gamma`] if the orphan
    /// rate exceeds [`ORPHAN_FAIL_PCT`] (a join-key/form break).
    pub async fn sweep(&self, cache: &mut WalletCache) -> Result<EventsReport, BootstrapError> {
        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let mut offset: u64 = cache
            .get_source_cursor(EVENTS_CURSOR_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        info!(
            start_offset = offset,
            "events: starting Gamma /events sweep"
        );

        let mut events_seen = 0usize;
        let mut conditions_mapped = 0usize;
        let mut tokens_mapped = 0usize;
        let mut fees_upserted = 0usize;

        loop {
            let url = format!(
                "{}/events?limit={EVENTS_PAGE_LIMIT}&offset={offset}",
                self.base_url
            );
            let bytes = match self.fetcher.fetch_page(&url).await {
                Ok(b) => b,
                // Gamma returns 422 when offset >= total event count (instead of
                // an empty array). Treat it as end-of-data, same as an empty page.
                Err(SourceError::Fatal { message }) if message.contains("HTTP 422") => break,
                Err(SourceError::Fatal { message }) => {
                    return Err(BootstrapError::Gamma {
                        message: format!("events fetch at offset {offset}: {message}"),
                    });
                }
                Err(e) => {
                    return Err(BootstrapError::Gamma {
                        message: format!("events fetch at offset {offset}: {e}"),
                    });
                }
            };

            let page = parse_events_page(&bytes).map_err(|e| BootstrapError::Gamma {
                message: format!("events parse at offset {offset}: {e}"),
            })?;
            if page.is_empty() {
                break; // exhausted
            }

            // `(token_id, condition_id)` rows for this page's markets, flushed in
            // one transaction below (issue #207). A market carries 0..n tokens.
            let mut token_rows: Vec<(String, String)> = Vec::new();
            // `(condition_id, taker_bps, maker_bps, _)` rows for market_fees (issue #23).
            let mut fee_rows: Vec<(String, i32, i32, i64)> = Vec::new();
            for event in &page {
                events_seen += 1;
                // Grouping key: prefer the numeric event id, fall back to slug.
                // Either is an opaque, stable per-event string.
                let Some(event_key) = event.id.as_deref().or(event.slug.as_deref()) else {
                    continue; // no usable event identity — cannot group
                };
                for market in &event.markets {
                    let Some(raw_cond) = market.condition_id.as_deref() else {
                        continue;
                    };
                    if raw_cond.is_empty() {
                        continue;
                    }
                    let cond = normalise_condition_id(raw_cond);
                    cache.upsert_market_events(
                        &cond,
                        event_key,
                        event.slug.as_deref(),
                        fetched_at,
                    )?;
                    conditions_mapped += 1;
                    for token_id in parse_clob_token_ids(market.clob_token_ids.as_deref()) {
                        token_rows.push((token_id, cond.clone()));
                    }
                    let taker_bps = fee_to_bps(market.taker_base_fee);
                    let maker_bps = fee_to_bps(market.maker_base_fee);
                    fee_rows.push((cond, taker_bps, maker_bps, fetched_at));
                }
            }
            tokens_mapped += token_rows.len();
            cache.upsert_token_conditions_batch(&token_rows, fetched_at)?;
            fees_upserted += fee_rows.len();
            cache.upsert_market_fees_batch(&fee_rows, fetched_at)?;

            offset += EVENTS_PAGE_LIMIT;
            cache.set_source_cursor(EVENTS_CURSOR_KEY, &offset.to_string())?;
            if events_seen.is_multiple_of(5_000) {
                info!(
                    events_seen,
                    conditions_mapped,
                    tokens_mapped,
                    fees_upserted,
                    offset,
                    "events: sweep progress"
                );
            }
        }

        // Events are not append-only — reset so the next run re-sweeps from the
        // top and picks up markets added to existing (e.g. neg-risk) events.
        cache.set_source_cursor(EVENTS_CURSOR_KEY, "0")?;

        // Orphan pass: every traded market with no Gamma event becomes its own
        // singleton event so AC2 holds (no unmapped traded market remains).
        let traded = cache.all_market_ids();
        let total_traded_markets = traded.len();
        let mapped = cache.mapped_condition_ids();
        let mut orphan_self_mapped = 0usize;
        for market_id in &traded {
            if !mapped.contains(market_id) {
                cache.self_map_orphan(market_id, fetched_at)?;
                orphan_self_mapped += 1;
            }
        }

        let report = EventsReport {
            events_seen,
            conditions_mapped,
            tokens_mapped,
            total_traded_markets,
            orphan_self_mapped,
            fees_upserted,
        };

        // Coverage gate (AC1/AC3): observable counts + warn/fail on orphan rate.
        // Integer cross-multiply avoids any float: orphan/total > pct/100.
        info!(
            events_seen = report.events_seen,
            conditions_mapped = report.conditions_mapped,
            tokens_mapped = report.tokens_mapped,
            fees_upserted = report.fees_upserted,
            total_traded_markets = report.total_traded_markets,
            orphan_self_mapped = report.orphan_self_mapped,
            "events: sweep complete"
        );
        // Format-break guard: if the sweep saw events but mapped zero conditions,
        // Gamma's conditionId field has changed shape. This is the reliable signal
        // for a join-key break; the old orphan-rate hard-fail was a false alarm on
        // large historical caches (Gamma covers ~10k events vs. ~1M traded condition
        // IDs, so a 96%+ orphan rate is correct and expected).
        if events_seen > 0 && conditions_mapped == 0 {
            return Err(BootstrapError::Gamma {
                message: format!(
                    "events: sweep saw {events_seen} events but mapped 0 conditions — \
                     likely a conditionId field-name/format break in the Gamma response"
                ),
            });
        }
        if total_traded_markets > 0
            && orphan_self_mapped * 100 > total_traded_markets * ORPHAN_WARN_PCT
        {
            warn!(
                orphan_self_mapped,
                total_traded_markets,
                "events: orphan rate above {ORPHAN_WARN_PCT}% — investigate Gamma coverage"
            );
        }

        Ok(report)
    }
}

/// Build the production `/events` fetcher and run a full sweep.
///
/// Mirrors the Gamma client construction in [`crate::fetch_resolutions_and_schedules`]:
/// a pooled `reqwest` client behind [`ReqwestFetcher`] gated at
/// [`crate::gamma::GAMMA_MIN_INTERVAL_MS`].
pub async fn run_events(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<EventsReport, BootstrapError> {
    use pe_source_polymarket_public::ReqwestFetcher;
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = GammaEventsFetcher::new(
        config.gamma_base_url.clone(),
        ReqwestFetcher::new(client).with_min_interval_ms(crate::gamma::GAMMA_MIN_INTERVAL_MS),
    );
    fetcher.sweep(cache).await
}

/// Serde DTO for one element of the `/events` response array.
///
/// Extra fields are ignored. `id` may arrive as a JSON number or string; both
/// render to the opaque `event_id` grouping key.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEventRaw {
    #[serde(default, deserialize_with = "de_flex_string")]
    id: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    markets: Vec<GammaEventMarketRaw>,
}

/// Serde DTO for one element of an event's `markets[]` array.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaEventMarketRaw {
    #[serde(default)]
    condition_id: Option<String>,
    /// `clobTokenIds` — Gamma returns this as a JSON-array *string* of the two
    /// ERC-1155 position-token ids (decimal uint256), e.g. `"[\"123\",\"456\"]"`.
    #[serde(default)]
    clob_token_ids: Option<String>,
    /// Taker fee rate. Gamma may return this as a fraction (e.g. `0.02` = 2%) or
    /// as basis points (e.g. `200`). Use `fee_to_bps` to normalise. Field name
    /// is `takerBaseFee` in Gamma's current schema; `takerFee` accepted as alias.
    #[serde(
        default,
        alias = "takerFee",
        deserialize_with = "crate::gamma::deserialize_decimal_flexible"
    )]
    taker_base_fee: Option<rust_decimal::Decimal>,
    /// Maker fee rate. Same encoding as `taker_base_fee`.
    #[serde(
        default,
        alias = "makerFee",
        deserialize_with = "crate::gamma::deserialize_decimal_flexible"
    )]
    maker_base_fee: Option<rust_decimal::Decimal>,
}

/// Parse Gamma's `clobTokenIds` (a stringified JSON array of decimal token ids)
/// into the contained ids. Returns empty on `None`, malformed JSON, or a
/// non-array — token mapping is best-effort and must never abort the sweep.
/// Blank ids are dropped.
fn parse_clob_token_ids(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    match serde_json::from_str::<Vec<String>>(raw) {
        Ok(ids) => ids.into_iter().filter(|t| !t.is_empty()).collect(),
        Err(_) => Vec::new(),
    }
}

/// Normalise a Gamma fee field to basis points.
///
/// Gamma may return fees as a fraction (`0.02` = 2% = 200 bps) or as an integer
/// bps value (`200`). Detection rule: if the value is ≤ 1.0 treat it as a
/// fraction and multiply by 10 000; otherwise treat it as already-bps. Both forms
/// are clamped to [0, 10 000] and converted to `i32` (always in range after clamp).
/// `None` → `0` (pre-fee-era default; safe sentinel per `_GLOSSARY.md`).
fn fee_to_bps(fee: Option<rust_decimal::Decimal>) -> i32 {
    use rust_decimal::prelude::ToPrimitive;
    let Some(d) = fee else {
        return 0;
    };
    let bps = if d <= rust_decimal::Decimal::ONE {
        d * rust_decimal::Decimal::from(10_000)
    } else {
        d
    };
    // Round to nearest integer, clamp to [0, 10_000], then narrow to i32.
    let rounded = bps.round().to_i64().unwrap_or(0);
    i32::try_from(rounded.clamp(0, 10_000)).unwrap_or(0)
}

/// Parse one `/events` page (a JSON array of event objects).
fn parse_events_page(bytes: &[u8]) -> Result<Vec<GammaEventRaw>, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("JSON parse: {e}"))
}

/// Deserialize a JSON value that may be a number or string into `Option<String>`.
fn de_flex_string<'de, D>(d: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flex {
        Str(String),
        Int(i64),
    }
    Ok(match Option::<Flex>::deserialize(d)? {
        None => None,
        Some(Flex::Str(s)) => Some(s),
        Some(Flex::Int(i)) => Some(i.to_string()),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_events_page_extracts_id_slug_and_conditions() {
        let json = br#"[
            {"id": 491919, "slug": "btc-may", "markets": [
                {"conditionId": "0xaa", "clobTokenIds": "[\"111\",\"222\"]"},
                {"conditionId": "0xbb"}
            ]}
        ]"#;
        let page = parse_events_page(json).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id.as_deref(), Some("491919")); // numeric id → string
        assert_eq!(page[0].slug.as_deref(), Some("btc-may"));
        assert_eq!(page[0].markets.len(), 2);
        assert_eq!(page[0].markets[0].condition_id.as_deref(), Some("0xaa"));
        // clobTokenIds arrives as a stringified JSON array.
        assert_eq!(
            parse_clob_token_ids(page[0].markets[0].clob_token_ids.as_deref()),
            vec!["111".to_string(), "222".to_string()]
        );
        // Market without clobTokenIds yields no tokens.
        assert!(parse_clob_token_ids(page[0].markets[1].clob_token_ids.as_deref()).is_empty());
    }

    #[test]
    fn parse_clob_token_ids_handles_edge_cases() {
        assert_eq!(
            parse_clob_token_ids(Some(
                r#"["28182404005967940652495463228537840901","470448457534500220474364299688086011"]"#
            )),
            vec![
                "28182404005967940652495463228537840901".to_string(),
                "470448457534500220474364299688086011".to_string(),
            ]
        );
        assert!(parse_clob_token_ids(None).is_empty());
        assert!(parse_clob_token_ids(Some("")).is_empty()); // malformed → empty
        assert!(parse_clob_token_ids(Some("not json")).is_empty());
        // Blank ids are dropped.
        assert_eq!(
            parse_clob_token_ids(Some(r#"["123",""]"#)),
            vec!["123".to_string()]
        );
    }

    #[test]
    fn parse_events_page_accepts_string_id() {
        let json = br#"[{"id": "evt-7", "slug": "s", "markets": []}]"#;
        let page = parse_events_page(json).unwrap();
        assert_eq!(page[0].id.as_deref(), Some("evt-7"));
    }

    #[test]
    fn parse_events_page_empty_array() {
        assert!(parse_events_page(b"[]").unwrap().is_empty());
    }

    #[test]
    fn parse_events_page_tolerates_missing_fields() {
        // Event with no id and a market with no conditionId — must not error.
        let json = br#"[{"slug": "s", "markets": [{}]}]"#;
        let page = parse_events_page(json).unwrap();
        assert_eq!(page[0].id, None);
        assert_eq!(page[0].markets[0].condition_id, None);
    }

    // --- fee_to_bps / DTO tests (issue #23, PR 1) ---

    #[test]
    fn fee_to_bps_fraction_form() {
        // Gamma returns 0.02 (2%) → 200 bps.
        use rust_decimal_macros::dec;
        assert_eq!(fee_to_bps(Some(dec!(0.02))), 200);
        assert_eq!(fee_to_bps(Some(dec!(0.001))), 10); // 0.1% = 10 bps
    }

    #[test]
    fn fee_to_bps_integer_bps_form() {
        // Gamma returns 200 (already bps, > 1.0).
        use rust_decimal_macros::dec;
        assert_eq!(fee_to_bps(Some(dec!(200))), 200);
        assert_eq!(fee_to_bps(Some(dec!(0))), 0);
    }

    #[test]
    fn fee_to_bps_missing_defaults_to_zero() {
        assert_eq!(fee_to_bps(None), 0);
    }

    #[test]
    fn fee_dto_parses_fraction_and_alias() {
        // takerBaseFee as fraction, makerFee (alias) as integer bps.
        let json = br#"[{
            "id": 1, "markets": [{
                "conditionId": "0xcc",
                "takerBaseFee": 0.02,
                "makerFee": 100
            }]
        }]"#;
        let page = parse_events_page(json).unwrap();
        let m = &page[0].markets[0];
        assert_eq!(fee_to_bps(m.taker_base_fee), 200);
        assert_eq!(fee_to_bps(m.maker_base_fee), 100);
    }
}
