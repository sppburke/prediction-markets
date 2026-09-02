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

use std::collections::{BTreeMap, HashMap, HashSet};

use futures::stream::{self, StreamExt};
use pe_core_types::{OutcomeId, PolymarketConditionId, PolymarketTokenId, ReceivedAt, SourceId};
use pe_source_core::SourceError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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
/// Stable source identity for recorded Gamma `/markets` metadata pages.
pub const GAMMA_MARKETS_SOURCE_ID: &str = "polymarket.gamma.markets";
/// Wire schema version for recorded Gamma `/markets` metadata pages.
pub const GAMMA_MARKETS_SCHEMA_VERSION: u32 = 1;
/// Parser version for recorded Gamma `/markets` metadata pages.
pub const GAMMA_MARKETS_PARSER_VERSION: u32 = 1;

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
    /// Market creation time as unix seconds, parsed from `createdAt` (issue #421 PR4 — the
    /// `entry_timing_vs_creation` CLV-bake-off feature). `None` when Gamma omits or returns an
    /// unparseable `createdAt` (nullable upstream), so a missing creation time degrades to "unknown"
    /// rather than failing the row.
    pub created_at_unix: Option<i64>,
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

/// Call-scoped evidence for one successful Gamma `/markets` page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPageEvidence {
    pub request_url: String,
    pub raw_page_hash: String,
    pub canonical_page_hash: String,
    pub received_at: ReceivedAt,
    pub source_id: SourceId,
    pub schema_version: u32,
    pub parser_version: u32,
}

/// A token-targeted Gamma result and the exact page that produced it.
pub struct GammaMarketsWithPages {
    pub markets: GammaMarkets,
    pub page: Option<(MetadataPageEvidence, Vec<u8>)>,
}

/// One token identity proven by a recorded Gamma market page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTokenIdentity {
    pub condition_id: PolymarketConditionId,
    pub outcome: OutcomeId,
    pub evidence_hash: String,
}

/// Strict token-identity validation failures in Gamma market metadata.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataIdentityError {
    #[error("token {token} is listed by {markets} markets")]
    MarketCardinality { token: String, markets: usize },
    #[error("market {condition_id} has empty or duplicate token ids")]
    EmptyOrDuplicateTokens { condition_id: String },
    #[error("market {condition_id} outcome {outcome} identifies multiple tokens")]
    DuplicateIdentity { condition_id: String, outcome: u16 },
    #[error("market {condition_id} token index {index} exceeds u16")]
    IndexOverflow { condition_id: String, index: usize },
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
    /// A token id contains a comma and would change the repeat-key request semantics.
    #[error("gamma token id must use one repeat-key value, got {token:?}")]
    InvalidTokenId { token: String },
    /// The token lookup method owns exactly one request. Its caller owns chunking.
    #[error("gamma token lookup has {tokens} ids, above the per-request limit {limit}")]
    TooManyTokenIds { tokens: usize, limit: usize },
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
                let end_date_unix = m.end_date.as_deref().and_then(parse_rfc3339_unix);
                let created_at_unix = m.created_at.as_deref().and_then(parse_rfc3339_unix);
                let outcome_prices = m.outcome_prices.as_deref().and_then(parse_outcome_prices);
                out.insert(
                    m.condition_id.clone(),
                    GammaMarket {
                        condition_id: m.condition_id,
                        end_date_unix,
                        created_at_unix,
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

    /// Fetch one page of markets by repeat-key `clob_token_ids`, retaining the raw page and
    /// call-scoped evidence for the successful request. Input order is preserved through
    /// deduplication. The caller owns chunking across requests.
    ///
    /// # Errors
    /// Returns [`GammaMarketsError::InvalidTokenId`] when one input contains a comma, because comma
    /// joining is not a valid Gamma token lookup. Fetch and parse failures have the same meanings as
    /// [`Self::fetch_markets`].
    pub async fn fetch_markets_by_token_ids(
        &self,
        token_ids: &[String],
        filter: MarketFilter,
    ) -> Result<GammaMarketsWithPages, GammaMarketsError> {
        if let Some(token) = token_ids.iter().find(|token| token.contains(',')) {
            return Err(GammaMarketsError::InvalidTokenId {
                token: token.clone(),
            });
        }

        let mut seen: HashSet<&str> = HashSet::with_capacity(token_ids.len());
        let unique = token_ids
            .iter()
            .map(String::as_str)
            .filter(|token| seen.insert(token))
            .collect::<Vec<_>>();
        if unique.len() > self.batch_size {
            return Err(GammaMarketsError::TooManyTokenIds {
                tokens: unique.len(),
                limit: self.batch_size,
            });
        }
        if unique.is_empty() {
            return Ok(GammaMarketsWithPages {
                markets: GammaMarkets {
                    markets: HashMap::new(),
                    unfetched: Vec::new(),
                },
                page: None,
            });
        }

        let chunk = unique
            .iter()
            .map(|token| (*token).to_owned())
            .collect::<Vec<_>>();
        let url = build_token_batch_url(&self.base_url, &chunk, filter.closed_param(), self.limit);
        let result = self.fetcher.fetch_page(&url).await;
        let received_at = ReceivedAt::now_utc();
        let raw = match result {
            Ok(raw) => raw,
            Err(SourceError::Fatal { message }) => {
                tracing::warn!(chunk_len = chunk.len(), error = %message, "gamma_markets: token batch fatal, marking chunk unfetched");
                return Ok(GammaMarketsWithPages {
                    markets: GammaMarkets {
                        markets: HashMap::new(),
                        unfetched: chunk,
                    },
                    page: None,
                });
            }
            Err(error) => return Err(GammaMarketsError::Fetch(error.to_string())),
        };
        let decoded: Vec<GammaMarketRaw> = serde_json::from_slice(&raw)
            .map_err(|error| GammaMarketsError::Parse(error.to_string()))?;
        let evidence = metadata_page_evidence(&url, &raw, received_at)?;
        let markets = decoded
            .into_iter()
            .map(gamma_market)
            .map(|market| (market.condition_id.clone(), market))
            .collect();
        Ok(GammaMarketsWithPages {
            markets: GammaMarkets {
                markets,
                unfetched: Vec::new(),
            },
            page: Some((evidence, raw)),
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

/// Build a repeat-key token lookup URL. Unlike the older condition lookup, the validated token
/// endpoint form places `limit` before the optional `closed` filter.
fn build_token_batch_url(base: &str, token_ids: &[String], closed: &str, limit: u32) -> String {
    let mut url = String::with_capacity(base.len() + token_ids.len() * 80 + closed.len() + 16);
    url.push_str(base);
    url.push_str("/markets?");
    for (index, token) in token_ids.iter().enumerate() {
        if index > 0 {
            url.push('&');
        }
        url.push_str("clob_token_ids=");
        url.push_str(token);
    }
    url.push_str("&limit=");
    url.push_str(&limit.to_string());
    url.push_str(closed);
    url
}

fn metadata_page_evidence(
    request_url: &str,
    raw: &[u8],
    received_at: ReceivedAt,
) -> Result<MetadataPageEvidence, GammaMarketsError> {
    let value: serde_json::Value =
        serde_json::from_slice(raw).map_err(|error| GammaMarketsError::Parse(error.to_string()))?;
    let canonical =
        serde_json::to_vec(&value).map_err(|error| GammaMarketsError::Parse(error.to_string()))?;
    Ok(MetadataPageEvidence {
        request_url: request_url.to_owned(),
        raw_page_hash: blake3::hash(raw).to_hex().to_string(),
        canonical_page_hash: blake3::hash(&canonical).to_hex().to_string(),
        received_at,
        source_id: SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()),
        schema_version: GAMMA_MARKETS_SCHEMA_VERSION,
        parser_version: GAMMA_MARKETS_PARSER_VERSION,
    })
}

/// Verify the venue-defined condition/outcome identity for every requested token that appeared in
/// the fetched, recorded pages. Tokens absent from every page remain absent from the result.
#[must_use]
pub fn verify_token_identities(
    requested: &[PolymarketTokenId],
    pages: &[(MetadataPageEvidence, Vec<u8>)],
) -> BTreeMap<PolymarketTokenId, Result<VerifiedTokenIdentity, MetadataIdentityError>> {
    let mut page_markets = Vec::new();
    let mut seen_markets = HashSet::new();
    for (evidence, raw) in pages {
        let Ok(decoded) = serde_json::from_slice::<Vec<GammaMarketRaw>>(raw) else {
            continue;
        };
        for market in decoded {
            let market = gamma_market(market);
            let key = (market.condition_id.clone(), market.clob_token_ids.clone());
            if seen_markets.insert(key) {
                page_markets.push((market, evidence.canonical_page_hash.clone()));
            }
        }
    }

    let mut verified = BTreeMap::new();
    for token in requested {
        if verified.contains_key(token) {
            continue;
        }
        let matching = page_markets
            .iter()
            .filter(|(market, _)| market.clob_token_ids.iter().any(|id| id == &token.0))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            continue;
        }
        if matching.len() != 1 {
            verified.insert(
                token.clone(),
                Err(MetadataIdentityError::MarketCardinality {
                    token: token.0.clone(),
                    markets: matching.len(),
                }),
            );
            continue;
        }

        let (market, evidence_hash) = matching[0];
        let unique_tokens = market.clob_token_ids.iter().collect::<HashSet<_>>();
        if market.clob_token_ids.is_empty()
            || market
                .clob_token_ids
                .iter()
                .any(|token| token.trim().is_empty())
            || unique_tokens.len() != market.clob_token_ids.len()
        {
            verified.insert(
                token.clone(),
                Err(MetadataIdentityError::EmptyOrDuplicateTokens {
                    condition_id: market.condition_id.clone(),
                }),
            );
            continue;
        }
        let Some(index) = market.clob_token_ids.iter().position(|id| id == &token.0) else {
            continue;
        };
        let Ok(outcome) = u16::try_from(index) else {
            verified.insert(
                token.clone(),
                Err(MetadataIdentityError::IndexOverflow {
                    condition_id: market.condition_id.clone(),
                    index,
                }),
            );
            continue;
        };
        verified.insert(
            token.clone(),
            Ok(VerifiedTokenIdentity {
                condition_id: PolymarketConditionId(market.condition_id.clone()),
                outcome: OutcomeId(outcome),
                evidence_hash: evidence_hash.clone(),
            }),
        );
    }

    let mut tokens_by_identity = HashMap::<(String, u16), HashSet<String>>::new();
    for (market, _) in &page_markets {
        for (index, token) in market.clob_token_ids.iter().enumerate() {
            if let Ok(outcome) = u16::try_from(index) {
                tokens_by_identity
                    .entry((market.condition_id.clone(), outcome))
                    .or_default()
                    .insert(token.clone());
            }
        }
    }
    let duplicate_identities = tokens_by_identity
        .into_iter()
        .filter_map(|(identity, tokens)| (tokens.len() > 1).then_some(identity))
        .collect::<HashSet<_>>();
    for identity in verified.values_mut() {
        let duplicate = match identity {
            Ok(identity) => duplicate_identities
                .contains(&(identity.condition_id.0.clone(), identity.outcome.0))
                .then(|| (identity.condition_id.0.clone(), identity.outcome.0)),
            Err(_) => None,
        };
        if let Some((condition_id, outcome)) = duplicate {
            *identity = Err(MetadataIdentityError::DuplicateIdentity {
                condition_id,
                outcome,
            });
        }
    }
    verified
}

/// Parse a Gamma RFC 3339 timestamp (e.g. `"2024-11-04T00:00:00Z"`) to unix seconds. `None` on any
/// parse failure. Shared by `endDate` (scheduled close) and `createdAt` (market creation, issue #421
/// PR4); the caller treats an unparseable value the same as a missing one.
fn parse_rfc3339_unix(s: &str) -> Option<i64> {
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
    /// Market creation date, RFC 3339. Nullable upstream (issue #421 PR4); `None` when omitted or
    /// unparseable. Parsed in the demux via [`parse_rfc3339_unix`].
    created_at: Option<String>,
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

fn gamma_market(market: GammaMarketRaw) -> GammaMarket {
    let end_date_unix = market.end_date.as_deref().and_then(parse_rfc3339_unix);
    let created_at_unix = market.created_at.as_deref().and_then(parse_rfc3339_unix);
    let outcome_prices = market
        .outcome_prices
        .as_deref()
        .and_then(parse_outcome_prices);
    GammaMarket {
        condition_id: market.condition_id,
        end_date_unix,
        created_at_unix,
        liquidity: market.liquidity,
        closed: market.closed,
        outcome_prices,
        volume: market.volume,
        clob_token_ids: market.clob_token_ids,
    }
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
    use crate::fetcher::FixtureFetcher;
    use crate::reconciliation::ReconciliationPageFetcher;
    use std::sync::Arc;
    use time::OffsetDateTime;

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_owned()).collect()
    }

    fn token(id: &str) -> PolymarketTokenId {
        PolymarketTokenId(id.to_owned())
    }

    fn fetched_page(raw: Vec<u8>) -> Vec<(MetadataPageEvidence, Vec<u8>)> {
        let received_at = ReceivedAt(OffsetDateTime::from_unix_timestamp(100).unwrap());
        let evidence = metadata_page_evidence(
            "https://g/markets?clob_token_ids=T&limit=500",
            &raw,
            received_at,
        )
        .unwrap();
        vec![(evidence, raw)]
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
    fn build_token_batch_url_uses_repeat_keys_and_validated_parameter_order() {
        assert_eq!(
            build_token_batch_url("https://g", &ids(&["A", "B"]), "", 500),
            "https://g/markets?clob_token_ids=A&clob_token_ids=B&limit=500"
        );
        assert_eq!(
            build_token_batch_url("https://g", &ids(&["A"]), "&closed=true", 500),
            "https://g/markets?clob_token_ids=A&limit=500&closed=true"
        );
    }

    #[tokio::test]
    async fn reconciliation_page_fetcher_delegates_to_reconciliation_fetch() {
        let url = "https://g/page";
        let expected = b"page".to_vec();
        let fetcher = ReconciliationPageFetcher(Arc::new(FixtureFetcher::new(HashMap::from([(
            url.to_owned(),
            expected.clone(),
        )]))));
        assert_eq!(fetcher.fetch_page(url).await.unwrap(), expected);
    }

    #[test]
    fn verify_token_identities_accepts_one_strict_market_and_omits_absent_token() {
        let fetched =
            fetched_page(br#"[{"conditionId":"0xcondition","clobTokenIds":["A","B"]}]"#.to_vec());
        let result = verify_token_identities(&[token("B"), token("missing")], &fetched);
        assert_eq!(
            result.get(&token("B")),
            Some(&Ok(VerifiedTokenIdentity {
                condition_id: PolymarketConditionId("0xcondition".to_owned()),
                outcome: OutcomeId(1),
                evidence_hash: fetched[0].0.canonical_page_hash.clone(),
            }))
        );
        assert!(!result.contains_key(&token("missing")));
    }

    #[test]
    fn verify_token_identities_rejects_market_cardinality() {
        let fetched = fetched_page(
            br#"[{"conditionId":"one","clobTokenIds":["T"]},{"conditionId":"two","clobTokenIds":["T"]}]"#
                .to_vec(),
        );
        assert_eq!(
            verify_token_identities(&[token("T")], &fetched)[&token("T")],
            Err(MetadataIdentityError::MarketCardinality {
                token: "T".to_owned(),
                markets: 2,
            })
        );
    }

    #[test]
    fn verify_token_identities_coalesces_identical_markets_across_pages() {
        let raw = br#"[{"conditionId":"condition","clobTokenIds":["A","B"]}]"#.to_vec();
        let mut pages = fetched_page(raw.clone());
        let first_hash = pages[0].0.canonical_page_hash.clone();
        let received_at = ReceivedAt(OffsetDateTime::from_unix_timestamp(101).unwrap());
        pages.push((
            metadata_page_evidence(
                "https://g/markets?clob_token_ids=B&limit=500",
                &raw,
                received_at,
            )
            .unwrap(),
            raw,
        ));

        let result = verify_token_identities(&[token("A"), token("B")], &pages);
        for token in [token("A"), token("B")] {
            let identity = result[&token].as_ref().unwrap();
            assert_eq!(identity.condition_id.0, "condition");
            assert_eq!(identity.evidence_hash, first_hash);
        }
    }

    #[test]
    fn verify_token_identities_rejects_duplicate_market_tokens() {
        let fetched =
            fetched_page(br#"[{"conditionId":"condition","clobTokenIds":["T","T"]}]"#.to_vec());
        assert_eq!(
            verify_token_identities(&[token("T")], &fetched)[&token("T")],
            Err(MetadataIdentityError::EmptyOrDuplicateTokens {
                condition_id: "condition".to_owned(),
            })
        );
    }

    #[test]
    fn verify_token_identities_rejects_blank_market_tokens() {
        let fetched =
            fetched_page(br#"[{"conditionId":"condition","clobTokenIds":["T",""]}]"#.to_vec());
        assert_eq!(
            verify_token_identities(&[token("T")], &fetched)[&token("T")],
            Err(MetadataIdentityError::EmptyOrDuplicateTokens {
                condition_id: "condition".to_owned(),
            })
        );
    }

    #[test]
    fn verify_token_identities_rejects_duplicate_condition_outcome_identity() {
        let fetched = fetched_page(
            br#"[{"conditionId":"condition","clobTokenIds":["A"]},{"conditionId":"condition","clobTokenIds":["B"]}]"#
                .to_vec(),
        );
        let result = verify_token_identities(&[token("A"), token("B")], &fetched);
        let expected = Err(MetadataIdentityError::DuplicateIdentity {
            condition_id: "condition".to_owned(),
            outcome: 0,
        });
        assert_eq!(result[&token("A")], expected);
        assert_eq!(result[&token("B")], expected);
    }

    #[test]
    fn verify_token_identities_rejects_outcome_index_overflow() {
        let tokens = (0..=usize::from(u16::MAX) + 1)
            .map(|index| {
                if index == usize::from(u16::MAX) + 1 {
                    "overflow".to_owned()
                } else {
                    format!("token-{index}")
                }
            })
            .collect::<Vec<_>>();
        let raw = serde_json::to_vec(&serde_json::json!([{
            "conditionId": "condition",
            "clobTokenIds": tokens,
        }]))
        .unwrap();
        let fetched = fetched_page(raw);
        assert_eq!(
            verify_token_identities(&[token("overflow")], &fetched)[&token("overflow")],
            Err(MetadataIdentityError::IndexOverflow {
                condition_id: "condition".to_owned(),
                index: usize::from(u16::MAX) + 1,
            })
        );
    }

    #[test]
    fn parse_rfc3339_unix_basic() {
        // 2020-11-04T00:00:00Z = 1604448000
        assert_eq!(
            parse_rfc3339_unix("2020-11-04T00:00:00Z"),
            Some(1_604_448_000)
        );
        assert_eq!(parse_rfc3339_unix("not-a-date"), None);
    }

    #[test]
    fn gamma_market_raw_parses_created_at() {
        // issue #421 PR4: `createdAt` feeds the entry_timing_vs_creation feature. Nullable upstream,
        // so an omitted field → None; a present RFC 3339 value parses to unix seconds in the demux.
        let json = r#"[{"conditionId":"0xA","createdAt":"2024-01-15T00:00:00Z"},
                       {"conditionId":"0xB"}]"#;
        let raws: Vec<GammaMarketRaw> = serde_json::from_slice(json.as_bytes()).unwrap();
        assert_eq!(raws[0].created_at.as_deref(), Some("2024-01-15T00:00:00Z"));
        assert_eq!(
            raws[0].created_at.as_deref().and_then(parse_rfc3339_unix),
            Some(1_705_276_800)
        );
        assert_eq!(raws[1].created_at, None, "createdAt absent → None");
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
