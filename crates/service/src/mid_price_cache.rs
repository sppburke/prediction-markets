//! Lazy 60 s-TTL cache of Polymarket Gamma mid prices for **open** markets.
//!
//! Sibling to [`MarketEndCache`](crate::market_end_cache), but for *mutable* mids:
//! the end-date cache is write-once because a resolution time is immutable, whereas
//! mids move, so entries here expire after [`TTL`] and the dashboard mark-to-market
//! tracks the live mid. Fetches delegate to the shared batched
//! [`GammaMarketsClient`](pe_source_polymarket_public::GammaMarketsClient) over a rate-limited
//! [`ReqwestFetcher`](pe_source_polymarket_public::ReqwestFetcher) (per-instance ≤ 20 req/s,
//! matching the other Gamma fetchers in this binary); a market whose fetch fails or lacks
//! `outcomePrices` is simply omitted, so the caller marks that position's unrealized P&L as null.
//!
//! Open vs settled classification is the caller's job (via the resolution store) —
//! this cache only fetches mids for the open markets it is handed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::{MarketId, MarketOutcomeId, Price, ReceivedAt, SourceTimestamp};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn};
#[cfg(test)]
use pe_source_polymarket_public::GAMMA_BATCH_LIMIT_PARAM;
use pe_source_polymarket_public::{
    GAMMA_MARKETS_SOURCE_ID, GammaMarketsClient, GammaOpenConditionRequest, MarketFilter,
    MetadataPageEvidence, PageFetcher, ReqwestFetcher,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::warn;

use crate::activity_ingest::SourceLogHandle;
use crate::risk_inputs::RiskInputsUnavailable;

/// How long a cached mid stays fresh before a refetch.
const TTL: Duration = Duration::from_secs(60);
/// Min spacing between requests on this fetcher: 50 ms ⇒ ≤ 20 req/s. See `_GLOSSARY`.
/// (Batch concurrency is the client's `GAMMA_CONCURRENCY`; this gate still bounds actual throughput.)
const GAMMA_MIN_INTERVAL_MS: u64 = 50;
/// Schema two binds the Gamma response or failure to the exact request that the strict price
/// acquisition consulted. Other Gamma source producers retain the legacy raw-page schema one.
pub(crate) const GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION: u32 = 2;
pub(crate) const GAMMA_PRICE_ATTEMPT_PARSER_VERSION: u32 = 1;

/// Durable request/result observation for one page consulted by a strict price acquisition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "result", deny_unknown_fields)]
pub(crate) enum GammaPriceAttemptRecord {
    Page {
        evidence: MetadataPageEvidence,
        payload: Vec<u8>,
        /// False when the enclosing batched fetch failed and therefore published none of its
        /// otherwise valid response rows into the cache.
        usable: bool,
    },
    Failure {
        request_url: String,
        error: String,
    },
}

/// Per-market Gamma liquidity metadata captured alongside the mids, for the
/// fill-time market-snapshot rows of WS2 (issue #350). It rides on the same
/// `/markets` fetch as the mids (these fields are free), is cached with them, and
/// is **not yet consumed** by the orchestrator — wired in a later PR — so it adds
/// zero fill-path risk.
#[derive(Debug, Clone, Default)]
pub struct MidMarketSnapshot {
    /// Gamma `liquidity` (USD order-book depth indicator). `None` when the field
    /// is absent or unparseable.
    pub liquidity: Option<Decimal>,
    /// Gamma `volume` (USD cumulative). `None` when absent or unparseable.
    pub volume: Option<Decimal>,
    /// Gamma `clobTokenIds`, ordered by outcome so `clob_token_ids[outcome_id]`
    /// is the filled outcome's CLOB token. Empty when absent or malformed.
    pub clob_token_ids: Vec<String>,
}

/// A cached market row: the mids (per `outcome_id`), the liquidity
/// [`MidMarketSnapshot`], and the instant it was fetched (for TTL expiry).
#[derive(Clone)]
struct CachedEntry {
    mids: Vec<Decimal>,
    strict_mids: Option<Vec<Price>>,
    snapshot: MidMarketSnapshot,
    observed_at: OffsetDateTime,
    receipt: Option<AppendReceipt>,
    conflicting: bool,
}

#[derive(Default)]
struct EnsuredEntries {
    entries: HashMap<MarketId, CachedEntry>,
    missing_receipts: Vec<AppendReceipt>,
}

#[derive(Default)]
struct RecordedGammaPages {
    page_receipts: HashMap<(String, String), AppendReceipt>,
    failure_receipts: Vec<(String, AppendReceipt)>,
}

/// Strict current-price evidence used by financial risk inputs (#545).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MidPriceObservation {
    pub price: Price,
    pub receipt: AppendReceipt,
    pub observed_unix: i64,
}

/// Receipt-bound market row consumed by the shared strict runtime/replay classifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StrictPriceInput {
    pub strict_mids: Option<Vec<Price>>,
    pub observed_at_unix_nanos: i128,
    pub receipt: AppendReceipt,
    pub conflicting: bool,
}

/// Classify one consulted page timestamp against the strict evaluation clock.
pub(crate) fn classify_strict_price_time(
    observed_at_unix_nanos: i128,
    evaluated_at_unix_nanos: i128,
) -> Result<(), RiskInputsUnavailable> {
    if observed_at_unix_nanos > evaluated_at_unix_nanos {
        return Err(RiskInputsUnavailable::PriceFuture);
    }
    let age_nanos = evaluated_at_unix_nanos
        .checked_sub(observed_at_unix_nanos)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    if age_nanos >= i128::try_from(TTL.as_nanos()).map_err(|_| RiskInputsUnavailable::Overflow)? {
        return Err(RiskInputsUnavailable::PriceStale);
    }
    Ok(())
}

/// Apply the strict cache classifier in requested-position order. Qualification reconstructs these
/// inputs from only the recorded acquisition receipts and calls this same function.
pub(crate) fn classify_strict_prices(
    ids: &[MarketOutcomeId],
    evaluated_at_unix_nanos: i128,
    entries: &HashMap<MarketId, StrictPriceInput>,
) -> Result<BTreeMap<(String, u16), MidPriceObservation>, RiskInputsUnavailable> {
    let mut output = BTreeMap::new();
    for id in ids {
        let entry = entries
            .get(id.market())
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        if entry.conflicting {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        classify_strict_price_time(entry.observed_at_unix_nanos, evaluated_at_unix_nanos)?;
        let outcome = usize::from(id.outcome().0);
        let price = entry
            .strict_mids
            .as_ref()
            .and_then(|prices| prices.get(outcome))
            .copied()
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        output.insert(
            (id.market().to_string(), id.outcome().0),
            MidPriceObservation {
                price,
                receipt: entry.receipt,
                observed_unix: i64::try_from(
                    entry.observed_at_unix_nanos.div_euclid(1_000_000_000),
                )
                .map_err(|_| RiskInputsUnavailable::Overflow)?,
            },
        );
    }
    Ok(output)
}

/// One strict price acquisition, including the evidence available when validation fails.
#[derive(Debug)]
pub(crate) struct StrictMidPriceAttempt {
    pub evaluated_at: OffsetDateTime,
    pub price_receipts: Vec<AppendReceipt>,
    pub result: Result<BTreeMap<(String, u16), MidPriceObservation>, RiskInputsUnavailable>,
}

/// Thread-safe TTL cache of open-market mid prices. Generic over the fetcher so
/// tests can inject a `FixtureFetcher`; production uses [`ReqwestFetcher`].
pub struct MidPriceCache<F: PageFetcher = ReqwestFetcher> {
    inner: Arc<Mutex<HashMap<MarketId, CachedEntry>>>,
    /// Shared batched Gamma `/markets` client. Held behind an `Arc` so the cache stays `Clone`
    /// even though the underlying [`ReqwestFetcher`] (rate-limit `Mutex`) is not `Clone`.
    client: Arc<GammaMarketsClient<F>>,
    source_log: Option<SourceLogHandle>,
    /// Stamps every appended page and the strict evaluation instant: one origin for observation
    /// time and freshness (wall clock in production; the injected clock in scenarios).
    clock: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

// Manual Clone: the `Arc`s clone regardless of whether `F: Clone` (`ReqwestFetcher` is not — it
// holds a `Mutex`), so all clones share one rate-gated client.
impl<F: PageFetcher> Clone for MidPriceCache<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            client: self.client.clone(),
            source_log: self.source_log.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl MidPriceCache<ReqwestFetcher> {
    /// Production cache over a rate-limited Gamma client.
    pub fn new(gamma_base_url: String) -> Self {
        Self::with_fetcher(
            ReqwestFetcher::new(reqwest::Client::new()).with_min_interval_ms(GAMMA_MIN_INTERVAL_MS),
            gamma_base_url,
        )
    }
}

impl<F: PageFetcher + Send + Sync> MidPriceCache<F> {
    /// Build a cache over an arbitrary fetcher (test seam).
    pub fn with_fetcher(fetcher: F, gamma_base_url: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            client: Arc::new(GammaMarketsClient::new(gamma_base_url, fetcher)),
            source_log: None,
            clock: Arc::new(OffsetDateTime::now_utc),
        }
    }

    /// Replace the wall clock (scenario seam): the same clock stamps pages and freezes the strict
    /// evaluation instant, so a scenario clock never disagrees with its own observations.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// Bind the service's sole source-log coordinator. Every subsequent successful Gamma page is
    /// appended once before its values can enter the cache.
    #[must_use]
    pub fn with_source_log(mut self, source_log: SourceLogHandle) -> Self {
        self.source_log = Some(source_log);
        self
    }

    /// Install a fresh but value-unavailable cache row for a deterministic paper-API scenario.
    /// This seam is absent from production builds and performs no network I/O.
    #[cfg(feature = "scenario")]
    pub async fn seed_unavailable_scenario_price(
        &self,
        market_id: MarketId,
        observed_at: OffsetDateTime,
        receipt: AppendReceipt,
    ) {
        self.inner.lock().await.insert(
            market_id,
            CachedEntry {
                mids: Vec::new(),
                strict_mids: None,
                snapshot: MidMarketSnapshot::default(),
                observed_at,
                receipt: Some(receipt),
                conflicting: false,
            },
        );
    }

    /// Current mids (per `outcome_id`) for `market_ids`, served from cache within
    /// [`TTL`] and otherwise fetched concurrently through the rate gate. Markets
    /// whose fetch fails or lacks `outcomePrices` are omitted from the result.
    pub async fn fetch_mids(&self, market_ids: &[MarketId]) -> HashMap<MarketId, Vec<Decimal>> {
        self.ensure_entries(market_ids, &*self.clock)
            .await
            .entries
            .into_iter()
            .map(|(id, entry)| (id, entry.mids))
            .collect()
    }

    /// Current liquidity [`MidMarketSnapshot`] (per market) for `market_ids`,
    /// sharing the same TTL cache and rate gate as [`fetch_mids`](Self::fetch_mids).
    /// A market whose fetch fails or lacks `outcomePrices` is omitted — the
    /// snapshot rides on the same row as the mids. Snapshot scalars/token-ids are
    /// best-effort: a missing or malformed `liquidity`/`volume`/`clobTokenIds`
    /// surfaces as `None`/empty rather than dropping the market.
    pub async fn fetch_snapshots(
        &self,
        market_ids: &[MarketId],
    ) -> HashMap<MarketId, MidMarketSnapshot> {
        self.ensure_entries(market_ids, &*self.clock)
            .await
            .entries
            .into_iter()
            .map(|(id, entry)| (id, entry.snapshot))
            .collect()
    }

    /// Complete, fresh outcome prices for entry risk. Unlike the display methods, this rejects the
    /// entire request when any requested outcome lacks one unambiguous durable observation.
    pub(crate) async fn fetch_mids_strict(
        &self,
        ids: &[MarketOutcomeId],
    ) -> Result<BTreeMap<(String, u16), MidPriceObservation>, RiskInputsUnavailable> {
        self.fetch_mids_strict_attempt(ids).await.result
    }

    pub(crate) async fn fetch_mids_strict_attempt(
        &self,
        ids: &[MarketOutcomeId],
    ) -> StrictMidPriceAttempt {
        self.fetch_mids_strict_with_clock(ids, &*self.clock).await
    }

    pub(crate) async fn fetch_mids_strict_with_clock<C>(
        &self,
        ids: &[MarketOutcomeId],
        clock: C,
    ) -> StrictMidPriceAttempt
    where
        C: Fn() -> OffsetDateTime,
    {
        let mut markets = ids.iter().map(|id| id.market().clone()).collect::<Vec<_>>();
        markets.sort_by_key(ToString::to_string);
        markets.dedup();
        let ensured = self.ensure_entries(&markets, &clock).await;

        // The response pages are durably appended by `ensure_entries` and stamped from the same
        // clock; the one risk clock is taken only after that await so a newly fetched observation
        // cannot be newer than its validator.
        let evaluated_at = clock();
        let map = self.inner.lock().await;
        let mut price_receipts = ensured.missing_receipts;
        price_receipts.extend(
            ids.iter()
                .filter_map(|id| map.get(id.market()).and_then(|entry| entry.receipt)),
        );
        price_receipts.sort_by_key(|receipt| receipt.sequence);
        price_receipts.dedup();
        let result = {
            let mut entries = HashMap::new();
            for (market, entry) in map.iter() {
                let Some(receipt) = entry.receipt else {
                    continue;
                };
                entries.insert(
                    market.clone(),
                    StrictPriceInput {
                        strict_mids: entry.strict_mids.clone(),
                        observed_at_unix_nanos: entry.observed_at.unix_timestamp_nanos(),
                        receipt,
                        conflicting: entry.conflicting,
                    },
                );
            }
            classify_strict_prices(ids, evaluated_at.unix_timestamp_nanos(), &entries)
        };
        StrictMidPriceAttempt {
            evaluated_at,
            price_receipts,
            result,
        }
    }

    /// Serve fresh [`CachedEntry`]s for `market_ids` from the cache and fetch the stale/missing ones
    /// via the shared batched [`GammaMarketsClient`] (one `OpenOnly` `/markets` request per
    /// [`GAMMA_BATCH_SIZE`](pe_source_polymarket_public::GAMMA_BATCH_SIZE)-id chunk, demuxed by
    /// `conditionId`), storing the results. Both [`fetch_mids`](Self::fetch_mids) and
    /// [`fetch_snapshots`](Self::fetch_snapshots) project from this single fetch path, so the mids
    /// surface is byte-identical whether or not snapshots are read.
    ///
    /// Best-effort: a client error (transient fetch / corrupt response) is logged and this tick
    /// serves only what was cached — coarser than the pre-#382 per-ID skip. The live Kelly path
    /// (`orchestrator`) fetches one market per call, so a failing market omits only itself; the
    /// multi-market dashboard call (`paper_api`) instead drops every stale market for that tick,
    /// self-healing on the next [`TTL`] refresh. A market that is unknown, in a 4xx chunk, or lacks
    /// `outcomePrices` is omitted, so the caller marks its unrealized P&L null.
    /// `clock` stamps every page appended by this call, so observation time and any later
    /// freshness validation share one origin (wall clock in production, the injected clock in tests).
    async fn ensure_entries<C>(&self, market_ids: &[MarketId], clock: C) -> EnsuredEntries
    where
        C: Fn() -> OffsetDateTime,
    {
        let mut out: HashMap<MarketId, CachedEntry> = HashMap::new();
        let mut stale: Vec<MarketId> = Vec::new();

        // Brief lock: serve fresh entries, collect the rest. Never held across a fetch. Freshness
        // is judged on the cache clock so the refetch decision and the strict validation agree.
        {
            let now = clock();
            let ttl = time::Duration::seconds(i64::try_from(TTL.as_secs()).unwrap_or(i64::MAX));
            let map = self.inner.lock().await;
            for id in market_ids {
                match map.get(id) {
                    Some(entry) if now - entry.observed_at < ttl => {
                        out.insert(id.clone(), entry.clone());
                    }
                    _ => stale.push(id.clone()),
                }
            }
        }

        if stale.is_empty() {
            return EnsuredEntries {
                entries: out,
                missing_receipts: Vec::new(),
            };
        }

        // Open query (no `&closed=true`) → live mids in `outcomePrices`. The client batches and
        // demuxes by `conditionId`; the whole call is best-effort (see method doc).
        let ids: Vec<String> = stale.iter().map(|m| m.to_string()).collect();
        let fetched = match self
            .client
            .fetch_markets_with_pages(&ids, MarketFilter::OpenOnly)
            .await
        {
            Ok(fetched) => fetched,
            Err(error) => {
                let missing_receipts = self
                    .record_pages(&error.pages, &error.failed_requests, false, clock())
                    .await
                    .map(|recorded| {
                        let mut receipts = recorded.page_receipts.into_values().collect::<Vec<_>>();
                        receipts.extend(
                            recorded
                                .failure_receipts
                                .into_iter()
                                .map(|(_, receipt)| receipt),
                        );
                        receipts
                    })
                    .unwrap_or_else(|()| {
                        warn!("mid-cache: source log closed while recording rejected Gamma pages");
                        Vec::new()
                    });
                warn!(error = %error, stale = stale.len(), "mid-cache: batch fetch error, omitting this tick");
                return EnsuredEntries {
                    entries: out,
                    missing_receipts,
                };
            }
        };
        let observed = clock();
        let recorded = match self
            .record_pages(&fetched.pages, &fetched.failed_requests, true, observed)
            .await
        {
            Ok(recorded) => recorded,
            Err(()) => {
                warn!("mid-cache: source log closed, omitting unrecorded Gamma prices");
                return EnsuredEntries {
                    entries: out,
                    missing_receipts: Vec::new(),
                };
            }
        };
        let mut missing_receipts = recorded
            .failure_receipts
            .iter()
            .map(|(_, receipt)| *receipt)
            .collect::<Vec<_>>();
        for (evidence, _) in &fetched.pages {
            let Some(request) = GammaOpenConditionRequest::parse(&evidence.request_url) else {
                continue;
            };
            if request.condition_ids.iter().any(|condition_id| {
                fetched.condition_page_hashes.get(condition_id) != Some(&evidence.raw_page_hash)
                    || fetched
                        .markets
                        .markets
                        .get(condition_id)
                        .is_none_or(|market| market.outcome_prices.is_none())
            }) && let Some(receipt) = recorded
                .page_receipts
                .get(&(evidence.request_url.clone(), evidence.raw_page_hash.clone()))
            {
                missing_receipts.push(*receipt);
            }
        }
        let conflicts = fetched
            .conflicting_condition_ids
            .into_iter()
            .collect::<HashSet<_>>();

        let mut map = self.inner.lock().await;
        for id in stale {
            // Demux by the echoed `conditionId`: a hit is structurally the requested market, so a
            // cross-market row (keyed under its own id) can never be attributed here.
            let Some(m) = fetched.markets.markets.get(&id.to_string()) else {
                continue; // unknown / 4xx-unfetched / absent → omit (P&L stays null)
            };
            // No / malformed `outcomePrices` → skip rather than mis-value the market.
            let Some(mids) = m.outcome_prices.clone() else {
                continue;
            };
            let snapshot = MidMarketSnapshot {
                liquidity: m.liquidity,
                volume: m.volume,
                clob_token_ids: m.clob_token_ids.clone(),
            };
            let entry = CachedEntry {
                mids,
                strict_mids: m.strict_outcome_prices.clone(),
                snapshot,
                observed_at: observed,
                receipt: fetched
                    .condition_page_hashes
                    .get(&id.to_string())
                    .and_then(|hash| {
                        fetched.pages.iter().find_map(|(evidence, _)| {
                            (evidence.raw_page_hash == *hash
                                && GammaOpenConditionRequest::parse(&evidence.request_url)
                                    .is_some_and(|request| {
                                        request.condition_ids.contains(&id.to_string())
                                    }))
                            .then_some((evidence.request_url.clone(), hash.clone()))
                        })
                    })
                    .and_then(|key| recorded.page_receipts.get(&key))
                    .copied(),
                conflicting: conflicts.contains(&id.to_string()),
            };
            map.insert(id.clone(), entry.clone());
            out.insert(id, entry);
        }
        EnsuredEntries {
            entries: out,
            missing_receipts,
        }
    }

    async fn record_pages(
        &self,
        pages: &[(MetadataPageEvidence, Vec<u8>)],
        failed_requests: &[(String, String)],
        usable: bool,
        observed: OffsetDateTime,
    ) -> Result<RecordedGammaPages, ()> {
        let Some(source_log) = &self.source_log else {
            return Ok(RecordedGammaPages::default());
        };
        let mut recorded = RecordedGammaPages::default();
        for (evidence, payload) in pages {
            let payload = serde_json::to_vec(&GammaPriceAttemptRecord::Page {
                evidence: evidence.clone(),
                payload: payload.clone(),
                usable,
            })
            .map_err(|_| ())?;
            let receipt = source_log
                .append(EnvelopeIn {
                    source_id: pe_core_types::SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()),
                    schema_version: GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
                    parser_version: GAMMA_PRICE_ATTEMPT_PARSER_VERSION,
                    observed_at: SourceTimestamp(observed),
                    received_at: ReceivedAt(observed),
                    content_type: ContentType::Json,
                    payload,
                })
                .await
                .map_err(|_| ())?;
            recorded.page_receipts.insert(
                (evidence.request_url.clone(), evidence.raw_page_hash.clone()),
                receipt,
            );
        }
        for (request_url, error) in failed_requests {
            let payload = serde_json::to_vec(&GammaPriceAttemptRecord::Failure {
                request_url: request_url.clone(),
                error: error.clone(),
            })
            .map_err(|_| ())?;
            let receipt = source_log
                .append(EnvelopeIn {
                    source_id: pe_core_types::SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()),
                    schema_version: GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
                    parser_version: GAMMA_PRICE_ATTEMPT_PARSER_VERSION,
                    observed_at: SourceTimestamp(observed),
                    received_at: ReceivedAt(observed),
                    content_type: ContentType::Json,
                    payload,
                })
                .await
                .map_err(|_| ())?;
            recorded
                .failure_receipts
                .push((request_url.clone(), receipt));
        }
        Ok(recorded)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::activity_ingest::ActivityIngest;
    use crate::health::new_shared_health_with_ws;
    use crate::source_event_sink::SourceEventSink;
    use pe_core_types::{EventSeq, OutcomeId};
    use pe_source_polymarket_public::FixtureFetcher;
    use time::macros::datetime;
    use tokio::sync::{Barrier, mpsc};

    struct OverlappingFetcher {
        barrier: Arc<Barrier>,
        calls: AtomicUsize,
    }

    impl PageFetcher for OverlappingFetcher {
        async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, pe_source_core::SourceError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait().await;
            let price = if call == 0 { "0.4" } else { "0.6" };
            Ok(format!(
                r#"[{{"conditionId":"0xoverlap","outcomePrices":"[\"{price}\",\"0.5\"]"}}]"#
            )
            .into_bytes())
        }
    }

    fn mid(s: &str) -> MarketId {
        s.parse().unwrap()
    }

    /// The exact URL the shared client builds for a single-id `OpenOnly` batch
    /// (`condition_ids={id}` plus the canonical limit, no `&closed=true`). The
    /// orchestrator/snapshot worker fetch one market per call, so every live mid fetch is a
    /// batch-of-one keyed like this.
    fn url(base: &str, id: &str) -> String {
        format!("{base}/markets?condition_ids={id}&limit={GAMMA_BATCH_LIMIT_PARAM}")
    }

    const BASE: &str = "https://gamma-api.polymarket.com";

    fn outcome(market: &str, index: u16) -> MarketOutcomeId {
        MarketOutcomeId::new(mid(market), OutcomeId(index))
    }

    fn receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(u8::MAX); 32]),
        }
    }

    async fn insert_strict_entry(
        cache: &MidPriceCache<FixtureFetcher>,
        market: &str,
        prices: Option<Vec<Price>>,
        observed_at: OffsetDateTime,
        receipt: Option<AppendReceipt>,
        conflicting: bool,
    ) {
        let mids = prices
            .as_ref()
            .map(|values| values.iter().map(|price| price.0).collect())
            .unwrap_or_default();
        cache.inner.lock().await.insert(
            mid(market),
            CachedEntry {
                mids,
                strict_mids: prices,
                snapshot: MidMarketSnapshot::default(),
                observed_at,
                receipt,
                conflicting,
            },
        );
    }

    /// PASS: a complete strict cache hit returns exact prices and reuses its original receipt.
    #[tokio::test]
    async fn strict_hit_is_complete_and_receipt_stable() {
        let cache =
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), BASE.to_owned());
        let now = datetime!(2026-09-05 12:00 UTC);
        insert_strict_entry(
            &cache,
            "0xstrict",
            Some(vec![
                Price::new(Decimal::new(6, 1)).unwrap(),
                Price::new(Decimal::new(4, 1)).unwrap(),
            ]),
            now,
            Some(receipt(7)),
            false,
        )
        .await;
        let ids = [outcome("0xstrict", 0), outcome("0xstrict", 1)];
        let first = cache
            .fetch_mids_strict_with_clock(&ids, || now)
            .await
            .result
            .unwrap();
        let second = cache
            .fetch_mids_strict_with_clock(&ids, || now)
            .await
            .result
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.get(&("0xstrict".to_owned(), 0)).unwrap().receipt,
            receipt(7)
        );
        assert_eq!(
            first.get(&("0xstrict".to_owned(), 1)).unwrap().price.0,
            Decimal::new(4, 1)
        );
    }

    /// PASS: any absent market, outcome, strict parse, or durable receipt fails the whole request.
    #[tokio::test]
    async fn strict_missing_and_incomplete_inputs_fail_closed() {
        let cache =
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), BASE.to_owned());
        let now = datetime!(2026-09-05 12:00 UTC);
        assert_eq!(
            cache
                .fetch_mids_strict_with_clock(&[outcome("missing", 0)], || now)
                .await
                .result,
            Err(RiskInputsUnavailable::PriceMissing)
        );
        insert_strict_entry(
            &cache,
            "incomplete",
            Some(vec![Price::new(Decimal::new(5, 1)).unwrap()]),
            now,
            Some(receipt(1)),
            false,
        )
        .await;
        assert_eq!(
            cache
                .fetch_mids_strict_with_clock(&[outcome("incomplete", 1)], || now)
                .await
                .result,
            Err(RiskInputsUnavailable::PriceMissing)
        );
        insert_strict_entry(&cache, "malformed", None, now, Some(receipt(2)), false).await;
        assert_eq!(
            cache
                .fetch_mids_strict_with_clock(&[outcome("malformed", 0)], || now)
                .await
                .result,
            Err(RiskInputsUnavailable::PriceMissing)
        );
        insert_strict_entry(
            &cache,
            "unrecorded",
            Some(vec![Price::new(Decimal::new(5, 1)).unwrap()]),
            now,
            None,
            false,
        )
        .await;
        assert_eq!(
            cache
                .fetch_mids_strict_with_clock(&[outcome("unrecorded", 0)], || now)
                .await
                .result,
            Err(RiskInputsUnavailable::PriceMissing)
        );
    }

    /// PASS: exact TTL, future, and conflicting evidence return their distinct typed refusals.
    #[tokio::test]
    async fn strict_time_and_conflict_failures_are_distinct() {
        let cache =
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), BASE.to_owned());
        let now = datetime!(2026-09-05 12:00 UTC);
        let one_price = || vec![Price::new(Decimal::new(5, 1)).unwrap()];
        insert_strict_entry(
            &cache,
            "stale",
            Some(one_price()),
            now - time::Duration::seconds(60),
            Some(receipt(1)),
            false,
        )
        .await;
        assert_eq!(
            cache
                .fetch_mids_strict_with_clock(&[outcome("stale", 0)], || now)
                .await
                .result,
            Err(RiskInputsUnavailable::PriceStale)
        );
        insert_strict_entry(
            &cache,
            "future",
            Some(one_price()),
            now + time::Duration::nanoseconds(1),
            Some(receipt(2)),
            false,
        )
        .await;
        let future_attempt = cache
            .fetch_mids_strict_with_clock(&[outcome("future", 0)], || now)
            .await;
        assert_eq!(
            future_attempt.result,
            Err(RiskInputsUnavailable::PriceFuture)
        );
        assert_eq!(future_attempt.price_receipts, vec![receipt(2)]);
        insert_strict_entry(
            &cache,
            "conflict",
            Some(one_price()),
            now,
            Some(receipt(3)),
            true,
        )
        .await;
        assert_eq!(
            cache
                .fetch_mids_strict_with_clock(&[outcome("conflict", 0)], || now)
                .await
                .result,
            Err(RiskInputsUnavailable::PriceConflict)
        );
    }

    /// PASS: with a clock that advances on every read, both a cold cache and a stale seeded entry
    /// refetch, stamp the page from the read taken after the response, and evaluate on a later
    /// read — the observation is never newer than its validator and the new receipt is recorded.
    #[tokio::test]
    async fn strict_cold_and_stale_fetches_evaluate_after_the_response() {
        for stale_seed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let source_path = dir.path().join("source.log");
            let (source_log, source_rx) = SourceLogHandle::channel(4);
            let (trigger_tx, _trigger_rx) = mpsc::channel(1);
            let ingest = tokio::spawn(
                ActivityIngest::poll_only(
                    SourceEventSink::open(&source_path).unwrap(),
                    source_rx,
                    trigger_tx,
                    new_shared_health_with_ws(false, false, 90),
                )
                .run(),
            );
            let mut fx = HashMap::new();
            fx.insert(
                url(BASE, "0xcold"),
                br#"[{"conditionId":"0xcold","outcomePrices":"[\"0.62\",\"0.38\"]"}]"#.to_vec(),
            );
            let base = datetime!(2026-09-05 12:00 UTC);
            let ticks = Arc::new(std::sync::atomic::AtomicI64::new(0));
            let clock_ticks = Arc::clone(&ticks);
            let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_owned())
                .with_source_log(source_log)
                .with_clock(Arc::new(move || {
                    base + time::Duration::seconds(
                        clock_ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    )
                }));
            if stale_seed {
                insert_strict_entry(
                    &cache,
                    "0xcold",
                    Some(vec![
                        Price::new(Decimal::new(5, 1)).unwrap(),
                        Price::new(Decimal::new(5, 1)).unwrap(),
                    ]),
                    base - time::Duration::seconds(61),
                    None,
                    false,
                )
                .await;
            }

            let attempt = cache
                .fetch_mids_strict_attempt(&[outcome("0xcold", 0)])
                .await;

            assert!(
                attempt.result.is_ok(),
                "stale_seed={stale_seed}: {:?}",
                attempt.result
            );
            assert_eq!(attempt.price_receipts.len(), 1, "stale_seed={stale_seed}");
            // Reads: the refetch decision, the post-response stamp, then the evaluation instant.
            assert_eq!(attempt.evaluated_at, base + time::Duration::seconds(2));
            assert_eq!(ticks.load(std::sync::atomic::Ordering::SeqCst), 3);
            ingest.abort();
            let _ = ingest.await;
        }
    }

    /// PASS: a process-local cache restart cannot inherit an earlier valid page; the failed new
    /// request records its own request-bound receipt and that receipt alone proves PriceMissing.
    #[tokio::test]
    async fn strict_post_restart_failure_records_only_the_failed_request() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let (source_log, source_rx) = SourceLogHandle::channel(4);
        let (trigger_tx, _trigger_rx) = mpsc::channel(1);
        let ingest = tokio::spawn(
            ActivityIngest::poll_only(
                SourceEventSink::open(&source_path).unwrap(),
                source_rx,
                trigger_tx,
                new_shared_health_with_ws(false, false, 90),
            )
            .run(),
        );
        let mut first_response = HashMap::new();
        first_response.insert(
            url(BASE, "0xrestart"),
            br#"[{"conditionId":"0xrestart","outcomePrices":"[\"0.62\",\"0.38\"]"}]"#.to_vec(),
        );
        let first =
            MidPriceCache::with_fetcher(FixtureFetcher::new(first_response), BASE.to_owned())
                .with_source_log(source_log.clone());
        let prior = first
            .fetch_mids_strict(&[outcome("0xrestart", 0)])
            .await
            .unwrap();
        let prior_receipt = prior.get(&("0xrestart".to_owned(), 0)).unwrap().receipt;
        drop(first);

        let restarted =
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), BASE.to_owned())
                .with_source_log(source_log);
        let failed = restarted
            .fetch_mids_strict_attempt(&[outcome("0xrestart", 0)])
            .await;

        assert_eq!(failed.result, Err(RiskInputsUnavailable::PriceMissing));
        assert_eq!(failed.price_receipts.len(), 1);
        assert_ne!(failed.price_receipts[0], prior_receipt);
        drop(restarted);
        ingest.abort();
        let _ = ingest.await;
        let entries = pe_event_log::Reader::replay(&source_path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let failed_envelope = entries
            .iter()
            .find(|(sequence, _)| *sequence == failed.price_receipts[0].sequence)
            .map(|(_, envelope)| envelope)
            .unwrap();
        assert_eq!(
            failed_envelope.schema_version,
            GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION
        );
        assert_eq!(
            serde_json::from_slice::<GammaPriceAttemptRecord>(&failed_envelope.payload).unwrap(),
            GammaPriceAttemptRecord::Failure {
                request_url: url(BASE, "0xrestart"),
                error: format!("no fixture for URL: {}", url(BASE, "0xrestart")),
            }
        );
    }

    /// PASS: overlapping cloned fetches may publish in either order, but each strict attempt binds
    /// only the receipt of the cache row it actually classified.
    #[tokio::test]
    async fn overlapping_clones_bind_the_price_row_each_attempt_classified() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let (source_log, source_rx) = SourceLogHandle::channel(4);
        let (trigger_tx, _trigger_rx) = mpsc::channel(1);
        let ingest = tokio::spawn(
            ActivityIngest::poll_only(
                SourceEventSink::open(&source_path).unwrap(),
                source_rx,
                trigger_tx,
                new_shared_health_with_ws(false, false, 90),
            )
            .run(),
        );
        let cache = MidPriceCache::with_fetcher(
            OverlappingFetcher {
                barrier: Arc::new(Barrier::new(2)),
                calls: AtomicUsize::new(0),
            },
            BASE.to_owned(),
        )
        .with_source_log(source_log);
        let left = cache.clone();
        let right = cache.clone();
        let left_ids = [outcome("0xoverlap", 0)];
        let right_ids = [outcome("0xoverlap", 0)];
        let (left_attempt, right_attempt) = tokio::join!(
            left.fetch_mids_strict_attempt(&left_ids),
            right.fetch_mids_strict_attempt(&right_ids),
        );

        for attempt in [left_attempt, right_attempt] {
            let observation = attempt
                .result
                .unwrap()
                .get(&("0xoverlap".to_owned(), 0))
                .copied()
                .unwrap();
            assert_eq!(attempt.price_receipts, vec![observation.receipt]);
        }
        drop(left);
        drop(right);
        drop(cache);
        ingest.abort();
        let _ = ingest.await;
        assert_eq!(
            pe_event_log::Reader::replay(&source_path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn fetches_and_parses_open_mid() {
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            out.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(62, 2), Decimal::new(38, 2)]
        );
    }

    #[tokio::test]
    async fn ttl_hit_serves_without_refetch() {
        // Second call uses an empty fetcher; the value must come from the cache.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.50\",\"0.50\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let _ = cache.fetch_mids(&[mid("0xcond")]).await; // populate
        let out = cache.fetch_mids(&[mid("0xcond")]).await; // within TTL → cache
        assert_eq!(
            out.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(5, 1), Decimal::new(5, 1)]
        );
    }

    #[tokio::test]
    async fn missing_market_is_softly_omitted() {
        // Fetcher has no fixture for this url → fetch error → omitted, not a panic.
        let cache =
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xabsent")]).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn condition_id_mismatch_is_rejected() {
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xwant"),
            br#"[{"conditionId":"0xother","outcomePrices":"[\"0.9\",\"0.1\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xwant")]).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn fetch_snapshots_parses_liquidity_volume_and_ordered_token_ids() {
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]","liquidity":"6434.84","volume":"99995.018095","clobTokenIds":"[\"111\",\"222\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = out.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, Some(Decimal::new(643484, 2)));
        assert_eq!(snap.volume, Some(Decimal::new(99995018095, 6)));
        // Ordered by outcome: index 0 = first outcome's token, index 1 = second.
        assert_eq!(
            snap.clob_token_ids,
            vec!["111".to_string(), "222".to_string()]
        );
    }

    #[tokio::test]
    async fn snapshot_fields_absent_yield_none_and_empty_without_dropping_mids() {
        // A row with no liquidity/volume/clobTokenIds still yields its mids; the
        // snapshot is simply empty. Proves the new fields are non-regressive.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.7\",\"0.3\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = snaps.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, None);
        assert_eq!(snap.volume, None);
        assert!(snap.clob_token_ids.is_empty());
        // Same cache row still serves the mids.
        let mids = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            mids.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(7, 1), Decimal::new(3, 1)]
        );
    }

    #[tokio::test]
    async fn liquidity_accepts_numeric_form() {
        // Gamma occasionally serializes the scalar as a JSON number; the lenient
        // decoder accepts it (`liquidityNum`-style fixtures).
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","liquidity":6434}]"#
                .to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        assert_eq!(
            snaps.get(&mid("0xcond")).unwrap().liquidity,
            Some(Decimal::from(6434))
        );
    }

    #[tokio::test]
    async fn malformed_liquidity_does_not_drop_mids() {
        // An unparseable liquidity string (and a malformed token array) must not
        // fail the row: the scalar is None / tokens empty, but the mids survive —
        // the zero-fill-path-risk guarantee.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.4\",\"0.6\"]","liquidity":"not-a-number","clobTokenIds":"oops"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = snaps.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, None);
        assert!(snap.clob_token_ids.is_empty());
        let mids = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            mids.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(4, 1), Decimal::new(6, 1)]
        );
    }

    #[tokio::test]
    async fn fetch_mids_unchanged_when_snapshot_fields_present() {
        // Regression guard: the extra Gamma fields do not alter the mids result.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.62\",\"0.38\"]","liquidity":"100","volume":"200","clobTokenIds":"[\"a\",\"b\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            out.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(62, 2), Decimal::new(38, 2)]
        );
    }

    #[tokio::test]
    async fn snapshot_served_from_cache_within_ttl() {
        // Second call uses an empty fetcher; the snapshot must come from the cache.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","liquidity":"12.5","clobTokenIds":"[\"t0\",\"t1\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let _ = cache.fetch_snapshots(&[mid("0xcond")]).await; // populate
        let out = cache.fetch_snapshots(&[mid("0xcond")]).await; // within TTL → cache
        let snap = out.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, Some(Decimal::new(125, 1)));
        assert_eq!(
            snap.clob_token_ids,
            vec!["t0".to_string(), "t1".to_string()]
        );
    }

    #[tokio::test]
    async fn clob_token_ids_preserve_blank_positions() {
        // A blank id must NOT be compacted away — `clob_token_ids[outcome_id]` is
        // indexed positionally, so dropping index 0 would misalign every outcome.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","clobTokenIds":"[\"\",\"222\"]"}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        assert_eq!(
            snaps.get(&mid("0xcond")).unwrap().clob_token_ids,
            vec![String::new(), "222".to_string()]
        );
    }

    #[tokio::test]
    async fn clob_token_ids_accept_native_json_array() {
        // Robustness: a native JSON array (not the stringified form) is accepted.
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.5\",\"0.5\"]","clobTokenIds":["111","222"]}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        assert_eq!(
            snaps.get(&mid("0xcond")).unwrap().clob_token_ids,
            vec!["111".to_string(), "222".to_string()]
        );
    }

    #[tokio::test]
    async fn unexpected_json_types_do_not_drop_mids() {
        // bool liquidity, object volume, native-array-of-numbers token ids — all
        // unexpected types. None must fail the row: scalars → None, and the mids
        // still parse (the zero-fill-path-risk guarantee, for any JSON shape).
        let mut fx = HashMap::new();
        fx.insert(
            url(BASE, "0xcond"),
            br#"[{"conditionId":"0xcond","outcomePrices":"[\"0.4\",\"0.6\"]","liquidity":true,"volume":{"x":1},"clobTokenIds":42}]"#.to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let snaps = cache.fetch_snapshots(&[mid("0xcond")]).await;
        let snap = snaps.get(&mid("0xcond")).unwrap();
        assert_eq!(snap.liquidity, None);
        assert_eq!(snap.volume, None);
        assert!(snap.clob_token_ids.is_empty());
        let mids = cache.fetch_mids(&[mid("0xcond")]).await;
        assert_eq!(
            mids.get(&mid("0xcond")).unwrap(),
            &vec![Decimal::new(4, 1), Decimal::new(6, 1)]
        );
    }

    #[tokio::test]
    async fn multi_id_batch_demuxes_and_rejects_cross_market_row() {
        // The dashboard path (`paper_api::fetch_mids(&open_markets)`) fetches many markets in one
        // batch. The client builds a single repeat-key URL; the response arrives out of order and
        // carries an unrelated row. Each requested market gets its own mids; the intruder (`0xZ`) is
        // keyed under its own id and never attributed to a requested market.
        let mut fx = HashMap::new();
        fx.insert(
            format!(
                "{BASE}/markets?condition_ids=0xA&condition_ids=0xB&limit={GAMMA_BATCH_LIMIT_PARAM}"
            ),
            br#"[{"conditionId":"0xB","outcomePrices":"[\"0.3\",\"0.7\"]"},
                 {"conditionId":"0xZ","outcomePrices":"[\"0.99\",\"0.01\"]"},
                 {"conditionId":"0xA","outcomePrices":"[\"0.6\",\"0.4\"]"}]"#
                .to_vec(),
        );
        let cache = MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string());
        let out = cache.fetch_mids(&[mid("0xA"), mid("0xB")]).await;
        assert_eq!(out.len(), 2);
        assert_eq!(
            out.get(&mid("0xA")).unwrap(),
            &vec![Decimal::new(6, 1), Decimal::new(4, 1)]
        );
        assert_eq!(
            out.get(&mid("0xB")).unwrap(),
            &vec![Decimal::new(3, 1), Decimal::new(7, 1)]
        );
        assert!(
            !out.contains_key(&mid("0xZ")),
            "an unrequested cross-market row must not be attributed"
        );
    }
}
