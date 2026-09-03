//! Process-lifetime Polymarket token identity authority (#555).
//!
//! Fresh Gamma pages become usable only after the service's single source-log
//! owner acknowledges their durable append. Verified identities retain that
//! immutable provenance across cache hits.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use pe_core_types::{PolymarketTokenId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_core::SourceError;
use pe_source_polymarket_public::gamma_markets::verify_token_identities;
use pe_source_polymarket_public::{
    GammaMarketsClient, GammaMarketsError, MarketFilter, MetadataPageEvidence,
    ReconciliationFetcher, ReconciliationPageFetcher, VerifiedTokenIdentity,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::activity_ingest::{SourceLogHandle, SourceLogHandleError};
use crate::source_event_sink::SourceEventSink;

/// Boot-time source-log owner shared with the recording position validator.
pub type BootSourceLog = Arc<Mutex<SourceEventSink>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IdentityProvenance {
    pub asset: PolymarketTokenId,
    pub source_log_sequence: u64,
    pub canonical_page_hash: String,
}

#[derive(Debug, Clone)]
struct CachedIdentity {
    identity: VerifiedTokenIdentity,
    provenance: IdentityProvenance,
}

#[derive(Clone)]
enum IdentityRecorder {
    Boot(BootSourceLog),
    Runtime(SourceLogHandle),
}

/// One call's verified/unverified results. `provenance` includes cache hits as
/// well as fresh identities.
pub struct ResolvedIdentities {
    pub verified: BTreeMap<PolymarketTokenId, VerifiedTokenIdentity>,
    pub unverified: BTreeMap<PolymarketTokenId, String>,
    pub provenance: BTreeMap<PolymarketTokenId, IdentityProvenance>,
}

pub struct AssetIdentityResolver {
    client: GammaMarketsClient<ReconciliationPageFetcher>,
    gamma_batch_size: usize,
    cache: RwLock<HashMap<PolymarketTokenId, CachedIdentity>>,
    recorder: RwLock<IdentityRecorder>,
}

impl AssetIdentityResolver {
    #[must_use]
    pub fn new(
        fetcher: Arc<dyn ReconciliationFetcher>,
        gamma_base_url: String,
        gamma_batch_size: usize,
        boot_source_log: BootSourceLog,
    ) -> Self {
        Self::with_recorder(
            fetcher,
            gamma_base_url,
            gamma_batch_size,
            IdentityRecorder::Boot(boot_source_log),
        )
    }

    #[must_use]
    pub fn new_runtime(
        fetcher: Arc<dyn ReconciliationFetcher>,
        gamma_base_url: String,
        gamma_batch_size: usize,
        source_log: SourceLogHandle,
    ) -> Self {
        Self::with_recorder(
            fetcher,
            gamma_base_url,
            gamma_batch_size,
            IdentityRecorder::Runtime(source_log),
        )
    }

    fn with_recorder(
        fetcher: Arc<dyn ReconciliationFetcher>,
        gamma_base_url: String,
        gamma_batch_size: usize,
        recorder: IdentityRecorder,
    ) -> Self {
        let gamma_batch_size = gamma_batch_size.max(1);
        Self {
            client: GammaMarketsClient::new(gamma_base_url, ReconciliationPageFetcher(fetcher))
                .with_batch_size(gamma_batch_size),
            gamma_batch_size,
            cache: RwLock::new(HashMap::new()),
            recorder: RwLock::new(recorder),
        }
    }

    /// Switch from the boot writer to the runtime coordinator before the
    /// latter opens the same source-log path.
    pub async fn activate_runtime(&self, source_log: SourceLogHandle) {
        *self.recorder.write().await = IdentityRecorder::Runtime(source_log);
    }

    pub async fn resolve(
        &self,
        tokens: impl IntoIterator<Item = PolymarketTokenId>,
    ) -> Result<ResolvedIdentities, SourceError> {
        let requested = tokens.into_iter().collect::<BTreeSet<_>>();
        let mut resolved = self.cached(&requested).await;
        let misses = requested
            .iter()
            .filter(|token| !resolved.verified.contains_key(*token))
            .cloned()
            .collect::<Vec<_>>();
        if misses.is_empty() {
            return Ok(resolved);
        }

        let mut pages = Vec::new();
        let mut sequences = HashMap::new();
        let mut newly_verified = BTreeMap::new();
        self.fetch_and_record_chunks(
            &misses,
            MarketFilter::OpenOnly,
            "gamma token lookup rejected",
            &mut pages,
            &mut sequences,
        )
        .await?;
        let open_identities = verify_token_identities(&misses, &pages);
        let leftovers = misses
            .iter()
            .filter(|token| !open_identities.contains_key(*token))
            .cloned()
            .collect::<Vec<_>>();

        if !leftovers.is_empty() {
            self.fetch_and_record_chunks(
                &leftovers,
                MarketFilter::ClosedOnly,
                "gamma closed-token lookup rejected",
                &mut pages,
                &mut sequences,
            )
            .await?;
        }

        collect_verified(
            verify_token_identities(&misses, &pages),
            &sequences,
            &mut newly_verified,
            &mut resolved.unverified,
        );

        for token in misses {
            if !newly_verified.contains_key(&token) && !resolved.unverified.contains_key(&token) {
                resolved.unverified.insert(
                    token,
                    "token absent from open and closed Gamma metadata".to_owned(),
                );
            }
        }
        {
            let mut cache = self.cache.write().await;
            for (token, cached) in &newly_verified {
                cache.insert(token.clone(), cached.clone());
            }
        }
        for (token, cached) in newly_verified {
            resolved
                .verified
                .insert(token.clone(), cached.identity.clone());
            resolved.provenance.insert(token, cached.provenance);
        }
        Ok(resolved)
    }

    async fn fetch_and_record_chunks(
        &self,
        tokens: &[PolymarketTokenId],
        filter: MarketFilter,
        rejected_message: &str,
        pages: &mut Vec<(MetadataPageEvidence, Vec<u8>)>,
        sequences: &mut HashMap<String, u64>,
    ) -> Result<(), SourceError> {
        for chunk in tokens.chunks(self.gamma_batch_size) {
            let fetched = match self
                .client
                .fetch_markets_by_token_ids(
                    &chunk
                        .iter()
                        .map(|token| token.0.clone())
                        .collect::<Vec<_>>(),
                    filter,
                )
                .await
            {
                Ok(fetched) => fetched,
                Err(error) => {
                    if let Some(page) = &error.page {
                        self.record_page(page).await?;
                    }
                    return Err(map_gamma_error(error.source));
                }
            };
            if let Some(page) = fetched.page {
                let sequence = self.record_page(&page).await?;
                sequences
                    .entry(page.0.canonical_page_hash.clone())
                    .or_insert(sequence);
                pages.push(page);
            }
            if !fetched.markets.unfetched.is_empty() {
                return Err(SourceError::Fatal {
                    message: format!(
                        "{rejected_message}: {}",
                        fetched.markets.unfetched.join(",")
                    ),
                });
            }
        }
        Ok(())
    }

    async fn cached(&self, requested: &BTreeSet<PolymarketTokenId>) -> ResolvedIdentities {
        let cache = self.cache.read().await;
        let mut verified = BTreeMap::new();
        let mut provenance = BTreeMap::new();
        for token in requested {
            if let Some(cached) = cache.get(token) {
                verified.insert(token.clone(), cached.identity.clone());
                provenance.insert(token.clone(), cached.provenance.clone());
            }
        }
        ResolvedIdentities {
            verified,
            unverified: BTreeMap::new(),
            provenance,
        }
    }

    async fn record_page(
        &self,
        page: &(MetadataPageEvidence, Vec<u8>),
    ) -> Result<u64, SourceError> {
        let recorder = self.recorder.read().await.clone();
        let (evidence, payload) = page;
        let envelope = EnvelopeIn {
            source_id: evidence.source_id.clone(),
            schema_version: evidence.schema_version,
            parser_version: evidence.parser_version,
            observed_at: SourceTimestamp(evidence.received_at.0),
            received_at: evidence.received_at.clone(),
            content_type: ContentType::Json,
            payload: payload.clone(),
        };
        let sequence =
            match &recorder {
                IdentityRecorder::Boot(sink) => sink
                    .lock()
                    .await
                    .append_durable(envelope)
                    .map_err(|error| SourceError::Fatal {
                        message: format!("source-log append failed: {error}"),
                    })?,
                IdentityRecorder::Runtime(source_log) => source_log
                    .append(envelope)
                    .await
                    .map_err(|SourceLogHandleError::Closed| SourceError::Fatal {
                        message: "source-log coordinator closed".to_owned(),
                    })?,
            };
        Ok(sequence.0)
    }
}

fn collect_verified(
    identities: BTreeMap<
        PolymarketTokenId,
        Result<VerifiedTokenIdentity, pe_source_polymarket_public::MetadataIdentityError>,
    >,
    sequences: &HashMap<String, u64>,
    verified: &mut BTreeMap<PolymarketTokenId, CachedIdentity>,
    unverified: &mut BTreeMap<PolymarketTokenId, String>,
) {
    for (token, identity) in identities {
        match identity {
            Ok(identity) => {
                if let Some(sequence) = sequences.get(&identity.evidence_hash) {
                    verified.insert(
                        token.clone(),
                        CachedIdentity {
                            provenance: IdentityProvenance {
                                asset: token,
                                source_log_sequence: *sequence,
                                canonical_page_hash: identity.evidence_hash.clone(),
                            },
                            identity,
                        },
                    );
                } else {
                    unverified.insert(
                        token,
                        "verified identity has no recorded metadata page".to_owned(),
                    );
                }
            }
            Err(error) => {
                unverified.insert(token, error.to_string());
            }
        }
    }
}

fn map_gamma_error(error: GammaMarketsError) -> SourceError {
    match error {
        GammaMarketsError::Fetch(message) => {
            let prefix = "rate limited: retry after ";
            let suffix = "s";
            if let Some(seconds) = message
                .strip_prefix(prefix)
                .and_then(|value| value.strip_suffix(suffix))
                .and_then(|value| value.parse::<u32>().ok())
            {
                SourceError::RateLimited {
                    retry_after_secs: seconds,
                }
            } else {
                SourceError::Transient { message }
            }
        }
        GammaMarketsError::Parse(message) => SourceError::Fatal {
            message: format!("gamma metadata parse failed: {message}"),
        },
        GammaMarketsError::InvalidTokenId { token } => SourceError::Fatal {
            message: format!("gamma metadata token invalid: {token}"),
        },
        GammaMarketsError::TooManyTokenIds { tokens, limit } => SourceError::Fatal {
            message: format!(
                "gamma metadata token batch has {tokens} ids, above per-request limit {limit}"
            ),
        },
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pe_event_log::Reader;
    use pe_source_polymarket_public::{
        GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION, GAMMA_MARKETS_SOURCE_ID,
    };

    use super::*;

    const BASE: &str = "https://gamma.example.test";

    struct GammaFixture {
        calls: AtomicUsize,
        urls: StdMutex<Vec<String>>,
        open: Vec<u8>,
        closed: Vec<u8>,
    }

    struct MixedChunkFixture {
        calls: AtomicUsize,
    }

    impl ReconciliationFetcher for MixedChunkFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if url.contains("clob_token_ids=token-b") {
                    Err(SourceError::Transient {
                        message: "injected second chunk failure".to_owned(),
                    })
                } else {
                    Ok(br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#.to_vec())
                }
            })
        }
    }

    impl GammaFixture {
        fn new(open: &[u8], closed: &[u8]) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                urls: StdMutex::new(Vec::new()),
                open: open.to_vec(),
                closed: closed.to_vec(),
            }
        }
    }

    impl ReconciliationFetcher for GammaFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.urls.lock().unwrap().push(url.to_owned());
                if url.contains("closed=true") {
                    Ok(self.closed.clone())
                } else {
                    Ok(self.open.clone())
                }
            })
        }
    }

    fn boot_resolver(
        fetcher: Arc<dyn ReconciliationFetcher>,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        BootSourceLog,
        AssetIdentityResolver,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let sink = Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap()));
        let resolver = AssetIdentityResolver::new(fetcher, BASE.to_owned(), 50, Arc::clone(&sink));
        (dir, path, sink, resolver)
    }

    #[tokio::test]
    async fn fresh_identity_is_recorded_before_use_and_cache_reuses_its_provenance() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a","token-b"]}]"#,
            b"[]",
        ));
        let (_dir, path, sink, resolver) = boot_resolver(fetcher.clone());
        let token = PolymarketTokenId("token-b".to_owned());

        let first = resolver.resolve([token.clone()]).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.verified[&token].condition_id.0, "condition-a");
        assert_eq!(first.verified[&token].outcome.0, 1);
        assert_eq!(first.provenance[&token].source_log_sequence, 0);
        assert_eq!(
            first.provenance[&token].canonical_page_hash,
            first.verified[&token].evidence_hash
        );

        let second = resolver.resolve([token.clone()]).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(second.provenance, first.provenance);

        drop(resolver);
        drop(sink);
        let entries = Reader::replay(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1);
        let (sequence, envelope) = &entries[0];
        assert_eq!(sequence.0, first.provenance[&token].source_log_sequence);
        assert_eq!(envelope.source_id.0, GAMMA_MARKETS_SOURCE_ID);
        assert_eq!(envelope.schema_version, GAMMA_MARKETS_SCHEMA_VERSION);
        assert_eq!(envelope.parser_version, GAMMA_MARKETS_PARSER_VERSION);
    }

    #[tokio::test]
    async fn closed_lookup_records_both_pages_and_uses_the_proving_page() {
        let fetcher = Arc::new(GammaFixture::new(
            b"[]",
            br#"[{"conditionId":"condition-closed","clobTokenIds":["token-a"]}]"#,
        ));
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let token = PolymarketTokenId("token-a".to_owned());

        let resolved = resolver.resolve([token.clone()]).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
        assert_eq!(resolved.provenance[&token].source_log_sequence, 1);
        let urls = fetcher.urls.lock().unwrap();
        assert!(!urls[0].contains("closed=true"));
        assert!(urls[1].contains("closed=true"));
    }

    #[tokio::test]
    async fn chunk_boundary_coalesces_one_market_and_verifies_both_tokens() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a","token-b"]}]"#,
            b"[]",
        ));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let sink = Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap()));
        let resolver =
            AssetIdentityResolver::new(fetcher.clone(), BASE.to_owned(), 1, Arc::clone(&sink));
        let token_a = PolymarketTokenId("token-a".to_owned());
        let token_b = PolymarketTokenId("token-b".to_owned());

        let resolved = resolver
            .resolve([token_a.clone(), token_b.clone()])
            .await
            .unwrap();

        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
        assert_eq!(resolved.verified[&token_a].condition_id.0, "condition-a");
        assert_eq!(resolved.verified[&token_a].outcome.0, 0);
        assert_eq!(resolved.verified[&token_b].condition_id.0, "condition-a");
        assert_eq!(resolved.verified[&token_b].outcome.0, 1);
        assert_eq!(resolved.provenance[&token_a].source_log_sequence, 0);
        assert_eq!(resolved.provenance[&token_b].source_log_sequence, 0);
    }

    #[tokio::test]
    async fn successful_chunk_is_recorded_before_later_transient_and_no_identity_is_cached() {
        let fetcher = Arc::new(MixedChunkFixture {
            calls: AtomicUsize::new(0),
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let sink = Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap()));
        let resolver =
            AssetIdentityResolver::new(fetcher.clone(), BASE.to_owned(), 1, Arc::clone(&sink));
        let token_a = PolymarketTokenId("token-a".to_owned());
        let token_b = PolymarketTokenId("token-b".to_owned());

        assert!(matches!(
            resolver.resolve([token_a.clone(), token_b]).await,
            Err(SourceError::Transient { message })
                if message.contains("injected second chunk failure")
        ));
        let entries_after_failure = Reader::replay(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries_after_failure.len(), 1);

        let resolved = resolver.resolve([token_a.clone()]).await.unwrap();
        assert_eq!(resolved.verified[&token_a].condition_id.0, "condition-a");
        assert_eq!(
            fetcher.calls.load(Ordering::SeqCst),
            3,
            "the failed multi-chunk lookup must not cache its successful sibling"
        );
    }

    #[tokio::test]
    async fn malformed_identity_stays_unverified() {
        let mut overflowing = (0..=usize::from(u16::MAX))
            .map(|index| format!("other-{index}"))
            .collect::<Vec<_>>();
        overflowing.push("token-a".to_owned());
        let variants = [
            serde_json::json!([
                {"conditionId":"condition-a","clobTokenIds":["token-a","token-a"]}
            ]),
            serde_json::json!([
                {"conditionId":"condition-a","clobTokenIds":["token-a"]},
                {"conditionId":"condition-b","clobTokenIds":["token-a"]}
            ]),
            serde_json::json!([
                {"conditionId":"condition-overflow","clobTokenIds":overflowing}
            ]),
            serde_json::json!([]),
        ];
        for variant in variants {
            let page = serde_json::to_vec(&variant).unwrap();
            let fetcher = Arc::new(GammaFixture::new(&page, &page));
            let (_dir, _path, _sink, resolver) = boot_resolver(fetcher);
            let token = PolymarketTokenId("token-a".to_owned());

            let resolved = resolver.resolve([token.clone()]).await.unwrap();
            assert!(!resolved.verified.contains_key(&token));
            assert!(resolved.unverified.contains_key(&token));
        }
    }

    #[tokio::test]
    async fn malformed_successful_pages_are_recorded_before_parse_error_and_not_cached() {
        for raw in [b"not-json".as_slice(), br#"{"not":"an array"}"#] {
            let fetcher = Arc::new(GammaFixture::new(raw, b"[]"));
            let (_dir, path, sink, resolver) = boot_resolver(fetcher);
            let token = PolymarketTokenId("token-a".to_owned());

            assert!(matches!(
                resolver.resolve([token]).await,
                Err(SourceError::Fatal { message })
                    if message.starts_with("gamma metadata parse failed:")
            ));
            assert!(resolver.cache.read().await.is_empty());

            drop(resolver);
            drop(sink);
            let entries = Reader::replay(&path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].1.source_id.0, GAMMA_MARKETS_SOURCE_ID);
            assert_eq!(entries[0].1.schema_version, GAMMA_MARKETS_SCHEMA_VERSION);
            assert_eq!(entries[0].1.parser_version, GAMMA_MARKETS_PARSER_VERSION);
            assert_eq!(entries[0].1.payload, raw);
        }
    }

    #[tokio::test]
    async fn boot_append_failure_aborts_before_identity_is_returned() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let (_dir, _path, sink, resolver) = boot_resolver(fetcher);
        sink.lock().await.fail_next_append();

        assert!(matches!(
            resolver
                .resolve([PolymarketTokenId("token-a".to_owned())])
                .await,
            Err(SourceError::Fatal { message }) if message.contains("source-log append failed")
        ));
    }

    #[tokio::test]
    async fn runtime_coordinator_closure_is_fatal() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let (source_log, source_rx) = SourceLogHandle::channel(1);
        drop(source_rx);
        let resolver = AssetIdentityResolver::new_runtime(fetcher, BASE.to_owned(), 50, source_log);

        assert!(matches!(
            resolver
                .resolve([PolymarketTokenId("token-a".to_owned())])
                .await,
            Err(SourceError::Fatal { message }) if message == "source-log coordinator closed"
        ));
    }
}
