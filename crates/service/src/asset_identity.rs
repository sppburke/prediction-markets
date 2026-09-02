//! Process-lifetime Polymarket token identity authority (#555).
//!
//! Fresh Gamma pages become usable only after the service's single source-log
//! owner acknowledges their durable append. Verified identities retain that
//! immutable provenance across cache hits.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Weak};

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

/// One call's verified/unverified results and only the fresh pages fetched by
/// that call. `provenance` includes cache hits as well as fresh identities.
pub struct ResolvedIdentities {
    pub verified: BTreeMap<PolymarketTokenId, VerifiedTokenIdentity>,
    pub unverified: BTreeMap<PolymarketTokenId, String>,
    pub pages: Vec<(MetadataPageEvidence, Vec<u8>)>,
    pub provenance: BTreeMap<PolymarketTokenId, IdentityProvenance>,
}

pub struct AssetIdentityResolver {
    client: GammaMarketsClient<ReconciliationPageFetcher>,
    cache: RwLock<HashMap<PolymarketTokenId, CachedIdentity>>,
    recorder: RwLock<IdentityRecorder>,
    /// Per-token single-flight locks let unrelated brackets resolve in
    /// parallel while a shared miss becomes one recorded fetch plus cache hits.
    resolve_locks: std::sync::Mutex<HashMap<PolymarketTokenId, Weak<Mutex<()>>>>,
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
        Self {
            client: GammaMarketsClient::new(gamma_base_url, ReconciliationPageFetcher(fetcher))
                .with_batch_size(gamma_batch_size),
            cache: RwLock::new(HashMap::new()),
            recorder: RwLock::new(recorder),
            resolve_locks: std::sync::Mutex::new(HashMap::new()),
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
        let mut misses = requested
            .iter()
            .filter(|token| !resolved.verified.contains_key(*token))
            .cloned()
            .collect::<Vec<_>>();
        if misses.is_empty() {
            return Ok(resolved);
        }

        let locks = self.token_locks(&misses);
        let mut _guards = Vec::with_capacity(locks.len());
        for lock in locks {
            _guards.push(lock.lock_owned().await);
        }
        resolved = self.cached(&requested).await;
        misses = requested
            .iter()
            .filter(|token| !resolved.verified.contains_key(*token))
            .cloned()
            .collect();
        if misses.is_empty() {
            return Ok(resolved);
        }

        let mut fresh = Vec::new();
        let mut newly_verified = BTreeMap::new();
        let open = self
            .client
            .fetch_markets_by_token_ids(
                &misses
                    .iter()
                    .map(|token| token.0.clone())
                    .collect::<Vec<_>>(),
                MarketFilter::OpenOnly,
            )
            .await
            .map_err(map_gamma_error)?;
        let open_sequences = self.record_pages(&open.raw_pages).await?;
        fresh.extend(open.raw_pages.clone());
        if !open.markets.unfetched.is_empty() {
            return Err(SourceError::Fatal {
                message: format!(
                    "gamma token lookup rejected: {}",
                    open.markets.unfetched.join(",")
                ),
            });
        }
        let open_identities = verify_token_identities(&misses, &open);
        let leftovers = misses
            .iter()
            .filter(|token| !open_identities.contains_key(*token))
            .cloned()
            .collect::<Vec<_>>();
        collect_verified(
            open_identities,
            &open_sequences,
            &mut newly_verified,
            &mut resolved.unverified,
        );

        if !leftovers.is_empty() {
            let closed = self
                .client
                .fetch_markets_by_token_ids(
                    &leftovers
                        .iter()
                        .map(|token| token.0.clone())
                        .collect::<Vec<_>>(),
                    MarketFilter::ClosedOnly,
                )
                .await
                .map_err(map_gamma_error)?;
            let closed_sequences = self.record_pages(&closed.raw_pages).await?;
            fresh.extend(closed.raw_pages.clone());
            if !closed.markets.unfetched.is_empty() {
                return Err(SourceError::Fatal {
                    message: format!(
                        "gamma closed-token lookup rejected: {}",
                        closed.markets.unfetched.join(",")
                    ),
                });
            }
            let closed_identities = verify_token_identities(&leftovers, &closed);
            collect_verified(
                closed_identities,
                &closed_sequences,
                &mut newly_verified,
                &mut resolved.unverified,
            );
        }

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
        resolved.pages = fresh;
        Ok(resolved)
    }

    fn token_locks(&self, tokens: &[PolymarketTokenId]) -> Vec<Arc<Mutex<()>>> {
        let mut by_token = self
            .resolve_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        by_token.retain(|_, lock| lock.strong_count() > 0);
        tokens
            .iter()
            .map(|token| {
                if let Some(lock) = by_token.get(token).and_then(Weak::upgrade) {
                    lock
                } else {
                    let lock = Arc::new(Mutex::new(()));
                    by_token.insert(token.clone(), Arc::downgrade(&lock));
                    lock
                }
            })
            .collect()
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
            pages: Vec::new(),
            provenance,
        }
    }

    async fn record_pages(
        &self,
        pages: &[(MetadataPageEvidence, Vec<u8>)],
    ) -> Result<HashMap<String, u64>, SourceError> {
        let recorder = self.recorder.read().await.clone();
        let mut sequences = HashMap::new();
        for (evidence, payload) in pages {
            let envelope = EnvelopeIn {
                source_id: evidence.source_id.clone(),
                schema_version: evidence.schema_version,
                parser_version: evidence.parser_version,
                observed_at: SourceTimestamp(evidence.received_at.0),
                received_at: evidence.received_at.clone(),
                content_type: ContentType::Json,
                payload: payload.clone(),
            };
            let sequence = match &recorder {
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
            sequences
                .entry(evidence.canonical_page_hash.clone())
                .or_insert(sequence.0);
        }
        Ok(sequences)
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

    struct ParallelGammaFixture {
        barrier: tokio::sync::Barrier,
        calls: AtomicUsize,
    }

    impl ReconciliationFetcher for ParallelGammaFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.barrier.wait().await;
                let token = if url.contains("token-a") {
                    "token-a"
                } else {
                    "token-b"
                };
                Ok(serde_json::to_vec(&vec![serde_json::json!({
                    "conditionId": format!("condition-{token}"),
                    "clobTokenIds": [token]
                })])
                .unwrap())
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
        assert_eq!(first.pages.len(), 1);
        assert_eq!(first.verified[&token].condition_id.0, "condition-a");
        assert_eq!(first.verified[&token].outcome.0, 1);
        assert_eq!(first.provenance[&token].source_log_sequence, 0);
        assert_eq!(
            first.provenance[&token].canonical_page_hash,
            first.verified[&token].evidence_hash
        );

        let second = resolver.resolve([token.clone()]).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert!(second.pages.is_empty());
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
    async fn concurrent_resolves_share_one_fresh_page_and_recorded_provenance() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let resolver = Arc::new(resolver);
        let token = PolymarketTokenId("token-a".to_owned());
        let (left, right) = tokio::join!(
            resolver.resolve([token.clone()]),
            resolver.resolve([token.clone()])
        );
        let left = left.unwrap();
        let right = right.unwrap();

        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(left.provenance, right.provenance);
        assert_eq!(
            usize::from(left.pages.is_empty()) + usize::from(right.pages.is_empty()),
            1
        );
    }

    #[tokio::test]
    async fn unrelated_token_misses_resolve_concurrently() {
        let fetcher = Arc::new(ParallelGammaFixture {
            barrier: tokio::sync::Barrier::new(2),
            calls: AtomicUsize::new(0),
        });
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let resolver = Arc::new(resolver);
        let token_a = PolymarketTokenId("token-a".to_owned());
        let token_b = PolymarketTokenId("token-b".to_owned());
        let (left, right) = tokio::join!(
            resolver.resolve([token_a.clone()]),
            resolver.resolve([token_b.clone()])
        );

        assert_eq!(
            left.unwrap().verified[&token_a].condition_id.0,
            "condition-token-a"
        );
        assert_eq!(
            right.unwrap().verified[&token_b].condition_id.0,
            "condition-token-b"
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
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
        assert_eq!(resolved.pages.len(), 2);
        assert_eq!(resolved.provenance[&token].source_log_sequence, 1);
        let urls = fetcher.urls.lock().unwrap();
        assert!(!urls[0].contains("closed=true"));
        assert!(urls[1].contains("closed=true"));
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
