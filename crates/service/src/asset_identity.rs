//! Polymarket token identity authority with generation-bound durable caching (#555).
//!
//! Fresh Gamma pages become usable only after the service's single source-log
//! owner acknowledges their durable append. Verified identities retain that
//! immutable provenance across cache hits.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Weak};

use pe_core_types::{
    EventSeq, OutcomeId, PolymarketConditionId, PolymarketTokenId, SourceTimestamp,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn};
use pe_paper_state::{
    ASSET_IDENTITY_INSERT_LIMIT, AssetIdentityRow, MigrationPhase, MigrationRecord, PaperStateDb,
    PaperStateError,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::gamma_markets::verify_token_identities;
use pe_source_polymarket_public::{
    GAMMA_BATCH_SIZE, GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION,
    GAMMA_MARKETS_SOURCE_ID, GammaMarketsClient, GammaMarketsError, MarketFilter,
    MetadataPageEvidence, ReconciliationFetcher, ReconciliationPageFetcher, VerifiedTokenIdentity,
    canonical_page_hash,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::activity_ingest::{SourceLogHandle, SourceLogHandleError};
use crate::risk_inputs::SourceReceiptIndex;
use crate::source_event_sink::SourceEventSink;

type IdentityPage = (MetadataPageEvidence, Vec<u8>);

#[derive(Clone, Default)]
struct RecordedIdentityPages {
    pages: Vec<IdentityPage>,
    sequences: HashMap<String, u64>,
    last_sequence: Option<u64>,
    absences: BTreeMap<PolymarketTokenId, i64>,
}

#[derive(Default)]
struct MemoryIdentityPages {
    recorded: RecordedIdentityPages,
    by_token: BTreeMap<PolymarketTokenId, BTreeSet<usize>>,
    by_condition: BTreeMap<String, BTreeSet<usize>>,
}

impl MemoryIdentityPages {
    fn insert(&mut self, recorded: &RecordedIdentityPages) -> Result<(), SourceError> {
        for page in &recorded.pages {
            let hash = &page.0.canonical_page_hash;
            if self.recorded.sequences.contains_key(hash) {
                continue;
            }
            let sequence = recorded
                .sequences
                .get(hash)
                .ok_or_else(|| identity_store_error("memory-only page has no recorded sequence"))?;
            let index = self.recorded.pages.len();
            for (token, conditions) in page_token_conditions(std::slice::from_ref(page)) {
                self.by_token.entry(token).or_default().insert(index);
                for condition in conditions {
                    self.by_condition
                        .entry(condition)
                        .or_default()
                        .insert(index);
                }
            }
            self.recorded.pages.push(page.clone());
            self.recorded.sequences.insert(hash.clone(), *sequence);
            self.recorded.last_sequence = self.recorded.last_sequence.max(Some(*sequence));
        }
        Ok(())
    }

    fn intersecting(
        &self,
        token_conditions: &BTreeMap<PolymarketTokenId, BTreeSet<String>>,
    ) -> Vec<IdentityPage> {
        let mut indices = BTreeSet::<usize>::new();
        for (token, conditions) in token_conditions {
            if let Some(pages) = self.by_token.get(token) {
                indices.extend(pages);
            }
            for condition in conditions {
                if let Some(pages) = self.by_condition.get(condition) {
                    indices.extend(pages);
                }
            }
        }
        indices
            .into_iter()
            .filter_map(|index| self.recorded.pages.get(index).cloned())
            .collect()
    }
}

struct IdentityChunk {
    requested: BTreeSet<PolymarketTokenId>,
    filter: MarketFilter,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LookupPurpose {
    Historical,
    Live,
}

struct AuthenticatedIdentityRows {
    identities: Vec<(AssetIdentityRow, CachedIdentity)>,
    pages: Vec<IdentityPage>,
    invalid: Vec<PolymarketTokenId>,
}

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

#[derive(Default)]
struct IdentityCache {
    identities: HashMap<PolymarketTokenId, CachedIdentity>,
    /// Completed condition lookups, including empty results, last for this resolver's process
    /// lifetime. Refetching each bracket read would put Gamma requests inside the stability
    /// window and re-record pages (R2 observed 78 wallets across 7,154 conditions).
    discovered_conditions: HashMap<String, BTreeSet<PolymarketTokenId>>,
    rejected: BTreeSet<String>,
    rejected_tokens: BTreeSet<PolymarketTokenId>,
    memory_pages: MemoryIdentityPages,
    // An expired caller cannot make a later live lookup skip the open query.
    completed_open: HashMap<PolymarketTokenId, Weak<()>>,
    // Unfinished condition discoveries share open progress only while their caller lives.
    completed_open_conditions: HashMap<String, Weak<()>>,
    // Boot's shared receipt index is extended only after its reducer walk. These pages
    // already have a synchronized append receipt and are dropped when that index is installed.
    boot_pages: BTreeMap<u64, (AppendReceipt, IdentityPage)>,
}

#[derive(Clone)]
struct IdentityStore {
    paper_state: Arc<PaperStateDb>,
    generation: String,
    source_receipts: SourceReceiptIndex,
}

/// Bind cache rows to the installed activation tails, never to a growing current log tail.
pub fn installed_identity_generation(
    record: &MigrationRecord,
) -> Result<Option<String>, SourceError> {
    if record.phase != MigrationPhase::Installed {
        return Ok(None);
    }
    let activation = record
        .activation_tails
        .as_ref()
        .ok_or_else(|| SourceError::Fatal {
            message: "installed migration metadata omitted activation tails".to_owned(),
        })?;
    let tails = [
        ("source", &activation.source),
        ("paper", &activation.paper),
        ("live_journal", &activation.live_journal),
    ]
    .map(|(name, tail)| {
        (
            name,
            serde_json::json!({
                "path": tail.path,
                "physical_tail": tail.physical_tail,
                "last_sequence": tail.last_sequence.map(|sequence| sequence.0),
                "last_hash": tail.last_hash.to_hex().to_string(),
            }),
        )
    })
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&tails)
        .map(Some)
        .map_err(identity_store_error)
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
    cache: RwLock<IdentityCache>,
    misses: Mutex<()>,
    recorder: RwLock<IdentityRecorder>,
    store: RwLock<Option<IdentityStore>>,
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
            cache: RwLock::new(IdentityCache::default()),
            misses: Mutex::new(()),
            recorder: RwLock::new(recorder),
            store: RwLock::new(None),
        }
    }

    #[must_use]
    pub fn with_paper_state(
        self,
        paper_state: Arc<PaperStateDb>,
        generation: String,
        source_receipts: SourceReceiptIndex,
    ) -> Self {
        Self {
            store: RwLock::new(Some(IdentityStore {
                paper_state,
                generation,
                source_receipts,
            })),
            ..self
        }
    }

    /// Attach the installed main after first-migration boot releases its side-main handle.
    pub async fn install_paper_state(
        &self,
        paper_state: Arc<PaperStateDb>,
        generation: String,
        source_receipts: SourceReceiptIndex,
    ) -> Result<(), SourceError> {
        let _misses = self.misses.lock().await;
        let store = IdentityStore {
            paper_state,
            generation,
            source_receipts,
        };
        let recorded = self.cache.read().await.memory_pages.recorded.clone();
        if !recorded.pages.is_empty() {
            self.save_verified(
                Some(&store),
                &recorded,
                &mut BTreeMap::new(),
                &mut BTreeMap::new(),
                &mut ResolvedIdentities {
                    verified: BTreeMap::new(),
                    unverified: BTreeMap::new(),
                    provenance: BTreeMap::new(),
                },
            )
            .await?;
        }
        *self.store.write().await = Some(store);
        self.cache.write().await.memory_pages = MemoryIdentityPages::default();
        self.release_indexed_boot_pages().await;
        Ok(())
    }

    /// Switch from the boot writer to the runtime coordinator before the
    /// latter opens the same source-log path.
    pub async fn activate_runtime(&self, source_log: SourceLogHandle) {
        *self.recorder.write().await = IdentityRecorder::Runtime(source_log);
    }

    /// Live decisions ignore completed absences so newly listed tokens can be discovered.
    pub async fn resolve_live(
        &self,
        tokens: impl IntoIterator<Item = PolymarketTokenId>,
    ) -> Result<ResolvedIdentities, SourceError> {
        self.resolve_inner(tokens, LookupPurpose::Live).await
    }

    /// Historical brackets reuse completed absences and retry exhausted transient chunks.
    pub async fn resolve_historical_for_bracket(
        &self,
        tokens: impl IntoIterator<Item = PolymarketTokenId>,
    ) -> Result<ResolvedIdentities, SourceError> {
        self.resolve_inner(tokens, LookupPurpose::Historical).await
    }

    /// Discover activity-backed conditions once per process lifetime, retaining empty results.
    /// Later reads reuse token-cache identities and original provenance, subject to rejections;
    /// refetching would put Gamma requests inside the stability window and re-record pages.
    pub async fn discover_conditions_for_bracket(
        &self,
        conditions: impl IntoIterator<Item = PolymarketConditionId>,
    ) -> Result<ResolvedIdentities, SourceError> {
        let requested = conditions
            .into_iter()
            .map(|condition| condition.0)
            .collect::<BTreeSet<_>>();
        let mut resolved = ResolvedIdentities {
            verified: BTreeMap::new(),
            unverified: BTreeMap::new(),
            provenance: BTreeMap::new(),
        };
        if requested.is_empty() {
            return Ok(resolved);
        }
        let mut read_pages = BTreeMap::new();
        let conditions = requested.iter().cloned().collect::<Vec<_>>();
        let open_phase = Arc::new(());
        for chunk in conditions.chunks(self.gamma_batch_size.min(GAMMA_BATCH_SIZE)) {
            let mut pending = chunk.to_vec();
            let mut recorded = RecordedIdentityPages::default();
            for filter in [MarketFilter::OpenOnly, MarketFilter::ClosedOnly] {
                // One bounded attempt owns the FIFO mutex, just like the token path.
                let _misses = self.misses.lock().await;
                let store = self.store.read().await.clone();
                let mut misses = Vec::new();
                {
                    let mut cache = self.cache.write().await;
                    for condition in &pending {
                        let Some(tokens) = cache.discovered_conditions.get(condition).cloned()
                        else {
                            misses.push(condition.clone());
                            continue;
                        };
                        if let Some(store) = &store {
                            check_condition_rejection(store, &mut cache, condition)?;
                            cache.rejected_tokens.extend(
                                store
                                    .paper_state
                                    .rejected_asset_tokens(
                                        &store.generation,
                                        &tokens.iter().cloned().collect::<Vec<_>>(),
                                    )
                                    .map_err(identity_store_error)?
                                    .into_keys(),
                            );
                        }
                        for token in tokens {
                            if cache.rejected.contains(condition)
                                || cache.rejected_tokens.contains(&token)
                            {
                                resolved
                                    .unverified
                                    .insert(token, rejected_identity_reason());
                            } else if let Some(cached) = cache.identities.get(&token) {
                                resolved
                                    .verified
                                    .insert(token.clone(), cached.identity.clone());
                                resolved.provenance.insert(token, cached.provenance.clone());
                            } else {
                                resolved.unverified.insert(token, absent_identity_reason());
                            }
                        }
                    }
                    evict_rejected(&mut cache, &mut resolved);
                }
                pending = misses;
                if pending.is_empty() {
                    break;
                }
                let mut fetch_conditions = pending.clone();
                if filter == MarketFilter::OpenOnly {
                    let cache = self.cache.read().await;
                    fetch_conditions.retain(|condition| {
                        cache
                            .completed_open_conditions
                            .get(condition)
                            .is_none_or(|phase| phase.strong_count() == 0)
                    });
                } else {
                    let mut cache = self.cache.write().await;
                    for condition in &fetch_conditions {
                        cache.completed_open_conditions.remove(condition);
                    }
                }
                if fetch_conditions.is_empty() {
                    continue;
                }
                let (pages, fetched) = match self
                    .client
                    .fetch_markets_with_pages(&fetch_conditions, filter)
                    .await
                {
                    Ok(fetched) => {
                        let result = if fetched.markets.unfetched.is_empty() {
                            Ok(())
                        } else {
                            Err(SourceError::Fatal {
                                message: format!(
                                    "gamma condition lookup rejected: {}",
                                    fetched.markets.unfetched.join(",")
                                ),
                            })
                        };
                        (fetched.pages, result)
                    }
                    Err(error) => (error.pages, Err(map_gamma_error(error.source))),
                };
                for page in pages {
                    let sequence = self.record_page(&page).await?;
                    recorded.last_sequence = Some(sequence);
                    recorded
                        .sequences
                        .entry(page.0.canonical_page_hash.clone())
                        .or_insert(sequence);
                    recorded.pages.push(page);
                }
                let tokens = page_token_conditions(&recorded.pages)
                    .into_iter()
                    .filter(|(_, conditions)| conditions.iter().any(|id| requested.contains(id)))
                    .map(|(token, _)| token)
                    .collect::<Vec<_>>();
                let mut newly_verified = BTreeMap::new();
                collect_verified(
                    verify_token_identities(&tokens, &recorded.pages),
                    &recorded.sequences,
                    &mut newly_verified,
                    &mut resolved.unverified,
                );
                let saved = self
                    .save_verified(
                        store.as_ref(),
                        &recorded,
                        &mut read_pages,
                        &mut newly_verified,
                        &mut resolved,
                    )
                    .await;
                if let Err(error) = fetched {
                    if let Err(save_error) = saved {
                        tracing::error!(%save_error, "failed to save partial Gamma condition identity progress");
                    }
                    return Err(error);
                }
                saved?;
                for (token, fresh) in newly_verified {
                    if requested.contains(&fresh.identity.condition_id.0) {
                        resolved.unverified.remove(&token);
                        resolved.verified.insert(token.clone(), fresh.identity);
                        resolved.provenance.insert(token, fresh.provenance);
                    }
                }
                let mut cache = self.cache.write().await;
                evict_rejected(&mut cache, &mut resolved);
                for condition in &pending {
                    let tokens = resolved
                        .verified
                        .iter()
                        .filter(|(_, identity)| &identity.condition_id.0 == condition)
                        .map(|(token, _)| token.clone())
                        .collect::<BTreeSet<_>>();
                    if filter == MarketFilter::ClosedOnly || !tokens.is_empty() {
                        cache
                            .discovered_conditions
                            .insert(condition.clone(), tokens);
                    } else if fetch_conditions.contains(condition) {
                        cache
                            .completed_open_conditions
                            .insert(condition.clone(), Arc::downgrade(&open_phase));
                    }
                }
                pending.retain(|condition| {
                    !resolved
                        .verified
                        .values()
                        .any(|identity| &identity.condition_id.0 == condition)
                });
                if pending.is_empty() {
                    break;
                }
            }
        }
        evict_rejected(&mut *self.cache.write().await, &mut resolved);
        Ok(resolved)
    }

    async fn resolve_inner(
        &self,
        tokens: impl IntoIterator<Item = PolymarketTokenId>,
        purpose: LookupPurpose,
    ) -> Result<ResolvedIdentities, SourceError> {
        let requested = tokens.into_iter().collect::<BTreeSet<_>>();
        {
            let cache = self.cache.read().await;
            if requested.iter().all(|token| {
                cache.identities.get(token).is_some_and(|cached| {
                    !cache.rejected_tokens.contains(token)
                        && !cache.rejected.contains(&cached.identity.condition_id.0)
                })
            }) {
                let identities = requested
                    .iter()
                    .filter_map(|token| cache.identities.get(token));
                return Ok(ResolvedIdentities {
                    verified: identities
                        .clone()
                        .map(|cached| (cached.provenance.asset.clone(), cached.identity.clone()))
                        .collect(),
                    provenance: identities
                        .map(|cached| (cached.provenance.asset.clone(), cached.provenance.clone()))
                        .collect(),
                    unverified: BTreeMap::new(),
                });
            }
        }
        let mut read_pages = BTreeMap::new();
        let mut resolved = self.cached(&requested, None, &mut read_pages).await?;
        let misses = requested
            .iter()
            .filter(|token| {
                !resolved.verified.contains_key(*token) && !resolved.unverified.contains_key(*token)
            })
            .cloned()
            .collect::<Vec<_>>();
        let open_phase = Arc::new(());
        for chunk in misses.chunks(self.gamma_batch_size.min(GAMMA_BATCH_SIZE)) {
            let mut chunk = IdentityChunk {
                requested: chunk.iter().cloned().collect(),
                filter: MarketFilter::OpenOnly,
            };
            let retry_delays: &[u64] = if purpose == LookupPurpose::Historical {
                &[2, 4, 8, 16]
            } else {
                &[]
            };
            let mut delays = retry_delays.iter();
            loop {
                let attempt = {
                    let _misses = self.misses.lock().await;
                    let store = self.store.read().await.clone();
                    let cached = self
                        .cached(&chunk.requested, store.as_ref(), &mut read_pages)
                        .await?;
                    for token in &chunk.requested {
                        resolved.verified.remove(token);
                        resolved.unverified.remove(token);
                        resolved.provenance.remove(token);
                    }
                    resolved.verified.extend(cached.verified);
                    resolved.unverified.extend(cached.unverified);
                    resolved.provenance.extend(cached.provenance);
                    self.resolve_chunk(
                        &mut chunk,
                        purpose,
                        store.as_ref(),
                        &mut read_pages,
                        &open_phase,
                        &mut resolved,
                    )
                    .await
                };
                match attempt {
                    Ok(true) => break,
                    Ok(false) => delays = retry_delays.iter(),
                    Err(error) => {
                        if matches!(error, SourceError::Transient { .. })
                            && let Some(delay) = delays.next()
                        {
                            tokio::time::sleep(std::time::Duration::from_secs(*delay)).await;
                            continue;
                        }
                        return Err(error);
                    }
                }
            }
        }
        // Another request can reject an earlier chunk's condition between chunks.
        evict_rejected(&mut *self.cache.write().await, &mut resolved);
        Ok(resolved)
    }

    async fn resolve_chunk(
        &self,
        chunk: &mut IdentityChunk,
        purpose: LookupPurpose,
        store: Option<&IdentityStore>,
        read_pages: &mut BTreeMap<u64, IdentityPage>,
        open_phase: &Arc<()>,
        resolved: &mut ResolvedIdentities,
    ) -> Result<bool, SourceError> {
        let requested = &chunk.requested;
        if let Some(store) = store {
            let misses = requested
                .iter()
                .filter(|token| {
                    !resolved.verified.contains_key(*token)
                        && !resolved.unverified.contains_key(*token)
                })
                .cloned()
                .collect::<Vec<_>>();
            for token in store
                .paper_state
                .rejected_asset_tokens(&store.generation, &misses)
                .map_err(identity_store_error)?
                .into_keys()
            {
                resolved
                    .unverified
                    .insert(token, rejected_identity_reason());
            }
            if purpose == LookupPurpose::Historical {
                for token in store
                    .paper_state
                    .absent_asset_tokens(&store.generation, &misses)
                    .map_err(identity_store_error)?
                    .into_keys()
                {
                    resolved
                        .unverified
                        .entry(token)
                        .or_insert_with(absent_identity_reason);
                }
            }
        }
        let misses = requested
            .iter()
            .filter(|token| {
                !resolved.verified.contains_key(*token) && !resolved.unverified.contains_key(*token)
            })
            .cloned()
            .collect::<Vec<_>>();
        if misses.is_empty() {
            return Ok(true);
        }

        let mut fetch_tokens = misses.clone();
        if chunk.filter == MarketFilter::OpenOnly {
            let cache = self.cache.read().await;
            fetch_tokens.retain(|token| {
                cache
                    .completed_open
                    .get(token)
                    .is_none_or(|phase| phase.strong_count() == 0)
            });
            if fetch_tokens.is_empty() {
                chunk.filter = MarketFilter::ClosedOnly;
                fetch_tokens = misses.clone();
            }
        }
        if chunk.filter == MarketFilter::ClosedOnly {
            let mut cache = self.cache.write().await;
            for token in &fetch_tokens {
                cache.completed_open.remove(token);
            }
        }

        let mut recorded = RecordedIdentityPages::default();
        let mut newly_verified = BTreeMap::new();
        let fetched = self
            .fetch_and_record_chunks(
                &fetch_tokens,
                chunk.filter,
                match chunk.filter {
                    MarketFilter::OpenOnly => "gamma token lookup rejected",
                    MarketFilter::ClosedOnly => "gamma closed-token lookup rejected",
                },
                &mut recorded,
            )
            .await;
        let identities = verify_token_identities(&misses, &recorded.pages);
        let leftovers = misses
            .iter()
            .filter(|token| !identities.contains_key(*token))
            .cloned()
            .collect::<Vec<_>>();
        let complete = chunk.filter == MarketFilter::ClosedOnly || leftovers.is_empty();

        let returned_tokens = page_tokens(&recorded.pages)
            .into_iter()
            .collect::<BTreeSet<_>>();
        if fetched.is_ok()
            && complete
            && store.is_some()
            && let Some(sequence) = recorded.last_sequence
        {
            let sequence = i64::try_from(sequence).map_err(identity_store_error)?;
            recorded.absences = misses
                .iter()
                .filter(|token| !returned_tokens.contains(token))
                .map(|token| (token.clone(), sequence))
                .collect();
        }
        let candidates: Vec<_> = if store.is_some() {
            returned_tokens.into_iter().collect()
        } else {
            resolved
                .verified
                .keys()
                .chain(resolved.unverified.keys())
                .chain(misses.iter())
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        collect_verified(
            verify_token_identities(&candidates, &recorded.pages),
            &recorded.sequences,
            &mut newly_verified,
            &mut resolved.unverified,
        );
        if store.is_none() {
            let mut cache = self.cache.write().await;
            for token in &candidates {
                if resolved.unverified.contains_key(token) && !newly_verified.contains_key(token) {
                    cache.identities.remove(token);
                    resolved.verified.remove(token);
                    resolved.provenance.remove(token);
                }
            }
        }

        for token in misses {
            if !newly_verified.contains_key(&token) && !resolved.unverified.contains_key(&token) {
                resolved.unverified.insert(token, absent_identity_reason());
            }
        }
        let saved = self
            .save_verified(store, &recorded, read_pages, &mut newly_verified, resolved)
            .await;
        if let Err(error) = fetched {
            if let Err(save_error) = saved {
                tracing::error!(%save_error, "failed to save partial Gamma identity progress");
            }
            return Err(error);
        }
        saved?;
        if chunk.filter == MarketFilter::OpenOnly {
            let mut cache = self.cache.write().await;
            for token in &leftovers {
                if fetch_tokens.contains(token) {
                    cache
                        .completed_open
                        .insert(token.clone(), Arc::downgrade(open_phase));
                }
            }
        }
        for (token, cached) in newly_verified {
            if !requested.contains(&token) {
                continue;
            }
            resolved.unverified.remove(&token);
            resolved
                .verified
                .insert(token.clone(), cached.identity.clone());
            resolved.provenance.insert(token, cached.provenance);
        }
        if !complete {
            chunk.requested = leftovers.into_iter().collect();
            chunk.filter = MarketFilter::ClosedOnly;
        }
        Ok(complete)
    }

    /// Release synchronized temporary pages once the boot extension has indexed their receipts.
    pub async fn release_indexed_boot_pages(&self) {
        if let Some(store) = self.store.read().await.clone() {
            self.cache
                .write()
                .await
                .boot_pages
                .retain(|_, (receipt, _)| {
                    !matches!(store.source_receipts.receipt_at(receipt.sequence),
                    Ok(Some((indexed, _))) if indexed == *receipt)
                });
        }
    }

    async fn save_verified(
        &self,
        store: Option<&IdentityStore>,
        recorded: &RecordedIdentityPages,
        read_pages: &mut BTreeMap<u64, IdentityPage>,
        newly_verified: &mut BTreeMap<PolymarketTokenId, CachedIdentity>,
        resolved: &mut ResolvedIdentities,
    ) -> Result<(), SourceError> {
        let pages = &recorded.pages;
        let sequences = &recorded.sequences;
        let mut cache = self.cache.write().await;
        if let Some(store) = store {
            let tokens = page_tokens(pages);
            let token_set = tokens.iter().cloned().collect::<BTreeSet<_>>();
            cache.rejected_tokens.extend(
                store
                    .paper_state
                    .rejected_asset_tokens(&store.generation, &tokens)
                    .map_err(identity_store_error)?
                    .into_keys(),
            );
            // A rejected token without a row extends its conflict to fresh conditions.
            let mut rejections = BTreeMap::new();
            for page in pages {
                for (token, conditions) in page_token_conditions(std::slice::from_ref(page)) {
                    if !cache.rejected_tokens.contains(&token) {
                        continue;
                    }
                    let sequence = sequences.get(&page.0.canonical_page_hash).ok_or_else(|| {
                        identity_store_error("conflict has no recorded fresh page")
                    })?;
                    for condition in conditions {
                        rejections.insert(
                            condition,
                            i64::try_from(*sequence).map_err(identity_store_error)?,
                        );
                    }
                }
            }
            let mut witnesses = Vec::new();
            for page in pages {
                let mut identities = BTreeMap::new();
                collect_verified(
                    verify_token_identities(
                        &page_tokens(std::slice::from_ref(page)),
                        std::slice::from_ref(page),
                    ),
                    sequences,
                    &mut identities,
                    &mut BTreeMap::new(),
                );
                witnesses.extend(identities.into_values());
            }
            let conditions = page_token_conditions(pages)
                .into_values()
                .flatten()
                .collect::<BTreeSet<_>>();
            for condition in &conditions {
                check_condition_rejection(store, &mut cache, condition)?;
            }
            let mut rows = store
                .paper_state
                .asset_identities_by_condition(&store.generation, &conditions)
                .map_err(identity_store_error)?
                .into_iter()
                .map(|row| (row.token.clone(), row))
                .collect::<BTreeMap<_, _>>();
            rows.extend(
                store
                    .paper_state
                    .asset_identities(&store.generation, &tokens)
                    .map_err(identity_store_error)?
                    .into_iter()
                    .map(|row| (row.token.clone(), row)),
            );
            let authenticated = authenticate_identity_rows(
                store,
                rows.values().cloned().collect(),
                &cache.boot_pages,
                read_pages,
            );
            delete_invalid_identity_rows(store, &authenticated.invalid).await?;
            let new_rows = witnesses
                .iter()
                .filter(|cached| !rejections.contains_key(&cached.identity.condition_id.0))
                .map(|witness| {
                    // Handoff persists the provenance already acknowledged from memory.
                    if !cache.memory_pages.recorded.pages.is_empty()
                        && let Some(cached) = cache.identities.get(&witness.provenance.asset)
                        && cached.identity.condition_id == witness.identity.condition_id
                        && cached.identity.outcome == witness.identity.outcome
                    {
                        return identity_row(&witness.provenance.asset, cached);
                    }
                    identity_row(&witness.provenance.asset, witness)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let saved = authenticated.identities;
            let mut combined_pages = pages.to_vec();
            combined_pages.extend(authenticated.pages);
            witnesses.extend(saved.iter().map(|(_, cached)| cached.clone()));
            let combined_tokens = page_tokens(&combined_pages);
            let verified = verify_token_identities(&combined_tokens, &combined_pages);
            for cached in &witnesses {
                if !conditions.contains(&cached.identity.condition_id.0) {
                    check_condition_rejection(store, &mut cache, &cached.identity.condition_id.0)?;
                }
                if verified
                    .get(&cached.provenance.asset)
                    .is_some_and(Result::is_err)
                {
                    let sequence = witnesses
                        .iter()
                        .filter(|fresh| {
                            sequences.contains_key(&fresh.provenance.canonical_page_hash)
                                && (fresh.provenance.asset == cached.provenance.asset
                                    || fresh.identity.condition_id == cached.identity.condition_id)
                        })
                        .map(|fresh| fresh.provenance.source_log_sequence)
                        .max()
                        .or_else(|| sequences.values().copied().max())
                        .ok_or_else(|| {
                            identity_store_error("conflict has no recorded fresh page")
                        })?;
                    rejections.insert(
                        cached.identity.condition_id.0.clone(),
                        i64::try_from(sequence).map_err(identity_store_error)?,
                    );
                }
            }
            // A single page can contain conflicting markets with no individually usable
            // token row. Their conditions still need independent rejection markers.
            let token_conditions = page_token_conditions(&combined_pages);
            for (token, identity) in &verified {
                if !matches!(identity, Err(
                    pe_source_polymarket_public::MetadataIdentityError::MarketCardinality { .. }
                    | pe_source_polymarket_public::MetadataIdentityError::DuplicateIdentity { .. }
                )) {
                    continue;
                }
                if let Some(conditions) = token_conditions.get(token) {
                    for condition in conditions {
                        if rejections.contains_key(condition) {
                            continue;
                        }
                        let sequence = pages
                            .iter()
                            .filter(|page| {
                                page_token_conditions(std::slice::from_ref(page))
                                    .contains_key(token)
                            })
                            .filter_map(|page| sequences.get(&page.0.canonical_page_hash))
                            .max()
                            .ok_or_else(|| {
                                identity_store_error("conflict has no recorded fresh page")
                            })?;
                        rejections.insert(
                            condition.clone(),
                            i64::try_from(*sequence).map_err(identity_store_error)?,
                        );
                    }
                }
            }
            let rejected_tokens = token_conditions
                .iter()
                .filter(|(_, conditions)| {
                    conditions.iter().any(|condition| {
                        rejections.contains_key(condition) || cache.rejected.contains(condition)
                    })
                })
                .map(|(token, _)| {
                    recorded
                        .last_sequence
                        .ok_or_else(|| identity_store_error("rejected token has no recorded page"))
                        .and_then(|sequence| i64::try_from(sequence).map_err(identity_store_error))
                        .map(|sequence| (token.clone(), sequence))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            // Rejections accompany the first bounded row insert, even if all fresh tokens
            // already belong to another condition. Later row inserts cannot clear a marker.
            if new_rows.is_empty()
                && (!rejections.is_empty()
                    || !recorded.absences.is_empty()
                    || !rejected_tokens.is_empty())
            {
                identity_mutation(|| {
                    store.paper_state.save_asset_identities(
                        &store.generation,
                        &[],
                        &rejections,
                        &recorded.absences,
                        &rejected_tokens,
                    )
                })
                .await?;
            } else {
                for (index, chunk) in new_rows.chunks(ASSET_IDENTITY_INSERT_LIMIT).enumerate() {
                    let markers = if index == 0 {
                        &rejections
                    } else {
                        &BTreeMap::new()
                    };
                    let absences = if index == 0 {
                        &recorded.absences
                    } else {
                        &BTreeMap::new()
                    };
                    let rejected_tokens = if index == 0 {
                        &rejected_tokens
                    } else {
                        &BTreeMap::new()
                    };
                    identity_mutation(|| {
                        store.paper_state.save_asset_identities(
                            &store.generation,
                            chunk,
                            markers,
                            absences,
                            rejected_tokens,
                        )
                    })
                    .await?;
                    if index == 0 {
                        cache.rejected.extend(rejections.keys().cloned());
                        evict_rejected(&mut cache, resolved);
                    }
                }
            }
            cache.rejected.extend(rejections.into_keys());
            cache.rejected_tokens.extend(rejected_tokens.into_keys());
            // Preserve saved provenance when a fresh page repeats a known identity.
            for (row, cached) in saved {
                if !cache.rejected.contains(&row.condition_id.0) {
                    cache.identities.entry(row.token).or_insert(cached);
                }
            }
            let mut fresh = BTreeMap::new();
            collect_verified(verified, sequences, &mut fresh, &mut BTreeMap::new());
            fresh.retain(|token, _| token_set.contains(token));
            *newly_verified = fresh;
            for cached in witnesses {
                if cache.rejected.contains(&cached.identity.condition_id.0) {
                    let token = cached.provenance.asset.clone();
                    newly_verified.remove(&token);
                    resolved.verified.remove(&token);
                    resolved.provenance.remove(&token);
                    resolved
                        .unverified
                        .insert(token, rejected_identity_reason());
                }
            }
            let rejected = cache.rejected.clone();
            newly_verified.retain(|_, cached| !rejected.contains(&cached.identity.condition_id.0));
            for (token, fresh) in newly_verified.iter_mut() {
                if let Some(cached) = cache.identities.get(token) {
                    *fresh = cached.clone();
                }
            }
            evict_rejected(&mut cache, resolved);
        } else {
            let fresh_conditions = page_token_conditions(pages);
            let mut combined_pages = pages.to_vec();
            combined_pages.extend(cache.memory_pages.intersecting(&fresh_conditions));
            cache.memory_pages.insert(recorded)?;
            let token_conditions = page_token_conditions(&combined_pages);
            let mut candidates = newly_verified
                .keys()
                .chain(resolved.verified.keys())
                .chain(resolved.unverified.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            // First tokens witness differing outcome vectors; shared tokens witness cross-condition conflicts.
            for (_, tokens) in page_markets(&combined_pages) {
                if let Some(token) = tokens.into_iter().next() {
                    candidates.insert(PolymarketTokenId(token));
                }
            }
            candidates.extend(
                token_conditions.iter().filter_map(|(token, conditions)| {
                    (conditions.len() > 1).then_some(token.clone())
                }),
            );
            let verified = verify_token_identities(
                &candidates.into_iter().collect::<Vec<_>>(),
                &combined_pages,
            );
            for (token, identity) in &verified {
                if matches!(identity, Err(
                    pe_source_polymarket_public::MetadataIdentityError::MarketCardinality { .. }
                    | pe_source_polymarket_public::MetadataIdentityError::DuplicateIdentity { .. }
                )) && let Some(conditions) = token_conditions.get(token)
                {
                    cache.rejected.extend(conditions.iter().cloned());
                }
            }
            for (token, conditions) in token_conditions {
                if conditions
                    .iter()
                    .any(|condition| cache.rejected.contains(condition))
                {
                    resolved
                        .unverified
                        .insert(token.clone(), rejected_identity_reason());
                    cache.rejected_tokens.insert(token);
                }
            }
            let mut fresh = BTreeMap::new();
            collect_verified(
                verified,
                &cache.memory_pages.recorded.sequences,
                &mut fresh,
                &mut BTreeMap::new(),
            );
            fresh.retain(|token, cached| {
                newly_verified.contains_key(token)
                    && !cache.rejected_tokens.contains(token)
                    && !cache.rejected.contains(&cached.identity.condition_id.0)
            });
            *newly_verified = fresh;
            evict_rejected(&mut cache, resolved);
        }
        for (token, fresh) in newly_verified.iter_mut() {
            if let Some(cached) = cache.identities.get(token) {
                *fresh = cached.clone();
            }
            cache.identities.insert(token.clone(), fresh.clone());
        }
        Ok(())
    }

    async fn fetch_and_record_chunks(
        &self,
        tokens: &[PolymarketTokenId],
        filter: MarketFilter,
        rejected_message: &str,
        recorded: &mut RecordedIdentityPages,
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
                recorded.last_sequence = Some(sequence);
                recorded
                    .sequences
                    .entry(page.0.canonical_page_hash.clone())
                    .or_insert(sequence);
                recorded.pages.push(page);
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

    async fn cached(
        &self,
        requested: &BTreeSet<PolymarketTokenId>,
        store: Option<&IdentityStore>,
        read_pages: &mut BTreeMap<u64, IdentityPage>,
    ) -> Result<ResolvedIdentities, SourceError> {
        let mut cache = self.cache.write().await;
        let mut unverified = requested
            .iter()
            .filter(|token| cache.rejected_tokens.contains(*token))
            .map(|token| (token.clone(), rejected_identity_reason()))
            .collect::<BTreeMap<_, _>>();
        if let Some(store) = store {
            let misses = requested
                .iter()
                .filter(|token| {
                    !cache.identities.contains_key(*token)
                        && !cache.rejected_tokens.contains(*token)
                })
                .cloned()
                .collect::<Vec<_>>();
            let rows = store
                .paper_state
                .asset_identities(&store.generation, &misses)
                .map_err(identity_store_error)?;
            let mut usable = Vec::new();
            for row in rows {
                check_condition_rejection(store, &mut cache, &row.condition_id.0)?;
                if cache.rejected.contains(&row.condition_id.0) {
                    unverified.insert(row.token, rejected_identity_reason());
                } else {
                    usable.push(row);
                }
            }
            let authenticated =
                authenticate_identity_rows(store, usable, &cache.boot_pages, read_pages);
            delete_invalid_identity_rows(store, &authenticated.invalid).await?;
            for (row, cached) in authenticated.identities {
                if cache.rejected.contains(&row.condition_id.0) {
                    unverified.insert(row.token, rejected_identity_reason());
                } else {
                    cache.identities.insert(row.token, cached);
                }
            }
        }
        let mut resolved = ResolvedIdentities {
            verified: BTreeMap::new(),
            unverified,
            provenance: BTreeMap::new(),
        };
        evict_rejected(&mut cache, &mut resolved);
        for token in requested {
            if let Some(cached) = cache.identities.get(token) {
                resolved
                    .verified
                    .insert(token.clone(), cached.identity.clone());
                resolved
                    .provenance
                    .insert(token.clone(), cached.provenance.clone());
            }
        }
        Ok(resolved)
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
        if matches!(recorder, IdentityRecorder::Boot(_)) {
            self.cache
                .write()
                .await
                .boot_pages
                .insert(sequence.sequence.0, (sequence, page.clone()));
        }
        Ok(sequence.sequence.0)
    }
}

fn identity_row(
    token: &PolymarketTokenId,
    cached: &CachedIdentity,
) -> Result<AssetIdentityRow, SourceError> {
    Ok(AssetIdentityRow {
        token: token.clone(),
        condition_id: cached.identity.condition_id.clone(),
        outcome: i64::from(cached.identity.outcome.0),
        source_log_sequence: i64::try_from(cached.provenance.source_log_sequence)
            .map_err(identity_store_error)?,
        canonical_page_hash: cached.provenance.canonical_page_hash.clone(),
    })
}

fn rejected_identity_reason() -> String {
    "condition has conflicting Gamma identities in the installed generation".to_owned()
}

fn absent_identity_reason() -> String {
    "token absent from open and closed Gamma metadata".to_owned()
}

fn evict_rejected(cache: &mut IdentityCache, resolved: &mut ResolvedIdentities) {
    let rejected_tokens = cache
        .identities
        .iter()
        .filter(|(token, cached)| {
            cache.rejected_tokens.contains(*token)
                || cache.rejected.contains(&cached.identity.condition_id.0)
        })
        .map(|(token, _)| token.clone())
        .chain(
            resolved
                .verified
                .iter()
                .filter(|(token, identity)| {
                    cache.rejected_tokens.contains(*token)
                        || cache.rejected.contains(&identity.condition_id.0)
                })
                .map(|(token, _)| token.clone()),
        )
        .collect::<BTreeSet<_>>();
    for token in rejected_tokens {
        cache.rejected_tokens.insert(token.clone());
        cache.identities.remove(&token);
        resolved.verified.remove(&token);
        resolved.provenance.remove(&token);
        resolved
            .unverified
            .insert(token, rejected_identity_reason());
    }
}

fn check_condition_rejection(
    store: &IdentityStore,
    cache: &mut IdentityCache,
    condition: &str,
) -> Result<(), SourceError> {
    if !cache.rejected.contains(condition)
        && store
            .paper_state
            .asset_identity_condition_rejection(&store.generation, condition)
            .map_err(identity_store_error)?
            .is_some()
    {
        cache.rejected.insert(condition.to_owned());
    }
    Ok(())
}

async fn delete_invalid_identity_rows(
    store: &IdentityStore,
    invalid: &[PolymarketTokenId],
) -> Result<(), SourceError> {
    for chunk in invalid.chunks(ASSET_IDENTITY_INSERT_LIMIT) {
        identity_mutation(|| {
            store
                .paper_state
                .delete_asset_identities(&store.generation, chunk)
        })
        .await?;
    }
    Ok(())
}

// Paper-state owns the mutex and transaction; only waiting for its caller-owned batch is async.
async fn identity_mutation(
    mutation: impl Fn() -> Result<(), PaperStateError>,
) -> Result<(), SourceError> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match mutation() {
            Ok(()) => return Ok(()),
            Err(PaperStateError::IdentityBatchOpen) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(identity_store_error(
                        "identity mutation waited 30 s for an open batch",
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(identity_store_error(error)),
        }
    }
}

fn authenticate_identity_rows(
    store: &IdentityStore,
    rows: Vec<AssetIdentityRow>,
    boot_pages: &BTreeMap<u64, (AppendReceipt, IdentityPage)>,
    read_pages: &mut BTreeMap<u64, IdentityPage>,
) -> AuthenticatedIdentityRows {
    let mut grouped = BTreeMap::<u64, Vec<AssetIdentityRow>>::new();
    let mut invalid = Vec::new();
    for row in rows {
        if let Ok(sequence) = u64::try_from(row.source_log_sequence) {
            grouped.entry(sequence).or_default().push(row);
        } else {
            invalid.push(row.token);
        }
    }
    let mut authenticated = Vec::new();
    let mut pages = Vec::new();
    for (sequence, rows) in grouped {
        let page = read_pages.get(&sequence).cloned().or_else(|| {
            let receipt = store.source_receipts.receipt_at(EventSeq(sequence)).ok()?;
            let page = if let Some((receipt, _)) = receipt {
                let page = store.source_receipts.source_envelope(receipt).ok()?;
                if page.content_type != ContentType::Json {
                    return None;
                }
                let page_hash = canonical_page_hash(&page.payload).ok()?;
                (
                    MetadataPageEvidence {
                        request_url: String::new(),
                        canonical_page_hash: page_hash,
                        raw_page_hash: blake3::hash(&page.payload).to_hex().to_string(),
                        received_at: page.received_at,
                        source_id: page.source_id,
                        schema_version: page.schema_version,
                        parser_version: page.parser_version,
                    },
                    page.payload,
                )
            } else {
                let (receipt, page) = boot_pages.get(&sequence)?;
                if receipt.sequence.0 != sequence {
                    return None;
                }
                page.clone()
            };
            let evidence = &page.0;
            if evidence.source_id.0 != GAMMA_MARKETS_SOURCE_ID
                || evidence.schema_version != GAMMA_MARKETS_SCHEMA_VERSION
                || evidence.parser_version != GAMMA_MARKETS_PARSER_VERSION
                || blake3::hash(&page.1).to_hex().as_str() != evidence.raw_page_hash
                || canonical_page_hash(&page.1).ok().as_ref() != Some(&evidence.canonical_page_hash)
            {
                return None;
            }
            read_pages.insert(sequence, page.clone());
            Some(page)
        });
        let Some(page) = page else {
            invalid.extend(rows.into_iter().map(|row| row.token));
            continue;
        };
        let tokens = rows.iter().map(|row| row.token.clone()).collect::<Vec<_>>();
        let mut verified = verify_token_identities(&tokens, std::slice::from_ref(&page));
        let start = authenticated.len();
        for row in rows {
            let identity = u16::try_from(row.outcome)
                .ok()
                .map(|outcome| VerifiedTokenIdentity {
                    condition_id: row.condition_id.clone(),
                    outcome: OutcomeId(outcome),
                    evidence_hash: row.canonical_page_hash.clone(),
                });
            if let Some(identity) = identity
                && row.canonical_page_hash == page.0.canonical_page_hash
                && verified.remove(&row.token).and_then(Result::ok).as_ref() == Some(&identity)
            {
                let provenance = IdentityProvenance {
                    asset: row.token.clone(),
                    source_log_sequence: sequence,
                    canonical_page_hash: row.canonical_page_hash.clone(),
                };
                authenticated.push((
                    row,
                    CachedIdentity {
                        identity,
                        provenance,
                    },
                ));
            } else {
                invalid.push(row.token);
            }
        }
        if authenticated.len() > start {
            pages.push(page);
        }
    }
    AuthenticatedIdentityRows {
        identities: authenticated,
        pages,
        invalid,
    }
}

// Enumerate candidates only; the source crate's verifier owns all identity validation.
fn page_tokens(pages: &[IdentityPage]) -> Vec<PolymarketTokenId> {
    page_token_conditions(pages).into_keys().collect()
}

fn page_token_conditions(pages: &[IdentityPage]) -> BTreeMap<PolymarketTokenId, BTreeSet<String>> {
    let mut tokens = BTreeMap::<_, BTreeSet<_>>::new();
    for (condition, ids) in page_markets(pages) {
        for token in ids {
            let conditions = tokens.entry(PolymarketTokenId(token)).or_default();
            if let Some(condition) = &condition {
                conditions.insert(condition.clone());
            }
        }
    }
    tokens
}

fn page_markets(pages: &[IdentityPage]) -> Vec<(Option<String>, Vec<String>)> {
    let mut candidates = Vec::new();
    for (_, raw) in pages {
        let Ok(markets) = serde_json::from_slice::<Vec<serde_json::Value>>(raw) else {
            continue;
        };
        for market in markets {
            let Some(ids) = market.get("clobTokenIds") else {
                continue;
            };
            candidates.push((
                market
                    .get("conditionId")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                clob_token_ids(ids),
            ));
        }
    }
    candidates
}

/// The Gamma decoder's `clobTokenIds` coercion (`deserialize_clob_token_ids` in
/// pe-source-polymarket-public's gamma_markets.rs), so enumeration and verification agree on
/// every token: a stringified array of strings, or a native array with each entry coerced to its
/// string form, positions preserved; any other shape yields no tokens. It is mirrored here because
/// that crate is a Forge runtime input held unchanged until #740 merges;
/// `clob_token_ids_matches_the_gamma_decoder` pins the two together.
fn clob_token_ids(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::String(encoded) => {
            serde_json::from_str::<Vec<String>>(encoded).unwrap_or_default()
        }
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| match item {
                serde_json::Value::String(token) => token.clone(),
                other => other.to_string(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn identity_store_error(error: impl std::fmt::Display) -> SourceError {
    SourceError::Fatal {
        message: format!("asset identity cache failed: {error}"),
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
        attempts: StdMutex<Vec<tokio::time::Instant>>,
    }

    impl ReconciliationFetcher for MixedChunkFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.attempts
                    .lock()
                    .unwrap()
                    .push(tokio::time::Instant::now());
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

    struct RetryFixture {
        attempts: StdMutex<Vec<(bool, tokio::time::Instant)>>,
    }

    impl ReconciliationFetcher for RetryFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                let closed = url.contains("closed=true");
                let mut attempts = self.attempts.lock().unwrap();
                let count = attempts.iter().filter(|(pass, _)| *pass == closed).count();
                attempts.push((closed, tokio::time::Instant::now()));
                if count < 2 {
                    Err(SourceError::Transient {
                        message: "exhausted fetcher".to_owned(),
                    })
                } else if closed {
                    Ok(
                        br#"[{"conditionId":"condition-closed","clobTokenIds":["token-a"]}]"#
                            .to_vec(),
                    )
                } else {
                    Ok(b"[]".to_vec())
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bracket_chunk_retry_recovers() {
        let fetcher = Arc::new(RetryFixture {
            attempts: StdMutex::new(Vec::new()),
        });
        let (_dir, path, _sink, resolver) = boot_resolver(fetcher.clone());
        let start = tokio::time::Instant::now();
        let token = PolymarketTokenId("token-a".to_owned());
        let resolved = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert_eq!(resolved.verified[&token].condition_id.0, "condition-closed");
        assert_eq!(resolved.provenance[&token].source_log_sequence, 1);
        assert_eq!(
            fetcher
                .attempts
                .lock()
                .unwrap()
                .iter()
                .map(|(closed, at)| (*closed, at.duration_since(start).as_secs()))
                .collect::<Vec<_>>(),
            [
                (false, 0),
                (false, 2),
                (false, 6),
                (true, 6),
                (true, 8),
                (true, 12)
            ]
        );
        assert_eq!(Reader::replay(path).unwrap().count(), 2);
        let cached = resolver
            .resolve_historical_for_bracket([token])
            .await
            .unwrap();
        assert_eq!(cached.provenance, resolved.provenance);
        assert_eq!(fetcher.attempts.lock().unwrap().len(), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn bracket_chunk_retry_exhausts() {
        let fetcher = Arc::new(MixedChunkFixture {
            calls: AtomicUsize::new(0),
            attempts: StdMutex::new(Vec::new()),
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let sink = Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap()));
        let resolver = AssetIdentityResolver::new(fetcher.clone(), BASE.to_owned(), 1, sink);
        let start = tokio::time::Instant::now();
        assert!(matches!(
            resolver
                .resolve_historical_for_bracket([
                    PolymarketTokenId("token-a".to_owned()),
                    PolymarketTokenId("token-b".to_owned())
                ])
                .await,
            Err(SourceError::Transient { .. })
        ));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 6);
        assert_eq!(
            tokio::time::Instant::now().duration_since(start).as_secs(),
            30
        );
        assert_eq!(
            fetcher
                .attempts
                .lock()
                .unwrap()
                .iter()
                .map(|at| at.duration_since(start).as_secs())
                .collect::<Vec<_>>(),
            [0, 0, 2, 6, 14, 30]
        );
        assert_eq!(resolver.cache.read().await.identities.len(), 1);
        assert_eq!(Reader::replay(&path).unwrap().count(), 1);
        resolver
            .resolve_historical_for_bracket([PolymarketTokenId("token-a".to_owned())])
            .await
            .unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn poller_resolve_single_attempt() {
        let fetcher = Arc::new(RetryFixture {
            attempts: StdMutex::new(Vec::new()),
        });
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let start = tokio::time::Instant::now();
        assert!(matches!(
            resolver
                .resolve_live([PolymarketTokenId("token-a".to_owned())])
                .await,
            Err(SourceError::Transient { .. })
        ));
        assert_eq!(fetcher.attempts.lock().unwrap().len(), 1);
        assert_eq!(tokio::time::Instant::now(), start);
        assert!(resolver.cache.read().await.identities.is_empty());
    }

    async fn http_resolver(
        responses: Vec<(axum::http::StatusCode, &'static str)>,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        BootSourceLog,
        AssetIdentityResolver,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().route(
            "/markets",
            axum::routing::get(move || {
                let counter = counter.clone();
                let responses = responses.clone();
                async move {
                    let hit = counter.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = responses[hit.min(responses.len() - 1)];
                    (status, [("retry-after", "1")], body)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let fetcher = Arc::new(
            pe_source_polymarket_public::ReqwestFetcher::new(reqwest::Client::new())
                .with_min_interval_ms(0)
                .with_rate_limit_retry_max_secs(1),
        );
        let (dir, path, sink, _) = boot_resolver(fetcher.clone());
        let resolver =
            AssetIdentityResolver::new(fetcher, format!("http://{address}"), 50, sink.clone());
        (dir, path, sink, resolver, hits, server)
    }

    // Keep local socket I/O runnable while advancing only the deterministic test clock.
    async fn resolve_http(
        resolver: &AssetIdentityResolver,
    ) -> Result<ResolvedIdentities, SourceError> {
        let resolve =
            resolver.resolve_historical_for_bracket([PolymarketTokenId("token-a".to_owned())]);
        tokio::pin!(resolve);
        loop {
            tokio::select! {
                biased;
                result = &mut resolve => return result,
                () = tokio::task::yield_now() => tokio::time::advance(std::time::Duration::from_millis(1)).await,
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bracket_http_attempts_bounded() {
        use axum::http::StatusCode;
        for mixed in [false, true] {
            let mut responses = Vec::new();
            for _ in 0..5 {
                // Interleave all ten short 429s with the four exhausted 5xx attempts.
                for attempt in 0..4 {
                    if mixed {
                        responses.extend(vec![
                            (StatusCode::TOO_MANY_REQUESTS, "{}");
                            if attempt < 2 { 3 } else { 2 }
                        ]);
                    }
                    responses.push((StatusCode::INTERNAL_SERVER_ERROR, "{}"));
                }
            }
            let expected = if mixed { 70 } else { 20 };
            let (_dir, _path, _sink, resolver, hits, server) = http_resolver(responses).await;
            assert!(
                matches!(resolve_http(&resolver).await, Err(SourceError::Transient { message }) if message.contains("HTTP 500"))
            );
            assert_eq!(hits.load(Ordering::SeqCst), expected);
            assert!(resolver.cache.read().await.identities.is_empty());
            server.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bracket_non_transient_failures_not_retried() {
        use axum::http::StatusCode;
        for case in [
            "fatal",
            "parse",
            "rate_limit",
            "record_success",
            "record_error",
        ] {
            let response = match case {
                "fatal" => (StatusCode::FORBIDDEN, "{}"),
                "parse" | "record_error" => (StatusCode::OK, "not-json"),
                "rate_limit" => (StatusCode::TOO_MANY_REQUESTS, "{}"),
                _ => (
                    StatusCode::OK,
                    r#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
                ),
            };
            let (_dir, path, sink, resolver, hits, server) = http_resolver(vec![response]).await;
            if case.starts_with("record_") {
                sink.lock().await.fail_next_append();
            }
            let result = resolve_http(&resolver).await;
            match case {
                "rate_limit" => assert!(matches!(
                    result,
                    Err(SourceError::RateLimited {
                        retry_after_secs: 1
                    })
                )),
                "fatal" => assert!(
                    matches!(result, Err(SourceError::Fatal { message }) if message.contains("gamma token lookup rejected"))
                ),
                "parse" => assert!(
                    matches!(result, Err(SourceError::Fatal { message }) if message.contains("gamma metadata parse failed"))
                ),
                _ => assert!(
                    matches!(result, Err(SourceError::Fatal { message }) if message.contains("source-log append failed"))
                ),
            }
            assert_eq!(
                hits.load(Ordering::SeqCst),
                if case == "rate_limit" { 11 } else { 1 },
                "{case}"
            );
            assert!(resolver.cache.read().await.identities.is_empty(), "{case}");
            assert_eq!(
                Reader::replay(path).unwrap().count(),
                usize::from(case == "parse"),
                "{case}"
            );
            server.abort();
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

    fn durable_resolver(
        path: &std::path::Path,
        paper_state: Arc<PaperStateDb>,
        generation: &str,
        fetcher: Arc<dyn ReconciliationFetcher>,
        batch_size: usize,
    ) -> AssetIdentityResolver {
        let sink = Arc::new(Mutex::new(SourceEventSink::open(path).unwrap()));
        let receipts = SourceReceiptIndex::replay(path).unwrap();
        AssetIdentityResolver::new(fetcher, BASE.to_owned(), batch_size, sink).with_paper_state(
            paper_state,
            generation.to_owned(),
            receipts,
        )
    }

    async fn assert_memory_pages_released(resolver: &AssetIdentityResolver) {
        let cache = resolver.cache.read().await;
        assert!(cache.memory_pages.recorded.pages.is_empty());
        assert!(cache.memory_pages.recorded.sequences.is_empty());
        assert!(cache.memory_pages.recorded.last_sequence.is_none());
        assert!(cache.memory_pages.by_token.is_empty());
        assert!(cache.memory_pages.by_condition.is_empty());
    }

    /// PASS: open discovery verifies both outcomes, filters unrelated markets, and records provenance.
    /// FAIL: a warmed token skips the first condition read or loses its original provenance.
    #[tokio::test]
    async fn condition_discovery_records_fresh_pages_even_with_warmed_cache() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-b"]},{"conditionId":"other","clobTokenIds":["unrequested"]}]"#,
            b"[]",
        ));
        let (_dir, path, _sink, resolver) = boot_resolver(fetcher.clone());
        let a = PolymarketTokenId("token-a".into());
        let cached = resolver.resolve_live([a.clone()]).await.unwrap();
        let discovered = resolver
            .discover_conditions_for_bracket([PolymarketConditionId("condition".into())])
            .await
            .unwrap();
        assert_eq!(discovered.verified.len(), 2);
        assert_eq!(discovered.verified[&a].outcome, OutcomeId(0));
        assert_eq!(
            discovered.verified[&PolymarketTokenId("token-b".into())].outcome,
            OutcomeId(1)
        );
        assert_eq!(discovered.provenance.len(), 2);
        assert_eq!(discovered.provenance[&a], cached.provenance[&a]);
        assert_eq!(
            fetcher.urls.lock().unwrap()[1],
            format!("{BASE}/markets?condition_ids=condition&limit=500")
        );
        let entries = Reader::replay(&path)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 2);
        for provenance in discovered.provenance.values() {
            let (sequence, envelope) = entries
                .iter()
                .find(|(sequence, _)| sequence.0 == provenance.source_log_sequence)
                .unwrap();
            assert_eq!(provenance.source_log_sequence, sequence.0);
            assert_eq!(
                provenance.canonical_page_hash,
                canonical_page_hash(&envelope.payload).unwrap()
            );
            assert_eq!(envelope.source_id.0, GAMMA_MARKETS_SOURCE_ID);
            assert_eq!(envelope.schema_version, GAMMA_MARKETS_SCHEMA_VERSION);
            assert_eq!(envelope.parser_version, GAMMA_MARKETS_PARSER_VERSION);
        }
    }

    /// PASS: completed open, closed and empty discoveries reuse identities and original receipts.
    /// FAIL: another read requests Gamma or re-records a completed condition's pages.
    #[tokio::test]
    async fn condition_discovery_memo_reuses_original_provenance_and_empty_results() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"open","clobTokenIds":["open-a","open-b"]}]"#,
            br#"[{"conditionId":"closed","clobTokenIds":["closed-a","closed-b"]}]"#,
        ));
        let (_dir, path, _sink, resolver) = boot_resolver(fetcher.clone());
        let conditions = ["open", "closed", "empty"].map(|id| PolymarketConditionId(id.into()));
        let first = resolver
            .discover_conditions_for_bracket(conditions.clone())
            .await
            .unwrap();
        assert_eq!(first.verified.len(), 4);
        assert!(resolver.cache.read().await.discovered_conditions["empty"].is_empty());
        let calls = fetcher.calls.load(Ordering::SeqCst);
        let second = resolver
            .discover_conditions_for_bracket(conditions)
            .await
            .unwrap();
        assert_eq!(second.verified, first.verified);
        assert_eq!(second.provenance, first.provenance);
        assert_eq!(second.unverified, first.unverified);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), calls);
        assert_eq!(Reader::replay(path).unwrap().count(), calls);
    }

    /// PASS: a mixed discovery requests only conditions whose lookup has not completed.
    /// FAIL: a remembered condition enters either the open or closed query again.
    #[tokio::test]
    async fn condition_discovery_mixed_request_fetches_only_unremembered_conditions() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"open","clobTokenIds":["token-a"]}]"#,
            br#"[{"conditionId":"closed","clobTokenIds":["token-b"]}]"#,
        ));
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let first = resolver
            .discover_conditions_for_bracket([PolymarketConditionId("open".into())])
            .await
            .unwrap();
        let mixed = resolver
            .discover_conditions_for_bracket([
                PolymarketConditionId("open".into()),
                PolymarketConditionId("closed".into()),
            ])
            .await
            .unwrap();
        assert_eq!(mixed.verified.len(), 2);
        assert_eq!(
            mixed.provenance[&PolymarketTokenId("token-a".into())],
            first.provenance[&PolymarketTokenId("token-a".into())]
        );
        assert_eq!(
            *fetcher.urls.lock().unwrap(),
            [
                format!("{BASE}/markets?condition_ids=open&limit=500"),
                format!("{BASE}/markets?condition_ids=closed&limit=500"),
                format!("{BASE}/markets?condition_ids=closed&closed=true&limit=500"),
            ]
        );
    }

    /// PASS: token-path conflicts invalidate every remembered sibling without another discovery.
    /// FAIL: memoized discovery resurrects an evicted or rejected token.
    #[tokio::test]
    async fn condition_discovery_memo_honors_later_token_path_rejection() {
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#.to_vec(),
                br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]}]"#.to_vec(),
            ])),
        });
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let condition = PolymarketConditionId("condition".into());
        let first = resolver
            .discover_conditions_for_bracket([condition.clone()])
            .await
            .unwrap();
        assert_eq!(first.verified.len(), 2);
        let rejected = resolver
            .resolve_live([PolymarketTokenId("token-b".into())])
            .await
            .unwrap();
        assert!(rejected.verified.is_empty());
        let second = resolver
            .discover_conditions_for_bracket([condition])
            .await
            .unwrap();
        assert!(second.verified.is_empty());
        assert!(second.provenance.is_empty());
        for token in first.verified.keys() {
            assert_eq!(second.unverified[token], rejected_identity_reason());
        }
        assert!(fetcher.pages.lock().unwrap().is_empty());
    }

    /// PASS: a missing cached token and durable condition/token markers remain unresolved.
    /// FAIL: remembering discovery bypasses the existing rejection or absence authority.
    #[tokio::test]
    async fn condition_discovery_memo_honors_absence_and_durable_rejections() {
        for case in ["absent", "condition", "token"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-b"]}]"#,
                b"[]",
            ));
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
            let condition = PolymarketConditionId("condition".into());
            let token = PolymarketTokenId("token-a".into());
            let first = resolver
                .discover_conditions_for_bracket([condition.clone()])
                .await
                .unwrap();
            if case == "absent" {
                resolver.cache.write().await.identities.remove(&token);
            } else {
                let sequence = i64::try_from(first.provenance[&token].source_log_sequence).unwrap();
                paper
                    .save_asset_identities(
                        "installed",
                        &[],
                        &if case == "condition" {
                            BTreeMap::from([("condition".into(), sequence)])
                        } else {
                            BTreeMap::new()
                        },
                        &BTreeMap::new(),
                        &if case == "token" {
                            BTreeMap::from([(token.clone(), sequence)])
                        } else {
                            BTreeMap::new()
                        },
                    )
                    .unwrap();
            }
            let second = resolver
                .discover_conditions_for_bracket([condition])
                .await
                .unwrap();
            assert!(!second.verified.contains_key(&token));
            assert!(!second.provenance.contains_key(&token));
            assert_eq!(
                second.unverified[&token],
                if case == "absent" {
                    absent_identity_reason()
                } else {
                    rejected_identity_reason()
                }
            );
            assert_eq!(second.verified.len(), usize::from(case != "condition"));
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        }
    }

    /// PASS: failed lookups remain eligible for a fresh open/closed attempt on retry.
    /// FAIL: a transient failure is remembered as a completed empty discovery.
    #[tokio::test(start_paused = true)]
    async fn condition_discovery_transient_failure_remembers_nothing_and_retries() {
        let fetcher = Arc::new(RetryFixture {
            attempts: StdMutex::new(Vec::new()),
        });
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let condition = PolymarketConditionId("condition-closed".into());
        for _ in 0..4 {
            assert!(matches!(
                resolver
                    .discover_conditions_for_bracket([condition.clone()])
                    .await,
                Err(SourceError::Transient { .. })
            ));
            assert!(resolver.cache.read().await.discovered_conditions.is_empty());
        }
        let retried = resolver
            .discover_conditions_for_bracket([condition.clone()])
            .await
            .unwrap();
        assert_eq!(retried.verified.len(), 1);
        assert_eq!(
            fetcher
                .attempts
                .lock()
                .unwrap()
                .iter()
                .map(|(closed, _)| *closed)
                .collect::<Vec<_>>(),
            [false, false, false, true, false, true, false, true]
        );
        let repeated = resolver
            .discover_conditions_for_bracket([condition])
            .await
            .unwrap();
        assert_eq!(repeated.provenance, retried.provenance);
        assert_eq!(fetcher.attempts.lock().unwrap().len(), 8);
    }

    /// PASS: an open condition stays remembered when another condition's closed lookup fails.
    /// FAIL: a partial discovery remembers an unfinished condition or refetches completed progress.
    #[tokio::test]
    async fn condition_discovery_partial_failure_remembers_only_completed_conditions() {
        struct ClosedFailure {
            inner: GammaFixture,
            closed_calls: AtomicUsize,
        }
        impl ReconciliationFetcher for ClosedFailure {
            fn fetch<'a>(
                &'a self,
                url: &'a str,
            ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>>
            {
                Box::pin(async move {
                    let page = self.inner.fetch(url).await?;
                    if url.contains("closed=true")
                        && self.closed_calls.fetch_add(1, Ordering::SeqCst) == 0
                    {
                        return Err(SourceError::Transient {
                            message: "closed lookup failed".into(),
                        });
                    }
                    Ok(page)
                })
            }
        }
        let fetcher = Arc::new(ClosedFailure {
            inner: GammaFixture::new(
                br#"[{"conditionId":"open","clobTokenIds":["token-a"]}]"#,
                br#"[{"conditionId":"closed","clobTokenIds":["token-b"]}]"#,
            ),
            closed_calls: AtomicUsize::new(0),
        });
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let conditions = ["open", "closed"].map(|id| PolymarketConditionId(id.into()));
        assert!(matches!(
            resolver
                .discover_conditions_for_bracket(conditions.clone())
                .await,
            Err(SourceError::Transient { .. })
        ));
        assert_eq!(
            resolver
                .cache
                .read()
                .await
                .discovered_conditions
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            ["open"]
        );
        let retried = resolver
            .discover_conditions_for_bracket(conditions)
            .await
            .unwrap();
        assert_eq!(retried.verified.len(), 2);
        assert_eq!(
            *fetcher.inner.urls.lock().unwrap(),
            [
                format!("{BASE}/markets?condition_ids=closed&condition_ids=open&limit=500"),
                format!("{BASE}/markets?condition_ids=closed&closed=true&limit=500"),
                format!("{BASE}/markets?condition_ids=closed&limit=500"),
                format!("{BASE}/markets?condition_ids=closed&closed=true&limit=500"),
            ]
        );
    }

    /// PASS: only unresolved conditions get a closed lookup and condition pages restore on restart.
    /// FAIL: an empty open lookup loses a closed market's remaining outcome tokens.
    #[tokio::test]
    async fn condition_discovery_closed_fallback_persists_reauthenticatable_identities() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"open","clobTokenIds":["open-a","open-b"]}]"#,
            br#"[{"conditionId":"closed","closed":true,"clobTokenIds":["closed-a","closed-b"]}]"#,
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let discovered = resolver
            .discover_conditions_for_bracket([
                PolymarketConditionId("open".into()),
                PolymarketConditionId("closed".into()),
            ])
            .await
            .unwrap();
        assert_eq!(discovered.verified.len(), 4);
        assert_eq!(
            *fetcher.urls.lock().unwrap(),
            [
                format!("{BASE}/markets?condition_ids=closed&condition_ids=open&limit=500"),
                format!("{BASE}/markets?condition_ids=closed&closed=true&limit=500")
            ]
        );
        let tokens = discovered.verified.keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            paper.asset_identities("installed", &tokens).unwrap().len(),
            4
        );
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        let restored = restarted
            .resolve_historical_for_bracket(tokens)
            .await
            .unwrap();
        assert_eq!(restored.verified, discovered.verified);
        assert_eq!(restored.provenance, discovered.provenance);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    /// PASS: fresh conflicting discovery rejects warmed and stored identities through the existing owner.
    /// FAIL: a cache hit or a cold stored row bypasses the new page's conflict.
    #[tokio::test]
    async fn condition_discovery_rejects_conflicting_warmed_and_stored_identities() {
        for durable in [false, true] {
            for warmed in [false, true] {
                if !durable && !warmed {
                    continue;
                }
                let fetcher = Arc::new(SequenceFixture {
                    pages: StdMutex::new(std::collections::VecDeque::from([
                        br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-b"]}]"#
                            .to_vec(),
                        br#"[{"conditionId":"condition","clobTokenIds":["token-b","token-a"]}]"#
                            .to_vec(),
                        b"[]".to_vec(),
                    ])),
                });
                let (dir, path, sink, resolver) = boot_resolver(fetcher.clone());
                let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
                let resolver = if durable {
                    resolver.with_paper_state(
                        paper.clone(),
                        "installed".into(),
                        SourceReceiptIndex::replay(&path).unwrap(),
                    )
                } else {
                    resolver
                };
                let token = PolymarketTokenId("token-a".into());
                resolver.resolve_live([token.clone()]).await.unwrap();
                let resolver = if warmed {
                    resolver
                } else {
                    drop(resolver);
                    drop(sink);
                    durable_resolver(&path, paper.clone(), "installed", fetcher, 50)
                };
                let discovered = resolver
                    .discover_conditions_for_bracket([PolymarketConditionId("condition".into())])
                    .await
                    .unwrap();
                assert!(discovered.verified.is_empty());
                assert!(discovered.provenance.is_empty());
                assert!(discovered.unverified.contains_key(&token));
                assert!(resolver.cache.read().await.identities.is_empty());
                if durable {
                    assert!(
                        paper
                            .asset_identity_condition_rejection("installed", "condition")
                            .unwrap()
                            .is_some()
                    );
                    assert!(
                        paper
                            .rejected_asset_tokens("installed", std::slice::from_ref(&token))
                            .unwrap()
                            .contains_key(&token)
                    );
                }
            }
        }
    }

    /// PASS: a durably rejected token without an identity row rejects its new condition and sibling.
    /// FAIL: condition discovery resurrects a previously rejected token after restart.
    #[tokio::test]
    async fn condition_discovery_previously_rejected_token_rejects_new_condition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let resolver = durable_resolver(&path, paper.clone(), "installed", Arc::new(GammaFixture::new(
            br#"[{"conditionId":"old-a","clobTokenIds":["token-a"]},{"conditionId":"old-b","clobTokenIds":["token-a"]}]"#, b"[]")), 50);
        assert!(
            resolver
                .resolve_live([token.clone()])
                .await
                .unwrap()
                .verified
                .is_empty()
        );
        assert!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        drop(resolver);
        let resolver = durable_resolver(
            &path,
            paper.clone(),
            "installed",
            Arc::new(GammaFixture::new(
                br#"[{"conditionId":"new","clobTokenIds":["token-a","token-b"]}]"#,
                b"[]",
            )),
            50,
        );
        let discovered = resolver
            .discover_conditions_for_bracket([PolymarketConditionId("new".into())])
            .await
            .unwrap();
        assert!(discovered.verified.is_empty());
        assert!(discovered.provenance.is_empty());
        let tokens = [token, PolymarketTokenId("token-b".into())];
        assert!(
            tokens
                .iter()
                .all(|token| discovered.unverified.contains_key(token))
        );
        assert!(
            paper
                .asset_identity_condition_rejection("installed", "new")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            paper
                .rejected_asset_tokens("installed", &tokens)
                .unwrap()
                .len(),
            2
        );
    }

    /// PASS: returned identities belong to requested conditions; duplicate outcomes fail verification.
    /// FAIL: a mismatched condition or malformed outcome vector supplies a discovered identity.
    #[tokio::test]
    async fn condition_discovery_mismatch_and_invalid_vector_return_no_identities() {
        for page in [
            br#"[{"conditionId":"other","clobTokenIds":["token-a","token-b"]}]"#.as_slice(),
            br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-a"]}]"#.as_slice(),
        ] {
            let fetcher = Arc::new(GammaFixture::new(page, b"[]"));
            let (_dir, path, _sink, resolver) = boot_resolver(fetcher.clone());
            let discovered = resolver
                .discover_conditions_for_bracket([PolymarketConditionId("condition".into())])
                .await
                .unwrap();
            assert!(discovered.verified.is_empty());
            assert!(discovered.provenance.is_empty());
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
            assert_eq!(Reader::replay(path).unwrap().count(), 2);
        }
    }

    /// PASS: parse failures retain their raw page; transport failures remain transient.
    /// FAIL: discovery uses malformed bytes or converts a transient failure into a persistent one.
    #[tokio::test]
    async fn condition_discovery_errors_record_parse_pages_and_preserve_transience() {
        let (_dir, path, _sink, resolver) =
            boot_resolver(Arc::new(GammaFixture::new(b"invalid-json", b"[]")));
        assert!(matches!(
            resolver
                .discover_conditions_for_bracket([PolymarketConditionId("condition".into())])
                .await,
            Err(SourceError::Fatal { .. })
        ));
        assert_eq!(
            Reader::replay(path)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .1
                .payload,
            b"invalid-json"
        );
        let (_dir, _path, _sink, resolver) = boot_resolver(Arc::new(RetryFixture {
            attempts: StdMutex::new(Vec::new()),
        }));
        assert!(matches!(
            resolver
                .discover_conditions_for_bracket([PolymarketConditionId("condition".into())])
                .await,
            Err(SourceError::Transient { .. })
        ));
    }

    #[tokio::test]
    async fn memory_only_handoff_preserves_comparison_evidence_and_rejects_later_conflict() {
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#.to_vec(),
                br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]}]"#.to_vec(),
            ])),
        });
        let (dir, path, sink, resolver) = boot_resolver(fetcher);
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let tokens = ["token-a", "token-x", "token-z", "token-b"]
            .map(|token| PolymarketTokenId(token.into()));
        let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        assert!(original.verified.contains_key(&tokens[0]));
        assert!(
            paper
                .asset_identities("installed", &tokens)
                .unwrap()
                .is_empty()
        );
        resolver
            .install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            )
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        let conflicting = resolver.resolve_live([tokens[3].clone()]).await.unwrap();
        assert!(conflicting.verified.is_empty());
        assert!(conflicting.provenance.is_empty());
        assert!(
            tokens
                .iter()
                .all(|token| conflicting.unverified.contains_key(token))
        );
        assert!(resolver.cache.read().await.identities.is_empty());
        assert_eq!(
            paper
                .asset_identity_condition_rejection("installed", "condition")
                .unwrap(),
            Some(1)
        );
        drop(resolver);
        drop(sink);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        for token in tokens {
            for purpose in [LookupPurpose::Live, LookupPurpose::Historical] {
                let result = restarted
                    .resolve_inner([token.clone()], purpose)
                    .await
                    .unwrap();
                assert!(result.verified.is_empty());
                assert!(result.provenance.is_empty());
                assert_eq!(result.unverified[&token], rejected_identity_reason());
            }
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn memory_only_clean_handoff_saves_siblings_and_restores_original_provenance() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#,
            b"[]",
        ));
        let (dir, path, sink, resolver) = boot_resolver(fetcher.clone());
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let sibling = PolymarketTokenId("token-x".into());
        let original = resolver.resolve_live([token.clone()]).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolver.cache.read().await.identities.len(), 1);
        resolver
            .install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            )
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        assert!(resolver.cache.read().await.boot_pages.is_empty());
        assert_eq!(
            paper
                .asset_identities("installed", &[token.clone(), sibling.clone()])
                .unwrap()
                .len(),
            2
        );
        drop(resolver);
        drop(sink);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        let restored = restarted.resolve_live([token.clone()]).await.unwrap();
        assert_eq!(restored.verified, original.verified);
        assert_eq!(restored.provenance, original.provenance);
        let restored = restarted.resolve_live([sibling.clone()]).await.unwrap();
        assert_eq!(restored.verified[&sibling].outcome, OutcomeId(1));
        assert_eq!(
            restored.provenance[&sibling].source_log_sequence,
            original.provenance[&token].source_log_sequence
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn clob_token_ids_matches_the_gamma_decoder() {
        let probes = [
            "a",
            "",
            "b",
            "123",
            "null",
            "true",
            "{\"token\":\"b\"}",
            "[\"c\"]",
            "malformed",
        ]
        .map(|probe| PolymarketTokenId(probe.to_owned()));
        for value in [
            serde_json::json!("[\"a\",\"\",\"b\"]"),
            serde_json::json!(["a", 123, "", null, true, {"token": "b"}, ["c"]]),
            serde_json::json!(["a", 123]),
            serde_json::json!("[\"a\",123]"),
            serde_json::json!("malformed"),
            serde_json::json!("{}"),
            serde_json::json!([]),
            serde_json::json!(null),
            serde_json::json!(123),
            serde_json::json!(true),
            serde_json::json!({"token": "a"}),
        ] {
            let page = (
                MetadataPageEvidence {
                    request_url: String::new(),
                    raw_page_hash: String::new(),
                    canonical_page_hash: "page".to_owned(),
                    received_at: pe_core_types::ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                    source_id: pe_core_types::SourceId(GAMMA_MARKETS_SOURCE_ID.into()),
                    schema_version: GAMMA_MARKETS_SCHEMA_VERSION,
                    parser_version: GAMMA_MARKETS_PARSER_VERSION,
                },
                serde_json::to_vec(&serde_json::json!([
                    {"conditionId": "condition", "clobTokenIds": value}
                ]))
                .unwrap(),
            );
            let enumerated = page_markets(std::slice::from_ref(&page))
                .into_iter()
                .flat_map(|(_, ids)| ids)
                .collect::<Vec<_>>();
            assert_eq!(enumerated, clob_token_ids(&value), "{value}");
            // The verifier decodes pages with the Gamma decoder; it reports exactly the tokens it
            // finds listed, so its keys over the probes must equal the enumerated tokens.
            let decoded = verify_token_identities(&probes, std::slice::from_ref(&page))
                .into_keys()
                .collect::<BTreeSet<_>>();
            let listed = probes
                .iter()
                .filter(|probe| enumerated.contains(&probe.0))
                .cloned()
                .collect::<BTreeSet<_>>();
            assert_eq!(decoded, listed, "{value}");
        }
    }

    #[tokio::test]
    async fn memory_only_mixed_array_handoff_rejects_conflict_across_restart() {
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition","clobTokenIds":["token-a",123]}]"#.to_vec(),
                br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]}]"#.to_vec(),
            ])),
        });
        let (dir, path, sink, resolver) = boot_resolver(fetcher);
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let tokens =
            ["token-a", "123", "token-z", "token-b"].map(|token| PolymarketTokenId(token.into()));
        let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        assert!(original.verified.contains_key(&tokens[0]));
        resolver
            .install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            )
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        assert_eq!(
            paper.asset_identities("installed", &tokens).unwrap().len(),
            2
        );
        let conflicting = resolver.resolve_live([tokens[3].clone()]).await.unwrap();
        assert!(conflicting.verified.is_empty());
        assert!(conflicting.provenance.is_empty());
        for token in &tokens {
            assert_eq!(conflicting.unverified[token], rejected_identity_reason());
        }
        assert!(resolver.cache.read().await.identities.is_empty());
        assert_eq!(
            paper
                .asset_identity_condition_rejection("installed", "condition")
                .unwrap(),
            Some(1)
        );
        drop(resolver);
        drop(sink);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        for token in tokens {
            for purpose in [LookupPurpose::Live, LookupPurpose::Historical] {
                let result = restarted
                    .resolve_inner([token.clone()], purpose)
                    .await
                    .unwrap();
                assert!(result.verified.is_empty());
                assert!(result.provenance.is_empty());
                assert_eq!(result.unverified[&token], rejected_identity_reason());
            }
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn durable_mixed_array_verifies_and_saves_string_and_coerced_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a",123]}]"#,
            b"[]",
        ));
        let tokens = ["token-a", "123"].map(|token| PolymarketTokenId(token.into()));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        assert_eq!(original.verified[&tokens[0]].outcome, OutcomeId(0));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        let rows = paper.asset_identities("installed", &tokens).unwrap();
        assert_eq!(rows.len(), 2);
        for (token, outcome) in tokens.iter().zip([0, 1]) {
            let row = rows.iter().find(|row| &row.token == token).unwrap();
            assert_eq!(row.outcome, outcome);
            assert_eq!(
                row.canonical_page_hash,
                original.provenance[&tokens[0]].canonical_page_hash
            );
        }
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        let restored = restarted.resolve_live(tokens.clone()).await.unwrap();
        assert_eq!(restored.verified[&tokens[0]], original.verified[&tokens[0]]);
        assert_eq!(
            restored.provenance[&tokens[0]],
            original.provenance[&tokens[0]]
        );
        assert_eq!(restored.verified[&tokens[1]].outcome, OutcomeId(1));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn memory_only_handoff_preserves_each_siblings_acknowledged_provenance() {
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"],"question":"first"}]"#.to_vec(),
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"],"question":"second"}]"#.to_vec(),
            ])),
        });
        let (dir, path, sink, resolver) = boot_resolver(fetcher);
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let tokens = ["token-a", "token-x"].map(|token| PolymarketTokenId(token.into()));
        let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        let sibling = resolver.resolve_live([tokens[1].clone()]).await.unwrap();
        assert_eq!(original.provenance[&tokens[0]].source_log_sequence, 0);
        assert_eq!(sibling.provenance[&tokens[1]].source_log_sequence, 1);
        assert_ne!(
            original.verified[&tokens[0]].evidence_hash,
            sibling.verified[&tokens[1]].evidence_hash
        );
        resolver
            .install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            )
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        let rows = paper.asset_identities("installed", &tokens).unwrap();
        assert_eq!(rows.len(), 2);
        for resolved in [&original, &sibling] {
            for (token, provenance) in &resolved.provenance {
                let row = rows.iter().find(|row| &row.token == token).unwrap();
                assert_eq!(
                    row.source_log_sequence,
                    i64::try_from(provenance.source_log_sequence).unwrap()
                );
                assert_eq!(row.canonical_page_hash, provenance.canonical_page_hash);
            }
        }
        drop(resolver);
        drop(sink);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        for (token, original) in tokens.into_iter().zip([original, sibling]) {
            let restored = restarted.resolve_live([token]).await.unwrap();
            assert_eq!(restored.verified, original.verified);
            assert_eq!(restored.provenance, original.provenance);
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn memory_only_absent_lookups_deduplicate_pages_and_keep_first_sequence_until_handoff() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#,
            b"[]",
        ));
        let (dir, path, _sink, resolver) = boot_resolver(fetcher.clone());
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let absent = PolymarketTokenId("absent".into());
        for _ in 0..8 {
            let result = resolver.resolve_live([absent.clone()]).await.unwrap();
            assert!(result.verified.is_empty());
            assert!(result.unverified.contains_key(&absent));
            let cache = resolver.cache.read().await;
            assert_eq!(cache.memory_pages.recorded.pages.len(), 2);
            assert_eq!(cache.memory_pages.recorded.sequences.len(), 2);
            assert_eq!(cache.memory_pages.by_token.len(), 2);
            assert_eq!(cache.memory_pages.by_condition.len(), 1);
            for (page, sequence) in cache.memory_pages.recorded.pages.iter().zip([0, 1]) {
                assert_eq!(
                    cache.memory_pages.recorded.sequences[&page.0.canonical_page_hash],
                    sequence
                );
            }
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 16);
        resolver
            .install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            )
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        let rows = paper
            .asset_identities("installed", &[PolymarketTokenId("token-a".into())])
            .unwrap();
        assert_eq!(rows[0].source_log_sequence, 0);
    }

    #[tokio::test]
    async fn memory_only_handoff_persists_conflicts_among_boot_pages_without_token_rows() {
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#.to_vec(),
                br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]}]"#.to_vec(),
            ])),
        });
        let (dir, path, sink, resolver) = boot_resolver(fetcher);
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let tokens = ["token-a", "token-x", "token-z", "token-b"]
            .map(|token| PolymarketTokenId(token.into()));
        resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        assert!(
            resolver
                .resolve_live([tokens[3].clone()])
                .await
                .unwrap()
                .verified
                .is_empty()
        );
        resolver
            .install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            )
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        assert!(resolver.cache.read().await.identities.is_empty());
        assert!(
            paper
                .asset_identities("installed", &tokens)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            paper
                .rejected_asset_tokens("installed", &tokens)
                .unwrap()
                .len(),
            4
        );
        assert!(
            paper
                .asset_identity_condition_rejection("installed", "condition")
                .unwrap()
                .is_some()
        );
        drop(resolver);
        drop(sink);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        for token in tokens {
            let result = restarted.resolve_live([token.clone()]).await.unwrap();
            assert!(result.verified.is_empty());
            assert_eq!(result.unverified[&token], rejected_identity_reason());
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn memory_only_handoff_waits_for_batch_end_and_commits_before_returning() {
        for rollback in [false, true] {
            let fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition","clobTokenIds":["token-a"]}]"#,
                b"[]",
            ));
            let (dir, path, _sink, resolver) = boot_resolver(fetcher);
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let token = PolymarketTokenId("token-a".into());
            let original = resolver.resolve_live([token.clone()]).await.unwrap();
            paper.begin_batch().unwrap();
            let mut installing = Box::pin(resolver.install_paper_state(
                paper.clone(),
                "installed".into(),
                SourceReceiptIndex::replay(&path).unwrap(),
            ));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(25), installing.as_mut())
                    .await
                    .is_err()
            );
            assert!(
                paper
                    .asset_identities("installed", std::slice::from_ref(&token))
                    .unwrap()
                    .is_empty()
            );
            if rollback {
                paper.rollback_batch().unwrap();
            } else {
                paper.commit_batch().unwrap();
            }
            installing.await.unwrap();
            assert_memory_pages_released(&resolver).await;
            let rows = paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].source_log_sequence,
                i64::try_from(original.provenance[&token].source_log_sequence).unwrap()
            );
            assert_eq!(
                resolver.resolve_live([token]).await.unwrap().provenance,
                original.provenance
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn memory_only_handoff_failure_retains_evidence_for_retry() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let (dir, path, _sink, resolver) = boot_resolver(fetcher);
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let token = PolymarketTokenId("token-a".into());
        resolver.resolve_live([token.clone()]).await.unwrap();
        let receipts = SourceReceiptIndex::replay(&path).unwrap();
        paper.begin_batch().unwrap();
        assert!(matches!(
            resolver.install_paper_state(paper.clone(), "installed".into(), receipts.clone()).await,
            Err(SourceError::Fatal { message }) if message.contains("identity mutation waited 30 s for an open batch")
        ));
        assert!(resolver.store.read().await.is_none());
        assert_eq!(
            resolver
                .cache
                .read()
                .await
                .memory_pages
                .recorded
                .pages
                .len(),
            1
        );
        assert!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        paper.rollback_batch().unwrap();
        resolver
            .install_paper_state(paper.clone(), "installed".into(), receipts)
            .await
            .unwrap();
        assert_memory_pages_released(&resolver).await;
        assert_eq!(
            paper.asset_identities("installed", &[token]).unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn durable_verified_page_saves_unrequested_siblings_for_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":"[\"token-a\",\"token-x\"]"}]"#,
            b"[]",
        ));
        let token = PolymarketTokenId("token-a".into());
        let sibling = PolymarketTokenId("token-x".into());
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher, 50);
        let original = resolver.resolve_live([token.clone()]).await.unwrap();
        assert_eq!(original.verified.len(), 1);
        let rows = paper
            .asset_identities("installed", &[token.clone(), sibling.clone()])
            .unwrap();
        assert_eq!(rows.len(), 2);
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        let restored = resolver.resolve_live([sibling.clone()]).await.unwrap();
        assert_eq!(restored.verified[&sibling].outcome, OutcomeId(1));
        assert_eq!(
            restored.provenance[&sibling].source_log_sequence,
            original.provenance[&token].source_log_sequence
        );
        assert_eq!(
            restored.provenance[&sibling].canonical_page_hash,
            original.provenance[&token].canonical_page_hash
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn runtime_verified_identities_are_written_before_resolve_returns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut sink = SourceEventSink::open(&path).unwrap();
        let receipts = SourceReceiptIndex::replay(&path).unwrap();
        let writer_receipts = receipts.clone();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let (source_log, mut receiver) = SourceLogHandle::channel(1);
        let writer = tokio::spawn(async move {
            while let Some((envelope, acknowledgement)) = receiver.recv_for_test().await {
                let receipt = sink.append_durable(envelope).unwrap();
                writer_receipts.catch_up_verified_tail().unwrap();
                acknowledgement.send(receipt).unwrap();
            }
        });
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#,
            b"[]",
        ));
        let resolver =
            AssetIdentityResolver::new_runtime(fetcher.clone(), BASE.into(), 50, source_log)
                .with_paper_state(paper.clone(), "installed".into(), receipts);
        let token = PolymarketTokenId("token-a".into());
        let resolved = resolver.resolve_live([token.clone()]).await.unwrap();
        let saved = paper
            .asset_identities("installed", std::slice::from_ref(&token))
            .unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(
            saved[0].canonical_page_hash,
            resolved.provenance[&token].canonical_page_hash
        );
        assert!(resolver.cache.read().await.boot_pages.is_empty());
        drop(resolver);
        writer.await.unwrap();
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        assert_eq!(
            resolver.resolve_live([token]).await.unwrap().provenance,
            resolved.provenance
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    struct OverlapFixture {
        calls: AtomicUsize,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl ReconciliationFetcher for OverlapFixture {
        fn fetch<'a>(
            &'a self,
            _url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                Ok(
                    br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#
                        .to_vec(),
                )
            })
        }
    }

    #[tokio::test]
    async fn overlapping_identity_requests_make_one_gamma_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(OverlapFixture {
            calls: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let resolver = Arc::new(durable_resolver(
            &path,
            paper,
            "installed",
            fetcher.clone(),
            50,
        ));
        let token = PolymarketTokenId("token-a".into());
        let first_resolver = resolver.clone();
        let first_token = token.clone();
        let first =
            tokio::spawn(async move { first_resolver.resolve_live([first_token]).await.unwrap() });
        fetcher.started.notified().await;
        let second_resolver = resolver.clone();
        let second_token = token.clone();
        let second = tokio::spawn(async move {
            second_resolver
                .resolve_live([second_token, PolymarketTokenId("token-x".into())])
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        fetcher.release.notify_one();
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(first.provenance[&token], second.provenance[&token]);
        assert_eq!(second.verified.len(), 2);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
    }

    struct InterleavedIdentityFixture {
        urls: StdMutex<Vec<String>>,
        release: tokio::sync::Notify,
        closed_only: bool,
    }

    impl ReconciliationFetcher for InterleavedIdentityFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                let first = {
                    let mut urls = self.urls.lock().unwrap();
                    urls.push(url.to_owned());
                    urls.len() == 1
                };
                if first {
                    self.release.notified().await;
                }
                if self.closed_only {
                    if url.contains("closed=true") {
                        Ok(br#"[{"conditionId":"condition","clobTokenIds":["token-b"]}]"#.to_vec())
                    } else {
                        Ok(b"[]".to_vec())
                    }
                } else if url.contains("clob_token_ids=token-a") {
                    Ok(
                        br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#
                            .to_vec(),
                    )
                } else {
                    Ok(
                        br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]}]"#
                            .to_vec(),
                    )
                }
            })
        }
    }

    async fn assert_interleaved_identity_conflict_is_rejected(durable: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let fetcher = Arc::new(InterleavedIdentityFixture {
            urls: StdMutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
            closed_only: false,
        });
        let resolver = if durable {
            durable_resolver(
                &path,
                Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap()),
                "installed",
                fetcher.clone(),
                1,
            )
        } else {
            AssetIdentityResolver::new(
                fetcher.clone(),
                BASE.into(),
                1,
                Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
            )
        };
        let token_a = PolymarketTokenId("token-a".into());
        let token_b = PolymarketTokenId("token-b".into());
        let mut first = Box::pin(resolver.resolve_live([token_a.clone(), token_b.clone()]));
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let mut second = Box::pin(resolver.resolve_live([token_b.clone()]));
        std::future::poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(fetcher.urls.lock().unwrap().len(), 1);
        fetcher.release.notify_one();
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();
        assert!(first.verified.is_empty());
        assert!(first.provenance.is_empty());
        assert!(first.unverified.contains_key(&token_a));
        assert!(first.unverified.contains_key(&token_b));
        assert!(second.verified.is_empty());
        assert!(second.provenance.is_empty());
        assert!(second.unverified.contains_key(&token_b));
        assert!(resolver.cache.read().await.identities.is_empty());
        for token in [
            token_a,
            token_b,
            PolymarketTokenId("token-x".into()),
            PolymarketTokenId("token-z".into()),
        ] {
            let rejected = resolver.resolve_live([token.clone()]).await.unwrap();
            assert!(rejected.verified.is_empty());
            assert!(rejected.provenance.is_empty());
            assert!(rejected.unverified.contains_key(&token));
        }
        let urls = fetcher.urls.lock().unwrap();
        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains("clob_token_ids=token-a"));
        assert!(urls[1].contains("clob_token_ids=token-b"));
    }

    #[tokio::test(start_paused = true)]
    async fn memory_only_interleaved_requests_reject_conflicting_condition_and_cached_siblings() {
        assert_interleaved_identity_conflict_is_rejected(false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn durable_interleaved_requests_reject_conflicting_condition_and_cached_siblings() {
        assert_interleaved_identity_conflict_is_rejected(true).await;
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_closed_token_requests_share_completed_open_query() {
        for durable in [false, true] {
            for purpose in [LookupPurpose::Live, LookupPurpose::Historical] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("source.log");
                let fetcher = Arc::new(InterleavedIdentityFixture {
                    urls: StdMutex::new(Vec::new()),
                    release: tokio::sync::Notify::new(),
                    closed_only: true,
                });
                let resolver = if durable {
                    durable_resolver(
                        &path,
                        Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap()),
                        "installed",
                        fetcher.clone(),
                        1,
                    )
                } else {
                    AssetIdentityResolver::new(
                        fetcher.clone(),
                        BASE.into(),
                        1,
                        Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
                    )
                };
                let token = PolymarketTokenId("token-b".into());
                let mut first = Box::pin(resolver.resolve_inner([token.clone()], purpose));
                std::future::poll_fn(|cx| {
                    assert!(first.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                let mut second = Box::pin(resolver.resolve_inner([token.clone()], purpose));
                std::future::poll_fn(|cx| {
                    assert!(second.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                assert_eq!(fetcher.urls.lock().unwrap().len(), 1);
                fetcher.release.notify_one();
                let (first, second) = tokio::join!(first, second);
                let first = first.unwrap();
                let second = second.unwrap();
                assert_eq!(first.verified.len(), 1);
                assert_eq!(second.verified, first.verified);
                assert_eq!(second.provenance, first.provenance);
                let urls = fetcher.urls.lock().unwrap();
                assert_eq!(urls.len(), 2);
                assert!(!urls[0].contains("closed=true"));
                assert!(urls[1].contains("closed=true"));
            }
        }
    }

    struct InterleavedConditionFixture {
        urls: StdMutex<Vec<String>>,
        release: tokio::sync::Notify,
        live_completed: std::sync::atomic::AtomicBool,
        closed_first: bool,
        empty_last: bool,
    }

    impl ReconciliationFetcher for InterleavedConditionFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                let url = reqwest::Url::parse(url).unwrap();
                let first_request = {
                    let mut urls = self.urls.lock().unwrap();
                    urls.push(url.to_string());
                    urls.len() == 1
                };
                let conditions = url
                    .query_pairs()
                    .filter(|(key, _)| key == "condition_ids")
                    .map(|(_, condition)| condition.into_owned())
                    .collect::<Vec<_>>();
                assert!(conditions.len() <= GAMMA_BATCH_SIZE);
                let closed = url.query_pairs().any(|(key, _)| key == "closed");
                let first = conditions
                    .iter()
                    .any(|condition| condition == "condition-000");
                if first && !closed {
                    if first_request {
                        self.release.notified().await;
                    }
                    if self.closed_first {
                        return Ok(b"[]".to_vec());
                    }
                } else if first
                    || conditions
                        .iter()
                        .any(|condition| condition == "condition-050")
                {
                    assert!(self.live_completed.load(Ordering::SeqCst));
                }
                let markets =
                    if conditions.is_empty() {
                        assert!(url.query_pairs().any(|(key, value)| {
                            key == "clob_token_ids" && value == "token-live"
                        }));
                        vec![serde_json::json!({
                            "conditionId": "condition-live",
                            "clobTokenIds": ["token-live"],
                        })]
                    } else {
                        conditions
                            .iter()
                            .filter(|condition| !self.empty_last || *condition != "condition-050")
                            .map(|condition| {
                                serde_json::json!({
                                    "conditionId": condition,
                                    "clobTokenIds": [condition.replace("condition-", "token-")],
                                })
                            })
                            .collect()
                    };
                serde_json::to_vec(&markets).map_err(identity_store_error)
            })
        }
    }

    /// PASS: overlapping discoveries share the completed open phase and the closed result.
    /// FAIL: a queued discovery repeats the empty open request or records its page again.
    #[tokio::test(start_paused = true)]
    async fn overlapping_closed_condition_discoveries_share_completed_open_query() {
        for durable in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let fetcher = Arc::new(InterleavedConditionFixture {
                urls: StdMutex::new(Vec::new()),
                release: tokio::sync::Notify::new(),
                live_completed: std::sync::atomic::AtomicBool::new(true),
                closed_first: true,
                empty_last: false,
            });
            let resolver = if durable {
                durable_resolver(
                    &path,
                    Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap()),
                    "installed",
                    fetcher.clone(),
                    1,
                )
            } else {
                AssetIdentityResolver::new(
                    fetcher.clone(),
                    BASE.into(),
                    1,
                    Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
                )
            };
            let condition = PolymarketConditionId("condition-000".into());
            let mut first = Box::pin(resolver.discover_conditions_for_bracket([condition.clone()]));
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let mut second = Box::pin(resolver.discover_conditions_for_bracket([condition]));
            std::future::poll_fn(|cx| {
                assert!(second.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(fetcher.urls.lock().unwrap().len(), 1);
            fetcher.release.notify_one();
            let (first, second) = tokio::join!(first, second);
            let first = first.unwrap();
            let second = second.unwrap();
            assert_eq!(first.verified.len(), 1);
            assert!(first.unverified.is_empty());
            assert_eq!(second.verified, first.verified);
            assert_eq!(second.provenance, first.provenance);
            assert_eq!(second.unverified, first.unverified);
            assert_eq!(
                *fetcher.urls.lock().unwrap(),
                [
                    format!("{BASE}/markets?condition_ids=condition-000&limit=500"),
                    format!("{BASE}/markets?condition_ids=condition-000&closed=true&limit=500"),
                ]
            );
            assert_eq!(Reader::replay(&path).unwrap().count(), 2);
        }
    }

    /// PASS: dropping a discovery after its empty open lookup lets a later caller fetch open.
    /// FAIL: unfinished condition progress outlives its caller or becomes a completed empty memo.
    #[tokio::test(start_paused = true)]
    async fn dropped_condition_discovery_does_not_share_completed_open_query() {
        for durable in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let fetcher = Arc::new(InterleavedConditionFixture {
                urls: StdMutex::new(Vec::new()),
                release: tokio::sync::Notify::new(),
                live_completed: std::sync::atomic::AtomicBool::new(true),
                closed_first: true,
                empty_last: false,
            });
            let resolver = if durable {
                durable_resolver(
                    &path,
                    Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap()),
                    "installed",
                    fetcher.clone(),
                    1,
                )
            } else {
                AssetIdentityResolver::new(
                    fetcher.clone(),
                    BASE.into(),
                    1,
                    Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
                )
            };
            let condition = PolymarketConditionId("condition-000".into());
            let mut first = Box::pin(resolver.discover_conditions_for_bracket([condition.clone()]));
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            // Queue a mutex waiter before the discovery can advance to its closed attempt.
            let mut waiting = Box::pin(resolver.misses.lock());
            std::future::poll_fn(|cx| {
                assert!(waiting.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            fetcher.release.notify_one();
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(fetcher.urls.lock().unwrap().len(), 1);
            assert_eq!(Reader::replay(&path).unwrap().count(), 1);
            assert_eq!(
                resolver.cache.read().await.completed_open_conditions[&condition.0].strong_count(),
                1
            );
            drop(first);
            assert_eq!(
                resolver.cache.read().await.completed_open_conditions[&condition.0].strong_count(),
                0
            );
            assert!(resolver.cache.read().await.discovered_conditions.is_empty());
            drop(waiting.await);
            let retried = resolver
                .discover_conditions_for_bracket([condition])
                .await
                .unwrap();
            assert_eq!(retried.verified.len(), 1);
            assert!(retried.unverified.is_empty());
            assert_eq!(
                *fetcher.urls.lock().unwrap(),
                [
                    format!("{BASE}/markets?condition_ids=condition-000&limit=500"),
                    format!("{BASE}/markets?condition_ids=condition-000&limit=500"),
                    format!("{BASE}/markets?condition_ids=condition-000&closed=true&limit=500"),
                ]
            );
            assert_eq!(Reader::replay(&path).unwrap().count(), 3);
        }
    }

    /// PASS: queued live work completes between discovery attempts; a queued discovery's
    /// completed condition and empty result are reused by the first discovery's later chunk.
    /// FAIL: discovery retains the FIFO mutex across attempts or refetches another caller's memo.
    #[tokio::test(start_paused = true)]
    async fn live_identity_lookup_completes_between_condition_discovery_chunks() {
        for durable in [false, true] {
            for closed_first in [false, true] {
                for overlapping_discovery in [false, true] {
                    let empty_last = closed_first && overlapping_discovery;
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("source.log");
                    let fetcher = Arc::new(InterleavedConditionFixture {
                        urls: StdMutex::new(Vec::new()),
                        release: tokio::sync::Notify::new(),
                        live_completed: std::sync::atomic::AtomicBool::new(false),
                        closed_first,
                        empty_last,
                    });
                    let resolver = if durable {
                        durable_resolver(
                            &path,
                            Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap()),
                            "installed",
                            fetcher.clone(),
                            GAMMA_BATCH_SIZE,
                        )
                    } else {
                        AssetIdentityResolver::new(
                            fetcher.clone(),
                            BASE.into(),
                            GAMMA_BATCH_SIZE,
                            Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
                        )
                    };
                    let conditions = (0..=GAMMA_BATCH_SIZE)
                        .map(|index| PolymarketConditionId(format!("condition-{index:03}")))
                        .collect::<Vec<_>>();
                    let mut discovery =
                        Box::pin(resolver.discover_conditions_for_bracket(conditions));
                    std::future::poll_fn(|cx| {
                        assert!(discovery.as_mut().poll(cx).is_pending());
                        std::task::Poll::Ready(())
                    })
                    .await;
                    let live_token = PolymarketTokenId("token-live".into());
                    let mut live = Box::pin(async {
                        let resolved = resolver.resolve_live([live_token.clone()]).await.unwrap();
                        fetcher.live_completed.store(true, Ordering::SeqCst);
                        resolved
                    });
                    // Explicit polling queues live work while the first condition chunk is gated.
                    std::future::poll_fn(|cx| {
                        assert!(live.as_mut().poll(cx).is_pending());
                        std::task::Poll::Ready(())
                    })
                    .await;
                    let mut queued = Vec::new();
                    if overlapping_discovery {
                        let mut waiting = Box::pin(resolver.discover_conditions_for_bracket([
                            PolymarketConditionId("condition-050".into()),
                        ]));
                        std::future::poll_fn(|cx| {
                            assert!(waiting.as_mut().poll(cx).is_pending());
                            std::task::Poll::Ready(())
                        })
                        .await;
                        queued.push(waiting);
                    }
                    assert_eq!(fetcher.urls.lock().unwrap().len(), 1);
                    fetcher.release.notify_one();
                    let (discovery, live, queued) =
                        tokio::join!(discovery, live, futures::future::join_all(queued));
                    let discovery = discovery.unwrap();
                    assert_eq!(
                        discovery.verified.len(),
                        GAMMA_BATCH_SIZE + usize::from(!empty_last)
                    );
                    assert!(discovery.unverified.is_empty());
                    assert_eq!(live.verified.len(), 1);
                    assert_eq!(live.provenance[&live_token].source_log_sequence, 1);
                    for completed in queued {
                        let completed = completed.unwrap();
                        let token = PolymarketTokenId("token-050".into());
                        assert_eq!(completed.verified.len(), usize::from(!empty_last));
                        if empty_last {
                            assert!(
                                resolver.cache.read().await.discovered_conditions["condition-050"]
                                    .is_empty()
                            );
                        } else {
                            assert_eq!(discovery.provenance[&token], completed.provenance[&token]);
                        }
                    }
                    let urls = fetcher.urls.lock().unwrap();
                    let expected = 3 + usize::from(closed_first) + usize::from(empty_last);
                    assert_eq!(urls.len(), expected);
                    assert!(urls[1].contains("clob_token_ids=token-live"));
                    assert_eq!(Reader::replay(&path).unwrap().count(), expected);
                }
            }
        }
    }

    struct InterleavedChunkFixture {
        urls: StdMutex<Vec<String>>,
        release: tokio::sync::Notify,
        live_completed: std::sync::atomic::AtomicBool,
        fail_first: bool,
    }

    impl ReconciliationFetcher for InterleavedChunkFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                let url = reqwest::Url::parse(url).unwrap();
                let tokens = url
                    .query_pairs()
                    .filter(|(key, _)| key == "clob_token_ids")
                    .map(|(_, token)| token.into_owned())
                    .collect::<Vec<_>>();
                assert!(tokens.len() <= GAMMA_BATCH_SIZE);
                assert!(!url.query_pairs().any(|(key, _)| key == "closed"));
                let attempt = {
                    let mut urls = self.urls.lock().unwrap();
                    let attempt = urls.len();
                    urls.push(url.to_string());
                    attempt
                };
                if tokens.iter().any(|token| token == "token-000") {
                    self.release.notified().await;
                    if self.fail_first && attempt == 0 {
                        return Err(SourceError::Transient {
                            message: "injected first chunk failure".into(),
                        });
                    }
                } else if tokens.iter().any(|token| token.ends_with("-050")) {
                    assert!(self.live_completed.load(Ordering::SeqCst));
                }
                serde_json::to_vec(
                    &tokens
                        .iter()
                        .map(|token| {
                            serde_json::json!({
                                "conditionId": format!("condition-{token}"),
                                "clobTokenIds": [token],
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .map_err(identity_store_error)
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn live_identity_lookup_completes_between_historical_chunks() {
        for (durable, queued_count) in [(false, 0), (true, 0), (true, 3)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let fetcher = Arc::new(InterleavedChunkFixture {
                urls: StdMutex::new(Vec::new()),
                release: tokio::sync::Notify::new(),
                live_completed: std::sync::atomic::AtomicBool::new(false),
                fail_first: false,
            });
            let resolver = if durable {
                durable_resolver(
                    &path,
                    Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap()),
                    "installed",
                    fetcher.clone(),
                    GAMMA_BATCH_SIZE,
                )
            } else {
                AssetIdentityResolver::new(
                    fetcher.clone(),
                    BASE.into(),
                    GAMMA_BATCH_SIZE,
                    Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
                )
            };
            let tokens = (0..=GAMMA_BATCH_SIZE)
                .map(|index| PolymarketTokenId(format!("token-{index:03}")))
                .collect::<Vec<_>>();
            let mut historical = Box::pin(resolver.resolve_historical_for_bracket(tokens.clone()));
            std::future::poll_fn(|cx| {
                assert!(historical.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let mut queued = Vec::new();
            for caller in 0..queued_count {
                let tokens = (0..=GAMMA_BATCH_SIZE)
                    .map(|index| PolymarketTokenId(format!("queued-{caller}-{index:03}")))
                    .collect::<Vec<_>>();
                let mut waiting = Box::pin(resolver.resolve_historical_for_bracket(tokens));
                std::future::poll_fn(|cx| {
                    assert!(waiting.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                queued.push(waiting);
            }
            let live_token = PolymarketTokenId("token-live".into());
            let mut live = Box::pin(async {
                let resolved = resolver.resolve_live([live_token.clone()]).await.unwrap();
                fetcher.live_completed.store(true, Ordering::SeqCst);
                resolved
            });
            // Polling queues this waiter while the first chunk still holds the mutex.
            std::future::poll_fn(|cx| {
                assert!(live.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(fetcher.urls.lock().unwrap().len(), 1);
            fetcher.release.notify_one();
            let (historical, queued, live) =
                tokio::join!(historical, futures::future::join_all(queued), live);
            let historical = historical.unwrap();
            for completed in queued {
                assert_eq!(completed.unwrap().verified.len(), tokens.len());
            }
            assert_eq!(historical.verified.len(), tokens.len());
            assert_eq!(live.verified.len(), 1);
            assert_eq!(
                live.provenance[&live_token].source_log_sequence,
                u64::try_from(queued_count + 1).unwrap()
            );
            assert_eq!(historical.provenance[&tokens[0]].source_log_sequence, 0);
            assert_eq!(
                historical.provenance[&tokens[GAMMA_BATCH_SIZE]].source_log_sequence,
                u64::try_from(queued_count + 2).unwrap()
            );
            let urls = fetcher.urls.lock().unwrap();
            assert_eq!(urls.len(), 3 + 2 * queued_count);
            assert!(urls[queued_count + 1].contains("clob_token_ids=token-live"));
            assert!(urls[queued_count + 2].contains("clob_token_ids=token-050"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn live_identity_lookup_does_not_wait_for_queued_bracket_retry_backoff() {
        for durable in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let fetcher = Arc::new(InterleavedChunkFixture {
                urls: StdMutex::new(Vec::new()),
                release: tokio::sync::Notify::new(),
                live_completed: std::sync::atomic::AtomicBool::new(false),
                fail_first: true,
            });
            let resolver = if durable {
                durable_resolver(
                    &path,
                    paper.clone(),
                    "installed",
                    fetcher.clone(),
                    GAMMA_BATCH_SIZE,
                )
            } else {
                AssetIdentityResolver::new(
                    fetcher.clone(),
                    BASE.into(),
                    GAMMA_BATCH_SIZE,
                    Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
                )
            };
            let start = tokio::time::Instant::now();
            let tokens = (0..=GAMMA_BATCH_SIZE)
                .map(|index| PolymarketTokenId(format!("token-{index:03}")))
                .collect::<Vec<_>>();
            let mut historical = Box::pin(resolver.resolve_historical_for_bracket(tokens.clone()));
            std::future::poll_fn(|cx| {
                assert!(historical.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let mut queued = Vec::new();
            for caller in 0..3 {
                let tokens = (0..=GAMMA_BATCH_SIZE)
                    .map(|index| PolymarketTokenId(format!("queued-{caller}-{index:03}")))
                    .collect::<Vec<_>>();
                let mut waiting = Box::pin(resolver.resolve_historical_for_bracket(tokens));
                std::future::poll_fn(|cx| {
                    assert!(waiting.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                queued.push(waiting);
            }
            let live_token = PolymarketTokenId("token-live".into());
            let mut live = Box::pin(async {
                let resolved = resolver.resolve_live([live_token.clone()]).await.unwrap();
                assert_eq!(tokio::time::Instant::now(), start);
                assert_eq!(fetcher.urls.lock().unwrap().len(), 5);
                fetcher.live_completed.store(true, Ordering::SeqCst);
                fetcher.release.notify_one();
                resolved
            });
            // All four bracket callers are ahead of live; the failed attempt must yield
            // without holding the mutex through its first backoff.
            std::future::poll_fn(|cx| {
                assert!(live.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            fetcher.release.notify_one();
            let (historical, queued, live) =
                tokio::join!(historical, futures::future::join_all(queued), live);
            let historical = historical.unwrap();
            assert_eq!(historical.verified.len(), tokens.len());
            for completed in queued {
                assert_eq!(completed.unwrap().verified.len(), tokens.len());
            }
            assert_eq!(live.verified.len(), 1);
            assert_eq!(
                tokio::time::Instant::now() - start,
                std::time::Duration::from_secs(2)
            );
            {
                let urls = fetcher.urls.lock().unwrap();
                assert_eq!(urls.len(), 10);
                assert!(urls[0].contains("clob_token_ids=token-000"));
                for caller in 0..3 {
                    assert!(
                        urls[caller + 1].contains(&format!("clob_token_ids=queued-{caller}-000"))
                    );
                }
                assert!(urls[4].contains("clob_token_ids=token-live"));
                assert_eq!(urls[0], urls[8]);
            }
            if durable {
                let saved = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
                let rows = saved.asset_identities("installed", &tokens).unwrap();
                assert_eq!(rows.len(), tokens.len());
                for row in rows {
                    assert_eq!(
                        u64::try_from(row.source_log_sequence).unwrap(),
                        historical.provenance[&row.token].source_log_sequence
                    );
                    assert_eq!(
                        row.canonical_page_hash,
                        historical.provenance[&row.token].canonical_page_hash
                    );
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bracket_retry_rechecks_table_for_identity_saved_during_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(InterleavedChunkFixture {
            urls: StdMutex::new(Vec::new()),
            release: tokio::sync::Notify::new(),
            live_completed: std::sync::atomic::AtomicBool::new(false),
            fail_first: true,
        });
        let resolver =
            durable_resolver(&path, paper, "installed", fetcher.clone(), GAMMA_BATCH_SIZE);
        let token = PolymarketTokenId("token-000".into());
        let start = tokio::time::Instant::now();
        let mut first = Box::pin(resolver.resolve_historical_for_bracket([token.clone()]));
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let mut second = Box::pin(resolver.resolve_live([token.clone()]));
        std::future::poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        fetcher.release.notify_one();
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            assert!(second.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(fetcher.urls.lock().unwrap().len(), 2);
        fetcher.release.notify_one();
        let saved = second.await.unwrap();
        assert_eq!(tokio::time::Instant::now(), start);
        resolver.cache.write().await.identities.clear();
        let retried = first.await.unwrap();
        assert_eq!(retried.provenance, saved.provenance);
        assert_eq!(retried.verified[&token], saved.verified[&token]);
        assert_eq!(fetcher.urls.lock().unwrap().len(), 2);
        assert_eq!(
            tokio::time::Instant::now() - start,
            std::time::Duration::from_secs(2)
        );
    }

    struct CrossConditionFixture;

    impl ReconciliationFetcher for CrossConditionFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                if url.contains("clob_token_ids=token-a") {
                    Ok(
                        br#"[{"conditionId":"condition-c","clobTokenIds":["token-a","token-x"]}]"#
                            .to_vec(),
                    )
                } else {
                    Ok(
                        br#"[{"conditionId":"condition-d","clobTokenIds":["token-a","token-b"]}]"#
                            .to_vec(),
                    )
                }
            })
        }
    }

    #[tokio::test]
    async fn durable_cross_condition_overlap_rejects_both_conditions_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let tokens = [
            PolymarketTokenId("token-a".into()),
            PolymarketTokenId("token-b".into()),
            PolymarketTokenId("token-x".into()),
        ];
        let resolver = durable_resolver(
            &path,
            paper.clone(),
            "installed",
            Arc::new(CrossConditionFixture),
            50,
        );
        let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        drop(resolver);
        let resolver = durable_resolver(
            &path,
            paper.clone(),
            "installed",
            Arc::new(CrossConditionFixture),
            50,
        );
        let conflicting = resolver.resolve_live([tokens[1].clone()]).await.unwrap();
        assert!(conflicting.verified.is_empty());
        assert!(
            tokens[..2]
                .iter()
                .all(|token| conflicting.unverified.contains_key(token))
        );
        let rows = paper.asset_identities("installed", &tokens).unwrap();
        assert_eq!(rows.len(), 3);
        for condition in rows.iter().map(|row| &row.condition_id.0) {
            assert!(
                paper
                    .asset_identity_condition_rejection("installed", condition)
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(rows[0].condition_id.0, "condition-c");
        assert_eq!(
            rows[0].canonical_page_hash,
            original.provenance[&tokens[0]].canonical_page_hash
        );
        drop(resolver);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        for token in tokens {
            let result = resolver.resolve_live([token.clone()]).await.unwrap();
            assert!(result.verified.is_empty());
            assert!(result.unverified.contains_key(&token));
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn durable_rejection_without_a_token_row_blocks_condition_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let resolver = durable_resolver(
            &path,
            paper.clone(),
            "installed",
            Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition-c","clobTokenIds":["token-a"]}]"#,
                b"[]",
            )),
            50,
        );
        resolver.resolve_live([token.clone()]).await.unwrap();
        drop(resolver);
        let resolver = durable_resolver(
            &path,
            paper.clone(),
            "installed",
            Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition-d","clobTokenIds":["token-a"]}]"#,
                b"[]",
            )),
            50,
        );
        // A different requested miss exposes D:[A], whose entire token set already belongs to C.
        let result = resolver
            .resolve_live([PolymarketTokenId("probe".into())])
            .await
            .unwrap();
        assert!(result.verified.is_empty());
        for condition in ["condition-c", "condition-d"] {
            assert_eq!(
                paper
                    .asset_identity_condition_rejection("installed", condition)
                    .unwrap(),
                Some(1)
            );
        }
        assert!(
            paper
                .asset_identities_by_condition("installed", &BTreeSet::from(["condition-d".into()]))
                .unwrap()
                .is_empty()
        );
        drop(resolver);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-d","clobTokenIds":["token-b"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let saved = resolver.resolve_live([token.clone()]).await.unwrap();
        assert!(saved.verified.is_empty());
        assert!(saved.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        let token = PolymarketTokenId("token-b".into());
        let later = resolver.resolve_live([token.clone()]).await.unwrap();
        assert!(later.verified.is_empty());
        assert!(later.unverified.contains_key(&token));
        assert!(later.provenance.is_empty());
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            paper
                .asset_identity_condition_rejection("installed", "condition-d")
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn conflicting_markets_in_one_page_persist_rejections_without_token_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let resolver = durable_resolver(&path, paper.clone(), "installed", Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-c","clobTokenIds":["token-a"]},{"conditionId":"condition-d","clobTokenIds":["token-a"]}]"#, b"[]",
        )), 50);
        let token = PolymarketTokenId("token-a".into());
        let result = resolver.resolve_live([token.clone()]).await.unwrap();
        assert!(result.verified.is_empty());
        assert!(result.unverified.contains_key(&token));
        assert!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        for condition in ["condition-c", "condition-d"] {
            assert_eq!(
                paper
                    .asset_identity_condition_rejection("installed", condition)
                    .unwrap(),
                Some(0)
            );
        }
        drop(resolver);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-d","clobTokenIds":["token-b"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let historical = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert!(historical.verified.is_empty());
        assert!(historical.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-e","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        for _ in 0..2 {
            let live = resolver.resolve_live([token.clone()]).await.unwrap();
            assert!(live.verified.is_empty());
            assert_eq!(live.unverified[&token], rejected_identity_reason());
            assert!(live.provenance.is_empty());
        }
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        assert!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-d","clobTokenIds":["token-b"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let later = resolver
            .resolve_live([PolymarketTokenId("token-b".into())])
            .await
            .unwrap();
        assert!(later.verified.is_empty());
        assert_eq!(later.unverified.len(), 1);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        drop(resolver);
        // An unusable new token in a previously rejected condition also has no row.
        let token = PolymarketTokenId("token-c".into());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-d","clobTokenIds":["token-c","token-c"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let malformed = resolver.resolve_live([token.clone()]).await.unwrap();
        assert!(malformed.verified.is_empty());
        assert!(malformed.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        let historical = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert!(historical.verified.is_empty());
        assert!(historical.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn durable_rejected_token_in_fresh_sibling_page_rejects_condition_across_restarts() {
        for purpose in [LookupPurpose::Live, LookupPurpose::Historical] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let db_path = dir.path().join("paper.db");
            let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
            let tokens = [
                PolymarketTokenId("token-a".into()),
                PolymarketTokenId("token-y".into()),
            ];
            let resolver = durable_resolver(&path, paper.clone(), "installed", Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition-c","clobTokenIds":["token-a"]},{"conditionId":"condition-d","clobTokenIds":["token-a"]}]"#, b"[]",
            )), 50);
            let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
            assert!(original.verified.is_empty());
            assert!(original.unverified.contains_key(&tokens[0]));
            assert!(
                paper
                    .asset_identities("installed", &tokens)
                    .unwrap()
                    .is_empty()
            );
            drop(resolver);
            drop(paper);

            let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
            let fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition-e","clobTokenIds":["token-a","token-y"]}]"#,
                b"[]",
            ));
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
            let sibling = match purpose {
                LookupPurpose::Live => resolver.resolve_live([tokens[1].clone()]).await,
                LookupPurpose::Historical => {
                    resolver
                        .resolve_historical_for_bracket([tokens[1].clone()])
                        .await
                }
            }
            .unwrap();
            assert!(sibling.verified.is_empty());
            assert_eq!(sibling.unverified[&tokens[1]], rejected_identity_reason());
            assert!(sibling.provenance.is_empty());
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                paper
                    .asset_identity_condition_rejection("installed", "condition-e")
                    .unwrap(),
                Some(1)
            );
            assert!(
                paper
                    .asset_identities("installed", &tokens)
                    .unwrap()
                    .is_empty()
            );
            for token in &tokens {
                let live = resolver.resolve_live([token.clone()]).await.unwrap();
                assert!(live.verified.is_empty());
                assert_eq!(live.unverified[token], rejected_identity_reason());
                assert!(live.provenance.is_empty());
            }
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
            drop(resolver);
            drop(paper);

            let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
            let fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition-f","clobTokenIds":["token-y"]}]"#,
                b"[]",
            ));
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
            let historical = resolver
                .resolve_historical_for_bracket([tokens[1].clone()])
                .await
                .unwrap();
            let live = resolver.resolve_live([tokens[1].clone()]).await.unwrap();
            for result in [historical, live] {
                assert!(result.verified.is_empty());
                assert_eq!(result.unverified[&tokens[1]], rejected_identity_reason());
                assert!(result.provenance.is_empty());
            }
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
            assert!(
                paper
                    .asset_identities("installed", &tokens)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn cross_condition_conflict_immediately_evicts_cached_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition-c","clobTokenIds":["token-a","token-b"]}]"#.to_vec(),
                br#"[{"conditionId":"condition-d","clobTokenIds":["token-a"]}]"#.to_vec(),
                b"[]".to_vec(),
            ])),
        });
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher, 50);
        let sibling = PolymarketTokenId("token-b".into());
        let first = resolver
            .resolve_live([PolymarketTokenId("token-a".into())])
            .await
            .unwrap();
        assert_eq!(first.verified.len(), 1);
        assert!(
            resolver
                .cache
                .read()
                .await
                .identities
                .contains_key(&sibling)
        );
        let conflicting = resolver
            .resolve_live([sibling.clone(), PolymarketTokenId("probe".into())])
            .await
            .unwrap();
        assert!(conflicting.verified.is_empty());
        assert!(conflicting.unverified.contains_key(&sibling));
        assert!(resolver.cache.read().await.identities.is_empty());
        let cached = resolver.resolve_live([sibling.clone()]).await.unwrap();
        assert!(cached.verified.is_empty());
        assert!(cached.unverified.contains_key(&sibling));
        for condition in ["condition-c", "condition-d"] {
            assert_eq!(
                paper
                    .asset_identity_condition_rejection("installed", condition)
                    .unwrap(),
                Some(1)
            );
        }
    }

    struct SequenceFixture {
        pages: StdMutex<std::collections::VecDeque<Vec<u8>>>,
    }

    impl ReconciliationFetcher for SequenceFixture {
        fn fetch<'a>(
            &'a self,
            _url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move { Ok(self.pages.lock().unwrap().pop_front().unwrap()) })
        }
    }

    #[tokio::test]
    async fn memory_only_cross_chunk_ambiguity_evicts_earlier_progress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#.to_vec(),
                br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]}]"#.to_vec(),
            ])),
        });
        let resolver = AssetIdentityResolver::new(
            fetcher,
            BASE.into(),
            1,
            Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap())),
        );
        let tokens = [
            PolymarketTokenId("token-a".into()),
            PolymarketTokenId("token-b".into()),
        ];
        let resolved = resolver.resolve_live(tokens.clone()).await.unwrap();
        assert!(resolved.verified.is_empty());
        assert!(resolved.provenance.is_empty());
        assert!(
            tokens
                .iter()
                .all(|token| resolved.unverified.contains_key(token))
        );
        assert!(resolver.cache.read().await.identities.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn memory_only_unrequested_conflict_rejects_conditions_and_evicts_cached_sibling() {
        let fetcher = Arc::new(SequenceFixture {
            pages: StdMutex::new(std::collections::VecDeque::from([
                br#"[{"conditionId":"condition-c","clobTokenIds":["token-a","token-x"]}]"#.to_vec(),
                br#"[{"conditionId":"condition-d","clobTokenIds":["token-z","token-a"]}]"#.to_vec(),
                b"[]".to_vec(),
                br#"[{"conditionId":"condition-d","clobTokenIds":["token-b"]}]"#.to_vec(),
            ])),
        });
        let (_dir, _path, _sink, resolver) = boot_resolver(fetcher.clone());
        let sibling = PolymarketTokenId("token-x".into());
        assert_eq!(
            resolver
                .resolve_live([sibling.clone()])
                .await
                .unwrap()
                .verified
                .len(),
            1
        );
        let conflicting = resolver
            .resolve_live([sibling.clone(), PolymarketTokenId("probe".into())])
            .await
            .unwrap();
        assert!(conflicting.verified.is_empty());
        assert!(conflicting.unverified.contains_key(&sibling));
        assert!(resolver.cache.read().await.identities.is_empty());
        for token in [
            sibling,
            PolymarketTokenId("token-a".into()),
            PolymarketTokenId("token-z".into()),
            PolymarketTokenId("token-b".into()),
        ] {
            let rejected = resolver.resolve_live([token.clone()]).await.unwrap();
            assert!(rejected.verified.is_empty());
            assert!(rejected.provenance.is_empty());
            assert!(rejected.unverified.contains_key(&token));
        }
        assert!(fetcher.pages.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn same_boot_conflicts_use_unindexed_pages_and_persist_after_restart() {
        for cross_condition in [false, true] {
            for extend_before_drop in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("source.log");
                let db_path = dir.path().join("paper.db");
                let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
                let fetcher: Arc<dyn ReconciliationFetcher> = if cross_condition {
                    Arc::new(CrossConditionFixture)
                } else {
                    Arc::new(SiblingConflictFixture)
                };
                let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher, 1);
                let receipts = resolver
                    .store
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .source_receipts
                    .clone();
                let tokens = [
                    PolymarketTokenId("token-a".into()),
                    PolymarketTokenId("token-b".into()),
                ];
                let first = resolver
                    .resolve_historical_for_bracket([tokens[0].clone()])
                    .await
                    .unwrap();
                assert!(first.verified.contains_key(&tokens[0]));
                assert_eq!(
                    paper.asset_identities("installed", &tokens).unwrap().len(),
                    1
                );
                assert!(receipts.receipt_at(EventSeq(0)).unwrap().is_none());
                paper.begin_batch().unwrap();
                paper.rollback_batch().unwrap();
                let conflicting = resolver
                    .resolve_historical_for_bracket([tokens[1].clone()])
                    .await
                    .unwrap();
                assert!(conflicting.verified.is_empty());
                assert!(
                    tokens
                        .iter()
                        .all(|token| conflicting.unverified.contains_key(token))
                );
                let conditions = if cross_condition {
                    vec!["condition-c", "condition-d"]
                } else {
                    vec!["condition"]
                };
                for condition in conditions {
                    assert_eq!(
                        paper
                            .asset_identity_condition_rejection("installed", condition)
                            .unwrap(),
                        Some(1)
                    );
                }
                assert_eq!(resolver.cache.read().await.boot_pages.len(), 2);
                assert!(receipts.receipt_at(EventSeq(1)).unwrap().is_none());
                if extend_before_drop {
                    receipts.catch_up_verified_tail().unwrap();
                    resolver.release_indexed_boot_pages().await;
                    assert!(resolver.cache.read().await.boot_pages.is_empty());
                }
                // Also drop the entire process state before brackets finish or boot.extend runs.
                drop(resolver);
                drop(paper);
                let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
                let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
                let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
                for token in tokens {
                    let result = resolver.resolve_live([token.clone()]).await.unwrap();
                    assert!(result.verified.is_empty());
                    assert!(result.unverified.contains_key(&token));
                }
                assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[tokio::test]
    async fn durable_absence_restart_blocks_historical_but_live_discovers_listing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let result = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert!(result.verified.is_empty());
        assert!(result.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
        // Both pages have the same canonical hash; the marker still names the final receipt.
        assert_eq!(
            paper
                .absent_asset_tokens("installed", std::slice::from_ref(&token))
                .unwrap(),
            BTreeMap::from([(token.clone(), 1)])
        );
        drop(resolver);
        drop(paper);

        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let historical = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert!(historical.verified.is_empty());
        assert!(historical.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        let live = resolver.resolve_live([token.clone()]).await.unwrap();
        assert_eq!(live.verified[&token].condition_id.0, "condition-a");
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap()
                .len(),
            1
        );
        drop(resolver);

        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let historical = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert_eq!(historical.verified, live.verified);
        assert_eq!(historical.provenance, live.provenance);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            paper
                .absent_asset_tokens("installed", &[token])
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn durable_absence_foreign_generation_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let old = durable_resolver(
            &path,
            paper.clone(),
            "old",
            Arc::new(GammaFixture::new(b"[]", b"[]")),
            50,
        );
        old.resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        drop(old);
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "current", fetcher.clone(), 50);
        let result = resolver
            .resolve_historical_for_bracket([token.clone()])
            .await
            .unwrap();
        assert!(result.verified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert!(
            paper
                .absent_asset_tokens("current", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        assert_eq!(paper.absent_asset_tokens("old", &[token]).unwrap().len(), 1);
    }

    struct FailedAbsenceFixture {
        fail_closed: bool,
    }

    impl ReconciliationFetcher for FailedAbsenceFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                if url.contains("closed=true") == self.fail_closed {
                    Err(SourceError::Transient {
                        message: "injected lookup failure".into(),
                    })
                } else {
                    Ok(b"[]".to_vec())
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_identity_lookup_saves_no_absence_markers() {
        for fail_closed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let db_path = dir.path().join("paper.db");
            let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
            let token = PolymarketTokenId("token-a".into());
            let resolver = durable_resolver(
                &path,
                paper.clone(),
                "installed",
                Arc::new(FailedAbsenceFixture { fail_closed }),
                50,
            );
            assert!(matches!(
                resolver
                    .resolve_historical_for_bracket([token.clone()])
                    .await,
                Err(SourceError::Transient { .. })
            ));
            assert!(
                paper
                    .absent_asset_tokens("installed", std::slice::from_ref(&token))
                    .unwrap()
                    .is_empty()
            );
            drop(resolver);
            drop(paper);
            let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
            let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
            let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
            resolver
                .resolve_historical_for_bracket([token])
                .await
                .unwrap();
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn durable_absence_write_waits_for_batch_and_survives_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        paper.begin_batch().unwrap();
        let mut resolving = Box::pin(resolver.resolve_historical_for_bracket([token.clone()]));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), resolving.as_mut())
                .await
                .is_err()
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
        assert!(
            paper
                .absent_asset_tokens("installed", std::slice::from_ref(&token))
                .unwrap()
                .is_empty()
        );
        paper.rollback_batch().unwrap();
        let result = resolving.await.unwrap();
        assert!(result.verified.is_empty());
        assert!(result.unverified.contains_key(&token));
        assert_eq!(
            paper
                .absent_asset_tokens("installed", std::slice::from_ref(&token))
                .unwrap(),
            BTreeMap::from([(token.clone(), 1)])
        );
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        resolver
            .resolve_historical_for_bracket([token])
            .await
            .unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn durable_restart_hit_retains_original_provenance_without_gamma_requests() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a","token-b"]}]"#,
            b"[]",
        ));
        let tokens = [
            PolymarketTokenId("token-a".into()),
            PolymarketTokenId("token-b".into()),
        ];
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let original = resolver.resolve_live(tokens.clone()).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            paper.asset_identities("installed", &tokens).unwrap().len(),
            2
        );
        drop(resolver);
        drop(paper);

        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        let restored = resolver
            .resolve_historical_for_bracket(tokens.clone())
            .await
            .unwrap();
        assert_eq!(restored.verified, original.verified);
        assert_eq!(restored.provenance, original.provenance);
        assert_eq!(resolver.cache.read().await.identities.len(), 2);
        assert_eq!(
            resolver.resolve_live(tokens).await.unwrap().provenance,
            original.provenance
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn identity_write_waits_for_batch_end_and_survives_rollback() {
        for rollback in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
                b"[]",
            ));
            let token = PolymarketTokenId("token-a".into());
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
            paper.begin_batch().unwrap();
            let mut resolving = Box::pin(resolver.resolve_historical_for_bracket([token.clone()]));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(25), resolving.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
            assert!(
                paper
                    .asset_identities("installed", std::slice::from_ref(&token))
                    .unwrap()
                    .is_empty()
            );
            if rollback {
                paper.rollback_batch().unwrap();
            } else {
                paper.commit_batch().unwrap();
            }
            let original = resolving.await.unwrap();
            assert!(original.verified.contains_key(&token));
            let saved = paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(
                saved[0].source_log_sequence,
                i64::try_from(original.provenance[&token].source_log_sequence).unwrap()
            );
            paper.begin_batch().unwrap();
            paper.rollback_batch().unwrap();
            drop(resolver);
            let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
            let restarted = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
            assert_eq!(
                restarted.resolve_live([token]).await.unwrap().provenance,
                original.provenance
            );
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn identity_write_times_out_without_acknowledging_or_caching() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let token = PolymarketTokenId("token-a".into());
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher, 50);
        paper.begin_batch().unwrap();
        let start = tokio::time::Instant::now();
        assert!(matches!(resolver.resolve_live([token.clone()]).await,
            Err(SourceError::Fatal { message }) if message.contains("waited 30 s")));
        assert_eq!(
            tokio::time::Instant::now().duration_since(start).as_secs(),
            30
        );
        assert!(resolver.cache.read().await.identities.is_empty());
        assert!(
            paper
                .asset_identities("installed", &[token])
                .unwrap()
                .is_empty()
        );
        paper.rollback_batch().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn durable_invalid_row_cleanup_waits_for_batch_end_before_refetch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let invalid = AssetIdentityRow {
            token: token.clone(),
            condition_id: pe_core_types::PolymarketConditionId("condition-a".into()),
            outcome: 0,
            source_log_sequence: 999,
            canonical_page_hash: "0".repeat(64),
        };
        paper
            .insert_asset_identities("installed", std::slice::from_ref(&invalid))
            .unwrap();
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        paper.begin_batch().unwrap();
        let mut resolving = Box::pin(resolver.resolve_historical_for_bracket([token.clone()]));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), resolving.as_mut())
                .await
                .is_err()
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            paper
                .asset_identities("installed", std::slice::from_ref(&token))
                .unwrap(),
            [invalid]
        );
        paper.rollback_batch().unwrap();
        let resolved = resolving.await.unwrap();
        assert!(resolved.verified.contains_key(&token));
        let saved = paper
            .asset_identities("installed", std::slice::from_ref(&token))
            .unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(
            saved[0].canonical_page_hash,
            resolved.provenance[&token].canonical_page_hash
        );
        assert_eq!(saved[0].source_log_sequence, 0);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        drop(resolver);
        let fetcher = Arc::new(GammaFixture::new(b"[]", b"[]"));
        let resolver = durable_resolver(&path, paper, "installed", fetcher.clone(), 50);
        assert_eq!(
            resolver.resolve_live([token]).await.unwrap().provenance,
            resolved.provenance
        );
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
    }

    struct SiblingConflictFixture;

    impl ReconciliationFetcher for SiblingConflictFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                if url.contains("clob_token_ids=token-a") {
                    Ok(
                        br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-x"]}]"#
                            .to_vec(),
                    )
                } else {
                    Ok(br#"[{"conditionId":"condition","clobTokenIds":["token-z","token-b"]},{"conditionId":"independent","clobTokenIds":["token-independent"]}]"#.to_vec())
                }
            })
        }
    }

    #[tokio::test]
    async fn durable_unsaved_sibling_ambiguity_rejects_condition_and_keeps_independent_progress() {
        for restart in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let tokens = [
                PolymarketTokenId("token-a".into()),
                PolymarketTokenId("token-b".into()),
            ];
            let mut resolver = durable_resolver(
                &path,
                paper.clone(),
                "installed",
                Arc::new(SiblingConflictFixture),
                50,
            );
            resolver.resolve_live([tokens[0].clone()]).await.unwrap();
            if restart {
                drop(resolver);
                resolver = durable_resolver(
                    &path,
                    paper.clone(),
                    "installed",
                    Arc::new(SiblingConflictFixture),
                    50,
                );
            }
            let independent = PolymarketTokenId("token-independent".into());
            let resolved = resolver
                .resolve_live([tokens[0].clone(), tokens[1].clone(), independent.clone()])
                .await
                .unwrap();
            assert_eq!(resolved.verified.keys().collect::<Vec<_>>(), [&independent]);
            assert!(
                tokens
                    .iter()
                    .all(|token| resolved.unverified.contains_key(token))
            );
            assert_eq!(resolver.cache.read().await.identities.len(), 1);
            let rows = paper.asset_identities("installed", &tokens).unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].outcome, 0);
            assert_eq!(rows[1].outcome, 1);
            assert_eq!(
                paper
                    .asset_identity_condition_rejection("installed", "condition")
                    .unwrap(),
                Some(1)
            );
            assert_eq!(
                paper
                    .asset_identity_condition_rejection("installed", "independent")
                    .unwrap(),
                None
            );
        }
    }

    #[tokio::test]
    async fn durable_condition_rejection_survives_restart_and_cannot_be_unrejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let db_path = dir.path().join("paper.db");
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let tokens = [
            PolymarketTokenId("token-a".into()),
            PolymarketTokenId("token-b".into()),
        ];
        let resolver = durable_resolver(
            &path,
            paper.clone(),
            "installed",
            Arc::new(SiblingConflictFixture),
            50,
        );
        let original = resolver.resolve_live([tokens[0].clone()]).await.unwrap();
        resolver.resolve_live([tokens[1].clone()]).await.unwrap();
        drop(resolver);
        drop(paper);
        let paper = Arc::new(PaperStateDb::open(&db_path).unwrap());
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition","clobTokenIds":["token-a","token-b","token-new"]}]"#,
            b"[]",
        ));
        let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 50);
        let restored = resolver.resolve_live(tokens.clone()).await.unwrap();
        assert!(restored.verified.is_empty());
        assert!(restored.provenance.is_empty());
        assert_eq!(restored.unverified.len(), 2);
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 0);
        let token = PolymarketTokenId("token-new".into());
        let fresh = resolver.resolve_live([token.clone()]).await.unwrap();
        assert!(fresh.verified.is_empty());
        assert!(fresh.unverified.contains_key(&token));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert!(resolver.cache.read().await.identities.is_empty());
        let rows = paper
            .asset_identities("installed", &[tokens[0].clone(), tokens[1].clone(), token])
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| &row.token).collect::<Vec<_>>(),
            tokens.iter().collect::<Vec<_>>()
        );
        for condition in rows.iter().map(|row| &row.condition_id.0) {
            assert!(
                paper
                    .asset_identity_condition_rejection("installed", condition)
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            rows[0].source_log_sequence,
            i64::try_from(original.provenance[&tokens[0]].source_log_sequence).unwrap()
        );
    }

    struct PartialFailureFixture {
        urls: StdMutex<Vec<String>>,
        fail_second: std::sync::atomic::AtomicBool,
        closed: bool,
    }

    impl ReconciliationFetcher for PartialFailureFixture {
        fn fetch<'a>(
            &'a self,
            url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async move {
                self.urls.lock().unwrap().push(url.to_owned());
                if self.closed && !url.contains("closed=true") {
                    return Ok(b"[]".to_vec());
                }
                let second = url.contains("clob_token_ids=token-b");
                if second && self.fail_second.load(Ordering::SeqCst) {
                    return Err(SourceError::Transient {
                        message: "failed second chunk".into(),
                    });
                }
                let suffix = if second { "b" } else { "a" };
                Ok(format!(
                    r#"[{{"conditionId":"condition-{suffix}","clobTokenIds":["token-{suffix}"]}}]"#
                )
                .into_bytes())
            })
        }
    }

    #[tokio::test]
    async fn durable_partial_failure_saves_first_chunk_and_restart_fetches_only_failed_chunk() {
        for closed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let fetcher = Arc::new(PartialFailureFixture {
                urls: StdMutex::new(Vec::new()),
                fail_second: std::sync::atomic::AtomicBool::new(true),
                closed,
            });
            let tokens = [
                PolymarketTokenId("token-a".into()),
                PolymarketTokenId("token-b".into()),
            ];
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 1);
            assert!(matches!(resolver.resolve_live(tokens.clone()).await,
                Err(SourceError::Transient { message }) if message.contains("failed second chunk")));
            let saved = paper.asset_identities("installed", &tokens).unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].token, tokens[0]);
            let provenance = IdentityProvenance {
                asset: saved[0].token.clone(),
                source_log_sequence: u64::try_from(saved[0].source_log_sequence).unwrap(),
                canonical_page_hash: saved[0].canonical_page_hash.clone(),
            };
            drop(resolver);
            fetcher.fail_second.store(false, Ordering::SeqCst);
            fetcher.urls.lock().unwrap().clear();
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher.clone(), 1);
            let retried = resolver.resolve_live(tokens.clone()).await.unwrap();
            assert_eq!(retried.verified.len(), 2);
            assert_eq!(retried.provenance[&tokens[0]], provenance);
            let urls = fetcher.urls.lock().unwrap();
            assert_eq!(urls.len(), if closed { 2 } else { 1 });
            assert!(
                urls.iter()
                    .all(|url| url.contains("clob_token_ids=token-b"))
            );
            assert_eq!(
                paper.asset_identities("installed", &tokens).unwrap().len(),
                2
            );
        }
    }

    #[tokio::test]
    async fn durable_foreign_generation_is_ignored_and_current_identity_is_saved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let token = PolymarketTokenId("token-a".into());
        let old_fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"old-condition","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let old = durable_resolver(&path, paper.clone(), "old", old_fetcher, 50);
        old.resolve_live([token.clone()]).await.unwrap();
        drop(old);
        let current_fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"current-condition","clobTokenIds":["token-a"]}]"#,
            b"[]",
        ));
        let current =
            durable_resolver(&path, paper.clone(), "current", current_fetcher.clone(), 50);
        let resolved = current.resolve_live([token.clone()]).await.unwrap();
        assert_eq!(
            resolved.verified[&token].condition_id.0,
            "current-condition"
        );
        assert_eq!(current_fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            paper
                .asset_identities("old", std::slice::from_ref(&token))
                .unwrap()[0]
                .condition_id
                .0,
            "old-condition"
        );
        assert_eq!(
            paper.asset_identities("current", &[token]).unwrap()[0]
                .condition_id
                .0,
            "current-condition"
        );
    }

    #[tokio::test]
    async fn durable_cross_run_ambiguity_marks_condition_rejected_and_rejects_both_tokens() {
        for request_saved in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let tokens = [
                PolymarketTokenId("token-a".into()),
                PolymarketTokenId("token-b".into()),
            ];
            let old_fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition","clobTokenIds":["token-a"]}]"#,
                b"[]",
            ));
            let old = durable_resolver(&path, paper.clone(), "installed", old_fetcher, 50);
            old.resolve_live([tokens[0].clone()]).await.unwrap();
            drop(old);
            let fetcher = Arc::new(GammaFixture::new(
                br#"[{"conditionId":"condition","clobTokenIds":["token-b"]}]"#,
                b"[]",
            ));
            let resolver = durable_resolver(&path, paper.clone(), "installed", fetcher, 50);
            let requested = if request_saved {
                tokens.to_vec()
            } else {
                vec![tokens[1].clone()]
            };
            let resolved = resolver.resolve_live(requested).await.unwrap();
            assert!(resolved.verified.is_empty());
            assert!(resolved.provenance.is_empty());
            assert_eq!(
                resolved.unverified.keys().cloned().collect::<Vec<_>>(),
                tokens
            );
            assert!(resolver.cache.read().await.identities.is_empty());
            let rows = paper.asset_identities("installed", &tokens).unwrap();
            assert_eq!(rows.len(), 2);
            for condition in rows.iter().map(|row| &row.condition_id.0) {
                assert!(
                    paper
                        .asset_identity_condition_rejection("installed", condition)
                        .unwrap()
                        .is_some()
                );
            }
        }
    }

    #[tokio::test]
    async fn durable_restore_rejects_invalid_rows_and_source_contracts_before_refetch() {
        use pe_core_types::{PolymarketConditionId, ReceivedAt, SourceId};
        for altered in [
            "source",
            "schema",
            "parser",
            "content_type",
            "condition",
            "outcome",
            "overflow_outcome",
            "page_hash",
            "sequence",
            "negative_sequence",
            "ambiguous",
            "absent",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let token = PolymarketTokenId("token-a".into());
            let valid = br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]}]"#;
            let payload = match altered {
                "ambiguous" => br#"[{"conditionId":"condition-a","clobTokenIds":["token-a"]},{"conditionId":"condition-a","clobTokenIds":["other"]}]"#.as_slice(),
                "absent" => b"[]".as_slice(),
                _ => valid.as_slice(),
            };
            let at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
            let sink = Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap()));
            sink.lock()
                .await
                .append_durable(EnvelopeIn {
                    source_id: SourceId(
                        if altered == "source" {
                            "different-source"
                        } else {
                            GAMMA_MARKETS_SOURCE_ID
                        }
                        .into(),
                    ),
                    schema_version: GAMMA_MARKETS_SCHEMA_VERSION + u32::from(altered == "schema"),
                    parser_version: GAMMA_MARKETS_PARSER_VERSION + u32::from(altered == "parser"),
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    content_type: if altered == "content_type" {
                        ContentType::Raw
                    } else {
                        ContentType::Json
                    },
                    payload: payload.to_vec(),
                })
                .unwrap();
            let original_hash = canonical_page_hash(payload).unwrap();
            let provenance = IdentityProvenance {
                asset: token.clone(),
                source_log_sequence: u64::from(altered == "sequence"),
                canonical_page_hash: if altered == "page_hash" {
                    "0".repeat(64)
                } else {
                    original_hash
                },
            };
            paper
                .insert_asset_identities(
                    "installed",
                    &[AssetIdentityRow {
                        token: token.clone(),
                        condition_id: PolymarketConditionId(
                            if altered == "condition" {
                                "different-condition"
                            } else {
                                "condition-a"
                            }
                            .into(),
                        ),
                        outcome: match altered {
                            "outcome" => 1,
                            "overflow_outcome" => 65_536,
                            _ => 0,
                        },
                        source_log_sequence: if altered == "negative_sequence" {
                            -1
                        } else {
                            i64::try_from(provenance.source_log_sequence).unwrap()
                        },
                        canonical_page_hash: provenance.canonical_page_hash,
                    }],
                )
                .unwrap();
            let fetcher = Arc::new(GammaFixture::new(valid, b"[]"));
            let resolver = AssetIdentityResolver::new(fetcher.clone(), BASE.into(), 50, sink)
                .with_paper_state(
                    paper.clone(),
                    "installed".into(),
                    SourceReceiptIndex::replay(&path).unwrap(),
                );
            let restored = resolver.resolve_live([token.clone()]).await.unwrap();
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1, "{altered}");
            assert_eq!(
                restored.verified[&token].condition_id.0, "condition-a",
                "{altered}"
            );
            assert_eq!(restored.verified[&token].outcome, OutcomeId(0), "{altered}");
            assert_eq!(
                restored.provenance[&token].source_log_sequence, 1,
                "{altered}"
            );
            let rows = paper.asset_identities("installed", &[token]).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].canonical_page_hash,
                restored.provenance[&rows[0].token].canonical_page_hash,
                "{altered}"
            );
        }
    }

    #[test]
    fn installed_identity_generation_uses_only_installed_activation_tails() {
        use pe_event_log::LogTailBinding;
        use pe_paper_state::DurableLogBindings;
        assert_eq!(
            ASSET_IDENTITY_INSERT_LIMIT,
            pe_source_polymarket_public::GAMMA_BATCH_SIZE
        );
        let tail = LogTailBinding {
            path: "/fixture/source.log".into(),
            physical_tail: 100,
            last_sequence: Some(EventSeq(7)),
            last_hash: blake3::hash(b"activation"),
        };
        let activation = DurableLogBindings {
            source: tail.clone(),
            paper: tail.clone(),
            live_journal: tail,
        };
        let mut record = MigrationRecord {
            version_one_boundary: activation.clone(),
            activation_tails: Some(activation),
            phase: MigrationPhase::BoundaryRecorded,
            side_main_path: "/fixture/side.db".into(),
            input_hashes: BTreeMap::new(),
        };
        assert_eq!(installed_identity_generation(&record).unwrap(), None);
        record.phase = MigrationPhase::Installed;
        let generation = installed_identity_generation(&record).unwrap();
        record.version_one_boundary.source.physical_tail += 1;
        record.side_main_path = "/fixture/moved.db".into();
        assert_eq!(installed_identity_generation(&record).unwrap(), generation);
        record
            .activation_tails
            .as_mut()
            .unwrap()
            .source
            .physical_tail += 1;
        assert_ne!(installed_identity_generation(&record).unwrap(), generation);
        record.activation_tails = None;
        assert!(matches!(
            installed_identity_generation(&record),
            Err(SourceError::Fatal { .. })
        ));
    }

    #[tokio::test]
    async fn fresh_identity_is_recorded_before_use_and_cache_reuses_its_provenance() {
        let fetcher = Arc::new(GammaFixture::new(
            br#"[{"conditionId":"condition-a","clobTokenIds":["token-a","token-b"]}]"#,
            b"[]",
        ));
        let (_dir, path, sink, resolver) = boot_resolver(fetcher.clone());
        let token = PolymarketTokenId("token-b".to_owned());

        let first = resolver.resolve_live([token.clone()]).await.unwrap();
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.verified[&token].condition_id.0, "condition-a");
        assert_eq!(first.verified[&token].outcome.0, 1);
        assert_eq!(first.provenance[&token].source_log_sequence, 0);
        assert_eq!(
            first.provenance[&token].canonical_page_hash,
            first.verified[&token].evidence_hash
        );

        let second = resolver.resolve_live([token.clone()]).await.unwrap();
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

        let resolved = resolver.resolve_live([token.clone()]).await.unwrap();
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
            .resolve_live([token_a.clone(), token_b.clone()])
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
    async fn successful_chunk_is_recorded_and_cached_before_later_transient() {
        let fetcher = Arc::new(MixedChunkFixture {
            calls: AtomicUsize::new(0),
            attempts: StdMutex::new(Vec::new()),
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let sink = Arc::new(Mutex::new(SourceEventSink::open(&path).unwrap()));
        let resolver =
            AssetIdentityResolver::new(fetcher.clone(), BASE.to_owned(), 1, Arc::clone(&sink));
        let token_a = PolymarketTokenId("token-a".to_owned());
        let token_b = PolymarketTokenId("token-b".to_owned());

        assert!(matches!(
            resolver.resolve_live([token_a.clone(), token_b]).await,
            Err(SourceError::Transient { message })
                if message.contains("injected second chunk failure")
        ));
        let entries_after_failure = Reader::replay(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries_after_failure.len(), 1);

        let resolved = resolver.resolve_live([token_a.clone()]).await.unwrap();
        assert_eq!(resolved.verified[&token_a].condition_id.0, "condition-a");
        assert_eq!(
            fetcher.calls.load(Ordering::SeqCst),
            2,
            "the failed multi-chunk lookup keeps its verified sibling"
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

            let resolved = resolver.resolve_live([token.clone()]).await.unwrap();
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
                resolver.resolve_live([token]).await,
                Err(SourceError::Fatal { message })
                    if message.starts_with("gamma metadata parse failed:")
            ));
            assert!(resolver.cache.read().await.identities.is_empty());

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
                .resolve_live([PolymarketTokenId("token-a".to_owned())])
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
                .resolve_live([PolymarketTokenId("token-a".to_owned())])
                .await,
            Err(SourceError::Fatal { message }) if message == "source-log coordinator closed"
        ));
    }
}
