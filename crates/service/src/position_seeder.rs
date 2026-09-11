//! Causal current-position validation for watchlist admission (#544).
//!
//! Venue positions own absolute balances at each proved anchor; ordered
//! activity owns exact causal effects after that anchor.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use pe_copy_signal_engine::{PositionSnapshot, PositionState, SignalConfig};
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, PolymarketTokenId, ReceivedAt, ReconstructionQuality,
    ShareAmount, SourceId, SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_paper_state::{NoCopyDisposition, PaperStateDb};
use pe_position_ledger::PositionLedger;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAssetMapping, ActivityParseError,
    ActivityReadError, ActivityValidationError, CompleteActivityRead, CompletePositionsRead,
    PositionClassification, PositionReadError, ReconciliationFetcher, fetch_complete_activity,
    fetch_complete_positions,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::asset_identity::{AssetIdentityResolver, BootSourceLog, IdentityProvenance};
use crate::bucket_commit::{
    AnchorInstallError, BucketCommitEngine, BucketDecisionContext, IdentityOverride,
};
use crate::orchestrator_control::{AdmissionLedgerCapture, OrchestratorControl};
use crate::trade_poller::ACTIVITY_POLL_SOURCE_ID;

#[cfg(feature = "scenario")]
type BracketStepHook = Arc<dyn Fn(WalletAddress, usize, &mut BucketCommitEngine) + Send + Sync>;

#[cfg(feature = "scenario")]
type AnchorInstallHook = Arc<dyn Fn(&[AnchorInstall]) + Send + Sync>;

pub const BRACKET_CONCURRENCY: usize = 4;

/// A venue-authoritative balance snapshot waiting for the single-owner install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorInstall {
    pub wallet: WalletAddress,
    pub balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
    pub cutoff: i64,
    pub proof: AnchorProof,
    pub expected: AnchorExpectation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorProof {
    pub positions_proof_hash: String,
    pub activity_bounds_json: String,
    pub source_log_generation: String,
    pub document: String,
    pub recorded_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorExpectation {
    pub ledger_hash: String,
    pub cursor: Option<i64>,
    pub anchor_seq: Option<i64>,
    pub coverage_generation: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum CausalPositionError {
    #[error("activity catch-up for {wallet}: {source}")]
    Activity {
        wallet: WalletAddress,
        source: pe_source_polymarket_public::ActivityReadError,
    },
    #[error("positions read for {wallet}: {source}")]
    Positions {
        wallet: WalletAddress,
        source: pe_source_polymarket_public::PositionReadError,
    },
    #[error("asset identity resolution for {wallet}: {source}")]
    Identity {
        wallet: WalletAddress,
        source: SourceError,
    },
    #[error("activity bucket proof encoding failed: {0}")]
    ProofEncoding(#[from] serde_json::Error),
    #[error("activity bucket commit failed for {wallet}: {message}")]
    BucketCommit {
        wallet: WalletAddress,
        message: String,
    },
    #[error("wallet {wallet} became durably fenced during validation")]
    Fenced { wallet: WalletAddress },
    #[error("activity changed between bracket steps for {wallet}")]
    InterveningActivity { wallet: WalletAddress },
    #[error("current-position semantic proofs changed for {wallet}")]
    PositionRevision { wallet: WalletAddress },
    #[error("activity ledger changed between bracket steps for {wallet}")]
    LedgerRevision { wallet: WalletAddress },
    #[error("activity fixed end moved backwards for {wallet}: previous {previous}, next {next}")]
    NonMonotonicActivityBounds {
        wallet: WalletAddress,
        previous: i64,
        next: i64,
    },
    #[error("orchestrator control channel closed during position validation")]
    ControlClosed,
    #[error("orchestrator dropped a position-validation acknowledgement")]
    AcknowledgementClosed,
    #[error("ledger proof contains multiple assets for {condition_id} outcome {outcome}")]
    DuplicateOutcome { condition_id: String, outcome: u16 },
    #[error("ledger proof component is too long")]
    ProofComponentTooLong,
    #[error("paper-state position proof install failed: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("position anchor install failed: {0}")]
    AnchorInstall(#[from] AnchorInstallError),
    #[error("reconstruction quality invariant failed")]
    ReconstructionQuality,
}

/// Exact per-wallet source outcomes that retry without fencing or failing boot.
///
/// A venue history the activity row parser refuses (issue #594: rehearsal attempt 9 aborted the
/// whole boot on one wallet's `TRADE` row with price 3.1968021978) is that wallet's problem: it
/// is excluded from the boot universe (left unvalidated for runtime admission) and, once live,
/// yields `Deferred` on the refresh cadence. Only the observed shape is deferred: malformed pages,
/// window invalidations, other row validations, aggregation and identity failures stay fatal.
#[must_use]
pub fn is_deferred_causal_position_error(error: &CausalPositionError) -> bool {
    match error {
        CausalPositionError::PositionRevision { .. }
        | CausalPositionError::InterveningActivity { .. } => true,
        CausalPositionError::Activity {
            source: ActivityReadError::SaturatedTerminalSecond { .. },
            ..
        } => true,
        CausalPositionError::Activity {
            source:
                ActivityReadError::Parse(ActivityParseError::InvalidRow {
                    source: ActivityValidationError::InvalidPrice { .. },
                    ..
                }),
            ..
        } => true,
        CausalPositionError::Activity {
            source: ActivityReadError::Fetch { source, .. },
            ..
        }
        | CausalPositionError::Positions {
            source: PositionReadError::Fetch { source, .. },
            ..
        }
        | CausalPositionError::Identity { source, .. } => matches!(
            source,
            SourceError::Transient { .. } | SourceError::RateLimited { .. }
        ),
        CausalPositionError::Positions { source, .. } => matches!(
            source,
            PositionReadError::MissingActivityMapping { .. }
                | PositionReadError::ConflictingActivityMapping { .. }
                | PositionReadError::MixedActivityClassification { .. }
                | PositionReadError::MetadataUnresolved { .. }
                | PositionReadError::ConflictingOutcomeMapping { .. }
                | PositionReadError::DuplicateAsset { .. }
                | PositionReadError::SaturatedTerminalPage { .. }
        ),
        _ => false,
    }
}

/// Shared real/fake-source bracket implementation.
#[derive(Clone)]
pub struct CausalPositionValidator {
    fetcher: Arc<dyn ReconciliationFetcher>,
    asset_identity: Arc<AssetIdentityResolver>,
    base_url: Arc<str>,
    source_log_generation: Arc<str>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    #[cfg(feature = "scenario")]
    step_hook: Option<BracketStepHook>,
    #[cfg(feature = "scenario")]
    install_hook: Option<AnchorInstallHook>,
}

/// What the anchor proof keeps from a complete activity read once its rows have
/// been committed: the fixed end and the page evidence. The rows themselves are
/// already recorded per group, and holding three full histories per wallet was
/// the boot's memory peak (#555 activation).
struct ActivityEvidence {
    fixed_end: i64,
    pages: Vec<pe_source_polymarket_public::ReconciliationPageEvidence>,
}

struct PreparedActivity {
    mapping: ActivityAssetMapping,
    identity_overrides: HashMap<SourceTradeId, IdentityOverride>,
    identity_unresolved: HashSet<SourceTradeId>,
    no_copy_dispositions: HashMap<SourceTradeId, NoCopyDisposition>,
    unresolved_assets: BTreeMap<String, String>,
    metadata_reads: BTreeMap<PolymarketTokenId, IdentityProvenance>,
}

impl From<CompleteActivityRead> for ActivityEvidence {
    fn from(read: CompleteActivityRead) -> Self {
        Self {
            fixed_end: read.fixed_end,
            pages: read.pages,
        }
    }
}

impl CausalPositionValidator {
    pub fn new(
        fetcher: Arc<dyn ReconciliationFetcher>,
        base_url: impl Into<Arc<str>>,
        source_log_generation: impl Into<Arc<str>>,
        asset_identity: Arc<AssetIdentityResolver>,
    ) -> Self {
        Self {
            fetcher,
            asset_identity,
            base_url: base_url.into(),
            source_log_generation: source_log_generation.into(),
            now: Arc::new(|| time::OffsetDateTime::now_utc().unix_timestamp()),
            #[cfg(feature = "scenario")]
            step_hook: None,
            #[cfg(feature = "scenario")]
            install_hook: None,
        }
    }

    /// Boot-migration form: every fetched activity/positions page is appended
    /// and synchronized through the normal source-log owner before parsing and
    /// application to the side database (#544).
    pub fn new_recording(
        fetcher: Arc<dyn ReconciliationFetcher>,
        base_url: impl Into<Arc<str>>,
        source_log_generation: impl Into<Arc<str>>,
        source_log: BootSourceLog,
        asset_identity: Arc<AssetIdentityResolver>,
    ) -> Self {
        let recording = DurableRecordingFetcher {
            inner: fetcher,
            sink: source_log,
        };
        Self::new(
            Arc::new(recording),
            base_url,
            source_log_generation,
            asset_identity,
        )
    }

    /// Deterministic bracket clock for hermetic scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// Inject a deterministic mutation between bracket steps in scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_step_hook(mut self, hook: BracketStepHook) -> Self {
        self.step_hook = Some(hook);
        self
    }

    /// Observe the one direct-install batch in deterministic scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_install_hook(mut self, hook: AnchorInstallHook) -> Self {
        self.install_hook = Some(hook);
        self
    }

    /// Validate every wallet through the live orchestrator owner. The caller
    /// sends all returned acceptances in one final installation command.
    pub async fn validate_via_control(
        &self,
        wallets: &[WalletAddress],
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
    ) -> Result<Vec<AnchorInstall>, CausalPositionError> {
        let mut completed = futures::stream::iter(wallets.iter().copied().enumerate())
            .map(|(index, wallet)| async move {
                (
                    index,
                    self.validate_control_with_retry(wallet, control_tx, paper_state)
                        .await,
                )
            })
            .buffer_unordered(BRACKET_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        completed.sort_by_key(|(index, _)| *index);
        let mut accepted = Vec::with_capacity(completed.len());
        for (_, result) in completed {
            accepted.push(result?);
        }
        Ok(accepted)
    }

    /// Boot-time form used before producers start. The same bucket engine is
    /// moved into the orchestrator after the atomic proof installation.
    pub async fn validate_direct(
        &self,
        wallets: &[WalletAddress],
        engine: &mut BucketCommitEngine,
        paper_state: &PaperStateDb,
    ) -> Result<Vec<AnchorInstall>, CausalPositionError> {
        let engine = Arc::new(tokio::sync::Mutex::new(engine));
        let mut completed = futures::stream::iter(wallets.iter().copied().enumerate())
            .map(|(index, wallet)| {
                let engine = Arc::clone(&engine);
                async move {
                    (
                        index,
                        wallet,
                        self.validate_direct_with_retry(wallet, &engine, paper_state)
                            .await,
                    )
                }
            })
            .buffer_unordered(BRACKET_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        completed.sort_by_key(|(index, _, _)| *index);
        let mut accepted = Vec::with_capacity(completed.len());
        for (_, wallet, result) in completed {
            match result {
                Ok(acceptance) => accepted.push(acceptance),
                // A newly durable fence is deterministic quarantine (the
                // commit already recorded it; the next boot's pre-bracket
                // filter would exclude the wallet anyway). A retryable
                // outcome — positions revised between reads, activity
                // intervening mid-bracket, or an incomplete source read —
                // leaves the wallet unvalidated: it stays history-incomplete,
                // is filtered after the bracket, and re-enters only through
                // the serialized runtime admission preparer, which owns the
                // retries (both observed live in the #544 activation
                // rehearsals as activation deadlocks). Infrastructure errors
                // and a post-loop ledger revision still fail the boot.
                Err(CausalPositionError::Fenced { wallet }) => {
                    tracing::warn!(
                        wallet = %wallet,
                        "boot bracket: wallet durably fenced; excluded from the boot universe"
                    );
                }
                Err(error) if is_deferred_causal_position_error(&error) => {
                    tracing::warn!(
                        wallet = %wallet,
                        outcome = %error,
                        "boot bracket: wallet left unvalidated for runtime admission"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        #[cfg(feature = "scenario")]
        if let Some(hook) = &self.install_hook {
            hook(&accepted);
        }
        engine.lock().await.install_anchors(&accepted)?;
        // The accepted bracket IS the plan's history-validating reconciliation:
        // promote each wallet's SEEDED (conservative) history row to complete.
        // Unseeded wallets stay incomplete and are filtered fail-closed (#544
        // activation fix — without this the first v2 boot can never publish).
        let wallets: Vec<WalletAddress> = accepted.iter().map(|install| install.wallet).collect();
        paper_state.mark_seeded_history_validated(
            &wallets,
            "{\"source\":\"causal_position_bracket_v2\"}",
            time::OffsetDateTime::now_utc().unix_timestamp(),
        )?;
        Ok(accepted)
    }

    async fn validate_control_with_retry(
        &self,
        wallet: WalletAddress,
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let first = self.validate_one_control(wallet, control_tx).await;
        if first.as_ref().is_err_and(is_bounded_retry_error) {
            if paper_state.is_wallet_fenced(&wallet)? {
                return Err(CausalPositionError::Fenced { wallet });
            }
            return self.validate_one_control(wallet, control_tx).await;
        }
        first
    }

    async fn validate_direct_with_retry(
        &self,
        wallet: WalletAddress,
        engine: &Arc<tokio::sync::Mutex<&mut BucketCommitEngine>>,
        paper_state: &PaperStateDb,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let first = self.validate_one_direct(wallet, engine, paper_state).await;
        if first.as_ref().is_err_and(is_bounded_retry_error) {
            if paper_state.is_wallet_fenced(&wallet)? {
                return Err(CausalPositionError::Fenced { wallet });
            }
            return self.validate_one_direct(wallet, engine, paper_state).await;
        }
        first
    }

    async fn validate_one_control(
        &self,
        wallet: WalletAddress,
        control_tx: &mpsc::Sender<OrchestratorControl>,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let mut metadata_reads = BTreeMap::new();
        let mut unresolved_assets = BTreeMap::new();
        let first_activity = self.activity(wallet).await?;
        let first_prepared = self.prepare_activity(wallet, &first_activity).await?;
        metadata_reads.extend(first_prepared.metadata_reads.clone());
        unresolved_assets.extend(first_prepared.unresolved_assets.clone());
        self.commit_control(wallet, &first_activity, &first_prepared, control_tx, false)
            .await?;
        let first_ledger = capture_control(wallet, control_tx).await?;
        let first_positions = self.positions(wallet, &first_prepared.mapping).await?;
        let first_activity = ActivityEvidence::from(first_activity);

        let second_activity = self.activity(wallet).await?;
        let second_prepared = self.prepare_activity(wallet, &second_activity).await?;
        metadata_reads.extend(second_prepared.metadata_reads.clone());
        unresolved_assets.extend(second_prepared.unresolved_assets.clone());
        if self
            .commit_control(wallet, &second_activity, &second_prepared, control_tx, true)
            .await?
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let second_ledger = capture_control(wallet, control_tx).await?;
        let second_positions = self.positions(wallet, &second_prepared.mapping).await?;
        let second_activity = ActivityEvidence::from(second_activity);

        let final_activity = self.activity(wallet).await?;
        let final_prepared = self.prepare_activity(wallet, &final_activity).await?;
        unresolved_assets.extend(final_prepared.unresolved_assets.clone());
        metadata_reads.extend(final_prepared.metadata_reads.clone());
        if self
            .commit_control(wallet, &final_activity, &final_prepared, control_tx, true)
            .await?
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let final_ledger = capture_control(wallet, control_tx).await?;
        let final_activity = ActivityEvidence::from(final_activity);
        let install = self.finish(
            wallet,
            [&first_activity, &second_activity, &final_activity],
            [&first_ledger, &second_ledger, &final_ledger],
            &first_positions,
            &second_positions,
            metadata_reads.into_values().collect(),
        )?;
        log_unresolved_activity_assets(wallet, &unresolved_assets);
        Ok(install)
    }

    async fn validate_one_direct(
        &self,
        wallet: WalletAddress,
        engine: &Arc<tokio::sync::Mutex<&mut BucketCommitEngine>>,
        paper_state: &PaperStateDb,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let mut metadata_reads = BTreeMap::new();
        let mut unresolved_assets = BTreeMap::new();
        let first_activity = self.activity(wallet).await?;
        let first_prepared = self.prepare_activity(wallet, &first_activity).await?;
        metadata_reads.extend(first_prepared.metadata_reads.clone());
        unresolved_assets.extend(first_prepared.unresolved_assets.clone());
        let first_ledger = {
            let mut engine = engine.lock().await;
            commit_direct(
                wallet,
                &first_activity,
                &first_prepared,
                &mut engine,
                false,
                &self.source_log_generation,
            )?;
            let captured = ledger_capture(engine.ledger(), paper_state, wallet)?;
            #[cfg(feature = "scenario")]
            self.run_step_hook(wallet, 1, &mut engine);
            captured
        };
        let first_positions = self.positions(wallet, &first_prepared.mapping).await?;
        let first_activity = ActivityEvidence::from(first_activity);
        #[cfg(feature = "scenario")]
        {
            let mut engine = engine.lock().await;
            self.run_step_hook(wallet, 2, &mut engine);
        }

        let second_activity = self.activity(wallet).await?;
        let second_prepared = self.prepare_activity(wallet, &second_activity).await?;
        metadata_reads.extend(second_prepared.metadata_reads.clone());
        unresolved_assets.extend(second_prepared.unresolved_assets.clone());
        let second_ledger = {
            let mut engine = engine.lock().await;
            if commit_direct(
                wallet,
                &second_activity,
                &second_prepared,
                &mut engine,
                true,
                &self.source_log_generation,
            )? {
                return Err(CausalPositionError::InterveningActivity { wallet });
            }
            let captured = ledger_capture(engine.ledger(), paper_state, wallet)?;
            #[cfg(feature = "scenario")]
            self.run_step_hook(wallet, 3, &mut engine);
            captured
        };
        let second_positions = self.positions(wallet, &second_prepared.mapping).await?;
        let second_activity = ActivityEvidence::from(second_activity);
        #[cfg(feature = "scenario")]
        {
            let mut engine = engine.lock().await;
            self.run_step_hook(wallet, 4, &mut engine);
        }

        let final_activity = self.activity(wallet).await?;
        let final_prepared = self.prepare_activity(wallet, &final_activity).await?;
        unresolved_assets.extend(final_prepared.unresolved_assets.clone());
        metadata_reads.extend(final_prepared.metadata_reads.clone());
        let final_ledger = {
            let mut engine = engine.lock().await;
            if commit_direct(
                wallet,
                &final_activity,
                &final_prepared,
                &mut engine,
                true,
                &self.source_log_generation,
            )? {
                return Err(CausalPositionError::InterveningActivity { wallet });
            }
            ledger_capture(engine.ledger(), paper_state, wallet)?
        };
        let final_activity = ActivityEvidence::from(final_activity);
        let install = self.finish(
            wallet,
            [&first_activity, &second_activity, &final_activity],
            [&first_ledger, &second_ledger, &final_ledger],
            &first_positions,
            &second_positions,
            metadata_reads.into_values().collect(),
        )?;
        log_unresolved_activity_assets(wallet, &unresolved_assets);
        #[cfg(feature = "scenario")]
        {
            let mut engine = engine.lock().await;
            self.run_step_hook(wallet, 5, &mut engine);
        }
        Ok(install)
    }

    #[cfg(feature = "scenario")]
    fn run_step_hook(&self, wallet: WalletAddress, step: usize, engine: &mut BucketCommitEngine) {
        if let Some(hook) = &self.step_hook {
            hook(wallet, step, engine);
        }
    }

    async fn activity(
        &self,
        wallet: WalletAddress,
    ) -> Result<CompleteActivityRead, CausalPositionError> {
        fetch_complete_activity(
            self.fetcher.as_ref(),
            &self.base_url,
            wallet,
            None,
            (self.now)(),
        )
        .await
        .map_err(|source| CausalPositionError::Activity { wallet, source })
    }

    async fn prepare_activity(
        &self,
        wallet: WalletAddress,
        activity: &CompleteActivityRead,
    ) -> Result<PreparedActivity, CausalPositionError> {
        let mut mapping = activity
            .asset_mapping()
            .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        let tokens = mapping.tokens().cloned().collect::<Vec<_>>();
        let resolved = self
            .asset_identity
            .resolve(tokens.clone())
            .await
            .map_err(|source| CausalPositionError::Identity { wallet, source })?;
        for (asset, identity) in &resolved.verified {
            mapping
                .apply_verified(asset, identity)
                .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        }
        for asset in &tokens {
            if mapping.classification(asset).is_none() {
                let source = PositionReadError::MixedActivityClassification {
                    asset: asset.0.clone(),
                };
                return Err(CausalPositionError::Positions { wallet, source });
            }
            if resolved.verified.contains_key(asset) && !resolved.provenance.contains_key(asset) {
                return Err(CausalPositionError::Identity {
                    wallet,
                    source: SourceError::Fatal {
                        message: format!(
                            "verified token {} has no durable metadata provenance",
                            asset.0
                        ),
                    },
                });
            }
        }

        let mut identity_overrides = HashMap::new();
        let unresolved_assets = tokens
            .iter()
            .filter(|asset| !resolved.verified.contains_key(*asset))
            .map(|asset| {
                (
                    asset.0.clone(),
                    resolved.unverified.get(asset).cloned().unwrap_or_else(|| {
                        "token absent from open and closed Gamma metadata".to_owned()
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut identity_unresolved = HashSet::new();
        let mut no_copy_dispositions = HashMap::new();
        for bucket in activity
            .buckets()
            .map_err(|source| CausalPositionError::Activity { wallet, source })?
        {
            for aggregate in bucket {
                let components = aggregate.group_id.components();
                let Some(asset) = &components.asset else {
                    continue;
                };
                let Some(identity) = resolved.verified.get(asset) else {
                    let source_trade_id = aggregate.group_id.key().clone();
                    identity_unresolved.insert(source_trade_id.clone());
                    no_copy_dispositions.insert(
                        source_trade_id,
                        NoCopyDisposition {
                            provenance: "rest_poll".to_owned(),
                            age_secs: activity
                                .fixed_end
                                .saturating_sub(aggregate.source_time.0.unix_timestamp()),
                            reason: "identity_unresolved".to_owned(),
                            recorded_at_unix: activity.fixed_end,
                        },
                    );
                    continue;
                };
                if components.condition_id.as_ref() != Some(&identity.condition_id)
                    || components.outcome != Some(identity.outcome)
                {
                    identity_overrides.insert(
                        aggregate.group_id.key().clone(),
                        IdentityOverride {
                            verified: MarketOutcomeId::new(
                                MarketId(VenueMarketId(identity.condition_id.0.clone())),
                                identity.outcome,
                            ),
                            evidence_hash: identity.evidence_hash.clone(),
                        },
                    );
                }
            }
        }
        Ok(PreparedActivity {
            mapping,
            identity_overrides,
            identity_unresolved,
            no_copy_dispositions,
            unresolved_assets,
            metadata_reads: resolved.provenance,
        })
    }

    async fn positions(
        &self,
        wallet: WalletAddress,
        mapping: &pe_source_polymarket_public::ActivityAssetMapping,
    ) -> Result<CompletePositionsRead, CausalPositionError> {
        fetch_complete_positions(self.fetcher.as_ref(), &self.base_url, wallet, mapping)
            .await
            .map_err(|source| CausalPositionError::Positions { wallet, source })
    }

    async fn commit_control(
        &self,
        wallet: WalletAddress,
        activity: &CompleteActivityRead,
        prepared: &PreparedActivity,
        control_tx: &mpsc::Sender<OrchestratorControl>,
        count_change: bool,
    ) -> Result<bool, CausalPositionError> {
        let mut changed = false;
        let buckets = activity
            .buckets()
            .map_err(|source| CausalPositionError::Activity { wallet, source })?;
        let context = Arc::new(bracket_context(
            activity,
            &self.source_log_generation,
            prepared,
        )?);
        for bucket in buckets {
            let (committed, acknowledgement) = oneshot::channel();
            control_tx
                .send(OrchestratorControl::CommitActivityBucket {
                    aggregates: bucket,
                    context: Arc::clone(&context),
                    committed,
                })
                .await
                .map_err(|_| CausalPositionError::ControlClosed)?;
            let result = acknowledgement
                .await
                .map_err(|_| CausalPositionError::AcknowledgementClosed)?
                .map_err(|message| CausalPositionError::BucketCommit { wallet, message })?;
            if result.newly_fenced.is_some() {
                return Err(CausalPositionError::Fenced { wallet });
            }
            changed |= count_change && !result.already_committed;
        }
        Ok(changed)
    }

    fn finish(
        &self,
        wallet: WalletAddress,
        activities: [&ActivityEvidence; 3],
        ledgers: [&AdmissionLedgerCapture; 3],
        first_positions: &CompletePositionsRead,
        second_positions: &CompletePositionsRead,
        metadata_reads: Vec<IdentityProvenance>,
    ) -> Result<AnchorInstall, CausalPositionError> {
        for pair in activities.windows(2) {
            if pair[1].fixed_end < pair[0].fixed_end {
                return Err(CausalPositionError::NonMonotonicActivityBounds {
                    wallet,
                    previous: pair[0].fixed_end,
                    next: pair[1].fixed_end,
                });
            }
        }
        if !first_positions.semantically_equal(second_positions) {
            return Err(CausalPositionError::PositionRevision { wallet });
        }
        if ledgers[0].hash != ledgers[1].hash || ledgers[1].hash != ledgers[2].hash {
            return Err(CausalPositionError::LedgerRevision { wallet });
        }
        let coverage_matches = |left: &AdmissionLedgerCapture, right: &AdmissionLedgerCapture| {
            left.cursor == right.cursor
                && left.anchor_seq == right.anchor_seq
                && left.coverage_generation == right.coverage_generation
        };
        if !coverage_matches(ledgers[0], ledgers[1]) || !coverage_matches(ledgers[1], ledgers[2]) {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let balances = ordinary_position_balances(first_positions)?;

        let activity_bounds = activities
            .iter()
            .map(|activity| {
                json!({
                    "fixed_end": activity.fixed_end,
                    "pages": activity.pages,
                })
            })
            .collect::<Vec<_>>();
        let proof = json!({
            "version": 1,
            "wallet": wallet,
            "source_id": ACTIVITY_POLL_SOURCE_ID,
            "positions_semantic_hash": first_positions.semantic_hash(),
            "expected_ledger_hash": ledgers[2].hash,
            "source_log_generation": self.source_log_generation,
            "activity_walks": activity_bounds,
            "metadata_reads": metadata_reads,
            "positions_reads": [
                {
                    "semantic_hash": first_positions.semantic_hash(),
                    "pages": first_positions.pages,
                },
                {
                    "semantic_hash": second_positions.semantic_hash(),
                    "pages": second_positions.pages,
                },
            ],
        });
        Ok(AnchorInstall {
            wallet,
            balances,
            cutoff: activities[1].fixed_end,
            proof: AnchorProof {
                positions_proof_hash: first_positions.semantic_hash().to_owned(),
                activity_bounds_json: serde_json::to_string(&activity_bounds)?,
                source_log_generation: self.source_log_generation.to_string(),
                document: serde_json::to_string(&proof)?,
                recorded_at_unix: (self.now)(),
            },
            expected: AnchorExpectation {
                ledger_hash: ledgers[2].hash.clone(),
                cursor: ledgers[2].cursor,
                anchor_seq: ledgers[2].anchor_seq,
                coverage_generation: ledgers[2].coverage_generation,
            },
        })
    }
}

fn is_bounded_retry_error(error: &CausalPositionError) -> bool {
    matches!(
        error,
        CausalPositionError::InterveningActivity { .. }
            | CausalPositionError::PositionRevision { .. }
    )
}

fn log_unresolved_activity_assets(
    wallet: WalletAddress,
    unresolved_assets: &BTreeMap<String, String>,
) {
    if !unresolved_assets.is_empty() {
        tracing::warn!(
            wallet = %wallet,
            raw_only_assets = ?unresolved_assets,
            "position bracket: activity assets recorded raw-only"
        );
    }
}

struct DurableRecordingFetcher {
    inner: Arc<dyn ReconciliationFetcher>,
    sink: BootSourceLog,
}

impl ReconciliationFetcher for DurableRecordingFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = self.inner.fetch(url).await?;
            let received_at = time::OffsetDateTime::now_utc();
            let envelope = EnvelopeIn {
                source_id: SourceId(ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: payload.clone(),
            };
            self.sink
                .lock()
                .await
                .append_durable(envelope)
                .map_err(|error| SourceError::Fatal {
                    message: format!("source-log append failed: {error}"),
                })?;
            Ok(payload)
        })
    }
}

fn bracket_context(
    activity: &CompleteActivityRead,
    source_log_generation: &str,
    prepared: &PreparedActivity,
) -> Result<BucketDecisionContext, CausalPositionError> {
    let reconstruction_quality =
        ReconstructionQuality::new(100).map_err(|_| CausalPositionError::ReconstructionQuality)?;
    Ok(BucketDecisionContext {
        applied_configuration: crate::runtime_config::RuntimeConfig::from_service_config(
            &crate::config::ServiceConfig::default(),
        ),
        decision_inputs_json: serde_json::to_string(&json!({
            "fixed_end": activity.fixed_end,
            "pages": activity.pages,
            "source_log_generation": source_log_generation,
        }))?,
        page_occurrences: Vec::new(),
        observed_source_receipts: HashMap::new(),
        reconstruction_quality,
        read_commitment: None,

        signal_config: SignalConfig::default(),
        copy_eligible: false,
        bracket_commit: true,
        recorded_at_unix: time::OffsetDateTime::now_utc().unix_timestamp(),
        observation_provenance: HashMap::new(),
        no_copy_dispositions: prepared.no_copy_dispositions.clone(),
        identity_overrides: prepared.identity_overrides.clone(),
        identity_unresolved: prepared.identity_unresolved.clone(),
        history_status: None,
    })
}

fn commit_direct(
    wallet: WalletAddress,
    activity: &CompleteActivityRead,
    prepared: &PreparedActivity,
    engine: &mut BucketCommitEngine,
    count_change: bool,
    source_log_generation: &str,
) -> Result<bool, CausalPositionError> {
    enum BatchOutcome {
        Complete { changed: bool },
        NewlyFenced,
    }

    let buckets = activity
        .buckets()
        .map_err(|source| CausalPositionError::Activity { wallet, source })?;
    let context = bracket_context(activity, source_log_generation, prepared)?;
    let outcome = engine
        .commit_batch(|engine| {
            let mut changed = false;
            for bucket in buckets {
                let result = engine.commit(
                    // Bracket catch-up is never copy-eligible (`copy_eligible: false`
                    // above), so no pending decision is created and the frozen basis is
                    // inert; a zero basis keeps that invariant explicit (#544).
                    bucket,
                    &context,
                    crate::bucket_commit::FrozenDecisionBasis {
                        win_rate_p: pe_core_types::Probability::ZERO,
                        bankroll: rust_decimal::Decimal::ZERO,
                    },
                )?;
                if result.newly_fenced.is_some() {
                    return Ok(BatchOutcome::NewlyFenced);
                }
                changed |= count_change && !result.already_committed;
            }
            Ok(BatchOutcome::Complete { changed })
        })
        .map_err(|error| CausalPositionError::BucketCommit {
            wallet,
            message: error.to_string(),
        })?;
    match outcome {
        BatchOutcome::Complete { changed } => Ok(changed),
        BatchOutcome::NewlyFenced => {
            // The fence bucket and every preceding bucket are durable now.
            Err(CausalPositionError::Fenced { wallet })
        }
    }
}

/// Canonical exact ledger proof for one wallet.
pub fn ledger_capture(
    ledger: &PositionLedger,
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
) -> Result<AdmissionLedgerCapture, CausalPositionError> {
    let mut all = BTreeMap::<(String, u16), (ShareAmount, ShareAmount)>::new();
    if let Some(snapshot) = ledger.position(&wallet) {
        for (key, state) in &snapshot.positions {
            all.insert(
                (key.market().to_string(), key.outcome().0),
                (state.long_contracts, state.short_contracts),
            );
        }
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"prediction-edge/admission-ledger/v1\0");
    hash_part(&mut hasher, wallet.to_string().as_bytes())?;
    for ((condition_id, outcome), (long, short)) in all {
        hash_part(&mut hasher, condition_id.as_bytes())?;
        hash_part(&mut hasher, &outcome.to_be_bytes())?;
        hash_part(&mut hasher, &long.atomic().to_be_bytes())?;
        hash_part(&mut hasher, &short.atomic().to_be_bytes())?;
    }
    let coverage = paper_state.wallet_coverage(&wallet)?;
    Ok(AdmissionLedgerCapture {
        wallet,
        hash: hasher.finalize().to_hex().to_string(),
        cursor: paper_state.cursor(&wallet)?,
        anchor_seq: coverage.anchor_seq,
        coverage_generation: coverage.coverage_generation,
    })
}

fn ordinary_position_balances(
    read: &CompletePositionsRead,
) -> Result<Vec<(MarketId, OutcomeId, ShareAmount)>, CausalPositionError> {
    let mut balances = BTreeMap::new();
    for position in &read.positions {
        if position.classification == PositionClassification::Combo
            || position.size == ShareAmount::ZERO
        {
            continue;
        }
        let key = (position.condition_id.0.clone(), position.outcome.0);
        if balances.insert(key.clone(), position.size).is_some() {
            return Err(CausalPositionError::DuplicateOutcome {
                condition_id: key.0,
                outcome: key.1,
            });
        }
    }
    Ok(balances
        .into_iter()
        .map(|((condition_id, outcome), amount)| {
            (
                MarketId(VenueMarketId(condition_id)),
                OutcomeId(outcome),
                amount,
            )
        })
        .collect())
}

fn hash_part(hasher: &mut blake3::Hasher, value: &[u8]) -> Result<(), CausalPositionError> {
    let len = u64::try_from(value.len()).map_err(|_| CausalPositionError::ProofComponentTooLong)?;
    hasher.update(&len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

async fn capture_control(
    wallet: WalletAddress,
    control_tx: &mpsc::Sender<OrchestratorControl>,
) -> Result<AdmissionLedgerCapture, CausalPositionError> {
    let (captured, acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::CaptureAdmissionLedger { wallet, captured })
        .await
        .map_err(|_| CausalPositionError::ControlClosed)?;
    acknowledgement
        .await
        .map_err(|_| CausalPositionError::AcknowledgementClosed)?
        .map_err(|message| CausalPositionError::BucketCommit { wallet, message })
}

// Retained only for the isolated stopped canary, whose authenticated posture
// needs strict `outcomeIndex` parsing but does not mutate the ordinary ledger.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanaryRawPosition {
    condition_id: String,
    outcome_index: Option<u16>,
    size: Decimal,
}

#[derive(Debug, thiserror::Error)]
pub enum PositionParseError {
    #[error("failed to deserialize positions response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("position omitted outcomeIndex")]
    MissingOutcomeIndex,
    #[error("position has invalid exact size {value}: {reason}")]
    InvalidAmount { value: Decimal, reason: String },
}

pub fn parse_positions_strict(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<PositionSnapshot, PositionParseError> {
    let raw: Vec<CanaryRawPosition> = serde_json::from_slice(bytes)?;
    let mut positions = HashMap::new();
    for item in raw {
        let outcome = item
            .outcome_index
            .ok_or(PositionParseError::MissingOutcomeIndex)?;
        let amount = ShareAmount::from_decimal_exact(item.size).map_err(|error| {
            PositionParseError::InvalidAmount {
                value: item.size,
                reason: error.to_string(),
            }
        })?;
        if amount == ShareAmount::ZERO {
            continue;
        }
        positions.insert(
            MarketOutcomeId::new(
                MarketId(VenueMarketId(item.condition_id)),
                OutcomeId(outcome),
            ),
            PositionState {
                long_contracts: amount,
                short_contracts: ShareAmount::ZERO,
            },
        );
    }
    Ok(PositionSnapshot { wallet, positions })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::source_event_sink::SourceEventSink;
    use pe_source_polymarket_public::{
        ActivityParseContext, ActivityTransport, aggregate_activity_rows, parse_activity_response,
    };

    struct EmptyFetcher;

    impl ReconciliationFetcher for EmptyFetcher {
        fn fetch<'a>(
            &'a self,
            _url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async { Ok(b"[]".to_vec()) })
        }
    }

    #[tokio::test]
    async fn recorder_append_failure_is_wrapped_as_a_fatal_source_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = SourceEventSink::open(dir.path().join("source.log")).unwrap();
        sink.fail_next_append();
        let fetcher = DurableRecordingFetcher {
            inner: Arc::new(EmptyFetcher),
            sink: Arc::new(tokio::sync::Mutex::new(sink)),
        };

        let error = fetcher
            .fetch("https://api.example.com/activity")
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SourceError::Fatal { message }
                if message.contains("source-log append failed: I/O error: injected append failure")
        ));
    }

    #[tokio::test]
    async fn control_commit_shares_one_context_across_read_buckets() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let observed_at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
        let row = |epoch: i64, asset: &str, market: &str, tx: &str| {
            json!({
                "proxyWallet": wallet,
                "timestamp": epoch,
                "conditionId": market,
                "type": "TRADE",
                "size": "1",
                "usdcSize": "0.5",
                "transactionHash": tx,
                "price": "0.5",
                "asset": asset,
                "side": "BUY",
                "outcomeIndex": 0,
                "outcome": "Yes",
                "isCombo": false
            })
        };
        let parsed = parse_activity_response(
            &serde_json::to_vec(&vec![
                row(1, "asset-first", "market-first", "0xfirst"),
                row(2, "asset-second", "market-second", "0xsecond"),
            ])
            .unwrap(),
            wallet,
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(observed_at),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap();
        let activity = CompleteActivityRead {
            requested_wallet: wallet,
            fixed_end: 100,
            rows: parsed.rows,
            pages: Vec::new(),
        };
        let prepared = PreparedActivity {
            mapping: activity.asset_mapping().unwrap(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            no_copy_dispositions: HashMap::new(),
            unresolved_assets: BTreeMap::new(),
            metadata_reads: BTreeMap::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(EmptyFetcher);
        let source_log = Arc::new(tokio::sync::Mutex::new(
            SourceEventSink::open(dir.path().join("source.log")).unwrap(),
        ));
        let resolver = Arc::new(AssetIdentityResolver::new(
            Arc::clone(&fetcher),
            "https://gamma.example.com".to_owned(),
            10,
            source_log,
        ));
        let validator = CausalPositionValidator::new(
            fetcher,
            "https://data.example.com",
            "fixture-generation",
            resolver,
        );
        let (control_tx, mut control_rx) = mpsc::channel(2);
        let actor = tokio::spawn(async move {
            let mut first_context = None;
            for source_epoch in [1, 2] {
                let Some(OrchestratorControl::CommitActivityBucket {
                    context, committed, ..
                }) = control_rx.recv().await
                else {
                    return false;
                };
                if let Some(first_context) = &first_context
                    && !Arc::ptr_eq(first_context, &context)
                {
                    return false;
                }
                first_context.get_or_insert_with(|| Arc::clone(&context));
                committed
                    .send(Ok(crate::bucket_commit::BucketCommitResult {
                        wallet,
                        source_epoch,
                        dispositions: BTreeMap::new(),
                        pending: Vec::new(),
                        newly_fenced: None,
                        already_committed: false,
                    }))
                    .unwrap();
            }
            true
        });

        assert!(
            validator
                .commit_control(wallet, &activity, &prepared, &control_tx, true)
                .await
                .unwrap()
        );
        assert!(actor.await.unwrap());
    }

    #[test]
    fn direct_batch_surfaces_fence_after_committing_its_prefix() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let observed_at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
        let row = |epoch: i64, activity_type: &str, asset: &str, market: &str, tx: &str| {
            json!({
                "proxyWallet": wallet,
                "timestamp": epoch,
                "conditionId": market,
                "type": activity_type,
                "size": "1",
                "usdcSize": "0.5",
                "transactionHash": tx,
                "price": "0.5",
                "asset": asset,
                "side": "BUY",
                "outcomeIndex": 0,
                "outcome": "Yes",
                "isCombo": false
            })
        };
        let parsed = parse_activity_response(
            &serde_json::to_vec(&vec![
                row(1, "TRADE", "asset-first", "market-first", "0xfirst"),
                row(2, "REDEEM", "asset-fence", "market-fence", "0xfence"),
                row(3, "TRADE", "asset-later", "market-later", "0xlater"),
            ])
            .unwrap(),
            wallet,
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(observed_at),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap();
        let aggregates = aggregate_activity_rows(&parsed.rows).unwrap();
        let first_id = aggregates[0].group_id.key().clone();
        let fence_id = aggregates[1].group_id.key().clone();
        let later_id = aggregates[2].group_id.key().clone();
        let activity = CompleteActivityRead {
            requested_wallet: wallet,
            fixed_end: 100,
            rows: parsed.rows,
            pages: Vec::new(),
        };
        let prepared = PreparedActivity {
            mapping: activity.asset_mapping().unwrap(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            no_copy_dispositions: HashMap::new(),
            unresolved_assets: BTreeMap::new(),
            metadata_reads: BTreeMap::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut engine =
            BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
        paper.set_cursor(&wallet, 0).unwrap();
        let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
        engine
            .install_anchors(&[AnchorInstall {
                wallet,
                balances: Vec::new(),
                cutoff: 0,
                proof: AnchorProof {
                    positions_proof_hash: "fixture".to_owned(),
                    activity_bounds_json: "[]".to_owned(),
                    source_log_generation: "fixture-generation".to_owned(),
                    document: "{}".to_owned(),
                    recorded_at_unix: 0,
                },
                expected: AnchorExpectation {
                    ledger_hash: captured.hash,
                    cursor: captured.cursor,
                    anchor_seq: captured.anchor_seq,
                    coverage_generation: captured.coverage_generation,
                },
            }])
            .unwrap();

        let error = commit_direct(
            wallet,
            &activity,
            &prepared,
            &mut engine,
            false,
            "fixture-generation",
        )
        .unwrap_err();

        assert!(matches!(error, CausalPositionError::Fenced { .. }));
        assert!(paper.activity_group_state(&first_id).unwrap().is_some());
        assert!(paper.activity_group_state(&fence_id).unwrap().is_some());
        assert!(paper.activity_group_state(&later_id).unwrap().is_none());
        assert!(paper.is_wallet_fenced(&wallet).unwrap());
        assert!(engine.is_fenced(&wallet));
        assert_eq!(
            engine
                .ledger()
                .position(&wallet)
                .and_then(|snapshot| {
                    snapshot.positions.get(&MarketOutcomeId::new(
                        MarketId(VenueMarketId("market-first".to_owned())),
                        OutcomeId(0),
                    ))
                })
                .map(|position| position.long_contracts.atomic()),
            Some(1_000_000)
        );
    }
}
