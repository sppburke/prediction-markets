//! Durable pre-publication admission checks for runtime watchlist additions (#544).
//!
//! Lane C owns the history/fence boundary here. Lane D extends the marked seam
//! with the causal positions bracket; no sidecar or positions overlay is accepted.

use std::sync::Arc;
use std::time::Duration;

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn};
use pe_paper_state::{PaperStateDb, WalletCoverage};
use pe_trader_index::WatchlistEntry;
use serde::Serialize;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::activity_ingest::{SourceLogHandle, SourceLogHandleError};
use crate::bucket_commit::AnchorInstallError;
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{
    CapacityMembershipArtifact, KnockoutCausalArtifact, MembershipAdmissionArtifact,
    MembershipAdmissionReceipt, MembershipChange, MembershipProofManifest, MembershipReason,
    RankingMembershipArtifact, SealedKnockoutEvidence,
};
use crate::position_seeder::{
    AnchorInstall, CausalPositionError, CausalPositionValidator, is_deferred_causal_position_error,
};

const ADMISSION_PREPARE_ACK_TIMEOUT_SECS: u64 = 30;
pub(crate) const CAPACITY_CONFIG_SOURCE_ID: &str = "pe-service.watchlist-capacity-config";
pub(crate) const RANKING_MEMBERSHIP_SOURCE_ID: &str = "pe-service.watchlist-ranking";
pub(crate) const MEMBERSHIP_ADMISSION_SOURCE_ID: &str = "pe-service.watchlist-admission";
pub(crate) const KNOCKOUT_CAUSAL_SOURCE_ID: &str = "pe-service.watchlist-knockout";
pub(crate) const MEMBERSHIP_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub(crate) const MEMBERSHIP_ARTIFACT_PARSER_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("reconciled market history unavailable for {missing} newly admitted wallet(s)")]
    MissingHistory { missing: usize },
    #[error("{fenced} newly admitted wallet(s) are durably fenced")]
    Fenced { fenced: usize },
    #[error("orchestrator control channel closed before admission preparation")]
    ControlClosed,
    #[error("orchestrator admission preparation acknowledgement closed")]
    AcknowledgementClosed,
    #[error("orchestrator admission preparation exceeded {0} seconds")]
    AcknowledgementTimeout(u64),
    #[error("orchestrator rejected structural membership publication: {0}")]
    PublicationRejected(String),
    #[error("causal current-position validation unavailable: {0}")]
    PositionValidation(#[source] CausalPositionError),
    #[error("orchestrator rejected the accepted position brackets: {0}")]
    ValidationRejected(AnchorInstallError),
    #[error("accepted position bracket durability failed: {0}")]
    ValidationInstall(String),
    #[error("paper-state admission read failed: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("anchor refresh requires a causal position validator")]
    PositionValidatorUnavailable,
    #[error("membership publication requires the synchronized source-log handle")]
    MembershipSourceLogUnavailable,
    #[error("encode immutable membership artifact: {0}")]
    MembershipArtifactEncoding(#[source] serde_json::Error),
    #[error("accepted capacity target {target} cannot be represented durably")]
    CapacityTargetOverflow { target: usize },
    #[error("record immutable membership artifact: {0}")]
    MembershipSourceLog(#[from] SourceLogHandleError),
    #[error("capture immutable membership proof: {0}")]
    MembershipProof(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorRefreshOutcome {
    Anchored,
    Skipped,
    Deferred,
}

/// The runtime predicate shared by periodic refresh and ordinary boot reuse.
#[must_use]
pub fn anchor_refresh_due(coverage: &WalletCoverage, now_unix: i64, refresh_secs: u64) -> bool {
    let refresh_secs = i64::try_from(refresh_secs).unwrap_or(i64::MAX);
    coverage.reanchor_required
        || coverage
            .anchored_at_unix
            .is_none_or(|anchored| now_unix.saturating_sub(anchored) > refresh_secs)
}

/// Shared serialized admission coordinator. Its durable checks are repeated by
/// the membership writer while holding the publication lock.
#[derive(Clone)]
pub struct AdmissionPreparer {
    inner: Arc<Preparer>,
    source_log: Option<SourceLogHandle>,
}

struct Preparer {
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    attempt: Mutex<()>,
    validator: Option<CausalPositionValidator>,
}

impl AdmissionPreparer {
    pub fn new(
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
    ) -> Self {
        Self {
            inner: Arc::new(Preparer {
                control_tx,
                paper_state,
                attempt: Mutex::new(()),
                validator: None,
            }),
            source_log: None,
        }
    }

    /// Production constructor with the five-step causal positions bracket.
    pub fn with_validator(
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
        validator: CausalPositionValidator,
    ) -> Self {
        Self {
            inner: Arc::new(Preparer {
                control_tx,
                paper_state,
                attempt: Mutex::new(()),
                validator: Some(validator),
            }),
            source_log: None,
        }
    }

    /// Install the process-wide synchronized source-log handle used to retain accepted
    /// capacity-change inputs before their structural membership publication.
    #[must_use]
    pub fn with_source_log(mut self, source_log: SourceLogHandle) -> Self {
        self.source_log = Some(source_log);
        self
    }

    /// Prove Lane C's durable prerequisites, then hand ownership to the
    /// orchestrator. `#544 Lane D integration`: complete the causal positions
    /// bracket before sending this acknowledgement-bearing command.
    pub async fn prepare(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        if additions.is_empty() {
            return Ok(());
        }
        let preparer = &self.inner;
        let _attempt = preparer.attempt.lock().await;

        self.check_prerequisites(additions)?;
        self.prepare_locked(additions).await
    }

    /// Synchronize one structural membership record, then publish its exact replacement entries.
    pub async fn publish_membership(
        &self,
        change: MembershipChange,
        replacements: Vec<WatchlistEntry>,
    ) -> Result<AppendReceipt, AdmissionError> {
        let (acknowledged, received) = oneshot::channel();
        self.inner
            .control_tx
            .send(OrchestratorControl::PublishMembership {
                change,
                replacements,
                acknowledged,
            })
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
        received
            .await
            .map_err(|_| AdmissionError::AcknowledgementClosed)?
            .map_err(AdmissionError::PublicationRejected)
    }

    /// Durably retain the exact accepted capacity request before its membership record can
    /// reference the returned immutable source-log receipt.
    pub(crate) async fn record_capacity_config(
        &self,
        generation: u64,
        target: usize,
        published_entries: Vec<WatchlistEntry>,
    ) -> Result<AppendReceipt, AdmissionError> {
        let durable_target =
            u64::try_from(target).map_err(|_| AdmissionError::CapacityTargetOverflow { target })?;
        self.record_artifact(
            CAPACITY_CONFIG_SOURCE_ID,
            &CapacityMembershipArtifact {
                generation,
                target: durable_target,
                published_entries,
            },
        )
        .await
    }

    /// Retain the exact batch-pinned ranking rows used by a full-rerank, or the exact
    /// candidate rows used by a knockout backfill.
    pub(crate) async fn record_ranking_membership(
        &self,
        batch_id: Option<i64>,
        entries: Vec<WatchlistEntry>,
    ) -> Result<AppendReceipt, AdmissionError> {
        self.record_artifact(
            RANKING_MEMBERSHIP_SOURCE_ID,
            &RankingMembershipArtifact { batch_id, entries },
        )
        .await
    }

    /// Snapshot each admitted wallet's installed immutable proof preimages into the source log.
    pub(crate) async fn record_admission_proofs(
        &self,
        additions: &[WalletAddress],
    ) -> Result<Vec<MembershipAdmissionReceipt>, AdmissionError> {
        let mut receipts = Vec::with_capacity(additions.len());
        for wallet in additions {
            let proof = MembershipProofManifest::capture(&self.inner.paper_state, &[*wallet])
                .map_err(|error| AdmissionError::MembershipProof(error.to_string()))?;
            let receipt = self
                .record_artifact(
                    MEMBERSHIP_ADMISSION_SOURCE_ID,
                    &MembershipAdmissionArtifact {
                        wallet: *wallet,
                        proof,
                    },
                )
                .await?;
            receipts.push(MembershipAdmissionReceipt {
                wallet: *wallet,
                receipt,
            });
        }
        Ok(receipts)
    }

    /// Retain each knockout's policy and cursor inputs and return the receipt-bound evidence.
    pub(crate) async fn record_knockout_inputs(
        &self,
        inputs: Vec<(MembershipReason, KnockoutCausalArtifact)>,
    ) -> Result<Vec<SealedKnockoutEvidence>, AdmissionError> {
        let mut evidence = Vec::with_capacity(inputs.len());
        for (reason, input) in inputs {
            let wallet = input.wallet;
            let causal_receipt = self
                .record_artifact(KNOCKOUT_CAUSAL_SOURCE_ID, &input)
                .await?;
            evidence.push(SealedKnockoutEvidence {
                wallet,
                reason,
                causal_receipt,
            });
        }
        Ok(evidence)
    }

    async fn record_artifact<T: Serialize>(
        &self,
        source_id: &str,
        artifact: &T,
    ) -> Result<AppendReceipt, AdmissionError> {
        let source_log = self
            .source_log
            .as_ref()
            .ok_or(AdmissionError::MembershipSourceLogUnavailable)?;
        let payload =
            serde_json::to_vec(artifact).map_err(AdmissionError::MembershipArtifactEncoding)?;
        let now = time::OffsetDateTime::now_utc();
        source_log
            .append(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version: MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
                parser_version: MEMBERSHIP_ARTIFACT_PARSER_VERSION,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload,
            })
            .await
            .map_err(AdmissionError::from)
    }

    /// Re-anchor one due wallet under the shared bracket mutex.
    pub async fn prepare_if_due(
        &self,
        wallet: WalletAddress,
        now_unix: i64,
        refresh_secs: u64,
    ) -> Result<AnchorRefreshOutcome, AdmissionError> {
        let preparer = &self.inner;
        let _attempt = preparer.attempt.lock().await;
        let coverage = preparer.paper_state.wallet_coverage(&wallet)?;
        if !anchor_refresh_due(&coverage, now_unix, refresh_secs) {
            return Ok(AnchorRefreshOutcome::Skipped);
        }
        match self.check_prerequisites(&[wallet]) {
            Ok(()) => {}
            Err(AdmissionError::Fenced { fenced }) => {
                warn!(wallet = %wallet, fenced, "anchor refresh skipped because wallet is durably fenced");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            Err(error) => return Err(error),
        }
        let validator = preparer
            .validator
            .as_ref()
            .ok_or(AdmissionError::PositionValidatorUnavailable)?;
        let installs = match validator
            .validate_via_control(&[wallet], &preparer.control_tx, &preparer.paper_state)
            .await
        {
            Ok(installs) => installs,
            Err(error) if is_deferred_causal_position_error(&error) => {
                return Ok(AnchorRefreshOutcome::Deferred);
            }
            Err(CausalPositionError::Fenced { wallet }) => {
                warn!(wallet = %wallet, "anchor refresh skipped because causal validation fenced the wallet");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            Err(error) => return Err(AdmissionError::PositionValidation(error)),
        };
        match self.install_anchors(installs).await {
            Ok(()) => Ok(AnchorRefreshOutcome::Anchored),
            Err(AdmissionError::ValidationRejected(_)) => Ok(AnchorRefreshOutcome::Deferred),
            Err(error) => Err(error),
        }
    }

    fn check_prerequisites(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        let preparer = &self.inner;

        let mut fenced = 0usize;
        let mut missing = 0usize;
        for wallet in additions {
            if preparer.paper_state.is_wallet_fenced(wallet)? {
                fenced = fenced.saturating_add(1);
            }
            if !preparer.paper_state.wallet_history_complete(wallet)? {
                missing = missing.saturating_add(1);
            }
        }
        if fenced > 0 {
            return Err(AdmissionError::Fenced { fenced });
        }
        if missing > 0 {
            return Err(AdmissionError::MissingHistory { missing });
        }
        Ok(())
    }

    async fn prepare_locked(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        let preparer = &self.inner;
        if let Some(validator) = &preparer.validator {
            let installs = validator
                .validate_via_control(additions, &preparer.control_tx, &preparer.paper_state)
                .await
                .map_err(AdmissionError::PositionValidation)?;
            return self.install_anchors(installs).await;
        }
        let (acknowledged, acknowledgement) = oneshot::channel();
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            async {
                preparer
                    .control_tx
                    .send(OrchestratorControl::PrepareAdmissions {
                        wallets: additions.to_vec(),
                        acknowledged,
                    })
                    .await
                    .map_err(|_| AdmissionError::ControlClosed)?;
                acknowledgement
                    .await
                    .map_err(|_| AdmissionError::AcknowledgementClosed)
            },
        )
        .await
        .map_err(|_| {
            AdmissionError::AcknowledgementTimeout(ADMISSION_PREPARE_ACK_TIMEOUT_SECS)
        })??;
        Ok(())
    }

    async fn install_anchors(&self, installs: Vec<AnchorInstall>) -> Result<(), AdmissionError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.inner
            .control_tx
            .send(OrchestratorControl::InstallAnchors {
                installs,
                acknowledged,
            })
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            acknowledgement,
        )
        .await
        .map_err(|_| AdmissionError::AcknowledgementTimeout(ADMISSION_PREPARE_ACK_TIMEOUT_SECS))?
        .map_err(|_| AdmissionError::AcknowledgementClosed)?
        .map_err(|error| match error {
            AnchorInstallError::Durability(message) => AdmissionError::ValidationInstall(message),
            rejection => AdmissionError::ValidationRejected(rejection),
        })
    }
}
