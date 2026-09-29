//! Durable pre-publication admission checks for runtime watchlist additions (#544).
//!
//! Lane C owns the history/fence boundary here. Lane D extends the marked seam
//! with the causal positions bracket; no sidecar or positions overlay is accepted.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
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
    MembershipAdmissionReceipt, MembershipChange, MembershipProofError, MembershipProofManifest,
    MembershipReason, RankingMembershipArtifact, SealedKnockoutEvidence,
};
use crate::position_seeder::{
    AnchorInstall, CausalPositionError, CausalPositionValidator, FailureClass, ValidationPurpose,
    is_deferred_causal_position_error,
};
use crate::watchlist_maintenance::MembershipCommit;

const ADMISSION_PREPARE_ACK_TIMEOUT_SECS: u64 = 30;
pub(crate) const CAPACITY_CONFIG_SOURCE_ID: &str = "pe-service.watchlist-capacity-config";
pub(crate) const RANKING_MEMBERSHIP_SOURCE_ID: &str = "pe-service.watchlist-ranking";
pub(crate) const MEMBERSHIP_ADMISSION_SOURCE_ID: &str = "pe-service.watchlist-admission";
pub(crate) const MEMBERSHIP_DEFERRAL_SOURCE_ID: &str = "pe-service.watchlist-deferral";
pub(crate) const KNOCKOUT_CAUSAL_SOURCE_ID: &str = "pe-service.watchlist-knockout";
pub(crate) const MEMBERSHIP_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub(crate) const MEMBERSHIP_ARTIFACT_PARSER_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("reconciled market history unavailable for wallet {wallet}")]
    MissingHistory { wallet: WalletAddress },
    #[error("newly admitted wallet {wallet} is durably fenced")]
    Fenced { wallet: WalletAddress },
    #[error("orchestrator control channel closed before admission preparation")]
    ControlClosed,
    #[error("orchestrator admission preparation acknowledgement closed")]
    AcknowledgementClosed,
    #[error("orchestrator admission preparation exceeded {0} seconds")]
    AcknowledgementTimeout(u64),
    #[error("orchestrator rejected structural membership publication: {0}")]
    PublicationRejected(crate::watchlist_maintenance::PublishError),
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
    MembershipProof(#[from] MembershipProofError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod admission_tests {
    use super::*;
    use pe_paper_state::{AnchorInstallRecord, WalletHistoryStatusRecord};

    fn wallet(byte: u8) -> WalletAddress {
        WalletAddress([byte; 20])
    }

    fn complete_history(state: &PaperStateDb, wallet: WalletAddress) {
        state
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn fence_and_history_are_per_wallet_and_ready_order_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let wallets = (1..=5).map(wallet).collect::<Vec<_>>();
        for wallet in [wallets[0], wallets[2], wallets[3], wallets[4]] {
            complete_history(&state, wallet);
        }
        rusqlite::Connection::open(&path).unwrap().execute(
            "INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)",
            [wallets[3].to_string()],
        ).unwrap();
        let (tx, mut rx) = mpsc::channel(5);
        let actor = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(OrchestratorControl::PrepareAdmissions {
                wallets,
                acknowledged,
            }) = rx.recv().await
            {
                seen.extend(wallets);
                acknowledged.send(()).unwrap();
            }
            seen
        });
        let preparer = AdmissionPreparer::new(tx, state);
        let result = preparer.prepare(&wallets).await.unwrap();
        assert_eq!(result.admitted, vec![wallets[0], wallets[2], wallets[4]]);
        assert_eq!(
            result
                .deferred
                .iter()
                .map(|item| (item.wallet, item.kind))
                .collect::<Vec<_>>(),
            vec![
                (wallets[3], "fence.active"),
                (wallets[1], "history.missing")
            ]
        );
        drop(preparer);
        assert_eq!(
            actor.await.unwrap(),
            vec![wallets[0], wallets[2], wallets[4]]
        );
    }

    #[tokio::test]
    async fn closed_control_channel_aborts_preparation() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let candidate = wallet(5);
        complete_history(&state, candidate);
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let abort = AdmissionPreparer::new(tx, state)
            .prepare(&[candidate])
            .await
            .unwrap_err();
        assert!(matches!(abort.cause, AdmissionError::ControlClosed));
        assert!(abort.deferred.is_empty());
    }

    #[test]
    fn proof_preflight_retains_prior_wallet_deferral_on_shared_read_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let missing_validation = wallet(6);
        let bad_read = wallet(7);
        for wallet in [missing_validation, bad_read] {
            complete_history(&state, wallet);
        }
        state.set_cursor(&missing_validation, 1).unwrap();
        state
            .install_anchors(&[AnchorInstallRecord {
                history_status: None,
                wallet: missing_validation,
                balances: Vec::new(),
                activity_cutoff_unix: 1,
                anchored_at_unix: 1,
                ledger_hash_after: "ledger".to_owned(),
                positions_proof_hash: "positions".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "test".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: 1,
            }])
            .unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "DELETE FROM position_validations WHERE wallet_hex = ?1",
            [missing_validation.to_string()],
        )
        .unwrap();
        conn.execute("UPDATE wallet_history_status_v2 SET proof_json = CAST(X'FF' AS TEXT) WHERE wallet_hex = ?1", [bad_read.to_string()]).unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let preparer = AdmissionPreparer::new(tx, state);
        let abort = preparer
            .capture_proofs(&[missing_validation, bad_read])
            .unwrap_err();
        assert!(matches!(
            abort.cause,
            AdmissionError::MembershipProof(MembershipProofError::PaperState(_))
        ));
        assert_eq!(abort.deferred.len(), 1);
        assert_eq!(abort.deferred[0].wallet, missing_validation);
        assert_eq!(abort.deferred[0].kind, "proof.missing_validation");
    }
}

impl AdmissionError {
    pub fn class(&self) -> FailureClass {
        match self {
            Self::MissingHistory { .. } | Self::Fenced { .. } => FailureClass::WalletPersistent,
            Self::PositionValidation(error) => error.class(),
            Self::ValidationRejected(error) => error.class(),
            Self::MembershipProof(error) => error.class(),
            Self::PublicationRejected(error) => error.class(),
            Self::ControlClosed
            | Self::AcknowledgementClosed
            | Self::AcknowledgementTimeout(_)
            | Self::ValidationInstall(_)
            | Self::PaperState(_)
            | Self::PositionValidatorUnavailable
            | Self::MembershipSourceLogUnavailable
            | Self::MembershipArtifactEncoding(_)
            | Self::CapacityTargetOverflow { .. }
            | Self::MembershipSourceLog(_) => FailureClass::Shared,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::MissingHistory { .. } => "history.missing",
            Self::Fenced { .. } => "fence.active",
            Self::PositionValidation(error) => error.kind(),
            Self::ValidationRejected(error) => error.kind(),
            Self::MembershipProof(error) => error.kind(),
            Self::PublicationRejected(error) => error.kind(),
            Self::ControlClosed => "control.closed",
            Self::AcknowledgementClosed => "ack.closed",
            Self::AcknowledgementTimeout(_) => "ack.timeout",
            Self::ValidationInstall(_) => "anchor.durability",
            Self::PaperState(_) => "paper_state.read",
            Self::PositionValidatorUnavailable => "validator.unavailable",
            Self::MembershipSourceLogUnavailable => "source_log.unavailable",
            Self::MembershipArtifactEncoding(_) => "artifact.encoding",
            Self::CapacityTargetOverflow { .. } => "capacity.overflow",
            Self::MembershipSourceLog(_) => "source_log.append",
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Deferral {
    pub wallet: WalletAddress,
    pub stage: &'static str,
    pub class: FailureClass,
    pub kind: &'static str,
    pub message: String,
}

impl Deferral {
    fn from_error(wallet: WalletAddress, stage: &'static str, error: &AdmissionError) -> Self {
        Self {
            wallet,
            stage,
            class: error.class(),
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct AdmissionOutcome {
    pub admitted: Vec<WalletAddress>,
    pub deferred: Vec<Deferral>,
}

pub(crate) type CapturedProofs = Vec<(WalletAddress, MembershipProofManifest)>;

#[derive(Debug, thiserror::Error)]
#[error("{cause}")]
pub struct AdmissionAbort {
    pub cause: AdmissionError,
    pub deferred: Vec<Deferral>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeferralContext {
    FullRerank { batch_id: i64 },
    Capacity { generation: u64, target: usize },
    Knockout { batch_id: i64 },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeferralOutcome {
    Published { paper_seq: u64 },
    NoChange,
    PendingCapacity,
    AbortedShared { kind: &'static str },
}

#[derive(Serialize)]
pub struct MembershipDeferralArtifact {
    version: u8,
    context: DeferralContext,
    deferrals: Vec<Deferral>,
    outcome: DeferralOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorRefreshOutcome {
    Anchored,
    Skipped,
    Deferred,
    Cancelled,
}

/// The poller may cancel validation before the install handoff. Once handoff starts, the
/// coordinator retains the wallet until the install acknowledgement has been observed.
#[derive(Clone, Default)]
pub(crate) struct RefreshHandoff(Arc<AtomicU8>);

impl RefreshHandoff {
    const VALIDATING: u8 = 0;
    const PRE_SEND: u8 = 1;
    const SENT: u8 = 2;
    const CANCELLED: u8 = 3;

    pub(crate) fn cancel_before_handoff(&self) -> bool {
        loop {
            let phase = self.0.load(Ordering::Acquire);
            if phase == Self::CANCELLED {
                return true;
            }
            if phase != Self::VALIDATING && phase != Self::PRE_SEND {
                return false;
            }
            if self
                .0
                .compare_exchange(phase, Self::CANCELLED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == Self::CANCELLED
    }

    fn pre_send(&self) -> bool {
        self.0
            .compare_exchange(
                Self::VALIDATING,
                Self::PRE_SEND,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn begin_handoff(&self) -> bool {
        self.0
            .compare_exchange(
                Self::PRE_SEND,
                Self::SENT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
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
/// the orchestrator while holding the publication lock.
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

    /// Seed the ranking's last-trade cursor before the causal bracket captures its ledger hash.
    /// The validator needs the seeded cursor to stay unchanged through anchor installation.
    /// The validator-free branch first passes the durable history gate.
    fn seed_ranked_cursors(
        &self,
        wallets: &[WalletAddress],
        ranked_last_trade: Option<&HashMap<WalletAddress, i64>>,
    ) -> Result<(), AdmissionError> {
        if let Some(ranked_last_trade) = ranked_last_trade {
            let seeds = wallets
                .iter()
                .filter_map(|wallet| ranked_last_trade.get(wallet).map(|time| (*wallet, *time)))
                .collect::<Vec<_>>();
            self.inner.paper_state.seed_cursors_if_absent(&seeds)?;
        }
        Ok(())
    }

    pub(crate) async fn prepare_ranked(
        &self,
        additions: &[WalletAddress],
        ranked_last_trade: &HashMap<WalletAddress, i64>,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_with_cursors(additions, Some(ranked_last_trade))
            .await
    }

    /// Reject fences before validation and require complete history after the accepted
    /// anchor installation is acknowledged. Validator-free callers require complete
    /// history before handing preparation to the orchestrator.
    pub async fn prepare(
        &self,
        additions: &[WalletAddress],
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_with_cursors(additions, None).await
    }

    async fn prepare_with_cursors(
        &self,
        additions: &[WalletAddress],
        ranked_last_trade: Option<&HashMap<WalletAddress, i64>>,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        let preparer = &self.inner;
        let _attempt = preparer.attempt.lock().await;
        let mut admitted = Vec::new();
        let mut deferred = Vec::new();
        let mut eligible = Vec::new();
        for wallet in additions {
            match self.check_fences(&[*wallet]) {
                Ok(()) => eligible.push(*wallet),
                Err(error) if error.class() != FailureClass::Shared => {
                    deferred.push(Deferral::from_error(*wallet, "fence", &error))
                }
                Err(cause) => return Err(AdmissionAbort { cause, deferred }),
            }
        }
        if let Some(validator) = &preparer.validator {
            if let Err(cause) = self.seed_ranked_cursors(&eligible, ranked_last_trade) {
                return Err(AdmissionAbort { cause, deferred });
            }
            let outcomes = validator
                .validate_via_control(
                    &eligible,
                    &preparer.control_tx,
                    &preparer.paper_state,
                    ValidationPurpose::CatchUp,
                )
                .await;
            for (wallet, error) in outcomes.deferred {
                let error = AdmissionError::PositionValidation(error);
                deferred.push(Deferral::from_error(wallet, "validation", &error));
            }
            if let Some(cause) = outcomes.shared {
                return Err(AdmissionAbort {
                    cause: AdmissionError::PositionValidation(cause),
                    deferred,
                });
            }
            let mut remaining = outcomes.accepted;
            while !remaining.is_empty() {
                match self.install_anchors(remaining.clone()).await {
                    Ok(()) => {
                        admitted.extend(remaining.iter().map(|install| install.wallet));
                        break;
                    }
                    Err(error) if error.class() != FailureClass::Shared => {
                        let wallet = match &error {
                            AdmissionError::ValidationRejected(cause) => cause.wallet(),
                            _ => None,
                        };
                        let Some(wallet) = wallet else {
                            return Err(AdmissionAbort {
                                cause: error,
                                deferred,
                            });
                        };
                        deferred.push(Deferral::from_error(wallet, "anchor_install", &error));
                        remaining.retain(|install| install.wallet != wallet);
                    }
                    Err(cause) => return Err(AdmissionAbort { cause, deferred }),
                }
            }
        } else {
            for wallet in eligible {
                match self.check_prerequisites(&[wallet]) {
                    Ok(()) => {}
                    Err(error) if error.class() != FailureClass::Shared => {
                        deferred.push(Deferral::from_error(wallet, "history", &error));
                        continue;
                    }
                    Err(cause) => return Err(AdmissionAbort { cause, deferred }),
                }
                if let Err(cause) = self.seed_ranked_cursors(&[wallet], ranked_last_trade) {
                    return Err(AdmissionAbort { cause, deferred });
                }
                if let Err(cause) = self.prepare_locked(&[wallet]).await {
                    return Err(AdmissionAbort { cause, deferred });
                }
                admitted.push(wallet);
            }
        }
        let mut ready = Vec::new();
        for wallet in admitted {
            match self.check_prerequisites(&[wallet]) {
                Ok(()) => ready.push(wallet),
                Err(error) if error.class() != FailureClass::Shared => {
                    deferred.push(Deferral::from_error(wallet, "history", &error))
                }
                Err(cause) => return Err(AdmissionAbort { cause, deferred }),
            }
        }
        Ok(AdmissionOutcome {
            admitted: ready,
            deferred,
        })
    }

    /// One ranked-step attempt for live-only deferred wallets. A failure of one bracket does
    /// not hold back another wallet selected in the same batch.
    pub async fn prepare_live_reentries(
        &self,
        wallets: &[WalletAddress],
    ) -> Result<AdmissionOutcome, AdmissionError> {
        let mut ready = Vec::new();
        let mut deferred = Vec::new();
        for wallet in wallets {
            match self.prepare(&[*wallet]).await {
                Ok(outcome) => {
                    ready.extend(outcome.admitted);
                    deferred.extend(outcome.deferred);
                }
                Err(abort) => return Err(abort.cause),
            }
        }
        Ok(AdmissionOutcome {
            admitted: ready,
            deferred,
        })
    }

    /// Recheck, seed, synchronize one structural record, then publish its exact entries.
    /// Callers release the writer lock before sending or awaiting this control message.
    pub async fn publish_membership(
        &self,
        change: MembershipChange,
        replacements: Vec<WatchlistEntry>,
        checks: MembershipCommit,
    ) -> Result<AppendReceipt, crate::watchlist_maintenance::PublishError> {
        let (acknowledged, received) = oneshot::channel();
        self.inner
            .control_tx
            .send(OrchestratorControl::PublishMembership {
                change,
                replacements,
                checks: Box::new(checks),
                acknowledged,
            })
            .await
            .map_err(|_| {
                crate::watchlist_maintenance::PublishError::Shared(
                    "control channel closed".to_owned(),
                )
            })?;
        received.await.map_err(|_| {
            crate::watchlist_maintenance::PublishError::Shared(
                "publication acknowledgement closed".to_owned(),
            )
        })?
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

    pub(crate) fn capture_proofs(
        &self,
        additions: &[WalletAddress],
    ) -> Result<(CapturedProofs, Vec<Deferral>), AdmissionAbort> {
        let mut captured = Vec::with_capacity(additions.len());
        let mut deferred = Vec::new();
        for wallet in additions {
            match MembershipProofManifest::capture(&self.inner.paper_state, &[*wallet]) {
                Ok(proof) => captured.push((*wallet, proof)),
                Err(error) => {
                    let error = AdmissionError::MembershipProof(error);
                    if error.class() == FailureClass::Shared {
                        return Err(AdmissionAbort {
                            cause: error,
                            deferred,
                        });
                    }
                    deferred.push(Deferral::from_error(*wallet, "proof", &error));
                }
            }
        }
        Ok((captured, deferred))
    }

    /// Append captured manifests without reading mutable paper state again.
    pub(crate) async fn record_admission_proofs(
        &self,
        captured: &[(WalletAddress, MembershipProofManifest)],
    ) -> Result<Vec<MembershipAdmissionReceipt>, AdmissionError> {
        let mut receipts = Vec::with_capacity(captured.len());
        for (wallet, proof) in captured {
            let receipt = self
                .record_artifact(
                    MEMBERSHIP_ADMISSION_SOURCE_ID,
                    &MembershipAdmissionArtifact {
                        wallet: *wallet,
                        proof: proof.clone(),
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

    pub(crate) async fn record_deferrals(
        &self,
        context: DeferralContext,
        deferrals: Vec<Deferral>,
        outcome: DeferralOutcome,
    ) {
        if deferrals.is_empty() {
            return;
        }
        for deferral in &deferrals {
            warn!(wallet = %deferral.wallet, stage = deferral.stage, kind = deferral.kind, ?deferral.class, ?context, "membership admission deferred");
        }
        let artifact = MembershipDeferralArtifact {
            version: 1,
            context,
            deferrals,
            outcome,
        };
        if let Err(error) = self
            .record_artifact(MEMBERSHIP_DEFERRAL_SOURCE_ID, &artifact)
            .await
        {
            warn!(%error, "membership deferral audit append failed");
        }
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
        self.prepare_if_due_observed(wallet, now_unix, refresh_secs, None)
            .await
    }

    pub(crate) async fn prepare_if_due_observed(
        &self,
        wallet: WalletAddress,
        now_unix: i64,
        refresh_secs: u64,
        handoff: Option<&RefreshHandoff>,
    ) -> Result<AnchorRefreshOutcome, AdmissionError> {
        let preparer = &self.inner;
        let Ok(_attempt) = preparer.attempt.try_lock() else {
            return Ok(AnchorRefreshOutcome::Skipped);
        };
        let coverage = preparer.paper_state.wallet_coverage(&wallet)?;
        if !anchor_refresh_due(&coverage, now_unix, refresh_secs) {
            return Ok(AnchorRefreshOutcome::Skipped);
        }
        match self.check_prerequisites(&[wallet]) {
            Ok(()) => {}
            Err(AdmissionError::Fenced { .. }) => {
                warn!(wallet = %wallet, "anchor refresh skipped because wallet is durably fenced");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            Err(error) => return Err(error),
        }
        let validator = preparer
            .validator
            .as_ref()
            .ok_or(AdmissionError::PositionValidatorUnavailable)?;
        let purpose = match coverage.activity_cutoff_unix {
            Some(cutoff)
                if !coverage.reanchor_required
                    && coverage.anchor_seq.is_some()
                    && preparer.paper_state.cursor(&wallet)?.is_some() =>
            {
                ValidationPurpose::RoutineRefresh { cutoff }
            }
            _ => ValidationPurpose::CatchUp,
        };
        let outcomes = validator
            .validate_via_control(
                &[wallet],
                &preparer.control_tx,
                &preparer.paper_state,
                purpose,
            )
            .await;
        if let Some(error) = outcomes.shared {
            // Periodic refresh retains its existing retry cadence for venue transport
            // failures. Admission attempts still abort on these shared errors.
            if is_deferred_causal_position_error(&error) {
                return Ok(AnchorRefreshOutcome::Deferred);
            }
            return Err(AdmissionError::PositionValidation(error));
        }
        if let Some((_, error)) = outcomes.deferred.into_iter().next() {
            if let CausalPositionError::Fenced { wallet } = error {
                warn!(wallet = %wallet, "anchor refresh skipped because causal validation fenced the wallet");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            if is_deferred_causal_position_error(&error) || error.class() != FailureClass::Shared {
                return Ok(AnchorRefreshOutcome::Deferred);
            }
            return Err(AdmissionError::PositionValidation(error));
        }
        if let Some(handoff) = handoff
            && !handoff.pre_send()
        {
            return Ok(AnchorRefreshOutcome::Cancelled);
        }
        let permit = self
            .inner
            .control_tx
            .reserve()
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
        if let Some(handoff) = handoff
            && !handoff.begin_handoff()
        {
            return Ok(AnchorRefreshOutcome::Cancelled);
        }
        let installed = Self::send_install_anchors(permit, outcomes.accepted).await;
        match installed {
            Ok(()) => Ok(AnchorRefreshOutcome::Anchored),
            Err(AdmissionError::ValidationRejected(_)) => Ok(AnchorRefreshOutcome::Deferred),
            Err(error) => Err(error),
        }
    }

    fn check_fences(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        for wallet in additions {
            if self.inner.paper_state.is_wallet_fenced(wallet)? {
                return Err(AdmissionError::Fenced { wallet: *wallet });
            }
        }
        Ok(())
    }

    fn check_prerequisites(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        self.check_fences(additions)?;
        let preparer = &self.inner;
        for wallet in additions {
            if !preparer.paper_state.wallet_history_complete(wallet)? {
                return Err(AdmissionError::MissingHistory { wallet: *wallet });
            }
        }
        Ok(())
    }

    async fn prepare_locked(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        let preparer = &self.inner;
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
        let permit = self
            .inner
            .control_tx
            .reserve()
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
        Self::send_install_anchors(permit, installs).await
    }

    async fn send_install_anchors(
        permit: mpsc::Permit<'_, OrchestratorControl>,
        installs: Vec<AnchorInstall>,
    ) -> Result<(), AdmissionError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        permit.send(OrchestratorControl::InstallAnchors {
            installs,
            acknowledged,
        });
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
    /// Exercise production artifact capture and locked publication from scenario harnesses.
    #[cfg(feature = "scenario")]
    pub async fn scenario_publish_ranking(
        &self,
        live: &crate::live_watchlist::LiveWatchlist,
        writer_lock: &Mutex<()>,
        entries: Vec<WatchlistEntry>,
        last_trade: &std::collections::HashMap<WalletAddress, i64>,
        cap: usize,
    ) -> Result<(), crate::watchlist_maintenance::MembershipApplyError> {
        use crate::paper_recovery::SealedMembershipEvidence;
        use crate::watchlist_maintenance::{
            MembershipApplyError, MembershipPublication, PublicationBinding, PublishError,
            apply_ranked_membership_locked, ranked_membership_change_set,
        };
        let (_, additions) =
            ranked_membership_change_set(&live.structural_membership(), &entries, cap);
        let ranking = self
            .record_ranking_membership(Some(546), entries.clone())
            .await
            .map_err(|error| {
                MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
            })?;
        let (proofs, deferred) = self.capture_proofs(&additions).map_err(|error| {
            MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
        })?;
        if !deferred.is_empty() {
            return Err(MembershipApplyError::Publication(PublishError::Shared(
                "scenario admissions deferred".to_owned(),
            )));
        }
        let binding = PublicationBinding::capture(live.structural_membership(), &proofs).map_err(
            |error| MembershipApplyError::Publication(PublishError::Shared(error.to_string())),
        )?;
        let admissions = self
            .record_admission_proofs(&proofs)
            .await
            .map_err(|error| {
                MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
            })?;
        let evidence =
            SealedMembershipEvidence::full_rerank(ranking, admissions).map_err(|error| {
                MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
            })?;
        let _writer = writer_lock.lock().await;
        apply_ranked_membership_locked(
            live,
            &self.inner.paper_state,
            self,
            MembershipPublication {
                reason: MembershipReason::FullRerank,
                ranking_batch_id: Some(546),
                evidence,
                binding,
            },
            &entries,
            last_trade,
            cap,
            _writer,
            None,
            &[],
        )
        .await?;
        Ok(())
    }
}
