//! `pe-execution-core` — auditable execution primitives for the prediction-edge system.
//!
//! The existing dispatcher continues to route ordinary `OrderIntent` values as before:
//! - `Shadow` / `Paper` → `PaperExecutor` (from `pe-strategy-winner-follow`)
//! - `LiveTiny` / `Promoted` → fail closed
//!
//! The isolated canary actor remains unchanged. The ordinary-live modules expose a separate
//! two-phase executor, account-tagged journal, and redemption state machine for service wiring.

#![forbid(unsafe_code)]

pub mod canary;
pub mod canary_actor;
pub mod dispatcher;
pub mod error;
pub mod live_executor;
pub mod live_journal;
pub mod redemption_machine;

pub use canary::{
    AttemptAttribution, AttemptOrigin, AttemptPhase, CampaignAttemptRecord, CampaignAuthorization,
    CampaignStage, CanaryAdmission, CanaryCampaignState, CanaryEvent, CanaryExposureAmounts,
    CanaryJournal, CanaryPosition, CanaryQuote, CanaryReconciliation, CanaryResolution,
    CanaryStateError, ClosureReason, CommandReceipt, OrganicStageAuthorization, PendingAttempt,
    ProbeAuthorization, artifact_evidence_hash, organic_evidence_bundle_hash, probe_authority_hash,
    raw_evidence_hash, response_evidence_hash,
};
pub use canary_actor::{
    CANARY_COMMAND_QUEUE_CAPACITY, CANARY_POST_TIMEOUT_SECS, CANARY_RECONCILIATION_TIMEOUT_SECS,
    CanaryActor, CanaryActorError, CanaryActorHandle, CanaryReconciler, CanarySubmitter,
    RawReconciliation,
};
pub use dispatcher::{DispatchResult, ExecutionDispatcher};
pub use error::ExecutionError;
pub use live_executor::{
    FrozenLiveTarget, LiveAccountStateFuture, LiveAdmissionArtifact, LiveExecutor,
    LiveExecutorError, LiveModeSnapshot, LiveOrderOutcome, LiveOrderRequest, LiveOrderVenue,
    LivePostClassification, LivePostFuture, LivePostParseError, LivePrepareResult,
    LiveReconciliationFuture, LiveVenueAccountReadError, LiveVenueAccountState,
    LiveVenuePreparationError, LiveVenuePrepareFuture, LiveVenuePrepareRequest, LiveVenuePrepared,
    LiveVenueReconciledOutcome, LiveVenueReconciliation, LiveVenueReconciliationError,
    PreparedLiveOrder,
};
pub use live_journal::{
    CredentialBindingIdentity, LadderAskAudit, LadderPlanAudit, LiveAccountReadFailure,
    LiveAccountStateAudit, LiveAdmissionArtifactAudit, LiveAdmissionEvaluationAudit,
    LiveAdmissionRefusal, LiveAdmissionVerdict, LiveControlMode, LiveFeeEvidenceAudit, LiveJournal,
    LiveJournalError, LiveJournalEvent, LiveJournalOrderOutcome, LiveJournalPayload,
    LiveMarketEvidenceAudit, LiveModeTransitionAudit, LiveModeTransitionReason,
    LiveOrderAmbiguityKind, LiveOrderIdentity, LiveOrderPostAudit, LiveOrderPreparationFailedAudit,
    LiveOrderPreparationFailure, LiveOrderPreparedAudit, LiveOrderReconciliationAudit,
    LiveOrderRejectKind, LiveReconciliationSource, RedemptionAttemptIdentity,
    RedemptionCustodyAudit, RedemptionReceiptAudit, RedemptionReceiptStatusAudit,
    RedemptionRequestAudit, RedemptionRequestedAudit, RedemptionTransactionAudit, replay_account,
};
pub use pe_core_types::{
    RawArtifactObservation, RawEvidence, RawHttpResponse, RawTransportFailure, TransportErrorClass,
};
pub use redemption_machine::{
    LIVE_REDEMPTION_SURFACE_AFTER_ATTEMPTS, RedemptionAction, RedemptionAttempt,
    RedemptionAttemptState, RedemptionDriverError, RedemptionEvent, RedemptionFailureKind,
    RedemptionPassInput, RedemptionPassResult, RedemptionPosture, RedemptionStatusObservation,
    RedemptionStatusReadError, RedemptionStatusReader, advance, redemption_posture,
    run_redemption_pass,
};
