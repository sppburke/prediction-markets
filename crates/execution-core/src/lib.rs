//! `pe-execution-core` — mode-aware execution dispatcher for the prediction-edge system.
//!
//! Routes ordinary `OrderIntent` values to paper execution. The isolated canary actor is the only
//! credentialed execution owner:
//! - `Shadow` / `Paper` → `PaperExecutor` (from `pe-strategy-winner-follow`)
//! - `LiveTiny` / `Promoted` → fail closed
//!
//! All executors write durable records to the event log before returning.

#![forbid(unsafe_code)]

pub mod canary;
pub mod canary_actor;
pub mod dispatcher;
pub mod error;

pub use canary::{
    AttemptAttribution, AttemptOrigin, AttemptPhase, CampaignAttemptRecord, CampaignAuthorization,
    CampaignStage, CanaryAdmission, CanaryCampaignState, CanaryEvent, CanaryExposureAmounts,
    CanaryJournal, CanaryPosition, CanaryQuote, CanaryReconciliation, CanaryResolution,
    CanaryStateError, ClosureReason, CommandReceipt, OrganicStageAuthorization, PendingAttempt,
    ProbeAuthorization, artifact_evidence_hash, probe_authority_hash, raw_evidence_hash,
    response_evidence_hash,
};
pub use canary_actor::{
    CANARY_COMMAND_QUEUE_CAPACITY, CANARY_POST_TIMEOUT_SECS, CANARY_RECONCILIATION_TIMEOUT_SECS,
    CanaryActor, CanaryActorError, CanaryActorHandle, CanaryReconciler, CanarySubmitter,
    RawReconciliation,
};
pub use dispatcher::{DispatchResult, ExecutionDispatcher};
pub use error::ExecutionError;
pub use pe_core_types::{
    RawArtifactObservation, RawEvidence, RawHttpResponse, RawTransportFailure, TransportErrorClass,
};
