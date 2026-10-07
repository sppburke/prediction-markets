//! Control-plane messages applied by the single-owner orchestrator loop.
//!
//! Every runtime watchlist addition must initialize the newly followed wallet's first-entry
//! history and current leader positions before that wallet becomes visible in
//! [`crate::live_watchlist`]. This channel preserves the orchestrator's single ownership of
//! those mutable ledgers while an acknowledgement gives the shared admission preparer
//! ([`crate::watchlist_admission`]) a strict seed-before-membership ordering.

use std::sync::Arc;

use pe_core_types::PolymarketConditionId;
use pe_core_types::WalletAddress;
use pe_event_log::AppendReceipt;
use pe_risk_engine::RiskHaltCause;
use pe_source_polymarket_public::ActivityAggregate;
use pe_trader_index::WatchlistEntry;
use tokio::sync::oneshot;

use crate::bucket_commit::{BucketCommitResult, BucketDecisionContext};
use crate::paper_recovery::{HaltState, MembershipChange, RiskHaltOwner};
use crate::position_seeder::AnchorInstall;
use crate::watchlist_maintenance::MembershipCommit;

/// Exact single-owner ledger capture used by the causal bracket.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AdmissionLedgerCapture {
    pub wallet: WalletAddress,
    pub hash: String,
    pub cursor: Option<i64>,
    pub anchor_seq: Option<i64>,
    pub coverage_generation: i64,
}

/// A control-plane update consumed ahead of trade events by the orchestrator's biased select.
pub enum OrchestratorControl {
    /// A synchronized frame, delivered in source receipt order.
    ActivityFrameDecision { receipt: AppendReceipt },
    /// Reconciliation acknowledgements are serialized with admissions by the existing owner.
    ReconciliationUpdate {
        update: ReconciliationUpdate,
        acknowledged: oneshot::Sender<Result<ReconciliationAcknowledgement, String>>,
    },
    /// Durable history/fence checks and Lane D's position bracket completed for
    /// these wallets. The orchestrator rechecks its loaded fence set before ack.
    PrepareAdmissions {
        wallets: Vec<WalletAddress>,
        acknowledged: oneshot::Sender<()>,
    },
    /// Install an all-or-nothing set of venue-authoritative position anchors
    /// after rechecking the captured ledger and coverage generation.
    InstallAnchors {
        installs: Vec<AnchorInstall>,
        acknowledged: oneshot::Sender<Result<(), crate::bucket_commit::AnchorInstallError>>,
    },
    /// Capture one exact ledger generation between bracket steps.
    CaptureAdmissionLedger {
        wallet: WalletAddress,
        captured: oneshot::Sender<Result<AdmissionLedgerCapture, String>>,
    },
    /// Complete fixed-end reconciliation bucket. `#544 Lane E integration`:
    /// source routing closes obligations before sending this command.
    CommitActivityBucket {
        aggregates: Vec<ActivityAggregate>,
        context: Arc<BucketDecisionContext>,
        committed: oneshot::Sender<Result<BucketCommitResult, String>>,
    },
    /// CLOB resolution evidence was durably appended by the caller. The orchestrator
    /// serializes its Prepared/authority/local/Final financial transition.
    ResolutionCandidate {
        condition: PolymarketConditionId,
        payout_by_outcome_index_json: String,
        receipt: AppendReceipt,
        acknowledged: oneshot::Sender<Result<(), String>>,
    },
    /// Publish one structural membership transition after its paper record synchronizes.
    PublishMembership {
        change: MembershipChange,
        replacements: Vec<WatchlistEntry>,
        checks: Box<MembershipCommit>,
        acknowledged:
            oneshot::Sender<Result<AppendReceipt, crate::watchlist_maintenance::PublishError>>,
    },
    /// Append one risk-cause edge before acknowledging it to the producer.
    RiskHaltChange {
        owner: RiskHaltOwner,
        cause: RiskHaltCause,
        state: HaltState,
        evidence: serde_json::Value,
        acknowledged: oneshot::Sender<Result<AppendReceipt, String>>,
    },
    /// Daily mark producer handoff. Lane D owns mark construction semantics.
    DailyBoundary {
        cutoff_unix: i64,
        boundary_receipt: AppendReceipt,
        acknowledged: oneshot::Sender<Result<(), String>>,
    },
    /// Qualification seal producer handoff. Lane F owns the verifier semantics.
    SealCheck {
        proposed_economic_hash: String,
        proposed_financial_semantic_version: u32,
        acknowledged: oneshot::Sender<Result<(), String>>,
    },
}

/// Retirement acknowledges only the current durable receipt authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationAcknowledgement {
    Applied,
    Superseded,
}

/// Ordinary observation retirement and fresh frontier publication.
#[derive(Debug, Clone)]
pub enum ReconciliationUpdate {
    /// The owner checks current admission/disposition before acknowledging barrier removal.
    RetireObservation {
        receipt: AppendReceipt,
        source_trade_id: pe_core_types::SourceTradeId,
        unbound: bool,
        verified_read: Option<Arc<crate::bucket_commit::VerifiedCommitment>>,
    },
    Frontier(
        crate::frame_admission::FeedHistoryFrontier,
        Option<Arc<crate::bucket_commit::VerifiedCommitment>>,
    ),
}
