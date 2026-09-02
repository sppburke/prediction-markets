//! Control-plane messages applied by the single-owner orchestrator loop.
//!
//! Every runtime watchlist addition must initialize the newly followed wallet's first-entry
//! history and current leader positions before that wallet becomes visible in
//! [`crate::live_watchlist`]. This channel preserves the orchestrator's single ownership of
//! those mutable ledgers while an acknowledgement gives the shared admission preparer
//! ([`crate::watchlist_admission`]) a strict seed-before-membership ordering.

use pe_core_types::WalletAddress;
use pe_source_polymarket_public::ActivityAggregate;
use tokio::sync::oneshot;

use crate::bucket_commit::{BucketCommitResult, BucketDecisionContext};
use crate::position_seeder::AnchorInstall;

/// Exact single-owner ledger capture used by the causal bracket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionLedgerCapture {
    pub wallet: WalletAddress,
    pub hash: String,
    pub cursor: Option<i64>,
    pub anchor_seq: Option<i64>,
    pub coverage_generation: i64,
}

/// A control-plane update consumed ahead of trade events by the orchestrator's biased select.
pub enum OrchestratorControl {
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
        context: Box<BucketDecisionContext>,
        committed: oneshot::Sender<Result<BucketCommitResult, String>>,
    },
}
