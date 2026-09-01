//! Control-plane messages applied by the single-owner orchestrator loop.
//!
//! Every runtime watchlist addition must initialize the newly followed wallet's first-entry
//! history and current leader positions before that wallet becomes visible in
//! [`crate::live_watchlist`]. This channel preserves the orchestrator's single ownership of
//! those mutable ledgers while an acknowledgement gives the shared admission preparer
//! ([`crate::watchlist_admission`]) a strict seed-before-membership ordering.

use std::collections::BTreeMap;

use pe_core_types::{ShareAmount, WalletAddress};
use pe_paper_state::PositionValidationRecord;
use pe_source_polymarket_public::ActivityAggregate;
use tokio::sync::oneshot;

use crate::bucket_commit::{BucketCommitResult, BucketDecisionContext};

/// Exact single-owner ledger capture used by the causal bracket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionLedgerCapture {
    pub wallet: WalletAddress,
    pub hash: String,
    pub positive_ordinary_balances: BTreeMap<(String, u16), ShareAmount>,
    pub has_positive_short: bool,
}

/// A control-plane update consumed ahead of trade events by the orchestrator's biased select.
pub enum OrchestratorControl {
    /// Durable history/fence checks and Lane D's position bracket completed for
    /// these wallets. The orchestrator rechecks its loaded fence set before ack.
    PrepareAdmissions {
        wallets: Vec<WalletAddress>,
        acknowledged: oneshot::Sender<()>,
    },
    /// Install an all-or-nothing set of accepted causal brackets after
    /// rechecking each final ledger hash against the single runtime owner.
    PrepareValidatedAdmissions {
        validations: Vec<PositionValidationRecord>,
        acknowledged: oneshot::Sender<Result<(), String>>,
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
        context: BucketDecisionContext,
        committed: oneshot::Sender<Result<BucketCommitResult, String>>,
    },
}
