//! Control-plane messages applied by the single-owner orchestrator loop.
//!
//! Runtime watchlist growth must initialize a newly followed wallet's first-entry history and
//! current leader positions before that wallet becomes visible in [`crate::live_watchlist`].
//! This channel preserves the orchestrator's single ownership of those mutable ledgers while an
//! acknowledgement gives the capacity controller a strict seed-before-membership ordering.

use std::collections::{HashMap, HashSet};

use pe_copy_signal_engine::PositionSnapshot;
use pe_core_types::{MarketId, WalletAddress};
use tokio::sync::oneshot;

/// A control-plane update consumed ahead of trade events by the orchestrator's biased select.
pub enum OrchestratorControl {
    /// Periodic replacement snapshots from the public positions API.
    PositionReseed(HashMap<WalletAddress, PositionSnapshot>),
    /// History and position seeds for wallets about to be admitted by a runtime capacity change.
    /// The controller waits for `acknowledged` before publishing the new membership generation.
    PrepareAdmissions {
        /// Conservative prior-market sets for every newly admitted wallet.
        history: HashMap<WalletAddress, HashSet<MarketId>>,
        /// Current leader positions for every newly admitted wallet, including empty snapshots.
        positions: HashMap<WalletAddress, PositionSnapshot>,
        /// One-shot proof that both maps have been applied by the orchestrator.
        acknowledged: oneshot::Sender<()>,
    },
}
