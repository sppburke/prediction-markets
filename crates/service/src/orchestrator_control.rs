//! Control-plane messages applied by the single-owner orchestrator loop.
//!
//! Every runtime watchlist addition must initialize the newly followed wallet's first-entry
//! history and current leader positions before that wallet becomes visible in
//! [`crate::live_watchlist`]. This channel preserves the orchestrator's single ownership of
//! those mutable ledgers while an acknowledgement gives the shared admission preparer
//! ([`crate::watchlist_admission`]) a strict seed-before-membership ordering.

use std::collections::{HashMap, HashSet};

use pe_copy_signal_engine::PositionSnapshot;
use pe_core_types::{MarketId, WalletAddress};
use tokio::sync::oneshot;

/// A control-plane update consumed ahead of trade events by the orchestrator's biased select.
pub enum OrchestratorControl {
    /// Periodic replacement snapshots from the public positions API.
    PositionReseed(HashMap<WalletAddress, PositionSnapshot>),
    /// History and position seeds for wallets about to be admitted by a capacity change, a
    /// full-rerank swap, or a knockout backfill. The preparer waits for `acknowledged` before
    /// the caller publishes the new membership generation.
    PrepareAdmissions {
        /// Conservative prior-market sets for every newly admitted wallet.
        history: HashMap<WalletAddress, HashSet<MarketId>>,
        /// Current leader positions for every newly admitted wallet, including empty snapshots.
        positions: HashMap<WalletAddress, PositionSnapshot>,
        /// One-shot proof that both maps have been applied by the orchestrator.
        acknowledged: oneshot::Sender<()>,
    },
}
