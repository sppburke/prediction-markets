//! Output types for the walk-forward ranker.

use pe_core_types::{
    BasisPoints, OperatorId, ReconstructionQuality, SourceTimestamp, WalletAddress,
};
use serde::{Deserialize, Serialize};

/// Tier assignment for a [`WatchlistEntry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchlistTier {
    /// Passed active-tier eligibility gates; eligible for live copy.
    Active,
    /// Passed incubator-tier gates; under observation, not yet copy-eligible.
    Incubator,
}

/// A single ranked entry in the watchlist.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchlistEntry {
    /// Representative wallet address (lowest-sorted wallet for operator groups).
    pub wallet: WalletAddress,
    /// Set when the wallet belongs to a confirmed operator cluster.
    pub operator_id: Option<OperatorId>,
    pub tier: WatchlistTier,
    /// Primary ranking signal: LCB_5pct + bonus/penalty terms (in basis points).
    pub leader_score_bps: BasisPoints,
    /// Raw LCB at the 5th percentile of the daily-return distribution (basis points).
    pub lcb_5pct_bps: BasisPoints,
    /// Number of closed trades observed within the eligibility window.
    pub closed_trades_in_window: u32,
    /// Minimum reconstruction quality across all ledgers contributing to this entry.
    pub reconstruction_quality: ReconstructionQuality,
}

/// Snapshot of the current ranked watchlist.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watchlist {
    /// Entries sorted descending by `leader_score_bps`.
    /// Active entries appear before incubator entries within each score band.
    pub entries: Vec<WatchlistEntry>,
    /// Timestamp at which the source snapshot was taken.
    pub snapshot_at: SourceTimestamp,
    /// Count of entries with `tier == Active`.
    pub active_count: usize,
    /// Count of entries with `tier == Incubator`.
    pub incubator_count: usize,
}
