//! Signal classification enums shared across strategy and execution crates.

use serde::{Deserialize, Serialize};

/// Observed action of a leader trader in a single `(market, outcome)` bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LeaderAction {
    /// Opening a new position (no prior exposure on this side).
    Entry,
    /// Adding to an existing same-side position.
    Add,
    /// Partially closing an opposite-side position; more than `near_close_remaining_pct`% remains.
    Trim,
    /// Closing or nearly-closing an opposite-side position; ≤ `near_close_remaining_pct`% remains.
    Exit,
    /// Closing one side and immediately opening the reverse.
    Flip,
    /// Action could not be reliably classified; always blocked from copy.
    Unknown,
}

/// Mechanism by which a leader signal was generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WinnerFollowSignalKind {
    /// Wallet or operator qualifies under active-leader ranking.
    NormalLeaderFollow,
    /// Fresh wallet (≤ `fresh_wallet_max_closed_trades`, age ≤ `fresh_wallet_max_age_seconds`)
    /// linked to a known operator; inherits a shrunk prior from that operator.
    FreshWalletFirstTrade,
    /// ≥ `cluster_coord_min_members` wallets of the same operator entered the same
    /// `(market, outcome, side)` within `cluster_coord_window_seconds` with aggregate
    /// notional ≥ `cluster_coord_min_aggregate_usd`.
    ClusterCoordination,
}
