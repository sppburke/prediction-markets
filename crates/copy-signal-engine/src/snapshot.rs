//! Input snapshot types consumed by [`classify_trade`].
//!
//! [`classify_trade`]: crate::classifier::classify_trade

use std::collections::HashMap;

use pe_core_types::{
    ContractQty, MarketId, MarketOutcomeId, OperatorId, OutcomeId, Price, Side, SourceTradeId,
    WalletAddress,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// A freshly observed trade from a leader wallet to be classified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomingTrade {
    pub wallet: WalletAddress,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    pub price: Price,
    pub contracts: ContractQty,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub received_at: OffsetDateTime,
    pub source_trade_id: SourceTradeId,
}

/// Net contract exposure on each side for one `(market, outcome)` bucket.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PositionState {
    /// Unmatched Buy (long) contracts.
    pub long_contracts: u64,
    /// Unmatched Sell (short) contracts.
    pub short_contracts: u64,
}

/// Current open positions for a single wallet across all `(market, outcome)` buckets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub wallet: WalletAddress,
    pub positions: HashMap<MarketOutcomeId, PositionState>,
}

/// Minimal wallet profile needed to classify `FreshWalletFirstTrade` signals.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WalletProfile {
    pub wallet: WalletAddress,
    /// Total closed trades observed for this wallet across all time.
    pub closed_trade_count: u32,
    /// Age of the wallet in seconds since first observed activity.
    pub age_seconds: u32,
}

/// A single wallet's entry in a cluster-coordination observation window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterEntry {
    pub wallet: WalletAddress,
    pub price: Price,
    pub contracts: ContractQty,
    /// Unix timestamp (seconds) when this entry was observed.
    pub observed_at_unix: i64,
}

/// Recent entries from all wallets of the same operator into the same `(market, outcome, side)`.
///
/// Caller provides this when the operator is known and ≥1 other wallet has entered
/// the same bucket within `cluster_coord_window_seconds`. The current trade's wallet
/// must be included in `wallet_entries`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterObs {
    pub operator_id: OperatorId,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    /// All entries within (or near) the coordination window, including the current trade.
    pub wallet_entries: Vec<ClusterEntry>,
}
