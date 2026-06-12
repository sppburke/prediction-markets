//! Input snapshot types consumed by [`classify_trade`].
//!
//! [`classify_trade`]: crate::classifier::classify_trade

use std::collections::HashMap;

use pe_core_types::{
    ContractQty, MarketId, MarketOutcomeId, OutcomeId, Price, Side, SourceTradeId, WalletAddress,
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
