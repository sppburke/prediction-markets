//! Input snapshot types for ledger reconstruction.

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, WalletAddress,
};
use serde::{Deserialize, Serialize};

/// A single observed trade for a wallet from a public data source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTrade {
    pub wallet: WalletAddress,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    /// Price on [0, 1]; each contract resolves to $1 USD.
    pub price: Price,
    pub contracts: ContractQty,
    pub timestamp: SourceTimestamp,
    pub source_trade_id: SourceTradeId,
}

/// Point-in-time collection of raw trades used as input to ledger reconstruction.
///
/// Callers fill this from whatever source (event-log replay, live API poll, test fixtures).
/// The reconstruction algorithm is deterministic: same snapshot always produces the same ledgers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSnapshot {
    /// All observed trades within the audit window, in any order.
    pub trades: Vec<RawTrade>,
    /// Timestamp at which this snapshot was captured.
    pub snapshot_at: SourceTimestamp,
    /// Number of calendar days included in the audit window.
    pub audit_window_days: u32,
}
