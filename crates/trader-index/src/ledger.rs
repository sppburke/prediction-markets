//! Output types produced by the ledger reconstruction engine.

use pe_core_types::{
    ContractQty, MarketId, OperatorId, OutcomeId, Price, ReconstructionQuality, Side,
    SourceTradeId, WalletAddress,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A fully matched position: one or more entry fills paired with one or more exit fills.
///
/// `realized_pnl_usd = (exit_price − entry_price) × contracts`, where each contract
/// resolves to $1 USD (standard Polymarket convention).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    /// Side on which the position was opened (entry side).
    pub side: Side,
    pub entry_price: Price,
    pub exit_price: Price,
    pub contracts: ContractQty,
    pub hold_duration_seconds: u64,
    pub realized_pnl_usd: Decimal,
    /// Unix timestamp of the entry fill (seconds since epoch, UTC).
    pub opened_at_unix: i64,
    /// Unix timestamp of the closing fill (seconds since epoch, UTC).
    pub closed_at_unix: i64,
    /// Source trade IDs contributing to this closed trade (entry ids first, then exit ids).
    pub source_trade_ids: Vec<SourceTradeId>,
}

/// An entry fill with no matching exit fill yet observed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    /// Side on which the position was opened.
    pub side: Side,
    /// FIFO-weighted average of all unmatched entry fill prices.
    pub avg_entry_price: Price,
    pub contracts: ContractQty,
    pub source_trade_ids: Vec<SourceTradeId>,
}

/// Reconstructed trade history and open positions for a single wallet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraderLedger {
    pub wallet: WalletAddress,
    /// Set when `OperatorIdentity.confidence_ppm >= LedgerConfig.operator_min_confidence_ppm`.
    pub operator_id: Option<OperatorId>,
    /// 0–100: fraction of entry contracts that were matched to exit fills.
    /// Values below `LedgerConfig.research_only_quality_threshold` (default 60)
    /// indicate the ledger is suitable for research only, not watchlist eligibility.
    pub reconstruction_quality: ReconstructionQuality,
    pub closed_trades: Vec<ClosedTrade>,
    pub open_positions: Vec<OpenPosition>,
    pub audit_window_days: u32,
}
