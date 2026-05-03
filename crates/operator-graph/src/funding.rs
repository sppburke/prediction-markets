//! Input types for the operator clustering engine.
//!
//! [`FundingSnapshot`] carries all on-chain funding evidence that
//! [`crate::build_operator_identities`] needs to derive operator identities.

use std::collections::HashMap;

use pe_core_types::{SourceTimestamp, WalletAddress};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A single funding event: `funder` transferred to `funded`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingEdge {
    pub funder: WalletAddress,
    pub funded: WalletAddress,
    /// Amount in USD (informational — used for flag thresholds).
    pub amount_usd: Decimal,
    pub timestamp: SourceTimestamp,
}

/// Known category of external address (CEX, bridge, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressCategory {
    CexDeposit,
    Bridge,
    Onramp,
    Unknown,
}

/// A snapshot of all on-chain funding evidence passed to `build_operator_identities`.
#[derive(Debug, Clone)]
pub struct FundingSnapshot {
    pub edges: Vec<FundingEdge>,
    /// Age of each wallet in seconds at the time of the snapshot.
    pub wallet_ages: HashMap<WalletAddress, u32>,
    /// Categorised external addresses (CEX/bridge/onramp).
    pub known_external: HashMap<WalletAddress, AddressCategory>,
    /// Number of closed trades per wallet (for fresh-wallet detection).
    pub closed_trade_counts: HashMap<WalletAddress, u32>,
    /// Realised PnL per wallet (USD), for DilutionAttack detection.
    pub realized_pnl_usd: HashMap<WalletAddress, Decimal>,
    /// Snapshot timestamp (used to compute seeding velocity windows).
    pub snapshot_at: SourceTimestamp,
}
