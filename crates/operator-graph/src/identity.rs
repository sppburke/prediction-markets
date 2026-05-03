//! Output types for the operator clustering engine.

use std::collections::{HashMap, HashSet};

use pe_core_types::{
    ClusterSize, FunderRootId, FundingHopCount, OperatorId, ReconstructionQuality, WalletAddress,
};
use serde::{Deserialize, Serialize};

/// Anti-gaming flags that can be raised against an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AntiGamingFlag {
    /// Seeding velocity above threshold AND a fresh wallet in the batch has large position.
    BaitWalletSuspect,
    /// Cluster grew by > 30% in 30d AND new members have negative PnL or < 5 trades.
    DilutionAttack,
    /// Funder root is < 7d old, first inbound from CEX/bridge, and fans out to ≥ 5 wallets in 72h.
    LaunderedFunder,
    /// ≥ 60% of intra-cluster trades match counterpart trades from another cluster member within 60s.
    ///
    /// TODO(trader-index): implement when trade-level data is available in trader-index /
    /// copy-signal-engine. Requires per-market trade timestamps — not present in FundingSnapshot.
    WashCluster,
    /// Single MarketFamily is > 60% of operator's audited PnL.
    ///
    /// TODO(trader-index): implement when trade-level data is available in trader-index /
    /// copy-signal-engine. Requires per-market PnL breakdown — not present in FundingSnapshot.
    MarketNarrowness,
}

/// Derived operator identity — output of the clustering engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorIdentity {
    pub operator_id: OperatorId,
    pub funder_root: FunderRootId,
    pub member_wallets: Vec<WalletAddress>,
    /// Hop count from funder root to each member wallet.
    pub hop_counts: HashMap<WalletAddress, FundingHopCount>,
    /// Confidence in this clustering (0..=1_000_000 ppm).
    pub confidence_ppm: u32,
    pub reconstruction_quality: ReconstructionQuality,
    pub cluster_size: ClusterSize,
    pub anti_gaming_flags: HashSet<AntiGamingFlag>,
}
