//! Output type assembled by the classifier.

use pe_core_types::{
    InheritedPriorPpm, LeaderAction, MarketId, OperatorId, OutcomeId, Price, ProbabilityPpm,
    Quantity, ReconstructionQuality, Side, SourceTradeId, TraderId, VenueId,
    WinnerFollowSignalKind,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// A classified, ready-to-gate trade signal from a watchlisted leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderSignal {
    pub leader: TraderId,
    pub operator_id: Option<OperatorId>,
    pub venue: VenueId,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub action: LeaderAction,
    pub leader_side: Side,
    pub leader_price: Price,
    pub leader_size: Quantity,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub received_at: OffsetDateTime,
    pub reconstruction_quality: ReconstructionQuality,
    pub signal_kind: WinnerFollowSignalKind,
    /// Non-`None` for `FreshWalletFirstTrade` signals; carries the shrunk operator prior.
    pub inherited_prior: Option<InheritedPriorPpm>,
    pub source_trade_id: SourceTradeId,
    /// Confidence in the action classification (0..=1_000_000).
    ///
    /// Linear proxy for reconstruction fidelity:
    /// `reconstruction_quality.get() as u32 * 10_000`.
    /// Model-calibrated weights deferred to `03-PHASE-MODEL-ENGINE.md`.
    pub action_confidence_ppm: ProbabilityPpm,
}
