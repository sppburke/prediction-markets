//! Canonical newtypes for the prediction-edge system.
//!
//! Every price, quantity, probability, identifier, and timestamp used across
//! the workspace is defined here. No `f64` for money, prices, or probabilities.

pub mod error;
pub mod identity;
pub mod ids;
pub mod operator;
pub mod polymarket;
pub mod price;
pub mod quantity;
pub mod side;
pub mod signal;
pub mod time;

pub use error::Error;
pub use identity::{FunderRootId, OperatorId, TraderId, VenueAccountId, WalletAddress};
pub use ids::{
    EventSeq, MarketId, MarketOutcomeId, ModelId, OrderLocalId, OutcomeId, ResolverCardId,
    SourceId, SourceTradeId, StrategyId, VenueId, VenueMarketId,
};
pub use operator::{ClusterSize, FundingHopCount, ReconstructionQuality, WalletAgeSeconds};
pub use polymarket::{
    PolymarketConditionId, PolymarketOrderId, PolymarketOrderType, PolymarketTokenId,
};
pub use price::{
    BasisPoints, InheritedPriorPpm, KalshiPriceCents, KellyFraction, PolymarketPriceDecimal, Price,
    PriceDelta, Probability, ProbabilityPpm, RoundingPolicy,
};
pub use quantity::{ContractQty, Quantity};
pub use side::Side;
pub use signal::{LeaderAction, WinnerFollowSignalKind};
pub use time::{ObservedAtBucket, ReceivedAt, SourceTimestamp};
