//! Canonical newtypes for the prediction-edge system.
//!
//! Every price, quantity, probability, identifier, and timestamp used across
//! the workspace is defined here. No `f64` for money, prices, or probabilities.

pub mod error;
pub mod identity;
pub mod ids;
pub mod polymarket;
pub mod price;
pub mod quantity;
pub mod reconstruction;
pub mod side;
pub mod signal;
pub mod time;

pub use error::Error;
pub use identity::{TraderId, VenueAccountId, WalletAddress};
pub use ids::{
    EventSeq, MarketId, MarketOutcomeId, ModelId, OrderLocalId, OutcomeId, ResolverCardId,
    SourceId, SourceTradeId, StrategyId, VenueId, VenueMarketId,
};
pub use polymarket::{
    PolymarketConditionId, PolymarketOrderId, PolymarketOrderType, PolymarketTokenId,
};
pub use price::{
    BasisPoints, KalshiPriceCents, KellyFraction, PolymarketPriceDecimal, Price, PriceDelta,
    Probability, ProbabilityPpm, RoundingPolicy,
};
pub use quantity::{ContractQty, Quantity};
pub use reconstruction::ReconstructionQuality;
pub use side::Side;
pub use signal::LeaderAction;
pub use time::{ObservedAtBucket, ReceivedAt, SourceTimestamp};
