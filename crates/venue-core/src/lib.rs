//! Venue-agnostic order types for the prediction-edge system.
//!
//! Strategy crates emit [`OrderIntent`]; only `execution-core` submits orders
//! to venues. This crate defines the boundary types shared across venue adapters.

use pe_core_types::{ContractQty, MarketId, OutcomeId, Price, Side, StrategyId};
use serde::{Deserialize, Serialize};

/// Intent to place an order, emitted by a strategy crate.
///
/// Only `execution-core` submits orders; strategy crates produce `OrderIntent` values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub strategy_id: StrategyId,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    pub contracts: ContractQty,
    pub limit_price: Price,
    pub validity_seconds: u32,
    pub idempotency_key: String,
}

/// Result of submitting an [`OrderIntent`] to a venue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum OrderOutcome {
    Filled {
        fill_price: Price,
        contracts: ContractQty,
    },
    PartialFill {
        fill_price: Price,
        filled_contracts: ContractQty,
        remaining_contracts: ContractQty,
    },
    Cancelled,
    Expired,
    Rejected {
        reason: String,
    },
}

/// Errors returned by venue adapter operations.
#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    #[error("network error: {message}")]
    Network { message: String },
    #[error("auth error: {message}")]
    Auth { message: String },
    #[error("rate limited: retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u32 },
    #[error("order rejected: {reason}")]
    OrderRejected { reason: String },
    #[error("venue error: {message}")]
    Other { message: String },
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rust_decimal_macros::dec;

    use super::*;
    use pe_core_types::{ContractQty, MarketId, OutcomeId, Price, Side, StrategyId, VenueMarketId};

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            strategy_id: StrategyId("winner-follow".to_string()),
            market_id: MarketId(VenueMarketId("market-abc-123".to_string())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            contracts: ContractQty(10),
            limit_price: Price::new(dec!(0.45)).unwrap(),
            validity_seconds: 60,
            idempotency_key: "winner-follow|trade-1|market-abc-123|0|buy|1700000000".to_string(),
        }
    }

    #[test]
    fn order_outcome_filled_serializes_with_status_tag() {
        let outcome = OrderOutcome::Filled {
            fill_price: Price::new(dec!(0.45)).unwrap(),
            contracts: ContractQty(10),
        };
        let json = serde_json::to_string(&outcome).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["status"], "filled");
    }

    #[test]
    fn venue_error_rate_limited_display() {
        let err = VenueError::RateLimited {
            retry_after_secs: 30,
        };
        assert_eq!(err.to_string(), "rate limited: retry after 30s");
    }

    #[test]
    fn order_intent_round_trips_json() {
        let intent = sample_intent();
        let json = serde_json::to_string(&intent).unwrap();
        let decoded: OrderIntent = serde_json::from_str(&json).unwrap();
        assert_eq!(intent, decoded);
    }
}
