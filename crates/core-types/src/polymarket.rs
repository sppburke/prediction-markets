//! Polymarket-specific identifier and order types.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Polymarket condition ID — uniquely identifies a binary market's condition on-chain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolymarketConditionId(pub String);

impl fmt::Display for PolymarketConditionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Polymarket ERC-1155 token ID — identifies a specific outcome's collateral token.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolymarketTokenId(pub String);

impl fmt::Display for PolymarketTokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Polymarket CLOB order ID — assigned by the CLOB after order submission.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolymarketOrderId(pub String);

impl fmt::Display for PolymarketOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Polymarket CLOB order types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolymarketOrderType {
    /// Good-till-cancelled: rests on the book until filled or cancelled.
    Gtc,
    /// Good-till-date: expires at a specific timestamp.
    Gtd,
    /// Fill-or-kill: must fill entirely immediately or is rejected.
    Fok,
    /// Fill-and-kill: fills whatever is available immediately, cancels the rest.
    Fak,
}

impl fmt::Display for PolymarketOrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gtc => f.write_str("GTC"),
            Self::Gtd => f.write_str("GTD"),
            Self::Fok => f.write_str("FOK"),
            Self::Fak => f.write_str("FAK"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn condition_id_round_trips() {
        let id = PolymarketConditionId("0xabc123".into());
        let json = serde_json::to_string(&id).unwrap();
        let back: PolymarketConditionId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn token_id_round_trips() {
        let id = PolymarketTokenId(
            "71321045679252212594626385532706912750332728571942532289631379312455583992563".into(),
        );
        let json = serde_json::to_string(&id).unwrap();
        let back: PolymarketTokenId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn order_id_round_trips() {
        let id = PolymarketOrderId("0xdeadbeef".into());
        let json = serde_json::to_string(&id).unwrap();
        let back: PolymarketOrderId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn order_type_serializes_screaming_snake() {
        let json = serde_json::to_string(&PolymarketOrderType::Gtc).unwrap();
        assert_eq!(json, r#""GTC""#);
        let json = serde_json::to_string(&PolymarketOrderType::Fak).unwrap();
        assert_eq!(json, r#""FAK""#);
    }

    #[test]
    fn order_type_round_trips() {
        for ot in [
            PolymarketOrderType::Gtc,
            PolymarketOrderType::Gtd,
            PolymarketOrderType::Fok,
            PolymarketOrderType::Fak,
        ] {
            let json = serde_json::to_string(&ot).unwrap();
            let back: PolymarketOrderType = serde_json::from_str(&json).unwrap();
            assert_eq!(ot, back);
        }
    }
}
