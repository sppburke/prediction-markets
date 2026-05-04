//! Polymarket public API endpoint enumeration.

use serde::{Deserialize, Serialize};

/// The five Polymarket public REST API endpoints polled by this source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketEndpoint {
    Leaderboard,
    UserTrades { user: String },
    CurrentPositions { user: String },
    ClosedPositions { user: String },
    UserActivity { user: String },
}

impl PolymarketEndpoint {
    /// Returns a stable string key for use in config maps and metrics.
    pub fn key(&self) -> &'static str {
        match self {
            Self::Leaderboard => "leaderboard",
            Self::UserTrades { .. } => "user_trades",
            Self::CurrentPositions { .. } => "current_positions",
            Self::ClosedPositions { .. } => "closed_positions",
            Self::UserActivity { .. } => "user_activity",
        }
    }

    /// Build the request URL given a base URL (no trailing slash).
    pub fn url(&self, base: &str) -> String {
        match self {
            Self::Leaderboard => format!("{base}/v1/leaderboard"),
            Self::UserTrades { user } => format!("{base}/data/trades?user={user}"),
            Self::CurrentPositions { user } => format!("{base}/positions?user={user}"),
            Self::ClosedPositions { user } => {
                format!("{base}/data/positions?user={user}&sizeThreshold=.01")
            }
            Self::UserActivity { user } => format!("{base}/activity?user={user}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaderboard_key_and_url() {
        let ep = PolymarketEndpoint::Leaderboard;
        assert_eq!(ep.key(), "leaderboard");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard"
        );
    }

    #[test]
    fn user_trades_key_and_url() {
        let ep = PolymarketEndpoint::UserTrades {
            user: "0xabc".into(),
        };
        assert_eq!(ep.key(), "user_trades");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/data/trades?user=0xabc"
        );
    }
}
