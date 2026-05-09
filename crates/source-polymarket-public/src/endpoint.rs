//! Polymarket public API endpoint enumeration.

use serde::{Deserialize, Serialize};

/// The four Polymarket public REST API endpoints polled by this source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketEndpoint {
    Leaderboard,
    /// Cursor-based trade history for a single wallet via `/activity?type=TRADE`.
    ///
    /// `end` (inclusive): return trades with `timestamp <= end`. `None` = no upper bound.
    /// `start` (exclusive): return trades with `timestamp > start`. `None` = no lower bound.
    /// Cursor boundary semantics empirically verified: `end` inclusive, `start` exclusive.
    UserTradeActivity {
        user: String,
        end: Option<i64>,
        start: Option<i64>,
    },
    CurrentPositions {
        user: String,
    },
    ClosedPositions {
        user: String,
    },
}

impl PolymarketEndpoint {
    /// Returns a stable string key for use in config maps and metrics.
    pub fn key(&self) -> &'static str {
        match self {
            Self::Leaderboard => "leaderboard",
            Self::UserTradeActivity { .. } => "user_trade_activity",
            Self::CurrentPositions { .. } => "current_positions",
            Self::ClosedPositions { .. } => "closed_positions",
        }
    }

    /// Build the request URL given a base URL (no trailing slash).
    pub fn url(&self, base: &str) -> String {
        match self {
            Self::Leaderboard => format!("{base}/v1/leaderboard"),
            Self::UserTradeActivity { user, end, start } => {
                let mut url = format!("{base}/activity?user={user}&type=TRADE&limit=500&offset=0");
                if let Some(e) = end {
                    url.push_str(&format!("&end={e}"));
                }
                if let Some(s) = start {
                    url.push_str(&format!("&start={s}"));
                }
                url
            }
            Self::CurrentPositions { user } => format!("{base}/positions?user={user}"),
            Self::ClosedPositions { user } => {
                format!("{base}/closed-positions?user={user}")
            }
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
    fn user_trade_activity_no_cursor() {
        let ep = PolymarketEndpoint::UserTradeActivity {
            user: "0xabc".into(),
            end: None,
            start: None,
        };
        assert_eq!(ep.key(), "user_trade_activity");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/activity?user=0xabc&type=TRADE&limit=500&offset=0"
        );
    }

    #[test]
    fn user_trade_activity_with_end_cursor() {
        let ep = PolymarketEndpoint::UserTradeActivity {
            user: "0xabc".into(),
            end: Some(1_700_000_000),
            start: None,
        };
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/activity?user=0xabc&type=TRADE&limit=500&offset=0&end=1700000000"
        );
    }

    #[test]
    fn user_trade_activity_with_start_cursor() {
        let ep = PolymarketEndpoint::UserTradeActivity {
            user: "0xabc".into(),
            end: None,
            start: Some(1_700_000_000),
        };
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/activity?user=0xabc&type=TRADE&limit=500&offset=0&start=1700000000"
        );
    }
}
