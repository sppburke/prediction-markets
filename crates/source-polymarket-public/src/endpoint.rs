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
    /// Live open positions for a single wallet via `/positions`.
    ///
    /// `redeemable`: when `Some(false)`, restrict to live (unresolved) positions.
    /// `limit` / `offset`: pagination controls (default 100; `limit=500` works).
    /// `size_threshold`: drop dust positions smaller than this many contracts.
    CurrentPositions {
        user: String,
        limit: Option<u32>,
        offset: Option<u32>,
        redeemable: Option<bool>,
        size_threshold: Option<u32>,
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
            Self::CurrentPositions {
                user,
                limit,
                offset,
                redeemable,
                size_threshold,
            } => {
                let mut url = format!("{base}/positions?user={user}");
                if let Some(r) = redeemable {
                    url.push_str(&format!("&redeemable={r}"));
                }
                if let Some(l) = limit {
                    url.push_str(&format!("&limit={l}"));
                }
                if let Some(o) = offset {
                    url.push_str(&format!("&offset={o}"));
                }
                if let Some(t) = size_threshold {
                    url.push_str(&format!("&sizeThreshold={t}"));
                }
                url
            }
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
    fn current_positions_no_params() {
        let ep = PolymarketEndpoint::CurrentPositions {
            user: "0xabc".into(),
            limit: None,
            offset: None,
            redeemable: None,
            size_threshold: None,
        };
        assert_eq!(ep.key(), "current_positions");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/positions?user=0xabc"
        );
    }

    #[test]
    fn current_positions_all_params() {
        let ep = PolymarketEndpoint::CurrentPositions {
            user: "0xabc".into(),
            limit: Some(500),
            offset: Some(500),
            redeemable: Some(false),
            size_threshold: Some(1),
        };
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/positions?user=0xabc&redeemable=false&limit=500&offset=500&sizeThreshold=1"
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
