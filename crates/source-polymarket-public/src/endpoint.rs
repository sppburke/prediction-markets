//! Polymarket public API endpoint enumeration.

use serde::{Deserialize, Serialize};

const POSITION_CHANGING_ACTIVITY_TYPES: &str = "TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION";

/// Sort dimension for the `/v1/leaderboard` endpoint (API `orderBy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaderboardSort {
    Profit,
    Volume,
}

impl LeaderboardSort {
    /// API `orderBy` query value.
    pub fn as_order_by(self) -> &'static str {
        match self {
            Self::Profit => "PNL",
            Self::Volume => "VOL",
        }
    }
}

/// Time window for the `/v1/leaderboard` endpoint (API `timePeriod`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaderboardWindow {
    Day,
    Week,
    Monthly,
    AllTime,
}

impl LeaderboardWindow {
    /// API `timePeriod` query value.
    pub fn as_time_period(self) -> &'static str {
        match self {
            Self::Day => "DAY",
            Self::Week => "WEEK",
            Self::Monthly => "MONTH",
            Self::AllTime => "ALL",
        }
    }
}

/// Category filter for the `/v1/leaderboard` endpoint (API `category`).
///
/// All ten values return HTTP 200 live (verified 2026-06-14). A bad value yields
/// HTTP 400 with no fallback, so callers iterating categories must skip + `warn!` on 4xx.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LeaderboardCategory {
    Overall,
    Politics,
    Sports,
    Crypto,
    Culture,
    Mentions,
    Weather,
    Economics,
    Tech,
    Finance,
}

impl LeaderboardCategory {
    /// All ten categories in API order — the default sweep set.
    pub const ALL: [LeaderboardCategory; 10] = [
        Self::Overall,
        Self::Politics,
        Self::Sports,
        Self::Crypto,
        Self::Culture,
        Self::Mentions,
        Self::Weather,
        Self::Economics,
        Self::Tech,
        Self::Finance,
    ];

    /// API `category` query value.
    pub fn as_param(self) -> &'static str {
        match self {
            Self::Overall => "OVERALL",
            Self::Politics => "POLITICS",
            Self::Sports => "SPORTS",
            Self::Crypto => "CRYPTO",
            Self::Culture => "CULTURE",
            Self::Mentions => "MENTIONS",
            Self::Weather => "WEATHER",
            Self::Economics => "ECONOMICS",
            Self::Tech => "TECH",
            Self::Finance => "FINANCE",
        }
    }
}

/// The Polymarket public REST API endpoints polled by this source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketEndpoint {
    /// Leaderboard endpoint — top traders by `sort` within `window` for `category`.
    ///
    /// `limit`: entries per request. The API hard-caps this at 50; larger values are
    /// silently truncated server-side (verified live 2026-06-14).
    Leaderboard {
        sort: LeaderboardSort,
        window: LeaderboardWindow,
        category: LeaderboardCategory,
        limit: u32,
    },
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
    /// Strict offset page used by the live canary to prove one bounded activity window complete.
    UserTradeActivityPage {
        user: String,
        end: i64,
        start: Option<i64>,
        offset: u32,
    },
    /// Fixed-end position-changing activity response, starting at offset zero (#544).
    UserPositionActivity {
        user: String,
        end: i64,
        start: Option<i64>,
    },
    /// Strict offset page over all five position-changing activity types (#544).
    UserPositionActivityPage {
        user: String,
        end: i64,
        start: Option<i64>,
        offset: u32,
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
            Self::Leaderboard { .. } => "leaderboard",
            Self::UserTradeActivity { .. } | Self::UserTradeActivityPage { .. } => {
                "user_trade_activity"
            }
            Self::UserPositionActivity { .. } | Self::UserPositionActivityPage { .. } => {
                "user_position_activity"
            }
            Self::CurrentPositions { .. } => "current_positions",
            Self::ClosedPositions { .. } => "closed_positions",
        }
    }

    /// Build the request URL given a base URL (no trailing slash).
    pub fn url(&self, base: &str) -> String {
        match self {
            Self::Leaderboard {
                sort,
                window,
                category,
                limit,
            } => {
                format!(
                    "{base}/v1/leaderboard?orderBy={}&timePeriod={}&category={}&limit={limit}",
                    sort.as_order_by(),
                    window.as_time_period(),
                    category.as_param(),
                )
            }
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
            Self::UserTradeActivityPage {
                user,
                end,
                start,
                offset,
            } => {
                let mut url = format!(
                    "{base}/activity?user={user}&type=TRADE&limit=500&offset={offset}&sortDirection=DESC&end={end}"
                );
                if let Some(start) = start {
                    url.push_str(&format!("&start={start}"));
                }
                url
            }
            Self::UserPositionActivity { user, end, start } => {
                let mut url = format!(
                    "{base}/activity?user={user}&type={POSITION_CHANGING_ACTIVITY_TYPES}&limit=500&offset=0&sortDirection=DESC&end={end}"
                );
                if let Some(start) = start {
                    url.push_str(&format!("&start={start}"));
                }
                url
            }
            Self::UserPositionActivityPage {
                user,
                end,
                start,
                offset,
            } => {
                let mut url = format!(
                    "{base}/activity?user={user}&type={POSITION_CHANGING_ACTIVITY_TYPES}&limit=500&offset={offset}&sortDirection=DESC&end={end}"
                );
                if let Some(start) = start {
                    url.push_str(&format!("&start={start}"));
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
    fn leaderboard_profit_monthly() {
        let ep = PolymarketEndpoint::Leaderboard {
            sort: LeaderboardSort::Profit,
            window: LeaderboardWindow::Monthly,
            category: LeaderboardCategory::Overall,
            limit: 50,
        };
        assert_eq!(ep.key(), "leaderboard");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard?orderBy=PNL&timePeriod=MONTH&category=OVERALL&limit=50"
        );
    }

    #[test]
    fn leaderboard_volume_alltime_crypto() {
        let ep = PolymarketEndpoint::Leaderboard {
            sort: LeaderboardSort::Volume,
            window: LeaderboardWindow::AllTime,
            category: LeaderboardCategory::Crypto,
            limit: 50,
        };
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard?orderBy=VOL&timePeriod=ALL&category=CRYPTO&limit=50"
        );
    }

    #[test]
    fn leaderboard_param_mappings() {
        assert_eq!(LeaderboardSort::Profit.as_order_by(), "PNL");
        assert_eq!(LeaderboardSort::Volume.as_order_by(), "VOL");
        assert_eq!(LeaderboardWindow::Day.as_time_period(), "DAY");
        assert_eq!(LeaderboardWindow::Week.as_time_period(), "WEEK");
        assert_eq!(LeaderboardWindow::Monthly.as_time_period(), "MONTH");
        assert_eq!(LeaderboardWindow::AllTime.as_time_period(), "ALL");
        assert_eq!(LeaderboardCategory::ALL.len(), 10);
        assert_eq!(LeaderboardCategory::Overall.as_param(), "OVERALL");
        assert_eq!(LeaderboardCategory::Finance.as_param(), "FINANCE");
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

    #[test]
    fn user_position_activity_has_fixed_end_and_all_five_types() {
        let ep = PolymarketEndpoint::UserPositionActivity {
            user: "0xabc".into(),
            end: 1_700_000_100,
            start: Some(1_700_000_000),
        };
        assert_eq!(ep.key(), "user_position_activity");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/activity?user=0xabc&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=0&sortDirection=DESC&end=1700000100&start=1700000000"
        );
    }

    #[test]
    fn user_position_activity_page_keeps_offset_and_optional_start() {
        let ep = PolymarketEndpoint::UserPositionActivityPage {
            user: "0xabc".into(),
            end: 1_700_000_100,
            start: None,
            offset: 500,
        };
        assert_eq!(ep.key(), "user_position_activity");
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/activity?user=0xabc&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&limit=500&offset=500&sortDirection=DESC&end=1700000100"
        );
    }
}
