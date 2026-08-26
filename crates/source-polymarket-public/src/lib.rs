//! Polymarket public REST API source connector.
//!
//! Polls the five Polymarket public endpoints (leaderboard, user trades,
//! current positions, closed positions, user activity) on a configurable
//! interval, emitting raw `SourceEvent` payloads for downstream parsing.

pub mod activity_ws;
pub mod clob_prices_history;
pub mod config;
pub mod connector;
pub mod endpoint;
pub mod fetcher;
pub mod gamma_markets;
pub mod live_admission;

pub use activity_ws::{
    ACTIVITY_WS_BACKOFF_CAP_SECS, ACTIVITY_WS_DEAD_SECS, ACTIVITY_WS_PARSER_VERSION,
    ACTIVITY_WS_SCHEMA_VERSION, ACTIVITY_WS_SILENCE_RESUBSCRIBE_SECS, ACTIVITY_WS_STALE_SECS,
    ACTIVITY_WS_SUBSCRIBE, ACTIVITY_WS_URL, ActivityTradeRaw, ActivityWsError, ActivityWsPolicy,
    ActivityWsStream, PolicyAction, ReconnectBackoff, WsStaleness, backoff_secs,
    parse_activity_frame,
};
pub use clob_prices_history::{
    CLOB_PRICES_HISTORY_FIDELITY_MINUTES, CLOB_PRICES_HISTORY_MIN_INTERVAL_MS, ClassifiedPage,
    ClassifiedPricesHistory, ClobPricesHistoryClient, ClobPricesHistoryError, PricePoint,
};
pub use config::PollingConfig;
pub use connector::PolymarketPublicConnector;
pub use endpoint::{LeaderboardCategory, LeaderboardSort, LeaderboardWindow, PolymarketEndpoint};
pub use fetcher::{FixtureFetcher, HttpRequestContext, PageFetcher, ReqwestFetcher};
pub use gamma_markets::{
    GAMMA_BATCH_SIZE, GAMMA_BROWSER_UA, GammaMarket, GammaMarkets, GammaMarketsClient,
    GammaMarketsError, MarketFilter, parse_outcome_prices,
};
pub use live_admission::{
    LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, LiveFeeEvidence, LiveMarketError,
    LiveMarketEvidence, validate_live_market,
};
pub mod canary;
