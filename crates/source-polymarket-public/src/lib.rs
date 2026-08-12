//! Polymarket public REST API source connector.
//!
//! Polls the five Polymarket public endpoints (leaderboard, user trades,
//! current positions, closed positions, user activity) on a configurable
//! interval, emitting raw `SourceEvent` payloads for downstream parsing.

pub mod clob_prices_history;
pub mod config;
pub mod connector;
pub mod endpoint;
pub mod fetcher;
pub mod gamma_markets;
pub mod live_admission;

pub use clob_prices_history::{
    CLOB_PRICES_HISTORY_FIDELITY_MINUTES, CLOB_PRICES_HISTORY_MIN_INTERVAL_MS,
    ClobPricesHistoryClient, ClobPricesHistoryError, PricePoint,
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
