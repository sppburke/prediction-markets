//! Polymarket public REST API source connector.
//!
//! Polls the five Polymarket public endpoints (leaderboard, user trades,
//! current positions, closed positions, user activity) on a configurable
//! interval, emitting raw `SourceEvent` payloads for downstream parsing.

pub mod config;
pub mod connector;
pub mod endpoint;
pub mod fetcher;
pub mod gamma_markets;

pub use config::PollingConfig;
pub use connector::PolymarketPublicConnector;
pub use endpoint::{LeaderboardCategory, LeaderboardSort, LeaderboardWindow, PolymarketEndpoint};
pub use fetcher::{FixtureFetcher, PageFetcher, ReqwestFetcher};
pub use gamma_markets::{
    GAMMA_BATCH_SIZE, GAMMA_BROWSER_UA, GammaMarket, GammaMarketsClient, GammaMarketsError,
    MarketFilter,
};
