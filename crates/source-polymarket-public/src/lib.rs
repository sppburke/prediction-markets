//! Polymarket public REST API source connector.
//!
//! Polls the five Polymarket public endpoints (leaderboard, user trades,
//! current positions, closed positions, user activity) on a configurable
//! interval, emitting raw `SourceEvent` payloads for downstream parsing.

pub mod activity;
pub mod activity_ws;
pub mod clob_prices_history;
pub mod clob_resolution;
pub mod config;
pub mod connector;
pub mod endpoint;
pub mod fetcher;
pub mod gamma_markets;
pub mod live_admission;
pub mod reconciliation;

pub use activity::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate, ActivityAggregationError,
    ActivityIdentityError, ActivityParseContext, ActivityParseError, ActivityRevisionComparison,
    ActivitySemanticRevision, ActivityTradeObservation, ActivityTransport, ActivityType,
    ActivityValidationError, ActivityWindowInvalidation, NormalizedActivity,
    NormalizedActivityWindow, PriceWeightedShareAmount, SourceActivityGroupComponents,
    SourceActivityGroupId, aggregate_activity_rows, parse_activity_response, parse_activity_row,
    parse_activity_trade_observation, project_legacy_contract_qty_v1,
};
#[cfg(feature = "scenario")]
pub use activity_ws::ActivityWsPeer;
pub use activity_ws::{
    ACTIVITY_WS_BACKOFF_CAP_SECS, ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS,
    ACTIVITY_WS_PARSER_VERSION, ACTIVITY_WS_READER_COUNT, ACTIVITY_WS_SCHEMA_VERSION,
    ACTIVITY_WS_SUBSCRIBE, ACTIVITY_WS_URL, ActivityWsError, ActivityWsStream, ReconnectBackoff,
    WireFrame, backoff_secs, parse_activity_frame,
};
pub use clob_prices_history::{
    CLOB_PRICES_HISTORY_FIDELITY_MINUTES, CLOB_PRICES_HISTORY_MIN_INTERVAL_MS, ClassifiedPage,
    ClassifiedPricesHistory, ClobPricesHistoryClient, ClobPricesHistoryError, PricePoint,
};
pub use clob_resolution::{
    BinaryPayoutVector, CLOB_END_CURSOR, CLOB_RESOLUTION_PARSER_VERSION,
    CLOB_RESOLUTION_SCHEMA_VERSION, ClobCoverageCounts, ClobCoverageManifest,
    ClobCoverageManifestError, ClobCoveragePage, ClobMarket, ClobMarketsPage, ClobPayoutResolution,
    ClobPayoutUnresolvedReason, ClobResolutionEvidence, ClobResolutionParseError, ClobTerminalKind,
    ClobTerminalProof, ClobToken, ClobTokenPrice, ClobWinnerAnalysis, ClobWinnerVerdict,
    analyze_clob_winners, hash_clob_page_sha256, is_clob_terminal_cursor, parse_clob_end_date,
    parse_clob_market, parse_clob_markets_page,
};
pub use config::PollingConfig;
pub use connector::PolymarketPublicConnector;
pub use endpoint::{
    LeaderboardCategory, LeaderboardSort, LeaderboardWindow, PolymarketEndpoint, PositionPartition,
};
pub use fetcher::{FixtureFetcher, HttpRequestContext, PageFetcher, ReqwestFetcher};
pub use gamma_markets::{
    GAMMA_BATCH_LIMIT_PARAM, GAMMA_BATCH_SIZE, GAMMA_BROWSER_UA, GAMMA_MARKETS_PARSER_VERSION,
    GAMMA_MARKETS_SCHEMA_VERSION, GAMMA_MARKETS_SOURCE_ID, GammaMarket, GammaMarkets,
    GammaMarketsClient, GammaMarketsError, GammaMarketsWithPages, GammaOpenConditionRequest,
    GammaTokenMarketsError, MarketFilter, MetadataIdentityError, MetadataPageEvidence,
    VerifiedTokenIdentity, parse_outcome_prices,
};
pub use live_admission::{
    LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, LiveMarketError, LiveMarketEvidence,
    validate_live_market,
};
pub use reconciliation::{
    ACTIVITY_MAX_OFFSET, ActivityAssetIdentity, ActivityAssetMapping, ActivityReadError,
    ActivityRequestBounds, CanonicalPosition, CompleteActivityRead, CompletePositionsRead,
    POSITION_PROOF_VERSION, POSITIONS_MAX_OFFSET, PositionClassification, PositionReadError,
    RECONCILIATION_PAGE_LIMIT, ReconciliationFetcher, ReconciliationPageEvidence,
    ReconciliationPageFetcher, fetch_complete_activity, fetch_complete_positions,
};
pub mod canary;
