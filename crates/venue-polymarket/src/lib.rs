//! Polymarket CLOB venue adapter for the prediction-edge system.
//!
//! Provides strict Polymarket V2 canary market validation and one-shot order submission.
//!
//! # Modules
//!
//! The retired V1 signing, retrying POST, and status-polling path is intentionally absent.

#![forbid(unsafe_code)]

pub mod canary_market;
pub mod v2;

pub use canary_market::{
    AskLevel, CanaryBookSnapshot, CanaryMarketError, ClobMarketEvidence, ExecutableLadder,
    executable_ladder, parse_book, parse_market_evidence,
};
pub use v2::{
    CLOB_V2_HOST, CanaryV2Client, CanaryV2Credentials, CanaryV2Error, PostOnceResult,
    PreparedPolymarketBuy, PreparedSubmission, SDK_ARCHIVE_SHA256, SDK_VERSION, V2BuyRequest,
};
