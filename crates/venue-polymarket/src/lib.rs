//! Polymarket CLOB venue adapter for the prediction-edge system.
//!
//! Provides strict Polymarket V2 canary market validation and one-shot order submission.
//!
//! # Modules
//!
//! The retired V1 signing, retrying POST, and status-polling path is intentionally absent.

#![forbid(unsafe_code)]

pub mod canary_market;
pub mod fee;
pub mod ladder;
pub mod redemption;
pub mod v2;

pub use canary_market::{
    AskLevel, CanaryBookSnapshot, CanaryMarketError, ClobMarketEvidence, ExecutableLadder,
    executable_ladder, parse_book, parse_market_evidence,
};
pub use fee::{
    CompactFeeSchedule, FeeError, FeeScheduleError, compact_fee_schedule, fee_reserve,
    fee_within_reserve, parse_compact_fee_schedule, principal_for_budget, taker_fee,
};
pub use ladder::{
    LADDER_MAX_AGE_MS, LadderError, LadderPlan, ladder_is_stale, plan_budget_buy, plan_exact_shares,
};
pub use redemption::{
    ApprovalCheckRequest, ApprovalEvidence, ApprovalReadError, ApprovalReader,
    BuilderApiKeyCredentials, CONDITIONAL_TOKENS, ConfirmedRedemption, CustodyKind,
    DEPOSIT_WALLET_RELAY_TARGET, NEGRISK_COLLATERAL_ADAPTER, REDEEM_POSITIONS_SELECTOR,
    REDEEM_POSITIONS_SIGNATURE, REDEMPTION_ADAPTER_VERSION, REDEMPTION_PARSER_VERSION,
    REDEMPTION_SCHEMA_VERSION, RELAYER_BASE_URL, RELAYER_DEPOSIT_WALLET_NONCE_PATH,
    RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX, RELAYER_LEGACY_NONCE_PATH,
    RELAYER_LEGACY_TRANSACTION_PATH, RELAYER_SUBMIT_PATH, RedemptionCall, RedemptionError,
    RedemptionSigningError, RedemptionTransport, RedemptionTransportError,
    RelayerApiKeyCredentials, RelayerCredentials, RelayerHttpRequest, RelayerNonce,
    RelayerPollPolicy, RelayerSignatureParams, RelayerState, RelayerTransportClient,
    STANDARD_COLLATERAL_ADAPTER, SignedRedemptionRequest, UnsupportedCustody,
    UnverifiedApprovalReader, build_redemption_call, redemption_adapter,
    redemption_approval_requests, require_verified_approval, sign_deposit_wallet_redemption,
};
pub use v2::{
    CLOB_V2_HOST, CanaryV2Client, CanaryV2Credentials, CanaryV2Error, PostOnceResult,
    PreparedPolymarketBuy, PreparedSubmission, SDK_ARCHIVE_SHA256, SDK_VERSION, V2BuyRequest,
};
