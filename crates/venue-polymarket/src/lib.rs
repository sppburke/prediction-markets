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
pub mod receipt;
pub mod redemption;
pub mod v2;

pub use canary_market::{
    AskLevel, CanaryBookSnapshot, CanaryMarketError, ClobMarketEvidence, CompactMarketEvidence,
    ExecutableLadder, executable_ladder, parse_book, parse_compact_market, parse_market_evidence,
};
pub use fee::{
    CompactFeeSchedule, FeeError, FeeScheduleError, fee_reserve, fee_within_reserve,
    parse_compact_fee_schedule, principal_for_budget, taker_fee,
};
pub use ladder::{
    BuySizing, KellyAllocator, LADDER_MAX_AGE_MS, LadderError, LadderPlan, SizedBuyPlan,
    ladder_is_stale, plan_sized_buy,
};
pub use receipt::{
    CTF_EXCHANGE_V2, DecodedOrderFill, FINALIZED_CHAIN_ID, FinalizedBlock, MatchedReceipt,
    NEG_RISK_CTF_EXCHANGE_V2, ReceiptError, TOPIC_ORDER_FILLED_V2, canonical_block_matches,
    decode_order_fills, parse_chain_id_response, parse_finalized_block_response,
    parse_receipt_response,
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
