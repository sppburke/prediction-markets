//! Account-tagged append-only journal for ordinary live execution.
//!
//! One service-owned instance serializes every account into one hash-chained stream. Per-account
//! ledgers are projections of that stream, preserving the global sequence assigned at append time.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::Path;
use std::sync::Mutex;

use pe_core_types::{
    AccountId, CollateralAmount, EventSeq, PolymarketConditionId, PolymarketTokenId, Price,
    RawHttpAttempt, ReceivedAt, ShareAmount, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{
    AppendReceipt, ContentType, EnvelopeIn, LogTailBinding, PoisonReason, Reader, Writer,
};
use pe_resolver_card::VenueSettlementRecord;
use pe_source_polymarket_public::LiveMarketEvidence;
use pe_venue_polymarket::{
    CompactFeeSchedule, DecodedOrderFill, LadderPlan, MatchedReceipt, PreparedPolymarketBuy,
    ReceiptError, canonical_block_matches, decode_order_fills, fee_within_reserve,
    parse_chain_id_response, parse_finalized_block_response, parse_receipt_response,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

const LIVE_JOURNAL_SOURCE: &str = "ordinary-live-execution";
const LIVE_JOURNAL_SCHEMA_VERSION: u32 = 2;
const LIVE_JOURNAL_PARSER_VERSION: u32 = 1;

/// A frozen credential identity. It deliberately cannot carry decrypted credential material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialBindingIdentity {
    pub version: i64,
    pub key_id: String,
}

/// Canonical, secret-free identity of one account-scoped HTTP request.
///
/// The ordered query is retained exactly as sent. `partition` names the logical page family
/// (for example `redeemable=false`), while `offset` is kept separately so an empty page still
/// proves which slice of which custody account was requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedHttpRequestDescriptor {
    pub account_id: String,
    pub credential_fingerprint: String,
    pub custody_wallet: String,
    pub method: String,
    pub path: String,
    pub partition: String,
    pub offset: u64,
    pub ordered_query: Vec<(String, String)>,
}

/// Nonsecret identity of the credentialed account read that produced a financial fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveAccountBindingAudit {
    pub account_id: AccountId,
    pub credential: CredentialBindingIdentity,
    pub custody_wallet: String,
    pub credential_fingerprint: String,
}

impl LiveAccountBindingAudit {
    #[must_use]
    pub fn new(
        account_id: AccountId,
        credential: CredentialBindingIdentity,
        custody_wallet: WalletAddress,
        credential_fingerprint: String,
    ) -> Self {
        let custody_wallet = custody_wallet.to_string();
        Self {
            account_id,
            credential,
            custody_wallet,
            credential_fingerprint,
        }
    }

    #[must_use]
    pub fn is_valid_for(&self, account_id: &AccountId) -> bool {
        &self.account_id == account_id
            && WalletAddress::from_hex(&self.custody_wallet)
                .is_ok_and(|wallet| wallet.to_string() == self.custody_wallet)
            && self.credential_fingerprint.len() == blake3::OUT_LEN * 2
            && self
                .credential_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    #[must_use]
    pub fn request_descriptor(
        &self,
        method: impl Into<String>,
        path: impl Into<String>,
        partition: impl Into<String>,
        offset: u64,
        ordered_query: Vec<(String, String)>,
    ) -> SanitizedHttpRequestDescriptor {
        SanitizedHttpRequestDescriptor {
            account_id: self.account_id.as_str().to_owned(),
            credential_fingerprint: self.credential_fingerprint.clone(),
            custody_wallet: self.custody_wallet.clone(),
            method: method.into(),
            path: path.into(),
            partition: partition.into(),
            offset,
            ordered_query,
        }
    }
}

/// Immutable metadata needed to rebuild a matched fill projection from the journal alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFillProjectionIdentity {
    pub leader_wallet: String,
    pub source_trade_id: Option<String>,
    pub market_id: String,
    pub outcome_id: i64,
    pub side: String,
}

/// Replay identity shared by admission, preparation, and all order transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveOrderIdentity {
    pub dispatch_id: String,
    pub idempotency_key: String,
    pub quote_id: String,
    pub config_hash: String,
    pub decision_hash: String,
    pub evidence_hashes: Vec<String>,
    #[serde(default)]
    pub fill_projection: Option<Box<LiveFillProjectionIdentity>>,
    pub schema_version: u16,
    pub parser_version: u16,
}

/// Requested/effective account mode recorded without importing service-owned control types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveControlMode {
    Off,
    LiveTiny,
}

/// Serializable, replay-complete projection of the source-owned market evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveMarketEvidenceAudit {
    pub condition_id: PolymarketConditionId,
    pub ordered_outcome_token_ids: [PolymarketTokenId; 2],
    pub neg_risk: bool,
    pub minimum_tick_size: Price,
    pub minimum_order_size: ShareAmount,
    pub observed_at_unix: i64,
    pub schema_version: u32,
    pub parser_version: u32,
    pub freshness_window_secs: u64,
}

impl From<&LiveMarketEvidence> for LiveMarketEvidenceAudit {
    fn from(value: &LiveMarketEvidence) -> Self {
        Self {
            condition_id: value.condition_id.clone(),
            ordered_outcome_token_ids: value.ordered_outcome_token_ids.clone(),
            neg_risk: value.neg_risk,
            minimum_tick_size: value.minimum_tick_size,
            minimum_order_size: value.minimum_order_size,
            observed_at_unix: value.observed_at_unix,
            schema_version: value.schema_version,
            parser_version: value.parser_version,
            freshness_window_secs: value.freshness_window_secs,
        }
    }
}

/// Durable receipts for the three source responses composing admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionReceipts {
    pub gamma: pe_event_log::AppendReceipt,
    pub clob_long: pe_event_log::AppendReceipt,
    pub clob_compact: pe_event_log::AppendReceipt,
}

/// Both halves of ordinary live admission and their synchronized source identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveAdmissionArtifactAudit {
    pub market: LiveMarketEvidenceAudit,
    pub settlement: VenueSettlementRecord,
    pub fee_schedule: CompactFeeSchedule,
    pub scheduled_end_unix: Option<i64>,
    pub receipts: AdmissionReceipts,
}

impl LiveAdmissionArtifactAudit {
    pub fn new(
        market: &LiveMarketEvidence,
        settlement: &VenueSettlementRecord,
        fee_schedule: CompactFeeSchedule,
        receipts: AdmissionReceipts,
    ) -> Self {
        Self {
            market: LiveMarketEvidenceAudit::from(market),
            settlement: settlement.clone(),
            fee_schedule,
            scheduled_end_unix: market.scheduled_end_unix,
            receipts,
        }
    }
}

/// Serializable projection of every used ask and exact financial field in a ladder plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LadderPlanAudit {
    pub used_asks: Vec<LadderAskAudit>,
    pub best_ask: Price,
    pub limit_price: Price,
    pub minimum_shares: ShareAmount,
    pub principal: CollateralAmount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LadderAskAudit {
    pub price: Price,
    pub shares: ShareAmount,
}

impl LadderPlanAudit {
    #[must_use]
    pub fn new(plan: &LadderPlan) -> Self {
        let used_asks = plan
            .used_asks
            .iter()
            .map(|ask| LadderAskAudit {
                price: ask.price,
                shares: ask.shares,
            })
            .collect::<Vec<_>>();
        Self {
            used_asks,
            best_ask: plan.best_ask,
            limit_price: plan.limit_price,
            minimum_shares: plan.shares,
            principal: plan.worst_case_debit,
        }
    }

    pub fn expected_shares(&self) -> Result<ShareAmount, pe_core_types::Error> {
        self.used_asks
            .iter()
            .try_fold(ShareAmount::ZERO, |total, ask| {
                total.checked_add(ask.shares)
            })
    }

    pub fn expected_spend(&self) -> Result<CollateralAmount, pe_core_types::Error> {
        let spend = self
            .used_asks
            .iter()
            .try_fold(rust_decimal::Decimal::ZERO, |total, ask| {
                ask.shares
                    .to_decimal()
                    .checked_mul(ask.price.0)
                    .and_then(|value| total.checked_add(value))
                    .ok_or(pe_core_types::Error::OutOfRange {
                        field: "LadderPlanAudit.expected_spend",
                    })
            })?;
        CollateralAmount::from_decimal_exact(
            spend.round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToNegativeInfinity),
        )
    }

    #[must_use]
    pub fn expected_vwap(&self) -> Option<Price> {
        let shares = self.expected_shares().ok()?.to_decimal();
        if shares <= rust_decimal::Decimal::ZERO {
            return None;
        }
        Price::new(
            self.expected_spend()
                .ok()?
                .to_decimal()
                .checked_div(shares)?,
        )
        .ok()
    }
}

/// Typed account-read failure; free-form client errors cannot enter the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveAccountReadFailure {
    Transport,
    Authentication,
    Protocol,
    Stale,
}

/// Full sanitized account observation used by admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveAccountStateAudit {
    pub observed_at: OffsetDateTime,
    pub closed_only: bool,
    pub geoblocked: bool,
    pub selected_spender: String,
    pub collateral_balance: CollateralAmount,
    pub allowance: CollateralAmount,
    pub reconciled_free_collateral: CollateralAmount,
    pub schema_version: u16,
    pub parser_version: u16,
    pub evidence: Vec<RawHttpAttempt>,
    /// Descriptor hashes in response order; transport failures have no response descriptor entry.
    pub request_descriptor_hashes: Vec<String>,
    pub evidence_hashes: Vec<String>,
}

/// Every ordinary live admission refusal is durable and machine-readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "reason", content = "detail")]
pub enum LiveAdmissionRefusal {
    #[error("account live mode is not armed")]
    ModeNotArmed,
    #[error("frozen credential binding no longer matches")]
    CredentialVersionChanged,
    #[error("market evidence is stale")]
    MarketEvidenceStale,
    #[error("market evidence is invalid")]
    MarketEvidenceInvalid,
    #[error("venue-settlement evidence is stale or invalid")]
    SettlementEvidenceStale,
    #[error("venue reports the market already resolved")]
    MarketAlreadyResolved,
    #[error("venue settlement is ambiguous")]
    SettlementAmbiguous,
    #[error("the two admission halves, quote, or ladder disagree")]
    ArtifactIdentityMismatch,
    #[error("the frozen ladder plan is invalid")]
    LadderInvalid,
    #[error("authenticated account state is unavailable: {0:?}")]
    AccountStateUnavailable(LiveAccountReadFailure),
    #[error("authenticated account is closed-only")]
    AccountClosedOnly,
    #[error("same-egress venue check is geoblocked")]
    Geoblocked,
    #[error("collateral balance is below worst-case debit")]
    InsufficientBalance {
        required: CollateralAmount,
        available: CollateralAmount,
    },
    #[error("selected-spender allowance is below worst-case debit")]
    InsufficientAllowance {
        required: CollateralAmount,
        available: CollateralAmount,
    },
    #[error("reconciled free collateral is below worst-case debit")]
    WorstCaseDebitExceedsFreeCollateral {
        required: CollateralAmount,
        available: CollateralAmount,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "verdict", content = "detail")]
pub enum LiveAdmissionVerdict {
    Approved,
    Refused(LiveAdmissionRefusal),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveAdmissionEvaluationAudit {
    pub identity: LiveOrderIdentity,
    pub frozen_binding: CredentialBindingIdentity,
    pub current_binding: CredentialBindingIdentity,
    pub requested_mode: LiveControlMode,
    pub effective_mode: LiveControlMode,
    pub economic: crate::economic::EconomicPrepared,
    pub account_state: Option<LiveAccountStateAudit>,
    pub account_read_failure_evidence: Vec<RawHttpAttempt>,
    /// Descriptor hashes in response order for retained failed account reads.
    pub account_read_failure_request_descriptor_hashes: Vec<String>,
    pub account_read_failure_evidence_hashes: Vec<String>,
    pub verdict: LiveAdmissionVerdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveOrderPreparationFailure {
    Venue,
    PreparedAuditMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveOrderPreparationFailedAudit {
    pub identity: LiveOrderIdentity,
    pub failure: LiveOrderPreparationFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveOrderPreparedAudit {
    pub identity: LiveOrderIdentity,
    pub frozen_binding: CredentialBindingIdentity,
    pub economic: crate::economic::EconomicPrepared,
    pub account_state: LiveAccountStateAudit,
    pub prepared: PreparedPolymarketBuy,
}

impl LiveOrderPreparedAudit {
    pub fn new(
        identity: LiveOrderIdentity,
        frozen_binding: CredentialBindingIdentity,
        economic: crate::economic::EconomicPrepared,
        account_state: LiveAccountStateAudit,
        prepared: PreparedPolymarketBuy,
    ) -> Self {
        Self {
            identity,
            frozen_binding,
            economic,
            account_state,
            prepared,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveOrderPostAudit {
    pub identity: LiveOrderIdentity,
    pub order_hash: String,
    pub evidence: RawHttpAttempt,
    pub evidence_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveOrderRejectKind {
    VenueRejected,
    InvalidResponse,
    PreparationFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveOrderAmbiguityKind {
    Transport,
    Timeout,
    UnexpectedResponse,
    ReconciliationUnavailable,
    ReconciliationPending,
}

/// Venue-reported executed collateral and quantity from a successful order POST.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveExecutedAmounts {
    pub making_amount: rust_decimal::Decimal,
    pub taking_amount: rust_decimal::Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome", content = "detail")]
pub enum LiveJournalOrderOutcome {
    Matched {
        venue_order_id: String,
        /// Every matching nonzero transaction hash retained by authenticated reconciliation.
        #[serde(default)]
        transaction_hashes: Vec<String>,
        /// Legacy authenticated/post amounts remain audit-only; finalized logs own economics.
        #[serde(default)]
        executed: Option<LiveExecutedAmounts>,
    },
    Killed {
        venue_order_id: Option<String>,
    },
    Rejected {
        venue_order_id: Option<String>,
        kind: LiveOrderRejectKind,
    },
    Ambiguous {
        kind: LiveOrderAmbiguityKind,
    },
    FinalityPending {
        reason: String,
    },
    FinalityConflict {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveReconciliationSource {
    PostResponse,
    OrderHashLookupAndCancel,
    PolygonFinality,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveOrderReconciliationAudit {
    pub identity: LiveOrderIdentity,
    pub order_hash: String,
    pub source: LiveReconciliationSource,
    pub outcome: LiveJournalOrderOutcome,
    pub evidence: Vec<RawHttpAttempt>,
    pub evidence_hashes: Vec<String>,
}

/// Durable identity of one redemption attempt family.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionAttemptIdentity {
    pub account_id: AccountId,
    pub condition_id: PolymarketConditionId,
    pub adapter: String,
    pub custody_wallet: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedemptionCustodyAudit {
    DepositWallet,
    Proxy,
    Safe,
    Eoa,
}

/// Sanitized redemption request. Nonce, signature, and API credentials are unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionRequestAudit {
    pub call_to: String,
    pub calldata: Vec<u8>,
    pub condition_id: PolymarketConditionId,
    pub neg_risk: bool,
    pub custody: RedemptionCustodyAudit,
    pub signer_address: String,
    pub custody_wallet: String,
    pub deadline_unix: Option<u64>,
    pub metadata_hash: String,
    pub request_hash: String,
    pub schema_version: u16,
    pub parser_version: u16,
    pub adapter_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedemptionReceiptStatusAudit {
    Pending,
    Confirmed,
    TerminalFailure,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionRequestedAudit {
    pub identity: RedemptionAttemptIdentity,
    pub attempt_count: u32,
    pub redeemable_balance: CollateralAmount,
    pub request: RedemptionRequestAudit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionTransactionAudit {
    pub identity: RedemptionAttemptIdentity,
    pub attempt_count: u32,
    pub transaction_id: String,
    pub submit_body_hash: String,
    pub evidence: Vec<RawHttpAttempt>,
    pub evidence_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionReceiptAudit {
    pub identity: RedemptionAttemptIdentity,
    pub attempt_count: u32,
    pub transaction_id: String,
    pub transaction_hash: Option<String>,
    pub status: RedemptionReceiptStatusAudit,
    pub evidence: Vec<RawHttpAttempt>,
    pub evidence_hashes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveModeTransitionReason {
    Armed,
    OperatorKill,
    CredentialInvalidated,
    PromotionInvalidated,
    AccountClosedOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveModeTransitionAudit {
    pub requested: LiveControlMode,
    pub previous_effective: LiveControlMode,
    pub new_effective: LiveControlMode,
    pub reason: LiveModeTransitionReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchedLogIdentity {
    pub transaction_hash: String,
    pub log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderFillFinalizedAudit {
    pub identity: LiveOrderIdentity,
    pub prepared_journal_seq: u64,
    pub principal: CollateralAmount,
    pub quantity: ShareAmount,
    pub fee: CollateralAmount,
    pub matched_logs: Vec<MatchedLogIdentity>,
    pub chain_id: u64,
    pub finalized_head: u64,
    pub receipts: Vec<RawHttpAttempt>,
    pub blocks: Vec<RawHttpAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionFinalizedAudit {
    pub condition_id: PolymarketConditionId,
    pub payout_by_outcome_index_json: String,
    pub source_append_receipt: pe_event_log::AppendReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalPositionAudit {
    pub condition_id: PolymarketConditionId,
    pub outcome_index: u8,
    pub token_id: PolymarketTokenId,
    pub size: ShareAmount,
    pub redeemable: bool,
}

/// One retained complete-position page bound to the exact request that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePositionPageAudit {
    pub request_identity: String,
    pub receipt: pe_event_log::AppendReceipt,
}

/// Complete-position evidence for one explicitly requested custody wallet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePositionEvidenceAudit {
    pub requested_wallet: String,
    pub pages: Vec<LivePositionPageAudit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionCustodyReconciledAudit {
    pub identity: RedemptionAttemptIdentity,
    pub account_state: LiveAccountStateAudit,
    pub venue_positions: Vec<CanonicalPositionAudit>,
    pub venue_position_receipts: Vec<pe_event_log::AppendReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkKind {
    Baseline,
    Daily,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarkPrice {
    pub condition_id: PolymarketConditionId,
    pub outcome_index: u8,
    pub price: Price,
    pub receipt: pe_event_log::AppendReceipt,
    pub observed_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountPortfolioMarkedAudit {
    pub kind: MarkKind,
    pub cutoff_unix: i64,
    pub account_binding: LiveAccountBindingAudit,
    pub account_state: LiveAccountStateAudit,
    /// Inventory observed by the current complete-position account pass.
    pub venue_positions: Vec<CanonicalPositionAudit>,
    pub venue_position_evidence: LivePositionEvidenceAudit,
    /// Journal-derived inventory at `cutoff_unix`; distinct from the current venue observation.
    pub marked_positions: Vec<CanonicalPositionAudit>,
    pub prices: Vec<MarkPrice>,
    pub equity: CollateralAmount,
}

/// Opaque schema-one payload retained when the superseding schema cannot represent it losslessly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LegacyV1Payload(serde_json::Value);

/// Payload variants are sanitized by construction: there is no credential, secret, passphrase,
/// private key, nonce, or signature field anywhere in the journal schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
pub enum LiveJournalPayload {
    AdmissionEvaluated(Box<LiveAdmissionEvaluationAudit>),
    OrderPreparationFailed(Box<LiveOrderPreparationFailedAudit>),
    OrderPrepared(Box<LiveOrderPreparedAudit>),
    OrderPosted(Box<LiveOrderPostAudit>),
    OrderReconciled(Box<LiveOrderReconciliationAudit>),
    RedemptionRequested(Box<RedemptionRequestedAudit>),
    RedemptionTransactionIdentified(Box<RedemptionTransactionAudit>),
    RedemptionReceiptTransition(Box<RedemptionReceiptAudit>),
    OrderFillFinalized(Box<OrderFillFinalizedAudit>),
    ResolutionFinalized(Box<ResolutionFinalizedAudit>),
    RedemptionCustodyReconciled(Box<RedemptionCustodyReconciledAudit>),
    AccountPortfolioMarked(Box<AccountPortfolioMarkedAudit>),
    CredentialBindingMismatch {
        frozen: CredentialBindingIdentity,
        current: CredentialBindingIdentity,
    },
    ModeTransitionApplied(LiveModeTransitionAudit),
    LegacyV1(Box<LegacyV1Payload>),
}

/// One globally sequenced, account-tagged journal event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveJournalEvent {
    pub account_id: AccountId,
    pub seq: u64,
    pub timestamp: OffsetDateTime,
    pub payload: LiveJournalPayload,
}

/// Account-local tail and complete journal-prefix identity from one verified account replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveJournalTail {
    pub last_sequence: Option<EventSeq>,
    pub last_hash: blake3::Hash,
    pub scanned_prefix_last_sequence: Option<EventSeq>,
    pub scanned_prefix_last_hash: blake3::Hash,
}

/// One prepared ordinary-live order that has no matching terminal journal fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOrderInventoryEntry {
    pub identity: LiveOrderIdentity,
    pub account_id: AccountId,
    pub prepared_journal_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One nonterminal order plus the journal evidence required to recover it without a projection.
pub struct OpenOrderRecoveryEntry {
    pub inventory: OpenOrderInventoryEntry,
    pub prepared: Option<Box<LiveOrderPreparedAudit>>,
    pub transaction_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Verified journal-owned accounts and nonterminal orders for one recovery pass.
pub struct LiveRecoveryInventory {
    pub account_ids: Vec<AccountId>,
    pub open_orders: Vec<OpenOrderRecoveryEntry>,
}

#[derive(Debug, thiserror::Error)]
pub enum LiveJournalError {
    #[error("live journal path is a symlink or is not mode 0600")]
    InsecurePermissions,
    #[error("live journal file operation failed: {0}")]
    Io(#[from] io::Error),
    #[error("live journal event log failed: {0}")]
    EventLog(#[from] pe_event_log::LogError),
    #[error("live journal encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("live journal contains an unsupported envelope")]
    UnexpectedEnvelope,
    #[error("live journal event sequence does not match its envelope")]
    SequenceMismatch,
    #[error("live journal order facts do not form a strict Prepared-to-terminal sequence")]
    OrderFactConflict,
    #[error("retained HTTP response is not bound to its canonical account request")]
    RequestBinding,
    #[error("retained Polygon reconciliation evidence is incomplete, malformed, or misclassified")]
    InvalidFinalityEvidence,
    #[error("live journal mutex is poisoned")]
    Poisoned,
}

struct LiveJournalInner {
    writer: Writer,
    next_seq: u64,
}

/// Synchronized owner of the single ordinary-live journal stream.
pub struct LiveJournal {
    inner: Mutex<LiveJournalInner>,
}

impl LiveJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LiveJournalError> {
        let path = path.as_ref();
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink()
                    || metadata.permissions().mode() & 0o777 != 0o600
                {
                    return Err(LiveJournalError::InsecurePermissions);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)?;
            }
            Err(error) => return Err(error.into()),
        }

        let writer = Writer::open(path)?;
        let events = replay_all(path)?;
        let next_seq =
            u64::try_from(events.len()).map_err(|_| LiveJournalError::SequenceMismatch)?;
        Ok(Self {
            inner: Mutex::new(LiveJournalInner { writer, next_seq }),
        })
    }

    /// Native payload replay plus the shared physical scanner's exact tail binding (#544).
    pub fn verified_tail(path: impl AsRef<Path>) -> Result<LogTailBinding, LiveJournalError> {
        let events = replay_all(path.as_ref())?;
        let binding = Reader::verified_tail(path.as_ref())?;
        let expected =
            u64::try_from(events.len()).map_err(|_| LiveJournalError::SequenceMismatch)?;
        let actual = match binding.last_sequence {
            None => 0,
            Some(sequence) => sequence
                .0
                .checked_add(1)
                .ok_or(LiveJournalError::SequenceMismatch)?,
        };
        if expected != actual {
            return Err(LiveJournalError::SequenceMismatch);
        }
        Ok(binding)
    }

    /// Typed writer poison state for service readiness and producer shutdown.
    pub fn poisoned(&self) -> Result<Option<PoisonReason>, LiveJournalError> {
        let inner = self.inner.lock().map_err(|_| LiveJournalError::Poisoned)?;
        Ok(inner.writer.poisoned().copied())
    }

    /// Append and fsync one event. The supplied timestamp is used for both payload and envelope.
    pub fn append(
        &self,
        account_id: AccountId,
        timestamp: OffsetDateTime,
        payload: LiveJournalPayload,
    ) -> Result<LiveJournalEvent, LiveJournalError> {
        let mut inner = self.inner.lock().map_err(|_| LiveJournalError::Poisoned)?;
        let event = LiveJournalEvent {
            account_id,
            seq: inner.next_seq,
            timestamp,
            payload,
        };
        let payload = serde_json::to_vec(&event)?;
        let assigned = inner.writer.append(EnvelopeIn {
            source_id: SourceId(LIVE_JOURNAL_SOURCE.to_owned()),
            schema_version: LIVE_JOURNAL_SCHEMA_VERSION,
            parser_version: LIVE_JOURNAL_PARSER_VERSION,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload,
        })?;
        if assigned.0 != event.seq {
            return Err(LiveJournalError::SequenceMismatch);
        }
        inner.writer.sync()?;
        inner.next_seq = inner
            .next_seq
            .checked_add(1)
            .ok_or(LiveJournalError::SequenceMismatch)?;
        Ok(event)
    }
}

/// Replay only `account_id` while validating the complete global hash chain and sequence.
pub fn replay_account(
    path: impl AsRef<Path>,
    account_id: &AccountId,
) -> Result<Vec<LiveJournalEvent>, LiveJournalError> {
    replay_all(path).map(|events| {
        events
            .into_iter()
            .filter(|event| &event.account_id == account_id)
            .collect()
    })
}

/// Replay one account and bind that projection to its final event in the verified journal scan.
pub fn replay_account_with_tail(
    path: impl AsRef<Path>,
    account_id: &AccountId,
) -> Result<(Vec<LiveJournalEvent>, LiveJournalTail), LiveJournalError> {
    let replayed = replay_all_with_receipts(path)?;
    let scanned_prefix = replayed
        .last()
        .map_or((None, blake3::Hash::from_bytes([0; 32])), |(_, receipt)| {
            (Some(receipt.sequence), receipt.this_hash)
        });
    let mut tail = LiveJournalTail {
        last_sequence: None,
        last_hash: blake3::Hash::from_bytes([0; 32]),
        scanned_prefix_last_sequence: scanned_prefix.0,
        scanned_prefix_last_hash: scanned_prefix.1,
    };
    let events = replayed
        .into_iter()
        .filter_map(|(event, receipt)| {
            (&event.account_id == account_id).then(|| {
                tail = LiveJournalTail {
                    last_sequence: Some(receipt.sequence),
                    last_hash: receipt.this_hash,
                    ..tail
                };
                event
            })
        })
        .collect();
    Ok((events, tail))
}

/// Verify that an account-local tail is the final account event in the recorded journal prefix.
///
/// The current journal may contain a suffix appended after the checkpoint. The recorded global
/// prefix identity makes an account-local `None` independently verifiable even in that case.
pub fn verify_account_tail_checkpoint(
    path: impl AsRef<Path>,
    account_id: &AccountId,
    checkpoint: LiveJournalTail,
) -> Result<(), LiveJournalError> {
    let replayed = replay_all_with_receipts(path)?;
    let prefix_len = match checkpoint.scanned_prefix_last_sequence {
        None => {
            if checkpoint.scanned_prefix_last_hash != blake3::Hash::from_bytes([0; 32]) {
                return Err(LiveJournalError::SequenceMismatch);
            }
            0
        }
        Some(last_sequence) => {
            let prefix_len = last_sequence
                .0
                .checked_add(1)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(LiveJournalError::SequenceMismatch)?;
            let (_, receipt) = replayed
                .get(prefix_len - 1)
                .ok_or(LiveJournalError::SequenceMismatch)?;
            if receipt.sequence != last_sequence
                || receipt.this_hash != checkpoint.scanned_prefix_last_hash
            {
                return Err(LiveJournalError::SequenceMismatch);
            }
            prefix_len
        }
    };
    let scanned_prefix = replayed
        .get(..prefix_len)
        .ok_or(LiveJournalError::SequenceMismatch)?;
    let actual_account_tail = scanned_prefix
        .iter()
        .rev()
        .find(|(event, _)| &event.account_id == account_id)
        .map_or((None, blake3::Hash::from_bytes([0; 32])), |(_, receipt)| {
            (Some(receipt.sequence), receipt.this_hash)
        });
    if actual_account_tail != (checkpoint.last_sequence, checkpoint.last_hash) {
        return Err(LiveJournalError::SequenceMismatch);
    }
    Ok(())
}

/// A journal fact whose identity must match an earlier `OrderPrepared` exactly.
pub enum PreparedOrderFact<'a> {
    Posted(&'a LiveOrderPostAudit),
    Reconciled(&'a LiveOrderReconciliationAudit),
    Finalized(&'a OrderFillFinalizedAudit),
}

/// Exact shared binding between an `OrderPrepared` record and every later order fact. The account
/// envelope is checked by the caller before invoking this matcher.
#[must_use]
pub fn prepared_order_fact_matches(
    prepared_seq: u64,
    prepared_identity: &LiveOrderIdentity,
    prepared_order_hash: &str,
    fact: PreparedOrderFact<'_>,
) -> bool {
    match fact {
        PreparedOrderFact::Posted(posted) => {
            posted.identity == *prepared_identity && posted.order_hash == prepared_order_hash
        }
        PreparedOrderFact::Reconciled(reconciled) => {
            let source_matches_outcome = matches!(
                (&reconciled.source, &reconciled.outcome),
                (
                    LiveReconciliationSource::PostResponse
                        | LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Matched { .. }
                        | LiveJournalOrderOutcome::Killed { .. }
                        | LiveJournalOrderOutcome::Rejected { .. }
                        | LiveJournalOrderOutcome::Ambiguous { .. }
                ) | (
                    LiveReconciliationSource::PolygonFinality,
                    LiveJournalOrderOutcome::FinalityPending { .. }
                        | LiveJournalOrderOutcome::FinalityConflict { .. }
                )
            );
            reconciled.identity == *prepared_identity
                && reconciled.order_hash == prepared_order_hash
                && source_matches_outcome
        }
        PreparedOrderFact::Finalized(finalized) => {
            finalized.identity == *prepared_identity
                && finalized.prepared_journal_seq == prepared_seq
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RetainedPolygonDisposition {
    Reconciled(LiveJournalOrderOutcome),
    Finalized,
}

fn malformed_polygon_body(error: &ReceiptError) -> bool {
    matches!(
        error,
        ReceiptError::MalformedRpc
            | ReceiptError::MissingResult
            | ReceiptError::UnsupportedFinalizedTag
            | ReceiptError::MalformedHex
    )
}

fn unavailable_polygon_error(error: &ReceiptError) -> bool {
    matches!(
        error,
        ReceiptError::MalformedRpc
            | ReceiptError::RpcError
            | ReceiptError::MissingResult
            | ReceiptError::UnsupportedFinalizedTag
    )
}

fn verified_rpc_body<'a>(
    attempt: &'a RawHttpAttempt,
    endpoint_kind: &str,
    method: &str,
    params: &serde_json::Value,
) -> Result<Option<&'a [u8]>, LiveJournalError> {
    let valid_query = |ordered_query: &[(String, String)]| {
        ordered_query.len() == 2
            && ordered_query[0] == ("rpc_method".to_owned(), method.to_owned())
            && ordered_query[1].0 == "rpc_params"
            && serde_json::from_str::<serde_json::Value>(&ordered_query[1].1)
                .is_ok_and(|actual| actual == *params)
    };
    match attempt {
        RawHttpAttempt::Response(response) => {
            if response.source_id != "polygon-receipt-rpc"
                || response.endpoint_kind != endpoint_kind
                || response.method != "POST"
                || response.path.is_empty()
                || !valid_query(&response.ordered_query)
                || response.attempt_ordinal != 1
                || response.received_at < response.observed_at
                || response.schema_version != 1
                || response.parser_version != 1
                || response.adapter_version.is_empty()
            {
                return Err(LiveJournalError::InvalidFinalityEvidence);
            }
            Ok((200..300)
                .contains(&response.status)
                .then_some(response.body.as_slice()))
        }
        RawHttpAttempt::TransportFailure(failure) => {
            if failure.source_id != "polygon-receipt-rpc"
                || failure.endpoint_kind != endpoint_kind
                || failure.method != "POST"
                || failure.path.is_empty()
                || !valid_query(&failure.ordered_query)
                || failure.attempt_ordinal != 1
                || failure.received_at < failure.observed_at
                || failure.schema_version != 1
                || failure.parser_version != 1
                || failure.adapter_version.is_empty()
            {
                return Err(LiveJournalError::InvalidFinalityEvidence);
            }
            Ok(None)
        }
    }
}

fn classify_retained_polygon_finality(
    prepared: &LiveOrderPreparedAudit,
    transaction_hashes: &[String],
    chain_id: Option<u64>,
    head: &Result<pe_venue_polymarket::FinalizedBlock, ReceiptError>,
    receipts: &BTreeMap<String, Result<Option<MatchedReceipt>, ReceiptError>>,
    blocks: &BTreeMap<u64, Result<pe_venue_polymarket::FinalizedBlock, ReceiptError>>,
) -> RetainedPolygonDisposition {
    let pending = |reason| {
        RetainedPolygonDisposition::Reconciled(LiveJournalOrderOutcome::FinalityPending { reason })
    };
    let conflict = |reason| {
        RetainedPolygonDisposition::Reconciled(LiveJournalOrderOutcome::FinalityConflict { reason })
    };
    if chain_id != Some(pe_venue_polymarket::FINALIZED_CHAIN_ID) {
        return pending("polygon chain identity unavailable or not 137".to_owned());
    }
    let Ok(head) = head else {
        return pending("polygon finalized head unavailable".to_owned());
    };
    if transaction_hashes.is_empty() {
        return pending("authenticated match has no nonzero transaction hash yet".to_owned());
    }

    let mut fills = BTreeMap::<(String, u64), DecodedOrderFill>::new();
    let mut has_unfinalized = false;
    for hash in transaction_hashes {
        let Some(observation) = receipts.get(hash) else {
            return conflict(format!(
                "receipt request missing for authenticated transaction {hash}"
            ));
        };
        let receipt = match observation {
            Ok(Some(receipt)) => receipt,
            Ok(None) => {
                has_unfinalized = true;
                continue;
            }
            Err(error) if unavailable_polygon_error(error) => {
                return pending(format!("transaction receipt unavailable: {error}"));
            }
            Err(error) => return conflict(format!("transaction receipt conflict: {error}")),
        };
        if receipt.block_number > head.number {
            has_unfinalized = true;
            continue;
        }
        if receipt.block_number == head.number {
            if receipt.block_hash != head.hash {
                return conflict("receipt block hash conflicts with finalized head".to_owned());
            }
        } else {
            let Some(block) = blocks.get(&receipt.block_number) else {
                return conflict("canonical lower block evidence is missing".to_owned());
            };
            match block {
                Ok(canonical) if canonical.hash == receipt.block_hash => {}
                Ok(_) => return conflict("receipt block hash is not canonical".to_owned()),
                Err(error) if unavailable_polygon_error(error) => {
                    return pending(format!("canonical block unavailable: {error}"));
                }
                Err(error) => return conflict(format!("canonical block conflict: {error}")),
            }
        }
        let decoded = match decode_order_fills(receipt, &prepared.prepared) {
            Ok(decoded) => decoded,
            Err(error) => return conflict(format!("OrderFilled conflict: {error}")),
        };
        for fill in decoded {
            let key = (fill.transaction_hash.clone(), fill.log_index);
            match fills.get(&key) {
                Some(existing) if existing == &fill => {}
                Some(_) => {
                    return conflict(
                        "OrderFilled log identity was reused with different content".to_owned(),
                    );
                }
                None => {
                    fills.insert(key, fill);
                }
            }
        }
    }
    if has_unfinalized {
        return pending("one or more authenticated transactions are not finalized".to_owned());
    }
    if fills.is_empty() {
        return conflict("finalized receipts contain zero matching OrderFilled logs".to_owned());
    }
    let aggregates = fills.values().try_fold(
        (
            CollateralAmount::ZERO,
            ShareAmount::ZERO,
            CollateralAmount::ZERO,
        ),
        |(principal, quantity, fee), fill| {
            Some((
                principal.checked_add(fill.principal).ok()?,
                quantity.checked_add(fill.quantity).ok()?,
                fee.checked_add(fill.fee).ok()?,
            ))
        },
    );
    let Some((principal, quantity, fee)) = aggregates else {
        return conflict("finalized fill aggregation overflow".to_owned());
    };
    if prepared.economic.sizing.principal != prepared.prepared.maker_collateral
        || prepared.economic.ladder.minimum_shares != prepared.prepared.taker_shares
    {
        return conflict("prepared economics disagree with the signed order".to_owned());
    }
    let expected_principal = prepared.prepared.maker_collateral;
    if principal < expected_principal {
        return pending("finalized principal remains below the signed principal".to_owned());
    }
    if principal > expected_principal {
        return conflict("finalized principal exceeds the signed principal".to_owned());
    }
    if quantity < prepared.economic.ladder.minimum_shares {
        return conflict(
            "full-principal finalized quantity is below the signed minimum".to_owned(),
        );
    }
    if !fee_within_reserve(fee, prepared.economic.fee.reserve) {
        return conflict("finalized fee exceeds the prepared reserve".to_owned());
    }
    RetainedPolygonDisposition::Finalized
}

/// Strictly re-execute a retained Polygon Pending/Conflict fact from its complete raw evidence.
/// The returned receipts are the immutable canonical observations that a later Final must retain.
pub fn verify_polygon_reconciliation(
    prepared: &LiveOrderPreparedAudit,
    transaction_hashes: &[String],
    reconciled: &LiveOrderReconciliationAudit,
) -> Result<BTreeMap<String, MatchedReceipt>, LiveJournalError> {
    if reconciled.source != LiveReconciliationSource::PolygonFinality
        || http_attempt_hashes(&reconciled.evidence)? != reconciled.evidence_hashes
    {
        return Err(LiveJournalError::InvalidFinalityEvidence);
    }
    let ordered_hashes = transaction_hashes.iter().cloned().collect::<BTreeSet<_>>();
    if ordered_hashes.len() != transaction_hashes.len() {
        return Err(LiveJournalError::InvalidFinalityEvidence);
    }
    let ordered_hashes = ordered_hashes.into_iter().collect::<Vec<_>>();
    let mut attempts = reconciled.evidence.iter();
    let chain_attempt = attempts
        .next()
        .ok_or(LiveJournalError::InvalidFinalityEvidence)?;
    let chain_body = verified_rpc_body(
        chain_attempt,
        "polygon-chain-id",
        "eth_chainId",
        &serde_json::json!([]),
    )?;
    let chain_id = match chain_body {
        Some(body) => Some(
            parse_chain_id_response(body).map_err(|_| LiveJournalError::InvalidFinalityEvidence)?,
        ),
        None => None,
    };

    let mut receipts = BTreeMap::new();
    for hash in &ordered_hashes {
        let attempt = attempts
            .next()
            .ok_or(LiveJournalError::InvalidFinalityEvidence)?;
        let body = verified_rpc_body(
            attempt,
            "polygon-transaction-receipt",
            "eth_getTransactionReceipt",
            &serde_json::json!([hash]),
        )?;
        let parsed = match body {
            Some(body) => match parse_receipt_response(body, hash) {
                Err(error) if malformed_polygon_body(&error) => {
                    return Err(LiveJournalError::InvalidFinalityEvidence);
                }
                parsed => parsed,
            },
            None => Err(ReceiptError::MalformedRpc),
        };
        receipts.insert(hash.clone(), parsed);
    }

    let head_attempt = attempts
        .next()
        .ok_or(LiveJournalError::InvalidFinalityEvidence)?;
    let head_body = verified_rpc_body(
        head_attempt,
        "polygon-finalized-block",
        "eth_getBlockByNumber",
        &serde_json::json!(["finalized", false]),
    )?;
    let head = match head_body {
        Some(body) => Ok(parse_finalized_block_response(body)
            .map_err(|_| LiveJournalError::InvalidFinalityEvidence)?),
        None => Err(ReceiptError::MalformedRpc),
    };

    let lower_heights = match &head {
        Ok(head) => receipts
            .values()
            .filter_map(|receipt| match receipt {
                Ok(Some(receipt)) if receipt.block_number < head.number => {
                    Some(receipt.block_number)
                }
                Ok(Some(_)) | Ok(None) | Err(_) => None,
            })
            .collect::<BTreeSet<_>>(),
        Err(_) => BTreeSet::new(),
    };
    let mut blocks = BTreeMap::new();
    for height in lower_heights {
        let expected_hash = receipts.values().find_map(|receipt| match receipt {
            Ok(Some(receipt)) if receipt.block_number == height => {
                Some(receipt.block_hash.as_str())
            }
            Ok(Some(_)) | Ok(None) | Err(_) => None,
        });
        let expected_hash = expected_hash.ok_or(LiveJournalError::InvalidFinalityEvidence)?;
        let attempt = attempts
            .next()
            .ok_or(LiveJournalError::InvalidFinalityEvidence)?;
        let body = verified_rpc_body(
            attempt,
            "polygon-canonical-block",
            "eth_getBlockByNumber",
            &serde_json::json!([format!("0x{height:x}"), false]),
        )?;
        let block = match body {
            Some(body) => match canonical_block_matches(body, height, expected_hash) {
                Err(error) if malformed_polygon_body(&error) => {
                    return Err(LiveJournalError::InvalidFinalityEvidence);
                }
                block => block,
            },
            None => Err(ReceiptError::MalformedRpc),
        };
        blocks.insert(height, block);
    }
    if attempts.next().is_some() {
        return Err(LiveJournalError::InvalidFinalityEvidence);
    }

    let derived = classify_retained_polygon_finality(
        prepared,
        &ordered_hashes,
        chain_id,
        &head,
        &receipts,
        &blocks,
    );
    if derived != RetainedPolygonDisposition::Reconciled(reconciled.outcome.clone()) {
        return Err(LiveJournalError::InvalidFinalityEvidence);
    }

    let mut immutable = BTreeMap::new();
    if chain_id == Some(pe_venue_polymarket::FINALIZED_CHAIN_ID)
        && let Ok(head) = &head
    {
        for (hash, receipt) in &receipts {
            let Ok(Some(receipt)) = receipt else {
                continue;
            };
            let canonical = if receipt.block_number > head.number {
                false
            } else if receipt.block_number == head.number {
                receipt.block_hash == head.hash
            } else {
                blocks.get(&receipt.block_number).is_some_and(|block| {
                    block
                        .as_ref()
                        .is_ok_and(|block| block.hash == receipt.block_hash)
                })
            };
            if canonical {
                immutable.insert(hash.clone(), receipt.clone());
            }
        }
    }
    Ok(immutable)
}

struct OpenOrderState {
    entry: OpenOrderInventoryEntry,
    order_hash: String,
    prepared: Option<Box<LiveOrderPreparedAudit>>,
    transaction_hashes: BTreeSet<String>,
    terminal: bool,
    finality_conflict: bool,
    terminal_reconciliation: Option<Box<LiveOrderReconciliationAudit>>,
}

fn insert_open_order(
    orders: &mut BTreeMap<(AccountId, String), OpenOrderState>,
    account_id: AccountId,
    prepared_journal_seq: u64,
    identity: LiveOrderIdentity,
    order_hash: String,
    prepared: Option<Box<LiveOrderPreparedAudit>>,
) -> Result<(), LiveJournalError> {
    let key = (account_id.clone(), identity.idempotency_key.clone());
    if orders.contains_key(&key) {
        return Err(LiveJournalError::OrderFactConflict);
    }
    orders.insert(
        key,
        OpenOrderState {
            entry: OpenOrderInventoryEntry {
                identity,
                account_id,
                prepared_journal_seq,
            },
            order_hash,
            prepared,
            transaction_hashes: BTreeSet::new(),
            terminal: false,
            finality_conflict: false,
            terminal_reconciliation: None,
        },
    );
    Ok(())
}

/// Replay the verified global journal once and return every Prepared order without a terminal fact.
/// `Killed`, `Rejected`, `FinalityConflict`, and `OrderFillFinalized` are terminal. A finality
/// conflict deliberately preserves the reservation while removing the order from automated
/// recovery; only an operator may resolve the frozen journal state.
pub fn open_order_inventory(
    path: impl AsRef<Path>,
) -> Result<Vec<OpenOrderInventoryEntry>, LiveJournalError> {
    Ok(recovery_inventory(path)?
        .open_orders
        .into_iter()
        .map(|order| order.inventory)
        .collect())
}

pub fn recovery_inventory(
    path: impl AsRef<Path>,
) -> Result<LiveRecoveryInventory, LiveJournalError> {
    let events = replay_all(path)?;
    let mut orders = BTreeMap::<(AccountId, String), OpenOrderState>::new();
    let mut account_ids = BTreeSet::new();
    for event in events {
        account_ids.insert(event.account_id.clone());
        match event.payload {
            LiveJournalPayload::OrderPrepared(prepared) => {
                let identity = prepared.identity.clone();
                let order_hash = prepared.prepared.order_hash.clone();
                insert_open_order(
                    &mut orders,
                    event.account_id,
                    event.seq,
                    identity,
                    order_hash,
                    Some(prepared),
                )?;
            }
            LiveJournalPayload::LegacyV1(raw) => {
                if let Some((identity, order_hash)) = legacy_v1::prepared_order(&raw.0)? {
                    insert_open_order(
                        &mut orders,
                        event.account_id,
                        event.seq,
                        identity,
                        order_hash,
                        None,
                    )?;
                }
            }
            LiveJournalPayload::OrderPosted(posted) => {
                let key = (event.account_id, posted.identity.idempotency_key.clone());
                let order = orders
                    .get(&key)
                    .ok_or(LiveJournalError::OrderFactConflict)?;
                if !prepared_order_fact_matches(
                    order.entry.prepared_journal_seq,
                    &order.entry.identity,
                    &order.order_hash,
                    PreparedOrderFact::Posted(&posted),
                ) {
                    return Err(LiveJournalError::OrderFactConflict);
                }
            }
            LiveJournalPayload::OrderReconciled(reconciled) => {
                let key = (
                    event.account_id,
                    reconciled.identity.idempotency_key.clone(),
                );
                let order = orders
                    .get_mut(&key)
                    .ok_or(LiveJournalError::OrderFactConflict)?;
                if order.terminal {
                    if order.terminal_reconciliation.as_deref() == Some(reconciled.as_ref()) {
                        continue;
                    }
                    return Err(LiveJournalError::OrderFactConflict);
                }
                if !prepared_order_fact_matches(
                    order.entry.prepared_journal_seq,
                    &order.entry.identity,
                    &order.order_hash,
                    PreparedOrderFact::Reconciled(&reconciled),
                ) {
                    return Err(LiveJournalError::OrderFactConflict);
                }
                if reconciled.source == LiveReconciliationSource::PolygonFinality {
                    let prepared = order
                        .prepared
                        .as_deref()
                        .ok_or(LiveJournalError::InvalidFinalityEvidence)?;
                    let transaction_hashes =
                        order.transaction_hashes.iter().cloned().collect::<Vec<_>>();
                    verify_polygon_reconciliation(prepared, &transaction_hashes, &reconciled)?;
                }
                if let LiveJournalOrderOutcome::Matched {
                    transaction_hashes, ..
                } = &reconciled.outcome
                {
                    order
                        .transaction_hashes
                        .extend(transaction_hashes.iter().cloned());
                }
                if matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::Killed { .. }
                        | LiveJournalOrderOutcome::Rejected { .. }
                ) {
                    order.terminal = true;
                    order.terminal_reconciliation = Some(reconciled.clone());
                }
                if matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::FinalityConflict { .. }
                ) {
                    order.terminal = true;
                    order.finality_conflict = true;
                    order.terminal_reconciliation = Some(reconciled);
                }
            }
            LiveJournalPayload::OrderFillFinalized(finalized) => {
                let key = (event.account_id, finalized.identity.idempotency_key.clone());
                let order = orders
                    .get_mut(&key)
                    .ok_or(LiveJournalError::OrderFactConflict)?;
                if !prepared_order_fact_matches(
                    order.entry.prepared_journal_seq,
                    &order.entry.identity,
                    &order.order_hash,
                    PreparedOrderFact::Finalized(&finalized),
                ) {
                    return Err(LiveJournalError::OrderFactConflict);
                }
                if order.terminal || order.finality_conflict {
                    return Err(LiveJournalError::OrderFactConflict);
                }
                order.terminal = true;
            }
            LiveJournalPayload::AdmissionEvaluated(_)
            | LiveJournalPayload::OrderPreparationFailed(_)
            | LiveJournalPayload::RedemptionRequested(_)
            | LiveJournalPayload::RedemptionTransactionIdentified(_)
            | LiveJournalPayload::RedemptionReceiptTransition(_)
            | LiveJournalPayload::ResolutionFinalized(_)
            | LiveJournalPayload::RedemptionCustodyReconciled(_)
            | LiveJournalPayload::AccountPortfolioMarked(_)
            | LiveJournalPayload::CredentialBindingMismatch { .. }
            | LiveJournalPayload::ModeTransitionApplied(_) => {}
        }
    }
    let mut open_orders = orders
        .into_values()
        .filter(|order| !order.terminal)
        .map(|order| OpenOrderRecoveryEntry {
            inventory: order.entry,
            prepared: order.prepared,
            transaction_hashes: order.transaction_hashes.into_iter().collect(),
        })
        .collect::<Vec<_>>();
    open_orders.sort_by_key(|order| order.inventory.prepared_journal_seq);
    Ok(LiveRecoveryInventory {
        account_ids: account_ids.into_iter().collect(),
        open_orders,
    })
}

mod legacy_v1 {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveAccountStateAudit {
        pub observed_at: OffsetDateTime,
        pub closed_only: bool,
        pub geoblocked: bool,
        pub selected_spender: String,
        pub collateral_balance: CollateralAmount,
        pub allowance: CollateralAmount,
        pub reconciled_free_collateral: CollateralAmount,
        pub schema_version: u16,
        pub parser_version: u16,
        pub evidence: Vec<RawHttpAttempt>,
        pub evidence_hashes: Vec<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveFeeEvidenceAudit {
        pub gamma_fees_enabled: Option<serde_json::Value>,
        pub gamma_fee_schedule: Option<serde_json::Value>,
        pub gamma_maker_base_fee_bps: Option<serde_json::Value>,
        pub gamma_taker_base_fee_bps: Option<serde_json::Value>,
        pub clob_maker_base_fee_bps: Option<serde_json::Value>,
        pub clob_taker_base_fee_bps: Option<serde_json::Value>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveMarketEvidenceAudit {
        pub condition_id: PolymarketConditionId,
        pub ordered_outcome_token_ids: [PolymarketTokenId; 2],
        pub neg_risk: bool,
        pub minimum_tick_size: Price,
        pub minimum_order_size: ShareAmount,
        pub fee_evidence: LiveFeeEvidenceAudit,
        pub raw_gamma_market_hash: String,
        pub raw_clob_market_hash: String,
        pub observed_at_unix: i64,
        pub schema_version: u32,
        pub parser_version: u32,
        pub freshness_window_secs: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveAdmissionArtifactAudit {
        pub market: LiveMarketEvidenceAudit,
        pub settlement: VenueSettlementRecord,
        pub artifact_bundle_hash: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LadderPlanAudit {
        pub used_asks: Vec<LadderAskAudit>,
        pub best_ask: Price,
        pub limit_price: Price,
        pub shares: ShareAmount,
        pub estimated_ladder_spend: CollateralAmount,
        pub worst_case_debit: CollateralAmount,
        pub plan_hash: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveAdmissionEvaluationAudit {
        pub identity: LiveOrderIdentity,
        pub frozen_binding: CredentialBindingIdentity,
        pub current_binding: CredentialBindingIdentity,
        pub requested_mode: LiveControlMode,
        pub effective_mode: LiveControlMode,
        pub artifact: LiveAdmissionArtifactAudit,
        pub ladder: LadderPlanAudit,
        pub account_state: Option<LiveAccountStateAudit>,
        pub account_read_failure_evidence: Vec<RawHttpAttempt>,
        pub account_read_failure_evidence_hashes: Vec<String>,
        pub verdict: LiveAdmissionVerdict,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveOrderPreparedAudit {
        pub identity: LiveOrderIdentity,
        pub frozen_binding: CredentialBindingIdentity,
        pub admission: LiveAdmissionArtifactAudit,
        pub account_state: LiveAccountStateAudit,
        pub ladder: LadderPlanAudit,
        pub prepared: PreparedPolymarketBuy,
        pub prepared_audit_hash: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
    pub(super) enum LiveJournalPayload {
        AdmissionEvaluated(Box<LiveAdmissionEvaluationAudit>),
        OrderPreparationFailed(Box<LiveOrderPreparationFailedAudit>),
        OrderPrepared(Box<LiveOrderPreparedAudit>),
        OrderPosted(Box<LiveOrderPostAudit>),
        OrderReconciled(Box<LiveOrderReconciliationAudit>),
        RedemptionRequested(Box<RedemptionRequestedAudit>),
        RedemptionTransactionIdentified(Box<RedemptionTransactionAudit>),
        RedemptionReceiptTransition(Box<RedemptionReceiptAudit>),
        CredentialBindingMismatch {
            frozen: CredentialBindingIdentity,
            current: CredentialBindingIdentity,
        },
        ModeTransitionApplied(LiveModeTransitionAudit),
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct LiveJournalEvent {
        pub account_id: AccountId,
        pub seq: u64,
        pub timestamp: OffsetDateTime,
        pub payload: LiveJournalPayload,
    }

    pub(super) fn prepared_order(
        raw_event: &serde_json::Value,
    ) -> Result<Option<(LiveOrderIdentity, String)>, serde_json::Error> {
        let event = serde_json::from_value::<LiveJournalEvent>(raw_event.clone())?;
        Ok(match event.payload {
            LiveJournalPayload::OrderPrepared(prepared) => {
                Some((prepared.identity, prepared.prepared.order_hash))
            }
            _ => None,
        })
    }

    pub(super) fn convert(
        event: LiveJournalEvent,
        raw_event: serde_json::Value,
    ) -> super::LiveJournalEvent {
        let payload = match event.payload {
            LiveJournalPayload::AdmissionEvaluated(_) | LiveJournalPayload::OrderPrepared(_) => {
                super::LiveJournalPayload::LegacyV1(Box::new(super::LegacyV1Payload(raw_event)))
            }
            LiveJournalPayload::OrderPreparationFailed(value) => {
                super::LiveJournalPayload::OrderPreparationFailed(value)
            }
            LiveJournalPayload::OrderPosted(value) => super::LiveJournalPayload::OrderPosted(value),
            LiveJournalPayload::OrderReconciled(value) => {
                super::LiveJournalPayload::OrderReconciled(value)
            }
            LiveJournalPayload::RedemptionRequested(value) => {
                super::LiveJournalPayload::RedemptionRequested(value)
            }
            LiveJournalPayload::RedemptionTransactionIdentified(value) => {
                super::LiveJournalPayload::RedemptionTransactionIdentified(value)
            }
            LiveJournalPayload::RedemptionReceiptTransition(value) => {
                super::LiveJournalPayload::RedemptionReceiptTransition(value)
            }
            LiveJournalPayload::CredentialBindingMismatch { frozen, current } => {
                super::LiveJournalPayload::CredentialBindingMismatch { frozen, current }
            }
            LiveJournalPayload::ModeTransitionApplied(value) => {
                super::LiveJournalPayload::ModeTransitionApplied(value)
            }
        };
        super::LiveJournalEvent {
            account_id: event.account_id,
            seq: event.seq,
            timestamp: event.timestamp,
            payload,
        }
    }
}

fn replay_all(path: impl AsRef<Path>) -> Result<Vec<LiveJournalEvent>, LiveJournalError> {
    replay_all_with_receipts(path)
        .map(|events| events.into_iter().map(|(event, _receipt)| event).collect())
}

fn replay_all_with_receipts(
    path: impl AsRef<Path>,
) -> Result<Vec<(LiveJournalEvent, AppendReceipt)>, LiveJournalError> {
    let mut events = Vec::new();
    for item in Reader::replay(path)? {
        let (seq, envelope) = item?;
        if envelope.source_id != SourceId(LIVE_JOURNAL_SOURCE.to_owned())
            || !matches!(envelope.schema_version, 1 | LIVE_JOURNAL_SCHEMA_VERSION)
            || envelope.parser_version != LIVE_JOURNAL_PARSER_VERSION
            || envelope.content_type != ContentType::Json
        {
            return Err(LiveJournalError::UnexpectedEnvelope);
        }
        let event = if envelope.schema_version == 1 {
            let raw_event = serde_json::from_slice(&envelope.payload)?;
            let legacy = serde_json::from_slice(&envelope.payload)?;
            legacy_v1::convert(legacy, raw_event)
        } else {
            serde_json::from_slice(&envelope.payload)?
        };
        let expected =
            u64::try_from(events.len()).map_err(|_| LiveJournalError::SequenceMismatch)?;
        if seq.0 != expected || event.seq != expected || event.timestamp != envelope.observed_at.0 {
            return Err(LiveJournalError::SequenceMismatch);
        }
        events.push((
            event,
            AppendReceipt {
                sequence: seq,
                this_hash: envelope.this_hash,
            },
        ));
    }
    Ok(events)
}

pub(crate) fn hash_serializable<T: Serialize + ?Sized>(
    value: &T,
) -> Result<String, LiveJournalError> {
    serde_json::to_vec(value)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(Into::into)
}

/// Stable hash that binds an account/custody request descriptor into retained account evidence.
pub fn request_descriptor_hash(
    descriptor: &SanitizedHttpRequestDescriptor,
) -> Result<String, LiveJournalError> {
    hash_serializable(&(
        "prediction-edge/live-http-request-descriptor/v1",
        descriptor,
    ))
}

/// Verify an explicit retained descriptor hash before its response body is parsed.
pub fn verify_http_response_request(
    descriptor: &SanitizedHttpRequestDescriptor,
    retained_descriptor_hash: &str,
) -> Result<(), LiveJournalError> {
    if request_descriptor_hash(descriptor)? != retained_descriptor_hash {
        return Err(LiveJournalError::RequestBinding);
    }
    Ok(())
}

pub(crate) fn http_attempt_hash(attempt: &RawHttpAttempt) -> Result<String, LiveJournalError> {
    hash_serializable(&("prediction-edge/live-http-attempt/v1", attempt))
}

pub fn http_attempt_hashes(attempts: &[RawHttpAttempt]) -> Result<Vec<String>, LiveJournalError> {
    attempts.iter().map(http_attempt_hash).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::os::unix::fs::PermissionsExt as _;

    use pe_core_types::{BasisPoints, EventSeq, Side};
    use pe_risk_engine::RiskSnapshot;
    use tempfile::tempdir;
    use time::macros::datetime;

    use super::*;
    use crate::economic::{
        BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit, MarketSelection,
        RiskAudit, RiskDecisionAudit, SizingAudit, SizingModeAudit,
    };

    fn identity(dispatch: &str) -> LiveOrderIdentity {
        LiveOrderIdentity {
            dispatch_id: dispatch.to_owned(),
            idempotency_key: format!("key-{dispatch}"),
            quote_id: "quote".to_owned(),
            config_hash: "config".to_owned(),
            decision_hash: "decision".to_owned(),
            evidence_hashes: vec!["evidence".to_owned()],
            fill_projection: None,
            schema_version: 1,
            parser_version: 1,
        }
    }

    fn current_prepared(dispatch: &str) -> Box<LiveOrderPreparedAudit> {
        let identity = identity(dispatch);
        let condition_id = PolymarketConditionId("condition".to_owned());
        let token_id = PolymarketTokenId("yes".to_owned());
        let price = Price::new(rust_decimal::Decimal::new(5, 1)).unwrap();
        let shares = ShareAmount::from_whole(1).unwrap();
        let principal =
            CollateralAmount::from_decimal_exact(rust_decimal::Decimal::new(5, 1)).unwrap();
        let receipt = AppendReceipt {
            sequence: EventSeq(1),
            this_hash: blake3::Hash::from_bytes([1; 32]),
        };
        let account_state = LiveAccountStateAudit {
            observed_at: datetime!(2026-08-11 12:00 UTC),
            closed_only: false,
            geoblocked: false,
            selected_spender: "exchange".to_owned(),
            collateral_balance: CollateralAmount::from_whole(10).unwrap(),
            allowance: CollateralAmount::from_whole(10).unwrap(),
            reconciled_free_collateral: CollateralAmount::from_whole(10).unwrap(),
            schema_version: 1,
            parser_version: 1,
            evidence: Vec::new(),
            request_descriptor_hashes: Vec::new(),
            evidence_hashes: Vec::new(),
        };
        let prepared = PreparedPolymarketBuy {
            condition_id: condition_id.clone(),
            outcome_id: pe_core_types::OutcomeId(0),
            token_id: token_id.clone(),
            maker: "maker".to_owned(),
            signer: "signer".to_owned(),
            funder: "funder".to_owned(),
            verifying_contract: "exchange".to_owned(),
            spender: "exchange".to_owned(),
            exchange_domain_version: 2,
            neg_risk: false,
            side: "BUY".to_owned(),
            salt: "1".to_owned(),
            timestamp_ms: 1,
            expiration: "0".to_owned(),
            maker_collateral: principal,
            taker_shares: shares,
            limit_price: price,
            minimum_tick_size: Price::new(rust_decimal::Decimal::new(1, 2)).unwrap(),
            signature_type: 1,
            order_type: "FOK".to_owned(),
            post_only: false,
            defer_exec: false,
            metadata: "metadata".to_owned(),
            builder: "0".to_owned(),
            order_hash: format!("order-{dispatch}"),
            post_body_hash: "body".to_owned(),
            sdk_version: "fixture".to_owned(),
            sdk_archive_sha256: "fixture".to_owned(),
            metadata_hashes: Vec::new(),
            worst_case_debit: principal,
        };
        Box::new(LiveOrderPreparedAudit {
            identity: identity.clone(),
            frozen_binding: CredentialBindingIdentity {
                version: 1,
                key_id: "key".to_owned(),
            },
            economic: EconomicPrepared {
                version: ECONOMIC_PREPARED_VERSION,
                market: MarketSelection {
                    condition_id: condition_id.clone(),
                    outcome_index: 0,
                    token_id: token_id.clone(),
                    side: Side::Buy,
                    market_id: condition_id.0.clone(),
                },
                admission: LiveAdmissionArtifactAudit {
                    market: LiveMarketEvidenceAudit {
                        condition_id: condition_id.clone(),
                        ordered_outcome_token_ids: [
                            token_id.clone(),
                            PolymarketTokenId("no".to_owned()),
                        ],
                        neg_risk: false,
                        minimum_tick_size: prepared.minimum_tick_size,
                        minimum_order_size: shares,
                        observed_at_unix: 0,
                        schema_version: 1,
                        parser_version: 1,
                        freshness_window_secs: 60,
                    },
                    settlement: VenueSettlementRecord {
                        schema_version: 1,
                        condition_id,
                        status: pe_resolver_card::VenueResolutionStatus::Unresolved,
                        raw_evidence_hash: "settlement".to_owned(),
                        source_timestamp_unix: None,
                        observed_at_unix: 0,
                        parser_version: 1,
                        freshness_window_secs: 60,
                    },
                    fee_schedule: CompactFeeSchedule::Zero,
                    scheduled_end_unix: None,
                    receipts: AdmissionReceipts {
                        gamma: receipt,
                        clob_long: receipt,
                        clob_compact: receipt,
                    },
                },
                ladder: LadderPlanAudit {
                    used_asks: vec![LadderAskAudit { price, shares }],
                    best_ask: price,
                    limit_price: price,
                    minimum_shares: shares,
                    principal,
                },
                book_receipt: receipt,
                observation: None,
                sizing: SizingAudit {
                    mode: SizingModeAudit::Contract { contracts: 1 },
                    budget: CollateralAmount::from_whole(10).unwrap(),
                    principal,
                    minimum_shares: shares,
                    expected_shares: shares,
                    expected_vwap: price,
                    all_in_price: price,
                    slippage_rate: rust_decimal::Decimal::ZERO,
                },
                fee: FeeAudit {
                    schedule: CompactFeeSchedule::Zero,
                    expected_fee: CollateralAmount::ZERO,
                    reserve: CollateralAmount::ZERO,
                },
                risk: RiskAudit {
                    snapshot: RiskSnapshot {
                        leader_exposure_bps: BasisPoints::ZERO,
                        market_exposure_bps: BasisPoints::ZERO,
                        family_exposure_bps: BasisPoints::ZERO,
                        total_copy_exposure_bps: BasisPoints::ZERO,
                        intraday_pnl_bps: BasisPoints::ZERO,
                        rolling_7d_pnl_bps: BasisPoints::ZERO,
                        absolute_pnl_bps: BasisPoints::ZERO,
                        copy_latency_kill_switch_active: false,
                        proposed_trade_bps: BasisPoints::ZERO,
                        per_trade_cap_bps: 100,
                        concentration_caps: None,
                    },
                    decision: RiskDecisionAudit::Approved,
                    price_receipts: Vec::new(),
                    evaluated_at_unix_ms: 0,
                },
                balance: BalanceAudit {
                    cash_before: CollateralAmount::from_whole(10).unwrap(),
                    worst_case_debit: principal,
                    price_impact_cap_bps: 100,
                    chase_ceiling: price,
                    band_floor: Price::ZERO,
                    band_ceiling_exclusive: Price::ONE,
                },
                applied_configuration_hash: identity.config_hash.clone(),
            },
            account_state,
            prepared,
        })
    }

    #[test]
    fn open_order_inventory_is_cross_account_and_terminal_fact_strict() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let open_account = AccountId::new("open").unwrap();
        let killed_account = AccountId::new("killed").unwrap();
        let finalized_account = AccountId::new("finalized").unwrap();
        let at = datetime!(2026-08-11 12:00 UTC);
        let open = current_prepared("open");
        let killed = current_prepared("killed");
        let finalized = current_prepared("finalized");

        journal
            .append(
                open_account.clone(),
                at,
                LiveJournalPayload::OrderPrepared(open.clone()),
            )
            .unwrap();
        journal
            .append(
                killed_account.clone(),
                at,
                LiveJournalPayload::OrderPrepared(killed.clone()),
            )
            .unwrap();
        journal
            .append(
                killed_account,
                at,
                LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                    identity: killed.identity.clone(),
                    order_hash: killed.prepared.order_hash.clone(),
                    source: LiveReconciliationSource::OrderHashLookupAndCancel,
                    outcome: LiveJournalOrderOutcome::Killed {
                        venue_order_id: Some("venue-killed".to_owned()),
                    },
                    evidence: Vec::new(),
                    evidence_hashes: Vec::new(),
                })),
            )
            .unwrap();
        journal
            .append(
                finalized_account.clone(),
                at,
                LiveJournalPayload::OrderPrepared(finalized.clone()),
            )
            .unwrap();
        journal
            .append(
                finalized_account,
                at,
                LiveJournalPayload::OrderFillFinalized(Box::new(OrderFillFinalizedAudit {
                    identity: finalized.identity.clone(),
                    prepared_journal_seq: 3,
                    principal: finalized.economic.sizing.principal,
                    quantity: finalized.economic.sizing.minimum_shares,
                    fee: CollateralAmount::ZERO,
                    matched_logs: vec![MatchedLogIdentity {
                        transaction_hash: format!("0x{}", "1".repeat(64)),
                        log_index: 0,
                    }],
                    chain_id: pe_venue_polymarket::FINALIZED_CHAIN_ID,
                    finalized_head: 1,
                    receipts: Vec::new(),
                    blocks: Vec::new(),
                })),
            )
            .unwrap();
        journal
            .append(
                open_account.clone(),
                at,
                LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                    identity: open.identity.clone(),
                    order_hash: open.prepared.order_hash.clone(),
                    source: LiveReconciliationSource::PostResponse,
                    outcome: LiveJournalOrderOutcome::Matched {
                        venue_order_id: "venue-open".to_owned(),
                        transaction_hashes: vec![format!("0x{}", "2".repeat(64))],
                        executed: None,
                    },
                    evidence: Vec::new(),
                    evidence_hashes: Vec::new(),
                })),
            )
            .unwrap();
        drop(journal);

        assert_eq!(
            open_order_inventory(&path).unwrap(),
            vec![OpenOrderInventoryEntry {
                identity: open.identity.clone(),
                account_id: open_account,
                prepared_journal_seq: 0,
            }]
        );
        let recovery = recovery_inventory(&path).unwrap();
        assert_eq!(recovery.account_ids.len(), 3);
        assert_eq!(recovery.open_orders.len(), 1);
        assert_eq!(
            recovery.open_orders[0].prepared.as_deref(),
            Some(open.as_ref())
        );
        assert_eq!(
            recovery.open_orders[0].transaction_hashes,
            vec![format!("0x{}", "2".repeat(64))]
        );
    }

    /// PASS: recovery tolerates only a complete exact retry of Killed/Rejected and rejects every
    /// changed reconciliation after that terminal fact.
    #[test]
    fn recovery_terminal_reconciliation_retries_are_complete_fact_exact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let account_id = AccountId::new("account").unwrap();
        let at = datetime!(2026-08-11 12:00 UTC);
        let prepared = current_prepared("terminal-retry");
        let terminal = LiveOrderReconciliationAudit {
            identity: prepared.identity.clone(),
            order_hash: prepared.prepared.order_hash.clone(),
            source: LiveReconciliationSource::OrderHashLookupAndCancel,
            outcome: LiveJournalOrderOutcome::Killed {
                venue_order_id: Some("venue-order".to_owned()),
            },
            evidence: Vec::new(),
            evidence_hashes: Vec::new(),
        };
        journal
            .append(
                account_id.clone(),
                at,
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            )
            .unwrap();
        for audit in [terminal.clone(), terminal.clone()] {
            journal
                .append(
                    account_id.clone(),
                    at,
                    LiveJournalPayload::OrderReconciled(Box::new(audit)),
                )
                .unwrap();
        }
        assert!(recovery_inventory(&path).unwrap().open_orders.is_empty());

        let mut changed = terminal;
        changed.source = LiveReconciliationSource::PostResponse;
        changed.outcome = LiveJournalOrderOutcome::Matched {
            venue_order_id: "venue-order".to_owned(),
            transaction_hashes: vec![format!("0x{}", "2".repeat(64))],
            executed: None,
        };
        journal
            .append(
                account_id,
                at,
                LiveJournalPayload::OrderReconciled(Box::new(changed)),
            )
            .unwrap();
        drop(journal);
        assert!(matches!(
            recovery_inventory(&path),
            Err(LiveJournalError::OrderFactConflict)
        ));
    }

    #[test]
    fn open_order_inventory_rejects_a_fact_for_another_prepared_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let account_id = AccountId::new("account").unwrap();
        let at = datetime!(2026-08-11 12:00 UTC);
        let prepared = current_prepared("prepared");
        let wrong = current_prepared("wrong");
        journal
            .append(
                account_id.clone(),
                at,
                LiveJournalPayload::OrderPrepared(prepared),
            )
            .unwrap();
        journal
            .append(
                account_id,
                at,
                LiveJournalPayload::OrderPosted(Box::new(LiveOrderPostAudit {
                    identity: wrong.identity.clone(),
                    order_hash: wrong.prepared.order_hash.clone(),
                    evidence: RawHttpAttempt::TransportFailure(
                        pe_core_types::RawTransportFailure {
                            source_id: "polymarket-clob-v2".to_owned(),
                            endpoint_kind: "post".to_owned(),
                            method: "POST".to_owned(),
                            path: "/order".to_owned(),
                            ordered_query: Vec::new(),
                            attempt_ordinal: 1,
                            observed_at: at,
                            received_at: at,
                            error_class: pe_core_types::TransportErrorClass::Other,
                            schema_version: 1,
                            parser_version: 1,
                            adapter_version: "fixture".to_owned(),
                        },
                    ),
                    evidence_hash: "evidence".to_owned(),
                })),
            )
            .unwrap();
        drop(journal);

        assert!(matches!(
            open_order_inventory(&path),
            Err(LiveJournalError::OrderFactConflict)
        ));
    }

    #[test]
    fn account_filter_preserves_exact_global_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let first = AccountId::new("first").unwrap();
        let second = AccountId::new("second").unwrap();
        let at = datetime!(2026-08-11 12:00 UTC);
        let event0 = journal
            .append(
                first.clone(),
                at,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: identity("d0"),
                        failure: LiveOrderPreparationFailure::Venue,
                    },
                )),
            )
            .unwrap();
        journal
            .append(
                second.clone(),
                at,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: identity("d1"),
                        failure: LiveOrderPreparationFailure::Venue,
                    },
                )),
            )
            .unwrap();
        let event2 = journal
            .append(
                first.clone(),
                at,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: identity("d2"),
                        failure: LiveOrderPreparationFailure::PreparedAuditMismatch,
                    },
                )),
            )
            .unwrap();
        journal
            .append(
                second,
                at,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: identity("d3"),
                        failure: LiveOrderPreparationFailure::Venue,
                    },
                )),
            )
            .unwrap();
        drop(journal);

        assert_eq!(
            replay_account(&path, &first).unwrap(),
            vec![event0.clone(), event2.clone()]
        );
        let (_, account_tail_envelope) = Reader::replay(&path).unwrap().nth(2).unwrap().unwrap();
        let (_, scanned_prefix_envelope) = Reader::replay(&path).unwrap().nth(3).unwrap().unwrap();
        let (events, tail) = replay_account_with_tail(&path, &first).unwrap();
        assert_eq!(events, vec![event0, event2]);
        assert_eq!(tail.last_sequence, Some(EventSeq(2)));
        assert_eq!(tail.last_hash, account_tail_envelope.this_hash);
        assert_eq!(tail.scanned_prefix_last_sequence, Some(EventSeq(3)));
        assert_eq!(
            tail.scanned_prefix_last_hash,
            scanned_prefix_envelope.this_hash
        );
        verify_account_tail_checkpoint(&path, &first, tail).unwrap();
        assert_eq!(
            LiveJournal::verified_tail(&path).unwrap().last_sequence,
            Some(EventSeq(3))
        );
    }

    #[test]
    fn schema_one_changed_payload_replays_as_explicit_legacy_audit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-live.log");
        let account_id = AccountId::new("legacy").unwrap();
        let at = datetime!(2026-08-11 12:00 UTC);
        let legacy = legacy_v1::LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 0,
            timestamp: at,
            payload: legacy_v1::LiveJournalPayload::AdmissionEvaluated(Box::new(
                legacy_v1::LiveAdmissionEvaluationAudit {
                    identity: identity("legacy-dispatch"),
                    frozen_binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                    current_binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                    requested_mode: LiveControlMode::LiveTiny,
                    effective_mode: LiveControlMode::LiveTiny,
                    artifact: legacy_v1::LiveAdmissionArtifactAudit {
                        market: legacy_v1::LiveMarketEvidenceAudit {
                            condition_id: PolymarketConditionId("condition".to_owned()),
                            ordered_outcome_token_ids: [
                                PolymarketTokenId("yes".to_owned()),
                                PolymarketTokenId("no".to_owned()),
                            ],
                            neg_risk: false,
                            minimum_tick_size: Price::new(rust_decimal::Decimal::new(1, 2))
                                .unwrap(),
                            minimum_order_size: ShareAmount::from_whole(1).unwrap(),
                            fee_evidence: legacy_v1::LiveFeeEvidenceAudit {
                                gamma_fees_enabled: None,
                                gamma_fee_schedule: None,
                                gamma_maker_base_fee_bps: None,
                                gamma_taker_base_fee_bps: None,
                                clob_maker_base_fee_bps: None,
                                clob_taker_base_fee_bps: None,
                            },
                            raw_gamma_market_hash: "gamma".to_owned(),
                            raw_clob_market_hash: "clob".to_owned(),
                            observed_at_unix: at.unix_timestamp(),
                            schema_version: 1,
                            parser_version: 1,
                            freshness_window_secs: 60,
                        },
                        settlement: VenueSettlementRecord {
                            schema_version: 1,
                            condition_id: PolymarketConditionId("condition".to_owned()),
                            status: pe_resolver_card::VenueResolutionStatus::Unresolved,
                            raw_evidence_hash: "settlement".to_owned(),
                            source_timestamp_unix: Some(at.unix_timestamp()),
                            observed_at_unix: at.unix_timestamp(),
                            parser_version: 1,
                            freshness_window_secs: 60,
                        },
                        artifact_bundle_hash: "bundle".to_owned(),
                    },
                    ladder: legacy_v1::LadderPlanAudit {
                        used_asks: vec![LadderAskAudit {
                            price: Price::new(rust_decimal::Decimal::new(5, 1)).unwrap(),
                            shares: ShareAmount::from_whole(1).unwrap(),
                        }],
                        best_ask: Price::new(rust_decimal::Decimal::new(5, 1)).unwrap(),
                        limit_price: Price::new(rust_decimal::Decimal::new(5, 1)).unwrap(),
                        shares: ShareAmount::from_whole(1).unwrap(),
                        estimated_ladder_spend: CollateralAmount::from_decimal_exact(
                            rust_decimal::Decimal::new(5, 1),
                        )
                        .unwrap(),
                        worst_case_debit: CollateralAmount::from_decimal_exact(
                            rust_decimal::Decimal::new(5, 1),
                        )
                        .unwrap(),
                        plan_hash: "plan".to_owned(),
                    },
                    account_state: None,
                    account_read_failure_evidence: Vec::new(),
                    account_read_failure_evidence_hashes: Vec::new(),
                    verdict: LiveAdmissionVerdict::Approved,
                },
            )),
        };
        let mut writer = Writer::open(&path).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(LIVE_JOURNAL_SOURCE.to_owned()),
                schema_version: 1,
                parser_version: LIVE_JOURNAL_PARSER_VERSION,
                observed_at: SourceTimestamp(at),
                received_at: ReceivedAt(at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&legacy).unwrap(),
            })
            .unwrap();
        let (admission, ladder) = match &legacy.payload {
            legacy_v1::LiveJournalPayload::AdmissionEvaluated(audit) => {
                (audit.artifact.clone(), audit.ladder.clone())
            }
            _ => unreachable!(),
        };
        let amount =
            CollateralAmount::from_decimal_exact(rust_decimal::Decimal::new(5, 1)).unwrap();
        let shares = ShareAmount::from_whole(1).unwrap();
        let price = Price::new(rust_decimal::Decimal::new(5, 1)).unwrap();
        let prepared = legacy_v1::LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 1,
            timestamp: at,
            payload: legacy_v1::LiveJournalPayload::OrderPrepared(Box::new(
                legacy_v1::LiveOrderPreparedAudit {
                    identity: identity("legacy-prepared"),
                    frozen_binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                    admission,
                    account_state: legacy_v1::LiveAccountStateAudit {
                        observed_at: at,
                        closed_only: false,
                        geoblocked: false,
                        selected_spender: "spender".to_owned(),
                        collateral_balance: CollateralAmount::from_whole(10).unwrap(),
                        allowance: CollateralAmount::from_whole(10).unwrap(),
                        reconciled_free_collateral: CollateralAmount::from_whole(10).unwrap(),
                        schema_version: 1,
                        parser_version: 1,
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                    ladder,
                    prepared: PreparedPolymarketBuy {
                        condition_id: PolymarketConditionId("condition".to_owned()),
                        outcome_id: pe_core_types::OutcomeId(0),
                        token_id: PolymarketTokenId("yes".to_owned()),
                        maker: "maker".to_owned(),
                        signer: "signer".to_owned(),
                        funder: "funder".to_owned(),
                        verifying_contract: "exchange".to_owned(),
                        spender: "exchange".to_owned(),
                        exchange_domain_version: 2,
                        neg_risk: false,
                        side: "BUY".to_owned(),
                        salt: "1".to_owned(),
                        timestamp_ms: 1,
                        expiration: "0".to_owned(),
                        maker_collateral: amount,
                        taker_shares: shares,
                        limit_price: price,
                        minimum_tick_size: Price::new(rust_decimal::Decimal::new(1, 2)).unwrap(),
                        signature_type: 1,
                        order_type: "FOK".to_owned(),
                        post_only: false,
                        defer_exec: false,
                        metadata: "metadata".to_owned(),
                        builder: "0".to_owned(),
                        order_hash: "order".to_owned(),
                        post_body_hash: "body".to_owned(),
                        sdk_version: "fixture".to_owned(),
                        sdk_archive_sha256: "fixture".to_owned(),
                        metadata_hashes: Vec::new(),
                        worst_case_debit: amount,
                    },
                    prepared_audit_hash: "legacy-prepared-audit".to_owned(),
                },
            )),
        };
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(LIVE_JOURNAL_SOURCE.to_owned()),
                schema_version: 1,
                parser_version: LIVE_JOURNAL_PARSER_VERSION,
                observed_at: SourceTimestamp(at),
                received_at: ReceivedAt(at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&prepared).unwrap(),
            })
            .unwrap();
        let reconciliation = legacy_v1::LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 2,
            timestamp: at,
            payload: legacy_v1::LiveJournalPayload::OrderReconciled(Box::new(
                LiveOrderReconciliationAudit {
                    identity: identity("legacy-prepared"),
                    order_hash: "order".to_owned(),
                    source: LiveReconciliationSource::OrderHashLookupAndCancel,
                    outcome: LiveJournalOrderOutcome::Matched {
                        venue_order_id: "venue-order".to_owned(),
                        transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
                        executed: None,
                    },
                    evidence: Vec::new(),
                    evidence_hashes: Vec::new(),
                },
            )),
        };
        let redemption_identity = RedemptionAttemptIdentity {
            account_id: account_id.clone(),
            condition_id: PolymarketConditionId("condition".to_owned()),
            adapter: "adapter".to_owned(),
            custody_wallet: "custody".to_owned(),
        };
        let legacy_redemption = [
            legacy_v1::LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: at,
                payload: legacy_v1::LiveJournalPayload::RedemptionRequested(Box::new(
                    RedemptionRequestedAudit {
                        identity: redemption_identity.clone(),
                        attempt_count: 1,
                        redeemable_balance: amount,
                        request: RedemptionRequestAudit {
                            call_to: "adapter".to_owned(),
                            calldata: vec![1],
                            condition_id: PolymarketConditionId("condition".to_owned()),
                            neg_risk: false,
                            custody: RedemptionCustodyAudit::DepositWallet,
                            signer_address: "signer".to_owned(),
                            custody_wallet: "custody".to_owned(),
                            deadline_unix: Some(1),
                            metadata_hash: "metadata".to_owned(),
                            request_hash: "request".to_owned(),
                            schema_version: 1,
                            parser_version: 1,
                            adapter_version: "adapter-v1".to_owned(),
                        },
                    },
                )),
            },
            legacy_v1::LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 4,
                timestamp: at,
                payload: legacy_v1::LiveJournalPayload::RedemptionTransactionIdentified(Box::new(
                    RedemptionTransactionAudit {
                        identity: redemption_identity.clone(),
                        attempt_count: 1,
                        transaction_id: "transaction".to_owned(),
                        submit_body_hash: "body".to_owned(),
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                )),
            },
            legacy_v1::LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 5,
                timestamp: at,
                payload: legacy_v1::LiveJournalPayload::RedemptionReceiptTransition(Box::new(
                    RedemptionReceiptAudit {
                        identity: redemption_identity,
                        attempt_count: 1,
                        transaction_id: "transaction".to_owned(),
                        transaction_hash: Some(format!("0x{}", "22".repeat(32))),
                        status: RedemptionReceiptStatusAudit::Confirmed,
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                )),
            },
        ];
        for event in std::iter::once(reconciliation).chain(legacy_redemption) {
            writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(LIVE_JOURNAL_SOURCE.to_owned()),
                    schema_version: 1,
                    parser_version: LIVE_JOURNAL_PARSER_VERSION,
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    content_type: ContentType::Json,
                    payload: serde_json::to_vec(&event).unwrap(),
                })
                .unwrap();
        }
        drop(writer);

        let replayed = replay_account(&path, &account_id).unwrap();
        assert_eq!(replayed.len(), 6);
        assert!(
            replayed[..2]
                .iter()
                .all(|event| matches!(event.payload, LiveJournalPayload::LegacyV1(_)))
        );
        assert!(matches!(
            &replayed[2].payload,
            LiveJournalPayload::OrderReconciled(audit)
                if matches!(audit.outcome, LiveJournalOrderOutcome::Matched { .. })
        ));
        assert!(matches!(
            replayed[3].payload,
            LiveJournalPayload::RedemptionRequested(_)
        ));
        assert!(matches!(
            replayed[4].payload,
            LiveJournalPayload::RedemptionTransactionIdentified(_)
        ));
        assert!(matches!(
            replayed[5].payload,
            LiveJournalPayload::RedemptionReceiptTransition(_)
        ));
        assert_eq!(
            open_order_inventory(&path).unwrap(),
            vec![OpenOrderInventoryEntry {
                identity: identity("legacy-prepared"),
                account_id,
                prepared_journal_seq: 1,
            }]
        );
    }

    #[test]
    fn ladder_audit_derives_exact_totals_and_vwap() {
        let first_price = Price::new(rust_decimal::Decimal::new(4, 1)).unwrap();
        let second_price = Price::new(rust_decimal::Decimal::new(6, 1)).unwrap();
        let audit = LadderPlanAudit {
            used_asks: vec![
                LadderAskAudit {
                    price: first_price,
                    shares: ShareAmount::from_whole(1).unwrap(),
                },
                LadderAskAudit {
                    price: second_price,
                    shares: ShareAmount::from_whole(1).unwrap(),
                },
            ],
            best_ask: first_price,
            limit_price: second_price,
            minimum_shares: ShareAmount::from_whole(2).unwrap(),
            principal: CollateralAmount::from_decimal_exact(rust_decimal::Decimal::new(12, 1))
                .unwrap(),
        };
        assert_eq!(
            audit.expected_shares().unwrap(),
            ShareAmount::from_whole(2).unwrap()
        );
        assert_eq!(
            audit.expected_spend().unwrap(),
            CollateralAmount::from_whole(1).unwrap()
        );
        assert_eq!(
            audit.expected_vwap(),
            Some(Price::new(rust_decimal::Decimal::new(5, 1)).unwrap())
        );
    }

    #[test]
    fn new_journal_is_exactly_mode_0600() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        drop(journal);
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn native_verified_tail_reports_exact_physical_binding() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live_journal.log");
        let journal = LiveJournal::open(&path).unwrap();
        journal
            .append(
                AccountId::new("first").unwrap(),
                datetime!(2026-08-11 12:00 UTC),
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: identity("d0"),
                        failure: LiveOrderPreparationFailure::Venue,
                    },
                )),
            )
            .unwrap();
        drop(journal);

        let binding = LiveJournal::verified_tail(&path).unwrap();
        assert_eq!(binding.path, fs::canonicalize(&path).unwrap());
        assert_eq!(binding.physical_tail, fs::metadata(&path).unwrap().len());
        assert_eq!(binding.last_sequence.map(|sequence| sequence.0), Some(0));
    }

    #[test]
    fn native_live_record_rejects_every_truncation_and_open_repairs_only_frame_tail() {
        let dir = tempdir().unwrap();
        let canonical = dir.path().join("canonical-live.log");
        let journal = LiveJournal::open(&canonical).unwrap();
        journal
            .append(
                AccountId::new("first").unwrap(),
                datetime!(2026-08-11 12:00 UTC),
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: identity("d0"),
                        failure: LiveOrderPreparationFailure::Venue,
                    },
                )),
            )
            .unwrap();
        drop(journal);
        let bytes = fs::read(&canonical).unwrap();

        for cut in 1..bytes.len() {
            let path = dir.path().join(format!("live-cut-{cut}.log"));
            fs::write(&path, &bytes[..cut]).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            if cut < 5 {
                assert!(LiveJournal::verified_tail(&path).is_err());
                assert!(LiveJournal::open(&path).is_err());
            } else if cut == 5 {
                assert!(LiveJournal::verified_tail(&path).is_ok());
                drop(LiveJournal::open(&path).unwrap());
            } else {
                assert!(LiveJournal::verified_tail(&path).is_err());
                drop(LiveJournal::open(&path).unwrap());
                assert_eq!(fs::read(&path).unwrap(), b"EDGE\x01");
            }
        }
    }
}
