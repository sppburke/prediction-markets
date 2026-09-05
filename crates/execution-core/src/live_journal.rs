//! Account-tagged append-only journal for ordinary live execution.
//!
//! One service-owned instance serializes every account into one hash-chained stream. Per-account
//! ledgers are projections of that stream, preserving the global sequence assigned at append time.

use std::fs;
use std::io;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::Path;
use std::sync::Mutex;

use pe_core_types::{
    AccountId, CollateralAmount, PolymarketConditionId, PolymarketTokenId, Price, RawHttpAttempt,
    ReceivedAt, ShareAmount, SourceId, SourceTimestamp,
};
use pe_event_log::{ContentType, EnvelopeIn, LogTailBinding, PoisonReason, Reader, Writer};
use pe_resolver_card::VenueSettlementRecord;
use pe_source_polymarket_public::LiveMarketEvidence;
use pe_venue_polymarket::{CompactFeeSchedule, LadderPlan, PreparedPolymarketBuy};
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
        CollateralAmount::from_decimal_exact(spend)
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
    pub account_state: LiveAccountStateAudit,
    pub venue_positions: Vec<CanonicalPositionAudit>,
    pub venue_position_receipts: Vec<pe_event_log::AppendReceipt>,
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

mod legacy_v1 {
    use super::*;

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
        events.push(event);
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

pub(crate) fn http_attempt_hash(attempt: &RawHttpAttempt) -> Result<String, LiveJournalError> {
    hash_serializable(&("prediction-edge/live-http-attempt/v1", attempt))
}

pub(crate) fn http_attempt_hashes(
    attempts: &[RawHttpAttempt],
) -> Result<Vec<String>, LiveJournalError> {
    attempts.iter().map(http_attempt_hash).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::os::unix::fs::PermissionsExt as _;

    use tempfile::tempdir;
    use time::macros::datetime;

    use super::*;

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
                second,
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
        drop(journal);

        assert_eq!(replay_account(&path, &first).unwrap(), vec![event0, event2]);
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
        drop(writer);

        let replayed = replay_account(&path, &account_id).unwrap();
        assert_eq!(replayed.len(), 1);
        assert!(matches!(
            replayed[0].payload,
            LiveJournalPayload::LegacyV1(_)
        ));
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
