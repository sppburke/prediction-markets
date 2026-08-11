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
use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
use pe_resolver_card::VenueSettlementRecord;
use pe_source_polymarket_public::LiveMarketEvidence;
use pe_venue_polymarket::{LadderPlan, PreparedPolymarketBuy};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

const LIVE_JOURNAL_SOURCE: &str = "ordinary-live-execution";
const LIVE_JOURNAL_SCHEMA_VERSION: u32 = 1;
const LIVE_JOURNAL_PARSER_VERSION: u32 = 1;

/// A frozen credential identity. It deliberately cannot carry decrypted credential material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialBindingIdentity {
    pub version: i64,
    pub key_id: String,
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

/// Complete fee fields retained by [`LiveMarketEvidence`]. They are audit inputs, not secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFeeEvidenceAudit {
    pub gamma_fees_enabled: Option<serde_json::Value>,
    pub gamma_fee_schedule: Option<serde_json::Value>,
    pub gamma_maker_base_fee_bps: Option<serde_json::Value>,
    pub gamma_taker_base_fee_bps: Option<serde_json::Value>,
    pub clob_maker_base_fee_bps: Option<serde_json::Value>,
    pub clob_taker_base_fee_bps: Option<serde_json::Value>,
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
    pub fee_evidence: LiveFeeEvidenceAudit,
    pub raw_gamma_market_hash: String,
    pub raw_clob_market_hash: String,
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
            fee_evidence: LiveFeeEvidenceAudit {
                gamma_fees_enabled: value.fee_evidence.gamma_fees_enabled.clone(),
                gamma_fee_schedule: value.fee_evidence.gamma_fee_schedule.clone(),
                gamma_maker_base_fee_bps: value.fee_evidence.gamma_maker_base_fee_bps.clone(),
                gamma_taker_base_fee_bps: value.fee_evidence.gamma_taker_base_fee_bps.clone(),
                clob_maker_base_fee_bps: value.fee_evidence.clob_maker_base_fee_bps.clone(),
                clob_taker_base_fee_bps: value.fee_evidence.clob_taker_base_fee_bps.clone(),
            },
            raw_gamma_market_hash: value.raw_gamma_market_hash.to_hex().to_string(),
            raw_clob_market_hash: value.raw_clob_market_hash.to_hex().to_string(),
            observed_at_unix: value.observed_at_unix,
            schema_version: value.schema_version,
            parser_version: value.parser_version,
            freshness_window_secs: value.freshness_window_secs,
        }
    }
}

/// Both halves of ordinary live admission, plus a stable commitment to their full typed payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveAdmissionArtifactAudit {
    pub market: LiveMarketEvidenceAudit,
    pub settlement: VenueSettlementRecord,
    pub artifact_bundle_hash: String,
}

impl LiveAdmissionArtifactAudit {
    pub fn new(
        market: &LiveMarketEvidence,
        settlement: &VenueSettlementRecord,
    ) -> Result<Self, LiveJournalError> {
        let market = LiveMarketEvidenceAudit::from(market);
        let bundle_hash = hash_serializable(&(
            "prediction-edge/live-admission-artifact/v1",
            &market,
            settlement,
        ))?;
        Ok(Self {
            market,
            settlement: settlement.clone(),
            artifact_bundle_hash: bundle_hash,
        })
    }
}

/// Serializable projection of every used ask and exact financial field in a ladder plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LadderPlanAudit {
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
pub struct LadderAskAudit {
    pub price: Price,
    pub shares: ShareAmount,
}

impl LadderPlanAudit {
    pub fn new(plan: &LadderPlan) -> Result<Self, LiveJournalError> {
        let used_asks = plan
            .used_asks
            .iter()
            .map(|ask| LadderAskAudit {
                price: ask.price,
                shares: ask.shares,
            })
            .collect::<Vec<_>>();
        let plan_hash = hash_serializable(&(
            "prediction-edge/live-ladder-plan/v1",
            &used_asks,
            plan.best_ask,
            plan.limit_price,
            plan.shares,
            plan.estimated_ladder_spend,
            plan.worst_case_debit,
        ))?;
        Ok(Self {
            used_asks,
            best_ask: plan.best_ask,
            limit_price: plan.limit_price,
            shares: plan.shares,
            estimated_ladder_spend: plan.estimated_ladder_spend,
            worst_case_debit: plan.worst_case_debit,
            plan_hash,
        })
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
    pub artifact: LiveAdmissionArtifactAudit,
    pub ladder: LadderPlanAudit,
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
    pub admission: LiveAdmissionArtifactAudit,
    pub account_state: LiveAccountStateAudit,
    pub ladder: LadderPlanAudit,
    pub prepared: PreparedPolymarketBuy,
    pub prepared_audit_hash: String,
}

impl LiveOrderPreparedAudit {
    pub fn new(
        identity: LiveOrderIdentity,
        frozen_binding: CredentialBindingIdentity,
        admission: LiveAdmissionArtifactAudit,
        account_state: LiveAccountStateAudit,
        ladder: LadderPlanAudit,
        prepared: PreparedPolymarketBuy,
    ) -> Result<Self, LiveJournalError> {
        let prepared_audit_hash = hash_serializable(&(
            "prediction-edge/prepared-live-order/v1",
            &identity,
            &frozen_binding,
            &admission,
            &account_state,
            &ladder,
            &prepared,
        ))?;
        Ok(Self {
            identity,
            frozen_binding,
            admission,
            account_state,
            ladder,
            prepared,
            prepared_audit_hash,
        })
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome", content = "detail")]
pub enum LiveJournalOrderOutcome {
    Matched {
        venue_order_id: String,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveReconciliationSource {
    PostResponse,
    OrderHashLookupAndCancel,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    CredentialBindingMismatch {
        frozen: CredentialBindingIdentity,
        current: CredentialBindingIdentity,
    },
    ModeTransitionApplied(LiveModeTransitionAudit),
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
    #[error("live journal writer is unavailable after a durability failure")]
    DurabilityFailed,
    #[error("live journal mutex is poisoned")]
    Poisoned,
}

struct LiveJournalInner {
    writer: Writer,
    next_seq: u64,
    durability_failed: bool,
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
            inner: Mutex::new(LiveJournalInner {
                writer,
                next_seq,
                durability_failed: false,
            }),
        })
    }

    /// Append and fsync one event. The supplied timestamp is used for both payload and envelope.
    pub fn append(
        &self,
        account_id: AccountId,
        timestamp: OffsetDateTime,
        payload: LiveJournalPayload,
    ) -> Result<LiveJournalEvent, LiveJournalError> {
        let mut inner = self.inner.lock().map_err(|_| LiveJournalError::Poisoned)?;
        if inner.durability_failed {
            return Err(LiveJournalError::DurabilityFailed);
        }
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
            inner.durability_failed = true;
            return Err(LiveJournalError::SequenceMismatch);
        }
        if let Err(error) = inner.writer.sync() {
            inner.durability_failed = true;
            return Err(error.into());
        }
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

fn replay_all(path: impl AsRef<Path>) -> Result<Vec<LiveJournalEvent>, LiveJournalError> {
    let mut events = Vec::new();
    for item in Reader::replay(path)? {
        let (seq, envelope) = item?;
        if envelope.source_id != SourceId(LIVE_JOURNAL_SOURCE.to_owned())
            || envelope.schema_version != LIVE_JOURNAL_SCHEMA_VERSION
            || envelope.parser_version != LIVE_JOURNAL_PARSER_VERSION
            || envelope.content_type != ContentType::Json
        {
            return Err(LiveJournalError::UnexpectedEnvelope);
        }
        let event: LiveJournalEvent = serde_json::from_slice(&envelope.payload)?;
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
}
