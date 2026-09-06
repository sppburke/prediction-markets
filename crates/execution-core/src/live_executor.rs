//! Ordinary per-account live execution with a durable prepare-before-POST boundary.

use std::future::Future;
use std::pin::Pin;

use pe_core_types::{
    AccountId, CollateralAmount, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price,
    RawHttpAttempt, RawHttpResponse, RawTransportFailure, ShareAmount, TransportErrorClass,
};
use pe_resolver_card::{VenueSettlementError, VenueSettlementRecord};
use pe_source_polymarket_public::{
    LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, LiveMarketError, LiveMarketEvidence,
};
use pe_venue_polymarket::{CompactFeeSchedule, LadderPlan, PreparedPolymarketBuy};
use rust_decimal::Decimal;
use serde_json::Value;
use time::OffsetDateTime;

use crate::live_journal::{
    AdmissionReceipts, CredentialBindingIdentity, LadderPlanAudit, LiveAccountBindingAudit,
    LiveAccountReadFailure, LiveAccountStateAudit, LiveAdmissionArtifactAudit,
    LiveAdmissionEvaluationAudit, LiveAdmissionRefusal, LiveAdmissionVerdict, LiveControlMode,
    LiveExecutedAmounts, LiveJournal, LiveJournalError, LiveJournalOrderOutcome,
    LiveJournalPayload, LiveOrderAmbiguityKind, LiveOrderIdentity, LiveOrderPostAudit,
    LiveOrderPreparationFailedAudit, LiveOrderPreparationFailure, LiveOrderPreparedAudit,
    LiveOrderReconciliationAudit, LiveOrderRejectKind, LiveReconciliationSource, http_attempt_hash,
    http_attempt_hashes, verify_http_response_request,
};

/// Frozen per-target account and credential identity supplied by the dispatch aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenLiveTarget {
    pub account_id: AccountId,
    pub credential_binding: CredentialBindingIdentity,
}

/// Current requested/effective control-plane state. Both values must be `live_tiny`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveModeSnapshot {
    pub requested: LiveControlMode,
    pub effective: LiveControlMode,
}

/// Account-read input to the pure ordered live-admission classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveAdmissionAccountEvidence<'a> {
    /// The ordered pre-I/O gates are being evaluated before an account read is attempted.
    NotRead,
    /// A complete authenticated account state was reconstructed from retained evidence.
    State(&'a LiveAccountStateAudit),
    /// Retained evidence deterministically classified as this account-read failure.
    ReadFailure(LiveAccountReadFailure),
}

/// All immutable inputs to the one ordered live-admission classifier.
pub struct LiveAdmissionClassificationInput<'a> {
    pub evaluated_at: OffsetDateTime,
    pub requested_mode: LiveControlMode,
    pub effective_mode: LiveControlMode,
    pub frozen_binding: &'a CredentialBindingIdentity,
    pub current_binding: &'a CredentialBindingIdentity,
    pub identity: &'a LiveOrderIdentity,
    pub condition_id: &'a PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: &'a PolymarketTokenId,
    pub admission: &'a LiveAdmissionArtifactAudit,
    pub ladder: &'a LadderPlanAudit,
    pub economic: &'a crate::economic::EconomicPrepared,
    pub account: LiveAdmissionAccountEvidence<'a>,
}

/// Signals that the ordered pre-I/O gates passed and the classifier requires account evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("live admission requires authenticated account evidence")]
pub struct LiveAdmissionNeedsAccountState;

impl LiveModeSnapshot {
    #[must_use]
    pub const fn is_armed(self) -> bool {
        matches!(self.requested, LiveControlMode::LiveTiny)
            && matches!(self.effective, LiveControlMode::LiveTiny)
    }
}

/// Frozen ordinary live admission evidence composed by the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAdmissionArtifact {
    pub market: LiveMarketEvidence,
    pub settlement: VenueSettlementRecord,
    pub fee_schedule: CompactFeeSchedule,
    pub receipts: AdmissionReceipts,
}

/// One complete frozen order target presented to [`LiveExecutor::prepare_with_clock`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveOrderRequest {
    pub target: FrozenLiveTarget,
    pub current_credential_binding: CredentialBindingIdentity,
    pub mode: LiveModeSnapshot,
    pub identity: LiveOrderIdentity,
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub admission: LiveAdmissionArtifact,
    pub ladder: LadderPlan,
    pub economic: crate::economic::EconomicPrepared,
}

/// Venue-neutral negRisk-aware preparation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenuePrepareRequest {
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub neg_risk: bool,
    pub limit_price: Price,
    pub shares: ShareAmount,
    pub maximum_collateral: CollateralAmount,
    pub tick_size: Price,
    pub metadata_hashes: Vec<String>,
}

/// Opaque POST capability paired with the complete sanitized prepared-order audit record.
pub struct LiveVenuePrepared<S> {
    audit: PreparedPolymarketBuy,
    submission: S,
}

impl<S> LiveVenuePrepared<S> {
    #[must_use]
    pub fn new(audit: PreparedPolymarketBuy, submission: S) -> Self {
        Self { audit, submission }
    }

    #[must_use]
    pub fn audit(&self) -> &PreparedPolymarketBuy {
        &self.audit
    }

    fn into_parts(self) -> (PreparedPolymarketBuy, S) {
        (self.audit, self.submission)
    }
}

/// Fresh authenticated venue state for the negRisk-selected spender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueAccountState {
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
    /// Descriptor hashes in exact attempt order, including transport failures.
    pub request_descriptor_hashes: Vec<String>,
}

impl LiveVenueAccountState {
    /// Project the authenticated response into the durable, credential-free journal shape.
    pub fn audit(&self) -> Result<LiveAccountStateAudit, LiveJournalError> {
        Ok(LiveAccountStateAudit {
            observed_at: self.observed_at,
            closed_only: self.closed_only,
            geoblocked: self.geoblocked,
            selected_spender: self.selected_spender.clone(),
            collateral_balance: self.collateral_balance,
            allowance: self.allowance,
            reconciled_free_collateral: self.reconciled_free_collateral,
            schema_version: self.schema_version,
            parser_version: self.parser_version,
            evidence: self.evidence.clone(),
            request_descriptor_hashes: self.request_descriptor_hashes.clone(),
            evidence_hashes: http_attempt_hashes(&self.evidence)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueAccountReadError {
    pub kind: LiveAccountReadFailure,
    pub evidence: Vec<RawHttpAttempt>,
    /// Descriptor hashes in exact attempt order for attempts retained by the failed read.
    pub request_descriptor_hashes: Vec<String>,
}

/// Canonical classification of one descriptor-bound raw account read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveAccountResponseClassification {
    State(LiveVenueAccountState),
    ReadFailure(LiveVenueAccountReadError),
}

/// Verify and classify retained account responses at the shared runtime/replay boundary.
///
/// Runtime callers omit `evidence_hashes`; durable replay callers supply the retained hashes.
/// Empty evidence and descriptor/hash mismatches are invalid retained evidence, distinct from a
/// response-derived transport, authentication, or protocol failure.
pub fn classify_live_account_responses(
    evidence: &[RawHttpAttempt],
    selected_spender: &str,
    request_descriptor_hashes: &[String],
    evidence_hashes: Option<&[String]>,
    binding: &LiveAccountBindingAudit,
) -> Result<LiveAccountResponseClassification, LiveJournalError> {
    if evidence.is_empty()
        || !binding.is_valid_for(&binding.account_id)
        || evidence.len() != request_descriptor_hashes.len()
    {
        return Err(LiveJournalError::RequestBinding);
    }
    for (attempt, retained_hash) in evidence.iter().zip(request_descriptor_hashes) {
        verify_http_response_request(
            &binding.request_descriptor_for_attempt(attempt),
            retained_hash,
        )?;
    }
    if let Some(retained) = evidence_hashes
        && http_attempt_hashes(evidence)? != retained
    {
        return Err(LiveJournalError::RequestBinding);
    }

    Ok(
        match classify_raw_account_responses(evidence, selected_spender, request_descriptor_hashes)
        {
            Ok(state) => LiveAccountResponseClassification::State(state),
            Err(error) => LiveAccountResponseClassification::ReadFailure(error),
        },
    )
}

/// Failure from replaying the complete durable admission evaluation.
#[derive(Debug, thiserror::Error)]
pub enum LiveAdmissionEvaluationError {
    #[error("recorded risk decision disagrees with deterministic risk evaluation")]
    InvalidRiskDecision,
    #[error("recorded admission account evidence or verdict is inconsistent")]
    InvalidAccountEvidence,
    #[error(transparent)]
    Journal(#[from] LiveJournalError),
}

/// Re-evaluate one durable admission from its retained economics and account evidence.
///
/// This is the sole outer admission verifier used by journal recovery and strict service
/// reduction. It performs no I/O and does not trust the producer-recorded verdict.
pub fn verify_live_admission_evaluation(
    admission: &LiveAdmissionEvaluationAudit,
    evaluated_at: OffsetDateTime,
    binding: Option<&LiveAccountBindingAudit>,
) -> Result<(), LiveAdmissionEvaluationError> {
    let binding = binding.ok_or(LiveAdmissionEvaluationError::InvalidAccountEvidence)?;
    if admission.identity.idempotency_key
        != LiveOrderIdentity::idempotency_key_for(
            &admission.identity.dispatch_id,
            &binding.account_id,
        )
    {
        return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
    }
    let risk_decision = match pe_risk_engine::evaluate_risk(&admission.economic.risk.snapshot) {
        pe_risk_engine::RiskDecision::Approved => crate::RiskDecisionAudit::Approved,
        pe_risk_engine::RiskDecision::Blocked(reason) => {
            crate::RiskDecisionAudit::Blocked { reason }
        }
    };
    if admission.economic.risk.decision != risk_decision
        || (admission.verdict == LiveAdmissionVerdict::Approved
            && risk_decision != crate::RiskDecisionAudit::Approved)
    {
        return Err(LiveAdmissionEvaluationError::InvalidRiskDecision);
    }

    let classify = |account| {
        classify_live_admission(LiveAdmissionClassificationInput {
            evaluated_at,
            requested_mode: admission.requested_mode,
            effective_mode: admission.effective_mode,
            frozen_binding: &admission.frozen_binding,
            current_binding: &admission.current_binding,
            identity: &admission.identity,
            condition_id: &admission.economic.market.condition_id,
            outcome_id: OutcomeId(u16::from(admission.economic.market.outcome_index)),
            token_id: &admission.economic.market.token_id,
            admission: &admission.economic.admission,
            ladder: &admission.economic.ladder,
            economic: &admission.economic,
            account,
        })
    };
    let has_failure_evidence = !admission.account_read_failure_evidence.is_empty()
        || !admission
            .account_read_failure_request_descriptor_hashes
            .is_empty()
        || !admission.account_read_failure_evidence_hashes.is_empty();
    if let Ok(expected) = classify(LiveAdmissionAccountEvidence::NotRead) {
        if admission.account_state.is_some()
            || has_failure_evidence
            || admission.verdict != expected
        {
            return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
        }
        return Ok(());
    }

    if !binding.is_valid_for_frozen_credential(&binding.account_id, &admission.frozen_binding)
        || admission.current_binding != binding.credential
        || (admission.account_state.is_some() == has_failure_evidence)
    {
        return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
    }
    let selected_spender = if admission.economic.admission.market.neg_risk {
        pe_venue_polymarket::CanaryV2Client::negrisk_spender()
    } else {
        pe_venue_polymarket::CanaryV2Client::standard_spender()
    }
    .map_err(|_| LiveAdmissionEvaluationError::InvalidAccountEvidence)?;
    let expected = if let Some(account) = &admission.account_state {
        let classified = classify_live_account_responses(
            &account.evidence,
            &selected_spender,
            &account.request_descriptor_hashes,
            Some(&account.evidence_hashes),
            binding,
        )?;
        let LiveAccountResponseClassification::State(state) = classified else {
            return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
        };
        let derived = state.audit()?;
        if *account != derived {
            return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
        }
        classify(LiveAdmissionAccountEvidence::State(&derived))
    } else {
        let classified = classify_live_account_responses(
            &admission.account_read_failure_evidence,
            &selected_spender,
            &admission.account_read_failure_request_descriptor_hashes,
            Some(&admission.account_read_failure_evidence_hashes),
            binding,
        )?;
        let LiveAccountResponseClassification::ReadFailure(failure) = classified else {
            return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
        };
        classify(LiveAdmissionAccountEvidence::ReadFailure(failure.kind))
    };
    if expected.map_err(|_| LiveAdmissionEvaluationError::InvalidAccountEvidence)?
        != admission.verdict
    {
        return Err(LiveAdmissionEvaluationError::InvalidAccountEvidence);
    }
    Ok(())
}

fn classify_raw_account_responses(
    evidence: &[RawHttpAttempt],
    selected_spender: &str,
    request_descriptor_hashes: &[String],
) -> Result<LiveVenueAccountState, LiveVenueAccountReadError> {
    let account_error = |kind| LiveVenueAccountReadError {
        kind,
        evidence: evidence.to_vec(),
        request_descriptor_hashes: request_descriptor_hashes.to_vec(),
    };
    if evidence
        .iter()
        .any(|item| matches!(item, RawHttpAttempt::TransportFailure(_)))
    {
        return Err(account_error(LiveAccountReadFailure::Transport));
    }
    if evidence.len() != 3
        || !is_known_account_spender(selected_spender)
        || ["geoblock", "closed-only", "balance-allowance"]
            .into_iter()
            .any(|kind| account_response_by_kind(evidence, kind).is_none())
    {
        return Err(account_error(LiveAccountReadFailure::Protocol));
    }
    let response = |kind: &str| {
        account_response_by_kind(evidence, kind)
            .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))
    };
    let geoblock = response("geoblock")?;
    let closed_only_response = response("closed-only")?;
    let balance = response("balance-allowance")?;
    if [geoblock, closed_only_response, balance]
        .iter()
        .any(|response| response.status == 401 || response.status == 403)
    {
        return Err(account_error(LiveAccountReadFailure::Authentication));
    }
    if !valid_account_response(geoblock, "geoblock", "/api/geoblock", &[])
        || !valid_account_response(
            closed_only_response,
            "closed-only",
            "/auth/ban-status/closed-only",
            &[],
        )
        || !valid_account_response(
            balance,
            "balance-allowance",
            "/balance-allowance",
            &[("asset_type", "COLLATERAL"), ("signature_type", "3")],
        )
    {
        return Err(account_error(LiveAccountReadFailure::Protocol));
    }
    let geoblock_json = account_response_json(geoblock)
        .map_err(|()| account_error(LiveAccountReadFailure::Protocol))?;
    let closed_json = account_response_json(closed_only_response)
        .map_err(|()| account_error(LiveAccountReadFailure::Protocol))?;
    let balance_json = account_response_json(balance)
        .map_err(|()| account_error(LiveAccountReadFailure::Protocol))?;
    let blocked = geoblock_json
        .get("blocked")
        .and_then(Value::as_bool)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let country = geoblock_json
        .get("country")
        .and_then(Value::as_str)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let geoblocked = blocked && !matches!(country, "IE" | "JP" | "MT" | "NL");
    let closed_only = closed_json
        .get("closed_only")
        .and_then(Value::as_bool)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let collateral_balance = account_atomic_amount(balance_json.get("balance"))
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let allowances = balance_json
        .get("allowances")
        .and_then(Value::as_object)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let allowance = match allowances
        .iter()
        .find(|(spender, _)| spender.eq_ignore_ascii_case(selected_spender))
    {
        Some((_, value)) => account_atomic_amount(Some(value))
            .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?,
        None => CollateralAmount::ZERO,
    };
    let observed_at = [geoblock, closed_only_response, balance]
        .iter()
        .map(|response| response.observed_at)
        .min()
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    Ok(LiveVenueAccountState {
        observed_at,
        closed_only,
        geoblocked,
        selected_spender: selected_spender.to_owned(),
        collateral_balance,
        allowance,
        // The ordinary service has no separate local reservation owner yet; the durable
        // dispatch reservation prevents another account/order from overtaking this read.
        reconciled_free_collateral: collateral_balance,
        schema_version: 1,
        parser_version: 1,
        evidence: evidence.to_vec(),
        request_descriptor_hashes: request_descriptor_hashes.to_vec(),
    })
}

fn account_response_by_kind<'a>(
    evidence: &'a [RawHttpAttempt],
    endpoint_kind: &str,
) -> Option<&'a RawHttpResponse> {
    let mut matches = evidence.iter().filter_map(|attempt| match attempt {
        RawHttpAttempt::Response(response) if response.endpoint_kind == endpoint_kind => {
            Some(response)
        }
        RawHttpAttempt::Response(_) | RawHttpAttempt::TransportFailure(_) => None,
    });
    let response = matches.next()?;
    matches.next().is_none().then_some(response)
}

fn is_known_account_spender(selected_spender: &str) -> bool {
    [
        pe_venue_polymarket::CanaryV2Client::standard_spender(),
        pe_venue_polymarket::CanaryV2Client::negrisk_spender(),
    ]
    .into_iter()
    .flatten()
    .any(|spender| spender == selected_spender)
}

fn valid_account_response(
    response: &RawHttpResponse,
    endpoint_kind: &str,
    path: &str,
    ordered_query: &[(&str, &str)],
) -> bool {
    let query_matches = response.ordered_query.len() == ordered_query.len()
        && response.ordered_query.iter().zip(ordered_query).all(
            |((actual_name, actual_value), (name, value))| {
                actual_name == name && actual_value == value
            },
        );
    response.source_id == "polymarket-clob-v2"
        && response.endpoint_kind == endpoint_kind
        && response.method == "GET"
        && response.path == path
        && query_matches
        && (200..300).contains(&response.status)
        && response.attempt_ordinal == 1
        && response.received_at >= response.observed_at
        && response.schema_version == 1
        && response.parser_version == 1
        && response.adapter_version == pe_venue_polymarket::SDK_VERSION
}

fn account_response_json(response: &RawHttpResponse) -> Result<Value, ()> {
    if !(200..300).contains(&response.status) {
        return Err(());
    }
    serde_json::from_slice(&response.body).map_err(|_| ())
}

fn account_atomic_amount(value: Option<&Value>) -> Option<CollateralAmount> {
    let value = value?;
    let encoded = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    encoded
        .parse::<u64>()
        .ok()
        .map(CollateralAmount::from_atomic)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LiveVenuePreparationError {
    #[error("venue order preparation failed")]
    Venue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LivePostParseError {
    #[error("order POST response could not be classified")]
    InvalidResponse,
}

/// Classification of the one raw POST response before any reconciliation fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivePostClassification {
    Matched {
        venue_order_id: String,
        executed: LiveExecutedAmounts,
        transaction_hashes: Vec<String>,
    },
    Killed {
        venue_order_id: Option<String>,
    },
    Rejected {
        venue_order_id: Option<String>,
    },
    Ambiguous {
        kind: LiveOrderAmbiguityKind,
    },
}

/// Result of the order-hash lookup and cancel-unexpected-order seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveVenueReconciledOutcome {
    Matched {
        venue_order_id: String,
        transaction_hashes: Vec<String>,
    },
    Killed {
        venue_order_id: Option<String>,
    },
    Rejected {
        venue_order_id: Option<String>,
    },
    Ambiguous {
        kind: LiveOrderAmbiguityKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueReconciliation {
    pub outcome: LiveVenueReconciledOutcome,
    pub evidence: Vec<RawHttpAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueReconciliationError {
    pub evidence: Vec<RawHttpAttempt>,
}

pub type LiveVenuePrepareFuture<'a, S> = Pin<
    Box<dyn Future<Output = Result<LiveVenuePrepared<S>, LiveVenuePreparationError>> + Send + 'a>,
>;

pub type LivePostFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + 'a>>;

pub type LiveReconciliationFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
            + Send
            + 'a,
    >,
>;

pub type LiveAccountStateFuture<'a> = Pin<
    Box<dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>> + Send + 'a>,
>;

/// Venue seam owned by execution-core. Implementations must perform exactly one request in
/// `post_once`; `reconcile_and_cancel_by_order_hash` must locate by signed order hash and cancel
/// any unexpected live order before reporting a terminal no-fill.
pub trait LiveOrderVenue: Send + Sync {
    type Submission: Send;

    fn prepare<'a>(
        &'a self,
        request: LiveVenuePrepareRequest,
    ) -> LiveVenuePrepareFuture<'a, Self::Submission>;

    fn post_once<'a>(&'a self, submission: Self::Submission) -> LivePostFuture<'a>;

    fn classify_post_response(
        &self,
        response: &RawHttpResponse,
    ) -> Result<LivePostClassification, LivePostParseError>;

    fn reconcile_and_cancel_by_order_hash<'a>(
        &'a self,
        order_hash: &'a str,
    ) -> LiveReconciliationFuture<'a>;

    fn read_balance_and_allowance<'a>(&'a self, neg_risk: bool) -> LiveAccountStateFuture<'a>;
}

/// Result of phase one. `Prepared` is the only variant carrying a POST capability.
pub enum LivePrepareResult<S> {
    Prepared(PreparedLiveOrder<S>),
    Terminal(LiveOrderOutcome),
}

/// Prepared order returned only after its full audit record has been fsynced.
pub struct PreparedLiveOrder<S> {
    account_id: AccountId,
    identity: LiveOrderIdentity,
    order_hash: String,
    prepared_audit: Box<LiveOrderPreparedAudit>,
    submission: S,
}

impl<S> PreparedLiveOrder<S> {
    #[must_use]
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    #[must_use]
    pub fn identity(&self) -> &LiveOrderIdentity {
        &self.identity
    }

    #[must_use]
    pub fn order_hash(&self) -> &str {
        &self.order_hash
    }

    #[must_use]
    pub fn audit(&self) -> &LiveOrderPreparedAudit {
        &self.prepared_audit
    }
}

/// Per-target outcome mapped by the service onto `dispatch_targets` lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveOrderOutcome {
    Refused {
        reason: LiveAdmissionRefusal,
    },
    Matched {
        order_hash: String,
        venue_order_id: String,
        transaction_hashes: Vec<String>,
        /// Retained for legacy audit only; receipt logs own financial projection.
        executed: Option<LiveExecutedAmounts>,
    },
    Killed {
        order_hash: String,
        venue_order_id: Option<String>,
    },
    Rejected {
        order_hash: Option<String>,
        venue_order_id: Option<String>,
        kind: LiveOrderRejectKind,
    },
    Ambiguous {
        order_hash: String,
        kind: LiveOrderAmbiguityKind,
        reconcile_first: bool,
    },
}

impl LiveOrderOutcome {
    /// Coarse dispatch state expected by `paper-state`.
    #[must_use]
    pub const fn dispatch_state(&self) -> &'static str {
        match self {
            Self::Matched { .. } => "submitted",
            Self::Ambiguous { .. } => "ambiguous",
            Self::Refused { .. } | Self::Killed { .. } | Self::Rejected { .. } => "terminal",
        }
    }

    /// Stable terminal detail for service persistence. Ambiguous results are nonterminal.
    #[must_use]
    pub const fn terminal_reason(&self) -> Option<&'static str> {
        match self {
            Self::Refused {
                reason: LiveAdmissionRefusal::CredentialVersionChanged,
            } => Some("credential_version_changed"),
            Self::Refused { .. } => Some("admission_refused"),
            Self::Matched { .. } => None,
            Self::Killed { .. } => Some("killed"),
            Self::Rejected { .. } => Some("rejected"),
            Self::Ambiguous { .. } => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LiveExecutorError {
    #[error(transparent)]
    Journal(#[from] LiveJournalError),
}

pub struct LiveExecutor<'a, V: LiveOrderVenue> {
    venue: &'a V,
    journal: &'a LiveJournal,
}

impl<'a, V: LiveOrderVenue> LiveExecutor<'a, V> {
    #[must_use]
    pub const fn new(venue: &'a V, journal: &'a LiveJournal) -> Self {
        Self { venue, journal }
    }

    /// Test-only fixed-instant compatibility wrapper.
    #[cfg(test)]
    pub async fn prepare(
        &self,
        request: LiveOrderRequest,
        now: OffsetDateTime,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError> {
        self.prepare_with_clock(request, move || now).await
    }

    /// Production prepare path. The clock is sampled at each classification and durable append
    /// boundary so awaited account and venue I/O cannot backdate later facts.
    pub async fn prepare_with_clock<C>(
        &self,
        request: LiveOrderRequest,
        clock: C,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError>
    where
        C: Fn() -> OffsetDateTime,
    {
        self.prepare_with_optional_account(request, None, clock)
            .await
    }

    /// Production prepare path when the caller has just observed the account state through this
    /// venue. This preserves that exact observation across a service-owned pre-dispatch risk
    /// check without issuing another asynchronous account read afterward.
    pub async fn prepare_with_observed_account_and_clock<C>(
        &self,
        request: LiveOrderRequest,
        account: LiveVenueAccountState,
        clock: C,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError>
    where
        C: Fn() -> OffsetDateTime,
    {
        self.prepare_with_optional_account(request, Some(account), clock)
            .await
    }

    async fn prepare_with_optional_account<C>(
        &self,
        request: LiveOrderRequest,
        observed_account: Option<LiveVenueAccountState>,
        clock: C,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError>
    where
        C: Fn() -> OffsetDateTime,
    {
        let economic = request.economic.clone();
        let admission = LiveAdmissionArtifactAudit::new(
            &request.admission.market,
            &request.admission.settlement,
            request.admission.fee_schedule,
            request.admission.receipts,
        );
        let ladder = LadderPlanAudit::new(&request.ladder);
        let classify = |evaluated_at, account| {
            classify_live_admission(LiveAdmissionClassificationInput {
                evaluated_at,
                requested_mode: request.mode.requested,
                effective_mode: request.mode.effective,
                frozen_binding: &request.target.credential_binding,
                current_binding: &request.current_credential_binding,
                identity: &request.identity,
                condition_id: &request.condition_id,
                outcome_id: request.outcome_id,
                token_id: &request.token_id,
                admission: &admission,
                ladder: &ladder,
                economic: &economic,
                account,
            })
        };

        let pre_account_at = clock();
        if let Ok(LiveAdmissionVerdict::Refused(reason)) =
            classify(pre_account_at, LiveAdmissionAccountEvidence::NotRead)
        {
            if reason == LiveAdmissionRefusal::CredentialVersionChanged {
                self.journal.append(
                    request.target.account_id.clone(),
                    pre_account_at,
                    LiveJournalPayload::CredentialBindingMismatch {
                        frozen: request.target.credential_binding.clone(),
                        current: request.current_credential_binding.clone(),
                    },
                )?;
            }
            return self.refuse(
                &request,
                pre_account_at,
                economic,
                None,
                Vec::new(),
                Vec::new(),
                reason,
            );
        }

        let account = if let Some(account) = observed_account {
            account
        } else {
            match self
                .venue
                .read_balance_and_allowance(request.admission.market.neg_risk)
                .await
            {
                Ok(account) => account,
                Err(error) => {
                    let evaluated_at = clock();
                    let verdict = classify(
                        evaluated_at,
                        LiveAdmissionAccountEvidence::ReadFailure(error.kind),
                    )
                    .unwrap_or(LiveAdmissionVerdict::Refused(
                        LiveAdmissionRefusal::AccountStateUnavailable(
                            LiveAccountReadFailure::Protocol,
                        ),
                    ));
                    let LiveAdmissionVerdict::Refused(reason) = verdict else {
                        return self.refuse(
                            &request,
                            evaluated_at,
                            economic,
                            None,
                            error.evidence,
                            error.request_descriptor_hashes,
                            LiveAdmissionRefusal::AccountStateUnavailable(
                                LiveAccountReadFailure::Protocol,
                            ),
                        );
                    };
                    return self.refuse(
                        &request,
                        evaluated_at,
                        economic,
                        None,
                        error.evidence,
                        error.request_descriptor_hashes,
                        reason,
                    );
                }
            }
        };
        let account_audit = account.audit()?;
        let admission_at = clock();
        let verdict = classify(
            admission_at,
            LiveAdmissionAccountEvidence::State(&account_audit),
        )
        .unwrap_or(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::AccountStateUnavailable(LiveAccountReadFailure::Protocol),
        ));
        if let LiveAdmissionVerdict::Refused(reason) = verdict {
            return self.refuse(
                &request,
                admission_at,
                economic,
                Some(account_audit),
                Vec::new(),
                Vec::new(),
                reason,
            );
        }

        self.journal_admission(
            &request,
            admission_at,
            economic.clone(),
            Some(account_audit.clone()),
            Vec::new(),
            Vec::new(),
            LiveAdmissionVerdict::Approved,
        )?;

        let venue_request = LiveVenuePrepareRequest {
            condition_id: request.condition_id.clone(),
            outcome_id: request.outcome_id,
            token_id: request.token_id.clone(),
            neg_risk: request.admission.market.neg_risk,
            limit_price: request.ladder.limit_price,
            shares: request.ladder.shares,
            maximum_collateral: request.ladder.worst_case_debit,
            tick_size: request.admission.market.minimum_tick_size,
            metadata_hashes: request.identity.evidence_hashes.clone(),
        };
        let venue_prepared = match self.venue.prepare(venue_request).await {
            Ok(prepared) => prepared,
            Err(_) => {
                let failed_at = clock();
                self.journal.append(
                    request.target.account_id.clone(),
                    failed_at,
                    LiveJournalPayload::OrderPreparationFailed(Box::new(
                        LiveOrderPreparationFailedAudit {
                            identity: request.identity,
                            failure: LiveOrderPreparationFailure::Venue,
                        },
                    )),
                )?;
                return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                    order_hash: None,
                    venue_order_id: None,
                    kind: LiveOrderRejectKind::PreparationFailed,
                }));
            }
        };
        if !prepared_matches_request(venue_prepared.audit(), &request, &account) {
            let failed_at = clock();
            self.journal.append(
                request.target.account_id.clone(),
                failed_at,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: request.identity,
                        failure: LiveOrderPreparationFailure::PreparedAuditMismatch,
                    },
                )),
            )?;
            return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                order_hash: None,
                venue_order_id: None,
                kind: LiveOrderRejectKind::PreparationFailed,
            }));
        }

        let (prepared, submission) = venue_prepared.into_parts();
        let prepared_audit = LiveOrderPreparedAudit::new(
            request.identity.clone(),
            request.target.credential_binding,
            economic,
            account_audit,
            prepared.clone(),
        );
        let prepared_at = clock();
        self.journal.append(
            request.target.account_id.clone(),
            prepared_at,
            LiveJournalPayload::OrderPrepared(Box::new(prepared_audit.clone())),
        )?;
        Ok(LivePrepareResult::Prepared(PreparedLiveOrder {
            account_id: request.target.account_id,
            identity: request.identity,
            order_hash: prepared.order_hash,
            prepared_audit: Box::new(prepared_audit),
            submission,
        }))
    }

    /// Resume a synchronized Approved admission after a crash without emitting a second
    /// `AdmissionEvaluated`. Expired or no-longer-reproducible frozen evidence is terminalized.
    pub async fn resume_approved_admission_with_clock<C>(
        &self,
        account_id: AccountId,
        admission: Box<LiveAdmissionEvaluationAudit>,
        current_mode: LiveModeSnapshot,
        current_binding: CredentialBindingIdentity,
        current_account_state: LiveVenueAccountState,
        clock: C,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError>
    where
        C: Fn() -> OffsetDateTime,
    {
        let evaluated_at = clock();
        let current_account_audit = current_account_state.audit()?;
        let reproduced = {
            classify_live_admission(LiveAdmissionClassificationInput {
                evaluated_at,
                requested_mode: current_mode.requested,
                effective_mode: current_mode.effective,
                frozen_binding: &admission.frozen_binding,
                current_binding: &current_binding,
                identity: &admission.identity,
                condition_id: &admission.economic.market.condition_id,
                outcome_id: OutcomeId(u16::from(admission.economic.market.outcome_index)),
                token_id: &admission.economic.market.token_id,
                admission: &admission.economic.admission,
                ladder: &admission.economic.ladder,
                economic: &admission.economic,
                account: LiveAdmissionAccountEvidence::State(&current_account_audit),
            })
        };
        if admission.verdict != LiveAdmissionVerdict::Approved
            || !recorded_risk_is_approved(&admission.economic)
            || !matches!(reproduced, Ok(LiveAdmissionVerdict::Approved))
        {
            self.terminalize_approved_admission(
                account_id,
                admission.identity.clone(),
                evaluated_at,
                LiveOrderPreparationFailure::RecoveryAdmissionExpired,
            )?;
            return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                order_hash: None,
                venue_order_id: None,
                kind: LiveOrderRejectKind::PreparationFailed,
            }));
        }
        let recorded_account_state = match admission.account_state.as_ref() {
            Some(account) => account.clone(),
            None => {
                self.terminalize_approved_admission(
                    account_id,
                    admission.identity.clone(),
                    evaluated_at,
                    LiveOrderPreparationFailure::RecoveryAdmissionExpired,
                )?;
                return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                    order_hash: None,
                    venue_order_id: None,
                    kind: LiveOrderRejectKind::PreparationFailed,
                }));
            }
        };
        let venue_request = venue_request_from_approved_admission(&admission);
        let venue_prepared = match self.venue.prepare(venue_request).await {
            Ok(prepared) => prepared,
            Err(_) => {
                self.terminalize_approved_admission(
                    account_id,
                    admission.identity.clone(),
                    clock(),
                    LiveOrderPreparationFailure::Venue,
                )?;
                return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                    order_hash: None,
                    venue_order_id: None,
                    kind: LiveOrderRejectKind::PreparationFailed,
                }));
            }
        };
        if !prepared_matches_approved_admission(
            venue_prepared.audit(),
            &admission,
            &current_account_audit,
        ) {
            self.terminalize_approved_admission(
                account_id,
                admission.identity.clone(),
                clock(),
                LiveOrderPreparationFailure::PreparedAuditMismatch,
            )?;
            return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                order_hash: None,
                venue_order_id: None,
                kind: LiveOrderRejectKind::PreparationFailed,
            }));
        }
        let (prepared, submission) = venue_prepared.into_parts();
        let prepared_audit = LiveOrderPreparedAudit::new(
            admission.identity.clone(),
            admission.frozen_binding.clone(),
            admission.economic.clone(),
            recorded_account_state,
            prepared.clone(),
        );
        self.journal.append(
            account_id.clone(),
            clock(),
            LiveJournalPayload::OrderPrepared(Box::new(prepared_audit.clone())),
        )?;
        Ok(LivePrepareResult::Prepared(PreparedLiveOrder {
            account_id,
            identity: admission.identity.clone(),
            order_hash: prepared.order_hash,
            prepared_audit: Box::new(prepared_audit),
            submission,
        }))
    }

    /// Consume an unmatched Approved admission with a durable typed preparation failure.
    fn terminalize_approved_admission(
        &self,
        account_id: AccountId,
        identity: LiveOrderIdentity,
        now: OffsetDateTime,
        failure: LiveOrderPreparationFailure,
    ) -> Result<(), LiveExecutorError> {
        self.journal.append(
            account_id,
            now,
            LiveJournalPayload::OrderPreparationFailed(Box::new(LiveOrderPreparationFailedAudit {
                identity,
                failure,
            })),
        )?;
        Ok(())
    }

    /// Drop an unconsumed POST capability and terminalize its durable Prepared record.
    pub fn terminalize_prepared(
        &self,
        prepared: PreparedLiveOrder<V::Submission>,
        now: OffsetDateTime,
        failure: LiveOrderPreparationFailure,
    ) -> Result<LiveOrderOutcome, LiveExecutorError> {
        let PreparedLiveOrder {
            account_id,
            identity,
            submission: _,
            ..
        } = prepared;
        self.terminalize_approved_admission(account_id, identity, now, failure)?;
        Ok(LiveOrderOutcome::Rejected {
            order_hash: None,
            venue_order_id: None,
            kind: LiveOrderRejectKind::PreparationFailed,
        })
    }

    /// Test-only fixed-instant compatibility wrapper.
    #[cfg(test)]
    pub async fn submit(
        &self,
        prepared: PreparedLiveOrder<V::Submission>,
        now: OffsetDateTime,
    ) -> Result<LiveOrderOutcome, LiveExecutorError> {
        self.submit_with_clock(prepared, move || now).await
    }

    /// Production POST path. The response and any subsequent reconciliation are stamped only
    /// after their corresponding I/O completes.
    pub async fn submit_with_clock<C>(
        &self,
        prepared: PreparedLiveOrder<V::Submission>,
        clock: C,
    ) -> Result<LiveOrderOutcome, LiveExecutorError>
    where
        C: Fn() -> OffsetDateTime,
    {
        let PreparedLiveOrder {
            account_id,
            identity,
            order_hash,
            submission,
            ..
        } = prepared;
        let post = self.venue.post_once(submission).await;
        let posted_at = clock();
        match post {
            Err(failure) => {
                let kind = if failure.error_class == TransportErrorClass::Timeout {
                    LiveOrderAmbiguityKind::Timeout
                } else {
                    LiveOrderAmbiguityKind::Transport
                };
                let attempt = RawHttpAttempt::TransportFailure(failure);
                self.journal_post(&account_id, &identity, &order_hash, posted_at, attempt)?;
                self.reconcile_ambiguous_with_clock(account_id, identity, order_hash, kind, &clock)
                    .await
            }
            Ok(response) => {
                let attempt = RawHttpAttempt::Response(response.clone());
                self.journal_post(
                    &account_id,
                    &identity,
                    &order_hash,
                    posted_at,
                    attempt.clone(),
                )?;
                match self.venue.classify_post_response(&response) {
                    Ok(LivePostClassification::Matched {
                        venue_order_id,
                        executed,
                        transaction_hashes,
                    }) => {
                        let journal_outcome = LiveJournalOrderOutcome::Matched {
                            venue_order_id: venue_order_id.clone(),
                            transaction_hashes: transaction_hashes.clone(),
                            executed: Some(executed.clone()),
                        };
                        self.journal_reconciliation(
                            &account_id,
                            &identity,
                            &order_hash,
                            posted_at,
                            LiveReconciliationSource::PostResponse,
                            journal_outcome,
                            vec![attempt],
                        )?;
                        Ok(LiveOrderOutcome::Matched {
                            order_hash,
                            venue_order_id,
                            transaction_hashes,
                            executed: Some(executed),
                        })
                    }
                    Ok(LivePostClassification::Killed { venue_order_id }) => {
                        let journal_outcome = LiveJournalOrderOutcome::Killed {
                            venue_order_id: venue_order_id.clone(),
                        };
                        self.journal_reconciliation(
                            &account_id,
                            &identity,
                            &order_hash,
                            posted_at,
                            LiveReconciliationSource::PostResponse,
                            journal_outcome,
                            vec![attempt],
                        )?;
                        Ok(LiveOrderOutcome::Killed {
                            order_hash,
                            venue_order_id,
                        })
                    }
                    Ok(LivePostClassification::Rejected { venue_order_id }) => {
                        let journal_outcome = LiveJournalOrderOutcome::Rejected {
                            venue_order_id: venue_order_id.clone(),
                            kind: LiveOrderRejectKind::VenueRejected,
                        };
                        self.journal_reconciliation(
                            &account_id,
                            &identity,
                            &order_hash,
                            posted_at,
                            LiveReconciliationSource::PostResponse,
                            journal_outcome,
                            vec![attempt],
                        )?;
                        Ok(LiveOrderOutcome::Rejected {
                            order_hash: Some(order_hash),
                            venue_order_id,
                            kind: LiveOrderRejectKind::VenueRejected,
                        })
                    }
                    Ok(LivePostClassification::Ambiguous { kind }) => {
                        self.reconcile_ambiguous_with_clock(
                            account_id, identity, order_hash, kind, &clock,
                        )
                        .await
                    }
                    Err(_) => {
                        self.reconcile_ambiguous_with_clock(
                            account_id,
                            identity,
                            order_hash,
                            LiveOrderAmbiguityKind::UnexpectedResponse,
                            &clock,
                        )
                        .await
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn refuse(
        &self,
        request: &LiveOrderRequest,
        now: OffsetDateTime,
        economic: crate::economic::EconomicPrepared,
        account_state: Option<LiveAccountStateAudit>,
        failure_evidence: Vec<RawHttpAttempt>,
        failure_request_descriptor_hashes: Vec<String>,
        reason: LiveAdmissionRefusal,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError> {
        self.journal_admission(
            request,
            now,
            economic,
            account_state,
            failure_evidence,
            failure_request_descriptor_hashes,
            LiveAdmissionVerdict::Refused(reason.clone()),
        )?;
        Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Refused {
            reason,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn journal_admission(
        &self,
        request: &LiveOrderRequest,
        now: OffsetDateTime,
        economic: crate::economic::EconomicPrepared,
        account_state: Option<LiveAccountStateAudit>,
        failure_evidence: Vec<RawHttpAttempt>,
        failure_request_descriptor_hashes: Vec<String>,
        verdict: LiveAdmissionVerdict,
    ) -> Result<(), LiveJournalError> {
        let failure_hashes = http_attempt_hashes(&failure_evidence)?;
        self.journal.append(
            request.target.account_id.clone(),
            now,
            LiveJournalPayload::AdmissionEvaluated(Box::new(LiveAdmissionEvaluationAudit {
                identity: request.identity.clone(),
                frozen_binding: request.target.credential_binding.clone(),
                current_binding: request.current_credential_binding.clone(),
                requested_mode: request.mode.requested,
                effective_mode: request.mode.effective,
                economic,
                account_state,
                account_read_failure_evidence: failure_evidence,
                account_read_failure_request_descriptor_hashes: failure_request_descriptor_hashes,
                account_read_failure_evidence_hashes: failure_hashes,
                verdict,
            })),
        )?;
        Ok(())
    }

    fn journal_post(
        &self,
        account_id: &AccountId,
        identity: &LiveOrderIdentity,
        order_hash: &str,
        now: OffsetDateTime,
        evidence: RawHttpAttempt,
    ) -> Result<(), LiveJournalError> {
        let evidence_hash = http_attempt_hash(&evidence)?;
        self.journal.append(
            account_id.clone(),
            now,
            LiveJournalPayload::OrderPosted(Box::new(LiveOrderPostAudit {
                identity: identity.clone(),
                order_hash: order_hash.to_owned(),
                evidence,
                evidence_hash,
            })),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn journal_reconciliation(
        &self,
        account_id: &AccountId,
        identity: &LiveOrderIdentity,
        order_hash: &str,
        now: OffsetDateTime,
        source: LiveReconciliationSource,
        outcome: LiveJournalOrderOutcome,
        evidence: Vec<RawHttpAttempt>,
    ) -> Result<(), LiveJournalError> {
        let evidence_hashes = http_attempt_hashes(&evidence)?;
        self.journal.append(
            account_id.clone(),
            now,
            LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                identity: identity.clone(),
                order_hash: order_hash.to_owned(),
                source,
                outcome,
                evidence,
                evidence_hashes,
            })),
        )?;
        Ok(())
    }

    async fn reconcile_ambiguous_with_clock<C>(
        &self,
        account_id: AccountId,
        identity: LiveOrderIdentity,
        order_hash: String,
        initial_kind: LiveOrderAmbiguityKind,
        clock: &C,
    ) -> Result<LiveOrderOutcome, LiveExecutorError>
    where
        C: Fn() -> OffsetDateTime,
    {
        let reconciliation = self
            .venue
            .reconcile_and_cancel_by_order_hash(&order_hash)
            .await;
        let reconciled_at = clock();
        let (outcome, evidence) = match reconciliation {
            Ok(reconciliation) => (reconciliation.outcome, reconciliation.evidence),
            Err(error) => {
                let journal_outcome = LiveJournalOrderOutcome::Ambiguous {
                    kind: LiveOrderAmbiguityKind::ReconciliationUnavailable,
                };
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    reconciled_at,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    journal_outcome,
                    error.evidence,
                )?;
                return Ok(LiveOrderOutcome::Ambiguous {
                    order_hash,
                    kind: initial_kind,
                    reconcile_first: true,
                });
            }
        };
        match outcome {
            LiveVenueReconciledOutcome::Matched {
                venue_order_id,
                transaction_hashes,
            } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    reconciled_at,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Matched {
                        venue_order_id: venue_order_id.clone(),
                        transaction_hashes: transaction_hashes.clone(),
                        executed: None,
                    },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Matched {
                    order_hash,
                    venue_order_id,
                    transaction_hashes,
                    executed: None,
                })
            }
            LiveVenueReconciledOutcome::Killed { venue_order_id } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    reconciled_at,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Killed {
                        venue_order_id: venue_order_id.clone(),
                    },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Killed {
                    order_hash,
                    venue_order_id,
                })
            }
            LiveVenueReconciledOutcome::Rejected { venue_order_id } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    reconciled_at,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Rejected {
                        venue_order_id: venue_order_id.clone(),
                        kind: LiveOrderRejectKind::VenueRejected,
                    },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Rejected {
                    order_hash: Some(order_hash),
                    venue_order_id,
                    kind: LiveOrderRejectKind::VenueRejected,
                })
            }
            LiveVenueReconciledOutcome::Ambiguous { kind } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    reconciled_at,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Ambiguous { kind },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Ambiguous {
                    order_hash,
                    kind: initial_kind,
                    reconcile_first: true,
                })
            }
        }
    }
}

/// Re-execute the production admission gates in their runtime order without I/O.
///
/// Calling with [`LiveAdmissionAccountEvidence::NotRead`] evaluates every gate that must precede
/// authenticated venue I/O. A successful pre-I/O pass returns
/// [`LiveAdmissionNeedsAccountState`]; callers then supply the retained, independently verified
/// account result to derive the final durable verdict.
pub fn classify_live_admission(
    input: LiveAdmissionClassificationInput<'_>,
) -> Result<LiveAdmissionVerdict, LiveAdmissionNeedsAccountState> {
    // The per-account kill is local and therefore precedes authenticated venue I/O.
    if !matches!(input.requested_mode, LiveControlMode::LiveTiny)
        || !matches!(input.effective_mode, LiveControlMode::LiveTiny)
    {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::ModeNotArmed,
        ));
    }

    // Ordered admission check 1: exact credential binding.
    if input.frozen_binding != input.current_binding {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::CredentialVersionChanged,
        ));
    }

    // Ordered admission check 2: both artifact halves and the frozen quote/ladder.
    let required = match validate_artifact_and_ladder(&input) {
        Ok(required) => required,
        Err(reason) => return Ok(LiveAdmissionVerdict::Refused(reason)),
    };

    let account = match input.account {
        LiveAdmissionAccountEvidence::NotRead => return Err(LiveAdmissionNeedsAccountState),
        LiveAdmissionAccountEvidence::ReadFailure(kind) => {
            return Ok(LiveAdmissionVerdict::Refused(
                LiveAdmissionRefusal::AccountStateUnavailable(kind),
            ));
        }
        LiveAdmissionAccountEvidence::State(account) => account,
    };

    // Ordered checks 3 and 4: account state, then same-egress geoblock.
    if account.closed_only {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::AccountClosedOnly,
        ));
    }
    if account.geoblocked {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::Geoblocked,
        ));
    }

    // Ordered check 5: balance and selected-spender allowance.
    if account.collateral_balance < required {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::InsufficientBalance {
                required,
                available: account.collateral_balance,
            },
        ));
    }
    if account.allowance < required {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::InsufficientAllowance {
                required,
                available: account.allowance,
            },
        ));
    }

    // Ordered check 6: reconciled free collateral is a distinct bound.
    if account.reconciled_free_collateral < required {
        return Ok(LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::WorstCaseDebitExceedsFreeCollateral {
                required,
                available: account.reconciled_free_collateral,
            },
        ));
    }

    Ok(LiveAdmissionVerdict::Approved)
}

fn validate_artifact_and_ladder(
    input: &LiveAdmissionClassificationInput<'_>,
) -> Result<CollateralAmount, LiveAdmissionRefusal> {
    let market = &input.admission.market;
    if market.schema_version != LIVE_MARKET_SCHEMA_VERSION
        || market.parser_version != LIVE_MARKET_PARSER_VERSION
        || market.minimum_tick_size == Price::ZERO
        || market.minimum_order_size == ShareAmount::ZERO
    {
        return Err(LiveAdmissionRefusal::MarketEvidenceInvalid);
    }
    LiveMarketEvidence {
        condition_id: market.condition_id.clone(),
        ordered_outcome_token_ids: market.ordered_outcome_token_ids.clone(),
        neg_risk: market.neg_risk,
        minimum_tick_size: market.minimum_tick_size,
        minimum_order_size: market.minimum_order_size,
        scheduled_end_unix: input.admission.scheduled_end_unix,
        observed_at_unix: market.observed_at_unix,
        schema_version: market.schema_version,
        parser_version: market.parser_version,
        freshness_window_secs: market.freshness_window_secs,
    }
    .validate_fresh(input.evaluated_at.unix_timestamp())
    .map_err(|error| match error {
        LiveMarketError::Stale | LiveMarketError::FutureObservation => {
            LiveAdmissionRefusal::MarketEvidenceStale
        }
        _ => LiveAdmissionRefusal::MarketEvidenceInvalid,
    })?;
    input
        .admission
        .settlement
        .validate_fresh_for_entry(input.evaluated_at)
        .map_err(|error| match error {
            VenueSettlementError::Stale | VenueSettlementError::Schema(_) => {
                LiveAdmissionRefusal::SettlementEvidenceStale
            }
            VenueSettlementError::AlreadyResolved => LiveAdmissionRefusal::MarketAlreadyResolved,
            VenueSettlementError::Ambiguous => LiveAdmissionRefusal::SettlementAmbiguous,
        })?;

    let outcome_index = usize::from(input.outcome_id.0);
    if input.identity.dispatch_id.trim().is_empty()
        || input.identity.idempotency_key.trim().is_empty()
        || input.identity.quote_id.trim().is_empty()
        || input.identity.config_hash.trim().is_empty()
        || input.identity.decision_hash.trim().is_empty()
        || input.identity.evidence_hashes.is_empty()
        || input.identity.schema_version == 0
        || input.identity.parser_version == 0
        || input.admission != &input.economic.admission
        || input.economic.market.condition_id != *input.condition_id
        || u16::from(input.economic.market.outcome_index) != input.outcome_id.0
        || input.economic.market.token_id != *input.token_id
        || input.economic.market.side != pe_core_types::Side::Buy
        || market.condition_id != *input.condition_id
        || input.admission.settlement.condition_id != *input.condition_id
        || market
            .ordered_outcome_token_ids
            .get(outcome_index)
            .is_none_or(|token| token != input.token_id)
    {
        return Err(LiveAdmissionRefusal::ArtifactIdentityMismatch);
    }
    validate_ladder(
        input.ladder,
        market.minimum_tick_size,
        market.minimum_order_size,
    )?;
    if input.ladder != &input.economic.ladder {
        return Err(LiveAdmissionRefusal::ArtifactIdentityMismatch);
    }
    let required = input
        .economic
        .worst_case_all_in_debit()
        .map_err(|_| LiveAdmissionRefusal::LadderInvalid)?;
    if input.economic.balance.worst_case_debit != required {
        return Err(LiveAdmissionRefusal::LadderInvalid);
    }
    Ok(required)
}

fn validate_ladder(
    plan: &LadderPlanAudit,
    minimum_tick_size: Price,
    minimum_order_size: ShareAmount,
) -> Result<(), LiveAdmissionRefusal> {
    if plan.used_asks.is_empty()
        || plan.minimum_shares < minimum_order_size
        || plan.best_ask == Price::ZERO
        || plan.limit_price == Price::ZERO
        || plan.best_ask > plan.limit_price
        || plan.limit_price.0 % minimum_tick_size.0 != Decimal::ZERO
    {
        return Err(LiveAdmissionRefusal::LadderInvalid);
    }
    let mut shares = ShareAmount::ZERO;
    let mut spend = Decimal::ZERO;
    let mut previous = None;
    for ask in &plan.used_asks {
        if ask.price == Price::ZERO
            || ask.shares == ShareAmount::ZERO
            || previous.is_some_and(|price| ask.price < price)
        {
            return Err(LiveAdmissionRefusal::LadderInvalid);
        }
        shares = shares
            .checked_add(ask.shares)
            .map_err(|_| LiveAdmissionRefusal::LadderInvalid)?;
        spend = spend
            .checked_add(ask.shares.to_decimal() * ask.price.0)
            .ok_or(LiveAdmissionRefusal::LadderInvalid)?;
        previous = Some(ask.price);
    }
    let expected_spend = CollateralAmount::from_decimal_exact(
        spend.round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToNegativeInfinity),
    )
    .map_err(|_| LiveAdmissionRefusal::LadderInvalid)?;
    if shares < plan.minimum_shares
        || expected_spend > plan.principal
        || plan.best_ask != plan.used_asks[0].price
        || plan.limit_price != plan.used_asks[plan.used_asks.len() - 1].price
    {
        return Err(LiveAdmissionRefusal::LadderInvalid);
    }
    Ok(())
}

fn recorded_risk_is_approved(economic: &crate::economic::EconomicPrepared) -> bool {
    let evaluated = match pe_risk_engine::evaluate_risk(&economic.risk.snapshot) {
        pe_risk_engine::RiskDecision::Approved => crate::RiskDecisionAudit::Approved,
        pe_risk_engine::RiskDecision::Blocked(reason) => {
            crate::RiskDecisionAudit::Blocked { reason }
        }
    };
    evaluated == economic.risk.decision && evaluated == crate::RiskDecisionAudit::Approved
}

fn venue_request_from_approved_admission(
    admission: &LiveAdmissionEvaluationAudit,
) -> LiveVenuePrepareRequest {
    LiveVenuePrepareRequest {
        condition_id: admission.economic.market.condition_id.clone(),
        outcome_id: OutcomeId(u16::from(admission.economic.market.outcome_index)),
        token_id: admission.economic.market.token_id.clone(),
        neg_risk: admission.economic.admission.market.neg_risk,
        limit_price: admission.economic.ladder.limit_price,
        shares: admission.economic.ladder.minimum_shares,
        maximum_collateral: admission.economic.ladder.principal,
        tick_size: admission.economic.admission.market.minimum_tick_size,
        metadata_hashes: admission.identity.evidence_hashes.clone(),
    }
}

fn prepared_matches_approved_admission(
    prepared: &PreparedPolymarketBuy,
    admission: &LiveAdmissionEvaluationAudit,
    account: &LiveAccountStateAudit,
) -> bool {
    let request = venue_request_from_approved_admission(admission);
    prepared.condition_id == request.condition_id
        && prepared.outcome_id == request.outcome_id
        && prepared.token_id == request.token_id
        && prepared.neg_risk == request.neg_risk
        && prepared.side == "BUY"
        && prepared.order_type == "FOK"
        && !prepared.post_only
        && !prepared.defer_exec
        && prepared.exchange_domain_version == 2
        && prepared.taker_shares == request.shares
        && prepared.limit_price == request.limit_price
        && prepared.minimum_tick_size == request.tick_size
        && prepared.maker_collateral == request.maximum_collateral
        && prepared.worst_case_debit == request.maximum_collateral
        && prepared.spender == account.selected_spender
        && prepared.verifying_contract == account.selected_spender
        && prepared.metadata_hashes == request.metadata_hashes
        && !prepared.order_hash.trim().is_empty()
        && !prepared.post_body_hash.trim().is_empty()
}

fn prepared_matches_request(
    prepared: &PreparedPolymarketBuy,
    request: &LiveOrderRequest,
    account: &LiveVenueAccountState,
) -> bool {
    request.economic.market.condition_id == request.condition_id
        && u16::from(request.economic.market.outcome_index) == request.outcome_id.0
        && request.economic.market.token_id == request.token_id
        && request.economic.market.side == pe_core_types::Side::Buy
        && prepared.condition_id == request.economic.market.condition_id
        && prepared.outcome_id == request.outcome_id
        && prepared.token_id == request.economic.market.token_id
        && prepared.neg_risk == request.admission.market.neg_risk
        && prepared.side == "BUY"
        && prepared.order_type == "FOK"
        && !prepared.post_only
        && !prepared.defer_exec
        && prepared.exchange_domain_version == 2
        && prepared.taker_shares == request.ladder.shares
        && prepared.limit_price == request.ladder.limit_price
        && prepared.minimum_tick_size == request.admission.market.minimum_tick_size
        && prepared.maker_collateral == request.ladder.worst_case_debit
        && prepared.worst_case_debit == request.ladder.worst_case_debit
        && prepared.spender == account.selected_spender
        && prepared.verifying_contract == account.selected_spender
        && prepared.metadata_hashes == request.identity.evidence_hashes
        && !prepared.order_hash.trim().is_empty()
        && !prepared.post_body_hash.trim().is_empty()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pe_resolver_card::{VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus};
    use pe_source_polymarket_public::LiveMarketEvidence;
    use pe_venue_polymarket::AskLevel;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;
    use time::macros::datetime;

    use super::*;
    use crate::economic::{
        BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit, MarketSelection,
        RiskAudit, RiskDecisionAudit, SizingAudit, SizingModeAudit,
    };
    use crate::live_journal::{
        LadderPlanAudit, LiveAdmissionArtifactAudit, LiveJournalPayload, replay_account,
    };

    struct FixtureVenue {
        account: Mutex<Result<LiveVenueAccountState, LiveVenueAccountReadError>>,
        classification: LivePostClassification,
        reconciliation: LiveVenueReconciledOutcome,
        posts: AtomicUsize,
        reconciliations: AtomicUsize,
    }

    impl FixtureVenue {
        fn new(classification: LivePostClassification) -> Self {
            Self {
                account: Mutex::new(Ok(account_state())),
                classification,
                reconciliation: LiveVenueReconciledOutcome::Ambiguous {
                    kind: LiveOrderAmbiguityKind::ReconciliationPending,
                },
                posts: AtomicUsize::new(0),
                reconciliations: AtomicUsize::new(0),
            }
        }
    }

    impl LiveOrderVenue for FixtureVenue {
        type Submission = ();

        fn prepare<'a>(
            &'a self,
            request: LiveVenuePrepareRequest,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            LiveVenuePrepared<Self::Submission>,
                            LiveVenuePreparationError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(LiveVenuePrepared::new(
                    prepared_order(&request, "spender"),
                    (),
                ))
            })
        }

        fn post_once<'a>(
            &'a self,
            _submission: Self::Submission,
        ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + 'a>>
        {
            self.posts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(raw_response("order-post")) })
        }

        fn classify_post_response(
            &self,
            _response: &RawHttpResponse,
        ) -> Result<LivePostClassification, LivePostParseError> {
            Ok(self.classification.clone())
        }

        fn reconcile_and_cancel_by_order_hash<'a>(
            &'a self,
            _order_hash: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
                    + Send
                    + 'a,
            >,
        > {
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            let outcome = self.reconciliation.clone();
            Box::pin(async move {
                Ok(LiveVenueReconciliation {
                    outcome,
                    evidence: vec![RawHttpAttempt::Response(raw_response("order-status"))],
                })
            })
        }

        fn read_balance_and_allowance<'a>(
            &'a self,
            _neg_risk: bool,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>>
                    + Send
                    + 'a,
            >,
        > {
            let result = self.account.lock().unwrap().clone();
            Box::pin(async move { result })
        }
    }

    fn now() -> OffsetDateTime {
        datetime!(2026-08-11 12:00 UTC)
    }

    fn raw_response(endpoint_kind: &str) -> RawHttpResponse {
        RawHttpResponse {
            source_id: "fixture".to_owned(),
            endpoint_kind: endpoint_kind.to_owned(),
            method: "GET".to_owned(),
            path: "/fixture".to_owned(),
            ordered_query: Vec::new(),
            status: 200,
            headers: Vec::new(),
            body: br#"{"ok":true}"#.to_vec(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: now(),
            received_at: now(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "fixture-v1".to_owned(),
        }
    }

    fn account_state() -> LiveVenueAccountState {
        LiveVenueAccountState {
            observed_at: now(),
            closed_only: false,
            geoblocked: false,
            selected_spender: "spender".to_owned(),
            collateral_balance: CollateralAmount::from_atomic(10_000_000),
            allowance: CollateralAmount::from_atomic(10_000_000),
            reconciled_free_collateral: CollateralAmount::from_atomic(10_000_000),
            schema_version: 1,
            parser_version: 1,
            evidence: vec![RawHttpAttempt::Response(raw_response("account-state"))],
            request_descriptor_hashes: Vec::new(),
        }
    }

    fn market() -> LiveMarketEvidence {
        LiveMarketEvidence {
            condition_id: PolymarketConditionId("condition".to_owned()),
            ordered_outcome_token_ids: [
                PolymarketTokenId("11".to_owned()),
                PolymarketTokenId("22".to_owned()),
            ],
            neg_risk: true,
            minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
            minimum_order_size: ShareAmount::from_atomic(5_000_000),
            scheduled_end_unix: None,
            observed_at_unix: now().unix_timestamp(),
            schema_version: LIVE_MARKET_SCHEMA_VERSION,
            parser_version: LIVE_MARKET_PARSER_VERSION,
            freshness_window_secs: 60,
        }
    }

    fn settlement() -> VenueSettlementRecord {
        VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: PolymarketConditionId("condition".to_owned()),
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: blake3::hash(b"settlement").to_hex().to_string(),
            source_timestamp_unix: Some(now().unix_timestamp()),
            observed_at_unix: now().unix_timestamp(),
            parser_version: 1,
            freshness_window_secs: 60,
        }
    }

    fn ladder() -> LadderPlan {
        let price = Price::new(dec!(0.50)).unwrap();
        let shares = ShareAmount::from_atomic(5_000_000);
        LadderPlan {
            used_asks: vec![AskLevel { price, shares }],
            best_ask: price,
            limit_price: price,
            shares,
            worst_case_debit: CollateralAmount::from_atomic(2_500_000),
        }
    }

    fn request(account_id: &AccountId) -> LiveOrderRequest {
        let identity = LiveOrderIdentity {
            dispatch_id: "dispatch-1".to_owned(),
            idempotency_key: LiveOrderIdentity::idempotency_key_for("dispatch-1", account_id),
            quote_id: "quote-1".to_owned(),
            config_hash: "config-hash".to_owned(),
            decision_hash: "decision-hash".to_owned(),
            evidence_hashes: vec!["market-hash".to_owned()],
            fill_projection: None,
            schema_version: 1,
            parser_version: 1,
        };
        let market = market();
        let settlement = settlement();
        let plan = ladder();
        let receipt = pe_event_log::AppendReceipt {
            sequence: pe_core_types::EventSeq(1),
            this_hash: blake3::Hash::from_bytes([1; 32]),
        };
        let receipts = AdmissionReceipts {
            gamma: receipt,
            clob_long: receipt,
            clob_compact: receipt,
        };
        let fee_schedule = CompactFeeSchedule::Zero;
        let admission_audit =
            LiveAdmissionArtifactAudit::new(&market, &settlement, fee_schedule, receipts);
        let ladder_audit = LadderPlanAudit::new(&plan);
        let risk_snapshot = pe_risk_engine::RiskSnapshot {
            leader_exposure_bps: pe_core_types::BasisPoints::ZERO,
            market_exposure_bps: pe_core_types::BasisPoints::ZERO,
            family_exposure_bps: pe_core_types::BasisPoints::ZERO,
            total_copy_exposure_bps: pe_core_types::BasisPoints::ZERO,
            intraday_pnl_bps: pe_core_types::BasisPoints::ZERO,
            rolling_7d_pnl_bps: pe_core_types::BasisPoints::ZERO,
            absolute_pnl_bps: pe_core_types::BasisPoints::ZERO,
            copy_latency_kill_switch_active: false,
            proposed_trade_bps: pe_core_types::BasisPoints(10),
            per_trade_cap_bps: 25,
            concentration_caps: None,
        };
        let economic = EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: PolymarketConditionId("condition".to_owned()),
                outcome_index: 0,
                token_id: PolymarketTokenId("11".to_owned()),
                side: pe_core_types::Side::Buy,
                market_id: "condition".to_owned(),
            },
            admission: admission_audit,
            ladder: ladder_audit,
            book_receipt: receipt,
            observation: None,
            sizing: SizingAudit {
                mode: SizingModeAudit::Contract { contracts: 5 },
                budget: CollateralAmount::from_atomic(10_000_000),
                principal: plan.worst_case_debit,
                minimum_shares: plan.shares,
                expected_shares: plan.shares,
                expected_vwap: plan.vwap().unwrap(),
                all_in_price: plan.limit_price,
                slippage_rate: Decimal::ZERO,
            },
            fee: FeeAudit {
                schedule: pe_venue_polymarket::CompactFeeSchedule::Zero,
                expected_fee: CollateralAmount::ZERO,
                reserve: CollateralAmount::ZERO,
            },
            risk: RiskAudit {
                financial_prefix: receipt,
                snapshot: risk_snapshot,
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 0,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_atomic(10_000_000),
                worst_case_debit: plan.worst_case_debit,
                price_impact_cap_bps: 100,
                chase_ceiling: plan.limit_price,
                band_floor: Price::ZERO,
                band_ceiling_exclusive: Price::ONE,
            },
            applied_configuration_hash: identity.config_hash.clone(),
        };
        LiveOrderRequest {
            target: FrozenLiveTarget {
                account_id: account_id.clone(),
                credential_binding: CredentialBindingIdentity {
                    version: 7,
                    key_id: "key-7".to_owned(),
                },
            },
            current_credential_binding: CredentialBindingIdentity {
                version: 7,
                key_id: "key-7".to_owned(),
            },
            mode: LiveModeSnapshot {
                requested: LiveControlMode::LiveTiny,
                effective: LiveControlMode::LiveTiny,
            },
            identity,
            condition_id: PolymarketConditionId("condition".to_owned()),
            outcome_id: OutcomeId(0),
            token_id: PolymarketTokenId("11".to_owned()),
            admission: LiveAdmissionArtifact {
                market,
                settlement,
                fee_schedule,
                receipts,
            },
            ladder: plan,
            economic,
        }
    }

    fn prepared_order(request: &LiveVenuePrepareRequest, spender: &str) -> PreparedPolymarketBuy {
        PreparedPolymarketBuy {
            condition_id: request.condition_id.clone(),
            outcome_id: request.outcome_id,
            token_id: request.token_id.clone(),
            maker: "maker".to_owned(),
            signer: "signer".to_owned(),
            funder: "funder".to_owned(),
            verifying_contract: spender.to_owned(),
            spender: spender.to_owned(),
            exchange_domain_version: 2,
            neg_risk: request.neg_risk,
            side: "BUY".to_owned(),
            salt: "1".to_owned(),
            timestamp_ms: 1,
            expiration: "0".to_owned(),
            maker_collateral: request.maximum_collateral,
            taker_shares: request.shares,
            limit_price: request.limit_price,
            minimum_tick_size: request.tick_size,
            signature_type: 3,
            order_type: "FOK".to_owned(),
            post_only: false,
            defer_exec: false,
            metadata: "0x00".to_owned(),
            builder: "0x00".to_owned(),
            order_hash: "order-hash".to_owned(),
            post_body_hash: "post-body-hash".to_owned(),
            sdk_version: "fixture-sdk".to_owned(),
            sdk_archive_sha256: "fixture-sdk-hash".to_owned(),
            metadata_hashes: request.metadata_hashes.clone(),
            worst_case_debit: request.maximum_collateral,
        }
    }

    async fn execute(
        classification: LivePostClassification,
    ) -> (LiveOrderOutcome, Vec<crate::LiveJournalEvent>, usize, usize) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(classification);
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = AccountId::new("account").unwrap();
        let prepared = match executor.prepare(request(&account_id), now()).await.unwrap() {
            LivePrepareResult::Prepared(prepared) => prepared,
            LivePrepareResult::Terminal(outcome) => {
                unreachable!("fixture admission unexpectedly failed: {outcome:?}")
            }
        };
        let outcome = executor.submit(prepared, now()).await.unwrap();
        let events = replay_account(&path, &account_id).unwrap();
        (
            outcome,
            events,
            venue.posts.load(Ordering::SeqCst),
            venue.reconciliations.load(Ordering::SeqCst),
        )
    }

    fn assert_common_terminal_evidence(events: &[crate::LiveJournalEvent]) {
        assert!(matches!(
            events,
            [
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::AdmissionEvaluated(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderPrepared(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderPosted(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderReconciled(_),
                    ..
                }
            ]
        ));
    }

    #[tokio::test]
    async fn matched_post_is_typed_and_fully_journaled() {
        let transaction_hash = format!("0x{}", "11".repeat(32));
        let (outcome, events, posts, reconciliations) = execute(LivePostClassification::Matched {
            venue_order_id: "venue-1".to_owned(),
            transaction_hashes: vec![transaction_hash.clone()],
            executed: LiveExecutedAmounts {
                making_amount: dec!(4.00),
                taking_amount: dec!(10),
            },
        })
        .await;
        assert!(matches!(
            &outcome,
            LiveOrderOutcome::Matched {
                transaction_hashes,
                executed: Some(LiveExecutedAmounts {
                    making_amount,
                    taking_amount,
                }),
                ..
            } if transaction_hashes.len() == 1
                && transaction_hashes.first() == Some(&transaction_hash)
                && *making_amount == dec!(4.00)
                && *taking_amount == dec!(10)
        ));
        assert_eq!(outcome.dispatch_state(), "submitted");
        assert_eq!(outcome.terminal_reason(), None);
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 0);
        assert_common_terminal_evidence(&events);
    }

    /// PASS: account admission, Prepared, and POST facts sample the injected clock after their
    /// respective awaited boundaries instead of reusing the tick's first instant.
    #[tokio::test]
    async fn durable_execution_facts_use_boundary_clocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: Some("venue-clock".to_owned()),
        });
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = AccountId::new("clock-account").unwrap();
        let ticks = AtomicUsize::new(0);
        let clock = || {
            now()
                + time::Duration::seconds(
                    i64::try_from(ticks.fetch_add(1, Ordering::SeqCst)).unwrap(),
                )
        };
        let prepared = match executor
            .prepare_with_clock(request(&account_id), &clock)
            .await
            .unwrap()
        {
            LivePrepareResult::Prepared(prepared) => prepared,
            LivePrepareResult::Terminal(outcome) => {
                unreachable!("fixture admission unexpectedly failed: {outcome:?}")
            }
        };
        executor.submit_with_clock(prepared, &clock).await.unwrap();

        let events = replay_account(&path, &account_id).unwrap();
        assert_eq!(events[0].timestamp, now() + time::Duration::seconds(1));
        assert_eq!(events[1].timestamp, now() + time::Duration::seconds(2));
        assert_eq!(events[2].timestamp, now() + time::Duration::seconds(3));
        assert_eq!(events[3].timestamp, now() + time::Duration::seconds(3));
    }

    /// PASS: a service-observed account snapshot reaches admission and preparation without a
    /// second venue read that could age the freshly checked risk evidence.
    #[tokio::test]
    async fn observed_account_prepare_has_no_later_account_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: None,
        });
        *venue.account.lock().unwrap() = Err(LiveVenueAccountReadError {
            kind: LiveAccountReadFailure::Authentication,
            evidence: Vec::new(),
            request_descriptor_hashes: Vec::new(),
        });
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = AccountId::new("observed-account").unwrap();

        assert!(matches!(
            executor
                .prepare_with_observed_account_and_clock(
                    request(&account_id),
                    account_state(),
                    now,
                )
                .await
                .unwrap(),
            LivePrepareResult::Prepared(_)
        ));
    }

    /// PASS: all three crash seams between synchronized Approved admission and synchronized
    /// Prepared resume the frozen identity without appending a second admission.
    /// FAIL: any crash seam appends a second Approved admission or resumes under a different
    /// identity.
    #[tokio::test]
    async fn approved_admission_crash_seams_resume_without_duplicate_admission() {
        let source_dir = tempdir().unwrap();
        let source_path = source_dir.path().join("source.log");
        let source_journal = LiveJournal::open(&source_path).unwrap();
        let source_venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: None,
        });
        let source_executor = LiveExecutor::new(&source_venue, &source_journal);
        let account_id = AccountId::new("resume-account").unwrap();
        let _ = source_executor
            .prepare(request(&account_id), now())
            .await
            .unwrap();
        let source_events = replay_account(&source_path, &account_id).unwrap();
        let admission = source_events
            .iter()
            .find_map(|event| match &event.payload {
                LiveJournalPayload::AdmissionEvaluated(admission) => Some(admission.clone()),
                _ => None,
            })
            .unwrap();

        for seam in [
            "after-admission-append",
            "during-venue-preparation",
            "before-prepared-append",
        ] {
            let dir = tempdir().unwrap();
            let path = dir.path().join(format!("{seam}.log"));
            let journal = LiveJournal::open(&path).unwrap();
            journal
                .append(
                    account_id.clone(),
                    now(),
                    LiveJournalPayload::AdmissionEvaluated(admission.clone()),
                )
                .unwrap();
            let venue = FixtureVenue::new(LivePostClassification::Killed {
                venue_order_id: None,
            });
            let executor = LiveExecutor::new(&venue, &journal);
            assert!(matches!(
                executor
                    .resume_approved_admission_with_clock(
                        account_id.clone(),
                        admission.clone(),
                        LiveModeSnapshot {
                            requested: LiveControlMode::LiveTiny,
                            effective: LiveControlMode::LiveTiny,
                        },
                        admission.frozen_binding.clone(),
                        account_state(),
                        now,
                    )
                    .await
                    .unwrap(),
                LivePrepareResult::Prepared(_)
            ));
            let events = replay_account(&path, &account_id).unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| {
                        matches!(&event.payload, LiveJournalPayload::AdmissionEvaluated(_))
                    })
                    .count(),
                1,
                "{seam}"
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| {
                        matches!(&event.payload, LiveJournalPayload::OrderPrepared(_))
                    })
                    .count(),
                1,
                "{seam}"
            );
        }
    }

    /// PASS: resumption evaluates the current control mode and terminalizes an old Approved
    /// admission without venue preparation or POST when the account is now off.
    #[tokio::test]
    async fn approved_admission_resume_requires_current_live_tiny_modes() {
        let source_dir = tempdir().unwrap();
        let source_path = source_dir.path().join("source.log");
        let source_journal = LiveJournal::open(&source_path).unwrap();
        let source_venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: None,
        });
        let source_executor = LiveExecutor::new(&source_venue, &source_journal);
        let account_id = AccountId::new("resume-off").unwrap();
        let _ = source_executor
            .prepare(request(&account_id), now())
            .await
            .unwrap();
        let admission = replay_account(&source_path, &account_id)
            .unwrap()
            .into_iter()
            .find_map(|event| match event.payload {
                LiveJournalPayload::AdmissionEvaluated(admission) => Some(admission),
                _ => None,
            })
            .unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        journal
            .append(
                account_id.clone(),
                now(),
                LiveJournalPayload::AdmissionEvaluated(admission.clone()),
            )
            .unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: None,
        });
        let executor = LiveExecutor::new(&venue, &journal);
        let outcome = executor
            .resume_approved_admission_with_clock(
                account_id.clone(),
                admission.clone(),
                LiveModeSnapshot {
                    requested: LiveControlMode::Off,
                    effective: LiveControlMode::Off,
                },
                admission.frozen_binding.clone(),
                account_state(),
                now,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, LivePrepareResult::Terminal(_)));
        assert_eq!(venue.posts.load(Ordering::SeqCst), 0);
        assert!(matches!(
            replay_account(&path, &account_id).unwrap().as_slice(),
            [
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::AdmissionEvaluated(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderPreparationFailed(failed),
                    ..
                }
            ] if failed.failure == LiveOrderPreparationFailure::RecoveryAdmissionExpired
        ));
    }

    #[tokio::test]
    async fn killed_post_is_typed_and_fully_journaled() {
        let (outcome, events, posts, reconciliations) = execute(LivePostClassification::Killed {
            venue_order_id: Some("venue-1".to_owned()),
        })
        .await;
        assert!(matches!(outcome, LiveOrderOutcome::Killed { .. }));
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 0);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn rejected_post_is_typed_and_fully_journaled() {
        let (outcome, events, posts, reconciliations) = execute(LivePostClassification::Rejected {
            venue_order_id: None,
        })
        .await;
        assert!(matches!(outcome, LiveOrderOutcome::Rejected { .. }));
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 0);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn ambiguous_post_freezes_and_reconciles_before_another_order() {
        let (outcome, events, posts, reconciliations) =
            execute(LivePostClassification::Ambiguous {
                kind: LiveOrderAmbiguityKind::UnexpectedResponse,
            })
            .await;
        assert!(matches!(
            outcome,
            LiveOrderOutcome::Ambiguous {
                reconcile_first: true,
                ..
            }
        ));
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 1);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn crash_between_preparation_and_post_is_distinguishable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Matched {
            venue_order_id: "venue-1".to_owned(),
            transaction_hashes: Vec::new(),
            executed: LiveExecutedAmounts {
                making_amount: dec!(4.00),
                taking_amount: dec!(10),
            },
        });
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = AccountId::new("account").unwrap();
        assert!(matches!(
            executor.prepare(request(&account_id), now()).await.unwrap(),
            LivePrepareResult::Prepared(_)
        ));
        assert_eq!(venue.posts.load(Ordering::SeqCst), 0);
        let events = replay_account(&path, &account_id).unwrap();
        assert!(matches!(
            events.last().map(|event| &event.payload),
            Some(LiveJournalPayload::OrderPrepared(_))
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.payload, LiveJournalPayload::OrderPosted(_)))
        );
    }

    async fn assert_refusal_result(
        request: LiveOrderRequest,
        account: Result<LiveVenueAccountState, LiveVenueAccountReadError>,
        expected: LiveAdmissionRefusal,
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: None,
        });
        *venue.account.lock().unwrap() = account;
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = request.target.account_id.clone();
        let outcome = executor.prepare(request, now()).await.unwrap();
        assert!(matches!(
            outcome,
            LivePrepareResult::Terminal(LiveOrderOutcome::Refused { reason }) if reason == expected.clone()
        ));
        assert_eq!(venue.posts.load(Ordering::SeqCst), 0);
        let events = replay_account(&path, &account_id).unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            LiveJournalPayload::AdmissionEvaluated(audit)
                if audit.verdict == LiveAdmissionVerdict::Refused(expected.clone())
        )));
        if expected == LiveAdmissionRefusal::CredentialVersionChanged {
            assert!(events.iter().any(|event| matches!(
                event.payload,
                LiveJournalPayload::CredentialBindingMismatch { .. }
            )));
        }
    }

    async fn assert_refusal(
        request: LiveOrderRequest,
        account: LiveVenueAccountState,
        expected: LiveAdmissionRefusal,
    ) {
        assert_refusal_result(request, Ok(account), expected).await;
    }

    #[tokio::test]
    async fn credential_mismatch_is_terminal_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.current_credential_binding.version += 1;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::CredentialVersionChanged,
        )
        .await;
    }

    #[tokio::test]
    async fn stale_artifact_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.admission.market.observed_at_unix -= 61;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::MarketEvidenceStale,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_market_artifact_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.admission.market.schema_version = 0;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::MarketEvidenceInvalid,
        )
        .await;
    }

    #[tokio::test]
    async fn stale_settlement_artifact_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.admission.settlement.observed_at_unix -= 61;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::SettlementEvidenceStale,
        )
        .await;
    }

    #[tokio::test]
    async fn resolved_and_ambiguous_settlements_are_distinct_refusals() {
        let account_id = AccountId::new("account").unwrap();
        let mut resolved = request(&account_id);
        resolved.admission.settlement.status =
            VenueResolutionStatus::ResolvedWinner { outcome_index: 0 };
        assert_refusal(
            resolved,
            account_state(),
            LiveAdmissionRefusal::MarketAlreadyResolved,
        )
        .await;

        let mut ambiguous = request(&account_id);
        ambiguous.admission.settlement.status = VenueResolutionStatus::ResolvedAmbiguous;
        assert_refusal(
            ambiguous,
            account_state(),
            LiveAdmissionRefusal::SettlementAmbiguous,
        )
        .await;
    }

    #[tokio::test]
    async fn artifact_identity_mismatch_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.token_id = PolymarketTokenId("different-token".to_owned());
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::ArtifactIdentityMismatch,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_ladder_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.ladder.used_asks.clear();
        assert_refusal(input, account_state(), LiveAdmissionRefusal::LadderInvalid).await;
    }

    #[test]
    fn improved_ladder_accepts_expected_quantity_above_signed_minimum() {
        let plan = LadderPlan {
            used_asks: vec![
                AskLevel {
                    price: Price::new(dec!(0.49)).unwrap(),
                    shares: ShareAmount::from_decimal_exact(dec!(2)).unwrap(),
                },
                AskLevel {
                    price: Price::new(dec!(0.50)).unwrap(),
                    shares: ShareAmount::from_decimal_exact(dec!(3.02)).unwrap(),
                },
            ],
            best_ask: Price::new(dec!(0.49)).unwrap(),
            limit_price: Price::new(dec!(0.50)).unwrap(),
            shares: ShareAmount::from_decimal_exact(dec!(4.9999)).unwrap(),
            worst_case_debit: CollateralAmount::from_decimal_exact(dec!(2.50)).unwrap(),
        };
        assert!(
            validate_ladder(
                &LadderPlanAudit::new(&plan),
                Price::new(dec!(0.01)).unwrap(),
                ShareAmount::from_whole(1).unwrap(),
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn account_gates_require_principal_plus_fee_reserve() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        let reserve = CollateralAmount::from_atomic(120);
        input.economic.fee.reserve = reserve;
        input.economic.balance.worst_case_debit = input
            .economic
            .sizing
            .principal
            .checked_add(reserve)
            .unwrap();
        let mut account = account_state();
        account.collateral_balance = input.economic.sizing.principal;
        assert_refusal(
            input,
            account,
            LiveAdmissionRefusal::InsufficientBalance {
                required: CollateralAmount::from_atomic(2_500_120),
                available: CollateralAmount::from_atomic(2_500_000),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn balance_audit_must_equal_derived_all_in_requirement() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.economic.fee.reserve = CollateralAmount::from_atomic(120);
        assert_refusal(input, account_state(), LiveAdmissionRefusal::LadderInvalid).await;
    }

    #[tokio::test]
    async fn unavailable_account_state_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let evidence = vec![RawHttpAttempt::Response(raw_response(
            "account-state-error",
        ))];
        assert_refusal_result(
            request(&account_id),
            Err(LiveVenueAccountReadError {
                kind: LiveAccountReadFailure::Authentication,
                evidence,
                request_descriptor_hashes: Vec::new(),
            }),
            LiveAdmissionRefusal::AccountStateUnavailable(LiveAccountReadFailure::Authentication),
        )
        .await;
    }

    #[tokio::test]
    async fn closed_only_and_geoblock_are_distinct_refusals() {
        let account_id = AccountId::new("account").unwrap();
        let mut closed = account_state();
        closed.closed_only = true;
        assert_refusal(
            request(&account_id),
            closed,
            LiveAdmissionRefusal::AccountClosedOnly,
        )
        .await;
        let mut blocked = account_state();
        blocked.geoblocked = true;
        assert_refusal(
            request(&account_id),
            blocked,
            LiveAdmissionRefusal::Geoblocked,
        )
        .await;
    }

    #[tokio::test]
    async fn insufficient_allowance_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut account = account_state();
        account.allowance = CollateralAmount::from_atomic(2_499_999);
        assert_refusal(
            request(&account_id),
            account,
            LiveAdmissionRefusal::InsufficientAllowance {
                required: CollateralAmount::from_atomic(2_500_000),
                available: CollateralAmount::from_atomic(2_499_999),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn insufficient_balance_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut account = account_state();
        account.collateral_balance = CollateralAmount::from_atomic(2_499_999);
        assert_refusal(
            request(&account_id),
            account,
            LiveAdmissionRefusal::InsufficientBalance {
                required: CollateralAmount::from_atomic(2_500_000),
                available: CollateralAmount::from_atomic(2_499_999),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn worst_case_debit_over_free_collateral_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut account = account_state();
        account.reconciled_free_collateral = CollateralAmount::from_atomic(2_499_999);
        assert_refusal(
            request(&account_id),
            account,
            LiveAdmissionRefusal::WorstCaseDebitExceedsFreeCollateral {
                required: CollateralAmount::from_atomic(2_500_000),
                available: CollateralAmount::from_atomic(2_499_999),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn per_account_kill_refuses_when_not_armed() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.mode.requested = LiveControlMode::Off;
        assert_refusal(input, account_state(), LiveAdmissionRefusal::ModeNotArmed).await;
    }
}
