//! Pure campaign reducer and durable journal for the isolated live canary.

use std::path::Path;
use std::{fs, io};

use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

use pe_core_types::{
    CanaryOrigin, CollateralAmount, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price,
    RawArtifactObservation, RawEvidence, RawHttpResponse, ReceivedAt, ShareAmount, SourceId,
    SourceTimestamp, TransportErrorClass,
};
use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
use pe_resolver_card::MarketFamily;
use pe_risk_engine::CanaryRiskSnapshot;
use pe_strategy_winner_follow::{
    OrganicCanaryPolicy, OrganicDecisionProof, organic_decision_proof_hash,
};
use pe_venue_core::ExactExecutionReport;
use pe_venue_polymarket::PreparedPolymarketBuy;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const CAMPAIGN_START_COLLATERAL: CollateralAmount = CollateralAmount::from_atomic(400_000_000);
pub const CAMPAIGN_MAX_COMMITMENT: CollateralAmount = CollateralAmount::from_atomic(8_000_000);
pub const CAMPAIGN_MAX_ALLOWANCE: CollateralAmount = CollateralAmount::from_atomic(8_000_000);
pub const PROBE_POST_LIMIT: u8 = 3;
pub const ORGANIC_POST_LIMIT: u8 = 5;
pub const CAMPAIGN_MAX_DURATION_SECS: i64 = 7 * 24 * 60 * 60;

/// Stable commitment to the complete sanitized response identity and exact body bytes.
pub fn response_evidence_hash(response: &RawHttpResponse) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    struct EvidenceCommitment<'a> {
        domain: &'static str,
        source_id: &'a str,
        endpoint_kind: &'a str,
        method: &'a str,
        path: &'a str,
        ordered_query: &'a [(String, String)],
        status: u16,
        headers: &'a [(String, String)],
        attempt_ordinal: u32,
        source_at: Option<OffsetDateTime>,
        observed_at: OffsetDateTime,
        received_at: OffsetDateTime,
        schema_version: u16,
        parser_version: u16,
        adapter_version: &'a str,
        body_hash: String,
    }
    let commitment = EvidenceCommitment {
        domain: "prediction-edge/http-response-evidence/v1",
        source_id: &response.source_id,
        endpoint_kind: &response.endpoint_kind,
        method: &response.method,
        path: &response.path,
        ordered_query: &response.ordered_query,
        status: response.status,
        headers: &response.headers,
        attempt_ordinal: response.attempt_ordinal,
        source_at: response.source_at,
        observed_at: response.observed_at,
        received_at: response.received_at,
        schema_version: response.schema_version,
        parser_version: response.parser_version,
        adapter_version: &response.adapter_version,
        body_hash: blake3::hash(&response.body).to_hex().to_string(),
    };
    serde_json::to_vec(&commitment).map(|bytes| blake3::hash(&bytes).to_hex().to_string())
}

/// Stable commitment to a complete non-HTTP artifact observation.
pub fn artifact_evidence_hash(
    artifact: &RawArtifactObservation,
) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    struct ArtifactCommitment<'a> {
        domain: &'static str,
        source_id: &'a str,
        artifact_kind: &'a str,
        path: &'a str,
        observed_at: OffsetDateTime,
        received_at: OffsetDateTime,
        schema_version: u16,
        parser_version: u16,
        adapter_version: &'a str,
        body_hash: String,
    }
    let commitment = ArtifactCommitment {
        domain: "prediction-edge/artifact-evidence/v1",
        source_id: &artifact.source_id,
        artifact_kind: &artifact.artifact_kind,
        path: &artifact.path,
        observed_at: artifact.observed_at,
        received_at: artifact.received_at,
        schema_version: artifact.schema_version,
        parser_version: artifact.parser_version,
        adapter_version: &artifact.adapter_version,
        body_hash: blake3::hash(&artifact.body).to_hex().to_string(),
    };
    serde_json::to_vec(&commitment).map(|bytes| blake3::hash(&bytes).to_hex().to_string())
}

/// Return the stable hash for evidence that contains replayable bytes.
/// Transport failures have no response body and therefore no content hash.
pub fn raw_evidence_hash(evidence: &RawEvidence) -> Result<Option<String>, serde_json::Error> {
    match evidence {
        RawEvidence::HttpResponse(response) => response_evidence_hash(response).map(Some),
        RawEvidence::Artifact(artifact) => artifact_evidence_hash(artifact).map(Some),
        RawEvidence::HttpTransportFailure(_) => Ok(None),
    }
}

pub type AttemptOrigin = CanaryOrigin;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignStage {
    #[default]
    Inactive,
    ProbesArmed,
    OrganicReady,
    OrganicArmed,
    ClosedObserving,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptPhase {
    Reserved,
    PostInFlight,
    Review,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosureReason {
    Killed,
    Expired,
    Exhausted,
    ProbeFailed,
    OrganicAttemptFailed,
    VenueAmbiguous,
    UnexpectedOrderState,
    CancellationFailed,
    AccountingDrift,
    AuthorizationInvalidated,
    ExternalActivity,
    Geoblocked,
    JurisdictionInvalidated,
    AccountClosedOnly,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandReceipt {
    pub command_id: String,
    pub command_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAuthorization {
    pub schema_version: u16,
    pub campaign_id: String,
    pub implementation_commit: String,
    pub binary_hash: String,
    pub config_hash: String,
    pub resolver_inventory_hash: String,
    pub sdk_archive_sha256: String,
    pub sdk_effective_vendor_tree_sha256: String,
    pub wallet: String,
    pub owner_signer: String,
    pub spender: String,
    pub jurisdiction: String,
    pub jurisdiction_attestation_hash: String,
    pub account_attestation_hash: String,
    pub issued_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
    pub starting_collateral: CollateralAmount,
    pub allowance: CollateralAmount,
    pub commitment_cap: CollateralAmount,
    pub probe_slots: u8,
    pub organic_slots: u8,
}

impl CampaignAuthorization {
    pub fn validate(&self, now: OffsetDateTime) -> Result<(), CanaryStateError> {
        let duration = (self.expires_at - self.issued_at).whole_seconds();
        if self.schema_version != 1
            || self.campaign_id.trim().is_empty()
            || self.implementation_commit.trim().is_empty()
            || self.binary_hash.trim().is_empty()
            || self.config_hash.trim().is_empty()
            || self.resolver_inventory_hash.trim().is_empty()
            || self.sdk_archive_sha256.trim().is_empty()
            || self.sdk_effective_vendor_tree_sha256.trim().is_empty()
            || self.wallet.trim().is_empty()
            || self.owner_signer.trim().is_empty()
            || self.spender.trim().is_empty()
            || self.jurisdiction.trim().is_empty()
            || self.jurisdiction_attestation_hash.trim().is_empty()
            || self.account_attestation_hash.trim().is_empty()
            || self.starting_collateral != CAMPAIGN_START_COLLATERAL
            || self.allowance > CAMPAIGN_MAX_ALLOWANCE
            || self.commitment_cap != CAMPAIGN_MAX_COMMITMENT
            || self.probe_slots != PROBE_POST_LIMIT
            || self.organic_slots != ORGANIC_POST_LIMIT
            || duration <= 0
            || duration > CAMPAIGN_MAX_DURATION_SECS
            || now < self.issued_at
            || now >= self.expires_at
        {
            return Err(CanaryStateError::AuthorityMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeAuthorization {
    pub schema_version: u16,
    pub campaign_id: String,
    pub campaign_authorization_hash: String,
    pub probe_ordinal: u8,
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub shares: ShareAmount,
    pub worst_price: Price,
    pub maximum_collateral: CollateralAmount,
    pub resolver_card_hash: String,
    pub authority_hash: String,
    pub expires_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryQuote {
    pub origin: AttemptOrigin,
    pub request_identity: String,
    pub snapshot_raw_hash: String,
    pub snapshot_observed_at_ms: u64,
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub full_ask_ladder_hash: String,
    pub best_ask: Price,
    pub executable_ask: Price,
    pub origin_price_ceiling: Price,
    pub kelly_cost: Option<Price>,
    pub minimum_fill_price: Price,
    pub maximum_fill_price_exclusive: Price,
    pub minimum_tick_size: Price,
    pub minimum_order_size: ShareAmount,
    pub shares: ShareAmount,
    pub maximum_collateral: CollateralAmount,
    pub metadata_hashes: Vec<String>,
    pub expires_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryAdmission {
    pub identity: String,
    pub campaign_id: String,
    pub origin: AttemptOrigin,
    pub neutral_request_hash: String,
    pub resolver_card_hash: String,
    pub quote: CanaryQuote,
    pub risk: CanaryRiskSnapshot,
    pub attribution: AttemptAttribution,
    pub probe_authorization: Option<ProbeAuthorization>,
    pub organic_decision_proof: Option<OrganicDecisionProof>,
}

/// The minimum durable identity needed to derive the campaign's concentration buckets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptAttribution {
    /// Present only for ordinary Winner-Follow attempts; probes have no leader signal.
    pub leader: Option<String>,
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub family: MarketFamily,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryReconciliation {
    pub observed_at: OffsetDateTime,
    pub geoblocked: bool,
    pub closed_only: bool,
    pub free_collateral: CollateralAmount,
    pub allowance: CollateralAmount,
    pub standard_spender_only: bool,
    pub open_order_ids: Vec<String>,
    pub all_trade_ids: Vec<String>,
    pub position_count: usize,
    pub positions: Vec<CanaryPosition>,
    pub resolutions: Vec<CanaryResolution>,
    pub unexpected_activity: bool,
    pub execution_report: Option<ExactExecutionReport>,
    pub evidence_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryResolution {
    pub condition_id: PolymarketConditionId,
    pub winner: OutcomeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryPosition {
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub shares: ShareAmount,
}

impl CanaryAdmission {
    pub fn validate(
        &self,
        state: &CanaryCampaignState,
        prepared: &PreparedPolymarketBuy,
        now: OffsetDateTime,
        reconciliation: &CanaryReconciliation,
    ) -> Result<(), CanaryStateError> {
        state.require_campaign(&self.campaign_id)?;
        let now_ms = now
            .unix_timestamp_nanos()
            .checked_div(1_000_000)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(CanaryStateError::AdmissionMismatch)?;
        let exposure = state.exposure_amounts(&self.attribution)?;
        let expected_leader_exposure = self
            .attribution
            .leader
            .as_ref()
            .map(|_| {
                pe_risk_engine::exposure_bps_ceil(exposure.leader, state.canary_bankroll)
                    .ok_or(CanaryStateError::Arithmetic)
            })
            .transpose()?;
        let expected_market_exposure =
            pe_risk_engine::exposure_bps_ceil(exposure.market, state.canary_bankroll)
                .ok_or(CanaryStateError::Arithmetic)?;
        let expected_family_exposure =
            pe_risk_engine::exposure_bps_ceil(exposure.family, state.canary_bankroll)
                .ok_or(CanaryStateError::Arithmetic)?;
        let expected_total_exposure =
            pe_risk_engine::exposure_bps_ceil(exposure.total, state.canary_bankroll)
                .ok_or(CanaryStateError::Arithmetic)?;
        let loss = state
            .starting_collateral
            .checked_sub(state.free_collateral)
            .unwrap_or(CollateralAmount::ZERO);
        let expected_drawdown = pe_risk_engine::exposure_bps_ceil(loss, state.starting_collateral)
            .map(|drawdown| pe_core_types::BasisPoints(-drawdown.0))
            .unwrap_or(pe_core_types::BasisPoints(i32::MIN));
        let organic_proof_valid = match (&self.origin, &self.organic_decision_proof) {
            (AttemptOrigin::OperatorProbe, None) => true,
            (AttemptOrigin::Organic, Some(proof)) => {
                let leader = proof.signal.leader.to_string();
                organic_decision_proof_hash(proof).ok().as_deref()
                    == Some(self.neutral_request_hash.as_str())
                    && proof.idempotency_key == self.identity
                    && proof.evidence_hashes == self.quote.metadata_hashes
                    && proof.signal.market_id.0.0 == self.quote.condition_id.0
                    && proof.signal.outcome_id == self.quote.outcome_id
                    && self.attribution.leader.as_deref() == Some(leader.as_str())
                    && OrganicCanaryPolicy
                        .evaluate(
                            &proof.signal,
                            proof.probability,
                            state.canary_bankroll,
                            self.quote.minimum_tick_size,
                        )
                        .is_ok_and(|evaluated| {
                            evaluated.intent.idempotency_key == proof.idempotency_key
                                && evaluated.intent.market_id.0.0 == self.quote.condition_id.0
                                && evaluated.intent.outcome_id == self.quote.outcome_id
                                && evaluated.shares == self.quote.shares
                                && evaluated.kelly_cost == self.quote.origin_price_ceiling
                                && evaluated.maximum_collateral >= self.quote.maximum_collateral
                        })
            }
            _ => false,
        };
        if self.identity.trim().is_empty()
            || self.neutral_request_hash.trim().is_empty()
            || self.resolver_card_hash.trim().is_empty()
            || !organic_proof_valid
            || self.quote.origin != self.origin
            || self.quote.request_identity != self.identity
            || self.quote.snapshot_raw_hash.trim().is_empty()
            || self.quote.full_ask_ladder_hash.trim().is_empty()
            || self.quote.metadata_hashes.is_empty()
            || now >= self.quote.expires_at
            || state.expires_at.is_none_or(|expires_at| now >= expires_at)
            || now_ms < self.quote.snapshot_observed_at_ms
            || now_ms - self.quote.snapshot_observed_at_ms > 2_000
            || self.quote.best_ask > self.quote.executable_ask
            || self.quote.executable_ask > self.quote.origin_price_ceiling
            || match self.origin {
                AttemptOrigin::OperatorProbe => self.quote.kelly_cost.is_some(),
                AttemptOrigin::Organic => {
                    self.quote.kelly_cost != Some(self.quote.origin_price_ceiling)
                }
            }
            || self.quote.executable_ask < self.quote.minimum_fill_price
            || self.quote.executable_ask >= self.quote.maximum_fill_price_exclusive
            || self.quote.shares < self.quote.minimum_order_size
            || self.quote.condition_id != prepared.condition_id
            || self.quote.outcome_id != prepared.outcome_id
            || self.quote.token_id != prepared.token_id
            || self.quote.shares != prepared.taker_shares
            || self.quote.executable_ask != prepared.limit_price
            || self.quote.minimum_tick_size != prepared.minimum_tick_size
            || self.quote.maximum_collateral != prepared.worst_case_debit
            || self.quote.maximum_collateral != prepared.maker_collateral
            || self.quote.metadata_hashes != prepared.metadata_hashes
            || prepared.exchange_domain_version != 2
            || prepared.neg_risk
            || prepared.side != "BUY"
            || self.attribution.condition_id != prepared.condition_id
            || self.attribution.outcome_id != prepared.outcome_id
            || match self.origin {
                AttemptOrigin::OperatorProbe => self.attribution.leader.is_some(),
                AttemptOrigin::Organic => self
                    .attribution
                    .leader
                    .as_ref()
                    .is_none_or(|leader| leader.trim().is_empty()),
            }
            || self.quote.executable_ask.0 % self.quote.minimum_tick_size.0
                != rust_decimal::Decimal::ZERO
            || self.risk.origin != self.origin
            || self.risk.proposed_worst_case_debit != prepared.worst_case_debit
            || self.risk.canary_bankroll != state.canary_bankroll
            || self.risk.leader_exposure_bps != expected_leader_exposure
            || self.risk.market_exposure_bps != expected_market_exposure
            || self.risk.family_exposure_bps != expected_family_exposure
            || self.risk.total_copy_exposure_bps != expected_total_exposure
            || self.risk.open_exposure_bps != expected_total_exposure
            || self.risk.drawdown_bps != expected_drawdown
            || self.risk.pending_reservation != state.pending.is_some()
            || self.risk.allowance != state.allowance
            || self.risk.account_state_fresh
                != ((now - reconciliation.observed_at).whole_seconds().abs() <= 30)
            || self.risk.venue_reconciliation_fresh
                != ((now - reconciliation.observed_at).whole_seconds().abs() <= 30)
            || self.risk.geoblock_fresh
                != ((now - reconciliation.observed_at).whole_seconds().abs() <= 30)
            || self.risk.closed_only_fresh
                != ((now - reconciliation.observed_at).whole_seconds().abs() <= 30)
            || self.risk.geoblocked != reconciliation.geoblocked
            || self.risk.closed_only != reconciliation.closed_only
            || self.risk.allowance != reconciliation.allowance
            || self.risk.standard_spender_only != reconciliation.standard_spender_only
            || reconciliation.unexpected_activity
            || !reconciliation.open_order_ids.is_empty()
            || state.wallet.as_deref() != Some(prepared.maker.as_str())
            || state.wallet.as_deref() != Some(prepared.signer.as_str())
            || state.wallet.as_deref() != Some(prepared.funder.as_str())
            || state.spender.as_deref() != Some(prepared.spender.as_str())
            || state.spender.as_deref() != Some(prepared.verifying_contract.as_str())
        {
            return Err(CanaryStateError::AdmissionMismatch);
        }
        if !matches!(
            pe_risk_engine::evaluate_canary_risk(&self.risk),
            pe_risk_engine::RiskDecision::Approved
        ) {
            return Err(CanaryStateError::RiskBlocked);
        }
        match (&self.origin, &self.probe_authorization) {
            (AttemptOrigin::OperatorProbe, Some(authority))
                if authority.schema_version == 1
                    && authority.campaign_id == self.campaign_id
                    && state.campaign_authorization_hash.as_deref()
                        == Some(authority.campaign_authorization_hash.as_str())
                    && authority.probe_ordinal == state.probe_posts.saturating_add(1)
                    && authority.condition_id == prepared.condition_id
                    && authority.outcome_id == prepared.outcome_id
                    && authority.token_id == prepared.token_id
                    && authority.shares == prepared.taker_shares
                    && authority.worst_price == self.quote.origin_price_ceiling
                    && authority.maximum_collateral
                        == CollateralAmount::from_decimal_exact(
                            authority.shares.to_decimal() * authority.worst_price.0,
                        )
                        .map_err(|_| CanaryStateError::Arithmetic)?
                    && prepared.worst_case_debit <= authority.maximum_collateral
                    && authority.resolver_card_hash == self.resolver_card_hash
                    && authority.authority_hash == probe_authority_hash(authority)?
                    && self.neutral_request_hash == authority.authority_hash
                    && now < authority.expires_at => {}
            (AttemptOrigin::Organic, None) => {}
            _ => return Err(CanaryStateError::AuthorityMismatch),
        }
        Ok(())
    }
}

/// Stable BLAKE3 identity for a probe authority. The declared hash field is omitted from the
/// payload so the authority is not self-referential.
pub fn probe_authority_hash(authority: &ProbeAuthorization) -> Result<String, CanaryStateError> {
    let payload = (
        authority.schema_version,
        &authority.campaign_id,
        &authority.campaign_authorization_hash,
        authority.probe_ordinal,
        &authority.condition_id,
        authority.outcome_id,
        &authority.token_id,
        authority.shares,
        authority.worst_price,
        authority.maximum_collateral,
        &authority.resolver_card_hash,
        authority.expires_at,
    );
    Ok(blake3::hash(&serde_json::to_vec(&payload)?)
        .to_hex()
        .to_string())
}

/// Stable BLAKE3 identity for the complete campaign authority consumed by the actor.
pub fn campaign_authorization_hash(
    authority: &CampaignAuthorization,
) -> Result<String, CanaryStateError> {
    Ok(blake3::hash(&serde_json::to_vec(authority)?)
        .to_hex()
        .to_string())
}

/// Stable ordered commitment to all operator-reviewed probe results and their raw evidence.
pub fn organic_evidence_bundle_hash(
    reviewed_probe_hashes: &[String],
) -> Result<String, CanaryStateError> {
    Ok(blake3::hash(&serde_json::to_vec(reviewed_probe_hashes)?)
        .to_hex()
        .to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrganicStageAuthorization {
    pub schema_version: u16,
    pub campaign_id: String,
    pub campaign_authorization_hash: String,
    pub reviewed_probe_count: u8,
    pub implementation_commit: String,
    pub evidence_bundle_hash: String,
    pub expires_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingAttempt {
    pub identity: String,
    pub origin: AttemptOrigin,
    pub worst_case_debit: CollateralAmount,
    pub requested_shares: ShareAmount,
    pub order_hash: String,
    pub prepared_body_hash: String,
    pub venue_order_id: Option<pe_core_types::VenueOrderId>,
    pub post_success: Option<bool>,
    pub phase: AttemptPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignAttemptRecord {
    pub identity: String,
    pub origin: AttemptOrigin,
    pub attribution: AttemptAttribution,
    pub shares: ShareAmount,
    /// Conservative unresolved cost. A reconciled no-fill reduces this to zero; a fill retains
    /// exact debit for the campaign because settlement/redemption is operator-reconciled.
    pub open_debit: CollateralAmount,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanaryExposureAmounts {
    pub leader: CollateralAmount,
    pub market: CollateralAmount,
    pub family: CollateralAmount,
    pub total: CollateralAmount,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCampaignState {
    pub campaign_id: Option<String>,
    pub campaign_authorization_hash: Option<String>,
    pub stage: CampaignStage,
    pub expires_at: Option<OffsetDateTime>,
    pub implementation_commit: Option<String>,
    pub binary_hash: Option<String>,
    pub config_hash: Option<String>,
    pub resolver_inventory_hash: Option<String>,
    pub wallet: Option<String>,
    pub owner_signer: Option<String>,
    pub spender: Option<String>,
    pub sdk_archive_sha256: Option<String>,
    pub sdk_effective_vendor_tree_sha256: Option<String>,
    pub starting_collateral: CollateralAmount,
    pub free_collateral: CollateralAmount,
    pub canary_bankroll: CollateralAmount,
    pub allowance: CollateralAmount,
    pub committed_debit: CollateralAmount,
    pub probe_posts: u8,
    pub organic_posts: u8,
    pub pending: Option<PendingAttempt>,
    pub attempts: Vec<CampaignAttemptRecord>,
    pub reviewed_probe_hashes: Vec<String>,
    pub kill_latched: bool,
    pub terminal_reason: Option<ClosureReason>,
    pub last_reconciliation_at: Option<OffsetDateTime>,
    pub reconciliation_failed: bool,
    pub recovery_unresolved: bool,
    pub shutdown_reconciliation_designated: bool,
    pub shutdown_reconciliation_succeeded: bool,
    pub known_trade_ids: Vec<String>,
    pub last_reconciliation: Option<CanaryReconciliation>,
    pub command_receipts: Vec<CommandReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CanaryEvent {
    CampaignArmed {
        authorization: CampaignAuthorization,
        receipt: CommandReceipt,
    },
    ProbeReviewAccepted {
        campaign_id: String,
        reviewed_probe_ordinal: u8,
        reviewed_probe_hash: String,
        receipt: CommandReceipt,
    },
    OrganicStageArmed {
        authorization: OrganicStageAuthorization,
        receipt: CommandReceipt,
    },
    PreReservationSkipped {
        identity: String,
        reason: String,
        evidence_hashes: Vec<String>,
        receipt: CommandReceipt,
    },
    AttemptReserved {
        admission: Box<CanaryAdmission>,
        prepared: PreparedPolymarketBuy,
        receipt: CommandReceipt,
    },
    PostInFlight {
        identity: String,
        order_hash: String,
        post_body_hash: String,
    },
    HttpResponseCaptured {
        identity: String,
        source_id: String,
        endpoint_kind: String,
        method: String,
        path: String,
        ordered_query: Vec<(String, String)>,
        attempt_ordinal: u32,
        status: u16,
        headers: Vec<(String, String)>,
        source_at: Option<OffsetDateTime>,
        observed_at: OffsetDateTime,
        received_at: OffsetDateTime,
        schema_version: u16,
        parser_version: u16,
        adapter_version: String,
        raw_body_hash: String,
        raw_body: Vec<u8>,
    },
    HttpTransportFailed {
        identity: String,
        source_id: String,
        endpoint_kind: String,
        method: String,
        path: String,
        ordered_query: Vec<(String, String)>,
        attempt_ordinal: u32,
        observed_at: OffsetDateTime,
        received_at: OffsetDateTime,
        error_class: TransportErrorClass,
        schema_version: u16,
        parser_version: u16,
        adapter_version: String,
    },
    ArtifactCaptured {
        identity: String,
        source_id: String,
        artifact_kind: String,
        path: String,
        observed_at: OffsetDateTime,
        received_at: OffsetDateTime,
        schema_version: u16,
        parser_version: u16,
        adapter_version: String,
        raw_body_hash: String,
        raw_body: Vec<u8>,
    },
    ReconciliationRecorded {
        context: String,
        snapshot: CanaryReconciliation,
        receipt: Option<CommandReceipt>,
        final_for_shutdown: bool,
    },
    ReconciliationFailed {
        context: String,
        error_class: String,
        final_for_shutdown: bool,
    },
    PostReturned {
        identity: String,
        success: bool,
        definitive: bool,
        order_id: String,
        error_message: Option<String>,
    },
    AdmissionClosed {
        reason: ClosureReason,
        receipt: Option<CommandReceipt>,
    },
    RecoveryRequired {
        reason: ClosureReason,
    },
    CampaignClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum CanaryStateError {
    #[error("campaign authority does not match the canonical canary contract")]
    AuthorityMismatch,
    #[error("event is invalid for campaign stage {0:?}")]
    InvalidStage(CampaignStage),
    #[error("attempt identity or origin is invalid")]
    InvalidAttempt,
    #[error("quote, risk, resolver, or prepared order admission proof does not match")]
    AdmissionMismatch,
    #[error("canary risk evaluation blocked the attempt")]
    RiskBlocked,
    #[error("one nonterminal attempt already exists")]
    PendingAttempt,
    #[error("campaign slot or commitment is exhausted")]
    Exhausted,
    #[error("campaign arithmetic overflow or underflow")]
    Arithmetic,
    #[error("event log failure: {0}")]
    EventLog(#[from] pe_event_log::LogError),
    #[error("event encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("unknown canary event schema {0}")]
    UnknownSchema(u32),
    #[error("canary journal contains a foreign or unsupported envelope")]
    UnexpectedJournalEnvelope,
    #[error("canary journal path is a symlink or has group/other permissions")]
    InsecureJournalPermissions,
    #[error("canary journal file operation failed: {0}")]
    JournalIo(#[from] io::Error),
}

impl CanaryCampaignState {
    pub fn apply(&mut self, event: &CanaryEvent) -> Result<(), CanaryStateError> {
        match event {
            CanaryEvent::CampaignArmed {
                authorization,
                receipt,
            } => {
                if self.stage != CampaignStage::Inactive {
                    return Err(CanaryStateError::InvalidStage(self.stage));
                }
                self.campaign_id = Some(authorization.campaign_id.clone());
                self.campaign_authorization_hash =
                    Some(campaign_authorization_hash(authorization)?);
                self.stage = CampaignStage::ProbesArmed;
                self.expires_at = Some(authorization.expires_at);
                self.implementation_commit = Some(authorization.implementation_commit.clone());
                self.binary_hash = Some(authorization.binary_hash.clone());
                self.config_hash = Some(authorization.config_hash.clone());
                self.resolver_inventory_hash = Some(authorization.resolver_inventory_hash.clone());
                self.wallet = Some(authorization.wallet.clone());
                self.owner_signer = Some(authorization.owner_signer.clone());
                self.spender = Some(authorization.spender.clone());
                self.sdk_archive_sha256 = Some(authorization.sdk_archive_sha256.clone());
                self.sdk_effective_vendor_tree_sha256 =
                    Some(authorization.sdk_effective_vendor_tree_sha256.clone());
                self.starting_collateral = authorization.starting_collateral;
                self.free_collateral = authorization.starting_collateral;
                self.canary_bankroll = authorization.starting_collateral;
                self.allowance = authorization.allowance;
                self.record_receipt(receipt)?;
            }
            CanaryEvent::ProbeReviewAccepted {
                campaign_id,
                reviewed_probe_ordinal,
                reviewed_probe_hash,
                receipt,
            } => {
                self.require_campaign(campaign_id)?;
                let pending = self
                    .pending
                    .as_ref()
                    .filter(|pending| {
                        pending.origin == AttemptOrigin::OperatorProbe
                            && pending.phase == AttemptPhase::Review
                    })
                    .ok_or(CanaryStateError::InvalidStage(self.stage))?;
                if self.stage != CampaignStage::ProbesArmed
                    || *reviewed_probe_ordinal != self.probe_posts
                    || self.reviewable_probe_hash()?.as_deref()
                        != Some(reviewed_probe_hash.as_str())
                    || pending.identity.is_empty()
                {
                    return Err(CanaryStateError::InvalidStage(self.stage));
                }
                self.pending = None;
                self.reviewed_probe_hashes.push(reviewed_probe_hash.clone());
                self.stage = if self.probe_posts == PROBE_POST_LIMIT {
                    CampaignStage::OrganicReady
                } else {
                    CampaignStage::ProbesArmed
                };
                self.record_receipt(receipt)?;
            }
            CanaryEvent::OrganicStageArmed {
                authorization,
                receipt,
            } => {
                self.require_campaign(&authorization.campaign_id)?;
                if self.stage != CampaignStage::OrganicReady
                    || authorization.schema_version != 1
                    || authorization.reviewed_probe_count != PROBE_POST_LIMIT
                    || self.campaign_authorization_hash.as_deref()
                        != Some(authorization.campaign_authorization_hash.as_str())
                    || self.implementation_commit.as_deref()
                        != Some(authorization.implementation_commit.as_str())
                    || organic_evidence_bundle_hash(&self.reviewed_probe_hashes)?
                        != authorization.evidence_bundle_hash
                {
                    return Err(CanaryStateError::AuthorityMismatch);
                }
                self.stage = CampaignStage::OrganicArmed;
                self.record_receipt(receipt)?;
            }
            CanaryEvent::PreReservationSkipped { receipt, .. } => {
                self.record_receipt(receipt)?;
            }
            CanaryEvent::HttpResponseCaptured { .. }
            | CanaryEvent::HttpTransportFailed { .. }
            | CanaryEvent::ArtifactCaptured { .. } => {}
            CanaryEvent::ReconciliationRecorded {
                snapshot,
                receipt,
                final_for_shutdown,
                ..
            } => {
                let balance_explained = self.reconciliation_balance_explained(snapshot);
                let allowance_explained = self
                    .expected_allowance_after(snapshot)
                    .is_some_and(|expected| expected == snapshot.allowance);
                let authoritative = balance_explained
                    && allowance_explained
                    && self.reconciliation_inventory_explained(snapshot)
                    && snapshot.open_order_ids.is_empty()
                    && !snapshot.unexpected_activity
                    && snapshot.standard_spender_only;
                self.reconciliation_failed = !authoritative;
                self.recovery_unresolved = !authoritative;
                if authoritative {
                    let previous_positions = self
                        .last_reconciliation
                        .as_ref()
                        .map(|previous| previous.positions.as_slice())
                        .unwrap_or_default();
                    for attempt in &mut self.attempts {
                        let previously_held = previous_positions.iter().any(|position| {
                            position.condition_id == attempt.attribution.condition_id
                                && position.outcome_id == attempt.attribution.outcome_id
                        });
                        let currently_held = snapshot.positions.iter().any(|position| {
                            position.condition_id == attempt.attribution.condition_id
                                && position.outcome_id == attempt.attribution.outcome_id
                        });
                        if previously_held && !currently_held {
                            attempt.open_debit = CollateralAmount::ZERO;
                        }
                    }
                    self.last_reconciliation = Some(snapshot.clone());
                    self.last_reconciliation_at = Some(snapshot.observed_at);
                    self.free_collateral = snapshot.free_collateral;
                    self.allowance = snapshot.allowance;
                    self.canary_bankroll = match &self.pending {
                        Some(pending) => snapshot
                            .free_collateral
                            .checked_sub(pending.worst_case_debit)
                            .map_err(|_| CanaryStateError::Arithmetic)?,
                        None => snapshot.free_collateral,
                    };
                }
                if *final_for_shutdown {
                    self.shutdown_reconciliation_designated = true;
                    self.shutdown_reconciliation_succeeded = authoritative;
                }
                if authoritative
                    && let Some(pending) = self
                        .pending
                        .as_ref()
                        .filter(|pending| pending.phase != AttemptPhase::Review)
                        .cloned()
                    && let Some(report) = snapshot.execution_report.as_ref()
                {
                    validate_report(&pending, report)?;
                    self.canary_bankroll = snapshot.free_collateral;
                    let attempt = self
                        .attempts
                        .iter_mut()
                        .find(|attempt| attempt.identity == pending.identity)
                        .ok_or(CanaryStateError::InvalidAttempt)?;
                    attempt.open_debit = report.filled_collateral;
                    for fill in &report.fills {
                        if !self.known_trade_ids.contains(&fill.trade_id) {
                            self.known_trade_ids.push(fill.trade_id.clone());
                        }
                    }
                    if self.stage != CampaignStage::ClosedObserving {
                        match pending.origin {
                            AttemptOrigin::OperatorProbe => {
                                if let Some(pending) = self.pending.as_mut() {
                                    pending.phase = AttemptPhase::Review;
                                }
                            }
                            AttemptOrigin::Organic if self.organic_posts < ORGANIC_POST_LIMIT => {
                                self.pending = None;
                                self.stage = CampaignStage::OrganicArmed;
                            }
                            AttemptOrigin::Organic => {
                                self.pending = None;
                                self.stage = CampaignStage::ClosedObserving;
                                self.terminal_reason = Some(ClosureReason::Exhausted);
                            }
                        }
                    } else {
                        self.pending = None;
                    }
                }
                if let Some(receipt) = receipt {
                    self.record_receipt(receipt)?;
                }
            }
            CanaryEvent::ReconciliationFailed {
                final_for_shutdown, ..
            } => {
                self.last_reconciliation_at = None;
                self.reconciliation_failed = true;
                self.recovery_unresolved = true;
                if self.pending.is_some()
                    || self
                        .attempts
                        .iter()
                        .any(|attempt| attempt.open_debit != CollateralAmount::ZERO)
                {
                    self.kill_latched = true;
                    if self.stage != CampaignStage::Closed {
                        self.stage = CampaignStage::ClosedObserving;
                    }
                    self.terminal_reason = Some(ClosureReason::VenueAmbiguous);
                }
                if *final_for_shutdown {
                    self.shutdown_reconciliation_designated = true;
                    self.shutdown_reconciliation_succeeded = false;
                }
            }
            CanaryEvent::AttemptReserved {
                admission,
                prepared,
                receipt,
            } => {
                if self.pending.is_some() {
                    return Err(CanaryStateError::PendingAttempt);
                }
                if admission.identity.trim().is_empty()
                    || prepared.worst_case_debit == CollateralAmount::ZERO
                {
                    return Err(CanaryStateError::InvalidAttempt);
                }
                match admission.origin {
                    AttemptOrigin::OperatorProbe
                        if self.stage == CampaignStage::ProbesArmed
                            && self.probe_posts < PROBE_POST_LIMIT =>
                    {
                        self.probe_posts += 1;
                    }
                    AttemptOrigin::Organic
                        if self.stage == CampaignStage::OrganicArmed
                            && self.organic_posts < ORGANIC_POST_LIMIT =>
                    {
                        self.organic_posts += 1;
                    }
                    _ => return Err(CanaryStateError::InvalidStage(self.stage)),
                }
                let committed = self
                    .committed_debit
                    .checked_add(prepared.worst_case_debit)
                    .map_err(|_| CanaryStateError::Arithmetic)?;
                if committed > CAMPAIGN_MAX_COMMITMENT {
                    return Err(CanaryStateError::Exhausted);
                }
                self.committed_debit = committed;
                self.canary_bankroll = self
                    .free_collateral
                    .checked_sub(prepared.worst_case_debit)
                    .map_err(|_| CanaryStateError::Arithmetic)?;
                self.pending = Some(PendingAttempt {
                    identity: admission.identity.clone(),
                    origin: admission.origin,
                    worst_case_debit: prepared.worst_case_debit,
                    requested_shares: prepared.taker_shares,
                    order_hash: prepared.order_hash.clone(),
                    prepared_body_hash: prepared.post_body_hash.clone(),
                    venue_order_id: None,
                    post_success: None,
                    phase: AttemptPhase::Reserved,
                });
                self.attempts.push(CampaignAttemptRecord {
                    identity: admission.identity.clone(),
                    origin: admission.origin,
                    attribution: admission.attribution.clone(),
                    shares: prepared.taker_shares,
                    open_debit: prepared.worst_case_debit,
                });
                self.record_receipt(receipt)?;
            }
            CanaryEvent::PostInFlight {
                identity,
                order_hash,
                post_body_hash,
            } => {
                let pending = self.pending_mut(identity)?;
                if pending.phase != AttemptPhase::Reserved
                    || pending.prepared_body_hash != *post_body_hash
                    || pending.order_hash != *order_hash
                {
                    return Err(CanaryStateError::InvalidAttempt);
                }
                pending.phase = AttemptPhase::PostInFlight;
            }
            CanaryEvent::PostReturned {
                identity,
                success,
                definitive,
                order_id,
                ..
            } => {
                let pending = self.pending_mut(identity)?;
                let origin = pending.origin;
                if pending.phase != AttemptPhase::PostInFlight {
                    return Err(CanaryStateError::InvalidAttempt);
                }
                if !order_id.trim().is_empty() {
                    pending.venue_order_id = Some(pe_core_types::VenueOrderId(order_id.clone()));
                }
                if *definitive && *success && pending.venue_order_id.is_none() {
                    return Err(CanaryStateError::InvalidAttempt);
                }
                pending.post_success = definitive.then_some(*success);
                if !success {
                    self.stage = CampaignStage::ClosedObserving;
                    self.terminal_reason = Some(if *definitive {
                        match origin {
                            AttemptOrigin::OperatorProbe => ClosureReason::ProbeFailed,
                            AttemptOrigin::Organic => ClosureReason::OrganicAttemptFailed,
                        }
                    } else {
                        ClosureReason::VenueAmbiguous
                    });
                }
            }
            CanaryEvent::AdmissionClosed { reason, receipt } => {
                self.kill_latched = true;
                if self.stage != CampaignStage::Closed {
                    self.stage = CampaignStage::ClosedObserving;
                }
                if *reason != ClosureReason::Killed || self.terminal_reason.is_none() {
                    self.terminal_reason = Some(*reason);
                }
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|attempt| attempt.phase == AttemptPhase::Review)
                {
                    self.pending = None;
                }
                if let Some(receipt) = receipt {
                    self.record_receipt(receipt)?;
                }
            }
            CanaryEvent::RecoveryRequired { reason } => {
                self.kill_latched = true;
                if self.stage != CampaignStage::Closed {
                    self.stage = CampaignStage::ClosedObserving;
                }
                self.terminal_reason = Some(*reason);
            }
            CanaryEvent::CampaignClosed => {
                if self.stage != CampaignStage::ClosedObserving
                    || self.pending.is_some()
                    || self
                        .attempts
                        .iter()
                        .any(|attempt| attempt.open_debit != CollateralAmount::ZERO)
                {
                    return Err(CanaryStateError::InvalidStage(self.stage));
                }
                self.stage = CampaignStage::Closed;
            }
        }
        Ok(())
    }

    fn require_campaign(&self, campaign_id: &str) -> Result<(), CanaryStateError> {
        if self.campaign_id.as_deref() == Some(campaign_id) {
            Ok(())
        } else {
            Err(CanaryStateError::AuthorityMismatch)
        }
    }

    pub fn command_receipt(&self, command_id: &str) -> Option<&CommandReceipt> {
        self.command_receipts
            .iter()
            .find(|receipt| receipt.command_id == command_id)
    }

    pub fn reviewable_probe_hash(&self) -> Result<Option<String>, CanaryStateError> {
        let Some(pending) = self.pending.as_ref().filter(|pending| {
            pending.origin == AttemptOrigin::OperatorProbe && pending.phase == AttemptPhase::Review
        }) else {
            return Ok(None);
        };
        let reconciliation = self
            .last_reconciliation
            .as_ref()
            .ok_or(CanaryStateError::InvalidAttempt)?;
        let payload = (
            &pending.identity,
            &pending.order_hash,
            &pending.prepared_body_hash,
            &pending.venue_order_id,
            pending.post_success,
            &reconciliation.execution_report,
            &reconciliation.evidence_hashes,
        );
        Ok(Some(
            blake3::hash(&serde_json::to_vec(&payload)?)
                .to_hex()
                .to_string(),
        ))
    }

    pub fn exposure_amounts(
        &self,
        attribution: &AttemptAttribution,
    ) -> Result<CanaryExposureAmounts, CanaryStateError> {
        self.attempts
            .iter()
            .try_fold(CanaryExposureAmounts::default(), |mut exposure, attempt| {
                exposure.total = exposure
                    .total
                    .checked_add(attempt.open_debit)
                    .map_err(|_| CanaryStateError::Arithmetic)?;
                if attempt.attribution.condition_id == attribution.condition_id {
                    exposure.market = exposure
                        .market
                        .checked_add(attempt.open_debit)
                        .map_err(|_| CanaryStateError::Arithmetic)?;
                }
                if attempt.attribution.family == attribution.family {
                    exposure.family = exposure
                        .family
                        .checked_add(attempt.open_debit)
                        .map_err(|_| CanaryStateError::Arithmetic)?;
                }
                if attribution.leader.is_some() && attempt.attribution.leader == attribution.leader
                {
                    exposure.leader = exposure
                        .leader
                        .checked_add(attempt.open_debit)
                        .map_err(|_| CanaryStateError::Arithmetic)?;
                }
                Ok(exposure)
            })
    }

    pub fn reconciliation_balance_explained(&self, snapshot: &CanaryReconciliation) -> bool {
        if self.campaign_id.is_none() {
            return true;
        }
        let filled_debit = snapshot
            .execution_report
            .as_ref()
            .map_or(CollateralAmount::ZERO, |report| report.filled_collateral);
        let Ok(expected_after_fill) = self.free_collateral.checked_sub(filled_debit) else {
            return false;
        };
        if snapshot.free_collateral < expected_after_fill {
            return false;
        }
        let credit = snapshot.free_collateral.atomic() - expected_after_fill.atomic();
        let previous_positions = self
            .last_reconciliation
            .as_ref()
            .map(|previous| previous.positions.as_slice())
            .unwrap_or_default();
        let mut settlement_credit = 0u64;
        for position in previous_positions.iter().filter(|previous| {
            !snapshot.positions.iter().any(|current| {
                current.condition_id == previous.condition_id
                    && current.outcome_id == previous.outcome_id
            })
        }) {
            let Some(resolution) = snapshot
                .resolutions
                .iter()
                .find(|resolution| resolution.condition_id == position.condition_id)
            else {
                return false;
            };
            if resolution.winner == position.outcome_id {
                let Some(total) = settlement_credit.checked_add(position.shares.atomic()) else {
                    return false;
                };
                settlement_credit = total;
            }
        }
        credit == settlement_credit
    }

    pub fn expected_allowance_after(
        &self,
        snapshot: &CanaryReconciliation,
    ) -> Option<CollateralAmount> {
        let filled_debit = snapshot
            .execution_report
            .as_ref()
            .map_or(CollateralAmount::ZERO, |report| report.filled_collateral);
        self.allowance.checked_sub(filled_debit).ok()
    }

    pub fn reconciliation_inventory_explained(&self, snapshot: &CanaryReconciliation) -> bool {
        let previous_positions = self
            .last_reconciliation
            .as_ref()
            .map(|previous| previous.positions.as_slice())
            .unwrap_or_default();
        let pending_fill = self.pending.as_ref().and_then(|pending| {
            let attribution = self
                .attempts
                .iter()
                .find(|attempt| attempt.identity == pending.identity)
                .map(|attempt| &attempt.attribution)?;
            let shares = snapshot.execution_report.as_ref()?.filled_shares;
            Some((attribution, shares))
        });

        let current_exact = snapshot.positions.iter().all(|current| {
            let previous = previous_positions
                .iter()
                .find(|position| {
                    position.condition_id == current.condition_id
                        && position.outcome_id == current.outcome_id
                })
                .map_or(ShareAmount::ZERO, |position| position.shares);
            let added = pending_fill
                .filter(|(attribution, _)| {
                    attribution.condition_id == current.condition_id
                        && attribution.outcome_id == current.outcome_id
                })
                .map_or(ShareAmount::ZERO, |(_, shares)| shares);
            previous
                .checked_add(added)
                .is_ok_and(|expected| current.shares == expected)
        });
        let no_unexplained_disappearance = previous_positions.iter().all(|previous| {
            snapshot.positions.iter().any(|current| {
                current.condition_id == previous.condition_id
                    && current.outcome_id == previous.outcome_id
            }) || snapshot
                .resolutions
                .iter()
                .any(|resolution| resolution.condition_id == previous.condition_id)
        });
        let pending_fill_present = pending_fill.is_none_or(|(attribution, shares)| {
            shares == ShareAmount::ZERO
                || snapshot.positions.iter().any(|position| {
                    position.condition_id == attribution.condition_id
                        && position.outcome_id == attribution.outcome_id
                })
                || snapshot
                    .resolutions
                    .iter()
                    .any(|resolution| resolution.condition_id == attribution.condition_id)
        });
        current_exact && no_unexplained_disappearance && pending_fill_present
    }

    fn record_receipt(&mut self, receipt: &CommandReceipt) -> Result<(), CanaryStateError> {
        if receipt.command_id.trim().is_empty() || receipt.command_hash.trim().is_empty() {
            return Err(CanaryStateError::InvalidAttempt);
        }
        match self.command_receipt(&receipt.command_id) {
            Some(existing) if existing.command_hash == receipt.command_hash => Ok(()),
            Some(_) => Err(CanaryStateError::AuthorityMismatch),
            None => {
                self.command_receipts.push(receipt.clone());
                Ok(())
            }
        }
    }

    fn pending_mut(&mut self, identity: &str) -> Result<&mut PendingAttempt, CanaryStateError> {
        self.pending
            .as_mut()
            .filter(|pending| pending.identity == identity)
            .ok_or(CanaryStateError::InvalidAttempt)
    }
}

fn validate_report(
    pending: &PendingAttempt,
    report: &ExactExecutionReport,
) -> Result<(), CanaryStateError> {
    let fill_collateral = report
        .fills
        .iter()
        .try_fold(CollateralAmount::ZERO, |sum, fill| {
            sum.checked_add(fill.collateral_debit)
        })
        .map_err(|_| CanaryStateError::Arithmetic)?;
    let fill_shares = report
        .fills
        .iter()
        .try_fold(ShareAmount::ZERO, |sum, fill| sum.checked_add(fill.shares))
        .map_err(|_| CanaryStateError::Arithmetic)?;
    let fill_fees = report
        .fills
        .iter()
        .try_fold(CollateralAmount::ZERO, |sum, fill| {
            sum.checked_add(fill.fee)
        })
        .map_err(|_| CanaryStateError::Arithmetic)?;
    if report.requested_collateral != pending.worst_case_debit
        || report.requested_shares != pending.requested_shares
        || report.venue_order_id != pending.venue_order_id
        || report.filled_collateral != fill_collateral
        || report.filled_shares != fill_shares
        || report.fees != fill_fees
        || report.fees != CollateralAmount::ZERO
        || report.filled_collateral > report.requested_collateral
        || !matches!(report.filled_shares, ShareAmount::ZERO)
            && report.filled_shares != report.requested_shares
        || (report.filled_shares == ShareAmount::ZERO
            && report.filled_collateral != CollateralAmount::ZERO)
        || report
            .fills
            .iter()
            .any(|fill| fill.trade_id.trim().is_empty())
        || (pending.post_success == Some(true)
            && (report.filled_shares != report.requested_shares || report.fills.is_empty()))
        || (pending.post_success == Some(false)
            && (report.filled_shares != ShareAmount::ZERO || !report.fills.is_empty()))
        || (pending.phase == AttemptPhase::Reserved
            && (report.filled_shares != ShareAmount::ZERO || !report.fills.is_empty()))
        || (pending.post_success.is_none()
            && pending.phase == AttemptPhase::PostInFlight
            && !report.fills.is_empty()
            && report.filled_shares != report.requested_shares)
    {
        return Err(CanaryStateError::AdmissionMismatch);
    }
    Ok(())
}

pub struct CanaryJournal {
    writer: Writer,
    #[cfg(test)]
    fail_next_sync: bool,
}

impl CanaryJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CanaryStateError> {
        let path = path.as_ref();
        match fs::symlink_metadata(path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || metadata.permissions().mode() & 0o077 != 0 {
                    return Err(CanaryStateError::InsecureJournalPermissions);
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
        Ok(Self {
            writer: Writer::open(path)?,
            #[cfg(test)]
            fail_next_sync: false,
        })
    }

    pub fn append_sync(
        &mut self,
        event: &CanaryEvent,
        observed_at: OffsetDateTime,
    ) -> Result<(), CanaryStateError> {
        let payload = serde_json::to_vec(event)?;
        self.writer.append(EnvelopeIn {
            source_id: SourceId("live-canary".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(observed_at),
            received_at: ReceivedAt::now_utc(),
            content_type: ContentType::Json,
            payload,
        })?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_sync) {
            return Err(io::Error::other("injected canary journal sync failure").into());
        }
        self.writer.sync()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn inject_next_sync_failure(&mut self) {
        self.fail_next_sync = true;
    }

    pub fn rebuild(path: impl AsRef<Path>) -> Result<CanaryCampaignState, CanaryStateError> {
        let mut state = CanaryCampaignState::default();
        for item in Reader::replay(path)? {
            let (_, envelope) = item?;
            if envelope.source_id != SourceId("live-canary".to_owned())
                || envelope.parser_version != 1
                || envelope.content_type != ContentType::Json
            {
                return Err(CanaryStateError::UnexpectedJournalEnvelope);
            }
            if envelope.schema_version != 1 {
                return Err(CanaryStateError::UnknownSchema(envelope.schema_version));
            }
            let event: CanaryEvent = serde_json::from_slice(&envelope.payload)?;
            state.apply(&event)?;
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{OutcomeId, PolymarketConditionId, PolymarketTokenId, Price};
    use rust_decimal_macros::dec;
    use tempfile::tempdir;
    use time::macros::datetime;

    use super::*;

    fn authority() -> CampaignAuthorization {
        CampaignAuthorization {
            schema_version: 1,
            campaign_id: "campaign".to_owned(),
            implementation_commit: "commit".to_owned(),
            binary_hash: "binary".to_owned(),
            config_hash: "config".to_owned(),
            resolver_inventory_hash: "resolver".to_owned(),
            sdk_archive_sha256: "sdk".to_owned(),
            sdk_effective_vendor_tree_sha256: "sdk-tree".to_owned(),
            wallet: "wallet".to_owned(),
            owner_signer: "owner".to_owned(),
            spender: "spender".to_owned(),
            jurisdiction: "jurisdiction".to_owned(),
            jurisdiction_attestation_hash: "jurisdiction-attestation".to_owned(),
            account_attestation_hash: "account-attestation".to_owned(),
            issued_at: datetime!(2026-07-17 0:00 UTC),
            expires_at: datetime!(2026-07-24 0:00 UTC),
            starting_collateral: CAMPAIGN_START_COLLATERAL,
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            commitment_cap: CAMPAIGN_MAX_COMMITMENT,
            probe_slots: PROBE_POST_LIMIT,
            organic_slots: ORGANIC_POST_LIMIT,
        }
    }

    fn receipt(id: &str) -> CommandReceipt {
        CommandReceipt {
            command_id: id.to_owned(),
            command_hash: format!("hash-{id}"),
        }
    }

    fn blank_reconciliation() -> CanaryReconciliation {
        CanaryReconciliation {
            observed_at: datetime!(2026-07-17 0:00 UTC),
            geoblocked: false,
            closed_only: false,
            free_collateral: CollateralAmount::ZERO,
            allowance: CollateralAmount::ZERO,
            standard_spender_only: false,
            open_order_ids: Vec::new(),
            all_trade_ids: Vec::new(),
            position_count: 0,
            positions: Vec::new(),
            resolutions: Vec::new(),
            unexpected_activity: false,
            execution_report: None,
            evidence_hashes: Vec::new(),
        }
    }

    fn prepared_for(identity: &str, debit: u64, origin: AttemptOrigin) -> CanaryEvent {
        let price = Price::new(dec!(0.1)).unwrap();
        let shares = ShareAmount::from_atomic(5_000_000);
        CanaryEvent::AttemptReserved {
            admission: Box::new(CanaryAdmission {
                identity: identity.to_owned(),
                campaign_id: "campaign".to_owned(),
                origin,
                neutral_request_hash: "request".to_owned(),
                resolver_card_hash: "resolver".to_owned(),
                quote: CanaryQuote {
                    origin,
                    request_identity: identity.to_owned(),
                    snapshot_raw_hash: "book".to_owned(),
                    snapshot_observed_at_ms: 1,
                    condition_id: PolymarketConditionId("condition".to_owned()),
                    outcome_id: OutcomeId(0),
                    token_id: PolymarketTokenId("11".to_owned()),
                    full_ask_ladder_hash: "ladder".to_owned(),
                    best_ask: price,
                    executable_ask: price,
                    origin_price_ceiling: price,
                    kelly_cost: (origin == AttemptOrigin::Organic).then_some(price),
                    minimum_fill_price: Price::ZERO,
                    maximum_fill_price_exclusive: Price::new(dec!(0.99)).unwrap(),
                    minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: shares,
                    shares,
                    maximum_collateral: CollateralAmount::from_atomic(debit),
                    metadata_hashes: vec!["metadata".to_owned()],
                    expires_at: datetime!(2026-07-18 0:00 UTC),
                },
                risk: pe_risk_engine::CanaryRiskSnapshot {
                    origin: match origin {
                        AttemptOrigin::OperatorProbe => pe_risk_engine::CanaryOrigin::OperatorProbe,
                        AttemptOrigin::Organic => pe_risk_engine::CanaryOrigin::Organic,
                    },
                    proposed_worst_case_debit: CollateralAmount::from_atomic(debit),
                    canary_bankroll: CAMPAIGN_START_COLLATERAL,
                    leader_exposure_bps: (origin == AttemptOrigin::Organic)
                        .then_some(pe_core_types::BasisPoints(0)),
                    market_exposure_bps: pe_core_types::BasisPoints(0),
                    family_exposure_bps: pe_core_types::BasisPoints(0),
                    total_copy_exposure_bps: pe_core_types::BasisPoints(0),
                    open_exposure_bps: pe_core_types::BasisPoints(0),
                    drawdown_bps: pe_core_types::BasisPoints(0),
                    resolver_tradable: true,
                    account_state_fresh: true,
                    venue_reconciliation_fresh: true,
                    geoblock_fresh: true,
                    geoblocked: false,
                    closed_only_fresh: true,
                    closed_only: false,
                    jurisdiction_attestation_valid: true,
                    pending_reservation: false,
                    allowance: CollateralAmount::from_atomic(7_500_000),
                    standard_spender_only: true,
                },
                attribution: AttemptAttribution {
                    leader: (origin == AttemptOrigin::Organic).then_some("leader-a".to_owned()),
                    condition_id: PolymarketConditionId("condition".to_owned()),
                    outcome_id: OutcomeId(0),
                    family: MarketFamily::PoliticsElections,
                },
                probe_authorization: None,
                organic_decision_proof: None,
            }),
            prepared: PreparedPolymarketBuy {
                condition_id: PolymarketConditionId("condition".to_owned()),
                outcome_id: OutcomeId(0),
                token_id: PolymarketTokenId("11".to_owned()),
                maker: "wallet".to_owned(),
                signer: "wallet".to_owned(),
                funder: "wallet".to_owned(),
                verifying_contract: "exchange".to_owned(),
                spender: "exchange".to_owned(),
                exchange_domain_version: 2,
                neg_risk: false,
                side: "BUY".to_owned(),
                salt: "1".to_owned(),
                timestamp_ms: 1,
                expiration: "0".to_owned(),
                maker_collateral: CollateralAmount::from_atomic(debit),
                taker_shares: shares,
                limit_price: price,
                minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                signature_type: 3,
                order_type: "FOK".to_owned(),
                post_only: false,
                defer_exec: false,
                metadata: "0x00".to_owned(),
                builder: "0x00".to_owned(),
                order_hash: "order".to_owned(),
                post_body_hash: "body".to_owned(),
                sdk_version: "0.7.0".to_owned(),
                sdk_archive_sha256: "sdk".to_owned(),
                metadata_hashes: vec![],
                worst_case_debit: CollateralAmount::from_atomic(debit),
            },
            receipt: receipt(identity),
        }
    }

    fn prepared(identity: &str, debit: u64) -> CanaryEvent {
        prepared_for(identity, debit, AttemptOrigin::OperatorProbe)
    }

    fn settle(state: &mut CanaryCampaignState, identity: &str) {
        state
            .apply(&CanaryEvent::PostInFlight {
                identity: identity.to_owned(),
                order_hash: "order".to_owned(),
                post_body_hash: "body".to_owned(),
            })
            .unwrap();
        state
            .apply(&CanaryEvent::PostReturned {
                identity: identity.to_owned(),
                success: true,
                definitive: true,
                order_id: format!("venue-{identity}"),
                error_message: None,
            })
            .unwrap();
        let report = ExactExecutionReport {
            venue_order_id: Some(pe_core_types::VenueOrderId(format!("venue-{identity}"))),
            requested_collateral: CollateralAmount::from_atomic(500_000),
            requested_shares: ShareAmount::from_atomic(5_000_000),
            filled_collateral: CollateralAmount::from_atomic(500_000),
            filled_shares: ShareAmount::from_atomic(5_000_000),
            fees: CollateralAmount::ZERO,
            fills: vec![pe_venue_core::VenueFill {
                trade_id: format!("trade-{identity}"),
                collateral_debit: CollateralAmount::from_atomic(500_000),
                shares: ShareAmount::from_atomic(5_000_000),
                fee: CollateralAmount::ZERO,
            }],
        };
        let free_collateral = state
            .free_collateral
            .checked_sub(CollateralAmount::from_atomic(500_000))
            .unwrap();
        let allowance = state
            .allowance
            .checked_sub(CollateralAmount::from_atomic(500_000))
            .unwrap();
        let shares = state
            .last_reconciliation
            .as_ref()
            .and_then(|snapshot| snapshot.positions.first())
            .map_or(ShareAmount::from_atomic(5_000_000), |position| {
                position
                    .shares
                    .checked_add(ShareAmount::from_atomic(5_000_000))
                    .unwrap()
            });
        state
            .apply(&CanaryEvent::ReconciliationRecorded {
                context: "test".to_owned(),
                snapshot: CanaryReconciliation {
                    observed_at: datetime!(2026-07-17 1:00 UTC),
                    geoblocked: false,
                    closed_only: false,
                    free_collateral,
                    allowance,
                    standard_spender_only: true,
                    open_order_ids: Vec::new(),
                    all_trade_ids: vec![format!("trade-{identity}")],
                    position_count: 1,
                    positions: vec![CanaryPosition {
                        condition_id: PolymarketConditionId("condition".to_owned()),
                        outcome_id: OutcomeId(0),
                        shares,
                    }],
                    resolutions: Vec::new(),
                    unexpected_activity: false,
                    execution_report: Some(report.clone()),
                    evidence_hashes: vec![format!("evidence-{identity}")],
                },
                receipt: None,
                final_for_shutdown: false,
            })
            .unwrap();
    }

    #[test]
    fn reservation_is_monotonic_and_pending_reduces_bankroll() {
        let mut state = CanaryCampaignState::default();
        state
            .apply(&CanaryEvent::CampaignArmed {
                authorization: authority(),
                receipt: receipt("arm"),
            })
            .unwrap();
        state.apply(&prepared("probe-1", 1_000_000)).unwrap();
        assert_eq!(state.committed_debit.atomic(), 1_000_000);
        assert_eq!(state.canary_bankroll.atomic(), 399_000_000);
        assert!(matches!(
            state.apply(&prepared("probe-2", 1_000_000)),
            Err(CanaryStateError::PendingAttempt)
        ));
    }

    #[test]
    fn attempt_phase_is_orthogonal_to_campaign_stage() {
        let mut state = CanaryCampaignState::default();
        state
            .apply(&CanaryEvent::CampaignArmed {
                authorization: authority(),
                receipt: receipt("arm"),
            })
            .unwrap();
        state.apply(&prepared("probe-1", 500_000)).unwrap();
        state
            .apply(&CanaryEvent::PostInFlight {
                identity: "probe-1".to_owned(),
                order_hash: "order".to_owned(),
                post_body_hash: "body".to_owned(),
            })
            .unwrap();
        assert_eq!(state.stage, CampaignStage::ProbesArmed);
        assert_eq!(state.pending.unwrap().phase, AttemptPhase::PostInFlight);
    }

    #[test]
    fn probe_authority_hash_binds_absolute_debit() {
        let mut probe = ProbeAuthorization {
            schema_version: 1,
            campaign_id: "campaign".to_owned(),
            campaign_authorization_hash: "campaign-authority".to_owned(),
            probe_ordinal: 1,
            condition_id: PolymarketConditionId("condition".to_owned()),
            outcome_id: OutcomeId(0),
            token_id: PolymarketTokenId("11".to_owned()),
            shares: ShareAmount::from_atomic(2_000_000),
            worst_price: Price(dec!(0.50)),
            maximum_collateral: CollateralAmount::from_atomic(1_000_000),
            resolver_card_hash: "resolver".to_owned(),
            authority_hash: String::new(),
            expires_at: datetime!(2026-07-18 0:00 UTC),
        };
        probe.authority_hash = probe_authority_hash(&probe).unwrap();
        let original = probe.authority_hash.clone();
        probe.maximum_collateral = CollateralAmount::from_atomic(999_999);
        assert_ne!(probe_authority_hash(&probe).unwrap(), original);
    }

    #[test]
    fn journal_rebuilds_the_same_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("canary.log");
        let mut journal = CanaryJournal::open(&path).unwrap();
        let armed = CanaryEvent::CampaignArmed {
            authorization: authority(),
            receipt: receipt("arm"),
        };
        journal
            .append_sync(&armed, datetime!(2026-07-17 1:00 UTC))
            .unwrap();
        journal
            .append_sync(
                &prepared("probe-1", 1_000_000),
                datetime!(2026-07-17 1:01 UTC),
            )
            .unwrap();
        drop(journal);
        let rebuilt = CanaryJournal::rebuild(&path).unwrap();
        assert_eq!(rebuilt.probe_posts, 1);
        assert_eq!(rebuilt.committed_debit.atomic(), 1_000_000);
        assert_eq!(rebuilt.pending.unwrap().identity, "probe-1");
    }

    #[test]
    fn reconciliation_and_attempt_finalization_rebuild_atomically() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("canary.log");
        let mut journal = CanaryJournal::open(&path).unwrap();
        let identity = "probe-1";
        let report = ExactExecutionReport {
            venue_order_id: Some(pe_core_types::VenueOrderId("venue-probe-1".to_owned())),
            requested_collateral: CollateralAmount::from_atomic(500_000),
            requested_shares: ShareAmount::from_atomic(5_000_000),
            filled_collateral: CollateralAmount::from_atomic(500_000),
            filled_shares: ShareAmount::from_atomic(5_000_000),
            fees: CollateralAmount::ZERO,
            fills: vec![pe_venue_core::VenueFill {
                trade_id: "trade-probe-1".to_owned(),
                collateral_debit: CollateralAmount::from_atomic(500_000),
                shares: ShareAmount::from_atomic(5_000_000),
                fee: CollateralAmount::ZERO,
            }],
        };
        let events = vec![
            CanaryEvent::CampaignArmed {
                authorization: authority(),
                receipt: receipt("arm"),
            },
            prepared(identity, 500_000),
            CanaryEvent::PostInFlight {
                identity: identity.to_owned(),
                order_hash: "order".to_owned(),
                post_body_hash: "body".to_owned(),
            },
            CanaryEvent::PostReturned {
                identity: identity.to_owned(),
                success: true,
                definitive: true,
                order_id: "venue-probe-1".to_owned(),
                error_message: None,
            },
            CanaryEvent::ReconciliationRecorded {
                context: "post_return".to_owned(),
                snapshot: CanaryReconciliation {
                    observed_at: datetime!(2026-07-17 1:05 UTC),
                    geoblocked: false,
                    closed_only: false,
                    free_collateral: CollateralAmount::from_atomic(399_500_000),
                    allowance: CollateralAmount::from_atomic(7_500_000),
                    standard_spender_only: true,
                    open_order_ids: Vec::new(),
                    all_trade_ids: vec!["trade-probe-1".to_owned()],
                    position_count: 1,
                    positions: vec![CanaryPosition {
                        condition_id: PolymarketConditionId("condition".to_owned()),
                        outcome_id: OutcomeId(0),
                        shares: ShareAmount::from_atomic(5_000_000),
                    }],
                    resolutions: Vec::new(),
                    unexpected_activity: false,
                    execution_report: Some(report),
                    evidence_hashes: vec!["evidence".to_owned()],
                },
                receipt: None,
                final_for_shutdown: false,
            },
        ];
        let mut expected = CanaryCampaignState::default();
        for event in events {
            expected.apply(&event).unwrap();
            journal
                .append_sync(&event, datetime!(2026-07-17 1:05 UTC))
                .unwrap();
        }
        drop(journal);

        let rebuilt = CanaryJournal::rebuild(&path).unwrap();
        assert_eq!(rebuilt, expected);
        assert_eq!(rebuilt.pending.unwrap().phase, AttemptPhase::Review);
        assert_eq!(rebuilt.attempts[0].open_debit.atomic(), 500_000);
        assert_eq!(rebuilt.free_collateral.atomic(), 399_500_000);
    }

    #[test]
    fn journal_rebuild_rejects_foreign_envelopes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("canary.log");
        let mut journal = CanaryJournal::open(&path).unwrap();
        journal
            .writer
            .append(EnvelopeIn {
                source_id: SourceId("foreign".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(datetime!(2026-07-17 1:00 UTC)),
                received_at: ReceivedAt::now_utc(),
                content_type: ContentType::Json,
                payload: b"{}".to_vec(),
            })
            .unwrap();
        journal.writer.sync().unwrap();
        drop(journal);
        assert!(matches!(
            CanaryJournal::rebuild(&path),
            Err(CanaryStateError::UnexpectedJournalEnvelope)
        ));
    }

    #[test]
    fn durable_attempts_derive_exact_concentration_buckets() {
        let state = CanaryCampaignState {
            attempts: vec![
                CampaignAttemptRecord {
                    identity: "a".to_owned(),
                    origin: AttemptOrigin::Organic,
                    attribution: AttemptAttribution {
                        leader: Some("leader-a".to_owned()),
                        condition_id: PolymarketConditionId("market-a".to_owned()),
                        outcome_id: OutcomeId(0),
                        family: MarketFamily::PoliticsElections,
                    },
                    shares: ShareAmount::from_atomic(4_000_000),
                    open_debit: CollateralAmount::from_atomic(400_001),
                },
                CampaignAttemptRecord {
                    identity: "b".to_owned(),
                    origin: AttemptOrigin::Organic,
                    attribution: AttemptAttribution {
                        leader: Some("leader-b".to_owned()),
                        condition_id: PolymarketConditionId("market-b".to_owned()),
                        outcome_id: OutcomeId(1),
                        family: MarketFamily::PoliticsElections,
                    },
                    shares: ShareAmount::from_atomic(5_000_000),
                    open_debit: CollateralAmount::from_atomic(500_000),
                },
            ],
            ..CanaryCampaignState::default()
        };
        let exposure = state
            .exposure_amounts(&state.attempts[0].attribution)
            .unwrap();
        assert_eq!(exposure.leader.atomic(), 400_001);
        assert_eq!(exposure.market.atomic(), 400_001);
        assert_eq!(exposure.family.atomic(), 900_001);
        assert_eq!(exposure.total.atomic(), 900_001);
    }

    #[test]
    fn stage_loops_enforce_three_probes_and_five_organic_attempts() {
        let mut state = CanaryCampaignState::default();
        state
            .apply(&CanaryEvent::CampaignArmed {
                authorization: authority(),
                receipt: receipt("arm"),
            })
            .unwrap();
        for ordinal in 1..=PROBE_POST_LIMIT {
            let identity = format!("probe-{ordinal}");
            state.apply(&prepared(&identity, 500_000)).unwrap();
            settle(&mut state, &identity);
            let reviewed_probe_hash = state.reviewable_probe_hash().unwrap().unwrap();
            state
                .apply(&CanaryEvent::ProbeReviewAccepted {
                    campaign_id: "campaign".to_owned(),
                    reviewed_probe_ordinal: ordinal,
                    reviewed_probe_hash,
                    receipt: receipt(&format!("review-{ordinal}")),
                })
                .unwrap();
        }
        assert_eq!(state.allowance, CollateralAmount::from_atomic(6_500_000));
        assert_eq!(
            state.free_collateral,
            CollateralAmount::from_atomic(398_500_000)
        );
        assert_eq!(state.stage, CampaignStage::OrganicReady);
        assert!(matches!(
            state.apply(&prepared("probe-4", 500_000)),
            Err(CanaryStateError::InvalidStage(CampaignStage::OrganicReady))
        ));
        state
            .apply(&CanaryEvent::OrganicStageArmed {
                authorization: OrganicStageAuthorization {
                    schema_version: 1,
                    campaign_id: "campaign".to_owned(),
                    campaign_authorization_hash: state.campaign_authorization_hash.clone().unwrap(),
                    reviewed_probe_count: PROBE_POST_LIMIT,
                    implementation_commit: "commit".to_owned(),
                    evidence_bundle_hash: organic_evidence_bundle_hash(
                        &state.reviewed_probe_hashes,
                    )
                    .unwrap(),
                    expires_at: datetime!(2026-07-24 0:00 UTC),
                },
                receipt: receipt("organic-arm"),
            })
            .unwrap();
        for ordinal in 1..=ORGANIC_POST_LIMIT {
            let identity = format!("organic-{ordinal}");
            state
                .apply(&prepared_for(&identity, 500_000, AttemptOrigin::Organic))
                .unwrap();
            settle(&mut state, &identity);
        }
        assert_eq!(state.organic_posts, ORGANIC_POST_LIMIT);
        assert_eq!(state.stage, CampaignStage::ClosedObserving);
        assert_eq!(state.terminal_reason, Some(ClosureReason::Exhausted));
        assert!(matches!(
            state.apply(&prepared_for("organic-6", 500_000, AttemptOrigin::Organic)),
            Err(CanaryStateError::InvalidStage(
                CampaignStage::ClosedObserving
            ))
        ));
    }

    #[test]
    fn command_id_reuse_with_different_content_is_rejected() {
        let mut state = CanaryCampaignState::default();
        state
            .apply(&CanaryEvent::PreReservationSkipped {
                identity: "one".to_owned(),
                reason: "test".to_owned(),
                evidence_hashes: Vec::new(),
                receipt: receipt("same"),
            })
            .unwrap();
        let mut conflicting = receipt("same");
        conflicting.command_hash = "different".to_owned();
        assert!(matches!(
            state.apply(&CanaryEvent::PreReservationSkipped {
                identity: "two".to_owned(),
                reason: "test".to_owned(),
                evidence_hashes: Vec::new(),
                receipt: conflicting,
            }),
            Err(CanaryStateError::AuthorityMismatch)
        ));
    }

    #[test]
    fn balance_changes_require_fill_or_bounded_disappeared_position_credit() {
        let attribution = AttemptAttribution {
            leader: Some("leader".to_owned()),
            condition_id: PolymarketConditionId("condition".to_owned()),
            outcome_id: OutcomeId(0),
            family: MarketFamily::PoliticsElections,
        };
        let held = CanaryPosition {
            condition_id: attribution.condition_id.clone(),
            outcome_id: attribution.outcome_id,
            shares: ShareAmount::from_atomic(2_000_000),
        };
        let base_snapshot = CanaryReconciliation {
            observed_at: datetime!(2026-07-17 1:00 UTC),
            geoblocked: false,
            closed_only: false,
            free_collateral: CollateralAmount::from_atomic(399_000_000),
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            standard_spender_only: true,
            open_order_ids: Vec::new(),
            all_trade_ids: Vec::new(),
            position_count: 1,
            positions: vec![held],
            resolutions: Vec::new(),
            unexpected_activity: false,
            execution_report: None,
            evidence_hashes: vec!["evidence".to_owned()],
        };
        let mut state = CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            free_collateral: base_snapshot.free_collateral,
            allowance: base_snapshot.allowance,
            last_reconciliation: Some(base_snapshot.clone()),
            attempts: vec![CampaignAttemptRecord {
                identity: "organic".to_owned(),
                origin: AttemptOrigin::Organic,
                attribution,
                shares: ShareAmount::from_atomic(2_000_000),
                open_debit: CollateralAmount::from_atomic(1_000_000),
            }],
            ..CanaryCampaignState::default()
        };
        let mut unexplained = base_snapshot.clone();
        unexplained.free_collateral = CollateralAmount::from_atomic(399_000_001);
        assert!(!state.reconciliation_balance_explained(&unexplained));
        let mut partial_transfer = base_snapshot.clone();
        partial_transfer.positions[0].shares = ShareAmount::from_atomic(1_999_999);
        assert!(!state.reconciliation_inventory_explained(&partial_transfer));

        let mut redeemed = unexplained;
        redeemed.positions.clear();
        redeemed.position_count = 0;
        redeemed.free_collateral = base_snapshot.free_collateral;
        assert!(!state.reconciliation_balance_explained(&redeemed));
        redeemed.free_collateral = CollateralAmount::from_atomic(401_000_000);
        assert!(!state.reconciliation_balance_explained(&redeemed));
        redeemed.free_collateral = CollateralAmount::from_atomic(401_000_001);
        assert!(!state.reconciliation_balance_explained(&redeemed));
        let mut losing_state = state.clone();
        let mut losing = redeemed.clone();
        losing.free_collateral = base_snapshot.free_collateral;
        losing.resolutions = vec![CanaryResolution {
            condition_id: PolymarketConditionId("condition".to_owned()),
            winner: OutcomeId(1),
        }];
        assert!(losing_state.reconciliation_balance_explained(&losing));
        losing_state
            .apply(&CanaryEvent::ReconciliationRecorded {
                context: "losing_settlement".to_owned(),
                snapshot: losing,
                receipt: None,
                final_for_shutdown: false,
            })
            .unwrap();
        assert_eq!(losing_state.attempts[0].open_debit, CollateralAmount::ZERO);

        redeemed.free_collateral = CollateralAmount::from_atomic(401_000_000);
        redeemed.resolutions = vec![CanaryResolution {
            condition_id: PolymarketConditionId("condition".to_owned()),
            winner: OutcomeId(0),
        }];
        assert!(state.reconciliation_balance_explained(&redeemed));
        state
            .apply(&CanaryEvent::ReconciliationRecorded {
                context: "settlement".to_owned(),
                snapshot: redeemed,
                receipt: None,
                final_for_shutdown: false,
            })
            .unwrap();
        assert_eq!(state.attempts[0].open_debit, CollateralAmount::ZERO);
    }

    #[test]
    fn allowance_reconciliation_accepts_only_the_exact_fill_debit() {
        let state = CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            ..CanaryCampaignState::default()
        };
        let report = ExactExecutionReport {
            venue_order_id: None,
            requested_collateral: CollateralAmount::from_atomic(500_000),
            requested_shares: ShareAmount::from_atomic(5_000_000),
            filled_collateral: CollateralAmount::from_atomic(500_000),
            filled_shares: ShareAmount::from_atomic(5_000_000),
            fees: CollateralAmount::ZERO,
            fills: Vec::new(),
        };
        let snapshot = CanaryReconciliation {
            allowance: CollateralAmount::from_atomic(7_500_000),
            execution_report: Some(report),
            ..blank_reconciliation()
        };
        assert_eq!(
            state.expected_allowance_after(&snapshot),
            Some(CollateralAmount::from_atomic(7_500_000))
        );
        for allowance in [7_499_999, 7_500_001, 0, 8_000_000] {
            let mut drifted = snapshot.clone();
            drifted.allowance = CollateralAmount::from_atomic(allowance);
            assert_ne!(
                state.expected_allowance_after(&drifted),
                Some(drifted.allowance)
            );
        }
    }

    #[test]
    fn recovery_drift_remains_durable_until_authoritative_reconciliation() {
        let baseline = CanaryReconciliation {
            observed_at: datetime!(2026-07-17 1:00 UTC),
            free_collateral: CAMPAIGN_START_COLLATERAL,
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            standard_spender_only: true,
            ..blank_reconciliation()
        };
        let mut state = CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            stage: CampaignStage::ClosedObserving,
            free_collateral: CAMPAIGN_START_COLLATERAL,
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            last_reconciliation: Some(baseline.clone()),
            ..CanaryCampaignState::default()
        };
        let mut drifted = baseline.clone();
        drifted.observed_at = datetime!(2026-07-17 1:01 UTC);
        drifted.free_collateral = CollateralAmount::from_atomic(399_999_999);
        state
            .apply(&CanaryEvent::ReconciliationRecorded {
                context: "drift".to_owned(),
                snapshot: drifted,
                receipt: None,
                final_for_shutdown: false,
            })
            .unwrap();
        assert!(state.recovery_unresolved);
        assert_eq!(state.last_reconciliation, Some(baseline.clone()));

        let mut recovered = baseline;
        recovered.observed_at = datetime!(2026-07-17 1:02 UTC);
        state
            .apply(&CanaryEvent::ReconciliationRecorded {
                context: "recovered".to_owned(),
                snapshot: recovered.clone(),
                receipt: None,
                final_for_shutdown: false,
            })
            .unwrap();
        assert!(!state.recovery_unresolved);
        assert_eq!(state.last_reconciliation, Some(recovered));
    }

    #[test]
    fn reconciliation_failure_retains_held_inventory_baseline_and_blocks_closure() {
        let attribution = AttemptAttribution {
            leader: Some("leader".to_owned()),
            condition_id: PolymarketConditionId("condition".to_owned()),
            outcome_id: OutcomeId(0),
            family: MarketFamily::PoliticsElections,
        };
        let baseline = CanaryReconciliation {
            observed_at: datetime!(2026-07-17 1:00 UTC),
            free_collateral: CollateralAmount::from_atomic(399_500_000),
            allowance: CollateralAmount::from_atomic(7_500_000),
            standard_spender_only: true,
            position_count: 1,
            positions: vec![CanaryPosition {
                condition_id: attribution.condition_id.clone(),
                outcome_id: attribution.outcome_id,
                shares: ShareAmount::from_atomic(5_000_000),
            }],
            ..blank_reconciliation()
        };
        let mut state = CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            stage: CampaignStage::OrganicArmed,
            last_reconciliation_at: Some(baseline.observed_at),
            last_reconciliation: Some(baseline.clone()),
            attempts: vec![CampaignAttemptRecord {
                identity: "attempt".to_owned(),
                origin: AttemptOrigin::Organic,
                attribution,
                shares: ShareAmount::from_atomic(5_000_000),
                open_debit: CollateralAmount::from_atomic(500_000),
            }],
            ..CanaryCampaignState::default()
        };

        state
            .apply(&CanaryEvent::ReconciliationFailed {
                context: "after_fill".to_owned(),
                error_class: "timeout".to_owned(),
                final_for_shutdown: false,
            })
            .unwrap();

        assert_eq!(state.last_reconciliation, Some(baseline));
        assert!(state.last_reconciliation_at.is_none());
        assert!(state.reconciliation_failed);
        assert!(state.recovery_unresolved);
        assert!(state.kill_latched);
        assert_eq!(state.stage, CampaignStage::ClosedObserving);
        assert_eq!(state.terminal_reason, Some(ClosureReason::VenueAmbiguous));
        assert!(state.apply(&CanaryEvent::CampaignClosed).is_err());
    }

    #[test]
    fn mixed_winning_and_losing_settlements_clear_both_exposures() {
        let attributions = ["winner", "loser"].map(|condition| AttemptAttribution {
            leader: Some(format!("leader-{condition}")),
            condition_id: PolymarketConditionId(condition.to_owned()),
            outcome_id: OutcomeId(0),
            family: MarketFamily::PoliticsElections,
        });
        let shares = [1_000_000, 2_000_000];
        let positions = attributions
            .iter()
            .zip(shares)
            .map(|(attribution, shares)| CanaryPosition {
                condition_id: attribution.condition_id.clone(),
                outcome_id: attribution.outcome_id,
                shares: ShareAmount::from_atomic(shares),
            })
            .collect::<Vec<_>>();
        let previous = CanaryReconciliation {
            observed_at: datetime!(2026-07-17 1:00 UTC),
            geoblocked: false,
            closed_only: false,
            free_collateral: CollateralAmount::from_atomic(397_000_000),
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            standard_spender_only: true,
            open_order_ids: Vec::new(),
            all_trade_ids: Vec::new(),
            position_count: 2,
            positions,
            resolutions: Vec::new(),
            unexpected_activity: false,
            execution_report: None,
            evidence_hashes: vec!["evidence".to_owned()],
        };
        let mut state = CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            free_collateral: previous.free_collateral,
            allowance: previous.allowance,
            last_reconciliation: Some(previous.clone()),
            attempts: attributions
                .into_iter()
                .zip(shares)
                .enumerate()
                .map(|(index, (attribution, shares))| CampaignAttemptRecord {
                    identity: format!("organic-{index}"),
                    origin: AttemptOrigin::Organic,
                    attribution,
                    shares: ShareAmount::from_atomic(shares),
                    open_debit: CollateralAmount::from_atomic(shares),
                })
                .collect(),
            ..CanaryCampaignState::default()
        };
        let mut settled = previous;
        settled.position_count = 0;
        settled.positions.clear();
        settled.resolutions = vec![
            CanaryResolution {
                condition_id: PolymarketConditionId("winner".to_owned()),
                winner: OutcomeId(0),
            },
            CanaryResolution {
                condition_id: PolymarketConditionId("loser".to_owned()),
                winner: OutcomeId(1),
            },
        ];
        let mut external_top_up = settled.clone();
        external_top_up.free_collateral = CollateralAmount::from_atomic(398_500_000);
        assert!(!state.reconciliation_balance_explained(&external_top_up));
        settled.free_collateral = CollateralAmount::from_atomic(398_000_000);
        assert!(state.reconciliation_balance_explained(&settled));
        state
            .apply(&CanaryEvent::ReconciliationRecorded {
                context: "mixed_settlement".to_owned(),
                snapshot: settled,
                receipt: None,
                final_for_shutdown: false,
            })
            .unwrap();
        assert!(
            state
                .attempts
                .iter()
                .all(|attempt| attempt.open_debit == CollateralAmount::ZERO)
        );
    }

    #[test]
    fn opposite_outcomes_allow_exactly_one_condition_payout() {
        let condition_id = PolymarketConditionId("condition".to_owned());
        let attributions = [0, 1].map(|outcome| AttemptAttribution {
            leader: Some(format!("leader-{outcome}")),
            condition_id: condition_id.clone(),
            outcome_id: OutcomeId(outcome),
            family: MarketFamily::PoliticsElections,
        });
        let shares = [1_000_000, 2_000_000];
        let previous = CanaryReconciliation {
            observed_at: datetime!(2026-07-17 1:00 UTC),
            geoblocked: false,
            closed_only: false,
            free_collateral: CollateralAmount::from_atomic(397_000_000),
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            standard_spender_only: true,
            open_order_ids: Vec::new(),
            all_trade_ids: Vec::new(),
            position_count: 2,
            positions: attributions
                .iter()
                .zip(shares)
                .map(|(attribution, shares)| CanaryPosition {
                    condition_id: attribution.condition_id.clone(),
                    outcome_id: attribution.outcome_id,
                    shares: ShareAmount::from_atomic(shares),
                })
                .collect(),
            resolutions: Vec::new(),
            unexpected_activity: false,
            execution_report: None,
            evidence_hashes: vec!["evidence".to_owned()],
        };
        let state = CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            free_collateral: previous.free_collateral,
            allowance: previous.allowance,
            last_reconciliation: Some(previous.clone()),
            attempts: attributions
                .into_iter()
                .zip(shares)
                .enumerate()
                .map(|(index, (attribution, shares))| CampaignAttemptRecord {
                    identity: format!("organic-{index}"),
                    origin: AttemptOrigin::Organic,
                    attribution,
                    shares: ShareAmount::from_atomic(shares),
                    open_debit: CollateralAmount::from_atomic(shares),
                })
                .collect(),
            ..CanaryCampaignState::default()
        };
        let settled_at = |free_collateral, winner| CanaryReconciliation {
            position_count: 0,
            positions: Vec::new(),
            resolutions: vec![CanaryResolution {
                condition_id: condition_id.clone(),
                winner: OutcomeId(winner),
            }],
            free_collateral: CollateralAmount::from_atomic(free_collateral),
            ..previous.clone()
        };

        assert!(state.reconciliation_balance_explained(&settled_at(398_000_000, 0)));
        assert!(state.reconciliation_balance_explained(&settled_at(399_000_000, 1)));
        assert!(!state.reconciliation_balance_explained(&settled_at(397_000_000, 0)));
        assert!(!state.reconciliation_balance_explained(&settled_at(398_500_000, 0)));
        assert!(!state.reconciliation_balance_explained(&settled_at(400_000_000, 1)));
    }

    #[test]
    fn response_evidence_hash_binds_request_headers_and_timing() {
        let response = RawHttpResponse {
            source_id: "source".to_owned(),
            endpoint_kind: "book".to_owned(),
            method: "GET".to_owned(),
            path: "/book".to_owned(),
            ordered_query: vec![("token_id".to_owned(), "one".to_owned())],
            status: 200,
            headers: vec![("etag".to_owned(), "one".to_owned())],
            body: b"same body".to_vec(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: datetime!(2026-07-17 1:00 UTC),
            received_at: datetime!(2026-07-17 1:00:01 UTC),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        };
        let expected = response_evidence_hash(&response).unwrap();
        let mut changed = response.clone();
        changed.path = "/orders".to_owned();
        assert_ne!(response_evidence_hash(&changed).unwrap(), expected);
        changed = response.clone();
        changed.ordered_query[0].1 = "two".to_owned();
        assert_ne!(response_evidence_hash(&changed).unwrap(), expected);
        changed = response.clone();
        changed.source_id = "other-source".to_owned();
        changed.endpoint_kind = "other-endpoint".to_owned();
        assert_ne!(response_evidence_hash(&changed).unwrap(), expected);
        changed = response.clone();
        changed.headers[0].1 = "two".to_owned();
        assert_ne!(response_evidence_hash(&changed).unwrap(), expected);
        changed = response;
        changed.received_at += time::Duration::milliseconds(1);
        assert_ne!(response_evidence_hash(&changed).unwrap(), expected);
    }

    #[test]
    fn raw_evidence_hashes_responses_and_artifacts_in_stream_order() {
        let response = RawHttpResponse {
            source_id: "source".to_owned(),
            endpoint_kind: "book".to_owned(),
            method: "GET".to_owned(),
            path: "/book".to_owned(),
            ordered_query: Vec::new(),
            status: 200,
            headers: Vec::new(),
            body: b"book".to_vec(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: datetime!(2026-07-17 1:00 UTC),
            received_at: datetime!(2026-07-17 1:00:01 UTC),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        };
        let artifact = RawArtifactObservation {
            source_id: "resolver-card".to_owned(),
            artifact_kind: "resolver-card".to_owned(),
            path: "/resolver.json".to_owned(),
            body: b"resolver".to_vec(),
            observed_at: datetime!(2026-07-17 1:00:02 UTC),
            received_at: datetime!(2026-07-17 1:00:03 UTC),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        };
        let stream = [
            RawEvidence::HttpResponse(response.clone()),
            RawEvidence::Artifact(artifact.clone()),
        ];
        let hashes = stream
            .iter()
            .map(raw_evidence_hash)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            hashes,
            [
                Some(response_evidence_hash(&response).unwrap()),
                Some(artifact_evidence_hash(&artifact).unwrap()),
            ]
        );
        let mut changed = artifact;
        changed.body.push(b'!');
        assert_ne!(
            raw_evidence_hash(&RawEvidence::Artifact(changed)).unwrap(),
            hashes[1]
        );
    }
}
