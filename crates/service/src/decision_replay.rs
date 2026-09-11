//! Pure replay and validation for post-boundary `decision_pending` evidence.

use pe_core_types::{CollateralAmount, EventSeq, Price, Side, SourceTradeId};
use pe_event_log::AppendReceipt;
use pe_execution_core::EconomicPrepared;
use pe_paper_state::{DecisionPendingRow, DecisionPendingState};
use pe_strategy_winner_follow::{WinnerFollowDeclineAudit, WinnerFollowError};
use pe_venue_polymarket::LadderPlan;
use serde::{Deserialize, Serialize};

use crate::bucket_commit::{
    DecisionContinuationError, DecisionContinuationFacts, DecisionContinuationV3,
};

const LEGACY_POST_BOUNDARY_EVIDENCE_VERSION: u16 = 2;
pub const POST_BOUNDARY_EVIDENCE_VERSION: u16 = 4;
pub const TERMINAL_EVIDENCE_VERSION: u16 = 5;
const LEGACY_FINANCIAL_SEMANTIC_VERSION: u32 = 0;
const EVIDENCE_OWNERS: [&str; 2] = ["source_log", "paper_log"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketEndEvidence {
    pub market_id: String,
    pub resolution_unix: Option<i64>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketPriceEvidence {
    pub market_id: String,
    pub outcome_id: u16,
    pub mid_price: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookEvidence {
    pub request_token_id: Option<String>,
    pub outcome: String,
    pub response_blake3: Option<String>,
    pub fetched_at_unix_ms: Option<u64>,
    pub best_ask: Option<String>,
    pub vwap_basis: Option<String>,
    pub ladder_plan_blake3: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionClockEvidence {
    pub purpose: String,
    pub unix_millis: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submillisecond_nanos: Option<u32>,
}

impl DecisionClockEvidence {
    /// Encode an exact Unix instant with Euclidean milliseconds and a nonnegative remainder.
    pub fn precise(purpose: &str, unix_nanos: i128) -> Result<Self, ReplayDecisionError> {
        Ok(Self {
            purpose: purpose.to_owned(),
            unix_millis: i64::try_from(unix_nanos.div_euclid(1_000_000))
                .map_err(|_| ReplayDecisionError::ClockPrecision)?,
            submillisecond_nanos: Some(
                u32::try_from(unix_nanos.rem_euclid(1_000_000))
                    .map_err(|_| ReplayDecisionError::ClockPrecision)?,
            ),
        })
    }

    pub(crate) fn precise_instant(&self) -> Result<time::OffsetDateTime, ReplayDecisionError> {
        self.validate_precision()?;
        let remainder = self
            .submillisecond_nanos
            .ok_or(ReplayDecisionError::ClockPrecision)?;
        time::OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(self.unix_millis) * 1_000_000 + i128::from(remainder),
        )
        .map_err(|_| ReplayDecisionError::ClockPrecision)
    }

    fn validate_precision(&self) -> Result<(), ReplayDecisionError> {
        if self
            .submillisecond_nanos
            .is_some_and(|value| value > 999_999)
        {
            return Err(ReplayDecisionError::ClockPrecision);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityEvidence {
    pub kind: String,
    pub outcome: String,
    pub bankroll: Option<String>,
}

impl AuthorityEvidence {
    pub(crate) fn not_read(reason: &str) -> Self {
        Self {
            kind: "not_read".to_owned(),
            outcome: reason.to_owned(),
            bankroll: None,
        }
    }

    pub(crate) fn local(outcome: &str) -> Self {
        Self {
            kind: "paper_state_sqlite".to_owned(),
            outcome: outcome.to_owned(),
            bankroll: None,
        }
    }

    pub(crate) fn commit_fill_v2(outcome: &str, bankroll: rust_decimal::Decimal) -> Self {
        Self {
            kind: "commit_fill_v2".to_owned(),
            outcome: outcome.to_owned(),
            bankroll: Some(bankroll.normalize().to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedFillEvidence {
    pub idempotency_key: String,
    pub market_id: String,
    pub outcome_id: u16,
    pub side: String,
    pub contracts: u64,
    pub fill_price: String,
    pub event_seq: u64,
}

/// Causal evidence retained when the financial prefix cannot construct a strategy risk snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WinnerFollowRiskInputEvidence {
    /// Exact paper prefix observed before risk construction. `None` preserves an acquisition
    /// failure that occurred before a verified financial prefix was available.
    pub financial_prefix: Option<AppendReceipt>,
    /// Exact set of source pages consulted by the strict price attempt: value-producing cached or
    /// fetched pages plus request-bound empty, rejected, or transport-failure observations.
    pub price_receipts: Vec<AppendReceipt>,
    /// Clock used for PnL, price freshness, and latency reconstruction.
    pub evaluated_at_unix_ms: i64,
    pub proposed_debit: CollateralAmount,
    pub per_trade_cap_bps: i32,
}

/// Receipt-bound inputs to the shared Winner-Follow decision, or the typed causal failure that
/// prevented their construction. Qualification replays both variants from the sealed prefixes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum WinnerFollowDecisionInputs {
    Evaluated {
        economic: Box<EconomicPrepared>,
    },
    RiskInputsUnavailable {
        cause: crate::risk_inputs::RiskInputsUnavailable,
        evidence: WinnerFollowRiskInputEvidence,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WinnerFollowDeclineEvidence {
    pub outcome: WinnerFollowDeclineAudit,
    pub inputs: WinnerFollowDecisionInputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalDispositionEvidence {
    pub disposition: String,
    pub reason: String,
    pub fill: Option<RecordedFillEvidence>,
    pub dispatch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decline: Option<WinnerFollowDeclineEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_receipt: Option<AppendReceipt>,
}

impl TerminalDispositionEvidence {
    pub(crate) fn no_fill(reason: &str) -> Self {
        Self {
            disposition: "no_fill".to_owned(),
            reason: reason.to_owned(),
            fill: None,
            dispatch_id: None,
            decline: None,
            final_receipt: None,
        }
    }

    pub(crate) fn no_copy(reason: &str) -> Self {
        Self {
            disposition: format!("no_copy:{reason}"),
            reason: reason.to_owned(),
            fill: None,
            dispatch_id: None,
            decline: None,
            final_receipt: None,
        }
    }

    pub(crate) fn settled_refusal() -> Self {
        Self {
            disposition: "no_fill:market_settled".to_owned(),
            reason: "market_settled".to_owned(),
            fill: None,
            dispatch_id: None,
            decline: None,
            final_receipt: None,
        }
    }

    pub(crate) fn dispatch_staged(dispatch_id: String) -> Self {
        Self {
            disposition: "dispatch_staged".to_owned(),
            reason: "live_targets_staged".to_owned(),
            fill: None,
            dispatch_id: Some(dispatch_id),
            decline: None,
            final_receipt: None,
        }
    }

    pub(crate) fn fill(
        idempotency_key: String,
        market_id: String,
        outcome_id: u16,
        side: &str,
        contracts: u64,
        fill_price: Price,
        event_seq: EventSeq,
    ) -> Self {
        Self {
            disposition: "fill".to_owned(),
            reason: "paper_fill_committed".to_owned(),
            fill: Some(RecordedFillEvidence {
                idempotency_key,
                market_id,
                outcome_id,
                side: side.to_owned(),
                contracts,
                fill_price: fill_price.0.normalize().to_string(),
                event_seq: event_seq.0,
            }),
            dispatch_id: None,
            decline: None,
            final_receipt: None,
        }
    }

    pub fn declined(error: &WinnerFollowError, inputs: WinnerFollowDecisionInputs) -> Self {
        Self {
            disposition: "no_fill".to_owned(),
            reason: format!("paper_reject:{error}"),
            fill: None,
            dispatch_id: None,
            decline: Some(WinnerFollowDeclineEvidence {
                outcome: WinnerFollowDeclineAudit::from(error),
                inputs,
            }),
            final_receipt: None,
        }
    }

    pub fn final_fill(final_receipt: AppendReceipt) -> Self {
        Self {
            disposition: "fill".to_owned(),
            reason: "paper_fill_committed".to_owned(),
            fill: None,
            dispatch_id: None,
            decline: None,
            final_receipt: Some(final_receipt),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPostBoundaryEvidenceBody {
    pub version: u16,
    pub owners: Vec<String>,
    pub source_trade_id: SourceTradeId,
    pub applied_configuration_hash: String,
    pub market_end: Option<MarketEndEvidence>,
    pub market_price: Option<MarketPriceEvidence>,
    pub book: Option<BookEvidence>,
    pub clocks: Vec<DecisionClockEvidence>,
    pub authority: AuthorityEvidence,
    pub terminal: TerminalDispositionEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPostBoundaryEvidence {
    #[serde(flatten)]
    pub body: DecisionPostBoundaryEvidenceBody,
    pub financial_semantic_version: u32,
    pub document_blake3: String,
}

impl DecisionPostBoundaryEvidence {
    /// Seal one post-boundary evidence body with its canonical BLAKE3 identity.
    pub fn from_body(body: DecisionPostBoundaryEvidenceBody) -> Result<Self, serde_json::Error> {
        let financial_semantic_version = crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION;
        let document_blake3 = body_hash(&body, financial_semantic_version)?;
        Ok(Self {
            body,
            financial_semantic_version,
            document_blake3,
        })
    }

    fn validate_hash(&self) -> Result<(), ReplayDecisionError> {
        let actual = body_hash(&self.body, self.financial_semantic_version)?;
        if actual != self.document_blake3 {
            return Err(ReplayDecisionError::DocumentHash {
                expected: self.document_blake3.clone(),
                actual,
            });
        }
        Ok(())
    }
}

/// Pre-financial wire contract retained only for validating durable v2 documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyDecisionPostBoundaryEvidence {
    #[serde(flatten)]
    body: DecisionPostBoundaryEvidenceBody,
    document_blake3: String,
}

impl LegacyDecisionPostBoundaryEvidence {
    fn validate_hash(&self) -> Result<(), ReplayDecisionError> {
        let actual = legacy_body_hash(&self.body)?;
        if actual != self.document_blake3 {
            return Err(ReplayDecisionError::DocumentHash {
                expected: self.document_blake3.clone(),
                actual,
            });
        }
        Ok(())
    }

    fn into_current(self) -> DecisionPostBoundaryEvidence {
        DecisionPostBoundaryEvidence {
            body: self.body,
            // Zero exists only in this decoded view and preserves the fact that the durable
            // pre-Start document carried no financial-semantic binding.
            financial_semantic_version: LEGACY_FINANCIAL_SEMANTIC_VERSION,
            document_blake3: self.document_blake3,
        }
    }
}

fn body_hash(
    body: &DecisionPostBoundaryEvidenceBody,
    financial_semantic_version: u32,
) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(&(financial_semantic_version, body))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn legacy_body_hash(body: &DecisionPostBoundaryEvidenceBody) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(body)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DecisionEvidenceCheckpointBody {
    version: u16,
    owners: Vec<String>,
    source_trade_id: SourceTradeId,
    applied_configuration_hash: String,
    market_end: Option<MarketEndEvidence>,
    market_price: Option<MarketPriceEvidence>,
    book: Option<BookEvidence>,
    clocks: Vec<DecisionClockEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatch_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DecisionEvidenceCheckpoint {
    #[serde(flatten)]
    body: DecisionEvidenceCheckpointBody,
    financial_semantic_version: u32,
    document_blake3: String,
}

/// Pre-financial checkpoint wire contract retained only for compatibility recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyDecisionEvidenceCheckpoint {
    #[serde(flatten)]
    body: DecisionEvidenceCheckpointBody,
    document_blake3: String,
}

fn checkpoint_hash(
    body: &DecisionEvidenceCheckpointBody,
    financial_semantic_version: u32,
) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(&(financial_semantic_version, body))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn legacy_checkpoint_hash(
    body: &DecisionEvidenceCheckpointBody,
) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(body)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

#[derive(Debug, Deserialize)]
struct EvidenceWireVersion {
    version: u16,
}

fn wire_version_and_financial_field(json: &str) -> Result<(u16, bool), ReplayDecisionError> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    let has_financial_semantic_version = value
        .as_object()
        .is_some_and(|object| object.contains_key("financial_semantic_version"));
    let version = serde_json::from_value::<EvidenceWireVersion>(value)?.version;
    Ok((version, has_financial_semantic_version))
}

struct DecodedDecisionEvidence {
    evidence: DecisionPostBoundaryEvidence,
    legacy: bool,
}

fn decode_decision_evidence(json: &str) -> Result<DecodedDecisionEvidence, ReplayDecisionError> {
    let (version, has_financial_semantic_version) = wire_version_and_financial_field(json)?;
    match (version, has_financial_semantic_version) {
        (LEGACY_POST_BOUNDARY_EVIDENCE_VERSION, false) => {
            let legacy: LegacyDecisionPostBoundaryEvidence = serde_json::from_str(json)?;
            legacy.validate_hash()?;
            validate_clock_precision(&legacy.body.clocks)?;
            Ok(DecodedDecisionEvidence {
                evidence: legacy.into_current(),
                legacy: true,
            })
        }
        (POST_BOUNDARY_EVIDENCE_VERSION | TERMINAL_EVIDENCE_VERSION, true) => {
            let current: DecisionPostBoundaryEvidence = serde_json::from_str(json)?;
            current.validate_hash()?;
            validate_clock_precision(&current.body.clocks)?;
            Ok(DecodedDecisionEvidence {
                evidence: current,
                legacy: false,
            })
        }
        (
            LEGACY_POST_BOUNDARY_EVIDENCE_VERSION
            | POST_BOUNDARY_EVIDENCE_VERSION
            | TERMINAL_EVIDENCE_VERSION,
            _,
        ) => Err(ReplayDecisionError::FinancialSemanticField { version }),
        _ => Err(ReplayDecisionError::Version(version)),
    }
}

struct DecodedCheckpoint {
    body: DecisionEvidenceCheckpointBody,
    financial_semantic_version: Option<u32>,
}

fn decode_checkpoint(json: &str) -> Result<DecodedCheckpoint, ReplayDecisionError> {
    let (version, has_financial_semantic_version) = wire_version_and_financial_field(json)?;
    match (version, has_financial_semantic_version) {
        (LEGACY_POST_BOUNDARY_EVIDENCE_VERSION, false) => {
            let legacy: LegacyDecisionEvidenceCheckpoint = serde_json::from_str(json)?;
            validate_clock_precision(&legacy.body.clocks)?;
            let actual = legacy_checkpoint_hash(&legacy.body)?;
            if actual != legacy.document_blake3 {
                return Err(ReplayDecisionError::DocumentHash {
                    expected: legacy.document_blake3,
                    actual,
                });
            }
            Ok(DecodedCheckpoint {
                body: legacy.body,
                financial_semantic_version: None,
            })
        }
        (POST_BOUNDARY_EVIDENCE_VERSION, true) => {
            let current: DecisionEvidenceCheckpoint = serde_json::from_str(json)?;
            validate_clock_precision(&current.body.clocks)?;
            let actual = checkpoint_hash(&current.body, current.financial_semantic_version)?;
            if actual != current.document_blake3 {
                return Err(ReplayDecisionError::DocumentHash {
                    expected: current.document_blake3,
                    actual,
                });
            }
            Ok(DecodedCheckpoint {
                body: current.body,
                financial_semantic_version: Some(current.financial_semantic_version),
            })
        }
        (LEGACY_POST_BOUNDARY_EVIDENCE_VERSION | POST_BOUNDARY_EVIDENCE_VERSION, _) => {
            Err(ReplayDecisionError::FinancialSemanticField { version })
        }
        _ => Err(ReplayDecisionError::Version(version)),
    }
}

#[derive(Debug, Clone)]
pub struct DecisionEvidenceAccumulator {
    source_trade_id: SourceTradeId,
    applied_configuration_hash: String,
    market_end: Option<MarketEndEvidence>,
    market_price: Option<MarketPriceEvidence>,
    book: Option<BookEvidence>,
    clocks: Vec<DecisionClockEvidence>,
    dispatch_id: Option<String>,
}

impl DecisionEvidenceAccumulator {
    pub(crate) fn new(continuation: &DecisionContinuationFacts) -> Self {
        Self {
            source_trade_id: continuation.source_trade_id.clone(),
            applied_configuration_hash: continuation.applied_configuration_hash.clone(),
            market_end: None,
            market_price: None,
            book: None,
            clocks: Vec::new(),
            dispatch_id: None,
        }
    }

    pub(crate) fn record_clock(&mut self, purpose: &str, unix_millis: i64) {
        self.clocks.push(DecisionClockEvidence {
            purpose: purpose.to_owned(),
            unix_millis,
            submillisecond_nanos: None,
        });
    }

    pub(crate) fn record_market_end(&mut self, evidence: MarketEndEvidence) {
        self.market_end = Some(evidence);
    }

    pub(crate) fn record_market_price(&mut self, evidence: MarketPriceEvidence) {
        self.market_price = Some(evidence);
    }

    pub(crate) fn record_book(&mut self, evidence: BookEvidence) {
        self.book = Some(evidence);
    }

    pub(crate) fn render(
        &self,
        authority: AuthorityEvidence,
        mut terminal: TerminalDispositionEvidence,
    ) -> Result<String, serde_json::Error> {
        if let Some(dispatch_id) = &self.dispatch_id {
            terminal.dispatch_id = Some(dispatch_id.clone());
        }
        let version = if terminal.decline.is_some() || terminal.final_receipt.is_some() {
            TERMINAL_EVIDENCE_VERSION
        } else {
            POST_BOUNDARY_EVIDENCE_VERSION
        };
        serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(
            DecisionPostBoundaryEvidenceBody {
                version,
                owners: EVIDENCE_OWNERS.into_iter().map(str::to_owned).collect(),
                source_trade_id: self.source_trade_id.clone(),
                applied_configuration_hash: self.applied_configuration_hash.clone(),
                market_end: self.market_end.clone(),
                market_price: self.market_price.clone(),
                book: self.book.clone(),
                clocks: self.clocks.clone(),
                authority,
                terminal,
            },
        )?)
    }

    /// Durable pre-side-effect checkpoint. A crash recovery combines these exact
    /// decision inputs with the idempotent authority result and recorded paper frame.
    pub(crate) fn checkpoint_json(&self) -> Result<String, serde_json::Error> {
        let body = DecisionEvidenceCheckpointBody {
            version: POST_BOUNDARY_EVIDENCE_VERSION,
            owners: EVIDENCE_OWNERS.into_iter().map(str::to_owned).collect(),
            source_trade_id: self.source_trade_id.clone(),
            applied_configuration_hash: self.applied_configuration_hash.clone(),
            market_end: self.market_end.clone(),
            market_price: self.market_price.clone(),
            book: self.book.clone(),
            clocks: self.clocks.clone(),
            dispatch_id: self.dispatch_id.clone(),
        };
        let financial_semantic_version = crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION;
        let document_blake3 = checkpoint_hash(&body, financial_semantic_version)?;
        serde_json::to_string(&DecisionEvidenceCheckpoint {
            body,
            financial_semantic_version,
            document_blake3,
        })
    }

    pub(crate) fn from_pending_checkpoint(
        row: &DecisionPendingRow,
    ) -> Result<Self, ReplayDecisionError> {
        let evidence = Self::from_checkpoint(row)?;
        if DecisionContinuationV3::from_durable(row)?.version() == 5 {
            paper_prepared_gate_clock(&evidence.clocks)?
                .ok_or(ReplayDecisionError::PaperPreparedClock)?;
        }
        Ok(evidence)
    }

    fn from_checkpoint(row: &DecisionPendingRow) -> Result<Self, ReplayDecisionError> {
        let continuation = DecisionContinuationV3::from_durable(row)?;
        let frozen = &continuation.facts;
        let checkpoint = decode_checkpoint(&row.post_commit_inputs_json)?;
        if checkpoint.body.owners
            != EVIDENCE_OWNERS
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        {
            return Err(ReplayDecisionError::Owners);
        }
        if checkpoint
            .financial_semantic_version
            .is_some_and(|version| version != crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION)
            || checkpoint.body.source_trade_id != frozen.source_trade_id
            || checkpoint.body.applied_configuration_hash != frozen.applied_configuration_hash
        {
            return Err(ReplayDecisionError::FrozenMismatch);
        }
        if continuation.version() == 5 {
            validate_staged_dispatch(
                &continuation,
                &checkpoint.body.clocks,
                checkpoint.body.dispatch_id.as_deref(),
            )?;
        }
        Ok(Self {
            source_trade_id: checkpoint.body.source_trade_id,
            applied_configuration_hash: checkpoint.body.applied_configuration_hash,
            market_end: checkpoint.body.market_end,
            market_price: checkpoint.body.market_price,
            book: checkpoint.body.book,
            clocks: checkpoint.body.clocks,
            dispatch_id: checkpoint.body.dispatch_id,
        })
    }

    pub(crate) fn record_staged_dispatch(&mut self, dispatch_id: String) {
        self.dispatch_id = Some(dispatch_id);
    }

    pub(crate) fn staged_dispatch_id(&self) -> Option<&str> {
        self.dispatch_id.as_deref()
    }

    /// Resume only staging ownership; inputs and admission clocks are sampled again before
    /// a new Prepared. Recovery of an existing Prepared uses `from_pending_checkpoint`.
    pub(crate) fn resume_staging_checkpoint(
        row: &DecisionPendingRow,
    ) -> Result<Self, ReplayDecisionError> {
        let mut evidence = Self::from_checkpoint(row)?;
        evidence.market_end = None;
        evidence.market_price = None;
        evidence.book = None;
        evidence
            .clocks
            .retain(|clock| clock.purpose == "dispatch_seed_created");
        Ok(evidence)
    }

    pub(crate) fn record_precise_clock(
        &mut self,
        purpose: &str,
        instant: time::OffsetDateTime,
    ) -> Result<(), ReplayDecisionError> {
        self.clocks.push(DecisionClockEvidence::precise(
            purpose,
            instant.unix_timestamp_nanos(),
        )?);
        Ok(())
    }
}

fn validate_clock_precision(clocks: &[DecisionClockEvidence]) -> Result<(), ReplayDecisionError> {
    for clock in clocks {
        clock.validate_precision()?;
    }
    Ok(())
}

pub(crate) fn paper_prepared_gate_clock(
    clocks: &[DecisionClockEvidence],
) -> Result<Option<time::OffsetDateTime>, ReplayDecisionError> {
    let mut gates = clocks
        .iter()
        .filter(|clock| clock.purpose == "paper_prepared_staleness_gate");
    let instant = gates
        .next()
        .map(DecisionClockEvidence::precise_instant)
        .transpose()?;
    if gates.next().is_some() {
        return Err(ReplayDecisionError::PaperPreparedClock);
    }
    Ok(instant)
}

fn validate_staged_dispatch(
    continuation: &DecisionContinuationV3,
    clocks: &[DecisionClockEvidence],
    dispatch_id: Option<&str>,
) -> Result<(), ReplayDecisionError> {
    let staging_clocks = clocks
        .iter()
        .filter(|clock| clock.purpose == "dispatch_seed_created")
        .count();
    if staging_clocks != usize::from(dispatch_id.is_some()) {
        return Err(ReplayDecisionError::ContinuationBinding);
    }
    if let Some(dispatch_id) = dispatch_id {
        let frozen = &continuation.facts;
        let expected = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
            &pe_core_types::TraderId(frozen.wallet).to_string(),
            &frozen.source_trade_id.0,
            &frozen.market_id.0.0,
            frozen.outcome_id.0,
            frozen.side,
            frozen.source_epoch,
        );
        if dispatch_id != expected {
            return Err(ReplayDecisionError::ContinuationBinding);
        }
    }
    Ok(())
}

pub(crate) fn ladder_plan_blake3(plan: &LadderPlan) -> String {
    let body = serde_json::json!({
        "used_asks": plan.used_asks.iter().map(|level| serde_json::json!({
            "price": level.price.0.normalize().to_string(),
            "shares_atomic": level.shares.atomic(),
        })).collect::<Vec<_>>(),
        "best_ask": plan.best_ask.0.normalize().to_string(),
        "limit_price": plan.limit_price.0.normalize().to_string(),
        "shares_atomic": plan.shares.atomic(),
        "worst_case_debit_atomic": plan.worst_case_debit.atomic(),
    });
    blake3::hash(body.to_string().as_bytes())
        .to_hex()
        .to_string()
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplayedDecision {
    pub continuation: DecisionContinuationV3,
    pub post_boundary: DecisionPostBoundaryEvidence,
    /// Exact durable terminal-decision bytes, retained after validation.
    pub recorded_decision_json: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayDecisionError {
    #[error(
        "terminal evidence is not bound to the frozen continuation (market/outcome/side/idempotency)"
    )]
    ContinuationBinding,
    #[error("authority outcome contradicts the terminal disposition")]
    AuthorityBinding,
    #[error("typed terminal evidence contradicts its disposition")]
    TerminalEvidenceBinding,
    #[error("decision_pending row is not terminal")]
    OpenRow,
    #[error("frozen decision continuation: {0}")]
    Continuation(#[from] DecisionContinuationError),
    #[error("post-boundary evidence json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported post-boundary evidence version {0}")]
    Version(u16),
    #[error(
        "post-boundary evidence version {version} has invalid financial_semantic_version field presence"
    )]
    FinancialSemanticField { version: u16 },
    #[error("post-boundary evidence owners are invalid")]
    Owners,
    #[error("post-boundary evidence does not match the frozen decision")]
    FrozenMismatch,
    #[error("post-boundary terminal transition does not match the durable row")]
    TerminalMismatch,
    #[error("decision clock has invalid or missing exact precision")]
    ClockPrecision,
    #[error("paper Prepared gate clock contradicts the continuation or terminal shape")]
    PaperPreparedClock,
    #[error("post-boundary document hash mismatch: expected {expected}, actual {actual}")]
    DocumentHash { expected: String, actual: String },
}

/// Reconstruct and validate one terminal decision using only its durable SQLite row.
pub fn replay_decision_pending(
    row: &DecisionPendingRow,
) -> Result<ReplayedDecision, ReplayDecisionError> {
    if row.state != DecisionPendingState::Terminal {
        return Err(ReplayDecisionError::OpenRow);
    }
    let continuation = DecisionContinuationV3::from_durable(row)?;
    let frozen = &continuation.facts;
    let decoded = decode_decision_evidence(&row.post_commit_inputs_json)?;
    let post_boundary = decoded.evidence;
    if post_boundary.body.owners
        != EVIDENCE_OWNERS
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    {
        return Err(ReplayDecisionError::Owners);
    }
    if (!decoded.legacy
        && post_boundary.financial_semantic_version
            != crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION)
        || post_boundary.body.source_trade_id != frozen.source_trade_id
        || post_boundary.body.applied_configuration_hash != frozen.applied_configuration_hash
        || frozen.applied_configuration.canonical_hash() != frozen.applied_configuration_hash
    {
        return Err(ReplayDecisionError::FrozenMismatch);
    }
    if row.terminal_disposition.as_deref() != Some(post_boundary.body.terminal.disposition.as_str())
    {
        return Err(ReplayDecisionError::TerminalMismatch);
    }
    // Semantic binding (#544 review rounds 3-4): a self-consistent document that
    // describes a DIFFERENT market/outcome/side/identity than the frozen
    // continuation — or an authority outcome contradicting the disposition —
    // must fail, or a recomputed-hash forgery replays as valid.
    let market = frozen.market_id.to_string();
    if let Some(evidence) = post_boundary.body.market_end.as_ref()
        && evidence.market_id != market
    {
        return Err(ReplayDecisionError::ContinuationBinding);
    }
    if let Some(evidence) = post_boundary.body.market_price.as_ref()
        && (evidence.market_id != market || evidence.outcome_id != frozen.outcome_id.0)
    {
        return Err(ReplayDecisionError::ContinuationBinding);
    }
    let disposition = post_boundary.body.terminal.disposition.as_str();
    let terminal = &post_boundary.body.terminal;
    if continuation.version() == 5 {
        if disposition == "dispatch_staged" {
            return Err(ReplayDecisionError::TerminalEvidenceBinding);
        }
        validate_staged_dispatch(
            &continuation,
            &post_boundary.body.clocks,
            terminal.dispatch_id.as_deref(),
        )?;
        let gate = paper_prepared_gate_clock(&post_boundary.body.clocks)?;
        let final_fill = terminal.disposition == "fill"
            && terminal.final_receipt.is_some()
            && terminal.fill.is_none()
            && terminal.decline.is_none();
        let expired = terminal.reason == "paper_stale_before_prepared";
        if expired
            && post_boundary.body.authority
                != AuthorityEvidence::not_read("terminal_before_fill_authority")
        {
            return Err(ReplayDecisionError::AuthorityBinding);
        }
        if gate.is_some() != (final_fill || expired)
            || (terminal.disposition == "fill" && !final_fill)
            || (expired
                && (terminal.disposition != "no_fill"
                    || terminal.fill.is_some()
                    || terminal.final_receipt.is_some()
                    || terminal.decline.is_some()))
        {
            return Err(ReplayDecisionError::PaperPreparedClock);
        }
    }
    if post_boundary.body.version == TERMINAL_EVIDENCE_VERSION {
        let typed_decline = terminal.decline.is_some()
            && terminal.disposition == "no_fill"
            && terminal.fill.is_none()
            && terminal.final_receipt.is_none();
        let final_fill = terminal.final_receipt.is_some()
            && terminal.disposition == "fill"
            && terminal.decline.is_none()
            && terminal.fill.is_none();
        if !typed_decline && !final_fill {
            return Err(ReplayDecisionError::TerminalEvidenceBinding);
        }
    }
    if terminal.decline.is_some()
        && (terminal.disposition != "no_fill"
            || terminal.fill.is_some()
            || terminal.final_receipt.is_some())
    {
        return Err(ReplayDecisionError::TerminalEvidenceBinding);
    }
    match (terminal.fill.as_ref(), terminal.final_receipt) {
        (Some(_), Some(_)) => return Err(ReplayDecisionError::TerminalEvidenceBinding),
        (Some(fill), None) => {
            if disposition != "fill" {
                // Only a fill disposition may carry recorded fill evidence.
                return Err(ReplayDecisionError::ContinuationBinding);
            }
            let expected_key = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                &pe_core_types::TraderId(frozen.wallet).to_string(),
                &frozen.source_trade_id.0,
                &frozen.market_id.0.0,
                frozen.outcome_id.0,
                frozen.side,
                frozen.source_epoch,
            );
            if fill.market_id != frozen.market_id.0.0
                || fill.outcome_id != frozen.outcome_id.0
                || !fill.side.eq_ignore_ascii_case(match frozen.side {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                })
                || fill.idempotency_key != expected_key
            {
                return Err(ReplayDecisionError::ContinuationBinding);
            }
        }
        (None, Some(_)) => {
            if disposition != "fill" {
                return Err(ReplayDecisionError::TerminalEvidenceBinding);
            }
        }
        (None, None) => {
            if disposition == "fill" {
                return Err(ReplayDecisionError::AuthorityBinding);
            }
        }
    }
    // Authority is validated as the exact (kind, outcome) pair the production
    // emitters produce (#544 review round 5): commit_fill_v2 →
    // applied|existing (fill) | settled_refusal (non-fill); the legacy
    // paper_state_sqlite protocol → committed (fill) | settled_refusal
    // (non-fill); not_read
    // carries a typed reason and is always non-fill. Unknown kinds reject.
    let authority_kind = post_boundary.body.authority.kind.as_str();
    let authority_outcome = post_boundary.body.authority.outcome.as_str();
    let is_fill = disposition == "fill";
    let authority_valid = match authority_kind {
        "commit_fill_v2" => match authority_outcome {
            "applied" | "existing" => is_fill,
            "settled_refusal" => !is_fill,
            _ => false,
        },
        "paper_state_sqlite" => match authority_outcome {
            "committed" | "recovered_from_paper_log" => is_fill,
            "settled_refusal" | "recovered_settled_refusal" => !is_fill,
            _ => false,
        },
        "not_read" => !is_fill,
        _ => false,
    };
    if !authority_valid {
        return Err(ReplayDecisionError::AuthorityBinding);
    }
    Ok(ReplayedDecision {
        continuation,
        post_boundary,
        recorded_decision_json: row.post_commit_inputs_json.clone(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use pe_copy_signal_engine::TradeProvenance;
    use pe_core_types::{
        LeaderAction, MarketId, OutcomeId, ProbabilityPpm, ShareAmount, Side, VenueMarketId,
        WalletAddress,
    };
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::bucket_commit::{pre_545_frozen_inputs, synthetic_legacy17_runtime_config};
    use crate::runtime_config::RuntimeConfig;

    const ORIGIN_MAIN_TERMINAL: &str =
        include_str!("../tests/fixtures/decision_replay_origin_main_v2_terminal.json");
    const ORIGIN_MAIN_CHECKPOINT: &str =
        include_str!("../tests/fixtures/decision_replay_origin_main_v2_checkpoint.json");

    /// PASS: exact clocks round-trip before and after the epoch, and both decoders reject an
    /// out-of-range remainder even when the evidence hash has been recomputed.
    #[test]
    fn precise_clocks_use_euclidean_milliseconds_and_validate_both_decoders() {
        for nanos in [-1, 0, 2_000_000_000, 2_000_000_001] {
            let clock =
                DecisionClockEvidence::precise("paper_prepared_staleness_gate", nanos).unwrap();
            assert_eq!(
                clock.precise_instant().unwrap().unix_timestamp_nanos(),
                nanos
            );
            if nanos == -1 {
                assert_eq!(clock.unix_millis, -1);
                assert_eq!(clock.submillisecond_nanos, Some(999_999));
            }
        }
        assert!(DecisionClockEvidence::precise("overflow", i128::MAX).is_err());
        let mut accumulator = DecisionEvidenceAccumulator::new(&legacy17_continuation(
            &SourceTradeId("g2:precision".to_owned()),
        ));
        accumulator.clocks.push(DecisionClockEvidence {
            purpose: "paper_prepared_staleness_gate".to_owned(),
            unix_millis: 0,
            submillisecond_nanos: Some(1_000_000),
        });
        assert!(matches!(
            decode_checkpoint(&accumulator.checkpoint_json().unwrap()),
            Err(ReplayDecisionError::ClockPrecision)
        ));
        assert!(matches!(
            decode_decision_evidence(
                &accumulator
                    .render(
                        AuthorityEvidence::not_read("terminal_before_fill_authority"),
                        TerminalDispositionEvidence::no_fill("paper_stale_before_prepared"),
                    )
                    .unwrap()
            ),
            Err(ReplayDecisionError::ClockPrecision)
        ));
    }

    fn wallet() -> WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    fn legacy17_continuation(source_trade_id: &SourceTradeId) -> DecisionContinuationFacts {
        continuation_with_configuration(source_trade_id, origin_main_configuration())
    }

    fn continuation_with_configuration(
        source_trade_id: &SourceTradeId,
        applied_configuration: RuntimeConfig,
    ) -> DecisionContinuationFacts {
        DecisionContinuationFacts {
            paper_freshness_policy: None,
            source_trade_id: source_trade_id.clone(),
            semantic_revision: "semantic-v2".to_owned(),
            transaction_hash: "0xtransaction".to_owned(),
            wallet: wallet(),
            source_epoch: 1_700_000_000,
            market_id: MarketId(VenueMarketId(format!("0x{}", "2".repeat(40)))),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price(dec!(0.40)),
            share_amount: ShareAmount::from_whole(10).unwrap(),
            provenance: TradeProvenance::RestPoll,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
            gate_result: "admitted".to_owned(),
            frozen_basis: crate::bucket_commit::FrozenDecisionBasis {
                win_rate_p: pe_core_types::Probability::ZERO,
                bankroll: rust_decimal::Decimal::ZERO,
            },
            applied_configuration_hash: applied_configuration.canonical_hash(),
            applied_configuration,
            decision_inputs: json!({"fixed_end": 1_700_000_010_i64, "pages": 1}),
        }
    }

    fn origin_main_configuration() -> RuntimeConfig {
        let configuration = synthetic_legacy17_runtime_config();
        assert_eq!(
            configuration.canonical_hash(),
            "f602cee694f90f8e48cdd43e70d6d9398879a9991662492af82ec4f7df31b222"
        );
        configuration
    }

    fn origin_main_row(
        source_trade_id: SourceTradeId,
        post_commit_inputs_json: &str,
        state: DecisionPendingState,
        terminal_disposition: Option<&str>,
    ) -> DecisionPendingRow {
        let continuation =
            continuation_with_configuration(&source_trade_id, origin_main_configuration());
        DecisionPendingRow {
            source_trade_id,
            semantic_revision: continuation.semantic_revision.clone(),
            wallet: continuation.wallet,
            source_epoch: continuation.source_epoch,
            frozen_inputs_json: legacy_v2_json(&continuation),
            post_commit_inputs_json: post_commit_inputs_json.to_owned(),
            state,
            terminal_disposition: terminal_disposition.map(str::to_owned),
            updated_at_unix: 1_700_000_001,
        }
    }

    fn legacy_v2_json(facts: &DecisionContinuationFacts) -> String {
        pre_545_frozen_inputs(facts)
    }

    fn accumulator(continuation: &DecisionContinuationFacts) -> DecisionEvidenceAccumulator {
        let mut evidence = DecisionEvidenceAccumulator::new(continuation);
        evidence.record_market_end(MarketEndEvidence {
            market_id: continuation.market_id.to_string(),
            resolution_unix: Some(1_700_086_400),
            source: "gamma.uma_end_date".to_owned(),
        });
        evidence.record_market_price(MarketPriceEvidence {
            market_id: continuation.market_id.to_string(),
            outcome_id: continuation.outcome_id.0,
            mid_price: Some("0.41".to_owned()),
            source: "gamma.outcome_prices".to_owned(),
        });
        evidence.record_book(BookEvidence {
            request_token_id: Some("token-0".to_owned()),
            outcome: "planned".to_owned(),
            response_blake3: Some("book-blake3".to_owned()),
            fetched_at_unix_ms: Some(1_700_000_000_100),
            best_ask: Some("0.42".to_owned()),
            vwap_basis: Some("0.425".to_owned()),
            ladder_plan_blake3: Some("ladder-blake3".to_owned()),
            reason: None,
        });
        evidence.record_clock("book_staleness_check", 1_700_000_000_125);
        evidence
    }

    fn unavailable_inputs() -> WinnerFollowDecisionInputs {
        WinnerFollowDecisionInputs::RiskInputsUnavailable {
            cause: crate::risk_inputs::RiskInputsUnavailable::PriceMissing,
            evidence: WinnerFollowRiskInputEvidence {
                financial_prefix: None,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 1_700_000_000_000,
                proposed_debit: CollateralAmount::ZERO,
                per_trade_cap_bps: 10_000,
            },
        }
    }

    fn terminal_row(
        source_suffix: &str,
        authority: AuthorityEvidence,
        terminal: TerminalDispositionEvidence,
    ) -> DecisionPendingRow {
        let source_trade_id = SourceTradeId(format!("g2:{source_suffix}"));
        let continuation = legacy17_continuation(&source_trade_id);
        let evidence = accumulator(&continuation)
            .render(authority, terminal.clone())
            .unwrap();
        DecisionPendingRow {
            source_trade_id,
            semantic_revision: continuation.semantic_revision.clone(),
            wallet: continuation.wallet,
            source_epoch: continuation.source_epoch,
            frozen_inputs_json: legacy_v2_json(&continuation),
            post_commit_inputs_json: evidence,
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some(terminal.disposition),
            updated_at_unix: 1_700_000_001,
        }
    }

    fn assert_byte_exact_replay(row: &DecisionPendingRow) -> ReplayedDecision {
        let replayed = replay_decision_pending(row).unwrap();
        assert_eq!(
            serde_json::to_string(&replayed.post_boundary)
                .unwrap()
                .as_bytes(),
            row.post_commit_inputs_json.as_bytes()
        );
        assert_eq!(
            replayed.recorded_decision_json.as_bytes(),
            row.post_commit_inputs_json.as_bytes()
        );
        replayed
    }

    #[test]
    fn replay_fill_no_copy_and_refusal_byte_exactly() {
        let fill = terminal_row(
            "fill",
            AuthorityEvidence::commit_fill_v2("applied", dec!(996)),
            TerminalDispositionEvidence::fill(
                // The semantic binding requires EXACT canonical key equality:
                // derive it from the same parts owner production uses.
                pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                    &pe_core_types::TraderId(wallet()).to_string(),
                    "g2:fill",
                    &format!("0x{}", "2".repeat(40)),
                    0,
                    pe_core_types::Side::Buy,
                    1_700_000_000,
                ),
                format!("0x{}", "2".repeat(40)),
                0,
                "buy",
                10,
                Price(dec!(0.40)),
                EventSeq(7),
            ),
        );
        let replayed = assert_byte_exact_replay(&fill);
        assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");

        let no_copy = terminal_row(
            "no-copy",
            AuthorityEvidence::not_read("terminal_before_fill_authority"),
            TerminalDispositionEvidence::no_copy("stale_fallback_past_copy_budget"),
        );
        let replayed = assert_byte_exact_replay(&no_copy);
        assert_eq!(
            replayed.post_boundary.body.terminal.disposition,
            "no_copy:stale_fallback_past_copy_budget"
        );

        let refusal = terminal_row(
            "refusal",
            AuthorityEvidence::commit_fill_v2("settled_refusal", dec!(996)),
            TerminalDispositionEvidence::settled_refusal(),
        );
        let replayed = assert_byte_exact_replay(&refusal);
        assert_eq!(
            replayed.post_boundary.body.authority.outcome,
            "settled_refusal"
        );
    }

    #[test]
    fn terminal_v5_keeps_legacy_body_readable_and_records_typed_outcomes() {
        let legacy: TerminalDispositionEvidence = serde_json::from_value(json!({
            "disposition": "no_fill",
            "reason": "legacy",
            "fill": null,
            "dispatch_id": null
        }))
        .unwrap();
        assert_eq!(legacy.decline, None);
        assert_eq!(legacy.final_receipt, None);
        assert!(
            !serde_json::to_value(&legacy)
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("decline")
        );

        let declined = TerminalDispositionEvidence::declined(
            &pe_strategy_winner_follow::WinnerFollowError::NoEdge,
            unavailable_inputs(),
        );
        assert_eq!(declined.disposition, "no_fill");
        assert_eq!(
            declined.reason,
            "paper_reject:no edge: Kelly sizing produced zero contracts"
        );
        assert_eq!(
            declined.decline.as_ref().map(|decline| &decline.outcome),
            Some(&pe_strategy_winner_follow::WinnerFollowDeclineAudit::NoEdge)
        );
        let declined_row = terminal_row(
            "typed-decline",
            AuthorityEvidence::not_read("strategy_declined"),
            declined,
        );
        let replayed = replay_decision_pending(&declined_row).unwrap();
        assert_eq!(
            replayed.post_boundary.body.version,
            TERMINAL_EVIDENCE_VERSION
        );

        let receipt = AppendReceipt {
            sequence: EventSeq(11),
            this_hash: blake3::Hash::from_bytes([11; 32]),
        };
        let final_fill = TerminalDispositionEvidence::final_fill(receipt);
        assert_eq!(final_fill.final_receipt, Some(receipt));
        let fill_row = terminal_row(
            "final-fill",
            AuthorityEvidence::commit_fill_v2("applied", dec!(996)),
            final_fill,
        );
        let replayed = replay_decision_pending(&fill_row).unwrap();
        assert_eq!(
            replayed.post_boundary.body.version,
            TERMINAL_EVIDENCE_VERSION
        );
        assert_eq!(TERMINAL_EVIDENCE_VERSION, 5);
    }

    /// PASS: the byte-exact origin/main v2 terminal document verifies its body-only hash and
    /// replays end-to-end without inventing a pre-Start financial-semantic binding.
    #[test]
    fn origin_main_terminal_fixture_replays_and_rejects_tampering() {
        let mut row = origin_main_row(
            SourceTradeId("g2:fill".to_owned()),
            ORIGIN_MAIN_TERMINAL,
            DecisionPendingState::Terminal,
            Some("fill"),
        );
        let replayed = replay_decision_pending(&row).unwrap();
        assert_eq!(
            replayed.post_boundary.body.version,
            LEGACY_POST_BOUNDARY_EVIDENCE_VERSION
        );
        assert_eq!(
            replayed.post_boundary.financial_semantic_version,
            LEGACY_FINANCIAL_SEMANTIC_VERSION
        );
        assert_eq!(replayed.recorded_decision_json, ORIGIN_MAIN_TERMINAL);
        assert!(
            replayed
                .post_boundary
                .body
                .clocks
                .iter()
                .all(|clock| clock.submillisecond_nanos.is_none())
        );

        let mut document: serde_json::Value = serde_json::from_str(ORIGIN_MAIN_TERMINAL).unwrap();
        document["terminal"]["fill"]["contracts"] = json!(11);
        row.post_commit_inputs_json = serde_json::to_string(&document).unwrap();
        assert!(matches!(
            replay_decision_pending(&row),
            Err(ReplayDecisionError::DocumentHash { .. })
        ));
    }

    /// PASS: the byte-exact origin/main open checkpoint verifies its body-only hash before its
    /// fields are converted into an accumulator and re-rendered under the current wire identity.
    #[test]
    fn origin_main_checkpoint_fixture_recovers_and_upgrades() {
        let row = origin_main_row(
            SourceTradeId("g2:checkpoint".to_owned()),
            ORIGIN_MAIN_CHECKPOINT,
            DecisionPendingState::Open,
            None,
        );
        let recovered = DecisionEvidenceAccumulator::from_pending_checkpoint(&row).unwrap();
        assert!(
            recovered
                .clocks
                .iter()
                .all(|clock| clock.submillisecond_nanos.is_none())
        );
        assert!(
            !recovered
                .checkpoint_json()
                .unwrap()
                .contains("submillisecond_nanos")
        );
        assert_eq!(
            recovered
                .book
                .as_ref()
                .and_then(|book| book.best_ask.as_deref()),
            Some("0.42")
        );
        let upgraded: serde_json::Value =
            serde_json::from_str(&recovered.checkpoint_json().unwrap()).unwrap();
        assert_eq!(upgraded["version"], json!(POST_BOUNDARY_EVIDENCE_VERSION));
        assert_eq!(
            upgraded["financial_semantic_version"],
            json!(crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION)
        );
    }

    /// PASS: continuation generations four and five retain their exact fixture bytes; four
    /// replays the legacy terminal, while five requires its precise final-gate evidence.
    #[test]
    fn continuation_four_and_five_byte_fixtures_replay_unchanged() {
        use crate::bucket_commit::{
            ActivityReadCommitmentReceipt, PageOccurrence, PaperFreshnessPolicy,
        };
        use pe_source_polymarket_public::{ActivityRequestBounds, ReconciliationPageEvidence};
        for version in [4, 5] {
            let mut row = origin_main_row(
                SourceTradeId("g2:fill".to_owned()),
                ORIGIN_MAIN_TERMINAL,
                DecisionPendingState::Terminal,
                Some("fill"),
            );
            let mut facts = legacy17_continuation(&row.source_trade_id);
            facts.paper_freshness_policy = (version == 5).then_some(PaperFreshnessPolicy {
                activity_ws_enabled: true,
                copy_latency_budget_secs: 2,
            });
            let receipt = AppendReceipt {
                sequence: EventSeq(2),
                this_hash: blake3::Hash::from_bytes([2; 32]),
            };
            let commitment = AppendReceipt {
                sequence: EventSeq(3),
                this_hash: blake3::Hash::from_bytes([3; 32]),
            };
            let page = PageOccurrence {
                request_url: "https://data-api.polymarket.com/activity?fixture=byte-compatibility"
                    .to_owned(),
                raw_hash: blake3::hash(b"[]").to_hex().to_string(),
                receipt,
            };
            let evidence = ReconciliationPageEvidence {
                request_url: page.request_url.clone(),
                bounds: Some(ActivityRequestBounds {
                    start: None,
                    end: 1_700_000_010,
                }),
                partition: None,
                offset: 0,
                row_count: 0,
                canonical_page_hash: pe_source_polymarket_public::canonical_page_hash(b"[]")
                    .unwrap(),
                raw_page_hash: page.raw_hash.clone(),
                received_at: pe_core_types::ReceivedAt(
                    time::OffsetDateTime::from_unix_timestamp(1_700_000_010).unwrap(),
                ),
                schema_version: 2,
                parser_version: 2,
            };
            facts.decision_inputs = json!({"fixed_end":1700000010,"pages":[evidence]});
            let continuation = DecisionContinuationV3::new(
                facts,
                None,
                vec![page],
                Some(if version == 5 {
                    ActivityReadCommitmentReceipt::BindingsV2(commitment)
                } else {
                    ActivityReadCommitmentReceipt::LegacyV1(commitment)
                }),
            );
            row.frozen_inputs_json = serde_json::to_string(&continuation).unwrap();
            let expected = if version == 5 {
                include_str!("../tests/fixtures/decision_continuation_v5.json")
            } else {
                include_str!("../tests/fixtures/decision_continuation_v4.json")
            };
            assert_eq!(row.frozen_inputs_json.as_bytes(), expected.as_bytes());
            row.frozen_inputs_json = expected.to_owned();
            if version == 5 {
                assert!(matches!(
                    replay_decision_pending(&row),
                    Err(ReplayDecisionError::PaperPreparedClock)
                ));
                let decoded = DecisionContinuationV3::from_durable(&row).unwrap();
                assert_eq!(serde_json::to_string(&decoded).unwrap(), expected);
                continue;
            }
            let replayed = replay_decision_pending(&row).unwrap();
            assert_eq!(replayed.continuation.version(), version);
            assert_eq!(replayed.recorded_decision_json, ORIGIN_MAIN_TERMINAL);
            assert_eq!(
                serde_json::to_string(&replayed.continuation).unwrap(),
                row.frozen_inputs_json
            );
        }
    }

    /// PASS: a current typed terminal that lost its financial semantic field is rejected as
    /// ambiguous instead of being decoded under either generation.
    #[test]
    fn current_terminal_without_financial_field_is_ambiguous() {
        let mut row = terminal_row(
            "missing-financial-field",
            AuthorityEvidence::not_read("strategy_declined"),
            TerminalDispositionEvidence::declined(
                &pe_strategy_winner_follow::WinnerFollowError::NoEdge,
                unavailable_inputs(),
            ),
        );
        let mut missing_field: serde_json::Value =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        missing_field
            .as_object_mut()
            .unwrap()
            .remove("financial_semantic_version");
        row.post_commit_inputs_json = serde_json::to_string(&missing_field).unwrap();
        assert!(matches!(
            replay_decision_pending(&row),
            Err(ReplayDecisionError::FinancialSemanticField {
                version: TERMINAL_EVIDENCE_VERSION
            })
        ));
    }

    #[test]
    fn replay_rejects_typed_terminal_decline_or_final_receipt_contradictions() {
        let mut row = terminal_row(
            "bad-typed-terminal",
            AuthorityEvidence::not_read("strategy_declined"),
            TerminalDispositionEvidence::declined(
                &pe_strategy_winner_follow::WinnerFollowError::NoEdge,
                unavailable_inputs(),
            ),
        );
        let document: DecisionPostBoundaryEvidence =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        let mut body = document.body;
        body.terminal.decline = None;
        row.post_commit_inputs_json =
            serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(body).unwrap()).unwrap();
        assert!(matches!(
            replay_decision_pending(&row),
            Err(ReplayDecisionError::TerminalEvidenceBinding)
        ));
    }

    /// PASS: structural replay does not interpret display text as Winner-Follow semantics; the
    /// qualification verifier independently re-executes the retained typed decision inputs.
    #[test]
    fn replay_does_not_make_decline_display_text_a_semantic_owner() {
        let mut row = terminal_row(
            "display-only",
            AuthorityEvidence::not_read("strategy_declined"),
            TerminalDispositionEvidence::declined(
                &pe_strategy_winner_follow::WinnerFollowError::NoEdge,
                unavailable_inputs(),
            ),
        );
        let document: DecisionPostBoundaryEvidence =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        let mut body = document.body;
        body.terminal.reason = "operator-facing text changed".to_owned();
        row.post_commit_inputs_json =
            serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(body).unwrap()).unwrap();

        replay_decision_pending(&row).unwrap();
    }

    #[test]
    fn replay_rejects_tampered_post_boundary_document() {
        let mut row = terminal_row(
            "tampered",
            AuthorityEvidence::not_read("terminal_before_fill_authority"),
            TerminalDispositionEvidence::no_fill("fill_price_at_or_above_max"),
        );
        let mut document: serde_json::Value =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        document["terminal"]["reason"] = json!("tampered");
        row.post_commit_inputs_json = serde_json::to_string(&document).unwrap();

        assert!(matches!(
            replay_decision_pending(&row),
            Err(ReplayDecisionError::DocumentHash { .. })
        ));
    }

    #[test]
    fn pre_side_effect_checkpoint_round_trips_and_rejects_tampering() {
        let source_trade_id = SourceTradeId("g2:checkpoint".to_owned());
        let continuation = legacy17_continuation(&source_trade_id);
        let evidence = accumulator(&continuation);
        let checkpoint = evidence.checkpoint_json().unwrap();
        let mut row = DecisionPendingRow {
            source_trade_id,
            semantic_revision: continuation.semantic_revision.clone(),
            wallet: continuation.wallet,
            source_epoch: continuation.source_epoch,
            frozen_inputs_json: legacy_v2_json(&continuation),
            post_commit_inputs_json: checkpoint,
            state: DecisionPendingState::Open,
            terminal_disposition: None,
            updated_at_unix: 1_700_000_001,
        };
        let recovered = DecisionEvidenceAccumulator::from_pending_checkpoint(&row).unwrap();
        assert_eq!(recovered.source_trade_id, continuation.source_trade_id);
        assert_eq!(recovered.book, evidence.book);

        let current_json = row.post_commit_inputs_json.clone();
        let mut missing_field: serde_json::Value = serde_json::from_str(&current_json).unwrap();
        missing_field
            .as_object_mut()
            .unwrap()
            .remove("financial_semantic_version");
        row.post_commit_inputs_json = serde_json::to_string(&missing_field).unwrap();
        assert!(matches!(
            DecisionEvidenceAccumulator::from_pending_checkpoint(&row),
            Err(ReplayDecisionError::FinancialSemanticField {
                version: POST_BOUNDARY_EVIDENCE_VERSION
            })
        ));

        row.post_commit_inputs_json = current_json;
        let current: DecisionEvidenceCheckpoint =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        let mut legacy_body = current.body;
        legacy_body.version = LEGACY_POST_BOUNDARY_EVIDENCE_VERSION;
        row.post_commit_inputs_json = serde_json::to_string(&LegacyDecisionEvidenceCheckpoint {
            document_blake3: legacy_checkpoint_hash(&legacy_body).unwrap(),
            body: legacy_body,
        })
        .unwrap();
        let recovered = DecisionEvidenceAccumulator::from_pending_checkpoint(&row).unwrap();
        assert_eq!(recovered.source_trade_id, continuation.source_trade_id);
        let rerendered: serde_json::Value =
            serde_json::from_str(&recovered.checkpoint_json().unwrap()).unwrap();
        assert_eq!(rerendered["version"], json!(POST_BOUNDARY_EVIDENCE_VERSION));
        assert_eq!(
            rerendered["financial_semantic_version"],
            json!(crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION)
        );

        let mut document: serde_json::Value =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        document["book"]["best_ask"] = json!("0.99");
        row.post_commit_inputs_json = serde_json::to_string(&document).unwrap();
        assert!(matches!(
            DecisionEvidenceAccumulator::from_pending_checkpoint(&row),
            Err(ReplayDecisionError::DocumentHash { .. })
        ));
    }

    /// PASS: the generation-five authority restriction does not change legacy reason validation.
    #[test]
    fn legacy_expiry_reason_keeps_existing_authority_contract() {
        let row = terminal_row(
            "legacy-expiry",
            AuthorityEvidence::commit_fill_v2("settled_refusal", dec!(99)),
            TerminalDispositionEvidence::no_fill("paper_stale_before_prepared"),
        );
        assert_byte_exact_replay(&row);
    }
}
