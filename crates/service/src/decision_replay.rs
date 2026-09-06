//! Pure replay and validation for post-boundary `decision_pending` evidence.

use pe_core_types::{EventSeq, Price, Side, SourceTradeId};
use pe_event_log::AppendReceipt;
use pe_paper_state::{DecisionPendingRow, DecisionPendingState};
use pe_strategy_winner_follow::{KellyErrorAudit, WinnerFollowDeclineAudit, WinnerFollowError};
use pe_venue_polymarket::LadderPlan;
use serde::{Deserialize, Serialize};

use crate::bucket_commit::{
    DecisionContinuationError, DecisionContinuationV2, DecisionContinuationV3,
};

pub const POST_BOUNDARY_EVIDENCE_VERSION: u16 = 2;
pub const TERMINAL_EVIDENCE_VERSION: u16 = 3;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalDispositionEvidence {
    pub disposition: String,
    pub reason: String,
    pub fill: Option<RecordedFillEvidence>,
    pub dispatch_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decline: Option<WinnerFollowDeclineAudit>,
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

    pub fn declined(error: &WinnerFollowError) -> Self {
        Self {
            disposition: "no_fill".to_owned(),
            reason: format!("paper_reject:{error}"),
            fill: None,
            dispatch_id: None,
            decline: Some(WinnerFollowDeclineAudit::from(error)),
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

fn body_hash(
    body: &DecisionPostBoundaryEvidenceBody,
    financial_semantic_version: u32,
) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(&(financial_semantic_version, body))?;
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DecisionEvidenceCheckpoint {
    #[serde(flatten)]
    body: DecisionEvidenceCheckpointBody,
    financial_semantic_version: u32,
    document_blake3: String,
}

fn checkpoint_hash(
    body: &DecisionEvidenceCheckpointBody,
    financial_semantic_version: u32,
) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(&(financial_semantic_version, body))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

#[derive(Debug, Clone)]
pub struct DecisionEvidenceAccumulator {
    source_trade_id: SourceTradeId,
    applied_configuration_hash: String,
    market_end: Option<MarketEndEvidence>,
    market_price: Option<MarketPriceEvidence>,
    book: Option<BookEvidence>,
    clocks: Vec<DecisionClockEvidence>,
}

impl DecisionEvidenceAccumulator {
    pub(crate) fn new(continuation: &DecisionContinuationV2) -> Self {
        Self {
            source_trade_id: continuation.source_trade_id.clone(),
            applied_configuration_hash: continuation.applied_configuration_hash.clone(),
            market_end: None,
            market_price: None,
            book: None,
            clocks: Vec::new(),
        }
    }

    pub(crate) fn record_clock(&mut self, purpose: &str, unix_millis: i64) {
        self.clocks.push(DecisionClockEvidence {
            purpose: purpose.to_owned(),
            unix_millis,
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
        terminal: TerminalDispositionEvidence,
    ) -> Result<String, serde_json::Error> {
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
        let continuation = DecisionContinuationV3::from_durable(row)?;
        let frozen = &continuation.prior;
        let checkpoint: DecisionEvidenceCheckpoint =
            serde_json::from_str(&row.post_commit_inputs_json)?;
        if checkpoint.body.version != POST_BOUNDARY_EVIDENCE_VERSION {
            return Err(ReplayDecisionError::Version(checkpoint.body.version));
        }
        if checkpoint.body.owners
            != EVIDENCE_OWNERS
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        {
            return Err(ReplayDecisionError::Owners);
        }
        let actual = checkpoint_hash(&checkpoint.body, checkpoint.financial_semantic_version)?;
        if actual != checkpoint.document_blake3 {
            return Err(ReplayDecisionError::DocumentHash {
                expected: checkpoint.document_blake3,
                actual,
            });
        }
        if checkpoint.financial_semantic_version
            != crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION
            || checkpoint.body.source_trade_id != frozen.source_trade_id
            || checkpoint.body.applied_configuration_hash != frozen.applied_configuration_hash
        {
            return Err(ReplayDecisionError::FrozenMismatch);
        }
        Ok(Self {
            source_trade_id: checkpoint.body.source_trade_id,
            applied_configuration_hash: checkpoint.body.applied_configuration_hash,
            market_end: checkpoint.body.market_end,
            market_price: checkpoint.body.market_price,
            book: checkpoint.body.book,
            clocks: checkpoint.body.clocks,
        })
    }
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
    #[error("version-three terminal evidence contradicts its disposition")]
    TerminalEvidenceBinding,
    #[error("typed Winner-Follow decline does not match its durable reason")]
    DeclineReasonBinding,
    #[error("decision_pending row is not terminal")]
    OpenRow,
    #[error("frozen decision continuation: {0}")]
    Continuation(#[from] DecisionContinuationError),
    #[error("post-boundary evidence json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported post-boundary evidence version {0}")]
    Version(u16),
    #[error("post-boundary evidence owners are invalid")]
    Owners,
    #[error("post-boundary evidence does not match the frozen decision")]
    FrozenMismatch,
    #[error("post-boundary terminal transition does not match the durable row")]
    TerminalMismatch,
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
    let frozen = &continuation.prior;
    let post_boundary: DecisionPostBoundaryEvidence =
        serde_json::from_str(&row.post_commit_inputs_json)?;
    if !matches!(
        post_boundary.body.version,
        POST_BOUNDARY_EVIDENCE_VERSION | TERMINAL_EVIDENCE_VERSION
    ) {
        return Err(ReplayDecisionError::Version(post_boundary.body.version));
    }
    if post_boundary.body.owners
        != EVIDENCE_OWNERS
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    {
        return Err(ReplayDecisionError::Owners);
    }
    post_boundary.validate_hash()?;
    if post_boundary.financial_semantic_version != crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION
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
    if post_boundary.body.version == TERMINAL_EVIDENCE_VERSION {
        let is_typed_decline =
            terminal.disposition == "no_fill" && terminal.reason.starts_with("paper_reject:");
        if terminal.decline.is_some() != is_typed_decline
            || terminal.final_receipt.is_some() != (terminal.disposition == "fill")
            || terminal.fill.is_some()
        {
            return Err(ReplayDecisionError::TerminalEvidenceBinding);
        }
    }
    if terminal.decline.is_some()
        && (terminal.disposition != "no_fill"
            || !terminal.reason.starts_with("paper_reject:")
            || terminal.fill.is_some()
            || terminal.final_receipt.is_some())
    {
        return Err(ReplayDecisionError::TerminalEvidenceBinding);
    }
    if terminal
        .decline
        .as_ref()
        .is_some_and(|decline| !decline_matches_reason(decline, &terminal.reason))
    {
        return Err(ReplayDecisionError::DeclineReasonBinding);
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

fn decline_matches_reason(decline: &WinnerFollowDeclineAudit, reason: &str) -> bool {
    let expected = match decline {
        WinnerFollowDeclineAudit::ShadowMode => {
            "paper_reject:shadow mode: signal recorded but not executed".to_owned()
        }
        WinnerFollowDeclineAudit::FlipNotApproved => {
            "paper_reject:flip action requires human approval (flip_human_approved = false)"
                .to_owned()
        }
        WinnerFollowDeclineAudit::NoEdge => {
            "paper_reject:no edge: Kelly sizing produced zero contracts".to_owned()
        }
        WinnerFollowDeclineAudit::Blocked(block) => {
            format!("paper_reject:risk blocked: {block:?}")
        }
        WinnerFollowDeclineAudit::RiskInputsUnavailable => {
            const PREFIX: &str = "paper_reject:risk inputs unavailable";
            return reason == PREFIX
                || reason
                    .strip_prefix(PREFIX)
                    .is_some_and(|detail| detail.starts_with(": ") && detail.len() > 2);
        }
        WinnerFollowDeclineAudit::KellySizing(error) => match error {
            KellyErrorAudit::InvalidProbability { value } => format!(
                "paper_reject:Kelly sizing error: probability p must be in [0, 1], got {value}"
            ),
            KellyErrorAudit::InvalidNetPrice { value } => format!(
                "paper_reject:Kelly sizing error: net price c must be in (0, 1), got {value}"
            ),
            KellyErrorAudit::InvalidBankroll { value } => {
                format!("paper_reject:Kelly sizing error: bankroll must be positive, got {value}")
            }
            KellyErrorAudit::ContractOverflow { value } => format!(
                "paper_reject:Kelly sizing error: contract count overflow: floored value {value} out of u64 range"
            ),
        },
    };
    reason == expected
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
    use crate::config::ServiceConfig;
    use crate::runtime_config::RuntimeConfig;

    fn wallet() -> WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    fn continuation(source_trade_id: &SourceTradeId) -> DecisionContinuationV2 {
        let applied_configuration = RuntimeConfig::from_service_config(&ServiceConfig::default());
        DecisionContinuationV2 {
            version: 2,
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

    fn accumulator(continuation: &DecisionContinuationV2) -> DecisionEvidenceAccumulator {
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

    fn terminal_row(
        source_suffix: &str,
        authority: AuthorityEvidence,
        terminal: TerminalDispositionEvidence,
    ) -> DecisionPendingRow {
        let source_trade_id = SourceTradeId(format!("g2:{source_suffix}"));
        let continuation = continuation(&source_trade_id);
        let evidence = accumulator(&continuation)
            .render(authority, terminal.clone())
            .unwrap();
        DecisionPendingRow {
            source_trade_id,
            semantic_revision: continuation.semantic_revision.clone(),
            wallet: continuation.wallet,
            source_epoch: continuation.source_epoch,
            frozen_inputs_json: serde_json::to_string(&continuation).unwrap(),
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
    fn terminal_v3_keeps_v2_readable_and_records_typed_outcomes() {
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
        );
        assert_eq!(declined.disposition, "no_fill");
        assert_eq!(
            declined.reason,
            "paper_reject:no edge: Kelly sizing produced zero contracts"
        );
        assert_eq!(
            declined.decline,
            Some(pe_strategy_winner_follow::WinnerFollowDeclineAudit::NoEdge)
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
        assert_eq!(TERMINAL_EVIDENCE_VERSION, 3);
    }

    #[test]
    fn replay_rejects_v3_decline_or_final_receipt_contradictions() {
        let mut row = terminal_row(
            "bad-v3",
            AuthorityEvidence::not_read("strategy_declined"),
            TerminalDispositionEvidence::declined(
                &pe_strategy_winner_follow::WinnerFollowError::NoEdge,
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

    /// PASS: recomputing the evidence hash cannot pair a typed decline with another decline's
    /// durable display reason.
    #[test]
    fn replay_rejects_decline_enum_that_disagrees_with_reason() {
        let mut row = terminal_row(
            "wrong-decline",
            AuthorityEvidence::not_read("strategy_declined"),
            TerminalDispositionEvidence::declined(
                &pe_strategy_winner_follow::WinnerFollowError::NoEdge,
            ),
        );
        let document: DecisionPostBoundaryEvidence =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        let mut body = document.body;
        body.terminal.decline = Some(WinnerFollowDeclineAudit::RiskInputsUnavailable);
        row.post_commit_inputs_json =
            serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(body).unwrap()).unwrap();

        assert!(matches!(
            replay_decision_pending(&row),
            Err(ReplayDecisionError::DeclineReasonBinding)
        ));
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
        let continuation = continuation(&source_trade_id);
        let evidence = accumulator(&continuation);
        let checkpoint = evidence.checkpoint_json().unwrap();
        let mut row = DecisionPendingRow {
            source_trade_id,
            semantic_revision: continuation.semantic_revision.clone(),
            wallet: continuation.wallet,
            source_epoch: continuation.source_epoch,
            frozen_inputs_json: serde_json::to_string(&continuation).unwrap(),
            post_commit_inputs_json: checkpoint,
            state: DecisionPendingState::Open,
            terminal_disposition: None,
            updated_at_unix: 1_700_000_001,
        };
        let recovered = DecisionEvidenceAccumulator::from_pending_checkpoint(&row).unwrap();
        assert_eq!(recovered.source_trade_id, continuation.source_trade_id);
        assert_eq!(recovered.book, evidence.book);

        let mut document: serde_json::Value =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        document["book"]["best_ask"] = json!("0.99");
        row.post_commit_inputs_json = serde_json::to_string(&document).unwrap();
        assert!(matches!(
            DecisionEvidenceAccumulator::from_pending_checkpoint(&row),
            Err(ReplayDecisionError::DocumentHash { .. })
        ));
    }
}
