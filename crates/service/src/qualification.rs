//! Network-free financial-era preparation and sealed paper qualification (#545).
//!
//! This module deliberately owns no HTTP client. It consumes only verified framed logs and the
//! read-only paper-state projection, and emits canonical compact JSON with a trailing line feed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use pe_core_types::{
    AccountId, CollateralAmount, EventSeq, MarketId, OutcomeId, Price, ReceivedAt, ShareAmount,
    Side, SourceId, SourceTimestamp, TraderId, VenueMarketId,
};
use pe_event_log::envelope::{HashInput, compute_hashes};
use pe_event_log::{
    AppendReceipt, ContentType, EnvelopeIn, LogTailBinding, Reader, Scanner, Writer,
};
use pe_execution_core::{
    AdmissionReceipts, EconomicInputs, EconomicPrepared, LiveAdmissionArtifact, RiskAudit,
    RiskDecisionAudit, SizingModeAudit,
};
use pe_kelly_sizer::{KellyInput, size_contracts};
use pe_paper_state::{
    DecisionPendingRow, DecisionPendingState, FillRow, FinancialSnapshot, PaperPositionRow,
    PaperStateDb, SettledMarketRow,
};
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::{
    BinaryPayout, KILL_SWITCH_DRAWDOWN_BPS, RiskDecision, RiskHaltCause, RiskSnapshot,
    aggregate_resolution_credit, evaluate_risk, nearest_rank_p95,
};
use pe_source_polymarket_public::ClassifiedPricesHistory;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, ActivityType, BinaryPayoutVector,
    ClobPricesHistoryClient, GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION,
    GAMMA_MARKETS_SOURCE_ID, GammaMarketsClient, LIVE_MARKET_PARSER_VERSION,
    LIVE_MARKET_SCHEMA_VERSION, MarketFilter, PageFetcher, aggregate_activity_rows,
    parse_activity_response, validate_live_market,
};
use pe_trader_index::score::lcb_5pct_decimal;
use pe_venue_polymarket::{BuySizing, LadderError, parse_compact_market, plan_sized_buy};
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::bucket_commit::DecisionContinuationV3;
use crate::config::ServiceConfig;
use crate::decision_replay::replay_decision_pending;
use crate::paper_recovery::{
    FinancialPayload, FinancialResult, MembershipReason, PAPER_LOG_SCHEMA_VERSION, PaperLogFrame,
    PaperLogRecord, PortfolioMark, QualificationSealed, QualificationStarted, RiskHaltOwner,
    ScannedPaperFrame, SealReason, TailBinding, active_risk_halts, paper_era, scan_paper_log,
};
use crate::risk_inputs::{build_paper_risk_base, build_paper_risk_snapshot, historical_mark_price};
use crate::runtime_config::{
    ConfigEra, ConfigRow, RISK_HALT_RELEASE_HASH_KEY, RuntimeConfig, parse_config,
};
use crate::supabase_state::resolution_source_received_at;

const QUALIFICATION_REPORT_VERSION: u16 = 1;
const FINANCIAL_ERA_KIND: &str = "financial-era-v1";
const QUALIFICATION_SOURCE_ID: &str = "pe-service.qualification";
const MINIMUM_COMPLETE_DAYS: usize = 30;
const MINIMUM_CLOSED_COPIES: usize = 90;
const MAX_P95_DELAY_MS: u64 = 2_000;

#[derive(Debug, thiserror::Error)]
pub enum QualificationError {
    #[error("paper log: {0}")]
    PaperLog(#[from] crate::paper_recovery::PaperLogScanError),
    #[error("event log: {0}")]
    EventLog(#[from] pe_event_log::LogError),
    #[error("paper state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("insufficient qualification evidence: {0}")]
    InsufficientEvidence(String),
}

#[derive(Debug, Clone)]
pub struct QualifyOptions {
    pub paper_log: PathBuf,
    pub source_log: PathBuf,
    pub paper_state: PathBuf,
    pub seal_hash: String,
    pub output: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationVerdict {
    Pass,
    Fail,
    InsufficientEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationThresholds {
    pub minimum_complete_days: usize,
    pub minimum_closed_copies: usize,
    pub maximum_p95_delay_ms: u64,
    pub maximum_drawdown_fraction_exclusive: Decimal,
    pub lcb_5pct_must_be_positive: bool,
}

impl QualificationThresholds {
    fn canonical() -> Self {
        Self {
            minimum_complete_days: MINIMUM_COMPLETE_DAYS,
            minimum_closed_copies: MINIMUM_CLOSED_COPIES,
            maximum_p95_delay_ms: MAX_P95_DELAY_MS,
            maximum_drawdown_fraction_exclusive: Decimal::from(KILL_SWITCH_DRAWDOWN_BPS).abs()
                / Decimal::from(10_000u64),
            lcb_5pct_must_be_positive: true,
        }
    }
}

/// Current evidence counts used by both automatic sealing and the offline verifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QualificationCompletion {
    pub complete_days: usize,
    pub causal_closes: usize,
}

impl QualificationCompletion {
    #[must_use]
    pub fn complete_days_met(self) -> bool {
        self.complete_days >= MINIMUM_COMPLETE_DAYS
    }

    #[must_use]
    pub fn causal_closes_met(self) -> bool {
        self.causal_closes >= MINIMUM_CLOSED_COPIES
    }

    #[must_use]
    pub fn is_complete(self) -> bool {
        self.complete_days_met() && self.causal_closes_met()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationMarkReport {
    pub cutoff_unix: i64,
    pub cash: Decimal,
    pub equity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationReplayReport {
    pub exact: bool,
    pub financial_prepared: usize,
    pub financial_final: usize,
    pub decisions: usize,
    pub fills: usize,
    pub no_fills: usize,
    pub no_copies: usize,
    pub membership_changes: usize,
    pub final_membership_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationEvidenceReport {
    pub start_sequence: Option<u64>,
    pub start_hash: Option<String>,
    pub seal_sequence: Option<u64>,
    pub seal_hash: String,
    pub source_prefix_hash: Option<String>,
    pub financial_prefix_hash: Option<String>,
    pub decision_evidence_digest: Option<String>,
    pub artifact_blake3: Option<String>,
    pub static_config_hash: Option<String>,
    pub hot_config_hash: Option<String>,
    pub policy_hash: Option<String>,
    pub financial_semantic_version: Option<u32>,
    pub economic_core_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationReport {
    pub version: u16,
    pub verdict: QualificationVerdict,
    pub reasons: Vec<String>,
    pub sealed_cutoff_unix: Option<i64>,
    pub first_valid_mark_unix: Option<i64>,
    pub promotion_anchor_mark_unix: Option<i64>,
    pub complete_days: usize,
    pub closed_copies: usize,
    pub paper_p95_delay_ms: Option<u64>,
    pub delay_samples_ms: Vec<u64>,
    pub complete_day_log_equity_growth: Vec<Decimal>,
    pub lcb_5pct_decimal: Option<Decimal>,
    pub promotion_max_drawdown_fraction: Option<Decimal>,
    pub start_to_seal_max_drawdown_fraction: Option<Decimal>,
    pub absolute_profit_loss: Option<Decimal>,
    pub demotions: usize,
    pub marks: Vec<QualificationMarkReport>,
    pub thresholds: QualificationThresholds,
    pub evidence: QualificationEvidenceReport,
    pub replay: QualificationReplayReport,
}

impl QualificationReport {
    fn insufficient(seal_hash: &str, reason: String) -> Self {
        Self {
            version: QUALIFICATION_REPORT_VERSION,
            verdict: QualificationVerdict::InsufficientEvidence,
            reasons: vec![reason],
            sealed_cutoff_unix: None,
            first_valid_mark_unix: None,
            promotion_anchor_mark_unix: None,
            complete_days: 0,
            closed_copies: 0,
            paper_p95_delay_ms: None,
            delay_samples_ms: Vec::new(),
            complete_day_log_equity_growth: Vec::new(),
            lcb_5pct_decimal: None,
            promotion_max_drawdown_fraction: None,
            start_to_seal_max_drawdown_fraction: None,
            absolute_profit_loss: None,
            demotions: 0,
            marks: Vec::new(),
            thresholds: QualificationThresholds::canonical(),
            evidence: QualificationEvidenceReport {
                start_sequence: None,
                start_hash: None,
                seal_sequence: None,
                seal_hash: seal_hash.to_owned(),
                source_prefix_hash: None,
                financial_prefix_hash: None,
                decision_evidence_digest: None,
                artifact_blake3: None,
                static_config_hash: None,
                hot_config_hash: None,
                policy_hash: None,
                financial_semantic_version: None,
                economic_core_hashes: Vec::new(),
            },
            replay: QualificationReplayReport {
                exact: false,
                financial_prepared: 0,
                financial_final: 0,
                decisions: 0,
                fills: 0,
                no_fills: 0,
                no_copies: 0,
                membership_changes: 0,
                final_membership_count: 0,
            },
        }
    }
}

/// Run the sealed verifier and atomically replace the requested report file.
///
/// Evidence failures are data: they produce an `InsufficientEvidence` report. Only inability to
/// encode or write that report is returned as an operational error.
pub async fn run_qualify(
    options: &QualifyOptions,
) -> Result<(QualificationVerdict, String), QualificationError> {
    let report = match verify_qualification(options).await {
        Ok(report) => report,
        Err(error) => QualificationReport::insufficient(&options.seal_hash, error.to_string()),
    };
    let mut bytes = serde_json::to_vec(&report)?;
    bytes.push(b'\n');
    let hash = blake3::hash(&bytes).to_hex().to_string();
    write_report(&options.output, &bytes)?;
    Ok((report.verdict, hash))
}

fn write_report(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .filter(|candidate| !candidate.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    use std::io::Write as _;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    if let Some(parent) = parent {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct SourceObservation {
    receipt: AppendReceipt,
    observed_at: SourceTimestamp,
    received_at: ReceivedAt,
    received_unix_ms: i64,
    source_id: String,
    schema_version: u32,
    parser_version: u32,
    content_type: ContentType,
    payload: Vec<u8>,
}

#[derive(Debug, Clone)]
struct CompletedFill {
    final_receipt: AppendReceipt,
    operation: crate::paper_recovery::PaperFillOperationIdentity,
    market_id: String,
    outcome_id: u16,
    side: Side,
    condition_id: String,
    delay_ms: u64,
    observation: pe_execution_core::ObservationEvidence,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    condition_id: String,
    market_id: String,
    outcome_index: u8,
    shares_atomic: u64,
}

#[derive(Debug, Clone)]
struct CompletedFinancialFact {
    prepared_receipt: AppendReceipt,
    final_receipt: AppendReceipt,
    payload: FinancialPayload,
    result: FinancialResult,
}

struct CausalFinancialState {
    cash: Decimal,
    positions: Vec<OpenPosition>,
    last_completed: Option<EventSeq>,
    completed_prepared: HashSet<EventSeq>,
    closed_fill_final_conditions: HashMap<u64, String>,
    open_fill_finals: Vec<(u64, String)>,
}

struct RiskReplayContext<'a> {
    cash: Decimal,
    positions: &'a [OpenPosition],
    fills: &'a [FillRow],
    settlements: &'a [SettledMarketRow],
    last_completed: Option<EventSeq>,
    start_receipt: AppendReceipt,
    paper_prefix: &'a [ScannedPaperFrame],
    source: &'a BTreeMap<u64, SourceObservation>,
    source_log_path: &'a Path,
    prepared_received_unix_ms: i64,
}

async fn verify_qualification(
    options: &QualifyOptions,
) -> Result<QualificationReport, QualificationError> {
    let requested_hash = blake3::Hash::from_hex(&options.seal_hash).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("invalid seal hash: {error}"))
    })?;
    let frames = scan_paper_log(&options.paper_log)?;
    let (seal_index, seal_receipt, seal) = find_seal(&frames, requested_hash)?;
    let (start_index, start_receipt, start) = find_start(&frames, seal_index, &seal)?;
    let financial_prefix_index = verify_seal_boundary(&frames, seal_index, &seal)?;
    verify_recorded_prefix(&options.source_log, &seal.source_prefix)?;
    verify_recorded_prefix(&options.paper_log, &seal.financial_prefix)?;
    verify_start_prefix(&frames[start_index], &start)?;
    if let SealReason::InsufficientEvidence(reason) = &seal.reason {
        return Ok(insufficient_seal_report(
            &start,
            start_receipt,
            &seal,
            seal_receipt,
            reason,
        ));
    }

    let source_observations = source_observations(&options.source_log, &seal.source_prefix)?;
    let state = PaperStateDb::open_read_only(&options.paper_state)?;
    let decisions = decision_rows_from_source_observations(
        &state,
        &start.source_prefix,
        &seal.source_prefix,
        &source_observations,
    )?;
    if decisions
        .iter()
        .any(|row| row.state != DecisionPendingState::Terminal)
    {
        return insufficient("open decision_pending row exists inside the sealed prefix");
    }
    let replayed_decisions = decisions
        .iter()
        .map(replay_decision_pending)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!("decision replay mismatch: {error}"))
        })?;
    let mut decision_observations = HashMap::new();
    for decision in &replayed_decisions {
        let observation =
            verify_decision_source_inputs(decision, &source_observations, &options.source_log)?;
        if decision_observations
            .insert(
                decision.continuation.prior.source_trade_id.clone(),
                observation,
            )
            .is_some()
        {
            return insufficient("sealed decision evidence repeats a source trade identity");
        }
    }
    let decision_keys = decisions
        .iter()
        .map(|row| (row.source_trade_id.clone(), row.semantic_revision.clone()))
        .collect::<Vec<_>>();
    let decision_bytes = state.seal_decision_evidence(&decision_keys)?;
    let decision_digest = blake3::hash(&decision_bytes).to_hex().to_string();
    if decision_digest != seal.decision_evidence_digest {
        return insufficient(format!(
            "decision evidence digest mismatch: sealed {}, replayed {decision_digest}",
            seal.decision_evidence_digest
        ));
    }

    let mut cash = start.starting_bankroll.to_decimal();
    // `scan_paper_log` is the sole Prepared/Final state-machine validator. This map retains only
    // the already-validated payloads needed for the accounting and identity re-execution below.
    let mut prepared = BTreeMap::<(u64, String), &FinancialPayload>::new();
    let mut completed_fills = Vec::new();
    let mut open_positions: Vec<OpenPosition> = Vec::new();
    let mut financial_fills = Vec::<FillRow>::new();
    let mut financial_settlements = Vec::<SettledMarketRow>::new();
    let mut completed_financial_facts = Vec::<CompletedFinancialFact>::new();
    let mut economic_hashes = Vec::new();
    let mut financial_final_count = 0usize;
    let mut replayed_financial_prefix = None;
    let mut membership = start.membership.iter().copied().collect::<HashSet<_>>();
    if membership.len() != start.membership.len() {
        return insufficient("QualificationStarted membership contains duplicates");
    }
    let mut membership_changes = 0usize;
    let mut demotions = 0usize;
    let mut anchor_mark_sequence = None;
    let mut anchor_mark_cutoff = None;
    let mut waiting_for_anchor_mark = true;
    let mut valid_marks = Vec::<(u64, QualificationMarkReport)>::new();
    let mut latest_causal_financial = None;

    for (offset, frame) in frames[start_index + 1..=financial_prefix_index]
        .iter()
        .enumerate()
    {
        let frame_index = start_index + 1 + offset;
        let PaperLogFrame::Record(record) = &frame.frame else {
            return insufficient("legacy paper fill exists after QualificationStarted");
        };
        match record {
            PaperLogRecord::FinancialPrepared { payload, .. } => {
                match payload {
                    FinancialPayload::Fill {
                        operation,
                        economic,
                    } => {
                        verify_economic(
                            operation,
                            economic,
                            &RiskReplayContext {
                                cash,
                                positions: &open_positions,
                                fills: &financial_fills,
                                settlements: &financial_settlements,
                                last_completed: replayed_financial_prefix,
                                start_receipt,
                                paper_prefix: &frames[start_index..frame_index],
                                source: &source_observations,
                                source_log_path: &options.source_log,
                                prepared_received_unix_ms: received_unix_ms(&frame.envelope)?,
                            },
                        )
                        .await?;
                        economic_hashes.push(economic.core_hash().map_err(|error| {
                            QualificationError::InsufficientEvidence(format!(
                                "economic core hash failed: {error}"
                            ))
                        })?);
                    }
                    FinancialPayload::Resolution { .. } => {}
                }
                prepared.insert(receipt_key(frame.receipt), payload);
            }
            PaperLogRecord::FinancialFinal {
                prepared_receipt,
                result,
            } => {
                let key = receipt_key(*prepared_receipt);
                let prepared_payload = prepared.get(&key).copied().ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "FinancialFinal references an unknown Prepared receipt".to_owned(),
                    )
                })?;
                match (prepared_payload, result) {
                    (
                        FinancialPayload::Fill {
                            operation,
                            economic,
                        },
                        FinancialResult::Fill { canonical },
                    ) => {
                        if economic.risk.decision != RiskDecisionAudit::Approved {
                            return insufficient(
                                "executed Financial Fill was not risk-approved at preparation",
                            );
                        }
                        if canonical.applied_prepared_seq != prepared_receipt.sequence
                            || canonical.quantity != economic.sizing.expected_shares
                            || canonical.principal != economic.sizing.principal
                            || canonical.fee != economic.fee.expected_fee
                            || canonical.fill_price != economic.sizing.expected_vwap
                            || !matches!(canonical.outcome.as_str(), "applied" | "existing")
                        {
                            return insufficient(
                                "Financial Fill Final differs from Prepared economics",
                            );
                        }
                        cash = cash
                            .checked_sub(canonical.principal.to_decimal())
                            .and_then(|value| value.checked_sub(canonical.fee.to_decimal()))
                            .ok_or_else(|| {
                                QualificationError::InsufficientEvidence(
                                    "paper cash underflow while replaying Fill Final".to_owned(),
                                )
                            })?;
                        if canonical.bankroll != cash {
                            return insufficient("Fill Final bankroll differs from exact replay");
                        }
                        apply_fill_position(&mut open_positions, economic, canonical.quantity)?;
                        let idempotency_key =
                            pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                                &TraderId(operation.leader_wallet).to_string(),
                                &operation.source_trade_id.0,
                                &economic.market.market_id,
                                u16::from(economic.market.outcome_index),
                                economic.market.side,
                                operation.observed_at_bucket,
                            );
                        financial_fills.push(FillRow {
                            idempotency_key,
                            market_id: MarketId(VenueMarketId(economic.market.market_id.clone())),
                            outcome_id: OutcomeId(u16::from(economic.market.outcome_index)),
                            side: economic.market.side,
                            quantity: canonical.quantity,
                            fill_price: canonical.fill_price,
                            principal: canonical.principal,
                            fee: canonical.fee,
                            event_seq: prepared_receipt.sequence,
                            prepared_seq: prepared_receipt.sequence,
                            source_receipt_seq: economic
                                .observation
                                .as_ref()
                                .map(|value| value.source_receipt.sequence),
                        });
                        let observation = economic.observation.as_ref().ok_or_else(|| {
                            QualificationError::InsufficientEvidence(
                                "Fill Prepared lacks source observation receipt".to_owned(),
                            )
                        })?;
                        let source = source_observations
                            .get(&observation.source_receipt.sequence.0)
                            .filter(|source| source.receipt == observation.source_receipt)
                            .ok_or_else(|| {
                                QualificationError::InsufficientEvidence(
                                    "Fill observation receipt is absent from sealed source prefix"
                                        .to_owned(),
                                )
                            })?;
                        let final_ms = received_unix_ms(&frame.envelope)?;
                        let delay =
                            final_ms
                                .checked_sub(source.received_unix_ms)
                                .ok_or_else(|| {
                                    QualificationError::InsufficientEvidence(
                                        "Fill Final precedes its source observation".to_owned(),
                                    )
                                })?;
                        completed_fills.push(CompletedFill {
                            final_receipt: frame.receipt,
                            operation: operation.clone(),
                            market_id: economic.market.market_id.clone(),
                            outcome_id: u16::from(economic.market.outcome_index),
                            side: economic.market.side,
                            condition_id: economic.market.condition_id.0.clone(),
                            delay_ms: u64::try_from(delay).map_err(|_| {
                                QualificationError::InsufficientEvidence(
                                    "paper delay conversion overflow".to_owned(),
                                )
                            })?,
                            observation: observation.clone(),
                        });
                    }
                    (
                        FinancialPayload::Resolution {
                            condition_id,
                            payout_by_outcome_index_json,
                            resolution_source_receipt,
                        },
                        FinancialResult::Resolution { canonical },
                    ) => {
                        if canonical.applied_prepared_seq != prepared_receipt.sequence
                            || !matches!(canonical.outcome.as_str(), "applied" | "existing")
                        {
                            return insufficient("Resolution Final authority mismatch");
                        }
                        if source_observations
                            .get(&resolution_source_receipt.sequence.0)
                            .is_none_or(|source| source.receipt != *resolution_source_receipt)
                        {
                            return insufficient(
                                "resolution receipt is absent from the sealed source prefix",
                            );
                        }
                        let settled_at_unix = resolution_source_received_at(
                            &options.source_log,
                            *resolution_source_receipt,
                            condition_id,
                            payout_by_outcome_index_json,
                        )
                        .map_err(|error| {
                            QualificationError::InsufficientEvidence(format!(
                                "resolution source evidence mismatch: {error}"
                            ))
                        })?;
                        if canonical.settled_at_unix != settled_at_unix {
                            return insufficient(
                                "Resolution Final settlement time differs from its source envelope",
                            );
                        }
                        let payouts =
                            BinaryPayoutVector::from_canonical_json(payout_by_outcome_index_json)
                                .map_err(|error| {
                                QualificationError::InsufficientEvidence(format!(
                                    "resolution payout decode: {error}"
                                ))
                            })?;
                        let decimals = payouts.decimals();
                        let payout =
                            BinaryPayout::new(decimals[0], decimals[1]).map_err(|error| {
                                QualificationError::InsufficientEvidence(format!(
                                    "resolution payout vector: {error}"
                                ))
                            })?;
                        let mut by_outcome = BTreeMap::<u16, ShareAmount>::new();
                        for position in open_positions
                            .iter()
                            .filter(|position| position.condition_id == condition_id.0)
                        {
                            let entry = by_outcome
                                .entry(u16::from(position.outcome_index))
                                .or_insert(ShareAmount::ZERO);
                            *entry = entry
                                .checked_add(ShareAmount::from_atomic(position.shares_atomic))
                                .map_err(|_| {
                                    QualificationError::InsufficientEvidence(
                                        "resolution quantity overflow".to_owned(),
                                    )
                                })?;
                        }
                        let expected_credit = aggregate_resolution_credit(
                            &by_outcome.into_iter().collect::<Vec<_>>(),
                            &payout,
                        )
                        .map_err(|error| {
                            QualificationError::InsufficientEvidence(format!(
                                "resolution credit arithmetic: {error}"
                            ))
                        })?;
                        if canonical.credit != expected_credit {
                            return insufficient(
                                "Resolution Final credit differs from exact replay",
                            );
                        }
                        cash = cash
                            .checked_add(expected_credit.to_decimal())
                            .ok_or_else(|| {
                                QualificationError::InsufficientEvidence(
                                    "paper cash overflow while replaying Resolution Final"
                                        .to_owned(),
                                )
                            })?;
                        if canonical.bankroll != cash {
                            return insufficient(
                                "Resolution Final bankroll differs from exact replay",
                            );
                        }
                        financial_settlements.push(SettledMarketRow {
                            market_id: MarketId(VenueMarketId(condition_id.0.clone())),
                            outcome_prices_json: payout_by_outcome_index_json.clone(),
                            credit_applied: expected_credit.to_decimal(),
                            settled_at_unix,
                            prepared_seq: Some(prepared_receipt.sequence),
                            source_receipt_seq: Some(resolution_source_receipt.sequence),
                        });
                        open_positions.retain(|position| position.condition_id != condition_id.0);
                    }
                    _ => return insufficient("Financial Prepared/Final kinds disagree"),
                }
                completed_financial_facts.push(CompletedFinancialFact {
                    prepared_receipt: *prepared_receipt,
                    final_receipt: frame.receipt,
                    payload: prepared_payload.clone(),
                    result: result.clone(),
                });
                financial_final_count = financial_final_count.saturating_add(1);
                replayed_financial_prefix = Some(prepared_receipt.sequence);
            }
            PaperLogRecord::MembershipChanged {
                reason,
                removed,
                added,
                capacity,
                ranking_batch_id,
                evidence,
            } => {
                if *reason == MembershipReason::Initial
                    || !evidence.is_object()
                    || evidence.as_object().is_none_or(serde_json::Map::is_empty)
                    || matches!(
                        *reason,
                        MembershipReason::FullRerank | MembershipReason::RankerRotation
                    ) && ranking_batch_id.is_none()
                    || removed.iter().collect::<HashSet<_>>().len() != removed.len()
                    || added.iter().collect::<HashSet<_>>().len() != added.len()
                    || removed.iter().any(|wallet| added.contains(wallet))
                    || removed.iter().any(|wallet| !membership.contains(wallet))
                    || added.iter().any(|wallet| membership.contains(wallet))
                {
                    return insufficient("MembershipChanged structural evidence is invalid");
                }
                for wallet in removed {
                    membership.remove(wallet);
                }
                for wallet in added {
                    if !membership.insert(*wallet) {
                        return insufficient("MembershipChanged adds an existing wallet");
                    }
                }
                if membership.len() > *capacity {
                    return insufficient("MembershipChanged exceeds its recorded capacity");
                }
                membership_changes = membership_changes.saturating_add(1);
                if reason_moves_anchor(*reason) {
                    demotions = demotions.saturating_add(1);
                    waiting_for_anchor_mark = true;
                    anchor_mark_sequence = None;
                    anchor_mark_cutoff = None;
                }
            }
            PaperLogRecord::PortfolioMark(mark) => {
                if mark.cutoff_unix > seal.sealed_cutoff_unix {
                    return insufficient("PortfolioMark occurs after the sealed cutoff");
                }
                if prepared.len() != financial_final_count {
                    return insufficient("PortfolioMark occurs while a Prepared is unmatched");
                }
                let causal = causal_financial_state(
                    start.starting_bankroll.to_decimal(),
                    &completed_financial_facts,
                    mark,
                    &source_observations,
                )?;
                let report = verify_mark(mark, &causal, &source_observations).await?;
                valid_marks.push((frame.receipt.sequence.0, report.clone()));
                if waiting_for_anchor_mark {
                    anchor_mark_sequence = Some(frame.receipt.sequence.0);
                    anchor_mark_cutoff = Some(report.cutoff_unix);
                    waiting_for_anchor_mark = false;
                }
                latest_causal_financial = Some(causal);
            }
            PaperLogRecord::QualificationStarted(_) => {
                return insufficient("a second QualificationStarted occurs before the seal");
            }
            PaperLogRecord::QualificationSealed(_) => {
                return insufficient("QualificationSealed occurs inside its financial prefix");
            }
            PaperLogRecord::RiskHaltChanged { .. } => {}
        }
    }
    if prepared.len() != financial_final_count {
        return insufficient("sealed financial prefix contains unmatched Prepared records");
    }

    bind_final_receipts(
        &replayed_decisions,
        &completed_fills,
        &decision_observations,
    )?;
    let anchor_sequence = anchor_mark_sequence.ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "no valid PortfolioMark exists after the latest demotion anchor".to_owned(),
        )
    })?;
    let anchor_cutoff = anchor_mark_cutoff.ok_or_else(|| {
        QualificationError::InsufficientEvidence("promotion anchor cutoff is absent".to_owned())
    })?;
    let promotion_marks = valid_marks
        .iter()
        .filter(|(sequence, _)| *sequence >= anchor_sequence)
        .map(|(_, mark)| mark.clone())
        .collect::<Vec<_>>();
    let growth = complete_day_growth(&promotion_marks)?;
    let promotion_equity = promotion_marks
        .iter()
        .map(|mark| mark.equity)
        .collect::<Vec<_>>();
    let all_equity = std::iter::once(start.starting_bankroll.to_decimal())
        .chain(valid_marks.iter().map(|(_, mark)| mark.equity))
        .collect::<Vec<_>>();
    let promotion_drawdown =
        pe_risk_engine::max_drawdown_fraction(&promotion_equity).map_err(|error| {
            QualificationError::InsufficientEvidence(format!("promotion drawdown: {error}"))
        })?;
    let era_drawdown = pe_risk_engine::max_drawdown_fraction(&all_equity).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("era drawdown: {error}"))
    })?;
    let lcb = lcb_5pct_decimal(&growth);

    let causal_financial = latest_causal_financial.ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "no causal financial state exists at the sealed mark".to_owned(),
        )
    })?;
    let mut closed = completed_fills
        .iter()
        .filter(|fill| fill.final_receipt.sequence.0 > anchor_sequence)
        .filter(|fill| {
            causal_financial
                .closed_fill_final_conditions
                .get(&fill.final_receipt.sequence.0)
                == Some(&fill.condition_id)
        })
        .collect::<Vec<_>>();
    closed.sort_by_key(|fill| fill.final_receipt.sequence.0);
    let completion = qualification_completion_for_causal_facts(
        &paper_era(frames[..=financial_prefix_index].to_vec()),
        &causal_financial.completed_prepared,
    )
    .ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "qualification completion inputs are invalid".to_owned(),
        )
    })?;
    if completion.complete_days != growth.len() || completion.causal_closes != closed.len() {
        return insufficient("qualification completion differs from verified replay");
    }
    let delays = closed.iter().map(|fill| fill.delay_ms).collect::<Vec<_>>();
    let p95 = nearest_rank_p95(&delays);
    let absolute_pnl = valid_marks.last().and_then(|(_, mark)| {
        mark.equity
            .checked_sub(start.starting_bankroll.to_decimal())
    });

    let thresholds = QualificationThresholds::canonical();
    let mut failures = Vec::new();
    if !completion.complete_days_met() {
        failures.push(format!(
            "complete days {} < {}",
            completion.complete_days, thresholds.minimum_complete_days
        ));
    }
    if !completion.causal_closes_met() {
        failures.push(format!(
            "closed copies {} < {}",
            completion.causal_closes, thresholds.minimum_closed_copies
        ));
    }
    if lcb.is_none_or(|value| value <= Decimal::ZERO) {
        failures.push("LCB_5pct is not positive".to_owned());
    }
    if promotion_drawdown >= thresholds.maximum_drawdown_fraction_exclusive {
        failures.push("promotion drawdown meets or exceeds the absolute-loss magnitude".to_owned());
    }
    if p95.is_none_or(|value| value > thresholds.maximum_p95_delay_ms) {
        failures.push("paper p95 copy delay is absent or above budget".to_owned());
    }
    if !open_positions.is_empty() {
        failures.push("paper positions remain open at the sealed financial prefix".to_owned());
    }

    let (verdict, mut reasons) = qualification_gate_verdict(failures);
    if verdict == QualificationVerdict::Pass {
        reasons
            .push("all sealed one-system gates passed; manual review remains required".to_owned());
    }

    let no_fills = replayed_decisions
        .iter()
        .filter(|decision| decision.post_boundary.body.terminal.disposition == "no_fill")
        .count();
    let no_copies = replayed_decisions
        .iter()
        .filter(|decision| {
            decision
                .post_boundary
                .body
                .terminal
                .disposition
                .starts_with("no_copy:")
        })
        .count();
    Ok(QualificationReport {
        version: QUALIFICATION_REPORT_VERSION,
        verdict,
        reasons,
        sealed_cutoff_unix: Some(seal.sealed_cutoff_unix),
        first_valid_mark_unix: valid_marks.first().map(|(_, mark)| mark.cutoff_unix),
        promotion_anchor_mark_unix: Some(anchor_cutoff),
        complete_days: completion.complete_days,
        closed_copies: completion.causal_closes,
        paper_p95_delay_ms: p95,
        delay_samples_ms: delays,
        complete_day_log_equity_growth: growth,
        lcb_5pct_decimal: lcb,
        promotion_max_drawdown_fraction: Some(promotion_drawdown),
        start_to_seal_max_drawdown_fraction: Some(era_drawdown),
        absolute_profit_loss: absolute_pnl,
        demotions,
        marks: valid_marks.into_iter().map(|(_, mark)| mark).collect(),
        thresholds,
        evidence: QualificationEvidenceReport {
            start_sequence: Some(start_receipt.sequence.0),
            start_hash: Some(start_receipt.this_hash.to_hex().to_string()),
            seal_sequence: Some(seal_receipt.sequence.0),
            seal_hash: seal_receipt.this_hash.to_hex().to_string(),
            source_prefix_hash: Some(seal.source_prefix.last_hash.clone()),
            financial_prefix_hash: Some(seal.financial_prefix.last_hash.clone()),
            decision_evidence_digest: Some(decision_digest),
            artifact_blake3: Some(start.artifact_blake3.clone()),
            static_config_hash: Some(start.static_config_hash.clone()),
            hot_config_hash: Some(start.hot_config_hash.clone()),
            policy_hash: Some(start.policy_hash.clone()),
            financial_semantic_version: Some(start.financial_semantic_version),
            economic_core_hashes: economic_hashes,
        },
        replay: QualificationReplayReport {
            exact: true,
            financial_prepared: prepared.len(),
            financial_final: financial_final_count,
            decisions: replayed_decisions.len(),
            fills: completed_fills.len(),
            no_fills,
            no_copies,
            membership_changes,
            final_membership_count: membership.len(),
        },
    })
}

fn qualification_gate_verdict(failures: Vec<String>) -> (QualificationVerdict, Vec<String>) {
    if failures.is_empty() {
        (QualificationVerdict::Pass, failures)
    } else {
        (QualificationVerdict::Fail, failures)
    }
}

fn insufficient_seal_report(
    start: &QualificationStarted,
    start_receipt: AppendReceipt,
    seal: &QualificationSealed,
    seal_receipt: AppendReceipt,
    reason: &str,
) -> QualificationReport {
    QualificationReport {
        version: QUALIFICATION_REPORT_VERSION,
        verdict: QualificationVerdict::InsufficientEvidence,
        reasons: vec![format!("seal result: {reason}")],
        sealed_cutoff_unix: Some(seal.sealed_cutoff_unix),
        first_valid_mark_unix: None,
        promotion_anchor_mark_unix: None,
        complete_days: 0,
        closed_copies: 0,
        paper_p95_delay_ms: None,
        delay_samples_ms: Vec::new(),
        complete_day_log_equity_growth: Vec::new(),
        lcb_5pct_decimal: None,
        promotion_max_drawdown_fraction: None,
        start_to_seal_max_drawdown_fraction: None,
        absolute_profit_loss: None,
        demotions: 0,
        marks: Vec::new(),
        thresholds: QualificationThresholds::canonical(),
        evidence: QualificationEvidenceReport {
            start_sequence: Some(start_receipt.sequence.0),
            start_hash: Some(start_receipt.this_hash.to_hex().to_string()),
            seal_sequence: Some(seal_receipt.sequence.0),
            seal_hash: seal_receipt.this_hash.to_hex().to_string(),
            source_prefix_hash: Some(seal.source_prefix.last_hash.clone()),
            financial_prefix_hash: Some(seal.financial_prefix.last_hash.clone()),
            decision_evidence_digest: Some(seal.decision_evidence_digest.clone()),
            artifact_blake3: Some(start.artifact_blake3.clone()),
            static_config_hash: Some(start.static_config_hash.clone()),
            hot_config_hash: Some(start.hot_config_hash.clone()),
            policy_hash: Some(start.policy_hash.clone()),
            financial_semantic_version: Some(start.financial_semantic_version),
            economic_core_hashes: Vec::new(),
        },
        replay: QualificationReplayReport {
            exact: false,
            financial_prepared: 0,
            financial_final: 0,
            decisions: 0,
            fills: 0,
            no_fills: 0,
            no_copies: 0,
            membership_changes: 0,
            final_membership_count: start.membership.len(),
        },
    }
}

fn insufficient<T>(reason: impl Into<String>) -> Result<T, QualificationError> {
    Err(QualificationError::InsufficientEvidence(reason.into()))
}

fn receipt_key(receipt: AppendReceipt) -> (u64, String) {
    (receipt.sequence.0, receipt.this_hash.to_hex().to_string())
}

fn find_seal(
    frames: &[ScannedPaperFrame],
    requested_hash: blake3::Hash,
) -> Result<(usize, AppendReceipt, QualificationSealed), QualificationError> {
    frames
        .iter()
        .enumerate()
        .find_map(|(index, frame)| {
            if frame.receipt.this_hash != requested_hash {
                return None;
            }
            match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(seal)) => {
                    Some((index, frame.receipt, seal.as_ref().clone()))
                }
                _ => None,
            }
        })
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "requested seal hash does not identify QualificationSealed".to_owned(),
            )
        })
}

fn find_start(
    frames: &[ScannedPaperFrame],
    seal_index: usize,
    seal: &QualificationSealed,
) -> Result<(usize, AppendReceipt, QualificationStarted), QualificationError> {
    frames[..seal_index]
        .iter()
        .enumerate()
        .find_map(|(index, frame)| {
            if frame.receipt != seal.start_receipt {
                return None;
            }
            match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::QualificationStarted(start)) => {
                    Some((index, frame.receipt, start.as_ref().clone()))
                }
                _ => None,
            }
        })
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "seal start receipt does not identify QualificationStarted".to_owned(),
            )
        })
}

fn verify_seal_boundary(
    frames: &[ScannedPaperFrame],
    seal_index: usize,
    seal: &QualificationSealed,
) -> Result<usize, QualificationError> {
    let bound_sequence = seal.financial_prefix.last_sequence.ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "QualificationSealed has an empty financial prefix".to_owned(),
        )
    })?;
    let bound_hash = tail_hash(&seal.financial_prefix)?;
    let bound_index = frames
        .iter()
        .position(|frame| frame.receipt.sequence == bound_sequence)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "QualificationSealed financial prefix is absent from the paper log".to_owned(),
            )
        })?;
    let seal_frame = frames.get(seal_index).ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "QualificationSealed frame index is invalid".to_owned(),
        )
    })?;
    if bound_index.checked_add(1) != Some(seal_index)
        || frames[bound_index].receipt.this_hash != bound_hash
        || seal_frame.envelope.prev_hash != bound_hash
        || bound_sequence.0.checked_add(1) != Some(seal_frame.receipt.sequence.0)
    {
        return insufficient(
            "QualificationSealed does not immediately follow its exact financial prefix",
        );
    }
    let seal_received_unix = seal_frame.envelope.received_at.0.unix_timestamp();
    let start_received_unix = frames
        .iter()
        .find(|frame| frame.receipt == seal.start_receipt)
        .map(|frame| frame.envelope.received_at.0.unix_timestamp())
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "QualificationSealed Start receipt is absent".to_owned(),
            )
        })?;
    if seal.sealed_cutoff_unix < start_received_unix || seal.sealed_cutoff_unix > seal_received_unix
    {
        return insufficient("QualificationSealed cutoff is outside its causal envelope interval");
    }
    Ok(bound_index)
}

fn tail_hash(binding: &TailBinding) -> Result<blake3::Hash, QualificationError> {
    blake3::Hash::from_hex(&binding.last_hash).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("invalid recorded tail hash: {error}"))
    })
}

fn verify_recorded_prefix(path: &Path, recorded: &TailBinding) -> Result<(), QualificationError> {
    let current = Scanner::verify(path)?;
    let expected = LogTailBinding {
        path: current.path,
        physical_tail: recorded.physical_tail,
        last_sequence: recorded.last_sequence,
        last_hash: tail_hash(recorded)?,
    };
    Scanner::verify_prefix(&expected)?;
    Ok(())
}

fn verify_start_prefix(
    start_frame: &ScannedPaperFrame,
    start: &QualificationStarted,
) -> Result<(), QualificationError> {
    let expected_sequence = start
        .paper_prefix
        .last_sequence
        .map_or(Some(0), |sequence| sequence.0.checked_add(1))
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence("Start sequence overflow".to_owned())
        })?;
    if start_frame.receipt.sequence.0 != expected_sequence
        || start_frame.envelope.prev_hash != tail_hash(&start.paper_prefix)?
    {
        return insufficient("QualificationStarted does not extend its paper prefix");
    }
    Ok(())
}

fn source_observations(
    path: &Path,
    prefix: &TailBinding,
) -> Result<BTreeMap<u64, SourceObservation>, QualificationError> {
    let Some(last_sequence) = prefix.last_sequence else {
        return Ok(BTreeMap::new());
    };
    let mut observations = BTreeMap::new();
    for item in Reader::replay(path)? {
        let (sequence, envelope) = item?;
        if sequence > last_sequence {
            break;
        }
        let received_unix_ms = received_unix_ms(&envelope)?;
        observations.insert(
            sequence.0,
            SourceObservation {
                receipt: AppendReceipt {
                    sequence,
                    this_hash: envelope.this_hash,
                },
                observed_at: envelope.observed_at,
                received_at: envelope.received_at,
                received_unix_ms,
                source_id: envelope.source_id.0,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                content_type: envelope.content_type,
                payload: envelope.payload,
            },
        );
    }
    let expected_hash = tail_hash(prefix)?;
    if observations
        .get(&last_sequence.0)
        .is_none_or(|observation| observation.receipt.this_hash != expected_hash)
    {
        return insufficient("source observations do not reach the sealed sequence/hash prefix");
    }
    Ok(observations)
}

/// Select the ordered durable decision rows whose complete source evidence is in the immutable
/// post-Start portion of `sealed_prefix`. Timestamp fields in the SQLite projection are never a
/// membership input.
pub(crate) fn decision_rows_for_source_prefix(
    state: &PaperStateDb,
    source_log_path: &Path,
    start_prefix: &TailBinding,
    sealed_prefix: &TailBinding,
) -> Result<Vec<DecisionPendingRow>, QualificationError> {
    let observations = source_observations(source_log_path, sealed_prefix)?;
    decision_rows_from_source_observations(state, start_prefix, sealed_prefix, &observations)
}

fn decision_rows_from_source_observations(
    state: &PaperStateDb,
    start_prefix: &TailBinding,
    sealed_prefix: &TailBinding,
    observations: &BTreeMap<u64, SourceObservation>,
) -> Result<Vec<DecisionPendingRow>, QualificationError> {
    let start_sequence = start_prefix.last_sequence;
    let Some(sealed_sequence) = sealed_prefix.last_sequence else {
        return Ok(Vec::new());
    };
    let mut selected = Vec::new();
    for row in state.decision_pending_history()? {
        let continuation = DecisionContinuationV3::from_durable(&row).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision source receipt link is invalid: {error}"
            ))
        })?;
        let receipts = continuation
            .page_occurrences()
            .iter()
            .map(|page| page.receipt)
            .chain(continuation.observed_source_receipt)
            .collect::<Vec<_>>();
        if receipts.is_empty()
            || receipts
                .iter()
                .any(|receipt| start_sequence.is_some_and(|start| receipt.sequence <= start))
            || receipts
                .iter()
                .any(|receipt| receipt.sequence > sealed_sequence)
        {
            continue;
        }
        if receipts.iter().any(|receipt| {
            observations
                .get(&receipt.sequence.0)
                .is_none_or(|observation| observation.receipt != *receipt)
        }) {
            return insufficient("decision source receipt does not match the sealed source prefix");
        }
        selected.push(row);
    }
    selected.sort_by(|left, right| {
        (left.source_epoch, &left.source_trade_id.0)
            .cmp(&(right.source_epoch, &right.source_trade_id.0))
    });
    Ok(selected)
}

fn verify_decision_source_inputs(
    decision: &crate::decision_replay::ReplayedDecision,
    source: &BTreeMap<u64, SourceObservation>,
    source_log_path: &Path,
) -> Result<pe_execution_core::ObservationEvidence, QualificationError> {
    let continuation = &decision.continuation;
    let frozen = &continuation.prior;
    let observation = continuation
        .observation_from_source_log(source_log_path)
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision source receipt replay failed: {error}"
            ))
        })?
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "post-Start decision has no version-three source evidence".to_owned(),
            )
        })?;
    for receipt in [
        observation.source_receipt,
        observation.complete_bound_receipt,
    ] {
        if source
            .get(&receipt.sequence.0)
            .is_none_or(|source| source.receipt != receipt)
        {
            return insufficient("decision receipt is outside the sealed source prefix");
        }
    }

    let mut rows = Vec::new();
    for page in continuation.page_occurrences() {
        let source = source
            .get(&page.receipt.sequence.0)
            .filter(|source| source.receipt == page.receipt)
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "decision activity page is absent from the sealed source prefix".to_owned(),
                )
            })?;
        if source.source_id != crate::trade_poller::ACTIVITY_POLL_SOURCE_ID
            || source.schema_version != pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
            || source.parser_version != pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
            || source.content_type != ContentType::Json
            || source.receipt.this_hash != page.receipt.this_hash
            || source.receipt.sequence > observation.complete_bound_receipt.sequence
        {
            return insufficient("decision activity page has the wrong source contract");
        }
        let window = parse_activity_response(
            &source.payload,
            frozen.wallet,
            &ActivityParseContext {
                source_id: SourceId(source.source_id.clone()),
                observed_at: source.observed_at.clone(),
                received_at: source.received_at.clone(),
                transport: ActivityTransport::Replay,
            },
        )
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision activity page production parse failed: {error}"
            ))
        })?;
        rows.extend(window.rows);
    }
    let aggregates = aggregate_activity_rows(&rows).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "decision activity aggregate replay failed: {error}"
        ))
    })?;
    let mut matching = aggregates
        .iter()
        .filter(|aggregate| aggregate.group_id.key() == &frozen.source_trade_id);
    let aggregate = matching.next().ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "decision activity aggregate is absent from its raw pages".to_owned(),
        )
    })?;
    if matching.next().is_some() {
        return insufficient("decision raw pages reconstruct duplicate aggregate identities");
    }
    let components = aggregate.group_id.components();
    if components.activity_type != ActivityType::Trade
        || components.wallet != frozen.wallet
        || components.transaction_hash != frozen.transaction_hash
        || components
            .condition_id
            .as_ref()
            .is_none_or(|condition| condition.0 != frozen.market_id.0.0)
        || components.outcome != Some(frozen.outcome_id)
        || components.side != Some(frozen.side)
        || aggregate.share_sum != frozen.share_amount
        || aggregate.volume_weighted_price().map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision aggregate price replay failed: {error}"
            ))
        })? != frozen.price
        || aggregate.source_time.0.unix_timestamp() != frozen.source_epoch
        || aggregate.semantic_revision.as_str() != frozen.semantic_revision
    {
        return insufficient("decision continuation differs from its raw activity aggregate");
    }
    Ok(observation)
}

fn received_unix_ms(envelope: &pe_event_log::EventEnvelope) -> Result<i64, QualificationError> {
    let millis = envelope.received_at.0.unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).map_err(|_| {
        QualificationError::InsufficientEvidence("event timestamp milliseconds overflow".to_owned())
    })
}

#[derive(Clone)]
struct RecordedPageFetcher {
    payload: Vec<u8>,
}

impl PageFetcher for RecordedPageFetcher {
    async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, pe_source_core::SourceError> {
        Ok(self.payload.clone())
    }
}

fn replayed_financial_snapshot(
    context: &RiskReplayContext<'_>,
    evaluated_at_unix: i64,
) -> Result<FinancialSnapshot, QualificationError> {
    const SEVEN_DAYS_SECS: i64 = 7 * 24 * 60 * 60;
    let lower = evaluated_at_unix
        .checked_sub(SEVEN_DAYS_SECS)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "risk snapshot seven-day window underflow".to_owned(),
            )
        })?;
    let positions = context
        .positions
        .iter()
        .map(|position| PaperPositionRow {
            market_id: MarketId(VenueMarketId(position.market_id.clone())),
            outcome_id: OutcomeId(u16::from(position.outcome_index)),
            long: ShareAmount::from_atomic(position.shares_atomic),
            short: ShareAmount::ZERO,
        })
        .collect::<Vec<_>>();
    let settlements_7d = context
        .settlements
        .iter()
        .filter(|row| row.settled_at_unix > lower && row.settled_at_unix <= evaluated_at_unix)
        .cloned()
        .collect::<Vec<_>>();
    let open_markets = positions
        .iter()
        .map(|position| position.market_id.clone())
        .collect::<HashSet<_>>();
    let settled_markets = settlements_7d
        .iter()
        .map(|settlement| settlement.market_id.clone())
        .collect::<HashSet<_>>();
    let fills_for_open_and_7d = context
        .fills
        .iter()
        .filter(|fill| {
            open_markets.contains(&fill.market_id) || settled_markets.contains(&fill.market_id)
        })
        .cloned()
        .collect();
    Ok(FinancialSnapshot {
        cash: context.cash,
        positions,
        settlements_7d,
        fills_for_open_and_7d,
        last_prepared_seq: context.last_completed,
        start: Some((
            context.start_receipt.sequence,
            context.start_receipt.this_hash,
        )),
    })
}

async fn replayed_risk_prices(
    price_receipts: &[AppendReceipt],
    evaluated_at_unix_ms: i64,
    positions: &[PaperPositionRow],
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<HashMap<(MarketId, OutcomeId), Price>, QualificationError> {
    if price_receipts
        .windows(2)
        .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return insufficient("risk price receipts are not sorted and deduplicated");
    }
    if positions.is_empty() {
        if price_receipts.is_empty() {
            return Ok(HashMap::new());
        }
        return insufficient("risk snapshot without positions records price receipts");
    }
    if price_receipts.is_empty() {
        return insufficient("risk snapshot with open positions has no price receipts");
    }

    let wanted = positions
        .iter()
        .map(|position| (position.market_id.clone(), position.outcome_id))
        .collect::<HashSet<_>>();
    if wanted.len() != positions.len() {
        return insufficient("risk snapshot contains duplicate open positions");
    }
    let mut prices = HashMap::new();
    let mut observed_markets = HashSet::new();
    for receipt in price_receipts {
        let observation = source
            .get(&receipt.sequence.0)
            .filter(|observation| observation.receipt == *receipt)
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "risk price receipt is absent from the sealed source prefix".to_owned(),
                )
            })?;
        if observation.source_id != GAMMA_MARKETS_SOURCE_ID
            || observation.schema_version != GAMMA_MARKETS_SCHEMA_VERSION
            || observation.parser_version != GAMMA_MARKETS_PARSER_VERSION
        {
            return insufficient("risk price receipt has the wrong Gamma source contract");
        }
        if observation.received_unix_ms > evaluated_at_unix_ms {
            return insufficient("risk price receipt is from the future");
        }
        let age_ms = evaluated_at_unix_ms
            .checked_sub(observation.received_unix_ms)
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "risk price observation age overflow".to_owned(),
                )
            })?;
        if age_ms >= 60_000 {
            return insufficient("risk price receipt is stale");
        }

        let requested = wanted
            .iter()
            .map(|(market, _)| market.to_string())
            .collect::<Vec<_>>();
        let replayed = GammaMarketsClient::new(
            "https://offline.invalid".to_owned(),
            RecordedPageFetcher {
                payload: observation.payload.clone(),
            },
        )
        .with_batch_size(requested.len().max(1))
        .fetch_markets_with_pages(&requested, MarketFilter::OpenOnly)
        .await
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "risk Gamma production parse failed: {error}"
            ))
        })?;
        if !replayed.conflicting_condition_ids.is_empty()
            || replayed.pages.len() != 1
            || replayed.pages[0].1 != observation.payload
        {
            return insufficient("risk Gamma response has conflicting or incomplete raw evidence");
        }
        let mut receipt_used = false;
        for row in replayed.markets.markets.into_values() {
            let market_id = MarketId(VenueMarketId(row.condition_id));
            let outcomes = wanted
                .iter()
                .filter(|(market, _)| market == &market_id)
                .map(|(_, outcome)| *outcome)
                .collect::<Vec<_>>();
            if outcomes.is_empty() {
                continue;
            }
            if !observed_markets.insert(market_id.clone()) {
                return insufficient("risk Gamma evidence repeats an open market");
            }
            let strict_prices = row.strict_outcome_prices.ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "risk Gamma evidence omits outcomePrices".to_owned(),
                )
            })?;
            for outcome in outcomes {
                let price = strict_prices.get(usize::from(outcome.0)).ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "risk Gamma evidence omits an open outcome".to_owned(),
                    )
                })?;
                if prices
                    .insert((market_id.clone(), outcome), *price)
                    .is_some()
                {
                    return insufficient("risk Gamma evidence conflicts for an open outcome");
                }
            }
            receipt_used = true;
        }
        if !receipt_used {
            return insufficient("risk price receipt was not consumed by an open position");
        }
    }
    if prices.len() != wanted.len() || wanted.iter().any(|key| !prices.contains_key(key)) {
        return insufficient("risk price evidence is incomplete for open positions");
    }
    Ok(prices)
}

fn source_receipt<'a>(
    source: &'a BTreeMap<u64, SourceObservation>,
    receipt: AppendReceipt,
    source_id: &str,
    schema_version: u32,
    parser_version: u32,
    prepared_received_unix_ms: i64,
) -> Result<&'a SourceObservation, QualificationError> {
    let observation = source
        .get(&receipt.sequence.0)
        .filter(|observation| observation.receipt == receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "{source_id} receipt is absent from the sealed source prefix"
            ))
        })?;
    if observation.source_id != source_id
        || observation.schema_version != schema_version
        || observation.parser_version != parser_version
        || observation.content_type != ContentType::Json
        || observation.received_unix_ms > prepared_received_unix_ms
    {
        return insufficient(format!(
            "{source_id} receipt has the wrong source contract or is noncausal"
        ));
    }
    Ok(observation)
}

fn replayed_admission(
    economic: &EconomicPrepared,
    context: &RiskReplayContext<'_>,
) -> Result<LiveAdmissionArtifact, QualificationError> {
    const CLOB_LONG_SOURCE_ID: &str = "polymarket.clob.markets";
    const CLOB_COMPACT_SOURCE_ID: &str = "polymarket.clob.compact-market";
    const LIVE_MARKET_FRESHNESS_SECS: u64 = 60;

    let receipts = economic.admission.receipts;
    if [
        receipts.gamma.sequence,
        receipts.clob_long.sequence,
        receipts.clob_compact.sequence,
        economic.book_receipt.sequence,
    ]
    .into_iter()
    .collect::<HashSet<_>>()
    .len()
        != 4
    {
        return insufficient("economic admission and book receipts are not unique");
    }
    let gamma = source_receipt(
        context.source,
        receipts.gamma,
        GAMMA_MARKETS_SOURCE_ID,
        GAMMA_MARKETS_SCHEMA_VERSION,
        GAMMA_MARKETS_PARSER_VERSION,
        context.prepared_received_unix_ms,
    )?;
    let clob_long = source_receipt(
        context.source,
        receipts.clob_long,
        CLOB_LONG_SOURCE_ID,
        LIVE_MARKET_SCHEMA_VERSION,
        LIVE_MARKET_PARSER_VERSION,
        context.prepared_received_unix_ms,
    )?;
    let clob_compact = source_receipt(
        context.source,
        receipts.clob_compact,
        CLOB_COMPACT_SOURCE_ID,
        LIVE_MARKET_SCHEMA_VERSION,
        LIVE_MARKET_PARSER_VERSION,
        context.prepared_received_unix_ms,
    )?;
    if economic.admission.market.freshness_window_secs != LIVE_MARKET_FRESHNESS_SECS
        || economic.admission.market.observed_at_unix < 0
        || economic
            .admission
            .market
            .observed_at_unix
            .checked_mul(1_000)
            .is_none_or(|observed| observed > gamma.received_unix_ms)
    {
        return insufficient("economic admission clock or freshness contract is invalid");
    }
    let market = validate_live_market(
        &gamma.payload,
        &clob_long.payload,
        &economic.market.condition_id,
        economic.admission.market.observed_at_unix,
        LIVE_MARKET_FRESHNESS_SECS,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "economic long-market replay failed: {error}"
        ))
    })?;
    let compact = parse_compact_market(
        &clob_compact.payload,
        &economic.market.condition_id,
        &market.ordered_outcome_token_ids,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "economic compact-market replay failed: {error}"
        ))
    })?;
    if compact.minimum_order_size != market.minimum_order_size
        || compact.minimum_tick_size != market.minimum_tick_size
        || compact.neg_risk != market.neg_risk
    {
        return insufficient("economic compact and long market rules disagree");
    }
    let settlement = VenueSettlementRecord {
        schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
        condition_id: economic.market.condition_id.clone(),
        status: VenueResolutionStatus::Unresolved,
        raw_evidence_hash: blake3::hash(&clob_long.payload).to_hex().to_string(),
        source_timestamp_unix: None,
        observed_at_unix: market.observed_at_unix,
        parser_version: 1,
        freshness_window_secs: LIVE_MARKET_FRESHNESS_SECS,
    };
    Ok(LiveAdmissionArtifact {
        market,
        settlement,
        fee_schedule: compact.fee_schedule,
        receipts: AdmissionReceipts {
            gamma: receipts.gamma,
            clob_long: receipts.clob_long,
            clob_compact: receipts.clob_compact,
        },
    })
}

fn replayed_sized_plan(
    economic: &EconomicPrepared,
    admission: &LiveAdmissionArtifact,
    cash_before: CollateralAmount,
    context: &RiskReplayContext<'_>,
) -> Result<pe_venue_polymarket::SizedBuyPlan, QualificationError> {
    let book_observation = source_receipt(
        context.source,
        economic.book_receipt,
        "polymarket.clob.book",
        1,
        1,
        context.prepared_received_unix_ms,
    )?;
    let book = crate::clob_book::OrderBook::from_book_json(&book_observation.payload).map_err(
        |error| {
            QualificationError::InsufficientEvidence(format!(
                "economic book replay failed: {error}"
            ))
        },
    )?;
    let asks = book.ladder().ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "economic book cannot form the production ladder".to_owned(),
        )
    })?;
    let best_ask = asks.first().map(|ask| ask.price).ok_or_else(|| {
        QualificationError::InsufficientEvidence("economic book has no eligible asks".to_owned())
    })?;
    let impact_ceiling = recorded_impact_ceiling(best_ask, economic.balance.price_impact_cap_bps)?;
    let allocate = |price: Price| {
        let SizingModeAudit::Kelly {
            fraction,
            probability,
        } = economic.sizing.mode
        else {
            return Err(LadderError::KellySizing);
        };
        let quantity = size_contracts(&KellyInput {
            p: probability,
            c: price,
            kelly_fraction: fraction,
            bankroll: cash_before.to_decimal(),
        })
        .map_err(|_| LadderError::KellySizing)?;
        ShareAmount::from_whole(quantity.0).map_err(|_| LadderError::Amount)
    };
    let sizing = match economic.sizing.mode {
        SizingModeAudit::Dollar { usd } => {
            let budget = CollateralAmount::from_decimal_exact(
                usd.max(Decimal::ZERO)
                    .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero),
            )
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "economic Dollar budget is invalid: {error}"
                ))
            })?;
            if budget != economic.sizing.budget {
                return insufficient("economic Dollar budget differs from its sizing mode");
            }
            BuySizing::Dollar { budget }
        }
        SizingModeAudit::Contract { contracts } => BuySizing::Contract { contracts },
        SizingModeAudit::Kelly { .. } => BuySizing::Kelly {
            allocate: &allocate,
            slippage_rate: economic.sizing.slippage_rate,
        },
    };
    let proportional_cap = CollateralAmount::from_decimal_exact(
        cash_before
            .to_decimal()
            .checked_mul(Decimal::from(economic.risk.snapshot.per_trade_cap_bps))
            .and_then(|value| value.checked_div(Decimal::from(10_000u32)))
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "economic per-trade cap arithmetic overflow".to_owned(),
                )
            })?
            .max(Decimal::ZERO)
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero),
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "economic per-trade cap is invalid: {error}"
        ))
    })?;
    let sized = plan_sized_buy(
        &asks,
        admission.fee_schedule,
        sizing,
        &[cash_before, proportional_cap],
        admission.market.minimum_order_size,
        admission.market.minimum_tick_size,
        economic.balance.band_floor,
        economic.balance.band_ceiling_exclusive,
        economic.balance.chase_ceiling,
        impact_ceiling,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "economic sized-plan replay failed: {error}"
        ))
    })?;
    if sized.budget != economic.sizing.budget
        || sized.reserve != economic.fee.reserve
        || pe_execution_core::LadderPlanAudit::new(&sized.ladder) != economic.ladder
    {
        return insufficient("economic ladder, sizing, or cap decision differs from replay");
    }
    Ok(sized)
}

fn replayed_risk_base(
    leader_wallet: pe_core_types::WalletAddress,
    market_id: &str,
    proposed_debit: CollateralAmount,
    per_trade_cap_bps: i32,
    era: &crate::paper_recovery::PaperEra,
) -> Result<RiskSnapshot, QualificationError> {
    build_paper_risk_base(
        era,
        leader_wallet,
        market_id,
        proposed_debit,
        per_trade_cap_bps,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "risk base reconstruction failed: {error}"
        ))
    })
}

async fn verify_economic(
    operation: &crate::paper_recovery::PaperFillOperationIdentity,
    economic: &EconomicPrepared,
    context: &RiskReplayContext<'_>,
) -> Result<(), QualificationError> {
    let cash_before = CollateralAmount::from_decimal_exact(context.cash).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "replayed cash cannot be represented exactly: {error}"
        ))
    })?;
    let observation = economic.observation.as_ref().ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "Financial Fill lacks receipt-bearing source observation".to_owned(),
        )
    })?;
    let observed_source = context
        .source
        .get(&observation.source_receipt.sequence.0)
        .filter(|source| source.receipt == observation.source_receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "Fill observation receipt is absent from the sealed source prefix".to_owned(),
            )
        })?;
    let complete_bound = context
        .source
        .get(&observation.complete_bound_receipt.sequence.0)
        .filter(|source| source.receipt == observation.complete_bound_receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "Fill complete-bound receipt is absent from the sealed source prefix".to_owned(),
            )
        })?;
    if observation.source_receipt.sequence > observation.complete_bound_receipt.sequence
        || observed_source.received_unix_ms > context.prepared_received_unix_ms
        || complete_bound.received_unix_ms > context.prepared_received_unix_ms
    {
        return insufficient("Fill source observation receipts are noncausal");
    }
    if economic.risk.evaluated_at_unix_ms < 0
        || economic.risk.evaluated_at_unix_ms > context.prepared_received_unix_ms
        || context
            .paper_prefix
            .last()
            .map(|frame| received_unix_ms(&frame.envelope))
            .transpose()?
            .is_none_or(|prefix_ms| economic.risk.evaluated_at_unix_ms < prefix_ms)
    {
        return insufficient("risk evaluation clock is outside its causal paper interval");
    }
    let admission = replayed_admission(economic, context)?;
    let sized = replayed_sized_plan(economic, &admission, cash_before, context)?;
    let evaluated_at_unix = economic.risk.evaluated_at_unix_ms.div_euclid(1_000);
    let snapshot = replayed_financial_snapshot(context, evaluated_at_unix)?;
    let current_prices = replayed_risk_prices(
        &economic.risk.price_receipts,
        economic.risk.evaluated_at_unix_ms,
        &snapshot.positions,
        context.source,
    )
    .await?;
    let era = paper_era(context.paper_prefix.to_vec());
    let latency_was_active =
        active_risk_halts(&era).contains(&(RiskHaltOwner::Paper, RiskHaltCause::CopyLatency));
    let proposed_debit = sized.worst_case_all_in_debit().map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "proposed risk debit reconstruction failed: {error}"
        ))
    })?;
    let base = replayed_risk_base(
        operation.leader_wallet,
        &economic.market.market_id,
        proposed_debit,
        economic.risk.snapshot.per_trade_cap_bps,
        &era,
    )?;
    let reconstructed = build_paper_risk_snapshot(
        &base,
        &snapshot,
        &era,
        &current_prices,
        context.source_log_path,
        evaluated_at_unix,
        latency_was_active,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "risk snapshot reconstruction failed: {error}"
        ))
    })?;
    if reconstructed != economic.risk.snapshot {
        return insufficient("EconomicPrepared risk snapshot differs from causal replay");
    }

    let expected_risk = match evaluate_risk(&reconstructed) {
        RiskDecision::Approved => RiskDecisionAudit::Approved,
        RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
    };
    if expected_risk != economic.risk.decision {
        return insufficient("EconomicPrepared risk decision differs from shared risk owner");
    }

    let risk = RiskAudit {
        snapshot: reconstructed,
        decision: expected_risk,
        price_receipts: economic.risk.price_receipts.clone(),
        evaluated_at_unix_ms: economic.risk.evaluated_at_unix_ms,
    };
    let recomposed = EconomicPrepared::compose(EconomicInputs {
        market: economic.market.clone(),
        admission: &admission,
        plan: &sized.ladder,
        book_receipt: economic.book_receipt,
        observation: economic.observation.clone(),
        sizing_mode: economic.sizing.mode,
        budget: sized.budget,
        slippage_rate: economic.sizing.slippage_rate,
        risk,
        cash_before,
        price_impact_cap_bps: economic.balance.price_impact_cap_bps,
        chase_ceiling: economic.balance.chase_ceiling,
        band_floor: economic.balance.band_floor,
        band_ceiling_exclusive: economic.balance.band_ceiling_exclusive,
        applied_configuration_hash: economic.applied_configuration_hash.clone(),
    })
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "EconomicPrepared canonical composition failed: {error}"
        ))
    })?;
    let recomposed_hash = recomposed.core_hash().map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "recomposed economic core hash failed: {error}"
        ))
    })?;
    let recorded_hash = economic.core_hash().map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "recorded economic core hash failed: {error}"
        ))
    })?;
    if &recomposed != economic || recomposed_hash != recorded_hash {
        return insufficient("EconomicPrepared differs from raw-evidence canonical replay");
    }
    Ok(())
}

fn recorded_impact_ceiling(best_ask: Price, cap_bps: i32) -> Result<Price, QualificationError> {
    let cap_bps = u32::try_from(cap_bps)
        .ok()
        .filter(|cap| (1..=10_000).contains(cap))
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "EconomicPrepared price-impact cap is outside 1..=10000".to_owned(),
            )
        })?;
    let numerator = 10_000u32.checked_add(cap_bps).ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "EconomicPrepared price-impact cap overflow".to_owned(),
        )
    })?;
    let ceiling = best_ask
        .0
        .checked_mul(Decimal::from(numerator))
        .and_then(|value| value.checked_div(Decimal::from(10_000u32)))
        .map(|value| value.min(Decimal::ONE))
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "EconomicPrepared price-impact arithmetic overflow".to_owned(),
            )
        })?;
    Price::new(ceiling).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "EconomicPrepared price-impact ceiling is invalid: {error}"
        ))
    })
}

fn apply_fill_position(
    positions: &mut Vec<OpenPosition>,
    economic: &EconomicPrepared,
    quantity: ShareAmount,
) -> Result<(), QualificationError> {
    let existing = positions.iter_mut().find(|position| {
        position.condition_id == economic.market.condition_id.0
            && position.outcome_index == economic.market.outcome_index
    });
    match (economic.market.side, existing) {
        (Side::Buy, Some(position)) => {
            position.shares_atomic = position
                .shares_atomic
                .checked_add(quantity.atomic())
                .ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "paper position quantity overflow".to_owned(),
                    )
                })?;
        }
        (Side::Buy, None) => positions.push(OpenPosition {
            condition_id: economic.market.condition_id.0.clone(),
            market_id: economic.market.market_id.clone(),
            outcome_index: economic.market.outcome_index,
            shares_atomic: quantity.atomic(),
        }),
        (Side::Sell, Some(position)) => {
            position.shares_atomic = position
                .shares_atomic
                .checked_sub(quantity.atomic())
                .ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "paper SELL exceeds replayed inventory".to_owned(),
                    )
                })?;
        }
        (Side::Sell, None) => return insufficient("paper SELL has no replayed inventory"),
    }
    positions.retain(|position| position.shares_atomic != 0);
    Ok(())
}

fn causal_financial_state(
    starting_bankroll: Decimal,
    facts: &[CompletedFinancialFact],
    mark: &PortfolioMark,
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<CausalFinancialState, QualificationError> {
    let cutoff_ms = mark.cutoff_unix.checked_mul(1_000).ok_or_else(|| {
        QualificationError::InsufficientEvidence("PortfolioMark cutoff overflow".to_owned())
    })?;
    let is_causal = |receipt: AppendReceipt| -> Result<bool, QualificationError> {
        let observation = source
            .get(&receipt.sequence.0)
            .filter(|observation| observation.receipt == receipt)
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "financial fact source receipt is absent from the sealed prefix".to_owned(),
                )
            })?;
        Ok(receipt.sequence <= mark.boundary_receipt.sequence
            && observation.received_unix_ms < cutoff_ms)
    };

    let mut state = CausalFinancialState {
        cash: starting_bankroll,
        positions: Vec::new(),
        last_completed: None,
        completed_prepared: HashSet::new(),
        closed_fill_final_conditions: HashMap::new(),
        open_fill_finals: Vec::new(),
    };
    for fact in facts {
        let causal = match &fact.payload {
            FinancialPayload::Fill { economic, .. } => {
                let observation = economic.observation.as_ref().ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "Financial Fill has no causal observation".to_owned(),
                    )
                })?;
                is_causal(observation.source_receipt)?
            }
            FinancialPayload::Resolution {
                resolution_source_receipt,
                ..
            } => is_causal(*resolution_source_receipt)?,
        };
        if !causal {
            continue;
        }
        match (&fact.payload, &fact.result) {
            (FinancialPayload::Fill { economic, .. }, FinancialResult::Fill { canonical }) => {
                state.cash = state
                    .cash
                    .checked_sub(canonical.principal.to_decimal())
                    .and_then(|cash| cash.checked_sub(canonical.fee.to_decimal()))
                    .ok_or_else(|| {
                        QualificationError::InsufficientEvidence(
                            "causal mark cash underflow while replaying a fill".to_owned(),
                        )
                    })?;
                apply_fill_position(&mut state.positions, economic, canonical.quantity)?;
                state.open_fill_finals.push((
                    fact.final_receipt.sequence.0,
                    economic.market.condition_id.0.clone(),
                ));
            }
            (
                FinancialPayload::Resolution {
                    condition_id,
                    payout_by_outcome_index_json,
                    ..
                },
                FinancialResult::Resolution { .. },
            ) => {
                let payouts = BinaryPayoutVector::from_canonical_json(payout_by_outcome_index_json)
                    .map_err(|error| {
                        QualificationError::InsufficientEvidence(format!(
                            "causal mark resolution payout decode: {error}"
                        ))
                    })?;
                let decimals = payouts.decimals();
                let payout = BinaryPayout::new(decimals[0], decimals[1]).map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "causal mark resolution payout vector: {error}"
                    ))
                })?;
                let mut positions = BTreeMap::<u16, ShareAmount>::new();
                for position in state
                    .positions
                    .iter()
                    .filter(|position| position.condition_id == condition_id.0)
                {
                    let entry = positions
                        .entry(u16::from(position.outcome_index))
                        .or_insert(ShareAmount::ZERO);
                    *entry = entry
                        .checked_add(ShareAmount::from_atomic(position.shares_atomic))
                        .map_err(|_| {
                            QualificationError::InsufficientEvidence(
                                "causal mark resolution quantity overflow".to_owned(),
                            )
                        })?;
                }
                let credit = aggregate_resolution_credit(
                    &positions.into_iter().collect::<Vec<_>>(),
                    &payout,
                )
                .map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "causal mark resolution credit arithmetic: {error}"
                    ))
                })?;
                state.cash = state.cash.checked_add(credit.to_decimal()).ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "causal mark resolution cash overflow".to_owned(),
                    )
                })?;
                for (fill_sequence, fill_condition) in &state.open_fill_finals {
                    if fill_condition == &condition_id.0 {
                        state
                            .closed_fill_final_conditions
                            .insert(*fill_sequence, fill_condition.clone());
                    }
                }
                state
                    .open_fill_finals
                    .retain(|(_, fill_condition)| fill_condition != &condition_id.0);
                state
                    .positions
                    .retain(|position| position.condition_id != condition_id.0);
            }
            _ => return insufficient("causal mark Prepared/Final kinds disagree"),
        }
        state.last_completed = Some(fact.prepared_receipt.sequence);
        state
            .completed_prepared
            .insert(fact.prepared_receipt.sequence);
    }
    Ok(state)
}

async fn verify_mark(
    mark: &PortfolioMark,
    financial: &CausalFinancialState,
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<QualificationMarkReport, QualificationError> {
    if mark.invalid.is_some()
        || mark.equity <= Decimal::ZERO
        || mark.cash != financial.cash
        || mark.cutoff_unix.rem_euclid(86_400) != 0
        || mark.financial_prefix_seq != financial.last_completed
    {
        return insufficient("PortfolioMark is invalid or its cash differs from replay");
    }
    let boundary = source
        .get(&mark.boundary_receipt.sequence.0)
        .filter(|observation| observation.receipt == mark.boundary_receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "PortfolioMark boundary receipt is absent from the sealed source prefix".to_owned(),
            )
        })?;
    let boundary_payload: serde_json::Value =
        serde_json::from_slice(&boundary.payload).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "PortfolioMark boundary payload is invalid: {error}"
            ))
        })?;
    if boundary.source_id != "pe-service.boundary"
        || boundary.schema_version != 1
        || boundary.parser_version != 1
        || boundary_payload
            .get("kind")
            .and_then(|value| value.as_str())
            != Some("daily_boundary")
        || boundary_payload
            .get("cutoff_unix")
            .and_then(serde_json::Value::as_i64)
            != Some(mark.cutoff_unix)
    {
        return insufficient("PortfolioMark boundary evidence is not canonical");
    }
    let source_tail_sequence = mark.source_tail.last_sequence.ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "PortfolioMark source tail is empty after its boundary".to_owned(),
        )
    })?;
    let expected_source_tail_hash = tail_hash(&mark.source_tail)?;
    if source_tail_sequence < mark.boundary_receipt.sequence
        || source
            .get(&source_tail_sequence.0)
            .is_none_or(|observation| observation.receipt.this_hash != expected_source_tail_hash)
    {
        return insufficient("PortfolioMark source tail does not bind its causal evidence");
    }

    let mut prices = HashMap::new();
    let mut price_receipts = HashSet::new();
    for price in &mark.prices {
        if price.invalid.is_some() {
            return insufficient("PortfolioMark contains invalid price evidence");
        }
        let value = price.price.ok_or_else(|| {
            QualificationError::InsufficientEvidence("PortfolioMark price is missing".to_owned())
        })?;
        let receipt = price.receipt.ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "PortfolioMark source receipt is missing".to_owned(),
            )
        })?;
        if source
            .get(&receipt.sequence.0)
            .is_none_or(|observation| observation.receipt != receipt)
        {
            return insufficient("PortfolioMark receipt is absent from sealed source prefix");
        }
        if receipt.sequence <= mark.boundary_receipt.sequence
            || receipt.sequence > source_tail_sequence
            || !price_receipts.insert(receipt_key(receipt))
        {
            return insufficient(
                "PortfolioMark price receipt is duplicated or outside its causal source interval",
            );
        }
        let observation = source.get(&receipt.sequence.0).ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "PortfolioMark price observation disappeared during replay".to_owned(),
            )
        })?;
        let classified = classify_recorded_prices_history(observation).await?;
        let selected =
            historical_mark_price(&classified, mark.cutoff_unix, receipt).map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "PortfolioMark historical selection failed: {error}"
                ))
            })?;
        if selected.price != value
            || Some(selected.sample_unix) != price.sample_unix
            || selected.receipt != receipt
        {
            return insufficient("PortfolioMark price differs from its causal historical response");
        }
        if prices
            .insert((price.market_id.clone(), price.outcome_id), value)
            .is_some()
        {
            return insufficient("PortfolioMark contains duplicate position prices");
        }
    }
    let mut equity = financial.cash;
    for position in &financial.positions {
        let outcome_id = u16::from(position.outcome_index);
        let price = prices
            .get(&(position.market_id.clone(), outcome_id))
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(format!(
                    "PortfolioMark omits {}/{outcome_id}",
                    position.market_id
                ))
            })?;
        let value = ShareAmount::from_atomic(position.shares_atomic)
            .to_decimal()
            .checked_mul(price.0)
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence("mark equity overflow".to_owned())
            })?;
        equity = equity.checked_add(value).ok_or_else(|| {
            QualificationError::InsufficientEvidence("mark equity overflow".to_owned())
        })?;
    }
    if equity != mark.equity {
        return insufficient("PortfolioMark equity differs from exact replay");
    }
    Ok(QualificationMarkReport {
        cutoff_unix: mark.cutoff_unix,
        cash: mark.cash,
        equity: mark.equity,
    })
}

async fn classify_recorded_prices_history(
    observation: &SourceObservation,
) -> Result<ClassifiedPricesHistory, QualificationError> {
    if observation.source_id != "pe-service.clob-prices-history"
        || observation.schema_version != 1
        || observation.parser_version != 1
    {
        return insufficient("PortfolioMark price receipt has the wrong source contract");
    }
    let replayed = ClobPricesHistoryClient::new(
        "https://offline.invalid".to_owned(),
        RecordedPageFetcher {
            payload: observation.payload.clone(),
        },
    )
    .with_fidelity_minutes(1)
    .fetch_prices_history_classified("recorded", 0, 1)
    .await
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "PortfolioMark production historical parse failed: {error}"
        ))
    })?;
    if replayed.body != observation.payload {
        return insufficient("PortfolioMark historical parser did not consume the recorded body");
    }
    if let ClassifiedPricesHistory::Points(points) = &replayed.outcome {
        let mut timestamps = HashSet::new();
        if points.iter().any(|point| !timestamps.insert(point.t)) {
            return insufficient("PortfolioMark historical response contains a duplicate sample");
        }
    }
    Ok(replayed.outcome)
}

fn complete_day_growth(
    marks: &[QualificationMarkReport],
) -> Result<Vec<Decimal>, QualificationError> {
    let mut growth = Vec::new();
    for pair in marks.windows(2) {
        if pair[1].cutoff_unix.checked_sub(pair[0].cutoff_unix) != Some(86_400) {
            return insufficient("PortfolioMark sequence omits or duplicates a complete UTC day");
        }
        let ratio = pair[1]
            .equity
            .checked_div(pair[0].equity)
            .and_then(|value| value.checked_ln())
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "complete-day log-equity growth is invalid".to_owned(),
                )
            })?;
        growth.push(ratio);
    }
    Ok(growth)
}

/// Scenario helper for fixtures whose entire financial era is already causal.
#[cfg(feature = "scenario")]
#[must_use]
pub fn qualification_completion(
    era: &crate::paper_recovery::PaperEra,
) -> Option<QualificationCompletion> {
    qualification_completion_inner(era, None)
}

/// Derive completion using the exact causal facts selected for the boundary's financial state.
/// Invalid or non-daily marks cannot trigger an automatic seal.
pub(crate) fn qualification_completion_for_causal_facts(
    era: &crate::paper_recovery::PaperEra,
    completed_prepared: &HashSet<EventSeq>,
) -> Option<QualificationCompletion> {
    qualification_completion_inner(era, Some(completed_prepared))
}

fn qualification_completion_inner(
    era: &crate::paper_recovery::PaperEra,
    completed_prepared: Option<&HashSet<EventSeq>>,
) -> Option<QualificationCompletion> {
    era.start.as_ref()?;

    let mut waiting_for_anchor_mark = true;
    let mut anchor_sequence = None;
    let mut promotion_marks = Vec::new();
    let mut fill_prepared = HashMap::<(u64, String), String>::new();
    let mut resolution_prepared = HashMap::<(u64, String), String>::new();
    let mut fill_finals = Vec::<(u64, String)>::new();
    let mut resolution_finals = HashMap::<String, Vec<u64>>::new();

    for frame in &era.frames {
        let PaperLogFrame::Record(record) = &frame.frame else {
            return None;
        };
        match record {
            PaperLogRecord::MembershipChanged { reason, .. } if reason_moves_anchor(*reason) => {
                waiting_for_anchor_mark = true;
                anchor_sequence = None;
                promotion_marks.clear();
            }
            PaperLogRecord::PortfolioMark(mark) => {
                if mark.invalid.is_some() || mark.equity <= Decimal::ZERO {
                    return None;
                }
                if waiting_for_anchor_mark {
                    anchor_sequence = Some(frame.receipt.sequence.0);
                    waiting_for_anchor_mark = false;
                }
                promotion_marks.push(QualificationMarkReport {
                    cutoff_unix: mark.cutoff_unix,
                    cash: mark.cash,
                    equity: mark.equity,
                });
            }
            PaperLogRecord::FinancialPrepared { payload, .. } => match payload {
                FinancialPayload::Fill { economic, .. } => {
                    fill_prepared.insert(
                        receipt_key(frame.receipt),
                        economic.market.condition_id.0.clone(),
                    );
                }
                FinancialPayload::Resolution { condition_id, .. } => {
                    resolution_prepared.insert(receipt_key(frame.receipt), condition_id.0.clone());
                }
            },
            PaperLogRecord::FinancialFinal {
                prepared_receipt,
                result,
            } if completed_prepared
                .is_none_or(|completed| completed.contains(&prepared_receipt.sequence)) =>
            {
                match result {
                    FinancialResult::Fill { .. } => {
                        let condition = fill_prepared.get(&receipt_key(*prepared_receipt))?;
                        fill_finals.push((frame.receipt.sequence.0, condition.clone()));
                    }
                    FinancialResult::Resolution { .. } => {
                        let condition = resolution_prepared.get(&receipt_key(*prepared_receipt))?;
                        resolution_finals
                            .entry(condition.clone())
                            .or_default()
                            .push(frame.receipt.sequence.0);
                    }
                }
            }
            PaperLogRecord::QualificationStarted(_)
            | PaperLogRecord::QualificationSealed(_)
            | PaperLogRecord::RiskHaltChanged { .. }
            | PaperLogRecord::MembershipChanged { .. }
            | PaperLogRecord::FinancialFinal { .. } => {}
        }
    }

    let anchor_sequence = anchor_sequence?;
    let complete_days = complete_day_growth(&promotion_marks).ok()?.len();
    let causal_closes = fill_finals
        .iter()
        .filter(|(fill_sequence, condition)| {
            *fill_sequence > anchor_sequence
                && resolution_finals.get(condition).is_some_and(|resolutions| {
                    resolutions
                        .iter()
                        .any(|resolution_sequence| resolution_sequence > fill_sequence)
                })
        })
        .count();
    Some(QualificationCompletion {
        complete_days,
        causal_closes,
    })
}

fn reason_moves_anchor(reason: MembershipReason) -> bool {
    matches!(
        reason,
        MembershipReason::KnockoutInactivity
            | MembershipReason::KnockoutInactivityHardCap
            | MembershipReason::KnockoutUnderperformance
    )
}

fn bind_final_receipts(
    decisions: &[crate::decision_replay::ReplayedDecision],
    fills: &[CompletedFill],
    decision_observations: &HashMap<
        pe_core_types::SourceTradeId,
        pe_execution_core::ObservationEvidence,
    >,
) -> Result<(), QualificationError> {
    let mut matched_finals = HashSet::new();
    for decision in decisions {
        let terminal = &decision.post_boundary.body.terminal;
        if terminal.disposition != "fill" {
            continue;
        }
        let final_receipt = terminal.final_receipt.ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "fill decision {} has no FinancialFinal receipt",
                decision.continuation.prior.source_trade_id
            ))
        })?;
        let mut candidates = fills
            .iter()
            .filter(|fill| fill.final_receipt == final_receipt);
        let fill = candidates.next().ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "fill decision {} references no FinancialFinal",
                decision.continuation.prior.source_trade_id
            ))
        })?;
        if candidates.next().is_some() || !matched_finals.insert(receipt_key(final_receipt)) {
            return insufficient("multiple fill decisions bind the same FinancialFinal");
        }

        let continuation = &decision.continuation.prior;
        let decision_key = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
            &TraderId(continuation.wallet).to_string(),
            &continuation.source_trade_id.0,
            &continuation.market_id.0.0,
            continuation.outcome_id.0,
            continuation.side,
            continuation.source_epoch,
        );
        let prepared_key = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
            &TraderId(fill.operation.leader_wallet).to_string(),
            &fill.operation.source_trade_id.0,
            &fill.market_id,
            fill.outcome_id,
            fill.side,
            fill.operation.observed_at_bucket,
        );
        if decision_key != prepared_key
            || continuation.wallet != fill.operation.leader_wallet
            || continuation.source_trade_id != fill.operation.source_trade_id
            || continuation.source_epoch != fill.operation.observed_at_bucket
            || continuation.market_id.0.0 != fill.market_id
            || continuation.outcome_id.0 != fill.outcome_id
            || continuation.side != fill.side
            || decision_observations.get(&continuation.source_trade_id) != Some(&fill.observation)
        {
            return insufficient(
                "fill decision, Prepared operation, and FinancialFinal identity disagree",
            );
        }
    }
    if matched_finals.len() != fills.len() {
        return insufficient("a FinancialFinal has no identical terminal fill decision");
    }
    Ok(())
}

/// Return the fail-closed seal result required before applying a changed semantic hash.
/// An unchanged hash (including an unrelated artifact rebuild) does not seal.
#[must_use]
pub fn seal_if_semantic_drift(
    started: &QualificationStarted,
    proposed_hash: &str,
) -> Option<SealReason> {
    (started.hot_config_hash != proposed_hash).then(|| {
        SealReason::InsufficientEvidence(format!(
            "economic configuration changed from {} to {proposed_hash}",
            started.hot_config_hash
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialEraPaths {
    pub paper_log: PathBuf,
    pub source_log: PathBuf,
    pub live_journal: PathBuf,
    pub paper_state: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// The shell driver owns additive, fsynced transition receipts at the manifest top level. Rust owns
// and validates the semantic Start inputs below, but must remain able to inspect the manifest after
// any driver boundary has appended its receipt.
pub struct FinancialEraManifest {
    pub kind: String,
    pub state: String,
    pub activation_id: String,
    pub generation: String,
    pub fresh_bankroll: CollateralAmount,
    pub target_revision: String,
    pub artifact_blake3: String,
    pub static_config_hash: String,
    pub ranking_batch_id: i64,
    pub policy_hash: String,
    pub membership: Vec<pe_core_types::WalletAddress>,
    pub schema_version: u32,
    pub parser_version: u32,
    pub financial_semantic_version: u32,
    pub start_unix: i64,
    pub paths: FinancialEraPaths,
    pub old_artifact_sha256: String,
    pub target_artifact_sha256: String,
    pub old_config_sha256: String,
    pub target_config_sha256: String,
    pub old_environment_sha256: String,
    pub target_environment_sha256: String,
    pub preparation: Option<FinancialEraPreparation>,
    #[serde(default)]
    pub stop_invoked: bool,
    #[serde(default)]
    pub service_was_active: Option<bool>,
    #[serde(default)]
    pub backup: Option<FinancialEraFileIdentity>,
    #[serde(default)]
    pub remote_census: Option<FinancialEraRemoteCensus>,
    #[serde(default)]
    pub guarded_logs: Option<BTreeMap<String, FinancialEraLogIdentity>>,
    #[serde(default)]
    pub start_receipt: Option<AppendReceipt>,
    #[serde(default)]
    pub started_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialEraFileIdentity {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialEraLogIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialEraRemoteCensus {
    pub paper_fills: u64,
    pub settled_markets: u64,
    pub paper_positions: u64,
    pub paper_bankroll: u64,
    pub fill_market_snapshots: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinancialEraPreparation {
    pub start: QualificationStarted,
    pub expected_receipt: AppendReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinancialEraCommand {
    Prepare,
    Start,
    RollbackCheck,
}

pub fn run_financial_era(
    command: FinancialEraCommand,
    manifest_path: &Path,
    config: &ServiceConfig,
    financial_config_rows_path: Option<&Path>,
) -> Result<String, QualificationError> {
    let bytes = fs::read(manifest_path)?;
    let manifest: FinancialEraManifest = serde_json::from_slice(&bytes)?;
    validate_financial_manifest(&manifest, config)?;
    match command {
        FinancialEraCommand::Prepare => {
            if manifest.state != "prepared" {
                return insufficient("financial-era prepare requires manifest state prepared");
            }
            validate_financial_target_config(config)?;
            let config_rows = read_financial_config_rows(financial_config_rows_path)?;
            let preparation = prepare_financial_era(&manifest, config, &config_rows)?;
            Ok(serde_json::to_string(&preparation)?)
        }
        FinancialEraCommand::Start => {
            validate_financial_target_config(config)?;
            let config_rows = read_financial_config_rows(financial_config_rows_path)?;
            start_financial_era(&manifest, config, &config_rows)
        }
        FinancialEraCommand::RollbackCheck => rollback_check_financial_era(&manifest, config),
    }
}

fn validate_financial_target_config(config: &ServiceConfig) -> Result<(), QualificationError> {
    if !config.supabase_authoritative
        || config.supabase_url.trim().is_empty()
        || config.supabase_secret_key.trim().is_empty()
    {
        return insufficient(
            "financial-era target requires authoritative mode, Supabase URL, and service-role credential",
        );
    }
    Ok(())
}

fn configured_live_journal_path(config: &ServiceConfig) -> PathBuf {
    config
        .event_log_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("live_journal.log")
}

fn validate_financial_manifest(
    manifest: &FinancialEraManifest,
    config: &ServiceConfig,
) -> Result<(), QualificationError> {
    let configured_live_journal = configured_live_journal_path(config);
    if manifest.kind != FINANCIAL_ERA_KIND
        || manifest.paths.paper_log != config.event_log_path
        || manifest.paths.source_log != config.source_event_log_path
        || manifest.paths.live_journal != configured_live_journal
        || manifest.paths.paper_state != config.paper_state_db_path
    {
        return insufficient("financial-era manifest kind or configured paths differ");
    }
    if !is_lower_hex_64(&manifest.policy_hash) {
        return insufficient("financial-era policy hash must be exactly 64 lowercase hex digits");
    }
    Ok(())
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn read_financial_config_rows(path: Option<&Path>) -> Result<Vec<ConfigRow>, QualificationError> {
    let path = path.ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "financial-era prepare/start requires the exported Financial15 rows".to_owned(),
        )
    })?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn derive_hot_config_hash(
    rows: &[ConfigRow],
    config: &ServiceConfig,
) -> Result<String, QualificationError> {
    if rows.iter().any(|row| row.key == RISK_HALT_RELEASE_HASH_KEY) {
        return insufficient(
            "exported Financial15 rows must exclude the incident-only risk halt release",
        );
    }
    parse_config(
        rows,
        &RuntimeConfig::from_service_config(config),
        false,
        ConfigEra::Financial15,
    )
    .map(|runtime| runtime.canonical_hash())
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "exported Financial15 rows are invalid: {error}"
        ))
    })
}

fn derive_membership_proofs_hash(
    state: &PaperStateDb,
    membership: &[pe_core_types::WalletAddress],
) -> Result<String, QualificationError> {
    let mut unique = HashSet::new();
    let mut proofs = Vec::with_capacity(membership.len());
    for wallet in membership {
        if !unique.insert(*wallet) {
            return insufficient(format!("financial-era membership repeats wallet {wallet}"));
        }
        if !state.wallet_history_complete(wallet)? {
            return insufficient(format!(
                "financial-era membership lacks complete history for {wallet}"
            ));
        }
        let coverage = state.wallet_coverage(wallet)?;
        let (Some(activity_cutoff_unix), Some(anchor_seq), Some(anchored_at_unix)) = (
            coverage.activity_cutoff_unix,
            coverage.anchor_seq,
            coverage.anchored_at_unix,
        ) else {
            return insufficient(format!(
                "financial-era membership lacks installed anchor coverage for {wallet}"
            ));
        };
        if coverage.reanchor_required {
            return insufficient(format!(
                "financial-era membership requires a new anchor for {wallet}"
            ));
        }
        let anchors = state.position_anchors(wallet)?;
        let anchor = anchors.last().ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "financial-era membership lacks an anchor record for {wallet}"
            ))
        })?;
        if anchor.anchor_seq != anchor_seq
            || anchor.activity_cutoff_unix != activity_cutoff_unix
            || anchor.anchored_at_unix != anchored_at_unix
        {
            return insufficient(format!(
                "financial-era membership anchor and coverage disagree for {wallet}"
            ));
        }
        let validation = state.position_validation(wallet)?.ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "financial-era membership lacks a current position validation for {wallet}"
            ))
        })?;
        if validation.ledger_hash != anchor.ledger_hash_after
            || validation.proof_json != anchor.proof_json
        {
            return insufficient(format!(
                "financial-era membership validation and anchor disagree for {wallet}"
            ));
        }
        proofs.push(serde_json::json!({
            "wallet": wallet.to_string(),
            "coverage": {
                "activity_cutoff_unix": activity_cutoff_unix,
                "coverage_generation": coverage.coverage_generation,
                "reanchor_required": coverage.reanchor_required,
                "anchor_seq": anchor_seq,
                "anchored_at_unix": anchored_at_unix,
            },
            "anchor": {
                "anchor_seq": anchor.anchor_seq,
                "anchored_at_unix": anchor.anchored_at_unix,
                "activity_cutoff_unix": anchor.activity_cutoff_unix,
                "balances_json": anchor.balances_json,
                "ledger_hash_after": anchor.ledger_hash_after,
                "proof_json": anchor.proof_json,
            },
            "validation": {
                "ledger_hash": validation.ledger_hash,
                "positions_proof_hash": validation.positions_proof_hash,
                "activity_bounds_json": validation.activity_bounds_json,
                "source_log_generation": validation.source_log_generation,
                "proof_json": validation.proof_json,
                "recorded_at_unix": validation.recorded_at_unix,
            },
        }));
    }
    let membership: Vec<_> = membership.iter().map(ToString::to_string).collect();
    let bytes = serde_json::to_vec(&serde_json::json!({
        "membership": membership,
        "proofs": proofs,
    }))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn verify_live_preparation_posture(
    live_path: &Path,
    source_log_path: &Path,
    status_path: &Path,
) -> Result<LogTailBinding, QualificationError> {
    let source_envelopes = Reader::replay(source_log_path)?
        .map(|item| item.map(|(_, envelope)| envelope))
        .collect::<Result<Vec<_>, _>>()?;
    let before = pe_execution_core::LiveJournal::verified_tail(live_path).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("live journal: {error}"))
    })?;
    let open_live_orders = pe_execution_core::live_journal::open_order_inventory(live_path)
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!("live open-order inventory: {error}"))
        })?;
    if !open_live_orders.is_empty() {
        return insufficient("financial-era prepare found a nonterminal live order");
    }

    let status: serde_json::Value =
        serde_json::from_slice(&fs::read(status_path)?).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "financial-era live status is invalid: {error}"
            ))
        })?;
    let live = status
        .get("live")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "financial-era live status block is absent".to_owned(),
            )
        })?;
    if live.get("stale").and_then(serde_json::Value::as_bool) != Some(false)
        || live
            .get("pending_dispatch_seeds")
            .and_then(serde_json::Value::as_u64)
            != Some(0)
        || live
            .get("ready_dispatch_seeds")
            .and_then(serde_json::Value::as_u64)
            != Some(0)
    {
        return insufficient("financial-era live status is stale or has pending dispatch work");
    }
    let status_accounts = live
        .get("accounts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "financial-era live account status is absent".to_owned(),
            )
        })?;
    let mut account_ids = HashSet::new();
    for account in status_accounts {
        let account_id = account
            .get("account_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "financial-era live account identity is absent".to_owned(),
                )
            })?;
        let account_id = AccountId::new(account_id).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "financial-era live account identity is invalid: {error}"
            ))
        })?;
        if account
            .get("requested_live_mode")
            .and_then(serde_json::Value::as_str)
            != Some("off")
            || account
                .get("effective_live_mode")
                .and_then(serde_json::Value::as_str)
                != Some("off")
            || account.get("armed").and_then(serde_json::Value::as_bool) != Some(false)
            || !account_ids.insert(account_id)
        {
            return insufficient(
                "financial-era prepare requires every live account to be uniquely off and unarmed",
            );
        }
    }

    #[derive(Deserialize)]
    struct LiveAccountEnvelope {
        account_id: AccountId,
    }
    for item in Reader::replay(live_path)? {
        let (_, envelope) = item?;
        let event: LiveAccountEnvelope =
            serde_json::from_slice(&envelope.payload).map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "live journal account envelope is invalid: {error}"
                ))
            })?;
        account_ids.insert(event.account_id);
    }
    for account_id in account_ids {
        let events = pe_execution_core::live_journal::replay_account(live_path, &account_id)
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "live journal account replay failed for {account_id}: {error}"
                ))
            })?;
        crate::live_fanout::derive_projection_rows_with_sources(
            &account_id,
            &events,
            &source_envelopes,
        )
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "live journal strict reduction failed for {account_id}: {error}"
            ))
        })?;
        if pe_execution_core::reconstruct_redemption_attempts(&events)
            .values()
            .any(|attempt| {
                pe_execution_core::redemption_posture(&attempt.state).closes_new_buy_admission
            })
        {
            return insufficient(format!(
                "financial-era prepare found unresolved live custody for {account_id}"
            ));
        }
    }

    let after = pe_execution_core::LiveJournal::verified_tail(live_path).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("live journal: {error}"))
    })?;
    if after != before {
        return insufficient("live journal changed during financial-era preparation");
    }
    Ok(after)
}

fn prepare_financial_era(
    manifest: &FinancialEraManifest,
    config: &ServiceConfig,
    financial_config_rows: &[ConfigRow],
) -> Result<FinancialEraPreparation, QualificationError> {
    let paper_prefix = Scanner::verify(&manifest.paths.paper_log)?;
    let source_prefix = Scanner::verify(&manifest.paths.source_log)?;
    let live_path = configured_live_journal_path(config);
    let live_prefix = verify_live_preparation_posture(
        &live_path,
        &manifest.paths.source_log,
        &config.status_path,
    )?;
    let state = PaperStateDb::open_read_only(&manifest.paths.paper_state)?;
    if !state.open_decision_pending()?.is_empty() {
        return insufficient("financial-era prepare found an open decision");
    }
    let paper_frames = scan_paper_log(&manifest.paths.paper_log)?;
    if paper_frames.iter().any(|frame| {
        matches!(
            frame.frame,
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_))
        )
    }) {
        return insufficient("financial era is already started");
    }
    let mut unmatched_prepared = HashSet::new();
    for frame in &paper_frames {
        match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. })
                if !unmatched_prepared.insert(receipt_key(frame.receipt)) =>
            {
                return insufficient("financial-era prepare found a duplicate Prepared receipt");
            }
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) if !unmatched_prepared.remove(&receipt_key(*prepared_receipt)) => {
                return insufficient("financial-era prepare found an unmatched Final");
            }
            _ => {}
        }
    }
    if !unmatched_prepared.is_empty() {
        return insufficient("financial-era prepare found an unmatched Prepared");
    }
    let hot_config_hash = derive_hot_config_hash(financial_config_rows, config)?;
    let membership_proofs_hash = derive_membership_proofs_hash(&state, &manifest.membership)?;
    let start = QualificationStarted {
        starting_bankroll: manifest.fresh_bankroll,
        paper_prefix: TailBinding::from(&paper_prefix),
        source_prefix: TailBinding::from(&source_prefix),
        live_prefix: TailBinding::from(&live_prefix),
        artifact_blake3: manifest.artifact_blake3.clone(),
        static_config_hash: manifest.static_config_hash.clone(),
        hot_config_hash,
        generation: manifest.generation.clone(),
        activation_id: manifest.activation_id.clone(),
        ranking_batch_id: manifest.ranking_batch_id,
        policy_hash: manifest.policy_hash.clone(),
        membership: manifest.membership.clone(),
        membership_proofs_hash,
        schema_version: manifest.schema_version,
        parser_version: manifest.parser_version,
        financial_semantic_version: manifest.financial_semantic_version,
    };
    let envelope = start_envelope(&start, manifest.start_unix)?;
    let next_sequence = paper_prefix
        .last_sequence
        .map_or(Some(0), |sequence| sequence.0.checked_add(1))
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence("Start sequence overflow".to_owned())
        })?;
    let (_, _, this_hash) = compute_hashes(HashInput {
        seq: EventSeq(next_sequence),
        source_id: &envelope.source_id,
        schema_version: envelope.schema_version,
        parser_version: envelope.parser_version,
        observed_at: &envelope.observed_at,
        received_at: &envelope.received_at,
        content_type: &envelope.content_type,
        prev_hash: &paper_prefix.last_hash,
        payload: &envelope.payload,
    })?;
    Ok(FinancialEraPreparation {
        start,
        expected_receipt: AppendReceipt {
            sequence: EventSeq(next_sequence),
            this_hash,
        },
    })
}

fn start_financial_era(
    manifest: &FinancialEraManifest,
    config: &ServiceConfig,
    financial_config_rows: &[ConfigRow],
) -> Result<String, QualificationError> {
    if manifest.state != "guarded" && manifest.state != "started" {
        return insufficient("financial-era start requires guarded or started state");
    }
    let preparation = manifest.preparation.as_ref().ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "financial-era manifest has no verified preparation".to_owned(),
        )
    })?;
    let frames = scan_paper_log(&manifest.paths.paper_log)?;
    if let Some(existing) = frames.iter().find(|frame| {
        matches!(
            frame.frame,
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_))
        )
    }) {
        if existing.receipt == preparation.expected_receipt
            && matches!(
                &existing.frame,
                PaperLogFrame::Record(PaperLogRecord::QualificationStarted(start))
                    if start.as_ref() == &preparation.start
            )
        {
            return Ok(serde_json::to_string(&existing.receipt)?);
        }
        return insufficient("paper log contains a different QualificationStarted suffix");
    }
    let current = Scanner::verify(&manifest.paths.paper_log)?;
    if TailBinding::from(&current) != preparation.start.paper_prefix {
        return insufficient("paper tail changed after financial-era preparation");
    }
    let recomputed = prepare_financial_era(manifest, config, financial_config_rows)?;
    if &recomputed != preparation {
        return insufficient("financial-era preparation differs from the verified manifest inputs");
    }
    let state = PaperStateDb::open(&manifest.paths.paper_state)?;
    if !state.open_decision_pending()?.is_empty() {
        return insufficient("financial-era start found an open decision");
    }
    state.reset_financial_era(preparation.expected_receipt, manifest.fresh_bankroll)?;
    let expected = LogTailBinding {
        path: current.path,
        physical_tail: current.physical_tail,
        last_sequence: current.last_sequence,
        last_hash: current.last_hash,
    };
    let mut writer = Writer::open_with_expected_tail(&manifest.paths.paper_log, &expected)?;
    let receipt = writer.append_synced(start_envelope(&preparation.start, manifest.start_unix)?)?;
    if receipt != preparation.expected_receipt {
        return insufficient("synchronized QualificationStarted receipt differs from preparation");
    }
    Ok(serde_json::to_string(&receipt)?)
}

fn rollback_check_financial_era(
    manifest: &FinancialEraManifest,
    config: &ServiceConfig,
) -> Result<String, QualificationError> {
    validate_financial_manifest(manifest, config)?;
    verify_live_preparation_posture(
        &configured_live_journal_path(config),
        &config.source_event_log_path,
        &config.status_path,
    )?;
    let scan = Scanner::inspect(&manifest.paths.paper_log)?;
    let mut repaired = false;
    if scan.incomplete_tail.is_some() {
        let preparation = manifest.preparation.as_ref().ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "rollback-check has no trusted prepared paper tail".to_owned(),
            )
        })?;
        let expected = LogTailBinding {
            path: scan.verified_tail.path.clone(),
            physical_tail: preparation.start.paper_prefix.physical_tail,
            last_sequence: preparation.start.paper_prefix.last_sequence,
            last_hash: tail_hash(&preparation.start.paper_prefix)?,
        };
        Writer::open_with_expected_tail(&manifest.paths.paper_log, &expected)?;
        repaired = true;
    }
    let frames = scan_paper_log(&manifest.paths.paper_log)?;
    if let Some(started) = frames.iter().find(|frame| {
        matches!(
            frame.frame,
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_))
        )
    }) {
        let preparation = manifest.preparation.as_ref().ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "complete Start exists without a prepared manifest identity".to_owned(),
            )
        })?;
        if started.receipt != preparation.expected_receipt
            || !matches!(
                &started.frame,
                PaperLogFrame::Record(PaperLogRecord::QualificationStarted(start))
                    if start.as_ref() == &preparation.start
            )
        {
            return insufficient("complete Start differs from the financial-era manifest");
        }
        return Ok(format!(
            "{{\"complete_start\":true,\"receipt\":{}}}",
            serde_json::to_string(&started.receipt)?
        ));
    }
    Ok(format!(
        "{{\"complete_start\":false,\"repaired\":{repaired}}}"
    ))
}

fn start_envelope(
    start: &QualificationStarted,
    start_unix: i64,
) -> Result<EnvelopeIn, QualificationError> {
    let timestamp = OffsetDateTime::from_unix_timestamp(start_unix).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("invalid Start timestamp: {error}"))
    })?;
    Ok(EnvelopeIn {
        source_id: SourceId(QUALIFICATION_SOURCE_ID.to_owned()),
        schema_version: PAPER_LOG_SCHEMA_VERSION,
        parser_version: start.parser_version,
        observed_at: SourceTimestamp(timestamp),
        received_at: ReceivedAt(timestamp),
        content_type: ContentType::Json,
        payload: serde_json::to_vec(&PaperLogRecord::QualificationStarted(Box::new(
            start.clone(),
        )))?,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use pe_core_types::{
        BasisPoints, KellyFraction, LeaderAction, PolymarketConditionId, PolymarketTokenId,
        Probability, ProbabilityPpm, SourceTradeId, WalletAddress,
    };
    use pe_event_log::EventEnvelope;
    use pe_execution_core::{
        BalanceAudit, ECONOMIC_PREPARED_VERSION, FeeAudit, LadderAskAudit, LadderPlanAudit,
        LiveAdmissionArtifactAudit, LiveMarketEvidenceAudit, MarketSelection, ObservationEvidence,
        SizingAudit,
    };
    use pe_venue_polymarket::CompactFeeSchedule;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::paper_recovery::PaperEra;

    fn financial_config_rows() -> Vec<ConfigRow> {
        [
            ("active_watchlist_size", "100", "integer"),
            ("mode", "paper", "text"),
            ("max_fill_price", "0.85", "decimal"),
            ("min_fill_price", "0.15", "decimal"),
            ("min_resolution_horizon_secs", "60", "integer"),
            ("max_resolution_horizon_secs", "172800", "integer"),
            ("price_impact_cap_bps", "100", "integer"),
            ("flip_human_approved", "false", "bool"),
            (
                "kelly_fraction_above_default_human_approved",
                "false",
                "bool",
            ),
            ("per_trade_cap", "unlimited", "text"),
            ("slippage_rate", "0.01", "decimal"),
            ("sizing_mode", "dollar", "text"),
            ("sizing_dollar_usd", "25", "decimal"),
            ("sizing_contracts", "1", "integer"),
        ]
        .into_iter()
        .map(|(key, value, value_type)| ConfigRow {
            key: key.to_owned(),
            value: value.to_owned(),
            value_type: value_type.to_owned(),
        })
        .collect()
    }

    fn started(hash: &str) -> QualificationStarted {
        QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            paper_prefix: TailBinding {
                physical_tail: 5,
                last_sequence: None,
                last_hash: "00".repeat(32),
            },
            source_prefix: TailBinding {
                physical_tail: 5,
                last_sequence: None,
                last_hash: "00".repeat(32),
            },
            live_prefix: TailBinding {
                physical_tail: 5,
                last_sequence: None,
                last_hash: "00".repeat(32),
            },
            artifact_blake3: "artifact".to_owned(),
            static_config_hash: "static".to_owned(),
            hot_config_hash: hash.to_owned(),
            generation: "g557".to_owned(),
            activation_id: "act-557".to_owned(),
            ranking_batch_id: 7,
            policy_hash: "policy".to_owned(),
            membership: vec![WalletAddress::from_hex(&format!("0x{}", "1".repeat(40))).unwrap()],
            membership_proofs_hash: "membership".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
        }
    }

    fn append_paper_record_at(
        writer: &mut Writer,
        record: &PaperLogRecord,
        unix: i64,
    ) -> AppendReceipt {
        let timestamp = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(record).unwrap(),
            })
            .unwrap()
    }

    fn risk_economic(market_id: &str, principal: CollateralAmount) -> EconomicPrepared {
        let price = Price::new(dec!(0.5)).unwrap();
        let shares = ShareAmount::from_whole(2).unwrap();
        let receipt = |sequence| AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::hash(&sequence.to_be_bytes()),
        };
        EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: PolymarketConditionId(market_id.to_owned()),
                outcome_index: 0,
                token_id: PolymarketTokenId(format!("token-{market_id}")),
                side: Side::Buy,
                market_id: market_id.to_owned(),
            },
            admission: LiveAdmissionArtifactAudit {
                market: LiveMarketEvidenceAudit {
                    condition_id: PolymarketConditionId(market_id.to_owned()),
                    ordered_outcome_token_ids: [
                        PolymarketTokenId(format!("token-{market_id}")),
                        PolymarketTokenId(format!("other-{market_id}")),
                    ],
                    neg_risk: false,
                    minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: shares,
                    observed_at_unix: 1,
                    schema_version: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                settlement: VenueSettlementRecord {
                    schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                    condition_id: PolymarketConditionId(market_id.to_owned()),
                    status: VenueResolutionStatus::Unresolved,
                    raw_evidence_hash: "settlement".to_owned(),
                    source_timestamp_unix: Some(1),
                    observed_at_unix: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                fee_schedule: CompactFeeSchedule::Zero,
                scheduled_end_unix: Some(100),
                receipts: AdmissionReceipts {
                    gamma: receipt(10),
                    clob_long: receipt(11),
                    clob_compact: receipt(12),
                },
            },
            ladder: LadderPlanAudit {
                used_asks: vec![LadderAskAudit { price, shares }],
                best_ask: price,
                limit_price: price,
                minimum_shares: shares,
                principal,
            },
            book_receipt: receipt(13),
            observation: None,
            sizing: SizingAudit {
                mode: SizingModeAudit::Kelly {
                    fraction: KellyFraction::new(dec!(0.25)).unwrap(),
                    probability: Probability::new(dec!(0.6)).unwrap(),
                },
                budget: principal,
                principal,
                minimum_shares: shares,
                expected_shares: shares,
                expected_vwap: price,
                all_in_price: price,
                slippage_rate: Decimal::ZERO,
            },
            fee: FeeAudit {
                schedule: CompactFeeSchedule::Zero,
                expected_fee: CollateralAmount::ZERO,
                reserve: CollateralAmount::ZERO,
            },
            risk: RiskAudit {
                snapshot: RiskSnapshot {
                    leader_exposure_bps: pe_core_types::BasisPoints::ZERO,
                    market_exposure_bps: pe_core_types::BasisPoints::ZERO,
                    family_exposure_bps: pe_core_types::BasisPoints::ZERO,
                    total_copy_exposure_bps: pe_core_types::BasisPoints::ZERO,
                    intraday_pnl_bps: pe_core_types::BasisPoints::ZERO,
                    rolling_7d_pnl_bps: pe_core_types::BasisPoints::ZERO,
                    absolute_pnl_bps: pe_core_types::BasisPoints::ZERO,
                    copy_latency_kill_switch_active: false,
                    proposed_trade_bps: pe_core_types::BasisPoints::ZERO,
                    per_trade_cap_bps: 10_000,
                    concentration_caps: None,
                },
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 1,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
                worst_case_debit: principal,
                price_impact_cap_bps: 100,
                chase_ceiling: price,
                band_floor: Price::ZERO,
                band_ceiling_exclusive: Price::ONE,
            },
            applied_configuration_hash: "config".to_owned(),
        }
    }

    fn risk_frame(sequence: u64, record: PaperLogRecord) -> ScannedPaperFrame {
        let receipt = AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::hash(&sequence.to_be_bytes()),
        };
        let timestamp =
            OffsetDateTime::from_unix_timestamp(i64::try_from(sequence).unwrap()).unwrap();
        ScannedPaperFrame {
            envelope: pe_event_log::EventEnvelope {
                seq: receipt.sequence,
                source_id: SourceId("paper-test".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                raw_payload_hash: blake3::hash(b"payload"),
                prev_hash: blake3::hash(b"previous"),
                this_hash: receipt.this_hash,
                payload: Vec::new(),
            },
            receipt,
            frame: PaperLogFrame::Record(record),
            legacy_fill: None,
        }
    }

    fn test_receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::hash(&sequence.to_be_bytes()),
        }
    }

    fn test_frame(sequence: u64, unix: i64, record: PaperLogRecord) -> ScannedPaperFrame {
        let receipt = test_receipt(sequence);
        let timestamp = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        ScannedPaperFrame {
            envelope: EventEnvelope {
                seq: receipt.sequence,
                source_id: SourceId("paper-test".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                raw_payload_hash: blake3::hash(b"payload"),
                prev_hash: blake3::hash(b"prev"),
                this_hash: receipt.this_hash,
                payload: Vec::new(),
            },
            receipt,
            frame: PaperLogFrame::Record(record),
            legacy_fill: None,
        }
    }

    fn test_economic(
        condition: &str,
        source_receipt: AppendReceipt,
        complete_bound_receipt: AppendReceipt,
    ) -> EconomicPrepared {
        let price = Price::new(dec!(0.5)).unwrap();
        let shares = ShareAmount::from_whole(2).unwrap();
        let principal = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
        EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: PolymarketConditionId(condition.to_owned()),
                outcome_index: 0,
                token_id: PolymarketTokenId(format!("{condition}-token")),
                side: Side::Buy,
                market_id: condition.to_owned(),
            },
            admission: LiveAdmissionArtifactAudit {
                market: LiveMarketEvidenceAudit {
                    condition_id: PolymarketConditionId(condition.to_owned()),
                    ordered_outcome_token_ids: [
                        PolymarketTokenId(format!("{condition}-token")),
                        PolymarketTokenId(format!("{condition}-other")),
                    ],
                    neg_risk: false,
                    minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: shares,
                    observed_at_unix: 1,
                    schema_version: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                settlement: VenueSettlementRecord {
                    schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                    condition_id: PolymarketConditionId(condition.to_owned()),
                    status: VenueResolutionStatus::Unresolved,
                    raw_evidence_hash: "settlement".to_owned(),
                    source_timestamp_unix: Some(1),
                    observed_at_unix: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                fee_schedule: CompactFeeSchedule::Zero,
                scheduled_end_unix: Some(100),
                receipts: AdmissionReceipts {
                    gamma: test_receipt(1_001),
                    clob_long: test_receipt(1_002),
                    clob_compact: test_receipt(1_003),
                },
            },
            ladder: LadderPlanAudit {
                used_asks: vec![LadderAskAudit { price, shares }],
                best_ask: price,
                limit_price: price,
                minimum_shares: shares,
                principal,
            },
            book_receipt: test_receipt(1_004),
            observation: Some(ObservationEvidence {
                source_receipt,
                complete_bound_receipt,
                observed_unix_ms: 1_000,
                provenance: "activity_ws".to_owned(),
            }),
            sizing: SizingAudit {
                mode: SizingModeAudit::Kelly {
                    fraction: KellyFraction::new(dec!(0.25)).unwrap(),
                    probability: Probability::new(dec!(0.6)).unwrap(),
                },
                budget: principal,
                principal,
                minimum_shares: shares,
                expected_shares: shares,
                expected_vwap: price,
                all_in_price: price,
                slippage_rate: Decimal::ZERO,
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
                    proposed_trade_bps: BasisPoints(10),
                    per_trade_cap_bps: 25,
                    concentration_caps: None,
                },
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 1_000,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
                worst_case_debit: principal,
                price_impact_cap_bps: 100,
                chase_ceiling: price,
                band_floor: Price::ZERO,
                band_ceiling_exclusive: Price::ONE,
            },
            applied_configuration_hash: "config".to_owned(),
        }
    }

    #[test]
    fn quiet_days_are_samples_and_initial_partial_day_is_excluded() {
        let marks = vec![
            QualificationMarkReport {
                cutoff_unix: 86_400,
                cash: dec!(100),
                equity: dec!(100),
            },
            QualificationMarkReport {
                cutoff_unix: 172_800,
                cash: dec!(100),
                equity: dec!(100),
            },
            QualificationMarkReport {
                cutoff_unix: 259_200,
                cash: dec!(101),
                equity: dec!(101),
            },
        ];
        let samples = complete_day_growth(&marks).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], Decimal::ZERO);
        assert!(samples[1] > Decimal::ZERO);
    }

    #[test]
    fn missing_daily_boundary_is_insufficient() {
        let marks = vec![
            QualificationMarkReport {
                cutoff_unix: 86_400,
                cash: dec!(100),
                equity: dec!(100),
            },
            QualificationMarkReport {
                cutoff_unix: 259_200,
                cash: dec!(100),
                equity: dec!(100),
            },
        ];
        assert!(matches!(
            complete_day_growth(&marks),
            Err(QualificationError::InsufficientEvidence(_))
        ));
    }

    #[test]
    fn delay_nearest_rank_edges_are_deterministic() {
        assert_eq!(nearest_rank_p95(&[]), None);
        assert_eq!(nearest_rank_p95(&[2_000]), Some(2_000));
        let samples = (1..=20).collect::<Vec<_>>();
        assert_eq!(nearest_rank_p95(&samples), Some(19));
    }

    #[test]
    fn only_semantic_hash_drift_seals_insufficient() {
        let start = started("same");
        assert_eq!(seal_if_semantic_drift(&start, "same"), None);
        assert!(matches!(
            seal_if_semantic_drift(&start, "different"),
            Some(SealReason::InsufficientEvidence(_))
        ));
    }

    #[test]
    fn drawdown_equality_fails_the_strict_gate() {
        let threshold = QualificationThresholds::canonical().maximum_drawdown_fraction_exclusive;
        assert_eq!(threshold, dec!(0.1));
        let passes = |drawdown: Decimal| drawdown < threshold;
        assert!(!passes(threshold));
    }

    #[test]
    fn growth_drawdown_and_all_verdict_edges_are_exact() {
        let marks = vec![
            QualificationMarkReport {
                cutoff_unix: 86_400,
                cash: dec!(100),
                equity: dec!(100),
            },
            QualificationMarkReport {
                cutoff_unix: 172_800,
                cash: dec!(110),
                equity: dec!(110),
            },
            QualificationMarkReport {
                cutoff_unix: 259_200,
                cash: dec!(110),
                equity: dec!(110),
            },
            QualificationMarkReport {
                cutoff_unix: 345_600,
                cash: dec!(99),
                equity: dec!(99),
            },
        ];
        let growth = complete_day_growth(&marks).unwrap();
        assert!(growth[0] > Decimal::ZERO);
        assert_eq!(growth[1], Decimal::ZERO);
        assert!(growth[2] < Decimal::ZERO);
        assert!(
            complete_day_growth(&[
                marks[0].clone(),
                QualificationMarkReport {
                    cutoff_unix: 172_800,
                    cash: Decimal::ZERO,
                    equity: Decimal::ZERO,
                },
            ])
            .is_err()
        );

        assert_eq!(
            pe_risk_engine::max_drawdown_fraction(&[dec!(100), dec!(90.000001)]).unwrap(),
            dec!(0.09999999)
        );
        assert_eq!(
            pe_risk_engine::max_drawdown_fraction(&[dec!(100), dec!(90)]).unwrap(),
            dec!(0.1)
        );
        assert_eq!(
            pe_risk_engine::max_drawdown_fraction(&[dec!(100), dec!(89.999999)]).unwrap(),
            dec!(0.10000001)
        );

        assert_eq!(
            qualification_gate_verdict(Vec::new()).0,
            QualificationVerdict::Pass
        );
        assert_eq!(
            qualification_gate_verdict(vec!["threshold".to_owned()]).0,
            QualificationVerdict::Fail
        );
        assert_eq!(
            QualificationReport::insufficient("aa", "evidence".to_owned()).verdict,
            QualificationVerdict::InsufficientEvidence
        );
    }

    #[test]
    fn only_qualification_demotions_reset_the_anchor() {
        for reason in [
            MembershipReason::KnockoutInactivity,
            MembershipReason::KnockoutInactivityHardCap,
            MembershipReason::KnockoutUnderperformance,
        ] {
            assert!(reason_moves_anchor(reason));
        }
        for reason in [
            MembershipReason::FullRerank,
            MembershipReason::RankerRotation,
            MembershipReason::CapacityChange,
            MembershipReason::Initial,
        ] {
            assert!(!reason_moves_anchor(reason));
        }
    }

    /// PASS: a pre-cutoff websocket fill remains causal when its complete page and Final arrive
    /// after the boundary, while a resolution first received after the cutoff leaves it open.
    #[test]
    fn causal_mark_uses_observation_membership_and_leaves_post_cutoff_resolution_open() {
        let cutoff = 172_800;
        let receipt_before = AppendReceipt {
            sequence: EventSeq(1),
            this_hash: blake3::hash(b"before"),
        };
        let late_complete = AppendReceipt {
            sequence: EventSeq(3),
            this_hash: blake3::hash(b"late-complete"),
        };
        let late_resolution = AppendReceipt {
            sequence: EventSeq(4),
            this_hash: blake3::hash(b"late-resolution"),
        };
        let timestamp = |unix| OffsetDateTime::from_unix_timestamp(unix).unwrap();
        let source = BTreeMap::from([
            (
                1,
                SourceObservation {
                    receipt: receipt_before,
                    observed_at: SourceTimestamp(timestamp(cutoff - 1)),
                    received_at: ReceivedAt(timestamp(cutoff - 1)),
                    received_unix_ms: (cutoff - 1) * 1_000,
                    source_id: "activity_ws".to_owned(),
                    schema_version: 1,
                    parser_version: 1,
                    content_type: ContentType::Json,
                    payload: Vec::new(),
                },
            ),
            (
                3,
                SourceObservation {
                    receipt: late_complete,
                    observed_at: SourceTimestamp(timestamp(cutoff + 1)),
                    received_at: ReceivedAt(timestamp(cutoff + 1)),
                    received_unix_ms: (cutoff + 1) * 1_000,
                    source_id: "activity_page".to_owned(),
                    schema_version: 1,
                    parser_version: 1,
                    content_type: ContentType::Json,
                    payload: Vec::new(),
                },
            ),
            (
                4,
                SourceObservation {
                    receipt: late_resolution,
                    observed_at: SourceTimestamp(timestamp(cutoff + 1)),
                    received_at: ReceivedAt(timestamp(cutoff + 1)),
                    received_unix_ms: (cutoff + 1) * 1_000,
                    source_id: "resolution".to_owned(),
                    schema_version: 1,
                    parser_version: 1,
                    content_type: ContentType::Json,
                    payload: Vec::new(),
                },
            ),
        ]);
        let condition = "condition-open";
        let fill = CompletedFinancialFact {
            prepared_receipt: test_receipt(10),
            final_receipt: test_receipt(11),
            payload: FinancialPayload::Fill {
                operation: crate::paper_recovery::PaperFillOperationIdentity {
                    leader_wallet: started("hot").membership[0],
                    source_trade_id: SourceTradeId(format!("g2:{}", "a".repeat(64))),
                    observed_at_bucket: cutoff - 1,
                },
                economic: test_economic(condition, receipt_before, late_complete),
            },
            result: FinancialResult::Fill {
                canonical: crate::paper_recovery::CanonicalFillResult {
                    outcome: "applied".to_owned(),
                    bankroll: dec!(99),
                    applied_prepared_seq: EventSeq(10),
                    quantity: ShareAmount::from_whole(2).unwrap(),
                    principal: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                    fee: CollateralAmount::ZERO,
                    fill_price: Price::new(dec!(0.5)).unwrap(),
                },
            },
        };
        let resolution = CompletedFinancialFact {
            prepared_receipt: test_receipt(12),
            final_receipt: test_receipt(13),
            payload: FinancialPayload::Resolution {
                condition_id: PolymarketConditionId(condition.to_owned()),
                payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                resolution_source_receipt: late_resolution,
            },
            result: FinancialResult::Resolution {
                canonical: crate::paper_recovery::CanonicalResolutionResult {
                    outcome: "applied".to_owned(),
                    bankroll: dec!(101),
                    applied_prepared_seq: EventSeq(12),
                    credit: CollateralAmount::from_decimal_exact(dec!(2)).unwrap(),
                    settled_at_unix: cutoff + 1,
                },
            },
        };
        let mark = PortfolioMark {
            boundary_receipt: AppendReceipt {
                sequence: EventSeq(2),
                this_hash: blake3::hash(b"boundary"),
            },
            cutoff_unix: cutoff,
            source_tail: TailBinding {
                physical_tail: 0,
                last_sequence: Some(EventSeq(3)),
                last_hash: "0".repeat(64),
            },
            financial_prefix_seq: Some(EventSeq(10)),
            prices: Vec::new(),
            cash: dec!(99),
            equity: dec!(100),
            invalid: None,
        };
        let replayed =
            causal_financial_state(dec!(100), &[fill, resolution], &mark, &source).unwrap();
        assert_eq!(replayed.last_completed, Some(EventSeq(10)));
        assert_eq!(replayed.cash, dec!(99));
        assert_eq!(replayed.positions.len(), 1);
        assert_eq!(replayed.positions[0].condition_id, condition);
        assert_eq!(replayed.completed_prepared, HashSet::from([EventSeq(10)]));
        assert!(replayed.closed_fill_final_conditions.is_empty());
    }

    /// PASS: boundary C supplies day 30 but a ninetieth Resolution Final excluded from C's causal
    /// financial state does not complete the close threshold.
    #[test]
    fn post_cutoff_resolution_does_not_complete_qualification() {
        let start = started("hot");
        let start_receipt = test_receipt(1);
        let mut frames = vec![test_frame(
            1,
            1,
            PaperLogRecord::QualificationStarted(Box::new(start.clone())),
        )];
        let mut sequence = 2u64;
        let push_mark = |frames: &mut Vec<ScannedPaperFrame>, sequence: u64, cutoff_unix: i64| {
            frames.push(test_frame(
                sequence,
                cutoff_unix,
                PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                    boundary_receipt: test_receipt(10_000 + sequence),
                    cutoff_unix,
                    source_tail: TailBinding {
                        physical_tail: 0,
                        last_sequence: Some(EventSeq(10_000 + sequence)),
                        last_hash: test_receipt(10_000 + sequence)
                            .this_hash
                            .to_hex()
                            .to_string(),
                    },
                    financial_prefix_seq: None,
                    prices: Vec::new(),
                    cash: dec!(100),
                    equity: dec!(100),
                    invalid: None,
                })),
            ));
        };
        push_mark(&mut frames, sequence, 86_400);
        sequence += 1;

        let mut causal_prepared = HashSet::new();
        for ordinal in 0..90u64 {
            let condition = format!("condition-{ordinal}");
            let source = test_receipt(20_000 + ordinal * 2);
            let complete = test_receipt(20_001 + ordinal * 2);
            let fill_prepared = test_receipt(sequence);
            frames.push(test_frame(
                sequence,
                100,
                PaperLogRecord::FinancialPrepared {
                    expected_authority: crate::paper_recovery::ExpectedAuthority {
                        qualification_start_receipt: start_receipt,
                        prior_completed_prepared_sequence: None,
                    },
                    payload: FinancialPayload::Fill {
                        operation: crate::paper_recovery::PaperFillOperationIdentity {
                            leader_wallet: start.membership[0],
                            source_trade_id: SourceTradeId(format!("g2:{:064x}", ordinal + 1)),
                            observed_at_bucket: 100,
                        },
                        economic: test_economic(&condition, source, complete),
                    },
                },
            ));
            causal_prepared.insert(fill_prepared.sequence);
            sequence += 1;
            frames.push(test_frame(
                sequence,
                100,
                PaperLogRecord::FinancialFinal {
                    prepared_receipt: fill_prepared,
                    result: FinancialResult::Fill {
                        canonical: crate::paper_recovery::CanonicalFillResult {
                            outcome: "applied".to_owned(),
                            bankroll: dec!(99),
                            applied_prepared_seq: fill_prepared.sequence,
                            quantity: ShareAmount::from_whole(2).unwrap(),
                            principal: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                            fee: CollateralAmount::ZERO,
                            fill_price: Price::new(dec!(0.5)).unwrap(),
                        },
                    },
                },
            ));
            sequence += 1;
            if ordinal < 89 {
                let resolution_prepared = test_receipt(sequence);
                frames.push(test_frame(
                    sequence,
                    101,
                    PaperLogRecord::FinancialPrepared {
                        expected_authority: crate::paper_recovery::ExpectedAuthority {
                            qualification_start_receipt: start_receipt,
                            prior_completed_prepared_sequence: Some(fill_prepared.sequence),
                        },
                        payload: FinancialPayload::Resolution {
                            condition_id: PolymarketConditionId(condition),
                            payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                            resolution_source_receipt: test_receipt(30_000 + ordinal),
                        },
                    },
                ));
                causal_prepared.insert(resolution_prepared.sequence);
                sequence += 1;
                frames.push(test_frame(
                    sequence,
                    101,
                    PaperLogRecord::FinancialFinal {
                        prepared_receipt: resolution_prepared,
                        result: FinancialResult::Resolution {
                            canonical: crate::paper_recovery::CanonicalResolutionResult {
                                outcome: "applied".to_owned(),
                                bankroll: dec!(101),
                                applied_prepared_seq: resolution_prepared.sequence,
                                credit: CollateralAmount::from_decimal_exact(dec!(2)).unwrap(),
                                settled_at_unix: 101,
                            },
                        },
                    },
                ));
                sequence += 1;
            }
        }

        for day in 2..=30i64 {
            push_mark(&mut frames, sequence, day * 86_400);
            sequence += 1;
        }
        let noncausal_resolution = test_receipt(sequence);
        frames.push(test_frame(
            sequence,
            31 * 86_400 + 1,
            PaperLogRecord::FinancialPrepared {
                expected_authority: crate::paper_recovery::ExpectedAuthority {
                    qualification_start_receipt: start_receipt,
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Resolution {
                    condition_id: PolymarketConditionId("condition-89".to_owned()),
                    payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                    resolution_source_receipt: test_receipt(40_000),
                },
            },
        ));
        sequence += 1;
        frames.push(test_frame(
            sequence,
            31 * 86_400 + 1,
            PaperLogRecord::FinancialFinal {
                prepared_receipt: noncausal_resolution,
                result: FinancialResult::Resolution {
                    canonical: crate::paper_recovery::CanonicalResolutionResult {
                        outcome: "applied".to_owned(),
                        bankroll: dec!(101),
                        applied_prepared_seq: noncausal_resolution.sequence,
                        credit: CollateralAmount::from_decimal_exact(dec!(2)).unwrap(),
                        settled_at_unix: 31 * 86_400 + 1,
                    },
                },
            },
        ));
        sequence += 1;
        push_mark(&mut frames, sequence, 31 * 86_400);
        let era = PaperEra {
            start: Some((start_receipt, start)),
            frames,
        };

        let completion = qualification_completion_for_causal_facts(&era, &causal_prepared)
            .expect("causal completion");
        assert_eq!(completion.complete_days, 30);
        assert_eq!(completion.causal_closes, 89);
        assert!(!completion.is_complete());
    }

    #[test]
    fn financial_target_config_requires_offline_authority_inputs() {
        let mut config = ServiceConfig::default();
        assert!(validate_financial_target_config(&config).is_err());
        config.supabase_authoritative = true;
        config.supabase_url = "https://example.invalid".to_owned();
        config.supabase_secret_key = "service-role".to_owned();
        assert!(validate_financial_target_config(&config).is_ok());
    }

    #[test]
    fn report_bytes_are_compact_deterministic_and_newline_terminated() {
        let report = QualificationReport::insufficient("aa", "missing".to_owned());
        let mut first = serde_json::to_vec(&report).unwrap();
        first.push(b'\n');
        let mut second = serde_json::to_vec(&report).unwrap();
        second.push(b'\n');
        assert_eq!(first, second);
        assert_eq!(first.last(), Some(&b'\n'));
        assert!(!first.contains(&b'\r'));
    }

    #[test]
    fn empty_source_prefix_exposes_no_later_frames() {
        let temp = tempfile::tempdir().unwrap();
        let source_log = temp.path().join("source.log");
        let mut writer = Writer::open(&source_log).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("test.source".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
                content_type: ContentType::Json,
                payload: br#"{"value":1}"#.to_vec(),
            })
            .unwrap();
        let prefix = TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };

        assert!(
            source_observations(&source_log, &prefix)
                .unwrap()
                .is_empty()
        );
    }

    /// PASS: runtime and qualification adapters produce byte-for-byte equal base snapshots from
    /// the same financial-era and proposal fixture because both call the shared reducer.
    #[test]
    fn runtime_and_qualification_risk_bases_are_equal() {
        let start = started("hot");
        let start_receipt = AppendReceipt {
            sequence: EventSeq(0),
            this_hash: blake3::hash(b"start"),
        };
        let leader = start.membership[0];
        let prepared_receipt = |sequence| AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::hash(&sequence.to_be_bytes()),
        };
        let expected = |prior| crate::paper_recovery::ExpectedAuthority {
            qualification_start_receipt: start_receipt,
            prior_completed_prepared_sequence: prior,
        };
        let fill =
            |source_trade_id: &str, market_id: &str, principal| PaperLogRecord::FinancialPrepared {
                expected_authority: expected(None),
                payload: FinancialPayload::Fill {
                    operation: crate::paper_recovery::PaperFillOperationIdentity {
                        leader_wallet: leader,
                        source_trade_id: SourceTradeId(source_trade_id.to_owned()),
                        observed_at_bucket: 1,
                    },
                    economic: risk_economic(market_id, principal),
                },
            };
        let fill_final = |sequence, principal| PaperLogRecord::FinancialFinal {
            prepared_receipt: prepared_receipt(sequence),
            result: FinancialResult::Fill {
                canonical: crate::paper_recovery::CanonicalFillResult {
                    outcome: "applied".to_owned(),
                    bankroll: dec!(99),
                    applied_prepared_seq: EventSeq(sequence),
                    quantity: ShareAmount::from_whole(2).unwrap(),
                    principal,
                    fee: CollateralAmount::ZERO,
                    fill_price: Price::new(dec!(0.5)).unwrap(),
                },
            },
        };
        let one = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
        let four = CollateralAmount::from_decimal_exact(dec!(4)).unwrap();
        let era = crate::paper_recovery::PaperEra {
            start: Some((start_receipt, start.clone())),
            frames: vec![
                risk_frame(1, fill("unresolved", "market-a", one)),
                risk_frame(2, fill_final(1, one)),
                risk_frame(3, fill("resolved", "market-resolved", four)),
                risk_frame(4, fill_final(3, four)),
                risk_frame(
                    5,
                    PaperLogRecord::FinancialPrepared {
                        expected_authority: expected(Some(EventSeq(3))),
                        payload: FinancialPayload::Resolution {
                            condition_id: PolymarketConditionId("market-resolved".to_owned()),
                            payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                            resolution_source_receipt: prepared_receipt(20),
                        },
                    },
                ),
                risk_frame(
                    6,
                    PaperLogRecord::FinancialFinal {
                        prepared_receipt: prepared_receipt(5),
                        result: FinancialResult::Resolution {
                            canonical: crate::paper_recovery::CanonicalResolutionResult {
                                outcome: "applied".to_owned(),
                                bankroll: dec!(103),
                                applied_prepared_seq: EventSeq(5),
                                credit: CollateralAmount::from_decimal_exact(dec!(8)).unwrap(),
                                settled_at_unix: 6,
                            },
                        },
                    },
                ),
            ],
        };
        let proposed_debit = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();

        let runtime =
            build_paper_risk_base(&era, leader, "market-a", proposed_debit, 10_000).unwrap();
        let qualification =
            replayed_risk_base(leader, "market-a", proposed_debit, 10_000, &era).unwrap();

        assert_eq!(runtime, qualification);
        assert_eq!(runtime.proposed_trade_bps, pe_core_types::BasisPoints(100));
        assert_eq!(runtime.leader_exposure_bps, pe_core_types::BasisPoints(100));
        assert_eq!(runtime.market_exposure_bps, pe_core_types::BasisPoints(100));
        assert_eq!(runtime.family_exposure_bps, pe_core_types::BasisPoints(100));
        assert_eq!(
            runtime.total_copy_exposure_bps,
            pe_core_types::BasisPoints(100)
        );
    }

    /// PASS: after sealing, a terminal row with an old source epoch but version-three receipts
    /// after the sealed source prefix leaves the verifier's complete report unchanged.
    /// FAIL: mutable SQLite time selection admits the late row or changes the sealed digest.
    #[tokio::test]
    async fn post_seal_decision_after_source_prefix_does_not_change_report() {
        let temp = tempfile::tempdir().unwrap();
        let paper_log = temp.path().join("paper.log");
        let source_log = temp.path().join("source.log");
        let paper_state = temp.path().join("paper.db");
        let output = temp.path().join("qualification.json");
        let mut paper_writer = Writer::open(&paper_log).unwrap();
        let mut source_writer = Writer::open(&source_log).unwrap();
        let empty_paper_prefix = Scanner::verify(&paper_log).unwrap();
        let empty_source_prefix = Scanner::verify(&source_log).unwrap();
        let cutoff_unix = 86_400;
        let start_unix = cutoff_unix - 100;
        let mut start = started("hot");
        start.paper_prefix = TailBinding::from(&empty_paper_prefix);
        start.source_prefix = TailBinding::from(&empty_source_prefix);
        start.live_prefix = TailBinding::from(&empty_source_prefix);
        let start_receipt = paper_writer
            .append_synced(start_envelope(&start, start_unix).unwrap())
            .unwrap();

        let boundary_at = OffsetDateTime::from_unix_timestamp(cutoff_unix).unwrap();
        let boundary_receipt = source_writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.boundary".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(boundary_at),
                received_at: ReceivedAt(boundary_at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&serde_json::json!({
                    "kind": "daily_boundary",
                    "cutoff_unix": cutoff_unix,
                }))
                .unwrap(),
            })
            .unwrap();
        let sealed_source_tail = Scanner::verify(&source_log).unwrap();
        append_paper_record_at(
            &mut paper_writer,
            &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt,
                cutoff_unix,
                source_tail: TailBinding::from(&sealed_source_tail),
                financial_prefix_seq: None,
                prices: Vec::new(),
                cash: dec!(100),
                equity: dec!(100),
                invalid: None,
            })),
            cutoff_unix,
        );
        let sealed_financial_tail = Scanner::verify(&paper_log).unwrap();
        let state = PaperStateDb::open(&paper_state).unwrap();
        let decision_evidence = state.seal_decision_evidence(&[]).unwrap();
        drop(state);
        let seal_receipt = append_paper_record_at(
            &mut paper_writer,
            &PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
                start_receipt,
                source_prefix: TailBinding::from(&sealed_source_tail),
                financial_prefix: TailBinding::from(&sealed_financial_tail),
                decision_evidence_digest: blake3::hash(&decision_evidence).to_hex().to_string(),
                sealed_cutoff_unix: cutoff_unix,
                reason: SealReason::Complete,
            })),
            cutoff_unix + 1,
        );
        let options = QualifyOptions {
            paper_log: paper_log.clone(),
            source_log: source_log.clone(),
            paper_state: paper_state.clone(),
            seal_hash: seal_receipt.this_hash.to_hex().to_string(),
            output,
        };
        let before = verify_qualification(&options).await.unwrap();

        let late_payload = br#"[]"#.to_vec();
        let late_at = OffsetDateTime::from_unix_timestamp(cutoff_unix + 2).unwrap();
        let late_receipt = source_writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(late_at),
                received_at: ReceivedAt(late_at),
                content_type: ContentType::Json,
                payload: late_payload.clone(),
            })
            .unwrap();
        let applied_configuration =
            crate::runtime_config::RuntimeConfig::from_service_config(&ServiceConfig::default());
        let source_trade_id = pe_core_types::SourceTradeId("g2:post-seal".to_owned());
        let continuation = crate::bucket_commit::DecisionContinuationV2 {
            version: 3,
            source_trade_id: source_trade_id.clone(),
            semantic_revision: "semantic-v3".to_owned(),
            transaction_hash: "0xpost-seal".to_owned(),
            wallet: start.membership[0],
            source_epoch: cutoff_unix - 1,
            market_id: MarketId(VenueMarketId("market-post-seal".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            share_amount: ShareAmount::from_whole(1).unwrap(),
            provenance: pe_copy_signal_engine::TradeProvenance::RestPoll,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
            gate_result: "admitted".to_owned(),
            applied_configuration_hash: applied_configuration.canonical_hash(),
            applied_configuration,
            frozen_basis: crate::bucket_commit::FrozenDecisionBasis {
                win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                bankroll: dec!(100),
            },
            decision_inputs: serde_json::json!({"post_seal": true}),
        };
        let terminal = crate::decision_replay::TerminalDispositionEvidence::no_copy("post_seal");
        let terminal_disposition = terminal.disposition.clone();
        let post_commit_inputs_json =
            crate::decision_replay::DecisionEvidenceAccumulator::new(&continuation)
                .render(
                    crate::decision_replay::AuthorityEvidence::not_read("post_seal"),
                    terminal,
                )
                .unwrap();
        let frozen_inputs_json =
            serde_json::to_string(&crate::bucket_commit::DecisionContinuationV3 {
                prior: continuation,
                observed_source_receipt: None,
                page_occurrences: vec![crate::bucket_commit::PageOccurrence {
                    request_url: "https://example.invalid/activity?end=86400".to_owned(),
                    raw_hash: blake3::hash(&late_payload).to_hex().to_string(),
                    receipt: late_receipt,
                }],
            })
            .unwrap();
        let connection = rusqlite::Connection::open(&paper_state).unwrap();
        connection
            .execute(
                "INSERT INTO decision_pending
                    (source_trade_id, semantic_revision, wallet_hex, source_epoch,
                     frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition,
                     updated_at_unix)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'terminal', ?7, ?8)",
                rusqlite::params![
                    source_trade_id.0,
                    "semantic-v3",
                    start.membership[0].to_string(),
                    cutoff_unix - 1,
                    frozen_inputs_json,
                    post_commit_inputs_json,
                    terminal_disposition,
                    cutoff_unix + 2,
                ],
            )
            .unwrap();

        let after = verify_qualification(&options).await.unwrap();
        assert_eq!(before, after);
        assert_eq!(after.replay.decisions, 0);
    }

    #[tokio::test]
    async fn risk_prices_require_exact_causal_gamma_receipts() {
        let receipt = AppendReceipt {
            sequence: EventSeq(3),
            this_hash: blake3::hash(b"gamma-risk-price"),
        };
        let market = MarketId(VenueMarketId("condition-a".to_owned()));
        let position = PaperPositionRow {
            market_id: market.clone(),
            outcome_id: OutcomeId(1),
            long: ShareAmount::from_atomic(1_000_000),
            short: ShareAmount::ZERO,
        };
        let observation = SourceObservation {
            receipt,
            observed_at: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            ),
            received_at: ReceivedAt(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
            received_unix_ms: 1_700_000_000_000,
            source_id: GAMMA_MARKETS_SOURCE_ID.to_owned(),
            schema_version: GAMMA_MARKETS_SCHEMA_VERSION,
            parser_version: GAMMA_MARKETS_PARSER_VERSION,
            content_type: ContentType::Json,
            payload: br#"[{"conditionId":"condition-a","outcomePrices":"[\"0.4\",\"0.6\"]"}]"#
                .to_vec(),
        };
        let source = BTreeMap::from([(receipt.sequence.0, observation.clone())]);
        let prices = replayed_risk_prices(
            &[receipt],
            observation.received_unix_ms + 59_999,
            std::slice::from_ref(&position),
            &source,
        )
        .await
        .unwrap();
        assert_eq!(
            prices.get(&(market, OutcomeId(1))),
            Some(&Price::new(dec!(0.6)).unwrap())
        );

        assert!(
            replayed_risk_prices(
                &[receipt],
                observation.received_unix_ms + 60_000,
                std::slice::from_ref(&position),
                &source,
            )
            .await
            .is_err()
        );

        let wrong_receipt = AppendReceipt {
            sequence: receipt.sequence,
            this_hash: blake3::hash(b"tampered"),
        };
        assert!(
            replayed_risk_prices(
                &[wrong_receipt],
                observation.received_unix_ms,
                &[position],
                &source,
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn financial_prepare_is_read_only_and_start_is_receipt_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let paper_log = temp.path().join("paper.log");
        let source_log = temp.path().join("source.log");
        let live_journal = temp.path().join("live_journal.log");
        let paper_state = temp.path().join("paper.db");
        let status_path = temp.path().join("status.json");
        drop(Writer::open(&paper_log).unwrap());
        drop(Writer::open(&source_log).unwrap());
        drop(pe_execution_core::LiveJournal::open(&live_journal).unwrap());
        drop(PaperStateDb::open(&paper_state).unwrap());
        fs::write(
            &status_path,
            br#"{"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"stale":false,"accounts":[]}}"#,
        )
        .unwrap();
        let mut config = ServiceConfig {
            event_log_path: paper_log.clone(),
            source_event_log_path: source_log.clone(),
            ..ServiceConfig::default()
        };
        config.paper_state_db_path = paper_state.clone();
        config.status_path = status_path;

        let paths = [&paper_log, &source_log, &live_journal, &paper_state];
        let before = paths
            .iter()
            .map(|path| fs::read(path).unwrap())
            .collect::<Vec<_>>();
        let mut manifest = FinancialEraManifest {
            kind: FINANCIAL_ERA_KIND.to_owned(),
            state: "prepared".to_owned(),
            activation_id: "act-545".to_owned(),
            generation: "g557".to_owned(),
            fresh_bankroll: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            target_revision: "1".repeat(40),
            artifact_blake3: "a".repeat(64),
            static_config_hash: "b".repeat(64),
            ranking_batch_id: 545,
            policy_hash: "d".repeat(64),
            membership: Vec::new(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
            start_unix: 1_700_000_000,
            paths: FinancialEraPaths {
                paper_log: paper_log.clone(),
                source_log: source_log.clone(),
                live_journal: live_journal.clone(),
                paper_state: paper_state.clone(),
            },
            old_artifact_sha256: "d".repeat(64),
            target_artifact_sha256: "e".repeat(64),
            old_config_sha256: "f".repeat(64),
            target_config_sha256: "0".repeat(64),
            old_environment_sha256: "1".repeat(64),
            target_environment_sha256: "2".repeat(64),
            preparation: None,
            stop_invoked: false,
            service_was_active: None,
            backup: None,
            remote_census: None,
            guarded_logs: None,
            start_receipt: None,
            started_unix: None,
        };
        let mut receipt_bearing_manifest = serde_json::to_value(&manifest).unwrap();
        receipt_bearing_manifest.as_object_mut().unwrap().insert(
            "remote_archive_completed".to_owned(),
            serde_json::Value::Bool(true),
        );
        assert!(
            serde_json::from_value::<FinancialEraManifest>(receipt_bearing_manifest).is_ok(),
            "driver-owned durable receipts must remain readable by offline commands"
        );
        let mut wrong_live_path = manifest.clone();
        wrong_live_path.paths.live_journal = temp.path().join("not-the-configured-journal.log");
        assert!(validate_financial_manifest(&wrong_live_path, &config).is_err());
        fs::write(
            &config.status_path,
            br#"{"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"stale":false,"accounts":[{"account_id":"live-a","requested_live_mode":"off","effective_live_mode":"off","armed":true}]}}"#,
        )
        .unwrap();
        assert!(
            verify_live_preparation_posture(
                &live_journal,
                &config.source_event_log_path,
                &config.status_path
            )
            .is_err()
        );
        fs::write(
            &config.status_path,
            br#"{"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"stale":false,"accounts":[]}}"#,
        )
        .unwrap();
        let config_rows = financial_config_rows();
        let preparation = prepare_financial_era(&manifest, &config, &config_rows).unwrap();
        assert_eq!(
            preparation.start.hot_config_hash,
            derive_hot_config_hash(&config_rows, &config).unwrap()
        );
        assert!(is_lower_hex_64(&preparation.start.membership_proofs_hash));
        let after = paths
            .iter()
            .map(|path| fs::read(path).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(before, after);

        manifest.state = "guarded".to_owned();
        manifest.preparation = Some(preparation.clone());
        let first = start_financial_era(&manifest, &config, &config_rows).unwrap();
        let second = start_financial_era(&manifest, &config, &config_rows).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            serde_json::from_str::<AppendReceipt>(&first).unwrap(),
            preparation.expected_receipt
        );
        assert_eq!(
            PaperStateDb::open(&paper_state)
                .unwrap()
                .bankroll()
                .unwrap(),
            Some(dec!(100))
        );
        assert_eq!(scan_paper_log(&paper_log).unwrap().len(), 1);
    }
}
