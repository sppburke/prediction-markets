//! Network-free financial-era preparation and sealed paper qualification (#545).
//!
//! This module deliberately owns no HTTP client. It consumes only verified framed logs and the
//! read-only paper-state projection, and emits canonical compact JSON with a trailing line feed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, OutcomeId, Price, ReceivedAt, ShareAmount, Side,
    SourceId, SourceTimestamp, TraderId, VenueMarketId,
};
use pe_event_log::envelope::{HashInput, compute_hashes};
use pe_event_log::{
    AppendReceipt, ContentType, EnvelopeIn, LogTailBinding, Reader, Scanner, Writer,
};
use pe_execution_core::{EconomicInputs, EconomicPrepared, LiveAdmissionArtifact};
use pe_paper_state::{
    DecisionPendingState, FinancialFillRow, FinancialPositionRow, FinancialSnapshot, PaperStateDb,
    SettledMarketRow,
};
use pe_risk_engine::{
    BinaryPayout, KILL_SWITCH_DRAWDOWN_BPS, RiskDecision, RiskHaltCause,
    aggregate_resolution_credit, evaluate_risk, nearest_rank_p95,
};
use pe_source_polymarket_public::{
    BinaryPayoutVector, GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION,
    GAMMA_MARKETS_SOURCE_ID, LiveMarketEvidence,
};
use pe_source_polymarket_public::{ClassifiedPricesHistory, PricePoint};
use pe_trader_index::score::lcb_5pct_decimal;
use pe_venue_polymarket::{AskLevel, LadderPlan};
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use time::OffsetDateTime;

use crate::config::ServiceConfig;
use crate::decision_replay::replay_decision_pending;
use crate::paper_recovery::{
    FinancialPayload, FinancialResult, MembershipReason, PAPER_LOG_SCHEMA_VERSION, PaperLogFrame,
    PaperLogRecord, PortfolioMark, QualificationSealed, QualificationStarted, RiskHaltOwner,
    ScannedPaperFrame, SealReason, TailBinding, active_risk_halts, paper_era, scan_paper_log,
};
use crate::risk_inputs::{build_paper_risk_snapshot, historical_mark_price};
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
pub fn run_qualify(
    options: &QualifyOptions,
) -> Result<(QualificationVerdict, String), QualificationError> {
    let report = match verify_qualification(options) {
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
    received_unix_ms: i64,
    source_id: String,
    schema_version: u32,
    parser_version: u32,
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
}

#[derive(Debug, Clone)]
struct OpenPosition {
    condition_id: String,
    market_id: String,
    outcome_index: u8,
    shares_atomic: u64,
}

struct RiskReplayContext<'a> {
    cash: Decimal,
    positions: &'a [OpenPosition],
    fills: &'a [FinancialFillRow],
    settlements: &'a [SettledMarketRow],
    last_completed: Option<EventSeq>,
    start_receipt: AppendReceipt,
    paper_prefix: &'a [ScannedPaperFrame],
    source: &'a BTreeMap<u64, SourceObservation>,
    source_log_path: &'a Path,
    prepared_received_unix_ms: i64,
}

fn verify_qualification(
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
    let start_unix = frames[start_index].envelope.received_at.0.unix_timestamp();
    let mut decisions = state
        .decision_pending_history()?
        .into_iter()
        .filter(|row| row.source_epoch >= start_unix && row.source_epoch <= seal.sealed_cutoff_unix)
        .collect::<Vec<_>>();
    decisions.sort_by(|left, right| {
        (left.source_epoch, &left.source_trade_id.0)
            .cmp(&(right.source_epoch, &right.source_trade_id.0))
    });
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
    let mut financial_fills = Vec::<FinancialFillRow>::new();
    let mut financial_settlements = Vec::<SettledMarketRow>::new();
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
                    FinancialPayload::Fill { economic, .. } => {
                        verify_economic(
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
                        )?;
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
                        financial_fills.push(FinancialFillRow {
                            idempotency_key,
                            market_id: MarketId(VenueMarketId(economic.market.market_id.clone())),
                            outcome_id: OutcomeId(u16::from(economic.market.outcome_index)),
                            side: economic.market.side,
                            quantity: canonical.quantity,
                            fill_price: canonical.fill_price,
                            principal: canonical.principal,
                            fee: canonical.fee,
                            prepared_seq: prepared_receipt.sequence,
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
                financial_final_count = financial_final_count.saturating_add(1);
                replayed_financial_prefix = Some(prepared_receipt.sequence);
            }
            PaperLogRecord::MembershipChanged {
                reason,
                removed,
                added,
                capacity,
                ..
            } => {
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
                let report = verify_mark(
                    mark,
                    cash,
                    &open_positions,
                    replayed_financial_prefix,
                    &source_observations,
                )?;
                valid_marks.push((frame.receipt.sequence.0, report.clone()));
                if waiting_for_anchor_mark {
                    anchor_mark_sequence = Some(frame.receipt.sequence.0);
                    anchor_mark_cutoff = Some(report.cutoff_unix);
                    waiting_for_anchor_mark = false;
                }
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

    bind_final_receipts(&replayed_decisions, &completed_fills)?;
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

    let resolution_sequences = resolution_sequences(&frames, start_index, financial_prefix_index);
    let mut closed = completed_fills
        .iter()
        .filter(|fill| fill.final_receipt.sequence.0 > anchor_sequence)
        .filter(|fill| {
            resolution_sequences
                .get(&fill.condition_id)
                .is_some_and(|sequences| {
                    sequences
                        .iter()
                        .any(|sequence| *sequence > fill.final_receipt.sequence.0)
                })
        })
        .collect::<Vec<_>>();
    closed.sort_by_key(|fill| fill.final_receipt.sequence.0);
    let completion =
        qualification_completion(&paper_era(frames[..=financial_prefix_index].to_vec()))
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

    // The current frozen service interfaces do not yet expose lane A/D's source-prefix
    // classification replay (including continuation-v3 page occurrences) or lane C's typed
    // membership-evidence verifier. Never turn structural/accounting agreement alone into a Pass.
    let exact_replay_gap = "source classification and typed membership evidence cannot yet be re-executed from the frozen interfaces";
    let (verdict, mut reasons) = match &seal.reason {
        SealReason::InsufficientEvidence(reason) => (
            QualificationVerdict::InsufficientEvidence,
            vec![format!("seal result: {reason}")],
        ),
        SealReason::Complete => {
            let mut reasons = vec![exact_replay_gap.to_owned()];
            reasons.extend(failures);
            (QualificationVerdict::InsufficientEvidence, reasons)
        }
    };
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
            exact: false,
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
                received_unix_ms,
                source_id: envelope.source_id.0,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
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

fn received_unix_ms(envelope: &pe_event_log::EventEnvelope) -> Result<i64, QualificationError> {
    let millis = envelope.received_at.0.unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).map_err(|_| {
        QualificationError::InsufficientEvidence("event timestamp milliseconds overflow".to_owned())
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordedGammaRiskMarket {
    condition_id: String,
    outcome_prices: Option<String>,
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
        .map(|position| FinancialPositionRow {
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

fn replayed_risk_prices(
    price_receipts: &[AppendReceipt],
    evaluated_at_unix_ms: i64,
    positions: &[FinancialPositionRow],
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

        let rows: Vec<RecordedGammaRiskMarket> = serde_json::from_slice(&observation.payload)
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "risk Gamma response is invalid: {error}"
                ))
            })?;
        let mut receipt_used = false;
        for row in rows {
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
            let encoded = row.outcome_prices.ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "risk Gamma evidence omits outcomePrices".to_owned(),
                )
            })?;
            let raw_prices: Vec<String> = serde_json::from_str(&encoded).map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "risk Gamma outcomePrices is invalid: {error}"
                ))
            })?;
            for outcome in outcomes {
                let raw = raw_prices.get(usize::from(outcome.0)).ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "risk Gamma evidence omits an open outcome".to_owned(),
                    )
                })?;
                let decimal = Decimal::from_str_exact(raw)
                    .or_else(|_| Decimal::from_scientific(raw))
                    .map_err(|error| {
                        QualificationError::InsufficientEvidence(format!(
                            "risk Gamma price is invalid: {error}"
                        ))
                    })?;
                let price = Price::new(decimal).map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "risk Gamma price is out of range: {error}"
                    ))
                })?;
                if prices.insert((market_id.clone(), outcome), price).is_some() {
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

fn verify_economic(
    economic: &EconomicPrepared,
    context: &RiskReplayContext<'_>,
) -> Result<(), QualificationError> {
    let cash_before = CollateralAmount::from_decimal_exact(context.cash).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "replayed cash cannot be represented exactly: {error}"
        ))
    })?;
    let admission = LiveAdmissionArtifact {
        market: LiveMarketEvidence {
            condition_id: economic.admission.market.condition_id.clone(),
            ordered_outcome_token_ids: economic.admission.market.ordered_outcome_token_ids.clone(),
            neg_risk: economic.admission.market.neg_risk,
            minimum_tick_size: economic.admission.market.minimum_tick_size,
            minimum_order_size: economic.admission.market.minimum_order_size,
            scheduled_end_unix: economic.admission.scheduled_end_unix,
            observed_at_unix: economic.admission.market.observed_at_unix,
            schema_version: economic.admission.market.schema_version,
            parser_version: economic.admission.market.parser_version,
            freshness_window_secs: economic.admission.market.freshness_window_secs,
        },
        settlement: economic.admission.settlement.clone(),
        fee_schedule: economic.admission.fee_schedule,
        receipts: economic.admission.receipts,
    };
    let plan = LadderPlan {
        used_asks: economic
            .ladder
            .used_asks
            .iter()
            .map(|ask| AskLevel {
                price: ask.price,
                shares: ask.shares,
            })
            .collect(),
        best_ask: economic.ladder.best_ask,
        limit_price: economic.ladder.limit_price,
        shares: economic.ladder.minimum_shares,
        worst_case_debit: economic.ladder.principal,
    };
    let recomposed = EconomicPrepared::compose(EconomicInputs {
        market: economic.market.clone(),
        admission: &admission,
        plan: &plan,
        book_receipt: economic.book_receipt,
        observation: economic.observation.clone(),
        sizing_mode: economic.sizing.mode,
        budget: economic.sizing.budget,
        slippage_rate: economic.sizing.slippage_rate,
        risk: economic.risk.clone(),
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
    if recomposed_hash != recorded_hash {
        return insufficient("EconomicPrepared differs from canonical composition");
    }

    let worst_case_debit = recomposed.worst_case_all_in_debit().map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "economic worst-case debit failed: {error}"
        ))
    })?;
    if cash_before < worst_case_debit || recomposed.sizing.budget < worst_case_debit {
        return insufficient("EconomicPrepared breaches its cash or monetary budget cap");
    }
    if recomposed.balance.band_floor >= recomposed.balance.band_ceiling_exclusive
        || recomposed.ladder.best_ask < recomposed.balance.band_floor
        || recomposed.ladder.best_ask >= recomposed.balance.band_ceiling_exclusive
        || recomposed.ladder.limit_price < recomposed.balance.band_floor
        || recomposed.ladder.limit_price >= recomposed.balance.band_ceiling_exclusive
        || recomposed.ladder.limit_price > recomposed.balance.chase_ceiling
    {
        return insufficient("EconomicPrepared breaches its band or chase cap");
    }
    let impact_ceiling = recorded_impact_ceiling(
        recomposed.ladder.best_ask,
        recomposed.balance.price_impact_cap_bps,
    )?;
    if recomposed.ladder.limit_price > impact_ceiling {
        return insufficient("EconomicPrepared breaches its price-impact cap");
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
    let evaluated_at_unix = economic.risk.evaluated_at_unix_ms.div_euclid(1_000);
    let snapshot = replayed_financial_snapshot(context, evaluated_at_unix)?;
    let current_prices = replayed_risk_prices(
        &economic.risk.price_receipts,
        economic.risk.evaluated_at_unix_ms,
        &snapshot.positions,
        context.source,
    )?;
    let era = paper_era(context.paper_prefix.to_vec());
    let latency_was_active =
        active_risk_halts(&era).contains(&(RiskHaltOwner::Paper, RiskHaltCause::CopyLatency));
    let reconstructed = build_paper_risk_snapshot(
        &economic.risk.snapshot,
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
        RiskDecision::Approved => pe_execution_core::RiskDecisionAudit::Approved,
        RiskDecision::Blocked(reason) => pe_execution_core::RiskDecisionAudit::Blocked { reason },
    };
    if expected_risk != economic.risk.decision {
        return insufficient("EconomicPrepared risk decision differs from shared risk owner");
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

fn verify_mark(
    mark: &PortfolioMark,
    cash: Decimal,
    positions: &[OpenPosition],
    replayed_financial_prefix: Option<EventSeq>,
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<QualificationMarkReport, QualificationError> {
    if mark.invalid.is_some()
        || mark.equity <= Decimal::ZERO
        || mark.cash != cash
        || mark.cutoff_unix.rem_euclid(86_400) != 0
        || mark.financial_prefix_seq != replayed_financial_prefix
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
        let classified = classify_recorded_prices_history(observation)?;
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
    let mut equity = cash;
    for position in positions {
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

#[derive(Deserialize)]
struct RecordedPricesHistory {
    history: Vec<RecordedPricePoint>,
}

#[derive(Deserialize)]
struct RecordedPricePoint {
    t: i64,
    p: Box<RawValue>,
}

fn classify_recorded_prices_history(
    observation: &SourceObservation,
) -> Result<ClassifiedPricesHistory, QualificationError> {
    if observation.source_id != "pe-service.clob-prices-history"
        || observation.schema_version != 1
        || observation.parser_version != 1
    {
        return insufficient("PortfolioMark price receipt has the wrong source contract");
    }
    let response: RecordedPricesHistory =
        serde_json::from_slice(&observation.payload).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "PortfolioMark historical response is invalid: {error}"
            ))
        })?;
    let mut seen_timestamps = HashSet::new();
    let mut points = Vec::with_capacity(response.history.len());
    for point in response.history {
        if !seen_timestamps.insert(point.t) {
            return insufficient("PortfolioMark historical response contains a duplicate sample");
        }
        let lexeme = point.p.get().trim();
        let unquoted = lexeme
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap_or(lexeme);
        let price = Decimal::from_str_exact(unquoted)
            .or_else(|_| Decimal::from_scientific(unquoted))
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "PortfolioMark historical price is invalid: {error}"
                ))
            })?;
        points.push(PricePoint { t: point.t, price });
    }
    Ok(if points.is_empty() {
        ClassifiedPricesHistory::Empty
    } else {
        ClassifiedPricesHistory::Points(points)
    })
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

/// Derive the completion inputs from the active financial era.
///
/// A close is causal only when its Fill Final follows the current promotion anchor and a matching
/// Resolution Final follows that fill. Invalid or non-daily marks cannot trigger an automatic
/// seal. The offline verifier invokes this same calculation against its verified sealed prefix.
#[must_use]
pub fn qualification_completion(
    era: &crate::paper_recovery::PaperEra,
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
            } => match result {
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
            },
            PaperLogRecord::QualificationStarted(_)
            | PaperLogRecord::QualificationSealed(_)
            | PaperLogRecord::RiskHaltChanged { .. }
            | PaperLogRecord::MembershipChanged { .. } => {}
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

fn resolution_sequences(
    frames: &[ScannedPaperFrame],
    start_index: usize,
    seal_index: usize,
) -> HashMap<String, Vec<u64>> {
    let mut prepared_conditions = HashMap::<(u64, String), String>::new();
    let mut resolved = HashMap::<String, Vec<u64>>::new();
    for frame in &frames[start_index + 1..=seal_index] {
        if let PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
            payload: FinancialPayload::Resolution { condition_id, .. },
            ..
        }) = &frame.frame
        {
            prepared_conditions.insert(receipt_key(frame.receipt), condition_id.0.clone());
        }
        if let PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result: FinancialResult::Resolution { .. },
        }) = &frame.frame
            && let Some(condition) = prepared_conditions.get(&receipt_key(*prepared_receipt))
        {
            resolved
                .entry(condition.clone())
                .or_default()
                .push(frame.receipt.sequence.0);
        }
    }
    resolved
}

fn bind_final_receipts(
    decisions: &[crate::decision_replay::ReplayedDecision],
    fills: &[CompletedFill],
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
                decision.continuation.source_trade_id
            ))
        })?;
        let mut candidates = fills
            .iter()
            .filter(|fill| fill.final_receipt == final_receipt);
        let fill = candidates.next().ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "fill decision {} references no FinancialFinal",
                decision.continuation.source_trade_id
            ))
        })?;
        if candidates.next().is_some() || !matched_finals.insert(receipt_key(final_receipt)) {
            return insufficient("multiple fill decisions bind the same FinancialFinal");
        }

        let continuation = &decision.continuation;
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
#[serde(deny_unknown_fields)]
pub struct FinancialEraManifest {
    pub kind: String,
    pub state: String,
    pub activation_id: String,
    pub generation: String,
    pub fresh_bankroll: CollateralAmount,
    pub target_revision: String,
    pub artifact_blake3: String,
    pub static_config_hash: String,
    pub hot_config_hash: String,
    pub ranking_batch_id: i64,
    pub policy_hash: String,
    pub membership: Vec<pe_core_types::WalletAddress>,
    pub membership_proofs_hash: String,
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
    pub expected_hot_config_names_hash: String,
    pub ranking_identity: String,
    pub fresh_bankroll_identity: String,
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
) -> Result<String, QualificationError> {
    let bytes = fs::read(manifest_path)?;
    let manifest: FinancialEraManifest = serde_json::from_slice(&bytes)?;
    validate_financial_manifest(&manifest, config)?;
    match command {
        FinancialEraCommand::Prepare => {
            if manifest.state != "prepared" {
                return insufficient("financial-era prepare requires manifest state prepared");
            }
            let preparation = prepare_financial_era(&manifest)?;
            Ok(serde_json::to_string(&preparation)?)
        }
        FinancialEraCommand::Start => start_financial_era(&manifest),
        FinancialEraCommand::RollbackCheck => rollback_check_financial_era(&manifest),
    }
}

fn validate_financial_manifest(
    manifest: &FinancialEraManifest,
    config: &ServiceConfig,
) -> Result<(), QualificationError> {
    if manifest.kind != FINANCIAL_ERA_KIND
        || manifest.paths.paper_log != config.event_log_path
        || manifest.paths.source_log != config.source_event_log_path
        || manifest.paths.paper_state != config.paper_state_db_path
    {
        return insufficient("financial-era manifest kind or configured paths differ");
    }
    Ok(())
}

fn prepare_financial_era(
    manifest: &FinancialEraManifest,
) -> Result<FinancialEraPreparation, QualificationError> {
    let paper_prefix = Scanner::verify(&manifest.paths.paper_log)?;
    let source_prefix = Scanner::verify(&manifest.paths.source_log)?;
    let live_prefix = pe_execution_core::LiveJournal::verified_tail(&manifest.paths.live_journal)
        .map_err(|error| {
        QualificationError::InsufficientEvidence(format!("live journal: {error}"))
    })?;
    let open_live_orders =
        pe_execution_core::live_journal::open_order_inventory(&manifest.paths.live_journal)
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "live open-order inventory: {error}"
                ))
            })?;
    if !open_live_orders.is_empty() {
        return insufficient("financial-era prepare found an open live order");
    }
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
    let start = QualificationStarted {
        starting_bankroll: manifest.fresh_bankroll,
        paper_prefix: TailBinding::from(&paper_prefix),
        source_prefix: TailBinding::from(&source_prefix),
        live_prefix: TailBinding::from(&live_prefix),
        artifact_blake3: manifest.artifact_blake3.clone(),
        static_config_hash: manifest.static_config_hash.clone(),
        hot_config_hash: manifest.hot_config_hash.clone(),
        generation: manifest.generation.clone(),
        activation_id: manifest.activation_id.clone(),
        ranking_batch_id: manifest.ranking_batch_id,
        policy_hash: manifest.policy_hash.clone(),
        membership: manifest.membership.clone(),
        membership_proofs_hash: manifest.membership_proofs_hash.clone(),
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

fn start_financial_era(manifest: &FinancialEraManifest) -> Result<String, QualificationError> {
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
    let recomputed = prepare_financial_era(manifest)?;
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
) -> Result<String, QualificationError> {
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
    use pe_core_types::WalletAddress;
    use rust_decimal_macros::dec;

    use super::*;

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

    #[test]
    fn risk_prices_require_exact_causal_gamma_receipts() {
        let receipt = AppendReceipt {
            sequence: EventSeq(3),
            this_hash: blake3::hash(b"gamma-risk-price"),
        };
        let market = MarketId(VenueMarketId("condition-a".to_owned()));
        let position = FinancialPositionRow {
            market_id: market.clone(),
            outcome_id: OutcomeId(1),
            long: ShareAmount::from_atomic(1_000_000),
            short: ShareAmount::ZERO,
        };
        let observation = SourceObservation {
            receipt,
            received_unix_ms: 1_700_000_000_000,
            source_id: GAMMA_MARKETS_SOURCE_ID.to_owned(),
            schema_version: GAMMA_MARKETS_SCHEMA_VERSION,
            parser_version: GAMMA_MARKETS_PARSER_VERSION,
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
            .is_err()
        );
    }

    #[test]
    fn financial_prepare_is_read_only_and_start_is_receipt_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let paper_log = temp.path().join("paper.log");
        let source_log = temp.path().join("source.log");
        let live_journal = temp.path().join("live.log");
        let paper_state = temp.path().join("paper.db");
        drop(Writer::open(&paper_log).unwrap());
        drop(Writer::open(&source_log).unwrap());
        drop(pe_execution_core::LiveJournal::open(&live_journal).unwrap());
        drop(PaperStateDb::open(&paper_state).unwrap());

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
            hot_config_hash: "c".repeat(64),
            ranking_batch_id: 545,
            policy_hash: "policy".to_owned(),
            membership: started("c").membership,
            membership_proofs_hash: "proof".to_owned(),
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
            expected_hot_config_names_hash: "3".repeat(64),
            ranking_identity: "batch:545".to_owned(),
            fresh_bankroll_identity: "100".to_owned(),
            preparation: None,
            stop_invoked: false,
            service_was_active: None,
            backup: None,
            remote_census: None,
            guarded_logs: None,
            start_receipt: None,
            started_unix: None,
        };
        let preparation = prepare_financial_era(&manifest).unwrap();
        let after = paths
            .iter()
            .map(|path| fs::read(path).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(before, after);

        manifest.state = "guarded".to_owned();
        manifest.preparation = Some(preparation.clone());
        let first = start_financial_era(&manifest).unwrap();
        let second = start_financial_era(&manifest).unwrap();
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
