//! Network-free financial-era preparation and sealed paper qualification (#545).
//!
//! This module deliberately owns no HTTP client. It consumes only verified framed logs and the
//! read-only paper-state projection, and emits canonical compact JSON with a trailing line feed.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use pe_core_types::{
    CollateralAmount, EventSeq, ReceivedAt, ShareAmount, Side, SourceId, SourceTimestamp,
    SourceTradeId,
};
use pe_event_log::envelope::{HashInput, compute_hashes};
use pe_event_log::{
    AppendReceipt, ContentType, EnvelopeIn, LogTailBinding, Reader, Scanner, Writer,
};
use pe_execution_core::EconomicPrepared;
use pe_paper_state::{DecisionPendingState, PaperStateDb};
use pe_risk_engine::{KILL_SWITCH_DRAWDOWN_BPS, RiskDecision, evaluate_risk};
use pe_trader_index::score::lcb_5pct_decimal;
use pe_venue_polymarket::taker_fee;
use rust_decimal::prelude::ToPrimitive as _;
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::config::ServiceConfig;
use crate::decision_replay::replay_decision_pending;
use crate::paper_recovery::{
    FinancialPayload, FinancialResult, MembershipReason, PAPER_LOG_SCHEMA_VERSION, PaperLogFrame,
    PaperLogRecord, PortfolioMark, QualificationSealed, QualificationStarted, ScannedPaperFrame,
    SealReason, TailBinding, scan_paper_log,
};

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
}

#[derive(Debug, Clone)]
enum PreparedFact {
    Fill {
        receipt: AppendReceipt,
        source_trade_id: SourceTradeId,
    },
    Resolution {
        receipt: AppendReceipt,
    },
}

impl PreparedFact {
    fn receipt(&self) -> AppendReceipt {
        match self {
            Self::Fill { receipt, .. } | Self::Resolution { receipt } => *receipt,
        }
    }
}

#[derive(Debug, Clone)]
struct CompletedFill {
    final_receipt: AppendReceipt,
    source_trade_id: SourceTradeId,
    condition_id: String,
    final_sequence: u64,
    delay_ms: u64,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    condition_id: String,
    market_id: String,
    outcome_index: u8,
    shares_atomic: u64,
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
    verify_recorded_prefix(&options.source_log, &seal.source_prefix)?;
    verify_recorded_prefix(&options.paper_log, &seal.financial_prefix)?;
    verify_start_prefix(&frames[start_index], &start)?;

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
    let mut prepared: BTreeMap<(u64, String), PreparedFact> = BTreeMap::new();
    let mut completed_prepared = HashSet::new();
    let mut completed_fills = Vec::new();
    let mut open_positions: Vec<OpenPosition> = Vec::new();
    let mut last_completed_prepared = None;
    let mut economic_hashes = Vec::new();
    let mut financial_final_count = 0usize;
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

    for frame in &frames[start_index + 1..=seal_index] {
        let PaperLogFrame::Record(record) = &frame.frame else {
            return insufficient("legacy paper fill exists after QualificationStarted");
        };
        match record {
            PaperLogRecord::FinancialPrepared {
                expected_authority,
                payload,
            } => {
                if expected_authority.qualification_start_receipt != start_receipt
                    || expected_authority.prior_completed_prepared_sequence
                        != last_completed_prepared
                {
                    return insufficient("FinancialPrepared authority chain mismatch");
                }
                match payload {
                    FinancialPayload::Fill {
                        operation,
                        economic,
                    } => {
                        verify_economic(economic, cash)?;
                        economic_hashes.push(economic.core_hash().map_err(|error| {
                            QualificationError::InsufficientEvidence(format!(
                                "economic core hash failed: {error}"
                            ))
                        })?);
                        let key = receipt_key(frame.receipt);
                        if prepared
                            .insert(
                                key,
                                PreparedFact::Fill {
                                    receipt: frame.receipt,
                                    source_trade_id: operation.source_trade_id.clone(),
                                },
                            )
                            .is_some()
                        {
                            return insufficient("duplicate FinancialPrepared receipt");
                        }
                    }
                    FinancialPayload::Resolution { .. } => {
                        let key = receipt_key(frame.receipt);
                        if prepared
                            .insert(
                                key,
                                PreparedFact::Resolution {
                                    receipt: frame.receipt,
                                },
                            )
                            .is_some()
                        {
                            return insufficient("duplicate FinancialPrepared receipt");
                        }
                    }
                }
            }
            PaperLogRecord::FinancialFinal {
                prepared_receipt,
                result,
            } => {
                let key = receipt_key(*prepared_receipt);
                if !completed_prepared.insert(key.clone()) {
                    return insufficient("FinancialPrepared has multiple Finals");
                }
                let prepared_frame =
                    find_prepared_record(&frames, start_index, frame, *prepared_receipt)?;
                let prepared_payload = match &prepared_frame.frame {
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                        payload, ..
                    }) => payload,
                    _ => {
                        return insufficient("FinancialFinal does not reference FinancialPrepared");
                    }
                };
                let prepared_fact = prepared.get(&key).ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "FinancialFinal references an unknown Prepared receipt".to_owned(),
                    )
                })?;
                match (prepared_payload, result) {
                    (
                        FinancialPayload::Fill { economic, .. },
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
                        let source_trade_id = match prepared_fact {
                            PreparedFact::Fill {
                                source_trade_id, ..
                            } => source_trade_id.clone(),
                            PreparedFact::Resolution { .. } => {
                                return insufficient("Fill Final references Resolution Prepared");
                            }
                        };
                        completed_fills.push(CompletedFill {
                            final_receipt: frame.receipt,
                            source_trade_id,
                            condition_id: economic.market.condition_id.0.clone(),
                            final_sequence: frame.receipt.sequence.0,
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
                            ..
                        },
                        FinancialResult::Resolution { canonical },
                    ) => {
                        if canonical.applied_prepared_seq != prepared_receipt.sequence
                            || !matches!(canonical.outcome.as_str(), "applied" | "existing")
                        {
                            return insufficient("Resolution Final authority mismatch");
                        }
                        let expected_credit = resolution_credit(
                            &open_positions,
                            &condition_id.0,
                            payout_by_outcome_index_json,
                        )?;
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
                        open_positions.retain(|position| position.condition_id != condition_id.0);
                    }
                    _ => return insufficient("Financial Prepared/Final kinds disagree"),
                }
                last_completed_prepared = Some(prepared_fact.receipt().sequence);
                financial_final_count = financial_final_count.saturating_add(1);
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
                let report = verify_mark(mark, cash, &open_positions, &source_observations)?;
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
            PaperLogRecord::QualificationSealed(candidate) => {
                if candidate.as_ref() != &seal || frame.receipt != seal_receipt {
                    return insufficient(
                        "a different QualificationSealed precedes the requested seal",
                    );
                }
            }
            PaperLogRecord::RiskHaltChanged { .. } => {}
        }
    }
    if prepared.len() != completed_prepared.len() {
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
    let all_equity = valid_marks
        .iter()
        .map(|(_, mark)| mark.equity)
        .collect::<Vec<_>>();
    let promotion_drawdown =
        pe_risk_engine::max_drawdown_fraction(&promotion_equity).map_err(|error| {
            QualificationError::InsufficientEvidence(format!("promotion drawdown: {error}"))
        })?;
    let era_drawdown = pe_risk_engine::max_drawdown_fraction(&all_equity).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("era drawdown: {error}"))
    })?;
    let lcb = lcb_5pct_decimal(&growth);

    let resolution_sequences = resolution_sequences(&frames, start_index, seal_index);
    let mut closed = completed_fills
        .iter()
        .filter(|fill| fill.final_sequence > anchor_sequence)
        .filter(|fill| {
            resolution_sequences
                .get(&fill.condition_id)
                .is_some_and(|sequences| {
                    sequences
                        .iter()
                        .any(|sequence| *sequence > fill.final_sequence)
                })
        })
        .collect::<Vec<_>>();
    closed.sort_by_key(|fill| fill.final_sequence);
    let delays = closed.iter().map(|fill| fill.delay_ms).collect::<Vec<_>>();
    let p95 = nearest_rank_p95(&delays);
    let absolute_pnl = valid_marks.last().and_then(|(_, mark)| {
        mark.equity
            .checked_sub(start.starting_bankroll.to_decimal())
    });

    let thresholds = QualificationThresholds::canonical();
    let mut failures = Vec::new();
    if growth.len() < thresholds.minimum_complete_days {
        failures.push(format!(
            "complete days {} < {}",
            growth.len(),
            thresholds.minimum_complete_days
        ));
    }
    if closed.len() < thresholds.minimum_closed_copies {
        failures.push(format!(
            "closed copies {} < {}",
            closed.len(),
            thresholds.minimum_closed_copies
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
        complete_days: growth.len(),
        closed_copies: closed.len(),
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
    let mut observations = BTreeMap::new();
    for item in Reader::replay(path)? {
        let (sequence, envelope) = item?;
        if prefix.last_sequence.is_some_and(|last| sequence > last) {
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
            },
        );
    }
    Ok(observations)
}

fn received_unix_ms(envelope: &pe_event_log::EventEnvelope) -> Result<i64, QualificationError> {
    let millis = envelope.received_at.0.unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).map_err(|_| {
        QualificationError::InsufficientEvidence("event timestamp milliseconds overflow".to_owned())
    })
}

fn verify_economic(economic: &EconomicPrepared, cash: Decimal) -> Result<(), QualificationError> {
    if economic.version != pe_execution_core::ECONOMIC_PREPARED_VERSION
        || economic.admission.fee_schedule != economic.fee.schedule
        || economic.ladder.minimum_shares != economic.sizing.minimum_shares
        || economic.ladder.principal != economic.sizing.principal
        || economic.ladder.expected_shares().ok() != Some(economic.sizing.expected_shares)
        || economic.ladder.expected_vwap() != Some(economic.sizing.expected_vwap)
        || economic.balance.cash_before.to_decimal() != cash
        || economic.worst_case_all_in_debit().ok() != Some(economic.balance.worst_case_debit)
    {
        return insufficient("EconomicPrepared structural replay mismatch");
    }
    let expected_fee = taker_fee(
        economic.fee.schedule,
        economic.sizing.minimum_shares,
        economic.ladder.limit_price,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!("fee replay failed: {error}"))
    })?;
    if expected_fee != economic.fee.expected_fee {
        return insufficient("EconomicPrepared fee differs from shared fee owner");
    }
    let expected_risk = evaluate_risk(&economic.risk.snapshot);
    let recorded_risk_matches = matches!(
        (expected_risk, economic.risk.decision),
        (
            RiskDecision::Approved,
            pe_execution_core::RiskDecisionAudit::Approved
        ) | (
            RiskDecision::Blocked(_),
            pe_execution_core::RiskDecisionAudit::Blocked { .. }
        )
    ) && match (expected_risk, economic.risk.decision) {
        (
            RiskDecision::Blocked(expected),
            pe_execution_core::RiskDecisionAudit::Blocked { reason },
        ) => expected == reason,
        _ => true,
    };
    if !recorded_risk_matches {
        return insufficient("EconomicPrepared risk decision differs from shared risk owner");
    }
    Ok(())
}

fn find_prepared_record<'a>(
    frames: &'a [ScannedPaperFrame],
    start_index: usize,
    final_frame: &ScannedPaperFrame,
    receipt: AppendReceipt,
) -> Result<&'a ScannedPaperFrame, QualificationError> {
    frames[start_index + 1..]
        .iter()
        .take_while(|candidate| candidate.receipt.sequence < final_frame.receipt.sequence)
        .find(|candidate| candidate.receipt == receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "FinancialFinal Prepared receipt is absent or non-causal".to_owned(),
            )
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

fn resolution_credit(
    positions: &[OpenPosition],
    condition_id: &str,
    payout_json: &str,
) -> Result<CollateralAmount, QualificationError> {
    let payouts: Vec<Decimal> = serde_json::from_str(payout_json).map_err(|error| {
        QualificationError::InsufficientEvidence(format!("resolution payout decode: {error}"))
    })?;
    let mut by_outcome = BTreeMap::<u8, u64>::new();
    for position in positions
        .iter()
        .filter(|position| position.condition_id == condition_id)
    {
        let entry = by_outcome.entry(position.outcome_index).or_default();
        *entry = entry.checked_add(position.shares_atomic).ok_or_else(|| {
            QualificationError::InsufficientEvidence("resolution quantity overflow".to_owned())
        })?;
    }
    let mut credit_atomic = 0u64;
    for (outcome_index, shares_atomic) in by_outcome {
        let payout = payouts
            .get(usize::from(outcome_index))
            .copied()
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "resolution payout omits an open outcome".to_owned(),
                )
            })?;
        if payout < Decimal::ZERO || payout > Decimal::ONE {
            return insufficient("resolution payout is outside [0,1]");
        }
        let outcome_credit = Decimal::from(shares_atomic)
            .checked_mul(payout)
            .map(|value| value.floor())
            .and_then(|value| value.to_u64())
            .ok_or_else(|| {
                QualificationError::InsufficientEvidence(
                    "resolution credit arithmetic overflow".to_owned(),
                )
            })?;
        credit_atomic = credit_atomic.checked_add(outcome_credit).ok_or_else(|| {
            QualificationError::InsufficientEvidence("resolution credit overflow".to_owned())
        })?;
    }
    Ok(CollateralAmount::from_atomic(credit_atomic))
}

fn verify_mark(
    mark: &PortfolioMark,
    cash: Decimal,
    positions: &[OpenPosition],
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<QualificationMarkReport, QualificationError> {
    if mark.invalid.is_some() || mark.equity <= Decimal::ZERO || mark.cash != cash {
        return insufficient("PortfolioMark is invalid or its cash differs from replay");
    }
    let mut prices = HashMap::new();
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
    let by_source = decisions
        .iter()
        .map(|decision| {
            (
                &decision.continuation.source_trade_id,
                &decision.post_boundary.body.terminal,
            )
        })
        .collect::<HashMap<_, _>>();
    for fill in fills {
        let terminal = by_source.get(&fill.source_trade_id).ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "Fill Final {} has no terminal decision",
                fill.source_trade_id
            ))
        })?;
        if terminal.final_receipt != Some(fill.final_receipt) {
            return insufficient("terminal decision does not bind its synchronized Fill Final");
        }
    }
    Ok(())
}

fn nearest_rank_p95(samples: &[u64]) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let rank = ordered.len().checked_mul(95)?.checked_add(99)? / 100;
    ordered.get(rank.saturating_sub(1)).copied()
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
    if state.fills_count()? != 0
        || state.settled_count()? != 0
        || !state.list_fill_snapshots()?.is_empty()
    {
        return insufficient(
            "local financial tables are nonempty; integration requires the schema-v3 atomic reset owner",
        );
    }
    state.replace_authoritative_state(manifest.fresh_bankroll.to_decimal(), &[])?;
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
