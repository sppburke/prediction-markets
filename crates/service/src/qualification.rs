//! Network-free financial-era preparation and sealed paper qualification (#545).
//!
//! This module deliberately owns no HTTP client. It consumes only verified framed logs and the
//! read-only paper-state projection, and emits canonical compact JSON with a trailing line feed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use pe_copy_signal_engine::{LeaderSignal, PositionState, SignalConfig};
use pe_core_types::{
    AccountId, CollateralAmount, EventSeq, MarketId, MarketOutcomeId, OutcomeId, Price,
    ProbabilityPpm, ReceivedAt, ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId,
    TraderId, VenueId, VenueMarketId,
};
use pe_event_log::envelope::{HashInput, compute_hashes};
use pe_event_log::{
    AppendReceipt, ContentType, EnvelopeIn, EventEnvelope, LogTailBinding, Reader, Scanner, Writer,
};
use pe_execution_core::live_executor::{
    LiveAdmissionAccountEvidence, LiveAdmissionClassificationInput, LiveAdmissionNeedsAccountState,
    classify_live_admission,
};
use pe_execution_core::{
    EconomicPrepared, LiveAdmissionVerdict, ObservationEvidence, RiskAudit, RiskDecisionAudit,
    SizingModeAudit,
};
#[cfg(test)]
use pe_kelly_sizer::{KELLY_NORMAL, KELLY_PAPER_BACKTEST};
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{
    DecisionPendingRow, DecisionPendingState, FillRow, FinancialSnapshot, PaperPositionRow,
    PaperStateDb, SettledMarketRow,
};
use pe_position_ledger::{
    AppliedEffect, LedgerMutation, PositionLedger, SecondVerdict, classify_complete_second,
};
#[cfg(test)]
use pe_risk_engine::RiskSnapshot;
use pe_risk_engine::{
    BinaryPayout, KILL_SWITCH_DRAWDOWN_BPS, RiskDecision, RiskHaltCause,
    aggregate_resolution_credit, evaluate_risk, nearest_rank_p95,
};
use pe_source_polymarket_public::ClassifiedPricesHistory;
#[cfg(test)]
use pe_source_polymarket_public::{
    ACTIVITY_MAX_OFFSET, GAMMA_BATCH_LIMIT_PARAM, GAMMA_MARKETS_PARSER_VERSION,
    GAMMA_MARKETS_SCHEMA_VERSION, GAMMA_MARKETS_SOURCE_ID, LIVE_MARKET_PARSER_VERSION,
    LIVE_MARKET_SCHEMA_VERSION, RECONCILIATION_PAGE_LIMIT, ReconciliationPageEvidence,
    validate_live_market,
};
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, ActivityType, BinaryPayoutVector,
    CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION, ClobPayoutResolution,
    ClobPricesHistoryClient, FixtureFetcher, aggregate_activity_rows, parse_activity_response,
    parse_activity_row, parse_activity_trade_observation, parse_clob_market,
};
use pe_trader_index::score::lcb_5pct_decimal;
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[cfg(test)]
use crate::bucket_commit::PageOccurrence;
use crate::bucket_commit::{CompleteActivityPage, DecisionContinuationV3};
use crate::config::ServiceConfig;
use crate::decision_replay::{
    WinnerFollowDecisionInputs, WinnerFollowRiskInputEvidence, replay_decision_pending,
};
use crate::live_fanout::{
    EconomicReplayError, RecordedEconomicSource, replay_source_backed_economic,
};
#[cfg(test)]
use crate::mid_price_cache::{
    GAMMA_PRICE_ATTEMPT_PARSER_VERSION, GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
    GammaPriceAttemptRecord, StrictPriceInput, classify_strict_prices,
};
use crate::mid_price_cache::{
    RecordedPriceAttempt, RiskPriceReplayError, replay_strict_risk_prices,
};
use crate::paper_recovery::{
    CapacityMembershipArtifact, FINANCIAL_SEMANTIC_VERSION, FinancialPayload, FinancialResult,
    KnockoutCausalArtifact, MembershipAdmissionArtifact, MembershipAdmissionReceipt,
    MembershipProofBinding, MembershipProofManifest, MembershipReason, PAPER_LOG_SCHEMA_VERSION,
    PaperLogFrame, PaperLogRecord, PortfolioMark, QualificationSealed, QualificationStarted,
    RankingMembershipArtifact, RiskHaltOwner, ScannedPaperFrame, SealReason,
    SealedMembershipEvidence, TailBinding, active_risk_halts, paper_era, scan_paper_log,
};
use crate::risk_inputs::{
    PaperExposureBase, RiskInputsUnavailable, apply_global_risk_halts, build_paper_risk_base,
    build_paper_risk_snapshot_from_source_receipts, historical_mark_price,
    paper_prefix_at_financial_prefix,
};
use crate::runtime_config::{
    ConfigEra, ConfigRow, RISK_HALT_RELEASE_HASH_KEY, RuntimeConfig, parse_config,
};
use crate::watchlist_admission::{
    CAPACITY_CONFIG_SOURCE_ID, KNOCKOUT_CAUSAL_SOURCE_ID, MEMBERSHIP_ADMISSION_SOURCE_ID,
    MEMBERSHIP_ARTIFACT_PARSER_VERSION, MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
    RANKING_MEMBERSHIP_SOURCE_ID,
};
use crate::watchlist_maintenance::{
    MaintenanceConfig, MembershipMode, knockout_decision, planned_admission_wallets,
    ranked_membership_change_wallets,
};

const QUALIFICATION_REPORT_VERSION: u16 = 1;
const FINANCIAL_ERA_KIND: &str = "financial-era-v1";
const QUALIFICATION_SOURCE_ID: &str = "pe-service.qualification";
const MINIMUM_COMPLETE_DAYS: usize = 30;
const MINIMUM_CLOSED_COPIES: usize = 90;
const MAX_P95_DELAY_MS: u64 = 2_000;
const QUALIFICATION_PASS_REASON: &str =
    "all sealed one-system gates passed; manual review remains required";

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
    pub live_journal: Option<PathBuf>,
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
    pub financial_semantic_version: Option<u32>,
    pub economic_core_hashes: Vec<String>,
    #[serde(default)]
    pub live_prefix_hash: Option<String>,
    #[serde(default)]
    pub live_journal_hash: Option<String>,
    #[serde(default)]
    pub live_wrapper_facts: Vec<QualificationLiveWrapperFact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationLiveWrapperFact {
    pub account_id: String,
    pub journal_sequence: u64,
    pub source_trade_id: String,
    pub economic_core_hash: String,
    pub paper_wrapper_hash: String,
    pub live_wrapper_hash: String,
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
    /// Validate the self-contained report invariants required for ordinary-live promotion.
    pub(crate) fn validate_for_live_promotion(&self) -> Result<(), &'static str> {
        if self.version != QUALIFICATION_REPORT_VERSION {
            return Err("qualification report version is unsupported");
        }
        if self.verdict != QualificationVerdict::Pass {
            return Err("qualification report verdict is not Pass");
        }
        if !self.replay.exact {
            return Err("qualification report replay is not exact");
        }

        let canonical_thresholds = QualificationThresholds::canonical();
        if self.thresholds != canonical_thresholds {
            return Err("qualification report thresholds are not canonical");
        }
        if self.complete_days < canonical_thresholds.minimum_complete_days {
            return Err("qualification report complete-days gate failed");
        }
        if self.closed_copies < canonical_thresholds.minimum_closed_copies {
            return Err("qualification report closed-copies gate failed");
        }
        if self
            .lcb_5pct_decimal
            .is_none_or(|value| value <= Decimal::ZERO)
        {
            return Err("qualification report LCB_5pct gate failed");
        }
        if self.promotion_max_drawdown_fraction.is_none_or(|value| {
            value < Decimal::ZERO
                || value >= canonical_thresholds.maximum_drawdown_fraction_exclusive
        }) {
            return Err("qualification report promotion-drawdown gate failed");
        }
        if self
            .paper_p95_delay_ms
            .is_none_or(|value| value > canonical_thresholds.maximum_p95_delay_ms)
        {
            return Err("qualification report paper-delay gate failed");
        }
        if self.reasons.len() != 1
            || self.reasons.first().map(String::as_str) != Some(QUALIFICATION_PASS_REASON)
        {
            return Err("qualification report Pass reasons are inconsistent");
        }
        Ok(())
    }

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
                financial_semantic_version: None,
                economic_core_hashes: Vec::new(),
                live_prefix_hash: None,
                live_journal_hash: None,
                live_wrapper_facts: Vec::new(),
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

fn resolution_source_received_at_observation(
    source: &SourceObservation,
    condition: &pe_core_types::PolymarketConditionId,
    payout_json: &str,
) -> Result<i64, String> {
    let corrupt = |message: String| format!("corrupt supabase value: {message}");
    if source.source_id != "polymarket.clob.market" {
        return Err(corrupt(
            "resolution receipt does not reference CLOB market evidence".to_owned(),
        ));
    }
    if source.schema_version != CLOB_RESOLUTION_SCHEMA_VERSION
        || source.parser_version != CLOB_RESOLUTION_PARSER_VERSION
    {
        return Err(corrupt(
            "resolution receipt uses an unsupported CLOB schema or parser version".to_owned(),
        ));
    }
    let market = parse_clob_market(&source.payload).map_err(|error| {
        corrupt(format!(
            "parse referenced CLOB resolution evidence: {error}"
        ))
    })?;
    if market.condition_id.as_deref() != Some(condition.0.as_str()) {
        return Err(corrupt(
            "referenced CLOB resolution condition differs from Prepared".to_owned(),
        ));
    }
    let ClobPayoutResolution::Resolved(payout) = market.resolution_evidence().payout else {
        return Err(corrupt("referenced CLOB market is not resolved".to_owned()));
    };
    if payout.canonical_json() != payout_json {
        return Err(corrupt(
            "referenced CLOB payout differs from Prepared".to_owned(),
        ));
    }
    Ok(source.received_at.0.unix_timestamp())
}

fn decision_source_receipt(
    source: &BTreeMap<u64, SourceObservation>,
    receipt: AppendReceipt,
) -> Result<&SourceObservation, QualificationError> {
    let Some(observation) = source.get(&receipt.sequence.0) else {
        return insufficient(format!(
            "decision source receipt replay failed: source receipt sequence {} is absent",
            receipt.sequence.0
        ));
    };
    if observation.receipt != receipt {
        return insufficient(format!(
            "decision source receipt replay failed: source receipt sequence {} does not match its frozen evidence",
            receipt.sequence.0
        ));
    }
    Ok(observation)
}

fn decision_observation_from_source(
    continuation: &DecisionContinuationV3,
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<Option<pe_execution_core::ObservationEvidence>, QualificationError> {
    let Some(selected_receipt) = continuation.observation_receipt() else {
        return Ok(None);
    };
    for page in continuation.page_occurrences() {
        let observation = decision_source_receipt(source, page.receipt)?;
        if observation.source_id != crate::trade_poller::ACTIVITY_POLL_SOURCE_ID
            || observation.schema_version != pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
            || observation.parser_version != pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
            || blake3::hash(&observation.payload).to_hex().as_str() != page.raw_hash
        {
            return insufficient(format!(
                "decision source receipt replay failed: source receipt sequence {} does not match its frozen evidence",
                page.receipt.sequence.0
            ));
        }
    }
    if let Some(websocket_receipt) = continuation.observed_source_receipt {
        let observation = decision_source_receipt(source, websocket_receipt)?;
        let activity = parse_activity_trade_observation(&observation.payload).map_err(|_| {
            QualificationError::InsufficientEvidence(format!(
                "decision source receipt replay failed: source receipt sequence {} does not match its frozen evidence",
                websocket_receipt.sequence.0
            ))
        })?;
        if observation.source_id != crate::activity_ingest::ACTIVITY_WS_SOURCE_ID
            || observation.schema_version != pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
            || observation.parser_version != pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
            || activity.wallet != continuation.facts.wallet
            || activity.group_id.key() != &continuation.facts.source_trade_id
        {
            return insufficient(format!(
                "decision source receipt replay failed: source receipt sequence {} does not match its frozen evidence",
                websocket_receipt.sequence.0
            ));
        }
    }
    let selected = decision_source_receipt(source, selected_receipt)?;
    let complete_bound_receipt = continuation.complete_bound().ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "post-Start decision has no version-three source evidence".to_owned(),
        )
    })?;
    let provenance = if continuation.observed_source_receipt == Some(selected_receipt) {
        "activity_ws"
    } else {
        "rest_poll"
    };
    Ok(Some(pe_execution_core::ObservationEvidence {
        source_receipt: selected_receipt,
        complete_bound_receipt,
        observed_unix_ms: selected.received_unix_ms,
        provenance: provenance.to_owned(),
    }))
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
    economic: EconomicPrepared,
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

struct FinancialFactState {
    cash: Decimal,
    positions: Vec<OpenPosition>,
    fills: Vec<FillRow>,
    settlements: Vec<SettledMarketRow>,
    last_completed: Option<EventSeq>,
}

impl FinancialFactState {
    fn new(starting_bankroll: Decimal) -> Self {
        Self {
            cash: starting_bankroll,
            positions: Vec::new(),
            fills: Vec::new(),
            settlements: Vec::new(),
            last_completed: None,
        }
    }
}

struct CausalFinancialState {
    cash: Decimal,
    positions: Vec<OpenPosition>,
    last_completed: Option<EventSeq>,
    completed_prepared: HashSet<EventSeq>,
    closed_fill_final_conditions: HashMap<u64, String>,
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
    prepared_received_unix_ms: i64,
    start_hot_config_hash: &'a str,
    financial_semantic_version: u32,
}

async fn verify_qualification(
    options: &QualifyOptions,
) -> Result<QualificationReport, QualificationError> {
    let live_journal = options.live_journal.as_deref().ok_or_else(|| {
        QualificationError::InsufficientEvidence(
            "live journal is required for exact qualification replay".to_owned(),
        )
    })?;
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
    verify_tail_extension(&start.live_prefix, &seal.live_prefix, "live journal")?;
    verify_recorded_prefix(live_journal, &start.live_prefix)?;
    verify_recorded_prefix(live_journal, &seal.live_prefix)?;
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
    verify_initial_membership(&start)?;
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
    verify_decision_configurations(&replayed_decisions, &start)?;
    let mut decision_observations = HashMap::new();
    for decision in &replayed_decisions {
        let observation = verify_decision_source_inputs(decision, &source_observations)?;
        verify_decision_classification(&state, decision)?;
        if decision_observations
            .insert(
                decision.continuation.facts.source_trade_id.clone(),
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
    let decision_bytes = state.seal_decision_evidence_for_source_prefix(
        &decision_keys,
        start.source_prefix.last_sequence,
        seal.source_prefix.last_sequence,
    )?;
    let decision_digest = blake3::hash(&decision_bytes).to_hex().to_string();
    if decision_digest != seal.decision_evidence_digest {
        return insufficient(format!(
            "decision evidence digest mismatch: sealed {}, replayed {decision_digest}",
            seal.decision_evidence_digest
        ));
    }

    let mut financial = FinancialFactState::new(start.starting_bankroll.to_decimal());
    // `scan_paper_log` is the sole Prepared/Final state-machine validator. This map retains only
    // the already-validated payloads needed for the accounting and identity re-execution below.
    let mut prepared = BTreeMap::<(u64, String), &FinancialPayload>::new();
    let mut completed_fills = Vec::new();
    let mut completed_financial_facts = Vec::<CompletedFinancialFact>::new();
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
    let mut latest_causal_financial = None;
    let mut previous_fill_financial_prefix = None;

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
                        let paper_prefix = recorded_fill_paper_prefix(
                            &frames[start_index..frame_index],
                            economic.risk.financial_prefix,
                            economic.risk.evaluated_at_unix_ms,
                            &mut previous_fill_financial_prefix,
                        )?;
                        let financial_at_prefix = financial_state_at_prefix(
                            start.starting_bankroll.to_decimal(),
                            &completed_financial_facts,
                            economic.risk.financial_prefix,
                        )?;
                        verify_economic(
                            operation,
                            economic,
                            &RiskReplayContext {
                                cash: financial_at_prefix.cash,
                                positions: &financial_at_prefix.positions,
                                fills: &financial_at_prefix.fills,
                                settlements: &financial_at_prefix.settlements,
                                last_completed: financial_at_prefix.last_completed,
                                start_receipt,
                                paper_prefix,
                                source: &source_observations,
                                prepared_received_unix_ms: received_unix_ms(&frame.envelope)?,
                                start_hot_config_hash: &start.hot_config_hash,
                                financial_semantic_version: start.financial_semantic_version,
                            },
                            true,
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
                let cash_before = financial.cash;
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
                            economic: economic.clone(),
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
                        let resolution_source = source_observations
                            .get(&resolution_source_receipt.sequence.0)
                            .filter(|source| source.receipt == *resolution_source_receipt)
                            .ok_or_else(|| {
                                QualificationError::InsufficientEvidence(
                                    "resolution receipt is absent from the sealed source prefix"
                                        .to_owned(),
                                )
                            })?;
                        let settled_at_unix = resolution_source_received_at_observation(
                            resolution_source,
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
                    }
                    _ => return insufficient("Financial Prepared/Final kinds disagree"),
                }
                let fact = CompletedFinancialFact {
                    prepared_receipt: *prepared_receipt,
                    final_receipt: frame.receipt,
                    payload: prepared_payload.clone(),
                    result: result.clone(),
                };
                financial = reduce_financial_facts(
                    financial,
                    std::slice::from_ref(&fact),
                    |_| true,
                    |_| Ok(true),
                )?;
                match result {
                    FinancialResult::Fill { canonical } => {
                        if canonical.bankroll != financial.cash {
                            return insufficient("Fill Final bankroll differs from exact replay");
                        }
                    }
                    FinancialResult::Resolution { canonical } => {
                        if cash_before.checked_add(canonical.credit.to_decimal())
                            != Some(financial.cash)
                        {
                            return insufficient(
                                "Resolution Final credit differs from exact replay",
                            );
                        }
                        if canonical.bankroll != financial.cash {
                            return insufficient(
                                "Resolution Final bankroll differs from exact replay",
                            );
                        }
                    }
                }
                completed_financial_facts.push(fact);
                financial_final_count = financial_final_count.saturating_add(1);
            }
            PaperLogRecord::MembershipChanged {
                reason,
                removed,
                added,
                capacity,
                ranking_batch_id,
                evidence,
            } => {
                if removed.iter().collect::<HashSet<_>>().len() != removed.len()
                    || added.iter().collect::<HashSet<_>>().len() != added.len()
                    || removed.iter().any(|wallet| added.contains(wallet))
                    || removed.iter().any(|wallet| !membership.contains(wallet))
                    || added.iter().any(|wallet| membership.contains(wallet))
                {
                    return insufficient("MembershipChanged structural evidence is invalid");
                }
                verify_membership_change_evidence(
                    *reason,
                    removed,
                    added,
                    *capacity,
                    *ranking_batch_id,
                    evidence,
                    &MembershipEvidenceContext {
                        source: &source_observations,
                        current_membership: &membership,
                    },
                )?;
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
        &start,
        &DeclineReplayContext {
            frames: &frames,
            start_index,
            source: &source_observations,
            completed_financial_facts: &completed_financial_facts,
            start_receipt,
        },
    )
    .await?;
    let live_evidence = verify_live_wrappers(
        live_journal,
        &options.source_log,
        &seal.source_prefix,
        &seal.live_prefix,
        &start,
        &frames[start_index + 1..=financial_prefix_index],
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
    if !financial.positions.is_empty() {
        failures.push("paper positions remain open at the sealed financial prefix".to_owned());
    }

    let (verdict, mut reasons) = qualification_gate_verdict(failures);
    if verdict == QualificationVerdict::Pass {
        reasons.push(QUALIFICATION_PASS_REASON.to_owned());
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
            financial_semantic_version: Some(start.financial_semantic_version),
            economic_core_hashes: economic_hashes,
            live_prefix_hash: live_evidence.prefix_hash,
            live_journal_hash: live_evidence.journal_hash,
            live_wrapper_facts: live_evidence.wrapper_facts,
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
            financial_semantic_version: Some(start.financial_semantic_version),
            economic_core_hashes: Vec::new(),
            live_prefix_hash: Some(start.live_prefix.last_hash.clone()),
            live_journal_hash: Some(seal.live_prefix.last_hash.clone()),
            live_wrapper_facts: Vec::new(),
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

// ── Live-wrapper verification ────────────────────────────────────────────────

struct VerifiedLiveEvidence {
    prefix_hash: Option<String>,
    journal_hash: Option<String>,
    wrapper_facts: Vec<QualificationLiveWrapperFact>,
}

#[derive(Clone)]
struct PaperLiveWrapperBasis {
    operation: crate::paper_recovery::PaperFillOperationIdentity,
    economic: EconomicPrepared,
    economic_core_hash: String,
    wrapper_hash: String,
}

fn sealed_source_envelopes(
    path: &Path,
    prefix: &TailBinding,
) -> Result<Vec<EventEnvelope>, QualificationError> {
    let Some(last_sequence) = prefix.last_sequence else {
        return Ok(Vec::new());
    };
    let expected_hash = tail_hash(prefix)?;
    let mut envelopes = Vec::new();
    for item in Reader::replay(path)? {
        let (sequence, envelope) = item?;
        if sequence > last_sequence {
            break;
        }
        envelopes.push(envelope);
    }
    if envelopes
        .last()
        .is_none_or(|envelope| envelope.seq != last_sequence || envelope.this_hash != expected_hash)
    {
        return insufficient("sealed source envelopes do not reach their sequence/hash prefix");
    }
    Ok(envelopes)
}

fn paper_live_wrapper_bases(
    frames: &[ScannedPaperFrame],
) -> Result<HashMap<String, PaperLiveWrapperBasis>, QualificationError> {
    let mut bases = HashMap::new();
    for frame in frames {
        let PaperLogFrame::Record(record) = &frame.frame else {
            continue;
        };
        let PaperLogRecord::FinancialPrepared {
            payload:
                FinancialPayload::Fill {
                    operation,
                    economic,
                },
            ..
        } = record
        else {
            continue;
        };
        let economic_core_hash = economic.core_hash().map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "paper economic core hash failed during live-wrapper verification: {error}"
            ))
        })?;
        let wrapper_hash = blake3::hash(&serde_json::to_vec(record)?)
            .to_hex()
            .to_string();
        let source_trade_id = operation.source_trade_id.0.clone();
        if bases
            .insert(
                source_trade_id.clone(),
                PaperLiveWrapperBasis {
                    operation: operation.clone(),
                    economic: economic.clone(),
                    economic_core_hash,
                    wrapper_hash,
                },
            )
            .is_some()
        {
            return insufficient(format!(
                "paper decision {source_trade_id} has multiple Prepared economic wrappers"
            ));
        }
    }
    Ok(bases)
}

fn verify_live_wrappers(
    live_journal: &Path,
    source_log: &Path,
    source_prefix: &TailBinding,
    live_prefix: &TailBinding,
    start: &QualificationStarted,
    paper_frames: &[ScannedPaperFrame],
) -> Result<VerifiedLiveEvidence, QualificationError> {
    let wrapper_events = replay_live_prefix(live_journal, &start.live_prefix, live_prefix)?;
    let source_envelopes = sealed_source_envelopes(source_log, source_prefix)?;
    let paper_bases = paper_live_wrapper_bases(paper_frames)?;
    let mut wrapper_facts = Vec::new();

    let account_ids = wrapper_events
        .iter()
        .map(|event| event.account_id.clone())
        .collect::<BTreeSet<_>>();
    for account_id in account_ids {
        // QualificationStarted is the financial-era boundary. Pre-Start live facts remain bound
        // by `start.live_prefix` but cannot authorize a wrapper or seed ordinary account state;
        // a post-Start Baseline is the reducer's sole Baseline for this era.
        let events = wrapper_events
            .iter()
            .filter(|event| event.account_id == account_id)
            .cloned()
            .collect::<Vec<_>>();
        let derived = crate::live_fanout::derive_projection_rows_with_sources(
            &account_id,
            &events,
            &source_envelopes,
        )
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "live journal strict reduction failed for {account_id}: {error}"
            ))
        })?;
        if let Some(missing) = derived.pending_approved_admission_keys.first() {
            return insufficient(format!(
                "live wrapper preimage is absent for account {account_id} decision {missing}"
            ));
        }

        for event in wrapper_events
            .iter()
            .filter(|event| event.account_id == account_id)
        {
            if let pe_execution_core::LiveJournalPayload::OrderPrepared(wrapper) = &event.payload {
                let key = &wrapper.identity.idempotency_key;
                if derived
                    .baseline_sequence
                    .is_some_and(|baseline_sequence| event.seq >= baseline_sequence)
                    && !derived.validated_prepared_sequences.contains(&event.seq)
                {
                    return insufficient(format!(
                        "live wrapper was not admitted by strict reduction for account {account_id} decision {key}"
                    ));
                }
                if derived
                    .baseline_sequence
                    .is_none_or(|baseline_sequence| event.seq < baseline_sequence)
                {
                    verify_paper_wrapper_admission(&account_id, &events, event, wrapper)?;
                }
                let projection = wrapper.identity.fill_projection.as_deref().ok_or_else(|| {
                        QualificationError::InsufficientEvidence(format!(
                            "live wrapper lacks paper-decision identity for account {account_id} decision {key}"
                        ))
                    })?;
                let source_trade_id = projection.source_trade_id.as_deref().ok_or_else(|| {
                        QualificationError::InsufficientEvidence(format!(
                            "live wrapper lacks source-trade identity for account {account_id} decision {key}"
                        ))
                    })?;
                let paper = paper_bases.get(source_trade_id).ok_or_else(|| {
                    QualificationError::InsufficientEvidence(format!(
                        "live wrapper references no paper Prepared decision {source_trade_id}"
                    ))
                })?;
                let expected_side = match paper.economic.market.side {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                };
                let paper_decision_id =
                    pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                        &TraderId(paper.operation.leader_wallet).to_string(),
                        &paper.operation.source_trade_id.0,
                        &paper.economic.market.market_id,
                        u16::from(paper.economic.market.outcome_index),
                        paper.economic.market.side,
                        paper.operation.observed_at_bucket,
                    );
                if wrapper.identity.dispatch_id != paper_decision_id
                    || wrapper.identity.idempotency_key
                        != pe_execution_core::LiveOrderIdentity::idempotency_key_for(
                            &paper_decision_id,
                            &account_id,
                        )
                    || projection.leader_wallet != paper.operation.leader_wallet.to_string()
                    || projection.market_id != paper.economic.market.market_id
                    || projection.outcome_id
                        != i64::from(u16::from(paper.economic.market.outcome_index))
                    || projection.side != expected_side
                {
                    return insufficient(format!(
                        "live wrapper identity differs from paper Prepared decision {source_trade_id}"
                    ));
                }
                let economic_core_hash = wrapper.economic.core_hash().map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "live economic core hash failed for decision {source_trade_id}: {error}"
                    ))
                })?;
                if wrapper.economic != paper.economic
                    || economic_core_hash != paper.economic_core_hash
                {
                    return insufficient(format!(
                        "live wrapper economic core differs from paper Prepared decision {source_trade_id}"
                    ));
                }
                let live_wrapper_hash = blake3::hash(&serde_json::to_vec(wrapper.as_ref())?)
                    .to_hex()
                    .to_string();
                if live_wrapper_hash == paper.wrapper_hash {
                    return insufficient(format!(
                        "live and paper wrapper hashes collide for decision {source_trade_id}"
                    ));
                }
                wrapper_facts.push(QualificationLiveWrapperFact {
                    account_id: account_id.as_str().to_owned(),
                    journal_sequence: event.seq,
                    source_trade_id: source_trade_id.to_owned(),
                    economic_core_hash,
                    paper_wrapper_hash: paper.wrapper_hash.clone(),
                    live_wrapper_hash,
                });
            }
        }
    }

    Ok(VerifiedLiveEvidence {
        prefix_hash: Some(start.live_prefix.last_hash.clone()),
        journal_hash: Some(live_prefix.last_hash.clone()),
        wrapper_facts,
    })
}

fn verify_paper_wrapper_admission(
    account_id: &AccountId,
    events: &[pe_execution_core::LiveJournalEvent],
    prepared_event: &pe_execution_core::LiveJournalEvent,
    wrapper: &pe_execution_core::LiveOrderPreparedAudit,
) -> Result<(), QualificationError> {
    let key = &wrapper.identity.idempotency_key;
    let admissions = events
        .iter()
        .filter_map(|event| match &event.payload {
            pe_execution_core::LiveJournalPayload::AdmissionEvaluated(admission)
                if admission.identity.idempotency_key == *key =>
            {
                Some((event, admission.as_ref()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let (admission_event, admission) = match admissions.as_slice() {
        [] => {
            return insufficient(format!(
                "paper-mode live wrapper has no AdmissionEvaluated preimage for account {account_id} decision {key}"
            ));
        }
        [admission] => *admission,
        _ => {
            return insufficient(format!(
                "paper-mode live wrapper has multiple AdmissionEvaluated preimages for account {account_id} decision {key}"
            ));
        }
    };
    if admission_event.seq >= prepared_event.seq {
        return insufficient(format!(
            "paper-mode AdmissionEvaluated does not precede Prepared for account {account_id} decision {key}"
        ));
    }
    if admission.identity != wrapper.identity {
        return insufficient(format!(
            "paper-mode AdmissionEvaluated identity differs from Prepared for account {account_id} decision {key}"
        ));
    }
    if admission.frozen_binding != wrapper.frozen_binding {
        return insufficient(format!(
            "paper-mode AdmissionEvaluated frozen binding differs from Prepared for account {account_id} decision {key}"
        ));
    }
    if admission.economic != wrapper.economic {
        return insufficient(format!(
            "paper-mode AdmissionEvaluated economic inputs differ from Prepared for account {account_id} decision {key}"
        ));
    }
    if admission.verdict != LiveAdmissionVerdict::Approved {
        return insufficient(format!(
            "paper-mode AdmissionEvaluated verdict is not Approved for account {account_id} decision {key}"
        ));
    }

    let reproduced = classify_live_admission(LiveAdmissionClassificationInput {
        evaluated_at: admission_event.timestamp,
        requested_mode: admission.requested_mode,
        effective_mode: admission.effective_mode,
        frozen_binding: &admission.frozen_binding,
        current_binding: &admission.current_binding,
        identity: &wrapper.identity,
        condition_id: &wrapper.economic.market.condition_id,
        outcome_id: OutcomeId(u16::from(wrapper.economic.market.outcome_index)),
        token_id: &wrapper.economic.market.token_id,
        admission: &wrapper.economic.admission,
        ladder: &wrapper.economic.ladder,
        economic: &wrapper.economic,
        account: LiveAdmissionAccountEvidence::NotRead,
    });
    match reproduced {
        Err(LiveAdmissionNeedsAccountState) => {}
        Ok(verdict) => {
            return insufficient(format!(
                "paper-mode pre-account admission replay did not require account state for account {account_id} decision {key}: {verdict:?}"
            ));
        }
    }
    Ok(())
}

fn replay_live_prefix(
    live_journal: &Path,
    start_prefix: &TailBinding,
    sealed_prefix: &TailBinding,
) -> Result<Vec<pe_execution_core::LiveJournalEvent>, QualificationError> {
    let Some(sealed_sequence) = sealed_prefix.last_sequence else {
        return Ok(Vec::new());
    };
    let events = pe_execution_core::LiveJournal::replay_prefix(live_journal, sealed_sequence)
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "live journal canonical replay failed: {error}"
            ))
        })?;
    let events = events
        .into_iter()
        .filter(|event| {
            start_prefix
                .last_sequence
                .is_none_or(|start_sequence| event.seq > start_sequence.0)
        })
        .collect::<Vec<_>>();
    if events
        .last()
        .map(|event| EventSeq(event.seq))
        .or(start_prefix.last_sequence)
        != Some(sealed_sequence)
    {
        return insufficient("live journal replay does not reach its sealed prefix");
    }
    Ok(events)
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

fn verify_tail_extension(
    start: &TailBinding,
    sealed: &TailBinding,
    label: &str,
) -> Result<(), QualificationError> {
    tail_hash(start)?;
    tail_hash(sealed)?;
    let sequence_extends = match (start.last_sequence, sealed.last_sequence) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(start), Some(sealed)) => sealed >= start,
    };
    if sealed.physical_tail < start.physical_tail || !sequence_extends {
        return insufficient(format!(
            "QualificationSealed {label} prefix precedes its Start prefix"
        ));
    }
    if start.last_sequence == sealed.last_sequence && start != sealed {
        return insufficient(format!(
            "QualificationSealed {label} prefix changes its Start boundary"
        ));
    }
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
    let history = state.decision_pending_history()?;
    let source_universe = source_trade_universe(sealed_sequence, observations)?;
    let history_states = history
        .iter()
        .map(|row| (row.source_trade_id.clone(), row.state))
        .collect::<HashMap<_, _>>();
    for (source_trade_id, first_sequence) in &source_universe {
        if start_sequence.is_none_or(|start| *first_sequence > start.0)
            && history_states.get(source_trade_id) != Some(&DecisionPendingState::Terminal)
        {
            return insufficient(format!(
                "source-log trade {source_trade_id} has no terminal decision_pending row"
            ));
        }
    }
    let complete_reads = complete_activity_read_scopes(&history, sealed_sequence, observations)?;
    let required = decision_keys_from_source_observations(
        start_sequence,
        sealed_sequence,
        observations,
        &source_universe,
        &complete_reads,
    )?;
    let required_by_trade = required
        .iter()
        .map(|(_, source_trade_id, semantic_revision)| {
            (source_trade_id.clone(), semantic_revision.clone())
        })
        .collect::<HashMap<_, _>>();
    let mut selected_by_trade = HashMap::new();
    for row in history {
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
        let Some(required_revision) = required_by_trade.get(&row.source_trade_id) else {
            // Complete reads may straddle Start, and their continuations therefore retain
            // pre-Start page receipts. Membership is determined from this trade's first raw
            // observation, not from the oldest unrelated page in its complete read.
            continue;
        };
        if required_revision != &row.semantic_revision {
            return insufficient(format!(
                "decision_pending row {}/{} is additional to the parsed source prefix",
                row.source_trade_id, row.semantic_revision
            ));
        }
        if selected_by_trade
            .insert(row.source_trade_id.clone(), row)
            .is_some()
        {
            return insufficient("sealed decision evidence repeats a source trade identity");
        }
    }
    required
        .into_iter()
        .map(|(_, source_trade_id, semantic_revision)| {
            selected_by_trade.remove(&source_trade_id).ok_or_else(|| {
                QualificationError::InsufficientEvidence(format!(
                    "parsed source trade {source_trade_id}/{semantic_revision} has no decision_pending row"
                ))
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
struct CompleteActivityReadScope {
    continuation: DecisionContinuationV3,
    decisions: HashMap<pe_core_types::SourceTradeId, Option<AppendReceipt>>,
}

fn complete_activity_read_scopes(
    rows: &[DecisionPendingRow],
    sealed_sequence: EventSeq,
    observations: &BTreeMap<u64, SourceObservation>,
) -> Result<Vec<CompleteActivityReadScope>, QualificationError> {
    let mut reads = Vec::<CompleteActivityReadScope>::new();
    for row in rows {
        let continuation = DecisionContinuationV3::from_durable(row).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision source receipt link is invalid: {error}"
            ))
        })?;
        if continuation.page_occurrences().is_empty() {
            continue;
        }
        if continuation
            .page_occurrences()
            .iter()
            .any(|page| page.receipt.sequence > sealed_sequence)
        {
            continue;
        }
        if continuation
            .page_occurrences()
            .iter()
            .map(|page| page.receipt)
            .chain(
                continuation
                    .observed_source_receipt
                    .filter(|receipt| receipt.sequence <= sealed_sequence),
            )
            .any(|receipt| {
                observations
                    .get(&receipt.sequence.0)
                    .is_none_or(|observation| observation.receipt != receipt)
            })
        {
            return insufficient("decision source receipt does not match the sealed source prefix");
        }
        if let Some(existing) = reads
            .iter_mut()
            .find(|read| read.continuation.page_occurrences() == continuation.page_occurrences())
        {
            if existing.continuation.facts.wallet != continuation.facts.wallet
                || existing.continuation.facts.decision_inputs != continuation.facts.decision_inputs
            {
                return insufficient(
                    "decision continuations disagree about one complete activity read",
                );
            }
            if existing
                .decisions
                .insert(
                    row.source_trade_id.clone(),
                    continuation.observed_source_receipt,
                )
                .is_some()
            {
                return insufficient("complete activity read repeats a decision identity");
            }
            continue;
        }
        if reads.iter().any(|read| {
            read.continuation.page_occurrences().iter().any(|existing| {
                continuation
                    .page_occurrences()
                    .iter()
                    .any(|candidate| candidate.receipt.sequence == existing.receipt.sequence)
            })
        }) {
            return insufficient("complete activity reads overlap source page receipts");
        }
        let observed_source_receipt = continuation.observed_source_receipt;
        reads.push(CompleteActivityReadScope {
            continuation,
            decisions: HashMap::from([(row.source_trade_id.clone(), observed_source_receipt)]),
        });
    }
    reads.sort_by_key(|read| {
        read.continuation
            .page_occurrences()
            .last()
            .map_or(0, |page| page.receipt.sequence.0)
    });
    Ok(reads)
}

/// Source-only, fail-closed trade membership from every raw REST activity page in the prefix.
/// Event envelopes retain payloads and receipts but not request URLs or bounds, so exact page
/// grouping remains validated from V3 continuations after this projection-independent check.
fn source_trade_universe(
    sealed_sequence: EventSeq,
    observations: &BTreeMap<u64, SourceObservation>,
) -> Result<HashMap<pe_core_types::SourceTradeId, u64>, QualificationError> {
    let mut first = HashMap::new();
    for observation in observations.values().filter(|observation| {
        observation.receipt.sequence <= sealed_sequence
            && observation.source_id == crate::trade_poller::ACTIVITY_POLL_SOURCE_ID
    }) {
        validate_activity_page_contract(observation)?;
        let raw_rows: Vec<Box<serde_json::value::RawValue>> =
            serde_json::from_slice(&observation.payload).map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "activity observation JSON failed: {error}"
                ))
            })?;
        let context = ActivityParseContext {
            source_id: SourceId(observation.source_id.clone()),
            observed_at: observation.observed_at.clone(),
            received_at: observation.received_at.clone(),
            transport: ActivityTransport::Replay,
        };
        for raw in raw_rows {
            let row =
                parse_activity_row(raw.get().as_bytes(), None, &context).map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "activity observation parse failed: {error}"
                    ))
                })?;
            let group_id = row.group_id().map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "activity observation identity failed: {error}"
                ))
            })?;
            if group_id.components().activity_type == ActivityType::Trade {
                first
                    .entry(group_id.key().clone())
                    .and_modify(|sequence: &mut u64| {
                        *sequence = (*sequence).min(observation.receipt.sequence.0);
                    })
                    .or_insert(observation.receipt.sequence.0);
            }
        }
    }
    Ok(first)
}

fn validate_activity_page_contract(
    observation: &SourceObservation,
) -> Result<(), QualificationError> {
    if observation.schema_version != pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
        || observation.parser_version != pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
        || observation.content_type != ContentType::Json
    {
        return insufficient("activity observation has the wrong source contract");
    }
    Ok(())
}

/// Reconstruct each immutable fixed-end activity read before deriving composite decision keys.
/// Page occurrences retain multiplicity, saturated parent segments contribute no production rows,
/// and a group's earliest raw receipt decides whether its production aggregate predates Start.
fn decision_keys_from_source_observations(
    start_sequence: Option<EventSeq>,
    sealed_sequence: EventSeq,
    observations: &BTreeMap<u64, SourceObservation>,
    source_universe: &HashMap<pe_core_types::SourceTradeId, u64>,
    complete_reads: &[CompleteActivityReadScope],
) -> Result<Vec<(u64, pe_core_types::SourceTradeId, String)>, QualificationError> {
    let in_prefix = complete_reads
        .iter()
        .filter(|read| {
            read.continuation
                .page_occurrences()
                .iter()
                .all(|page| page.receipt.sequence <= sealed_sequence)
        })
        .collect::<Vec<_>>();
    let candidates = in_prefix
        .iter()
        .flat_map(|read| {
            read.decisions
                .iter()
                .filter_map(|(source_trade_id, observed)| {
                    observed
                        .is_none_or(|receipt| receipt.sequence <= sealed_sequence)
                        .then_some(source_trade_id.clone())
                })
        })
        .collect::<HashSet<_>>();
    let mut first_observations = source_universe.clone();
    for read in &in_prefix {
        for (source_trade_id, receipt) in &read.decisions {
            let Some(receipt) = receipt.filter(|receipt| receipt.sequence <= sealed_sequence)
            else {
                continue;
            };
            let observation = decision_source_receipt(observations, receipt)?;
            let activity =
                parse_activity_trade_observation(&observation.payload).map_err(|_| {
                    QualificationError::InsufficientEvidence(format!(
                        "decision websocket observation {} is invalid",
                        receipt.sequence.0
                    ))
                })?;
            if observation.source_id != crate::activity_ingest::ACTIVITY_WS_SOURCE_ID
                || observation.schema_version
                    != pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
                || observation.parser_version
                    != pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
                || activity.group_id.key() != source_trade_id
            {
                return insufficient(
                    "decision websocket observation has the wrong source contract",
                );
            }
            first_observations
                .entry(source_trade_id.clone())
                .and_modify(|sequence| *sequence = (*sequence).min(receipt.sequence.0))
                .or_insert(receipt.sequence.0);
        }
    }

    let mut revisions = HashMap::<pe_core_types::SourceTradeId, String>::new();
    for read in in_prefix {
        let mut lookup = |receipt| -> Result<_, QualificationError> {
            let observation = decision_source_receipt(observations, receipt)?;
            Ok(CompleteActivityPage {
                payload: observation.payload.clone(),
                observed_at: observation.observed_at.clone(),
                received_at: observation.received_at.clone(),
                source_id: observation.source_id.clone(),
                schema_version: observation.schema_version,
                parser_version: observation.parser_version,
                content_type: observation.content_type.clone(),
            })
        };
        let aggregates = read
            .continuation
            .reconstruct_complete_activity_read(&mut lookup)
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "complete activity read reconstruction failed: {error}"
                ))
            })?;
        for aggregate in aggregates {
            let source_trade_id = aggregate.group_id.key().clone();
            if aggregate.group_id.components().activity_type != ActivityType::Trade
                || !candidates.contains(&source_trade_id)
            {
                continue;
            }
            let semantic_revision = aggregate.semantic_revision.as_str().to_owned();
            match revisions.entry(source_trade_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(semantic_revision);
                }
                std::collections::hash_map::Entry::Occupied(entry)
                    if entry.get() != &semantic_revision =>
                {
                    return insufficient(format!(
                        "source trade {source_trade_id} has multiple semantic revisions in the sealed prefix"
                    ));
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
    }
    for source_trade_id in &candidates {
        if !revisions.contains_key(source_trade_id) {
            return insufficient(format!(
                "decision source trade {source_trade_id} is absent from its complete activity read"
            ));
        }
    }
    let mut ordered = revisions
        .into_iter()
        .filter_map(|(source_trade_id, semantic_revision)| {
            let receipt_sequence = first_observations.get(&source_trade_id).copied()?;
            (start_sequence.is_none_or(|start| receipt_sequence > start.0)).then_some((
                receipt_sequence,
                source_trade_id,
                semantic_revision,
            ))
        })
        .collect::<Vec<_>>();
    ordered
        .sort_by(|left, right| (left.0, &left.1.0, &left.2).cmp(&(right.0, &right.1.0, &right.2)));
    Ok(ordered)
}

fn verify_initial_membership(start: &QualificationStarted) -> Result<(), QualificationError> {
    if start.financial_semantic_version != FINANCIAL_SEMANTIC_VERSION {
        return insufficient(format!(
            "QualificationStarted financial semantic version {} differs from verifier version {FINANCIAL_SEMANTIC_VERSION}",
            start.financial_semantic_version
        ));
    }
    MembershipProofBinding::decode_and_verify(&start.membership_proofs_hash, &start.membership)
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "QualificationStarted immutable membership proof is invalid: {error}"
            ))
        })?;
    Ok(())
}

fn verify_decision_configurations(
    decisions: &[crate::decision_replay::ReplayedDecision],
    start: &QualificationStarted,
) -> Result<(), QualificationError> {
    for decision in decisions {
        if decision.continuation.facts.applied_configuration_hash != start.hot_config_hash
            || decision.post_boundary.financial_semantic_version != start.financial_semantic_version
        {
            return insufficient(format!(
                "decision {} configuration or financial semantics differ from QualificationStarted",
                decision.continuation.facts.source_trade_id
            ));
        }
    }
    Ok(())
}

struct MembershipEvidenceContext<'a> {
    source: &'a BTreeMap<u64, SourceObservation>,
    current_membership: &'a HashSet<pe_core_types::WalletAddress>,
}

fn verify_membership_change_evidence(
    reason: MembershipReason,
    removed: &[pe_core_types::WalletAddress],
    added: &[pe_core_types::WalletAddress],
    capacity: usize,
    ranking_batch_id: Option<i64>,
    evidence: &serde_json::Value,
    context: &MembershipEvidenceContext<'_>,
) -> Result<Vec<pe_trader_index::WatchlistEntry>, QualificationError> {
    let MembershipEvidenceContext {
        source,
        current_membership,
    } = context;
    let evidence: SealedMembershipEvidence =
        serde_json::from_value(evidence.clone()).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "MembershipChanged evidence schema is invalid: {error}"
            ))
        })?;
    let replacements = match (&evidence, reason) {
        (
            SealedMembershipEvidence::FullRerank {
                ranking_receipt,
                admission_receipts,
            },
            MembershipReason::FullRerank,
        ) => {
            let Some(ranking_batch_id) = ranking_batch_id else {
                return insufficient("ranking membership change lacks a ranking batch identity");
            };
            let artifact: RankingMembershipArtifact =
                membership_artifact(source, *ranking_receipt, RANKING_MEMBERSHIP_SOURCE_ID)?;
            if artifact.batch_id != Some(ranking_batch_id) {
                return insufficient(
                    "MembershipChanged ranking receipt names a different published batch",
                );
            }
            verify_ranked_change(
                current_membership,
                removed,
                added,
                capacity,
                &artifact.entries,
            )?;
            verify_admission_receipts(added, admission_receipts, source)?;
            artifact.entries
        }
        (
            SealedMembershipEvidence::KnockoutBackfill {
                evictions,
                ranking_receipt,
                admission_receipts,
            },
            MembershipReason::KnockoutInactivity
            | MembershipReason::KnockoutInactivityHardCap
            | MembershipReason::KnockoutUnderperformance,
        ) => {
            let mut proved = HashSet::new();
            for eviction in evictions {
                if !removed.contains(&eviction.wallet) || !proved.insert(eviction.wallet) {
                    return insufficient(
                        "MembershipChanged knockout evidence does not prove its removed wallets",
                    );
                }
                let artifact: KnockoutCausalArtifact = membership_artifact(
                    source,
                    eviction.causal_receipt,
                    KNOCKOUT_CAUSAL_SOURCE_ID,
                )?;
                if artifact.wallet != eviction.wallet {
                    return insufficient(
                        "MembershipChanged knockout receipt names a different wallet",
                    );
                }
                let fills = artifact
                    .fills
                    .iter()
                    .map(crate::paper_recovery::KnockoutFillArtifact::to_row)
                    .collect::<Vec<_>>();
                let settlements = artifact
                    .settlements
                    .iter()
                    .map(crate::paper_recovery::KnockoutSettlementArtifact::to_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        QualificationError::InsufficientEvidence(format!(
                            "MembershipChanged knockout settlement encoding is invalid: {error}"
                        ))
                    })?;
                verify_knockout_causal_inputs(eviction.wallet, &fills, &settlements)?;
                let resolutions = replay_membership_resolutions(&settlements)?;
                let stats = crate::demotion_stat::wallet_edge_stats(
                    &fills,
                    &resolutions,
                    artifact.demotion_cb_alpha,
                    artifact.evaluated_at_unix,
                    artifact.demotion_pnl_window_secs,
                );
                let config = MaintenanceConfig {
                    interval_secs: 0,
                    inactivity_threshold_secs: artifact.inactivity_threshold_secs,
                    inactivity_hard_cap_secs: artifact.inactivity_hard_cap_secs,
                    demotion_min_trades: artifact.demotion_min_trades,
                    demotion_cb_alpha: artifact.demotion_cb_alpha,
                    demotion_pnl_window_secs: artifact.demotion_pnl_window_secs,
                    bench_overfetch: 0,
                    membership_mode: MembershipMode::Knockout,
                };
                let rederived = knockout_decision(
                    artifact.last_trade_unix,
                    stats.get(&eviction.wallet.to_string()),
                    &config,
                    artifact.evaluated_at_unix,
                )
                .map(MembershipReason::from);
                if rederived != Some(eviction.reason) {
                    return insufficient(
                        "MembershipChanged knockout semantic owner rejects its causal inputs",
                    );
                }
            }
            if proved.len() != removed.len() {
                return insufficient(
                    "MembershipChanged knockout evidence omits a removed wallet statistic",
                );
            }
            if !evictions.is_empty() && knockout_record_reason(evictions) != Some(reason) {
                return insufficient(
                    "MembershipChanged knockout reason differs from its proved evictions",
                );
            }
            let candidates = match ranking_receipt {
                Some(receipt) => {
                    let artifact: RankingMembershipArtifact =
                        membership_artifact(source, *receipt, RANKING_MEMBERSHIP_SOURCE_ID)?;
                    if artifact.batch_id != ranking_batch_id {
                        return insufficient(
                            "MembershipChanged knockout ranking receipt names a different batch",
                        );
                    }
                    artifact.entries
                }
                None => Vec::new(),
            };
            let removed_set = removed.iter().copied().collect::<HashSet<_>>();
            let expected =
                planned_admission_wallets(current_membership, &removed_set, &candidates, capacity)
                    .into_iter()
                    .collect::<HashSet<_>>();
            if expected != added.iter().copied().collect() {
                return insufficient(
                    "MembershipChanged knockout candidate receipt disagrees with its additions",
                );
            }
            verify_admission_receipts(added, admission_receipts, source)?;
            candidates
        }
        (
            SealedMembershipEvidence::CapacityChange {
                generation,
                config_receipt,
                admission_receipts,
            },
            MembershipReason::CapacityChange,
        ) => {
            let artifact: CapacityMembershipArtifact =
                membership_artifact(source, *config_receipt, CAPACITY_CONFIG_SOURCE_ID)?;
            if *generation == 0
                || artifact.generation != *generation
                || usize::try_from(artifact.target) != Ok(capacity)
            {
                return insufficient(
                    "MembershipChanged capacity receipt differs from generation or target",
                );
            }
            if ranking_batch_id.is_some() {
                return insufficient(
                    "MembershipChanged capacity change unexpectedly names a batch",
                );
            }
            verify_ranked_change(
                current_membership,
                removed,
                added,
                capacity,
                &artifact.published_entries,
            )?;
            verify_admission_receipts(added, admission_receipts, source)?;
            artifact.published_entries
        }
        _ => return insufficient("MembershipChanged evidence kind does not match its reason"),
    };
    Ok(replacements)
}

fn membership_artifact<T: serde::de::DeserializeOwned>(
    source: &BTreeMap<u64, SourceObservation>,
    receipt: AppendReceipt,
    expected_source_id: &str,
) -> Result<T, QualificationError> {
    let observation = source
        .get(&receipt.sequence.0)
        .filter(|observation| observation.receipt == receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "MembershipChanged artifact receipt is absent from the sealed source prefix"
                    .to_owned(),
            )
        })?;
    if observation.source_id != expected_source_id
        || observation.schema_version != MEMBERSHIP_ARTIFACT_SCHEMA_VERSION
        || observation.parser_version != MEMBERSHIP_ARTIFACT_PARSER_VERSION
        || observation.content_type != ContentType::Json
    {
        return insufficient("MembershipChanged artifact receipt has the wrong envelope identity");
    }
    serde_json::from_slice(&observation.payload).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "MembershipChanged artifact payload is invalid: {error}"
        ))
    })
}

fn verify_ranked_change(
    current: &HashSet<pe_core_types::WalletAddress>,
    removed: &[pe_core_types::WalletAddress],
    added: &[pe_core_types::WalletAddress],
    capacity: usize,
    entries: &[pe_trader_index::WatchlistEntry],
) -> Result<(), QualificationError> {
    let entry_wallets = entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();
    if entry_wallets.len() != entries.len() || entries.len() > capacity {
        return insufficient("MembershipChanged ranked artifact is duplicate or over-capacity");
    }
    let current_ordered: Vec<pe_core_types::WalletAddress> = current.iter().copied().collect();
    let (expected_removed, expected_added) =
        ranked_membership_change_wallets(&current_ordered, entries, capacity);
    let expected_removed: HashSet<pe_core_types::WalletAddress> =
        expected_removed.into_iter().collect();
    let expected_added: HashSet<pe_core_types::WalletAddress> =
        expected_added.into_iter().collect();
    if expected_removed != removed.iter().copied().collect::<HashSet<_>>()
        || expected_added != added.iter().copied().collect::<HashSet<_>>()
    {
        return insufficient(
            "MembershipChanged ranked artifact disagrees with its exact wallet mutation",
        );
    }
    Ok(())
}

fn verify_admission_receipts(
    added: &[pe_core_types::WalletAddress],
    receipts: &[MembershipAdmissionReceipt],
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<(), QualificationError> {
    let mut proved = HashSet::new();
    for proof_receipt in receipts {
        if !added.contains(&proof_receipt.wallet) || !proved.insert(proof_receipt.wallet) {
            return insufficient(
                "MembershipChanged admission receipts repeat or name an unadded wallet",
            );
        }
        let artifact: MembershipAdmissionArtifact = membership_artifact(
            source,
            proof_receipt.receipt,
            MEMBERSHIP_ADMISSION_SOURCE_ID,
        )?;
        if artifact.wallet != proof_receipt.wallet {
            return insufficient("MembershipChanged admission receipt names a different wallet");
        }
        artifact
            .proof
            .verify(&[proof_receipt.wallet])
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "MembershipChanged admission proof is invalid: {error}"
                ))
            })?;
    }
    if proved != added.iter().copied().collect() {
        return insufficient("MembershipChanged omits an added wallet admission receipt");
    }
    Ok(())
}

fn replay_membership_resolutions(
    settlements: &[SettledMarketRow],
) -> Result<ResolutionStore, QualificationError> {
    let state = std::sync::Arc::new(PaperStateDb::open(Path::new(":memory:"))?);
    for settlement in settlements {
        state.record_settled_market(
            &settlement.market_id,
            &settlement.outcome_prices_json,
            settlement.credit_applied,
            settlement.settled_at_unix,
        )?;
    }
    ResolutionStore::load(state).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "MembershipChanged settlement replay failed: {error}"
        ))
    })
}

fn knockout_record_reason(
    evidence: &[crate::paper_recovery::SealedKnockoutEvidence],
) -> Option<MembershipReason> {
    evidence
        .iter()
        .map(|eviction| eviction.reason)
        .find(|reason| *reason == MembershipReason::KnockoutUnderperformance)
        .or_else(|| {
            evidence
                .iter()
                .map(|eviction| eviction.reason)
                .find(|reason| *reason == MembershipReason::KnockoutInactivityHardCap)
        })
        .or_else(|| evidence.first().map(|eviction| eviction.reason))
}

/// Immutable source-log view shared by every membership record during one boot replay.
pub(crate) struct PublishedMembershipSource {
    observations: BTreeMap<u64, SourceObservation>,
}

impl PublishedMembershipSource {
    pub(crate) fn scan(source_log: &Path) -> Result<Self, QualificationError> {
        let verified_prefix = TailBinding::from(&Scanner::verify(source_log)?);
        Ok(Self {
            observations: source_observations(source_log, &verified_prefix)?,
        })
    }
}

/// Verify one synchronized production membership record against one immutable, verified
/// source-log view and return the exact replacement vector retained by its receipt-bound artifact.
pub(crate) fn replay_published_membership_change(
    record: &PaperLogRecord,
    source: &PublishedMembershipSource,
    current_membership: &HashSet<pe_core_types::WalletAddress>,
) -> Result<Vec<pe_trader_index::WatchlistEntry>, QualificationError> {
    let PaperLogRecord::MembershipChanged {
        reason,
        removed,
        added,
        capacity,
        ranking_batch_id,
        evidence,
        ..
    } = record
    else {
        return insufficient("published record is not MembershipChanged");
    };
    verify_membership_change_evidence(
        *reason,
        removed,
        added,
        *capacity,
        *ranking_batch_id,
        evidence,
        &MembershipEvidenceContext {
            source: &source.observations,
            current_membership,
        },
    )
}

#[cfg(test)]
pub(crate) fn verify_published_membership_change(
    record: &PaperLogRecord,
    source_log: &Path,
    current_membership: &HashSet<pe_core_types::WalletAddress>,
) -> Result<(), QualificationError> {
    let source = PublishedMembershipSource::scan(source_log)?;
    replay_published_membership_change(record, &source, current_membership).map(drop)
}

fn verify_knockout_causal_inputs(
    wallet: pe_core_types::WalletAddress,
    fills: &[FillRow],
    settlements: &[SettledMarketRow],
) -> Result<(), QualificationError> {
    let wallet_hex = wallet.to_string();
    let mut fill_keys = HashSet::new();
    let mut settlement_markets = HashSet::new();
    for settlement in settlements {
        if !settlement_markets.insert(settlement.market_id.clone()) {
            return insufficient("MembershipChanged knockout settlements repeat a market");
        }
    }
    for fill in fills {
        if !fill_keys.insert(fill.idempotency_key.clone())
            || crate::paper_api::ParsedKey::from_key(&fill.idempotency_key)
                .leader
                .as_deref()
                != Some(&wallet_hex)
            || !settlement_markets.contains(&fill.market_id)
        {
            return insufficient(
                "MembershipChanged knockout causal fills are duplicated, foreign, or unsettled",
            );
        }
    }
    if settlements.iter().any(|settlement| {
        !fills
            .iter()
            .any(|fill| fill.market_id == settlement.market_id)
    }) {
        return insufficient("MembershipChanged knockout includes an unused settlement");
    }
    Ok(())
}

fn verify_decision_source_inputs(
    decision: &crate::decision_replay::ReplayedDecision,
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<pe_execution_core::ObservationEvidence, QualificationError> {
    let continuation = &decision.continuation;
    let frozen = &continuation.facts;
    let observation = decision_observation_from_source(continuation, source)?.ok_or_else(|| {
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

/// Rebuild the exact pre-second leader ledger from its latest causal position anchor and the
/// append-only applied activity effects, then invoke the same complete-second classifier as the
/// runtime bucket owner. The entry-history arguments cannot affect `TradeDecision::action`; the
/// durable continuation exists only for a gate-admitted entry, so this verifier compares the
/// classifier-owned action and immutable trade facts rather than trusting that recorded label.
fn verify_decision_classification(
    state: &PaperStateDb,
    decision: &crate::decision_replay::ReplayedDecision,
) -> Result<(), QualificationError> {
    let frozen = &decision.continuation.facts;
    let anchors = state.position_anchors(&frozen.wallet)?;
    let anchor = anchors
        .iter()
        .rev()
        .find(|anchor| anchor.activity_cutoff_unix < frozen.source_epoch)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "decision {} has no causal position anchor",
                frozen.source_trade_id
            ))
        })?;
    let balances: Vec<(String, u16, ShareAmount)> = serde_json::from_str(&anchor.balances_json)
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision {} position anchor is malformed: {error}",
                frozen.source_trade_id
            ))
        })?;
    let mut positions = HashMap::new();
    for (market_id, outcome_id, long_contracts) in balances {
        if positions
            .insert(
                MarketOutcomeId::new(MarketId(VenueMarketId(market_id)), OutcomeId(outcome_id)),
                PositionState {
                    long_contracts,
                    short_contracts: ShareAmount::ZERO,
                },
            )
            .is_some()
        {
            return insufficient(format!(
                "decision {} position anchor repeats a market outcome",
                frozen.source_trade_id
            ));
        }
    }
    let mut ledger = PositionLedger::new();
    ledger.replace_wallet_snapshot(frozen.wallet, positions);
    let capture =
        crate::position_seeder::ledger_capture(&ledger, state, frozen.wallet).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision {} position anchor hash failed: {error}",
                frozen.source_trade_id
            ))
        })?;
    if capture.hash != anchor.ledger_hash_after {
        return insufficient(format!(
            "decision {} position anchor hash differs from its balances",
            frozen.source_trade_id
        ));
    }

    let groups = state.activity_groups_after(&frozen.wallet, anchor.activity_cutoff_unix)?;
    let mut bucket_start = 0usize;
    while let Some(first) = groups.get(bucket_start) {
        if first.source_epoch > frozen.source_epoch {
            break;
        }
        let mut bucket_end = bucket_start.saturating_add(1);
        while groups
            .get(bucket_end)
            .is_some_and(|group| group.source_epoch == first.source_epoch)
        {
            bucket_end = bucket_end.saturating_add(1);
        }
        let (mutations, expected) = recorded_applied_bucket(
            frozen.wallet,
            first.source_epoch,
            &groups[bucket_start..bucket_end],
        )?;
        if first.source_epoch < frozen.source_epoch {
            let actual = ledger.apply_all_or_none(&mutations).map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "decision {} causal ledger replay failed: {error}",
                    frozen.source_trade_id
                ))
            })?;
            if actual != expected {
                return insufficient(format!(
                    "decision {} causal ledger effects differ from the durable history",
                    frozen.source_trade_id
                ));
            }
        } else {
            return verify_complete_second_action(
                &ledger,
                &decision.continuation,
                &mutations,
                &expected,
            );
        }
        bucket_start = bucket_end;
    }
    insufficient(format!(
        "decision {} is absent from its causal activity second",
        frozen.source_trade_id
    ))
}

fn recorded_applied_bucket(
    wallet: pe_core_types::WalletAddress,
    source_epoch: i64,
    groups: &[pe_paper_state::ActivityGroupRow],
) -> Result<(Vec<LedgerMutation>, Vec<AppliedEffect>), QualificationError> {
    let source_time = OffsetDateTime::from_unix_timestamp(source_epoch).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "causal activity bucket has invalid source epoch: {error}"
        ))
    })?;
    let mut mutations = Vec::new();
    let mut expected = Vec::new();
    for group in groups {
        if !recorded_group_was_applied(&group.source_trade_id, &group.disposition)? {
            continue;
        }
        let applied = AppliedEffect::from_document(&group.proof_json).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "causal activity group {} effect document is invalid: {error}",
                group.source_trade_id
            ))
        })?;
        mutations.push(LedgerMutation {
            source_trade_id: group.source_trade_id.clone(),
            // The classifier does not consume transaction hashes. The durable semantic effect is
            // the replay owner, matching the service's restart reconstruction.
            transaction_hash: group.source_trade_id.0.clone(),
            wallet,
            source_time: SourceTimestamp(source_time),
            effect: applied.effect.clone(),
        });
        expected.push(applied);
    }
    Ok((mutations, expected))
}

fn recorded_group_was_applied(
    source_trade_id: &pe_core_types::SourceTradeId,
    disposition: &str,
) -> Result<bool, QualificationError> {
    if matches!(
        disposition,
        "applied"
            | "wallet_fenced_applied"
            | "decision_pending"
            | "not_copy_eligible"
            | "not_an_entry"
            | "not_first_entry"
            | "not_buy"
            | "wallet_history_incomplete"
            | "ambiguous_first_entry_same_second"
            | "order_dependent_equal_second_action"
            | "stale_fallback_past_copy_budget"
            | "stale_activity_ws_past_copy_budget"
    ) {
        return Ok(true);
    }
    if matches!(
        disposition,
        "raw_only"
            | "reanchor_required_redemption"
            | "reanchor_required_late_group"
            | "anchor_covered"
            | "anchor_covered_late"
            | "wallet_fenced"
            | "revised_applied_aggregate"
            | "late_group_after_bucket_commit"
            | "invalid_mapping"
            | "position_underflow"
            | "position_overflow"
            | "conversion_unknown_conditions"
            | "unknown_activity_effect"
            | "order_dependent_equal_second"
    ) {
        return Ok(false);
    }
    insufficient(format!(
        "causal activity group {source_trade_id} has unknown disposition {disposition}"
    ))
}

fn verify_complete_second_action(
    ledger: &PositionLedger,
    continuation: &DecisionContinuationV3,
    mutations: &[LedgerMutation],
    expected: &[AppliedEffect],
) -> Result<(), QualificationError> {
    let frozen = &continuation.facts;
    let verdict = classify_complete_second(
        ledger,
        frozen.wallet,
        mutations,
        frozen.reconstruction_quality,
        &SignalConfig::default(),
        true,
        &|_| false,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "decision {} complete-second classification failed: {error}",
            frozen.source_trade_id
        ))
    })?;
    let SecondVerdict::OrderIndependent {
        applied, decisions, ..
    } = verdict
    else {
        return insufficient(format!(
            "decision {} complete second is order-dependent",
            frozen.source_trade_id
        ));
    };
    if applied != expected {
        return insufficient(format!(
            "decision {} complete-second effects differ from durable history",
            frozen.source_trade_id
        ));
    }
    let mut matching = decisions
        .iter()
        .filter(|candidate| candidate.source_trade_id == frozen.source_trade_id);
    let classified = matching.next().ok_or_else(|| {
        QualificationError::InsufficientEvidence(format!(
            "decision {} is absent from complete-second classification",
            frozen.source_trade_id
        ))
    })?;
    let expected_confidence =
        ProbabilityPpm(u32::from(frozen.reconstruction_quality.get()).saturating_mul(10_000));
    if matching.next().is_some()
        || classified.market_id != frozen.market_id
        || classified.outcome_id != frozen.outcome_id
        || classified.side != frozen.side
        || classified.amount != frozen.share_amount
        || classified.price != frozen.price
        || classified.action != frozen.pre_bucket_action
        || classified.action_order_dependent
        || frozen.action_confidence_ppm != expected_confidence
    {
        return insufficient(format!(
            "decision {} classification differs from complete-second replay",
            frozen.source_trade_id
        ));
    }
    Ok(())
}

fn received_unix_ms(envelope: &pe_event_log::EventEnvelope) -> Result<i64, QualificationError> {
    let millis = envelope.received_at.0.unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).map_err(|_| {
        QualificationError::InsufficientEvidence("event timestamp milliseconds overflow".to_owned())
    })
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

impl From<RiskPriceReplayError> for QualificationError {
    fn from(error: RiskPriceReplayError) -> Self {
        match error {
            RiskPriceReplayError::Unavailable(cause) => {
                Self::InsufficientEvidence(format!("risk price reconstruction failed: {cause}"))
            }
            RiskPriceReplayError::Insufficient(reason) => Self::InsufficientEvidence(reason),
        }
    }
}

async fn replayed_risk_prices(
    price_receipts: &[AppendReceipt],
    evaluated_at_unix_ms: i64,
    positions: &[PaperPositionRow],
    source: &BTreeMap<u64, SourceObservation>,
) -> Result<HashMap<(MarketId, OutcomeId), Price>, RiskPriceReplayError> {
    let ids = positions
        .iter()
        .map(|position| MarketOutcomeId::new(position.market_id.clone(), position.outcome_id))
        .collect::<Vec<_>>();
    replay_strict_risk_prices(&ids, price_receipts, evaluated_at_unix_ms, |receipt| {
        let observation = source
            .get(&receipt.sequence.0)
            .filter(|observation| observation.receipt == receipt)
            .ok_or_else(|| {
                RiskPriceReplayError::Insufficient(
                    "risk price receipt is absent from the sealed source prefix".to_owned(),
                )
            })?;
        Ok(RecordedPriceAttempt {
            payload: observation.payload.clone(),
            received_unix_ms: observation.received_unix_ms,
            source_id: observation.source_id.clone(),
            schema_version: observation.schema_version,
            parser_version: observation.parser_version,
            content_type: observation.content_type.clone(),
        })
    })
    .await
    .map(|prices| {
        prices
            .into_iter()
            .map(|((market, outcome), observation)| {
                (
                    (MarketId(VenueMarketId(market)), OutcomeId(outcome)),
                    observation.price,
                )
            })
            .collect()
    })
}

fn replayed_risk_base(
    leader_wallet: pe_core_types::WalletAddress,
    market_id: &str,
    proposed_debit: CollateralAmount,
    per_trade_cap_bps: i32,
    era: &crate::paper_recovery::PaperEra,
) -> Result<PaperExposureBase, QualificationError> {
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

fn verify_economic_configuration(
    economic: &EconomicPrepared,
    start_hot_config_hash: &str,
    financial_semantic_version: u32,
) -> Result<(), QualificationError> {
    if economic.applied_configuration_hash != start_hot_config_hash {
        return insufficient("EconomicPrepared configuration differs from QualificationStarted");
    }
    if u32::from(economic.version) != financial_semantic_version {
        return insufficient(
            "EconomicPrepared version differs from QualificationStarted financial semantics",
        );
    }
    Ok(())
}

async fn verify_economic(
    operation: &crate::paper_recovery::PaperFillOperationIdentity,
    economic: &EconomicPrepared,
    context: &RiskReplayContext<'_>,
    require_risk_approval: bool,
) -> Result<EconomicPrepared, QualificationError> {
    verify_economic_configuration(
        economic,
        context.start_hot_config_hash,
        context.financial_semantic_version,
    )?;
    let financial_prefix = context
        .paper_prefix
        .last()
        .map(|frame| frame.receipt)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(
                "EconomicPrepared risk prefix has no causal paper frame".to_owned(),
            )
        })?;
    if financial_prefix != economic.risk.financial_prefix {
        return insufficient("EconomicPrepared risk prefix differs from causal replay");
    }
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
        || observed_source.received_unix_ms > economic.risk.evaluated_at_unix_ms
        || complete_bound.received_unix_ms > economic.risk.evaluated_at_unix_ms
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
    let source_backed_economic = replay_source_backed_economic(
        economic,
        economic.risk.evaluated_at_unix_ms,
        cash_before,
        |receipt| {
            let observation = context
                .source
                .get(&receipt.sequence.0)
                .filter(|observation| observation.receipt == receipt)
                .ok_or_else(|| {
                    EconomicReplayError(
                        "economic receipt is absent from the sealed source prefix".to_owned(),
                    )
                })?;
            Ok(RecordedEconomicSource {
                payload: observation.payload.clone(),
                received_unix_ms: observation.received_unix_ms,
                source_id: observation.source_id.clone(),
                schema_version: observation.schema_version,
                parser_version: observation.parser_version,
                content_type: observation.content_type.clone(),
            })
        },
    )
    .map_err(|error| QualificationError::InsufficientEvidence(error.to_string()))?;
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
    let active_halts = active_risk_halts(&era);
    let latency_was_active =
        active_halts.contains(&(RiskHaltOwner::Paper, RiskHaltCause::CopyLatency));
    let proposed_debit = source_backed_economic
        .proposed_debit()
        .map_err(|error| QualificationError::InsufficientEvidence(error.to_string()))?;
    let base = replayed_risk_base(
        operation.leader_wallet,
        &economic.market.market_id,
        proposed_debit,
        economic.risk.snapshot.per_trade_cap_bps,
        &era,
    )?;
    let mut reconstructed = build_paper_risk_snapshot_from_source_receipts(
        &base,
        &snapshot,
        &era,
        &current_prices,
        |receipt| {
            let Some(source) = context.source.get(&receipt.sequence.0) else {
                return Err(RiskInputsUnavailable::PriceMissing);
            };
            if source.receipt != receipt {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            Ok(source.received_unix_ms)
        },
        evaluated_at_unix,
        latency_was_active,
    )
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "risk snapshot reconstruction failed: {error}"
        ))
    })?;
    apply_global_risk_halts(&active_halts, &mut reconstructed);
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
    if require_risk_approval && expected_risk != RiskDecisionAudit::Approved {
        return insufficient("Financial Fill risk decision is not an approval");
    }

    let risk = RiskAudit {
        financial_prefix,
        snapshot: reconstructed,
        decision: expected_risk,
        price_receipts: economic.risk.price_receipts.clone(),
        evaluated_at_unix_ms: economic.risk.evaluated_at_unix_ms,
    };
    source_backed_economic
        .recompose(economic, risk, context.start_hot_config_hash.to_owned())
        .map_err(|error| QualificationError::InsufficientEvidence(error.to_string()))
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

fn reduce_financial_facts(
    mut state: FinancialFactState,
    facts: &[CompletedFinancialFact],
    in_prefix: impl Fn(&CompletedFinancialFact) -> bool,
    is_causal: impl Fn(&CompletedFinancialFact) -> Result<bool, QualificationError>,
) -> Result<FinancialFactState, QualificationError> {
    for fact in facts {
        if !in_prefix(fact) || !is_causal(fact)? {
            continue;
        }
        match (&fact.payload, &fact.result) {
            (
                FinancialPayload::Fill {
                    operation,
                    economic,
                },
                FinancialResult::Fill { canonical },
            ) => {
                state.cash = state
                    .cash
                    .checked_sub(canonical.principal.to_decimal())
                    .and_then(|cash| cash.checked_sub(canonical.fee.to_decimal()))
                    .ok_or_else(|| {
                        QualificationError::InsufficientEvidence(
                            "paper cash underflow while replaying Fill Final".to_owned(),
                        )
                    })?;
                apply_fill_position(&mut state.positions, economic, canonical.quantity)?;
                state.fills.push(FillRow {
                    idempotency_key:
                        pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                            &TraderId(operation.leader_wallet).to_string(),
                            &operation.source_trade_id.0,
                            &economic.market.market_id,
                            u16::from(economic.market.outcome_index),
                            economic.market.side,
                            operation.observed_at_bucket,
                        ),
                    market_id: MarketId(VenueMarketId(economic.market.market_id.clone())),
                    outcome_id: OutcomeId(u16::from(economic.market.outcome_index)),
                    side: economic.market.side,
                    quantity: canonical.quantity,
                    fill_price: canonical.fill_price,
                    principal: canonical.principal,
                    fee: canonical.fee,
                    event_seq: fact.prepared_receipt.sequence,
                    prepared_seq: fact.prepared_receipt.sequence,
                    source_receipt_seq: economic
                        .observation
                        .as_ref()
                        .map(|observation| observation.source_receipt.sequence),
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
                let payouts = BinaryPayoutVector::from_canonical_json(payout_by_outcome_index_json)
                    .map_err(|error| {
                        QualificationError::InsufficientEvidence(format!(
                            "resolution payout decode: {error}"
                        ))
                    })?;
                let decimals = payouts.decimals();
                let payout = BinaryPayout::new(decimals[0], decimals[1]).map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "resolution payout vector: {error}"
                    ))
                })?;
                let mut by_outcome = BTreeMap::<u16, ShareAmount>::new();
                for position in state
                    .positions
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
                let credit = aggregate_resolution_credit(
                    &by_outcome.into_iter().collect::<Vec<_>>(),
                    &payout,
                )
                .map_err(|error| {
                    QualificationError::InsufficientEvidence(format!(
                        "resolution credit arithmetic: {error}"
                    ))
                })?;
                state.cash = state.cash.checked_add(credit.to_decimal()).ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "paper cash overflow while replaying Resolution Final".to_owned(),
                    )
                })?;
                state.settlements.push(SettledMarketRow {
                    market_id: MarketId(VenueMarketId(condition_id.0.clone())),
                    outcome_prices_json: payout_by_outcome_index_json.clone(),
                    credit_applied: credit.to_decimal(),
                    settled_at_unix: canonical.settled_at_unix,
                    prepared_seq: Some(fact.prepared_receipt.sequence),
                    source_receipt_seq: Some(resolution_source_receipt.sequence),
                });
                state
                    .positions
                    .retain(|position| position.condition_id != condition_id.0);
            }
            _ => return insufficient("Financial Prepared/Final kinds disagree"),
        }
        state.last_completed = Some(fact.prepared_receipt.sequence);
    }
    Ok(state)
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
    let receipt_is_causal = |receipt: AppendReceipt| -> Result<bool, QualificationError> {
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
    let mut completed_prepared = HashSet::new();
    for fact in facts {
        let causal = match &fact.payload {
            FinancialPayload::Fill { economic, .. } => {
                let observation = economic.observation.as_ref().ok_or_else(|| {
                    QualificationError::InsufficientEvidence(
                        "Financial Fill has no causal observation".to_owned(),
                    )
                })?;
                receipt_is_causal(observation.source_receipt)?
            }
            FinancialPayload::Resolution {
                resolution_source_receipt,
                ..
            } => receipt_is_causal(*resolution_source_receipt)?,
        };
        if causal {
            completed_prepared.insert(fact.prepared_receipt.sequence);
        }
    }
    let financial = reduce_financial_facts(
        FinancialFactState::new(starting_bankroll),
        facts,
        |_| true,
        |fact| Ok(completed_prepared.contains(&fact.prepared_receipt.sequence)),
    )?;
    let mut closed_fill_final_conditions = HashMap::new();
    let mut open_fill_finals = Vec::new();
    for fact in facts
        .iter()
        .filter(|fact| completed_prepared.contains(&fact.prepared_receipt.sequence))
    {
        match (&fact.payload, &fact.result) {
            (FinancialPayload::Fill { economic, .. }, FinancialResult::Fill { .. }) => {
                open_fill_finals.push((
                    fact.final_receipt.sequence.0,
                    economic.market.condition_id.0.clone(),
                ));
            }
            (
                FinancialPayload::Resolution { condition_id, .. },
                FinancialResult::Resolution { .. },
            ) => {
                for (fill_sequence, fill_condition) in &open_fill_finals {
                    if fill_condition == &condition_id.0 {
                        closed_fill_final_conditions.insert(*fill_sequence, fill_condition.clone());
                    }
                }
                open_fill_finals.retain(|(_, fill_condition)| fill_condition != &condition_id.0);
            }
            _ => return insufficient("causal mark Prepared/Final kinds disagree"),
        }
    }
    Ok(CausalFinancialState {
        cash: financial.cash,
        positions: financial.positions,
        last_completed: financial.last_completed,
        completed_prepared,
        closed_fill_final_conditions,
    })
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
    let base_url = "https://offline.invalid";
    let request_url =
        format!("{base_url}/prices-history?market=recorded&startTs=0&endTs=1&fidelity=1");
    let replayed = ClobPricesHistoryClient::new(
        base_url.to_owned(),
        FixtureFetcher::new(HashMap::from([(request_url, observation.payload.clone())])),
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

struct DeclineReplayContext<'a> {
    frames: &'a [ScannedPaperFrame],
    start_index: usize,
    source: &'a BTreeMap<u64, SourceObservation>,
    completed_financial_facts: &'a [CompletedFinancialFact],
    start_receipt: AppendReceipt,
}

fn financial_state_at_prefix(
    starting_bankroll: Decimal,
    facts: &[CompletedFinancialFact],
    financial_prefix: AppendReceipt,
) -> Result<FinancialFactState, QualificationError> {
    reduce_financial_facts(
        FinancialFactState::new(starting_bankroll),
        facts,
        |fact| fact.final_receipt.sequence <= financial_prefix.sequence,
        |_| Ok(true),
    )
}

fn recorded_fill_paper_prefix<'a>(
    frames_before_prepared: &'a [ScannedPaperFrame],
    financial_prefix: AppendReceipt,
    evaluated_at_unix_ms: i64,
    previous_fill_financial_prefix: &mut Option<AppendReceipt>,
) -> Result<&'a [ScannedPaperFrame], QualificationError> {
    let prefix = paper_prefix_at_financial_prefix(
        frames_before_prepared,
        financial_prefix,
        evaluated_at_unix_ms,
    )
    .map_err(|error| QualificationError::InsufficientEvidence(error.to_string()))?;
    if previous_fill_financial_prefix
        .is_some_and(|previous| financial_prefix.sequence < previous.sequence)
    {
        return insufficient("fill financial prefix regresses from the previous fill");
    }
    *previous_fill_financial_prefix = Some(financial_prefix);
    Ok(prefix)
}

async fn bind_final_receipts(
    decisions: &[crate::decision_replay::ReplayedDecision],
    fills: &[CompletedFill],
    decision_observations: &HashMap<
        pe_core_types::SourceTradeId,
        pe_execution_core::ObservationEvidence,
    >,
    started: &QualificationStarted,
    decline_context: &DeclineReplayContext<'_>,
) -> Result<(), QualificationError> {
    let mut matched_finals = HashSet::new();
    for decision in decisions {
        let terminal = &decision.post_boundary.body.terminal;
        if let Some(expected_decline) = terminal.decline.as_ref() {
            verify_winner_follow_decline_decision(
                &decision.continuation,
                &expected_decline.inputs,
                &expected_decline.outcome,
                decision_observations,
                started,
                decline_context,
            )
            .await?;
            continue;
        }
        if terminal.disposition != "fill" {
            continue;
        }
        let final_receipt = terminal.final_receipt.ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "fill decision {} has no FinancialFinal receipt",
                decision.continuation.facts.source_trade_id
            ))
        })?;
        let mut candidates = fills
            .iter()
            .filter(|fill| fill.final_receipt == final_receipt);
        let fill = candidates.next().ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "fill decision {} references no FinancialFinal",
                decision.continuation.facts.source_trade_id
            ))
        })?;
        if candidates.next().is_some() || !matched_finals.insert(receipt_key(final_receipt)) {
            return insufficient("multiple fill decisions bind the same FinancialFinal");
        }

        let continuation = &decision.continuation.facts;
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
            || continuation.applied_configuration_hash != fill.economic.applied_configuration_hash
            || continuation.applied_configuration_hash != started.hot_config_hash
            || u32::from(fill.economic.version) != started.financial_semantic_version
        {
            return insufficient(
                "fill decision, Prepared economics, and FinancialFinal identity/configuration disagree",
            );
        }
        verify_winner_follow_fill_decision(&decision.continuation, fill)?;
    }
    if matched_finals.len() != fills.len() {
        return insufficient("a FinancialFinal has no identical terminal fill decision");
    }
    Ok(())
}

fn verify_winner_follow_fill_decision(
    continuation: &DecisionContinuationV3,
    fill: &CompletedFill,
) -> Result<(), QualificationError> {
    let frozen = &continuation.facts;
    let economic = &fill.economic;
    let configuration = &frozen.applied_configuration;
    if economic.applied_configuration_hash != frozen.applied_configuration_hash
        || economic.market.market_id != frozen.market_id.0.0
        || u16::from(economic.market.outcome_index) != frozen.outcome_id.0
        || economic.market.side != frozen.side
    {
        return insufficient(format!(
            "decision {} economic inputs differ from its frozen strategy basis",
            frozen.source_trade_id
        ));
    }

    let (signal, mode) = verify_winner_follow_economic_policy(continuation, economic)?;
    let intent =
        pe_strategy_winner_follow::WinnerFollowStrategy::new(configuration.winner_follow_config())
            .evaluate_at_price(
                &signal,
                economic.sizing.all_in_price,
                frozen.frozen_basis.win_rate_p,
                economic.risk.snapshot.clone(),
                frozen.frozen_basis.bankroll,
                mode,
            )
            .map_err(|error| {
                QualificationError::InsufficientEvidence(format!(
                    "decision {} recorded a fill but Winner-Follow replay declined: {:?}",
                    frozen.source_trade_id,
                    pe_strategy_winner_follow::WinnerFollowDeclineAudit::from(&error)
                ))
            })?;
    verify_winner_follow_intent_plan(&intent, economic, &frozen.source_trade_id)?;
    if intent.market_id != frozen.market_id
        || intent.outcome_id != frozen.outcome_id
        || intent.side != frozen.side
        || intent.limit_price != frozen.price
        || intent.idempotency_key
            != pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                &TraderId(frozen.wallet).to_string(),
                &frozen.source_trade_id.0,
                &frozen.market_id.0.0,
                frozen.outcome_id.0,
                frozen.side,
                frozen.source_epoch,
            )
    {
        return insufficient(format!(
            "decision {} Winner-Follow intent differs from durable fill economics",
            frozen.source_trade_id
        ));
    }
    Ok(())
}

fn verify_winner_follow_economic_policy(
    continuation: &DecisionContinuationV3,
    economic: &EconomicPrepared,
) -> Result<(LeaderSignal, pe_strategy_winner_follow::ExecutionMode), QualificationError> {
    let frozen = &continuation.facts;
    let configuration = &frozen.applied_configuration;
    let (signal, mode) = reconstruct_winner_follow_signal(continuation)?;
    let strategy =
        pe_strategy_winner_follow::WinnerFollowStrategy::new(configuration.winner_follow_config());
    let expected_sizing = match strategy.config().sizing_mode {
        pe_strategy_winner_follow::SizingMode::Kelly => SizingModeAudit::Kelly {
            fraction: strategy.effective_kelly_fraction(mode),
            probability: frozen.frozen_basis.win_rate_p,
        },
        pe_strategy_winner_follow::SizingMode::Dollar { usd } => SizingModeAudit::Dollar { usd },
        pe_strategy_winner_follow::SizingMode::Contract { contracts } => {
            SizingModeAudit::Contract { contracts }
        }
    };
    let band_floor =
        Price::new(configuration.min_fill_price.max(Decimal::ZERO)).map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "decision {} frozen minimum fill price is invalid: {error}",
                frozen.source_trade_id
            ))
        })?;
    let band_ceiling_exclusive = Price::new(if configuration.max_fill_price > Decimal::ZERO {
        configuration.max_fill_price
    } else {
        Decimal::ONE
    })
    .map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "decision {} frozen maximum fill price is invalid: {error}",
            frozen.source_trade_id
        ))
    })?;
    if economic.sizing.mode != expected_sizing
        || economic.sizing.slippage_rate != strategy.config().slippage_rate
        || economic.balance.price_impact_cap_bps != configuration.price_impact_cap_bps
        || economic.balance.band_floor != band_floor
        || economic.balance.band_ceiling_exclusive != band_ceiling_exclusive
        || economic.balance.chase_ceiling != signal.leader_price
    {
        return insufficient(format!(
            "decision {} EconomicPrepared execution policy differs from its frozen configuration and signal",
            frozen.source_trade_id
        ));
    }
    Ok((signal, mode))
}

fn verify_winner_follow_intent_plan(
    intent: &pe_venue_core::OrderIntent,
    economic: &EconomicPrepared,
    source_trade_id: &SourceTradeId,
) -> Result<(), QualificationError> {
    let allocation_matches = match economic.sizing.mode {
        SizingModeAudit::Kelly { .. } => ShareAmount::from_whole(intent.contracts.0)
            .is_ok_and(|shares| shares == economic.ladder.minimum_shares),
        SizingModeAudit::Contract { contracts } => {
            intent.contracts.0 == contracts
                && ShareAmount::from_whole(intent.contracts.0)
                    .is_ok_and(|shares| shares == economic.ladder.minimum_shares)
        }
        SizingModeAudit::Dollar { usd } => {
            usd.checked_div(economic.sizing.all_in_price.0)
                .map(|contracts| contracts.floor())
                == Some(Decimal::from(intent.contracts.0))
        }
    };
    if !allocation_matches
        || economic.sizing.minimum_shares != economic.ladder.minimum_shares
        || intent.limit_price != economic.balance.chase_ceiling
        || economic.ladder.limit_price > intent.limit_price
    {
        return insufficient(format!(
            "decision {source_trade_id} Winner-Follow allocation or limit differs from its sized economic plan"
        ));
    }
    Ok(())
}

async fn replay_unavailable_risk_inputs(
    continuation: &DecisionContinuationV3,
    evidence: &WinnerFollowRiskInputEvidence,
    started: &QualificationStarted,
    context: &DeclineReplayContext<'_>,
) -> Result<Option<RiskInputsUnavailable>, QualificationError> {
    let Some(financial_prefix) = evidence.financial_prefix else {
        return Ok(Some(RiskInputsUnavailable::SnapshotSequenceMismatch));
    };
    let paper_prefix = paper_prefix_at_financial_prefix(
        &context.frames[context.start_index..],
        financial_prefix,
        evidence.evaluated_at_unix_ms,
    )
    .map_err(|error| QualificationError::InsufficientEvidence(error.to_string()))?;
    let financial = financial_state_at_prefix(
        started.starting_bankroll.to_decimal(),
        context.completed_financial_facts,
        financial_prefix,
    )?;
    let replay = RiskReplayContext {
        cash: financial.cash,
        positions: &financial.positions,
        fills: &financial.fills,
        settlements: &financial.settlements,
        last_completed: financial.last_completed,
        start_receipt: context.start_receipt,
        paper_prefix,
        source: context.source,
        prepared_received_unix_ms: evidence.evaluated_at_unix_ms,
        start_hot_config_hash: &started.hot_config_hash,
        financial_semantic_version: started.financial_semantic_version,
    };
    let evaluated_at_unix = evidence.evaluated_at_unix_ms.div_euclid(1_000);
    let snapshot = replayed_financial_snapshot(&replay, evaluated_at_unix)?;
    let current_prices = match replayed_risk_prices(
        &evidence.price_receipts,
        evidence.evaluated_at_unix_ms,
        &snapshot.positions,
        context.source,
    )
    .await
    {
        Ok(prices) => prices,
        Err(RiskPriceReplayError::Unavailable(RiskInputsUnavailable::PriceMissing)) => {
            return Ok(Some(RiskInputsUnavailable::PriceMissing));
        }
        Err(RiskPriceReplayError::Unavailable(cause)) => return Ok(Some(cause)),
        Err(RiskPriceReplayError::Insufficient(reason)) => return insufficient(reason),
    };
    let era = paper_era(paper_prefix.to_vec());
    let base = match build_paper_risk_base(
        &era,
        continuation.facts.wallet,
        &continuation.facts.market_id.0.0,
        evidence.proposed_debit,
        evidence.per_trade_cap_bps,
    ) {
        Ok(base) => base,
        Err(cause) => return Ok(Some(cause)),
    };
    let active_halts = active_risk_halts(&era);
    let latency_was_active =
        active_halts.contains(&(RiskHaltOwner::Paper, RiskHaltCause::CopyLatency));
    match build_paper_risk_snapshot_from_source_receipts(
        &base,
        &snapshot,
        &era,
        &current_prices,
        |receipt| {
            let Some(source) = context.source.get(&receipt.sequence.0) else {
                return Err(RiskInputsUnavailable::PriceMissing);
            };
            if source.receipt != receipt {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            Ok(source.received_unix_ms)
        },
        evaluated_at_unix,
        latency_was_active,
    ) {
        Ok(_) => Ok(None),
        Err(cause) => Ok(Some(cause)),
    }
}

async fn verify_winner_follow_decline_decision(
    continuation: &DecisionContinuationV3,
    inputs: &WinnerFollowDecisionInputs,
    expected: &pe_strategy_winner_follow::WinnerFollowDeclineAudit,
    decision_observations: &HashMap<SourceTradeId, ObservationEvidence>,
    started: &QualificationStarted,
    context: &DeclineReplayContext<'_>,
) -> Result<(), QualificationError> {
    let frozen = &continuation.facts;
    let actual = match inputs {
        WinnerFollowDecisionInputs::RiskInputsUnavailable { cause, evidence } => {
            let replayed =
                replay_unavailable_risk_inputs(continuation, evidence, started, context).await?;
            if replayed != Some(*cause) {
                return insufficient(format!(
                    "decision {} recorded unavailable risk input {cause} but causal replay produced {replayed:?}",
                    frozen.source_trade_id
                ));
            }
            pe_strategy_winner_follow::WinnerFollowDeclineAudit::RiskInputsUnavailable
        }
        WinnerFollowDecisionInputs::Evaluated { economic } => {
            if economic.applied_configuration_hash != frozen.applied_configuration_hash
                || economic.market.market_id != frozen.market_id.0.0
                || u16::from(economic.market.outcome_index) != frozen.outcome_id.0
                || economic.market.side != frozen.side
                || economic.observation.as_ref()
                    != decision_observations.get(&frozen.source_trade_id)
            {
                return insufficient(format!(
                    "decision {} decline economics differ from its frozen strategy basis",
                    frozen.source_trade_id
                ));
            }
            let (signal, mode) = verify_winner_follow_economic_policy(continuation, economic)?;
            let paper_prefix = paper_prefix_at_financial_prefix(
                &context.frames[context.start_index..],
                economic.risk.financial_prefix,
                economic.risk.evaluated_at_unix_ms,
            )
            .map_err(|error| QualificationError::InsufficientEvidence(error.to_string()))?;
            let financial = financial_state_at_prefix(
                started.starting_bankroll.to_decimal(),
                context.completed_financial_facts,
                economic.risk.financial_prefix,
            )?;
            let replay = RiskReplayContext {
                cash: financial.cash,
                positions: &financial.positions,
                fills: &financial.fills,
                settlements: &financial.settlements,
                last_completed: financial.last_completed,
                start_receipt: context.start_receipt,
                paper_prefix,
                source: context.source,
                prepared_received_unix_ms: economic.risk.evaluated_at_unix_ms,
                start_hot_config_hash: &started.hot_config_hash,
                financial_semantic_version: started.financial_semantic_version,
            };
            let operation = crate::paper_recovery::PaperFillOperationIdentity {
                leader_wallet: frozen.wallet,
                source_trade_id: frozen.source_trade_id.clone(),
                observed_at_bucket: frozen.source_epoch,
            };
            let reconstructed = verify_economic(&operation, economic, &replay, false).await?;
            match pe_strategy_winner_follow::WinnerFollowStrategy::new(
                frozen.applied_configuration.winner_follow_config(),
            )
            .evaluate_at_price(
                &signal,
                reconstructed.sizing.all_in_price,
                frozen.frozen_basis.win_rate_p,
                reconstructed.risk.snapshot,
                frozen.frozen_basis.bankroll,
                mode,
            ) {
                Ok(intent) => {
                    return insufficient(format!(
                        "decision {} recorded decline {expected:?} but Winner-Follow replay emitted intent {intent:?}",
                        frozen.source_trade_id
                    ));
                }
                Err(error) => pe_strategy_winner_follow::WinnerFollowDeclineAudit::from(&error),
            }
        }
    };
    if &actual != expected {
        return insufficient(format!(
            "decision {} Winner-Follow decline differs: recorded {expected:?}, replayed {actual:?}",
            frozen.source_trade_id
        ));
    }
    Ok(())
}

fn reconstruct_winner_follow_signal(
    continuation: &DecisionContinuationV3,
) -> Result<(LeaderSignal, pe_strategy_winner_follow::ExecutionMode), QualificationError> {
    let frozen = &continuation.facts;
    let mode = crate::runtime_config::parse_execution_mode(&frozen.applied_configuration.mode)
        .ok_or_else(|| {
            QualificationError::InsufficientEvidence(format!(
                "decision {} has an invalid frozen execution mode",
                frozen.source_trade_id
            ))
        })?;
    let incoming = continuation.incoming_trade().map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "decision {} cannot reconstruct its strategy signal: {error}",
            frozen.source_trade_id
        ))
    })?;
    Ok((
        LeaderSignal {
            leader: TraderId(incoming.wallet),
            venue: VenueId::polymarket(),
            market_id: incoming.market_id,
            outcome_id: incoming.outcome_id,
            action: frozen.pre_bucket_action,
            leader_side: incoming.side,
            leader_price: incoming.price,
            leader_size: incoming.contracts,
            observed_at: incoming.observed_at,
            received_at: incoming.received_at,
            reconstruction_quality: frozen.reconstruction_quality,
            source_trade_id: incoming.source_trade_id,
            action_confidence_ppm: frozen.action_confidence_ppm,
        },
        mode,
    ))
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
    Ok(())
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
    let manifest = MembershipProofManifest::capture(state, membership).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "financial-era membership proof capture failed: {error}"
        ))
    })?;
    MembershipProofBinding::encode(manifest).map_err(|error| {
        QualificationError::InsufficientEvidence(format!(
            "financial-era membership proof binding failed: {error}"
        ))
    })
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
    let recovery =
        pe_execution_core::live_journal::recovery_inventory(live_path, None).map_err(|error| {
            QualificationError::InsufficientEvidence(format!("live recovery inventory: {error}"))
        })?;
    if !recovery.open_orders.is_empty() {
        return insufficient("financial-era prepare found a nonterminal live order");
    }
    if !recovery.approved_admissions.is_empty() {
        return insufficient("financial-era prepare found an unmatched Approved admission");
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
        let derived = crate::live_fanout::derive_projection_rows_with_sources(
            &account_id,
            &events,
            &source_envelopes,
        )
        .map_err(|error| {
            QualificationError::InsufficientEvidence(format!(
                "live journal strict reduction failed for {account_id}: {error}"
            ))
        })?;
        if !derived.pending_approved_admission_keys.is_empty() {
            return insufficient(format!(
                "financial-era prepare found an unmatched Approved admission for {account_id}"
            ));
        }
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
    let scan = Scanner::inspect(&manifest.paths.paper_log)?;
    let frames = scan_verified_paper_prefix(
        &manifest.paths.paper_log,
        scan.verified_tail.physical_tail,
        scan.incomplete_tail.is_none(),
    )?;
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
    verify_live_preparation_posture(
        &configured_live_journal_path(config),
        &config.source_event_log_path,
        &config.status_path,
    )?;
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
    Ok(format!(
        "{{\"complete_start\":false,\"repaired\":{repaired}}}"
    ))
}

static ROLLBACK_PREFIX_COPY_SEQUENCE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn scan_verified_paper_prefix(
    path: &Path,
    verified_tail: u64,
    complete_file: bool,
) -> Result<Vec<ScannedPaperFrame>, QualificationError> {
    if complete_file {
        return Ok(scan_paper_log(path)?);
    }

    let ordinal = ROLLBACK_PREFIX_COPY_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temporary_path = std::env::temp_dir().join(format!(
        ".pe-financial-era-prefix-{}-{ordinal}.log",
        std::process::id()
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut target = options.open(&temporary_path)?;
    let result = (|| -> Result<Vec<ScannedPaperFrame>, QualificationError> {
        use std::io::Read as _;

        let source = fs::File::open(path)?;
        let copied = std::io::copy(&mut source.take(verified_tail), &mut target)?;
        if copied != verified_tail {
            return insufficient(
                "rollback-check could not copy the complete verified paper prefix",
            );
        }
        target.sync_all()?;
        drop(target);
        Ok(scan_paper_log(&temporary_path)?)
    })();
    let cleanup = fs::remove_file(&temporary_path);
    let frames = result?;
    cleanup?;
    Ok(frames)
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
        Probability, ProbabilityPpm, RawHttpAttempt, RawHttpResponse, SourceTradeId, WalletAddress,
    };
    use pe_event_log::EventEnvelope;
    use pe_execution_core::live_journal::{LivePositionEvidenceAudit, LivePositionPageAudit};
    use pe_execution_core::{
        AdmissionReceipts, EconomicInputs, LiveAdmissionArtifact, SizingModeAudit,
    };
    use pe_execution_core::{
        BalanceAudit, ECONOMIC_PREPARED_VERSION, FeeAudit, LadderAskAudit, LadderPlanAudit,
        LiveAccountStateAudit, LiveAdmissionArtifactAudit, LiveAdmissionEvaluationAudit,
        LiveControlMode, LiveFillProjectionIdentity, LiveJournal, LiveJournalPayload,
        LiveMarketEvidenceAudit, LiveOrderIdentity, LiveOrderPreparedAudit, MarketSelection,
        ObservationEvidence, SizingAudit,
    };
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_venue_polymarket::{
        BuySizing, CanaryV2Client, CompactFeeSchedule, PreparedPolymarketBuy, SDK_VERSION,
        parse_compact_market, plan_sized_buy,
    };
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
            membership: vec![WalletAddress::from_hex(&format!("0x{}", "1".repeat(40))).unwrap()],
            membership_proofs_hash: "membership".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
        }
    }

    /// PASS: a sealed live tail may extend the Start tail, while regression and a changed binding
    /// at the same sequence both fail closed before live-wrapper replay.
    #[test]
    fn sealed_live_tail_must_extend_the_exact_start_boundary() {
        let start = TailBinding {
            physical_tail: 100,
            last_sequence: Some(EventSeq(4)),
            last_hash: blake3::hash(b"start").to_hex().to_string(),
        };
        let extension = TailBinding {
            physical_tail: 140,
            last_sequence: Some(EventSeq(5)),
            last_hash: blake3::hash(b"seal").to_hex().to_string(),
        };
        verify_tail_extension(&start, &extension, "live journal").unwrap();

        let preceding = TailBinding {
            physical_tail: 90,
            last_sequence: Some(EventSeq(3)),
            last_hash: blake3::hash(b"preceding").to_hex().to_string(),
        };
        assert!(matches!(
            verify_tail_extension(&start, &preceding, "live journal"),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("precedes its Start prefix")
        ));

        let changed_boundary = TailBinding {
            physical_tail: start.physical_tail,
            last_sequence: start.last_sequence,
            last_hash: blake3::hash(b"changed").to_hex().to_string(),
        };
        assert!(matches!(
            verify_tail_extension(&start, &changed_boundary, "live journal"),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("changes its Start boundary")
        ));
    }

    /// PASS: a diagnostic invocation without the live journal emits typed insufficient evidence
    /// and cannot claim exact replay.
    #[tokio::test]
    async fn omitted_live_journal_is_not_pass_or_exact() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("qualification.json");
        let options = QualifyOptions {
            paper_log: temp.path().join("missing-paper.log"),
            source_log: temp.path().join("missing-source.log"),
            live_journal: None,
            paper_state: temp.path().join("missing-paper.db"),
            seal_hash: "not-a-seal".to_owned(),
            output: output.clone(),
        };

        let (verdict, _) = run_qualify(&options).await.unwrap();
        let report: QualificationReport =
            serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        assert_eq!(verdict, QualificationVerdict::InsufficientEvidence);
        assert_eq!(report.verdict, QualificationVerdict::InsufficientEvidence);
        assert!(!report.replay.exact);
        assert!(
            report
                .reasons
                .iter()
                .any(|reason| reason.contains("live journal is required"))
        );
    }

    /// PASS: qualification delegates source-id, schema-version, and parser-version rejection to
    /// the canonical prefix reader for hash-valid current-event envelopes.
    #[test]
    fn live_prefix_rejects_every_unsupported_canonical_envelope_contract() {
        let contracts = [
            ("wrong-source", 2_u32, 1_u32, "source"),
            ("ordinary-live-execution", 99, 1, "schema"),
            ("ordinary-live-execution", 2, 99, "parser"),
        ];
        for (source_id, schema_version, parser_version, label) in contracts {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("wrong-{label}.log"));
            let mut writer = Writer::open(&path).unwrap();
            let start = TailBinding::from(&Scanner::verify(&path).unwrap());
            let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();
            let event = pe_execution_core::LiveJournalEvent {
                account_id: AccountId::new("qualification-test").unwrap(),
                seq: 0,
                timestamp,
                payload: pe_execution_core::LiveJournalPayload::ModeTransitionApplied(
                    pe_execution_core::LiveModeTransitionAudit {
                        requested: pe_execution_core::LiveControlMode::LiveTiny,
                        previous_effective: pe_execution_core::LiveControlMode::Off,
                        new_effective: pe_execution_core::LiveControlMode::LiveTiny,
                        reason: pe_execution_core::LiveModeTransitionReason::Armed,
                    },
                ),
            };
            let receipt = writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(source_id.to_owned()),
                    schema_version,
                    parser_version,
                    observed_at: SourceTimestamp(timestamp),
                    received_at: ReceivedAt(timestamp),
                    content_type: ContentType::Json,
                    payload: serde_json::to_vec(&event).unwrap(),
                })
                .unwrap();
            drop(writer);
            let sealed = TailBinding::from(&Scanner::verify(&path).unwrap());

            assert!(matches!(
                pe_execution_core::LiveJournal::replay_prefix(&path, receipt.sequence),
                Err(pe_execution_core::LiveJournalError::UnexpectedEnvelope)
            ));
            assert!(matches!(
                replay_live_prefix(&path, &start, &sealed),
                Err(QualificationError::InsufficientEvidence(reason))
                    if reason.contains("unsupported envelope")
            ));
        }
    }

    fn activity_observation(sequence: u64, payload: &[u8]) -> SourceObservation {
        let at = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();
        SourceObservation {
            receipt: AppendReceipt {
                sequence: EventSeq(sequence),
                this_hash: blake3::hash(payload),
            },
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            received_unix_ms: 1_700_000_100_000,
            source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
            schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        }
    }

    fn activity_row(
        condition: &str,
        asset: &str,
        size: &str,
        usdc_size: &str,
        transaction_hash: &str,
        timestamp: i64,
    ) -> serde_json::Value {
        serde_json::json!({
            "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "type": "TRADE",
            "conditionId": condition,
            "asset": asset,
            "side": "BUY",
            "size": size,
            "usdcSize": usdc_size,
            "price": "0.5",
            "timestamp": timestamp.to_string(),
            "transactionHash": transaction_hash,
            "outcomeIndex": "0"
        })
    }

    fn activity_payload(rows: Vec<serde_json::Value>) -> Vec<u8> {
        serde_json::to_vec(&rows).unwrap()
    }

    fn full_page_with_target(target: serde_json::Value, timestamp: i64) -> Vec<u8> {
        let mut rows = vec![target];
        for index in 0..(RECONCILIATION_PAGE_LIMIT - 1) {
            rows.push(activity_row(
                &format!("0xfiller-condition-{index}"),
                &format!("filler-asset-{index}"),
                "1",
                "0.5",
                &format!("0xfiller-transaction-{index}"),
                timestamp,
            ));
        }
        activity_payload(rows)
    }

    fn page_evidence(
        url: &str,
        payload: &[u8],
        start: Option<i64>,
        end: i64,
        offset: u32,
    ) -> ReconciliationPageEvidence {
        let row_count = serde_json::from_slice::<Vec<serde_json::Value>>(payload)
            .unwrap()
            .len();
        ReconciliationPageEvidence {
            request_url: url.to_owned(),
            bounds: Some(pe_source_polymarket_public::ActivityRequestBounds { start, end }),
            partition: None,
            offset,
            row_count: u32::try_from(row_count).unwrap(),
            canonical_page_hash: format!("canonical-{url}"),
            raw_page_hash: blake3::hash(payload).to_hex().to_string(),
            received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
            schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
        }
    }

    fn page_occurrence(
        observation: &SourceObservation,
        url: &str,
        payload: &[u8],
    ) -> PageOccurrence {
        PageOccurrence {
            request_url: url.to_owned(),
            raw_hash: blake3::hash(payload).to_hex().to_string(),
            receipt: observation.receipt,
        }
    }

    fn read_scope(
        fixed_end: i64,
        pages: Vec<(PageOccurrence, ReconciliationPageEvidence)>,
        decisions: impl IntoIterator<Item = SourceTradeId>,
    ) -> CompleteActivityReadScope {
        let mut continuation = classification_fixture().0;
        continuation.facts.wallet =
            WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        continuation.facts.decision_inputs = serde_json::json!({
            "fixed_end": fixed_end,
            "pages": pages
                .iter()
                .map(|(_, evidence)| evidence)
                .collect::<Vec<_>>(),
        });
        continuation.page_occurrences = pages
            .into_iter()
            .map(|(occurrence, _)| occurrence)
            .collect();
        CompleteActivityReadScope {
            continuation,
            decisions: decisions.into_iter().map(|id| (id, None)).collect(),
        }
    }

    fn parsed_aggregates(
        payloads: &[&[u8]],
    ) -> Vec<pe_source_polymarket_public::ActivityAggregate> {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut rows = Vec::new();
        for payload in payloads {
            let observation = activity_observation(0, payload);
            rows.extend(
                parse_activity_response(
                    payload,
                    wallet,
                    &ActivityParseContext {
                        source_id: SourceId(observation.source_id),
                        observed_at: observation.observed_at,
                        received_at: observation.received_at,
                        transport: ActivityTransport::Replay,
                    },
                )
                .unwrap()
                .rows,
            );
        }
        aggregate_activity_rows(&rows).unwrap()
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
                financial_prefix: receipt(1),
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

    fn classification_fixture() -> (DecisionContinuationV3, LedgerMutation) {
        let wallet = WalletAddress::from_hex(&format!("0x{}", "a".repeat(40))).unwrap();
        let source_trade_id = SourceTradeId("g2:classification-fixture".to_owned());
        let market_id = MarketId(VenueMarketId("classification-market".to_owned()));
        let source_epoch = 1_700_000_000;
        let configuration =
            crate::runtime_config::RuntimeConfig::from_service_config(&ServiceConfig::default());
        let continuation = DecisionContinuationV3::new(
            crate::bucket_commit::DecisionContinuationFacts {
                source_trade_id: source_trade_id.clone(),
                semantic_revision: "semantic-v3".to_owned(),
                transaction_hash: "0xclassification".to_owned(),
                wallet,
                source_epoch,
                market_id: market_id.clone(),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                price: Price::new(dec!(0.5)).unwrap(),
                share_amount: ShareAmount::from_whole(1).unwrap(),
                provenance: pe_copy_signal_engine::TradeProvenance::RestPoll,
                pre_bucket_action: LeaderAction::Entry,
                reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
                action_confidence_ppm: ProbabilityPpm(1_000_000),
                gate_result: "admitted".to_owned(),
                applied_configuration_hash: configuration.canonical_hash(),
                applied_configuration: configuration,
                frozen_basis: crate::bucket_commit::FrozenDecisionBasis {
                    win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                    bankroll: dec!(100),
                },
                decision_inputs: serde_json::json!({"fixture": "classification"}),
            },
            None,
            Vec::new(),
        );
        let mutation = LedgerMutation {
            source_trade_id,
            transaction_hash: "0xclassification".to_owned(),
            wallet,
            source_time: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(source_epoch).unwrap(),
            ),
            effect: pe_position_ledger::LedgerEffect::Trade {
                market_id,
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                amount: ShareAmount::from_whole(1).unwrap(),
                price: Price::new(dec!(0.5)).unwrap(),
            },
        };
        (continuation, mutation)
    }

    fn source_observation(
        sequence: u64,
        received_unix_ms: i64,
        source_id: &str,
        schema_version: u32,
        parser_version: u32,
        payload: &[u8],
    ) -> SourceObservation {
        let at =
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(received_unix_ms) * 1_000_000)
                .unwrap();
        SourceObservation {
            receipt: test_receipt(sequence),
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            received_unix_ms,
            source_id: source_id.to_owned(),
            schema_version,
            parser_version,
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        }
    }

    fn gamma_price_page_record(
        request_url: &str,
        received_unix_ms: i64,
        raw: &[u8],
        usable: bool,
    ) -> Vec<u8> {
        let received_at = ReceivedAt(
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(received_unix_ms) * 1_000_000)
                .unwrap(),
        );
        let canonical = serde_json::from_slice::<serde_json::Value>(raw)
            .and_then(|value| serde_json::to_vec(&value))
            .unwrap_or_else(|_| raw.to_vec());
        serde_json::to_vec(&GammaPriceAttemptRecord::Page {
            evidence: pe_source_polymarket_public::MetadataPageEvidence {
                request_url: request_url.to_owned(),
                raw_page_hash: blake3::hash(raw).to_hex().to_string(),
                canonical_page_hash: blake3::hash(&canonical).to_hex().to_string(),
                received_at,
                source_id: SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()),
                schema_version: GAMMA_MARKETS_SCHEMA_VERSION,
                parser_version: GAMMA_MARKETS_PARSER_VERSION,
            },
            payload: raw.to_vec(),
            usable,
        })
        .unwrap()
    }

    fn gamma_price_failure_record(request_url: &str, error: &str) -> Vec<u8> {
        serde_json::to_vec(&GammaPriceAttemptRecord::Failure {
            request_url: request_url.to_owned(),
            error: error.to_owned(),
        })
        .unwrap()
    }

    struct DeclineFixture {
        decision: crate::decision_replay::ReplayedDecision,
        start: QualificationStarted,
        frames: Vec<ScannedPaperFrame>,
        source: BTreeMap<u64, SourceObservation>,
        facts: Vec<CompletedFinancialFact>,
        observations: HashMap<SourceTradeId, ObservationEvidence>,
    }

    impl DeclineFixture {
        fn context(&self) -> DeclineReplayContext<'_> {
            DeclineReplayContext {
                frames: &self.frames,
                start_index: 0,
                source: &self.source,
                completed_financial_facts: &self.facts,
                start_receipt: self.frames[0].receipt,
            }
        }
    }

    async fn receipt_backed_decline_fixture() -> DeclineFixture {
        const CONDITION: &str =
            "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee";
        const OPEN_CONDITION: &str =
            "0x5c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ef";
        const EVALUATED_MS: i64 = 1_800_000_010_000;
        let (mut continuation, _) = classification_fixture();
        continuation.facts.market_id = MarketId(VenueMarketId(CONDITION.to_owned()));
        continuation.facts.price = Price::new(dec!(0.50)).unwrap();
        continuation.facts.applied_configuration.sizing_mode =
            pe_strategy_winner_follow::SizingMode::Contract { contracts: 5 };
        continuation.facts.applied_configuration.sizing_contracts = 5;
        continuation.facts.applied_configuration.per_trade_cap =
            pe_strategy_winner_follow::PerTradeCap::Unlimited;
        continuation.facts.applied_configuration.slippage_rate = Decimal::ZERO;
        continuation.facts.applied_configuration_hash =
            continuation.facts.applied_configuration.canonical_hash();
        let mut start = started(&continuation.facts.applied_configuration_hash);
        start.membership = vec![continuation.facts.wallet];

        let gamma = include_bytes!("../tests/fixtures/golden_stream_v1/gamma_long.json");
        let clob_long = include_bytes!("../tests/fixtures/golden_stream_v1/clob_long.json");
        let clob_compact = include_bytes!("../tests/fixtures/golden_stream_v1/clob_compact.json");
        let book = format!(
            r#"{{"market":"{CONDITION}","asset_id":"11","timestamp":"1800000000100","min_order_size":"5","tick_size":"0.01","neg_risk":false,"bids":[{{"price":"0.48","size":"10"}}],"asks":[{{"price":"0.50","size":"10"}},{{"price":"0.49","size":"10"}}]}}"#
        )
        .into_bytes();
        let risk_prices = serde_json::to_vec(&serde_json::json!([{
            "conditionId": OPEN_CONDITION,
            "active": true,
            "closed": false,
            "outcomePrices": "[\"0.45\",\"0.55\"]"
        }]))
        .unwrap();
        let risk_price_url = format!(
            "https://offline.invalid/markets?condition_ids={OPEN_CONDITION}&limit={GAMMA_BATCH_LIMIT_PARAM}"
        );
        let risk_price_record =
            gamma_price_page_record(&risk_price_url, EVALUATED_MS - 3_000, &risk_prices, true);
        let source = BTreeMap::from([
            (
                1001,
                source_observation(
                    1001,
                    EVALUATED_MS - 9_000,
                    GAMMA_MARKETS_SOURCE_ID,
                    GAMMA_MARKETS_SCHEMA_VERSION,
                    GAMMA_MARKETS_PARSER_VERSION,
                    gamma,
                ),
            ),
            (
                1002,
                source_observation(
                    1002,
                    EVALUATED_MS - 8_000,
                    "polymarket.clob.markets",
                    LIVE_MARKET_SCHEMA_VERSION,
                    LIVE_MARKET_PARSER_VERSION,
                    clob_long,
                ),
            ),
            (
                1003,
                source_observation(
                    1003,
                    EVALUATED_MS - 7_000,
                    "polymarket.clob.compact-market",
                    LIVE_MARKET_SCHEMA_VERSION,
                    LIVE_MARKET_PARSER_VERSION,
                    clob_compact,
                ),
            ),
            (
                1004,
                source_observation(
                    1004,
                    EVALUATED_MS - 6_000,
                    "polymarket.clob.book",
                    1,
                    1,
                    &book,
                ),
            ),
            (
                1005,
                source_observation(
                    1005,
                    EVALUATED_MS - 5_000,
                    crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                    pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    b"[]",
                ),
            ),
            (
                1006,
                source_observation(
                    1006,
                    EVALUATED_MS - 4_000,
                    crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                    pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    b"[]",
                ),
            ),
            (
                1007,
                source_observation(
                    1007,
                    EVALUATED_MS - 3_000,
                    GAMMA_MARKETS_SOURCE_ID,
                    GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
                    GAMMA_PRICE_ATTEMPT_PARSER_VERSION,
                    &risk_price_record,
                ),
            ),
            (
                1008,
                source_observation(
                    1008,
                    EVALUATED_MS - 60_000,
                    crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                    pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    b"[]",
                ),
            ),
        ]);
        let admission_market = validate_live_market(
            gamma,
            clob_long,
            &PolymarketConditionId(CONDITION.to_owned()),
            // The recorded market clock is receipt-derived: max(Gamma, CLOB-long) receive time.
            (EVALUATED_MS - 8_000).div_euclid(1_000),
            60,
        )
        .unwrap();
        let compact = parse_compact_market(
            clob_compact,
            &PolymarketConditionId(CONDITION.to_owned()),
            &admission_market.ordered_outcome_token_ids,
        )
        .unwrap();
        let admission = LiveAdmissionArtifact {
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: PolymarketConditionId(CONDITION.to_owned()),
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: blake3::hash(clob_long).to_hex().to_string(),
                source_timestamp_unix: None,
                observed_at_unix: admission_market.observed_at_unix,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            market: admission_market,
            fee_schedule: compact.fee_schedule,
            receipts: AdmissionReceipts {
                gamma: test_receipt(1001),
                clob_long: test_receipt(1002),
                clob_compact: test_receipt(1003),
            },
        };
        let order_book = crate::clob_book::OrderBook::from_book_json(&book).unwrap();
        let asks = order_book.ladder().unwrap();
        let cash_before = CollateralAmount::from_decimal_exact(dec!(80)).unwrap();
        let sized = plan_sized_buy(
            &asks,
            admission.fee_schedule,
            BuySizing::Contract { contracts: 5 },
            &[cash_before],
            admission.market.minimum_order_size,
            admission.market.minimum_tick_size,
            Price::new(dec!(0.15)).unwrap(),
            Price::new(dec!(0.85)).unwrap(),
            Price::new(dec!(0.50)).unwrap(),
            Price::new(dec!(0.4949)).unwrap(),
        )
        .unwrap();

        let start_frame = test_frame(
            1,
            EVALUATED_MS.div_euclid(1_000) - 100,
            PaperLogRecord::QualificationStarted(Box::new(start.clone())),
        );
        let mut prior = risk_economic(
            OPEN_CONDITION,
            CollateralAmount::from_decimal_exact(dec!(20)).unwrap(),
        );
        prior.sizing.principal = CollateralAmount::from_decimal_exact(dec!(20)).unwrap();
        prior.fee.expected_fee = CollateralAmount::ZERO;
        prior.observation = Some(ObservationEvidence {
            source_receipt: test_receipt(1008),
            complete_bound_receipt: test_receipt(1008),
            observed_unix_ms: EVALUATED_MS - 60_000,
            provenance: "rest_poll".to_owned(),
        });
        prior.applied_configuration_hash = start.hot_config_hash.clone();
        let prior_operation = crate::paper_recovery::PaperFillOperationIdentity {
            leader_wallet: continuation.facts.wallet,
            source_trade_id: SourceTradeId(format!("g2:{}", "b".repeat(64))),
            observed_at_bucket: continuation.facts.source_epoch - 1,
        };
        let prior_payload = FinancialPayload::Fill {
            operation: prior_operation.clone(),
            economic: prior.clone(),
        };
        let prepared = PaperLogRecord::FinancialPrepared {
            expected_authority: crate::paper_recovery::ExpectedAuthority {
                qualification_start_receipt: start_frame.receipt,
                prior_completed_prepared_sequence: None,
            },
            payload: prior_payload.clone(),
        };
        let prepared_frame = test_frame(2, EVALUATED_MS.div_euclid(1_000) - 50, prepared.clone());
        let result = FinancialResult::Fill {
            canonical: crate::paper_recovery::CanonicalFillResult {
                outcome: "applied".to_owned(),
                bankroll: dec!(80),
                applied_prepared_seq: prepared_frame.receipt.sequence,
                quantity: ShareAmount::from_whole(40).unwrap(),
                principal: CollateralAmount::from_decimal_exact(dec!(20)).unwrap(),
                fee: CollateralAmount::ZERO,
                fill_price: Price::new(dec!(0.50)).unwrap(),
            },
        };
        let final_frame = test_frame(
            3,
            EVALUATED_MS.div_euclid(1_000) - 40,
            PaperLogRecord::FinancialFinal {
                prepared_receipt: prepared_frame.receipt,
                result: result.clone(),
            },
        );
        let facts = vec![CompletedFinancialFact {
            prepared_receipt: prepared_frame.receipt,
            final_receipt: final_frame.receipt,
            payload: prior_payload,
            result,
        }];
        let frames = vec![start_frame, prepared_frame, final_frame];
        let financial = financial_state_at_prefix(
            start.starting_bankroll.to_decimal(),
            &facts,
            frames[2].receipt,
        )
        .unwrap();
        let paper_prefix = &frames[..];
        let risk_context = RiskReplayContext {
            cash: financial.cash,
            positions: &financial.positions,
            fills: &financial.fills,
            settlements: &financial.settlements,
            last_completed: financial.last_completed,
            start_receipt: frames[0].receipt,
            paper_prefix,
            source: &source,
            prepared_received_unix_ms: EVALUATED_MS,
            start_hot_config_hash: &start.hot_config_hash,
            financial_semantic_version: start.financial_semantic_version,
        };
        let snapshot =
            replayed_financial_snapshot(&risk_context, EVALUATED_MS.div_euclid(1_000)).unwrap();
        let prices = replayed_risk_prices(
            &[test_receipt(1007)],
            EVALUATED_MS,
            &snapshot.positions,
            &source,
        )
        .await
        .unwrap();
        let era = paper_era(frames.clone());
        let proposed_debit = sized.worst_case_all_in_debit().unwrap();
        let base = build_paper_risk_base(
            &era,
            continuation.facts.wallet,
            CONDITION,
            proposed_debit,
            10_000,
        )
        .unwrap();
        let snapshot = build_paper_risk_snapshot_from_source_receipts(
            &base,
            &snapshot,
            &era,
            &prices,
            |receipt| Ok(source.get(&receipt.sequence.0).unwrap().received_unix_ms),
            EVALUATED_MS.div_euclid(1_000),
            false,
        )
        .unwrap();
        let risk = RiskAudit {
            financial_prefix: frames[2].receipt,
            decision: match evaluate_risk(&snapshot) {
                RiskDecision::Approved => RiskDecisionAudit::Approved,
                RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
            },
            snapshot,
            price_receipts: vec![test_receipt(1007)],
            evaluated_at_unix_ms: EVALUATED_MS,
        };
        // A REST-polled observation IS its own complete-read bound (bucket_commit's producer).
        let observation = ObservationEvidence {
            source_receipt: test_receipt(1005),
            complete_bound_receipt: test_receipt(1005),
            observed_unix_ms: EVALUATED_MS - 5_000,
            provenance: "rest_poll".to_owned(),
        };
        let economic = EconomicPrepared::compose(EconomicInputs {
            market: MarketSelection {
                condition_id: PolymarketConditionId(CONDITION.to_owned()),
                outcome_index: 0,
                token_id: admission.market.ordered_outcome_token_ids[0].clone(),
                side: Side::Buy,
                market_id: CONDITION.to_owned(),
            },
            admission: &admission,
            plan: &sized.ladder,
            book_receipt: test_receipt(1004),
            observation: Some(observation.clone()),
            sizing_mode: SizingModeAudit::Contract { contracts: 5 },
            budget: sized.budget,
            slippage_rate: Decimal::ZERO,
            risk,
            cash_before,
            price_impact_cap_bps: 100,
            chase_ceiling: Price::new(dec!(0.50)).unwrap(),
            band_floor: Price::new(dec!(0.15)).unwrap(),
            band_ceiling_exclusive: Price::new(dec!(0.85)).unwrap(),
            applied_configuration_hash: start.hot_config_hash.clone(),
        })
        .unwrap();
        let (signal, mode) = reconstruct_winner_follow_signal(&continuation).unwrap();
        let error = pe_strategy_winner_follow::WinnerFollowStrategy::new(
            continuation
                .facts
                .applied_configuration
                .winner_follow_config(),
        )
        .evaluate_at_price(
            &signal,
            economic.sizing.all_in_price,
            continuation.facts.frozen_basis.win_rate_p,
            economic.risk.snapshot.clone(),
            continuation.facts.frozen_basis.bankroll,
            mode,
        )
        .expect_err("the replayed financial loss must decline");
        let terminal = crate::decision_replay::TerminalDispositionEvidence::declined(
            &error,
            WinnerFollowDecisionInputs::Evaluated {
                economic: Box::new(economic),
            },
        );
        let terminal_disposition = terminal.disposition.clone();
        let post_commit_inputs_json =
            crate::decision_replay::DecisionEvidenceAccumulator::new(&continuation.facts)
                .render(
                    crate::decision_replay::AuthorityEvidence::not_read("strategy_declined"),
                    terminal,
                )
                .unwrap();
        let facts_json = serde_json::to_string(&continuation.facts).unwrap();
        let frozen_inputs_json =
            format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap());
        let decision = replay_decision_pending(&DecisionPendingRow {
            source_trade_id: continuation.facts.source_trade_id.clone(),
            semantic_revision: continuation.facts.semantic_revision.clone(),
            wallet: continuation.facts.wallet,
            source_epoch: continuation.facts.source_epoch,
            frozen_inputs_json,
            post_commit_inputs_json,
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some(terminal_disposition),
            updated_at_unix: 1_700_000_001,
        })
        .unwrap();
        DeclineFixture {
            decision,
            start,
            frames,
            source,
            facts,
            observations: HashMap::from([(
                continuation.facts.source_trade_id.clone(),
                observation,
            )]),
        }
    }

    fn evaluated_economic(
        decision: &crate::decision_replay::ReplayedDecision,
    ) -> &EconomicPrepared {
        decision
            .post_boundary
            .body
            .terminal
            .decline
            .as_ref()
            .and_then(|decline| match &decline.inputs {
                WinnerFollowDecisionInputs::Evaluated { economic } => Some(economic.as_ref()),
                WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
            })
            .expect("fixture must contain evaluated economics")
    }

    fn winner_follow_policy_fixture() -> (DecisionContinuationV3, EconomicPrepared) {
        let (mut continuation, _) = classification_fixture();
        continuation.facts.applied_configuration.sizing_mode =
            pe_strategy_winner_follow::SizingMode::Contract { contracts: 5 };
        continuation.facts.applied_configuration.sizing_contracts = 5;
        continuation.facts.applied_configuration.slippage_rate = Decimal::ZERO;
        continuation.facts.applied_configuration_hash =
            continuation.facts.applied_configuration.canonical_hash();

        let frozen = &continuation.facts;
        let mut economic = risk_economic(
            &frozen.market_id.0.0,
            CollateralAmount::from_decimal_exact(dec!(2.5)).unwrap(),
        );
        let shares = ShareAmount::from_whole(5).unwrap();
        economic.applied_configuration_hash = frozen.applied_configuration_hash.clone();
        economic.sizing.mode = SizingModeAudit::Contract { contracts: 5 };
        economic.sizing.minimum_shares = shares;
        economic.sizing.expected_shares = shares;
        economic.sizing.slippage_rate = Decimal::ZERO;
        economic.ladder.minimum_shares = shares;
        economic
            .ladder
            .used_asks
            .first_mut()
            .expect("fixture must contain one ask")
            .shares = shares;
        economic.balance.price_impact_cap_bps = 100;
        economic.balance.chase_ceiling = frozen.price;
        economic.balance.band_floor = Price::new(dec!(0.15)).unwrap();
        economic.balance.band_ceiling_exclusive = Price::new(dec!(0.85)).unwrap();
        (continuation, economic)
    }

    fn winner_follow_kelly_policy_fixture() -> (DecisionContinuationV3, EconomicPrepared) {
        let (mut continuation, mut economic) = winner_follow_policy_fixture();
        continuation.facts.applied_configuration.sizing_mode =
            pe_strategy_winner_follow::SizingMode::Kelly;
        continuation.facts.applied_configuration.sizing_contracts = 0;
        continuation.facts.applied_configuration_hash =
            continuation.facts.applied_configuration.canonical_hash();
        economic.applied_configuration_hash = continuation.facts.applied_configuration_hash.clone();
        economic.sizing.mode = SizingModeAudit::Kelly {
            fraction: KELLY_PAPER_BACKTEST,
            probability: continuation.facts.frozen_basis.win_rate_p,
        };
        (continuation, economic)
    }

    /// PASS: the REST observation (its own complete-read bound), Gamma, CLOB-long, compact-CLOB,
    /// and book receipts at the recorded risk clock are causal for a zero-position first trade.
    /// FAIL: moving any one receipt one millisecond after risk remains before Prepared but fails.
    #[tokio::test]
    async fn economic_receipts_are_bounded_by_the_recorded_risk_clock() {
        const RECEIPTS: [u64; 5] = [1005, 1001, 1002, 1003, 1004];
        let fixture = receipt_backed_decline_fixture().await;
        let continuation = fixture.decision.continuation.clone();
        let mut economic = evaluated_economic(&fixture.decision).clone();
        let frames = vec![fixture.frames[0].clone()];
        let empty_positions = Vec::<OpenPosition>::new();
        let empty_fills = Vec::<FillRow>::new();
        let empty_settlements = Vec::<SettledMarketRow>::new();
        let evaluated_at_unix_ms = economic.risk.evaluated_at_unix_ms;
        let prepared_received_unix_ms = evaluated_at_unix_ms + 1_000;
        let mut source = fixture.source.clone();
        for sequence in RECEIPTS {
            source.get_mut(&sequence).unwrap().received_unix_ms = evaluated_at_unix_ms;
        }
        // The observation's recorded clock must agree with its (moved) selected receipt, and the
        // recorded market/settlement clocks must equal the receipt-derived seconds.
        if let Some(observation) = economic.observation.as_mut() {
            observation.observed_unix_ms = evaluated_at_unix_ms;
        }
        economic.admission.market.observed_at_unix = evaluated_at_unix_ms.div_euclid(1_000);
        economic.admission.settlement.observed_at_unix = evaluated_at_unix_ms.div_euclid(1_000);
        economic.risk.financial_prefix = frames[0].receipt;
        economic.risk.price_receipts.clear();
        economic.balance.cash_before = fixture.start.starting_bankroll;
        economic.sizing.budget = fixture.start.starting_bankroll;

        let initial_context = RiskReplayContext {
            cash: fixture.start.starting_bankroll.to_decimal(),
            positions: &empty_positions,
            fills: &empty_fills,
            settlements: &empty_settlements,
            last_completed: None,
            start_receipt: frames[0].receipt,
            paper_prefix: &frames,
            source: &source,
            prepared_received_unix_ms,
            start_hot_config_hash: &fixture.start.hot_config_hash,
            financial_semantic_version: fixture.start.financial_semantic_version,
        };
        let snapshot =
            replayed_financial_snapshot(&initial_context, evaluated_at_unix_ms.div_euclid(1_000))
                .unwrap();
        assert!(snapshot.positions.is_empty());
        assert!(economic.risk.price_receipts.is_empty());
        let era = paper_era(frames.clone());
        let base = build_paper_risk_base(
            &era,
            continuation.facts.wallet,
            &economic.market.market_id,
            economic.balance.worst_case_debit,
            economic.risk.snapshot.per_trade_cap_bps,
        )
        .unwrap();
        economic.risk.snapshot = build_paper_risk_snapshot_from_source_receipts(
            &base,
            &snapshot,
            &era,
            &HashMap::new(),
            |receipt| Ok(source.get(&receipt.sequence.0).unwrap().received_unix_ms),
            evaluated_at_unix_ms.div_euclid(1_000),
            false,
        )
        .unwrap();
        economic.risk.decision = match evaluate_risk(&economic.risk.snapshot) {
            RiskDecision::Approved => RiskDecisionAudit::Approved,
            RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
        };
        let operation = crate::paper_recovery::PaperFillOperationIdentity {
            leader_wallet: continuation.facts.wallet,
            source_trade_id: continuation.facts.source_trade_id,
            observed_at_bucket: continuation.facts.source_epoch,
        };
        let context = RiskReplayContext {
            source: &source,
            ..initial_context
        };
        verify_economic(&operation, &economic, &context, false)
            .await
            .unwrap();

        for sequence in RECEIPTS {
            let mut late_source = source.clone();
            late_source.get_mut(&sequence).unwrap().received_unix_ms = evaluated_at_unix_ms + 1;
            let late_context = RiskReplayContext {
                source: &late_source,
                ..context
            };
            assert!(matches!(
                verify_economic(&operation, &economic, &late_context, false).await,
                Err(QualificationError::InsufficientEvidence(_))
            ));
        }
    }

    /// PASS: an EconomicPrepared whose copied policy equals the frozen continuation is accepted.
    /// FAIL: the untampered fixture is rejected by qualification policy comparison.
    #[tokio::test]
    async fn winner_follow_policy_accepts_untampered_frozen_configuration() {
        let fixture = receipt_backed_decline_fixture().await;
        verify_winner_follow_economic_policy(
            &fixture.decision.continuation,
            evaluated_economic(&fixture.decision),
        )
        .unwrap();
    }

    /// PASS: changing only Contract sizing from five to ten fails closed.
    #[tokio::test]
    async fn winner_follow_policy_rejects_tampered_contract_quantity() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut economic = evaluated_economic(&fixture.decision).clone();
        economic.sizing.mode = SizingModeAudit::Contract { contracts: 10 };
        assert!(
            verify_winner_follow_economic_policy(&fixture.decision.continuation, &economic,)
                .is_err()
        );
    }

    /// PASS: changing only the copied slippage rate fails closed.
    #[tokio::test]
    async fn winner_follow_policy_rejects_tampered_slippage_rate() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut economic = evaluated_economic(&fixture.decision).clone();
        economic.sizing.slippage_rate = dec!(0.01);
        assert!(
            verify_winner_follow_economic_policy(&fixture.decision.continuation, &economic,)
                .is_err()
        );
    }

    /// PASS: changing only the copied price-impact cap fails closed.
    #[tokio::test]
    async fn winner_follow_policy_rejects_tampered_price_impact_cap() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut economic = evaluated_economic(&fixture.decision).clone();
        economic.balance.price_impact_cap_bps = 10_000;
        assert!(
            verify_winner_follow_economic_policy(&fixture.decision.continuation, &economic,)
                .is_err()
        );
    }

    /// PASS: changing only the copied price-band floor fails closed.
    #[tokio::test]
    async fn winner_follow_policy_rejects_tampered_price_band_floor() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut economic = evaluated_economic(&fixture.decision).clone();
        economic.balance.band_floor = Price::ZERO;
        assert!(
            verify_winner_follow_economic_policy(&fixture.decision.continuation, &economic,)
                .is_err()
        );
    }

    /// PASS: changing only the copied exclusive price-band ceiling fails closed.
    #[tokio::test]
    async fn winner_follow_policy_rejects_tampered_price_band_ceiling() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut economic = evaluated_economic(&fixture.decision).clone();
        economic.balance.band_ceiling_exclusive = Price::ONE;
        assert!(
            verify_winner_follow_economic_policy(&fixture.decision.continuation, &economic,)
                .is_err()
        );
    }

    /// PASS: changing only the copied no-chase ceiling from 0.50 to 0.90 fails closed.
    #[tokio::test]
    async fn winner_follow_policy_rejects_tampered_chase_ceiling() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut economic = evaluated_economic(&fixture.decision).clone();
        economic.balance.chase_ceiling = Price::new(dec!(0.90)).unwrap();
        assert!(
            verify_winner_follow_economic_policy(&fixture.decision.continuation, &economic,)
                .is_err()
        );
    }

    /// PASS: changing only the copied Kelly fraction fails closed.
    #[test]
    fn winner_follow_policy_rejects_tampered_kelly_fraction() {
        let (continuation, mut economic) = winner_follow_kelly_policy_fixture();
        assert!(matches!(
            economic.sizing.mode,
            SizingModeAudit::Kelly { .. }
        ));
        if let SizingModeAudit::Kelly { fraction, .. } = &mut economic.sizing.mode {
            *fraction = KELLY_NORMAL;
        }
        assert!(verify_winner_follow_economic_policy(&continuation, &economic).is_err());
    }

    /// PASS: changing only the copied Kelly probability fails closed.
    #[test]
    fn winner_follow_policy_rejects_tampered_probability() {
        let (continuation, mut economic) = winner_follow_kelly_policy_fixture();
        assert!(matches!(
            economic.sizing.mode,
            SizingModeAudit::Kelly { .. }
        ));
        if let SizingModeAudit::Kelly { probability, .. } = &mut economic.sizing.mode {
            *probability = Probability::new(dec!(0.70)).unwrap();
        }
        assert!(verify_winner_follow_economic_policy(&continuation, &economic).is_err());
    }

    /// PASS: the strategy's five-contract allocation equals the signed economic plan.
    /// FAIL: changing only the plan quantity to ten contracts fails closed.
    #[test]
    fn winner_follow_intent_allocation_must_equal_the_sized_plan() {
        let (continuation, mut economic) = winner_follow_policy_fixture();
        let (signal, mode) =
            verify_winner_follow_economic_policy(&continuation, &economic).unwrap();
        let intent = pe_strategy_winner_follow::WinnerFollowStrategy::new(
            continuation
                .facts
                .applied_configuration
                .winner_follow_config(),
        )
        .evaluate_at_price(
            &signal,
            economic.sizing.all_in_price,
            continuation.facts.frozen_basis.win_rate_p,
            economic.risk.snapshot.clone(),
            continuation.facts.frozen_basis.bankroll,
            mode,
        )
        .unwrap();
        verify_winner_follow_intent_plan(&intent, &economic, &continuation.facts.source_trade_id)
            .unwrap();

        economic.sizing.minimum_shares = ShareAmount::from_whole(10).unwrap();
        assert!(
            verify_winner_follow_intent_plan(
                &intent,
                &economic,
                &continuation.facts.source_trade_id,
            )
            .is_err()
        );
    }

    /// PASS: the strategy limit bounds the exact economic ladder limit.
    /// FAIL: moving only the ladder limit above the strategy limit fails closed.
    #[test]
    fn winner_follow_intent_limit_must_bound_the_sized_plan() {
        let (continuation, mut economic) = winner_follow_policy_fixture();
        let (signal, mode) =
            verify_winner_follow_economic_policy(&continuation, &economic).unwrap();
        let intent = pe_strategy_winner_follow::WinnerFollowStrategy::new(
            continuation
                .facts
                .applied_configuration
                .winner_follow_config(),
        )
        .evaluate_at_price(
            &signal,
            economic.sizing.all_in_price,
            continuation.facts.frozen_basis.win_rate_p,
            economic.risk.snapshot.clone(),
            continuation.facts.frozen_basis.bankroll,
            mode,
        )
        .unwrap();
        verify_winner_follow_intent_plan(&intent, &economic, &continuation.facts.source_trade_id)
            .unwrap();

        economic.ladder.limit_price = Price::new(dec!(0.51)).unwrap();
        assert!(
            verify_winner_follow_intent_plan(
                &intent,
                &economic,
                &continuation.facts.source_trade_id,
            )
            .is_err()
        );
    }

    /// PASS: a fill whose market resolves after its recorded risk prefix replays against the open
    /// position at that prefix, while an absent or regressing receipt is insufficient evidence.
    /// FAIL: replay uses the post-resolution state, or either falsifier reaches economic replay.
    #[tokio::test]
    async fn fill_risk_replay_is_bounded_by_its_recorded_financial_prefix() {
        const EVALUATED_MS: i64 = 1_800_000_010_000;
        let mut fixture = receipt_backed_decline_fixture().await;
        let continuation = fixture.decision.continuation.clone();
        let economic = fixture
            .decision
            .post_boundary
            .body
            .terminal
            .decline
            .as_ref()
            .and_then(|decline| match &decline.inputs {
                WinnerFollowDecisionInputs::Evaluated { economic, .. } => {
                    Some(economic.as_ref().clone())
                }
                WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
            })
            .expect("receipt-backed fixture must contain evaluated inputs");
        let recorded_prefix = fixture.frames[2].receipt;
        assert_eq!(economic.risk.financial_prefix, recorded_prefix);

        let open_condition = fixture
            .facts
            .first()
            .and_then(|fact| match &fact.payload {
                FinancialPayload::Fill { economic, .. } => {
                    Some(economic.market.condition_id.clone())
                }
                FinancialPayload::Resolution { .. } => None,
            })
            .expect("fixture must begin with a fill");
        let resolution_payload = FinancialPayload::Resolution {
            condition_id: open_condition,
            payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
            resolution_source_receipt: test_receipt(2_000),
        };
        let resolution_prepared = test_frame(
            4,
            EVALUATED_MS.div_euclid(1_000) + 1,
            PaperLogRecord::FinancialPrepared {
                expected_authority: crate::paper_recovery::ExpectedAuthority {
                    qualification_start_receipt: fixture.frames[0].receipt,
                    prior_completed_prepared_sequence: Some(EventSeq(2)),
                },
                payload: resolution_payload.clone(),
            },
        );
        let resolution_result = FinancialResult::Resolution {
            canonical: crate::paper_recovery::CanonicalResolutionResult {
                outcome: "applied".to_owned(),
                bankroll: dec!(120),
                applied_prepared_seq: resolution_prepared.receipt.sequence,
                credit: CollateralAmount::from_decimal_exact(dec!(40)).unwrap(),
                settled_at_unix: EVALUATED_MS.div_euclid(1_000) + 1,
            },
        };
        let resolution_final = test_frame(
            5,
            EVALUATED_MS.div_euclid(1_000) + 2,
            PaperLogRecord::FinancialFinal {
                prepared_receipt: resolution_prepared.receipt,
                result: resolution_result.clone(),
            },
        );
        fixture.facts.push(CompletedFinancialFact {
            prepared_receipt: resolution_prepared.receipt,
            final_receipt: resolution_final.receipt,
            payload: resolution_payload,
            result: resolution_result,
        });
        fixture.frames.push(resolution_prepared);
        fixture.frames.push(resolution_final);

        let operation = crate::paper_recovery::PaperFillOperationIdentity {
            leader_wallet: continuation.facts.wallet,
            source_trade_id: continuation.facts.source_trade_id.clone(),
            observed_at_bucket: continuation.facts.source_epoch,
        };
        fixture.frames.push(test_frame(
            6,
            EVALUATED_MS.div_euclid(1_000) + 3,
            PaperLogRecord::FinancialPrepared {
                expected_authority: crate::paper_recovery::ExpectedAuthority {
                    qualification_start_receipt: fixture.frames[0].receipt,
                    prior_completed_prepared_sequence: Some(EventSeq(4)),
                },
                payload: FinancialPayload::Fill {
                    operation: operation.clone(),
                    economic: economic.clone(),
                },
            },
        ));
        let prepared_index = fixture.frames.len() - 1;
        let mut previous_fill_prefix = None;
        let paper_prefix = recorded_fill_paper_prefix(
            &fixture.frames[..prepared_index],
            economic.risk.financial_prefix,
            economic.risk.evaluated_at_unix_ms,
            &mut previous_fill_prefix,
        )
        .unwrap();
        assert_eq!(paper_prefix.last().unwrap().receipt, recorded_prefix);

        let financial_at_prefix = financial_state_at_prefix(
            fixture.start.starting_bankroll.to_decimal(),
            &fixture.facts,
            recorded_prefix,
        )
        .unwrap();
        let post_resolution = financial_state_at_prefix(
            fixture.start.starting_bankroll.to_decimal(),
            &fixture.facts,
            fixture.frames[4].receipt,
        )
        .unwrap();
        assert_eq!(financial_at_prefix.positions.len(), 1);
        assert!(post_resolution.positions.is_empty());

        let replay = RiskReplayContext {
            cash: financial_at_prefix.cash,
            positions: &financial_at_prefix.positions,
            fills: &financial_at_prefix.fills,
            settlements: &financial_at_prefix.settlements,
            last_completed: financial_at_prefix.last_completed,
            start_receipt: fixture.frames[0].receipt,
            paper_prefix,
            source: &fixture.source,
            prepared_received_unix_ms: received_unix_ms(&fixture.frames[prepared_index].envelope)
                .unwrap(),
            start_hot_config_hash: &fixture.start.hot_config_hash,
            financial_semantic_version: fixture.start.financial_semantic_version,
        };
        assert_eq!(
            verify_economic(&operation, &economic, &replay, false)
                .await
                .unwrap(),
            economic
        );
        assert!(matches!(
            verify_economic(&operation, &economic, &replay, true).await,
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("risk decision is not an approval")
        ));

        assert!(matches!(
            recorded_fill_paper_prefix(
                &fixture.frames[..prepared_index],
                test_receipt(999),
                economic.risk.evaluated_at_unix_ms,
                &mut previous_fill_prefix,
            ),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("absent")
        ));
        assert!(matches!(
            recorded_fill_paper_prefix(
                &fixture.frames[..prepared_index],
                fixture.frames[0].receipt,
                economic.risk.evaluated_at_unix_ms,
                &mut previous_fill_prefix,
            ),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("regresses")
        ));
    }

    /// PASS: qualification replays every foreign-owner halt cause through the shared global
    /// overlay, and a recovered raw absolute-loss value remains blocked by its paper-owned latch.
    /// FAIL: any recorded globally clamped snapshot differs from causal qualification replay.
    #[tokio::test]
    async fn economic_risk_replay_applies_complete_global_halt_overlay() {
        let cases = [
            (
                RiskHaltOwner::LiveAccount(AccountId::new("live-absolute").unwrap()),
                RiskHaltCause::AbsoluteLoss,
            ),
            (
                RiskHaltOwner::LiveAccount(AccountId::new("live-intraday").unwrap()),
                RiskHaltCause::IntradayDrawdown,
            ),
            (
                RiskHaltOwner::LiveAccount(AccountId::new("live-rolling").unwrap()),
                RiskHaltCause::Rolling7dDrawdown,
            ),
            (
                RiskHaltOwner::LiveAccount(AccountId::new("live-latency").unwrap()),
                RiskHaltCause::CopyLatency,
            ),
            (RiskHaltOwner::Paper, RiskHaltCause::AbsoluteLoss),
        ];

        for (owner, cause) in cases {
            let fixture = receipt_backed_decline_fixture().await;
            let continuation = fixture.decision.continuation.clone();
            let mut economic = fixture
                .decision
                .post_boundary
                .body
                .terminal
                .decline
                .as_ref()
                .and_then(|decline| match &decline.inputs {
                    WinnerFollowDecisionInputs::Evaluated { economic, .. } => {
                        Some(economic.as_ref().clone())
                    }
                    WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
                })
                .expect("receipt-backed fixture must contain evaluated inputs");
            if owner == RiskHaltOwner::Paper {
                assert!(
                    economic.risk.snapshot.absolute_pnl_bps.0 > KILL_SWITCH_DRAWDOWN_BPS,
                    "the manual-latch case requires recovered raw absolute PnL"
                );
            }
            let halt = test_frame(
                4,
                economic.risk.evaluated_at_unix_ms.div_euclid(1_000) - 20,
                PaperLogRecord::RiskHaltChanged {
                    owner: owner.clone(),
                    cause,
                    state: crate::paper_recovery::HaltState::Engaged,
                    evidence: serde_json::json!({}),
                },
            );
            economic.risk.financial_prefix = halt.receipt;
            apply_global_risk_halts(
                &HashSet::from([(owner, cause)]),
                &mut economic.risk.snapshot,
            );
            economic.risk.decision = match evaluate_risk(&economic.risk.snapshot) {
                RiskDecision::Approved => RiskDecisionAudit::Approved,
                RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
            };
            let mut frames = fixture.frames.clone();
            frames.push(halt);
            let financial = financial_state_at_prefix(
                fixture.start.starting_bankroll.to_decimal(),
                &fixture.facts,
                economic.risk.financial_prefix,
            )
            .unwrap();
            let operation = crate::paper_recovery::PaperFillOperationIdentity {
                leader_wallet: continuation.facts.wallet,
                source_trade_id: continuation.facts.source_trade_id,
                observed_at_bucket: continuation.facts.source_epoch,
            };
            let replay = RiskReplayContext {
                cash: financial.cash,
                positions: &financial.positions,
                fills: &financial.fills,
                settlements: &financial.settlements,
                last_completed: financial.last_completed,
                start_receipt: frames[0].receipt,
                paper_prefix: &frames,
                source: &fixture.source,
                prepared_received_unix_ms: economic.risk.evaluated_at_unix_ms,
                start_hot_config_hash: &fixture.start.hot_config_hash,
                financial_semantic_version: fixture.start.financial_semantic_version,
            };

            assert_eq!(
                verify_economic(&operation, &economic, &replay, false)
                    .await
                    .unwrap(),
                economic
            );
        }
    }

    /// PASS: the verifier accepts a recorded Entry only when the shared complete-second
    /// classifier independently derives Entry from the causal ledger.
    #[test]
    fn complete_second_replay_accepts_matching_entry_classification() {
        let (continuation, mutation) = classification_fixture();
        let ledger = PositionLedger::new();
        let (_, expected) = ledger
            .simulate_all_or_none(std::slice::from_ref(&mutation))
            .unwrap();

        verify_complete_second_action(&ledger, &continuation, &[mutation], &expected).unwrap();
    }

    /// PASS: a continuation that records Entry is insufficient evidence when the causal ledger
    /// makes the same BUY an Add.
    #[test]
    fn complete_second_replay_rejects_wrong_recorded_entry_classification() {
        let (continuation, mutation) = classification_fixture();
        let frozen = &continuation.facts;
        let mut ledger = PositionLedger::new();
        ledger.replace_wallet_snapshot(
            frozen.wallet,
            HashMap::from([(
                MarketOutcomeId::new(frozen.market_id.clone(), frozen.outcome_id),
                PositionState {
                    long_contracts: ShareAmount::from_whole(1).unwrap(),
                    short_contracts: ShareAmount::ZERO,
                },
            )]),
        );
        let (_, expected) = ledger
            .simulate_all_or_none(std::slice::from_ref(&mutation))
            .unwrap();

        assert!(matches!(
            verify_complete_second_action(&ledger, &continuation, &[mutation], &expected),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("classification differs")
        ));
    }

    /// PASS: the frozen configuration, probability, bankroll, all-in price, and risk snapshot
    /// reproduce the successful Winner-Follow outcome used by the golden stream.
    #[test]
    fn winner_follow_replay_accepts_golden_compatible_positive_outcome() {
        let (continuation, economic) = winner_follow_policy_fixture();
        let frozen = &continuation.facts;
        // A REST-polled observation IS its own complete-read bound (bucket_commit's producer).
        let observation = ObservationEvidence {
            source_receipt: test_receipt(20),
            complete_bound_receipt: test_receipt(20),
            observed_unix_ms: frozen.source_epoch * 1_000,
            provenance: "rest_poll".to_owned(),
        };
        let fill = CompletedFill {
            final_receipt: test_receipt(22),
            operation: crate::paper_recovery::PaperFillOperationIdentity {
                leader_wallet: frozen.wallet,
                source_trade_id: frozen.source_trade_id.clone(),
                observed_at_bucket: frozen.source_epoch,
            },
            market_id: frozen.market_id.0.0.clone(),
            outcome_id: frozen.outcome_id.0,
            side: frozen.side,
            condition_id: frozen.market_id.0.0.clone(),
            delay_ms: 0,
            observation,
            economic,
        };

        verify_winner_follow_fill_decision(&continuation, &fill).unwrap();
    }

    /// PASS: a paper Entry whose receipt-backed financial loss blocks risk is sealed as a typed
    /// decline, and qualification accepts it only after reconstructing and re-executing the inputs.
    #[tokio::test]
    async fn winner_follow_replay_accepts_receipt_backed_decline() {
        let fixture = receipt_backed_decline_fixture().await;
        bind_final_receipts(
            std::slice::from_ref(&fixture.decision),
            &[],
            &fixture.observations,
            &fixture.start,
            &fixture.context(),
        )
        .await
        .unwrap();
    }

    /// PASS: evaluated inputs have no wrapper financial-prefix field, and changing the economic
    /// owner's prefix makes the decline insufficient even after recomputing its document hash.
    /// FAIL: an evaluated wrapper prefix deserializes, or the tampered economic core is certified.
    #[tokio::test]
    async fn winner_follow_replay_rejects_tampered_economic_financial_prefix() {
        let fixture = receipt_backed_decline_fixture().await;
        let decision = fixture.decision.clone();
        let mut body = decision.post_boundary.body;
        let decline = body.terminal.decline.as_mut().unwrap();
        let economic = match &mut decline.inputs {
            WinnerFollowDecisionInputs::Evaluated { economic } => Some(economic),
            WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
        }
        .expect("receipt-backed fixture must contain evaluated inputs");
        economic.risk.financial_prefix = fixture.frames[0].receipt;
        let mut duplicate = serde_json::to_value(&decline.inputs).unwrap();
        duplicate.as_object_mut().unwrap().insert(
            "financial_prefix".to_owned(),
            serde_json::to_value(fixture.frames[2].receipt).unwrap(),
        );
        assert!(serde_json::from_value::<WinnerFollowDecisionInputs>(duplicate).is_err());
        let post_commit_inputs_json = serde_json::to_string(
            &crate::decision_replay::DecisionPostBoundaryEvidence::from_body(body).unwrap(),
        )
        .unwrap();
        let facts_json = serde_json::to_string(&decision.continuation.facts).unwrap();
        let frozen_inputs_json =
            format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap());
        let recomputed = replay_decision_pending(&DecisionPendingRow {
            source_trade_id: decision.continuation.facts.source_trade_id.clone(),
            semantic_revision: decision.continuation.facts.semantic_revision.clone(),
            wallet: decision.continuation.facts.wallet,
            source_epoch: decision.continuation.facts.source_epoch,
            frozen_inputs_json,
            post_commit_inputs_json,
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some("no_fill".to_owned()),
            updated_at_unix: 1_700_000_001,
        })
        .unwrap();

        assert!(matches!(
            bind_final_receipts(
                &[recomputed],
                &[],
                &fixture.observations,
                &fixture.start,
                &fixture.context(),
            )
            .await,
            Err(QualificationError::InsufficientEvidence(_))
        ));
    }

    /// PASS: a decline whose recorded risk decision differs from evaluating its recorded snapshot
    /// is insufficient even when its other receipt-backed inputs remain unchanged.
    #[tokio::test]
    async fn winner_follow_replay_rejects_mismatched_decline_risk_decision() {
        let fixture = receipt_backed_decline_fixture().await;
        let mut decision = fixture.decision.clone();
        let economic = decision
            .post_boundary
            .body
            .terminal
            .decline
            .as_mut()
            .and_then(|decline| match &mut decline.inputs {
                WinnerFollowDecisionInputs::Evaluated { economic, .. } => Some(economic),
                WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
            })
            .expect("receipt-backed fixture must contain evaluated inputs");
        economic.risk.decision = RiskDecisionAudit::Approved;

        assert!(matches!(
            bind_final_receipts(
                &[decision],
                &[],
                &fixture.observations,
                &fixture.start,
                &fixture.context(),
            )
            .await,
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("risk decision differs from shared risk owner")
        ));
    }

    /// PASS: recomputing the terminal document hash after replacing `NoEdge` with another typed
    /// decline remains insufficient because qualification re-executes the shared decision.
    #[tokio::test]
    async fn winner_follow_replay_rejects_fabricated_snapshot_with_recomputed_hash() {
        let fixture = receipt_backed_decline_fixture().await;
        let decision = fixture.decision.clone();
        let mut row = DecisionPendingRow {
            source_trade_id: decision.continuation.facts.source_trade_id.clone(),
            semantic_revision: decision.continuation.facts.semantic_revision.clone(),
            wallet: decision.continuation.facts.wallet,
            source_epoch: decision.continuation.facts.source_epoch,
            frozen_inputs_json: {
                let facts_json = serde_json::to_string(&decision.continuation.facts).unwrap();
                format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap())
            },
            post_commit_inputs_json: String::new(),
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some("no_fill".to_owned()),
            updated_at_unix: 1_700_000_001,
        };
        let mut body = decision.post_boundary.body;
        let decline = body.terminal.decline.as_mut().unwrap();
        decline.outcome = pe_strategy_winner_follow::WinnerFollowDeclineAudit::NoEdge;
        let economic = match &mut decline.inputs {
            WinnerFollowDecisionInputs::Evaluated { economic, .. } => Some(economic),
            WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
        }
        .expect("receipt-backed fixture must contain evaluated inputs");
        economic.risk.snapshot.leader_exposure_bps = BasisPoints::ZERO;
        row.post_commit_inputs_json = serde_json::to_string(
            &crate::decision_replay::DecisionPostBoundaryEvidence::from_body(body).unwrap(),
        )
        .unwrap();
        let recomputed = replay_decision_pending(&row).unwrap();

        assert!(matches!(
            bind_final_receipts(
                &[recomputed],
                &[],
                &fixture.observations,
                &fixture.start,
                &fixture.context(),
            )
            .await,
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("risk snapshot differs from causal replay")
        ));
    }

    /// PASS: dropping the valid page that supplied an open-position price leaves that market with
    /// no consulted request and is insufficient evidence, even after recomputing the document hash.
    #[tokio::test]
    async fn winner_follow_replay_rejects_unavailable_inputs_when_reconstruction_succeeds() {
        let fixture = receipt_backed_decline_fixture().await;
        let decision = fixture.decision.clone();
        let mut row = DecisionPendingRow {
            source_trade_id: decision.continuation.facts.source_trade_id.clone(),
            semantic_revision: decision.continuation.facts.semantic_revision.clone(),
            wallet: decision.continuation.facts.wallet,
            source_epoch: decision.continuation.facts.source_epoch,
            frozen_inputs_json: {
                let facts_json = serde_json::to_string(&decision.continuation.facts).unwrap();
                format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap())
            },
            post_commit_inputs_json: String::new(),
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some("no_fill".to_owned()),
            updated_at_unix: 1_800_000_011,
        };
        let mut body = decision.post_boundary.body;
        let decline = body.terminal.decline.as_mut().unwrap();
        let economic = match &decline.inputs {
            WinnerFollowDecisionInputs::Evaluated { economic } => Some(economic),
            WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
        }
        .expect("receipt-backed fixture must contain evaluated inputs");
        let financial_prefix = economic.risk.financial_prefix;
        decline.outcome =
            pe_strategy_winner_follow::WinnerFollowDeclineAudit::RiskInputsUnavailable;
        decline.inputs = WinnerFollowDecisionInputs::RiskInputsUnavailable {
            cause: RiskInputsUnavailable::PriceMissing,
            evidence: WinnerFollowRiskInputEvidence {
                financial_prefix: Some(financial_prefix),
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: economic.risk.evaluated_at_unix_ms,
                proposed_debit: economic.balance.worst_case_debit,
                per_trade_cap_bps: economic.risk.snapshot.per_trade_cap_bps,
            },
        };
        row.post_commit_inputs_json = serde_json::to_string(
            &crate::decision_replay::DecisionPostBoundaryEvidence::from_body(body).unwrap(),
        )
        .unwrap();
        let recomputed = replay_decision_pending(&row).unwrap();

        assert!(matches!(
            bind_final_receipts(
                &[recomputed],
                &[],
                &fixture.observations,
                &fixture.start,
                &fixture.context(),
            )
            .await,
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("no consulted page for an open position")
        ));
    }

    /// PASS: after a restart, an earlier fresh price is not inferred into the empty cache; the
    /// current request-bound transport failure proves the runtime's PriceMissing decline.
    #[tokio::test]
    async fn winner_follow_replay_accepts_post_restart_failed_price_attempt() {
        const OPEN_CONDITION: &str =
            "0x5c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ef";
        const EVALUATED_MS: i64 = 1_800_000_010_000;
        let mut fixture = receipt_backed_decline_fixture().await;
        let failed_receipt = test_receipt(1009);
        let request_url = format!(
            "https://offline.invalid/markets?condition_ids={OPEN_CONDITION}&limit={GAMMA_BATCH_LIMIT_PARAM}"
        );
        fixture.source.insert(
            failed_receipt.sequence.0,
            source_observation(
                failed_receipt.sequence.0,
                EVALUATED_MS,
                GAMMA_MARKETS_SOURCE_ID,
                GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
                GAMMA_PRICE_ATTEMPT_PARSER_VERSION,
                &gamma_price_failure_record(&request_url, "transport unavailable"),
            ),
        );
        let decision = fixture.decision.clone();
        let mut row = DecisionPendingRow {
            source_trade_id: decision.continuation.facts.source_trade_id.clone(),
            semantic_revision: decision.continuation.facts.semantic_revision.clone(),
            wallet: decision.continuation.facts.wallet,
            source_epoch: decision.continuation.facts.source_epoch,
            frozen_inputs_json: {
                let facts_json = serde_json::to_string(&decision.continuation.facts).unwrap();
                format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap())
            },
            post_commit_inputs_json: String::new(),
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some("no_fill".to_owned()),
            updated_at_unix: 1_800_000_011,
        };
        let mut body = decision.post_boundary.body;
        let decline = body.terminal.decline.as_mut().unwrap();
        let economic = match &decline.inputs {
            WinnerFollowDecisionInputs::Evaluated { economic } => Some(economic),
            WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
        }
        .expect("receipt-backed fixture must contain evaluated inputs");
        let financial_prefix = economic.risk.financial_prefix;
        decline.outcome =
            pe_strategy_winner_follow::WinnerFollowDeclineAudit::RiskInputsUnavailable;
        decline.inputs = WinnerFollowDecisionInputs::RiskInputsUnavailable {
            cause: RiskInputsUnavailable::PriceMissing,
            evidence: WinnerFollowRiskInputEvidence {
                financial_prefix: Some(financial_prefix),
                price_receipts: vec![failed_receipt],
                evaluated_at_unix_ms: economic.risk.evaluated_at_unix_ms,
                proposed_debit: economic.balance.worst_case_debit,
                per_trade_cap_bps: economic.risk.snapshot.per_trade_cap_bps,
            },
        };
        row.post_commit_inputs_json = serde_json::to_string(
            &crate::decision_replay::DecisionPostBoundaryEvidence::from_body(body).unwrap(),
        )
        .unwrap();
        let replayed = replay_decision_pending(&row).unwrap();

        bind_final_receipts(
            &[replayed],
            &[],
            &fixture.observations,
            &fixture.start,
            &fixture.context(),
        )
        .await
        .unwrap();
    }

    /// PASS: a malformed strict price records its request-bound Gamma receipt and replays as the
    /// same typed PriceMissing decline.
    #[tokio::test]
    async fn winner_follow_replay_accepts_receipt_backed_missing_price_decline() {
        const OPEN_CONDITION: &str =
            "0x5c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ef";
        let mut fixture = receipt_backed_decline_fixture().await;
        let raw = serde_json::to_vec(&serde_json::json!([{
            "conditionId": OPEN_CONDITION,
            "active": true,
            "closed": false,
            "outcomePrices": "[\"1.1\",\"-0.1\"]"
        }]))
        .unwrap();
        fixture.source.get_mut(&1007).unwrap().payload = gamma_price_page_record(
            &format!(
                "https://offline.invalid/markets?condition_ids={OPEN_CONDITION}&limit={GAMMA_BATCH_LIMIT_PARAM}"
            ),
            1_800_000_007_000,
            &raw,
            true,
        );
        let decision = fixture.decision.clone();
        let mut row = DecisionPendingRow {
            source_trade_id: decision.continuation.facts.source_trade_id.clone(),
            semantic_revision: decision.continuation.facts.semantic_revision.clone(),
            wallet: decision.continuation.facts.wallet,
            source_epoch: decision.continuation.facts.source_epoch,
            frozen_inputs_json: {
                let facts_json = serde_json::to_string(&decision.continuation.facts).unwrap();
                format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap())
            },
            post_commit_inputs_json: String::new(),
            state: DecisionPendingState::Terminal,
            terminal_disposition: Some("no_fill".to_owned()),
            updated_at_unix: 1_800_000_011,
        };
        let mut body = decision.post_boundary.body;
        let decline = body.terminal.decline.as_mut().unwrap();
        let economic = match &decline.inputs {
            WinnerFollowDecisionInputs::Evaluated { economic } => Some(economic),
            WinnerFollowDecisionInputs::RiskInputsUnavailable { .. } => None,
        }
        .expect("receipt-backed fixture must contain evaluated inputs");
        let financial_prefix = economic.risk.financial_prefix;
        let price_receipt = economic.risk.price_receipts[0];
        decline.outcome =
            pe_strategy_winner_follow::WinnerFollowDeclineAudit::RiskInputsUnavailable;
        decline.inputs = WinnerFollowDecisionInputs::RiskInputsUnavailable {
            cause: RiskInputsUnavailable::PriceMissing,
            evidence: WinnerFollowRiskInputEvidence {
                financial_prefix: Some(financial_prefix),
                price_receipts: vec![price_receipt],
                evaluated_at_unix_ms: economic.risk.evaluated_at_unix_ms,
                proposed_debit: economic.balance.worst_case_debit,
                per_trade_cap_bps: economic.risk.snapshot.per_trade_cap_bps,
            },
        };
        row.post_commit_inputs_json = serde_json::to_string(
            &crate::decision_replay::DecisionPostBoundaryEvidence::from_body(body).unwrap(),
        )
        .unwrap();
        let replayed = replay_decision_pending(&row).unwrap();

        bind_final_receipts(
            &[replayed],
            &[],
            &fixture.observations,
            &fixture.start,
            &fixture.context(),
        )
        .await
        .unwrap();
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
                financial_prefix: test_receipt(1),
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

    #[derive(Clone, Copy)]
    enum PaperWrapperCase {
        Valid,
        MissingAdmission,
        DuplicateAdmission,
        RelabeledAdmission,
        RefusedAdmission,
        TamperedEconomic,
        PreStartAdmission,
        PreStartBaseline,
    }

    fn boundary_baseline(
        account_id: &AccountId,
        binding: &pe_execution_core::CredentialBindingIdentity,
        account_state: &LiveAccountStateAudit,
        cutoff_unix: i64,
    ) -> LiveJournalPayload {
        let account_binding = pe_execution_core::live_journal::LiveAccountBindingAudit::new(
            account_id.clone(),
            binding.clone(),
            WalletAddress([1; 20]),
            "11".repeat(32),
        );
        LiveJournalPayload::AccountPortfolioMarked(Box::new(
            pe_execution_core::AccountPortfolioMarkedAudit {
                kind: pe_execution_core::MarkKind::Baseline,
                cutoff_unix,
                account_binding: account_binding.clone(),
                account_state: account_state.clone(),
                venue_positions: Vec::new(),
                venue_position_evidence:
                    pe_execution_core::live_journal::LivePositionEvidenceAudit {
                        requested_wallet: account_binding.custody_wallet,
                        pages: Vec::new(),
                    },
                marked_positions: Vec::new(),
                prices: Vec::new(),
                equity: account_state.collateral_balance,
            },
        ))
    }

    fn authenticated_boundary_state(
        account_id: &AccountId,
        binding: &pe_execution_core::CredentialBindingIdentity,
        observed_at: OffsetDateTime,
    ) -> LiveAccountStateAudit {
        let account_binding = pe_execution_core::live_journal::LiveAccountBindingAudit::new(
            account_id.clone(),
            binding.clone(),
            WalletAddress([1; 20]),
            "11".repeat(32),
        );
        let spender = CanaryV2Client::standard_spender().unwrap();
        let collateral = CollateralAmount::from_atomic(10_000_000);
        let response = |endpoint_kind: &str,
                        path: &str,
                        ordered_query: Vec<(String, String)>,
                        body: Vec<u8>| {
            RawHttpAttempt::Response(RawHttpResponse {
                source_id: "polymarket-clob-v2".to_owned(),
                endpoint_kind: endpoint_kind.to_owned(),
                method: "GET".to_owned(),
                path: path.to_owned(),
                ordered_query,
                status: 200,
                headers: Vec::new(),
                body,
                attempt_ordinal: 1,
                source_at: None,
                observed_at,
                received_at: observed_at,
                schema_version: 1,
                parser_version: 1,
                adapter_version: SDK_VERSION.to_owned(),
            })
        };
        let evidence = vec![
            response(
                "geoblock",
                "/api/geoblock",
                Vec::new(),
                br#"{"blocked":false,"country":"US"}"#.to_vec(),
            ),
            response(
                "closed-only",
                "/auth/ban-status/closed-only",
                Vec::new(),
                br#"{"closed_only":false}"#.to_vec(),
            ),
            response(
                "balance-allowance",
                "/balance-allowance",
                vec![
                    ("asset_type".to_owned(), "COLLATERAL".to_owned()),
                    ("signature_type".to_owned(), "3".to_owned()),
                ],
                serde_json::to_vec(&serde_json::json!({
                    "balance": collateral.atomic().to_string(),
                    "allowances": { spender.clone(): collateral.atomic().to_string() },
                }))
                .unwrap(),
            ),
        ];
        let request_descriptor_hashes = evidence
            .iter()
            .map(|attempt| {
                pe_execution_core::live_journal::request_descriptor_hash(
                    &account_binding.request_descriptor_for_attempt(attempt),
                )
                .unwrap()
            })
            .collect();
        LiveAccountStateAudit {
            observed_at,
            closed_only: false,
            geoblocked: false,
            selected_spender: spender,
            collateral_balance: collateral,
            allowance: collateral,
            reconciled_free_collateral: collateral,
            schema_version: 1,
            parser_version: 1,
            evidence_hashes: pe_execution_core::http_attempt_hashes(&evidence).unwrap(),
            evidence,
            request_descriptor_hashes,
        }
    }

    fn append_empty_position_sources(
        writer: &mut Writer,
        binding: &pe_execution_core::live_journal::LiveAccountBindingAudit,
        observed_at: OffsetDateTime,
    ) -> LivePositionEvidenceAudit {
        let pages = [false, true]
            .into_iter()
            .map(|redeemable| {
                let ordered_query = vec![
                    ("user".to_owned(), binding.custody_wallet.clone()),
                    ("sizeThreshold".to_owned(), "0".to_owned()),
                    ("includeArchived".to_owned(), "true".to_owned()),
                    ("limit".to_owned(), "500".to_owned()),
                    ("sortBy".to_owned(), "TOKENS".to_owned()),
                    ("sortDirection".to_owned(), "ASC".to_owned()),
                    ("redeemable".to_owned(), redeemable.to_string()),
                    ("offset".to_owned(), "0".to_owned()),
                ];
                let request_identity = format!(
                    "/positions?user={}&sizeThreshold=0&includeArchived=true&limit=500&sortBy=TOKENS&sortDirection=ASC&redeemable={redeemable}&offset=0",
                    binding.custody_wallet
                );
                let request = binding.request_descriptor(
                    "GET",
                    "/positions",
                    format!("redeemable={redeemable}"),
                    0,
                    ordered_query,
                );
                let payload = serde_json::to_vec(&serde_json::json!({
                    "request": request,
                    "body": b"[]".to_vec(),
                }))
                .unwrap();
                let receipt = writer
                    .append_synced(EnvelopeIn {
                        source_id: SourceId("polymarket.data.complete-positions".to_owned()),
                        schema_version: 2,
                        parser_version: 1,
                        observed_at: SourceTimestamp(observed_at),
                        received_at: ReceivedAt(observed_at),
                        content_type: ContentType::Json,
                        payload,
                    })
                    .unwrap();
                LivePositionPageAudit {
                    request_identity,
                    receipt,
                }
            })
            .collect();
        LivePositionEvidenceAudit {
            requested_wallet: binding.custody_wallet.clone(),
            pages,
        }
    }

    fn verify_paper_wrapper_case(
        case: PaperWrapperCase,
    ) -> Result<VerifiedLiveEvidence, QualificationError> {
        let temp = tempfile::tempdir().unwrap();
        let live_path = temp.path().join("live.log");
        let source_path = temp.path().join("source.log");
        let journal = LiveJournal::open(&live_path).unwrap();
        drop(Writer::open(&source_path).unwrap());
        let source_prefix = TailBinding::from(&Scanner::verify(&source_path).unwrap());
        let mut start = started("hot");

        let account_id = AccountId::new("paper-account").unwrap();
        let operation = crate::paper_recovery::PaperFillOperationIdentity {
            leader_wallet: start.membership[0],
            source_trade_id: SourceTradeId("g2:paper-wrapper".to_owned()),
            observed_at_bucket: 1,
        };
        let mut economic = test_economic("paper-condition", test_receipt(31), test_receipt(32));
        if matches!(case, PaperWrapperCase::TamperedEconomic) {
            economic.admission.market.schema_version = 0;
        }
        let dispatch_id = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
            &TraderId(operation.leader_wallet).to_string(),
            &operation.source_trade_id.0,
            &economic.market.market_id,
            u16::from(economic.market.outcome_index),
            economic.market.side,
            operation.observed_at_bucket,
        );
        let identity = LiveOrderIdentity {
            dispatch_id: dispatch_id.clone(),
            idempotency_key: LiveOrderIdentity::idempotency_key_for(&dispatch_id, &account_id),
            quote_id: "paper-quote".to_owned(),
            config_hash: economic.applied_configuration_hash.clone(),
            decision_hash: "paper-decision".to_owned(),
            evidence_hashes: vec!["paper-evidence".to_owned()],
            fill_projection: Some(Box::new(LiveFillProjectionIdentity {
                leader_wallet: operation.leader_wallet.to_string(),
                source_trade_id: Some(operation.source_trade_id.0.clone()),
                market_id: economic.market.market_id.clone(),
                outcome_id: i64::from(u16::from(economic.market.outcome_index)),
                side: "buy".to_owned(),
            })),
            schema_version: 1,
            parser_version: 1,
        };
        let binding = pe_execution_core::CredentialBindingIdentity {
            version: 1,
            key_id: "paper-key".to_owned(),
        };
        let observed_at = OffsetDateTime::from_unix_timestamp(1).unwrap();
        let account_state = LiveAccountStateAudit {
            observed_at,
            closed_only: false,
            geoblocked: false,
            selected_spender: "paper-spender".to_owned(),
            collateral_balance: economic.balance.cash_before,
            allowance: economic.balance.cash_before,
            reconciled_free_collateral: economic.balance.cash_before,
            schema_version: 1,
            parser_version: 1,
            evidence: Vec::new(),
            request_descriptor_hashes: Vec::new(),
            evidence_hashes: Vec::new(),
        };
        let wrapper = LiveOrderPreparedAudit {
            identity: identity.clone(),
            frozen_binding: binding.clone(),
            economic: economic.clone(),
            account_state: account_state.clone(),
            prepared: PreparedPolymarketBuy {
                condition_id: economic.market.condition_id.clone(),
                outcome_id: OutcomeId(u16::from(economic.market.outcome_index)),
                token_id: economic.market.token_id.clone(),
                maker: "paper-maker".to_owned(),
                signer: "paper-signer".to_owned(),
                funder: "paper-funder".to_owned(),
                verifying_contract: "paper-spender".to_owned(),
                spender: "paper-spender".to_owned(),
                exchange_domain_version: 2,
                neg_risk: economic.admission.market.neg_risk,
                side: "BUY".to_owned(),
                salt: "1".to_owned(),
                timestamp_ms: 1_000,
                expiration: "0".to_owned(),
                maker_collateral: economic.balance.worst_case_debit,
                taker_shares: economic.sizing.expected_shares,
                limit_price: economic.ladder.limit_price,
                minimum_tick_size: economic.admission.market.minimum_tick_size,
                signature_type: 3,
                order_type: "FOK".to_owned(),
                post_only: false,
                defer_exec: false,
                metadata: "paper-metadata".to_owned(),
                builder: "paper-builder".to_owned(),
                order_hash: "paper-order-hash".to_owned(),
                post_body_hash: "paper-post-body-hash".to_owned(),
                sdk_version: "paper-sdk".to_owned(),
                sdk_archive_sha256: "paper-sdk-sha".to_owned(),
                metadata_hashes: identity.evidence_hashes.clone(),
                worst_case_debit: economic.balance.worst_case_debit,
            },
        };
        let mut admission = LiveAdmissionEvaluationAudit {
            identity,
            frozen_binding: binding.clone(),
            current_binding: binding.clone(),
            requested_mode: LiveControlMode::LiveTiny,
            effective_mode: LiveControlMode::LiveTiny,
            economic: economic.clone(),
            account_state: Some(account_state.clone()),
            account_read_failure_evidence: Vec::new(),
            account_read_failure_request_descriptor_hashes: Vec::new(),
            account_read_failure_evidence_hashes: Vec::new(),
            verdict: LiveAdmissionVerdict::Approved,
        };
        if matches!(case, PaperWrapperCase::RelabeledAdmission) {
            admission.identity.dispatch_id = "relabeled-dispatch".to_owned();
        }
        if matches!(case, PaperWrapperCase::RefusedAdmission) {
            admission.verdict = LiveAdmissionVerdict::Refused(
                pe_execution_core::LiveAdmissionRefusal::ModeNotArmed,
            );
        }
        if matches!(case, PaperWrapperCase::PreStartAdmission) {
            journal
                .append(
                    account_id.clone(),
                    observed_at,
                    LiveJournalPayload::AdmissionEvaluated(Box::new(admission.clone())),
                )
                .unwrap();
        }
        if matches!(case, PaperWrapperCase::PreStartBaseline) {
            journal
                .append(
                    account_id.clone(),
                    observed_at,
                    boundary_baseline(&account_id, &binding, &account_state, 1),
                )
                .unwrap();
        }
        start.live_prefix = TailBinding::from(&LiveJournal::verified_tail(&live_path).unwrap());
        if !matches!(
            case,
            PaperWrapperCase::MissingAdmission | PaperWrapperCase::PreStartAdmission
        ) {
            journal
                .append(
                    account_id.clone(),
                    observed_at,
                    LiveJournalPayload::AdmissionEvaluated(Box::new(admission.clone())),
                )
                .unwrap();
        }
        if matches!(case, PaperWrapperCase::DuplicateAdmission) {
            journal
                .append(
                    account_id.clone(),
                    observed_at,
                    LiveJournalPayload::AdmissionEvaluated(Box::new(admission)),
                )
                .unwrap();
        }
        journal
            .append(
                account_id,
                OffsetDateTime::from_unix_timestamp(2).unwrap(),
                LiveJournalPayload::OrderPrepared(Box::new(wrapper)),
            )
            .unwrap();
        drop(journal);
        let live_prefix = TailBinding::from(&LiveJournal::verified_tail(&live_path).unwrap());
        let paper_frames = vec![test_frame(
            40,
            1,
            PaperLogRecord::FinancialPrepared {
                expected_authority: crate::paper_recovery::ExpectedAuthority {
                    qualification_start_receipt: test_receipt(30),
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Fill {
                    operation,
                    economic,
                },
            },
        )];

        verify_live_wrappers(
            &live_path,
            &source_path,
            &source_prefix,
            &live_prefix,
            &start,
            &paper_frames,
        )
    }

    /// PASS: a paper-mode AdmissionEvaluated and Prepared pair with no Baseline qualifies through
    /// the shared admission classifier.
    #[test]
    fn qualification_accepts_paper_wrapper_without_baseline() {
        let verified = verify_paper_wrapper_case(PaperWrapperCase::Valid).unwrap();
        assert_eq!(verified.wrapper_facts.len(), 1);
        let retained_pre_start_baseline =
            verify_paper_wrapper_case(PaperWrapperCase::PreStartBaseline).unwrap();
        assert_eq!(retained_pre_start_baseline.wrapper_facts.len(), 1);
    }

    /// PASS: a retained pre-Start Baseline is excluded while a later post-Start Baseline remains
    /// available as the sole ordinary-account era boundary.
    #[test]
    fn qualification_live_slice_retains_only_post_start_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let live_path = temp.path().join("live.log");
        let journal = LiveJournal::open(&live_path).unwrap();
        let account_id = AccountId::new("baseline-boundary").unwrap();
        let binding = pe_execution_core::CredentialBindingIdentity {
            version: 1,
            key_id: "key".to_owned(),
        };
        let account_state = authenticated_boundary_state(
            &account_id,
            &binding,
            OffsetDateTime::from_unix_timestamp(1).unwrap(),
        );
        let account_binding = pe_execution_core::live_journal::LiveAccountBindingAudit::new(
            account_id.clone(),
            binding.clone(),
            WalletAddress([1; 20]),
            "11".repeat(32),
        );
        let source_path = temp.path().join("source.log");
        let mut source_writer = Writer::open(&source_path).unwrap();
        let position_evidence = append_empty_position_sources(
            &mut source_writer,
            &account_binding,
            OffsetDateTime::from_unix_timestamp(1).unwrap(),
        );
        journal
            .append(
                account_id.clone(),
                OffsetDateTime::from_unix_timestamp(1).unwrap(),
                boundary_baseline(&account_id, &binding, &account_state, 1),
            )
            .unwrap();
        let start_prefix = TailBinding::from(&LiveJournal::verified_tail(&live_path).unwrap());
        let mut post_start_baseline = boundary_baseline(
            &AccountId::new("baseline-boundary").unwrap(),
            &binding,
            &account_state,
            2,
        );
        let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut post_start_baseline else {
            unreachable!();
        };
        mark.venue_position_evidence = position_evidence;
        journal
            .append(
                account_id,
                OffsetDateTime::from_unix_timestamp(2).unwrap(),
                post_start_baseline,
            )
            .unwrap();
        drop(journal);
        drop(source_writer);
        let sealed_prefix = TailBinding::from(&LiveJournal::verified_tail(&live_path).unwrap());

        let events = replay_live_prefix(&live_path, &start_prefix, &sealed_prefix).unwrap();
        assert_eq!(events.len(), 1);
        let LiveJournalPayload::AccountPortfolioMarked(mark) = &events[0].payload else {
            unreachable!();
        };
        assert_eq!(mark.kind, pe_execution_core::MarkKind::Baseline);
        assert_eq!(mark.cutoff_unix, 2);

        let source_prefix = TailBinding::from(&Scanner::verify(&source_path).unwrap());
        let mut start = started("hot");
        start.live_prefix = start_prefix;
        let verified = verify_live_wrappers(
            &live_path,
            &source_path,
            &source_prefix,
            &sealed_prefix,
            &start,
            &[],
        )
        .unwrap();
        assert!(verified.wrapper_facts.is_empty());
    }

    /// PASS: missing, duplicate, relabeled, refused, and economically tampered paper admissions
    /// each fail qualification as insufficient evidence.
    #[test]
    fn qualification_rejects_invalid_paper_wrapper_admissions() {
        let cases = [
            (PaperWrapperCase::MissingAdmission, "no AdmissionEvaluated"),
            (
                PaperWrapperCase::DuplicateAdmission,
                "multiple AdmissionEvaluated",
            ),
            (PaperWrapperCase::RelabeledAdmission, "identity differs"),
            (PaperWrapperCase::PreStartAdmission, "no AdmissionEvaluated"),
            (
                PaperWrapperCase::RefusedAdmission,
                "verdict is not Approved",
            ),
            (
                PaperWrapperCase::TamperedEconomic,
                "pre-account admission replay did not require account state",
            ),
        ];
        for (case, expected_reason) in cases {
            let error = verify_paper_wrapper_case(case).err().unwrap();
            assert!(matches!(
                error,
                QualificationError::InsufficientEvidence(reason)
                    if reason.contains(expected_reason)
            ));
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

    /// PASS: rows for one trade split across two pages produce the one semantic revision of the
    /// complete multiset, not two contradictory page-local revisions.
    #[test]
    fn decision_keys_aggregate_a_split_group_across_the_complete_read() {
        let first_target = activity_row("0xtarget", "target", "2", "1", "0xtx", 100);
        let second_target = activity_row("0xtarget", "target", "3", "1.5", "0xtx", 100);
        let first_payload = full_page_with_target(first_target, 100);
        let second_payload = activity_payload(vec![second_target]);
        let first = activity_observation(1, &first_payload);
        let second = activity_observation(2, &second_payload);
        let observations = BTreeMap::from([(1, first.clone()), (2, second.clone())]);
        let expected = parsed_aggregates(&[&first_payload, &second_payload])
            .into_iter()
            .find(|aggregate| {
                aggregate
                    .group_id
                    .components()
                    .condition_id
                    .as_ref()
                    .is_some_and(|condition| condition.0 == "0xtarget")
            })
            .unwrap();
        let target_id = expected.group_id.key().clone();
        let read = read_scope(
            100,
            vec![
                (
                    page_occurrence(&first, "page-0", &first_payload),
                    page_evidence("page-0", &first_payload, Some(99), 100, 0),
                ),
                (
                    page_occurrence(&second, "page-500", &second_payload),
                    page_evidence(
                        "page-500",
                        &second_payload,
                        Some(99),
                        100,
                        RECONCILIATION_PAGE_LIMIT,
                    ),
                ),
            ],
            [target_id.clone()],
        );

        let source_universe = source_trade_universe(EventSeq(2), &observations).unwrap();
        let keys = decision_keys_from_source_observations(
            None,
            EventSeq(2),
            &observations,
            &source_universe,
            &[read],
        )
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].1, target_id);
        assert_eq!(keys[0].2, expected.semantic_revision.as_str());
    }

    /// PASS: byte-identical pages at distinct durable occurrences contribute their full
    /// multiplicity to the complete-read semantic revision.
    #[test]
    fn decision_keys_preserve_repeated_page_multiplicity() {
        let target = activity_row("0xtarget", "target", "2", "1", "0xtx", 100);
        let repeated_payload = full_page_with_target(target, 100);
        let terminal_payload = activity_payload(Vec::new());
        let first = activity_observation(1, &repeated_payload);
        let second = activity_observation(2, &repeated_payload);
        let terminal = activity_observation(3, &terminal_payload);
        let observations = BTreeMap::from([
            (1, first.clone()),
            (2, second.clone()),
            (3, terminal.clone()),
        ]);
        let expected = parsed_aggregates(&[&repeated_payload, &repeated_payload])
            .into_iter()
            .find(|aggregate| {
                aggregate
                    .group_id
                    .components()
                    .condition_id
                    .as_ref()
                    .is_some_and(|condition| condition.0 == "0xtarget")
            })
            .unwrap();
        let target_id = expected.group_id.key().clone();
        let read = read_scope(
            100,
            vec![
                (
                    page_occurrence(&first, "page-0", &repeated_payload),
                    page_evidence("page-0", &repeated_payload, Some(99), 100, 0),
                ),
                (
                    page_occurrence(&second, "page-500", &repeated_payload),
                    page_evidence(
                        "page-500",
                        &repeated_payload,
                        Some(99),
                        100,
                        RECONCILIATION_PAGE_LIMIT,
                    ),
                ),
                (
                    page_occurrence(&terminal, "page-1000", &terminal_payload),
                    page_evidence(
                        "page-1000",
                        &terminal_payload,
                        Some(99),
                        100,
                        RECONCILIATION_PAGE_LIMIT * 2,
                    ),
                ),
            ],
            [target_id],
        );

        let source_universe = source_trade_universe(EventSeq(3), &observations).unwrap();
        let keys = decision_keys_from_source_observations(
            None,
            EventSeq(3),
            &observations,
            &source_universe,
            &[read],
        )
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].2, expected.semantic_revision.as_str());
        assert_eq!(expected.row_count, 2);
    }

    /// PASS: a saturated parent segment remains receipt evidence but only its recursively fetched
    /// complete child contributes rows to the production-equivalent aggregate.
    #[test]
    fn decision_keys_exclude_saturated_parent_rows() {
        let parent_target = activity_row("0xtarget", "target", "2", "1", "0xtx", 100);
        let child_target = activity_row("0xtarget", "target", "3", "1.5", "0xtx", 100);
        let parent_payload = full_page_with_target(parent_target, 100);
        let child_payload = activity_payload(vec![child_target]);
        let mut observations = BTreeMap::new();
        let mut pages = Vec::new();
        for index in 0..=ACTIVITY_MAX_OFFSET / RECONCILIATION_PAGE_LIMIT {
            let sequence = u64::from(index) + 1;
            let offset = index * RECONCILIATION_PAGE_LIMIT;
            let url = format!("parent-{offset}");
            let observation = activity_observation(sequence, &parent_payload);
            pages.push((
                page_occurrence(&observation, &url, &parent_payload),
                page_evidence(&url, &parent_payload, None, 100, offset),
            ));
            observations.insert(sequence, observation);
        }
        let child_sequence = u64::from(ACTIVITY_MAX_OFFSET / RECONCILIATION_PAGE_LIMIT) + 2;
        let child = activity_observation(child_sequence, &child_payload);
        pages.push((
            page_occurrence(&child, "child", &child_payload),
            page_evidence("child", &child_payload, Some(99), 100, 0),
        ));
        observations.insert(child_sequence, child);
        let older_sequence = child_sequence + 1;
        let older_payload = activity_payload(Vec::new());
        let older = activity_observation(older_sequence, &older_payload);
        pages.push((
            page_occurrence(&older, "older", &older_payload),
            page_evidence("older", &older_payload, None, 99, 0),
        ));
        observations.insert(older_sequence, older);
        let expected = parsed_aggregates(&[&child_payload]).remove(0);
        let target_id = expected.group_id.key().clone();
        let read = read_scope(100, pages, [target_id]);

        let source_universe =
            source_trade_universe(EventSeq(older_sequence), &observations).unwrap();
        let keys = decision_keys_from_source_observations(
            None,
            EventSeq(older_sequence),
            &observations,
            &source_universe,
            &[read],
        )
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].2, expected.semantic_revision.as_str());
        assert_eq!(expected.share_sum.to_decimal(), dec!(3));
    }

    /// PASS: cursor overlap may repeat a pre-Start trade in the first post-Start complete read,
    /// but only the trade whose first raw observation is post-Start becomes a required key.
    #[test]
    fn decision_keys_exclude_pre_start_trade_repeated_after_start() {
        let old = activity_row("0xold", "old", "2", "1", "0xoldtx", 100);
        let new = activity_row("0xnew", "new", "4", "2", "0xnewtx", 101);
        let pre_start_payload = activity_payload(vec![old.clone()]);
        let post_start_payload = activity_payload(vec![old, new]);
        let pre_start = activity_observation(1, &pre_start_payload);
        let post_start = activity_observation(2, &post_start_payload);
        let observations = BTreeMap::from([(1, pre_start.clone()), (2, post_start.clone())]);
        let old_id = parsed_aggregates(&[&pre_start_payload])
            .remove(0)
            .group_id
            .key()
            .clone();
        let new_id = parsed_aggregates(&[&post_start_payload])
            .into_iter()
            .find(|aggregate| {
                aggregate
                    .group_id
                    .components()
                    .condition_id
                    .as_ref()
                    .is_some_and(|condition| condition.0 == "0xnew")
            })
            .unwrap()
            .group_id
            .key()
            .clone();
        let pre_read = read_scope(
            100,
            vec![(
                page_occurrence(&pre_start, "pre", &pre_start_payload),
                page_evidence("pre", &pre_start_payload, Some(99), 100, 0),
            )],
            [old_id],
        );
        let post_read = read_scope(
            101,
            vec![(
                page_occurrence(&post_start, "post", &post_start_payload),
                page_evidence("post", &post_start_payload, Some(99), 101, 0),
            )],
            [new_id.clone()],
        );

        let source_universe = source_trade_universe(EventSeq(2), &observations).unwrap();
        let keys = decision_keys_from_source_observations(
            Some(EventSeq(1)),
            EventSeq(2),
            &observations,
            &source_universe,
            &[pre_read, post_read],
        )
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].1, new_id);
        assert_eq!(keys[0].0, 2);
    }

    /// PASS: an unrelated pre-Start page in a complete read does not hide the exact decision row
    /// for a trade whose first raw observation is on the read's post-Start terminal page.
    #[test]
    fn decision_rows_include_post_start_trade_from_a_straddling_complete_read() {
        let old = activity_row("0xold", "old", "2", "1", "0xoldtx", 100);
        let new = activity_row("0xnew", "new", "4", "2", "0xnewtx", 100);
        let first_payload = full_page_with_target(old, 100);
        let second_payload = activity_payload(vec![new]);
        let first = activity_observation(1, &first_payload);
        let second = activity_observation(2, &second_payload);
        let observations = BTreeMap::from([(1, first.clone()), (2, second.clone())]);
        let expected = parsed_aggregates(&[&first_payload, &second_payload])
            .into_iter()
            .find(|aggregate| {
                aggregate
                    .group_id
                    .components()
                    .condition_id
                    .as_ref()
                    .is_some_and(|condition| condition.0 == "0xnew")
            })
            .unwrap();
        let page_pairs = [
            (
                page_occurrence(&first, "page-0", &first_payload),
                page_evidence("page-0", &first_payload, Some(99), 100, 0),
            ),
            (
                page_occurrence(&second, "page-500", &second_payload),
                page_evidence(
                    "page-500",
                    &second_payload,
                    Some(99),
                    100,
                    RECONCILIATION_PAGE_LIMIT,
                ),
            ),
        ];
        let (mut continuation, _) = classification_fixture();
        continuation.facts.source_trade_id = expected.group_id.key().clone();
        continuation.facts.semantic_revision = expected.semantic_revision.as_str().to_owned();
        continuation.facts.wallet =
            WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        continuation.facts.source_epoch = 100;
        continuation.facts.decision_inputs = serde_json::json!({
            "fixed_end": 100,
            "pages": page_pairs
                .iter()
                .map(|(_, evidence)| evidence)
                .collect::<Vec<_>>(),
        });
        continuation.page_occurrences = page_pairs
            .iter()
            .map(|(occurrence, _)| occurrence.clone())
            .collect();

        let temp = tempfile::tempdir().unwrap();
        let paper_state = temp.path().join("paper.db");
        let state = PaperStateDb::open(&paper_state).unwrap();
        let connection = rusqlite::Connection::open(&paper_state).unwrap();
        connection
            .execute(
                "INSERT INTO decision_pending
                    (source_trade_id, semantic_revision, wallet_hex, source_epoch,
                     frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition,
                     updated_at_unix)
                 VALUES (?1, ?2, ?3, 100, ?4, '{}', 'terminal', 'no_fill', 101)",
                rusqlite::params![
                    continuation.facts.source_trade_id.0,
                    continuation.facts.semantic_revision,
                    continuation.facts.wallet.to_string(),
                    serde_json::to_string(&continuation).unwrap(),
                ],
            )
            .unwrap();
        let start = TailBinding {
            physical_tail: 1,
            last_sequence: Some(EventSeq(1)),
            last_hash: first.receipt.this_hash.to_hex().to_string(),
        };
        let sealed = TailBinding {
            physical_tail: 2,
            last_sequence: Some(EventSeq(2)),
            last_hash: second.receipt.this_hash.to_hex().to_string(),
        };

        let rows =
            decision_rows_from_source_observations(&state, &start, &sealed, &observations).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_trade_id, expected.group_id.key().clone());
        assert_eq!(
            rows[0].semantic_revision,
            expected.semantic_revision.as_str()
        );
    }

    /// PASS: the immutable source prefix independently requires its decision key after the
    /// terminal decision and every source-keyed companion projection are deleted.
    #[test]
    fn source_trade_cannot_disappear_with_every_projection_deleted() {
        let payload = br#"[{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","type":"TRADE","conditionId":"0xcondition","asset":"123","side":"BUY","size":"5","usdcSize":"2.5","price":"0.5","timestamp":"1700000100","transactionHash":"0xabc","outcomeIndex":"0"}]"#;
        let temp = tempfile::tempdir().unwrap();
        let source_path = temp.path().join("source.log");
        let mut source_writer = Writer::open(&source_path).unwrap();
        let start = TailBinding::from(&Scanner::verify(&source_path).unwrap());
        let at = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();
        let source_receipt = source_writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(at),
                received_at: ReceivedAt(at),
                content_type: ContentType::Json,
                payload: payload.to_vec(),
            })
            .unwrap();
        drop(source_writer);
        let sealed = TailBinding::from(&Scanner::verify(&source_path).unwrap());
        let observation = activity_observation(source_receipt.sequence.0, payload);
        let observation = SourceObservation {
            receipt: source_receipt,
            ..observation
        };
        let observations = BTreeMap::from([(0, observation.clone())]);
        let target_id = parsed_aggregates(&[payload])
            .remove(0)
            .group_id
            .key()
            .clone();
        let read = read_scope(
            1_700_000_100,
            vec![(
                page_occurrence(&observation, "page", payload),
                page_evidence("page", payload, Some(1_700_000_099), 1_700_000_100, 0),
            )],
            [target_id.clone()],
        );
        let source_universe = source_trade_universe(EventSeq(0), &observations)
            .expect("valid source universe derives from the activity row");
        let keys = decision_keys_from_source_observations(
            None,
            EventSeq(0),
            &observations,
            &source_universe,
            &[read],
        )
        .expect("valid complete activity read derives one decision key");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].1, target_id);

        let state = PaperStateDb::open(&temp.path().join("paper.db")).unwrap();
        let connection = rusqlite::Connection::open(temp.path().join("paper.db")).unwrap();
        let source_trade_id = keys[0].1.0.clone();
        connection
            .execute(
                "INSERT INTO activity_groups
                    (source_trade_id, transaction_hash, wallet_hex, source_epoch,
                     semantic_revision, activity_type, disposition, proof_json)
                 VALUES (?1, '0xabc', ?2, 1700000100, ?3, 'trade',
                         'decision_pending', '{}')",
                rusqlite::params![
                    source_trade_id,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    keys[0].2,
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO activity_group_revisions
                    (source_trade_id, semantic_revision, transaction_hash, disposition,
                     proof_json, recorded_at_unix)
                 VALUES (?1, ?2, '0xabc', 'decision_pending', '{}', 1700000101)",
                rusqlite::params![source_trade_id, keys[0].2],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO no_copy_dispositions
                    (source_trade_id, provenance, age_secs, reason, recorded_at_unix)
                 VALUES (?1, 'rest_poll', 0, 'no_edge', 1700000101)",
                rusqlite::params![source_trade_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO entry_gate_results
                    (source_trade_id, wallet_hex, market_id, source_epoch, result,
                     history_consumed)
                 VALUES (?1, ?2, '0xcondition', 1700000100, 'admitted', 1)",
                rusqlite::params![
                    source_trade_id,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO decision_pending
                    (source_trade_id, semantic_revision, wallet_hex, source_epoch,
                     frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition,
                     updated_at_unix)
                 VALUES (?1, ?2, ?3, 1700000100, '{}', '{\"decline\":\"no_edge\"}',
                         'terminal', 'no_fill', 1700000101)",
                rusqlite::params![
                    source_trade_id,
                    keys[0].2,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ],
            )
            .unwrap();
        for table in [
            "decision_pending",
            "no_copy_dispositions",
            "entry_gate_results",
            "activity_group_revisions",
            "activity_groups",
        ] {
            connection
                .execute(
                    &format!("DELETE FROM {table} WHERE source_trade_id = ?1"),
                    rusqlite::params![source_trade_id],
                )
                .unwrap();
        }
        assert!(matches!(
            decision_rows_for_source_prefix(&state, &source_path, &start, &sealed),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("has no terminal decision_pending row")
        ));
    }

    /// PASS: a self-consistent economic record made under H2 cannot qualify Start H1.
    #[test]
    fn economic_configuration_must_equal_start_configuration() {
        let economic = risk_economic(
            "market",
            CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
        );
        assert!(matches!(
            verify_economic_configuration(&economic, "different-start-hash", 1),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("configuration differs")
        ));
        assert!(matches!(
            verify_economic_configuration(&economic, "config", 2),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("financial semantics")
        ));
    }

    /// PASS: an arbitrary nonempty object is not typed membership evidence.
    #[test]
    fn arbitrary_membership_evidence_is_insufficient() {
        assert!(matches!(
            verify_membership_change_evidence(
                MembershipReason::FullRerank,
                &[],
                &[],
                1,
                Some(7),
                &serde_json::json!({"x": 1}),
                &MembershipEvidenceContext {
                    source: &BTreeMap::new(),
                    current_membership: &HashSet::new(),
                },
            ),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("evidence schema is invalid")
        ));
    }

    /// PASS: knockout verification recomputes underperformance from receipt-bound fill and
    /// settlement inputs; removing those inputs cannot preserve the publisher's reason.
    #[test]
    fn knockout_replays_retained_demotion_inputs() {
        let wallet = WalletAddress([9; 20]);
        let evaluated_at_unix = 1_900_000_000;
        let quantity = ShareAmount::from_whole(100).unwrap();
        let principal = CollateralAmount::from_decimal_exact(dec!(50)).unwrap();
        let mut fills = Vec::new();
        let mut settlements = Vec::new();
        for index in 0..10u64 {
            let market_id = MarketId(VenueMarketId(format!("losing-market-{index}")));
            fills.push(crate::paper_recovery::KnockoutFillArtifact::from_row(
                &FillRow {
                    idempotency_key: format!(
                        "wf|{wallet}|g2:{}|{}|0|buy|{}",
                        "a".repeat(64),
                        market_id.0.0,
                        evaluated_at_unix - 10
                    ),
                    market_id: market_id.clone(),
                    outcome_id: OutcomeId(0),
                    side: Side::Buy,
                    quantity,
                    fill_price: Price::new(dec!(0.5)).unwrap(),
                    principal,
                    fee: CollateralAmount::ZERO,
                    event_seq: EventSeq(index + 1),
                    prepared_seq: EventSeq(index + 1),
                    source_receipt_seq: None,
                },
            ));
            settlements.push(crate::paper_recovery::KnockoutSettlementArtifact {
                market_id,
                outcome_prices: vec![Decimal::ZERO, Decimal::ONE],
                credit_applied: Decimal::ZERO,
                settled_at_unix: evaluated_at_unix - 1,
            });
        }
        let artifact = KnockoutCausalArtifact {
            wallet,
            evaluated_at_unix,
            last_trade_unix: Some(evaluated_at_unix - 1),
            inactivity_threshold_secs: 259_200,
            inactivity_hard_cap_secs: 604_800,
            demotion_min_trades: 10,
            demotion_cb_alpha: dec!(0.10),
            demotion_pnl_window_secs: 2_592_000,
            fills,
            settlements,
        };
        let observation = |sequence: u64, artifact: &KnockoutCausalArtifact| {
            let payload = serde_json::to_vec(artifact).unwrap();
            let receipt = AppendReceipt {
                sequence: EventSeq(sequence),
                this_hash: blake3::hash(&payload),
            };
            let at = OffsetDateTime::from_unix_timestamp(evaluated_at_unix).unwrap();
            (
                receipt,
                SourceObservation {
                    receipt,
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    received_unix_ms: evaluated_at_unix * 1_000,
                    source_id: KNOCKOUT_CAUSAL_SOURCE_ID.to_owned(),
                    schema_version: MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
                    parser_version: MEMBERSHIP_ARTIFACT_PARSER_VERSION,
                    content_type: ContentType::Json,
                    payload,
                },
            )
        };
        let (receipt, source_observation) = observation(1, &artifact);
        let evidence = SealedMembershipEvidence::knockout_backfill(
            vec![crate::paper_recovery::SealedKnockoutEvidence {
                wallet,
                reason: MembershipReason::KnockoutUnderperformance,
                causal_receipt: receipt,
            }],
            None,
            Vec::new(),
        )
        .unwrap();
        verify_membership_change_evidence(
            MembershipReason::KnockoutUnderperformance,
            &[wallet],
            &[],
            1,
            None,
            &evidence,
            &MembershipEvidenceContext {
                source: &BTreeMap::from([(1, source_observation)]),
                current_membership: &HashSet::from([wallet]),
            },
        )
        .unwrap();

        let mut omitted = artifact;
        omitted.fills.clear();
        omitted.settlements.clear();
        let (receipt, source_observation) = observation(2, &omitted);
        let evidence = SealedMembershipEvidence::knockout_backfill(
            vec![crate::paper_recovery::SealedKnockoutEvidence {
                wallet,
                reason: MembershipReason::KnockoutUnderperformance,
                causal_receipt: receipt,
            }],
            None,
            Vec::new(),
        )
        .unwrap();
        assert!(matches!(
            verify_membership_change_evidence(
                MembershipReason::KnockoutUnderperformance,
                &[wallet],
                &[],
                1,
                None,
                &evidence,
                &MembershipEvidenceContext {
                    source: &BTreeMap::from([(2, source_observation)]),
                    current_membership: &HashSet::from([wallet]),
                },
            ),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("semantic owner rejects")
        ));
    }

    /// PASS: the verifier rederives even an empty initial-membership proof instead of trusting
    /// the caller-provided label.
    #[test]
    fn initial_membership_label_is_not_proof() {
        let mut start = started("config");
        start.membership.clear();
        start.membership_proofs_hash = "not-a-proof".to_owned();
        assert!(matches!(
            verify_initial_membership(&start),
            Err(QualificationError::InsufficientEvidence(reason))
                if reason.contains("immutable membership proof is invalid")
        ));
    }

    /// PASS: Start retains the exact accepted membership preimages, so deleting the mutable
    /// current position-validation projection after Start cannot invalidate the era proof.
    #[test]
    fn initial_membership_proof_does_not_read_current_projection() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("paper.db");
        let state = PaperStateDb::open(&state_path).unwrap();
        let wallet = WalletAddress([42; 20]);
        let at = 1_700_000_100;
        state
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{\"complete\":true}".to_owned(),
                updated_at_unix: at,
            })
            .unwrap();
        state.seed_cursors_if_absent(&[(wallet, at)]).unwrap();
        state
            .install_anchors(&[pe_paper_state::AnchorInstallRecord {
                wallet,
                balances: Vec::new(),
                activity_cutoff_unix: at,
                anchored_at_unix: at,
                ledger_hash_after: "ledger-at-start".to_owned(),
                positions_proof_hash: "positions-at-start".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "source-at-start".to_owned(),
                proof_json: "{\"anchor\":1}".to_owned(),
                recorded_at_unix: at,
            }])
            .unwrap();
        let mut start = started("config");
        start.membership = vec![wallet];
        start.membership_proofs_hash = derive_membership_proofs_hash(&state, &[wallet]).unwrap();

        let connection = rusqlite::Connection::open(&state_path).unwrap();
        connection
            .execute(
                "DELETE FROM position_validations WHERE wallet_hex = ?1",
                rusqlite::params![wallet.to_string()],
            )
            .unwrap();
        assert!(!state.position_validation_current(&wallet).unwrap());
        verify_initial_membership(&start).unwrap();
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
            MembershipReason::CapacityChange,
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
        let live_journal = temp.path().join("live_journal.log");
        let paper_state = temp.path().join("paper.db");
        let output = temp.path().join("qualification.json");
        let mut paper_writer = Writer::open(&paper_log).unwrap();
        let mut source_writer = Writer::open(&source_log).unwrap();
        let empty_paper_prefix = Scanner::verify(&paper_log).unwrap();
        let empty_source_prefix = Scanner::verify(&source_log).unwrap();
        let cutoff_unix = 86_400;
        let start_unix = cutoff_unix - 100;
        let state = PaperStateDb::open(&paper_state).unwrap();
        drop(pe_execution_core::LiveJournal::open(&live_journal).unwrap());
        let mut start = started("hot");
        let decision_wallet = start.membership[0];
        start.membership.clear();
        start.membership_proofs_hash = derive_membership_proofs_hash(&state, &[]).unwrap();
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
        let decision_evidence = state.seal_decision_evidence(&[]).unwrap();
        let seal_receipt = append_paper_record_at(
            &mut paper_writer,
            &PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
                start_receipt,
                source_prefix: TailBinding::from(&sealed_source_tail),
                financial_prefix: TailBinding::from(&sealed_financial_tail),
                live_prefix: TailBinding::from(&empty_source_prefix),
                decision_evidence_digest: blake3::hash(&decision_evidence).to_hex().to_string(),
                sealed_cutoff_unix: cutoff_unix,
                reason: SealReason::Complete,
            })),
            cutoff_unix + 1,
        );
        let options = QualifyOptions {
            paper_log: paper_log.clone(),
            source_log: source_log.clone(),
            live_journal: Some(live_journal),
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
        let continuation = crate::bucket_commit::DecisionContinuationFacts {
            source_trade_id: source_trade_id.clone(),
            semantic_revision: "semantic-v3".to_owned(),
            transaction_hash: "0xpost-seal".to_owned(),
            wallet: decision_wallet,
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
            serde_json::to_string(&crate::bucket_commit::DecisionContinuationV3::new(
                continuation,
                None,
                vec![crate::bucket_commit::PageOccurrence {
                    request_url: "https://example.invalid/activity?end=86400".to_owned(),
                    raw_hash: blake3::hash(&late_payload).to_hex().to_string(),
                    receipt: late_receipt,
                }],
            ))
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
                    decision_wallet.to_string(),
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

    /// PASS: successful and failed attempts inside one millisecond, the final fresh millisecond,
    /// and the exact TTL boundary retain the runtime classifier's outcome after receipt-bound
    /// evidence is reconstructed by qualification.
    #[tokio::test]
    async fn strict_price_millisecond_outcomes_round_trip_through_qualification() {
        const OBSERVED_NS: i128 = 100_000_900_000;
        const SAME_MILLISECOND_EVALUATED_NS: i128 = 100_000_950_000;
        const LAST_FRESH_EVALUATED_NS: i128 = 159_999_950_000;
        const STALE_BOUNDARY_EVALUATED_NS: i128 = 160_000_000_000;

        let market = MarketId(VenueMarketId("condition-millisecond".to_owned()));
        let position = PaperPositionRow {
            market_id: market.clone(),
            outcome_id: OutcomeId(0),
            long: ShareAmount::from_atomic(1_000_000),
            short: ShareAmount::ZERO,
        };
        let request_url = format!(
            "https://offline.invalid/markets?condition_ids={market}&limit={GAMMA_BATCH_LIMIT_PARAM}"
        );
        let raw =
            br#"[{"conditionId":"condition-millisecond","outcomePrices":"[\"0.4\",\"0.6\"]"}]"#;
        let observed_ms = i64::try_from(OBSERVED_NS.div_euclid(1_000_000)).unwrap();

        for (label, page_succeeded, evaluated_ns, expected_error) in [
            (
                "successful_same_millisecond",
                true,
                SAME_MILLISECOND_EVALUATED_NS,
                None,
            ),
            (
                "failed_same_millisecond",
                false,
                SAME_MILLISECOND_EVALUATED_NS,
                Some(RiskInputsUnavailable::PriceMissing),
            ),
            (
                "last_fresh_millisecond",
                true,
                LAST_FRESH_EVALUATED_NS,
                None,
            ),
            (
                "exact_ttl_boundary",
                true,
                STALE_BOUNDARY_EVALUATED_NS,
                Some(RiskInputsUnavailable::PriceStale),
            ),
        ] {
            let receipt = test_receipt(2_000);
            let evaluated_ms = i64::try_from(evaluated_ns.div_euclid(1_000_000)).unwrap();
            let mut runtime_entries = HashMap::new();
            if page_succeeded {
                runtime_entries.insert(
                    market.clone(),
                    StrictPriceInput {
                        strict_mids: Some(vec![
                            Price::new(dec!(0.4)).unwrap(),
                            Price::new(dec!(0.6)).unwrap(),
                        ]),
                        observed_at_unix_ms: i128::from(observed_ms),
                        receipt,
                        conflicting: false,
                    },
                );
            }
            let runtime = classify_strict_prices(
                &[MarketOutcomeId::new(market.clone(), OutcomeId(0))],
                i128::from(evaluated_ms),
                &runtime_entries,
            );
            let payload = if page_succeeded {
                gamma_price_page_record(&request_url, observed_ms, raw, true)
            } else {
                gamma_price_failure_record(&request_url, "transport unavailable")
            };
            let durable = source_observation(
                receipt.sequence.0,
                observed_ms,
                GAMMA_MARKETS_SOURCE_ID,
                GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
                GAMMA_PRICE_ATTEMPT_PARSER_VERSION,
                &payload,
            );
            let source = BTreeMap::from([(receipt.sequence.0, durable)]);
            let replay = replayed_risk_prices(
                &[receipt],
                evaluated_ms,
                std::slice::from_ref(&position),
                &source,
            )
            .await;

            match expected_error {
                None => {
                    assert!(runtime.is_ok(), "{label}: {runtime:?}");
                    assert!(replay.is_ok(), "{label}: {replay:?}");
                    let runtime = runtime.unwrap();
                    let replay = replay.unwrap();
                    assert_eq!(
                        runtime.get(&(market.to_string(), 0)).map(|row| row.price),
                        replay.get(&(market.clone(), OutcomeId(0))).copied(),
                        "{label}"
                    );
                }
                Some(expected) => {
                    assert_eq!(runtime.unwrap_err(), expected, "{label}");
                    assert!(
                        matches!(replay, Err(RiskPriceReplayError::Unavailable(actual)) if actual == expected),
                        "{label}: {replay:?}"
                    );
                }
            }
        }
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
        let raw = br#"[{"conditionId":"condition-a","outcomePrices":"[\"0.4\",\"0.6\"]"}]"#;
        let request_url = format!(
            "https://offline.invalid/markets?condition_ids=condition-a&limit={GAMMA_BATCH_LIMIT_PARAM}"
        );
        let observation = SourceObservation {
            receipt,
            observed_at: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            ),
            received_at: ReceivedAt(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
            received_unix_ms: 1_700_000_000_000,
            source_id: GAMMA_MARKETS_SOURCE_ID.to_owned(),
            schema_version: GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION,
            parser_version: GAMMA_PRICE_ATTEMPT_PARSER_VERSION,
            content_type: ContentType::Json,
            payload: gamma_price_page_record(&request_url, 1_700_000_000_000, raw, true),
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
            prices.get(&(market.clone(), OutcomeId(1))),
            Some(&Price::new(dec!(0.6)).unwrap())
        );

        let repeated_receipt = AppendReceipt {
            sequence: EventSeq(4),
            this_hash: blake3::hash(b"gamma-risk-price-repeat"),
        };
        let mut repeated_source = source.clone();
        repeated_source.insert(
            repeated_receipt.sequence.0,
            SourceObservation {
                receipt: repeated_receipt,
                ..observation.clone()
            },
        );
        let repeated_prices = replayed_risk_prices(
            &[receipt, repeated_receipt],
            observation.received_unix_ms + 59_999,
            std::slice::from_ref(&position),
            &repeated_source,
        )
        .await
        .unwrap();
        assert_eq!(
            repeated_prices.get(&(market.clone(), OutcomeId(1))),
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
    fn financial_prepare_rejects_unmatched_approved_admission() {
        let temp = tempfile::tempdir().unwrap();
        let source_log = temp.path().join("source.log");
        let live_journal = temp.path().join("live_journal.log");
        let status_path = temp.path().join("status.json");
        drop(Writer::open(&source_log).unwrap());
        let journal = LiveJournal::open(&live_journal).unwrap();
        let account_id = AccountId::new("live-a").unwrap();
        let binding = pe_execution_core::CredentialBindingIdentity {
            version: 1,
            key_id: "key".to_owned(),
        };
        let observed_at = OffsetDateTime::from_unix_timestamp(1).unwrap();
        let account_state = authenticated_boundary_state(&account_id, &binding, observed_at);
        journal
            .append(
                account_id.clone(),
                observed_at,
                boundary_baseline(&account_id, &binding, &account_state, 1),
            )
            .unwrap();
        let economic = test_economic("prepare-approved", test_receipt(41), test_receipt(42));
        let identity = LiveOrderIdentity {
            dispatch_id: "prepare-approved".to_owned(),
            idempotency_key: LiveOrderIdentity::idempotency_key_for(
                "prepare-approved",
                &AccountId::new("live-a").unwrap(),
            ),
            quote_id: "prepare-quote".to_owned(),
            config_hash: economic.applied_configuration_hash.clone(),
            decision_hash: "prepare-decision".to_owned(),
            evidence_hashes: vec!["prepare-evidence".to_owned()],
            fill_projection: None,
            schema_version: 1,
            parser_version: 1,
        };
        journal
            .append(
                account_id,
                observed_at,
                LiveJournalPayload::AdmissionEvaluated(Box::new(LiveAdmissionEvaluationAudit {
                    identity,
                    frozen_binding: binding.clone(),
                    current_binding: binding,
                    requested_mode: LiveControlMode::LiveTiny,
                    effective_mode: LiveControlMode::LiveTiny,
                    economic,
                    account_state: Some(account_state),
                    account_read_failure_evidence: Vec::new(),
                    account_read_failure_request_descriptor_hashes: Vec::new(),
                    account_read_failure_evidence_hashes: Vec::new(),
                    verdict: LiveAdmissionVerdict::Approved,
                })),
            )
            .unwrap();
        drop(journal);
        fs::write(
            &status_path,
            br#"{"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"stale":false,"accounts":[{"account_id":"live-a","requested_live_mode":"off","effective_live_mode":"off","armed":false}]}}"#,
        )
        .unwrap();

        let error =
            verify_live_preparation_posture(&live_journal, &source_log, &status_path).unwrap_err();
        assert!(matches!(
            error,
            QualificationError::InsufficientEvidence(reason)
                if reason.contains("unmatched Approved admission")
        ));
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
        let membership_manifest = MembershipProofBinding::decode_and_verify(
            &preparation.start.membership_proofs_hash,
            &[],
        )
        .unwrap();
        assert!(membership_manifest.membership.is_empty());
        assert!(membership_manifest.proofs.is_empty());
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
        fs::remove_file(&config.status_path).unwrap();
        let rollback_check = rollback_check_financial_era(&manifest, &config).unwrap();
        let rollback_check: serde_json::Value = serde_json::from_str(&rollback_check).unwrap();
        assert_eq!(
            rollback_check.get("complete_start"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            rollback_check.get("receipt"),
            Some(&serde_json::to_value(preparation.expected_receipt).unwrap())
        );
        {
            use std::io::Write as _;

            fs::OpenOptions::new()
                .append(true)
                .open(&paper_log)
                .unwrap()
                .write_all(&[0])
                .unwrap();
        }
        let incomplete_len = fs::metadata(&paper_log).unwrap().len();
        let rollback_check = rollback_check_financial_era(&manifest, &config).unwrap();
        let rollback_check: serde_json::Value = serde_json::from_str(&rollback_check).unwrap();
        assert_eq!(
            rollback_check.get("complete_start"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(fs::metadata(&paper_log).unwrap().len(), incomplete_len);
    }
}
