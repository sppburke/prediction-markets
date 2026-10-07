//! Deterministic wallet epoch-second activity commits (#544).
//!
//! A complete reconciled second is classified from one immutable pre-state and
//! reaches paper-state in one transaction. Lexical `g2:` order is used only to
//! make storage/replay output canonical; it never selects a causal winner.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, PositionState, SignalConfig, TradeProvenance};
use pe_core_types::{
    LeaderAction, MarketId, MarketOutcomeId, OutcomeId, PolymarketTokenId, Price, Probability,
    ProbabilityPpm, ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId,
    SourceTimestamp, SourceTradeId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType};
use pe_execution_core::ObservationEvidence;
use pe_paper_state::{
    ActivityBucketCommit, ActivityDispositionRecord, ActivityGroupState, AnchorInstallRecord,
    DecisionPendingRecord, DecisionPendingRow, EntryGateResultRecord, LeaderPositionRow,
    MarketHistoryRecord, NoCopyDisposition, PaperStateDb, ReanchorRecord, WalletFenceRecord,
    WalletHistoryStatusRecord,
};
use pe_position_ledger::{
    AppliedEffect, LedgerEffect, LedgerEffectDocumentError, LedgerError, LedgerMutation,
    PositionLedger, SecondVerdict, TradeDecision, WalletFenceCause, classify_complete_second,
};
use pe_source_polymarket_public::{
    ACTIVITY_MAX_OFFSET, ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate,
    ActivityAggregationError, ActivityParseContext, ActivityTransport, ActivityType,
    NormalizedActivity, PolymarketEndpoint, RECONCILIATION_PAGE_LIMIT, ReconciliationPageEvidence,
    aggregate_activity_rows, canonical_page_hash, parse_activity_response,
    parse_activity_trade_observation,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use crate::position_seeder::{AnchorInstall, ledger_capture};
use crate::risk_inputs::SourceReceiptIndex;
use crate::runtime_config::{RuntimeConfig, decode_pre_545_runtime_config};

/// Admitted history whose copying is suppressed by a causal bracket.
pub const HISTORY_ONLY_BRACKET: &str = "history_only_bracket";

/// Source id of the per-read commitment record appended after one complete fixed-end activity
/// read and before any of its buckets commit (#565). Never parsed as an activity page.
pub const ACTIVITY_READ_COMMITMENT_SOURCE_ID: &str = "pe-service.activity-read-commitment";
pub const ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION: u32 = 2;
pub const ACTIVITY_READ_COMMITMENT_V1_SCHEMA_VERSION: u32 = 1;
pub const ACTIVITY_READ_COMMITMENT_PARSER_VERSION: u32 = 1;
const ACTIVITY_READ_COMMITMENT_V1_DOMAIN: &[u8] = b"prediction-edge/activity-read-commitment/v1";
const ACTIVITY_READ_COMMITMENT_DOMAIN: &[u8] = b"prediction-edge/activity-read-commitment/v2";

/// The generation and synchronized receipt of a producer's complete-read commitment.
/// The raw envelope and payload are authenticated again by every source verifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityReadCommitmentReceipt {
    LegacyV1(AppendReceipt),
    BindingsV2(AppendReceipt),
}

impl ActivityReadCommitmentReceipt {
    fn receipt(self) -> AppendReceipt {
        match self {
            Self::LegacyV1(receipt) | Self::BindingsV2(receipt) => receipt,
        }
    }
}

/// Boot-owned freshness inputs frozen for continuation generation five.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperFreshnessPolicy {
    pub activity_ws_enabled: bool,
    pub copy_latency_budget_secs: u64,
}

impl PaperFreshnessPolicy {
    pub(crate) fn valid(self) -> bool {
        crate::config::valid_copy_latency_budget_secs(self.copy_latency_budget_secs)
    }

    /// Strict source-age comparison, retaining the full precision of both instants.
    #[must_use]
    pub(crate) fn expired(self, source_time: SourceTimestamp, now: time::OffsetDateTime) -> bool {
        self.activity_ws_enabled
            && (now - source_time.0).whole_nanoseconds()
                > i128::from(self.copy_latency_budget_secs) * 1_000_000_000
    }
}

/// Decision inputs already read before the atomic bucket commit.
#[derive(Debug, Clone)]
pub struct BucketDecisionContext {
    /// Shared authentication of the whole read. Its private fields prevent unverified construction.
    pub verified_read: Option<std::sync::Arc<VerifiedCommitment>>,
    pub applied_configuration: RuntimeConfig,
    pub decision_inputs_json: String,
    /// Receipt-bearing page occurrences are frozen in continuation versions 3, 4, and 5. They stay
    /// out of `decision_inputs`, which owns the logical read proof rather than source-log
    /// identities.
    pub page_occurrences: Vec<PageOccurrence>,
    /// Lowest websocket receipt per admitted group; frozen in continuation versions 3, 4, and 5.
    pub observed_source_receipts: HashMap<SourceTradeId, AppendReceipt>,
    /// Receipt of the complete-read commitment record appended for this read (#565). `None` only
    /// for non-copying bracket contexts, which never freeze a continuation.
    pub read_commitment: Option<ActivityReadCommitmentReceipt>,
    pub reconstruction_quality: ReconstructionQuality,
    pub signal_config: SignalConfig,
    pub copy_eligible: bool,
    /// Bracket catch-up installs an anchor immediately after this read.
    pub bracket_commit: bool,
    pub recorded_at_unix: i64,
    /// Transport retained from the first durable observation of each group.
    pub observation_provenance: HashMap<SourceTradeId, TradeProvenance>,
    /// Typed early-gate dispositions supplied by reconciliation (#544).
    pub no_copy_dispositions: HashMap<SourceTradeId, NoCopyDisposition>,
    /// Venue-metadata corrections keyed by the immutable raw activity group.
    pub identity_overrides: HashMap<SourceTradeId, IdentityOverride>,
    /// Groups whose token identity could not be established by venue metadata.
    pub identity_unresolved: HashSet<SourceTradeId>,
    /// Unseen outcome restamps whose unattributed member rows reproduce a recorded group.
    pub restamp_twins: HashSet<SourceTradeId>,
    /// Lane E supplies this only after a complete fixed-end history walk.
    pub history_status: Option<WalletHistoryStatusRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityOverride {
    pub verified: MarketOutcomeId,
    pub evidence_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketCommitResult {
    pub retained_revision: bool,
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub dispositions: BTreeMap<String, String>,
    pub pending: Vec<SourceTradeId>,
    pub newly_fenced: Option<WalletFenceCause>,
    pub already_committed: bool,
}

/// Mutable decision inputs the orchestrator freezes atomically with the bucket
/// transaction (#544 review round 3): the leader's win-rate probability and the
/// pre-sizing bankroll. A resumed continuation evaluates under these, never a
/// refreshed live watchlist or bankroll, so the same durable checkpoint always
/// reproduces the same decision.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FrozenDecisionBasis {
    pub win_rate_p: Probability,
    pub bankroll: Decimal,
}

/// Receipt-free continuation facts frozen by the bucket transaction.
/// Inputs read after this boundary are appended to paper/source logs and the
/// terminal `decision_pending` transition; offline replay never executes it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContinuationFacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paper_freshness_policy: Option<PaperFreshnessPolicy>,
    pub source_trade_id: SourceTradeId,
    pub semantic_revision: String,
    pub transaction_hash: String,
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    pub price: Price,
    pub share_amount: ShareAmount,
    pub provenance: TradeProvenance,
    pub pre_bucket_action: LeaderAction,
    pub reconstruction_quality: ReconstructionQuality,
    pub action_confidence_ppm: ProbabilityPpm,
    pub gate_result: String,
    pub applied_configuration_hash: String,
    pub applied_configuration: RuntimeConfig,
    pub frozen_basis: FrozenDecisionBasis,
    pub decision_inputs: Value,
}

/// Private compatibility decoder for durable version-two continuation rows.
#[derive(Debug, Deserialize)]
struct DecisionContinuationV2Wire {
    version: u16,
    #[serde(flatten)]
    facts: DecisionContinuationFacts,
}

/// One occurrence of a fixed-end activity page and the receipt assigned by the source log.
/// Repeated request URLs and payload hashes remain distinct entries (#545).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageOccurrence {
    pub request_url: String,
    pub raw_hash: String,
    pub receipt: AppendReceipt,
}

/// The recorded source contract authorizing a version-seven decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthority {
    CompleteRead,
    ActivityFrame,
}

/// Durable receipt-bearing successor and runtime owner of a frozen continuation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContinuationV3 {
    version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_authority: Option<SourceAuthority>,
    #[serde(flatten)]
    pub facts: DecisionContinuationFacts,
    pub observed_source_receipt: Option<AppendReceipt>,
    pub page_occurrences: Vec<PageOccurrence>,
    /// Receipt of the complete-read commitment; required for complete reads from version 4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_commitment: Option<AppendReceipt>,
}

/// Rewrite one synthetic continuation to the exact pre-#545 configuration shape (#584).
#[cfg(test)]
#[track_caller]
#[allow(clippy::expect_used)]
pub(crate) fn pre_545_applied_configuration(value: &mut Value) {
    let configuration = value
        .get_mut("applied_configuration")
        .and_then(Value::as_object_mut)
        .expect("continuation must contain an applied_configuration object");
    let era = configuration
        .remove("era")
        .expect("current RuntimeConfig must contain era");
    assert_eq!(era, Value::String("legacy17".to_owned()));
    let mut compatibility = configuration
        .remove("legacy_compatibility")
        .and_then(|value| value.as_object().cloned())
        .expect("Legacy17 RuntimeConfig must contain compatibility data");
    for key in ["fill_mode", "polymarket_fee_rate"] {
        configuration.insert(
            key.to_owned(),
            compatibility
                .remove(key)
                .expect("Legacy17 compatibility pair must be complete"),
        );
    }
    assert!(compatibility.is_empty());
}

/// Serialize synthetic historical facts as an exact pre-#545 version-two continuation (#584).
#[cfg(test)]
#[track_caller]
#[allow(clippy::expect_used)]
pub(crate) fn pre_545_frozen_inputs(facts: &DecisionContinuationFacts) -> String {
    let mut value = serde_json::to_value(facts).expect("facts must serialize");
    value
        .as_object_mut()
        .expect("serialized facts must be an object")
        .insert("version".to_owned(), Value::from(2));
    pre_545_applied_configuration(&mut value);
    serde_json::to_string(&value).expect("pre-#545 continuation must serialize")
}

/// Shared synthetic owner of the Legacy17 compatibility values used by #584 tests.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) fn synthetic_legacy17_runtime_config() -> RuntimeConfig {
    let mut value = serde_json::to_value(RuntimeConfig::from_service_config(
        &crate::config::ServiceConfig::default(),
    ))
    .expect("RuntimeConfig must serialize");
    value["era"] = Value::String("legacy17".to_owned());
    value["legacy_compatibility"] = json!({
        "fill_mode": "clob_best_ask",
        "polymarket_fee_rate": "0.04"
    });
    serde_json::from_value(value).expect("synthetic Legacy17 RuntimeConfig must deserialize")
}

/// One source-log activity page resolved by its frozen V3 receipt.
#[derive(Clone)]
pub(crate) struct CompleteActivityPage {
    pub(crate) payload: Vec<u8>,
    pub(crate) observed_at: SourceTimestamp,
    pub(crate) received_at: ReceivedAt,
    pub(crate) source_id: String,
    pub(crate) schema_version: u32,
    pub(crate) parser_version: u32,
    pub(crate) content_type: ContentType,
}

impl From<pe_event_log::EventEnvelope> for CompleteActivityPage {
    fn from(envelope: pe_event_log::EventEnvelope) -> Self {
        Self {
            payload: envelope.payload,
            observed_at: envelope.observed_at,
            received_at: envelope.received_at,
            source_id: envelope.source_id.0,
            schema_version: envelope.schema_version,
            parser_version: envelope.parser_version,
            content_type: envelope.content_type,
        }
    }
}

/// Fail-closed error from the shared complete-read reconstruction owner.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CompleteActivityReadError(String);

/// The aggregates an attributed TRADE or REDEEM group's member rows produce under each
/// unattributed outcome form the parser emits (`Some(999)`, `None`); a restamp reproduces the
/// group first published in one of them (#730 item 5). Any other group has none.
pub(crate) fn unattributed_forms(
    members: &[&NormalizedActivity],
) -> Result<Vec<ActivityAggregate>, ActivityAggregationError> {
    let Some(first) = members.first() else {
        return Ok(Vec::new());
    };
    if !matches!(
        first.activity_type,
        ActivityType::Trade | ActivityType::Redeem
    ) || first
        .outcome
        .is_none_or(|outcome| outcome == OutcomeId(999))
    {
        return Ok(Vec::new());
    }
    let mut forms = Vec::new();
    for outcome in [Some(OutcomeId(999)), None] {
        let rows = members
            .iter()
            .map(|member| {
                let mut row = (*member).clone();
                row.outcome = outcome;
                row
            })
            .collect::<Vec<_>>();
        forms.extend(aggregate_activity_rows(&rows)?);
    }
    Ok(forms)
}

/// Restamp pairs inside one complete read, keyed by the attributed restamp: an unattributed group
/// reproduced, by id and semantic revision, by exactly one attributed group's member rows under
/// its unattributed outcome. Both are one trade, so feed correlation and its replay count them
/// once; an original reproduced by several groups pairs with none of them.
pub(crate) fn read_restamp_pairs(
    rows: &[NormalizedActivity],
) -> Result<HashMap<SourceTradeId, SourceTradeId>, ActivityAggregationError> {
    let mut members = HashMap::<SourceTradeId, Vec<&NormalizedActivity>>::new();
    for row in rows {
        members
            .entry(row.group_id()?.key().clone())
            .or_default()
            .push(row);
    }
    let revisions = aggregate_activity_rows(rows)?
        .into_iter()
        .map(|aggregate| {
            (
                aggregate.group_id.key().clone(),
                aggregate.semantic_revision,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut restamps = HashMap::<SourceTradeId, Vec<SourceTradeId>>::new();
    for (group, members) in &members {
        for original in unattributed_forms(members)? {
            if revisions.get(original.group_id.key()) == Some(&original.semantic_revision) {
                restamps
                    .entry(original.group_id.key().clone())
                    .or_default()
                    .push(group.clone());
            }
        }
    }
    Ok(restamps
        .into_iter()
        .filter_map(|(original, groups)| match groups.as_slice() {
            [restamp] => Some((restamp.clone(), original)),
            _ => None,
        })
        .collect())
}

/// Count a restamp pair as one correlation candidate: the unattributed original stays and its
/// attributed restamp leaves. Genuinely distinct candidates remain ambiguous.
pub(crate) fn collapse_restamp_pairs(
    candidates: &mut Vec<&ActivityAggregate>,
    pairs: &HashMap<SourceTradeId, SourceTradeId>,
) {
    let keys = candidates
        .iter()
        .map(|candidate| candidate.group_id.key().clone())
        .collect::<HashSet<_>>();
    candidates.retain(|candidate| {
        pairs
            .get(candidate.group_id.key())
            .is_none_or(|original| !keys.contains(original))
    });
}

pub(crate) fn complete_activity_read_error(
    message: impl Into<String>,
) -> CompleteActivityReadError {
    CompleteActivityReadError(message.into())
}

#[derive(Deserialize)]
struct CompleteActivityReadWire {
    fixed_end: Option<i64>,
    pages: Option<Vec<ReconciliationPageEvidence>>,
}

struct CompleteActivitySegment {
    rows: Vec<pe_source_polymarket_public::NormalizedActivity>,
    children: Vec<(Option<i64>, i64)>,
}

// Retained only for one observation operation or shared read.
struct VerifiedActivityRead {
    aggregates: Vec<ActivityAggregate>,
    commitment: Option<ActivityReadCommitment>,
    bindings: VerifiedObservationBindings,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct VerifiedObservationBindings {
    observations: BTreeMap<
        pe_core_types::EventSeq,
        (
            AppendReceipt,
            pe_source_polymarket_public::ActivityTradeObservation,
        ),
    >,
    identities: HashMap<SourceTradeId, MarketOutcomeId>,
    restamp_pairs: HashMap<SourceTradeId, SourceTradeId>,
}

impl VerifiedObservationBindings {
    // Frozen facts are the effective identity; the durable effect comparator separately
    // binds them to the recorded raw-to-effective correction. Check every shared-read row.
    pub(crate) fn verify_facts(
        &self,
        facts: &DecisionContinuationFacts,
    ) -> Result<(), CompleteActivityReadError> {
        if self
            .identities
            .get(&facts.source_trade_id)
            .is_some_and(|identity| {
                identity.market() != &facts.market_id || identity.outcome() != facts.outcome_id
            })
        {
            return Err(complete_activity_read_error(
                "binding metadata differs from the effective target identity",
            ));
        }
        Ok(())
    }
}

fn complete_read_version(commitment: Option<ActivityReadCommitmentReceipt>) -> u16 {
    match commitment {
        None => 3,
        Some(ActivityReadCommitmentReceipt::LegacyV1(_)) => 4,
        Some(ActivityReadCommitmentReceipt::BindingsV2(_)) => 5,
    }
}

fn current_paper_version(version: u16) -> u16 {
    if matches!(version, 5..=7) { 7 } else { version }
}

/// Complete reads emit wire 7 exactly for payload-2 commitments; older read contracts emit
/// their historical wire. Use that emitted wire for bucket classification and read it back from
/// the recorded decision in qualification. Dispositions retain every other piece, so both owners
/// reconstruct the same whole second without consulting today's configuration.
pub(crate) fn complete_read_entry_policy(
    version: u16,
) -> pe_position_ledger::SameSecondEntryPolicy {
    if version == 7 {
        pe_position_ledger::SameSecondEntryPolicy::HomogeneousPieces
    } else {
        pe_position_ledger::SameSecondEntryPolicy::Legacy
    }
}

impl DecisionContinuationV3 {
    /// Wire version 5 for commitment v2, version 4 for v1, and version 3 without a commitment.
    pub(crate) fn new(
        facts: DecisionContinuationFacts,
        observed_source_receipt: Option<AppendReceipt>,
        page_occurrences: Vec<PageOccurrence>,
        read_commitment: Option<ActivityReadCommitmentReceipt>,
    ) -> Self {
        Self {
            version: complete_read_version(read_commitment),
            source_authority: None,
            facts,
            observed_source_receipt,
            page_occurrences,
            read_commitment: read_commitment.map(ActivityReadCommitmentReceipt::receipt),
        }
    }

    /// New REST paper writes retain the v5 source contract under continuation wire 7.
    pub(crate) fn current_paper(mut self) -> Self {
        self.version = current_paper_version(self.version);
        if self.version == 7 {
            self.source_authority = Some(SourceAuthority::CompleteRead);
        }
        self
    }

    /// Durable wire version (2 through 7).
    #[must_use]
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Financial era of the frozen continuation, independently of economic wire version.
    #[must_use]
    pub fn financial_semantic(&self) -> u32 {
        match self.version {
            2..=5 => 1,
            6 => 2,
            7 => 3,
            _ => 0,
        }
    }

    /// REST-only operations require complete-read authority.
    pub(crate) fn require_complete_read(&self) -> Result<(), DecisionContinuationError> {
        match (self.version, self.source_authority) {
            (2..=6, None) | (7, Some(SourceAuthority::CompleteRead)) => Ok(()),
            _ => Err(DecisionContinuationError::DurableMismatch),
        }
    }

    #[must_use]
    pub fn is_activity_frame(&self) -> bool {
        self.version == 7 && self.source_authority == Some(SourceAuthority::ActivityFrame)
    }

    /// Receipt-independent structural validation; source verification authenticates the prefix.
    pub(crate) fn validate_authority(&self) -> Result<(), DecisionContinuationError> {
        if !self.is_activity_frame() {
            return self.require_complete_read();
        }
        let proof: crate::frame_admission::FrameDecisionProof =
            serde_json::from_value(self.facts.decision_inputs.clone())
                .map_err(|_| DecisionContinuationError::DurableMismatch)?;
        let inputs = &proof.inputs;
        for (refused, fact) in [
            (!inputs.copy_eligible, "wallet not copy eligible"),
            (!inputs.history_complete, "wallet history incomplete"),
            (inputs.fenced, "wallet fenced"),
            (
                inputs.coverage.reanchor_required,
                "wallet requires reanchor",
            ),
            (inputs.latch.engaged(), "feed latch engaged"),
            (inputs.market_consumed, "market history consumed"),
            (
                !inputs.frontier.current(
                    inputs.received_at,
                    inputs.admitted_at,
                    inputs.poll_round_stale_secs,
                    inputs
                        .earlier_frames
                        .iter()
                        .filter(|frame| frame.wallet == self.facts.wallet)
                        .map(|frame| frame.received_at)
                        .min(),
                ),
                "history frontier not current",
            ),
            (
                crate::frame_admission::frame_prefix_blocks(
                    &inputs.earlier_frames,
                    self.facts.wallet,
                    &self.facts.market_id,
                ),
                "earlier unresolved buy",
            ),
        ] {
            if refused {
                return Err(DecisionContinuationError::FrameAdmissionRefused(fact));
            }
        }
        if inputs.version == 2
            && self.facts.paper_freshness_policy.is_none_or(|policy| {
                policy.expired(SourceTimestamp(inputs.source_time), inputs.admitted_at)
            })
        {
            return Err(DecisionContinuationError::FrameAdmissionRefused(
                "frame copy budget expired",
            ));
        }
        if inputs.admitted_at < inputs.received_at
            || inputs.frontier.commitment.sequence >= proof.admission_receipt.sequence
            || self.facts.action_confidence_ppm
                != ProbabilityPpm(u32::from(self.facts.reconstruction_quality.get()) * 10_000)
            || !matches!(inputs.version, 1 | 2)
            || (inputs.version == 2) != inputs.identity.is_some()
            || self.observed_source_receipt != Some(inputs.frame_receipt)
            || !self.page_occurrences.is_empty()
            || self.read_commitment.is_some()
            || self.facts.provenance != TradeProvenance::ActivityWs
            || self.facts.side != Side::Buy
            || self.facts.share_amount == ShareAmount::ZERO
            || inputs.frontier.wallet != self.facts.wallet
            || inputs.source_time.unix_timestamp() != self.facts.source_epoch
            || inputs.ledger_capture.wallet != self.facts.wallet
            || proof.admission_receipt.sequence <= inputs.frame_receipt.sequence
            || !crate::frame_admission::unique_earlier(&inputs.earlier_frames, inputs.frame_receipt)
            || crate::frame_admission::frame_revision(inputs)? != self.facts.semantic_revision
        {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        let positions = inputs
            .positions(self.facts.wallet, &self.facts.market_id)
            .map_err(|_| {
                DecisionContinuationError::FrameAdmissionRefused("market ledger rebuild differs")
            })?;
        if inputs.ledger_capture.anchor_seq != inputs.coverage.anchor_seq
            || inputs.ledger_capture.coverage_generation != inputs.coverage.coverage_generation
            || inputs
                .earlier_frames
                .iter()
                .any(|frame| frame.wallet != self.facts.wallet)
        {
            return Err(DecisionContinuationError::DurableMismatch);
        }

        let trade = self.incoming_trade_unchecked()?;
        if pe_copy_signal_engine::classify_leader_action(
            &trade,
            Some(&positions),
            self.facts.reconstruction_quality,
            &SignalConfig::default(),
        ) != LeaderAction::Entry
        {
            return Err(DecisionContinuationError::FrameAdmissionRefused(
                "confirmed market position is not an entry",
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_activity_frame_with_index(
        &self,
        index: &SourceReceiptIndex,
    ) -> Result<(SourceTimestamp, Option<PolymarketTokenId>), CompleteActivityReadError> {
        #[cfg(feature = "scenario")]
        if let Some(receipt) = self.observed_source_receipt {
            index.record_frame_verification(receipt);
        }
        let proof: crate::frame_admission::FrameDecisionProof =
            serde_json::from_value(self.facts.decision_inputs.clone())
                .map_err(|error| complete_activity_read_error(error.to_string()))?;
        index
            .verify_frame_frontier(&proof.inputs.frontier)
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        self.verify_activity_frame_inner(
            &mut |receipt| {
                index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
            },
            Some(&proof.inputs.frontier),
        )
    }

    /// Authenticate the exact frame and prefix before using any frozen decision fact.
    pub(crate) fn verify_activity_frame<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<(SourceTimestamp, Option<PolymarketTokenId>), CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.verify_activity_frame_inner(lookup, None)
    }

    fn verify_activity_frame_inner<L, E>(
        &self,
        lookup: &mut L,
        verified_frontier: Option<&crate::frame_admission::FeedHistoryFrontier>,
    ) -> Result<(SourceTimestamp, Option<PolymarketTokenId>), CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.validate_authority()
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let proof: crate::frame_admission::FrameDecisionProof =
            serde_json::from_value(self.facts.decision_inputs.clone())
                .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let inputs = &proof.inputs;
        let source = lookup(inputs.frame_receipt)
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let observation = verified_stream_observation(&source, self.facts.wallet)?;
        let components = observation.group_id.components();
        if observation.group_id.key() != &self.facts.source_trade_id
            || observation.is_combo
            || observation.share_amount != self.facts.share_amount
            || observation.price != self.facts.price
            || observation.source_time.0 != inputs.source_time
            || source.observed_at.0 != inputs.source_time
            || source.received_at.0 != inputs.received_at
            || components.transaction_hash != self.facts.transaction_hash
            || components.condition_id.as_ref().map(|id| id.0.as_str())
                != Some(self.facts.market_id.0.0.as_str())
            || components.outcome != Some(self.facts.outcome_id)
            || components.side != Some(self.facts.side)
        {
            return Err(complete_activity_read_error(
                "frame facts differ from authenticated envelope",
            ));
        }
        if let Some(identity) = &inputs.identity {
            if Some(&identity.provenance.asset) != components.asset.as_ref()
                || identity.provenance.source_log_sequence != identity.receipt.sequence.0
                || identity.receipt.sequence >= proof.admission_receipt.sequence
            {
                return Err(complete_activity_read_error(
                    "frame identity provenance differs",
                ));
            }
            let source = lookup(identity.receipt)
                .map_err(|error| complete_activity_read_error(error.to_string()))?;
            let verified = verify_binding_identity(&source, &identity.provenance)?;
            if verified.condition_id.0 != self.facts.market_id.0.0
                || verified.outcome != self.facts.outcome_id
            {
                return Err(complete_activity_read_error(
                    "frame asset differs from claimed market",
                ));
            }
        }
        let admission = lookup(proof.admission_receipt)
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        if admission.source_id != crate::frame_admission::FRAME_ADMISSION_SOURCE_ID
            || admission.schema_version != u32::from(inputs.version)
            || admission.parser_version != 1
            || admission.content_type != ContentType::Json
            || admission.observed_at.0 != inputs.admitted_at
            || admission.received_at.0 != inputs.admitted_at
            || serde_json::from_slice::<crate::frame_admission::FrameAdmissionArtifact>(
                &admission.payload,
            )
            .map_err(|error| complete_activity_read_error(error.to_string()))?
                != crate::frame_admission::FrameAdmissionArtifact::from_inputs(inputs)
                    .map_err(|error| complete_activity_read_error(error.to_string()))?
        {
            return Err(complete_activity_read_error(
                "frame admission prefix differs from authenticated capture",
            ));
        }
        if verified_frontier != Some(&inputs.frontier) {
            inputs
                .frontier
                .verify(lookup)
                .map_err(|error| complete_activity_read_error(error.to_string()))?;
        }
        // Every earlier frame is authenticated, rather than accepting caller-supplied identities.
        for earlier in &inputs.earlier_frames {
            let source = lookup(earlier.receipt)
                .map_err(|error| complete_activity_read_error(error.to_string()))?;
            let observation = verified_stream_observation(&source, earlier.wallet)?;
            if observation.group_id.key() != &earlier.source_trade_id
                || observation
                    .group_id
                    .components()
                    .condition_id
                    .as_ref()
                    .map(|id| id.0.as_str())
                    != Some(earlier.market.0.0.as_str())
                || source.received_at.0 != earlier.received_at
                || (earlier.unresolved_buy
                    && (observation.is_combo
                        || observation.share_amount == ShareAmount::ZERO
                        || observation.group_id.components().side != Some(Side::Buy)))
            {
                return Err(complete_activity_read_error(
                    "earlier frame differs from admission prefix",
                ));
            }
        }
        Ok((
            SourceTimestamp(inputs.source_time),
            components.asset.clone(),
        ))
    }

    pub(crate) fn authority_receipts(
        &self,
    ) -> Result<Vec<AppendReceipt>, DecisionContinuationError> {
        let mut receipts = self
            .page_occurrences
            .iter()
            .map(|page| page.receipt)
            .chain(self.observed_source_receipt)
            .chain(self.read_commitment)
            .collect::<Vec<_>>();
        if self.is_activity_frame() {
            let proof: crate::frame_admission::FrameDecisionProof =
                serde_json::from_value(self.facts.decision_inputs.clone())?;
            receipts.push(proof.admission_receipt);
            receipts.extend(
                proof
                    .inputs
                    .identity
                    .as_ref()
                    .map(|identity| identity.receipt),
            );
            receipts.push(proof.inputs.frontier.commitment);
            receipts.extend(
                proof
                    .inputs
                    .frontier
                    .page_occurrences
                    .iter()
                    .map(|page| page.receipt),
            );
            receipts.extend(
                proof
                    .inputs
                    .earlier_frames
                    .iter()
                    .map(|frame| frame.receipt),
            );
        }
        Ok(receipts)
    }

    /// Whether both continuations describe one complete read: equal wallet, logical read proof,
    /// ordered page occurrences, and read commitment (#565). Consumed by the open-continuation
    /// validator and qualification's read-scope agreement.
    #[must_use]
    pub(crate) fn same_complete_read(&self, other: &Self) -> bool {
        self.version == other.version
            && self.source_authority == other.source_authority
            && self.facts.wallet == other.facts.wallet
            && self.facts.decision_inputs == other.facts.decision_inputs
            && self.page_occurrences == other.page_occurrences
            && self.read_commitment == other.read_commitment
    }

    /// Greatest synchronized activity-page receipt in this complete read.
    #[must_use]
    pub fn complete_bound(&self) -> Option<AppendReceipt> {
        if self.is_activity_frame() {
            return self.observed_source_receipt;
        }
        self.require_complete_read().ok()?;
        self.page_occurrences
            .iter()
            .map(|page| page.receipt)
            .max_by_key(|receipt| receipt.sequence)
    }

    /// Earliest complete observation and its envelope receive time. Legacy V2 rows have no
    /// receipt evidence and return `None`.
    #[must_use]
    fn observation_at(&self, observed_unix_ms: i64) -> Option<ObservationEvidence> {
        let complete_bound_receipt = self.complete_bound()?;
        let (source_receipt, provenance) = match self.observed_source_receipt {
            Some(websocket) if websocket.sequence < complete_bound_receipt.sequence => {
                (websocket, "activity_ws")
            }
            Some(websocket) if websocket.sequence == complete_bound_receipt.sequence => {
                if websocket.this_hash != complete_bound_receipt.this_hash {
                    return None;
                }
                (websocket, "activity_ws")
            }
            _ => (complete_bound_receipt, "rest_poll"),
        };
        Some(ObservationEvidence {
            source_receipt,
            complete_bound_receipt,
            observed_unix_ms,
            provenance: provenance.to_owned(),
        })
    }

    #[must_use]
    pub fn observation_receipt(&self) -> Option<AppendReceipt> {
        let complete_bound = self.complete_bound()?;
        match self.observed_source_receipt {
            Some(websocket) if websocket.sequence < complete_bound.sequence => Some(websocket),
            Some(websocket) if websocket.sequence == complete_bound.sequence => {
                (websocket.this_hash == complete_bound.this_hash).then_some(websocket)
            }
            _ => Some(complete_bound),
        }
    }

    #[must_use]
    pub fn page_occurrences(&self) -> &[PageOccurrence] {
        &self.page_occurrences
    }

    fn read_verification(&self) -> ActivityReadVerification<'_> {
        ActivityReadVerification {
            binding_filter: None,
            counterpart_depth: 0,
            version: self.version,
            wallet: self.facts.wallet,
            decision_inputs: &self.facts.decision_inputs,
            page_occurrences: &self.page_occurrences,
            read_commitment: self.read_commitment,
        }
    }

    pub(crate) fn reconstruct_complete_activity_read<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<Vec<ActivityAggregate>, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.reconstruct_verified_activity_read(lookup)
            .map(|read| read.aggregates)
    }

    /// Retain authenticated bindings so qualification can check every sibling in one read.
    pub(crate) fn reconstruct_complete_activity_read_with_bindings<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<(Vec<ActivityAggregate>, VerifiedObservationBindings), CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.reconstruct_verified_activity_read(lookup)
            .map(|read| (read.aggregates, read.bindings))
    }

    fn reconstruct_verified_activity_read<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<VerifiedActivityRead, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.require_complete_read()
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let read = self
            .read_verification()
            .reconstruct_verified_activity_read(lookup)?;
        read.bindings.verify_facts(&self.facts)?;
        Ok(read)
    }

    fn reconstruct_verified_activity_read_parts<L, E>(
        &self,
        fixed_end: i64,
        pages: &[ReconciliationPageEvidence],
        commitment: Option<ActivityReadCommitment>,
        lookup: &mut L,
    ) -> Result<VerifiedActivityRead, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.require_complete_read()
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let read = self
            .read_verification()
            .reconstruct_verified_activity_read_parts(fixed_end, pages, commitment, lookup)?;
        read.bindings.verify_facts(&self.facts)?;
        Ok(read)
    }

    fn verify_stream_binding_in_read<L, E>(
        &self,
        target_id: &SourceTradeId,
        receipt: AppendReceipt,
        read: &VerifiedActivityRead,
        lookup: &mut L,
    ) -> Result<SourceTimestamp, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.require_complete_read()
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        read.bindings.verify_facts(&self.facts)?;
        self.read_verification()
            .verify_stream_binding_in_read(target_id, receipt, read, lookup)
    }

    pub(crate) fn commitment_contract(&self) -> Option<(u32, u32)> {
        self.require_complete_read().ok()?;
        self.read_verification().commitment_contract()
    }

    fn verify_read_commitment<L, E>(
        &self,
        fixed_end: i64,
        pages: &[ReconciliationPageEvidence],
        lookup: &mut L,
    ) -> Result<Option<ActivityReadCommitment>, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.require_complete_read()
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        self.read_verification()
            .verify_read_commitment(fixed_end, pages, lookup)
    }

    pub(crate) fn verify_stream_binding<L, E>(
        &self,
        target_id: &SourceTradeId,
        receipt: AppendReceipt,
        lookup: &mut L,
    ) -> Result<SourceTimestamp, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        self.require_complete_read()
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        if matches!(self.version, 5..=7) {
            let read = self.reconstruct_verified_activity_read(lookup)?;
            return self.verify_stream_binding_in_read(target_id, receipt, &read, lookup);
        }
        self.read_verification()
            .verify_stream_binding(target_id, receipt, lookup)
    }
}

pub(crate) fn verify_feed_frontier<L, E>(
    frontier: &crate::frame_admission::FeedHistoryFrontier,
    lookup: &mut L,
) -> Result<(), CompleteActivityReadError>
where
    L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
    E: Display,
{
    let inputs = json!({"fixed_end": frontier.fixed_end, "pages": frontier.pages});
    let verifier = ActivityReadVerification {
        binding_filter: None,
        counterpart_depth: 0,
        version: 7,
        wallet: frontier.wallet,
        decision_inputs: &inputs,
        page_occurrences: &frontier.page_occurrences,
        read_commitment: Some(frontier.commitment),
    };
    verifier.reconstruct_verified_activity_read(lookup)?;
    Ok(())
}

// The same read verifier serves a continuation and a commitment-only boot candidate. It owns
// no decision, defaults, financial state, or persistence; all inputs are borrowed recorded proof.
struct ActivityReadVerification<'a> {
    version: u16,
    counterpart_depth: u8,
    binding_filter: Option<&'a HashSet<(pe_core_types::EventSeq, blake3::Hash)>>,
    wallet: WalletAddress,
    decision_inputs: &'a Value,
    page_occurrences: &'a [PageOccurrence],
    read_commitment: Option<AppendReceipt>,
}

impl ActivityReadVerification<'_> {
    /// Reconstruct one frozen fixed-end activity read from its exact receipt occurrences.
    ///
    /// Page evidence is joined with multiplicity, every retained payload is parsed and checked,
    /// and saturated parent segments contribute no production aggregates. Only complete leaves of
    /// the validated split graph are aggregated, matching the production reconciliation reader.
    fn reconstruct_verified_activity_read<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<VerifiedActivityRead, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let wire: CompleteActivityReadWire = serde_json::from_value(self.decision_inputs.clone())
            .map_err(|error| {
            complete_activity_read_error(format!(
                "decision complete activity read is invalid: {error}"
            ))
        })?;
        if wire.fixed_end.is_some() != wire.pages.is_some() {
            return Err(complete_activity_read_error(
                "decision complete activity read proof is partial",
            ));
        }
        let (fixed_end, pages) = match (wire.fixed_end, wire.pages.as_deref()) {
            (Some(fixed_end), Some(pages)) => (fixed_end, pages),
            (None, None) => {
                return Err(complete_activity_read_error(
                    "decision continuation is missing its complete activity read proof",
                ));
            }
            _ => {
                return Err(complete_activity_read_error(
                    "decision complete activity read proof is partial",
                ));
            }
        };
        let commitment = self.verify_read_commitment(fixed_end, pages, lookup)?;
        self.reconstruct_verified_activity_read_parts(fixed_end, pages, commitment, lookup)
    }

    fn reconstruct_verified_activity_read_parts<L, E>(
        &self,
        fixed_end: i64,
        pages: &[ReconciliationPageEvidence],
        commitment: Option<ActivityReadCommitment>,
        lookup: &mut L,
    ) -> Result<VerifiedActivityRead, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let mut sources =
            HashMap::<(pe_core_types::EventSeq, blake3::Hash), CompleteActivityPage>::new();
        // Keep only this read's pages. A counterpart basis owns and releases its own
        // page cache instead of retaining every historical full read in the outer one.
        let current_pages = self
            .page_occurrences
            .iter()
            .map(|page| (page.receipt.sequence, page.receipt.this_hash))
            .collect::<HashSet<_>>();
        let mut cached_lookup = |receipt: AppendReceipt| {
            let key = (receipt.sequence, receipt.this_hash);
            if let Some(source) = sources.get(&key) {
                return Ok::<CompleteActivityPage, CompleteActivityReadError>(source.clone());
            }
            let source =
                lookup(receipt).map_err(|error| complete_activity_read_error(error.to_string()))?;
            if current_pages.contains(&key) {
                sources.insert(key, source.clone());
            }
            Ok(source)
        };
        let lookup = &mut cached_lookup;
        let rows = self.reconstruct_rich_activity_read(fixed_end, pages, lookup)?;
        let aggregates = aggregate_activity_rows(&rows).map_err(|error| {
            complete_activity_read_error(format!(
                "complete activity read aggregate failed: {error}"
            ))
        })?;
        let bindings = if let Some(commitment) = &commitment {
            self.verify_observation_bindings(commitment, &rows, &aggregates, lookup)?
        } else {
            VerifiedObservationBindings::default()
        };
        Ok(VerifiedActivityRead {
            aggregates,
            commitment,
            bindings,
        })
    }

    /// Select the complete commitment envelope contract from the continuation generation.
    pub(crate) fn commitment_contract(&self) -> Option<(u32, u32)> {
        match self.version {
            4 => Some((
                ACTIVITY_READ_COMMITMENT_V1_SCHEMA_VERSION,
                ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
            )),
            5..=7 => Some((
                ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
            )),
            _ => None,
        }
    }

    fn verify_read_commitment<L, E>(
        &self,
        fixed_end: i64,
        pages: &[ReconciliationPageEvidence],
        lookup: &mut L,
    ) -> Result<Option<ActivityReadCommitment>, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let Some((schema, parser)) = self.commitment_contract() else {
            return if self.version == 3 && self.read_commitment.is_none() {
                Ok(None)
            } else {
                Err(complete_activity_read_error(
                    "complete activity read has inconsistent wire generation",
                ))
            };
        };
        let receipt = self.read_commitment.ok_or_else(|| {
            complete_activity_read_error("complete activity read commitment is missing")
        })?;
        if self
            .page_occurrences
            .iter()
            .any(|page| page.receipt.sequence >= receipt.sequence)
        {
            return Err(complete_activity_read_error(
                "complete activity read commitment precedes its pages",
            ));
        }
        let source = lookup(receipt).map_err(|error| {
            complete_activity_read_error(format!("commitment receipt lookup failed: {error}"))
        })?;
        if source.source_id != ACTIVITY_READ_COMMITMENT_SOURCE_ID
            || source.schema_version != schema
            || source.parser_version != parser
            || source.content_type != ContentType::Json
        {
            return Err(complete_activity_read_error(
                "complete activity read commitment has the wrong source contract",
            ));
        }
        let value: Value = serde_json::from_slice(&source.payload).map_err(|error| {
            complete_activity_read_error(format!(
                "complete activity read commitment is invalid: {error}"
            ))
        })?;
        if (matches!(self.version, 5..=7)) != value.get("bindings").is_some() {
            return Err(complete_activity_read_error(
                "complete activity read commitment has inconsistent binding generation",
            ));
        }
        let commitment: ActivityReadCommitment =
            serde_json::from_slice(&source.payload).map_err(|error| {
                complete_activity_read_error(format!(
                    "complete activity read commitment is invalid: {error}"
                ))
            })?;
        if let Some(bindings) = &commitment.bindings
            && serde_json::to_value(bindings)
                .map_err(|error| complete_activity_read_error(error.to_string()))?
                != value["bindings"]
        {
            return Err(complete_activity_read_error(
                "observation bindings have noncanonical or unknown fields",
            ));
        }
        let expected_version = if matches!(self.version, 5..=7) { 2 } else { 1 };
        if commitment.version != expected_version
            || (expected_version == 2) != commitment.bindings.is_some()
            || commitment.wallet != self.wallet
            || commitment.fixed_end != fixed_end
        {
            return Err(complete_activity_read_error(
                "complete activity read commitment differs from its frozen proof",
            ));
        }
        if commitment
            .bindings
            .as_ref()
            .is_some_and(|bindings| !bindings.is_empty())
            && commitment.read_proof.is_none()
        {
            return Err(complete_activity_read_error(
                "binding commitment read proof is absent",
            ));
        }
        if let Some(proof) = &commitment.read_proof
            && (!matches!(self.version, 5..=7)
                || proof.page_occurrences != self.page_occurrences
                || proof.pages != pages)
        {
            return Err(complete_activity_read_error(
                "commitment read proof differs from its frozen proof",
            ));
        }
        if let Some(bindings) = &commitment.bindings
            && canonical_bindings(bindings)? != *bindings
        {
            return Err(complete_activity_read_error(
                "observation bindings are not canonical",
            ));
        }
        let digest = activity_read_digest_versioned(
            self.wallet,
            fixed_end,
            self.page_occurrences,
            pages,
            commitment.bindings.as_deref(),
        )?;
        if commitment.digest != digest.to_hex().as_str() {
            return Err(complete_activity_read_error(
                "complete activity read commitment differs from its frozen proof",
            ));
        }
        Ok(Some(commitment))
    }

    fn verify_observation_bindings<L, E>(
        &self,
        commitment: &ActivityReadCommitment,
        rows: &[NormalizedActivity],
        aggregates: &[ActivityAggregate],
        lookup: &mut L,
    ) -> Result<VerifiedObservationBindings, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let mut verified = VerifiedObservationBindings::default();
        let mut identities = HashMap::new();
        let mut counterpart_proofs = HashMap::new();
        let Some(bindings) = &commitment.bindings else {
            return Ok(verified);
        };
        let restamp_pairs = read_restamp_pairs(rows).map_err(|error| {
            complete_activity_read_error(format!(
                "complete activity read aggregate failed: {error}"
            ))
        })?;
        let commitment_receipt = self
            .read_commitment
            .ok_or_else(|| complete_activity_read_error("binding commitment receipt is absent"))?;
        let proof: CompleteActivityReadWire = serde_json::from_value(self.decision_inputs.clone())
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let pages = proof
            .pages
            .ok_or_else(|| complete_activity_read_error("binding read pages are absent"))?;
        let joined = joined_read_pages(self.page_occurrences, &pages)?;
        for binding in bindings.iter().filter(|binding| {
            self.binding_filter.is_none_or(|receipts| {
                receipts.contains(&(
                    binding.stream_receipt.sequence,
                    binding.stream_receipt.this_hash,
                ))
            })
        }) {
            if binding.stream_receipt.sequence >= commitment_receipt.sequence
                || binding
                    .identity_receipt
                    .is_some_and(|receipt| receipt.sequence >= commitment_receipt.sequence)
            {
                return Err(complete_activity_read_error(
                    "binding evidence is not before its commitment",
                ));
            }
            let stream = lookup(binding.stream_receipt).map_err(|error| {
                complete_activity_read_error(format!(
                    "binding stream receipt lookup failed: {error}"
                ))
            })?;
            let observation = verified_stream_observation(&stream, self.wallet)?;
            if observation.group_id.key() != &binding.stream_group_id {
                return Err(complete_activity_read_error(
                    "binding stream group differs from its receipt",
                ));
            }
            let target = aggregates
                .iter()
                .find(|aggregate| aggregate.group_id.key() == &binding.history_group_id)
                .ok_or_else(|| {
                    complete_activity_read_error("binding target is absent from the complete read")
                })?;
            if target.semantic_revision.as_str() != binding.semantic_revision {
                return Err(complete_activity_read_error(
                    "binding target revision differs",
                ));
            }
            let index = usize::try_from(binding.page_occurrence_index)
                .map_err(|_| complete_activity_read_error("binding occurrence index overflow"))?;
            let occurrence = self
                .page_occurrences
                .get(index)
                .filter(|page| page.raw_hash == binding.page_raw_hash)
                .ok_or_else(|| {
                    complete_activity_read_error("binding target page occurrence differs")
                })?;
            let evidence = joined[index].1;
            if pages.iter().any(|page| {
                page.bounds == evidence.bounds
                    && page.offset == ACTIVITY_MAX_OFFSET
                    && page.row_count == RECONCILIATION_PAGE_LIMIT
            }) {
                return Err(complete_activity_read_error(
                    "binding target page belongs to a saturated parent",
                ));
            }
            let page = complete_activity_page(occurrence, self.version, lookup)?;
            let window = parse_complete_activity_page(
                &page,
                self.wallet,
                "binding target page parse failed",
            )?;
            if !window.rows.iter().any(|row| {
                row.group_id()
                    .is_ok_and(|group| group.key() == &binding.history_group_id)
            }) {
                return Err(complete_activity_read_error(
                    "binding target does not occur on its page",
                ));
            }
            let frame_audit = if let Some(receipt) = binding.frame_admission_receipt {
                let admission = lookup(receipt)
                    .map_err(|error| complete_activity_read_error(error.to_string()))?;
                let artifact: crate::frame_admission::FrameAdmissionArtifact =
                    serde_json::from_slice(&admission.payload)
                        .map_err(|error| complete_activity_read_error(error.to_string()))?;
                if admission.source_id != crate::frame_admission::FRAME_ADMISSION_SOURCE_ID
                    || admission.schema_version != 1
                    || admission.parser_version != 1
                    || admission.content_type != ContentType::Json
                    || receipt.sequence >= commitment_receipt.sequence
                    || artifact.version != 1
                    || artifact.capture_digest.len() != 64
                    || artifact.frame_receipt != binding.stream_receipt
                    || observation.is_combo
                    || observation.share_amount == ShareAmount::ZERO
                    || observation.group_id.components().side != Some(Side::Buy)
                {
                    return Err(complete_activity_read_error(
                        "frame audit admission differs from its observation",
                    ));
                }
                true
            } else {
                false
            };
            let original = observation.group_id.components();
            let history = target.group_id.components();
            let exact = aggregates
                .iter()
                .find(|aggregate| aggregate.group_id.key() == observation.group_id.key());
            if frame_audit {
                let fixed = if let Some(basis) = binding.counterpart_basis_receipt {
                    if basis.sequence >= commitment_receipt.sequence {
                        return Err(complete_activity_read_error(
                            "frame counterpart basis is not before commitment",
                        ));
                    }
                    let key = (
                        basis.sequence,
                        basis.this_hash,
                        binding.stream_receipt.sequence,
                        binding.stream_receipt.this_hash,
                    );
                    let target = match counterpart_proofs.entry(key) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            // Authenticate one basis at a time; retain only its compact target.
                            let previous = verified_commitment_bindings_at_depth(
                                basis,
                                &mut |receipt| {
                                    lookup(receipt).map_err(|error| {
                                        complete_activity_read_error(error.to_string())
                                    })
                                },
                                self.counterpart_depth + 1,
                                Some(&HashSet::from([(
                                    binding.stream_receipt.sequence,
                                    binding.stream_receipt.this_hash,
                                )])),
                            )?;
                            if previous.wallet != self.wallet {
                                return Err(complete_activity_read_error(
                                    "frame counterpart basis wallet differs",
                                ));
                            }
                            let target = if let Some(prior) = previous.bindings.first() {
                                if prior.frame_admission_receipt != binding.frame_admission_receipt
                                {
                                    return Err(complete_activity_read_error(
                                        "frame counterpart basis admission differs",
                                    ));
                                }
                                Some(prior.history_group_id.clone())
                            } else if previous.full_history
                                && !previous
                                    .transaction_aggregates
                                    .get(&original.transaction_hash)
                                    .into_iter()
                                    .flatten()
                                    .filter_map(|position| previous.aggregates.get(*position))
                                    .any(|aggregate| {
                                        aggregate.group_id.components().activity_type
                                            == ActivityType::Trade
                                    })
                            {
                                None
                            } else {
                                return Err(complete_activity_read_error(
                                    "frame counterpart basis has no binding or absence",
                                ));
                            };
                            entry.insert(target)
                        }
                    };
                    Some(target.as_ref())
                } else {
                    None
                };
                let candidates = crate::feed_audit::resolve_frame_counterpart(
                    &observation,
                    fixed,
                    aggregates,
                    &restamp_pairs,
                );
                if candidates.len() != 1 || candidates[0].group_id != target.group_id {
                    return Err(complete_activity_read_error(
                        "frame audit counterpart is absent or ambiguous",
                    ));
                }
            } else if binding.counterpart_basis_receipt.is_some() {
                return Err(complete_activity_read_error(
                    "ordinary binding has frame counterpart basis",
                ));
            } else if let Some(exact) = exact {
                if exact.group_id != target.group_id {
                    return Err(complete_activity_read_error(
                        "binding substitutes an exact history match",
                    ));
                }
            } else {
                let mut candidates = aggregates
                    .iter()
                    .filter(|aggregate| {
                        let candidate = aggregate.group_id.components();
                        candidate.activity_type == original.activity_type
                            && candidate.wallet == original.wallet
                            && candidate.transaction_hash == original.transaction_hash
                            && candidate.asset == original.asset
                            && candidate.side == original.side
                    })
                    .collect::<Vec<_>>();
                collapse_restamp_pairs(&mut candidates, &restamp_pairs);
                if candidates.len() != 1
                    || candidates[0].group_id != target.group_id
                    || original.asset.is_none()
                {
                    return Err(complete_activity_read_error(
                        "binding history identity is absent or ambiguous",
                    ));
                }
            }
            match (&binding.identity_provenance, binding.identity_receipt) {
                (None, None) if original == history || frame_audit => {}
                (Some(provenance), Some(receipt)) => {
                    if receipt.sequence.0 != provenance.source_log_sequence
                        || history.asset.as_ref() != Some(&provenance.asset)
                    {
                        return Err(complete_activity_read_error(
                            "binding metadata provenance differs",
                        ));
                    }
                    let identity_key = (
                        receipt.sequence,
                        receipt.this_hash,
                        provenance.asset.clone(),
                        provenance.canonical_page_hash.clone(),
                    );
                    let identity = match identities.entry(identity_key) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let metadata = lookup(receipt).map_err(|error| {
                                complete_activity_read_error(format!(
                                    "binding metadata receipt lookup failed: {error}"
                                ))
                            })?;
                            entry.insert(verify_binding_identity(&metadata, provenance)?)
                        }
                    };
                    let target_identity = MarketOutcomeId::new(
                        MarketId(pe_core_types::VenueMarketId(
                            identity.condition_id.0.clone(),
                        )),
                        identity.outcome,
                    );
                    if verified
                        .identities
                        .insert(target.group_id.key().clone(), target_identity.clone())
                        .is_some_and(|previous| previous != target_identity)
                    {
                        return Err(complete_activity_read_error(
                            "binding metadata disagrees about the effective target identity",
                        ));
                    }
                }
                _ => {
                    return Err(complete_activity_read_error(
                        "binding corrected identity has no complete metadata proof",
                    ));
                }
            }
            verified.observations.insert(
                binding.stream_receipt.sequence,
                (binding.stream_receipt, observation),
            );
        }
        verified.restamp_pairs = restamp_pairs;
        Ok(verified)
    }

    /// Authenticate a stream receipt and its target, returning the earliest recorded source clock.
    /// V5 corrections require a verified binding; historical generations retain exact equality.
    pub(crate) fn verify_stream_binding<L, E>(
        &self,
        target_id: &SourceTradeId,
        receipt: AppendReceipt,
        lookup: &mut L,
    ) -> Result<SourceTimestamp, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        if matches!(self.version, 5..=7) {
            let read = self.reconstruct_verified_activity_read(lookup)?;
            return self.verify_stream_binding_in_read(target_id, receipt, &read, lookup);
        }
        let source = lookup(receipt).map_err(|error| {
            complete_activity_read_error(format!("stream receipt lookup failed: {error}"))
        })?;
        let observation = verified_stream_observation(&source, self.wallet)?;
        if observation.group_id.key() != target_id {
            return Err(complete_activity_read_error(
                "websocket differs from the reconciled trade",
            ));
        }
        Ok(observation.source_time)
    }

    fn verify_stream_binding_in_read<L, E>(
        &self,
        target_id: &SourceTradeId,
        receipt: AppendReceipt,
        read: &VerifiedActivityRead,
        lookup: &mut L,
    ) -> Result<SourceTimestamp, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let observation = if let Some((known, observation)) =
            read.bindings.observations.get(&receipt.sequence)
            && *known == receipt
        {
            observation.clone()
        } else {
            let source = lookup(receipt).map_err(|error| {
                complete_activity_read_error(format!("stream receipt lookup failed: {error}"))
            })?;
            verified_stream_observation(&source, self.wallet)?
        };
        if !matches!(self.version, 5..=7) {
            if observation.group_id.key() != target_id {
                return Err(complete_activity_read_error(
                    "websocket differs from the reconciled trade",
                ));
            }
            return Ok(observation.source_time);
        }
        let target = read
            .aggregates
            .iter()
            .find(|aggregate| aggregate.group_id.key() == target_id)
            .ok_or_else(|| {
                complete_activity_read_error("stream target is absent from its complete read")
            })?;
        if (observation.group_id != target.group_id
            || observation.source_time != target.source_time)
            && read
                .commitment
                .as_ref()
                .and_then(|commitment| commitment.bindings.as_deref())
                .is_none_or(|bindings| {
                    !bindings.iter().any(|binding| {
                        binding.stream_receipt == receipt
                            && binding.stream_group_id == *observation.group_id.key()
                            && binding.history_group_id == *target_id
                            && binding.semantic_revision == target.semantic_revision.as_str()
                    })
                })
        {
            return Err(complete_activity_read_error(
                "websocket correction has no verified observation binding",
            ));
        }
        Ok(SourceTimestamp(
            observation.source_time.0.min(target.source_time.0),
        ))
    }

    fn reconstruct_rich_activity_read<L, E>(
        &self,
        fixed_end: i64,
        pages: &[ReconciliationPageEvidence],
        lookup: &mut L,
    ) -> Result<Vec<pe_source_polymarket_public::NormalizedActivity>, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let joined = joined_read_pages(self.page_occurrences, pages)?;
        let mut grouped_pages = BTreeMap::<
            (Option<i64>, i64),
            Vec<(&PageOccurrence, &ReconciliationPageEvidence)>,
        >::new();
        for (occurrence, page) in joined {
            let bounds = page.bounds.ok_or_else(|| {
                complete_activity_read_error("complete activity read page has no activity bounds")
            })?;
            if page.partition.is_some()
                || page.schema_version != ACTIVITY_SCHEMA_VERSION
                || page.parser_version != ACTIVITY_PARSER_VERSION
                || page.row_count > RECONCILIATION_PAGE_LIMIT
                || page.offset > ACTIVITY_MAX_OFFSET
                || bounds.end > fixed_end
            {
                return Err(complete_activity_read_error(
                    "complete activity read page evidence is invalid",
                ));
            }
            let base_url = occurrence
                .request_url
                .split_once("/activity?")
                .map(|(base_url, _)| base_url)
                .filter(|base_url| !base_url.is_empty())
                .ok_or_else(|| {
                    complete_activity_read_error(
                        "complete activity read page request URL is invalid",
                    )
                })?;
            let expected_url = PolymarketEndpoint::UserPositionActivityPage {
                user: self.wallet.to_string(),
                end: bounds.end,
                start: bounds.start.map(|start| start.saturating_add(1)),
                offset: page.offset,
            }
            .url(base_url);
            if occurrence.request_url != expected_url {
                return Err(complete_activity_read_error(
                    "complete activity read page request URL differs from its source contract",
                ));
            }
            grouped_pages
                .entry((bounds.start, bounds.end))
                .or_default()
                .push((occurrence, page));
        }
        if grouped_pages.keys().map(|(_, end)| *end).max() != Some(fixed_end) {
            return Err(complete_activity_read_error(
                "complete activity read does not reach its fixed end",
            ));
        }

        let mut segments = BTreeMap::new();
        for ((start, end), mut pages) in grouped_pages {
            pages.sort_by_key(|(_, page)| page.offset);
            let page_count = u32::try_from(pages.len()).map_err(|_| {
                complete_activity_read_error("complete activity read page count overflow")
            })?;
            for (index, (_, page)) in pages.iter().enumerate() {
                let index = u32::try_from(index).map_err(|_| {
                    complete_activity_read_error("complete activity read page count overflow")
                })?;
                if page.offset != index.saturating_mul(RECONCILIATION_PAGE_LIMIT)
                    || (index + 1 < page_count && page.row_count != RECONCILIATION_PAGE_LIMIT)
                {
                    return Err(complete_activity_read_error(
                        "complete activity read page offsets are incomplete",
                    ));
                }
            }
            let Some((_, terminal)) = pages.last() else {
                return Err(complete_activity_read_error(
                    "complete activity read contains an empty segment",
                ));
            };
            let saturated = terminal.offset == ACTIVITY_MAX_OFFSET
                && terminal.row_count == RECONCILIATION_PAGE_LIMIT;
            if terminal.row_count == RECONCILIATION_PAGE_LIMIT && !saturated {
                return Err(complete_activity_read_error(
                    "complete activity read segment has no terminal page",
                ));
            }
            let mut segment_rows = Vec::new();
            for (occurrence, page) in pages {
                let source = complete_activity_page(occurrence, self.version, lookup)?;
                let actual_canonical_hash =
                    canonical_page_hash(&source.payload).map_err(|error| {
                        complete_activity_read_error(format!(
                            "complete activity read canonical page hash failed: {error}"
                        ))
                    })?;
                if actual_canonical_hash != page.canonical_page_hash {
                    return Err(complete_activity_read_error(
                        "complete activity read canonical page hash differs",
                    ));
                }
                let window = parse_complete_activity_page(
                    &source,
                    self.wallet,
                    "complete activity read page parse failed",
                )?;
                if u32::try_from(window.rows.len()).ok() != Some(page.row_count)
                    || window.rows.iter().any(|row| {
                        let timestamp = row.source_time.0.unix_timestamp();
                        timestamp > end || start.is_some_and(|start| timestamp <= start)
                    })
                {
                    return Err(complete_activity_read_error(
                        "complete activity read page differs from its bounds/count",
                    ));
                }
                segment_rows.extend(window.rows);
            }

            let children = if saturated {
                let boundary = segment_rows
                    .iter()
                    .map(|row| row.source_time.0.unix_timestamp())
                    .min()
                    .ok_or_else(|| {
                        complete_activity_read_error(
                            "saturated complete activity read segment has no rows",
                        )
                    })?;
                let terminal_start = boundary.checked_sub(1).ok_or_else(|| {
                    complete_activity_read_error("complete activity read split boundary underflow")
                })?;
                if (start.is_some_and(|value| value >= terminal_start) && end <= boundary)
                    || boundary > end
                    || start.is_some_and(|value| boundary <= value)
                {
                    return Err(complete_activity_read_error(
                        "complete activity read has an invalid saturated split",
                    ));
                }
                let mut children = Vec::with_capacity(3);
                if boundary < end {
                    children.push((Some(boundary), end));
                }
                children.push((Some(terminal_start), boundary));
                if start.is_none_or(|value| value < terminal_start) {
                    children.push((start, terminal_start));
                }
                children
            } else {
                Vec::new()
            };
            segments.insert(
                (start, end),
                CompleteActivitySegment {
                    rows: segment_rows,
                    children,
                },
            );
        }

        validate_complete_activity_segment_graph(fixed_end, &segments)?;
        Ok(segments
            .into_values()
            .filter(|segment| segment.children.is_empty())
            .flat_map(|segment| segment.rows)
            .collect())
    }
}

impl DecisionContinuationV3 {
    /// Resolve every V3 receipt through the boot-owned verified source-receipt index and derive
    /// observation time from the selected receipt. No timestamp copied into continuation JSON is
    /// trusted, and the growing source log is never replayed on this hot path (#545).
    pub fn observation_from_receipt_index(
        &self,
        source_receipts: &SourceReceiptIndex,
    ) -> Result<Option<ObservationEvidence>, DecisionContinuationError> {
        if self.is_activity_frame() {
            self.verify_activity_frame_with_index(source_receipts)
                .map_err(|_| DecisionContinuationError::DurableMismatch)?;
            let receipt = self
                .observed_source_receipt
                .ok_or(DecisionContinuationError::DurableMismatch)?;
            return Ok(Some(ObservationEvidence {
                source_receipt: receipt,
                complete_bound_receipt: receipt,
                observed_unix_ms: source_receipts.received_millis(receipt)?,
                provenance: "activity_ws".to_owned(),
            }));
        }
        self.require_complete_read()?;
        let Some(selected) = self.observation_receipt() else {
            return Ok(None);
        };
        if matches!(self.version, 5..=7) {
            let mut lookup = |receipt| {
                #[cfg(test)]
                continuation_validation_tests::LOOKUPS.with(|count| count.set(count.get() + 1));
                source_receipts
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
            };
            let proof: CompleteActivityReadWire =
                serde_json::from_value(self.facts.decision_inputs.clone())?;
            let (Some(fixed_end), Some(pages)) = (proof.fixed_end, proof.pages) else {
                return Err(DecisionContinuationError::DurableMismatch);
            };
            let mismatch = || DecisionContinuationError::SourceReceiptMismatch {
                sequence: selected.sequence.0,
            };
            let commitment = self
                .verify_read_commitment(fixed_end, &pages, &mut lookup)
                .map_err(|_| mismatch())?;
            if self.observed_source_receipt.is_some()
                || commitment
                    .as_ref()
                    .and_then(|commitment| commitment.bindings.as_ref())
                    .is_some_and(|bindings| !bindings.is_empty())
            {
                let read = self
                    .reconstruct_verified_activity_read_parts(
                        fixed_end,
                        &pages,
                        commitment,
                        &mut lookup,
                    )
                    .map_err(|_| mismatch())?;
                if let Some(websocket) = self.observed_source_receipt {
                    self.verify_stream_binding_in_read(
                        &self.facts.source_trade_id,
                        websocket,
                        &read,
                        &mut lookup,
                    )
                    .map_err(|_| {
                        DecisionContinuationError::SourceReceiptMismatch {
                            sequence: websocket.sequence.0,
                        }
                    })?;
                }
            } else {
                // Preserve the poll-only empty-binding fast path: boot/qualification own
                // aggregate reconstruction, while observation authenticates exact receipts.
                for page in &self.page_occurrences {
                    complete_activity_page(page, self.version, &mut lookup).map_err(|_| {
                        DecisionContinuationError::SourceReceiptMismatch {
                            sequence: page.receipt.sequence.0,
                        }
                    })?;
                }
            }
            return self
                .observation_at(source_receipts.received_millis(selected)?)
                .ok_or(DecisionContinuationError::SourceReceiptMismatch {
                    sequence: selected.sequence.0,
                })
                .map(Some);
        }
        for page in &self.page_occurrences {
            let envelope = source_receipts.source_envelope(page.receipt)?;
            if !activity_page_generation(
                &envelope.source_id.0,
                envelope.schema_version,
                envelope.parser_version,
                &envelope.content_type,
            )
            .is_ok_and(|generation| generation.matches_continuation(self.version))
                || envelope.raw_payload_hash.to_hex().as_str() != page.raw_hash
            {
                return Err(DecisionContinuationError::SourceReceiptMismatch {
                    sequence: page.receipt.sequence.0,
                });
            }
        }
        if self.commitment_contract().is_some() {
            let proof: CompleteActivityReadWire =
                serde_json::from_value(self.facts.decision_inputs.clone())?;
            let result = match (proof.fixed_end, proof.pages) {
                (Some(end), Some(pages)) => {
                    self.verify_read_commitment(end, &pages, &mut |receipt| {
                        source_receipts
                            .source_envelope(receipt)
                            .map(CompleteActivityPage::from)
                    })
                }
                _ => return Err(DecisionContinuationError::DurableMismatch),
            };
            // Legacy commitments carry no bindings (the shared verifier rejects them for
            // generations below 5); generation 5 returned above through its bound read.
            result.map_err(|_| DecisionContinuationError::SourceReceiptMismatch {
                sequence: selected.sequence.0,
            })?;
        }
        self.verify_websocket_receipt(source_receipts)?;
        let observed_unix_ms = source_receipts.received_millis(selected)?;
        self.observation_at(observed_unix_ms)
            .ok_or(DecisionContinuationError::SourceReceiptMismatch {
                sequence: selected.sequence.0,
            })
            .map(Some)
    }
}

/// Source corrections preserve the oldest authenticated clock, independently of receive order.
pub(crate) fn earliest_bound_source_time(
    history: time::OffsetDateTime,
    observations: impl Iterator<Item = time::OffsetDateTime>,
) -> time::OffsetDateTime {
    observations.fold(history, time::OffsetDateTime::min)
}

impl DecisionContinuationV3 {
    /// Verify the frozen websocket receipt, when present, through exact indexed reads: source
    /// contract, payload parse, and binding to this continuation's wallet and trade (#565).
    pub(crate) fn verify_websocket_receipt(
        &self,
        source_receipts: &SourceReceiptIndex,
    ) -> Result<(), DecisionContinuationError> {
        self.require_complete_read()?;
        let Some(websocket) = self.observed_source_receipt else {
            return Ok(());
        };
        self.verify_stream_binding(&self.facts.source_trade_id, websocket, &mut |receipt| {
            source_receipts
                .source_envelope(receipt)
                .map(CompleteActivityPage::from)
        })
        .map_err(|_| DecisionContinuationError::SourceReceiptMismatch {
            sequence: websocket.sequence.0,
        })?;
        Ok(())
    }

    /// Verify the complete history and its bindings before selecting the earliest source clock and target asset.
    /// Receive timestamps remain separate evidence for copy-delay measurement.
    pub(crate) fn verified_source_time<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<(SourceTimestamp, Option<PolymarketTokenId>), CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        if self.is_activity_frame() {
            return self.verify_activity_frame(lookup);
        }
        let aggregates = self.reconstruct_complete_activity_read(lookup)?;
        let target = aggregates
            .iter()
            .find(|aggregate| aggregate.group_id.key() == &self.facts.source_trade_id)
            .filter(|aggregate| {
                aggregate.semantic_revision.as_str() == self.facts.semantic_revision
                    && aggregate.source_time.0.unix_timestamp() == self.facts.source_epoch
            })
            .ok_or_else(|| {
                complete_activity_read_error("source clock target differs from continuation")
            })?;
        let mut earliest = target.source_time.clone();
        if matches!(self.version, 5..=7) {
            let wire: CompleteActivityReadWire =
                serde_json::from_value(self.facts.decision_inputs.clone())
                    .map_err(|error| complete_activity_read_error(error.to_string()))?;
            let commitment = self
                .verify_read_commitment(
                    wire.fixed_end
                        .ok_or_else(|| complete_activity_read_error("read end missing"))?,
                    wire.pages
                        .as_deref()
                        .ok_or_else(|| complete_activity_read_error("read pages missing"))?,
                    lookup,
                )?
                .ok_or_else(|| complete_activity_read_error("binding commitment is absent"))?;
            for binding in commitment.bindings.iter().flatten().filter(|binding| {
                binding.history_group_id == self.facts.source_trade_id
                    && binding.semantic_revision == self.facts.semantic_revision
            }) {
                let source = lookup(binding.stream_receipt).map_err(|error| {
                    complete_activity_read_error(format!("stream receipt lookup failed: {error}"))
                })?;
                let observation = verified_stream_observation(&source, self.facts.wallet)?;
                earliest = SourceTimestamp(earliest_bound_source_time(
                    earliest.0,
                    std::iter::once(observation.source_time.0),
                ));
            }
        }
        Ok((earliest, target.group_id.components().asset.clone()))
    }
}

impl DecisionContinuationFacts {
    /// The applied effect recorded with this trade's durable activity group, which must carry the
    /// frozen semantic revision and transaction hash (#565). The effect's recorded identity
    /// correction, when present, is what the fact comparator honors.
    pub(crate) fn durable_group_effect(
        &self,
        paper_state: &PaperStateDb,
    ) -> Result<AppliedEffect, String> {
        let group = paper_state
            .activity_group_state(&self.source_trade_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "decision activity group is absent".to_owned())?;
        if group.semantic_revision != self.semantic_revision
            || group.transaction_hash != self.transaction_hash
        {
            return Err("decision activity group differs from its frozen identity".to_owned());
        }
        AppliedEffect::from_document(&group.proof_json)
            .map_err(|error| format!("decision activity effect is invalid: {error}"))
    }
}

/// Which producer generation wrote one reconciliation page envelope (#565).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageGeneration {
    /// Written before the read commitment existed (`schema_version == ACTIVITY_SCHEMA_VERSION`).
    Historical,
    /// Written by a commitment-aware producer (`schema_version == ACTIVITY_POLL_PAGE_SCHEMA_VERSION`).
    Committed,
}

impl PageGeneration {
    pub(crate) fn matches_continuation(self, version: u16) -> bool {
        matches!(
            (self, version),
            (Self::Historical, 3) | (Self::Committed, 4..=7)
        )
    }
}

/// The one shared page-contract check: source id, schema generation, parser version, and content
/// type of one reconciliation page envelope. Every verifier routes its page checks through here.
pub(crate) fn activity_page_generation(
    source_id: &str,
    schema_version: u32,
    parser_version: u32,
    content_type: &ContentType,
) -> Result<PageGeneration, CompleteActivityReadError> {
    if source_id != crate::trade_poller::ACTIVITY_POLL_SOURCE_ID
        || parser_version != ACTIVITY_PARSER_VERSION
        || *content_type != ContentType::Json
    {
        return Err(complete_activity_read_error(
            "complete activity read page has the wrong source contract",
        ));
    }
    match schema_version {
        ACTIVITY_SCHEMA_VERSION => Ok(PageGeneration::Historical),
        crate::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION => Ok(PageGeneration::Committed),
        _ => Err(complete_activity_read_error(
            "complete activity read page has the wrong source contract",
        )),
    }
}

/// A receipt-bound stream observation associated with one complete history aggregate.
/// `page_occurrence_index` is zero-based in the acquisition-ordered `page_occurrences`;
/// `page_raw_hash` must match that occurrence. Metadata corrections carry the resolver's
/// existing provenance and the exact receipt at its source sequence.
/// Bindings sort lexicographically by their canonical JSON bytes (sorted object keys).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationBinding {
    pub stream_group_id: SourceTradeId,
    pub stream_receipt: AppendReceipt,
    pub history_group_id: SourceTradeId,
    pub semantic_revision: String,
    pub page_raw_hash: String,
    pub page_occurrence_index: u32,
    #[serde(deserialize_with = "deserialize_binding_provenance")]
    pub identity_provenance: Option<crate::asset_identity::IdentityProvenance>,
    pub identity_receipt: Option<AppendReceipt>,
    /// First binding, or the full-history absence proof preceding a late counterpart.
    /// Omitted on initial discovery and historical bindings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counterpart_basis_receipt: Option<AppendReceipt>,
    /// Historical admitted-frame binding authority; new commitments omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_admission_receipt: Option<AppendReceipt>,
}

fn deserialize_binding_provenance<'de, D>(
    deserializer: D,
) -> Result<Option<crate::asset_identity::IdentityProvenance>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Provenance {
        asset: pe_core_types::PolymarketTokenId,
        source_log_sequence: u64,
        canonical_page_hash: String,
    }
    Ok(
        Option::<Provenance>::deserialize(deserializer)?.map(|value| {
            crate::asset_identity::IdentityProvenance {
                asset: value.asset,
                source_log_sequence: value.source_log_sequence,
                canonical_page_hash: value.canonical_page_hash,
            }
        }),
    )
}

/// Payload of a complete-read commitment. V1 has no `bindings` field; V2 requires it,
/// including the empty list for reads with no selected observation bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityReadCommitment {
    pub version: u16,
    pub wallet: WalletAddress,
    pub fixed_end: i64,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bindings: Option<Vec<ObservationBinding>>,
    /// Existing digest inputs retained for binding authentication without a pending decision.
    /// Absent on legacy and frontier-only commitments; historical negative audits may retain it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_proof: Option<CommittedReadProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedReadProof {
    pub page_occurrences: Vec<PageOccurrence>,
    pub pages: Vec<ReconciliationPageEvidence>,
}

/// Immutable v1 preimage; never add fields to this historical encoding.
#[derive(Serialize)]
struct ActivityReadPreimage<'a> {
    wallet: WalletAddress,
    fixed_end: i64,
    pages: Vec<(&'a PageOccurrence, &'a ReconciliationPageEvidence)>,
}

pub(crate) fn canonical_json(value: &impl Serialize) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_value(value).and_then(|value| serde_json::to_vec(&value))
}

fn canonical_bindings(
    bindings: &[ObservationBinding],
) -> Result<Vec<ObservationBinding>, CompleteActivityReadError> {
    let mut encoded = bindings
        .iter()
        .map(|binding| {
            canonical_json(binding)
                .map(|bytes| (bytes, binding.clone()))
                .map_err(|error| {
                    complete_activity_read_error(format!("commitment encoding failed: {error}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    let mut receipts = HashSet::new();
    if encoded
        .iter()
        .any(|(_, binding)| !receipts.insert(binding.stream_receipt.sequence))
    {
        return Err(complete_activity_read_error(
            "observation bindings repeat a stream receipt",
        ));
    }
    Ok(encoded.into_iter().map(|(_, binding)| binding).collect())
}

fn activity_read_digest_versioned(
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
    bindings: Option<&[ObservationBinding]>,
) -> Result<blake3::Hash, CompleteActivityReadError> {
    let preimage = ActivityReadPreimage {
        wallet,
        fixed_end,
        pages: joined_read_pages(occurrences, pages)?,
    };
    let (domain, canonical) = match bindings {
        None => (
            ACTIVITY_READ_COMMITMENT_V1_DOMAIN,
            canonical_json(&preimage),
        ),
        Some(bindings) => {
            #[derive(Serialize)]
            struct PreimageV2<'a> {
                #[serde(flatten)]
                read: ActivityReadPreimage<'a>,
                bindings: Vec<ObservationBinding>,
            }
            (
                ACTIVITY_READ_COMMITMENT_DOMAIN,
                canonical_json(&PreimageV2 {
                    read: preimage,
                    bindings: canonical_bindings(bindings)?,
                }),
            )
        }
    };
    let canonical = canonical.map_err(|error| {
        complete_activity_read_error(format!("commitment encoding failed: {error}"))
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&canonical);
    Ok(hasher.finalize())
}

/// Exact legacy digest retained for authentic generation-one fixtures and reconstruction.
pub(crate) fn activity_read_digest(
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
) -> Result<blake3::Hash, CompleteActivityReadError> {
    activity_read_digest_versioned(wallet, fixed_end, occurrences, pages, None)
}

/// Immutable legacy payload, envelope schema 1 and parser 1.
pub fn activity_read_commitment_payload_v1(
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
) -> Result<Vec<u8>, CompleteActivityReadError> {
    encode_activity_read_commitment(wallet, fixed_end, occurrences, pages, None)
}

/// Current payload, envelope schema 2 and parser 1. An empty binding list is valid.
pub fn activity_read_commitment_payload(
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
) -> Result<Vec<u8>, CompleteActivityReadError> {
    activity_read_commitment_payload_v2(wallet, fixed_end, occurrences, pages, &[])
}

/// Encode the complete current contract, including canonical observation bindings.
pub fn activity_read_commitment_payload_v2(
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
    bindings: &[ObservationBinding],
) -> Result<Vec<u8>, CompleteActivityReadError> {
    encode_activity_read_commitment(wallet, fixed_end, occurrences, pages, Some(bindings))
}

fn encode_activity_read_commitment(
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
    bindings: Option<&[ObservationBinding]>,
) -> Result<Vec<u8>, CompleteActivityReadError> {
    let digest = match bindings {
        None => activity_read_digest(wallet, fixed_end, occurrences, pages)?,
        Some(bindings) => {
            activity_read_digest_versioned(wallet, fixed_end, occurrences, pages, Some(bindings))?
        }
    };
    serde_json::to_vec(&ActivityReadCommitment {
        version: if bindings.is_some() { 2 } else { 1 },
        wallet,
        fixed_end,
        digest: digest.to_hex().to_string(),
        bindings: bindings.map(canonical_bindings).transpose()?,
        read_proof: bindings
            .filter(|bindings| !bindings.is_empty())
            .map(|_| CommittedReadProof {
                page_occurrences: occurrences.to_vec(),
                pages: pages.to_vec(),
            }),
    })
    .map_err(|error| complete_activity_read_error(format!("commitment payload failed: {error}")))
}

/// Authenticate only selected obligation receipts, retaining the complete read's proof checks.
/// A verified commitment is evidence of correlation, never evidence of durable disposition.
pub(crate) fn verified_commitment_bindings(
    receipt: AppendReceipt,
    source_receipts: &SourceReceiptIndex,
    binding_filter: &HashSet<(pe_core_types::EventSeq, blake3::Hash)>,
) -> Result<VerifiedCommitment, CompleteActivityReadError> {
    #[cfg(feature = "scenario")]
    source_receipts.record_read_verification(receipt);
    let epoch = source_receipts.retention_epoch();
    let mut read = verified_commitment_bindings_at_depth(
        receipt,
        &mut |receipt| {
            source_receipts
                .source_envelope(receipt)
                .map(CompleteActivityPage::from)
                .map_err(|error| complete_activity_read_error(error.to_string()))
        },
        0,
        Some(binding_filter),
    )?;
    read.retention_epoch = Some(epoch);
    Ok(read)
}

/// An authenticated commitment: its bindings and the restamp pairs its complete read proves.
#[derive(Debug)]
pub struct VerifiedCommitment {
    pub(crate) retention_epoch: Option<u64>,
    pub(crate) receipt: AppendReceipt,
    pub(crate) frontier: Option<crate::frame_admission::FeedHistoryFrontier>,
    pub(crate) bindings: Vec<ObservationBinding>,
    pub(crate) restamp_pairs: HashMap<SourceTradeId, SourceTradeId>,
    pub(crate) wallet: WalletAddress,
    pub(crate) fixed_end: i64,
    pub(crate) full_history: bool,
    pub(crate) transaction_aggregates: HashMap<String, Vec<usize>>,
    pub(crate) binding_indices: HashMap<(pe_core_types::EventSeq, blake3::Hash), usize>,
    pub(crate) aggregates: Vec<ActivityAggregate>,
    pub(crate) identities: HashMap<SourceTradeId, MarketOutcomeId>,
}

fn index_bindings(
    bindings: &[ObservationBinding],
) -> HashMap<(pe_core_types::EventSeq, blake3::Hash), usize> {
    bindings
        .iter()
        .enumerate()
        .map(|(index, binding)| {
            (
                (
                    binding.stream_receipt.sequence,
                    binding.stream_receipt.this_hash,
                ),
                index,
            )
        })
        .collect()
}

fn index_transactions(aggregates: &[ActivityAggregate]) -> HashMap<String, Vec<usize>> {
    let mut transactions: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, aggregate) in aggregates.iter().enumerate() {
        transactions
            .entry(aggregate.group_id.components().transaction_hash.clone())
            .or_default()
            .push(index);
    }
    transactions
}

pub fn verified_read_for_routing(
    receipt: AppendReceipt,
    wallet: WalletAddress,
    fixed_end: i64,
    occurrences: &[PageOccurrence],
    pages: &[ReconciliationPageEvidence],
    index: &SourceReceiptIndex,
) -> Result<VerifiedCommitment, CompleteActivityReadError> {
    #[cfg(feature = "scenario")]
    index.record_read_verification(receipt);
    let epoch = index.retention_epoch();
    let inputs = json!({"fixed_end": fixed_end, "pages": pages});
    let verifier = ActivityReadVerification {
        binding_filter: None,
        counterpart_depth: 0,
        version: 7,
        wallet,
        decision_inputs: &inputs,
        page_occurrences: occurrences,
        read_commitment: Some(receipt),
    };
    let read = verifier.reconstruct_verified_activity_read(&mut |receipt| {
        index
            .source_envelope(receipt)
            .map(CompleteActivityPage::from)
    })?;
    let read = VerifiedCommitment {
        retention_epoch: Some(epoch),
        receipt,
        frontier: Some(crate::frame_admission::FeedHistoryFrontier {
            version: 1,
            wallet,
            fixed_end,
            commitment: receipt,
            page_occurrences: occurrences.to_vec(),
            pages: pages.to_vec(),
        }),
        wallet,
        fixed_end,
        full_history: pages.iter().any(|page| {
            page.bounds
                .is_some_and(|bounds| bounds.start == Some(0) && bounds.end == fixed_end)
        }),
        binding_indices: index_bindings(
            read.commitment
                .as_ref()
                .and_then(|commitment| commitment.bindings.as_deref())
                .unwrap_or_default(),
        ),
        transaction_aggregates: index_transactions(&read.aggregates),
        bindings: read
            .commitment
            .and_then(|commitment| commitment.bindings)
            .unwrap_or_default(),
        aggregates: read.aggregates,
        restamp_pairs: read.bindings.restamp_pairs,
        identities: read.bindings.identities,
    };
    Ok(read)
}

/// The same commitment-only authentication for either an indexed or replayed sealed prefix.
pub(crate) fn verified_commitment_bindings_with_lookup<L, E>(
    receipt: AppendReceipt,
    lookup: &mut L,
) -> Result<VerifiedCommitment, CompleteActivityReadError>
where
    L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
    E: Display,
{
    verified_commitment_bindings_at_depth(
        receipt,
        &mut |receipt| {
            lookup(receipt).map_err(|error| complete_activity_read_error(error.to_string()))
        },
        0,
        None,
    )
}

fn verified_commitment_bindings_at_depth(
    receipt: AppendReceipt,
    mut lookup: &mut dyn FnMut(
        AppendReceipt,
    ) -> Result<CompleteActivityPage, CompleteActivityReadError>,
    counterpart_depth: u8,
    binding_filter: Option<&HashSet<(pe_core_types::EventSeq, blake3::Hash)>>,
) -> Result<VerifiedCommitment, CompleteActivityReadError> {
    if counterpart_depth > 2 {
        return Err(complete_activity_read_error(
            "frame counterpart proof chain exceeds its first binding/absence basis",
        ));
    }
    let source =
        lookup(receipt).map_err(|error| complete_activity_read_error(error.to_string()))?;
    let commitment: ActivityReadCommitment = serde_json::from_slice(&source.payload)
        .map_err(|error| complete_activity_read_error(error.to_string()))?;
    if source.source_id != ACTIVITY_READ_COMMITMENT_SOURCE_ID
        || source.schema_version != ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION
        || source.parser_version != ACTIVITY_READ_COMMITMENT_PARSER_VERSION
        || source.content_type != ContentType::Json
        || commitment.version != 2
    {
        return Err(complete_activity_read_error(
            "binding commitment source generation differs",
        ));
    }
    let bindings = commitment
        .bindings
        .as_ref()
        .ok_or_else(|| complete_activity_read_error("v2 commitment bindings are absent"))?;
    if bindings.is_empty() && commitment.read_proof.is_none() {
        return Ok(VerifiedCommitment {
            retention_epoch: None,
            receipt,
            frontier: None,
            bindings: Vec::new(),
            restamp_pairs: HashMap::new(),
            wallet: commitment.wallet,
            fixed_end: commitment.fixed_end,
            full_history: false,
            binding_indices: HashMap::new(),
            transaction_aggregates: HashMap::new(),
            aggregates: Vec::new(),
            identities: HashMap::new(),
        });
    }
    let proof = commitment
        .read_proof
        .as_ref()
        .ok_or_else(|| complete_activity_read_error("binding commitment read proof is absent"))?;
    let inputs = json!({ "fixed_end": commitment.fixed_end, "pages": proof.pages });
    let verifier = ActivityReadVerification {
        binding_filter,
        counterpart_depth,
        version: 5,
        wallet: commitment.wallet,
        decision_inputs: &inputs,
        page_occurrences: &proof.page_occurrences,
        read_commitment: Some(receipt),
    };
    let read = verifier.reconstruct_verified_activity_read(&mut lookup)?;
    let selected_bindings = bindings
        .iter()
        .filter(|binding| {
            binding_filter.is_none_or(|receipts| {
                receipts.contains(&(
                    binding.stream_receipt.sequence,
                    binding.stream_receipt.this_hash,
                ))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    Ok(VerifiedCommitment {
        retention_epoch: None,
        receipt,
        frontier: Some(crate::frame_admission::FeedHistoryFrontier {
            version: 1,
            wallet: commitment.wallet,
            fixed_end: commitment.fixed_end,
            commitment: receipt,
            page_occurrences: proof.page_occurrences.clone(),
            pages: proof.pages.clone(),
        }),
        binding_indices: index_bindings(&selected_bindings),
        bindings: selected_bindings,
        restamp_pairs: read.bindings.restamp_pairs,
        identities: read.bindings.identities,
        wallet: commitment.wallet,
        fixed_end: commitment.fixed_end,
        full_history: proof.pages.iter().any(|page| {
            page.bounds
                .is_some_and(|bounds| bounds.start == Some(0) && bounds.end == commitment.fixed_end)
        }),
        transaction_aggregates: index_transactions(&read.aggregates),
        aggregates: read.aggregates,
    })
}

pub(crate) fn joined_read_pages<'a>(
    occurrences: &'a [PageOccurrence],
    pages: &'a [ReconciliationPageEvidence],
) -> Result<Vec<(&'a PageOccurrence, &'a ReconciliationPageEvidence)>, CompleteActivityReadError> {
    if occurrences.len() != pages.len() {
        return Err(complete_activity_read_error(
            "complete activity read page multiplicity is inconsistent",
        ));
    }
    let mut evidence = BTreeMap::<(String, String), VecDeque<&ReconciliationPageEvidence>>::new();
    for page in pages {
        evidence
            .entry((page.request_url.clone(), page.raw_page_hash.clone()))
            .or_default()
            .push_back(page);
    }
    let mut joined = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let key = (occurrence.request_url.clone(), occurrence.raw_hash.clone());
        let page = evidence
            .get_mut(&key)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| {
                complete_activity_read_error(
                    "complete activity read page occurrence has no page evidence",
                )
            })?;
        joined.push((occurrence, page));
    }
    Ok(joined)
}

fn complete_activity_page<L, E>(
    occurrence: &PageOccurrence,
    version: u16,
    lookup: &mut L,
) -> Result<CompleteActivityPage, CompleteActivityReadError>
where
    L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
    E: Display,
{
    let source = lookup(occurrence.receipt).map_err(|error| {
        complete_activity_read_error(format!(
            "complete activity read page receipt lookup failed: {error}"
        ))
    })?;
    let generation = activity_page_generation(
        &source.source_id,
        source.schema_version,
        source.parser_version,
        &source.content_type,
    )?;
    if !generation.matches_continuation(version) {
        return Err(complete_activity_read_error(
            "complete activity read page generation differs from continuation version",
        ));
    }
    if blake3::hash(&source.payload).to_hex().as_str() != occurrence.raw_hash {
        return Err(complete_activity_read_error(
            "complete activity read page payload hash differs",
        ));
    }
    Ok(source)
}

fn verified_stream_observation(
    source: &CompleteActivityPage,
    wallet: WalletAddress,
) -> Result<pe_source_polymarket_public::ActivityTradeObservation, CompleteActivityReadError> {
    if source.source_id != crate::activity_ingest::ACTIVITY_WS_SOURCE_ID
        || source.schema_version != ACTIVITY_SCHEMA_VERSION
        || source.parser_version != ACTIVITY_PARSER_VERSION
        || source.content_type != ContentType::Json
    {
        return Err(complete_activity_read_error(
            "binding stream has the wrong source contract",
        ));
    }
    let observation = parse_activity_trade_observation(&source.payload).map_err(|error| {
        complete_activity_read_error(format!("binding stream parse failed: {error}"))
    })?;
    if observation.wallet != wallet {
        return Err(complete_activity_read_error(
            "binding stream wallet differs",
        ));
    }
    Ok(observation)
}

pub(crate) fn verify_binding_identity(
    source: &CompleteActivityPage,
    provenance: &crate::asset_identity::IdentityProvenance,
) -> Result<
    pe_source_polymarket_public::gamma_markets::VerifiedTokenIdentity,
    CompleteActivityReadError,
> {
    use pe_source_polymarket_public::{
        GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION, GAMMA_MARKETS_SOURCE_ID,
        MetadataPageEvidence,
    };
    if source.source_id != GAMMA_MARKETS_SOURCE_ID
        || source.schema_version != GAMMA_MARKETS_SCHEMA_VERSION
        || source.parser_version != GAMMA_MARKETS_PARSER_VERSION
        || source.content_type != ContentType::Json
        || canonical_page_hash(&source.payload)
            .map_err(|error| complete_activity_read_error(error.to_string()))?
            != provenance.canonical_page_hash
    {
        return Err(complete_activity_read_error(
            "binding metadata has the wrong source contract or page hash",
        ));
    }
    let evidence = MetadataPageEvidence {
        request_url: String::new(),
        canonical_page_hash: provenance.canonical_page_hash.clone(),
        raw_page_hash: blake3::hash(&source.payload).to_hex().to_string(),
        received_at: source.received_at.clone(),
        source_id: SourceId(source.source_id.clone()),
        schema_version: source.schema_version,
        parser_version: source.parser_version,
    };
    let mut identities = pe_source_polymarket_public::gamma_markets::verify_token_identities(
        std::slice::from_ref(&provenance.asset),
        &[(evidence, source.payload.clone())],
    );
    identities
        .remove(&provenance.asset)
        .and_then(Result::ok)
        .ok_or_else(|| {
            complete_activity_read_error("binding token identity is unverified or ambiguous")
        })
}

fn parse_complete_activity_page(
    source: &CompleteActivityPage,
    wallet: WalletAddress,
    error_context: &str,
) -> Result<pe_source_polymarket_public::NormalizedActivityWindow, CompleteActivityReadError> {
    parse_activity_response(
        &source.payload,
        wallet,
        &ActivityParseContext {
            source_id: SourceId(source.source_id.clone()),
            observed_at: source.observed_at.clone(),
            received_at: source.received_at.clone(),
            transport: ActivityTransport::Replay,
        },
    )
    .map_err(|error| complete_activity_read_error(format!("{error_context}: {error}")))
}

fn validate_complete_activity_segment_graph(
    fixed_end: i64,
    segments: &BTreeMap<(Option<i64>, i64), CompleteActivitySegment>,
) -> Result<(), CompleteActivityReadError> {
    let mut parent_counts = BTreeMap::<(Option<i64>, i64), usize>::new();
    for segment in segments.values() {
        for child in &segment.children {
            if !segments.contains_key(child) {
                return Err(complete_activity_read_error(
                    "complete activity read is missing a split child segment",
                ));
            }
            let count = parent_counts.entry(*child).or_default();
            *count = count.saturating_add(1);
            if *count > 1 {
                return Err(complete_activity_read_error(
                    "complete activity read split child has multiple parents",
                ));
            }
        }
    }
    let roots = segments
        .keys()
        .filter(|bounds| !parent_counts.contains_key(bounds))
        .copied()
        .collect::<Vec<_>>();
    let [root] = roots.as_slice() else {
        return Err(complete_activity_read_error(
            "complete activity read does not have one root segment",
        ));
    };
    if root.1 != fixed_end {
        return Err(complete_activity_read_error(
            "complete activity read root differs from its fixed end",
        ));
    }
    let mut pending = vec![*root];
    let mut visited = HashSet::new();
    while let Some(bounds) = pending.pop() {
        if !visited.insert(bounds) {
            return Err(complete_activity_read_error(
                "complete activity read split graph repeats a segment",
            ));
        }
        let segment = segments.get(&bounds).ok_or_else(|| {
            complete_activity_read_error("complete activity read split segment is absent")
        })?;
        pending.extend(segment.children.iter().copied());
    }
    if visited.len() != segments.len() {
        return Err(complete_activity_read_error(
            "complete activity read contains an unrelated segment",
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum DecisionContinuationError {
    #[error("frame admission refused: {0}")]
    FrameAdmissionRefused(&'static str),
    #[error("invalid frozen continuation json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported frozen continuation version {0}")]
    Version(u16),
    #[error("frozen continuation does not match durable row")]
    DurableMismatch,
    #[error("frozen continuation has invalid source epoch {0}")]
    SourceEpoch(i64),
    #[error("source receipt lookup failed: {0}")]
    SourceReceiptLookup(#[from] crate::risk_inputs::RiskInputsUnavailable),
    #[error("source receipt sequence {sequence} does not match its frozen evidence")]
    SourceReceiptMismatch { sequence: u64 },
}

impl DecisionContinuationV3 {
    /// Decode and bind a frozen continuation to its durable outer row.
    pub fn from_durable(row: &DecisionPendingRow) -> Result<Self, DecisionContinuationError> {
        let mut value: Value = serde_json::from_str(&row.frozen_inputs_json)?;
        let version = value
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or(DecisionContinuationError::Version(0))?;
        let policy_present = value.get("paper_freshness_policy").is_some();
        let authority_present = value.get("source_authority").is_some();
        if (version == 7) != authority_present {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        let continuation = match version {
            2 => {
                let applied_configuration =
                    value.get_mut("applied_configuration").ok_or_else(|| {
                        <serde_json::Error as serde::de::Error>::custom(
                            "missing field `applied_configuration`",
                        )
                    })?;
                let configuration =
                    decode_pre_545_runtime_config(std::mem::take(applied_configuration))?;
                *applied_configuration = serde_json::to_value(configuration)?;
                let legacy: DecisionContinuationV2Wire = serde_json::from_value(value)?;
                Self {
                    version: legacy.version,
                    source_authority: None,
                    facts: legacy.facts,
                    observed_source_receipt: None,
                    page_occurrences: Vec::new(),
                    read_commitment: None,
                }
            }
            3..=7 => serde_json::from_value(value)?,
            version => return Err(DecisionContinuationError::Version(version)),
        };
        if (version == 7) != continuation.source_authority.is_some() {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        let frozen = &continuation.facts;
        if frozen.source_trade_id != row.source_trade_id
            || frozen.semantic_revision != row.semantic_revision
            || frozen.wallet != row.wallet
            || frozen.source_epoch != row.source_epoch
            || frozen.gate_result != "admitted"
            || frozen.pre_bucket_action != LeaderAction::Entry
            || frozen.applied_configuration.canonical_hash() != frozen.applied_configuration_hash
        {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        if (matches!(version, 5..=7)) != policy_present
            || (matches!(version, 5..=7)) != frozen.paper_freshness_policy.is_some()
            || frozen
                .paper_freshness_policy
                .is_some_and(|policy| !policy.valid())
        {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        if matches!(version, 3..=7)
            && continuation.source_authority != Some(SourceAuthority::ActivityFrame)
        {
            if (frozen.provenance == TradeProvenance::ActivityWs)
                != continuation.observed_source_receipt.is_some()
                || matches!(version, 4..=7) != continuation.read_commitment.is_some()
                || continuation.read_commitment.is_some_and(|receipt| {
                    continuation
                        .page_occurrences
                        .iter()
                        .any(|page| page.receipt.sequence >= receipt.sequence)
                })
            {
                return Err(DecisionContinuationError::DurableMismatch);
            }
            let page_occurrences = continuation.page_occurrences();
            if page_occurrences.is_empty() {
                return Err(DecisionContinuationError::DurableMismatch);
            }
            let proof: CompleteActivityReadWire =
                serde_json::from_value(continuation.facts.decision_inputs.clone())?;
            if proof.fixed_end.is_none() || proof.pages.as_ref().is_none_or(Vec::is_empty) {
                return Err(DecisionContinuationError::DurableMismatch);
            }
            let mut previous = None;
            for page in page_occurrences {
                if previous.is_some_and(|sequence| page.receipt.sequence <= sequence) {
                    return Err(DecisionContinuationError::DurableMismatch);
                }
                previous = Some(page.receipt.sequence);
            }
        }
        if continuation.is_activity_frame() {
            continuation.validate_authority()?;
        }
        Ok(continuation)
    }

    /// Reconstruct only the transport-neutral trade facts needed by the existing
    /// idempotent decision continuation. Ledger/classification/gate are not rerun.
    pub fn incoming_trade(&self) -> Result<IncomingTrade, DecisionContinuationError> {
        self.validate_authority()?;
        self.incoming_trade_unchecked()
    }

    fn incoming_trade_unchecked(&self) -> Result<IncomingTrade, DecisionContinuationError> {
        let observed_at = time::OffsetDateTime::from_unix_timestamp(self.facts.source_epoch)
            .map_err(|_| DecisionContinuationError::SourceEpoch(self.facts.source_epoch))?;
        Ok(IncomingTrade {
            wallet: self.facts.wallet,
            market_id: self.facts.market_id.clone(),
            outcome_id: self.facts.outcome_id,
            side: self.facts.side,
            price: self.facts.price,
            contracts: self.facts.share_amount,
            observed_at,
            received_at: if self.is_activity_frame() {
                serde_json::from_value::<crate::frame_admission::FrameDecisionProof>(
                    self.facts.decision_inputs.clone(),
                )?
                .inputs
                .received_at
            } else {
                observed_at
            },
            source_trade_id: self.facts.source_trade_id.clone(),
            transaction_hash: Some(self.facts.transaction_hash.clone()),
            provenance: self.facts.provenance,
        })
    }
}

/// Fail-closed boot/census error identifying the open row (when one was reached) and its cause.
#[derive(Debug, thiserror::Error)]
#[error("open decision continuation {}: {cause}", source_trade_id.as_ref().map_or("<scan>", |id| id.0.as_str()))]
pub struct ContinuationValidationError {
    pub source_trade_id: Option<SourceTradeId>,
    pub cause: String,
}

/// Validate every open continuation before any can resume, reconstructing each shared read once.
pub fn validate_open_continuations(
    paper_state: &PaperStateDb,
    source_receipts: &SourceReceiptIndex,
) -> Result<usize, ContinuationValidationError> {
    let rows =
        paper_state
            .open_decision_pending()
            .map_err(|error| ContinuationValidationError {
                source_trade_id: None,
                cause: error.to_string(),
            })?;
    validate_continuation_rows(paper_state, rows, source_receipts)
}

/// Validate open rows read earlier (for example before a log bound) against `source_receipts`.
pub(crate) fn validate_continuation_rows(
    paper_state: &PaperStateDb,
    rows: Vec<DecisionPendingRow>,
    source_receipts: &SourceReceiptIndex,
) -> Result<usize, ContinuationValidationError> {
    let validated = rows.len();
    let mut reads = Vec::<(DecisionContinuationV3, Vec<DecisionContinuationV3>)>::new();
    let mut page_reads = HashMap::<pe_core_types::EventSeq, usize>::new();
    for row in rows {
        let fail = |cause: String| ContinuationValidationError {
            source_trade_id: Some(row.source_trade_id.clone()),
            cause,
        };
        let continuation =
            DecisionContinuationV3::from_durable(&row).map_err(|error| fail(error.to_string()))?;
        if continuation.is_activity_frame() {
            let proof: crate::frame_admission::FrameDecisionProof =
                serde_json::from_value(continuation.facts.decision_inputs.clone())
                    .map_err(|error| fail(error.to_string()))?;
            #[cfg(feature = "scenario")]
            let before = source_receipts.read_verification_count(proof.inputs.frontier.commitment);
            continuation
                .verify_activity_frame_with_index(source_receipts)
                .map_err(|error| fail(error.to_string()))?;
            #[cfg(feature = "scenario")]
            if source_receipts.read_verification_count(proof.inputs.frontier.commitment) > before {
                source_receipts
                    .record_binding_verification_category(proof.inputs.frontier.commitment, 2);
            }
            proof
                .inputs
                .verify_durable(paper_state, &continuation.facts)
                .map_err(|error| fail(error.to_string()))?;
            continue;
        }
        continuation
            .require_complete_read()
            .map_err(|error| fail(error.to_string()))?;
        if continuation.page_occurrences().is_empty() {
            return Err(fail(
                "open decision continuation has no receipt-bearing activity read".to_owned(),
            ));
        }
        let shared_read = continuation
            .page_occurrences()
            .iter()
            .find_map(|page| page_reads.get(&page.receipt.sequence).copied());
        if let Some(index) = shared_read {
            let (existing, related) = &mut reads[index];
            if !continuation.same_complete_read(existing)
                || continuation.version() != existing.version()
            {
                return Err(fail(
                    "decision continuations disagree about one complete activity read".to_owned(),
                ));
            }
            related.push(continuation);
        } else {
            for page in continuation.page_occurrences() {
                page_reads.insert(page.receipt.sequence, reads.len());
            }
            reads.push((continuation, Vec::new()));
        }
    }
    for (continuation, related) in reads {
        #[cfg(feature = "scenario")]
        if let Some(receipt) = continuation.read_commitment {
            source_receipts.record_binding_verification_category(receipt, 2);
            source_receipts.record_read_verification(receipt);
        }
        let mut lookup = |receipt| {
            #[cfg(test)]
            continuation_validation_tests::LOOKUPS.with(|count| count.set(count.get() + 1));
            source_receipts
                .source_envelope(receipt)
                .map(|envelope| CompleteActivityPage {
                    payload: envelope.payload,
                    observed_at: envelope.observed_at,
                    received_at: envelope.received_at,
                    source_id: envelope.source_id.0,
                    schema_version: envelope.schema_version,
                    parser_version: envelope.parser_version,
                    content_type: envelope.content_type,
                })
        };
        let read = continuation
            .reconstruct_verified_activity_read(&mut lookup)
            .map_err(|error| ContinuationValidationError {
                source_trade_id: Some(continuation.facts.source_trade_id.clone()),
                cause: error.to_string(),
            })?;
        for continuation in std::iter::once(&continuation).chain(&related) {
            let facts = &continuation.facts;
            let fail = |cause: String| ContinuationValidationError {
                source_trade_id: Some(facts.source_trade_id.clone()),
                cause,
            };
            read.bindings
                .verify_facts(facts)
                .map_err(|error| fail(error.to_string()))?;
            if let Some(websocket) = continuation.observed_source_receipt {
                continuation
                    .verify_stream_binding_in_read(
                        &facts.source_trade_id,
                        websocket,
                        &read,
                        &mut lookup,
                    )
                    .map_err(|error| fail(error.to_string()))?;
            }
            let mut matching = read
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.group_id.key() == &facts.source_trade_id);
            let aggregate = matching.next().ok_or_else(|| {
                fail("complete activity read has no aggregate for the open decision".to_owned())
            })?;
            if matching.next().is_some() {
                return Err(fail(
                    "complete activity read repeats the open decision aggregate".to_owned(),
                ));
            }
            let applied = facts.durable_group_effect(paper_state).map_err(fail)?;
            crate::qualification::verify_decision_continuation_facts(
                aggregate,
                facts,
                applied.effect.correction(),
            )
            .map_err(|error| fail(error.to_string()))?;
        }
    }
    Ok(validated)
}

#[cfg(test)]
pub(crate) mod continuation_validation_tests {
    #![allow(clippy::unwrap_used)]

    use std::cell::Cell;

    use pe_event_log::{EnvelopeIn, Writer};
    use pe_source_polymarket_public::ActivityRequestBounds;
    use rust_decimal_macros::dec;

    use super::*;

    thread_local! {
        pub(super) static LOOKUPS: Cell<usize> = const { Cell::new(0) };
    }

    /// Engine-created open rows from two BUYs in one real, committed complete read.
    pub(crate) fn producer_fixture() -> (tempfile::TempDir, Arc<PaperStateDb>, SourceReceiptIndex) {
        let dir = tempfile::tempdir().unwrap();
        let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let epoch = 1_700_000_000;
        let fixed_end = epoch + 10;
        let observed_at =
            SourceTimestamp(time::OffsetDateTime::from_unix_timestamp(fixed_end).unwrap());
        let received_at =
            ReceivedAt(time::OffsetDateTime::from_unix_timestamp(fixed_end + 1).unwrap());
        let payload = serde_json::to_vec(&json!([
            {
                "proxyWallet": wallet.to_string(), "timestamp": epoch,
                "conditionId": "0xcondition-a", "type": "TRADE", "size": "2.000000",
                "usdcSize": "1.000000", "transactionHash": "0xtransaction-a", "price": "0.5",
                "asset": "asset-a", "side": "BUY", "outcomeIndex": 0, "outcome": "Yes",
                "isCombo": false
            },
            {
                "proxyWallet": wallet.to_string(), "timestamp": epoch,
                "conditionId": "0xcondition-b", "type": "TRADE", "size": "4.000000",
                "usdcSize": "2.000000", "transactionHash": "0xtransaction-b", "price": "0.5",
                "asset": "asset-b", "side": "BUY", "outcomeIndex": 0, "outcome": "Yes",
                "isCombo": false
            }
        ]))
        .unwrap();
        let source_id = SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned());
        let parsed = parse_activity_response(
            &payload,
            wallet,
            &ActivityParseContext {
                source_id: source_id.clone(),
                observed_at: observed_at.clone(),
                received_at: received_at.clone(),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap();
        let aggregates = parsed.aggregates().unwrap();
        assert_eq!(aggregates.len(), 2);
        let path = dir.path().join("source.log");
        let mut writer = Writer::open(&path).unwrap();
        let page_receipt = writer
            .append_synced(EnvelopeIn {
                source_id,
                schema_version: crate::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: observed_at.clone(),
                received_at: received_at.clone(),
                content_type: ContentType::Json,
                payload: payload.clone(),
            })
            .unwrap();
        let request_url = PolymarketEndpoint::UserPositionActivityPage {
            user: wallet.to_string(),
            end: fixed_end,
            start: None,
            offset: 0,
        }
        .url("https://data-api.polymarket.com");
        let raw_hash = blake3::hash(&payload).to_hex().to_string();
        let pages = vec![ReconciliationPageEvidence {
            request_url: request_url.clone(),
            bounds: Some(ActivityRequestBounds {
                start: None,
                end: fixed_end,
            }),
            partition: None,
            offset: 0,
            row_count: 2,
            canonical_page_hash: canonical_page_hash(&payload).unwrap(),
            raw_page_hash: raw_hash.clone(),
            received_at: received_at.clone(),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
        }];
        let occurrences = vec![PageOccurrence {
            request_url,
            raw_hash,
            receipt: page_receipt,
        }];
        let commitment = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_READ_COMMITMENT_V1_SCHEMA_VERSION,
                parser_version: ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
                observed_at,
                received_at,
                content_type: ContentType::Json,
                payload: activity_read_commitment_payload_v1(
                    wallet,
                    fixed_end,
                    &occurrences,
                    &pages,
                )
                .unwrap(),
            })
            .unwrap();
        drop(writer);
        paper_state.set_cursor(&wallet, 0).unwrap();
        paper_state
            .install_anchors(&[AnchorInstallRecord {
                repaired_history: Vec::new(),
                expected_fence: None,
                history_status: None,
                wallet,
                balances: Vec::new(),
                activity_cutoff_unix: 0,
                anchored_at_unix: 0,
                ledger_hash_after: "empty".to_owned(),
                positions_proof_hash: "empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "validation-fixture".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: 0,
            }])
            .unwrap();
        let context = BucketDecisionContext {
            verified_read: None,
            applied_configuration: synthetic_legacy17_runtime_config(),
            decision_inputs_json: json!({"fixed_end": fixed_end, "pages": pages}).to_string(),
            page_occurrences: occurrences,
            observed_source_receipts: HashMap::new(),
            read_commitment: Some(
                crate::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(commitment),
            ),
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            signal_config: SignalConfig::default(),
            copy_eligible: true,
            bracket_commit: false,
            recorded_at_unix: fixed_end + 1,
            observation_provenance: HashMap::new(),
            no_copy_dispositions: HashMap::new(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            restamp_twins: Default::default(),
            history_status: Some(WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{\"fixed_end_walk\":\"complete\"}".to_owned(),
                updated_at_unix: fixed_end + 1,
            }),
        };
        let result = BucketCommitEngine::load(paper_state.clone(), PositionLedger::new())
            .unwrap()
            .commit(
                aggregates,
                &context,
                FrozenDecisionBasis {
                    win_rate_p: Probability::new(dec!(0.7)).unwrap(),
                    bankroll: dec!(1000),
                },
            )
            .unwrap();
        assert_eq!(result.pending.len(), 2);
        assert!(result.newly_fenced.is_none());
        let index = SourceReceiptIndex::replay(&path).unwrap();
        (dir, paper_state, index)
    }

    /// PASS: real engine-created V4 rows validate; changing only one frozen price names that row.
    /// FAIL: valid rows fail, altered facts pass, the wrong identity is reported, or rows are repaired.
    #[test]
    fn validate_open_continuations_accepts_producer_shaped_rows_and_rejects_altered_facts() {
        let (dir, paper_state, index) = producer_fixture();
        assert_eq!(
            validate_open_continuations(&paper_state, &index).unwrap(),
            2
        );
        let before = paper_state.open_decision_pending().unwrap();
        let row = &before[1];
        let mut frozen: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
        frozen["price"] = serde_json::to_value(Price::new(dec!(0.6)).unwrap()).unwrap();
        let altered = frozen.to_string();
        let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        conn.execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![altered, row.source_trade_id.0],
        )
        .unwrap();
        let error = validate_open_continuations(&paper_state, &index).unwrap_err();
        assert_eq!(error.source_trade_id.as_ref(), Some(&row.source_trade_id));
        assert!(
            error
                .cause
                .contains("differs from its raw activity aggregate")
        );
        assert!(error.to_string().contains(&row.source_trade_id.0));
        assert_eq!(
            paper_state
                .decision_pending_for(&row.source_trade_id)
                .unwrap()
                .unwrap()
                .frozen_inputs_json,
            altered
        );
        assert_eq!(paper_state.open_decision_pending().unwrap().len(), 2);
    }

    /// PASS: two decisions sharing a read perform exactly one page lookup and one commitment lookup.
    /// FAIL: a shared read is reconstructed per row or an extra observation/page scan is performed.
    #[test]
    fn shared_read_is_reconstructed_once() {
        let (_dir, paper_state, index) = producer_fixture();
        LOOKUPS.with(|count| count.set(0));
        assert_eq!(
            validate_open_continuations(&paper_state, &index).unwrap(),
            2
        );
        assert_eq!(LOOKUPS.with(Cell::get), 2);
    }

    /// PASS: receipt-free open V2 rows and inconsistent descriptions of shared receipts are refused.
    /// FAIL: erased receipt evidence passes, shared-read disagreement passes, or the wrong row is named.
    #[test]
    fn validate_open_continuations_rejects_legacy_reads_and_disagreement() {
        let (dir, paper_state, index) = producer_fixture();
        let rows = paper_state.open_decision_pending().unwrap();
        let row = &rows[1];
        let original: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
        let mut legacy = original.clone();
        legacy["version"] = json!(2);
        pre_545_applied_configuration(&mut legacy);
        for key in [
            "page_occurrences",
            "observed_source_receipt",
            "read_commitment",
            "source_authority",
            "paper_freshness_policy",
        ] {
            legacy.as_object_mut().unwrap().remove(key);
        }
        let mut disagreement = original;
        disagreement["decision_inputs"]["fixed_end"] = json!(1_700_000_011);
        let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        for (altered, cause) in [
            (legacy, "no receipt-bearing activity read"),
            (
                disagreement,
                "decision continuations disagree about one complete activity read",
            ),
        ] {
            conn.execute(
                "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                rusqlite::params![altered.to_string(), row.source_trade_id.0],
            )
            .unwrap();
            let error = validate_open_continuations(&paper_state, &index).unwrap_err();
            assert_eq!(error.source_trade_id.as_ref(), Some(&row.source_trade_id));
            assert!(error.cause.contains(cause), "{error}");
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BucketCommitError {
    #[error("cannot commit an empty activity bucket")]
    Empty,
    #[error("activity bucket mixes wallets or epoch seconds")]
    MixedBucket,
    #[error("activity bucket is partially durable")]
    PartialDurableBucket,
    #[error("invalid decision input json: {0}")]
    DecisionInputs(#[from] serde_json::Error),
    #[error("activity ledger: {0}")]
    Ledger(#[from] LedgerError),
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("ledger effect document: {0}")]
    EffectDocument(#[from] LedgerEffectDocumentError),
    #[error("{0}")]
    Invariant(String),
}

#[derive(Debug, thiserror::Error)]
pub enum AnchorInstallError {
    #[error("wallet {wallet} is durably fenced")]
    Fenced { wallet: WalletAddress },
    #[error("wallet {wallet} ledger changed before anchor install")]
    LedgerHashChanged { wallet: WalletAddress },
    #[error("wallet {wallet} cursor changed before anchor install")]
    CursorChanged { wallet: WalletAddress },
    #[error("wallet {wallet} anchor sequence changed before anchor install")]
    AnchorSeqChanged { wallet: WalletAddress },
    #[error("wallet {wallet} coverage generation changed before anchor install")]
    CoverageGenerationChanged { wallet: WalletAddress },
    #[error("wallet {wallet} anchor cutoff regressed from {stored} to {candidate}")]
    CutoffRegression {
        wallet: WalletAddress,
        stored: i64,
        candidate: i64,
    },
    #[error("unsafe recorded activity prevents recovery for wallet {wallet}")]
    UnsafeRecovery { wallet: WalletAddress },
    #[error("anchor install durability failure: {0}")]
    Durability(String),
}

impl AnchorInstallError {
    pub fn class(&self) -> crate::position_seeder::FailureClass {
        use crate::position_seeder::FailureClass;
        match self {
            Self::UnsafeRecovery { .. } | Self::Fenced { .. } => FailureClass::WalletPersistent,
            Self::LedgerHashChanged { .. }
            | Self::CursorChanged { .. }
            | Self::AnchorSeqChanged { .. }
            | Self::CoverageGenerationChanged { .. }
            | Self::CutoffRegression { .. } => FailureClass::WalletTransient,
            Self::Durability(_) => FailureClass::Shared,
        }
    }

    pub fn wallet(&self) -> Option<WalletAddress> {
        match self {
            Self::UnsafeRecovery { wallet }
            | Self::Fenced { wallet }
            | Self::LedgerHashChanged { wallet }
            | Self::CursorChanged { wallet }
            | Self::AnchorSeqChanged { wallet }
            | Self::CoverageGenerationChanged { wallet }
            | Self::CutoffRegression { wallet, .. } => Some(*wallet),
            Self::Durability(_) => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Fenced { .. } => "anchor.fenced",
            Self::UnsafeRecovery { .. } => "anchor.unsafe_recovery",
            Self::LedgerHashChanged { .. } => "anchor.ledger_hash_changed",
            Self::CursorChanged { .. } => "anchor.cursor_changed",
            Self::AnchorSeqChanged { .. } => "anchor.anchor_seq_changed",
            Self::CoverageGenerationChanged { .. } => "anchor.coverage_generation_changed",
            Self::CutoffRegression { .. } => "anchor.cutoff_regression",
            Self::Durability(_) => "anchor.durability",
        }
    }
}

impl From<pe_paper_state::PaperStateError> for AnchorInstallError {
    fn from(error: pe_paper_state::PaperStateError) -> Self {
        Self::Durability(format!("paper-state: {error}"))
    }
}

pub(crate) struct FrameAdmissionContext {
    pub admitted_at: time::OffsetDateTime,
    pub stale_secs: i64,
    pub quality: ReconstructionQuality,
    pub signal_config: SignalConfig,
    pub copy_eligible: bool,
    pub configuration: RuntimeConfig,
    pub basis: FrozenDecisionBasis,
    pub latch: crate::frame_admission::FeedLatchBasis,
    pub paper_prefix: Option<AppendReceipt>,
    pub identity: Option<crate::frame_admission::FrameIdentityProof>,
    pub freshness_policy: PaperFreshnessPolicy,
}

pub(crate) enum FrameRoute {
    Ignored,
    Fallback(Box<crate::frame_admission::FrameFallbackArtifact>),
    Admission(
        Box<(
            crate::frame_admission::FrameAdmissionInputs,
            FrameAdmissionContext,
        )>,
    ),
}

/// Single runtime owner for the exact leader ledger, durable gate projection,
/// wallet fences, and decision admission.
pub struct BucketCommitEngine {
    paper_state: Arc<PaperStateDb>,
    ledger: PositionLedger,
    entry_gate: CopyEntryGate,
    complete_history: HashSet<WalletAddress>,
    fences: HashSet<WalletAddress>,
    earlier_frames: Vec<crate::frame_admission::EarlierFrame>,
    frame_decisions: HashMap<WalletAddress, Vec<pe_paper_state::ActivityFrameDecisionIndex>>,
    frame_transactions: HashMap<(WalletAddress, String), Vec<usize>>,
    admitted_frame_receipts:
        HashMap<(pe_core_types::EventSeq, blake3::Hash), (WalletAddress, usize)>,
    verified_frontiers: HashMap<WalletAddress, crate::frame_admission::FeedHistoryFrontier>,
    frontier_hints: HashMap<WalletAddress, crate::frame_admission::FeedHistoryFrontier>,
    routed_frame_receipts: HashSet<pe_core_types::EventSeq>,
    frame_source_index: Option<SourceReceiptIndex>,
}

impl BucketCommitEngine {
    /// Delete durable working state before dropping the corresponding runtime projections.
    pub fn retire_wallet(
        &mut self,
        wallet: WalletAddress,
        recent_since_unix: i64,
        latest_prepared: Option<pe_core_types::EventSeq>,
    ) -> Result<
        pe_paper_state::RetentionTransaction<Option<pe_paper_state::WalletRetentionWait>>,
        BucketCommitError,
    > {
        let retired = self
            .paper_state
            .retire_wallet(wallet, recent_since_unix, latest_prepared)?;
        if retired.result.is_none() {
            for frame in self
                .earlier_frames
                .iter()
                .filter(|frame| frame.wallet == wallet)
            {
                self.routed_frame_receipts.remove(&frame.receipt.sequence);
            }
            if let Some(frames) = self.frame_decisions.get(&wallet) {
                for frame in frames {
                    if let Some(receipt) = frame.observed_source_receipt {
                        self.routed_frame_receipts.remove(&receipt.sequence);
                    }
                }
            }
            let mut snapshots = self.ledger.snapshots().clone();
            snapshots.remove(&wallet);
            self.ledger = PositionLedger::from_snapshots(snapshots);
            self.complete_history.remove(&wallet);
            self.frame_decisions.remove(&wallet);
            self.frame_transactions
                .retain(|(owner, _), _| *owner != wallet);
            self.admitted_frame_receipts
                .retain(|_, (owner, _)| *owner != wallet);
            self.earlier_frames.retain(|frame| frame.wallet != wallet);
            self.verified_frontiers.remove(&wallet);
            self.frontier_hints.remove(&wallet);
            if let Some(index) = &self.frame_source_index {
                index.forget_verified_frontier(wallet);
            }
        }
        Ok(retired)
    }

    /// Load every durable decision boundary before producers start.
    pub fn load(
        paper_state: Arc<PaperStateDb>,
        ledger: PositionLedger,
    ) -> Result<Self, BucketCommitError> {
        let entry_gate = CopyEntryGate::new(CopyEntryGateConfig, paper_state.gate_history()?);
        let complete_history = paper_state.complete_history_wallets()?;
        let fences = paper_state
            .wallet_fences()?
            .into_iter()
            .map(|fence| fence.wallet)
            .collect();
        let frontier_hints: crate::frame_admission::FrontierCollection =
            serde_json::from_value(paper_state.feed_history_frontiers()?)
                .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        if frontier_hints.version != 1 {
            return Err(BucketCommitError::Invariant(
                "unsupported frontier collection".to_owned(),
            ));
        }
        let mut frame_decisions =
            HashMap::<WalletAddress, Vec<pe_paper_state::ActivityFrameDecisionIndex>>::new();
        for frame in paper_state.activity_frame_decision_index(None)? {
            frame_decisions.entry(frame.wallet).or_default().push(frame);
        }
        let mut frame_transactions: HashMap<(WalletAddress, String), Vec<usize>> = HashMap::new();
        let mut admitted_frame_receipts = HashMap::new();
        for (wallet, frames) in &frame_decisions {
            for (index, frame) in frames.iter().enumerate() {
                frame_transactions
                    .entry((*wallet, frame.transaction_hash.clone()))
                    .or_default()
                    .push(index);
                if let Some(receipt) = frame.observed_source_receipt {
                    admitted_frame_receipts
                        .insert((receipt.sequence, receipt.this_hash), (*wallet, index));
                }
            }
        }
        Ok(Self {
            paper_state,
            ledger,
            entry_gate,
            complete_history,
            fences,
            earlier_frames: Vec::new(),
            frame_decisions,
            frame_transactions,
            admitted_frame_receipts,
            verified_frontiers: HashMap::new(),
            frontier_hints: frontier_hints
                .frontiers
                .into_iter()
                .map(|frontier| (frontier.wallet, frontier))
                .collect(),
            routed_frame_receipts: HashSet::new(),
            frame_source_index: None,
        })
    }

    /// Select only transactions present in this bucket; leg discovery still uses the whole read.
    fn frames_for_aggregates(
        &self,
        aggregates: &[ActivityAggregate],
    ) -> Vec<&pe_paper_state::ActivityFrameDecisionIndex> {
        let transactions = aggregates
            .iter()
            .map(|aggregate| {
                let components = aggregate.group_id.components();
                (components.wallet, components.transaction_hash.clone())
            })
            .collect::<HashSet<_>>();
        let mut frames = transactions
            .into_iter()
            .flat_map(|key| {
                self.frame_transactions
                    .get(&key)
                    .into_iter()
                    .flatten()
                    .filter_map(move |index| {
                        self.frame_decisions
                            .get(&key.0)
                            .and_then(|frames| frames.get(*index))
                    })
            })
            .collect::<Vec<_>>();
        frames.sort_by(|left, right| {
            left.source_epoch
                .cmp(&right.source_epoch)
                .then_with(|| left.source_trade_id.0.cmp(&right.source_trade_id.0))
        });
        frames
    }

    /// Install the authenticated receipt owner used to discover durable frame counterparts.
    #[must_use]
    pub fn with_source_receipt_index(mut self, index: SourceReceiptIndex) -> Self {
        self.set_source_receipt_index(index);
        self
    }

    pub(crate) fn set_source_receipt_index(&mut self, index: SourceReceiptIndex) {
        self.frame_source_index = Some(index);
    }

    fn observe_frame(&mut self, incoming: crate::frame_admission::EarlierFrame) {
        let admitted = |receipt: AppendReceipt| {
            self.admitted_frame_receipts
                .contains_key(&(receipt.sequence, receipt.this_hash))
        };
        if let Some(existing) = self.earlier_frames.iter_mut().find(|frame| {
            frame.wallet == incoming.wallet && frame.source_trade_id == incoming.source_trade_id
        }) {
            if crate::frame_admission::prefer_observation(
                existing.receipt,
                admitted(existing.receipt),
                existing.unresolved_buy,
                incoming.receipt,
                admitted(incoming.receipt),
                incoming.unresolved_buy,
            ) {
                *existing = incoming;
            }
        } else {
            self.earlier_frames.push(incoming);
        }
    }

    #[cfg(feature = "scenario")]
    pub(crate) fn unresolved_receipts(&self, wallet: WalletAddress) -> Vec<AppendReceipt> {
        let mut receipts = self
            .earlier_frames
            .iter()
            .filter(|frame| frame.wallet == wallet)
            .map(|frame| frame.receipt)
            .collect::<Vec<_>>();
        receipts.sort_by_key(|receipt| receipt.sequence);
        receipts
    }

    fn retire_observation_barrier(&mut self, receipt: AppendReceipt) {
        self.earlier_frames.retain(|frame| frame.receipt != receipt);
    }

    pub(crate) fn retire_observation(
        &mut self,
        receipt: AppendReceipt,
        source_trade_id: &SourceTradeId,
        unbound: bool,
        read: Option<&VerifiedCommitment>,
    ) -> Result<crate::orchestrator_control::ReconciliationAcknowledgement, String> {
        use crate::orchestrator_control::ReconciliationAcknowledgement;
        if let Some(frame) = self
            .paper_state
            .activity_frame_decision(source_trade_id)
            .map_err(|error| error.to_string())?
        {
            return Ok(if frame.observed_source_receipt == Some(receipt) {
                ReconciliationAcknowledgement::Applied
            } else {
                ReconciliationAcknowledgement::Superseded
            });
        }
        let index = self
            .frame_source_index
            .as_ref()
            .ok_or_else(|| "frame source index absent".to_owned())?;
        if read.is_some_and(|read| read.retention_epoch != Some(index.retention_epoch())) {
            return Err("retention epoch changed before observation retirement".to_owned());
        }
        let source = index
            .source_envelope(receipt)
            .map_err(|error| error.to_string())?;
        let observation =
            parse_activity_trade_observation(&source.payload).map_err(|error| error.to_string())?;
        let mut bound_disposition = false;
        if let Some(read) = read
            && let Some(binding) = read
                .binding_indices
                .get(&(receipt.sequence, receipt.this_hash))
                .and_then(|index| read.bindings.get(*index))
            && &binding.stream_group_id == source_trade_id
        {
            bound_disposition = self
                .paper_state
                .activity_revision_disposed(&binding.history_group_id, &binding.semantic_revision)
                .map_err(|error| error.to_string())?;
        }
        if observation.group_id.key() != source_trade_id
            || (!self
                .paper_state
                .is_wallet_fenced(&observation.wallet)
                .map_err(|error| error.to_string())?
                && self
                    .paper_state
                    .activity_group_state(source_trade_id)
                    .map_err(|error| error.to_string())?
                    .is_none()
                && !bound_disposition
                && !self
                    .paper_state
                    .activity_observation_retired(receipt)
                    .map_err(|error| error.to_string())?)
        {
            return Err("ordinary observation retirement lacks durable disposition".to_owned());
        }
        // An observation without its own disposed group keeps exact retirement authority.
        if !self
            .paper_state
            .activity_observation_retired(receipt)
            .map_err(|error| error.to_string())?
            && self
                .paper_state
                .activity_group_state(source_trade_id)
                .map_err(|error| error.to_string())?
                .is_none()
        {
            self.paper_state
                .retire_activity_observation(
                    receipt,
                    unbound
                        && read.is_none_or(|read| {
                            !read
                                .binding_indices
                                .contains_key(&(receipt.sequence, receipt.this_hash))
                        }),
                )
                .map_err(|error| error.to_string())?;
        }
        self.retire_observation_barrier(receipt);
        Ok(ReconciliationAcknowledgement::Applied)
    }

    pub(crate) fn restore_frame_prefix(
        &mut self,
        receipts: &[AppendReceipt],
        undelivered: &[AppendReceipt],
        index: &SourceReceiptIndex,
    ) -> Result<(), String> {
        self.frame_source_index = Some(index.clone());
        self.routed_frame_receipts = receipts
            .iter()
            .filter(|receipt| !undelivered.contains(receipt))
            .map(|receipt| receipt.sequence)
            .collect();
        self.earlier_frames.clear();
        for receipt in receipts {
            let source = index
                .source_envelope(*receipt)
                .map_err(|error| error.to_string())?;
            let observation = parse_activity_trade_observation(&source.payload)
                .map_err(|error| error.to_string())?;
            let parts = observation.group_id.components();
            let market = MarketId(pe_core_types::VenueMarketId(
                parts
                    .condition_id
                    .as_ref()
                    .ok_or_else(|| "frame condition missing".to_owned())?
                    .0
                    .clone(),
            ));
            let admitted = self
                .paper_state
                .activity_frame_decision(observation.group_id.key())
                .map_err(|error| error.to_string())?
                .is_some();
            if admitted
                || (self
                    .paper_state
                    .activity_observation_retired(*receipt)
                    .map_err(|error| error.to_string())?
                    || self
                        .paper_state
                        .activity_group_state(observation.group_id.key())
                        .map_err(|error| error.to_string())?
                        .is_some())
            {
                continue;
            }
            self.observe_frame(crate::frame_admission::EarlierFrame {
                receipt: *receipt,
                wallet: observation.wallet,
                source_trade_id: observation.group_id.key().clone(),
                market,
                received_at: source.received_at.0,
                unresolved_buy: parts.side == Some(Side::Buy)
                    && observation.share_amount != ShareAmount::ZERO
                    && !observation.is_combo,
            });
        }
        Ok(())
    }

    /// Publish only a verified, contiguous completed read, after all bucket acknowledgements.
    pub(crate) fn publish_frontier(
        &mut self,
        frontier: crate::frame_admission::FeedHistoryFrontier,
        index: &SourceReceiptIndex,
        read: Option<&VerifiedCommitment>,
    ) -> Result<(), String> {
        let epoch = index.retention_epoch();
        if let Some(read) = read
            && read.frontier.as_ref() != Some(&frontier)
        {
            return Err("frontier differs from authenticated read".to_owned());
        }
        if read.is_none_or(|read| read.retention_epoch != Some(epoch)) {
            index
                .verify_frame_frontier(&frontier)
                .map_err(|error| error.to_string())?;
        }
        // Admissions may have committed while the poller awaited persistence/acknowledgements.
        // Recheck the serialized owner's current barrier immediately before publishing H.
        for frame in self
            .earlier_frames
            .iter()
            .filter(|frame| frame.wallet == frontier.wallet)
        {
            let source = index
                .source_envelope(frame.receipt)
                .map_err(|error| error.to_string())?;
            let observation = parse_activity_trade_observation(&source.payload)
                .map_err(|error| error.to_string())?;
            if observation.source_time.0.unix_timestamp() <= frontier.fixed_end {
                return Ok(());
            }
        }
        let mut frontiers = self.verified_frontiers.clone();
        if let Some(previous) = frontiers.get(&frontier.wallet) {
            if frontier.fixed_end < previous.fixed_end
                || frontier.commitment.sequence <= previous.commitment.sequence
            {
                return Ok(());
            }
            if frontier
                .pages
                .iter()
                .filter_map(|page| page.bounds.and_then(|bounds| bounds.start))
                .min()
                .is_some_and(|start| start > previous.fixed_end)
            {
                return Err("frontier read skips the previous fixed end".to_owned());
            }
        }
        index
            .remember_verified_frontier(&frontier, epoch)
            .map_err(|error| error.to_string())?;
        frontiers.insert(frontier.wallet, frontier);
        let mut hints = self.frontier_hints.clone();
        hints.extend(frontiers.clone());
        crate::frame_admission::persist_frontiers(&self.paper_state, &hints)
            .map_err(|error| error.to_string())?;
        self.verified_frontiers = frontiers;
        Ok(())
    }

    /// Capture admission state under the orchestrator's structural writer lock.
    pub(crate) fn prepare_activity_frame(
        &mut self,
        receipt: AppendReceipt,
        index: &SourceReceiptIndex,
        context: FrameAdmissionContext,
    ) -> Result<FrameRoute, BucketCommitError> {
        self.frame_source_index = Some(index.clone());
        use crate::frame_admission::*;
        if self.routed_frame_receipts.contains(&receipt.sequence) {
            return Ok(FrameRoute::Ignored);
        }
        if self.paper_state.activity_observation_retired(receipt)? {
            self.retire_observation_barrier(receipt);
            self.routed_frame_receipts.insert(receipt.sequence);
            return Ok(FrameRoute::Ignored);
        }
        let source = index
            .source_envelope(receipt)
            .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        let observation = parse_activity_trade_observation(&source.payload)
            .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        let parts = observation.group_id.components();
        if source.source_id.0 != crate::activity_ingest::ACTIVITY_WS_SOURCE_ID
            || source.schema_version != ACTIVITY_SCHEMA_VERSION
            || source.parser_version != ACTIVITY_PARSER_VERSION
            || source.content_type != ContentType::Json
        {
            return Err(BucketCommitError::Invariant(
                "invalid frame source contract".to_owned(),
            ));
        }
        let condition = parts
            .condition_id
            .as_ref()
            .ok_or_else(|| BucketCommitError::Invariant("frame condition absent".to_owned()))?;
        let market = MarketId(pe_core_types::VenueMarketId(condition.0.clone()));
        let candidate_shaped = parts.side == Some(Side::Buy)
            && observation.share_amount != ShareAmount::ZERO
            && !observation.is_combo;
        let earlier_identity = self.earlier_frames.iter().find(|frame| {
            frame.source_trade_id == *observation.group_id.key()
                && frame.receipt.sequence < receipt.sequence
                && frame.unresolved_buy
        });
        // Preserve the guard's short circuit: an earlier receipt needs no continuation read.
        let continuation = if earlier_identity.is_none() {
            self.paper_state
                .decision_pending_for(observation.group_id.key())?
        } else {
            None
        };
        if earlier_identity.is_some() || continuation.is_some() {
            if candidate_shaped {
                let earlier_hash =
                    earlier_identity.map(|frame| frame.receipt.this_hash.to_hex().to_string());
                tracing::info!(
                    receipt_sequence = receipt.sequence.0,
                    receipt_hash = %receipt.this_hash,
                    wallet = %observation.wallet,
                    market = %market,
                    outcome = parts.outcome.map(|outcome| outcome.0),
                    source_trade_id = %observation.group_id.key(),
                    reason = "identity_seen",
                    earlier_receipt_sequence = earlier_identity.map(|frame| frame.receipt.sequence.0),
                    earlier_receipt_hash = earlier_hash.as_deref(),
                    continuation_source_trade_id = continuation.as_ref().map(|row| row.source_trade_id.0.as_str()),
                    continuation_semantic_revision = continuation.as_ref().map(|row| row.semantic_revision.as_str()),
                    "frame admission ignored"
                );
            }
            return Ok(FrameRoute::Ignored);
        }
        // Any durable REST disposition owns this identity, including raw-only refusals.
        if let Some(group) = self
            .paper_state
            .activity_group_state(observation.group_id.key())?
        {
            if candidate_shaped {
                tracing::info!(
                    receipt_sequence = receipt.sequence.0,
                    receipt_hash = %receipt.this_hash,
                    wallet = %observation.wallet,
                    market = %market,
                    outcome = parts.outcome.map(|outcome| outcome.0),
                    source_trade_id = %observation.group_id.key(),
                    reason = "rest_owned",
                    semantic_revision = %group.semantic_revision,
                    disposition = %group.disposition,
                    "frame admission ignored"
                );
            }
            self.routed_frame_receipts.insert(receipt.sequence);
            return Ok(FrameRoute::Ignored);
        }
        // REST winning first already consumed history, before this admission capture.
        let market_history = self
            .paper_state
            .market_history_record(&observation.wallet, &market)?;
        let market_consumed = market_history.is_some();
        let incoming = IncomingTrade {
            wallet: observation.wallet,
            market_id: market.clone(),
            outcome_id: parts.outcome.ok_or(BucketCommitError::Empty)?,
            side: parts.side.ok_or(BucketCommitError::Empty)?,
            price: observation.price,
            contracts: observation.share_amount,
            observed_at: observation.source_time.0,
            received_at: source.received_at.0,
            source_trade_id: observation.group_id.key().clone(),
            transaction_hash: Some(parts.transaction_hash.clone()),
            provenance: TradeProvenance::ActivityWs,
        };
        let position = self.ledger.position(&observation.wallet);
        let action = pe_copy_signal_engine::classify_leader_action(
            &incoming,
            position,
            context.quality,
            &context.signal_config,
        );
        let balance = position
            .and_then(|snapshot| {
                snapshot
                    .positions
                    .get(&MarketOutcomeId::new(market.clone(), incoming.outcome_id))
            })
            .copied()
            .unwrap_or_default();
        let entry_market_consumed = self.entry_gate.has_market(&observation.wallet, &market);
        let earlier = self
            .earlier_frames
            .iter()
            .filter(|frame| {
                frame.wallet == observation.wallet && frame.receipt.sequence < receipt.sequence
            })
            .cloned()
            .collect::<Vec<_>>();
        let qualifying = incoming.side == Side::Buy
            && observation.share_amount != ShareAmount::ZERO
            && !observation.is_combo
            && action == LeaderAction::Entry
            && !entry_market_consumed
            && context.copy_eligible;
        let unresolved = incoming.side == Side::Buy
            && observation.share_amount != ShareAmount::ZERO
            && !observation.is_combo;
        self.observe_frame(EarlierFrame {
            receipt,
            wallet: observation.wallet,
            source_trade_id: observation.group_id.key().clone(),
            market: market.clone(),
            received_at: source.received_at.0,
            unresolved_buy: unresolved,
        });
        self.routed_frame_receipts.insert(receipt.sequence);
        if !qualifying {
            if candidate_shaped {
                let reason = if entry_market_consumed {
                    "market_consumed"
                } else if action != LeaderAction::Entry {
                    "not_entry"
                } else {
                    "not_copy_eligible"
                };
                tracing::info!(
                    receipt_sequence = receipt.sequence.0,
                    receipt_hash = %receipt.this_hash,
                    wallet = %observation.wallet,
                    market = %market,
                    outcome = incoming.outcome_id.0,
                    source_trade_id = %incoming.source_trade_id,
                    reason,
                    consuming_source_trade_id = market_history.as_ref().map(|row| row.source_trade_id.0.as_str()),
                    first_epoch = market_history.as_ref().map(|row| row.first_epoch),
                    action = ?action,
                    balance = %balance.long_contracts,
                    short_balance = %balance.short_contracts,
                    "frame admission ignored"
                );
            }
            return Ok(FrameRoute::Ignored);
        }
        let frontier = self.verified_frontiers.get(&observation.wallet).cloned();
        let coverage = self.paper_state.wallet_coverage(&observation.wallet)?;
        let wallet_ready = self.entry_gate.has_wallet(&observation.wallet)
            && self.complete_history.contains(&observation.wallet)
            && !self.fences.contains(&observation.wallet)
            && !coverage.reanchor_required;
        let reason = if context.identity.is_none() {
            Some(FrameFallbackReason::IdentityUnverified)
        } else if !wallet_ready {
            Some(FrameFallbackReason::WalletNotReady)
        } else if frame_prefix_blocks(&earlier, observation.wallet, &market) {
            Some(FrameFallbackReason::EarlierUnresolvedBuy)
        } else if frontier.as_ref().is_none_or(|frontier| {
            !frontier.current(
                source.received_at.0,
                context.admitted_at,
                context.stale_secs,
                earlier
                    .iter()
                    .filter(|frame| frame.wallet == observation.wallet)
                    .map(|frame| frame.received_at)
                    .min(),
            )
        }) {
            Some(FrameFallbackReason::HistoryBehind)
        } else if context
            .freshness_policy
            .expired(observation.source_time.clone(), context.admitted_at)
        {
            Some(FrameFallbackReason::CopyExpired)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Ok(FrameRoute::Fallback(Box::new(FrameFallbackArtifact {
                version: 1,
                frame_receipt: receipt,
                routing_clock: context.admitted_at,
                reason,
                frontier,
                latest_incident_basis: context.latch,
            })));
        }
        let frontier = frontier.ok_or_else(|| {
            BucketCommitError::Invariant("admitted frame has no frontier".to_owned())
        })?;
        let ledger_capture = ledger_capture(&self.ledger, &self.paper_state, observation.wallet)
            .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        let ledger_anchor = coverage
            .anchor_seq
            .map(|sequence| {
                self.paper_state
                    .position_anchor(&observation.wallet, sequence)
            })
            .transpose()?
            .flatten();
        let anchor_balances = match ledger_anchor {
            Some(anchor) => {
                serde_json::from_str::<Vec<(String, u16, ShareAmount)>>(&anchor.balances_json)?
                    .into_iter()
                    .filter(|(id, _, _)| id == &market.to_string())
                    .map(|(_, outcome, amount)| (outcome, amount))
                    .collect()
            }
            None => Vec::new(),
        };
        let ledger_group_boundary = self
            .paper_state
            .activity_group_boundary(&observation.wallet)?;
        let mut ledger_groups = Vec::new();
        for group in self.paper_state.activity_groups_at_boundary(
            &observation.wallet,
            coverage.activity_cutoff_unix.unwrap_or(i64::MIN),
            ledger_group_boundary,
        )? {
            if crate::frame_admission::group_affects_market(&group, &market)
                .map_err(|error| BucketCommitError::Invariant(error.to_string()))?
            {
                ledger_groups.push(group);
            }
        }
        Ok(FrameRoute::Admission(Box::new((
            FrameAdmissionInputs {
                version: 2,
                frame_receipt: receipt,
                admitted_at: context.admitted_at,
                received_at: source.received_at.0,
                source_time: observation.source_time.0,
                ledger_capture,
                ledger_group_boundary,
                anchor_balances,
                ledger_groups,
                market_consumed,
                earlier_frames: earlier,
                copy_eligible: context.copy_eligible,
                history_complete: wallet_ready,
                fenced: false,
                coverage,
                frontier,
                poll_round_stale_secs: context.stale_secs,
                latch: context.latch.clone(),
                paper_prefix: context.paper_prefix,
                identity: context.identity.clone(),
            },
            context,
        ))))
    }

    pub(crate) fn commit_activity_frame(
        &mut self,
        proof: crate::frame_admission::FrameDecisionProof,
        policy: PaperFreshnessPolicy,
        context: FrameAdmissionContext,
        index: &SourceReceiptIndex,
    ) -> Result<SourceTradeId, BucketCommitError> {
        let source = index
            .source_envelope(proof.inputs.frame_receipt)
            .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        let observation = parse_activity_trade_observation(&source.payload)
            .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        let parts = observation.group_id.components();
        let id = observation.group_id.key().clone();
        let market = MarketId(pe_core_types::VenueMarketId(
            parts
                .condition_id
                .as_ref()
                .ok_or(BucketCommitError::Empty)?
                .0
                .clone(),
        ));
        let revision = crate::frame_admission::frame_revision(&proof.inputs)?;
        let facts = DecisionContinuationFacts {
            paper_freshness_policy: Some(policy),
            source_trade_id: id.clone(),
            semantic_revision: revision.clone(),
            transaction_hash: parts.transaction_hash.clone(),
            wallet: observation.wallet,
            source_epoch: observation.source_time.0.unix_timestamp(),
            market_id: market.clone(),
            outcome_id: parts.outcome.ok_or(BucketCommitError::Empty)?,
            side: parts.side.ok_or(BucketCommitError::Empty)?,
            price: observation.price,
            share_amount: observation.share_amount,
            provenance: TradeProvenance::ActivityWs,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: context.quality,
            action_confidence_ppm: ProbabilityPpm(u32::from(context.quality.get()) * 10_000),
            gate_result: "admitted".to_owned(),
            applied_configuration_hash: context.configuration.canonical_hash(),
            applied_configuration: context.configuration.clone(),
            frozen_basis: context.basis,
            decision_inputs: serde_json::to_value(&proof)?,
        };
        let continuation = DecisionContinuationV3 {
            version: 7,
            source_authority: Some(SourceAuthority::ActivityFrame),
            facts,
            observed_source_receipt: Some(proof.inputs.frame_receipt),
            page_occurrences: Vec::new(),
            read_commitment: None,
        };
        continuation
            .verify_activity_frame_with_index(index)
            .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
        self.paper_state
            .commit_activity_frame(&pe_paper_state::ActivityFrameCommit {
                gate: EntryGateResultRecord {
                    source_trade_id: id.clone(),
                    wallet: observation.wallet,
                    market_id: market.clone(),
                    source_epoch: observation.source_time.0.unix_timestamp(),
                    result: "admitted".to_owned(),
                    history_consumed: true,
                },
                history: MarketHistoryRecord {
                    wallet: observation.wallet,
                    market_id: market.clone(),
                    first_epoch: observation.source_time.0.unix_timestamp(),
                    source_trade_id: id.clone(),
                },
                pending: DecisionPendingRecord {
                    source_trade_id: id.clone(),
                    semantic_revision: revision,
                    wallet: observation.wallet,
                    source_epoch: observation.source_time.0.unix_timestamp(),
                    frozen_inputs_json: serde_json::to_string(&continuation)?,
                    updated_at_unix: proof.inputs.admitted_at.unix_timestamp(),
                },
            })?;
        self.entry_gate.record_entry(observation.wallet, &market);
        let frames = self.frame_decisions.entry(observation.wallet).or_default();
        self.frame_transactions
            .entry((
                observation.wallet,
                continuation.facts.transaction_hash.clone(),
            ))
            .or_default()
            .push(frames.len());
        self.admitted_frame_receipts.insert(
            (
                proof.inputs.frame_receipt.sequence,
                proof.inputs.frame_receipt.this_hash,
            ),
            (observation.wallet, frames.len()),
        );
        frames.push(pe_paper_state::ActivityFrameDecisionIndex {
            version: 7,
            semantic_revision: continuation.facts.semantic_revision,
            source_trade_id: id.clone(),
            wallet: observation.wallet,
            source_epoch: continuation.facts.source_epoch,
            transaction_hash: continuation.facts.transaction_hash,
            market_id: market,
            outcome_id: continuation.facts.outcome_id,
            observed_source_receipt: continuation.observed_source_receipt,
            admission_receipt: proof.admission_receipt,
            received_at: proof.inputs.received_at,
            copy_latency_budget_secs: policy.copy_latency_budget_secs,
        });
        self.earlier_frames
            .retain(|frame| frame.source_trade_id != id);
        Ok(id)
    }

    #[must_use]
    pub fn ledger(&self) -> &PositionLedger {
        &self.ledger
    }

    /// Move the validated boot ledger into the runtime orchestrator owner.
    #[must_use]
    pub fn into_ledger(self) -> PositionLedger {
        self.ledger
    }

    pub(crate) fn ledger_mut(&mut self) -> &mut PositionLedger {
        &mut self.ledger
    }

    pub(crate) fn entry_gate(&self) -> &CopyEntryGate {
        &self.entry_gate
    }

    pub(crate) fn entry_gate_mut(&mut self) -> &mut CopyEntryGate {
        &mut self.entry_gate
    }

    #[must_use]
    pub fn is_fenced(&self, wallet: &WalletAddress) -> bool {
        self.fences.contains(wallet)
    }

    #[must_use]
    pub fn history_complete(&self, wallet: &WalletAddress) -> bool {
        self.complete_history.contains(wallet)
    }

    /// Commit several activity buckets as one durable paper-state batch.
    ///
    /// Connection ownership makes this safe: the only production caller is the boot bracket's
    /// `commit_direct`, under the engine lock before producers start; `begin_batch` therefore has
    /// that single production caller and batches never nest. Identity-cache inserts, corrupt-row
    /// deletions and condition rejection markers commit in separate transactions: their writer
    /// checks autocommit under the connection mutex, releases it and asynchronously waits while
    /// this batch is open. They cannot join a batch because bracket rollback must not erase an
    /// acknowledged identity or rejection. `mark_seeded_history_validated` runs after every bracket
    /// completes. A failed `ROLLBACK` surfaces as the bracket error, and the next `BEGIN IMMEDIATE`
    /// then fails, so boot fails closed instead of committing partial state.
    pub fn commit_batch<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, BucketCommitError>,
    ) -> Result<T, BucketCommitError> {
        let ledger = self.ledger.clone();
        let entry_gate = self.entry_gate.clone();
        let complete_history = self.complete_history.clone();
        let fences = self.fences.clone();
        self.paper_state.begin_batch()?;
        let result = f(self).and_then(|value| {
            self.paper_state.commit_batch()?;
            Ok(value)
        });
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let rollback = self.paper_state.rollback_batch();
                self.ledger = ledger;
                self.entry_gate = entry_gate;
                self.complete_history = complete_history;
                self.fences = fences;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(rollback_error.into()),
                }
            }
        }
    }

    /// Compare-and-swap one complete anchor batch, then publish the prebuilt
    /// in-memory ledger only after the durable transaction commits.
    pub fn install_anchors(
        &mut self,
        installs: &[AnchorInstall],
    ) -> Result<(), AnchorInstallError> {
        let mut wallets = HashSet::new();
        for install in installs {
            if !wallets.insert(install.wallet) {
                return Err(AnchorInstallError::Durability(format!(
                    "anchor ledger proof for {}: duplicate wallet in anchor batch",
                    install.wallet
                )));
            }
            if self.paper_state.wallet_fence(&install.wallet)? != install.expected_fence {
                return Err(AnchorInstallError::Fenced {
                    wallet: install.wallet,
                });
            }
            let capture = ledger_capture(&self.ledger, &self.paper_state, install.wallet).map_err(
                |error| {
                    AnchorInstallError::Durability(format!(
                        "anchor ledger proof for {}: {error}",
                        install.wallet
                    ))
                },
            )?;
            if capture.hash != install.expected.ledger_hash {
                return Err(AnchorInstallError::LedgerHashChanged {
                    wallet: install.wallet,
                });
            }
            if capture.cursor != install.expected.cursor {
                return Err(AnchorInstallError::CursorChanged {
                    wallet: install.wallet,
                });
            }
            if capture.anchor_seq != install.expected.anchor_seq {
                return Err(AnchorInstallError::AnchorSeqChanged {
                    wallet: install.wallet,
                });
            }
            if capture.coverage_generation != install.expected.coverage_generation {
                return Err(AnchorInstallError::CoverageGenerationChanged {
                    wallet: install.wallet,
                });
            }
            // Clearance must cover every recorded revision so its exact retry stays
            // recognizable after the wallet is unfenced, including after UTC rollback.
            if install.expected_fence.is_some()
                && let Some(stored) = capture.cursor
                && stored > install.cutoff
            {
                return Err(AnchorInstallError::CutoffRegression {
                    wallet: install.wallet,
                    stored,
                    candidate: install.cutoff,
                });
            }
            let coverage = self.paper_state.wallet_coverage(&install.wallet)?;
            if let Some(stored) = coverage.activity_cutoff_unix
                && stored > install.cutoff
            {
                return Err(AnchorInstallError::CutoffRegression {
                    wallet: install.wallet,
                    stored,
                    candidate: install.cutoff,
                });
            }
        }

        let mut candidate = self.ledger.clone();
        let mut records = Vec::with_capacity(installs.len());
        for install in installs {
            let mut repaired_history = BTreeMap::<String, MarketHistoryRecord>::new();
            let mut proof_json = install.proof.document.clone();
            let mut history_status = install.history_status.clone();
            if let Some(fence) = &install.expected_fence {
                if !crate::position_seeder::recoverable_fence(&self.paper_state, fence)?
                    || crate::position_seeder::fence_epoch(fence)
                        .is_none_or(|epoch| install.cutoff <= epoch)
                {
                    return Err(AnchorInstallError::Fenced {
                        wallet: install.wallet,
                    });
                }
                let mut history = install.fresh_history.clone();
                for row in self
                    .paper_state
                    .recovery_activity_evidence(&install.wallet, install.cutoff)?
                {
                    let applied = AppliedEffect::from_document(&row.proof_json)
                        .map_err(|error| AnchorInstallError::Durability(error.to_string()))?;
                    match applied.effect.effective() {
                        LedgerEffect::Conversion | LedgerEffect::UnknownEffect => {
                            return Err(AnchorInstallError::UnsafeRecovery {
                                wallet: install.wallet,
                            });
                        }
                        LedgerEffect::Trade {
                            market_id,
                            side: Side::Buy,
                            ..
                        } => history.push(MarketHistoryRecord {
                            wallet: install.wallet,
                            market_id: market_id.clone(),
                            first_epoch: row.source_epoch,
                            source_trade_id: row.source_trade_id,
                        }),
                        _ => {}
                    }
                }
                for record in history {
                    let entry = repaired_history
                        .entry(record.market_id.to_string())
                        .or_insert_with(|| record.clone());
                    if (record.first_epoch, &record.source_trade_id.0)
                        < (entry.first_epoch, &entry.source_trade_id.0)
                    {
                        *entry = record;
                    }
                }
                let mut proof: Value = serde_json::from_str(&proof_json)
                    .map_err(|error| AnchorInstallError::Durability(error.to_string()))?;
                proof["cleared_fence"] = serde_json::to_value(fence)
                    .map_err(|error| AnchorInstallError::Durability(error.to_string()))?;
                proof_json = serde_json::to_string(&proof)
                    .map_err(|error| AnchorInstallError::Durability(error.to_string()))?;
                if let Some(status) = history_status.as_mut() {
                    status.proof_json = proof_json.clone();
                }
            }
            let mut positions = HashMap::new();
            for (market_id, outcome_id, amount) in &install.balances {
                let key = MarketOutcomeId::new(market_id.clone(), *outcome_id);
                if positions
                    .insert(
                        key,
                        PositionState {
                            long_contracts: *amount,
                            short_contracts: ShareAmount::ZERO,
                        },
                    )
                    .is_some()
                {
                    return Err(AnchorInstallError::Durability(format!(
                        "anchor ledger proof for {}: duplicate anchored balance for {market_id} outcome {}",
                        install.wallet, outcome_id.0
                    )));
                }
            }
            candidate.replace_wallet_snapshot(install.wallet, positions);
            let post =
                ledger_capture(&candidate, &self.paper_state, install.wallet).map_err(|error| {
                    AnchorInstallError::Durability(format!(
                        "anchor ledger proof for {}: {error}",
                        install.wallet
                    ))
                })?;
            records.push(AnchorInstallRecord {
                repaired_history: repaired_history.into_values().collect(),
                expected_fence: install.expected_fence.clone(),
                history_status,
                wallet: install.wallet,
                balances: install.balances.clone(),
                activity_cutoff_unix: install.cutoff,
                anchored_at_unix: install.proof.recorded_at_unix,
                ledger_hash_after: post.hash,
                positions_proof_hash: install.proof.positions_proof_hash.clone(),
                activity_bounds_json: install.proof.activity_bounds_json.clone(),
                source_log_generation: install.proof.source_log_generation.clone(),
                proof_json,
                recorded_at_unix: install.proof.recorded_at_unix,
            });
        }
        self.paper_state.install_anchors(&records)?;
        self.ledger = candidate;
        for record in &records {
            self.apply_history_projection(
                record.wallet,
                &record.repaired_history,
                record.history_status.as_ref(),
            );
            if record.expected_fence.is_some() {
                self.fences.remove(&record.wallet);
            }
        }
        Ok(())
    }

    /// Commit a complete reconciled wallet-second. Input order is deliberately
    /// discarded before any classification, gate, or financial work.
    pub fn commit(
        &mut self,
        aggregates: Vec<ActivityAggregate>,
        context: &BucketDecisionContext,
        frozen_basis: FrozenDecisionBasis,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        self.commit_with_freshness_policy(aggregates, context, frozen_basis, None)
    }

    /// Commit current decisions with the policy captured by the orchestrator at the bucket boundary.
    pub fn commit_with_freshness_policy(
        &mut self,
        mut aggregates: Vec<ActivityAggregate>,
        context: &BucketDecisionContext,
        frozen_basis: FrozenDecisionBasis,
        paper_freshness_policy: Option<PaperFreshnessPolicy>,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let mut context = context.clone();
        if let Some(read) = context.verified_read.as_ref() {
            let index = self.frame_source_index.as_ref().ok_or_else(|| {
                BucketCommitError::Invariant("authenticated read source index absent".to_owned())
            })?;
            if read.retention_epoch != Some(index.retention_epoch()) {
                let epoch = index.retention_epoch();
                let mut fresh =
                    verified_commitment_bindings_with_lookup(read.receipt, &mut |receipt| {
                        index
                            .source_envelope(receipt)
                            .map(CompleteActivityPage::from)
                    })
                    .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
                if index.retention_epoch() != epoch {
                    return Err(BucketCommitError::Invariant(
                        "retention epoch changed during decision verification".to_owned(),
                    ));
                }
                fresh.retention_epoch = Some(epoch);
                context.verified_read = Some(std::sync::Arc::new(fresh));
            }
        }
        if let Some(read) = context.verified_read.as_ref()
            && (context.read_commitment
                != Some(ActivityReadCommitmentReceipt::BindingsV2(read.receipt))
                || aggregates
                    .iter()
                    .any(|aggregate| aggregate.group_id.components().wallet != read.wallet))
        {
            return Err(BucketCommitError::Invariant(
                "bucket authenticated read differs".to_owned(),
            ));
        }
        // Durable frame decisions own copies by wallet, transaction, asset and side,
        // including after terminalization and restart.
        let frame_decisions = self.frames_for_aggregates(&aggregates);
        let mut frame_gate_ids = HashSet::new();
        for frame in frame_decisions {
            let facts = frame;
            if let Some(gate) = self.paper_state.entry_gate_result(&facts.source_trade_id)?
                && gate.wallet == facts.wallet
                && gate.market_id == facts.market_id
                && gate.source_epoch == facts.source_epoch
                && gate.result == "admitted"
                && gate.history_consumed
            {
                frame_gate_ids.insert(facts.source_trade_id.clone());
                // Brackets apply history and anchors only; they never discover counterparts or
                // create continuations. Preserve the frame-owned gate without a source index.
                if context.bracket_commit {
                    continue;
                }
                let receipt = frame.observed_source_receipt.ok_or_else(|| {
                    BucketCommitError::Invariant("frame receipt absent".to_owned())
                })?;
                let source = self
                    .frame_source_index
                    .as_ref()
                    .ok_or_else(|| {
                        BucketCommitError::Invariant("frame source index absent".to_owned())
                    })?
                    .source_envelope(receipt)
                    .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
                let observation = parse_activity_trade_observation(&source.payload)
                    .map_err(|error| BucketCommitError::Invariant(error.to_string()))?;
                let original = observation.group_id.components();
                for aggregate in &aggregates {
                    let candidate = aggregate.group_id.components();
                    if candidate.activity_type != ActivityType::Trade
                        || candidate.wallet != original.wallet
                        || candidate.transaction_hash != original.transaction_hash
                        || candidate.asset != original.asset
                        || candidate.side != original.side
                    {
                        continue;
                    }
                    let id = aggregate.group_id.key();
                    context.observed_source_receipts.insert(id.clone(), receipt);
                    if id != &facts.source_trade_id {
                        context.no_copy_dispositions.insert(
                            id.clone(),
                            NoCopyDisposition {
                                provenance: "reconciled_rest".to_owned(),
                                age_secs: context
                                    .recorded_at_unix
                                    .saturating_sub(aggregate.source_time.0.unix_timestamp())
                                    .max(0),
                                reason: "applied".to_owned(),
                                recorded_at_unix: context.recorded_at_unix,
                            },
                        );
                    }
                }
            } else {
                return Err(BucketCommitError::Invariant(
                    "durable frame admission gate is missing or invalid".to_owned(),
                ));
            }
        }
        let context = &context;
        let first = aggregates.first().ok_or(BucketCommitError::Empty)?;
        let wallet = first.group_id.components().wallet;
        let source_epoch = first.source_time.0.unix_timestamp();
        if aggregates.iter().any(|aggregate| {
            aggregate.group_id.components().wallet != wallet
                || aggregate.source_time.0.unix_timestamp() != source_epoch
        }) {
            return Err(BucketCommitError::MixedBucket);
        }
        aggregates.sort_by(|left, right| left.group_id.key().0.cmp(&right.group_id.key().0));
        // Resolve identity before the coverage branch so covered effects and
        // first-entry history consume the same venue-authoritative mutation as
        // the ordinary apply path.
        let recordable_mutations = aggregates
            .iter()
            .map(|aggregate| recordable_mutation(aggregate, context))
            .collect::<Vec<_>>();

        let mut durable = aggregates
            .iter()
            .map(|aggregate| {
                self.paper_state
                    .activity_group_state(aggregate.group_id.key())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let coverage = self.paper_state.wallet_coverage(&wallet)?;
        // Both fence dispatches must recognize exact disposed revisions before comparing them,
        // including while an ineligible fence keeps the wallet quarantined.
        for (aggregate, state) in aggregates.iter().zip(&mut durable) {
            if let Some(original) = state
                && original.semantic_revision != aggregate.semantic_revision.as_str()
                && original.transaction_hash == aggregate.group_id.components().transaction_hash
                && (self.fences.contains(&wallet)
                    || coverage
                        .activity_cutoff_unix
                        .is_some_and(|cutoff| source_epoch <= cutoff))
                && let Some(revision) = self.paper_state.activity_revision_state(
                    aggregate.group_id.key(),
                    aggregate.semantic_revision.as_str(),
                )?
                && revision.transaction_hash == aggregate.group_id.components().transaction_hash
            {
                *original = revision;
            }
        }
        let correlation_inputs: Value = serde_json::from_str(&context.decision_inputs_json)?;
        if let Some(observations) = correlation_inputs.get("invalid_mapping_observations") {
            let observations: Vec<(SourceTradeId, AppendReceipt)> =
                serde_json::from_value(observations.clone())?;
            if !observations.is_empty() {
                // This is the deterministic bucket fence trigger, not a selected correlation
                // target. Every ambiguous original receipt stays in the fence proof; no binding
                // to any candidate is manufactured.
                let trigger = aggregates
                    .iter()
                    .find(|aggregate| {
                        !context
                            .identity_unresolved
                            .contains(aggregate.group_id.key())
                    })
                    .or_else(|| aggregates.first())
                    .ok_or(BucketCommitError::Empty)?
                    .group_id
                    .key();
                return self.commit_changed_bucket_fence(
                    &aggregates,
                    &durable,
                    wallet,
                    source_epoch,
                    (WalletFenceCause::InvalidMapping, trigger.clone()),
                    context,
                );
            }
        }
        let changed: Vec<_> = aggregates
            .iter()
            .zip(&durable)
            .filter(|(aggregate, state)| {
                state.as_ref().is_some_and(|state| {
                    state.semantic_revision != aggregate.semantic_revision.as_str()
                        || state.transaction_hash
                            != aggregate.group_id.components().transaction_hash
                })
            })
            .map(|(aggregate, _)| aggregate.group_id.key().clone())
            .collect();
        if let Some(trigger) = changed
            .iter()
            .find(|trigger| !context.identity_unresolved.contains(*trigger))
            .or_else(|| changed.first())
        {
            return self.commit_changed_bucket_fence(
                &aggregates,
                &durable,
                wallet,
                source_epoch,
                (WalletFenceCause::RevisedAggregate, trigger.clone()),
                context,
            );
        }
        let seen = durable.iter().filter(|state| state.is_some()).count();
        if seen == aggregates.len() {
            if context.bracket_commit
                && !self.fences.contains(&wallet)
                && !self
                    .covered_history_effects(wallet, source_epoch, &recordable_mutations)
                    .is_empty()
            {
                // An older bracket may have recorded these groups without consuming
                // their markets. Keep those exact records while repairing history
                // from this verified read; the repair must count as bracket activity.
                return self.commit_covered_bucket(
                    &aggregates,
                    &durable,
                    &recordable_mutations,
                    wallet,
                    source_epoch,
                    coverage.anchor_seq.is_some(),
                    context,
                );
            }
            self.paper_state.set_cursor(&wallet, source_epoch)?;
            return Ok(BucketCommitResult {
                retained_revision: false,
                wallet,
                source_epoch,
                dispositions: BTreeMap::new(),
                pending: Vec::new(),
                newly_fenced: None,
                already_committed: true,
            });
        }
        if aggregates
            .iter()
            .zip(&durable)
            .filter(|(_, state)| state.is_none())
            .all(|(aggregate, _)| context.restamp_twins.contains(aggregate.group_id.key()))
        {
            let mut dispositions = BTreeMap::new();
            let records = aggregates
                .iter()
                .zip(&durable)
                .filter(|(_, state)| state.is_none())
                .map(|(aggregate, _)| {
                    dispositions.insert(aggregate.group_id.key().0.clone(), "raw_only".to_owned());
                    activity_record(
                        aggregate,
                        "raw_only".to_owned(),
                        &LedgerEffect::RawOnly,
                        None,
                        None,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.paper_state
                .commit_activity_bucket(&ActivityBucketCommit {
                    wallet,
                    source_epoch,
                    dispositions: records,
                    leader_positions: Vec::new(),
                    gate_results: Vec::new(),
                    history_effects: Vec::new(),
                    history_status: None,
                    pending: Vec::new(),
                    fence: None,
                    reanchor: None,
                    advance_cursor: true,
                })?;
            return Ok(BucketCommitResult {
                retained_revision: false,
                wallet,
                source_epoch,
                dispositions,
                pending: Vec::new(),
                newly_fenced: None,
                already_committed: false,
            });
        }
        if seen == 0
            && (coverage.reanchor_required
                || (coverage
                    .activity_cutoff_unix
                    .is_some_and(|cutoff| source_epoch > cutoff)
                    && self
                        .paper_state
                        .last_activity_group_epoch(&wallet)?
                        .is_some_and(|last_epoch| last_epoch >= source_epoch)))
        {
            return self.commit_late_group_reanchor(
                &aggregates,
                &recordable_mutations,
                wallet,
                source_epoch,
                context,
            );
        }
        if coverage
            .activity_cutoff_unix
            .is_none_or(|cutoff| source_epoch <= cutoff)
        {
            return self.commit_covered_bucket(
                &aggregates,
                &durable,
                &recordable_mutations,
                wallet,
                source_epoch,
                coverage.anchor_seq.is_some(),
                context,
            );
        }
        let unseen = aggregates
            .iter()
            .zip(&durable)
            .filter(|(_, state)| state.is_none())
            .map(|(aggregate, _)| aggregate.group_id.key())
            .collect::<Vec<_>>();
        if seen != 0 {
            let trigger = aggregates
                .iter()
                .zip(&durable)
                .find(|(_, state)| state.is_none())
                .map(|(aggregate, _)| aggregate.group_id.key().clone())
                .ok_or(BucketCommitError::PartialDurableBucket)?;
            return self.commit_changed_bucket_fence(
                &aggregates,
                &durable,
                wallet,
                source_epoch,
                (WalletFenceCause::LateEqualSecondGroup, trigger),
                context,
            );
        }
        if !unseen.is_empty()
            && unseen
                .iter()
                .all(|source_trade_id| context.identity_unresolved.contains(*source_trade_id))
        {
            return self.commit_covered_bucket(
                &aggregates,
                &durable,
                &recordable_mutations,
                wallet,
                source_epoch,
                true,
                context,
            );
        }

        let decision_inputs: Value = serde_json::from_str(&context.decision_inputs_json)?;
        let mut mutations = Vec::with_capacity(aggregates.len());
        for (aggregate, recordable) in aggregates.iter().zip(&recordable_mutations) {
            if context
                .identity_unresolved
                .contains(aggregate.group_id.key())
            {
                mutations.push(recordable.clone());
                continue;
            }
            match LedgerMutation::from_activity(aggregate) {
                Ok(mutation) => mutations.push(resolve_identity(mutation, context)),
                Err(error) => {
                    return self.commit_fence(
                        &aggregates,
                        wallet,
                        source_epoch,
                        error.fence_cause(),
                        aggregate.group_id.key().clone(),
                        context,
                    );
                }
            }
        }

        if self.fences.contains(&wallet) {
            return self.commit_fenced_bucket(
                &aggregates,
                &mutations,
                wallet,
                source_epoch,
                context,
            );
        }
        if let Some((trigger, cause)) =
            mutations
                .iter()
                .find_map(|mutation| match mutation.effect.effective() {
                    LedgerEffect::Conversion => Some((
                        mutation.source_trade_id.clone(),
                        WalletFenceCause::Conversion,
                    )),
                    LedgerEffect::UnknownEffect => Some((
                        mutation.source_trade_id.clone(),
                        WalletFenceCause::UnknownEffect,
                    )),
                    _ => None,
                })
        {
            return self.commit_fence(&aggregates, wallet, source_epoch, cause, trigger, context);
        }
        let reanchor_trigger =
            aggregates
                .iter()
                .zip(&mutations)
                .find_map(|(aggregate, mutation)| {
                    if !context.bracket_commit
                        && context
                            .identity_unresolved
                            .contains(&mutation.source_trade_id)
                    {
                        Some((
                            mutation.source_trade_id.clone(),
                            "identity_unresolved".to_owned(),
                        ))
                    } else if matches!(mutation.effect.effective(), LedgerEffect::RequiresAnchor)
                        && aggregate.group_id.components().condition_id.is_none()
                    {
                        Some((
                            mutation.source_trade_id.clone(),
                            "reanchor_required_redemption".to_owned(),
                        ))
                    } else {
                        None
                    }
                });
        let history_complete = context
            .history_status
            .as_ref()
            .filter(|status| status.wallet == wallet)
            .map_or_else(
                || self.complete_history.contains(&wallet),
                |status| status.complete,
            );
        let (applied, trade_decisions, first_entries) = match classify_complete_second(
            complete_read_entry_policy(current_paper_version(complete_read_version(
                context.read_commitment,
            ))),
            &self.ledger,
            wallet,
            &mutations,
            context.reconstruction_quality,
            &context.signal_config,
            history_complete,
            &|market_id| self.entry_gate.has_market(&wallet, market_id),
        ) {
            Ok(SecondVerdict::OrderIndependent {
                applied,
                decisions,
                first_entries,
            }) => (applied, decisions, first_entries),
            Ok(SecondVerdict::OrderDependent { .. }) => {
                let trigger = mutations
                    .iter()
                    .find(|mutation| {
                        !context
                            .identity_unresolved
                            .contains(&mutation.source_trade_id)
                    })
                    .map(|mutation| mutation.source_trade_id.clone())
                    .ok_or(BucketCommitError::Empty)?;
                return self.commit_fence(
                    &aggregates,
                    wallet,
                    source_epoch,
                    WalletFenceCause::OrderDependentEqualSecond,
                    trigger,
                    context,
                );
            }
            Err(error) => {
                let cause = error.fence_cause();
                let trigger = mutation_error_id(&error);
                return self.commit_fence(
                    &aggregates,
                    wallet,
                    source_epoch,
                    cause,
                    trigger,
                    context,
                );
            }
        };

        let mut candidate = self.ledger.clone();
        if let Err(error) = candidate.apply_all_or_none(&mutations) {
            return self.commit_fence(
                &aggregates,
                wallet,
                source_epoch,
                error.fence_cause(),
                mutation_error_id(&error),
                context,
            );
        }
        let (mut gate_results, history_effects, gate_outcomes) =
            Self::derive_gate_results(wallet, source_epoch, &trade_decisions, &first_entries);
        // IDs omit epochs. Only an already authenticated durable frame gate owns this exception.
        gate_results.retain(|gate| !frame_gate_ids.contains(&gate.source_trade_id));

        let mut pending = Vec::new();
        let mut dispositions = BTreeMap::new();
        let mut disposition_records = Vec::with_capacity(aggregates.len());
        for ((aggregate, mutation), applied_effect) in
            aggregates.iter().zip(&mutations).zip(&applied)
        {
            let source_trade_id = aggregate.group_id.key().clone();
            let disposition = match mutation.effect.effective() {
                LedgerEffect::RawOnly => "raw_only".to_owned(),
                LedgerEffect::RequiresAnchor => {
                    if aggregate.group_id.components().condition_id.is_some() {
                        "raw_only".to_owned()
                    } else {
                        "reanchor_required_redemption".to_owned()
                    }
                }
                LedgerEffect::Trade { .. } => {
                    let outcome = gate_outcomes
                        .get(&source_trade_id.0)
                        .cloned()
                        .unwrap_or_else(|| "not_an_entry".to_owned());
                    if let Some(no_copy) = context.no_copy_dispositions.get(&source_trade_id) {
                        no_copy.reason.clone()
                    } else if frame_gate_ids.contains(&source_trade_id) {
                        "applied".to_owned()
                    } else if outcome == "admitted"
                        && context.copy_eligible
                        && !coverage.reanchor_required
                        && reanchor_trigger.is_none()
                        && !trade_decisions.iter().any(|decision| {
                            decision.source_trade_id == source_trade_id
                                && decision.action_order_dependent
                        })
                    {
                        let decision = trade_decisions
                            .iter()
                            .find(|decision| decision.source_trade_id == source_trade_id)
                            .ok_or(BucketCommitError::Empty)?;
                        let LedgerEffect::Trade {
                            market_id,
                            outcome_id,
                            side,
                            amount,
                            price,
                        } = mutation.effect.effective()
                        else {
                            return Err(BucketCommitError::Empty);
                        };
                        let facts = DecisionContinuationFacts {
                            paper_freshness_policy: if matches!(
                                context.read_commitment,
                                Some(ActivityReadCommitmentReceipt::BindingsV2(_))
                            ) {
                                paper_freshness_policy
                            } else {
                                None
                            },
                            source_trade_id: source_trade_id.clone(),
                            semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
                            transaction_hash: aggregate
                                .group_id
                                .components()
                                .transaction_hash
                                .clone(),
                            wallet,
                            source_epoch,
                            market_id: market_id.clone(),
                            outcome_id: *outcome_id,
                            side: *side,
                            price: *price,
                            share_amount: *amount,
                            provenance: context
                                .observation_provenance
                                .get(&source_trade_id)
                                .copied()
                                .unwrap_or(TradeProvenance::RestPoll),
                            pre_bucket_action: decision.action,
                            reconstruction_quality: context.reconstruction_quality,
                            action_confidence_ppm: ProbabilityPpm(
                                u32::from(context.reconstruction_quality.get()) * 10_000,
                            ),
                            gate_result: "admitted".to_owned(),
                            frozen_basis,
                            applied_configuration_hash: context
                                .applied_configuration
                                .canonical_hash(),
                            applied_configuration: context.applied_configuration.clone(),
                            decision_inputs: decision_inputs.clone(),
                        };
                        if context.page_occurrences.is_empty() {
                            return Err(BucketCommitError::Invariant(
                                "admitted continuation is missing source page receipts".to_owned(),
                            ));
                        }
                        if context.read_commitment.is_none() {
                            return Err(BucketCommitError::Invariant(
                                "admitted continuation is missing its complete-read commitment"
                                    .to_owned(),
                            ));
                        }
                        if matches!(
                            context.read_commitment,
                            Some(ActivityReadCommitmentReceipt::BindingsV2(_))
                        ) && paper_freshness_policy.is_none_or(|policy| !policy.valid())
                        {
                            return Err(BucketCommitError::Invariant(
                                "current continuation is missing a valid frozen freshness policy"
                                    .to_owned(),
                            ));
                        }
                        let frozen_inputs_json = serde_json::to_string(
                            &DecisionContinuationV3::new(
                                facts,
                                context
                                    .observed_source_receipts
                                    .get(&source_trade_id)
                                    .copied(),
                                context.page_occurrences.clone(),
                                context.read_commitment,
                            )
                            .current_paper(),
                        )?;
                        pending.push(DecisionPendingRecord {
                            source_trade_id: source_trade_id.clone(),
                            semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
                            wallet,
                            source_epoch,
                            frozen_inputs_json,
                            updated_at_unix: context.recorded_at_unix,
                        });
                        "decision_pending".to_owned()
                    } else if outcome == "admitted" {
                        if trade_decisions.iter().any(|decision| {
                            decision.source_trade_id == source_trade_id
                                && decision.action_order_dependent
                        }) {
                            "order_dependent_equal_second_action".to_owned()
                        } else if context.bracket_commit
                            && !context.copy_eligible
                            && !coverage.reanchor_required
                            && reanchor_trigger.is_none()
                        {
                            HISTORY_ONLY_BRACKET.to_owned()
                        } else {
                            "not_copy_eligible".to_owned()
                        }
                    } else {
                        outcome
                    }
                }
                _ => "applied".to_owned(),
            };
            let no_copy = if disposition == HISTORY_ONLY_BRACKET {
                Some(NoCopyDisposition {
                    provenance: "reconciled_rest".to_owned(),
                    age_secs: context.recorded_at_unix.saturating_sub(source_epoch).max(0),
                    reason: HISTORY_ONLY_BRACKET.to_owned(),
                    recorded_at_unix: context.recorded_at_unix,
                })
            } else {
                context.no_copy_dispositions.get(&source_trade_id).cloned()
            };
            dispositions.insert(source_trade_id.0.clone(), disposition.clone());
            disposition_records.push(activity_record(
                aggregate,
                disposition,
                &applied_effect.effect,
                applied_effect.clamped_residual,
                no_copy,
            )?);
        }

        let bucket = ActivityBucketCommit {
            wallet,
            source_epoch,
            dispositions: disposition_records,
            leader_positions: touched_leader_rows(&candidate, wallet, &mutations),
            gate_results,
            history_effects: history_effects.clone(),
            history_status: context.history_status.clone(),
            pending: pending.clone(),
            fence: None,
            reanchor: reanchor_trigger
                .clone()
                .map(|(source_trade_id, reason)| ReanchorRecord {
                    source_trade_id,
                    reason,
                }),
            advance_cursor: true,
        };
        self.paper_state.commit_activity_bucket(&bucket)?;
        self.ledger = candidate;
        self.apply_history_projection(wallet, &history_effects, context.history_status.as_ref());
        Ok(BucketCommitResult {
            retained_revision: false,
            wallet,
            source_epoch,
            dispositions,
            pending: pending
                .into_iter()
                .map(|record| record.source_trade_id)
                .collect(),
            newly_fenced: None,
            already_committed: false,
        })
    }

    fn commit_late_group_reanchor(
        &mut self,
        aggregates: &[ActivityAggregate],
        resolved_mutations: &[LedgerMutation],
        wallet: WalletAddress,
        source_epoch: i64,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        const DISPOSITION: &str = "reanchor_required_late_group";
        let trigger = aggregates
            .iter()
            .find(|aggregate| {
                !context
                    .identity_unresolved
                    .contains(aggregate.group_id.key())
            })
            .map(|aggregate| aggregate.group_id.key().clone())
            .or_else(|| {
                aggregates
                    .first()
                    .map(|aggregate| aggregate.group_id.key().clone())
            })
            .ok_or(BucketCommitError::Empty)?;
        let mut dispositions = BTreeMap::new();
        let records = aggregates
            .iter()
            .map(|aggregate| {
                dispositions.insert(aggregate.group_id.key().0.clone(), DISPOSITION.to_owned());
                activity_record(
                    aggregate,
                    DISPOSITION.to_owned(),
                    &LedgerEffect::RawOnly,
                    None,
                    context
                        .no_copy_dispositions
                        .get(aggregate.group_id.key())
                        .cloned(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let history_effects = if context.bracket_commit && !self.fences.contains(&wallet) {
            self.covered_history_effects(wallet, source_epoch, resolved_mutations)
        } else {
            Vec::new()
        };
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: history_effects.clone(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: None,
                reanchor: Some(ReanchorRecord {
                    source_trade_id: trigger,
                    reason: DISPOSITION.to_owned(),
                }),
                advance_cursor: false,
            })?;
        self.apply_history_projection(wallet, &history_effects, None);
        Ok(BucketCommitResult {
            retained_revision: false,
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: None,
            already_committed: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_covered_bucket(
        &mut self,
        aggregates: &[ActivityAggregate],
        durable: &[Option<ActivityGroupState>],
        resolved_mutations: &[LedgerMutation],
        wallet: WalletAddress,
        source_epoch: i64,
        late: bool,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let all_stored = durable.iter().all(Option::is_some);
        let disposition = if late {
            "anchor_covered_late"
        } else {
            "anchor_covered"
        };
        let mut dispositions = BTreeMap::new();
        let mut records = Vec::new();
        let mut mutations = Vec::new();
        let repair_history = context.bracket_commit && !self.fences.contains(&wallet);
        for ((aggregate, state), mutation) in aggregates.iter().zip(durable).zip(resolved_mutations)
        {
            if let Some(state) = state {
                dispositions.insert(
                    aggregate.group_id.key().0.clone(),
                    "already_committed".to_owned(),
                );
                if repair_history {
                    let original = self
                        .paper_state
                        .activity_group_state(aggregate.group_id.key())?;
                    let state = original.as_ref().unwrap_or(state);
                    records.push(ActivityDispositionRecord {
                        source_trade_id: aggregate.group_id.key().clone(),
                        transaction_hash: state.transaction_hash.clone(),
                        wallet,
                        source_epoch: state.source_epoch,
                        semantic_revision: state.semantic_revision.clone(),
                        activity_type: aggregate
                            .group_id
                            .components()
                            .activity_type
                            .as_str()
                            .to_owned(),
                        disposition: state.disposition.clone(),
                        proof_json: state.proof_json.clone(),
                        no_copy: None,
                    });
                }
                continue;
            }
            let unresolved = context
                .identity_unresolved
                .contains(aggregate.group_id.key());
            let group_disposition = if unresolved { "raw_only" } else { disposition };
            dispositions.insert(
                aggregate.group_id.key().0.clone(),
                group_disposition.to_owned(),
            );
            records.push(activity_record(
                aggregate,
                group_disposition.to_owned(),
                &mutation.effect,
                None,
                context
                    .no_copy_dispositions
                    .get(aggregate.group_id.key())
                    .cloned(),
            )?);
            mutations.push(mutation.clone());
        }
        let history_effects = if context.bracket_commit && self.fences.contains(&wallet) {
            Vec::new()
        } else {
            self.covered_history_effects(
                wallet,
                source_epoch,
                if repair_history {
                    resolved_mutations
                } else {
                    &mutations
                },
            )
        };
        let unresolved_trigger = (!context.bracket_commit)
            .then(|| {
                mutations.iter().find_map(|mutation| {
                    context
                        .identity_unresolved
                        .contains(&mutation.source_trade_id)
                        .then(|| mutation.source_trade_id.clone())
                })
            })
            .flatten();
        let late_trigger = late
            .then(|| {
                mutations.iter().find_map(|mutation| {
                    (!context
                        .identity_unresolved
                        .contains(&mutation.source_trade_id))
                    .then(|| mutation.source_trade_id.clone())
                })
            })
            .flatten();
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: history_effects.clone(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: None,
                reanchor: unresolved_trigger
                    .map(|source_trade_id| ReanchorRecord {
                        source_trade_id,
                        reason: "identity_unresolved".to_owned(),
                    })
                    .or_else(|| {
                        late_trigger.map(|source_trade_id| ReanchorRecord {
                            source_trade_id,
                            reason: disposition.to_owned(),
                        })
                    }),
                advance_cursor: !all_stored,
            })?;
        // The running projection follows the committed transaction before any
        // later write, so a failed cursor update cannot leave durable history
        // unpublished.
        self.apply_history_projection(wallet, &history_effects, context.history_status.as_ref());
        if all_stored {
            self.paper_state.set_cursor(&wallet, source_epoch)?;
        }
        Ok(BucketCommitResult {
            retained_revision: false,
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: None,
            already_committed: false,
        })
    }

    fn covered_history_effects(
        &self,
        wallet: WalletAddress,
        source_epoch: i64,
        mutations: &[LedgerMutation],
    ) -> Vec<MarketHistoryRecord> {
        let mut first_buys: BTreeMap<String, (MarketId, SourceTradeId)> = BTreeMap::new();
        for mutation in mutations {
            let LedgerEffect::Trade {
                market_id,
                side: Side::Buy,
                ..
            } = mutation.effect.effective()
            else {
                continue;
            };
            if self.entry_gate.has_market(&wallet, market_id) {
                continue;
            }
            first_buys
                .entry(market_id.to_string())
                .and_modify(|(_, current)| {
                    if mutation.source_trade_id.0 < current.0 {
                        current.clone_from(&mutation.source_trade_id);
                    }
                })
                .or_insert_with(|| (market_id.clone(), mutation.source_trade_id.clone()));
        }
        first_buys
            .into_values()
            .map(|(market_id, source_trade_id)| MarketHistoryRecord {
                wallet,
                market_id,
                first_epoch: source_epoch,
                source_trade_id,
            })
            .collect()
    }

    fn apply_history_projection(
        &mut self,
        wallet: WalletAddress,
        history_effects: &[MarketHistoryRecord],
        history_status: Option<&WalletHistoryStatusRecord>,
    ) {
        for history in history_effects {
            self.entry_gate
                .record_entry(history.wallet, &history.market_id);
        }
        if let Some(status) = history_status.filter(|status| status.wallet == wallet) {
            if status.complete {
                self.complete_history.insert(wallet);
                if !self.entry_gate.has_wallet(&wallet) {
                    self.entry_gate
                        .merge_history(HashMap::from([(wallet, HashSet::new())]));
                }
            } else {
                self.complete_history.remove(&wallet);
            }
        }
    }

    fn derive_gate_results(
        wallet: WalletAddress,
        source_epoch: i64,
        decisions: &[TradeDecision],
        first_entries: &[(MarketId, SourceTradeId)],
    ) -> (
        Vec<EntryGateResultRecord>,
        Vec<MarketHistoryRecord>,
        BTreeMap<String, String>,
    ) {
        let outcomes = decisions
            .iter()
            .map(|decision| {
                (
                    decision.source_trade_id.0.clone(),
                    decision.entry.as_str().to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let history = first_entries
            .iter()
            .map(|(market_id, source_trade_id)| MarketHistoryRecord {
                wallet,
                market_id: market_id.clone(),
                first_epoch: source_epoch,
                source_trade_id: source_trade_id.clone(),
            })
            .collect::<Vec<_>>();
        let gate_results = decisions
            .iter()
            .map(|decision| EntryGateResultRecord {
                source_trade_id: decision.source_trade_id.clone(),
                wallet,
                market_id: decision.market_id.clone(),
                source_epoch,
                result: decision.entry.as_str().to_owned(),
                history_consumed: history
                    .iter()
                    .any(|effect| effect.source_trade_id == decision.source_trade_id),
            })
            .collect();
        (gate_results, history, outcomes)
    }

    fn commit_fence(
        &mut self,
        aggregates: &[ActivityAggregate],
        wallet: WalletAddress,
        source_epoch: i64,
        cause: WalletFenceCause,
        trigger: SourceTradeId,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let proof = json!({"bucket_epoch": source_epoch, "cause": cause.as_str()});
        let mut dispositions = BTreeMap::new();
        let mut records = Vec::with_capacity(aggregates.len());
        let mut unresolved_trigger = None;
        for aggregate in aggregates {
            let unresolved = context
                .identity_unresolved
                .contains(aggregate.group_id.key());
            let disposition = if unresolved {
                if !context.bracket_commit {
                    unresolved_trigger.get_or_insert_with(|| aggregate.group_id.key().clone());
                }
                "raw_only".to_owned()
            } else if aggregate.group_id.key() == &trigger {
                cause.as_str().to_owned()
            } else {
                "wallet_fenced".to_owned()
            };
            dispositions.insert(aggregate.group_id.key().0.clone(), disposition.clone());
            let mutation = recordable_mutation(aggregate, context);
            records.push(activity_record(
                aggregate,
                disposition,
                &mutation.effect,
                None,
                unresolved
                    .then(|| {
                        context
                            .no_copy_dispositions
                            .get(aggregate.group_id.key())
                            .cloned()
                    })
                    .flatten(),
            )?);
        }
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: Some(WalletFenceRecord {
                    wallet,
                    source_trade_id: trigger,
                    cause: cause.as_str().to_owned(),
                    proof_json: serde_json::to_string(&proof)?,
                    fenced_at_unix: context.recorded_at_unix,
                }),
                reanchor: unresolved_trigger.map(|source_trade_id| ReanchorRecord {
                    source_trade_id,
                    reason: "identity_unresolved".to_owned(),
                }),
                advance_cursor: true,
            })?;
        self.fences.insert(wallet);
        Ok(BucketCommitResult {
            retained_revision: false,
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: Some(cause),
            already_committed: false,
        })
    }

    fn commit_changed_bucket_fence(
        &mut self,
        aggregates: &[ActivityAggregate],
        durable: &[Option<ActivityGroupState>],
        wallet: WalletAddress,
        source_epoch: i64,
        fence: (WalletFenceCause, SourceTradeId),
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let (cause, trigger) = fence;
        let already_fenced = self.fences.contains(&wallet);
        let mut proof = json!({"bucket_epoch": source_epoch, "cause": cause.as_str()});
        if cause == WalletFenceCause::InvalidMapping {
            let inputs: Value = serde_json::from_str(&context.decision_inputs_json)?;
            if let Some(observations) = inputs.get("invalid_mapping_observations") {
                proof["invalid_mapping_observations"] = observations.clone();
            }
        }
        let proof_json = serde_json::to_string(&proof)?;
        if cause == WalletFenceCause::InvalidMapping
            && !already_fenced
            && let Some(aggregate) = aggregates
                .iter()
                .zip(durable)
                .find_map(|(aggregate, state)| state.is_some().then_some(aggregate))
        {
            // Comparison states may contain a disposed revision; the witness must use
            // the immutable original even after a recovery cleared the previous fence.
            let state = self
                .paper_state
                .activity_group_state(aggregate.group_id.key())?
                .ok_or(BucketCommitError::PartialDurableBucket)?;
            // Preserve the original disposition and its history clock as the fence witness.
            // Once that fence is durable, the existing bucket owner can retain any revised
            // candidates without replacing their original economic effects. A crash between
            // these commits leaves the wallet fenced and the unfinished read replayable.
            self.paper_state
                .commit_activity_bucket(&ActivityBucketCommit {
                    wallet,
                    source_epoch: state.source_epoch,
                    dispositions: vec![ActivityDispositionRecord {
                        source_trade_id: aggregate.group_id.key().clone(),
                        transaction_hash: state.transaction_hash.clone(),
                        wallet,
                        source_epoch: state.source_epoch,
                        semantic_revision: state.semantic_revision.clone(),
                        activity_type: aggregate
                            .group_id
                            .components()
                            .activity_type
                            .as_str()
                            .to_owned(),
                        disposition: state.disposition.clone(),
                        proof_json: state.proof_json.clone(),
                        no_copy: None,
                    }],
                    leader_positions: Vec::new(),
                    gate_results: Vec::new(),
                    history_effects: Vec::new(),
                    history_status: None,
                    pending: Vec::new(),
                    fence: Some(WalletFenceRecord {
                        wallet,
                        source_trade_id: aggregate.group_id.key().clone(),
                        cause: cause.as_str().to_owned(),
                        proof_json,
                        fenced_at_unix: context.recorded_at_unix,
                    }),
                    reanchor: None,
                    advance_cursor: false,
                })?;
            self.fences.insert(wallet);
            let mut result = self.commit_changed_bucket_fence(
                aggregates,
                durable,
                wallet,
                source_epoch,
                (cause, trigger),
                context,
            )?;
            result.newly_fenced = Some(cause);
            return Ok(result);
        }
        let mut dispositions = BTreeMap::new();
        let mut records = Vec::new();
        let mut unresolved_trigger = None;
        let mut retained_revision = false;
        for (aggregate, state) in aggregates.iter().zip(durable) {
            let differs = state.as_ref().is_none_or(|state| {
                state.semantic_revision != aggregate.semantic_revision.as_str()
                    || state.transaction_hash != aggregate.group_id.components().transaction_hash
            });
            let unresolved = cause != WalletFenceCause::LateEqualSecondGroup
                && context
                    .identity_unresolved
                    .contains(aggregate.group_id.key());
            let disposition = if differs {
                if unresolved {
                    if !context.bracket_commit {
                        unresolved_trigger.get_or_insert_with(|| aggregate.group_id.key().clone());
                    }
                    "raw_only".to_owned()
                } else if aggregate.group_id.key() == &trigger && !already_fenced {
                    cause.as_str().to_owned()
                } else {
                    "wallet_fenced".to_owned()
                }
            } else {
                "already_committed".to_owned()
            };
            dispositions.insert(aggregate.group_id.key().0.clone(), disposition.clone());
            if differs {
                retained_revision |= state.as_ref().is_some_and(|original| {
                    original.semantic_revision != aggregate.semantic_revision.as_str()
                }) && !self.paper_state.activity_revision_matches(
                    aggregate.group_id.key(),
                    aggregate.semantic_revision.as_str(),
                    &aggregate.group_id.components().transaction_hash,
                )?;
                let mutation = recordable_mutation(aggregate, context);
                records.push(activity_record(
                    aggregate,
                    disposition,
                    &mutation.effect,
                    None,
                    unresolved
                        .then(|| {
                            context
                                .no_copy_dispositions
                                .get(aggregate.group_id.key())
                                .cloned()
                        })
                        .flatten(),
                )?);
            }
        }
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: (!already_fenced).then(|| WalletFenceRecord {
                    wallet,
                    source_trade_id: trigger,
                    cause: cause.as_str().to_owned(),
                    proof_json,
                    fenced_at_unix: context.recorded_at_unix,
                }),
                reanchor: unresolved_trigger.map(|source_trade_id| ReanchorRecord {
                    source_trade_id,
                    reason: "identity_unresolved".to_owned(),
                }),
                advance_cursor: true,
            })?;
        if !already_fenced {
            self.fences.insert(wallet);
        }
        Ok(BucketCommitResult {
            retained_revision,
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: (!already_fenced).then_some(cause),
            already_committed: false,
        })
    }

    fn commit_fenced_bucket(
        &mut self,
        aggregates: &[ActivityAggregate],
        mutations: &[LedgerMutation],
        wallet: WalletAddress,
        source_epoch: i64,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let known: Vec<_> = mutations
            .iter()
            .filter(|mutation| {
                !matches!(
                    mutation.effect.effective(),
                    LedgerEffect::Conversion | LedgerEffect::UnknownEffect
                )
            })
            .cloned()
            .collect();
        let mut candidate = self.ledger.clone();
        let history_complete = context
            .history_status
            .as_ref()
            .filter(|status| status.wallet == wallet)
            .map_or_else(
                || self.complete_history.contains(&wallet),
                |status| status.complete,
            );
        let applied_outcomes = match classify_complete_second(
            pe_position_ledger::SameSecondEntryPolicy::Legacy,
            &self.ledger,
            wallet,
            &known,
            context.reconstruction_quality,
            &context.signal_config,
            history_complete,
            &|market_id| self.entry_gate.has_market(&wallet, market_id),
        ) {
            Ok(SecondVerdict::OrderIndependent { applied, .. }) => {
                candidate.apply_all_or_none(&known).ok().map(|_| applied)
            }
            Ok(SecondVerdict::OrderDependent { .. }) | Err(_) => None,
        };
        let applied = applied_outcomes.is_some();
        let applied_by_id = known
            .iter()
            .zip(applied_outcomes.iter().flatten())
            .map(|(mutation, outcome)| (mutation.source_trade_id.clone(), outcome))
            .collect::<HashMap<_, _>>();
        let mut dispositions = BTreeMap::new();
        let unresolved_trigger = (!context.bracket_commit)
            .then(|| {
                mutations.iter().find_map(|mutation| {
                    context
                        .identity_unresolved
                        .contains(&mutation.source_trade_id)
                        .then(|| mutation.source_trade_id.clone())
                })
            })
            .flatten();
        let records = aggregates
            .iter()
            .zip(mutations)
            .map(|(aggregate, mutation)| {
                let disposition = if context
                    .identity_unresolved
                    .contains(&mutation.source_trade_id)
                {
                    "raw_only"
                } else if applied
                    && !matches!(
                        mutation.effect.effective(),
                        LedgerEffect::Conversion | LedgerEffect::UnknownEffect
                    )
                {
                    "wallet_fenced_applied"
                } else {
                    "wallet_fenced"
                };
                dispositions.insert(aggregate.group_id.key().0.clone(), disposition.to_owned());
                let applied_effect = applied_by_id.get(&mutation.source_trade_id);
                activity_record(
                    aggregate,
                    disposition.to_owned(),
                    applied_effect.map_or(&mutation.effect, |outcome| &outcome.effect),
                    applied_effect.and_then(|outcome| outcome.clamped_residual),
                    context
                        .no_copy_dispositions
                        .get(&mutation.source_trade_id)
                        .cloned(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: if applied {
                    touched_leader_rows(&candidate, wallet, &known)
                } else {
                    Vec::new()
                },
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: None,
                reanchor: unresolved_trigger.map(|source_trade_id| ReanchorRecord {
                    source_trade_id,
                    reason: "identity_unresolved".to_owned(),
                }),
                advance_cursor: true,
            })?;
        if applied {
            self.ledger = candidate;
        }
        Ok(BucketCommitResult {
            retained_revision: false,
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: None,
            already_committed: false,
        })
    }
}

fn activity_record(
    aggregate: &ActivityAggregate,
    disposition: String,
    effect: &LedgerEffect,
    clamped_residual: Option<u64>,
    no_copy: Option<NoCopyDisposition>,
) -> Result<ActivityDispositionRecord, LedgerEffectDocumentError> {
    let components = aggregate.group_id.components();
    Ok(ActivityDispositionRecord {
        source_trade_id: aggregate.group_id.key().clone(),
        transaction_hash: components.transaction_hash.clone(),
        wallet: components.wallet,
        source_epoch: aggregate.source_time.0.unix_timestamp(),
        semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
        activity_type: components.activity_type.as_str().to_owned(),
        disposition,
        proof_json: AppliedEffect {
            effect: effect.clone(),
            clamped_residual,
        }
        .to_document()?,
        no_copy,
    })
}

pub(crate) fn recordable_mutation(
    aggregate: &ActivityAggregate,
    context: &BucketDecisionContext,
) -> LedgerMutation {
    let mutation = LedgerMutation::from_activity(aggregate).unwrap_or_else(|_| LedgerMutation {
        source_trade_id: aggregate.group_id.key().clone(),
        transaction_hash: aggregate.group_id.components().transaction_hash.clone(),
        wallet: aggregate.group_id.components().wallet,
        source_time: aggregate.source_time.clone(),
        effect: LedgerEffect::UnknownEffect,
    });
    resolve_identity(mutation, context)
}

fn resolve_identity(
    mut mutation: LedgerMutation,
    context: &BucketDecisionContext,
) -> LedgerMutation {
    if context
        .identity_unresolved
        .contains(&mutation.source_trade_id)
    {
        mutation.effect = LedgerEffect::RawOnly;
        return mutation;
    }
    if let Some(identity) = context.identity_overrides.get(&mutation.source_trade_id) {
        return mutation
            .with_verified_identity(identity.verified.clone(), identity.evidence_hash.clone());
    }
    mutation
}

fn mutation_error_id(error: &LedgerError) -> SourceTradeId {
    match error {
        LedgerError::InvalidMapping { source_trade_id }
        | LedgerError::Underflow { source_trade_id }
        | LedgerError::Overflow { source_trade_id }
        | LedgerError::Conversion { source_trade_id }
        | LedgerError::UnknownEffect { source_trade_id } => source_trade_id.clone(),
    }
}

fn encode_key(key: &MarketOutcomeId) -> String {
    format!("{}|{}", key.market(), key.outcome().0)
}

fn touched_leader_rows(
    ledger: &PositionLedger,
    wallet: WalletAddress,
    mutations: &[LedgerMutation],
) -> Vec<LeaderPositionRow> {
    let mut keys: BTreeMap<String, MarketOutcomeId> = BTreeMap::new();
    for mutation in mutations {
        for key in mutation.touched_keys() {
            keys.insert(encode_key(&key), key);
        }
    }
    keys.into_values()
        .map(|key| {
            let state = ledger
                .position(&wallet)
                .and_then(|snapshot| snapshot.positions.get(&key))
                .copied()
                .unwrap_or_default();
            LeaderPositionRow {
                wallet,
                market_id: key.market().clone(),
                outcome_id: key.outcome(),
                long_contracts: state.long_contracts,
                short_contracts: state.short_contracts,
            }
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod continuation_v3_tests {
    #![allow(clippy::unwrap_used)]

    use std::fs::OpenOptions;
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    use pe_core_types::{EventSeq, Probability, ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
    use pe_paper_state::DecisionPendingState;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

    fn receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(u8::MAX); 32]),
        }
    }

    fn activity_page(payload: &[u8]) -> CompleteActivityPage {
        CompleteActivityPage {
            payload: payload.to_vec(),
            observed_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
            source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
            content_type: ContentType::Json,
        }
    }

    fn activity_request_url(start: Option<i64>, end: i64, offset: u32) -> String {
        PolymarketEndpoint::UserPositionActivityPage {
            user: "0x1111111111111111111111111111111111111111".to_owned(),
            end,
            start: start.map(|start| start.saturating_add(1)),
            offset,
        }
        .url("https://data-api.polymarket.com")
    }

    fn activity_page_fixture(
        payload: &[u8],
        start: Option<i64>,
        end: i64,
        offset: u32,
        receipt: AppendReceipt,
    ) -> (PageOccurrence, ReconciliationPageEvidence) {
        let request_url = activity_request_url(start, end, offset);
        let raw_hash = blake3::hash(payload).to_hex().to_string();
        let row_count = serde_json::from_slice::<Vec<Value>>(payload).unwrap().len();
        (
            PageOccurrence {
                request_url: request_url.clone(),
                raw_hash: raw_hash.clone(),
                receipt,
            },
            ReconciliationPageEvidence {
                request_url,
                bounds: Some(pe_source_polymarket_public::ActivityRequestBounds { start, end }),
                partition: None,
                offset,
                row_count: u32::try_from(row_count).unwrap(),
                canonical_page_hash: canonical_page_hash(payload).unwrap(),
                raw_page_hash: raw_hash,
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
            },
        )
    }

    fn complete_read_inputs(fixed_end: i64, pages: &[ReconciliationPageEvidence]) -> Value {
        json!({"fixed_end": fixed_end, "pages": pages})
    }

    fn proof_for_occurrences(
        fixed_end: i64,
        occurrences: &[PageOccurrence],
    ) -> Vec<ReconciliationPageEvidence> {
        let last = occurrences.len().saturating_sub(1);
        occurrences
            .iter()
            .enumerate()
            .map(|(index, occurrence)| ReconciliationPageEvidence {
                request_url: occurrence.request_url.clone(),
                bounds: Some(pe_source_polymarket_public::ActivityRequestBounds {
                    start: Some(fixed_end.saturating_sub(1)),
                    end: fixed_end,
                }),
                partition: None,
                offset: u32::try_from(index).unwrap() * RECONCILIATION_PAGE_LIMIT,
                row_count: if index == last {
                    0
                } else {
                    RECONCILIATION_PAGE_LIMIT
                },
                canonical_page_hash: "00".repeat(32),
                raw_page_hash: occurrence.raw_hash.clone(),
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
            })
            .collect()
    }

    fn facts(decision_inputs: Value) -> DecisionContinuationFacts {
        let configuration =
            RuntimeConfig::from_service_config(&crate::config::ServiceConfig::default());
        DecisionContinuationFacts {
            paper_freshness_policy: None,
            source_trade_id: SourceTradeId("g2:continuation".to_owned()),
            semantic_revision: "semantic".to_owned(),
            transaction_hash: "0xtransaction".to_owned(),
            wallet: WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap(),
            source_epoch: 1_700_000_000,
            market_id: MarketId(pe_core_types::VenueMarketId("market".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            share_amount: ShareAmount::from_whole(1).unwrap(),
            provenance: TradeProvenance::RestPoll,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
            gate_result: "admitted".to_owned(),
            applied_configuration_hash: configuration.canonical_hash(),
            applied_configuration: configuration,
            frozen_basis: FrozenDecisionBasis {
                win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                bankroll: dec!(100),
            },
            decision_inputs,
        }
    }

    fn legacy17_facts(decision_inputs: Value) -> DecisionContinuationFacts {
        with_legacy17(facts(decision_inputs))
    }

    /// Replace only the configuration and its hash so the facts model a pre-#545 record.
    fn with_legacy17(mut facts: DecisionContinuationFacts) -> DecisionContinuationFacts {
        facts.applied_configuration = synthetic_legacy17_runtime_config();
        facts.applied_configuration_hash = facts.applied_configuration.canonical_hash();
        facts
    }

    fn legacy_v2_json(facts: &DecisionContinuationFacts) -> String {
        pre_545_frozen_inputs(facts)
    }

    fn durable(value: &impl Serialize) -> DecisionPendingRow {
        let frozen_inputs_json = serde_json::to_string(value).unwrap();
        let continuation: DecisionContinuationV3 =
            serde_json::from_str(&frozen_inputs_json).unwrap();
        DecisionPendingRow {
            source_trade_id: continuation.facts.source_trade_id.clone(),
            semantic_revision: continuation.facts.semantic_revision.clone(),
            wallet: continuation.facts.wallet,
            source_epoch: continuation.facts.source_epoch,
            frozen_inputs_json,
            post_commit_inputs_json: String::new(),
            state: DecisionPendingState::Open,
            terminal_disposition: None,
            updated_at_unix: 1_700_000_001,
        }
    }

    /// PASS: V3 keeps repeated payload occurrences, derives the greatest complete bound, and picks
    /// the lower websocket receipt; the caller-supplied time is not persisted in continuation JSON.
    #[test]
    fn v3_observation_uses_lower_receipt_and_preserves_multiplicity() {
        let fixed_end = 1_700_000_010_i64;
        let pages = vec![
            PageOccurrence {
                request_url: activity_request_url(Some(fixed_end.saturating_sub(1)), fixed_end, 0),
                raw_hash: "same".to_owned(),
                receipt: receipt(9),
            },
            PageOccurrence {
                request_url: activity_request_url(
                    Some(fixed_end.saturating_sub(1)),
                    fixed_end,
                    RECONCILIATION_PAGE_LIMIT,
                ),
                raw_hash: "same".to_owned(),
                receipt: receipt(11),
            },
        ];
        let proof = proof_for_occurrences(fixed_end, &pages);
        let mut frozen = facts(complete_read_inputs(fixed_end, &proof));
        frozen.provenance = TradeProvenance::ActivityWs;
        let value = DecisionContinuationV3::new(frozen, Some(receipt(7)), pages, None);
        let decoded = DecisionContinuationV3::from_durable(&durable(&value)).unwrap();
        assert_eq!(decoded.version, 3);
        assert_eq!(decoded.page_occurrences().len(), 2);
        assert_eq!(decoded.complete_bound(), Some(receipt(11)));
        assert_eq!(
            decoded.observation_at(1_700_000_000_007),
            Some(ObservationEvidence {
                source_receipt: receipt(7),
                complete_bound_receipt: receipt(11),
                observed_unix_ms: 1_700_000_000_007,
                provenance: "activity_ws".to_owned(),
            })
        );
    }

    /// PASS: polling-only V3 uses the complete page bound, while a legacy V2 row has no fabricated
    /// observation evidence.
    #[test]
    fn poll_only_v3_and_legacy_v2_have_distinct_observation_semantics() {
        let fixed_end = 1_700_000_010_i64;
        let pages = vec![PageOccurrence {
            request_url: activity_request_url(Some(fixed_end.saturating_sub(1)), fixed_end, 0),
            raw_hash: "hash".to_owned(),
            receipt: receipt(4),
        }];
        let proof = proof_for_occurrences(fixed_end, &pages);
        let value = DecisionContinuationV3::new(
            facts(complete_read_inputs(fixed_end, &proof)),
            None,
            pages,
            None,
        );
        let decoded = DecisionContinuationV3::from_durable(&durable(&value)).unwrap();
        assert_eq!(
            decoded.observation_at(1_700_000_000_004),
            Some(ObservationEvidence {
                source_receipt: receipt(4),
                complete_bound_receipt: receipt(4),
                observed_unix_ms: 1_700_000_000_004,
                provenance: "rest_poll".to_owned(),
            })
        );

        let legacy = legacy17_facts(json!({"fixed_end": fixed_end}));
        let mut row = durable(&value);
        row.frozen_inputs_json = legacy_v2_json(&legacy);
        let decoded = DecisionContinuationV3::from_durable(&row).unwrap();
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.observation_at(1_700_000_000_004), None);
    }

    #[test]
    fn from_durable_accepts_pre_545_version_2() {
        let facts = legacy17_facts(json!({"fixed_end": 1_700_000_010_i64}));
        let holder = DecisionContinuationV3::new(facts.clone(), None, Vec::new(), None);
        let mut row = durable(&holder);
        row.frozen_inputs_json = pre_545_frozen_inputs(&facts);

        let decoded = DecisionContinuationV3::from_durable(&row).unwrap();
        assert_eq!(decoded.version(), 2);
        assert_eq!(decoded.facts, facts);
        assert_eq!(
            decoded.facts.applied_configuration.canonical_hash(),
            decoded.facts.applied_configuration_hash
        );
    }

    #[test]
    fn from_durable_refuses_version_2_with_era() {
        let facts = legacy17_facts(json!({"fixed_end": 1_700_000_010_i64}));
        let holder = DecisionContinuationV3::new(facts.clone(), None, Vec::new(), None);
        let mut row = durable(&holder);
        let mut era_bearing = serde_json::to_value(&facts).unwrap();
        era_bearing
            .as_object_mut()
            .unwrap()
            .insert("version".to_owned(), json!(2));

        for era in [json!("legacy17"), Value::Null] {
            let mut document = era_bearing.clone();
            document["applied_configuration"]["era"] = era;
            row.frozen_inputs_json = document.to_string();
            let error = DecisionContinuationV3::from_durable(&row).unwrap_err();
            assert!(matches!(&error, DecisionContinuationError::Json(_)));
            assert!(error.to_string().contains("era"), "{error}");
        }
    }

    #[test]
    fn from_durable_refuses_era_less_version_3_and_4() {
        let facts = legacy17_facts(json!({"fixed_end": 1_700_000_010_i64}));
        let holder = DecisionContinuationV3::new(facts, None, Vec::new(), None);
        let mut row = durable(&holder);
        let mut era_less = serde_json::to_value(&holder).unwrap();
        pre_545_applied_configuration(&mut era_less);

        for version in [3, 4, 5] {
            let mut document = era_less.clone();
            document["version"] = json!(version);
            row.frozen_inputs_json = document.to_string();
            let error = DecisionContinuationV3::from_durable(&row).unwrap_err();
            assert!(matches!(&error, DecisionContinuationError::Json(_)));
            assert!(error.to_string().contains("era"), "{error}");
        }
    }

    /// PASS: polling observation time and hashes are recovered through exact receipt-index reads
    /// even when an unrelated interior frame is unreadable; tampering a frozen raw-page hash
    /// still fails closed.
    /// FAIL: a corrupted or missing retained page is accepted, or a valid retained page is
    /// rejected or resolved against the wrong receipt.
    #[test]
    fn receipt_index_is_the_scoped_observation_time_and_hash_owner() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.log");
        let mut writer = Writer::open(&path).unwrap();
        let append = |writer: &mut Writer, payload: &[u8], received_unix: i64| {
            let time = time::OffsetDateTime::from_unix_timestamp(received_unix).unwrap();
            writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                    schema_version: ACTIVITY_SCHEMA_VERSION,
                    parser_version: ACTIVITY_PARSER_VERSION,
                    observed_at: SourceTimestamp(time),
                    received_at: ReceivedAt(time),
                    content_type: ContentType::Json,
                    payload: payload.to_vec(),
                })
                .unwrap()
        };
        let first_payload = br#"[{"page":1}]"#;
        let unrelated_payload = br#"[{"unrelated":true}]"#;
        let second_payload = br#"[{"page":2}]"#;
        let first = append(&mut writer, first_payload, 1_700_000_001);
        let _unrelated = append(&mut writer, unrelated_payload, 1_700_000_001);
        let second = append(&mut writer, second_payload, 1_700_000_002);
        drop(writer);
        let offsets = Reader::replay_with_offsets(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let source_receipts = SourceReceiptIndex::replay(&path).unwrap();

        // Corrupt only the unrelated frame after boot built the verified index. A complete-log
        // replay now fails, while direct reads of the two receipt-bound page frames remain valid.
        let corrupt_at = offsets[1].0.checked_add(4).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        file.write_all(&[byte[0] ^ 0xff]).unwrap();
        file.sync_all().unwrap();
        assert!(
            Reader::replay(&path).is_err(),
            "complete replay sees the corrupt neighbor"
        );

        let fixed_end = 1_700_000_010_i64;
        let occurrences = vec![
            PageOccurrence {
                request_url: activity_request_url(Some(fixed_end.saturating_sub(1)), fixed_end, 0),
                raw_hash: blake3::hash(first_payload).to_hex().to_string(),
                receipt: first,
            },
            PageOccurrence {
                request_url: activity_request_url(
                    Some(fixed_end.saturating_sub(1)),
                    fixed_end,
                    RECONCILIATION_PAGE_LIMIT,
                ),
                raw_hash: blake3::hash(second_payload).to_hex().to_string(),
                receipt: second,
            },
        ];
        let proof = proof_for_occurrences(fixed_end, &occurrences);
        let value = DecisionContinuationV3::new(
            facts(complete_read_inputs(fixed_end, &proof)),
            None,
            occurrences,
            None,
        );
        let decoded = DecisionContinuationV3::from_durable(&durable(&value)).unwrap();
        assert_eq!(
            decoded
                .observation_from_receipt_index(&source_receipts)
                .unwrap(),
            Some(ObservationEvidence {
                source_receipt: second,
                complete_bound_receipt: second,
                observed_unix_ms: 1_700_000_002_000,
                provenance: "rest_poll".to_owned(),
            })
        );

        let mut tampered = decoded;
        tampered.page_occurrences[1].raw_hash = "00".repeat(32);
        assert!(matches!(
            tampered.observation_from_receipt_index(&source_receipts),
            Err(DecisionContinuationError::SourceReceiptMismatch { .. })
        ));
    }

    /// PASS: the shared owner rejects a continuation whose logical complete-read proof is dropped
    /// or unrelated, whatever version it records — no producer emits page occurrences without
    /// the proof, so there is no one-page compatibility path.
    /// FAIL: a proof-free continuation (current or version two) reconstructs an aggregate.
    #[test]
    fn strict_live_shared_owner_rejects_proof_free_continuations_of_any_version() {
        let payload = br#"[{"proxyWallet":"0x1111111111111111111111111111111111111111","type":"TRADE","conditionId":"0xcondition","asset":"123","side":"BUY","size":"1","usdcSize":"0.5","price":"0.5","timestamp":"1700000000","transactionHash":"0xtransaction","outcomeIndex":"0"}]"#;
        let occurrence = PageOccurrence {
            request_url: "https://source/page".to_owned(),
            raw_hash: blake3::hash(payload).to_hex().to_string(),
            receipt: receipt(1),
        };
        for missing_proof in [json!({}), json!({"unrelated": true})] {
            let current = DecisionContinuationV3::new(
                facts(missing_proof),
                None,
                vec![occurrence.clone()],
                None,
            );
            let mut lookup = |_receipt| -> Result<_, &str> { Ok(activity_page(payload)) };
            let error = current
                .reconstruct_complete_activity_read(&mut lookup)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("decision continuation is missing its complete activity read proof")
            );
        }

        let version_two = DecisionContinuationV3 {
            version: 2,
            source_authority: None,
            facts: facts(json!({"legacy": true})),
            observed_source_receipt: None,
            page_occurrences: vec![occurrence],
            read_commitment: None,
        };
        let mut lookup = |_receipt| -> Result<_, &str> { Ok(activity_page(payload)) };
        let error = version_two
            .reconstruct_complete_activity_read(&mut lookup)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("decision continuation is missing its complete activity read proof")
        );
    }

    /// PASS: V3 decode requires the producer's fixed end and at least one page-evidence record.
    /// FAIL: increasing page receipts alone make a proof-free V3 durable row executable.
    #[test]
    fn v3_decode_requires_rich_complete_read_proof() {
        let payload = b"[]";
        let (occurrence, evidence) =
            activity_page_fixture(payload, Some(1_699_999_999), 1_700_000_000, 0, receipt(1));
        let valid = DecisionContinuationV3::new(
            facts(complete_read_inputs(
                1_700_000_000,
                std::slice::from_ref(&evidence),
            )),
            None,
            vec![occurrence.clone()],
            None,
        );
        DecisionContinuationV3::from_durable(&durable(&valid)).unwrap();

        let pages_without_fixed_end = json!({"pages": [evidence]});
        for decision_inputs in [
            json!({}),
            json!({"fixed_end": 1_700_000_000}),
            json!({"fixed_end": 1_700_000_000, "pages": []}),
            pages_without_fixed_end,
        ] {
            let invalid = DecisionContinuationV3::new(
                facts(decision_inputs),
                None,
                vec![occurrence.clone()],
                None,
            );
            assert!(matches!(
                DecisionContinuationV3::from_durable(&durable(&invalid)),
                Err(DecisionContinuationError::DurableMismatch)
            ));
        }
    }

    /// PASS: the shared owner accepts a source-shaped short terminal page and rejects the same
    /// retained payload when both continuation URL copies are changed together.
    /// FAIL: a URL survives by acting only as an equality key inside the continuation.
    #[test]
    fn complete_read_validates_source_owned_request_url() {
        let payload = br#"[
            {"proxyWallet":"0x1111111111111111111111111111111111111111","type":"TRADE","conditionId":"0xcondition","asset":"123","side":"BUY","size":"1","usdcSize":"0.5","price":"0.5","timestamp":"1700000000","transactionHash":"0xtransaction","outcomeIndex":"0"}
        ]"#;
        let (occurrence, evidence) =
            activity_page_fixture(payload, Some(1_699_999_999), 1_700_000_000, 0, receipt(1));
        let valid = DecisionContinuationV3::new(
            facts(complete_read_inputs(
                1_700_000_000,
                std::slice::from_ref(&evidence),
            )),
            None,
            vec![occurrence.clone()],
            None,
        );
        let mut lookup = |_receipt| -> Result<_, &str> { Ok(activity_page(payload)) };
        assert_eq!(
            valid
                .reconstruct_complete_activity_read(&mut lookup)
                .unwrap()
                .len(),
            1
        );

        let tampered_url = format!("{}&tampered=true", occurrence.request_url);
        let mut tampered_evidence = evidence;
        tampered_evidence.request_url.clone_from(&tampered_url);
        let mut tampered_occurrence = occurrence;
        tampered_occurrence.request_url = tampered_url;
        let tampered = DecisionContinuationV3::new(
            facts(complete_read_inputs(1_700_000_000, &[tampered_evidence])),
            None,
            vec![tampered_occurrence],
            None,
        );
        let error = tampered
            .reconstruct_complete_activity_read(&mut lookup)
            .unwrap_err();
        assert!(error.to_string().contains("source contract"));
    }

    /// PASS: canonical page evidence is recomputed by the producer-owned hash helper, independently
    /// of the raw-byte hash, and a changed canonical digest fails closed.
    /// FAIL: valid raw bytes make a forged canonical page hash irrelevant.
    #[test]
    fn complete_read_verifies_canonical_page_hash() {
        let payload = br#"[ {"proxyWallet":"0x1111111111111111111111111111111111111111","type":"TRADE","conditionId":"0xcondition","asset":"123","side":"BUY","size":"1","usdcSize":"0.5","price":"0.5","timestamp":"1700000000","transactionHash":"0xtransaction","outcomeIndex":"0"} ]"#;
        let (occurrence, mut evidence) =
            activity_page_fixture(payload, Some(1_699_999_999), 1_700_000_000, 0, receipt(1));
        assert_ne!(evidence.canonical_page_hash, evidence.raw_page_hash);
        evidence
            .canonical_page_hash
            .clone_from(&evidence.raw_page_hash);
        let continuation = DecisionContinuationV3::new(
            facts(complete_read_inputs(1_700_000_000, &[evidence])),
            None,
            vec![occurrence],
            None,
        );
        let mut lookup = |_receipt| -> Result<_, &str> { Ok(activity_page(payload)) };
        let error = continuation
            .reconstruct_complete_activity_read(&mut lookup)
            .unwrap_err();
        assert!(error.to_string().contains("canonical page hash differs"));
    }

    /// PASS: no segment may continue to a short page beyond the producer's maximum offset.
    /// FAIL: consecutive full pages through the maximum make an extra short page replayable.
    #[test]
    fn complete_read_rejects_short_page_beyond_maximum_offset() {
        let row = json!({
            "proxyWallet": "0x1111111111111111111111111111111111111111",
            "type": "TRADE",
            "conditionId": "0xcondition",
            "asset": "123",
            "side": "BUY",
            "size": "1",
            "usdcSize": "0.5",
            "price": "0.5",
            "timestamp": "1700000000",
            "transactionHash": "0xtransaction",
            "outcomeIndex": "0"
        });
        let full_payload = serde_json::to_vec(
            &(0..RECONCILIATION_PAGE_LIMIT)
                .map(|_| row.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let terminal_payload = b"[]";
        let terminal_offset = ACTIVITY_MAX_OFFSET + RECONCILIATION_PAGE_LIMIT;
        let mut occurrences = Vec::new();
        let mut evidence = Vec::new();
        let mut offset = 0;
        while offset <= terminal_offset {
            let payload = if offset == terminal_offset {
                terminal_payload.as_slice()
            } else {
                full_payload.as_slice()
            };
            let (occurrence, page) = activity_page_fixture(
                payload,
                Some(1_699_999_999),
                1_700_000_000,
                offset,
                receipt(u64::from(offset / RECONCILIATION_PAGE_LIMIT) + 1),
            );
            occurrences.push(occurrence);
            evidence.push(page);
            offset += RECONCILIATION_PAGE_LIMIT;
        }
        let continuation = DecisionContinuationV3::new(
            facts(complete_read_inputs(1_700_000_000, &evidence)),
            None,
            occurrences,
            None,
        );
        let terminal_sequence = u64::from(terminal_offset / RECONCILIATION_PAGE_LIMIT) + 1;
        let mut lookup = |page: AppendReceipt| -> Result<_, &str> {
            let payload = if page.sequence.0 == terminal_sequence {
                terminal_payload.as_slice()
            } else {
                full_payload.as_slice()
            };
            Ok(activity_page(payload))
        };
        let error = continuation
            .reconstruct_complete_activity_read(&mut lookup)
            .unwrap_err();
        assert!(error.to_string().contains("page evidence is invalid"));
    }
    fn committed_empty_read() -> (DecisionContinuationV3, Vec<u8>) {
        let (page, evidence) = activity_page_fixture(b"[]", None, 100, 0, receipt(2));
        let mut frozen = facts(complete_read_inputs(100, std::slice::from_ref(&evidence)));
        frozen.provenance = TradeProvenance::RestPoll;
        let payload = activity_read_commitment_payload_v1(
            frozen.wallet,
            100,
            std::slice::from_ref(&page),
            &[evidence],
        )
        .unwrap();
        (
            DecisionContinuationV3::new(
                frozen,
                None,
                vec![page],
                Some(crate::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(receipt(3))),
            ),
            payload,
        )
    }

    fn committed_empty_read_v2() -> (DecisionContinuationV3, Vec<u8>) {
        let (mut continuation, _) = committed_empty_read();
        continuation.version = 5;
        continuation.facts.paper_freshness_policy = Some(PaperFreshnessPolicy {
            activity_ws_enabled: true,
            copy_latency_budget_secs: 2,
        });
        let pages = serde_json::from_value::<Vec<ReconciliationPageEvidence>>(
            continuation.facts.decision_inputs["pages"].clone(),
        )
        .unwrap();
        let payload = activity_read_commitment_payload(
            continuation.facts.wallet,
            100,
            &continuation.page_occurrences,
            &pages,
        )
        .unwrap();
        (continuation, payload)
    }

    fn committed_lookup(
        receipt: AppendReceipt,
        commitment_payload: &[u8],
    ) -> Result<CompleteActivityPage, String> {
        let mut source = activity_page(b"[]");
        match receipt.sequence.0 {
            2 => source.schema_version = crate::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
            3 => {
                source.source_id = ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned();
                source.schema_version = u32::from(
                    serde_json::from_slice::<ActivityReadCommitment>(commitment_payload)
                        .unwrap()
                        .version,
                );
                source.parser_version = ACTIVITY_READ_COMMITMENT_PARSER_VERSION;
                source.payload = commitment_payload.to_vec();
            }
            _ => return Err("receipt outside prefix".to_owned()),
        }
        Ok(source)
    }

    /// PASS: V3/V4 decode exactly the two paired provenance/receipt forms; a historical V2
    /// record decodes without a receipt and keeps either provenance.
    /// FAIL: either mismatch decodes, a paired form is rejected, or V2 loses its provenance.
    #[test]
    fn receipt_provenance_pairing_is_bijective() {
        for version in [3, 4, 5] {
            for provenance in [TradeProvenance::RestPoll, TradeProvenance::ActivityWs] {
                for websocket in [None, Some(receipt(1))] {
                    let (mut continuation, _) = committed_empty_read();
                    continuation.version = version;
                    continuation.facts.paper_freshness_policy =
                        (version == 5).then_some(PaperFreshnessPolicy {
                            activity_ws_enabled: false,
                            copy_latency_budget_secs: 2,
                        });
                    continuation.read_commitment = matches!(version, 4 | 5).then(|| receipt(3));
                    continuation.facts.provenance = provenance;
                    continuation.observed_source_receipt = websocket;
                    let result = DecisionContinuationV3::from_durable(&durable(&continuation));
                    if (provenance == TradeProvenance::ActivityWs) == websocket.is_some() {
                        assert!(result.is_ok());
                    } else {
                        assert!(matches!(
                            result,
                            Err(DecisionContinuationError::DurableMismatch)
                        ));
                    }
                }
            }
        }
        // A pre-#545 version-2 record carries no receipt, so both provenances decode without one
        // and the decoded facts keep the provenance the writer froze (#584).
        for provenance in [TradeProvenance::RestPoll, TradeProvenance::ActivityWs] {
            let (mut continuation, _) = committed_empty_read();
            continuation.facts.provenance = provenance;
            let mut legacy = durable(&continuation);
            legacy.frozen_inputs_json = legacy_v2_json(&with_legacy17(continuation.facts));
            let decoded = DecisionContinuationV3::from_durable(&legacy).unwrap();
            assert_eq!(decoded.version(), 2);
            assert_eq!(decoded.facts.provenance, provenance);
            assert!(decoded.observed_source_receipt.is_none());
        }
    }

    /// PASS: missing, early, wrong, unavailable, and downgraded commitments fail at decode or reconstruction.
    /// FAIL: altered commitment identity or schema-3 evidence without V4 reconstructs.
    #[test]
    fn read_commitment_missing_wrong_out_of_prefix_or_downgraded_fails() {
        for (continuation, payload) in [committed_empty_read(), committed_empty_read_v2()] {
            let decoded = DecisionContinuationV3::from_durable(&durable(&continuation)).unwrap();
            assert!(
                decoded
                    .reconstruct_complete_activity_read(&mut |receipt| committed_lookup(
                        receipt, &payload
                    ))
                    .is_ok()
            );
            for commitment in [None, Some(receipt(1)), Some(receipt(2))] {
                let mut changed = continuation.clone();
                changed.read_commitment = commitment;
                assert!(matches!(
                    DecisionContinuationV3::from_durable(&durable(&changed)),
                    Err(DecisionContinuationError::DurableMismatch)
                ));
            }
            let mut wrong = continuation.clone();
            wrong.read_commitment = Some(receipt(4));
            let wrong = DecisionContinuationV3::from_durable(&durable(&wrong)).unwrap();
            assert!(
                wrong
                    .reconstruct_complete_activity_read(&mut |receipt| committed_lookup(
                        receipt, &payload
                    ))
                    .is_err()
            );
            let mut wrong_payload: ActivityReadCommitment =
                serde_json::from_slice(&payload).unwrap();
            wrong_payload.digest = "00".repeat(32);
            assert!(
                continuation
                    .reconstruct_complete_activity_read(&mut |receipt| committed_lookup(
                        receipt,
                        &serde_json::to_vec(&wrong_payload).unwrap()
                    ))
                    .is_err()
            );
            assert!(
                continuation
                    .reconstruct_complete_activity_read(&mut |receipt| {
                        if receipt.sequence.0 > 2 {
                            Err("outside sealed prefix".to_owned())
                        } else {
                            committed_lookup(receipt, &payload)
                        }
                    })
                    .is_err()
            );
            let mut downgraded = continuation.clone();
            downgraded.version = 3;
            assert!(DecisionContinuationV3::from_durable(&durable(&downgraded)).is_err());
            downgraded.read_commitment = None;
            downgraded.facts.paper_freshness_policy = None;
            let downgraded = DecisionContinuationV3::from_durable(&durable(&downgraded)).unwrap();
            assert!(
                downgraded
                    .reconstruct_complete_activity_read(&mut |receipt| committed_lookup(
                        receipt, &payload
                    ))
                    .is_err()
            );
            assert!(
                continuation
                    .reconstruct_complete_activity_read(&mut |receipt| {
                        let mut source = committed_lookup(receipt, &payload)?;
                        if receipt.sequence.0 == 2 {
                            source.schema_version = ACTIVITY_SCHEMA_VERSION;
                        }
                        Ok::<_, String>(source)
                    })
                    .is_err()
            );
        }
    }

    /// PASS: authentic 4/v1 and 5/v2 pairs pass; cross-pairing and schema/parser/domain drift fail.
    #[test]
    fn commitment_generation_matrix_rejects_cross_pairing_and_contract_drift() {
        for (continuation, payload) in [committed_empty_read(), committed_empty_read_v2()] {
            for case in ["cross_pair", "schema", "parser", "domain", "bindings"] {
                let mut changed = continuation.clone();
                let mut document: Value = serde_json::from_slice(&payload).unwrap();
                if case == "cross_pair" {
                    changed.version = if continuation.version == 5 { 4 } else { 5 };
                    changed.facts.paper_freshness_policy =
                        (changed.version == 5).then_some(PaperFreshnessPolicy {
                            activity_ws_enabled: false,
                            copy_latency_budget_secs: 2,
                        });
                }
                if case == "domain" {
                    let pages = serde_json::from_value::<Vec<ReconciliationPageEvidence>>(
                        changed.facts.decision_inputs["pages"].clone(),
                    )
                    .unwrap();
                    document["digest"] = json!(
                        activity_read_digest_versioned(
                            changed.facts.wallet,
                            100,
                            &changed.page_occurrences,
                            &pages,
                            if continuation.version == 5 {
                                None
                            } else {
                                Some(&[])
                            }
                        )
                        .unwrap()
                        .to_hex()
                        .to_string()
                    );
                }
                if case == "bindings" {
                    if continuation.version == 5 {
                        document.as_object_mut().unwrap().remove("bindings");
                    } else {
                        document["bindings"] = Value::Null;
                    }
                }
                let bytes = serde_json::to_vec(&document).unwrap();
                assert!(
                    changed
                        .reconstruct_complete_activity_read(&mut |receipt| {
                            let mut page = committed_lookup(receipt, &bytes)?;
                            if receipt.sequence.0 == 3 {
                                if case == "schema" {
                                    page.schema_version = 9;
                                }
                                if case == "parser" {
                                    page.parser_version = 9;
                                }
                            }
                            Ok::<_, String>(page)
                        })
                        .is_err(),
                    "{case}, continuation {}",
                    continuation.version
                );
            }
        }
    }

    /// PASS: generation five requires a typed bounded policy; legacy generations exclude it.
    #[test]
    fn paper_freshness_policy_decode_is_generation_bound() {
        let (current, _) = committed_empty_read_v2();
        let mut row = durable(&current);
        assert!(DecisionContinuationV3::from_durable(&row).is_ok());
        for policy in [
            Value::Null,
            json!({}),
            json!({"activity_ws_enabled": true, "copy_latency_budget_secs": 0}),
            json!({"activity_ws_enabled": true, "copy_latency_budget_secs": 3601}),
            json!({"activity_ws_enabled": "true", "copy_latency_budget_secs": 2}),
            json!({"activity_ws_enabled": true, "copy_latency_budget_secs": "2"}),
            json!({"activity_ws_enabled": true, "copy_latency_budget_secs": -1}),
            json!({"activity_ws_enabled": true, "copy_latency_budget_secs": 2, "extra": false}),
        ] {
            let mut value = serde_json::to_value(&current).unwrap();
            value["paper_freshness_policy"] = policy;
            row.frozen_inputs_json = value.to_string();
            assert!(DecisionContinuationV3::from_durable(&row).is_err());
        }
        let mut value = serde_json::to_value(&current).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("paper_freshness_policy");
        row.frozen_inputs_json = value.to_string();
        assert!(DecisionContinuationV3::from_durable(&row).is_err());
        for version in [3, 4] {
            let mut value = serde_json::to_value(&current).unwrap();
            value["version"] = json!(version);
            row.frozen_inputs_json = value.to_string();
            assert!(DecisionContinuationV3::from_durable(&row).is_err());
        }
        for budget in [1, 3600] {
            let mut value = serde_json::to_value(&current).unwrap();
            value["paper_freshness_policy"]["copy_latency_budget_secs"] = json!(budget);
            row.frozen_inputs_json = value.to_string();
            assert!(DecisionContinuationV3::from_durable(&row).is_ok());
        }
    }

    /// PASS: producer order cannot change v2 bytes; duplicate stream receipts are rejected.
    #[test]
    fn observation_binding_encoding_is_canonical() {
        let (continuation, _) = committed_empty_read_v2();
        let pages = serde_json::from_value::<Vec<ReconciliationPageEvidence>>(
            continuation.facts.decision_inputs["pages"].clone(),
        )
        .unwrap();
        let first = ObservationBinding {
            counterpart_basis_receipt: None,
            frame_admission_receipt: None,
            stream_group_id: SourceTradeId(format!("g2:{}", "1".repeat(64))),
            stream_receipt: receipt(0),
            history_group_id: SourceTradeId(format!("g2:{}", "2".repeat(64))),
            semantic_revision: "3".repeat(64),
            page_raw_hash: continuation.page_occurrences[0].raw_hash.clone(),
            page_occurrence_index: 0,
            identity_provenance: None,
            identity_receipt: None,
        };
        let mut second = first.clone();
        second.stream_receipt = receipt(1);
        let encode = |bindings: &[ObservationBinding]| {
            activity_read_commitment_payload_v2(
                continuation.facts.wallet,
                100,
                &continuation.page_occurrences,
                &pages,
                bindings,
            )
        };
        assert_eq!(
            encode(&[first.clone(), second.clone()]).unwrap(),
            encode(&[second, first.clone()]).unwrap()
        );
        assert!(encode(&[first.clone(), first]).is_err());
    }

    pub(crate) struct BindingFixture {
        pub(crate) dir: tempfile::TempDir,
        pub(crate) continuation: DecisionContinuationV3,
        pub(crate) index: SourceReceiptIndex,
        pub(crate) aggregate: ActivityAggregate,
        pub(crate) metadata_receipt: AppendReceipt,
    }

    pub(crate) fn binding_fixture(case: &str) -> BindingFixture {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binding.log");
        let mut writer = Writer::open(&path).unwrap();
        let (continuation, aggregate, metadata_receipt) = append_binding_read(case, &mut writer);
        drop(writer);
        let index = SourceReceiptIndex::replay(&path).unwrap();
        BindingFixture {
            dir,
            continuation,
            index,
            aggregate,
            metadata_receipt,
        }
    }

    pub(crate) fn append_binding_read(
        case: &str,
        writer: &mut Writer,
    ) -> (DecisionContinuationV3, ActivityAggregate, AppendReceipt) {
        let condition = match case {
            "recorded_correction" | "shared_recorded_correction" => "stamped",
            "other_target" | "exact_other_target" => "other",
            _ => "new",
        };
        let at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
        let append = |writer: &mut Writer, source: &str, schema, parser, payload: Vec<u8>| {
            writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(source.to_owned()),
                    schema_version: schema,
                    parser_version: parser,
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    content_type: ContentType::Json,
                    payload,
                })
                .unwrap()
        };
        let disposed = case.starts_with("disposed_");
        // An exact stream carries its history row's own stamp; an alias carries another one.
        let (stream_condition, stream_outcome) = match case {
            "alias" | "disposed_alias" => ("alias", if disposed { 0 } else { 1 }),
            "exact" | "exact_digest" | "exact_other_target" => ("new", 0),
            _ => ("old", if disposed { 0 } else { 1 }),
        };
        let stream_payload = serde_json::to_vec(
            &json!({"proxyWallet":"0x1111111111111111111111111111111111111111",
        "conditionId":stream_condition, "asset":"123", "side":"BUY", "size":1, "price":0.5,
        "timestamp":99, "transactionHash":"tx", "outcomeIndex":stream_outcome}),
        )
        .unwrap();
        let stream = parse_activity_trade_observation(&stream_payload).unwrap();
        let stream_receipt = append(
            writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            2,
            2,
            stream_payload.clone(),
        );
        let second_stream = (case == "conflicting_metadata").then(|| {
            append(
                writer,
                crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                2,
                2,
                stream_payload,
            )
        });
        let mut rows = json!([{"proxyWallet":"0x1111111111111111111111111111111111111111",
            "type":"TRADE", "conditionId": condition,
            "asset":"123", "side":"BUY", "size":"1", "usdcSize":"0.5", "price":"0.5",
            "timestamp":100, "transactionHash":"tx", "outcomeIndex":0}]);
        if disposed {
            rows[0]["outcomeIndex"] = json!(999);
            rows[0]["outcome"] = json!("Over");
        }
        if matches!(case, "pair" | "original_only") {
            // The unattributed original, alone or listed with its restamp.
            rows[0]["outcome"] = json!("Over");
            let mut original = rows[0].clone();
            original["outcomeIndex"] = json!(999);
            rows = if case == "pair" {
                json!([original, rows[0].clone()])
            } else {
                json!([original])
            };
        }
        if matches!(
            case,
            "ambiguous_history" | "distinct_legs" | "shared_read" | "shared_recorded_correction"
        ) {
            let mut leg = rows[0].clone();
            leg["conditionId"] = json!("other");
            if case != "ambiguous_history" {
                leg["asset"] = json!("456");
            }
            rows.as_array_mut().unwrap().push(leg);
            if case == "distinct_legs" {
                let mut leg = rows[0].clone();
                leg["side"] = json!("SELL");
                rows.as_array_mut().unwrap().push(leg);
            }
        }
        let raw = serde_json::to_vec(&rows).unwrap();
        let page_receipt = append(
            writer,
            crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
            3,
            2,
            raw.clone(),
        );
        let (page, evidence) = activity_page_fixture(&raw, None, 100, 0, page_receipt);
        let aggregate = parse_complete_activity_page(
            &activity_page(&raw),
            WalletAddress([0x11; 20]),
            "fixture",
        )
        .unwrap()
        .aggregates()
        .unwrap()
        .into_iter()
        .find(|aggregate| {
            let components = aggregate.group_id.components();
            components
                .condition_id
                .as_ref()
                .is_some_and(|stamped| stamped.0 == condition)
                && (case != "pair" || components.outcome == Some(OutcomeId(999)))
                && components
                    .asset
                    .as_ref()
                    .is_some_and(|asset| asset.0 == "123")
                && components.side == Some(Side::Buy)
        })
        .unwrap();
        let metadata = serde_json::to_vec(&json!([{
            "conditionId": if matches!(case, "metadata_wrong_condition" | "other_target" | "exact_other_target") { "other" } else { "new" },
            "clobTokenIds": match case {
                "metadata_unverified" => "[\"456\"]",
                "metadata_wrong_outcome" => "[\"456\",\"123\"]",
                _ => "[\"123\",\"456\"]",
            }
        }]))
        .unwrap();
        let metadata_receipt = append(
            writer,
            if case == "metadata_source" {
                "unrelated.source"
            } else {
                pe_source_polymarket_public::GAMMA_MARKETS_SOURCE_ID
            },
            if case == "metadata_schema" { 2 } else { 1 },
            if case == "metadata_parser" { 2 } else { 1 },
            metadata.clone(),
        );
        let mut binding = ObservationBinding {
            counterpart_basis_receipt: None,
            frame_admission_receipt: None,
            stream_group_id: stream.group_id.key().clone(),
            stream_receipt,
            history_group_id: aggregate.group_id.key().clone(),
            semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
            page_raw_hash: page.raw_hash.clone(),
            page_occurrence_index: 0,
            identity_provenance: Some(crate::asset_identity::IdentityProvenance {
                asset: pe_core_types::PolymarketTokenId("123".to_owned()),
                source_log_sequence: metadata_receipt.sequence.0,
                canonical_page_hash: canonical_page_hash(&metadata).unwrap(),
            }),
            identity_receipt: Some(metadata_receipt),
        };
        match case {
            "absent_history" => {
                binding.history_group_id = SourceTradeId(format!("g2:{}", "f".repeat(64)))
            }
            "revision" => binding.semantic_revision = "00".repeat(32),
            "page_hash" => binding.page_raw_hash = "00".repeat(32),
            "occurrence" | "disposed_invalid_occurrence" => binding.page_occurrence_index = 1,
            "stream_group" => binding.stream_group_id = aggregate.group_id.key().clone(),
            "stream_receipt" => binding.stream_receipt = page_receipt,
            "metadata_hash" => {
                binding
                    .identity_provenance
                    .as_mut()
                    .unwrap()
                    .canonical_page_hash = "00".repeat(32)
            }
            "metadata_sequence" => {
                binding
                    .identity_provenance
                    .as_mut()
                    .unwrap()
                    .source_log_sequence = 0
            }
            "metadata_missing" => binding.identity_receipt = None,
            "exact" | "exact_digest" => {
                binding.identity_provenance = None;
                binding.identity_receipt = None;
            }
            "future_receipt" => binding.stream_receipt = receipt(100),
            _ => {}
        }
        let mut bindings = if matches!(case, "poll_only" | "disposed_no_binding") {
            Vec::new()
        } else {
            vec![binding]
        };
        if let Some(stream_receipt) = second_stream {
            let metadata = br#"[{"conditionId":"other","clobTokenIds":["123","456"]}]"#.to_vec();
            let identity_receipt = append(
                writer,
                pe_source_polymarket_public::GAMMA_MARKETS_SOURCE_ID,
                1,
                1,
                metadata.clone(),
            );
            let mut binding = bindings[0].clone();
            binding.stream_receipt = stream_receipt;
            binding.identity_receipt = Some(identity_receipt);
            let provenance = binding.identity_provenance.as_mut().unwrap();
            provenance.source_log_sequence = identity_receipt.sequence.0;
            provenance.canonical_page_hash = canonical_page_hash(&metadata).unwrap();
            bindings.push(binding);
        }
        let mut payload = activity_read_commitment_payload_v2(
            WalletAddress([0x11; 20]),
            100,
            std::slice::from_ref(&page),
            std::slice::from_ref(&evidence),
            &bindings,
        )
        .unwrap();
        if case == "duplicate_member" {
            payload = String::from_utf8(payload)
                .unwrap()
                .replacen(
                    "\"page_occurrence_index\":0",
                    "\"page_occurrence_index\":1,\"page_occurrence_index\":0",
                    1,
                )
                .into_bytes();
        }
        if case == "duplicate_receipt" {
            let mut value: Value = serde_json::from_slice(&payload).unwrap();
            let binding = value["bindings"][0].clone();
            value["bindings"].as_array_mut().unwrap().push(binding);
            // Authenticate the exact malformed preimage; rejection must be the binding contract.
            let mut preimage = serde_json::to_value(ActivityReadPreimage {
                wallet: WalletAddress([0x11; 20]),
                fixed_end: 100,
                pages: joined_read_pages(
                    std::slice::from_ref(&page),
                    std::slice::from_ref(&evidence),
                )
                .unwrap(),
            })
            .unwrap();
            preimage["bindings"] = value["bindings"].clone();
            let mut hasher = blake3::Hasher::new();
            hasher.update(ACTIVITY_READ_COMMITMENT_DOMAIN);
            hasher.update(&serde_json::to_vec(&preimage).unwrap());
            value["digest"] = json!(hasher.finalize().to_hex().to_string());
            payload = serde_json::to_vec(&value).unwrap();
        }
        if matches!(case, "digest" | "exact_digest" | "unknown_field") {
            let mut value: Value = serde_json::from_slice(&payload).unwrap();
            if case != "unknown_field" {
                value["bindings"][0]["semantic_revision"] = json!("changed");
            } else {
                value["bindings"][0]["unexpected"] = json!(true);
            }
            payload = serde_json::to_vec(&value).unwrap();
        }
        let commitment = append(writer, ACTIVITY_READ_COMMITMENT_SOURCE_ID, 2, 1, payload);
        let mut frozen = facts(complete_read_inputs(100, &[evidence]));
        frozen.source_trade_id = aggregate.group_id.key().clone();
        frozen.semantic_revision = aggregate.semantic_revision.as_str().to_owned();
        frozen.source_epoch = 100;
        frozen.transaction_hash = "tx".to_owned();
        frozen.market_id = MarketId(pe_core_types::VenueMarketId(
            if matches!(case, "other_target" | "exact_other_target") {
                "other"
            } else {
                "new"
            }
            .to_owned(),
        ));
        frozen.provenance = if case == "poll_only" {
            TradeProvenance::RestPoll
        } else {
            TradeProvenance::ActivityWs
        };
        frozen.paper_freshness_policy = Some(PaperFreshnessPolicy {
            activity_ws_enabled: true,
            copy_latency_budget_secs: 2,
        });
        let continuation = DecisionContinuationV3::new(
            frozen,
            (case != "poll_only").then_some(stream_receipt),
            vec![page],
            Some(ActivityReadCommitmentReceipt::BindingsV2(commitment)),
        );
        (continuation, aggregate, metadata_receipt)
    }

    #[test]
    fn retention_epoch_reauthenticates_binding_dependencies_before_publication_and_decision() {
        let mut fixture = binding_fixture("valid");
        let path = fixture.dir.path().join("binding.log");
        let mut writer = Writer::open(&path).unwrap();
        let at = time::OffsetDateTime::from_unix_timestamp(101).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("suffix".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(at),
                received_at: ReceivedAt(at),
                content_type: ContentType::Json,
                payload: b"{}".to_vec(),
            })
            .unwrap();
        drop(writer);
        fixture.index = SourceReceiptIndex::replay(&path).unwrap();
        let continuation = &fixture.continuation;
        let pages: Vec<ReconciliationPageEvidence> =
            serde_json::from_value(continuation.facts.decision_inputs["pages"].clone()).unwrap();
        let read = std::sync::Arc::new(
            verified_read_for_routing(
                continuation.read_commitment.unwrap(),
                continuation.facts.wallet,
                100,
                &continuation.page_occurrences,
                &pages,
                &fixture.index,
            )
            .unwrap(),
        );
        let frontier = read.frontier.as_ref().unwrap().clone();
        fixture
            .index
            .remember_verified_frontier(&frontier, 0)
            .unwrap();
        fixture.index.verify_frame_frontier(&frontier).unwrap();
        let state = binding_state(&fixture);
        let before = state.decision_pending_history().unwrap();
        let mut engine = BucketCommitEngine::load(state.clone(), PositionLedger::new())
            .unwrap()
            .with_source_receipt_index(fixture.index.clone());
        let frames = Reader::replay_with_offsets(&path)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        let suffix = frames.last().unwrap();
        let erased = continuation.observed_source_receipt.unwrap();
        let tail = fixture.index.current_tail_binding().unwrap();
        let authority = pe_event_log::RetentionAuthority {
            format_version: 1,
            epoch: 1,
            advanced_at: 101,
            boundary: pe_event_log::RetentionBoundary {
                sequence: suffix.1,
                offset: suffix.0,
            },
            chain_head: suffix.2.prev_hash,
            pins: frames
                .iter()
                .filter(|(_, sequence, _)| *sequence < suffix.1 && *sequence != erased.sequence)
                .map(|(offset, sequence, frame)| pe_event_log::RetentionPin {
                    sequence: *sequence,
                    offset: *offset,
                    hash: frame.this_hash,
                    predecessor_hash: frame.prev_hash,
                    reducer: false,
                    wallet: None,
                })
                .collect(),
            retained_tail: (&tail).into(),
            feed: Vec::new(),
        };
        authority.write(&path).unwrap();
        fixture.index.install_retention(authority).unwrap();
        assert!(
            fixture
                .index
                .remember_verified_frontier(&frontier, 0)
                .is_err()
        );
        assert!(
            fixture
                .index
                .verify_frame_frontier(&frontier)
                .unwrap_err()
                .to_string()
                .contains("erased")
        );
        assert!(
            engine
                .publish_frontier(frontier, &fixture.index, Some(&read))
                .unwrap_err()
                .contains("erased")
        );
        assert!(engine.verified_frontiers.is_empty());
        let context = BucketDecisionContext {
            verified_read: Some(read),
            applied_configuration: continuation.facts.applied_configuration.clone(),
            decision_inputs_json: continuation.facts.decision_inputs.to_string(),
            page_occurrences: continuation.page_occurrences.clone(),
            observed_source_receipts: HashMap::new(),
            read_commitment: continuation
                .read_commitment
                .map(ActivityReadCommitmentReceipt::BindingsV2),
            reconstruction_quality: continuation.facts.reconstruction_quality,
            signal_config: SignalConfig::default(),
            copy_eligible: true,
            bracket_commit: false,
            recorded_at_unix: 101,
            observation_provenance: HashMap::new(),
            no_copy_dispositions: HashMap::new(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            restamp_twins: HashSet::new(),
            history_status: None,
        };
        assert!(
            engine
                .commit(
                    vec![fixture.aggregate.clone()],
                    &context,
                    continuation.facts.frozen_basis
                )
                .unwrap_err()
                .to_string()
                .contains("erased")
        );
        assert_eq!(state.decision_pending_history().unwrap(), before);
    }

    /// PASS: raw stream/history/metadata evidence authenticates a correction and its oldest clock;
    /// altered binding fields or missing, early, and out-of-prefix receipts fail closed.
    #[test]
    fn observation_bindings_verify_recorded_raw_evidence() {
        let mut accepted_invalid = Vec::new();
        for case in [
            "valid",
            "revision",
            "page_hash",
            "occurrence",
            "stream_group",
            "stream_receipt",
            "metadata_hash",
            "metadata_sequence",
            "metadata_missing",
            "metadata_unverified",
            "metadata_wrong_condition",
            "metadata_wrong_outcome",
            "conflicting_metadata",
            "metadata_source",
            "metadata_schema",
            "metadata_parser",
            "absent_history",
            "ambiguous_history",
            "duplicate_receipt",
            "duplicate_member",
            "recorded_correction",
            "distinct_legs",
            "future_receipt",
            "digest",
            "unknown_field",
            "prefix",
        ] {
            let fixture = binding_fixture(case);
            let BindingFixture {
                continuation,
                index,
                aggregate,
                metadata_receipt,
                ..
            } = &fixture;
            let stream_receipt = continuation.observed_source_receipt.unwrap();
            let result = continuation.reconstruct_complete_activity_read(&mut |receipt| {
                if case == "prefix" && receipt == *metadata_receipt {
                    return Err("outside prefix".to_owned());
                }
                index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
                    .map_err(|error| error.to_string())
            });
            if matches!(case, "valid" | "recorded_correction" | "distinct_legs") {
                let aggregates = result.unwrap();
                assert_eq!(
                    aggregates.len(),
                    if case == "distinct_legs" { 3 } else { 1 }
                );
                assert!(aggregates.contains(aggregate));
                continuation.observation_from_receipt_index(index).unwrap();
                let time = continuation
                    .verify_stream_binding(
                        &continuation.facts.source_trade_id,
                        stream_receipt,
                        &mut |receipt| {
                            index
                                .source_envelope(receipt)
                                .map(CompleteActivityPage::from)
                        },
                    )
                    .unwrap();
                assert_eq!(time.0.unix_timestamp(), 99);
                let source_time = continuation
                    .verified_source_time(&mut |receipt| {
                        index
                            .source_envelope(receipt)
                            .map(CompleteActivityPage::from)
                    })
                    .unwrap();
                assert_eq!(source_time.0, time);
                assert_eq!(source_time.1, aggregate.group_id.components().asset);
                assert_eq!(aggregate.source_time.0.unix_timestamp(), 100);
                let mut poll_selected = continuation.clone();
                poll_selected.observed_source_receipt = None;
                assert_eq!(
                    poll_selected
                        .verified_source_time(&mut |receipt| {
                            index
                                .source_envelope(receipt)
                                .map(CompleteActivityPage::from)
                        })
                        .unwrap(),
                    source_time
                );
            } else if result.is_ok() {
                accepted_invalid.push(case);
            }
        }
        assert!(
            accepted_invalid.is_empty(),
            "accepted invalid bindings: {accepted_invalid:?}"
        );
    }

    /// PASS: literal pre-#588 v1 bytes and digest remain exact and reconstruct successfully.
    #[test]
    fn parent_v1_commitment_bytes_and_digest_are_unchanged() {
        let (continuation, payload) = committed_empty_read();
        let expected = include_bytes!("../tests/fixtures/activity_read_commitment_v1.json");
        let preimage =
            include_bytes!("../tests/fixtures/activity_read_commitment_v1_preimage.json");
        let digest = include_str!("../tests/fixtures/activity_read_commitment_v1.digest");
        // Parent ab120d9997f216a27dfdb0199739df695a65c9ac hashes the v1 domain followed
        // by sorted-key JSON of wallet/fixed_end/joined page pairs, without bindings.
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"prediction-edge/activity-read-commitment/v1");
        hasher.update(preimage);
        assert_eq!(hasher.finalize().to_hex().as_str(), digest);
        assert_eq!(payload, expected);
        let pages: Vec<ReconciliationPageEvidence> =
            serde_json::from_value(continuation.facts.decision_inputs["pages"].clone()).unwrap();
        assert_eq!(
            canonical_json(&ActivityReadPreimage {
                wallet: continuation.facts.wallet,
                fixed_end: 100,
                pages: joined_read_pages(&continuation.page_occurrences, &pages).unwrap(),
            })
            .unwrap(),
            preimage
        );
        assert_eq!(
            activity_read_digest(
                continuation.facts.wallet,
                100,
                &continuation.page_occurrences,
                &pages
            )
            .unwrap()
            .to_hex()
            .as_str(),
            digest
        );
        assert!(
            continuation
                .reconstruct_complete_activity_read(&mut |receipt| committed_lookup(
                    receipt, expected
                ))
                .unwrap()
                .is_empty()
        );
    }

    /// PASS: a nested duplicate binding member is rejected from authentic raw source bytes.
    #[test]
    fn commitment_v2_rejects_duplicate_binding_member() {
        let fixture = binding_fixture("duplicate_member");
        let error = fixture
            .continuation
            .reconstruct_complete_activity_read(&mut |receipt| {
                fixture
                    .index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
            })
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate field `page_occurrence_index`"),
            "{error}"
        );
    }

    /// PASS: the historical typed decoder rejects duplicate digest members, including a correct last digest.
    #[test]
    fn commitment_v1_rejects_duplicate_member() {
        let (continuation, payload) = committed_empty_read();
        let duplicate = String::from_utf8(payload.clone())
            .unwrap()
            .replacen("{", "{\"digest\":\"invalid\",", 1)
            .into_bytes();
        let error = continuation
            .reconstruct_complete_activity_read(&mut |receipt| {
                let mut source = committed_lookup(receipt, &payload)?;
                if receipt.sequence.0 == 3 {
                    source.payload = duplicate.clone();
                }
                Ok::<_, String>(source)
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("duplicate field `digest`"),
            "{error}"
        );
    }

    /// PASS: a bound v5 observation reads each of its four authenticated source frames once.
    #[test]
    fn observation_shared_read_is_reconstructed_once() {
        let fixture = binding_fixture("valid");
        super::continuation_validation_tests::LOOKUPS.with(|count| count.set(0));
        fixture
            .continuation
            .observation_from_receipt_index(&fixture.index)
            .unwrap();
        assert_eq!(
            super::continuation_validation_tests::LOOKUPS.with(std::cell::Cell::get),
            4
        );
        let mut counts = HashMap::new();
        fixture
            .continuation
            .verify_stream_binding(
                &fixture.continuation.facts.source_trade_id,
                fixture.continuation.observed_source_receipt.unwrap(),
                &mut |receipt| {
                    *counts.entry(receipt.sequence).or_insert(0) += 1;
                    fixture
                        .index
                        .source_envelope(receipt)
                        .map(CompleteActivityPage::from)
                },
            )
            .unwrap();
        assert_eq!(counts.len(), 4);
        assert!(counts.values().all(|count| *count == 1));
        let poll = binding_fixture("poll_only");
        super::continuation_validation_tests::LOOKUPS.with(|count| count.set(0));
        poll.continuation
            .observation_from_receipt_index(&poll.index)
            .unwrap();
        assert_eq!(
            super::continuation_validation_tests::LOOKUPS.with(std::cell::Cell::get),
            2
        );
    }

    #[test]
    fn continuation_seven_inherits_corrected_binding_and_earliest_source_time() {
        let fixture = binding_fixture("valid");
        let historical_time = fixture
            .continuation
            .verified_source_time(&mut |receipt| {
                fixture
                    .index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
            })
            .unwrap();
        let current = fixture.continuation.clone().current_paper();
        assert_eq!(current.version(), 7);
        let decoded = DecisionContinuationV3::from_durable(&durable(&current)).unwrap();
        assert_eq!(
            decoded
                .observation_from_receipt_index(&fixture.index)
                .unwrap(),
            fixture
                .continuation
                .observation_from_receipt_index(&fixture.index)
                .unwrap()
        );
        assert_eq!(
            decoded
                .verified_source_time(&mut |receipt| {
                    fixture
                        .index
                        .source_envelope(receipt)
                        .map(CompleteActivityPage::from)
                })
                .unwrap(),
            historical_time
        );

        let mut incompatible = current;
        incompatible.read_commitment = None;
        assert!(DecisionContinuationV3::from_durable(&durable(&incompatible)).is_err());
    }

    #[test]
    fn continuation_authority_round_trip_preserves_historical_bytes() {
        let (mut historical, _) = committed_empty_read_v2();
        for version in 2..=6 {
            historical.version = version;
            historical.source_authority = None;
            let json = serde_json::to_string(&historical).unwrap();
            assert!(!json.contains("source_authority"));
            let decoded: DecisionContinuationV3 = serde_json::from_str(&json).unwrap();
            assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
            assert_eq!(
                decoded.financial_semantic(),
                if version == 6 { 2 } else { 1 }
            );
        }
        DecisionContinuationV3::from_durable(&durable(&historical)).unwrap();
        let legacy_bytes = serde_json::to_string(&historical).unwrap();
        for authority in [SourceAuthority::CompleteRead] {
            let mut current = historical.clone().current_paper();
            current.source_authority = Some(authority);
            let json = serde_json::to_string(&current).unwrap();
            let decoded = DecisionContinuationV3::from_durable(&durable(&current)).unwrap();
            assert_eq!(decoded, current);
            assert_eq!(decoded.financial_semantic(), 3);
            assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
            assert!(json.contains(match authority {
                SourceAuthority::CompleteRead => "\"source_authority\":\"complete_read\"",
                SourceAuthority::ActivityFrame => "\"source_authority\":\"activity_frame\"",
            }));
            for version in 2..=6 {
                let mut mismatched = serde_json::to_value(&current).unwrap();
                mismatched["version"] = json!(version);
                assert!(matches!(
                    DecisionContinuationV3::from_durable(&durable(&mismatched)),
                    Err(DecisionContinuationError::DurableMismatch)
                ));
            }
        }
        let mut missing = historical.current_paper();
        missing.source_authority = None;
        assert!(matches!(
            DecisionContinuationV3::from_durable(&durable(&missing)),
            Err(DecisionContinuationError::DurableMismatch)
        ));
        let mut null = serde_json::to_value(&missing).unwrap();
        null["source_authority"] = Value::Null;
        assert!(matches!(
            DecisionContinuationV3::from_durable(&durable(&null)),
            Err(DecisionContinuationError::DurableMismatch)
        ));
        assert!(!legacy_bytes.contains("source_authority"));
    }

    #[test]
    fn activity_frame_without_admission_evidence_fails_closed() {
        let fixture = binding_fixture("valid");
        let mut frame = fixture.continuation.current_paper();
        frame.source_authority = Some(SourceAuthority::ActivityFrame);
        frame.page_occurrences.clear();
        frame.read_commitment = None;
        frame.facts.decision_inputs = json!({});
        assert!(DecisionContinuationV3::from_durable(&durable(&frame)).is_err());
    }

    pub(crate) fn binding_state(fixture: &BindingFixture) -> Arc<PaperStateDb> {
        let state = Arc::new(PaperStateDb::open(&fixture.dir.path().join("paper.db")).unwrap());
        let continuation = &fixture.continuation;
        let wallet = continuation.facts.wallet;
        state.set_cursor(&wallet, 0).unwrap();
        state
            .install_anchors(&[AnchorInstallRecord {
                repaired_history: Vec::new(),
                expected_fence: None,
                wallet,
                balances: Vec::new(),
                activity_cutoff_unix: 0,
                anchored_at_unix: 0,
                ledger_hash_after: "empty".to_owned(),
                positions_proof_hash: "empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "binding-fixture".to_owned(),
                history_status: None,
                proof_json: "{}".to_owned(),
                recorded_at_unix: 0,
            }])
            .unwrap();
        let aggregates = parse_complete_activity_page(
            &CompleteActivityPage::from(
                fixture
                    .index
                    .source_envelope(continuation.page_occurrences[0].receipt)
                    .unwrap(),
            ),
            wallet,
            "fixture",
        )
        .unwrap()
        .aggregates()
        .unwrap();
        let context = BucketDecisionContext {
            verified_read: None,
            applied_configuration: continuation.facts.applied_configuration.clone(),
            decision_inputs_json: continuation.facts.decision_inputs.to_string(),
            page_occurrences: continuation.page_occurrences.clone(),
            observed_source_receipts: HashMap::from([(
                continuation.facts.source_trade_id.clone(),
                continuation.observed_source_receipt.unwrap(),
            )]),
            read_commitment: continuation
                .read_commitment
                .map(ActivityReadCommitmentReceipt::BindingsV2),
            reconstruction_quality: continuation.facts.reconstruction_quality,
            signal_config: SignalConfig::default(),
            copy_eligible: true,
            bracket_commit: false,
            recorded_at_unix: 100,
            observation_provenance: HashMap::from([(
                continuation.facts.source_trade_id.clone(),
                TradeProvenance::ActivityWs,
            )]),
            no_copy_dispositions: HashMap::new(),
            identity_overrides: if fixture
                .aggregate
                .group_id
                .components()
                .condition_id
                .as_ref()
                .is_some_and(|condition| condition.0 != continuation.facts.market_id.0.0)
            {
                HashMap::from([(
                    continuation.facts.source_trade_id.clone(),
                    IdentityOverride {
                        verified: MarketOutcomeId::new(
                            continuation.facts.market_id.clone(),
                            continuation.facts.outcome_id,
                        ),
                        evidence_hash: canonical_page_hash(
                            &fixture
                                .index
                                .source_envelope(fixture.metadata_receipt)
                                .unwrap()
                                .payload,
                        )
                        .unwrap(),
                    },
                )])
            } else {
                HashMap::new()
            },
            identity_unresolved: HashSet::new(),
            restamp_twins: Default::default(),
            history_status: Some(WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: 100,
            }),
        };
        let result = BucketCommitEngine::load(state.clone(), PositionLedger::new())
            .unwrap()
            .commit_with_freshness_policy(
                aggregates,
                &context,
                continuation.facts.frozen_basis,
                continuation.facts.paper_freshness_policy,
            )
            .unwrap();
        assert!(!result.pending.is_empty());
        state
    }

    #[test]
    fn boot_authenticates_corrected_retirement_and_open_continuation_separately() {
        let fixture = binding_fixture("valid");
        let state = binding_state(&fixture);
        let commitment = fixture.continuation.read_commitment.unwrap();
        let stream = fixture.continuation.observed_source_receipt.unwrap();
        let source = fixture.index.source_envelope(stream).unwrap();
        let observation = parse_activity_trade_observation(&source.payload).unwrap();
        assert!(
            state
                .activity_group_state(observation.group_id.key())
                .unwrap()
                .is_none()
        );
        let rebuilt = crate::trade_poller::rebuild_reconciliation_obligations_with_index(
            &fixture.dir.path().join("binding.log"),
            &state,
            &fixture.index,
        )
        .unwrap();
        assert!(rebuilt.is_empty());
        #[cfg(feature = "scenario")]
        {
            assert_eq!(fixture.index.read_verification_count(commitment), 1);
            assert_eq!(
                fixture.index.binding_verification_counts(commitment),
                [1, 0, 0]
            );
        }
        assert_eq!(
            validate_open_continuations(&state, &fixture.index).unwrap(),
            1
        );
        #[cfg(feature = "scenario")]
        assert_eq!(
            fixture.index.binding_verification_counts(commitment),
            [1, 0, 1]
        );
        state.retire_activity_observation(stream, false).unwrap();
        let fresh = SourceReceiptIndex::replay(&fixture.dir.path().join("binding.log")).unwrap();
        let rebuilt = crate::trade_poller::rebuild_reconciliation_obligations_with_index(
            &fixture.dir.path().join("binding.log"),
            &state,
            &fresh,
        )
        .unwrap();
        assert!(rebuilt.is_empty());
        #[cfg(feature = "scenario")]
        assert_eq!(fresh.read_verification_count(commitment), 0);
    }

    #[test]
    fn boot_mixed_commitment_authenticates_only_ordinary_obligation_receipts() {
        let fixture = binding_fixture("shared_read");
        let path = fixture.dir.path().join("binding.log");
        let initial = fixture.continuation.read_commitment.unwrap();
        let source = fixture.index.source_envelope(initial).unwrap();
        let read: ActivityReadCommitment = serde_json::from_slice(&source.payload).unwrap();
        let ordinary = read.bindings.as_ref().unwrap()[0].clone();
        let proof = read.read_proof.as_ref().unwrap();
        let mut writer = Writer::open(&path).unwrap();
        let append = |writer: &mut Writer, source: &str, schema, parser, payload: Vec<u8>| {
            let at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
            writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(source.to_owned()),
                    schema_version: schema,
                    parser_version: parser,
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    content_type: ContentType::Json,
                    payload,
                })
                .unwrap()
        };
        let stream_payload = fixture
            .index
            .source_envelope(ordinary.stream_receipt)
            .unwrap()
            .payload;
        let mut feed: Value = serde_json::from_slice(&stream_payload).unwrap();
        feed["conditionId"] = json!("feed");
        let feed = serde_json::to_vec(&feed).unwrap();
        let feed_receipt = append(
            &mut writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            2,
            2,
            feed.clone(),
        );
        let admission = append(
            &mut writer,
            crate::frame_admission::FRAME_ADMISSION_SOURCE_ID,
            1,
            1,
            serde_json::to_vec(&crate::frame_admission::FrameAdmissionArtifact {
                version: 1,
                frame_receipt: feed_receipt,
                capture_digest: "a".repeat(64),
                identity: None,
            })
            .unwrap(),
        );
        let mut feed_binding = ordinary.clone();
        feed_binding.stream_receipt = feed_receipt;
        feed_binding.stream_group_id = parse_activity_trade_observation(&feed)
            .unwrap()
            .group_id
            .key()
            .clone();
        feed_binding.frame_admission_receipt = Some(admission);
        let encode = |bindings: &[ObservationBinding]| {
            activity_read_commitment_payload_v2(
                read.wallet,
                read.fixed_end,
                &proof.page_occurrences,
                &proof.pages,
                bindings,
            )
            .unwrap()
        };
        let basis = append(
            &mut writer,
            ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            2,
            1,
            encode(&[feed_binding.clone()]),
        );
        feed_binding.counterpart_basis_receipt = Some(basis);
        let mut second: Value = serde_json::from_slice(&stream_payload).unwrap();
        second["conditionId"] = json!("other");
        second["asset"] = json!("456");
        second["outcomeIndex"] = json!(0);
        second["timestamp"] = json!(100);
        let second = serde_json::to_vec(&second).unwrap();
        let second_receipt = append(
            &mut writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            2,
            2,
            second.clone(),
        );
        let mut second_binding = ordinary.clone();
        second_binding.stream_group_id = parse_activity_trade_observation(&second)
            .unwrap()
            .group_id
            .key()
            .clone();
        let second_target = fixture
            .continuation
            .reconstruct_complete_activity_read(&mut |receipt| {
                fixture
                    .index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
            })
            .unwrap()
            .into_iter()
            .find(|aggregate| aggregate.group_id.key() == &second_binding.stream_group_id)
            .unwrap();
        second_binding.stream_receipt = second_receipt;
        second_binding.history_group_id = second_target.group_id.key().clone();
        second_binding.semantic_revision = second_target.semantic_revision.as_str().to_owned();
        second_binding.identity_provenance = None;
        second_binding.identity_receipt = None;
        let mixed = append(
            &mut writer,
            ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            2,
            1,
            encode(&[ordinary.clone(), second_binding.clone(), feed_binding]),
        );
        drop(writer);
        let index = SourceReceiptIndex::replay(&path).unwrap();
        let filter = HashSet::from([
            (
                ordinary.stream_receipt.sequence,
                ordinary.stream_receipt.this_hash,
            ),
            (second_receipt.sequence, second_receipt.this_hash),
        ]);
        let mut lookups = Vec::new();
        let selected = verified_commitment_bindings_at_depth(
            mixed,
            &mut |receipt| {
                lookups.push(receipt);
                index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
                    .map_err(|error| complete_activity_read_error(error.to_string()))
            },
            0,
            Some(&filter),
        )
        .unwrap();
        assert_eq!(selected.bindings.len(), 2);
        for excluded in [feed_receipt, admission, basis] {
            assert!(
                !lookups.contains(&excluded),
                "unexpected lookup: {excluded:?}"
            );
        }
        lookups.clear();
        let full = verified_commitment_bindings_with_lookup(mixed, &mut |receipt| {
            lookups.push(receipt);
            index
                .source_envelope(receipt)
                .map(CompleteActivityPage::from)
        })
        .unwrap();
        assert_eq!(full.bindings.len(), 3);
        assert!(lookups.contains(&basis));

        let error = verified_commitment_bindings_at_depth(
            mixed,
            &mut |receipt| {
                let mut page = index
                    .source_envelope(receipt)
                    .map(CompleteActivityPage::from)
                    .map_err(|error| complete_activity_read_error(error.to_string()))?;
                if receipt == mixed {
                    let mut payload: Value = serde_json::from_slice(&page.payload).unwrap();
                    payload["digest"] = json!("changed");
                    page.payload = serde_json::to_vec(&payload).unwrap();
                }
                Ok(page)
            },
            0,
            Some(&filter),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("commitment differs from its frozen proof")
        );

        let state = PaperStateDb::open(&fixture.dir.path().join("paper.db")).unwrap();
        state
            .retire_activity_observation(feed_receipt, false)
            .unwrap();
        let rebuilt = crate::trade_poller::rebuild_reconciliation_obligations_with_index(
            &path, &state, &index,
        )
        .unwrap();
        assert_eq!(rebuilt.len(), 2);
        #[cfg(feature = "scenario")]
        {
            assert_eq!(index.read_verification_count(initial), 1);
            assert_eq!(index.read_verification_count(mixed), 1);
            assert_eq!(index.read_verification_count(basis), 0);
            assert_eq!(index.binding_verification_counts(mixed), [0, 1, 0]);
            assert_eq!(index.frame_verification_count(feed_receipt), 0);
        }
    }

    /// PASS: boot shares one authenticated v5 read across a stream decision and a poll decision.
    #[test]
    fn boot_bound_shared_read_is_reconstructed_once() {
        for case in ["shared_read", "shared_recorded_correction"] {
            let fixture = binding_fixture(case);
            let state = binding_state(&fixture);
            super::continuation_validation_tests::LOOKUPS.with(|count| count.set(0));
            assert_eq!(
                validate_open_continuations(&state, &fixture.index).unwrap(),
                2
            );
            assert_eq!(
                super::continuation_validation_tests::LOOKUPS.with(std::cell::Cell::get),
                4
            );
        }
    }

    /// PASS: even a matching durable correction cannot substitute an identity absent from shared-read metadata.
    #[test]
    fn boot_shared_binding_authenticates_each_effective_identity() {
        let fixture = binding_fixture("shared_read");
        let state = binding_state(&fixture);
        validate_open_continuations(&state, &fixture.index).unwrap();
        let id = &fixture.continuation.facts.source_trade_id;
        let row = state.decision_pending_for(id).unwrap().unwrap();
        let mut continuation = DecisionContinuationV3::from_durable(&row).unwrap();
        continuation.facts.market_id = MarketId(pe_core_types::VenueMarketId("unbound".to_owned()));
        let mutation = LedgerMutation::from_activity(&fixture.aggregate)
            .unwrap()
            .with_verified_identity(
                MarketOutcomeId::new(
                    continuation.facts.market_id.clone(),
                    continuation.facts.outcome_id,
                ),
                canonical_page_hash(
                    &fixture
                        .index
                        .source_envelope(fixture.metadata_receipt)
                        .unwrap()
                        .payload,
                )
                .unwrap(),
            );
        let proof = AppliedEffect {
            effect: mutation.effect,
            clamped_residual: None,
        }
        .to_document()
        .unwrap();
        let conn = rusqlite::Connection::open(fixture.dir.path().join("paper.db")).unwrap();
        conn.execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![serde_json::to_string(&continuation).unwrap(), id.0],
        )
        .unwrap();
        conn.execute(
            "UPDATE activity_groups SET proof_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![proof, id.0],
        )
        .unwrap();
        let error = validate_open_continuations(&state, &fixture.index).unwrap_err();
        assert_eq!(error.source_trade_id.as_ref(), Some(id));
        assert!(
            error
                .cause
                .contains("binding metadata differs from the effective target identity"),
            "{error}"
        );
    }

    /// PASS: generation substitutions fail at boot and the indexed observation owner used on resume.
    #[test]
    fn boot_and_resume_refuse_v5_downgrade_and_substitution() {
        for case in ["v4", "v3", "substitute_receipt"] {
            let fixture = binding_fixture("valid");
            let state = binding_state(&fixture);
            assert_eq!(
                validate_open_continuations(&state, &fixture.index).unwrap(),
                1
            );
            let mut continuation =
                DecisionContinuationV3::from_durable(&state.open_decision_pending().unwrap()[0])
                    .unwrap();
            match case {
                "v4" => {
                    continuation.version = 4;
                    continuation.facts.paper_freshness_policy = None;
                }
                "v3" => {
                    continuation.version = 3;
                    continuation.facts.paper_freshness_policy = None;
                    continuation.read_commitment = None;
                }
                "substitute_receipt" => {
                    continuation.read_commitment = Some(fixture.metadata_receipt)
                }
                _ => {}
            }
            let frozen = serde_json::to_string(&continuation).unwrap();
            rusqlite::Connection::open(fixture.dir.path().join("paper.db"))
                .unwrap()
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1",
                    [frozen],
                )
                .unwrap();
            assert!(
                validate_open_continuations(&state, &fixture.index).is_err(),
                "{case}"
            );
            assert!(
                continuation
                    .observation_from_receipt_index(&fixture.index)
                    .is_err(),
                "{case}"
            );
            assert_eq!(state.open_decision_pending().unwrap().len(), 1);
        }
    }

    /// PASS: authentic v5 policy survives checkpoint/terminal replay; malformed policy and any v2 policy fail both owners.
    #[test]
    fn freshness_policy_is_checked_by_checkpoint_restore_and_row_replay() {
        use crate::decision_replay::{
            AuthorityEvidence, DecisionEvidenceAccumulator, TerminalDispositionEvidence,
            replay_decision_pending,
        };
        let fixture = binding_fixture("valid");
        fixture
            .continuation
            .observation_from_receipt_index(&fixture.index)
            .unwrap();
        let current = durable(&fixture.continuation);
        let mut legacy = durable(&fixture.continuation);
        let facts = with_legacy17(fixture.continuation.facts.clone());
        let mut legacy_facts = facts.clone();
        legacy_facts.paper_freshness_policy = None;
        legacy.frozen_inputs_json = legacy_v2_json(&legacy_facts);
        assert_eq!(
            DecisionContinuationV3::from_durable(&legacy)
                .unwrap()
                .version(),
            2
        );
        for base in [current, legacy] {
            let continuation = DecisionContinuationV3::from_durable(&base).unwrap();
            let evidence = DecisionEvidenceAccumulator::historical(&continuation.facts);
            let mut checkpoint = DecisionEvidenceAccumulator::historical(&continuation.facts);
            if continuation.version() == 5 {
                checkpoint
                    .record_precise_clock(
                        "paper_prepared_staleness_gate",
                        time::OffsetDateTime::from_unix_timestamp(101).unwrap(),
                    )
                    .unwrap();
            }
            let mut valid = base.clone();
            valid.post_commit_inputs_json = checkpoint.checkpoint_json().unwrap();
            DecisionEvidenceAccumulator::from_pending_checkpoint(&valid).unwrap();
            let terminal = evidence
                .render(
                    AuthorityEvidence::not_read("fixture"),
                    TerminalDispositionEvidence::no_copy("fixture"),
                )
                .unwrap();
            valid.state = DecisionPendingState::Terminal;
            valid.terminal_disposition = Some("no_copy:fixture".to_owned());
            valid.post_commit_inputs_json = terminal.clone();
            replay_decision_pending(&valid).unwrap();
            for policy in [
                None,
                Some(Value::Null),
                Some(json!({})),
                Some(json!({"activity_ws_enabled":true,"copy_latency_budget_secs":0})),
                Some(json!({"activity_ws_enabled":true,"copy_latency_budget_secs":3601})),
                Some(json!({"activity_ws_enabled":"true","copy_latency_budget_secs":2})),
                Some(json!({"activity_ws_enabled":true,"copy_latency_budget_secs":"2"})),
                Some(json!({"activity_ws_enabled":true,"copy_latency_budget_secs":-1})),
                Some(
                    json!({"activity_ws_enabled":true,"copy_latency_budget_secs":2,"extra":false}),
                ),
                Some(json!({"activity_ws_enabled":true,"copy_latency_budget_secs":2})),
            ] {
                if (continuation.version() == 5
                    && policy
                        == Some(json!({"activity_ws_enabled":true,"copy_latency_budget_secs":2})))
                    || (continuation.version() == 2 && policy.is_none())
                {
                    continue;
                }
                let mut value: Value = serde_json::from_str(&base.frozen_inputs_json).unwrap();
                match policy {
                    Some(policy) => value["paper_freshness_policy"] = policy,
                    None => {
                        value
                            .as_object_mut()
                            .unwrap()
                            .remove("paper_freshness_policy");
                    }
                }
                let mut changed = base.clone();
                changed.frozen_inputs_json = value.to_string();
                changed.post_commit_inputs_json = checkpoint.checkpoint_json().unwrap();
                assert!(DecisionEvidenceAccumulator::from_pending_checkpoint(&changed).is_err());
                changed.state = DecisionPendingState::Terminal;
                changed.terminal_disposition = Some("no_copy:fixture".to_owned());
                changed.post_commit_inputs_json = terminal.clone();
                assert!(replay_decision_pending(&changed).is_err());
            }
        }
    }

    /// PASS: `same_complete_read` binds every field that defines one complete read.
    /// FAIL: a changed commitment, logical proof, occurrence list, or wallet still compares equal.
    #[test]
    fn same_complete_read_binds_every_read_field() {
        let (first, _payload) = committed_empty_read();
        let mut second = first.clone();
        second.facts.source_trade_id = SourceTradeId("g2:second".to_owned());
        assert!(first.same_complete_read(&second));
        second.version = 5;
        assert!(!first.same_complete_read(&second));
        second = first.clone();
        second.read_commitment = Some(receipt(4));
        assert!(!first.same_complete_read(&second));
        second = first.clone();
        second.facts.decision_inputs["fixed_end"] = json!(101);
        assert!(!first.same_complete_read(&second));
        second = first.clone();
        second.facts.wallet = WalletAddress([2; 20]);
        assert!(!first.same_complete_read(&second));
        second = first.clone();
        second.page_occurrences[0].receipt = receipt(1);
        assert!(!first.same_complete_read(&second));
    }

    /// PASS: a verified websocket receipt binds both wallet and trade in the dispatch path.
    /// FAIL: a wrong-wallet or wrong-trade receipt is accepted despite valid envelope hashes.
    #[test]
    fn websocket_receipt_binds_wallet_and_trade() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("websocket.log");
        let mut writer = Writer::open(&path).unwrap();
        let websocket = json!({"topic":"activity","type":"trades","payload": {
            "proxyWallet":"0x1111111111111111111111111111111111111111",
            "conditionId":"0xcondition", "asset":"123", "side":"BUY", "size":1,
            "price":0.5, "timestamp":1700000000, "transactionHash":"0xtransaction", "outcomeIndex":0
        }});
        let payload = serde_json::to_vec(&websocket["payload"]).unwrap();
        let activity = parse_activity_trade_observation(&payload).unwrap();
        let ws = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                content_type: ContentType::Json,
                payload,
            })
            .unwrap();
        let page = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                content_type: ContentType::Json,
                payload: b"[]".to_vec(),
            })
            .unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&path).unwrap();
        let (occurrence, proof) = activity_page_fixture(b"[]", None, 1700000000, 0, page);
        let mut frozen = facts(complete_read_inputs(1700000000, &[proof]));
        frozen.provenance = TradeProvenance::ActivityWs;
        frozen.source_trade_id = activity.group_id.key().clone();
        let continuation = DecisionContinuationV3::new(frozen, Some(ws), vec![occurrence], None);
        assert!(
            continuation
                .observation_from_receipt_index(&index)
                .unwrap()
                .is_some()
        );
        let mut wrong = continuation.clone();
        wrong.facts.wallet = WalletAddress([2; 20]);
        assert!(matches!(
            wrong.observation_from_receipt_index(&index),
            Err(DecisionContinuationError::SourceReceiptMismatch { .. })
        ));
        wrong = continuation;
        wrong.facts.source_trade_id = SourceTradeId("g2:wrong".to_owned());
        assert!(matches!(
            wrong.observation_from_receipt_index(&index),
            Err(DecisionContinuationError::SourceReceiptMismatch { .. })
        ));
    }
}

#[cfg(test)]
mod activity_exemption_tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{OutcomeId, ReceivedAt, SourceId, SourceTimestamp, VenueMarketId};
    use pe_paper_state::AnchorInstallRecord;
    use pe_source_polymarket_public::{
        ActivityParseContext, ActivityTransport, parse_activity_response,
    };
    use serde_json::json;

    use super::*;

    fn fixture() -> (tempfile::TempDir, Arc<PaperStateDb>, BucketCommitEngine) {
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        paper.set_cursor(&wallet, 0).unwrap();
        paper
            .install_anchors(&[AnchorInstallRecord {
                wallet,
                balances: Vec::new(),
                activity_cutoff_unix: 0,
                anchored_at_unix: 0,
                ledger_hash_after: "empty".to_owned(),
                positions_proof_hash: "empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "fixture".to_owned(),
                history_status: None,
                proof_json: "{}".to_owned(),
                recorded_at_unix: 0,
                repaired_history: Vec::new(),
                expected_fence: None,
            }])
            .unwrap();
        let engine = BucketCommitEngine::load(paper.clone(), PositionLedger::new()).unwrap();
        (dir, paper, engine)
    }

    fn group(
        kind: &str,
        transaction: &str,
        epoch: i64,
        outcome: u16,
        combo: bool,
        condition: &str,
    ) -> ActivityAggregate {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        parse_activity_response(
            &serde_json::to_vec(&json!([{
                "proxyWallet": wallet.to_string(), "timestamp": epoch, "conditionId": condition,
                "type": kind, "size": if condition.is_empty() { "0" } else { "2" }, "usdcSize": "1", "transactionHash": transaction,
                "price": "0.5", "asset": if kind == "TRADE" { "asset" } else { "" },
                "side": if kind == "TRADE" { "BUY" } else { "" },
                "outcomeIndex": outcome, "outcome": if outcome == 999 && kind != "TRADE" { "" } else { "Yes" },
                "isCombo": combo,
            }])).unwrap(),
            wallet,
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(time::OffsetDateTime::UNIX_EPOCH),
                transport: ActivityTransport::Rest,
            },
        ).unwrap().aggregates().unwrap().remove(0)
    }

    fn context() -> BucketDecisionContext {
        BucketDecisionContext {
            verified_read: None,
            applied_configuration: synthetic_legacy17_runtime_config(),
            decision_inputs_json: "{}".to_owned(),
            page_occurrences: Vec::new(),
            observed_source_receipts: HashMap::new(),
            read_commitment: None,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            signal_config: SignalConfig::default(),
            copy_eligible: false,
            bracket_commit: false,
            recorded_at_unix: 110,
            observation_provenance: HashMap::new(),
            no_copy_dispositions: HashMap::new(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            restamp_twins: HashSet::new(),
            history_status: None,
        }
    }

    fn basis() -> FrozenDecisionBasis {
        FrozenDecisionBasis {
            win_rate_p: pe_core_types::Probability::ZERO,
            bankroll: rust_decimal::Decimal::ZERO,
        }
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn frame_admission_ignored_lines() {
        use crate::frame_admission::{EarlierFrame, FeedLatchBasis};
        use pe_event_log::{EnvelopeIn, Writer};

        for case in [
            "earlier_receipt",
            "open_continuation",
            "terminal_continuation",
            "rest_owned",
            "market_consumed",
            "not_entry",
            "not_copy_eligible",
        ] {
            for shape in ["buy", "sell", "zero", "combo"] {
                let (dir, paper, mut engine) = fixture();
                let wallet = WalletAddress([0xaa; 20]);
                let market = MarketId(VenueMarketId("market".to_owned()));
                let at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
                let live = crate::live_watchlist::LiveWatchlist::new(pe_trader_index::Watchlist {
                    entries: vec![pe_trader_index::WatchlistEntry {
                        wallet,
                        tier: pe_trader_index::WatchlistTier::Active,
                        leader_score_bps: pe_core_types::BasisPoints(100),
                        lcb_5pct_bps: pe_core_types::BasisPoints(100),
                        win_rate_bps: pe_core_types::BasisPoints(7000),
                        closed_trades_in_window: 90,
                        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                    }],
                    snapshot_at: SourceTimestamp(at),
                    active_count: 1,
                    incubator_count: 0,
                });
                let raw = json!({
                    "proxyWallet": wallet.to_string(), "timestamp": 100, "conditionId": "market",
                    "type": "TRADE", "size": if shape == "zero" { "0" } else { "2" },
                    "usdcSize": "1", "transactionHash": "frame", "price": "0.5", "asset": "asset",
                    "side": if shape == "sell" { "SELL" } else { "BUY" }, "outcomeIndex": 0,
                    "outcome": "Yes", "isCombo": shape == "combo",
                });
                let payload = serde_json::to_vec(&raw).unwrap();
                let observation = parse_activity_trade_observation(&payload).unwrap();
                let id = observation.group_id.key().clone();
                let source_path = dir.path().join("frames.log");
                let mut writer = Writer::open(&source_path).unwrap();
                let envelope = || EnvelopeIn {
                    source_id: SourceId(crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned()),
                    schema_version: ACTIVITY_SCHEMA_VERSION,
                    parser_version: ACTIVITY_PARSER_VERSION,
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    content_type: ContentType::Json,
                    payload: payload.clone(),
                };
                let earlier_receipt = writer.append_synced(envelope()).unwrap();
                let receipt = writer.append_synced(envelope()).unwrap();
                if case == "not_copy_eligible" {
                    assert_eq!(live.snapshot().entries.len(), 1);
                    live.remove_fenced(&HashSet::from([wallet]));
                    assert!(live.snapshot().entries.is_empty());
                }
                drop(writer);
                let index = SourceReceiptIndex::replay(&source_path).unwrap();

                if matches!(
                    case,
                    "earlier_receipt"
                        | "open_continuation"
                        | "terminal_continuation"
                        | "rest_owned"
                        | "market_consumed"
                ) {
                    let consumed = case == "market_consumed";
                    let durable_id = if consumed {
                        group("TRADE", "consuming", 100, 0, false, "market")
                            .group_id
                            .key()
                            .clone()
                    } else {
                        id.clone()
                    };
                    let pending = case.ends_with("continuation") || case == "earlier_receipt";
                    paper
                        .commit_activity_bucket(&ActivityBucketCommit {
                            wallet,
                            source_epoch: 100,
                            dispositions: vec![ActivityDispositionRecord {
                                source_trade_id: durable_id.clone(),
                                transaction_hash: if consumed { "consuming" } else { "frame" }
                                    .to_owned(),
                                wallet,
                                source_epoch: 100,
                                semantic_revision: "rest-revision".to_owned(),
                                activity_type: "TRADE".to_owned(),
                                disposition: "raw_only".to_owned(),
                                proof_json: "{}".to_owned(),
                                no_copy: None,
                            }],
                            leader_positions: Vec::new(),
                            gate_results: Vec::new(),
                            history_effects: if consumed {
                                vec![MarketHistoryRecord {
                                    wallet,
                                    market_id: market.clone(),
                                    first_epoch: 100,
                                    source_trade_id: durable_id.clone(),
                                }]
                            } else {
                                Vec::new()
                            },
                            history_status: None,
                            pending: if pending {
                                vec![DecisionPendingRecord {
                                    source_trade_id: durable_id,
                                    semantic_revision: "continuation-revision".to_owned(),
                                    wallet,
                                    source_epoch: 100,
                                    frozen_inputs_json: "{}".to_owned(),
                                    updated_at_unix: 100,
                                }]
                            } else {
                                Vec::new()
                            },
                            fence: None,
                            reanchor: None,
                            advance_cursor: false,
                        })
                        .unwrap();
                    if case == "terminal_continuation" {
                        paper
                            .close_decision_pending(&id, "{}", "no_copy:fixture", 100)
                            .unwrap();
                    }
                    engine.entry_gate =
                        CopyEntryGate::new(CopyEntryGateConfig, paper.gate_history().unwrap());
                }
                if case == "earlier_receipt" {
                    engine.observe_frame(EarlierFrame {
                        receipt: earlier_receipt,
                        wallet,
                        source_trade_id: id.clone(),
                        market: market.clone(),
                        received_at: at,
                        unresolved_buy: true,
                    });
                }
                // Empty confirmed inventory keeps Entry classification after membership removal
                // changes the orchestrator context to quality zero and copy_eligible false.
                let mut positions = HashMap::new();
                if matches!(case, "market_consumed" | "not_entry") {
                    positions.insert(
                        MarketOutcomeId::new(market.clone(), OutcomeId(0)),
                        PositionState {
                            long_contracts: ShareAmount::from_atomic(3_000_000),
                            short_contracts: ShareAmount::ZERO,
                        },
                    );
                }
                engine.ledger.replace_wallet_snapshot(wallet, positions);
                let quality = live
                    .snapshot()
                    .entries
                    .first()
                    .map_or(ReconstructionQuality::new(0).unwrap(), |entry| {
                        entry.reconstruction_quality
                    });
                let context = || FrameAdmissionContext {
                    admitted_at: at,
                    stale_secs: 90,
                    quality,
                    signal_config: SignalConfig::default(),
                    copy_eligible: false,
                    configuration: synthetic_legacy17_runtime_config(),
                    basis: basis(),
                    latch: FeedLatchBasis::default(),
                    paper_prefix: None,
                    identity: None,
                    freshness_policy: PaperFreshnessPolicy {
                        activity_ws_enabled: true,
                        copy_latency_budget_secs: 120,
                    },
                };
                let logs = CapturedLogs::default();
                let subscriber = tracing_subscriber::fmt()
                    .json()
                    .without_time()
                    .with_ansi(false)
                    .with_writer(logs.clone())
                    .finish();
                tracing::subscriber::with_default(subscriber, || {
                    assert!(matches!(
                        engine
                            .prepare_activity_frame(receipt, &index, context())
                            .unwrap(),
                        FrameRoute::Ignored
                    ));
                    let lines = String::from_utf8(logs.0.lock().unwrap().clone())
                        .unwrap()
                        .lines()
                        .map(|line| serde_json::from_str::<Value>(line).unwrap())
                        .filter(|line| line["fields"]["message"] == "frame admission ignored")
                        .collect::<Vec<_>>();
                    if shape == "buy" {
                        assert_eq!(lines.len(), 1, "{case}");
                        let fields = &lines[0]["fields"];
                        let reason = if case == "earlier_receipt" || case.ends_with("continuation")
                        {
                            "identity_seen"
                        } else {
                            case
                        };
                        assert_eq!(lines[0]["level"], "INFO");
                        assert_eq!(fields["reason"], reason);
                        assert_eq!(fields["receipt_sequence"], receipt.sequence.0);
                        assert_eq!(
                            fields["receipt_hash"],
                            receipt.this_hash.to_hex().to_string()
                        );
                        assert_eq!(fields["wallet"], wallet.to_string());
                        assert_eq!(fields["market"], market.to_string());
                        assert_eq!(fields["outcome"], 0);
                        assert_eq!(fields["source_trade_id"], id.0);
                        match case {
                            "earlier_receipt" => {
                                assert_eq!(
                                    fields["earlier_receipt_sequence"],
                                    earlier_receipt.sequence.0
                                );
                                assert_eq!(
                                    fields["earlier_receipt_hash"],
                                    earlier_receipt.this_hash.to_hex().to_string()
                                );
                                assert!(fields["continuation_source_trade_id"].is_null());
                            }
                            "open_continuation" | "terminal_continuation" => {
                                assert_eq!(fields["continuation_source_trade_id"], id.0);
                                assert_eq!(
                                    fields["continuation_semantic_revision"],
                                    "continuation-revision"
                                );
                            }
                            "rest_owned" => {
                                assert_eq!(fields["semantic_revision"], "rest-revision");
                                assert_eq!(fields["disposition"], "raw_only");
                            }
                            "market_consumed" => {
                                let history = paper
                                    .market_history_record(&wallet, &market)
                                    .unwrap()
                                    .unwrap();
                                assert_eq!(
                                    fields["consuming_source_trade_id"],
                                    history.source_trade_id.0
                                );
                                assert_eq!(fields["first_epoch"], history.first_epoch);
                            }
                            "not_entry" => {
                                assert_eq!(fields["action"], "Add");
                                assert_eq!(fields["balance"], "3");
                                assert_eq!(fields["short_balance"], "0");
                            }
                            _ => assert_eq!(fields["action"], "Entry"),
                        }
                    } else {
                        assert!(lines.is_empty(), "{case}/{shape}: {lines:?}");
                    }
                    logs.0.lock().unwrap().clear();
                    engine.routed_frame_receipts.insert(receipt.sequence);
                    assert!(matches!(
                        engine
                            .prepare_activity_frame(receipt, &index, context())
                            .unwrap(),
                        FrameRoute::Ignored
                    ));
                    assert!(
                        logs.0.lock().unwrap().is_empty(),
                        "already routed {case}/{shape}"
                    );
                });
            }
        }
    }

    #[test]
    fn all_twins_precede_late_covered_and_partial_routing_without_effects() {
        for covered in [false, true] {
            for alongside_recorded in [false, true] {
                let (_dir, paper, mut engine) = fixture();
                let original = group("TRADE", "trade", 100, 999, false, "market");
                let wallet = original.group_id.components().wallet;
                let mut initial = context();
                initial.identity_overrides.insert(
                    original.group_id.key().clone(),
                    IdentityOverride {
                        verified: MarketOutcomeId::new(
                            MarketId(VenueMarketId("market".to_owned())),
                            OutcomeId(0),
                        ),
                        evidence_hash: "gamma".to_owned(),
                    },
                );
                engine
                    .commit(vec![original.clone()], &initial, basis())
                    .unwrap();
                let redeem = group("REDEEM", "redeem", 100, 0, false, "other");
                let redeem_original = group("REDEEM", "redeem", 100, 999, false, "other");
                // The recorded sibling is raw history evidence, independent of the new twin route.
                paper
                    .commit_activity_bucket(&ActivityBucketCommit {
                        wallet,
                        source_epoch: 100,
                        dispositions: vec![
                            activity_record(
                                &redeem_original,
                                "raw_only".to_owned(),
                                &LedgerEffect::RequiresAnchor,
                                None,
                                None,
                            )
                            .unwrap(),
                        ],
                        leader_positions: Vec::new(),
                        gate_results: Vec::new(),
                        history_effects: Vec::new(),
                        history_status: None,
                        pending: Vec::new(),
                        fence: None,
                        reanchor: None,
                        advance_cursor: false,
                    })
                    .unwrap();
                if covered {
                    paper
                        .install_anchors(&[AnchorInstallRecord {
                            wallet,
                            balances: Vec::new(),
                            activity_cutoff_unix: 100,
                            anchored_at_unix: 100,
                            ledger_hash_after: "empty".to_owned(),
                            positions_proof_hash: "empty".to_owned(),
                            activity_bounds_json: "[]".to_owned(),
                            source_log_generation: "fixture".to_owned(),
                            history_status: None,
                            proof_json: "{}".to_owned(),
                            recorded_at_unix: 100,
                            repaired_history: Vec::new(),
                            expected_fence: None,
                        }])
                        .unwrap();
                }
                let twin = group("TRADE", "trade", 100, 0, false, "market");
                let mut context = context();
                context
                    .restamp_twins
                    .extend([twin.group_id.key().clone(), redeem.group_id.key().clone()]);
                let mut bucket = vec![twin.clone(), redeem.clone()];
                if alongside_recorded {
                    bucket.push(original);
                }
                let ledger_before = engine.ledger().snapshots().clone();
                let history_before = paper.gate_history().unwrap();
                let coverage_before = paper.wallet_coverage(&wallet).unwrap();
                let result = engine.commit(bucket, &context, basis()).unwrap();
                assert_eq!(result.dispositions.len(), 2);
                assert!(
                    result
                        .dispositions
                        .values()
                        .all(|disposition| disposition == "raw_only")
                );
                assert_eq!(result.newly_fenced, None);
                assert!(result.pending.is_empty());
                assert_eq!(engine.ledger().snapshots(), &ledger_before);
                assert_eq!(paper.gate_history().unwrap(), history_before);
                assert_eq!(paper.wallet_coverage(&wallet).unwrap(), coverage_before);
                assert_eq!(paper.cursor(&wallet).unwrap(), Some(100));
                assert!(!paper.is_wallet_fenced(&wallet).unwrap());
                // A repeated all-twin bucket follows the already-committed contract.
                let repeat = engine.commit(vec![twin, redeem], &context, basis());
                assert!(repeat.unwrap().already_committed);
            }
        }
    }

    #[test]
    fn known_redemptions_and_combos_are_raw_only_only_on_the_ordinary_path() {
        for (kind, combo) in [("REDEEM", false), ("TRADE", true), ("REDEEM", true)] {
            for arrival in ["ordinary", "late", "covered", "partial"] {
                let (_dir, paper, mut engine) = fixture();
                let first = group("TRADE", "recorded", 100, 0, false, "first");
                let wallet = first.group_id.components().wallet;
                if arrival != "ordinary" {
                    engine
                        .commit(vec![first.clone()], &context(), basis())
                        .unwrap();
                }
                let epoch = if arrival == "ordinary" { 101 } else { 100 };
                let exemption = group(kind, "new", epoch, 999, combo, "resolved");
                let id = exemption.group_id.key().clone();
                let mut context = context();
                if arrival == "covered" {
                    // Coverage can advance without adding a new activity record.
                    paper
                        .install_anchors(&[AnchorInstallRecord {
                            wallet,
                            balances: Vec::new(),
                            activity_cutoff_unix: 100,
                            anchored_at_unix: 100,
                            ledger_hash_after: "empty".to_owned(),
                            positions_proof_hash: "empty".to_owned(),
                            activity_bounds_json: "[]".to_owned(),
                            source_log_generation: "fixture".to_owned(),
                            history_status: None,
                            proof_json: "{}".to_owned(),
                            recorded_at_unix: 100,
                            repaired_history: Vec::new(),
                            expected_fence: None,
                        }])
                        .unwrap();
                }
                let bucket = if arrival == "partial" {
                    vec![first, exemption]
                } else {
                    vec![exemption]
                };
                context.copy_eligible = false;
                let result = engine.commit(bucket, &context, basis()).unwrap();
                match arrival {
                    "ordinary" => {
                        assert_eq!(result.dispositions[&id.0], "raw_only");
                        assert!(!paper.wallet_coverage(&wallet).unwrap().reanchor_required);
                        assert!(!paper.is_wallet_fenced(&wallet).unwrap());
                        assert_eq!(paper.cursor(&wallet).unwrap(), Some(epoch));
                    }
                    "late" => {
                        assert_eq!(result.dispositions[&id.0], "reanchor_required_late_group");
                        assert!(paper.wallet_coverage(&wallet).unwrap().reanchor_required);
                    }
                    "covered" => {
                        assert_eq!(result.dispositions[&id.0], "anchor_covered_late");
                        assert!(paper.wallet_coverage(&wallet).unwrap().reanchor_required);
                    }
                    "partial" => assert_eq!(
                        result.newly_fenced,
                        Some(WalletFenceCause::LateEqualSecondGroup)
                    ),
                    _ => unreachable!(),
                }
            }
        }
        let (_dir, paper, mut engine) = fixture();
        let unknown = group("REDEEM", "unknown", 100, 999, false, "");
        let wallet = unknown.group_id.components().wallet;
        let id = unknown.group_id.key().clone();
        let result = engine.commit(vec![unknown], &context(), basis()).unwrap();
        assert_eq!(result.dispositions[&id.0], "reanchor_required_redemption");
        assert!(paper.wallet_coverage(&wallet).unwrap().reanchor_required);
    }
}
