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
    LeaderAction, MarketId, MarketOutcomeId, OutcomeId, Price, Probability, ProbabilityPpm,
    ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId,
    WalletAddress,
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
    ActivityParseContext, ActivityTransport, PolymarketEndpoint, RECONCILIATION_PAGE_LIMIT,
    ReconciliationPageEvidence, aggregate_activity_rows, canonical_page_hash,
    parse_activity_response, parse_activity_trade_observation,
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

/// Durable receipt-bearing successor and runtime owner of a frozen continuation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContinuationV3 {
    version: u16,
    #[serde(flatten)]
    pub facts: DecisionContinuationFacts,
    pub observed_source_receipt: Option<AppendReceipt>,
    pub page_occurrences: Vec<PageOccurrence>,
    /// Receipt of the complete-read commitment record; present exactly in versions 4 and 5.
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

fn complete_activity_read_error(message: impl Into<String>) -> CompleteActivityReadError {
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

impl DecisionContinuationV3 {
    /// Wire version 5 for commitment v2, version 4 for v1, and version 3 without a commitment.
    pub(crate) fn new(
        facts: DecisionContinuationFacts,
        observed_source_receipt: Option<AppendReceipt>,
        page_occurrences: Vec<PageOccurrence>,
        read_commitment: Option<ActivityReadCommitmentReceipt>,
    ) -> Self {
        Self {
            version: match read_commitment {
                None => 3,
                Some(ActivityReadCommitmentReceipt::LegacyV1(_)) => 4,
                Some(ActivityReadCommitmentReceipt::BindingsV2(_)) => 5,
            },
            facts,
            observed_source_receipt,
            page_occurrences,
            read_commitment: read_commitment.map(ActivityReadCommitmentReceipt::receipt),
        }
    }

    /// Durable wire version (2, 3, 4, or 5).
    #[must_use]
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Whether both continuations describe one complete read: equal wallet, logical read proof,
    /// ordered page occurrences, and read commitment (#565). Consumed by the open-continuation
    /// validator and qualification's read-scope agreement.
    #[must_use]
    pub(crate) fn same_complete_read(&self, other: &Self) -> bool {
        self.version == other.version
            && self.facts.wallet == other.facts.wallet
            && self.facts.decision_inputs == other.facts.decision_inputs
            && self.page_occurrences == other.page_occurrences
            && self.read_commitment == other.read_commitment
    }

    /// Greatest synchronized activity-page receipt in this complete read.
    #[must_use]
    pub fn complete_bound(&self) -> Option<AppendReceipt> {
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
        read.bindings.verify_facts(&self.facts)?;
        self.read_verification()
            .verify_stream_binding_in_read(target_id, receipt, read, lookup)
    }

    pub(crate) fn commitment_contract(&self) -> Option<(u32, u32)> {
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
        if self.version == 5 {
            let read = self.reconstruct_verified_activity_read(lookup)?;
            return self.verify_stream_binding_in_read(target_id, receipt, &read, lookup);
        }
        self.read_verification()
            .verify_stream_binding(target_id, receipt, lookup)
    }
}

// The same read verifier serves a continuation and a commitment-only boot candidate. It owns
// no decision, defaults, financial state, or persistence; all inputs are borrowed recorded proof.
struct ActivityReadVerification<'a> {
    version: u16,
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
        let mut cached_lookup = |receipt: AppendReceipt| {
            let key = (receipt.sequence, receipt.this_hash);
            if let Some(source) = sources.get(&key) {
                return Ok::<CompleteActivityPage, CompleteActivityReadError>(source.clone());
            }
            let source =
                lookup(receipt).map_err(|error| complete_activity_read_error(error.to_string()))?;
            sources.insert(key, source.clone());
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
            self.verify_observation_bindings(commitment, &aggregates, lookup)?
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
            5 => Some((
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
        if (self.version == 5) != value.get("bindings").is_some() {
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
        let expected_version = if self.version == 5 { 2 } else { 1 };
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
            && (self.version != 5
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
        aggregates: &[ActivityAggregate],
        lookup: &mut L,
    ) -> Result<VerifiedObservationBindings, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
        let mut verified = VerifiedObservationBindings::default();
        let mut identities = HashMap::new();
        let Some(bindings) = &commitment.bindings else {
            return Ok(verified);
        };
        let commitment_receipt = self
            .read_commitment
            .ok_or_else(|| complete_activity_read_error("binding commitment receipt is absent"))?;
        let proof: CompleteActivityReadWire = serde_json::from_value(self.decision_inputs.clone())
            .map_err(|error| complete_activity_read_error(error.to_string()))?;
        let pages = proof
            .pages
            .ok_or_else(|| complete_activity_read_error("binding read pages are absent"))?;
        let joined = joined_read_pages(self.page_occurrences, &pages)?;
        for binding in bindings {
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
            let original = observation.group_id.components();
            let history = target.group_id.components();
            let exact = aggregates
                .iter()
                .find(|aggregate| aggregate.group_id.key() == observation.group_id.key());
            if let Some(exact) = exact {
                if exact.group_id != target.group_id {
                    return Err(complete_activity_read_error(
                        "binding substitutes an exact history match",
                    ));
                }
            } else {
                let candidates = aggregates
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
                (None, None) if original == history => {}
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
        if self.version == 5 {
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
        if self.version != 5 {
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
        let Some(selected) = self.observation_receipt() else {
            return Ok(None);
        };
        if self.version == 5 {
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

    /// Verify the complete history and its bindings before selecting the earliest source clock.
    /// Receive timestamps remain separate evidence for copy-delay measurement.
    pub(crate) fn verified_source_time<L, E>(
        &self,
        lookup: &mut L,
    ) -> Result<SourceTimestamp, CompleteActivityReadError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: Display,
    {
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
        if self.version == 5 {
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
        Ok(earliest)
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
            (Self::Historical, 3) | (Self::Committed, 4 | 5)
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
    /// Absent on legacy and empty-binding commitments.
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

fn canonical_json(value: &impl Serialize) -> Result<Vec<u8>, CompleteActivityReadError> {
    serde_json::to_value(value)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|error| {
            complete_activity_read_error(format!("commitment encoding failed: {error}"))
        })
}

fn canonical_bindings(
    bindings: &[ObservationBinding],
) -> Result<Vec<ObservationBinding>, CompleteActivityReadError> {
    let mut encoded = bindings
        .iter()
        .map(|binding| canonical_json(binding).map(|bytes| (bytes, binding.clone())))
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
            canonical_json(&preimage)?,
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
                })?,
            )
        }
    };
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

/// Authenticate bindings collected by the existing boot scan, with exact indexed reads only.
/// A verified commitment is evidence of correlation, never evidence of durable disposition.
pub(crate) fn verified_commitment_bindings(
    receipt: AppendReceipt,
    source_receipts: &SourceReceiptIndex,
) -> Result<Vec<ObservationBinding>, CompleteActivityReadError> {
    let source = source_receipts
        .source_envelope(receipt)
        .map_err(|error| complete_activity_read_error(error.to_string()))?;
    let commitment: ActivityReadCommitment = serde_json::from_slice(&source.payload)
        .map_err(|error| complete_activity_read_error(error.to_string()))?;
    if source.source_id.0 != ACTIVITY_READ_COMMITMENT_SOURCE_ID
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
        return Ok(Vec::new());
    }
    let proof = commitment
        .read_proof
        .as_ref()
        .ok_or_else(|| complete_activity_read_error("binding commitment read proof is absent"))?;
    let inputs = json!({ "fixed_end": commitment.fixed_end, "pages": proof.pages });
    let verifier = ActivityReadVerification {
        version: 5,
        wallet: commitment.wallet,
        decision_inputs: &inputs,
        page_occurrences: &proof.page_occurrences,
        read_commitment: Some(receipt),
    };
    verifier.reconstruct_complete_activity_read(&mut |receipt| {
        source_receipts
            .source_envelope(receipt)
            .map(CompleteActivityPage::from)
    })?;
    Ok(bindings.clone())
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

fn verify_binding_identity(
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
                    facts: legacy.facts,
                    observed_source_receipt: None,
                    page_occurrences: Vec::new(),
                    read_commitment: None,
                }
            }
            3..=5 => serde_json::from_value(value)?,
            version => return Err(DecisionContinuationError::Version(version)),
        };
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
        if (version == 5) != policy_present
            || (version == 5) != frozen.paper_freshness_policy.is_some()
            || frozen
                .paper_freshness_policy
                .is_some_and(|policy| !policy.valid())
        {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        if matches!(version, 3..=5) {
            if (frozen.provenance == TradeProvenance::ActivityWs)
                != continuation.observed_source_receipt.is_some()
                || matches!(version, 4 | 5) != continuation.read_commitment.is_some()
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
        Ok(continuation)
    }

    /// Reconstruct only the transport-neutral trade facts needed by the existing
    /// idempotent decision continuation. Ledger/classification/gate are not rerun.
    pub fn incoming_trade(&self) -> Result<IncomingTrade, DecisionContinuationError> {
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
            received_at: observed_at,
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
    #[error("anchor install durability failure: {0}")]
    Durability(String),
}

impl From<pe_paper_state::PaperStateError> for AnchorInstallError {
    fn from(error: pe_paper_state::PaperStateError) -> Self {
        Self::Durability(format!("paper-state: {error}"))
    }
}

/// Single runtime owner for the exact leader ledger, durable gate projection,
/// wallet fences, and decision admission.
pub struct BucketCommitEngine {
    paper_state: Arc<PaperStateDb>,
    ledger: PositionLedger,
    entry_gate: CopyEntryGate,
    complete_history: HashSet<WalletAddress>,
    fences: HashSet<WalletAddress>,
}

impl BucketCommitEngine {
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
        Ok(Self {
            paper_state,
            ledger,
            entry_gate,
            complete_history,
            fences,
        })
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
    /// that single production caller and batches never nest. The sole boot-path paper-state write
    /// outside that lock, `mark_seeded_history_validated`, runs after every bracket completes. A
    /// failed `ROLLBACK` surfaces as the bracket error, and the next `BEGIN IMMEDIATE` then fails,
    /// so boot fails closed instead of committing partial state.
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
            if self.is_fenced(&install.wallet) {
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
                history_status: install.history_status.clone(),
                wallet: install.wallet,
                balances: install.balances.clone(),
                activity_cutoff_unix: install.cutoff,
                anchored_at_unix: install.proof.recorded_at_unix,
                ledger_hash_after: post.hash,
                positions_proof_hash: install.proof.positions_proof_hash.clone(),
                activity_bounds_json: install.proof.activity_bounds_json.clone(),
                source_log_generation: install.proof.source_log_generation.clone(),
                proof_json: install.proof.document.clone(),
                recorded_at_unix: install.proof.recorded_at_unix,
            });
        }
        self.paper_state.install_anchors(&records)?;
        self.ledger = candidate;
        for install in installs {
            if let Some(status) = &install.history_status {
                self.apply_history_projection(install.wallet, &[], Some(status));
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

        let durable = aggregates
            .iter()
            .map(|aggregate| {
                self.paper_state
                    .activity_group_state(aggregate.group_id.key())
            })
            .collect::<Result<Vec<_>, _>>()?;
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
        let coverage = self.paper_state.wallet_coverage(&wallet)?;
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
                wallet,
                source_epoch,
                dispositions: BTreeMap::new(),
                pending: Vec::new(),
                newly_fenced: None,
                already_committed: true,
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
        let reanchor_trigger = mutations.iter().find_map(|mutation| {
            if !context.bracket_commit
                && context
                    .identity_unresolved
                    .contains(&mutation.source_trade_id)
            {
                Some((
                    mutation.source_trade_id.clone(),
                    "identity_unresolved".to_owned(),
                ))
            } else if matches!(mutation.effect.effective(), LedgerEffect::RequiresAnchor) {
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
        let (gate_results, history_effects, gate_outcomes) =
            Self::derive_gate_results(wallet, source_epoch, &trade_decisions, &first_entries);

        let mut pending = Vec::new();
        let mut dispositions = BTreeMap::new();
        let mut disposition_records = Vec::with_capacity(aggregates.len());
        for ((aggregate, mutation), applied_effect) in
            aggregates.iter().zip(&mutations).zip(&applied)
        {
            let source_trade_id = aggregate.group_id.key().clone();
            let disposition = match mutation.effect.effective() {
                LedgerEffect::RawOnly => "raw_only".to_owned(),
                LedgerEffect::RequiresAnchor => "reanchor_required_redemption".to_owned(),
                LedgerEffect::Trade { .. } => {
                    let outcome = gate_outcomes
                        .get(&source_trade_id.0)
                        .cloned()
                        .unwrap_or_else(|| "not_an_entry".to_owned());
                    if let Some(no_copy) = context.no_copy_dispositions.get(&source_trade_id) {
                        no_copy.reason.clone()
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
                        let frozen_inputs_json =
                            serde_json::to_string(&DecisionContinuationV3::new(
                                facts,
                                context
                                    .observed_source_receipts
                                    .get(&source_trade_id)
                                    .copied(),
                                context.page_occurrences.clone(),
                                context.read_commitment,
                            ))?;
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
        let history_effects = self.covered_history_effects(
            wallet,
            source_epoch,
            if repair_history {
                resolved_mutations
            } else {
                &mutations
            },
        );
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
            && let Some((aggregate, state)) = aggregates
                .iter()
                .zip(durable)
                .find_map(|(aggregate, state)| state.as_ref().map(|state| (aggregate, state)))
        {
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

fn recordable_mutation(
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
            "other_target" => "other",
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
        let stream_payload = serde_json::to_vec(
            &json!({"proxyWallet":"0x1111111111111111111111111111111111111111",
        "conditionId":"old", "asset":"123", "side":"BUY", "size":1, "price":0.5,
        "timestamp":99, "transactionHash":"tx", "outcomeIndex":1}),
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
            components.condition_id.as_ref().is_some_and(|condition| {
                condition.0
                    == match case {
                        "recorded_correction" | "shared_recorded_correction" => "stamped",
                        "other_target" => "other",
                        _ => "new",
                    }
            }) && components
                .asset
                .as_ref()
                .is_some_and(|asset| asset.0 == "123")
                && components.side == Some(Side::Buy)
        })
        .unwrap();
        let metadata = serde_json::to_vec(&json!([{
            "conditionId": if matches!(case, "metadata_wrong_condition" | "other_target") { "other" } else { "new" },
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
            "occurrence" => binding.page_occurrence_index = 1,
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
            "future_receipt" => binding.stream_receipt = receipt(100),
            _ => {}
        }
        let mut bindings = if case == "poll_only" {
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
        if matches!(case, "digest" | "unknown_field") {
            let mut value: Value = serde_json::from_slice(&payload).unwrap();
            if case == "digest" {
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
            if case == "other_target" {
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
                assert_eq!(source_time, time);
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

    pub(crate) fn binding_state(fixture: &BindingFixture) -> Arc<PaperStateDb> {
        let state = Arc::new(PaperStateDb::open(&fixture.dir.path().join("paper.db")).unwrap());
        let continuation = &fixture.continuation;
        let wallet = continuation.facts.wallet;
        state.set_cursor(&wallet, 0).unwrap();
        state
            .install_anchors(&[AnchorInstallRecord {
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
            let evidence = DecisionEvidenceAccumulator::new(&continuation.facts);
            let mut checkpoint = DecisionEvidenceAccumulator::new(&continuation.facts);
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
