//! Durable fixed-end Polymarket activity reconciliation (#544).
//!
//! Websocket observations and public polling pages are raw evidence. This owner
//! coalesces their wakeups per wallet, walks the existing paged/rate-gated REST
//! reader to a fixed end, and sends only complete epoch-second buckets to the
//! orchestrator's single [`crate::bucket_commit::BucketCommitEngine`] owner.
//! A websocket observation remains an obligation, derived from the source log
//! on restart, until its target has a durable terminal/apply record or its
//! wallet has a permanent fence and the observation cannot be bound to a target.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;

use pe_copy_signal_engine::{SignalConfig, TradeProvenance};
use pe_core_types::{
    MarketId, MarketOutcomeId, PolymarketTokenId, ReceivedAt, ReconstructionQuality, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, EventEnvelope, Reader};
use pe_paper_state::{NoCopyDisposition, PaperStateDb};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate, ActivityReadError,
    ActivityType, ReconciliationFetcher, fetch_complete_activity, parse_activity_trade_observation,
};
use pe_trader_index::WatchlistTier;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::warn;

use crate::activity_ingest::{
    ACTIVITY_WS_SOURCE_ID, ReconciliationTrigger, SourceLogHandle, SourceLogHandleError,
};
use crate::asset_identity::{AssetIdentityResolver, IdentityProvenance};
use crate::bucket_commit::{
    ACTIVITY_READ_COMMITMENT_PARSER_VERSION, ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
    ACTIVITY_READ_COMMITMENT_SOURCE_ID, BucketCommitResult, BucketDecisionContext,
    IdentityOverride, ObservationBinding, PageOccurrence, activity_read_commitment_payload_v2,
    joined_read_pages,
};
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
use crate::risk_inputs::SourceReceiptIndex;
use crate::runtime_config::LiveRuntimeConfig;
use crate::watchlist_admission::{AdmissionPreparer, AnchorRefreshOutcome, anchor_refresh_due};

/// Source id stamped on every fixed-end activity page before it is parsed.
pub const ACTIVITY_POLL_SOURCE_ID: &str = "polymarket-public.activity-reconciliation";
pub const DAILY_BOUNDARY_SOURCE_ID: &str = "pe-service.boundary";
/// Service-owned envelope schema of reconciliation pages written by a commitment-aware producer
/// (#565). The activity parser contract (`ACTIVITY_PARSER_VERSION`) is unchanged.
pub const ACTIVITY_POLL_PAGE_SCHEMA_VERSION: u32 = 3;
/// Best-effort cadence for refreshing venue-authoritative position anchors.
pub const ANCHOR_REFRESH_SECS: u64 = 3_600;
/// Compiled bound: one urgent wallet operation and one backstop/anchor operation.
pub const TRADE_RECONCILIATION_CONCURRENCY: usize = 2;
const SECONDS_PER_DAY: i64 = 86_400;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BoundaryScheduleError {
    #[error("daily boundary timestamp arithmetic overflow")]
    Overflow,
}

/// Every completed UTC midnight strictly after the latest boundary or Start, oldest first.
pub fn completed_midnight_cutoffs_after(
    anchor_unix: i64,
    now_unix: i64,
) -> Result<Vec<i64>, BoundaryScheduleError> {
    if now_unix <= anchor_unix {
        return Ok(Vec::new());
    }
    let anchor_day = anchor_unix.div_euclid(SECONDS_PER_DAY);
    let mut cutoff = anchor_day
        .checked_add(1)
        .and_then(|day| day.checked_mul(SECONDS_PER_DAY))
        .ok_or(BoundaryScheduleError::Overflow)?;
    let latest_completed = now_unix
        .div_euclid(SECONDS_PER_DAY)
        .checked_mul(SECONDS_PER_DAY)
        .ok_or(BoundaryScheduleError::Overflow)?;
    let mut cutoffs = Vec::new();
    while cutoff <= latest_completed {
        cutoffs.push(cutoff);
        if cutoff == latest_completed {
            break;
        }
        cutoff = cutoff
            .checked_add(SECONDS_PER_DAY)
            .ok_or(BoundaryScheduleError::Overflow)?;
    }
    Ok(cutoffs)
}

/// Configuration for the existing per-wallet polling cadence.
#[derive(Debug, Clone)]
pub struct TradePollerConfig {
    pub base_url: String,
    pub poll_interval_secs: u64,
    pub activity_ws_enabled: bool,
    pub copy_latency_budget_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Obligation {
    group_id: SourceTradeId,
    received_at: OffsetDateTime,
    receipt: pe_event_log::AppendReceipt,
}

type WalletObligations = BTreeMap<i64, BTreeMap<String, Obligation>>;
type CoalescedObligations = HashMap<WalletAddress, WalletObligations>;

fn insert_coalesced_obligation(
    by_wallet: &mut CoalescedObligations,
    wallet: WalletAddress,
    epoch: i64,
    obligation: Obligation,
) {
    let groups = by_wallet
        .entry(wallet)
        .or_default()
        .entry(epoch)
        .or_default();
    groups
        .entry(obligation.group_id.0.clone())
        .and_modify(|existing| {
            if obligation.receipt.sequence < existing.receipt.sequence {
                existing.received_at = obligation.received_at;
                existing.receipt = obligation.receipt;
            }
        })
        .or_insert(obligation);
}

fn insert_reconciliation_trigger(
    by_wallet: &mut CoalescedObligations,
    trigger: ReconciliationTrigger,
) {
    let epoch = trigger.source_time.unix_timestamp();
    insert_coalesced_obligation(
        by_wallet,
        trigger.wallet,
        epoch,
        Obligation {
            group_id: trigger.source_trade_id,
            received_at: trigger.received_at,
            receipt: trigger.receipt,
        },
    );
}

#[derive(Clone, Default)]
struct BucketIdentities {
    overrides: HashMap<SourceTradeId, IdentityOverride>,
    unresolved: HashSet<SourceTradeId>,
    provenance: BTreeMap<PolymarketTokenId, IdentityProvenance>,
}

/// Coalesced durable websocket work rebuilt from source evidence on restart.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconciliationObligations {
    by_wallet: CoalescedObligations,
    boundary: Option<PendingBoundary>,
    last_boundary_cutoff: Option<i64>,
}

/// Log-pure websocket candidates awaiting one durable-state filter (#572).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ActivityCandidates {
    by_wallet: CoalescedObligations,
    binding_commitments: Vec<AppendReceipt>,
}

impl ActivityCandidates {
    /// Observe one source frame without consulting paper state (#572).
    pub(crate) fn observe_activity(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<(), ObligationRebuildError> {
        if envelope.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID {
            let commitment: crate::bucket_commit::ActivityReadCommitment =
                serde_json::from_slice(&envelope.payload)
                    .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
            let generation = (
                envelope.schema_version,
                envelope.parser_version,
                commitment.version,
            );
            if !matches!(generation, (1, 1, 1) | (2, 1, 2))
                || (commitment.version == 2) != commitment.bindings.is_some()
                || (commitment.version == 1 && commitment.read_proof.is_some())
                || envelope.content_type != ContentType::Json
            {
                return Err(ObligationRebuildError::Binding(
                    "commitment generation differs".to_owned(),
                ));
            }
            if commitment
                .bindings
                .as_ref()
                .is_some_and(|bindings| !bindings.is_empty())
                || commitment.read_proof.is_some()
            {
                self.binding_commitments.push(AppendReceipt {
                    sequence: envelope.seq,
                    this_hash: envelope.this_hash,
                });
            }
            return Ok(());
        }
        if envelope.source_id.0 != ACTIVITY_WS_SOURCE_ID
            || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
            || envelope.parser_version != ACTIVITY_PARSER_VERSION
        {
            return Ok(());
        }
        let activity = parse_activity_trade_observation(&envelope.payload)?;
        insert_reconciliation_trigger(
            &mut self.by_wallet,
            ReconciliationTrigger {
                wallet: activity.wallet,
                source_time: activity.source_time.0,
                source_trade_id: activity.group_id.key().clone(),
                provenance: TradeProvenance::ActivityWs,
                received_at: envelope.received_at.0,
                receipt: AppendReceipt {
                    sequence: envelope.seq,
                    this_hash: envelope.this_hash,
                },
            },
        );
        Ok(())
    }

    /// Number of coalesced candidates.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.by_wallet
            .values()
            .flat_map(BTreeMap::values)
            .map(BTreeMap::len)
            .sum()
    }

    /// Filter the coalesced log candidates against durable paper state (#572). The result is a
    /// keyed map, so the visiting order does not affect it.
    pub(crate) fn into_obligations(
        self,
        paper_state: &PaperStateDb,
        source_receipts: &SourceReceiptIndex,
    ) -> Result<ReconciliationObligations, ObligationRebuildError> {
        let mut bindings = HashMap::<_, Vec<ObservationBinding>>::new();
        for receipt in self.binding_commitments {
            for binding in
                crate::bucket_commit::verified_commitment_bindings(receipt, source_receipts)
                    .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?
            {
                bindings
                    .entry((
                        binding.stream_group_id.clone(),
                        binding.stream_receipt.sequence,
                        binding.stream_receipt.this_hash,
                    ))
                    .or_default()
                    .push(binding);
            }
        }
        let mut obligations = ReconciliationObligations::default();
        for (wallet, epochs) in self.by_wallet {
            let fenced = paper_state.is_wallet_fenced(&wallet)?;
            for (epoch, groups) in epochs {
                for obligation in groups.into_values() {
                    let candidates = bindings
                        .get(&(
                            obligation.group_id.clone(),
                            obligation.receipt.sequence,
                            obligation.receipt.this_hash,
                        ))
                        .map_or(&[][..], Vec::as_slice);
                    let disposed = if candidates.is_empty() {
                        // Historical exact-ID receipts retain their existing acknowledgement contract.
                        // A permanent fence also refuses original observations without a binding.
                        fenced
                            || paper_state
                                .activity_group_state(&obligation.group_id)?
                                .is_some()
                    } else {
                        let mut disposed = false;
                        for binding in candidates {
                            disposed |= binding_target_disposed(paper_state, binding)?;
                        }
                        disposed
                    };
                    if !disposed {
                        insert_coalesced_obligation(
                            &mut obligations.by_wallet,
                            wallet,
                            epoch,
                            obligation,
                        );
                    }
                }
            }
        }
        Ok(obligations)
    }
}

/// The sole source-ordered daily boundary waiting for qualifying activity acknowledgements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingBoundary {
    pub cutoff_unix: i64,
    pub receipt: AppendReceipt,
}

impl ReconciliationObligations {
    /// Add one already-durable reader observation. Reader duplicates coalesce
    /// by wallet, source second, and version-two group identity.
    pub fn insert(&mut self, trigger: ReconciliationTrigger) {
        insert_reconciliation_trigger(&mut self.by_wallet, trigger);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_wallet.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_wallet
            .values()
            .flat_map(BTreeMap::values)
            .map(BTreeMap::len)
            .sum()
    }

    /// Install one pending boundary. A second boundary never replaces the oldest.
    pub fn install_boundary(&mut self, boundary: PendingBoundary) -> bool {
        if self.boundary.is_some() {
            return false;
        }
        self.boundary = Some(boundary);
        true
    }

    pub fn set_boundary_anchor(&mut self, cutoff_unix: i64) {
        self.last_boundary_cutoff = Some(cutoff_unix);
    }

    #[must_use]
    pub fn boundary_anchor(&self) -> Option<i64> {
        self.last_boundary_cutoff
    }

    /// The oldest pending boundary, if any.
    #[must_use]
    pub fn pending_boundary(&self) -> Option<PendingBoundary> {
        self.boundary
    }

    /// True after every obligation inside both the receipt and receive-time bounds is gone.
    #[must_use]
    pub fn boundary_ready(&self) -> bool {
        let Some(boundary) = self.boundary else {
            return false;
        };
        !self.by_wallet.values().any(|epochs| {
            epochs.values().any(|groups| {
                groups.values().any(|obligation| {
                    obligation.receipt.sequence <= boundary.receipt.sequence
                        && obligation.received_at.unix_timestamp() < boundary.cutoff_unix
                })
            })
        })
    }

    /// Remove and return the boundary only after its qualifying obligations are acknowledged.
    pub fn take_ready_boundary(&mut self) -> Option<PendingBoundary> {
        if self.boundary_ready() {
            self.boundary.take()
        } else {
            None
        }
    }

    /// Stable activation census persisted with the paper migration record.
    #[must_use]
    pub fn migration_evidence(&self) -> serde_json::Value {
        let mut rows = self
            .by_wallet
            .iter()
            .flat_map(|(wallet, epochs)| {
                epochs.iter().flat_map(move |(epoch, groups)| {
                    groups.values().map(move |obligation| {
                        serde_json::json!({
                            "wallet": wallet.to_string(),
                            "source_epoch": epoch,
                            "source_trade_id": obligation.group_id.0,
                            "received_at_unix": obligation.received_at.unix_timestamp(),
                            "receipt": obligation.receipt,
                        })
                    })
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.to_string());
        serde_json::Value::Array(rows)
    }

    fn wallets(&self) -> impl Iterator<Item = WalletAddress> + '_ {
        self.by_wallet.keys().copied()
    }

    #[cfg(test)]
    fn earliest_epoch(&self, wallet: &WalletAddress) -> Option<i64> {
        self.by_wallet
            .get(wallet)
            .and_then(|epochs| epochs.first_key_value().map(|(epoch, _)| *epoch))
    }

    fn observation(
        &self,
        wallet: &WalletAddress,
        epoch: i64,
        group: &SourceTradeId,
    ) -> Option<(AppendReceipt, OffsetDateTime)> {
        self.by_wallet
            .get(wallet)?
            .get(&epoch)?
            .get(&group.0)
            .map(|obligation| (obligation.receipt, obligation.received_at))
    }

    fn remove_selected(&mut self, wallet: WalletAddress, epoch: i64, selected: &Obligation) {
        if self
            .observation(&wallet, epoch, &selected.group_id)
            .is_some_and(|(receipt, _)| receipt == selected.receipt)
        {
            self.remove(&wallet, epoch, &selected.group_id);
        }
    }

    fn remove(&mut self, wallet: &WalletAddress, epoch: i64, group: &SourceTradeId) {
        let mut remove_wallet = false;
        if let Some(epochs) = self.by_wallet.get_mut(wallet) {
            if let Some(groups) = epochs.get_mut(&epoch) {
                groups.remove(&group.0);
                if groups.is_empty() {
                    epochs.remove(&epoch);
                }
            }
            remove_wallet = epochs.is_empty();
        }
        if remove_wallet {
            self.by_wallet.remove(wallet);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObligationRebuildError {
    #[error("source-log replay: {0}")]
    Log(#[from] pe_event_log::LogError),
    #[error("source-log websocket observation: {0}")]
    Activity(#[from] pe_source_polymarket_public::ActivityParseError),
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("paper-log boundary replay: {0}")]
    PaperLog(String),
    #[error("invalid daily-boundary source frame: {0}")]
    Boundary(String),
    #[error("source-log observation binding: {0}")]
    Binding(String),
}

/// Log-pure daily-boundary candidates awaiting the paper-log anchor (#572).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DailyBoundaryCandidates {
    boundaries: Vec<PendingBoundary>,
}

impl DailyBoundaryCandidates {
    /// Observe and validate one matching source frame without reading the paper log (#572).
    pub(crate) fn observe_daily_boundary(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<(), ObligationRebuildError> {
        if envelope.source_id.0 != DAILY_BOUNDARY_SOURCE_ID {
            return Ok(());
        }
        let value: serde_json::Value = serde_json::from_slice(&envelope.payload)
            .map_err(|error| ObligationRebuildError::Boundary(error.to_string()))?;
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("daily_boundary") {
            return Err(ObligationRebuildError::Boundary(
                "boundary source id carries an unexpected kind".to_owned(),
            ));
        }
        let cutoff_unix = value
            .get("cutoff_unix")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                ObligationRebuildError::Boundary("boundary cutoff is absent".to_owned())
            })?;
        self.boundaries.push(PendingBoundary {
            cutoff_unix,
            receipt: AppendReceipt {
                sequence: envelope.seq,
                this_hash: envelope.this_hash,
            },
        });
        Ok(())
    }
}

/// Compute and install the Start/latest-mark anchor before source observation (#572).
pub(crate) fn recover_daily_boundary_anchor(
    paper_log_path: &Path,
    obligations: &mut ReconciliationObligations,
) -> Result<Option<i64>, ObligationRebuildError> {
    let frames = crate::paper_recovery::scan_paper_log(paper_log_path)
        .map_err(|error| ObligationRebuildError::PaperLog(error.to_string()))?;
    let era = crate::paper_recovery::paper_era(frames);
    let Some((_, start)) = era.start.as_ref() else {
        return Ok(None);
    };
    let start_unix = era
        .frames
        .iter()
        .find_map(|frame| match &frame.frame {
            crate::paper_recovery::PaperLogFrame::Record(
                crate::paper_recovery::PaperLogRecord::QualificationStarted(candidate),
            ) if candidate.as_ref() == start => Some(frame.envelope.received_at.0.unix_timestamp()),
            _ => None,
        })
        .ok_or_else(|| ObligationRebuildError::Boundary("Start envelope is absent".to_owned()))?;
    let anchor = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            crate::paper_recovery::PaperLogFrame::Record(
                crate::paper_recovery::PaperLogRecord::PortfolioMark(mark),
            ) => Some(mark.cutoff_unix),
            _ => None,
        })
        .max()
        .unwrap_or(start_unix);
    obligations.set_boundary_anchor(anchor);
    Ok(Some(anchor))
}

/// Select and install the oldest source candidate after the computed paper anchor (#572).
pub(crate) fn recover_daily_boundary_from_candidates(
    candidates: DailyBoundaryCandidates,
    anchor: i64,
    obligations: &mut ReconciliationObligations,
) {
    if let Some(boundary) = candidates
        .boundaries
        .into_iter()
        .filter(|boundary| boundary.cutoff_unix > anchor)
        .min_by_key(|boundary| (boundary.cutoff_unix, boundary.receipt.sequence))
    {
        obligations.install_boundary(boundary);
    }
}

/// Recover the oldest unacknowledged source boundary and the Start/latest-mark anchor.
pub fn recover_daily_boundary(
    source_log_path: &Path,
    paper_log_path: &Path,
    obligations: &mut ReconciliationObligations,
) -> Result<(), ObligationRebuildError> {
    let Some(anchor) = recover_daily_boundary_anchor(paper_log_path, obligations)? else {
        return Ok(());
    };
    let mut candidates = DailyBoundaryCandidates::default();
    for item in Reader::replay(source_log_path)? {
        let (_sequence, envelope) = item?;
        candidates.observe_daily_boundary(&envelope)?;
    }
    recover_daily_boundary_from_candidates(candidates, anchor, obligations);
    Ok(())
}

/// Rebuild unresolved websocket obligations before any producer starts.
pub fn rebuild_reconciliation_obligations(
    source_log_path: &Path,
    paper_state: &PaperStateDb,
) -> Result<ReconciliationObligations, ObligationRebuildError> {
    let mut candidates = ActivityCandidates::default();
    let mut staging = SourceReceiptIndex::staging(source_log_path)
        .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
    let mut last_sequence = None;
    let mut last_hash = blake3::Hash::from_bytes([0; 32]);
    for item in Reader::replay_with_offsets(source_log_path)? {
        let (offset, sequence, envelope) = item?;
        staging
            .observe(offset, &envelope)
            .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
        last_sequence = Some(sequence);
        last_hash = envelope.this_hash;
        candidates.observe_activity(&envelope)?;
    }
    let binding = pe_event_log::LogTailBinding {
        path: std::fs::canonicalize(source_log_path)
            .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?,
        physical_tail: std::fs::metadata(source_log_path)
            .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?
            .len(),
        last_sequence,
        last_hash,
    };
    let index = staging
        .complete(&binding)
        .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
    candidates.into_obligations(paper_state, &index)
}

#[derive(Debug, thiserror::Error)]
enum ReconciliationError {
    #[error("activity reconciliation: {0}")]
    Activity(#[from] ActivityReadError),
    #[error("asset identity resolution: {0}")]
    Identity(SourceError),
    #[error("source-log coordinator closed")]
    SourceLogClosed,
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("orchestrator control channel closed")]
    ControlClosed,
    #[error("orchestrator bucket commit failed: {0}")]
    BucketCommit(String),
    #[error("invalid reconstruction quality")]
    ReconstructionQuality,
    #[error("decision evidence json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("activity page receipts do not match the complete read")]
    PageReceiptMismatch,
    #[error("recorded observation binding: {0}")]
    Binding(String),
}

impl ReconciliationError {
    fn retryable(&self) -> bool {
        matches!(self, Self::Activity(_))
            || matches!(
                self,
                Self::Identity(SourceError::Transient { .. } | SourceError::RateLimited { .. })
            )
    }
}

/// Existing polling actor, now a complete fixed-end reconciliation coordinator.
pub struct TradePoller {
    config: TradePollerConfig,
    live_watchlist: LiveWatchlist,
    fetcher: Arc<dyn ReconciliationFetcher>,
    asset_identity: Arc<AssetIdentityResolver>,
    source_log: SourceLogHandle,
    trigger_rx: mpsc::Receiver<ReconciliationTrigger>,
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,

    signal_config: SignalConfig,
    runtime_config: LiveRuntimeConfig,
    obligations: ReconciliationObligations,
    admission_preparer: Option<AdmissionPreparer>,
    refresh_cursor: usize,
    refresh_reanchor_turn: bool,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    source_receipts: Option<SourceReceiptIndex>,
    #[cfg(feature = "scenario")]
    progress: Option<mpsc::Sender<PollerProgress>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnchorRefreshClass {
    ReanchorRequired,
    AgeDue,
}

#[derive(Debug, thiserror::Error)]
pub enum TradePollerOwnerError {
    #[error("fixed-end reconciliation owner failed: {0}")]
    Reconciliation(String),
    #[error("activity reconciliation trigger channel closed")]
    TriggerChannelClosed,
    #[error("trade poll interval is zero")]
    ZeroPollInterval,
    #[error("position anchor refresh failed: {0}")]
    AnchorRefresh(String),
    #[error("daily boundary owner failed: {0}")]
    DailyBoundary(String),
}

#[derive(Clone, Default)]
struct WalletAttempt {
    selected: WalletObligations,
    deadline: Option<OffsetDateTime>,
    last_fixed_end: Option<i64>,
}

impl WalletAttempt {
    fn ready(&self, now: OffsetDateTime) -> bool {
        self.last_fixed_end.is_none()
            || (self.deadline.is_some_and(|deadline| now <= deadline)
                && self
                    .last_fixed_end
                    .is_some_and(|end| now.unix_timestamp() > end))
    }
}

#[derive(PartialEq, Eq)]
enum RoundStage {
    Boundary,
    Wallets,
    Refresh,
    Publish,
    Done,
}

struct BackstopRound {
    wallets: VecDeque<WalletAddress>,
    live_wallets: Vec<WalletAddress>,
    stage: RoundStage,
    refresh_wallet: Option<WalletAddress>,
    successes: usize,
    failures: usize,
}

impl BackstopRound {
    fn new(poller: &TradePoller) -> Self {
        let mut live_wallets = poller
            .live_watchlist
            .snapshot()
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .collect::<Vec<_>>();
        live_wallets.sort_by_key(ToString::to_string);
        live_wallets.dedup();
        let mut wallets = live_wallets.clone();
        wallets.extend(poller.obligations.wallets());
        wallets.sort_by_key(ToString::to_string);
        wallets.dedup();
        Self {
            wallets: wallets.into(),
            live_wallets,
            stage: RoundStage::Boundary,
            refresh_wallet: None,
            successes: 0,
            failures: 0,
        }
    }
}

enum Completion {
    Reconciled {
        wallet: WalletAddress,
        urgent: bool,
        selected: Vec<AppendReceipt>,
        result: Result<Vec<(i64, Obligation)>, ReconciliationError>,
    },
    Refreshed(
        WalletAddress,
        Result<(AnchorRefreshOutcome, bool), TradePollerOwnerError>,
    ),
    BoundaryInstalled(Result<PendingBoundary, TradePollerOwnerError>),
    BoundaryPublished(PendingBoundary, Result<(), TradePollerOwnerError>),
}

/// Bounded scenario-only completion barriers; production has no observer channel.
#[cfg(feature = "scenario")]
#[derive(Debug)]
pub enum PollerProgress {
    Started {
        wallet: WalletAddress,
        urgent: bool,
        frontier: Vec<AppendReceipt>,
    },
    Completed {
        wallet: WalletAddress,
        selected: Vec<AppendReceipt>,
    },
    RoundCompleted,
}

fn remove_wallet_obligation(selected: &mut WalletObligations, epoch: i64, group: &SourceTradeId) {
    if let Some(groups) = selected.get_mut(&epoch) {
        groups.remove(&group.0);
        if groups.is_empty() {
            selected.remove(&epoch);
        }
    }
}

impl TradePoller {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: TradePollerConfig,
        live_watchlist: LiveWatchlist,
        fetcher: Arc<dyn ReconciliationFetcher>,
        asset_identity: Arc<AssetIdentityResolver>,
        source_log: SourceLogHandle,
        trigger_rx: mpsc::Receiver<ReconciliationTrigger>,
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
        health: SharedHealth,

        signal_config: SignalConfig,
        runtime_config: LiveRuntimeConfig,
        obligations: ReconciliationObligations,
        admission_preparer: Option<AdmissionPreparer>,
    ) -> Self {
        Self {
            config,
            live_watchlist,
            fetcher,
            asset_identity,
            source_log,
            trigger_rx,
            control_tx,
            paper_state,
            health,
            signal_config,
            runtime_config,
            obligations,
            admission_preparer,
            refresh_cursor: 0,
            refresh_reanchor_turn: true,
            now: Arc::new(OffsetDateTime::now_utc),
            source_receipts: None,
            #[cfg(feature = "scenario")]
            progress: None,
        }
    }

    /// Deterministic reconciliation clock for hermetic scenarios.
    #[cfg(feature = "scenario")]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// Share the coordinator's existing receipt index for raw observation and metadata lookup.
    #[must_use]
    pub fn with_source_receipt_index(mut self, index: SourceReceiptIndex) -> Self {
        self.source_receipts = Some(index);
        self
    }

    /// Explicit operation barriers for deterministic scheduler scenarios.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_progress(mut self, progress: mpsc::Sender<PollerProgress>) -> Self {
        self.progress = Some(progress);
        self
    }

    pub async fn run(self) {
        let _ = self.run_until(std::future::pending::<()>()).await;
    }

    /// Receive triggers throughout reads. Shutdown stops admission and drains every started
    /// operation while the source coordinator and serialized control owner remain available.
    pub async fn run_until(
        mut self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), TradePollerOwnerError> {
        if self.config.poll_interval_secs == 0 {
            return Err(TradePollerOwnerError::ZeroPollInterval);
        }
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .poll_started_at = Some((self.now)());
        tokio::pin!(shutdown);
        let mut tasks = JoinSet::new();
        let mut busy_wallets = HashSet::new();
        let mut urgent_busy = false;
        let mut backstop_busy = false;
        // Expiry changes readiness, not ownership: the frozen frontier returns to the backstop
        // until disposed, while later receipts remain queued for a subsequent attempt.
        let mut attempts = HashMap::<WalletAddress, WalletAttempt>::new();
        let mut refresh_reconcile = HashMap::<WalletAddress, Option<i64>>::new();
        let mut refresh_retry = VecDeque::new();
        let mut round = BackstopRound::new(&self);
        let mut cadence = tokio::time::Instant::now();
        let mut stopping = false;
        let mut failure = None;
        loop {
            // Poll shutdown before admitting work, including at startup.
            if !stopping && shutdown.as_mut().now_or_never().is_some() {
                stopping = true;
            }
            if !stopping {
                self.drain_triggers();
                let now = (self.now)();
                if !backstop_busy
                    && round.stage == RoundStage::Refresh
                    && round.refresh_wallet.is_none()
                {
                    match self.select_refresh_wallet(&round.live_wallets) {
                        Ok(wallet) => round.refresh_wallet = wallet,
                        Err(error) => {
                            failure = Some(error);
                            stopping = true;
                        }
                    }
                }
                if !stopping && !urgent_busy && tasks.len() < TRADE_RECONCILIATION_CONCURRENCY {
                    let mut wallets = self
                        .obligations
                        .wallets()
                        .chain(refresh_reconcile.keys().copied())
                        .collect::<Vec<_>>();
                    wallets.sort_by_key(|wallet| {
                        (
                            self.obligations.by_wallet.get(wallet).and_then(|epochs| {
                                epochs
                                    .values()
                                    .flat_map(BTreeMap::values)
                                    .map(|obligation| obligation.receipt.sequence)
                                    .min()
                            }),
                            wallet.to_string(),
                        )
                    });
                    wallets.dedup();
                    for wallet in wallets {
                        if busy_wallets.contains(&wallet)
                            || round.refresh_wallet == Some(wallet)
                            || refresh_retry.contains(&wallet)
                        {
                            continue;
                        }
                        let forced = refresh_reconcile
                            .get(&wallet)
                            .is_some_and(|end| end.is_none_or(|end| now.unix_timestamp() > end));
                        let ready = if let Some(attempt) = attempts.get(&wallet) {
                            attempt.ready(now)
                        } else {
                            self.freeze_attempt(wallet, true).is_some()
                        };
                        if !forced && !ready {
                            continue;
                        }
                        let attempt = attempts.entry(wallet).or_insert_with(|| {
                            self.freeze_attempt(wallet, !forced).unwrap_or_default()
                        });
                        attempt.last_fixed_end = Some(now.unix_timestamp());
                        if forced {
                            refresh_reconcile.insert(wallet, Some(now.unix_timestamp()));
                        }
                        self.spawn_reconciliation(
                            &mut tasks,
                            wallet,
                            attempt,
                            true,
                            now.unix_timestamp(),
                        );
                        busy_wallets.insert(wallet);
                        urgent_busy = true;
                        break;
                    }
                }
                if !stopping && !backstop_busy {
                    if round.stage == RoundStage::Done && tokio::time::Instant::now() >= cadence {
                        round = BackstopRound::new(&self);
                        // An operation already admitted in the urgent slot is this wallet's
                        // current visit; it must not hold the background round at its barrier.
                        round
                            .wallets
                            .retain(|wallet| !busy_wallets.contains(wallet));
                    }
                    let operation = self.operation();
                    match round.stage {
                        RoundStage::Boundary => {
                            round.stage = RoundStage::Wallets;
                            if self.obligations.pending_boundary().is_none()
                                && let Some(anchor) = self.obligations.boundary_anchor()
                            {
                                match completed_midnight_cutoffs_after(anchor, now.unix_timestamp())
                                {
                                    Ok(cutoffs) => {
                                        if let Some(cutoff) = cutoffs.into_iter().next() {
                                            tasks.spawn(async move {
                                                Completion::BoundaryInstalled(
                                                    operation.install_boundary(cutoff).await,
                                                )
                                            });
                                            backstop_busy = true;
                                        }
                                    }
                                    Err(error) => {
                                        failure = Some(TradePollerOwnerError::DailyBoundary(
                                            error.to_string(),
                                        ));
                                        stopping = true;
                                    }
                                }
                            }
                        }
                        RoundStage::Wallets => {
                            if let Some(index) = round
                                .wallets
                                .iter()
                                .position(|wallet| !busy_wallets.contains(wallet))
                            {
                                if let Some(wallet) = round.wallets.remove(index) {
                                    if attempts.get(&wallet).is_some_and(|attempt| {
                                        !attempt.selected.is_empty()
                                            && attempt
                                                .deadline
                                                .is_some_and(|deadline| now <= deadline)
                                            && attempt
                                                .last_fixed_end
                                                .is_some_and(|end| now.unix_timestamp() <= end)
                                    }) {
                                        continue;
                                    }
                                    let attempt = attempts.entry(wallet).or_insert_with(|| {
                                        self.freeze_attempt(wallet, false).unwrap_or_default()
                                    });
                                    attempt.last_fixed_end = Some(now.unix_timestamp());
                                    self.spawn_reconciliation(
                                        &mut tasks,
                                        wallet,
                                        attempt,
                                        false,
                                        now.unix_timestamp(),
                                    );
                                    busy_wallets.insert(wallet);
                                    backstop_busy = true;
                                }
                            } else if round.wallets.is_empty() {
                                round.stage = RoundStage::Refresh;
                            }
                        }
                        RoundStage::Refresh => {
                            if let Some(wallet) = round.refresh_wallet {
                                if !busy_wallets.contains(&wallet) {
                                    busy_wallets.insert(wallet);
                                    tasks.spawn(async move {
                                        Completion::Refreshed(
                                            wallet,
                                            operation.refresh_one_wallet(wallet).await,
                                        )
                                    });
                                    backstop_busy = true;
                                    round.refresh_wallet = None;
                                    round.stage = RoundStage::Publish;
                                }
                            } else {
                                round.stage = RoundStage::Publish;
                            }
                        }
                        RoundStage::Publish => {
                            round.stage = RoundStage::Done;
                            self.record_round_health(&round);
                            cadence = tokio::time::Instant::now()
                                + Duration::from_secs(self.config.poll_interval_secs);
                            #[cfg(feature = "scenario")]
                            self.report_progress(PollerProgress::RoundCompleted);
                            if let Some(boundary) = self.obligations.take_ready_boundary() {
                                tasks.spawn(async move {
                                    Completion::BoundaryPublished(
                                        boundary,
                                        operation.publish_boundary(boundary).await,
                                    )
                                });
                                backstop_busy = true;
                            }
                        }
                        RoundStage::Done => {
                            if let Some(index) = refresh_retry
                                .iter()
                                .position(|wallet| !busy_wallets.contains(wallet))
                            {
                                if let Some(wallet) = refresh_retry.remove(index) {
                                    busy_wallets.insert(wallet);
                                    tasks.spawn(async move {
                                        Completion::Refreshed(
                                            wallet,
                                            operation.refresh_one_wallet(wallet).await,
                                        )
                                    });
                                    backstop_busy = true;
                                }
                            } else if let Some(boundary) = self.obligations.take_ready_boundary() {
                                tasks.spawn(async move {
                                    Completion::BoundaryPublished(
                                        boundary,
                                        operation.publish_boundary(boundary).await,
                                    )
                                });
                                backstop_busy = true;
                            }
                        }
                    }
                }
            }
            if stopping && tasks.is_empty() {
                break;
            }
            // Synchronous stage transitions must finish without waiting for a timer or trigger.
            let background_ready = match round.stage {
                RoundStage::Boundary | RoundStage::Publish => true,
                RoundStage::Wallets => {
                    round.wallets.is_empty()
                        || round
                            .wallets
                            .iter()
                            .any(|wallet| !busy_wallets.contains(wallet))
                }
                RoundStage::Refresh => round
                    .refresh_wallet
                    .is_none_or(|wallet| !busy_wallets.contains(&wallet)),
                RoundStage::Done => false,
            };
            if !stopping && !backstop_busy && background_ready {
                continue;
            }
            let now = (self.now)();
            let retry_pending = !refresh_reconcile.is_empty()
                || attempts
                    .values()
                    .any(|attempt| attempt.deadline.is_some_and(|deadline| now <= deadline));
            let retry_delay = Duration::from_nanos(1_000_000_000 - u64::from(now.nanosecond()));
            let retry_ready = tokio::time::Instant::now() + retry_delay;
            let wake = match (round.stage == RoundStage::Done, retry_pending) {
                (true, true) => Some(cadence.min(retry_ready)),
                (true, false) => Some(cadence),
                (false, true) => Some(retry_ready),
                (false, false) => None,
            };
            tokio::select! {
                biased;
                () = &mut shutdown, if !stopping => stopping = true,
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    match completed {
                        Some(Ok(Completion::Reconciled { wallet, urgent, selected, result })) => {
                            busy_wallets.remove(&wallet);
                            if urgent { urgent_busy = false; } else { backstop_busy = false; }
                            let counts_round = if urgent {
                                if let Some(index) = round.wallets.iter().position(|candidate| *candidate == wallet) {
                                    round.wallets.remove(index);
                                    true
                                } else { false }
                            } else { true };
                            match result {
                                Ok(resolved) => {
                                    if counts_round { round.successes += 1; }
                                    for (epoch, obligation) in resolved {
                                        self.obligations.remove_selected(wallet, epoch, &obligation);
                                        if let Some(attempt) = attempts.get_mut(&wallet) {
                                            remove_wallet_obligation(&mut attempt.selected, epoch, &obligation.group_id);
                                        }
                                    }
                                    if attempts.get(&wallet).is_some_and(|attempt| attempt.selected.is_empty()) {
                                        attempts.remove(&wallet);
                                    }
                                    if refresh_reconcile.contains_key(&wallet) && !refresh_retry.contains(&wallet) {
                                        refresh_retry.push_back(wallet);
                                    }
                                }
                                Err(error) if error.retryable() => {
                                    if counts_round { round.failures += 1; }
                                    if attempts.get(&wallet).is_some_and(|attempt| attempt.selected.is_empty()) {
                                        attempts.remove(&wallet);
                                    }
                                    warn!(wallet = %wallet, error = %error, "fixed-end activity reconciliation will retry");
                                }
                                Err(error) => failure = Some(TradePollerOwnerError::Reconciliation(error.to_string())),
                            }
                            #[cfg(feature = "scenario")]
                            self.report_progress(PollerProgress::Completed { wallet, selected });
                            #[cfg(not(feature = "scenario"))]
                            let _ = selected;
                        }
                        Some(Ok(Completion::Refreshed(wallet, result))) => {
                            busy_wallets.remove(&wallet);
                            backstop_busy = false;
                            match result {
                                Ok((AnchorRefreshOutcome::Deferred, true)) => { refresh_reconcile.entry(wallet).or_insert(None); }
                                Ok(_) => { refresh_reconcile.remove(&wallet); }
                                Err(error) => failure = Some(error),
                            }
                        }
                        Some(Ok(Completion::BoundaryInstalled(result))) => {
                            backstop_busy = false;
                            match result {
                                Ok(boundary) => { self.obligations.install_boundary(boundary); }
                                Err(error) => failure = Some(error),
                            }
                        }
                        Some(Ok(Completion::BoundaryPublished(boundary, result))) => {
                            backstop_busy = false;
                            match result {
                                Ok(()) => self.obligations.set_boundary_anchor(boundary.cutoff_unix),
                                Err(error) => { self.obligations.install_boundary(boundary); failure = Some(error); }
                            }
                        }
                        Some(Err(error)) => failure = Some(TradePollerOwnerError::Reconciliation(error.to_string())),
                        None => {}
                    }
                }
                trigger = self.trigger_rx.recv(), if !stopping => match trigger {
                    Some(trigger) => self.obligations.insert(trigger),
                    None => failure = Some(TradePollerOwnerError::TriggerChannelClosed),
                },
                () = async { if let Some(wake) = wake { tokio::time::sleep_until(wake).await; } else { std::future::pending::<()>().await; } }, if !stopping => {}
            }
            stopping |= failure.is_some();
        }
        failure.map_or(Ok(()), Err)
    }

    fn drain_triggers(&mut self) {
        for _ in 0..self.trigger_rx.max_capacity() {
            let Ok(trigger) = self.trigger_rx.try_recv() else {
                break;
            };
            self.obligations.insert(trigger);
        }
    }

    fn freeze_attempt(&self, wallet: WalletAddress, urgent: bool) -> Option<WalletAttempt> {
        let now = (self.now)();
        let budget = time::Duration::seconds(
            i64::try_from(self.config.copy_latency_budget_secs).unwrap_or(i64::MAX),
        );
        let selected = self
            .obligations
            .by_wallet
            .get(&wallet)
            .cloned()
            .unwrap_or_default();
        if urgent
            && (selected.is_empty()
                || (self.config.activity_ws_enabled
                    && !selected.keys().any(|epoch| {
                        OffsetDateTime::from_unix_timestamp(*epoch)
                            .is_ok_and(|source| now - source <= budget)
                    })))
        {
            return None;
        }
        let deadline = self
            .config
            .activity_ws_enabled
            .then(|| {
                selected
                    .first_key_value()
                    .and_then(|(epoch, _)| OffsetDateTime::from_unix_timestamp(*epoch).ok())
                    .and_then(|source| source.checked_add(budget))
            })
            .flatten();
        Some(WalletAttempt {
            selected,
            deadline,
            last_fixed_end: None,
        })
    }

    fn operation(&self) -> WalletOperation {
        WalletOperation {
            config: self.config.clone(),
            fetcher: self.fetcher.clone(),
            asset_identity: self.asset_identity.clone(),
            source_log: self.source_log.clone(),
            source_receipts: self.source_receipts.clone(),
            control_tx: self.control_tx.clone(),
            paper_state: self.paper_state.clone(),
            signal_config: self.signal_config.clone(),
            runtime_config: self.runtime_config.clone(),
            now: self.now.clone(),
            admission_preparer: self.admission_preparer.clone(),
        }
    }

    fn spawn_reconciliation(
        &self,
        tasks: &mut JoinSet<Completion>,
        wallet: WalletAddress,
        attempt: &WalletAttempt,
        urgent: bool,
        fixed_end: i64,
    ) {
        let operation = self.operation();
        let selected = attempt.selected.clone();
        let frontier = selected
            .values()
            .flat_map(BTreeMap::values)
            .map(|obligation| obligation.receipt)
            .collect::<Vec<_>>();
        let entry = self
            .live_watchlist
            .snapshot()
            .entries
            .iter()
            .find(|entry| entry.wallet == wallet)
            .cloned();
        #[cfg(feature = "scenario")]
        self.report_progress(PollerProgress::Started {
            wallet,
            urgent,
            frontier: frontier.clone(),
        });
        tasks.spawn(async move {
            let result = operation
                .reconcile_wallet(wallet, entry.as_ref(), &selected, fixed_end)
                .await;
            Completion::Reconciled {
                wallet,
                urgent,
                selected: frontier,
                result,
            }
        });
    }

    fn record_round_health(&self, round: &BackstopRound) {
        if round.successes == 0 && round.failures == 0 {
            return;
        }
        let mut health = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if round.successes > 0 {
            let now = (self.now)();
            health.polymarket_last_event_at = Some(now);
            health.poll_last_round_at = Some(now);
            health.poll_error_streak = 0;
        } else {
            health.poll_error_streak = health.poll_error_streak.saturating_add(1);
        }
    }

    #[cfg(feature = "scenario")]
    fn report_progress(&self, progress: PollerProgress) {
        if let Some(sender) = &self.progress {
            let _ = sender.try_send(progress);
        }
    }

    fn select_refresh_wallet(
        &mut self,
        wallets: &[WalletAddress],
    ) -> Result<Option<WalletAddress>, TradePollerOwnerError> {
        if self.admission_preparer.is_none() {
            return Ok(None);
        }
        if wallets.is_empty() {
            self.refresh_cursor = 0;
            return Ok(None);
        }
        let now_unix = (self.now)().unix_timestamp();
        let mut candidates = Vec::new();
        for offset in 0..wallets.len() {
            let index = self.refresh_cursor.saturating_add(offset) % wallets.len();
            let wallet = wallets[index];
            let coverage = self
                .paper_state
                .wallet_coverage(&wallet)
                .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?;
            if !anchor_refresh_due(&coverage, now_unix, ANCHOR_REFRESH_SECS) {
                continue;
            }
            let class = if coverage.reanchor_required {
                AnchorRefreshClass::ReanchorRequired
            } else {
                AnchorRefreshClass::AgeDue
            };
            candidates.push((index, class));
        }
        let Some(index) = select_refresh_candidate(&candidates, &mut self.refresh_reanchor_turn)
        else {
            return Ok(None);
        };
        let wallet = wallets[index];
        self.refresh_cursor = index.saturating_add(1) % wallets.len();
        Ok(Some(wallet))
    }
}

#[derive(Clone)]
struct WalletOperation {
    config: TradePollerConfig,
    fetcher: Arc<dyn ReconciliationFetcher>,
    asset_identity: Arc<AssetIdentityResolver>,
    source_log: SourceLogHandle,
    source_receipts: Option<SourceReceiptIndex>,
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    signal_config: SignalConfig,
    runtime_config: LiveRuntimeConfig,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    admission_preparer: Option<AdmissionPreparer>,
}

impl WalletOperation {
    async fn refresh_one_wallet(
        &self,
        wallet: WalletAddress,
    ) -> Result<(AnchorRefreshOutcome, bool), TradePollerOwnerError> {
        let Some(preparer) = &self.admission_preparer else {
            return Ok((AnchorRefreshOutcome::Skipped, false));
        };
        let coverage = self
            .paper_state
            .wallet_coverage(&wallet)
            .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?;
        let routine = coverage.anchor_seq.is_some() && !coverage.reanchor_required;
        let outcome = preparer
            .prepare_if_due(wallet, (self.now)().unix_timestamp(), ANCHOR_REFRESH_SECS)
            .await
            .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?;
        Ok((outcome, routine))
    }

    async fn install_boundary(
        &self,
        cutoff_unix: i64,
    ) -> Result<PendingBoundary, TradePollerOwnerError> {
        let now = (self.now)();
        let receipt = self
            .source_log
            .append(EnvelopeIn {
                source_id: SourceId(DAILY_BOUNDARY_SOURCE_ID.to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(
                    &serde_json::json!({ "kind": "daily_boundary", "cutoff_unix": cutoff_unix }),
                )
                .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?,
            })
            .await
            .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?;
        Ok(PendingBoundary {
            cutoff_unix,
            receipt,
        })
    }

    async fn publish_boundary(
        &self,
        boundary: PendingBoundary,
    ) -> Result<(), TradePollerOwnerError> {
        let (acknowledged, received) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::DailyBoundary {
                cutoff_unix: boundary.cutoff_unix,
                boundary_receipt: boundary.receipt,
                acknowledged,
            })
            .await
            .map_err(|_| {
                TradePollerOwnerError::DailyBoundary(
                    "orchestrator control channel closed".to_owned(),
                )
            })?;
        received
            .await
            .map_err(|_| {
                TradePollerOwnerError::DailyBoundary(
                    "daily boundary acknowledgement closed".to_owned(),
                )
            })?
            .map_err(TradePollerOwnerError::DailyBoundary)
    }

    async fn reconcile_wallet(
        &self,
        wallet: WalletAddress,
        entry: Option<&pe_trader_index::WatchlistEntry>,
        selected: &WalletObligations,
        fixed_end: i64,
    ) -> Result<Vec<(i64, Obligation)>, ReconciliationError> {
        let cursor_start = self
            .paper_state
            .cursor(&wallet)?
            .map(|value| value.saturating_sub(1));
        let obligation_start = selected
            .first_key_value()
            .map(|(epoch, _)| epoch.saturating_sub(1));
        let start = match (cursor_start, obligation_start) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, None) => left,
            (None, Some(_)) => None,
        };
        let append_closed = Arc::new(AtomicBool::new(false));
        let recording = RecordingFetcher {
            inner: self.fetcher.clone(),
            source_log: self.source_log.clone(),
            append_closed: append_closed.clone(),
            occurrences: Arc::new(Mutex::new(Vec::new())),
            now: self.now.clone(),
        };
        let activity = match fetch_complete_activity(
            &recording,
            &self.config.base_url,
            wallet,
            start,
            fixed_end,
        )
        .await
        {
            Ok(activity) => activity,
            Err(_) if append_closed.load(Ordering::Acquire) => {
                return Err(ReconciliationError::SourceLogClosed);
            }
            Err(error) => return Err(error.into()),
        };
        let page_occurrences = recording.join_occurrences(&activity.pages)?;
        let buckets = activity.buckets()?;
        if buckets.is_empty() {
            return Ok(if self.paper_state.is_wallet_fenced(&wallet)? {
                selected
                    .iter()
                    .flat_map(|(epoch, groups)| {
                        groups
                            .values()
                            .map(|obligation| (*epoch, obligation.clone()))
                    })
                    .collect()
            } else {
                Vec::new()
            });
        }
        // Resolve every required token and record its metadata before freezing the read commitment.
        let mut identities = Vec::with_capacity(buckets.len());
        for bucket in &buckets {
            identities.push(self.resolve_bucket(bucket).await?);
        }
        let correlation = self.correlate(
            wallet,
            selected,
            &buckets,
            &identities,
            &page_occurrences,
            &activity.pages,
        )?;
        let bindings = correlation
            .matched
            .iter()
            .map(|matched| matched.binding.clone())
            .collect::<Vec<_>>();
        let read_commitment = self
            .append_read_commitment(
                wallet,
                fixed_end,
                &page_occurrences,
                &activity.pages,
                &bindings,
            )
            .await?;
        if let Some(latest) = buckets
            .iter()
            .flatten()
            .map(|aggregate| aggregate.source_time.0.unix_timestamp())
            .max()
        {
            self.paper_state.set_activity(&wallet, latest)?;
        }
        let quality = match entry {
            Some(entry) => entry.reconstruction_quality,
            None => ReconstructionQuality::new(0)
                .map_err(|_| ReconciliationError::ReconstructionQuality)?,
        };
        let copy_eligible = entry.is_some_and(|entry| entry.tier == WatchlistTier::Active);
        // Only unmatched observations block ordering. Matched observations use the endpoint's
        // bucket clock; the original stream second remains in the binding and the age check.
        for (bucket, identities) in buckets.into_iter().zip(identities) {
            let source_epoch = bucket_epoch(&bucket)?;
            if correlation
                .unmatched_epoch
                .is_some_and(|epoch| source_epoch >= epoch)
                && correlation.ambiguous.is_empty()
            {
                break;
            }
            let mut context = self.context(
                fixed_end,
                quality,
                copy_eligible,
                &activity.pages,
                &page_occurrences,
                read_commitment,
                &bucket,
                identities,
                &correlation.matched,
            )?;
            if !correlation.ambiguous.is_empty() {
                let mut inputs: serde_json::Value =
                    serde_json::from_str(&context.decision_inputs_json)?;
                inputs["invalid_mapping_observations"] =
                    serde_json::to_value(&correlation.ambiguous)?;
                context.decision_inputs_json = serde_json::to_string(&inputs)?;
            }
            let result = self.commit_bucket(bucket, context).await?;
            if result.newly_fenced.is_some() {
                break;
            }
        }
        let mut resolved = Vec::new();
        for matched in &correlation.matched {
            if binding_target_disposed(&self.paper_state, &matched.binding)? {
                resolved.push((matched.epoch, matched.obligation.clone()));
            }
        }
        // A permanent wallet fence is the durable refusal for observations without a target.
        // Bound observations still require their exact target revision's disposition.
        if self.paper_state.is_wallet_fenced(&wallet)? {
            for (epoch, groups) in selected {
                for obligation in groups.values() {
                    if !correlation
                        .matched
                        .iter()
                        .any(|matched| matched.epoch == *epoch && matched.obligation == *obligation)
                    {
                        resolved.push((*epoch, obligation.clone()));
                    }
                }
            }
        }
        Ok(resolved)
    }

    fn correlate(
        &self,
        wallet: WalletAddress,
        selected: &WalletObligations,
        buckets: &[Vec<ActivityAggregate>],
        identities: &[BucketIdentities],
        occurrences: &[PageOccurrence],
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
    ) -> Result<Correlation, ReconciliationError> {
        let mut result = Correlation::default();
        if selected.is_empty() {
            return Ok(result);
        }
        let index = self.source_receipts.as_ref().ok_or_else(|| {
            ReconciliationError::Binding("source receipt index is absent".to_owned())
        })?;
        let aggregates = buckets.iter().flatten().collect::<Vec<_>>();
        let by_group = aggregates
            .iter()
            .map(|aggregate| (aggregate.group_id.key(), *aggregate))
            .collect::<HashMap<_, _>>();
        let mut target_pages = HashMap::new();
        for (occurrence_index, (occurrence, evidence)) in joined_read_pages(occurrences, pages)
            .map_err(|error| ReconciliationError::Binding(error.to_string()))?
            .into_iter()
            .enumerate()
        {
            if pages.iter().any(|page| {
                page.bounds == evidence.bounds
                    && page.offset == pe_source_polymarket_public::ACTIVITY_MAX_OFFSET
                    && page.row_count == pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT
            }) {
                continue;
            }
            let envelope = index
                .source_envelope(occurrence.receipt)
                .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
            let context = pe_source_polymarket_public::ActivityParseContext {
                source_id: envelope.source_id,
                observed_at: envelope.observed_at,
                received_at: envelope.received_at,
                transport: pe_source_polymarket_public::ActivityTransport::Rest,
            };
            let page = pe_source_polymarket_public::parse_activity_response(
                &envelope.payload,
                wallet,
                &context,
            )
            .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
            for row in page.rows {
                let group = row
                    .group_id()
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                target_pages
                    .entry(group.key().clone())
                    .or_insert(occurrence_index);
            }
        }
        for (epoch, groups) in selected {
            for obligation in groups.values() {
                let envelope = index
                    .source_envelope(obligation.receipt)
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                if envelope.source_id.0 != ACTIVITY_WS_SOURCE_ID
                    || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
                    || envelope.parser_version != ACTIVITY_PARSER_VERSION
                    || envelope.content_type != ContentType::Json
                {
                    return Err(ReconciliationError::Binding(
                        "stream source contract differs".to_owned(),
                    ));
                }
                let stream = parse_activity_trade_observation(&envelope.payload)
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                if stream.wallet != wallet
                    || stream.group_id.key() != &obligation.group_id
                    || stream.source_time.0.unix_timestamp() != *epoch
                {
                    return Err(ReconciliationError::Binding(
                        "stream receipt differs from the frozen obligation".to_owned(),
                    ));
                }
                let original = stream.group_id.components();
                let mut candidates = if let Some(exact) = by_group.get(&obligation.group_id) {
                    vec![*exact]
                } else {
                    aggregates
                        .iter()
                        .copied()
                        .filter(|aggregate| {
                            let candidate = aggregate.group_id.components();
                            candidate.activity_type == original.activity_type
                                && candidate.wallet == original.wallet
                                && candidate.transaction_hash == original.transaction_hash
                                && candidate.asset == original.asset
                                && candidate.side == original.side
                        })
                        .collect::<Vec<_>>()
                };
                let provenance = if by_group.contains_key(&obligation.group_id) {
                    None
                } else {
                    original
                        .asset
                        .as_ref()
                        .and_then(|asset| {
                            identities
                                .iter()
                                .find_map(|identities| identities.provenance.get(asset))
                        })
                        .cloned()
                };
                if !by_group.contains_key(&obligation.group_id) && provenance.is_none() {
                    candidates.clear();
                }
                if candidates.len() > 1 {
                    result
                        .ambiguous
                        .push((obligation.group_id.clone(), obligation.receipt));
                    continue;
                }
                let target = candidates.first().copied();
                let Some(target) = target else {
                    result.unmatched_epoch = Some(
                        result
                            .unmatched_epoch
                            .map_or(*epoch, |current| current.min(*epoch)),
                    );
                    continue;
                };
                let occurrence_index = *target_pages
                    .get(target.group_id.key())
                    .ok_or(ReconciliationError::PageReceiptMismatch)?;
                let identity_receipt = provenance
                    .as_ref()
                    .map(|proof| {
                        index
                            .receipt_at(pe_core_types::EventSeq(proof.source_log_sequence))
                            .map_err(|error| ReconciliationError::Binding(error.to_string()))?
                            .map(|(receipt, _)| receipt)
                            .ok_or_else(|| {
                                ReconciliationError::Binding(
                                    "metadata receipt is absent".to_owned(),
                                )
                            })
                    })
                    .transpose()?;
                result.matched.push(MatchedObservation {
                    epoch: *epoch,
                    obligation: obligation.clone(),
                    source_time: stream.source_time.0,
                    binding: ObservationBinding {
                        stream_group_id: obligation.group_id.clone(),
                        stream_receipt: obligation.receipt,
                        history_group_id: target.group_id.key().clone(),
                        semantic_revision: target.semantic_revision.as_str().to_owned(),
                        page_raw_hash: occurrences[occurrence_index].raw_hash.clone(),
                        page_occurrence_index: u32::try_from(occurrence_index)
                            .map_err(|_| ReconciliationError::PageReceiptMismatch)?,
                        identity_provenance: provenance,
                        identity_receipt,
                    },
                });
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn context(
        &self,
        fixed_end: i64,
        reconstruction_quality: ReconstructionQuality,
        copy_eligible: bool,
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
        page_occurrences: &[PageOccurrence],
        read_commitment: AppendReceipt,
        bucket: &[ActivityAggregate],
        identities: BucketIdentities,
        matched: &[MatchedObservation],
    ) -> Result<BucketDecisionContext, ReconciliationError> {
        let now = (self.now)();
        let mut observation_provenance = HashMap::new();
        let mut no_copy_dispositions = HashMap::new();
        let mut observed_source_receipts = HashMap::<SourceTradeId, AppendReceipt>::new();
        for aggregate in bucket {
            let group = aggregate.group_id.key().clone();
            let observation = matched
                .iter()
                .filter(|observation| observation.binding.history_group_id == group)
                .min_by_key(|observation| observation.obligation.receipt.sequence);
            let provenance = observation
                .map(|_| TradeProvenance::ActivityWs)
                .unwrap_or(TradeProvenance::RestPoll);
            if let Some(observation) = observation {
                observed_source_receipts.insert(group.clone(), observation.obligation.receipt);
            }
            let source_time = crate::bucket_commit::earliest_bound_source_time(
                aggregate.source_time.0,
                matched
                    .iter()
                    .filter(|observation| observation.binding.history_group_id == group)
                    .map(|observation| observation.source_time),
            );
            observation_provenance.insert(group.clone(), provenance);
            if aggregate.group_id.components().activity_type == ActivityType::Trade
                && let Some(disposition) = stale_disposition(
                    provenance,
                    source_time,
                    now,
                    self.config.activity_ws_enabled,
                    self.config.copy_latency_budget_secs,
                )
            {
                no_copy_dispositions.insert(group, disposition);
            }
        }
        for group in &identities.unresolved {
            let provenance = observation_provenance
                .get(group)
                .copied()
                .unwrap_or(TradeProvenance::RestPoll);
            let source_time = bucket
                .iter()
                .find(|aggregate| aggregate.group_id.key() == group)
                .map(|aggregate| aggregate.source_time.0)
                .unwrap_or(now);
            no_copy_dispositions.insert(
                group.clone(),
                identity_unresolved_disposition(provenance, source_time, now),
            );
        }
        Ok(BucketDecisionContext {
            applied_configuration: self.runtime_config.snapshot().as_ref().clone(),
            decision_inputs_json: serde_json::to_string(&serde_json::json!({
                "fixed_end": fixed_end,
                "pages": pages,
            }))?,
            page_occurrences: page_occurrences.to_vec(),
            observed_source_receipts,
            reconstruction_quality,
            read_commitment: Some(
                crate::bucket_commit::ActivityReadCommitmentReceipt::BindingsV2(read_commitment),
            ),
            signal_config: self.signal_config.clone(),
            copy_eligible,
            bracket_commit: false,
            recorded_at_unix: now.unix_timestamp(),
            observation_provenance,
            no_copy_dispositions,
            identity_overrides: identities.overrides,
            identity_unresolved: identities.unresolved,
            history_status: None,
        })
    }

    /// Synchronize the commitment record binding one complete read (#565); the receipt is
    /// acknowledged only after the durable append, exactly like a page.
    async fn append_read_commitment(
        &self,
        wallet: WalletAddress,
        fixed_end: i64,
        page_occurrences: &[PageOccurrence],
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
        bindings: &[ObservationBinding],
    ) -> Result<AppendReceipt, ReconciliationError> {
        let payload = activity_read_commitment_payload_v2(
            wallet,
            fixed_end,
            page_occurrences,
            pages,
            bindings,
        )
        .map_err(|_| ReconciliationError::PageReceiptMismatch)?;
        let recorded_at = (self.now)();
        self.source_log
            .append(EnvelopeIn {
                source_id: SourceId(ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                parser_version: ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
                observed_at: SourceTimestamp(recorded_at),
                received_at: ReceivedAt(recorded_at),
                content_type: ContentType::Json,
                payload,
            })
            .await
            .map_err(|SourceLogHandleError::Closed| ReconciliationError::SourceLogClosed)
    }

    async fn resolve_bucket(
        &self,
        bucket: &[ActivityAggregate],
    ) -> Result<BucketIdentities, ReconciliationError> {
        let tokens = bucket
            .iter()
            .filter_map(|aggregate| aggregate.group_id.components().asset.clone())
            .collect::<HashSet<PolymarketTokenId>>();
        let resolved = self
            .asset_identity
            .resolve(tokens)
            .await
            .map_err(ReconciliationError::Identity)?;
        let mut identities = BucketIdentities {
            provenance: resolved.provenance,
            ..BucketIdentities::default()
        };
        for aggregate in bucket {
            let components = aggregate.group_id.components();
            let Some(asset) = &components.asset else {
                continue;
            };
            let group = aggregate.group_id.key().clone();
            let Some(verified) = resolved.verified.get(asset) else {
                identities.unresolved.insert(group);
                continue;
            };
            let differs = components.condition_id.as_ref() != Some(&verified.condition_id)
                || components.outcome != Some(verified.outcome);
            if differs {
                identities.overrides.insert(
                    group,
                    IdentityOverride {
                        verified: MarketOutcomeId::new(
                            MarketId(VenueMarketId(verified.condition_id.0.clone())),
                            verified.outcome,
                        ),
                        evidence_hash: verified.evidence_hash.clone(),
                    },
                );
            }
        }
        Ok(identities)
    }

    async fn commit_bucket(
        &self,
        aggregates: Vec<ActivityAggregate>,
        context: BucketDecisionContext,
    ) -> Result<BucketCommitResult, ReconciliationError> {
        let (committed, result) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::CommitActivityBucket {
                aggregates,
                context: Arc::new(context),
                committed,
            })
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?;
        result
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?
            .map_err(ReconciliationError::BucketCommit)
    }
}

#[derive(Default)]
struct Correlation {
    matched: Vec<MatchedObservation>,
    unmatched_epoch: Option<i64>,
    ambiguous: Vec<(SourceTradeId, AppendReceipt)>,
}

struct MatchedObservation {
    epoch: i64,
    obligation: Obligation,
    source_time: OffsetDateTime,
    binding: ObservationBinding,
}

fn binding_target_disposed(
    paper_state: &PaperStateDb,
    binding: &ObservationBinding,
) -> Result<bool, pe_paper_state::PaperStateError> {
    paper_state.activity_revision_disposed(&binding.history_group_id, &binding.semantic_revision)
}

fn select_refresh_candidate(
    candidates: &[(usize, AnchorRefreshClass)],
    reanchor_turn: &mut bool,
) -> Option<usize> {
    let preferred = if *reanchor_turn {
        AnchorRefreshClass::ReanchorRequired
    } else {
        AnchorRefreshClass::AgeDue
    };
    *reanchor_turn = !*reanchor_turn;
    candidates
        .iter()
        .find(|(_, class)| *class == preferred)
        .or_else(|| candidates.first())
        .map(|(index, _)| *index)
}

fn bucket_epoch(bucket: &[ActivityAggregate]) -> Result<i64, ReconciliationError> {
    bucket
        .first()
        .map(|aggregate| aggregate.source_time.0.unix_timestamp())
        .ok_or_else(|| ReconciliationError::BucketCommit("empty activity bucket".to_owned()))
}

fn stale_disposition(
    provenance: TradeProvenance,
    source_time: OffsetDateTime,
    now: OffsetDateTime,
    activity_ws_enabled: bool,
    copy_latency_budget_secs: u64,
) -> Option<NoCopyDisposition> {
    if !activity_ws_enabled {
        return None;
    }
    let budget =
        time::Duration::seconds(i64::try_from(copy_latency_budget_secs).unwrap_or(i64::MAX));
    let age = now - source_time;
    if age <= budget {
        return None;
    }
    let (provenance, reason) = match provenance {
        TradeProvenance::RestPoll => ("rest_poll", "stale_fallback_past_copy_budget"),
        TradeProvenance::ActivityWs => ("activity_ws", "stale_activity_ws_past_copy_budget"),
    };
    Some(NoCopyDisposition {
        provenance: provenance.to_owned(),
        age_secs: age.whole_seconds(),
        reason: reason.to_owned(),
        recorded_at_unix: now.unix_timestamp(),
    })
}

fn identity_unresolved_disposition(
    provenance: TradeProvenance,
    source_time: OffsetDateTime,
    now: OffsetDateTime,
) -> NoCopyDisposition {
    NoCopyDisposition {
        provenance: match provenance {
            TradeProvenance::RestPoll => "rest_poll",
            TradeProvenance::ActivityWs => "activity_ws",
        }
        .to_owned(),
        age_secs: (now - source_time).whole_seconds(),
        reason: "identity_unresolved".to_owned(),
        recorded_at_unix: now.unix_timestamp(),
    }
}

struct RecordingFetcher {
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    inner: Arc<dyn ReconciliationFetcher>,
    source_log: SourceLogHandle,
    append_closed: Arc<AtomicBool>,
    occurrences: Arc<Mutex<Vec<PageOccurrence>>>,
}

impl RecordingFetcher {
    fn join_occurrences(
        &self,
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
    ) -> Result<Vec<PageOccurrence>, ReconciliationError> {
        let occurrences = self
            .occurrences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        joined_read_pages(&occurrences, pages)
            .map_err(|_| ReconciliationError::PageReceiptMismatch)?;
        Ok(occurrences.clone())
    }
}

impl ReconciliationFetcher for RecordingFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = self.inner.fetch(url).await?;
            let received_at = (self.now)();
            let envelope = EnvelopeIn {
                source_id: SourceId(ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: payload.clone(),
            };
            let receipt = self.source_log.append(envelope).await.map_err(
                |SourceLogHandleError::Closed| {
                    self.append_closed.store(true, Ordering::Release);
                    SourceError::Fatal {
                        message: "source-log coordinator closed".to_owned(),
                    }
                },
            )?;
            self.occurrences
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(PageOccurrence {
                    request_url: url.to_owned(),
                    raw_hash: blake3::hash(&payload).to_hex().to_string(),
                    receipt,
                });
            Ok(payload)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::CollateralAmount;
    use pe_event_log::Writer;
    use pe_paper_state::{ActivityBucketCommit, ActivityDispositionRecord};
    use tempfile::tempdir;

    use crate::paper_recovery::{
        PAPER_LOG_SCHEMA_VERSION, PaperLogRecord, PortfolioMark, QualificationStarted, TailBinding,
    };

    fn recorded(sequence: u64, url: &str, hash: &str) -> PageOccurrence {
        PageOccurrence {
            request_url: url.to_owned(),
            raw_hash: hash.to_owned(),
            receipt: AppendReceipt {
                sequence: pe_core_types::EventSeq(sequence),
                this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(0); 32]),
            },
        }
    }

    fn evidence(
        url: &str,
        hash: &str,
        end: i64,
    ) -> pe_source_polymarket_public::ReconciliationPageEvidence {
        pe_source_polymarket_public::ReconciliationPageEvidence {
            request_url: url.to_owned(),
            bounds: Some(pe_source_polymarket_public::ActivityRequestBounds {
                start: Some(end - 1),
                end,
            }),
            partition: None,
            offset: 0,
            row_count: 0,
            canonical_page_hash: format!("canonical-{end}"),
            raw_page_hash: hash.to_owned(),
            received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
        }
    }

    fn append_source_frame(
        writer: &mut Writer,
        source_id: &str,
        payload: &[u8],
        received_at_unix: i64,
        schema_version: u32,
        parser_version: u32,
    ) -> AppendReceipt {
        let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version,
                parser_version,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: payload.to_vec(),
            })
            .unwrap()
    }

    fn activity_payload(transaction_suffix: char, source_unix: i64) -> Vec<u8> {
        format!(
            r#"{{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","conditionId":"0xc1","asset":"123","side":"BUY","size":"5","price":"0.5","timestamp":{source_unix},"transactionHash":"0x{}","outcomeIndex":"0"}}"#,
            transaction_suffix.to_string().repeat(64)
        )
        .into_bytes()
    }

    fn resolve_activity_group(paper_state: &PaperStateDb, payload: &[u8]) {
        let activity = parse_activity_trade_observation(payload).unwrap();
        paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet: activity.wallet,
                source_epoch: activity.source_time.0.unix_timestamp(),
                dispositions: vec![ActivityDispositionRecord {
                    source_trade_id: activity.group_id.key().clone(),
                    transaction_hash: activity.group_id.components().transaction_hash.clone(),
                    wallet: activity.wallet,
                    source_epoch: activity.source_time.0.unix_timestamp(),
                    semantic_revision: "candidate-split-test-v1".to_owned(),
                    activity_type: "TRADE".to_owned(),
                    disposition: "decision_pending".to_owned(),
                    proof_json: "{\"version\":1}".to_owned(),
                    no_copy: None,
                }],
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
    }

    fn empty_tail() -> TailBinding {
        TailBinding {
            physical_tail: 0,
            last_sequence: None,
            last_hash: blake3::Hash::from_bytes([0; 32]).to_hex().to_string(),
        }
    }

    fn qualification_start(wallet: WalletAddress) -> PaperLogRecord {
        PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
            starting_bankroll: CollateralAmount::from_atomic(100_000_000),
            paper_prefix: empty_tail(),
            source_prefix: empty_tail(),
            live_prefix: empty_tail(),
            artifact_blake3: "artifact".to_owned(),
            static_config_hash: "static".to_owned(),
            hot_config_hash: "hot".to_owned(),
            generation: "generation".to_owned(),
            activation_id: "activation".to_owned(),
            ranking_batch_id: 572,
            membership: vec![wallet],
            membership_proofs_hash: "membership".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
        }))
    }

    fn append_paper_record(
        writer: &mut Writer,
        record: &PaperLogRecord,
        received_at_unix: i64,
    ) -> AppendReceipt {
        let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
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

    /// PASS: logically sorted saturated pages join by multiplicity while receipt output retains
    /// acquisition order, including repeated identical request/hash pairs.
    #[test]
    fn page_receipts_join_as_a_fifo_multiset() {
        let acquired = vec![
            recorded(1, "parent", "p"),
            recorded(2, "child", "same"),
            recorded(3, "child", "same"),
        ];
        let logical = vec![
            evidence("child", "same", 1),
            evidence("child", "same", 2),
            evidence("parent", "p", 3),
        ];
        let joined = joined_read_pages(&acquired, &logical).unwrap();
        assert_eq!(
            joined
                .iter()
                .map(|(page, _)| page.receipt.sequence.0)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let missing = vec![evidence("child", "same", 1), evidence("parent", "p", 2)];
        assert!(joined_read_pages(&acquired, &missing).is_err());
    }

    #[test]
    fn refresh_selection_alternates_due_classes_and_falls_through() {
        let candidates = [
            (0, AnchorRefreshClass::ReanchorRequired),
            (1, AnchorRefreshClass::AgeDue),
            (2, AnchorRefreshClass::ReanchorRequired),
            (3, AnchorRefreshClass::AgeDue),
        ];
        let mut reanchor_turn = true;
        assert_eq!(
            select_refresh_candidate(&candidates, &mut reanchor_turn),
            Some(0)
        );
        assert_eq!(
            select_refresh_candidate(&candidates[1..], &mut reanchor_turn),
            Some(1)
        );
        assert_eq!(
            select_refresh_candidate(&candidates[2..], &mut reanchor_turn),
            Some(2)
        );
        assert_eq!(
            select_refresh_candidate(&candidates[3..], &mut reanchor_turn),
            Some(3)
        );

        let only_age_due = [(7, AnchorRefreshClass::AgeDue)];
        assert_eq!(
            select_refresh_candidate(&only_age_due, &mut reanchor_turn),
            Some(7),
            "reanchor turn falls through to age-due"
        );
        assert!(!reanchor_turn, "the class turn still advances");
        assert_eq!(select_refresh_candidate(&[], &mut reanchor_turn), None);
        let only_reanchor = [(9, AnchorRefreshClass::ReanchorRequired)];
        assert_eq!(
            select_refresh_candidate(&only_reanchor, &mut reanchor_turn),
            Some(9)
        );
    }

    #[test]
    fn strict_copy_budget_boundary_preserves_both_sides() {
        for (stream_epoch, history_epoch) in [(100, 101), (101, 100)] {
            let stream = OffsetDateTime::from_unix_timestamp(stream_epoch).unwrap();
            let history = OffsetDateTime::from_unix_timestamp(history_epoch).unwrap();
            let oldest = stream.min(history);
            let boundary = oldest + time::Duration::seconds(2);
            assert!(
                stale_disposition(TradeProvenance::ActivityWs, oldest, boundary, true, 2).is_none()
            );
            assert!(
                stale_disposition(
                    TradeProvenance::ActivityWs,
                    oldest,
                    boundary + time::Duration::nanoseconds(1),
                    true,
                    2
                )
                .is_some()
            );
        }
        let source = OffsetDateTime::from_unix_timestamp(100).unwrap();
        assert!(
            stale_disposition(
                TradeProvenance::ActivityWs,
                source,
                source + time::Duration::seconds(1),
                true,
                2,
            )
            .is_none()
        );
        assert!(
            stale_disposition(
                TradeProvenance::ActivityWs,
                source,
                source + time::Duration::seconds(2),
                true,
                2,
            )
            .is_none()
        );
        let disposition = stale_disposition(
            TradeProvenance::ActivityWs,
            source,
            source + time::Duration::seconds(2) + time::Duration::nanoseconds(1),
            true,
            2,
        )
        .unwrap();
        assert_eq!(disposition.reason, "stale_activity_ws_past_copy_budget");
        assert_eq!(disposition.age_secs, 2);
    }

    #[test]
    fn obligations_coalesce_three_readers_by_wallet_group() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let source_time = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let mut obligations = ReconciliationObligations::default();
        for received in [103, 101, 102] {
            obligations.insert(ReconciliationTrigger {
                wallet,
                source_time,
                source_trade_id: SourceTradeId("g2:a".to_owned()),
                provenance: TradeProvenance::ActivityWs,
                received_at: OffsetDateTime::from_unix_timestamp(received).unwrap(),
                receipt: pe_event_log::AppendReceipt {
                    sequence: pe_core_types::EventSeq(u64::try_from(received).unwrap()),
                    this_hash: blake3::Hash::from_bytes([u8::try_from(received).unwrap_or(0); 32]),
                },
            });
        }
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations.earliest_epoch(&wallet), Some(100));
        let observation = obligations
            .observation(&wallet, 100, &SourceTradeId("g2:a".to_owned()))
            .unwrap();
        assert_eq!(observation.0.sequence, pe_core_types::EventSeq(101));
        assert_eq!(observation.1.unix_timestamp(), 101);
        let selected = obligations.by_wallet.get(&wallet).unwrap().clone();
        let snapshot = selected.clone();
        obligations.insert(ReconciliationTrigger {
            wallet,
            source_time: OffsetDateTime::from_unix_timestamp(101).unwrap(),
            source_trade_id: SourceTradeId("later-group".to_owned()),
            provenance: TradeProvenance::ActivityWs,
            received_at: OffsetDateTime::from_unix_timestamp(102).unwrap(),
            receipt: recorded(99, "later", "later").receipt,
        });
        assert_eq!(
            selected, snapshot,
            "later readers do not mutate a frozen attempt"
        );
        assert_eq!(obligations.len(), 2);
    }

    /// PASS: only an obligation received before the cutoff and appended inside the boundary prefix
    /// delays the boundary; either crossing order belongs to the later interval.
    #[test]
    fn boundary_uses_both_receipt_and_receive_time_bounds() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let source_time = OffsetDateTime::from_unix_timestamp(900).unwrap();
        let trigger = |id: &str, sequence: u64, received_at: i64| ReconciliationTrigger {
            wallet,
            source_time,
            source_trade_id: SourceTradeId(id.to_owned()),
            provenance: TradeProvenance::ActivityWs,
            received_at: OffsetDateTime::from_unix_timestamp(received_at).unwrap(),
            receipt: AppendReceipt {
                sequence: pe_core_types::EventSeq(sequence),
                this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(0); 32]),
            },
        };
        let mut obligations = ReconciliationObligations::default();
        assert!(obligations.install_boundary(PendingBoundary {
            cutoff_unix: 1_000,
            receipt: trigger("boundary", 10, 1_000).receipt,
        }));
        assert!(!obligations.install_boundary(PendingBoundary {
            cutoff_unix: 2_000,
            receipt: trigger("later", 20, 2_000).receipt,
        }));
        obligations.insert(trigger("post-receive", 9, 1_001));
        obligations.insert(trigger("late-append", 11, 999));
        assert!(obligations.boundary_ready());

        obligations.insert(trigger("qualifying", 8, 999));
        assert!(!obligations.boundary_ready());
        obligations.remove(&wallet, 900, &SourceTradeId("qualifying".to_owned()));
        assert_eq!(
            obligations.take_ready_boundary().unwrap().cutoff_unix,
            1_000
        );
    }

    /// PASS: restart/quiet-day catch-up yields every completed midnight after Start in oldest-first
    /// order and never repeats an existing boundary cutoff.
    #[test]
    fn completed_midnights_catch_up_oldest_first() {
        assert_eq!(
            completed_midnight_cutoffs_after(100, 3 * SECONDS_PER_DAY + 1).unwrap(),
            vec![SECONDS_PER_DAY, 2 * SECONDS_PER_DAY, 3 * SECONDS_PER_DAY]
        );
        assert_eq!(
            completed_midnight_cutoffs_after(SECONDS_PER_DAY, 2 * SECONDS_PER_DAY).unwrap(),
            vec![2 * SECONDS_PER_DAY]
        );
        assert!(
            completed_midnight_cutoffs_after(100, 99)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn activity_candidates_filter_after_database_changes_and_match_rebuild() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let paper_state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let resolved_payload = activity_payload('a', 100);
        let unresolved_payload = activity_payload('b', 101);
        let suffix_payload = activity_payload('c', 102);
        let unresolved_group = parse_activity_trade_observation(&unresolved_payload)
            .unwrap()
            .group_id
            .key()
            .clone();
        let suffix_group = parse_activity_trade_observation(&suffix_payload)
            .unwrap()
            .group_id
            .key()
            .clone();

        let mut writer = Writer::open(&source_path).unwrap();
        append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &resolved_payload,
            110,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        let first_unresolved = append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &unresolved_payload,
            111,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &unresolved_payload,
            112,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );

        let initial = Reader::replay(&source_path)
            .unwrap()
            .map(|item| item.unwrap().1)
            .collect::<Vec<_>>();
        let mut candidates = ActivityCandidates::default();
        for envelope in &initial {
            candidates.observe_activity(envelope).unwrap();
        }
        resolve_activity_group(&paper_state, &resolved_payload);

        let suffix_receipt = append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &suffix_payload,
            113,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        let suffix = Reader::replay(&source_path)
            .unwrap()
            .nth(initial.len())
            .unwrap()
            .unwrap()
            .1;
        candidates.observe_activity(&suffix).unwrap();
        drop(writer);

        let split = candidates
            .into_obligations(
                &paper_state,
                &SourceReceiptIndex::replay(&source_path).unwrap(),
            )
            .unwrap();
        let rebuilt = rebuild_reconciliation_obligations(&source_path, &paper_state).unwrap();
        assert_eq!(split, rebuilt);
        assert_eq!(split.len(), 2);
        assert_eq!(
            split
                .observation(
                    &WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
                    101,
                    &unresolved_group,
                )
                .unwrap()
                .0,
            first_unresolved,
            "duplicate observations retain their earliest source receipt"
        );
        assert_eq!(
            split
                .observation(
                    &WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
                    102,
                    &suffix_group,
                )
                .unwrap()
                .0,
            suffix_receipt,
            "a later-observed log suffix remains after the final state filter"
        );
    }

    #[test]
    fn malformed_activity_candidate_matches_rebuild_error() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let paper_state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let mut writer = Writer::open(&source_path).unwrap();
        append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            b"{",
            100,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        drop(writer);
        let envelope = Reader::replay(&source_path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1;
        let observed = ActivityCandidates::default()
            .observe_activity(&envelope)
            .unwrap_err();
        let rebuilt = rebuild_reconciliation_obligations(&source_path, &paper_state).unwrap_err();
        assert!(matches!(observed, ObligationRebuildError::Activity(_)));
        assert!(matches!(rebuilt, ObligationRebuildError::Activity(_)));
        assert_eq!(observed.to_string(), rebuilt.to_string());
    }

    #[test]
    fn daily_boundary_candidates_match_recovery_with_paper_anchor() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let paper_path = dir.path().join("paper.log");
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut paper_writer = Writer::open(&paper_path).unwrap();
        append_paper_record(&mut paper_writer, &qualification_start(wallet), 100);
        append_paper_record(
            &mut paper_writer,
            &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt: AppendReceipt {
                    sequence: pe_core_types::EventSeq(7),
                    this_hash: blake3::Hash::from_bytes([7; 32]),
                },
                cutoff_unix: 200,
                source_tail: empty_tail(),
                financial_prefix_seq: None,
                prices: Vec::new(),
                cash: rust_decimal::Decimal::ZERO,
                equity: rust_decimal::Decimal::ZERO,
                invalid: None,
            })),
            201,
        );
        drop(paper_writer);

        let mut source_writer = Writer::open(&source_path).unwrap();
        for cutoff_unix in [300, 150] {
            append_source_frame(
                &mut source_writer,
                DAILY_BOUNDARY_SOURCE_ID,
                &serde_json::to_vec(&serde_json::json!({
                    "kind": "daily_boundary",
                    "cutoff_unix": cutoff_unix,
                }))
                .unwrap(),
                cutoff_unix,
                1,
                1,
            );
        }
        let expected = append_source_frame(
            &mut source_writer,
            DAILY_BOUNDARY_SOURCE_ID,
            &serde_json::to_vec(&serde_json::json!({
                "kind": "daily_boundary",
                "cutoff_unix": 250,
            }))
            .unwrap(),
            250,
            1,
            1,
        );
        drop(source_writer);

        let mut candidates = DailyBoundaryCandidates::default();
        for item in Reader::replay(&source_path).unwrap() {
            candidates.observe_daily_boundary(&item.unwrap().1).unwrap();
        }
        let mut split = ReconciliationObligations::default();
        let anchor = recover_daily_boundary_anchor(&paper_path, &mut split)
            .unwrap()
            .unwrap();
        recover_daily_boundary_from_candidates(candidates, anchor, &mut split);

        let mut recovered = ReconciliationObligations::default();
        recover_daily_boundary(&source_path, &paper_path, &mut recovered).unwrap();
        assert_eq!(split, recovered);
        assert_eq!(split.boundary_anchor(), Some(200));
        assert_eq!(
            split.pending_boundary(),
            Some(PendingBoundary {
                cutoff_unix: 250,
                receipt: expected,
            })
        );
    }

    #[test]
    fn malformed_daily_boundary_candidates_preserve_errors() {
        let dir = tempdir().unwrap();
        for (name, payload, expected) in [
            (
                "kind",
                serde_json::to_vec(&serde_json::json!({"kind": "other", "cutoff_unix": 1}))
                    .unwrap(),
                "boundary source id carries an unexpected kind",
            ),
            (
                "cutoff",
                serde_json::to_vec(&serde_json::json!({"kind": "daily_boundary"})).unwrap(),
                "boundary cutoff is absent",
            ),
        ] {
            let path = dir.path().join(format!("{name}.log"));
            let mut writer = Writer::open(&path).unwrap();
            append_source_frame(&mut writer, DAILY_BOUNDARY_SOURCE_ID, &payload, 1, 1, 1);
            drop(writer);
            let envelope = Reader::replay(&path).unwrap().next().unwrap().unwrap().1;
            let error = DailyBoundaryCandidates::default()
                .observe_daily_boundary(&envelope)
                .unwrap_err();
            assert!(matches!(
                error,
                ObligationRebuildError::Boundary(ref message) if message == expected
            ));
        }
    }

    #[test]
    fn daily_boundary_recovery_before_financial_start_does_not_read_source_log() {
        let dir = tempdir().unwrap();
        let paper_path = dir.path().join("paper.log");
        let missing_source_path = dir.path().join("source-does-not-exist.log");
        drop(Writer::open(&paper_path).unwrap());

        let mut obligations = ReconciliationObligations::default();
        recover_daily_boundary(&missing_source_path, &paper_path, &mut obligations).unwrap();

        assert!(obligations.boundary_anchor().is_none());
        assert!(obligations.pending_boundary().is_none());
        assert!(!missing_source_path.exists());
    }
}
