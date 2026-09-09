//! Durable fixed-end Polymarket activity reconciliation (#544).
//!
//! Websocket observations and public polling pages are raw evidence. This owner
//! coalesces their wakeups per wallet, walks the existing paged/rate-gated REST
//! reader to a fixed end, and sends only complete epoch-second buckets to the
//! orchestrator's single [`crate::bucket_commit::BucketCommitEngine`] owner.
//! A websocket observation remains an obligation, derived from the source log
//! on restart, until its group has a durable terminal/apply record.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
use tracing::warn;

use crate::activity_ingest::{
    ACTIVITY_WS_SOURCE_ID, ReconciliationTrigger, SourceLogHandle, SourceLogHandleError,
};
use crate::asset_identity::AssetIdentityResolver;
use crate::bucket_commit::{
    ACTIVITY_READ_COMMITMENT_PARSER_VERSION, ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
    ACTIVITY_READ_COMMITMENT_SOURCE_ID, BucketCommitResult, BucketDecisionContext,
    IdentityOverride, PageOccurrence, activity_read_commitment_payload, joined_read_pages,
};
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
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

type CoalescedObligations = HashMap<WalletAddress, BTreeMap<i64, BTreeMap<String, Obligation>>>;

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

#[derive(Default)]
struct BucketIdentities {
    overrides: HashMap<SourceTradeId, IdentityOverride>,
    unresolved: HashSet<SourceTradeId>,
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
}

impl ActivityCandidates {
    /// Observe one source frame without consulting paper state (#572).
    pub(crate) fn observe_activity(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<(), ObligationRebuildError> {
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
    ) -> Result<ReconciliationObligations, ObligationRebuildError> {
        let mut obligations = ReconciliationObligations::default();
        for (wallet, epochs) in self.by_wallet {
            for (epoch, groups) in epochs {
                for obligation in groups.into_values() {
                    if paper_state
                        .activity_group_state(&obligation.group_id)?
                        .is_none()
                    {
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

    fn earliest_epoch(&self, wallet: &WalletAddress) -> Option<i64> {
        self.by_wallet
            .get(wallet)
            .and_then(|epochs| epochs.first_key_value().map(|(epoch, _)| *epoch))
    }

    fn groups_at(&self, wallet: &WalletAddress, epoch: i64) -> Vec<SourceTradeId> {
        self.by_wallet
            .get(wallet)
            .and_then(|epochs| epochs.get(&epoch))
            .map(|groups| {
                groups
                    .values()
                    .map(|obligation| obligation.group_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn contains(&self, wallet: &WalletAddress, epoch: i64, group: &SourceTradeId) -> bool {
        self.by_wallet
            .get(wallet)
            .and_then(|epochs| epochs.get(&epoch))
            .is_some_and(|groups| groups.contains_key(&group.0))
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
    for item in Reader::replay(source_log_path)? {
        let (_seq, envelope) = item?;
        candidates.observe_activity(&envelope)?;
    }
    candidates.into_obligations(paper_state)
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
        }
    }

    /// Deterministic reconciliation clock for hermetic scenarios.
    #[cfg(feature = "scenario")]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// Run on the pre-existing poll cadence; durable websocket triggers coalesce
    /// while that cadence elapses and never create a per-row fetch loop.
    pub async fn run(self) {
        let _ = self.run_until(std::future::pending::<()>()).await;
    }

    /// Production entry point. A shutdown request is honored only between complete fixed-end
    /// reconciliation rounds, so a partially applied wallet round is never manufactured.
    pub async fn run_until(
        mut self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), TradePollerOwnerError> {
        {
            let mut health = self
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            health.poll_started_at = Some(OffsetDateTime::now_utc());
        }
        tokio::pin!(shutdown);
        loop {
            self.drain_triggers();
            self.poll_round().await?;
            if self.config.poll_interval_secs == 0 {
                return Err(TradePollerOwnerError::ZeroPollInterval);
            }
            let cadence = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs));
            tokio::pin!(cadence);
            loop {
                tokio::select! {
                    biased;
                    () = &mut shutdown => return Ok(()),
                    () = &mut cadence => break,
                    trigger = self.trigger_rx.recv() => match trigger {
                        Some(trigger) => self.obligations.insert(trigger),
                        None => return Err(TradePollerOwnerError::TriggerChannelClosed),
                    }
                }
            }
        }
    }

    fn drain_triggers(&mut self) {
        while let Ok(trigger) = self.trigger_rx.try_recv() {
            self.obligations.insert(trigger);
        }
    }

    async fn poll_round(&mut self) -> Result<(), TradePollerOwnerError> {
        self.install_next_boundary().await?;
        let snapshot = self.live_watchlist.snapshot();
        let mut live_wallets: Vec<WalletAddress> =
            snapshot.entries.iter().map(|entry| entry.wallet).collect();
        live_wallets.sort_by_key(ToString::to_string);
        live_wallets.dedup();
        let mut wallets = live_wallets.clone();
        wallets.extend(self.obligations.wallets());
        wallets.sort_by_key(ToString::to_string);
        wallets.dedup();
        let mut successes = 0usize;
        let mut failures = 0usize;

        for wallet in wallets {
            let entry = snapshot.entries.iter().find(|entry| entry.wallet == wallet);
            match self.reconcile_wallet(wallet, entry).await {
                Ok(()) => successes = successes.saturating_add(1),
                Err(error) if error.retryable() => {
                    failures = failures.saturating_add(1);
                    warn!(wallet = %wallet, error = %error, "fixed-end activity reconciliation will retry");
                }
                Err(error) => {
                    warn!(wallet = %wallet, error = %error, "fixed-end activity reconciliation owner stopped");
                    return Err(TradePollerOwnerError::Reconciliation(error.to_string()));
                }
            }
        }

        if successes > 0 || failures > 0 {
            let mut health = self
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if successes > 0 {
                let now = OffsetDateTime::now_utc();
                health.polymarket_last_event_at = Some(now);
                health.poll_last_round_at = Some(now);
                health.poll_error_streak = 0;
            } else {
                health.poll_error_streak = health.poll_error_streak.saturating_add(1);
            }
        }
        self.refresh_one_wallet(&live_wallets).await?;
        self.publish_ready_boundary().await?;
        Ok(())
    }

    async fn install_next_boundary(&mut self) -> Result<(), TradePollerOwnerError> {
        if self.obligations.pending_boundary().is_some() {
            return Ok(());
        }
        let Some(anchor) = self.obligations.boundary_anchor() else {
            return Ok(());
        };
        let Some(cutoff_unix) =
            completed_midnight_cutoffs_after(anchor, (self.now)().unix_timestamp())
                .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?
                .into_iter()
                .next()
        else {
            return Ok(());
        };
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
                payload: serde_json::to_vec(&serde_json::json!({
                    "kind": "daily_boundary",
                    "cutoff_unix": cutoff_unix,
                }))
                .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?,
            })
            .await
            .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?;
        self.obligations.install_boundary(PendingBoundary {
            cutoff_unix,
            receipt,
        });
        Ok(())
    }

    async fn publish_ready_boundary(&mut self) -> Result<(), TradePollerOwnerError> {
        let Some(boundary) = self.obligations.take_ready_boundary() else {
            return Ok(());
        };
        let (acknowledged, received) = oneshot::channel();
        if self
            .control_tx
            .send(OrchestratorControl::DailyBoundary {
                cutoff_unix: boundary.cutoff_unix,
                boundary_receipt: boundary.receipt,
                acknowledged,
            })
            .await
            .is_err()
        {
            self.obligations.install_boundary(boundary);
            return Err(TradePollerOwnerError::DailyBoundary(
                "orchestrator control channel closed".to_owned(),
            ));
        }
        match received.await {
            Ok(Ok(())) => self.obligations.set_boundary_anchor(boundary.cutoff_unix),
            Ok(Err(error)) => {
                self.obligations.install_boundary(boundary);
                return Err(TradePollerOwnerError::DailyBoundary(error));
            }
            Err(_) => {
                self.obligations.install_boundary(boundary);
                return Err(TradePollerOwnerError::DailyBoundary(
                    "daily boundary acknowledgement closed".to_owned(),
                ));
            }
        }
        Ok(())
    }

    async fn refresh_one_wallet(
        &mut self,
        wallets: &[WalletAddress],
    ) -> Result<(), TradePollerOwnerError> {
        let Some(preparer) = self.admission_preparer.as_ref() else {
            return Ok(());
        };
        if wallets.is_empty() {
            self.refresh_cursor = 0;
            return Ok(());
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
            return Ok(());
        };
        let wallet = wallets[index];
        self.refresh_cursor = index.saturating_add(1) % wallets.len();
        match preparer
            .prepare_if_due(wallet, now_unix, ANCHOR_REFRESH_SECS)
            .await
            .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?
        {
            AnchorRefreshOutcome::Anchored
            | AnchorRefreshOutcome::Skipped
            | AnchorRefreshOutcome::Deferred => {}
        }
        Ok(())
    }

    async fn reconcile_wallet(
        &mut self,
        wallet: WalletAddress,
        entry: Option<&pe_trader_index::WatchlistEntry>,
    ) -> Result<(), ReconciliationError> {
        let cursor = self.paper_state.cursor(&wallet)?;
        let cursor_start = cursor.map(|value| value.saturating_sub(1));
        let obligation_start = self
            .obligations
            .earliest_epoch(&wallet)
            .map(|value| value.saturating_sub(1));
        let start = match (cursor_start, obligation_start) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, None) => left,
            (None, Some(_)) => None,
        };
        let fixed_end = (self.now)().unix_timestamp();
        let append_closed = Arc::new(AtomicBool::new(false));
        let recording = RecordingFetcher {
            inner: self.fetcher.clone(),
            source_log: self.source_log.clone(),
            append_closed: append_closed.clone(),
            occurrences: Arc::new(Mutex::new(Vec::new())),
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
            return Ok(());
        }
        // One commitment per complete read with buckets, synchronized before any bucket commits
        // (#565); every decision frozen from this read references it.
        let read_commitment = self
            .append_read_commitment(wallet, fixed_end, &page_occurrences, &activity.pages)
            .await?;
        if let Some(latest_activity) = buckets
            .iter()
            .flatten()
            .map(|aggregate| aggregate.source_time.0.unix_timestamp())
            .max()
        {
            // Preserve the independent source-activity clock even while an
            // older websocket obligation deliberately holds bucket apply and
            // the lower-bound cursor (#511/#544).
            self.paper_state.set_activity(&wallet, latest_activity)?;
        }
        let quality = match entry {
            Some(entry) => entry.reconstruction_quality,
            None => ReconstructionQuality::new(0)
                .map_err(|_| ReconciliationError::ReconstructionQuality)?,
        };
        let copy_eligible = entry.is_some_and(|entry| entry.tier == WatchlistTier::Active);

        for bucket in buckets {
            let source_epoch = bucket_epoch(&bucket)?;
            let blocker = self.obligations.earliest_epoch(&wallet);
            if blocker.is_some_and(|epoch| source_epoch > epoch) {
                break;
            }
            let bucket_ids: HashSet<SourceTradeId> = bucket
                .iter()
                .map(|aggregate| aggregate.group_id.key().clone())
                .collect();
            if blocker.is_some_and(|epoch| epoch == source_epoch)
                && !self
                    .obligations
                    .groups_at(&wallet, source_epoch)
                    .iter()
                    .all(|group| bucket_ids.contains(group))
            {
                break;
            }
            let identities = self.resolve_bucket(&bucket).await?;
            let context = self.context(
                wallet,
                source_epoch,
                fixed_end,
                quality,
                copy_eligible,
                &activity.pages,
                &page_occurrences,
                read_commitment,
                &bucket,
                identities,
            )?;
            let result = self.commit_bucket(bucket, context).await?;
            for group in bucket_ids {
                if self.obligations.contains(&wallet, source_epoch, &group)
                    && self.paper_state.activity_group_state(&group)?.is_some()
                {
                    self.obligations.remove(&wallet, source_epoch, &group);
                }
            }
            if result.newly_fenced.is_some() {
                break;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn context(
        &self,
        wallet: WalletAddress,
        source_epoch: i64,
        fixed_end: i64,
        reconstruction_quality: ReconstructionQuality,
        copy_eligible: bool,
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
        page_occurrences: &[PageOccurrence],
        read_commitment: AppendReceipt,
        bucket: &[ActivityAggregate],
        identities: BucketIdentities,
    ) -> Result<BucketDecisionContext, ReconciliationError> {
        let now = (self.now)();
        let mut observation_provenance = HashMap::new();
        let mut no_copy_dispositions = HashMap::new();
        let mut observed_source_receipts = HashMap::<SourceTradeId, AppendReceipt>::new();
        for aggregate in bucket {
            let group = aggregate.group_id.key().clone();
            let observation = self.obligations.observation(&wallet, source_epoch, &group);
            let provenance = observation
                .map(|_| TradeProvenance::ActivityWs)
                .unwrap_or(TradeProvenance::RestPoll);
            if let Some((receipt, _received_at)) = observation {
                observed_source_receipts.insert(group.clone(), receipt);
            }
            observation_provenance.insert(group.clone(), provenance);
            if aggregate.group_id.components().activity_type == ActivityType::Trade
                && let Some(disposition) = stale_disposition(
                    provenance,
                    aggregate.source_time.0,
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
            read_commitment: Some(read_commitment),
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
    ) -> Result<AppendReceipt, ReconciliationError> {
        let payload = activity_read_commitment_payload(wallet, fixed_end, page_occurrences, pages)
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
        let mut identities = BucketIdentities::default();
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
            let received_at = OffsetDateTime::now_utc();
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
    }

    #[test]
    fn strict_copy_budget_boundary_preserves_both_sides() {
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

        let split = candidates.into_obligations(&paper_state).unwrap();
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
