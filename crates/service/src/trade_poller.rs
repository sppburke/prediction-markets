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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pe_copy_signal_engine::{SignalConfig, TradeProvenance};
use pe_core_types::{
    MarketId, MarketOutcomeId, PolymarketTokenId, ReceivedAt, ReconstructionQuality, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Reader};
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
use crate::bucket_commit::{BucketCommitResult, BucketDecisionContext, IdentityOverride};
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
use crate::runtime_config::LiveRuntimeConfig;
use crate::watchlist_admission::{AdmissionPreparer, AnchorRefreshOutcome, anchor_refresh_due};

/// Source id stamped on every fixed-end activity page before it is parsed.
pub const ACTIVITY_POLL_SOURCE_ID: &str = "polymarket-public.activity-reconciliation";
/// Best-effort cadence for refreshing venue-authoritative position anchors.
pub const ANCHOR_REFRESH_SECS: u64 = 3_600;

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
    received_at_unix: i64,
}

#[derive(Default)]
struct BucketIdentities {
    overrides: HashMap<SourceTradeId, IdentityOverride>,
    unresolved: HashSet<SourceTradeId>,
}

/// Coalesced durable websocket work rebuilt from source evidence on restart.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconciliationObligations {
    by_wallet: HashMap<WalletAddress, BTreeMap<i64, BTreeMap<String, Obligation>>>,
}

impl ReconciliationObligations {
    /// Add one already-durable reader observation. Reader duplicates coalesce
    /// by wallet, source second, and version-two group identity.
    pub fn insert(&mut self, trigger: ReconciliationTrigger) {
        let epoch = trigger.source_time.unix_timestamp();
        let groups = self
            .by_wallet
            .entry(trigger.wallet)
            .or_default()
            .entry(epoch)
            .or_default();
        groups
            .entry(trigger.source_trade_id.0.clone())
            .and_modify(|obligation| {
                obligation.received_at_unix = obligation
                    .received_at_unix
                    .min(trigger.received_at.unix_timestamp());
            })
            .or_insert(Obligation {
                group_id: trigger.source_trade_id,
                received_at_unix: trigger.received_at.unix_timestamp(),
            });
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
                            "received_at_unix": obligation.received_at_unix,
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
}

/// Rebuild unresolved websocket obligations before any producer starts.
pub fn rebuild_reconciliation_obligations(
    source_log_path: &Path,
    paper_state: &PaperStateDb,
) -> Result<ReconciliationObligations, ObligationRebuildError> {
    let mut obligations = ReconciliationObligations::default();
    for item in Reader::replay(source_log_path)? {
        let (_seq, envelope) = item?;
        if envelope.source_id.0 != ACTIVITY_WS_SOURCE_ID
            || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
            || envelope.parser_version != ACTIVITY_PARSER_VERSION
        {
            continue;
        }
        let activity = parse_activity_trade_observation(&envelope.payload)?;
        if paper_state
            .activity_group_state(activity.group_id.key())?
            .is_none()
        {
            obligations.insert(ReconciliationTrigger {
                wallet: activity.wallet,
                source_time: activity.source_time.0,
                source_trade_id: activity.group_id.key().clone(),
                provenance: TradeProvenance::ActivityWs,
                received_at: envelope.received_at.0,
            });
        }
    }
    Ok(obligations)
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
        let buckets = activity.buckets()?;
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
        bucket: &[ActivityAggregate],
        identities: BucketIdentities,
    ) -> Result<BucketDecisionContext, ReconciliationError> {
        let now = (self.now)();
        let mut observation_provenance = HashMap::new();
        let mut no_copy_dispositions = HashMap::new();
        for aggregate in bucket {
            let group = aggregate.group_id.key().clone();
            let provenance = if self.obligations.contains(&wallet, source_epoch, &group) {
                TradeProvenance::ActivityWs
            } else {
                TradeProvenance::RestPoll
            };
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
            reconstruction_quality,
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
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: payload.clone(),
            };
            self.source_log
                .append(envelope)
                .await
                .map_err(|SourceLogHandleError::Closed| {
                    self.append_closed.store(true, Ordering::Release);
                    SourceError::Fatal {
                        message: "source-log coordinator closed".to_owned(),
                    }
                })?;
            Ok(payload)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
            });
        }
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations.earliest_epoch(&wallet), Some(100));
    }
}
