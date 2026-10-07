//! Runtime ownership of a verified, incrementally extended checkpoint prefix.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pe_event_log::{LogError, LogTailBinding, Scanner};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use super::retention::{self, PreparedRetention, RetentionCommitOutcome, RetentionContext};
use super::{CheckpointData, PublishError, PublishOutcome, SerializedCandidate};
use crate::risk_inputs::SourceReceiptIndex;
use crate::source_log_boot::{ACTIVITY_REDUCER_VERSION, FrozenCheckpoint, FrozenPrefix, Reducers};
use crate::supervisor::{
    ShutdownPhase, ShutdownReceiver, TaskExit, TaskFailure, TaskFailureKind, TaskResult,
};

pub const CHECKPOINT_PUBLISH_SECS: u64 = 3_600;
pub const CHECKPOINT_RETRY_SECS: u64 = 60;

#[derive(Debug, Error)]
enum OwnerError {
    #[error("source checkpoint prefix mismatch at {offset}: expected {expected}, found {actual}")]
    PrefixMismatch {
        offset: u64,
        expected: String,
        actual: String,
    },
    #[error("source checkpoint walk binding changed: expected {expected:?}, found {actual:?}")]
    BindingMismatch {
        expected: Box<LogTailBinding>,
        actual: Box<LogTailBinding>,
    },
    #[error("source checkpoint capture had no readable authority; quiesced recovery required")]
    AuthorityUnavailable,
    #[error("source checkpoint authority generation changed from {candidate} to {current}")]
    GenerationChanged { candidate: u64, current: u64 },
    #[error("source checkpoint scan: {0}")]
    Scan(#[from] LogError),
    #[error("source checkpoint projection: {0:#}")]
    Projection(#[from] anyhow::Error),
    #[error("source checkpoint serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("source checkpoint clock or scenario seam: {0}")]
    Io(#[from] std::io::Error),
    #[error("source checkpoint blocking job join: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("source checkpoint job slot is cancelled")]
    Cancelled,
}

enum JobOutput {
    Candidate(Box<FrozenCheckpoint>, Option<Arc<SerializedCandidate>>),
    Publication(Result<PublishOutcome, PublishError>),
    RetentionPrepared(Box<FrozenCheckpoint>, PreparedRetention),
    RetentionCommitted(RetentionCommitOutcome),
    RetentionWindow(
        Box<FrozenCheckpoint>,
        HashSet<pe_core_types::WalletAddress>,
        Option<Arc<SerializedCandidate>>,
    ),
    Punched(u64),
    Paused(&'static str),
}

type Job = JoinHandle<Result<JobOutput, TaskFailure>>;

/// The async owner and main share this slot. Aborting the owner never detaches its blocking child.
#[derive(Clone, Default)]
pub struct CheckpointJobSlot {
    job: Arc<Mutex<Option<Job>>>,
    cancelled: Arc<AtomicBool>,
    quarantine_failed: Arc<AtomicBool>,
}

impl CheckpointJobSlot {
    #[must_use]
    pub fn quarantine_failed(&self) -> bool {
        self.quarantine_failed.load(Ordering::Acquire)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    async fn execute(
        &self,
        work: impl FnOnce(Arc<AtomicBool>) -> Result<JobOutput, TaskFailure> + Send + 'static,
    ) -> Result<JobOutput, TaskFailure> {
        let mut slot = self.job.lock().await;
        if self.cancelled.load(Ordering::Acquire) {
            return Err(TaskFailure::typed(OwnerError::Cancelled));
        }
        let cancel = self.cancelled.clone();
        *slot = Some(tokio::task::spawn_blocking(move || {
            if cancel.load(Ordering::Acquire) {
                return Err(TaskFailure::typed(OwnerError::Cancelled));
            }
            work(cancel)
        }));
        let result = match slot.as_mut() {
            Some(job) => job
                .await
                .map_err(|error| TaskFailure::typed(OwnerError::Join(error))),
            None => return Err(TaskFailure::typed(OwnerError::Cancelled)),
        };
        *slot = None;
        if self.cancelled.load(Ordering::Acquire) {
            // A cancelled walk cannot advance the state or start a publication.
            return Err(TaskFailure::typed(OwnerError::Cancelled));
        }
        result?
    }

    /// Cancel and join any blocking child left after supervisor joins/aborts. Main supplies the bound.
    pub async fn join(&self) -> Result<(), tokio::task::JoinError> {
        self.cancel();
        let mut slot = self.job.lock().await;
        let result = match slot.as_mut() {
            Some(job) => job.await.map(|_| ()),
            None => Ok(()),
        };
        *slot = None;
        result
    }
}

#[cfg(feature = "scenario")]
type PublicationObserver = Arc<dyn Fn(&[u8], u64, bool) + Send + Sync>;

/// Scenario-only boundaries; production always uses the scanner and durable publisher directly.
#[cfg(feature = "scenario")]
#[derive(Default)]
pub struct CheckpointOwnerHooks {
    pub publish_interval: Option<Duration>,
    pub before_hash: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub before_walk: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub checkpoint_write: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub after_publication: Option<PublicationObserver>,
    pub after_publication_applied: Option<Arc<dyn Fn(Option<u64>) + Send + Sync>>,
    pub clock: Option<Arc<dyn Fn() -> std::io::Result<u64> + Send + Sync>>,
    pub before_invalidation: Option<Arc<dyn Fn() + Send + Sync>>,
    pub invalidation: super::InvalidationHooks,
}

pub struct SourceCheckpointOwner {
    frozen: Option<FrozenCheckpoint>,
    receipts: SourceReceiptIndex,
    slot: CheckpointJobSlot,
    pending: Option<Arc<SerializedCandidate>>,
    paper_state: Option<Arc<pe_paper_state::PaperStateDb>>,
    attempt: u32,
    last_published_capture: Option<u64>,
    retention: Option<RetentionContext>,
    retention_epoch: u64,
    finishing_retention: bool,
    retention_walk_complete: bool,
    retention_tail: Option<LogTailBinding>,
    window_wallets: HashSet<pe_core_types::WalletAddress>,
    published_observation_wallets: HashSet<pe_core_types::WalletAddress>,
    #[cfg(feature = "scenario")]
    hooks: Arc<CheckpointOwnerHooks>,
}

impl Drop for SourceCheckpointOwner {
    fn drop(&mut self) {
        self.slot.cancel();
    }
}

impl SourceCheckpointOwner {
    pub(crate) fn new(
        frozen: FrozenCheckpoint,
        receipts: SourceReceiptIndex,
        slot: CheckpointJobSlot,
    ) -> Self {
        Self {
            frozen: Some(frozen),
            receipts,
            slot,
            pending: None,
            paper_state: None,
            attempt: 0,
            last_published_capture: None,
            retention: None,
            retention_epoch: 0,
            finishing_retention: false,
            retention_walk_complete: false,
            retention_tail: None,
            window_wallets: HashSet::new(),
            published_observation_wallets: HashSet::new(),
            #[cfg(feature = "scenario")]
            hooks: Arc::new(CheckpointOwnerHooks::default()),
        }
    }

    #[must_use]
    pub fn with_paper_state(mut self, paper_state: Arc<pe_paper_state::PaperStateDb>) -> Self {
        self.paper_state = Some(paper_state);
        self
    }

    /// Construct the actual owner around recorded fixtures without installing migration metadata.
    #[cfg(feature = "scenario")]
    pub fn for_retention_scenario(
        activation: LogTailBinding,
        receipts: SourceReceiptIndex,
        slot: CheckpointJobSlot,
        deferred: bool,
    ) -> anyhow::Result<Self> {
        let tail = receipts.current_tail_binding()?;
        let authority = pe_event_log::RetentionAuthority::load(&tail.path)?;
        let mut reducers = Reducers::new(true);
        if let Some(authority) = &authority {
            for pin in &authority.pins {
                let (envelope, _) = authority.verify_pin(&tail.path, pin)?;
                if pin.reducer {
                    reducers.observe(&envelope);
                }
            }
        }
        let mut digest = blake3::Hasher::new();
        let actual = Scanner::walk_bounded(
            &tail.path,
            tail.physical_tail,
            &activation,
            None,
            &mut digest,
            &mut |_, envelope| reducers.observe(envelope),
        )?;
        require_binding(&tail, actual)?;
        reducers.take_error()?;
        let prefix = if deferred {
            FrozenPrefix::Deferred {
                tail: tail.clone(),
                prefix_blake3: digest.finalize().to_hex().to_string(),
            }
        } else {
            FrozenPrefix::Verified(Box::new(digest))
        };
        Ok(Self::new(
            FrozenCheckpoint {
                authority_generation: super::read_authority(&tail.path)?.generation(),
                capture_unix_ms: 0,
                financial_era: true,
                activation,
                tail,
                prefix,
                reducers,
            },
            receipts,
            slot,
        ))
    }

    /// Lane D supplies the receipt-index boundary installer and the database input owner.
    /// The service boot must already have installed the retention fence before listening.
    pub fn with_retention(mut self, context: RetentionContext) -> anyhow::Result<Self> {
        let path = &self.receipts.current_tail_binding()?.path;
        let authority = pe_event_log::RetentionAuthority::load(path)?;
        self.retention_epoch = authority.as_ref().map_or(0, |authority| authority.epoch);
        self.finishing_retention = authority.is_some();
        self.paper_state = Some(context.paper_state.clone());
        self.retention = Some(context);
        Ok(self)
    }

    #[cfg(feature = "scenario")]
    pub fn retention_window_for_scenario(&self) -> Option<&HashSet<pe_core_types::WalletAddress>> {
        self.retention_walk_complete.then_some(&self.window_wallets)
    }

    #[cfg(feature = "scenario")]
    pub async fn retention_for_scenario(&mut self) -> Result<(), TaskFailure> {
        self.run_retention().await
    }

    #[cfg(feature = "scenario")]
    pub fn set_scenario_hooks(&mut self, hooks: Arc<CheckpointOwnerHooks>) {
        self.hooks = hooks;
    }

    #[cfg(feature = "scenario")]
    pub async fn initialize_for_scenario(&mut self) -> Result<(), TaskFailure> {
        self.capture(None).await?;
        if self.finishing_retention {
            self.pending = None;
            self.run_retention().await
        } else {
            self.attempt_publication().await
        }
    }

    #[cfg(feature = "scenario")]
    pub fn record_synced_append_for_scenario(
        &self,
        receipt: pe_event_log::AppendReceipt,
        envelope: &pe_event_log::EnvelopeIn,
    ) -> Result<(), crate::risk_inputs::RiskInputsUnavailable> {
        self.receipts.record_synced_append(receipt, envelope)
    }

    #[cfg(feature = "scenario")]
    pub async fn publish_hourly_for_scenario(&mut self) -> Result<(), TaskFailure> {
        let tail = self
            .receipts
            .current_tail_binding()
            .map_err(TaskFailure::typed)?;
        let capture = self.unix_ms().map_err(TaskFailure::typed)?;
        self.capture(Some((tail, capture))).await?;
        self.attempt_publication().await
    }

    fn unix_ms(&self) -> std::io::Result<u64> {
        #[cfg(feature = "scenario")]
        if let Some(clock) = &self.hooks.clock {
            return clock();
        }
        super::unix_ms()
    }

    pub async fn run(mut self, shutdown: ShutdownReceiver) -> TaskResult {
        // Keep an independent cancellation handle while the inner future borrows the owner.
        let slot = self.slot.clone();
        tokio::select! {
            biased;
            () = shutdown.wait_for(ShutdownPhase::StopProducers) => {
                slot.cancel();
                Ok(TaskExit::CleanShutdown)
            }
            result = self.run_inner() => result,
        }
    }

    async fn run_inner(&mut self) -> TaskResult {
        let publish_interval = Duration::from_secs(CHECKPOINT_PUBLISH_SECS);
        #[cfg(feature = "scenario")]
        let publish_interval = self.hooks.publish_interval.unwrap_or(publish_interval);
        // Schedule from owner start, including a slow initial verification/publication.
        let mut hourly = tokio::time::interval_at(
            tokio::time::Instant::now() + publish_interval,
            publish_interval,
        );
        hourly.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.capture(None).await?;
        if self.finishing_retention {
            self.pending = None;
            self.run_retention().await?;
        } else {
            self.attempt_publication().await?;
        }
        let mut retry_at = tokio::time::Instant::now() + Duration::from_secs(CHECKPOINT_RETRY_SECS);
        loop {
            tokio::select! {
                biased;
                _ = hourly.tick() => {
                    if !self.finishing_retention {
                        let tail = self.receipts.current_tail_binding().map_err(TaskFailure::typed)?;
                        let capture = self.unix_ms().map_err(TaskFailure::typed)?;
                        self.capture(Some((tail, capture))).await?;
                        self.attempt_publication().await?;
                    }
                    self.run_retention().await?;
                }
                _ = tokio::time::sleep_until(retry_at), if self.pending.is_some() || self.finishing_retention => {
                    if self.finishing_retention { self.run_retention().await?; }
                    else { self.attempt_publication().await?; }
                },
            }
            retry_at = tokio::time::Instant::now() + Duration::from_secs(CHECKPOINT_RETRY_SECS);
        }
    }

    async fn capture(&mut self, bound: Option<(LogTailBinding, u64)>) -> Result<(), TaskFailure> {
        let frozen = self
            .frozen
            .take()
            .ok_or_else(|| TaskFailure::typed(OwnerError::Cancelled))?;
        let receipts = self.receipts.clone();
        let quarantine_failed = self.slot.quarantine_failed.clone();
        #[cfg(feature = "scenario")]
        let hooks = self.hooks.clone();
        let paper_state = self.paper_state.clone();
        let epoch = self.retention_epoch;
        let result = self.slot.execute(move |cancel| {
            let mut frozen = frozen;
            if matches!(frozen.prefix, FrozenPrefix::Deferred { .. }) {
                let started = Instant::now();
                let verified = verify_deferred(&frozen, &cancel,
                    #[cfg(feature = "scenario")]
                    &hooks,
                );
                let digest = match verified {
                    Ok(digest) => digest,
                    Err(error) => {
                        if cancel.load(Ordering::Acquire) || matches!(error, OwnerError::Scan(LogError::Cancelled)) {
                            return Err(TaskFailure::typed(OwnerError::Cancelled));
                        }
                        error!(%error, "source checkpoint prefix invalid; invalidating");
                        #[cfg(feature = "scenario")]
                        if let Some(hook) = &hooks.before_invalidation {
                            hook();
                        }
                        #[cfg(feature = "scenario")]
                        let invalidation = super::invalidate_with_hooks(&frozen.tail.path, &hooks.invalidation);
                        #[cfg(not(feature = "scenario"))]
                        let invalidation = super::invalidate(&frozen.tail.path);
                        if let Err(invalidation) = invalidation {
                            let failed = matches!(invalidation, super::InvalidationError::QuarantineFailed(_));
                            if failed {
                                quarantine_failed.store(true, Ordering::Release);
                            }
                            return Err(TaskFailure {
                                kind: if failed {
                                    TaskFailureKind::CheckpointInvalidationFailed
                                } else { TaskFailureKind::TypedError },
                                message: format!("{error}; {invalidation}"),
                            });
                        }
                        return Err(TaskFailure::typed(error));
                    }
                };
                if let FrozenPrefix::Deferred { tail, prefix_blake3 } = &frozen.prefix {
                    info!(checkpoint_used = true, prefix_verification = "deferred",
                        checkpoint_offset = tail.physical_tail,
                        checkpoint_sequence = tail.last_sequence.map(|seq| seq.0),
                        checkpoint_hash = %tail.last_hash.to_hex(), prefix_blake3,
                        prefix_bytes = tail.physical_tail,
                        suffix_bytes = frozen.tail.physical_tail.saturating_sub(tail.physical_tail),
                        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "source checkpoint prefix verified");
                }
                frozen.prefix = FrozenPrefix::Verified(Box::new(digest));
            }
            if let Some((tail, capture)) = bound {
                extend(&mut frozen, tail, capture, &cancel,
                    #[cfg(feature = "scenario")]
                    &hooks,
                ).map_err(TaskFailure::typed)?;
            }
            if let Some(paper_state) = &paper_state {
                let mut activity = frozen.reducers.activity.clone();
                activity.prune(paper_state, &receipts).map_err(TaskFailure::typed)?;
                if let Err(error) = paper_state.sync_checkpoint_dispositions() {
                    warn!(%error, "source checkpoint disposition barrier failed; publication skipped");
                    return Ok(JobOutput::Candidate(Box::new(frozen), None));
                }
                frozen.reducers.activity = activity;
            }
            let candidate = candidate(&frozen, &receipts, epoch).map_err(TaskFailure::typed)?;
            if let Some(candidate) = &candidate {
                let (activity_triggers, activity_candidates, activity_commitments, routed_frame_receipts) =
                    frozen.reducers.activity.checkpoint_counts();
                let daily_boundary_entries = frozen.reducers.daily_boundary.as_ref().map_or(0, |entries| entries.len());
                info!(manifest_bytes = candidate.bytes.len(), activity_triggers, activity_candidates,
                    activity_commitments, daily_boundary_entries, routed_frame_receipts,
                    "source checkpoint manifest serialized");
            }
            Ok(JobOutput::Candidate(Box::new(frozen), candidate.map(Arc::new)))
        }).await?;
        if let JobOutput::Candidate(frozen, candidate) = result {
            self.frozen = Some(*frozen);
            self.pending = candidate;
            self.attempt = 0;
        }
        Ok(())
    }

    async fn attempt_publication(&mut self) -> Result<(), TaskFailure> {
        let Some(candidate) = self.pending.clone() else {
            return Ok(());
        };
        self.attempt = self.attempt.saturating_add(1);
        let attempt = self.attempt;
        #[cfg(feature = "scenario")]
        let hooks = self.hooks.clone();
        let capture = candidate.capture_unix_ms;
        let now = self.unix_ms().map_err(TaskFailure::typed)?;
        info!(
            attempt,
            capture_unix_ms = capture,
            last_published_capture_unix_ms = self.last_published_capture,
            last_published_age_ms = self
                .last_published_capture
                .map(|time| now.saturating_sub(time)),
            "source checkpoint publication attempt"
        );
        let result = self
            .slot
            .execute(move |cancel| {
                // The publication starts here. Once started its atomic write finishes even on shutdown.
                if cancel.load(Ordering::Acquire) {
                    return Err(TaskFailure::typed(OwnerError::Cancelled));
                }
                #[cfg(feature = "scenario")]
                let result = {
                    struct HookWriter(Arc<CheckpointOwnerHooks>);
                    impl super::Finalization for HookWriter {
                        fn checkpoint_write(
                            &mut self,
                            path: &std::path::Path,
                            bytes: &[u8],
                        ) -> std::io::Result<()> {
                            if let Some(hook) = &self.0.checkpoint_write {
                                hook()?;
                            }
                            crate::qualification::write_report(path, bytes)
                        }
                    }
                    super::publish_with_finalization(
                        &candidate,
                        attempt,
                        &mut HookWriter(hooks.clone()),
                        || {
                            hooks
                                .clock
                                .as_ref()
                                .map_or_else(super::unix_ms, |clock| clock())
                        },
                    )
                };
                #[cfg(not(feature = "scenario"))]
                let result = super::publish(&candidate, attempt);
                #[cfg(feature = "scenario")]
                if let Some(hook) = &hooks.after_publication {
                    hook(&candidate.bytes, candidate.capture_unix_ms, result.is_ok());
                }
                Ok(JobOutput::Publication(result))
            })
            .await?;
        if let JobOutput::Publication(result) = result {
            match result {
                Ok(PublishOutcome::Published(receipt)) => {
                    self.last_published_capture = Some(receipt.capture_unix_ms);
                    self.pending = None;
                    if let Some(frozen) = &self.frozen {
                        self.published_observation_wallets =
                            retention::observation_wallets(frozen).map_err(TaskFailure::typed)?;
                    }
                }
                Ok(PublishOutcome::GenerationChanged { candidate, current }) => {
                    return Err(TaskFailure::typed(OwnerError::GenerationChanged {
                        candidate,
                        current,
                    }));
                }
                Ok(PublishOutcome::RetentionChanged { current, .. }) => {
                    self.pending = None;
                    self.retention_epoch = current;
                    self.retention_walk_complete = false;
                    self.retention_tail = None;
                    self.finishing_retention = current > 0;
                }
                Ok(PublishOutcome::Refused(_)) => self.pending = None,
                Err(PublishError::Io(error)) => error!(%error, capture_unix_ms = capture,
                    last_published_capture_unix_ms = self.last_published_capture,
                    last_published_age_ms = self.last_published_capture.map(|time| now.saturating_sub(time)),
                    "source checkpoint publication retry"),
            }
        }
        #[cfg(feature = "scenario")]
        if let Some(hook) = &self.hooks.after_publication_applied {
            hook(self.last_published_capture);
        }
        Ok(())
    }
    async fn run_retention(&mut self) -> Result<(), TaskFailure> {
        let Some(context) = self.retention.clone() else {
            return Ok(());
        };
        let started = Instant::now();
        let mut punched = 0;
        let mut skip = None;
        let mut database_boundary = None;
        if !self.finishing_retention {
            self.retention_walk_complete = false;
            self.window_wallets.clear();
        }
        let mut result = self
            .retention_inner(&context, &mut punched, &mut skip, &mut database_boundary)
            .await;
        if !self.finishing_retention
            && let Some(boundary_current) = database_boundary
            && let Err(error) =
                self.database_retention(boundary_current, self.retention_walk_complete)
        {
            result = Err(error);
        }
        if result.is_err() && skip.is_none() {
            skip = Some("retention_failed");
        }
        let authority = self.receipts.current_tail_binding().ok().and_then(|tail| {
            pe_event_log::RetentionAuthority::load(&tail.path)
                .ok()
                .flatten()
        });
        info!(
            epoch = authority.as_ref().map_or(0, |authority| authority.epoch),
            boundary = authority
                .as_ref()
                .map_or(0, |authority| authority.boundary.sequence.0),
            pins = authority
                .as_ref()
                .map_or(0, |authority| authority.pins.len()),
            bytes_punched = punched,
            feed_frames = authority
                .as_ref()
                .and_then(|authority| authority
                    .feed
                    .last()
                    .filter(|entry| entry.epoch == authority.epoch))
                .map_or(0, |entry| entry.frame_count),
            anchors_blanked = 0,
            wallets_swapped_out = 0,
            wallets_waiting = "database_retention_not_integrated",
            database_lock_ms = 0,
            skip_reason = skip,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "source retention"
        );
        // Storage failures before a commit defer. A committed authority remains unfinished and is
        // retried before the next daily gate, retaining its immutable archive and exact candidate.
        if let Err(error) = result {
            warn!(%error, finishing = self.finishing_retention, "source retention deferred");
        }
        Ok(())
    }

    async fn retention_inner(
        &mut self,
        context: &RetentionContext,
        punched: &mut u64,
        skip: &mut Option<&'static str>,
        database_boundary: &mut Option<bool>,
    ) -> anyhow::Result<()> {
        let receipts = self.receipts.clone();
        if !self.finishing_retention {
            if self.pending.is_some() {
                *skip = Some("checkpoint_publication_pending");
                return Ok(());
            }
            let frozen = self.frozen.take().ok_or(OwnerError::Cancelled)?;
            let job_context = context.clone();
            let now = i64::try_from(self.unix_ms()? / 1000)?;
            let output = self
                .slot
                .execute(move |cancel| {
                    let prepared =
                        retention::prepare(&frozen, &job_context, &receipts, now, &cancel);
                    // Restore the owner's frozen state even when preparation fails.
                    let prepared = match prepared {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            warn!(%error, "source retention preparation deferred");
                            PreparedRetention::Deferred("preparation_failed")
                        }
                    };
                    Ok(JobOutput::RetentionPrepared(Box::new(frozen), prepared))
                })
                .await
                .map_err(|error| anyhow::anyhow!("{}", error.message))?;
            let JobOutput::RetentionPrepared(frozen, prepared) = output else {
                return Err(OwnerError::Cancelled.into());
            };
            self.frozen = Some(*frozen);
            match prepared {
                PreparedRetention::Deferred(reason) => {
                    *skip = Some(reason);
                    if matches!(reason, "preparation_failed" | "disposition_barrier") {
                        *database_boundary = Some(false);
                    }
                    return Ok(());
                }
                PreparedRetention::Nothing {
                    tail,
                    boundary_current,
                } => {
                    *database_boundary = Some(boundary_current);
                    self.build_retention_window(context, tail).await?;
                    return Ok(());
                }
                PreparedRetention::Advance(request) => {
                    *database_boundary = Some(false);
                    let output = self
                        .slot
                        .execute(move |_| {
                            retention::send_commit(*request)
                                .map(JobOutput::RetentionCommitted)
                                .map_err(TaskFailure::typed)
                        })
                        .await
                        .map_err(|error| anyhow::anyhow!("{}", error.message))?;
                    match output {
                        JobOutput::RetentionCommitted(RetentionCommitOutcome::Deferred(reason)) => {
                            *skip = Some(reason);
                            *database_boundary = None;
                            return Ok(());
                        }
                        JobOutput::RetentionCommitted(RetentionCommitOutcome::Committed {
                            authority,
                            directory_synced,
                        }) => {
                            self.finishing_retention = true;
                            self.retention_walk_complete = false;
                            self.retention_tail = Some(
                                authority
                                    .retained_tail
                                    .resolve(&self.receipts.current_tail_binding()?.path)?,
                            );
                            self.retention_epoch = authority.epoch;
                            *database_boundary = Some(true);
                            self.pending = None;
                            // A visible authority is committed even if its directory is unsynced.
                            if !directory_synced {
                                *skip = Some("authority_directory_sync");
                            }
                            #[cfg(feature = "scenario")]
                            if let Some(hook) = &context.hooks.after_commit {
                                hook()?;
                            }
                        }
                        _ => return Err(OwnerError::Cancelled.into()),
                    }
                }
            }
        }
        *database_boundary = Some(true);
        self.finish_retention(context, punched, skip).await
    }

    async fn build_retention_window(
        &mut self,
        context: &RetentionContext,
        tail: LogTailBinding,
    ) -> anyhow::Result<()> {
        self.retention_walk_complete = false;
        let frozen = self.frozen.take().ok_or(OwnerError::Cancelled)?;
        let receipts = self.receipts.clone();
        let epoch = self.retention_epoch;
        let capture = self.unix_ms()?;
        let paper_state = context.paper_state.clone();
        #[cfg(feature = "scenario")]
        let hooks = self.hooks.clone();
        let output = self
            .slot
            .execute(move |cancel| {
                let mut frozen = frozen;
                let work = (|| -> anyhow::Result<_> {
                    if tail.physical_tail > frozen.tail.physical_tail {
                        extend(
                            &mut frozen,
                            tail.clone(),
                            capture,
                            &cancel,
                            #[cfg(feature = "scenario")]
                            &hooks,
                        )?;
                    }
                    let authority = pe_event_log::RetentionAuthority::load(&tail.path)?;
                    let wallets =
                        retention::verified_window(&mut frozen, authority.as_ref(), tail, &cancel)?;
                    let mut activity = frozen.reducers.activity.clone();
                    activity.prune(&paper_state, &receipts)?;
                    paper_state.sync_checkpoint_dispositions()?;
                    frozen.reducers.activity = activity;
                    frozen.capture_unix_ms = capture;
                    let candidate = candidate(&frozen, &receipts, epoch)?.map(Arc::new);
                    Ok((wallets, candidate))
                })();
                match work {
                    Ok((wallets, candidate)) => Ok(JobOutput::RetentionWindow(
                        Box::new(frozen),
                        wallets,
                        candidate,
                    )),
                    Err(error) => {
                        warn!(%error, "source retention bounded walk deferred");
                        Ok(JobOutput::RetentionPrepared(
                            Box::new(frozen),
                            PreparedRetention::Deferred("window_walk_failed"),
                        ))
                    }
                }
            })
            .await
            .map_err(|error| anyhow::anyhow!("{}", error.message))?;
        match output {
            JobOutput::RetentionWindow(frozen, wallets, candidate) => {
                self.frozen = Some(*frozen);
                self.window_wallets = wallets;
                self.retention_walk_complete = true;
                self.pending = candidate;
                self.attempt = 0;
                Ok(())
            }
            JobOutput::RetentionPrepared(frozen, _) => {
                self.frozen = Some(*frozen);
                anyhow::bail!("retention bounded walk or disposition barrier failed")
            }
            _ => Err(OwnerError::Cancelled.into()),
        }
    }

    async fn finish_retention(
        &mut self,
        context: &RetentionContext,
        punched: &mut u64,
        skip: &mut Option<&'static str>,
    ) -> anyhow::Result<()> {
        let receipts = self.receipts.clone();
        let tail = receipts.current_tail_binding()?;
        let job_context = context.clone();
        let path = tail.path.clone();
        let output = self
            .slot
            .execute(move |_| {
                if let Some(reason) =
                    retention::pause_reason(&job_context).map_err(TaskFailure::typed)?
                {
                    return Ok(JobOutput::Paused(reason));
                }
                retention::sync_authority(&job_context, &path).map_err(TaskFailure::typed)?;
                Ok(JobOutput::Punched(0))
            })
            .await
            .map_err(|error| anyhow::anyhow!("{}", error.message))?;
        if let JobOutput::Paused(reason) = output {
            *skip = Some(reason);
            return Ok(());
        }
        if !self.retention_walk_complete {
            self.build_retention_window(
                context,
                self.retention_tail.clone().unwrap_or_else(|| tail.clone()),
            )
            .await?;
        }
        let capture = self
            .pending
            .as_ref()
            .map(|candidate| candidate.capture_unix_ms);
        let verified_epoch = self.retention_epoch;
        self.attempt_publication()
            .await
            .map_err(|error| anyhow::anyhow!("{}", error.message))?;
        if self.retention_epoch != verified_epoch {
            *skip = Some("epoch_changed");
            return Ok(());
        }
        if self.pending.is_some()
            || capture.is_some_and(|capture| self.last_published_capture != Some(capture))
        {
            *skip = Some("checkpoint_publication_pending");
            return Ok(());
        }
        #[cfg(feature = "scenario")]
        if let Some(hook) = &context.hooks.after_publication {
            hook()?;
        }
        let job_context = context.clone();
        let epoch = self.retention_epoch;
        let activation = self
            .frozen
            .as_ref()
            .ok_or(OwnerError::Cancelled)?
            .activation
            .clone();
        let financial_era = self
            .frozen
            .as_ref()
            .ok_or(OwnerError::Cancelled)?
            .financial_era;
        let output = self.slot.execute(move |cancel| {
            if let Some(reason) = retention::pause_reason(&job_context).map_err(TaskFailure::typed)? {
                return Ok(JobOutput::Paused(reason));
            }
            let _lock = super::CheckpointLock::acquire(&tail.path).map_err(TaskFailure::typed)?;
            let authority = pe_event_log::RetentionAuthority::load(&tail.path).map_err(TaskFailure::typed)?
                .ok_or_else(|| TaskFailure::typed(OwnerError::AuthorityUnavailable))?;
            if authority.epoch != epoch { return Ok(JobOutput::Paused("epoch_changed")); }
            // A refused publication is never a license to punch: require a compatible installed
            // checkpoint through the committed tail and synchronize its directory again.
            let (check, _) = super::check_artifact(&tail.path, &activation, financial_era).map_err(TaskFailure::typed)?;
            if !matches!(check, super::ArtifactCheck::Valid(ref header) if header.retention_epoch == epoch
                && header.tail.physical_tail >= authority.retained_tail.physical_tail) {
                return Ok(JobOutput::Paused("checkpoint_not_durable"));
            }
            super::sync_directory(&super::checkpoint_path(&tail.path)).map_err(TaskFailure::typed)?;
            let bytes = retention::punch(&tail.path, &authority, &cancel,
                #[cfg(feature = "scenario")] &job_context.hooks,
            ).map_err(TaskFailure::typed)?;
            Ok(JobOutput::Punched(bytes))
        }).await.map_err(|error| anyhow::anyhow!("{}", error.message))?;
        match output {
            JobOutput::Punched(bytes) => {
                *punched = bytes;
                self.finishing_retention = false;
                self.retention_tail = None;
            }
            JobOutput::Paused(reason) => *skip = Some(reason),
            _ => return Err(OwnerError::Cancelled.into()),
        }
        Ok(())
    }

    fn database_retention(
        &self,
        boundary_current: bool,
        walk_completed: bool,
    ) -> anyhow::Result<()> {
        let path = self.receipts.current_tail_binding()?.path;
        let authority = pe_event_log::RetentionAuthority::load(&path)?;
        let reducer_pin_wallets = authority.as_ref().map_or_else(HashSet::new, |authority| {
            authority
                .pins
                .iter()
                .filter(|pin| pin.reducer)
                .filter_map(|pin| pin.wallet)
                .collect()
        });
        // LANE D INTEGRATION: run_database_retention(boundary_current, walk_completed,
        // &self.window_wallets, &reducer_pin_wallets, &self.published_observation_wallets,
        // &self.slot.cancelled). Steps 8–9 belong to the database retention owner.
        let _ = (boundary_current, walk_completed, reducer_pin_wallets);
        Ok(())
    }
}

fn require_binding(expected: &LogTailBinding, actual: LogTailBinding) -> Result<(), OwnerError> {
    if actual != *expected {
        return Err(OwnerError::BindingMismatch {
            expected: Box::new(expected.clone()),
            actual: Box::new(actual),
        });
    }
    Ok(())
}

fn verify_deferred(
    frozen: &FrozenCheckpoint,
    cancel: &AtomicBool,
    #[cfg(feature = "scenario")] hooks: &CheckpointOwnerHooks,
) -> Result<blake3::Hasher, OwnerError> {
    let FrozenPrefix::Deferred {
        tail,
        prefix_blake3,
    } = &frozen.prefix
    else {
        return Err(OwnerError::Cancelled);
    };
    #[cfg(feature = "scenario")]
    if let Some(hook) = &hooks.before_hash {
        hook()?;
    }
    if let Some(authority) = pe_event_log::RetentionAuthority::load(&tail.path)
        .map_err(|error| OwnerError::Io(std::io::Error::other(error)))?
    {
        for pin in &authority.pins {
            if cancel.load(Ordering::Acquire) {
                return Err(OwnerError::Cancelled);
            }
            authority.verify_pin(&tail.path, pin)?;
        }
    }
    let mut digest =
        Scanner::hash_prefix_cancellable(&tail.path, tail.physical_tail, Some(cancel))?;
    let actual = digest.finalize().to_hex().to_string();
    if actual != *prefix_blake3 {
        return Err(OwnerError::PrefixMismatch {
            offset: tail.physical_tail,
            expected: prefix_blake3.clone(),
            actual,
        });
    }
    let actual = Scanner::walk_bounded_cancellable(
        &tail.path,
        frozen.tail.physical_tail,
        &frozen.activation,
        Some(tail),
        &mut digest,
        &mut |_, _| {},
        Some(cancel),
    )?;
    require_binding(&frozen.tail, actual)?;
    Ok(digest)
}

fn extend(
    frozen: &mut FrozenCheckpoint,
    tail: LogTailBinding,
    capture: u64,
    cancel: &AtomicBool,
    #[cfg(feature = "scenario")] hooks: &CheckpointOwnerHooks,
) -> Result<(), OwnerError> {
    let FrozenPrefix::Verified(digest) = &frozen.prefix else {
        return Err(OwnerError::Cancelled);
    };
    let mut digest = digest.clone();
    let mut reducers = Reducers::new(frozen.financial_era);
    reducers.activity = frozen.reducers.activity.clone();
    reducers.daily_boundary = frozen.reducers.daily_boundary.clone();
    #[cfg(feature = "scenario")]
    if let Some(hook) = &hooks.before_walk {
        hook()?;
    }
    let actual = Scanner::walk_bounded_cancellable(
        &tail.path,
        tail.physical_tail,
        &frozen.activation,
        Some(&frozen.tail),
        &mut digest,
        &mut |_, envelope| reducers.observe(envelope),
        Some(cancel),
    )?;
    require_binding(&tail, actual)?;
    reducers.take_error()?;
    frozen.tail = tail;
    frozen.capture_unix_ms = capture;
    frozen.reducers = reducers;
    frozen.prefix = FrozenPrefix::Verified(digest);
    Ok(())
}

fn candidate(
    frozen: &FrozenCheckpoint,
    receipts: &SourceReceiptIndex,
    retention_epoch: u64,
) -> Result<Option<SerializedCandidate>, OwnerError> {
    let Some(generation) = frozen.authority_generation else {
        error!(reason = %OwnerError::AuthorityUnavailable, capture_unix_ms = frozen.capture_unix_ms, "source checkpoint candidate refused");
        return Ok(None);
    };
    let FrozenPrefix::Verified(digest) = &frozen.prefix else {
        return Err(OwnerError::Cancelled);
    };
    let count = frozen
        .tail
        .last_sequence
        .map_or(Some(0), |seq| {
            usize::try_from(seq.0).ok().and_then(|n| n.checked_add(1))
        })
        .ok_or_else(|| anyhow::anyhow!("checkpoint receipt count overflow"))?;
    let start = super::capture_start(&frozen.activation, frozen.financial_era, count);
    Ok(Some(super::serialize_for_epoch(
        CheckpointData {
            format_version: 2,
            generation,
            receipt_count: Some(count),
            scanner_version: 1,
            reducer_version: ACTIVITY_REDUCER_VERSION,
            financial_era: frozen.financial_era,
            activation: frozen.activation.clone(),
            tail: frozen.tail.clone(),
            prefix_blake3: digest.finalize().to_hex().to_string(),
            receipts: receipts
                .checkpoint_suffix(start, count, &frozen.tail)
                .map_err(anyhow::Error::from)?,
            activity: frozen.reducers.activity.clone(),
            daily_boundary: frozen.reducers.daily_boundary.clone(),
        },
        generation,
        frozen.capture_unix_ms,
        retention_epoch,
    )?))
}

#[cfg(feature = "scenario")]
pub fn cli_owner_hooks() -> std::io::Result<Arc<CheckpointOwnerHooks>> {
    let mut hooks = CheckpointOwnerHooks::default();
    if let Ok(value) = std::env::var("PE_SCENARIO_CHECKPOINT_PUBLISH_INTERVAL_MS") {
        let millis = value.parse::<u64>().map_err(std::io::Error::other)?;
        if millis == 0 {
            return Err(std::io::Error::other(
                "scenario publication interval must be positive",
            ));
        }
        hooks.publish_interval = Some(Duration::from_millis(millis));
    }
    if let Some(path) = std::env::var_os("PE_SCENARIO_CHECKPOINT_PAUSE_BEFORE_HASH") {
        let path = std::path::PathBuf::from(path);
        hooks.before_hash = Some(Arc::new(move || {
            std::fs::write(path.with_extension("ready"), b"ready")?;
            while !path.with_extension("resume").exists() {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(())
        }));
    }
    hooks.invalidation.fail_quarantine_rename.store(
        std::env::var_os("PE_SCENARIO_CHECKPOINT_FAIL_QUARANTINE_RENAME").is_some(),
        Ordering::Relaxed,
    );
    hooks.invalidation.fail_quarantine_sync.store(
        std::env::var_os("PE_SCENARIO_CHECKPOINT_FAIL_QUARANTINE_SYNC").is_some(),
        Ordering::Relaxed,
    );
    Ok(Arc::new(hooks))
}

#[cfg(all(test, feature = "scenario"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::{ContentType, EnvelopeIn, Writer};
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::mpsc;

    struct Fixture {
        _dir: tempfile::TempDir,
        writer: Writer,
        activation: LogTailBinding,
        receipts: SourceReceiptIndex,
    }
    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source.log");
            let mut writer = Writer::open(&path).unwrap();
            let activation = writer.verified_tail().unwrap();
            let receipts = SourceReceiptIndex::replay(&path).unwrap();
            Self {
                _dir: dir,
                writer,
                activation,
                receipts,
            }
        }
        fn append(&mut self) -> LogTailBinding {
            let clock = time::OffsetDateTime::UNIX_EPOCH;
            let input = || EnvelopeIn {
                source_id: SourceId("checkpoint-runtime-fixture".into()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(clock),
                received_at: ReceivedAt(clock),
                content_type: ContentType::Raw,
                payload: b"bounded frame".to_vec(),
            };
            let receipt = self.writer.append_synced(input()).unwrap();
            self.receipts
                .record_synced_append(receipt, &input())
                .unwrap();
            self.writer.verified_tail().unwrap()
        }
        fn owner(
            &self,
            slot: CheckpointJobSlot,
            deferred: bool,
            generation: u64,
        ) -> SourceCheckpointOwner {
            let tail = self.receipts.current_tail_binding().unwrap();
            let digest = Scanner::hash_prefix(&tail.path, tail.physical_tail).unwrap();
            SourceCheckpointOwner::new(
                FrozenCheckpoint {
                    authority_generation: Some(generation),
                    capture_unix_ms: 123,
                    financial_era: false,
                    activation: self.activation.clone(),
                    tail: tail.clone(),
                    prefix: if deferred {
                        FrozenPrefix::Deferred {
                            tail,
                            prefix_blake3: digest.finalize().to_hex().to_string(),
                        }
                    } else {
                        FrozenPrefix::Verified(Box::new(digest))
                    },
                    reducers: Reducers::new(false),
                },
                self.receipts.clone(),
                slot,
            )
        }
    }

    #[tokio::test(start_paused = true)]
    async fn changed_boot_suffix_fails_binding() {
        let mut fixture = Fixture::new();
        let prefix = fixture.append();
        let expected_hash = Scanner::hash_prefix(&prefix.path, prefix.physical_tail)
            .unwrap()
            .finalize()
            .to_hex()
            .to_string();
        fixture.append();
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), false, 0);
        let frozen = owner.frozen.as_mut().unwrap();
        frozen.prefix = FrozenPrefix::Deferred {
            tail: prefix,
            prefix_blake3: expected_hash,
        };
        frozen.tail.last_hash = blake3::hash(b"changed boot binding");
        let error = owner.initialize_for_scenario().await.unwrap_err();
        assert!(error.message.contains("binding changed"), "{error:?}");
        assert_eq!(
            super::super::read_authority(&fixture.activation.path)
                .unwrap()
                .generation(),
            Some(1)
        );
        slot.join().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_walk_failure_is_critical() {
        let mut fixture = Fixture::new();
        fixture.append();
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), false, 0);
        owner.initialize_for_scenario().await.unwrap();
        let published =
            std::fs::read(super::super::checkpoint_path(&fixture.activation.path)).unwrap();
        let tail = fixture.append();
        let mut bytes = std::fs::read(&tail.path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&tail.path, bytes).unwrap();
        let error = owner.capture(Some((tail, 456))).await.unwrap_err();
        assert!(error.message.contains("CRC"), "{error:?}");
        assert_eq!(
            super::super::read_authority(&fixture.activation.path)
                .unwrap()
                .generation(),
            Some(0)
        );
        assert_eq!(
            std::fs::read(super::super::checkpoint_path(&fixture.activation.path)).unwrap(),
            published
        );
        slot.join().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn generation_change_under_running_owner_restarts() {
        let mut fixture = Fixture::new();
        fixture.append();
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), false, 0);
        owner.initialize_for_scenario().await.unwrap();
        let generation = super::super::invalidate(&fixture.activation.path).unwrap();
        let fresh = fixture.owner(CheckpointJobSlot::default(), false, generation);
        let candidate = candidate(fresh.frozen.as_ref().unwrap(), &fixture.receipts, 0)
            .unwrap()
            .unwrap();
        super::super::publish(&candidate, 1).unwrap();
        let tail = fixture.append();
        owner.capture(Some((tail.clone(), 456))).await.unwrap();
        let error = owner.attempt_publication().await.unwrap_err();
        assert!(error.message.contains("generation changed"));
        assert_eq!(
            super::super::load_checkpoint(&tail.path, &fixture.activation, false)
                .unwrap()
                .unwrap()
                .data
                .tail,
            candidate.tail
        );
        // Restart establishes fresh authority and can extend and publish again.
        let mut restarted = fixture.owner(CheckpointJobSlot::default(), false, generation);
        restarted.initialize_for_scenario().await.unwrap();
        assert_eq!(
            super::super::load_checkpoint(&tail.path, &fixture.activation, false)
                .unwrap()
                .unwrap()
                .data
                .tail,
            tail
        );
        slot.join().await.unwrap();
    }

    // The scheduler is observed through completed protocol attempts. Keep a runnable waiter so
    // paused Tokio time never auto-advances while a real blocking child is completing.
    async fn receive<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
        loop {
            match rx.try_recv() {
                Ok(value) => return value,
                Err(mpsc::error::TryRecvError::Empty) => tokio::task::yield_now().await,
                Err(error) => panic!("checkpoint observation channel: {error}"),
            }
        }
    }
    #[derive(Clone)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for CapturedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }
    fn retry_logs(bytes: &Arc<std::sync::Mutex<Vec<u8>>>) -> Vec<serde_json::Value> {
        String::from_utf8(bytes.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["fields"].clone())
            .filter(|line| line["message"] == "source checkpoint publication retry")
            .collect()
    }
    async fn advance_clock(clock: &std::sync::atomic::AtomicU64, seconds: u64) {
        clock.fetch_add(seconds * 1000, Ordering::AcqRel);
        tokio::time::advance(Duration::from_secs(seconds)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_publication_failure_retries_retained_candidate() {
        let mut fixture = Fixture::new();
        fixture.append();
        let failures = Arc::new(AtomicBool::new(false));
        let flag = failures.clone();
        let clock = Arc::new(std::sync::atomic::AtomicU64::new(123));
        let hook_clock = clock.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let hooks = Arc::new(CheckpointOwnerHooks {
            checkpoint_write: Some(Arc::new(move || {
                if flag.load(Ordering::Acquire) {
                    Err(std::io::Error::other("retryable write"))
                } else {
                    Ok(())
                }
            })),
            clock: Some(Arc::new(move || Ok(hook_clock.load(Ordering::Acquire)))),
            after_publication: Some(Arc::new(move |bytes, capture, success| {
                tx.send((bytes.to_vec(), capture, success)).unwrap();
            })),
            after_publication_applied: Some(Arc::new(move |last| {
                applied_tx.send(last).unwrap();
            })),
            ..Default::default()
        });
        let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .json()
                .with_writer(CapturedLogs(bytes.clone()))
                .finish(),
        );
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), false, 0);
        owner.set_scenario_hooks(hooks);
        let (shutdown, _) = crate::supervisor::ShutdownController::new();
        let task = tokio::spawn(owner.run(shutdown.subscribe()));
        let initial = receive(&mut rx).await;
        assert!(initial.2);
        assert_eq!(receive(&mut applied_rx).await, Some(initial.1));
        let first_tail = fixture.append();
        failures.store(true, Ordering::Release);
        advance_clock(&clock, CHECKPOINT_PUBLISH_SECS).await;
        let failed = receive(&mut rx).await;
        assert!(!failed.2);
        assert_eq!(receive(&mut applied_rx).await, Some(initial.1));
        let decoded: serde_json::Value = serde_json::from_slice(&failed.0[65..]).unwrap();
        assert_eq!(decoded["tail"]["physical_tail"], first_tail.physical_tail);
        let mut retry_times = vec![clock.load(Ordering::Acquire)];
        for _ in 0..2 {
            advance_clock(&clock, CHECKPOINT_RETRY_SECS).await;
            assert_eq!(receive(&mut rx).await, failed);
            assert_eq!(receive(&mut applied_rx).await, Some(initial.1));
            retry_times.push(clock.load(Ordering::Acquire));
        }
        let logs = retry_logs(&bytes);
        assert_eq!(logs.len(), retry_times.len());
        for (log, now) in logs.iter().zip(retry_times) {
            assert_eq!(log["capture_unix_ms"], failed.1);
            assert_eq!(log["last_published_capture_unix_ms"], initial.1);
            assert_eq!(log["last_published_age_ms"], now - initial.1);
        }
        // New frames never alter a retained retry. The next hourly capture replaces it.
        let later = fixture.append();
        advance_clock(&clock, CHECKPOINT_PUBLISH_SECS - 2 * CHECKPOINT_RETRY_SECS).await;
        let replacement = receive(&mut rx).await;
        assert!(!replacement.2);
        assert_eq!(receive(&mut applied_rx).await, Some(initial.1));
        assert_ne!(replacement.0, failed.0);
        let body: serde_json::Value = serde_json::from_slice(&replacement.0[65..]).unwrap();
        assert_eq!(body["tail"]["physical_tail"], later.physical_tail);
        let logs = retry_logs(&bytes);
        assert_eq!(logs.len(), 4);
        assert_eq!(logs[3]["capture_unix_ms"], replacement.1);
        assert_eq!(logs[3]["last_published_capture_unix_ms"], initial.1);
        assert_eq!(
            logs[3]["last_published_age_ms"],
            clock.load(Ordering::Acquire) - initial.1
        );
        failures.store(false, Ordering::Release);
        advance_clock(&clock, CHECKPOINT_RETRY_SECS).await;
        let success = receive(&mut rx).await;
        assert_eq!((&success.0, success.1), (&replacement.0, replacement.1));
        assert!(success.2);
        assert_eq!(receive(&mut applied_rx).await, Some(replacement.1));
        assert_eq!(
            std::fs::read(super::super::checkpoint_path(&later.path)).unwrap(),
            replacement.0
        );
        shutdown.advance(ShutdownPhase::StopProducers);
        assert_eq!(task.await.unwrap().unwrap(), TaskExit::CleanShutdown);
        slot.join().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn initial_publication_failure_beyond_one_hour() {
        for active in [false, true] {
            let mut fixture = Fixture::new();
            fixture.append();
            if active {
                std::fs::write(
                    super::super::record_path(&fixture.activation.path),
                    br#"{"generation":4,"active":true}"#,
                )
                .unwrap();
            }
            let generation = if active { 4 } else { 0 };
            let slot = CheckpointJobSlot::default();
            let mut owner = fixture.owner(slot.clone(), false, generation);
            let fail = Arc::new(AtomicBool::new(true));
            let write_fail = fail.clone();
            let clock = Arc::new(std::sync::atomic::AtomicU64::new(123));
            let hook_clock = clock.clone();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
            owner.set_scenario_hooks(Arc::new(CheckpointOwnerHooks {
                clock: Some(Arc::new(move || Ok(hook_clock.load(Ordering::Acquire)))),
                checkpoint_write: Some(Arc::new(move || {
                    if write_fail.load(Ordering::Acquire) {
                        Err(std::io::Error::other("storage outage"))
                    } else {
                        Ok(())
                    }
                })),
                after_publication: Some(Arc::new(move |bytes, capture, success| {
                    tx.send((bytes.to_vec(), capture, success)).unwrap();
                })),
                after_publication_applied: Some(Arc::new(move |last| {
                    applied_tx.send(last).unwrap();
                })),
                ..Default::default()
            }));
            let (shutdown, _) = crate::supervisor::ShutdownController::new();
            let task = tokio::spawn(owner.run(shutdown.subscribe()));
            let first = receive(&mut rx).await;
            assert_eq!(first.1, 123);
            assert!(!first.2);
            assert_eq!(receive(&mut applied_rx).await, None);
            let tail = fixture.append();
            // Drive the real hourly/retry select loop. Until the hourly boundary every retry
            // uses the initial serialized candidate even though the source tail has extended.
            for _ in 1..(CHECKPOINT_PUBLISH_SECS / CHECKPOINT_RETRY_SECS) {
                advance_clock(&clock, CHECKPOINT_RETRY_SECS).await;
                assert_eq!(receive(&mut rx).await, first);
                assert_eq!(receive(&mut applied_rx).await, None);
            }
            advance_clock(&clock, CHECKPOINT_RETRY_SECS).await;
            let replacement = receive(&mut rx).await;
            assert_eq!(receive(&mut applied_rx).await, None);
            assert!(!replacement.2);
            assert_ne!(replacement.0, first.0);
            assert_eq!(replacement.1, 123 + CHECKPOINT_PUBLISH_SECS * 1000);
            let body: serde_json::Value = serde_json::from_slice(&replacement.0[65..]).unwrap();
            assert_eq!(body["tail"]["physical_tail"], tail.physical_tail);
            advance_clock(&clock, CHECKPOINT_RETRY_SECS).await;
            assert_eq!(receive(&mut rx).await, replacement);
            assert_eq!(receive(&mut applied_rx).await, None);
            if active {
                assert_eq!(
                    super::super::read_authority(&tail.path).unwrap(),
                    super::super::Authority::Readable(super::super::InvalidationRecord {
                        generation,
                        active: true,
                        retention_fence: false
                    })
                );
            }
            fail.store(false, Ordering::Release);
            advance_clock(&clock, CHECKPOINT_RETRY_SECS).await;
            let success = receive(&mut rx).await;
            assert_eq!((&success.0, success.1), (&replacement.0, replacement.1));
            assert!(success.2);
            assert_eq!(receive(&mut applied_rx).await, Some(replacement.1));
            shutdown.advance(ShutdownPhase::StopProducers);
            assert_eq!(task.await.unwrap().unwrap(), TaskExit::CleanShutdown);
            slot.join().await.unwrap();
            assert!(
                super::super::read_authority(&tail.path)
                    .unwrap()
                    .permits_checkpoint()
            );
            let loaded = super::super::load_checkpoint(&tail.path, &fixture.activation, false)
                .unwrap()
                .unwrap();
            let staging = loaded.staging;
            let loaded = loaded.data;
            assert_eq!(loaded.tail, tail);
            let receipts = staging.complete(&loaded.tail).unwrap();
            let mut reducers = Reducers::new(loaded.financial_era);
            reducers.activity = loaded.activity;
            reducers.daily_boundary = loaded.daily_boundary;
            let restarted_slot = CheckpointJobSlot::default();
            let mut restarted = SourceCheckpointOwner::new(
                FrozenCheckpoint {
                    authority_generation: Some(generation),
                    capture_unix_ms: clock.load(Ordering::Acquire),
                    financial_era: loaded.financial_era,
                    activation: loaded.activation,
                    tail: loaded.tail.clone(),
                    prefix: FrozenPrefix::Deferred {
                        tail: loaded.tail,
                        prefix_blake3: loaded.prefix_blake3,
                    },
                    reducers,
                },
                receipts,
                restarted_slot.clone(),
            );
            restarted.initialize_for_scenario().await.unwrap();
            assert_eq!(
                super::super::load_checkpoint(&tail.path, &fixture.activation, false)
                    .unwrap()
                    .unwrap()
                    .data
                    .tail,
                tail
            );
            restarted_slot.join().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quarantine_failure_survives_owner_cancellation() {
        let mut fixture = Fixture::new();
        fixture.append();
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), true, 0);
        let mut bytes = std::fs::read(&fixture.activation.path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&fixture.activation.path, bytes).unwrap();
        let (started, mut starts) = mpsc::unbounded_channel();
        let (release, wait) = std::sync::mpsc::channel();
        let wait = std::sync::Mutex::new(wait);
        let hooks = CheckpointOwnerHooks {
            before_invalidation: Some(Arc::new(move || {
                started.send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            })),
            ..Default::default()
        };
        hooks
            .invalidation
            .fail_quarantine_rename
            .store(true, Ordering::Release);
        owner.set_scenario_hooks(Arc::new(hooks));
        let (shutdown, _) = crate::supervisor::ShutdownController::new();
        let task = tokio::spawn(owner.run(shutdown.subscribe()));
        receive(&mut starts).await;
        shutdown.advance(ShutdownPhase::StopProducers);
        assert_eq!(task.await.unwrap().unwrap(), TaskExit::CleanShutdown);
        assert!(!slot.quarantine_failed());
        release.send(()).unwrap();
        slot.join().await.unwrap();
        assert!(slot.quarantine_failed());
    }

    #[tokio::test(start_paused = true)]
    async fn orphan_temporary_file_does_not_block_publication() {
        let mut fixture = Fixture::new();
        fixture.append();
        std::fs::write(
            super::super::record_path(&fixture.activation.path),
            br#"{"generation":4,"active":true}"#,
        )
        .unwrap();
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), false, 4);
        let fail = Arc::new(AtomicBool::new(true));
        let flag = fail.clone();
        owner.set_scenario_hooks(Arc::new(CheckpointOwnerHooks {
            checkpoint_write: Some(Arc::new(move || {
                if flag.swap(false, Ordering::AcqRel) {
                    Err(std::io::Error::other("first attempt"))
                } else {
                    Ok(())
                }
            })),
            ..Default::default()
        }));
        owner.initialize_for_scenario().await.unwrap();
        let pending = owner.pending.clone().unwrap();
        let mut other = Vec::new();
        for target in [
            super::super::checkpoint_path(&fixture.activation.path),
            super::super::record_path(&fixture.activation.path),
        ] {
            std::fs::write(
                target.with_extension(format!("tmp.{}", std::process::id())),
                b"orphan",
            )
            .unwrap();
            let path = target.with_extension(format!("tmp.{}", u64::from(std::process::id()) + 1));
            std::fs::write(&path, b"another process").unwrap();
            other.push(path);
        }
        tokio::time::advance(Duration::from_secs(CHECKPOINT_RETRY_SECS)).await;
        owner.attempt_publication().await.unwrap();
        assert_eq!(
            std::fs::read(super::super::checkpoint_path(&fixture.activation.path)).unwrap(),
            pending.bytes
        );
        assert_eq!(owner.last_published_capture, Some(pending.capture_unix_ms));
        assert!(
            super::super::read_authority(&fixture.activation.path)
                .unwrap()
                .permits_checkpoint()
        );
        for path in other {
            assert_eq!(std::fs::read(path).unwrap(), b"another process");
        }
        slot.join().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn checkpoint_blocking_jobs_cancel_on_shutdown_and_owner_abort() {
        for stage in ["prefix", "hourly", "publication"] {
            for abort in [false, true] {
                let mut fixture = Fixture::new();
                fixture.append();
                let slot = CheckpointJobSlot::default();
                let mut owner = fixture.owner(slot.clone(), stage == "prefix", 0);
                let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
                let (started, mut starts) = mpsc::unbounded_channel();
                let (release, wait) = std::sync::mpsc::channel();
                let wait = std::sync::Mutex::new(wait);
                let hook: Arc<dyn Fn() -> std::io::Result<()> + Send + Sync> =
                    Arc::new(move || {
                        started.send(()).map_err(std::io::Error::other)?;
                        wait.lock().unwrap().recv().map_err(std::io::Error::other)
                    });
                let writes = Arc::new(AtomicUsize::new(0));
                let seen_writes = writes.clone();
                let mut hooks = CheckpointOwnerHooks {
                    after_publication_applied: Some(Arc::new(move |last| {
                        applied_tx.send(last).unwrap();
                    })),
                    after_publication: Some(Arc::new(move |_, _, _| {
                        seen_writes.fetch_add(1, Ordering::Release);
                    })),
                    ..Default::default()
                };
                match stage {
                    "prefix" => hooks.before_hash = Some(hook),
                    "hourly" => hooks.before_walk = Some(hook),
                    "publication" => hooks.checkpoint_write = Some(hook),
                    _ => unreachable!(),
                }
                owner.set_scenario_hooks(Arc::new(hooks));
                let (shutdown, _) = crate::supervisor::ShutdownController::new();
                let task = tokio::spawn(owner.run(shutdown.subscribe()));
                if stage == "hourly" {
                    assert!(receive(&mut applied_rx).await.is_some());
                    fixture.append();
                    tokio::time::advance(Duration::from_secs(CHECKPOINT_PUBLISH_SECS)).await;
                }
                receive(&mut starts).await;
                if abort {
                    task.abort();
                } else {
                    shutdown.advance(ShutdownPhase::StopProducers);
                }
                if abort {
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    assert_eq!(task.await.unwrap().unwrap(), TaskExit::CleanShutdown);
                }
                assert!(slot.cancelled.load(Ordering::Acquire));
                // Child remains independently joinable while blocked, including after owner abort.
                {
                    let joining = tokio::time::timeout(Duration::from_secs(1), slot.join());
                    tokio::pin!(joining);
                    assert!(futures::poll!(&mut joining).is_pending());
                    tokio::time::advance(Duration::from_secs(1)).await;
                    assert!(joining.await.is_err());
                }
                release.send(()).unwrap();
                slot.join().await.unwrap();
                let expected = usize::from(stage != "prefix");
                assert_eq!(
                    writes.load(Ordering::Acquire),
                    expected,
                    "{stage} abort={abort}"
                );
                assert_eq!(
                    super::super::checkpoint_path(&fixture.activation.path).exists(),
                    stage != "prefix"
                );
                assert!(
                    slot.execute(|_| panic!("cancelled slot started a new job"))
                        .await
                        .is_err()
                );
            }
        }
    }
}
