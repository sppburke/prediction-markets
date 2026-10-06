//! Runtime ownership of a verified, incrementally extended checkpoint prefix.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pe_event_log::{LogError, LogTailBinding, Scanner};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info};

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
}

type Job = JoinHandle<Result<JobOutput, TaskFailure>>;

/// The async owner and main share this slot. Aborting the owner never detaches its blocking child.
#[derive(Clone, Default)]
pub struct CheckpointJobSlot {
    job: Arc<Mutex<Option<Job>>>,
    cancelled: Arc<AtomicBool>,
}

impl CheckpointJobSlot {
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
    pub invalidation: super::InvalidationHooks,
}

pub struct SourceCheckpointOwner {
    frozen: Option<FrozenCheckpoint>,
    receipts: SourceReceiptIndex,
    slot: CheckpointJobSlot,
    pending: Option<Arc<SerializedCandidate>>,
    attempt: u32,
    last_published_capture: Option<u64>,
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
            attempt: 0,
            last_published_capture: None,
            #[cfg(feature = "scenario")]
            hooks: Arc::new(CheckpointOwnerHooks::default()),
        }
    }

    #[cfg(feature = "scenario")]
    pub fn set_scenario_hooks(&mut self, hooks: Arc<CheckpointOwnerHooks>) {
        self.hooks = hooks;
    }

    #[cfg(feature = "scenario")]
    pub async fn initialize_for_scenario(&mut self) -> Result<(), TaskFailure> {
        self.capture(None).await?;
        self.attempt_publication().await
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
        let capture = super::unix_ms().map_err(TaskFailure::typed)?;
        self.capture(Some((tail, capture))).await?;
        self.attempt_publication().await
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
        self.attempt_publication().await?;
        let mut retry_at = tokio::time::Instant::now() + Duration::from_secs(CHECKPOINT_RETRY_SECS);
        loop {
            tokio::select! {
                biased;
                _ = hourly.tick() => {
                    let tail = self.receipts.current_tail_binding().map_err(TaskFailure::typed)?;
                    let capture = super::unix_ms().map_err(TaskFailure::typed)?;
                    self.capture(Some((tail, capture))).await?;
                    self.attempt_publication().await?;
                }
                _ = tokio::time::sleep_until(retry_at), if self.pending.is_some() => self.attempt_publication().await?,
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
        #[cfg(feature = "scenario")]
        let hooks = self.hooks.clone();
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
                        #[cfg(feature = "scenario")]
                        let invalidation = super::invalidate_with_hooks(&frozen.tail.path, &hooks.invalidation);
                        #[cfg(not(feature = "scenario"))]
                        let invalidation = super::invalidate(&frozen.tail.path);
                        if let Err(invalidation) = invalidation {
                            return Err(TaskFailure {
                                kind: if matches!(invalidation, super::InvalidationError::QuarantineFailed(_)) {
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
            let candidate = candidate(&frozen, &receipts).map_err(TaskFailure::typed)?;
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
        let now = super::unix_ms().map_err(TaskFailure::typed)?;
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
                        super::unix_ms,
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
                }
                Ok(PublishOutcome::GenerationChanged { candidate, current }) => {
                    return Err(TaskFailure::typed(OwnerError::GenerationChanged {
                        candidate,
                        current,
                    }));
                }
                Ok(PublishOutcome::Refused(_)) => self.pending = None,
                Err(PublishError::Io(error)) => error!(%error, capture_unix_ms = capture,
                    last_published_capture_unix_ms = self.last_published_capture,
                    last_published_age_ms = self.last_published_capture.map(|time| now.saturating_sub(time)),
                    "source checkpoint publication retry"),
            }
        }
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
    Ok(Some(super::serialize(
        CheckpointData {
            format_version: 1,
            scanner_version: 1,
            reducer_version: ACTIVITY_REDUCER_VERSION,
            financial_era: frozen.financial_era,
            activation: frozen.activation.clone(),
            tail: frozen.tail.clone(),
            prefix_blake3: digest.finalize().to_hex().to_string(),
            receipts: receipts
                .checkpoint_prefix(count, &frozen.tail)
                .map_err(anyhow::Error::from)?,
            activity: frozen.reducers.activity.clone(),
            daily_boundary: frozen.reducers.daily_boundary.clone(),
        },
        generation,
        frozen.capture_unix_ms,
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
        let candidate = candidate(fresh.frozen.as_ref().unwrap(), &fixture.receipts)
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
    async fn tick() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_publication_failure_retries_retained_candidate() {
        let mut fixture = Fixture::new();
        fixture.append();
        let failures = Arc::new(AtomicBool::new(false));
        let flag = failures.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let hooks = Arc::new(CheckpointOwnerHooks {
            checkpoint_write: Some(Arc::new(move || {
                if flag.load(Ordering::Acquire) {
                    Err(std::io::Error::other("retryable write"))
                } else {
                    Ok(())
                }
            })),
            after_publication: Some(Arc::new(move |bytes, capture, success| {
                tx.send((bytes.to_vec(), capture, success)).unwrap();
            })),
            ..Default::default()
        });
        let slot = CheckpointJobSlot::default();
        let mut owner = fixture.owner(slot.clone(), false, 0);
        owner.set_scenario_hooks(hooks);
        let (shutdown, _) = crate::supervisor::ShutdownController::new();
        let task = tokio::spawn(owner.run(shutdown.subscribe()));
        let initial = receive(&mut rx).await;
        assert!(initial.2);
        let first_tail = fixture.append();
        failures.store(true, Ordering::Release);
        tick().await;
        tokio::time::advance(Duration::from_secs(CHECKPOINT_PUBLISH_SECS)).await;
        let failed = receive(&mut rx).await;
        assert!(!failed.2);
        let decoded: serde_json::Value = serde_json::from_slice(&failed.0[65..]).unwrap();
        assert_eq!(decoded["tail"]["physical_tail"], first_tail.physical_tail);
        for _ in 0..2 {
            tick().await;
            tokio::time::advance(Duration::from_secs(CHECKPOINT_RETRY_SECS)).await;
            assert_eq!(receive(&mut rx).await, failed);
        }
        // New frames never alter a retained retry. The next hourly capture replaces it.
        let later = fixture.append();
        tick().await;
        tokio::time::advance(Duration::from_secs(
            CHECKPOINT_PUBLISH_SECS - 2 * CHECKPOINT_RETRY_SECS,
        ))
        .await;
        let replacement = receive(&mut rx).await;
        assert!(!replacement.2);
        assert_ne!(replacement.0, failed.0);
        let body: serde_json::Value = serde_json::from_slice(&replacement.0[65..]).unwrap();
        assert_eq!(body["tail"]["physical_tail"], later.physical_tail);
        failures.store(false, Ordering::Release);
        tick().await;
        tokio::time::advance(Duration::from_secs(CHECKPOINT_RETRY_SECS)).await;
        let success = receive(&mut rx).await;
        assert_eq!((&success.0, success.1), (&replacement.0, replacement.1));
        assert!(success.2);
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
            let slot = CheckpointJobSlot::default();
            let mut owner = fixture.owner(slot.clone(), false, if active { 4 } else { 0 });
            let fail = Arc::new(AtomicBool::new(true));
            let write_fail = fail.clone();
            owner.set_scenario_hooks(Arc::new(CheckpointOwnerHooks {
                checkpoint_write: Some(Arc::new(move || {
                    if write_fail.load(Ordering::Acquire) {
                        Err(std::io::Error::other("storage outage"))
                    } else {
                        Ok(())
                    }
                })),
                ..Default::default()
            }));
            owner.initialize_for_scenario().await.unwrap();
            let first = owner.pending.clone().unwrap();
            assert_eq!(first.capture_unix_ms, 123);
            assert!(owner.last_published_capture.is_none());
            tokio::time::advance(Duration::from_secs(CHECKPOINT_PUBLISH_SECS + 1)).await;
            owner.attempt_publication().await.unwrap();
            assert!(Arc::ptr_eq(&first, owner.pending.as_ref().unwrap()));
            let tail = fixture.append();
            owner.capture(Some((tail.clone(), 789))).await.unwrap();
            assert!(!Arc::ptr_eq(&first, owner.pending.as_ref().unwrap()));
            owner.attempt_publication().await.unwrap();
            fail.store(false, Ordering::Release);
            tokio::time::advance(Duration::from_secs(CHECKPOINT_RETRY_SECS)).await;
            owner.attempt_publication().await.unwrap();
            assert_eq!(owner.last_published_capture, Some(789));
            let authority = super::super::read_authority(&tail.path).unwrap();
            assert!(authority.permits_checkpoint());
            assert_eq!(
                super::super::load_checkpoint(&tail.path, &fixture.activation, false)
                    .unwrap()
                    .data
                    .tail,
                tail
            );
            slot.join().await.unwrap();
        }
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
                    while writes.load(Ordering::Acquire) == 0 {
                        tokio::task::yield_now().await;
                    }
                    fixture.append();
                    tick().await;
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
