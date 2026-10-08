//! One locked walk of the source event log at ordinary boot (#572).
//!
//! An installed generation used to verify and decode the whole source log nine or more times per
//! boot. This facade composes the owners that already exist: the scanner verifies every frame once
//! under the writer's exclusive lock while feeding the receipt index and the activity and
//! daily-boundary candidate reducers; the migration owner binds the recorded activation prefix
//! during that same walk; the boot appends are walked once more, bounded by the synchronized writer
//! tail; and the projections are handed to their owners only after the walk and every reducer
//! succeeded. The two crate-private capability values (the installed-source proof and the
//! index-backed membership source) never leave this module: the binary receives results only.

use std::cell::RefCell;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
#[cfg(feature = "scenario")]
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result, ensure};
use pe_core_types::EventSeq;
use pe_event_log::{CheckpointPrefix, EventEnvelope, LogTailBinding, ReceiptRecord, Scanner};
use pe_paper_state::PaperStateDb;
use pe_trader_index::Watchlist;
use tracing::info;

use crate::paper_migration::{
    InstalledSourceProof, PaperMigrationBoot, PaperMigrationPaths, installed_source_prefix,
};
use crate::paper_recovery::{
    MembershipReplayError, PaperEra, ReplayedMembership, replay_membership_with_source,
};
use crate::qualification::PublishedMembershipSource;
use crate::risk_inputs::{SourceFrameMetadata, SourceReceiptIndex, SourceReceiptIndexStaging};
use crate::source_checkpoint::{
    self, CheckpointData, ConditionalInvalidation, PublicationReceipt, PublishOutcome,
};
use crate::source_event_sink::SourceEventSink;
use crate::trade_poller::{
    ActivityCandidates, DailyBoundaryCandidates, ObligationRebuildError, ReconciliationObligations,
    recover_daily_boundary_anchor_from_era, recover_daily_boundary_from_candidates,
};

// Version 1 retained only the first non-admitted observation and could discard a
// later qualifying BUY. Rebuild those checkpoints from the authenticated source log.
pub(crate) const ACTIVITY_REDUCER_VERSION: u32 = 3;

/// Scenario-only fault seams (absent from ordinary builds).
#[cfg(feature = "scenario")]
#[derive(Default)]
pub struct SourceLogBootHooks {
    /// Poison the boot sink immediately before the bounded suffix walk captures its tail.
    pub poison_sink_before_extend: std::sync::atomic::AtomicBool,
}

/// The reducers fed by every verified frame. Each reducer latches its own first error, reported
/// after the walk in owner order (activity, then daily boundary); a physical scanner error
/// surfaces first because the walk itself returns it.
pub(crate) struct Reducers {
    pub(crate) activity: ActivityCandidates,
    pub(crate) daily_boundary: Option<DailyBoundaryCandidates>,
    activity_error: Option<ObligationRebuildError>,
    boundary_error: Option<ObligationRebuildError>,
    frames: u64,
}

impl Reducers {
    pub(crate) fn new(financial_era: bool) -> Self {
        Self {
            activity: ActivityCandidates::default(),
            daily_boundary: financial_era.then(DailyBoundaryCandidates::default),
            activity_error: None,
            boundary_error: None,
            frames: 0,
        }
    }

    pub(crate) fn observe(&mut self, envelope: &EventEnvelope) {
        self.frames = self.frames.saturating_add(1);
        if self.activity_error.is_none()
            && let Err(error) = self.activity.observe_activity(envelope)
        {
            self.activity_error = Some(error);
        }
        if self.boundary_error.is_none()
            && let Some(candidates) = self.daily_boundary.as_mut()
            && let Err(error) = candidates.observe_daily_boundary(envelope)
        {
            self.boundary_error = Some(error);
        }
    }

    pub(crate) fn take_error(&mut self) -> Result<()> {
        self.take_activity_error()
            .context("rebuild durable activity reconciliation obligations")?;
        self.take_daily_error()
            .context("recover causal daily boundary")?;
        Ok(())
    }

    pub(crate) fn take_activity_error(&mut self) -> Result<(), ObligationRebuildError> {
        if let Some(error) = self.activity_error.take() {
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn take_daily_error(&mut self) -> Result<(), ObligationRebuildError> {
        if let Some(error) = self.boundary_error.take() {
            return Err(error);
        }
        Ok(())
    }
}

/// One recovery projection: durable receipt metadata, authenticated reducer pins, then
/// the scanner-verified retained window. Dependency pins are verified but never reduced.
pub(crate) struct SourceLogRebuild {
    staging: SourceReceiptIndexStaging,
    reducers: Reducers,
    index_error: Option<crate::risk_inputs::RiskInputsUnavailable>,
}

pub(crate) struct RebuiltSourceLog {
    pub(crate) index: SourceReceiptIndex,
    pub(crate) reducers: Reducers,
}

impl SourceLogRebuild {
    pub(crate) fn new(path: &Path, financial_era: bool) -> Result<Self> {
        let mut staging = SourceReceiptIndex::staging(path)?;
        let mut reducers = Reducers::new(financial_era);
        if let Some(authority) = staging.retention().cloned() {
            let mut receipts = BufReader::new(
                File::open(source_checkpoint::receipts_path(path))
                    .context("open durable retention receipt prefix")?,
            );
            let boundary = LogTailBinding {
                path: std::fs::canonicalize(path)?,
                physical_tail: authority.boundary.offset,
                last_sequence: Some(EventSeq(authority.boundary.sequence.0 - 1)),
                last_hash: authority.chain_head,
            };
            for sequence in 0..authority.boundary.sequence.0 {
                let record = ReceiptRecord::read(&mut receipts, EventSeq(sequence))
                    .context("read durable retention receipt prefix")?;
                staging.restore_record(
                    SourceFrameMetadata {
                        receipt: record.receipt,
                        received_millis: record.received_millis,
                        byte_offset: record.byte_offset,
                    },
                    &boundary,
                )?;
            }
            staging.validate_checkpoint(&boundary)?;
            staging.validate_retention()?;
            for pin in &authority.pins {
                let (envelope, end) = authority
                    .verify_pin(path, pin)
                    .context("verify retained source pin before rebuilding")?;
                let millis =
                    i64::try_from(envelope.received_at.0.unix_timestamp_nanos() / 1_000_000)?;
                ensure!(
                    staging
                        .frame_metadata(pin.sequence)
                        .is_some_and(|frame| frame.receipt.this_hash == pin.hash
                            && frame.received_millis == millis),
                    "retained source pin differs from its durable receipt"
                );
                let next = pin.sequence.0 + 1;
                let expected_end = if next == authority.boundary.sequence.0 {
                    authority.boundary.offset
                } else {
                    staging
                        .frame_metadata(EventSeq(next))
                        .and_then(|frame| frame.byte_offset)
                        .context("retained source pin successor receipt is absent")?
                };
                ensure!(
                    end == expected_end,
                    "retained source pin end differs from its durable receipt"
                );
                if pin.reducer {
                    reducers.observe(&envelope);
                }
            }
        }
        Ok(Self {
            staging,
            reducers,
            index_error: None,
        })
    }

    fn from_checkpoint(loaded: source_checkpoint::LoadedCheckpoint, financial_era: bool) -> Self {
        let mut reducers = Reducers::new(financial_era);
        reducers.activity = loaded.data.activity;
        reducers.daily_boundary = loaded.data.daily_boundary;
        Self {
            staging: loaded.staging,
            reducers,
            index_error: None,
        }
    }

    pub(crate) fn observe(&mut self, offset: u64, envelope: &EventEnvelope) {
        if self.index_error.is_none()
            && let Err(error) = self.staging.observe(offset, envelope)
        {
            self.index_error = Some(error);
        }
        self.reducers.observe(envelope);
    }

    pub(crate) fn complete(self, tail: &LogTailBinding) -> Result<RebuiltSourceLog> {
        if let Some(error) = self.index_error {
            return Err(error.into());
        }
        let index = self.staging.complete(tail)?;
        Ok(RebuiltSourceLog {
            index,
            reducers: self.reducers,
        })
    }
}

/// Read-only finite rebuild used by census, receipt, obligation and financial fallbacks.
pub(crate) fn rebuild_source_log(
    path: &Path,
    financial_era: bool,
    expected: Option<&LogTailBinding>,
) -> Result<RebuiltSourceLog> {
    let mut rebuild = SourceLogRebuild::new(path, financial_era)?;
    let byte_bound = std::fs::metadata(path)?.len();
    let default_expected = match rebuild.staging.retention() {
        Some(authority) => {
            ensure!(
                byte_bound >= authority.retained_tail.physical_tail,
                "source log is shorter than its committed retained tail"
            );
            authority.retained_tail.resolve(path)?
        }
        None => LogTailBinding {
            path: std::fs::canonicalize(path)?,
            physical_tail: pe_event_log::HEADER_LEN,
            last_sequence: None,
            last_hash: blake3::Hash::from_bytes([0; 32]),
        },
    };
    let tail = Scanner::walk_bounded(
        path,
        byte_bound,
        expected.unwrap_or(&default_expected),
        None,
        &mut blake3::Hasher::new(),
        &mut |offset, envelope| rebuild.observe(offset, envelope),
    )?;
    // A reader never repairs a torn capture, even beyond the committed tail.
    ensure!(
        tail.physical_tail == byte_bound,
        "source-log rebuild has an incomplete final frame"
    );
    rebuild.complete(&tail)
}

pub(crate) fn reopen_source_sink(path: &Path) -> Result<SourceEventSink> {
    let expected = match pe_event_log::RetentionAuthority::load(path)? {
        Some(authority) => authority.retained_tail.resolve(path)?,
        None => LogTailBinding {
            path: std::fs::canonicalize(path)?,
            physical_tail: pe_event_log::HEADER_LEN,
            last_sequence: None,
            last_hash: blake3::Hash::from_bytes([0; 32]),
        },
    };
    let mut rebuild = SourceLogRebuild::new(path, false)?;
    let (sink, tail, _) = SourceEventSink::open_verified_checkpoint(
        path,
        &expected,
        None,
        CheckpointPrefix::Defer,
        &mut blake3::Hasher::new(),
        &mut |_| {},
        &mut |offset, envelope| rebuild.observe(offset, envelope),
    )?;
    rebuild.complete(&tail)?;
    Ok(sink)
}

/// The published boot projections of one installed source log.
pub struct SourceLogBoot {
    receipt_index: SourceReceiptIndex,
    checkpoint: FrozenCheckpoint,
    reducers: Reducers,
    proof: InstalledSourceProof,
    binding_after: Option<LogTailBinding>,
    #[cfg(feature = "scenario")]
    hooks: Option<Arc<SourceLogBootHooks>>,
}

/// Result of the locked walk: the projections, the still-locked sink, and the whole-file binding.
pub struct OpenedSourceLog {
    pub boot: SourceLogBoot,
    pub sink: SourceEventSink,
    pub binding: LogTailBinding,
}

pub(crate) enum FrozenPrefix {
    Verified(Box<blake3::Hasher>),
    Deferred {
        tail: LogTailBinding,
        prefix_blake3: String,
    },
}

pub(crate) struct FrozenCheckpoint {
    pub(crate) authority_generation: Option<u64>,
    pub(crate) capture_unix_ms: u64,
    pub(crate) financial_era: bool,
    pub(crate) activation: LogTailBinding,
    pub(crate) tail: LogTailBinding,
    pub(crate) prefix: FrozenPrefix,
    pub(crate) reducers: Reducers,
}

impl SourceLogBoot {
    /// Walk the source log once under the writer lock when the paper main holds an exact
    /// `Installed` record whose recorded source path is the configured one. Any other record
    /// (a migration boot or a mid-migration phase) yields `None` and the caller keeps its
    /// existing flow. Suffix frame errors, lock contention, or truncation into the recorded
    /// prefix fail closed; only a scanner-proven incomplete final frame is repaired. A loaded
    /// checkpoint's raw prefix is deferred to the runtime checkpoint owner after listening.
    pub fn open(
        paths: &PaperMigrationPaths,
        financial_era: bool,
    ) -> Result<Option<OpenedSourceLog>> {
        let Some(prefix) = installed_source_prefix(paths)
            .context("read the installed migration record before the source-log walk")?
        else {
            return Ok(None);
        };
        let started = Instant::now();
        let loading_started = std::time::Instant::now();
        let epoch = source_checkpoint::retention_epoch(&paths.source_log)?;
        let authority = source_checkpoint::read_authority(&paths.source_log);
        let mut checkpoint = if authority
            .as_ref()
            .is_ok_and(|value| value.permits_checkpoint())
        {
            source_checkpoint::load_checkpoint(&paths.source_log, prefix.binding(), financial_era)?
                .filter(|loaded| loaded.retention_epoch == epoch)
        } else {
            None
        };
        let authority_generation = if checkpoint.is_none() {
            source_checkpoint::read_authority(&paths.source_log)
                .ok()
                .and_then(|value| value.generation())
        } else {
            authority.as_ref().ok().and_then(|value| value.generation())
        };
        info!(
            elapsed_ms = u64::try_from(loading_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "source checkpoint loaded"
        );
        let checkpoint_binding = checkpoint
            .as_ref()
            .map(|loaded| (loaded.data.tail.clone(), loaded.data.prefix_blake3.clone()));
        let staged = RefCell::new(None);
        let mut start = |used: bool| {
            let rebuild = if used {
                checkpoint
                    .take()
                    .context("loaded checkpoint missing")
                    .map(|loaded| SourceLogRebuild::from_checkpoint(loaded, financial_era))
            } else {
                checkpoint = None;
                SourceLogRebuild::new(&paths.source_log, financial_era)
            };
            *staged.borrow_mut() = Some(rebuild);
        };
        let mut observer = |offset: u64, envelope: &EventEnvelope| {
            if let Some(Ok(rebuild)) = staged.borrow_mut().as_mut() {
                rebuild.observe(offset, envelope);
            }
        };
        let mut digest = blake3::Hasher::new();
        let (sink, binding, verification) = SourceEventSink::open_verified_checkpoint(
            &paths.source_log,
            prefix.binding(),
            checkpoint_binding
                .as_ref()
                .map(|(tail, hash)| (tail, hash.as_str())),
            CheckpointPrefix::Defer,
            &mut digest,
            &mut start,
            &mut observer,
        )
        .with_context(|| {
            format!(
                "verify and open source event log {}",
                paths.source_log.display()
            )
        })?;
        let checkpoint_used = verification.used;
        if !verification.deferred {
            info!(
                elapsed_ms =
                    u64::try_from(verification.prefix_elapsed.as_millis()).unwrap_or(u64::MAX),
                "source checkpoint raw prefix verified"
            );
        }
        info!(
            elapsed_ms = u64::try_from(verification.suffix_elapsed.as_millis()).unwrap_or(u64::MAX),
            "source checkpoint suffix processed"
        );
        let RebuiltSourceLog {
            index: receipt_index,
            mut reducers,
            ..
        } = staged
            .into_inner()
            .context("source-log walk did not initialize its rebuild")??
            .complete(&binding)?;
        reducers.take_error()?;
        let hydration_started = Instant::now();
        reducers.activity.hydrate_bindings(&receipt_index)?;
        info!(
            elapsed_ms = u64::try_from(hydration_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "source checkpoint activity bindings hydrated"
        );
        let frozen = FrozenCheckpoint {
            authority_generation,
            capture_unix_ms: source_checkpoint::unix_ms()?,
            financial_era,
            activation: prefix.binding().clone(),
            tail: binding.clone(),
            prefix: if verification.deferred {
                let (tail, prefix_blake3) = checkpoint_binding
                    .as_ref()
                    .context("deferred checkpoint missing loaded binding")?;
                FrozenPrefix::Deferred {
                    tail: tail.clone(),
                    prefix_blake3: prefix_blake3.clone(),
                }
            } else {
                FrozenPrefix::Verified(Box::new(digest))
            },
            reducers: Reducers {
                activity: reducers.activity.clone(),
                daily_boundary: reducers.daily_boundary.clone(),
                ..Reducers::new(financial_era)
            },
        };
        info!(
            checkpoint_used,
            prefix_verification = if verification.deferred {
                "deferred"
            } else {
                "full_walk"
            },
            checkpoint_offset = checkpoint_binding
                .as_ref()
                .filter(|_| checkpoint_used)
                .map(|(tail, _)| tail.physical_tail),
            checkpoint_sequence = checkpoint_binding
                .as_ref()
                .filter(|_| checkpoint_used)
                .and_then(|(tail, _)| tail.last_sequence.map(|seq| seq.0)),
            checkpoint_hash = checkpoint_binding
                .as_ref()
                .filter(|_| checkpoint_used)
                .map(|(tail, _)| tail.last_hash.to_hex().to_string()),
            prefix_blake3 = checkpoint_binding
                .as_ref()
                .filter(|_| checkpoint_used)
                .map(|(_, hash)| hash.as_str()),
            prefix_bytes = checkpoint_binding
                .as_ref()
                .filter(|_| checkpoint_used)
                .map_or(0, |(tail, _)| tail.physical_tail),
            suffix_bytes = binding.physical_tail.saturating_sub(
                checkpoint_binding
                    .as_ref()
                    .filter(|_| checkpoint_used)
                    .map_or(0, |(tail, _)| tail.physical_tail)
            ),
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "source checkpoint verification completed"
        );
        let proof = prefix.prove();
        info!(
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            bytes = binding.physical_tail,
            frames = reducers.frames,
            activity_candidates = reducers.activity.len(),
            "source log walked"
        );
        Ok(Some(OpenedSourceLog {
            boot: Self {
                receipt_index,
                checkpoint: frozen,
                reducers,
                proof,
                binding_after: None,
                #[cfg(feature = "scenario")]
                hooks: None,
            },
            sink,
            binding,
        }))
    }

    /// Move the frozen boot prefix to its sole runtime owner after HTTP starts.
    pub fn into_checkpoint_owner(
        self,
        slot: source_checkpoint::CheckpointJobSlot,
    ) -> source_checkpoint::SourceCheckpointOwner {
        source_checkpoint::SourceCheckpointOwner::new(self.checkpoint, self.receipt_index, slot)
    }

    /// Prepare while an installed service appends: read-only database/log access, finite size
    /// bound, no upgrades, activation census, writer lock, or torn-tail repair; also validates
    /// the open continuations read before the bound.
    pub fn prepare_checkpoint(paper_path: &Path) -> Result<(PublicationReceipt, usize)> {
        Self::prepare_checkpoint_inner(
            paper_path,
            #[cfg(feature = "scenario")]
            None,
        )
    }

    /// Prepare with deterministic scenario-only pauses at capture, prefix-check and walk boundaries.
    #[cfg(feature = "scenario")]
    pub fn prepare_checkpoint_with_hooks(
        paper_path: &Path,
        hooks: &source_checkpoint::PreparationHooks,
    ) -> Result<(PublicationReceipt, usize)> {
        Self::prepare_checkpoint_inner(paper_path, Some(hooks))
    }

    fn prepare_checkpoint_inner(
        paper_path: &Path,
        #[cfg(feature = "scenario")] hooks: Option<&source_checkpoint::PreparationHooks>,
    ) -> Result<(PublicationReceipt, usize)> {
        loop {
            let activation = source_checkpoint::installed_activation(paper_path)?;
            let path = &activation.path;
            let epoch = source_checkpoint::retention_epoch(path)?;
            let attempt = (|| -> Result<Option<(PublicationReceipt, usize)>> {
                // The authority belongs to the verification, not to its eventual publication.
                let authority = source_checkpoint::read_authority(path)?;
                let mut generation = authority.generation();
                let paper = PaperStateDb::open_read_only_allowing_unmigrated(paper_path)?;
                let financial_era = paper.financial_start()?.is_some();
                // Capture open continuations before bounding: their receipts are already synchronized.
                let open = paper.open_decision_pending()?;
                let byte_bound = std::fs::metadata(path)?.len();
                let capture_unix_ms = source_checkpoint::unix_ms()?;
                #[cfg(feature = "scenario")]
                if let Some(pause) = hooks.and_then(|hooks| hooks.after_bound.as_ref()) {
                    pause()?;
                }
                let cached = if authority.permits_checkpoint() {
                    source_checkpoint::load_checkpoint(path, &activation, financial_era)?
                        .filter(|loaded| loaded.retention_epoch == epoch)
                } else {
                    None
                };
                if cached.is_none() {
                    generation = source_checkpoint::read_authority(path)?.generation();
                }
                let mut verified = None;
                if let Some(loaded) =
                    cached.filter(|loaded| loaded.data.tail.physical_tail <= byte_bound)
                {
                    #[cfg(feature = "scenario")]
                    if let Some(pause) = hooks.and_then(|hooks| hooks.before_cached_hash.as_ref()) {
                        pause()?;
                    }
                    // An I/O failure proves no digest mismatch: fall back without invalidation.
                    if let Ok(digest) = Scanner::hash_prefix(path, loaded.data.tail.physical_tail) {
                        #[cfg(feature = "scenario")]
                        if let Some(pause) =
                            hooks.and_then(|hooks| hooks.after_cached_hash.as_ref())
                        {
                            pause()?;
                        }
                        if digest.finalize().to_hex().as_str() == loaded.data.prefix_blake3 {
                            verified = Some((loaded, digest));
                        } else {
                            if source_checkpoint::retention_epoch(path)? != epoch {
                                return Ok(None);
                            }
                            match source_checkpoint::invalidate_if_current(
                                path,
                                generation
                                    .context("cached checkpoint requires readable authority")?,
                                epoch,
                                &loaded.checksum,
                            )? {
                                ConditionalInvalidation::Invalidated(current) => {
                                    generation = Some(current)
                                }
                                ConditionalInvalidation::Changed => return Ok(None),
                            }
                        }
                    }
                }
                let (mut rebuild, resume, mut digest) = if let Some((loaded, digest)) = verified {
                    let tail = loaded.data.tail.clone();
                    (
                        SourceLogRebuild::from_checkpoint(loaded, financial_era),
                        Some(tail),
                        digest,
                    )
                } else {
                    (
                        SourceLogRebuild::new(path, financial_era)?,
                        None,
                        blake3::Hasher::new(),
                    )
                };
                if let Some(retention) = rebuild.staging.retention() {
                    ensure!(
                        byte_bound >= retention.retained_tail.physical_tail,
                        "source log is shorter than its committed retained tail"
                    );
                }
                let tail = Scanner::walk_bounded(
                    path,
                    byte_bound,
                    &activation,
                    resume.as_ref(),
                    &mut digest,
                    &mut |offset, envelope| rebuild.observe(offset, envelope),
                )?;
                let RebuiltSourceLog {
                    index,
                    mut reducers,
                    ..
                } = rebuild.complete(&tail)?;
                reducers.take_error()?;
                if resume.is_some() {
                    index.verify_retention_pins()?;
                }
                reducers.activity.hydrate_bindings(&index)?;
                let validated =
                    crate::bucket_commit::validate_continuation_rows(&paper, open, &index)
                        .context("validate open decision continuations before deployment")?;
                #[cfg(feature = "scenario")]
                if let Some(pause) = hooks.and_then(|hooks| hooks.after_walk.as_ref()) {
                    pause()?;
                }
                let count = tail.last_sequence.map_or(Ok(0), |seq| {
                    usize::try_from(seq.0)
                        .ok()
                        .and_then(|n| n.checked_add(1))
                        .context("checkpoint receipt count overflow")
                })?;
                let start = source_checkpoint::capture_start(&activation, financial_era, count);
                let candidate = source_checkpoint::serialize(
                    CheckpointData {
                        format_version: 2,
                        generation: generation.unwrap_or(0),
                        receipt_count: Some(count),
                        scanner_version: 1,
                        reducer_version: ACTIVITY_REDUCER_VERSION,
                        financial_era,
                        activation: activation.clone(),
                        tail: tail.clone(),
                        prefix_blake3: digest.finalize().to_hex().to_string(),
                        receipts: index.checkpoint_suffix(start, count, &tail)?,
                        activity: reducers.activity,
                        daily_boundary: reducers.daily_boundary,
                    },
                    generation.context(
                        "checkpoint preparation authority unreadable; quiesced recovery required",
                    )?,
                    capture_unix_ms,
                    epoch,
                )?;
                info!(
                    manifest_bytes = candidate.bytes.len(),
                    "source checkpoint manifest serialized"
                );
                if source_checkpoint::retention_epoch(path)? != epoch {
                    return Ok(None);
                }
                match source_checkpoint::publish(&candidate, 1)? {
                    PublishOutcome::Published(receipt) => Ok(Some((receipt, validated))),
                    PublishOutcome::RetentionChanged { .. } => Ok(None),
                    outcome => anyhow::bail!("source checkpoint publication refused: {outcome:?}"),
                }
            })();
            if source_checkpoint::retention_epoch(path)? != epoch {
                continue;
            }
            if let Some(prepared) = attempt? {
                return Ok(prepared);
            }
        }
    }

    /// Install the scenario-only fault seams. Scenario builds only.
    #[cfg(feature = "scenario")]
    pub fn set_scenario_hooks(&mut self, hooks: Arc<SourceLogBootHooks>) {
        self.hooks = Some(hooks);
    }

    #[cfg(feature = "scenario")]
    pub fn checkpoint_assisted_for_scenario(&self) -> bool {
        matches!(self.checkpoint.prefix, FrozenPrefix::Deferred { .. })
    }

    /// The verified receipt projection (shared, `Arc`-backed).
    #[must_use]
    pub fn receipt_index(&self) -> SourceReceiptIndex {
        self.receipt_index.clone()
    }

    /// Replay the Start-bound structural membership through exact indexed reads.
    pub fn replay_membership(
        &self,
        era: &PaperEra,
        start_batch: Watchlist,
    ) -> Result<Option<ReplayedMembership>, MembershipReplayError> {
        let source = PublishedMembershipSource::from_index(self.receipt_index.clone());
        replay_membership_with_source(era, start_batch, &source)
    }

    /// Resume the installed paper migration, reusing this walk's matched source prefix.
    pub fn prepare_installed(
        &self,
        paths: PaperMigrationPaths,
        imported_at_unix: i64,
    ) -> Result<PaperMigrationBoot> {
        PaperMigrationBoot::prepare_installed(paths, imported_at_unix, self.proof.clone())
    }

    /// Walk the frames this boot appended, bounded by the sink's synchronized tail, and return
    /// that tail as the runtime source-log binding.
    pub fn extend(&mut self, sink: &mut SourceEventSink) -> Result<LogTailBinding> {
        #[cfg(feature = "scenario")]
        if self.hooks.as_ref().is_some_and(|hooks| {
            hooks
                .poison_sink_before_extend
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        }) {
            sink.poison_for_scenario();
        }
        let after = sink
            .verified_tail()
            .context("capture the synchronized source-log tail after the boot appends")?;
        let mut suffix_frames = 0u64;
        {
            let reducers = &mut self.reducers;
            let mut observer = |_offset: u64, envelope: &EventEnvelope| {
                suffix_frames = suffix_frames.saturating_add(1);
                reducers.observe(envelope);
            };
            self.receipt_index
                .catch_up_to(after.physical_tail, &mut observer)
                .context("walk the source-log frames appended during boot")?;
        }
        self.reducers.take_error()?;
        info!(frames = suffix_frames, "source log suffix walked");
        self.binding_after = Some(after.clone());
        Ok(after)
    }

    /// The obligations rebuilt from the walked candidates: the database filter, then (financial
    /// era only) the daily-boundary selection anchored on the paper log. Requires `extend`.
    pub fn obligations(
        &mut self,
        paper_state: &PaperStateDb,
        paper_log_path: &Path,
    ) -> Result<ReconciliationObligations> {
        ensure!(
            self.binding_after.is_some(),
            "source-log obligations requested before the boot suffix walk"
        );
        let activity = std::mem::take(&mut self.reducers.activity);
        let mut obligations = activity
            .into_obligations(paper_state, &self.receipt_index)
            .context("rebuild durable activity reconciliation obligations")?;
        let era = crate::paper_recovery::paper_era(crate::paper_recovery::scan_paper_log(
            paper_log_path,
        )?);
        if let Some(candidates) = self.reducers.daily_boundary.take()
            && let Some(anchor) = recover_daily_boundary_anchor_from_era(&era, &mut obligations)
                .context("recover causal daily boundary")?
        {
            recover_daily_boundary_from_candidates(candidates, anchor, &mut obligations);
        }
        Ok(obligations)
    }

    /// Constant-time drift check immediately before the sink is handed to the runtime
    /// coordinator: the synchronized tail must still equal the suffix-walk binding.
    pub fn verify_handoff(&self, sink: &mut SourceEventSink) -> Result<()> {
        let expected = self
            .binding_after
            .as_ref()
            .context("source-log handoff requested before the boot suffix walk")?;
        let actual = sink
            .verified_tail()
            .context("capture the source-log tail at the runtime handoff")?;
        ensure!(
            actual == *expected,
            "source log changed between the boot suffix walk and the runtime handoff: expected tail {} sequence {:?}, found tail {} sequence {:?}",
            expected.physical_tail,
            expected.last_sequence,
            actual.physical_tail,
            actual.last_sequence
        );
        Ok(())
    }
}
