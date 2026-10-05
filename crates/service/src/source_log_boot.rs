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

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
#[cfg(feature = "scenario")]
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result, ensure};
use pe_event_log::{EventEnvelope, LogTailBinding, Scanner};
use pe_paper_state::PaperStateDb;
use pe_paper_state::{MigrationMetadata, MigrationPhase};
use pe_trader_index::Watchlist;
use tracing::info;

use crate::paper_migration::{
    InstalledSourceProof, PaperMigrationBoot, PaperMigrationPaths, installed_source_prefix,
};
use crate::paper_recovery::{
    MembershipReplayError, PaperEra, ReplayedMembership, replay_membership_with_source,
};
use crate::qualification::PublishedMembershipSource;
use crate::risk_inputs::{SourceFrameMetadata, SourceReceiptIndex};
use crate::source_event_sink::SourceEventSink;
use crate::trade_poller::{
    ActivityCandidates, DailyBoundaryCandidates, ObligationRebuildError, ReconciliationObligations,
    recover_daily_boundary_anchor_from_era, recover_daily_boundary_from_candidates,
};

// Version 1 retained only the first non-admitted observation and could discard a
// later qualifying BUY. Rebuild those checkpoints from the authenticated source log.
const ACTIVITY_REDUCER_VERSION: u32 = 2;

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
struct Reducers {
    activity: ActivityCandidates,
    daily_boundary: Option<DailyBoundaryCandidates>,
    activity_error: Option<ObligationRebuildError>,
    boundary_error: Option<ObligationRebuildError>,
    frames: u64,
}

impl Reducers {
    fn new(financial_era: bool) -> Self {
        Self {
            activity: ActivityCandidates::default(),
            daily_boundary: financial_era.then(DailyBoundaryCandidates::default),
            activity_error: None,
            boundary_error: None,
            frames: 0,
        }
    }

    fn observe(&mut self, envelope: &EventEnvelope) {
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

    fn take_error(&mut self) -> Result<()> {
        if let Some(error) = self.activity_error.take() {
            return Err(error).context("rebuild durable activity reconciliation obligations");
        }
        if let Some(error) = self.boundary_error.take() {
            return Err(error).context("recover causal daily boundary");
        }
        Ok(())
    }
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

#[derive(Serialize, Deserialize)]
struct CheckpointData {
    format_version: u32,
    scanner_version: u32,
    reducer_version: u32,
    financial_era: bool,
    activation: LogTailBinding,
    tail: LogTailBinding,
    prefix_blake3: String,
    receipts: Vec<SourceFrameMetadata>,
    activity: ActivityCandidates,
    daily_boundary: Option<DailyBoundaryCandidates>,
}

const CHECKPOINT_HEADER_LEN: usize = 65;

struct FrozenCheckpoint {
    financial_era: bool,
    activation: LogTailBinding,
    tail: LogTailBinding,
    prefix_blake3: String,
    receipt_count: usize,
    activity: ActivityCandidates,
    daily_boundary: Option<DailyBoundaryCandidates>,
}

fn checkpoint_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".boot-checkpoint");
    PathBuf::from(value)
}

fn load_checkpoint(
    path: &Path,
    activation: &LogTailBinding,
    financial_era: bool,
) -> Option<CheckpointData> {
    let bytes = std::fs::read(checkpoint_path(path)).ok()?;
    let projections = bytes.get(CHECKPOINT_HEADER_LEN..)?;
    if bytes.get(..64)? != blake3::hash(projections).to_hex().as_bytes() || bytes[64] != b'\n' {
        return None;
    }
    let data: CheckpointData = serde_json::from_slice(projections).ok()?;
    drop(bytes);
    if data.format_version != 1
        || data.scanner_version != 1
        || data.reducer_version != ACTIVITY_REDUCER_VERSION
        || data.financial_era != financial_era
        || data.activation != *activation
        || data.tail.path != activation.path
        || data.tail.physical_tail < activation.physical_tail
        || data.tail.physical_tail > std::fs::metadata(path).ok()?.len()
        || data.tail.last_sequence < activation.last_sequence
        || (data.tail.physical_tail > activation.physical_tail
            && data.tail.last_sequence == activation.last_sequence)
        || data.daily_boundary.is_some() != financial_era
        || (data.tail.physical_tail == activation.physical_tail && data.tail != *activation)
    {
        return None;
    }
    SourceReceiptIndex::checkpoint_metadata_valid(path, &data.receipts, &data.tail).ok()?;
    Some(data)
}

fn publish_data(data: CheckpointData) -> Result<()> {
    let path = checkpoint_path(&data.tail.path);
    // Hash the exact serialized bytes, including map order, without an escaped payload copy.
    let mut encoded = vec![b'0'; CHECKPOINT_HEADER_LEN];
    encoded[64] = b'\n';
    serde_json::to_writer(&mut encoded, &data)?;
    drop(data);
    let checksum = blake3::hash(&encoded[CHECKPOINT_HEADER_LEN..]).to_hex();
    encoded[..64].copy_from_slice(checksum.as_bytes());
    crate::qualification::write_report(&path, &encoded)
        .context("publish source-log boot checkpoint")
}

impl SourceLogBoot {
    /// Walk the source log once under the writer lock when the paper main holds an exact
    /// `Installed` record whose recorded source path is the configured one. Any other record
    /// (a migration boot or a mid-migration phase) yields `None` and the caller keeps its
    /// existing flow. The walk fails closed on any frame error, prefix mismatch, lock
    /// contention, or truncation into the recorded prefix; it repairs only a scanner-proven
    /// incomplete final frame after that prefix.
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
        let mut checkpoint = load_checkpoint(&paths.source_log, prefix.binding(), financial_era);
        info!(
            elapsed_ms = u64::try_from(loading_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "source checkpoint loaded"
        );
        let checkpoint_binding = checkpoint
            .as_ref()
            .map(|data| (data.tail.clone(), data.prefix_blake3.clone()));
        let staged = RefCell::new((
            SourceReceiptIndex::staging(&paths.source_log).context("stage source receipt index")?,
            Reducers::new(financial_era),
            None,
        ));
        let mut start = |used: bool| {
            if !used {
                checkpoint = None;
                return;
            }
            if let Some(data) = checkpoint.take() {
                let mut stage = staged.borrow_mut();
                // Compatibility and receipt consistency were checked before the locked raw hash.
                match SourceReceiptIndex::restore_staging(
                    &paths.source_log,
                    data.receipts,
                    &data.tail,
                ) {
                    Ok(index) => stage.0 = index,
                    Err(error) => stage.2 = Some(error),
                }
                stage.1.activity = data.activity;
                stage.1.daily_boundary = data.daily_boundary;
            }
        };
        let mut observer = |offset: u64, envelope: &EventEnvelope| {
            let mut stage = staged.borrow_mut();
            if stage.2.is_none()
                && let Err(error) = stage.0.observe(offset, envelope)
            {
                stage.2 = Some(error);
            }
            stage.1.observe(envelope);
        };
        let mut digest = blake3::Hasher::new();
        let (sink, binding, verification) = SourceEventSink::open_verified_checkpoint(
            &paths.source_log,
            prefix.binding(),
            checkpoint_binding
                .as_ref()
                .map(|(tail, hash)| (tail, hash.as_str())),
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
        info!(
            elapsed_ms = u64::try_from(verification.prefix_elapsed.as_millis()).unwrap_or(u64::MAX),
            "source checkpoint raw prefix verified"
        );
        info!(
            elapsed_ms = u64::try_from(verification.suffix_elapsed.as_millis()).unwrap_or(u64::MAX),
            "source checkpoint suffix processed"
        );
        let (staging, mut reducers, index_error) = staged.into_inner();
        if let Some(error) = index_error {
            return Err(error).context("index source-log receipts during boot walk");
        }
        reducers.take_error()?;
        let receipt_index = staging
            .complete(&binding)
            .context("complete source receipt index at verified tail")?;
        let frozen = FrozenCheckpoint {
            financial_era,
            activation: prefix.binding().clone(),
            tail: binding.clone(),
            prefix_blake3: digest.finalize().to_hex().to_string(),
            receipt_count: binding.last_sequence.map_or(Ok(0), |seq| {
                usize::try_from(seq.0)
                    .ok()
                    .and_then(|n| n.checked_add(1))
                    .context("checkpoint receipt count overflow")
            })?,
            activity: reducers.activity.clone(),
            daily_boundary: reducers.daily_boundary.clone(),
        };
        info!(
            checkpoint_used,
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

    /// Publish the frozen initial boot prefix, after the producer barrier. Later appends are
    /// intentionally excluded and become the next restart's suffix.
    pub fn publish_checkpoint(&self) -> Result<()> {
        let frozen = &self.checkpoint;
        publish_data(CheckpointData {
            format_version: 1,
            scanner_version: 1,
            reducer_version: ACTIVITY_REDUCER_VERSION,
            financial_era: frozen.financial_era,
            activation: frozen.activation.clone(),
            tail: frozen.tail.clone(),
            prefix_blake3: frozen.prefix_blake3.clone(),
            receipts: self
                .receipt_index
                .checkpoint_prefix(frozen.receipt_count, &frozen.tail)?,
            activity: frozen.activity.clone(),
            daily_boundary: frozen.daily_boundary.clone(),
        })
    }

    /// Prepare while an installed service appends: read-only database/log access, finite size
    /// bound, no upgrades, activation census, writer lock, or torn-tail repair; also validates
    /// the open continuations read before the bound.
    pub fn prepare_checkpoint(paper_path: &Path) -> Result<(LogTailBinding, usize)> {
        let record = MigrationMetadata::read_read_only(paper_path)?
            .context("checkpoint preparation requires installed migration metadata")?;
        ensure!(
            record.phase == MigrationPhase::Installed,
            "checkpoint preparation requires installed paper state"
        );
        let activation = record
            .activation_tails
            .context("installed migration omitted activation tails")?
            .source;
        let paper = PaperStateDb::open_read_only_allowing_unmigrated(paper_path)?;
        let financial_era = paper.financial_start()?.is_some();
        // Read rows before bounding the log: every referenced frame is synced before its row
        // commits, so the bounded index covers them.
        let open = paper.open_decision_pending()?;
        let path = &activation.path;
        let byte_bound = std::fs::metadata(path)?.len();
        let cached = load_checkpoint(path, &activation, financial_era);
        let verified = cached
            .filter(|data| data.tail.physical_tail <= byte_bound)
            .and_then(|data| {
                let digest = Scanner::hash_prefix(path, data.tail.physical_tail).ok()?;
                (digest.finalize().to_hex().as_str() == data.prefix_blake3)
                    .then_some((data, digest))
            });
        let (mut staging, mut reducers, resume, mut digest) = if let Some((data, digest)) = verified
        {
            let staging = SourceReceiptIndex::restore_staging(path, data.receipts, &data.tail)?;
            let mut reducers = Reducers::new(financial_era);
            reducers.activity = data.activity;
            reducers.daily_boundary = data.daily_boundary;
            (staging, reducers, Some(data.tail), digest)
        } else {
            (
                SourceReceiptIndex::staging(path)?,
                Reducers::new(financial_era),
                None,
                blake3::Hasher::new(),
            )
        };
        let mut index_error = None;
        let tail = Scanner::walk_bounded(
            path,
            byte_bound,
            &activation,
            resume.as_ref(),
            &mut digest,
            &mut |offset, envelope| {
                if index_error.is_none()
                    && let Err(error) = staging.observe(offset, envelope)
                {
                    index_error = Some(error);
                }
                reducers.observe(envelope);
            },
        )?;
        if let Some(error) = index_error {
            return Err(error).context("prepare source receipt index");
        }
        reducers.take_error()?;
        let index = staging.complete(&tail)?;
        let validated = crate::bucket_commit::validate_continuation_rows(&paper, open, &index)
            .context("validate open decision continuations before deployment")?;
        let count = tail.last_sequence.map_or(Ok(0), |seq| {
            usize::try_from(seq.0)
                .ok()
                .and_then(|n| n.checked_add(1))
                .context("checkpoint receipt count overflow")
        })?;
        publish_data(CheckpointData {
            format_version: 1,
            scanner_version: 1,
            reducer_version: ACTIVITY_REDUCER_VERSION,
            financial_era,
            activation,
            tail: tail.clone(),
            prefix_blake3: digest.finalize().to_hex().to_string(),
            receipts: index.checkpoint_prefix(count, &tail)?,
            activity: reducers.activity,
            daily_boundary: reducers.daily_boundary,
        })?;
        Ok((tail, validated))
    }

    /// Install the scenario-only fault seams. Scenario builds only.
    #[cfg(feature = "scenario")]
    pub fn set_scenario_hooks(&mut self, hooks: Arc<SourceLogBootHooks>) {
        self.hooks = Some(hooks);
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
        crate::paper_recovery::feed_latch_basis(&era)?;
        obligations.retire_feed_incidents(&era, paper_state)?;
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
