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

use std::path::Path;
#[cfg(feature = "scenario")]
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result, ensure};
use pe_event_log::{EventEnvelope, LogTailBinding};
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
use crate::risk_inputs::SourceReceiptIndex;
use crate::source_event_sink::SourceEventSink;
use crate::trade_poller::{
    ActivityCandidates, DailyBoundaryCandidates, ObligationRebuildError, ReconciliationObligations,
    recover_daily_boundary_anchor, recover_daily_boundary_from_candidates,
};

/// Scenario-only fault seams (absent from ordinary builds).
#[cfg(feature = "scenario")]
#[derive(Default)]
pub struct SourceLogBootHooks {
    /// Poison the boot sink immediately before the bounded suffix walk captures its tail.
    pub poison_sink_before_extend: std::sync::atomic::AtomicBool,
}

/// The reducers fed by every verified frame. The first reducer error is latched and reported after
/// the walk; a physical scanner error surfaces first because the walk itself returns it.
struct Reducers {
    activity: ActivityCandidates,
    daily_boundary: Option<DailyBoundaryCandidates>,
    first_error: Option<ObligationRebuildError>,
    frames: u64,
}

impl Reducers {
    fn new(financial_era: bool) -> Self {
        Self {
            activity: ActivityCandidates::default(),
            daily_boundary: financial_era.then(DailyBoundaryCandidates::default),
            first_error: None,
            frames: 0,
        }
    }

    fn observe(&mut self, envelope: &EventEnvelope) {
        self.frames = self.frames.saturating_add(1);
        if self.first_error.is_some() {
            return;
        }
        if let Err(error) = self.activity.observe_activity(envelope) {
            self.first_error = Some(error);
            return;
        }
        if let Some(candidates) = self.daily_boundary.as_mut()
            && let Err(error) = candidates.observe_daily_boundary(envelope)
        {
            self.first_error = Some(error);
        }
    }

    fn take_error(&mut self) -> Result<()> {
        match self.first_error.take() {
            Some(error) => Err(error).context("reduce source-log frames during the boot walk"),
            None => Ok(()),
        }
    }
}

/// The published boot projections of one installed source log.
pub struct SourceLogBoot {
    receipt_index: SourceReceiptIndex,
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
        let mut staging = SourceReceiptIndex::staging(&paths.source_log)
            .context("stage the source receipt index")?;
        let mut reducers = Reducers::new(financial_era);
        let mut index_error = None;
        let (sink, binding) = {
            let mut observer = |offset: u64, envelope: &EventEnvelope| {
                if index_error.is_none()
                    && let Err(error) = staging.observe(offset, envelope)
                {
                    index_error = Some(error);
                }
                reducers.observe(envelope);
            };
            SourceEventSink::open_verified(&paths.source_log, Some(prefix.binding()), &mut observer)
                .with_context(|| {
                    format!(
                        "verify and open source event log {}",
                        paths.source_log.display()
                    )
                })?
        };
        if let Some(error) = index_error {
            return Err(error).context("index source-log receipts during the boot walk");
        }
        reducers.take_error()?;
        let receipt_index = staging
            .complete(&binding)
            .context("complete the source receipt index at the verified tail")?;
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
            .into_obligations(paper_state)
            .context("rebuild durable activity reconciliation obligations")?;
        if let Some(candidates) = self.reducers.daily_boundary.take()
            && let Some(anchor) = recover_daily_boundary_anchor(paper_log_path, &mut obligations)
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
