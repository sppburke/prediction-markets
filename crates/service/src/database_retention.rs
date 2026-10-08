//! Database half of the checkpoint owner's daily retention job.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pe_core_types::{EventSeq, WalletAddress};
use pe_paper_state::{AnchorProofCursor, PaperStateDb, WalletRetentionWait};
use tokio::sync::{mpsc, oneshot};

use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{PaperLog, PaperLogFrame, PaperLogRecord, ScannedPaperFrame};
use crate::watchlist_admission::AdmissionPreparer;

/// Superseded proofs blanked per transaction. Each run's transactions resume after the last key
/// blanked; on 10/8 in production, 16-row batches that rescanned the blank rows held the shared
/// connection for up to 5.3 s.
const BLANK_BATCH_ROWS: usize = 4;
/// A page walks every row of its wallets' tables; one 128-wallet page held 0.53 s in production on 10/8.
const CANDIDATE_PAGE_WALLETS: usize = 32;
/// Trade ids a drain transaction removes (about four row deletions each; 2.6 ms per trade id in
/// production on 10/8, so about 0.17 s per transaction).
const DRAIN_TRADE_IDS: usize = 64;
/// Every retention transaction and eligibility check is followed by a pause at least as long as it
/// held the shared connection, and never shorter than this. That bounds the job's duty cycle only;
/// the connection's mutex is not fair, so it gives no waiter a maximum wait.
const PAUSE_FLOOR: Duration = Duration::from_millis(50);

#[derive(Debug, thiserror::Error)]
pub enum DatabaseRetentionError {
    #[error("database retention: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("database retention paper log: {0}")]
    PaperLog(#[from] crate::paper_recovery::PaperLogScanError),
    #[error("database retention orchestrator unavailable")]
    OrchestratorUnavailable,
    #[error("wallet retirement: {0}")]
    Retirement(String),
    #[error("database retention clock underflow")]
    ClockUnderflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionWait {
    BoundaryNotCurrent,
    WalkIncomplete,
    StructuralMember,
    RecentMembership,
    MembershipDuringProcess,
    BootObligation,
    CurrentObligation,
    RetainedWindow,
    ReducerPin,
    PublishedObservation,
    Durable(WalletRetentionWait),
}

#[derive(Debug)]
pub struct WalletRetirement {
    pub waiting: Option<RetentionWait>,
    pub lock_time: Option<Duration>,
}

#[derive(Debug, Default)]
pub struct DatabaseRetentionReport {
    pub anchors_blanked: usize,
    pub wallets_swapped_out: usize,
    pub wallets_waiting: Vec<(WalletAddress, RetentionWait)>,
    pub wallets_drained: usize,
    pub trade_ids_drained: usize,
    /// Wallets still listed for draining when the run ended; `None` when it was cancelled (a
    /// cancelled run starts no further hold, so it does not read the list again).
    pub wallets_listed: Option<usize>,
    pub cancelled: bool,
}

/// One wallet's completed drain.
#[derive(Debug, Default)]
pub struct WalletDrainReport {
    pub trade_ids: usize,
}

async fn pace(lock_time: Duration) {
    tokio::time::sleep(lock_time.max(PAUSE_FLOOR)).await;
}

/// The drain list, read as one logged hold followed by its pause.
pub async fn drain_list(
    paper_state: &PaperStateDb,
) -> Result<Vec<WalletAddress>, pe_paper_state::PaperStateError> {
    let read = paper_state.retirement_drains()?;
    pace_drain_list_read(read.result.len(), read.lock_time).await;
    Ok(read.result)
}

pub(crate) async fn pace_drain_list_read(wallets: usize, lock_time: Duration) {
    tracing::info!(
        wallets,
        lock_time_micros = lock_time.as_micros(),
        "source retention drain list read"
    );
    pace(lock_time).await;
}

/// Drain a listed wallet's remaining trade rows in paced transactions until its list entry is gone.
/// The caller holds the admission attempt lock. Returns `None` when `stop` holds before a
/// transaction (cancellation or a deadline); the wallet then stays listed for the next job or its
/// admission.
pub async fn drain_retired_wallet(
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Option<WalletDrainReport>, pe_paper_state::PaperStateError> {
    let mut report = WalletDrainReport::default();
    loop {
        if stop() {
            return Ok(None);
        }
        let step = paper_state.drain_retired_wallet(wallet, DRAIN_TRADE_IDS)?;
        report.trade_ids += step.result.trade_ids;
        tracing::info!(
            %wallet,
            trade_ids = step.result.trade_ids,
            lock_time_micros = step.lock_time.as_micros(),
            "source retention drain transaction"
        );
        pace(step.lock_time).await;
        if step.result.finished {
            return Ok(Some(report));
        }
    }
}

/// Capture once before producers start; never refresh the boot prefix or obligation set.
pub struct DatabaseRetention {
    paper_state: Arc<PaperStateDb>,
    paper_log: PaperLog,
    live: LiveWatchlist,
    preparer: AdmissionPreparer,
    control_tx: mpsc::Sender<OrchestratorControl>,
    boot_last_sequence: Option<EventSeq>,
    boot_obligation_wallets: HashSet<WalletAddress>,
}

impl DatabaseRetention {
    pub fn new(
        paper_state: Arc<PaperStateDb>,
        paper_log: PaperLog,
        live: LiveWatchlist,
        preparer: AdmissionPreparer,
        control_tx: mpsc::Sender<OrchestratorControl>,
        boot_obligation_wallets: HashSet<WalletAddress>,
    ) -> Result<Self, DatabaseRetentionError> {
        let boot_last_sequence = paper_log
            .snapshot()?
            .last()
            .map(|frame| frame.receipt.sequence);
        Ok(Self {
            paper_state,
            paper_log,
            live,
            preparer,
            control_tx,
            boot_last_sequence,
            boot_obligation_wallets,
        })
    }

    /// The first reason the wallet waits, and how long the durable check held the shared connection
    /// (`None` when an earlier reason answered without it).
    fn waiting(
        &self,
        wallet: WalletAddress,
        recent_since_unix: i64,
        inputs: &DatabaseRetentionInputs<'_>,
    ) -> Result<(Option<RetentionWait>, Option<Duration>), DatabaseRetentionError> {
        if !inputs.committed_boundary_current {
            return Ok((Some(RetentionWait::BoundaryNotCurrent), None));
        }
        if !inputs.verified_walk_complete {
            return Ok((Some(RetentionWait::WalkIncomplete), None));
        }
        if self.live.structural_membership().contains(&wallet) {
            return Ok((Some(RetentionWait::StructuralMember), None));
        }
        let frames = self.paper_log.snapshot()?;
        let (memberships, latest_prepared) = retention_paper_state(&frames);
        if let Some((sequence, received_at)) = memberships.get(&wallet) {
            if self.boot_last_sequence.is_none_or(|boot| *sequence > boot) {
                return Ok((Some(RetentionWait::MembershipDuringProcess), None));
            }
            if received_at.unix_timestamp_nanos() > i128::from(recent_since_unix) * 1_000_000_000 {
                return Ok((Some(RetentionWait::RecentMembership), None));
            }
        }
        if self.boot_obligation_wallets.contains(&wallet) {
            return Ok((Some(RetentionWait::BootObligation), None));
        }
        if inputs.obligation_wallets.contains(&wallet) {
            return Ok((Some(RetentionWait::CurrentObligation), None));
        }
        if inputs.walk_wallets.contains(&wallet) {
            return Ok((Some(RetentionWait::RetainedWindow), None));
        }
        if inputs.reducer_pin_wallets.contains(&wallet) {
            return Ok((Some(RetentionWait::ReducerPin), None));
        }
        if inputs.published_observation_wallets.contains(&wallet) {
            return Ok((Some(RetentionWait::PublishedObservation), None));
        }
        let check =
            self.paper_state
                .wallet_retention_wait(wallet, recent_since_unix, latest_prepared)?;
        Ok((
            check.result.map(RetentionWait::Durable),
            Some(check.lock_time),
        ))
    }
}

/// All sets belong to this job's captured tail and committed authority. The owner supplies the
/// current obligation snapshot as well as the last *published*, rather than pending, reducer state.
pub struct DatabaseRetentionInputs<'a> {
    pub now_unix: i64,
    pub committed_boundary_current: bool,
    pub verified_walk_complete: bool,
    pub walk_wallets: &'a HashSet<WalletAddress>,
    pub reducer_pin_wallets: &'a HashSet<WalletAddress>,
    pub published_observation_wallets: &'a HashSet<WalletAddress>,
    pub obligation_wallets: &'a HashSet<WalletAddress>,
}

pub(crate) fn retention_paper_state(
    frames: &[Arc<ScannedPaperFrame>],
) -> (
    HashMap<WalletAddress, (EventSeq, time::OffsetDateTime)>,
    Option<EventSeq>,
) {
    let mut membership = HashMap::new();
    let mut latest_prepared = None;
    for frame in frames {
        match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::MembershipChanged { removed, added, .. }) => {
                for wallet in removed.iter().chain(added) {
                    membership.insert(
                        *wallet,
                        (frame.receipt.sequence, frame.envelope.received_at.0),
                    );
                }
            }
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. }) => {
                latest_prepared = Some(frame.receipt.sequence);
            }
            _ => {}
        }
    }
    (membership, latest_prepared)
}

async fn cancelled(cancel: &AtomicBool) {
    while !cancel.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Called only under the daily job's Start/seal/live-evidence pauses. Listed wallets are drained
/// first and blanking runs even if an advance deferred; removal requires a current boundary and this
/// job's complete verified walk.
pub async fn run_database_retention(
    state: &DatabaseRetention,
    inputs: DatabaseRetentionInputs<'_>,
    cancel: &AtomicBool,
) -> Result<DatabaseRetentionReport, DatabaseRetentionError> {
    let mut report = DatabaseRetentionReport::default();
    run_database_retention_steps(state, inputs, cancel, &mut report).await?;
    report.cancelled |= cancel.load(Ordering::Acquire);
    if !report.cancelled {
        report.wallets_listed = Some(drain_list(&state.paper_state).await?.len());
    }
    Ok(report)
}

/// Record one drained wallet, or the cancellation that left it listed.
fn drained(report: &mut DatabaseRetentionReport, drain: Option<WalletDrainReport>) -> bool {
    match drain {
        Some(drain) => {
            report.wallets_drained += 1;
            report.trade_ids_drained += drain.trade_ids;
            true
        }
        None => {
            report.cancelled = true;
            false
        }
    }
}

/// One eligibility check; a check that held the connection is logged and followed by its pause.
async fn checked(
    state: &DatabaseRetention,
    wallet: WalletAddress,
    recent_since_unix: i64,
    inputs: &DatabaseRetentionInputs<'_>,
) -> Result<Option<RetentionWait>, DatabaseRetentionError> {
    let (waiting, held) = state.waiting(wallet, recent_since_unix, inputs)?;
    if let Some(held) = held {
        tracing::info!(
            %wallet,
            waiting = ?waiting,
            lock_time_micros = held.as_micros(),
            "source retention eligibility check"
        );
        pace(held).await;
    }
    Ok(waiting)
}

async fn run_database_retention_steps(
    state: &DatabaseRetention,
    inputs: DatabaseRetentionInputs<'_>,
    cancel: &AtomicBool,
    report: &mut DatabaseRetentionReport,
) -> Result<(), DatabaseRetentionError> {
    let recent_since_unix = inputs
        .now_unix
        .checked_sub(crate::source_checkpoint::SOURCE_RETENTION_BUFFER_SECS)
        .ok_or(DatabaseRetentionError::ClockUnderflow)?;
    let stopped = || cancel.load(Ordering::Acquire);
    if stopped() {
        report.cancelled = true;
        return Ok(());
    }
    // A wallet removed earlier and not yet drained is finished first, under the attempt lock (E36).
    for wallet in drain_list(&state.paper_state).await? {
        let _attempt = tokio::select! {
            guard = state.preparer.lock_for_retention() => guard,
            () = cancelled(cancel) => { report.cancelled = true; return Ok(()); }
        };
        let drain = drain_retired_wallet(&state.paper_state, wallet, &stopped).await?;
        if !drained(report, drain) {
            return Ok(());
        }
    }
    let mut cursor = AnchorProofCursor::start();
    loop {
        if cancel.load(Ordering::Acquire) {
            report.cancelled = true;
            return Ok(());
        }
        let batch = state
            .paper_state
            .blank_superseded_anchor_proofs(&cursor, BLANK_BATCH_ROWS)?;
        let (blanked, next) = batch.result;
        report.anchors_blanked += blanked;
        tracing::info!(
            anchors_blanked = blanked,
            lock_time_micros = batch.lock_time.as_micros(),
            "source retention database transaction"
        );
        pace(batch.lock_time).await;
        if blanked < BLANK_BATCH_ROWS {
            break;
        }
        cursor = next;
    }
    let mut after = None;
    loop {
        if cancel.load(Ordering::Acquire) {
            report.cancelled = true;
            break;
        }
        // The candidate page holds the shared database connection like a transaction (AC1).
        let page = state
            .paper_state
            .retention_wallets(after, CANDIDATE_PAGE_WALLETS)?;
        tracing::info!(
            wallets = page.result.len(),
            lock_time_micros = page.lock_time.as_micros(),
            "source retention database scan"
        );
        pace(page.lock_time).await;
        let wallets = page.result;
        if wallets.is_empty() {
            break;
        }
        for wallet in wallets {
            after = Some(wallet);
            if cancel.load(Ordering::Acquire) {
                report.cancelled = true;
                return Ok(());
            }
            let waiting = checked(state, wallet, recent_since_unix, &inputs).await?;
            if let Some(reason) = waiting {
                report.wallets_waiting.push((wallet, reason));
                continue;
            }
            let attempt = tokio::select! {
                guard = state.preparer.lock_for_retention() => guard,
                () = cancelled(cancel) => { report.cancelled = true; return Ok(()); }
            };
            if cancel.load(Ordering::Acquire) {
                report.cancelled = true;
                return Ok(());
            }
            let waiting = checked(state, wallet, recent_since_unix, &inputs).await?;
            if let Some(reason) = waiting {
                report.wallets_waiting.push((wallet, reason));
                continue;
            }
            // A cancel that arrived during the check's pause starts no removal.
            if cancel.load(Ordering::Acquire) {
                report.cancelled = true;
                return Ok(());
            }
            let (acknowledged, acknowledgement) = oneshot::channel();
            tokio::select! {
                result = state.control_tx.send(OrchestratorControl::RetireWallet { wallet, recent_since_unix, acknowledged }) => {
                    result.map_err(|_| DatabaseRetentionError::OrchestratorUnavailable)?;
                }
                () = cancelled(cancel) => { report.cancelled = true; return Ok(()); }
            }
            // Once handed off, keep the attempt lock until the owner acknowledges even on cancel.
            let result = acknowledgement
                .await
                .map_err(|_| DatabaseRetentionError::OrchestratorUnavailable)?
                .map_err(DatabaseRetentionError::Retirement)?;
            if let Some(time) = result.lock_time {
                tracing::info!(%wallet, lock_time_micros = time.as_micros(), "source retention database transaction");
                pace(time).await;
            }
            if let Some(reason) = result.waiting {
                report.wallets_waiting.push((wallet, reason));
                continue;
            }
            report.wallets_swapped_out += 1;
            // Still holding the attempt lock: no catch-up can rebuild the wallet while it drains.
            let drain = drain_retired_wallet(&state.paper_state, wallet, &stopped).await?;
            drop(attempt);
            if !drained(report, drain) {
                return Ok(());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn retention_pacing_waits_for_floor_or_full_lock_time() {
        for held in [
            Duration::ZERO,
            Duration::from_millis(49),
            Duration::from_millis(125),
        ] {
            let required = held.max(PAUSE_FLOOR);
            let start = tokio::time::Instant::now();
            let pause = pace(held);
            tokio::pin!(pause);
            assert!(futures::poll!(pause.as_mut()).is_pending());
            tokio::time::advance(required - Duration::from_millis(1)).await;
            assert!(futures::poll!(pause.as_mut()).is_pending());
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(futures::poll!(pause.as_mut()).is_ready());
            assert_eq!(start.elapsed(), required);
        }
    }
}
