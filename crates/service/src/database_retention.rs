//! Database half of the checkpoint owner's daily retention job.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pe_core_types::{EventSeq, WalletAddress};
use pe_paper_state::{PaperStateDb, WalletRetentionWait};
use tokio::sync::{mpsc, oneshot};

use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{PaperLog, PaperLogFrame, PaperLogRecord, ScannedPaperFrame};
use crate::watchlist_admission::AdmissionPreparer;

pub const RETENTION_BUFFER_SECS: i64 = 7 * 24 * 60 * 60;
const DATABASE_BATCH_LIMIT: usize = 128;

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
    pub transaction_lock_times: Vec<Duration>,
    pub cancelled: bool,
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

    fn waiting(
        &self,
        wallet: WalletAddress,
        recent_since_unix: i64,
        inputs: &DatabaseRetentionInputs<'_>,
    ) -> Result<Option<RetentionWait>, DatabaseRetentionError> {
        if !inputs.committed_boundary_current {
            return Ok(Some(RetentionWait::BoundaryNotCurrent));
        }
        if !inputs.verified_walk_complete {
            return Ok(Some(RetentionWait::WalkIncomplete));
        }
        if self.live.structural_membership().contains(&wallet) {
            return Ok(Some(RetentionWait::StructuralMember));
        }
        let frames = self.paper_log.snapshot()?;
        let (memberships, latest_prepared) = retention_paper_state(&frames);
        if let Some((sequence, received_at)) = memberships.get(&wallet) {
            if self.boot_last_sequence.is_none_or(|boot| *sequence > boot) {
                return Ok(Some(RetentionWait::MembershipDuringProcess));
            }
            if received_at.unix_timestamp_nanos() > i128::from(recent_since_unix) * 1_000_000_000 {
                return Ok(Some(RetentionWait::RecentMembership));
            }
        }
        if self.boot_obligation_wallets.contains(&wallet) {
            return Ok(Some(RetentionWait::BootObligation));
        }
        if inputs.obligation_wallets.contains(&wallet) {
            return Ok(Some(RetentionWait::CurrentObligation));
        }
        if inputs.walk_wallets.contains(&wallet) {
            return Ok(Some(RetentionWait::RetainedWindow));
        }
        if inputs.reducer_pin_wallets.contains(&wallet) {
            return Ok(Some(RetentionWait::ReducerPin));
        }
        if inputs.published_observation_wallets.contains(&wallet) {
            return Ok(Some(RetentionWait::PublishedObservation));
        }
        Ok(self
            .paper_state
            .wallet_retention_wait(wallet, recent_since_unix, latest_prepared)?
            .map(RetentionWait::Durable))
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

/// Called only under the daily job's Start/seal/live-evidence pauses. Blanking runs even if an
/// advance deferred; deletion requires a current boundary and this job's complete verified walk.
pub async fn run_database_retention(
    state: &DatabaseRetention,
    inputs: DatabaseRetentionInputs<'_>,
    cancel: &AtomicBool,
) -> Result<DatabaseRetentionReport, DatabaseRetentionError> {
    let mut report = DatabaseRetentionReport::default();
    let recent_since_unix = inputs
        .now_unix
        .checked_sub(RETENTION_BUFFER_SECS)
        .ok_or(DatabaseRetentionError::ClockUnderflow)?;
    loop {
        if cancel.load(Ordering::Acquire) {
            report.cancelled = true;
            return Ok(report);
        }
        let batch = state
            .paper_state
            .blank_superseded_anchor_proofs(DATABASE_BATCH_LIMIT)?;
        report.anchors_blanked += batch.result;
        report.transaction_lock_times.push(batch.lock_time);
        tracing::info!(
            anchors_blanked = batch.result,
            lock_time_micros = batch.lock_time.as_micros(),
            "source retention database transaction"
        );
        if batch.result < DATABASE_BATCH_LIMIT {
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut after = None;
    loop {
        if cancel.load(Ordering::Acquire) {
            report.cancelled = true;
            break;
        }
        // The candidate page holds the shared database connection like a transaction (AC1).
        let started = std::time::Instant::now();
        let wallets = state
            .paper_state
            .retention_wallets(after, DATABASE_BATCH_LIMIT)?;
        let lock_time = started.elapsed();
        report.transaction_lock_times.push(lock_time);
        tracing::info!(
            wallets = wallets.len(),
            lock_time_micros = lock_time.as_micros(),
            "source retention database scan"
        );
        if wallets.is_empty() {
            break;
        }
        for wallet in wallets {
            after = Some(wallet);
            if cancel.load(Ordering::Acquire) {
                report.cancelled = true;
                return Ok(report);
            }
            if let Some(reason) = state.waiting(wallet, recent_since_unix, &inputs)? {
                report.wallets_waiting.push((wallet, reason));
                continue;
            }
            let attempt = tokio::select! {
                guard = state.preparer.lock_for_retention() => guard,
                () = cancelled(cancel) => { report.cancelled = true; return Ok(report); }
            };
            if cancel.load(Ordering::Acquire) {
                report.cancelled = true;
                return Ok(report);
            }
            if let Some(reason) = state.waiting(wallet, recent_since_unix, &inputs)? {
                report.wallets_waiting.push((wallet, reason));
                continue;
            }
            let (acknowledged, acknowledgement) = oneshot::channel();
            tokio::select! {
                result = state.control_tx.send(OrchestratorControl::RetireWallet { wallet, recent_since_unix, acknowledged }) => {
                    result.map_err(|_| DatabaseRetentionError::OrchestratorUnavailable)?;
                }
                () = cancelled(cancel) => { report.cancelled = true; return Ok(report); }
            }
            // Once handed off, keep the attempt lock until the owner acknowledges even on cancel.
            let result = acknowledgement
                .await
                .map_err(|_| DatabaseRetentionError::OrchestratorUnavailable)?
                .map_err(DatabaseRetentionError::Retirement)?;
            drop(attempt);
            if let Some(time) = result.lock_time {
                report.transaction_lock_times.push(time);
                tracing::info!(%wallet, lock_time_micros = time.as_micros(), "source retention database transaction");
            }
            if let Some(reason) = result.waiting {
                report.wallets_waiting.push((wallet, reason));
            } else {
                report.wallets_swapped_out += 1;
            }
        }
        tokio::task::yield_now().await;
    }
    Ok(report)
}
