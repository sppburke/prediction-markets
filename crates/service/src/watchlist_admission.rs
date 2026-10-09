//! Durable pre-publication admission checks for runtime watchlist additions (#544).
//!
//! Lane C owns the history/fence boundary here. Lane D extends the marked seam
//! with the causal positions bracket; no sidecar or positions overlay is accepted.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn};
use pe_paper_state::{PaperStateDb, WalletCoverage};
use pe_trader_index::WatchlistEntry;
use serde::Serialize;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::activity_ingest::{SourceLogHandle, SourceLogHandleError};
use crate::bucket_commit::AnchorInstallError;
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{
    CapacityMembershipArtifact, KnockoutCausalArtifact, MembershipAdmissionArtifact,
    MembershipAdmissionReceipt, MembershipChange, MembershipProofError, MembershipProofManifest,
    MembershipReason, RankingMembershipArtifact, SealedKnockoutEvidence,
};
use crate::position_seeder::{
    AnchorInstall, CausalPositionError, CausalPositionValidator, FailureClass, ValidationPurpose,
    is_deferred_causal_position_error,
};
use crate::watchlist_maintenance::MembershipCommit;

const ADMISSION_PREPARE_ACK_TIMEOUT_SECS: u64 = 30;
pub const ADMISSION_RETRY_SECS: u64 = 300;
pub const ADMISSION_RETRY_MAX_SECS: u64 = 21_600;

/// Cooldown after a wallet's `consecutive_failures`-th consecutive transient admission-bracket
/// failure: [`ADMISSION_RETRY_SECS`] doubled per earlier failure, capped at
/// [`ADMISSION_RETRY_MAX_SECS`]. A count of 0 or 1 is the base cooldown.
#[must_use]
pub fn admission_retry_after(consecutive_failures: u32) -> Duration {
    let doublings = consecutive_failures.saturating_sub(1).min(16);
    Duration::from_secs(
        ADMISSION_RETRY_SECS
            .saturating_mul(1_u64 << doublings)
            .min(ADMISSION_RETRY_MAX_SECS),
    )
}

pub(crate) const CAPACITY_CONFIG_SOURCE_ID: &str = "pe-service.watchlist-capacity-config";
pub(crate) const RANKING_MEMBERSHIP_SOURCE_ID: &str = "pe-service.watchlist-ranking";
pub(crate) const MEMBERSHIP_ADMISSION_SOURCE_ID: &str = "pe-service.watchlist-admission";
pub(crate) const MEMBERSHIP_DEFERRAL_SOURCE_ID: &str = "pe-service.watchlist-deferral";
pub(crate) const KNOCKOUT_CAUSAL_SOURCE_ID: &str = "pe-service.watchlist-knockout";
pub(crate) const MEMBERSHIP_ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub(crate) const MEMBERSHIP_ARTIFACT_PARSER_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("reconciled market history unavailable for wallet {wallet}")]
    MissingHistory { wallet: WalletAddress },
    #[error("newly admitted wallet {wallet} is durably fenced")]
    Fenced { wallet: WalletAddress },
    #[error("orchestrator control channel closed before admission preparation")]
    ControlClosed,
    #[error("orchestrator admission preparation acknowledgement closed")]
    AcknowledgementClosed,
    #[error("orchestrator admission preparation exceeded {0} seconds")]
    AcknowledgementTimeout(u64),
    #[error("orchestrator rejected structural membership publication: {0}")]
    PublicationRejected(crate::watchlist_maintenance::PublishError),
    #[error("causal current-position validation unavailable: {0}")]
    PositionValidation(#[source] CausalPositionError),
    #[error("orchestrator rejected the accepted position brackets: {0}")]
    ValidationRejected(AnchorInstallError),
    #[error("accepted position bracket durability failed: {0}")]
    ValidationInstall(String),
    #[error("paper-state admission read failed: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("anchor refresh requires a causal position validator")]
    PositionValidatorUnavailable,
    #[error("membership publication requires the synchronized source-log handle")]
    MembershipSourceLogUnavailable,
    #[error("encode immutable membership artifact: {0}")]
    MembershipArtifactEncoding(#[source] serde_json::Error),
    #[error("accepted capacity target {target} cannot be represented durably")]
    CapacityTargetOverflow { target: usize },
    #[error("record immutable membership artifact: {0}")]
    MembershipSourceLog(#[from] SourceLogHandleError),
    #[error("capture immutable membership proof: {0}")]
    MembershipProof(#[from] MembershipProofError),
    #[error("admission queue key counter exhausted")]
    QueueKeyExhausted,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod admission_tests {
    use super::*;
    use pe_paper_state::{AnchorInstallRecord, WalletHistoryStatusRecord};

    fn wallet(byte: u8) -> WalletAddress {
        WalletAddress([byte; 20])
    }

    fn complete_history(state: &PaperStateDb, wallet: WalletAddress) {
        state
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn fence_and_history_are_per_wallet_and_ready_order_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let wallets = (1..=5).map(wallet).collect::<Vec<_>>();
        for wallet in [wallets[0], wallets[2], wallets[3], wallets[4]] {
            complete_history(&state, wallet);
        }
        rusqlite::Connection::open(&path).unwrap().execute(
            "INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)",
            [wallets[3].to_string()],
        ).unwrap();
        let (tx, mut rx) = mpsc::channel(5);
        let actor = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(OrchestratorControl::PrepareAdmissions {
                wallets,
                acknowledged,
            }) = rx.recv().await
            {
                seen.extend(wallets);
                acknowledged.send(()).unwrap();
            }
            seen
        });
        let preparer = AdmissionPreparer::new(tx, state);
        let result = preparer.prepare(&wallets).await.unwrap();
        assert_eq!(result.admitted, vec![wallets[0], wallets[2], wallets[4]]);
        assert_eq!(
            result
                .deferred
                .iter()
                .map(|item| (item.wallet, item.kind))
                .collect::<Vec<_>>(),
            vec![
                (wallets[3], "fence.active"),
                (wallets[1], "history.missing")
            ]
        );
        drop(preparer);
        assert_eq!(
            actor.await.unwrap(),
            vec![wallets[0], wallets[2], wallets[4]]
        );
    }

    #[tokio::test]
    async fn closed_control_channel_aborts_preparation() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let candidate = wallet(5);
        complete_history(&state, candidate);
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let abort = AdmissionPreparer::new(tx, state)
            .prepare(&[candidate])
            .await
            .unwrap_err();
        assert!(matches!(abort.cause, AdmissionError::ControlClosed));
        assert!(abort.deferred.is_empty());
    }

    #[test]
    fn proof_preflight_retains_prior_wallet_deferral_on_shared_read_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let missing_validation = wallet(6);
        let bad_read = wallet(7);
        for wallet in [missing_validation, bad_read] {
            complete_history(&state, wallet);
        }
        state.set_cursor(&missing_validation, 1).unwrap();
        state
            .install_anchors(&[AnchorInstallRecord {
                repaired_history: Vec::new(),
                expected_fence: None,
                history_status: None,
                wallet: missing_validation,
                balances: Vec::new(),
                activity_cutoff_unix: 1,
                anchored_at_unix: 1,
                ledger_hash_after: "ledger".to_owned(),
                positions_proof_hash: "positions".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "test".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: 1,
            }])
            .unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "DELETE FROM position_validations WHERE wallet_hex = ?1",
            [missing_validation.to_string()],
        )
        .unwrap();
        conn.execute("UPDATE wallet_history_status_v2 SET proof_json = CAST(X'FF' AS TEXT) WHERE wallet_hex = ?1", [bad_read.to_string()]).unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let preparer = AdmissionPreparer::new(tx, state);
        let abort = preparer
            .capture_proofs(&[missing_validation, bad_read])
            .unwrap_err();
        assert!(matches!(
            abort.cause,
            AdmissionError::MembershipProof(MembershipProofError::PaperState(_))
        ));
        assert_eq!(abort.deferred.len(), 1);
        assert_eq!(abort.deferred[0].wallet, missing_validation);
        assert_eq!(abort.deferred[0].kind, "proof.missing_validation");
    }
    #[tokio::test(start_paused = true)]
    async fn paper_service_rollout_mutex_wait_consumes_launch_budget_without_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let (tx, mut rx) = mpsc::channel(1);
        let preparer = AdmissionPreparer::new(tx, state.clone());
        let held = preparer.inner.attempt.lock().await;
        let deadline = Instant::now() + Duration::from_secs(10);
        let outcome = preparer
            .prepare_with_cursors(
                &[wallet(1), wallet(2)],
                None,
                Some(deadline),
                AdmissionContext::Other,
            )
            .await
            .unwrap();
        assert_eq!(Instant::now(), deadline);
        assert!(outcome.started.is_empty());
        assert!(outcome.deferred.is_empty());
        assert_eq!(outcome.unstarted, vec![wallet(1), wallet(2)]);
        assert!(rx.try_recv().is_err());
        assert!(state.cursor(&wallet(1)).unwrap().is_none());
        drop(held);
    }

    /// A wallet retention removed but did not drain: a group and a gate result left, with their trade rows.
    fn listed_with_leftovers(
        path: &std::path::Path,
        state: &PaperStateDb,
        candidate: WalletAddress,
    ) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute(
            "INSERT INTO activity_groups VALUES ('left', 'tx', ?1, 1, 'r', 'TRADE', 'applied', '{}')",
            [candidate.to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO entry_gate_results VALUES ('left-gate', ?1, 'market', 1, 'admitted', 1)",
            [candidate.to_string()],
        )
        .unwrap();
        for id in ["left", "left-gate"] {
            conn.execute("INSERT INTO seen_trades_v2 VALUES (?1, 2, 'tx')", [id])
                .unwrap();
        }
        assert_eq!(
            state.retire_wallet(candidate, 0, None).unwrap().result,
            None
        );
        assert_eq!(state.retirement_drains().unwrap().result, vec![candidate]);
    }

    fn seen(state: &PaperStateDb, id: &str) -> bool {
        state
            .is_seen(&pe_core_types::SourceTradeId(id.to_owned()))
            .unwrap()
    }

    #[tokio::test]
    async fn preparation_drains_a_listed_wallet_even_when_its_fence_check_defers_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let candidate = wallet(8);
        listed_with_leftovers(&path, &state, candidate);
        // A fence the wallet holds defers it at the fence check; the drain runs before that check.
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "INSERT INTO wallet_fences VALUES (?1, 'left', 'fixture', '{}', 1)",
                [candidate.to_string()],
            )
            .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let outcome = AdmissionPreparer::new(tx, state.clone())
            .prepare(&[candidate])
            .await
            .unwrap();
        assert!(matches!(
            outcome.deferred.as_slice(),
            [deferral] if deferral.wallet == candidate && deferral.stage == "fence"
        ));
        assert!(outcome.started.is_empty() && outcome.admitted.is_empty());
        assert!(rx.try_recv().is_err());
        assert!(state.retirement_drains().unwrap().result.is_empty());
        assert!(!seen(&state, "left") && !seen(&state, "left-gate"));
        assert_eq!(state.last_activity_group_epoch(&candidate).unwrap(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn preparation_deadline_mid_drain_leaves_the_wallet_listed_and_unstarted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let candidate = wallet(9);
        listed_with_leftovers(&path, &state, candidate);
        let (tx, mut rx) = mpsc::channel(1);
        let fetcher = Arc::new(pe_source_polymarket_public::FixtureFetcher::new(
            HashMap::new(),
        ));
        let identity = Arc::new(crate::asset_identity::AssetIdentityResolver::new(
            fetcher.clone(),
            "https://fixture.invalid".to_owned(),
            pe_source_polymarket_public::GAMMA_BATCH_SIZE,
            Arc::new(Mutex::new(
                crate::source_event_sink::SourceEventSink::open(dir.path().join("source.log"))
                    .unwrap(),
            )),
        ));
        let validator = CausalPositionValidator::new(
            fetcher,
            "https://fixture.invalid",
            "admission-test",
            identity,
        );
        let ranked = HashMap::from([(candidate, 1234)]);
        let preparer = AdmissionPreparer::with_validator(tx, state.clone(), validator);
        let day = Duration::from_secs(86_400);
        let deadline = Instant::now() + 2 * day;
        let additions = [candidate];
        let prepare = preparer.prepare_with_cursors(
            &additions,
            Some(&ranked),
            Some(deadline),
            AdmissionContext::Other,
        );
        // Pauses follow measured lock times, so the test moves the paused clock itself (tokio does not
        // auto-advance while this task keeps yielding). One day ends the drain-list read's pause
        // whatever it measured; the group's drain transaction then commits and preparation parks in
        // its pause (at least 50 ms) until the second day passes the deadline, so the gate result's
        // transaction never starts.
        let drive = async {
            tokio::time::advance(day).await;
            for _ in 0..1_000 {
                if !seen(&state, "left") {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(!seen(&state, "left"), "the group's transaction committed");
            tokio::time::advance(day).await;
        };
        let (outcome, ()) = tokio::join!(prepare, drive);
        let outcome = outcome.unwrap();
        assert_eq!(outcome.unstarted, vec![candidate]);
        assert!(outcome.started.is_empty() && outcome.deferred.is_empty());
        assert!(outcome.admitted.is_empty());
        assert!(rx.try_recv().is_err());
        assert_eq!(state.retirement_drains().unwrap().result, vec![candidate]);
        assert!(!seen(&state, "left"), "the group's transaction committed");
        assert!(
            seen(&state, "left-gate"),
            "the gate result's transaction never started"
        );
        assert!(state.cursor(&candidate).unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn preparation_without_listed_additions_takes_no_pause() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let state = Arc::new(PaperStateDb::open(&path).unwrap());
        let candidate = wallet(8);
        let other = wallet(9);
        let (tx, _rx) = mpsc::channel(1);
        let preparer = AdmissionPreparer::new(tx, state.clone());
        for listed in [false, true] {
            if listed {
                listed_with_leftovers(&path, &state, other);
            }
            let start = Instant::now();
            let result = preparer.prepare(&[candidate]).await.unwrap();
            assert_eq!(Instant::now(), start);
            assert_eq!(result.deferred[0].kind, "history.missing");
            assert!(result.admitted.is_empty());
            assert_eq!(
                state.retirement_drains().unwrap().result,
                if listed { vec![other] } else { Vec::new() }
            );
        }
        assert!(seen(&state, "left") && seen(&state, "left-gate"));
    }
}

impl AdmissionError {
    pub fn class(&self) -> FailureClass {
        match self {
            Self::MissingHistory { .. } | Self::Fenced { .. } => FailureClass::WalletPersistent,
            Self::PositionValidation(error) => error.class(),
            Self::ValidationRejected(error) => error.class(),
            Self::MembershipProof(error) => error.class(),
            Self::PublicationRejected(error) => error.class(),
            Self::ControlClosed
            | Self::AcknowledgementClosed
            | Self::AcknowledgementTimeout(_)
            | Self::ValidationInstall(_)
            | Self::PaperState(_)
            | Self::PositionValidatorUnavailable
            | Self::MembershipSourceLogUnavailable
            | Self::MembershipArtifactEncoding(_)
            | Self::CapacityTargetOverflow { .. }
            | Self::MembershipSourceLog(_) => FailureClass::Shared,
            Self::QueueKeyExhausted => FailureClass::Shared,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::MissingHistory { .. } => "history.missing",
            Self::Fenced { .. } => "fence.active",
            Self::PositionValidation(error) => error.kind(),
            Self::ValidationRejected(error) => error.kind(),
            Self::MembershipProof(error) => error.kind(),
            Self::PublicationRejected(error) => error.kind(),
            Self::ControlClosed => "control.closed",
            Self::AcknowledgementClosed => "ack.closed",
            Self::AcknowledgementTimeout(_) => "ack.timeout",
            Self::ValidationInstall(_) => "anchor.durability",
            Self::PaperState(_) => "paper_state.read",
            Self::PositionValidatorUnavailable => "validator.unavailable",
            Self::MembershipSourceLogUnavailable => "source_log.unavailable",
            Self::MembershipArtifactEncoding(_) => "artifact.encoding",
            Self::CapacityTargetOverflow { .. } => "capacity.overflow",
            Self::MembershipSourceLog(_) => "source_log.append",
            Self::QueueKeyExhausted => "queue.exhausted",
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Deferral {
    #[serde(skip)]
    pub(crate) completed_at: Option<Instant>,
    pub wallet: WalletAddress,
    pub stage: &'static str,
    pub class: FailureClass,
    pub kind: &'static str,
    pub message: String,
}

impl Deferral {
    fn from_error(wallet: WalletAddress, stage: &'static str, error: &AdmissionError) -> Self {
        Self {
            completed_at: Some(tokio::time::Instant::now()),
            wallet,
            stage,
            class: error.class(),
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct AdmissionOutcome {
    pub started: Vec<WalletAddress>,
    pub unstarted: Vec<WalletAddress>,
    pub admitted: Vec<WalletAddress>,
    pub deferred: Vec<Deferral>,
}

pub(crate) type CapturedProofs = Vec<(WalletAddress, MembershipProofManifest)>;

#[derive(Debug, thiserror::Error)]
#[error("{cause}")]
pub struct AdmissionAbort {
    pub started: Vec<WalletAddress>,
    pub unstarted: Vec<WalletAddress>,
    pub admitted: Vec<WalletAddress>,
    pub cause: AdmissionError,
    pub deferred: Vec<Deferral>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeferralContext {
    FullRerank { batch_id: i64 },
    Capacity { generation: u64, target: usize },
    Knockout { batch_id: i64 },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeferralOutcome {
    Published { paper_seq: u64 },
    NoChange,
    PendingCapacity,
    AbortedShared { kind: &'static str },
}

#[derive(Serialize)]
pub struct MembershipDeferralArtifact {
    version: u8,
    context: DeferralContext,
    deferrals: Vec<Deferral>,
    outcome: DeferralOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorRefreshOutcome {
    Anchored,
    Skipped,
    Deferred,
    Cancelled,
}

/// The poller may cancel validation before the install handoff. Once handoff starts, the
/// coordinator retains the wallet until the install acknowledgement has been observed.
#[derive(Clone, Default)]
pub(crate) struct RefreshHandoff(Arc<AtomicU8>);

impl RefreshHandoff {
    const VALIDATING: u8 = 0;
    const PRE_SEND: u8 = 1;
    const SENT: u8 = 2;
    const CANCELLED: u8 = 3;

    pub(crate) fn cancel_before_handoff(&self) -> bool {
        loop {
            let phase = self.0.load(Ordering::Acquire);
            if phase == Self::CANCELLED {
                return true;
            }
            if phase != Self::VALIDATING && phase != Self::PRE_SEND {
                return false;
            }
            if self
                .0
                .compare_exchange(phase, Self::CANCELLED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == Self::CANCELLED
    }

    fn pre_send(&self) -> bool {
        self.0
            .compare_exchange(
                Self::VALIDATING,
                Self::PRE_SEND,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn begin_handoff(&self) -> bool {
        self.0
            .compare_exchange(
                Self::PRE_SEND,
                Self::SENT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

/// The runtime predicate shared by periodic refresh and ordinary boot reuse.
#[must_use]
pub fn anchor_refresh_due(coverage: &WalletCoverage, now_unix: i64, refresh_secs: u64) -> bool {
    let refresh_secs = i64::try_from(refresh_secs).unwrap_or(i64::MAX);
    coverage.reanchor_required
        || coverage
            .anchored_at_unix
            .is_none_or(|anchored| now_unix.saturating_sub(anchored) > refresh_secs)
}

/// Shared serialized admission coordinator. Its durable checks are repeated by
/// the orchestrator while holding the publication lock.
#[derive(Clone)]
pub struct AdmissionPreparer {
    inner: Arc<Preparer>,
    source_log: Option<SourceLogHandle>,
}

struct Preparer {
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    attempt: Mutex<AdmissionQueue>,
    validator: Option<CausalPositionValidator>,
}

#[derive(Default)]
struct AdmissionQueue {
    next: u64,
    keys: HashMap<WalletAddress, u64>,
}

/// Keeps catch-up brackets serialized with departed-wallet deletion.
pub struct RetentionAdmissionGuard<'a> {
    _attempt: tokio::sync::MutexGuard<'a, AdmissionQueue>,
}

impl AdmissionQueue {
    fn offer(&mut self, wallet: WalletAddress) -> Result<(), AdmissionError> {
        if !self.keys.contains_key(&wallet) {
            self.move_back(wallet)?;
        }
        Ok(())
    }

    fn move_back(&mut self, wallet: WalletAddress) -> Result<(), AdmissionError> {
        let next = self
            .next
            .checked_add(1)
            .ok_or(AdmissionError::QueueKeyExhausted)?;
        self.keys.insert(wallet, self.next);
        self.next = next;
        Ok(())
    }
}

/// One preparation context; capacity work does not participate in tick alternation.
#[derive(Clone, Copy)]
pub(crate) enum AdmissionContext {
    Addition { first: bool },
    Reentry { first: bool },
    Capacity,
    Other,
}

/// Emit exactly once on every return, including lock timeout and shared failure.
struct AdmissionLaunchLog {
    context: AdmissionContext,
    eligible: Vec<String>,
    started_previous_keys: Vec<(String, u64)>,
}

impl AdmissionLaunchLog {
    fn started(
        &mut self,
        queue: &mut AdmissionQueue,
        wallet: WalletAddress,
    ) -> Result<(), AdmissionError> {
        if let Some(key) = queue.keys.get(&wallet) {
            self.started_previous_keys.push((wallet.to_string(), *key));
        }
        queue.move_back(wallet)
    }
}

impl Drop for AdmissionLaunchLog {
    fn drop(&mut self) {
        let (path, first) = match self.context {
            AdmissionContext::Addition { first } => ("addition", Some(first)),
            AdmissionContext::Reentry { first } => ("reentry", Some(first)),
            AdmissionContext::Capacity => ("capacity", None),
            AdmissionContext::Other => ("other", None),
        };
        info!(path, first, eligible = %serde_json::json!(self.eligible), started = self.started_previous_keys.len(),
            started_previous_keys = %serde_json::json!(self.started_previous_keys), "admission launch order");
    }
}

impl AdmissionPreparer {
    /// Acquire outside the orchestrator and hold through the retirement acknowledgement.
    pub async fn lock_for_retention(&self) -> RetentionAdmissionGuard<'_> {
        RetentionAdmissionGuard {
            _attempt: self.inner.attempt.lock().await,
        }
    }

    pub fn new(
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
    ) -> Self {
        Self {
            inner: Arc::new(Preparer {
                control_tx,
                paper_state,
                attempt: Mutex::new(AdmissionQueue::default()),
                validator: None,
            }),
            source_log: None,
        }
    }

    pub(crate) fn paper_state(&self) -> &PaperStateDb {
        &self.inner.paper_state
    }

    /// Production constructor with the five-step causal positions bracket.
    pub fn with_validator(
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
        validator: CausalPositionValidator,
    ) -> Self {
        Self {
            inner: Arc::new(Preparer {
                control_tx,
                paper_state,
                attempt: Mutex::new(AdmissionQueue::default()),
                validator: Some(validator),
            }),
            source_log: None,
        }
    }

    /// Install the process-wide synchronized source-log handle used to retain accepted
    /// capacity-change inputs before their structural membership publication.
    #[must_use]
    pub fn with_source_log(mut self, source_log: SourceLogHandle) -> Self {
        self.source_log = Some(source_log);
        self
    }

    /// Seed the ranking's last-trade cursor before the causal bracket captures its ledger hash.
    /// The validator needs the seeded cursor to stay unchanged through anchor installation.
    /// The validator-free branch first passes the durable history gate.
    fn seed_ranked_cursors(
        &self,
        wallets: &[WalletAddress],
        ranked_last_trade: Option<&HashMap<WalletAddress, i64>>,
    ) -> Result<(), AdmissionError> {
        if let Some(ranked_last_trade) = ranked_last_trade {
            let seeds = wallets
                .iter()
                .filter_map(|wallet| ranked_last_trade.get(wallet).map(|time| (*wallet, *time)))
                .collect::<Vec<_>>();
            self.inner.paper_state.seed_cursors_if_absent(&seeds)?;
        }
        Ok(())
    }

    pub(crate) async fn prepare_ranked_until(
        &self,
        additions: &[WalletAddress],
        ranked_last_trade: &HashMap<WalletAddress, i64>,
        deadline: Option<Instant>,
        context: AdmissionContext,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_with_cursors(additions, Some(ranked_last_trade), deadline, context)
            .await
    }

    #[cfg(feature = "scenario")]
    pub async fn scenario_prepare_until(
        &self,
        additions: &[WalletAddress],
        deadline: tokio::time::Instant,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_with_cursors(additions, None, Some(deadline), AdmissionContext::Other)
            .await
    }

    #[cfg(feature = "scenario")]
    pub async fn scenario_prepare_ranked_until(
        &self,
        additions: &[WalletAddress],
        ranked_last_trade: &HashMap<WalletAddress, i64>,
        deadline: Option<Instant>,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_ranked_until(
            additions,
            ranked_last_trade,
            deadline,
            AdmissionContext::Other,
        )
        .await
    }

    /// Exercise the runtime re-entry routing with the same ranked cursor seeding as maintenance.
    #[cfg(feature = "scenario")]
    pub async fn scenario_prepare_reentry(
        &self,
        wallets: &[WalletAddress],
        ranked_last_trade: &HashMap<WalletAddress, i64>,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_ranked_until(
            wallets,
            ranked_last_trade,
            None,
            AdmissionContext::Reentry { first: false },
        )
        .await
    }

    /// Reject fences before validation and require complete history after the accepted
    /// anchor installation is acknowledged. Validator-free callers require complete
    /// history before handing preparation to the orchestrator.
    pub async fn prepare(
        &self,
        additions: &[WalletAddress],
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_in(additions, AdmissionContext::Other).await
    }

    /// As [`Self::prepare`], logging the caller's launch-order path.
    pub(crate) async fn prepare_in(
        &self,
        additions: &[WalletAddress],
        context: AdmissionContext,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        self.prepare_with_cursors(additions, None, None, context)
            .await
    }

    async fn prepare_with_cursors(
        &self,
        additions: &[WalletAddress],
        ranked_last_trade: Option<&HashMap<WalletAddress, i64>>,
        deadline: Option<Instant>,
        context: AdmissionContext,
    ) -> Result<AdmissionOutcome, AdmissionAbort> {
        let preparer = &self.inner;
        let mut started = Vec::new();
        let mut unstarted = Vec::new();
        let mut launch = AdmissionLaunchLog {
            context,
            eligible: Vec::new(),
            started_previous_keys: Vec::new(),
        };
        let mut attempt = if let Some(end) = deadline {
            match tokio::time::timeout_at(end, preparer.attempt.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    return Ok(AdmissionOutcome {
                        started,
                        unstarted: additions.to_vec(),
                        admitted: Vec::new(),
                        deferred: Vec::new(),
                    });
                }
            }
        } else {
            preparer.attempt.lock().await
        };
        let mut admitted = Vec::new();
        let mut deferred = Vec::new();
        let mut eligible = Vec::new();
        // A wallet retention removed but has not drained is drained before anything reads it; one
        // the deadline interrupts stays listed and unstarted.
        let mut ready = Vec::with_capacity(additions.len());
        let past_deadline = || deadline.is_some_and(|end| Instant::now() >= end);
        let drained = async {
            let read = preparer.paper_state.retirement_drains()?;
            let listed = read.result;
            if additions.iter().any(|wallet| listed.contains(wallet)) {
                crate::database_retention::pace_drain_list_read(listed.len(), read.lock_time).await;
            }
            for wallet in additions {
                if listed.contains(wallet)
                    && crate::database_retention::drain_retired_wallet(
                        &preparer.paper_state,
                        *wallet,
                        &past_deadline,
                    )
                    .await?
                    .is_none()
                {
                    unstarted.push(*wallet);
                } else {
                    ready.push(*wallet);
                }
            }
            Ok::<(), pe_paper_state::PaperStateError>(())
        };
        if let Err(error) = drained.await {
            return Err(AdmissionAbort {
                started,
                unstarted,
                admitted,
                cause: AdmissionError::PaperState(error),
                deferred,
            });
        }
        for wallet in &ready {
            match self.check_recovery_fence(wallet) {
                Ok(()) => eligible.push(*wallet),
                Err(error) if error.class() != FailureClass::Shared => {
                    deferred.push(Deferral::from_error(*wallet, "fence", &error))
                }
                Err(cause) => {
                    return Err(AdmissionAbort {
                        started,
                        unstarted,
                        admitted,
                        cause,
                        deferred,
                    });
                }
            }
        }
        for wallet in &eligible {
            if let Err(cause) = attempt.offer(*wallet) {
                return Err(AdmissionAbort {
                    started,
                    unstarted,
                    admitted,
                    cause,
                    deferred,
                });
            }
        }
        // Stable sorting preserves caller rank order for equal keys.
        eligible.sort_by_key(|wallet| attempt.keys.get(wallet).copied());
        launch.eligible = eligible.iter().map(ToString::to_string).collect();
        if let Some(validator) = &preparer.validator {
            if let Err(cause) = self.seed_ranked_cursors(&eligible, ranked_last_trade) {
                return Err(AdmissionAbort {
                    started,
                    unstarted,
                    admitted,
                    cause,
                    deferred,
                });
            }
            let outcomes = validator
                .validate_via_control(
                    &eligible,
                    &preparer.control_tx,
                    &preparer.paper_state,
                    if matches!(context, AdmissionContext::Reentry { .. }) {
                        ValidationPurpose::Reentry
                    } else {
                        ValidationPurpose::CatchUp
                    },
                    deadline,
                )
                .await;
            started.extend_from_slice(&eligible[..outcomes.started_prefix]);
            unstarted.extend_from_slice(&eligible[outcomes.started_prefix..]);
            for wallet in &started {
                if let Err(cause) = launch.started(&mut attempt, *wallet) {
                    return Err(AdmissionAbort {
                        started,
                        unstarted,
                        admitted,
                        cause,
                        deferred,
                    });
                }
            }
            for (wallet, error) in outcomes.deferred {
                let error = AdmissionError::PositionValidation(error);
                let mut deferral = Deferral::from_error(wallet, "validation", &error);
                deferral.completed_at = outcomes.completed_at.get(&wallet).copied();
                deferred.push(deferral);
            }
            if let Some(cause) = outcomes.shared {
                return Err(AdmissionAbort {
                    started,
                    unstarted,
                    admitted,
                    cause: AdmissionError::PositionValidation(cause),
                    deferred,
                });
            }
            let mut remaining = outcomes.accepted;
            while !remaining.is_empty() {
                match self.install_anchors(remaining.clone()).await {
                    Ok(()) => {
                        admitted.extend(remaining.iter().map(|install| install.wallet));
                        break;
                    }
                    Err(error) if error.class() != FailureClass::Shared => {
                        let wallet = match &error {
                            AdmissionError::ValidationRejected(cause) => cause.wallet(),
                            _ => None,
                        };
                        let Some(wallet) = wallet else {
                            return Err(AdmissionAbort {
                                started,
                                unstarted,
                                admitted,
                                cause: error,
                                deferred,
                            });
                        };
                        let mut deferral = Deferral::from_error(wallet, "anchor_install", &error);
                        deferral.completed_at = Some(Instant::now());
                        deferred.push(deferral);
                        remaining.retain(|install| install.wallet != wallet);
                    }
                    Err(cause) => {
                        return Err(AdmissionAbort {
                            started,
                            unstarted,
                            admitted,
                            cause,
                            deferred,
                        });
                    }
                }
            }
        } else {
            for wallet in eligible {
                if deadline.is_some_and(|end| Instant::now() >= end) {
                    // A zero-start call still idempotently seeds ranked cursors.
                    if let Err(cause) = self.seed_ranked_cursors(&[wallet], ranked_last_trade) {
                        return Err(AdmissionAbort {
                            started,
                            unstarted,
                            admitted,
                            cause,
                            deferred,
                        });
                    }
                    unstarted.push(wallet);
                    continue;
                }
                started.push(wallet);
                if let Err(cause) = launch.started(&mut attempt, wallet) {
                    return Err(AdmissionAbort {
                        started,
                        unstarted,
                        admitted,
                        cause,
                        deferred,
                    });
                }
                match self.check_prerequisites(&[wallet]) {
                    Ok(()) => {}
                    Err(error) if error.class() != FailureClass::Shared => {
                        deferred.push(Deferral::from_error(wallet, "history", &error));
                        continue;
                    }
                    Err(cause) => {
                        return Err(AdmissionAbort {
                            started,
                            unstarted,
                            admitted,
                            cause,
                            deferred,
                        });
                    }
                }
                if let Err(cause) = self.seed_ranked_cursors(&[wallet], ranked_last_trade) {
                    return Err(AdmissionAbort {
                        started,
                        unstarted,
                        admitted,
                        cause,
                        deferred,
                    });
                }
                if let Err(cause) = self.prepare_locked(&[wallet]).await {
                    return Err(AdmissionAbort {
                        started,
                        unstarted,
                        admitted,
                        cause,
                        deferred,
                    });
                }
                admitted.push(wallet);
            }
        }
        let mut ready = Vec::new();
        for wallet in admitted.iter().copied() {
            match self.check_prerequisites(&[wallet]) {
                Ok(()) => ready.push(wallet),
                Err(error) if error.class() != FailureClass::Shared => {
                    deferred.push(Deferral::from_error(wallet, "history", &error))
                }
                Err(cause) => {
                    return Err(AdmissionAbort {
                        started,
                        unstarted,
                        admitted,
                        cause,
                        deferred,
                    });
                }
            }
        }
        Ok(AdmissionOutcome {
            started,
            unstarted,
            admitted: ready,
            deferred,
        })
    }

    /// Recheck, seed, synchronize one structural record, then publish its exact entries.
    /// Callers release the writer lock before sending or awaiting this control message.
    pub async fn publish_membership(
        &self,
        change: MembershipChange,
        replacements: Vec<WatchlistEntry>,
        checks: MembershipCommit,
    ) -> Result<AppendReceipt, crate::watchlist_maintenance::PublishError> {
        let (acknowledged, received) = oneshot::channel();
        self.inner
            .control_tx
            .send(OrchestratorControl::PublishMembership {
                change,
                replacements,
                checks: Box::new(checks),
                acknowledged,
            })
            .await
            .map_err(|_| {
                crate::watchlist_maintenance::PublishError::Shared(
                    "control channel closed".to_owned(),
                )
            })?;
        received.await.map_err(|_| {
            crate::watchlist_maintenance::PublishError::Shared(
                "publication acknowledgement closed".to_owned(),
            )
        })?
    }

    /// Durably retain the exact accepted capacity request before its membership record can
    /// reference the returned immutable source-log receipt.
    pub(crate) async fn record_capacity_config(
        &self,
        generation: u64,
        target: usize,
        published_entries: Vec<WatchlistEntry>,
    ) -> Result<AppendReceipt, AdmissionError> {
        let durable_target =
            u64::try_from(target).map_err(|_| AdmissionError::CapacityTargetOverflow { target })?;
        self.record_artifact(
            CAPACITY_CONFIG_SOURCE_ID,
            &CapacityMembershipArtifact {
                generation,
                target: durable_target,
                published_entries,
            },
        )
        .await
    }

    /// Retain the exact batch-pinned ranking rows used by a full-rerank, or the exact
    /// candidate rows used by a knockout backfill.
    pub(crate) async fn record_ranking_membership(
        &self,
        batch_id: Option<i64>,
        entries: Vec<WatchlistEntry>,
    ) -> Result<AppendReceipt, AdmissionError> {
        self.record_artifact(
            RANKING_MEMBERSHIP_SOURCE_ID,
            &RankingMembershipArtifact { batch_id, entries },
        )
        .await
    }

    pub(crate) fn capture_proofs(
        &self,
        additions: &[WalletAddress],
    ) -> Result<(CapturedProofs, Vec<Deferral>), Box<AdmissionAbort>> {
        let mut captured = Vec::with_capacity(additions.len());
        let mut deferred = Vec::new();
        for wallet in additions {
            match MembershipProofManifest::capture(&self.inner.paper_state, &[*wallet]) {
                Ok(proof) => captured.push((*wallet, proof)),
                Err(error) => {
                    let error = AdmissionError::MembershipProof(error);
                    if error.class() == FailureClass::Shared {
                        return Err(Box::new(AdmissionAbort {
                            started: Vec::new(),
                            unstarted: Vec::new(),
                            admitted: Vec::new(),
                            cause: error,
                            deferred,
                        }));
                    }
                    deferred.push(Deferral::from_error(*wallet, "proof", &error));
                }
            }
        }
        Ok((captured, deferred))
    }

    /// Append captured manifests without reading mutable paper state again.
    pub(crate) async fn record_admission_proofs(
        &self,
        captured: &[(WalletAddress, MembershipProofManifest)],
    ) -> Result<Vec<MembershipAdmissionReceipt>, AdmissionError> {
        let mut receipts = Vec::with_capacity(captured.len());
        for (wallet, proof) in captured {
            let receipt = self
                .record_artifact(
                    MEMBERSHIP_ADMISSION_SOURCE_ID,
                    &MembershipAdmissionArtifact {
                        wallet: *wallet,
                        proof: proof.clone(),
                    },
                )
                .await?;
            receipts.push(MembershipAdmissionReceipt {
                wallet: *wallet,
                receipt,
            });
        }
        Ok(receipts)
    }

    pub(crate) async fn record_deferrals(
        &self,
        context: DeferralContext,
        deferrals: Vec<Deferral>,
        outcome: DeferralOutcome,
    ) {
        if deferrals.is_empty() {
            return;
        }
        for deferral in &deferrals {
            warn!(wallet = %deferral.wallet, stage = deferral.stage, kind = deferral.kind, ?deferral.class, ?context, "membership admission deferred");
        }
        let artifact = MembershipDeferralArtifact {
            version: 1,
            context,
            deferrals,
            outcome,
        };
        if let Err(error) = self
            .record_artifact(MEMBERSHIP_DEFERRAL_SOURCE_ID, &artifact)
            .await
        {
            warn!(%error, "membership deferral audit append failed");
        }
    }

    /// Retain each knockout's policy and cursor inputs and return the receipt-bound evidence.
    pub(crate) async fn record_knockout_inputs(
        &self,
        inputs: Vec<(MembershipReason, KnockoutCausalArtifact)>,
    ) -> Result<Vec<SealedKnockoutEvidence>, AdmissionError> {
        let mut evidence = Vec::with_capacity(inputs.len());
        for (reason, input) in inputs {
            let wallet = input.wallet;
            let causal_receipt = self
                .record_artifact(KNOCKOUT_CAUSAL_SOURCE_ID, &input)
                .await?;
            evidence.push(SealedKnockoutEvidence {
                wallet,
                reason,
                causal_receipt,
            });
        }
        Ok(evidence)
    }

    async fn record_artifact<T: Serialize>(
        &self,
        source_id: &str,
        artifact: &T,
    ) -> Result<AppendReceipt, AdmissionError> {
        let source_log = self
            .source_log
            .as_ref()
            .ok_or(AdmissionError::MembershipSourceLogUnavailable)?;
        let payload =
            serde_json::to_vec(artifact).map_err(AdmissionError::MembershipArtifactEncoding)?;
        let now = time::OffsetDateTime::now_utc();
        source_log
            .append(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version: MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
                parser_version: MEMBERSHIP_ARTIFACT_PARSER_VERSION,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload,
            })
            .await
            .map_err(AdmissionError::from)
    }

    /// Re-anchor one due wallet under the shared bracket mutex.
    pub async fn prepare_if_due(
        &self,
        wallet: WalletAddress,
        now_unix: i64,
        refresh_secs: u64,
    ) -> Result<AnchorRefreshOutcome, AdmissionError> {
        self.prepare_if_due_observed(wallet, now_unix, refresh_secs, None)
            .await
    }

    pub(crate) async fn prepare_if_due_observed(
        &self,
        wallet: WalletAddress,
        now_unix: i64,
        refresh_secs: u64,
        handoff: Option<&RefreshHandoff>,
    ) -> Result<AnchorRefreshOutcome, AdmissionError> {
        let preparer = &self.inner;
        let Ok(_attempt) = preparer.attempt.try_lock() else {
            return Ok(AnchorRefreshOutcome::Skipped);
        };
        let coverage = preparer.paper_state.wallet_coverage(&wallet)?;
        if !anchor_refresh_due(&coverage, now_unix, refresh_secs) {
            return Ok(AnchorRefreshOutcome::Skipped);
        }
        match self.check_prerequisites(&[wallet]) {
            Ok(()) => {}
            Err(AdmissionError::Fenced { .. }) => {
                warn!(wallet = %wallet, "anchor refresh skipped because wallet is durably fenced");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            Err(error) => return Err(error),
        }
        let validator = preparer
            .validator
            .as_ref()
            .ok_or(AdmissionError::PositionValidatorUnavailable)?;
        let purpose = match coverage.activity_cutoff_unix {
            Some(cutoff)
                if !coverage.reanchor_required
                    && coverage.anchor_seq.is_some()
                    && preparer.paper_state.cursor(&wallet)?.is_some() =>
            {
                ValidationPurpose::RoutineRefresh { cutoff }
            }
            _ => ValidationPurpose::CatchUp,
        };
        let outcomes = validator
            .validate_via_control(
                &[wallet],
                &preparer.control_tx,
                &preparer.paper_state,
                purpose,
                None,
            )
            .await;
        if let Some(error) = outcomes.shared {
            // Periodic refresh retains its existing retry cadence for venue transport
            // failures. Admission attempts still abort on these shared errors.
            if is_deferred_causal_position_error(&error) {
                return Ok(AnchorRefreshOutcome::Deferred);
            }
            return Err(AdmissionError::PositionValidation(error));
        }
        if let Some((_, error)) = outcomes.deferred.into_iter().next() {
            if let CausalPositionError::Fenced { wallet } = error {
                warn!(wallet = %wallet, "anchor refresh skipped because causal validation fenced the wallet");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            if is_deferred_causal_position_error(&error) || error.class() != FailureClass::Shared {
                return Ok(AnchorRefreshOutcome::Deferred);
            }
            return Err(AdmissionError::PositionValidation(error));
        }
        if let Some(handoff) = handoff
            && !handoff.pre_send()
        {
            return Ok(AnchorRefreshOutcome::Cancelled);
        }
        let permit = self
            .inner
            .control_tx
            .reserve()
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
        if let Some(handoff) = handoff
            && !handoff.begin_handoff()
        {
            return Ok(AnchorRefreshOutcome::Cancelled);
        }
        let installed = Self::send_install_anchors(permit, outcomes.accepted).await;
        match installed {
            Ok(()) => Ok(AnchorRefreshOutcome::Anchored),
            Err(AdmissionError::ValidationRejected(_)) => Ok(AnchorRefreshOutcome::Deferred),
            Err(error) => Err(error),
        }
    }

    fn check_recovery_fence(&self, wallet: &WalletAddress) -> Result<(), AdmissionError> {
        if let Some(fence) = self.inner.paper_state.wallet_fence(wallet)?
            && !crate::position_seeder::recoverable_fence(&self.inner.paper_state, &fence)?
        {
            return Err(AdmissionError::Fenced { wallet: *wallet });
        }
        Ok(())
    }

    fn check_fences(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        for wallet in additions {
            if self.inner.paper_state.is_wallet_fenced(wallet)? {
                return Err(AdmissionError::Fenced { wallet: *wallet });
            }
        }
        Ok(())
    }

    fn check_prerequisites(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        self.check_fences(additions)?;
        let preparer = &self.inner;
        for wallet in additions {
            if !preparer.paper_state.wallet_history_complete(wallet)? {
                return Err(AdmissionError::MissingHistory { wallet: *wallet });
            }
        }
        Ok(())
    }

    async fn prepare_locked(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        let preparer = &self.inner;
        let (acknowledged, acknowledgement) = oneshot::channel();
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            async {
                preparer
                    .control_tx
                    .send(OrchestratorControl::PrepareAdmissions {
                        wallets: additions.to_vec(),
                        acknowledged,
                    })
                    .await
                    .map_err(|_| AdmissionError::ControlClosed)?;
                acknowledgement
                    .await
                    .map_err(|_| AdmissionError::AcknowledgementClosed)
            },
        )
        .await
        .map_err(|_| {
            AdmissionError::AcknowledgementTimeout(ADMISSION_PREPARE_ACK_TIMEOUT_SECS)
        })??;
        Ok(())
    }

    async fn install_anchors(&self, installs: Vec<AnchorInstall>) -> Result<(), AdmissionError> {
        let permit = self
            .inner
            .control_tx
            .reserve()
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
        Self::send_install_anchors(permit, installs).await
    }

    async fn send_install_anchors(
        permit: mpsc::Permit<'_, OrchestratorControl>,
        installs: Vec<AnchorInstall>,
    ) -> Result<(), AdmissionError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        permit.send(OrchestratorControl::InstallAnchors {
            installs,
            acknowledged,
        });
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            acknowledgement,
        )
        .await
        .map_err(|_| AdmissionError::AcknowledgementTimeout(ADMISSION_PREPARE_ACK_TIMEOUT_SECS))?
        .map_err(|_| AdmissionError::AcknowledgementClosed)?
        .map_err(|error| match error {
            AnchorInstallError::Durability(message) => AdmissionError::ValidationInstall(message),
            rejection => AdmissionError::ValidationRejected(rejection),
        })
    }
    /// Exercise production artifact capture and locked publication from scenario harnesses.
    #[cfg(feature = "scenario")]
    pub async fn scenario_publish_ranking(
        &self,
        live: &crate::live_watchlist::LiveWatchlist,
        writer_lock: &Mutex<()>,
        entries: Vec<WatchlistEntry>,
        last_trade: &std::collections::HashMap<WalletAddress, i64>,
        cap: usize,
    ) -> Result<(), crate::watchlist_maintenance::MembershipApplyError> {
        use crate::paper_recovery::SealedMembershipEvidence;
        use crate::watchlist_maintenance::{
            MembershipApplyError, MembershipPublication, PublicationBinding, PublishError,
            apply_ranked_membership_locked, ranked_membership_change_set,
        };
        let (_, additions) =
            ranked_membership_change_set(&live.structural_membership(), &entries, cap);
        let ranking = self
            .record_ranking_membership(Some(546), entries.clone())
            .await
            .map_err(|error| {
                MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
            })?;
        let (proofs, deferred) = self.capture_proofs(&additions).map_err(|error| {
            MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
        })?;
        if !deferred.is_empty() {
            return Err(MembershipApplyError::Publication(PublishError::Shared(
                "scenario admissions deferred".to_owned(),
            )));
        }
        let binding = PublicationBinding::capture(live.structural_membership(), &proofs).map_err(
            |error| MembershipApplyError::Publication(PublishError::Shared(error.to_string())),
        )?;
        let admissions = self
            .record_admission_proofs(&proofs)
            .await
            .map_err(|error| {
                MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
            })?;
        let evidence =
            SealedMembershipEvidence::full_rerank(ranking, admissions).map_err(|error| {
                MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
            })?;
        let _writer = writer_lock.lock().await;
        apply_ranked_membership_locked(
            live,
            &self.inner.paper_state,
            self,
            MembershipPublication {
                reason: MembershipReason::FullRerank,
                ranking_batch_id: Some(546),
                evidence,
                binding,
            },
            &entries,
            last_trade,
            cap,
            _writer,
            None,
            &[],
        )
        .await?;
        Ok(())
    }
}
