//! Durable pre-publication admission checks for runtime watchlist additions (#544).
//!
//! Lane C owns the history/fence boundary here. Lane D extends the marked seam
//! with the causal positions bracket; no sidecar or positions overlay is accepted.

use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_paper_state::{PaperStateDb, WalletCoverage};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::bucket_commit::AnchorInstallError;
use crate::orchestrator_control::OrchestratorControl;
use crate::position_seeder::{
    AnchorInstall, CausalPositionError, CausalPositionValidator, is_deferred_causal_position_error,
};

const ADMISSION_PREPARE_ACK_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("reconciled market history unavailable for {missing} newly admitted wallet(s)")]
    MissingHistory { missing: usize },
    #[error("{fenced} newly admitted wallet(s) are durably fenced")]
    Fenced { fenced: usize },
    #[error("orchestrator control channel closed before admission preparation")]
    ControlClosed,
    #[error("orchestrator admission preparation acknowledgement closed")]
    AcknowledgementClosed,
    #[error("orchestrator admission preparation exceeded {0} seconds")]
    AcknowledgementTimeout(u64),
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorRefreshOutcome {
    Anchored,
    Skipped,
    Deferred,
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
/// the membership writer while holding the publication lock.
#[derive(Clone)]
pub struct AdmissionPreparer {
    inner: Arc<Preparer>,
}

struct Preparer {
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    attempt: Mutex<()>,
    validator: Option<CausalPositionValidator>,
}

impl AdmissionPreparer {
    pub fn new(
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
    ) -> Self {
        Self {
            inner: Arc::new(Preparer {
                control_tx,
                paper_state,
                attempt: Mutex::new(()),
                validator: None,
            }),
        }
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
                attempt: Mutex::new(()),
                validator: Some(validator),
            }),
        }
    }

    /// Prove Lane C's durable prerequisites, then hand ownership to the
    /// orchestrator. `#544 Lane D integration`: complete the causal positions
    /// bracket before sending this acknowledgement-bearing command.
    pub async fn prepare(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        if additions.is_empty() {
            return Ok(());
        }
        let preparer = &self.inner;
        let _attempt = preparer.attempt.lock().await;

        self.check_prerequisites(additions)?;
        self.prepare_locked(additions).await
    }

    /// Re-anchor one due wallet under the shared bracket mutex.
    pub async fn prepare_if_due(
        &self,
        wallet: WalletAddress,
        now_unix: i64,
        refresh_secs: u64,
    ) -> Result<AnchorRefreshOutcome, AdmissionError> {
        let preparer = &self.inner;
        let _attempt = preparer.attempt.lock().await;
        let coverage = preparer.paper_state.wallet_coverage(&wallet)?;
        if !anchor_refresh_due(&coverage, now_unix, refresh_secs) {
            return Ok(AnchorRefreshOutcome::Skipped);
        }
        match self.check_prerequisites(&[wallet]) {
            Ok(()) => {}
            Err(AdmissionError::Fenced { fenced }) => {
                warn!(wallet = %wallet, fenced, "anchor refresh skipped because wallet is durably fenced");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            Err(error) => return Err(error),
        }
        let validator = preparer
            .validator
            .as_ref()
            .ok_or(AdmissionError::PositionValidatorUnavailable)?;
        let installs = match validator
            .validate_via_control(&[wallet], &preparer.control_tx, &preparer.paper_state)
            .await
        {
            Ok(installs) => installs,
            Err(error) if is_deferred_causal_position_error(&error) => {
                return Ok(AnchorRefreshOutcome::Deferred);
            }
            Err(CausalPositionError::Fenced { wallet }) => {
                warn!(wallet = %wallet, "anchor refresh skipped because causal validation fenced the wallet");
                return Ok(AnchorRefreshOutcome::Skipped);
            }
            Err(error) => return Err(AdmissionError::PositionValidation(error)),
        };
        match self.install_anchors(installs).await {
            Ok(()) => Ok(AnchorRefreshOutcome::Anchored),
            Err(AdmissionError::ValidationRejected(_)) => Ok(AnchorRefreshOutcome::Deferred),
            Err(error) => Err(error),
        }
    }

    fn check_prerequisites(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        let preparer = &self.inner;

        let mut fenced = 0usize;
        let mut missing = 0usize;
        for wallet in additions {
            if preparer.paper_state.is_wallet_fenced(wallet)? {
                fenced = fenced.saturating_add(1);
            }
            if !preparer.paper_state.wallet_history_complete(wallet)? {
                missing = missing.saturating_add(1);
            }
        }
        if fenced > 0 {
            return Err(AdmissionError::Fenced { fenced });
        }
        if missing > 0 {
            return Err(AdmissionError::MissingHistory { missing });
        }
        Ok(())
    }

    async fn prepare_locked(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        let preparer = &self.inner;
        if let Some(validator) = &preparer.validator {
            let installs = validator
                .validate_via_control(additions, &preparer.control_tx, &preparer.paper_state)
                .await
                .map_err(AdmissionError::PositionValidation)?;
            return self.install_anchors(installs).await;
        }
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
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.inner
            .control_tx
            .send(OrchestratorControl::InstallAnchors {
                installs,
                acknowledged,
            })
            .await
            .map_err(|_| AdmissionError::ControlClosed)?;
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
}
