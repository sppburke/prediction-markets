//! Durable pre-publication admission checks for runtime watchlist additions (#544).
//!
//! Lane C owns the history/fence boundary here. Lane D extends the marked seam
//! with the causal positions bracket; no sidecar or positions overlay is accepted.

use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::orchestrator_control::OrchestratorControl;
use crate::position_seeder::CausalPositionValidator;

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
    PositionValidation(String),
    #[error("orchestrator rejected the accepted position brackets: {0}")]
    ValidationInstall(String),
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

        let mut fenced = 0usize;
        let mut missing = 0usize;
        for wallet in additions {
            if preparer
                .paper_state
                .is_wallet_fenced(wallet)
                .unwrap_or(true)
            {
                fenced = fenced.saturating_add(1);
            }
            if !preparer
                .paper_state
                .wallet_history_complete(wallet)
                .unwrap_or(false)
            {
                missing = missing.saturating_add(1);
            }
        }
        if fenced > 0 {
            return Err(AdmissionError::Fenced { fenced });
        }
        if missing > 0 {
            return Err(AdmissionError::MissingHistory { missing });
        }

        let command = if let Some(validator) = &preparer.validator {
            let accepted = validator
                .validate_via_control(additions, &preparer.control_tx)
                .await
                .map_err(|error| AdmissionError::PositionValidation(error.to_string()))?;
            let validations = accepted
                .into_iter()
                .map(|acceptance| acceptance.validation)
                .collect();
            let (acknowledged, acknowledgement) = oneshot::channel();
            preparer
                .control_tx
                .send(OrchestratorControl::PrepareValidatedAdmissions {
                    validations,
                    acknowledged,
                })
                .await
                .map_err(|_| AdmissionError::ControlClosed)?;
            return tokio::time::timeout(
                Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
                acknowledgement,
            )
            .await
            .map_err(|_| {
                AdmissionError::AcknowledgementTimeout(ADMISSION_PREPARE_ACK_TIMEOUT_SECS)
            })?
            .map_err(|_| AdmissionError::AcknowledgementClosed)?
            .map_err(AdmissionError::ValidationInstall);
        } else {
            let (acknowledged, acknowledgement) = oneshot::channel();
            (
                OrchestratorControl::PrepareAdmissions {
                    wallets: additions.to_vec(),
                    acknowledged,
                },
                acknowledgement,
            )
        };
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            async {
                preparer
                    .control_tx
                    .send(command.0)
                    .await
                    .map_err(|_| AdmissionError::ControlClosed)?;
                command
                    .1
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
}
