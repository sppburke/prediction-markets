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

        let (acknowledged, acknowledgement) = oneshot::channel();
        let command = OrchestratorControl::PrepareAdmissions {
            wallets: additions.to_vec(),
            acknowledged,
        };
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            async {
                preparer
                    .control_tx
                    .send(command)
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
}
