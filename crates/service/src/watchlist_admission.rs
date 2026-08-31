//! Serialized pre-publication admission preparation for runtime watchlist additions (#542).
//!
//! A wallet becomes copy-visible the moment it appears in [`crate::live_watchlist`]. Two
//! single-owner ledgers must already describe it by then:
//!
//! * the copy-entry gate's prior-market history — an absent wallet is fail-open by default
//!   (`entry_gate_fail_closed`, `docs/_GLOSSARY.md`), so a true re-entry reads as a first entry;
//! * the leader position ledger — an absent snapshot defaults the market position to zero, so an
//!   Add reads as an Entry (`copy-signal-engine::classifier`).
//!
//! Boot loads both before the trade producers start. This preparer is the single runtime owner of
//! the same two loads for every post-boot addition set, shared by
//! [`crate::watchlist_capacity`] and [`crate::watchlist_maintenance`] so both publish only
//! prepared wallets.
//!
//! ## Serialization and ordering
//!
//! One preparer-wide attempt mutex is held from the history load through the orchestrator's
//! acknowledgement. It gives three properties that the ledgers themselves do not:
//!
//! * [`pe_position_ledger::PositionLedger::overlay`] replaces a wallet's snapshot wholesale with
//!   no observation-time guard, so two concurrent attempts for one wallet could otherwise apply
//!   the older snapshot last. Serialized attempts fetch in the same order they send, and the
//!   orchestrator's control channel is FIFO, so among admission attempts the newest observation
//!   is applied last. (The periodic `PositionReseed` keeps its existing last-arrival-wins
//!   behaviour and is outside this ordering by design.)
//! * [`crate::wallet_history::WalletHistoryLoader`] persists its sidecar through one fixed
//!   `.json.tmp` path; serializing the attempt serializes that read-modify-write without a
//!   second lock.
//! * The structural membership writer lock is never held across this network work.
//!
//! Cancellation is safe at every point. The capacity worker drops a superseded `apply` future
//! (`config_poller::run_capacity_worker`): before the control send nothing was published and the
//! attempt is simply released; after it, the message is already queued and the single-owner
//! orchestrator still applies it ahead of any later attempt's message, because a later attempt
//! could not have started fetching until this one released the mutex.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_source_polymarket_public::ReqwestFetcher;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::orchestrator_control::OrchestratorControl;
use crate::position_seeder::seed_all;
use crate::wallet_history::WalletHistoryLoader;

/// Maximum time to wait for the orchestrator to apply pre-admission history and positions.
const ADMISSION_PREPARE_ACK_TIMEOUT_SECS: u64 = 30;

/// Failure surface for one admission-preparation attempt. The caller must not publish the
/// attempted additions. A load failure returns before anything is sent; a control failure after
/// the send may still see the orchestrator apply the queued maps, which is harmless because the
/// wallets stay unpublished.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("market history unavailable for {missing} newly admitted wallet(s)")]
    MissingHistory { missing: usize },
    #[error("current positions unavailable for {missing} newly admitted wallet(s)")]
    MissingPositions { missing: usize },
    #[error("orchestrator control channel closed before admission preparation")]
    ControlClosed,
    #[error("orchestrator admission preparation acknowledgement closed")]
    AcknowledgementClosed,
    #[error("orchestrator admission preparation exceeded {0} seconds")]
    AcknowledgementTimeout(u64),
}

/// Shared runtime-admission preparer. Cloning shares the attempt mutex, the request fetcher, and
/// the orchestrator control sender, so every caller joins one serialized admission order.
#[derive(Clone)]
pub struct AdmissionPreparer {
    inner: Arc<Preparer>,
}

struct Preparer {
    control_tx: mpsc::Sender<OrchestratorControl>,
    /// One fetcher for both loaders keeps all admission traffic behind a single rate gate.
    fetcher: ReqwestFetcher,
    attempt: Mutex<()>,
    polymarket_base_url: String,
    wallet_history_path: PathBuf,
    position_page_limit: u32,
    position_size_threshold: u32,
}

impl AdmissionPreparer {
    pub fn new(
        control_tx: mpsc::Sender<OrchestratorControl>,
        client: reqwest::Client,
        polymarket_base_url: String,
        wallet_history_path: PathBuf,
        position_page_limit: u32,
        position_size_threshold: u32,
    ) -> Self {
        Self {
            inner: Arc::new(Preparer {
                control_tx,
                fetcher: ReqwestFetcher::new(client),
                attempt: Mutex::new(()),
                polymarket_base_url,
                wallet_history_path,
                position_page_limit,
                position_size_threshold,
            }),
        }
    }

    /// Load and apply the admission prerequisites for exactly `additions`, returning only after
    /// the orchestrator has applied both maps.
    ///
    /// An empty set is a no-op and issues no requests, so eviction-only maintenance ticks and
    /// no-op reranks stay silent on the Polymarket API. A history failure returns before any
    /// position request is made.
    pub async fn prepare(&self, additions: &[WalletAddress]) -> Result<(), AdmissionError> {
        if additions.is_empty() {
            return Ok(());
        }
        let preparer = &self.inner;
        let _attempt = preparer.attempt.lock().await;

        // The loader returns the merged sidecar, which covers every wallet ever prepared; keep
        // only this attempt's additions so the control message applies exactly what it prepared.
        let mut history = WalletHistoryLoader::load(
            additions,
            &preparer.polymarket_base_url,
            &preparer.wallet_history_path,
            &preparer.fetcher,
        )
        .await;
        let missing = additions
            .iter()
            .filter(|wallet| !history.contains_key(wallet))
            .count();
        if missing > 0 {
            return Err(AdmissionError::MissingHistory { missing });
        }
        let wanted: HashSet<WalletAddress> = additions.iter().copied().collect();
        history.retain(|wallet, _| wanted.contains(wallet));

        let positions = seed_all(
            additions,
            &preparer.polymarket_base_url,
            preparer.position_page_limit,
            preparer.position_size_threshold,
            &preparer.fetcher,
        )
        .await;
        let missing = additions
            .iter()
            .filter(|wallet| !positions.contains_key(wallet))
            .count();
        if missing > 0 {
            return Err(AdmissionError::MissingPositions { missing });
        }

        let (acknowledged, acknowledgement) = oneshot::channel();
        let command = OrchestratorControl::PrepareAdmissions {
            history,
            positions,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::Query;
    use axum::{Json, Router, routing::get};
    use tempfile::TempDir;

    use super::*;

    /// Records the order in which the fake Polymarket endpoints were hit, so a test can prove
    /// that a second attempt did not start fetching while the first held the attempt.
    #[derive(Default)]
    struct Calls {
        order: StdMutex<Vec<String>>,
        positions: AtomicUsize,
    }

    fn wallet(byte: u8) -> WalletAddress {
        WalletAddress([byte; 20])
    }

    /// Serve `/activity` and `/positions` for every wallet, recording each hit. `history_status`
    /// of `false` makes `/activity` fail so the history stage cannot complete.
    async fn serve(calls: Arc<Calls>, history_ok: bool) -> String {
        let activity_calls = Arc::clone(&calls);
        let position_calls = Arc::clone(&calls);
        let app = Router::new()
            .route(
                "/activity",
                get(move |Query(q): Query<HashMap<String, String>>| {
                    let calls = Arc::clone(&activity_calls);
                    async move {
                        let user = q.get("user").cloned().unwrap_or_default();
                        calls
                            .order
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(format!("activity:{user}"));
                        if history_ok {
                            Ok(Json(Vec::<serde_json::Value>::new()))
                        } else {
                            Err(axum::http::StatusCode::NOT_FOUND)
                        }
                    }
                }),
            )
            .route(
                "/positions",
                get(move |Query(q): Query<HashMap<String, String>>| {
                    let calls = Arc::clone(&position_calls);
                    async move {
                        let user = q.get("user").cloned().unwrap_or_default();
                        calls
                            .order
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(format!("positions:{user}"));
                        calls.positions.fetch_add(1, Ordering::SeqCst);
                        Json(Vec::<serde_json::Value>::new())
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        std::mem::drop(tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        }));
        format!("http://{address}")
    }

    fn preparer(
        base_url: String,
        temp: &TempDir,
        control_tx: mpsc::Sender<OrchestratorControl>,
    ) -> AdmissionPreparer {
        AdmissionPreparer::new(
            control_tx,
            reqwest::Client::new(),
            base_url,
            temp.path().join("history.json"),
            500,
            1,
        )
    }

    fn prepared_wallets(message: &OrchestratorControl) -> Vec<WalletAddress> {
        match message {
            OrchestratorControl::PrepareAdmissions {
                history, positions, ..
            } => {
                let mut wallets: Vec<WalletAddress> = positions.keys().copied().collect();
                assert_eq!(
                    history.keys().copied().collect::<HashSet<_>>(),
                    wallets.iter().copied().collect::<HashSet<_>>(),
                    "history and positions must cover exactly the same wallets"
                );
                wallets.sort_unstable_by_key(|w| w.0);
                wallets
            }
            OrchestratorControl::PositionReseed(_) => panic!("expected an admission preparation"),
        }
    }

    fn acknowledge(message: OrchestratorControl) {
        match message {
            OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                acknowledged.send(()).unwrap();
            }
            OrchestratorControl::PositionReseed(_) => panic!("expected an admission preparation"),
        }
    }

    #[tokio::test]
    async fn empty_additions_issue_no_requests() {
        let calls = Arc::new(Calls::default());
        let base_url = serve(Arc::clone(&calls), true).await;
        let temp = TempDir::new().unwrap();
        let (control_tx, mut control_rx) = mpsc::channel(1);
        preparer(base_url, &temp, control_tx)
            .prepare(&[])
            .await
            .unwrap();
        assert!(
            calls
                .order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        assert!(control_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn history_failure_skips_every_position_request() {
        let calls = Arc::new(Calls::default());
        let base_url = serve(Arc::clone(&calls), false).await;
        let temp = TempDir::new().unwrap();
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let error = preparer(base_url, &temp, control_tx)
            .prepare(&[wallet(1)])
            .await
            .expect_err("history must fail closed");
        assert!(matches!(
            error,
            AdmissionError::MissingHistory { missing: 1 }
        ));
        assert_eq!(calls.positions.load(Ordering::SeqCst), 0);
        assert!(control_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn concurrent_attempts_are_serialized_through_acknowledgement() {
        let calls = Arc::new(Calls::default());
        let base_url = serve(Arc::clone(&calls), true).await;
        let temp = TempDir::new().unwrap();
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let shared = preparer(base_url, &temp, control_tx);

        let first = shared.clone();
        let first_task = tokio::spawn(async move { first.prepare(&[wallet(1)]).await });
        let first_message = control_rx.recv().await.unwrap();
        assert_eq!(prepared_wallets(&first_message), vec![wallet(1)]);

        // The second attempt starts only now, while the first still holds the attempt awaiting
        // its acknowledgement. It must not fetch or send until the first completes. The barrier
        // proves the second task ran up to its first pending await — the contested mutex.
        let second = shared.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let second_task = {
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                started.notify_one();
                second.prepare(&[wallet(2)]).await
            })
        };
        started.notified().await;
        tokio::task::yield_now().await;
        assert!(
            control_rx.try_recv().is_err(),
            "the second attempt sent while the first held the attempt"
        );
        assert_eq!(
            calls.positions.load(Ordering::SeqCst),
            1,
            "the second attempt fetched positions while the first held the attempt"
        );

        acknowledge(first_message);
        first_task.await.unwrap().unwrap();
        let second_message = control_rx.recv().await.unwrap();
        assert_eq!(prepared_wallets(&second_message), vec![wallet(2)]);
        acknowledge(second_message);
        second_task.await.unwrap().unwrap();

        let order = calls
            .order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let first_hex = wallet(1).to_string();
        let second_hex = wallet(2).to_string();
        let last_first = order
            .iter()
            .rposition(|call| call.ends_with(&first_hex))
            .unwrap();
        let first_second = order
            .iter()
            .position(|call| call.ends_with(&second_hex))
            .unwrap();
        assert!(
            last_first < first_second,
            "attempts interleaved their fetches: {order:?}"
        );
    }

    #[tokio::test]
    async fn cancelled_attempt_keeps_its_queued_control_ahead_of_the_next() {
        // The capacity worker drops a superseded apply future. A message already handed to the
        // control channel stays queued, and the next attempt's message is queued behind it, so
        // the orchestrator applies the older observation first and the newer one last.
        let calls = Arc::new(Calls::default());
        let base_url = serve(Arc::clone(&calls), true).await;
        let temp = TempDir::new().unwrap();
        let (control_tx, mut control_rx) = mpsc::channel(4);
        let shared = preparer(base_url, &temp, control_tx);

        let first = shared.clone();
        let first_task = tokio::spawn(async move { first.prepare(&[wallet(1)]).await });
        // Leave the first message in the queue; wait only for it to have been sent.
        while control_rx.is_empty() {
            tokio::task::yield_now().await;
        }
        first_task.abort();
        assert!(first_task.await.unwrap_err().is_cancelled());

        // The cancelled attempt released the mutex, so the next one proceeds and queues behind.
        let second = shared.clone();
        let second_task = tokio::spawn(async move { second.prepare(&[wallet(2)]).await });
        while control_rx.len() < 2 {
            tokio::task::yield_now().await;
        }
        let first_message = control_rx.recv().await.unwrap();
        let second_message = control_rx.recv().await.unwrap();
        assert_eq!(prepared_wallets(&first_message), vec![wallet(1)]);
        assert_eq!(prepared_wallets(&second_message), vec![wallet(2)]);
        acknowledge(second_message);
        second_task.await.unwrap().unwrap();
    }
}
