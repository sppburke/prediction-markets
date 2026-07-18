//! Single-owner campaign actor with a capacity-one normal mailbox and priority kill latch.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pe_core_types::{RawEvidence, RawHttpResponse, RawTransportFailure, TransportErrorClass};
use pe_venue_polymarket::{PostOnceResult, PreparedSubmission, V2BuyRequest};
use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};

use crate::canary::{
    CAMPAIGN_MAX_ALLOWANCE, CAMPAIGN_START_COLLATERAL, CampaignAuthorization, CanaryAdmission,
    CanaryCampaignState, CanaryEvent, CanaryJournal, CanaryReconciliation, CanaryStateError,
    ClosureReason, CommandReceipt, OrganicStageAuthorization, PendingAttempt, raw_evidence_hash,
};

pub const CANARY_COMMAND_QUEUE_CAPACITY: usize = 1;
pub const CANARY_POST_TIMEOUT_SECS: u64 = 10;
pub const CANARY_RECONCILIATION_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawReconciliation {
    pub evidence: Vec<RawEvidence>,
    pub protocol_failure: Option<String>,
}

pub trait CanarySubmitter: Send + Sync + 'static {
    fn prepare_buy(
        &self,
        request: V2BuyRequest,
    ) -> Pin<Box<dyn Future<Output = Result<PreparedSubmission, String>> + Send + '_>>;

    fn post_order_once(
        &self,
        submission: PreparedSubmission,
    ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + '_>>;
    fn cancel_order_once(
        &self,
        order_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + '_>>;
    fn parse_post_response(&self, response: &RawHttpResponse) -> Result<PostOnceResult, String>;
}

pub trait CanaryReconciler: Send + Sync + 'static {
    fn reconcile_raw(
        &self,
        tracked_conditions: Vec<pe_core_types::PolymarketConditionId>,
        pending_order_hash: Option<String>,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = Result<RawReconciliation, String>> + Send + '_>>;

    fn parse_reconciliation(
        &self,
        raw: &RawReconciliation,
        pending: Option<&PendingAttempt>,
        known_trade_ids: &[String],
    ) -> Result<CanaryReconciliation, String>;
}

#[derive(Debug, thiserror::Error)]
pub enum CanaryActorError {
    #[error("canary actor is not running")]
    NotRunning,
    #[error("campaign state error: {0}")]
    State(#[from] CanaryStateError),
    #[error("canary admission is killed")]
    Killed,
    #[error("canary command mailbox is saturated")]
    Busy,
    #[error("venue POST failed or timed out: {0}")]
    Post(String),
}

enum Command {
    Arm {
        receipt: CommandReceipt,
        authorization: Box<CampaignAuthorization>,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
    ReviewProbe {
        receipt: CommandReceipt,
        campaign_id: String,
        ordinal: u8,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
    ArmOrganic {
        receipt: CommandReceipt,
        authorization: OrganicStageAuthorization,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
    Dispatch {
        receipt: CommandReceipt,
        admission: Box<CanaryAdmission>,
        request: V2BuyRequest,
        evidence: Vec<RawEvidence>,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
    Skip {
        receipt: CommandReceipt,
        identity: String,
        reason: String,
        evidence: Vec<RawEvidence>,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
    Reconcile {
        receipt: Option<CommandReceipt>,
        context: String,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
    Status {
        reply: oneshot::Sender<CanaryCampaignState>,
    },
    Shutdown {
        work_deadline: Option<tokio::time::Instant>,
        reply: oneshot::Sender<Result<CanaryCampaignState, CanaryActorError>>,
    },
}

#[derive(Clone)]
pub struct CanaryActorHandle {
    normal: mpsc::Sender<Command>,
    dispatch_gate: Arc<Mutex<()>>,
    killed: Arc<AtomicBool>,
    final_reconciliation_requested: Arc<AtomicBool>,
    kill_wakeup: Arc<Notify>,
    kill_request: Arc<Mutex<Option<KillRequest>>>,
}

struct KillRequest {
    receipt: CommandReceipt,
    work_deadline: Option<tokio::time::Instant>,
    reply: oneshot::Sender<Result<(), CanaryActorError>>,
}

impl CanaryActorHandle {
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T, CanaryActorError> {
        let (reply, response) = oneshot::channel();
        self.normal
            .try_send(command(reply))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => CanaryActorError::Busy,
                mpsc::error::TrySendError::Closed(_) => CanaryActorError::NotRunning,
            })?;
        response.await.map_err(|_| CanaryActorError::NotRunning)
    }

    pub async fn arm(
        &self,
        receipt: CommandReceipt,
        authorization: CampaignAuthorization,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::Arm {
            receipt,
            authorization: Box::new(authorization),
            reply,
        })
        .await?
    }

    pub async fn review_probe(
        &self,
        receipt: CommandReceipt,
        campaign_id: String,
        ordinal: u8,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::ReviewProbe {
            receipt,
            campaign_id,
            ordinal,
            reply,
        })
        .await?
    }

    pub async fn arm_organic(
        &self,
        receipt: CommandReceipt,
        authorization: OrganicStageAuthorization,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::ArmOrganic {
            receipt,
            authorization,
            reply,
        })
        .await?
    }

    pub async fn dispatch(
        &self,
        receipt: CommandReceipt,
        admission: CanaryAdmission,
        request: V2BuyRequest,
        evidence: Vec<RawEvidence>,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::Dispatch {
            receipt,
            admission: Box::new(admission),
            request,
            evidence,
            reply,
        })
        .await?
    }

    pub async fn reconcile(
        &self,
        receipt: Option<CommandReceipt>,
        context: String,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::Reconcile {
            receipt,
            context,
            reply,
        })
        .await?
    }

    pub async fn record_skip(
        &self,
        receipt: CommandReceipt,
        identity: String,
        reason: String,
        evidence: Vec<RawEvidence>,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::Skip {
            receipt,
            identity,
            reason,
            evidence,
            reply,
        })
        .await?
    }

    pub async fn status(&self) -> Result<CanaryCampaignState, CanaryActorError> {
        self.request(|reply| Command::Status { reply }).await
    }

    /// Priority kill: latch admission while holding the same gate as dispatch, wake the actor,
    /// and acknowledge only after the actor syncs the closure event.
    pub async fn kill(&self, receipt: CommandReceipt) -> Result<(), CanaryActorError> {
        self.kill_until(receipt, None).await
    }

    pub async fn kill_before(
        &self,
        receipt: CommandReceipt,
        work_deadline: tokio::time::Instant,
    ) -> Result<(), CanaryActorError> {
        self.kill_until(receipt, Some(work_deadline)).await
    }

    async fn kill_until(
        &self,
        receipt: CommandReceipt,
        work_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), CanaryActorError> {
        let (reply, response) = oneshot::channel();
        {
            let _gate = self.dispatch_gate.lock().await;
            let mut request = self.kill_request.lock().await;
            if request.is_some() {
                return Err(CanaryActorError::Busy);
            }
            *request = Some(KillRequest {
                receipt,
                work_deadline,
                reply,
            });
            self.killed.store(true, Ordering::SeqCst);
            self.final_reconciliation_requested
                .store(true, Ordering::SeqCst);
        }
        self.kill_wakeup.notify_one();
        response.await.map_err(|_| CanaryActorError::NotRunning)?
    }

    pub async fn shutdown_before(
        &self,
        work_deadline: tokio::time::Instant,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        let (reply, response) = oneshot::channel();
        self.normal
            .send(Command::Shutdown {
                work_deadline: Some(work_deadline),
                reply,
            })
            .await
            .map_err(|_| CanaryActorError::NotRunning)?;
        response.await.map_err(|_| CanaryActorError::NotRunning)?
    }
}

pub struct CanaryActor<S: CanarySubmitter + CanaryReconciler> {
    state: CanaryCampaignState,
    journal: CanaryJournal,
    submitter: S,
    normal: mpsc::Receiver<Command>,
    dispatch_gate: Arc<Mutex<()>>,
    killed: Arc<AtomicBool>,
    final_reconciliation_requested: Arc<AtomicBool>,
    kill_wakeup: Arc<Notify>,
    kill_request: Arc<Mutex<Option<KillRequest>>>,
    journal_failed: bool,
}

impl<S: CanarySubmitter + CanaryReconciler> CanaryActor<S> {
    pub fn new(
        state: CanaryCampaignState,
        journal: CanaryJournal,
        submitter: S,
    ) -> (Self, CanaryActorHandle) {
        let (normal_tx, normal_rx) = mpsc::channel(CANARY_COMMAND_QUEUE_CAPACITY);
        let dispatch_gate = Arc::new(Mutex::new(()));
        let killed = Arc::new(AtomicBool::new(
            state.kill_latched || state.pending.is_some(),
        ));
        let final_reconciliation_requested = Arc::new(AtomicBool::new(false));
        let kill_wakeup = Arc::new(Notify::new());
        let kill_request = Arc::new(Mutex::new(None));
        (
            Self {
                state,
                journal,
                submitter,
                normal: normal_rx,
                dispatch_gate: dispatch_gate.clone(),
                killed: killed.clone(),
                final_reconciliation_requested: final_reconciliation_requested.clone(),
                kill_wakeup: kill_wakeup.clone(),
                kill_request: kill_request.clone(),
                journal_failed: false,
            },
            CanaryActorHandle {
                normal: normal_tx,
                dispatch_gate,
                killed,
                final_reconciliation_requested,
                kill_wakeup,
                kill_request,
            },
        )
    }

    pub async fn run(mut self) {
        if self.state.pending.is_some()
            && self
                .commit(CanaryEvent::AdmissionClosed {
                    reason: ClosureReason::VenueAmbiguous,
                    receipt: None,
                })
                .is_err()
        {
            return;
        }
        if self.state.campaign_id.is_some()
            && self
                .capture_reconciliation("startup_recovery", None)
                .await
                .is_err()
        {
            let _ = self.commit(CanaryEvent::RecoveryRequired {
                reason: ClosureReason::VenueAmbiguous,
            });
        }
        loop {
            tokio::select! {
                biased;
                () = self.kill_wakeup.notified() => {
                    let request = self.kill_request.lock().await.take();
                    if let Some(request) = request {
                        let result = match self.check_receipt(&request.receipt) {
                            Err(error) => Err(error),
                            Ok(true) => Ok(()),
                            Ok(false) if self.state.campaign_id.is_none() => Ok(()),
                            Ok(false) => {
                                let mut result = self.commit(CanaryEvent::AdmissionClosed {
                                    reason: ClosureReason::Killed,
                                    receipt: Some(request.receipt),
                                }).map_err(CanaryActorError::from);
                                let final_reconciliation_failed = if result.is_err() {
                                    false
                                } else if self.state.shutdown_reconciliation_designated {
                                    !self.state.shutdown_reconciliation_succeeded
                                } else {
                                    self.capture_reconciliation_until(
                                        "operator_kill",
                                        None,
                                        request.work_deadline,
                                    )
                                    .await
                                    .is_err()
                                };
                                if final_reconciliation_failed {
                                    result = self.commit(CanaryEvent::RecoveryRequired {
                                        reason: ClosureReason::VenueAmbiguous,
                                    }).map_err(CanaryActorError::from);
                                }
                                result
                            }
                        };
                        let fatal = matches!(result, Err(CanaryActorError::State(CanaryStateError::EventLog(_))));
                        let _ = request.reply.send(result);
                        if fatal || self.journal_failed { break; }
                    }
                }
                command = self.normal.recv() => {
                    let Some(command) = command else { break };
                    if self.handle(command).await || self.journal_failed { break; }
                }
            }
        }
    }

    async fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::Arm {
                receipt,
                authorization,
                reply,
            } => {
                let result = if self.killed.load(Ordering::SeqCst) {
                    Err(CanaryActorError::Killed)
                } else {
                    match self.check_receipt(&receipt) {
                        Err(error) => Err(error),
                        Ok(true) => Ok(self.state.clone()),
                        Ok(false) => match authorization
                            .validate(OffsetDateTime::now_utc())
                            .map_err(CanaryActorError::from)
                        {
                            Err(error) => Err(error),
                            Ok(()) => match self.capture_reconciliation("arm_probes", None).await {
                                Err(error) => Err(error),
                                Ok(snapshot)
                                    if snapshot.free_collateral == CAMPAIGN_START_COLLATERAL
                                        && snapshot.allowance == authorization.allowance
                                        && snapshot.standard_spender_only
                                        && !snapshot.geoblocked
                                        && !snapshot.closed_only
                                        && snapshot.open_order_ids.is_empty()
                                        && snapshot.all_trade_ids.is_empty()
                                        && snapshot.position_count == 0
                                        && !snapshot.unexpected_activity =>
                                {
                                    self.commit(CanaryEvent::CampaignArmed {
                                        authorization: *authorization,
                                        receipt,
                                    })
                                    .map(|()| self.state.clone())
                                    .map_err(CanaryActorError::from)
                                }
                                Ok(_) => Err(CanaryActorError::State(
                                    CanaryStateError::AdmissionMismatch,
                                )),
                            },
                        },
                    }
                };
                let _ = reply.send(result);
            }
            Command::ReviewProbe {
                receipt,
                campaign_id,
                ordinal,
                reply,
            } => {
                let result = match self.check_receipt(&receipt) {
                    Err(error) => Err(error),
                    Ok(true) => Ok(self.state.clone()),
                    Ok(false) => match self.state.reviewable_probe_hash() {
                        Ok(Some(reviewed_probe_hash)) => self
                            .commit(CanaryEvent::ProbeReviewAccepted {
                                campaign_id,
                                reviewed_probe_ordinal: ordinal,
                                reviewed_probe_hash,
                                receipt,
                            })
                            .map(|()| self.state.clone())
                            .map_err(CanaryActorError::from),
                        Ok(None) => Err(CanaryActorError::State(CanaryStateError::InvalidAttempt)),
                        Err(error) => Err(CanaryActorError::State(error)),
                    },
                };
                let _ = reply.send(result);
            }
            Command::ArmOrganic {
                receipt,
                authorization,
                reply,
            } => {
                let result = if let Err(error) = self.check_receipt(&receipt) {
                    Err(error)
                } else if self.check_receipt(&receipt).ok() == Some(true) {
                    Ok(self.state.clone())
                } else if OffsetDateTime::now_utc() >= authorization.expires_at {
                    Err(CanaryActorError::State(CanaryStateError::AuthorityMismatch))
                } else {
                    match self.capture_reconciliation("advance_organic", None).await {
                        Ok(snapshot)
                            if !snapshot.geoblocked
                                && !snapshot.closed_only
                                && !snapshot.unexpected_activity
                                && snapshot.open_order_ids.is_empty() =>
                        {
                            self.commit(CanaryEvent::OrganicStageArmed {
                                authorization,
                                receipt,
                            })
                            .map(|()| self.state.clone())
                            .map_err(CanaryActorError::from)
                        }
                        Ok(_) => Err(CanaryActorError::State(CanaryStateError::AdmissionMismatch)),
                        Err(error) => Err(error),
                    }
                };
                let _ = reply.send(result);
            }
            Command::Dispatch {
                receipt,
                admission,
                request,
                evidence,
                reply,
            } => {
                let result = match self.check_receipt(&receipt) {
                    Err(error) => Err(error),
                    Ok(true) => Ok(self.state.clone()),
                    Ok(false) => self.dispatch(receipt, *admission, request, evidence).await,
                };
                let _ = reply.send(result);
            }
            Command::Skip {
                receipt,
                identity,
                reason,
                evidence,
                reply,
            } => {
                let result = match self.check_receipt(&receipt) {
                    Err(error) => Err(error),
                    Ok(true) => Ok(self.state.clone()),
                    Ok(false) => (|| {
                        let evidence_hashes = self.capture_evidence(&identity, &evidence)?;
                        self.commit(CanaryEvent::PreReservationSkipped {
                            identity,
                            reason,
                            evidence_hashes,
                            receipt,
                        })?;
                        Ok(self.state.clone())
                    })(),
                };
                let _ = reply.send(result);
            }
            Command::Reconcile {
                receipt,
                context,
                reply,
            } => {
                let result = match receipt.as_ref().map(|receipt| self.check_receipt(receipt)) {
                    Some(Err(error)) => Err(error),
                    Some(Ok(true)) => Ok(self.state.clone()),
                    _ if self.state.shutdown_reconciliation_designated => Ok(self.state.clone()),
                    _ => self
                        .capture_reconciliation(&context, receipt)
                        .await
                        .map(|_| self.state.clone()),
                };
                let _ = reply.send(result);
            }
            Command::Status { reply } => {
                let _ = reply.send(self.state.clone());
            }
            Command::Shutdown {
                work_deadline,
                reply,
            } => {
                if self.state.campaign_id.is_none() {
                    let _ = reply.send(Ok(self.state.clone()));
                    return true;
                }
                let final_reconciliation = if self.state.shutdown_reconciliation_designated {
                    self.state.shutdown_reconciliation_succeeded.then_some(())
                } else {
                    self.final_reconciliation_requested
                        .store(true, Ordering::SeqCst);
                    self.capture_reconciliation_until("shutdown_final", None, work_deadline)
                        .await
                        .ok()
                        .map(|_| ())
                };
                let transition = if final_reconciliation.is_none() || self.state.pending.is_some() {
                    self.commit(CanaryEvent::RecoveryRequired {
                        reason: ClosureReason::VenueAmbiguous,
                    })
                    .map_err(CanaryActorError::from)
                } else if self.state.stage == crate::canary::CampaignStage::Closed {
                    Ok(())
                } else {
                    self.commit(CanaryEvent::AdmissionClosed {
                        reason: ClosureReason::Killed,
                        receipt: None,
                    })
                    .map_err(CanaryActorError::from)
                };
                let result = transition.map(|()| self.state.clone());
                let _ = reply.send(result);
                return true;
            }
        }
        false
    }

    async fn dispatch(
        &mut self,
        receipt: CommandReceipt,
        admission: CanaryAdmission,
        request: V2BuyRequest,
        evidence: Vec<RawEvidence>,
    ) -> Result<CanaryCampaignState, CanaryActorError> {
        if self.killed.load(Ordering::SeqCst) {
            return Err(CanaryActorError::Killed);
        }
        if evidence.is_empty() {
            return Err(CanaryActorError::State(CanaryStateError::AdmissionMismatch));
        }
        let evidence_hashes = self.capture_evidence(&admission.identity, &evidence)?;
        if !evidence_matches_quote(
            &admission.quote.metadata_hashes,
            &admission.quote.snapshot_raw_hash,
            &evidence_hashes,
        ) {
            return Err(CanaryActorError::State(CanaryStateError::AdmissionMismatch));
        }
        let submission = self
            .submitter
            .prepare_buy(request)
            .await
            .map_err(CanaryActorError::Post)?;
        let prepared = submission.prepared().clone();
        let preflight = self.capture_reconciliation("pre_reservation", None).await?;
        admission.validate(
            &self.state,
            &prepared,
            OffsetDateTime::now_utc(),
            &preflight,
        )?;
        let identity = admission.identity.clone();
        {
            let gate = self.dispatch_gate.clone();
            let _dispatch = gate.lock().await;
            if self.killed.load(Ordering::SeqCst) {
                return Err(CanaryActorError::Killed);
            }
            self.commit(CanaryEvent::AttemptReserved {
                admission: Box::new(admission),
                prepared: prepared.clone(),
                receipt,
            })?;
            self.commit(CanaryEvent::PostInFlight {
                identity: identity.clone(),
                order_hash: prepared.order_hash,
                post_body_hash: prepared.post_body_hash,
            })?;
        }

        let post_observed_at = OffsetDateTime::now_utc();
        let result = tokio::time::timeout(
            Duration::from_secs(CANARY_POST_TIMEOUT_SECS),
            self.submitter.post_order_once(submission),
        )
        .await;
        let raw_response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(failure)) => {
                self.capture_evidence(
                    &identity,
                    &[RawEvidence::HttpTransportFailure(failure.clone())],
                )?;
                return self
                    .recover_ambiguous_post(failure.error_class.to_string())
                    .await;
            }
            Err(_) => {
                let failure = RawTransportFailure {
                    source_id: "polymarket-clob-v2".to_owned(),
                    endpoint_kind: "order-post".to_owned(),
                    method: "POST".to_owned(),
                    path: "/order".to_owned(),
                    ordered_query: Vec::new(),
                    attempt_ordinal: 1,
                    observed_at: post_observed_at,
                    received_at: OffsetDateTime::now_utc(),
                    error_class: TransportErrorClass::Timeout,
                    schema_version: 1,
                    parser_version: 1,
                    adapter_version: pe_venue_polymarket::SDK_VERSION.to_owned(),
                };
                self.capture_evidence(&identity, &[RawEvidence::HttpTransportFailure(failure)])?;
                return self.recover_ambiguous_post("timeout".to_owned()).await;
            }
        };
        self.capture_evidence(
            &identity,
            &[RawEvidence::HttpResponse(raw_response.clone())],
        )?;
        let result = match self.submitter.parse_post_response(&raw_response) {
            Ok(result) => result,
            Err(error) => {
                return self.recover_ambiguous_post(error).await;
            }
        };
        self.commit(CanaryEvent::PostReturned {
            identity: identity.clone(),
            success: result.success,
            definitive: result.definitive,
            order_id: result.order_id.clone(),
            error_message: result.error_message.clone(),
        })?;
        if !result.definitive {
            self.commit(CanaryEvent::RecoveryRequired {
                reason: ClosureReason::VenueAmbiguous,
            })?;
        }
        if !result.success && !result.order_id.is_empty() {
            match self
                .submitter
                .cancel_order_once(result.order_id.clone())
                .await
            {
                Ok(response) => {
                    let status = response.status;
                    self.capture_evidence(&identity, &[RawEvidence::HttpResponse(response)])?;
                    if !(200..300).contains(&status) {
                        self.commit(CanaryEvent::AdmissionClosed {
                            reason: ClosureReason::CancellationFailed,
                            receipt: None,
                        })?;
                    }
                }
                Err(failure) => {
                    self.capture_evidence(
                        &identity,
                        &[RawEvidence::HttpTransportFailure(failure)],
                    )?;
                    self.commit(CanaryEvent::AdmissionClosed {
                        reason: ClosureReason::CancellationFailed,
                        receipt: None,
                    })?;
                }
            }
        }
        self.capture_reconciliation("post_return", None).await?;
        if self
            .state
            .pending
            .as_ref()
            .is_some_and(|attempt| attempt.identity == identity)
        {
            return Err(CanaryActorError::State(CanaryStateError::AdmissionMismatch));
        }
        Ok(self.state.clone())
    }

    async fn recover_ambiguous_post<T>(&mut self, error: String) -> Result<T, CanaryActorError> {
        self.commit(CanaryEvent::RecoveryRequired {
            reason: ClosureReason::VenueAmbiguous,
        })?;
        // Reconciliation is the only safe resolution after the single POST
        // seam becomes ambiguous. Preserve its events even though the caller
        // still receives the original submission error.
        let _ = self.capture_reconciliation("post_ambiguous", None).await;
        Err(CanaryActorError::Post(error))
    }

    async fn capture_reconciliation(
        &mut self,
        context: &str,
        receipt: Option<CommandReceipt>,
    ) -> Result<CanaryReconciliation, CanaryActorError> {
        self.capture_reconciliation_until(context, receipt, None)
            .await
    }

    async fn capture_reconciliation_until(
        &mut self,
        context: &str,
        receipt: Option<CommandReceipt>,
        work_deadline: Option<tokio::time::Instant>,
    ) -> Result<CanaryReconciliation, CanaryActorError> {
        let timeout = work_deadline.map_or(
            Duration::from_secs(CANARY_RECONCILIATION_TIMEOUT_SECS),
            |deadline| {
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(Duration::from_secs(CANARY_RECONCILIATION_TIMEOUT_SECS))
            },
        );
        let mut tracked_conditions = self
            .state
            .attempts
            .iter()
            .filter(|attempt| attempt.open_debit != pe_core_types::CollateralAmount::ZERO)
            .map(|attempt| attempt.attribution.condition_id.clone())
            .collect::<Vec<_>>();
        tracked_conditions.sort_by(|left, right| left.0.cmp(&right.0));
        tracked_conditions.dedup();
        let deadline = tokio::time::Instant::now() + timeout;
        let pending_order_hash = self
            .state
            .pending
            .as_ref()
            .map(|pending| pending.order_hash.clone());
        let raw = match self
            .submitter
            .reconcile_raw(tracked_conditions, pending_order_hash, deadline)
            .await
        {
            Err(error) => {
                return Err(self.record_reconciliation_failure(context, error, receipt));
            }
            Ok(raw) => raw,
        };
        self.capture_evidence(&format!("reconciliation:{context}"), &raw.evidence)?;
        if let Some(failure) = raw
            .evidence
            .iter()
            .find_map(|observation| match observation {
                RawEvidence::HttpTransportFailure(failure) => Some(failure),
                RawEvidence::HttpResponse(_) | RawEvidence::Artifact(_) => None,
            })
        {
            return Err(self.record_reconciliation_failure(
                context,
                failure.error_class.to_string(),
                receipt,
            ));
        }
        if let Some(protocol_failure) = raw.protocol_failure.as_ref() {
            return Err(self.record_reconciliation_failure(
                context,
                protocol_failure.clone(),
                receipt,
            ));
        }
        let mut snapshot = match self.submitter.parse_reconciliation(
            &raw,
            self.state.pending.as_ref(),
            &self.state.known_trade_ids,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(self.record_reconciliation_failure(context, error, receipt));
            }
        };
        if !self.state.reconciliation_inventory_explained(&snapshot) {
            snapshot.unexpected_activity = true;
        }
        let accounting_drift = !self.state.reconciliation_balance_explained(&snapshot);
        let allowance_drift = self.state.campaign_id.is_some()
            && self
                .state
                .expected_allowance_after(&snapshot)
                .is_none_or(|expected| snapshot.allowance != expected);
        let final_for_shutdown = self.final_reconciliation_requested.load(Ordering::SeqCst)
            && !self.state.shutdown_reconciliation_designated;
        self.commit(CanaryEvent::ReconciliationRecorded {
            context: context.to_owned(),
            snapshot: snapshot.clone(),
            receipt,
            final_for_shutdown,
        })?;
        if self
            .state
            .expires_at
            .is_some_and(|expires_at| OffsetDateTime::now_utc() >= expires_at)
            && matches!(
                self.state.stage,
                crate::canary::CampaignStage::ProbesArmed
                    | crate::canary::CampaignStage::OrganicReady
                    | crate::canary::CampaignStage::OrganicArmed
            )
        {
            self.commit(CanaryEvent::AdmissionClosed {
                reason: ClosureReason::Expired,
                receipt: None,
            })?;
        } else if snapshot.geoblocked
            || snapshot.closed_only
            || !snapshot.standard_spender_only
            || snapshot.allowance > CAMPAIGN_MAX_ALLOWANCE
            || accounting_drift
            || allowance_drift
            || !snapshot.open_order_ids.is_empty()
            || snapshot.unexpected_activity
        {
            self.commit(CanaryEvent::AdmissionClosed {
                reason: if snapshot.geoblocked {
                    ClosureReason::Geoblocked
                } else if snapshot.closed_only {
                    ClosureReason::AccountClosedOnly
                } else if snapshot.unexpected_activity {
                    ClosureReason::ExternalActivity
                } else if !snapshot.open_order_ids.is_empty() {
                    ClosureReason::UnexpectedOrderState
                } else {
                    ClosureReason::AccountingDrift
                },
                receipt: None,
            })?;
        }
        if self.state.stage == crate::canary::CampaignStage::ClosedObserving
            && self.state.pending.is_none()
            && snapshot.open_order_ids.is_empty()
            && snapshot.position_count == 0
            && !snapshot.unexpected_activity
            && !accounting_drift
            && !allowance_drift
            && !self.state.recovery_unresolved
            && self
                .state
                .attempts
                .iter()
                .all(|attempt| attempt.open_debit == pe_core_types::CollateralAmount::ZERO)
        {
            self.commit(CanaryEvent::CampaignClosed)?;
        }
        Ok(snapshot)
    }

    fn record_reconciliation_failure(
        &mut self,
        context: &str,
        error_class: String,
        _receipt: Option<CommandReceipt>,
    ) -> CanaryActorError {
        match self.commit(CanaryEvent::ReconciliationFailed {
            context: context.to_owned(),
            error_class: error_class.clone(),
            final_for_shutdown: self.final_reconciliation_requested.load(Ordering::SeqCst)
                && !self.state.shutdown_reconciliation_designated,
        }) {
            Ok(()) => CanaryActorError::Post(error_class),
            Err(error) => CanaryActorError::State(error),
        }
    }

    fn commit(&mut self, event: CanaryEvent) -> Result<(), CanaryStateError> {
        let mut next = self.state.clone();
        next.apply(&event)?;
        if let Err(error) = self.journal.append_sync(&event, OffsetDateTime::now_utc()) {
            self.journal_failed = true;
            return Err(error);
        }
        self.state = next;
        if self.state.kill_latched {
            self.killed.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn capture_evidence(
        &mut self,
        identity: &str,
        evidence: &[RawEvidence],
    ) -> Result<Vec<String>, CanaryActorError> {
        let mut hashes = Vec::with_capacity(evidence.len());
        for observation in evidence {
            match observation {
                RawEvidence::HttpResponse(response) => {
                    self.commit(CanaryEvent::HttpResponseCaptured {
                        identity: identity.to_owned(),
                        source_id: response.source_id.clone(),
                        endpoint_kind: response.endpoint_kind.clone(),
                        method: response.method.clone(),
                        path: response.path.clone(),
                        ordered_query: response.ordered_query.clone(),
                        attempt_ordinal: response.attempt_ordinal,
                        status: response.status,
                        headers: response.headers.clone(),
                        source_at: response.source_at,
                        observed_at: response.observed_at,
                        received_at: response.received_at,
                        schema_version: response.schema_version,
                        parser_version: response.parser_version,
                        adapter_version: response.adapter_version.clone(),
                        raw_body_hash: blake3::hash(&response.body).to_hex().to_string(),
                        raw_body: response.body.clone(),
                    })?;
                }
                RawEvidence::HttpTransportFailure(failure) => {
                    self.commit(CanaryEvent::HttpTransportFailed {
                        identity: identity.to_owned(),
                        source_id: failure.source_id.clone(),
                        endpoint_kind: failure.endpoint_kind.clone(),
                        method: failure.method.clone(),
                        path: failure.path.clone(),
                        ordered_query: failure.ordered_query.clone(),
                        attempt_ordinal: failure.attempt_ordinal,
                        observed_at: failure.observed_at,
                        received_at: failure.received_at,
                        error_class: failure.error_class,
                        schema_version: failure.schema_version,
                        parser_version: failure.parser_version,
                        adapter_version: failure.adapter_version.clone(),
                    })?;
                }
                RawEvidence::Artifact(artifact) => {
                    self.commit(CanaryEvent::ArtifactCaptured {
                        identity: identity.to_owned(),
                        source_id: artifact.source_id.clone(),
                        artifact_kind: artifact.artifact_kind.clone(),
                        path: artifact.path.clone(),
                        observed_at: artifact.observed_at,
                        received_at: artifact.received_at,
                        schema_version: artifact.schema_version,
                        parser_version: artifact.parser_version,
                        adapter_version: artifact.adapter_version.clone(),
                        raw_body_hash: blake3::hash(&artifact.body).to_hex().to_string(),
                        raw_body: artifact.body.clone(),
                    })?;
                }
            }
            if let Some(hash) = raw_evidence_hash(observation)
                .map_err(|error| CanaryActorError::Post(error.to_string()))?
            {
                hashes.push(hash);
            }
        }
        Ok(hashes)
    }

    fn check_receipt(&self, receipt: &CommandReceipt) -> Result<bool, CanaryActorError> {
        match self.state.command_receipt(&receipt.command_id) {
            Some(existing) if existing.command_hash == receipt.command_hash => Ok(true),
            Some(_) => Err(CanaryActorError::State(CanaryStateError::AuthorityMismatch)),
            None if receipt.command_id.trim().is_empty()
                || receipt.command_hash.trim().is_empty() =>
            {
                Err(CanaryActorError::State(CanaryStateError::InvalidAttempt))
            }
            None => Ok(false),
        }
    }
}

fn evidence_matches_quote(expected: &[String], snapshot: &str, captured: &[String]) -> bool {
    expected == captured && captured.iter().any(|hash| hash == snapshot)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tempfile::tempdir;
    use time::Duration as TimeDuration;

    use super::*;
    use crate::canary::{
        CAMPAIGN_MAX_COMMITMENT, CAMPAIGN_START_COLLATERAL, CampaignStage, ORGANIC_POST_LIMIT,
        PROBE_POST_LIMIT,
    };

    struct FakeIo {
        reconciliations: Arc<AtomicUsize>,
        fail_reconciliation: Arc<AtomicBool>,
        block_reconciliation: Option<(Arc<Notify>, Arc<Notify>)>,
        geoblocked: bool,
    }

    fn test_transport_failure(endpoint_kind: &str) -> RawTransportFailure {
        RawTransportFailure {
            source_id: "test".to_owned(),
            endpoint_kind: endpoint_kind.to_owned(),
            method: "POST".to_owned(),
            path: "/test".to_owned(),
            ordered_query: Vec::new(),
            attempt_ordinal: 1,
            observed_at: OffsetDateTime::UNIX_EPOCH,
            received_at: OffsetDateTime::UNIX_EPOCH,
            error_class: TransportErrorClass::Other,
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        }
    }

    impl CanarySubmitter for FakeIo {
        fn prepare_buy(
            &self,
            _request: V2BuyRequest,
        ) -> Pin<Box<dyn Future<Output = Result<PreparedSubmission, String>> + Send + '_>> {
            Box::pin(async { Err("POST preparation must not be reached".to_owned()) })
        }

        fn post_order_once(
            &self,
            _submission: PreparedSubmission,
        ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + '_>>
        {
            Box::pin(async { Err(test_transport_failure("post")) })
        }

        fn cancel_order_once(
            &self,
            _order_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + '_>>
        {
            Box::pin(async { Err(test_transport_failure("cancel")) })
        }

        fn parse_post_response(
            &self,
            _response: &RawHttpResponse,
        ) -> Result<PostOnceResult, String> {
            Err("POST parsing must not be reached".to_owned())
        }
    }

    impl CanaryReconciler for FakeIo {
        fn reconcile_raw(
            &self,
            _tracked_conditions: Vec<pe_core_types::PolymarketConditionId>,
            _pending_order_hash: Option<String>,
            _deadline: tokio::time::Instant,
        ) -> Pin<Box<dyn Future<Output = Result<RawReconciliation, String>> + Send + '_>> {
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            let fail = self.fail_reconciliation.load(Ordering::SeqCst);
            let block = self.block_reconciliation.clone();
            Box::pin(async move {
                if let Some((started, release)) = block {
                    started.notify_one();
                    release.notified().await;
                }
                if fail {
                    Err("injected reconciliation failure".to_owned())
                } else {
                    Ok(RawReconciliation {
                        evidence: Vec::new(),
                        protocol_failure: None,
                    })
                }
            })
        }

        fn parse_reconciliation(
            &self,
            _raw: &RawReconciliation,
            _pending: Option<&PendingAttempt>,
            _known_trade_ids: &[String],
        ) -> Result<CanaryReconciliation, String> {
            let mut snapshot = snapshot();
            snapshot.geoblocked = self.geoblocked;
            Ok(snapshot)
        }
    }

    fn snapshot() -> CanaryReconciliation {
        CanaryReconciliation {
            observed_at: OffsetDateTime::now_utc(),
            geoblocked: false,
            closed_only: false,
            free_collateral: CAMPAIGN_START_COLLATERAL,
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            standard_spender_only: true,
            open_order_ids: Vec::new(),
            all_trade_ids: Vec::new(),
            position_count: 0,
            positions: Vec::new(),
            resolutions: Vec::new(),
            unexpected_activity: false,
            execution_report: None,
            evidence_hashes: Vec::new(),
        }
    }

    fn active_state() -> CanaryCampaignState {
        CanaryCampaignState {
            campaign_id: Some("campaign".to_owned()),
            stage: CampaignStage::ProbesArmed,
            starting_collateral: CAMPAIGN_START_COLLATERAL,
            free_collateral: CAMPAIGN_START_COLLATERAL,
            canary_bankroll: CAMPAIGN_START_COLLATERAL,
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            ..CanaryCampaignState::default()
        }
    }

    fn authority() -> CampaignAuthorization {
        let now = OffsetDateTime::now_utc();
        CampaignAuthorization {
            schema_version: 1,
            campaign_id: "campaign".to_owned(),
            implementation_commit: "commit".to_owned(),
            binary_hash: "binary".to_owned(),
            config_hash: "config".to_owned(),
            resolver_inventory_hash: "resolver".to_owned(),
            sdk_archive_sha256: "sdk".to_owned(),
            sdk_effective_vendor_tree_sha256: "sdk-tree".to_owned(),
            wallet: "wallet".to_owned(),
            owner_signer: "owner".to_owned(),
            spender: "spender".to_owned(),
            jurisdiction: "jurisdiction".to_owned(),
            jurisdiction_attestation_hash: "jurisdiction-hash".to_owned(),
            account_attestation_hash: "account-hash".to_owned(),
            issued_at: now,
            expires_at: now + TimeDuration::hours(1),
            starting_collateral: CAMPAIGN_START_COLLATERAL,
            allowance: CAMPAIGN_MAX_ALLOWANCE,
            commitment_cap: CAMPAIGN_MAX_COMMITMENT,
            probe_slots: PROBE_POST_LIMIT,
            organic_slots: ORGANIC_POST_LIMIT,
        }
    }

    fn receipt(id: &str) -> CommandReceipt {
        CommandReceipt {
            command_id: id.to_owned(),
            command_hash: format!("hash-{id}"),
        }
    }

    #[test]
    fn quote_evidence_requires_exact_order_multiplicity_and_membership() {
        let expected = vec![
            "ranking".to_owned(),
            "resolver".to_owned(),
            "book".to_owned(),
        ];
        assert!(evidence_matches_quote(&expected, "book", &expected));
        assert!(!evidence_matches_quote(
            &expected,
            "book",
            &[
                "resolver".to_owned(),
                "ranking".to_owned(),
                "book".to_owned()
            ]
        ));
        assert!(!evidence_matches_quote(
            &expected,
            "book",
            &[
                "ranking".to_owned(),
                "resolver".to_owned(),
                "book".to_owned(),
                "book".to_owned(),
            ]
        ));
        assert!(!evidence_matches_quote(
            &expected,
            "book",
            &[
                "ranking".to_owned(),
                "resolver".to_owned(),
                "extra".to_owned(),
                "book".to_owned(),
            ]
        ));
    }

    #[tokio::test]
    async fn malformed_resolver_artifact_is_synced_before_skip_and_rebuilds() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("canary.log");
        let journal = CanaryJournal::open(&path).unwrap();
        let (actor, handle) = CanaryActor::new(
            CanaryCampaignState::default(),
            journal,
            FakeIo {
                reconciliations: Arc::new(AtomicUsize::new(0)),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: None,
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        let artifact = pe_core_types::RawArtifactObservation {
            source_id: "resolver-card".to_owned(),
            artifact_kind: "resolver-card".to_owned(),
            path: "/resolver/malformed.json".to_owned(),
            body: b"{malformed".to_vec(),
            observed_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        };
        handle
            .record_skip(
                receipt("resolver-skip"),
                "resolver-skip".to_owned(),
                "resolver validation failed".to_owned(),
                vec![RawEvidence::Artifact(artifact.clone())],
            )
            .await
            .unwrap();
        handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        task.await.unwrap();

        let events = pe_event_log::Reader::replay(&path)
            .unwrap()
            .map(|item| {
                let (_, envelope) = item.unwrap();
                serde_json::from_slice::<CanaryEvent>(&envelope.payload).unwrap()
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            events.as_slice(),
            [
                CanaryEvent::ArtifactCaptured { raw_body, .. },
                CanaryEvent::PreReservationSkipped { .. }
            ] if raw_body == &artifact.body
        ));
        let rebuilt = CanaryJournal::rebuild(&path).unwrap();
        assert!(rebuilt.command_receipt("resolver-skip").is_some());
    }

    #[tokio::test]
    async fn priority_kill_prevents_a_later_arm_even_without_a_campaign() {
        let directory = tempdir().unwrap();
        let journal = CanaryJournal::open(directory.path().join("canary.log")).unwrap();
        let (actor, handle) = CanaryActor::new(
            CanaryCampaignState::default(),
            journal,
            FakeIo {
                reconciliations: Arc::new(AtomicUsize::new(0)),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: None,
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        handle.kill(receipt("kill")).await.unwrap();
        assert!(matches!(
            handle.arm(receipt("arm"), authority()).await,
            Err(CanaryActorError::Killed)
        ));
        handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn journal_sync_failure_stops_the_actor_before_campaign_arm() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("canary.log");
        let mut journal = CanaryJournal::open(&path).unwrap();
        journal.inject_next_sync_failure();
        let (actor, handle) = CanaryActor::new(
            CanaryCampaignState::default(),
            journal,
            FakeIo {
                reconciliations: Arc::new(AtomicUsize::new(0)),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: None,
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        assert!(matches!(
            handle.arm(receipt("arm"), authority()).await,
            Err(CanaryActorError::State(CanaryStateError::JournalIo(_)))
        ));
        task.await.unwrap();
        assert!(CanaryJournal::rebuild(&path).unwrap().campaign_id.is_none());
    }

    #[tokio::test]
    async fn active_startup_reconciles_before_serving_status() {
        let directory = tempdir().unwrap();
        let journal = CanaryJournal::open(directory.path().join("canary.log")).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let (actor, handle) = CanaryActor::new(
            active_state(),
            journal,
            FakeIo {
                reconciliations: count.clone(),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: None,
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        let state = handle.status().await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(state.last_reconciliation.is_some());
        handle.kill(receipt("kill")).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn reconciliation_running_at_kill_is_the_single_designated_final_pass() {
        let directory = tempdir().unwrap();
        let journal = CanaryJournal::open(directory.path().join("canary.log")).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (actor, handle) = CanaryActor::new(
            active_state(),
            journal,
            FakeIo {
                reconciliations: count.clone(),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: Some((started.clone(), release.clone())),
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        started.notified().await;
        let kill_handle = handle.clone();
        let kill = tokio::spawn(async move { kill_handle.kill(receipt("kill")).await });
        while !handle.final_reconciliation_requested.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        release.notify_one();
        kill.await.unwrap().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let shutdown = handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(shutdown.shutdown_reconciliation_succeeded);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn kill_during_reconciliation_preserves_geoblock_closure() {
        let directory = tempdir().unwrap();
        let journal = CanaryJournal::open(directory.path().join("canary.log")).unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (actor, handle) = CanaryActor::new(
            active_state(),
            journal,
            FakeIo {
                reconciliations: Arc::new(AtomicUsize::new(0)),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: Some((started.clone(), release.clone())),
                geoblocked: true,
            },
        );
        let task = tokio::spawn(actor.run());
        started.notified().await;
        let kill_handle = handle.clone();
        let kill = tokio::spawn(async move { kill_handle.kill(receipt("kill")).await });
        while !handle.final_reconciliation_requested.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        release.notify_one();
        kill.await.unwrap().unwrap();
        let state = handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(state.stage, CampaignStage::Closed);
        assert_eq!(state.terminal_reason, Some(ClosureReason::Geoblocked));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn kill_during_final_clean_reconciliation_does_not_reopen_closed_campaign() {
        let directory = tempdir().unwrap();
        let journal = CanaryJournal::open(directory.path().join("canary.log")).unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut state = active_state();
        state.stage = CampaignStage::ClosedObserving;
        let (actor, handle) = CanaryActor::new(
            state,
            journal,
            FakeIo {
                reconciliations: Arc::new(AtomicUsize::new(0)),
                fail_reconciliation: Arc::new(AtomicBool::new(false)),
                block_reconciliation: Some((started.clone(), release.clone())),
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        started.notified().await;
        let kill_handle = handle.clone();
        let kill = tokio::spawn(async move { kill_handle.kill(receipt("kill")).await });
        while !handle.final_reconciliation_requested.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        release.notify_one();
        kill.await.unwrap().unwrap();
        let state = handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(state.stage, CampaignStage::Closed);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn failed_designated_reconciliation_is_not_retried_on_shutdown() {
        let directory = tempdir().unwrap();
        let journal = CanaryJournal::open(directory.path().join("canary.log")).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let (actor, handle) = CanaryActor::new(
            active_state(),
            journal,
            FakeIo {
                reconciliations: count.clone(),
                fail_reconciliation: fail.clone(),
                block_reconciliation: None,
                geoblocked: false,
            },
        );
        let task = tokio::spawn(actor.run());
        let state = handle.status().await.unwrap();
        assert!(!state.reconciliation_failed);
        fail.store(true, Ordering::SeqCst);
        handle.kill(receipt("kill")).await.unwrap();
        let shutdown = handle
            .shutdown_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(shutdown.reconciliation_failed);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        task.await.unwrap();
    }
}
