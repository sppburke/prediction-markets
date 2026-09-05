//! Pure durable redemption state machine plus one-pass transport driver.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use pe_core_types::{CollateralAmount, RawHttpAttempt};
use pe_venue_polymarket::{
    CustodyKind, REDEMPTION_ADAPTER_VERSION, REDEMPTION_PARSER_VERSION, REDEMPTION_SCHEMA_VERSION,
    RedemptionTransport, RedemptionTransportError, SignedRedemptionRequest,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::live_journal::{
    LiveJournal, LiveJournalError, LiveJournalEvent, LiveJournalPayload, RedemptionAttemptIdentity,
    RedemptionCustodyAudit, RedemptionReceiptAudit, RedemptionReceiptStatusAudit,
    RedemptionRequestAudit, RedemptionRequestedAudit, RedemptionTransactionAudit,
    http_attempt_hashes,
};

/// An account's redemption failure becomes prominent after this many submitted attempts.
pub const LIVE_REDEMPTION_SURFACE_AFTER_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedemptionFailureKind {
    Authentication,
    Rejected,
    Transport,
    Protocol,
    Terminal,
    InvalidRequest,
}

/// Small persisted state for one [`RedemptionAttemptIdentity`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "detail")]
pub enum RedemptionAttemptState {
    Idle {
        attempt_count: u32,
    },
    SubmissionReserved {
        attempt_count: u32,
        redeemable_balance: CollateralAmount,
        request_hash: String,
    },
    InFlight {
        attempt_count: u32,
        transaction_id: String,
        submit_body_hash: String,
    },
    Ambiguous {
        attempt_count: u32,
        transaction_id: Option<String>,
        submit_body_hash: String,
    },
    Failed {
        attempt_count: u32,
        retry_not_before: OffsetDateTime,
        failure: RedemptionFailureKind,
    },
    ConfirmedAwaitingBalance {
        attempt_count: u32,
        transaction_id: String,
        transaction_hash: String,
        confirmed_at: OffsetDateTime,
    },
    Complete {
        attempt_count: u32,
        transaction_id: String,
        transaction_hash: String,
        reconciled_at: OffsetDateTime,
        remaining_redeemable: CollateralAmount,
    },
}

impl Default for RedemptionAttemptState {
    fn default() -> Self {
        Self::Idle { attempt_count: 0 }
    }
}

impl RedemptionAttemptState {
    #[must_use]
    pub const fn attempt_count(&self) -> u32 {
        match self {
            Self::Idle { attempt_count }
            | Self::SubmissionReserved { attempt_count, .. }
            | Self::InFlight { attempt_count, .. }
            | Self::Ambiguous { attempt_count, .. }
            | Self::Failed { attempt_count, .. }
            | Self::ConfirmedAwaitingBalance { attempt_count, .. }
            | Self::Complete { attempt_count, .. } => *attempt_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedemptionAttempt {
    pub identity: RedemptionAttemptIdentity,
    pub state: RedemptionAttemptState,
}

/// Rebuild the latest durable state of every account-scoped redemption family.
///
/// A request without a later transaction identity remains `SubmissionReserved`; callers must
/// freeze it because the journal cannot prove whether the relayer accepted the submission.
#[must_use]
pub fn reconstruct_redemption_attempts(
    events: &[LiveJournalEvent],
) -> HashMap<RedemptionAttemptIdentity, RedemptionAttempt> {
    let mut attempts = HashMap::new();
    for event in events {
        match &event.payload {
            LiveJournalPayload::RedemptionRequested(audit) => {
                let state = RedemptionAttemptState::SubmissionReserved {
                    attempt_count: audit.attempt_count,
                    redeemable_balance: audit.redeemable_balance,
                    request_hash: audit.request.request_hash.clone(),
                };
                attempts.insert(
                    audit.identity.clone(),
                    RedemptionAttempt {
                        identity: audit.identity.clone(),
                        state,
                    },
                );
            }
            LiveJournalPayload::RedemptionTransactionIdentified(audit) => {
                attempts.insert(
                    audit.identity.clone(),
                    RedemptionAttempt {
                        identity: audit.identity.clone(),
                        state: RedemptionAttemptState::InFlight {
                            attempt_count: audit.attempt_count,
                            transaction_id: audit.transaction_id.clone(),
                            submit_body_hash: audit.submit_body_hash.clone(),
                        },
                    },
                );
            }
            LiveJournalPayload::RedemptionReceiptTransition(audit) => {
                let Some(prior) = attempts.get(&audit.identity) else {
                    continue;
                };
                let submit_body_hash = match &prior.state {
                    RedemptionAttemptState::InFlight {
                        submit_body_hash, ..
                    }
                    | RedemptionAttemptState::Ambiguous {
                        submit_body_hash, ..
                    } => submit_body_hash.clone(),
                    _ => String::new(),
                };
                let state = match audit.status {
                    RedemptionReceiptStatusAudit::Pending => RedemptionAttemptState::InFlight {
                        attempt_count: audit.attempt_count,
                        transaction_id: audit.transaction_id.clone(),
                        submit_body_hash,
                    },
                    RedemptionReceiptStatusAudit::Ambiguous => RedemptionAttemptState::Ambiguous {
                        attempt_count: audit.attempt_count,
                        transaction_id: Some(audit.transaction_id.clone()),
                        submit_body_hash,
                    },
                    RedemptionReceiptStatusAudit::Confirmed => match &audit.transaction_hash {
                        Some(transaction_hash) if !transaction_hash.trim().is_empty() => {
                            RedemptionAttemptState::ConfirmedAwaitingBalance {
                                attempt_count: audit.attempt_count,
                                transaction_id: audit.transaction_id.clone(),
                                transaction_hash: transaction_hash.clone(),
                                confirmed_at: event.timestamp,
                            }
                        }
                        _ => RedemptionAttemptState::Ambiguous {
                            attempt_count: audit.attempt_count,
                            transaction_id: Some(audit.transaction_id.clone()),
                            submit_body_hash,
                        },
                    },
                    RedemptionReceiptStatusAudit::TerminalFailure => {
                        RedemptionAttemptState::Failed {
                            attempt_count: audit.attempt_count,
                            retry_not_before: event.timestamp,
                            failure: RedemptionFailureKind::Terminal,
                        }
                    }
                };
                attempts.insert(
                    audit.identity.clone(),
                    RedemptionAttempt {
                        identity: audit.identity.clone(),
                        state,
                    },
                );
            }
            LiveJournalPayload::AdmissionEvaluated(_)
            | LiveJournalPayload::OrderPreparationFailed(_)
            | LiveJournalPayload::OrderPrepared(_)
            | LiveJournalPayload::OrderPosted(_)
            | LiveJournalPayload::OrderReconciled(_)
            | LiveJournalPayload::OrderFillFinalized(_)
            | LiveJournalPayload::ResolutionFinalized(_)
            | LiveJournalPayload::RedemptionCustodyReconciled(_)
            | LiveJournalPayload::AccountPortfolioMarked(_)
            | LiveJournalPayload::CredentialBindingMismatch { .. }
            | LiveJournalPayload::ModeTransitionApplied(_)
            | LiveJournalPayload::LegacyV1(_) => {}
        }
    }
    attempts
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedemptionEvent {
    ResolvedWinnerBalanceObserved {
        now: OffsetDateTime,
        redeemable_balance: CollateralAmount,
        request_hash: String,
    },
    SubmissionAccepted {
        transaction_id: String,
        submit_body_hash: String,
    },
    SubmissionConfirmed {
        transaction_id: String,
        transaction_hash: String,
        confirmed_at: OffsetDateTime,
    },
    SubmissionAmbiguous {
        transaction_id: Option<String>,
        submit_body_hash: String,
    },
    SubmissionFailed {
        retry_not_before: OffsetDateTime,
        failure: RedemptionFailureKind,
    },
    ReconciliationPending,
    ReconciliationConfirmed {
        transaction_hash: String,
        confirmed_at: OffsetDateTime,
    },
    ReconciliationAmbiguous,
    ReconciliationFailed {
        retry_not_before: OffsetDateTime,
        failure: RedemptionFailureKind,
    },
    BalanceReconciled {
        reconciled_at: OffsetDateTime,
        credited_collateral: CollateralAmount,
        remaining_redeemable: CollateralAmount,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedemptionAction {
    SubmitOnce {
        request_hash: String,
    },
    ReconcileExisting {
        transaction_id: String,
    },
    WaitForBackoff {
        retry_not_before: OffsetDateTime,
    },
    AwaitTransactionIdentity,
    ReconcileBalance,
    ProceedsSpendable {
        credited_collateral: CollateralAmount,
    },
}

/// Total, deterministic reducer. Events invalid for the current state are safe no-ops.
#[must_use]
pub fn advance(
    state: RedemptionAttemptState,
    event: RedemptionEvent,
) -> (RedemptionAttemptState, Vec<RedemptionAction>) {
    match (state, event) {
        (
            RedemptionAttemptState::Idle { attempt_count }
            | RedemptionAttemptState::Complete { attempt_count, .. },
            RedemptionEvent::ResolvedWinnerBalanceObserved {
                redeemable_balance,
                request_hash,
                ..
            },
        ) if redeemable_balance > CollateralAmount::ZERO => {
            reserve_submission(attempt_count, redeemable_balance, request_hash)
        }
        (
            state @ (RedemptionAttemptState::Idle { .. } | RedemptionAttemptState::Complete { .. }),
            RedemptionEvent::ResolvedWinnerBalanceObserved { .. },
        ) => (state, Vec::new()),
        (
            RedemptionAttemptState::Failed {
                attempt_count,
                retry_not_before,
                failure,
            },
            RedemptionEvent::ResolvedWinnerBalanceObserved {
                now,
                redeemable_balance,
                request_hash,
            },
        ) => {
            if now < retry_not_before {
                (
                    RedemptionAttemptState::Failed {
                        attempt_count,
                        retry_not_before,
                        failure,
                    },
                    vec![RedemptionAction::WaitForBackoff { retry_not_before }],
                )
            } else if redeemable_balance > CollateralAmount::ZERO {
                reserve_submission(attempt_count, redeemable_balance, request_hash)
            } else {
                (RedemptionAttemptState::Idle { attempt_count }, Vec::new())
            }
        }
        (
            state @ RedemptionAttemptState::SubmissionReserved { .. },
            RedemptionEvent::ResolvedWinnerBalanceObserved { .. },
        ) => (state, vec![RedemptionAction::AwaitTransactionIdentity]),
        (
            RedemptionAttemptState::InFlight {
                attempt_count,
                transaction_id,
                submit_body_hash,
            },
            RedemptionEvent::ResolvedWinnerBalanceObserved { .. },
        ) => {
            let action_transaction_id = transaction_id.clone();
            (
                RedemptionAttemptState::InFlight {
                    attempt_count,
                    transaction_id,
                    submit_body_hash,
                },
                vec![RedemptionAction::ReconcileExisting {
                    transaction_id: action_transaction_id,
                }],
            )
        }
        (
            RedemptionAttemptState::Ambiguous {
                attempt_count,
                transaction_id: Some(transaction_id),
                submit_body_hash,
            },
            RedemptionEvent::ResolvedWinnerBalanceObserved { .. },
        ) => {
            let action_transaction_id = transaction_id.clone();
            (
                RedemptionAttemptState::Ambiguous {
                    attempt_count,
                    transaction_id: Some(transaction_id),
                    submit_body_hash,
                },
                vec![RedemptionAction::ReconcileExisting {
                    transaction_id: action_transaction_id,
                }],
            )
        }
        (
            state @ RedemptionAttemptState::Ambiguous {
                transaction_id: None,
                ..
            },
            RedemptionEvent::ResolvedWinnerBalanceObserved { .. },
        ) => (state, vec![RedemptionAction::AwaitTransactionIdentity]),
        (
            state @ RedemptionAttemptState::ConfirmedAwaitingBalance { .. },
            RedemptionEvent::ResolvedWinnerBalanceObserved { .. },
        ) => (state, vec![RedemptionAction::ReconcileBalance]),
        (
            RedemptionAttemptState::SubmissionReserved { attempt_count, .. },
            RedemptionEvent::SubmissionAccepted {
                transaction_id,
                submit_body_hash,
            },
        ) => (
            RedemptionAttemptState::InFlight {
                attempt_count,
                transaction_id,
                submit_body_hash,
            },
            Vec::new(),
        ),
        (
            state @ (RedemptionAttemptState::SubmissionReserved { .. }
            | RedemptionAttemptState::InFlight { .. }
            | RedemptionAttemptState::Ambiguous { .. }),
            RedemptionEvent::SubmissionConfirmed {
                transaction_id,
                transaction_hash,
                confirmed_at,
            },
        ) => (
            RedemptionAttemptState::ConfirmedAwaitingBalance {
                attempt_count: state.attempt_count(),
                transaction_id,
                transaction_hash,
                confirmed_at,
            },
            vec![RedemptionAction::ReconcileBalance],
        ),
        (
            RedemptionAttemptState::SubmissionReserved { attempt_count, .. },
            RedemptionEvent::SubmissionAmbiguous {
                transaction_id,
                submit_body_hash,
            },
        ) => {
            let actions = transaction_id.as_ref().map_or_else(
                || vec![RedemptionAction::AwaitTransactionIdentity],
                |transaction_id| {
                    vec![RedemptionAction::ReconcileExisting {
                        transaction_id: transaction_id.clone(),
                    }]
                },
            );
            (
                RedemptionAttemptState::Ambiguous {
                    attempt_count,
                    transaction_id,
                    submit_body_hash,
                },
                actions,
            )
        }
        (
            RedemptionAttemptState::SubmissionReserved { attempt_count, .. },
            RedemptionEvent::SubmissionFailed {
                retry_not_before,
                failure,
            },
        ) => (
            RedemptionAttemptState::Failed {
                attempt_count,
                retry_not_before,
                failure,
            },
            vec![RedemptionAction::WaitForBackoff { retry_not_before }],
        ),
        (
            RedemptionAttemptState::InFlight {
                attempt_count,
                transaction_id,
                submit_body_hash,
            }
            | RedemptionAttemptState::Ambiguous {
                attempt_count,
                transaction_id: Some(transaction_id),
                submit_body_hash,
            },
            RedemptionEvent::ReconciliationPending,
        ) => (
            RedemptionAttemptState::InFlight {
                attempt_count,
                transaction_id,
                submit_body_hash,
            },
            Vec::new(),
        ),
        (
            state @ (RedemptionAttemptState::InFlight { .. }
            | RedemptionAttemptState::Ambiguous {
                transaction_id: Some(_),
                ..
            }),
            RedemptionEvent::ReconciliationConfirmed {
                transaction_hash,
                confirmed_at,
            },
        ) => {
            let transaction_id = transaction_id(&state).unwrap_or_default();
            (
                RedemptionAttemptState::ConfirmedAwaitingBalance {
                    attempt_count: state.attempt_count(),
                    transaction_id,
                    transaction_hash,
                    confirmed_at,
                },
                vec![RedemptionAction::ReconcileBalance],
            )
        }
        (
            RedemptionAttemptState::InFlight {
                attempt_count,
                transaction_id,
                submit_body_hash,
            }
            | RedemptionAttemptState::Ambiguous {
                attempt_count,
                transaction_id: Some(transaction_id),
                submit_body_hash,
            },
            RedemptionEvent::ReconciliationAmbiguous,
        ) => (
            RedemptionAttemptState::Ambiguous {
                attempt_count,
                transaction_id: Some(transaction_id),
                submit_body_hash,
            },
            Vec::new(),
        ),
        (
            state @ (RedemptionAttemptState::InFlight { .. }
            | RedemptionAttemptState::Ambiguous { .. }),
            RedemptionEvent::ReconciliationFailed {
                retry_not_before,
                failure,
            },
        ) => (
            RedemptionAttemptState::Failed {
                attempt_count: state.attempt_count(),
                retry_not_before,
                failure,
            },
            vec![RedemptionAction::WaitForBackoff { retry_not_before }],
        ),
        (
            RedemptionAttemptState::ConfirmedAwaitingBalance {
                attempt_count,
                transaction_id,
                transaction_hash,
                ..
            },
            RedemptionEvent::BalanceReconciled {
                reconciled_at,
                credited_collateral,
                remaining_redeemable,
            },
        ) => {
            let actions = if credited_collateral > CollateralAmount::ZERO {
                vec![RedemptionAction::ProceedsSpendable {
                    credited_collateral,
                }]
            } else {
                Vec::new()
            };
            (
                RedemptionAttemptState::Complete {
                    attempt_count,
                    transaction_id,
                    transaction_hash,
                    reconciled_at,
                    remaining_redeemable,
                },
                actions,
            )
        }
        (state, _) => (state, Vec::new()),
    }
}

fn reserve_submission(
    attempt_count: u32,
    redeemable_balance: CollateralAmount,
    request_hash: String,
) -> (RedemptionAttemptState, Vec<RedemptionAction>) {
    let attempt_count = attempt_count.saturating_add(1);
    (
        RedemptionAttemptState::SubmissionReserved {
            attempt_count,
            redeemable_balance,
            request_hash: request_hash.clone(),
        },
        vec![RedemptionAction::SubmitOnce { request_hash }],
    )
}

fn transaction_id(state: &RedemptionAttemptState) -> Option<String> {
    match state {
        RedemptionAttemptState::InFlight { transaction_id, .. } => Some(transaction_id.clone()),
        RedemptionAttemptState::Ambiguous {
            transaction_id: Some(transaction_id),
            ..
        } => Some(transaction_id.clone()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedemptionPosture {
    pub closes_new_buy_admission: bool,
    pub surface_prominently: bool,
}

#[must_use]
pub const fn redemption_posture(state: &RedemptionAttemptState) -> RedemptionPosture {
    let closes_new_buy_admission = matches!(
        state,
        RedemptionAttemptState::SubmissionReserved { .. }
            | RedemptionAttemptState::InFlight { .. }
            | RedemptionAttemptState::Ambiguous { .. }
            | RedemptionAttemptState::Failed { .. }
            | RedemptionAttemptState::ConfirmedAwaitingBalance { .. }
    );
    let is_unresolved_attempt = matches!(
        state,
        RedemptionAttemptState::SubmissionReserved { .. }
            | RedemptionAttemptState::InFlight { .. }
            | RedemptionAttemptState::Ambiguous { .. }
            | RedemptionAttemptState::Failed { .. }
    );
    RedemptionPosture {
        closes_new_buy_admission,
        surface_prominently: is_unresolved_attempt
            && state.attempt_count() >= LIVE_REDEMPTION_SURFACE_AFTER_ATTEMPTS,
    }
}

/// Execution-owned status seam missing from the current venue transport. Implementations query
/// the already-issued transaction ID and never submit.
pub trait RedemptionStatusReader: Send + Sync {
    fn reconcile_transaction<'a>(
        &'a self,
        transaction_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<RedemptionStatusObservation, RedemptionStatusReadError>>
                + Send
                + 'a,
        >,
    >;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedemptionStatusObservation {
    Pending {
        evidence: Vec<RawHttpAttempt>,
    },
    Confirmed {
        transaction_hash: String,
        evidence: Vec<RawHttpAttempt>,
    },
    TerminalFailure {
        evidence: Vec<RawHttpAttempt>,
    },
    Ambiguous {
        evidence: Vec<RawHttpAttempt>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedemptionStatusReadError {
    pub evidence: Vec<RawHttpAttempt>,
}

pub struct RedemptionPassInput<'a> {
    pub attempt: RedemptionAttempt,
    pub now: OffsetDateTime,
    pub resolved_winner_redeemable: CollateralAmount,
    pub signed_request: Option<&'a SignedRedemptionRequest>,
    pub request_hash: Option<&'a str>,
    /// Service-owned backoff policy supplies the next allowed instant; this crate adds no number.
    pub retry_not_before_on_failure: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedemptionPassResult {
    pub attempt: RedemptionAttempt,
    pub actions: Vec<RedemptionAction>,
    pub failure: Option<RedemptionFailureKind>,
}

#[derive(Debug, thiserror::Error)]
pub enum RedemptionDriverError {
    #[error(transparent)]
    Journal(#[from] LiveJournalError),
    #[error("a positive redeemable balance requires a signed request and request hash")]
    MissingRequest,
    #[error("signed request does not match the durable attempt identity")]
    IdentityMismatch,
}

/// Run at most one external reconciliation or one new submission pass.
pub async fn run_redemption_pass<T: RedemptionTransport, R: RedemptionStatusReader>(
    transport: &T,
    status_reader: &R,
    journal: &LiveJournal,
    input: RedemptionPassInput<'_>,
) -> Result<RedemptionPassResult, RedemptionDriverError> {
    let request_hash = input.request_hash.unwrap_or_default().to_owned();
    let (state, actions) = advance(
        input.attempt.state.clone(),
        RedemptionEvent::ResolvedWinnerBalanceObserved {
            now: input.now,
            redeemable_balance: input.resolved_winner_redeemable,
            request_hash,
        },
    );
    let mut attempt = RedemptionAttempt {
        identity: input.attempt.identity.clone(),
        state,
    };
    let Some(action) = actions.first().cloned() else {
        return Ok(RedemptionPassResult {
            attempt,
            actions,
            failure: None,
        });
    };

    match action {
        RedemptionAction::SubmitOnce { request_hash } => {
            let request = input
                .signed_request
                .ok_or(RedemptionDriverError::MissingRequest)?;
            if request_hash.is_empty() {
                return Err(RedemptionDriverError::MissingRequest);
            }
            validate_request_identity(&attempt.identity, request)?;
            let attempt_count = attempt.state.attempt_count();
            let request_audit = redemption_request_audit(request, request_hash.clone());
            journal.append(
                attempt.identity.account_id.clone(),
                input.now,
                LiveJournalPayload::RedemptionRequested(Box::new(RedemptionRequestedAudit {
                    identity: attempt.identity.clone(),
                    attempt_count,
                    redeemable_balance: input.resolved_winner_redeemable,
                    request: request_audit,
                })),
            )?;

            match transport.submit_and_confirm(request).await {
                Ok(confirmed) => {
                    let evidence = confirmed
                        .evidence
                        .into_iter()
                        .map(RawHttpAttempt::Response)
                        .collect::<Vec<_>>();
                    journal_transaction(
                        journal,
                        &attempt,
                        input.now,
                        &confirmed.transaction_id,
                        &confirmed.submit_body_hash,
                        &evidence,
                    )?;
                    journal_receipt(
                        journal,
                        &attempt,
                        input.now,
                        &confirmed.transaction_id,
                        Some(confirmed.transaction_hash.clone()),
                        RedemptionReceiptStatusAudit::Confirmed,
                        evidence,
                    )?;
                    let (state, actions) = advance(
                        attempt.state,
                        RedemptionEvent::SubmissionConfirmed {
                            transaction_id: confirmed.transaction_id,
                            transaction_hash: confirmed.transaction_hash,
                            confirmed_at: input.now,
                        },
                    );
                    attempt.state = state;
                    Ok(RedemptionPassResult {
                        attempt,
                        actions,
                        failure: None,
                    })
                }
                Err(error) => handle_submission_error(journal, attempt, input, error),
            }
        }
        RedemptionAction::ReconcileExisting { transaction_id } => {
            let observation = status_reader.reconcile_transaction(&transaction_id).await;
            handle_status_observation(journal, attempt, input, transaction_id, observation)
        }
        RedemptionAction::WaitForBackoff { .. }
        | RedemptionAction::AwaitTransactionIdentity
        | RedemptionAction::ReconcileBalance
        | RedemptionAction::ProceedsSpendable { .. } => Ok(RedemptionPassResult {
            attempt,
            actions,
            failure: None,
        }),
    }
}

fn handle_submission_error(
    journal: &LiveJournal,
    mut attempt: RedemptionAttempt,
    input: RedemptionPassInput<'_>,
    error: RedemptionTransportError,
) -> Result<RedemptionPassResult, RedemptionDriverError> {
    match error {
        RedemptionTransportError::AmbiguousAfterSubmit {
            transaction_id,
            submit_body_hash,
            evidence,
            ..
        } => {
            let evidence = evidence
                .into_iter()
                .map(RawHttpAttempt::Response)
                .collect::<Vec<_>>();
            if let Some(transaction_id) = transaction_id.as_ref() {
                journal_transaction(
                    journal,
                    &attempt,
                    input.now,
                    transaction_id,
                    &submit_body_hash,
                    &evidence,
                )?;
                journal_receipt(
                    journal,
                    &attempt,
                    input.now,
                    transaction_id,
                    None,
                    RedemptionReceiptStatusAudit::Ambiguous,
                    evidence.clone(),
                )?;
            }
            let (state, actions) = advance(
                attempt.state,
                RedemptionEvent::SubmissionAmbiguous {
                    transaction_id,
                    submit_body_hash,
                },
            );
            attempt.state = state;
            Ok(RedemptionPassResult {
                attempt,
                actions,
                failure: Some(RedemptionFailureKind::Transport),
            })
        }
        RedemptionTransportError::TerminalFailure {
            transaction_id,
            evidence,
            ..
        } => {
            let evidence = evidence
                .into_iter()
                .map(RawHttpAttempt::Response)
                .collect::<Vec<_>>();
            journal_transaction(
                journal,
                &attempt,
                input.now,
                &transaction_id,
                "terminal-failure",
                &evidence,
            )?;
            journal_receipt(
                journal,
                &attempt,
                input.now,
                &transaction_id,
                None,
                RedemptionReceiptStatusAudit::TerminalFailure,
                evidence,
            )?;
            fail_attempt(
                attempt,
                input.retry_not_before_on_failure,
                RedemptionFailureKind::Terminal,
            )
        }
        RedemptionTransportError::Authentication { .. } => fail_attempt(
            attempt,
            input.retry_not_before_on_failure,
            RedemptionFailureKind::Authentication,
        ),
        RedemptionTransportError::Rejected { .. } => fail_attempt(
            attempt,
            input.retry_not_before_on_failure,
            RedemptionFailureKind::Rejected,
        ),
        RedemptionTransportError::Transport { .. } => fail_attempt(
            attempt,
            input.retry_not_before_on_failure,
            RedemptionFailureKind::Transport,
        ),
        RedemptionTransportError::Protocol { .. } => fail_attempt(
            attempt,
            input.retry_not_before_on_failure,
            RedemptionFailureKind::Protocol,
        ),
        RedemptionTransportError::Configuration(_)
        | RedemptionTransportError::UnsupportedCustody(_)
        | RedemptionTransportError::Request(_) => fail_attempt(
            attempt,
            input.retry_not_before_on_failure,
            RedemptionFailureKind::InvalidRequest,
        ),
    }
}

fn fail_attempt(
    mut attempt: RedemptionAttempt,
    retry_not_before: OffsetDateTime,
    failure: RedemptionFailureKind,
) -> Result<RedemptionPassResult, RedemptionDriverError> {
    let (state, actions) = advance(
        attempt.state,
        RedemptionEvent::SubmissionFailed {
            retry_not_before,
            failure,
        },
    );
    attempt.state = state;
    Ok(RedemptionPassResult {
        attempt,
        actions,
        failure: Some(failure),
    })
}

fn handle_status_observation(
    journal: &LiveJournal,
    mut attempt: RedemptionAttempt,
    input: RedemptionPassInput<'_>,
    transaction_id: String,
    observation: Result<RedemptionStatusObservation, RedemptionStatusReadError>,
) -> Result<RedemptionPassResult, RedemptionDriverError> {
    let (event, status, transaction_hash, evidence, failure) = match observation {
        Ok(RedemptionStatusObservation::Pending { evidence }) => (
            RedemptionEvent::ReconciliationPending,
            RedemptionReceiptStatusAudit::Pending,
            None,
            evidence,
            None,
        ),
        Ok(RedemptionStatusObservation::Confirmed {
            transaction_hash,
            evidence,
        }) => (
            RedemptionEvent::ReconciliationConfirmed {
                transaction_hash: transaction_hash.clone(),
                confirmed_at: input.now,
            },
            RedemptionReceiptStatusAudit::Confirmed,
            Some(transaction_hash),
            evidence,
            None,
        ),
        Ok(RedemptionStatusObservation::TerminalFailure { evidence }) => (
            RedemptionEvent::ReconciliationFailed {
                retry_not_before: input.retry_not_before_on_failure,
                failure: RedemptionFailureKind::Terminal,
            },
            RedemptionReceiptStatusAudit::TerminalFailure,
            None,
            evidence,
            Some(RedemptionFailureKind::Terminal),
        ),
        Ok(RedemptionStatusObservation::Ambiguous { evidence }) => (
            RedemptionEvent::ReconciliationAmbiguous,
            RedemptionReceiptStatusAudit::Ambiguous,
            None,
            evidence,
            Some(RedemptionFailureKind::Transport),
        ),
        Err(error) => (
            RedemptionEvent::ReconciliationAmbiguous,
            RedemptionReceiptStatusAudit::Ambiguous,
            None,
            error.evidence,
            Some(RedemptionFailureKind::Transport),
        ),
    };
    journal_receipt(
        journal,
        &attempt,
        input.now,
        &transaction_id,
        transaction_hash,
        status,
        evidence,
    )?;
    let (state, actions) = advance(attempt.state, event);
    attempt.state = state;
    Ok(RedemptionPassResult {
        attempt,
        actions,
        failure,
    })
}

fn validate_request_identity(
    identity: &RedemptionAttemptIdentity,
    request: &SignedRedemptionRequest,
) -> Result<(), RedemptionDriverError> {
    if identity.condition_id != request.call.condition_id
        || !identity.adapter.eq_ignore_ascii_case(&request.call.to)
        || !identity
            .custody_wallet
            .eq_ignore_ascii_case(&request.custody_wallet)
    {
        return Err(RedemptionDriverError::IdentityMismatch);
    }
    Ok(())
}

fn redemption_request_audit(
    request: &SignedRedemptionRequest,
    request_hash: String,
) -> RedemptionRequestAudit {
    RedemptionRequestAudit {
        call_to: request.call.to.clone(),
        calldata: request.call.calldata.clone(),
        condition_id: request.call.condition_id.clone(),
        neg_risk: request.call.neg_risk,
        custody: match request.custody {
            CustodyKind::DepositWallet => RedemptionCustodyAudit::DepositWallet,
            CustodyKind::Proxy => RedemptionCustodyAudit::Proxy,
            CustodyKind::Safe => RedemptionCustodyAudit::Safe,
            CustodyKind::Eoa => RedemptionCustodyAudit::Eoa,
        },
        signer_address: request.signer_address.clone(),
        custody_wallet: request.custody_wallet.clone(),
        deadline_unix: request.deadline_unix,
        metadata_hash: blake3::hash(request.metadata.as_bytes())
            .to_hex()
            .to_string(),
        request_hash,
        schema_version: REDEMPTION_SCHEMA_VERSION,
        parser_version: REDEMPTION_PARSER_VERSION,
        adapter_version: REDEMPTION_ADAPTER_VERSION.to_owned(),
    }
}

fn journal_transaction(
    journal: &LiveJournal,
    attempt: &RedemptionAttempt,
    now: OffsetDateTime,
    transaction_id: &str,
    submit_body_hash: &str,
    evidence: &[RawHttpAttempt],
) -> Result<(), LiveJournalError> {
    journal.append(
        attempt.identity.account_id.clone(),
        now,
        LiveJournalPayload::RedemptionTransactionIdentified(Box::new(RedemptionTransactionAudit {
            identity: attempt.identity.clone(),
            attempt_count: attempt.state.attempt_count(),
            transaction_id: transaction_id.to_owned(),
            submit_body_hash: submit_body_hash.to_owned(),
            evidence: evidence.to_vec(),
            evidence_hashes: http_attempt_hashes(evidence)?,
        })),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn journal_receipt(
    journal: &LiveJournal,
    attempt: &RedemptionAttempt,
    now: OffsetDateTime,
    transaction_id: &str,
    transaction_hash: Option<String>,
    status: RedemptionReceiptStatusAudit,
    evidence: Vec<RawHttpAttempt>,
) -> Result<(), LiveJournalError> {
    let evidence_hashes = http_attempt_hashes(&evidence)?;
    journal.append(
        attempt.identity.account_id.clone(),
        now,
        LiveJournalPayload::RedemptionReceiptTransition(Box::new(RedemptionReceiptAudit {
            identity: attempt.identity.clone(),
            attempt_count: attempt.state.attempt_count(),
            transaction_id: transaction_id.to_owned(),
            transaction_hash,
            status,
            evidence,
            evidence_hashes,
        })),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pe_core_types::{PolymarketConditionId, RawHttpResponse};
    use pe_venue_polymarket::{
        ConfirmedRedemption, RedemptionCall, RelayerNonce, RelayerSignatureParams,
        build_redemption_call,
    };
    use tempfile::tempdir;
    use time::Duration;
    use time::macros::datetime;

    use super::*;
    use crate::live_journal::{LiveJournalPayload, replay_account};

    const CONDITION: &str = "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const SIGNER: &str = "0x1111111111111111111111111111111111111111";
    const WALLET: &str = "0x2222222222222222222222222222222222222222";

    #[derive(Debug, Clone, Copy)]
    enum SubmitBehavior {
        Confirmed,
        Ambiguous,
        Rejected,
    }

    struct FixtureTransport {
        behavior: Mutex<SubmitBehavior>,
        submissions: AtomicUsize,
    }

    impl FixtureTransport {
        fn new(behavior: SubmitBehavior) -> Self {
            Self {
                behavior: Mutex::new(behavior),
                submissions: AtomicUsize::new(0),
            }
        }
    }

    impl RedemptionTransport for FixtureTransport {
        fn fetch_nonce<'a>(
            &'a self,
            _signer_address: &'a str,
            _custody: CustodyKind,
        ) -> Pin<Box<dyn Future<Output = Result<RelayerNonce, RedemptionTransportError>> + Send + 'a>>
        {
            Box::pin(async {
                Err(RedemptionTransportError::Configuration(
                    "fixture nonce is unused".to_owned(),
                ))
            })
        }

        fn submit_and_confirm<'a>(
            &'a self,
            _request: &'a SignedRedemptionRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<ConfirmedRedemption, RedemptionTransportError>>
                    + Send
                    + 'a,
            >,
        > {
            self.submissions.fetch_add(1, Ordering::SeqCst);
            let behavior = *self.behavior.lock().unwrap();
            Box::pin(async move {
                match behavior {
                    SubmitBehavior::Confirmed => Ok(ConfirmedRedemption {
                        transaction_id: "tx-1".to_owned(),
                        transaction_hash: "0xreceipt".to_owned(),
                        submit_body_hash: "body-hash".to_owned(),
                        evidence: vec![response("redemption-submit")],
                    }),
                    SubmitBehavior::Ambiguous => {
                        Err(RedemptionTransportError::AmbiguousAfterSubmit {
                            transaction_id: Some("tx-1".to_owned()),
                            submit_body_hash: "body-hash".to_owned(),
                            evidence: vec![response("redemption-submit")],
                            reason: "fixture timeout".to_owned(),
                        })
                    }
                    SubmitBehavior::Rejected => Err(RedemptionTransportError::Rejected {
                        phase: "submit",
                        status: 400,
                        response_body_hash: "rejection-hash".to_owned(),
                    }),
                }
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum StatusBehavior {
        Pending,
        Confirmed,
    }

    struct FixtureStatus {
        behavior: Mutex<StatusBehavior>,
        reads: AtomicUsize,
    }

    impl FixtureStatus {
        fn new(behavior: StatusBehavior) -> Self {
            Self {
                behavior: Mutex::new(behavior),
                reads: AtomicUsize::new(0),
            }
        }
    }

    impl RedemptionStatusReader for FixtureStatus {
        fn reconcile_transaction<'a>(
            &'a self,
            _transaction_id: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<RedemptionStatusObservation, RedemptionStatusReadError>>
                    + Send
                    + 'a,
            >,
        > {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let behavior = *self.behavior.lock().unwrap();
            Box::pin(async move {
                Ok(match behavior {
                    StatusBehavior::Pending => RedemptionStatusObservation::Pending {
                        evidence: vec![RawHttpAttempt::Response(response("redemption-status"))],
                    },
                    StatusBehavior::Confirmed => RedemptionStatusObservation::Confirmed {
                        transaction_hash: "0xreceipt".to_owned(),
                        evidence: vec![RawHttpAttempt::Response(response("redemption-status"))],
                    },
                })
            })
        }
    }

    fn now() -> OffsetDateTime {
        datetime!(2026-08-11 12:00 UTC)
    }

    fn response(endpoint_kind: &str) -> RawHttpResponse {
        RawHttpResponse {
            source_id: "fixture-relayer".to_owned(),
            endpoint_kind: endpoint_kind.to_owned(),
            method: "POST".to_owned(),
            path: "/fixture".to_owned(),
            ordered_query: Vec::new(),
            status: 200,
            headers: Vec::new(),
            body: br#"{"ok":true}"#.to_vec(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: now(),
            received_at: now(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: "fixture-v1".to_owned(),
        }
    }

    fn call() -> RedemptionCall {
        build_redemption_call(PolymarketConditionId(CONDITION.to_owned()), false).unwrap()
    }

    fn signed() -> SignedRedemptionRequest {
        SignedRedemptionRequest {
            call: call(),
            custody: CustodyKind::DepositWallet,
            signer_address: SIGNER.to_owned(),
            custody_wallet: WALLET.to_owned(),
            nonce: "7".to_owned(),
            signature: "0xsignature-never-journaled".to_owned(),
            deadline_unix: Some(2_000_000_000),
            signature_params: Some(RelayerSignatureParams {
                gas_price: "0".to_owned(),
                operation: "0".to_owned(),
                safe_txn_gas: "0".to_owned(),
                base_gas: "0".to_owned(),
                gas_token: "0x0000000000000000000000000000000000000000".to_owned(),
                refund_receiver: "0x0000000000000000000000000000000000000000".to_owned(),
            }),
            metadata: "redeem".to_owned(),
        }
    }

    fn attempt(state: RedemptionAttemptState) -> RedemptionAttempt {
        let call = call();
        RedemptionAttempt {
            identity: RedemptionAttemptIdentity {
                account_id: pe_core_types::AccountId::new("account").unwrap(),
                condition_id: call.condition_id,
                adapter: call.to,
                custody_wallet: WALLET.to_owned(),
            },
            state,
        }
    }

    fn input<'a>(
        attempt: RedemptionAttempt,
        at: OffsetDateTime,
        redeemable: CollateralAmount,
        request: Option<&'a SignedRedemptionRequest>,
    ) -> RedemptionPassInput<'a> {
        RedemptionPassInput {
            attempt,
            now: at,
            resolved_winner_redeemable: redeemable,
            signed_request: request,
            request_hash: Some("request-hash"),
            retry_not_before_on_failure: at + Duration::seconds(10),
        }
    }

    #[tokio::test]
    async fn resolved_winner_submits_and_journals_request_tx_receipt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let transport = FixtureTransport::new(SubmitBehavior::Confirmed);
        let status = FixtureStatus::new(StatusBehavior::Pending);
        let request = signed();
        let result = run_redemption_pass(
            &transport,
            &status,
            &journal,
            input(
                attempt(RedemptionAttemptState::default()),
                now(),
                CollateralAmount::from_atomic(1),
                Some(&request),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            result.attempt.state,
            RedemptionAttemptState::ConfirmedAwaitingBalance { .. }
        ));
        assert_eq!(transport.submissions.load(Ordering::SeqCst), 1);
        let events = replay_account(&path, &result.attempt.identity.account_id).unwrap();
        assert!(matches!(
            events.as_slice(),
            [
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::RedemptionRequested(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::RedemptionTransactionIdentified(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::RedemptionReceiptTransition(_),
                    ..
                }
            ]
        ));
        let encoded = serde_json::to_string(&events).unwrap();
        assert!(!encoded.contains("signature-never-journaled"));
        assert!(!encoded.contains("\"nonce\""));
    }

    #[tokio::test]
    async fn already_redeemed_zero_balance_is_a_noop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let transport = FixtureTransport::new(SubmitBehavior::Confirmed);
        let status = FixtureStatus::new(StatusBehavior::Pending);
        let result = run_redemption_pass(
            &transport,
            &status,
            &journal,
            input(
                attempt(RedemptionAttemptState::default()),
                now(),
                CollateralAmount::ZERO,
                None,
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.attempt.state, RedemptionAttemptState::default());
        assert_eq!(transport.submissions.load(Ordering::SeqCst), 0);
        assert!(
            replay_account(&path, &result.attempt.identity.account_id)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn restart_mid_flight_reconstructs_and_reconciles_without_resubmit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let transport = FixtureTransport::new(SubmitBehavior::Ambiguous);
        let status = FixtureStatus::new(StatusBehavior::Confirmed);
        let request = signed();
        let first = run_redemption_pass(
            &transport,
            &status,
            &journal,
            input(
                attempt(RedemptionAttemptState::default()),
                now(),
                CollateralAmount::from_atomic(1),
                Some(&request),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            first.attempt.state,
            RedemptionAttemptState::Ambiguous {
                transaction_id: Some(_),
                ..
            }
        ));
        let events = replay_account(&path, &first.attempt.identity.account_id).unwrap();
        let mut reconstructed = reconstruct_redemption_attempts(&events);
        let resumed = reconstructed.remove(&first.attempt.identity).unwrap();
        assert!(matches!(
            resumed.state,
            RedemptionAttemptState::Ambiguous {
                transaction_id: Some(_),
                ..
            }
        ));
        let second = run_redemption_pass(
            &transport,
            &status,
            &journal,
            input(
                resumed,
                now() + Duration::seconds(1),
                CollateralAmount::from_atomic(1),
                Some(&request),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            second.attempt.state,
            RedemptionAttemptState::ConfirmedAwaitingBalance { .. }
        ));
        assert_eq!(transport.submissions.load(Ordering::SeqCst), 1);
        assert_eq!(status.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn requested_without_transaction_identity_restarts_frozen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let original = attempt(RedemptionAttemptState::SubmissionReserved {
            attempt_count: 1,
            redeemable_balance: CollateralAmount::from_atomic(1),
            request_hash: "request-hash".to_owned(),
        });
        journal
            .append(
                original.identity.account_id.clone(),
                now(),
                LiveJournalPayload::RedemptionRequested(Box::new(RedemptionRequestedAudit {
                    identity: original.identity.clone(),
                    attempt_count: 1,
                    redeemable_balance: CollateralAmount::from_atomic(1),
                    request: redemption_request_audit(&signed(), "request-hash".to_owned()),
                })),
            )
            .unwrap();
        let events = replay_account(&path, &original.identity.account_id).unwrap();
        let resumed = reconstruct_redemption_attempts(&events)
            .remove(&original.identity)
            .unwrap();
        let transport = FixtureTransport::new(SubmitBehavior::Confirmed);
        let status = FixtureStatus::new(StatusBehavior::Pending);
        let result = run_redemption_pass(
            &transport,
            &status,
            &journal,
            input(resumed, now(), CollateralAmount::from_atomic(1), None),
        )
        .await
        .unwrap();
        assert!(matches!(
            result.actions.as_slice(),
            [RedemptionAction::AwaitTransactionIdentity]
        ));
        assert_eq!(transport.submissions.load(Ordering::SeqCst), 0);
        assert_eq!(status.reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restart_with_in_flight_attempt_resumes_without_duplicate_submit() {
        let dir = tempdir().unwrap();
        let journal = LiveJournal::open(dir.path().join("live.log")).unwrap();
        let transport = FixtureTransport::new(SubmitBehavior::Confirmed);
        let status = FixtureStatus::new(StatusBehavior::Pending);
        let resumed = attempt(RedemptionAttemptState::InFlight {
            attempt_count: 1,
            transaction_id: "tx-existing".to_owned(),
            submit_body_hash: "body-existing".to_owned(),
        });
        let result = run_redemption_pass(
            &transport,
            &status,
            &journal,
            input(resumed, now(), CollateralAmount::from_atomic(1), None),
        )
        .await
        .unwrap();
        assert!(matches!(
            result.attempt.state,
            RedemptionAttemptState::InFlight { .. }
        ));
        assert_eq!(transport.submissions.load(Ordering::SeqCst), 0);
        assert_eq!(status.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_submission_honors_backoff_and_surfaces_after_three_attempts() {
        let dir = tempdir().unwrap();
        let journal = LiveJournal::open(dir.path().join("live.log")).unwrap();
        let transport = FixtureTransport::new(SubmitBehavior::Rejected);
        let status = FixtureStatus::new(StatusBehavior::Pending);
        let request = signed();
        let mut current = attempt(RedemptionAttemptState::default());
        let mut at = now();
        for ordinal in 1..=3 {
            let failed = run_redemption_pass(
                &transport,
                &status,
                &journal,
                input(
                    current,
                    at,
                    CollateralAmount::from_atomic(1),
                    Some(&request),
                ),
            )
            .await
            .unwrap();
            assert!(matches!(
                failed.attempt.state,
                RedemptionAttemptState::Failed { .. }
            ));
            assert_eq!(failed.attempt.state.attempt_count(), ordinal);

            let waiting = run_redemption_pass(
                &transport,
                &status,
                &journal,
                input(
                    failed.attempt.clone(),
                    at + Duration::seconds(5),
                    CollateralAmount::from_atomic(1),
                    Some(&request),
                ),
            )
            .await
            .unwrap();
            assert!(matches!(
                waiting.actions.as_slice(),
                [RedemptionAction::WaitForBackoff { .. }]
            ));
            assert_eq!(
                transport.submissions.load(Ordering::SeqCst),
                ordinal as usize
            );
            current = failed.attempt;
            at += Duration::seconds(11);
        }
        let posture = redemption_posture(&current.state);
        assert!(posture.closes_new_buy_admission);
        assert!(posture.surface_prominently);
    }

    #[test]
    fn posture_reopens_only_after_confirmed_receipt_and_balance_reconcile() {
        let pending = RedemptionAttemptState::InFlight {
            attempt_count: 1,
            transaction_id: "tx-1".to_owned(),
            submit_body_hash: "body".to_owned(),
        };
        assert!(redemption_posture(&pending).closes_new_buy_admission);
        let (confirmed, actions) = advance(
            pending,
            RedemptionEvent::ReconciliationConfirmed {
                transaction_hash: "0xreceipt".to_owned(),
                confirmed_at: now(),
            },
        );
        assert!(redemption_posture(&confirmed).closes_new_buy_admission);
        assert!(matches!(
            actions.as_slice(),
            [RedemptionAction::ReconcileBalance]
        ));
        let (complete, actions) = advance(
            confirmed,
            RedemptionEvent::BalanceReconciled {
                reconciled_at: now(),
                credited_collateral: CollateralAmount::from_atomic(1_000_000),
                remaining_redeemable: CollateralAmount::ZERO,
            },
        );
        assert!(!redemption_posture(&complete).closes_new_buy_admission);
        assert!(matches!(
            actions.as_slice(),
            [RedemptionAction::ProceedsSpendable {
                credited_collateral
            }] if *credited_collateral == CollateralAmount::from_atomic(1_000_000)
        ));
    }

    #[test]
    fn any_positive_redeemable_amount_has_no_dust_floor() {
        let (state, actions) = advance(
            RedemptionAttemptState::default(),
            RedemptionEvent::ResolvedWinnerBalanceObserved {
                now: now(),
                redeemable_balance: CollateralAmount::from_atomic(1),
                request_hash: "request".to_owned(),
            },
        );
        assert!(matches!(
            state,
            RedemptionAttemptState::SubmissionReserved { .. }
        ));
        assert!(matches!(
            actions.as_slice(),
            [RedemptionAction::SubmitOnce { .. }]
        ));
    }
}
