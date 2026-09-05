//! Ordinary per-account live execution with a durable prepare-before-POST boundary.

use std::future::Future;
use std::pin::Pin;

use pe_core_types::{
    AccountId, CollateralAmount, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price,
    RawHttpAttempt, RawHttpResponse, RawTransportFailure, ShareAmount, TransportErrorClass,
};
use pe_resolver_card::{VenueSettlementError, VenueSettlementRecord};
use pe_source_polymarket_public::{
    LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, LiveMarketError, LiveMarketEvidence,
};
use pe_venue_polymarket::{CompactFeeSchedule, LadderPlan, PreparedPolymarketBuy};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use crate::live_journal::{
    AdmissionReceipts, CredentialBindingIdentity, LiveAccountReadFailure, LiveAccountStateAudit,
    LiveAdmissionEvaluationAudit, LiveAdmissionRefusal, LiveAdmissionVerdict, LiveControlMode,
    LiveExecutedAmounts, LiveJournal, LiveJournalError, LiveJournalOrderOutcome,
    LiveJournalPayload, LiveOrderAmbiguityKind, LiveOrderIdentity, LiveOrderPostAudit,
    LiveOrderPreparationFailedAudit, LiveOrderPreparationFailure, LiveOrderPreparedAudit,
    LiveOrderReconciliationAudit, LiveOrderRejectKind, LiveReconciliationSource, http_attempt_hash,
    http_attempt_hashes,
};

/// Frozen per-target account and credential identity supplied by the dispatch aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenLiveTarget {
    pub account_id: AccountId,
    pub credential_binding: CredentialBindingIdentity,
}

/// Current requested/effective control-plane state. Both values must be `live_tiny`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveModeSnapshot {
    pub requested: LiveControlMode,
    pub effective: LiveControlMode,
}

impl LiveModeSnapshot {
    #[must_use]
    pub const fn is_armed(self) -> bool {
        matches!(self.requested, LiveControlMode::LiveTiny)
            && matches!(self.effective, LiveControlMode::LiveTiny)
    }
}

/// Frozen ordinary live admission evidence composed by the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveAdmissionArtifact {
    pub market: LiveMarketEvidence,
    pub settlement: VenueSettlementRecord,
    pub fee_schedule: CompactFeeSchedule,
    pub receipts: AdmissionReceipts,
}

/// One complete frozen order target presented to [`LiveExecutor::prepare`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveOrderRequest {
    pub target: FrozenLiveTarget,
    pub current_credential_binding: CredentialBindingIdentity,
    pub mode: LiveModeSnapshot,
    pub identity: LiveOrderIdentity,
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub admission: LiveAdmissionArtifact,
    pub ladder: LadderPlan,
    pub economic: crate::economic::EconomicPrepared,
}

/// Venue-neutral negRisk-aware preparation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenuePrepareRequest {
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub neg_risk: bool,
    pub limit_price: Price,
    pub shares: ShareAmount,
    pub maximum_collateral: CollateralAmount,
    pub tick_size: Price,
    pub metadata_hashes: Vec<String>,
}

/// Opaque POST capability paired with the complete sanitized prepared-order audit record.
pub struct LiveVenuePrepared<S> {
    audit: PreparedPolymarketBuy,
    submission: S,
}

impl<S> LiveVenuePrepared<S> {
    #[must_use]
    pub fn new(audit: PreparedPolymarketBuy, submission: S) -> Self {
        Self { audit, submission }
    }

    #[must_use]
    pub fn audit(&self) -> &PreparedPolymarketBuy {
        &self.audit
    }

    fn into_parts(self) -> (PreparedPolymarketBuy, S) {
        (self.audit, self.submission)
    }
}

/// Fresh authenticated venue state for the negRisk-selected spender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueAccountState {
    pub observed_at: OffsetDateTime,
    pub closed_only: bool,
    pub geoblocked: bool,
    pub selected_spender: String,
    pub collateral_balance: CollateralAmount,
    pub allowance: CollateralAmount,
    pub reconciled_free_collateral: CollateralAmount,
    pub schema_version: u16,
    pub parser_version: u16,
    pub evidence: Vec<RawHttpAttempt>,
}

impl LiveVenueAccountState {
    fn audit(&self) -> Result<LiveAccountStateAudit, LiveJournalError> {
        Ok(LiveAccountStateAudit {
            observed_at: self.observed_at,
            closed_only: self.closed_only,
            geoblocked: self.geoblocked,
            selected_spender: self.selected_spender.clone(),
            collateral_balance: self.collateral_balance,
            allowance: self.allowance,
            reconciled_free_collateral: self.reconciled_free_collateral,
            schema_version: self.schema_version,
            parser_version: self.parser_version,
            evidence: self.evidence.clone(),
            evidence_hashes: http_attempt_hashes(&self.evidence)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueAccountReadError {
    pub kind: LiveAccountReadFailure,
    pub evidence: Vec<RawHttpAttempt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LiveVenuePreparationError {
    #[error("venue order preparation failed")]
    Venue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LivePostParseError {
    #[error("order POST response could not be classified")]
    InvalidResponse,
}

/// Classification of the one raw POST response before any reconciliation fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivePostClassification {
    Matched {
        venue_order_id: String,
        executed: LiveExecutedAmounts,
        transaction_hashes: Vec<String>,
    },
    Killed {
        venue_order_id: Option<String>,
    },
    Rejected {
        venue_order_id: Option<String>,
    },
    Ambiguous {
        kind: LiveOrderAmbiguityKind,
    },
}

/// Result of the order-hash lookup and cancel-unexpected-order seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveVenueReconciledOutcome {
    Matched {
        venue_order_id: String,
        transaction_hashes: Vec<String>,
    },
    Killed {
        venue_order_id: Option<String>,
    },
    Rejected {
        venue_order_id: Option<String>,
    },
    Ambiguous {
        kind: LiveOrderAmbiguityKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueReconciliation {
    pub outcome: LiveVenueReconciledOutcome,
    pub evidence: Vec<RawHttpAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVenueReconciliationError {
    pub evidence: Vec<RawHttpAttempt>,
}

pub type LiveVenuePrepareFuture<'a, S> = Pin<
    Box<dyn Future<Output = Result<LiveVenuePrepared<S>, LiveVenuePreparationError>> + Send + 'a>,
>;

pub type LivePostFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + 'a>>;

pub type LiveReconciliationFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
            + Send
            + 'a,
    >,
>;

pub type LiveAccountStateFuture<'a> = Pin<
    Box<dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>> + Send + 'a>,
>;

/// Venue seam owned by execution-core. Implementations must perform exactly one request in
/// `post_once`; `reconcile_and_cancel_by_order_hash` must locate by signed order hash and cancel
/// any unexpected live order before reporting a terminal no-fill.
pub trait LiveOrderVenue: Send + Sync {
    type Submission: Send;

    fn prepare<'a>(
        &'a self,
        request: LiveVenuePrepareRequest,
    ) -> LiveVenuePrepareFuture<'a, Self::Submission>;

    fn post_once<'a>(&'a self, submission: Self::Submission) -> LivePostFuture<'a>;

    fn classify_post_response(
        &self,
        response: &RawHttpResponse,
    ) -> Result<LivePostClassification, LivePostParseError>;

    fn reconcile_and_cancel_by_order_hash<'a>(
        &'a self,
        order_hash: &'a str,
    ) -> LiveReconciliationFuture<'a>;

    fn read_balance_and_allowance<'a>(&'a self, neg_risk: bool) -> LiveAccountStateFuture<'a>;
}

/// Result of phase one. `Prepared` is the only variant carrying a POST capability.
pub enum LivePrepareResult<S> {
    Prepared(PreparedLiveOrder<S>),
    Terminal(LiveOrderOutcome),
}

/// Prepared order returned only after its full audit record has been fsynced.
pub struct PreparedLiveOrder<S> {
    account_id: AccountId,
    identity: LiveOrderIdentity,
    order_hash: String,
    prepared_audit: Box<LiveOrderPreparedAudit>,
    submission: S,
}

impl<S> PreparedLiveOrder<S> {
    #[must_use]
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    #[must_use]
    pub fn identity(&self) -> &LiveOrderIdentity {
        &self.identity
    }

    #[must_use]
    pub fn order_hash(&self) -> &str {
        &self.order_hash
    }

    #[must_use]
    pub fn audit(&self) -> &LiveOrderPreparedAudit {
        &self.prepared_audit
    }
}

/// Per-target outcome mapped by the service onto `dispatch_targets` lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveOrderOutcome {
    Refused {
        reason: LiveAdmissionRefusal,
    },
    Matched {
        order_hash: String,
        venue_order_id: String,
        transaction_hashes: Vec<String>,
        /// Retained for legacy audit only; receipt logs own financial projection.
        executed: Option<LiveExecutedAmounts>,
    },
    Killed {
        order_hash: String,
        venue_order_id: Option<String>,
    },
    Rejected {
        order_hash: Option<String>,
        venue_order_id: Option<String>,
        kind: LiveOrderRejectKind,
    },
    Ambiguous {
        order_hash: String,
        kind: LiveOrderAmbiguityKind,
        reconcile_first: bool,
    },
}

impl LiveOrderOutcome {
    /// Coarse dispatch state expected by `paper-state`.
    #[must_use]
    pub const fn dispatch_state(&self) -> &'static str {
        match self {
            Self::Matched { .. } => "submitted",
            Self::Ambiguous { .. } => "ambiguous",
            Self::Refused { .. } | Self::Killed { .. } | Self::Rejected { .. } => "terminal",
        }
    }

    /// Stable terminal detail for service persistence. Ambiguous results are nonterminal.
    #[must_use]
    pub const fn terminal_reason(&self) -> Option<&'static str> {
        match self {
            Self::Refused {
                reason: LiveAdmissionRefusal::CredentialVersionChanged,
            } => Some("credential_version_changed"),
            Self::Refused { .. } => Some("admission_refused"),
            Self::Matched { .. } => None,
            Self::Killed { .. } => Some("killed"),
            Self::Rejected { .. } => Some("rejected"),
            Self::Ambiguous { .. } => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LiveExecutorError {
    #[error(transparent)]
    Journal(#[from] LiveJournalError),
}

pub struct LiveExecutor<'a, V: LiveOrderVenue> {
    venue: &'a V,
    journal: &'a LiveJournal,
}

impl<'a, V: LiveOrderVenue> LiveExecutor<'a, V> {
    #[must_use]
    pub const fn new(venue: &'a V, journal: &'a LiveJournal) -> Self {
        Self { venue, journal }
    }

    /// Phase one: evaluate ordered admission checks, prepare, and fsync the full audit record.
    pub async fn prepare(
        &self,
        request: LiveOrderRequest,
        now: OffsetDateTime,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError> {
        let economic = request.economic.clone();

        // The per-account kill is local and therefore precedes any authenticated venue I/O.
        if !request.mode.is_armed() {
            return self.refuse(
                &request,
                now,
                economic,
                None,
                Vec::new(),
                LiveAdmissionRefusal::ModeNotArmed,
            );
        }

        // Ordered admission check 1: exact credential binding.
        if request.target.credential_binding != request.current_credential_binding {
            self.journal.append(
                request.target.account_id.clone(),
                now,
                LiveJournalPayload::CredentialBindingMismatch {
                    frozen: request.target.credential_binding.clone(),
                    current: request.current_credential_binding.clone(),
                },
            )?;
            return self.refuse(
                &request,
                now,
                economic,
                None,
                Vec::new(),
                LiveAdmissionRefusal::CredentialVersionChanged,
            );
        }

        // Ordered admission check 2: both artifact halves and the frozen quote/ladder.
        if let Err(reason) = validate_artifact_and_ladder(&request, now) {
            return self.refuse(&request, now, economic, None, Vec::new(), reason);
        }

        let account = match self
            .venue
            .read_balance_and_allowance(request.admission.market.neg_risk)
            .await
        {
            Ok(account) => account,
            Err(error) => {
                return self.refuse(
                    &request,
                    now,
                    economic,
                    None,
                    error.evidence,
                    LiveAdmissionRefusal::AccountStateUnavailable(error.kind),
                );
            }
        };
        let account_audit = account.audit()?;

        // Ordered checks 3 and 4: account state, then same-egress geoblock.
        if account.closed_only {
            return self.refuse(
                &request,
                now,
                economic,
                Some(account_audit),
                Vec::new(),
                LiveAdmissionRefusal::AccountClosedOnly,
            );
        }
        if account.geoblocked {
            return self.refuse(
                &request,
                now,
                economic,
                Some(account_audit),
                Vec::new(),
                LiveAdmissionRefusal::Geoblocked,
            );
        }

        // Ordered check 5: balance and selected-spender allowance.
        let required = request.ladder.worst_case_debit;
        if account.collateral_balance < required {
            return self.refuse(
                &request,
                now,
                economic,
                Some(account_audit),
                Vec::new(),
                LiveAdmissionRefusal::InsufficientBalance {
                    required,
                    available: account.collateral_balance,
                },
            );
        }
        if account.allowance < required {
            return self.refuse(
                &request,
                now,
                economic,
                Some(account_audit),
                Vec::new(),
                LiveAdmissionRefusal::InsufficientAllowance {
                    required,
                    available: account.allowance,
                },
            );
        }

        // Ordered check 6: reconciled free collateral is a distinct bound.
        if account.reconciled_free_collateral < required {
            return self.refuse(
                &request,
                now,
                economic,
                Some(account_audit),
                Vec::new(),
                LiveAdmissionRefusal::WorstCaseDebitExceedsFreeCollateral {
                    required,
                    available: account.reconciled_free_collateral,
                },
            );
        }

        self.journal_admission(
            &request,
            now,
            economic.clone(),
            Some(account_audit.clone()),
            Vec::new(),
            LiveAdmissionVerdict::Approved,
        )?;

        let venue_request = LiveVenuePrepareRequest {
            condition_id: request.condition_id.clone(),
            outcome_id: request.outcome_id,
            token_id: request.token_id.clone(),
            neg_risk: request.admission.market.neg_risk,
            limit_price: request.ladder.limit_price,
            shares: request.ladder.shares,
            maximum_collateral: request.ladder.worst_case_debit,
            tick_size: request.admission.market.minimum_tick_size,
            metadata_hashes: request.identity.evidence_hashes.clone(),
        };
        let venue_prepared = match self.venue.prepare(venue_request).await {
            Ok(prepared) => prepared,
            Err(_) => {
                self.journal.append(
                    request.target.account_id.clone(),
                    now,
                    LiveJournalPayload::OrderPreparationFailed(Box::new(
                        LiveOrderPreparationFailedAudit {
                            identity: request.identity,
                            failure: LiveOrderPreparationFailure::Venue,
                        },
                    )),
                )?;
                return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                    order_hash: None,
                    venue_order_id: None,
                    kind: LiveOrderRejectKind::PreparationFailed,
                }));
            }
        };
        if !prepared_matches_request(venue_prepared.audit(), &request, &account) {
            self.journal.append(
                request.target.account_id.clone(),
                now,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    LiveOrderPreparationFailedAudit {
                        identity: request.identity,
                        failure: LiveOrderPreparationFailure::PreparedAuditMismatch,
                    },
                )),
            )?;
            return Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Rejected {
                order_hash: None,
                venue_order_id: None,
                kind: LiveOrderRejectKind::PreparationFailed,
            }));
        }

        let (prepared, submission) = venue_prepared.into_parts();
        let prepared_audit = LiveOrderPreparedAudit::new(
            request.identity.clone(),
            request.target.credential_binding,
            economic,
            account_audit,
            prepared.clone(),
        );
        self.journal.append(
            request.target.account_id.clone(),
            now,
            LiveJournalPayload::OrderPrepared(Box::new(prepared_audit.clone())),
        )?;
        Ok(LivePrepareResult::Prepared(PreparedLiveOrder {
            account_id: request.target.account_id,
            identity: request.identity,
            order_hash: prepared.order_hash,
            prepared_audit: Box::new(prepared_audit),
            submission,
        }))
    }

    /// Phase two: consume the POST capability exactly once and classify/reconcile its result.
    pub async fn submit(
        &self,
        prepared: PreparedLiveOrder<V::Submission>,
        now: OffsetDateTime,
    ) -> Result<LiveOrderOutcome, LiveExecutorError> {
        let PreparedLiveOrder {
            account_id,
            identity,
            order_hash,
            submission,
            ..
        } = prepared;
        let post = self.venue.post_once(submission).await;
        match post {
            Err(failure) => {
                let kind = if failure.error_class == TransportErrorClass::Timeout {
                    LiveOrderAmbiguityKind::Timeout
                } else {
                    LiveOrderAmbiguityKind::Transport
                };
                let attempt = RawHttpAttempt::TransportFailure(failure);
                self.journal_post(&account_id, &identity, &order_hash, now, attempt)?;
                self.reconcile_ambiguous(account_id, identity, order_hash, now, kind)
                    .await
            }
            Ok(response) => {
                let attempt = RawHttpAttempt::Response(response.clone());
                self.journal_post(&account_id, &identity, &order_hash, now, attempt.clone())?;
                match self.venue.classify_post_response(&response) {
                    Ok(LivePostClassification::Matched {
                        venue_order_id,
                        executed,
                        transaction_hashes,
                    }) => {
                        let journal_outcome = LiveJournalOrderOutcome::Matched {
                            venue_order_id: venue_order_id.clone(),
                            transaction_hashes: transaction_hashes.clone(),
                            executed: Some(executed.clone()),
                        };
                        self.journal_reconciliation(
                            &account_id,
                            &identity,
                            &order_hash,
                            now,
                            LiveReconciliationSource::PostResponse,
                            journal_outcome,
                            vec![attempt],
                        )?;
                        Ok(LiveOrderOutcome::Matched {
                            order_hash,
                            venue_order_id,
                            transaction_hashes,
                            executed: Some(executed),
                        })
                    }
                    Ok(LivePostClassification::Killed { venue_order_id }) => {
                        let journal_outcome = LiveJournalOrderOutcome::Killed {
                            venue_order_id: venue_order_id.clone(),
                        };
                        self.journal_reconciliation(
                            &account_id,
                            &identity,
                            &order_hash,
                            now,
                            LiveReconciliationSource::PostResponse,
                            journal_outcome,
                            vec![attempt],
                        )?;
                        Ok(LiveOrderOutcome::Killed {
                            order_hash,
                            venue_order_id,
                        })
                    }
                    Ok(LivePostClassification::Rejected { venue_order_id }) => {
                        let journal_outcome = LiveJournalOrderOutcome::Rejected {
                            venue_order_id: venue_order_id.clone(),
                            kind: LiveOrderRejectKind::VenueRejected,
                        };
                        self.journal_reconciliation(
                            &account_id,
                            &identity,
                            &order_hash,
                            now,
                            LiveReconciliationSource::PostResponse,
                            journal_outcome,
                            vec![attempt],
                        )?;
                        Ok(LiveOrderOutcome::Rejected {
                            order_hash: Some(order_hash),
                            venue_order_id,
                            kind: LiveOrderRejectKind::VenueRejected,
                        })
                    }
                    Ok(LivePostClassification::Ambiguous { kind }) => {
                        self.reconcile_ambiguous(account_id, identity, order_hash, now, kind)
                            .await
                    }
                    Err(_) => {
                        self.reconcile_ambiguous(
                            account_id,
                            identity,
                            order_hash,
                            now,
                            LiveOrderAmbiguityKind::UnexpectedResponse,
                        )
                        .await
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn refuse(
        &self,
        request: &LiveOrderRequest,
        now: OffsetDateTime,
        economic: crate::economic::EconomicPrepared,
        account_state: Option<LiveAccountStateAudit>,
        failure_evidence: Vec<RawHttpAttempt>,
        reason: LiveAdmissionRefusal,
    ) -> Result<LivePrepareResult<V::Submission>, LiveExecutorError> {
        self.journal_admission(
            request,
            now,
            economic,
            account_state,
            failure_evidence,
            LiveAdmissionVerdict::Refused(reason.clone()),
        )?;
        Ok(LivePrepareResult::Terminal(LiveOrderOutcome::Refused {
            reason,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn journal_admission(
        &self,
        request: &LiveOrderRequest,
        now: OffsetDateTime,
        economic: crate::economic::EconomicPrepared,
        account_state: Option<LiveAccountStateAudit>,
        failure_evidence: Vec<RawHttpAttempt>,
        verdict: LiveAdmissionVerdict,
    ) -> Result<(), LiveJournalError> {
        let failure_hashes = http_attempt_hashes(&failure_evidence)?;
        self.journal.append(
            request.target.account_id.clone(),
            now,
            LiveJournalPayload::AdmissionEvaluated(Box::new(LiveAdmissionEvaluationAudit {
                identity: request.identity.clone(),
                frozen_binding: request.target.credential_binding.clone(),
                current_binding: request.current_credential_binding.clone(),
                requested_mode: request.mode.requested,
                effective_mode: request.mode.effective,
                economic,
                account_state,
                account_read_failure_evidence: failure_evidence,
                account_read_failure_evidence_hashes: failure_hashes,
                verdict,
            })),
        )?;
        Ok(())
    }

    fn journal_post(
        &self,
        account_id: &AccountId,
        identity: &LiveOrderIdentity,
        order_hash: &str,
        now: OffsetDateTime,
        evidence: RawHttpAttempt,
    ) -> Result<(), LiveJournalError> {
        let evidence_hash = http_attempt_hash(&evidence)?;
        self.journal.append(
            account_id.clone(),
            now,
            LiveJournalPayload::OrderPosted(Box::new(LiveOrderPostAudit {
                identity: identity.clone(),
                order_hash: order_hash.to_owned(),
                evidence,
                evidence_hash,
            })),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn journal_reconciliation(
        &self,
        account_id: &AccountId,
        identity: &LiveOrderIdentity,
        order_hash: &str,
        now: OffsetDateTime,
        source: LiveReconciliationSource,
        outcome: LiveJournalOrderOutcome,
        evidence: Vec<RawHttpAttempt>,
    ) -> Result<(), LiveJournalError> {
        let evidence_hashes = http_attempt_hashes(&evidence)?;
        self.journal.append(
            account_id.clone(),
            now,
            LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                identity: identity.clone(),
                order_hash: order_hash.to_owned(),
                source,
                outcome,
                evidence,
                evidence_hashes,
            })),
        )?;
        Ok(())
    }

    async fn reconcile_ambiguous(
        &self,
        account_id: AccountId,
        identity: LiveOrderIdentity,
        order_hash: String,
        now: OffsetDateTime,
        initial_kind: LiveOrderAmbiguityKind,
    ) -> Result<LiveOrderOutcome, LiveExecutorError> {
        let reconciliation = self
            .venue
            .reconcile_and_cancel_by_order_hash(&order_hash)
            .await;
        let (outcome, evidence) = match reconciliation {
            Ok(reconciliation) => (reconciliation.outcome, reconciliation.evidence),
            Err(error) => {
                let journal_outcome = LiveJournalOrderOutcome::Ambiguous {
                    kind: LiveOrderAmbiguityKind::ReconciliationUnavailable,
                };
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    now,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    journal_outcome,
                    error.evidence,
                )?;
                return Ok(LiveOrderOutcome::Ambiguous {
                    order_hash,
                    kind: initial_kind,
                    reconcile_first: true,
                });
            }
        };
        match outcome {
            LiveVenueReconciledOutcome::Matched {
                venue_order_id,
                transaction_hashes,
            } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    now,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Matched {
                        venue_order_id: venue_order_id.clone(),
                        transaction_hashes: transaction_hashes.clone(),
                        executed: None,
                    },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Matched {
                    order_hash,
                    venue_order_id,
                    transaction_hashes,
                    executed: None,
                })
            }
            LiveVenueReconciledOutcome::Killed { venue_order_id } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    now,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Killed {
                        venue_order_id: venue_order_id.clone(),
                    },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Killed {
                    order_hash,
                    venue_order_id,
                })
            }
            LiveVenueReconciledOutcome::Rejected { venue_order_id } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    now,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Rejected {
                        venue_order_id: venue_order_id.clone(),
                        kind: LiveOrderRejectKind::VenueRejected,
                    },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Rejected {
                    order_hash: Some(order_hash),
                    venue_order_id,
                    kind: LiveOrderRejectKind::VenueRejected,
                })
            }
            LiveVenueReconciledOutcome::Ambiguous { kind } => {
                self.journal_reconciliation(
                    &account_id,
                    &identity,
                    &order_hash,
                    now,
                    LiveReconciliationSource::OrderHashLookupAndCancel,
                    LiveJournalOrderOutcome::Ambiguous { kind },
                    evidence,
                )?;
                Ok(LiveOrderOutcome::Ambiguous {
                    order_hash,
                    kind: initial_kind,
                    reconcile_first: true,
                })
            }
        }
    }
}

fn validate_artifact_and_ladder(
    request: &LiveOrderRequest,
    now: OffsetDateTime,
) -> Result<(), LiveAdmissionRefusal> {
    let market = &request.admission.market;
    if market.schema_version != LIVE_MARKET_SCHEMA_VERSION
        || market.parser_version != LIVE_MARKET_PARSER_VERSION
        || market.minimum_tick_size == Price::ZERO
        || market.minimum_order_size == ShareAmount::ZERO
    {
        return Err(LiveAdmissionRefusal::MarketEvidenceInvalid);
    }
    market
        .validate_fresh(now.unix_timestamp())
        .map_err(|error| match error {
            LiveMarketError::Stale | LiveMarketError::FutureObservation => {
                LiveAdmissionRefusal::MarketEvidenceStale
            }
            _ => LiveAdmissionRefusal::MarketEvidenceInvalid,
        })?;
    request
        .admission
        .settlement
        .validate_fresh_for_entry(now)
        .map_err(|error| match error {
            VenueSettlementError::Stale | VenueSettlementError::Schema(_) => {
                LiveAdmissionRefusal::SettlementEvidenceStale
            }
            VenueSettlementError::AlreadyResolved => LiveAdmissionRefusal::MarketAlreadyResolved,
            VenueSettlementError::Ambiguous => LiveAdmissionRefusal::SettlementAmbiguous,
        })?;

    let outcome_index = usize::from(request.outcome_id.0);
    if request.identity.dispatch_id.trim().is_empty()
        || request.identity.idempotency_key.trim().is_empty()
        || request.identity.quote_id.trim().is_empty()
        || request.identity.config_hash.trim().is_empty()
        || request.identity.decision_hash.trim().is_empty()
        || request.identity.evidence_hashes.is_empty()
        || request.identity.schema_version == 0
        || request.identity.parser_version == 0
        || market.condition_id != request.condition_id
        || request.admission.settlement.condition_id != request.condition_id
        || market
            .ordered_outcome_token_ids
            .get(outcome_index)
            .is_none_or(|token| token != &request.token_id)
    {
        return Err(LiveAdmissionRefusal::ArtifactIdentityMismatch);
    }
    validate_ladder(
        &request.ladder,
        market.minimum_tick_size,
        market.minimum_order_size,
    )
}

fn validate_ladder(
    plan: &LadderPlan,
    minimum_tick_size: Price,
    minimum_order_size: ShareAmount,
) -> Result<(), LiveAdmissionRefusal> {
    if plan.used_asks.is_empty()
        || plan.shares < minimum_order_size
        || plan.best_ask == Price::ZERO
        || plan.limit_price == Price::ZERO
        || plan.best_ask > plan.limit_price
        || plan.limit_price.0 % minimum_tick_size.0 != Decimal::ZERO
        || plan.estimated_ladder_spend > plan.worst_case_debit
    {
        return Err(LiveAdmissionRefusal::LadderInvalid);
    }
    let mut shares = ShareAmount::ZERO;
    let mut spend = Decimal::ZERO;
    let mut previous = None;
    for ask in &plan.used_asks {
        if ask.price == Price::ZERO
            || ask.shares == ShareAmount::ZERO
            || previous.is_some_and(|price| ask.price < price)
        {
            return Err(LiveAdmissionRefusal::LadderInvalid);
        }
        shares = shares
            .checked_add(ask.shares)
            .map_err(|_| LiveAdmissionRefusal::LadderInvalid)?;
        spend = spend
            .checked_add(ask.shares.to_decimal() * ask.price.0)
            .ok_or(LiveAdmissionRefusal::LadderInvalid)?;
        previous = Some(ask.price);
    }
    let expected_spend = CollateralAmount::from_decimal_exact(spend)
        .map_err(|_| LiveAdmissionRefusal::LadderInvalid)?;
    let expected_worst =
        CollateralAmount::from_decimal_exact(plan.shares.to_decimal() * plan.limit_price.0)
            .map_err(|_| LiveAdmissionRefusal::LadderInvalid)?;
    if shares != plan.shares
        || expected_spend != plan.estimated_ladder_spend
        || expected_worst != plan.worst_case_debit
        || plan.best_ask != plan.used_asks[0].price
        || plan.limit_price != plan.used_asks[plan.used_asks.len() - 1].price
    {
        return Err(LiveAdmissionRefusal::LadderInvalid);
    }
    Ok(())
}

fn prepared_matches_request(
    prepared: &PreparedPolymarketBuy,
    request: &LiveOrderRequest,
    account: &LiveVenueAccountState,
) -> bool {
    prepared.condition_id == request.condition_id
        && prepared.outcome_id == request.outcome_id
        && prepared.token_id == request.token_id
        && prepared.neg_risk == request.admission.market.neg_risk
        && prepared.side == "BUY"
        && prepared.order_type == "FOK"
        && !prepared.post_only
        && !prepared.defer_exec
        && prepared.exchange_domain_version == 2
        && prepared.taker_shares == request.ladder.shares
        && prepared.limit_price == request.ladder.limit_price
        && prepared.minimum_tick_size == request.admission.market.minimum_tick_size
        && prepared.maker_collateral == request.ladder.worst_case_debit
        && prepared.worst_case_debit == request.ladder.worst_case_debit
        && prepared.spender == account.selected_spender
        && prepared.verifying_contract == account.selected_spender
        && prepared.metadata_hashes == request.identity.evidence_hashes
        && !prepared.order_hash.trim().is_empty()
        && !prepared.post_body_hash.trim().is_empty()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pe_resolver_card::{VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus};
    use pe_source_polymarket_public::{LiveFeeEvidence, LiveMarketEvidence};
    use pe_venue_polymarket::AskLevel;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;
    use time::macros::datetime;

    use super::*;
    use crate::economic::{
        BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit, MarketSelection,
        RiskAudit, RiskDecisionAudit, SizingAudit, SizingModeAudit,
    };
    use crate::live_journal::{
        LadderPlanAudit, LiveAdmissionArtifactAudit, LiveJournalPayload, replay_account,
    };

    struct FixtureVenue {
        account: Mutex<Result<LiveVenueAccountState, LiveVenueAccountReadError>>,
        classification: LivePostClassification,
        reconciliation: LiveVenueReconciledOutcome,
        posts: AtomicUsize,
        reconciliations: AtomicUsize,
    }

    impl FixtureVenue {
        fn new(classification: LivePostClassification) -> Self {
            Self {
                account: Mutex::new(Ok(account_state())),
                classification,
                reconciliation: LiveVenueReconciledOutcome::Ambiguous {
                    kind: LiveOrderAmbiguityKind::ReconciliationPending,
                },
                posts: AtomicUsize::new(0),
                reconciliations: AtomicUsize::new(0),
            }
        }
    }

    impl LiveOrderVenue for FixtureVenue {
        type Submission = ();

        fn prepare<'a>(
            &'a self,
            request: LiveVenuePrepareRequest,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            LiveVenuePrepared<Self::Submission>,
                            LiveVenuePreparationError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(LiveVenuePrepared::new(
                    prepared_order(&request, "spender"),
                    (),
                ))
            })
        }

        fn post_once<'a>(
            &'a self,
            _submission: Self::Submission,
        ) -> Pin<Box<dyn Future<Output = Result<RawHttpResponse, RawTransportFailure>> + Send + 'a>>
        {
            self.posts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(raw_response("order-post")) })
        }

        fn classify_post_response(
            &self,
            _response: &RawHttpResponse,
        ) -> Result<LivePostClassification, LivePostParseError> {
            Ok(self.classification.clone())
        }

        fn reconcile_and_cancel_by_order_hash<'a>(
            &'a self,
            _order_hash: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
                    + Send
                    + 'a,
            >,
        > {
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            let outcome = self.reconciliation.clone();
            Box::pin(async move {
                Ok(LiveVenueReconciliation {
                    outcome,
                    evidence: vec![RawHttpAttempt::Response(raw_response("order-status"))],
                })
            })
        }

        fn read_balance_and_allowance<'a>(
            &'a self,
            _neg_risk: bool,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>>
                    + Send
                    + 'a,
            >,
        > {
            let result = self.account.lock().unwrap().clone();
            Box::pin(async move { result })
        }
    }

    fn now() -> OffsetDateTime {
        datetime!(2026-08-11 12:00 UTC)
    }

    fn raw_response(endpoint_kind: &str) -> RawHttpResponse {
        RawHttpResponse {
            source_id: "fixture".to_owned(),
            endpoint_kind: endpoint_kind.to_owned(),
            method: "GET".to_owned(),
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

    fn account_state() -> LiveVenueAccountState {
        LiveVenueAccountState {
            observed_at: now(),
            closed_only: false,
            geoblocked: false,
            selected_spender: "spender".to_owned(),
            collateral_balance: CollateralAmount::from_atomic(10_000_000),
            allowance: CollateralAmount::from_atomic(10_000_000),
            reconciled_free_collateral: CollateralAmount::from_atomic(10_000_000),
            schema_version: 1,
            parser_version: 1,
            evidence: vec![RawHttpAttempt::Response(raw_response("account-state"))],
        }
    }

    fn market() -> LiveMarketEvidence {
        LiveMarketEvidence {
            condition_id: PolymarketConditionId("condition".to_owned()),
            ordered_outcome_token_ids: [
                PolymarketTokenId("11".to_owned()),
                PolymarketTokenId("22".to_owned()),
            ],
            neg_risk: true,
            minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
            minimum_order_size: ShareAmount::from_atomic(5_000_000),
            scheduled_end_unix: None,
            fee_evidence: LiveFeeEvidence {
                gamma_fees_enabled: Some(serde_json::json!(false)),
                gamma_fee_schedule: None,
                gamma_maker_base_fee_bps: Some(serde_json::json!(0)),
                gamma_taker_base_fee_bps: Some(serde_json::json!(0)),
                clob_maker_base_fee_bps: Some(serde_json::json!(0)),
                clob_taker_base_fee_bps: Some(serde_json::json!(0)),
            },
            raw_gamma_market_hash: blake3::hash(b"gamma"),
            raw_clob_market_hash: blake3::hash(b"clob"),
            observed_at_unix: now().unix_timestamp(),
            schema_version: LIVE_MARKET_SCHEMA_VERSION,
            parser_version: LIVE_MARKET_PARSER_VERSION,
            freshness_window_secs: 60,
        }
    }

    fn settlement() -> VenueSettlementRecord {
        VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: PolymarketConditionId("condition".to_owned()),
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: blake3::hash(b"settlement").to_hex().to_string(),
            source_timestamp_unix: Some(now().unix_timestamp()),
            observed_at_unix: now().unix_timestamp(),
            parser_version: 1,
            freshness_window_secs: 60,
        }
    }

    fn ladder() -> LadderPlan {
        let price = Price::new(dec!(0.50)).unwrap();
        let shares = ShareAmount::from_atomic(5_000_000);
        LadderPlan {
            used_asks: vec![AskLevel { price, shares }],
            best_ask: price,
            limit_price: price,
            shares,
            estimated_ladder_spend: CollateralAmount::from_atomic(2_500_000),
            worst_case_debit: CollateralAmount::from_atomic(2_500_000),
        }
    }

    fn request(account_id: &AccountId) -> LiveOrderRequest {
        let identity = LiveOrderIdentity {
            dispatch_id: "dispatch-1".to_owned(),
            idempotency_key: "idempotency-1".to_owned(),
            quote_id: "quote-1".to_owned(),
            config_hash: "config-hash".to_owned(),
            decision_hash: "decision-hash".to_owned(),
            evidence_hashes: vec!["market-hash".to_owned()],
            fill_projection: None,
            schema_version: 1,
            parser_version: 1,
        };
        let market = market();
        let settlement = settlement();
        let plan = ladder();
        let receipt = pe_event_log::AppendReceipt {
            sequence: pe_core_types::EventSeq(1),
            this_hash: blake3::Hash::from_bytes([1; 32]),
        };
        let receipts = AdmissionReceipts {
            gamma: receipt,
            clob_long: receipt,
            clob_compact: receipt,
        };
        let fee_schedule = CompactFeeSchedule::Zero;
        let admission_audit =
            LiveAdmissionArtifactAudit::new(&market, &settlement, fee_schedule, receipts);
        let ladder_audit = LadderPlanAudit::new(&plan);
        let risk_snapshot = pe_risk_engine::RiskSnapshot {
            leader_exposure_bps: pe_core_types::BasisPoints::ZERO,
            market_exposure_bps: pe_core_types::BasisPoints::ZERO,
            family_exposure_bps: pe_core_types::BasisPoints::ZERO,
            total_copy_exposure_bps: pe_core_types::BasisPoints::ZERO,
            intraday_pnl_bps: pe_core_types::BasisPoints::ZERO,
            rolling_7d_pnl_bps: pe_core_types::BasisPoints::ZERO,
            absolute_pnl_bps: pe_core_types::BasisPoints::ZERO,
            copy_latency_kill_switch_active: false,
            proposed_trade_bps: pe_core_types::BasisPoints(10),
            per_trade_cap_bps: 25,
            concentration_caps: None,
        };
        let economic = EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: PolymarketConditionId("condition".to_owned()),
                outcome_index: 0,
                token_id: PolymarketTokenId("11".to_owned()),
                side: pe_core_types::Side::Buy,
                market_id: "condition".to_owned(),
            },
            admission: admission_audit,
            ladder: ladder_audit,
            book_receipt: receipt,
            observation: None,
            sizing: SizingAudit {
                mode: SizingModeAudit::Contract { contracts: 5 },
                budget: CollateralAmount::from_atomic(10_000_000),
                principal: plan.worst_case_debit,
                minimum_shares: plan.shares,
                expected_shares: plan.shares,
                expected_vwap: plan.vwap().unwrap(),
                all_in_price: plan.limit_price,
                slippage_rate: Decimal::ZERO,
            },
            fee: FeeAudit {
                schedule: pe_venue_polymarket::CompactFeeSchedule::Zero,
                expected_fee: CollateralAmount::ZERO,
                reserve: CollateralAmount::ZERO,
            },
            risk: RiskAudit {
                snapshot: risk_snapshot,
                decision: RiskDecisionAudit::Approved,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_atomic(10_000_000),
                worst_case_debit: plan.worst_case_debit,
                price_impact_cap_bps: 100,
                chase_ceiling: plan.limit_price,
                band_floor: Price::ZERO,
                band_ceiling_exclusive: Price::ONE,
            },
            applied_configuration_hash: identity.config_hash.clone(),
        };
        LiveOrderRequest {
            target: FrozenLiveTarget {
                account_id: account_id.clone(),
                credential_binding: CredentialBindingIdentity {
                    version: 7,
                    key_id: "key-7".to_owned(),
                },
            },
            current_credential_binding: CredentialBindingIdentity {
                version: 7,
                key_id: "key-7".to_owned(),
            },
            mode: LiveModeSnapshot {
                requested: LiveControlMode::LiveTiny,
                effective: LiveControlMode::LiveTiny,
            },
            identity,
            condition_id: PolymarketConditionId("condition".to_owned()),
            outcome_id: OutcomeId(0),
            token_id: PolymarketTokenId("11".to_owned()),
            admission: LiveAdmissionArtifact {
                market,
                settlement,
                fee_schedule,
                receipts,
            },
            ladder: plan,
            economic,
        }
    }

    fn prepared_order(request: &LiveVenuePrepareRequest, spender: &str) -> PreparedPolymarketBuy {
        PreparedPolymarketBuy {
            condition_id: request.condition_id.clone(),
            outcome_id: request.outcome_id,
            token_id: request.token_id.clone(),
            maker: "maker".to_owned(),
            signer: "signer".to_owned(),
            funder: "funder".to_owned(),
            verifying_contract: spender.to_owned(),
            spender: spender.to_owned(),
            exchange_domain_version: 2,
            neg_risk: request.neg_risk,
            side: "BUY".to_owned(),
            salt: "1".to_owned(),
            timestamp_ms: 1,
            expiration: "0".to_owned(),
            maker_collateral: request.maximum_collateral,
            taker_shares: request.shares,
            limit_price: request.limit_price,
            minimum_tick_size: request.tick_size,
            signature_type: 3,
            order_type: "FOK".to_owned(),
            post_only: false,
            defer_exec: false,
            metadata: "0x00".to_owned(),
            builder: "0x00".to_owned(),
            order_hash: "order-hash".to_owned(),
            post_body_hash: "post-body-hash".to_owned(),
            sdk_version: "fixture-sdk".to_owned(),
            sdk_archive_sha256: "fixture-sdk-hash".to_owned(),
            metadata_hashes: request.metadata_hashes.clone(),
            worst_case_debit: request.maximum_collateral,
        }
    }

    async fn execute(
        classification: LivePostClassification,
    ) -> (LiveOrderOutcome, Vec<crate::LiveJournalEvent>, usize, usize) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(classification);
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = AccountId::new("account").unwrap();
        let prepared = match executor.prepare(request(&account_id), now()).await.unwrap() {
            LivePrepareResult::Prepared(prepared) => prepared,
            LivePrepareResult::Terminal(outcome) => {
                unreachable!("fixture admission unexpectedly failed: {outcome:?}")
            }
        };
        let outcome = executor.submit(prepared, now()).await.unwrap();
        let events = replay_account(&path, &account_id).unwrap();
        (
            outcome,
            events,
            venue.posts.load(Ordering::SeqCst),
            venue.reconciliations.load(Ordering::SeqCst),
        )
    }

    fn assert_common_terminal_evidence(events: &[crate::LiveJournalEvent]) {
        assert!(matches!(
            events,
            [
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::AdmissionEvaluated(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderPrepared(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderPosted(_),
                    ..
                },
                crate::LiveJournalEvent {
                    payload: LiveJournalPayload::OrderReconciled(_),
                    ..
                }
            ]
        ));
    }

    #[tokio::test]
    async fn matched_post_is_typed_and_fully_journaled() {
        let transaction_hash = format!("0x{}", "11".repeat(32));
        let (outcome, events, posts, reconciliations) = execute(LivePostClassification::Matched {
            venue_order_id: "venue-1".to_owned(),
            transaction_hashes: vec![transaction_hash.clone()],
            executed: LiveExecutedAmounts {
                making_amount: dec!(4.00),
                taking_amount: dec!(10),
            },
        })
        .await;
        assert!(matches!(
            &outcome,
            LiveOrderOutcome::Matched {
                transaction_hashes,
                executed: Some(LiveExecutedAmounts {
                    making_amount,
                    taking_amount,
                }),
                ..
            } if transaction_hashes.len() == 1
                && transaction_hashes.first() == Some(&transaction_hash)
                && *making_amount == dec!(4.00)
                && *taking_amount == dec!(10)
        ));
        assert_eq!(outcome.dispatch_state(), "submitted");
        assert_eq!(outcome.terminal_reason(), None);
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 0);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn killed_post_is_typed_and_fully_journaled() {
        let (outcome, events, posts, reconciliations) = execute(LivePostClassification::Killed {
            venue_order_id: Some("venue-1".to_owned()),
        })
        .await;
        assert!(matches!(outcome, LiveOrderOutcome::Killed { .. }));
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 0);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn rejected_post_is_typed_and_fully_journaled() {
        let (outcome, events, posts, reconciliations) = execute(LivePostClassification::Rejected {
            venue_order_id: None,
        })
        .await;
        assert!(matches!(outcome, LiveOrderOutcome::Rejected { .. }));
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 0);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn ambiguous_post_freezes_and_reconciles_before_another_order() {
        let (outcome, events, posts, reconciliations) =
            execute(LivePostClassification::Ambiguous {
                kind: LiveOrderAmbiguityKind::UnexpectedResponse,
            })
            .await;
        assert!(matches!(
            outcome,
            LiveOrderOutcome::Ambiguous {
                reconcile_first: true,
                ..
            }
        ));
        assert_eq!(posts, 1);
        assert_eq!(reconciliations, 1);
        assert_common_terminal_evidence(&events);
    }

    #[tokio::test]
    async fn crash_between_preparation_and_post_is_distinguishable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Matched {
            venue_order_id: "venue-1".to_owned(),
            transaction_hashes: Vec::new(),
            executed: LiveExecutedAmounts {
                making_amount: dec!(4.00),
                taking_amount: dec!(10),
            },
        });
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = AccountId::new("account").unwrap();
        assert!(matches!(
            executor.prepare(request(&account_id), now()).await.unwrap(),
            LivePrepareResult::Prepared(_)
        ));
        assert_eq!(venue.posts.load(Ordering::SeqCst), 0);
        let events = replay_account(&path, &account_id).unwrap();
        assert!(matches!(
            events.last().map(|event| &event.payload),
            Some(LiveJournalPayload::OrderPrepared(_))
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.payload, LiveJournalPayload::OrderPosted(_)))
        );
    }

    async fn assert_refusal_result(
        request: LiveOrderRequest,
        account: Result<LiveVenueAccountState, LiveVenueAccountReadError>,
        expected: LiveAdmissionRefusal,
    ) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let venue = FixtureVenue::new(LivePostClassification::Killed {
            venue_order_id: None,
        });
        *venue.account.lock().unwrap() = account;
        let executor = LiveExecutor::new(&venue, &journal);
        let account_id = request.target.account_id.clone();
        let outcome = executor.prepare(request, now()).await.unwrap();
        assert!(matches!(
            outcome,
            LivePrepareResult::Terminal(LiveOrderOutcome::Refused { reason }) if reason == expected.clone()
        ));
        assert_eq!(venue.posts.load(Ordering::SeqCst), 0);
        let events = replay_account(&path, &account_id).unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            LiveJournalPayload::AdmissionEvaluated(audit)
                if audit.verdict == LiveAdmissionVerdict::Refused(expected.clone())
        )));
        if expected == LiveAdmissionRefusal::CredentialVersionChanged {
            assert!(events.iter().any(|event| matches!(
                event.payload,
                LiveJournalPayload::CredentialBindingMismatch { .. }
            )));
        }
    }

    async fn assert_refusal(
        request: LiveOrderRequest,
        account: LiveVenueAccountState,
        expected: LiveAdmissionRefusal,
    ) {
        assert_refusal_result(request, Ok(account), expected).await;
    }

    #[tokio::test]
    async fn credential_mismatch_is_terminal_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.current_credential_binding.version += 1;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::CredentialVersionChanged,
        )
        .await;
    }

    #[tokio::test]
    async fn stale_artifact_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.admission.market.observed_at_unix -= 61;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::MarketEvidenceStale,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_market_artifact_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.admission.market.schema_version = 0;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::MarketEvidenceInvalid,
        )
        .await;
    }

    #[tokio::test]
    async fn stale_settlement_artifact_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.admission.settlement.observed_at_unix -= 61;
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::SettlementEvidenceStale,
        )
        .await;
    }

    #[tokio::test]
    async fn resolved_and_ambiguous_settlements_are_distinct_refusals() {
        let account_id = AccountId::new("account").unwrap();
        let mut resolved = request(&account_id);
        resolved.admission.settlement.status =
            VenueResolutionStatus::ResolvedWinner { outcome_index: 0 };
        assert_refusal(
            resolved,
            account_state(),
            LiveAdmissionRefusal::MarketAlreadyResolved,
        )
        .await;

        let mut ambiguous = request(&account_id);
        ambiguous.admission.settlement.status = VenueResolutionStatus::ResolvedAmbiguous;
        assert_refusal(
            ambiguous,
            account_state(),
            LiveAdmissionRefusal::SettlementAmbiguous,
        )
        .await;
    }

    #[tokio::test]
    async fn artifact_identity_mismatch_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.token_id = PolymarketTokenId("different-token".to_owned());
        assert_refusal(
            input,
            account_state(),
            LiveAdmissionRefusal::ArtifactIdentityMismatch,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_ladder_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.ladder.used_asks.clear();
        assert_refusal(input, account_state(), LiveAdmissionRefusal::LadderInvalid).await;
    }

    #[tokio::test]
    async fn unavailable_account_state_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let evidence = vec![RawHttpAttempt::Response(raw_response(
            "account-state-error",
        ))];
        assert_refusal_result(
            request(&account_id),
            Err(LiveVenueAccountReadError {
                kind: LiveAccountReadFailure::Authentication,
                evidence,
            }),
            LiveAdmissionRefusal::AccountStateUnavailable(LiveAccountReadFailure::Authentication),
        )
        .await;
    }

    #[tokio::test]
    async fn closed_only_and_geoblock_are_distinct_refusals() {
        let account_id = AccountId::new("account").unwrap();
        let mut closed = account_state();
        closed.closed_only = true;
        assert_refusal(
            request(&account_id),
            closed,
            LiveAdmissionRefusal::AccountClosedOnly,
        )
        .await;
        let mut blocked = account_state();
        blocked.geoblocked = true;
        assert_refusal(
            request(&account_id),
            blocked,
            LiveAdmissionRefusal::Geoblocked,
        )
        .await;
    }

    #[tokio::test]
    async fn insufficient_allowance_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut account = account_state();
        account.allowance = CollateralAmount::from_atomic(2_499_999);
        assert_refusal(
            request(&account_id),
            account,
            LiveAdmissionRefusal::InsufficientAllowance {
                required: CollateralAmount::from_atomic(2_500_000),
                available: CollateralAmount::from_atomic(2_499_999),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn insufficient_balance_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut account = account_state();
        account.collateral_balance = CollateralAmount::from_atomic(2_499_999);
        assert_refusal(
            request(&account_id),
            account,
            LiveAdmissionRefusal::InsufficientBalance {
                required: CollateralAmount::from_atomic(2_500_000),
                available: CollateralAmount::from_atomic(2_499_999),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn worst_case_debit_over_free_collateral_is_typed_before_post() {
        let account_id = AccountId::new("account").unwrap();
        let mut account = account_state();
        account.reconciled_free_collateral = CollateralAmount::from_atomic(2_499_999);
        assert_refusal(
            request(&account_id),
            account,
            LiveAdmissionRefusal::WorstCaseDebitExceedsFreeCollateral {
                required: CollateralAmount::from_atomic(2_500_000),
                available: CollateralAmount::from_atomic(2_499_999),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn per_account_kill_refuses_when_not_armed() {
        let account_id = AccountId::new("account").unwrap();
        let mut input = request(&account_id);
        input.mode.requested = LiveControlMode::Off;
        assert_refusal(input, account_state(), LiveAdmissionRefusal::ModeNotArmed).await;
    }
}
