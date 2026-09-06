//! Ordinary-live bindings from the concrete Polymarket clients to execution-core.
//!
//! The V2 client intentionally exposes raw, observed reconciliation rather than separate
//! authenticated account/order methods. This adapter consumes that one composite seam for both
//! account admission and order-hash recovery, preserving every raw observation in the
//! execution-core audit types. Market admission records fresh Gamma-long, CLOB-long, and compact
//! CLOB responses before composing the shared artifact. The long observations use a 60-second
//! freshness window; the independently fetched executable ladder keeps its stricter two-second
//! venue-owned bound.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pe_core_types::{
    AccountId, CollateralAmount, Price, RawEvidence, RawHttpAttempt, RawHttpResponse,
    RawTransportFailure, ReceivedAt, SourceId, SourceTimestamp, TransportErrorClass, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_execution_core::live_journal::{LiveAccountBindingAudit, request_descriptor_hash};
use pe_execution_core::{
    AdmissionReceipts, LiveAccountReadFailure, LiveAccountStateFuture, LiveAdmissionArtifact,
    LiveExecutedAmounts, LiveOrderAmbiguityKind, LiveOrderVenue, LivePostClassification,
    LivePostFuture, LivePostParseError, LiveReconciliationFuture, LiveVenueAccountReadError,
    LiveVenueAccountState, LiveVenuePreparationError, LiveVenuePrepareFuture,
    LiveVenuePrepareRequest, LiveVenuePrepared, LiveVenueReconciledOutcome,
    LiveVenueReconciliation, LiveVenueReconciliationError, RedemptionStatusObservation,
    RedemptionStatusReadError, RedemptionStatusReader,
};
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_source_polymarket_public::{
    GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION, GAMMA_MARKETS_SOURCE_ID,
    LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, validate_live_market,
};
use pe_venue_polymarket::{
    CLOB_V2_HOST, CanaryV2Client, CanaryV2Credentials, CustodyKind, PreparedSubmission,
    REDEMPTION_ADAPTER_VERSION, REDEMPTION_PARSER_VERSION, REDEMPTION_SCHEMA_VERSION,
    RELAYER_BASE_URL, RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX,
    RELAYER_LEGACY_TRANSACTION_PATH, RedemptionTransport, RedemptionTransportError,
    RelayerCredentials, RelayerPollPolicy, RelayerTransportClient, SignedRedemptionRequest,
    V2BuyRequest, parse_compact_market,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use time::OffsetDateTime;

use crate::activity_ingest::{SourceLogHandle, SourceLogHandleError};
use crate::live_credentials::LiveAccountCredentials;

const LIVE_MARKET_FRESHNESS_SECS: u64 = 60;
const MARKET_REQUEST_TIMEOUT_SECS: u64 = 10;
const RECONCILIATION_TIMEOUT_SECS: u64 = 30;
const CLOB_LONG_MARKET_SOURCE_ID: &str = "polymarket.clob.markets";
const CLOB_COMPACT_MARKET_SOURCE_ID: &str = "polymarket.clob.compact-market";
const POLYGON_RECEIPT_RPC_SCHEMA_VERSION: u16 = 1;
const POLYGON_RECEIPT_RPC_PARSER_VERSION: u16 = 1;

/// Single-attempt, key-free Polygon JSON-RPC transport using the service's bounded HTTP client.
#[derive(Clone)]
pub struct PolygonReceiptRpc {
    http: reqwest::Client,
    url: String,
}

/// Narrow read-only seam used by the existing recovery pass and deterministic receipt fixtures.
pub trait PolygonReceiptReader: Send + Sync {
    fn chain_id(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>>;
    fn transaction_receipt<'a>(
        &'a self,
        transaction_hash: &'a str,
    ) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + 'a>>;
    fn finalized_block(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>>;
    fn block_by_number(
        &self,
        number: u64,
    ) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>>;
}

impl PolygonReceiptRpc {
    #[must_use]
    pub fn new(http: reqwest::Client, url: impl Into<String>) -> Self {
        Self {
            http,
            url: url.into(),
        }
    }

    pub async fn chain_id(&self) -> RawHttpAttempt {
        self.call("polygon-chain-id", "eth_chainId", serde_json::json!([]))
            .await
    }

    pub async fn transaction_receipt(&self, transaction_hash: &str) -> RawHttpAttempt {
        self.call(
            "polygon-transaction-receipt",
            "eth_getTransactionReceipt",
            serde_json::json!([transaction_hash]),
        )
        .await
    }

    pub async fn finalized_block(&self) -> RawHttpAttempt {
        self.call(
            "polygon-finalized-block",
            "eth_getBlockByNumber",
            serde_json::json!(["finalized", false]),
        )
        .await
    }

    pub async fn block_by_number(&self, number: u64) -> RawHttpAttempt {
        self.call(
            "polygon-canonical-block",
            "eth_getBlockByNumber",
            serde_json::json!([format!("0x{number:x}"), false]),
        )
        .await
    }

    async fn call(&self, endpoint_kind: &str, method: &str, params: Value) -> RawHttpAttempt {
        let observed_at = OffsetDateTime::now_utc();
        let ordered_query = vec![
            ("rpc_method".to_owned(), method.to_owned()),
            ("rpc_params".to_owned(), params.to_string()),
        ];
        let response = match self
            .http
            .post(&self.url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                return RawHttpAttempt::TransportFailure(RawTransportFailure {
                    source_id: "polygon-receipt-rpc".to_owned(),
                    endpoint_kind: endpoint_kind.to_owned(),
                    method: "POST".to_owned(),
                    path: self.url.clone(),
                    ordered_query,
                    attempt_ordinal: 1,
                    observed_at,
                    received_at: OffsetDateTime::now_utc(),
                    error_class: classify_reqwest_error(&error),
                    schema_version: POLYGON_RECEIPT_RPC_SCHEMA_VERSION,
                    parser_version: POLYGON_RECEIPT_RPC_PARSER_VERSION,
                    adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
                });
            }
        };
        let status = response.status().as_u16();
        let mut headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect::<Vec<_>>();
        headers.sort();
        let body = match response.bytes().await {
            Ok(body) => body.to_vec(),
            Err(_) => {
                return RawHttpAttempt::TransportFailure(RawTransportFailure {
                    source_id: "polygon-receipt-rpc".to_owned(),
                    endpoint_kind: endpoint_kind.to_owned(),
                    method: "POST".to_owned(),
                    path: self.url.clone(),
                    ordered_query,
                    attempt_ordinal: 1,
                    observed_at,
                    received_at: OffsetDateTime::now_utc(),
                    error_class: TransportErrorClass::BodyRead,
                    schema_version: POLYGON_RECEIPT_RPC_SCHEMA_VERSION,
                    parser_version: POLYGON_RECEIPT_RPC_PARSER_VERSION,
                    adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
                });
            }
        };
        RawHttpAttempt::Response(RawHttpResponse {
            source_id: "polygon-receipt-rpc".to_owned(),
            endpoint_kind: endpoint_kind.to_owned(),
            method: "POST".to_owned(),
            path: self.url.clone(),
            ordered_query,
            status,
            headers,
            body,
            attempt_ordinal: 1,
            source_at: None,
            observed_at,
            received_at: OffsetDateTime::now_utc(),
            schema_version: POLYGON_RECEIPT_RPC_SCHEMA_VERSION,
            parser_version: POLYGON_RECEIPT_RPC_PARSER_VERSION,
            adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
        })
    }
}

impl PolygonReceiptReader for PolygonReceiptRpc {
    fn chain_id(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
        Box::pin(PolygonReceiptRpc::chain_id(self))
    }

    fn transaction_receipt<'a>(
        &'a self,
        transaction_hash: &'a str,
    ) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + 'a>> {
        Box::pin(PolygonReceiptRpc::transaction_receipt(
            self,
            transaction_hash,
        ))
    }

    fn finalized_block(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
        Box::pin(PolygonReceiptRpc::finalized_block(self))
    }

    fn block_by_number(
        &self,
        number: u64,
    ) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
        Box::pin(PolygonReceiptRpc::block_by_number(self, number))
    }
}

fn classify_reqwest_error(error: &reqwest::Error) -> TransportErrorClass {
    if error.is_timeout() {
        TransportErrorClass::Timeout
    } else if error.is_connect() {
        TransportErrorClass::Connect
    } else if error.is_builder() {
        TransportErrorClass::RequestBuild
    } else {
        TransportErrorClass::Other
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LiveVenueAdapterError {
    #[error("V2 client construction failed: {0}")]
    Client(String),
    #[error("live market request failed: {0}")]
    MarketTransport(String),
    #[error("live market source returned HTTP {0}")]
    MarketStatus(u16),
    #[error("live market validation failed: {0}")]
    MarketValidation(String),
    #[error("source-log coordinator closed while recording live admission")]
    SourceLogClosed,
    #[error("requested outcome is not binary")]
    Outcome,
    #[error("redemption transport configuration failed: {0}")]
    Redemption(String),
}

/// Concrete per-account V2 order venue. Constructed only after the account bundle decrypts.
pub struct PolymarketLiveVenue {
    client: CanaryV2Client,
    account_binding: LiveAccountBindingAudit,
}

pub(crate) struct BoundLiveVenueAccountState {
    pub state: LiveVenueAccountState,
    pub binding: LiveAccountBindingAudit,
}

impl PolymarketLiveVenue {
    pub async fn from_credentials(
        credentials: &LiveAccountCredentials,
    ) -> Result<Self, LiveVenueAdapterError> {
        let client = CanaryV2Client::new(
            CanaryV2Credentials {
                private_key: credentials.private_key.clone(),
                api_key: credentials.api_key.clone(),
                api_secret: credentials.api_secret.clone(),
                api_passphrase: credentials.api_passphrase.clone(),
                deposit_wallet: credentials.deposit_wallet.clone(),
            },
            CLOB_V2_HOST,
        )
        .await
        .map_err(|error| LiveVenueAdapterError::Client(error.to_string()))?;
        // Authentication/version observations belong to the first real read, not to a later
        // unrelated operation. The composite reconciliation call captures its own responses.
        let _ = client.take_observations();
        let account_id = AccountId::new(&credentials.account_id)
            .map_err(|error| LiveVenueAdapterError::Client(error.to_string()))?;
        let custody_wallet = WalletAddress::from_hex(&credentials.deposit_wallet)
            .map_err(|error| LiveVenueAdapterError::Client(error.to_string()))?;
        let account_binding = LiveAccountBindingAudit::new(
            account_id,
            pe_execution_core::CredentialBindingIdentity {
                version: credentials.bundle_version,
                key_id: credentials.key_id.clone(),
            },
            custody_wallet,
            live_account_credential_fingerprint(credentials),
        );
        Ok(Self {
            client,
            account_binding,
        })
    }

    #[must_use]
    pub fn deposit_wallet(&self) -> String {
        self.client.deposit_wallet()
    }

    #[must_use]
    pub fn owner_signer(&self) -> String {
        self.client.owner_signer()
    }

    #[must_use]
    pub(crate) fn account_binding(&self) -> &LiveAccountBindingAudit {
        &self.account_binding
    }

    async fn account_state(
        &self,
        neg_risk: bool,
    ) -> Result<LiveVenueAccountState, LiveVenueAccountReadError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(RECONCILIATION_TIMEOUT_SECS);
        let (evidence, protocol_failure) = self.client.account_probe_raw(deadline).await;
        let request_descriptor_hashes =
            bind_account_read_responses(&evidence, &self.account_binding).map_err(|()| {
                account_read_error(
                    LiveAccountReadFailure::Protocol,
                    evidence.clone(),
                    Vec::new(),
                )
            })?;
        if protocol_failure.is_some() {
            return Err(account_read_error(
                LiveAccountReadFailure::Protocol,
                evidence,
                request_descriptor_hashes,
            ));
        }
        parse_account_state(evidence, neg_risk, request_descriptor_hashes)
    }

    pub(crate) async fn account_states(
        &self,
    ) -> Result<(BoundLiveVenueAccountState, BoundLiveVenueAccountState), LiveVenueAccountReadError>
    {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(RECONCILIATION_TIMEOUT_SECS);
        let (evidence, protocol_failure) = self.client.account_probe_raw(deadline).await;
        let request_descriptor_hashes =
            bind_account_read_responses(&evidence, &self.account_binding).map_err(|()| {
                account_read_error(
                    LiveAccountReadFailure::Protocol,
                    evidence.clone(),
                    Vec::new(),
                )
            })?;
        if protocol_failure.is_some() {
            return Err(account_read_error(
                LiveAccountReadFailure::Protocol,
                evidence,
                request_descriptor_hashes,
            ));
        }
        let standard =
            parse_account_state(evidence.clone(), false, request_descriptor_hashes.clone())?;
        let neg_risk = parse_account_state(evidence, true, request_descriptor_hashes)?;
        Ok((
            BoundLiveVenueAccountState {
                state: standard,
                binding: self.account_binding.clone(),
            },
            BoundLiveVenueAccountState {
                state: neg_risk,
                binding: self.account_binding.clone(),
            },
        ))
    }
}

fn bind_account_read_responses(
    evidence: &[RawEvidence],
    binding: &LiveAccountBindingAudit,
) -> Result<Vec<String>, ()> {
    evidence
        .iter()
        .filter_map(|item| match item {
            RawEvidence::HttpResponse(response) => Some(response),
            RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
        })
        .map(|response| {
            let descriptor = binding.request_descriptor(
                response.method.clone(),
                response.path.clone(),
                response.endpoint_kind.clone(),
                0,
                response.ordered_query.clone(),
            );
            request_descriptor_hash(&descriptor).map_err(|_| ())
        })
        .collect()
}

fn live_account_credential_fingerprint(credentials: &LiveAccountCredentials) -> String {
    let mut hasher = blake3::Hasher::new();
    for component in [
        b"ordinary-live-account-read-credential-v1".as_slice(),
        credentials.private_key.as_bytes(),
        credentials.api_key.as_bytes(),
        credentials.api_secret.as_bytes(),
        credentials.api_passphrase.as_bytes(),
    ] {
        hasher.update(blake3::hash(component).as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

impl LiveOrderVenue for PolymarketLiveVenue {
    type Submission = PreparedSubmission;

    fn prepare<'a>(
        &'a self,
        request: LiveVenuePrepareRequest,
    ) -> LiveVenuePrepareFuture<'a, Self::Submission> {
        Box::pin(async move {
            let neg_risk = request.neg_risk;
            let submission = self
                .client
                .prepare_buy_for_market(
                    V2BuyRequest {
                        condition_id: request.condition_id,
                        outcome_id: request.outcome_id,
                        token_id: request.token_id,
                        limit_price: request.limit_price,
                        shares: request.shares,
                        maximum_collateral: request.maximum_collateral,
                        tick_size: request.tick_size,
                        metadata_hashes: request.metadata_hashes,
                    },
                    neg_risk,
                )
                .await
                .map_err(|_| LiveVenuePreparationError::Venue)?;
            Ok(LiveVenuePrepared::new(
                submission.prepared().clone(),
                submission,
            ))
        })
    }

    fn post_once<'a>(&'a self, submission: Self::Submission) -> LivePostFuture<'a> {
        Box::pin(async move { self.client.post_order_once(submission).await })
    }

    fn classify_post_response(
        &self,
        response: &RawHttpResponse,
    ) -> Result<LivePostClassification, LivePostParseError> {
        classify_order_post_response(response)
    }

    fn reconcile_and_cancel_by_order_hash<'a>(
        &'a self,
        order_hash: &'a str,
    ) -> LiveReconciliationFuture<'a> {
        Box::pin(async move {
            let deadline =
                tokio::time::Instant::now() + Duration::from_secs(RECONCILIATION_TIMEOUT_SECS);
            let (raw, protocol_failure) = self
                .client
                .clob_reconciliation_raw(Some(order_hash), deadline)
                .await;
            if protocol_failure.is_some()
                || raw
                    .iter()
                    .any(|item| matches!(item, RawEvidence::HttpTransportFailure(_)))
            {
                return Err(LiveVenueReconciliationError {
                    evidence: raw.into_iter().filter_map(raw_attempt).collect(),
                });
            }
            let mut evidence = raw
                .iter()
                .cloned()
                .filter_map(raw_attempt)
                .collect::<Vec<_>>();
            let outcome = classify_reconciliation(&raw, order_hash).map_err(|_| {
                LiveVenueReconciliationError {
                    evidence: evidence.clone(),
                }
            })?;
            if let ReconciliationClassification::Cancel { order_id } = outcome {
                match self.client.cancel_order_once(&order_id).await {
                    Ok(response) if (200..300).contains(&response.status) => {
                        evidence.push(RawHttpAttempt::Response(response));
                        return Ok(LiveVenueReconciliation {
                            outcome: LiveVenueReconciledOutcome::Killed {
                                venue_order_id: Some(order_id),
                            },
                            evidence,
                        });
                    }
                    Ok(response) => evidence.push(RawHttpAttempt::Response(response)),
                    Err(failure) => evidence.push(RawHttpAttempt::TransportFailure(failure)),
                }
                return Ok(LiveVenueReconciliation {
                    outcome: LiveVenueReconciledOutcome::Ambiguous {
                        kind: LiveOrderAmbiguityKind::ReconciliationPending,
                    },
                    evidence,
                });
            }
            Ok(LiveVenueReconciliation {
                outcome: outcome.into_venue_outcome(),
                evidence,
            })
        })
    }

    fn read_balance_and_allowance<'a>(&'a self, neg_risk: bool) -> LiveAccountStateFuture<'a> {
        Box::pin(async move { self.account_state(neg_risk).await })
    }
}

fn classify_order_post_response(
    response: &RawHttpResponse,
) -> Result<LivePostClassification, LivePostParseError> {
    let parsed = CanaryV2Client::parse_post_response(response)
        .map_err(|_| LivePostParseError::InvalidResponse)?;
    if parsed.success {
        if parsed.order_id.trim().is_empty()
            || parsed.making_amount <= Decimal::ZERO
            || parsed.taking_amount <= Decimal::ZERO
            || parsed
                .making_amount
                .checked_div(parsed.taking_amount)
                .and_then(|price| Price::new(price).ok())
                .is_none()
        {
            return Err(LivePostParseError::InvalidResponse);
        }
        return Ok(LivePostClassification::Matched {
            venue_order_id: parsed.order_id,
            executed: LiveExecutedAmounts {
                making_amount: parsed.making_amount,
                taking_amount: parsed.taking_amount,
            },
            transaction_hashes: post_transaction_hashes(response)?,
        });
    }
    if parsed.definitive && parsed.order_id.trim().is_empty() {
        return Ok(LivePostClassification::Rejected {
            venue_order_id: None,
        });
    }
    // A returned order identity must be looked up and, if still live, cancelled. A
    // contradictory/nonterminal FOK response is likewise ambiguous until that pass.
    Ok(LivePostClassification::Ambiguous {
        kind: LiveOrderAmbiguityKind::UnexpectedResponse,
    })
}

fn post_transaction_hashes(response: &RawHttpResponse) -> Result<Vec<String>, LivePostParseError> {
    let value: Value =
        serde_json::from_slice(&response.body).map_err(|_| LivePostParseError::InvalidResponse)?;
    let hashes = value.get("transactionHashes");
    let Some(hashes) = hashes else {
        return Ok(Vec::new());
    };
    let hashes = hashes
        .as_array()
        .ok_or(LivePostParseError::InvalidResponse)?;
    let mut canonical = BTreeSet::new();
    for value in hashes {
        let raw = value.as_str().ok_or(LivePostParseError::InvalidResponse)?;
        if raw.trim().is_empty()
            || raw
                .trim_start_matches("0x")
                .bytes()
                .all(|byte| byte == b'0')
        {
            continue;
        }
        let hash =
            canonical_nonzero_transaction_hash(raw).ok_or(LivePostParseError::InvalidResponse)?;
        canonical.insert(hash);
    }
    Ok(canonical.into_iter().collect())
}

enum ReconciliationClassification {
    Matched {
        venue_order_id: String,
        transaction_hashes: Vec<String>,
    },
    Killed(Option<String>),
    Rejected(Option<String>),
    Cancel {
        order_id: String,
    },
    Ambiguous,
}

impl ReconciliationClassification {
    fn into_venue_outcome(self) -> LiveVenueReconciledOutcome {
        match self {
            Self::Matched {
                venue_order_id,
                transaction_hashes,
            } => LiveVenueReconciledOutcome::Matched {
                venue_order_id,
                transaction_hashes,
            },
            Self::Killed(venue_order_id) => LiveVenueReconciledOutcome::Killed { venue_order_id },
            Self::Rejected(venue_order_id) => {
                LiveVenueReconciledOutcome::Rejected { venue_order_id }
            }
            Self::Cancel { .. } | Self::Ambiguous => LiveVenueReconciledOutcome::Ambiguous {
                kind: LiveOrderAmbiguityKind::ReconciliationPending,
            },
        }
    }
}

fn classify_reconciliation(
    evidence: &[RawEvidence],
    order_hash: &str,
) -> Result<ReconciliationClassification, ()> {
    let matching_trades = evidence
        .iter()
        .flat_map(|item| match item {
            RawEvidence::HttpResponse(response) if response.endpoint_kind == "trades-page" => {
                response_json(response)
                    .ok()
                    .and_then(|value| page_rows(&value).ok().map(|rows| rows.to_vec()))
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|trade| trade_matches_order(trade, order_hash))
                    .collect::<Vec<_>>()
            }
            RawEvidence::HttpResponse(_)
            | RawEvidence::HttpTransportFailure(_)
            | RawEvidence::Artifact(_) => Vec::new(),
        })
        .collect::<Vec<_>>();
    if !matching_trades.is_empty() {
        let venue_order_id = matching_trades
            .iter()
            .find_map(|trade| {
                trade
                    .get("taker_order_id")
                    .or_else(|| trade.get("takerOrderId"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
            })
            .unwrap_or(order_hash)
            .to_owned();
        let mut transaction_hashes = BTreeSet::new();
        for trade in &matching_trades {
            let Some(raw) = trade.get("transaction_hash").and_then(Value::as_str) else {
                continue;
            };
            if raw.trim().is_empty()
                || raw
                    .trim_start_matches("0x")
                    .bytes()
                    .all(|byte| byte == b'0')
            {
                continue;
            }
            transaction_hashes.insert(canonical_nonzero_transaction_hash(raw).ok_or(())?);
        }
        return Ok(ReconciliationClassification::Matched {
            venue_order_id,
            transaction_hashes: transaction_hashes.into_iter().collect(),
        });
    }

    let exact = evidence.iter().find_map(|item| match item {
        RawEvidence::HttpResponse(response) if response.endpoint_kind == "exact-order" => {
            Some(response)
        }
        RawEvidence::HttpResponse(_)
        | RawEvidence::HttpTransportFailure(_)
        | RawEvidence::Artifact(_) => None,
    });
    let exact = exact.ok_or(())?;
    if exact.status == 404 {
        return Ok(ReconciliationClassification::Killed(None));
    }
    let value = response_json(exact)?;
    let order_id = value
        .get("id")
        .or_else(|| value.get("order_id"))
        .or_else(|| value.get("orderID"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(order_hash)
        .to_owned();
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    if matches!(status.as_str(), "MATCHED" | "FILLED") {
        return Ok(ReconciliationClassification::Matched {
            venue_order_id: order_id,
            transaction_hashes: Vec::new(),
        });
    }
    if matches!(
        status.as_str(),
        "CANCELED" | "CANCELLED" | "EXPIRED" | "UNMATCHED"
    ) {
        return Ok(ReconciliationClassification::Killed(Some(order_id)));
    }
    if matches!(status.as_str(), "REJECTED" | "INVALID") {
        return Ok(ReconciliationClassification::Rejected(Some(order_id)));
    }
    if status.is_empty() || matches!(status.as_str(), "LIVE" | "OPEN" | "DELAYED") {
        return Ok(ReconciliationClassification::Cancel { order_id });
    }
    Ok(ReconciliationClassification::Ambiguous)
}

fn canonical_nonzero_transaction_hash(value: &str) -> Option<String> {
    let digits = value.strip_prefix("0x")?;
    if digits.len() != 64
        || !digits.bytes().all(|byte| byte.is_ascii_hexdigit())
        || digits.bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    Some(format!("0x{}", digits.to_ascii_lowercase()))
}

fn parse_account_state(
    evidence: Vec<RawEvidence>,
    neg_risk: bool,
    request_descriptor_hashes: Vec<String>,
) -> Result<LiveVenueAccountState, LiveVenueAccountReadError> {
    let selected_spender = if neg_risk {
        CanaryV2Client::negrisk_spender()
    } else {
        CanaryV2Client::standard_spender()
    }
    .map_err(|_| {
        account_read_error(
            LiveAccountReadFailure::Protocol,
            evidence.clone(),
            request_descriptor_hashes.clone(),
        )
    })?;
    classify_account_responses(
        evidence.into_iter().filter_map(raw_attempt).collect(),
        selected_spender,
        request_descriptor_hashes,
    )
}

/// Classify the retained, credential-free account responses into the one canonical live state.
pub(crate) fn classify_account_responses(
    evidence: Vec<RawHttpAttempt>,
    selected_spender: String,
    request_descriptor_hashes: Vec<String>,
) -> Result<LiveVenueAccountState, LiveVenueAccountReadError> {
    let account_error = |kind| LiveVenueAccountReadError {
        kind,
        evidence: evidence.clone(),
        request_descriptor_hashes: request_descriptor_hashes.clone(),
    };
    if evidence
        .iter()
        .any(|item| matches!(item, RawHttpAttempt::TransportFailure(_)))
    {
        return Err(account_error(LiveAccountReadFailure::Transport));
    }
    if evidence.len() != 3
        || !is_known_account_spender(&selected_spender)
        || ["geoblock", "closed-only", "balance-allowance"]
            .into_iter()
            .any(|kind| response_by_kind(&evidence, kind).is_none())
    {
        return Err(account_error(LiveAccountReadFailure::Protocol));
    }
    let response = |kind: &str| {
        response_by_kind(&evidence, kind)
            .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))
    };
    let geoblock = response("geoblock")?;
    let closed_only_response = response("closed-only")?;
    let balance = response("balance-allowance")?;
    if [geoblock, closed_only_response, balance]
        .iter()
        .any(|response| response.status == 401 || response.status == 403)
    {
        return Err(account_error(LiveAccountReadFailure::Authentication));
    }
    if !valid_account_response(geoblock, "geoblock", "/api/geoblock", &[])
        || !valid_account_response(
            closed_only_response,
            "closed-only",
            "/auth/ban-status/closed-only",
            &[],
        )
        || !valid_account_response(
            balance,
            "balance-allowance",
            "/balance-allowance",
            &[("asset_type", "COLLATERAL"), ("signature_type", "3")],
        )
    {
        return Err(account_error(LiveAccountReadFailure::Protocol));
    }
    let geoblock_json =
        response_json(geoblock).map_err(|_| account_error(LiveAccountReadFailure::Protocol))?;
    let closed_json = response_json(closed_only_response)
        .map_err(|_| account_error(LiveAccountReadFailure::Protocol))?;
    let balance_json =
        response_json(balance).map_err(|_| account_error(LiveAccountReadFailure::Protocol))?;
    let blocked = geoblock_json
        .get("blocked")
        .and_then(Value::as_bool)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let country = geoblock_json
        .get("country")
        .and_then(Value::as_str)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let geoblocked = blocked && !matches!(country, "IE" | "JP" | "MT" | "NL");
    let closed_only = closed_json
        .get("closed_only")
        .and_then(Value::as_bool)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let collateral_balance = atomic_amount(balance_json.get("balance"))
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let allowances = balance_json
        .get("allowances")
        .and_then(Value::as_object)
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    let allowance = match allowances
        .iter()
        .find(|(spender, _)| spender.eq_ignore_ascii_case(&selected_spender))
    {
        Some((_, value)) => atomic_amount(Some(value))
            .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?,
        None => CollateralAmount::ZERO,
    };
    let observed_at = [geoblock, closed_only_response, balance]
        .iter()
        .map(|response| response.observed_at)
        .min()
        .ok_or_else(|| account_error(LiveAccountReadFailure::Protocol))?;
    Ok(LiveVenueAccountState {
        observed_at,
        closed_only,
        geoblocked,
        selected_spender,
        collateral_balance,
        allowance,
        // The ordinary service has no separate local reservation owner yet; the durable
        // dispatch reservation prevents another account/order from overtaking this read.
        reconciled_free_collateral: collateral_balance,
        schema_version: 1,
        parser_version: 1,
        evidence,
        request_descriptor_hashes,
    })
}

fn response_by_kind<'a>(
    evidence: &'a [RawHttpAttempt],
    endpoint_kind: &str,
) -> Option<&'a RawHttpResponse> {
    let mut matches = evidence.iter().filter_map(|attempt| match attempt {
        RawHttpAttempt::Response(response) if response.endpoint_kind == endpoint_kind => {
            Some(response)
        }
        RawHttpAttempt::Response(_) | RawHttpAttempt::TransportFailure(_) => None,
    });
    let response = matches.next()?;
    matches.next().is_none().then_some(response)
}

fn is_known_account_spender(selected_spender: &str) -> bool {
    [
        CanaryV2Client::standard_spender(),
        CanaryV2Client::negrisk_spender(),
    ]
    .into_iter()
    .flatten()
    .any(|spender| spender == selected_spender)
}

fn valid_account_response(
    response: &RawHttpResponse,
    endpoint_kind: &str,
    path: &str,
    ordered_query: &[(&str, &str)],
) -> bool {
    let query_matches = response.ordered_query.len() == ordered_query.len()
        && response.ordered_query.iter().zip(ordered_query).all(
            |((actual_name, actual_value), (name, value))| {
                actual_name == name && actual_value == value
            },
        );
    response.source_id == "polymarket-clob-v2"
        && response.endpoint_kind == endpoint_kind
        && response.method == "GET"
        && response.path == path
        && query_matches
        && (200..300).contains(&response.status)
        && response.attempt_ordinal == 1
        && response.received_at >= response.observed_at
        && response.schema_version == 1
        && response.parser_version == 1
        && response.adapter_version == pe_venue_polymarket::SDK_VERSION
}

fn account_read_error(
    kind: LiveAccountReadFailure,
    evidence: Vec<RawEvidence>,
    request_descriptor_hashes: Vec<String>,
) -> LiveVenueAccountReadError {
    LiveVenueAccountReadError {
        kind,
        evidence: evidence.into_iter().filter_map(raw_attempt).collect(),
        request_descriptor_hashes,
    }
}

fn raw_attempt(item: RawEvidence) -> Option<RawHttpAttempt> {
    match item {
        RawEvidence::HttpResponse(response) => Some(RawHttpAttempt::Response(response)),
        RawEvidence::HttpTransportFailure(failure) => {
            Some(RawHttpAttempt::TransportFailure(failure))
        }
        RawEvidence::Artifact(_) => None,
    }
}

fn response_json(response: &RawHttpResponse) -> Result<Value, ()> {
    if !(200..300).contains(&response.status) {
        return Err(());
    }
    serde_json::from_slice(&response.body).map_err(|_| ())
}

fn page_rows(value: &Value) -> Result<&[Value], ()> {
    value
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or(())
}

fn trade_matches_order(trade: &Value, order_id: &str) -> bool {
    trade
        .get("taker_order_id")
        .or_else(|| trade.get("takerOrderId"))
        .and_then(Value::as_str)
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(order_id))
        || trade
            .get("maker_orders")
            .or_else(|| trade.get("makerOrders"))
            .and_then(Value::as_array)
            .is_some_and(|orders| {
                orders.iter().any(|order| {
                    order
                        .get("order_id")
                        .or_else(|| order.get("orderId"))
                        .and_then(Value::as_str)
                        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(order_id))
                })
            })
}

fn atomic_amount(value: Option<&Value>) -> Option<CollateralAmount> {
    let value = value?;
    let encoded = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    encoded
        .parse::<u64>()
        .ok()
        .map(CollateralAmount::from_atomic)
}

/// Fresh two-source market-admission builder for one condition.
#[derive(Clone)]
pub struct LiveAdmissionBuilder {
    client: reqwest::Client,
    gamma_base_url: String,
    clob_base_url: String,
    source_log: SourceLogHandle,
}

impl LiveAdmissionBuilder {
    pub fn new(
        client: reqwest::Client,
        gamma_base_url: impl Into<String>,
        clob_base_url: impl Into<String>,
        source_log: SourceLogHandle,
    ) -> Self {
        Self {
            client,
            gamma_base_url: gamma_base_url.into().trim_end_matches('/').to_owned(),
            clob_base_url: clob_base_url.into().trim_end_matches('/').to_owned(),
            source_log,
        }
    }

    pub async fn build(
        &self,
        condition_id: &pe_core_types::PolymarketConditionId,
        now: OffsetDateTime,
    ) -> Result<LiveAdmissionArtifact, LiveVenueAdapterError> {
        let gamma_url = format!(
            "{}/markets?condition_ids={}&limit=500&include_tag=true",
            self.gamma_base_url, condition_id.0
        );
        let clob_url = format!("{}/markets/{}", self.clob_base_url, condition_id.0);
        let compact_url = format!("{}/clob-markets/{}", self.clob_base_url, condition_id.0);
        let (gamma_raw, gamma_receipt) = self
            .fetch_and_record(
                &gamma_url,
                GAMMA_MARKETS_SOURCE_ID,
                GAMMA_MARKETS_SCHEMA_VERSION,
                GAMMA_MARKETS_PARSER_VERSION,
            )
            .await?;
        let (clob_raw, clob_long_receipt) = self
            .fetch_and_record(
                &clob_url,
                CLOB_LONG_MARKET_SOURCE_ID,
                LIVE_MARKET_SCHEMA_VERSION,
                LIVE_MARKET_PARSER_VERSION,
            )
            .await?;
        let market = validate_live_market(
            &gamma_raw,
            &clob_raw,
            condition_id,
            now.unix_timestamp(),
            LIVE_MARKET_FRESHNESS_SECS,
        )
        .map_err(|error| LiveVenueAdapterError::MarketValidation(error.to_string()))?;
        let (compact_raw, clob_compact_receipt) = self
            .fetch_and_record(
                &compact_url,
                CLOB_COMPACT_MARKET_SOURCE_ID,
                LIVE_MARKET_SCHEMA_VERSION,
                LIVE_MARKET_PARSER_VERSION,
            )
            .await?;
        let compact = parse_compact_market(
            &compact_raw,
            condition_id,
            &market.ordered_outcome_token_ids,
        )
        .map_err(|error| LiveVenueAdapterError::MarketValidation(error.to_string()))?;
        if compact.minimum_order_size != market.minimum_order_size
            || compact.minimum_tick_size != market.minimum_tick_size
            || compact.neg_risk != market.neg_risk
        {
            return Err(LiveVenueAdapterError::MarketValidation(
                "compact and long CLOB market rules disagree".to_owned(),
            ));
        }
        // validate_live_market admits only active=true, closed=false evidence from BOTH
        // payloads. Therefore the same observed payloads prove the entry settlement status is
        // unresolved; any resolved/ambiguous shape was already rejected above.
        let settlement = VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: condition_id.clone(),
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: blake3::hash(&clob_raw).to_hex().to_string(),
            source_timestamp_unix: None,
            observed_at_unix: now.unix_timestamp(),
            parser_version: 1,
            freshness_window_secs: LIVE_MARKET_FRESHNESS_SECS,
        };
        Ok(LiveAdmissionArtifact {
            market,
            settlement,
            fee_schedule: compact.fee_schedule,
            receipts: AdmissionReceipts {
                gamma: gamma_receipt,
                clob_long: clob_long_receipt,
                clob_compact: clob_compact_receipt,
            },
        })
    }

    async fn fetch_and_record(
        &self,
        url: &str,
        source_id: &str,
        schema_version: u32,
        parser_version: u32,
    ) -> Result<(Vec<u8>, pe_event_log::AppendReceipt), LiveVenueAdapterError> {
        let response = self
            .client
            .get(url)
            .timeout(Duration::from_secs(MARKET_REQUEST_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|error| LiveVenueAdapterError::MarketTransport(error.to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| LiveVenueAdapterError::MarketTransport(error.to_string()))?;
        let received_at = OffsetDateTime::now_utc();
        let receipt = self
            .source_log
            .append(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version,
                parser_version,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: body.clone(),
            })
            .await
            .map_err(|SourceLogHandleError::Closed| LiveVenueAdapterError::SourceLogClosed)?;
        if !status.is_success() {
            return Err(LiveVenueAdapterError::MarketStatus(status.as_u16()));
        }
        Ok((body, receipt))
    }
}

#[derive(Clone)]
enum StatusAuthentication {
    RelayerApiKey {
        api_key: String,
        address: String,
    },
    /// Builder status signing is owned by the venue client, whose by-id request builder is not
    /// exported. Preserve a typed fail-closed posture rather than reproducing crypto here.
    BuilderUnavailable,
}

/// One per-account Relayer adapter. Submission delegates to the venue client; recovery polls the
/// exported transaction path because `RelayerTransportClient` has no public by-id query.
pub struct LiveRedemptionAdapter {
    transport: RelayerTransportClient,
    http: reqwest::Client,
    base_url: String,
    custody: CustodyKind,
    authentication: StatusAuthentication,
}

impl LiveRedemptionAdapter {
    pub fn new(
        credentials: RelayerCredentials,
        custody: CustodyKind,
        policy: RelayerPollPolicy,
    ) -> Result<Self, LiveVenueAdapterError> {
        let authentication = match &credentials {
            RelayerCredentials::RelayerApiKey(credentials) => StatusAuthentication::RelayerApiKey {
                api_key: credentials.api_key.clone(),
                address: credentials.address.clone(),
            },
            RelayerCredentials::BuilderApiKey(_) => StatusAuthentication::BuilderUnavailable,
        };
        let transport = RelayerTransportClient::new(RELAYER_BASE_URL, credentials, policy.clone())
            .map_err(|error| LiveVenueAdapterError::Redemption(error.to_string()))?;
        let http = reqwest::Client::builder()
            .timeout(policy.request_timeout)
            .build()
            .map_err(|error| LiveVenueAdapterError::Redemption(error.to_string()))?;
        Ok(Self {
            transport,
            http,
            base_url: RELAYER_BASE_URL.to_owned(),
            custody,
            authentication,
        })
    }

    pub fn submission_body_hash(
        &self,
        request: &SignedRedemptionRequest,
        request_timestamp_unix: i64,
    ) -> Result<String, LiveVenueAdapterError> {
        self.transport
            .build_submission_at(request, request_timestamp_unix)
            .map(|submission| submission.body_hash)
            .map_err(|error| LiveVenueAdapterError::Redemption(error.to_string()))
    }

    async fn status(
        &self,
        transaction_id: &str,
    ) -> Result<RedemptionStatusObservation, RedemptionStatusReadError> {
        if transaction_id.is_empty()
            || !transaction_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(RedemptionStatusReadError {
                evidence: Vec::new(),
            });
        }
        let StatusAuthentication::RelayerApiKey { api_key, address } = &self.authentication else {
            return Err(RedemptionStatusReadError {
                evidence: Vec::new(),
            });
        };
        let (path, query) = match self.custody {
            CustodyKind::DepositWallet => (
                format!("{RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX}{transaction_id}"),
                Vec::new(),
            ),
            CustodyKind::Proxy | CustodyKind::Safe => (
                RELAYER_LEGACY_TRANSACTION_PATH.to_owned(),
                vec![("id", transaction_id)],
            ),
            CustodyKind::Eoa => {
                return Err(RedemptionStatusReadError {
                    evidence: Vec::new(),
                });
            }
        };
        let observed_at = OffsetDateTime::now_utc();
        let mut request = self
            .http
            .get(format!("{}{}", self.base_url, path))
            .header("RELAYER_API_KEY", api_key)
            .header("RELAYER_API_KEY_ADDRESS", address);
        if !query.is_empty() {
            request = request.query(&query);
        }
        let response = request
            .send()
            .await
            .map_err(|_| RedemptionStatusReadError {
                evidence: vec![RawHttpAttempt::TransportFailure(RawTransportFailure {
                    source_id: "polymarket-relayer-v2".to_owned(),
                    endpoint_kind: "redemption-status".to_owned(),
                    method: "GET".to_owned(),
                    path: path.clone(),
                    ordered_query: query
                        .iter()
                        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                        .collect(),
                    attempt_ordinal: 1,
                    observed_at,
                    received_at: OffsetDateTime::now_utc(),
                    error_class: TransportErrorClass::Connect,
                    schema_version: REDEMPTION_SCHEMA_VERSION,
                    parser_version: REDEMPTION_PARSER_VERSION,
                    adapter_version: REDEMPTION_ADAPTER_VERSION.to_owned(),
                })],
            })?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|_| RedemptionStatusReadError {
                evidence: Vec::new(),
            })?
            .to_vec();
        let raw = RawHttpResponse {
            source_id: "polymarket-relayer-v2".to_owned(),
            endpoint_kind: "redemption-status".to_owned(),
            method: "GET".to_owned(),
            path,
            ordered_query: query
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            status,
            headers: Vec::new(),
            body: body.clone(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at,
            received_at: OffsetDateTime::now_utc(),
            schema_version: REDEMPTION_SCHEMA_VERSION,
            parser_version: REDEMPTION_PARSER_VERSION,
            adapter_version: REDEMPTION_ADAPTER_VERSION.to_owned(),
        };
        let evidence = vec![RawHttpAttempt::Response(raw)];
        if !(200..300).contains(&status) {
            return Ok(RedemptionStatusObservation::Ambiguous { evidence });
        }
        let value: Value =
            serde_json::from_slice(&body).map_err(|_| RedemptionStatusReadError {
                evidence: evidence.clone(),
            })?;
        let value = match value {
            Value::Array(mut rows) if rows.len() == 1 => rows.remove(0),
            Value::Array(_) => {
                return Ok(RedemptionStatusObservation::Ambiguous { evidence });
            }
            value => value,
        };
        let parsed: RelayerStatusBody =
            serde_json::from_value(value).map_err(|_| RedemptionStatusReadError {
                evidence: evidence.clone(),
            })?;
        if !parsed.transaction_id.eq_ignore_ascii_case(transaction_id) {
            return Ok(RedemptionStatusObservation::Ambiguous { evidence });
        }
        match parsed.state.as_str() {
            "STATE_CONFIRMED" => parsed
                .transaction_hash
                .filter(|hash| !hash.trim().is_empty())
                .map(|transaction_hash| RedemptionStatusObservation::Confirmed {
                    transaction_hash,
                    evidence: evidence.clone(),
                })
                .ok_or(RedemptionStatusReadError { evidence }),
            "STATE_INVALID" | "STATE_FAILED" => {
                Ok(RedemptionStatusObservation::TerminalFailure { evidence })
            }
            "STATE_NEW" | "STATE_EXECUTED" | "STATE_MINED" => {
                Ok(RedemptionStatusObservation::Pending { evidence })
            }
            _ => Ok(RedemptionStatusObservation::Ambiguous { evidence }),
        }
    }
}

#[derive(Deserialize)]
struct RelayerStatusBody {
    #[serde(rename = "transactionID", alias = "transactionId")]
    transaction_id: String,
    #[serde(default, rename = "transactionHash")]
    transaction_hash: Option<String>,
    state: String,
}

impl RedemptionTransport for LiveRedemptionAdapter {
    fn fetch_nonce<'a>(
        &'a self,
        signer_address: &'a str,
        custody: CustodyKind,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<pe_venue_polymarket::RelayerNonce, RedemptionTransportError>>
                + Send
                + 'a,
        >,
    > {
        self.transport.fetch_nonce(signer_address, custody)
    }

    fn submit_and_confirm<'a>(
        &'a self,
        request: &'a SignedRedemptionRequest,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        pe_venue_polymarket::ConfirmedRedemption,
                        RedemptionTransportError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        self.transport.submit_and_confirm(request)
    }
}

impl RedemptionStatusReader for LiveRedemptionAdapter {
    fn reconcile_transaction<'a>(
        &'a self,
        transaction_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<RedemptionStatusObservation, RedemptionStatusReadError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { self.status(transaction_id).await })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use axum::Router;
    use axum::http::{StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use pe_event_log::Reader;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::clob_book::ClobBookFetcher as _;

    const ADMISSION_CONDITION: &str =
        "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee";

    async fn admission_and_book_fixture(uri: Uri) -> Response {
        let path = uri.path();
        let body = if path == "/markets" {
            json!([{
                "conditionId": ADMISSION_CONDITION,
                "active": true,
                "closed": false,
                "acceptingOrders": true,
                "enableOrderBook": true,
                "negRisk": false,
                "outcomes": "[\"Yes\",\"No\"]",
                "clobTokenIds": "[\"11\",\"22\"]",
                "orderPriceMinTickSize": "0.01",
                "orderMinSize": "5",
                "secondsDelay": 0
            }])
        } else if path.strip_prefix("/markets/") == Some(ADMISSION_CONDITION) {
            json!({
                "condition_id": ADMISSION_CONDITION,
                "end_date_iso": "2026-09-06T12:00:00Z",
                "active": true,
                "closed": false,
                "accepting_orders": true,
                "enable_order_book": true,
                "minimum_order_size": "5",
                "minimum_tick_size": "0.01",
                "neg_risk": false,
                "seconds_delay": 0,
                "tokens": [
                    {"token_id": "11", "outcome": "Yes"},
                    {"token_id": "22", "outcome": "No"}
                ],
                "maker_base_fee": 0,
                "taker_base_fee": 0
            })
        } else if path.strip_prefix("/clob-markets/") == Some(ADMISSION_CONDITION) {
            json!({
                "c": ADMISSION_CONDITION,
                "t": [{"t":"11","o":"Yes"},{"t":"22","o":"No"}],
                "mts": 0.01,
                "mos": 5,
                "nr": false,
                "mbf": 0,
                "tbf": 0
            })
        } else if path == "/book" {
            json!({
                "asks": [{"price":"0.50","size":"5"}],
                "bids": []
            })
        } else {
            return StatusCode::NOT_FOUND.into_response();
        };
        (StatusCode::OK, axum::Json(body)).into_response()
    }

    /// PASS: Gamma-long, CLOB-long, compact, and the consumed book each append exactly once and
    /// return the receipt of the exact synchronized source frame.
    #[tokio::test]
    async fn admission_and_book_append_each_response_once() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(get(admission_and_book_fixture)),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let source_sink = crate::source_event_sink::SourceEventSink::open(&source_path).unwrap();
        let (source_log, source_rx) = crate::activity_ingest::SourceLogHandle::channel(8);
        let (trigger_tx, _trigger_rx) = tokio::sync::mpsc::channel(1);
        let coordinator = tokio::spawn(
            crate::activity_ingest::ActivityIngest::poll_only(
                source_sink,
                source_rx,
                trigger_tx,
                crate::health::new_shared_health_with_ws(false, true, 90),
            )
            .run(),
        );
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let admission = LiveAdmissionBuilder::new(
            client.clone(),
            base.clone(),
            base.clone(),
            source_log.clone(),
        )
        .build(
            &pe_core_types::PolymarketConditionId(ADMISSION_CONDITION.to_owned()),
            OffsetDateTime::now_utc(),
        )
        .await
        .unwrap();
        let book = crate::clob_book::ReqwestClobBookFetcher::new(client)
            .with_base_url(base)
            .with_source_log(source_log.clone())
            .fetch_book("11")
            .await
            .unwrap();

        let frames = Reader::replay(&source_path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(
            frames
                .iter()
                .map(|(_, frame)| frame.source_id.0.as_str())
                .collect::<Vec<_>>(),
            vec![
                "polymarket.gamma.markets",
                "polymarket.clob.markets",
                "polymarket.clob.compact-market",
                "polymarket.clob.book"
            ]
        );
        assert_eq!(admission.receipts.gamma.sequence, frames[0].0);
        assert_eq!(admission.receipts.clob_long.sequence, frames[1].0);
        assert_eq!(admission.receipts.clob_compact.sequence, frames[2].0);
        assert_eq!(book.source_receipt.unwrap().sequence, frames[3].0);

        drop(source_log);
        coordinator.abort();
        server.abort();
    }

    fn post_response(body: serde_json::Value) -> RawHttpResponse {
        RawHttpResponse {
            source_id: "test".to_owned(),
            endpoint_kind: "order-post".to_owned(),
            method: "POST".to_owned(),
            path: "/order".to_owned(),
            ordered_query: Vec::new(),
            status: 200,
            headers: Vec::new(),
            body: serde_json::to_vec(&body).unwrap(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: OffsetDateTime::UNIX_EPOCH,
            received_at: OffsetDateTime::UNIX_EPOCH,
            schema_version: 1,
            parser_version: 1,
            adapter_version: "test".to_owned(),
        }
    }

    /// PASS: an external account response carrying the former internal sentinel name remains
    /// literal response evidence and is classified with its separate request descriptor hash.
    #[test]
    fn account_response_accepts_literal_request_descriptor_named_header() {
        let account_id = AccountId::new("account").unwrap();
        let custody_wallet =
            WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap();
        let binding = LiveAccountBindingAudit::new(
            account_id,
            pe_execution_core::CredentialBindingIdentity {
                version: 1,
                key_id: "key".to_owned(),
            },
            custody_wallet,
            blake3::hash(b"credential").to_hex().to_string(),
        );
        let spender = CanaryV2Client::standard_spender().unwrap();
        let response = |endpoint_kind: &str,
                        path: &str,
                        ordered_query: Vec<(String, String)>,
                        headers: Vec<(String, String)>,
                        body: Vec<u8>| {
            RawEvidence::HttpResponse(RawHttpResponse {
                source_id: "polymarket-clob-v2".to_owned(),
                endpoint_kind: endpoint_kind.to_owned(),
                method: "GET".to_owned(),
                path: path.to_owned(),
                ordered_query,
                status: 200,
                headers,
                body,
                attempt_ordinal: 1,
                source_at: None,
                observed_at: OffsetDateTime::UNIX_EPOCH,
                received_at: OffsetDateTime::UNIX_EPOCH,
                schema_version: 1,
                parser_version: 1,
                adapter_version: pe_venue_polymarket::SDK_VERSION.to_owned(),
            })
        };
        let literal_header = (
            "x-pe-request-descriptor-blake3".to_owned(),
            "venue-value".to_owned(),
        );
        let evidence = vec![
            response(
                "geoblock",
                "/api/geoblock",
                Vec::new(),
                vec![literal_header.clone()],
                br#"{"blocked":false,"country":"US"}"#.to_vec(),
            ),
            response(
                "closed-only",
                "/auth/ban-status/closed-only",
                Vec::new(),
                Vec::new(),
                br#"{"closed_only":false}"#.to_vec(),
            ),
            response(
                "balance-allowance",
                "/balance-allowance",
                vec![
                    ("asset_type".to_owned(), "COLLATERAL".to_owned()),
                    ("signature_type".to_owned(), "3".to_owned()),
                ],
                Vec::new(),
                serde_json::to_vec(&json!({
                    "balance": "1000000",
                    "allowances": { spender.clone(): "1000000" },
                }))
                .unwrap(),
            ),
        ];

        let descriptor_hashes = bind_account_read_responses(&evidence, &binding).unwrap();
        let headers = evidence
            .iter()
            .find_map(|item| match item {
                RawEvidence::HttpResponse(response) if response.endpoint_kind == "geoblock" => {
                    Some(&response.headers)
                }
                RawEvidence::HttpResponse(_)
                | RawEvidence::HttpTransportFailure(_)
                | RawEvidence::Artifact(_) => None,
            })
            .unwrap();
        assert_eq!(headers.as_slice(), &[literal_header]);
        let attempts = evidence.into_iter().filter_map(raw_attempt).collect();
        let state = classify_account_responses(attempts, spender, descriptor_hashes).unwrap();
        assert_eq!(
            state.collateral_balance,
            CollateralAmount::from_atomic(1_000_000)
        );
    }

    /// PASS: post evidence retains exact fractional improved quantity for audit-only use.
    #[test]
    fn matched_post_classification_retains_fractional_price_improvement_amounts() {
        let response = post_response(json!({
            "errorMsg": null,
            "makingAmount": "4.05",
            "takingAmount": "10.125",
            "orderID": "venue-order",
            "status": "MATCHED",
            "success": true,
            "transactionHashes": [],
            "tradeIds": ["trade-1"]
        }));
        assert_eq!(
            classify_order_post_response(&response).unwrap(),
            LivePostClassification::Matched {
                venue_order_id: "venue-order".to_owned(),
                executed: LiveExecutedAmounts {
                    making_amount: dec!(4.05),
                    taking_amount: dec!(10.125),
                },
                transaction_hashes: Vec::new(),
            }
        );
    }

    #[test]
    fn matched_post_with_missing_amounts_is_unparseable_for_reconcile_first() {
        let response = post_response(json!({
            "errorMsg": null,
            "makingAmount": "",
            "takingAmount": "",
            "orderID": "venue-order",
            "status": "MATCHED",
            "success": true,
            "transactionHashes": [],
            "tradeIds": ["trade-1"]
        }));
        assert_eq!(
            classify_order_post_response(&response),
            Err(LivePostParseError::InvalidResponse)
        );
    }

    /// PASS: authenticated reconciliation preserves every distinct matching nonzero hash.
    #[test]
    fn reconciliation_retains_every_matching_transaction_hash() {
        let order_hash = format!("0x{}", "aa".repeat(32));
        let first = format!("0x{}", "11".repeat(32));
        let second = format!("0x{}", "22".repeat(32));
        let response = RawHttpResponse {
            endpoint_kind: "trades-page".to_owned(),
            body: serde_json::to_vec(&json!({"data":[
                {"taker_order_id": order_hash, "transaction_hash": second},
                {"taker_order_id": order_hash, "transaction_hash": first},
                {"taker_order_id": order_hash, "transaction_hash": second},
                {"taker_order_id": order_hash, "transaction_hash": format!("0x{}", "00".repeat(32))},
                {"taker_order_id": "other", "transaction_hash": format!("0x{}", "33".repeat(32))}
            ]}))
            .unwrap(),
            ..post_response(json!({}))
        };
        let result = classify_reconciliation(
            &[RawEvidence::HttpResponse(response)],
            &format!("0x{}", "aa".repeat(32)),
        )
        .unwrap();
        let transaction_hashes = match result {
            ReconciliationClassification::Matched {
                transaction_hashes, ..
            } => Some(transaction_hashes),
            ReconciliationClassification::Killed(_)
            | ReconciliationClassification::Rejected(_)
            | ReconciliationClassification::Cancel { .. }
            | ReconciliationClassification::Ambiguous => None,
        };
        assert_eq!(
            transaction_hashes,
            Some(vec![
                format!("0x{}", "11".repeat(32)),
                format!("0x{}", "22".repeat(32))
            ])
        );
    }
}
