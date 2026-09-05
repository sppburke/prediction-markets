//! Ordinary-live bindings from the concrete Polymarket clients to execution-core.
//!
//! The V2 client intentionally exposes raw, observed reconciliation rather than separate
//! authenticated account/order methods. This adapter consumes that one composite seam for both
//! account admission and order-hash recovery, preserving every raw observation in the
//! execution-core audit types. Market admission records fresh Gamma-long, CLOB-long, and compact
//! CLOB responses before composing the shared artifact. The long observations use a 60-second
//! freshness window; the independently fetched executable ladder keeps its stricter two-second
//! venue-owned bound.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pe_core_types::{
    CollateralAmount, Price, RawEvidence, RawHttpAttempt, RawHttpResponse, RawTransportFailure,
    ReceivedAt, SourceId, SourceTimestamp, TransportErrorClass,
};
use pe_event_log::{ContentType, EnvelopeIn};
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
        Ok(Self { client })
    }

    #[must_use]
    pub fn deposit_wallet(&self) -> String {
        self.client.deposit_wallet()
    }

    #[must_use]
    pub fn owner_signer(&self) -> String {
        self.client.owner_signer()
    }

    async fn account_state(
        &self,
        neg_risk: bool,
    ) -> Result<LiveVenueAccountState, LiveVenueAccountReadError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(RECONCILIATION_TIMEOUT_SECS);
        let (evidence, protocol_failure) = self.client.account_probe_raw(deadline).await;
        if protocol_failure.is_some() {
            return Err(account_read_error(
                LiveAccountReadFailure::Protocol,
                evidence,
            ));
        }
        parse_account_state(evidence, neg_risk)
    }

    pub(crate) async fn account_states(
        &self,
    ) -> Result<(LiveVenueAccountState, LiveVenueAccountState), LiveVenueAccountReadError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(RECONCILIATION_TIMEOUT_SECS);
        let (evidence, protocol_failure) = self.client.account_probe_raw(deadline).await;
        if protocol_failure.is_some() {
            return Err(account_read_error(
                LiveAccountReadFailure::Protocol,
                evidence,
            ));
        }
        let standard = parse_account_state(evidence.clone(), false)?;
        let neg_risk = parse_account_state(evidence, true)?;
        Ok((standard, neg_risk))
    }
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

enum ReconciliationClassification {
    Matched(String),
    Killed(Option<String>),
    Rejected(Option<String>),
    Cancel { order_id: String },
    Ambiguous,
}

impl ReconciliationClassification {
    fn into_venue_outcome(self) -> LiveVenueReconciledOutcome {
        match self {
            Self::Matched(venue_order_id) => LiveVenueReconciledOutcome::Matched { venue_order_id },
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
    let matching_trade = evidence.iter().find_map(|item| match item {
        RawEvidence::HttpResponse(response) if response.endpoint_kind == "trades-page" => {
            response_json(response)
                .ok()
                .and_then(|value| page_rows(&value).ok().map(|rows| rows.to_vec()))
                .and_then(|rows| {
                    rows.into_iter()
                        .find(|trade| trade_matches_order(trade, order_hash))
                })
        }
        RawEvidence::HttpResponse(_)
        | RawEvidence::HttpTransportFailure(_)
        | RawEvidence::Artifact(_) => None,
    });
    if let Some(trade) = matching_trade {
        let venue_order_id = trade
            .get("taker_order_id")
            .or_else(|| trade.get("takerOrderId"))
            .and_then(Value::as_str)
            .unwrap_or(order_hash)
            .to_owned();
        return Ok(ReconciliationClassification::Matched(venue_order_id));
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
        return Ok(ReconciliationClassification::Matched(order_id));
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

fn parse_account_state(
    evidence: Vec<RawEvidence>,
    neg_risk: bool,
) -> Result<LiveVenueAccountState, LiveVenueAccountReadError> {
    if evidence
        .iter()
        .any(|item| matches!(item, RawEvidence::HttpTransportFailure(_)))
    {
        return Err(account_read_error(
            LiveAccountReadFailure::Transport,
            evidence,
        ));
    }
    let response = |kind: &str| {
        evidence.iter().find_map(|item| match item {
            RawEvidence::HttpResponse(response) if response.endpoint_kind == kind => Some(response),
            RawEvidence::HttpResponse(_)
            | RawEvidence::HttpTransportFailure(_)
            | RawEvidence::Artifact(_) => None,
        })
    };
    let geoblock = response("geoblock")
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let closed_only_response = response("closed-only")
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let balance = response("balance-allowance")
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    if [geoblock, closed_only_response, balance]
        .iter()
        .any(|response| response.status == 401 || response.status == 403)
    {
        return Err(account_read_error(
            LiveAccountReadFailure::Authentication,
            evidence,
        ));
    }
    let geoblock_json = response_json(geoblock)
        .map_err(|_| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let closed_json = response_json(closed_only_response)
        .map_err(|_| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let balance_json = response_json(balance)
        .map_err(|_| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let blocked = geoblock_json
        .get("blocked")
        .and_then(Value::as_bool)
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let country = geoblock_json
        .get("country")
        .and_then(Value::as_str)
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let geoblocked = blocked && !matches!(country, "IE" | "JP" | "MT" | "NL");
    let closed_only = closed_json
        .get("closed_only")
        .and_then(Value::as_bool)
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let collateral_balance = atomic_amount(balance_json.get("balance"))
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let selected_spender = if neg_risk {
        CanaryV2Client::negrisk_spender()
    } else {
        CanaryV2Client::standard_spender()
    }
    .map_err(|_| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let allowances = balance_json
        .get("allowances")
        .and_then(Value::as_object)
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
    let allowance = allowances
        .iter()
        .find(|(spender, _)| spender.eq_ignore_ascii_case(&selected_spender))
        .and_then(|(_, value)| atomic_amount(Some(value)))
        .unwrap_or(CollateralAmount::ZERO);
    let observed_at = [geoblock, closed_only_response, balance]
        .iter()
        .map(|response| response.observed_at)
        .min()
        .ok_or_else(|| account_read_error(LiveAccountReadFailure::Protocol, evidence.clone()))?;
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
        evidence: evidence.into_iter().filter_map(raw_attempt).collect(),
    })
}

fn account_read_error(
    kind: LiveAccountReadFailure,
    evidence: Vec<RawEvidence>,
) -> LiveVenueAccountReadError {
    LiveVenueAccountReadError {
        kind,
        evidence: evidence.into_iter().filter_map(raw_attempt).collect(),
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

    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

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
}
