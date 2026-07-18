//! Pinned Polymarket V2 preparation and exactly-one-POST seam.

use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use pe_core_types::{
    CollateralAmount, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price, RawEvidence,
    RawHttpResponse, RawTransportFailure, ShareAmount, TransportErrorClass,
};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, Normal, PrivateKeySigner, Signer as _, Uuid};
use polymarket_client_sdk_v2::clob::types::request::{
    BalanceAllowanceRequest, OrdersRequest, TradesRequest,
};
use polymarket_client_sdk_v2::clob::types::{
    OrderPayload, OrderStatusType, OrderType, Side, SignatureType, TickSize,
};
use polymarket_client_sdk_v2::clob::{Client, Config, standard_v2_order_hash};
use polymarket_client_sdk_v2::types::{Address, B256, U256};
use polymarket_client_sdk_v2::{
    POLYGON, ResponseObservation, ResponseObserver, TransportFailureClass,
    TransportFailureObservation, contract_config,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const CLOB_V2_HOST: &str = "https://clob.polymarket.com";
pub const SDK_VERSION: &str = "0.7.0";
pub const SDK_ARCHIVE_SHA256: &str =
    "ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a";

#[derive(Clone)]
pub struct CanaryV2Credentials {
    pub private_key: String,
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    pub deposit_wallet: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2BuyRequest {
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub limit_price: Price,
    pub shares: ShareAmount,
    pub maximum_collateral: CollateralAmount,
    pub tick_size: Price,
    pub metadata_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedPolymarketBuy {
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub maker: String,
    pub signer: String,
    pub funder: String,
    pub verifying_contract: String,
    pub spender: String,
    pub exchange_domain_version: u8,
    pub neg_risk: bool,
    pub side: String,
    pub salt: String,
    pub timestamp_ms: u64,
    pub expiration: String,
    pub maker_collateral: CollateralAmount,
    pub taker_shares: ShareAmount,
    pub limit_price: Price,
    pub minimum_tick_size: Price,
    pub signature_type: u8,
    pub order_type: String,
    pub post_only: bool,
    pub defer_exec: bool,
    pub metadata: String,
    pub builder: String,
    pub order_hash: String,
    pub post_body_hash: String,
    pub sdk_version: String,
    pub sdk_archive_sha256: String,
    pub metadata_hashes: Vec<String>,
    pub worst_case_debit: CollateralAmount,
}

/// Sensitive serialized bytes remain in memory and deliberately have no `Debug` implementation.
pub struct PreparedSubmission {
    prepared: PreparedPolymarketBuy,
    serialized_body: Vec<u8>,
}

impl PreparedSubmission {
    #[must_use]
    pub fn prepared(&self) -> &PreparedPolymarketBuy {
        &self.prepared
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostOnceResult {
    pub success: bool,
    /// True only when the response proves acceptance or rejection. A gateway
    /// status or a contradictory payload remains ambiguous until reconciliation.
    pub definitive: bool,
    pub order_id: String,
    pub error_message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CanaryV2Error {
    #[error("credential or SDK client validation failed: {0}")]
    Client(String),
    #[error("system clock is before the Unix epoch")]
    Clock,
    #[error("request is not an exact whole-share, fee-free BUY")]
    Request,
    #[error("SDK produced a non-V2 or mismatched order")]
    PreparedMismatch,
    #[error("serialized order validation failed: {0}")]
    Wire(String),
    #[error("exact amount overflow")]
    Amount,
    #[error("POST body changed after reservation")]
    BodyChanged,
    #[error("POST failed: {0}")]
    Post(String),
}

#[derive(Default)]
struct ObservationCollector(Mutex<Vec<ResponseObservation>>);

impl ResponseObserver for ObservationCollector {
    fn observe(&self, observation: ResponseObservation) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observation);
    }
}

pub struct CanaryV2Client {
    client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    deposit_wallet: Address,
    observer: Arc<ObservationCollector>,
}

impl CanaryV2Client {
    pub async fn new(credentials: CanaryV2Credentials, host: &str) -> Result<Self, CanaryV2Error> {
        if host != CLOB_V2_HOST && !cfg!(test) {
            return Err(CanaryV2Error::Client(
                "canary requires the explicit production V2 host".to_owned(),
            ));
        }
        let signer = PrivateKeySigner::from_str(&credentials.private_key)
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?
            .with_chain_id(Some(POLYGON));
        let deposit_wallet = Address::from_str(&credentials.deposit_wallet)
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        let api_key = Uuid::parse_str(&credentials.api_key)
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        let observer = Arc::new(ObservationCollector::default());
        let observer_trait: Arc<dyn ResponseObserver> = observer.clone();
        let config = if cfg!(test) {
            Config::builder()
                .response_observer(observer_trait)
                .geoblock_host(host)
                .build()
        } else {
            Config::builder().response_observer(observer_trait).build()
        };
        let client = Client::new(host, config)
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                api_key,
                credentials.api_secret,
                credentials.api_passphrase,
            ))
            .funder(deposit_wallet)
            .signature_type(SignatureType::Poly1271)
            .authenticate()
            .await
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        Ok(Self {
            client,
            signer,
            deposit_wallet,
            observer,
        })
    }

    pub async fn prepare_buy(
        &self,
        request: V2BuyRequest,
    ) -> Result<PreparedSubmission, CanaryV2Error> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CanaryV2Error::Clock)?;
        if request.shares.atomic() == 0
            || !request.shares.atomic().is_multiple_of(1_000_000)
            || request.limit_price == Price::ZERO
            || request.maximum_collateral
                != CollateralAmount::from_decimal_exact(
                    request.shares.to_decimal() * request.limit_price.0,
                )
                .map_err(|_| CanaryV2Error::Amount)?
        {
            return Err(CanaryV2Error::Request);
        }
        let token_id = U256::from_str(&request.token_id.0)
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        let tick_size = TickSize::try_from(request.tick_size.0)
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        self.client.set_tick_size(token_id, tick_size);
        self.client.set_neg_risk(token_id, false);

        let whole_shares = request.shares.atomic() / 1_000_000;
        let signable = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Buy)
            .price(request.limit_price.0)
            .size(polymarket_client_sdk_v2::types::Decimal::from(whole_shares))
            .order_type(OrderType::FOK)
            .post_only(false)
            .defer_exec(false)
            .metadata(B256::ZERO)
            .builder_code(B256::ZERO)
            .build()
            .await
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        let signed = self
            .client
            .sign(&self.signer, signable)
            .await
            .map_err(|e| CanaryV2Error::Client(e.to_string()))?;
        let payload = match &signed.payload {
            OrderPayload::V2(payload) => payload,
            OrderPayload::V1(_) => return Err(CanaryV2Error::PreparedMismatch),
            _ => return Err(CanaryV2Error::PreparedMismatch),
        };
        let order = &payload.order;
        let maker = order.maker.to_string();
        let signer = order.signer.to_string();
        let wallet = self.deposit_wallet.to_string();
        let maker_collateral = amount_from_u256(&order.makerAmount)?;
        let taker_shares = share_from_u256(&order.takerAmount)?;
        if maker != wallet
            || signer != wallet
            || order.tokenId != token_id
            || order.side != Side::Buy as u8
            || order.signatureType != SignatureType::Poly1271 as u8
            || maker_collateral != request.maximum_collateral
            || taker_shares != request.shares
            || payload.expiration != U256::ZERO
            || order.metadata != B256::ZERO
            || order.builder != B256::ZERO
            || signed.order_type != OrderType::FOK
            || signed.post_only != Some(false)
            || signed.defer_exec != Some(false)
        {
            return Err(CanaryV2Error::PreparedMismatch);
        }

        let serialized_body =
            serde_json::to_vec(&signed).map_err(|e| CanaryV2Error::Wire(e.to_string()))?;
        validate_wire(&serialized_body)?;
        let order_hash = standard_order_hash(order)?;
        let post_body_hash = blake3::hash(&serialized_body);
        let standard = contract_config(POLYGON, false)
            .and_then(|config| config.exchange_v2)
            .ok_or_else(|| CanaryV2Error::Client("missing standard V2 contract".to_owned()))?;
        let prepared = PreparedPolymarketBuy {
            condition_id: request.condition_id,
            outcome_id: request.outcome_id,
            token_id: request.token_id,
            maker,
            signer,
            funder: wallet,
            verifying_contract: standard.to_string(),
            spender: standard.to_string(),
            exchange_domain_version: 2,
            neg_risk: false,
            side: "BUY".to_owned(),
            salt: order.salt.to_string(),
            timestamp_ms: order
                .timestamp
                .to_string()
                .parse()
                .map_err(|_| CanaryV2Error::PreparedMismatch)?,
            expiration: payload.expiration.to_string(),
            maker_collateral,
            taker_shares,
            limit_price: request.limit_price,
            minimum_tick_size: request.tick_size,
            signature_type: order.signatureType,
            order_type: "FOK".to_owned(),
            post_only: false,
            defer_exec: false,
            metadata: order.metadata.to_string(),
            builder: order.builder.to_string(),
            order_hash,
            post_body_hash: post_body_hash.to_hex().to_string(),
            sdk_version: SDK_VERSION.to_owned(),
            sdk_archive_sha256: SDK_ARCHIVE_SHA256.to_owned(),
            metadata_hashes: request.metadata_hashes,
            worst_case_debit: maker_collateral,
        };
        Ok(PreparedSubmission {
            prepared,
            serialized_body,
        })
    }

    pub async fn post_order_once(
        &self,
        submission: PreparedSubmission,
    ) -> Result<RawHttpResponse, RawTransportFailure> {
        if blake3::hash(&submission.serialized_body).to_hex().as_str()
            != submission.prepared.post_body_hash
        {
            return Err(local_failure(
                "order-post",
                "POST",
                "/order",
                TransportErrorClass::RequestBuild,
                OffsetDateTime::now_utc(),
            ));
        }
        let _ = self.take_observations();
        let observed_at = OffsetDateTime::now_utc();
        self.client
            .post_order_once_raw(submission.serialized_body)
            .await
            .map(|observation| {
                let source_at = source_at(&observation.headers);
                RawHttpResponse {
                    source_id: "polymarket-clob-v2".to_owned(),
                    endpoint_kind: "order-post".to_owned(),
                    method: observation.method,
                    path: observation.path,
                    ordered_query: observation.ordered_query,
                    status: observation.status,
                    headers: observation.headers,
                    body: observation.body,
                    attempt_ordinal: 1,
                    source_at,
                    observed_at: observation.observed_at.into(),
                    received_at: observation.received_at.into(),
                    schema_version: 1,
                    parser_version: 1,
                    adapter_version: SDK_VERSION.to_owned(),
                }
            })
            .map_err(|error| {
                raw_failure(&error, "order-post", 1).unwrap_or_else(|| {
                    local_failure(
                        "order-post",
                        "POST",
                        "/order",
                        TransportErrorClass::RequestBuild,
                        observed_at,
                    )
                })
            })
    }

    pub async fn cancel_order_once(
        &self,
        order_id: &str,
    ) -> Result<RawHttpResponse, RawTransportFailure> {
        if order_id.trim().is_empty() {
            return Err(local_failure(
                "order-cancel",
                "DELETE",
                "/order",
                TransportErrorClass::RequestBuild,
                OffsetDateTime::now_utc(),
            ));
        }
        let observed_at = OffsetDateTime::now_utc();
        self.client
            .cancel_order_once_raw(order_id)
            .await
            .map(|response| raw_response("order-cancel", response))
            .map_err(|error| {
                raw_failure(&error, "order-cancel", 1).unwrap_or_else(|| {
                    local_failure(
                        "order-cancel",
                        "DELETE",
                        "/order",
                        TransportErrorClass::RequestBuild,
                        observed_at,
                    )
                })
            })
    }

    pub fn parse_post_response(
        observation: &RawHttpResponse,
    ) -> Result<PostOnceResult, CanaryV2Error> {
        if !(200..300).contains(&observation.status) {
            return Ok(PostOnceResult {
                success: false,
                definitive: false,
                order_id: String::new(),
                error_message: Some(format!("HTTP status {}", observation.status)),
            });
        }
        let response: polymarket_client_sdk_v2::clob::types::response::PostOrderResponse =
            serde_json::from_slice(&observation.body)
                .map_err(|error| CanaryV2Error::Post(format!("response decode: {error}")))?;
        let matched = response.status == OrderStatusType::Matched;
        let definitive = !response.success || matched;
        Ok(PostOnceResult {
            success: response.success && matched,
            definitive,
            order_id: response.order_id,
            error_message: if response.success && !matched {
                Some(format!("unexpected FOK status {}", response.status))
            } else {
                response.error_msg
            },
        })
    }

    pub async fn clob_reconciliation_raw(
        &self,
        pending_order_hash: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> (Vec<RawEvidence>, Option<String>) {
        const PAGE_LIMIT: usize = 100;

        let deadline = deadline.into_std();
        let mut evidence = Vec::new();
        macro_rules! capture {
            ($endpoint:literal, $future:expr) => {
                match $future.await {
                    Ok(observation) => evidence.push(RawEvidence::HttpResponse(raw_response(
                        $endpoint,
                        observation,
                    ))),
                    Err(error) => match raw_failure(&error, $endpoint, 1) {
                        Some(failure) => {
                            evidence.push(RawEvidence::HttpTransportFailure(failure));
                            return (evidence, None);
                        }
                        None => return (evidence, Some(error.to_string())),
                    },
                }
            };
        }
        capture!("geoblock", self.client.check_geoblock_raw(Some(deadline)));
        capture!(
            "closed-only",
            self.client.closed_only_mode_raw(Some(deadline))
        );
        capture!(
            "balance-allowance",
            self.client
                .balance_allowance_raw(BalanceAllowanceRequest::default(), Some(deadline))
        );
        if let Some(order_hash) = pending_order_hash {
            capture!(
                "exact-order",
                self.client.order_raw(order_hash, Some(deadline))
            );
        }

        let mut cursor = None;
        for _ in 0..PAGE_LIMIT {
            let response = match self
                .client
                .orders_raw(&OrdersRequest::default(), cursor.clone(), Some(deadline))
                .await
            {
                Ok(observation) => raw_response("orders-page", observation),
                Err(error) => match raw_failure(&error, "orders-page", 1) {
                    Some(failure) => {
                        evidence.push(RawEvidence::HttpTransportFailure(failure));
                        return (evidence, None);
                    }
                    None => return (evidence, Some(error.to_string())),
                },
            };
            cursor = match capture_page_and_next_cursor(&mut evidence, response) {
                Ok(cursor) => cursor,
                Err(error) => return (evidence, Some(error)),
            };
            if cursor.is_none() {
                break;
            }
        }
        if cursor.is_some() {
            return (evidence, Some("orders pagination cap reached".to_owned()));
        }

        cursor = None;
        for _ in 0..PAGE_LIMIT {
            let response = match self
                .client
                .trades_raw(&TradesRequest::default(), cursor.clone(), Some(deadline))
                .await
            {
                Ok(observation) => raw_response("trades-page", observation),
                Err(error) => match raw_failure(&error, "trades-page", 1) {
                    Some(failure) => {
                        evidence.push(RawEvidence::HttpTransportFailure(failure));
                        return (evidence, None);
                    }
                    None => return (evidence, Some(error.to_string())),
                },
            };
            cursor = match capture_page_and_next_cursor(&mut evidence, response) {
                Ok(cursor) => cursor,
                Err(error) => return (evidence, Some(error)),
            };
            if cursor.is_none() {
                break;
            }
        }
        if cursor.is_some() {
            return (evidence, Some("trades pagination cap reached".to_owned()));
        }
        (evidence, None)
    }

    #[must_use]
    pub fn deposit_wallet(&self) -> String {
        self.deposit_wallet.to_string()
    }

    #[must_use]
    pub fn owner_signer(&self) -> String {
        self.signer.address().to_string()
    }

    pub fn standard_spender() -> Result<String, CanaryV2Error> {
        contract_config(POLYGON, false)
            .and_then(|config| config.exchange_v2)
            .map(|address| address.to_string())
            .ok_or_else(|| CanaryV2Error::Client("missing standard V2 contract".to_owned()))
    }

    #[must_use]
    pub fn take_observations(&self) -> Vec<ResponseObservation> {
        std::mem::take(
            &mut *self
                .observer
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    #[must_use]
    pub fn take_raw_observations(&self, endpoint_kind: &str) -> Vec<RawHttpResponse> {
        self.take_observations()
            .into_iter()
            .map(|observation| raw_response(endpoint_kind, observation))
            .collect()
    }
}

fn raw_response(endpoint_kind: &str, observation: ResponseObservation) -> RawHttpResponse {
    let source_at = source_at(&observation.headers);
    RawHttpResponse {
        source_id: "polymarket-clob-v2".to_owned(),
        endpoint_kind: endpoint_kind.to_owned(),
        method: observation.method,
        path: observation.path,
        ordered_query: observation.ordered_query,
        status: observation.status,
        headers: observation.headers,
        body: observation.body,
        attempt_ordinal: 1,
        source_at,
        observed_at: observation.observed_at.into(),
        received_at: observation.received_at.into(),
        schema_version: 1,
        parser_version: 1,
        adapter_version: SDK_VERSION.to_owned(),
    }
}

fn raw_failure(
    error: &polymarket_client_sdk_v2::error::Error,
    endpoint_kind: &str,
    attempt_ordinal: u32,
) -> Option<RawTransportFailure> {
    let failure = error.downcast_ref::<TransportFailureObservation>()?;
    Some(RawTransportFailure {
        source_id: "polymarket-clob-v2".to_owned(),
        endpoint_kind: endpoint_kind.to_owned(),
        method: failure.method.clone(),
        path: failure.path.clone(),
        ordered_query: failure.ordered_query.clone(),
        attempt_ordinal,
        observed_at: failure.observed_at.into(),
        received_at: failure.received_at.into(),
        error_class: match failure.error_class {
            TransportFailureClass::Timeout => TransportErrorClass::Timeout,
            TransportFailureClass::Connect => TransportErrorClass::Connect,
            TransportFailureClass::BodyRead => TransportErrorClass::BodyRead,
            TransportFailureClass::Other => TransportErrorClass::Other,
        },
        schema_version: 1,
        parser_version: 1,
        adapter_version: SDK_VERSION.to_owned(),
    })
}

fn local_failure(
    endpoint_kind: &str,
    method: &str,
    path: &str,
    error_class: TransportErrorClass,
    observed_at: OffsetDateTime,
) -> RawTransportFailure {
    RawTransportFailure {
        source_id: "polymarket-clob-v2".to_owned(),
        endpoint_kind: endpoint_kind.to_owned(),
        method: method.to_owned(),
        path: path.to_owned(),
        ordered_query: Vec::new(),
        attempt_ordinal: 1,
        observed_at,
        received_at: OffsetDateTime::now_utc(),
        error_class,
        schema_version: 1,
        parser_version: 1,
        adapter_version: SDK_VERSION.to_owned(),
    }
}

fn source_at(headers: &[(String, String)]) -> Option<OffsetDateTime> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("date"))
        .and_then(|(_, value)| {
            OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822).ok()
        })
}

fn next_cursor(response: &RawHttpResponse) -> Result<Option<String>, String> {
    if !(200..300).contains(&response.status) {
        return Err(format!(
            "reconciliation page returned HTTP {}",
            response.status
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|error| format!("reconciliation page is malformed JSON: {error}"))?;
    let cursor = value
        .as_object()
        .and_then(|object| object.get("next_cursor"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "reconciliation page omitted string next_cursor".to_owned())?;
    if cursor == "LTE=" {
        Ok(None)
    } else if cursor.is_empty() {
        Err("reconciliation page returned an empty next_cursor".to_owned())
    } else {
        Ok(Some(cursor.to_owned()))
    }
}

fn capture_page_and_next_cursor(
    evidence: &mut Vec<RawEvidence>,
    response: RawHttpResponse,
) -> Result<Option<String>, String> {
    evidence.push(RawEvidence::HttpResponse(response));
    evidence
        .last()
        .ok_or_else(|| "reconciliation response capture failed".to_owned())
        .and_then(|observation| match observation {
            RawEvidence::HttpResponse(response) => next_cursor(response),
            RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => {
                Err("reconciliation response capture changed kind".to_owned())
            }
        })
}

fn amount_from_u256(value: &U256) -> Result<CollateralAmount, CanaryV2Error> {
    value
        .to_string()
        .parse::<u64>()
        .map(CollateralAmount::from_atomic)
        .map_err(|_| CanaryV2Error::Amount)
}

fn share_from_u256(value: &U256) -> Result<ShareAmount, CanaryV2Error> {
    value
        .to_string()
        .parse::<u64>()
        .map(ShareAmount::from_atomic)
        .map_err(|_| CanaryV2Error::Amount)
}

fn validate_wire(bytes: &[u8]) -> Result<(), CanaryV2Error> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| CanaryV2Error::Wire(e.to_string()))?;
    if value.get("orderType").and_then(serde_json::Value::as_str) != Some("FOK")
        || value.get("postOnly").and_then(serde_json::Value::as_bool) != Some(false)
        || value.get("deferExec").and_then(serde_json::Value::as_bool) != Some(false)
        || value
            .pointer("/order/expiration")
            .and_then(serde_json::Value::as_str)
            != Some("0")
        || value
            .pointer("/order/signatureType")
            .and_then(serde_json::Value::as_u64)
            != Some(3)
    {
        return Err(CanaryV2Error::Wire(
            "required V2 FOK fields are absent or changed".to_owned(),
        ));
    }
    Ok(())
}

fn standard_order_hash(
    order: &polymarket_client_sdk_v2::clob::types::OrderV2,
) -> Result<String, CanaryV2Error> {
    standard_v2_order_hash(order)
        .map(|hash| hash.to_string())
        .ok_or(CanaryV2Error::PreparedMismatch)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::{Path as AxumPath, State};
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

    #[derive(Clone)]
    struct ServerState {
        posts: Arc<AtomicUsize>,
        body: Arc<Mutex<Vec<u8>>>,
        content_type: Arc<Mutex<Option<String>>>,
        version: Arc<AtomicU32>,
        order_status: Arc<AtomicU16>,
        order_delay_ms: Arc<AtomicU64>,
        geoblock_delay_ms: Arc<AtomicU64>,
        balance_delay_ms: Arc<AtomicU64>,
        orders_calls: Arc<AtomicUsize>,
        second_orders_delay_ms: Arc<AtomicU64>,
        exact_order_calls: Arc<AtomicUsize>,
        order_response: Arc<Mutex<serde_json::Value>>,
    }

    impl Default for ServerState {
        fn default() -> Self {
            Self {
                posts: Arc::new(AtomicUsize::new(0)),
                body: Arc::new(Mutex::new(Vec::new())),
                content_type: Arc::new(Mutex::new(None)),
                version: Arc::new(AtomicU32::new(2)),
                order_status: Arc::new(AtomicU16::new(200)),
                order_delay_ms: Arc::new(AtomicU64::new(0)),
                geoblock_delay_ms: Arc::new(AtomicU64::new(0)),
                balance_delay_ms: Arc::new(AtomicU64::new(0)),
                orders_calls: Arc::new(AtomicUsize::new(0)),
                second_orders_delay_ms: Arc::new(AtomicU64::new(0)),
                exact_order_calls: Arc::new(AtomicUsize::new(0)),
                order_response: Arc::new(Mutex::new(matched_response())),
            }
        }
    }

    fn matched_response() -> serde_json::Value {
        json!({
            "errorMsg": null,
            "makingAmount": "0.5",
            "takingAmount": "5",
            "orderID": "0xorder",
            "status": "MATCHED",
            "success": true,
            "transactionHashes": [],
            "tradeIds": ["trade-1"]
        })
    }

    async fn version(State(state): State<ServerState>) -> axum::Json<serde_json::Value> {
        axum::Json(json!({"version": state.version.load(Ordering::SeqCst)}))
    }

    async fn order(State(state): State<ServerState>, headers: HeaderMap, body: Bytes) -> Response {
        state.posts.fetch_add(1, Ordering::SeqCst);
        *state.body.lock().unwrap() = body.to_vec();
        *state.content_type.lock().unwrap() = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let delay = state.order_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let status = StatusCode::from_u16(state.order_status.load(Ordering::SeqCst)).unwrap();
        let response = state.order_response.lock().unwrap().clone();
        (status, axum::Json(response)).into_response()
    }

    async fn geoblock(State(state): State<ServerState>) -> axum::Json<serde_json::Value> {
        let delay = state.geoblock_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        axum::Json(json!({"blocked": false}))
    }

    async fn closed_only() -> axum::Json<serde_json::Value> {
        axum::Json(json!({"closed_only": false}))
    }

    async fn balance(State(state): State<ServerState>) -> axum::Json<serde_json::Value> {
        let delay = state.balance_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        axum::Json(json!({"balance": "400000000", "allowances": {}}))
    }

    async fn orders(State(state): State<ServerState>, uri: Uri) -> axum::Json<serde_json::Value> {
        let call = state.orders_calls.fetch_add(1, Ordering::SeqCst);
        if call == 1 {
            tokio::time::sleep(Duration::from_millis(
                state.second_orders_delay_ms.load(Ordering::SeqCst),
            ))
            .await;
        }
        let has_cursor = uri
            .query()
            .is_some_and(|query| query.contains("next_cursor="));
        axum::Json(json!({
            "data": [],
            "next_cursor": if has_cursor { "LTE=" } else { "abc" }
        }))
    }

    async fn exact_order(
        State(state): State<ServerState>,
        AxumPath(order_id): AxumPath<String>,
    ) -> Response {
        state.exact_order_calls.fetch_add(1, Ordering::SeqCst);
        (StatusCode::NOT_FOUND, axum::Json(json!({"id": order_id}))).into_response()
    }

    async fn server(state: ServerState) -> String {
        let app = Router::new()
            .route("/version", get(version))
            .route("/api/geoblock", get(geoblock))
            .route("/auth/ban-status/closed-only", get(closed_only))
            .route("/balance-allowance", get(balance))
            .route("/data/orders", get(orders))
            .route("/data/order/{order_id}", get(exact_order))
            .route("/order", post(order))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    async fn make_client(host: &str) -> Result<CanaryV2Client, CanaryV2Error> {
        CanaryV2Client::new(
            CanaryV2Credentials {
                private_key: "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                    .to_owned(),
                api_key: Uuid::nil().to_string(),
                api_secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
                api_passphrase: "test-passphrase".to_owned(),
                deposit_wallet: "0x1111111111111111111111111111111111111111".to_owned(),
            },
            host,
        )
        .await
    }

    async fn client() -> (CanaryV2Client, ServerState) {
        let state = ServerState::default();
        let host = server(state.clone()).await;
        let client = make_client(&host).await.unwrap();
        (client, state)
    }

    async fn prepared(client: &CanaryV2Client) -> Result<PreparedSubmission, CanaryV2Error> {
        client
            .prepare_buy(V2BuyRequest {
                condition_id: PolymarketConditionId("0xcondition".to_owned()),
                outcome_id: OutcomeId(0),
                token_id: PolymarketTokenId("11".to_owned()),
                limit_price: Price::new(dec!(0.10)).unwrap(),
                shares: ShareAmount::from_atomic(5_000_000),
                maximum_collateral: CollateralAmount::from_atomic(500_000),
                tick_size: Price::new(dec!(0.01)).unwrap(),
                metadata_hashes: vec!["metadata-hash".to_owned()],
            })
            .await
    }

    #[tokio::test]
    async fn prepares_v2_fok_and_posts_exact_bytes_once() {
        let (client, state) = client().await;
        let submission = prepared(&client).await.unwrap();
        assert_eq!(submission.prepared().order_type, "FOK");
        assert!(!submission.prepared().post_only);
        assert!(!submission.prepared().defer_exec);
        assert_eq!(submission.prepared().worst_case_debit.atomic(), 500_000);
        let version_observations = client.take_observations();
        assert_eq!(version_observations.len(), 1);
        assert_eq!(version_observations[0].path, "/version");

        let raw = client.post_order_once(submission).await.unwrap();
        assert_eq!(raw.path, "/order");
        let result = CanaryV2Client::parse_post_response(&raw).unwrap();
        assert!(result.success);
        assert_eq!(result.order_id, "0xorder");
        assert_eq!(state.posts.load(Ordering::SeqCst), 1);
        assert_eq!(
            state.content_type.lock().unwrap().as_deref(),
            Some("application/json")
        );
        let body: serde_json::Value = serde_json::from_slice(&state.body.lock().unwrap()).unwrap();
        assert_eq!(body["orderType"], "FOK");
        assert_eq!(body["postOnly"], false);
        assert_eq!(body["deferExec"], false);
        assert_eq!(body["order"]["makerAmount"], "500000");
        assert_eq!(body["order"]["takerAmount"], "5000000");
    }

    #[tokio::test]
    async fn version_mismatch_fails_before_any_post() {
        let state = ServerState::default();
        state.version.store(1, Ordering::SeqCst);
        let host = server(state.clone()).await;
        let client = make_client(&host).await.unwrap();

        assert!(
            prepared(&client).await.is_err(),
            "a V1 venue response must fail V2 preparation closed"
        );
        assert_eq!(state.posts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn venue_rejection_posts_once_and_fails_closed() {
        let (client, state) = client().await;
        *state.order_response.lock().unwrap() = json!({
            "errorMsg": "rejected",
            "makingAmount": "0",
            "takingAmount": "0",
            "orderID": "",
            "status": "MATCHED",
            "success": false,
            "transactionHashes": [],
            "tradeIds": []
        });

        let raw = client
            .post_order_once(prepared(&client).await.unwrap())
            .await
            .unwrap();
        let result = CanaryV2Client::parse_post_response(&raw).unwrap();
        assert!(!result.success);
        assert_eq!(result.error_message.as_deref(), Some("rejected"));
        assert_eq!(state.posts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn server_error_posts_once_and_fails_closed() {
        let (client, state) = client().await;
        state.order_status.store(500, Ordering::SeqCst);

        let raw = client
            .post_order_once(prepared(&client).await.unwrap())
            .await
            .unwrap();
        let result = CanaryV2Client::parse_post_response(&raw).unwrap();
        assert!(!result.success);
        assert_eq!(result.error_message.as_deref(), Some("HTTP status 500"));
        assert_eq!(state.posts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caller_timeout_does_not_retry_the_post_seam() {
        let (client, state) = client().await;
        state.order_delay_ms.store(100, Ordering::SeqCst);

        let result = tokio::time::timeout(
            Duration::from_millis(10),
            client.post_order_once(prepared(&client).await.unwrap()),
        )
        .await;
        assert!(result.is_err());
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.posts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(state.posts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn authenticated_reconciliation_timeout_retains_exact_request_identity() {
        let (client, state) = client().await;
        state.geoblock_delay_ms.store(100, Ordering::SeqCst);

        let (evidence, protocol_failure) = client
            .clob_reconciliation_raw(
                None,
                tokio::time::Instant::now() + Duration::from_millis(10),
            )
            .await;

        assert!(protocol_failure.is_none());
        assert!(matches!(
            evidence.as_slice(),
            [RawEvidence::HttpTransportFailure(_)]
        ));
        let failure = evidence
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpTransportFailure(failure) => Some(failure),
                RawEvidence::HttpResponse(_) | RawEvidence::Artifact(_) => None,
            })
            .expect("transport failure was asserted above");
        assert_eq!(failure.source_id, "polymarket-clob-v2");
        assert_eq!(failure.endpoint_kind, "geoblock");
        assert_eq!(failure.method, "GET");
        assert_eq!(failure.path, "/api/geoblock");
        assert!(failure.ordered_query.is_empty());
        assert_eq!(failure.error_class, TransportErrorClass::Timeout);
    }

    #[tokio::test]
    async fn balance_timeout_retains_complete_authenticated_query() {
        let (client, state) = client().await;
        state.balance_delay_ms.store(500, Ordering::SeqCst);

        let (evidence, protocol_failure) = client
            .clob_reconciliation_raw(
                None,
                tokio::time::Instant::now() + Duration::from_millis(100),
            )
            .await;

        assert!(protocol_failure.is_none());
        assert!(matches!(
            evidence.as_slice(),
            [
                RawEvidence::HttpResponse(_),
                RawEvidence::HttpResponse(_),
                RawEvidence::HttpTransportFailure(_)
            ]
        ));
        let failure = evidence
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpTransportFailure(failure) => Some(failure),
                RawEvidence::HttpResponse(_) | RawEvidence::Artifact(_) => None,
            })
            .expect("balance transport failure was asserted above");
        assert_eq!(failure.endpoint_kind, "balance-allowance");
        assert_eq!(failure.path, "/balance-allowance");
        assert_eq!(
            failure.ordered_query,
            [
                ("asset_type".to_owned(), "COLLATERAL".to_owned()),
                ("signature_type".to_owned(), "3".to_owned()),
            ]
        );
        assert_eq!(failure.error_class, TransportErrorClass::Timeout);
    }

    #[tokio::test]
    async fn cursor_timeout_retains_the_exact_next_cursor_query() {
        let (client, state) = client().await;
        state.second_orders_delay_ms.store(500, Ordering::SeqCst);

        let (evidence, protocol_failure) = client
            .clob_reconciliation_raw(
                None,
                tokio::time::Instant::now() + Duration::from_millis(100),
            )
            .await;

        assert!(protocol_failure.is_none());
        assert_eq!(evidence.len(), 5);
        let first_page = evidence
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpResponse(response) if response.endpoint_kind == "orders-page" => {
                    Some(response)
                }
                RawEvidence::HttpResponse(_)
                | RawEvidence::HttpTransportFailure(_)
                | RawEvidence::Artifact(_) => None,
            })
            .expect("first orders page must be captured");
        assert!(first_page.ordered_query.is_empty());
        let failure = evidence
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpTransportFailure(failure) => Some(failure),
                RawEvidence::HttpResponse(_) | RawEvidence::Artifact(_) => None,
            })
            .expect("second orders page must time out");
        assert_eq!(failure.endpoint_kind, "orders-page");
        assert_eq!(failure.path, "/data/orders");
        assert_eq!(
            failure.ordered_query,
            [("next_cursor".to_owned(), "abc".to_owned())]
        );
        assert_eq!(failure.error_class, TransportErrorClass::Timeout);
    }

    #[tokio::test]
    async fn pending_reconciliation_captures_exact_order_hash_lookup() {
        let (client, state) = client().await;
        let (evidence, _) = client
            .clob_reconciliation_raw(
                Some("0xexact-order-hash"),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await;

        let exact = evidence
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpResponse(response) if response.endpoint_kind == "exact-order" => {
                    Some(response)
                }
                RawEvidence::HttpResponse(_)
                | RawEvidence::HttpTransportFailure(_)
                | RawEvidence::Artifact(_) => None,
            })
            .expect("exact order response must be captured");
        assert_eq!(exact.method, "GET");
        assert_eq!(exact.path, "/data/order/0xexact-order-hash");
        assert!(exact.ordered_query.is_empty());
        assert_eq!(exact.status, 404);
        assert_eq!(state.exact_order_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn successful_nonmatched_fok_response_is_not_accepted() {
        let raw = RawHttpResponse {
            source_id: "test".to_owned(),
            endpoint_kind: "order-post".to_owned(),
            method: "POST".to_owned(),
            path: "/order".to_owned(),
            ordered_query: Vec::new(),
            status: 200,
            headers: Vec::new(),
            body: serde_json::to_vec(&json!({
                "errorMsg": null,
                "makingAmount": "0.5",
                "takingAmount": "5",
                "orderID": "0xorder",
                "status": "LIVE",
                "success": true,
                "transactionHashes": [],
                "tradeIds": []
            }))
            .unwrap(),
            attempt_ordinal: 1,
            source_at: None,
            observed_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: SDK_VERSION.to_owned(),
        };
        let result = CanaryV2Client::parse_post_response(&raw).unwrap();
        assert!(!result.success);
        assert_eq!(result.order_id, "0xorder");
        assert!(
            result
                .error_message
                .unwrap()
                .contains("unexpected FOK status")
        );
    }

    #[test]
    fn malformed_orders_page_is_retained_before_protocol_failure() {
        let mut responses = Vec::new();
        let response = reconciliation_page("orders-page", 200, b"not-json".to_vec());
        assert!(capture_page_and_next_cursor(&mut responses, response).is_err());
        assert_eq!(responses.len(), 1);
        let response = responses
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpResponse(response) => Some(response),
                RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
            })
            .expect("response evidence was just captured");
        assert_eq!(response.endpoint_kind, "orders-page");
        assert_eq!(response.body, b"not-json");
    }

    #[test]
    fn non_success_trades_page_is_retained_before_protocol_failure() {
        let mut responses = Vec::new();
        let response = reconciliation_page("trades-page", 503, b"unavailable".to_vec());
        assert!(capture_page_and_next_cursor(&mut responses, response).is_err());
        assert_eq!(responses.len(), 1);
        let response = responses
            .iter()
            .find_map(|evidence| match evidence {
                RawEvidence::HttpResponse(response) => Some(response),
                RawEvidence::HttpTransportFailure(_) | RawEvidence::Artifact(_) => None,
            })
            .expect("response evidence was just captured");
        assert_eq!(response.endpoint_kind, "trades-page");
        assert_eq!(response.status, 503);
    }

    fn reconciliation_page(endpoint_kind: &str, status: u16, body: Vec<u8>) -> RawHttpResponse {
        RawHttpResponse {
            source_id: "test".to_owned(),
            endpoint_kind: endpoint_kind.to_owned(),
            method: "GET".to_owned(),
            path: "/data/page".to_owned(),
            ordered_query: Vec::new(),
            status,
            headers: Vec::new(),
            body,
            attempt_ordinal: 1,
            source_at: None,
            observed_at: OffsetDateTime::now_utc(),
            received_at: OffsetDateTime::now_utc(),
            schema_version: 1,
            parser_version: 1,
            adapter_version: SDK_VERSION.to_owned(),
        }
    }
}
