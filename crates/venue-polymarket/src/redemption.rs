//! V2 collateral-adapter redemption and authenticated Polymarket Relayer transport.
//!
//! Calldata construction is signing-free. The execution layer supplies a wallet-signed envelope;
//! this module binds it to the selected redemption call, authenticates the HTTP request with either
//! Relayer API credentials or Builder credentials, submits exactly once, and polls to confirmation.

use std::future::Future;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use alloy_primitives::keccak256;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE;
use hmac::{Hmac, Mac as _};
use pe_core_types::{PolymarketConditionId, RawHttpResponse};
use polymarket_client_sdk_v2::auth::{PrivateKeySigner, Signer as _};
use polymarket_client_sdk_v2::types::{Address, B256, U256};
use polymarket_client_sdk_v2::{POLYGON, contract_config};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use time::OffsetDateTime;

/// verified 2026-08-11 from https://docs.polymarket.com/resources/contracts
pub const STANDARD_COLLATERAL_ADAPTER: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
/// verified 2026-08-11 from https://docs.polymarket.com/resources/contracts
pub const NEGRISK_COLLATERAL_ADAPTER: &str = "0xadA2005600Dec949baf300f4C6120000bDB6eAab";
/// verified 2026-08-11 from https://docs.polymarket.com/resources/contracts
pub const CONDITIONAL_TOKENS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
/// verified 2026-08-11 from https://docs.polymarket.com/resources/contracts
pub const DEPOSIT_WALLET_RELAY_TARGET: &str = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07";

/// verified 2026-08-11 from https://docs.polymarket.com/trading/positions/manage
pub const REDEEM_POSITIONS_SIGNATURE: &str = "redeemPositions(address,bytes32,bytes32,uint256[])";
/// verified 2026-08-11 from https://docs.polymarket.com/trading/positions/manage
pub const REDEEM_POSITIONS_SELECTOR: [u8; 4] = [0x01, 0xb7, 0x03, 0x7c];

/// verified 2026-08-11 from https://docs.polymarket.com/api-reference/relayer/submit-a-transaction
pub const RELAYER_BASE_URL: &str = "https://relayer-v2.polymarket.com";
/// verified 2026-08-11 from https://docs.polymarket.com/api-reference/relayer/submit-a-transaction
pub const RELAYER_SUBMIT_PATH: &str = "/submit";
/// verified 2026-08-11 from https://docs.polymarket.com/trading/wallets-auth
pub const RELAYER_DEPOSIT_WALLET_NONCE_PATH: &str = "/v1/account/transactions/params";
/// verified 2026-08-11 from https://docs.polymarket.com/trading/wallets-auth
pub const RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX: &str = "/v1/account/transactions/";
/// verified 2026-08-11 from https://docs.polymarket.com/api-reference/relayer/get-relayer-address-and-nonce
pub const RELAYER_LEGACY_NONCE_PATH: &str = "/relay-payload";
/// verified 2026-08-11 from https://docs.polymarket.com/api-reference/relayer/get-a-transaction-by-id
pub const RELAYER_LEGACY_TRANSACTION_PATH: &str = "/transaction";

pub const REDEMPTION_SCHEMA_VERSION: u16 = 1;
pub const REDEMPTION_PARSER_VERSION: u16 = 1;
pub const REDEMPTION_ADAPTER_VERSION: &str = "polymarket-redemption-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedemptionCall {
    pub to: String,
    pub calldata: Vec<u8>,
    pub condition_id: PolymarketConditionId,
    pub neg_risk: bool,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RedemptionError {
    #[error("condition id must be an exact bytes32 value")]
    InvalidConditionId,
    #[error("required Polygon V2 contract configuration is unavailable")]
    ContractConfig,
    #[error("contract address is invalid")]
    InvalidAddress,
    #[error("redemption adapter approval is not verified")]
    ApprovalUnverified,
    #[error("redemption adapter is not approved for all outcome tokens")]
    ApprovalDenied,
}

#[must_use]
pub const fn redemption_adapter(neg_risk: bool) -> &'static str {
    if neg_risk {
        NEGRISK_COLLATERAL_ADAPTER
    } else {
        STANDARD_COLLATERAL_ADAPTER
    }
}

pub fn build_redemption_call(
    condition_id: PolymarketConditionId,
    neg_risk: bool,
) -> Result<RedemptionCall, RedemptionError> {
    let condition =
        B256::from_str(&condition_id.0).map_err(|_| RedemptionError::InvalidConditionId)?;
    let adapter = Address::from_str(redemption_adapter(neg_risk))
        .map_err(|_| RedemptionError::InvalidAddress)?;
    // Polygon pUSD is selected from the vendored V2 SDK config and was independently reverified
    // 2026-08-11 from https://docs.polymarket.com/resources/contracts.
    let collateral = contract_config(POLYGON, neg_risk)
        .map(|config| config.collateral)
        .ok_or(RedemptionError::ContractConfig)?;

    let mut calldata = Vec::with_capacity(4 + 7 * 32);
    calldata.extend_from_slice(&REDEEM_POSITIONS_SELECTOR);
    push_address_word(&mut calldata, &collateral);
    calldata.extend_from_slice(B256::ZERO.as_slice());
    calldata.extend_from_slice(condition.as_slice());
    push_u256_word(&mut calldata, &U256::from(128));
    push_u256_word(&mut calldata, &U256::from(2));
    push_u256_word(&mut calldata, &U256::from(1));
    push_u256_word(&mut calldata, &U256::from(2));

    Ok(RedemptionCall {
        to: adapter.to_string(),
        calldata,
        condition_id,
        neg_risk,
    })
}

fn push_u256_word(target: &mut Vec<u8>, value: &U256) {
    target.extend_from_slice(&value.to_be_bytes::<32>());
}

fn push_address_word(target: &mut Vec<u8>, value: &Address) {
    target.extend_from_slice(&[0_u8; 12]);
    target.extend_from_slice(value.as_slice());
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(2 + bytes.len() * 2);
    encoded.push_str("0x");
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustodyKind {
    DepositWallet,
    Proxy,
    Safe,
    Eoa,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayerSignatureParams {
    pub gas_price: String,
    pub operation: String,
    pub safe_txn_gas: String,
    pub base_gas: String,
    pub gas_token: String,
    pub refund_receiver: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedRedemptionRequest {
    pub call: RedemptionCall,
    pub custody: CustodyKind,
    pub signer_address: String,
    pub custody_wallet: String,
    pub nonce: String,
    pub signature: String,
    pub deadline_unix: Option<u64>,
    pub signature_params: Option<RelayerSignatureParams>,
    pub metadata: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedemptionSigningError {
    #[error("private key is invalid")]
    InvalidPrivateKey,
    #[error("custody wallet is not a valid EVM address")]
    InvalidCustodyWallet,
    #[error("redemption call target is not a valid EVM address")]
    InvalidCallTarget,
    #[error("Relayer nonce is not a valid uint256")]
    InvalidNonce,
    #[error("alloy signer failed to sign the Deposit Wallet batch digest")]
    SigningFailed,
    #[error("alloy local signer unexpectedly deferred signing")]
    SigningDeferred,
}

// verified 2026-08-11 from https://docs.polymarket.com/trading/wallets-auth
// and https://github.com/Polymarket/builder-relayer-client/blob/main/src/builder/deposit-wallet.ts
// Domain: DepositWallet/1, Polygon 137, verifyingContract = the Deposit Wallet.
const DEPOSIT_WALLET_DOMAIN_TYPE: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
const DEPOSIT_WALLET_DOMAIN_NAME: &str = "DepositWallet";
const DEPOSIT_WALLET_DOMAIN_VERSION: &str = "1";

// verified 2026-08-11 from https://docs.polymarket.com/trading/wallets-auth
// and https://github.com/Polymarket/builder-relayer-client/blob/main/src/builder/deposit-wallet.ts
// EIP-712 encodeType appends the referenced Call type to the primary Batch type.
const DEPOSIT_WALLET_CALL_TYPE: &str = "Call(address target,uint256 value,bytes data)";
const DEPOSIT_WALLET_BATCH_TYPE: &str = concat!(
    "Batch(address wallet,uint256 nonce,uint256 deadline,Call[] calls)",
    "Call(address target,uint256 value,bytes data)"
);

// verified 2026-08-11 from the official client's signTypedData call at
// https://github.com/Polymarket/builder-relayer-client/blob/main/src/builder/deposit-wallet.ts;
// digest construction is EIP-712: https://eips.ethereum.org/EIPS/eip-712.
fn deposit_wallet_redemption_digest(
    call: &RedemptionCall,
    custody_wallet: &str,
    nonce: &str,
    deadline_unix: u64,
) -> Result<B256, RedemptionSigningError> {
    let wallet = Address::from_str(custody_wallet)
        .map_err(|_| RedemptionSigningError::InvalidCustodyWallet)?;
    let target =
        Address::from_str(&call.to).map_err(|_| RedemptionSigningError::InvalidCallTarget)?;
    let nonce = U256::from_str(nonce).map_err(|_| RedemptionSigningError::InvalidNonce)?;

    let mut domain = Vec::with_capacity(5 * 32);
    domain.extend_from_slice(keccak256(DEPOSIT_WALLET_DOMAIN_TYPE).as_slice());
    domain.extend_from_slice(keccak256(DEPOSIT_WALLET_DOMAIN_NAME).as_slice());
    domain.extend_from_slice(keccak256(DEPOSIT_WALLET_DOMAIN_VERSION).as_slice());
    push_u256_word(&mut domain, &U256::from(POLYGON));
    push_address_word(&mut domain, &wallet);
    let domain_separator = keccak256(domain);

    let mut encoded_call = Vec::with_capacity(4 * 32);
    encoded_call.extend_from_slice(keccak256(DEPOSIT_WALLET_CALL_TYPE).as_slice());
    push_address_word(&mut encoded_call, &target);
    push_u256_word(&mut encoded_call, &U256::ZERO);
    encoded_call.extend_from_slice(keccak256(&call.calldata).as_slice());
    let call_hash = keccak256(encoded_call);
    let calls_hash = keccak256(call_hash.as_slice());

    let mut batch = Vec::with_capacity(5 * 32);
    batch.extend_from_slice(keccak256(DEPOSIT_WALLET_BATCH_TYPE).as_slice());
    push_address_word(&mut batch, &wallet);
    push_u256_word(&mut batch, &nonce);
    push_u256_word(&mut batch, &U256::from(deadline_unix));
    batch.extend_from_slice(calls_hash.as_slice());
    let batch_hash = keccak256(batch);

    let mut digest = Vec::with_capacity(66);
    digest.extend_from_slice(&[0x19, 0x01]);
    digest.extend_from_slice(domain_separator.as_slice());
    digest.extend_from_slice(batch_hash.as_slice());
    Ok(keccak256(digest))
}

struct LocalSignerWake;

impl Wake for LocalSignerWake {
    fn wake(self: Arc<Self>) {}
}

/// Sign one official Relayer `WALLET` Deposit Wallet batch containing the supplied redemption.
///
/// Safe and Proxy custody use different wallet-specific payloads and are deliberately not covered
/// by this constructor.
pub fn sign_deposit_wallet_redemption(
    private_key: &str,
    call: &RedemptionCall,
    custody_wallet: &str,
    nonce: &str,
    deadline_unix: u64,
) -> Result<SignedRedemptionRequest, RedemptionSigningError> {
    let signer = PrivateKeySigner::from_str(private_key)
        .map_err(|_| RedemptionSigningError::InvalidPrivateKey)?;
    let digest = deposit_wallet_redemption_digest(call, custody_wallet, nonce, deadline_unix)?;

    // PrivateKeySigner is a synchronous local alloy signer behind an async trait. Polling its
    // sign_hash future once avoids nesting a Tokio runtime inside this intentionally sync API.
    let waker = Waker::from(Arc::new(LocalSignerWake));
    let mut context = Context::from_waker(&waker);
    let mut signature = signer.sign_hash(&digest);
    let signature = match signature.as_mut().poll(&mut context) {
        Poll::Ready(Ok(signature)) => signature,
        Poll::Ready(Err(_)) => return Err(RedemptionSigningError::SigningFailed),
        Poll::Pending => return Err(RedemptionSigningError::SigningDeferred),
    };

    Ok(SignedRedemptionRequest {
        call: call.clone(),
        custody: CustodyKind::DepositWallet,
        signer_address: signer.address().to_string(),
        custody_wallet: custody_wallet.to_owned(),
        nonce: nonce.to_owned(),
        signature: signature.to_string(),
        deadline_unix: Some(deadline_unix),
        signature_params: None,
        metadata: "Redeem positions".to_owned(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("custody kind {custody:?} is unsupported by redemption transport v1")]
pub struct UnsupportedCustody {
    pub custody: CustodyKind,
}

#[derive(Clone)]
pub struct RelayerApiKeyCredentials {
    pub api_key: String,
    pub address: String,
}

#[derive(Clone)]
pub struct BuilderApiKeyCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

#[derive(Clone)]
pub enum RelayerCredentials {
    RelayerApiKey(RelayerApiKeyCredentials),
    BuilderApiKey(BuilderApiKeyCredentials),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayerPollPolicy {
    pub request_timeout: Duration,
    pub poll_interval: Duration,
    pub maximum_polls: u32,
}

pub struct RelayerHttpRequest {
    pub method: String,
    pub path: String,
    pub ordered_query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub body_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayerNonce {
    pub custody: CustodyKind,
    pub address: String,
    pub nonce: String,
    pub evidence: RawHttpResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayerState {
    New,
    Executed,
    Mined,
    Confirmed,
    Invalid,
    Failed,
}

impl RelayerState {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "STATE_NEW" => Some(Self::New),
            "STATE_EXECUTED" => Some(Self::Executed),
            "STATE_MINED" => Some(Self::Mined),
            "STATE_CONFIRMED" => Some(Self::Confirmed),
            "STATE_INVALID" => Some(Self::Invalid),
            "STATE_FAILED" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedRedemption {
    pub transaction_id: String,
    pub transaction_hash: String,
    pub submit_body_hash: String,
    pub evidence: Vec<RawHttpResponse>,
}

#[derive(Debug, thiserror::Error)]
pub enum RedemptionTransportError {
    #[error("redemption transport configuration is invalid: {0}")]
    Configuration(String),
    #[error(transparent)]
    UnsupportedCustody(#[from] UnsupportedCustody),
    #[error("redemption request is invalid: {0}")]
    Request(String),
    #[error("Relayer authentication failed during {phase} with HTTP {status}")]
    Authentication {
        phase: &'static str,
        status: u16,
        response_body_hash: String,
    },
    #[error("Relayer rejected {phase} with HTTP {status}")]
    Rejected {
        phase: &'static str,
        status: u16,
        response_body_hash: String,
    },
    #[error("Relayer transport failed before submission during {phase}: {message}")]
    Transport {
        phase: &'static str,
        message: String,
    },
    #[error("redemption is ambiguous after submission: {reason}")]
    AmbiguousAfterSubmit {
        transaction_id: Option<String>,
        submit_body_hash: String,
        evidence: Vec<RawHttpResponse>,
        reason: String,
    },
    #[error("Relayer transaction {transaction_id} terminated as {state:?}")]
    TerminalFailure {
        transaction_id: String,
        state: RelayerState,
        error_message: Option<String>,
        evidence: Vec<RawHttpResponse>,
    },
    #[error("Relayer protocol failed during {phase}: {message}")]
    Protocol {
        phase: &'static str,
        message: String,
        evidence: Vec<RawHttpResponse>,
    },
}

pub trait RedemptionTransport: Send + Sync {
    fn fetch_nonce<'a>(
        &'a self,
        signer_address: &'a str,
        custody: CustodyKind,
    ) -> Pin<Box<dyn Future<Output = Result<RelayerNonce, RedemptionTransportError>> + Send + 'a>>;

    fn submit_and_confirm<'a>(
        &'a self,
        request: &'a SignedRedemptionRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<ConfirmedRedemption, RedemptionTransportError>> + Send + 'a>,
    >;
}

pub struct RelayerTransportClient {
    http: reqwest::Client,
    base_url: String,
    credentials: RelayerCredentials,
    policy: RelayerPollPolicy,
}

impl RelayerTransportClient {
    pub fn new(
        base_url: impl Into<String>,
        credentials: RelayerCredentials,
        policy: RelayerPollPolicy,
    ) -> Result<Self, RedemptionTransportError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        if base_url.is_empty() || policy.request_timeout.is_zero() || policy.maximum_polls == 0 {
            return Err(RedemptionTransportError::Configuration(
                "base URL, request timeout, and maximum polls must be nonzero".to_owned(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(policy.request_timeout)
            .build()
            .map_err(|error| RedemptionTransportError::Configuration(error.to_string()))?;
        Ok(Self {
            http,
            base_url,
            credentials,
            policy,
        })
    }

    pub fn build_submission_at(
        &self,
        request: &SignedRedemptionRequest,
        request_timestamp_unix: i64,
    ) -> Result<RelayerHttpRequest, RedemptionTransportError> {
        validate_signed_request(request)?;
        let body = match request.custody {
            CustodyKind::DepositWallet => build_deposit_wallet_body(request)?,
            CustodyKind::Proxy | CustodyKind::Safe => build_legacy_wallet_body(request)?,
            CustodyKind::Eoa => {
                return Err(UnsupportedCustody {
                    custody: CustodyKind::Eoa,
                }
                .into());
            }
        };
        self.build_http_request(
            "POST",
            RELAYER_SUBMIT_PATH,
            Vec::new(),
            body,
            request_timestamp_unix,
            &request.signer_address,
        )
    }

    pub fn build_nonce_request_at(
        &self,
        signer_address: &str,
        custody: CustodyKind,
        request_timestamp_unix: i64,
    ) -> Result<RelayerHttpRequest, RedemptionTransportError> {
        validate_address(signer_address)?;
        let (path, nonce_type) = match custody {
            CustodyKind::DepositWallet => (RELAYER_DEPOSIT_WALLET_NONCE_PATH, "WALLET"),
            CustodyKind::Proxy => (RELAYER_LEGACY_NONCE_PATH, "PROXY"),
            CustodyKind::Safe => (RELAYER_LEGACY_NONCE_PATH, "SAFE"),
            CustodyKind::Eoa => return Err(UnsupportedCustody { custody }.into()),
        };
        self.build_http_request(
            "GET",
            path,
            vec![
                ("address".to_owned(), signer_address.to_owned()),
                ("type".to_owned(), nonce_type.to_owned()),
            ],
            Vec::new(),
            request_timestamp_unix,
            signer_address,
        )
    }

    fn build_poll_request_at(
        &self,
        transaction_id: &str,
        signer_address: &str,
        custody: CustodyKind,
        request_timestamp_unix: i64,
    ) -> Result<RelayerHttpRequest, RedemptionTransportError> {
        if transaction_id.is_empty()
            || !transaction_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(RedemptionTransportError::Request(
                "transaction id is not a safe path segment".to_owned(),
            ));
        }
        let (path, ordered_query) = match custody {
            CustodyKind::DepositWallet => (
                format!("{RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX}{transaction_id}"),
                Vec::new(),
            ),
            CustodyKind::Proxy | CustodyKind::Safe => (
                RELAYER_LEGACY_TRANSACTION_PATH.to_owned(),
                vec![("id".to_owned(), transaction_id.to_owned())],
            ),
            CustodyKind::Eoa => return Err(UnsupportedCustody { custody }.into()),
        };
        self.build_http_request(
            "GET",
            &path,
            ordered_query,
            Vec::new(),
            request_timestamp_unix,
            signer_address,
        )
    }

    fn build_http_request(
        &self,
        method: &str,
        path: &str,
        ordered_query: Vec<(String, String)>,
        body: Vec<u8>,
        timestamp: i64,
        signer_address: &str,
    ) -> Result<RelayerHttpRequest, RedemptionTransportError> {
        let headers = authentication_headers(
            &self.credentials,
            method,
            path,
            &body,
            timestamp,
            signer_address,
        )?;
        Ok(RelayerHttpRequest {
            method: method.to_owned(),
            path: path.to_owned(),
            ordered_query,
            headers,
            body_hash: blake3::hash(&body).to_hex().to_string(),
            body,
        })
    }

    async fn fetch_nonce_impl(
        &self,
        signer_address: &str,
        custody: CustodyKind,
    ) -> Result<RelayerNonce, RedemptionTransportError> {
        let request = self.build_nonce_request_at(
            signer_address,
            custody,
            OffsetDateTime::now_utc().unix_timestamp(),
        )?;
        let response =
            self.execute(request)
                .await
                .map_err(|message| RedemptionTransportError::Transport {
                    phase: "nonce",
                    message,
                })?;
        ensure_pre_submit_status("nonce", &response)?;
        #[derive(Deserialize)]
        struct NonceResponse {
            address: String,
            nonce: String,
        }
        let parsed: NonceResponse = serde_json::from_slice(&response.body).map_err(|error| {
            RedemptionTransportError::Protocol {
                phase: "nonce",
                message: error.to_string(),
                evidence: vec![response.clone()],
            }
        })?;
        if parsed.nonce.is_empty()
            || Address::from_str(&parsed.address).is_err()
            || (custody == CustodyKind::DepositWallet
                && !parsed.address.eq_ignore_ascii_case(signer_address))
        {
            return Err(RedemptionTransportError::Protocol {
                phase: "nonce",
                message: "nonce identity or value did not match the request".to_owned(),
                evidence: vec![response],
            });
        }
        Ok(RelayerNonce {
            custody,
            address: parsed.address,
            nonce: parsed.nonce,
            evidence: response,
        })
    }

    async fn submit_and_confirm_impl(
        &self,
        request: &SignedRedemptionRequest,
    ) -> Result<ConfirmedRedemption, RedemptionTransportError> {
        let submission =
            self.build_submission_at(request, OffsetDateTime::now_utc().unix_timestamp())?;
        let submit_body_hash = submission.body_hash.clone();
        let submit_response = match self.execute(submission).await {
            Ok(response) => response,
            Err(reason) => {
                return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                    transaction_id: None,
                    submit_body_hash,
                    evidence: Vec::new(),
                    reason,
                });
            }
        };
        if submit_response.status == 401 || submit_response.status == 403 {
            return Err(RedemptionTransportError::Authentication {
                phase: "submit",
                status: submit_response.status,
                response_body_hash: blake3::hash(&submit_response.body).to_hex().to_string(),
            });
        }
        if !(200..300).contains(&submit_response.status) {
            if submit_response.status < 500 {
                return Err(RedemptionTransportError::Rejected {
                    phase: "submit",
                    status: submit_response.status,
                    response_body_hash: blake3::hash(&submit_response.body).to_hex().to_string(),
                });
            }
            return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                transaction_id: None,
                submit_body_hash,
                evidence: vec![submit_response],
                reason: "Relayer returned a server error after receiving the POST".to_owned(),
            });
        }
        let submit: SubmitResponse = match serde_json::from_slice(&submit_response.body) {
            Ok(response) => response,
            Err(error) => {
                return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                    transaction_id: None,
                    submit_body_hash,
                    evidence: vec![submit_response],
                    reason: format!("submit response decode failed: {error}"),
                });
            }
        };
        if submit.transaction_id.is_empty() {
            return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                transaction_id: None,
                submit_body_hash,
                evidence: vec![submit_response],
                reason: "submit response omitted transactionID".to_owned(),
            });
        }
        let submit_state = RelayerState::parse(&submit.state).ok_or_else(|| {
            RedemptionTransportError::AmbiguousAfterSubmit {
                transaction_id: Some(submit.transaction_id.clone()),
                submit_body_hash: submit_body_hash.clone(),
                evidence: vec![submit_response.clone()],
                reason: format!("unknown submit state {}", submit.state),
            }
        })?;
        let mut evidence = vec![submit_response];
        if matches!(submit_state, RelayerState::Invalid | RelayerState::Failed) {
            return Err(RedemptionTransportError::TerminalFailure {
                transaction_id: submit.transaction_id,
                state: submit_state,
                error_message: None,
                evidence,
            });
        }

        for poll_ordinal in 0..self.policy.maximum_polls {
            let poll_request = match self.build_poll_request_at(
                &submit.transaction_id,
                &request.signer_address,
                request.custody,
                OffsetDateTime::now_utc().unix_timestamp(),
            ) {
                Ok(poll_request) => poll_request,
                Err(error) => {
                    return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                        transaction_id: Some(submit.transaction_id),
                        submit_body_hash,
                        evidence,
                        reason: error.to_string(),
                    });
                }
            };
            let poll_response = match self.execute(poll_request).await {
                Ok(response) => response,
                Err(reason) => {
                    return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                        transaction_id: Some(submit.transaction_id),
                        submit_body_hash,
                        evidence,
                        reason,
                    });
                }
            };
            if !(200..300).contains(&poll_response.status) {
                let status = poll_response.status;
                evidence.push(poll_response);
                return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                    transaction_id: Some(submit.transaction_id),
                    submit_body_hash,
                    evidence,
                    reason: format!("status poll returned HTTP {status}"),
                });
            }
            let poll = match parse_poll_response(&poll_response.body) {
                Ok(poll) => poll,
                Err(reason) => {
                    evidence.push(poll_response);
                    return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                        transaction_id: Some(submit.transaction_id),
                        submit_body_hash,
                        evidence,
                        reason,
                    });
                }
            };
            evidence.push(poll_response);
            if poll.transaction_id != submit.transaction_id {
                return Err(RedemptionTransportError::AmbiguousAfterSubmit {
                    transaction_id: Some(submit.transaction_id),
                    submit_body_hash,
                    evidence,
                    reason: "poll response transaction identity changed".to_owned(),
                });
            }
            let state = RelayerState::parse(&poll.state).ok_or_else(|| {
                RedemptionTransportError::AmbiguousAfterSubmit {
                    transaction_id: Some(submit.transaction_id.clone()),
                    submit_body_hash: submit_body_hash.clone(),
                    evidence: evidence.clone(),
                    reason: format!("unknown poll state {}", poll.state),
                }
            })?;
            match state {
                RelayerState::Confirmed => {
                    let transaction_hash = poll
                        .transaction_hash
                        .filter(|hash| !hash.is_empty())
                        .ok_or_else(|| RedemptionTransportError::AmbiguousAfterSubmit {
                            transaction_id: Some(submit.transaction_id.clone()),
                            submit_body_hash: submit_body_hash.clone(),
                            evidence: evidence.clone(),
                            reason: "confirmed response omitted transaction hash".to_owned(),
                        })?;
                    return Ok(ConfirmedRedemption {
                        transaction_id: submit.transaction_id,
                        transaction_hash,
                        submit_body_hash,
                        evidence,
                    });
                }
                RelayerState::Invalid | RelayerState::Failed => {
                    return Err(RedemptionTransportError::TerminalFailure {
                        transaction_id: submit.transaction_id,
                        state,
                        error_message: poll.error_message,
                        evidence,
                    });
                }
                RelayerState::New | RelayerState::Executed | RelayerState::Mined => {}
            }
            if poll_ordinal + 1 < self.policy.maximum_polls {
                tokio::time::sleep(self.policy.poll_interval).await;
            }
        }
        Err(RedemptionTransportError::AmbiguousAfterSubmit {
            transaction_id: Some(submit.transaction_id),
            submit_body_hash,
            evidence,
            reason: "confirmation poll budget exhausted".to_owned(),
        })
    }

    async fn execute(&self, request: RelayerHttpRequest) -> Result<RawHttpResponse, String> {
        let method =
            Method::from_bytes(request.method.as_bytes()).map_err(|error| error.to_string())?;
        let url = format!("{}{}", self.base_url, request.path);
        let mut builder = self.http.request(method, url);
        if !request.ordered_query.is_empty() {
            builder = builder.query(&request.ordered_query);
        }
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        if !request.body.is_empty() {
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(request.body);
        }
        let observed_at = OffsetDateTime::now_utc();
        let response = builder.send().await.map_err(|error| error.to_string())?;
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
        let source_at = source_at(&headers);
        let body = response
            .bytes()
            .await
            .map_err(|error| error.to_string())?
            .to_vec();
        Ok(RawHttpResponse {
            source_id: "polymarket-relayer-v2".to_owned(),
            endpoint_kind: if request.path == RELAYER_SUBMIT_PATH {
                "redemption-submit".to_owned()
            } else if request.path == RELAYER_DEPOSIT_WALLET_NONCE_PATH
                || request.path == RELAYER_LEGACY_NONCE_PATH
            {
                "redemption-nonce".to_owned()
            } else {
                "redemption-status".to_owned()
            },
            method: request.method,
            path: request.path,
            ordered_query: request.ordered_query,
            status,
            headers,
            body,
            attempt_ordinal: 1,
            source_at,
            observed_at,
            received_at: OffsetDateTime::now_utc(),
            schema_version: REDEMPTION_SCHEMA_VERSION,
            parser_version: REDEMPTION_PARSER_VERSION,
            adapter_version: REDEMPTION_ADAPTER_VERSION.to_owned(),
        })
    }
}

impl RedemptionTransport for RelayerTransportClient {
    fn fetch_nonce<'a>(
        &'a self,
        signer_address: &'a str,
        custody: CustodyKind,
    ) -> Pin<Box<dyn Future<Output = Result<RelayerNonce, RedemptionTransportError>> + Send + 'a>>
    {
        Box::pin(async move { self.fetch_nonce_impl(signer_address, custody).await })
    }

    fn submit_and_confirm<'a>(
        &'a self,
        request: &'a SignedRedemptionRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<ConfirmedRedemption, RedemptionTransportError>> + Send + 'a>,
    > {
        Box::pin(async move { self.submit_and_confirm_impl(request).await })
    }
}

fn authentication_headers(
    credentials: &RelayerCredentials,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: i64,
    signer_address: &str,
) -> Result<Vec<(String, String)>, RedemptionTransportError> {
    match credentials {
        RelayerCredentials::RelayerApiKey(credentials) => {
            validate_address(&credentials.address)?;
            if !credentials.address.eq_ignore_ascii_case(signer_address)
                || credentials.api_key.is_empty()
            {
                return Err(RedemptionTransportError::Request(
                    "Relayer API key address must match the signer".to_owned(),
                ));
            }
            Ok(vec![
                ("RELAYER_API_KEY".to_owned(), credentials.api_key.clone()),
                (
                    "RELAYER_API_KEY_ADDRESS".to_owned(),
                    credentials.address.clone(),
                ),
            ])
        }
        RelayerCredentials::BuilderApiKey(credentials) => {
            if credentials.api_key.is_empty()
                || credentials.api_secret.is_empty()
                || credentials.api_passphrase.is_empty()
            {
                return Err(RedemptionTransportError::Request(
                    "Builder key, secret, and passphrase are all required".to_owned(),
                ));
            }
            let decoded_secret = URL_SAFE
                .decode(&credentials.api_secret)
                .map_err(|error| RedemptionTransportError::Request(error.to_string()))?;
            let body = std::str::from_utf8(body)
                .map_err(|error| RedemptionTransportError::Request(error.to_string()))?;
            let message = format!("{timestamp}{method}{path}{body}");
            let mut mac = Hmac::<Sha256>::new_from_slice(&decoded_secret)
                .map_err(|error| RedemptionTransportError::Request(error.to_string()))?;
            mac.update(message.as_bytes());
            let signature = URL_SAFE.encode(mac.finalize().into_bytes());
            Ok(vec![
                (
                    "POLY_BUILDER_API_KEY".to_owned(),
                    credentials.api_key.clone(),
                ),
                ("POLY_BUILDER_TIMESTAMP".to_owned(), timestamp.to_string()),
                (
                    "POLY_BUILDER_PASSPHRASE".to_owned(),
                    credentials.api_passphrase.clone(),
                ),
                ("POLY_BUILDER_SIGNATURE".to_owned(), signature),
            ])
        }
    }
}

fn validate_signed_request(
    request: &SignedRedemptionRequest,
) -> Result<(), RedemptionTransportError> {
    validate_address(&request.signer_address)?;
    validate_address(&request.custody_wallet)?;
    validate_address(&request.call.to)?;
    if request.nonce.is_empty()
        || request.signature.is_empty()
        || !request.signature.starts_with("0x")
        || request.call.calldata.is_empty()
    {
        return Err(RedemptionTransportError::Request(
            "nonce, wallet signature, and calldata are required".to_owned(),
        ));
    }
    match request.custody {
        CustodyKind::DepositWallet if request.deadline_unix.is_none() => {
            Err(RedemptionTransportError::Request(
                "Deposit Wallet requests require a signed deadline".to_owned(),
            ))
        }
        CustodyKind::Proxy | CustodyKind::Safe if request.signature_params.is_none() => {
            Err(RedemptionTransportError::Request(
                "Proxy/Safe requests require wallet signature parameters".to_owned(),
            ))
        }
        CustodyKind::Eoa => Err(UnsupportedCustody {
            custody: CustodyKind::Eoa,
        }
        .into()),
        CustodyKind::DepositWallet | CustodyKind::Proxy | CustodyKind::Safe => Ok(()),
    }
}

fn validate_address(address: &str) -> Result<(), RedemptionTransportError> {
    Address::from_str(address)
        .map(|_| ())
        .map_err(|_| RedemptionTransportError::Request("invalid EVM address".to_owned()))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WalletCall<'a> {
    target: &'a str,
    value: &'static str,
    data: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DepositWalletParams<'a> {
    deposit_wallet: &'a str,
    deadline: String,
    calls: [WalletCall<'a>; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DepositWalletBody<'a> {
    #[serde(rename = "type")]
    transaction_type: &'static str,
    from: &'a str,
    to: &'static str,
    nonce: &'a str,
    signature: &'a str,
    metadata: &'a str,
    deposit_wallet_params: DepositWalletParams<'a>,
}

fn build_deposit_wallet_body(
    request: &SignedRedemptionRequest,
) -> Result<Vec<u8>, RedemptionTransportError> {
    let deadline = request.deadline_unix.ok_or_else(|| {
        RedemptionTransportError::Request(
            "Deposit Wallet requests require a signed deadline".to_owned(),
        )
    })?;
    serde_json::to_vec(&DepositWalletBody {
        transaction_type: "WALLET",
        from: &request.signer_address,
        to: DEPOSIT_WALLET_RELAY_TARGET,
        nonce: &request.nonce,
        signature: &request.signature,
        metadata: &request.metadata,
        deposit_wallet_params: DepositWalletParams {
            deposit_wallet: &request.custody_wallet,
            deadline: deadline.to_string(),
            calls: [WalletCall {
                target: &request.call.to,
                value: "0",
                data: hex(&request.call.calldata),
            }],
        },
    })
    .map_err(|error| RedemptionTransportError::Request(error.to_string()))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyWalletBody<'a> {
    from: &'a str,
    to: &'a str,
    proxy_wallet: &'a str,
    data: String,
    nonce: &'a str,
    signature: &'a str,
    signature_params: &'a RelayerSignatureParams,
    #[serde(rename = "type")]
    transaction_type: &'static str,
}

fn build_legacy_wallet_body(
    request: &SignedRedemptionRequest,
) -> Result<Vec<u8>, RedemptionTransportError> {
    let signature_params = request.signature_params.as_ref().ok_or_else(|| {
        RedemptionTransportError::Request(
            "Proxy/Safe requests require wallet signature parameters".to_owned(),
        )
    })?;
    let transaction_type = match request.custody {
        CustodyKind::Proxy => "PROXY",
        CustodyKind::Safe => "SAFE",
        CustodyKind::DepositWallet | CustodyKind::Eoa => {
            return Err(RedemptionTransportError::Request(
                "legacy wallet body requires Proxy or Safe custody".to_owned(),
            ));
        }
    };
    serde_json::to_vec(&LegacyWalletBody {
        from: &request.signer_address,
        to: &request.call.to,
        proxy_wallet: &request.custody_wallet,
        data: hex(&request.call.calldata),
        nonce: &request.nonce,
        signature: &request.signature,
        signature_params,
        transaction_type,
    })
    .map_err(|error| RedemptionTransportError::Request(error.to_string()))
}

#[derive(Deserialize)]
struct SubmitResponse {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    state: String,
}

#[derive(Deserialize)]
struct PollResponse {
    #[serde(alias = "transactionID")]
    transaction_id: String,
    #[serde(default, alias = "transactionHash")]
    transaction_hash: Option<String>,
    state: String,
    #[serde(default, alias = "errorMsg")]
    error_message: Option<String>,
}

fn parse_poll_response(body: &[u8]) -> Result<PollResponse, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    let value = match value {
        serde_json::Value::Array(mut values) if values.len() == 1 => values.remove(0),
        serde_json::Value::Array(_) => {
            return Err("status response must identify exactly one transaction".to_owned());
        }
        value => value,
    };
    serde_json::from_value(value).map_err(|error| error.to_string())
}

fn ensure_pre_submit_status(
    phase: &'static str,
    response: &RawHttpResponse,
) -> Result<(), RedemptionTransportError> {
    let response_body_hash = blake3::hash(&response.body).to_hex().to_string();
    if response.status == 401 || response.status == 403 {
        return Err(RedemptionTransportError::Authentication {
            phase,
            status: response.status,
            response_body_hash,
        });
    }
    if !(200..300).contains(&response.status) {
        return Err(RedemptionTransportError::Rejected {
            phase,
            status: response.status,
            response_body_hash,
        });
    }
    Ok(())
}

fn source_at(headers: &[(String, String)]) -> Option<OffsetDateTime> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("date"))
        .and_then(|(_, value)| {
            OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822).ok()
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalCheckRequest {
    pub conditional_tokens: String,
    pub custody_wallet: String,
    pub adapter: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalEvidence {
    pub request: ApprovalCheckRequest,
    pub approved: bool,
    pub observed_at_unix: i64,
    pub raw_evidence_hash: blake3::Hash,
    pub verifier: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ApprovalReadError {
    #[error("no verified Polygon read source is configured")]
    Unavailable,
    #[error("approval read failed: {0}")]
    Read(String),
}

pub trait ApprovalReader: Send + Sync {
    fn is_approved_for_all<'a>(
        &'a self,
        request: &'a ApprovalCheckRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ApprovalEvidence, ApprovalReadError>> + Send + 'a>>;
}

/// Fail-closed reader for deployments without a configured Polygon RPC or independently verified
/// operator artifact. The Relayer API does not expose an `isApprovedForAll` read endpoint.
pub struct UnverifiedApprovalReader;

impl ApprovalReader for UnverifiedApprovalReader {
    fn is_approved_for_all<'a>(
        &'a self,
        _request: &'a ApprovalCheckRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ApprovalEvidence, ApprovalReadError>> + Send + 'a>>
    {
        Box::pin(async { Err(ApprovalReadError::Unavailable) })
    }
}

pub fn redemption_approval_requests(
    custody_wallet: &str,
) -> Result<[ApprovalCheckRequest; 2], RedemptionTransportError> {
    validate_address(custody_wallet)?;
    Ok([
        ApprovalCheckRequest {
            conditional_tokens: CONDITIONAL_TOKENS.to_owned(),
            custody_wallet: custody_wallet.to_owned(),
            adapter: STANDARD_COLLATERAL_ADAPTER.to_owned(),
        },
        ApprovalCheckRequest {
            conditional_tokens: CONDITIONAL_TOKENS.to_owned(),
            custody_wallet: custody_wallet.to_owned(),
            adapter: NEGRISK_COLLATERAL_ADAPTER.to_owned(),
        },
    ])
}

pub fn require_verified_approval(
    evidence: Option<&ApprovalEvidence>,
) -> Result<(), RedemptionError> {
    let evidence = evidence.ok_or(RedemptionError::ApprovalUnverified)?;
    if !evidence.approved {
        return Err(RedemptionError::ApprovalDenied);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use serde_json::json;

    use super::*;

    const SIGNER: &str = "0x1111111111111111111111111111111111111111";
    const WALLET: &str = "0x2222222222222222222222222222222222222222";
    const RELAYER: &str = "0x3333333333333333333333333333333333333333";
    const CONDITION: &str = "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    fn call(neg_risk: bool) -> RedemptionCall {
        build_redemption_call(PolymarketConditionId(CONDITION.to_owned()), neg_risk).unwrap()
    }

    fn signed(custody: CustodyKind) -> SignedRedemptionRequest {
        SignedRedemptionRequest {
            call: call(false),
            custody,
            signer_address: SIGNER.to_owned(),
            custody_wallet: WALLET.to_owned(),
            nonce: "7".to_owned(),
            signature: "0x1234".to_owned(),
            deadline_unix: Some(2_000_000_000),
            signature_params: Some(RelayerSignatureParams {
                gas_price: "0".to_owned(),
                operation: "0".to_owned(),
                safe_txn_gas: "0".to_owned(),
                base_gas: "0".to_owned(),
                gas_token: "0x0000000000000000000000000000000000000000".to_owned(),
                refund_receiver: "0x0000000000000000000000000000000000000000".to_owned(),
            }),
            metadata: "Redeem positions".to_owned(),
        }
    }

    fn credentials(key: &str) -> RelayerCredentials {
        RelayerCredentials::RelayerApiKey(RelayerApiKeyCredentials {
            api_key: key.to_owned(),
            address: SIGNER.to_owned(),
        })
    }

    fn policy(request_timeout: Duration) -> RelayerPollPolicy {
        RelayerPollPolicy {
            request_timeout,
            poll_interval: Duration::ZERO,
            maximum_polls: 3,
        }
    }

    #[test]
    fn redemption_calldata_matches_golden_bytes() {
        let call = call(false);
        assert_eq!(
            hex(&call.calldata),
            concat!(
                "0x01b7037c",
                "000000000000000000000000c011a7e12a19f7b1f670d46f03b03f3342e82dfb",
                "0000000000000000000000000000000000000000000000000000000000000000",
                "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                "0000000000000000000000000000000000000000000000000000000000000080",
                "0000000000000000000000000000000000000000000000000000000000000002",
                "0000000000000000000000000000000000000000000000000000000000000001",
                "0000000000000000000000000000000000000000000000000000000000000002"
            )
        );
        assert_eq!(
            &keccak256(REDEEM_POSITIONS_SIGNATURE.as_bytes()).as_slice()[..4],
            REDEEM_POSITIONS_SELECTOR
        );
    }

    #[test]
    fn adapter_selection_follows_negrisk_evidence() {
        assert_eq!(
            call(false).to,
            Address::from_str(STANDARD_COLLATERAL_ADAPTER)
                .unwrap()
                .to_string()
        );
        assert_eq!(
            call(true).to,
            Address::from_str(NEGRISK_COLLATERAL_ADAPTER)
                .unwrap()
                .to_string()
        );
    }

    #[test]
    fn deposit_wallet_redemption_signature_matches_official_eip712_golden() {
        // Public Hardhat key also used by the official builder-relayer-client signature fixtures.
        const PRIVATE_KEY: &str =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let call = call(false);
        let digest = deposit_wallet_redemption_digest(&call, WALLET, "7", 2_000_000_000).unwrap();
        assert_eq!(
            digest.to_string(),
            "0xc83b365dc194257ce9fd3df833d4981a6e2213820cbeba0b58aaeab51b1c584d"
        );

        let request =
            sign_deposit_wallet_redemption(PRIVATE_KEY, &call, WALLET, "7", 2_000_000_000).unwrap();
        assert_eq!(
            request.signature,
            concat!(
                "0xf8d5727615cccfd2f2de2d3bd4f18438011c4394df8a4a39e16bb006e2e803c3",
                "519a72060bfdc3104d68e89f9a1a17ab797ca26c734fdae325ac06bdebbe9abc1b"
            )
        );
        assert_eq!(
            request.signer_address,
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        );
        assert_eq!(request.custody, CustodyKind::DepositWallet);
        assert_eq!(request.signature_params, None);
    }

    #[test]
    fn signed_deposit_wallet_request_serialization_matches_official_shape() {
        const PRIVATE_KEY: &str =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let client = RelayerTransportClient::new(
            RELAYER_BASE_URL,
            RelayerCredentials::RelayerApiKey(RelayerApiKeyCredentials {
                api_key: "good-key".to_owned(),
                address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_owned(),
            }),
            policy(Duration::from_secs(1)),
        )
        .unwrap();
        let signed =
            sign_deposit_wallet_redemption(PRIVATE_KEY, &call(false), WALLET, "7", 2_000_000_000)
                .unwrap();
        let submission = client.build_submission_at(&signed, 1_000).unwrap();
        assert_eq!(
            String::from_utf8(submission.body).unwrap(),
            concat!(
                r#"{"type":"WALLET","from":"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266","to":"0x00000000000Fb5C9ADea0298D729A0CB3823Cc07","nonce":"7","signature":"0xf8d5727615cccfd2f2de2d3bd4f18438011c4394df8a4a39e16bb006e2e803c3519a72060bfdc3104d68e89f9a1a17ab797ca26c734fdae325ac06bdebbe9abc1b","metadata":"Redeem positions","depositWalletParams":{"depositWallet":"0x2222222222222222222222222222222222222222","deadline":"2000000000","calls":[{"target":"0xAdA100Db00Ca00073811820692005400218FcE1f","value":"0","data":"0x01b7037c"#,
                "000000000000000000000000c011a7e12a19f7b1f670d46f03b03f3342e82dfb",
                "0000000000000000000000000000000000000000000000000000000000000000",
                "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                "0000000000000000000000000000000000000000000000000000000000000080",
                "0000000000000000000000000000000000000000000000000000000000000002",
                "0000000000000000000000000000000000000000000000000000000000000001",
                "0000000000000000000000000000000000000000000000000000000000000002",
                r#""}]}}"#
            )
        );
    }

    #[derive(Clone, Default)]
    struct FixtureState {
        submit_calls: Arc<AtomicUsize>,
        poll_calls: Arc<AtomicUsize>,
        auth_ok: Arc<AtomicBool>,
        poll_delay_ms: Arc<AtomicU64>,
        body: Arc<Mutex<Vec<u8>>>,
    }

    async fn submit(
        State(state): State<FixtureState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        state.submit_calls.fetch_add(1, Ordering::SeqCst);
        *state.body.lock().unwrap() = body.to_vec();
        let key_ok = headers
            .get("RELAYER_API_KEY")
            .and_then(|value| value.to_str().ok())
            == Some("good-key");
        state.auth_ok.store(key_ok, Ordering::SeqCst);
        if !key_ok {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"error":"auth"})),
            )
                .into_response();
        }
        axum::Json(json!({"transactionID":"tx-1","state":"STATE_NEW"})).into_response()
    }

    async fn nonce(headers: HeaderMap) -> Response {
        if headers.get("RELAYER_API_KEY").is_none() {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        axum::Json(json!({"address":SIGNER,"nonce":"7"})).into_response()
    }

    #[derive(Deserialize)]
    struct LegacyQuery {
        id: Option<String>,
    }

    async fn legacy_nonce(headers: HeaderMap) -> Response {
        if headers.get("RELAYER_API_KEY").is_none() {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        axum::Json(json!({"address":RELAYER,"nonce":"8"})).into_response()
    }

    async fn poll(
        State(state): State<FixtureState>,
        Path(transaction_id): Path<String>,
    ) -> Response {
        let delay = state.poll_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let call = state.poll_calls.fetch_add(1, Ordering::SeqCst);
        axum::Json(json!({
            "transaction_id": transaction_id,
            "transaction_hash": if call == 0 { serde_json::Value::Null } else { json!("0xabc") },
            "state": if call == 0 { "STATE_MINED" } else { "STATE_CONFIRMED" },
            "error_msg": null
        }))
        .into_response()
    }

    async fn legacy_poll(
        State(state): State<FixtureState>,
        Query(query): Query<LegacyQuery>,
    ) -> Response {
        let Some(transaction_id) = query.id else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let call = state.poll_calls.fetch_add(1, Ordering::SeqCst);
        axum::Json(json!([{
            "transactionID": transaction_id,
            "transactionHash": if call == 0 { serde_json::Value::Null } else { json!("0xdef") },
            "state": if call == 0 { "STATE_EXECUTED" } else { "STATE_CONFIRMED" },
            "errorMsg": null
        }]))
        .into_response()
    }

    async fn server(state: FixtureState) -> String {
        let app = Router::new()
            .route(RELAYER_SUBMIT_PATH, post(submit))
            .route(RELAYER_DEPOSIT_WALLET_NONCE_PATH, get(nonce))
            .route(RELAYER_LEGACY_NONCE_PATH, get(legacy_nonce))
            .route("/v1/account/transactions/{transaction_id}", get(poll))
            .route(RELAYER_LEGACY_TRANSACTION_PATH, get(legacy_poll))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn relayer_submit_and_confirm_captures_raw_evidence() {
        let state = FixtureState::default();
        let host = server(state.clone()).await;
        let client = RelayerTransportClient::new(
            host,
            credentials("good-key"),
            policy(Duration::from_secs(1)),
        )
        .unwrap();
        let nonce = client
            .fetch_nonce(SIGNER, CustodyKind::DepositWallet)
            .await
            .unwrap();
        assert_eq!(nonce.custody, CustodyKind::DepositWallet);
        assert_eq!(nonce.nonce, "7");

        let confirmed = client
            .submit_and_confirm(&signed(CustodyKind::DepositWallet))
            .await
            .unwrap();
        assert_eq!(confirmed.transaction_id, "tx-1");
        assert_eq!(confirmed.transaction_hash, "0xabc");
        assert_eq!(confirmed.evidence.len(), 3);
        assert_eq!(state.submit_calls.load(Ordering::SeqCst), 1);
        assert!(state.auth_ok.load(Ordering::SeqCst));
        let body: serde_json::Value = serde_json::from_slice(&state.body.lock().unwrap()).unwrap();
        assert_eq!(body["type"], "WALLET");
        assert_eq!(
            body["depositWalletParams"]["calls"][0]["target"],
            call(false).to
        );
        assert_eq!(
            body["depositWalletParams"]["calls"][0]["data"],
            hex(&call(false).calldata)
        );
    }

    #[tokio::test]
    async fn relayer_auth_failure_is_typed_and_never_retried() {
        let state = FixtureState::default();
        let host = server(state.clone()).await;
        let client = RelayerTransportClient::new(
            host,
            credentials("bad-key"),
            policy(Duration::from_secs(1)),
        )
        .unwrap();
        let error = client
            .submit_and_confirm(&signed(CustodyKind::DepositWallet))
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            RedemptionTransportError::Authentication {
                phase: "submit",
                status: 401,
                ..
            }
        ));
        assert_eq!(state.submit_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn timeout_after_submit_is_ambiguous_and_reconcilable() {
        let state = FixtureState::default();
        state.poll_delay_ms.store(100, Ordering::SeqCst);
        let host = server(state.clone()).await;
        let client = RelayerTransportClient::new(
            host,
            credentials("good-key"),
            policy(Duration::from_millis(10)),
        )
        .unwrap();
        let error = client
            .submit_and_confirm(&signed(CustodyKind::DepositWallet))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RedemptionTransportError::AmbiguousAfterSubmit { .. }
        ));
        if let RedemptionTransportError::AmbiguousAfterSubmit {
            transaction_id,
            submit_body_hash,
            evidence,
            ..
        } = error
        {
            assert_eq!(transaction_id.as_deref(), Some("tx-1"));
            assert!(!submit_body_hash.is_empty());
            assert_eq!(
                evidence.len(),
                1,
                "the accepted submit response is retained"
            );
        }
        assert_eq!(state.submit_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unsupported_eoa_custody_fails_closed() {
        let client = RelayerTransportClient::new(
            RELAYER_BASE_URL,
            credentials("good-key"),
            policy(Duration::from_secs(1)),
        )
        .unwrap();
        assert!(matches!(
            client.build_submission_at(&signed(CustodyKind::Eoa), 1_000),
            Err(RedemptionTransportError::UnsupportedCustody(
                UnsupportedCustody {
                    custody: CustodyKind::Eoa
                }
            ))
        ));
    }

    #[test]
    fn custody_specific_requests_use_current_official_paths() {
        let client = RelayerTransportClient::new(
            RELAYER_BASE_URL,
            credentials("good-key"),
            policy(Duration::from_secs(1)),
        )
        .unwrap();
        let wallet_nonce = client
            .build_nonce_request_at(SIGNER, CustodyKind::DepositWallet, 1_000)
            .unwrap();
        assert_eq!(wallet_nonce.path, RELAYER_DEPOSIT_WALLET_NONCE_PATH);
        assert_eq!(wallet_nonce.ordered_query[1].1, "WALLET");
        let safe_nonce = client
            .build_nonce_request_at(SIGNER, CustodyKind::Safe, 1_000)
            .unwrap();
        assert_eq!(safe_nonce.path, RELAYER_LEGACY_NONCE_PATH);
        assert_eq!(safe_nonce.ordered_query[1].1, "SAFE");

        let wallet_poll = client
            .build_poll_request_at("tx-1", SIGNER, CustodyKind::DepositWallet, 1_000)
            .unwrap();
        assert_eq!(
            wallet_poll.path,
            format!("{RELAYER_DEPOSIT_WALLET_TRANSACTION_PATH_PREFIX}tx-1")
        );
        assert!(wallet_poll.ordered_query.is_empty());
        let proxy_poll = client
            .build_poll_request_at("tx-1", SIGNER, CustodyKind::Proxy, 1_000)
            .unwrap();
        assert_eq!(proxy_poll.path, RELAYER_LEGACY_TRANSACTION_PATH);
        assert_eq!(
            proxy_poll.ordered_query,
            [("id".to_owned(), "tx-1".to_owned())]
        );
    }

    #[test]
    fn builder_auth_headers_match_hmac_golden() {
        let headers = authentication_headers(
            &RelayerCredentials::BuilderApiKey(BuilderApiKeyCredentials {
                api_key: "builder-key".to_owned(),
                api_secret: "c2VjcmV0".to_owned(),
                api_passphrase: "builder-passphrase".to_owned(),
            }),
            "POST",
            RELAYER_SUBMIT_PATH,
            br#"{"x":1}"#,
            1_000,
            SIGNER,
        )
        .unwrap();
        assert_eq!(
            headers,
            [
                ("POLY_BUILDER_API_KEY".to_owned(), "builder-key".to_owned()),
                ("POLY_BUILDER_TIMESTAMP".to_owned(), "1000".to_owned()),
                (
                    "POLY_BUILDER_PASSPHRASE".to_owned(),
                    "builder-passphrase".to_owned()
                ),
                (
                    "POLY_BUILDER_SIGNATURE".to_owned(),
                    "nSb5frFB28ayCX28ri2UQhMq2t-qXbez-BtsP8xF2TM=".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn proxy_and_safe_requests_use_typed_legacy_envelopes() {
        let client = RelayerTransportClient::new(
            RELAYER_BASE_URL,
            credentials("good-key"),
            policy(Duration::from_secs(1)),
        )
        .unwrap();
        for (custody, expected_type) in [(CustodyKind::Proxy, "PROXY"), (CustodyKind::Safe, "SAFE")]
        {
            let request = client.build_submission_at(&signed(custody), 1_000).unwrap();
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["type"], expected_type);
            assert_eq!(body["proxyWallet"], WALLET);
            assert_eq!(body["to"], call(false).to);
        }
    }

    #[tokio::test]
    async fn proxy_and_safe_transports_confirm_through_legacy_status_contract() {
        for custody in [CustodyKind::Proxy, CustodyKind::Safe] {
            let state = FixtureState::default();
            let host = server(state).await;
            let client = RelayerTransportClient::new(
                host,
                credentials("good-key"),
                policy(Duration::from_secs(1)),
            )
            .unwrap();
            let nonce = client.fetch_nonce(SIGNER, custody).await.unwrap();
            assert_eq!(nonce.custody, custody);
            assert_eq!(nonce.address, RELAYER);
            assert_eq!(nonce.nonce, "8");
            let confirmed = client.submit_and_confirm(&signed(custody)).await.unwrap();
            assert_eq!(confirmed.transaction_id, "tx-1");
            assert_eq!(confirmed.transaction_hash, "0xdef");
        }
    }

    #[tokio::test]
    async fn unverified_approval_reader_fails_closed_for_both_adapters() {
        let requests = redemption_approval_requests(WALLET).unwrap();
        assert_eq!(requests[0].adapter, STANDARD_COLLATERAL_ADAPTER);
        assert_eq!(requests[1].adapter, NEGRISK_COLLATERAL_ADAPTER);
        let reader = UnverifiedApprovalReader;
        for request in &requests {
            assert_eq!(
                reader.is_approved_for_all(request).await,
                Err(ApprovalReadError::Unavailable)
            );
        }
        assert_eq!(
            require_verified_approval(None),
            Err(RedemptionError::ApprovalUnverified)
        );
    }
}
