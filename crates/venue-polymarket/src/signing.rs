//! L1 EIP-712 order signing and L2 HMAC-SHA256 API authentication.
//!
//! L1: EIP-712 signature over the Polymarket CTFExchange order struct using the
//! funder's EOA private key. Required by the on-chain exchange for order validity.
//!
//! L2: HMAC-SHA256 over (timestamp + method + path + body) using the CLOB API secret.
//! Required by the CLOB REST API for off-chain authentication.
//!
//! Canonical reference:
//! - L1: CTFExchange contract at `MAINNET_EXCHANGE_ADDRESS` (Polygon mainnet)
//! - L2: `docs/15-SOURCES.md` "Polymarket CLOB" "Last checked" entry

use alloy::primitives::{Address, U256};
use alloy::signers::Signer as _;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::{SolStruct as _, eip712_domain};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use time::OffsetDateTime;

use crate::error::PolymarketError;

// ── Constants ──────────────────────────────────────────────────────────────────

/// Polygon mainnet chain ID.
pub const MAINNET_CHAIN_ID: u64 = 137;

/// Polymarket CTFExchange contract address on Polygon mainnet.
pub const MAINNET_EXCHANGE_ADDRESS: Address =
    alloy::primitives::address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");

/// Signature type 0 = EOA (standard EIP-712 secp256k1 signature).
pub const SIG_TYPE_EOA: u8 = 0;

// ── L1 EIP-712 order struct ────────────────────────────────────────────────────

sol! {
    /// Polymarket CTFExchange order struct for EIP-712 signing.
    ///
    /// Field names and types must match the CTFExchange contract exactly.
    /// See: https://polygonscan.com/address/0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
    #[derive(Debug, PartialEq, Eq)]
    struct ClobOrder {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

/// Build the EIP-712 domain for the Polymarket CTFExchange.
pub fn ctf_exchange_domain(
    chain_id: u64,
    exchange_address: Address,
) -> alloy::sol_types::Eip712Domain {
    eip712_domain! {
        name: "CTF Exchange",
        version: "1",
        chain_id: chain_id,
        verifying_contract: exchange_address,
    }
}

/// Sign a `ClobOrder` with an EOA private key using EIP-712.
///
/// Returns the 65-byte signature (r + s + v) as a 0x-prefixed hex string.
/// Async because `alloy::signers::Signer::sign_hash` is an async method.
pub async fn sign_order_eip712(
    order: &ClobOrder,
    chain_id: u64,
    exchange_address: Address,
    private_key_hex: &str,
) -> Result<String, PolymarketError> {
    let signer: PrivateKeySigner = private_key_hex
        .parse()
        .map_err(|e| PolymarketError::Signing(format!("invalid private key: {e}")))?;
    let domain = ctf_exchange_domain(chain_id, exchange_address);
    let hash = order.eip712_signing_hash(&domain);
    let sig = signer
        .sign_hash(&hash)
        .await
        .map_err(|e| PolymarketError::Signing(format!("sign_hash failed: {e}")))?;
    let bytes = sig.as_bytes();
    Ok(format!("0x{}", alloy::hex::encode(bytes)))
}

// ── L2 HMAC-SHA256 authentication ─────────────────────────────────────────────

/// CLOB API credentials for L2 authentication.
pub struct L2Credentials {
    pub funder_address: String,
    pub api_key: String,
    /// Base64-encoded HMAC secret.
    pub api_secret_b64: String,
    pub api_passphrase: String,
}

/// Computed L2 auth headers to attach to each CLOB REST request.
#[derive(Debug, Clone)]
pub struct L2Headers {
    pub poly_address: String,
    pub poly_api_key: String,
    pub poly_signature: String,
    pub poly_timestamp: String,
    pub poly_passphrase: String,
}

type HmacSha256 = Hmac<Sha256>;

/// Compute L2 HMAC-SHA256 authentication headers for a CLOB REST request.
///
/// Message: `timestamp + METHOD + path + body` (concatenated, no separator).
/// Signature: `BASE64(HMAC-SHA256(BASE64DECODE(api_secret), message))`.
/// Reference: Polymarket CLOB API docs — https://docs.polymarket.com
pub fn compute_l2_headers(
    creds: &L2Credentials,
    method: &str,
    path: &str,
    body: &str,
    timestamp_secs: i64,
) -> Result<L2Headers, PolymarketError> {
    let secret = BASE64
        .decode(&creds.api_secret_b64)
        .map_err(PolymarketError::Base64)?;
    let message = format!("{timestamp_secs}{method}{path}{body}");
    let mut mac = HmacSha256::new_from_slice(&secret)
        .map_err(|e| PolymarketError::Signing(format!("HMAC key error: {e}")))?;
    mac.update(message.as_bytes());
    let sig_bytes = mac.finalize().into_bytes();
    let signature = BASE64.encode(sig_bytes);

    Ok(L2Headers {
        poly_address: creds.funder_address.clone(),
        poly_api_key: creds.api_key.clone(),
        poly_signature: signature,
        poly_timestamp: timestamp_secs.to_string(),
        poly_passphrase: creds.api_passphrase.clone(),
    })
}

/// Compute L2 headers using the current wall-clock timestamp.
pub fn compute_l2_headers_now(
    creds: &L2Credentials,
    method: &str,
    path: &str,
    body: &str,
) -> Result<L2Headers, PolymarketError> {
    let ts = OffsetDateTime::now_utc().unix_timestamp();
    compute_l2_headers(creds, method, path, body, ts)
}

// ── Amount helpers ─────────────────────────────────────────────────────────────

/// Convert a `rust_decimal::Decimal` amount (already floored to an integer value)
/// to a `U256` for inclusion in the on-chain order struct.
pub fn decimal_to_u256(d: rust_decimal::Decimal) -> Result<U256, PolymarketError> {
    let s = d.floor().to_string();
    s.parse::<U256>()
        .map_err(|_| PolymarketError::Signing(format!("cannot convert {s} to U256")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_creds() -> L2Credentials {
        L2Credentials {
            funder_address: "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf".into(),
            api_key: "test-key".into(),
            api_secret_b64: BASE64.encode(b"test-secret"),
            api_passphrase: "test-pass".into(),
        }
    }

    #[test]
    fn l2_hmac_is_deterministic() {
        let h1 = compute_l2_headers(&test_creds(), "POST", "/order", "{}", 1_000_000_000).unwrap();
        let h2 = compute_l2_headers(&test_creds(), "POST", "/order", "{}", 1_000_000_000).unwrap();
        assert_eq!(h1.poly_signature, h2.poly_signature);
        assert_eq!(h1.poly_timestamp, "1000000000");
    }

    #[test]
    fn l2_hmac_differs_by_timestamp() {
        let h1 = compute_l2_headers(&test_creds(), "POST", "/order", "{}", 1_000_000_000).unwrap();
        let h2 = compute_l2_headers(&test_creds(), "POST", "/order", "{}", 1_000_000_001).unwrap();
        assert_ne!(h1.poly_signature, h2.poly_signature);
    }

    #[test]
    fn l2_hmac_differs_by_method() {
        let h1 = compute_l2_headers(&test_creds(), "POST", "/order", "", 1_000_000_000).unwrap();
        let h2 = compute_l2_headers(&test_creds(), "GET", "/order", "", 1_000_000_000).unwrap();
        assert_ne!(h1.poly_signature, h2.poly_signature);
    }

    #[test]
    fn l2_hmac_output_is_valid_base64() {
        let h = compute_l2_headers(&test_creds(), "POST", "/order", "{}", 1_000_000_000).unwrap();
        // Must decode successfully
        BASE64.decode(&h.poly_signature).unwrap();
    }

    #[test]
    fn l2_hmac_golden() {
        // Golden value: HMAC-SHA256(key="test-secret", msg="1000000000POST/order{}")
        // Computed independently: openssl dgst -sha256 -hmac "test-secret" -binary | base64
        // Expected: verify on first run and lock down.
        let h = compute_l2_headers(&test_creds(), "POST", "/order", "{}", 1_000_000_000).unwrap();
        // Use insta to snapshot the golden value on first run.
        insta::assert_snapshot!(h.poly_signature);
    }

    #[test]
    fn decimal_to_u256_converts_correctly() {
        use rust_decimal_macros::dec;
        let d = dec!(6_500_000);
        let u = decimal_to_u256(d).unwrap();
        assert_eq!(u, U256::from(6_500_000u64));
    }

    #[test]
    fn decimal_to_u256_floors() {
        use rust_decimal_macros::dec;
        let d = dec!(6_500_000.99);
        let u = decimal_to_u256(d).unwrap();
        assert_eq!(u, U256::from(6_500_000u64));
    }

    #[test]
    fn clob_order_eip712_hash_is_deterministic() {
        let order = ClobOrder {
            salt: U256::from(12345u64),
            maker: "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse()
                .unwrap(),
            signer: "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse()
                .unwrap(),
            taker: Address::ZERO,
            tokenId: U256::from(1u64),
            makerAmount: U256::from(650_000u64),
            takerAmount: U256::from(1_000_000u64),
            expiration: U256::from(1_700_000_000u64),
            nonce: U256::ZERO,
            feeRateBps: U256::ZERO,
            side: 0,
            signatureType: SIG_TYPE_EOA,
        };
        let domain = ctf_exchange_domain(MAINNET_CHAIN_ID, MAINNET_EXCHANGE_ADDRESS);
        let h1 = order.eip712_signing_hash(&domain);
        let h2 = order.eip712_signing_hash(&domain);
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn sign_order_eip712_produces_valid_hex() {
        // Well-known test key (private key = 1).
        let pk = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let order = ClobOrder {
            salt: U256::from(1u64),
            maker: "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse()
                .unwrap(),
            signer: "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse()
                .unwrap(),
            taker: Address::ZERO,
            tokenId: U256::from(1u64),
            makerAmount: U256::from(500_000u64),
            takerAmount: U256::from(1_000_000u64),
            expiration: U256::from(1_700_000_000u64),
            nonce: U256::ZERO,
            feeRateBps: U256::ZERO,
            side: 0,
            signatureType: SIG_TYPE_EOA,
        };
        let sig = sign_order_eip712(&order, MAINNET_CHAIN_ID, MAINNET_EXCHANGE_ADDRESS, pk)
            .await
            .unwrap();
        assert!(sig.starts_with("0x"), "sig must be hex: {sig}");
        assert_eq!(sig.len(), 132, "65 bytes = 130 hex chars + '0x' prefix");
    }
}
