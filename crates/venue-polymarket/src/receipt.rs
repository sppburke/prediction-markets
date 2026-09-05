//! Pure Polygon JSON-RPC finality and Polymarket V2 `OrderFilled` verification (#545).
//!
//! Transport stays service-owned. This module accepts retained response bytes and validates only
//! the exact V2 exchange/order identity frozen by [`PreparedPolymarketBuy`].

use std::str::FromStr as _;

use alloy_primitives::{Address, B256, U256, address, b256};
use pe_core_types::{CollateralAmount, ShareAmount};
use serde::Deserialize;
use serde_json::Value;

use crate::PreparedPolymarketBuy;

/// Polygon PoS chain id required by ordinary live finality.
pub const FINALIZED_CHAIN_ID: u64 = 137;

/// Current standard V2 exchange selected by ordinary preparation.
pub const CTF_EXCHANGE_V2: Address = address!("E111180000d2663C0091e4f400237545B87B996B");

/// Current NegRisk V2 exchange selected by ordinary preparation.
pub const NEG_RISK_CTF_EXCHANGE_V2: Address = address!("e2222d279d744050d28e00520010520000310F59");

/// `keccak256("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")`.
pub const TOPIC_ORDER_FILLED_V2: B256 =
    b256!("d543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedBlock {
    pub number: u64,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedReceipt {
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_hash: String,
    logs: Vec<ReceiptLog>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceiptLog {
    transaction_hash: String,
    block_number: u64,
    block_hash: String,
    log_index: u64,
    address: Address,
    topics: Vec<B256>,
    data: String,
    removed: bool,
    raw_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOrderFill {
    pub transaction_hash: String,
    pub log_index: u64,
    pub principal: CollateralAmount,
    pub quantity: ShareAmount,
    pub fee: CollateralAmount,
    /// Stable hash of the complete JSON log object, including fields not used in decoding.
    pub raw_log_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReceiptError {
    #[error("malformed JSON-RPC response")]
    MalformedRpc,
    #[error("JSON-RPC returned an error")]
    RpcError,
    #[error("JSON-RPC result is missing")]
    MissingResult,
    #[error("the finalized block tag is unsupported")]
    UnsupportedFinalizedTag,
    #[error("hexadecimal quantity or identity is malformed")]
    MalformedHex,
    #[error("JSON-RPC transaction identity disagrees with the request")]
    TransactionMismatch,
    #[error("receipt reports a reverted transaction")]
    Reverted,
    #[error("receipt or log block identity is inconsistent")]
    BlockIdentityMismatch,
    #[error("an OrderFilled log was removed")]
    RemovedLog,
    #[error("an OrderFilled log has a malformed ABI shape")]
    MalformedOrderFilled,
    #[error("an OrderFilled log conflicts with the prepared order")]
    OrderIdentityMismatch,
    #[error("an OrderFilled amount exceeds the exact six-decimal representation")]
    AmountOverflow,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireReceipt {
    transaction_hash: String,
    block_number: String,
    block_hash: String,
    status: String,
    logs: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireLog {
    transaction_hash: String,
    block_number: String,
    block_hash: String,
    log_index: String,
    address: String,
    topics: Vec<String>,
    data: String,
    removed: bool,
}

/// Parse and require the chain id result returned by `eth_chainId`.
pub fn parse_chain_id_response(bytes: &[u8]) -> Result<u64, ReceiptError> {
    let result = rpc_result(bytes)?;
    let raw = result.as_str().ok_or(ReceiptError::MalformedRpc)?;
    parse_quantity(raw)
}

/// Parse the non-null block returned by `eth_getBlockByNumber("finalized", false)`.
pub fn parse_finalized_block_response(bytes: &[u8]) -> Result<FinalizedBlock, ReceiptError> {
    let result = rpc_result(bytes)?;
    if result.is_null() {
        return Err(ReceiptError::UnsupportedFinalizedTag);
    }
    parse_block(result)
}

/// Parse `eth_getTransactionReceipt`; a null result is an ordinary pending receipt.
pub fn parse_receipt_response(
    bytes: &[u8],
    requested_transaction_hash: &str,
) -> Result<Option<MatchedReceipt>, ReceiptError> {
    let result = rpc_result(bytes)?;
    if result.is_null() {
        return Ok(None);
    }
    let wire: WireReceipt =
        serde_json::from_value(result.clone()).map_err(|_| ReceiptError::MalformedRpc)?;
    let transaction_hash = canonical_b256(&wire.transaction_hash)?;
    if transaction_hash != canonical_b256(requested_transaction_hash)? {
        return Err(ReceiptError::TransactionMismatch);
    }
    if parse_quantity(&wire.status)? != 1 {
        return Err(ReceiptError::Reverted);
    }
    let block_number = parse_quantity(&wire.block_number)?;
    let block_hash = canonical_b256(&wire.block_hash)?;
    let mut logs = Vec::with_capacity(wire.logs.len());
    for raw in wire.logs {
        let raw_bytes = serde_json::to_vec(&raw).map_err(|_| ReceiptError::MalformedRpc)?;
        let raw_hash = blake3::hash(&raw_bytes).to_hex().to_string();
        let item: WireLog = serde_json::from_value(raw).map_err(|_| ReceiptError::MalformedRpc)?;
        let log_transaction_hash = canonical_b256(&item.transaction_hash)?;
        let log_block_number = parse_quantity(&item.block_number)?;
        let log_block_hash = canonical_b256(&item.block_hash)?;
        if log_transaction_hash != transaction_hash
            || log_block_number != block_number
            || log_block_hash != block_hash
        {
            return Err(ReceiptError::BlockIdentityMismatch);
        }
        logs.push(ReceiptLog {
            transaction_hash: log_transaction_hash,
            block_number: log_block_number,
            block_hash: log_block_hash,
            log_index: parse_quantity(&item.log_index)?,
            address: Address::from_str(&item.address).map_err(|_| ReceiptError::MalformedHex)?,
            topics: item
                .topics
                .iter()
                .map(|topic| B256::from_str(topic).map_err(|_| ReceiptError::MalformedHex))
                .collect::<Result<Vec<_>, _>>()?,
            data: item.data,
            removed: item.removed,
            raw_hash,
        });
    }
    Ok(Some(MatchedReceipt {
        transaction_hash,
        block_number,
        block_hash,
        logs,
    }))
}

/// Require a canonical block response to bind the receipt height and hash.
pub fn canonical_block_matches(
    bytes: &[u8],
    expected_number: u64,
    expected_hash: &str,
) -> Result<FinalizedBlock, ReceiptError> {
    let result = rpc_result(bytes)?;
    if result.is_null() {
        return Err(ReceiptError::MissingResult);
    }
    let block = parse_block(result)?;
    if block.number != expected_number || block.hash != canonical_b256(expected_hash)? {
        return Err(ReceiptError::BlockIdentityMismatch);
    }
    Ok(block)
}

/// Decode every log for the prepared order. Unrelated contracts, topics, and order hashes are
/// ignored; once the prepared order hash matches, every remaining identity and ABI word is strict.
pub fn decode_order_fills(
    receipt: &MatchedReceipt,
    prepared: &PreparedPolymarketBuy,
) -> Result<Vec<DecodedOrderFill>, ReceiptError> {
    let expected_exchange = Address::from_str(&prepared.verifying_contract)
        .map_err(|_| ReceiptError::OrderIdentityMismatch)?;
    let supported_exchange = if prepared.neg_risk {
        NEG_RISK_CTF_EXCHANGE_V2
    } else {
        CTF_EXCHANGE_V2
    };
    if prepared.exchange_domain_version != 2
        || expected_exchange != supported_exchange
        || !prepared.side.eq_ignore_ascii_case("buy")
    {
        return Err(ReceiptError::OrderIdentityMismatch);
    }
    let expected_order_hash =
        B256::from_str(&prepared.order_hash).map_err(|_| ReceiptError::OrderIdentityMismatch)?;
    let expected_maker =
        Address::from_str(&prepared.maker).map_err(|_| ReceiptError::OrderIdentityMismatch)?;
    let expected_token =
        U256::from_str(&prepared.token_id.0).map_err(|_| ReceiptError::OrderIdentityMismatch)?;
    let expected_builder =
        B256::from_str(&prepared.builder).map_err(|_| ReceiptError::OrderIdentityMismatch)?;
    let expected_metadata =
        B256::from_str(&prepared.metadata).map_err(|_| ReceiptError::OrderIdentityMismatch)?;

    if expected_builder != B256::ZERO {
        return Err(ReceiptError::OrderIdentityMismatch);
    }

    let mut decoded = Vec::new();
    for log in &receipt.logs {
        if log.address != expected_exchange
            || log.topics.first().copied() != Some(TOPIC_ORDER_FILLED_V2)
        {
            continue;
        }
        if log.topics.get(1).copied() != Some(expected_order_hash) {
            continue;
        }
        if log.removed {
            return Err(ReceiptError::RemovedLog);
        }
        if log.topics.len() != 4 || topic_address(log.topics[2])? != expected_maker {
            return Err(ReceiptError::OrderIdentityMismatch);
        }
        let words = data_words(&log.data)?;
        let side = word_u64(&words[0])?;
        let token = U256::from_be_slice(&words[1]);
        let builder = B256::from_slice(&words[5]);
        let metadata = B256::from_slice(&words[6]);
        if side != 0
            || token != expected_token
            || builder != expected_builder
            || metadata != expected_metadata
        {
            return Err(ReceiptError::OrderIdentityMismatch);
        }
        decoded.push(DecodedOrderFill {
            transaction_hash: log.transaction_hash.clone(),
            log_index: log.log_index,
            principal: CollateralAmount::from_atomic(word_u64(&words[2])?),
            quantity: ShareAmount::from_atomic(word_u64(&words[3])?),
            fee: CollateralAmount::from_atomic(word_u64(&words[4])?),
            raw_log_hash: log.raw_hash.clone(),
        });
    }
    Ok(decoded)
}

fn rpc_result(bytes: &[u8]) -> Result<Value, ReceiptError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| ReceiptError::MalformedRpc)?;
    let object = value.as_object().ok_or(ReceiptError::MalformedRpc)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(ReceiptError::MalformedRpc);
    }
    if object.get("error").is_some_and(|error| !error.is_null()) {
        return Err(ReceiptError::RpcError);
    }
    object
        .get("result")
        .cloned()
        .ok_or(ReceiptError::MissingResult)
}

fn parse_block(value: Value) -> Result<FinalizedBlock, ReceiptError> {
    let object = value.as_object().ok_or(ReceiptError::MalformedRpc)?;
    let number = object
        .get("number")
        .and_then(Value::as_str)
        .ok_or(ReceiptError::MalformedRpc)
        .and_then(parse_quantity)?;
    let hash = object
        .get("hash")
        .and_then(Value::as_str)
        .ok_or(ReceiptError::MalformedRpc)
        .and_then(canonical_b256)?;
    Ok(FinalizedBlock { number, hash })
}

fn parse_quantity(raw: &str) -> Result<u64, ReceiptError> {
    let digits = raw.strip_prefix("0x").ok_or(ReceiptError::MalformedHex)?;
    if digits.is_empty()
        || digits.len() > 16
        || !digits.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ReceiptError::MalformedHex);
    }
    u64::from_str_radix(digits, 16).map_err(|_| ReceiptError::MalformedHex)
}

fn canonical_b256(raw: &str) -> Result<String, ReceiptError> {
    B256::from_str(raw)
        .map(|value| format!("{value:#x}"))
        .map_err(|_| ReceiptError::MalformedHex)
}

fn topic_address(word: B256) -> Result<Address, ReceiptError> {
    let bytes = word.as_slice();
    if bytes[..12].iter().any(|byte| *byte != 0) {
        return Err(ReceiptError::MalformedOrderFilled);
    }
    Ok(Address::from_slice(&bytes[12..]))
}

fn data_words(data: &str) -> Result<Vec<[u8; 32]>, ReceiptError> {
    let digits = data
        .strip_prefix("0x")
        .ok_or(ReceiptError::MalformedOrderFilled)?;
    if digits.len() != 7 * 64 || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ReceiptError::MalformedOrderFilled);
    }
    let bytes =
        alloy_primitives::hex::decode(digits).map_err(|_| ReceiptError::MalformedOrderFilled)?;
    bytes
        .chunks_exact(32)
        .map(|word| {
            word.try_into()
                .map_err(|_| ReceiptError::MalformedOrderFilled)
        })
        .collect()
}

fn word_u64(word: &[u8]) -> Result<u64, ReceiptError> {
    if word.len() != 32 || word[..24].iter().any(|byte| *byte != 0) {
        return Err(ReceiptError::AmountOverflow);
    }
    let tail: [u8; 8] = word[24..]
        .try_into()
        .map_err(|_| ReceiptError::MalformedOrderFilled)?;
    Ok(u64::from_be_bytes(tail))
}
