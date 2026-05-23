//! `OrderFilled` (V1 and V2) ABI decoder for the Polymarket CTF Exchange contracts
//! (issue #207 Slice 1).
//!
//! Two contract versions emit subtly different `OrderFilled` events:
//!
//! - **V1** (`CTF_EXCHANGE_V1`, `NEG_RISK_CTF_EXCHANGE_V1`):
//!   `OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker,
//!                uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled,
//!                uint256 takerAmountFilled, uint256 fee)` — two separate per-leg asset
//!   ids (one is USDC collateral, one is a CTF position token).
//! - **V2** (`CTF_EXCHANGE_V2`, `NEG_RISK_CTF_EXCHANGE_V2`):
//!   `OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker,
//!                uint8 side, uint256 tokenId, uint256 makerAmountFilled,
//!                uint256 takerAmountFilled, uint256 fee, bytes32 builder,
//!                bytes32 sweepBuilder)` — one unified `tokenId` (always the position
//!   token) plus a `side` flag indicating whether the maker is BUY or SELL.
//!
//! The two are defined in private submodules so `alloy::sol!` can derive each
//! topic0 from its own canonical signature without a Rust-side name collision.
//! Topic0 values are cross-checked against the pinned constants in
//! [`crate::contracts`] (themselves self-validated by keccak-recomputation tests).
//!
//! Decoded token ids are emitted as **decimal strings** (`U256::to_string`) to
//! match the `token_conditions.token_id` join key (issue #207 Slice 0, populated
//! from Gamma's `clobTokenIds`). Amounts and fees are emitted as raw `U256`
//! values (decimal unit-conversion happens downstream in the reconciliation
//! step; the decoder is unit-agnostic).

use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;

use crate::contracts::{TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2};

// ── Solidity event definitions ────────────────────────────────────────────────
// Each version lives in its own module so `alloy::sol!` can derive a distinct
// `SIGNATURE_HASH` from "OrderFilled(<types>)" without colliding on the Rust
// type name.

mod v1 {
    alloy::sol! {
        event OrderFilled(
            bytes32 indexed orderHash,
            address indexed maker,
            address indexed taker,
            uint256 makerAssetId,
            uint256 takerAssetId,
            uint256 makerAmountFilled,
            uint256 takerAmountFilled,
            uint256 fee
        );
    }
}

mod v2 {
    alloy::sol! {
        event OrderFilled(
            bytes32 indexed orderHash,
            address indexed maker,
            address indexed taker,
            uint8 side,
            uint256 tokenId,
            uint256 makerAmountFilled,
            uint256 takerAmountFilled,
            uint256 fee,
            bytes32 builder,
            bytes32 sweepBuilder
        );
    }
}

// ── Decoded output ────────────────────────────────────────────────────────────

/// A decoded `OrderFilled` log, agnostic to V1 / V2.
///
/// Coordinate semantics:
/// - For V1, `maker_asset_id_dec` and `taker_asset_id_dec` carry the two
///   distinct per-leg ERC-1155 ids (one of which is USDC collateral, identified
///   at reconciliation time via `token_conditions`). `side` is `None`.
/// - For V2, `maker_asset_id_dec` carries the unified `tokenId` (always the
///   position token), `taker_asset_id_dec` is `None` (the other leg is implicit
///   USDC), and `side` carries the maker's direction (`Some(0)` = BUY,
///   `Some(1)` = SELL).
///
/// Amounts stay as raw `U256` — no unit conversion. The downstream reconciliation
/// joins to `token_conditions` to identify the USDC leg and converts amounts
/// using `crate::contracts::COLLATERAL_DECIMALS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOrderFilled {
    pub tx_hash: B256,
    pub log_index: u64,
    pub block_number: u64,
    pub block_ts_unix: i64,
    pub contract_addr: Address,
    /// `1` for V1 (`TOPIC_ORDER_FILLED_V1`), `2` for V2 (`TOPIC_ORDER_FILLED_V2`).
    /// Distinct from the bitmask values in `crate::contracts::CONTRACT_VERSION_BIT_*`.
    pub contract_version: u8,
    pub maker: Address,
    pub taker: Address,
    /// V1: `makerAssetId` (decimal `U256`). V2: `tokenId` (decimal `U256`, the
    /// position token regardless of `side`).
    pub maker_asset_id_dec: String,
    /// V1: `takerAssetId` (decimal `U256`). V2: `None` (other leg is implicit USDC).
    pub taker_asset_id_dec: Option<String>,
    /// V1: `None`. V2: `Some(0)` = maker is BUY side, `Some(1)` = maker is SELL side.
    pub side: Option<u8>,
    pub maker_amount_raw: U256,
    pub taker_amount_raw: U256,
    pub fee_raw: U256,
}

// ── Public decode entry point ─────────────────────────────────────────────────

/// Decode one raw RPC log into a [`DecodedOrderFilled`], dispatching on `topic0`.
///
/// Returns `None` (silently skipping the log) when:
/// - the log is missing `block_number` / `transaction_hash` / `block_timestamp` /
///   `log_index` (every consumer pipeline depends on all four),
/// - `topic0` is neither V1 nor V2 (caller's filter should already restrict to
///   `ALL_ORDER_FILLED_TOPICS`, but a wider filter must not panic the decoder),
/// - the ABI payload fails to decode (rare; logged by the caller for visibility).
///
/// Skipping (vs. erroring) matches the established `events.rs` and `decoder.rs`
/// best-effort policy: a single malformed log must not abort a multi-hour scan.
#[must_use]
pub fn decode_order_filled(log: &Log) -> Option<DecodedOrderFilled> {
    let topic0 = log.inner.topics().first().copied()?;
    let block_number = log.block_number?;
    let tx_hash = log.transaction_hash?;
    let log_index = log.log_index?;
    let block_ts_secs = log.block_timestamp?;
    let block_ts_unix = i64::try_from(block_ts_secs).ok()?;

    // Maker (topic2) / taker (topic3) are the indexed addresses on both versions.
    let topics = log.inner.topics();
    let maker = address_from_topic(topics.get(2))?;
    let taker = address_from_topic(topics.get(3))?;

    let contract_addr = log.inner.address;

    if topic0 == TOPIC_ORDER_FILLED_V1 {
        let decoded = v1::OrderFilled::decode_log(&log.inner).ok()?;
        Some(DecodedOrderFilled {
            tx_hash,
            log_index,
            block_number,
            block_ts_unix,
            contract_addr,
            contract_version: 1,
            maker,
            taker,
            maker_asset_id_dec: decoded.makerAssetId.to_string(),
            taker_asset_id_dec: Some(decoded.takerAssetId.to_string()),
            side: None,
            maker_amount_raw: decoded.makerAmountFilled,
            taker_amount_raw: decoded.takerAmountFilled,
            fee_raw: decoded.fee,
        })
    } else if topic0 == TOPIC_ORDER_FILLED_V2 {
        let decoded = v2::OrderFilled::decode_log(&log.inner).ok()?;
        Some(DecodedOrderFilled {
            tx_hash,
            log_index,
            block_number,
            block_ts_unix,
            contract_addr,
            contract_version: 2,
            maker,
            taker,
            maker_asset_id_dec: decoded.tokenId.to_string(),
            taker_asset_id_dec: None,
            side: Some(decoded.side),
            maker_amount_raw: decoded.makerAmountFilled,
            taker_amount_raw: decoded.takerAmountFilled,
            fee_raw: decoded.fee,
        })
    } else {
        None
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Read the lower 20 bytes of an indexed-address topic.
///
/// Mirrors `funder_discovery::topic_to_wallet` but returns `alloy::Address` so
/// the decoded value lines up with the rest of the alloy-typed event payload
/// (avoiding a round-trip through `WalletAddress`).
fn address_from_topic(topic: Option<&B256>) -> Option<Address> {
    let topic = topic?;
    let bytes = topic.as_slice();
    if bytes.len() != 32 {
        return None;
    }
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&bytes[12..]);
    Some(Address::from(addr))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use alloy::primitives::{Bytes, LogData};
    use alloy::rpc::types::Log as RpcLog;

    /// Synthesize a `Log` for a V1 `OrderFilled` event with the given indexed
    /// addresses + data payload. The data payload is built from five 32-byte
    /// big-endian uint256 chunks: `[makerAssetId, takerAssetId, makerAmount,
    /// takerAmount, fee]`.
    #[allow(clippy::too_many_arguments)]
    fn build_v1_log(
        contract: Address,
        maker: Address,
        taker: Address,
        maker_asset_id: U256,
        taker_asset_id: U256,
        maker_amount: U256,
        taker_amount: U256,
        fee: U256,
        block_number: u64,
        log_index: u64,
        block_ts_secs: u64,
        tx_hash: B256,
    ) -> RpcLog {
        let order_hash = B256::ZERO; // not used in tests
        let mut data = Vec::with_capacity(5 * 32);
        for v in [
            maker_asset_id,
            taker_asset_id,
            maker_amount,
            taker_amount,
            fee,
        ] {
            data.extend_from_slice(&v.to_be_bytes::<32>());
        }
        let topics = vec![
            TOPIC_ORDER_FILLED_V1,
            order_hash,
            address_to_topic(maker),
            address_to_topic(taker),
        ];
        let inner = alloy::primitives::Log {
            address: contract,
            data: LogData::new_unchecked(topics, Bytes::from(data)),
        };
        RpcLog {
            inner,
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: Some(block_ts_secs),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    /// Synthesize a `Log` for a V2 `OrderFilled` event. Data layout is
    /// `[side, tokenId, makerAmount, takerAmount, fee, builder, sweepBuilder]`
    /// — seven 32-byte chunks; `side` is left-padded to a uint256.
    #[allow(clippy::too_many_arguments)]
    fn build_v2_log(
        contract: Address,
        maker: Address,
        taker: Address,
        side: u8,
        token_id: U256,
        maker_amount: U256,
        taker_amount: U256,
        fee: U256,
        block_number: u64,
        log_index: u64,
        block_ts_secs: u64,
        tx_hash: B256,
    ) -> RpcLog {
        let order_hash = B256::ZERO;
        let builder = B256::ZERO;
        let sweep_builder = B256::ZERO;
        let mut data = Vec::with_capacity(7 * 32);
        data.extend_from_slice(&U256::from(side).to_be_bytes::<32>());
        data.extend_from_slice(&token_id.to_be_bytes::<32>());
        data.extend_from_slice(&maker_amount.to_be_bytes::<32>());
        data.extend_from_slice(&taker_amount.to_be_bytes::<32>());
        data.extend_from_slice(&fee.to_be_bytes::<32>());
        data.extend_from_slice(builder.as_slice());
        data.extend_from_slice(sweep_builder.as_slice());
        let topics = vec![
            TOPIC_ORDER_FILLED_V2,
            order_hash,
            address_to_topic(maker),
            address_to_topic(taker),
        ];
        let inner = alloy::primitives::Log {
            address: contract,
            data: LogData::new_unchecked(topics, Bytes::from(data)),
        };
        RpcLog {
            inner,
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: Some(block_ts_secs),
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn address_to_topic(addr: Address) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(addr.as_slice());
        B256::from(bytes)
    }

    fn addr(byte: u8) -> Address {
        let mut bytes = [0u8; 20];
        bytes[19] = byte;
        Address::from(bytes)
    }

    #[test]
    fn decodes_v1_fields_into_separate_per_leg_asset_ids() {
        let log = build_v1_log(
            addr(0xCC),
            addr(0xAA),
            addr(0xBB),
            U256::from(111u64),
            U256::from(222u64),
            U256::from(1_000_000u64),
            U256::from(2_000_000u64),
            U256::from(500u64),
            17_000_000,
            3,
            1_700_000_000,
            B256::repeat_byte(0xDE),
        );

        let decoded = decode_order_filled(&log).expect("v1 must decode");
        assert_eq!(decoded.contract_version, 1);
        assert_eq!(decoded.contract_addr, addr(0xCC));
        assert_eq!(decoded.maker, addr(0xAA));
        assert_eq!(decoded.taker, addr(0xBB));
        assert_eq!(decoded.maker_asset_id_dec, "111");
        assert_eq!(decoded.taker_asset_id_dec.as_deref(), Some("222"));
        assert!(decoded.side.is_none());
        assert_eq!(decoded.maker_amount_raw, U256::from(1_000_000u64));
        assert_eq!(decoded.taker_amount_raw, U256::from(2_000_000u64));
        assert_eq!(decoded.fee_raw, U256::from(500u64));
        assert_eq!(decoded.block_number, 17_000_000);
        assert_eq!(decoded.log_index, 3);
        assert_eq!(decoded.block_ts_unix, 1_700_000_000);
        assert_eq!(decoded.tx_hash, B256::repeat_byte(0xDE));
    }

    #[test]
    fn decodes_v2_fields_with_unified_token_id_and_side() {
        let log = build_v2_log(
            addr(0xCC),
            addr(0xAA),
            addr(0xBB),
            1, // SELL
            U256::from(999_999u64),
            U256::from(7u64),
            U256::from(8u64),
            U256::from(9u64),
            17_000_000,
            5,
            1_700_000_010,
            B256::repeat_byte(0xEF),
        );

        let decoded = decode_order_filled(&log).expect("v2 must decode");
        assert_eq!(decoded.contract_version, 2);
        assert_eq!(decoded.maker_asset_id_dec, "999999");
        assert!(decoded.taker_asset_id_dec.is_none());
        assert_eq!(decoded.side, Some(1));
        assert_eq!(decoded.maker_amount_raw, U256::from(7u64));
        assert_eq!(decoded.taker_amount_raw, U256::from(8u64));
        assert_eq!(decoded.fee_raw, U256::from(9u64));
        assert_eq!(decoded.log_index, 5);
    }

    #[test]
    fn decodes_v2_buy_side_zero() {
        let log = build_v2_log(
            addr(0xCC),
            addr(0xAA),
            addr(0xBB),
            0,
            U256::from(42u64),
            U256::from(1u64),
            U256::from(2u64),
            U256::from(3u64),
            1,
            0,
            1,
            B256::ZERO,
        );
        let decoded = decode_order_filled(&log).expect("v2 buy must decode");
        assert_eq!(decoded.side, Some(0));
    }

    #[test]
    fn handles_max_uint256_token_ids_via_decimal_string() {
        // ERC-1155 token ids can be full 256 bits; the decimal-string match
        // against token_conditions must round-trip the maximum value cleanly.
        let big = U256::MAX;
        let expected = big.to_string();
        let log = build_v2_log(
            addr(0xCC),
            addr(0xAA),
            addr(0xBB),
            0,
            big,
            U256::from(1u64),
            U256::from(1u64),
            U256::from(0u64),
            1,
            0,
            1,
            B256::ZERO,
        );
        let decoded = decode_order_filled(&log).expect("max-uint256 must decode");
        assert_eq!(decoded.maker_asset_id_dec, expected);
    }

    #[test]
    fn returns_none_for_unknown_topic0() {
        let unknown_topic = B256::repeat_byte(0xAB);
        let inner = alloy::primitives::Log {
            address: addr(0xCC),
            data: LogData::new_unchecked(
                vec![
                    unknown_topic,
                    B256::ZERO,
                    address_to_topic(addr(0xAA)),
                    address_to_topic(addr(0xBB)),
                ],
                Bytes::new(),
            ),
        };
        let log = RpcLog {
            inner,
            block_hash: None,
            block_number: Some(1),
            block_timestamp: Some(1),
            transaction_hash: Some(B256::ZERO),
            transaction_index: None,
            log_index: Some(0),
            removed: false,
        };
        assert!(decode_order_filled(&log).is_none());
    }

    #[test]
    fn returns_none_when_block_metadata_missing() {
        let log = build_v1_log(
            addr(0xCC),
            addr(0xAA),
            addr(0xBB),
            U256::from(1u64),
            U256::from(2u64),
            U256::from(3u64),
            U256::from(4u64),
            U256::from(5u64),
            1,
            0,
            1,
            B256::ZERO,
        );
        let mut no_ts = log.clone();
        no_ts.block_timestamp = None;
        assert!(
            decode_order_filled(&no_ts).is_none(),
            "missing block_timestamp must skip"
        );
        let mut no_block = log.clone();
        no_block.block_number = None;
        assert!(decode_order_filled(&no_block).is_none());
        let mut no_tx = log.clone();
        no_tx.transaction_hash = None;
        assert!(decode_order_filled(&no_tx).is_none());
        let mut no_idx = log.clone();
        no_idx.log_index = None;
        assert!(decode_order_filled(&no_idx).is_none());
    }
}
