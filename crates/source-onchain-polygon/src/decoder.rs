//! ABI decoders: alloy [`alloy::rpc::types::Log`] → [`PolygonEvent`].
//!
//! Each decoder corresponds to one on-chain event. Unrecognised logs, logs with
//! missing RPC fields, and logs that decode cleanly but carry no semantically
//! useful state (e.g. WCOL transfer between two non-zero addresses) return `None`.
//!
//! **Alchemy requirement**: [`decode_log`] relies on `block_timestamp` being
//! present in the RPC response. Standard Ethereum nodes may omit this field;
//! Alchemy populates it. Logs without a timestamp are silently skipped.

use alloy::{primitives::Address, rpc::types::Log, sol, sol_types::SolEvent};
use rust_decimal::Decimal;
use time::OffsetDateTime;

use pe_core_types::{SourceTimestamp, WalletAddress};

use crate::{
    contracts::{
        COLLATERAL_DECIMALS, CTF, GNOSIS_SAFE_FACTORY, TOPIC_ERC20_TRANSFER, TOPIC_PROXY_CREATION,
        USDC, WCOL,
    },
    event::{PolygonEvent, TxHash},
};

// ── Solidity event definitions ────────────────────────────────────────────────

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event ProxyCreation(address indexed proxy, address singleton);
}

// ── Public decode entry point ─────────────────────────────────────────────────

/// Decode one raw RPC log into a [`PolygonEvent`], or `None` to skip it.
pub fn decode_log(log: &Log) -> Option<PolygonEvent> {
    let address = log.inner.address;
    let topic0 = log.inner.topics().first().copied()?;
    let block_number = log.block_number?;
    let alloy_tx_hash = log.transaction_hash?;
    let timestamp = block_timestamp(log)?;
    let tx_hash = TxHash(alloy_tx_hash.0);

    if address == USDC && topic0 == TOPIC_ERC20_TRANSFER {
        decode_usdc_transfer(log, block_number, tx_hash, timestamp)
    } else if address == WCOL && topic0 == TOPIC_ERC20_TRANSFER {
        decode_wcol_transfer(log, block_number, tx_hash, timestamp)
    } else if address == GNOSIS_SAFE_FACTORY && topic0 == TOPIC_PROXY_CREATION {
        decode_proxy_creation(log, block_number, tx_hash, timestamp)
    } else {
        None
    }
}

// ── Private decoders ──────────────────────────────────────────────────────────

fn decode_usdc_transfer(
    log: &Log,
    block_number: u64,
    tx_hash: TxHash,
    timestamp: SourceTimestamp,
) -> Option<PolygonEvent> {
    let decoded = Transfer::decode_log(&log.inner).ok()?;
    let amount_usd = u256_to_decimal(decoded.value, COLLATERAL_DECIMALS)?;
    Some(PolygonEvent::UsdcTransfer {
        from: to_wallet_address(decoded.from),
        to: to_wallet_address(decoded.to),
        to_collateral_contract: decoded.to == CTF,
        from_collateral_contract: decoded.from == CTF,
        amount_usd,
        block_number,
        tx_hash,
        timestamp,
    })
}

fn decode_wcol_transfer(
    log: &Log,
    block_number: u64,
    tx_hash: TxHash,
    timestamp: SourceTimestamp,
) -> Option<PolygonEvent> {
    let decoded = Transfer::decode_log(&log.inner).ok()?;
    let amount_usd = u256_to_decimal(decoded.value, COLLATERAL_DECIMALS)?;

    if decoded.from == Address::ZERO {
        Some(PolygonEvent::PUsdMint {
            to: to_wallet_address(decoded.to),
            amount_usd,
            block_number,
            tx_hash,
            timestamp,
        })
    } else if decoded.to == Address::ZERO {
        Some(PolygonEvent::PUsdBurn {
            from: to_wallet_address(decoded.from),
            amount_usd,
            block_number,
            tx_hash,
            timestamp,
        })
    } else {
        None // WCOL transfer between two non-zero addresses; not tracked
    }
}

fn decode_proxy_creation(
    log: &Log,
    block_number: u64,
    tx_hash: TxHash,
    timestamp: SourceTimestamp,
) -> Option<PolygonEvent> {
    let decoded = ProxyCreation::decode_log(&log.inner).ok()?;
    Some(PolygonEvent::ProxyWalletDeployed {
        proxy: to_wallet_address(decoded.proxy),
        owner: to_wallet_address(decoded.singleton),
        block_number,
        tx_hash,
        timestamp,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn to_wallet_address(addr: Address) -> WalletAddress {
    WalletAddress(addr.0.0)
}

fn u256_to_decimal(value: alloy::primitives::U256, decimals: u32) -> Option<Decimal> {
    let raw: u128 = value.try_into().ok()?;
    let divisor = Decimal::from(10u64.pow(decimals));
    Some(Decimal::from(raw) / divisor)
}

fn block_timestamp(log: &Log) -> Option<SourceTimestamp> {
    // Alchemy nodes populate block_timestamp in eth_getLogs / subscription responses.
    // Standard Ethereum nodes may omit it; logs without a timestamp are skipped.
    let ts_secs = log.block_timestamp?;
    let odt = OffsetDateTime::from_unix_timestamp(ts_secs as i64).ok()?;
    Some(SourceTimestamp(odt))
}
