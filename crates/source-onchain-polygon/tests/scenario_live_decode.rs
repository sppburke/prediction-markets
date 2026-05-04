//! Scenario tests for ABI decoders: [`pe_source_onchain_polygon::decoder::decode_log`].
//!
//! Each test constructs a synthetic alloy `Log` from known inputs and asserts
//! the decoded [`PolygonEvent`] fields match golden values.
//! No live RPC calls; deterministic from in-process fixtures.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use alloy::primitives::{Address, B256, Bytes, Log as PrimLog, LogData, U256, address};
use alloy::rpc::types::Log;
use rust_decimal_macros::dec;

use pe_source_onchain_polygon::{
    contracts::{CTF, GNOSIS_SAFE_FACTORY, TOPIC_ERC20_TRANSFER, TOPIC_PROXY_CREATION, USDC, WCOL},
    decoder::decode_log,
    event::PolygonEvent,
};

// ── Fixture helpers ───────────────────────────────────────────────────────────

const BLOCK_NUMBER: u64 = 55_000_000;
const BLOCK_TIMESTAMP: u64 = 1_700_000_000; // 2023-11-14 22:13:20 UTC

fn addr_topic(a: Address) -> B256 {
    let mut t = B256::ZERO;
    t[12..].copy_from_slice(a.as_slice());
    t
}

fn u256_to_bytes(v: U256) -> Bytes {
    Bytes::from(v.to_be_bytes::<32>().to_vec())
}

fn make_erc20_transfer_log(contract: Address, from: Address, to: Address, value: U256) -> Log {
    let topics = vec![TOPIC_ERC20_TRANSFER, addr_topic(from), addr_topic(to)];
    let data = LogData::new_unchecked(topics, u256_to_bytes(value));
    Log {
        inner: PrimLog {
            address: contract,
            data,
        },
        block_number: Some(BLOCK_NUMBER),
        block_timestamp: Some(BLOCK_TIMESTAMP),
        transaction_hash: Some(alloy::primitives::TxHash::ZERO),
        transaction_index: None,
        block_hash: None,
        log_index: None,
        removed: false,
    }
}

fn make_proxy_creation_log(proxy: Address, singleton: Address) -> Log {
    // ProxyCreation(address indexed proxy, address singleton)
    // topic0 + indexed proxy in topic1; singleton is non-indexed data (ABI-encoded address)
    let topics = vec![TOPIC_PROXY_CREATION, addr_topic(proxy)];
    let mut data_bytes = [0u8; 32];
    data_bytes[12..].copy_from_slice(singleton.as_slice());
    let data = LogData::new_unchecked(topics, Bytes::from(data_bytes.to_vec()));
    Log {
        inner: PrimLog {
            address: GNOSIS_SAFE_FACTORY,
            data,
        },
        block_number: Some(BLOCK_NUMBER),
        block_timestamp: Some(BLOCK_TIMESTAMP),
        transaction_hash: Some(alloy::primitives::TxHash::ZERO),
        transaction_index: None,
        block_hash: None,
        log_index: None,
        removed: false,
    }
}

// ── Scenarios ─────────────────────────────────────────────────────────────────

#[test]
fn scenario_usdc_transfer_decoded() {
    let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let to = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let value = U256::from(5_000_000u64); // 5.000000 USDC

    let log = make_erc20_transfer_log(USDC, from, to, value);
    let event = decode_log(&log).expect("USDC Transfer should decode");

    match event {
        PolygonEvent::UsdcTransfer {
            from: f,
            to: t,
            amount_usd,
            block_number,
            to_collateral_contract,
            from_collateral_contract,
            ..
        } => {
            assert_eq!(f.0, *from.as_slice(), "from address");
            assert_eq!(t.0, *to.as_slice(), "to address");
            assert_eq!(amount_usd, dec!(5.000000), "amount_usd");
            assert_eq!(block_number, BLOCK_NUMBER, "block_number");
            assert!(!to_collateral_contract, "to is not CTF");
            assert!(!from_collateral_contract, "from is not CTF");
        }
        other => panic!("expected UsdcTransfer, got {other:?}"),
    }
}

#[test]
fn scenario_usdc_transfer_to_ctf_sets_flag() {
    let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let value = U256::from(1_000_000u64); // 1 USDC

    let log = make_erc20_transfer_log(USDC, from, CTF, value);
    let event = decode_log(&log).expect("USDC → CTF Transfer should decode");

    match event {
        PolygonEvent::UsdcTransfer {
            to_collateral_contract,
            from_collateral_contract,
            ..
        } => {
            assert!(to_collateral_contract, "to CTF should set flag");
            assert!(!from_collateral_contract);
        }
        other => panic!("expected UsdcTransfer, got {other:?}"),
    }
}

#[test]
fn scenario_wcol_mint_decoded() {
    // WCOL Transfer with from = 0x0 → PUsdMint
    let to = address!("cccccccccccccccccccccccccccccccccccccccc");
    let value = U256::from(100_000_000u64); // 100.000000 WCOL

    let log = make_erc20_transfer_log(WCOL, Address::ZERO, to, value);
    let event = decode_log(&log).expect("WCOL Mint should decode");

    match event {
        PolygonEvent::PUsdMint {
            to: t,
            amount_usd,
            block_number,
            ..
        } => {
            assert_eq!(t.0, *to.as_slice(), "mint recipient");
            assert_eq!(amount_usd, dec!(100.000000), "amount_usd");
            assert_eq!(block_number, BLOCK_NUMBER);
        }
        other => panic!("expected PUsdMint, got {other:?}"),
    }
}

#[test]
fn scenario_wcol_burn_decoded() {
    // WCOL Transfer with to = 0x0 → PUsdBurn
    let from = address!("dddddddddddddddddddddddddddddddddddddddd");
    let value = U256::from(50_000_000u64); // 50.000000 WCOL

    let log = make_erc20_transfer_log(WCOL, from, Address::ZERO, value);
    let event = decode_log(&log).expect("WCOL Burn should decode");

    match event {
        PolygonEvent::PUsdBurn {
            from: f,
            amount_usd,
            ..
        } => {
            assert_eq!(f.0, *from.as_slice(), "burn from");
            assert_eq!(amount_usd, dec!(50.000000), "amount_usd");
        }
        other => panic!("expected PUsdBurn, got {other:?}"),
    }
}

#[test]
fn scenario_wcol_transfer_between_wallets_returns_none() {
    // WCOL transfer where neither end is 0x0 — not a Mint or Burn; skipped
    let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let to = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let value = U256::from(10_000_000u64);

    let log = make_erc20_transfer_log(WCOL, from, to, value);
    assert!(
        decode_log(&log).is_none(),
        "WCOL wallet-to-wallet should be None"
    );
}

#[test]
fn scenario_proxy_creation_decoded() {
    let proxy = address!("1111111111111111111111111111111111111111");
    let singleton = address!("2222222222222222222222222222222222222222");

    let log = make_proxy_creation_log(proxy, singleton);
    let event = decode_log(&log).expect("ProxyCreation should decode");

    match event {
        PolygonEvent::ProxyWalletDeployed {
            proxy: p,
            owner: o,
            block_number,
            ..
        } => {
            assert_eq!(p.0, *proxy.as_slice(), "proxy address");
            assert_eq!(o.0, *singleton.as_slice(), "owner (singleton) address");
            assert_eq!(block_number, BLOCK_NUMBER);
        }
        other => panic!("expected ProxyWalletDeployed, got {other:?}"),
    }
}

#[test]
fn scenario_unknown_contract_returns_none() {
    let unknown = address!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let to = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let log = make_erc20_transfer_log(unknown, from, to, U256::from(1_000_000u64));
    assert!(
        decode_log(&log).is_none(),
        "unknown contract should be None"
    );
}

#[test]
fn scenario_log_without_timestamp_returns_none() {
    let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let to = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let mut log = make_erc20_transfer_log(USDC, from, to, U256::from(1_000_000u64));
    log.block_timestamp = None; // simulate non-Alchemy node

    assert!(
        decode_log(&log).is_none(),
        "log without block_timestamp should be None"
    );
}
