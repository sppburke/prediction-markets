//! Scenario: on-chain OrderFilled scanner end-to-end (issue #207 Slice 1b).
//!
//! Drives `counterparty_edges::scan_counterparty_edges` against an in-memory
//! [`InMemoryChainLogFetcher`] (no RPC) and a real on-disk `WalletCache`.
//!
//! PASS: a synthetic batch of one V1 and one V2 `OrderFilled` log persists to
//!       `counterparty_edges` with the version-correct shape — V1 carries both
//!       per-leg asset ids, V2 carries the unified `tokenId` + `side` — and the
//!       scan cursor advances to the requested `to_block`.
//! FAIL: any of the above does not hold (a row is missing, version dispatch
//!       picks the wrong branch, or the cursor is not persisted).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use alloy::primitives::{Address, B256, Bytes, LogData, U256};
use alloy::rpc::types::Log;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::counterparty_edges::{COUNTERPARTY_EDGES_CURSOR_KEY, scan_counterparty_edges};
use pe_source_onchain_polygon::contracts::{
    CTF_EXCHANGE_V1, CTF_EXCHANGE_V2, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
};
use pe_source_onchain_polygon::eth_logs::test_support::InMemoryChainLogFetcher;
use tempfile::TempDir;

fn addr(byte: u8) -> Address {
    let mut bytes = [0u8; 20];
    bytes[19] = byte;
    Address::from(bytes)
}

fn address_to_topic(a: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(a.as_slice());
    B256::from(bytes)
}

/// Build a V1 OrderFilled log with the given fields.
#[allow(clippy::too_many_arguments)]
fn v1_log(
    contract: Address,
    maker: Address,
    taker: Address,
    maker_asset_id: U256,
    taker_asset_id: U256,
    block_number: u64,
    log_index: u64,
    block_ts_secs: u64,
    tx_hash_byte: u8,
) -> Log {
    let mut data = Vec::with_capacity(5 * 32);
    for v in [
        maker_asset_id,
        taker_asset_id,
        U256::from(1_000_000u64),
        U256::from(2_000_000u64),
        U256::from(500u64),
    ] {
        data.extend_from_slice(&v.to_be_bytes::<32>());
    }
    let inner = alloy::primitives::Log {
        address: contract,
        data: LogData::new_unchecked(
            vec![
                TOPIC_ORDER_FILLED_V1,
                B256::ZERO,
                address_to_topic(maker),
                address_to_topic(taker),
            ],
            Bytes::from(data),
        ),
    };
    Log {
        inner,
        block_hash: None,
        block_number: Some(block_number),
        block_timestamp: Some(block_ts_secs),
        transaction_hash: Some(B256::repeat_byte(tx_hash_byte)),
        transaction_index: None,
        log_index: Some(log_index),
        removed: false,
    }
}

/// Build a V2 OrderFilled log. `side`: 0 = maker BUY, 1 = maker SELL.
#[allow(clippy::too_many_arguments)]
fn v2_log(
    contract: Address,
    maker: Address,
    taker: Address,
    side: u8,
    token_id: U256,
    block_number: u64,
    log_index: u64,
    block_ts_secs: u64,
    tx_hash_byte: u8,
) -> Log {
    let mut data = Vec::with_capacity(7 * 32);
    data.extend_from_slice(&U256::from(side).to_be_bytes::<32>());
    data.extend_from_slice(&token_id.to_be_bytes::<32>());
    data.extend_from_slice(&U256::from(7u64).to_be_bytes::<32>());
    data.extend_from_slice(&U256::from(8u64).to_be_bytes::<32>());
    data.extend_from_slice(&U256::from(9u64).to_be_bytes::<32>());
    data.extend_from_slice(B256::ZERO.as_slice());
    data.extend_from_slice(B256::ZERO.as_slice());
    let inner = alloy::primitives::Log {
        address: contract,
        data: LogData::new_unchecked(
            vec![
                TOPIC_ORDER_FILLED_V2,
                B256::ZERO,
                address_to_topic(maker),
                address_to_topic(taker),
            ],
            Bytes::from(data),
        ),
    };
    Log {
        inner,
        block_hash: None,
        block_number: Some(block_number),
        block_timestamp: Some(block_ts_secs),
        transaction_hash: Some(B256::repeat_byte(tx_hash_byte)),
        transaction_index: None,
        log_index: Some(log_index),
        removed: false,
    }
}

#[test]
fn scenario_counterparty_edges_persists_v1_v2_mix_and_advances_cursor() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();

    let v1 = v1_log(
        CTF_EXCHANGE_V1,
        addr(0xAA),
        addr(0xBB),
        U256::from(111u64),
        U256::from(222u64),
        100,
        0,
        1_700_000_000,
        0xD1,
    );
    let v2 = v2_log(
        CTF_EXCHANGE_V2,
        addr(0xCC),
        addr(0xDD),
        1, // SELL
        U256::from(999_999u64),
        150,
        0,
        1_700_000_001,
        0xD2,
    );

    let fetcher = InMemoryChainLogFetcher::ok(200, vec![v1, v2]);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Single chunk so the same canned logs aren't returned for each window.
    let report = rt
        .block_on(scan_counterparty_edges(
            &fetcher,
            &mut cache,
            0,
            Some(200),
            1_000,
        ))
        .expect("scan must succeed");

    // Per-row shape: V1 has separate asset ids + NULL side; V2 has unified
    // tokenId in maker_asset_id_dec + NULL taker_asset_id_dec + SELL side.
    let rows: Vec<(String, Option<String>, Option<i64>, i64)> = cache
        .raw_conn_for_test()
        .prepare("SELECT maker_asset_id_dec, taker_asset_id_dec, side, contract_version FROM counterparty_edges ORDER BY block_number")
        .unwrap()
        .query_map([], |r| Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<i64>>(2)?,
            r.get::<_, i64>(3)?,
        )))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    let v1_ok = rows[0] == ("111".to_string(), Some("222".to_string()), None, 1);
    let v2_ok = rows[1] == ("999999".to_string(), None, Some(1), 2);
    let cursor_ok = cache
        .get_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY)
        .as_deref()
        == Some("200");
    let count_ok = cache.counterparty_edge_count() == 2;
    let report_ok = report.edges_upserted == 2
        && report.edges_skipped == 0
        && report.from_block == 0
        && report.to_block == 200;

    let pass = v1_ok && v2_ok && cursor_ok && count_ok && report_ok;
    println!(
        "Scenario counterparty_edges_persists_v1_v2_mix_and_advances_cursor: {} \
         (v1={v1_ok} v2={v2_ok} cursor={cursor_ok} count={count_ok} report={report_ok})",
        if pass { "PASS" } else { "FAIL" }
    );
    assert!(
        pass,
        "expected V1+V2 persisted with version-correct shape and cursor=200"
    );
}
