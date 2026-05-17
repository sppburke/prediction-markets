//! Scenario: wallet-enumeration resilience and V1/V2 attribution (issue #186).
//!
//! Covers the three pieces of work in #186:
//!
//! 1. **Mid-chunk save resilience** — chunk-level upsert preserves prior
//!    chunks if a later chunk's RPC call fails. A crash at chunk N loses at
//!    most one chunk's worth of work, not the whole contract.
//! 2. **V1/V2 attribution** — `polymarket_contracts_seen` is OR-merged across
//!    upserts: a wallet that surfaces on the V1 topic and then later on the
//!    V2 topic ends up with both bits set.
//! 3. **Alloy migration** — the chunk-level path uses
//!    `PolymarketTraderEnumeration::enumerate_chunk` against a
//!    `ChainLogFetcher`, mirroring the production code path that lib.rs::run
//!    now exercises after the Etherscan REST → alloy switch.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Mutex;

use alloy::primitives::{Address, B256, Bytes, LogData};
use alloy::rpc::types::{Filter, Log};
use pe_bootstrap::cache::{WalletCache, WalletUpsertRow};
use pe_bootstrap::pile::SRC_WALLET_SET_JSON;
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::ChainLogFetcher;
use pe_source_onchain_polygon::contracts::{
    CONTRACT_VERSION_BIT_V1, CONTRACT_VERSION_BIT_V2, CTF_EXCHANGE_V1, CTF_EXCHANGE_V2,
    TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2, topic_to_contract_version_bit,
};
use pe_source_onchain_polygon::eth_logs::PolygonRpcError;
use pe_source_onchain_polygon::wallet_enumeration::{
    EnumerationConfig, PolymarketTraderEnumeration, SCAN_CHUNK_BLOCKS,
};
use tempfile::TempDir;

// ── Test fetcher that fails on a specific chunk ──────────────────────────────

/// `ChainLogFetcher` that returns `fixture` for every call until the call
/// hitting `fail_on_call_index` (0-based) — that one returns an error so the
/// caller can verify mid-sweep persistence of chunks already upserted.
struct FailAfterNFetcher {
    fixture: Vec<Log>,
    fail_on_call_index: usize,
    call_count: Mutex<usize>,
}

impl FailAfterNFetcher {
    fn new(fixture: Vec<Log>, fail_on_call_index: usize) -> Self {
        Self {
            fixture,
            fail_on_call_index,
            call_count: Mutex::new(0),
        }
    }
}

impl ChainLogFetcher for FailAfterNFetcher {
    async fn get_block_number(&self) -> Result<u64, PolygonRpcError> {
        Ok(SCAN_CHUNK_BLOCKS * 10)
    }

    async fn get_logs(
        &self,
        _filter: Filter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, PolygonRpcError> {
        let mut count = self.call_count.lock().unwrap();
        let idx = *count;
        *count += 1;
        if idx == self.fail_on_call_index {
            return Err(PolygonRpcError::GetLogs {
                from,
                to,
                message: "simulated mid-sweep failure".to_owned(),
            });
        }
        Ok(self.fixture.clone())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn w(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn wallet_topic(addr: WalletAddress) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(&addr.0);
    B256::from(bytes)
}

fn order_filled_log(topic0: B256, maker: WalletAddress, taker: WalletAddress) -> Log {
    let order_hash = B256::repeat_byte(0xaa);
    let inner = alloy::primitives::Log {
        address: Address::ZERO,
        data: LogData::new_unchecked(
            vec![topic0, order_hash, wallet_topic(maker), wallet_topic(taker)],
            Bytes::from(vec![0u8; 32]),
        ),
    };
    Log {
        inner,
        ..Default::default()
    }
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

fn make_row(wallet: WalletAddress, contract_bit: i64) -> WalletUpsertRow {
    (
        wallet.to_string(),
        SRC_WALLET_SET_JSON,
        false,
        None,
        None,
        None,
        contract_bit,
    )
}

// ── Scenario 1: per-chunk persistence ────────────────────────────────────────

/// PASS: a sweep that walks N chunks, upserting after each one, leaves all
///       wallets discovered in chunks `[0, K]` durably in the cache even if
///       chunk `K + 1` fails.
/// FAIL: a chunk-level RPC failure rolls back / discards work already
///       upserted for prior chunks — the bug that motivated #186 piece #1.
#[tokio::test]
async fn mid_sweep_crash_preserves_earlier_chunks() {
    let (_dir, mut cache) = open_cache();

    let alice = w(0xa1);
    let bob = w(0xb2);
    let logs = vec![order_filled_log(TOPIC_ORDER_FILLED_V1, alice, bob)];

    // 4 chunks: chunks 0, 1 succeed; chunk 2 fails; chunk 3 never runs.
    let fetcher = FailAfterNFetcher::new(logs, 2);
    let config = EnumerationConfig {
        from_block: 0,
        to_block: SCAN_CHUNK_BLOCKS * 4 - 1,
        operator_addresses: vec![],
    };
    let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);
    let bit = topic_to_contract_version_bit(TOPIC_ORDER_FILLED_V1).unwrap();

    // Simulate the chunk-level loop the bootstrap performs in lib.rs::run().
    let mut chunk_from: u64 = 0;
    let mut chunks_persisted = 0usize;
    let mut sweep_err: Option<String> = None;
    while chunk_from < SCAN_CHUNK_BLOCKS * 4 {
        let chunk_to = (chunk_from + SCAN_CHUNK_BLOCKS - 1).min(SCAN_CHUNK_BLOCKS * 4 - 1);
        match enumerator
            .enumerate_chunk(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, chunk_from, chunk_to)
            .await
        {
            Ok(found) => {
                let rows: Vec<WalletUpsertRow> =
                    found.iter().copied().map(|w| make_row(w, bit)).collect();
                cache.upsert_wallets_bulk(&rows).unwrap();
                chunks_persisted += 1;
                chunk_from = chunk_to + 1;
            }
            Err(e) => {
                sweep_err = Some(e.to_string());
                break;
            }
        }
    }

    assert_eq!(
        chunks_persisted, 2,
        "two chunks must have persisted before the simulated failure"
    );
    assert!(
        sweep_err.is_some(),
        "third chunk must surface the simulated RPC failure"
    );

    // Wallets from chunks 0 and 1 are durably in the cache.
    let alice_seen = cache.conn_for_test_contracts_seen(&alice.to_string());
    let bob_seen = cache.conn_for_test_contracts_seen(&bob.to_string());
    assert_eq!(
        alice_seen, CONTRACT_VERSION_BIT_V1,
        "alice must carry V1 bit after pre-failure chunks"
    );
    assert_eq!(
        bob_seen, CONTRACT_VERSION_BIT_V1,
        "bob must carry V1 bit after pre-failure chunks"
    );
}

// ── Scenario 2: V1 + V2 bit accumulation ─────────────────────────────────────

/// PASS: a wallet that appears in both a V1-topic chunk and a V2-topic chunk
///       ends up with both bits set in `polymarket_contracts_seen`.
/// FAIL: the second upsert overwrites the first (no OR-merge) and one bit is
///       lost — would defeat the whole point of the column.
#[test]
fn v1_and_v2_bits_accumulate_via_or_merge() {
    let (_dir, mut cache) = open_cache();
    let alice = w(0xab);

    // First upsert: V1 bit.
    cache
        .upsert_wallets_bulk(&[make_row(alice, CONTRACT_VERSION_BIT_V1)])
        .unwrap();
    assert_eq!(
        cache.conn_for_test_contracts_seen(&alice.to_string()),
        CONTRACT_VERSION_BIT_V1
    );

    // Second upsert: V2 bit. UPSERT must OR-merge with the existing column.
    cache
        .upsert_wallets_bulk(&[make_row(alice, CONTRACT_VERSION_BIT_V2)])
        .unwrap();
    let combined = cache.conn_for_test_contracts_seen(&alice.to_string());
    assert_eq!(
        combined,
        CONTRACT_VERSION_BIT_V1 | CONTRACT_VERSION_BIT_V2,
        "polymarket_contracts_seen must OR-merge to 0b11; got 0b{combined:b}"
    );

    // Third upsert: re-asserting V1 must not flip the V2 bit off.
    cache
        .upsert_wallets_bulk(&[make_row(alice, CONTRACT_VERSION_BIT_V1)])
        .unwrap();
    let again = cache.conn_for_test_contracts_seen(&alice.to_string());
    assert_eq!(
        again,
        CONTRACT_VERSION_BIT_V1 | CONTRACT_VERSION_BIT_V2,
        "re-asserting an existing bit must not clear any other bit"
    );
}

// ── Scenario 3: zero-bit non-enumeration paths preserve prior attribution ────

/// PASS: a wallet first discovered via on-chain enumeration with the V1 bit
///       set, then re-upserted later via a non-enumeration path (Dune, trades)
///       with `bit = 0`, retains its V1 bit. The OR-merge SQL never clears a
///       previously-set bit.
/// FAIL: subsequent zero-bit upserts overwrite the column, erasing V1/V2
///       attribution earned during the on-chain sweep.
#[test]
fn zero_bit_upserts_preserve_prior_contract_attribution() {
    let (_dir, mut cache) = open_cache();
    let alice = w(0xcd);

    cache
        .upsert_wallets_bulk(&[make_row(alice, CONTRACT_VERSION_BIT_V1)])
        .unwrap();
    // Simulate a later Dune-CSV or trade-fetch upsert that carries bit=0
    // because it cannot attribute V1 vs V2 from those sources.
    cache.upsert_wallets_bulk(&[make_row(alice, 0)]).unwrap();
    assert_eq!(
        cache.conn_for_test_contracts_seen(&alice.to_string()),
        CONTRACT_VERSION_BIT_V1,
        "bit=0 upsert must not clear pre-existing V1 attribution"
    );
}

// ── Scenario 4: schema migration is idempotent ───────────────────────────────

/// PASS: `WalletCache::open` may be called repeatedly on the same path
///       without error. The `ALTER TABLE ... ADD COLUMN
///       polymarket_contracts_seen` migration guards against re-adding the
///       column when it already exists.
/// FAIL: the second `open` returns a SQLite "duplicate column" error,
///       breaking backward-compat for already-deployed caches.
#[test]
fn schema_migration_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");

    {
        let cache = WalletCache::open(&path).unwrap();
        drop(cache);
    }
    // Second open on the same file must not error — proves the migration's
    // pragma_table_info guard works.
    let cache = WalletCache::open(&path).unwrap();
    drop(cache);
}

// ── Scenario 5: unknown topic → None bit ─────────────────────────────────────

/// PASS: `topic_to_contract_version_bit` returns `None` for any topic that is
///       not one of the known `OrderFilled` signatures, so the bootstrap's
///       fallback `.unwrap_or(0)` produces a zero bit — neither V1 nor V2 is
///       attributed.
/// FAIL: an unknown topic silently maps to V1 or V2, producing false
///       attribution.
#[test]
fn unknown_topic_returns_none_for_contract_bit() {
    let bogus = B256::repeat_byte(0xee);
    assert_eq!(
        topic_to_contract_version_bit(bogus),
        None,
        "unknown topic0 must return None — caller must fall back to bit=0"
    );

    // Known topics map deterministically.
    assert_eq!(
        topic_to_contract_version_bit(TOPIC_ORDER_FILLED_V1),
        Some(CONTRACT_VERSION_BIT_V1)
    );
    assert_eq!(
        topic_to_contract_version_bit(TOPIC_ORDER_FILLED_V2),
        Some(CONTRACT_VERSION_BIT_V2)
    );
}

// ── Scenario 6: enumerator outputs match V1 vs V2 contract source ────────────

/// PASS: a sweep against the V1 contract with the V1 topic returns wallets
///       carrying the V1 bit, and the V2 contract+topic produces the V2 bit.
///       The bootstrap's per-topic outer loop deterministically attributes
///       each chunk's wallets to the correct contract version.
/// FAIL: the bit picked up by upsert is swapped between V1 and V2, or both
///       map to the same value.
#[tokio::test]
async fn enumerator_attributes_v1_and_v2_to_distinct_bits() {
    let (_dir, mut cache) = open_cache();

    let v1_wallet = w(0x11);
    let v2_wallet = w(0x22);

    // Separate runs against separate (contract, topic) pairs.
    for (contract, topic, wallet, expected_bit) in [
        (
            CTF_EXCHANGE_V1,
            TOPIC_ORDER_FILLED_V1,
            v1_wallet,
            CONTRACT_VERSION_BIT_V1,
        ),
        (
            CTF_EXCHANGE_V2,
            TOPIC_ORDER_FILLED_V2,
            v2_wallet,
            CONTRACT_VERSION_BIT_V2,
        ),
    ] {
        let fetcher = FailAfterNFetcher::new(
            vec![order_filled_log(topic, wallet, wallet)],
            usize::MAX, // never fail
        );
        let config = EnumerationConfig {
            from_block: 0,
            to_block: SCAN_CHUNK_BLOCKS - 1,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);
        let bit = topic_to_contract_version_bit(topic).unwrap();
        let found = enumerator
            .enumerate_chunk(contract, topic, 0, SCAN_CHUNK_BLOCKS - 1)
            .await
            .unwrap();
        let rows: Vec<WalletUpsertRow> = found.iter().copied().map(|w| make_row(w, bit)).collect();
        cache.upsert_wallets_bulk(&rows).unwrap();
        let seen = cache.conn_for_test_contracts_seen(&wallet.to_string());
        assert_eq!(
            seen, expected_bit,
            "wallet {wallet:?} discovered via topic {topic} must carry bit 0b{expected_bit:b}, got 0b{seen:b}"
        );
    }
}
