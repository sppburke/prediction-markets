//! Scenario: bulk funder discovery via batched `eth_getLogs` (issue #203).
//!
//! Drives `funder::run_funder_eth_logs` end-to-end against an in-memory
//! [`InMemoryChainLogFetcher`] (no RPC) and a real on-disk `WalletCache`.
//!
//! PASS: the discovered edge (funder → funded, with its block timestamp) is
//!       persisted, AND every pending wallet — including one with zero
//!       discovered funders — is marked done so it is not re-queried.
//! FAIL: the edge is missing / mis-timestamped, OR the zero-funder wallet
//!       remains pending (the regression this scenario guards).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use alloy::primitives::{Address, Bytes, LogData};
use alloy::rpc::types::Log;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::funder;
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::{TOPIC_ERC20_TRANSFER, USDC};
use pe_source_onchain_polygon::eth_logs::test_support::InMemoryChainLogFetcher;
use pe_source_onchain_polygon::{BlockRange, wallet_to_topic};
use tempfile::TempDir;

const FUNDED_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ORPHAN_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FUNDER_F: &str = "0x1111111111111111111111111111111111111111";
const EVENT_TS: u64 = 1_700_000_000;

fn addr(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

/// Mark a wallet as an active tradeable candidate (issue #201 view scope), so
/// `wallets_needing_funder_lookup` can observe whether it was marked done.
fn activate(cache: &mut WalletCache, wallet: WalletAddress) {
    cache
        .upsert_wallets_bulk(&[(wallet.to_string(), 0, false, None, None, None, 0)])
        .unwrap();
    cache.conn_for_test_set_active(&wallet.to_string(), 1);
}

/// ERC-20 `Transfer(from=funder, to=funded, _)` log: topic[1]=funder,
/// topic[2]=funded, plus a populated block timestamp (as Alchemy provides).
fn transfer_log(funder: WalletAddress, funded: WalletAddress, ts: u64, contract: Address) -> Log {
    let inner = alloy::primitives::Log {
        address: contract,
        data: LogData::new_unchecked(
            vec![
                TOPIC_ERC20_TRANSFER,
                wallet_to_topic(&funder),
                wallet_to_topic(&funded),
            ],
            Bytes::new(),
        ),
    };
    Log {
        inner,
        block_timestamp: Some(ts),
        ..Default::default()
    }
}

#[tokio::test]
async fn eth_logs_funder_persists_edges_and_marks_zero_funder_done() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    let funded = addr(FUNDED_A);
    let orphan = addr(ORPHAN_B);
    let funder_f = addr(FUNDER_F);
    activate(&mut cache, funded);
    activate(&mut cache, orphan);

    // The fetcher returns one funding transfer to FUNDED_A; ORPHAN_B has none.
    let fetcher =
        InMemoryChainLogFetcher::ok(100, vec![transfer_log(funder_f, funded, EVENT_TS, USDC)]);

    // One batch (topic_batch_size > 2) and one chunk (chunk_blocks > span).
    let pending = vec![funded, orphan];
    let range = BlockRange { from: 0, to: 100 };
    let report = funder::run_funder_eth_logs(&mut cache, &pending, range, &fetcher, 1_000, 1_000)
        .await
        .unwrap();

    assert_eq!(report.pending, 2);
    assert_eq!(report.processed, 2);
    assert_eq!(report.failed, 0);

    // The discovered edge is persisted with its on-chain block timestamp.
    let edges = cache.load_funder_edges_with_timestamp().unwrap();
    assert_eq!(
        edges,
        vec![(funder_f, funded, i64::try_from(EVENT_TS).unwrap())]
    );

    // BOTH wallets are marked done — the zero-funder ORPHAN_B must NOT reappear
    // in the pending set on a subsequent run.
    let still_pending = cache.wallets_needing_funder_lookup(0).unwrap();
    assert!(
        still_pending.is_empty(),
        "every pending wallet must be marked done, incl. the zero-funder wallet; got {still_pending:?}"
    );
}
