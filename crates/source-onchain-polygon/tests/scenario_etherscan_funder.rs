//! Scenario: Etherscan funder discovery returns the union of incoming-USDC
//! senders for a seed set.
//!
//! PASS: `funders_of({wallet_a, wallet_b}, range)` returns exactly the
//!       four `0xfffffff*` funder addresses, with outgoing transfers and
//!       same-wallet transfers filtered out.
//! FAIL: missing funder, includes outgoing-transfer recipients, or panics.
//!
//! No network calls — fixture JSON files under `tests/fixtures/` are loaded
//! from disk by a `FixtureFetcher` that maps Etherscan URLs to fixture bodies.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::EtherscanFunderLookup;
use pe_source_onchain_polygon::etherscan::{FetchError, HttpFetcher};
use pe_source_onchain_polygon::funder_discovery::{BlockRange, FunderLookup};

const WALLET_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WALLET_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const USDC_NATIVE: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";
const USDC_BRIDGED: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read(&path).expect("fixture file missing")
}

/// HTTP fetcher backed by an in-memory map of URL substring → body.
/// Matches on URL substrings rather than full URLs so the test does not
/// have to reproduce the exact query-string ordering.
struct FixtureFetcher {
    rules: Vec<(String, Vec<u8>)>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FixtureFetcher {
    fn new(rules: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            rules,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a shared handle to the call log so tests can inspect it after
    /// the fetcher has been moved into an `EtherscanFunderLookup`.
    fn call_log(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.calls)
    }
}

impl HttpFetcher for FixtureFetcher {
    async fn fetch(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        self.calls.lock().unwrap().push(url.to_owned());
        for (substr, body) in &self.rules {
            if url.contains(substr) {
                return Ok(body.clone());
            }
        }
        Err(FetchError::Fatal(format!("no fixture matched url: {url}")))
    }
}

/// Scenario: EtherscanFunderLookup::current_block() decodes the hex response
/// from the Etherscan V2 proxy API into a `u64` block number.
///
/// PASS: block number == 5_054_680 (0x4d20d8) from fixture
/// FAIL: wrong number, error returned, or panic
#[tokio::test]
async fn etherscan_current_block_returns_decoded_height() {
    // Fixture contains: {"jsonrpc":"2.0","id":1,"result":"0x4d20d8"} — block 5_054_680
    let rules = vec![(
        "action=eth_blockNumber".to_owned(),
        fixture("etherscan_eth_block_number.json"),
    )];
    let fetcher = FixtureFetcher::new(rules);
    let lookup = EtherscanFunderLookup::with_fetcher(
        fetcher,
        "TESTKEY".to_owned(),
        "https://example.invalid/v2/api".to_owned(),
    );

    let block = lookup
        .current_block()
        .await
        .expect("current_block must succeed with fixture");
    assert_eq!(
        block, 5_054_680u64,
        "current_block must decode 0x4d20d8 to 5_054_680"
    );
}

#[tokio::test]
async fn etherscan_funders_of_returns_incoming_senders() {
    let wallet_a = WalletAddress::from_hex(WALLET_A).unwrap();
    let wallet_b = WalletAddress::from_hex(WALLET_B).unwrap();

    let rules = vec![
        (
            format!("contractaddress={USDC_NATIVE}&address={WALLET_A}"),
            fixture("etherscan_native_usdc_wallet_a.json"),
        ),
        (
            format!("contractaddress={USDC_BRIDGED}&address={WALLET_A}"),
            fixture("etherscan_bridged_usdc_wallet_a.json"),
        ),
        (
            format!("contractaddress={USDC_NATIVE}&address={WALLET_B}"),
            fixture("etherscan_native_usdc_wallet_b.json"),
        ),
        (
            format!("contractaddress={USDC_BRIDGED}&address={WALLET_B}"),
            fixture("etherscan_bridged_usdc_wallet_b.json"),
        ),
    ];
    let fetcher = FixtureFetcher::new(rules);

    let lookup = EtherscanFunderLookup::with_fetcher(
        fetcher,
        "TESTKEY".to_owned(),
        "https://example.invalid/v2/api".to_owned(),
    );

    let mut seeds: HashSet<WalletAddress> = HashSet::new();
    seeds.insert(wallet_a);
    seeds.insert(wallet_b);

    let range = BlockRange {
        from: 1,
        to: 100_000_000,
    };
    let funders = lookup.funders_of(&seeds, range).await.unwrap();

    let expected: HashSet<WalletAddress> = [
        "0xfffffffffffffffffffffffffffffffffffffff1",
        "0xfffffffffffffffffffffffffffffffffffffff2",
        "0xfffffffffffffffffffffffffffffffffffffff3",
        "0xfffffffffffffffffffffffffffffffffffffff4",
    ]
    .iter()
    .map(|h| WalletAddress::from_hex(h).unwrap())
    .collect();

    assert_eq!(
        funders, expected,
        "etherscan funders_of must return exactly the incoming-transfer senders"
    );
}

/// Scenario: N=4 concurrent calls to funders_of_with_timestamps produce the
/// same funder set as a sequential run on the same fixtures.
///
/// PASS: concurrent result equals sequential result (set equality)
/// FAIL: missing funders or extra entries in either direction
#[tokio::test]
async fn concurrent_funders_of_with_timestamps_matches_sequential() {
    let wallet_a = WalletAddress::from_hex(WALLET_A).unwrap();
    let wallet_b = WalletAddress::from_hex(WALLET_B).unwrap();

    let rules = vec![
        (
            format!("contractaddress={USDC_NATIVE}&address={WALLET_A}"),
            fixture("etherscan_native_usdc_wallet_a.json"),
        ),
        (
            format!("contractaddress={USDC_BRIDGED}&address={WALLET_A}"),
            fixture("etherscan_bridged_usdc_wallet_a.json"),
        ),
        (
            format!("contractaddress={USDC_NATIVE}&address={WALLET_B}"),
            fixture("etherscan_native_usdc_wallet_b.json"),
        ),
        (
            format!("contractaddress={USDC_BRIDGED}&address={WALLET_B}"),
            fixture("etherscan_bridged_usdc_wallet_b.json"),
        ),
    ];

    let range = BlockRange {
        from: 1,
        to: 100_000_000,
    };

    // Sequential reference run.
    let seq_lookup = EtherscanFunderLookup::with_fetcher(
        FixtureFetcher::new(rules.clone()),
        "TESTKEY".to_owned(),
        "https://example.invalid/v2/api".to_owned(),
    );
    let mut seq_result = std::collections::HashMap::new();
    for wallet in [wallet_a, wallet_b] {
        let wallet_set = std::iter::once(wallet).collect();
        let funders = seq_lookup
            .funders_of_with_timestamps(&wallet_set, range)
            .await
            .expect("sequential run must succeed");
        seq_result.extend(funders);
    }

    // Concurrent run (N=4, fixture fetcher is Arc-shared so concurrent access is safe).
    let lookup = Arc::new(EtherscanFunderLookup::with_fetcher(
        FixtureFetcher::new(rules),
        "TESTKEY".to_owned(),
        "https://example.invalid/v2/api".to_owned(),
    ));
    let wallets = [wallet_a, wallet_b];
    let handles: Vec<_> = wallets
        .iter()
        .map(|&wallet| {
            let lookup = Arc::clone(&lookup);
            tokio::spawn(async move {
                let wallet_set = std::iter::once(wallet).collect();
                lookup
                    .funders_of_with_timestamps(&wallet_set, range)
                    .await
                    .expect("concurrent task must succeed")
            })
        })
        .collect();

    let mut concurrent_result = std::collections::HashMap::new();
    for handle in handles {
        concurrent_result.extend(handle.await.expect("task must not panic"));
    }

    assert_eq!(
        seq_result.keys().collect::<std::collections::HashSet<_>>(),
        concurrent_result
            .keys()
            .collect::<std::collections::HashSet<_>>(),
        "concurrent and sequential runs must return the same funder set"
    );
}

// ── Helpers for pagination scenarios ─────────────────────────────────────────

/// Build a synthetic Etherscan `tokentx` response JSON with `count` entries.
///
/// `from` addresses run `start..start+count` (as `0x000...{n:040x}`).
/// Every entry carries `blockNumber = block_number` — needed by the block-range
/// cursor in `fetch_all_tokentx_pages` to advance `startblock` between batches.
/// Built in-process; no large committed fixture files.
fn synthetic_page(wallet: &str, start: usize, count: usize, block_number: u64) -> Vec<u8> {
    let entries: Vec<String> = (start..start + count)
        .map(|n| {
            format!(
                r#"{{"from":"0x{n:040x}","to":"{wallet}","timeStamp":"1700000000","blockNumber":"{block_number}"}}"#
            )
        })
        .collect();
    let body = entries.join(",");
    format!(r#"{{"status":"1","message":"OK","result":[{body}]}}"#).into_bytes()
}

/// Scenario: when the first batch returns a full page (10k entries) the cursor
/// advances via `blockNumber` and a second request fetches the remaining 500.
///
/// PASS: 10,500 unique funders, exactly 2 HTTP calls for USDC_NATIVE
/// FAIL: missing entries, wrong count, or wrong call count
#[tokio::test]
async fn two_page_fetch_returns_all_entries() {
    let wallet = WalletAddress::from_hex(WALLET_A).unwrap();
    let range = BlockRange {
        from: 1,
        to: 100_000_000,
    };

    // Batch 1: 10k entries, all at blockNumber=9999 → cursor advances to 9999.
    // Batch 2: triggered by startblock=9999; 500 entries at blockNumber=10499.
    // Non-overlapping `from` addresses → 10,500 unique funders after HashMap dedup.
    let batch1 = synthetic_page(WALLET_A, 0, 10_000, 9_999);
    let batch2 = synthetic_page(WALLET_A, 10_000, 500, 10_499);
    let bridged_empty: Vec<u8> =
        br#"{"status":"0","message":"No transactions found","result":[]}"#.to_vec();

    // The startblock=9999 rule must appear BEFORE the generic contractaddress rule
    // so FixtureFetcher matches it first on the second request.
    let rules = vec![
        (
            format!("contractaddress={USDC_NATIVE}&address={WALLET_A}&startblock=9999&"),
            batch2,
        ),
        (
            format!("contractaddress={USDC_NATIVE}&address={WALLET_A}"),
            batch1,
        ),
        (
            format!("contractaddress={USDC_BRIDGED}&address={WALLET_A}"),
            bridged_empty,
        ),
    ];

    let fetcher = FixtureFetcher::new(rules);
    let call_log = fetcher.call_log();
    let lookup = EtherscanFunderLookup::with_fetcher(
        fetcher,
        "TESTKEY".to_owned(),
        "https://example.invalid/v2/api".to_owned(),
    );

    let wallet_set = std::iter::once(wallet).collect();
    let funders = lookup
        .funders_of_with_timestamps(&wallet_set, range)
        .await
        .expect("two-page fetch must succeed");

    assert_eq!(
        funders.len(),
        10_500,
        "must return all 10,500 entries across both cursor iterations"
    );

    let calls = call_log.lock().unwrap();
    let native_calls: Vec<_> = calls
        .iter()
        .filter(|u| u.contains(&format!("contractaddress={USDC_NATIVE}")))
        .collect();
    assert_eq!(
        native_calls.len(),
        2,
        "must make exactly 2 HTTP calls for the USDC_NATIVE contract"
    );
}

/// Scenario: when every cursor iteration returns a full page (10k entries), the
/// loop terminates after MAX_PAGES iterations rather than running forever.
///
/// PASS: exactly 10 HTTP calls for USDC_NATIVE; 100,000 unique funders returned
/// FAIL: loop does not terminate, wrong call count, or wrong entry count
#[tokio::test]
async fn max_pages_exhaustion_terminates_loop() {
    let wallet = WalletAddress::from_hex(WALLET_A).unwrap();
    let range = BlockRange {
        from: 1,
        to: 100_000_000,
    };

    // Iteration k uses startblock = (k-1)*10_000 (1 for the first iteration).
    // Entries carry blockNumber = k*10_000 so the cursor advances to k*10_000 and
    // the next request has startblock=k*10_000. Unique `from` addresses give
    // 100,000 distinct funders after 10 iterations.
    //
    // Rule discriminator `startblock=S&` matches exactly: `startblock=1&` is not
    // a substring of `startblock=10000&` because `1` is followed by `&` vs `0`.
    let empty: Vec<u8> =
        br#"{"status":"0","message":"No transactions found","result":[]}"#.to_vec();
    let mut rules: Vec<(String, Vec<u8>)> = (1u32..=10)
        .map(|k| {
            let startblock = if k == 1 { 1 } else { (k as u64 - 1) * 10_000 };
            let block_number = k as u64 * 10_000;
            (
                format!(
                    "contractaddress={USDC_NATIVE}&address={WALLET_A}&startblock={startblock}&"
                ),
                synthetic_page(WALLET_A, (k as usize - 1) * 10_000, 10_000, block_number),
            )
        })
        .collect();
    rules.push((
        format!("contractaddress={USDC_BRIDGED}&address={WALLET_A}"),
        empty,
    ));

    let fetcher = FixtureFetcher::new(rules);
    let call_log = fetcher.call_log();
    let lookup = EtherscanFunderLookup::with_fetcher(
        fetcher,
        "TESTKEY".to_owned(),
        "https://example.invalid/v2/api".to_owned(),
    );

    let wallet_set = std::iter::once(wallet).collect();
    let funders = lookup
        .funders_of_with_timestamps(&wallet_set, range)
        .await
        .expect("max-pages exhaustion must return Ok (with warn)");

    let calls = call_log.lock().unwrap();
    let native_calls: Vec<_> = calls
        .iter()
        .filter(|u| u.contains(&format!("contractaddress={USDC_NATIVE}")))
        .collect();
    assert_eq!(
        native_calls.len(),
        10,
        "must stop after exactly MAX_PAGES=10 HTTP calls for USDC_NATIVE"
    );
    assert_eq!(
        funders.len(),
        100_000,
        "must return 10 cursor iterations × 10,000 entries = 100,000 funders"
    );
}
