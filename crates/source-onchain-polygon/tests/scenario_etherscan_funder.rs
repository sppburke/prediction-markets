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
use std::sync::Mutex;

use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::EtherscanFunderLookup;
use pe_source_onchain_polygon::etherscan::HttpFetcher;
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
    calls: Mutex<Vec<String>>,
}

impl FixtureFetcher {
    fn new(rules: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            rules,
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl HttpFetcher for FixtureFetcher {
    async fn fetch(&self, url: &str) -> Result<Vec<u8>, String> {
        self.calls.lock().unwrap().push(url.to_owned());
        for (substr, body) in &self.rules {
            if url.contains(substr) {
                return Ok(body.clone());
            }
        }
        Err(format!("no fixture matched url: {url}"))
    }
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
