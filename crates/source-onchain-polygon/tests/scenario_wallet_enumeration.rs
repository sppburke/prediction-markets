//! Scenario: wallet enumeration via Etherscan `eth_getLogs` on three paginated responses.
//!
//! PASS: `PolymarketTraderEnumeration::enumerate()` across 3 fixture responses
//!       (one per paginated call chain) returns a deduped set of exactly
//!       the maker and taker addresses present in the fixture logs.
//!
//! FAIL: operator addresses appear in the output set, OR the returned count
//!       differs from the expected unique wallet count.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::{ALL_EXCHANGE_CONTRACTS, TOPIC_ORDER_FILLED};
use pe_source_onchain_polygon::etherscan::FetchError;
use pe_source_onchain_polygon::etherscan::HttpFetcher;
use pe_source_onchain_polygon::wallet_enumeration::{
    EnumerationConfig, PolymarketTraderEnumeration,
};

// ── Fixture fetcher ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct FixtureFetcher {
    responses: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl FixtureFetcher {
    fn new(responses: HashMap<String, Vec<u8>>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses)),
        }
    }
}

impl HttpFetcher for FixtureFetcher {
    async fn fetch(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        self.responses
            .lock()
            .unwrap()
            .get(url)
            .cloned()
            .ok_or_else(|| FetchError::Fatal(format!("no fixture for: {url}")))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn w(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn topic_hex(addr: WalletAddress) -> String {
    let mut hex = String::from("0x");
    hex.push_str(&"0".repeat(24));
    for byte in addr.0.iter() {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

fn log_entry_json(maker: WalletAddress, taker: WalletAddress) -> String {
    format!(
        r#"{{"topics":["0x{topic0}","0x{zero}","{maker}","{taker}"]}}"#,
        topic0 = TOPIC_ORDER_FILLED,
        zero = "0".repeat(64),
        maker = topic_hex(maker),
        taker = topic_hex(taker),
    )
}

fn ok_response(entries: &[String]) -> Vec<u8> {
    format!(
        r#"{{"status":"1","message":"OK","result":[{}]}}"#,
        entries.join(",")
    )
    .into_bytes()
}

fn empty_response() -> Vec<u8> {
    br#"{"status":"0","message":"No records found","result":[]}"#.to_vec()
}

// ── Scenario ──────────────────────────────────────────────────────────────────

/// Three contracts each return one paginated response; one wallet appears in
/// multiple contracts (deduplicated). Operator address excluded.
#[tokio::test]
async fn three_paginated_responses_produce_correct_wallet_set() {
    // Fixture wallets:
    //   contract_v1: maker=0x01, taker=0x02 (one log)
    //   contract_neg_risk: maker=0x01 (duplicate), taker=0x03 (one log)
    //   contract_v2/neg_risk_v2: empty
    //   operator: 0xff — should be excluded
    let w01 = w(0x01);
    let w02 = w(0x02);
    let w03 = w(0x03);
    let op = w(0xff);

    let from_block: u64 = 100;
    let to_block: u64 = 200;
    let api_key = "TESTKEY";
    let base = "http://scenario.local";

    let build_url = |addr: &str| {
        format!(
            "{base}?chainid=137&module=logs&action=getLogs\
             &address={addr}&topic0=0x{topic0}\
             &fromBlock={from_block}&toBlock={to_block}\
             &offset=1000&page=1&apikey={api_key}",
            topic0 = TOPIC_ORDER_FILLED,
        )
    };

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();

    // CTFExchange V1 → 1 log: maker=w01, taker=w02
    let v1_url = build_url(&format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[0]));
    responses.insert(v1_url, ok_response(&[log_entry_json(w01, w02)]));

    // NegRiskCtfExchange V1 → 1 log: maker=w01, taker=w03 (w01 is a duplicate)
    let neg_v1_url = build_url(&format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[1]));
    responses.insert(neg_v1_url, ok_response(&[log_entry_json(w01, w03)]));

    // CTFExchange V2 → empty
    let v2_url = build_url(&format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[2]));
    responses.insert(v2_url, empty_response());

    // NegRiskCtfExchange V2 → 1 log with operator as taker (must be filtered)
    let neg_v2_url = build_url(&format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[3]));
    responses.insert(neg_v2_url, ok_response(&[log_entry_json(w02, op)]));

    let config = EnumerationConfig {
        from_block,
        to_block,
        operator_addresses: vec![op],
    };

    let fetcher = FixtureFetcher::new(responses);
    let enumerator = PolymarketTraderEnumeration::with_fetcher(
        fetcher,
        api_key.to_owned(),
        config,
        base.to_owned(),
    );

    let wallets = enumerator.enumerate().await.unwrap();

    // Expected: {w01, w02, w03} — operator excluded, w01 deduplicated.
    let expected: HashSet<WalletAddress> = [w01, w02, w03].into_iter().collect();
    assert_eq!(
        wallets, expected,
        "wallet set mismatch: got {wallets:?}, expected {expected:?}"
    );

    // Operator must not be in the result.
    assert!(
        !wallets.contains(&op),
        "operator address must be excluded from wallet set"
    );
}
