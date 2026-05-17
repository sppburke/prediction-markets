//! Scenario: wallet enumeration via Etherscan `eth_getLogs` across both
//! `OrderFilled` topic versions (V1 + V2) on all four exchange contracts.
//!
//! PASS: `PolymarketTraderEnumeration::enumerate()` issues one Etherscan call
//!       per (contract, topic) pair (8 total) and unions the resulting wallet
//!       sets. Operator addresses are excluded.
//!
//! FAIL: operator addresses appear in the output set, OR V2-topic wallets
//!       are silently dropped (the issue #179 production bug), OR the
//!       returned count differs from the expected unique wallet count.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use alloy::primitives::B256;
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
};
use pe_source_onchain_polygon::etherscan::FetchError;
use pe_source_onchain_polygon::etherscan::HttpFetcher;
use pe_source_onchain_polygon::wallet_enumeration::{
    EnumerationConfig, PolymarketTraderEnumeration,
};

// ── Fixture fetcher ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct FixtureFetcher {
    responses: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FixtureFetcher {
    fn new(responses: HashMap<String, Vec<u8>>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses)),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn call_log(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl HttpFetcher for FixtureFetcher {
    async fn fetch(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        self.calls.lock().unwrap().push(url.to_owned());
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

fn log_entry_json(topic0: B256, maker: WalletAddress, taker: WalletAddress) -> String {
    format!(
        r#"{{"topics":["{topic0}","0x{zero}","{maker}","{taker}"]}}"#,
        topic0 = topic0, // B256 Display already includes "0x"
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

/// `enumerate()` must scan every (contract, topic) pair in
/// `ALL_EXCHANGE_CONTRACTS × ALL_ORDER_FILLED_TOPICS` — 4 × 2 = 8 calls — and
/// union the resulting wallet sets.
///
/// Issue #179 regression coverage: a V2-topic-only wallet must surface in
/// the output (pre-fix it was silently dropped).
#[tokio::test]
async fn enumerate_unions_v1_and_v2_topic_wallets() {
    // V1 contracts emit V1-topic logs only; V2 contracts emit V2-topic logs
    // only. Issue #179's bug: pre-fix the enumerator only filtered V1 topic,
    // so V2-contract wallets were invisible.
    let v1_only = w(0x01); // appears via CTF_EXCHANGE_V1 / V1-topic
    let neg_v1_only = w(0x02); // appears via NEG_RISK_CTF_EXCHANGE_V1 / V1-topic
    let v2_only = w(0x03); // appears via CTF_EXCHANGE_V2 / V2-topic
    let neg_v2_only = w(0x04); // appears via NEG_RISK_CTF_EXCHANGE_V2 / V2-topic
    let shared = w(0x05); // appears in BOTH V1 and V2 contracts → dedup test
    let op = w(0xff);

    let from_block: u64 = 100;
    let to_block: u64 = 200;
    let api_key = "TESTKEY";
    let base = "http://scenario.local";

    let build_url = |contract_addr: alloy::primitives::Address, topic: B256| {
        format!(
            "{base}?chainid=137&module=logs&action=getLogs\
             &address=0x{contract_addr:x}&topic0={topic0}\
             &fromBlock={from_block}&toBlock={to_block}\
             &offset=1000&page=1&apikey={api_key}",
            topic0 = topic, // B256 Display already includes "0x"
        )
    };

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();

    // Each contract is reachable via both topics. Default every
    // (contract, topic) pair to empty, then override the ones with logs.
    for contract in ALL_EXCHANGE_CONTRACTS {
        for topic in ALL_ORDER_FILLED_TOPICS {
            responses.insert(build_url(contract, topic), empty_response());
        }
    }

    // CTF_EXCHANGE_V1 / V1-topic → v1_only + shared (taker = operator filtered)
    responses.insert(
        build_url(ALL_EXCHANGE_CONTRACTS[0], TOPIC_ORDER_FILLED_V1),
        ok_response(&[
            log_entry_json(TOPIC_ORDER_FILLED_V1, v1_only, shared),
            log_entry_json(TOPIC_ORDER_FILLED_V1, v1_only, op),
        ]),
    );
    // NEG_RISK_CTF_EXCHANGE_V1 / V1-topic → neg_v1_only
    responses.insert(
        build_url(ALL_EXCHANGE_CONTRACTS[1], TOPIC_ORDER_FILLED_V1),
        ok_response(&[log_entry_json(
            TOPIC_ORDER_FILLED_V1,
            neg_v1_only,
            neg_v1_only,
        )]),
    );
    // CTF_EXCHANGE_V2 / V2-topic → v2_only + shared (the dedup target)
    responses.insert(
        build_url(ALL_EXCHANGE_CONTRACTS[2], TOPIC_ORDER_FILLED_V2),
        ok_response(&[log_entry_json(TOPIC_ORDER_FILLED_V2, v2_only, shared)]),
    );
    // NEG_RISK_CTF_EXCHANGE_V2 / V2-topic → neg_v2_only
    responses.insert(
        build_url(ALL_EXCHANGE_CONTRACTS[3], TOPIC_ORDER_FILLED_V2),
        ok_response(&[log_entry_json(
            TOPIC_ORDER_FILLED_V2,
            neg_v2_only,
            neg_v2_only,
        )]),
    );

    let config = EnumerationConfig {
        from_block,
        to_block,
        operator_addresses: vec![op],
    };

    let fetcher = FixtureFetcher::new(responses);
    let enumerator = PolymarketTraderEnumeration::with_fetcher(
        fetcher.clone(),
        api_key.to_owned(),
        config,
        base.to_owned(),
    );

    let wallets = enumerator.enumerate().await.unwrap();

    // Expected union: {v1_only, neg_v1_only, v2_only, neg_v2_only, shared}.
    // Operator excluded; `shared` deduplicated despite appearing in both
    // V1 (CTF_EXCHANGE_V1) and V2 (CTF_EXCHANGE_V2).
    let expected: HashSet<WalletAddress> =
        [v1_only, neg_v1_only, v2_only, neg_v2_only, shared].into();
    assert_eq!(
        wallets, expected,
        "wallet set mismatch: got {wallets:?}, expected {expected:?}"
    );
    assert!(
        !wallets.contains(&op),
        "operator address must be excluded from wallet set"
    );

    // Verify the enumerator actually called every (contract, topic) pair —
    // missing any of the 8 URLs means a V1- or V2-only wallet would be lost.
    let calls = fetcher.call_log();
    for contract in ALL_EXCHANGE_CONTRACTS {
        for topic in ALL_ORDER_FILLED_TOPICS {
            let expected_url = build_url(contract, topic);
            assert!(
                calls.contains(&expected_url),
                "expected call for contract 0x{contract:x} topic {topic} not made"
            );
        }
    }
    assert_eq!(
        calls.len(),
        8,
        "must issue exactly 4 contracts × 2 topics = 8 Etherscan calls; got {}",
        calls.len()
    );
}
