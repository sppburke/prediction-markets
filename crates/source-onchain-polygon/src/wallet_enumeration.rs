//! Polymarket wallet enumeration via Etherscan `eth_getLogs` on Polygon (chain 137).
//!
//! Scans `OrderFilled` events across all four Polymarket exchange contracts
//! (CTFExchange V1/V2 and NegRiskCtfExchange V1/V2) to build the full set of
//! distinct wallets that have ever traded on Polymarket. Both `maker` (topic2)
//! and `taker` (topic3) are indexed and extracted directly from log topics —
//! no ABI decoding of the data payload is required.
//!
//! ## Pagination — bisect on cap
//!
//! Etherscan caps `eth_getLogs` responses at [`LOGS_PAGE_CAP`] entries. If a
//! block range returns exactly that many logs, we recursively bisect the range
//! until every sub-range fits below the cap. Any range that cannot be bisected
//! further (i.e. `from == to`) emits a warning — those logs are retained and
//! counted, but we cannot guarantee completeness for that single block.
//!
//! ## Operator filter
//!
//! Polymarket's matching operator typically appears as `taker` on
//! maker-vs-operator legs. These addresses are configurable via
//! [`EnumerationConfig::operator_addresses`] and filtered from the output set.

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use pe_core_types::WalletAddress;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, CTF_EXCHANGE_V1_DEPLOY_BLOCK,
};
use crate::etherscan::{FetchError, HttpFetcher};
use crate::funder_discovery::FunderDiscoveryError;

const ETHERSCAN_BASE_URL: &str = "https://api.etherscan.io/v2/api";
const POLYGON_CHAIN_ID: u32 = 137;

/// Etherscan free-tier hard cap on `eth_getLogs` results per call.
/// Canonical value in `docs/_GLOSSARY.md` "Wallet enumeration defaults".
const LOGS_PAGE_CAP: usize = 1_000;

/// Rate-limit delay between Etherscan calls (200 ms = 5 req/s budget).
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const RATE_LIMIT_DELAY_MS: u64 = 200;

/// Maximum block span for a single top-level Etherscan request before the
/// bisection recurses. Keeping this small avoids "server too busy" errors
/// that Etherscan returns when asked to scan very large ranges in one shot.
/// Canonical value in `docs/_GLOSSARY.md` "Wallet enumeration defaults".
const SCAN_CHUNK_BLOCKS: u64 = 500_000;

/// Maximum retry backoff when Etherscan returns transient errors.
const MAX_BACKOFF_SECS: u64 = 60;

/// Maximum retry attempts before propagating the error to the caller.
const MAX_ATTEMPTS: u32 = 6;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for [`PolymarketTraderEnumeration`].
#[derive(Debug, Clone)]
pub struct EnumerationConfig {
    /// Start block for the scan. Defaults to [`CTF_EXCHANGE_V1_DEPLOY_BLOCK`].
    pub from_block: u64,
    /// End block for the scan (inclusive). Callers should supply the current
    /// chain head; use [`crate::etherscan::EtherscanFunderLookup::current_block`].
    pub to_block: u64,
    /// Addresses to exclude from the result set (e.g. Polymarket matching operators).
    /// Configurable via `PE_POLYMARKET_OPERATOR_ADDRESSES` (comma-separated hex).
    pub operator_addresses: Vec<WalletAddress>,
}

impl EnumerationConfig {
    /// Construct with default `from_block` and the given `to_block`.
    pub fn new(to_block: u64) -> Self {
        Self {
            from_block: CTF_EXCHANGE_V1_DEPLOY_BLOCK,
            to_block,
            operator_addresses: Vec::new(),
        }
    }
}

// ── Error ─────────────────────────────────────────────────────────────────────

/// Errors produced by wallet enumeration.
#[derive(Debug, thiserror::Error)]
pub enum EnumerationError {
    #[error("etherscan eth_getLogs: {0}")]
    Etherscan(String),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

impl From<FunderDiscoveryError> for EnumerationError {
    fn from(e: FunderDiscoveryError) -> Self {
        EnumerationError::Etherscan(e.to_string())
    }
}

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

/// Top-level Etherscan response envelope for `eth_getLogs`.
#[derive(Deserialize)]
struct EtherscanLogsResponse {
    status: String,
    #[serde(default)]
    message: String,
    result: serde_json::Value,
}

/// A single `eth_getLogs` result entry.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogEntry {
    /// topic0, topic1, topic2, topic3 as hex strings.
    topics: Vec<String>,
}

// ── Main struct ───────────────────────────────────────────────────────────────

/// Enumerates every distinct Polymarket wallet by scanning `OrderFilled` events
/// via Etherscan V2 `eth_getLogs`.
pub struct PolymarketTraderEnumeration<F: HttpFetcher> {
    api_key: String,
    fetcher: F,
    config: EnumerationConfig,
    base_url: String,
}

impl PolymarketTraderEnumeration<reqwest::Client> {
    /// Construct with a default `reqwest::Client` and the production Etherscan base URL.
    pub fn new(api_key: String, config: EnumerationConfig) -> Self {
        Self {
            api_key,
            fetcher: reqwest::Client::new(),
            config,
            base_url: ETHERSCAN_BASE_URL.to_owned(),
        }
    }
}

impl<F: HttpFetcher> PolymarketTraderEnumeration<F> {
    /// Construct with a custom fetcher and base URL — used by tests.
    pub fn with_fetcher(
        fetcher: F,
        api_key: String,
        config: EnumerationConfig,
        base_url: String,
    ) -> Self {
        Self {
            api_key,
            fetcher,
            config,
            base_url,
        }
    }

    /// Enumerate every distinct trader wallet across all Polymarket exchange contracts.
    ///
    /// Returns a deduplicated `HashSet<WalletAddress>` with operator addresses
    /// removed. Scans both maker (topic2) and taker (topic3) from `OrderFilled` events
    /// for every topic in [`ALL_ORDER_FILLED_TOPICS`] (V1 and V2).
    pub async fn enumerate(&self) -> Result<HashSet<WalletAddress>, EnumerationError> {
        if self.config.from_block > self.config.to_block {
            return Err(EnumerationError::InvalidConfig(format!(
                "from_block {} > to_block {}",
                self.config.from_block, self.config.to_block
            )));
        }
        let mut wallets = HashSet::new();
        for contract in &ALL_EXCHANGE_CONTRACTS {
            wallets.extend(self.enumerate_one_contract(*contract).await?);
        }
        Ok(wallets)
    }

    /// Enumerate every distinct trader wallet for a single exchange `contract`,
    /// scanning every topic in [`ALL_ORDER_FILLED_TOPICS`] (V1 and V2) and
    /// returning the unioned wallet set.
    ///
    /// Thin wrapper over [`Self::enumerate_one_contract_for_topic`]. Issued as
    /// one Etherscan REST call per topic per contract — the free-tier endpoint
    /// rejects duplicate `topic0` query params with `"Invalid topic0 length"`,
    /// so a single union call is not possible at the REST layer.
    ///
    /// # Precondition
    /// `config.from_block <= config.to_block` — callers must validate the
    /// range before calling (e.g. via [`Self::enumerate`] which checks up front).
    pub async fn enumerate_one_contract(
        &self,
        contract: Address,
    ) -> Result<HashSet<WalletAddress>, EnumerationError> {
        let mut wallets: HashSet<WalletAddress> = HashSet::new();
        for topic in &ALL_ORDER_FILLED_TOPICS {
            wallets.extend(
                self.enumerate_one_contract_for_topic(contract, *topic)
                    .await?,
            );
        }
        Ok(wallets)
    }

    /// Enumerate every distinct trader wallet for a single exchange `contract`
    /// filtered to a single `topic0`. The unit primitive used by the bootstrap
    /// per-(topic, contract) loop and by [`Self::enumerate_one_contract`].
    ///
    /// Scans `[from_block, to_block]` in `SCAN_CHUNK_BLOCKS`-sized windows,
    /// bisecting on page-cap responses. Operator addresses are excluded.
    /// Returns a deduplicated `HashSet<WalletAddress>`.
    ///
    /// # Precondition
    /// `config.from_block <= config.to_block` — callers must validate the
    /// range before calling.
    pub async fn enumerate_one_contract_for_topic(
        &self,
        contract: Address,
        topic0: B256,
    ) -> Result<HashSet<WalletAddress>, EnumerationError> {
        let operator_set: HashSet<WalletAddress> =
            self.config.operator_addresses.iter().copied().collect();
        let mut wallets: HashSet<WalletAddress> = HashSet::new();
        // Each contract address is queried separately; Etherscan's `address` param
        // is a single address (not an array) for the free-tier `eth_getLogs` endpoint.
        // Pre-chunk the full range into SCAN_CHUNK_BLOCKS windows so that the initial
        // request per chunk is small enough for Etherscan to handle without timing out.
        let contract_hex = format!("0x{contract:x}");
        let topic0_hex = format!("{topic0}");
        let mut chunk_from = self.config.from_block;
        while chunk_from <= self.config.to_block {
            let chunk_to = (chunk_from + SCAN_CHUNK_BLOCKS - 1).min(self.config.to_block);
            tracing::info!(
                contract = %contract_hex,
                topic0 = %topic0_hex,
                chunk_from,
                chunk_to,
                "wallet enumeration: scanning chunk"
            );
            self.scan_range(
                &contract_hex,
                &topic0_hex,
                chunk_from,
                chunk_to,
                &operator_set,
                &mut wallets,
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
            chunk_from = chunk_to + 1;
        }
        Ok(wallets)
    }

    /// Recursively scan `[from, to]` for `OrderFilled` logs of `topic0_hex`,
    /// bisecting if the response hits the page cap.
    fn scan_range<'a>(
        &'a self,
        contract_hex: &'a str,
        topic0_hex: &'a str,
        from: u64,
        to: u64,
        operator_set: &'a HashSet<WalletAddress>,
        wallets: &'a mut HashSet<WalletAddress>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), EnumerationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let url = self.build_url(contract_hex, topic0_hex, from, to);
            let logs = self.fetch_logs_with_backoff(&url).await?;
            let count = logs.len();

            if count >= LOGS_PAGE_CAP {
                if from == to {
                    // Single-block cap: can't bisect further. Retain the logs we have
                    // and warn — this block is unusually dense but we capture what Etherscan
                    // returns.
                    warn!(
                        contract = %contract_hex,
                        topic0 = %topic0_hex,
                        block = from,
                        count,
                        cap = LOGS_PAGE_CAP,
                        "wallet enumeration: single-block log cap hit; results may be incomplete"
                    );
                    extract_wallets(&logs, operator_set, wallets);
                    return Ok(());
                }
                // Bisect and recurse. Sleep before each sub-fetch to honour the
                // 5 req/s budget — the triggering fetch above already consumed one slot.
                let mid = from + (to - from) / 2;
                tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
                self.scan_range(contract_hex, topic0_hex, from, mid, operator_set, wallets)
                    .await?;
                tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
                self.scan_range(contract_hex, topic0_hex, mid + 1, to, operator_set, wallets)
                    .await?;
            } else {
                extract_wallets(&logs, operator_set, wallets);
            }
            Ok(())
        })
    }

    fn build_url(&self, contract_hex: &str, topic0_hex: &str, from: u64, to: u64) -> String {
        format!(
            "{base}?chainid={chain}&module=logs&action=getLogs\
             &address={contract}&topic0={topic0}\
             &fromBlock={from}&toBlock={to}\
             &offset={cap}&page=1&apikey={key}",
            base = self.base_url,
            chain = POLYGON_CHAIN_ID,
            contract = contract_hex,
            topic0 = topic0_hex,
            from = from,
            to = to,
            cap = LOGS_PAGE_CAP,
            key = self.api_key,
        )
    }

    async fn fetch_logs_with_backoff(&self, url: &str) -> Result<Vec<LogEntry>, EnumerationError> {
        let mut backoff_secs: u64 = 1;
        let mut last_err: Option<String> = None;

        for attempt in 1..=MAX_ATTEMPTS {
            let outcome = match self.fetcher.fetch(url).await {
                Ok(bytes) => parse_logs_response(&bytes),
                Err(FetchError::Fatal(m)) => {
                    return Err(EnumerationError::Etherscan(format!("fatal: {m}")));
                }
                Err(FetchError::Transient(m)) => Err(ParseOutcome::Transient(m)),
            };

            match outcome {
                Ok(logs) => return Ok(logs),
                Err(ParseOutcome::Fatal(m)) => {
                    return Err(EnumerationError::Etherscan(format!("fatal: {m}")));
                }
                Err(ParseOutcome::Transient(m)) => {
                    warn!(
                        attempt,
                        max = MAX_ATTEMPTS,
                        error = %m,
                        "wallet enumeration: transient error, retrying"
                    );
                    last_err = Some(m);
                    if attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    }
                }
            }
        }

        Err(EnumerationError::Etherscan(format!(
            "exhausted {MAX_ATTEMPTS} attempts: {}",
            last_err.unwrap_or_else(|| "no error captured".to_owned())
        )))
    }
}

// ── Parser helpers ────────────────────────────────────────────────────────────

#[derive(Debug)]
enum ParseOutcome {
    Transient(String),
    Fatal(String),
}

fn parse_logs_response(bytes: &[u8]) -> Result<Vec<LogEntry>, ParseOutcome> {
    let resp: EtherscanLogsResponse = serde_json::from_slice(bytes)
        .map_err(|e| ParseOutcome::Fatal(format!("decode envelope: {e}")))?;

    match resp.status.as_str() {
        "1" => serde_json::from_value::<Vec<LogEntry>>(resp.result)
            .map_err(|e| ParseOutcome::Fatal(format!("decode result array: {e}"))),
        "0" if resp.message.eq_ignore_ascii_case("No records found") => Ok(Vec::new()),
        "0" if resp.message.eq_ignore_ascii_case("No logs found") => Ok(Vec::new()),
        "0" => {
            let body = resp.result.to_string();
            let msg_lc = resp.message.to_lowercase();
            if resp.message.to_uppercase().contains("NOTOK")
                || body.to_lowercase().contains("rate limit")
                || msg_lc.contains("too busy")
                || msg_lc.contains("timeout")
                || msg_lc.contains("try again")
                || msg_lc.contains("server error")
            {
                Err(ParseOutcome::Transient(format!(
                    "API status=0 message={} body={body}",
                    resp.message
                )))
            } else {
                Err(ParseOutcome::Fatal(format!(
                    "API status=0 message={}: {body}",
                    resp.message
                )))
            }
        }
        other => Err(ParseOutcome::Fatal(format!(
            "API status={other} message={}",
            resp.message
        ))),
    }
}

/// Extract maker (topic2) and taker (topic3) from each log, decode as
/// `WalletAddress`, and insert into `wallets` — skipping operator addresses.
fn extract_wallets(
    logs: &[LogEntry],
    operator_set: &HashSet<WalletAddress>,
    wallets: &mut HashSet<WalletAddress>,
) {
    for log in logs {
        // topic0 = event sig, topic1 = orderHash, topic2 = maker, topic3 = taker
        for topic_idx in [2usize, 3usize] {
            let Some(topic_hex) = log.topics.get(topic_idx) else {
                continue;
            };
            // Topics are 32-byte hex; lower 20 bytes = address.
            let hex = topic_hex.trim_start_matches("0x");
            if hex.len() < 40 {
                continue;
            }
            let addr_hex = format!("0x{}", &hex[hex.len() - 40..]);
            match WalletAddress::from_hex(&addr_hex) {
                Ok(addr) if !operator_set.contains(&addr) => {
                    wallets.insert(addr);
                }
                Ok(_) => {}
                Err(e) => {
                    debug!(topic = %topic_hex, error = %e, "wallet enumeration: skipping unparseable address");
                }
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::contracts::{
        CTF_EXCHANGE_V1, CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V1, NEG_RISK_CTF_EXCHANGE_V2,
        TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
    };

    use super::*;

    fn w(b: u8) -> WalletAddress {
        let mut bytes = [0u8; 20];
        bytes[19] = b;
        WalletAddress(bytes)
    }

    /// Minimal 32-byte topic hex for a wallet address (last 20 bytes = address).
    fn topic_hex(addr: WalletAddress) -> String {
        let mut hex = String::from("0x");
        hex.push_str(&"0".repeat(24)); // 12 zero bytes padding
        for byte in addr.0.iter() {
            hex.push_str(&format!("{byte:02x}"));
        }
        hex
    }

    fn make_log(topic0: B256, maker: WalletAddress, taker: WalletAddress) -> LogEntry {
        LogEntry {
            topics: vec![
                format!("{topic0}"),
                "0x".to_owned() + &"0".repeat(64), // orderHash
                topic_hex(maker),
                topic_hex(taker),
            ],
        }
    }

    #[test]
    fn parse_valid_logs_response() {
        let json = format!(
            r#"{{"status":"1","message":"OK","result":[{{"topics":["0x{}","0x{}","0x{}","0x{}"]}}]}}"#,
            "0".repeat(64),
            "0".repeat(64),
            "0".repeat(64),
            "0".repeat(64)
        );
        let logs = parse_logs_response(json.as_bytes()).unwrap();
        assert_eq!(logs.len(), 1);
    }

    #[test]
    fn parse_no_records_returns_empty() {
        let json = br#"{"status":"0","message":"No records found","result":[]}"#;
        let logs = parse_logs_response(json).unwrap();
        assert!(logs.is_empty());
    }

    #[test]
    fn parse_no_logs_found_returns_empty() {
        let json = br#"{"status":"0","message":"No logs found","result":[]}"#;
        let logs = parse_logs_response(json).unwrap();
        assert!(logs.is_empty());
    }

    #[test]
    fn parse_rate_limit_is_transient() {
        let json = br#"{"status":"0","message":"NOTOK","result":"Max rate limit reached"}"#;
        let result = parse_logs_response(json);
        assert!(matches!(result, Err(ParseOutcome::Transient(_))));
    }

    #[test]
    fn parse_malformed_json_is_fatal() {
        let result = parse_logs_response(b"not json");
        assert!(matches!(result, Err(ParseOutcome::Fatal(_))));
    }

    #[test]
    fn extract_wallets_collects_maker_and_taker() {
        let maker = w(0xaa);
        let taker = w(0xbb);
        let logs = vec![make_log(TOPIC_ORDER_FILLED_V1, maker, taker)];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert!(wallets.contains(&maker));
        assert!(wallets.contains(&taker));
    }

    #[test]
    fn extract_wallets_filters_operators() {
        let maker = w(0xaa);
        let operator = w(0xcc);
        let logs = vec![make_log(TOPIC_ORDER_FILLED_V1, maker, operator)];
        let mut operators = HashSet::new();
        operators.insert(operator);
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert!(wallets.contains(&maker));
        assert!(!wallets.contains(&operator));
    }

    #[test]
    fn extract_wallets_deduplicates() {
        let maker = w(0xaa);
        let taker = w(0xbb);
        let logs = vec![
            make_log(TOPIC_ORDER_FILLED_V1, maker, taker),
            make_log(TOPIC_ORDER_FILLED_V1, maker, taker),
        ];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert_eq!(wallets.len(), 2);
    }

    /// Regression test for issue #179: the wallet-extractor must accept a
    /// V2-topic log without dropping the maker/taker. Topic[0] is metadata only
    /// — `extract_wallets` reads topic[2]/topic[3] regardless of version.
    #[test]
    fn extract_wallets_captures_v2_topic_log() {
        let maker = w(0xab);
        let taker = w(0xcd);
        let logs = vec![make_log(TOPIC_ORDER_FILLED_V2, maker, taker)];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert!(wallets.contains(&maker), "V2-topic maker must be captured");
        assert!(wallets.contains(&taker), "V2-topic taker must be captured");
    }

    #[test]
    fn extract_wallets_skips_short_topics() {
        let log = LogEntry {
            topics: vec![
                "0x".to_owned() + &"0".repeat(64),
                "0x".to_owned() + &"0".repeat(64),
                "0xshort".to_owned(), // too short
            ],
        };
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&[log], &operators, &mut wallets);
        // No valid addresses extracted — no panic.
        assert!(wallets.is_empty());
    }

    // ── Fixture fetcher for bisect test ───────────────────────────────────────

    #[derive(Clone)]
    struct FixtureFetcher {
        responses: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        call_log: Arc<Mutex<Vec<String>>>,
    }

    impl FixtureFetcher {
        fn new(responses: HashMap<String, Vec<u8>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
                call_log: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.call_log.lock().unwrap().clone()
        }
    }

    impl HttpFetcher for FixtureFetcher {
        async fn fetch(&self, url: &str) -> Result<Vec<u8>, FetchError> {
            self.call_log.lock().unwrap().push(url.to_owned());
            self.responses
                .lock()
                .unwrap()
                .get(url)
                .cloned()
                .ok_or_else(|| FetchError::Fatal(format!("no fixture for: {url}")))
        }
    }

    fn json_logs(topic0: B256, n: usize) -> Vec<u8> {
        let maker = w(0xaa);
        let taker = w(0xbb);
        let topic0_str = format!("{topic0}"); // B256 Display already includes "0x"
        let zero_str = "0x".to_owned() + &"0".repeat(64);
        let maker_str = topic_hex(maker);
        let taker_str = topic_hex(taker);
        let entry =
            format!(r#"{{"topics":["{topic0_str}","{zero_str}","{maker_str}","{taker_str}"]}}"#,);
        let entries: Vec<&str> = (0..n).map(|_| entry.as_str()).collect();
        format!(
            r#"{{"status":"1","message":"OK","result":[{}]}}"#,
            entries.join(",")
        )
        .into_bytes()
    }

    fn json_empty() -> Vec<u8> {
        br#"{"status":"0","message":"No records found","result":[]}"#.to_vec()
    }

    /// Bisect-on-cap: if a range returns exactly LOGS_PAGE_CAP logs, the
    /// enumerator must bisect that range and re-fetch both halves. Verifies
    /// that the second-level calls are made with the correct sub-ranges.
    ///
    /// Under #179, `enumerate()` issues one call per (contract, topic) pair —
    /// so the fixture map registers 2 URLs per contract (8 total) plus the
    /// 2 bisect halves on the cap-triggering V1 call.
    #[tokio::test]
    async fn bisect_on_cap_triggers_recursion() {
        let api_key = "TESTKEY";
        let base = "http://test.local";

        let url_for = |contract: alloy::primitives::Address, topic: B256, from: u64, to: u64| {
            format!(
                "{base}?chainid={chain_id}&module=logs&action=getLogs\
                 &address=0x{contract:x}&topic0={topic0}\
                 &fromBlock={from}&toBlock={to}\
                 &offset={cap}&page=1&apikey={api_key}",
                chain_id = POLYGON_CHAIN_ID,
                topic0 = topic, // B256 Display already includes "0x"
                cap = LOGS_PAGE_CAP,
            )
        };

        // CTF_EXCHANGE_V1 / V1-topic: cap-hit forces bisect.
        let v1_full = url_for(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, 100, 101);
        let v1_left = url_for(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, 100, 100);
        let v1_right = url_for(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, 101, 101);

        let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
        responses.insert(v1_full, json_logs(TOPIC_ORDER_FILLED_V1, LOGS_PAGE_CAP));
        responses.insert(v1_left, json_logs(TOPIC_ORDER_FILLED_V1, 1));
        responses.insert(v1_right, json_empty());

        // All other (contract, topic) pairs return empty — that includes
        // CTF_EXCHANGE_V1/V2-topic, plus the full grid for the remaining
        // 3 contracts × 2 topics = 6 URLs. Total: 1 (cap) + 2 (bisect) + 7 (empty).
        for contract_addr in [
            CTF_EXCHANGE_V1,
            NEG_RISK_CTF_EXCHANGE_V1,
            CTF_EXCHANGE_V2,
            NEG_RISK_CTF_EXCHANGE_V2,
        ] {
            for topic in [TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2] {
                let url = url_for(contract_addr, topic, 100, 101);
                responses.entry(url).or_insert_with(json_empty);
            }
        }

        let fetcher = FixtureFetcher::new(responses);
        let config = EnumerationConfig {
            from_block: 100,
            to_block: 101,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(
            fetcher.clone(),
            api_key.to_owned(),
            config,
            base.to_owned(),
        );

        let wallets = enumerator.enumerate().await.unwrap();
        let calls = fetcher.calls();

        // Must have called both sub-ranges (not just the cap range).
        assert!(
            calls
                .iter()
                .any(|u| u.contains("fromBlock=100&toBlock=100")),
            "expected left sub-range call; calls: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|u| u.contains("fromBlock=101&toBlock=101")),
            "expected right sub-range call; calls: {calls:?}"
        );
        assert!(
            !wallets.is_empty(),
            "wallets should contain at least one address"
        );
    }

    #[test]
    fn invalid_config_from_gt_to_returns_error() {
        let config = EnumerationConfig {
            from_block: 200,
            to_block: 100,
            operator_addresses: vec![],
        };
        let fetcher = FixtureFetcher::new(HashMap::new());
        let enumerator = PolymarketTraderEnumeration::with_fetcher(
            fetcher,
            "KEY".to_owned(),
            config,
            "http://test.local".to_owned(),
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(enumerator.enumerate());
        assert!(matches!(result, Err(EnumerationError::InvalidConfig(_))));
    }
}
