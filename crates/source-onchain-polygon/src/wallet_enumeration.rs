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

use pe_core_types::WalletAddress;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::contracts::{ALL_EXCHANGE_CONTRACTS, CTF_EXCHANGE_V1_DEPLOY_BLOCK, TOPIC_ORDER_FILLED};
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
    /// removed. Scans both maker (topic2) and taker (topic3) from `OrderFilled` events.
    pub async fn enumerate(&self) -> Result<HashSet<WalletAddress>, EnumerationError> {
        if self.config.from_block > self.config.to_block {
            return Err(EnumerationError::InvalidConfig(format!(
                "from_block {} > to_block {}",
                self.config.from_block, self.config.to_block
            )));
        }

        let operator_set: HashSet<WalletAddress> =
            self.config.operator_addresses.iter().copied().collect();
        let mut wallets: HashSet<WalletAddress> = HashSet::new();

        // Each contract address is queried separately; Etherscan's `address` param
        // is a single address (not an array) for the free-tier `eth_getLogs` endpoint.
        for contract in &ALL_EXCHANGE_CONTRACTS {
            let contract_hex = format!("0x{contract:x}");
            debug!(contract = %contract_hex, "wallet enumeration: scanning contract");

            self.scan_range(
                &contract_hex,
                self.config.from_block,
                self.config.to_block,
                &operator_set,
                &mut wallets,
            )
            .await?;

            tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
        }

        Ok(wallets)
    }

    /// Recursively scan `[from, to]` for `OrderFilled` logs, bisecting if the
    /// response hits the page cap.
    fn scan_range<'a>(
        &'a self,
        contract_hex: &'a str,
        from: u64,
        to: u64,
        operator_set: &'a HashSet<WalletAddress>,
        wallets: &'a mut HashSet<WalletAddress>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), EnumerationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let url = self.build_url(contract_hex, from, to);
            let logs = self.fetch_logs_with_backoff(&url).await?;
            let count = logs.len();

            if count >= LOGS_PAGE_CAP {
                if from == to {
                    // Single-block cap: can't bisect further. Retain the logs we have
                    // and warn — this block is unusually dense but we capture what Etherscan
                    // returns.
                    warn!(
                        contract = %contract_hex,
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
                self.scan_range(contract_hex, from, mid, operator_set, wallets)
                    .await?;
                tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
                self.scan_range(contract_hex, mid + 1, to, operator_set, wallets)
                    .await?;
            } else {
                extract_wallets(&logs, operator_set, wallets);
            }
            Ok(())
        })
    }

    fn build_url(&self, contract_hex: &str, from: u64, to: u64) -> String {
        // B256 Display already includes the "0x" prefix — do not add a second one.
        let topic0 = format!("{TOPIC_ORDER_FILLED}");
        format!(
            "{base}?chainid={chain}&module=logs&action=getLogs\
             &address={contract}&topic0={topic0}\
             &fromBlock={from}&toBlock={to}\
             &offset={cap}&page=1&apikey={key}",
            base = self.base_url,
            chain = POLYGON_CHAIN_ID,
            contract = contract_hex,
            topic0 = topic0,
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
            if resp.message.to_uppercase().contains("NOTOK")
                || body.to_lowercase().contains("rate limit")
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

    fn make_log(maker: WalletAddress, taker: WalletAddress) -> LogEntry {
        LogEntry {
            topics: vec![
                format!("{TOPIC_ORDER_FILLED}"),
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
        let logs = vec![make_log(maker, taker)];
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
        let logs = vec![make_log(maker, operator)];
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
        let logs = vec![make_log(maker, taker), make_log(maker, taker)];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert_eq!(wallets.len(), 2);
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

    fn json_logs(n: usize) -> Vec<u8> {
        let maker = w(0xaa);
        let taker = w(0xbb);
        let topic0_str = format!("{TOPIC_ORDER_FILLED}"); // B256 Display already includes "0x"
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
    #[tokio::test]
    async fn bisect_on_cap_triggers_recursion() {
        // Block range 100..=101 on a single contract.
        // First call (100..=101) → exactly LOGS_PAGE_CAP logs → must bisect.
        // Second call (100..=100) → 1 log.
        // Third call (101..=101) → 0 logs.
        let contract = format!("0x{:x}", CTF_EXCHANGE_V1);
        let api_key = "TESTKEY";
        let base = "http://test.local";

        let chain_id = POLYGON_CHAIN_ID;
        let full_url = format!(
            "{base}?chainid={chain_id}&module=logs&action=getLogs\
             &address={contract}&topic0={topic0}\
             &fromBlock=100&toBlock=101\
             &offset={cap}&page=1&apikey={api_key}",
            topic0 = TOPIC_ORDER_FILLED, // B256 Display already includes "0x"
            cap = LOGS_PAGE_CAP,
        );
        let left_url = full_url.replace("fromBlock=100&toBlock=101", "fromBlock=100&toBlock=100");
        let right_url = full_url.replace("fromBlock=100&toBlock=101", "fromBlock=101&toBlock=101");

        // Build URLs for the other 3 contracts (all empty).
        let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
        responses.insert(full_url.clone(), json_logs(LOGS_PAGE_CAP));
        responses.insert(left_url.clone(), json_logs(1));
        responses.insert(right_url.clone(), json_empty());

        // Add empty responses for V2 and NegRisk contracts.
        for contract_addr in [
            NEG_RISK_CTF_EXCHANGE_V1,
            CTF_EXCHANGE_V2,
            NEG_RISK_CTF_EXCHANGE_V2,
        ] {
            let chain_id = POLYGON_CHAIN_ID;
            let url = format!(
                "{base}?chainid={chain_id}&module=logs&action=getLogs\
                 &address=0x{contract_addr:x}&topic0={topic0}\
                 &fromBlock=100&toBlock=101\
                 &offset={cap}&page=1&apikey={api_key}",
                topic0 = TOPIC_ORDER_FILLED, // B256 Display already includes "0x"
                cap = LOGS_PAGE_CAP,
            );
            responses.insert(url, json_empty());
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
