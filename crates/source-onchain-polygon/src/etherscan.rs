//! Etherscan V2 funder discovery for Polygon (chain id 137).
//!
//! Implements [`FunderLookup`] by issuing one [`tokentx`][1] query per
//! `(wallet, USDC contract)` pair. Two USDC contracts are queried per wallet:
//! native USDC (`0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359`) and the bridged
//! USDC.e (`0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`). Funders are the
//! `from` addresses of incoming transfers; outgoing transfers are filtered
//! out at parse time.
//!
//! Compared to [`crate::funder_discovery::EthGetLogsLookup`], which batches
//! multiple recipients into a single `topic[2]` filter per call, this lookup
//! issues one HTTP request per wallet per contract. That trades request
//! volume for staying inside Etherscan's free 3 req/s budget instead of
//! consuming Alchemy compute units. For seed sets of ~10–100 wallets and
//! `funding_max_hops = 3`, total wall time is bounded at a few minutes.
//!
//! Each `(wallet, contract)` pair is fetched via a block-range cursor: the first
//! query uses `page=1&offset=10000&startblock=range.from`; on a full page the
//! cursor advances to the highest `blockNumber` in that batch and the next query
//! uses that block as `startblock`. Up to `MAX_PAGES = 100` cursor iterations
//! (≤ 1M transfers) are attempted. A `tracing::warn!` is emitted at the cap so
//! hub-like wallets with extreme transfer volumes are visible in logs.
//!
//! [1]: https://docs.etherscan.io/etherscan-v2/api-endpoints/accounts#get-a-list-of-erc20-token-transfer-events-by-address

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use pe_core_types::WalletAddress;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::funder_discovery::{BlockRange, FunderDiscoveryError, FunderLookup};

const ETHERSCAN_BASE_URL: &str = "https://api.etherscan.io/v2/api";
const POLYGON_CHAIN_ID: u32 = 137;
/// Native USDC on Polygon (Circle-issued).
const USDC_NATIVE: &str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359";
/// Bridged USDC.e on Polygon (legacy bridged from Ethereum).
const USDC_BRIDGED: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
/// Free-tier rate limit is 3 req/s. Every HTTP call acquires a token from this
/// bucket; retries also acquire tokens so backoff windows don't bypass the cap.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const FUNDER_RATE_LIMIT_RPS: NonZeroU32 = NonZeroU32::MIN.saturating_add(2); // = 3
/// Cap on retry backoff when transient errors occur.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const MAX_BACKOFF_SECS: u64 = 60;
/// Maximum retry attempts before failing with the last error.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const MAX_ATTEMPTS: u32 = 6;
/// Per-request HTTP timeout.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const HTTP_TIMEOUT_SECS: u64 = 30;
/// Etherscan's per-page result cap.
const MAX_RESULTS_PER_PAGE: usize = 10_000;
/// Safety cap on paginated cursor iterations per (wallet, contract) pair. Keeps
/// the loop bounded for pathological hub wallets; 100 iterations × 10k results =
/// 1M transfers max.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
/// Public so integration tests can reference the cap symbolically.
pub const MAX_PAGES: u32 = 100;

// ── HTTP abstraction ──────────────────────────────────────────────────────────

/// Classified fetch error.
///
/// `Transient` (429, 5xx, network) is retryable; `Fatal` (4xx other than 429,
/// or malformed responses) is not. The retry loop in
/// [`EtherscanFunderLookup::fetch_with_backoff`] uses this to skip pointless
/// retries on permanent errors like a bad API key.
#[derive(Debug, Clone)]
pub enum FetchError {
    Transient(String),
    Fatal(String),
}

/// HTTP fetcher used by [`EtherscanFunderLookup`].
///
/// Production uses `reqwest::Client`; tests substitute a fixture-backed impl
/// to avoid live network calls.
pub trait HttpFetcher: Send + Sync {
    fn fetch(
        &self,
        url: &str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, FetchError>> + Send;
}

impl HttpFetcher for reqwest::Client {
    async fn fetch(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        let resp = self
            .get(url)
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|e| FetchError::Transient(e.to_string()))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| FetchError::Transient(e.to_string()))?;
        if status.is_success() {
            return Ok(bytes.to_vec());
        }
        let body = String::from_utf8_lossy(&bytes).to_string();
        // 429 + 5xx are retryable; other 4xx (auth, not-found, …) are permanent.
        if status.as_u16() == 429 || status.is_server_error() {
            Err(FetchError::Transient(format!("HTTP {status}: {body}")))
        } else {
            Err(FetchError::Fatal(format!("HTTP {status}: {body}")))
        }
    }
}

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

/// Top-level Etherscan response. `result` is `Vec<TokenTxEntry>` on success
/// (`status = "1"`) and a string error message on failure (`status = "0"`).
#[derive(Deserialize)]
struct EtherscanResponse {
    status: String,
    #[serde(default)]
    message: String,
    result: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenTxEntry {
    from: String,
    to: String,
    /// Block timestamp as a Unix-seconds string. Etherscan always populates this
    /// in `tokentx` results; `default` guards against fixture JSON that omits it.
    #[serde(rename = "timeStamp", default)]
    timestamp_str: String,
    /// Block number as a decimal string (e.g. `"12345678"`). Used by the
    /// block-range cursor in `fetch_all_tokentx_pages` to advance `startblock`
    /// after a full page. `default` keeps old fixture JSON compatible.
    #[serde(default)]
    block_number: String,
}

/// JSON-RPC response from `eth_blockNumber` proxy endpoint.
/// `result` is the current block number as a hex string (e.g. `"0x4d20d8"`).
#[derive(Deserialize)]
struct EthBlockNumberResponse {
    result: String,
}

// ── Lookup ────────────────────────────────────────────────────────────────────

/// [`FunderLookup`] backed by the Etherscan V2 tokentx API.
pub struct EtherscanFunderLookup<F: HttpFetcher> {
    fetcher: F,
    api_key: String,
    base_url: String,
    limiter: DefaultDirectRateLimiter,
}

fn build_limiter() -> DefaultDirectRateLimiter {
    let quota = Quota::per_second(FUNDER_RATE_LIMIT_RPS).allow_burst(FUNDER_RATE_LIMIT_RPS);
    RateLimiter::direct(quota)
}

impl EtherscanFunderLookup<reqwest::Client> {
    /// Construct with a default `reqwest::Client` and the production base URL.
    pub fn new(api_key: String) -> Self {
        Self {
            fetcher: reqwest::Client::new(),
            api_key,
            base_url: ETHERSCAN_BASE_URL.to_owned(),
            limiter: build_limiter(),
        }
    }
}

impl<F: HttpFetcher> EtherscanFunderLookup<F> {
    /// Construct with a custom fetcher and base URL — used by tests to
    /// substitute fixture responses for the live HTTP layer.
    pub fn with_fetcher(fetcher: F, api_key: String, base_url: String) -> Self {
        Self {
            fetcher,
            api_key,
            base_url,
            limiter: build_limiter(),
        }
    }

    /// Override the request-rate cap, builder-style (issue #201). Default is
    /// 3 req/s (Etherscan free tier); a paid tier can raise this to shorten a
    /// large funder backlog. Additive — leaves `new`/`with_fetcher` call sites
    /// (incl. the live source) untouched.
    #[must_use]
    pub fn with_rate_limit_rps(mut self, rps: NonZeroU32) -> Self {
        let quota = Quota::per_second(rps).allow_burst(rps);
        self.limiter = RateLimiter::direct(quota);
        self
    }

    /// Test-only constructor that overrides the rate limit. Production code uses
    /// [`Self::with_fetcher`] (3 req/s); the cap-exhaustion scenario test bumps
    /// this so its `MAX_PAGES` cursor iterations don't take 33+ seconds at 3 rps.
    #[doc(hidden)]
    pub fn with_fetcher_and_rps(
        fetcher: F,
        api_key: String,
        base_url: String,
        rps: NonZeroU32,
    ) -> Self {
        let quota = Quota::per_second(rps).allow_burst(rps);
        Self {
            fetcher,
            api_key,
            base_url,
            limiter: RateLimiter::direct(quota),
        }
    }

    fn build_block_number_url(&self) -> String {
        format!(
            "{base}?chainid={chain}&module=proxy&action=eth_blockNumber&apikey={key}",
            base = self.base_url,
            chain = POLYGON_CHAIN_ID,
            key = self.api_key,
        )
    }

    /// Build a `tokentx` URL. Always uses `page=1&offset=MAX_RESULTS_PER_PAGE`;
    /// callers advance the block-range cursor between calls instead of incrementing
    /// the page number (Etherscan enforces `page × offset ≤ 10_000`).
    fn build_url(
        &self,
        wallet: &WalletAddress,
        contract: &str,
        startblock: u64,
        endblock: u64,
    ) -> String {
        format!(
            "{base}?chainid={chain}&module=account&action=tokentx\
             &contractaddress={contract}&address={wallet}\
             &startblock={startblock}&endblock={endblock}\
             &page=1&offset={offset}&sort=asc&apikey={key}",
            base = self.base_url,
            chain = POLYGON_CHAIN_ID,
            contract = contract,
            wallet = wallet,
            startblock = startblock,
            endblock = endblock,
            offset = MAX_RESULTS_PER_PAGE,
            key = self.api_key,
        )
    }

    /// Fetch all `tokentx` results for a single `(wallet, contract, range)` triple
    /// using a block-range cursor.
    ///
    /// After each full page (10k entries) the cursor advances to the highest
    /// `blockNumber` in that batch and the next request uses it as `startblock`.
    /// Entries near the cursor boundary may appear in two consecutive batches; the
    /// `HashMap` in callers deduplicates them by `from` address. Up to `MAX_PAGES`
    /// iterations are attempted; a `warn!` is emitted if the cap is hit.
    async fn fetch_all_tokentx_pages(
        &self,
        wallet: &WalletAddress,
        contract: &str,
        range: BlockRange,
    ) -> Result<Vec<TokenTxEntry>, FunderDiscoveryError> {
        let mut all_entries: Vec<TokenTxEntry> = Vec::new();
        let mut startblock = range.from;

        for _iter in 1..=MAX_PAGES {
            let url = self.build_url(wallet, contract, startblock, range.to);
            let entries = self.fetch_and_parse_with_backoff(&url).await?;
            let count = entries.len();

            if count < MAX_RESULTS_PER_PAGE {
                all_entries.extend(entries);
                return Ok(all_entries);
            }

            // Full page: advance cursor to the max block seen so the next query
            // starts there. If block_number is absent or unparseable (old fixture
            // format), bail out with the guard below to avoid an infinite loop.
            let next_start = entries
                .iter()
                .filter_map(|e| e.block_number.parse::<u64>().ok())
                .max()
                .unwrap_or(startblock);

            all_entries.extend(entries);

            if next_start <= startblock {
                warn!(
                    wallet = %wallet,
                    contract,
                    startblock,
                    "etherscan: cursor did not advance (missing blockNumber?); stopping early"
                );
                return Ok(all_entries);
            }
            startblock = next_start;
        }

        warn!(
            wallet = %wallet,
            contract,
            max_pages = MAX_PAGES,
            "etherscan: hit max page limit; some funders may be missing for this wallet"
        );
        Ok(all_entries)
    }

    /// Generic fetch-and-parse loop with exponential backoff.
    ///
    /// Retries on transient HTTP errors and on Etherscan's in-band rate-limit
    /// responses (HTTP 200 + status="0"). `parser` maps raw response bytes to
    /// `Result<T, ParseOutcome>`; a `Fatal` HTTP error or `ParseOutcome::Fatal`
    /// returns immediately without retry.
    async fn fetch_with_backoff<T, P>(
        &self,
        url: &str,
        parser: P,
    ) -> Result<T, FunderDiscoveryError>
    where
        P: Fn(&[u8]) -> Result<T, ParseOutcome>,
    {
        let mut backoff_secs: u64 = 1;
        let mut last_err: Option<String> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            self.limiter.until_ready().await;
            let outcome = match self.fetcher.fetch(url).await {
                Ok(bytes) => parser(&bytes),
                Err(FetchError::Fatal(m)) => {
                    return Err(FunderDiscoveryError::Etherscan(format!("fatal: {m}")));
                }
                Err(FetchError::Transient(m)) => Err(ParseOutcome::Transient(m)),
            };
            match outcome {
                Ok(value) => return Ok(value),
                Err(ParseOutcome::Transient(m)) => {
                    warn!(
                        attempt,
                        max = MAX_ATTEMPTS,
                        error = %m,
                        "etherscan: transient error, retrying"
                    );
                    last_err = Some(m);
                    if attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    }
                }
                Err(ParseOutcome::Fatal(m)) => {
                    return Err(FunderDiscoveryError::Etherscan(format!("fatal: {m}")));
                }
            }
        }
        Err(FunderDiscoveryError::Etherscan(format!(
            "exhausted {MAX_ATTEMPTS} attempts: {}",
            last_err.unwrap_or_else(|| "no error captured".to_owned())
        )))
    }

    async fn fetch_and_parse_with_backoff(
        &self,
        url: &str,
    ) -> Result<Vec<TokenTxEntry>, FunderDiscoveryError> {
        self.fetch_with_backoff(url, parse_tokentx_response).await
    }

    /// Return the current Polygon block number via the Etherscan V2 proxy API.
    ///
    /// Calls `eth_blockNumber` on chain id 137. Retries with the same backoff
    /// policy as funder discovery so the etherscan path needs no Alchemy URL.
    ///
    /// # Precondition
    /// `api_key` must be non-empty; an empty key will return a rate-limit
    /// or auth error from Etherscan on the first attempt.
    pub async fn current_block(&self) -> Result<u64, FunderDiscoveryError> {
        let url = self.build_block_number_url();
        self.fetch_with_backoff(&url, parse_block_number_response)
            .await
    }
}

impl<F: HttpFetcher> EtherscanFunderLookup<F> {
    /// Return the earliest Polygon block timestamp (Unix seconds) per unique funder address
    /// for every incoming USDC transfer to `wallets` within `range`.
    ///
    /// Queries both native USDC and bridged USDC.e. When the same funder address appears in
    /// multiple transfers the minimum `timeStamp` is kept (earliest known funding event).
    /// Entries with an unparseable or absent `timeStamp` are stored with sentinel `0`
    /// (epoch — always visible in walk-forward simulations).
    pub async fn funders_of_with_timestamps(
        &self,
        wallets: &HashSet<WalletAddress>,
        range: BlockRange,
    ) -> Result<HashMap<WalletAddress, i64>, FunderDiscoveryError> {
        let mut result: HashMap<WalletAddress, i64> = HashMap::new();
        for wallet in wallets {
            for contract in [USDC_NATIVE, USDC_BRIDGED] {
                let transfers = self
                    .fetch_all_tokentx_pages(wallet, contract, range)
                    .await?;
                let wallet_hex = wallet.to_string();
                for entry in transfers {
                    if !entry.to.eq_ignore_ascii_case(&wallet_hex) {
                        continue;
                    }
                    let Ok(addr) = WalletAddress::from_hex(&entry.from) else {
                        debug!(from = %entry.from, "etherscan: skipping unparseable from address");
                        continue;
                    };
                    let ts: i64 = entry.timestamp_str.parse().unwrap_or(0);
                    result
                        .entry(addr)
                        .and_modify(|existing| *existing = (*existing).min(ts))
                        .or_insert(ts);
                }
            }
        }
        Ok(result)
    }
}

impl<F: HttpFetcher> FunderLookup for EtherscanFunderLookup<F> {
    async fn funders_of(
        &self,
        wallets: &HashSet<WalletAddress>,
        range: BlockRange,
    ) -> Result<HashSet<WalletAddress>, FunderDiscoveryError> {
        let mut funders: HashSet<WalletAddress> = HashSet::new();
        for wallet in wallets {
            for contract in [USDC_NATIVE, USDC_BRIDGED] {
                let transfers = self
                    .fetch_all_tokentx_pages(wallet, contract, range)
                    .await?;
                let wallet_hex = wallet.to_string();
                for entry in transfers {
                    if !entry.to.eq_ignore_ascii_case(&wallet_hex) {
                        continue;
                    }
                    match WalletAddress::from_hex(&entry.from) {
                        Ok(addr) => {
                            funders.insert(addr);
                        }
                        Err(e) => {
                            debug!(from = %entry.from, error = %e, "etherscan: skipping unparseable from address");
                        }
                    }
                }
            }
        }
        Ok(funders)
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Outcome of parsing one Etherscan response — distinguishes in-band errors
/// that should trigger a retry (rate limit, transient API failure) from
/// permanent API errors (malformed JSON, unexpected schema).
#[derive(Debug)]
enum ParseOutcome {
    Transient(String),
    Fatal(String),
}

fn parse_tokentx_response(bytes: &[u8]) -> Result<Vec<TokenTxEntry>, ParseOutcome> {
    let resp: EtherscanResponse = serde_json::from_slice(bytes)
        .map_err(|e| ParseOutcome::Fatal(format!("decode envelope: {e}")))?;
    match resp.status.as_str() {
        "1" => serde_json::from_value::<Vec<TokenTxEntry>>(resp.result)
            .map_err(|e| ParseOutcome::Fatal(format!("decode result array: {e}"))),
        // "No transactions found" is reported as status="0" with an empty
        // result array — treat as success with zero transfers.
        "0" if resp.message.eq_ignore_ascii_case("No transactions found") => Ok(Vec::new()),
        // Etherscan reports rate-limit failures in-band with HTTP 200; treat
        // them as transient so the retry loop kicks in.
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

fn parse_block_number_response(bytes: &[u8]) -> Result<u64, ParseOutcome> {
    let resp: EthBlockNumberResponse = serde_json::from_slice(bytes)
        .map_err(|e| ParseOutcome::Fatal(format!("decode block number response: {e}")))?;
    // Etherscan returns rate-limit failures in the result field as plain strings
    // (e.g. "Max rate limit reached") even for the JSON-RPC proxy endpoint.
    // Detect these before attempting hex parse so they are retried, not failed.
    if resp.result.to_lowercase().contains("rate limit")
        || resp.result.to_lowercase().contains("notok")
    {
        return Err(ParseOutcome::Transient(format!(
            "eth_blockNumber API error: {}",
            resp.result
        )));
    }
    let hex = resp.result.trim_start_matches("0x");
    u64::from_str_radix(hex, 16)
        .map_err(|e| ParseOutcome::Fatal(format!("parse hex block number '{}': {e}", resp.result)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_ok_response() {
        let json = br#"{"status":"1","message":"OK","result":[
            {"from":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","to":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
            {"from":"0xcccccccccccccccccccccccccccccccccccccccc","to":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
        ]}"#;
        let entries = parse_tokentx_response(json).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].from,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn parses_no_transactions_as_empty() {
        let json = br#"{"status":"0","message":"No transactions found","result":[]}"#;
        let entries = parse_tokentx_response(json).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn rate_limited_response_is_transient() {
        let json = br#"{"status":"0","message":"NOTOK","result":"Max rate limit reached"}"#;
        let result = parse_tokentx_response(json);
        assert!(
            matches!(result, Err(ParseOutcome::Transient(_))),
            "rate-limit must be classified Transient so the retry loop kicks in, got {result:?}"
        );
    }

    #[test]
    fn malformed_json_is_fatal() {
        let result = parse_tokentx_response(b"not json");
        assert!(matches!(result, Err(ParseOutcome::Fatal(_))));
    }

    #[test]
    fn build_url_includes_required_params() {
        let lookup = EtherscanFunderLookup::new("KEY".to_owned());
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let url = lookup.build_url(&wallet, USDC_NATIVE, 100, 200);
        assert!(url.contains("chainid=137"));
        assert!(url.contains("module=account"));
        assert!(url.contains("action=tokentx"));
        assert!(url.contains(&format!("contractaddress={USDC_NATIVE}")));
        assert!(url.contains(&format!("address={wallet}")));
        assert!(url.contains("startblock=100"));
        assert!(url.contains("endblock=200"));
        assert!(url.contains("page=1"));
        assert!(url.contains("apikey=KEY"));
    }

    #[test]
    fn build_url_startblock_cursor_advances() {
        let lookup = EtherscanFunderLookup::new("KEY".to_owned());
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let url1 = lookup.build_url(&wallet, USDC_NATIVE, 1, 100_000_000);
        let url2 = lookup.build_url(&wallet, USDC_NATIVE, 50_000, 100_000_000);
        assert!(
            url1.contains("startblock=1"),
            "first cursor url must have startblock=1"
        );
        assert!(
            url2.contains("startblock=50000"),
            "advanced cursor must appear in url"
        );
        assert_ne!(
            url1, url2,
            "different startblocks must produce different URLs"
        );
    }

    #[test]
    fn parse_block_number_response_decodes_hex() {
        // 0x4d20d8 == 5_054_680
        let json = br#"{"jsonrpc":"2.0","id":1,"result":"0x4d20d8"}"#;
        let block = parse_block_number_response(json).unwrap();
        assert_eq!(block, 5_054_680u64);
    }

    #[test]
    fn parse_block_number_response_bad_hex_is_fatal() {
        let json = br#"{"jsonrpc":"2.0","id":1,"result":"0xnothex"}"#;
        let result = parse_block_number_response(json);
        assert!(
            matches!(result, Err(ParseOutcome::Fatal(_))),
            "invalid hex must be Fatal, got {result:?}"
        );
    }

    #[test]
    fn parse_block_number_response_malformed_json_is_fatal() {
        let result = parse_block_number_response(b"not json at all");
        assert!(matches!(result, Err(ParseOutcome::Fatal(_))));
    }

    #[test]
    fn parse_block_number_response_rate_limit_is_transient() {
        // Etherscan returns rate-limit errors in the result field as plain strings
        // even for the JSON-RPC proxy endpoint. Must be Transient so fetch_with_backoff retries.
        let json = br#"{"jsonrpc":"2.0","id":1,"result":"Max rate limit reached"}"#;
        let result = parse_block_number_response(json);
        assert!(
            matches!(result, Err(ParseOutcome::Transient(_))),
            "rate-limit result must be Transient so the retry loop kicks in, got {result:?}"
        );
    }

    #[test]
    fn build_block_number_url_includes_required_params() {
        let lookup = EtherscanFunderLookup::new("MYKEY".to_owned());
        let url = lookup.build_block_number_url();
        assert!(url.contains("chainid=137"));
        assert!(url.contains("module=proxy"));
        assert!(url.contains("action=eth_blockNumber"));
        assert!(url.contains("apikey=MYKEY"));
    }
}
