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
//! volume for staying inside Etherscan's free 5 req/s budget instead of
//! consuming Alchemy compute units. For seed sets of ~10–100 wallets and
//! `funding_max_hops = 3`, total wall time is bounded at a few minutes.
//!
//! Pagination is intentionally not implemented: Etherscan caps results at
//! `MAX_RESULTS_PER_PAGE = 10_000` and a wallet with more than ~10k incoming
//! USDC transfers in the discovery range is exceptional. A `tracing::warn!`
//! is emitted at the cap so silent truncation is visible.
//!
//! [1]: https://docs.etherscan.io/etherscan-v2/api-endpoints/accounts#get-a-list-of-erc20-token-transfer-events-by-address

use std::collections::HashSet;
use std::time::Duration;

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
/// Free-tier rate limit is 5 req/s. Sleep between sequential calls to stay under it.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const RATE_LIMIT_DELAY_MS: u64 = 200;
/// Cap on retry backoff when transient errors occur.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const MAX_BACKOFF_SECS: u64 = 60;
/// Maximum retry attempts before failing with the last error.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const MAX_ATTEMPTS: u32 = 6;
/// Per-request HTTP timeout.
/// Canonical value in `docs/_GLOSSARY.md` "Etherscan funder defaults".
const HTTP_TIMEOUT_SECS: u64 = 30;
/// Etherscan's per-page result cap. Hitting this is a signal that pagination
/// is missing for the wallet, not that the wallet has exactly 10k transfers.
const MAX_RESULTS_PER_PAGE: usize = 10_000;

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
}

// ── Lookup ────────────────────────────────────────────────────────────────────

/// [`FunderLookup`] backed by the Etherscan V2 tokentx API.
pub struct EtherscanFunderLookup<F: HttpFetcher> {
    fetcher: F,
    api_key: String,
    base_url: String,
}

impl EtherscanFunderLookup<reqwest::Client> {
    /// Construct with a default `reqwest::Client` and the production base URL.
    pub fn new(api_key: String) -> Self {
        Self {
            fetcher: reqwest::Client::new(),
            api_key,
            base_url: ETHERSCAN_BASE_URL.to_owned(),
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
        }
    }

    fn build_url(&self, wallet: &WalletAddress, contract: &str, range: BlockRange) -> String {
        format!(
            "{base}?chainid={chain}&module=account&action=tokentx\
             &contractaddress={contract}&address={wallet}\
             &startblock={start}&endblock={end}\
             &page=1&offset={offset}&sort=asc&apikey={key}",
            base = self.base_url,
            chain = POLYGON_CHAIN_ID,
            contract = contract,
            wallet = wallet,
            start = range.from,
            end = range.to,
            offset = MAX_RESULTS_PER_PAGE,
            key = self.api_key,
        )
    }

    /// Fetch and parse a single page, retrying on transient HTTP errors AND
    /// on Etherscan's in-band rate-limit responses (HTTP 200 + status="0" +
    /// body containing "Max rate limit reached"). A `Fatal` HTTP error or a
    /// permanent API error returns immediately without retry.
    async fn fetch_and_parse_with_backoff(
        &self,
        url: &str,
    ) -> Result<Vec<TokenTxEntry>, FunderDiscoveryError> {
        let mut backoff_secs: u64 = 1;
        let mut last_err: Option<String> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            let outcome = match self.fetcher.fetch(url).await {
                Ok(bytes) => parse_tokentx_response(&bytes),
                Err(FetchError::Fatal(m)) => {
                    return Err(FunderDiscoveryError::Etherscan(format!("fatal: {m}")));
                }
                Err(FetchError::Transient(m)) => Err(ParseOutcome::Transient(m)),
            };
            match outcome {
                Ok(entries) => return Ok(entries),
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
                let url = self.build_url(wallet, contract, range);
                let transfers = self.fetch_and_parse_with_backoff(&url).await?;
                if transfers.len() >= MAX_RESULTS_PER_PAGE {
                    warn!(
                        wallet = %wallet,
                        contract,
                        result_count = transfers.len(),
                        "etherscan: response hit per-page cap; some funders may be missing"
                    );
                }
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
                tokio::time::sleep(Duration::from_millis(RATE_LIMIT_DELAY_MS)).await;
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
        let url = lookup.build_url(&wallet, USDC_NATIVE, BlockRange { from: 100, to: 200 });
        assert!(url.contains("chainid=137"));
        assert!(url.contains("module=account"));
        assert!(url.contains("action=tokentx"));
        assert!(url.contains(&format!("contractaddress={USDC_NATIVE}")));
        assert!(url.contains(&format!("address={wallet}")));
        assert!(url.contains("startblock=100"));
        assert!(url.contains("endblock=200"));
        assert!(url.contains("apikey=KEY"));
    }
}
