//! Polymarket wallet enumeration via alloy `eth_getLogs` on Polygon (chain 137).
//!
//! Scans `OrderFilled` events across all four Polymarket exchange contracts
//! (CTFExchange V1/V2 and NegRiskCtfExchange V1/V2) to build the full set of
//! distinct wallets that have ever traded on Polymarket. Both `maker` (topic2)
//! and `taker` (topic3) are indexed and extracted directly from log topics —
//! no ABI decoding of the data payload is required.
//!
//! ## Backend (issue #186)
//!
//! Backed by a [`ChainLogFetcher`] — production code passes
//! [`crate::AlloyChainLogFetcher`] wrapping an Alchemy alloy provider. Bisect
//! and HTTP 429 retry live inside [`crate::eth_get_logs_bisect`]; this module
//! only orchestrates the per-`(contract, topic, chunk)` scan and operator
//! filtering. Replaces the legacy Etherscan REST path that was capped at
//! 100k requests/day on the free tier.
//!
//! ## Chunking
//!
//! The full `[from_block, to_block]` range is sliced into
//! [`SCAN_CHUNK_BLOCKS`]-sized windows. This is the persistence granularity
//! for [`crate::wallet_enumeration::PolymarketTraderEnumeration::enumerate_chunk`]:
//! callers that want mid-sweep crash safety (e.g. `pe-bootstrap`) loop over
//! chunks and upsert after each one.
//!
//! ## Operator filter
//!
//! Polymarket's matching operator typically appears as `taker` on
//! maker-vs-operator legs. These addresses are configurable via
//! [`EnumerationConfig::operator_addresses`] and filtered from the output set.

use std::collections::HashSet;

use alloy::primitives::{Address, B256};
use alloy::rpc::types::{Filter, Log};
use pe_core_types::WalletAddress;
use tracing::debug;

use crate::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, CTF_EXCHANGE_V1_DEPLOY_BLOCK,
};
use crate::eth_logs::{ChainLogFetcher, PolygonRpcError};
use crate::funder_discovery::topic_to_wallet;

/// Maximum block span for a single top-level `eth_getLogs` request before the
/// bisection inside [`crate::eth_get_logs_bisect`] takes over. Set small enough
/// that a single chunk fits well under provider per-call response caps even
/// on dense historical periods; the bootstrap loops over chunks and persists
/// after each one, so this is also the crash-recovery granularity.
/// Canonical default in `docs/_GLOSSARY.md` "Wallet enumeration defaults".
pub const SCAN_CHUNK_BLOCKS: u64 = 500_000;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for [`PolymarketTraderEnumeration`].
#[derive(Debug, Clone)]
pub struct EnumerationConfig {
    /// Start block for the scan. Defaults to [`CTF_EXCHANGE_V1_DEPLOY_BLOCK`].
    pub from_block: u64,
    /// End block for the scan (inclusive). Callers should supply the current
    /// chain head.
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
    #[error("provider eth_getLogs: {0}")]
    Provider(#[from] PolygonRpcError),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

// ── Main struct ───────────────────────────────────────────────────────────────

/// Enumerates every distinct Polymarket wallet by scanning `OrderFilled` events
/// via a [`ChainLogFetcher`].
pub struct PolymarketTraderEnumeration<F: ChainLogFetcher> {
    fetcher: F,
    config: EnumerationConfig,
}

impl<F: ChainLogFetcher> PolymarketTraderEnumeration<F> {
    /// Construct with any [`ChainLogFetcher`] backend.
    ///
    /// Production callers pass [`crate::AlloyChainLogFetcher`]; tests pass
    /// [`crate::eth_logs::test_support::InMemoryChainLogFetcher`].
    pub fn with_fetcher(fetcher: F, config: EnumerationConfig) -> Self {
        Self { fetcher, config }
    }

    /// Borrow the active [`EnumerationConfig`] (read-only).
    #[must_use]
    pub fn config(&self) -> &EnumerationConfig {
        &self.config
    }

    /// Enumerate every distinct trader wallet across all Polymarket exchange contracts.
    ///
    /// Returns a deduplicated `HashSet<WalletAddress>` with operator addresses
    /// removed. Scans both maker (topic2) and taker (topic3) from `OrderFilled`
    /// events for every topic in [`ALL_ORDER_FILLED_TOPICS`] (V1 and V2).
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

    /// Enumerate every distinct trader wallet for a single `(contract, topic0)`
    /// pair across the full configured block range, looping over
    /// [`SCAN_CHUNK_BLOCKS`]-sized windows via [`Self::enumerate_chunk`].
    ///
    /// # Precondition
    /// `config.from_block <= config.to_block`.
    pub async fn enumerate_one_contract_for_topic(
        &self,
        contract: Address,
        topic0: B256,
    ) -> Result<HashSet<WalletAddress>, EnumerationError> {
        let mut wallets: HashSet<WalletAddress> = HashSet::new();
        let mut chunk_from = self.config.from_block;
        while chunk_from <= self.config.to_block {
            let chunk_to = (chunk_from + SCAN_CHUNK_BLOCKS - 1).min(self.config.to_block);
            wallets.extend(
                self.enumerate_chunk(contract, topic0, chunk_from, chunk_to)
                    .await?,
            );
            chunk_from = chunk_to + 1;
        }
        Ok(wallets)
    }

    /// Scan a single `[from_block, to_block]` chunk for `OrderFilled` logs of
    /// `(contract, topic0)` and return the deduplicated wallet set with
    /// operator addresses removed.
    ///
    /// This is the unit of crash-safe persistence: `pe-bootstrap` calls this
    /// once per chunk and upserts the result before advancing, so a process
    /// crash at chunk `N` loses at most one chunk's worth of work rather than
    /// the entire contract.
    ///
    /// Bisect-on-cap and HTTP 429 retry live inside [`ChainLogFetcher::get_logs`]
    /// (which production-wraps [`crate::eth_get_logs_bisect`]).
    ///
    /// # Precondition
    /// `from_block <= to_block`.
    pub async fn enumerate_chunk(
        &self,
        contract: Address,
        topic0: B256,
        from_block: u64,
        to_block: u64,
    ) -> Result<HashSet<WalletAddress>, EnumerationError> {
        let operator_set: HashSet<WalletAddress> =
            self.config.operator_addresses.iter().copied().collect();
        let filter = Filter::new()
            .address(vec![contract])
            .event_signature(topic0);
        tracing::info!(
            contract = %format!("0x{contract:x}"),
            topic0 = %format!("{topic0}"),
            from_block,
            to_block,
            "wallet enumeration: scanning chunk"
        );
        let logs = self.fetcher.get_logs(filter, from_block, to_block).await?;
        let mut wallets: HashSet<WalletAddress> = HashSet::new();
        extract_wallets(&logs, &operator_set, &mut wallets);
        Ok(wallets)
    }
}

// ── Parser helpers ────────────────────────────────────────────────────────────

/// Extract maker (topic[2]) and taker (topic[3]) from each log and insert
/// into `wallets`, skipping operator addresses. Logs with malformed topic
/// layouts (missing topic[2] / topic[3]) are silently dropped via
/// [`topic_to_wallet`] returning `None`.
fn extract_wallets(
    logs: &[Log],
    operator_set: &HashSet<WalletAddress>,
    wallets: &mut HashSet<WalletAddress>,
) {
    for log in logs {
        let topics = log.topics();
        for idx in [2usize, 3usize] {
            match topic_to_wallet(topics.get(idx)) {
                Some(addr) if !operator_set.contains(&addr) => {
                    wallets.insert(addr);
                }
                Some(_) => {
                    debug!(
                        topic_idx = idx,
                        "wallet enumeration: skipping operator address"
                    );
                }
                None => {}
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, LogData};
    use alloy::rpc::types::Log;

    use crate::contracts::{CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2};
    use crate::eth_logs::test_support::InMemoryChainLogFetcher;

    use super::*;

    fn w(b: u8) -> WalletAddress {
        let mut bytes = [0u8; 20];
        bytes[19] = b;
        WalletAddress(bytes)
    }

    fn wallet_topic(b: u8) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[31] = b;
        B256::from(bytes)
    }

    fn order_filled_log(topic0: B256, maker: u8, taker: u8) -> Log {
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

    fn malformed_log(topic0: B256) -> Log {
        let inner = alloy::primitives::Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![topic0], Bytes::new()),
        };
        Log {
            inner,
            ..Default::default()
        }
    }

    #[test]
    fn extract_wallets_collects_maker_and_taker() {
        let maker = w(0xaa);
        let taker = w(0xbb);
        let logs = vec![order_filled_log(TOPIC_ORDER_FILLED_V1, 0xaa, 0xbb)];
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
        let logs = vec![order_filled_log(TOPIC_ORDER_FILLED_V1, 0xaa, 0xcc)];
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
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0xaa, 0xbb),
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0xaa, 0xbb),
        ];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert_eq!(wallets.len(), 2);
        assert!(wallets.contains(&maker));
        assert!(wallets.contains(&taker));
    }

    /// Regression test for issue #179: the wallet-extractor must accept a
    /// V2-topic log without dropping the maker/taker. Topic[0] is metadata
    /// only — `extract_wallets` reads topic[2]/topic[3] regardless of version.
    #[test]
    fn extract_wallets_captures_v2_topic_log() {
        let maker = w(0xab);
        let taker = w(0xcd);
        let logs = vec![order_filled_log(TOPIC_ORDER_FILLED_V2, 0xab, 0xcd)];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert!(wallets.contains(&maker), "V2-topic maker must be captured");
        assert!(wallets.contains(&taker), "V2-topic taker must be captured");
    }

    #[test]
    fn extract_wallets_skips_malformed_topics() {
        let logs = vec![malformed_log(TOPIC_ORDER_FILLED_V1)];
        let operators = HashSet::new();
        let mut wallets = HashSet::new();
        extract_wallets(&logs, &operators, &mut wallets);
        assert!(wallets.is_empty());
    }

    /// `enumerate_chunk` returns the deduplicated wallet set from a single
    /// `ChainLogFetcher::get_logs` call, with operators filtered.
    #[tokio::test]
    async fn enumerate_chunk_returns_extracted_wallets() {
        let logs = vec![
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x01, 0x02),
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x03, 0x04),
        ];
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, logs);
        let config = EnumerationConfig {
            from_block: 100,
            to_block: 600,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);
        let wallets = enumerator
            .enumerate_chunk(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, 100, 600)
            .await
            .unwrap();
        assert_eq!(wallets.len(), 4);
        assert!(wallets.contains(&w(0x01)));
        assert!(wallets.contains(&w(0x02)));
        assert!(wallets.contains(&w(0x03)));
        assert!(wallets.contains(&w(0x04)));
        assert_eq!(enumerator.fetcher.last_call(), Some((100, 600)));
    }

    /// Provider error propagates as `EnumerationError::Provider`.
    #[tokio::test]
    async fn enumerate_chunk_propagates_provider_error() {
        let fetcher = InMemoryChainLogFetcher::get_logs_err(1_000_000, "boom");
        let config = EnumerationConfig {
            from_block: 100,
            to_block: 600,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);
        let result = enumerator
            .enumerate_chunk(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, 100, 600)
            .await;
        assert!(matches!(result, Err(EnumerationError::Provider(_))));
    }

    /// `enumerate` rejects an inverted range up front, without issuing any
    /// `get_logs` call.
    #[tokio::test]
    async fn invalid_config_from_gt_to_returns_error() {
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, Vec::new());
        let config = EnumerationConfig {
            from_block: 200,
            to_block: 100,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);
        let result = enumerator.enumerate().await;
        assert!(matches!(result, Err(EnumerationError::InvalidConfig(_))));
        assert_eq!(enumerator.fetcher.last_call(), None);
    }
}
