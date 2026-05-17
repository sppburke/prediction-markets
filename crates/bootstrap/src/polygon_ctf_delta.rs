//! Polygon CTF delta scanner — issue #176.
//!
//! Scans `OrderFilled` events across all four Polymarket exchange contracts
//! (`ALL_EXCHANGE_CONTRACTS`) to derive the set of wallets that traded
//! on-chain in `[from_block, to_block]`. The result is consumed by
//! `backfill::run_backfill` to narrow the per-day Polymarket API surface.
//!
//! - `to_block = None` → `fetcher.get_block_number() - confirmations` (256
//!   blocks ≈ 8.5 min on Polygon's 2 s blocktime; covers worst-case reorg).
//! - `from_block = None` → resolve from `POLYGON_CTF_BACKFILL_CURSOR_KEY`,
//!   falling back to `to_block - 43_200` (~24 h of Polygon blocks) on cold
//!   start so the first run scans a single day rather than the full chain.
//!
//! The cursor itself is NOT written here — [`scan_active_wallets`] returns
//! the target block in `DeltaScanResult.new_cursor`. The caller persists it
//! only after the broader pipeline succeeds.

use std::collections::HashSet;

use alloy::rpc::types::{Filter, Log};
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::{ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS};
use pe_source_onchain_polygon::{ChainLogFetcher, topic_to_wallet};

use crate::cache::WalletCache;
use crate::error::BootstrapError;

/// `source_cursor` key for the daily delta scan. Independent of
/// [`crate::polygon_ctf::POLYGON_CTF_CURSOR_KEY`] (which the historical
/// resolution scanner uses) so a failure in one doesn't block the other.
pub const POLYGON_CTF_BACKFILL_CURSOR_KEY: &str = "polygon_ctf_backfill_last_block";

/// Cold-start window (Polygon blocks ≈ 24 h at 2 s blocktime) used when no
/// cursor is present in `source_cursor`. Keeps the first run from scanning
/// the entire chain history.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub const COLD_START_LOOKBACK_BLOCKS: u64 = 43_200;

/// Result of a delta scan run.
///
/// `new_cursor` encodes the success/failure invariant in the type system:
/// `Some(to_block)` on success, `None` on scan failure. The caller pattern-matches
/// rather than checking `scan_error.is_none()` separately.
#[derive(Debug)]
pub struct DeltaScanResult {
    pub active_wallets: HashSet<WalletAddress>,
    pub new_cursor: Option<u64>,
    pub scan_error: Option<BootstrapError>,
}

/// Extract maker (topic[2]) + taker (topic[3]) addresses from a slice of
/// `OrderFilled` logs into a deduplicated set.
///
/// Pure helper — no I/O. Logs with malformed topic layouts (missing topic[2]
/// or topic[3]) are silently skipped via [`topic_to_wallet`] returning `None`.
pub fn extract_active_wallets(logs: &[Log]) -> HashSet<WalletAddress> {
    let mut wallets = HashSet::with_capacity(logs.len() * 2);
    for log in logs {
        let topics = log.topics();
        if let Some(maker) = topic_to_wallet(topics.get(2)) {
            wallets.insert(maker);
        }
        if let Some(taker) = topic_to_wallet(topics.get(3)) {
            wallets.insert(taker);
        }
    }
    wallets
}

/// Resolve the cold-start `from_block` given a `to_block`.
///
/// Returns `to_block.saturating_sub(COLD_START_LOOKBACK_BLOCKS)` — exposed so
/// unit tests can verify the contract without a cache.
#[must_use]
pub fn cold_start_from_block(to_block: u64) -> u64 {
    to_block.saturating_sub(COLD_START_LOOKBACK_BLOCKS)
}

/// Scan `OrderFilled` events across all exchange contracts in
/// `[from_block, to_block]` and return the deduplicated active-wallet set.
///
/// Behaviour:
/// 1. **Resolve `to_block`**: if `None`, calls `fetcher.get_block_number()`
///    and subtracts `confirmations` with saturating arithmetic. When the
///    caller passes `Some(X)`, `confirmations` is **ignored** — `X` is
///    treated as authoritative (the caller has already applied its own
///    finality buffer).
/// 2. **Resolve `from_block`**: if `None`, reads
///    [`POLYGON_CTF_BACKFILL_CURSOR_KEY`] from `source_cursor`; falls back
///    to [`cold_start_from_block`] when the cursor is absent.
/// 3. **Empty range short-circuit**: if `from_block > to_block`, returns a
///    successful empty scan (`new_cursor = Some(to_block)`). Mirrors the
///    `scan_resolutions` precedent.
/// 4. **Fetch logs**: builds a multi-address filter for all four exchange
///    contracts + every topic in `ALL_ORDER_FILLED_TOPICS` (V1 and V2) and
///    calls `fetcher.get_logs(...)`. Any RPC error is captured in
///    `scan_error` with `new_cursor = None`; the caller falls back to legacy
///    full-fetch this run.
/// 5. **Extract wallets**: via [`extract_active_wallets`] from `logs`.
/// 6. **Return**: `new_cursor = Some(to_block)` on success.
///
/// This function does NOT persist the cursor; it returns the target value
/// so the caller can advance it only after the broader pipeline succeeds.
pub async fn scan_active_wallets<F: ChainLogFetcher>(
    fetcher: &F,
    from_block: Option<u64>,
    to_block: Option<u64>,
    confirmations: u64,
    cache: &WalletCache,
) -> DeltaScanResult {
    let resolved_to = match to_block {
        Some(x) => x,
        None => match fetcher.get_block_number().await {
            Ok(head) => head.saturating_sub(confirmations),
            Err(e) => {
                tracing::warn!(error = %e, "polygon_ctf_delta: get_block_number failed");
                return DeltaScanResult {
                    active_wallets: HashSet::new(),
                    new_cursor: None,
                    scan_error: Some(BootstrapError::PolygonCtf {
                        message: e.to_string(),
                    }),
                };
            }
        },
    };

    let resolved_from = match from_block {
        Some(x) => x,
        None => cache
            .get_source_cursor(POLYGON_CTF_BACKFILL_CURSOR_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .map_or_else(
                || cold_start_from_block(resolved_to),
                |c| c.saturating_add(1),
            ),
    };

    if resolved_from > resolved_to {
        tracing::info!(
            from_block = resolved_from,
            to_block = resolved_to,
            "polygon_ctf_delta: empty range, advancing cursor only"
        );
        return DeltaScanResult {
            active_wallets: HashSet::new(),
            new_cursor: Some(resolved_to),
            scan_error: None,
        };
    }

    // Topic-0 OR-filter: alloy `event_signature` accepts `Into<Topic>`, and
    // `Topic = FilterSet<B256>` has `From<Vec<B256>>`. Passing every known
    // OrderFilled topic version captures V1 and V2 in a single RPC round-trip.
    // Issue #179: a single-topic filter silently missed ~98% of current
    // volume (V2 contracts) before this change.
    let filter = Filter::new()
        .address(ALL_EXCHANGE_CONTRACTS.to_vec())
        .event_signature(ALL_ORDER_FILLED_TOPICS.to_vec());

    tracing::info!(
        from_block = resolved_from,
        to_block = resolved_to,
        "polygon_ctf_delta: scanning OrderFilled events"
    );

    let logs = match fetcher.get_logs(filter, resolved_from, resolved_to).await {
        Ok(logs) => logs,
        Err(e) => {
            tracing::warn!(error = %e, "polygon_ctf_delta: get_logs failed");
            return DeltaScanResult {
                active_wallets: HashSet::new(),
                new_cursor: None,
                scan_error: Some(BootstrapError::PolygonCtf {
                    message: e.to_string(),
                }),
            };
        }
    };

    let active = extract_active_wallets(&logs);
    tracing::info!(
        from_block = resolved_from,
        to_block = resolved_to,
        logs = logs.len(),
        active_wallets = active.len(),
        "polygon_ctf_delta: scan complete"
    );

    DeltaScanResult {
        active_wallets: active,
        new_cursor: Some(resolved_to),
        scan_error: None,
    }
}

#[cfg(any(test, feature = "scenario"))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub mod test_support {
    //! In-memory [`ChainLogFetcher`] for inline tests + scenario tests.
    //!
    //! Mirrors the `InMemoryLookup` pattern in
    //! `pe_source_onchain_polygon::funder_discovery::tests`. Production code
    //! uses [`pe_source_onchain_polygon::AlloyChainLogFetcher`].

    use std::sync::Mutex;

    use alloy::rpc::types::{Filter, Log};
    use pe_source_onchain_polygon::{ChainLogFetcher, PolygonRpcError};

    /// In-memory fetcher returning canned responses.
    pub struct InMemoryChainLogFetcher {
        pub block_number: Result<u64, String>,
        pub logs: Result<Vec<Log>, String>,
        pub last_call: Mutex<Option<(u64, u64)>>,
    }

    impl InMemoryChainLogFetcher {
        pub fn ok(block: u64, logs: Vec<Log>) -> Self {
            Self {
                block_number: Ok(block),
                logs: Ok(logs),
                last_call: Mutex::new(None),
            }
        }

        pub fn block_number_err(msg: impl Into<String>) -> Self {
            Self {
                block_number: Err(msg.into()),
                logs: Ok(Vec::new()),
                last_call: Mutex::new(None),
            }
        }

        pub fn get_logs_err(block: u64, msg: impl Into<String>) -> Self {
            Self {
                block_number: Ok(block),
                logs: Err(msg.into()),
                last_call: Mutex::new(None),
            }
        }

        pub fn last_call(&self) -> Option<(u64, u64)> {
            *self.last_call.lock().unwrap()
        }
    }

    impl ChainLogFetcher for InMemoryChainLogFetcher {
        async fn get_block_number(&self) -> Result<u64, PolygonRpcError> {
            self.block_number
                .clone()
                .map_err(PolygonRpcError::GetBlockNumber)
        }

        async fn get_logs(
            &self,
            _filter: Filter,
            from: u64,
            to: u64,
        ) -> Result<Vec<Log>, PolygonRpcError> {
            *self.last_call.lock().unwrap() = Some((from, to));
            self.logs.clone().map_err(|msg| PolygonRpcError::GetLogs {
                from,
                to,
                message: msg,
            })
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, B256, Bytes, LogData};
    use alloy::rpc::types::Log;
    use pe_core_types::WalletAddress;
    use pe_source_onchain_polygon::contracts::{TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2};
    use tempfile::TempDir;
    use test_support::InMemoryChainLogFetcher;

    fn open_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    fn wallet_topic(byte: u8) -> B256 {
        let mut b = [0u8; 32];
        b[31] = byte;
        B256::from(b)
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
        // Only topic[0]; no maker/taker → topic_to_wallet returns None twice.
        let inner = alloy::primitives::Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![topic0], Bytes::new()),
        };
        Log {
            inner,
            ..Default::default()
        }
    }

    fn make_wallet(byte: u8) -> WalletAddress {
        let mut b = [0u8; 20];
        b[19] = byte;
        WalletAddress(b)
    }

    #[test]
    fn extract_active_wallets_two_logs_four_unique() {
        // Two OrderFilled logs with 4 distinct maker/taker addresses.
        let logs = vec![
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x01, 0x02),
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x03, 0x04),
        ];
        let set = extract_active_wallets(&logs);
        assert_eq!(set.len(), 4);
        assert!(set.contains(&make_wallet(0x01)));
        assert!(set.contains(&make_wallet(0x02)));
        assert!(set.contains(&make_wallet(0x03)));
        assert!(set.contains(&make_wallet(0x04)));
    }

    #[test]
    fn extract_active_wallets_deduplicates_repeated_addresses() {
        // Same maker/taker pair in 3 logs → set size 2, not 6.
        let logs = vec![
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x05, 0x06),
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x05, 0x06),
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x06, 0x05),
        ];
        let set = extract_active_wallets(&logs);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn extract_active_wallets_empty_input_empty_output() {
        let logs: Vec<Log> = Vec::new();
        assert!(extract_active_wallets(&logs).is_empty());
    }

    #[test]
    fn extract_active_wallets_skips_malformed_topics() {
        let logs = vec![malformed_log(TOPIC_ORDER_FILLED_V1)];
        assert!(extract_active_wallets(&logs).is_empty());
    }

    #[test]
    fn cold_start_from_block_subtracts_one_day() {
        assert_eq!(cold_start_from_block(100_000_000), 100_000_000 - 43_200);
        // saturating-sub: tiny block numbers floor at 0.
        assert_eq!(cold_start_from_block(100), 0);
    }

    #[tokio::test]
    async fn scan_success_returns_active_wallets_and_new_cursor() {
        let (_dir, cache) = open_cache();
        let logs = vec![order_filled_log(TOPIC_ORDER_FILLED_V1, 0x10, 0x11)];
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, logs);
        let result = scan_active_wallets(&fetcher, Some(900_000), Some(950_000), 256, &cache).await;
        assert_eq!(result.active_wallets.len(), 2);
        assert_eq!(result.new_cursor, Some(950_000));
        assert!(result.scan_error.is_none());
    }

    /// Regression test for #179: a V2-topic log must be captured by the
    /// delta-scan extractor (both V1 and V2 use topic[2]/topic[3] for
    /// maker/taker — the change is purely the topic-0 filter). Without this
    /// test, a future filter regression that silently drops V2 logs could
    /// recur (the production bug was exactly this).
    #[tokio::test]
    async fn scan_captures_v2_topic_orderfilled_logs() {
        let (_dir, cache) = open_cache();
        let logs = vec![
            order_filled_log(TOPIC_ORDER_FILLED_V1, 0x21, 0x22),
            order_filled_log(TOPIC_ORDER_FILLED_V2, 0x23, 0x24),
        ];
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, logs);
        let result = scan_active_wallets(&fetcher, Some(900_000), Some(950_000), 256, &cache).await;
        assert_eq!(
            result.active_wallets.len(),
            4,
            "must include both V1- and V2-topic maker/taker addresses"
        );
        assert!(result.active_wallets.contains(&make_wallet(0x23)));
        assert!(result.active_wallets.contains(&make_wallet(0x24)));
    }

    #[tokio::test]
    async fn scan_empty_logs_still_advances_cursor() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, Vec::new());
        let result = scan_active_wallets(&fetcher, Some(900_000), Some(950_000), 256, &cache).await;
        assert!(result.active_wallets.is_empty());
        assert_eq!(result.new_cursor, Some(950_000));
        assert!(result.scan_error.is_none());
    }

    #[tokio::test]
    async fn scan_block_number_failure_returns_none_cursor() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::block_number_err("rpc down");
        // `to_block = None` forces the chain-head fetch path → error.
        let result = scan_active_wallets(&fetcher, Some(900_000), None, 256, &cache).await;
        assert!(result.active_wallets.is_empty());
        assert!(result.new_cursor.is_none());
        assert!(result.scan_error.is_some());
    }

    #[tokio::test]
    async fn scan_get_logs_failure_returns_none_cursor() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::get_logs_err(1_000_000, "log limit exceeded");
        let result = scan_active_wallets(&fetcher, Some(900_000), Some(950_000), 256, &cache).await;
        assert!(result.active_wallets.is_empty());
        assert!(result.new_cursor.is_none());
        assert!(result.scan_error.is_some());
    }

    #[tokio::test]
    async fn scan_cold_start_from_block_derives_from_to_minus_43200() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, Vec::new());
        // from_block=None + no cursor → from = to - 43_200
        let result = scan_active_wallets(&fetcher, None, Some(950_000), 256, &cache).await;
        assert_eq!(result.new_cursor, Some(950_000));
        // The fetcher recorded which from/to it was called with.
        assert_eq!(fetcher.last_call(), Some((950_000 - 43_200, 950_000)));
    }

    #[tokio::test]
    async fn scan_supplied_to_block_ignores_confirmations() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::ok(999_999, Vec::new());
        // to_block=Some(X) ignores confirmations: fetcher gets X, NOT X-256.
        let result = scan_active_wallets(&fetcher, Some(900_000), Some(950_000), 256, &cache).await;
        assert_eq!(result.new_cursor, Some(950_000));
        assert_eq!(fetcher.last_call(), Some((900_000, 950_000)));
    }

    #[tokio::test]
    async fn scan_empty_range_short_circuits_without_calling_get_logs() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, Vec::new());
        // from > to → empty range, cursor still Some(to), no get_logs call.
        let result = scan_active_wallets(&fetcher, Some(950_001), Some(950_000), 256, &cache).await;
        assert!(result.active_wallets.is_empty());
        assert_eq!(result.new_cursor, Some(950_000));
        assert!(result.scan_error.is_none());
        assert_eq!(fetcher.last_call(), None);
    }

    #[tokio::test]
    async fn scan_resolves_to_block_from_chain_head_minus_confirmations() {
        let (_dir, cache) = open_cache();
        let fetcher = InMemoryChainLogFetcher::ok(1_000_000, Vec::new());
        // to_block=None → fetcher head minus confirmations: 1_000_000 - 256 = 999_744
        let result = scan_active_wallets(&fetcher, Some(900_000), None, 256, &cache).await;
        assert_eq!(result.new_cursor, Some(999_744));
        assert_eq!(fetcher.last_call(), Some((900_000, 999_744)));
    }
}
