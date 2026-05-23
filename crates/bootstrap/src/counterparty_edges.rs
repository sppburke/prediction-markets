//! On-chain `OrderFilled` scanner — populates `counterparty_edges` for the
//! §5 PnL-inflation reconciliation (issue #207 Slice 1b).
//!
//! Scans both V1 and V2 CTF Exchange contracts (`ALL_EXCHANGE_CONTRACTS`) for
//! the V1/V2 `OrderFilled` topics (`ALL_ORDER_FILLED_TOPICS`) in one combined
//! filter, decoding each leg via `pe_source_onchain_polygon::decode_order_filled`
//! (PR #226, Slice 1a) and batch-upserting into the `counterparty_edges` table.
//!
//! Resumability: progress is checkpointed in `source_cursor.counterparty_edges_last_block`
//! after every successfully-processed chunk. The default starting block when no
//! cursor exists is `CTF_EXCHANGE_V1_DEPLOY_BLOCK` (= 33_605_403); the default
//! ending block is the current chain head, snapshotted at entry so a long
//! backfill does not drift forward.
//!
//! Generic over `ChainLogFetcher` so the production binary uses
//! `AlloyChainLogFetcher<HttpProvider>` while scenario tests inject an
//! in-memory fixture. The bisect-on-cap retry and rate-limit backoff live
//! inside `eth_get_logs_bisect`, reused via that trait.

use alloy::providers::ProviderBuilder;
use alloy::rpc::types::Filter;
use tracing::info;

use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, CTF_EXCHANGE_V1_DEPLOY_BLOCK,
};
use pe_source_onchain_polygon::{
    AlloyChainLogFetcher, ChainLogFetcher, DecodedOrderFilled, decode_order_filled,
};

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;

/// `source_cursor` key for the OrderFilled scan checkpoint. Value is the last
/// successfully-scanned block as a decimal string.
pub const COUNTERPARTY_EDGES_CURSOR_KEY: &str = "counterparty_edges_last_block";

/// Default block-range width per `eth_getLogs` request. Matches
/// `pe_source_onchain_polygon::wallet_enumeration::SCAN_CHUNK_BLOCKS` (tuned
/// down from 500k → 50k on 2026-05-17 to bound bisect recursion depth).
const CHUNK_BLOCKS: u64 = 50_000;

/// Minimum chunk size for the bisect-on-cap fallback in `eth_get_logs_bisect`.
/// One block is the strict floor (same as `polygon_ctf::MIN_CHUNK_BLOCKS`).
const MIN_CHUNK_BLOCKS: u64 = 1;

/// Outcome of one `scan_counterparty_edges` invocation.
#[derive(Debug, Default, Clone, Copy)]
pub struct CounterpartyEdgesReport {
    /// Rows written (deduplicates against existing `(tx_hash, log_index)` keys
    /// via `INSERT OR REPLACE`; this is the count of successfully-decoded legs,
    /// not the net new-row count).
    pub edges_upserted: usize,
    /// Logs that decoded to `None` (missing block metadata, unknown topic0,
    /// or ABI decode failure). Tracked separately so a malformed-log spike
    /// surfaces in the report rather than silently dropping data.
    pub edges_skipped: usize,
    /// Number of chunks (`CHUNK_BLOCKS`-sized windows) fetched.
    pub chunks_scanned: usize,
    /// Inclusive start block of the scan (cursor + 1 on resume; `CTF_EXCHANGE_V1_DEPLOY_BLOCK`
    /// on first run).
    pub from_block: u64,
    /// Inclusive end block (`Some(b)` argument on resume; chain head otherwise).
    pub to_block: u64,
}

/// Build the production scanner (HTTP `AlloyChainLogFetcher`) and run it.
///
/// Mirrors the `funder` subcommand's RPC construction in `main.rs` — same
/// `polygon_rpc_url` config field, same `ProviderBuilder::new().connect_http(...)`
/// pattern.
pub async fn run_counterparty_edges(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<CounterpartyEdgesReport, BootstrapError> {
    let rpc_url = config.polygon_rpc_url.as_ref().ok_or_else(|| {
        BootstrapError::MissingEnv("PE_POLYGON_HTTP_URL or PE_BOOTSTRAP_POLYGON_RPC_URL".to_owned())
    })?;
    let http_url = rpc_url
        .parse::<reqwest::Url>()
        .map_err(|e| BootstrapError::PolygonCtf {
            message: format!("invalid polygon_rpc_url: {e}"),
        })?;
    let fetcher = AlloyChainLogFetcher {
        provider: ProviderBuilder::new().connect_http(http_url),
        min_chunk: MIN_CHUNK_BLOCKS,
    };
    scan_counterparty_edges(
        &fetcher,
        cache,
        CTF_EXCHANGE_V1_DEPLOY_BLOCK,
        None,
        CHUNK_BLOCKS,
    )
    .await
}

/// Scan `OrderFilled` logs in `[from, to]` and upsert each decoded leg.
///
/// `from_block_default` is used only when the cursor is absent (first run);
/// otherwise the scan resumes from `cursor + 1`. `to_block_override = Some(b)`
/// pins the upper bound (used by tests); `None` fetches the current chain head
/// once via `get_block_number` and pins it for the entire scan.
pub async fn scan_counterparty_edges<F: ChainLogFetcher>(
    fetcher: &F,
    cache: &mut WalletCache,
    from_block_default: u64,
    to_block_override: Option<u64>,
    chunk_blocks: u64,
) -> Result<CounterpartyEdgesReport, BootstrapError> {
    let from_block = cache
        .get_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY)
        .and_then(|s| s.parse::<u64>().ok())
        .map(|n| n.saturating_add(1))
        .unwrap_or(from_block_default);
    let to_block = match to_block_override {
        Some(b) => b,
        None => fetcher
            .get_block_number()
            .await
            .map_err(|e| BootstrapError::PolygonCtf {
                message: format!("get_block_number: {e}"),
            })?,
    };

    info!(
        from_block,
        to_block, chunk_blocks, "counterparty-edges: starting scan"
    );

    if from_block > to_block {
        info!(
            from_block,
            to_block, "counterparty-edges: nothing to scan (cursor already at head)"
        );
        return Ok(CounterpartyEdgesReport {
            from_block,
            to_block,
            ..CounterpartyEdgesReport::default()
        });
    }

    // One filter for both V1 + V2 topics across all 4 exchange contracts — the
    // bisect retry inside `eth_get_logs_bisect` halves the range on cap errors.
    let filter = Filter::new()
        .address(ALL_EXCHANGE_CONTRACTS.to_vec())
        .event_signature(ALL_ORDER_FILLED_TOPICS.to_vec());

    let mut edges_upserted = 0usize;
    let mut edges_skipped = 0usize;
    let mut chunks_scanned = 0usize;

    let mut block = from_block;
    while block <= to_block {
        let chunk_to = block.saturating_add(chunk_blocks - 1).min(to_block);
        let logs = fetcher
            .get_logs(filter.clone(), block, chunk_to)
            .await
            .map_err(|e| BootstrapError::PolygonCtf {
                message: format!("eth_getLogs [{block}, {chunk_to}]: {e}"),
            })?;

        let mut batch = Vec::with_capacity(logs.len());
        for log in &logs {
            match decode_order_filled(log) {
                Some(d) => batch.push(decoded_to_row(d)),
                None => edges_skipped += 1,
            }
        }
        let batch_len = batch.len();
        if !batch.is_empty() {
            cache.upsert_counterparty_edges_batch(&batch)?;
        }
        edges_upserted += batch_len;

        cache.set_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY, &chunk_to.to_string())?;
        chunks_scanned += 1;

        // Progress every 50 chunks (~2.5M blocks at 50k each).
        if chunks_scanned.is_multiple_of(50) {
            info!(
                edges_upserted,
                edges_skipped,
                chunks_scanned,
                current_block = chunk_to,
                "counterparty-edges: scan progress"
            );
        }

        block = chunk_to.saturating_add(1);
    }

    info!(
        edges_upserted,
        edges_skipped, chunks_scanned, from_block, to_block, "counterparty-edges: scan complete"
    );

    Ok(CounterpartyEdgesReport {
        edges_upserted,
        edges_skipped,
        chunks_scanned,
        from_block,
        to_block,
    })
}

/// Project a [`DecodedOrderFilled`] into the 14-tuple shape expected by
/// [`WalletCache::upsert_counterparty_edges_batch`].
///
/// All identifiers normalise to `0x`-prefixed lowercase hex (matching the
/// rest of the schema). Block numbers and log indices use `i64::try_from`
/// to surface any 2^63-overflow as a clamp to `i64::MAX` rather than a panic
/// (Polygon block height is ~74M as of 2026, ~290 billion years from the limit).
#[allow(clippy::type_complexity)]
fn decoded_to_row(
    d: DecodedOrderFilled,
) -> (
    String,
    i64,
    i64,
    i64,
    String,
    i64,
    String,
    String,
    String,
    Option<String>,
    Option<i64>,
    String,
    String,
    String,
) {
    (
        format!("{:#x}", d.tx_hash),
        i64::try_from(d.log_index).unwrap_or(i64::MAX),
        i64::try_from(d.block_number).unwrap_or(i64::MAX),
        d.block_ts_unix,
        format!("{:#x}", d.contract_addr),
        i64::from(d.contract_version),
        format!("{:#x}", d.maker),
        format!("{:#x}", d.taker),
        d.maker_asset_id_dec,
        d.taker_asset_id_dec,
        d.side.map(i64::from),
        d.maker_amount_raw.to_string(),
        d.taker_amount_raw.to_string(),
        d.fee_raw.to_string(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use alloy::primitives::{Address, B256, Bytes, LogData, U256};
    use alloy::rpc::types::Log as RpcLog;
    use pe_source_onchain_polygon::PolygonRpcError;
    use pe_source_onchain_polygon::contracts::{
        CTF_EXCHANGE_V1, CTF_EXCHANGE_V2, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
    };
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// In-memory `ChainLogFetcher`: serves a fixed list of logs for any
    /// `[from, to]` range that overlaps a per-log block, and records every call
    /// so tests can assert on the chunk windows the scanner actually requested.
    struct FixedFetcher {
        head: u64,
        logs: Vec<RpcLog>,
        calls: Mutex<Vec<(u64, u64)>>,
    }

    impl FixedFetcher {
        fn new(head: u64, logs: Vec<RpcLog>) -> Self {
            Self {
                head,
                logs,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl ChainLogFetcher for FixedFetcher {
        async fn get_block_number(&self) -> Result<u64, PolygonRpcError> {
            Ok(self.head)
        }
        async fn get_logs(
            &self,
            _filter: Filter,
            from: u64,
            to: u64,
        ) -> Result<Vec<RpcLog>, PolygonRpcError> {
            self.calls.lock().unwrap().push((from, to));
            Ok(self
                .logs
                .iter()
                .filter(|l| {
                    let b = l.block_number.unwrap_or(0);
                    from <= b && b <= to
                })
                .cloned()
                .collect())
        }
    }

    fn addr(byte: u8) -> Address {
        let mut bytes = [0u8; 20];
        bytes[19] = byte;
        Address::from(bytes)
    }

    fn address_to_topic(a: Address) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(a.as_slice());
        B256::from(bytes)
    }

    /// Build a synthetic V1 OrderFilled log with the given block + log index.
    /// Data payload: makerAssetId | takerAssetId | makerAmount | takerAmount | fee.
    #[allow(clippy::too_many_arguments)]
    fn v1_log(
        contract: Address,
        maker: Address,
        taker: Address,
        maker_asset_id: U256,
        taker_asset_id: U256,
        block_number: u64,
        log_index: u64,
        block_ts_secs: u64,
        tx_hash_byte: u8,
    ) -> RpcLog {
        let mut data = Vec::with_capacity(5 * 32);
        for v in [
            maker_asset_id,
            taker_asset_id,
            U256::from(1_000_000u64),
            U256::from(2_000_000u64),
            U256::from(500u64),
        ] {
            data.extend_from_slice(&v.to_be_bytes::<32>());
        }
        let inner = alloy::primitives::Log {
            address: contract,
            data: LogData::new_unchecked(
                vec![
                    TOPIC_ORDER_FILLED_V1,
                    B256::ZERO,
                    address_to_topic(maker),
                    address_to_topic(taker),
                ],
                Bytes::from(data),
            ),
        };
        RpcLog {
            inner,
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: Some(block_ts_secs),
            transaction_hash: Some(B256::repeat_byte(tx_hash_byte)),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    /// Build a synthetic V2 OrderFilled log. Data payload: side | tokenId |
    /// makerAmount | takerAmount | fee | builder | sweepBuilder.
    #[allow(clippy::too_many_arguments)]
    fn v2_log(
        contract: Address,
        maker: Address,
        taker: Address,
        side: u8,
        token_id: U256,
        block_number: u64,
        log_index: u64,
        block_ts_secs: u64,
        tx_hash_byte: u8,
    ) -> RpcLog {
        let mut data = Vec::with_capacity(7 * 32);
        data.extend_from_slice(&U256::from(side).to_be_bytes::<32>());
        data.extend_from_slice(&token_id.to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(7u64).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(8u64).to_be_bytes::<32>());
        data.extend_from_slice(&U256::from(9u64).to_be_bytes::<32>());
        data.extend_from_slice(B256::ZERO.as_slice());
        data.extend_from_slice(B256::ZERO.as_slice());
        let inner = alloy::primitives::Log {
            address: contract,
            data: LogData::new_unchecked(
                vec![
                    TOPIC_ORDER_FILLED_V2,
                    B256::ZERO,
                    address_to_topic(maker),
                    address_to_topic(taker),
                ],
                Bytes::from(data),
            ),
        };
        RpcLog {
            inner,
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: Some(block_ts_secs),
            transaction_hash: Some(B256::repeat_byte(tx_hash_byte)),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn run(
        fetcher: &FixedFetcher,
        cache: &mut WalletCache,
        from: u64,
        to: Option<u64>,
        chunk: u64,
    ) -> Result<CounterpartyEdgesReport, BootstrapError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(scan_counterparty_edges(fetcher, cache, from, to, chunk))
    }

    #[test]
    fn scan_persists_v1_and_v2_legs_and_advances_cursor() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();

        let v1 = v1_log(
            CTF_EXCHANGE_V1,
            addr(0xAA),
            addr(0xBB),
            U256::from(111u64),
            U256::from(222u64),
            100,
            0,
            1_700_000_000,
            0xD1,
        );
        let v2 = v2_log(
            CTF_EXCHANGE_V2,
            addr(0xAA),
            addr(0xCC),
            1,
            U256::from(999_999u64),
            150,
            0,
            1_700_000_001,
            0xD2,
        );
        let fetcher = FixedFetcher::new(200, vec![v1, v2]);

        let report = run(&fetcher, &mut cache, 50, None, 100).expect("scan");
        assert_eq!(report.edges_upserted, 2);
        assert_eq!(report.edges_skipped, 0);
        assert_eq!(report.from_block, 50);
        assert_eq!(report.to_block, 200);

        assert_eq!(cache.counterparty_edge_count(), 2);
        assert_eq!(cache.counterparty_edges_max_block(), Some(150));
        // Cursor must advance to the last scanned block (200, not 150).
        let cursor = cache
            .get_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY)
            .unwrap();
        assert_eq!(cursor, "200");
    }

    #[test]
    fn scan_resumes_from_cursor_plus_one() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .set_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY, "120")
            .unwrap();

        let pre_cursor = v1_log(
            CTF_EXCHANGE_V1,
            addr(0xAA),
            addr(0xBB),
            U256::from(1u64),
            U256::from(2u64),
            110,
            0,
            1,
            0xAA,
        );
        let post_cursor = v1_log(
            CTF_EXCHANGE_V1,
            addr(0xAA),
            addr(0xBB),
            U256::from(3u64),
            U256::from(4u64),
            150,
            0,
            2,
            0xBB,
        );
        let fetcher = FixedFetcher::new(200, vec![pre_cursor, post_cursor]);

        let report = run(&fetcher, &mut cache, 0, None, 100).expect("scan");
        assert_eq!(
            report.from_block, 121,
            "must start at cursor+1, not default"
        );
        assert_eq!(report.edges_upserted, 1, "only post-cursor log is scanned");

        let calls = fetcher.calls.lock().unwrap();
        assert!(
            calls.iter().all(|(f, _)| *f >= 121),
            "no chunk may begin below cursor+1; got {calls:?}"
        );
    }

    #[test]
    fn scan_with_no_logs_still_advances_cursor() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        let fetcher = FixedFetcher::new(150, Vec::new());

        let report = run(&fetcher, &mut cache, 100, None, 50).expect("scan");
        assert_eq!(report.edges_upserted, 0);
        assert_eq!(report.edges_skipped, 0);
        // Empty range still produces cursor — next run can fast-skip.
        let cursor = cache
            .get_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY)
            .unwrap();
        assert_eq!(cursor, "150");
    }

    #[test]
    fn scan_noop_when_cursor_already_at_head() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        cache
            .set_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY, "200")
            .unwrap();
        let fetcher = FixedFetcher::new(200, Vec::new());

        let report = run(&fetcher, &mut cache, 0, None, 100).expect("scan");
        assert_eq!(report.chunks_scanned, 0);
        assert_eq!(fetcher.calls.lock().unwrap().len(), 0);
        // Cursor unchanged.
        assert_eq!(
            cache
                .get_source_cursor(COUNTERPARTY_EDGES_CURSOR_KEY)
                .as_deref(),
            Some("200")
        );
    }

    #[test]
    fn scan_chunks_the_range_in_chunk_blocks_windows() {
        // 10 blocks, chunk=4 ⇒ [0..=3], [4..=7], [8..=10]: 3 chunks.
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
        let fetcher = FixedFetcher::new(10, Vec::new());

        let report = run(&fetcher, &mut cache, 0, Some(10), 4).expect("scan");
        assert_eq!(report.chunks_scanned, 3);
        let calls = fetcher.calls.lock().unwrap();
        assert_eq!(*calls, vec![(0, 3), (4, 7), (8, 10)]);
    }

    #[test]
    fn scan_skips_malformed_logs_without_aborting() {
        // A log missing block_timestamp must be silently skipped (decoder
        // returns None), letting a single bad row not kill a multi-hour scan.
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();

        let mut bad = v1_log(
            CTF_EXCHANGE_V1,
            addr(0xAA),
            addr(0xBB),
            U256::from(1u64),
            U256::from(2u64),
            100,
            0,
            1,
            0xAA,
        );
        bad.block_timestamp = None; // ← decoder will return None for this log
        let good = v1_log(
            CTF_EXCHANGE_V1,
            addr(0xAA),
            addr(0xBB),
            U256::from(3u64),
            U256::from(4u64),
            120,
            0,
            1,
            0xBB,
        );
        let fetcher = FixedFetcher::new(200, vec![bad, good]);

        let report = run(&fetcher, &mut cache, 0, None, 100).expect("scan");
        assert_eq!(report.edges_upserted, 1);
        assert_eq!(report.edges_skipped, 1);
    }
}
