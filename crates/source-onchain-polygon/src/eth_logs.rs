//! Generic `eth_getLogs` primitives lifted from `pe-bootstrap` (issue #176).
//!
//! Two consumers exist in the workspace:
//! - `pe_bootstrap::polygon_ctf::scan_resolutions` — scans CTF
//!   `ConditionResolution` events to populate `market_resolutions`.
//! - `pe_bootstrap::polygon_ctf_delta::scan_active_wallets` — scans
//!   `OrderFilled` events across all exchange contracts to build the
//!   on-chain wallet activity set for the daily delta-backfill flow.
//!
//! Both delegate the bisect-on-cap retry to [`eth_get_logs_bisect`] and (in
//! the future, optionally) the chain-head + log fetch to the
//! [`ChainLogFetcher`] trait. The trait abstraction mirrors the
//! [`crate::FunderLookup`] precedent: production code wraps an alloy
//! [`Provider`] in [`AlloyChainLogFetcher`]; tests substitute an in-memory
//! impl so unit and scenario tests don't need a real Polygon RPC.

use alloy::providers::Provider;
use alloy::rpc::types::{BlockNumberOrTag, Filter, Log};

/// Errors emitted by the generic Polygon-RPC primitives in this module.
///
/// Library-style error (downstream crates consume this); follows the same
/// `thiserror` pattern as [`crate::FunderDiscoveryError`].
#[derive(Debug, thiserror::Error)]
pub enum PolygonRpcError {
    #[error("eth_getLogs [{from}, {to}]: {message}")]
    GetLogs { from: u64, to: u64, message: String },
    #[error("eth_blockNumber: {0}")]
    GetBlockNumber(String),
}

/// Issue a single `eth_getLogs` request for `[from, to]`, bisecting on
/// "response too large" errors. Cap-error detection is heuristic (substring
/// match on the error string) because providers return non-standard JSON-RPC
/// error codes; the fallback halves the range until one of:
///   - the request succeeds,
///   - the range reaches `min_chunk` blocks and still fails → propagate the
///     error so the caller can lower `chunk_blocks` or switch RPC tiers.
///
/// `filter` is supplied **without block range set** — the function clones it
/// and injects `.from_block(...)` / `.to_block(...)` for each request,
/// including the recursive halves. This keeps the caller's intent (which
/// addresses, which topic0) decoupled from the chunking strategy.
pub async fn eth_get_logs_bisect<P: Provider>(
    provider: &P,
    filter: Filter,
    from: u64,
    to: u64,
    min_chunk: u64,
) -> Result<Vec<Log>, PolygonRpcError> {
    let span = to.saturating_sub(from).saturating_add(1);
    let req_filter = filter
        .clone()
        .from_block(BlockNumberOrTag::Number(from))
        .to_block(BlockNumberOrTag::Number(to));
    match provider.get_logs(&req_filter).await {
        Ok(logs) => Ok(logs),
        Err(e) => {
            if span <= min_chunk {
                return Err(PolygonRpcError::GetLogs {
                    from,
                    to,
                    message: format!("min-chunk floor reached: {e}"),
                });
            }
            let msg = e.to_string().to_lowercase();
            // Heuristic: response-too-large / range-too-wide / log-limit-exceeded.
            let is_cap = msg.contains("too large")
                || msg.contains("too many")
                || msg.contains("response size")
                || msg.contains("limit exceeded")
                || msg.contains("range");
            if !is_cap {
                return Err(PolygonRpcError::GetLogs {
                    from,
                    to,
                    message: e.to_string(),
                });
            }
            tracing::warn!(
                from,
                to,
                error = %e,
                "polygon eth_getLogs: response cap hit, bisecting"
            );
            let mid = from.saturating_add(span / 2);
            let mut left = Box::pin(eth_get_logs_bisect(
                provider,
                filter.clone(),
                from,
                mid.saturating_sub(1),
                min_chunk,
            ))
            .await?;
            let right = Box::pin(eth_get_logs_bisect(provider, filter, mid, to, min_chunk)).await?;
            left.extend(right);
            Ok(left)
        }
    }
}

/// Testability seam for code that scans Polygon logs (issue #176).
///
/// Production code uses [`AlloyChainLogFetcher`], which wraps an alloy
/// [`Provider`] and bisects internally. Tests substitute an in-memory impl
/// holding canned responses — mirrors the [`crate::FunderLookup`] + in-memory
/// stub pattern.
pub trait ChainLogFetcher {
    fn get_block_number(
        &self,
    ) -> impl std::future::Future<Output = Result<u64, PolygonRpcError>> + Send;
    fn get_logs(
        &self,
        filter: Filter,
        from: u64,
        to: u64,
    ) -> impl std::future::Future<Output = Result<Vec<Log>, PolygonRpcError>> + Send;
}

/// Production [`ChainLogFetcher`] backed by an alloy [`Provider`].
///
/// `min_chunk` is the floor below which `eth_get_logs_bisect` propagates the
/// underlying RPC error instead of bisecting further — set this to 1 in
/// production unless the upstream is known to return cap errors on
/// individual blocks (a paid-tier red flag).
pub struct AlloyChainLogFetcher<P> {
    pub provider: P,
    pub min_chunk: u64,
}

impl<P: Provider + Clone + Send + Sync> ChainLogFetcher for AlloyChainLogFetcher<P> {
    async fn get_block_number(&self) -> Result<u64, PolygonRpcError> {
        self.provider
            .get_block_number()
            .await
            .map_err(|e| PolygonRpcError::GetBlockNumber(e.to_string()))
    }

    async fn get_logs(
        &self,
        filter: Filter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, PolygonRpcError> {
        eth_get_logs_bisect(&self.provider, filter, from, to, self.min_chunk).await
    }
}
