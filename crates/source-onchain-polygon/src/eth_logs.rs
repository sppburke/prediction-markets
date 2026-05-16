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

use std::time::Duration;

use alloy::providers::Provider;
use alloy::rpc::types::{BlockNumberOrTag, Filter, Log};

/// Maximum per-attempt backoff (seconds) when an `eth_getLogs` request hits
/// HTTP 429 / rate-limit. Exponential backoff is `1 → 2 → 4 → 8 → 16 → 32`s
/// before the retry loop gives up and propagates the error to the caller
/// (which falls back to legacy full-fetch in `backfill::run_backfill`).
///
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const RATE_LIMIT_MAX_BACKOFF_SECS: u64 = 32;

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

/// Issue an `eth_getLogs` request for `[from, to]` with exponential-backoff
/// retry on HTTP 429 (rate limit) and bisect on "response too large".
///
/// Error handling, in order:
///
/// 1. **Rate-limit retry**: if the underlying error matches a 429 / "compute
///    units" / "throttle" / "too many requests" heuristic, sleep
///    exponentially (1, 2, 4, 8, 16, 32 s) and retry the same range. After
///    backoff exceeds [`RATE_LIMIT_MAX_BACKOFF_SECS`], propagate to the
///    caller (which falls back to legacy fetch in production). Mirrors the
///    pattern from `funder_discovery.rs::EthGetLogsLookup` but bounded.
///
/// 2. **Cap-on-response-size**: heuristic substring match (providers return
///    non-standard JSON-RPC error codes). Halves the range and retries until
///    either it succeeds or the range reaches `min_chunk` blocks (then the
///    error propagates so the caller can lower `chunk_blocks` or switch RPC
///    tiers).
///
/// 3. **Other errors**: propagated unchanged.
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

    // Step 1 — rate-limit retry. Bounded exponential backoff; the loop exits
    // either via successful `Ok(logs)` (returned immediately) or by breaking
    // out with the final error for the cap-vs-other classification below.
    let mut backoff_secs: u64 = 1;
    let final_err = loop {
        match provider.get_logs(&req_filter).await {
            Ok(logs) => return Ok(logs),
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                let is_rate_limit = msg.contains("429")
                    || msg.contains("compute units")
                    || msg.contains("throttle")
                    || msg.contains("too many requests")
                    || msg.contains("rate limit");
                if is_rate_limit && backoff_secs <= RATE_LIMIT_MAX_BACKOFF_SECS {
                    tracing::warn!(
                        from,
                        to,
                        backoff_secs,
                        error = %e,
                        "polygon eth_getLogs: rate-limited, retrying with backoff"
                    );
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = backoff_secs.saturating_mul(2);
                    continue;
                }
                break e;
            }
        }
    };

    // Step 2 — cap detection: if span > min_chunk and the error looks like a
    // response-size cap, recurse on halves; otherwise propagate.
    let e = final_err;
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
