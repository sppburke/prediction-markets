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
                let msg = e.to_string();
                if let Some(kind) = classify_transient_error(&msg)
                    && backoff_secs <= RATE_LIMIT_MAX_BACKOFF_SECS
                {
                    tracing::warn!(
                        from,
                        to,
                        backoff_secs,
                        kind = kind.as_str(),
                        error = %e,
                        "polygon eth_getLogs: transient failure, retrying with backoff"
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

/// Classification of which transient error class a `Provider::get_logs`
/// failure falls into. Used by [`eth_get_logs_bisect`] to decide whether
/// to retry with exponential backoff vs propagate immediately. `None`
/// means "not transient — propagate to caller for cap-vs-other classification."
///
/// The `as_str()` rendering goes into the tracing label so operators can
/// distinguish rate-limit-induced waits from decode-flake-induced waits in
/// production dashboards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransientErrorKind {
    /// Provider-side throttling: HTTP 429, "compute units exceeded", "throttle",
    /// "too many requests", "rate limit". Backoff is the canonical mitigation.
    RateLimit,
    /// Client-side deserialization failure on an apparently-2xx HTTP response:
    /// truncated stream, gateway 5xx body served as HTML/text, mid-response TCP
    /// reset, JSON parse hitting EOF or an unexpected token. Observed in
    /// production 2026-05-17 during a Polymarket V2 dense-region sweep —
    /// Alchemy occasionally served partial responses that alloy's JSON-RPC
    /// client could not parse. Retry typically clears it.
    DecodeError,
    /// Transport-level mid-stream failure: TCP RST mid-request, write to a
    /// closed socket (`broken pipe`), upstream gateway closing a long-running
    /// query (`connection closed before message completed`), or client-side
    /// request timeout (reqwest `operation timed out` / `request timeout`).
    /// Statistically common on long-running sweeps against a remote provider;
    /// retrying typically succeeds on the next attempt. Issue #191.
    TransportError,
}

impl TransientErrorKind {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "rate-limited",
            Self::DecodeError => "decode-error",
            Self::TransportError => "transport-error",
        }
    }
}

/// Classify an alloy `Provider` error message as a transient retry-worthy
/// failure, or `None` if the caller should propagate / bisect instead.
///
/// Case-insensitive substring match against the rendered error. Keep new
/// patterns narrow — false-positive transient classifications cause silent
/// indefinite retry on errors that should fail fast (e.g. config issues).
///
/// # Substring-collision discipline (issue #191)
///
/// New substrings must be checked against the cap-hit branch in
/// [`eth_get_logs_bisect`] (matches `"too large"`, `"too many"`,
/// `"response size"`, `"limit exceeded"`, `"range"`). A substring that
/// matches BOTH a transient pattern AND a cap-hit pattern causes the retry
/// loop to fire FIRST, retrying the same too-large range up to 6×32s
/// before propagating — the bisect branch never gets to halve the range.
///
/// **Concretely**: bare `"timeout"` is EXCLUDED because Alchemy's
/// `"Query timeout exceeded. Consider reducing your block range."` cap-hit
/// error contains it; the narrower `"timed out"` is included instead
/// (different conjugation, no collision). Verified against the production
/// Alchemy error logged 2026-05-17T20:21:39Z.
#[must_use]
pub(crate) fn classify_transient_error(error_message: &str) -> Option<TransientErrorKind> {
    let msg = error_message.to_lowercase();
    if msg.contains("429")
        || msg.contains("compute units")
        || msg.contains("throttle")
        || msg.contains("too many requests")
        || msg.contains("rate limit")
    {
        return Some(TransientErrorKind::RateLimit);
    }
    // Issue #191 — transport-level mid-stream failures. Carefully chosen
    // substrings to avoid colliding with the cap-hit branch (see the
    // module doc-comment on substring-collision discipline above).
    // Checked BEFORE the DecodeError branch because hyper's
    // `"connection closed before message completed"` could plausibly be
    // wrapped as a decode error in some alloy versions; classifying it
    // as TransportError is more accurate for operator dashboards.
    if msg.contains("connection reset")
        || msg.contains("broken pipe")
        || msg.contains("connection closed")
        || msg.contains("timed out")
        || msg.contains("request timeout")
        || msg.contains("early eof")
    {
        return Some(TransientErrorKind::TransportError);
    }
    if msg.contains("decoding response body")
        || msg.contains("error decoding response")
        || msg.contains("eof while parsing")
        || msg.contains("expected value")
        || msg.contains("unexpected end of stream")
    {
        return Some(TransientErrorKind::DecodeError);
    }
    None
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

/// In-memory [`ChainLogFetcher`] for inline tests + scenario tests across crates.
///
/// Issue #186: relocated from `pe_bootstrap::polygon_ctf_delta::test_support` to
/// this module (where the `ChainLogFetcher` trait lives) so that
/// `wallet_enumeration` tests can use it without violating the dependency
/// direction (`pe-source-onchain-polygon` cannot depend on `pe-bootstrap`).
#[cfg(any(test, feature = "scenario"))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub mod test_support {
    use std::sync::Mutex;

    use alloy::rpc::types::{Filter, Log};

    use super::{ChainLogFetcher, PolygonRpcError};

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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod classify_transient_error_tests {
    use super::{TransientErrorKind, classify_transient_error};

    /// PASS: every documented rate-limit-class substring classifies as
    ///       `RateLimit`, regardless of surrounding text or case.
    /// FAIL: any rate-limit phrase returns `None` (would cause the
    ///       bisect retry loop to skip backoff and propagate the error
    ///       immediately — regression on the issue #176 retry behavior).
    #[test]
    fn rate_limit_phrases_classify_as_rate_limit() {
        for raw in [
            "HTTP 429 Too Many Requests",
            "Compute Units exceeded for this minute",
            "request was throttled by upstream",
            "Too Many Requests, please slow down",
            "Rate limit reached for /eth_getLogs",
            // Mixed case + surrounding noise — the classifier lowercases first.
            "Some prefix RATE LIMIT some suffix",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                Some(TransientErrorKind::RateLimit),
                "expected RateLimit classification for: {raw}"
            );
        }
    }

    /// PASS: every documented decode-flake substring classifies as
    ///       `DecodeError`. These are the patterns observed in production
    ///       2026-05-17 (alloy + Alchemy partial responses on dense V2 regions).
    /// FAIL: any decode-flake phrase returns `None` — without retry the
    ///       whole bootstrap arm fails and costs a ~12-min
    ///       `refresh_trade_counts` replay on wrapper restart.
    #[test]
    fn decode_flake_phrases_classify_as_decode_error() {
        for raw in [
            "error decoding response body: expected value at line 1",
            "error decoding response: io error",
            "EOF while parsing a value at line 0 column 0",
            "expected value at line 5 column 17",
            "unexpected end of stream",
            // The exact alloy message observed at 2026-05-17T20:42:19Z.
            "eth_getLogs [86133528, 86134308]: error decoding response body",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                Some(TransientErrorKind::DecodeError),
                "expected DecodeError classification for: {raw}"
            );
        }
    }

    /// PASS: non-transient errors return `None` so the caller's
    ///       cap-vs-propagate logic in `eth_get_logs_bisect` runs. Notably,
    ///       cap-hit errors ("response too large", "limit exceeded", "range")
    ///       MUST NOT classify as transient — they're handled by the bisect
    ///       branch downstream.
    /// FAIL: a cap-hit phrase returns `Some(_)` — would cause the bisect
    ///       retry loop to retry the same too-large range indefinitely
    ///       instead of halving.
    #[test]
    fn cap_hit_and_unknown_errors_return_none() {
        for raw in [
            "Log response size exceeded",
            "block range is too large",
            "query returned too many results",
            "Connection refused",
            "DNS resolution failed",
            "could not connect to host",
            "",
            "some random error nothing transient",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                None,
                "expected None classification for: {raw}"
            );
        }
    }

    /// PASS: `as_str()` produces the operator-visible label that ends up
    ///       in the tracing record's `kind` field. Pins the rendering
    ///       so a future enum addition doesn't silently break log parsers
    ///       grepping for these literals.
    #[test]
    fn as_str_renders_stable_labels() {
        assert_eq!(TransientErrorKind::RateLimit.as_str(), "rate-limited");
        assert_eq!(TransientErrorKind::DecodeError.as_str(), "decode-error");
        assert_eq!(
            TransientErrorKind::TransportError.as_str(),
            "transport-error"
        );
    }

    /// PASS: every documented transport-flake substring classifies as
    ///       `TransportError`. These are textbook transient — TCP RST,
    ///       broken pipe, upstream gateway close, client-side timeout.
    ///       Retry typically clears them on the next attempt.
    /// FAIL: any phrase returns `None` — without retry the entire bootstrap
    ///       arm fails and costs a ~12-min `refresh_trade_counts` replay on
    ///       wrapper restart.
    #[test]
    fn transport_phrases_classify_as_transport_error() {
        for raw in [
            "connection reset by peer",
            "broken pipe",
            "connection closed before message completed",
            "operation timed out",
            "request timed out waiting for response",
            "request timeout",
            "early eof while parsing",
            // Mixed case — classifier lowercases first.
            "Some prefix CONNECTION RESET some suffix",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                Some(TransientErrorKind::TransportError),
                "expected TransportError classification for: {raw}"
            );
        }
    }

    /// PASS: the EXACT production Alchemy cap-hit error string still classifies
    ///       as `None` (caller's cap-hit branch handles it via bisect). This
    ///       regression test prevents the substring-collision foot-gun called
    ///       out in the classify_transient_error doc-comment.
    ///
    /// **Why this is critical**: the cap-hit string contains the substring
    /// `"timeout"`. If a future contributor adds bare `"timeout"` to the
    /// classifier without checking the cap-hit branch, this test will catch
    /// it — the retry loop would fire on cap-hit-disguised-as-timeout, retry
    /// the same too-large range up to 6×32s, then propagate without ever
    /// bisecting. Dense-region sweeps would stall indefinitely.
    /// FAIL: this regression test catching adding bare `"timeout"` is the
    ///       point.
    #[test]
    fn alchemy_query_timeout_exceeded_must_not_classify_as_transient() {
        // Verbatim from `/tmp/bootstrap-v3-onchain.out` line at 2026-05-17T20:21:39Z.
        let alchemy_cap_hit_error = "HTTP error 400 with body: {\"jsonrpc\":\"2.0\",\"id\":330,\
            \"error\":{\"code\":-32000,\"message\":\"Query timeout exceeded. Consider \
            reducing your block range. Based on your parameters and the response size \
            limit, this block range should work: [0x5223c98, 0x5223e80]\"}}";
        assert_eq!(
            classify_transient_error(alchemy_cap_hit_error),
            None,
            "Alchemy cap-hit error MUST classify as None (handled by bisect branch); \
             classifying as transient causes indefinite retry on a too-large range"
        );
    }
}
