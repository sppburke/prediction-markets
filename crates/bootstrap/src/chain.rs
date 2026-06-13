//! Bootstrap-local Polygon-RPC primitives (relocated in #326 PR4).
//!
//! The `source-onchain-polygon` crate was deleted in the operator/funder purge
//! (#326). The surviving bootstrap paths still need a small, self-contained set
//! of Polygon PoS contract constants and the `eth_getLogs` bisect helper:
//!
//! - [`scan_resolutions`](crate::polygon_ctf::scan_resolutions) — the primary
//!   precise market-resolution source ([`CTF`], [`TOPIC_CONDITION_RESOLUTION`],
//!   [`eth_get_logs_bisect`]).
//! - [`fetch_resolutions_and_schedules`](crate::fetch_resolutions_and_schedules)
//!   — floor block when no cursor exists ([`CTF_DEPLOY_BLOCK`]).
//! - [`auto_migrate_legacy`](crate::migrate::auto_migrate_legacy) +
//!   [`run_enumerate`](crate::enumerate::run_enumerate) (Dune arm) — legacy
//!   enum-state synthesis ([`ALL_EXCHANGE_CONTRACTS`],
//!   [`ALL_ORDER_FILLED_TOPICS`], [`TOPIC_ORDER_FILLED_V1`]).
//!
//! On-chain wallet enumeration, the delta scan, and funder discovery were
//! deleted with their crate, so the fetcher abstraction (`ChainLogFetcher`,
//! `AlloyChainLogFetcher`) and the order-fill decoders did NOT come along —
//! only the constants and the bisect primitive survive here.
//!
//! Every constant carries its original `verified <date> from <source>` comment.

use std::time::Duration;

use alloy::primitives::{Address, B256, address, b256};
use alloy::providers::Provider;
use alloy::rpc::types::{BlockNumberOrTag, Filter, Log};

// ── Contract addresses ───────────────────────────────────────────────────────

/// Conditional Token Framework (CTF) — binary and neg-risk market settlement.
/// verified 2026-05-04 from github.com/Polymarket/py-clob-client config.py chain 137
pub const CTF: Address = address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045");

/// Polygon block of the CTF contract deployment. Used as the floor `from_block`
/// for the multi-source pipeline's `eth_getLogs` resolution scan (issue #149)
/// when no prior `polygon_ctf_last_block` cursor exists.
/// verified 2026-05-12 from polygonscan.com/address/0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
/// (ContractCreator → tx 0xf822536aff16fdb8df59bc9c0b5854c5bec4b9a76484ea9d6944908ced563389
/// at block 4_023_686, Sep-03-2020 18:07:23 UTC)
pub const CTF_DEPLOY_BLOCK: u64 = 4_023_686;

/// Polymarket CTFExchange V1 (binary YES/NO markets).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const CTF_EXCHANGE_V1: Address = address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");

/// Polymarket NegRiskCtfExchange V1 (multi-outcome / neg-risk markets).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const NEG_RISK_CTF_EXCHANGE_V1: Address = address!("C5d563A36AE78145C45a50134d48A1215220f80a");

/// Polymarket CTFExchange V2 (binary YES/NO markets — current).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const CTF_EXCHANGE_V2: Address = address!("E111180000d2663C0091e4f400237545B87B996B");

/// Polymarket NegRiskCtfExchange V2 (multi-outcome / neg-risk — current).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const NEG_RISK_CTF_EXCHANGE_V2: Address = address!("e2222d279d744050d28e00520010520000310F59");

/// All Polymarket exchange contracts (V1 + V2). Used to synthesize the legacy
/// "all contracts enumerated" enum-state in [`crate::migrate::auto_migrate_legacy`]
/// and the Dune-arm enumeration completion marker.
pub const ALL_EXCHANGE_CONTRACTS: [Address; 4] = [
    CTF_EXCHANGE_V1,
    NEG_RISK_CTF_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_CTF_EXCHANGE_V2,
];

// ── Event topic0 hashes ──────────────────────────────────────────────────────

/// V1 `OrderFilled` — emitted by [`CTF_EXCHANGE_V1`] and [`NEG_RISK_CTF_EXCHANGE_V1`].
/// Self-validated by [`tests::topic_order_filled_v1_matches_signature`].
/// verified 2026-05-16 via Polygonscan getabi on 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
pub const TOPIC_ORDER_FILLED_V1: B256 =
    b256!("d0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6");

/// V2 `OrderFilled` — emitted by [`CTF_EXCHANGE_V2`] and [`NEG_RISK_CTF_EXCHANGE_V2`].
/// Self-validated by [`tests::topic_order_filled_v2_matches_signature`].
/// verified 2026-05-16 via Polygonscan getabi on 0xE111180000d2663C0091e4f400237545B87B996B
pub const TOPIC_ORDER_FILLED_V2: B256 =
    b256!("d543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee");

/// Canonical "what to scan" set used by the legacy enum-state synthesis.
pub const ALL_ORDER_FILLED_TOPICS: [B256; 2] = [TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2];

/// CTF ConditionResolution(bytes32 indexed conditionId, address indexed oracle,
///   bytes32 indexed questionId, uint outcomeSlotCount, uint[] payoutNumerators).
/// Used by the multi-source pipeline (issue #149) to scan settled markets via
/// Polygon `eth_getLogs`. Self-validated by [`tests::topic_condition_resolution_matches_signature`].
/// verified 2026-05-12 via `alloy::primitives::keccak256` of the canonical signature
pub const TOPIC_CONDITION_RESOLUTION: B256 =
    b256!("b44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894");

// ── eth_getLogs primitives ────────────────────────────────────────────────────

/// Maximum per-attempt backoff (seconds) when an `eth_getLogs` request hits
/// HTTP 429 / rate-limit. Exponential backoff is `1 → 2 → 4 → 8 → 16 → 32`s
/// before the retry loop gives up and propagates the error to the caller.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const RATE_LIMIT_MAX_BACKOFF_SECS: u64 = 32;

/// Errors emitted by the generic Polygon-RPC primitives in this module.
#[derive(Debug, thiserror::Error)]
pub enum PolygonRpcError {
    #[error("eth_getLogs [{from}, {to}]: {message}")]
    GetLogs { from: u64, to: u64, message: String },
    #[error("eth_blockNumber: {0}")]
    GetBlockNumber(String),
}

/// Classification of which transient error class a `Provider::get_logs` failure
/// falls into. Used by [`eth_get_logs_bisect`] to decide whether to retry with
/// exponential backoff vs propagate immediately. `None` means "not transient —
/// propagate to caller for cap-vs-other classification."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransientErrorKind {
    /// Provider-side throttling: HTTP 429, "compute units exceeded", "throttle",
    /// "too many requests", "rate limit". Backoff is the canonical mitigation.
    RateLimit,
    /// Client-side deserialization failure on an apparently-2xx HTTP response:
    /// truncated stream, gateway 5xx body served as HTML/text, mid-response TCP
    /// reset, JSON parse hitting EOF or an unexpected token.
    DecodeError,
    /// Transport-level mid-stream failure: TCP RST mid-request, write to a closed
    /// socket (`broken pipe`), upstream gateway close, or client-side timeout.
    TransportError,
}

impl TransientErrorKind {
    #[must_use]
    fn as_str(self) -> &'static str {
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
/// # Substring-collision discipline (issue #191)
///
/// New substrings must be checked against the cap-hit branch in
/// [`eth_get_logs_bisect`] (matches `"too large"`, `"too many"`,
/// `"response size"`, `"limit exceeded"`, `"range"`). A substring that matches
/// BOTH a transient pattern AND a cap-hit pattern causes the retry loop to fire
/// FIRST, retrying the same too-large range up to 6×32s before propagating — the
/// bisect branch never gets to halve the range. Concretely: bare `"timeout"` is
/// EXCLUDED because Alchemy's `"Query timeout exceeded..."` cap-hit error
/// contains it; the narrower `"timed out"` is included instead.
#[must_use]
fn classify_transient_error(error_message: &str) -> Option<TransientErrorKind> {
    let msg = error_message.to_lowercase();
    if msg.contains("429")
        || msg.contains("compute units")
        || msg.contains("throttle")
        || msg.contains("too many requests")
        || msg.contains("rate limit")
    {
        return Some(TransientErrorKind::RateLimit);
    }
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

/// Issue an `eth_getLogs` request for `[from, to]` with exponential-backoff
/// retry on HTTP 429 (rate limit) and bisect on "response too large".
///
/// `filter` is supplied **without block range set** — the function clones it and
/// injects `.from_block(...)` / `.to_block(...)` for each request, including the
/// recursive halves. This keeps the caller's intent (which addresses, which
/// topic0) decoupled from the chunking strategy.
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
    // either via successful `Ok(logs)` (returned immediately) or by breaking out
    // with the final error for the cap-vs-other classification below.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use alloy::primitives::keccak256;

    use super::{
        ALL_ORDER_FILLED_TOPICS, TOPIC_CONDITION_RESOLUTION, TOPIC_ORDER_FILLED_V1,
        TOPIC_ORDER_FILLED_V2, TransientErrorKind, classify_transient_error,
    };

    /// Self-validating proof that [`TOPIC_CONDITION_RESOLUTION`] matches the
    /// canonical Gnosis-CTF event signature.
    #[test]
    fn topic_condition_resolution_matches_signature() {
        let computed = keccak256(b"ConditionResolution(bytes32,address,bytes32,uint256,uint256[])");
        assert_eq!(
            computed, TOPIC_CONDITION_RESOLUTION,
            "TOPIC_CONDITION_RESOLUTION drifted from the canonical signature"
        );
    }

    /// Self-validating proof for V1 `OrderFilled` (8-argument signature).
    #[test]
    fn topic_order_filled_v1_matches_signature() {
        let computed = keccak256(
            b"OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
        );
        assert_eq!(
            computed, TOPIC_ORDER_FILLED_V1,
            "TOPIC_ORDER_FILLED_V1 drifted from the canonical V1 signature"
        );
    }

    /// Self-validating proof for V2 `OrderFilled` (10-argument signature).
    #[test]
    fn topic_order_filled_v2_matches_signature() {
        let computed = keccak256(
            b"OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)",
        );
        assert_eq!(
            computed, TOPIC_ORDER_FILLED_V2,
            "TOPIC_ORDER_FILLED_V2 drifted from the canonical V2 signature"
        );
    }

    /// `ALL_ORDER_FILLED_TOPICS` must list each known topic exactly once, V1→V2.
    #[test]
    fn all_order_filled_topics_lists_v1_then_v2() {
        assert_eq!(
            ALL_ORDER_FILLED_TOPICS,
            [TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2]
        );
    }

    /// PASS: every documented rate-limit-class substring classifies as `RateLimit`.
    #[test]
    fn rate_limit_phrases_classify_as_rate_limit() {
        for raw in [
            "HTTP 429 Too Many Requests",
            "Compute Units exceeded for this minute",
            "request was throttled by upstream",
            "Too Many Requests, please slow down",
            "Rate limit reached for /eth_getLogs",
            "Some prefix RATE LIMIT some suffix",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                Some(TransientErrorKind::RateLimit),
                "expected RateLimit classification for: {raw}"
            );
        }
    }

    /// PASS: every documented decode-flake substring classifies as `DecodeError`.
    #[test]
    fn decode_flake_phrases_classify_as_decode_error() {
        for raw in [
            "error decoding response body: expected value at line 1",
            "error decoding response: io error",
            "EOF while parsing a value at line 0 column 0",
            "expected value at line 5 column 17",
            "unexpected end of stream",
            "eth_getLogs [86133528, 86134308]: error decoding response body",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                Some(TransientErrorKind::DecodeError),
                "expected DecodeError classification for: {raw}"
            );
        }
    }

    /// PASS: every documented transport-flake substring classifies as `TransportError`.
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
            "Some prefix CONNECTION RESET some suffix",
        ] {
            assert_eq!(
                classify_transient_error(raw),
                Some(TransientErrorKind::TransportError),
                "expected TransportError classification for: {raw}"
            );
        }
    }

    /// PASS: cap-hit and unknown errors return `None` so the bisect branch runs.
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

    /// PASS: the EXACT production Alchemy cap-hit error string still classifies as
    /// `None` (caller's cap-hit branch handles it via bisect). Guards the
    /// substring-collision foot-gun: the cap-hit string contains `"timeout"`.
    #[test]
    fn alchemy_query_timeout_exceeded_must_not_classify_as_transient() {
        let alchemy_cap_hit_error = "HTTP error 400 with body: {\"jsonrpc\":\"2.0\",\"id\":330,\
            \"error\":{\"code\":-32000,\"message\":\"Query timeout exceeded. Consider \
            reducing your block range. Based on your parameters and the response size \
            limit, this block range should work: [0x5223c98, 0x5223e80]\"}}";
        assert_eq!(
            classify_transient_error(alchemy_cap_hit_error),
            None,
            "Alchemy cap-hit error MUST classify as None (handled by bisect branch)"
        );
    }
}
