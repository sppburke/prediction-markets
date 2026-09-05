//! Bootstrap-local Polygon PoS contract constants (relocated in #326 PR4).
//!
//! The `source-onchain-polygon` crate was deleted in the operator/funder purge
//! (#326), and the on-chain CTF resolution scan was removed when CLOB became the
//! sole market-resolution source (#369). The surviving bootstrap paths need a
//! small set of Polygon PoS contract constants; V1 stays local for legacy migration,
//! while V2 is re-exported from the venue owner:
//!
//! - [`auto_migrate_legacy`](crate::migrate::auto_migrate_legacy) — legacy
//!   V1-done enum-state synthesis for the `wallet_set.json` one-shot
//!   ([`ALL_EXCHANGE_CONTRACTS`], [`TOPIC_ORDER_FILLED_V1`]).
//! - [`normalise_condition_id`] — `0x` condition-id canonicalisation shared by
//!   the Gamma `/events` sweep (relocated from the deleted `dune.rs` in #335).
//!
//! On-chain wallet enumeration, the delta scan, funder discovery, and the CTF
//! resolution scan were all deleted with their callers, so the fetcher
//! abstraction and the `eth_getLogs` bisect primitive did NOT survive — only the
//! exchange/order-filled constants and `normalise_condition_id` remain here.
//!
//! Every locally owned constant carries its original `verified <date> from <source>` comment.

use alloy_primitives::{Address, B256, address, b256};

pub use pe_venue_polymarket::{CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2, TOPIC_ORDER_FILLED_V2};

// ── Contract addresses ───────────────────────────────────────────────────────

/// Polymarket CTFExchange V1 (binary YES/NO markets).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const CTF_EXCHANGE_V1: Address = address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");

/// Polymarket NegRiskCtfExchange V1 (multi-outcome / neg-risk markets).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const NEG_RISK_CTF_EXCHANGE_V1: Address = address!("C5d563A36AE78145C45a50134d48A1215220f80a");

/// All Polymarket exchange contracts (V1 + V2). Used to synthesize the legacy
/// "all contracts enumerated" enum-state marker in
/// [`crate::migrate::auto_migrate_legacy`].
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

/// Canonical "what to scan" set used by the legacy enum-state synthesis.
pub const ALL_ORDER_FILLED_TOPICS: [B256; 2] = [TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2];

/// Normalise a varbinary-cast condition id to `0x`-prefixed lowercase hex.
///
/// Upstream casts of the form `CAST(conditionid AS VARCHAR)` emit a `\x`-prefixed
/// string (e.g. `\x0aff…`); the cache stores `0x`-prefixed strings to match the
/// Polymarket trade-data format. The Gamma `/events` sweep ([`crate::events`])
/// normalises its `conditionId` through this single source of truth so the join
/// key `market_events.condition_id ↔ trades.market_id` never drifts by source.
///
/// Relocated from the deleted `dune.rs` in #335: it outlived its original SQL
/// caller, but the Gamma path still needs it.
pub(crate) fn normalise_condition_id(raw: &str) -> String {
    if let Some(hex) = raw.strip_prefix("\\x") {
        format!("0x{hex}")
    } else {
        raw.to_owned()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use alloy_primitives::keccak256;

    use super::{
        ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
        normalise_condition_id,
    };

    /// PASS: `\x`-prefixed varbinary casts become `0x`-prefixed; already-`0x`
    /// strings pass through unchanged.
    #[test]
    fn normalise_condition_id_handles_both_forms() {
        assert_eq!(normalise_condition_id("\\x0aff"), "0x0aff");
        assert_eq!(normalise_condition_id("0xdeadbeef"), "0xdeadbeef");
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
}
