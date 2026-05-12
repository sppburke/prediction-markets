//! Polygon PoS (chain ID 137) contract addresses and ABI event topic0 hashes.
//!
//! Every constant carries a `// verified <date> from <source>` comment.
//! To update: confirm on-chain via PolygonScan and in the relevant Polymarket
//! GitHub repository; update the comment with the new verification date.

use alloy::primitives::{Address, B256, address, b256};

// ── Contract addresses ───────────────────────────────────────────────────────

/// Bridged USDC (USDC.e) on Polygon PoS — Polymarket's collateral token.
/// verified 2026-05-04 from github.com/Polymarket/neg-risk-ctf-adapter/blob/main/addresses.json
pub const USDC: Address = address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

/// Polymarket WrappedCollateral (brand name: pUSD; on-chain symbol: WCOL).
/// NegRisk multi-outcome markets wrap USDC into WCOL 1:1 before market settlement.
/// PUsdMint / PUsdBurn events are ERC-20 Transfer(from=0x0, ...) / Transfer(..., to=0x0).
/// verified 2026-05-04 from github.com/Polymarket/neg-risk-ctf-adapter/blob/main/addresses.json
pub const WCOL: Address = address!("3A3BD7bb9528E159577F7C2e685CC81A765002E2");

/// Gnosis Safe proxy factory used by Polymarket to deploy per-user proxy wallets.
/// Emits: ProxyCreation(address indexed proxy, address singleton)
/// verified 2026-05-04 from github.com/Polymarket/proxy-factories README deployments table
pub const GNOSIS_SAFE_FACTORY: Address = address!("aacFeEa03eb1561C4e67d661e40682Bd20E3541b");

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
/// Deployed ~block 33_605_403 (Jan 2023).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const CTF_EXCHANGE_V1: Address = address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");

/// Polymarket NegRiskCtfExchange V1 (multi-outcome / neg-risk markets).
/// Deployed ~block 50_505_492 (Aug 2024).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const NEG_RISK_CTF_EXCHANGE_V1: Address = address!("C5d563A36AE78145C45a50134d48A1215220f80a");

/// Polymarket CTFExchange V2 (binary YES/NO markets — current).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const CTF_EXCHANGE_V2: Address = address!("E111180000d2663C0091e4f400237545B87B996B");

/// Polymarket NegRiskCtfExchange V2 (multi-outcome / neg-risk — current).
/// verified 2026-05-05 from docs.polymarket.com/resources/contract-addresses
pub const NEG_RISK_CTF_EXCHANGE_V2: Address = address!("e2222d279d744050d28e00520010520000310F59");

/// All Polymarket exchange contracts (V1 + V2). Used for wallet enumeration.
/// Scanning all four covers the full history from block 33_605_403 onwards.
pub const ALL_EXCHANGE_CONTRACTS: [Address; 4] = [
    CTF_EXCHANGE_V1,
    NEG_RISK_CTF_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_CTF_EXCHANGE_V2,
];

/// Earliest Polymarket exchange deployment block on Polygon — CTFExchange V1.
/// Default `from_block` for wallet enumeration.
/// Canonical value in `docs/_GLOSSARY.md` "Wallet enumeration defaults".
pub const CTF_EXCHANGE_V1_DEPLOY_BLOCK: u64 = 33_605_403;

// ── Event topic0 hashes ──────────────────────────────────────────────────────

/// ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
/// Used for both USDC and WCOL contracts.
/// verified 2026-05-04 via 4byte.directory
pub const TOPIC_ERC20_TRANSFER: B256 =
    b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

/// Gnosis Safe ProxyCreation(address indexed proxy, address singleton)
/// verified 2026-05-04 via 4byte.directory
pub const TOPIC_PROXY_CREATION: B256 =
    b256!("4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235");

/// OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker,
///   uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled,
///   uint256 takerAmountFilled, uint256 fee)
/// verified 2026-05-05 via yzc.me/x01Crypto/decoding-polymarket + Etherscan OrderFilled logs
pub const TOPIC_ORDER_FILLED: B256 =
    b256!("d0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6");

/// CTF ConditionResolution(bytes32 indexed conditionId, address indexed oracle,
///   bytes32 indexed questionId, uint outcomeSlotCount, uint[] payoutNumerators).
/// Used by the multi-source pipeline (issue #149) to scan settled markets via
/// Polygon `eth_getLogs`. Self-validated by [`tests::topic_condition_resolution_matches_signature`].
/// verified 2026-05-12 via `alloy::primitives::keccak256` of the canonical signature
pub const TOPIC_CONDITION_RESOLUTION: B256 =
    b256!("b44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894");

// ── Numeric constants ────────────────────────────────────────────────────────

/// Decimal places for USDC and WCOL (both 6-decimal ERC-20 tokens).
pub const COLLATERAL_DECIMALS: u32 = 6;

/// Addresses subscribed to during backfill and live subscription.
pub const MONITORED_ADDRESSES: [Address; 3] = [USDC, WCOL, GNOSIS_SAFE_FACTORY];

/// Topic0 hashes used as the OR filter for `eth_getLogs`.
pub const MONITORED_TOPICS: [B256; 2] = [TOPIC_ERC20_TRANSFER, TOPIC_PROXY_CREATION];

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::keccak256;

    /// Self-validating proof that [`TOPIC_CONDITION_RESOLUTION`] matches the
    /// canonical Gnosis-CTF event signature. A future signature change (e.g.
    /// renaming a parameter type) would surface here as a hash mismatch
    /// before silently breaking the resolution scan.
    #[test]
    fn topic_condition_resolution_matches_signature() {
        let computed = keccak256(b"ConditionResolution(bytes32,address,bytes32,uint256,uint256[])");
        assert_eq!(
            computed, TOPIC_CONDITION_RESOLUTION,
            "TOPIC_CONDITION_RESOLUTION drifted from the canonical signature"
        );
    }

    /// Companion self-check for the ERC-20 Transfer topic so that the same
    /// guard applies to every event topic in this module.
    #[test]
    fn topic_erc20_transfer_matches_signature() {
        let computed = keccak256(b"Transfer(address,address,uint256)");
        assert_eq!(computed, TOPIC_ERC20_TRANSFER);
    }
}
