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

// ── Numeric constants ────────────────────────────────────────────────────────

/// Decimal places for USDC and WCOL (both 6-decimal ERC-20 tokens).
pub const COLLATERAL_DECIMALS: u32 = 6;

/// Addresses subscribed to during backfill and live subscription.
pub const MONITORED_ADDRESSES: [Address; 3] = [USDC, WCOL, GNOSIS_SAFE_FACTORY];

/// Topic0 hashes used as the OR filter for `eth_getLogs`.
pub const MONITORED_TOPICS: [B256; 2] = [TOPIC_ERC20_TRANSFER, TOPIC_PROXY_CREATION];
