//! Raw Polygon-chain event connector for the prediction-edge system.
//!
//! This crate emits raw [`PolygonEvent`] payloads serialized as JSON into
//! [`pe_source_core::SourceEvent`] envelopes. Downstream crates decode the
//! payload bytes and interpret funder identity.
//!
//! **Provenance invariant**: This crate never infers funder identity from a
//! "first USDC sender" heuristic alone — every event carries structured
//! provenance fields (`from`, `bridge`, `source_kind`, etc.) so that
//! `operator-graph` can reconstruct multi-hop funding chains from the raw log
//! without losing context.
//!
//! # Modules
//!
//! | Module | Purpose |
//! |---|---|
//! | [`contracts`] | Verified Polygon PoS contract addresses and topic0 hashes |
//! | [`decoder`] | ABI decoders: alloy `Log` → [`PolygonEvent`] |
//! | [`connector`] | [`PolygonReplayConnector`] for deterministic replay |
//! | [`live`] | [`LivePolygonConnector`] for live data via HTTP backfill + WS |
//! | [`event`] | [`PolygonEvent`] enum and supporting types |
//! | [`etherscan`] | [`EtherscanFunderLookup`] — alternate funder backend (chain 137) |
//! | [`wallet_enumeration`] | [`PolymarketTraderEnumeration`] — discover all wallets via `OrderFilled` events |
//!
//! # Funder discovery backends
//!
//! Two `FunderLookup` implementations are available; selection is via
//! [`PolygonConnectorConfig::funder_source`]:
//!
//! - `eth_logs` (default): [`EthGetLogsLookup`] — batches multiple recipients
//!   per `topic[2]` filter against an Alchemy-compatible JSON-RPC endpoint.
//!   Spends compute units; intended for short-range backfill.
//! - `etherscan`: [`EtherscanFunderLookup`] — one tokentx query per
//!   `(wallet, USDC contract)` pair against the Etherscan V2 free tier.
//!   Spends 5 req/s; intended for the historical 21M-block discovery sweep.
//!   **Caveat:** unlike the eth_logs backend, the Etherscan backend does NOT
//!   forward decoded USDC Transfer events into `event_tx`; it only computes
//!   the funder closure. Operators flipping a fresh deploy should bootstrap
//!   the event log under `eth_logs` once before switching, otherwise replay
//!   reproducibility for the historical range will be incomplete.

pub mod connector;
pub mod contracts;
pub mod decoder;
pub mod etherscan;
pub mod event;
pub mod funder_discovery;
pub mod live;
pub mod wallet_enumeration;

pub use connector::PolygonReplayConnector;
pub use etherscan::{EtherscanFunderLookup, HttpFetcher};
pub use event::{ExternalAddressKind, PolygonEvent, PolygonEventError, TxHash};
pub use funder_discovery::{
    BlockRange, EthGetLogsLookup, FunderDiscoveryError, FunderLookup, discover_to_depth,
};
pub use live::{LivePolygonConnector, LivePolygonError, PolygonConnectorConfig};
pub use wallet_enumeration::{EnumerationConfig, EnumerationError, PolymarketTraderEnumeration};
