//! Polymarket CLOB venue adapter for the prediction-edge system.
//!
//! Provides order submission and fill confirmation against the Polymarket CLOB REST API.
//!
//! # Modules
//!
//! - **signing** — L1 EIP-712 order signing + L2 HMAC-SHA256 API auth (internal).
//! - **clob_client** — [`CLOBClient`] trait + [`ReqwestCLOBClient`] + [`FixtureCLOBClient`].
//! - **adapter** — [`PolymarketVenueAdapter`]: ties signing + HTTP client together.
//! - **error** — [`PolymarketError`].

#![forbid(unsafe_code)]

pub mod adapter;
pub mod clob_client;
pub mod error;
pub mod signing;

pub use adapter::{PolymarketCredentials, PolymarketVenueAdapter};
pub use clob_client::{CLOBClient, FixtureCLOBClient, ReqwestCLOBClient};
pub use error::PolymarketError;
