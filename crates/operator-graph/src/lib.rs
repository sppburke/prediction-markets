//! `pe-operator-graph` — pure, deterministic operator identity clustering.
//!
//! Groups trading wallets that share a common on-chain funder root into a single
//! [`OperatorIdentity`]. The algorithm is deterministic: same [`FundingSnapshot`]
//! and [`ClusteringConfig`] always produce the same set of [`OperatorIdentity`]
//! values.
//!
//! # Architecture constraints
//! - Pure crate: NO I/O, NO network, NO async, NO `std::io`.
//! - `float_arithmetic = "deny"` — all numeric operations use [`rust_decimal::Decimal`]
//!   or integers.
//! - No `unwrap`/`expect`/`panic!` in production code.
//! - No raw `f64`.

pub mod clustering;
pub mod error;
pub mod funding;
pub mod identity;

pub use clustering::{ClusteringConfig, build_operator_identities};
pub use error::OperatorGraphError;
pub use funding::{AddressCategory, FundingEdge, FundingSnapshot};
pub use identity::{AntiGamingFlag, OperatorIdentity};

#[cfg(test)]
mod clustering_tests;
