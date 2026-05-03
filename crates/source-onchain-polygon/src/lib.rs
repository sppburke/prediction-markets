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

pub mod connector;
pub mod event;

pub use connector::PolygonReplayConnector;
pub use event::{ExternalAddressKind, PolygonEvent, PolygonEventError, TxHash};
