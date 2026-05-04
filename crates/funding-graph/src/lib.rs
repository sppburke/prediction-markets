//! `pe-funding-graph` — pure, deterministic accumulator that converts decoded
//! [`PolygonEvent`]s into a [`FundingSnapshot`] for
//! [`pe_operator_graph::build_operator_identities`].
//!
//! # Architecture constraints
//! - Pure crate: NO I/O, NO network, NO async.
//! - `float_arithmetic = "deny"`.
//! - No `unwrap`/`expect`/`panic!` in production code.
//! - Depends on `pe-source-onchain-polygon` (input) and `pe-operator-graph`
//!   (output types). `pe-operator-graph` does NOT depend on this crate — the
//!   service layer is the glue.

pub mod accumulator;

pub use accumulator::FundingGraphAccumulator;
