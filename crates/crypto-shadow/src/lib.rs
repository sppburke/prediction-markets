//! BTC latency-arb **shadow measurement** harness (`pe-crypto-shadow`).
//!
//! Measures whether a latency-arb edge survives Polymarket's verified
//! `crypto_fees_v2` taker fee on short-horizon BTC up/down markets (5m & 15m).
//! Shadow/paper only — it places **no orders** and its dependency closure
//! excludes execution/venue/strategy/risk crates (see issue #297). The measured
//! edge is observation-side, location-dependent, and an upper bound that
//! excludes order execution latency; always read it alongside the `meta`
//! vantage point.

pub mod chainlink_ws;
pub mod clob_ws;
pub mod config;
pub mod db;
pub mod fees;
pub mod gamma;
pub mod join;
pub mod report;
pub mod types;

mod error;
mod runner;
mod ws;

pub use config::{ConfigError, ShadowConfig, load};
pub use error::Error;
pub use report::{Report, ReportGroup, build_report};
pub use runner::{RunSummary, generate_report, run};
