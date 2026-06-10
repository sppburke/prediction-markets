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
pub mod consensus;
pub mod db;
pub mod exchange_ws;
pub mod fees;
pub mod gamma;
pub mod join;
pub mod report;
pub mod resolve;
pub mod types;

mod error;
mod runner;
mod stats;
mod sweep;
mod ws;

pub use config::{ConfigError, ShadowConfig, load};
pub use error::Error;
pub use report::{RealizedGroup, Report, ReportGroup, build_report};
pub use resolve::{BtcResolutionFetcher, MarketResolution};
pub use runner::{RunSummary, generate_report, resolve, run};
pub use sweep::{
    BUY_HOLD_FEE_PROVENANCE, CAPTURE_CONFIG_PROVENANCE, CellResult, DecodeStats, FidelitySummary,
    ReferenceParams, SCALP_FEE_PROVENANCE, ScalpGroup, SweepArgs, SweepOutput, TapeValidity, sweep,
    sweep_cmd,
};
