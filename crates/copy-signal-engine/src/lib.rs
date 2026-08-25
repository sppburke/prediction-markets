//! `pe-copy-signal-engine` — classify leader trades into typed [`LeaderSignal`]s.
//!
//! Determines what a watchlisted leader just did ([`LeaderAction`]) and computes
//! `action_confidence_ppm` from the wallet's reconstruction quality.
//!
//! # Architecture constraints
//! - Pure crate: NO I/O, NO network, NO async.
//! - `float_arithmetic = "deny"` — all numeric operations use integers or `rust_decimal`.
//! - No `unwrap`/`expect`/`panic!` in production code.
//!
//! [`LeaderAction`]: pe_core_types::LeaderAction

pub mod classifier;
pub mod config;
pub mod signal;
pub mod snapshot;

pub use classifier::classify_trade;
pub use config::SignalConfig;
pub use signal::LeaderSignal;
pub use snapshot::{IncomingTrade, PositionSnapshot, PositionState, TradeProvenance};
