//! `pe-trader-index` — pure, deterministic wallet-level ledger reconstruction.
//!
//! Reconstructs trade histories from raw public event snapshots, annotates
//! each wallet with its [`OperatorIdentity`] from `pe-operator-graph`, and
//! produces [`TraderLedger`] records ready for the ranker.
//!
//! # Architecture constraints
//! - Pure crate: NO I/O, NO network, NO async, NO `std::io`.
//! - `float_arithmetic = "deny"` — all numeric operations use [`rust_decimal::Decimal`]
//!   or integers.
//! - No `unwrap`/`expect`/`panic!` in production code.
//! - No raw `f64`.
//!
//! [`OperatorIdentity`]: pe_operator_graph::OperatorIdentity

pub mod config;
pub mod error;
pub mod ledger;
pub mod reconstruction;
pub mod snapshot;

pub use config::LedgerConfig;
pub use error::TraderIndexError;
pub use ledger::{ClosedTrade, OpenPosition, TraderLedger};
pub use reconstruction::build_trader_ledgers;
pub use snapshot::{RawTrade, TradeSnapshot};
