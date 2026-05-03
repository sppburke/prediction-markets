//! `pe-trader-index` — pure, deterministic wallet-level ledger reconstruction and ranking.
//!
//! Reconstructs trade histories from raw public event snapshots, annotates
//! each wallet with its [`OperatorIdentity`] from `pe-operator-graph`, and
//! produces [`TraderLedger`] records. The walk-forward ranker then groups
//! ledgers by operator, scores them with LCB_5pct, and returns a [`Watchlist`]
//! ready for `copy-signal-engine`.
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
pub mod ranker;
pub mod reconstruction;
pub mod score;
pub mod snapshot;
pub mod watchlist;

pub use config::{LedgerConfig, RankerConfig};
pub use error::TraderIndexError;
pub use ledger::{ClosedTrade, OpenPosition, TraderLedger};
pub use ranker::build_watchlist;
pub use reconstruction::build_trader_ledgers;
pub use snapshot::{RawTrade, TradeSnapshot};
pub use watchlist::{Watchlist, WatchlistEntry, WatchlistTier};
