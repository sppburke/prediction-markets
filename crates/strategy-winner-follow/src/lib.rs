//! `pe-strategy-winner-follow` — Winner-Follow signal → Kelly sizing → risk gate → `OrderIntent`.
//!
//! Pure crate: NO I/O, NO network, NO async.
//! `float_arithmetic = "deny"` — all numeric operations use integers or `rust_decimal`.
//! No `unwrap`/`expect`/`panic!` in production code.
//!
//! # Entry point
//!
//! ```ignore
//! let strategy = WinnerFollowStrategy::new(config);
//! let result = strategy.evaluate(&signal, snapshot, bankroll, mode);
//! ```

pub mod config;
pub mod error;
pub mod evaluate;
pub mod mode;

pub use config::WinnerFollowConfig;
pub use error::WinnerFollowError;
pub use evaluate::WinnerFollowStrategy;
pub use mode::ExecutionMode;
