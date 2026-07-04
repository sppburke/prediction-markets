//! `pe-strategy-winner-follow` — Winner-Follow strategy evaluation and paper execution.
//!
//! # Modules
//!
//! - **evaluate** — pure signal → Kelly → risk → `OrderIntent` pipeline. No I/O.
//! - **paper** — `PaperExecutor`: simulates fills and writes `PaperFill` records to
//!   the event-log. Contains I/O; keep separate from the pure evaluation path.
//!
//! `float_arithmetic = "deny"` — all numeric operations use integers or `rust_decimal`.
//! No `unwrap`/`expect`/`panic!` in production code.

pub mod config;
pub mod error;
pub mod evaluate;
pub mod mode;
pub mod paper;

pub use config::{PerTradeCap, SizingMode, WinnerFollowConfig};
pub use error::{PaperExecutionError, WinnerFollowError};
pub use evaluate::WinnerFollowStrategy;
pub use mode::ExecutionMode;
pub use paper::{FillSource, PaperExecutor, PaperFill};
