//! `pe-execution-core` — mode-aware execution dispatcher for the prediction-edge system.
//!
//! Routes `OrderIntent` to paper or live execution based on `ExecutionMode`:
//! - `Shadow` / `Paper` → `PaperExecutor` (from `pe-strategy-winner-follow`)
//! - `LiveTiny` / `Promoted` → `LiveExecutor` (Polymarket CLOB via `pe-venue-polymarket`)
//!
//! All executors write durable records to the event log before returning.

#![forbid(unsafe_code)]

pub mod dispatcher;
pub mod error;
pub mod live;

pub use dispatcher::{DispatchResult, ExecutionDispatcher};
pub use error::ExecutionError;
pub use live::{LiveExecuteResult, LiveExecutor, LiveFill, LiveOrderTerminal};
