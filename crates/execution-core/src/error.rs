//! Error types for `pe-execution-core`.

use thiserror::Error;

/// Errors produced by the execution dispatcher and live executor.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("paper execution failed: {0}")]
    Paper(#[from] pe_strategy_winner_follow::PaperExecutionError),

    #[error("live execution failed: {0}")]
    Live(String),

    #[error("event log write failed: {0}")]
    EventLog(#[from] pe_event_log::LogError),

    #[error("serialisation failed: {0}")]
    Serialise(#[from] serde_json::Error),

    #[error("venue error: {0}")]
    Venue(#[from] pe_venue_core::VenueError),
}
