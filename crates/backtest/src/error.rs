//! Error types for the backtest binary.

use pe_bootstrap::error::BootstrapError;
use pe_operator_graph::OperatorGraphError;
use pe_source_onchain_polygon::FunderDiscoveryError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BacktestError {
    #[error("missing required env var: {0}")]
    MissingEnv(String),

    #[error("funder discovery: {0}")]
    FunderDiscovery(#[from] FunderDiscoveryError),

    #[error("operator graph: {0}")]
    OperatorGraph(#[from] OperatorGraphError),

    #[error("wallet cache: {0}")]
    Cache(#[from] BootstrapError),

    #[error("invalid wallet address: {0}")]
    InvalidAddress(String),

    #[error("output write: {0}")]
    OutputWrite(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("internal: {0}")]
    Internal(String),

    #[error("invalid kelly sweep fraction: {0}")]
    InvalidKellySweep(String),
}
