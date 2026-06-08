//! Crate-wide error aggregating module errors for the orchestration entry points.

use crate::config::ConfigError;
use crate::db::DbError;
use crate::gamma::GammaError;

/// Top-level error for `run` / report generation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    #[error("db: {0}")]
    Db(#[from] DbError),
    #[error("gamma: {0}")]
    Gamma(#[from] GammaError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
