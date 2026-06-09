//! Crate-wide error aggregating module errors for the orchestration entry points.

use crate::config::ConfigError;
use crate::db::DbError;
use crate::gamma::GammaError;
use crate::resolve::ResolveError;

/// Top-level error for `run` / `resolve` / report generation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    #[error("db: {0}")]
    Db(#[from] DbError),
    #[error("gamma: {0}")]
    Gamma(#[from] GammaError),
    #[error("resolve: {0}")]
    Resolve(#[from] ResolveError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
