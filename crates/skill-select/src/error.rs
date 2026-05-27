//! Error type for the skill-selection pipeline.

/// Failures from the skill-selection persistence and (later) compute layers.
///
/// Library-style `thiserror` enum so downstream binaries and tests can match on
/// the cause; mirrors the boundary convention used by `pe-bootstrap`.
#[derive(Debug, thiserror::Error)]
pub enum SkillSelectError {
    /// SQLite open / migrate / query / exec failure.
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// A read against the bootstrap `WalletCache` (trades / wallets / event map)
    /// failed during extraction.
    #[error("bootstrap cache: {0}")]
    Bootstrap(#[from] pe_bootstrap::error::BootstrapError),

    /// A persisted value could not be decoded back into its typed form
    /// (e.g. a money TEXT field that no longer parses as `Decimal`, or an
    /// integer column outside the target type's range).
    #[error("decode: {0}")]
    Decode(String),

    /// Config load/parse failure (TOML file or `PE_SKILL_*` env). Boxed because
    /// `figment::Error` is large — keeps `SkillSelectError` (and every
    /// `Result<_, SkillSelectError>`) small (clippy `result_large_err`).
    #[error("config: {0}")]
    Config(Box<figment::Error>),

    /// Field-mapping failure in `export-watchlist` (e.g. `ReconstructionQuality`
    /// out-of-range, `OffsetDateTime` conversion failure, or `serde_json` error).
    #[error("watchlist mapping: {0}")]
    WatchlistMapping(String),

    /// Filesystem I/O failure (file read, write, or atomic rename).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl From<figment::Error> for SkillSelectError {
    fn from(e: figment::Error) -> Self {
        SkillSelectError::Config(Box::new(e))
    }
}
