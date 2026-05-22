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

    /// A persisted value could not be decoded back into its typed form
    /// (e.g. a money TEXT field that no longer parses as `Decimal`, or an
    /// integer column outside the target type's range).
    #[error("decode: {0}")]
    Decode(String),
}
