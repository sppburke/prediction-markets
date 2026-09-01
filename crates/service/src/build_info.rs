//! Embedded ordinary-service build identity (#544).

use crate::build_identity::{BuildIdentityError, EmbeddedBuildIdentity};
use std::path::{Path, PathBuf};

#[must_use]
pub const fn embedded() -> EmbeddedBuildIdentity {
    EmbeddedBuildIdentity {
        source_revision: env!("PE_BUILD_COMMIT"),
        configuration_identity_slot: env!("PE_BUILD_CONFIG_IDENTITY_SLOT"),
    }
}

pub fn verify_staged_revision(expected: &str) -> Result<(), BuildIdentityError> {
    crate::build_identity::verify_staged_revision(expected, embedded().source_revision)
}

#[derive(Debug, thiserror::Error)]
pub enum StagedArtifactError {
    #[error(transparent)]
    Identity(#[from] BuildIdentityError),
    #[error("resolve the running executable: {0}")]
    CurrentExecutable(#[source] std::io::Error),
    #[error("read staged executable {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("staging expected BLAKE3 is not a 64-digit hexadecimal digest: {0:?}")]
    InvalidHash(String),
    #[error("staged binary hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch { expected: String, actual: String },
}

/// Verify both identities used by deployment staging: the reviewed Git object embedded in the
/// binary and the BLAKE3 digest of the exact executable bytes (#544).
pub fn verify_staged_identity(
    expected_revision: &str,
    expected_hash: &str,
) -> Result<String, StagedArtifactError> {
    verify_staged_revision(expected_revision)?;
    let executable = std::env::current_exe().map_err(StagedArtifactError::CurrentExecutable)?;
    verify_artifact_hash(&executable, expected_hash)
}

pub fn verify_artifact_hash(
    path: &Path,
    expected_hash: &str,
) -> Result<String, StagedArtifactError> {
    if expected_hash.len() != 64 || !expected_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StagedArtifactError::InvalidHash(expected_hash.to_owned()));
    }
    let bytes = std::fs::read(path).map_err(|source| StagedArtifactError::Read {
        path: path.to_owned(),
        source,
    })?;
    let actual = blake3::hash(&bytes).to_hex().to_string();
    if !actual.eq_ignore_ascii_case(expected_hash) {
        return Err(StagedArtifactError::HashMismatch {
            expected: expected_hash.to_owned(),
            actual,
        });
    }
    Ok(actual)
}

#[must_use]
pub fn version_line() -> String {
    crate::build_identity::version_line(
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        &embedded(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn artifact_hash_verification_accepts_exact_bytes_and_rejects_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("artifact");
        std::fs::write(&path, b"reviewed artifact").unwrap();
        let expected = blake3::hash(b"reviewed artifact").to_hex().to_string();
        assert_eq!(verify_artifact_hash(&path, &expected).unwrap(), expected);
        assert!(matches!(
            verify_artifact_hash(
                &path,
                "0000000000000000000000000000000000000000000000000000000000000000"
            ),
            Err(StagedArtifactError::HashMismatch { .. })
        ));
    }
}
