//! Shared build-identity derivation and staging verification (#544).
//!
//! Release-like profiles accept only a clean Git checkout with a full object identity. Dev/test
//! profiles deliberately use `dev-dirty` for a dirty or unavailable checkout so local iteration
//! remains possible; that sentinel is never accepted by a release build.

use std::fmt;

pub const DEV_DIRTY_REVISION: &str = "dev-dirty";
pub const RUNTIME_CONFIG_IDENTITY_SLOT: &str = "runtime-applied";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedBuildIdentity {
    pub source_revision: &'static str,
    pub configuration_identity_slot: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildIdentityError {
    UnknownCheckout(String),
    InvalidRevision(String),
    DirtyReleaseCheckout,
    StagedRevisionInvalid(String),
    StagedRevisionMismatch { expected: String, embedded: String },
}

impl fmt::Display for BuildIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCheckout(reason) => {
                write!(formatter, "Git checkout unavailable: {reason}")
            }
            Self::InvalidRevision(revision) => {
                write!(
                    formatter,
                    "Git returned an invalid full revision: {revision:?}"
                )
            }
            Self::DirtyReleaseCheckout => {
                formatter.write_str("release build requires a clean Git checkout")
            }
            Self::StagedRevisionInvalid(revision) => write!(
                formatter,
                "staging expected revision is not a full Git object identity: {revision:?}"
            ),
            Self::StagedRevisionMismatch { expected, embedded } => write!(
                formatter,
                "staged binary revision mismatch: expected {expected}, embedded {embedded}"
            ),
        }
    }
}

impl std::error::Error for BuildIdentityError {}

#[must_use]
pub fn is_release_profile(profile: &str) -> bool {
    matches!(profile, "release" | "release-lto" | "bench" | "profiling")
}

#[must_use]
pub fn valid_full_revision(revision: &str) -> bool {
    revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Resolve a checked-out revision from already-captured Git facts.
///
/// This pure boundary is shared with unit tests; `build.rs` is the sole command runner.
pub fn resolve_revision(
    profile: &str,
    revision: Result<&str, &str>,
    dirty: Result<bool, &str>,
) -> Result<String, BuildIdentityError> {
    let release = is_release_profile(profile);
    let revision = match revision {
        Ok(revision) if valid_full_revision(revision) => revision,
        Ok(revision) if release => {
            return Err(BuildIdentityError::InvalidRevision(revision.to_owned()));
        }
        Err(reason) if release => {
            return Err(BuildIdentityError::UnknownCheckout(reason.to_owned()));
        }
        Ok(_) | Err(_) => return Ok(DEV_DIRTY_REVISION.to_owned()),
    };
    match dirty {
        Ok(false) => Ok(revision.to_owned()),
        Ok(true) if release => Err(BuildIdentityError::DirtyReleaseCheckout),
        Err(reason) if release => Err(BuildIdentityError::UnknownCheckout(reason.to_owned())),
        Ok(true) | Err(_) => Ok(DEV_DIRTY_REVISION.to_owned()),
    }
}

/// Deployment preflight helper: require the staged binary's embedded revision to match the
/// reviewed full Git object identity.
pub fn verify_staged_revision(expected: &str, embedded: &str) -> Result<(), BuildIdentityError> {
    if !valid_full_revision(expected) {
        return Err(BuildIdentityError::StagedRevisionInvalid(
            expected.to_owned(),
        ));
    }
    if embedded != expected {
        return Err(BuildIdentityError::StagedRevisionMismatch {
            expected: expected.to_owned(),
            embedded: embedded.to_owned(),
        });
    }
    Ok(())
}

#[must_use]
pub fn version_line(package: &str, version: &str, identity: &EmbeddedBuildIdentity) -> String {
    format!(
        "{package} {version} revision={} config_identity={}",
        identity.source_revision, identity.configuration_identity_slot
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn release_rejects_dirty_unknown_and_invalid_checkouts() {
        assert_eq!(
            resolve_revision("release", Ok(REVISION), Ok(true)),
            Err(BuildIdentityError::DirtyReleaseCheckout)
        );
        assert_eq!(
            resolve_revision("release-lto", Ok(REVISION), Ok(true)),
            Err(BuildIdentityError::DirtyReleaseCheckout)
        );
        assert!(matches!(
            resolve_revision("release", Err("git missing"), Ok(false)),
            Err(BuildIdentityError::UnknownCheckout(_))
        ));
        assert!(matches!(
            resolve_revision("profiling", Ok("short"), Ok(false)),
            Err(BuildIdentityError::InvalidRevision(_))
        ));
    }

    #[test]
    fn dev_uses_explicit_dirty_sentinel_but_clean_dev_keeps_revision() {
        assert_eq!(
            resolve_revision("debug", Ok(REVISION), Ok(false)).unwrap(),
            REVISION
        );
        assert_eq!(
            resolve_revision("debug", Ok(REVISION), Ok(true)).unwrap(),
            DEV_DIRTY_REVISION
        );
        assert_eq!(
            resolve_revision("debug", Err("no git"), Err("no git")).unwrap(),
            DEV_DIRTY_REVISION
        );
    }
}
