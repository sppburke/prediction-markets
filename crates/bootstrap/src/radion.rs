//! Radion REST API wallet discovery — stub (issue #324).
//!
//! Full implementation is deferred until the Radion REST contract is finalised.
//! The stub returns `Ok(0)` immediately. Configure `radion_api_url` in
//! `BootstrapConfig` to enable future discovery; the stub logs a `debug` notice
//! when called.

use crate::error::BootstrapError;

/// No-op stub — Radion discovery not yet implemented.
pub async fn run_radion_discovery() -> Result<usize, BootstrapError> {
    tracing::debug!("radion: stub — discovery not yet implemented");
    Ok(0)
}
