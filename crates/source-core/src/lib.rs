//! Shared connector trait and types for all `source-*` crates.
//!
//! `SourceEvent` is the raw event returned by a connector before being written
//! to the append-only log. It is independent of `pe-event-log` so that source
//! crates do not need to depend on the log crate. Callers convert to
//! `EnvelopeIn` when writing to the log.

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use serde::{Deserialize, Serialize};

/// Raw event produced by a connector before being written to the event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceEvent {
    pub source_id: SourceId,
    pub schema_version: u32,
    pub parser_version: u32,
    pub observed_at: SourceTimestamp,
    pub received_at: ReceivedAt,
    pub payload: Vec<u8>,
}

/// Coarse health classification for a source connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    Healthy,
    Degraded,
    Dead,
}

/// Health snapshot for a source connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceHealth {
    pub source_id: SourceId,
    pub last_event_at: Option<SourceTimestamp>,
    pub status: SourceStatus,
}

/// Errors that a [`SourceConnector`] may return.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("transient error: {message}")]
    Transient { message: String },
    #[error("fatal error: {message}")]
    Fatal { message: String },
    #[error("rate limited: retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u32 },
}

/// Trait implemented by every source connector.
///
/// `next_event` drives the connector's polling loop. The caller is responsible
/// for backoff when `SourceError::Transient` or `SourceError::RateLimited` is
/// returned.
///
/// `async fn` in trait is stable since Rust 1.75. The `async_fn_in_trait`
/// lint is suppressed because this trait is internal and auto-trait bounds
/// (e.g. `Send`) are not required at this level.
#[allow(async_fn_in_trait)]
pub trait SourceConnector {
    fn source_id(&self) -> SourceId;
    fn health(&self) -> SourceHealth;
    async fn next_event(&mut self) -> Result<SourceEvent, SourceError>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn source_error_transient_display() {
        let err = SourceError::Transient {
            message: "test".to_string(),
        };
        assert_eq!(err.to_string(), "transient error: test");
    }

    #[test]
    fn source_error_fatal_display() {
        let err = SourceError::Fatal {
            message: "boom".to_string(),
        };
        assert_eq!(err.to_string(), "fatal error: boom");
    }

    #[test]
    fn source_error_rate_limited_display() {
        let err = SourceError::RateLimited {
            retry_after_secs: 30,
        };
        assert_eq!(err.to_string(), "rate limited: retry after 30s");
    }

    #[test]
    fn source_status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&SourceStatus::Healthy).unwrap(),
            "\"healthy\""
        );
        assert_eq!(
            serde_json::to_string(&SourceStatus::Degraded).unwrap(),
            "\"degraded\""
        );
        assert_eq!(
            serde_json::to_string(&SourceStatus::Dead).unwrap(),
            "\"dead\""
        );
    }

    #[test]
    fn source_status_deserializes_snake_case() {
        let s: SourceStatus = serde_json::from_str("\"healthy\"").unwrap();
        assert_eq!(s, SourceStatus::Healthy);
        let s: SourceStatus = serde_json::from_str("\"degraded\"").unwrap();
        assert_eq!(s, SourceStatus::Degraded);
        let s: SourceStatus = serde_json::from_str("\"dead\"").unwrap();
        assert_eq!(s, SourceStatus::Dead);
    }
}
