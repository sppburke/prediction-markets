//! Neutral, replay-complete evidence for one external HTTP attempt.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Canonical, secret-free identity of one account-scoped HTTP request.
///
/// The ordered query is retained exactly as sent. `partition` names the logical page family
/// (for example `redeemable=false`), while `offset` is kept separately so an empty page still
/// proves which slice of which custody account was requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedHttpRequestDescriptor {
    pub account_id: String,
    pub credential_fingerprint: String,
    pub custody_wallet: String,
    pub method: String,
    pub path: String,
    pub partition: String,
    pub offset: u64,
    pub ordered_query: Vec<(String, String)>,
}

/// Reserved sanitized response-header name carrying the canonical request-descriptor hash.
pub const REQUEST_DESCRIPTOR_HASH_HEADER: &str = "x-pe-request-descriptor-blake3";

/// Normalized transport failure classes shared by source, venue, and execution boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum TransportErrorClass {
    #[error("timeout")]
    Timeout,
    #[error("connect")]
    Connect,
    #[error("body_read")]
    BodyRead,
    #[error("request_build")]
    RequestBuild,
    #[error("cancelled")]
    Cancelled,
    #[error("other")]
    Other,
}

/// A complete sanitized HTTP response observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawHttpResponse {
    pub source_id: String,
    pub endpoint_kind: String,
    pub method: String,
    pub path: String,
    pub ordered_query: Vec<(String, String)>,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub attempt_ordinal: u32,
    /// Remote-system time when supplied by a valid response `Date` header.
    pub source_at: Option<OffsetDateTime>,
    /// Local time immediately before the request attempt began.
    pub observed_at: OffsetDateTime,
    /// Local time after the body was captured completely.
    pub received_at: OffsetDateTime,
    pub schema_version: u16,
    pub parser_version: u16,
    pub adapter_version: String,
}

/// A transport failure with the same request identity as a response and no fabricated body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTransportFailure {
    pub source_id: String,
    pub endpoint_kind: String,
    pub method: String,
    pub path: String,
    pub ordered_query: Vec<(String, String)>,
    pub attempt_ordinal: u32,
    pub observed_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub error_class: TransportErrorClass,
    pub schema_version: u16,
    pub parser_version: u16,
    pub adapter_version: String,
}

/// A complete observation of a local or otherwise non-HTTP input artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawArtifactObservation {
    pub source_id: String,
    pub artifact_kind: String,
    pub path: String,
    pub body: Vec<u8>,
    pub observed_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub schema_version: u16,
    pub parser_version: u16,
    pub adapter_version: String,
}

/// Exactly one response or transport failure for an attempted request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "observation")]
pub enum RawHttpAttempt {
    Response(RawHttpResponse),
    TransportFailure(RawTransportFailure),
}

/// One ordered external-input observation, independent of its transport protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "observation")]
pub enum RawEvidence {
    HttpResponse(RawHttpResponse),
    HttpTransportFailure(RawTransportFailure),
    Artifact(RawArtifactObservation),
}

impl From<RawHttpAttempt> for RawEvidence {
    fn from(value: RawHttpAttempt) -> Self {
        match value {
            RawHttpAttempt::Response(response) => Self::HttpResponse(response),
            RawHttpAttempt::TransportFailure(failure) => Self::HttpTransportFailure(failure),
        }
    }
}
