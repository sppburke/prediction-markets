use serde::{Deserialize, Serialize};

/// Timestamp of an event as recorded by the external source. Always UTC, RFC3339 on the wire.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceTimestamp(#[serde(with = "time::serde::rfc3339")] pub time::OffsetDateTime);

/// Wall-clock time when this process first received a message from the source. RFC3339 on wire.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReceivedAt(#[serde(with = "time::serde::rfc3339")] pub time::OffsetDateTime);

impl ReceivedAt {
    pub fn now_utc() -> Self {
        Self(time::OffsetDateTime::now_utc())
    }
}

/// Floor(observed_at_ms / 1_000); groups timestamps into 1-second buckets for idempotency keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObservedAtBucket(pub i64);

impl ObservedAtBucket {
    pub const BUCKET_MS: i64 = 1_000;

    pub fn from_observed_at_ms(ms: i64) -> Self {
        Self(ms.div_euclid(Self::BUCKET_MS))
    }
}
