//! #530: service-owned single-writer sink for raw websocket source frames.
//!
//! A SEPARATE append-only log (`source_event_log_path`) from the paper fill
//! log, so the fill log's consumers (boot reconciliation, dispatch recovery,
//! the Supabase mirror) stay byte-identical — they deserialize every frame as
//! a paper fill and must never meet a source frame. The generic event-log
//! writer supplies the replay invariant (sequence, hash chain, raw payload).
//!
//! Durability contract (one state machine, review-settled): the caller appends
//! (with `sync`) BEFORE delivering the trade for decisions. Any append/sync
//! failure POISONS the sink — the `Writer` is discarded, because an append can
//! fail after partial frame writes and `sync` does not revalidate the chain —
//! and websocket delivery stays blocked (REST fallback + dedup carry the
//! trades) until [`SourceEventSink::try_reopen`] succeeds: `Writer::open`
//! re-verifies the header and chain tail, which is the required revalidation.

use std::path::{Path, PathBuf};

use pe_core_types::EventSeq;
use pe_event_log::{EnvelopeIn, LogError, Writer};

/// Single-owner handle for the source event log. Owned by the ingest task; no
/// channel or command/ack layer — append-before-deliver is enforced by call
/// order inside that one task.
#[derive(Debug)]
pub struct SourceEventSink {
    path: PathBuf,
    writer: Option<Writer>,
    poisoned: bool,
}

impl SourceEventSink {
    /// Open (or create) the source log. With the websocket enabled this runs at
    /// boot and a failure fails boot — an unwritable source log cannot satisfy
    /// the raw-evidence invariant, so the service refuses to start rather than
    /// silently running websocket-blind.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let path = path.as_ref().to_owned();
        let writer = Writer::open(&path)?;
        Ok(Self {
            path,
            writer: Some(writer),
            poisoned: false,
        })
    }

    /// Durably append one source event (append + sync). On any failure the
    /// sink poisons: the writer is discarded and every subsequent append fails
    /// until [`Self::try_reopen`] succeeds.
    pub fn append_durable(&mut self, envelope: EnvelopeIn) -> Result<EventSeq, LogError> {
        let Some(writer) = self.writer.as_mut() else {
            return Err(LogError::Io(std::io::Error::other(
                "source event sink poisoned",
            )));
        };
        let result = writer
            .append(envelope)
            .and_then(|seq| writer.sync().map(|()| seq));
        if result.is_err() {
            // Partial frame bytes may be on disk; only a reopen's chain
            // re-verification can prove the tail. Drop the writer (releases
            // the advisory lock) and poison.
            self.writer = None;
            self.poisoned = true;
        }
        result
    }

    /// Whether the sink is poisoned (websocket delivery must stay blocked).
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }

    /// Attempt recovery: reopen the log, which re-verifies the file header and
    /// hash chain. Only a successful reopen clears poison. Called from the
    /// reconnect cycle so recovery needs no dedicated timer.
    pub fn try_reopen(&mut self) -> bool {
        if !self.poisoned && self.writer.is_some() {
            return true;
        }
        match Writer::open(&self.path) {
            Ok(writer) => {
                self.writer = Some(writer);
                self.poisoned = false;
                true
            }
            Err(error) => {
                tracing::warn!(error = %error, path = %self.path.display(),
                    "source event log reopen failed; sink stays poisoned");
                false
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::ContentType;
    use time::OffsetDateTime;

    fn envelope(payload: &[u8]) -> EnvelopeIn {
        let now = OffsetDateTime::from_unix_timestamp(1_787_600_000).unwrap();
        EnvelopeIn {
            source_id: SourceId("polymarket-activity-ws".to_string()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(now),
            received_at: ReceivedAt(now),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn append_durable_then_reopen_replays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut sink = SourceEventSink::open(&path).unwrap();
        let seq = sink.append_durable(envelope(br#"{"a":1}"#)).unwrap();
        assert_eq!(seq.0, 0);
        assert!(!sink.poisoned());
        drop(sink);
        // Reopen verifies the chain and continues the sequence.
        let mut sink = SourceEventSink::open(&path).unwrap();
        let seq = sink.append_durable(envelope(br#"{"b":2}"#)).unwrap();
        assert_eq!(seq.0, 1, "reopen must continue the verified chain");
    }

    #[test]
    fn poisoned_sink_refuses_appends_until_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut sink = SourceEventSink::open(&path).unwrap();
        // Force poison by hand (the failure path itself is exercised through
        // the writer's own error tests; here we prove the sink contract).
        sink.writer = None;
        sink.poisoned = true;
        assert!(sink.append_durable(envelope(b"{}")).is_err());
        assert!(sink.poisoned());
        assert!(
            sink.try_reopen(),
            "reopen against a healthy file must clear poison"
        );
        assert!(!sink.poisoned());
        assert!(sink.append_durable(envelope(b"{}")).is_ok());
    }
}
