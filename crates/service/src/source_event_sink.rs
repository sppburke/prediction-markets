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
//! durability uncertainty POISONS the writer instance. The sink discards that
//! instance, and websocket delivery stays blocked (REST fallback + dedup carry
//! the trades) until [`SourceEventSink::try_reopen`] succeeds: `Writer::open`
//! repairs only a scanner-proven incomplete tail, re-verifies the complete
//! prefix, and synchronizes it.

use std::path::{Path, PathBuf};

use pe_event_log::{AppendReceipt, EnvelopeIn, LogError, Writer};

/// Single-owner handle for the source event log. Owned by the ingest task; no
/// channel or command/ack layer — append-before-deliver is enforced by call
/// order inside that one task.
#[derive(Debug)]
pub struct SourceEventSink {
    path: PathBuf,
    writer: Option<Writer>,
    /// Crate-private one-shot faults for the coordinator's module tests only
    /// (#546): fail the next append, post-frame synchronization, or reopen exactly once.
    /// Absent from production builds.
    #[cfg(test)]
    fail_next_append: bool,
    #[cfg(test)]
    fail_next_sync: bool,
    #[cfg(test)]
    fail_next_reopen: bool,
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
            #[cfg(test)]
            fail_next_append: false,
            #[cfg(test)]
            fail_next_sync: false,
            #[cfg(test)]
            fail_next_reopen: false,
        })
    }

    /// Arm one append failure (poisons like a real append/sync error).
    #[cfg(test)]
    pub(crate) fn fail_next_append(&mut self) {
        self.fail_next_append = true;
    }

    /// Arm one synchronization uncertainty after a complete frame has reached the file.
    #[cfg(test)]
    pub(crate) fn fail_next_sync(&mut self) {
        self.fail_next_sync = true;
    }

    /// Arm one reopen failure (the sink stays poisoned for that attempt).
    #[cfg(test)]
    pub(crate) fn fail_next_reopen(&mut self) {
        self.fail_next_reopen = true;
    }

    /// Durably append one source event (append + sync). Writer-reported
    /// durability uncertainty discards that poisoned instance until
    /// [`Self::try_reopen`] succeeds.
    pub fn append_durable(&mut self, envelope: EnvelopeIn) -> Result<AppendReceipt, LogError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_append) {
            self.writer = None;
            return Err(LogError::Io(std::io::Error::other(
                "injected append failure",
            )));
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_sync) {
            let Some(writer) = self.writer.as_mut() else {
                return Err(LogError::Io(std::io::Error::other(
                    "source event sink poisoned",
                )));
            };
            writer.append(envelope)?;
            writer.flush()?;
            self.writer = None;
            return Err(LogError::Io(std::io::Error::other(
                "injected synchronization uncertainty",
            )));
        }
        let Some(writer) = self.writer.as_mut() else {
            return Err(LogError::Io(std::io::Error::other(
                "source event sink poisoned",
            )));
        };
        let result = writer.append_synced(envelope);
        if writer.poisoned().is_some() {
            // Partial frame bytes may be on disk. Drop the poisoned instance (releasing the
            // lock); a new writer repairs only a proven incomplete EOF and resynchronizes.
            self.writer = None;
        }
        result
    }

    /// Whether the sink is poisoned (websocket delivery must stay blocked).
    pub fn poisoned(&self) -> bool {
        self.writer
            .as_ref()
            .is_none_or(|writer| writer.poisoned().is_some())
    }

    /// Attempt recovery: reopen the log, which re-verifies the file header and
    /// hash chain. Only a successful reopen clears poison. Called from the
    /// reconnect cycle so recovery needs no dedicated timer.
    pub fn try_reopen(&mut self) -> bool {
        if !self.poisoned() {
            return true;
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_reopen) {
            return false;
        }
        match Writer::open(&self.path) {
            Ok(writer) => {
                self.writer = Some(writer);
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
        let receipt = sink.append_durable(envelope(br#"{"a":1}"#)).unwrap();
        assert_eq!(receipt.sequence.0, 0);
        assert!(!sink.poisoned());
        drop(sink);
        // Reopen verifies the chain and continues the sequence.
        let mut sink = SourceEventSink::open(&path).unwrap();
        let receipt = sink.append_durable(envelope(br#"{"b":2}"#)).unwrap();
        assert_eq!(
            receipt.sequence.0, 1,
            "reopen must continue the verified chain"
        );
    }

    #[test]
    fn poisoned_sink_refuses_appends_until_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut sink = SourceEventSink::open(&path).unwrap();
        // Force poison by hand (the failure path itself is exercised through
        // the writer's own error tests; here we prove the sink contract).
        sink.writer = None;
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
