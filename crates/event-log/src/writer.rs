use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use blake3::Hash;
use fs2::FileExt;
use pe_core_types::EventSeq;

use crate::envelope::{EnvelopeIn, EventEnvelope, HashInput, compute_hashes};
use crate::frame::{write_file_header, write_frame};
use crate::scanner::inspect_open;
use crate::{LogError, PoisonReason};

trait DurableWrite: Write + Send + Sync {
    fn sync_all(&self) -> std::io::Result<()>;
}

impl DurableWrite for File {
    fn sync_all(&self) -> std::io::Result<()> {
        File::sync_all(self)
    }
}

/// Single-writer handle for an append-only event log file.
///
/// Acquires an OS advisory exclusive lock on open. A partial final frame is truncated and
/// synchronized only after the shared scanner proves the preceding physical prefix. Any uncertain
/// append, flush, or synchronization permanently poisons this instance (#544).
pub struct Writer {
    path: PathBuf,
    inner: BufWriter<Box<dyn DurableWrite>>,
    next_seq: u64,
    last_hash: Hash,
    poison_reason: Option<PoisonReason>,
}

impl fmt::Debug for Writer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Writer")
            .field("path", &self.path)
            .field("next_seq", &self.next_seq)
            .field("last_hash", &self.last_hash)
            .field("poison_reason", &self.poison_reason)
            .finish_non_exhaustive()
    }
}

impl Writer {
    /// Open or create the log and acquire its exclusive advisory writer lock.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        file.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                LogError::Locked {
                    path: path.to_owned(),
                }
            } else {
                LogError::Io(error)
            }
        })?;

        let file_len = file.metadata()?.len();
        if file_len == 0 {
            write_file_header(&mut file)?;
            file.flush()?;
            file.sync_all()?;
            return Ok(Self::from_file(
                path.to_owned(),
                file,
                0,
                Hash::from_bytes([0; 32]),
            ));
        }

        let scan = inspect_open(path, &file)?;
        if scan.incomplete_tail.is_some() {
            file.set_len(scan.verified_tail.physical_tail)?;
        }
        // A new instance clears a prior instance's synchronization uncertainty only after the
        // complete scanner-verified prefix itself is synchronized.
        file.sync_all()?;
        file.seek(SeekFrom::Start(scan.verified_tail.physical_tail))?;
        let next_seq = match scan.verified_tail.last_sequence {
            None => 0,
            Some(last) => last.0.checked_add(1).ok_or(LogError::SequenceOverflow)?,
        };
        Ok(Self::from_file(
            path.to_owned(),
            file,
            next_seq,
            scan.verified_tail.last_hash,
        ))
    }

    fn from_file(path: PathBuf, file: File, next_seq: u64, last_hash: Hash) -> Self {
        Self {
            path,
            inner: BufWriter::new(Box::new(file)),
            next_seq,
            last_hash,
            poison_reason: None,
        }
    }

    /// Reason this writer can no longer prove its durable byte state.
    pub fn poisoned(&self) -> Option<&PoisonReason> {
        self.poison_reason.as_ref()
    }

    /// Append a new event. Does not fsync; call [`sync`](Self::sync) at commit boundaries.
    #[tracing::instrument(skip(self, envelope_in), fields(path = %self.path.display()))]
    pub fn append(&mut self, envelope_in: EnvelopeIn) -> Result<EventSeq, LogError> {
        self.ensure_healthy()?;
        let seq = EventSeq(self.next_seq);
        let next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or(LogError::SequenceOverflow)?;
        let prev_hash = self.last_hash;
        let (raw_payload_hash, _canonical, this_hash) = compute_hashes(HashInput {
            seq,
            source_id: &envelope_in.source_id,
            schema_version: envelope_in.schema_version,
            parser_version: envelope_in.parser_version,
            observed_at: &envelope_in.observed_at,
            received_at: &envelope_in.received_at,
            content_type: &envelope_in.content_type,
            prev_hash: &prev_hash,
            payload: &envelope_in.payload,
        })?;
        let envelope = EventEnvelope {
            seq,
            source_id: envelope_in.source_id,
            schema_version: envelope_in.schema_version,
            parser_version: envelope_in.parser_version,
            observed_at: envelope_in.observed_at,
            received_at: envelope_in.received_at,
            content_type: envelope_in.content_type,
            raw_payload_hash,
            prev_hash,
            this_hash,
            payload: envelope_in.payload,
        };
        let json = serde_json::to_vec(&envelope)?;
        if let Err(error) = write_frame(&mut self.inner, &json) {
            if matches!(&error, LogError::Io(_)) {
                self.poison_reason = Some(PoisonReason::Append);
            }
            return Err(error);
        }
        self.next_seq = next_seq;
        self.last_hash = this_hash;
        Ok(seq)
    }

    /// Flush buffered frame bytes. A failure permanently poisons the writer.
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.ensure_healthy()?;
        if let Err(error) = self.inner.flush() {
            self.poison_reason = Some(PoisonReason::Flush);
            return Err(LogError::Io(error));
        }
        Ok(())
    }

    /// Flush and issue `fsync`. A failure permanently poisons the writer.
    pub fn sync(&mut self) -> Result<(), LogError> {
        self.flush()?;
        if let Err(error) = self.inner.get_ref().sync_all() {
            self.poison_reason = Some(PoisonReason::Synchronize);
            return Err(LogError::Io(error));
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), LogError> {
        match self.poison_reason {
            Some(reason) => Err(LogError::Poisoned { reason }),
            None => Ok(()),
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.poison_reason.is_none()
            && let Err(error) = self.sync()
        {
            tracing::error!(path = %self.path.display(), error = %error, "Writer::drop failed to sync");
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::io;

    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
    use time::OffsetDateTime;

    use super::*;
    use crate::ContentType;

    #[derive(Debug, Clone, Copy)]
    enum Fault {
        Write,
        Flush,
        Sync,
    }

    struct FaultingTarget {
        fault: Fault,
        bytes: Vec<u8>,
    }

    impl Write for FaultingTarget {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if matches!(self.fault, Fault::Write) {
                return Err(io::Error::other("injected write uncertainty"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if matches!(self.fault, Fault::Flush) {
                return Err(io::Error::other("injected flush uncertainty"));
            }
            Ok(())
        }
    }

    impl DurableWrite for FaultingTarget {
        fn sync_all(&self) -> io::Result<()> {
            if matches!(self.fault, Fault::Sync) {
                return Err(io::Error::other("injected sync uncertainty"));
            }
            Ok(())
        }
    }

    fn writer(fault: Fault) -> Writer {
        Writer {
            path: PathBuf::from("injected.log"),
            inner: BufWriter::with_capacity(
                1,
                Box::new(FaultingTarget {
                    fault,
                    bytes: Vec::new(),
                }),
            ),
            next_seq: 0,
            last_hash: Hash::from_bytes([0; 32]),
            poison_reason: None,
        }
    }

    fn envelope() -> EnvelopeIn {
        EnvelopeIn {
            source_id: SourceId("test".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
            content_type: ContentType::Raw,
            payload: vec![1; 32],
        }
    }

    #[test]
    fn append_uncertainty_poisons_later_appends() {
        let mut writer = writer(Fault::Write);
        assert!(writer.append(envelope()).is_err());
        assert_eq!(writer.poisoned(), Some(&PoisonReason::Append));
        assert!(matches!(
            writer.append(envelope()),
            Err(LogError::Poisoned {
                reason: PoisonReason::Append
            })
        ));
    }

    #[test]
    fn flush_uncertainty_poisons_later_appends() {
        let mut writer = writer(Fault::Flush);
        assert!(writer.append(envelope()).is_ok());
        assert!(writer.flush().is_err());
        assert_eq!(writer.poisoned(), Some(&PoisonReason::Flush));
        assert!(matches!(
            writer.append(envelope()),
            Err(LogError::Poisoned { .. })
        ));
    }

    #[test]
    fn sync_uncertainty_poisons_later_appends() {
        let mut writer = writer(Fault::Sync);
        assert!(writer.append(envelope()).is_ok());
        assert!(writer.sync().is_err());
        assert_eq!(writer.poisoned(), Some(&PoisonReason::Synchronize));
        assert!(matches!(
            writer.append(envelope()),
            Err(LogError::Poisoned { .. })
        ));
    }
}
