use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use blake3::Hash;
use fs2::FileExt;
use pe_core_types::EventSeq;
use serde::{Deserialize, Serialize};

use crate::envelope::{EnvelopeIn, EventEnvelope, HashInput, compute_hashes};
use crate::frame::{HEADER_LEN, write_file_header, write_frame};
use crate::scanner::{LogTailBinding, PrefixVerdict, inspect_open, walk_locked};
use crate::{LogError, PoisonReason};

trait DurableWrite: Write + Send + Sync {
    fn sync_all(&self) -> std::io::Result<()>;
    fn len(&self) -> std::io::Result<u64>;
}

impl DurableWrite for File {
    fn sync_all(&self) -> std::io::Result<()> {
        File::sync_all(self)
    }

    fn len(&self) -> std::io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

/// Identity of one synchronized frame: its sequence and BLAKE3 chain hash (#545).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendReceipt {
    pub sequence: EventSeq,
    #[serde(with = "crate::envelope::hex_hash")]
    pub this_hash: blake3::Hash,
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
    cursor: u64,
    poison_reason: Option<PoisonReason>,
}

impl fmt::Debug for Writer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Writer")
            .field("path", &self.path)
            .field("next_seq", &self.next_seq)
            .field("last_hash", &self.last_hash)
            .field("cursor", &self.cursor)
            .field("poison_reason", &self.poison_reason)
            .finish_non_exhaustive()
    }
}

impl Writer {
    /// Open or create the log and acquire its exclusive advisory writer lock.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    /// Open for append, repairing a scanner-proven incomplete final frame by truncation.
    ///
    /// Repair is heuristic at the wire level: a torn final write and a tampered final
    /// length prefix are indistinguishable byte patterns, so ordinary open bounds the
    /// loss to the final frame (interior damage stays fatal — the chain hash covers
    /// every verified frame). Callers holding a trusted tail binding (migration and
    /// activation paths) must use [`Self::open_with_expected_tail`], which refuses
    /// destructive repair when the verified prefix disagrees with the binding (#544).
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
                HEADER_LEN,
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
            scan.verified_tail.physical_tail,
        ))
    }

    /// Open and verify an existing log under its exclusive writer lock (#572).
    ///
    /// Every verified frame is reported to `observer`. A scanner-proven incomplete final frame is
    /// repaired only when it begins outside a matching trusted prefix; missing files are never
    /// created by this entry point.
    pub fn open_verified(
        path: impl AsRef<Path>,
        expected_prefix: Option<&LogTailBinding>,
        observer: &mut dyn FnMut(u64, &EventEnvelope),
    ) -> Result<(Self, LogTailBinding), LogError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        file.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                LogError::Locked {
                    path: path.to_owned(),
                }
            } else {
                LogError::Io(error)
            }
        })?;

        if file.metadata()?.len() == 0 {
            if expected_prefix.is_some() {
                return Err(LogError::BadHeader {
                    path: path.to_owned(),
                });
            }
            write_file_header(&mut file)?;
            file.flush()?;
            file.sync_all()?;
            let binding = LogTailBinding {
                path: std::fs::canonicalize(path)?,
                physical_tail: HEADER_LEN,
                last_sequence: None,
                last_hash: Hash::from_bytes([0; 32]),
            };
            return Ok((
                Self::from_file(
                    path.to_owned(),
                    file,
                    0,
                    Hash::from_bytes([0; 32]),
                    HEADER_LEN,
                ),
                binding,
            ));
        }

        let (scan, verdict) = walk_locked(path, &file, expected_prefix, observer)?;
        // As `Scanner::verify_prefix`: with an expected prefix, a torn final frame is reported
        // before any boundary verdict unless the prefix matched (the tear then lies wholly after
        // it and is repaired below). A tear that cuts into the prefix can never match.
        if let Some(incomplete) = scan.incomplete_tail
            && expected_prefix.is_some()
            && verdict != PrefixVerdict::Matched
        {
            return Err(LogError::Truncated {
                at: incomplete.next_sequence,
                byte_offset: incomplete.byte_offset,
            });
        }
        verdict.require_match()?;

        if scan.incomplete_tail.is_some() {
            file.set_len(scan.verified_tail.physical_tail)?;
        }
        // As `open`: the verified prefix is synchronized whether or not a repair happened, so a
        // previous writer's synchronization uncertainty is cleared before the binding is published.
        file.sync_all()?;
        file.seek(SeekFrom::Start(scan.verified_tail.physical_tail))?;
        let next_seq = match scan.verified_tail.last_sequence {
            None => 0,
            Some(last) => last.0.checked_add(1).ok_or(LogError::SequenceOverflow)?,
        };
        let binding = scan.verified_tail;
        let writer = Self::from_file(
            path.to_owned(),
            file,
            next_seq,
            binding.last_hash,
            binding.physical_tail,
        );
        Ok((writer, binding))
    }

    /// Open for append with a trusted external tail binding: destructive repair is
    /// permitted only when the scanner-verified prefix equals the binding exactly, so
    /// a tampered or shortened prefix fails closed instead of being "repaired" away.
    /// Migration and activation paths, which hold stored bindings, must use this
    /// entry; ordinary startup without a binding uses [`Self::open`] (#544 review).
    pub fn open_with_expected_tail(
        path: impl AsRef<Path>,
        expected: &LogTailBinding,
    ) -> Result<Self, LogError> {
        let path = path.as_ref();
        let scan = crate::scanner::Scanner::inspect(path)?;
        let tail = &scan.verified_tail;
        // A binding names one canonical file (#572): a byte-identical twin must not authorize
        // this file's repair.
        if tail.path != expected.path {
            return Err(LogError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected tail binding names a different event log",
            )));
        }
        if tail.physical_tail != expected.physical_tail
            || tail.last_sequence != expected.last_sequence
            || tail.last_hash != expected.last_hash
        {
            return Err(LogError::ExpectedTailMismatch {
                path: path.to_owned(),
                expected_tail: expected.physical_tail,
                actual_tail: tail.physical_tail,
            });
        }
        Self::open(path)
    }

    fn from_file(path: PathBuf, file: File, next_seq: u64, last_hash: Hash, cursor: u64) -> Self {
        Self {
            path,
            inner: BufWriter::new(Box::new(file)),
            next_seq,
            last_hash,
            cursor,
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
        let frame_bytes = match write_frame(&mut self.inner, &json) {
            Ok(frame_bytes) => frame_bytes,
            Err(error) => {
                if matches!(&error, LogError::Io(_)) {
                    self.poison_reason = Some(PoisonReason::Append);
                }
                return Err(error);
            }
        };
        self.cursor = match self.cursor.checked_add(frame_bytes) {
            Some(cursor) => cursor,
            None => {
                self.poison_reason = Some(PoisonReason::Append);
                return Err(LogError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "event-log byte cursor overflowed",
                )));
            }
        };
        self.next_seq = next_seq;
        self.last_hash = this_hash;
        Ok(seq)
    }

    /// Append, flush, and fsync one frame; the receipt is returned only after synchronization.
    pub fn append_synced(&mut self, envelope_in: EnvelopeIn) -> Result<AppendReceipt, LogError> {
        let sequence = self.append(envelope_in)?;
        let receipt = AppendReceipt {
            sequence,
            this_hash: self.last_hash,
        };
        self.sync()?;
        Ok(receipt)
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

    /// Synchronize pending appends and bind the writer's verified byte cursor (#572).
    pub fn verified_tail(&mut self) -> Result<LogTailBinding, LogError> {
        self.sync()?;
        let actual_tail = self.inner.get_ref().len()?;
        if actual_tail != self.cursor {
            return Err(LogError::ExpectedTailMismatch {
                path: self.path.clone(),
                expected_tail: self.cursor,
                actual_tail,
            });
        }
        Ok(LogTailBinding {
            path: std::fs::canonicalize(&self.path)?,
            physical_tail: self.cursor,
            last_sequence: self.next_seq.checked_sub(1).map(EventSeq),
            last_hash: self.last_hash,
        })
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
    use crate::{ContentType, Reader};

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

        fn len(&self) -> io::Result<u64> {
            u64::try_from(self.bytes.len())
                .map_err(|_| io::Error::other("injected length overflow"))
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
            cursor: 0,
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

    #[test]
    fn verified_tail_refuses_a_poisoned_writer() {
        let mut writer = writer(Fault::Sync);
        assert!(writer.sync().is_err());
        assert!(matches!(
            writer.verified_tail(),
            Err(LogError::Poisoned {
                reason: PoisonReason::Synchronize
            })
        ));
    }

    /// PASS: a synchronized append returns the sequence and hash replayed from its frame.
    #[test]
    fn append_synced_receipt_matches_replayed_frame_hash() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("receipt.log");
        let mut writer = Writer::open(&path).unwrap();

        let receipt = writer.append_synced(envelope()).unwrap();
        let (sequence, replayed) = Reader::replay(&path).unwrap().next().unwrap().unwrap();

        assert_eq!(receipt.sequence, sequence);
        assert_eq!(receipt.this_hash, replayed.this_hash);
    }
}
