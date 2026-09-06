use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::{Duration, Instant};

use pe_core_types::EventSeq;

use blake3::Hash;

use crate::frame::verify_file_header;
use crate::scanner::{LogTailBinding, ScanState, ScanStep, Scanner, read_verified_frame};
use crate::{EventEnvelope, LogError};

/// How long tail mode retries a partial frame before giving up.
const TRUNCATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Read-only access to an event log file.
pub struct Reader;

impl Reader {
    /// Open `path` and return an iterator over scanner-verified frames through current EOF.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn replay(
        path: impl AsRef<Path>,
    ) -> Result<impl Iterator<Item = Result<(EventSeq, EventEnvelope), LogError>>, LogError> {
        Ok(Self::replay_with_offsets(path)?
            .map(|item| item.map(|(_byte_offset, sequence, envelope)| (sequence, envelope))))
    }

    /// Open `path` and return scanner-verified frames with each frame's starting byte offset.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn replay_with_offsets(
        path: impl AsRef<Path>,
    ) -> Result<impl Iterator<Item = Result<(u64, EventSeq, EventEnvelope), LogError>>, LogError>
    {
        // Verify through physical EOF before exposing frame 0. This prevents a consumer from
        // mutating replay state from a valid prefix before discovering corrupt interior bytes.
        Scanner::verify(path.as_ref())?;
        ReplayIter::open(path.as_ref(), None)
    }

    /// Read and verify the frame at an already scanner-proven byte offset.
    ///
    /// The expected sequence and preceding chain hash bind this isolated read to the verified
    /// metadata retained by the caller, without rescanning or retaining the rest of a large log.
    pub fn read_at(
        path: impl AsRef<Path>,
        byte_offset: u64,
        expected_sequence: EventSeq,
        expected_previous_hash: Hash,
    ) -> Result<EventEnvelope, LogError> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        verify_file_header(path, &mut reader)?;
        reader.seek(SeekFrom::Start(byte_offset))?;
        let mut state = ScanState::at_frame(expected_sequence, expected_previous_hash, byte_offset);
        match read_verified_frame(&mut reader, &mut state)? {
            ScanStep::Frame(envelope) => Ok(envelope),
            ScanStep::Eof => Err(LogError::Truncated {
                at: expected_sequence,
                byte_offset,
            }),
            ScanStep::Incomplete(incomplete) => Err(LogError::Truncated {
                at: incomplete.next_sequence,
                byte_offset: incomplete.byte_offset,
            }),
        }
    }

    /// Verify the complete file and report its exact resolved-path/physical-tail binding.
    pub fn verified_tail(path: impl AsRef<Path>) -> Result<LogTailBinding, LogError> {
        Scanner::verify(path)
    }

    /// Open `path` and return an iterator that waits at EOF for newly appended frames.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn tail(
        path: impl AsRef<Path>,
        poll_interval: Duration,
    ) -> Result<impl Iterator<Item = Result<(EventSeq, EventEnvelope), LogError>>, LogError> {
        Ok(ReplayIter::open(path.as_ref(), Some(poll_interval))?
            .map(|item| item.map(|(_byte_offset, sequence, envelope)| (sequence, envelope))))
    }
}

struct ReplayIter {
    reader: BufReader<File>,
    state: ScanState,
    poll_interval: Option<Duration>,
    poisoned: bool,
    truncation_deadline: Option<Instant>,
}

impl ReplayIter {
    fn open(path: &Path, poll_interval: Option<Duration>) -> Result<Self, LogError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        verify_file_header(path, &mut reader)?;
        Ok(Self {
            reader,
            state: ScanState::after_header(),
            poll_interval,
            poisoned: false,
            truncation_deadline: None,
        })
    }
}

impl Iterator for ReplayIter {
    type Item = Result<(u64, EventSeq, EventEnvelope), LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        loop {
            let frame_start = self.state.physical_tail();
            match read_verified_frame(&mut self.reader, &mut self.state) {
                Ok(ScanStep::Frame(envelope)) => {
                    self.truncation_deadline = None;
                    return Some(Ok((frame_start, envelope.seq, envelope)));
                }
                Ok(ScanStep::Eof) => match self.poll_interval {
                    None => return None,
                    Some(interval) => std::thread::sleep(interval),
                },
                Ok(ScanStep::Incomplete(incomplete)) => {
                    if let Some(interval) = self.poll_interval {
                        let deadline = self
                            .truncation_deadline
                            .get_or_insert_with(|| Instant::now() + TRUNCATION_TIMEOUT);
                        if Instant::now() < *deadline
                            && self.reader.seek(SeekFrom::Start(frame_start)).is_ok()
                        {
                            std::thread::sleep(interval);
                            continue;
                        }
                        self.truncation_deadline = None;
                    }
                    self.poisoned = true;
                    return Some(Err(LogError::Truncated {
                        at: incomplete.next_sequence,
                        byte_offset: incomplete.byte_offset,
                    }));
                }
                Err(error) => {
                    self.poisoned = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
    use time::OffsetDateTime;

    use super::*;
    use crate::{ContentType, EnvelopeIn, Writer};

    fn envelope(payload: &[u8]) -> EnvelopeIn {
        EnvelopeIn {
            source_id: SourceId("reader-offset-test".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        }
    }

    /// PASS: offset replay identifies each frame start and an isolated read reproduces the exact
    /// payload while checking its sequence and preceding chain hash.
    #[test]
    fn replay_offsets_support_verified_isolated_reads() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("offsets.log");
        let mut writer = Writer::open(&path).unwrap();
        let first = writer.append_synced(envelope(b"first")).unwrap();
        let second = writer.append_synced(envelope(b"second")).unwrap();
        drop(writer);

        let replayed = Reader::replay_with_offsets(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(replayed.len(), 2);
        assert!(replayed[0].0 < replayed[1].0);
        assert_eq!(replayed[0].1, first.sequence);
        assert_eq!(replayed[1].1, second.sequence);

        let isolated =
            Reader::read_at(&path, replayed[1].0, second.sequence, first.this_hash).unwrap();
        assert_eq!(isolated.payload, b"second");
        assert_eq!(isolated.this_hash, second.this_hash);
    }
}
