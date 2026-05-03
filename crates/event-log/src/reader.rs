use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

use blake3::Hash;
use pe_core_types::EventSeq;

use crate::LogError;
use crate::envelope::{ChainError, EventEnvelope, verify_chain};
use crate::frame::{FrameReadError, HEADER_LEN, read_frame, verify_file_header};

/// Read-only access to an event log file.
pub struct Reader;

impl Reader {
    /// Open `path` and return an iterator that yields all frames up to the current EOF,
    /// validating the BLAKE3 hash chain at each step. Stops at EOF or on the first error.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn replay(
        path: impl AsRef<Path>,
    ) -> Result<impl Iterator<Item = Result<(EventSeq, EventEnvelope), LogError>>, LogError> {
        ReplayIter::open(path.as_ref(), None)
    }

    /// Open `path` and return an iterator that yields frames indefinitely, blocking for
    /// `poll_interval` when it reaches EOF, then retrying. Useful for live replay-while-writing.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn tail(
        path: impl AsRef<Path>,
        poll_interval: Duration,
    ) -> Result<impl Iterator<Item = Result<(EventSeq, EventEnvelope), LogError>>, LogError> {
        ReplayIter::open(path.as_ref(), Some(poll_interval))
    }
}

struct ReplayIter {
    reader: BufReader<File>,
    prev_hash: Hash,
    next_seq: u64,
    byte_offset: u64,
    /// None = replay (stop at EOF); Some = tail (poll at EOF).
    poll_interval: Option<Duration>,
    /// Set to true after a hard error; prevents further iteration.
    poisoned: bool,
}

impl ReplayIter {
    fn open(path: &Path, poll_interval: Option<Duration>) -> Result<Self, LogError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        verify_file_header(path, &mut reader)?;
        Ok(Self {
            reader,
            prev_hash: Hash::from_bytes([0u8; 32]),
            next_seq: 0,
            byte_offset: HEADER_LEN,
            poll_interval,
            poisoned: false,
        })
    }
}

impl Iterator for ReplayIter {
    type Item = Result<(EventSeq, EventEnvelope), LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        loop {
            // Capture the exact logical read position before calling read_frame so that,
            // in tail mode, a transient truncation can seek back to retry the same frame.
            // self.byte_offset tracks decompressed size (error-reporting approximation only),
            // so we ask the reader for the real position.
            let frame_start = self.reader.stream_position().unwrap_or(self.byte_offset);

            match read_frame(&mut self.reader, self.byte_offset) {
                Ok(None) => {
                    // EOF: stop (replay) or sleep and retry (tail).
                    match self.poll_interval {
                        None => return None,
                        Some(interval) => {
                            std::thread::sleep(interval);
                            continue;
                        }
                    }
                }

                Ok(Some(json)) => {
                    let frame_byte_offset = self.byte_offset;

                    let envelope: EventEnvelope = match serde_json::from_slice(&json) {
                        Ok(e) => e,
                        Err(e) => {
                            self.poisoned = true;
                            return Some(Err(LogError::Serde(e)));
                        }
                    };

                    // Validate chain.
                    match verify_chain(&envelope, &self.prev_hash) {
                        Ok(()) => {}
                        Err(ChainError::PrevHashMismatch { expected, actual }) => {
                            self.poisoned = true;
                            return Some(Err(LogError::ChainBroken {
                                at_seq: envelope.seq,
                                byte_offset: frame_byte_offset,
                                expected,
                                actual,
                            }));
                        }
                        Err(_) => {
                            self.poisoned = true;
                            return Some(Err(LogError::ChainBroken {
                                at_seq: envelope.seq,
                                byte_offset: frame_byte_offset,
                                expected: self.prev_hash.to_hex().to_string(),
                                actual: envelope.this_hash.to_hex().to_string(),
                            }));
                        }
                    }

                    // Advance byte offset (approximate — used only for error reporting).
                    self.byte_offset += 4 + json.len() as u64 + 4;
                    self.prev_hash = envelope.this_hash;
                    self.next_seq = envelope.seq.0 + 1;

                    let seq = envelope.seq;
                    return Some(Ok((seq, envelope)));
                }

                Err(FrameReadError::CrcMismatch { byte_offset }) => {
                    self.poisoned = true;
                    return Some(Err(LogError::CrcMismatch {
                        at_seq: EventSeq(self.next_seq),
                        byte_offset,
                    }));
                }

                Err(FrameReadError::Truncated { byte_offset }) => {
                    if let Some(interval) = self.poll_interval {
                        // Partial frame is a normal transient condition: the writer has
                        // flushed the LEN but not yet the full zstd block + CRC.
                        // Seek back to the frame start and retry after the poll interval.
                        if self.reader.seek(SeekFrom::Start(frame_start)).is_ok() {
                            std::thread::sleep(interval);
                            continue;
                        }
                    }
                    self.poisoned = true;
                    return Some(Err(LogError::Truncated {
                        at: EventSeq(self.next_seq),
                        byte_offset,
                    }));
                }

                Err(FrameReadError::Decompress(msg)) => {
                    self.poisoned = true;
                    return Some(Err(LogError::Compress(msg)));
                }

                Err(FrameReadError::Io(e)) => {
                    self.poisoned = true;
                    return Some(Err(LogError::Io(e)));
                }
            }
        }
    }
}
