use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use blake3::Hash;
use fs2::FileExt;
use pe_core_types::EventSeq;

use crate::LogError;
use crate::envelope::{
    ChainError, EnvelopeIn, EventEnvelope, HashInput, compute_hashes, verify_chain,
};
use crate::frame::{
    FrameReadError, HEADER_LEN, MAX_FRAME_BYTES, read_frame, verify_file_header, write_file_header,
    write_frame,
};

/// Single-writer handle for an append-only event log file.
///
/// Acquires an OS advisory exclusive lock on open; the lock is released when dropped.
/// Per-append fsync is skipped by default for throughput; call `sync()` at commit boundaries.
#[derive(Debug)]
pub struct Writer {
    path: PathBuf,
    inner: BufWriter<File>,
    next_seq: u64,
    last_hash: Hash,
}

impl Writer {
    /// Open (or create) the log at `path`, verify or write the file header, and acquire an
    /// exclusive advisory lock. Returns `LogError::Locked` if another writer holds the lock.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let path = path.as_ref();

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        file.try_lock_exclusive().map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                LogError::Locked {
                    path: path.to_owned(),
                }
            } else {
                LogError::Io(e)
            }
        })?;

        let file_len = file.metadata()?.len();

        if file_len == 0 {
            let mut buf = BufWriter::new(file);
            write_file_header(&mut buf)?;
            buf.flush()?;
            return Ok(Self {
                path: path.to_owned(),
                inner: buf,
                next_seq: 0,
                last_hash: Hash::from_bytes([0u8; 32]),
            });
        }

        // Existing file: scan from the beginning to find seq and last_hash, then seek to end.
        let (next_seq, last_hash) = scan_existing(path, &file)?;
        let mut file = file;
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path: path.to_owned(),
            inner: BufWriter::new(file),
            next_seq,
            last_hash,
        })
    }

    /// Append a new event. Computes seq, prev_hash, raw_payload_hash, and this_hash; writes one frame.
    /// Returns the assigned `EventSeq`. Does not fsync; call `sync()` at commit boundaries.
    #[tracing::instrument(skip(self, envelope_in), fields(path = %self.path.display()))]
    pub fn append(&mut self, envelope_in: EnvelopeIn) -> Result<EventSeq, LogError> {
        let seq = EventSeq(self.next_seq);
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
        write_frame(&mut self.inner, &json)?;

        self.next_seq += 1;
        self.last_hash = this_hash;

        Ok(seq)
    }

    /// Flush the write buffer and issue fsync. Call this at transaction commit boundaries.
    pub fn sync(&mut self) -> Result<(), LogError> {
        self.inner.flush()?;
        self.inner.get_ref().sync_all()?;
        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            tracing::error!(path = %self.path.display(), error = %e, "Writer::drop failed to sync");
        }
    }
}

/// Scan an existing file (from the beginning) to determine next_seq and last_hash.
fn scan_existing(path: &Path, file: &File) -> Result<(u64, Hash), LogError> {
    use std::io::BufReader;

    let mut reader = BufReader::new(file);
    verify_file_header(path, &mut reader)?;

    let mut seq: u64 = 0;
    let mut prev_hash = Hash::from_bytes([0u8; 32]);
    let mut byte_offset = HEADER_LEN;

    loop {
        match read_frame(&mut reader, byte_offset) {
            Ok(None) => break,
            Ok(Some(json)) => {
                let frame_json_len = json.len() as u64;
                let envelope: EventEnvelope = serde_json::from_slice(&json)?;

                match verify_chain(&envelope, &prev_hash) {
                    Ok(()) => {}
                    Err(ChainError::PrevHashMismatch { expected, actual }) => {
                        return Err(LogError::ChainBroken {
                            at_seq: envelope.seq,
                            byte_offset,
                            expected,
                            actual,
                        });
                    }
                    Err(_) => {
                        return Err(LogError::ChainBroken {
                            at_seq: envelope.seq,
                            byte_offset,
                            expected: prev_hash.to_hex().to_string(),
                            actual: envelope.this_hash.to_hex().to_string(),
                        });
                    }
                }

                prev_hash = envelope.this_hash;
                seq = envelope.seq.0 + 1;
                // Advance offset by frame overhead + compressed size approximation.
                // Exact tracking is not needed here; we seek to EOF after scanning.
                byte_offset += 4 + frame_json_len + 4;
            }
            Err(FrameReadError::Truncated { byte_offset: off }) => {
                return Err(LogError::Truncated {
                    at: EventSeq(seq),
                    byte_offset: off,
                });
            }
            Err(FrameReadError::CrcMismatch { byte_offset: off }) => {
                return Err(LogError::CrcMismatch {
                    at_seq: EventSeq(seq),
                    byte_offset: off,
                });
            }
            Err(FrameReadError::FrameTooLarge {
                byte_offset: off,
                len,
            }) => {
                return Err(LogError::FrameTooLarge {
                    byte_offset: off,
                    len,
                    max: MAX_FRAME_BYTES,
                });
            }
            Err(FrameReadError::Decompress(msg)) => return Err(LogError::Compress(msg)),
            Err(FrameReadError::Io(e)) => return Err(LogError::Io(e)),
        }
    }

    Ok((seq, prev_hash))
}
