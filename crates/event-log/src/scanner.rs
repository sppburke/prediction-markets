//! One physical event-log scanner shared by append recovery and read-side verification.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use blake3::Hash;
use pe_core_types::EventSeq;

use crate::LogError;
use crate::envelope::{ChainError, EventEnvelope, verify_chain};
use crate::frame::{FrameReadError, HEADER_LEN, MAX_FRAME_BYTES, read_frame, verify_file_header};

/// Exact identity of a completely verified append-only log prefix (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogTailBinding {
    /// Canonical absolute path resolved from the configured path.
    pub path: PathBuf,
    /// Byte immediately after the last completely verified frame (or the header for an empty log).
    pub physical_tail: u64,
    /// Sequence of the final verified frame; `None` when the log has no frames.
    pub last_sequence: Option<EventSeq>,
    /// Chain hash of the final verified frame, or the all-zero genesis hash for an empty log.
    pub last_hash: Hash,
}

/// A partial frame proven to start at the end of the verified prefix and run to physical EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncompleteTail {
    pub byte_offset: u64,
    pub next_sequence: EventSeq,
}

/// Full scanner result. Only [`incomplete_tail`](Self::incomplete_tail) is repairable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOutcome {
    pub verified_tail: LogTailBinding,
    pub incomplete_tail: Option<IncompleteTail>,
}

/// Stateless entry point for physical log verification (#544).
pub struct Scanner;

impl Scanner {
    /// Inspect a log without mutating it. A partial final frame is reported separately;
    /// checksum, size, decode, sequence, raw-hash, and chain failures return typed errors.
    pub fn inspect(path: impl AsRef<Path>) -> Result<ScanOutcome, LogError> {
        let path = path.as_ref();
        let file = File::open(path)?;
        inspect_open(path, &file)
    }

    /// Require the whole physical file to be verified through EOF.
    pub fn verify(path: impl AsRef<Path>) -> Result<LogTailBinding, LogError> {
        let outcome = Self::inspect(path)?;
        match outcome.incomplete_tail {
            None => Ok(outcome.verified_tail),
            Some(incomplete) => Err(LogError::Truncated {
                at: incomplete.next_sequence,
                byte_offset: incomplete.byte_offset,
            }),
        }
    }

    /// Verify a recorded prefix and the complete current suffix in one pass.
    ///
    /// The boundary verdict is deferred until the scan reaches a clean end, so a typed frame
    /// failure in the suffix takes precedence over a boundary mismatch (#572).
    pub fn verify_prefix(binding: &LogTailBinding) -> Result<LogTailBinding, LogError> {
        let file = File::open(&binding.path)?;
        let mut observer = |_: u64, _: &EventEnvelope| {};
        let (outcome, verdict) = walk_locked(&binding.path, &file, Some(binding), &mut observer)?;
        if let Some(incomplete) = outcome.incomplete_tail {
            return Err(LogError::Truncated {
                at: incomplete.next_sequence,
                byte_offset: incomplete.byte_offset,
            });
        }
        verdict.require_match()?;
        Ok(outcome.verified_tail)
    }

    /// Verify and observe only the frames that begin below a sealed physical tail (#574).
    ///
    /// Returns `None` when the file ends before the tail, a frame crosses it, or its terminal
    /// sequence and hash do not match. Physical frame failures below the tail remain errors.
    pub fn walk_prefix(
        sealed: &LogTailBinding,
        observer: &mut dyn FnMut(u64, &EventEnvelope),
    ) -> Result<Option<LogTailBinding>, LogError> {
        let file = File::open(&sealed.path)?;
        let (outcome, verdict) = walk_locked(
            &sealed.path,
            &file,
            WalkRequest::until_expected_prefix(sealed),
            observer,
        )?;
        if let Some(incomplete) = outcome.incomplete_tail {
            return Err(LogError::Truncated {
                at: incomplete.next_sequence,
                byte_offset: incomplete.byte_offset,
            });
        }
        Ok((verdict == PrefixVerdict::Matched).then_some(outcome.verified_tail))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefixVerdict {
    NotRequested,
    Matched,
    Shorter,
    Mismatch,
}

impl PrefixVerdict {
    pub(crate) fn require_match(self) -> Result<(), LogError> {
        match self {
            Self::NotRequested | Self::Matched => Ok(()),
            Self::Shorter => Err(LogError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "event log is shorter than the recorded migration boundary",
            ))),
            Self::Mismatch => Err(LogError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "event log does not match the recorded migration boundary",
            ))),
        }
    }
}

struct PrefixTracker<'a> {
    expected: Option<&'a LogTailBinding>,
    /// The binding names one canonical file; a binding for another file can never match.
    same_file: bool,
    matched: bool,
}

impl<'a> PrefixTracker<'a> {
    fn new(expected: Option<&'a LogTailBinding>, resolved_path: &Path) -> Self {
        Self {
            expected,
            same_file: expected.is_none_or(|expected| expected.path == resolved_path),
            matched: false,
        }
    }

    fn observe(&mut self, state: &ScanState) {
        let Some(expected) = self.expected else {
            return;
        };
        if self.same_file
            && state.physical_tail == expected.physical_tail
            && state.next_sequence.checked_sub(1).map(EventSeq) == expected.last_sequence
            && state.previous_hash == expected.last_hash
        {
            self.matched = true;
        }
    }

    fn verdict(&self, final_state: &ScanState) -> PrefixVerdict {
        match self.expected {
            None => PrefixVerdict::NotRequested,
            Some(_) if !self.same_file => PrefixVerdict::Mismatch,
            Some(_) if self.matched => PrefixVerdict::Matched,
            Some(expected) if final_state.physical_tail < expected.physical_tail => {
                PrefixVerdict::Shorter
            }
            Some(_) => PrefixVerdict::Mismatch,
        }
    }

    fn verdict_before_read(&self, state: &ScanState) -> Option<PrefixVerdict> {
        let expected = self.expected?;
        (state.physical_tail >= expected.physical_tail).then(|| self.verdict(state))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkBoundary {
    PhysicalEof,
    ExpectedPrefix,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WalkRequest<'a> {
    expected_prefix: Option<&'a LogTailBinding>,
    boundary: WalkBoundary,
}

impl<'a> WalkRequest<'a> {
    fn until_expected_prefix(expected_prefix: &'a LogTailBinding) -> Self {
        Self {
            expected_prefix: Some(expected_prefix),
            boundary: WalkBoundary::ExpectedPrefix,
        }
    }
}

impl<'a> From<Option<&'a LogTailBinding>> for WalkRequest<'a> {
    fn from(expected_prefix: Option<&'a LogTailBinding>) -> Self {
        Self {
            expected_prefix,
            boundary: WalkBoundary::PhysicalEof,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ScanState {
    next_sequence: u64,
    previous_hash: Hash,
    physical_tail: u64,
}

impl ScanState {
    pub(crate) fn after_header() -> Self {
        Self {
            next_sequence: 0,
            previous_hash: Hash::from_bytes([0u8; 32]),
            physical_tail: HEADER_LEN,
        }
    }

    pub(crate) fn at_frame(sequence: EventSeq, previous_hash: Hash, byte_offset: u64) -> Self {
        Self {
            next_sequence: sequence.0,
            previous_hash,
            physical_tail: byte_offset,
        }
    }

    pub(crate) fn physical_tail(&self) -> u64 {
        self.physical_tail
    }
}

pub(crate) enum ScanStep {
    Frame(EventEnvelope),
    Eof,
    Incomplete(IncompleteTail),
}

pub(crate) fn read_verified_frame(
    reader: &mut (impl std::io::Read + std::io::Seek),
    state: &mut ScanState,
) -> Result<ScanStep, LogError> {
    let frame_start = state.physical_tail;
    let json = match read_frame(reader, frame_start) {
        Ok(Some(json)) => json,
        Ok(None) => return Ok(ScanStep::Eof),
        Err(FrameReadError::Truncated { byte_offset }) => {
            return Ok(ScanStep::Incomplete(IncompleteTail {
                byte_offset,
                next_sequence: EventSeq(state.next_sequence),
            }));
        }
        Err(FrameReadError::CrcMismatch { byte_offset }) => {
            return Err(LogError::CrcMismatch {
                at_seq: EventSeq(state.next_sequence),
                byte_offset,
            });
        }
        Err(FrameReadError::FrameTooLarge { byte_offset, len }) => {
            return Err(LogError::FrameTooLarge {
                byte_offset,
                len,
                max: MAX_FRAME_BYTES,
            });
        }
        Err(FrameReadError::Decompress(message)) => {
            return Err(LogError::Decompress {
                at_seq: EventSeq(state.next_sequence),
                byte_offset: frame_start,
                message,
            });
        }
        Err(FrameReadError::Io(error)) => return Err(LogError::Io(error)),
    };

    let envelope: EventEnvelope =
        serde_json::from_slice(&json).map_err(|source| LogError::EnvelopeDecode {
            at_seq: EventSeq(state.next_sequence),
            byte_offset: frame_start,
            source,
        })?;
    let expected_sequence = EventSeq(state.next_sequence);
    if envelope.seq != expected_sequence {
        return Err(LogError::SequenceMismatch {
            byte_offset: frame_start,
            expected: expected_sequence,
            actual: envelope.seq,
        });
    }

    let computed_raw_hash = blake3::hash(&envelope.payload);
    if computed_raw_hash != envelope.raw_payload_hash {
        return Err(LogError::RawPayloadHashMismatch {
            at_seq: envelope.seq,
            byte_offset: frame_start,
            expected: computed_raw_hash.to_hex().to_string(),
            actual: envelope.raw_payload_hash.to_hex().to_string(),
        });
    }

    match verify_chain(&envelope, &state.previous_hash) {
        Ok(()) => {}
        Err(ChainError::PrevHashMismatch { expected, actual }) => {
            return Err(LogError::ChainBroken {
                at_seq: envelope.seq,
                byte_offset: frame_start,
                expected,
                actual,
            });
        }
        Err(ChainError::ThisHashMismatch { expected, actual }) => {
            return Err(LogError::ChainBroken {
                at_seq: envelope.seq,
                byte_offset: frame_start,
                expected,
                actual,
            });
        }
        Err(ChainError::ComputeError) => {
            return Err(LogError::ChainComputation {
                at_seq: envelope.seq,
                byte_offset: frame_start,
            });
        }
    }

    state.physical_tail = reader.stream_position()?;
    state.previous_hash = envelope.this_hash;
    state.next_sequence = state
        .next_sequence
        .checked_add(1)
        .ok_or(LogError::SequenceOverflow)?;
    Ok(ScanStep::Frame(envelope))
}

pub(crate) fn inspect_open(path: &Path, file: &File) -> Result<ScanOutcome, LogError> {
    let mut observer = |_: u64, _: &EventEnvelope| {};
    let (outcome, _) = walk_locked(path, file, Option::<&LogTailBinding>::None, &mut observer)?;
    Ok(outcome)
}

/// Walk an already-open handle without mutating it (#572). The writer holds the exclusive lock
/// when a repair may follow; `verify_prefix` and `walk_prefix` walk a plain read handle.
///
/// Frame verification has one owner. The prefix verdict is returned separately so the writer can
/// reject a truncation into the trusted prefix before deciding whether tail repair is permitted.
pub(crate) fn walk_locked<'a>(
    path: &Path,
    file: &File,
    request: impl Into<WalkRequest<'a>>,
    observer: &mut dyn FnMut(u64, &EventEnvelope),
) -> Result<(ScanOutcome, PrefixVerdict), LogError> {
    let request = request.into();
    let resolved_path = std::fs::canonicalize(path)?;
    let mut reader = BufReader::new(file);
    verify_file_header(path, &mut reader)?;
    let mut state = ScanState::after_header();
    let mut prefix = PrefixTracker::new(request.expected_prefix, &resolved_path);
    prefix.observe(&state);

    loop {
        if request.boundary == WalkBoundary::ExpectedPrefix
            && let Some(verdict) = prefix.verdict_before_read(&state)
        {
            return Ok((
                ScanOutcome {
                    verified_tail: tail_binding(resolved_path, &state),
                    incomplete_tail: None,
                },
                verdict,
            ));
        }
        let frame_start = state.physical_tail();
        match read_verified_frame(&mut reader, &mut state)? {
            ScanStep::Frame(envelope) => {
                observer(frame_start, &envelope);
                prefix.observe(&state);
            }
            ScanStep::Eof => {
                let verdict = prefix.verdict(&state);
                return Ok((
                    ScanOutcome {
                        verified_tail: tail_binding(resolved_path, &state),
                        incomplete_tail: None,
                    },
                    verdict,
                ));
            }
            ScanStep::Incomplete(incomplete_tail) => {
                let verdict = prefix.verdict(&state);
                return Ok((
                    ScanOutcome {
                        verified_tail: tail_binding(resolved_path, &state),
                        incomplete_tail: Some(incomplete_tail),
                    },
                    verdict,
                ));
            }
        }
    }
}

fn tail_binding(path: PathBuf, state: &ScanState) -> LogTailBinding {
    LogTailBinding {
        path,
        physical_tail: state.physical_tail,
        last_sequence: state.next_sequence.checked_sub(1).map(EventSeq),
        last_hash: state.previous_hash,
    }
}
