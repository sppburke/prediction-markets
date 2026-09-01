use std::path::PathBuf;

use pe_core_types::EventSeq;
use thiserror::Error;

/// Operation whose uncertain effect permanently poisoned a writer (#544).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonReason {
    Append,
    Flush,
    Synchronize,
}

impl std::fmt::Display for PoisonReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Append => formatter.write_str("append"),
            Self::Flush => formatter.write_str("flush"),
            Self::Synchronize => formatter.write_str("synchronization"),
        }
    }
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("bad file header at {path}: expected EDGE\\x01")]
    BadHeader { path: PathBuf },

    #[error("file already locked: {path}")]
    Locked { path: PathBuf },

    #[error("CRC mismatch at seq {at_seq}, byte offset {byte_offset}")]
    CrcMismatch { at_seq: EventSeq, byte_offset: u64 },

    #[error(
        "chain broken at seq {at_seq}, byte offset {byte_offset}: expected {expected}, got {actual}"
    )]
    ChainBroken {
        at_seq: EventSeq,
        byte_offset: u64,
        expected: String,
        actual: String,
    },

    #[error(
        "sequence mismatch at byte offset {byte_offset}: expected {expected:?}, got {actual:?}"
    )]
    SequenceMismatch {
        byte_offset: u64,
        expected: EventSeq,
        actual: EventSeq,
    },

    #[error(
        "raw payload hash mismatch at seq {at_seq:?}, byte offset {byte_offset}: expected {expected}, got {actual}"
    )]
    RawPayloadHashMismatch {
        at_seq: EventSeq,
        byte_offset: u64,
        expected: String,
        actual: String,
    },

    #[error("chain hash computation failed at seq {at_seq:?}, byte offset {byte_offset}")]
    ChainComputation { at_seq: EventSeq, byte_offset: u64 },

    #[error("file truncated; next seq would be {at}, at byte offset {byte_offset}")]
    Truncated { at: EventSeq, byte_offset: u64 },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("envelope decode failed at seq {at_seq:?}, byte offset {byte_offset}: {source}")]
    EnvelopeDecode {
        at_seq: EventSeq,
        byte_offset: u64,
        source: serde_json::Error,
    },

    #[error("zstd error: {0}")]
    Compress(String),

    #[error("zstd decode failed at seq {at_seq:?}, byte offset {byte_offset}: {message}")]
    Decompress {
        at_seq: EventSeq,
        byte_offset: u64,
        message: String,
    },

    #[error("event-log writer is poisoned after uncertain {reason}")]
    Poisoned { reason: PoisonReason },

    #[error("event sequence exhausted u64")]
    SequenceOverflow,

    #[error("frame at byte offset {byte_offset} claims {len} bytes, exceeding {max}-byte limit")]
    FrameTooLarge {
        byte_offset: u64,
        len: u32,
        max: u32,
    },

    #[error(
        "log {path} verified tail {actual_tail} does not match the trusted expected tail {expected_tail}; refusing destructive repair"
    )]
    ExpectedTailMismatch {
        path: std::path::PathBuf,
        expected_tail: u64,
        actual_tail: u64,
    },
}
