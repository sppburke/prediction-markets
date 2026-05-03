use std::path::PathBuf;

use pe_core_types::EventSeq;
use thiserror::Error;

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

    #[error("file truncated; next seq would be {at}, at byte offset {byte_offset}")]
    Truncated { at: EventSeq, byte_offset: u64 },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("zstd error: {0}")]
    Compress(String),

    #[error("frame at byte offset {byte_offset} claims {len} bytes, exceeding {max}-byte limit")]
    FrameTooLarge {
        byte_offset: u64,
        len: u32,
        max: u32,
    },
}
