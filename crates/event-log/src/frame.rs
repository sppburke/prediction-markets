use std::io::{self, Read, Write};
use std::path::Path;

use crate::LogError;

pub const MAGIC: &[u8; 4] = b"EDGE";
pub const VERSION: u8 = 0x01;
/// File header length: MAGIC (4 bytes) + VER (1 byte).
pub const HEADER_LEN: u64 = 5;
const ZSTD_LEVEL: i32 = 3;

pub fn write_file_header(w: &mut impl Write) -> Result<(), LogError> {
    w.write_all(MAGIC)?;
    w.write_all(&[VERSION])?;
    Ok(())
}

pub fn verify_file_header(path: &Path, r: &mut impl Read) -> Result<(), LogError> {
    let mut buf = [0u8; 5];
    match r.read_exact(&mut buf) {
        Ok(()) => {}
        Err(_) => {
            return Err(LogError::BadHeader {
                path: path.to_owned(),
            });
        }
    }
    if &buf[..4] != MAGIC || buf[4] != VERSION {
        return Err(LogError::BadHeader {
            path: path.to_owned(),
        });
    }
    Ok(())
}

/// Compress `json_bytes` and write one frame: `LEN(u32 LE) | zstd_block | CRC32(u32 LE)`.
/// CRC32 covers the zstd block bytes only.
pub fn write_frame(w: &mut impl Write, json_bytes: &[u8]) -> Result<(), LogError> {
    let compressed =
        zstd::encode_all(json_bytes, ZSTD_LEVEL).map_err(|e| LogError::Compress(e.to_string()))?;

    let len = u32::try_from(compressed.len())
        .map_err(|_| LogError::Compress("compressed frame exceeds 4 GiB".into()))?;
    let crc = crc32fast::hash(&compressed);

    w.write_all(&len.to_le_bytes())?;
    w.write_all(&compressed)?;
    w.write_all(&crc.to_le_bytes())?;
    Ok(())
}

/// Read one frame from `r` starting at `byte_offset`.
///
/// Returns `Ok(Some(json_bytes))` on success, `Ok(None)` on clean EOF (no bytes read for this frame),
/// or a `FrameReadError` on truncation, CRC failure, or I/O error.
pub fn read_frame(r: &mut impl Read, byte_offset: u64) -> Result<Option<Vec<u8>>, FrameReadError> {
    // One-byte probe to distinguish clean EOF from truncated LEN field.
    let mut first = [0u8; 1];
    if r.read(&mut first)? == 0 {
        return Ok(None);
    }

    let mut len_rest = [0u8; 3];
    r.read_exact(&mut len_rest)
        .map_err(|_| FrameReadError::Truncated { byte_offset })?;

    let len = u32::from_le_bytes([first[0], len_rest[0], len_rest[1], len_rest[2]]) as usize;

    let mut compressed = vec![0u8; len];
    r.read_exact(&mut compressed)
        .map_err(|_| FrameReadError::Truncated { byte_offset })?;

    let mut crc_buf = [0u8; 4];
    r.read_exact(&mut crc_buf)
        .map_err(|_| FrameReadError::Truncated { byte_offset })?;

    let stored_crc = u32::from_le_bytes(crc_buf);
    let computed_crc = crc32fast::hash(&compressed);
    if computed_crc != stored_crc {
        return Err(FrameReadError::CrcMismatch { byte_offset });
    }

    let decompressed = zstd::decode_all(compressed.as_slice())
        .map_err(|e| FrameReadError::Decompress(e.to_string()))?;

    Ok(Some(decompressed))
}

#[derive(Debug)]
pub enum FrameReadError {
    Truncated { byte_offset: u64 },
    CrcMismatch { byte_offset: u64 },
    Decompress(String),
    Io(io::Error),
}

impl From<io::Error> for FrameReadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
