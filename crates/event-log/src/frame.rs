use std::io::{self, Read, Write};
use std::path::Path;

use crate::LogError;

pub const MAGIC: &[u8; 4] = b"EDGE";
pub const VERSION: u8 = 0x01;
/// File header length: MAGIC (4 bytes) + VER (1 byte).
pub const HEADER_LEN: u64 = 5;
const ZSTD_LEVEL: i32 = 3;
/// Maximum allowed compressed frame body size (64 MiB).
pub const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

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
pub fn write_frame(w: &mut impl Write, json_bytes: &[u8]) -> Result<u64, LogError> {
    let compressed =
        zstd::encode_all(json_bytes, ZSTD_LEVEL).map_err(|e| LogError::Compress(e.to_string()))?;

    let len = u32::try_from(compressed.len())
        .map_err(|_| LogError::Compress("compressed frame exceeds 4 GiB".into()))?;
    // Enforce the scan-side bound at write time (#544 review): a frame the writer
    // accepts must always be one its own scanner re-admits, or an append could
    // succeed and then make the log unopenable.
    if len > MAX_FRAME_BYTES {
        return Err(LogError::FrameTooLarge {
            byte_offset: 0,
            len,
            max: MAX_FRAME_BYTES,
        });
    }
    let crc = crc32fast::hash(&compressed);

    w.write_all(&len.to_le_bytes())?;
    w.write_all(&compressed)?;
    w.write_all(&crc.to_le_bytes())?;
    Ok(u64::from(len) + 8)
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
    read_exact_frame(r, &mut len_rest, byte_offset)?;

    let len_raw = u32::from_le_bytes([first[0], len_rest[0], len_rest[1], len_rest[2]]);
    if len_raw > MAX_FRAME_BYTES {
        return Err(FrameReadError::FrameTooLarge {
            byte_offset,
            len: len_raw,
        });
    }
    let len = usize::try_from(len_raw).map_err(|_| FrameReadError::FrameTooLarge {
        byte_offset,
        len: len_raw,
    })?;

    let mut compressed = Vec::with_capacity(len);
    r.by_ref()
        .take(u64::from(len_raw))
        .read_to_end(&mut compressed)?;
    if compressed.len() < len {
        // A short read can also mean LEN swallowed the real CRC and following frames.
        // A complete self-delimiting zstd frame with trailing bytes proves length corruption;
        // an unfinished zstd frame (including a partial header) remains an incomplete tail.
        if complete_zstd_frame_has_trailing_bytes(&compressed) {
            return Err(FrameReadError::CrcMismatch { byte_offset });
        }
        return Err(FrameReadError::Truncated { byte_offset });
    }

    let mut crc_buf = [0u8; 4];
    if let Err(error) = read_exact_frame(r, &mut crc_buf, byte_offset) {
        // A LEN that swallowed the real CRC and part of the next frame reads its body completely
        // and only runs short here; the body then holds a complete zstd frame plus trailing
        // bytes. A genuine partial write with a complete body and a short CRC holds exactly one
        // frame and stays an incomplete tail.
        if matches!(error, FrameReadError::Truncated { .. })
            && complete_zstd_frame_has_trailing_bytes(&compressed)
        {
            return Err(FrameReadError::CrcMismatch { byte_offset });
        }
        return Err(error);
    }

    let stored_crc = u32::from_le_bytes(crc_buf);
    let computed_crc = crc32fast::hash(&compressed);
    if computed_crc != stored_crc {
        return Err(FrameReadError::CrcMismatch { byte_offset });
    }

    let decompressed = zstd::decode_all(compressed.as_slice())
        .map_err(|e| FrameReadError::Decompress(e.to_string()))?;

    Ok(Some(decompressed))
}

fn complete_zstd_frame_has_trailing_bytes(compressed: &[u8]) -> bool {
    let Ok(decoder) = zstd::stream::read::Decoder::with_buffer(compressed) else {
        return false;
    };
    let mut decoder = decoder.single_frame();
    // Decode to a sink so this classification does not retain another decompressed payload.
    // The slice is already a BufRead; finish returns exactly the unconsumed input bytes.
    io::copy(&mut decoder, &mut io::sink()).is_ok() && !decoder.finish().is_empty()
}

fn read_exact_frame(
    reader: &mut impl Read,
    buffer: &mut [u8],
    byte_offset: u64,
) -> Result<(), FrameReadError> {
    reader.read_exact(buffer).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            FrameReadError::Truncated { byte_offset }
        } else {
            FrameReadError::Io(error)
        }
    })
}

#[derive(Debug)]
pub enum FrameReadError {
    Truncated { byte_offset: u64 },
    CrcMismatch { byte_offset: u64 },
    FrameTooLarge { byte_offset: u64, len: u32 },
    Decompress(String),
    Io(io::Error),
}

impl From<io::Error> for FrameReadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
