//! Append-only framed event log with BLAKE3 hash chain and zstd compression.
//!
//! # Wire format
//!
//! File header (5 bytes, written once at offset 0):
//! ```text
//! MAGIC = b"EDGE"  (4 bytes)
//! VER   = 0x01     (1 byte)
//! ```
//!
//! Each frame:
//! ```text
//! LEN   (u32, little-endian) — byte count of the following zstd block
//! ZSTD  (LEN bytes)          — zstd-compressed JSON of EventEnvelope
//! CRC32 (u32, little-endian) — CRC32 of the zstd block bytes
//! ```
//!
//! CRC32 is checked before BLAKE3; it catches bit-flips cheaply.
//! Hashing operates on uncompressed bytes so the chain is independent of zstd version or level.
//!
//! # Hash chain
//!
//! For each envelope:
//! - `raw_payload_hash = blake3(payload)`
//! - `canonical_bytes  = JCS-style JSON of all envelope fields except `this_hash` and `payload`
//! - `this_hash        = blake3(prev_hash || canonical_bytes || payload)`
//! - First frame: `prev_hash = [0u8; 32]`
//!
//! # Durability
//!
//! `Writer::append` writes the frame but does not fsync per append (for throughput). Call
//! `Writer::sync()` at commit boundaries. `Drop` calls `sync()` best-effort.

pub mod envelope;
pub mod error;
mod frame;
pub mod reader;
pub mod writer;

pub use envelope::{ContentType, EnvelopeIn, EventEnvelope};
pub use error::LogError;
pub use reader::Reader;
pub use writer::Writer;
