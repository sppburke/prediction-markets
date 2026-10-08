//! Dense, fixed-width receipt metadata shared with source checkpoint publication.

use std::io::{self, Read};

use blake3::Hash;
use pe_core_types::EventSeq;

use crate::AppendReceipt;

pub const RECEIPT_RECORD_LEN: u64 = 80;

/// Record position supplies the sequence; absent offsets retain the historical sentinel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptRecord {
    pub receipt: AppendReceipt,
    pub received_millis: i64,
    pub byte_offset: Option<u64>,
}

impl ReceiptRecord {
    pub fn encode(&self) -> [u8; 80] {
        let mut bytes = [0; 80];
        bytes[..32].copy_from_slice(self.receipt.this_hash.as_bytes());
        bytes[32..40].copy_from_slice(&self.received_millis.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.byte_offset.unwrap_or(u64::MAX).to_le_bytes());
        let checksum = blake3::hash(&bytes[..48]);
        bytes[48..].copy_from_slice(checksum.as_bytes());
        bytes
    }

    pub fn read(reader: &mut impl Read, sequence: EventSeq) -> io::Result<Self> {
        let mut bytes = [0; 80];
        reader.read_exact(&mut bytes)?;
        if bytes[48..] != *blake3::hash(&bytes[..48]).as_bytes() {
            return Err(io::Error::other("checkpoint receipt checksum mismatch"));
        }
        let hash = bytes[..32].try_into().map_err(io::Error::other)?;
        let received = bytes[32..40].try_into().map_err(io::Error::other)?;
        let offset = u64::from_le_bytes(bytes[40..48].try_into().map_err(io::Error::other)?);
        Ok(Self {
            receipt: AppendReceipt {
                sequence,
                this_hash: Hash::from_bytes(hash),
            },
            received_millis: i64::from_le_bytes(received),
            byte_offset: (offset != u64::MAX).then_some(offset),
        })
    }
}
