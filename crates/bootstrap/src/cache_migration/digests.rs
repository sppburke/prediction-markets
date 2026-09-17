//! Incremental equivalents of the migration's canonical JSON commitments.

use serde::Serialize;
use sha2::{Digest as _, Sha256};

pub(super) struct JsonArrayDigest {
    hash: Sha256,
    populated: bool,
}

impl JsonArrayDigest {
    pub(super) fn new() -> Self {
        let mut hash = Sha256::new();
        hash.update(b"[");
        Self {
            hash,
            populated: false,
        }
    }

    pub(super) fn push(&mut self, value: &impl Serialize) -> serde_json::Result<()> {
        self.push_json(serde_json::to_string(value)?.as_bytes());
        Ok(())
    }

    // The caller serialized a typed wallet Vec once for both its receipt and
    // this commitment. Empty wallets must not introduce a separator.
    pub(super) fn extend_array(&mut self, json: &str) -> serde_json::Result<()> {
        let interior = json
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| {
                <serde_json::Error as serde::ser::Error>::custom("expected serialized array")
            })?;
        if !interior.is_empty() {
            self.push_json(interior.as_bytes());
        }
        Ok(())
    }

    fn push_json(&mut self, json: &[u8]) {
        if self.populated {
            self.hash.update(b",");
        }
        self.hash.update(json);
        self.populated = true;
    }

    pub(super) fn finish(mut self) -> String {
        self.hash.update(b"]");
        format!("{:x}", self.hash.finalize())
    }
}

pub(super) struct ReceiptSetDigest {
    array: JsonArrayDigest,
    suffix: String,
}

impl ReceiptSetDigest {
    pub(super) fn new(generation: u64, reference: &str, end: i64) -> serde_json::Result<Self> {
        // The original json! envelope sorts keys lexically. Receipt objects
        // (including their pages) must also pass through Value, unlike aggregates.
        let mut array = JsonArrayDigest::new();
        array.hash = Sha256::new();
        array.hash.update(format!(
            "{{\"fixed_end_unix\":{end},\"generation\":{generation},\"receipts\":["
        ));
        Ok(Self {
            array,
            suffix: format!(
                "],\"reference_sha256\":{}}}",
                serde_json::to_string(reference)?
            ),
        })
    }

    pub(super) fn push(&mut self, receipt: &impl Serialize) -> serde_json::Result<()> {
        self.array.push(&serde_json::to_value(receipt)?)
    }

    pub(super) fn finish(mut self) -> String {
        self.array.hash.update(self.suffix.as_bytes());
        format!("{:x}", self.array.hash.finalize())
    }
}
