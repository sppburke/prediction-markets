//! Incremental equivalents of the migration's canonical JSON commitments.

use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// C3's commitment, shared by admission and finalization. The stored drop array
/// is a JSON string in this envelope, not a nested array.
pub(super) fn certificate_digest(connection: &rusqlite::Connection) -> rusqlite::Result<String> {
    let mut statement = connection.prepare(
        "SELECT wallet_hex, generation, newest_source_unix, newest_trade_unix,
                aggregate_count, source_row_count, ordered_digest, scope_drops_json
         FROM activity_wallet_history_v3 ORDER BY wallet_hex",
    )?;
    let mut rows = statement.query([])?;
    let mut digest = JsonArrayDigest::new();
    while let Some(row) = rows.next()? {
        digest
            .push(&serde_json::json!({
                "wallet_hex": row.get::<_, String>(0)?,
                "generation": row.get::<_, i64>(1)?,
                "newest_source_unix": row.get::<_, Option<i64>>(2)?,
                "newest_trade_unix": row.get::<_, Option<i64>>(3)?,
                "aggregate_count": row.get::<_, i64>(4)?,
                "source_row_count": row.get::<_, i64>(5)?,
                "ordered_digest": row.get::<_, String>(6)?,
                "scope_drops_json": row.get::<_, String>(7)?,
            }))
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    }
    Ok(digest.finish())
}

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

    // One element's already-serialized canonical JSON.
    pub(super) fn push_json(&mut self, json: &[u8]) {
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
