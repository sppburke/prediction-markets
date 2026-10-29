//! Incremental equivalents of the migration's canonical JSON commitments.

use serde::Serialize;
use sha2::{Digest as _, Sha256};

/// C3's commitment, shared by admission and finalization: every certificate, then a
/// seeded cache's carried drops. The stored drop array is a JSON string in this
/// envelope, not a nested array.
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
    // A seeded cache's carried drops wait here for their wallet's first certificate.
    let carried: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'activity_seed_drops_v3')",
        [],
        |row| row.get(0),
    )?;
    if carried {
        let mut statement = connection.prepare(
            "SELECT wallet_hex, scope_drops_json FROM activity_seed_drops_v3 ORDER BY wallet_hex",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            digest
                .push(&serde_json::json!({
                    "carried_scope_drops_json": row.get::<_, String>(1)?,
                    "wallet_hex": row.get::<_, String>(0)?,
                }))
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
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

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "a broken fixture must fail its test")]
mod tests {
    use super::*;

    /// The certificate digest of one fixed certificate, pinned beside its Python twin
    /// (`test_push_ranking_filter.py`).
    const CERTIFICATE_DIGEST: &str =
        "6b1e572a222ca687fc9cdf10454f2dc0554776fabf858dfda9ccd44f169755e9";
    /// The same certificate followed by one carried drop (twin: `test_push_ranking_filter.py`).
    const CARRIED_CERTIFICATE_DIGEST: &str =
        "70db2b47614ff22f4bb229b227401a4ab438bfad821b4842b93d9e034c4f92ac";

    fn one_certificate() -> rusqlite::Connection {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TABLE activity_wallet_history_v3 (wallet_hex TEXT PRIMARY KEY,
                 generation INTEGER NOT NULL, newest_source_unix INTEGER NULL,
                 newest_trade_unix INTEGER NULL, aggregate_count INTEGER NOT NULL,
                 source_row_count INTEGER NOT NULL, ordered_digest TEXT NOT NULL,
                 scope_drops_json TEXT NOT NULL);
                 INSERT INTO activity_wallet_history_v3 VALUES
                 ('0x1111111111111111111111111111111111111111', 1, 1800000000, NULL, 3, 4,
                  '{}', '[]')",
                "a".repeat(64)
            ))
            .unwrap();
        connection
    }

    /// FAIL: a cache without carried drops no longer digests to its pinned bytes.
    #[test]
    fn certificate_digest_keeps_its_pinned_bytes() {
        assert_eq!(
            certificate_digest(&one_certificate()).unwrap(),
            CERTIFICATE_DIGEST
        );
    }

    /// FAIL: a seeded cache's carried drops are not committed after its certificates in the
    /// pinned bytes the Python twin checks, or an empty carry table changes the digest.
    #[test]
    fn certificate_digest_commits_carried_drops() {
        let connection = one_certificate();
        connection
            .execute_batch(
                "CREATE TABLE activity_seed_drops_v3 (wallet_hex TEXT PRIMARY KEY,
                 scope_drops_json TEXT NOT NULL)",
            )
            .unwrap();
        assert_eq!(
            certificate_digest(&connection).unwrap(),
            CERTIFICATE_DIGEST
        );
        connection
            .execute(
                "INSERT INTO activity_seed_drops_v3 VALUES
                 ('0x2222222222222222222222222222222222222222', ?1)",
                [r#"[{"cause":"conversion","dropped_at_unix":7,"scope_id":"0xe","scope_kind":"market"}]"#],
            )
            .unwrap();
        assert_eq!(
            certificate_digest(&connection).unwrap(),
            CARRIED_CERTIFICATE_DIGEST
        );
    }
}
