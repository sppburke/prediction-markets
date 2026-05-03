use blake3::Hash;
use pe_core_types::{EventSeq, ReceivedAt, SourceId, SourceTimestamp};
use serde::{Deserialize, Serialize};

use crate::LogError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentType {
    Json,
    MessagePack,
    Cbor,
    Raw,
}

/// A single event stored in the append-only log. All hash fields are filled by the Writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub seq: EventSeq,
    pub source_id: SourceId,
    pub schema_version: u32,
    pub parser_version: u32,
    pub observed_at: SourceTimestamp,
    pub received_at: ReceivedAt,
    pub content_type: ContentType,
    #[serde(with = "hex_hash")]
    pub raw_payload_hash: Hash,
    #[serde(with = "hex_hash")]
    pub prev_hash: Hash,
    #[serde(with = "hex_hash")]
    pub this_hash: Hash,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

/// User-supplied fields for appending a new event. Writer fills seq, prev_hash, this_hash, raw_payload_hash.
pub struct EnvelopeIn {
    pub source_id: SourceId,
    pub schema_version: u32,
    pub parser_version: u32,
    pub observed_at: SourceTimestamp,
    pub received_at: ReceivedAt,
    pub content_type: ContentType,
    pub payload: Vec<u8>,
}

/// Input to `compute_hashes`. Groups the envelope metadata fields to stay under clippy's argument limit.
pub struct HashInput<'a> {
    pub seq: EventSeq,
    pub source_id: &'a SourceId,
    pub schema_version: u32,
    pub parser_version: u32,
    pub observed_at: &'a SourceTimestamp,
    pub received_at: &'a ReceivedAt,
    pub content_type: &'a ContentType,
    pub prev_hash: &'a Hash,
    pub payload: &'a [u8],
}

/// Intermediate form for computing this_hash — envelope metadata without this_hash or payload.
/// Fields declared in lexicographic order; serde_json with no preserve_order feature uses BTreeMap
/// for Object, so serialization is key-sorted (JCS-compatible) regardless of declaration order.
#[derive(Serialize)]
struct CanonicalFields<'a> {
    content_type: &'a ContentType,
    observed_at: &'a SourceTimestamp,
    parser_version: u32,
    prev_hash: String,
    raw_payload_hash: String,
    received_at: &'a ReceivedAt,
    schema_version: u32,
    seq: u64,
    source_id: &'a SourceId,
}

/// Compute BLAKE3-chained hash fields for a new envelope.
///
/// Returns `(raw_payload_hash, canonical_bytes, this_hash)`.
/// `canonical_bytes` is the JCS-style JSON of the metadata fields (sorted keys, no whitespace).
pub fn compute_hashes(input: HashInput<'_>) -> Result<(Hash, Vec<u8>, Hash), LogError> {
    let raw_payload_hash = blake3::hash(input.payload);

    let fields = CanonicalFields {
        content_type: input.content_type,
        observed_at: input.observed_at,
        parser_version: input.parser_version,
        prev_hash: input.prev_hash.to_hex().to_string(),
        raw_payload_hash: raw_payload_hash.to_hex().to_string(),
        received_at: input.received_at,
        schema_version: input.schema_version,
        seq: input.seq.0,
        source_id: input.source_id,
    };
    // to_value produces a Value::Object backed by BTreeMap (no preserve_order) → keys sorted.
    // to_vec on that value is compact JSON with no whitespace. This is JCS-equivalent for flat objects.
    let canonical_value = serde_json::to_value(&fields)?;
    let canonical_bytes = serde_json::to_vec(&canonical_value)?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(input.prev_hash.as_bytes());
    hasher.update(&canonical_bytes);
    hasher.update(input.payload);
    let this_hash = hasher.finalize();

    Ok((raw_payload_hash, canonical_bytes, this_hash))
}

/// Verify the hash chain for a decoded envelope. Returns `Ok(())` if the chain is valid.
pub fn verify_chain(envelope: &EventEnvelope, expected_prev_hash: &Hash) -> Result<(), ChainError> {
    if &envelope.prev_hash != expected_prev_hash {
        return Err(ChainError::PrevHashMismatch {
            expected: expected_prev_hash.to_hex().to_string(),
            actual: envelope.prev_hash.to_hex().to_string(),
        });
    }

    let result = compute_hashes(HashInput {
        seq: envelope.seq,
        source_id: &envelope.source_id,
        schema_version: envelope.schema_version,
        parser_version: envelope.parser_version,
        observed_at: &envelope.observed_at,
        received_at: &envelope.received_at,
        content_type: &envelope.content_type,
        prev_hash: &envelope.prev_hash,
        payload: &envelope.payload,
    });

    match result {
        Ok((_, _, expected_this)) if expected_this == envelope.this_hash => Ok(()),
        Ok((_, _, expected_this)) => Err(ChainError::ThisHashMismatch {
            expected: expected_this.to_hex().to_string(),
            actual: envelope.this_hash.to_hex().to_string(),
        }),
        Err(_) => Err(ChainError::ComputeError),
    }
}

#[derive(Debug)]
pub enum ChainError {
    PrevHashMismatch { expected: String, actual: String },
    ThisHashMismatch { expected: String, actual: String },
    ComputeError,
}

pub mod hex_hash {
    use blake3::Hash;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(hash: &Hash, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(hash.to_hex().as_str())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Hash, D::Error> {
        let s = String::deserialize(d)?;
        Hash::from_hex(&s).map_err(serde::de::Error::custom)
    }
}
