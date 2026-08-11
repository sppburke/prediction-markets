//! Automated venue-settlement admission record (#508 Decision 11a).
//!
//! Ordinary live copy-trading cannot populate a full [`crate::ResolverCard`] — its mandatory
//! timing/rounding/tie/finality/revision fields are resolution *semantics* no automated source
//! can supply, so hand-installed full cards remain canary-only. This deliberately smaller
//! record is the automated half of the live venue admission artifact: venue settlement is the
//! payoff authority, and this record carries exactly what an automated Gamma/CLOB read can
//! prove — the venue's resolution status for a condition, with raw-evidence provenance and a
//! freshness window. Missing, stale, ambiguous, or unresolved-when-required evidence fails
//! closed at admission (the caller enforces via [`VenueSettlementRecord::validate_fresh`]).

use pe_core_types::PolymarketConditionId;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Schema version for [`VenueSettlementRecord`]; bump on incompatible layout changes.
pub const VENUE_SETTLEMENT_SCHEMA_VERSION: u16 = 1;

/// The venue's resolution status for a condition, as read from the automated source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum VenueResolutionStatus {
    /// The market is open/unresolved at observation time.
    Unresolved,
    /// The venue reports the market resolved with this winning outcome index.
    ResolvedWinner { outcome_index: u8 },
    /// The venue reports a resolved market but the winner cannot be mapped unambiguously.
    ResolvedAmbiguous,
}

/// The automated venue-settlement half of the live admission artifact (#508 Decision 11a).
///
/// The governing source is venue settlement by construction — there is no source field to
/// mis-set. Composed with the source/venue-owned market evidence (condition/outcome/token
/// mapping, tick/min-order, `negRisk`) by the live-admission validator; each half carries its
/// own raw-evidence hash, timestamps, and schema/parser versions for replay/audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueSettlementRecord {
    pub schema_version: u16,
    pub condition_id: PolymarketConditionId,
    pub status: VenueResolutionStatus,
    /// BLAKE3 hex of the raw venue payload this record was parsed from.
    pub raw_evidence_hash: String,
    /// Venue-reported timestamp (Unix seconds) when available; `None` when the payload
    /// carries none (observation time then bounds freshness alone).
    pub source_timestamp_unix: Option<i64>,
    /// Local observation time (Unix seconds) of the fetch that produced this record.
    pub observed_at_unix: i64,
    /// Version of the parser that produced this record.
    pub parser_version: u16,
    /// Freshness window (seconds): the record is admissible only while
    /// `now − observed_at ≤ freshness_window_secs`.
    pub freshness_window_secs: u64,
}

/// Typed failures validating a [`VenueSettlementRecord`] at admission. Every variant fails
/// closed (no order).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VenueSettlementError {
    #[error("unsupported venue-settlement schema version {0}")]
    Schema(u16),
    #[error("venue-settlement record is stale or from the future")]
    Stale,
    #[error("venue reports the market resolved; live entry is not admissible")]
    AlreadyResolved,
    #[error("venue resolution is ambiguous")]
    Ambiguous,
}

impl VenueSettlementRecord {
    /// Validate this record for a live ENTRY at `now`: schema supported, fresh within its
    /// window, and the venue still reports the market unresolved (an already-resolved or
    /// ambiguous market must never admit a new live entry). Fails closed on any violation.
    pub fn validate_fresh_for_entry(&self, now: OffsetDateTime) -> Result<(), VenueSettlementError> {
        if self.schema_version != VENUE_SETTLEMENT_SCHEMA_VERSION {
            return Err(VenueSettlementError::Schema(self.schema_version));
        }
        let now_unix = now.unix_timestamp();
        let age = now_unix - self.observed_at_unix;
        if age < 0 || u64::try_from(age).map_or(true, |a| a > self.freshness_window_secs) {
            return Err(VenueSettlementError::Stale);
        }
        match self.status {
            VenueResolutionStatus::Unresolved => Ok(()),
            VenueResolutionStatus::ResolvedWinner { .. } => {
                Err(VenueSettlementError::AlreadyResolved)
            }
            VenueResolutionStatus::ResolvedAmbiguous => Err(VenueSettlementError::Ambiguous),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn record(status: VenueResolutionStatus, observed: i64) -> VenueSettlementRecord {
        VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: PolymarketConditionId("0xc".to_owned()),
            status,
            raw_evidence_hash: blake3::hash(b"payload").to_hex().to_string(),
            source_timestamp_unix: None,
            observed_at_unix: observed,
            parser_version: 1,
            freshness_window_secs: 60,
        }
    }

    fn at(unix: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(unix).unwrap()
    }

    #[test]
    fn fresh_unresolved_admits_and_everything_else_fails_closed() {
        let fresh = record(VenueResolutionStatus::Unresolved, 1_000);
        assert_eq!(fresh.validate_fresh_for_entry(at(1_030)), Ok(()));
        // Stale (past the window) and future observations fail closed.
        assert_eq!(
            fresh.validate_fresh_for_entry(at(1_061)),
            Err(VenueSettlementError::Stale)
        );
        assert_eq!(
            fresh.validate_fresh_for_entry(at(999)),
            Err(VenueSettlementError::Stale)
        );
        // Resolved / ambiguous markets never admit a live entry.
        assert_eq!(
            record(VenueResolutionStatus::ResolvedWinner { outcome_index: 0 }, 1_000)
                .validate_fresh_for_entry(at(1_010)),
            Err(VenueSettlementError::AlreadyResolved)
        );
        assert_eq!(
            record(VenueResolutionStatus::ResolvedAmbiguous, 1_000)
                .validate_fresh_for_entry(at(1_010)),
            Err(VenueSettlementError::Ambiguous)
        );
        // An unsupported schema version fails closed.
        let mut wrong = record(VenueResolutionStatus::Unresolved, 1_000);
        wrong.schema_version = 99;
        assert_eq!(
            wrong.validate_fresh_for_entry(at(1_010)),
            Err(VenueSettlementError::Schema(99))
        );
    }

    #[test]
    fn serde_round_trips_and_rejects_unknown_fields() {
        let original = record(VenueSettlementStatus::Unresolved, 1_000);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(
            serde_json::from_str::<VenueSettlementRecord>(&json).unwrap(),
            original
        );
        // deny_unknown_fields: a forged extra field must fail to parse.
        let forged = r#"{"schema_version":1,"condition_id":"0xc","status":{"kind":"unresolved"},
            "raw_evidence_hash":"aa","source_timestamp_unix":null,"observed_at_unix":1000,
            "parser_version":1,"freshness_window_secs":60,"extra":"nope"}"#;
        assert!(serde_json::from_str::<VenueSettlementRecord>(forged).is_err());
    }

    use super::VenueResolutionStatus as VenueSettlementStatus;
}
