//! Offline validation for the canonical, versioned resolver-card contract.

#![forbid(unsafe_code)]

use pe_core_types::{PolymarketConditionId, ResolverCardId, SourceTimestamp};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

const SCHEMA_VERSION: u16 = 1;
pub const RESOLVER_CARD_SCHEMA_V1: &str = include_str!("../schema/resolver-card-v1.schema.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketFamily {
    Weather,
    CryptoBenchmarkWindow,
    CryptoSpotPath,
    SportsOfficial,
    MacroRelease,
    ChartRanking,
    Documents,
    EventFeed,
    PoliticsElections,
    Other,
}

impl MarketFamily {
    #[must_use]
    pub const fn supported_by_polymarket_canary(self) -> bool {
        matches!(
            self,
            Self::ChartRanking | Self::Documents | Self::EventFeed | Self::PoliticsElections
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ResolverSource {
    NamedOfficialPage(String),
    OfficialApi(String),
    ChainlinkStream { feed_id: String },
    KalshiBenchmark { source: String, window: WindowSpec },
    StationHistory { station_id: String, page: String },
    LeagueOfficial { league: String, game_id: String },
    Custom { description: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum OutputSpace {
    Binary,
    Categorical {
        n: u8,
    },
    NumericRange {
        min: Decimal,
        max: Decimal,
        ticks: u32,
    },
    Threshold {
        value: Decimal,
        direction: Direction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum TimingRule {
    PointInTime(SourceTimestamp),
    Window(WindowSpec),
    BusinessDayClose { tz: String },
    OnFirstPublicationAfter(SourceTimestamp),
    OnNthOccurrence { n: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowSpec {
    pub start: SourceTimestamp,
    pub end: SourceTimestamp,
    pub tz: String,
    pub sample_policy: SamplePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SamplePolicy {
    ArithmeticMean,
    TwapByMillisecond,
    MedianOfSamples,
    FirstObserved,
    LastObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum RoundingRule {
    None,
    HalfEven { dp: u8 },
    DownToTick { tick: Decimal },
    AsPublished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TieRule {
    NoTradePastEqual,
    YesOnEqual,
    NoOnEqual,
    SourceDefined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum FinalityRule {
    AsPublished,
    AfterStablePeriod { hours: u32 },
    AfterRevisionsCleared,
    AfterOfficialMark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevisionPolicy {
    NoRevisions,
    AcceptUntilFinal,
    AcceptOnlyOfficialCorrections,
    HumanReviewRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    GreaterEqual,
    Greater,
    LessEqual,
    Less,
    Equal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolverStatus {
    Tradable,
    Ambiguous,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverCard {
    pub schema_version: u16,
    pub card_id: ResolverCardId,
    pub condition_id: PolymarketConditionId,
    pub family: MarketFamily,
    pub resolver_source: ResolverSource,
    pub upstream_sources: Vec<ResolverSource>,
    pub output_space: OutputSpace,
    pub timing: TimingRule,
    pub rounding: RoundingRule,
    pub tie_rule: TieRule,
    pub finality: FinalityRule,
    pub revision_policy: RevisionPolicy,
    pub status: ResolverStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub valid_from: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub valid_until: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedResolverCard {
    pub card: ResolverCard,
    pub canonical_hash: blake3::Hash,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResolverCardError {
    #[error("resolver card JSON is invalid: {0}")]
    Json(String),
    #[error("unsupported resolver card schema version {0}")]
    Schema(u16),
    #[error("resolver card is ambiguous")]
    Ambiguous,
    #[error("resolver card is deferred")]
    Deferred,
    #[error("resolver card validity window is invalid")]
    InvalidWindow,
    #[error("resolver card is not valid at the supplied time")]
    Stale,
    #[error("resolver card is incomplete or unsupported by the canary")]
    Incomplete,
    #[error("resolver card hash does not match the installed authority")]
    HashMismatch,
    #[error("resolver card canonical encoding failed: {0}")]
    Canonical(String),
}

pub fn validate_install(
    bytes: &[u8],
    now: OffsetDateTime,
) -> Result<ValidatedResolverCard, ResolverCardError> {
    validate_install_expected(bytes, now, None)
}

pub fn validate_install_expected(
    bytes: &[u8],
    now: OffsetDateTime,
    expected_hash: Option<&str>,
) -> Result<ValidatedResolverCard, ResolverCardError> {
    let card: ResolverCard =
        serde_json::from_slice(bytes).map_err(|e| ResolverCardError::Json(e.to_string()))?;
    if card.schema_version != SCHEMA_VERSION {
        return Err(ResolverCardError::Schema(card.schema_version));
    }
    match card.status {
        ResolverStatus::Ambiguous => return Err(ResolverCardError::Ambiguous),
        ResolverStatus::Deferred => return Err(ResolverCardError::Deferred),
        ResolverStatus::Tradable => {}
    }
    if card.valid_from >= card.valid_until {
        return Err(ResolverCardError::InvalidWindow);
    }
    if now < card.valid_from || now >= card.valid_until {
        return Err(ResolverCardError::Stale);
    }
    if !card.family.supported_by_polymarket_canary()
        || !matches!(card.output_space, OutputSpace::Binary)
        || source_is_empty(&card.resolver_source)
    {
        return Err(ResolverCardError::Incomplete);
    }
    let canonical =
        serde_json::to_vec(&card).map_err(|e| ResolverCardError::Canonical(e.to_string()))?;
    let canonical_hash = blake3::hash(&canonical);
    if expected_hash.is_some_and(|expected| expected != canonical_hash.to_hex().as_str()) {
        return Err(ResolverCardError::HashMismatch);
    }
    Ok(ValidatedResolverCard {
        card,
        canonical_hash,
    })
}

fn source_is_empty(source: &ResolverSource) -> bool {
    match source {
        ResolverSource::NamedOfficialPage(value)
        | ResolverSource::OfficialApi(value)
        | ResolverSource::Custom { description: value } => value.trim().is_empty(),
        ResolverSource::ChainlinkStream { feed_id } => feed_id.trim().is_empty(),
        ResolverSource::KalshiBenchmark { source, .. } => source.trim().is_empty(),
        ResolverSource::StationHistory { station_id, page } => {
            station_id.trim().is_empty() || page.trim().is_empty()
        }
        ResolverSource::LeagueOfficial { league, game_id } => {
            league.trim().is_empty() || game_id.trim().is_empty()
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    #[test]
    fn published_v1_schema_is_valid_json() {
        let schema: serde_json::Value = serde_json::from_str(RESOLVER_CARD_SCHEMA_V1)
            .expect("checked-in resolver schema must be valid JSON");
        assert_eq!(schema["properties"]["schema_version"]["const"], 1);
    }
    use pe_core_types::{PolymarketConditionId, ResolverCardId};
    use time::macros::datetime;
    use uuid::Uuid;

    const RFC3339_CARD_JSON: &[u8] = br#"{
        "schema_version": 1,
        "card_id": "00000000-0000-0000-0000-000000000000",
        "condition_id": "0x01",
        "family": "event_feed",
        "resolver_source": {
            "kind": "official_api",
            "value": "https://example.invalid/feed"
        },
        "upstream_sources": [],
        "output_space": { "kind": "binary" },
        "timing": {
            "kind": "point_in_time",
            "value": "2026-07-20T00:00:00Z"
        },
        "rounding": { "kind": "as_published" },
        "tie_rule": "source_defined",
        "finality": { "kind": "as_published" },
        "revision_policy": "accept_only_official_corrections",
        "status": "tradable",
        "valid_from": "2026-07-01T00:00:00Z",
        "valid_until": "2026-08-01T00:00:00Z"
    }"#;

    fn card() -> ResolverCard {
        ResolverCard {
            schema_version: 1,
            card_id: ResolverCardId(Uuid::nil()),
            condition_id: PolymarketConditionId("0x01".to_owned()),
            family: MarketFamily::EventFeed,
            resolver_source: ResolverSource::OfficialApi("https://example.invalid/feed".to_owned()),
            upstream_sources: vec![],
            output_space: OutputSpace::Binary,
            timing: TimingRule::PointInTime(SourceTimestamp(datetime!(2026-07-20 0:00 UTC))),
            rounding: RoundingRule::AsPublished,
            tie_rule: TieRule::SourceDefined,
            finality: FinalityRule::AsPublished,
            revision_policy: RevisionPolicy::AcceptOnlyOfficialCorrections,
            status: ResolverStatus::Tradable,
            valid_from: datetime!(2026-07-01 0:00 UTC),
            valid_until: datetime!(2026-08-01 0:00 UTC),
        }
    }

    #[test]
    fn validates_canonical_card_and_expected_hash() {
        let bytes = serde_json::to_vec(&card()).unwrap();
        let validated = validate_install(&bytes, datetime!(2026-07-17 0:00 UTC)).unwrap();
        validate_install_expected(
            &bytes,
            datetime!(2026-07-17 0:00 UTC),
            Some(validated.canonical_hash.to_hex().as_str()),
        )
        .unwrap();
    }

    #[test]
    fn validates_and_emits_schema_rfc3339_validity_timestamps() {
        let validated =
            validate_install(RFC3339_CARD_JSON, datetime!(2026-07-17 0:00 UTC)).unwrap();
        let encoded = serde_json::to_value(&validated.card).unwrap();

        assert_eq!(encoded["valid_from"], "2026-07-01T00:00:00Z");
        assert_eq!(encoded["valid_until"], "2026-08-01T00:00:00Z");
    }

    #[test]
    fn rejects_malformed_and_legacy_sequence_validity_timestamps() {
        for invalid in [
            serde_json::json!("not-a-timestamp"),
            serde_json::json!([2026, 182, 0, 0, 0, 0, 0, 0, 0]),
        ] {
            let mut value: serde_json::Value = serde_json::from_slice(RFC3339_CARD_JSON).unwrap();
            value["valid_from"] = invalid;
            assert!(matches!(
                validate_install(
                    &serde_json::to_vec(&value).unwrap(),
                    datetime!(2026-07-17 0:00 UTC)
                ),
                Err(ResolverCardError::Json(_))
            ));
        }
    }

    #[test]
    fn ambiguous_stale_and_hash_mismatch_fail_closed() {
        let mut value = card();
        value.status = ResolverStatus::Ambiguous;
        assert_eq!(
            validate_install(
                &serde_json::to_vec(&value).unwrap(),
                datetime!(2026-07-17 0:00 UTC)
            ),
            Err(ResolverCardError::Ambiguous)
        );
        let bytes = serde_json::to_vec(&card()).unwrap();
        assert_eq!(
            validate_install(&bytes, datetime!(2026-09-01 0:00 UTC)),
            Err(ResolverCardError::Stale)
        );
        assert_eq!(
            validate_install_expected(&bytes, datetime!(2026-07-17 0:00 UTC), Some("wrong")),
            Err(ResolverCardError::HashMismatch)
        );
    }
}
