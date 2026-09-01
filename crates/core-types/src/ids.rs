use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

/// Stable venue identifier. Only "polymarket" and "kalshi" are valid values.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct VenueId(&'static str);

impl VenueId {
    pub const fn polymarket() -> Self {
        Self("polymarket")
    }

    pub const fn kalshi() -> Self {
        Self("kalshi")
    }
}

impl fmt::Debug for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("VenueId").field(&self.0).finish()
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Serialize for VenueId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.0)
    }
}

impl<'de> Deserialize<'de> for VenueId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "polymarket" => Ok(Self::polymarket()),
            "kalshi" => Ok(Self::kalshi()),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &["polymarket", "kalshi"],
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VenueMarketId(pub String);

impl fmt::Display for VenueMarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Opaque venue-assigned order identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VenueOrderId(pub String);

impl fmt::Display for VenueOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for VenueMarketId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MarketId(pub VenueMarketId);

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for MarketId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(VenueMarketId(s.to_string())))
    }
}

/// Zero-based outcome index within a market.
///
/// Widened to `u16` (issue #159) after Polymarket multi-outcome markets were
/// observed emitting `outcomeIndex` > 255 (e.g. `999`). u16's 65,535 ceiling
/// subsumes any realistic outcome cardinality; SQLite stores as `INTEGER`
/// either way so the wire/storage cost is unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutcomeId(pub u16);

impl fmt::Display for OutcomeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for OutcomeId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u16>()
            .map(OutcomeId)
            .map_err(|e| Error::ParseError {
                message: e.to_string(),
            })
    }
}

impl From<OutcomeId> for i64 {
    fn from(id: OutcomeId) -> i64 {
        i64::from(id.0)
    }
}

impl TryFrom<i64> for OutcomeId {
    type Error = Error;

    fn try_from(v: i64) -> Result<Self, Self::Error> {
        u16::try_from(v)
            .map(OutcomeId)
            .map_err(|e| Error::ParseError {
                message: e.to_string(),
            })
    }
}

/// Compound key identifying a (market, outcome) pair. Fields private; use accessors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketOutcomeId {
    market: MarketId,
    outcome: OutcomeId,
}

impl MarketOutcomeId {
    pub fn new(market: MarketId, outcome: OutcomeId) -> Self {
        Self { market, outcome }
    }

    pub fn market(&self) -> &MarketId {
        &self.market
    }

    pub fn outcome(&self) -> OutcomeId {
        self.outcome
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResolverCardId(pub uuid::Uuid);

impl fmt::Display for ResolverCardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ResolverCardId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::parse_str(s)
            .map(ResolverCardId)
            .map_err(|e| Error::ParseError {
                message: e.to_string(),
            })
    }
}

/// Opaque identifier for a data source (e.g. "polymarket-clob-ws", "kalshi-rest-v2").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceId(pub String);

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for SourceId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StrategyId(pub String);

impl fmt::Display for StrategyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for StrategyId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelId(pub String);

impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ModelId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderLocalId(pub uuid::Uuid);

impl fmt::Display for OrderLocalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for OrderLocalId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::parse_str(s)
            .map(OrderLocalId)
            .map_err(|e| Error::ParseError {
                message: e.to_string(),
            })
    }
}

/// Venue-assigned identifier for a trade record observed from an external source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceTradeId(pub String);

/// Identity generation carried by a [`SourceTradeId`] (#544).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceTradeIdentityVersion {
    /// Version-one inputs retain their transaction-hash identity.
    TransactionHashV1,
    /// Version-two reconciled activity groups use `g2:` plus lowercase BLAKE3.
    ReconciledGroupV2,
}

impl SourceTradeId {
    /// Discriminate immutable v1 identity from a reconciled v2 group key.
    ///
    /// Only the fixed-width lowercase `g2:` encoding is version two. Every
    /// other historical shape remains version one and therefore cannot be
    /// mistaken for a reconciled activity group during replay (#544).
    #[must_use]
    pub fn identity_version(&self) -> SourceTradeIdentityVersion {
        let bytes = self.0.as_bytes();
        let is_v2 = bytes.len() == 67
            && bytes.starts_with(b"g2:")
            && bytes[3..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte));
        if is_v2 {
            SourceTradeIdentityVersion::ReconciledGroupV2
        } else {
            SourceTradeIdentityVersion::TransactionHashV1
        }
    }

    #[must_use]
    pub fn is_reconciled_v2(&self) -> bool {
        self.identity_version() == SourceTradeIdentityVersion::ReconciledGroupV2
    }

    /// Whether this is a canonical 32-byte `0x` transaction-hash identity.
    ///
    /// Historical fixtures may carry shorter opaque v1 keys, so
    /// [`Self::identity_version`] preserves their generation. New generation-
    /// specific readers use this stricter helper at their version-one boundary.
    #[must_use]
    pub fn is_canonical_transaction_hash_v1(&self) -> bool {
        let bytes = self.0.as_bytes();
        bytes.len() == 66
            && bytes.starts_with(b"0x")
            && bytes[2..].iter().all(u8::is_ascii_hexdigit)
    }
}

impl fmt::Display for SourceTradeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for SourceTradeId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

/// Monotonically increasing position within the append-only event log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSeq(pub u64);

impl fmt::Display for EventSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn outcome_id_round_trips_large_index_through_json() {
        // Issue #159: Polymarket multi-outcome markets observed with outcomeIndex > 255.
        let v = OutcomeId(999);
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, "999");
        let back: OutcomeId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, OutcomeId(999));
    }

    #[test]
    fn outcome_id_round_trips_u16_max_through_json() {
        let v = OutcomeId(u16::MAX);
        let json = serde_json::to_string(&v).unwrap();
        let back: OutcomeId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, OutcomeId(u16::MAX));
    }

    #[test]
    fn outcome_id_try_from_i64_in_range() {
        assert_eq!(OutcomeId::try_from(0_i64).unwrap(), OutcomeId(0));
        assert_eq!(OutcomeId::try_from(1_i64).unwrap(), OutcomeId(1));
        assert_eq!(OutcomeId::try_from(255_i64).unwrap(), OutcomeId(255));
        assert_eq!(OutcomeId::try_from(999_i64).unwrap(), OutcomeId(999));
        assert_eq!(
            OutcomeId::try_from(i64::from(u16::MAX)).unwrap(),
            OutcomeId(u16::MAX)
        );
    }

    #[test]
    fn outcome_id_try_from_i64_out_of_range() {
        assert!(OutcomeId::try_from(-1_i64).is_err());
        assert!(OutcomeId::try_from(i64::from(u16::MAX) + 1).is_err());
        assert!(OutcomeId::try_from(i64::MAX).is_err());
        assert!(OutcomeId::try_from(i64::MIN).is_err());
    }

    #[test]
    fn outcome_id_to_i64_round_trip() {
        for raw in [0u16, 1, 999, u16::MAX] {
            let id = OutcomeId(raw);
            let as_i64: i64 = id.into();
            assert_eq!(as_i64, i64::from(raw));
            assert_eq!(OutcomeId::try_from(as_i64).unwrap(), id);
        }
    }

    #[test]
    fn outcome_id_from_str_accepts_widened_range() {
        assert_eq!(OutcomeId::from_str("0").unwrap(), OutcomeId(0));
        assert_eq!(OutcomeId::from_str("999").unwrap(), OutcomeId(999));
        assert_eq!(OutcomeId::from_str("65535").unwrap(), OutcomeId(u16::MAX));
        assert!(OutcomeId::from_str("65536").is_err());
        assert!(OutcomeId::from_str("-1").is_err());
        assert!(OutcomeId::from_str("not-a-number").is_err());
    }

    #[test]
    fn source_trade_identity_versions_never_mix() {
        let v2 = SourceTradeId(format!("g2:{}", "a".repeat(64)));
        assert_eq!(
            v2.identity_version(),
            SourceTradeIdentityVersion::ReconciledGroupV2
        );
        assert!(v2.is_reconciled_v2());
        assert!(!v2.is_canonical_transaction_hash_v1());

        let v1 = SourceTradeId(format!("0x{}", "b".repeat(64)));
        assert_eq!(
            v1.identity_version(),
            SourceTradeIdentityVersion::TransactionHashV1
        );
        assert!(v1.is_canonical_transaction_hash_v1());

        for malformed in [
            format!("g2:{}", "A".repeat(64)),
            format!("g2:{}", "a".repeat(63)),
            format!("g2:{}z", "a".repeat(63)),
        ] {
            assert_eq!(
                SourceTradeId(malformed).identity_version(),
                SourceTradeIdentityVersion::TransactionHashV1
            );
        }
    }
}
