use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Error;

fn hex_nibble(b: u8) -> Result<u8, ()> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(()),
    }
}

/// 20-byte Ethereum-compatible wallet address.
/// Serde: lowercase hex with 0x prefix. Equality/Hash on raw bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalletAddress(pub [u8; 20]);

impl WalletAddress {
    /// Parse a 0x-prefixed 40-hex-char address (case-insensitive).
    pub fn from_hex(s: &str) -> Result<Self, Error> {
        let s = s.strip_prefix("0x").ok_or_else(|| Error::ParseError {
            message: "wallet address must start with 0x".to_string(),
        })?;
        if s.len() != 40 {
            return Err(Error::ParseError {
                message: format!("expected 40 hex chars, got {}", s.len()),
            });
        }
        let mut bytes = [0u8; 20];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0]).map_err(|()| Error::ParseError {
                message: format!("invalid hex char at position {}", i * 2),
            })?;
            let lo = hex_nibble(chunk[1]).map_err(|()| Error::ParseError {
                message: format!("invalid hex char at position {}", i * 2 + 1),
            })?;
            bytes[i] = (hi << 4) | lo;
        }
        Ok(WalletAddress(bytes))
    }
}

impl fmt::Display for WalletAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for WalletAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WalletAddress({self})")
    }
}

impl Serialize for WalletAddress {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for WalletAddress {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        WalletAddress::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// Polymarket trader identity — a wallet address used as a Polymarket account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraderId(pub WalletAddress);

impl fmt::Display for TraderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Serialize for TraderId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl<'de> Deserialize<'de> for TraderId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        WalletAddress::deserialize(d).map(TraderId)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VenueAccountId(pub String);

impl fmt::Display for VenueAccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Live-execution account identity (#508 Decision 2): a lowercase text slug chosen at
/// panel creation (e.g. `sppburke`), immutable thereafter. One canonical grammar —
/// `[a-z0-9_-]{1,32}` — validated identically here and by the `accounts.account_id`
/// database `CHECK` (`scripts/supabase_multi_account_live_schema.sql`), the
/// [`WalletAddress::from_hex`] validation precedent. Deliberately NOT
/// [`VenueAccountId`]: an account here exists before any venue account does.
/// Serde: the plain validated string.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct AccountId(String);

impl AccountId {
    /// Parse and validate a slug against the canonical grammar `[a-z0-9_-]{1,32}`.
    pub fn new(slug: &str) -> Result<Self, Error> {
        if slug.is_empty() || slug.len() > 32 {
            return Err(Error::ParseError {
                message: format!("account slug must be 1..=32 chars, got {}", slug.len()),
            });
        }
        if !slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        {
            return Err(Error::ParseError {
                message: format!("account slug {slug:?} violates [a-z0-9_-]{{1,32}}"),
            });
        }
        Ok(AccountId(slug.to_owned()))
    }

    /// The validated slug.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AccountId({})", self.0)
    }
}

impl<'de> Deserialize<'de> for AccountId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        AccountId::new(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod account_id_tests {
    #![allow(clippy::unwrap_used)]

    use super::AccountId;

    #[test]
    fn grammar_accepts_and_rejects_canonically() {
        for ok in ["sppburke", "a", "partner-2", "x_1", &"a".repeat(32)] {
            assert!(AccountId::new(ok).is_ok(), "{ok:?} must parse");
        }
        for bad in ["", "Upper", "space here", "é", "dot.", &"a".repeat(33)] {
            assert!(AccountId::new(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn serde_round_trips_and_rejects_invalid() {
        let id = AccountId::new("sppburke").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"sppburke\"");
        assert_eq!(serde_json::from_str::<AccountId>(&json).unwrap(), id);
        assert!(serde_json::from_str::<AccountId>("\"Bad Slug\"").is_err());
    }
}
