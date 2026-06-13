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
