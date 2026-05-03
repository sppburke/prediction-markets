//! Polygon-chain event types emitted by this source connector.

use std::fmt;

use pe_core_types::{SourceTimestamp, WalletAddress};
use rust_decimal::Decimal;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
};

// ---------------------------------------------------------------------------
// Hex decoding helper
// ---------------------------------------------------------------------------

/// Decode a single ASCII hex nibble into its 0–15 value.
fn hex_nibble(b: u8) -> Result<u8, ()> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(()),
    }
}

// ---------------------------------------------------------------------------
// TxHash
// ---------------------------------------------------------------------------

/// A 32-byte Ethereum transaction hash.
///
/// Wire format: `"0x<64 lowercase hex chars>"`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxHash(pub [u8; 32]);

impl fmt::Debug for TxHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxHash(0x")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

impl Serialize for TxHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = String::with_capacity(66);
        s.push_str("0x");
        for b in &self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        serializer.serialize_str(&s)
    }
}

struct TxHashVisitor;

impl<'de> Visitor<'de> for TxHashVisitor {
    type Value = TxHash;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a 0x-prefixed 64-char hex string")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<TxHash, E> {
        let hex = v.strip_prefix("0x").unwrap_or(v);
        if hex.len() != 64 {
            return Err(E::custom(format!(
                "expected 64 hex chars, got {}",
                hex.len()
            )));
        }
        let mut bytes = [0u8; 32];
        let hex_bytes = hex.as_bytes();
        for (i, chunk) in hex_bytes.chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0])
                .map_err(|()| E::custom(format!("invalid hex char '{}'", chunk[0] as char)))?;
            let lo = hex_nibble(chunk[1])
                .map_err(|()| E::custom(format!("invalid hex char '{}'", chunk[1] as char)))?;
            bytes[i] = (hi << 4) | lo;
        }
        Ok(TxHash(bytes))
    }
}

impl<'de> Deserialize<'de> for TxHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(TxHashVisitor)
    }
}

// ---------------------------------------------------------------------------
// ExternalAddressKind
// ---------------------------------------------------------------------------

/// Classifies the external address that initiated a bridge or on-ramp receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAddressKind {
    CexDeposit,
    Bridge,
    Onramp,
    Unknown,
}

// ---------------------------------------------------------------------------
// PolygonEvent
// ---------------------------------------------------------------------------

/// All Polygon-chain events emitted by this source.
///
/// Every variant carries structured provenance fields so that `operator-graph`
/// can reconstruct multi-hop funding chains without re-reading raw logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PolygonEvent {
    /// pUSD minted for a wallet (collateral deposited into Polymarket).
    PUsdMint {
        to: WalletAddress,
        amount_usd: Decimal,
        block_number: u64,
        tx_hash: TxHash,
        timestamp: SourceTimestamp,
    },
    /// pUSD burned from a wallet (collateral withdrawn from Polymarket).
    PUsdBurn {
        from: WalletAddress,
        amount_usd: Decimal,
        block_number: u64,
        tx_hash: TxHash,
        timestamp: SourceTimestamp,
    },
    /// ERC-20 USDC transfer between two addresses on Polygon.
    UsdcTransfer {
        from: WalletAddress,
        to: WalletAddress,
        /// True when `to` is the Polymarket collateral contract.
        to_collateral_contract: bool,
        /// True when `from` is the Polymarket collateral contract.
        from_collateral_contract: bool,
        amount_usd: Decimal,
        block_number: u64,
        tx_hash: TxHash,
        timestamp: SourceTimestamp,
    },
    /// A new Polymarket proxy wallet was deployed on-chain.
    ProxyWalletDeployed {
        proxy: WalletAddress,
        owner: WalletAddress,
        block_number: u64,
        tx_hash: TxHash,
        timestamp: SourceTimestamp,
    },
    /// A CEX deposit address forwarded funds to a Polymarket proxy.
    DepositAddressFunding {
        from: WalletAddress,
        deposit_address: WalletAddress,
        amount_usd: Decimal,
        block_number: u64,
        tx_hash: TxHash,
        timestamp: SourceTimestamp,
    },
    /// A bridge or on-ramp delivered funds directly to a wallet.
    BridgeOnrampReceipt {
        to: WalletAddress,
        bridge: WalletAddress,
        amount_usd: Decimal,
        source_kind: ExternalAddressKind,
        block_number: u64,
        tx_hash: TxHash,
        timestamp: SourceTimestamp,
    },
}

impl PolygonEvent {
    /// Return the event timestamp, regardless of variant.
    pub fn timestamp(&self) -> &SourceTimestamp {
        match self {
            Self::PUsdMint { timestamp, .. }
            | Self::PUsdBurn { timestamp, .. }
            | Self::UsdcTransfer { timestamp, .. }
            | Self::ProxyWalletDeployed { timestamp, .. }
            | Self::DepositAddressFunding { timestamp, .. }
            | Self::BridgeOnrampReceipt { timestamp, .. } => timestamp,
        }
    }
}

// ---------------------------------------------------------------------------
// PolygonEventError
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum PolygonEventError {
    #[error("serialize failed: {0}")]
    Serialize(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn tx_hash_round_trip() {
        let raw = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let h: TxHash = serde_json::from_str(&format!("\"{raw}\"")).unwrap();
        let out = serde_json::to_string(&h).unwrap();
        assert_eq!(out, format!("\"{raw}\""));
    }

    #[test]
    fn tx_hash_debug_format() {
        let h: TxHash = serde_json::from_str(
            "\"0x0000000000000000000000000000000000000000000000000000000000000001\"",
        )
        .unwrap();
        let dbg = format!("{h:?}");
        assert!(dbg.starts_with("TxHash(0x"));
        assert!(dbg.ends_with(')'));
    }

    #[test]
    fn p_usd_mint_round_trip() {
        let json = r#"{
            "kind": "p_usd_mint",
            "to": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "amount_usd": "500.00",
            "block_number": 50000002,
            "tx_hash": "0x0000000000000000000000000000000000000000000000000000000000000003",
            "timestamp": "2024-01-15T10:02:00Z"
        }"#;
        let event: PolygonEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(event, PolygonEvent::PUsdMint { .. }));
        // round-trip
        let reser = serde_json::to_string(&event).unwrap();
        let event2: PolygonEvent = serde_json::from_str(&reser).unwrap();
        assert!(matches!(event2, PolygonEvent::PUsdMint { .. }));
    }

    #[test]
    fn external_address_kind_snake_case() {
        assert_eq!(
            serde_json::to_string(&ExternalAddressKind::CexDeposit).unwrap(),
            "\"cex_deposit\""
        );
        assert_eq!(
            serde_json::to_string(&ExternalAddressKind::Bridge).unwrap(),
            "\"bridge\""
        );
    }

    #[test]
    fn polygon_event_kind_tags() {
        let cases = [
            (
                r#"{"kind":"p_usd_mint","to":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","amount_usd":"1.0","block_number":1,"tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"2024-01-01T00:00:00Z"}"#,
                "p_usd_mint",
            ),
            (
                r#"{"kind":"proxy_wallet_deployed","proxy":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","owner":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","block_number":1,"tx_hash":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"2024-01-01T00:00:00Z"}"#,
                "proxy_wallet_deployed",
            ),
        ];
        for (json, expected_kind) in cases {
            let event: PolygonEvent = serde_json::from_str(json).unwrap();
            let reser: serde_json::Value = serde_json::to_value(&event).unwrap();
            assert_eq!(reser["kind"].as_str().unwrap(), expected_kind);
        }
    }
}
