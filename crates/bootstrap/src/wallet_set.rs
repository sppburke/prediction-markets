//! Wallet set cache — persists the enumerated wallet address list to disk.
//!
//! Atomic write: serialize to `<path>.tmp`, then `rename` to `<path>`.
//! No TTL; delete the file to force re-enumeration.
//!
//! File layout:
//! ```json
//! ["0xabc...", "0xdef...", ...]
//! ```

use std::path::Path;

use pe_core_types::WalletAddress;

use crate::error::BootstrapError;

/// Load the wallet set from `path`. Returns `None` if the file does not exist,
/// triggering a fresh enumeration by the caller.
pub fn load(path: &Path) -> Result<Option<Vec<WalletAddress>>, BootstrapError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    let hexes: Vec<String> = serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Cache {
        message: format!("parse wallet set at {}: {e}", path.display()),
    })?;
    let wallets = hexes
        .iter()
        .filter_map(|h| {
            WalletAddress::from_hex(h)
                .map_err(|e| {
                    tracing::warn!(address = %h, error = %e, "wallet set: skipping unparseable address");
                })
                .ok()
        })
        .collect();
    Ok(Some(wallets))
}

/// Atomically write the wallet set to `path`.
pub fn save(path: &Path, wallets: &[WalletAddress]) -> Result<(), BootstrapError> {
    let hexes: Vec<String> = wallets.iter().map(|w| w.to_string()).collect();
    let json = serde_json::to_vec_pretty(&hexes)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn addr(hex: &str) -> WalletAddress {
        WalletAddress::from_hex(hex).unwrap()
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load(&dir.path().join("wallets.json")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn round_trip_preserves_all_addresses() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        let wallets = vec![
            addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ];
        save(&path, &wallets).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains(&addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        assert!(loaded.contains(&addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")));
    }

    #[test]
    fn empty_set_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        save(&path, &[]).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert!(loaded.is_empty());
    }
}
