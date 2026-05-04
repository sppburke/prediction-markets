//! Wallet trade history cache — JSON file, 7-day TTL.
//!
//! Atomic write: serialize to `<path>.tmp`, then `rename` to `<path>`.
//! Wallets fetched within `bootstrap_wallet_cache_ttl_days = 7` are skipped.
//!
//! File layout:
//! ```json
//! { "entries": { "0xabc...": { "fetched_at_unix": 1234567890, "trades": [...] } } }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use pe_trader_index::snapshot::RawTrade;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::BootstrapError;

// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const CACHE_TTL_DAYS: i64 = 7;

#[derive(Debug, Serialize, Deserialize)]
struct CacheFile {
    entries: HashMap<String, CacheEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    fetched_at_unix: i64,
    trades: Vec<RawTrade>,
}

/// Persistent wallet trade-history cache backed by a JSON file.
pub struct WalletCache {
    path: PathBuf,
    data: CacheFile,
}

impl WalletCache {
    /// Open the cache at `path`, creating an empty one if the file does not exist.
    pub fn open(path: &Path) -> Result<Self, BootstrapError> {
        let data = if path.exists() {
            let bytes = std::fs::read(path)?;
            serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Cache {
                message: format!("parse cache at {}: {e}", path.display()),
            })?
        } else {
            CacheFile {
                entries: HashMap::new(),
            }
        };
        Ok(Self {
            path: path.to_owned(),
            data,
        })
    }

    /// Returns `Some(&trades)` if the wallet has a fresh cache entry (within TTL), else `None`.
    pub fn get(&self, wallet_hex: &str) -> Option<&[RawTrade]> {
        let entry = self.data.entries.get(wallet_hex)?;
        let age_secs = OffsetDateTime::now_utc().unix_timestamp() - entry.fetched_at_unix;
        if age_secs <= CACHE_TTL_DAYS * 86_400 {
            Some(&entry.trades)
        } else {
            None
        }
    }

    /// Insert or overwrite a wallet's trade history, stamped with the current time.
    pub fn insert(&mut self, wallet_hex: String, trades: Vec<RawTrade>) {
        self.data.entries.insert(
            wallet_hex,
            CacheEntry {
                fetched_at_unix: OffsetDateTime::now_utc().unix_timestamp(),
                trades,
            },
        );
    }

    /// Atomically write the cache to disk.
    pub fn save(&self) -> Result<(), BootstrapError> {
        let tmp = self.path.with_extension("json.tmp");
        let json = serde_json::to_vec_pretty(&self.data)?;
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_cache(dir: &TempDir) -> WalletCache {
        WalletCache::open(&dir.path().join("cache.json")).unwrap()
    }

    #[test]
    fn fresh_cache_returns_none() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert!(
            cache
                .get("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .is_none()
        );
    }

    #[test]
    fn insert_and_retrieve() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let addr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        cache.insert(addr.to_owned(), vec![]);
        assert!(cache.get(addr).is_some());
        assert_eq!(cache.get(addr).unwrap().len(), 0);
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.json");
        {
            let mut cache = WalletCache::open(&path).unwrap();
            cache.insert(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                vec![],
            );
            cache.save().unwrap();
        }
        let cache2 = WalletCache::open(&path).unwrap();
        assert!(
            cache2
                .get("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .is_some()
        );
    }
}
