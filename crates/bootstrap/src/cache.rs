//! Permanent wallet trade-history cache — JSON file, no TTL.
//!
//! Atomic write: serialize to a uniquely-named `.tmp` file, then `rename` to
//! the final path. Each checkpoint is assigned a monotonically increasing
//! sequence number so concurrent in-flight writes (via `spawn_blocking`) use
//! separate tmp paths and never collide.
//!
//! On parse failure (legacy format, truncated file) the file is treated as a
//! blank cache and a `tracing::warn!` is emitted — no error propagated, no
//! user action required.  Follows the `load_state` precedent in `wallet_set.rs`.
//!
//! File layout:
//! ```json
//! { "trades": { "0xtxhash": { ... } }, "by_wallet": { "0xaddr": { ... } } }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use pe_core_types::SourceTradeId;
use pe_trader_index::snapshot::RawTrade;
use serde::{Deserialize, Serialize};

use crate::error::BootstrapError;

/// Number of consecutive known `source_trade_id`s that signals the incremental fetch is done.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const INCREMENTAL_STOP_THRESHOLD: usize = 3;

/// How many successful wallet inserts between periodic disk checkpoints.
/// Canonical default in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const CHECKPOINT_INTERVAL: usize = 50;

// ── On-disk types ─────────────────────────────────────────────────────────────

/// Per-wallet index: fast incremental-fetch cursor + ordered trade-id list.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WalletIndex {
    /// `source_trade_id` of the most recently observed trade (incremental cursor).
    pub newest_trade_id: Option<SourceTradeId>,
    /// Unix timestamp of the most recently observed trade (diagnostic only).
    pub newest_trade_at: i64,
    /// All `source_trade_id`s for this wallet, newest-first (insertion order).
    pub trade_ids: Vec<SourceTradeId>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TradeCache {
    /// All trades, keyed by `source_trade_id`; each trade stored exactly once.
    trades: HashMap<SourceTradeId, RawTrade>,
    /// Secondary index: wallet-hex → `WalletIndex`.
    by_wallet: HashMap<String, WalletIndex>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Permanent wallet trade-history cache backed by a JSON file.
pub struct WalletCache {
    pub(crate) path: PathBuf,
    data: TradeCache,
    /// Counts successful `insert_new` calls; drives the checkpoint interval.
    insert_count: usize,
}

impl WalletCache {
    /// Open the cache at `path`.
    ///
    /// On parse failure (legacy format or corruption) emits `tracing::warn!`
    /// and returns a blank cache so the run continues as a cold start.
    pub fn open(path: &Path) -> Result<Self, BootstrapError> {
        let data = if path.exists() {
            let bytes = std::fs::read(path)?;
            match serde_json::from_slice::<TradeCache>(&bytes) {
                Ok(d) => d,
                Err(e) => {
                    let bak = path.with_extension("json.bak");
                    if let Err(bak_err) = std::fs::rename(path, &bak) {
                        tracing::warn!(error = %bak_err, "trade cache: could not back up corrupt file");
                    }
                    tracing::warn!(
                        path = %path.display(),
                        bak = %bak.display(),
                        error = %e,
                        "trade cache: parse failed; corrupt file renamed to .bak, starting blank"
                    );
                    TradeCache::default()
                }
            }
        } else {
            TradeCache::default()
        };
        Ok(Self {
            path: path.to_owned(),
            data,
            insert_count: 0,
        })
    }

    /// Returns the `source_trade_id`s known for `wallet_hex`, newest-first.
    ///
    /// # Precondition
    /// Returns `&[]` if the wallet has never been fetched.
    pub fn known_trade_ids(&self, wallet_hex: &str) -> &[SourceTradeId] {
        self.data
            .by_wallet
            .get(wallet_hex)
            .map(|i| i.trade_ids.as_slice())
            .unwrap_or(&[])
    }

    /// Returns clones of all trades for `wallet_hex`.
    ///
    /// # Precondition
    /// Returns an empty `Vec` if the wallet has never been fetched.
    pub fn trades_for(&self, wallet_hex: &str) -> Vec<RawTrade> {
        let Some(index) = self.data.by_wallet.get(wallet_hex) else {
            return Vec::new();
        };
        index
            .trade_ids
            .iter()
            .filter_map(|id| self.data.trades.get(id).cloned())
            .collect()
    }

    /// Returns all trades across all wallets.
    ///
    /// Used by the backtest binary; order is unspecified.
    pub fn all_trades_unchecked(&self) -> Vec<RawTrade> {
        self.data.trades.values().cloned().collect()
    }

    /// Returns all wallet hex addresses present in the cache.
    pub fn all_wallet_addresses(&self) -> Vec<String> {
        self.data.by_wallet.keys().cloned().collect()
    }

    /// Append trades not already in the cache for `wallet_hex`.
    ///
    /// `trades` should be ordered newest-first (as returned by the Polymarket API).
    /// Trades whose `source_trade_id` is already present are silently skipped —
    /// so this method is idempotent.
    pub fn insert_new(&mut self, wallet_hex: &str, trades: Vec<RawTrade>) {
        // Read existing index before mutating (avoids simultaneous borrow).
        let prev_newest_at = self
            .data
            .by_wallet
            .get(wallet_hex)
            .map_or(0, |i| i.newest_trade_at);
        let prev_newest_id = self
            .data
            .by_wallet
            .get(wallet_hex)
            .and_then(|i| i.newest_trade_id.clone());

        let mut newest_at = prev_newest_at;
        let mut newest_id = prev_newest_id;
        let mut new_ids: Vec<SourceTradeId> = Vec::new();

        for trade in trades {
            let id = trade.source_trade_id.clone();
            if self.data.trades.contains_key(&id) {
                continue;
            }
            let ts = trade.timestamp.0.unix_timestamp();
            if ts > newest_at {
                newest_at = ts;
                newest_id = Some(id.clone());
            }
            new_ids.push(id.clone());
            self.data.trades.insert(id, trade);
        }

        let index = self
            .data
            .by_wallet
            .entry(wallet_hex.to_owned())
            .or_default();
        // Prepend so newest trades remain at the front.
        let old_ids = std::mem::take(&mut index.trade_ids);
        index.trade_ids = new_ids;
        index.trade_ids.extend(old_ids);
        index.newest_trade_at = newest_at;
        index.newest_trade_id = newest_id;

        self.insert_count += 1;
    }

    /// Returns `Some(seq)` every `CHECKPOINT_INTERVAL` inserts, `None` otherwise.
    ///
    /// `seq` is a monotonically increasing sequence number used as a unique
    /// suffix for the tmp file so concurrent in-flight writes don't collide.
    /// Must be called while holding the cache mutex so the counter increments
    /// are serialized and only one task gets `Some` per interval.
    pub(crate) fn checkpoint_seq(&self) -> Option<usize> {
        if self.insert_count.is_multiple_of(CHECKPOINT_INTERVAL) {
            Some(self.insert_count)
        } else {
            None
        }
    }

    /// Serialize the cache to compact JSON bytes.
    ///
    /// Call while holding the cache mutex to obtain a consistent snapshot.
    /// The caller is responsible for writing the bytes to disk (outside the
    /// mutex so disk I/O does not block concurrent inserts).
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>, BootstrapError> {
        Ok(serde_json::to_vec(&self.data)?)
    }

    /// Atomically write `bytes` to disk at `path` using a unique tmp name.
    ///
    /// `seq` is appended to the tmp filename so concurrent in-flight writes
    /// (from different checkpoint intervals) use separate tmp paths.
    /// On Linux, `rename` is atomic: the final path always holds a complete
    /// consistent snapshot, never a partial write.
    pub(crate) fn atomic_write(
        path: &Path,
        bytes: &[u8],
        seq: usize,
    ) -> Result<(), BootstrapError> {
        let tmp = path.with_extension(format!("json.{seq}.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Atomically write the full cache to disk (used for the final flush and tests).
    pub fn save(&self) -> Result<(), BootstrapError> {
        let bytes = self.to_bytes()?;
        Self::atomic_write(&self.path, &bytes, 0)
    }

    /// Total number of unique trades stored across all wallets.
    #[cfg(any(test, feature = "scenario"))]
    pub fn trade_count(&self) -> usize {
        self.data.trades.len()
    }

    /// Number of wallet entries present in the index.
    #[cfg(any(test, feature = "scenario"))]
    pub fn wallet_count(&self) -> usize {
        self.data.by_wallet.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, VenueMarketId,
        WalletAddress,
    };
    use rust_decimal_macros::dec;
    use tempfile::TempDir;
    use time::OffsetDateTime;

    fn addr(hex: &str) -> WalletAddress {
        WalletAddress::from_hex(hex).unwrap()
    }

    fn make_trade(id: &str, wallet: WalletAddress, ts: i64) -> RawTrade {
        RawTrade {
            wallet,
            market_id: MarketId(VenueMarketId("0xcond".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.60)).unwrap(),
            contracts: ContractQty(1),
            timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
            source_trade_id: SourceTradeId(id.to_owned()),
        }
    }

    fn tmp_cache(dir: &TempDir) -> WalletCache {
        WalletCache::open(&dir.path().join("cache.json")).unwrap()
    }

    #[test]
    fn fresh_cache_is_empty() {
        let dir = TempDir::new().unwrap();
        let cache = tmp_cache(&dir);
        assert_eq!(cache.trade_count(), 0);
        assert!(cache.known_trade_ids("0xaaaa").is_empty());
        assert!(
            cache
                .trades_for("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .is_empty()
        );
    }

    #[test]
    fn insert_new_stores_trades_and_updates_index() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();

        let trades = vec![
            make_trade("0xtx2", wallet, 1_704_067_200),
            make_trade("0xtx1", wallet, 1_704_067_100),
        ];
        cache.insert_new(&hex, trades);

        assert_eq!(cache.trade_count(), 2);
        let ids = cache.known_trade_ids(&hex);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], SourceTradeId("0xtx2".to_owned()));
        assert_eq!(cache.trades_for(&hex).len(), 2);
    }

    #[test]
    fn insert_new_is_idempotent_on_duplicate_ids() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();

        let trades = vec![make_trade("0xtx1", wallet, 1_704_067_100)];
        cache.insert_new(&hex, trades.clone());
        cache.insert_new(&hex, trades);

        assert_eq!(cache.trade_count(), 1, "duplicate must not be stored twice");
    }

    #[test]
    fn insert_new_updates_newest_pointer() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();

        cache.insert_new(&hex, vec![make_trade("0xtx1", wallet, 1_000_000)]);
        cache.insert_new(&hex, vec![make_trade("0xtx2", wallet, 2_000_000)]);

        let index = cache.data.by_wallet.get(&hex).unwrap();
        assert_eq!(index.newest_trade_at, 2_000_000);
        assert_eq!(
            index.newest_trade_id,
            Some(SourceTradeId("0xtx2".to_owned()))
        );
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.json");
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();
        {
            let mut cache = WalletCache::open(&path).unwrap();
            cache.insert_new(&hex, vec![make_trade("0xtx1", wallet, 1_704_067_100)]);
            cache.save().unwrap();
        }
        let cache2 = WalletCache::open(&path).unwrap();
        assert_eq!(cache2.trade_count(), 1);
        assert_eq!(cache2.trades_for(&hex).len(), 1);
    }

    #[test]
    fn legacy_file_results_in_blank_cache_and_bak() {
        // Old CacheEntry format; cannot be parsed as TradeCache — must silently start blank.
        // The corrupt file is renamed to .bak so it can be inspected.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.json");
        let legacy = r#"{"entries":{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":{"fetched_at_unix":1704067200,"trades":[]}}}"#;
        std::fs::write(&path, legacy).unwrap();

        let cache = WalletCache::open(&path).unwrap();
        assert_eq!(cache.trade_count(), 0, "legacy file must yield blank cache");
        assert!(
            path.with_extension("json.bak").exists(),
            "corrupt file must be renamed to .bak"
        );
        assert!(
            !path.exists(),
            "original must be moved to .bak, not left in place"
        );
    }

    #[test]
    fn save_is_atomic_no_tmp_lingers() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.json");
        let mut cache = WalletCache::open(&path).unwrap();
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        cache.insert_new(
            &wallet.to_string(),
            vec![make_trade("0xtx1", wallet, 1_704_067_100)],
        );
        cache.save().unwrap();
        assert!(path.exists());
        // save() uses seq=0 → tmp is "cache.json.0.tmp"
        assert!(!path.with_extension("json.0.tmp").exists());
    }

    #[test]
    fn checkpoint_seq_fires_at_interval() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let wallet = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let hex = wallet.to_string();

        // First CHECKPOINT_INTERVAL - 1 inserts: no checkpoint.
        for i in 0..CHECKPOINT_INTERVAL - 1 {
            cache.insert_new(
                &hex,
                vec![make_trade(&format!("0xtx{i}"), wallet, i as i64)],
            );
            assert!(
                cache.checkpoint_seq().is_none(),
                "no checkpoint before interval"
            );
        }
        // The CHECKPOINT_INTERVAL-th insert triggers.
        cache.insert_new(
            &hex,
            vec![make_trade(
                &format!("0xtx{}", CHECKPOINT_INTERVAL - 1),
                wallet,
                (CHECKPOINT_INTERVAL - 1) as i64,
            )],
        );
        assert_eq!(cache.checkpoint_seq(), Some(CHECKPOINT_INTERVAL));
    }

    #[test]
    fn all_trades_unchecked_aggregates_across_wallets() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache.insert_new(&w1.to_string(), vec![make_trade("0xtx1", w1, 1_000_000)]);
        cache.insert_new(&w2.to_string(), vec![make_trade("0xtx2", w2, 2_000_000)]);
        assert_eq!(cache.all_trades_unchecked().len(), 2);
    }

    #[test]
    fn all_wallet_addresses_enumerates_all() {
        let dir = TempDir::new().unwrap();
        let mut cache = tmp_cache(&dir);
        let w1 = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let w2 = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        cache.insert_new(&w1.to_string(), vec![make_trade("0xtx1", w1, 1_000_000)]);
        cache.insert_new(&w2.to_string(), vec![make_trade("0xtx2", w2, 2_000_000)]);
        let addrs = cache.all_wallet_addresses();
        assert_eq!(addrs.len(), 2);
        assert!(addrs.contains(&w1.to_string()));
        assert!(addrs.contains(&w2.to_string()));
    }
}
