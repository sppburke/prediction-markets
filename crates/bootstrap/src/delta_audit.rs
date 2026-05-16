//! Delta-backfill audit writer — issue #176.
//!
//! Classifies every wallet that had at least one new trade OR appeared in
//! the on-chain delta_set this run, then writes one row per classified
//! wallet to the `delta_audit` table. True-negative wallets (no new trades
//! AND not in delta_set) produce no row — the table grows ~5–10% of the
//! daily backfill size, not full ~105k/day.
//!
//! Buckets (mutually exclusive):
//!
//! - `DELTA_HIT` — wallet in `delta_set` AND had new trades. The on-chain
//!   scan correctly identified an active wallet.
//! - `DELTA_MISS` — wallet had new trades but was NOT in `delta_set`. The
//!   scan missed an active wallet — flagged as a `tracing::error!` so
//!   operators see it in the logs; persisted in the audit table for offline
//!   analysis. Operators flip to `DeltaMode::Delta` only after these stay
//!   zero across multiple runs.
//! - `DELTA_EXTRA` — wallet in `delta_set` but had no new trades. False
//!   positive of the scan — not a fidelity concern (worst case: one extra
//!   API call) but tracked for visibility.

use std::collections::{HashMap, HashSet};

use pe_core_types::WalletAddress;

use crate::cache::WalletCache;
use crate::error::BootstrapError;

pub const DELTA_HIT: &str = "DELTA_HIT";
pub const DELTA_MISS: &str = "DELTA_MISS";
pub const DELTA_EXTRA: &str = "DELTA_EXTRA";

/// Classify every relevant wallet for the run and write its row to `delta_audit`.
///
/// `new_trades` contains entries only for wallets with `count > 0` (per
/// `PolymarketBulkFetcher::fetch_all`'s contract); `delta_set` is the on-chain
/// scan result. The function iterates `new_trades ∪ delta_set` and writes
/// exactly one row per wallet in that union, classified per the buckets above.
///
/// Returns the count of rows written (= |new_trades ∪ delta_set|). Idempotent
/// on `(run_at_unix, wallet_hex)` — `INSERT OR IGNORE` makes re-running safe.
pub fn classify_and_record(
    cache: &mut WalletCache,
    run_at_unix: i64,
    delta_set: &HashSet<WalletAddress>,
    new_trades: &HashMap<WalletAddress, usize>,
) -> Result<usize, BootstrapError> {
    let mut rows: Vec<(i64, String, &'static str, i64)> =
        Vec::with_capacity(new_trades.len() + delta_set.len());

    // 1. Every wallet with new trades → DELTA_HIT (if in delta_set) or DELTA_MISS.
    for (wallet, count) in new_trades {
        let count_i64 = i64::try_from(*count).unwrap_or(i64::MAX);
        if delta_set.contains(wallet) {
            rows.push((run_at_unix, wallet.to_string(), DELTA_HIT, count_i64));
        } else {
            tracing::error!(
                wallet = %wallet,
                new_trades_fetched = count_i64,
                "delta_audit: DELTA_MISS — delta scan missed an active wallet"
            );
            rows.push((run_at_unix, wallet.to_string(), DELTA_MISS, count_i64));
        }
    }

    // 2. Every wallet in delta_set NOT in new_trades → DELTA_EXTRA (count 0).
    for wallet in delta_set {
        if !new_trades.contains_key(wallet) {
            rows.push((run_at_unix, wallet.to_string(), DELTA_EXTRA, 0));
        }
    }

    let count = rows.len();
    cache.insert_delta_audit_rows(&rows)?;
    Ok(count)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rusqlite::params;
    use tempfile::TempDir;

    fn open_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    fn wallet(b: u8) -> WalletAddress {
        let mut bytes = [0u8; 20];
        bytes[19] = b;
        WalletAddress(bytes)
    }

    fn count_rows_by_class(cache: &WalletCache, class: &str) -> i64 {
        cache
            .raw_conn_for_test()
            .query_row(
                "SELECT COUNT(*) FROM delta_audit WHERE classification = ?1",
                params![class],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0)
    }

    #[test]
    fn classify_writes_one_row_per_classified_wallet() {
        let (_dir, mut cache) = open_cache();
        let delta_set: HashSet<WalletAddress> = [wallet(0x01), wallet(0x02), wallet(0x03)]
            .into_iter()
            .collect();
        let mut new_trades: HashMap<WalletAddress, usize> = HashMap::new();
        new_trades.insert(wallet(0x01), 5); // HIT — in both
        new_trades.insert(wallet(0x04), 3); // MISS — trades but not in delta_set

        let written =
            classify_and_record(&mut cache, 1_700_000_000, &delta_set, &new_trades).unwrap();
        // Union size: {0x01, 0x02, 0x03, 0x04} = 4
        assert_eq!(written, 4);
        assert_eq!(count_rows_by_class(&cache, DELTA_HIT), 1);
        assert_eq!(count_rows_by_class(&cache, DELTA_MISS), 1);
        assert_eq!(count_rows_by_class(&cache, DELTA_EXTRA), 2);
    }

    #[test]
    fn classify_no_rows_for_true_negatives() {
        // delta_set empty + no new trades → no rows at all.
        let (_dir, mut cache) = open_cache();
        let delta_set: HashSet<WalletAddress> = HashSet::new();
        let new_trades: HashMap<WalletAddress, usize> = HashMap::new();
        let written =
            classify_and_record(&mut cache, 1_700_000_000, &delta_set, &new_trades).unwrap();
        assert_eq!(written, 0);
        assert_eq!(count_rows_by_class(&cache, DELTA_HIT), 0);
        assert_eq!(count_rows_by_class(&cache, DELTA_MISS), 0);
        assert_eq!(count_rows_by_class(&cache, DELTA_EXTRA), 0);
    }

    #[test]
    fn classify_idempotent_on_repeated_runs() {
        let (_dir, mut cache) = open_cache();
        let delta_set: HashSet<WalletAddress> = [wallet(0x01)].into_iter().collect();
        let new_trades: HashMap<WalletAddress, usize> = HashMap::new();
        // First call writes 1 row (DELTA_EXTRA).
        let _ = classify_and_record(&mut cache, 1_700_000_000, &delta_set, &new_trades).unwrap();
        // Second call with same run_at_unix → INSERT OR IGNORE no-op.
        let _ = classify_and_record(&mut cache, 1_700_000_000, &delta_set, &new_trades).unwrap();
        assert_eq!(count_rows_by_class(&cache, DELTA_EXTRA), 1);
    }

    #[test]
    fn classify_new_trades_count_persisted() {
        let (_dir, mut cache) = open_cache();
        let delta_set: HashSet<WalletAddress> = [wallet(0x01)].into_iter().collect();
        let mut new_trades: HashMap<WalletAddress, usize> = HashMap::new();
        new_trades.insert(wallet(0x01), 42);
        classify_and_record(&mut cache, 1_700_000_000, &delta_set, &new_trades).unwrap();
        let count: i64 = cache
            .raw_conn_for_test()
            .query_row(
                "SELECT new_trades_fetched FROM delta_audit WHERE wallet_hex = ?1",
                params![wallet(0x01).to_string()],
                |r| r.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(count, 42);
    }
}
