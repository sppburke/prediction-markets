//! One-shot pile population (`pe-bootstrap migrate`, issue #166).
//!
//! Populates the `wallets` table from every existing source:
//!
//! 1. `wallet_set.json` → UPSERT with `source_bits = SRC_WALLET_SET_JSON`.
//! 2. `trades.wallet_hex` distinct set → UPSERT + refresh `trade_count`.
//! 3. Seed `last_polymarket_fetch_at` from `MAX(trades.timestamp_unix)` so the
//!    daily backfill doesn't redundantly re-fetch already-seeded wallets.
//! 4. `apply_activation_rules` LAST — by this point every fetch-timestamp is
//!    seeded, so a crash here never leaves `is_active=1` wallets with NULL
//!    fetch timestamps.
//!
//! All steps are idempotent. UPSERT semantics OR `source_bits` together so a
//! wallet present in multiple sources accumulates all its bits.

use std::path::Path;

use crate::chain::{ALL_EXCHANGE_CONTRACTS, TOPIC_ORDER_FILLED_V1};
use pe_core_types::WalletAddress;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::pile::{self, SRC_TRADES, SRC_WALLET_SET_JSON};
use crate::wallet_set;

/// Per-stage counts reported back to the caller for logging / scenario tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct MigrateReport {
    pub wallet_set_rows: usize,
    pub trades_rows: usize,
    pub trade_count_refreshed: usize,
    pub last_polymarket_fetch_seeded: usize,
    pub activated: usize,
}

/// Run the migrate flow against `cache`.
pub fn run_migrate(
    cache: &mut WalletCache,
    wallet_set_path: &Path,
) -> Result<MigrateReport, BootstrapError> {
    // 1. wallet_set.json → SRC_WALLET_SET_JSON
    let wallet_set_rows = ingest_wallet_set(cache, wallet_set_path)?;
    tracing::info!(
        rows = wallet_set_rows,
        path = %wallet_set_path.display(),
        "migrate: wallet_set.json ingested"
    );

    // 2. trades.wallet_hex → SRC_TRADES + refresh trade_count
    let trades_rows = ingest_trades_wallets(cache)?;
    let trade_count_refreshed = cache.refresh_trade_counts()?;
    tracing::info!(
        rows = trades_rows,
        refreshed = trade_count_refreshed,
        "migrate: trades-table wallets ingested + trade_count refreshed"
    );

    // 3. Seed last_polymarket_fetch_at from trades.
    let last_polymarket_fetch_seeded = cache.seed_last_polymarket_fetch_from_trades()?;
    tracing::info!(
        seeded = last_polymarket_fetch_seeded,
        "migrate: last_polymarket_fetch_at seeded"
    );

    // 4. Activation rules LAST.
    let activated = pile::apply_activation_rules(cache)?;
    tracing::info!(activated, "migrate: activation rules applied");

    Ok(MigrateReport {
        wallet_set_rows,
        trades_rows,
        trade_count_refreshed,
        last_polymarket_fetch_seeded,
        activated,
    })
}

/// Convenience: load `BootstrapConfig` paths and call `run_migrate`.
pub fn run_migrate_from_config(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<MigrateReport, BootstrapError> {
    run_migrate(cache, &config.wallet_set_path)
}

/// `source_cursor` key for the enumeration-progress migration (issue #181).
/// JSON-encoded `Vec<String>` of lowercase-hex contract addresses.
pub const CURSOR_WALLET_ENUM_COMPLETED_CONTRACTS: &str = "wallet_enum_completed_contracts";

/// `source_cursor` key for the enumeration-progress migration (issue #181).
/// JSON-encoded `Vec<String>` of B256-Display topic hashes (each prefixed `0x`).
pub const CURSOR_WALLET_ENUM_TOPIC_HASHES: &str = "wallet_enum_topic_hashes";

/// `source_cursor` key for the chunk-level enumeration-progress cursor
/// (issue #188 Item 2). JSON-encoded `HashMap<String, u64>` keyed by
/// `"{topic_hex}|{contract_hex}"` mapping to the last block successfully
/// scanned for that `(topic, contract)` pair. Mid-topic crash recovery
/// resumes from `last_completed_chunk_to + 1` instead of `wallet_from_block`,
/// saving up to ~168 redundant `eth_getLogs` calls on a 21M-block sweep.
///
/// Missing key (fresh install or pre-#188 deployed cache) is treated as
/// "no chunks done"; every `(topic, contract)` resumes from
/// `wallet_from_block`. Backward-compat additive — older binaries that don't
/// read this key continue to work.
pub const CURSOR_WALLET_ENUM_CHUNK_PROGRESS: &str = "wallet_enum_chunk_progress";

/// Persist enumeration progress to the SQLite `source_cursor` table. Both
/// vecs are JSON-encoded and written under the [`CURSOR_WALLET_ENUM_*`] keys.
///
/// The on-chain re-enumeration reader was retired with the `enumerate`
/// subcommand in #335, so these cursors are no longer read back; the write is
/// retained as a no-cost migration-audit marker on the `wallet_set.json`
/// one-shot in [`auto_migrate_legacy`].
pub fn save_enum_state(
    cache: &mut WalletCache,
    completed_contracts: &[String],
    enumerated_topic_hashes: &[String],
) -> Result<(), BootstrapError> {
    let contracts_json = serde_json::to_string(completed_contracts)?;
    let topics_json = serde_json::to_string(enumerated_topic_hashes)?;
    cache.set_source_cursor(CURSOR_WALLET_ENUM_COMPLETED_CONTRACTS, &contracts_json)?;
    cache.set_source_cursor(CURSOR_WALLET_ENUM_TOPIC_HASHES, &topics_json)?;
    Ok(())
}

/// Build the chunk-progress key for a given `(topic_hex, contract_hex)` pair.
/// Format: `"<topic_hex>|<contract_hex>"` where both hexes are produced via
/// the same `format!("{topic}")` / `format!("0x{contract:x}")` calls that
/// `lib.rs::run()` uses to populate `enumerated_topic_hashes` /
/// `completed_contracts`. Sharing this helper avoids subtle key-shape drift.
#[must_use]
pub fn chunk_progress_key(topic_hex: &str, contract_hex: &str) -> String {
    format!("{topic_hex}|{contract_hex}")
}

/// Read the chunk-level progress cursor from `source_cursor`. Returns an empty
/// map on missing key (fresh install, pre-#188 deployed cache). Each entry is
/// `(topic_hex, contract_hex) → last_completed_chunk_to` per
/// [`CURSOR_WALLET_ENUM_CHUNK_PROGRESS`]. Keys use [`chunk_progress_key`].
pub fn load_chunk_progress(
    cache: &WalletCache,
) -> Result<std::collections::HashMap<String, u64>, BootstrapError> {
    let map = cache
        .get_source_cursor(CURSOR_WALLET_ENUM_CHUNK_PROGRESS)
        .map(|s| serde_json::from_str::<std::collections::HashMap<String, u64>>(&s))
        .transpose()?
        .unwrap_or_default();
    Ok(map)
}

/// Persist the chunk-level progress cursor. Written after every successful
/// chunk upsert in `lib.rs::run()` so mid-topic crash recovery skips already-
/// completed chunks (issue #188 Item 2).
pub fn save_chunk_progress(
    cache: &mut WalletCache,
    progress: &std::collections::HashMap<String, u64>,
) -> Result<(), BootstrapError> {
    let json = serde_json::to_string(progress)?;
    cache.set_source_cursor(CURSOR_WALLET_ENUM_CHUNK_PROGRESS, &json)?;
    Ok(())
}

/// One-shot consolidation of legacy on-disk artifacts into the SQLite cache.
/// Issue #181 — called at the top of `lib.rs::run()` on every invocation.
/// Idempotent: detects legacy files on disk, ingests them, persists
/// enumeration-progress markers to `source_cursor`, and removes/archives the
/// originals so subsequent runs see no legacy state.
///
/// After successful return from a first deploy that had legacy artifacts:
/// - `data/wallet_set.json` is deleted
/// - `source_cursor` carries [`CURSOR_WALLET_ENUM_COMPLETED_CONTRACTS`] and
///   [`CURSOR_WALLET_ENUM_TOPIC_HASHES`] keys with JSON-encoded values
/// - the `wallets` table contains every wallet hex from the legacy sources
///   with the correct `source_bits` accumulated
///
/// On subsequent runs (no legacy files present) the function is a near-no-op:
/// two `Path::exists()` syscalls and the post-ingest no-op-on-empty
/// SQLite passes (`ingest_trades_wallets`, `refresh_trade_counts`, etc.).
///
/// # Precondition
/// `config.wallet_set_path` is set; `cache` is already opened. The function
/// is sync because every underlying operation (SQL, fs) is sync.
pub fn auto_migrate_legacy(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<(), BootstrapError> {
    let mut did_ingest = false;

    // 1. wallet_set.json → SQLite + source_cursor + delete.
    if config.wallet_set_path.exists() {
        did_ingest = true;
        // Detect file shape BEFORE ingest so we know what enum-state to persist.
        let (contracts, topics) = match wallet_set::load_state(&config.wallet_set_path)? {
            Some(state) => {
                // Checkpoint format (pre-#179 with empty `enumerated_topic_hashes`
                // via `#[serde(default)]`, OR post-#179 with both topics populated).
                (state.completed_contracts, state.enumerated_topic_hashes)
            }
            None => {
                // Bare-array legacy format (PR #68). V1-only by construction;
                // synthesize the same V1-done state the prior `lib.rs:140-148`
                // upgrade path encoded so downstream `legacy_v1_done` detection
                // (now via the source_cursor read) stays identical.
                let contracts = ALL_EXCHANGE_CONTRACTS
                    .iter()
                    .map(|c| format!("0x{c:x}"))
                    .collect();
                let topics = vec![format!("{TOPIC_ORDER_FILLED_V1}")];
                (contracts, topics)
            }
        };
        let rows = ingest_wallet_set(cache, &config.wallet_set_path)?;
        tracing::info!(
            rows,
            path = %config.wallet_set_path.display(),
            "auto_migrate_legacy: wallet_set.json ingested"
        );
        // The on-chain re-enumeration reader was retired with the `enumerate`
        // subcommand (#335); this write is retained as a no-cost migration-audit
        // marker (the cursors are no longer read back). See `save_enum_state`.
        save_enum_state(cache, &contracts, &topics)?;
        std::fs::remove_file(&config.wallet_set_path)?;
        tracing::info!(
            path = %config.wallet_set_path.display(),
            "auto_migrate_legacy: wallet_set.json deleted; enum-progress persisted to source_cursor"
        );
    }

    // 2. Post-ingest sequence — matches `run_migrate` exactly so the
    //    consolidated path is functionally equivalent to invoking
    //    `pe-bootstrap migrate` once. GATED on `did_ingest` because each
    //    helper issues a full-table UPDATE against the ~2.7M-row `wallets`
    //    table; running them on every steady-state `run()` (after the
    //    one-shot migration has fired) would add multi-second per-run cost
    //    for zero state change. The pre-existing `backfill::run_backfill`
    //    already calls `apply_activation_rules` on its own cadence so the
    //    sticky 0→1 activation gate keeps firing for newly-active wallets.
    if did_ingest {
        let trades_rows = ingest_trades_wallets(cache)?;
        let trade_count_refreshed = cache.refresh_trade_counts()?;
        let last_polymarket_fetch_seeded = cache.seed_last_polymarket_fetch_from_trades()?;
        let activated = pile::apply_activation_rules(cache)?;
        tracing::info!(
            trades_rows,
            trade_count_refreshed,
            last_polymarket_fetch_seeded,
            activated,
            "auto_migrate_legacy: post-ingest sequence complete"
        );
    }

    Ok(())
}

fn ingest_wallet_set(cache: &mut WalletCache, path: &Path) -> Result<usize, BootstrapError> {
    let Some(state) = wallet_set::load_state(path)? else {
        // Fall back to legacy bare-array format.
        let Some(wallets) = wallet_set::load(path)? else {
            return Ok(0);
        };
        let rows: Vec<WalletUpsertRow> = wallets
            .into_iter()
            .map(|w| {
                (
                    w.to_string(),
                    SRC_WALLET_SET_JSON,
                    false,
                    None,
                    None,
                    None,
                    0,
                )
            })
            .collect();
        cache.upsert_wallets_bulk(&rows)?;
        return Ok(rows.len());
    };
    let mut rows: Vec<WalletUpsertRow> = Vec::new();
    for hex in &state.wallets {
        if let Ok(w) = WalletAddress::from_hex(hex) {
            rows.push((
                w.to_string(),
                SRC_WALLET_SET_JSON,
                false,
                None,
                None,
                None,
                0,
            ));
        }
    }
    cache.upsert_wallets_bulk(&rows)?;
    Ok(rows.len())
}

fn ingest_trades_wallets(cache: &mut WalletCache) -> Result<usize, BootstrapError> {
    let hexes = cache.all_wallet_addresses();
    let rows: Vec<WalletUpsertRow> = hexes
        .into_iter()
        .filter_map(|h| {
            // Trade rows already use canonical form via WalletAddress::Display,
            // but normalise defensively.
            let normalised = WalletAddress::from_hex(&h).ok()?.to_string();
            Some((normalised, SRC_TRADES, false, None, None, None, 0))
        })
        .collect();
    let len = rows.len();
    cache.upsert_wallets_bulk(&rows)?;
    Ok(len)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn run_migrate_empty_inputs_produce_zero_report() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let wallet_set_path = dir.path().join("wallet_set.json");
        // wallet_set.json does not exist — a valid empty-pile scenario.
        let r = run_migrate(&mut cache, &wallet_set_path).unwrap();
        assert_eq!(r.wallet_set_rows, 0);
        assert_eq!(r.trades_rows, 0);
        assert_eq!(r.activated, 0);
    }

    #[test]
    fn run_migrate_idempotent_across_reruns() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        // wallet_set.json with one wallet (bare-array legacy format).
        let wallet_set_path = dir.path().join("wallet_set.json");
        std::fs::write(
            &wallet_set_path,
            r#"["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]"#,
        )
        .unwrap();

        let r1 = run_migrate(&mut cache, &wallet_set_path).unwrap();
        assert_eq!(r1.wallet_set_rows, 1);

        // Re-run is idempotent: the wallet is already present.
        let r2 = run_migrate(&mut cache, &wallet_set_path).unwrap();
        assert_eq!(r2.wallet_set_rows, 1);
        // No trades ⇒ never crosses the activation count gate.
        assert_eq!(cache.active_wallet_count().unwrap(), 0);
    }
}
