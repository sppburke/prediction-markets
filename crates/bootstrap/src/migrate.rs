//! One-shot pile population (`pe-bootstrap migrate`, issue #166).
//!
//! Populates the `wallets` table from every existing source:
//!
//! 1. `wallet_set.json` → UPSERT with `source_bits = SRC_WALLET_SET_JSON`.
//! 2. `trades.wallet_hex` distinct set → UPSERT + refresh `trade_count`.
//! 3. Dune CSVs under `data/dune_csvs/` → UPSERT with the bit derived from
//!    each CSV's filename (infra / leaderboard / radion / gap502 / generic).
//! 4. Seed `last_polymarket_fetch_at` from `MAX(trades.timestamp_unix)` so the
//!    daily backfill doesn't redundantly re-fetch already-seeded wallets.
//! 5. Seed `last_funder_fetch_at` from `funder_lookup_done.fetched_at_unix`
//!    so the weekly funder refresh doesn't redundantly re-fetch existing wallets.
//! 6. `apply_activation_rules` LAST — by this point every fetch-timestamp is
//!    seeded, so a crash here never leaves `is_active=1` wallets with NULL
//!    fetch timestamps.
//!
//! All steps are idempotent. UPSERT semantics OR `source_bits` together so a
//! wallet present in multiple sources accumulates all its bits.

use std::path::{Path, PathBuf};

use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::{ALL_EXCHANGE_CONTRACTS, TOPIC_ORDER_FILLED_V1};

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::config::BootstrapConfig;
use crate::error::BootstrapError;
use crate::pile::{
    self, SRC_DUNE_CSV, SRC_GAP502, SRC_LEADERBOARD, SRC_RADION, SRC_TRADES, SRC_WALLET_SET_JSON,
};
use crate::wallet_set;

/// Per-row payload parsed out of a Dune CSV.
type DuneCsvRow = (String, Option<i64>, Option<i64>, Option<i64>);

/// Per-stage counts reported back to the caller for logging / scenario tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct MigrateReport {
    pub wallet_set_rows: usize,
    pub trades_rows: usize,
    pub dune_csv_rows: usize,
    pub infra_rows: usize,
    pub trade_count_refreshed: usize,
    pub last_polymarket_fetch_seeded: usize,
    pub last_funder_fetch_seeded: usize,
    pub activated: usize,
}

/// Run the migrate flow against `cache`.
///
/// `dune_csv_dir` is optional — when `None`, the CSV stage is skipped (useful
/// for tests on a synthetic DB). Production runs point this at `data/dune_csvs/`.
pub fn run_migrate(
    cache: &mut WalletCache,
    wallet_set_path: &Path,
    dune_csv_dir: Option<&Path>,
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

    // 3. Dune CSVs
    let (dune_csv_rows, infra_rows) = if let Some(dir) = dune_csv_dir {
        let counts = ingest_dune_csvs(cache, dir)?;
        tracing::info!(
            rows = counts.0,
            infra = counts.1,
            dir = %dir.display(),
            "migrate: dune CSVs ingested"
        );
        counts
    } else {
        tracing::info!("migrate: dune CSV directory not provided — skipping CSV ingest");
        (0, 0)
    };

    // 4. Seed last_polymarket_fetch_at from trades.
    let last_polymarket_fetch_seeded = cache.seed_last_polymarket_fetch_from_trades()?;
    tracing::info!(
        seeded = last_polymarket_fetch_seeded,
        "migrate: last_polymarket_fetch_at seeded"
    );

    // 5. Seed last_funder_fetch_at from funder_lookup_done.
    let last_funder_fetch_seeded = cache.seed_last_funder_fetch_from_done()?;
    tracing::info!(
        seeded = last_funder_fetch_seeded,
        "migrate: last_funder_fetch_at seeded"
    );

    // 6. Activation rules LAST.
    let activated = pile::apply_activation_rules(cache)?;
    tracing::info!(activated, "migrate: activation rules applied");

    Ok(MigrateReport {
        wallet_set_rows,
        trades_rows,
        dune_csv_rows,
        infra_rows,
        trade_count_refreshed,
        last_polymarket_fetch_seeded,
        last_funder_fetch_seeded,
        activated,
    })
}

/// Convenience: load `BootstrapConfig` paths and call `run_migrate`.
pub fn run_migrate_from_config(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<MigrateReport, BootstrapError> {
    let dune_csv_dir = dune_csv_dir(config);
    run_migrate(cache, &config.wallet_set_path, dune_csv_dir.as_deref())
}

/// Default `data/dune_csvs/` relative to the cache path's parent.
fn dune_csv_dir(config: &BootstrapConfig) -> Option<PathBuf> {
    let parent = config.cache_path.parent()?;
    let candidate = parent.join("dune_csvs");
    if candidate.is_dir() {
        Some(candidate)
    } else {
        None
    }
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

/// Read enumeration progress from the SQLite `source_cursor` table. Returns
/// `(completed_contracts, enumerated_topic_hashes)` — empty `Vec`s on missing
/// keys (fresh install). The `enumerated_topic_hashes.is_empty()` case
/// combined with a full `completed_contracts` is the legacy-V1-done sentinel
/// — see `lib.rs::run()` for the V2-only re-enumeration logic that triggers.
pub fn load_enum_state(cache: &WalletCache) -> Result<(Vec<String>, Vec<String>), BootstrapError> {
    let contracts = cache
        .get_source_cursor(CURSOR_WALLET_ENUM_COMPLETED_CONTRACTS)
        .map(|s| serde_json::from_str::<Vec<String>>(&s))
        .transpose()?
        .unwrap_or_default();
    let topics = cache
        .get_source_cursor(CURSOR_WALLET_ENUM_TOPIC_HASHES)
        .map(|s| serde_json::from_str::<Vec<String>>(&s))
        .transpose()?
        .unwrap_or_default();
    Ok((contracts, topics))
}

/// Persist enumeration progress to the SQLite `source_cursor` table. Both
/// vecs are JSON-encoded and written under the [`CURSOR_WALLET_ENUM_*`] keys.
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
/// - `data/dune_csvs/*.csv` files are renamed to `*.csv.imported`
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
        save_enum_state(cache, &contracts, &topics)?;
        std::fs::remove_file(&config.wallet_set_path)?;
        tracing::info!(
            path = %config.wallet_set_path.display(),
            "auto_migrate_legacy: wallet_set.json deleted; enum-progress persisted to source_cursor"
        );
    }

    // 2. data/dune_csvs/*.csv → SQLite + rename to .csv.imported.
    if let Some(dir) = dune_csv_dir(config) {
        did_ingest = true;
        let counts = ingest_dune_csvs(cache, &dir)?;
        tracing::info!(
            non_infra = counts.0,
            infra = counts.1,
            dir = %dir.display(),
            "auto_migrate_legacy: dune CSVs ingested"
        );
        // Rename each ingested .csv to .csv.imported so the next run skips it
        // (ingest_dune_csvs's extension filter `!= Some("csv")` excludes
        // `.imported` automatically). Non-fatal on per-file rename failure so
        // a single permissions edge case doesn't abort the migration.
        let entries = std::fs::read_dir(&dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("csv") {
                continue;
            }
            let archived = path.with_extension("csv.imported");
            if let Err(e) = std::fs::rename(&path, &archived) {
                tracing::warn!(
                    file = %path.display(),
                    error = %e,
                    "auto_migrate_legacy: rename to .imported failed; file will re-ingest on next run (idempotent)"
                );
            }
        }
    }

    // 3. Post-ingest sequence — matches `run_migrate` exactly so the
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
        let last_funder_fetch_seeded = cache.seed_last_funder_fetch_from_done()?;
        let activated = pile::apply_activation_rules(cache)?;
        tracing::info!(
            trades_rows,
            trade_count_refreshed,
            last_polymarket_fetch_seeded,
            last_funder_fetch_seeded,
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

/// Returns `(non_infra_rows, infra_rows)`.
fn ingest_dune_csvs(cache: &mut WalletCache, dir: &Path) -> Result<(usize, usize), BootstrapError> {
    let mut non_infra: usize = 0;
    let mut infra: usize = 0;
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("csv") {
            continue;
        }
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let (bits, mark_infra) = classify_csv(&filename);
        let rows = parse_dune_csv(&path)?;
        if mark_infra {
            let hexes: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
            cache.mark_infra_bulk(&hexes)?;
            // Infra wallets still get a row so source_bits records they were seen.
            let upserts: Vec<WalletUpsertRow> = rows
                .into_iter()
                .map(|(hex, fs, cm, wr)| (hex, bits, true, fs, cm, wr, 0))
                .collect();
            infra += upserts.len();
            cache.upsert_wallets_bulk(&upserts)?;
        } else {
            let upserts: Vec<WalletUpsertRow> = rows
                .into_iter()
                .map(|(hex, fs, cm, wr)| (hex, bits, false, fs, cm, wr, 0))
                .collect();
            non_infra += upserts.len();
            cache.upsert_wallets_bulk(&upserts)?;
        }
        tracing::info!(file = %filename, bits = bits, infra = mark_infra, "migrate: ingested CSV");
    }
    Ok((non_infra, infra))
}

/// Map a Dune CSV filename to its `(source_bits, is_infra)` tuple.
///
/// The infra CSV is detected by `infra` in the filename. Other CSVs use the
/// most specific match: leaderboard → bit4, radion → bit5, 502-gap → bit6,
/// generic Dune → bit2.
pub fn classify_csv(filename_lower: &str) -> (i64, bool) {
    if filename_lower.contains("infra") {
        // Infra CSV: bit2 stays unset so the activation rule's
        // `dune_closed_markets >= N` arm doesn't fire spuriously. The infra
        // gate (is_infra=1) is the load-bearing flag.
        return (0, true);
    }
    if filename_lower.contains("leaderboard") {
        return (SRC_LEADERBOARD, false);
    }
    if filename_lower.contains("radion") {
        return (SRC_RADION, false);
    }
    if filename_lower.contains("502") || filename_lower.contains("gap") {
        return (SRC_GAP502, false);
    }
    (SRC_DUNE_CSV, false)
}

/// Parse a Dune CSV file into `(wallet_hex, first_seen_unix, closed_markets, win_rate_bps)` rows.
///
/// Expected columns (case-insensitive header): `wallet_hex` required; optional
/// `first_seen_unix` (or `first_seen_at_unix`, `dune_first_seen_unix`),
/// `closed_markets` (or `dune_closed_markets`), `win_rate_bps` (or
/// `dune_win_rate_bps`, or `win_rate_pct` × 100 if present).
pub fn parse_dune_csv(path: &Path) -> Result<Vec<DuneCsvRow>, BootstrapError> {
    let bytes = std::fs::read(path)?;
    let text = String::from_utf8_lossy(&bytes);
    let mut lines = text.lines();
    let header = lines.next().unwrap_or("");
    let cols: Vec<&str> = header.split(',').map(str::trim).collect();
    let idx = |aliases: &[&str]| -> Option<usize> {
        cols.iter().position(|c| {
            let lower = c.to_ascii_lowercase();
            aliases.iter().any(|a| lower == *a)
        })
    };
    let wallet_idx = idx(&["wallet_hex", "wallet", "address"]);
    let Some(wi) = wallet_idx else {
        tracing::warn!(path = %path.display(), "migrate: CSV has no wallet column, skipping file");
        return Ok(Vec::new());
    };
    let first_seen_idx = idx(&[
        "first_seen_unix",
        "first_seen_at_unix",
        "dune_first_seen_unix",
    ]);
    let closed_idx = idx(&["closed_markets", "dune_closed_markets"]);
    let win_rate_bps_idx = idx(&["win_rate_bps", "dune_win_rate_bps"]);

    let mut out = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        let raw = match fields.get(wi) {
            Some(v) => v.trim(),
            None => continue,
        };
        if raw.is_empty() {
            continue;
        }
        let hex = match WalletAddress::from_hex(raw) {
            Ok(w) => w.to_string(),
            Err(_) => {
                // Try with 0x-prefix added.
                let prefixed = if raw.starts_with("0x") {
                    raw.to_string()
                } else {
                    format!("0x{raw}")
                };
                match WalletAddress::from_hex(&prefixed) {
                    Ok(w) => w.to_string(),
                    Err(_) => {
                        tracing::warn!(value = raw, "migrate: skipping unparseable wallet");
                        continue;
                    }
                }
            }
        };
        let first_seen = first_seen_idx
            .and_then(|i| fields.get(i))
            .and_then(|s| s.parse::<i64>().ok());
        let closed = closed_idx
            .and_then(|i| fields.get(i))
            .and_then(|s| s.parse::<i64>().ok());
        let win_rate_bps = win_rate_bps_idx
            .and_then(|i| fields.get(i))
            .and_then(|s| s.parse::<i64>().ok());
        out.push((hex, first_seen, closed, win_rate_bps));
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn classify_csv_routes_filenames_to_correct_bits() {
        assert_eq!(classify_csv("dune-infra-wallets-2026-05-15.csv"), (0, true));
        assert_eq!(
            classify_csv("polymarket-leaderboard-2026-05-01.csv"),
            (SRC_LEADERBOARD, false)
        );
        assert_eq!(classify_csv("radion-traders.csv"), (SRC_RADION, false));
        assert_eq!(classify_csv("502-gap-wallets.csv"), (SRC_GAP502, false));
        assert_eq!(classify_csv("gap-discovery.csv"), (SRC_GAP502, false));
        assert_eq!(
            classify_csv("dune-universe-2026.csv"),
            (SRC_DUNE_CSV, false)
        );
    }

    #[test]
    fn parse_dune_csv_handles_mixed_case_and_optional_columns() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "wallet_hex,closed_markets,win_rate_bps,first_seen_unix").unwrap();
        writeln!(
            f,
            "0xAAAaaaaAaAAAAAAaAaaaaAAaaaAaAAaAAAAaAAAA,150,9500,1700000000"
        )
        .unwrap();
        writeln!(f, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,,,").unwrap();
        f.flush().unwrap();
        let rows = parse_dune_csv(&path).unwrap();
        assert_eq!(rows.len(), 2);
        // canonical: lowercase 0x + 40 hex
        assert_eq!(rows[0].0, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(rows[0].1, Some(1_700_000_000));
        assert_eq!(rows[0].2, Some(150));
        assert_eq!(rows[0].3, Some(9_500));
        // Empty optional columns parse to None.
        assert_eq!(
            rows[1],
            (
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                None,
                None,
                None
            )
        );
    }

    #[test]
    fn parse_dune_csv_skips_unparseable_wallets() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "wallet_hex").unwrap();
        writeln!(f, "not-a-wallet").unwrap();
        writeln!(f, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        f.flush().unwrap();
        let rows = parse_dune_csv(&path).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn run_migrate_empty_directory_produces_zero_report() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let dune_csv_dir = dir.path().join("dune_csvs");
        std::fs::create_dir(&dune_csv_dir).unwrap();
        let wallet_set_path = dir.path().join("wallet_set.json");
        // wallet_set.json does not exist — that's a valid empty-pile scenario.
        let r = run_migrate(&mut cache, &wallet_set_path, Some(&dune_csv_dir)).unwrap();
        assert_eq!(r.wallet_set_rows, 0);
        assert_eq!(r.trades_rows, 0);
        assert_eq!(r.dune_csv_rows, 0);
        assert_eq!(r.activated, 0);
    }

    #[test]
    fn run_migrate_idempotent_across_reruns() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let dune_csv_dir = dir.path().join("dune_csvs");
        std::fs::create_dir(&dune_csv_dir).unwrap();
        // One CSV: leaderboard with one wallet that should activate.
        let mut f = std::fs::File::create(dune_csv_dir.join("leaderboard.csv")).unwrap();
        writeln!(f, "wallet_hex").unwrap();
        writeln!(f, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        f.flush().unwrap();

        let wallet_set_path = dir.path().join("wallet_set.json");
        let r1 = run_migrate(&mut cache, &wallet_set_path, Some(&dune_csv_dir)).unwrap();
        assert_eq!(r1.dune_csv_rows, 1);
        assert_eq!(r1.activated, 1);

        // Re-run: nothing new to activate (sticky 0→1).
        let r2 = run_migrate(&mut cache, &wallet_set_path, Some(&dune_csv_dir)).unwrap();
        assert_eq!(r2.activated, 0);
        assert_eq!(cache.active_wallet_count().unwrap(), 1);
    }
}
