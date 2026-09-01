//! Resumable, generation-sealed wallet-cache migration and fixed-path cutover (#544).
//!
//! Version one remains available only as `*_v1_sealed` audit tables. Version-two
//! consumers use the typed `activity_groups_v2` and CLOB payout APIs; no Rust API
//! unions the generations. This makes a cross-generation ranking/state read a
//! schema/API error instead of a filter callers can forget.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION};
use pe_source_polymarket_public::{
    CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION, ClobCoverageManifest,
    ReconciliationFetcher, fetch_complete_activity,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::cache::{CACHE_SCHEMA_VERSION_V1, CACHE_SCHEMA_VERSION_V2, REQUIRED_TRADES_INDEXES};
use crate::error::BootstrapError;
use crate::lock::ForgeActivationLocks;
use crate::reclamation_evidence::{
    ReclamationEvidenceReport, capture as capture_reclamation_evidence, eval_results_dir_for_cache,
};

const CACHE_BUILD_MANIFEST_VERSION: u32 = 1;
const FROZEN_PAYLOAD_REFERENCE_VERSION: u32 = 1;
const FINAL_STAGE_RECORD_VERSION: u32 = 1;

const V2_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sealed_generation_manifests (
    generation                 INTEGER PRIMARY KEY NOT NULL CHECK(generation = 1),
    input_manifest_sha256      TEXT    NOT NULL,
    source_bounds_json         TEXT    NOT NULL,
    cursors_json               TEXT    NOT NULL,
    hashes_json                TEXT    NOT NULL,
    legacy_trade_count         INTEGER NOT NULL,
    legacy_resolution_count    INTEGER NOT NULL,
    legacy_cursor_count        INTEGER NOT NULL,
    sealed_at_unix             INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_coverage_manifests_v2 (
    generation          INTEGER PRIMARY KEY NOT NULL,
    source_bounds_json  TEXT    NOT NULL,
    cursors_json        TEXT    NOT NULL,
    page_hashes_json    TEXT    NOT NULL,
    group_count         INTEGER NOT NULL,
    schema_version      INTEGER NOT NULL CHECK(schema_version = 2),
    parser_version      INTEGER NOT NULL CHECK(parser_version = 2),
    completed_at_unix   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS activity_groups_v2 (
    source_trade_id                  TEXT    PRIMARY KEY NOT NULL
        CHECK(substr(source_trade_id, 1, 3) = 'g2:'),
    coverage_generation             INTEGER NOT NULL,
    semantic_revision               TEXT    NOT NULL,
    components_json                 TEXT    NOT NULL,
    wallet_hex                      TEXT    NOT NULL,
    transaction_hash                TEXT    NOT NULL,
    activity_type                   TEXT    NOT NULL,
    condition_id                    TEXT    NULL,
    asset                           TEXT    NULL,
    outcome_id                      INTEGER NULL,
    side                            TEXT    NULL CHECK(side IN ('buy','sell')),
    row_count                       INTEGER NOT NULL CHECK(row_count > 0),
    share_amount_str                TEXT    NOT NULL,
    price_weighted_share_amount_str TEXT    NOT NULL,
    source_usdc_amount_str          TEXT    NOT NULL,
    source_time_unix                INTEGER NOT NULL,
    is_combo                        INTEGER NOT NULL CHECK(is_combo IN (0,1)),
    schema_version                  INTEGER NOT NULL CHECK(schema_version = 2),
    parser_version                  INTEGER NOT NULL CHECK(parser_version = 2)
);
CREATE INDEX IF NOT EXISTS idx_activity_groups_v2_wallet_time
    ON activity_groups_v2(wallet_hex, source_time_unix, source_trade_id);
CREATE INDEX IF NOT EXISTS idx_activity_groups_v2_condition
    ON activity_groups_v2(condition_id, outcome_id, source_time_unix);

CREATE TABLE IF NOT EXISTS cache_frozen_payload_verifications (
    reference_sha256          TEXT PRIMARY KEY NOT NULL,
    active_wallets_json       TEXT    NOT NULL,
    freshness_json            TEXT    NOT NULL,
    legacy_trade_count        INTEGER NOT NULL,
    cross_generation_matches  INTEGER NOT NULL CHECK(cross_generation_matches = 0),
    verified_at_unix          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_v2_migration_state (
    singleton              INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
    phase                  TEXT    NOT NULL CHECK(phase IN (
        'schema_sealed','frozen_payload_verified','finalized'
    )),
    input_manifest_sha256  TEXT    NOT NULL,
    updated_at_unix        INTEGER NOT NULL
);
";

/// Operator-captured identity of the verified online-backup input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheV2BuildManifest {
    pub manifest_version: u32,
    pub backup_sha256: String,
    pub source_bounds: Value,
    pub cursors: Value,
    pub hashes: BTreeMap<String, String>,
    pub sealed_at_unix: i64,
}

/// Complete activity walk needed before a v2 side cache can be finalized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityCoverageManifestV2 {
    pub generation: u64,
    pub source_bounds: Value,
    pub cursors: Value,
    pub page_hashes: Vec<String>,
    pub completed_at_unix: i64,
    pub schema_version: u32,
    pub parser_version: u32,
}

/// Exact cache evidence captured with the accepted publication payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCacheFreshness {
    pub newest_trade_unix: i64,
    pub newest_resolution_fetch_unix: i64,
    pub clob_cursor: String,
    pub clob_cursor_updated_at: i64,
}

/// Supplied frozen reference used to rerun the production active filter against
/// the sealed v1 tables. `ranked_wallets` is the pre-filter universe and
/// `active_wallets` is the accepted set, both canonical lowercase addresses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenPayloadReference {
    pub version: u32,
    pub process_now_unix: i64,
    pub active_window_hours: u64,
    pub max_cache_staleness_hours: u64,
    pub ranked_wallets: Vec<String>,
    pub active_wallets: Vec<String>,
    pub freshness: FrozenCacheFreshness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CacheMigrationReport {
    pub cache_path: PathBuf,
    pub input_manifest_sha256: String,
    pub legacy_trade_count: u64,
    pub legacy_resolution_count: u64,
    pub legacy_cursor_count: u64,
    pub resumed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrozenPayloadVerification {
    pub reference_sha256: String,
    pub active_wallets: Vec<String>,
    pub freshness: FrozenCacheFreshness,
    pub legacy_trade_count: u64,
    pub cross_generation_matches: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFinalStageRecord {
    pub version: u32,
    pub cache_path: PathBuf,
    pub cache_sha256: String,
    pub schema_version: i64,
    pub sealed_generation: u64,
    pub activity_coverage_generation: u64,
    pub payout_coverage_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheActivationRequest {
    pub fixed_path: PathBuf,
    pub side_path: PathBuf,
    pub version_one_backup_path: PathBuf,
    pub expected_side_sha256: String,
}

#[derive(Debug, Serialize)]
pub struct CacheActivationReport {
    pub installed_path: PathBuf,
    pub installed_sha256: String,
    pub version_one_backup_path: PathBuf,
    pub activation_evidence: Option<ReclamationEvidenceReport>,
    pub resumed: bool,
}

/// Populate a complete fixed-end v2 activity generation for the preserved
/// active/tradeable universe. Partial rows are harmless: the coverage manifest
/// is written only after every wallet completes, and exact group inserts make a
/// retry idempotent.
pub async fn populate_activity_v2(
    cache_path: &Path,
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    fixed_end_unix: i64,
    generation: u64,
    completed_at_unix: i64,
) -> Result<ActivityCoverageManifestV2, BootstrapError> {
    let mut cache = crate::cache::WalletCache::open(cache_path)?;
    if cache.schema_version()? != CACHE_SCHEMA_VERSION_V2 {
        return invalid("activity v2 population requires a schema-v2 side cache".to_owned());
    }
    let mut wallets = cache.active_tradeable_wallet_hexes()?;
    wallets.sort();
    let mut page_hashes = Vec::new();
    let mut cursors = BTreeMap::new();
    for wallet_hex in &wallets {
        let wallet =
            WalletAddress::from_hex(wallet_hex).map_err(|error| BootstrapError::Invalid {
                message: format!("active universe contains invalid wallet {wallet_hex}: {error}"),
            })?;
        let complete = fetch_complete_activity(fetcher, base_url, wallet, None, fixed_end_unix)
            .await
            .map_err(|error| BootstrapError::Polymarket {
                wallet: wallet_hex.clone(),
                message: error.to_string(),
            })?;
        for page in &complete.pages {
            page_hashes.push(format!("{wallet_hex}:{}", page.raw_page_hash));
        }
        for bucket in complete
            .buckets()
            .map_err(|error| BootstrapError::Polymarket {
                wallet: wallet_hex.clone(),
                message: error.to_string(),
            })?
        {
            for aggregate in bucket {
                cache.insert_activity_aggregate_v2(generation, &aggregate)?;
            }
        }
        cursors.insert(wallet_hex.clone(), fixed_end_unix);
    }
    drop(cache);
    page_hashes.sort();
    let manifest = ActivityCoverageManifestV2 {
        generation,
        source_bounds: serde_json::json!({
            "start_exclusive": null,
            "end_inclusive": fixed_end_unix,
            "wallet_count": wallets.len(),
        }),
        cursors: serde_json::to_value(cursors)?,
        page_hashes,
        completed_at_unix,
        schema_version: ACTIVITY_SCHEMA_VERSION,
        parser_version: ACTIVITY_PARSER_VERSION,
    };
    record_activity_coverage_v2(cache_path, &manifest)?;
    Ok(manifest)
}

/// Seal a verified hash-qualified online backup as generation two.
///
/// `PRAGMA integrity_check` runs before mutation. A WAL backup is checkpointed
/// with `wal_checkpoint(TRUNCATE)` and must report `busy=0` and every frame
/// checkpointed; otherwise migration refuses rather than guessing whether the
/// main file contains the committed tail.
pub fn migrate_cache_v2(
    cache_path: &Path,
    manifest_path: &Path,
) -> Result<CacheMigrationReport, BootstrapError> {
    require_regular_file(cache_path, "cache online backup")?;
    require_regular_file(manifest_path, "cache build manifest")?;
    let manifest_bytes = std::fs::read(manifest_path)?;
    let manifest: CacheV2BuildManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.manifest_version != CACHE_BUILD_MANIFEST_VERSION {
        return invalid(format!(
            "cache build manifest version {} is unsupported",
            manifest.manifest_version
        ));
    }
    validate_hex_sha256(&manifest.backup_sha256, "backup_sha256")?;
    let before_hash = sha256_file(cache_path)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);

    let mut connection = open_existing_rw(cache_path)?;
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found == CACHE_SCHEMA_VERSION_V2 {
        integrity_check(&connection)?;
        checkpoint_truncate(&connection)?;
        let stored: Option<(String, i64, i64, i64)> = connection
            .query_row(
                "SELECT input_manifest_sha256, legacy_trade_count, legacy_resolution_count, \
                        legacy_cursor_count FROM sealed_generation_manifests WHERE generation = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((stored_hash, trades, resolutions, cursors)) = stored else {
            return invalid("v2 cache omitted its sealed-generation manifest".to_owned());
        };
        if stored_hash != manifest_sha256 {
            return invalid("v2 cache was built from a different manifest".to_owned());
        }
        connection.close().map_err(|(_, error)| error)?;
        sync_file_and_parent(cache_path)?;
        return Ok(CacheMigrationReport {
            cache_path: std::fs::canonicalize(cache_path)?,
            input_manifest_sha256: manifest_sha256,
            legacy_trade_count: to_u64(trades, "legacy trade count")?,
            legacy_resolution_count: to_u64(resolutions, "legacy resolution count")?,
            legacy_cursor_count: to_u64(cursors, "legacy cursor count")?,
            resumed: true,
        });
    }
    if before_hash != manifest.backup_sha256 {
        return invalid(format!(
            "online-backup hash mismatch: manifest {}, file {before_hash}",
            manifest.backup_sha256
        ));
    }
    verify_manifest_wal_binding(cache_path, &manifest)?;
    integrity_check(&connection)?;
    checkpoint_truncate(&connection)?;
    require_reclamation_ready(&connection)?;
    if found != 0 && found != CACHE_SCHEMA_VERSION_V1 {
        return invalid(format!("cannot migrate cache schema version {found}"));
    }
    for table in ["trades", "market_resolutions", "source_cursor"] {
        require_table(&connection, table)?;
    }

    let legacy_trade_count = count_rows(&connection, "trades")?;
    let legacy_resolution_count = count_rows(&connection, "market_resolutions")?;
    let legacy_cursor_count = count_rows(&connection, "source_cursor")?;
    let source_bounds_json = canonical_json(&manifest.source_bounds)?;
    let cursors_json = canonical_json(&manifest.cursors)?;
    let hashes_json = canonical_json(&manifest.hashes)?;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "ALTER TABLE trades RENAME TO trades_v1_sealed;
         ALTER TABLE market_resolutions RENAME TO market_resolutions_v1_sealed;
         ALTER TABLE source_cursor RENAME TO source_cursor_v1_sealed;",
    )?;
    transaction.execute_batch(V2_SCHEMA)?;
    transaction.execute(
        "INSERT INTO sealed_generation_manifests
             (generation, input_manifest_sha256, source_bounds_json, cursors_json, hashes_json,
              legacy_trade_count, legacy_resolution_count, legacy_cursor_count, sealed_at_unix)
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            manifest_sha256,
            source_bounds_json,
            cursors_json,
            hashes_json,
            legacy_trade_count,
            legacy_resolution_count,
            legacy_cursor_count,
            manifest.sealed_at_unix,
        ],
    )?;
    transaction.execute(
        "INSERT INTO cache_v2_migration_state
             (singleton, phase, input_manifest_sha256, updated_at_unix)
         VALUES (1, 'schema_sealed', ?1, ?2)",
        params![manifest_sha256, manifest.sealed_at_unix],
    )?;
    transaction.pragma_update(None, "user_version", CACHE_SCHEMA_VERSION_V2)?;
    transaction.commit()?;
    integrity_check(&connection)?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    sync_file_and_parent(cache_path)?;

    Ok(CacheMigrationReport {
        cache_path: std::fs::canonicalize(cache_path)?,
        input_manifest_sha256: manifest_sha256,
        legacy_trade_count: to_u64(legacy_trade_count, "legacy trade count")?,
        legacy_resolution_count: to_u64(legacy_resolution_count, "legacy resolution count")?,
        legacy_cursor_count: to_u64(legacy_cursor_count, "legacy cursor count")?,
        resumed: false,
    })
}

/// Install a complete v2 activity-coverage manifest after normalized groups
/// have been stored through [`crate::cache::WalletCache::insert_activity_aggregate_v2`].
pub fn record_activity_coverage_v2(
    cache_path: &Path,
    manifest: &ActivityCoverageManifestV2,
) -> Result<(), BootstrapError> {
    if manifest.schema_version != ACTIVITY_SCHEMA_VERSION
        || manifest.parser_version != ACTIVITY_PARSER_VERSION
    {
        return invalid(
            "activity coverage manifest has the wrong parser/schema version".to_owned(),
        );
    }
    let mut connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    let group_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM activity_groups_v2 WHERE coverage_generation = ?1",
        params![to_i64(manifest.generation, "activity coverage generation")?],
        |row| row.get(0),
    )?;
    let transaction = connection.transaction()?;
    let changed = transaction.execute(
        "INSERT INTO activity_coverage_manifests_v2
             (generation, source_bounds_json, cursors_json, page_hashes_json, group_count,
              schema_version, parser_version, completed_at_unix)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(generation) DO UPDATE SET
             source_bounds_json = excluded.source_bounds_json,
             cursors_json = excluded.cursors_json,
             page_hashes_json = excluded.page_hashes_json,
             group_count = excluded.group_count,
             schema_version = excluded.schema_version,
             parser_version = excluded.parser_version,
             completed_at_unix = excluded.completed_at_unix
         WHERE activity_coverage_manifests_v2.source_bounds_json = excluded.source_bounds_json
           AND activity_coverage_manifests_v2.cursors_json = excluded.cursors_json
           AND activity_coverage_manifests_v2.page_hashes_json = excluded.page_hashes_json
           AND activity_coverage_manifests_v2.group_count = excluded.group_count
           AND activity_coverage_manifests_v2.schema_version = excluded.schema_version
           AND activity_coverage_manifests_v2.parser_version = excluded.parser_version
           AND activity_coverage_manifests_v2.completed_at_unix = excluded.completed_at_unix",
        params![
            to_i64(manifest.generation, "activity coverage generation")?,
            canonical_json(&manifest.source_bounds)?,
            canonical_json(&manifest.cursors)?,
            canonical_json(&manifest.page_hashes)?,
            group_count,
            i64::from(manifest.schema_version),
            i64::from(manifest.parser_version),
            manifest.completed_at_unix,
        ],
    )?;
    if changed == 0 {
        return invalid(format!(
            "activity coverage generation {} was replayed with different evidence",
            manifest.generation
        ));
    }
    transaction.commit()?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    sync_file_and_parent(cache_path)
}

/// Re-run the exact active/freshness filter against the sealed v1 generation.
pub fn verify_frozen_payload_v1(
    cache_path: &Path,
    reference_path: &Path,
    verified_at_unix: i64,
) -> Result<FrozenPayloadVerification, BootstrapError> {
    let reference_bytes = std::fs::read(reference_path)?;
    let reference: FrozenPayloadReference = serde_json::from_slice(&reference_bytes)?;
    if reference.version != FROZEN_PAYLOAD_REFERENCE_VERSION {
        return invalid(format!(
            "frozen payload reference version {} is unsupported",
            reference.version
        ));
    }
    let reference_sha256 = sha256_bytes(&reference_bytes);
    let connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    let cutoff = reference
        .process_now_unix
        .checked_sub(hours_to_seconds(reference.active_window_hours)?)
        .ok_or_else(|| BootstrapError::Invalid {
            message: "active-filter cutoff overflow".to_owned(),
        })?;
    let staleness = hours_to_seconds(reference.max_cache_staleness_hours)?;

    let mut active = Vec::new();
    for wallet in &reference.ranked_wallets {
        validate_wallet_hex(wallet)?;
        let last: Option<i64> = connection.query_row(
            "SELECT MAX(timestamp_unix) FROM trades_v1_sealed WHERE wallet_hex = ?1",
            params![wallet],
            |row| row.get(0),
        )?;
        if last.is_some_and(|timestamp| timestamp >= cutoff) {
            active.push(wallet.clone());
        }
    }
    let mut expected = reference.active_wallets.clone();
    for wallet in &expected {
        validate_wallet_hex(wallet)?;
    }
    active.sort();
    active.dedup();
    expected.sort();
    expected.dedup();
    if active != expected {
        return invalid(format!(
            "sealed v1 active-filter mismatch: expected {expected:?}, got {active:?}"
        ));
    }

    let actual_freshness = frozen_freshness(&connection)?;
    if actual_freshness != reference.freshness {
        return invalid(format!(
            "sealed v1 freshness mismatch: expected {:?}, got {actual_freshness:?}",
            reference.freshness
        ));
    }
    for (label, timestamp) in [
        ("trade", actual_freshness.newest_trade_unix),
        ("resolution", actual_freshness.newest_resolution_fetch_unix),
        ("CLOB cursor", actual_freshness.clob_cursor_updated_at),
    ] {
        if reference.process_now_unix.saturating_sub(timestamp) > staleness {
            return invalid(format!(
                "sealed v1 {label} freshness exceeds the supplied bound"
            ));
        }
    }
    if !actual_freshness.clob_cursor.is_empty() {
        return invalid("sealed v1 CLOB cursor is not terminal".to_owned());
    }

    let legacy_trade_count = count_rows(&connection, "trades_v1_sealed")?;
    let cross_generation_matches: i64 = connection.query_row(
        "SELECT COUNT(*) FROM activity_groups_v2 v2
         JOIN trades_v1_sealed v1 ON v1.source_trade_id = v2.source_trade_id",
        [],
        |row| row.get(0),
    )?;
    if cross_generation_matches != 0 {
        return invalid("a sealed v1 trade identity entered activity_groups_v2".to_owned());
    }
    let active_json = canonical_json(&active)?;
    let freshness_json = canonical_json(&actual_freshness)?;
    connection.execute(
        "INSERT INTO cache_frozen_payload_verifications
             (reference_sha256, active_wallets_json, freshness_json, legacy_trade_count,
              cross_generation_matches, verified_at_unix)
         VALUES (?1, ?2, ?3, ?4, 0, ?5)
         ON CONFLICT(reference_sha256) DO UPDATE SET
             active_wallets_json = excluded.active_wallets_json,
             freshness_json = excluded.freshness_json,
             legacy_trade_count = excluded.legacy_trade_count,
             cross_generation_matches = 0",
        params![
            reference_sha256,
            active_json,
            freshness_json,
            legacy_trade_count,
            verified_at_unix,
        ],
    )?;
    connection.execute(
        "UPDATE cache_v2_migration_state
         SET phase = CASE WHEN phase = 'schema_sealed' THEN 'frozen_payload_verified' ELSE phase END,
             updated_at_unix = ?1 WHERE singleton = 1",
        params![verified_at_unix],
    )?;
    checkpoint_truncate(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    sync_file_and_parent(cache_path)?;
    Ok(FrozenPayloadVerification {
        reference_sha256,
        active_wallets: active,
        freshness: actual_freshness,
        legacy_trade_count: to_u64(legacy_trade_count, "legacy trade count")?,
        cross_generation_matches: to_u64(cross_generation_matches, "cross-generation match count")?,
    })
}

/// Close and seal a complete v2 side cache, then emit a hash-bound stage record.
pub fn finalize_cache_v2(
    cache_path: &Path,
    stage_record_path: &Path,
    finalized_at_unix: i64,
) -> Result<CacheFinalStageRecord, BootstrapError> {
    let connection = open_existing_rw(cache_path)?;
    require_schema(&connection, CACHE_SCHEMA_VERSION_V2)?;
    integrity_check(&connection)?;
    let sealed_generation = required_max(&connection, "sealed_generation_manifests", "generation")?;
    let activity_generation =
        required_max(&connection, "activity_coverage_manifests_v2", "generation")?;
    let payout_generation = required_max(
        &connection,
        "clob_payout_coverage_manifests_v2",
        "generation",
    )?;
    verify_payout_coverage(&connection, payout_generation)?;
    let orphan_activity: i64 = connection.query_row(
        "SELECT COUNT(*) FROM activity_groups_v2 groups_v2
         LEFT JOIN activity_coverage_manifests_v2 manifests
           ON manifests.generation = groups_v2.coverage_generation
         WHERE manifests.generation IS NULL",
        [],
        |row| row.get(0),
    )?;
    let changed_activity_generation: i64 = connection.query_row(
        "SELECT COUNT(*) FROM activity_coverage_manifests_v2 manifests
         WHERE manifests.group_count != (
             SELECT COUNT(*) FROM activity_groups_v2 groups_v2
             WHERE groups_v2.coverage_generation = manifests.generation
         )",
        [],
        |row| row.get(0),
    )?;
    if orphan_activity != 0 || changed_activity_generation != 0 {
        return invalid(format!(
            "activity coverage is incomplete: orphan_groups={orphan_activity}, changed_generations={changed_activity_generation}"
        ));
    }
    let frozen_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM cache_frozen_payload_verifications",
        [],
        |row| row.get(0),
    )?;
    if frozen_count == 0 {
        return invalid("frozen-payload verification is missing".to_owned());
    }
    connection.execute(
        "UPDATE cache_v2_migration_state
         SET phase = 'finalized', updated_at_unix = ?1 WHERE singleton = 1",
        params![finalized_at_unix],
    )?;
    checkpoint_truncate(&connection)?;
    integrity_check(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    reject_nonempty_sidecars(cache_path)?;
    sync_file_and_parent(cache_path)?;
    let record = CacheFinalStageRecord {
        version: FINAL_STAGE_RECORD_VERSION,
        cache_path: std::fs::canonicalize(cache_path)?,
        cache_sha256: sha256_file(cache_path)?,
        schema_version: CACHE_SCHEMA_VERSION_V2,
        sealed_generation: to_u64(sealed_generation, "sealed generation")?,
        activity_coverage_generation: to_u64(activity_generation, "activity generation")?,
        payout_coverage_generation: to_u64(payout_generation, "payout generation")?,
    };
    atomic_write_json(stage_record_path, &record)?;
    Ok(record)
}

/// Install a finalized v2 main at the fixed Forge path under the reviewed
/// loop → one-shot run → cache lock order.
pub fn activate_cache_v2(
    request: &CacheActivationRequest,
) -> Result<CacheActivationReport, BootstrapError> {
    let _locks = ForgeActivationLocks::acquire(&request.fixed_path)?;
    require_regular_file(&request.fixed_path, "current fixed cache")?;
    validate_hex_sha256(&request.expected_side_sha256, "expected side sha256")?;
    if !request.side_path.exists() {
        let installed_hash = sha256_file(&request.fixed_path)?;
        if installed_hash != request.expected_side_sha256 {
            return invalid(
                "v2 side cache is missing and the fixed cache has another hash".to_owned(),
            );
        }
        require_regular_file(&request.version_one_backup_path, "version-one cache backup")?;
        verify_version_one_main(&request.version_one_backup_path)?;
        let installed = open_existing_ro(&request.fixed_path)?;
        require_schema(&installed, CACHE_SCHEMA_VERSION_V2)?;
        integrity_check(&installed)?;
        verify_finalized_v2_manifests(&installed)?;
        installed.close().map_err(|(_, error)| error)?;
        reject_nonempty_sidecars(&request.fixed_path)?;
        return Ok(CacheActivationReport {
            installed_path: std::fs::canonicalize(&request.fixed_path)?,
            installed_sha256: installed_hash,
            version_one_backup_path: std::fs::canonicalize(&request.version_one_backup_path)?,
            activation_evidence: None,
            resumed: true,
        });
    }
    require_regular_file(&request.side_path, "version-two side cache")?;
    let activation_evidence = capture_reclamation_evidence(
        &request.fixed_path,
        &eval_results_dir_for_cache(&request.fixed_path),
    )?;
    if !activation_evidence.activation_ready {
        return invalid(
            "current cache failed the locked reclamation/index activation gate".to_owned(),
        );
    }
    if sha256_file(&request.side_path)? != request.expected_side_sha256 {
        return invalid("version-two side-cache hash changed after finalization".to_owned());
    }
    require_same_device(
        &request.fixed_path,
        &request.side_path,
        &request.version_one_backup_path,
    )?;

    let current = open_existing_rw(&request.fixed_path)?;
    require_reclamation_ready(&current)?;
    checkpoint_truncate(&current)?;
    integrity_check(&current)?;
    let current_version: i64 =
        current.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current_version != 0 && current_version != CACHE_SCHEMA_VERSION_V1 {
        return invalid(format!(
            "cache activation expected version one at the fixed path, found {current_version}"
        ));
    }
    current.close().map_err(|(_, error)| error)?;

    let side = open_existing_rw(&request.side_path)?;
    require_schema(&side, CACHE_SCHEMA_VERSION_V2)?;
    integrity_check(&side)?;
    checkpoint_truncate(&side)?;
    side.close().map_err(|(_, error)| error)?;
    reject_nonempty_sidecars(&request.side_path)?;
    if request.version_one_backup_path.exists() {
        if sha256_file(&request.version_one_backup_path)? != sha256_file(&request.fixed_path)? {
            return invalid(format!(
                "existing version-one backup differs from the fixed cache: {}",
                request.version_one_backup_path.display()
            ));
        }
    } else {
        preserve_main_and_sidecars(&request.fixed_path, &request.version_one_backup_path)?;
    }
    verify_version_one_main(&request.version_one_backup_path)?;
    remove_sidecars(&request.fixed_path)?;
    std::fs::rename(&request.side_path, &request.fixed_path).map_err(map_rename_error)?;
    sync_parent(&request.fixed_path)?;
    reject_nonempty_sidecars(&request.fixed_path)?;
    let installed_hash = sha256_file(&request.fixed_path)?;
    if installed_hash != request.expected_side_sha256 {
        return invalid("installed cache hash differs from finalized side cache".to_owned());
    }
    let installed = open_existing_ro(&request.fixed_path)?;
    require_schema(&installed, CACHE_SCHEMA_VERSION_V2)?;
    integrity_check(&installed)?;
    verify_finalized_v2_manifests(&installed)?;
    installed.close().map_err(|(_, error)| error)?;
    // Even a read-only validation open may materialize empty WAL/SHM files for
    // a WAL-mode database. Remove those newly created empties as well; no
    // sidecar from either generation is part of the installed artifact.
    reject_nonempty_sidecars(&request.fixed_path)?;
    sync_parent(&request.fixed_path)?;
    Ok(CacheActivationReport {
        installed_path: std::fs::canonicalize(&request.fixed_path)?,
        installed_sha256: installed_hash,
        version_one_backup_path: std::fs::canonicalize(&request.version_one_backup_path)?,
        activation_evidence: Some(activation_evidence),
        resumed: false,
    })
}

/// Pre-activation rollback: restore only a checkpointed v1 main. WAL/SHM
/// sidecars are never restored.
pub fn rollback_cache_to_v1(
    fixed_path: &Path,
    version_one_backup_path: &Path,
    failed_v2_backup_path: &Path,
) -> Result<(), BootstrapError> {
    let _locks = ForgeActivationLocks::acquire(fixed_path)?;
    require_regular_file(version_one_backup_path, "version-one rollback main")?;
    verify_version_one_main(version_one_backup_path)?;
    require_same_device(fixed_path, version_one_backup_path, failed_v2_backup_path)?;
    if fixed_path.is_file() {
        let current = open_existing_rw(fixed_path)?;
        checkpoint_truncate(&current)?;
        integrity_check(&current)?;
        current.close().map_err(|(_, error)| error)?;
        if failed_v2_backup_path.exists() {
            if sha256_file(failed_v2_backup_path)? != sha256_file(fixed_path)? {
                return invalid(format!(
                    "existing failed-v2 backup differs from the fixed cache: {}",
                    failed_v2_backup_path.display()
                ));
            }
        } else {
            preserve_main_and_sidecars(fixed_path, failed_v2_backup_path)?;
        }
    }
    remove_sidecars(fixed_path)?;
    std::fs::rename(version_one_backup_path, fixed_path).map_err(map_rename_error)?;
    sync_parent(fixed_path)?;
    reject_nonempty_sidecars(fixed_path)?;
    let restored = open_existing_ro(fixed_path)?;
    let version: i64 = restored.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != 0 && version != CACHE_SCHEMA_VERSION_V1 {
        return invalid(format!("rollback main is not version one: {version}"));
    }
    integrity_check(&restored)?;
    Ok(())
}

fn frozen_freshness(connection: &Connection) -> Result<FrozenCacheFreshness, BootstrapError> {
    let newest_trade_unix: Option<i64> = connection.query_row(
        "SELECT MAX(timestamp_unix) FROM trades_v1_sealed",
        [],
        |row| row.get(0),
    )?;
    let newest_resolution_fetch_unix: Option<i64> = connection.query_row(
        "SELECT MAX(fetched_at_unix) FROM market_resolutions_v1_sealed",
        [],
        |row| row.get(0),
    )?;
    let cursor: Option<(String, i64)> = connection
        .query_row(
            "SELECT value, updated_at FROM source_cursor_v1_sealed WHERE key = 'clob_closed'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((clob_cursor, clob_cursor_updated_at)) = cursor else {
        return invalid("sealed v1 CLOB cursor is missing".to_owned());
    };
    Ok(FrozenCacheFreshness {
        newest_trade_unix: newest_trade_unix.ok_or_else(|| BootstrapError::Invalid {
            message: "sealed v1 trade watermark is missing".to_owned(),
        })?,
        newest_resolution_fetch_unix: newest_resolution_fetch_unix.ok_or_else(|| {
            BootstrapError::Invalid {
                message: "sealed v1 resolution watermark is missing".to_owned(),
            }
        })?,
        clob_cursor,
        clob_cursor_updated_at,
    })
}

fn require_reclamation_ready(connection: &Connection) -> Result<(), BootstrapError> {
    let pending: Option<String> = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'reclamation_pending'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if pending.is_some() {
        return invalid("cache reclamation_pending marker is present".to_owned());
    }
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'trades'")?;
    let present = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let missing = REQUIRED_TRADES_INDEXES
        .iter()
        .filter(|required| !present.iter().any(|actual| actual == **required))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return invalid(format!("required trades indexes are missing: {missing:?}"));
    }
    Ok(())
}

fn verify_manifest_wal_binding(
    cache_path: &Path,
    manifest: &CacheV2BuildManifest,
) -> Result<(), BootstrapError> {
    let wal = sidecar_path(cache_path, "-wal");
    let wal_is_nonempty = wal.is_file() && std::fs::metadata(&wal)?.len() != 0;
    let expected = manifest.hashes.get("backup_wal_sha256");
    match (wal_is_nonempty, expected) {
        (true, Some(expected_hash)) => {
            validate_hex_sha256(expected_hash, "hashes.backup_wal_sha256")?;
            let actual = sha256_file(&wal)?;
            if &actual != expected_hash {
                return invalid(format!(
                    "online-backup WAL hash mismatch: manifest {expected_hash}, file {actual}"
                ));
            }
            Ok(())
        }
        (true, None) => invalid(
            "online backup has uncheckpointed WAL frames without hashes.backup_wal_sha256"
                .to_owned(),
        ),
        (false, Some(_)) => invalid(
            "cache build manifest binds a WAL but the online backup has no non-empty WAL"
                .to_owned(),
        ),
        (false, None) => Ok(()),
    }
}

fn integrity_check(connection: &Connection) -> Result<(), BootstrapError> {
    let result: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if result == "ok" {
        Ok(())
    } else {
        invalid(format!("SQLite integrity_check failed: {result}"))
    }
}

fn checkpoint_truncate(connection: &Connection) -> Result<(), BootstrapError> {
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if journal_mode.eq_ignore_ascii_case("wal") {
        let (busy, log, checkpointed): (i64, i64, i64) =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || log != checkpointed {
            return invalid(format!(
                "SQLite checkpoint incomplete: busy={busy}, log={log}, checkpointed={checkpointed}"
            ));
        }
    }
    Ok(())
}

fn open_existing_rw(path: &Path) -> Result<Connection, BootstrapError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(connection)
}

fn open_existing_ro(path: &Path) -> Result<Connection, BootstrapError> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(BootstrapError::from)
}

fn require_schema(connection: &Connection, expected: i64) -> Result<(), BootstrapError> {
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found == expected {
        Ok(())
    } else {
        invalid(format!(
            "cache schema version is {found}, expected {expected}"
        ))
    }
}

fn verify_version_one_main(path: &Path) -> Result<(), BootstrapError> {
    let connection = open_existing_ro(path)?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != 0 && version != CACHE_SCHEMA_VERSION_V1 {
        return invalid(format!(
            "version-one backup has unsupported cache schema {version}"
        ));
    }
    integrity_check(&connection)?;
    connection.close().map_err(|(_, error)| error)?;
    Ok(())
}

fn require_table(connection: &Connection, table: &str) -> Result<(), BootstrapError> {
    let found: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        params![table],
        |row| row.get(0),
    )?;
    if found {
        Ok(())
    } else {
        invalid(format!("required cache table {table} is missing"))
    }
}

fn count_rows(connection: &Connection, table: &str) -> Result<i64, BootstrapError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection
        .query_row(&sql, [], |row| row.get(0))
        .map_err(BootstrapError::from)
}

fn required_max(connection: &Connection, table: &str, column: &str) -> Result<i64, BootstrapError> {
    let sql = format!("SELECT MAX({column}) FROM {table}");
    let value: Option<i64> = connection.query_row(&sql, [], |row| row.get(0))?;
    value.ok_or_else(|| BootstrapError::Invalid {
        message: format!("required manifest table {table} is empty"),
    })
}

fn verify_finalized_v2_manifests(connection: &Connection) -> Result<(), BootstrapError> {
    if required_max(connection, "sealed_generation_manifests", "generation")? != 1 {
        return invalid("installed cache has an invalid sealed generation".to_owned());
    }
    required_max(connection, "activity_coverage_manifests_v2", "generation")?;
    required_max(
        connection,
        "clob_payout_coverage_manifests_v2",
        "generation",
    )?;
    let frozen: i64 = connection.query_row(
        "SELECT COUNT(*) FROM cache_frozen_payload_verifications",
        [],
        |row| row.get(0),
    )?;
    let phase: Option<String> = connection
        .query_row(
            "SELECT phase FROM cache_v2_migration_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if frozen == 0 || phase.as_deref() != Some("finalized") {
        return invalid(
            "installed cache omitted its frozen-payload proof or finalized stage".to_owned(),
        );
    }
    Ok(())
}

fn verify_payout_coverage(connection: &Connection, generation: i64) -> Result<(), BootstrapError> {
    let (manifest_json, market_count, schema_version, parser_version): (String, i64, i64, i64) =
        connection.query_row(
            "SELECT manifest_json, market_count, schema_version, parser_version
             FROM clob_payout_coverage_manifests_v2 WHERE generation = ?1",
            params![generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    let manifest: ClobCoverageManifest = serde_json::from_str(&manifest_json)?;
    let rebuilt = ClobCoverageManifest::complete(manifest.generation, manifest.pages.clone())
        .map_err(|error| BootstrapError::Invalid {
            message: format!("CLOB payout coverage manifest is incomplete: {error}"),
        })?;
    if rebuilt != manifest
        || to_i64(manifest.generation, "CLOB payout generation")? != generation
        || manifest.schema_version != CLOB_RESOLUTION_SCHEMA_VERSION
        || manifest.parser_version != CLOB_RESOLUTION_PARSER_VERSION
        || schema_version != i64::from(CLOB_RESOLUTION_SCHEMA_VERSION)
        || parser_version != i64::from(CLOB_RESOLUTION_PARSER_VERSION)
        || to_i64(manifest.counts.markets, "CLOB payout market count")? != market_count
    {
        return invalid(
            "CLOB payout coverage manifest does not match its stored generation".to_owned(),
        );
    }
    let evidence_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM clob_payout_evidence_v2 WHERE coverage_generation = ?1",
        params![generation],
        |row| row.get(0),
    )?;
    if evidence_count != market_count {
        return invalid(format!(
            "CLOB payout coverage is incomplete: manifest={market_count}, evidence={evidence_count}"
        ));
    }
    Ok(())
}

fn require_regular_file(path: &Path, label: &str) -> Result<(), BootstrapError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        invalid(format!(
            "{label} is not a regular non-symlink file: {}",
            path.display()
        ))
    }
}

fn validate_wallet_hex(wallet: &str) -> Result<(), BootstrapError> {
    if wallet.len() == 42
        && wallet.starts_with("0x")
        && wallet[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid(format!(
            "frozen payload has non-canonical wallet {wallet:?}"
        ))
    }
}

fn validate_hex_sha256(value: &str, field: &str) -> Result<(), BootstrapError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        invalid(format!(
            "{field} must be 64 lowercase hexadecimal characters"
        ))
    }
}

fn canonical_json(value: &impl Serialize) -> Result<String, BootstrapError> {
    serde_json::to_string(value).map_err(BootstrapError::from)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn sha256_file(path: &Path) -> Result<String, BootstrapError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

fn hours_to_seconds(hours: u64) -> Result<i64, BootstrapError> {
    let seconds = hours
        .checked_mul(3_600)
        .ok_or_else(|| BootstrapError::Invalid {
            message: "hour bound overflow".to_owned(),
        })?;
    i64::try_from(seconds).map_err(|_| BootstrapError::Invalid {
        message: "hour bound exceeds i64".to_owned(),
    })
}

fn to_i64(value: u64, field: &str) -> Result<i64, BootstrapError> {
    i64::try_from(value).map_err(|_| BootstrapError::Invalid {
        message: format!("{field} exceeds SQLite integer range"),
    })
}

fn to_u64(value: i64, field: &str) -> Result<u64, BootstrapError> {
    u64::try_from(value).map_err(|_| BootstrapError::Invalid {
        message: format!("{field} is negative"),
    })
}

fn sync_file_and_parent(path: &Path) -> Result<(), BootstrapError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?
        .sync_all()?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), BootstrapError> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<(), BootstrapError> {
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("stage"),
        std::process::id()
    ));
    let rendered = serde_json::to_vec_pretty(value)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&rendered)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&temp, path).map_err(map_rename_error)?;
    sync_parent(path)
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn reject_nonempty_sidecars(path: &Path) -> Result<(), BootstrapError> {
    let wal = sidecar_path(path, "-wal");
    if wal.is_file() && std::fs::metadata(&wal)?.len() != 0 {
        return invalid(format!("non-empty SQLite WAL remains: {}", wal.display()));
    }
    if wal.exists() {
        std::fs::remove_file(wal)?;
    }
    // SQLite's shared-memory index has a fixed non-zero allocation even after
    // TRUNCATE checkpoints. Once every connection is closed and WAL is empty,
    // it contains no durable state and must be removed, not installed.
    let shared_memory = sidecar_path(path, "-shm");
    if shared_memory.exists() {
        std::fs::remove_file(shared_memory)?;
    }
    Ok(())
}

fn remove_sidecars(path: &Path) -> Result<(), BootstrapError> {
    for suffix in ["-wal", "-shm"] {
        let sidecar = sidecar_path(path, suffix);
        match std::fs::remove_file(&sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn preserve_main_and_sidecars(source: &Path, target: &Path) -> Result<(), BootstrapError> {
    if target.exists() {
        return invalid(format!(
            "immutable backup target exists: {}",
            target.display()
        ));
    }
    copy_file_atomic(source, target)?;
    for suffix in ["-wal", "-shm"] {
        let source_sidecar = sidecar_path(source, suffix);
        if source_sidecar.exists() {
            let target_sidecar = sidecar_path(target, suffix);
            copy_file_atomic(&source_sidecar, &target_sidecar)?;
        }
    }
    sync_parent(target)
}

fn copy_file_atomic(source: &Path, target: &Path) -> Result<(), BootstrapError> {
    let mut pending = target.as_os_str().to_owned();
    pending.push(".pending");
    let pending = PathBuf::from(pending);
    match std::fs::remove_file(&pending) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::copy(source, &pending)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pending)?
        .sync_all()?;
    std::fs::rename(&pending, target).map_err(map_rename_error)?;
    sync_parent(target)
}

fn require_same_device(first: &Path, second: &Path, third: &Path) -> Result<(), BootstrapError> {
    let device = device_for_existing_or_parent(first)?;
    for path in [second, third] {
        let other = device_for_existing_or_parent(path)?;
        if other != device {
            return invalid(format!(
                "atomic rename refused across devices: {} is device {other}, expected {device}",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn device_for_existing_or_parent(path: &Path) -> Result<u64, BootstrapError> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = if path.exists() {
        std::fs::metadata(path)?
    } else {
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::metadata(parent)?
    };
    Ok(metadata.dev())
}

#[cfg(not(unix))]
fn device_for_existing_or_parent(_path: &Path) -> Result<u64, BootstrapError> {
    Ok(0)
}

fn map_rename_error(error: std::io::Error) -> BootstrapError {
    if error.raw_os_error() == Some(18) {
        BootstrapError::Invalid {
            message: "atomic rename refused across devices".to_owned(),
        }
    } else {
        BootstrapError::Io(error)
    }
}

fn invalid<T>(message: String) -> Result<T, BootstrapError> {
    Err(BootstrapError::Invalid { message })
}
