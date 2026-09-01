//! Machine-owned schema-v1-to-v2 migration metadata and log-boundary verification (#544).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use blake3::Hash;
use pe_core_types::EventSeq;
use pe_event_log::LogTailBinding;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};

use crate::PaperStateError;
use crate::schema::{SCHEMA, SCHEMA_VERSION};

const MIGRATION_RECORD_KEY: &str = "trustworthy_v2_migration_record";
const VERSION_ONE_ORIGIN_HASH_KEY: &str = "trustworthy_v2_origin_v1_blake3";

/// Stable owner name for one of the three append-only histories bound at cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableLogName {
    Source,
    Paper,
    LiveJournal,
}

impl std::fmt::Display for DurableLogName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => formatter.write_str("source"),
            Self::Paper => formatter.write_str("paper"),
            Self::LiveJournal => formatter.write_str("live_journal"),
        }
    }
}

/// One exact binding for every log involved in the authority transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLogBindings {
    pub source: LogTailBinding,
    pub paper: LogTailBinding,
    pub live_journal: LogTailBinding,
}

/// Machine-owned migration state. Serialized as stable snake-case text in schema-v1 `meta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    BoundaryRecorded,
    VersionTwoInputsAppending,
    SideStateBuilt,
    ActivationTailsRecorded,
    Installed,
}

impl MigrationPhase {
    fn next(self) -> Option<Self> {
        match self {
            Self::BoundaryRecorded => Some(Self::VersionTwoInputsAppending),
            Self::VersionTwoInputsAppending => Some(Self::SideStateBuilt),
            Self::SideStateBuilt => Some(Self::ActivationTailsRecorded),
            Self::ActivationTailsRecorded => Some(Self::Installed),
            Self::Installed => None,
        }
    }
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BoundaryRecorded => formatter.write_str("boundary_recorded"),
            Self::VersionTwoInputsAppending => formatter.write_str("version_two_inputs_appending"),
            Self::SideStateBuilt => formatter.write_str("side_state_built"),
            Self::ActivationTailsRecorded => formatter.write_str("activation_tails_recorded"),
            Self::Installed => formatter.write_str("installed"),
        }
    }
}

/// Complete resumable authority-transition record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRecord {
    pub version_one_boundary: DurableLogBindings,
    pub activation_tails: Option<DurableLogBindings>,
    pub phase: MigrationPhase,
    pub side_main_path: PathBuf,
    /// Named BLAKE3/SHA-256 input hashes. `BTreeMap` makes encoding deterministic.
    pub input_hashes: BTreeMap<String, String>,
}

/// Boundary component whose exact equality check failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryField {
    Path,
    PhysicalTail,
    LastSequence,
    LastHash,
}

/// Typed fail-closed mismatch returned without mutating either the log or paper-state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{log} log {field:?} does not match its stored boundary")]
pub struct BoundaryMismatch {
    pub log: DurableLogName,
    pub field: BoundaryField,
}

/// Compare current scanner/native-replay results to stored bindings exactly.
pub fn verify_log_bindings(
    stored: &DurableLogBindings,
    current: &DurableLogBindings,
) -> Result<(), BoundaryMismatch> {
    verify_one(DurableLogName::Source, &stored.source, &current.source)?;
    verify_one(DurableLogName::Paper, &stored.paper, &current.paper)?;
    verify_one(
        DurableLogName::LiveJournal,
        &stored.live_journal,
        &current.live_journal,
    )
}

/// Require a resume/mirror target to be the exact resolved side main stored in the record.
pub fn verify_side_main_path(
    record: &MigrationRecord,
    current_path: &Path,
) -> Result<(), PaperStateError> {
    let actual = std::fs::canonicalize(current_path)?;
    if actual == record.side_main_path {
        Ok(())
    } else {
        Err(PaperStateError::MigrationSidePathMismatch {
            expected: record.side_main_path.clone(),
            actual,
        })
    }
}

fn verify_one(
    log: DurableLogName,
    stored: &LogTailBinding,
    current: &LogTailBinding,
) -> Result<(), BoundaryMismatch> {
    let field = if stored.path != current.path {
        Some(BoundaryField::Path)
    } else if stored.physical_tail != current.physical_tail {
        Some(BoundaryField::PhysicalTail)
    } else if stored.last_sequence != current.last_sequence {
        Some(BoundaryField::LastSequence)
    } else if stored.last_hash != current.last_hash {
        Some(BoundaryField::LastHash)
    } else {
        None
    };
    match field {
        Some(field) => Err(BoundaryMismatch { log, field }),
        None => Ok(()),
    }
}

/// Bootstrap-only metadata access. These path-based methods inspect schema/migration state before
/// normal [`crate::PaperStateDb::open`] and therefore do not reject a version-two side main.
pub struct MigrationMetadata;

/// Result of building a version-two side main from the final checkpointed v1
/// generation. The immutable v1 hash is captured after checkpoint/truncate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperSideBuildReport {
    pub version_one_main_hash: String,
    pub side_main_hash: String,
    pub sealed_seen_trade_count: u64,
    pub sealed_leader_position_count: u64,
    pub sealed_poll_cursor_count: u64,
    pub resumed: bool,
}

/// Final synchronized identity of a side or installed paper main.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperMainSeal {
    pub path: PathBuf,
    pub hash: String,
    pub schema_version: i64,
}

impl MigrationMetadata {
    /// Inspect `PRAGMA user_version` without applying current-schema DDL.
    pub fn schema_version(path: &Path) -> Result<i64, PaperStateError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(PaperStateError::from)
    }

    /// Checkpoint and validate the fixed version-one main, then return its
    /// immutable BLAKE3 identity for deterministic side/backup names.
    pub fn seal_version_one_main(path: &Path) -> Result<String, PaperStateError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        require_schema_version(&connection, 1)?;
        checkpoint_truncate(&connection)?;
        integrity_and_foreign_key_check(&connection)?;
        connection.close().map_err(|(_, error)| error)?;
        remove_checkpoint_sidecars(path)?;
        sync_file(path)?;
        hash_file(path)
    }

    /// Preserve the exact checkpointed v1 generation before migration metadata
    /// or any version-two input can change the fixed main or its logs (#544).
    pub fn preserve_version_one_main(
        fixed_path: &Path,
        backup_path: &Path,
        expected_hash: &str,
    ) -> Result<(), PaperStateError> {
        require_same_device(fixed_path, backup_path)?;
        if backup_path.exists() {
            let actual = hash_file(backup_path)?;
            if actual != expected_hash {
                return Err(PaperStateError::Corrupt(format!(
                    "immutable version-one backup hash mismatch: expected {expected_hash}, got {actual}"
                )));
            }
        } else {
            let actual = hash_file(fixed_path)?;
            if actual != expected_hash {
                return Err(PaperStateError::Corrupt(format!(
                    "version-one main changed before preservation: expected {expected_hash}, got {actual}"
                )));
            }
            copy_file_atomic(fixed_path, backup_path)?;
        }
        let backup = Connection::open_with_flags(backup_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        require_schema_version(&backup, 1)?;
        integrity_and_foreign_key_check(&backup)?;
        backup.close().map_err(|(_, error)| error)?;
        sync_file(backup_path)
    }

    /// Persist the first record exactly once in the fixed version-one main.
    pub fn record_once(path: &Path, record: &MigrationRecord) -> Result<(), PaperStateError> {
        if record.phase != MigrationPhase::BoundaryRecorded || record.activation_tails.is_some() {
            return Err(PaperStateError::MigrationPhaseTransition {
                from: "unrecorded".to_owned(),
                to: record.phase.to_string(),
            });
        }
        verify_side_main_path(record, &record.side_main_path)?;
        let mut connection = open_bootstrap(path)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        // One IMMEDIATE transaction makes read-check-insert atomic across
        // connections: two concurrent first writers serialize on the write
        // lock, and the loser sees the winner's record instead of silently
        // replacing the supposedly immutable boundary (#544 review).
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![MIGRATION_RECORD_KEY],
                |row| row.get(0),
            )
            .optional()?;
        match existing {
            None => {
                let encoded = serde_json::to_string(&StoredRecord::from(record))?;
                transaction.execute(
                    "INSERT INTO meta (key, value) VALUES (?1, ?2)",
                    params![MIGRATION_RECORD_KEY, encoded],
                )?;
                transaction.commit()?;
                finalize_record_write(path, connection)
            }
            Some(encoded) => {
                let stored: MigrationRecord =
                    serde_json::from_str::<StoredRecord>(&encoded)?.try_into()?;
                transaction.commit()?;
                if stored == *record {
                    finalize_record_write(path, connection)
                } else {
                    Err(PaperStateError::MigrationRecordConflict)
                }
            }
        }
    }

    /// Read migration state without applying the normal current-schema check.
    pub fn read(path: &Path) -> Result<Option<MigrationRecord>, PaperStateError> {
        let connection = open_bootstrap(path)?;
        read_record(&connection)
    }

    /// Advance one state-machine edge and synchronize the database main file.
    pub fn advance_phase(
        path: &Path,
        expected: MigrationPhase,
        next: MigrationPhase,
    ) -> Result<MigrationRecord, PaperStateError> {
        if expected.next() != Some(next) {
            return Err(PaperStateError::MigrationPhaseTransition {
                from: expected.to_string(),
                to: next.to_string(),
            });
        }
        // The tails edge is data-bearing and must go through
        // `record_activation_tails`, which binds the tails in the same write —
        // the generic edge would otherwise reach `ActivationTailsRecorded`
        // with `activation_tails == None` (#544 review).
        if next == MigrationPhase::ActivationTailsRecorded {
            return Err(PaperStateError::MigrationPhaseTransition {
                from: expected.to_string(),
                to: format!("{next} (requires record_activation_tails)"),
            });
        }
        let connection = open_bootstrap(path)?;
        let mut record =
            read_record(&connection)?.ok_or(PaperStateError::MigrationRecordMissing)?;
        if record.phase == next {
            write_record(path, connection, &record)?;
            return Ok(record);
        }
        if record.phase != expected {
            return Err(PaperStateError::MigrationPhaseTransition {
                from: record.phase.to_string(),
                to: next.to_string(),
            });
        }
        record.phase = next;
        write_record(path, connection, &record)?;
        Ok(record)
    }

    /// Immutably bind final version-two tails while advancing the matching phase edge.
    pub fn record_activation_tails(
        path: &Path,
        tails: DurableLogBindings,
    ) -> Result<MigrationRecord, PaperStateError> {
        let connection = open_bootstrap(path)?;
        let mut record =
            read_record(&connection)?.ok_or(PaperStateError::MigrationRecordMissing)?;
        if record.phase == MigrationPhase::ActivationTailsRecorded {
            return match record.activation_tails.as_ref() {
                Some(existing) if existing == &tails => {
                    write_record(path, connection, &record)?;
                    Ok(record)
                }
                _ => Err(PaperStateError::MigrationRecordConflict),
            };
        }
        if record.phase != MigrationPhase::SideStateBuilt {
            return Err(PaperStateError::MigrationPhaseTransition {
                from: record.phase.to_string(),
                to: MigrationPhase::ActivationTailsRecorded.to_string(),
            });
        }
        record.activation_tails = Some(tails);
        record.phase = MigrationPhase::ActivationTailsRecorded;
        write_record(path, connection, &record)?;
        Ok(record)
    }

    /// Bring the side main to the fixed main's exact synchronized record. Immutable identity
    /// divergence fails closed; a missing side record is initialized from the authority.
    pub fn mirror_to_side(fixed_path: &Path, side_path: &Path) -> Result<(), PaperStateError> {
        let authority = Self::read(fixed_path)?.ok_or(PaperStateError::MigrationRecordMissing)?;
        verify_side_main_path(&authority, side_path)?;
        let connection = open_bootstrap(side_path)?;
        if let Some(side) = read_record(&connection)?
            && !same_migration_identity(&authority, &side)
        {
            return Err(PaperStateError::MigrationRecordDivergent);
        }
        write_record(side_path, connection, &authority)
    }

    /// Require fixed and side metadata to be present and byte-equivalent before resuming.
    pub fn read_mirrored(
        fixed_path: &Path,
        side_path: &Path,
    ) -> Result<MigrationRecord, PaperStateError> {
        let fixed = Self::read(fixed_path)?.ok_or(PaperStateError::MigrationRecordMissing)?;
        verify_side_main_path(&fixed, side_path)?;
        let side = Self::read(side_path)?.ok_or(PaperStateError::MigrationRecordMissing)?;
        if fixed != side {
            return Err(PaperStateError::MigrationRecordDivergent);
        }
        Ok(fixed)
    }

    /// Build the deterministic v2 side main from a checkpointed v1 main.
    ///
    /// Legacy input dedup, leader positions, and poll cursors are renamed to
    /// `*_v1_sealed`. Fresh v2 tables are created from the canonical schema.
    /// There is deliberately no union view: only a complete v2 source replay,
    /// authority reload, and causal position bracket may populate active state.
    pub fn build_side_main_v2(
        fixed_v1_path: &Path,
        side_v2_path: &Path,
    ) -> Result<PaperSideBuildReport, PaperStateError> {
        if side_v2_path.exists() {
            let connection =
                Connection::open_with_flags(side_v2_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            require_schema_version(&connection, SCHEMA_VERSION)?;
            integrity_and_foreign_key_check(&connection)?;
            let report = side_build_report(&connection, fixed_v1_path, side_v2_path, true)?;
            connection.close().map_err(|(_, error)| error)?;
            return Ok(report);
        }

        require_same_device(fixed_v1_path, side_v2_path)?;
        let fixed = Connection::open_with_flags(fixed_v1_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        fixed.busy_timeout(Duration::from_secs(5))?;
        require_schema_version(&fixed, 1)?;
        checkpoint_truncate(&fixed)?;
        integrity_and_foreign_key_check(&fixed)?;
        fixed.close().map_err(|(_, error)| error)?;
        let version_one_main_hash = hash_file(fixed_v1_path)?;
        std::fs::copy(fixed_v1_path, side_v2_path)?;
        sync_file(side_v2_path)?;

        let mut side =
            Connection::open_with_flags(side_v2_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        side.busy_timeout(Duration::from_secs(5))?;
        require_schema_version(&side, 1)?;
        side.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")?;
        let transaction =
            side.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "ALTER TABLE seen_trades RENAME TO seen_trades_v1_sealed;
             ALTER TABLE leader_positions RENAME TO leader_positions_v1_sealed;
             ALTER TABLE poll_cursors RENAME TO poll_cursors_v1_sealed;",
        )?;
        // Connection-level durability pragmas cannot run inside a transaction.
        // They were applied above; keep the schema replacement itself atomic.
        let schema_ddl = SCHEMA
            .strip_prefix("\nPRAGMA journal_mode = WAL;\nPRAGMA synchronous = NORMAL;\n\n")
            .ok_or_else(|| {
                PaperStateError::Internal(
                    "paper-state schema no longer starts with the expected durability pragmas"
                        .to_owned(),
                )
            })?;
        transaction.execute_batch(schema_ddl)?;
        transaction.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)",
            params![VERSION_ONE_ORIGIN_HASH_KEY, version_one_main_hash],
        )?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
        integrity_and_foreign_key_check(&side)?;
        checkpoint_truncate(&side)?;
        let report =
            side_build_report_with_v1_hash(&side, side_v2_path, version_one_main_hash, false)?;
        side.close().map_err(|(_, error)| error)?;
        remove_checkpoint_sidecars(side_v2_path)?;
        sync_file(side_v2_path)?;
        Ok(report)
    }

    /// Checkpoint, close, integrity/version-check, hash, and synchronize a v2
    /// main file before atomic installation.
    pub fn finalize_side_main_v2(path: &Path) -> Result<PaperMainSeal, PaperStateError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        require_schema_version(&connection, SCHEMA_VERSION)?;
        integrity_and_foreign_key_check(&connection)?;
        checkpoint_truncate(&connection)?;
        connection.close().map_err(|(_, error)| error)?;
        remove_checkpoint_sidecars(path)?;
        sync_file(path)?;
        Ok(PaperMainSeal {
            path: std::fs::canonicalize(path)?,
            hash: hash_file(path)?,
            schema_version: SCHEMA_VERSION,
        })
    }

    /// Require the final activation census and its binary binding before tails
    /// may be frozen or the side main installed.
    pub fn activation_facts_hash(
        path: &Path,
        binary_identity: &str,
    ) -> Result<String, PaperStateError> {
        let connection = open_bootstrap(path)?;
        let (facts_json, stored_hash, stored_binary): (String, String, String) = connection
            .query_row(
                "SELECT facts_json, facts_blake3, binary_identity
                 FROM migration_activation_facts_v2 WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| {
                PaperStateError::Corrupt(format!(
                    "paper migration activation facts are missing: {error}"
                ))
            })?;
        let actual = blake3::hash(facts_json.as_bytes()).to_hex().to_string();
        if actual != stored_hash || stored_binary != binary_identity {
            return Err(PaperStateError::Corrupt(
                "paper migration activation facts hash/binary mismatch".to_owned(),
            ));
        }
        Ok(stored_hash)
    }

    /// Atomically install only the finalized side main. The checkpointed v1
    /// main is moved to `version_one_backup_path`; WAL/SHM files are removed and
    /// never installed or restored.
    pub fn install_side_main_v2(
        fixed_path: &Path,
        side_path: &Path,
        version_one_backup_path: &Path,
        expected_side_hash: &str,
    ) -> Result<PaperMainSeal, PaperStateError> {
        require_same_device(fixed_path, side_path)?;
        require_same_device(fixed_path, version_one_backup_path)?;
        let side = Self::finalize_side_main_v2(side_path)?;
        if side.hash != expected_side_hash {
            return Err(PaperStateError::Corrupt(format!(
                "side-main hash changed: expected {expected_side_hash}, got {}",
                side.hash
            )));
        }
        let side_connection = open_bootstrap(side_path)?;
        let version_one_origin = version_one_origin_hash(&side_connection)?;
        side_connection.close().map_err(|(_, error)| error)?;
        if !version_one_backup_path.is_file() {
            return Err(PaperStateError::Corrupt(format!(
                "immutable version-one backup is missing: {}",
                version_one_backup_path.display()
            )));
        }
        let backup_hash = hash_file(version_one_backup_path)?;
        if backup_hash != version_one_origin {
            return Err(PaperStateError::Corrupt(format!(
                "version-one backup does not match the v2 origin: expected {version_one_origin}, got {backup_hash}"
            )));
        }
        let backup =
            Connection::open_with_flags(version_one_backup_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        require_schema_version(&backup, 1)?;
        integrity_and_foreign_key_check(&backup)?;
        backup.close().map_err(|(_, error)| error)?;
        let fixed = Connection::open_with_flags(fixed_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        fixed.busy_timeout(Duration::from_secs(5))?;
        require_schema_version(&fixed, 1)?;
        checkpoint_truncate(&fixed)?;
        integrity_and_foreign_key_check(&fixed)?;
        fixed.close().map_err(|(_, error)| error)?;
        remove_checkpoint_sidecars(fixed_path)?;
        std::fs::rename(side_path, fixed_path).map_err(map_rename_error)?;
        sync_parent(fixed_path)?;

        let installed = Self::finalize_side_main_v2(fixed_path)?;
        if installed.hash != expected_side_hash {
            return Err(PaperStateError::Corrupt(format!(
                "installed paper main hash changed: expected {expected_side_hash}, got {}",
                installed.hash
            )));
        }
        Ok(installed)
    }

    /// Restore the immutable v1 main only while the phase proves no v2 append
    /// or active-state commit occurred. Log boundaries must still match exactly.
    pub fn rollback_pre_activation(
        fixed_path: &Path,
        version_one_backup_path: &Path,
        failed_side_path: &Path,
        current_logs: &DurableLogBindings,
    ) -> Result<(), PaperStateError> {
        let record = Self::read(fixed_path)?.ok_or(PaperStateError::MigrationRecordMissing)?;
        if record.phase != MigrationPhase::BoundaryRecorded {
            return Err(PaperStateError::MigrationPhaseTransition {
                from: record.phase.to_string(),
                to: "version_one_rollback (roll-forward required)".to_owned(),
            });
        }
        verify_log_bindings(&record.version_one_boundary, current_logs)
            .map_err(|error| PaperStateError::Corrupt(error.to_string()))?;
        require_same_device(fixed_path, version_one_backup_path)?;
        let version_one =
            Connection::open_with_flags(version_one_backup_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        require_schema_version(&version_one, 1)?;
        integrity_and_foreign_key_check(&version_one)?;
        version_one.close().map_err(|(_, error)| error)?;
        if failed_side_path.exists() {
            let failed = Self::finalize_side_main_v2(failed_side_path)?;
            let mut preserved = failed_side_path.as_os_str().to_owned();
            preserved.push(format!(".failed.{}", failed.hash));
            std::fs::rename(failed_side_path, PathBuf::from(preserved))
                .map_err(map_rename_error)?;
        }
        remove_checkpoint_sidecars(fixed_path)?;
        std::fs::rename(version_one_backup_path, fixed_path).map_err(map_rename_error)?;
        remove_checkpoint_sidecars(fixed_path)?;
        sync_file(fixed_path)
    }
}

fn same_migration_identity(left: &MigrationRecord, right: &MigrationRecord) -> bool {
    left.version_one_boundary == right.version_one_boundary
        && left.side_main_path == right.side_main_path
        && left.input_hashes == right.input_hashes
}

fn side_build_report(
    connection: &Connection,
    fixed_v1_path: &Path,
    side_v2_path: &Path,
    resumed: bool,
) -> Result<PaperSideBuildReport, PaperStateError> {
    side_build_report_with_v1_hash(connection, side_v2_path, hash_file(fixed_v1_path)?, resumed)
}

fn side_build_report_with_v1_hash(
    connection: &Connection,
    side_v2_path: &Path,
    version_one_main_hash: String,
    resumed: bool,
) -> Result<PaperSideBuildReport, PaperStateError> {
    let stored_origin = version_one_origin_hash(connection)?;
    if stored_origin != version_one_main_hash {
        return Err(PaperStateError::Corrupt(format!(
            "v2 side main origin hash mismatch: stored {stored_origin}, fixed {version_one_main_hash}"
        )));
    }
    Ok(PaperSideBuildReport {
        version_one_main_hash,
        side_main_hash: hash_file(side_v2_path)?,
        sealed_seen_trade_count: count_rows(connection, "seen_trades_v1_sealed")?,
        sealed_leader_position_count: count_rows(connection, "leader_positions_v1_sealed")?,
        sealed_poll_cursor_count: count_rows(connection, "poll_cursors_v1_sealed")?,
        resumed,
    })
}

fn version_one_origin_hash(connection: &Connection) -> Result<String, PaperStateError> {
    connection
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            params![VERSION_ONE_ORIGIN_HASH_KEY],
            |row| row.get(0),
        )
        .map_err(|error| {
            PaperStateError::Corrupt(format!(
                "v2 side main omitted its version-one origin hash: {error}"
            ))
        })
}

fn count_rows(connection: &Connection, table: &str) -> Result<u64, PaperStateError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = connection.query_row(&sql, [], |row| row.get(0))?;
    u64::try_from(count)
        .map_err(|_| PaperStateError::Corrupt(format!("negative row count for {table}")))
}

fn require_schema_version(connection: &Connection, expected: i64) -> Result<(), PaperStateError> {
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found == expected {
        Ok(())
    } else {
        Err(PaperStateError::SchemaVersionMismatch { found, expected })
    }
}

fn integrity_and_foreign_key_check(connection: &Connection) -> Result<(), PaperStateError> {
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(PaperStateError::Corrupt(format!(
            "SQLite integrity_check failed: {integrity}"
        )));
    }
    let foreign_key_failure: Option<(String, i64)> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?;
    if let Some((table, rowid)) = foreign_key_failure {
        return Err(PaperStateError::Corrupt(format!(
            "SQLite foreign_key_check failed at {table} row {rowid}"
        )));
    }
    Ok(())
}

fn checkpoint_truncate(connection: &Connection) -> Result<(), PaperStateError> {
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if journal_mode.eq_ignore_ascii_case("wal") {
        let (busy, log, checkpointed): (i64, i64, i64) =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        if busy != 0 || log != checkpointed {
            return Err(PaperStateError::MigrationCheckpointIncomplete {
                busy,
                log,
                checkpointed,
            });
        }
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<String, PaperStateError> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn remove_checkpoint_sidecars(path: &Path) -> Result<(), PaperStateError> {
    let wal = sqlite_sidecar(path, "-wal");
    if wal.is_file() && std::fs::metadata(&wal)?.len() != 0 {
        return Err(PaperStateError::Corrupt(format!(
            "non-empty WAL remains after checkpoint: {}",
            wal.display()
        )));
    }
    for sidecar in [wal, sqlite_sidecar(path, "-shm")] {
        match std::fs::remove_file(sidecar) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn sqlite_sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

#[cfg(unix)]
fn require_same_device(left: &Path, right: &Path) -> Result<(), PaperStateError> {
    use std::os::unix::fs::MetadataExt as _;
    let left_device = std::fs::metadata(left)?.dev();
    let right_parent = right
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let right_device = std::fs::metadata(right_parent)?.dev();
    if left_device == right_device {
        Ok(())
    } else {
        Err(PaperStateError::Internal(format!(
            "atomic rename refused across devices: {} is {left_device}, {} is {right_device}",
            left.display(),
            right.display()
        )))
    }
}

#[cfg(not(unix))]
fn require_same_device(_left: &Path, _right: &Path) -> Result<(), PaperStateError> {
    Ok(())
}

fn map_rename_error(error: std::io::Error) -> PaperStateError {
    if error.raw_os_error() == Some(18) {
        PaperStateError::Internal("atomic rename refused across devices".to_owned())
    } else {
        PaperStateError::Synchronization(error)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecord {
    version_one_boundary: StoredBindings,
    activation_tails: Option<StoredBindings>,
    phase: MigrationPhase,
    side_main_path: PathBuf,
    input_hashes: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredBindings {
    source: StoredTail,
    paper: StoredTail,
    live_journal: StoredTail,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTail {
    path: PathBuf,
    physical_tail: u64,
    last_sequence: Option<u64>,
    last_hash: String,
}

impl From<&MigrationRecord> for StoredRecord {
    fn from(record: &MigrationRecord) -> Self {
        Self {
            version_one_boundary: StoredBindings::from(&record.version_one_boundary),
            activation_tails: record.activation_tails.as_ref().map(StoredBindings::from),
            phase: record.phase,
            side_main_path: record.side_main_path.clone(),
            input_hashes: record.input_hashes.clone(),
        }
    }
}

impl TryFrom<StoredRecord> for MigrationRecord {
    type Error = PaperStateError;

    fn try_from(record: StoredRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            version_one_boundary: record.version_one_boundary.try_into()?,
            activation_tails: record.activation_tails.map(TryInto::try_into).transpose()?,
            phase: record.phase,
            side_main_path: record.side_main_path,
            input_hashes: record.input_hashes,
        })
    }
}

impl From<&DurableLogBindings> for StoredBindings {
    fn from(bindings: &DurableLogBindings) -> Self {
        Self {
            source: StoredTail::from(&bindings.source),
            paper: StoredTail::from(&bindings.paper),
            live_journal: StoredTail::from(&bindings.live_journal),
        }
    }
}

impl TryFrom<StoredBindings> for DurableLogBindings {
    type Error = PaperStateError;

    fn try_from(bindings: StoredBindings) -> Result<Self, Self::Error> {
        Ok(Self {
            source: bindings.source.try_into()?,
            paper: bindings.paper.try_into()?,
            live_journal: bindings.live_journal.try_into()?,
        })
    }
}

impl From<&LogTailBinding> for StoredTail {
    fn from(tail: &LogTailBinding) -> Self {
        Self {
            path: tail.path.clone(),
            physical_tail: tail.physical_tail,
            last_sequence: tail.last_sequence.map(|sequence| sequence.0),
            last_hash: tail.last_hash.to_hex().to_string(),
        }
    }
}

impl TryFrom<StoredTail> for LogTailBinding {
    type Error = PaperStateError;

    fn try_from(tail: StoredTail) -> Result<Self, Self::Error> {
        let last_hash = Hash::from_hex(&tail.last_hash).map_err(|error| {
            PaperStateError::Corrupt(format!("invalid migration log hash: {error}"))
        })?;
        Ok(Self {
            path: tail.path,
            physical_tail: tail.physical_tail,
            last_sequence: tail.last_sequence.map(EventSeq),
            last_hash,
        })
    }
}

fn open_bootstrap(path: &Path) -> Result<Connection, PaperStateError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let has_meta = connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'meta'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !has_meta {
        return Err(PaperStateError::MigrationMetaMissing);
    }
    Ok(connection)
}

fn read_record(connection: &Connection) -> Result<Option<MigrationRecord>, PaperStateError> {
    let encoded: Option<String> = connection
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            params![MIGRATION_RECORD_KEY],
            |row| row.get(0),
        )
        .optional()?;
    encoded
        .map(|value| serde_json::from_str::<StoredRecord>(&value)?.try_into())
        .transpose()
}

fn write_record(
    path: &Path,
    mut connection: Connection,
    record: &MigrationRecord,
) -> Result<(), PaperStateError> {
    let encoded = serde_json::to_string(&StoredRecord::from(record))?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![MIGRATION_RECORD_KEY, encoded],
    )?;
    transaction.commit()?;
    finalize_record_write(path, connection)
}

/// Checkpoint-truncate the WAL, close, and fsync the main file plus parent
/// directory — the shared durable tail of every migration-record write.
fn finalize_record_write(path: &Path, connection: Connection) -> Result<(), PaperStateError> {
    let (busy, log, checkpointed): (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if busy != 0 || log != checkpointed {
        return Err(PaperStateError::MigrationCheckpointIncomplete {
            busy,
            log,
            checkpointed,
        });
    }
    connection.close().map_err(|(_, error)| error)?;
    sync_file(path)
}

fn sync_file(path: &Path) -> Result<(), PaperStateError> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    file.sync_all()?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<(), PaperStateError> {
    let parent = path.parent().ok_or_else(|| {
        PaperStateError::Internal(format!(
            "migration database has no parent: {}",
            path.display()
        ))
    })?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn copy_file_atomic(source: &Path, target: &Path) -> Result<(), PaperStateError> {
    let mut pending = target.as_os_str().to_owned();
    pending.push(".pending");
    let pending = PathBuf::from(pending);
    match std::fs::remove_file(&pending) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::copy(source, &pending)?;
    sync_file(&pending)?;
    std::fs::rename(&pending, target).map_err(map_rename_error)?;
    sync_parent(target)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::PaperStateDb;

    fn tail(path: &Path, byte: u8) -> LogTailBinding {
        LogTailBinding {
            path: path.to_owned(),
            physical_tail: u64::from(byte),
            last_sequence: Some(EventSeq(u64::from(byte))),
            last_hash: Hash::from_bytes([byte; 32]),
        }
    }

    fn record(dir: &Path) -> MigrationRecord {
        MigrationRecord {
            version_one_boundary: DurableLogBindings {
                source: tail(&dir.join("source.log"), 1),
                paper: tail(&dir.join("paper.log"), 2),
                live_journal: tail(&dir.join("live_journal.log"), 3),
            },
            activation_tails: None,
            phase: MigrationPhase::BoundaryRecorded,
            side_main_path: dir.join("paper_state.v2.abc123.db"),
            input_hashes: BTreeMap::from([("sidecar".to_owned(), "abc123".to_owned())]),
        }
    }

    #[test]
    fn bootstrap_reads_schema_mismatch_and_record_once_is_immutable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixed.db");
        drop(PaperStateDb::open(&path).unwrap());
        drop(PaperStateDb::open(&record(dir.path()).side_main_path).unwrap());
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "user_version", crate::SCHEMA_VERSION + 1)
            .unwrap();
        drop(connection);

        let first = record(dir.path());
        MigrationMetadata::record_once(&path, &first).unwrap();
        assert_eq!(MigrationMetadata::read(&path).unwrap(), Some(first.clone()));
        assert!(matches!(
            PaperStateDb::open(&path),
            Err(PaperStateError::SchemaVersionMismatch { found: 3, .. })
        ));
        let mut different = first;
        different
            .input_hashes
            .insert("other".into(), "def456".into());
        assert!(matches!(
            MigrationMetadata::record_once(&path, &different),
            Err(PaperStateError::MigrationRecordConflict)
        ));
    }

    #[test]
    fn mirrored_phase_updates_are_exact_and_resumable() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        let side = dir.path().join("paper_state.v2.abc123.db");
        drop(PaperStateDb::open(&fixed).unwrap());
        drop(PaperStateDb::open(&side).unwrap());
        MigrationMetadata::record_once(&fixed, &record(dir.path())).unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();
        assert!(MigrationMetadata::read_mirrored(&fixed, &side).is_ok());

        MigrationMetadata::advance_phase(
            &fixed,
            MigrationPhase::BoundaryRecorded,
            MigrationPhase::VersionTwoInputsAppending,
        )
        .unwrap();
        assert!(matches!(
            MigrationMetadata::read_mirrored(&fixed, &side),
            Err(PaperStateError::MigrationRecordDivergent)
        ));
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();
        assert_eq!(
            MigrationMetadata::read_mirrored(&fixed, &side)
                .unwrap()
                .phase,
            MigrationPhase::VersionTwoInputsAppending
        );
    }

    #[test]
    fn every_binding_field_mismatch_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let stored = record(dir.path()).version_one_boundary;
        let mut current = stored.clone();
        current.paper.physical_tail += 1;
        assert_eq!(
            verify_log_bindings(&stored, &current).unwrap_err(),
            BoundaryMismatch {
                log: DurableLogName::Paper,
                field: BoundaryField::PhysicalTail,
            }
        );
        current = stored.clone();
        current.live_journal.path.push("different");
        assert_eq!(
            verify_log_bindings(&stored, &current).unwrap_err().field,
            BoundaryField::Path
        );
        current = stored.clone();
        current.source.last_sequence = Some(EventSeq(99));
        assert_eq!(
            verify_log_bindings(&stored, &current).unwrap_err().field,
            BoundaryField::LastSequence
        );
        current = stored.clone();
        current.paper.last_hash = Hash::from_bytes([9; 32]);
        assert_eq!(
            verify_log_bindings(&stored, &current).unwrap_err().field,
            BoundaryField::LastHash
        );
    }

    #[test]
    fn activation_tails_record_once_and_phase_advances_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        drop(PaperStateDb::open(&fixed).unwrap());
        drop(PaperStateDb::open(&record(dir.path()).side_main_path).unwrap());
        MigrationMetadata::record_once(&fixed, &record(dir.path())).unwrap();
        for (expected, next) in [
            (
                MigrationPhase::BoundaryRecorded,
                MigrationPhase::VersionTwoInputsAppending,
            ),
            (
                MigrationPhase::VersionTwoInputsAppending,
                MigrationPhase::SideStateBuilt,
            ),
        ] {
            MigrationMetadata::advance_phase(&fixed, expected, next).unwrap();
            assert_eq!(
                MigrationMetadata::advance_phase(&fixed, expected, next)
                    .unwrap()
                    .phase,
                next
            );
        }
        let tails = record(dir.path()).version_one_boundary;
        let recorded = MigrationMetadata::record_activation_tails(&fixed, tails.clone()).unwrap();
        assert_eq!(recorded.activation_tails, Some(tails.clone()));
        assert_eq!(
            MigrationMetadata::record_activation_tails(&fixed, tails)
                .unwrap()
                .phase,
            MigrationPhase::ActivationTailsRecorded
        );
        let mut changed = record(dir.path()).version_one_boundary;
        changed.source.physical_tail += 1;
        assert!(matches!(
            MigrationMetadata::record_activation_tails(&fixed, changed),
            Err(PaperStateError::MigrationRecordConflict)
        ));
    }

    #[test]
    fn restart_before_and_after_every_record_update_converges_to_one_installed_record() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        let side = dir.path().join("paper_state.v2.abc123.db");
        drop(PaperStateDb::open(&fixed).unwrap());
        drop(PaperStateDb::open(&side).unwrap());
        let initial = record(dir.path());

        // A restart before the first update observes exactly the missing-record state.
        assert_eq!(MigrationMetadata::read(&fixed).unwrap(), None);
        MigrationMetadata::record_once(&fixed, &initial).unwrap();
        MigrationMetadata::record_once(&fixed, &initial).unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();
        assert_eq!(
            MigrationMetadata::read_mirrored(&fixed, &side)
                .unwrap()
                .phase,
            MigrationPhase::BoundaryRecorded
        );

        for (expected, next) in [
            (
                MigrationPhase::BoundaryRecorded,
                MigrationPhase::VersionTwoInputsAppending,
            ),
            (
                MigrationPhase::VersionTwoInputsAppending,
                MigrationPhase::SideStateBuilt,
            ),
        ] {
            MigrationMetadata::advance_phase(&fixed, expected, next).unwrap();
            assert!(matches!(
                MigrationMetadata::read_mirrored(&fixed, &side),
                Err(PaperStateError::MigrationRecordDivergent)
            ));
            MigrationMetadata::advance_phase(&fixed, expected, next).unwrap();
            MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();
            assert_eq!(
                MigrationMetadata::read_mirrored(&fixed, &side)
                    .unwrap()
                    .phase,
                next
            );
        }

        let activation = initial.version_one_boundary.clone();
        MigrationMetadata::record_activation_tails(&fixed, activation.clone()).unwrap();
        assert!(matches!(
            MigrationMetadata::read_mirrored(&fixed, &side),
            Err(PaperStateError::MigrationRecordDivergent)
        ));
        MigrationMetadata::record_activation_tails(&fixed, activation.clone()).unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();

        MigrationMetadata::advance_phase(
            &fixed,
            MigrationPhase::ActivationTailsRecorded,
            MigrationPhase::Installed,
        )
        .unwrap();
        MigrationMetadata::advance_phase(
            &fixed,
            MigrationPhase::ActivationTailsRecorded,
            MigrationPhase::Installed,
        )
        .unwrap();
        MigrationMetadata::mirror_to_side(&fixed, &side).unwrap();
        let installed = MigrationMetadata::read_mirrored(&fixed, &side).unwrap();
        assert_eq!(installed.phase, MigrationPhase::Installed);
        assert_eq!(installed.activation_tails, Some(activation));
        assert_eq!(installed.version_one_boundary, initial.version_one_boundary);
    }

    #[test]
    fn mirror_rejects_a_side_main_other_than_the_recorded_path() {
        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("fixed.db");
        let recorded_side = dir.path().join("paper_state.v2.abc123.db");
        let wrong_side = dir.path().join("paper_state.v2.wrong.db");
        drop(PaperStateDb::open(&fixed).unwrap());
        drop(PaperStateDb::open(&recorded_side).unwrap());
        drop(PaperStateDb::open(&wrong_side).unwrap());
        MigrationMetadata::record_once(&fixed, &record(dir.path())).unwrap();

        assert!(matches!(
            MigrationMetadata::mirror_to_side(&fixed, &wrong_side),
            Err(PaperStateError::MigrationSidePathMismatch { .. })
        ));
        assert_eq!(MigrationMetadata::read(&wrong_side).unwrap(), None);
    }

    #[test]
    fn advance_phase_rejects_the_data_bearing_tails_edge() {
        // SideStateBuilt -> ActivationTailsRecorded must go through
        // record_activation_tails, or the record reaches that phase with
        // activation_tails == None (#544 review).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixed.db");
        drop(PaperStateDb::open(&path).unwrap());
        drop(PaperStateDb::open(&record(dir.path()).side_main_path).unwrap());
        let first = record(dir.path());
        MigrationMetadata::record_once(&path, &first).unwrap();
        MigrationMetadata::advance_phase(
            &path,
            MigrationPhase::BoundaryRecorded,
            MigrationPhase::VersionTwoInputsAppending,
        )
        .unwrap();
        MigrationMetadata::advance_phase(
            &path,
            MigrationPhase::VersionTwoInputsAppending,
            MigrationPhase::SideStateBuilt,
        )
        .unwrap();
        let refused = MigrationMetadata::advance_phase(
            &path,
            MigrationPhase::SideStateBuilt,
            MigrationPhase::ActivationTailsRecorded,
        );
        assert!(matches!(
            refused,
            Err(PaperStateError::MigrationPhaseTransition { .. })
        ));
        let stored = MigrationMetadata::read(&path).unwrap().unwrap();
        assert_eq!(stored.phase, MigrationPhase::SideStateBuilt);
        assert!(stored.activation_tails.is_none());
    }

    #[test]
    fn concurrent_record_once_writers_cannot_replace_the_boundary() {
        // Two first writers race; exactly one record survives and the loser sees
        // a typed conflict — never a silent overwrite (#544 review).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixed.db");
        drop(PaperStateDb::open(&path).unwrap());
        drop(PaperStateDb::open(&record(dir.path()).side_main_path).unwrap());
        let a = record(dir.path());
        let mut b = record(dir.path());
        b.input_hashes
            .insert("sidecar".to_owned(), "different".to_owned());

        let path_a = path.clone();
        let path_b = path.clone();
        let (ra, rb) = std::thread::scope(|scope| {
            let ta = scope.spawn(|| MigrationMetadata::record_once(&path_a, &a));
            let tb = scope.spawn(|| MigrationMetadata::record_once(&path_b, &b));
            (ta.join().unwrap(), tb.join().unwrap())
        });
        let winners = [ra.is_ok(), rb.is_ok()].iter().filter(|ok| **ok).count();
        assert_eq!(winners, 1, "exactly one writer must win: {ra:?} / {rb:?}");
        let stored = MigrationMetadata::read(&path).unwrap().unwrap();
        assert!(stored == a || stored == b);
    }

    #[test]
    fn v1_fixture_builds_sealed_v2_and_old_insert_fails_structurally() {
        const V1_SCHEMA: &str = include_str!("../tests/fixtures/paper_state_v1.sql");
        const OLD_INSERT: &str = "INSERT OR IGNORE INTO seen_trades (source_trade_id) VALUES (?1)";

        let dir = tempfile::tempdir().unwrap();
        let fixed = dir.path().join("paper_state.db");
        let side = dir.path().join("paper_state.v2.fixture.db");
        let connection = Connection::open(&fixed).unwrap();
        connection.execute_batch(V1_SCHEMA).unwrap();
        // The old binary opens version one and its exact mandatory insert works.
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        connection.execute(OLD_INSERT, params!["0xsecond"]).unwrap();
        drop(connection);

        let built = MigrationMetadata::build_side_main_v2(&fixed, &side).unwrap();
        assert_eq!(built.sealed_seen_trade_count, 2);
        assert_eq!(built.sealed_leader_position_count, 1);
        assert_eq!(built.sealed_poll_cursor_count, 1);
        assert!(!built.resumed);
        assert!(
            MigrationMetadata::build_side_main_v2(&fixed, &side)
                .unwrap()
                .resumed
        );

        let v2 = Connection::open(&side).unwrap();
        assert_eq!(
            v2.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        // The exact v1 fixture DDL cannot be applied to the v2 file because the
        // mandatory `seen_trades` owner is now a view. A real old binary also
        // refuses at its user_version check. Even if both checks were bypassed,
        // its exact mandatory insert is rejected by the v2 trigger.
        let ddl_error = v2.execute_batch(V1_SCHEMA);
        assert!(matches!(
            ddl_error,
            Err(rusqlite::Error::SqlInputError { msg, .. })
                if msg.contains("seen_trades already exists")
        ));
        let error = v2.execute(OLD_INSERT, params!["0xold-binary-write"]);
        assert!(matches!(
            error,
            Err(rusqlite::Error::SqliteFailure(_, Some(message)))
                if message.contains("v2 seen_trades requires identity_version")
        ));
        let active_seen: i64 = v2
            .query_row("SELECT COUNT(*) FROM seen_trades", [], |row| row.get(0))
            .unwrap();
        let sealed_seen: i64 = v2
            .query_row("SELECT COUNT(*) FROM seen_trades_v1_sealed", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(active_seen, 0);
        assert_eq!(sealed_seen, 2);
        drop(v2);
        assert!(PaperStateDb::open(&side).is_ok());
    }
}
