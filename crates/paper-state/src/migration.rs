//! Machine-owned schema-v1-to-v2 migration metadata and log-boundary verification (#544).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

use blake3::Hash;
use pe_core_types::EventSeq;
use pe_event_log::LogTailBinding;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};

use crate::PaperStateError;

const MIGRATION_RECORD_KEY: &str = "trustworthy_v2_migration_record";

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

impl MigrationMetadata {
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
}

fn same_migration_identity(left: &MigrationRecord, right: &MigrationRecord) -> bool {
    left.version_one_boundary == right.version_one_boundary
        && left.side_main_path == right.side_main_path
        && left.input_hashes == right.input_hashes
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
}
