#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use pe_execution_core::LiveJournal;
use pe_paper_state::{
    LEGACY_EXACT_MIGRATION_VERSION, MigrationMetadata, MigrationPhase, PaperStateDb, SCHEMA_VERSION,
};
use pe_service::paper_migration::{PaperMigrationBoot, PaperMigrationPaths};
use rusqlite::Connection;
use time::OffsetDateTime;

const V1_SCHEMA: &str = include_str!("../../paper-state/tests/fixtures/paper_state_v1.sql");
const V2_SCHEMA: &str = include_str!("../../paper-state/tests/fixtures/paper_state_schema_v2.sql");

fn append(path: &std::path::Path, payload: &[u8]) {
    let timestamp = OffsetDateTime::from_unix_timestamp(1_788_192_000).unwrap();
    let mut writer = Writer::open(path).unwrap();
    writer
        .append(EnvelopeIn {
            source_id: SourceId("migration-fixture".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .unwrap();
    writer.sync().unwrap();
}

fn migration_fixture() -> (tempfile::TempDir, PaperMigrationPaths) {
    let dir = tempfile::tempdir().unwrap();
    let fixed = dir.path().join("paper_state.db");
    let source = dir.path().join("source.log");
    let paper = dir.path().join("paper.log");
    let journal = dir.path().join("live_journal.log");
    let history = dir.path().join("wallet_market_history.json");
    Connection::open(&fixed)
        .unwrap()
        .execute_batch(V1_SCHEMA)
        .unwrap();
    append(&source, br#"{"source":1}"#);
    append(&paper, br#"{"paper":1}"#);
    drop(LiveJournal::open(&journal).unwrap());
    std::fs::write(
        &history,
        br#"{"wallets":[{"wallet":"0x1111111111111111111111111111111111111111","markets":["condition-1"]}]}"#,
    )
    .unwrap();
    (
        dir,
        PaperMigrationPaths {
            fixed_main: fixed,
            source_log: source,
            paper_log: paper,
            live_journal: journal,
            legacy_history: history,
            binary_identity: "fixture-build".to_owned(),
        },
    )
}

#[test]
fn paper_migration_resumes_roll_forward_and_installs_only_the_v2_main() {
    let (_dir, paths) = migration_fixture();
    let fixed = paths.fixed_main.clone();
    let source = paths.source_log.clone();

    let first = PaperMigrationBoot::prepare(paths.clone(), 1_788_192_000).unwrap();
    assert_eq!(
        first.record.phase,
        MigrationPhase::VersionTwoInputsAppending
    );
    let side = first.active_main.clone();
    let v1_hash = first
        .record
        .input_hashes
        .get("version_one_main_blake3")
        .unwrap();
    let v1_backup = fixed.parent().unwrap().join(format!(
        "{}.v1.{v1_hash}.db",
        fixed.file_name().unwrap().to_str().unwrap()
    ));
    assert_eq!(
        blake3::hash(&std::fs::read(&v1_backup).unwrap())
            .to_hex()
            .as_str(),
        v1_hash
    );
    let mut interrupted_backup = v1_backup.as_os_str().to_owned();
    interrupted_backup.push(".pending");
    std::fs::write(
        std::path::PathBuf::from(interrupted_backup),
        b"partial-copy",
    )
    .unwrap();
    drop(first);

    let resumed = PaperMigrationBoot::prepare(paths.clone(), 1_788_192_001).unwrap();
    assert_eq!(resumed.active_main, side);
    append(&source, br#"{"source":2}"#);
    let side_state = PaperStateDb::open(&resumed.active_main).unwrap();
    side_state
        .record_migration_activation_facts(
            &serde_json::json!({"fixture": "complete"}),
            &paths.binary_identity,
        )
        .unwrap();
    drop(side_state);
    let seal = resumed.session.unwrap().finish().unwrap();
    assert_eq!(seal.schema_version, SCHEMA_VERSION);
    assert_eq!(
        MigrationMetadata::schema_version(&fixed).unwrap(),
        SCHEMA_VERSION
    );
    let installed = MigrationMetadata::read(&fixed).unwrap().unwrap();
    assert_eq!(installed.phase, MigrationPhase::Installed);
    assert!(installed.activation_tails.is_some());
    assert!(!side.exists());
    assert!(PaperStateDb::open(&fixed).is_ok());

    append(&source, br#"{"source":3}"#);
    let ordinary_restart = PaperMigrationBoot::prepare(paths, 1_788_192_002).unwrap();
    assert!(ordinary_restart.session.is_none());
    assert_eq!(ordinary_restart.record.phase, MigrationPhase::Installed);
}

/// PASS: an installed schema-two main preserves its text migration record during the boot
/// precheck, upgrades only on writable open, and an unrelated schema version remains rejected.
#[test]
fn installed_schema_two_main_boots_before_writable_upgrade() {
    let (dir, paths) = migration_fixture();
    let migration = PaperMigrationBoot::prepare(paths.clone(), 1_788_192_000).unwrap();
    append(&paths.source_log, br#"{"source":2}"#);
    let side_state = PaperStateDb::open(&migration.active_main).unwrap();
    side_state
        .record_migration_activation_facts(
            &serde_json::json!({"fixture": "complete"}),
            &paths.binary_identity,
        )
        .unwrap();
    drop(side_state);
    migration.session.unwrap().finish().unwrap();

    let installed_record = MigrationMetadata::read(&paths.fixed_main).unwrap().unwrap();
    assert_eq!(installed_record.phase, MigrationPhase::Installed);

    let v2_main = dir.path().join("paper_state_schema_v2.db");
    let connection = Connection::open(&v2_main).unwrap();
    connection.execute_batch(V2_SCHEMA).unwrap();
    connection
        .pragma_update(None, "user_version", LEGACY_EXACT_MIGRATION_VERSION)
        .unwrap();
    connection
        .execute(
            "ATTACH DATABASE ?1 AS installed",
            [paths.fixed_main.to_str().unwrap()],
        )
        .unwrap();
    connection
        .execute_batch(
            "INSERT INTO meta (key, value)
                 SELECT key, value FROM installed.meta;
             INSERT INTO migration_activation_facts_v2
                 (singleton, facts_json, facts_blake3, binary_identity)
                 SELECT singleton, facts_json, facts_blake3, binary_identity
                 FROM installed.migration_activation_facts_v2;",
        )
        .unwrap();
    let copied_meta_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM meta", [], |row| row.get(0))
        .unwrap();
    let installed_meta_rows: i64 = connection
        .query_row("SELECT COUNT(*) FROM installed.meta", [], |row| row.get(0))
        .unwrap();
    assert_eq!(copied_meta_rows, installed_meta_rows);
    let activation_fact_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM migration_activation_facts_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(activation_fact_rows, 1);
    let record_storage_class: String = connection
        .query_row(
            "SELECT typeof(value) FROM meta WHERE key = 'trustworthy_v2_migration_record'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(record_storage_class, "text");
    connection
        .execute_batch("DETACH DATABASE installed")
        .unwrap();
    drop(connection);

    assert_eq!(
        MigrationMetadata::read(&v2_main).unwrap(),
        Some(installed_record.clone())
    );

    let unknown_main = dir.path().join("paper_state_unknown.db");
    std::fs::copy(&v2_main, &unknown_main).unwrap();
    let unknown_connection = Connection::open(&unknown_main).unwrap();
    unknown_connection
        .pragma_update(None, "user_version", 999)
        .unwrap();
    drop(unknown_connection);

    let mut v2_paths = paths.clone();
    v2_paths.fixed_main = v2_main.clone();
    let prepared = PaperMigrationBoot::prepare(v2_paths, 1_788_192_001).unwrap();
    assert_eq!(prepared.active_main, v2_main);
    assert_eq!(prepared.record, installed_record);

    drop(PaperStateDb::open(&v2_main).unwrap());
    assert_eq!(
        MigrationMetadata::schema_version(&v2_main).unwrap(),
        SCHEMA_VERSION
    );
    assert_eq!(
        MigrationMetadata::read(&v2_main).unwrap(),
        Some(installed_record)
    );

    let mut unknown_paths = paths;
    unknown_paths.fixed_main = unknown_main;
    assert!(PaperMigrationBoot::prepare(unknown_paths, 1_788_192_002).is_err());
}

#[test]
fn paper_migration_resume_refuses_truncated_log_and_changed_identities() {
    for defect in ["truncated_log", "legacy_history", "binary", "v1_backup"] {
        let (_dir, mut paths) = migration_fixture();
        let first = PaperMigrationBoot::prepare(paths.clone(), 1_788_192_000).unwrap();
        let v1_hash = first
            .record
            .input_hashes
            .get("version_one_main_blake3")
            .unwrap();
        let v1_backup = paths.fixed_main.parent().unwrap().join(format!(
            "{}.v1.{v1_hash}.db",
            paths.fixed_main.file_name().unwrap().to_str().unwrap()
        ));
        drop(first);
        match defect {
            "truncated_log" => {
                let length = paths.source_log.metadata().unwrap().len();
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&paths.source_log)
                    .unwrap()
                    .set_len(length - 1)
                    .unwrap();
            }
            "legacy_history" => {
                std::fs::write(&paths.legacy_history, br#"{"wallets":[]}"#).unwrap();
            }
            "binary" => paths.binary_identity = "different-build".to_owned(),
            "v1_backup" => std::fs::write(v1_backup, b"tampered").unwrap(),
            _ => unreachable!(),
        }
        let error = format!(
            "{:#}",
            PaperMigrationBoot::prepare(paths, 1_788_192_001).unwrap_err()
        );
        match defect {
            "truncated_log" => assert!(error.contains("source-log prefix"), "{error}"),
            "legacy_history" => assert!(error.contains("history hash changed"), "{error}"),
            "binary" => assert!(error.contains("binary identity changed"), "{error}"),
            "v1_backup" => assert!(error.contains("backup hash mismatch"), "{error}"),
            _ => unreachable!(),
        }
    }
}
