#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use pe_execution_core::LiveJournal;
use pe_paper_state::{MigrationMetadata, MigrationPhase, PaperStateDb, SCHEMA_VERSION};
use pe_service::paper_migration::{PaperMigrationBoot, PaperMigrationPaths};
use rusqlite::Connection;
use time::OffsetDateTime;

const V1_SCHEMA: &str = include_str!("../../paper-state/tests/fixtures/paper_state_v1.sql");

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
