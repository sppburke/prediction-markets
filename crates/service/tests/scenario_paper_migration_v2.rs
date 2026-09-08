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

/// Drive the fixture through the one-time migration to an installed main.
fn install_generation(paths: &PaperMigrationPaths) {
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
    assert_eq!(
        MigrationMetadata::read(&paths.fixed_main)
            .unwrap()
            .unwrap()
            .phase,
        MigrationPhase::Installed
    );
}

/// Copy the five generation files into a fresh directory (the rehearsal's private copy) and
/// return that directory's paths.
fn copied_generation(paths: &PaperMigrationPaths) -> (tempfile::TempDir, PaperMigrationPaths) {
    Connection::open(&paths.fixed_main)
        .unwrap()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let copy = |from: &std::path::Path| {
        let to = dir.path().join(from.file_name().unwrap());
        std::fs::copy(from, &to).unwrap();
        to
    };
    let copied = PaperMigrationPaths {
        fixed_main: copy(&paths.fixed_main),
        source_log: copy(&paths.source_log),
        paper_log: copy(&paths.paper_log),
        live_journal: copy(&paths.live_journal),
        legacy_history: copy(&paths.legacy_history),
        binary_identity: paths.binary_identity.clone(),
    };
    (dir, copied)
}

fn canonical(path: &std::path::Path) -> std::path::PathBuf {
    std::fs::canonicalize(path).unwrap()
}

/// PASS: a copied installed generation is refused by the boot pre-check until its recorded log
/// paths are updated; the update rewrites only the three activation-tail paths, is idempotent,
/// works on a schema-two copy before the writable upgrade, and leaves the original generation
/// byte-identical and bootable (#570).
#[test]
fn copied_installed_generation_boots_after_updating_its_recorded_log_paths() {
    let (_dir, paths) = migration_fixture();
    install_generation(&paths);
    let original_record = MigrationMetadata::read(&paths.fixed_main).unwrap().unwrap();
    let original_bytes = std::fs::read(&paths.fixed_main).unwrap();

    let (_copy_dir, copied) = copied_generation(&paths);
    let refused = PaperMigrationBoot::prepare(copied.clone(), 1_788_192_003).unwrap_err();
    assert!(
        format!("{refused:#}")
            .contains("configured source log no longer matches the migration record"),
        "{refused:#}"
    );

    assert!(pe_service::paper_migration::update_installed_log_paths(&copied).unwrap());
    let booted = PaperMigrationBoot::prepare(copied.clone(), 1_788_192_004).unwrap();
    assert!(booted.session.is_none());
    assert_eq!(booted.record.phase, MigrationPhase::Installed);
    let mut expected_tails = original_record.activation_tails.clone().unwrap();
    expected_tails.source.path = canonical(&copied.source_log);
    expected_tails.paper.path = canonical(&copied.paper_log);
    expected_tails.live_journal.path = canonical(&copied.live_journal);
    assert_eq!(booted.record.activation_tails, Some(expected_tails));
    assert_eq!(
        booted.record.version_one_boundary,
        original_record.version_one_boundary
    );
    assert_eq!(booted.record.side_main_path, original_record.side_main_path);
    assert_eq!(booted.record.input_hashes, original_record.input_hashes);
    assert!(!pe_service::paper_migration::update_installed_log_paths(&copied).unwrap());

    // The production case: the copy is still at the exact-migration schema version. The update
    // and the pre-check leave the version alone; only the writable open upgrades it.
    let (_v2_dir, v2_copy) = copied_generation(&paths);
    Connection::open(&v2_copy.fixed_main)
        .unwrap()
        .pragma_update(None, "user_version", LEGACY_EXACT_MIGRATION_VERSION)
        .unwrap();
    assert!(pe_service::paper_migration::update_installed_log_paths(&v2_copy).unwrap());
    let v2_booted = PaperMigrationBoot::prepare(v2_copy.clone(), 1_788_192_005).unwrap();
    assert_eq!(v2_booted.record.phase, MigrationPhase::Installed);
    assert_eq!(
        MigrationMetadata::schema_version(&v2_copy.fixed_main).unwrap(),
        LEGACY_EXACT_MIGRATION_VERSION
    );
    drop(PaperStateDb::open(&v2_copy.fixed_main).unwrap());
    assert_eq!(
        MigrationMetadata::schema_version(&v2_copy.fixed_main).unwrap(),
        SCHEMA_VERSION
    );

    assert_eq!(
        MigrationMetadata::read(&paths.fixed_main).unwrap().unwrap(),
        original_record
    );
    assert_eq!(std::fs::read(&paths.fixed_main).unwrap(), original_bytes);
    assert!(PaperMigrationBoot::prepare(paths, 1_788_192_006).is_ok());
}

/// PASS: the update refuses to rebind the original main to alternate logs, refuses a copy whose
/// recorded prefix is mutated, truncated, or whose live journal carries invalid native content,
/// and refuses an unknown schema version; a valid suffix appended after the recorded prefix is
/// accepted (#570).
#[test]
fn updating_recorded_log_paths_refuses_the_origin_main_and_every_prefix_mismatch() {
    let (_dir, paths) = migration_fixture();
    install_generation(&paths);
    let original_record = MigrationMetadata::read(&paths.fixed_main).unwrap().unwrap();
    let original_bytes = std::fs::read(&paths.fixed_main).unwrap();
    let recorded = original_record.activation_tails.clone().unwrap();

    // The production shape: the original main with its own recorded logs is refused, not a no-op.
    let own_paths = pe_service::paper_migration::update_installed_log_paths(&paths).unwrap_err();
    assert!(
        format!("{own_paths:#}").contains("origin directory"),
        "{own_paths:#}"
    );
    assert_eq!(std::fs::read(&paths.fixed_main).unwrap(), original_bytes);

    let (_alternate_dir, alternate) = copied_generation(&paths);
    let rebind_production = PaperMigrationPaths {
        fixed_main: paths.fixed_main.clone(),
        source_log: alternate.source_log.clone(),
        paper_log: alternate.paper_log.clone(),
        live_journal: alternate.live_journal.clone(),
        legacy_history: paths.legacy_history.clone(),
        binary_identity: paths.binary_identity.clone(),
    };
    let refused =
        pe_service::paper_migration::update_installed_log_paths(&rebind_production).unwrap_err();
    assert!(
        format!("{refused:#}").contains("origin directory"),
        "{refused:#}"
    );
    assert_eq!(std::fs::read(&paths.fixed_main).unwrap(), original_bytes);

    let (_suffix_dir, with_suffix) = copied_generation(&paths);
    append(&with_suffix.source_log, br#"{"source":9}"#);
    assert!(pe_service::paper_migration::update_installed_log_paths(&with_suffix).unwrap());
    assert!(PaperMigrationBoot::prepare(with_suffix.clone(), 1_788_192_007).is_ok());
    // A rerun on an already-updated copy still verifies the logs: a truncated source log is
    // refused instead of reported as unchanged.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&with_suffix.source_log)
        .unwrap()
        .set_len(recorded.source.physical_tail - 1)
        .unwrap();
    assert!(pe_service::paper_migration::update_installed_log_paths(&with_suffix).is_err());

    let (_mutated_dir, mutated) = copied_generation(&paths);
    let mut paper_bytes = std::fs::read(&mutated.paper_log).unwrap();
    let inside_prefix = usize::try_from(recorded.paper.physical_tail).unwrap() - 1;
    paper_bytes[inside_prefix] ^= 0xff;
    std::fs::write(&mutated.paper_log, paper_bytes).unwrap();
    let before = std::fs::read(&mutated.fixed_main).unwrap();
    assert!(pe_service::paper_migration::update_installed_log_paths(&mutated).is_err());
    assert_eq!(std::fs::read(&mutated.fixed_main).unwrap(), before);

    let (_journal_dir, bad_journal) = copied_generation(&paths);
    append(
        &bad_journal.live_journal,
        b"not a native live-journal event",
    );
    assert!(pe_service::paper_migration::update_installed_log_paths(&bad_journal).is_err());

    let (_short_dir, truncated) = copied_generation(&paths);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&truncated.source_log)
        .unwrap()
        .set_len(recorded.source.physical_tail - 1)
        .unwrap();
    assert!(pe_service::paper_migration::update_installed_log_paths(&truncated).is_err());

    let (_unknown_dir, unknown) = copied_generation(&paths);
    Connection::open(&unknown.fixed_main)
        .unwrap()
        .pragma_update(None, "user_version", 999)
        .unwrap();
    assert!(pe_service::paper_migration::update_installed_log_paths(&unknown).is_err());

    assert_eq!(
        MigrationMetadata::read(&paths.fixed_main).unwrap().unwrap(),
        original_record
    );
}

/// PASS: the `--update-paper-migration-paths` command loads the configuration from the file plus
/// the copied-generation overrides under a cleared environment, reports `updated` then
/// `unchanged`, exits 0, and the copy boots afterwards (#570).
#[test]
fn update_paper_migration_paths_command_rebinds_a_copied_generation() {
    let (dir, paths) = migration_fixture();
    install_generation(&paths);
    let (copy_dir, copied) = copied_generation(&paths);
    let config = dir.path().join("service.toml");
    std::fs::write(&config, "").unwrap();
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"))
            .env_clear()
            .env("PE_PAPER_STATE_DB_PATH", &copied.fixed_main)
            .env("PE_EVENT_LOG_PATH", &copied.paper_log)
            .env("PE_SOURCE_EVENT_LOG_PATH", &copied.source_log)
            .env("PE_LEGACY_WALLET_HISTORY_PATH", &copied.legacy_history)
            .env("PE_JSONL_LOG_PATH", copy_dir.path().join("paper.jsonl"))
            .env("PE_STATUS_PATH", copy_dir.path().join("status.json"))
            .arg(&config)
            .arg("--update-paper-migration-paths")
            .output()
            .unwrap()
    };
    let first = run();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&first.stdout).trim(),
        "paper migration paths updated"
    );
    let second = run();
    assert!(second.status.success());
    assert_eq!(
        String::from_utf8_lossy(&second.stdout).trim(),
        "paper migration paths unchanged"
    );
    let booted = PaperMigrationBoot::prepare(copied, 1_788_192_008).unwrap();
    assert!(booted.session.is_none());
    assert_eq!(booted.record.phase, MigrationPhase::Installed);
}
