//! Composed boot scenario for the read-once source-log walk (#572): one locked whole-file walk
//! plus one bounded suffix walk, publication only after success, the gated repair of a torn
//! final frame, refusal of a truncation into the recorded prefix, a typed boot failure on poison
//! followed by restart recovery, the handoff drift check, and the not-installed fallbacks.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pe_core_types::{
    BasisPoints, CollateralAmount, EventSeq, ReceivedAt, ReconstructionQuality, SourceId,
    SourceTimestamp, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Scanner, Writer};
use pe_execution_core::LiveJournal;
use pe_paper_state::{MigrationMetadata, MigrationPhase, PaperStateDb};
use pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID;
use pe_service::paper_migration::{PaperMigrationBoot, PaperMigrationPaths};
use pe_service::paper_recovery::{
    PaperLogRecord, QualificationStarted, TailBinding, paper_era, replay_membership, scan_paper_log,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::source_log_boot::{SourceLogBoot, SourceLogBootHooks};
use pe_service::trade_poller::{DAILY_BOUNDARY_SOURCE_ID, rebuild_reconciliation_obligations};
use pe_source_polymarket_public::{ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rusqlite::Connection;
use rust_decimal_macros::dec;
use time::OffsetDateTime;

const V1_SCHEMA: &str = include_str!("../../paper-state/tests/fixtures/paper_state_v1.sql");
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NOW_UNIX: i64 = 1_788_192_000;

fn envelope(source_id: &str, schema: u32, parser: u32, payload: &[u8], unix: i64) -> EnvelopeIn {
    let timestamp = OffsetDateTime::from_unix_timestamp(unix).unwrap();
    EnvelopeIn {
        source_id: SourceId(source_id.to_owned()),
        schema_version: schema,
        parser_version: parser,
        observed_at: SourceTimestamp(timestamp),
        received_at: ReceivedAt(timestamp),
        content_type: ContentType::Json,
        payload: payload.to_vec(),
    }
}

fn activity_payload(tx: &str, observed_unix: i64) -> Vec<u8> {
    format!(
        r#"{{"proxyWallet":"{WALLET}","conditionId":"0x{}","asset":"123","side":"BUY","size":"100","price":"0.50","timestamp":"{observed_unix}","transactionHash":"{tx}","outcomeIndex":"0"}}"#,
        "11".repeat(32)
    )
    .into_bytes()
}

fn activity_envelope(tx: &str, observed_unix: i64) -> EnvelopeIn {
    envelope(
        ACTIVITY_WS_SOURCE_ID,
        ACTIVITY_SCHEMA_VERSION,
        ACTIVITY_PARSER_VERSION,
        &activity_payload(tx, observed_unix),
        observed_unix,
    )
}

fn append(path: &Path, envelope_in: EnvelopeIn) {
    let mut writer = Writer::open(path).unwrap();
    writer.append_synced(envelope_in).unwrap();
}

fn append_paper_record(path: &Path, record: &PaperLogRecord) {
    append(
        path,
        envelope(
            "pe-service.paper",
            2,
            1,
            &serde_json::to_vec(record).unwrap(),
            NOW_UNIX,
        ),
    );
}

fn empty_tail() -> TailBinding {
    TailBinding {
        physical_tail: 5,
        last_sequence: None,
        last_hash: "00".repeat(32),
    }
}

fn start_record() -> PaperLogRecord {
    PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
        starting_bankroll: CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
        paper_prefix: empty_tail(),
        source_prefix: empty_tail(),
        live_prefix: empty_tail(),
        artifact_blake3: "artifact".to_owned(),
        static_config_hash: "static".to_owned(),
        hot_config_hash: "config".to_owned(),
        generation: "scenario".to_owned(),
        activation_id: "scenario".to_owned(),
        ranking_batch_id: 572,
        membership: vec![WalletAddress::from_hex(WALLET).unwrap()],
        membership_proofs_hash: "proof".to_owned(),
        schema_version: 3,
        parser_version: 1,
        financial_semantic_version: 1,
    }))
}

fn start_batch() -> Watchlist {
    let score = BasisPoints(200);
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet: WalletAddress::from_hex(WALLET).unwrap(),
            tier: WatchlistTier::Active,
            leader_score_bps: score,
            lcb_5pct_bps: score,
            win_rate_bps: BasisPoints(7_000),
            closed_trades_in_window: 0,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }],
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: 1,
        incubator_count: 0,
    }
}

fn paths_in(dir: &Path) -> PaperMigrationPaths {
    PaperMigrationPaths {
        fixed_main: dir.join("paper_state.db"),
        source_log: dir.join("source.log"),
        paper_log: dir.join("paper.log"),
        live_journal: dir.join("live_journal.log"),
        legacy_history: dir.join("wallet_market_history.json"),
        binary_identity: "scenario-build".to_owned(),
    }
}

/// A version-one generation before any migration boot.
fn version_one_fixture() -> (tempfile::TempDir, PaperMigrationPaths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    Connection::open(&paths.fixed_main)
        .unwrap()
        .execute_batch(V1_SCHEMA)
        .unwrap();
    append(
        &paths.source_log,
        envelope("migration-fixture", 1, 1, br#"{"source":1}"#, NOW_UNIX),
    );
    // Header-only: every paper-log frame is decoded as a paper record by its consumers.
    drop(Writer::open(&paths.paper_log).unwrap());
    drop(LiveJournal::open(&paths.live_journal).unwrap());
    std::fs::write(
        &paths.legacy_history,
        format!(r#"{{"wallets":[{{"wallet":"{WALLET}","markets":["condition-1"]}}]}}"#),
    )
    .unwrap();
    (dir, paths)
}

/// An installed version-two generation whose recorded activation prefix ends at the source log's
/// current tail; every frame appended afterwards lies after that prefix.
fn installed_fixture() -> (tempfile::TempDir, PaperMigrationPaths) {
    let (dir, paths) = version_one_fixture();
    let boot = PaperMigrationBoot::prepare(paths.clone(), NOW_UNIX).unwrap();
    let side_state = PaperStateDb::open(&boot.active_main).unwrap();
    side_state
        .record_migration_activation_facts(
            &serde_json::json!({"fixture": "complete"}),
            &paths.binary_identity,
        )
        .unwrap();
    drop(side_state);
    boot.session.unwrap().finish().unwrap();
    let installed = MigrationMetadata::read(&paths.fixed_main).unwrap().unwrap();
    assert_eq!(installed.phase, MigrationPhase::Installed);
    (dir, paths)
}

fn recorded_source(paths: &PaperMigrationPaths) -> pe_event_log::LogTailBinding {
    MigrationMetadata::read(&paths.fixed_main)
        .unwrap()
        .unwrap()
        .activation_tails
        .unwrap()
        .source
}

fn recorded_source_tail(paths: &PaperMigrationPaths) -> u64 {
    recorded_source(paths).physical_tail
}

/// Sequence of the frame `offset` frames after the recorded activation prefix.
fn sequence_after_prefix(paths: &PaperMigrationPaths, offset: u64) -> Option<EventSeq> {
    let last = recorded_source(paths).last_sequence.unwrap().0;
    Some(EventSeq(last + offset))
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// Bytes this process has requested through read system calls (Linux `/proc/self/io`), which
/// counts page-cache hits too: one whole-file walk reads about the file's length.
#[cfg(target_os = "linux")]
fn read_chars() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn open_error(paths: &PaperMigrationPaths, financial_era: bool) -> anyhow::Error {
    match SourceLogBoot::open(paths, financial_era) {
        Err(error) => error,
        Ok(Some(_)) => panic!("expected the boot walk to fail, it opened"),
        Ok(None) => panic!("expected the boot walk to fail, it reported not installed"),
    }
}

fn assert_same_receipts(index: &SourceReceiptIndex, expected: &SourceReceiptIndex, last: u64) {
    for sequence in 0..=last + 1 {
        let sequence = EventSeq(sequence);
        assert_eq!(
            index.receipt_at(sequence).unwrap(),
            expected.receipt_at(sequence).unwrap(),
            "receipt {sequence:?}"
        );
    }
}

#[test]
fn installed_boot_walks_once_and_publishes_only_after_the_suffix_walk() {
    let (_dir, paths) = installed_fixture();
    let prefix_tail = recorded_source_tail(&paths);
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 1));
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 1)); // duplicate group
    append(
        &paths.source_log,
        envelope("other-source", 1, 1, br#"{"ignored":true}"#, NOW_UNIX + 2),
    );
    append(&paths.source_log, activity_envelope("0xt2", NOW_UNIX + 3));
    assert!(file_len(&paths.source_log) > prefix_tail);

    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    assert_eq!(opened.binding, Scanner::verify(&paths.source_log).unwrap());
    assert_eq!(
        opened.binding.last_sequence,
        sequence_after_prefix(&paths, 4)
    );
    let walked = SourceReceiptIndex::replay(&paths.source_log).unwrap();
    assert_same_receipts(
        &boot.receipt_index(),
        &walked,
        opened.binding.last_sequence.unwrap().0,
    );

    // Boot appends land after the whole-file walk and are walked once, bounded by the sink tail.
    sink.append_durable(activity_envelope("0xt3", NOW_UNIX + 4))
        .unwrap();
    sink.append_durable(activity_envelope("0xt2", NOW_UNIX + 3))
        .unwrap(); // coalesces with the earlier group; the earliest receipt wins
    let after = boot.extend(&mut sink).unwrap();
    assert_eq!(after, Scanner::verify(&paths.source_log).unwrap());
    assert_eq!(after.last_sequence, sequence_after_prefix(&paths, 6));
    let extended = SourceReceiptIndex::replay(&paths.source_log).unwrap();
    assert_same_receipts(
        &boot.receipt_index(),
        &extended,
        after.last_sequence.unwrap().0,
    );

    let paper_state = PaperStateDb::open(&paths.fixed_main).unwrap();
    let published = boot.obligations(&paper_state, &paths.paper_log).unwrap();
    let rebuilt = rebuild_reconciliation_obligations(&paths.source_log, &paper_state).unwrap();
    assert_eq!(published, rebuilt);
    assert_eq!(
        published.len(),
        3,
        "t1, t2, t3 groups coalesced by earliest receipt"
    );
    boot.verify_handoff(&mut sink).unwrap();

    // The installed migration resumes through the walked prefix without a second source scan.
    let resumed = boot
        .prepare_installed(paths.clone(), NOW_UNIX + 10)
        .unwrap();
    assert!(resumed.session.is_none());
    assert_eq!(resumed.record.phase, MigrationPhase::Installed);
}

#[test]
fn obligations_are_refused_before_the_suffix_walk() {
    let (_dir, paths) = installed_fixture();
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    let mut boot = opened.boot;
    let paper_state = PaperStateDb::open(&paths.fixed_main).unwrap();
    let error = boot
        .obligations(&paper_state, &paths.paper_log)
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("before the boot suffix walk"),
        "{error:#}"
    );
    let mut sink = opened.sink;
    let error = boot.verify_handoff(&mut sink).unwrap_err();
    assert!(
        format!("{error:#}").contains("before the boot suffix walk"),
        "{error:#}"
    );
}

#[test]
fn torn_final_frame_after_the_prefix_is_repaired_once_under_the_lock() {
    let (_dir, paths) = installed_fixture();
    let prefix_tail = recorded_source_tail(&paths);
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 1));
    let before_last = file_len(&paths.source_log);
    append(&paths.source_log, activity_envelope("0xt2", NOW_UNIX + 2));
    let clean = Scanner::verify(&paths.source_log).unwrap();
    let bytes = std::fs::read(&paths.source_log).unwrap();
    let last_frame = &bytes[usize::try_from(before_last).unwrap()..];
    assert!(last_frame.len() > 8);
    // A torn copy of the last frame: begins at the verified tail, ends before its declared end.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.source_log)
        .unwrap();
    file.write_all(&last_frame[..last_frame.len() / 2]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(clean.physical_tail > prefix_tail);

    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert_eq!(
        opened.binding, clean,
        "the torn suffix is repaired to the verified tail"
    );
    assert_eq!(file_len(&paths.source_log), clean.physical_tail);
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    sink.append_durable(activity_envelope("0xt3", NOW_UNIX + 3))
        .unwrap();
    let after = boot.extend(&mut sink).unwrap();
    assert_eq!(after, Scanner::verify(&paths.source_log).unwrap());
    assert_eq!(after.last_sequence, sequence_after_prefix(&paths, 3));
}

#[test]
fn truncation_into_the_recorded_prefix_is_refused_without_repair() {
    let (_dir, paths) = installed_fixture();
    let prefix_tail = recorded_source_tail(&paths);
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 1));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&paths.source_log)
        .unwrap();
    file.set_len(prefix_tail - 3).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let bytes_before = std::fs::read(&paths.source_log).unwrap();

    let error = open_error(&paths, false);
    assert!(
        format!("{error:#}").contains("verify and open source event log"),
        "{error:#}"
    );
    assert_eq!(
        std::fs::read(&paths.source_log).unwrap(),
        bytes_before,
        "a truncation into the prefix is never repaired"
    );
}

#[test]
fn reducer_errors_refuse_publication_and_scanner_errors_take_precedence() {
    // A malformed matching activity frame is a reducer error: nothing is published.
    let (_dir, paths) = installed_fixture();
    append(
        &paths.source_log,
        envelope(
            ACTIVITY_WS_SOURCE_ID,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
            br#"{"bad":true}"#,
            NOW_UNIX + 1,
        ),
    );
    let error = open_error(&paths, false);
    assert!(
        format!("{error:#}").contains("rebuild durable activity reconciliation obligations"),
        "{error:#}"
    );

    // The same malformed frame followed by interior corruption reports the scanner error.
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 2));
    let mut bytes = std::fs::read(&paths.source_log).unwrap();
    let flip = bytes.len() - 40;
    bytes[flip] ^= 0xff;
    std::fs::write(&paths.source_log, &bytes).unwrap();
    let error = open_error(&paths, false);
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("verify and open source event log"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("activity reconciliation obligations"),
        "the physical scanner error must win: {rendered}"
    );
}

#[test]
fn poisoned_sink_fails_the_boot_typed_and_a_restart_recovers() {
    let (_dir, paths) = installed_fixture();
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 1));
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    let hooks = Arc::new(SourceLogBootHooks::default());
    hooks
        .poison_sink_before_extend
        .store(true, std::sync::atomic::Ordering::SeqCst);
    boot.set_scenario_hooks(Arc::clone(&hooks));
    sink.append_durable(activity_envelope("0xt2", NOW_UNIX + 2))
        .unwrap();
    let error = boot.extend(&mut sink).unwrap_err();
    assert!(
        format!("{error:#}").contains("source event sink poisoned"),
        "{error:#}"
    );
    drop(sink);
    drop(boot);

    // A plain restart re-verifies the complete file and resumes without operator state.
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert_eq!(opened.binding, Scanner::verify(&paths.source_log).unwrap());
    assert_eq!(
        opened.binding.last_sequence,
        sequence_after_prefix(&paths, 2)
    );
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    let after = boot.extend(&mut sink).unwrap();
    assert_eq!(after, opened.binding);
    let paper_state = PaperStateDb::open(&paths.fixed_main).unwrap();
    assert_eq!(
        boot.obligations(&paper_state, &paths.paper_log).unwrap(),
        rebuild_reconciliation_obligations(&paths.source_log, &paper_state).unwrap()
    );
}

#[test]
fn external_growth_or_truncation_before_the_handoff_fails_closed() {
    for grow in [true, false] {
        let (_dir, paths) = installed_fixture();
        let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        let mut boot = opened.boot;
        let mut sink = opened.sink;
        sink.append_durable(activity_envelope("0xt1", NOW_UNIX + 1))
            .unwrap();
        boot.extend(&mut sink).unwrap();
        let file = std::fs::OpenOptions::new()
            .append(grow)
            .write(true)
            .open(&paths.source_log)
            .unwrap();
        if grow {
            (&file).write_all(b"xx").unwrap();
        } else {
            file.set_len(file_len(&paths.source_log) - 1).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);
        let error = boot.verify_handoff(&mut sink).unwrap_err();
        assert!(
            format!("{error:#}").contains("source-log tail at the runtime handoff"),
            "grow={grow}: {error:#}"
        );
    }
}

#[test]
fn pre_financial_boot_ignores_a_malformed_daily_boundary_frame_and_a_financial_boot_refuses_it() {
    let (_dir, paths) = installed_fixture();
    append(
        &paths.source_log,
        envelope(
            DAILY_BOUNDARY_SOURCE_ID,
            1,
            1,
            br#"{"kind":"other"}"#,
            NOW_UNIX + 1,
        ),
    );
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    drop(opened);
    let error = open_error(&paths, true);
    assert!(
        format!("{error:#}").contains("recover causal daily boundary"),
        "{error:#}"
    );
}

#[test]
fn financial_boot_replays_start_membership_through_the_index_and_recovers_the_boundary() {
    let (_dir, paths) = installed_fixture();
    append_paper_record(&paths.paper_log, &start_record());
    let cutoff = NOW_UNIX + 86_400;
    append(
        &paths.source_log,
        envelope(
            DAILY_BOUNDARY_SOURCE_ID,
            1,
            1,
            &serde_json::to_vec(
                &serde_json::json!({"kind": "daily_boundary", "cutoff_unix": cutoff}),
            )
            .unwrap(),
            cutoff,
        ),
    );
    append(&paths.source_log, activity_envelope("0xt1", cutoff + 1));
    let era = paper_era(scan_paper_log(&paths.paper_log).unwrap());
    assert!(era.start.is_some());

    let opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    let replayed = boot
        .replay_membership(&era, start_batch())
        .unwrap()
        .unwrap();
    let expected = replay_membership(&era, start_batch(), &paths.source_log)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&replayed.watchlist.entries).unwrap(),
        serde_json::to_vec(&expected.watchlist.entries).unwrap()
    );
    assert_eq!(
        replayed.last_ranking_batch_id,
        expected.last_ranking_batch_id
    );

    boot.extend(&mut sink).unwrap();
    let paper_state = PaperStateDb::open(&paths.fixed_main).unwrap();
    let published = boot.obligations(&paper_state, &paths.paper_log).unwrap();
    let mut rebuilt = rebuild_reconciliation_obligations(&paths.source_log, &paper_state).unwrap();
    pe_service::trade_poller::recover_daily_boundary(
        &paths.source_log,
        &paths.paper_log,
        &mut rebuilt,
    )
    .unwrap();
    assert_eq!(published, rebuilt);
}

#[test]
fn generations_that_are_not_exactly_installed_keep_the_existing_flow() {
    let (_dir, paths) = version_one_fixture();
    assert!(SourceLogBoot::open(&paths, false).unwrap().is_none());
    let mid_migration = PaperMigrationBoot::prepare(paths.clone(), NOW_UNIX).unwrap();
    assert!(mid_migration.session.is_some());
    assert!(SourceLogBoot::open(&paths, false).unwrap().is_none());
    drop(mid_migration);

    // A moved generation whose record still names its origin path is not this generation.
    let (_dir2, installed) = installed_fixture();
    let moved = tempfile::tempdir().unwrap();
    for name in [
        "paper_state.db",
        "source.log",
        "paper.log",
        "live_journal.log",
        "wallet_market_history.json",
    ] {
        std::fs::copy(
            installed.fixed_main.parent().unwrap().join(name),
            moved.path().join(name),
        )
        .unwrap();
    }
    let moved_paths = paths_in(moved.path());
    assert!(SourceLogBoot::open(&moved_paths, false).unwrap().is_none());
}

#[test]
fn migration_record_drift_after_the_walk_falls_back_to_the_full_prefix_verification() {
    let (_dir, paths) = installed_fixture();
    append(&paths.source_log, activity_envelope("0xt1", NOW_UNIX + 1));
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    let boot = opened.boot;

    // A copy whose record was rebound to its own paths carries a different activation source
    // binding, so the proof does not apply and the source prefix is verified again: a copy whose
    // source log lost part of that prefix is refused.
    let moved = tempfile::tempdir().unwrap();
    for name in [
        "paper_state.db",
        "source.log",
        "paper.log",
        "live_journal.log",
        "wallet_market_history.json",
    ] {
        std::fs::copy(
            paths.fixed_main.parent().unwrap().join(name),
            moved.path().join(name),
        )
        .unwrap();
    }
    let moved_paths = paths_in(moved.path());
    assert!(pe_service::paper_migration::update_installed_log_paths(&moved_paths).unwrap());
    let prefix_tail = recorded_source_tail(&moved_paths);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&moved_paths.source_log)
        .unwrap();
    file.set_len(prefix_tail - 3).unwrap();
    drop(file);
    let error = boot
        .prepare_installed(moved_paths.clone(), NOW_UNIX + 5)
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("verify recorded source-log prefix"),
        "{error:#}"
    );

    // The origin record still equals the proof: the migration resumes on the walked prefix.
    let resumed = boot.prepare_installed(paths.clone(), NOW_UNIX + 6).unwrap();
    assert_eq!(resumed.record.phase, MigrationPhase::Installed);
    let _keep: PathBuf = resumed.active_main;
}

/// PASS: installed boot plus open-row validation reads less than 1.5 source-log lengths.
/// FAIL: a hidden second whole-log pass exceeds that bound.
/// Acceptance criterion 1: an installed boot reads the source log once (plus the bounded suffix
/// of its own appends). Measured through the process read counter rather than an internal
/// counter, so a hidden second pass in the walk, migration resume, obligation rebuild, or the
/// handoff would show up as a second file length. The index-backed membership path is measured
/// the same way in `paper_recovery`'s membership replay test.
#[cfg(target_os = "linux")]
#[test]
fn installed_boot_reads_the_source_log_once() {
    let (_dir, paths) = installed_fixture();
    {
        let mut writer = Writer::open(&paths.source_log).unwrap();
        for index in 0..20_000_i64 {
            writer
                .append(activity_envelope(
                    &format!("0x{index:064x}"),
                    NOW_UNIX + 1 + index,
                ))
                .unwrap();
        }
        writer.sync().unwrap();
    }
    let paper_state = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
    install_committed_open_read(&paper_state, &paths.source_log);
    let length = file_len(&paths.source_log);
    assert!(length > 2_000_000, "fixture log is {length} bytes");

    let before = read_chars();
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    let resumed = boot
        .prepare_installed(paths.clone(), NOW_UNIX + 10)
        .unwrap();
    assert_eq!(resumed.record.phase, MigrationPhase::Installed);
    sink.append_durable(activity_envelope("0xboot", NOW_UNIX + 30_000))
        .unwrap();
    let after_binding = boot.extend(&mut sink).unwrap();
    let obligations = boot.obligations(&paper_state, &paths.paper_log).unwrap();
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&paper_state, &boot.receipt_index())
            .unwrap(),
        1
    );
    boot.verify_handoff(&mut sink).unwrap();
    let read = read_chars() - before;

    assert_eq!(obligations.len(), 20_001);
    assert_eq!(after_binding, Scanner::verify(&paths.source_log).unwrap());
    assert!(
        read >= length,
        "the walk must read the whole log: read {read} of {length} bytes"
    );
    assert!(
        read < length + length / 2,
        "more than one whole-file pass: read {read} bytes for a {length}-byte log"
    );
}

fn install_committed_open_read(paper: &Arc<PaperStateDb>, source_log: &Path) {
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    support::install_empty_anchor(paper, wallet, 0);
    paper
        .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: NOW_UNIX,
        })
        .unwrap();
    let payload = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": WALLET, "timestamp": NOW_UNIX + 25_000,
        "conditionId": "0xboot-open", "type": "TRADE", "size": "2.5", "usdcSize": "1.25",
        "transactionHash": "0xboot-open", "price": "0.5", "asset": "boot-token",
        "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false,
    }]))
    .unwrap();
    let mut writer = Writer::open(source_log).unwrap();
    let (read, commitment) = support::append_committed_read_v1(
        &mut writer,
        wallet,
        &payload,
        NOW_UNIX + 25_001,
        NOW_UNIX + 25_002,
    );
    let context = support::read_context(&read, commitment, NOW_UNIX + 25_003);
    let mut engine = pe_service::bucket_commit::BucketCommitEngine::load(
        Arc::clone(paper),
        pe_service::paper_recovery::build_leader_ledger(paper).unwrap(),
    )
    .unwrap();
    let committed = engine
        .commit(
            read.aggregates,
            &context,
            pe_service::bucket_commit::FrozenDecisionBasis {
                win_rate_p: pe_core_types::Probability::ZERO,
                bankroll: rust_decimal::Decimal::ZERO,
            },
        )
        .unwrap();
    assert_eq!(committed.pending.len(), 1);
}

/// PASS: validation reads only its referenced page and commitment after indexing a log with
/// 20,000 trailing padding frames. FAIL: validation performs another whole-log scan.
#[cfg(target_os = "linux")]
#[test]
fn continuation_validation_reads_only_referenced_frames() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    install_committed_open_read(&paper, &source_path);
    let referenced_bytes = file_len(&source_path);
    {
        let mut writer = Writer::open(&source_path).unwrap();
        for index in 0..20_000_i64 {
            writer
                .append(envelope(
                    "scenario.padding",
                    1,
                    1,
                    &serde_json::to_vec(
                        &serde_json::json!({"index": index, "padding": "x".repeat(128)}),
                    )
                    .unwrap(),
                    NOW_UNIX,
                ))
                .unwrap();
        }
        writer.sync().unwrap();
    }
    let padding_bytes = file_len(&source_path) - referenced_bytes;
    let index = SourceReceiptIndex::replay(&source_path).unwrap();
    let before = read_chars();
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&paper, &index).unwrap(),
        1
    );
    let read = read_chars() - before;
    assert!(padding_bytes > 2_000_000);
    assert!(read > 0, "the referenced source frames must be read");
    assert!(
        read < padding_bytes / 10,
        "validation read {read} bytes with {referenced_bytes} referenced bytes and {padding_bytes} padding bytes"
    );
}

/// PASS: the installed boot reducer collects a cross-second binding in its existing walk,
/// retains the original obligation without a target disposition, and clears it after the
/// same target revision commits. It agrees with the fallback reducer at both boundaries.
#[test]
fn installed_boot_binding_requires_its_durable_target() {
    let (_dir, paths) = installed_fixture();
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    let mut writer = Writer::open(&paths.source_log).unwrap();
    let stream_payload = activity_payload("binding-installed-boot", NOW_UNIX + 1);
    let stream =
        pe_source_polymarket_public::parse_activity_trade_observation(&stream_payload).unwrap();
    let stream_receipt = writer
        .append_synced(envelope(
            ACTIVITY_WS_SOURCE_ID,
            2,
            2,
            &stream_payload,
            NOW_UNIX + 1,
        ))
        .unwrap();
    let mut history: serde_json::Value = serde_json::from_slice(&stream_payload).unwrap();
    history["timestamp"] = serde_json::json!(NOW_UNIX + 2);
    history["type"] = serde_json::json!("TRADE");
    history["usdcSize"] = serde_json::json!("50");
    let (read, _) = support::append_committed_read_v2(
        &mut writer,
        wallet,
        &serde_json::to_vec(&[history]).unwrap(),
        NOW_UNIX + 3,
        NOW_UNIX + 3,
    );
    let target = &read.aggregates[0];
    let proof: serde_json::Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
    let pages = serde_json::from_value::<
        Vec<pe_source_polymarket_public::ReconciliationPageEvidence>,
    >(proof["pages"].clone())
    .unwrap();
    let binding = pe_service::bucket_commit::ObservationBinding {
        stream_group_id: stream.group_id.key().clone(),
        stream_receipt,
        history_group_id: target.group_id.key().clone(),
        semantic_revision: target.semantic_revision.as_str().to_owned(),
        page_raw_hash: read.page.raw_hash.clone(),
        page_occurrence_index: 0,
        identity_provenance: None,
        identity_receipt: None,
    };
    let payload = pe_service::bucket_commit::activity_read_commitment_payload_v2(
        wallet,
        NOW_UNIX + 3,
        std::slice::from_ref(&read.page),
        &pages,
        &[binding],
    )
    .unwrap();
    let commitment = writer
        .append_synced(envelope(
            pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            2,
            1,
            &payload,
            NOW_UNIX + 3,
        ))
        .unwrap();
    drop(writer);
    let paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
    for disposed in [false, true] {
        if disposed {
            support::install_empty_anchor(&paper, wallet, 0);
            let mut engine = pe_service::bucket_commit::BucketCommitEngine::load(
                paper.clone(),
                pe_service::paper_recovery::build_leader_ledger(&paper).unwrap(),
            )
            .unwrap();
            let mut context = support::read_context(&read, commitment, NOW_UNIX + 3);
            context
                .observed_source_receipts
                .insert(target.group_id.key().clone(), stream_receipt);
            context.observation_provenance.insert(
                target.group_id.key().clone(),
                pe_copy_signal_engine::TradeProvenance::ActivityWs,
            );
            engine
                .commit_with_freshness_policy(
                    read.aggregates.clone(),
                    &context,
                    pe_service::bucket_commit::FrozenDecisionBasis {
                        win_rate_p: pe_core_types::Probability::ZERO,
                        bankroll: rust_decimal::Decimal::ZERO,
                    },
                    Some(pe_service::bucket_commit::PaperFreshnessPolicy {
                        activity_ws_enabled: true,
                        copy_latency_budget_secs: 2,
                    }),
                )
                .unwrap();
        }
        let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        let mut boot = opened.boot;
        let mut sink = opened.sink;
        boot.extend(&mut sink).unwrap();
        let published = boot.obligations(&paper, &paths.paper_log).unwrap();
        let rebuilt = rebuild_reconciliation_obligations(&paths.source_log, &paper).unwrap();
        assert_eq!(published, rebuilt);
        assert_eq!(published.len(), usize::from(!disposed));
        boot.verify_handoff(&mut sink).unwrap();
    }
}

/// Exercise the actual binary, including migration-selected state, rather than assembling its
/// selection helpers in the test. The server advances the batch when it returns the marker.
async fn pre_start_boot_membership_case(case: PreStartBootCase) {
    use axum::{Json, Router, extract::State, http::Uri, routing::get};
    use std::sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    };

    #[derive(Clone)]
    struct BootSource {
        requests: Arc<Mutex<Vec<Uri>>>,
        batch: Arc<AtomicI64>,
        selected: WalletAddress,
        fenced: WalletAddress,
    }
    async fn respond(State(source): State<BootSource>, uri: Uri) -> Json<serde_json::Value> {
        use serde_json::json;
        source.requests.lock().unwrap().push(uri.clone());
        let query = uri.query().unwrap_or_default();
        Json(match uri.path() {
            "/rest/v1/service_config" => {
                let rows = [
                    ("active_watchlist_size", "1", "integer"),
                    ("mode", "paper", "text"),
                    ("max_fill_price", "0.85", "decimal"),
                    ("min_fill_price", "0.15", "decimal"),
                    ("min_resolution_horizon_secs", "60", "integer"),
                    ("max_resolution_horizon_secs", "172800", "integer"),
                    ("price_impact_cap_bps", "100", "integer"),
                    ("flip_human_approved", "false", "bool"),
                    (
                        "kelly_fraction_above_default_human_approved",
                        "false",
                        "bool",
                    ),
                    ("per_trade_cap", "unlimited", "text"),
                    ("slippage_rate", "0.01", "decimal"),
                    ("sizing_mode", "dollar", "text"),
                    ("sizing_dollar_usd", "25", "decimal"),
                    ("sizing_contracts", "1", "integer"),
                    ("fill_mode", "clob_best_ask", "text"),
                    ("polymarket_fee_rate", "0.04", "decimal"),
                ];
                json!(rows.map(|(key, value, value_type)| json!({"key": key, "value": value, "value_type": value_type})))
            }
            "/rest/v1/ranking_batches" => {
                let captured = source.batch.swap(9, Ordering::SeqCst);
                json!([{"batch_id": captured}])
            }
            "/rest/v1/ranking_entries" => {
                assert_eq!(source.batch.load(Ordering::SeqCst), 9);
                assert!(query.contains("batch_id=eq.8"), "{uri}");
                assert!(query.contains("limit=200"), "{uri}");
                assert!(query.contains("survives=is.true"), "{uri}");
                json!([
                    {"batch_id":8,"rank":1,"wallet_hex":source.fenced,"ls_tstat":"3","hit_rate":"0.6","n_trades":20,"last_trade_unix":10,"survives":true},
                    {"batch_id":8,"rank":2,"wallet_hex":source.selected,"ls_tstat":"2","hit_rate":"0.6","n_trades":20,"last_trade_unix":10,"survives":true}
                ])
            }
            "/rest/v1/paper_bankroll" => json!([{"bankroll_str":"1000"}]),
            "/rest/v1/paper_positions" => json!([]),
            "/activity" => {
                assert!(
                    query.contains(&format!("user={}", source.selected)),
                    "{uri}"
                );
                json!([{"proxyWallet":source.selected,"timestamp":10,"conditionId":"condition-1","type":"TRADE","size":"1","usdcSize":"0.5","transactionHash":"0xboot-membership","price":"0.5","asset":"123","side":"BUY","outcomeIndex":0}])
            }
            "/positions" => {
                assert!(
                    query.contains(&format!("user={}", source.selected)),
                    "{uri}"
                );
                if query.contains("redeemable=false") {
                    json!([{"proxyWallet":source.selected,"asset":"123","conditionId":"condition-1","size":"1","outcomeIndex":0,"negativeRisk":false}])
                } else {
                    json!([])
                }
            }
            "/markets" => json!([{"conditionId":"condition-1","clobTokenIds":["123","456"]}]),
            _ => panic!("unexpected boot request {uri}"),
        })
    }

    let (dir, mut paths) = version_one_fixture();
    paths.binary_identity = pe_service::build_info::embedded()
        .source_revision
        .to_owned();
    let selected = WalletAddress::from_hex(WALLET).unwrap();
    let fenced = WalletAddress([0x22; 20]);
    // The configured v1 main disagrees with the active side main on the eligible wallet.
    let configured = Connection::open(&paths.fixed_main).unwrap();
    configured.execute_batch("CREATE TABLE wallet_fences (wallet_hex TEXT PRIMARY KEY NOT NULL, source_trade_id TEXT NOT NULL, cause TEXT NOT NULL, proof_json TEXT NOT NULL, fenced_at_unix INTEGER NOT NULL);").unwrap();
    configured
        .execute(
            "INSERT INTO wallet_fences VALUES (?1, 'configured', 'invalid_mapping', '{}', 1)",
            [selected.to_string()],
        )
        .unwrap();
    drop(configured);
    let boot = PaperMigrationBoot::prepare(paths.clone(), NOW_UNIX).unwrap();
    assert_ne!(boot.active_main, paths.fixed_main);
    let active = Connection::open(&boot.active_main).unwrap();
    active
        .execute(
            "DELETE FROM wallet_fences WHERE wallet_hex = ?1",
            [selected.to_string()],
        )
        .unwrap();
    active
        .execute(
            "INSERT INTO wallet_fences VALUES (?1, 'active', 'invalid_mapping', '{}', 1)",
            [fenced.to_string()],
        )
        .unwrap();
    if matches!(case, PreStartBootCase::Empty) {
        active.execute("INSERT INTO wallet_fences VALUES (?1, 'active-selected', 'invalid_mapping', '{}', 1)", [selected.to_string()]).unwrap();
    }
    drop(active);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let source = BootSource {
        requests: requests.clone(),
        batch: Arc::new(AtomicI64::new(8)),
        selected,
        fenced,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .fallback(get(respond))
        .with_state(source.clone());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let cfg = pe_service::config::ServiceConfig {
        bind: "127.0.0.1:0".to_owned(),
        paper_state_db_path: paths.fixed_main.clone(),
        source_event_log_path: paths.source_log.clone(),
        event_log_path: paths.paper_log.clone(),
        legacy_wallet_history_path: paths.legacy_history.clone(),
        jsonl_log_path: dir.path().join("service.jsonl"),
        status_path: dir.path().join("status.json"),
        supabase_url: base.clone(),
        supabase_secret_key: "fixture".to_owned(),
        supabase_authoritative: true,
        polymarket_base_url: base.clone(),
        gamma_base_url: base.clone(),
        polymarket_clob_base_url: base.clone(),
        polygon_receipt_rpc_url: base,
        bankroll_usd: "1000".to_owned(),
        ..Default::default()
    };
    let config_path = dir.path().join("service.toml");
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
    let output = boot_binary(&config_path, true).await;
    let stderr = String::from_utf8(output.stderr).unwrap();
    let boot_requests = requests.lock().unwrap().clone();
    assert_eq!(
        boot_requests
            .iter()
            .filter(|uri| uri.path() == "/rest/v1/ranking_batches")
            .count(),
        1,
        "{stderr}"
    );
    assert_eq!(
        boot_requests
            .iter()
            .filter(|uri| uri.path() == "/rest/v1/ranking_entries")
            .count(),
        1,
        "{stderr}"
    );
    let brackets = boot_requests
        .iter()
        .filter(|uri| uri.path() == "/activity")
        .collect::<Vec<_>>();
    if matches!(case, PreStartBootCase::Empty) {
        assert!(!output.status.success());
        assert!(
            stderr.contains("no wallets eligible after durable fence/history/acceptance filtering"),
            "{stderr}"
        );
        assert!(brackets.is_empty());
    } else {
        assert!(output.status.success(), "{stderr}");
        assert_eq!(brackets.len(), 3);
        assert!(
            brackets
                .iter()
                .all(|uri| uri.query().unwrap().contains(&format!("user={selected}")))
        );
        let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
        assert!(paper.wallet_history_complete(&selected).unwrap());
        assert!(paper.position_validation_current(&selected).unwrap());
        assert!(paper.is_wallet_fenced(&fenced).unwrap());
        assert!(!paper.is_wallet_fenced(&selected).unwrap());
    }
    if matches!(case, PreStartBootCase::InvalidContinuation) {
        // The completed anchor boot supplies an installed generation and a reusable selected
        // wallet. Keep the newer pending wallet outside that bracket so boot cannot overwrite
        // its evidence before the all-wallet continuation census.
        let paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
        let (snapshot_state, snapshot_source, id) = support::post_snapshot_invalid_continuation(
            &paper,
            &paths.fixed_main,
            &paths.source_log,
            WalletAddress([0xcc; 20]),
        );
        let offline = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"))
            .env_clear()
            .arg("--validate-open-continuations")
            .arg("--paper-state")
            .arg(snapshot_state)
            .arg("--source-log")
            .arg(snapshot_source)
            .output()
            .unwrap();
        assert!(offline.status.success(), "{offline:?}");
        assert_eq!(offline.stdout, b"open_rows=1 validated=1\n");
        // Compare raw SQLite values, including the exact JSON strings, without decoding or
        // re-encoding the rows. Include all wallets and all cursor/history columns.
        let capture = || {
            let connection = Connection::open(&paths.fixed_main).unwrap();
            [
                "decision_pending",
                "leader_positions",
                "wallet_market_history_v2",
                "entry_gate_results",
                "wallet_history_status_v2",
                "poll_cursors",
                "activity_groups",
                "activity_group_revisions",
            ]
            .map(|table| {
                let mut statement = connection
                    .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                    .unwrap();
                let columns = statement.column_count();
                let rows = statement
                    .query_map([], |row| {
                        (0..columns)
                            .map(|column| row.get::<_, rusqlite::types::Value>(column))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                (table, rows)
            })
        };
        let before = capture();
        let paper_log = std::fs::read(&paths.paper_log).unwrap();
        let live_log = std::fs::read(&paths.live_journal).unwrap();
        let source_log = std::fs::read(&paths.source_log).unwrap();
        assert!(!cfg.status_path.exists());
        requests.lock().unwrap().clear();
        source.batch.store(8, Ordering::SeqCst);
        let output = boot_binary(&config_path, false).await;
        let stderr = String::from_utf8(output.stderr).unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(!output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stderr.contains("validate open decision continuations before resume"),
            "{stderr}"
        );
        assert!(
            stderr.contains(&format!("open decision continuation {id}:")),
            "{stderr}"
        );
        assert_eq!(capture(), before);
        assert_eq!(std::fs::read(&paths.paper_log).unwrap(), paper_log);
        assert_eq!(std::fs::read(&paths.live_journal).unwrap(), live_log);
        assert_eq!(std::fs::read(&paths.source_log).unwrap(), source_log);
        assert!(!cfg.status_path.exists());
        for message in ["pe-service listening", "service_config poll loop started"] {
            assert!(!stdout.contains(message), "{stdout}");
            assert!(!stderr.contains(message), "{stderr}");
        }
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .map(|uri| uri.path().to_owned())
                .collect::<Vec<_>>(),
            [
                "/rest/v1/service_config",
                "/rest/v1/ranking_batches",
                "/rest/v1/ranking_entries",
                "/rest/v1/paper_bankroll",
                "/rest/v1/paper_positions",
            ],
            "only boot reads; no producer requests"
        );
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}

enum PreStartBootCase {
    Selected,
    Empty,
    InvalidContinuation,
}

async fn boot_binary(config_path: &Path, exit_after_anchors: bool) -> std::process::Output {
    fn read_pipe(mut pipe: impl std::io::Read) -> Vec<u8> {
        let mut bytes = Vec::new();
        pipe.read_to_end(&mut bytes).unwrap();
        bytes
    }
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .current_dir(config_path.parent().unwrap())
        .arg(config_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if exit_after_anchors {
        command.arg("--exit-after-anchors");
    }
    let mut child = command.spawn().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut stdout = tokio::task::spawn_blocking(move || read_pipe(stdout));
    let mut stderr = tokio::task::spawn_blocking(move || read_pipe(stderr));
    // EOF signals process completion. A regression that starts serving must fail within a
    // bound, killing and reaping this child rather than leaving the test waiting for shutdown.
    let completed = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::join!(&mut stdout, &mut stderr)
    })
    .await;
    let timed_out = completed.is_err();
    let (stdout, stderr) = match completed {
        Ok(output) => output,
        Err(_) => {
            child.kill().unwrap();
            tokio::join!(stdout, stderr)
        }
    };
    let output = std::process::Output {
        status: tokio::task::spawn_blocking(move || child.wait().unwrap())
            .await
            .unwrap(),
        stdout: stdout.unwrap(),
        stderr: stderr.unwrap(),
    };
    assert!(!timed_out, "boot exceeded its failure bound: {output:?}");
    output
}

#[tokio::test]
async fn fresh_pre_start_boot_pins_batch_and_brackets_active_main_survivors() {
    pre_start_boot_membership_case(PreStartBootCase::Selected).await;
}

#[tokio::test]
async fn fresh_pre_start_boot_refuses_empty_active_main_selection() {
    pre_start_boot_membership_case(PreStartBootCase::Empty).await;
}

/// PASS: the real binary rejects the altered post-snapshot continuation by ID before resume,
/// producer requests, HTTP serving or status publication, preserving every pending/ledger/gate/
/// cursor/group value and both financial logs. The older offline census still passes.
#[tokio::test]
async fn post_snapshot_invalid_continuation_refuses_real_binary_boot() {
    pre_start_boot_membership_case(PreStartBootCase::InvalidContinuation).await;
}
